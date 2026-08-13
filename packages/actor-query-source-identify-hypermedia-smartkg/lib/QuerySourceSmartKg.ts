/* eslint-disable import/no-nodejs-modules */
import {
  closeSync,
  createReadStream,
  createWriteStream,
  existsSync,
  fstatSync,
  mkdirSync,
  openSync,
  renameSync,
  readSync,
  unlinkSync,
} from 'node:fs';
import { basename, isAbsolute, join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import type { MediatorHttp } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  MetadataBindings,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { BufferedIterator, EmptyIterator } from 'asynciterator';
import { termToString } from 'rdf-string';
import type { ISmartKgFamily, ISmartKgMetadata } from './Utils';
import {
  extractPredicates,
  findMatchingFamilies,
  getShippingStrategyHint,
  getStarPatternCount,
  hasInfrequentPredicate,
  metadataToSets,
  parseSmartKgMetadata,
  resolveFamiliesToMaterialized,
  selectOptimalFamilies,
} from './Utils';

type HdtDocument = {
  searchBindings: (
    bindingsFactory: BindingsFactory,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    options: { offset: number; limit: number },
  ) => Promise<{ bindings?: RDF.Bindings[] }>;
  countTriples: (
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
  ) => Promise<{ totalCount: number }>;
  close: () => Promise<void>;
};
let hdtModulePromise: Promise<{ fromFile: (path: string) => Promise<HdtDocument> }> | undefined;
const partitionDownloadPromises = new Map<string, Promise<string>>();
const smartKgMetadataPromises = new Map<string, Promise<ISmartKgMetadata>>();
interface ISmartKgPlusPlanStep {
  signature: string;
  control: string;
}
interface IStarExecution {
  patterns: Algebra.Pattern[];
  control?: string;
}
const smartKgPlusPlanPromises = new Map<string, Promise<ISmartKgPlusPlanStep[] | undefined>>();

async function loadHdtModule(): Promise<{ fromFile: (path: string) => Promise<HdtDocument> }> {
  if (!hdtModulePromise) {
    hdtModulePromise = <Promise<{ fromFile: (path: string) => Promise<HdtDocument> }>> import('hdt');
  }
  return hdtModulePromise;
}

function createMetadataValidationState(): MetadataBindings['state'] {
  const listeners: (() => void)[] = [];
  const state: MetadataBindings['state'] = {
    valid: true,
    invalidate: (): void => {
      if (!state.valid) {
        return;
      }
      state.valid = false;
      for (const listener of listeners) {
        listener();
      }
    },
    addInvalidateListener: (listener: () => void): void => {
      listeners.push(listener);
    },
  };
  return state;
}

class HdtDocumentCache {
  private readonly cache = new Map<string, Promise<HdtDocument>>();
  private opening: Promise<void> = Promise.resolve();
  // Native HDT documents map data and indexes outside the V8 heap. Keeping
  // several large partitions open made the client retain many gigabytes even
  // after a family had been evaluated. Two documents still permit reuse for a
  // join while keeping memory bounded on mobile-class clients.
  private static readonly maxDocuments = 2;

  public async getDocument(path: string): Promise<HdtDocument> {
    const existing = this.cache.get(path);
    if (existing) {
      // Refresh the insertion order so this map acts as an LRU cache.
      this.cache.delete(path);
      this.cache.set(path, existing);
      return existing;
    }
    // The native HDT index builder is not safe when multiple unindexed partitions are opened concurrently.
    const documentPromise = this.opening.then(async() => (await loadHdtModule()).fromFile(path));
    this.opening = documentPromise.then(() => {}, () => {});
    this.cache.set(path, documentPromise);
    while (this.cache.size > HdtDocumentCache.maxDocuments) {
      const oldestPath = this.cache.keys().next().value;
      if (oldestPath === undefined) {
        break;
      }
      const evicted = this.cache.get(oldestPath);
      this.cache.delete(oldestPath);
      if (evicted) {
        try {
          await (await evicted).close();
        } catch {
          // A failed/opening document has no usable native handle to close.
        }
      }
    }
    return documentPromise;
  }

  public async dispose(): Promise<void> {
    const documents = [ ...this.cache.values() ];
    this.cache.clear();
    await Promise.all(documents.map(async(document) => {
      try {
        await (await document).close();
      } catch {
        // Ignore documents that failed while opening.
      }
    }));
  }
}

class ArrayBindingsIterator extends BufferedIterator<RDF.Bindings> {
  public constructor(bindingsPromise: Promise<RDF.Bindings[]>, variables: MetadataBindings['variables']) {
    super({ autoStart: true, maxBufferSize: 256 });
    this.pushAll(bindingsPromise, variables).catch(error => this.destroy(<Error> error));
  }

  private async pushAll(
    bindingsPromise: Promise<RDF.Bindings[]>,
    variables: MetadataBindings['variables'],
  ): Promise<void> {
    try {
      const bindings = await bindingsPromise;
      this.setProperty('metadata', {
        state: createMetadataValidationState(),
        cardinality: { type: 'exact', value: bindings.length },
        variables,
        next: [],
      } satisfies MetadataBindings);
      for (const [ index, binding ] of bindings.entries()) {
        this._push(binding);
        if ((index + 1) % 128 === 0) {
          await new Promise<void>(resolve => setImmediate(resolve));
        }
      }
      this.close();
    } catch (error) {
      this.destroy(<Error> error);
    }
  }

  public override _read(_count: number, done: () => void): void {
    done();
  }
}

class StreamingBindingsIterator extends BufferedIterator<RDF.Bindings> {
  private emitted = 0;
  private readonly state = createMetadataValidationState();
  private availableSlots = 128;
  private readonly slotWaiters: (() => void)[] = [];

  public constructor(
    producer: (emit: (binding: RDF.Bindings) => Promise<void>) => Promise<void>,
    private readonly variables: MetadataBindings['variables'],
  ) {
    super({ autoStart: true, maxBufferSize: 128 });
    this.setProperty('metadata', {
      state: this.state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
    producer(async(binding) => {
      await this.acquireSlot();
      if (this.done) {
        this.releaseSlot();
        return;
      }
      this.emitted++;
      this._push(binding);
    }).then(() => {
      this.setProperty('metadata', {
        state: createMetadataValidationState(),
        cardinality: { type: 'exact', value: this.emitted },
        variables: this.variables,
        next: [],
      } satisfies MetadataBindings);
      this.state.invalidate();
      this.close();
    }).catch(error => this.destroy(<Error> error));
  }

  public override _read(_count: number, done: () => void): void {
    done();
  }

  public override read(): RDF.Bindings | null {
    const binding = super.read();
    if (binding !== null) {
      this.releaseSlot();
    }
    return binding;
  }

  protected override _destroy(cause: Error | undefined, callback: (error?: Error) => void): void {
    while (this.slotWaiters.length > 0) {
      this.slotWaiters.shift()!();
    }
    super._destroy(cause, callback);
  }

  private async acquireSlot(): Promise<void> {
    if (this.availableSlots > 0) {
      this.availableSlots--;
      return;
    }
    await new Promise<void>(resolve => this.slotWaiters.push(resolve));
  }

  private releaseSlot(): void {
    const waiter = this.slotWaiters.shift();
    if (waiter) {
      waiter();
    } else {
      this.availableSlots++;
    }
  }
}

export class QuerySourceSmartKg implements IQuerySource {
  public readonly referenceValue: string;

  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly mediatorHttp: MediatorHttp;
  private readonly defaultContext: IActionContext;
  private readonly cacheFolder: string;
  private readonly hdtCache = new HdtDocumentCache();
  private readonly originalSourceUrl: string;
  private readonly metadataUrl: string;
  private readonly partitionsBaseUrl: string;
  private readonly planUrl: string;
  private readonly mediatorQuerySourceDereferenceLink?: any;
  private readonly smartKgPlusSource: boolean;
  private readonly hdtPatternCache = new Map<string, Promise<RDF.Bindings[]>>();
  private readonly patternCardinalityCache = new Map<string, number>();
  private bindingsFactory: BindingsFactory | undefined;
  private metadata: ISmartKgMetadata | undefined;

  public constructor(
    url: string,
    dataFactory: ComunicaDataFactory,
    mediatorHttp: MediatorHttp,
    context: IActionContext,
    mediatorQuerySourceDereferenceLink?: any,
  ) {
    const normalizedUrl = normalizeUrl(url);
    const sourceLocation = new URL(normalizedUrl);
    const datasetName = sourceLocation.pathname.split('/').filter(Boolean).at(-1);
    if (!datasetName) {
      throw new Error(`SmartKG source URL must identify a dataset path: ${url}`);
    }
    const origin = sourceLocation.origin;

    this.referenceValue = normalizedUrl;
    this.originalSourceUrl = normalizedUrl;
    this.metadataUrl = `${origin}/molecule/${datasetName}`;
    this.partitionsBaseUrl = `${origin}/molecule/${datasetName}`;
    this.planUrl = `${origin}/plan`;
    this.smartKgPlusSource = datasetName.toLowerCase() === 'smartkg+';
    this.dataFactory = dataFactory;
    this.mediatorHttp = mediatorHttp;
    this.defaultContext = context;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.cacheFolder = join(process.cwd(), '.smartkg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder, { recursive: true });
    }

    const algebraFactory = new AlgebraFactory(<RDF.DataFactory><unknown> this.dataFactory);
    this.selectorShape = {
      type: 'disjunction',
      children: [
        {
          type: 'operation',
          operation: {
            operationType: 'pattern',
            pattern: algebraFactory.createPattern(
              this.dataFactory.variable('s'),
              this.dataFactory.variable('p'),
              this.dataFactory.variable('o'),
            ),
          },
          variablesOptional: [
            this.dataFactory.variable('s'),
            this.dataFactory.variable('p'),
            this.dataFactory.variable('o'),
          ],
        },
        {
          type: 'operation',
          operation: { operationType: 'type', type: Algebra.Types.BGP },
        },
        {
          type: 'operation',
          operation: { operationType: 'type', type: Algebra.Types.JOIN },
        },
      ],
    };
  }

  public async getFilterFactor(_context: IActionContext): Promise<number> {
    return 0.5;
  }

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    const variables = getOperationVariables(operation);
    return <BindingsStream><unknown>
      new StreamingBindingsIterator(
        emit => this.streamOperation(operation, context, options, emit),
        variables,
      );
  }

  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): AsyncIterator<RDF.Quad> {
    return new EmptyIterator<RDF.Quad>();
  }

  public async queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('ASK queries are not supported by QuerySourceSmartKg.');
  }

  public async queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('UPDATE queries are not supported by QuerySourceSmartKg.');
  }

  public toString(): string {
    return `QuerySourceSmartKg(${this.referenceValue})`;
  }

  public async dispose(): Promise<void> {
    this.hdtPatternCache.clear();
    await this.hdtCache.dispose();
  }

  private async streamOperation(
    operation: Algebra.Operation,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    if (isKnownOperation(operation, Algebra.Types.BGP)) {
      await this.streamPatternsByStars(operation.patterns, context, options, emit);
      return;
    }
    if (isKnownOperation(operation, Algebra.Types.JOIN)) {
      const patterns = collectPatterns(operation);
      if (patterns.length === 0) {
        throw new Error(`Unsupported non-pattern operation '${operation.type}' in QuerySourceSmartKg join.`);
      }
      await this.streamPatternsByStars(patterns, context, options, emit);
      return;
    }

    if (!isKnownOperation(operation, Algebra.Types.PATTERN)) {
      throw new Error(`Unsupported operation type '${operation.type}' for QuerySourceSmartKg.`);
    }
    for (const binding of await this.evaluatePattern(operation, context, options)) {
      await emit(binding);
    }
  }

  private async streamPatternsByStars(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const stars = await this.orderStarsForExecution(groupPatternsBySubject(patterns), context);
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];

    for (const [ index, star ] of stars.entries()) {
      const starPatterns = star.patterns;
      if (this.smartKgPlusSource && index === stars.length - 1 && star.control &&
        !new URL(star.control, this.originalSourceUrl).pathname.includes('/molecule/')) {
        await this.streamSmartKgPlusControlledStar(starPatterns, results, star.control, context, options, emit);
        return;
      }
      if (index === stars.length - 1 && starPatterns.length === 1 &&
        !star.control && !await this.shouldUsePartitions(starPatterns[0], context)) {
        await this.streamJoinedFallbackBindings(results, starPatterns[0], context, options, emit);
        return;
      }
      const starResults = await this.evaluateStar(starPatterns, context, options, star.control);
      if (index === stars.length - 1) {
        await this.emitJoinedBindings(results, starResults, emit);
        return;
      }
      results = await this.joinBindingsLists(results, starResults);
      if (results.length === 0) {
        return;
      }
    }
  }

  private async orderStarsForExecution(
    stars: Algebra.Pattern[][],
    context: IActionContext,
  ): Promise<IStarExecution[]> {
    if (stars.length < 2) {
      return stars.map(patterns => ({ patterns }));
    }

    // The original SmartKG client planner orders subplans using the smallest
    // TPF cardinality in every star. SmartKG+ deliberately uses the same
    // connected ordering: its server plan controls can choose an incomplete
    // typed family and generate enormous intermediates. This changes only the
    // smartkg+ route; the regular SmartKG branch already used this planner.
    const estimated = await Promise.all(stars.map(async star => ({
      star,
      cardinality: Math.min(...await Promise.all(
        star.map(pattern => this.estimateOriginalSourcePatternCardinality(pattern, context)),
      )),
    })));
    return orderConnectedStarsByCardinality(estimated).map(patterns => ({ patterns }));
  }

  private async fetchSmartKgPlusStarOrder(
    stars: Algebra.Pattern[][],
  ): Promise<IStarExecution[] | undefined> {
    const patterns = stars.flat();
    // The SmartKG+ cost models require both values. They affect the shipping
    // annotation, while this client uses the server's cardinality-based star
    // order and retains its completeness-preserving local strategy choice.
    const parameters = new URLSearchParams({
      bgp: serializeBgpForPlan(patterns),
      speed: '100',
      latency: '1',
    });
    const url = `${this.planUrl}?${parameters.toString()}`;
    let planPromise = smartKgPlusPlanPromises.get(url);
    if (!planPromise) {
      planPromise = this.fetchSmartKgPlusPlanSignatures(url);
      smartKgPlusPlanPromises.set(url, planPromise);
      while (smartKgPlusPlanPromises.size > 128) {
        const oldestUrl = smartKgPlusPlanPromises.keys().next().value;
        if (oldestUrl === undefined) {
          break;
        }
        smartKgPlusPlanPromises.delete(oldestUrl);
      }
    }
    try {
      const controls = await planPromise;
      if (!controls || controls.length !== stars.length) {
        return undefined;
      }

      const bySignature = new Map(stars.map(star => [ starSignature(star), star ]));
      const ordered: IStarExecution[] = [];
      for (const step of controls) {
        const star = bySignature.get(step.signature);
        if (!star || ordered.some(entry => entry.patterns === star)) {
          return undefined;
        }
        ordered.push({ patterns: star, control: step.control });
      }
      return ordered;
    } catch {
      return undefined;
    }
  }

  private async fetchSmartKgPlusPlanSignatures(url: string): Promise<ISmartKgPlusPlanStep[] | undefined> {
    try {
      const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: url });
      if (!response.ok) {
        return undefined;
      }
      const content = await readStreamToString(ActorHttp.toNodeReadable(response.body));
      const controls = flattenSmartKgPlusPlanStars(<unknown> JSON.parse(content));
      return controls.length > 0 ? controls : undefined;
    } catch {
      return undefined;
    }
  }

  private async evaluatePattern(
    pattern: Algebra.Pattern,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const usePartitions = await this.shouldUsePartitions(pattern, context);
    if (usePartitions) {
      const partitionResults = await this.queryPatternsViaPartitions([ pattern ], context, options);
      if (partitionResults.length > 0) {
        return partitionResults;
      }
    }
    return this.collectBindingsFromStream(await this.queryFallbackBindings(pattern, context, options));
  }

  private async evaluateStar(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
    control?: string,
  ): Promise<RDF.Bindings[]> {
    if (this.smartKgPlusSource && control) {
      return this.evaluateSmartKgPlusControlledStar(patterns, control, context, options);
    }
    const metadata = await this.fetchMetadata();
    if (patterns.length >= 2 && isStarEligibleForPartitions(patterns, metadata)) {
      const partitionResults = await this.queryPatternsViaPartitions(patterns, context, options);
      if (partitionResults.length > 0) {
        return partitionResults;
      }
    }

    return this.queryPatternsViaOriginalSource(patterns, context, options);
  }

  private async evaluateSmartKgPlusControlledStar(
    patterns: Algebra.Pattern[],
    control: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const controlUrl = this.getSafeSmartKgPlusControlUrl(patterns, control);
    const controlPath = new URL(controlUrl).pathname;

    if (controlPath.includes('/molecule/')) {
      const partitionPath = await this.fetchPartitionFileByUrl(controlUrl);
      return this.evaluatePatternsOnPartition(partitionPath, patterns);
    }
    return this.queryPatternsViaSource(patterns, controlUrl, context, options);
  }

  private getSafeSmartKgPlusControlUrl(patterns: Algebra.Pattern[], control: string): string {
    const controlUrl = new URL(control, this.originalSourceUrl).toString();
    const controlPath = new URL(controlUrl).pathname;
    const partitionControl = controlPath.includes('/molecule/') || controlPath.includes('/partition/');
    // Older servers select the first typed family for stars that can span
    // several families. Such a control is incomplete unless rdf:type is fixed.
    return partitionControl && !hasConcreteTypeConstraint(patterns) ? this.originalSourceUrl : controlUrl;
  }

  private async streamSmartKgPlusControlledStar(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    control: string,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const sourceUrl = this.getSafeSmartKgPlusControlUrl(patterns, control);
    const orderedPatterns = await this.sortPatternsByOriginalSourceCardinality(patterns, context);
    let results = inputBindings;
    for (const [ index, pattern ] of orderedPatterns.entries()) {
      const finalPattern = index === orderedPatterns.length - 1;
      if (finalPattern) {
        await this.streamSourcePatternWithInput(pattern, results, sourceUrl, context, options, emit);
        return;
      }
      results = await this.querySourcePatternWithInput(pattern, results, sourceUrl, context, options);
      if (results.length === 0) {
        return;
      }
    }
  }

  private async queryPatternsViaSource(
    patterns: Algebra.Pattern[],
    sourceUrl: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of await this.sortPatternsByOriginalSourceCardinality(patterns, context)) {
      results = await this.querySourcePatternWithInput(pattern, results, sourceUrl, context, options);
      if (results.length === 0) {
        break;
      }
    }
    return deduplicateBindings(results);
  }

  private async querySourcePatternWithInput(
    pattern: Algebra.Pattern,
    inputBindings: RDF.Bindings[],
    sourceUrl: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    // Hypermedia fallback sources may only partially implement joinBindings.
    // Explicitly compatibility-join every bounded response batch: this avoids
    // duplicated broad fragments without materializing the unrestricted
    // pattern, which can exceed low-end client memory.
    const results: RDF.Bindings[] = [];
    for (const batch of chunkBindings(inputBindings, 30)) {
      const stream = await this.queryFallbackBindings(
        pattern,
        context,
        this.withJoinBindings(options, batch),
        sourceUrl,
      );
      const patternBindings = await this.collectBindingsFromStream(stream);
      appendItems(results, await this.joinBindingsLists(batch, patternBindings));
    }
    return deduplicateBindings(results);
  }

  private async streamSourcePatternWithInput(
    pattern: Algebra.Pattern,
    inputBindings: RDF.Bindings[],
    sourceUrl: string,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    for (const batch of chunkBindings(inputBindings, 30)) {
      const stream = await this.queryFallbackBindings(
        pattern,
        context,
        this.withJoinBindings(options, batch),
        sourceUrl,
      );
      const patternBindings = await this.collectBindingsFromStream(stream);
      for (const binding of await this.joinBindingsLists(batch, patternBindings)) {
        await emit(binding);
      }
    }
  }

  private withJoinBindings(
    options: IQueryBindingsOptions | undefined,
    bindings: RDF.Bindings[],
  ): IQueryBindingsOptions {
    const variables = getBindingsVariables(bindings);
    if (variables.length === 0) {
      return { ...options };
    }
    return {
      ...options,
      joinBindings: {
        bindings: <BindingsStream><unknown> new ArrayBindingsIterator(Promise.resolve(bindings), variables),
        metadata: {
          state: createMetadataValidationState(),
          cardinality: { type: 'exact', value: bindings.length },
          variables,
          next: [],
        },
      },
    };
  }

  private async queryPatternsViaOriginalSource(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    // Typed planning is beneficial only when rdf:type is concretely bound. For
    // an unbound type it merely adds cardinality requests without reducing the
    // typed-family search space, so retain the original streaming order.
    const orderedPatterns = this.smartKgPlusSource && hasConcreteTypeConstraint(patterns) ?
      await this.sortPatternsByOriginalSourceCardinality(patterns, context) :
      patterns;
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of orderedPatterns) {
      const stream = await this.queryFallbackBindings(pattern, context, options);
      const bindings = await this.collectBindingsFromStream(stream);
      results = await this.joinBindingsLists(results, bindings);
    }
    return deduplicateBindings(results);
  }

  private async sortPatternsByOriginalSourceCardinality(
    patterns: Algebra.Pattern[],
    context: IActionContext,
  ): Promise<Algebra.Pattern[]> {
    const estimated = await Promise.all(patterns.map(async pattern => ({
      pattern,
      cardinality: await this.estimateOriginalSourcePatternCardinality(pattern, context),
    })));
    return estimated
      .sort((left, right) => left.cardinality - right.cardinality)
      .map(entry => entry.pattern);
  }

  private async estimateOriginalSourcePatternCardinality(
    pattern: Algebra.Pattern,
    _context: IActionContext,
  ): Promise<number> {
    const key = patternToKey(pattern);
    const cached = this.patternCardinalityCache.get(key);
    if (cached !== undefined) {
      return cached;
    }

    const url = this.buildOriginalSourcePatternUrl(pattern);
    try {
      const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: url });
      if (!response.ok) {
        return Number.MAX_SAFE_INTEGER;
      }
      const body = ActorHttp.toNodeReadable(response.body);
      const content = await readStreamToString(body);
      const cardinality = parseTpfCardinality(content) ?? Number.MAX_SAFE_INTEGER;
      this.patternCardinalityCache.set(key, cardinality);
      return cardinality;
    } catch {
      return Number.MAX_SAFE_INTEGER;
    }
  }

  private buildOriginalSourcePatternUrl(pattern: Algebra.Pattern): string {
    const parameters = new URLSearchParams();
    this.appendOriginalSourcePatternParameter(parameters, 'subject', pattern.subject);
    this.appendOriginalSourcePatternParameter(parameters, 'predicate', pattern.predicate);
    this.appendOriginalSourcePatternParameter(parameters, 'object', pattern.object);
    const queryString = parameters.toString();
    return queryString.length > 0 ? `${this.originalSourceUrl}?${queryString}` : this.originalSourceUrl;
  }

  private appendOriginalSourcePatternParameter(parameters: URLSearchParams, key: string, term: RDF.Term): void {
    const value = this.termToOriginalSourceParameter(term);
    if (value !== undefined) {
      parameters.append(key, value);
    }
  }

  private termToOriginalSourceParameter(term: RDF.Term): string | undefined {
    if (term.termType === 'Variable') {
      return undefined;
    }
    if (term.termType === 'NamedNode') {
      return term.value;
    }
    return termToString(term);
  }

  private async shouldUsePartitions(pattern: Algebra.Pattern, context: IActionContext): Promise<boolean> {
    if (pattern.predicate.termType === 'Variable') {
      return false;
    }

    const starPatternCount = getStarPatternCount(<any> context, pattern);
    return starPatternCount >= 2 && getShippingStrategyHint(<any> context, pattern) !== 'TP-S';
  }

  private async queryPatternsViaPartitions(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const metadata = await this.fetchMetadata();
    const sets = metadataToSets(metadata);
    if (hasInfrequentPredicate(patterns, sets.blockedPredicates)) {
      return [];
    }

    const candidateFamilies = this.smartKgPlusSource ?
      selectCompatibleTypedStarFamilies(patterns, metadata) :
      selectCompatibleStarFamilies(patterns, metadata);
    if (candidateFamilies.length === 0) {
      return this.queryPatternsViaAllPartitionFamilies(patterns, metadata);
    }

    const scopedFamilies = selectStarScopedMaterializedFamilies(patterns, candidateFamilies, metadata);
    if (scopedFamilies.completeFamilies.length === 0 && scopedFamilies.partialFamilies.length === 0) {
      return this.queryPatternsViaAllPartitionFamilies(patterns, metadata);
    }

    const orderedPatterns = sortPatternsByEstimatedPartitionSize(patterns, metadata);
    const completeResults = await this.queryPatternsOnMaterializedFamilies(
      orderedPatterns,
      scopedFamilies.completeFamilies,
    );
    const results = await this.queryPatternsViaPartitionFamilies(
      orderedPatterns,
      metadata,
      scopedFamilies.partialFamilies,
    );
    const hybridResults = results.length === 0 ?
      await this.queryPatternsViaPartialFamilies(
        orderedPatterns,
        metadata,
        scopedFamilies.partialFamilies,
        context,
        options,
      ) :
        [];
    const scopedResults = deduplicateBindings([ ...completeResults, ...results, ...hybridResults ]);
    if (!this.smartKgPlusSource) {
      return scopedResults;
    }

    return scopedResults;
  }

  private async queryPatternsViaAllPartitionFamilies(
    patterns: Algebra.Pattern[],
    metadata: ISmartKgMetadata,
  ): Promise<RDF.Bindings[]> {
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of sortPatternsByEstimatedPartitionSize(patterns, metadata)) {
      const patternResults = await this.queryPatternViaPartitions(pattern, metadata);
      results = deduplicateBindings(await this.joinBindingsLists(results, patternResults));
      if (results.length === 0) {
        break;
      }
    }
    return deduplicateBindings(results);
  }

  private async queryPatternsViaPartitionFamilies(
    patterns: Algebra.Pattern[],
    metadata: ISmartKgMetadata,
    candidateFamilies: ISmartKgFamily[],
  ): Promise<RDF.Bindings[]> {
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of sortPatternsByEstimatedPartitionSize(patterns, metadata)) {
      const patternResults = await this.queryPatternViaPartitions(pattern, metadata, candidateFamilies);
      results = deduplicateBindings(await this.joinBindingsLists(results, patternResults));
      if (results.length === 0) {
        break;
      }
    }

    return deduplicateBindings(results);
  }

  private async queryPatternsOnMaterializedFamilies(
    patterns: Algebra.Pattern[],
    families: ISmartKgFamily[],
  ): Promise<RDF.Bindings[]> {
    const allResults: RDF.Bindings[] = [];
    const seen = new Set<string>();
    for (const family of families) {
      if (family.numTriples === 0 || !family.name) {
        continue;
      }
      const partitionPath = await this.fetchPartitionFile(family);
      const partitionResults = await this.evaluatePatternsOnPartition(partitionPath, patterns);
      appendUniqueBindings(allResults, seen, partitionResults);
    }

    return allResults;
  }

  private async queryPatternsViaPartialFamilies(
    patterns: Algebra.Pattern[],
    metadata: ISmartKgMetadata,
    candidateFamilies: ISmartKgFamily[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const starPredicates = extractPredicates(patterns);
    const partialFamilies = candidateFamilies.filter(family => !predicatesAreContainedInFamily(starPredicates, family));
    const allResults: RDF.Bindings[] = [];
    const seen = new Set<string>();
    const fallbackCache = new Map<string, RDF.Bindings[]>();

    for (const family of partialFamilies) {
      const familyPredicates = new Set(family.predicateSet);
      const partitionPatterns = patterns.filter(pattern => isPatternPredicateContainedInSet(pattern, familyPredicates));
      const fallbackPatterns = patterns.filter(pattern => !isPatternPredicateContainedInSet(pattern, familyPredicates));
      if (partitionPatterns.length === 0 || fallbackPatterns.length === 0) {
        continue;
      }

      const materializedFamilies = selectOptimalFamilies([ family ], metadata, {
        completeCoverage: true,
        maxFamilies: Number.POSITIVE_INFINITY,
        preferGrouped: false,
      });
      const partitionResults = await this.queryPatternsViaPartitionFamilies(
        partitionPatterns,
        metadata,
        materializedFamilies,
      );
      if (partitionResults.length === 0) {
        continue;
      }

      let results = partitionResults;
      for (const pattern of fallbackPatterns) {
        const bindings = await this.getFallbackPatternBindings(pattern, context, options, fallbackCache);
        results = deduplicateBindings(await this.joinBindingsLists(results, bindings));
        if (results.length === 0) {
          break;
        }
      }
      appendUniqueBindings(allResults, seen, results);
    }

    return allResults;
  }

  private async getFallbackPatternBindings(
    pattern: Algebra.Pattern,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    cache: Map<string, RDF.Bindings[]>,
  ): Promise<RDF.Bindings[]> {
    const key = patternToKey(pattern);
    const cached = cache.get(key);
    if (cached) {
      return cached;
    }
    const stream = await this.queryFallbackBindings(pattern, context, options);
    const bindings = await this.collectBindingsFromStream(stream);
    cache.set(key, bindings);
    return bindings;
  }

  private async queryPatternViaPartitions(
    pattern: Algebra.Pattern,
    metadata: ISmartKgMetadata,
    candidateFamilies?: ISmartKgFamily[],
  ): Promise<RDF.Bindings[]> {
    if (pattern.predicate.termType === 'Variable') {
      return [];
    }

    const predicates = extractPredicates([ pattern ]);
    const matchingFamilies = candidateFamilies ?
      candidateFamilies.filter(family => predicatesAreContainedInFamily(predicates, family)) :
      findMatchingFamilies(predicates, metadata.families);
    const optimalFamilies = selectOptimalFamilies(matchingFamilies, metadata, {
      completeCoverage: true,
      maxFamilies: Number.POSITIVE_INFINITY,
      preferGrouped: false,
    });
    if (optimalFamilies.length === 0) {
      return [];
    }

    const allResults: RDF.Bindings[] = [];
    const seen = new Set<string>();
    for (const family of optimalFamilies) {
      if (family.numTriples === 0 || !family.name) {
        continue;
      }
      const partitionPath = await this.fetchPartitionFile(family);
      const partitionResults = await this.evaluatePatternsOnPartition(partitionPath, [ pattern ]);
      appendUniqueBindings(allResults, seen, partitionResults);
    }

    return allResults;
  }

  private async evaluatePatternsOnPartition(
    partitionPath: string,
    patterns: Algebra.Pattern[],
  ): Promise<RDF.Bindings[]> {
    const document = await this.hdtCache.getDocument(partitionPath);
    if (patterns.length === 0) {
      return [ await this.emptyBinding() ];
    }
    if (patterns.length === 1) {
      return this.collectCachedHdtBindings(partitionPath, document, patterns[0]);
    }

    // This follows the same cardinality-first, bounded pipeline principles as
    // Comunica's TPF/brTPF bind joins. Only the smallest HDT pattern is
    // materialized. Every later pattern is read page-by-page and immediately
    // hash-joined, avoiding a second full-pattern array and repeated full-array
    // deduplication at each star step.
    const orderedPatterns = await this.orderPartitionPatternsByCardinality(document, patterns);
    let results = await this.collectHdtBindings(document, orderedPatterns[0]);
    for (const pattern of orderedPatterns.slice(1)) {
      results = await this.joinBindingsWithHdtPattern(results, document, pattern);
      if (results.length === 0) {
        return [];
      }
    }
    return results;
  }

  private async orderPartitionPatternsByCardinality(
    document: HdtDocument,
    patterns: Algebra.Pattern[],
  ): Promise<Algebra.Pattern[]> {
    const estimates = await Promise.all(patterns.map(async(pattern, position) => {
      try {
        const count = await document.countTriples(pattern.subject, pattern.predicate, pattern.object);
        return { pattern, position, cardinality: count.totalCount };
      } catch {
        return { pattern, position, cardinality: Number.POSITIVE_INFINITY };
      }
    }));
    return estimates
      .sort((left, right) => left.cardinality - right.cardinality || left.position - right.position)
      .map(entry => entry.pattern);
  }

  private async joinBindingsWithHdtPattern(
    leftBindings: RDF.Bindings[],
    document: HdtDocument,
    pattern: Algebra.Pattern,
  ): Promise<RDF.Bindings[]> {
    if (leftBindings.length === 0) {
      return [];
    }

    const bindingsFactory = await this.getBindingsFactory();
    const leftRecords = leftBindings.map(bindingToRecord);
    const sharedVariables = getPatternVariableNames(pattern)
      .filter(variable => leftRecords.every(record => Boolean(record[variable])));
    const index = new Map<string, Record<string, RDF.Term>[]>();
    if (sharedVariables.length > 0) {
      for (const record of leftRecords) {
        const key = joinKey(record, sharedVariables);
        const bucket = index.get(key);
        if (bucket) {
          bucket.push(record);
        } else {
          index.set(key, [ record ]);
        }
      }
    }

    const results: RDF.Bindings[] = [];
    const seen = new Set<string>();
    await this.forEachHdtBindingsPage(document, pattern, (rightBindings) => {
      for (const rightBinding of rightBindings) {
        const rightRecord = bindingToRecord(rightBinding);
        const candidates = sharedVariables.length > 0 ?
          index.get(joinKey(rightRecord, sharedVariables)) ?? [] :
          leftRecords;
        for (const leftRecord of candidates) {
          if (!areBindingRecordsCompatible(leftRecord, rightRecord)) {
            continue;
          }
          const binding = bindingsFactory.fromRecord({ ...leftRecord, ...rightRecord });
          const key = bindingKey(binding);
          if (!seen.has(key)) {
            seen.add(key);
            results.push(binding);
          }
        }
      }
    });
    return results;
  }

  private collectCachedHdtBindings(
    partitionPath: string,
    document: HdtDocument,
    pattern: Algebra.Pattern,
  ): Promise<RDF.Bindings[]> {
    const cacheKey = `${partitionPath}|${patternToKey(pattern)}`;
    const cached = this.hdtPatternCache.get(cacheKey);
    if (cached) {
      return cached;
    }
    const promise = this.collectHdtBindings(document, pattern);
    this.hdtPatternCache.set(cacheKey, promise);
    // A cache of large binding arrays defeats HDT's compact representation and
    // was the principal source of multi-gigabyte retention on 50M datasets.
    // Keep only small reusable lookups; large scans remain page-bounded and are
    // released as soon as their join stage completes.
    promise.then((bindings) => {
      if (bindings.length > 4_096 && this.hdtPatternCache.get(cacheKey) === promise) {
        this.hdtPatternCache.delete(cacheKey);
      }
    }).catch(() => {
      if (this.hdtPatternCache.get(cacheKey) === promise) {
        this.hdtPatternCache.delete(cacheKey);
      }
    });
    while (this.hdtPatternCache.size > 4) {
      const oldestKey = this.hdtPatternCache.keys().next().value;
      if (oldestKey === undefined) {
        break;
      }
      this.hdtPatternCache.delete(oldestKey);
    }
    return promise;
  }

  private async collectHdtBindings(document: HdtDocument, pattern: Algebra.Pattern): Promise<RDF.Bindings[]> {
    const results: RDF.Bindings[] = [];
    await this.forEachHdtBindingsPage(document, pattern, bindings => appendItems(results, bindings));
    return deduplicateBindings(results);
  }

  private async forEachHdtBindingsPage(
    document: HdtDocument,
    pattern: Algebra.Pattern,
    consume: (bindings: RDF.Bindings[]) => void,
  ): Promise<void> {
    const bindingsFactory = await this.getBindingsFactory();
    // Smaller than the old 16K page: this caps transient native/JS objects and
    // yields often enough for low-end clients without turning HDT access into
    // thousands of tiny calls.
    const pageSize = 4_096;
    let offset = 0;
    while (true) {
      const result = await document.searchBindings(
        bindingsFactory,
        pattern.subject,
        pattern.predicate,
        pattern.object,
        { offset, limit: pageSize },
      );
      const bindings: RDF.Bindings[] = Array.isArray(result?.bindings) ? result.bindings : [];
      consume(bindings);
      if (bindings.length < pageSize) {
        return;
      }
      offset += bindings.length;
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  private async queryFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
    sourceUrl: string = this.originalSourceUrl,
  ): Promise<BindingsStream> {
    const variables = getOperationVariables(operation);
    return <BindingsStream><unknown>
      new ArrayBindingsIterator(this.collectFallbackBindings(operation, context, options, sourceUrl), variables);
  }

  private async collectFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
    sourceUrl: string = this.originalSourceUrl,
  ): Promise<RDF.Bindings[]> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      return [];
    }

    const fallbackContext = setContextFlag(context, 'smartkgFallback', true);
    const handledDatasets: Record<string, boolean> = {};
    const queuedLinks: string[] = [ sourceUrl ];
    const seenLinks = new Set<string>();
    const results: RDF.Bindings[] = [];

    while (queuedLinks.length > 0) {
      const nextUrl = queuedLinks.shift()!;
      if (seenLinks.has(nextUrl)) {
        continue;
      }
      seenLinks.add(nextUrl);

      const sourceResult = await this.mediatorQuerySourceDereferenceLink.mediate({
        context: fallbackContext,
        link: { url: nextUrl },
        handledDatasets,
      });
      const source = sourceResult?.source;
      if (!source || typeof source.queryBindings !== 'function') {
        continue;
      }

      const stream = source.queryBindings(operation, fallbackContext, options);
      const { bindings, metadata } = await this.collectBindingsAndMetadata(stream);
      appendItems(results, bindings);

      const streamNextLinks = getMetadataNextLinks(metadata);
      const nextLinks = streamNextLinks.length > 0 ? streamNextLinks : getMetadataNextLinks(sourceResult.metadata);
      for (const link of nextLinks) {
        if (!seenLinks.has(link) && !isDatasetPageLink(link, sourceUrl)) {
          queuedLinks.push(link);
        }
      }
    }

    return deduplicateBindings(results);
  }

  private async streamJoinedFallbackBindings(
    leftBindings: RDF.Bindings[],
    operation: Algebra.Operation,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      return;
    }

    const fallbackContext = setContextFlag(context, 'smartkgFallback', true);
    const handledDatasets: Record<string, boolean> = {};
    const queuedLinks = [ this.originalSourceUrl ];
    const seenLinks = new Set<string>();
    const bindingsFactory = await this.getBindingsFactory();
    const leftRecords = leftBindings.map(bindingToRecord);
    const sharedVariables = getOperationVariables(operation)
      .map(({ variable }) => variable.value)
      .filter(variable => leftRecords.every(record => Boolean(record[variable])));
    const leftIndex = new Map<string, Record<string, RDF.Term>[]>();
    if (sharedVariables.length > 0) {
      for (const leftRecord of leftRecords) {
        const key = joinKey(leftRecord, sharedVariables);
        const bucket = leftIndex.get(key) ?? [];
        bucket.push(leftRecord);
        leftIndex.set(key, bucket);
      }
    }

    while (queuedLinks.length > 0) {
      const nextUrl = queuedLinks.shift()!;
      if (seenLinks.has(nextUrl)) {
        continue;
      }
      seenLinks.add(nextUrl);

      const sourceResult = await this.mediatorQuerySourceDereferenceLink.mediate({
        context: fallbackContext,
        link: { url: nextUrl },
        handledDatasets,
      });
      const source = sourceResult?.source;
      if (!source || typeof source.queryBindings !== 'function') {
        continue;
      }

      const stream = source.queryBindings(operation, fallbackContext, options);
      let metadata: MetadataBindings | undefined;
      let resolveMetadata: (metadata?: MetadataBindings) => void = () => {};
      const metadataPromise = new Promise<MetadataBindings | undefined>((resolve) => {
        resolveMetadata = resolve;
      });
      if (typeof stream.getProperty === 'function') {
        stream.getProperty('metadata', (value: MetadataBindings) => {
          metadata = value;
          resolveMetadata(value);
        });
      } else {
        resolveMetadata();
      }
      for await (const rightBinding of stream) {
        const rightRecord = bindingToRecord(rightBinding);
        const candidates = sharedVariables.length > 0 &&
          sharedVariables.every(variable => Boolean(rightRecord[variable])) ?
          leftIndex.get(joinKey(rightRecord, sharedVariables)) ?? [] :
          leftRecords;
        for (const leftRecord of candidates) {
          if (areBindingRecordsCompatible(leftRecord, rightRecord)) {
            const binding = bindingsFactory.fromRecord({ ...leftRecord, ...rightRecord });
            // One HDT/TPF source exposes disjoint pages, while leftBindings is
            // already deduplicated. Retaining a serialized key for every
            // emitted result therefore consumed memory proportional to the
            // complete answer without removing legitimate duplicates.
            await emit(binding);
          }
        }
      }
      if (!metadata) {
        await Promise.race([
          metadataPromise,
          new Promise(resolve => setImmediate(resolve)),
        ]);
      }

      const streamNextLinks = getMetadataNextLinks(metadata);
      const nextLinks = streamNextLinks.length > 0 ? streamNextLinks : getMetadataNextLinks(sourceResult.metadata);
      for (const link of nextLinks) {
        if (!seenLinks.has(link) && !isDatasetPageLink(link, this.originalSourceUrl)) {
          queuedLinks.push(link);
        }
      }
    }
  }

  private async fetchMetadata(): Promise<ISmartKgMetadata> {
    if (!this.metadata) {
      let metadataPromise = smartKgMetadataPromises.get(this.metadataUrl);
      if (!metadataPromise) {
        metadataPromise = this.fetchText(this.metadataUrl).then(parseSmartKgMetadata);
        smartKgMetadataPromises.set(this.metadataUrl, metadataPromise);
        metadataPromise.catch(() => {
          // Do not retain a rejected network request globally: a later query
          // may safely retry after a transient server or connection failure.
          if (smartKgMetadataPromises.get(this.metadataUrl) === metadataPromise) {
            smartKgMetadataPromises.delete(this.metadataUrl);
          }
        });
      }
      this.metadata = await metadataPromise;
    }
    return this.metadata;
  }

  private async fetchPartitionFile(family: ISmartKgFamily): Promise<string> {
    if (isAbsolute(family.name) && isHdtFile(family.name)) {
      return family.name;
    }

    const partitionName = basename(family.name);
    if (!partitionName || partitionName === '.') {
      throw new Error(`SmartKG family has no valid partition filename: ${family.name}`);
    }
    const partitionUrl = `${this.partitionsBaseUrl}/${encodeURIComponent(partitionName)}`;
    const partitionPath = join(this.cacheFolder, encodeURIComponent(partitionUrl));
    if (isHdtFile(partitionPath)) {
      return partitionPath;
    }

    const existingDownload = partitionDownloadPromises.get(partitionPath);
    if (existingDownload) {
      return existingDownload;
    }

    const download = this.downloadPartitionFile(partitionUrl, partitionPath);
    partitionDownloadPromises.set(partitionPath, download);
    try {
      return await download;
    } finally {
      if (partitionDownloadPromises.get(partitionPath) === download) {
        partitionDownloadPromises.delete(partitionPath);
      }
    }
  }

  private async fetchPartitionFileByUrl(partitionUrl: string): Promise<string> {
    const partitionPath = join(this.cacheFolder, encodeURIComponent(partitionUrl));
    if (isHdtFile(partitionPath)) {
      return partitionPath;
    }
    const existingDownload = partitionDownloadPromises.get(partitionPath);
    if (existingDownload) {
      return existingDownload;
    }
    const download = this.downloadPartitionFile(partitionUrl, partitionPath);
    partitionDownloadPromises.set(partitionPath, download);
    try {
      return await download;
    } finally {
      if (partitionDownloadPromises.get(partitionPath) === download) {
        partitionDownloadPromises.delete(partitionPath);
      }
    }
  }

  private async downloadPartitionFile(partitionUrl: string, partitionPath: string): Promise<string> {
    const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: partitionUrl });
    if (!response.ok) {
      throw new Error(`SmartKG partition download failed with HTTP ${response.status}: ${partitionUrl}`);
    }
    const temporaryPath = `${partitionPath}.download-${process.pid}-${Date.now()}`;
    await pipeline(ActorHttp.toNodeReadable(response.body), createWriteStream(temporaryPath));
    try {
      if (isHdtFile(temporaryPath)) {
        renameSync(temporaryPath, partitionPath);
      } else {
        await extractTarPartitionFile(temporaryPath, partitionPath);
        unlinkSync(temporaryPath);
      }
    } catch (error) {
      if (existsSync(temporaryPath)) {
        unlinkSync(temporaryPath);
      }
      throw error;
    }
    if (!existsSync(`${partitionPath}.index.v1-1`)) {
      const indexResponse = await this.mediatorHttp.mediate({
        context: this.defaultContext,
        input: `${partitionUrl}.index.v1-1`,
      });
      if (indexResponse.ok) {
        const temporaryIndexPath = `${partitionPath}.index.download-${process.pid}-${Date.now()}`;
        await pipeline(ActorHttp.toNodeReadable(indexResponse.body), createWriteStream(temporaryIndexPath));
        renameSync(temporaryIndexPath, `${partitionPath}.index.v1-1`);
      }
    }
    return partitionPath;
  }

  private async fetchText(url: string): Promise<string> {
    const cachePath = join(this.cacheFolder, encodeURIComponent(`${url}.json`));
    if (existsSync(cachePath)) {
      return readFileUtf8(cachePath);
    }

    const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: url });
    if (!response.ok) {
      throw new Error(`SmartKG metadata download failed with HTTP ${response.status}: ${url}`);
    }
    const body = ActorHttp.toNodeReadable(response.body);
    const content = await readStreamToString(body);
    await new Promise<void>((resolve, reject) => {
      const output = createWriteStream(cachePath, { encoding: 'utf8' });
      output.write(content, (error) => {
        if (error) {
          reject(error);
          return;
        }
        output.end();
      });
      output.on('finish', () => resolve());
      output.on('error', reject);
    });
    return content;
  }

  private async getBindingsFactory(): Promise<BindingsFactory> {
    if (!this.bindingsFactory) {
      const module = await import('@comunica/utils-bindings-factory');
      this.bindingsFactory = new module.BindingsFactory(this.dataFactory);
    }
    return this.bindingsFactory;
  }

  private async emptyBinding(): Promise<RDF.Bindings> {
    const bindingsFactory = await this.getBindingsFactory();
    return bindingsFactory.bindings();
  }

  private async collectBindingsFromStream(stream: BindingsStream): Promise<RDF.Bindings[]> {
    const bindings: RDF.Bindings[] = [];
    for await (const binding of <AsyncIterable<RDF.Bindings>><any> stream) {
      bindings.push(binding);
    }
    return bindings;
  }

  private async collectBindingsAndMetadata(
    stream: BindingsStream,
  ): Promise<{ bindings: RDF.Bindings[]; metadata?: MetadataBindings }> {
    let metadata: MetadataBindings | undefined;
    let resolveMetadata: (metadata?: MetadataBindings) => void = () => {};
    const metadataPromise = new Promise<MetadataBindings | undefined>((resolve) => {
      resolveMetadata = value => resolve(value);
    });

    if (typeof (<any> stream).getProperty === 'function') {
      (<any> stream).getProperty('metadata', (value: MetadataBindings) => {
        metadata = value;
        resolveMetadata(value);
      });
    } else {
      resolveMetadata();
    }

    const bindings = await this.collectBindingsFromStream(stream);
    if (!metadata) {
      await Promise.race([
        metadataPromise,
        new Promise(resolve => setImmediate(resolve)),
      ]);
    }

    return { bindings, metadata };
  }

  private async joinBindingsLists(left: RDF.Bindings[], right: RDF.Bindings[]): Promise<RDF.Bindings[]> {
    const bindingsFactory = await this.getBindingsFactory();
    return joinBindingRecords(left.map(bindingToRecord), right.map(bindingToRecord), bindingsFactory);
  }

  private async emitJoinedBindings(
    left: RDF.Bindings[],
    right: RDF.Bindings[],
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const bindingsFactory = await this.getBindingsFactory();
    const seen = new Set<string>();
    for (const binding of joinBindingRecordsIterator(
      left.map(bindingToRecord),
      right.map(bindingToRecord),
      bindingsFactory,
    )) {
      const key = bindingKey(binding);
      if (!seen.has(key)) {
        seen.add(key);
        await emit(binding);
      }
    }
  }
}

function normalizeUrl(url: string): string {
  return url.replace(/\/$/u, '');
}

function setContextFlag(context: IActionContext, key: string, value: boolean): IActionContext {
  if (typeof (<any> context).setRaw === 'function') {
    return (<any> context).setRaw(key, value);
  }
  return context;
}

function getOperationVariables(operation: Algebra.Operation): MetadataBindings['variables'] {
  const seen = new Set<string>();
  const variables: MetadataBindings['variables'] = [];
  const pushVariable = (variable: RDF.Variable): void => {
    if (seen.has(variable.value)) {
      return;
    }
    seen.add(variable.value);
    variables.push({ variable, canBeUndef: false });
  };

  if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
    const pattern = operation;
    if (pattern.subject.termType === 'Variable') {
      pushVariable(pattern.subject);
    }
    if (pattern.predicate.termType === 'Variable') {
      pushVariable(pattern.predicate);
    }
    if (pattern.object.termType === 'Variable') {
      pushVariable(pattern.object);
    }
    return variables;
  }

  if (isKnownOperation(operation, Algebra.Types.BGP)) {
    for (const pattern of (operation).patterns) {
      if (pattern.subject.termType === 'Variable') {
        pushVariable(pattern.subject);
      }
      if (pattern.predicate.termType === 'Variable') {
        pushVariable(pattern.predicate);
      }
      if (pattern.object.termType === 'Variable') {
        pushVariable(pattern.object);
      }
    }
  }

  for (const pattern of collectPatterns(operation)) {
    if (pattern.subject.termType === 'Variable') {
      pushVariable(pattern.subject);
    }
    if (pattern.predicate.termType === 'Variable') {
      pushVariable(pattern.predicate);
    }
    if (pattern.object.termType === 'Variable') {
      pushVariable(pattern.object);
    }
  }

  return variables;
}

function getBindingsVariables(bindings: RDF.Bindings[]): MetadataBindings['variables'] {
  const variables = new Map<string, RDF.Variable>();
  for (const binding of bindings) {
    for (const [ variable ] of binding) {
      variables.set(variable.value, variable);
    }
  }
  return [ ...variables.values() ].map(variable => ({ variable, canBeUndef: false }));
}

function* chunkBindings(bindings: RDF.Bindings[], size: number): IterableIterator<RDF.Bindings[]> {
  for (let offset = 0; offset < bindings.length; offset += size) {
    yield bindings.slice(offset, offset + size);
  }
}

function collectPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
  const patterns: Algebra.Pattern[] = [];
  collectPatternsRecursive(operation, patterns, new Set<Algebra.Pattern>());
  return patterns;
}

function collectPatternsRecursive(
  operation: Algebra.Operation,
  patterns: Algebra.Pattern[],
  seen: Set<Algebra.Pattern>,
): void {
  if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
    if (!seen.has(operation)) {
      seen.add(operation);
      patterns.push(operation);
    }
    return;
  }

  const operationLike = <Algebra.Operation & {
    input?: Algebra.Operation | Algebra.Operation[];
    patterns?: Algebra.Pattern[];
  }> operation;
  if (Array.isArray(operationLike.input)) {
    for (const input of operationLike.input) {
      collectPatternsRecursive(input, patterns, seen);
    }
  } else if (operationLike.input) {
    collectPatternsRecursive(operationLike.input, patterns, seen);
  }
  if (Array.isArray(operationLike.patterns)) {
    for (const pattern of operationLike.patterns) {
      collectPatternsRecursive(pattern, patterns, seen);
    }
  }
}

function bindingToRecord(binding: RDF.Bindings): Record<string, RDF.Term> {
  const record: Record<string, RDF.Term> = {};
  for (const [ variable, value ] of binding) {
    record[variable.value] = value;
  }
  return record;
}

function areBindingRecordsCompatible(left: Record<string, RDF.Term>, right: Record<string, RDF.Term>): boolean {
  for (const [ variable, value ] of Object.entries(right)) {
    if (left[variable] && !left[variable].equals(value)) {
      return false;
    }
  }
  return true;
}

function joinBindingRecords(
  leftRecords: Record<string, RDF.Term>[],
  rightRecords: Record<string, RDF.Term>[],
  bindingsFactory: BindingsFactory,
): RDF.Bindings[] {
  return [ ...joinBindingRecordsIterator(leftRecords, rightRecords, bindingsFactory) ];
}

function* joinBindingRecordsIterator(
  leftRecords: Record<string, RDF.Term>[],
  rightRecords: Record<string, RDF.Term>[],
  bindingsFactory: BindingsFactory,
): IterableIterator<RDF.Bindings> {
  if (leftRecords.length === 0 || rightRecords.length === 0) {
    return;
  }

  const sharedVariables = getSharedBoundVariables(leftRecords, rightRecords);
  if (sharedVariables.length === 0) {
    yield* nestedJoinBindingRecordsIterator(leftRecords, rightRecords, bindingsFactory);
    return;
  }

  const indexedIsLeft = leftRecords.length <= rightRecords.length;
  const indexed = indexedIsLeft ? leftRecords : rightRecords;
  const probes = indexedIsLeft ? rightRecords : leftRecords;
  const index = new Map<string, Record<string, RDF.Term>[]>();

  for (const record of indexed) {
    const key = joinKey(record, sharedVariables);
    const bucket = index.get(key);
    if (bucket) {
      bucket.push(record);
    } else {
      index.set(key, [ record ]);
    }
  }

  for (const probe of probes) {
    const candidates = index.get(joinKey(probe, sharedVariables));
    if (!candidates) {
      continue;
    }
    for (const candidate of candidates) {
      const leftMap = indexedIsLeft ? candidate : probe;
      const rightMap = indexedIsLeft ? probe : candidate;
      if (areBindingRecordsCompatible(leftMap, rightMap)) {
        yield bindingsFactory.fromRecord({ ...leftMap, ...rightMap });
      }
    }
  }
}

function* nestedJoinBindingRecordsIterator(
  leftRecords: Record<string, RDF.Term>[],
  rightRecords: Record<string, RDF.Term>[],
  bindingsFactory: BindingsFactory,
): IterableIterator<RDF.Bindings> {
  for (const leftMap of leftRecords) {
    for (const rightMap of rightRecords) {
      if (areBindingRecordsCompatible(leftMap, rightMap)) {
        yield bindingsFactory.fromRecord({ ...leftMap, ...rightMap });
      }
    }
  }
}

function getSharedBoundVariables(
  leftRecords: Record<string, RDF.Term>[],
  rightRecords: Record<string, RDF.Term>[],
): string[] {
  const leftVariables = collectRecordVariables(leftRecords);
  const rightVariables = collectRecordVariables(rightRecords);
  const shared = [ ...leftVariables ].filter(variable => rightVariables.has(variable));
  return shared.every(variable =>
    leftRecords.every(record => Boolean(record[variable])) &&
    rightRecords.every(record => Boolean(record[variable]))) ?
    shared :
      [];
}

function collectRecordVariables(records: Record<string, RDF.Term>[]): Set<string> {
  const variables = new Set<string>();
  for (const record of records) {
    for (const variable of Object.keys(record)) {
      variables.add(variable);
    }
  }
  return variables;
}

function getPatternVariableNames(pattern: Algebra.Pattern): string[] {
  const variables = new Set<string>();
  if (pattern.subject.termType === 'Variable') {
    variables.add(pattern.subject.value);
  }
  if (pattern.predicate.termType === 'Variable') {
    variables.add(pattern.predicate.value);
  }
  if (pattern.object.termType === 'Variable') {
    variables.add(pattern.object.value);
  }
  return [ ...variables ];
}

function joinKey(record: Record<string, RDF.Term>, variables: string[]): string {
  return variables.map(variable => termToString(record[variable])).join('\u001F');
}

function appendItems<T>(target: T[], items: Iterable<T>): void {
  for (const item of items) {
    target.push(item);
  }
}

function appendUniqueBindings(target: RDF.Bindings[], seen: Set<string>, bindings: Iterable<RDF.Bindings>): void {
  for (const binding of bindings) {
    const key = bindingKey(binding);
    if (!seen.has(key)) {
      seen.add(key);
      target.push(binding);
    }
  }
}

function deduplicateBindings(bindings: RDF.Bindings[]): RDF.Bindings[] {
  const seen = new Set<string>();
  const results: RDF.Bindings[] = [];
  for (const binding of bindings) {
    const key = [ ...binding ]
      .map(([ variable, value ]) => `${variable.value}=${termToString(value)}`)
      .sort()
      .join('|');
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    results.push(binding);
  }
  return results;
}

function bindingKey(binding: RDF.Bindings): string {
  return [ ...binding ]
    .map(([ variable, value ]) => `${variable.value}=${termToString(value)}`)
    .sort()
    .join('|');
}

function groupPatternsBySubject(patterns: Algebra.Pattern[]): Algebra.Pattern[][] {
  const stars = new Map<string, Algebra.Pattern[]>();
  for (const pattern of patterns) {
    const subject = termToString(pattern.subject);
    const bucket = stars.get(subject) ?? [];
    bucket.push(pattern);
    stars.set(subject, bucket);
  }
  return [ ...stars.values() ];
}

function orderConnectedStarsByCardinality(
  estimated: { star: Algebra.Pattern[]; cardinality: number }[],
): Algebra.Pattern[][] {
  const remaining = [ ...estimated ].sort((left, right) => left.cardinality - right.cardinality);
  const first = remaining.shift();
  if (!first) {
    return [];
  }

  const ordered = [ first.star ];
  const boundVariables = getStarVariableNames(first.star);
  while (remaining.length > 0) {
    const connectedIndex = remaining.findIndex(entry =>
      [ ...getStarVariableNames(entry.star) ].some(variable => boundVariables.has(variable)));
    const [ next ] = remaining.splice(connectedIndex < 0 ? 0 : connectedIndex, 1);
    ordered.push(next.star);
    for (const variable of getStarVariableNames(next.star)) {
      boundVariables.add(variable);
    }
  }
  return ordered;
}

function getStarVariableNames(patterns: Algebra.Pattern[]): Set<string> {
  const variables = new Set<string>();
  for (const pattern of patterns) {
    for (const variable of getPatternVariableNames(pattern)) {
      variables.add(variable);
    }
  }
  return variables;
}

function serializeBgpForPlan(patterns: Algebra.Pattern[]): string {
  return patterns
    .map(pattern => `${serializeTermForPlan(pattern.subject)} ${serializeTermForPlan(pattern.predicate)} ` +
      `${serializeTermForPlan(pattern.object)} .`)
    .join('\n');
}

function serializeTermForPlan(term: RDF.Term): string {
  if (term.termType === 'Variable') {
    return `?${term.value}`;
  }
  if (term.termType === 'NamedNode') {
    return `<${term.value}>`;
  }
  return termToString(term);
}

function starSignature(patterns: Algebra.Pattern[]): string {
  return [
    serializeTermForPlan(patterns[0].subject),
    ...patterns
      .map(pattern => `${serializeTermForPlan(pattern.predicate)} ${serializeTermForPlan(pattern.object)}`)
      .sort(),
  ].join('|');
}

function flattenSmartKgPlusPlanStars(plan: unknown): ISmartKgPlusPlanStep[] {
  const steps: ISmartKgPlusPlanStep[] = [];
  const visit = (node: unknown): boolean => {
    if (!node || typeof node !== 'object') {
      return true;
    }
    const record = <Record<string, unknown>> node;
    if (!visit(record.subplan)) {
      return false;
    }
    if (record.operator === undefined) {
      return true;
    }
    if (!record.operator || typeof record.operator !== 'object') {
      return false;
    }
    const operator = <Record<string, unknown>> record.operator;
    const star = operator.star;
    if (typeof operator.control !== 'string') {
      return false;
    }
    if (!star || typeof star !== 'object') {
      return false;
    }
    const starRecord = <Record<string, unknown>> star;
    if (typeof starRecord.subject !== 'string' || !Array.isArray(starRecord.triples)) {
      return false;
    }
    const triples: string[] = [];
    for (const triple of starRecord.triples) {
      if (!triple || typeof triple !== 'object') {
        return false;
      }
      const tripleRecord = <Record<string, unknown>> triple;
      if (typeof tripleRecord.x !== 'string' || typeof tripleRecord.y !== 'string') {
        return false;
      }
      triples.push(`${serializePlanValue(tripleRecord.x, 'predicate')} ` +
        `${serializePlanValue(tripleRecord.y, 'object')}`);
    }
    steps.push({
      signature: [
        serializePlanValue(starRecord.subject, 'subject'),
        ...triples.sort(),
      ].join('|'),
      control: operator.control,
    });
    return true;
  };
  return visit(plan) ? steps : [];
}

function serializePlanValue(value: string, position: 'subject' | 'predicate' | 'object'): string {
  const trimmed = value.trim();
  if (trimmed.startsWith('?') || trimmed.startsWith('_:') ||
    (trimmed.startsWith('<') && trimmed.endsWith('>'))) {
    return trimmed;
  }
  if (position !== 'object' || /^[a-z][a-z\d+.-]*:/iu.test(trimmed)) {
    return `<${trimmed}>`;
  }
  return trimmed;
}

function getMetadataNextLinks(metadata: MetadataBindings | Record<string, unknown> | undefined): string[] {
  const next = (<Record<string, unknown> | undefined> metadata)?.next;
  if (Array.isArray(next)) {
    return next.filter((link): link is string => typeof link === 'string');
  }
  if (typeof next === 'string') {
    return [ next ];
  }
  return [];
}

function isDatasetPageLink(link: string, originalSourceUrl: string): boolean {
  try {
    const nextUrl = new URL(link);
    const sourceUrl = new URL(originalSourceUrl);
    const sameSource = nextUrl.origin === sourceUrl.origin &&
      decodeURIComponent(nextUrl.pathname.replace(/\/$/u, '')) ===
      decodeURIComponent(sourceUrl.pathname.replace(/\/$/u, ''));
    if (!sameSource || !nextUrl.searchParams.has('page')) {
      return false;
    }
    return ![ 'subject', 'predicate', 'object', 'graph' ].some(parameter => nextUrl.searchParams.has(parameter));
  } catch {
    return false;
  }
}

function isStarEligibleForPartitions(patterns: Algebra.Pattern[], metadata: ISmartKgMetadata): boolean {
  if (patterns.length < 2 || patterns.some(pattern => pattern.predicate.termType === 'Variable')) {
    return false;
  }
  const { blockedPredicates } = metadataToSets(metadata);
  if (hasInfrequentPredicate(patterns, blockedPredicates)) {
    return false;
  }

  return patterns.every((pattern) => {
    if (pattern.predicate.termType === 'Variable') {
      return false;
    }
    return findMatchingFamilies(new Set([ termToString(pattern.predicate) ]), metadata.families, blockedPredicates)
      .length > 0;
  });
}

// Typed family partitions only reduce the candidate set when the query fixes a
// class. A variable rdf:type object must keep the regular family strategy.
function hasConcreteTypeConstraint(patterns: Algebra.Pattern[]): boolean {
  return patterns.some(pattern =>
    pattern.predicate.termType === 'NamedNode' &&
    pattern.predicate.value === 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' &&
    pattern.object.termType !== 'Variable');
}

function selectCompatibleStarFamilies(patterns: Algebra.Pattern[], metadata: ISmartKgMetadata): ISmartKgFamily[] {
  const queryPredicates = extractPredicates(patterns);
  if (queryPredicates.size === 0) {
    return [];
  }

  const materializedFamilies = metadata.families.filter((family) => {
    if (!family.predicateSet || family.predicateSet.length === 0) {
      return false;
    }
    return isResolvableFamily(family);
  });

  const matchingSupersets = materializedFamilies.filter((family) => {
    const familyPredicates = new Set(family.predicateSet);
    return isSubset(queryPredicates, familyPredicates);
  });

  const exactMatches = matchingSupersets.filter((family) => {
    const familyPredicates = new Set(family.predicateSet);
    return isSameSet(queryPredicates, familyPredicates);
  });
  const minimalSupersets = exactMatches.length > 0 ? exactMatches : filterMinimalByPredicateSet(matchingSupersets);

  const properSubsets = materializedFamilies.filter((family) => {
    const familyPredicates = new Set(family.predicateSet);
    return isProperSubset(familyPredicates, queryPredicates) && familyPredicates.size > 1;
  });
  const maximalSubsets = filterMaximalByPredicateSet(properSubsets);

  return deduplicateFamilies([ ...minimalSupersets, ...maximalSubsets ]);
}

function selectCompatibleTypedStarFamilies(patterns: Algebra.Pattern[], metadata: ISmartKgMetadata): ISmartKgFamily[] {
  const families = selectCompatibleStarFamilies(patterns, metadata);
  const requiredClasses = patterns
    .filter(pattern => pattern.predicate.termType === 'NamedNode' &&
      pattern.predicate.value === 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' &&
      pattern.object.termType === 'NamedNode')
    .map(pattern => pattern.object.value);
  if (requiredClasses.length === 0) {
    return families;
  }
  return families.filter(family =>
    requiredClasses.every(classIri => family.classesSet?.includes(classIri)));
}

function sortPatternsByEstimatedPartitionSize(
  patterns: Algebra.Pattern[],
  metadata: ISmartKgMetadata,
): Algebra.Pattern[] {
  return [ ...patterns ].sort((left, right) =>
    getPatternEstimatedPartitionSize(left, metadata) - getPatternEstimatedPartitionSize(right, metadata));
}

function getPatternEstimatedPartitionSize(pattern: Algebra.Pattern, metadata: ISmartKgMetadata): number {
  if (pattern.predicate.termType === 'Variable') {
    return Number.POSITIVE_INFINITY;
  }

  const predicate = termToString(pattern.predicate);
  let totalTriples = 0;
  let matchingFamilies = 0;
  for (const family of metadata.families) {
    if (family.predicateSet?.includes(predicate)) {
      matchingFamilies++;
      totalTriples += family.numTriples;
    }
  }
  return matchingFamilies === 0 ? Number.POSITIVE_INFINITY : totalTriples;
}

function selectStarScopedMaterializedFamilies(
  patterns: Algebra.Pattern[],
  candidateFamilies: ISmartKgFamily[],
  metadata: ISmartKgMetadata,
): { completeFamilies: ISmartKgFamily[]; partialFamilies: ISmartKgFamily[] } {
  const starPredicates = extractPredicates(patterns);
  const scoped = new Map<number, ISmartKgFamily>();

  for (const family of candidateFamilies) {
    const resolvedFamilies = resolveFamiliesToMaterialized([ family ], metadata);
    const scopedFamilies = Array.isArray(family.sourceSet) && family.sourceSet.length > 0 ?
      resolvedFamilies.filter(resolvedFamily => predicatesAreContainedInFamily(starPredicates, resolvedFamily)) :
      resolvedFamilies;
    for (const scopedFamily of scopedFamilies) {
      scoped.set(scopedFamily.index, scopedFamily);
    }
  }

  const families = deduplicateFamilies([ ...scoped.values() ]);
  return {
    completeFamilies: families.filter(family => predicatesAreContainedInFamily(starPredicates, family)),
    partialFamilies: families.filter(family => !predicatesAreContainedInFamily(starPredicates, family)),
  };
}

function isPatternPredicateContainedInSet(pattern: Algebra.Pattern, predicates: Set<string>): boolean {
  return pattern.predicate.termType !== 'Variable' && predicates.has(termToString(pattern.predicate));
}

function patternToKey(pattern: Algebra.Pattern): string {
  return [
    termToString(pattern.subject),
    termToString(pattern.predicate),
    termToString(pattern.object),
  ].join(' ');
}

function predicatesAreContainedInFamily(predicates: Set<string>, family: ISmartKgFamily): boolean {
  const familyPredicates = new Set(family.predicateSet);
  return [ ...predicates ].every(predicate => familyPredicates.has(predicate));
}

function isResolvableFamily(family: ISmartKgFamily): boolean {
  if (Array.isArray(family.sourceSet) && family.sourceSet.length > 0) {
    return true;
  }
  return family.numTriples > 0 && Boolean(family.name);
}

function filterMinimalByPredicateSet(families: ISmartKgFamily[]): ISmartKgFamily[] {
  return families.filter((family) => {
    const familyPredicates = new Set(family.predicateSet);
    return !families.some((other) => {
      if (other.index === family.index) {
        return false;
      }
      return isProperSubset(new Set(other.predicateSet), familyPredicates);
    });
  });
}

function filterMaximalByPredicateSet(families: ISmartKgFamily[]): ISmartKgFamily[] {
  // Many materialized families share the same characteristic set. Comparing
  // every family with every other family is quadratic in the partition count
  // (over 21K entries for WatDiv 10M). Determine maximal characteristic sets
  // once, then retain every family carrying one of those sets. This preserves
  // precisely the same partitions and answers.
  const signatureByFamily = new Map<ISmartKgFamily, string>();
  const predicatesBySignature = new Map<string, Set<string>>();
  for (const family of families) {
    const signature = [ ...family.predicateSet ].sort().join('\u0000');
    signatureByFamily.set(family, signature);
    if (!predicatesBySignature.has(signature)) {
      predicatesBySignature.set(signature, new Set(family.predicateSet));
    }
  }

  const uniqueSets = [ ...predicatesBySignature.entries() ];
  const maximalSignatures = new Set(uniqueSets
    .filter(([ leftSignature, leftPredicates ]) => !uniqueSets.some(([ rightSignature, rightPredicates ]) =>
      leftSignature !== rightSignature && isProperSubset(leftPredicates, rightPredicates)))
    .map(([ signature ]) => signature));
  return families.filter(family => maximalSignatures.has(signatureByFamily.get(family)!));
}

function deduplicateFamilies(families: ISmartKgFamily[]): ISmartKgFamily[] {
  const deduplicated = new Map<number, ISmartKgFamily>();
  for (const family of families) {
    deduplicated.set(family.index, family);
  }
  return [ ...deduplicated.values() ].sort((left, right) => left.index - right.index);
}

function isSameSet(left: Set<string>, right: Set<string>): boolean {
  return left.size === right.size && isSubset(left, right);
}

function isProperSubset(left: Set<string>, right: Set<string>): boolean {
  return left.size < right.size && isSubset(left, right);
}

function isSubset(left: Set<string>, right: Set<string>): boolean {
  return [ ...left ].every(value => right.has(value));
}

async function readStreamToString(stream: NodeJS.ReadableStream): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of <AsyncIterable<Buffer | string>><any> stream) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

function parseTpfCardinality(content: string): number | undefined {
  // eslint-disable-next-line max-len
  const match = /(?:hydra:totalItems|void:triples|<http:\/\/www\.w3\.org\/ns\/hydra\/core#totalItems>|<http:\/\/rdfs\.org\/ns\/void#triples>)\s+"?(\d+)/u
    .exec(content);
  return match ? Number(match[1]) : undefined;
}

function isHdtBuffer(buffer: Buffer): boolean {
  return buffer.subarray(0, 4).toString('utf8') === '$HDT';
}

function isHdtFile(path: string): boolean {
  if (!existsSync(path)) {
    return false;
  }
  const fd = openSync(path, 'r');
  try {
    const header = Buffer.allocUnsafe(4);
    return readSync(fd, header, 0, 4, 0) === 4 && isHdtBuffer(header);
  } finally {
    closeSync(fd);
  }
}

async function extractTarPartitionFile(archivePath: string, partitionPath: string): Promise<void> {
  const fd = openSync(archivePath, 'r');
  let hdtFound = false;
  try {
    const archiveSize = fstatSync(fd).size;
    let offset = 0;
    while (offset + 512 <= archiveSize) {
      const header = Buffer.allocUnsafe(512);
      if (readSync(fd, header, 0, header.length, offset) !== header.length || header.every(byte => byte === 0)) {
        break;
      }
      const name = header.subarray(0, 100).toString('utf8').replace(/\0.*$/u, '');
      const sizeText = header.subarray(124, 136).toString('utf8').replace(/\0.*$/u, '').trim();
      const size = Number.parseInt(sizeText, 8);
      const payloadStart = offset + 512;
      const payloadEnd = payloadStart + size;
      if (!name || !Number.isFinite(size) || size < 0 || payloadEnd > archiveSize) {
        throw new Error('SmartKG partition response contained an invalid tar archive.');
      }
      let target: string | undefined;
      if (name.endsWith('.hdt.index.v1-1')) {
        target = `${partitionPath}.index.v1-1`;
      } else if (name.endsWith('.hdt')) {
        target = partitionPath;
      }
      if (target) {
        await pipeline(
          createReadStream(archivePath, { start: payloadStart, end: payloadEnd - 1 }),
          createWriteStream(target),
        );
        hdtFound ||= target === partitionPath;
      }
      offset = payloadStart + Math.ceil(size / 512) * 512;
    }
  } finally {
    closeSync(fd);
  }
  if (!hdtFound || !isHdtFile(partitionPath)) {
    throw new Error('SmartKG partition response did not contain a valid HDT payload.');
  }
}

async function readFileUtf8(path: string): Promise<string> {
  const stream = createReadStream(path, { encoding: 'utf8' });
  let content = '';
  for await (const chunk of <AsyncIterable<string>><any> stream) {
    content += chunk;
  }
  return content;
}
