/* eslint-disable import/no-nodejs-modules */
import { closeSync, existsSync, mkdirSync, openSync, readSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import { KeysCore } from '@comunica/context-entries';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
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
import {
  getContextRaw,
  KEY_CONTEXT_WISEKG_FALLBACK,
  KEY_CONTEXT_WISEKG_LATENCY_MS,
  KEY_CONTEXT_WISEKG_SPEED_MBPS,
  normalizeUrl,
  setContextFlag,
} from './Utils';
import type {
  IWiseKgExecutableStep,
  IWiseKgFetchedPlan,
  IWiseKgPlanStar,
} from './WiseKgPlan';
import {
  flattenWiseKgPlan,
  getWiseKgPlanExpiry,
  isWiseKgPlanNode,
} from './WiseKgPlan';

const DEFAULT_PLAN_SPEED_MBPS = 1;
const DEFAULT_PLAN_LATENCY_MS = 1_000;
type HdtDocument = {
  searchBindings: (
    bindingsFactory: BindingsFactory,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    options: { offset: number; limit: number },
  ) => Promise<{ bindings?: RDF.Bindings[] }>;
};
type HttpMediator =
Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>, IActionHttp, IActorTest, IActorHttpOutput>;
let hdtModulePromise: Promise<{ fromFile: (path: string) => Promise<HdtDocument> }> | undefined;

// Lazily load HDT support so startup does not eagerly open native modules.
async function loadHdtModule(): Promise<{ fromFile: (path: string) => Promise<HdtDocument> }> {
  if (!hdtModulePromise) {
    hdtModulePromise = <Promise<{ fromFile: (path: string) => Promise<HdtDocument> }>> import('hdt');
  }
  return hdtModulePromise;
}

// Create metadata state for in-memory bindings streams.
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

// Keeps HDT documents open per cache path during a query-source lifetime.
class HdtDocumentCache {
  private readonly cache = new Map<string, Promise<HdtDocument>>();

  // Load or reuse the HDT document for a partition file.
  public async getDocument(path: string): Promise<HdtDocument> {
    const existing = this.cache.get(path);
    if (existing) {
      return existing;
    }
    const documentPromise = loadHdtModule().then(hdt => hdt.fromFile(path));
    this.cache.set(path, documentPromise);
    return documentPromise;
  }

  // Clear cached HDT document promises when the query source is disposed.
  public async dispose(): Promise<void> {
    this.cache.clear();
  }
}

// Wraps an array-producing promise as a Comunica bindings stream.
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
      for (const binding of bindings) {
        this._push(binding);
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

// Emits final bindings as soon as the plan executor produces them.
class StreamingBindingsIterator extends BufferedIterator<RDF.Bindings> {
  private emitted = 0;
  private readonly state = createMetadataValidationState();

  public constructor(
    producer: (emit: (binding: RDF.Bindings) => void) => Promise<void>,
    private readonly variables: MetadataBindings['variables'],
  ) {
    super({ autoStart: true, maxBufferSize: 256 });
    this.setProperty('metadata', {
      state: this.state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
    producer((binding) => {
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
}

// Query source that executes WiseKG plans with local HDT partitions and fallback requests.
export class QuerySourceWiseKg implements IQuerySource {
  public readonly referenceValue: string;

  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly mediatorHttp: HttpMediator;
  private readonly defaultContext: IActionContext;
  private readonly cacheFolder: string;
  private readonly hdtCache = new HdtDocumentCache();
  private readonly originalSourceUrl: string;
  private readonly metadataUrl: string;
  private readonly partitionsBaseUrl: string;
  private readonly planUrl: string;
  private readonly mediatorQuerySourceDereferenceLink?: any;
  private readonly patternCardinalityCache = new Map<string, number>();
  private readonly planCache = new Map<string, IWiseKgFetchedPlan>();
  private readonly hdtPatternCache = new Map<string, Promise<RDF.Bindings[]>>();
  private bindingsFactory: BindingsFactory | undefined;

  public constructor(
    url: string,
    dataFactory: ComunicaDataFactory,
    mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>, IActionHttp, IActorTest, IActorHttpOutput>,
    context: IActionContext,
    mediatorQuerySourceDereferenceLink?: any,
  ) {
    const normalizedUrl = normalizeUrl(url);
    const datasetName = normalizedUrl.split('/').filter(Boolean).pop() ?? 'wisekg';
    const origin = normalizedUrl.replace(/\/$/u, '').replace(/\/[^/]+$/u, '');

    this.referenceValue = normalizedUrl;
    this.originalSourceUrl = normalizedUrl;
    this.metadataUrl = `${origin}/molecule/${datasetName}`;
    this.partitionsBaseUrl = `${origin}/molecule/${datasetName}`;
    this.planUrl = `${origin}/plan`;
    this.dataFactory = dataFactory;
    this.mediatorHttp = mediatorHttp;
    this.defaultContext = context;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.cacheFolder = join(process.cwd(), '.wisekg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder, { recursive: true });
    }

    const algebraFactory = new AlgebraFactory(<RDF.DataFactory> <unknown> this.dataFactory);
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

  // Expose the WiseKG selector shape for patterns, BGPs, and joins.
  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  // Evaluate the operation and emit final bindings incrementally.
  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    const variables = getOperationVariables(operation);
    return <BindingsStream> <unknown>
      new StreamingBindingsIterator(
        emit => this.streamOperation(operation, context, options, emit),
        variables,
      );
  }

  // Return no quads because WiseKG evaluation produces bindings.
  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): AsyncIterator<RDF.Quad> {
    return new EmptyIterator<RDF.Quad>();
  }

  public async queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('ASK queries are not supported by QuerySourceWiseKg.');
  }

  public async queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('UPDATE queries are not supported by QuerySourceWiseKg.');
  }

  public toString(): string {
    return `QuerySourceWiseKg(${this.referenceValue})`;
  }

  // Release cached HDT document references.
  public async dispose(): Promise<void> {
    await this.hdtCache.dispose();
  }

  // Route supported algebra operation types to the WiseKG evaluation path.
  private async evaluateOperation(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
      return this.queryStarViaOriginalSource([ operation ], context, options);
    }
    if (isKnownOperation(operation, Algebra.Types.BGP)) {
      return this.evaluateBgp(operation, context, options);
    }
    if (isKnownOperation(operation, Algebra.Types.JOIN)) {
      const patterns = collectPatterns(operation);
      if (patterns.length === 0) {
        throw new Error(`Unsupported non-pattern operation '${operation.type}' in QuerySourceWiseKg join.`);
      }
      return this.evaluatePatternsByPlan(patterns, context, options);
    }
    throw new Error(`Unsupported operation type '${operation.type}' for QuerySourceWiseKg.`);
  }

  private async streamOperation(
    operation: Algebra.Operation,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => void,
  ): Promise<void> {
    const patterns = isKnownOperation(operation, Algebra.Types.PATTERN) ?
        [ operation ] :
      collectPatterns(operation);
    if (patterns.length === 0) {
      throw new Error(`Unsupported operation type '${operation.type}' for QuerySourceWiseKg.`);
    }

    const fetchedPlan = patterns.length > 1 ? await this.fetchWiseKgPlan(patterns, context) : undefined;
    if (fetchedPlan) {
      await this.executeWiseKgPlanStreaming(patterns, fetchedPlan, context, options, emit);
      return;
    }

    for (const binding of await this.queryStarViaOriginalSourceWithRetry(patterns, context, options)) {
      emit(binding);
    }
  }

  // Fetch a WiseKG plan for a BGP and execute it.
  private async evaluateBgp(
    bgp: Algebra.Bgp,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const fetchedPlan = await this.fetchWiseKgPlan(bgp.patterns, context);

    if (fetchedPlan) {
      return this.executeWiseKgPlan(bgp.patterns, fetchedPlan, context, options);
    }

    return this.queryStarViaOriginalSourceWithRetry(bgp.patterns, context, options);
  }

  // Fetch a WiseKG plan for patterns collected from a join.
  private async evaluatePatternsByPlan(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const fetchedPlan = await this.fetchWiseKgPlan(patterns, context);

    if (fetchedPlan) {
      return this.executeWiseKgPlan(patterns, fetchedPlan, context, options);
    }

    return this.queryStarViaOriginalSourceWithRetry(patterns, context, options);
  }

  // Request and cache the WiseKG server plan for a set of patterns.
  private async fetchWiseKgPlan(
    patterns: Algebra.Pattern[],
    context: IActionContext,
  ): Promise<IWiseKgFetchedPlan | undefined> {
    const url = this.buildWiseKgPlanUrl(patterns, context);
    const cached = this.planCache.get(url);
    if (cached && (!cached.expiresAt || Date.now() <= cached.expiresAt)) {
      this.logDebug(context, `WiseKG reusing cached plan ${url}`);
      return cached;
    }
    if (cached) {
      this.planCache.delete(url);
    }

    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        this.logDebug(context, `WiseKG requesting plan ${url}`);
        const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: url });
        if (!response.ok) {
          this.logDebug(context, `WiseKG /plan returned HTTP ${response.status}; falling back.`);
          return undefined;
        }

        const body = ActorHttp.toNodeReadable(response.body);
        const content = await readStreamToString(body);
        const parsed = <unknown> JSON.parse(content);
        if (!isWiseKgPlanNode(parsed)) {
          this.logDebug(context, 'WiseKG /plan returned malformed JSON; falling back.');
          return undefined;
        }

        const plan = parsed;
        const steps = flattenWiseKgPlan(plan);
        if (steps.length === 0) {
          this.logDebug(context, 'WiseKG /plan returned no executable steps; falling back.');
          return undefined;
        }

        this.logDebug(context, `WiseKG flattened plan into ${steps.length} step(s).`);
        for (const step of steps) {
          this.logDebug(context, `WiseKG plan step control ${step.control}`);
        }

        const fetchedPlan = {
          plan,
          steps,
          expiresAt: getWiseKgPlanExpiry(plan),
        };
        this.planCache.set(url, fetchedPlan);
        return fetchedPlan;
      } catch (error) {
        if (attempt === maxAttempts) {
          this.logDebug(context, 'WiseKG /plan request failed; falling back.', { error });
          return undefined;
        }
        this.logDebug(context, `WiseKG /plan request failed; retrying (${attempt}/${maxAttempts}).`, { error });
        await sleep(250 * attempt);
      }
    }
    return undefined;
  }

  // Build the /plan URL, including benchmark speed and latency context.
  private buildWiseKgPlanUrl(patterns: Algebra.Pattern[], context: IActionContext): string {
    const serializedBgp = this.serializeBgpForPlan(patterns);
    const parameters = [ `bgp=${encodeURIComponent(serializedBgp)}` ];
    const speed = getContextRaw<string | number>(context, KEY_CONTEXT_WISEKG_SPEED_MBPS) ?? DEFAULT_PLAN_SPEED_MBPS;
    const latency = getContextRaw<string | number>(context, KEY_CONTEXT_WISEKG_LATENCY_MS) ?? DEFAULT_PLAN_LATENCY_MS;
    parameters.push(`speed=${encodeURIComponent(String(speed))}`);
    parameters.push(`latency=${encodeURIComponent(String(latency))}`);
    return `${this.planUrl}?${parameters.join('&')}`;
  }

  // Serialize a BGP as the text payload expected by the WiseKG planner.
  private serializeBgpForPlan(patterns: Algebra.Pattern[]): string {
    return patterns
      .map(pattern =>
        `${this.serializeTermForPlan(pattern.subject)} ` +
        `${this.serializeTermForPlan(pattern.predicate)} ${this.serializeTermForPlan(pattern.object)} .`)
      .join('\n');
  }

  // Serialize one RDF term in the planner's compact SPARQL-like syntax.
  private serializeTermForPlan(term: RDF.Term): string {
    if (term.termType === 'Variable') {
      return `?${term.value}`;
    }
    if (term.termType === 'NamedNode') {
      return `<${term.value}>`;
    }
    return termToString(term);
  }

  // Execute the server-ordered plan and pass each step's bindings to the next.
  private async executeWiseKgPlan(
    _originalPatterns: Algebra.Pattern[],
    fetchedPlan: IWiseKgFetchedPlan,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    let steps = [ ...fetchedPlan.steps ];
    let expiresAt = fetchedPlan.expiresAt;

    if (expiresAt && Date.now() > expiresAt) {
      this.logDebug(context, 'WiseKG plan expired; requesting a new plan for the BGP.');
      const remainingPatterns = steps.flatMap(step => this.wiseKgStarToPatterns(step.star));
      const newPlan = await this.fetchWiseKgPlan(remainingPatterns, context);
      if (newPlan) {
        steps = [ ...newPlan.steps ];
        expiresAt = newPlan.expiresAt;
      }
    }

    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const step of steps) {
      const starPatterns = this.wiseKgStarToPatterns(step.star);
      results = await this.evaluateWiseKgStep(step, starPatterns, results, context, options);
      if (results.length === 0) {
        break;
      }
    }

    return this.deduplicateBindings(results);
  }

  private async executeWiseKgPlanStreaming(
    originalPatterns: Algebra.Pattern[],
    fetchedPlan: IWiseKgFetchedPlan,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => void,
  ): Promise<void> {
    let steps = [ ...fetchedPlan.steps ];
    if (fetchedPlan.expiresAt && Date.now() > fetchedPlan.expiresAt) {
      const newPlan = await this.fetchWiseKgPlan(originalPatterns, context);
      if (newPlan) {
        steps = [ ...newPlan.steps ];
      }
    }

    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const [ index, step ] of steps.entries()) {
      const starPatterns = this.wiseKgStarToPatterns(step.star);
      if (index === steps.length - 1) {
        await this.streamWiseKgStep(step, starPatterns, results, context, options, emit);
        return;
      }
      results = await this.evaluateWiseKgStep(step, starPatterns, results, context, options);
      if (results.length === 0) {
        return;
      }
    }
  }

  // Execute one plan step and join it with the incoming bindings.
  private async evaluateWiseKgStep(
    step: IWiseKgExecutableStep,
    starPatterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    if (this.isPartitionShippingControl(step.control)) {
      const partitionUrl = this.getPartitionHdtUrlForControl(step.control);
      this.logDebug(context, `WiseKG downloading/evaluating partition ${partitionUrl}`);
      const partitionPath = await this.fetchPartitionFileByControl(step.control);
      const stepResults = await this.evaluatePatternsOnPartition(partitionPath, starPatterns);
      const localResults = await this.joinBindingsLists(inputBindings, stepResults);
      const completionResults = await this.queryOriginalStarWithInput(starPatterns, inputBindings, context, options);
      return this.deduplicateBindings([ ...localResults, ...completionResults ]);
    }

    if (this.isServerSourceControl(step.control)) {
      this.logDebug(context, `WiseKG evaluating server-controlled step ${step.control}`);
      const controlledResults = await this.queryStarViaControlledSource(
        starPatterns,
        inputBindings,
        step.control,
        context,
        options,
      );
      if (!this.isServerPartitionControl(step.control)) {
        return controlledResults;
      }
      const completionResults = await this.queryOriginalStarWithInput(starPatterns, inputBindings, context, options);
      return this.deduplicateBindings([ ...controlledResults, ...completionResults ]);
    }

    throw new Error(`WiseKG encountered unsupported plan control '${step.control}'.`);
  }

  private async streamWiseKgStep(
    step: IWiseKgExecutableStep,
    starPatterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => void,
  ): Promise<void> {
    const seen = new Set<string>();
    const emitOnce = (binding: RDF.Bindings): void => {
      const key = bindingKey(binding);
      if (!seen.has(key)) {
        seen.add(key);
        emit(binding);
      }
    };

    if (this.isPartitionShippingControl(step.control)) {
      const partitionUrl = this.getPartitionHdtUrlForControl(step.control);
      this.logDebug(context, `WiseKG downloading/evaluating partition ${partitionUrl}`);
      const partitionPath = await this.fetchPartitionFileByControl(step.control);
      const stepResults = await this.evaluatePatternsOnPartition(partitionPath, starPatterns);
      await this.emitJoinedBindings(inputBindings, stepResults, emitOnce);
      for (const binding of await this.queryOriginalStarWithInput(starPatterns, inputBindings, context, options)) {
        emitOnce(binding);
      }
      return;
    }

    if (this.isServerSourceControl(step.control)) {
      if (this.isServerPartitionControl(step.control)) {
        const stream = await this.queryPartitionStarBindings(
          starPatterns,
          inputBindings,
          step.control,
          context,
          options,
        );
        for await (const binding of <AsyncIterable<RDF.Bindings>> <any> stream) {
          emitOnce(binding);
        }
        for (const binding of await this.queryOriginalStarWithInput(starPatterns, inputBindings, context, options)) {
          emitOnce(binding);
        }
        return;
      }
      for (const binding of await this.queryStarViaControlledSource(
        starPatterns,
        inputBindings,
        step.control,
        context,
        options,
      )) {
        emitOnce(binding);
      }
      return;
    }

    throw new Error(`WiseKG encountered unsupported plan control '${step.control}'.`);
  }

  // Convert a WiseKG plan star back into algebra triple patterns.
  private wiseKgStarToPatterns(star: IWiseKgPlanStar): Algebra.Pattern[] {
    const algebraFactory = new AlgebraFactory(<RDF.DataFactory> <unknown> this.dataFactory);
    const subject = this.planValueToTerm(star.subject, 'subject');
    return star.triples.map((triple) => {
      const predicate = this.planValueToTerm(triple.x, 'predicate');
      const object = this.planValueToTerm(triple.y, 'object');
      return algebraFactory.createPattern(subject, predicate, object);
    });
  }

  // Convert one WiseKG plan value into an RDF/JS term.
  private planValueToTerm(value: string, position: 'subject' | 'predicate' | 'object'): RDF.Term {
    const trimmed = value.trim();
    if (trimmed.startsWith('?')) {
      return this.dataFactory.variable(trimmed.slice(1));
    }
    if (trimmed.startsWith('<') && trimmed.endsWith('>')) {
      return this.dataFactory.namedNode(trimmed.slice(1, -1));
    }
    if (trimmed.startsWith('_:')) {
      return this.dataFactory.blankNode(trimmed.slice(2));
    }
    if (looksLikeIri(trimmed) || position === 'predicate' || position === 'subject') {
      return this.dataFactory.namedNode(trimmed);
    }
    return this.dataFactory.literal(trimmed);
  }

  // Molecule controls ship HDT partitions for local client-side evaluation.
  private isPartitionShippingControl(control: string): boolean {
    return control.includes('/molecule/') || control.startsWith('molecule/') || control.endsWith('.hdt');
  }

  // Dataset and /partition controls are evaluated by their server-side fragment interface.
  private isServerSourceControl(control: string): boolean {
    return control === 'wisekg' || control === 'smartkg+' || control.includes('/partition/') ||
      control.startsWith('partition/') || normalizeUrl(control) === this.originalSourceUrl;
  }

  // Download or reuse the local HDT file for a partition control.
  private async fetchPartitionFileByControl(control: string): Promise<string> {
    const partitionUrl = this.getPartitionHdtUrlForControl(control);
    const partitionPath = join(this.cacheFolder, encodeURIComponent(partitionUrl));
    if (isHdtFile(partitionPath)) {
      return partitionPath;
    }

    const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: partitionUrl });
    if (!response.ok) {
      throw new Error(`WiseKG partition download failed with HTTP ${response.status}: ${partitionUrl}`);
    }
    const body = ActorHttp.toNodeReadable(response.body);
    const payload = await readStreamToBuffer(body);
    this.writePartitionPayload(partitionPath, payload);
    if (!existsSync(`${partitionPath}.index.v1-1`)) {
      const indexResponse = await this.mediatorHttp.mediate({
        context: this.defaultContext,
        input: `${partitionUrl}.index.v1-1`,
      });
      if (indexResponse.ok) {
        const indexBody = ActorHttp.toNodeReadable(indexResponse.body);
        writeFileSync(`${partitionPath}.index.v1-1`, await readStreamToBuffer(indexBody));
      }
    }
    return partitionPath;
  }

  // Persist a partition payload, extracting HDT files from tar payloads when needed.
  private writePartitionPayload(partitionPath: string, payload: Buffer): void {
    if (isHdtBuffer(payload)) {
      writeFileSync(partitionPath, payload);
      return;
    }

    const entries = extractTarEntries(payload);
    const hdtEntry = entries.find(entry => entry.name.endsWith('.hdt') && !entry.name.endsWith('.index.v1-1'));
    if (!hdtEntry) {
      writeFileSync(partitionPath, payload);
      return;
    }

    writeFileSync(partitionPath, hdtEntry.payload);
    const indexEntry = entries.find(entry => entry.name === `${hdtEntry.name}.index.v1-1`);
    if (indexEntry) {
      writeFileSync(`${partitionPath}.index.v1-1`, indexEntry.payload);
    }
  }

  // Convert a plan control string into the partition HDT URL.
  private getPartitionHdtUrlForControl(control: string): string {
    if (control.endsWith('.hdt') && /^https?:\/\//u.test(control)) {
      return control;
    }

    const id = this.extractPartitionId(control);
    if (id) {
      return `${this.partitionsBaseUrl}/${id}.hdt`;
    }

    throw new Error(`Unsupported WiseKG partition control '${control}'.`);
  }

  // Extract a partition id from relative controls, molecule paths, or URLs.
  private extractPartitionId(control: string): string | undefined {
    if (control.startsWith('partition/')) {
      return control.slice('partition/'.length).replace(/\.hdt$/u, '');
    }

    if (control.startsWith('molecule/')) {
      return control.split('/').filter(Boolean).pop()?.replace(/\.hdt$/u, '');
    }

    if (!control.includes('/') && control.endsWith('.hdt')) {
      return control.replace(/\.hdt$/u, '');
    }

    try {
      const parsed = new URL(control);
      const match = /\/partition\/([^/?#]+)/u.exec(parsed.pathname);
      const partitionId = match?.[1]?.replace(/\.hdt$/u, '');
      if (partitionId) {
        return partitionId;
      }
      const moleculeMatch = /\/molecule\/(?:[^/?#]+\/)?([^/?#]+(?:\.hdt)?)$/u.exec(parsed.pathname);
      return moleculeMatch?.[1]?.replace(/\.hdt$/u, '');
    } catch {
      return undefined;
    }

    return undefined;
  }

  // Evaluate a star or pattern sequence against a local HDT partition.
  private async evaluatePatternsOnPartition(
    partitionPath: string,
    patterns: Algebra.Pattern[],
  ): Promise<RDF.Bindings[]> {
    const document = await this.hdtCache.getDocument(partitionPath);
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];

    for (const pattern of patterns) {
      const bindings = await this.collectCachedHdtBindings(partitionPath, document, pattern);
      results = this.deduplicateBindings(await this.joinBindingsLists(results, bindings));
      if (results.length === 0) {
        break;
      }
    }

    return this.deduplicateBindings(results);
  }

  // Cache local HDT lookups by partition path and pattern key.
  private collectCachedHdtBindings(
    partitionPath: string,
    document: HdtDocument,
    pattern: Algebra.Pattern,
  ): Promise<RDF.Bindings[]> {
    const cacheKey = `${partitionPath}|${patternKey(pattern)}`;
    const cached = this.hdtPatternCache.get(cacheKey);
    if (cached) {
      return cached;
    }
    const promise = this.collectHdtBindings(document, pattern);
    this.hdtPatternCache.set(cacheKey, promise);
    return promise;
  }

  // Page through HDT search results for one pattern.
  private async collectHdtBindings(document: HdtDocument, pattern: Algebra.Pattern): Promise<RDF.Bindings[]> {
    const bindingsFactory = await this.getBindingsFactory();
    const pageSize = 16_384;
    let offset = 0;
    const results: RDF.Bindings[] = [];

    while (true) {
      const result = await document.searchBindings(
        bindingsFactory,
        pattern.subject,
        pattern.predicate,
        pattern.object,
        { offset, limit: pageSize },
      );
      const bindings: RDF.Bindings[] = Array.isArray(result?.bindings) ? result.bindings : [];
      results.push(...bindings);

      if (bindings.length < pageSize) {
        break;
      }
      offset += bindings.length;
    }

    return this.deduplicateBindings(results);
  }

  // Evaluate a star through the original source using an adaptive order.
  private async queryStarViaOriginalSource(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
    inputBindings?: RDF.Bindings[],
  ): Promise<RDF.Bindings[]> {
    const initialBindings = inputBindings ?? [ await this.emptyBinding() ];
    const unboundOrder = initialBindings.length === 1 && initialBindings[0].size === 0 ?
      await this.getUnboundOriginalSourceJoinOrder(patterns, context) :
      undefined;
    if (unboundOrder) {
      return this.queryStarViaOriginalSourceUnbound(unboundOrder, context, options);
    }

    let results = initialBindings;
    const remaining = [ ...patterns ];

    while (remaining.length > 0) {
      const nextIndex = await this.selectNextOriginalSourcePattern(remaining, results, context);
      const [ pattern ] = remaining.splice(nextIndex, 1);
      results = await this.queryOriginalSourcePatternWithBindings(pattern, results, context, options);
      if (results.length === 0) {
        break;
      }
    }
    return this.deduplicateBindings(results);
  }

  // Choose an unbound join order when bound requests would be too expensive.
  private async getUnboundOriginalSourceJoinOrder(
    patterns: Algebra.Pattern[],
    context: IActionContext,
  ): Promise<Algebra.Pattern[] | undefined> {
    if (patterns.length < 2) {
      return undefined;
    }

    const estimated = await Promise.all(patterns.map(async pattern => ({
      pattern,
      cardinality: await this.estimateOriginalSourcePatternCardinality(pattern, context),
    })));
    if (estimated.some(entry => !Number.isFinite(entry.cardinality) || entry.cardinality === Number.MAX_SAFE_INTEGER)) {
      return undefined;
    }

    const minCardinality = Math.min(...estimated.map(entry => entry.cardinality));
    const estimatedBindingRequests = 1 + (minCardinality * Math.max(1, patterns.length - 1));
    const unboundRequests = patterns.length;
    if (estimatedBindingRequests <= unboundRequests * 8) {
      return undefined;
    }

    return estimated
      .sort((left, right) => left.cardinality - right.cardinality)
      .map(entry => entry.pattern);
  }

  // Query the original source without bound requests and join locally.
  private async queryStarViaOriginalSourceUnbound(
    orderedPatterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of orderedPatterns) {
      const stream = await this.queryFallbackBindings(pattern, context, options);
      const bindings = await this.collectBindingsFromStream(stream);
      results = this.deduplicateBindings(await this.joinBindingsLists(results, bindings));
      if (results.length === 0) {
        break;
      }
    }
    return this.deduplicateBindings(results);
  }

  // Retry original-source evaluation to tolerate transient server failures.
  private async queryStarViaOriginalSourceWithRetry(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
    inputBindings?: RDF.Bindings[],
  ): Promise<RDF.Bindings[]> {
    const maxAttempts = 8;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await this.queryStarViaOriginalSource(patterns, context, options, inputBindings);
      } catch (error) {
        if (attempt === maxAttempts) {
          throw error;
        }
        this.logDebug(
          context,
          `WiseKG original-source star request failed; retrying (${attempt}/${maxAttempts}).`,
          { error },
        );
        await sleep(1_000 * attempt);
      }
    }
    return [];
  }

  // Select the next original-source pattern using cardinality and bound-term hints.
  private async selectNextOriginalSourcePattern(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
  ): Promise<number> {
    let bestIndex = 0;
    let bestScore = Number.POSITIVE_INFINITY;

    for (const [ index, pattern ] of patterns.entries()) {
      const boundTermCount = this.countBoundTerms(pattern, inputBindings);
      const cardinality = await this.estimateOriginalSourcePatternCardinality(pattern, context);
      const score = cardinality - (boundTermCount * 1_000_000);
      if (score < bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    }

    return bestIndex;
  }

  // Estimate original-source pattern cardinality through metadata.
  private async estimateOriginalSourcePatternCardinality(
    pattern: Algebra.Pattern,
    context: IActionContext,
  ): Promise<number> {
    const key = patternKey(pattern);
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
      this.logDebug(context, `WiseKG estimated original-source pattern cardinality ${cardinality} for ${key}`);
      return cardinality;
    } catch (error) {
      this.logDebug(context, `WiseKG could not estimate original-source pattern cardinality for ${key}.`, { error });
      return Number.MAX_SAFE_INTEGER;
    }
  }

  // Build a TPF-style URL for one original-source pattern.
  private buildOriginalSourcePatternUrl(pattern: Algebra.Pattern): string {
    const parameters = new URLSearchParams();
    this.appendOriginalSourcePatternParameter(parameters, 'subject', pattern.subject);
    this.appendOriginalSourcePatternParameter(parameters, 'predicate', pattern.predicate);
    this.appendOriginalSourcePatternParameter(parameters, 'object', pattern.object);
    const queryString = parameters.toString();
    return queryString.length > 0 ? `${this.originalSourceUrl}?${queryString}` : this.originalSourceUrl;
  }

  // Add one concrete pattern term to the original-source URL parameters.
  private appendOriginalSourcePatternParameter(parameters: URLSearchParams, key: string, term: RDF.Term): void {
    const value = this.termToOriginalSourceParameter(term);
    if (value !== undefined) {
      parameters.append(key, value);
    }
  }

  // Encode an RDF term as an original-source URL parameter.
  private termToOriginalSourceParameter(term: RDF.Term): string | undefined {
    if (term.termType === 'Variable') {
      return undefined;
    }
    if (term.termType === 'NamedNode') {
      return term.value;
    }
    return termToString(term);
  }

  // Query the original source for one bound pattern group and join results.
  private async queryOriginalSourcePatternWithBindings(
    pattern: Algebra.Pattern,
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
    sourceUrl = this.originalSourceUrl,
  ): Promise<RDF.Bindings[]> {
    const grouped = new Map<string, { pattern: Algebra.Pattern; inputBindings: RDF.Bindings[] }>();
    for (const inputBinding of inputBindings) {
      const boundPattern = this.bindPattern(pattern, inputBinding);
      const key = patternKey(boundPattern);
      const group = grouped.get(key) ?? { pattern: boundPattern, inputBindings: []};
      group.inputBindings.push(inputBinding);
      grouped.set(key, group);
    }

    const results: RDF.Bindings[] = [];
    for (const group of grouped.values()) {
      const stream = await this.queryFallbackBindings(group.pattern, context, options, sourceUrl);
      const bindings = await this.collectBindingsFromStream(stream);
      results.push(...await this.joinBindingsLists(group.inputBindings, bindings));
    }

    return this.deduplicateBindings(results);
  }

  private async queryStarViaControlledSource(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    control: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const sourceUrl = this.resolveControlUrl(control);
    if (this.isServerPartitionControl(control)) {
      const stream = await this.queryPartitionStarBindings(patterns, inputBindings, control, context, options);
      return this.collectBindingsFromStream(stream);
    }

    let results = inputBindings;
    const remaining = [ ...patterns ];
    while (remaining.length > 0) {
      const nextIndex = await this.selectNextOriginalSourcePattern(remaining, results, context);
      const [ pattern ] = remaining.splice(nextIndex, 1);
      results = await this.queryOriginalSourcePatternWithBindings(pattern, results, context, options, sourceUrl);
      if (results.length === 0) {
        break;
      }
    }
    return this.deduplicateBindings(results);
  }

  private async queryOriginalStarWithInput(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const sourceResults = await this.queryStarViaOriginalSourceWithRetry(patterns, context, options);
    return this.deduplicateBindings(await this.joinBindingsLists(inputBindings, sourceResults));
  }

  private isServerPartitionControl(control: string): boolean {
    return control.includes('/partition/') || control.startsWith('partition/');
  }

  private async queryPartitionStarBindings(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    control: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<BindingsStream> {
    const sourceUrl = this.resolveControlUrl(control);
    const operation = new AlgebraFactory(<RDF.DataFactory> <unknown> this.dataFactory).createBgp(patterns);
    const joinBindings = this.createJoinBindings(inputBindings);
    return this.queryForcedSourceBindings(
      operation,
      context,
      { ...options, joinBindings },
      sourceUrl,
      'spf',
    );
  }

  private async queryForcedSourceBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options: IQueryBindingsOptions,
    sourceUrl: string,
    forceSourceType: string,
  ): Promise<BindingsStream> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      throw new Error(`WiseKG can not dereference forced ${forceSourceType} source ${sourceUrl}.`);
    }
    const fallbackContext = setContextFlag(context, KEY_CONTEXT_WISEKG_FALLBACK, true);
    const result = await this.mediatorQuerySourceDereferenceLink.mediate({
      context: fallbackContext,
      link: { url: sourceUrl, forceSourceType },
      handledDatasets: {},
    });
    if (!result?.source || typeof result.source.queryBindings !== 'function') {
      throw new Error(`WiseKG did not obtain a queryable ${forceSourceType} source for ${sourceUrl}.`);
    }
    return result.source.queryBindings(operation, fallbackContext, options);
  }

  private createJoinBindings(bindings: RDF.Bindings[]): NonNullable<IQueryBindingsOptions['joinBindings']> {
    const variables = getBindingsVariables(bindings);
    return {
      bindings: <BindingsStream> <unknown> new ArrayBindingsIterator(Promise.resolve(bindings), variables),
      metadata: {
        state: createMetadataValidationState(),
        cardinality: { type: 'exact', value: bindings.length },
        variables,
        next: [],
      },
    };
  }

  private resolveControlUrl(control: string): string {
    if (control === 'wisekg' || control === 'smartkg+' || normalizeUrl(control) === this.originalSourceUrl) {
      return this.originalSourceUrl;
    }
    if (/^https?:\/\//u.test(control)) {
      return control;
    }
    const origin = new URL(this.originalSourceUrl).origin;
    return new URL(control.replace(/^\//u, ''), `${origin}/`).toString().replace(/\/$/u, '');
  }

  // Substitute variables in a pattern with values from one binding.
  private bindPattern(pattern: Algebra.Pattern, binding: RDF.Bindings): Algebra.Pattern {
    const algebraFactory = new AlgebraFactory(<RDF.DataFactory> <unknown> this.dataFactory);
    return algebraFactory.createPattern(
      this.bindTerm(pattern.subject, binding),
      this.bindTerm(pattern.predicate, binding),
      this.bindTerm(pattern.object, binding),
    );
  }

  // Substitute one term when the binding contains a value for its variable.
  private bindTerm(term: RDF.Term, binding: RDF.Bindings): RDF.Term {
    if (term.termType !== 'Variable') {
      return term;
    }
    return bindingToRecord(binding)[term.value] ?? term;
  }

  // Count how many pattern positions are already bound in a sample of bindings.
  private countBoundTerms(pattern: Algebra.Pattern, bindings: RDF.Bindings[]): number {
    if (bindings.length === 0) {
      return 0;
    }
    const sampleBindings = bindings.slice(0, 8);
    let maxCount = 0;
    for (const binding of sampleBindings) {
      let count = 0;
      if (pattern.subject.termType === 'Variable' && bindingToRecord(binding)[pattern.subject.value]) {
        count++;
      }
      if (pattern.predicate.termType === 'Variable' && bindingToRecord(binding)[pattern.predicate.value]) {
        count++;
      }
      if (pattern.object.termType === 'Variable' && bindingToRecord(binding)[pattern.object.value]) {
        count++;
      }
      maxCount = Math.max(maxCount, count);
    }
    return maxCount;
  }

  // Query fallback sources through Comunica and expose a bindings stream.
  private async queryFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
    sourceUrl = this.originalSourceUrl,
  ): Promise<BindingsStream> {
    const variables = getOperationVariables(operation);
    return <BindingsStream> <unknown>
      new ArrayBindingsIterator(
        this.collectFallbackBindings(operation, context, options, sourceUrl),
        variables,
      );
  }

  // Traverse fallback links and collect bindings from compatible sources.
  private async collectFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
    sourceUrl = this.originalSourceUrl,
  ): Promise<RDF.Bindings[]> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      return [];
    }

    const fallbackContext = setContextFlag(context, KEY_CONTEXT_WISEKG_FALLBACK, true);
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

      const result = await this.mediatorQuerySourceDereferenceLink.mediate({
        context: fallbackContext,
        link: { url: nextUrl },
        handledDatasets,
      });
      const source = result?.source;
      if (!source || typeof source.queryBindings !== 'function') {
        continue;
      }

      const stream = source.queryBindings(operation, fallbackContext, options);
      const { bindings, metadata } = await this.collectBindingsAndMetadata(stream);
      results.push(...bindings);

      const streamNextLinks = getMetadataNextLinks(metadata);
      const nextLinks = streamNextLinks.length > 0 ?
        streamNextLinks :
        getMetadataNextLinks(result?.metadata);
      for (const link of nextLinks) {
        if (!seenLinks.has(link) && !isDatasetPageLink(link, sourceUrl)) {
          queuedLinks.push(link);
        }
      }
    }

    return this.deduplicateBindings(results);
  }

  // Lazily create the bindings factory used for local joins.
  private async getBindingsFactory(): Promise<BindingsFactory> {
    if (!this.bindingsFactory) {
      const module = await import('@comunica/utils-bindings-factory');
      this.bindingsFactory = new module.BindingsFactory(this.dataFactory);
    }
    return this.bindingsFactory;
  }

  // Create the initial empty binding for local joins.
  private async emptyBinding(): Promise<RDF.Bindings> {
    const bindingsFactory = await this.getBindingsFactory();
    return bindingsFactory.bindings();
  }

  // Collect all bindings from a stream into an array.
  private async collectBindingsFromStream(stream: BindingsStream): Promise<RDF.Bindings[]> {
    const bindings: RDF.Bindings[] = [];
    for await (const binding of <AsyncIterable<RDF.Bindings>> <any> stream) {
      bindings.push(binding);
    }
    return bindings;
  }

  // Collect stream bindings and capture metadata when the stream exposes it.
  private async collectBindingsAndMetadata(
    stream: BindingsStream,
  ): Promise<{ bindings: RDF.Bindings[]; metadata?: MetadataBindings }> {
    let metadata: MetadataBindings | undefined;
    let resolveMetadata: (metadata?: MetadataBindings) => void = () => {};
    const metadataPromise = new Promise<MetadataBindings | undefined>((resolve) => {
      resolveMetadata = resolve;
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

  // Join two binding arrays using the local bindings factory.
  private async joinBindingsLists(left: RDF.Bindings[], right: RDF.Bindings[]): Promise<RDF.Bindings[]> {
    const bindingsFactory = await this.getBindingsFactory();
    return joinBindingRecords(left.map(bindingToRecord), right.map(bindingToRecord), bindingsFactory);
  }

  private async emitJoinedBindings(
    left: RDF.Bindings[],
    right: RDF.Bindings[],
    emit: (binding: RDF.Bindings) => void,
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
        emit(binding);
      }
    }
  }

  // Remove duplicate bindings by stable variable-term assignments.
  private deduplicateBindings(bindings: RDF.Bindings[]): RDF.Bindings[] {
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

  // Emit debug logging only when the Comunica logger is available.
  private logDebug(context: IActionContext, message: string, data?: unknown): void {
    const logger = context.get(KeysCore.log);
    if (logger && typeof logger.debug === 'function') {
      logger.debug(message, data);
    }
  }
}

// Detect string values that can be safely treated as IRIs.
function looksLikeIri(value: string): boolean {
  return /^[a-z][a-z0-9+.-]*:/iu.test(value);
}

// Determine metadata variables for a query operation.
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

// Collect all pattern operations from a supported algebra tree.
function collectPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
  const patterns: Algebra.Pattern[] = [];
  collectPatternsRecursive(operation, patterns, new Set<Algebra.Pattern>());
  return patterns;
}

// Recursively visit nested algebra inputs while avoiding duplicate patterns.
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

// Convert an RDF/JS bindings object into a plain variable record.
function bindingToRecord(binding: RDF.Bindings): Record<string, RDF.Term> {
  const record: Record<string, RDF.Term> = {};
  for (const [ variable, value ] of binding) {
    record[variable.value] = value;
  }
  return record;
}

// Check whether two variable records can be joined without conflicts.
function areBindingRecordsCompatible(left: Record<string, RDF.Term>, right: Record<string, RDF.Term>): boolean {
  for (const [ variable, value ] of Object.entries(right)) {
    if (left[variable] && !left[variable].equals(value)) {
      return false;
    }
  }
  return true;
}

// Join binding records using an index when shared variables exist.
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

// Join binding records with a nested-loop fallback.
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

function bindingKey(binding: RDF.Bindings): string {
  return [ ...binding ]
    .map(([ variable, value ]) => `${variable.value}=${termToString(value)}`)
    .sort()
    .join('|');
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

// Find variables that are bound on both sides of a join.
function getSharedBoundVariables(
  leftRecords: Record<string, RDF.Term>[],
  rightRecords: Record<string, RDF.Term>[],
): string[] {
  const leftVariables = new Set(leftRecords.flatMap(record => Object.keys(record)));
  const rightVariables = new Set(rightRecords.flatMap(record => Object.keys(record)));
  const shared = [ ...leftVariables ].filter(variable => rightVariables.has(variable));
  return shared.every(variable =>
    leftRecords.every(record => Boolean(record[variable])) &&
    rightRecords.every(record => Boolean(record[variable]))) ?
    shared :
      [];
}

// Build an index key for a set of shared variables.
function joinKey(record: Record<string, RDF.Term>, variables: string[]): string {
  return variables.map(variable => termToString(record[variable])).join('\u001F');
}

// Build a stable cache key for an algebra pattern.
function patternKey(pattern: Algebra.Pattern): string {
  return [
    termToString(pattern.subject),
    termToString(pattern.predicate),
    termToString(pattern.object),
  ].join(' ');
}

// Parse a TPF-style cardinality number from response content.
function parseTpfCardinality(content: string): number | undefined {
  const match = /(?:hydra:totalItems|void:triples)\s+"?(\d+)/u.exec(content);
  return match ? Number(match[1]) : undefined;
}

// Extract next links from bindings metadata or hypermedia metadata.
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

// Detect dataset pagination links that should not be followed as data sources.
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

// Collect a readable stream into a UTF-8 string.
async function readStreamToString(stream: NodeJS.ReadableStream): Promise<string> {
  const decoder = new TextDecoder();
  let content = '';
  for await (const chunk of <AsyncIterable<Uint8Array | string>> <any> stream) {
    content += typeof chunk === 'string' ? chunk : decoder.decode(chunk, { stream: true });
  }
  return content + decoder.decode();
}

// Collect a readable stream into a Buffer.
async function readStreamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of <AsyncIterable<Uint8Array | string>> <any> stream) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

// Check whether a payload starts with the HDT magic header.
function isHdtBuffer(buffer: Buffer): boolean {
  return buffer.subarray(0, 4).toString('utf8') === '$HDT';
}

// Check whether an existing file appears to be an HDT file.
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

// Extract file entries from a simple tar payload.
function extractTarEntries(buffer: Buffer): { name: string; payload: Buffer }[] {
  const entries: { name: string; payload: Buffer }[] = [];
  let offset = 0;

  while (offset + 512 <= buffer.length) {
    const header = buffer.subarray(offset, offset + 512);
    if (header.every(byte => byte === 0)) {
      break;
    }

    const name = header.subarray(0, 100).toString('utf8').replace(/\0.*$/u, '');
    const sizeText = header.subarray(124, 136).toString('utf8').replace(/\0.*$/u, '').trim();
    const size = Number.parseInt(sizeText, 8);
    if (!name || !Number.isFinite(size) || size < 0) {
      break;
    }

    const payloadStart = offset + 512;
    const payloadEnd = payloadStart + size;
    if (payloadEnd > buffer.length) {
      break;
    }

    entries.push({ name, payload: buffer.subarray(payloadStart, payloadEnd) });
    offset = payloadStart + Math.ceil(size / 512) * 512;
  }

  return entries;
}

// Wait before retrying transient WiseKG requests.
async function sleep(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms));
}
