import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import type { IActorTest, Mediator, Actor } from '@comunica/core';
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
import * as HDT from 'hdt';
import { AsyncIterator, BufferedIterator, EmptyIterator } from 'asynciterator';
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

function createMetadataValidationState(): MetadataBindings['state'] {
  const listeners: Array<() => void> = [];
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
  private readonly cache = new Map<string, { document: HDT.Document; refCount: number }>();

  public async getDocument(path: string): Promise<HDT.Document> {
    const existing = this.cache.get(path);
    if (existing) {
      existing.refCount++;
      return existing.document;
    }
    const document = await (HDT as any).fromFile(path);
    this.cache.set(path, { document, refCount: 1 });
    return document;
  }

  public releaseDocument(path: string): void {
    const entry = this.cache.get(path);
    if (entry) {
      entry.refCount = Math.max(0, entry.refCount - 1);
    }
  }

  public async dispose(): Promise<void> {
    this.cache.clear();
  }
}

class ArrayBindingsIterator extends BufferedIterator<RDF.Bindings> {
  public constructor(bindingsPromise: Promise<RDF.Bindings[]>, variables: MetadataBindings['variables']) {
    super({ autoStart: true, maxBufferSize: 256 });
    void this.pushAll(bindingsPromise, variables);
  }

  private async pushAll(bindingsPromise: Promise<RDF.Bindings[]>, variables: MetadataBindings['variables']): Promise<void> {
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
      this.destroy(error as Error);
    }
  }

  public override _read(_count: number, done: () => void): void {
    done();
  }
}

export class QuerySourceSmartKg implements IQuerySource {
  public readonly referenceValue: string;

  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>, IActionHttp, IActorTest, IActorHttpOutput>;
  private readonly defaultContext: IActionContext;
  private readonly cacheFolder: string;
  private readonly hdtCache = new HdtDocumentCache();
  private readonly originalSourceUrl: string;
  private readonly metadataUrl: string;
  private readonly partitionsBaseUrl: string;
  private readonly mediatorQuerySourceDereferenceLink?: any;
  private readonly smartKgPlusSource: boolean;
  private bindingsFactory: BindingsFactory | undefined;
  private metadata: ISmartKgMetadata | undefined;

  public constructor(
    url: string,
    dataFactory: ComunicaDataFactory,
    mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>, IActionHttp, IActorTest, IActorHttpOutput>,
    context: IActionContext,
    mediatorQuerySourceDereferenceLink?: any,
  ) {
    const normalizedUrl = normalizeUrl(url);
    const datasetName = normalizedUrl.split('/').filter(Boolean).pop() ?? 'smartkg';
    const origin = normalizedUrl.replace(/\/$/, '').replace(/\/[^/]+$/, '');

    this.referenceValue = normalizedUrl;
    this.originalSourceUrl = normalizedUrl;
    this.metadataUrl = `${origin}/molecule/${datasetName}`;
    this.partitionsBaseUrl = `${origin}/molecule/${datasetName}`;
    this.smartKgPlusSource = datasetName.toLowerCase() === 'smartkg+';
    this.dataFactory = dataFactory;
    this.mediatorHttp = mediatorHttp;
    this.defaultContext = context;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.cacheFolder = join(process.cwd(), '.smartkg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder, { recursive: true });
    }

    const algebraFactory = new AlgebraFactory(this.dataFactory as unknown as RDF.DataFactory);
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
    return new ArrayBindingsIterator(this.evaluateOperation(operation, context, options), variables) as unknown as BindingsStream;
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
    await this.hdtCache.dispose();
  }

  private async evaluateOperation(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
      return this.evaluatePattern(operation, context, options);
    }
    if (isKnownOperation(operation, Algebra.Types.BGP)) {
      return this.evaluateBgp(operation as Algebra.Bgp, context, options);
    }
    if (isKnownOperation(operation, Algebra.Types.JOIN)) {
      return this.evaluateStarJoin(operation, context, options);
    }
    throw new Error(`Unsupported operation type '${operation.type}' for QuerySourceSmartKg.`);
  }

  private async evaluateBgp(
    bgp: Algebra.Bgp,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const stars = new Map<string, Algebra.Pattern[]>();
    for (const pattern of bgp.patterns) {
      const subject = termToString(pattern.subject);
      const bucket = stars.get(subject) ?? [];
      bucket.push(pattern);
      stars.set(subject, bucket);
    }

    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const starPatterns of stars.values()) {
      const starResults = await this.evaluateStar(starPatterns, context, options);
      results = await this.joinBindingsLists(results, starResults);
    }
    return deduplicateBindings(results);
  }

  private async evaluateStarJoin(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const patterns = collectPatterns(operation);
    if (patterns.length === 0) {
      throw new Error(`Unsupported non-pattern operation '${operation.type}' in QuerySourceSmartKg join.`);
    }
    return this.evaluatePatternsByStars(patterns, context, options);
  }

  private async evaluatePatternsByStars(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const stars = new Map<string, Algebra.Pattern[]>();
    for (const pattern of patterns) {
      const subject = termToString(pattern.subject);
      const bucket = stars.get(subject) ?? [];
      bucket.push(pattern);
      stars.set(subject, bucket);
    }

    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const starPatterns of stars.values()) {
      const starResults = await this.evaluateStar(starPatterns, context, options);
      results = await this.joinBindingsLists(results, starResults);
    }
    return deduplicateBindings(results);
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
  ): Promise<RDF.Bindings[]> {
    const metadata = await this.fetchMetadata();
    if (this.smartKgPlusSource && patterns.some(pattern => hasPredicate(pattern, 'http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase'))) {
      return this.queryPatternsViaOriginalSource(patterns, context, options);
    }
    if (patterns.length >= 2 && isStarEligibleForPartitions(patterns, metadata)) {
      return this.queryPatternsViaPartitions(patterns, context, options);
    }

    return this.queryPatternsViaOriginalSource(patterns, context, options);
  }

  private async queryPatternsViaOriginalSource(
    patterns: Algebra.Pattern[],
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of patterns) {
      const stream = await this.queryFallbackBindings(pattern, context, options);
      const bindings = await this.collectBindingsFromStream(stream);
      results = await this.joinBindingsLists(results, bindings);
    }
    return deduplicateBindings(results);
  }

  private async shouldUsePartitions(pattern: Algebra.Pattern, context: IActionContext): Promise<boolean> {
    if (pattern.predicate.termType === 'Variable') {
      return false;
    }

    const starPatternCount = getStarPatternCount(context as any, pattern);
    return starPatternCount >= 2 && getShippingStrategyHint(context as any, pattern) !== 'TP-S';
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

    const candidateFamilies = selectCompatibleStarFamilies(patterns, metadata);
    if (candidateFamilies.length === 0) {
      return this.queryPatternsViaAllPartitionFamilies(patterns, metadata);
    }

    const scopedFamilies = selectStarScopedMaterializedFamilies(patterns, candidateFamilies, metadata);
    if (scopedFamilies.completeFamilies.length === 0 && scopedFamilies.partialFamilies.length === 0) {
      return this.queryPatternsViaAllPartitionFamilies(patterns, metadata);
    }

    const completeResults = await this.queryPatternsOnMaterializedFamilies(patterns, scopedFamilies.completeFamilies);
    const results = await this.queryPatternsViaPartitionFamilies(patterns, metadata, scopedFamilies.partialFamilies);
    const hybridResults = results.length === 0 ?
      await this.queryPatternsViaPartialFamilies(
        patterns,
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

    const exhaustiveResults = await this.queryPatternsViaAllPartitionFamilies(patterns, metadata);
    return exhaustiveResults.length > scopedResults.length ? exhaustiveResults : scopedResults;
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
    for (const pattern of patterns) {
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
    for (const family of families) {
      if (family.numTriples === 0 || !family.name) {
        continue;
      }
      const partitionPath = await this.fetchPartitionFile(family);
      const partitionResults = await this.evaluatePatternsOnPartition(partitionPath, patterns);
      allResults.push(...partitionResults);
    }

    return deduplicateBindings(allResults);
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
      const partitionResults = await this.queryPatternsViaPartitionFamilies(partitionPatterns, metadata, materializedFamilies);
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
      allResults.push(...results);
    }

    return deduplicateBindings(allResults);
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
    for (const family of optimalFamilies) {
      if (family.numTriples === 0 || !family.name) {
        continue;
      }
      const partitionPath = await this.fetchPartitionFile(family);
      const partitionResults = await this.evaluatePatternsOnPartition(partitionPath, [ pattern ]);
      allResults.push(...partitionResults);
    }

    return deduplicateBindings(allResults);
  }

  private async evaluatePatternsOnPartition(partitionPath: string, patterns: Algebra.Pattern[]): Promise<RDF.Bindings[]> {
    const document = await this.hdtCache.getDocument(partitionPath);
    try {
      let results: RDF.Bindings[] = [ await this.emptyBinding() ];

      for (const pattern of patterns) {
        const bindings = await this.collectHdtBindings(document, pattern);
        results = deduplicateBindings(await this.joinBindingsLists(results, bindings));
        if (results.length === 0) {
          break;
        }
      }

      return deduplicateBindings(results);
    } finally {
      this.hdtCache.releaseDocument(partitionPath);
    }
  }

  private async collectHdtBindings(document: any, pattern: Algebra.Pattern): Promise<RDF.Bindings[]> {
    const bindingsFactory = await this.getBindingsFactory();
    const pageSize = 128;
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

    return deduplicateBindings(results);
  }

  private async queryFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<BindingsStream> {
    const variables = getOperationVariables(operation);
    return new ArrayBindingsIterator(this.collectFallbackBindings(operation, context, options), variables) as unknown as BindingsStream;
  }

  private async collectFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      return [];
    }

    const fallbackContext = setContextFlag(context, 'smartkgFallback', true);
    const handledDatasets: Record<string, boolean> = {};
    const queuedLinks: string[] = [ this.originalSourceUrl ];
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
      results.push(...bindings);

      const streamNextLinks = getMetadataNextLinks(metadata);
      const nextLinks = streamNextLinks.length > 0 ? streamNextLinks : getMetadataNextLinks(sourceResult.metadata);
      for (const link of nextLinks) {
        if (!seenLinks.has(link) && !isDatasetPageLink(link, this.originalSourceUrl)) {
          queuedLinks.push(link);
        }
      }
    }

    return deduplicateBindings(results);
  }

  private async fetchMetadata(): Promise<ISmartKgMetadata> {
    if (!this.metadata) {
      const metadataString = await this.fetchText(this.metadataUrl);
      this.metadata = parseSmartKgMetadata(metadataString);
    }
    return this.metadata;
  }

  private async fetchPartitionFile(family: ISmartKgFamily): Promise<string> {
    const partitionUrl = `${this.partitionsBaseUrl}/${family.name}`;
    const partitionPath = join(this.cacheFolder, encodeURIComponent(partitionUrl));
    if (existsSync(partitionPath) && isHdtBuffer(readFileSync(partitionPath))) {
      return partitionPath;
    }

    const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: partitionUrl });
    const body = ActorHttp.toNodeReadable(response.body);
    const payload = await readStreamToBuffer(body);
    writePartitionPayload(partitionPath, payload);
    return partitionPath;
  }

  private async fetchText(url: string): Promise<string> {
    const cachePath = join(this.cacheFolder, encodeURIComponent(`${url}.json`));
    if (existsSync(cachePath)) {
      return readFileUtf8(cachePath);
    }

    const response = await this.mediatorHttp.mediate({ context: this.defaultContext, input: url });
    const body = ActorHttp.toNodeReadable(response.body);
    const content = await readStreamToString(body);
    await new Promise<void>((resolve, reject) => {
      const output = createWriteStream(cachePath, { encoding: 'utf8' });
      output.write(content, error => {
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

  private emptyBindingsStream(variables: MetadataBindings['variables']): BindingsStream {
    const iterator = new EmptyIterator<RDF.Bindings>() as any;
    if (typeof iterator.setProperty === 'function') {
      iterator.setProperty('metadata', {
        state: createMetadataValidationState(),
        cardinality: { type: 'exact', value: 0 },
        variables,
        next: [],
      } satisfies MetadataBindings);
    }
    return iterator as BindingsStream;
  }

  private async collectBindingsFromStream(stream: BindingsStream): Promise<RDF.Bindings[]> {
    const bindings: RDF.Bindings[] = [];
    for await (const binding of stream as any as AsyncIterable<RDF.Bindings>) {
      bindings.push(binding);
    }
    return bindings;
  }

  private async collectBindingsAndMetadata(
    stream: BindingsStream,
  ): Promise<{ bindings: RDF.Bindings[]; metadata?: MetadataBindings }> {
    let metadata: MetadataBindings | undefined;
    let resolveMetadata: (metadata: MetadataBindings | undefined) => void = () => {};
    const metadataPromise = new Promise<MetadataBindings | undefined>((resolve) => {
      resolveMetadata = resolve;
    });

    if (typeof (stream as any).getProperty === 'function') {
      (stream as any).getProperty('metadata', (value: MetadataBindings) => {
        metadata = value;
        resolveMetadata(value);
      });
    } else {
      resolveMetadata(undefined);
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
    const results: RDF.Bindings[] = [];
    for (const leftBinding of left) {
      const leftMap = bindingToRecord(leftBinding);
      for (const rightBinding of right) {
        const rightMap = bindingToRecord(rightBinding);
        let compatible = true;
        for (const [ key, value ] of Object.entries(rightMap)) {
          if (leftMap[key] && !leftMap[key].equals(value)) {
            compatible = false;
            break;
          }
        }
        if (!compatible) {
          continue;
        }
        results.push(bindingsFactory.fromRecord({ ...leftMap, ...rightMap }));
      }
    }
    return results;
  }
}

function normalizeUrl(url: string): string {
  return url.replace(/\/$/, '');
}

function setContextFlag(context: IActionContext, key: string, value: boolean): IActionContext {
  if (typeof (context as any).setRaw === 'function') {
    return (context as any).setRaw(key, value);
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
    const pattern = operation as Algebra.Pattern;
    if (pattern.subject.termType === 'Variable') {
      pushVariable(pattern.subject as RDF.Variable);
    }
    if (pattern.predicate.termType === 'Variable') {
      pushVariable(pattern.predicate as RDF.Variable);
    }
    if (pattern.object.termType === 'Variable') {
      pushVariable(pattern.object as RDF.Variable);
    }
    return variables;
  }

  if (isKnownOperation(operation, Algebra.Types.BGP)) {
    for (const pattern of (operation as Algebra.Bgp).patterns) {
      if (pattern.subject.termType === 'Variable') {
        pushVariable(pattern.subject as RDF.Variable);
      }
      if (pattern.predicate.termType === 'Variable') {
        pushVariable(pattern.predicate as RDF.Variable);
      }
      if (pattern.object.termType === 'Variable') {
        pushVariable(pattern.object as RDF.Variable);
      }
    }
  }

  for (const pattern of collectPatterns(operation)) {
    if (pattern.subject.termType === 'Variable') {
      pushVariable(pattern.subject as RDF.Variable);
    }
    if (pattern.predicate.termType === 'Variable') {
      pushVariable(pattern.predicate as RDF.Variable);
    }
    if (pattern.object.termType === 'Variable') {
      pushVariable(pattern.object as RDF.Variable);
    }
  }

  return variables;
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

  const operationLike = operation as Algebra.Operation & {
    input?: Algebra.Operation | Algebra.Operation[];
    patterns?: Algebra.Pattern[];
  };
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

function getMetadataNextLinks(metadata: MetadataBindings | Record<string, unknown> | undefined): string[] {
  const next = (metadata as Record<string, unknown> | undefined)?.next;
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
      decodeURIComponent(nextUrl.pathname.replace(/\/$/, '')) === decodeURIComponent(sourceUrl.pathname.replace(/\/$/, ''));
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

function hasPredicate(pattern: Algebra.Pattern, predicate: string): boolean {
  return pattern.predicate.termType !== 'Variable' && termToString(pattern.predicate) === predicate;
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
  return families.filter((family) => {
    const familyPredicates = new Set(family.predicateSet);
    return !families.some((other) => {
      if (other.index === family.index) {
        return false;
      }
      return isProperSubset(familyPredicates, new Set(other.predicateSet));
    });
  });
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
  for await (const chunk of stream as any as AsyncIterable<Buffer | string>) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function readStreamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream as any as AsyncIterable<Buffer | string>) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks);
}

function writePartitionPayload(partitionPath: string, payload: Buffer): void {
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

function isHdtBuffer(buffer: Buffer): boolean {
  return buffer.subarray(0, 4).toString('utf8') === '$HDT';
}

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

async function readFileUtf8(path: string): Promise<string> {
  const stream = createReadStream(path, { encoding: 'utf8' });
  let content = '';
  for await (const chunk of stream as any as AsyncIterable<string>) {
    content += chunk;
  }
  return content;
}
