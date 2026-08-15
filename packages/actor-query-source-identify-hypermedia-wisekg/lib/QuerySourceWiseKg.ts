/* eslint-disable import/no-nodejs-modules */
import { closeSync, existsSync, mkdirSync, openSync, readSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
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
import { BufferedIterator, EmptyIterator, WrappingIterator } from 'asynciterator';
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
const PLAN_PIPELINE_BATCH_SIZE = 256;
// A shipped HDT is scanned for every upstream batch. A somewhat larger, still
// fixed batch avoids repeating that scan dozens of times for broad first stars
// (notably Q1/Q2-shaped plans), without making memory scale with dataset size.
const PARTITION_PLAN_PIPELINE_BATCH_SIZE = 2_048;
// Preserve a small first handoff for low latency, then amortize server-side
// SPF and partition setup across a larger, still dataset-independent batch.
// Without this, broad 10M+ intermediates trigger thousands of sequential
// downstream star evaluations after producing an early first binding.
const PLAN_PIPELINE_STEADY_BATCH_SIZE = 32_768;
const EARLY_PLAN_INPUT_LIMIT = 128;
const EARLY_PLAN_OUTPUT_LIMIT = 4;
const TPF_BOUND_REQUEST_CONCURRENCY = 8;
const TPF_PIPELINE_BATCH_SIZE = 64;
const TPF_PIPELINE_STEADY_BATCH_SIZE = 2_048;
const BOUND_PATTERN_CACHE_SIZE = 1_024;
const BOUND_PATTERN_CACHE_MAX_RESULTS = 256;
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
const partitionDownloadPromises = new Map<string, Promise<string>>();

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
  private opening: Promise<void> = Promise.resolve();
  private static readonly maxDocuments = 4;

  // Load or reuse the HDT document for a partition file.
  public async getDocument(path: string): Promise<HdtDocument> {
    const existing = this.cache.get(path);
    if (existing) {
      return existing;
    }
    const documentPromise = this.opening.then(async() => (await loadHdtModule()).fromFile(path));
    this.opening = documentPromise.then(() => {}, () => {});
    this.cache.set(path, documentPromise);
    while (this.cache.size > HdtDocumentCache.maxDocuments) {
      const oldestPath = this.cache.keys().next().value;
      if (oldestPath === undefined) {
        break;
      }
      this.cache.delete(oldestPath);
    }
    return documentPromise;
  }

  // Clear cached HDT document promises when the query source is disposed.
  public async dispose(): Promise<void> {
    this.cache.clear();
  }
}

// Wraps an array-producing promise as a Comunica bindings stream.
class ArrayBindingsIterator extends WrappingIterator<RDF.Bindings> {
  public constructor(bindingsPromise: Promise<RDF.Bindings[]>, variables: MetadataBindings['variables']) {
    super(bindingsPromise);
    const state = createMetadataValidationState();
    this.setProperty('metadata', {
      state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
    bindingsPromise.then((bindings) => {
      this.setProperty('metadata', {
        state: createMetadataValidationState(),
        cardinality: { type: 'exact', value: bindings.length },
        variables,
        next: [],
      } satisfies MetadataBindings);
      state.invalidate();
    }).catch(() => {});
  }
}

// Emits final bindings as soon as the plan executor produces them.
class StreamingBindingsIterator extends BufferedIterator<RDF.Bindings> {
  private emitted = 0;
  private readonly state = createMetadataValidationState();
  private availableSlots = 128;
  private readonly slotWaiters: (() => void)[] = [];
  private started = false;

  public constructor(
    private readonly producer: (emit: (binding: RDF.Bindings) => Promise<void>) => Promise<void>,
    private readonly variables: MetadataBindings['variables'],
  ) {
    // Source selection and join planning may instantiate a bindings stream
    // only to inspect its metadata. Starting eagerly executes the complete
    // WiseKG plan twice and doubles network, CPU, and memory pressure.
    super({ autoStart: false, maxBufferSize: 128 });
    this.setProperty('metadata', {
      state: this.state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
  }

  public override _read(_count: number, done: () => void): void {
    if (!this.started) {
      this.started = true;
      this.runProducer();
    }
    done();
  }

  private runProducer(): void {
    this.producer(async(binding) => {
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
    }).catch((error: unknown) => this.destroy(<Error> error));
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
  private readonly boundPatternCache = new Map<string, Promise<RDF.Bindings[]>>();
  private bindingsFactory: BindingsFactory | undefined;

  public constructor(
    url: string,
    dataFactory: ComunicaDataFactory,
    mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>, IActionHttp, IActorTest, IActorHttpOutput>,
    context: IActionContext,
    mediatorQuerySourceDereferenceLink?: any,
  ) {
    const normalizedUrl = normalizeUrl(url);
    const sourceLocation = new URL(normalizedUrl);
    const datasetName = sourceLocation.pathname.split('/').filter(Boolean).at(-1);
    if (!datasetName) {
      throw new Error(`WiseKG source URL must identify a dataset path: ${url}`);
    }
    const origin = sourceLocation.origin;

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
    return <BindingsStream> <unknown> new StreamingBindingsIterator(
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
    emit: (binding: RDF.Bindings) => Promise<void>,
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
      await emit(binding);
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
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    let steps = [ ...fetchedPlan.steps ];
    if (fetchedPlan.expiresAt && Date.now() > fetchedPlan.expiresAt) {
      const newPlan = await this.fetchWiseKgPlan(originalPatterns, context);
      if (newPlan) {
        steps = [ ...newPlan.steps ];
      }
    }

    const earlyBindings = await this.emitEarlyPlanResults(steps, context, options, emit);
    const emitRemaining = async(binding: RDF.Bindings): Promise<void> => {
      if (!earlyBindings.delete(bindingKey(binding))) {
        await emit(binding);
      }
    };
    await this.streamWiseKgPlanSteps(
      steps,
      0,
      [ await this.emptyBinding() ],
      context,
      options,
      emitRemaining,
    );
  }

  // Produce a few valid answers without waiting for the complete first plan
  // star. The normal executor still runs afterwards and remains responsible
  // for completeness; emitted early bindings are suppressed once there.
  private async emitEarlyPlanResults(
    steps: IWiseKgExecutableStep[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<Set<string>> {
    const emitted = new Set<string>();
    const firstStep = steps[0];
    if (!firstStep || steps.length < 2 ||
      this.resolveControlUrl(firstStep.control) !== this.originalSourceUrl) {
      return emitted;
    }

    try {
      const firstPatterns = this.wiseKgStarToPatterns(firstStep.star);
      const firstStream = await this.queryServerStarBindings(
        firstPatterns,
        [ await this.emptyBinding() ],
        firstStep.control,
        context,
        options,
      );
      let results: RDF.Bindings[] = [];
      for await (const binding of <AsyncIterable<RDF.Bindings>> <any> firstStream) {
        results.push(binding);
        if (results.length >= EARLY_PLAN_INPUT_LIMIT) {
          firstStream.destroy();
          break;
        }
      }

      for (const step of steps.slice(1)) {
        if (results.length === 0) {
          return emitted;
        }
        results = await this.evaluateWiseKgStep(
          step,
          this.wiseKgStarToPatterns(step.star),
          results,
          context,
          options,
        );
      }

      for (const binding of results.slice(0, EARLY_PLAN_OUTPUT_LIMIT)) {
        emitted.add(bindingKey(binding));
        await emit(binding);
      }
    } catch (error) {
      this.logDebug(context, 'WiseKG early-result lane failed; continuing with complete execution.', { error });
    }
    return emitted;
  }

  // Pipe intermediate plan bindings forward in bounded batches. This keeps
  // the first result incremental and prevents a large early star from being
  // retained in full before evaluation of the next star begins.
  private async streamWiseKgPlanSteps(
    steps: IWiseKgExecutableStep[],
    index: number,
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const step = steps[index];
    if (!step || inputBindings.length === 0) {
      return;
    }
    const starPatterns = this.wiseKgStarToPatterns(step.star);
    if (index === steps.length - 1) {
      await this.streamWiseKgStep(step, starPatterns, inputBindings, context, options, emit);
      return;
    }

    const nextStep = steps[index + 1];
    let pipelineBatchSize = nextStep && this.isPartitionShippingControl(nextStep.control) ?
      PARTITION_PLAN_PIPELINE_BATCH_SIZE :
      PLAN_PIPELINE_BATCH_SIZE;
    let batch: RDF.Bindings[] = [];
    await this.streamWiseKgStep(step, starPatterns, inputBindings, context, options, async(binding) => {
      batch.push(binding);
      if (batch.length >= pipelineBatchSize) {
        const nextBatch = batch;
        batch = [];
        await this.streamWiseKgPlanSteps(steps, index + 1, nextBatch, context, options, emit);
        pipelineBatchSize = PLAN_PIPELINE_STEADY_BATCH_SIZE;
      }
    });
    if (batch.length > 0) {
      await this.streamWiseKgPlanSteps(steps, index + 1, batch, context, options, emit);
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
    if (step.control === 'tpf') {
      const results: RDF.Bindings[] = [];
      await this.streamPatternsViaTpf(starPatterns, inputBindings, context, options, async(binding) => {
        results.push(binding);
      });
      return this.deduplicateBindings(results);
    }

    if (this.isPartitionShippingControl(step.control)) {
      const stepResults: RDF.Bindings[] = [];
      for (const control of this.splitControls(step.control)) {
        const partitionUrl = this.getPartitionHdtUrlForControl(control);
        this.logDebug(context, `WiseKG downloading/evaluating partition ${partitionUrl}`);
        const partitionPath = await this.fetchPartitionFileByControl(control);
        stepResults.push(...await this.evaluatePatternsOnPartition(partitionPath, starPatterns));
      }
      const localResults = await this.joinBindingsLists(inputBindings, stepResults);
      return this.deduplicateBindings(localResults);
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
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const seen = new Set<string>();
    const emitOnce = async(binding: RDF.Bindings): Promise<void> => {
      const key = bindingKey(binding);
      if (!seen.has(key)) {
        seen.add(key);
        await emit(binding);
      }
    };

    if (step.control === 'tpf') {
      // A single TPF pipeline produces every solution once for an HDT-backed
      // dataset. Do not retain an output-sized Set merely to deduplicate it:
      // C3 already has 400K answers at 10M and this must remain viable at
      // 100M+ scale.
      await this.streamPatternsViaTpf(starPatterns, inputBindings, context, options, emit);
      return;
    }

    if (this.isPartitionShippingControl(step.control)) {
      for (const control of this.splitControls(step.control)) {
        const partitionUrl = this.getPartitionHdtUrlForControl(control);
        this.logDebug(context, `WiseKG downloading/evaluating partition ${partitionUrl}`);
        const partitionPath = await this.fetchPartitionFileByControl(control);
        await this.streamPatternsOnPartition(partitionPath, starPatterns, inputBindings, emitOnce);
      }
      return;
    }

    if (this.isServerSourceControl(step.control)) {
      const sourceUrl = this.resolveControlUrl(step.control);
      if (sourceUrl === this.originalSourceUrl) {
        if (inputBindings.length === 1 && inputBindings[0].size === 0) {
          // SPF is still the efficient representation for an unbound anchor
          // star: the server can intersect its patterns directly. The paging
          // defect only appears once join bindings are supplied.
          const stream = await this.queryServerStarBindings(
            starPatterns,
            inputBindings,
            step.control,
            context,
            options,
          );
          for await (const binding of <AsyncIterable<RDF.Bindings>> <any> stream) {
            await emitOnce(binding);
          }
          return;
        }

        // The WiseKG/SPF representation paginates a binding-restricted star
        // as if it were unbound on 10M+ datasets. One input can consequently
        // crawl hundreds of full predicate pages. Keep the server-provided
        // star order, but evaluate root controls through complete TPF lookups,
        // as SmartKG+ does for its root-controlled plan steps.
        for (const binding of await this.queryStarViaOriginalSourceWithRetry(
          starPatterns,
          context,
          options,
          inputBindings,
        )) {
          await emit(binding);
        }
        return;
      }

      if (this.isServerPartitionControl(step.control)) {
        const stream = await this.queryServerStarBindings(
          starPatterns,
          inputBindings,
          step.control,
          context,
          options,
        );
        for await (const binding of <AsyncIterable<RDF.Bindings>> <any> stream) {
          await emitOnce(binding);
        }
        if (this.isServerPartitionControl(step.control)) {
          for (const binding of await this.queryOriginalStarWithInput(starPatterns, inputBindings, context, options)) {
            await emitOnce(binding);
          }
        }
        return;
      }

      if (await this.shouldUseUnboundLocalJoin(starPatterns, inputBindings, context)) {
        for (const binding of await this.queryStarViaControlledSource(
          starPatterns,
          inputBindings,
          step.control,
          context,
          options,
        )) {
          await emitOnce(binding);
        }
      } else {
        const stream = await this.queryServerStarBindings(
          starPatterns,
          inputBindings,
          step.control,
          context,
          options,
        );
        for await (const binding of <AsyncIterable<RDF.Bindings>> <any> stream) {
          await emitOnce(binding);
        }
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
    const controls = this.splitControls(control);
    return controls.length > 0 && controls.every(part =>
      part.includes('/molecule/') || part.startsWith('molecule/') || part.endsWith('.hdt'));
  }

  private splitControls(control: string): string[] {
    return control.split('|').map(part => part.trim()).filter(Boolean);
  }

  // Dataset and /partition controls are evaluated by their server-side fragment interface.
  private isServerSourceControl(control: string): boolean {
    return control === 'wisekg' || control === 'smartkg+' || control.includes('/partition/') ||
      control.startsWith('partition/') || normalizeUrl(control) === this.originalSourceUrl;
  }

  // Evaluate a fallback star as a bounded, binding-restricted TPF pipeline.
  // This is used only when normal-family partitions are incomplete and the
  // plan consists of a single star, where the server's compound SPF iterator
  // can be more expensive than incremental triple-pattern evaluation.
  private async streamPatternsViaTpf(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    const ordered = (await Promise.all(patterns.map(async pattern => ({
      pattern,
      connected: this.countBoundTerms(pattern, inputBindings) > 0,
      cardinality: await this.estimateOriginalSourcePatternCardinality(pattern, context),
    }))))
      // A pattern connected to incoming bindings must be the anchor even when
      // an unrelated predicate has a smaller global cardinality. Otherwise a
      // selective F3-style join degenerates into a complete predicate scan.
      .sort((left, right) => Number(right.connected) - Number(left.connected) ||
        left.cardinality - right.cardinality)
      .map(entry => entry.pattern);

    const evaluate = async(index: number, bindings: RDF.Bindings[]): Promise<void> => {
      if (bindings.length === 0) {
        return;
      }
      const results = await this.queryOriginalSourcePatternWithBindings(
        ordered[index],
        bindings,
        context,
        options,
      );
      if (index === ordered.length - 1) {
        for (const binding of results) {
          await emit(binding);
        }
        return;
      }

      for (let offset = 0; offset < results.length; offset += TPF_PIPELINE_BATCH_SIZE) {
        await evaluate(index + 1, results.slice(offset, offset + TPF_PIPELINE_BATCH_SIZE));
      }
    };

    if (ordered.length === 0) {
      return;
    }

    // Stream an unbound anchor page-by-page instead of collecting its whole
    // predicate fragment. Only a fixed-size handoff is retained, after which
    // later patterns use bounded subject/object lookups.
    if (inputBindings.length === 1 && inputBindings[0].size === 0) {
      let anchorBatch: RDF.Bindings[] = [];
      let anchorBatchSize = TPF_PIPELINE_BATCH_SIZE;
      await this.streamFallbackBindings(
        ordered[0],
        context,
        options,
        this.originalSourceUrl,
        async(binding) => {
          if (ordered.length === 1) {
            await emit(binding);
            return;
          }
          anchorBatch.push(binding);
          if (anchorBatch.length >= anchorBatchSize) {
            const nextBatch = anchorBatch;
            anchorBatch = [];
            anchorBatchSize = TPF_PIPELINE_STEADY_BATCH_SIZE;
            await evaluate(1, nextBatch);
          }
        },
      );
      if (anchorBatch.length > 0) {
        await evaluate(1, anchorBatch);
      }
      return;
    }

    await evaluate(0, inputBindings);
  }

  // Download or reuse the local HDT file for a partition control.
  private async fetchPartitionFileByControl(control: string): Promise<string> {
    const partitionUrl = this.getPartitionHdtUrlForControl(control);
    const partitionPath = join(this.cacheFolder, encodeURIComponent(partitionUrl));
    removeEmptyFile(`${partitionPath}.index.v1-1`);
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
        const indexPayload = await readStreamToBuffer(indexBody);
        if (indexPayload.length > 0) {
          writeFileSync(`${partitionPath}.index.v1-1`, <any> indexPayload);
        }
      }
    }
    return partitionPath;
  }

  // Persist a partition payload, extracting HDT files from tar payloads when needed.
  private writePartitionPayload(partitionPath: string, payload: Buffer): void {
    if (isHdtBuffer(payload)) {
      writeFileSync(partitionPath, <any> payload);
      return;
    }

    const entries = extractTarEntries(payload);
    const hdtEntry = entries.find(entry => entry.name.endsWith('.hdt') && !entry.name.endsWith('.index.v1-1'));
    if (!hdtEntry) {
      writeFileSync(partitionPath, <any> payload);
      return;
    }

    writeFileSync(partitionPath, <any> hdtEntry.payload);
    const indexEntry = entries.find(entry => entry.name === `${hdtEntry.name}.index.v1-1`);
    if (indexEntry && indexEntry.payload.length > 0) {
      writeFileSync(`${partitionPath}.index.v1-1`, <any> indexEntry.payload);
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

  // Evaluate all but the final local pattern eagerly, then page the final HDT
  // lookup into the join. This mirrors SmartKG+'s first-result path: shipped
  // partitions remain the complete source of truth, while the client no
  // longer waits for the complete last HDT scan before exposing one result.
  private async streamPatternsOnPartition(
    partitionPath: string,
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    if (patterns.length === 0 || inputBindings.length === 0) {
      return;
    }

    const document = await this.hdtCache.getDocument(partitionPath);
    // Build the star inside the partition before applying upstream bindings.
    // Applying them before the star reaches its shared variable can create a
    // massive temporary Cartesian product (for example, rating bindings only
    // meet the item star at rev:hasReview).
    let results: RDF.Bindings[] = [ await this.emptyBinding() ];
    for (const pattern of patterns.slice(0, -1)) {
      const bindings = await this.collectCachedHdtBindings(partitionPath, document, pattern);
      results = this.deduplicateBindings(await this.joinBindingsLists(results, bindings));
      if (results.length === 0) {
        return;
      }
    }

    const bindingsFactory = await this.getBindingsFactory();
    const finalPattern = patterns.at(-1)!;
    const pageSize = 16_384;
    let offset = 0;
    while (true) {
      const result = await document.searchBindings(
        bindingsFactory,
        finalPattern.subject,
        finalPattern.predicate,
        finalPattern.object,
        { offset, limit: pageSize },
      );
      const page: RDF.Bindings[] = Array.isArray(result?.bindings) ? result.bindings : [];
      if (page.length > 0) {
        const partitionPage = await this.joinBindingsLists(results, page);
        await this.emitJoinedBindings(inputBindings, partitionPage, emit);
      }
      if (page.length < pageSize) {
        return;
      }
      offset += page.length;
    }
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
    while (this.hdtPatternCache.size > 8) {
      const oldestKey = this.hdtPatternCache.keys().next().value;
      if (oldestKey === undefined) {
        break;
      }
      this.hdtPatternCache.delete(oldestKey);
    }
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

    // A concrete term is a selective anchor. Scanning every other predicate
    // unbound can turn a handful of indexed lookups into millions of triples
    // (notably F1-F3 at 10M). Let the adaptive bound pipeline start at that
    // anchor instead. The all-variable C3-style stars still use the one-scan
    // local join path to avoid tens of thousands of HTTP lookups.
    if (patterns.some(pattern =>
      pattern.subject.termType !== 'Variable' || pattern.object.termType !== 'Variable')) {
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
    allowUnboundScan = true,
  ): Promise<RDF.Bindings[]> {
    if (allowUnboundScan && inputBindings.length > 0 &&
      await this.shouldUseUnboundLocalJoin([ pattern ], inputBindings, context)) {
      return this.queryPatternViaStreamingUnboundJoin(pattern, inputBindings, context, options, sourceUrl);
    }

    const grouped = new Map<string, { pattern: Algebra.Pattern; inputBindings: RDF.Bindings[] }>();
    for (const inputBinding of inputBindings) {
      const boundPattern = this.bindPattern(pattern, inputBinding);
      const key = patternKey(boundPattern);
      const group = grouped.get(key) ?? { pattern: boundPattern, inputBindings: []};
      group.inputBindings.push(inputBinding);
      grouped.set(key, group);
    }

    const results: RDF.Bindings[] = [];
    const groups = [ ...grouped.values() ];
    for (let offset = 0; offset < groups.length; offset += TPF_BOUND_REQUEST_CONCURRENCY) {
      const joinedGroups = await Promise.all(groups
        .slice(offset, offset + TPF_BOUND_REQUEST_CONCURRENCY)
        .map(async(group) => {
          const bindings = await this.queryBoundPatternCached(group.pattern, context, options, sourceUrl);
          return this.joinBindingsLists(group.inputBindings, bindings);
        }));
      for (const joined of joinedGroups) {
        for (const binding of joined) {
          results.push(binding);
        }
      }
    }

    return this.deduplicateBindings(results);
  }

  // Scan a broad tail fragment once and probe a bounded upstream hash table.
  // Unlike the previous local-join path, fragment bindings are consumed one
  // page at a time and never accumulated in a dataset-sized array.
  private async queryPatternViaStreamingUnboundJoin(
    pattern: Algebra.Pattern,
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    sourceUrl: string,
  ): Promise<RDF.Bindings[]> {
    const inputRecords = inputBindings.map(bindingToRecord);
    const patternVariables = [ pattern.subject, pattern.predicate, pattern.object ]
      .filter((term): term is RDF.Variable => term.termType === 'Variable')
      .map(term => term.value);
    const sharedVariables = patternVariables.filter(variable =>
      inputRecords.every(record => record[variable] !== undefined));
    if (sharedVariables.length === 0) {
      return this.queryOriginalSourcePatternWithBoundRequests(pattern, inputBindings, context, options, sourceUrl);
    }

    const index = new Map<string, Record<string, RDF.Term>[]>();
    for (const record of inputRecords) {
      const key = joinKey(record, sharedVariables);
      const bucket = index.get(key);
      if (bucket) {
        bucket.push(record);
      } else {
        index.set(key, [ record ]);
      }
    }

    const bindingsFactory = await this.getBindingsFactory();
    const results: RDF.Bindings[] = [];
    await this.streamFallbackBindings(pattern, context, options, sourceUrl, async(binding) => {
      const sourceRecord = bindingToRecord(binding);
      const candidates = index.get(joinKey(sourceRecord, sharedVariables));
      if (candidates) {
        for (const candidate of candidates) {
          if (areBindingRecordsCompatible(candidate, sourceRecord)) {
            results.push(bindingsFactory.fromRecord({ ...candidate, ...sourceRecord }));
          }
        }
      }
    });
    return results;
  }

  private async queryOriginalSourcePatternWithBoundRequests(
    pattern: Algebra.Pattern,
    inputBindings: RDF.Bindings[],
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    sourceUrl: string,
  ): Promise<RDF.Bindings[]> {
    return this.queryOriginalSourcePatternWithBindings(pattern, inputBindings, context, options, sourceUrl, false);
  }

  // Reuse nearby subject/object lookups across bounded pipeline batches.
  // Star products often split combinations for one subject over multiple
  // batches, which otherwise repeats the same HTTP request many times. Both
  // entry count and per-entry result count are capped, so memory does not grow
  // with dataset cardinality.
  private async queryBoundPatternCached(
    pattern: Algebra.Pattern,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    sourceUrl: string,
  ): Promise<RDF.Bindings[]> {
    const key = `${sourceUrl}|${patternKey(pattern)}`;
    const cached = this.boundPatternCache.get(key);
    if (cached) {
      this.boundPatternCache.delete(key);
      this.boundPatternCache.set(key, cached);
      return cached;
    }

    const promise = (async(): Promise<RDF.Bindings[]> => {
      const stream = await this.queryFallbackBindings(pattern, context, options, sourceUrl);
      return this.collectBindingsFromStream(stream);
    })();
    this.boundPatternCache.set(key, promise);
    while (this.boundPatternCache.size > BOUND_PATTERN_CACHE_SIZE) {
      const oldestKey = this.boundPatternCache.keys().next().value;
      if (oldestKey !== undefined) {
        this.boundPatternCache.delete(oldestKey);
      }
    }

    try {
      const bindings = await promise;
      if (bindings.length > BOUND_PATTERN_CACHE_MAX_RESULTS) {
        this.boundPatternCache.delete(key);
      }
      return bindings;
    } catch (error) {
      this.boundPatternCache.delete(key);
      throw error;
    }
  }

  private async queryStarViaControlledSource(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    control: string,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<RDF.Bindings[]> {
    const sourceUrl = this.resolveControlUrl(control);
    if (sourceUrl === this.originalSourceUrl) {
      return this.queryStarViaOriginalSourceWithRetry(patterns, context, options, inputBindings);
    }

    // A WiseKG server-side control denotes SPF evaluation, both for the
    // complete dataset and for a selected partition. Evaluating a root
    // control as a sequence of TPF requests loses the central benefit of the
    // server-side branch of the WiseKG plan and creates large intermediates.
    return this.collectBindingsFromStream(
      await this.queryServerStarBindings(patterns, inputBindings, control, context, options),
    );
  }

  private async shouldUseUnboundLocalJoin(
    patterns: Algebra.Pattern[],
    inputBindings: RDF.Bindings[],
    context: IActionContext,
  ): Promise<boolean> {
    if (patterns.length !== 1 || inputBindings.length === 0) {
      return false;
    }

    // Match SmartKG(+)'s request-cost decision. At 10M+ scale a fixed
    // cardinality ceiling is counterproductive: a common predicate can exceed
    // that ceiling while one lookup per incoming binding is still an order of
    // magnitude more expensive than scanning the fragment once and hash
    // joining it locally. This is especially important for the one-pattern
    // tail stars in Q1-Q4 and Q6.
    const pattern = patterns[0];
    const distinctBoundPatterns = new Set<string>();
    let subjectBound = false;
    let objectBound = false;
    for (const inputBinding of inputBindings) {
      const boundPattern = this.bindPattern(pattern, inputBinding);
      distinctBoundPatterns.add(patternKey(boundPattern));
      subjectBound ||= pattern.subject.termType === 'Variable' &&
        boundPattern.subject.termType !== 'Variable';
      objectBound ||= pattern.object.termType === 'Variable' &&
        boundPattern.object.termType !== 'Variable';
    }

    if (!subjectBound && !objectBound) {
      return true;
    }

    const cardinality = await this.estimateOriginalSourcePatternCardinality(patterns[0], context);
    const boundLookupCost = distinctBoundPatterns.size * (subjectBound ? 10 : 100);
    return Number.isFinite(cardinality) && cardinality <= boundLookupCost;
  }

  // Complete a planned star through the dataset source. Some generated
  // molecule and SPF controls cover only a subset of the matching families.
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

  private async queryServerStarBindings(
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
    const results: RDF.Bindings[] = [];
    await this.streamFallbackBindings(operation, context, options, sourceUrl, async(binding) => {
      results.push(binding);
    });
    return this.deduplicateBindings(results);
  }

  // Traverse fallback pages while retaining at most one fragment page. This
  // is the bounded counterpart of collectFallbackBindings for broad anchors.
  private async streamFallbackBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
    sourceUrl: string,
    emit: (binding: RDF.Bindings) => Promise<void>,
  ): Promise<void> {
    if (!this.mediatorQuerySourceDereferenceLink) {
      return;
    }

    const fallbackContext = setContextFlag(context, KEY_CONTEXT_WISEKG_FALLBACK, true);
    const handledDatasets: Record<string, boolean> = {};
    const queuedLinks: string[] = [ sourceUrl ];
    const seenLinks = new Set<string>();

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
      for (const binding of bindings) {
        await emit(binding);
      }

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
  return content + decoder.decode(<any> undefined);
}

// Collect a readable stream into a Buffer.
async function readStreamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of <AsyncIterable<Uint8Array | string>> <any> stream) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : Buffer.from(chunk));
  }
  return Buffer.concat(<any> chunks);
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
    return readSync(fd, <any> header, 0, 4, 0) === 4 && isHdtBuffer(header);
  } finally {
    closeSync(fd);
  }
}

function removeEmptyFile(path: string): void {
  if (existsSync(path) && statSync(path).size === 0) {
    unlinkSync(path);
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
