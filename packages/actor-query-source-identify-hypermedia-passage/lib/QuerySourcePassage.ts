import type { BindMethod } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import { QuerySourceSparql } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import type { MediatorHttp } from '@comunica/bus-http';
import type { MediatorQueryProcess } from '@comunica/bus-query-process';
import type { MediatorQuerySerialize } from '@comunica/bus-query-serialize';
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContextKey, Actor } from '@comunica/core';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IPhysicalQueryPlanLogger,
  IQueryBindingsOptions,
  IQuerySource,
  MetadataVariable,
  QueryResultCardinality,
} from '@comunica/types';
import type { AlgebraFactory } from '@comunica/utils-algebra';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { EmptyIterator, TransformIterator, wrap } from 'asynciterator';
import { SparqlEndpointFetcher } from 'fetch-sparql-endpoint';
import { LRUCache } from 'lru-cache';
import { Shapes } from './Shapes';

/**
 * Query source for Passage continuation-query endpoints.
 */
export class QuerySourcePassage implements IQuerySource {
  protected static readonly SELECTOR_SHAPE: FragmentSelectorShape = getDefaultSelectorShape();

  public readonly referenceValue: string;
  private readonly url: string;
  private readonly context: IActionContext;
  private readonly mediatorHttp: MediatorHttp;
  private readonly mediatorQuerySerialize: MediatorQuerySerialize;
  private readonly bindMethod: BindMethod;
  private readonly countTimeout: number;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly algebraFactory: AlgebraFactory;
  private readonly bindingsFactory: BindingsFactory;
  private readonly mediatorQueryProcess: MediatorQueryProcess;
  private readonly endpointFetcher: SparqlEndpointFetcher;
  private readonly cache: LRUCache<string, QueryResultCardinality> | undefined;

  private lastSourceContext: IActionContext | undefined;

  public constructor(
    url: string,
    context: IActionContext,
    mediatorHttp: MediatorHttp,
    mediatorQuerySerialize: MediatorQuerySerialize,
    bindMethod: BindMethod,
    dataFactory: ComunicaDataFactory,
    algebraFactory: AlgebraFactory,
    bindingsFactory: BindingsFactory,
    forceHttpGet: boolean,
    cacheSize: number,
    countTimeout: number,
    mediatorQueryProcess: MediatorQueryProcess,
  ) {
    this.mediatorQueryProcess = mediatorQueryProcess;
    this.referenceValue = url;
    this.url = url;
    this.context = context;
    this.mediatorHttp = mediatorHttp;
    this.mediatorQuerySerialize = mediatorQuerySerialize;
    this.bindMethod = bindMethod;
    this.dataFactory = dataFactory;
    this.algebraFactory = algebraFactory;
    this.bindingsFactory = bindingsFactory;
    this.endpointFetcher = new SparqlEndpointFetcher({
      method: forceHttpGet ? 'GET' : 'POST',
      fetch: (input: Request | string, init?: RequestInit) => this.mediatorHttp.mediate({
        input,
        init,
        context: this.lastSourceContext!,
      }),
      prefixVariableQuestionMark: true,
      dataFactory,
    });
    this.cache = cacheSize > 0 ?
      new LRUCache<string, QueryResultCardinality>({ max: cacheSize }) :
      undefined;
    this.countTimeout = countTimeout;
  }

  public async getFilterFactor(_context: IActionContext): Promise<number> {
    return 1;
  }

  // Select the operator shape from context overrides or the configured default.
  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    const shapeKey = new ActionContextKey<FragmentSelectorShape>('shape');
    return context.get(shapeKey) ?? this.context.get(shapeKey) ?? QuerySourcePassage.SELECTOR_SHAPE;
  }

  // Serialize the algebra operation and start a Passage bindings stream.
  public queryBindings(
    operationIn: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    const operationPromise: Promise<Algebra.Operation> = options?.joinBindings ?
      QuerySourceSparql.addBindingsToOperation(
        this.algebraFactory,
        this.bindMethod,
        operationIn,
        options.joinBindings,
      ) :
      Promise.resolve(operationIn);

    const bindings: BindingsStream = new TransformIterator(async() => {
      const operation = await operationPromise;
      const selectQuery = await this.toSelectQuery(context, operation, options);
      const variables = algebraUtils.inScopeVariables(operation);
      const undefVariables = QuerySourceSparql.getOperationUndefs(operation);

      Actor.getContextLogger(context)?.info(`Asking for:\n${selectQuery}`, {});

      return this.queryBindingsRemote(operation, this.url, selectQuery, variables, context, undefVariables);
    }, { autoStart: false });

    this.attachMetadata(bindings, context, operationPromise, options);

    return bindings;
  }

  // Attach early metadata before remote results or continuations are available.
  protected attachMetadata(
    target: AsyncIterator<any>,
    context: IActionContext,
    operationPromise: Promise<Algebra.Operation>,
    options?: IQueryBindingsOptions,
  ): void {
    operationPromise.then(async(operation) => {
      const undefVariables = QuerySourceSparql.getOperationUndefs(operation);
      const variablesScoped = algebraUtils.inScopeVariables(operation);
      const variablesCount: MetadataVariable[] = variablesScoped.map(variable => ({
        variable,
        canBeUndef: undefVariables.some(undefVariable => undefVariable.equals(variable)),
      }));
      const selectQuery = await this.toSelectQuery(context, operation, options);

      target.setProperty('metadata', {
        state: new MetadataValidationState(),
        cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
        variables: variablesCount,
        query: selectQuery,
      });

      QuerySourcePassage.initializeLogger(context, operation, selectQuery);
      context = context.set(KeysInitQuery.physicalQueryPlanNode, operation);
    }).catch(error => target.destroy(error));
  }

  // Execute one Passage request and append continuation requests when present.
  public async queryBindingsRemote(
    operation: Algebra.Operation,
    endpoint: string,
    query: string,
    variables: RDF.Variable[],
    context: IActionContext,
    undefVariables: RDF.Variable[],
  ): Promise<BindingsStream> {
    QuerySourcePassage.updateStartTime(context, operation);
    const undefVariablesIndex = new Set(undefVariables.map(variable => variable.value));

    const shouldStopShared: any = context.get(new ActionContextKey('abort'));
    if (shouldStopShared?.value) {
      QuerySourcePassage.logError(context, operation, 'The query has been aborted early.');
      return <BindingsStream> new EmptyIterator<RDF.Bindings>();
    }

    this.lastSourceContext = this.context.merge(context);
    let rawStream;
    try {
      rawStream = await this.endpointFetcher.fetchBindings(endpoint, query)
        .then((stream) => {
          QuerySourcePassage.updateFirstResultTime(context, operation);
          return stream;
        })
        .catch((error: Error) => {
          QuerySourcePassage.logError(context, operation, error.message);
          throw error;
        });
    } finally {
      this.lastSourceContext = undefined;
    }

    if (!rawStream) {
      throw new Error(`The Passage endpoint ${endpoint} did not return a bindings stream.`);
    }

    const iterator = wrap<any>(rawStream, { autoStart: false, maxBufferSize: Number.POSITIVE_INFINITY })
      .map<RDF.Bindings>((rawData: Record<string, RDF.Term>) => {
        const nbResults: number = iterator.getProperty('nbResults') ?? 0;
        iterator.setProperty('nbResults', nbResults + 1);

        return this.bindingsFactory.bindings(
          variables
            .map((variable) => {
              const value = rawData[`?${variable.value}`];
              if (!undefVariablesIndex.has(variable.value) && !value) {
                Actor.getContextLogger(this.context)
                  ?.warn(`The endpoint ${endpoint} failed to provide a binding for ${variable.value}.`);
              }
              return <[RDF.Variable, RDF.Term]>[ variable, value ];
            })
            .filter(([ , value ]) => Boolean(value)),
        );
      });

    const nextPromise: Promise<string | void> = new Promise((resolve, reject) => {
      rawStream.on('metadata', (metadata: { next?: string }) => {
        if (metadata.next) {
          Actor.getContextLogger(this.context)?.info(`Next query to get complete result:\n${metadata.next}`, {});
        }
        resolve(metadata.next);
      });
      rawStream.on('end', () => {
        resolve();
      });
      iterator.on('error', (error: Error) => {
        QuerySourcePassage.logError(context, operation, error.message);
        reject(error);
      });
      iterator.on('end', () => {
        QuerySourcePassage.updateDoneTime(context, operation);
        QuerySourcePassage.updateNbResults(context, operation, iterator.getProperty('nbResults') ?? 0);
      });
    });

    const nextIterator: BindingsStream = wrap(nextPromise.then(async(next) => {
      if (!next) {
        nextIterator.close();
        return new EmptyIterator<RDF.Bindings>();
      }

      const nextOperation = this.algebraFactory.createService(
        operation,
        this.dataFactory.namedNode(endpoint),
        false,
      );
      QuerySourcePassage.initializeLogger(context, nextOperation, next);

      const shouldStopShared: any = context.get(new ActionContextKey('abort'));
      if (shouldStopShared?.value) {
        QuerySourcePassage.logError(context, nextOperation, 'The query has been aborted early.');
        nextIterator.close();
        return new EmptyIterator<RDF.Bindings>();
      }

      // Passage continuations are endpoint-owned SPARQL queries, so querying the
      // same source recursively avoids re-running the whole Comunica pipeline.
      return this.queryBindingsRemote(nextOperation, endpoint, next, variables, context, undefVariables);
    }), { autoStart: false });

    return iterator.append(nextIterator);
  }

  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in QuerySourcePassage.');
  }

  public async queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourcePassage.');
  }

  public async queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourcePassage.');
  }

  public toString(): string {
    return `QuerySourcePassage(${this.url})`;
  }

  // Convert an operation to the SELECT query that Passage should execute.
  private async toSelectQuery(
    context: IActionContext,
    operation: Algebra.Operation,
    options?: IQueryBindingsOptions,
  ): Promise<string> {
    const variables = algebraUtils.inScopeVariables(operation);
    const queryString = context.get<string>(KeysInitQuery.queryString);
    const queryFormat = context.get<RDF.QueryFormat>(KeysInitQuery.queryFormat);
    if (!options?.joinBindings && queryString && queryFormat?.language === 'sparql') {
      return queryString;
    }
    if (operation.type === Algebra.Types.PROJECT || operation.type === Algebra.Types.SLICE) {
      return this.operationToQuery(operation);
    }
    return this.operationToSelectQuery(operation, variables);
  }

  // Wrap non-project algebra in a projection over the in-scope variables.
  private operationToSelectQuery(operation: Algebra.Operation, variables: RDF.Variable[]): Promise<string> {
    return this.operationToQuery(this.algebraFactory.createProject(operation, variables));
  }

  // Use Comunica serialization to convert algebra back into SPARQL.
  private async operationToQuery(operation: Algebra.Operation): Promise<string> {
    return (await this.mediatorQuerySerialize.mediate({
      queryFormat: { language: 'sparql', version: '1.2' },
      operation,
      newlines: false,
      indentWidth: 0,
      context: this.context,
    })).query;
  }

  // Create a physical query-plan node for a Passage remote request.
  public static initializeLogger(context: IActionContext, operation: Algebra.Operation, query: string): void {
    const physicalQueryPlanLogger = context.get(KeysInitQuery.physicalQueryPlanLogger);
    if (!physicalQueryPlanLogger) {
      return;
    }

    physicalQueryPlanLogger.logOperation(
      Algebra.Types.SERVICE,
      undefined,
      operation,
      context.get(KeysInitQuery.physicalQueryPlanNode),
      'passage',
      {
        variables: algebraUtils.inScopeVariables(operation).map(variable => variable.value),
        query,
      },
    );
  }

  // Record when the remote Passage request starts.
  public static updateStartTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.startAt = Date.now();
      physicalQueryPlanLogger.appendMetadata(operation, { startAt: node.startAt });
    }
  }

  // Record time to first result for benchmark timing output.
  public static updateFirstResultTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.firstResultAt = Date.now();
      node.timeFirstResult = node.firstResultAt - node.startAt;
      physicalQueryPlanLogger.appendMetadata(operation, { firstResultAt: node.firstResultAt });
      physicalQueryPlanLogger.appendMetadata(operation, { timeFirstResult: node.timeFirstResult });
    }
  }

  // Record completion time for benchmark timing output.
  public static updateDoneTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.doneAt = Date.now();
      node.timeLife = node.doneAt - node.startAt;
      physicalQueryPlanLogger.appendMetadata(operation, { doneAt: node.doneAt });
      physicalQueryPlanLogger.appendMetadata(operation, { timeLife: node.timeLife });
    }
  }

  // Store the final number of returned bindings in the query-plan logger.
  public static updateNbResults(context: IActionContext, operation: Algebra.Operation, nbResults: number): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.cardinalityReal = nbResults;
      physicalQueryPlanLogger.appendMetadata(operation, { cardinalityReal: nbResults });
    }
  }

  // Mark a Passage plan node as failed with the provided message.
  public static logError(context: IActionContext, operation: Algebra.Operation, message: string): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      physicalQueryPlanLogger.appendMetadata(operation, { status: 'error', message });
    }
  }

  // Find the physical query-plan logger across supported context keys.
  private static getPhysicalQueryPlanLogger(context: IActionContext): IPhysicalQueryPlanLogger | undefined {
    return context.get(KeysInitQuery.physicalQueryPlanLogger) ??
      context.get(new ActionContextKey<IPhysicalQueryPlanLogger>('physicalQueryPlanLogger'));
  }
}

// Resolve the Passage selector shape from the SHAPE environment override.
function getDefaultSelectorShape(): FragmentSelectorShape {
  if (process.env.SHAPE === 'tpf') {
    return Shapes.TPF;
  }
  if (process.env.SHAPE === 'brtpf') {
    return Shapes.BRTPF;
  }
  if (process.env.SHAPE === 'no-union') {
    return Shapes.PASSAGE_NO_UNION;
  }
  return Shapes.PASSAGE;
}
