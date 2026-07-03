import type { MediatorHttp } from '@comunica/bus-http';
import type { MediatorQueryProcess } from '@comunica/bus-query-process';
import type { MediatorQuerySerialize } from '@comunica/bus-query-serialize';
import type { BindMethod } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import { QuerySourceSparql } from '@comunica/actor-query-source-identify-hypermedia-sparql';
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
  protected static readonly SELECTOR_SHAPE: FragmentSelectorShape =
    process.env.SHAPE === 'tpf' ?
      Shapes.TPF :
      process.env.SHAPE === 'brtpf' ?
        Shapes.BRTPF :
        process.env.SHAPE === 'no-union' ?
          Shapes.PASSAGE_NO_UNION :
          Shapes.PASSAGE;

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

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    const shapeKey = new ActionContextKey<FragmentSelectorShape>('shape');
    return context.get(shapeKey) ?? this.context.get(shapeKey) ?? QuerySourcePassage.SELECTOR_SHAPE;
  }

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

      Actor.getContextLogger(context)?.info(`Asking for:\n${selectQuery}`);

      return this.queryBindingsRemote(operation, this.url, selectQuery, variables, context, undefVariables);
    }, { autoStart: false });

    this.attachMetadata(bindings, context, operationPromise, options);

    return bindings;
  }

  protected attachMetadata(
    target: AsyncIterator<any>,
    context: IActionContext,
    operationPromise: Promise<Algebra.Operation>,
    options?: IQueryBindingsOptions,
  ): void {
    let variablesCount: MetadataVariable[] = [];
    new Promise<Algebra.Operation>(async(resolve, reject) => {
      try {
        const operation = await operationPromise;
        const undefVariables = QuerySourceSparql.getOperationUndefs(operation);
        const variablesScoped = algebraUtils.inScopeVariables(operation);
        variablesCount = variablesScoped.map(variable => ({
          variable,
          canBeUndef: undefVariables.some(undefVariable => undefVariable.equals(variable)),
        }));
        resolve(operation);
      } catch (error: unknown) {
        reject(error);
      }
    }).then(async(operation) => {
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
      return new EmptyIterator<RDF.Bindings>() as BindingsStream;
    }

    this.lastSourceContext = this.context.merge(context);
    const rawStream = await this.endpointFetcher.fetchBindings(endpoint, query)
      .then((stream) => {
        QuerySourcePassage.updateFirstResultTime(context, operation);
        return stream;
      })
      .catch((error: Error) => {
        QuerySourcePassage.logError(context, operation, error.message);
      });
    this.lastSourceContext = undefined;

    if (!rawStream) {
      return new EmptyIterator<RDF.Bindings>() as BindingsStream;
    }

    const iterator = wrap<any>(rawStream, { autoStart: false, maxBufferSize: Number.POSITIVE_INFINITY })
      .map<RDF.Bindings>((rawData: Record<string, RDF.Term>) => {
        const nbResults: number = iterator.getProperty('nbResults') || 0;
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

    const nextPromise: Promise<string | void> = new Promise(resolve => {
      rawStream.on('metadata', async(metadata: { next?: string }) => {
        Actor.getContextLogger(this.context)?.info(`Next query to get complete result:\n${metadata.next}`);
        resolve(metadata.next);
      });
      rawStream.on('end', async() => {
        resolve();
      });
      iterator.on('error', async(error: Error) => {
        QuerySourcePassage.logError(context, operation, error.message);
        resolve();
      });
      iterator.on('end', async() => {
        QuerySourcePassage.updateDoneTime(context, operation);
        QuerySourcePassage.updateNbResults(context, operation, iterator.getProperty('nbResults') || 0);
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
    }), { autoStart: false }) as BindingsStream;

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

  private operationToSelectQuery(operation: Algebra.Operation, variables: RDF.Variable[]): Promise<string> {
    return this.operationToQuery(this.algebraFactory.createProject(operation, variables));
  }

  private async operationToQuery(operation: Algebra.Operation): Promise<string> {
    return (await this.mediatorQuerySerialize.mediate({
      queryFormat: { language: 'sparql', version: '1.2' },
      operation,
      newlines: false,
      indentWidth: 0,
      context: this.context,
    })).query;
  }

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

  public static updateStartTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = operation as Record<string, any>;
      node.startAt = Date.now();
      physicalQueryPlanLogger.appendMetadata(operation, { startAt: node.startAt });
    }
  }

  public static updateFirstResultTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = operation as Record<string, any>;
      node.firstResultAt = Date.now();
      node.timeFirstResult = node.firstResultAt - node.startAt;
      physicalQueryPlanLogger.appendMetadata(operation, { firstResultAt: node.firstResultAt });
      physicalQueryPlanLogger.appendMetadata(operation, { timeFirstResult: node.timeFirstResult });
    }
  }

  public static updateDoneTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = operation as Record<string, any>;
      node.doneAt = Date.now();
      node.timeLife = node.doneAt - node.startAt;
      physicalQueryPlanLogger.appendMetadata(operation, { doneAt: node.doneAt });
      physicalQueryPlanLogger.appendMetadata(operation, { timeLife: node.timeLife });
    }
  }

  public static updateNbResults(context: IActionContext, operation: Algebra.Operation, nbResults: number): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = operation as Record<string, any>;
      node.cardinalityReal = nbResults;
      physicalQueryPlanLogger.appendMetadata(operation, { cardinalityReal: nbResults });
    }
  }

  public static logError(context: IActionContext, operation: Algebra.Operation, message: string): void {
    const physicalQueryPlanLogger = QuerySourcePassage.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      physicalQueryPlanLogger.appendMetadata(operation, { status: 'error', message });
    }
  }

  private static getPhysicalQueryPlanLogger(context: IActionContext): IPhysicalQueryPlanLogger | undefined {
    return context.get(KeysInitQuery.physicalQueryPlanLogger) ??
      context.get(new ActionContextKey<IPhysicalQueryPlanLogger>('physicalQueryPlanLogger'));
  }
}
