import type { BindMethod } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import { QuerySourceSparql } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import type { MediatorHttp } from '@comunica/bus-http';
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
 * Query source for Raw continuation-query helper endpoints.
 */
export class QuerySourceRaw implements IQuerySource {
  protected static readonly SELECTOR_SHAPE: FragmentSelectorShape = Shapes.ALL;

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
  ) {
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

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return QuerySourceRaw.SELECTOR_SHAPE;
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
      const variables = algebraUtils.inScopeVariables(operation);
      const selectQuery = await this.toSelectQuery(context, operation, options);
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

      QuerySourceRaw.initializeLogger(context, operation, selectQuery);
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
    QuerySourceRaw.updateStartTime(context, operation);
    const undefVariablesIndex = new Set(undefVariables.map(variable => variable.value));

    this.lastSourceContext = this.context.merge(context);
    const rawStream = await this.endpointFetcher.fetchBindings(endpoint, query);
    this.lastSourceContext = undefined;

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

    iterator.on('end', () => {
      QuerySourceRaw.updateDoneTime(context, operation);
      QuerySourceRaw.updateNbResults(context, operation, iterator.getProperty('nbResults') ?? 0);
    });

    return iterator;
  }

  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): AsyncIterator<RDF.Quad> {
    return new EmptyIterator<RDF.Quad>();
  }

  public async queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceRaw.');
  }

  public async queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceRaw.');
  }

  public toString(): string {
    return `QuerySourceRaw(${this.url})`;
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
    if (operation.type === Algebra.Types.PROJECT) {
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
      'raw',
      {
        variables: algebraUtils.inScopeVariables(operation).map(variable => variable.value),
        query,
      },
    );
  }

  public static updateStartTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourceRaw.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.startAt = Date.now();
      physicalQueryPlanLogger.appendMetadata(operation, { startAt: node.startAt });
    }
  }

  public static updateDoneTime(context: IActionContext, operation: Algebra.Operation): void {
    const physicalQueryPlanLogger = QuerySourceRaw.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.doneAt = Date.now();
      node.timeLife = node.doneAt - node.startAt;
      physicalQueryPlanLogger.appendMetadata(operation, { doneAt: node.doneAt });
      physicalQueryPlanLogger.appendMetadata(operation, { timeLife: node.timeLife });
    }
  }

  public static updateNbResults(context: IActionContext, operation: Algebra.Operation, nbResults: number): void {
    const physicalQueryPlanLogger = QuerySourceRaw.getPhysicalQueryPlanLogger(context);
    if (physicalQueryPlanLogger) {
      const node = <Record<string, any>> operation;
      node.cardinalityReal = nbResults;
      physicalQueryPlanLogger.appendMetadata(operation, { cardinalityReal: nbResults });
    }
  }

  private static getPhysicalQueryPlanLogger(context: IActionContext): IPhysicalQueryPlanLogger | undefined {
    return context.get(KeysInitQuery.physicalQueryPlanLogger) ??
      context.get(new ActionContextKey<IPhysicalQueryPlanLogger>('physicalQueryPlanLogger'));
  }
}
