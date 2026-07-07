import type { IActorQueryOperationArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTyped } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
import { isQuerySourceSpf } from '@comunica/query-source-spf';
import type { IActionContext, IQueryOperationResult, IQuerySourceWrapper } from '@comunica/types';
import { Algebra } from '@comunica/utils-algebra';
import { getMetadataBindings } from '@comunica/utils-metadata';
import { getOperationSource } from '@comunica/utils-query-operation';

/**
 * Handles sourced BGP operations against SPF query sources.
 */
export class ActorQueryOperationBgpSpf extends ActorQueryOperationTyped<Algebra.Bgp> {
  public constructor(args: IActorQueryOperationArgs) {
    super(args, Algebra.Types.BGP);
  }

  public async testOperation(
    operation: Algebra.Bgp,
    _context: IActionContext,
  ): Promise<TestResult<IActorTest>> {
    const source = getOperationSource(operation);
    if (!source || !isQuerySourceSpf(source.source)) {
      return failTest(`Actor ${this.name} only handles BGP operations with an SPF query source.`);
    }
    return passTest({ httpRequests: 0 });
  }

  public async runOperation(
    operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<IQueryOperationResult> {
    const sourceWrapper: IQuerySourceWrapper = getOperationSource(operation)!;
    const mergedContext = sourceWrapper.context ? context.merge(sourceWrapper.context) : context;
    const bindingsStream = sourceWrapper.source.queryBindings(operation, mergedContext);
    return {
      type: 'bindings',
      bindingsStream,
      metadata: getMetadataBindings(bindingsStream),
    };
  }
}
