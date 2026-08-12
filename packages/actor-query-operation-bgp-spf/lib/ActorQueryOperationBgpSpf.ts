import { isQuerySourceSpf } from '@comunica/actor-query-source-identify-hypermedia-spf';
import type { IActorQueryOperationArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTyped } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
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
    const source = getSpfSource(operation);
    if (!source || !isQuerySourceSpf(source.source)) {
      return failTest(`Actor ${this.name} only handles BGP operations with an SPF query source.`);
    }
    return passTest({ httpRequests: 0 });
  }

  // Execute the BGP directly against the SPF query source and expose metadata.
  public async runOperation(
    operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<IQueryOperationResult> {
    const sourceWrapper: IQuerySourceWrapper = getSpfSource(operation)!;
    const mergedContext = sourceWrapper.context ? context.merge(sourceWrapper.context) : context;
    const bindingsStream = sourceWrapper.source.queryBindings(operation, mergedContext);
    return {
      type: 'bindings',
      bindingsStream,
      metadata: getMetadataBindings(bindingsStream),
    };
  }
}

function getSpfSource(operation: Algebra.Bgp): IQuerySourceWrapper | undefined {
  const bgpSource = getOperationSource(operation);
  if (bgpSource) {
    return isQuerySourceSpf(bgpSource.source) ? bgpSource : undefined;
  }
  if (operation.patterns.length === 0) {
    return;
  }
  const source = getOperationSource(operation.patterns[0]);
  if (!source || !isQuerySourceSpf(source.source)) {
    return;
  }
  const hasCommonSource = operation.patterns
    .every(pattern => getOperationSource(pattern)?.source === source.source);
  return hasCommonSource ? source : undefined;
}
