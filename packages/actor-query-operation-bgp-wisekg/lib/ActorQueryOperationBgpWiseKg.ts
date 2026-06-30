import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { IActionContext, IQueryOperationResult } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';

export const KEY_CONTEXT_WISEKG_BGP_HANDLED = 'wisekgBgpHandled';

/**
 * BGP actor for WiseKG engines.
 *
 * WiseKG execution choices come from `/plan`, so this actor only marks BGP
 * operations as seen before delegating to the normal query-operation mediator.
 */
export class ActorQueryOperationBgpWiseKg extends ActorQueryOperationTypedMediated<Algebra.Bgp> {
  public constructor(args: IActorQueryOperationBgpWiseKgArgs) {
    super(args, 'bgp');
  }

  public async testOperation(
    _operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<TestResult<IActorTest>> {
    if (isContextFlagSet(context, KEY_CONTEXT_WISEKG_BGP_HANDLED)) {
      return failTest(`Actor ${this.name} skips already handled WiseKG BGP operations.`);
    }
    return passTestVoid();
  }

  public async runOperation(
    operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<IQueryOperationResult> {
    let nextContext = context;

    if (typeof (nextContext as any).setRaw === 'function') {
      nextContext = (nextContext as any).setRaw(KEY_CONTEXT_WISEKG_BGP_HANDLED, true);
    }

    return this.mediatorQueryOperation.mediate({
      operation,
      context: nextContext,
    });
  }
}

function isContextFlagSet(context: unknown, key: string): boolean {
  if (!context) {
    return false;
  }

  const rawValue = typeof (context as any).getRaw === 'function' ?
    (context as any).getRaw(key) :
    undefined;
  if (rawValue !== undefined) {
    return Boolean(rawValue);
  }

  return Boolean((context as Record<string, unknown>)[key]);
}

export interface IActorQueryOperationBgpWiseKgArgs extends IActorQueryOperationTypedMediatedArgs {}
