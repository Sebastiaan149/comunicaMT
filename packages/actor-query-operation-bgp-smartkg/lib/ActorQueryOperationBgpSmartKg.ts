import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { IActionContext, IQueryOperationResult } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';

type SmartKgShippingStrategy = 'P-S' | 'TP-S';

interface ISmartKgStarHint {
  subject: string;
  patternCount: number;
  strategy: SmartKgShippingStrategy;
}

/**
 * BGP actor that detects subject-based stars inside a BGP and stores
 * SmartKG shipping hints in the context before delegating to the normal
 * query-operation mediator.
 */
export class ActorQueryOperationBgpSmartKg extends ActorQueryOperationTypedMediated<Algebra.Bgp> {
  public readonly maxFamilies: number;
  public readonly minPatternCount: number;

  public constructor(args: IActorQueryOperationBgpSmartKgArgs) {
    super(args, 'bgp');
    this.maxFamilies = args.maxFamilies ?? 100;
    this.minPatternCount = args.minPatternCount ?? 2;
  }

  public async testOperation(
    _operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<TestResult<IActorTest>> {
    if (isContextFlagSet(context, 'smartkgBgpHandled')) {
      return failTest(`Actor ${this.name} skips already handled SmartKG BGP operations.`);
    }
    return passTestVoid();
  }

  public async runOperation(
    operation: Algebra.Bgp,
    context: IActionContext,
  ): Promise<IQueryOperationResult> {
    const stars = toStarHints(operation);
    let nextContext = context;

    if (typeof (nextContext as any).setRaw === 'function') {
      nextContext = (nextContext as any).setRaw('smartkgStars', stars);
      nextContext = (nextContext as any).setRaw('smartkgBgpHandled', true);
    }

    return this.mediatorQueryOperation.mediate({
      operation,
      context: nextContext,
    });
  }
}

function toStarHints(operation: Algebra.Bgp): ISmartKgStarHint[] {
  const stars = new Map<string, Algebra.Pattern[]>();

  for (const pattern of operation.patterns) {
    const subject = termToString(pattern.subject);
    const bucket = stars.get(subject) ?? [];
    bucket.push(pattern);
    stars.set(subject, bucket);
  }

  return [ ...stars.entries() ].map(([ subject, patterns ]) => ({
    subject,
    patternCount: patterns.length,
    strategy:
      patterns.length >= 2 &&
      patterns.every(pattern => pattern.predicate.termType !== 'Variable')
        ? 'P-S'
        : 'TP-S',
  }));
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

export interface IActorQueryOperationBgpSmartKgArgs extends IActorQueryOperationTypedMediatedArgs {
  maxFamilies?: number;
  minPatternCount?: number;
}
