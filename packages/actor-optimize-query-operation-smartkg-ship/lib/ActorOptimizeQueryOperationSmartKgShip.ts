import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { Algebra } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';

type SmartKgShippingStrategy = 'P-S' | 'TP-S';

interface ISmartKgStarHint {
  subject: string;
  patternCount: number;
  strategy: SmartKgShippingStrategy;
}

/**
 * Optimizer actor that inspects the algebra tree, detects subject-based stars,
 * and annotates the context with SmartKG shipping hints under `smartkgStars`.
 *
 * It does not rewrite the operation itself.
 */
export class ActorOptimizeQueryOperationSmartKgShip extends ActorOptimizeQueryOperation {
  public async test(_action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  // Store the detected SmartKG star hints for later source-selection actors.
  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const stars = extractStars(action.operation);
    let context: IActionContext = action.context;

    if (typeof (<any> context).setRaw === 'function') {
      context = (<any> context).setRaw('smartkgStars', stars);
    }

    return {
      operation: action.operation,
      context,
    };
  }
}

// Group patterns by subject and choose a shipping hint for each star.
function extractStars(operation: Algebra.Operation): ISmartKgStarHint[] {
  const patterns: Algebra.Pattern[] = [];
  collectPatternsRecursive(operation, patterns, new Set<Algebra.Pattern>());

  const stars = new Map<string, Algebra.Pattern[]>();

  for (const pattern of patterns) {
    const subject = termToString(pattern.subject);
    const bucket = stars.get(subject) ?? [];
    bucket.push(pattern);
    stars.set(subject, bucket);
  }

  return [ ...stars.entries() ].map(([ subject, starPatterns ]) => ({
    subject,
    patternCount: starPatterns.length,
    strategy:
      starPatterns.length >= 2 &&
      starPatterns.every(pattern => pattern.predicate.termType !== 'Variable') ?
        'P-S' :
        'TP-S',
  }));
}

/**
 * Recursively collect all PATTERN operations from an algebra tree.
 */
function collectPatternsRecursive(
  operation: Algebra.Operation,
  patterns: Algebra.Pattern[],
  seen: Set<Algebra.Pattern>,
): void {
  if (operation.type === Algebra.Types.PATTERN) {
    pushPattern(<Algebra.Pattern> operation, patterns, seen);
    return;
  }

  const op = <any> operation;

  if (Array.isArray(op.input)) {
    for (const child of op.input) {
      collectPatternsRecursive(<Algebra.Operation> child, patterns, seen);
    }
  } else if (op.input) {
    collectPatternsRecursive(<Algebra.Operation> op.input, patterns, seen);
  }

  if (Array.isArray(op.patterns)) {
    for (const child of op.patterns) {
      if (child?.type === Algebra.Types.PATTERN) {
        pushPattern(<Algebra.Pattern> child, patterns, seen);
      }
    }
  }
}

// Append a pattern once while preserving traversal order.
function pushPattern(pattern: Algebra.Pattern, patterns: Algebra.Pattern[], seen: Set<Algebra.Pattern>): void {
  if (seen.has(pattern)) {
    return;
  }
  seen.add(pattern);
  patterns.push(pattern);
}

export { ActorOptimizeQueryOperationSmartKgShip as default };
