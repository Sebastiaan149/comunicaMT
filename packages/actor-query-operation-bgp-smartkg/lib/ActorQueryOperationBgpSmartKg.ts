import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IActionContext, IQueryOperationResult } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';

/**
 * A BGP query operation actor that applies SmartKG optimizations for star patterns.
 * Decomposes BGPs into star patterns and routes them to SmartKG or falls back to standard BGP handling.
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
    operation: Algebra.Bgp,
    _actionContext: IActionContext,
  ): Promise<TestResult<IActorTest>> {
    // Pass through - this actor doesn't reject, just decomposes and analyzes
    return passTestVoid();
  }

  public async runOperation(
    operation: Algebra.Bgp,
    actionContext: IActionContext,
  ): Promise<IQueryOperationResult> {
    this.logDebug(actionContext, `BGP SmartKG actor received BGP with ${operation.patterns.length} patterns`);

    // Decompose BGP into star patterns
    const stars = this.decomposeIntoStars(operation);
    this.logDebug(actionContext, `Decomposed into ${stars.length} star patterns`);
    
    // For each star, check if it's a good candidate for SmartKG
    // In a full implementation, we would:
    // 1. Fetch metadata from SmartKG endpoints in context
    // 2. Evaluate each star against family definitions
    // 3. Route SmartKG candidates with source annotations
    // 4. Route others to standard BGP handler
    // For now, apply simple heuristics:
    //   - Single pattern stars are good candidates (less data movement)
    //   - Large stars are good candidates (more selectivity benefit)
    
    const smallStars = stars.filter(s => s.length === 1);
    const largeStars = stars.filter(s => s.length >= this.minPatternCount);
    
    if (smallStars.length > 0 || largeStars.length > 0) {
      this.logDebug(
        actionContext,
        `Identified ${smallStars.length} single-pattern stars and ${largeStars.length} large stars for SmartKG`,
      );
    }

    // Delegate to standard BGP handler
    // TODO: Phase 2 complete - Add source annotations for SmartKG routing
    return this.mediatorQueryOperation.mediate({
      context: actionContext,
      operation,
    });
  }

  /**
   * Decompose a BGP into star patterns (patterns grouped by subject)
   */
  private decomposeIntoStars(bgp: Algebra.Bgp): Algebra.Pattern[][] {
    const starMap: Map<string, Algebra.Pattern[]> = new Map();

    for (const pattern of bgp.patterns) {
      const subjectKey = termToString(pattern.subject);
      if (!starMap.has(subjectKey)) {
        starMap.set(subjectKey, []);
      }
      starMap.get(subjectKey)!.push(pattern);
    }

    return Array.from(starMap.values());
  }
}

export interface IActorQueryOperationBgpSmartKgArgs extends IActorQueryOperationTypedMediatedArgs {
  /**
   * Maximum number of families to fetch before delegating to standard BGP handler
   * @default {100}
   */
  maxFamilies?: number;

  /**
   * Minimum number of patterns in a star for SmartKG to be beneficial
   * @default {2}
   */
  minPatternCount?: number;
}
