import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, passTestVoid } from '@comunica/core';
import type { IActionContext, IQueryOperationResult } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import { AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';

/**
 * Context key for SmartKG star pattern candidates that may benefit from SmartKG optimization
 */
export const KEY_SMARTKG_CANDIDATES = new ActionContextKey<Algebra.Pattern[][]>(
  '@comunica/actor-query-operation-bgp-smartkg:candidates',
);

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
    this.logDebug(actionContext, `[SmartKG BGP] Received ${operation.patterns.length} patterns`);

    // Decompose BGP into star patterns
    const stars = this.decomposeIntoStars(operation);
    this.logDebug(actionContext, `[SmartKG BGP] Decomposed into ${stars.length} star patterns`);
    
    for (let i = 0; i < stars.length; i++) {
      const star = stars[i];
      this.logDebug(
        actionContext,
        `[SmartKG BGP] Star ${i + 1}: ${star.length} patterns, subject=${termToString(star[0]?.subject)}, predicates=[${star.map(p => termToString(p.predicate)).join(', ')}]`,
      );
    }
    
    // Identify SmartKG candidates using heuristics
    const smallStars = stars.filter(s => s.length === 1);
    const largeStars = stars.filter(s => s.length >= this.minPatternCount);
    const smartkgCandidates = [...smallStars, ...largeStars];
    
    if (smartkgCandidates.length > 0) {
      this.logDebug(
        actionContext,
        `[SmartKG BGP] Identified ${smallStars.length} single-pattern stars and ${largeStars.length} large stars`,
      );
      
      // Pass SmartKG candidate hints to downstream operators
      actionContext = actionContext.set(KEY_SMARTKG_CANDIDATES, smartkgCandidates);
    }

    this.logDebug(actionContext, `[SmartKG BGP] Delegating to standard BGP handler`);
    
    // Delegate to standard BGP handler
    const result = await this.mediatorQueryOperation.mediate({
      context: actionContext,
      operation,
    });

    this.logDebug(actionContext, `[SmartKG BGP] Received result from mediator: type=${result.type}`);
    
    return result;
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
