import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';

/**
 * A Smart-KG Query Operation Optimizer that decomposes joins into star patterns
 * and determines partition vs TPF shipping strategies.
 */
export class ActorOptimizeQueryOperationSmartKgShip extends ActorOptimizeQueryOperation {
  public async test(_action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    let operation: Algebra.Operation = action.operation;

    // Map over all JOIN operations to decompose them into stars
    operation = algebraUtils.mapOperation(operation, {
      [Algebra.Types.JOIN]: {
        preVisitor: () => ({ continue: false }),
        transform: (op: Algebra.Join) => this.optimizeJoin(op),
      },
    });

    return { operation, context: action.context };
  }

  /**
   * Optimize a JOIN operation by decomposing into stars and annotating with
   * shipping strategy decisions (Partition Shipping vs Triple Pattern Shipping).
   */
  private optimizeJoin(join: Algebra.Join): Algebra.Operation {
    // Extract all patterns from the join tree
    const patterns = this.collectPatterns(join.input);

    if (patterns.length < 2) {
      // No optimization needed for single patterns
      return join;
    }

    // Group patterns into stars by subject variable/term
    const stars = this.extractStarsFromPatterns(patterns);

    // For each star, decide on shipping strategy
    const annotatedStars = stars.map(star => ({
      patterns: star,
      strategy: this.decideShippingStrategy(star),
    }));

    // Attach metadata to the join with all star information
    const operationWithMetadata = { ...join };
    (operationWithMetadata as Record<string, unknown>).smartkgStars = annotatedStars;

    return operationWithMetadata;
  }

  /**
   * Recursively collect all patterns from join input operations.
   */
  private collectPatterns(operations: Algebra.Operation[]): Algebra.Pattern[] {
    const patterns: Algebra.Pattern[] = [];

    for (const op of operations) {
      if (op.type === Algebra.Types.PATTERN) {
        patterns.push(op as Algebra.Pattern);
      } else if (op.type === Algebra.Types.JOIN) {
        patterns.push(...this.collectPatterns((op as Algebra.Join).input));
      } else if ('input' in op && op.input) {
        // Handle other wrapper operations like FILTER, DISTINCT, etc.
        const inputOp = (op as Record<string, unknown>).input;
        if (Array.isArray(inputOp)) {
          patterns.push(...this.collectPatterns(inputOp as Algebra.Operation[]));
        } else if (inputOp && typeof inputOp === 'object') {
          patterns.push(...this.collectPatterns([inputOp as Algebra.Operation]));
        }
      }
    }

    return patterns;
  }

  /**
   * Extract a list of star patterns from a flat list of patterns.
   * Each star is a group of patterns with the same subject.
   */
  private extractStarsFromPatterns(patterns: Algebra.Pattern[]): Algebra.Pattern[][] {
    const starMap = new Map<string, Algebra.Pattern[]>();

    for (const pattern of patterns) {
      // Get the subject key (variable name or term string representation)
      const subjectKey = this.getTermKey(pattern.subject);

      if (!starMap.has(subjectKey)) {
        starMap.set(subjectKey, []);
      }
      starMap.get(subjectKey)!.push(pattern);
    }

    // Convert to array of stars (only return multi-pattern stars)
    return Array.from(starMap.values()).filter(star => star.length > 1);
  }

  /**
   * Decide shipping strategy for a star pattern.
   * Rule from SMART-KG paper: Only use Partition Shipping if 2+ patterns in star.
   */
  private decideShippingStrategy(star: Algebra.Pattern[]): string {
    if (star.length < 2) {
      // Single pattern must use TPF (Triple Pattern Shipping)
      return 'TP-S';
    }

    // Multi-pattern star: can use Partition Shipping if predicate coverage is good
    // For now, default to P-S for multi-pattern stars
    // The executor will verify actual predicate coverage against available partitions
    return 'P-S';
  }

  /**
   * Get unique key for a term (variable or RDF term).
   */
  private getTermKey(term: RDF.Term): string {
    if (term.termType === 'Variable') {
      return `var:${(term as RDF.Variable).value}`;
    }
    return term.value;
  }
}

export { ActorOptimizeQueryOperationSmartKgShip as default };
