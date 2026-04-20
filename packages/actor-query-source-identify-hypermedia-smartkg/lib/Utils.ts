import type { Algebra } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';
import type * as RDF from '@rdfjs/types';

/**
 * Represents a family from SmartKG metadata
 */
export interface ISmartKgFamily {
  index: number;
  name: string;
  numSubjects: number;
  numTriples: number;
  grouped: boolean;
  predicateSet: string[];
}

/**
 * Represents parsed SmartKG metadata
 */
export interface ISmartKgMetadata {
  numFamilies: number;
  infrequentPredicates: string[];
  families: ISmartKgFamily[];
}

/**
 * Extract all predicates from a pattern or star pattern
 */
export function extractPredicates(patterns: Algebra.Pattern[]): Set<string> {
  const predicates = new Set<string>();
  for (const pattern of patterns) {
    // Skip variable predicates
    if (pattern.predicate.termType !== 'Variable') {
      predicates.add(termToString(pattern.predicate));
    }
  }
  return predicates;
}

/**
 * Check if any predicate is in the infrequent list
 */
export function hasInfrequentPredicate(patterns: Algebra.Pattern[], infrequentPredicates: Set<string>): boolean {
  for (const pattern of patterns) {
    if (pattern.predicate.termType !== 'Variable') {
      const predicateStr = termToString(pattern.predicate);
      if (infrequentPredicates.has(predicateStr)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Find matching families for the given predicates
 * Returns families where all query predicates are in the family's predicate set
 */
export function findMatchingFamilies(
  queryPredicates: Set<string>,
  families: ISmartKgFamily[],
  infrequentPredicates: Set<string>,
): ISmartKgFamily[] {
  const matching: ISmartKgFamily[] = [];

  for (const family of families) {
    // Skip families with 0 triples
    if (family.numTriples === 0) {
      continue;
    }

    // Check if all query predicates are in this family
    const familyPredicateSet = new Set(family.predicateSet);
    let allMatch = true;
    for (const pred of queryPredicates) {
      if (!familyPredicateSet.has(pred)) {
        allMatch = false;
        break;
      }
    }

    if (allMatch) {
      matching.push(family);
    }
  }

  return matching;
}

/**
 * Apply grouping optimization: prefer grouped families (merged sets) over individual ones.
 * Returns the minimal set of families needed to cover all predicates.
 */
export function selectOptimalFamilies(
  matchingFamilies: ISmartKgFamily[],
  options: {
    preferGrouped?: boolean;
    maxFamilies?: number;
  } = {},
): ISmartKgFamily[] {
  const { preferGrouped = true, maxFamilies = 100 } = options;

  if (matchingFamilies.length === 0) {
    return [];
  }

  // Optimization 1: If we have grouped families, prefer those
  if (preferGrouped) {
    const groupedFamilies = matchingFamilies.filter(f => f.grouped);
    if (groupedFamilies.length > 0) {
      // Among grouped families, select those with smallest predicate sets (fewest extra predicates)
      const sorted = groupedFamilies.sort((a, b) => a.predicateSet.length - b.predicateSet.length);
      // Return only the smallest ones (with minimum predicate set size)
      const minSize = sorted[0].predicateSet.length;
      const optimal = sorted.filter(f => f.predicateSet.length === minSize);
      return optimal.slice(0, maxFamilies);
    }
  }

  // Optimization 2: If too many families, filter by size
  if (matchingFamilies.length > maxFamilies) {
    const sorted = matchingFamilies.sort((a, b) => a.predicateSet.length - b.predicateSet.length);
    return sorted.slice(0, maxFamilies);
  }

  return matchingFamilies;
}

/**
 * Parse SmartKG metadata from JSON string
 */
export function parseSmartKgMetadata(jsonStr: string): ISmartKgMetadata {
  const raw = JSON.parse(jsonStr);
  return {
    numFamilies: raw.numFamilies || 0,
    infrequentPredicates: raw.infrequentPredicates || [],
    families: (raw.families || []).map((f: any) => ({
      index: f.index,
      name: f.name,
      numSubjects: f.numSubjects,
      numTriples: f.numTriples,
      grouped: f.grouped || false,
      predicateSet: f.predicateSet || [],
    })),
  };
}

/**
 * Convert metadata predicates list to Set for faster lookup
 */
export function metadataToSets(metadata: ISmartKgMetadata): {
  infrequentPredicates: Set<string>;
} {
  return {
    infrequentPredicates: new Set(metadata.infrequentPredicates),
  };
}

/**
 * Determine if a star pattern should be handled by SmartKG
 */
export function isStarPatternSmartKg(
  patterns: Algebra.Pattern[],
  metadata: ISmartKgMetadata,
  minPatternsForSmartKg: number = 2,
): boolean {
  // SmartKG only beneficial for multi-pattern stars
  if (patterns.length < minPatternsForSmartKg) {
    return false;
  }

  // Check for variable or infrequent predicates
  const infrequentSet = new Set(metadata.infrequentPredicates);
  if (hasInfrequentPredicate(patterns, infrequentSet)) {
    return false;
  }

  // Check if we can find matching families
  const predicates = extractPredicates(patterns);
  if (predicates.size === 0) {
    return false;
  }

  const matching = findMatchingFamilies(predicates, metadata.families, infrequentSet);
  return matching.length > 0;
}

/**
 * Extract Smart-KG shipping strategy hints from query operation metadata
 * 
 * @param context - The execution context that may contain optimization metadata
 * @param pattern - The current pattern being evaluated
 * @returns The recommended shipping strategy ('P-S' or 'TP-S') or undefined if not determined
 */
export function getShippingStrategyHint(
  context: any,
  pattern: Algebra.Pattern,
): 'P-S' | 'TP-S' | undefined {
  try {
    // Check if context has Smart-KG optimization metadata
    if (context && context.smartkgStars && Array.isArray(context.smartkgStars)) {
      const subjectStr = termToString(pattern.subject);
      
      // Find the star that contains this pattern's subject
      for (const star of context.smartkgStars) {
        if (star.subject === subjectStr) {
          return star.strategy;
        }
      }
    }
  } catch (_err) {
    // Silently ignore errors, will fall back to default behavior
  }
  
  return undefined;
}

/**
 * Check if a pattern is part of a multi-pattern star (eligible for P-S)
 * based on optimization metadata
 * 
 * @param context - The execution context
 * @param pattern - The current pattern
 * @returns The number of patterns in this star, or 0 if not determined
 */
export function getStarPatternCount(
  context: any,
  pattern: Algebra.Pattern,
): number {
  try {
    if (context && context.smartkgStars && Array.isArray(context.smartkgStars)) {
      const subjectStr = termToString(pattern.subject);
      
      for (const star of context.smartkgStars) {
        if (star.subject === subjectStr) {
          return star.patternCount;
        }
      }
    }
  } catch (_err) {
    // Silently ignore
  }
  
  return 0;
}

/**
 * Detect the number of patterns with the same subject in a join operation
 * Recursively extracts patterns from the operation tree
 * 
 * @param operation - The operation to analyze
 * @param targetSubject - The subject term to match
 * @returns The count of patterns with matching subject
 */
export function detectStarPatternCount(
  operation: Algebra.Operation | undefined,
  targetSubject: RDF.Term,
): number {
  if (!operation) {
    return 0;
  }

  const patterns: Algebra.Pattern[] = [];
  collectPatternsRecursive(operation, patterns);

  const targetSubjectStr = termToString(targetSubject);
  let count = 0;
  for (const pattern of patterns) {
    if (termToString(pattern.subject) === targetSubjectStr) {
      count++;
    }
  }

  return count;
}

/**
 * Recursively collect all patterns from an operation tree
 */
function collectPatternsRecursive(
  operation: Algebra.Operation,
  patterns: Algebra.Pattern[],
): void {
  if (operation.type === 'pattern') {
    patterns.push(operation as Algebra.Pattern);
  } else if (operation.type === 'join') {
    const join = operation as Algebra.Join;
    for (const input of join.input) {
      collectPatternsRecursive(input, patterns);
    }
  } else if ('input' in operation && operation.input) {
    const input = (operation as any).input;
    if (Array.isArray(input)) {
      for (const op of input) {
        collectPatternsRecursive(op, patterns);
      }
    } else if (input && typeof input === 'object') {
      collectPatternsRecursive(input as Algebra.Operation, patterns);
    }
  }
}
