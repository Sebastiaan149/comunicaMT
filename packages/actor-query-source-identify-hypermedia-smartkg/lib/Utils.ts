import type { Algebra } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';
import type * as RDF from '@rdfjs/types';

export interface ISmartKgFamily {
  index: number;
  name: string;
  numSubjects: number;
  numTriples: number;
  grouped?: boolean;
  predicateSet: string[];
  sourceSet?: number[];
  originalFamily?: number;
  noMaterialized?: boolean;
}

export interface ISmartKgMetadata {
  numFamilies: number;
  infrequentPredicates: string[];
  massivePredicates?: string[];
  families: ISmartKgFamily[];
}

export interface ISmartKgStarHint {
  subject: string;
  patternCount: number;
  strategy: 'P-S' | 'TP-S';
}

export function extractPredicates(patterns: Algebra.Pattern[]): Set<string> {
  const predicates = new Set<string>();
  for (const pattern of patterns) {
    if (pattern.predicate.termType !== 'Variable') {
      predicates.add(termToString(pattern.predicate));
    }
  }
  return predicates;
}

export function hasInfrequentPredicate(patterns: Algebra.Pattern[], blockedPredicates: Set<string>): boolean {
  for (const pattern of patterns) {
    if (pattern.predicate.termType !== 'Variable' && blockedPredicates.has(termToString(pattern.predicate))) {
      return true;
    }
  }
  return false;
}

export function hasBlockedPredicate(patterns: Algebra.Pattern[], blockedPredicates: Set<string>): boolean {
  return hasInfrequentPredicate(patterns, blockedPredicates);
}

export function findMatchingFamilies(
  queryPredicates: Set<string>,
  families: ISmartKgFamily[],
  _blockedPredicates?: Set<string>,
): ISmartKgFamily[] {
  if (queryPredicates.size === 0) {
    return [];
  }

  const matching: ISmartKgFamily[] = [];
  for (const family of families) {
    if (!family.predicateSet || family.predicateSet.length === 0) {
      continue;
    }
    const familyPredicateSet = new Set(family.predicateSet);
    let allMatch = true;
    for (const predicate of queryPredicates) {
      if (!familyPredicateSet.has(predicate)) {
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

export function resolveFamiliesToMaterialized(
  families: ISmartKgFamily[],
  metadata: ISmartKgMetadata,
): ISmartKgFamily[] {
  const byIndex = new Map<number, ISmartKgFamily>();
  for (const family of metadata.families) {
    byIndex.set(family.index, family);
  }

  const resolved = new Map<number, ISmartKgFamily>();
  const visiting = new Set<number>();
  const visit = (family: ISmartKgFamily): void => {
    if (resolved.has(family.index)) {
      return;
    }
    if (visiting.has(family.index)) {
      return;
    }

    visiting.add(family.index);
    if (Array.isArray(family.sourceSet) && family.sourceSet.length > 0) {
      for (const index of family.sourceSet) {
        const sourceFamily = byIndex.get(index);
        if (sourceFamily) {
          visit(sourceFamily);
        }
      }
      visiting.delete(family.index);
      return;
    }

    const isMaterialized = !family.noMaterialized && family.numTriples > 0 && Boolean(family.name);
    if (isMaterialized) {
      resolved.set(family.index, family);
      visiting.delete(family.index);
      return;
    }

    if (typeof family.originalFamily === 'number') {
      const original = byIndex.get(family.originalFamily);
      if (original) {
        visit(original);
      }
    }
    visiting.delete(family.index);
  };

  for (const family of families) {
    visit(family);
  }

  return [ ...resolved.values() ];
}

interface ISelectOptimalFamiliesOptions {
  preferGrouped?: boolean;
  maxFamilies?: number;
  resolveMaterialized?: boolean;
  completeCoverage?: boolean;
}

export function selectOptimalFamilies(
  matchingFamilies: ISmartKgFamily[],
  metadataOrOptions?: ISmartKgMetadata | ISelectOptimalFamiliesOptions,
  maybeOptions?: ISelectOptimalFamiliesOptions,
): ISmartKgFamily[] {
  let metadata: ISmartKgMetadata | undefined;
  let options: ISelectOptimalFamiliesOptions | undefined;

  if (metadataOrOptions && 'families' in metadataOrOptions) {
    metadata = metadataOrOptions;
    options = maybeOptions;
  } else {
    options = metadataOrOptions as ISelectOptimalFamiliesOptions | undefined;
  }

  const preferGrouped = options?.preferGrouped ?? true;
  const maxFamilies = options?.maxFamilies ?? 100;
  const resolveMaterialized = options?.resolveMaterialized ?? true;
  const completeCoverage = options?.completeCoverage ?? false;

  if (matchingFamilies.length === 0) {
    return [];
  }

  let candidates = [ ...matchingFamilies ];

  if (preferGrouped && !completeCoverage) {
    const grouped = candidates.filter(family => Boolean(family.grouped));
    if (grouped.length > 0) {
      candidates = grouped;
    }
  }

  candidates.sort((left, right) => {
    const leftSize = left.predicateSet?.length ?? Number.MAX_SAFE_INTEGER;
    const rightSize = right.predicateSet?.length ?? Number.MAX_SAFE_INTEGER;
    if (leftSize !== rightSize) {
      return leftSize - rightSize;
    }
    return left.index - right.index;
  });

  if (!completeCoverage) {
    const smallestPredicateSetSize = candidates[0].predicateSet?.length ?? 0;
    candidates = candidates.filter(family => (family.predicateSet?.length ?? 0) === smallestPredicateSetSize);
  }

  if (metadata && resolveMaterialized) {
    candidates = resolveFamiliesToMaterialized(candidates, metadata);
  }

  return candidates.slice(0, maxFamilies);
}

export function parseSmartKgMetadata(jsonStr: string): ISmartKgMetadata {
  const raw = JSON.parse(jsonStr) as Record<string, any>;
  const familiesRaw = Array.isArray(raw.families) ? raw.families : [];
  return {
    numFamilies: typeof raw.numFamilies === 'number' ? raw.numFamilies : familiesRaw.length,
    infrequentPredicates: Array.isArray(raw.infrequentPredicates) ? raw.infrequentPredicates : [],
    massivePredicates: Array.isArray(raw.massivePredicates) ? raw.massivePredicates : [],
    families: familiesRaw.map((family: Record<string, any>) => ({
      index: typeof family.index === 'number' ? family.index : 0,
      name: typeof family.name === 'string' ? family.name : '',
      numSubjects: typeof family.numSubjects === 'number' ? family.numSubjects : 0,
      numTriples: typeof family.numTriples === 'number' ? family.numTriples : 0,
      grouped: Boolean(family.grouped),
      predicateSet: Array.isArray(family.predicateSet) ? family.predicateSet : [],
      sourceSet: Array.isArray(family.sourceSet) ? family.sourceSet.filter((value: unknown) => typeof value === 'number') : undefined,
      originalFamily: typeof family.originalFamily === 'number' ? family.originalFamily : undefined,
      noMaterialized: Boolean(family.noMaterialized),
    })),
  };
}

export function metadataToSets(metadata: ISmartKgMetadata): {
  infrequentPredicates: Set<string>;
  massivePredicates: Set<string>;
  blockedPredicates: Set<string>;
} {
  const infrequentPredicates = new Set(metadata.infrequentPredicates);
  const massivePredicates = new Set(metadata.massivePredicates ?? []);
  const blockedPredicates = new Set<string>([ ...infrequentPredicates, ...massivePredicates ]);
  return { infrequentPredicates, massivePredicates, blockedPredicates };
}

export function isStarPatternSmartKg(
  patterns: Algebra.Pattern[],
  metadata: ISmartKgMetadata,
  minPatternsForSmartKg = 2,
): boolean {
  if (patterns.length < minPatternsForSmartKg) {
    return false;
  }

  if (patterns.some(pattern => pattern.predicate.termType === 'Variable')) {
    return false;
  }

  const { blockedPredicates } = metadataToSets(metadata);
  if (hasBlockedPredicate(patterns, blockedPredicates)) {
    return false;
  }

  const predicates = extractPredicates(patterns);
  return findMatchingFamilies(predicates, metadata.families).length > 0;
}

export function getShippingStrategyHint(
  context: any,
  pattern: Algebra.Pattern,
): 'P-S' | 'TP-S' | undefined {
  const stars = getSmartKgStars(context);
  const subject = termToString(pattern.subject);
  const star = stars.find(entry => entry.subject === subject);
  return star?.strategy;
}

export function getStarPatternCount(
  context: any,
  pattern: Algebra.Pattern,
): number {
  const stars = getSmartKgStars(context);
  const subject = termToString(pattern.subject);
  return stars.find(entry => entry.subject === subject)?.patternCount ?? 0;
}

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
  return patterns.filter(pattern => termToString(pattern.subject) === targetSubjectStr).length;
}

export function collectPatternsRecursive(
  operation: Algebra.Operation,
  patterns: Algebra.Pattern[],
): void {
  if (operation.type === 'pattern') {
    patterns.push(operation as Algebra.Pattern);
    return;
  }

  if ('input' in operation && (operation as any).input) {
    const input = (operation as any).input;
    if (Array.isArray(input)) {
      for (const child of input) {
        collectPatternsRecursive(child as Algebra.Operation, patterns);
      }
    } else if (typeof input === 'object') {
      collectPatternsRecursive(input as Algebra.Operation, patterns);
    }
  }
}

function getSmartKgStars(context: any): ISmartKgStarHint[] {
  if (!context) {
    return [];
  }

  if (Array.isArray(context.smartkgStars)) {
    return context.smartkgStars as ISmartKgStarHint[];
  }

  if (typeof context.getRaw === 'function') {
    const raw = context.getRaw('smartkgStars');
    if (Array.isArray(raw)) {
      return raw as ISmartKgStarHint[];
    }
  }

  return [];
}
