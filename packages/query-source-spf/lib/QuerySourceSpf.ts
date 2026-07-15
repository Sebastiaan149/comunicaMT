import type { ISearchForm } from '@comunica/actor-rdf-metadata-extract-hydra-controls';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  MetadataBindings,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import { BufferedIterator, EmptyIterator } from 'asynciterator';
import { termToString } from 'rdf-string';
import { termToString as termToStringTtl } from 'rdf-string-ttl';
import { Readable } from 'stream';

const VOID_TRIPLES = 'http://rdfs.org/ns/void#triples';
const DEFAULT_MAX_MPR = 50;
const SOURCE_TYPE = 'spf';

export interface ISpfSearchMappings {
  subject: string;
  triples: string;
  star: string;
  values: string;
}

export interface ISpfSourceControl {
  searchForm: ISearchForm;
  mappings: ISpfSearchMappings;
}

export interface ISpfStar {
  subject: RDF.Term;
  patterns: Algebra.Pattern[];
}

export interface ISpfPage {
  url: string;
  metadata: Record<string, any>;
  quads: RDF.Quad[];
  cardinality?: number;
  nextPageUrl?: string;
}

interface ISpfStarChoice {
  star: ISpfStar;
  firstPage: ISpfPage;
  count?: number;
  empty?: boolean;
}

type BindingChunk = RDF.Bindings[];

interface IBindingChunkIterator {
  getNext(): Promise<BindingChunk | undefined>;
}

export class QuerySourceSpf implements IQuerySource {
  public readonly sourceType = SOURCE_TYPE;
  public readonly referenceValue: string;
  public readonly searchForm: ISearchForm;
  public readonly searchMappings: ISpfSearchMappings;
  protected readonly selectorShape: FragmentSelectorShape;

  private readonly mediatorMetadata: MediatorRdfMetadata;
  private readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  private readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly algebraFactory: AlgebraFactory;
  private readonly bindingsFactory: BindingsFactory;
  private readonly maxMpR: number;

  public constructor(
    mediatorMetadata: MediatorRdfMetadata,
    mediatorMetadataExtract: MediatorRdfMetadataExtract,
    mediatorDereferenceRdf: MediatorDereferenceRdf,
    dataFactory: ComunicaDataFactory,
    algebraFactory: AlgebraFactory,
    bindingsFactory: BindingsFactory,
    url: string,
    searchForm: ISearchForm,
    searchMappings: ISpfSearchMappings,
    maxMpR: number = DEFAULT_MAX_MPR,
  ) {
    this.referenceValue = url;
    this.mediatorMetadata = mediatorMetadata;
    this.mediatorMetadataExtract = mediatorMetadataExtract;
    this.mediatorDereferenceRdf = mediatorDereferenceRdf;
    this.dataFactory = dataFactory;
    this.algebraFactory = algebraFactory;
    this.bindingsFactory = bindingsFactory;
    this.searchForm = searchForm;
    this.searchMappings = searchMappings;
    this.maxMpR = maxMpR;

    this.selectorShape = {
      type: 'disjunction',
      children: [
        {
          type: 'operation',
          operation: {
            operationType: 'pattern',
            pattern: this.algebraFactory.createPattern(
              this.dataFactory.variable('s'),
              this.dataFactory.variable('p'),
              this.dataFactory.variable('o'),
            ),
          },
          variablesOptional: [
            this.dataFactory.variable('s'),
            this.dataFactory.variable('p'),
            this.dataFactory.variable('o'),
          ],
        },
        {
          type: 'operation',
          operation: { operationType: 'type', type: Algebra.Types.BGP },
        },
        {
          type: 'operation',
          operation: { operationType: 'type', type: Algebra.Types.JOIN },
        },
      ],
    };
  }

  public async getFilterFactor(_context: IActionContext): Promise<number> {
    return 1;
  }

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    _options?: IQueryBindingsOptions,
  ): BindingsStream {
    const patterns = this.getOperationPatterns(operation);
    const bgp = this.algebraFactory.createBgp(patterns);
    const variables = getOperationVariables(bgp);
    return new SpfBindingsIterator(
      Promise.resolve(new QueryIterator(
        new BasicGraphPatternIterator(
          new RootBindingChunkIterator(this.bindingsFactory.bindings()),
          decomposeSubjectStars(bgp.patterns),
          this,
          context,
          this.maxMpR,
        ),
      )),
      variables,
      this.maxMpR,
    ) as unknown as BindingsStream;
  }

  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): EmptyIterator<RDF.Quad> {
    return new EmptyIterator<RDF.Quad>();
  }

  public async queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('ASK queries are not supported by QuerySourceSpf.');
  }

  public async queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('UPDATE queries are not supported by QuerySourceSpf.');
  }

  public toString(): string {
    return `QuerySourceSpf(${this.referenceValue})`;
  }

  public getControl(): ISpfSourceControl {
    return {
      searchForm: this.searchForm,
      mappings: this.searchMappings,
    };
  }

  public async fetchFirstSpfPage(
    star: ISpfStar,
    bindings: BindingChunk,
    context: IActionContext,
  ): Promise<ISpfPage> {
    return this.fetchSpfPage(star, bindings, context);
  }

  public async fetchNextSpfPage(
    star: ISpfStar,
    bindings: BindingChunk,
    context: IActionContext,
    pageUrl: string,
  ): Promise<ISpfPage> {
    return this.fetchSpfPage(star, bindings, context, pageUrl);
  }

  public matchPageToStarBindings(star: ISpfStar, page: ISpfPage): RDF.Bindings[] {
    return matchStarResponse(star, page.quads, this.bindingsFactory);
  }

  public joinBindingChunks(left: BindingChunk, right: BindingChunk): BindingChunk {
    return joinBindingSets(left, right, this.bindingsFactory);
  }

  private getOperationPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
    if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
      return [ operation ];
    }
    if (isKnownOperation(operation, Algebra.Types.BGP)) {
      return operation.patterns;
    }
    if (isKnownOperation(operation, Algebra.Types.JOIN)) {
      const patterns = collectPatterns(operation);
      if (patterns.length === 0) {
        throw new Error(`Unsupported non-pattern JOIN operation for QuerySourceSpf.`);
      }
      return patterns;
    }
    throw new Error(`Unsupported operation type '${operation.type}' for QuerySourceSpf.`);
  }

  private async fetchSpfPage(
    star: ISpfStar,
    bindings: BindingChunk,
    context: IActionContext,
    pageUrl?: string,
  ): Promise<ISpfPage> {
    const url = createSpfRequestUrl(this.getControl(), star, deduplicateBindings(bindings), this.maxMpR, pageUrl);
    const dereferenceRdfOutput = await this.mediatorDereferenceRdf.mediate({ context, url });
    const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate({
      context,
      url: dereferenceRdfOutput.url,
      quads: dereferenceRdfOutput.data,
      triples: dereferenceRdfOutput.metadata?.triples,
    });

    const metadataQuads = await collectRdfStream(rdfMetadataOutput.metadata, 'SPF metadata');
    const { metadata } = await this.mediatorMetadataExtract.mediate({
      context,
      url: dereferenceRdfOutput.url,
      metadata: rdfStreamFromArray(metadataQuads),
      requestTime: dereferenceRdfOutput.requestTime,
      headers: dereferenceRdfOutput.headers,
    });
    const quads = await collectRdfStream(rdfMetadataOutput.data, 'SPF data');

    return {
      url: dereferenceRdfOutput.url,
      metadata,
      quads,
      cardinality: extractVoidTriples(metadataQuads) ?? extractMetadataCardinality(metadata),
      nextPageUrl: extractNextPageUrl(metadata),
    };
  }
}

export function isQuerySourceSpf(source: unknown): source is QuerySourceSpf {
  return Boolean(source) && (source as { sourceType?: string }).sourceType === SOURCE_TYPE;
}

export function detectSpfSearchForm(metadata: Record<string, any>): ISpfSourceControl | undefined {
  const searchForms = metadata.searchForms?.values;
  if (!Array.isArray(searchForms)) {
    return;
  }

  for (const searchForm of searchForms as ISearchForm[]) {
    const mappings = detectSpfMappings(searchForm);
    if (mappings) {
      return { searchForm, mappings };
    }
  }
}

export function createSpfSearchForm(url: string): ISpfSourceControl {
  const dataset = stripQueryAndHash(url);
  const template = `${dataset}{?s,triples,star,values}`;
  const searchForm: ISearchForm = {
    dataset,
    template,
    mappings: {
      s: 's',
      triples: 'triples',
      star: 'star',
      values: 'values',
    },
    getUri: (entries: Record<string, string>) => {
      const params = new URLSearchParams();
      for (const [ key, value ] of Object.entries(entries)) {
        if (value !== undefined && value !== '') {
          params.set(key, value);
        }
      }
      const queryString = params.toString();
      return queryString ? `${dataset}?${queryString}` : dataset;
    },
  };

  return {
    searchForm,
    mappings: {
      subject: 's',
      triples: 'triples',
      star: 'star',
      values: 'values',
    },
  };
}

export function detectSpfMappings(searchForm: ISearchForm): ISpfSearchMappings | undefined {
  let subject: string | undefined;
  let triples: string | undefined;
  let star: string | undefined;
  let values: string | undefined;

  for (const property of Object.keys(searchForm.mappings)) {
    const name = getHydraPropertyName(property);
    if ((name === 'subject' || name === 's') && !subject) {
      subject = property;
    } else if (name === 'triples' && !triples) {
      triples = property;
    } else if (name === 'star' && !star) {
      star = property;
    } else if (name === 'values' && !values) {
      values = property;
    }
  }

  if (subject && triples && star && values) {
    return { subject, triples, star, values };
  }
}

export function decomposeSubjectStars(patterns: Algebra.Pattern[]): ISpfStar[] {
  return patterns.map(pattern => ({ subject: pattern.subject, patterns: [ pattern ]}));
}

export function createSpfRequestUrl(
  control: ISpfSourceControl,
  star: ISpfStar,
  bindings: BindingChunk,
  maxMpR: number = DEFAULT_MAX_MPR,
  pageUrl?: string,
): string {
  const entries: Record<string, string> = {};
  entries[control.mappings.subject] = encodePatternTerm(star.subject);
  entries[control.mappings.triples] = String(star.patterns.length);
  entries[control.mappings.star] = encodeStar(star);

  const values = encodeValues(star, bindings.slice(0, maxMpR));
  if (values) {
    entries[control.mappings.values] = values;
  }
  if (pageUrl) {
    const page = new URL(pageUrl).searchParams.get('page');
    if (page) {
      entries.page = page;
    }
  }

  return control.searchForm.getUri(entries);
}

class RootBindingChunkIterator implements IBindingChunkIterator {
  private emitted = false;

  public constructor(private readonly emptyBinding: RDF.Bindings) {}

  public async getNext(): Promise<BindingChunk | undefined> {
    if (this.emitted) {
      return;
    }
    this.emitted = true;
    return [ this.emptyBinding ];
  }
}

class SingletonBindingChunkIterator implements IBindingChunkIterator {
  private emitted = false;

  public constructor(private readonly bindings: BindingChunk) {}

  public async getNext(): Promise<BindingChunk | undefined> {
    if (this.emitted) {
      return;
    }
    this.emitted = true;
    return this.bindings;
  }
}

class StarPatternIterator implements IBindingChunkIterator {
  private currentPage: ISpfPage | undefined;
  private currentSourceChunk: BindingChunk | undefined;
  private unreadMappings: BindingChunk = [];
  private firstPageConsumed = false;

  public constructor(
    private readonly sourceIterator: IBindingChunkIterator,
    private readonly star: ISpfStar,
    private readonly source: QuerySourceSpf,
    private readonly context: IActionContext,
    private readonly maxMpR: number,
    private readonly firstPage?: ISpfPage,
  ) {}

  public async getNext(): Promise<BindingChunk | undefined> {
    while (this.unreadMappings.length === 0) {
      if (this.currentPage?.nextPageUrl && this.currentSourceChunk) {
        this.currentPage = await this.source.fetchNextSpfPage(
          this.star,
          this.currentSourceChunk,
          this.context,
          this.currentPage.nextPageUrl,
        );
        this.unreadMappings = this.joinCurrentPage();
      } else {
        this.currentSourceChunk = await this.sourceIterator.getNext();
        if (!this.currentSourceChunk) {
          return;
        }
        this.currentPage = await this.fetchFirstPageForCurrentChunk();
        this.unreadMappings = this.joinCurrentPage();
      }
    }

    return this.unreadMappings.splice(0, this.maxMpR);
  }

  private async fetchFirstPageForCurrentChunk(): Promise<ISpfPage> {
    if (!this.firstPageConsumed && this.firstPage) {
      this.firstPageConsumed = true;
      return this.firstPage;
    }
    return this.source.fetchFirstSpfPage(this.star, this.currentSourceChunk!, this.context);
  }

  private joinCurrentPage(): BindingChunk {
    if (!this.currentPage || !this.currentSourceChunk) {
      return [];
    }
    const starBindings = this.source.matchPageToStarBindings(this.star, this.currentPage);
    return this.source.joinBindingChunks(this.currentSourceChunk, starBindings);
  }
}

class BasicGraphPatternIterator implements IBindingChunkIterator {
  private currentPipeline: IBindingChunkIterator | undefined;

  public constructor(
    private readonly sourceIterator: IBindingChunkIterator,
    private readonly stars: ISpfStar[],
    private readonly source: QuerySourceSpf,
    private readonly context: IActionContext,
    private readonly maxMpR: number,
  ) {}

  public async getNext(): Promise<BindingChunk | undefined> {
    while (!this.currentPipeline) {
      const sourceChunk = await this.sourceIterator.getNext();
      if (!sourceChunk) {
        return;
      }

      const choice = await this.chooseNextStar(this.stars, sourceChunk);
      if (choice.empty) {
        continue;
      }

      const selectedStar = choice.star;
      const rest = this.stars.filter(star => star !== selectedStar);
      const selectedIterator = new StarPatternIterator(
        new SingletonBindingChunkIterator(sourceChunk),
        selectedStar,
        this.source,
        this.context,
        this.maxMpR,
        choice.firstPage,
      );

      this.currentPipeline = rest.length === 0 ?
        selectedIterator :
        new BasicGraphPatternIterator(selectedIterator, rest, this.source, this.context, this.maxMpR);
    }

    const result = await this.currentPipeline.getNext();
    if (!result) {
      this.currentPipeline = undefined;
      return this.getNext();
    }
    return result;
  }

  private async chooseNextStar(stars: ISpfStar[], currentBindings: BindingChunk): Promise<ISpfStarChoice> {
    let best: ISpfStarChoice | undefined;
    let firstAbsentCount: ISpfStarChoice | undefined;
    const bound = hasBindings(currentBindings);

    for (const star of stars) {
      const firstPage = await this.source.fetchFirstSpfPage(star, currentBindings, this.context);
      const count = firstPage.cardinality;
      if (count === 0) {
        return { star, firstPage, count, empty: true };
      }
      if (count === undefined) {
        firstAbsentCount ??= { star, firstPage };
        continue;
      }
      if (!Number.isFinite(count) || count < 0) {
        throw new Error(`Malformed SPF metadata: invalid void:triples cardinality '${count}'.`);
      }
      if (!best || (bound ? count < best.count! : count > best.count!)) {
        best = { star, firstPage, count };
      }
    }

    return best ?? firstAbsentCount ?? {
      star: stars[0],
      firstPage: await this.source.fetchFirstSpfPage(stars[0], currentBindings, this.context),
    };
  }
}

class QueryIterator {
  private currentChunk: BindingChunk = [];

  public constructor(private readonly bgpIterator: BasicGraphPatternIterator) {}

  public async getNextBinding(): Promise<RDF.Bindings | undefined> {
    while (this.currentChunk.length === 0) {
      const chunk = await this.bgpIterator.getNext();
      if (!chunk) {
        return;
      }
      this.currentChunk = chunk;
    }
    return this.currentChunk.shift();
  }
}

class SpfBindingsIterator extends BufferedIterator<RDF.Bindings> {
  private emitted = 0;
  private readonly state = new MetadataValidationState();
  private readonly seenBindings = new Set<string>();

  public constructor(
    queryIteratorPromise: Promise<QueryIterator>,
    private readonly variables: MetadataBindings['variables'],
    maxBufferSize: number,
  ) {
    super({ autoStart: true, maxBufferSize });
    this.setProperty('metadata', {
      state: this.state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
    void this.pushAll(queryIteratorPromise);
  }

  private async pushAll(queryIteratorPromise: Promise<QueryIterator>): Promise<void> {
    try {
      const queryIterator = await queryIteratorPromise;
      let binding: RDF.Bindings | undefined;
      while ((binding = await queryIterator.getNextBinding())) {
        const key = bindingKey(binding);
        if (this.seenBindings.has(key)) {
          continue;
        }
        this.seenBindings.add(key);
        this.emitted++;
        this._push(binding);
      }
      this.setProperty('metadata', {
        state: new MetadataValidationState(),
        cardinality: { type: 'exact', value: this.emitted },
        variables: this.variables,
        next: [],
      } satisfies MetadataBindings);
      this.state.invalidate();
      this.close();
    } catch (error: unknown) {
      this.destroy(error as Error);
    }
  }

  public override _read(_count: number, done: () => void): void {
    done();
  }
}

function matchStarResponse(
  star: ISpfStar,
  quads: RDF.Quad[],
  bindingsFactory: BindingsFactory,
): RDF.Bindings[] {
  const triplesBySubject = new Map<string, RDF.Quad[]>();
  for (const quad of quads) {
    if (!isQuadLike(quad)) {
      throw new Error('Malformed SPF page: encountered a non-quad item in the data stream.');
    }
    const key = stableTermToString(quad.subject);
    const group = triplesBySubject.get(key) ?? [];
    group.push(quad);
    triplesBySubject.set(key, group);
  }

  const results: RDF.Bindings[] = [];
  for (const triplesForSubject of triplesBySubject.values()) {
    const records = matchStarGroup(star, triplesForSubject);
    for (const record of records) {
      results.push(bindingsFactory.fromRecord(record));
    }
  }
  return deduplicateBindings(results);
}

function matchStarGroup(star: ISpfStar, triplesForSubject: RDF.Quad[]): Record<string, RDF.Term>[] {
  let mappings: Record<string, RDF.Term>[] = [{}];

  for (const pattern of star.patterns) {
    const nextMappings: Record<string, RDF.Term>[] = [];
    for (const triple of triplesForSubject) {
      const partial = matchTriplePattern(pattern, triple);
      if (!partial) {
        continue;
      }
      for (const mapping of mappings) {
        const merged = mergeRecords(mapping, partial);
        if (merged) {
          nextMappings.push(merged);
        }
      }
    }
    mappings = deduplicateRecords(nextMappings);
    if (mappings.length === 0) {
      break;
    }
  }

  return mappings;
}

function matchTriplePattern(pattern: Algebra.Pattern, quad: RDF.Quad): Record<string, RDF.Term> | undefined {
  let mapping: Record<string, RDF.Term> = {};
  for (const [ patternTerm, quadTerm ] of [
    [ pattern.subject, quad.subject ],
    [ pattern.predicate, quad.predicate ],
    [ pattern.object, quad.object ],
    [ pattern.graph, quad.graph ],
  ] as [RDF.Term, RDF.Term][]) {
    const next = matchTerm(patternTerm, quadTerm, mapping);
    if (!next) {
      return;
    }
    mapping = next;
  }
  return mapping;
}

function matchTerm(
  patternTerm: RDF.Term,
  dataTerm: RDF.Term,
  mapping: Record<string, RDF.Term>,
): Record<string, RDF.Term> | undefined {
  if (patternTerm.termType !== 'Variable') {
    return patternTerm.equals(dataTerm) ? mapping : undefined;
  }

  const existing = mapping[patternTerm.value];
  if (existing) {
    return existing.equals(dataTerm) ? mapping : undefined;
  }
  return { ...mapping, [patternTerm.value]: dataTerm };
}

function joinBindingSets(
  left: BindingChunk,
  right: BindingChunk,
  bindingsFactory: BindingsFactory,
): BindingChunk {
  const results: RDF.Bindings[] = [];
  for (const leftBinding of left) {
    const leftRecord = bindingToRecord(leftBinding);
    for (const rightBinding of right) {
      const rightRecord = bindingToRecord(rightBinding);
      const merged = mergeRecords(leftRecord, rightRecord);
      if (merged) {
        results.push(bindingsFactory.fromRecord(merged));
      }
    }
  }
  return deduplicateBindings(results);
}

function mergeRecords(
  left: Record<string, RDF.Term>,
  right: Record<string, RDF.Term>,
): Record<string, RDF.Term> | undefined {
  const merged: Record<string, RDF.Term> = { ...left };
  for (const [ variable, value ] of Object.entries(right)) {
    if (merged[variable] && !merged[variable].equals(value)) {
      return;
    }
    merged[variable] = value;
  }
  return merged;
}

function encodeStar(star: ISpfStar): string {
  return star.patterns
    .flatMap((pattern, index) => {
      const position = index + 1;
      return [
        `p${position},${encodePatternTerm(pattern.predicate)}`,
        `o${position},${encodePatternTerm(pattern.object)}`,
      ];
    })
    .join(';')
    .replace(/^/, '[')
    .replace(/$/, ']');
}

function encodeValues(star: ISpfStar, bindings: BindingChunk): string | undefined {
  const fields = getBoundStarFields(star, bindings);
  if (fields.length === 0) {
    return;
  }

  const rows = bindings.map((binding) => {
    const values = fields.map((field) => {
      const term = binding.get(field.variable);
      return term ? termToStringTtl(term) : 'UNDEF';
    });
    return `(${values.join(' ')})`;
  });
  return `(${fields.map(field => `?${field.field}`).join(' ')}) { ${rows.join(' ')} }`;
}

function encodePatternTerm(term: RDF.Term): string {
  return term.termType === 'Variable' ? `?${term.value}` : termToStringTtl(term);
}

function getStarVariables(star: ISpfStar): Set<string> {
  const variables = new Set<string>();
  for (const pattern of star.patterns) {
    for (const term of [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]) {
      if (term.termType === 'Variable') {
        variables.add(term.value);
      }
    }
  }
  return variables;
}

function getBoundStarFields(
  star: ISpfStar,
  bindings: BindingChunk,
): { variable: string; field: string }[] {
  const fields: { variable: string; field: string }[] = [];
  const seen = new Set<string>();
  const addField = (term: RDF.Term, field: string): void => {
    if (term.termType !== 'Variable' || !bindings.some(binding => Boolean(binding.get(term.value)))) {
      return;
    }
    const key = `${term.value}:${field}`;
    if (!seen.has(key)) {
      seen.add(key);
      fields.push({ variable: term.value, field });
    }
  };

  addField(star.subject, 'subject');
  star.patterns.forEach((pattern, index) => {
    const position = index + 1;
    addField(pattern.predicate, `p${position}`);
    addField(pattern.object, `o${position}`);
  });
  return fields;
}

function hasBindings(bindings: BindingChunk): boolean {
  return bindings.some(binding => [ ...binding ].length > 0);
}

function collectPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
  const patterns: Algebra.Pattern[] = [];
  collectPatternsRecursive(operation, patterns, new Set<Algebra.Pattern>());
  return patterns;
}

function collectPatternsRecursive(
  operation: Algebra.Operation,
  patterns: Algebra.Pattern[],
  seen: Set<Algebra.Pattern>,
): void {
  if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
    if (!seen.has(operation)) {
      seen.add(operation);
      patterns.push(operation);
    }
    return;
  }

  const operationLike = operation as Algebra.Operation & {
    input?: Algebra.Operation | Algebra.Operation[];
    patterns?: Algebra.Pattern[];
  };
  if (Array.isArray(operationLike.input)) {
    for (const input of operationLike.input) {
      collectPatternsRecursive(input, patterns, seen);
    }
  } else if (operationLike.input) {
    collectPatternsRecursive(operationLike.input, patterns, seen);
  }
  if (Array.isArray(operationLike.patterns)) {
    for (const pattern of operationLike.patterns) {
      collectPatternsRecursive(pattern, patterns, seen);
    }
  }
}

function getOperationVariables(operation: Algebra.Operation): MetadataBindings['variables'] {
  const seen = new Set<string>();
  const variables: MetadataBindings['variables'] = [];
  for (const pattern of collectPatterns(operation)) {
    for (const term of [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]) {
      if (term.termType === 'Variable' && !seen.has(term.value)) {
        seen.add(term.value);
        variables.push({ variable: term, canBeUndef: false });
      }
    }
  }
  return variables;
}

function bindingToRecord(binding: RDF.Bindings): Record<string, RDF.Term> {
  const record: Record<string, RDF.Term> = {};
  for (const [ variable, value ] of binding) {
    record[variable.value] = value;
  }
  return record;
}

function deduplicateBindings(bindings: RDF.Bindings[]): RDF.Bindings[] {
  const seen = new Set<string>();
  const results: RDF.Bindings[] = [];
  for (const binding of bindings) {
    const key = bindingKey(binding);
    if (!seen.has(key)) {
      seen.add(key);
      results.push(binding);
    }
  }
  return results;
}

function deduplicateRecords(records: Record<string, RDF.Term>[]): Record<string, RDF.Term>[] {
  const seen = new Set<string>();
  const results: Record<string, RDF.Term>[] = [];
  for (const record of records) {
    const key = Object.entries(record)
      .map(([ variable, value ]) => `${variable}=${stableTermToString(value)}`)
      .sort()
      .join('|');
    if (!seen.has(key)) {
      seen.add(key);
      results.push(record);
    }
  }
  return results;
}

function bindingKey(binding: RDF.Bindings): string {
  return [ ...binding ]
    .map(([ variable, value ]) => `${variable.value}=${stableTermToString(value)}`)
    .sort()
    .join('|');
}

function extractVoidTriples(metadataQuads: RDF.Quad[]): number | undefined {
  for (const quad of metadataQuads) {
    if (quad.predicate.value === VOID_TRIPLES) {
      const value = Number.parseInt(quad.object.value, 10);
      if (Number.isNaN(value)) {
        throw new Error(`Malformed SPF metadata: void:triples value '${quad.object.value}' is not a number.`);
      }
      return value;
    }
  }
}

function extractMetadataCardinality(metadata: Record<string, any>): number | undefined {
  const cardinality = metadata.cardinality;
  const value = cardinality?.value;
  if (value === undefined) {
    return;
  }
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error(`Malformed SPF metadata: cardinality value '${String(value)}' is not a number.`);
  }
  if (value === 0 && !cardinality.dataset) {
    return;
  }
  return value;
}

function extractNextPageUrl(metadata: Record<string, any>): string | undefined {
  const next = metadata.next;
  if (Array.isArray(next)) {
    return next.find((url): url is string => typeof url === 'string');
  }
  return typeof next === 'string' ? next : undefined;
}

function collectRdfStream(stream: RDF.Stream, label: string): Promise<RDF.Quad[]> {
  return new Promise((resolve, reject) => {
    const quads: RDF.Quad[] = [];
    stream.on('error', error => reject(new Error(`Failed to parse ${label}: ${(error as Error).message}`)));
    stream.on('data', (quad: RDF.Quad) => {
      if (!isQuadLike(quad)) {
        reject(new Error(`Malformed ${label}: expected RDF/JS quads.`));
        return;
      }
      quads.push(quad);
    });
    stream.on('end', () => resolve(quads));
  });
}

function rdfStreamFromArray(quads: RDF.Quad[]): RDF.Stream {
  return Readable.from(quads, { objectMode: true }) as RDF.Stream;
}

function isQuadLike(quad: unknown): quad is RDF.Quad {
  return Boolean(quad) &&
    isTermLike((quad as RDF.Quad).subject) &&
    isTermLike((quad as RDF.Quad).predicate) &&
    isTermLike((quad as RDF.Quad).object) &&
    isTermLike((quad as RDF.Quad).graph);
}

function isTermLike(term: unknown): term is RDF.Term {
  return Boolean(term) &&
    typeof (term as RDF.Term).termType === 'string' &&
    typeof (term as RDF.Term).value === 'string' &&
    typeof (term as RDF.Term).equals === 'function';
}

function getHydraPropertyName(property: string): string {
  const decoded = decodeURIComponent(property);
  const hashIndex = decoded.lastIndexOf('#');
  const slashIndex = decoded.lastIndexOf('/');
  const colonIndex = decoded.lastIndexOf(':');
  const index = Math.max(hashIndex, slashIndex, colonIndex);
  return decoded.slice(index + 1).toLowerCase();
}

function stableTermToString(term: RDF.Term): string {
  return termToString(term);
}

function stripQueryAndHash(url: string): string {
  const parsed = new URL(url);
  parsed.search = '';
  parsed.hash = '';
  return parsed.toString().replace(/\/$/, '');
}
