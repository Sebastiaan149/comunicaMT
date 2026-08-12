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
import type { AlgebraFactory } from '@comunica/utils-algebra';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import { BufferedIterator, EmptyIterator } from 'asynciterator';
import { termToString } from 'rdf-string';
import { termToString as termToStringTtl } from 'rdf-string-ttl';
import { Readable } from 'readable-stream';

const VOID_TRIPLES = 'http://rdfs.org/ns/void#triples';
// Keep one VALUES block aligned with the server's bounded fragment page.
const DEFAULT_MAX_MPR = 100;
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
  getNext: () => Promise<BindingChunk | undefined>;
}

// Query source that evaluates BGPs through Star Pattern Fragment requests.
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

  // Expose the SPF selector shape for patterns, BGPs, and joins.
  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  // Decompose the operation into subject stars and stream joined bindings.
  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    const patterns = this.getOperationPatterns(operation);
    const bgp = this.algebraFactory.createBgp(patterns);
    const variables = getPatternVariables(patterns);
    const rootIterator: IBindingChunkIterator = options?.joinBindings ?
      new BindingsStreamChunkIterator(options.joinBindings.bindings, this.maxMpR) :
      new RootBindingChunkIterator(this.bindingsFactory.bindings());
    return <BindingsStream> <unknown> new SpfBindingsIterator(
      new QueryIterator(
        new BasicGraphPatternIterator(
          rootIterator,
          decomposeSubjectStars(bgp.patterns),
          this,
          context,
          this.maxMpR,
        ),
      ),
      variables,
      this.maxMpR,
    );
  }

  // Return no quads because SPF is used through bindings in this actor.
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

  // Dereference one SPF page. Like QuerySourceQpf.match, this is the single
  // request path for initial and continuation fragments.
  public async fetchSpfPage(
    star: ISpfStar,
    bindings: BindingChunk,
    context: IActionContext,
    pageUrl?: string,
  ): Promise<ISpfPage> {
    const url = createSpfRequestUrl({
      searchForm: this.searchForm,
      mappings: this.searchMappings,
    }, star, deduplicateBindings(bindings), this.maxMpR, pageUrl);
    const dereferenceRdfOutput = await this.mediatorDereferenceRdf.mediate({ context, url });
    const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate({
      context,
      url: dereferenceRdfOutput.url,
      quads: dereferenceRdfOutput.data,
      triples: dereferenceRdfOutput.metadata?.triples,
    });

    // The metadata actor tees the response into metadata and data streams. Drain
    // both branches concurrently: awaiting one branch first makes the other one
    // buffer a complete page and can deadlock when stream high-water marks fill.
    const [ metadataQuads, quads ] = await Promise.all([
      collectRdfStream(rdfMetadataOutput.metadata, 'SPF metadata'),
      collectRdfStream(rdfMetadataOutput.data, 'SPF data'),
    ]);
    const { metadata } = await this.mediatorMetadataExtract.mediate({
      context,
      url: dereferenceRdfOutput.url,
      metadata: rdfStreamFromArray(metadataQuads),
      requestTime: dereferenceRdfOutput.requestTime,
      headers: dereferenceRdfOutput.headers,
    });
    return {
      url: dereferenceRdfOutput.url,
      metadata,
      quads,
      cardinality: extractVoidTriples(metadataQuads) ?? extractMetadataCardinality(metadata),
      nextPageUrl: extractNextPageUrl(metadata),
    };
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
      return collectPatterns(operation);
    }
    throw new Error(`Attempted to pass unsupported operation '${operation.type}' to QuerySourceSpf.`);
  }
}

// Detect whether a query source instance is the SPF implementation.
export function isQuerySourceSpf(source: unknown): source is QuerySourceSpf {
  return Boolean(source) && (<{ sourceType?: string }> source).sourceType === SOURCE_TYPE;
}

// Detect an SPF Hydra search form in RDF metadata.
export function detectSpfSearchForm(metadata: Record<string, any>): ISpfSourceControl | undefined {
  const searchForms = metadata.searchForms?.values;
  if (!Array.isArray(searchForms)) {
    return;
  }

  for (const searchForm of <ISearchForm[]> searchForms) {
    const mappings = detectSpfMappings(searchForm);
    if (mappings) {
      return { searchForm, mappings };
    }
  }
}

// Build a fallback SPF search form for forced SPF sources.
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

// Map Hydra template variables to the SPF fields required by the protocol.
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

// Split a BGP into maximal subject stars. SPF accepts an arbitrary number of
// triple patterns per star, so keeping them intact lets the server perform the
// complete star join and avoids sending its intermediate bindings to the client.
export function decomposeSubjectStars(patterns: Algebra.Pattern[]): ISpfStar[] {
  const stars = new Map<string, ISpfStar>();
  for (const pattern of patterns) {
    const key = stableTermToString(pattern.subject);
    const star = stars.get(key);
    if (star) {
      star.patterns.push(pattern);
    } else {
      stars.set(key, { subject: pattern.subject, patterns: [ pattern ]});
    }
  }
  return [ ...stars.values() ];
}

// Build the concrete SPF request URL for one star and one binding chunk.
export function createSpfRequestUrl(
  control: ISpfSourceControl,
  star: ISpfStar,
  bindings: BindingChunk,
  maxMpR: number = DEFAULT_MAX_MPR,
  pageUrl?: string,
): string {
  // A one-pattern star is exactly TPF/brTPF. Use that mature server path
  // directly instead of invoking the SPF server's star-join machinery.
  if (star.patterns.length === 1) {
    return createTpfRequestUrl(control, star.patterns[0], bindings, maxMpR, pageUrl);
  }

  const entries: Record<string, string> = {};
  entries[control.mappings.subject] = encodePatternTerm(star.subject);
  entries[control.mappings.triples] = String(star.patterns.length);
  entries[control.mappings.star] = encodeStar(star);

  const values = encodeValues(star, deduplicateBindingsForStarFields(star, bindings).slice(0, maxMpR));
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

function createTpfRequestUrl(
  control: ISpfSourceControl,
  pattern: Algebra.Pattern,
  bindings: BindingChunk,
  maxMpR: number,
  pageUrl?: string,
): string {
  const url = new URL(control.searchForm.dataset);
  url.searchParams.set('subject', encodePatternTerm(pattern.subject));
  url.searchParams.set('predicate', encodePatternTerm(pattern.predicate));
  url.searchParams.set('object', encodePatternTerm(pattern.object));

  const variables = [ pattern.subject, pattern.predicate, pattern.object ]
    .filter((term): term is RDF.Variable => term.termType === 'Variable');
  const relevant = deduplicateBindings(bindings).slice(0, maxMpR);
  if (variables.some(variable => relevant.some(binding => binding.has(variable)))) {
    const rows = relevant.map(binding => `(${variables
      .map(variable => binding.get(variable) ? termToStringTtl(binding.get(variable)!) : 'UNDEF')
      .join(' ')})`);
    url.searchParams.set(
      'values',
      `(${variables.map(variable => `?${variable.value}`).join(' ')}) { ${rows.join(' ')} }`,
    );
  }
  if (pageUrl) {
    const page = new URL(pageUrl).searchParams.get('page');
    if (page) {
      url.searchParams.set('page', page);
    }
  }
  return url.toString();
}

// Emits the initial empty binding chunk that starts BGP evaluation.
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

// Reads incoming join bindings in bounded chunks for SPF VALUES requests.
class BindingsStreamChunkIterator implements IBindingChunkIterator {
  private readonly iterator: AsyncIterator<RDF.Bindings>;

  public constructor(bindings: BindingsStream, private readonly chunkSize: number) {
    this.iterator = bindings[Symbol.asyncIterator]();
  }

  public async getNext(): Promise<BindingChunk | undefined> {
    const bindings: RDF.Bindings[] = [];
    while (bindings.length < this.chunkSize) {
      const next = await this.iterator.next();
      if (next.done) {
        break;
      }
      bindings.push(next.value);
    }
    return bindings.length > 0 ? bindings : undefined;
  }
}

// Emits one precomputed binding chunk for the next star pipeline stage.
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

// Evaluates one subject star against SPF pages and joins page matches.
class StarPatternIterator implements IBindingChunkIterator {
  private currentPage: ISpfPage | undefined;
  private currentSourceChunk: BindingChunk | undefined;
  private unreadMappings: BindingChunk = [];
  private firstPageConsumed = false;
  private previousPageMappings = new Set<string>();

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
        this.currentPage = await this.source.fetchSpfPage(
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
        this.previousPageMappings.clear();
        this.currentPage = await this.fetchFirstPageForCurrentChunk();
        this.unreadMappings = this.joinCurrentPage();
      }
    }

    return this.unreadMappings.splice(0, this.maxMpR);
  }

  // Reuse an already-fetched first page when star ordering selected it.
  private async fetchFirstPageForCurrentChunk(): Promise<ISpfPage> {
    if (!this.firstPageConsumed && this.firstPage) {
      this.firstPageConsumed = true;
      return this.firstPage;
    }
    return this.source.fetchSpfPage(this.star, this.currentSourceChunk!, this.context);
  }

  // Join current page matches with the current input chunk.
  private joinCurrentPage(): BindingChunk {
    if (!this.currentPage || !this.currentSourceChunk) {
      return [];
    }
    const pageBindings = this.source.matchPageToStarBindings(this.star, this.currentPage);
    // The server serializes a page's answer-stars as one RDF graph. When a
    // subject straddles two pages, reconstructing bindings from that graph can
    // repeat mappings from the preceding page. Suppress only this bounded page
    // overlap instead of retaining every result of the query.
    const currentPageMappings = new Set<string>();
    const starBindings: RDF.Bindings[] = [];
    for (const binding of pageBindings) {
      const key = bindingKey(binding);
      currentPageMappings.add(key);
      if (!this.previousPageMappings.has(key)) {
        starBindings.push(binding);
      }
    }
    this.previousPageMappings = currentPageMappings;
    return this.source.joinBindingChunks(this.currentSourceChunk, starBindings);
  }
}

// Chains star iterators together to evaluate the complete BGP.
class BasicGraphPatternIterator implements IBindingChunkIterator {
  private currentPipeline: IBindingChunkIterator | undefined;
  private selectedStar: ISpfStar | undefined;

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

      const choice = this.selectedStar ?
          {
            star: this.selectedStar,
            firstPage: await this.source.fetchSpfPage(this.selectedStar, sourceChunk, this.context),
          } :
        await this.chooseNextStar(this.stars, sourceChunk);
      if (choice.empty) {
        continue;
      }

      const selectedStar = choice.star;
      this.selectedStar = selectedStar;
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

  // Select the next star using available cardinality estimates.
  private async chooseNextStar(stars: ISpfStar[], currentBindings: BindingChunk): Promise<ISpfStarChoice> {
    let best: ISpfStarChoice | undefined;
    let firstAbsentCount: ISpfStarChoice | undefined;

    for (const star of stars) {
      const firstPage = await this.source.fetchSpfPage(star, currentBindings, this.context);
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
      if (!best || count < best.count!) {
        best = { star, firstPage, count };
      }
    }

    return best ?? firstAbsentCount ?? {
      star: stars[0],
      firstPage: await this.source.fetchSpfPage(stars[0], currentBindings, this.context),
    };
  }
}

// Flattens binding chunks into one binding at a time for the stream wrapper.
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

// Adapts the SPF query iterator to a Comunica bindings stream.
class SpfBindingsIterator extends BufferedIterator<RDF.Bindings> {
  private emitted = 0;
  private readonly state = new MetadataValidationState();
  private reading = false;
  private sourceEnded = false;
  private readonly readBatchSize: number;

  public constructor(
    private readonly queryIterator: QueryIterator,
    private readonly variables: MetadataBindings['variables'],
    maxBufferSize: number,
  ) {
    super({ autoStart: true, maxBufferSize });
    this.readBatchSize = Math.max(1, Math.min(maxBufferSize, 64));
    this.setProperty('metadata', {
      state: this.state,
      cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
      variables,
      next: [],
    } satisfies MetadataBindings);
  }

  public override _read(_count: number, done: () => void): void {
    if (this.reading || this.sourceEnded) {
      done();
      return;
    }
    this.reading = true;
    this.readNext().then(() => {
      this.reading = false;
      done();
    }, (error: unknown) => {
      this.reading = false;
      this.destroy(<Error> error);
      done();
    });
  }

  private async readNext(): Promise<void> {
    let pushed = 0;
    while (pushed < this.readBatchSize) {
      const binding = await this.queryIterator.getNextBinding();
      if (!binding) {
        this.finish();
        return;
      }
      this.emitted++;
      this._push(binding);
      pushed++;
    }
  }

  private finish(): void {
    this.sourceEnded = true;
    this.setProperty('metadata', {
      state: new MetadataValidationState(),
      cardinality: { type: 'exact', value: this.emitted },
      variables: this.variables,
      next: [],
    } satisfies MetadataBindings);
    this.state.invalidate();
    this.close();
  }
}

// Match all quads in an SPF page against the requested subject star.
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

// Match one subject group against every triple pattern in the star.
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

// Match a single quad against one algebra pattern.
function matchTriplePattern(pattern: Algebra.Pattern, quad: RDF.Quad): Record<string, RDF.Term> | undefined {
  let mapping: Record<string, RDF.Term> = {};
  for (const [ patternTerm, quadTerm ] of <[RDF.Term, RDF.Term][]> [
    [ pattern.subject, quad.subject ],
    [ pattern.predicate, quad.predicate ],
    [ pattern.object, quad.object ],
    [ pattern.graph, quad.graph ],
  ]) {
    const next = matchTerm(patternTerm, quadTerm, mapping);
    if (!next) {
      return;
    }
    mapping = next;
  }
  return mapping;
}

// Bind or compare one RDF term during pattern matching.
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

// Join two binding chunks and remove duplicate bindings.
function joinBindingSets(
  left: BindingChunk,
  right: BindingChunk,
  bindingsFactory: BindingsFactory,
): BindingChunk {
  const results: RDF.Bindings[] = [];

  // Index the right-hand page on a shared, frequently-bound variable. SPF
  // VALUES responses normally share the star subject (or an object joining two
  // stars), so this changes the hot path from |left|*|right| comparisons to
  // linear index construction plus small compatibility buckets. Bindings where
  // the index variable is UNDEF remain wildcard candidates, as required by
  // SPARQL compatibility.
  const indexVariable = findJoinIndexVariable(left, right);
  if (indexVariable) {
    const index = new Map<string, RDF.Bindings[]>();
    const unbound: RDF.Bindings[] = [];
    for (const rightBinding of right) {
      const value = rightBinding.get(indexVariable);
      if (!value) {
        unbound.push(rightBinding);
        continue;
      }
      const key = stableTermToString(value);
      const bucket = index.get(key);
      if (bucket) {
        bucket.push(rightBinding);
      } else {
        index.set(key, [ rightBinding ]);
      }
    }

    for (const leftBinding of left) {
      const value = leftBinding.get(indexVariable);
      if (value) {
        appendCompatibleBindings(
          leftBinding,
          index.get(stableTermToString(value)) ?? [],
          results,
          bindingsFactory,
        );
        appendCompatibleBindings(leftBinding, unbound, results, bindingsFactory);
      } else {
        appendCompatibleBindings(leftBinding, right, results, bindingsFactory);
      }
    }
    return results;
  }

  for (const leftBinding of left) {
    appendCompatibleBindings(leftBinding, right, results, bindingsFactory);
  }
  // Do not deduplicate here: SPARQL uses bag semantics, and retaining a
  // query-sized Set of emitted bindings makes large result sets unbounded.
  return results;
}

function appendCompatibleBindings(
  leftBinding: RDF.Bindings,
  candidates: RDF.Bindings[],
  output: RDF.Bindings[],
  bindingsFactory: BindingsFactory,
): void {
  const leftRecord = bindingToRecord(leftBinding);
  for (const rightBinding of candidates) {
    const merged = mergeRecords(leftRecord, bindingToRecord(rightBinding));
    if (merged) {
      output.push(bindingsFactory.fromRecord(merged));
    }
  }
}

function findJoinIndexVariable(left: BindingChunk, right: BindingChunk): string | undefined {
  const leftCounts = countBoundVariables(left);
  const rightCounts = countBoundVariables(right);
  let best: string | undefined;
  let bestScore = 0;
  for (const [ variable, leftCount ] of leftCounts) {
    const rightCount = rightCounts.get(variable) ?? 0;
    const score = leftCount * rightCount;
    if (score > bestScore) {
      best = variable;
      bestScore = score;
    }
  }
  return best;
}

function countBoundVariables(bindings: BindingChunk): Map<string, number> {
  const counts = new Map<string, number>();
  for (const binding of bindings) {
    for (const [ variable ] of binding) {
      counts.set(variable.value, (counts.get(variable.value) ?? 0) + 1);
    }
  }
  return counts;
}

// Merge two variable records when shared variables agree.
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

// Encode an SPF star parameter from predicate and object positions.
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
    .replace(/^/u, '[')
    .replace(/$/u, ']');
}

// Encode VALUES bindings for variables that are already bound.
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

// Deduplicate bindings by fields that are relevant to the current star.
function deduplicateBindingsForStarFields(star: ISpfStar, bindings: BindingChunk): BindingChunk {
  const fields = getBoundStarFields(star, bindings);
  if (fields.length === 0) {
    return deduplicateBindings(bindings);
  }

  const seen = new Set<string>();
  const results: BindingChunk = [];
  for (const binding of bindings) {
    const key = fields
      .map((field) => {
        const term = binding.get(field.variable);
        return `${field.field}=${term ? stableTermToString(term) : 'UNDEF'}`;
      })
      .join('|');
    if (!seen.has(key)) {
      seen.add(key);
      results.push(binding);
    }
  }
  return results;
}

// Encode variables and RDF terms in SPF URL parameters.
function encodePatternTerm(term: RDF.Term): string {
  return term.termType === 'Variable' ? `?${term.value}` : termToStringTtl(term);
}

// Determine which star fields have bound values in the input chunk.
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
  for (const [ index, pattern ] of star.patterns.entries()) {
    const position = index + 1;
    addField(pattern.predicate, `p${position}`);
    addField(pattern.object, `o${position}`);
  }
  return fields;
}

function collectPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
  if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
    return [ operation ];
  }
  if (!isKnownOperation(operation, Algebra.Types.JOIN)) {
    return [];
  }
  return operation.input.flatMap(collectPatterns);
}

function getPatternVariables(patterns: Algebra.Pattern[]): MetadataBindings['variables'] {
  const seen = new Set<string>();
  const variables: MetadataBindings['variables'] = [];
  for (const pattern of patterns) {
    for (const term of [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]) {
      if (term.termType === 'Variable' && !seen.has(term.value)) {
        seen.add(term.value);
        variables.push({ variable: term, canBeUndef: false });
      }
    }
  }
  return variables;
}

// Convert an RDF/JS bindings object into a plain variable record.
function bindingToRecord(binding: RDF.Bindings): Record<string, RDF.Term> {
  const record: Record<string, RDF.Term> = {};
  for (const [ variable, value ] of binding) {
    record[variable.value] = value;
  }
  return record;
}

// Remove duplicate RDF/JS binding objects by stable term strings.
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

// Remove duplicate plain variable records by stable term strings.
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

// Create a stable string key for an RDF/JS bindings object.
function bindingKey(binding: RDF.Bindings): string {
  return [ ...binding ]
    .map(([ variable, value ]) => `${variable.value}=${stableTermToString(value)}`)
    .sort()
    .join('|');
}

// Extract void:triples cardinality from metadata quads.
function extractVoidTriples(metadataQuads: RDF.Quad[]): number | undefined {
  for (const quad of metadataQuads) {
    if (quad.predicate.value === VOID_TRIPLES) {
      const value = Number.parseInt(quad.object.value, 10);
      if (Number.isNaN(value)) {
        throw new TypeError(`Malformed SPF metadata: void:triples value '${quad.object.value}' is not a number.`);
      }
      return value;
    }
  }
}

// Extract cardinality from Comunica metadata when available.
function extractMetadataCardinality(metadata: Record<string, any>): number | undefined {
  const cardinality = metadata.cardinality;
  const value = cardinality?.value;
  if (value === undefined) {
    return;
  }
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new TypeError(`Malformed SPF metadata: cardinality value '${String(value)}' is not a number.`);
  }
  if (value === 0 && !cardinality.dataset) {
    return;
  }
  return value;
}

// Extract the next page URL from Comunica metadata.
function extractNextPageUrl(metadata: Record<string, any>): string | undefined {
  const next = metadata.next;
  if (Array.isArray(next)) {
    return next.find((url): url is string => typeof url === 'string');
  }
  return typeof next === 'string' ? next : undefined;
}

// Collect an RDF stream into an array while validating quad-like values.
function collectRdfStream(stream: RDF.Stream, label: string): Promise<RDF.Quad[]> {
  return new Promise((resolve, reject) => {
    const quads: RDF.Quad[] = [];
    stream.on('error', error => reject(new Error(`Failed to parse ${label}: ${(<Error> error).message}`)));
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

// Convert collected quads back into an RDF stream for metadata extraction.
function rdfStreamFromArray(quads: RDF.Quad[]): RDF.Stream {
  return <RDF.Stream> Readable.from(quads, { objectMode: true });
}

// Validate that a value looks like an RDF/JS quad.
function isQuadLike(quad: unknown): quad is RDF.Quad {
  return Boolean(quad) &&
    isTermLike((<RDF.Quad> quad).subject) &&
    isTermLike((<RDF.Quad> quad).predicate) &&
    isTermLike((<RDF.Quad> quad).object) &&
    isTermLike((<RDF.Quad> quad).graph);
}

// Validate that a value looks like an RDF/JS term.
function isTermLike(term: unknown): term is RDF.Term {
  return Boolean(term) &&
    typeof (<RDF.Term> term).termType === 'string' &&
    typeof (<RDF.Term> term).value === 'string' &&
    typeof (<RDF.Term> term).equals === 'function';
}

// Normalize Hydra property IRIs to their local names.
function getHydraPropertyName(property: string): string {
  const decoded = decodeURIComponent(property);
  const hashIndex = decoded.lastIndexOf('#');
  const slashIndex = decoded.lastIndexOf('/');
  const colonIndex = decoded.lastIndexOf(':');
  const index = Math.max(hashIndex, slashIndex, colonIndex);
  return decoded.slice(index + 1).toLowerCase();
}

// Convert an RDF term to the canonical string used for grouping.
function stableTermToString(term: RDF.Term): string {
  return termToString(term);
}

// Remove query and hash parts from a dataset URL.
function stripQueryAndHash(url: string): string {
  const parsed = new URL(url);
  parsed.search = '';
  parsed.hash = '';
  return parsed.toString().replace(/\/$/u, '');
}
