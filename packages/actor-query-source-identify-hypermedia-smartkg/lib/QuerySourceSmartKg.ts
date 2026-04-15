import { createReadStream, createWriteStream, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import type { BindingsStream, IActionContext, IQueryBindingsOptions } from '@comunica/types';
import type { ComunicaDataFactory } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import type {
  FragmentSelectorShape,
  IQuerySource,
} from '@comunica/types';
import {
  Algebra,
  AlgebraFactory,
  isKnownOperation,
} from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import * as HDT from 'hdt';
import type { Mediator, Actor, IActorTest } from '@comunica/core';
import { EmptyIterator, BufferedIterator, TransformIterator, UnionIterator, AsyncIterator } from 'asynciterator';
import { Readable } from 'stream';
import { termToString } from 'rdf-string';
const stringifyStream = require('stream-to-string');

/**
 * Type definition for HDT searchBindings result
 */
interface HDTBindingsResult {
  bindings: RDF.Bindings[];
}
import type {
  ISmartKgFamily,
  ISmartKgMetadata,
} from './Utils';
import {
  extractPredicates,
  findMatchingFamilies,
  hasInfrequentPredicate,
  metadataToSets,
  parseSmartKgMetadata,
  selectOptimalFamilies,
} from './Utils';

/**
 * Cache for loaded HDT documents to avoid reloading
 */
class HdtDocumentCache {
  private readonly cache: Map<string, { document: HDT.Document; refCount: number }> = new Map();

  public async getDocument(path: string): Promise<HDT.Document> {
    const existing = this.cache.get(path);
    if (existing) {
      existing.refCount++;
      return existing.document;
    }
    const document = await HDT.fromFile(path);
    this.cache.set(path, { document, refCount: 1 });
    return document;
  }

  public releaseDocument(path: string): void {
    const entry = this.cache.get(path);
    if (entry) {
      entry.refCount--;
      if (entry.refCount === 0) {
        // Keep the document in cache for reuse, only dispose if needed later
      }
    }
  }

  public async dispose(): Promise<void> {
    for (const { document } of this.cache.values()) {
      // HDT documents don't always have explicit dispose, but we clear the cache
    }
    this.cache.clear();
  }
}

/**
 * Bindings iterator that queries HDT documents
 */
class HdtBindingsIterator extends BufferedIterator<RDF.Bindings> {
  protected readonly hdtDocument: HDT.Document;
  protected readonly bindingsFactory: BindingsFactory;
  protected readonly subject: RDF.Term;
  protected readonly predicate: RDF.Term;
  protected readonly object: RDF.Term;
  protected position: number;

  public constructor(
    hdtDocument: any,
    bindingsFactory: BindingsFactory,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    options: any = {},
  ) {
    super(options);
    this.hdtDocument = hdtDocument;
    this.bindingsFactory = bindingsFactory;
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.position = 0;

    // Set metadata with cardinality
    this.hdtDocument.countTriples(subject, predicate, object)
      .then(({ totalCount, hasExactCount }: { totalCount: number; hasExactCount: boolean }) => {
        const variables: any[] = [];
        if (subject.termType === 'Variable') {
          variables.push({ variable: subject, canBeUndef: false });
        }
        if (predicate.termType === 'Variable' && !variables.some(v => v.variable.equals(predicate))) {
          variables.push({ variable: predicate, canBeUndef: false });
        }
        if (object.termType === 'Variable' && !variables.some(v => v.variable.equals(object))) {
          variables.push({ variable: object, canBeUndef: false });
        }

        this.setProperty('metadata', {
          state: { valid: true },
          cardinality: { type: hasExactCount ? 'exact' : 'estimate', value: totalCount },
          variables,
        });
      })
      .catch((error: any) => this.destroy(error));
  }

  public override _read(count: number, done: () => void): void {
    if ((this.hdtDocument as any).closed) {
      this.close();
      return done();
    }
    
    this.hdtDocument.searchBindings(
      this.bindingsFactory,
      this.subject,
      this.predicate,
      this.object,
      { offset: this.position, limit: count },
    ).then((result: any) => {
      for (const binding of result.bindings) {
        this._push(binding);
      }
      if (result.bindings.length < count) {
        this.close();
      }
      this.position += count;
      done();
    })
      .catch((error: Error) => {
        this.emit('error', error);
        done();
      });
  }
}

/**
 * QuerySource for SmartKG - queries compressed KG partitions
 */
export class QuerySourceSmartKg implements IQuerySource {
  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly baseUrl: string;
  private readonly mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>,
  IActionHttp, IActorTest, IActorHttpOutput>;
  private readonly context: IActionContext;
  private readonly cacheFolder: string;
  private cachedMetadata: ISmartKgMetadata | undefined;
  private readonly hdtCache: HdtDocumentCache;
  private bindingsFactory: BindingsFactory | undefined;

  public readonly referenceValue: string;

  public constructor(
    url: string,
    dataFactory: ComunicaDataFactory,
    mediatorHttp: Mediator<Actor<IActionHttp, IActorTest, IActorHttpOutput>,
    IActionHttp, IActorTest, IActorHttpOutput>,
    context: IActionContext,
  ) {
    this.referenceValue = url;
    this.baseUrl = url;
    this.dataFactory = dataFactory;
    this.mediatorHttp = mediatorHttp;
    this.context = context;
    this.hdtCache = new HdtDocumentCache();

    // Initialize cache folder
    this.cacheFolder = join(process.cwd(), '.smartkg-cache');
    if (!existsSync(this.cacheFolder)) {
      mkdirSync(this.cacheFolder, { recursive: true });
    }

    const AF = new AlgebraFactory(<RDF.DataFactory> this.dataFactory);
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: AF.createPattern(
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
    };
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    // SmartKG provides filtered data through partitioning
    return 0.5;
  }

  /**
   * Get or create bindings factory
   */
  private async getBindingsFactory(): Promise<BindingsFactory> {
    if (!this.bindingsFactory) {
      const { BindingsFactory: BF } = await import('@comunica/utils-bindings-factory');
      this.bindingsFactory = new BF(this.dataFactory);
    }
    return this.bindingsFactory;
  }

  /**
   * Fetch content from URL and cache it locally
   */
  private async fetchAndCache(uri: string): Promise<string> {
    const localPath = join(this.cacheFolder, encodeURIComponent(uri));

    // Return from cache if exists
    if (existsSync(localPath)) {
      return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        const stream = createReadStream(localPath);
        stream.on('data', (chunk: any) => chunks.push(chunk as Buffer));
        stream.on('end', () => {
          const buffer = Buffer.concat(chunks);
          resolve(buffer.toString('utf-8'));
        });
        stream.on('error', reject);
      });
    }

    // Fetch from URL
    const httpResponse: IActorHttpOutput = await this.mediatorHttp.mediate({
      context: this.context,
      input: uri,
    });
    const readableStream = ActorHttp.toNodeReadable(httpResponse.body);
    const content = await stringifyStream(readableStream);

    // Cache the result
    return new Promise((resolve, reject) => {
      const writeStream = createWriteStream(localPath);
      const buffer = typeof content === 'string' ? Buffer.from(content) : (content as Buffer);
      writeStream.on('finish', () => {
        resolve(content);
      });
      writeStream.on('error', (err) => {
        reject(err);
      });
      writeStream.write(buffer);
      writeStream.end();
    });
  }

  /**
   * Fetch SmartKG metadata from server
   */
  private async fetchMetadata(): Promise<ISmartKgMetadata> {
    if (this.cachedMetadata) {
      return this.cachedMetadata;
    }

    const baseUrlClean = this.baseUrl.replace(/\/$/, '');
    const metadataUrl = `${baseUrlClean}/molecule/smartkg`;
    
    const metadataJson = await this.fetchAndCache(metadataUrl);
    this.cachedMetadata = parseSmartKgMetadata(metadataJson);
    return this.cachedMetadata;
  }

  /**
   * Fetch HDT file and return local path
   */
  private async fetchHdtFile(familyName: string): Promise<string> {
    const hdtUrl = `${this.baseUrl.replace(/\/$/, '')}/molecule/smartkg/${familyName}`;
    const localPath = join(this.cacheFolder, encodeURIComponent(hdtUrl));

    if (existsSync(localPath)) {
      return localPath;
    }

    // Fetch the HDT file
    const httpResponse: IActorHttpOutput = await this.mediatorHttp.mediate({
      context: this.context,
      input: hdtUrl,
    });
    const readableStream = ActorHttp.toNodeReadable(httpResponse.body);

    return new Promise((resolve, reject) => {
      const writeStream = createWriteStream(localPath);
      readableStream.pipe(writeStream);
      writeStream.on('finish', () => resolve(localPath));
      writeStream.on('error', reject);
    });
  }

  /**
   * Query HDT file for matching bindings
   */
  private async queryHdtPartition(
    hdtPath: string,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
  ): Promise<BindingsStream> {
    // Load HDT document from cache
    const hdtDocument = await this.hdtCache.getDocument(hdtPath);
    const bindingsFactory = await this.getBindingsFactory();

    // Create an iterator that will query the HDT document
    return new HdtBindingsIterator(
      hdtDocument,
      bindingsFactory,
      subject,
      predicate,
      object,
      { autoStart: false, maxBufferSize: 128 },
    );
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    _options?: IQueryBindingsOptions,
  ): BindingsStream {
    if (!isKnownOperation(operation, Algebra.Types.PATTERN)) {
      throw new Error(
        `Attempted to pass non-pattern operation '${operation.type}' to QuerySourceSmartKg`,
      );
    }

    const pattern = operation as Algebra.Pattern;
    const self = this;

    // Create an async function that sets up and returns the source iterator
    const setupQuery = async function*(): AsyncIterable<RDF.Bindings> {
      try {
        // Fetch metadata to determine which families to query
        const metadata = await self.fetchMetadata();
        
        const predicates = extractPredicates([pattern]);
        
        const infrequentSet = new Set(metadata.infrequentPredicates);
        
        if (hasInfrequentPredicate([pattern], infrequentSet)) {
          return;
        }
        
        const matchingFamilies = findMatchingFamilies(predicates, metadata.families, infrequentSet);
        
        if (matchingFamilies.length === 0) {
          return;
        }
        
        const optimalFamilies = selectOptimalFamilies(matchingFamilies);
        
        const familyIterators: BindingsStream[] = [];
        
        for (const optimizedFamily of optimalFamilies) {
          const hdtPath = await self.fetchHdtFile(optimizedFamily.name);
          
          const iter = await self.queryHdtPartition(
            hdtPath,
            pattern.subject,
            pattern.predicate,
            pattern.object,
          );
          
          familyIterators.push(iter);
        }
        
        if (familyIterators.length === 0) {
          return;
        }

        const sourceIterator = familyIterators.length === 1
          ? familyIterators[0]
          : new UnionIterator<RDF.Bindings>(familyIterators);
        
        // Yield all bindings from the source iterator
        for await (const binding of sourceIterator) {
          yield binding;
        }
      } catch (error) {
        throw error;
      }
    };

    // Convert async iterable to a stream by properly implementing _read
    class SmartKgAsyncIteratorAdapter extends BufferedIterator<RDF.Bindings> {
      private readonly asyncIterable: AsyncIterable<RDF.Bindings>;
      private hasSetMetadata = false;

      public constructor(asyncIterable: AsyncIterable<RDF.Bindings>) {
        super({ autoStart: true, maxBufferSize: 1000 });
        this.asyncIterable = asyncIterable;
        this.startIteration();
      }

      private async startIteration(): Promise<void> {
        try {
          // Collect first binding to extract metadata
          let firstBinding: RDF.Bindings | undefined;
          for await (const binding of this.asyncIterable) {
            if (!this.hasSetMetadata && !firstBinding) {
              firstBinding = binding;
              // Set metadata based on the first binding's variables
              const variables: any[] = [];
              for (const key of binding.keys()) {
                variables.push({ variable: key, canBeUndef: false });
              }
              this.setProperty('metadata', {
                state: { valid: true },
                cardinality: { type: 'estimate', value: 100000 },
                variables,
              });
              this.hasSetMetadata = true;
            }
            this._push(binding);
          }
          this.close();
        } catch (error) {
          this.destroy(error as Error);
        }
      }

      public override _read(count: number, done: () => void): void {
        done();
      }
    }

    return new SmartKgAsyncIteratorAdapter(setupQuery()) as BindingsStream;
  }

  public toString(): string {
    return 'QuerySourceSmartKg';
  }

  public queryQuads(
    operation: Algebra.Operation,
    _context: IActionContext,
  ): any {
    if (!isKnownOperation(operation, Algebra.Types.PATTERN)) {
      throw new Error(
        `Attempted to pass non-pattern operation '${operation.type}' to QuerySourceSmartKg`,
      );
    }

    // For now, return empty iterator
    // TODO: Implement quad retrieval by converting bindings to quads
    return new EmptyIterator();
  }

  public async queryBoolean(): Promise<boolean> {
    throw new Error('ASK queries not supported by SmartKG source');
  }

  public async queryVoid(): Promise<void> {
    throw new Error('UPDATE queries not supported by SmartKG source');
  }

  public async dispose(): Promise<void> {
    // Clean up HDT document cache
    await this.hdtCache.dispose();
  }
}
