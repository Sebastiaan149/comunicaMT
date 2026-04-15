import type { MediatorHttp } from '@comunica/bus-http';
import type {
  IActionQuerySourceIdentifyHypermedia,
  IActorQuerySourceIdentifyHypermediaArgs,
  IActorQuerySourceIdentifyHypermediaOutput,
  IActorQuerySourceIdentifyHypermediaTest,
} from '@comunica/bus-query-source-identify-hypermedia';
import { ActorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { KeysInitQuery } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { QuerySourceSmartKg } from './QuerySourceSmartKg';

/**
 * A comunica SmartKg Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaSmartKg extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSmartKgArgs) {
    const completeArgs: any = {
      ...args,
      mediatorMetadataAccumulate: args.mediatorMetadataAccumulate || { mediate: async () => {} },
      mediatorQuerySourceDereferenceLink: args.mediatorQuerySourceDereferenceLink || { mediate: async () => {} },
      mediatorRdfResolveHypermediaLinks: args.mediatorRdfResolveHypermediaLinks || { mediate: async () => {} },
      mediatorRdfResolveHypermediaLinksQueue: args.mediatorRdfResolveHypermediaLinksQueue || { mediate: async () => {} },
      mediatorMergeBindingsContext: args.mediatorMergeBindingsContext || { mediate: async () => {} },
      cacheSize: args.cacheSize ?? 100,
      maxIterators: args.maxIterators ?? 64,
    };
    super(completeArgs, 'smartkg');
    this.mediatorHttp = args.mediatorHttp;
  }

  public override async test(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    // Check if URL contains 'smartkg' or if source type is explicitly forced to 'smartkg'
    if (action.forceSourceType === 'smartkg' || (action.url && action.url.includes('smartkg'))) {
      this.logInfo(action.context, `SmartKG actor: detected SmartKG URL pattern`);
      return passTest({ filterFactor: 1 });
    }

    return failTest(`Actor ${this.name} could not detect a SmartKG server at ${action.url}.`);
  }

  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    this.logInfo(action.context, `SMARTKG TESTMETADATAAAA`);
    // 1. Check if source type is explicitly forced to 'smartkg'
    if (action.forceSourceType === 'smartkg') {
      this.logInfo(action.context, `SmartKG actor: detected SmartKG from forceSourceType`);
      return passTest({ filterFactor: 1 });
    }

    // 2. Check if void:Dataset metadata matches the current URL (indicates SmartKG)
    if (action.metadata?.datasets && Array.isArray(action.metadata.datasets)) {
      const normalizedUrl = this.normalizeUrl(action.url);
      for (const dataset of action.metadata.datasets) {
        if (dataset.uri && this.normalizeUrl(dataset.uri) === normalizedUrl) {
          this.logInfo(
            action.context,
            `SmartKG actor: detected SmartKG from void:Dataset metadata matching URL`,
          );
          return passTest({ filterFactor: 1 });
        }
      }
    }

    // 3. Fallback: Check if URL contains 'smartkg'
    if (action.url && action.url.includes('smartkg')) {
      this.logInfo(action.context, `SmartKG actor: detected SmartKG from URL pattern`);
      return passTest({ filterFactor: 1 });
    }

    return failTest(`Actor ${this.name} could not detect a SmartKG server at ${action.url}.`);
  }

  public async run(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    this.logInfo(
      action.context,
      `Identified ${action.url} as a SmartKG source with service URL: ${action.metadata?.sparqlService || action.url}`,
    );

    const dataFactory: ComunicaDataFactory = action.context.getSafe(
      KeysInitQuery.dataFactory,
    );
    const algebraFactory = new AlgebraFactory(dataFactory);
    this.logInfo(action.context, 'log test', () => algebraFactory);

    const source = new QuerySourceSmartKg(
      action.forceSourceType ?
        action.url :
        action.metadata?.sparqlService || action.url,
      dataFactory,
      this.mediatorHttp,
      action.context,
    );

    return { source };
  }

  /**
   * Normalize a URL by removing trailing slashes for consistent comparison.
   * This ensures that 'http://localhost:8080/smartkg' and 'http://localhost:8080/smartkg/'
   * are treated as equivalent.
   *
   * @param url - The URL to normalize
   * @returns The normalized URL (lowercase with trailing slash removed)
   */
  private normalizeUrl(url: string): string {
    return url.replace(/\/$/, '').toLowerCase();
  }
}

export interface IActorQuerySourceIdentifyHypermediaSmartKgArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
  /**
   * Optional: The metadata accumulate mediator
   */
  mediatorMetadataAccumulate?: any;
  /**
   * Optional: The query source dereference link mediator
   */
  mediatorQuerySourceDereferenceLink?: any;
  /**
   * Optional: The RDF resolve hypermedia links mediator
   */
  mediatorRdfResolveHypermediaLinks?: any;
  /**
   * Optional: The RDF resolve hypermedia links queue mediator
   */
  mediatorRdfResolveHypermediaLinksQueue?: any;
  /**
   * Optional: The merge bindings context mediator
   */
  mediatorMergeBindingsContext?: any;
  /**
   * Optional: Cache size
   */
  cacheSize?: number;
  /**
   * Optional: Max iterators
   */
  maxIterators?: number;
}
