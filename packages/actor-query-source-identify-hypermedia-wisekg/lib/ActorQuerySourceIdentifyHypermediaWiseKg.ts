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
import { QuerySourceWiseKg } from './QuerySourceWiseKg';
import { isContextFlagSet, KEY_CONTEXT_WISEKG_FALLBACK } from './Utils';

// Identifies WiseKG and SmartKG+ root sources and creates their query source.
export class ActorQuerySourceIdentifyHypermediaWiseKg extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorQuerySourceDereferenceLink?: any;

  public constructor(args: IActorQuerySourceIdentifyHypermediaWiseKgArgs) {
    super(args, 'wisekg');
    this.mediatorHttp = args.mediatorHttp;
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  // Reject fallback loops and accept only WiseKG-compatible source types.
  public override async test(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, KEY_CONTEXT_WISEKG_FALLBACK)) {
      return failTest(`Actor ${this.name} ignores WiseKG fallback delegation contexts.`);
    }

    if (action.forceSourceType && action.forceSourceType !== 'wisekg' && action.forceSourceType !== 'smartkg+') {
      return failTest(`Actor ${this.name} is not able to handle source type ${action.forceSourceType}.`);
    }
    return this.testMetadata(action);
  }

  // Detect WiseKG root dataset URLs when no source type was forced.
  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, KEY_CONTEXT_WISEKG_FALLBACK)) {
      return failTest(`Actor ${this.name} ignores WiseKG fallback delegation contexts.`);
    }

    if (action.forceSourceType === 'wisekg' || action.forceSourceType === 'smartkg+' || isRootDatasetUrl(action.url)) {
      return passTest({ filterFactor: 2 });
    }

    return failTest(`Actor ${this.name} could not detect a WiseKG root dataset URL at ${action.url}.`);
  }

  // Build a WiseKG query source and remove page links that would trigger crawling.
  public async run(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const metadataLinks = (<Record<string, unknown> | undefined> action.metadata)?.links;
    if (Array.isArray(metadataLinks)) {
      metadataLinks.length = 0;
    }
    if (action.metadata && typeof action.metadata === 'object') {
      clearPageLinks(<Record<string, unknown>> action.metadata);
    }

    const source = new QuerySourceWiseKg(
      action.url,
      dataFactory,
      this.mediatorHttp,
      action.context,
      this.mediatorQuerySourceDereferenceLink,
    );
    return { source, dataset: action.url };
  }
}

// Remove hypermedia pagination links so the root source owns traversal decisions.
function clearPageLinks(metadata: Record<string, unknown>): void {
  metadata.next = [];
  metadata.previous = [];
  metadata.first = [];
  metadata.last = [];
}

// Detect root dataset URLs without query parameters.
function isRootDatasetUrl(url: string): boolean {
  try {
    const parsedUrl = new URL(url);
    return !parsedUrl.search && parsedUrl.pathname.replace(/\/$/u, '').split('/').some(Boolean);
  } catch {
    return !url.includes('?') && url.replace(/\/$/u, '').length > 0;
  }
}

export interface IActorQuerySourceIdentifyHypermediaWiseKgArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  mediatorHttp: MediatorHttp;
  mediatorQuerySourceDereferenceLink?: any;
}
