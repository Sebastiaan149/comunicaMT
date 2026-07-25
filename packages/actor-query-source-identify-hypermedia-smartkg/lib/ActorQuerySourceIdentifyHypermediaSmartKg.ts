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
import { QuerySourceSmartKg } from './QuerySourceSmartKg';

// Identifies SmartKG and SmartKG+ root sources and creates their query source.
export class ActorQuerySourceIdentifyHypermediaSmartKg extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorQuerySourceDereferenceLink?: any;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSmartKgArgs) {
    super(args, 'smartkg');
    this.mediatorHttp = args.mediatorHttp;
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  // Reject fallback loops and accept only SmartKG-compatible source types.
  public override async test(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, 'smartkgFallback')) {
      return failTest(`Actor ${this.name} ignores SmartKG fallback delegation contexts.`);
    }

    if (action.forceSourceType && action.forceSourceType !== 'smartkg' && action.forceSourceType !== 'smartkg+') {
      return failTest(`Actor ${this.name} is not able to handle source type ${action.forceSourceType}.`);
    }
    return this.testMetadata(action);
  }

  // Detect SmartKG root dataset URLs when no source type was forced.
  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, 'smartkgFallback')) {
      return failTest(`Actor ${this.name} ignores SmartKG fallback delegation contexts.`);
    }

    if (action.forceSourceType === 'smartkg' || action.forceSourceType === 'smartkg+') {
      return passTest({ filterFactor: 1 });
    }

    if (!isRootSmartKgUrl(action.url)) {
      return failTest(`Actor ${this.name} could not detect a SmartKG server at ${action.url}.`);
    }

    return passTest({ filterFactor: 1 });
  }

  // Build a SmartKG query source and remove page links that would trigger crawling.
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

    const sourceUrl = action.forceSourceType ? action.url : action.metadata.sparqlService || action.url;
    const source = new QuerySourceSmartKg(
      sourceUrl,
      dataFactory,
      this.mediatorHttp,
      action.context,
      this.mediatorQuerySourceDereferenceLink,
    );
    return { source, dataset: sourceUrl };
  }
}

// Remove hypermedia pagination links so the root source owns traversal decisions.
function clearPageLinks(metadata: Record<string, unknown>): void {
  metadata.next = [];
  metadata.previous = [];
  metadata.first = [];
  metadata.last = [];
}

// Detect root SmartKG URLs without query parameters.
function isRootSmartKgUrl(url: string): boolean {
  try {
    const parsedUrl = new URL(url);
    return !parsedUrl.search && isSmartKgDatasetPath(parsedUrl.pathname);
  } catch {
    return isSmartKgDatasetPath(url) && !url.includes('?');
  }
}

// Recognize the canonical SmartKG dataset path suffix.
function isSmartKgDatasetPath(path: string): boolean {
  const normalizedPath = path.replace(/\/$/u, '').toLowerCase();
  return normalizedPath.endsWith('/smartkg');
}

// Read context flags from both Comunica ActionContext and plain objects.
function isContextFlagSet(context: unknown, key: string): boolean {
  if (!context) {
    return false;
  }

  const rawValue = typeof (<any> context).getRaw === 'function' ?
      (<any> context).getRaw(key) :
    undefined;
  if (rawValue !== undefined) {
    return Boolean(rawValue);
  }

  return Boolean((<Record<string, unknown>> context)[key]);
}

export interface IActorQuerySourceIdentifyHypermediaSmartKgArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  mediatorHttp: MediatorHttp;
  mediatorQuerySourceDereferenceLink?: any;
}
