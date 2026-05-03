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

export class ActorQuerySourceIdentifyHypermediaSmartKg extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorQuerySourceDereferenceLink?: any;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSmartKgArgs) {
    super(args, 'smartkg');
    this.mediatorHttp = args.mediatorHttp;
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  public override async test(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, 'smartkgFallback')) {
      return failTest(`Actor ${this.name} ignores SmartKG fallback delegation contexts.`);
    }

    if (action.forceSourceType && action.forceSourceType !== 'smartkg') {
      return failTest(`Actor ${this.name} is not able to handle source type ${action.forceSourceType}.`);
    }
    return this.testMetadata(action);
  }

  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (isContextFlagSet(action.context, 'smartkgFallback')) {
      return failTest(`Actor ${this.name} ignores SmartKG fallback delegation contexts.`);
    }

    if (action.forceSourceType === 'smartkg') {
      return passTest({ filterFactor: 2 });
    }

    if (!isRootSmartKgUrl(action.url)) {
      return failTest(`Actor ${this.name} only auto-detects root SmartKG dataset URLs.`);
    }

    return passTest({ filterFactor: 2 });
  }

  public async run(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const metadataLinks = (action.metadata as Record<string, unknown> | undefined)?.links;
    if (Array.isArray(metadataLinks)) {
      metadataLinks.length = 0;
    }
    if (action.metadata && typeof action.metadata === 'object') {
      clearPageLinks(action.metadata as Record<string, unknown>);
    }

    const source = new QuerySourceSmartKg(
      action.url,
      dataFactory,
      this.mediatorHttp,
      action.context,
      this.mediatorQuerySourceDereferenceLink,
    );
    return { source, dataset: action.url };
  }
}

function clearPageLinks(metadata: Record<string, unknown>): void {
  metadata.next = [];
  metadata.previous = [];
  metadata.first = [];
  metadata.last = [];
}

function isRootSmartKgUrl(url: string): boolean {
  try {
    const parsedUrl = new URL(url);
    return !parsedUrl.search && parsedUrl.pathname.replace(/\/$/u, '').toLowerCase().endsWith('/smartkg');
  } catch {
    return url.replace(/\/$/u, '').toLowerCase().endsWith('/smartkg') && !url.includes('?');
  }
}

function isContextFlagSet(context: unknown, key: string): boolean {
  if (!context) {
    return false;
  }

  const rawValue = typeof (context as any).getRaw === 'function' ?
    (context as any).getRaw(key) :
    undefined;
  if (rawValue !== undefined) {
    return Boolean(rawValue);
  }

  return Boolean((context as Record<string, unknown>)[key]);
}

export interface IActorQuerySourceIdentifyHypermediaSmartKgArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  mediatorHttp: MediatorHttp;
  mediatorQuerySourceDereferenceLink?: any;
}
