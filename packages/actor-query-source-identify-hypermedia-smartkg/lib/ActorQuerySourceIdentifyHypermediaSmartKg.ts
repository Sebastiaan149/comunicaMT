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
import { passTest } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { QuerySourceSmartKg } from './QuerySourceSmartKg';

/**
 * A comunica SmartKg Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaSmartKg extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSmartKgArgs) {
    super(args, 'smartkg');
    this.mediatorHttp = args.mediatorHttp;
  }

  public override async test(
    _action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    return passTest({ filterFactor: 1 });
  }

  public async testMetadata(
    _action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    return passTest({ filterFactor: 1 });
  }

  public async run(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    this.logInfo(
      action.context,
      `Identified ${action.url} as a SmartKG source with service URL: ${action.metadata.sparqlService || action.url}`,
    );

    const dataFactory: ComunicaDataFactory = action.context.getSafe(
      KeysInitQuery.dataFactory,
    );
    const algebraFactory = new AlgebraFactory(dataFactory);
    this.logInfo(action.context, 'log test', () => algebraFactory);

    const source = new QuerySourceSmartKg(
      action.forceSourceType ?
        action.url :
        action.metadata.sparqlService || action.url,
      dataFactory,
      this.mediatorHttp,
      action.context,
    );

    return { source };
  }
}

export interface IActorQuerySourceIdentifyHypermediaSmartKgArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
}
