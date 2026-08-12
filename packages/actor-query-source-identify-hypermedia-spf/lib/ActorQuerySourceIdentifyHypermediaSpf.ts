import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type {
  IActionQuerySourceIdentifyHypermedia,
  IActorQuerySourceIdentifyHypermediaArgs,
  IActorQuerySourceIdentifyHypermediaOutput,
  IActorQuerySourceIdentifyHypermediaTest,
} from '@comunica/bus-query-source-identify-hypermedia';
import { ActorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import { KeysInitQuery } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
import type { ComunicaDataFactory, IActionContext } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import {
  createSpfSearchForm,
  detectSpfSearchForm,
  QuerySourceSpf,
} from './QuerySourceSpf';

/**
 * Identifies hypermedia sources that expose an SPF Hydra search control.
 */
export class ActorQuerySourceIdentifyHypermediaSpf extends ActorQuerySourceIdentifyHypermedia
  implements IActorQuerySourceIdentifyHypermediaSpfArgs {
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  public readonly maxMpR: number;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSpfArgs) {
    super(args, 'spf');
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorMergeBindingsContext = args.mediatorMergeBindingsContext;
    this.maxMpR = args.maxMpR ?? 30;
  }

  // Accept forced SPF sources or metadata that exposes an SPF control.
  public override async test(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    if (action.forceSourceType && action.forceSourceType !== 'spf') {
      return failTest(`Actor ${this.name} is not able to handle source type ${action.forceSourceType}.`);
    }
    return this.testMetadata(action);
  }

  // Validate the Hydra form fields needed for SPF requests.
  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    const control = detectSpfSearchForm(action.metadata);
    if (!control) {
      if (action.forceSourceType === 'spf') {
        return passTest({ filterFactor: 1 });
      }
      return failTest(`Actor ${this.name} requires an SPF Hydra control with subject/s, triples, star, and values fields.`);
    }
    if (action.handledDatasets && action.handledDatasets[control.searchForm.dataset]) {
      return failTest(`Actor ${this.name} can only be applied for the first page of an SPF dataset.`);
    }
    return passTest({ filterFactor: 1 });
  }

  // Create the SPF query source for the detected dataset.
  public async run(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    const source = await this.createSource(action.url, action.metadata, action.context);
    return { source, dataset: source.searchForm.dataset };
  }

  // Assemble the query-source dependencies from Comunica mediators.
  protected async createSource(
    url: string,
    metadata: Record<string, any>,
    context: IActionContext,
  ): Promise<QuerySourceSpf> {
    const control = detectSpfSearchForm(metadata) ?? createSpfSearchForm(url);

    const dataFactory: ComunicaDataFactory = context.getSafe(KeysInitQuery.dataFactory);
    const algebraFactory = new AlgebraFactory(dataFactory);
    return new QuerySourceSpf(
      this.mediatorMetadata,
      this.mediatorMetadataExtract,
      this.mediatorDereferenceRdf,
      dataFactory,
      algebraFactory,
      await BindingsFactory.create(this.mediatorMergeBindingsContext, context, dataFactory),
      url,
      control.searchForm,
      control.mappings,
      this.maxMpR,
    );
  }
}

export interface IActorQuerySourceIdentifyHypermediaSpfArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The metadata mediator.
   */
  mediatorMetadata: MediatorRdfMetadata;
  /**
   * The metadata extract mediator.
   */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
  /**
   * The RDF dereference mediator.
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * A mediator for creating binding context merge handlers.
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  /**
   * The maximum mappings per SPF request.
   * @default {50}
   */
  maxMpR?: number;
}
