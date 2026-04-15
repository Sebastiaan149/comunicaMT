import { ActorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { ActorQuerySourceIdentifyHypermediaSmartKg } from '../../lib/ActorQuerySourceIdentifyHypermediaSmartKg';
import { QuerySourceSmartKg } from '../../lib/QuerySourceSmartKg';
import '@comunica/utils-jest';

const DF = new DataFactory();

describe('ActorQuerySourceIdentifyHypermediaSmartKg', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext({ [KeysInitQuery.dataFactory.name]: DF });
  });

  describe('The ActorQuerySourceIdentifyHypermediaSmartKg module', () => {
    it('should be a function', () => {
      expect(ActorQuerySourceIdentifyHypermediaSmartKg).toBeInstanceOf(Function);
    });

    it('should be a ActorQuerySourceIdentifyHypermediaSmartKg constructor', () => {
      expect(new (<any> ActorQuerySourceIdentifyHypermediaSmartKg)({
        name: 'actor',
        bus,
        mediatorHttp: <any> {},
      }))
        .toBeInstanceOf(ActorQuerySourceIdentifyHypermediaSmartKg);
      expect(new (<any> ActorQuerySourceIdentifyHypermediaSmartKg)({
        name: 'actor',
        bus,
        mediatorHttp: <any> {},
      }))
        .toBeInstanceOf(ActorQuerySourceIdentifyHypermedia);
    });

    it('should not be able to create new ActorQuerySourceIdentifyHypermediaSmartKg objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQuerySourceIdentifyHypermediaSmartKg)();
      }).toThrow(`Class constructor ActorQuerySourceIdentifyHypermediaSmartKg cannot be invoked without 'new'`);
    });
  });

  describe('An ActorQuerySourceIdentifyHypermediaSmartKg instance', () => {
    let actor: ActorQuerySourceIdentifyHypermediaSmartKg;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyHypermediaSmartKg({
        name: 'actor',
        bus,
        mediatorHttp: <any> {},
      });
    });

    describe('#test', () => {
      it('should test with a forced smartkg source type', async() => {
        await expect(actor.test({
          url: 'http://localhost:8080/other',
          metadata: {},
          quads: <any> null,
          forceSourceType: 'smartkg',
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should not test with a forced unknown source type', async() => {
        await expect(actor.test({
          url: 'http://localhost:8080/smartkg',
          metadata: {},
          quads: <any> null,
          forceSourceType: 'unknown',
          context,
        }))
          .resolves.toFailTest('Actor actor is not able to handle source type unknown.');
      });

      it('should test with a URL containing smartkg', async() => {
        await expect(actor.test({
          url: 'http://localhost:8080/smartkg',
          metadata: {},
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should test with a URL containing smartkg with trailing slash', async() => {
        await expect(actor.test({
          url: 'http://localhost:8080/smartkg/',
          metadata: {},
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should not test without smartkg in URL and no metadata', async() => {
        await expect(actor.test({
          url: 'http://localhost:8080/other',
          metadata: {},
          quads: <any> null,
          context,
        })).resolves
          .toFailTest('Actor actor could not detect a SmartKG server at http://localhost:8080/other.');
      });
    });

    describe('#testMetadata', () => {
      it('should test with a forced smartkg source type', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/other',
          metadata: {},
          quads: <any> null,
          forceSourceType: 'smartkg',
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should test with a void:Dataset matching the URL', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/smartkg',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should test with a void:Dataset matching the URL ignoring trailing slashes', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/smartkg/',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/smartkg',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should test with a void:Dataset matching the URL ignoring case', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/SmartKG',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/smartkg',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should test with a void:Dataset when multiple datasets are present', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/other',
                source: 'http://localhost:8080/',
              },
              {
                uri: 'http://localhost:8080/smartkg',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should fallback to URL pattern when void:Dataset does not match', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/other',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should fallback to URL pattern when datasets array is empty', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            datasets: [],
          },
          quads: <any> null,
          context,
        })).resolves
          .toPassTest({ filterFactor: 1 });
      });

      it('should fail when dataset URI does not match URL and no URL pattern match', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/other',
          metadata: {
            datasets: [
              {
                uri: 'http://localhost:8080/smartkg',
                source: 'http://localhost:8080/',
              },
            ],
          },
          quads: <any> null,
          context,
        })).resolves
          .toFailTest('Actor actor could not detect a SmartKG server at http://localhost:8080/other.');
      });

      it('should fail when no datasets in metadata and URL contains no smartkg pattern', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/sparql',
          metadata: {},
          quads: <any> null,
          context,
        })).resolves
          .toFailTest('Actor actor could not detect a SmartKG server at http://localhost:8080/sparql.');
      });

      it('should fail when datasets is undefined', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/other',
          metadata: {
            datasets: undefined,
          },
          quads: <any> null,
          context,
        })).resolves
          .toFailTest('Actor actor could not detect a SmartKG server at http://localhost:8080/other.');
      });

      it('should fail when datasets is null', async() => {
        await expect(actor.testMetadata({
          url: 'http://localhost:8080/other',
          metadata: {
            datasets: null,
          },
          quads: <any> null,
          context,
        })).resolves
          .toFailTest('Actor actor could not detect a SmartKG server at http://localhost:8080/other.');
      });
    });

    describe('#run', () => {
      it('should return a QuerySourceSmartKg instance', async() => {
        const output = await actor.run({
          url: 'http://localhost:8080/smartkg',
          metadata: {},
          quads: <any> null,
          context,
        });
        expect(output.source).toBeInstanceOf(QuerySourceSmartKg);
        expect((<any> output.source).referenceValue).toBe('http://localhost:8080/smartkg');
      });

      it('should use metadata sparqlService if available', async() => {
        const output = await actor.run({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            sparqlService: 'http://custom:8080/endpoint',
          },
          quads: <any> null,
          context,
        });
        expect(output.source).toBeInstanceOf(QuerySourceSmartKg);
        expect((<any> output.source).referenceValue).toBe('http://custom:8080/endpoint');
      });

      it('should use the provided URL when forceSourceType is smartkg', async() => {
        const output = await actor.run({
          url: 'http://localhost:8080/smartkg',
          metadata: {
            sparqlService: 'http://custom:8080/endpoint',
          },
          quads: <any> null,
          forceSourceType: 'smartkg',
          context,
        });
        expect(output.source).toBeInstanceOf(QuerySourceSmartKg);
        expect((<any> output.source).referenceValue).toBe('http://localhost:8080/smartkg');
      });
    });
  });
});
