import { Bus } from '@comunica/core';
import { ActorQueryOperationBgpWiseKg } from '../lib';

describe('ActorQueryOperationBgpWiseKg', () => {
  let actor: ActorQueryOperationBgpWiseKg;

  describe('The ActorQueryOperationBgpWiseKg module', () => {
    it('should be importable from the package root', () => {
      expect(ActorQueryOperationBgpWiseKg).toBeDefined();
    });
  });

  describe('An ActorQueryOperationBgpWiseKg instance', () => {
    beforeEach(() => {
      actor = new ActorQueryOperationBgpWiseKg({
        name: 'test-bgp-wisekg',
        bus: new Bus({ name: 'bus' }),
        mediatorQueryOperation: <any> {},
      });
    });

    it('should have the right name', () => {
      expect(actor.name).toBe('test-bgp-wisekg');
    });
  });
});
