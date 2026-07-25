import { Bus } from '@comunica/core';
import { ActorQueryOperationBgpSmartKg } from '../lib';

describe('ActorQueryOperationBgpSmartKg', () => {
  let actor: ActorQueryOperationBgpSmartKg;

  describe('The ActorQueryOperationBgpSmartKg module', () => {
    it('should be importable from the package root', () => {
      expect(ActorQueryOperationBgpSmartKg).toBeDefined();
    });
  });

  describe('An ActorQueryOperationBgpSmartKg instance', () => {
    beforeEach(() => {
      actor = new ActorQueryOperationBgpSmartKg({
        name: 'test-bgp-smartkg',
        bus: new Bus({ name: 'bus' }),
        mediatorQueryOperation: <any> {},
      });
    });

    it('should have the right name', () => {
      expect(actor.name).toBe('test-bgp-smartkg');
    });

    it('should have default maxFamilies value', () => {
      expect(actor.maxFamilies).toBe(100);
    });

    it('should have default minPatternCount value', () => {
      expect(actor.minPatternCount).toBe(2);
    });
  });
});
