import { Bus } from "@comunica/core";
import { BindingsFactory } from "@comunica/utils-bindings-factory";
import { DataFactory } from "rdf-data-factory";
import { ArrayIterator } from "asynciterator";
import { ActorQueryOperationReducedMy } from "../lib/ActorQueryOperationReducedMy";
const arrayifyStream = require("arrayify-stream");

// RDF/JS DataFactory and Bindings factory
const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe("ActorQueryOperationReducedMy", () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let cacheSize: any;

  beforeEach(() => {
    bus = new Bus({ name: "bus" });
    mediatorQueryOperation = {
      mediate: (arg: any) =>
        Promise.resolve({
          bindingsStream: new ArrayIterator(
            [
              BF.bindings([[DF.variable("a"), DF.literal("1")]]),
              BF.bindings([[DF.variable("a"), DF.literal("2")]]),
              BF.bindings([[DF.variable("a"), DF.literal("3")]]),
              BF.bindings([[DF.variable("a"), DF.literal("3")]]),
              BF.bindings([[DF.variable("a"), DF.literal("2")]]),
            ],
            { autoStart: false },
          ),
          metadata: () =>
            Promise.resolve({ totalItems: 5, variables: [DF.variable("a")] }),
          operated: arg,
          type: "bindings",
          variables: ["?a"],
          canContainUndefs: false,
        }),
    };
    cacheSize = 20;
  });

  describe("#newReducedMyFilter", () => {
    let actor: ActorQueryOperationReducedMy;

    beforeEach(() => {
      actor = new ActorQueryOperationReducedMy({
        name: "actor",
        bus,
        mediatorQueryOperation,
      });
    });

    it("should exist", () => {
      expect(actor).toBeDefined();
    });
  });
});
