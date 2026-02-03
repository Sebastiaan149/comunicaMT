import {
  ActorQueryOperationTypedMediated,
  IActorQueryOperationTypedMediatedArgs,
} from "@comunica/bus-query-operation";
import { TestResult, IActorTest, passTestVoid } from "@comunica/core";
import type { IActionContext, IQueryOperationResult } from "@comunica/types";
import { getSafeBindings } from "@comunica/utils-query-operation";
import { Algebra } from "sparqlalgebrajs";

/**
 * A comunica Reduced My Query Operation Actor.
 */
export class ActorQueryOperationReducedMy extends ActorQueryOperationTypedMediated<Algebra.Reduced> {
  public constructor(args: IActorQueryOperationTypedMediatedArgs) {
    super(args, "reduced");
  }

  public async testOperation(
    pattern: Algebra.Reduced,
    context: IActionContext,
  ): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(
    operation: Algebra.Reduced,
    context: IActionContext,
  ): Promise<IQueryOperationResult> {
    // Call other query operations like this:
    // const output: IQueryOperationResult = await this.mediatorQueryOperation.mediate({ operation, context });
    const output: IQueryOperationResult = getSafeBindings(
      await this.mediatorQueryOperation.mediate({
        operation: operation.input,
        context,
      }),
    );

    // For this example, we simply return the output of the other operation directly
    return {
      type: "bindings",
      bindingsStream: output.bindingsStream,
      metadata: output.metadata,
    };
  }
}
