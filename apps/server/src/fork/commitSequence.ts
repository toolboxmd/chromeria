import { CommandId, type OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class ForkStateOnlyCommitError extends Schema.TaggedError<ForkStateOnlyCommitError>()(
  "ForkStateOnlyCommitError",
  { commandId: CommandId, kind: Schema.Literals(["missing_plan", "core_effects"]) },
) {}

export function validateForkStateOnlyCommand(input: {
  readonly commandId: CommandId;
  readonly events: ReadonlyArray<unknown>;
  readonly cancelUnsettledEffects?: unknown;
  readonly forkPlans?: ReadonlyArray<unknown>;
  readonly effects: ReadonlyArray<unknown>;
}) {
  if (input.events.length > 0) return Effect.void;
  if ((input.forkPlans?.length ?? 0) === 0)
    return Effect.fail(
      new ForkStateOnlyCommitError({ commandId: input.commandId, kind: "missing_plan" }),
    );
  if (input.effects.length > 0 || input.cancelUnsettledEffects !== undefined)
    return Effect.fail(
      new ForkStateOnlyCommitError({ commandId: input.commandId, kind: "core_effects" }),
    );
  return Effect.void;
}

/** State-only fork commands have receipts but publish no events and enqueue no core effects. */
export function forkCommitSequence<E, R>(
  input: {
    readonly commandId: CommandId;
    readonly forkPlans?: ReadonlyArray<unknown>;
    readonly effects: ReadonlyArray<unknown>;
  },
  storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>,
  latestThreadSequence: Effect.Effect<number, E, R>,
): Effect.Effect<number, E | ForkStateOnlyCommitError, R> {
  const sequence = storedEvents.at(-1)?.sequence;
  if (sequence !== undefined) return Effect.succeed(sequence);
  if ((input.forkPlans?.length ?? 0) === 0)
    return Effect.fail(
      new ForkStateOnlyCommitError({ commandId: input.commandId, kind: "missing_plan" }),
    );
  if (input.effects.length > 0)
    return Effect.fail(
      new ForkStateOnlyCommitError({ commandId: input.commandId, kind: "core_effects" }),
    );
  return latestThreadSequence;
}
