import { assert, it } from "@effect/vitest";
import { CommandId, EventId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { spectrumRegistrationPlan } from "./registrationPlan.ts";
import { makeState, NOW } from "./testFixtures.ts";

const guard = Effect.fn("test.Spectrum.registrationGuard")(function* (
  acknowledged: ReadonlyArray<string>,
  read: ReadonlyArray<string>,
) {
  const caller = yield* setup;
  const current = {
    ...caller,
    forkResumedRetirements: acknowledged.map((token) => CommandId.make(token)),
  };
  yield* (yield* EventSink.EventSinkV2).write({
    events: [
      {
        id: EventId.make("caller:acknowledged"),
        type: "thread.metadata-updated",
        threadId: caller.id,
        occurredAt: NOW,
        payload: current,
      },
    ],
  });
  const plan = spectrumRegistrationPlan(
    makeState({ threadId: input.threadId, callerThreadId: caller.id, callerRunId: null }),
    { ...current, forkResumedRetirements: read.map((token) => CommandId.make(token)) },
  );
  return yield* Effect.all(plan.guards, { discard: true }).pipe(Effect.exit);
});

it.effect("registration rejects acknowledgements that only match when joined", () =>
  Effect.gen(function* () {
    const exit = yield* guard(["a", "b"], ["a,b"]);
    assert.strictEqual(exit._tag, "Failure");
    assert.include(String(exit), "state_conflict");
  }).pipe(Effect.provide(Layer.fresh(base))),
);

it.effect("registration admits the exact acknowledgements it read", () =>
  Effect.gen(function* () {
    const exit = yield* guard(["a", "b"], ["a", "b"]);
    assert.strictEqual(exit._tag, "Success");
  }).pipe(Effect.provide(Layer.fresh(base))),
);
