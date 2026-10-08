import { assert, it } from "@effect/vitest";
import { CommandId, EventId, type OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Settings from "../serverSettings.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import * as Reactor from "./RecoveryReactor.ts";
import {
  recoveryTestLayer,
  recoveryRun as run,
  recoveryEvents,
  threadFor,
} from "./recovery.testkit.ts";

const dependencies = Layer.mergeAll(
  recoveryTestLayer,
  Coordinator.layer.pipe(Layer.provide(Layer.mergeAll(recoveryTestLayer, Settings.layerTest()))),
);
it.effect(
  "committed failure and idle Stop events trigger retry and release without waiting for a clock tick",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const coordinator = yield* Coordinator.RecoveryCoordinator;
      const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
      const released = yield* Deferred.make<void>();
      const noFinalization = Effect.gen(function* () {
        const held = yield* coordinator.holdsResult(run.threadId);
        if (!held) yield* Deferred.succeed(released, undefined);
      });
      const external = Layer.mergeAll(
        Layer.mock(Scheduler.Scheduler)({ register: () => Effect.void }),
        Layer.mock(Threads.ThreadManagementService)({
          dispatch: (command) =>
            Queue.offer(commands, command).pipe(Effect.as({ sequence: 0, storedEvents: [] })),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          recoverDelegatedTasks: noFinalization.pipe(Effect.orDie),
        }),
      );
      yield* Effect.gen(function* () {
        yield* sink.commitCommand({
          commandId: CommandId.make("seed:events"),
          commandType: "seed",
          threadId: run.threadId,
          acceptedAt: run.requestedAt,
          events: recoveryEvents(run),
          effects: [],
        });
        yield* sink.write({
          events: [
            {
              id: EventId.make("event:failed:observed"),
              type: "run.updated",
              threadId: run.threadId,
              occurredAt: run.requestedAt,
              payload: run,
            },
          ],
        });
        const retry = yield* Queue.take(commands);
        assert.strictEqual(retry.commandId, "prism-retry:thread:recovery:run:original");
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), true);
        yield* sink.write({
          events: [
            {
              id: EventId.make("event:idle-stop:observed"),
              type: "thread.metadata-updated",
              threadId: run.threadId,
              occurredAt: run.requestedAt,
              payload: {
                ...threadFor(run),
                forkRetirement: { token: CommandId.make("stop:idle") },
              },
            },
          ],
        });
        yield* Deferred.await(released);
        assert.strictEqual(yield* coordinator.holdsResult(run.threadId), false);
        assert.strictEqual(yield* Queue.size(commands), 0);
      }).pipe(Effect.provide(Reactor.layer.pipe(Layer.provide(external))));
    }).pipe(Effect.provide(dependencies)),
);
