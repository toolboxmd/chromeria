import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  RunId,
  ThreadId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
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
          recoverDelegatedTask: () => noFinalization.pipe(Effect.orDie),
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

it.effect("buffered run updates coalesce recovery without sweeping settled children", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const recovered = yield* Deferred.make<void>();
    const touched: string[] = [];
    const marker = {
      ...run,
      id: RunId.make("marker-run"),
      threadId: ThreadId.make("marker-child"),
      status: "completed" as const,
    };
    const external = Layer.mergeAll(
      Layer.mock(Scheduler.Scheduler)({ register: () => Effect.void }),
      Layer.mock(Threads.ThreadManagementService)({
        dispatch: () => Effect.succeed({ sequence: 0, storedEvents: [] }),
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({
        recoverDelegatedTasks: Effect.sync(() => {
          touched.push("global");
        }).pipe(Effect.andThen(Deferred.succeed(recovered, undefined)), Effect.asVoid),
        recoverDelegatedTask: (threadId) =>
          Effect.sync(() => {
            touched.push(threadId);
          }).pipe(
            Effect.andThen(
              threadId === marker.threadId ? Deferred.succeed(recovered, undefined) : Effect.void,
            ),
            Effect.asVoid,
          ),
      }),
    );
    yield* Effect.gen(function* () {
      const settled = Array.from({ length: 64 }, (_, index) => ({
        ...run,
        id: RunId.make(`settled-run:${index}`),
        threadId: ThreadId.make(`settled-child:${index}`),
        status: "completed" as const,
      }));
      for (const child of [...settled, run, marker]) {
        yield* sink.commitCommand({
          commandId: CommandId.make(`seed:${child.id}`),
          commandType: "seed",
          threadId: child.threadId,
          acceptedAt: child.requestedAt,
          events: recoveryEvents(child).map((event, index) => {
            const identified = { ...event, id: EventId.make(`seed:${child.id}:${index}`) };
            return identified.type === "thread.created"
              ? {
                  ...identified,
                  payload: {
                    ...identified.payload,
                    lineage: {
                      parentThreadId: ThreadId.make("parent"),
                      relationshipToParent: "subagent" as const,
                      rootThreadId: ThreadId.make("parent"),
                    },
                  },
                }
              : identified;
          }),
          effects: [],
        });
      }
      yield* sink.write({
        events: [run, run, run, marker].map((changed, index) => ({
          id: EventId.make(`changed-active-run:${index}`),
          type: "run.updated" as const,
          threadId: changed.threadId,
          occurredAt: changed.requestedAt,
          payload: changed,
        })),
      });
      yield* Deferred.await(recovered);
      assert.deepStrictEqual(touched, [run.threadId, marker.threadId]);
    }).pipe(Effect.provide(Reactor.layer.pipe(Layer.provide(external))));
  }).pipe(Effect.provide(dependencies)),
);

it.effect("thread-scoped recovery candidates use indexed searches", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const [query, parameters] = Reactor.recoveryCandidates(sql, run.threadId).compile();
    const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);
    const steps = plan.map((row) => row.detail);
    assert.isTrue(steps.some((step) => step.startsWith("SEARCH r ")));
    assert.deepStrictEqual(
      steps.filter((step) => step.startsWith("SCAN ")),
      [],
    );
  }).pipe(Effect.provide(dependencies)),
);
