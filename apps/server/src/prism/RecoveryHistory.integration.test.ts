// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RunId,
  type OrchestrationV2Run,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Persistence from "../persistence/Sqlite.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Settings from "../serverSettings.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Coordinator from "./RecoveryCoordinator.ts";
import * as History from "./RecoveryHistory.ts";
import * as Reactor from "./RecoveryReactor.ts";
import * as Store from "./RecoveryStore.ts";
import { continuationAdmission } from "./continuationAdmission.ts";
import { continuationRunFields } from "./RecoveryHooks.ts";
import { retryAdmission } from "./recoveryAdmission.ts";
import { retryCommand } from "./recoveryPolicy.ts";
import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import {
  recoveryTestLayer,
  recoveryRun as run,
  recoveryEvents,
  errorFor,
  threadFor,
} from "./recovery.testkit.ts";

const dependencies = Layer.mergeAll(
  recoveryTestLayer,
  Coordinator.layer.pipe(Layer.provide(Layer.mergeAll(recoveryTestLayer, Settings.layerTest()))),
);
const persist = Effect.fn("historyTest.persist")(function* (
  id: string,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
) {
  yield* (yield* EventSink.EventSinkV2).commitCommand({
    commandId: CommandId.make(id),
    commandType: "fixture",
    threadId: run.threadId,
    acceptedAt: run.requestedAt,
    events,
    effects: [],
  });
});
const event = (
  source: OrchestrationV2Run,
  suffix: string,
): Extract<OrchestrationV2DomainEvent, { type: "run.created" }> => ({
  id: EventId.make(`event:${suffix}`),
  type: "run.created",
  threadId: source.threadId,
  occurredAt: source.requestedAt,
  payload: source,
});
const external = (commands: Queue.Queue<OrchestrationV2ServerCommand>, wake = Effect.void) =>
  Layer.mergeAll(
    Layer.mock(Scheduler.Scheduler)({ register: () => Effect.void }),
    Layer.mock(Threads.ThreadManagementService)({
      dispatch: (command) =>
        Queue.offer(commands, command).pipe(Effect.as({ sequence: 0, storedEvents: [] })),
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({ recoverDelegatedTasks: wake }),
  );
const withReactor = <A, E, R>(
  work: Effect.Effect<A, E, R | Reactor.RecoveryReactor>,
  commands: Queue.Queue<OrchestrationV2ServerCommand>,
) =>
  work.pipe(
    Effect.provide(
      Reactor.layer.pipe(
        Layer.provide(
          Layer.mergeAll(external(commands), Layer.succeed(ServerActivation, Effect.never)),
        ),
      ),
    ),
  );
const prepareRetry = Effect.gen(function* () {
  yield* persist("seed", recoveryEvents(run));
  const failure = errorFor(run);
  if (failure.type !== "error") throw new Error("fixture");
  const record = yield* (yield* Store.RecoveryStore).reconcile({
    previous: null,
    run,
    failure: failure.failure,
    stoppedOrRetired: false,
    autoResume: true,
  });
  yield* History.writeRecoveryOutcome({
    sourceRunId: run.id,
    threadId: run.threadId,
    status: "pending",
    reason: "retry",
  });
  const command = retryCommand(record);
  if (command?.type !== "message.dispatch") throw new Error("fixture");
  const successor = {
    ...run,
    ...continuationRunFields(command),
    id: RunId.make("run:successor"),
    ordinal: 2,
    userMessageId: command.messageId,
    status: "running" as const,
    completedAt: null,
  };
  const events = [event(successor, "successor")];
  const input = {
    commandId: command.commandId,
    commandType: command.type,
    threadId: run.threadId,
    acceptedAt: run.requestedAt,
    events,
    effects: [],
    forkPlans: [retryAdmission(command)!, continuationAdmission(command, events)!],
  };
  return { command, successor, input, sink: yield* EventSink.EventSinkV2 };
});

describe("Prism exact-source recovery history", () => {
  it.effect(
    "opt-out, re-enabled pending reset and admitted continuation survive real database reopens without rewriting the decision",
    () => {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "prism-history-restart-"),
      );
      const path = NodePath.join(directory, "state.sqlite");
      const inProcess = <A, E, R>(work: Effect.Effect<A, E, R>) => {
        const database = Persistence.layerFromPath(path).pipe(Layer.provide(NodeServices.layer));
        const stores = Layer.mergeAll(
          database,
          EventStore.layer.pipe(Layer.provide(database)),
          Projection.layer.pipe(Layer.provide(database)),
        );
        const base = Layer.mergeAll(
          stores,
          EventSink.layer.pipe(Layer.provide(stores)),
          Store.layer.pipe(Layer.provide(database)),
        );
        return work.pipe(
          Effect.provide(
            Layer.mergeAll(
              base,
              Coordinator.layer.pipe(Layer.provide(Layer.mergeAll(base, Settings.layerTest()))),
            ),
          ),
        );
      };
      const resetAt = "2026-10-08T12:00:00Z";
      const optedOut = {
        sourceRunId: run.id,
        threadId: run.threadId,
        status: "decided" as const,
        outcome: "not_retryable" as const,
        reason: "opted_out" as const,
      };
      const command: OrchestrationV2ServerCommand = {
        type: "message.dispatch",
        commandId: CommandId.make("reset:admission"),
        messageId: MessageId.make("reset:message"),
        threadId: run.threadId,
        usageLimitContinuationOfRunId: run.id,
        usageLimitRecoveryRequestId: CommandId.make("reset:enabled"),
        text: "Continue",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "server",
      };
      const successor = {
        ...run,
        ...continuationRunFields(command),
        id: RunId.make("run:reset:successor"),
        ordinal: 2,
        userMessageId: command.messageId,
        status: "running" as const,
        completedAt: null,
      };
      return Effect.gen(function* () {
        yield* inProcess(
          Effect.gen(function* () {
            const seeded = recoveryEvents(run).map((item) =>
              item.type === "thread.created"
                ? {
                    ...item,
                    payload: {
                      ...item.payload,
                      limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
                    },
                  }
                : item.type === "turn-item.updated" && item.payload.type === "error"
                  ? {
                      ...item,
                      payload: {
                        ...item.payload,
                        failure: {
                          ...item.payload.failure,
                          class: "usage_limit" as const,
                          resetAt,
                        },
                      },
                    }
                  : item,
            );
            yield* persist("seed:optout", seeded);
            const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
            yield* withReactor(
              Effect.gen(function* () {
                yield* (yield* Reactor.RecoveryReactor).sweep;
              }),
              commands,
            );
            assert.deepStrictEqual(yield* History.readRecoveryState(run.id), {
              decision: optedOut,
              admittedContinuations: [],
              pendingRecovery: null,
            });
          }),
        );
        yield* inProcess(
          Effect.gen(function* () {
            const thread = yield* (yield* Projection.ProjectionStoreV2).getThread(run.threadId);
            yield* persist("reenable:committed", [
              {
                id: EventId.make("reenable:metadata"),
                type: "thread.metadata-updated",
                threadId: run.threadId,
                occurredAt: run.requestedAt,
                payload: {
                  ...thread,
                  limitRecovery: {
                    runId: run.id,
                    resetAt,
                    autoResume: true,
                    snooze: false,
                    requestId: CommandId.make("reset:enabled"),
                  },
                },
              },
            ]);
            // Reader sees committed intent even before the reactor can sweep.
            assert.deepStrictEqual(yield* History.readRecoveryState(run.id), {
              decision: optedOut,
              admittedContinuations: [],
              pendingRecovery: { sourceRunId: run.id, reason: "reset" },
            });
          }),
        );
        yield* inProcess(
          Effect.gen(function* () {
            const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
            yield* withReactor(
              Effect.gen(function* () {
                yield* (yield* Reactor.RecoveryReactor).sweep;
              }),
              commands,
            );
            assert.deepStrictEqual((yield* History.readRecoveryState(run.id)).pendingRecovery, {
              sourceRunId: run.id,
              reason: "reset",
            });
            const events = [event(successor, "reset:successor")];
            const sql = yield* SqlClient.SqlClient;
            yield* sql.withTransaction(
              Effect.gen(function* () {
                yield* (yield* EventSink.EventSinkV2).commitCommand({
                  commandId: command.commandId,
                  commandType: command.type,
                  threadId: run.threadId,
                  acceptedAt: run.requestedAt,
                  events,
                  effects: [],
                  forkPlans: [continuationAdmission(command, events)!],
                });
                assert.deepStrictEqual(yield* History.readRecoveryState(run.id), {
                  decision: optedOut,
                  admittedContinuations: [
                    { sourceRunId: run.id, successorRunId: successor.id, threadId: run.threadId },
                  ],
                  pendingRecovery: null,
                });
              }),
            );
          }),
        );
        yield* inProcess(
          Effect.gen(function* () {
            // A subsequent ordinary lifecycle payload cannot erase or replace the admitted source.
            const updated = {
              ...successor,
              forkPrismContinuationSourceRunId: RunId.make("wrong:new-source"),
              status: "completed" as const,
              completedAt: run.completedAt,
            };
            yield* persist("successor:completed", [
              { ...event(updated, "successor:completed"), type: "run.updated" },
            ]);
            const state = yield* History.readRecoveryState(run.id);
            assert.deepStrictEqual(state, {
              decision: optedOut,
              admittedContinuations: [
                { sourceRunId: run.id, successorRunId: successor.id, threadId: run.threadId },
              ],
              pendingRecovery: null,
            });
            assert.lengthOf(
              (yield* History.readRecoveryState(RunId.make("wrong:new-source")))
                .admittedContinuations,
              0,
            );
          }),
        );
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
        ),
      );
    },
  );

  it.effect(
    "receipt dedupe atomically persists both source directions and never rewrites a prior link",
    () =>
      Effect.gen(function* () {
        const { sink, input, successor, command } = yield* prepareRetry;
        yield* Effect.all([sink.commitCommand(input), sink.commitCommand(input)], {
          concurrency: "unbounded",
        });
        assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
          sourceRunId: run.id,
          threadId: run.threadId,
          status: "decided",
          outcome: "retried",
          successorRunId: successor.id,
        });
        const projection = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(
          run.threadId,
          ["runs"],
        );
        assert.strictEqual(
          projection.runs.find((item) => item.id === successor.id)
            ?.forkPrismContinuationSourceRunId,
          run.id,
        );
        const changed = { ...successor, id: RunId.make("run:wrong-successor") };
        const events = [event(changed, "wrong-successor")];
        const result = yield* sink
          .commitCommand({
            ...input,
            commandId: CommandId.make("duplicate-with-new-identity"),
            events,
            forkPlans: [continuationAdmission(command, events)!],
          })
          .pipe(Effect.flip);
        assert.strictEqual(result._tag, "ForkCommitGuardRejected");
        assert.strictEqual((yield* History.readRecoveryOutcome(run.id))?.status, "decided");
        assert.lengthOf(
          (yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(run.threadId, ["runs"]))
            .runs,
          2,
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "a later mutation failure rolls back the run, link, receipt and retry budget together",
    () =>
      Effect.gen(function* () {
        const { sink, input } = yield* prepareRetry;
        yield* sink
          .commitCommand({
            ...input,
            forkPlans: [
              ...input.forkPlans,
              {
                guards: [],
                mutations: [
                  Effect.fail(
                    new ForkCommitGuardRejected({
                      threadId: run.threadId,
                      kind: "storage_failure",
                    }),
                  ),
                ],
              },
            ],
          })
          .pipe(Effect.flip);
        assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
          sourceRunId: run.id,
          threadId: run.threadId,
          status: "pending",
          reason: "retry",
        });
        assert.strictEqual(
          (yield* (yield* Store.RecoveryStore).get(run.threadId))?.state,
          "retry_pending",
        );
        const sql = yield* SqlClient.SqlClient;
        assert.lengthOf(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${input.commandId}`,
          0,
        );
        assert.lengthOf(
          (yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(run.threadId, ["runs"]))
            .runs,
          1,
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("accepted no-op and metadata-only reset cannot invent a successor", () =>
    Effect.gen(function* () {
      const { sink, input, command } = yield* prepareRetry;
      yield* sink.commitCommand({
        ...input,
        forkPlans: [
          ...input.forkPlans,
          { guards: [Effect.succeed("accept_noop" as const)], mutations: [] },
        ],
      });
      assert.strictEqual((yield* History.readRecoveryOutcome(run.id))?.status, "pending");
      assert.lengthOf(
        (yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(run.threadId, ["runs"]))
          .runs,
        1,
      );
      assert.strictEqual(
        continuationAdmission(
          { ...command, forkPrismRetryOfRunId: undefined, usageLimitContinuationOfRunId: run.id },
          [],
        ),
        null,
      );
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect.each(["wrong_source", "wrong_model", "existing_run_id"] as const)(
    "refuses %s before events or facts commit",
    (variant) =>
      Effect.gen(function* () {
        const { sink, input, command, successor } = yield* prepareRetry;
        const wrong =
          variant === "wrong_source"
            ? { ...successor, forkPrismContinuationSourceRunId: RunId.make("wrong-source") }
            : { ...successor, modelSelection: { ...successor.modelSelection, model: "different" } };
        const events = [event(wrong, variant)];
        const result = yield* sink
          .commitCommand({ ...input, events, forkPlans: [continuationAdmission(command, events)!] })
          .pipe(Effect.flip);
        assert.strictEqual(result._tag, "ForkCommitGuardRejected");
        assert.strictEqual((yield* History.readRecoveryOutcome(run.id))?.status, "pending");
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect.each([
    ["mcp", "running"],
    ["mcp", "completed"],
    ["web", "running"],
    ["web", "completed"],
  ] as const)(
    "restart sweep concludes an older %s failure behind a newer %s run",
    ([origin, status]) =>
      Effect.gen(function* () {
        const seeded = recoveryEvents(run).map((item) =>
          item.type === "thread.created"
            ? { ...item, payload: { ...item.payload, creationSource: origin } }
            : item,
        );
        yield* persist("seed:older", seeded);
        const newer = {
          ...run,
          id: RunId.make("run:newer"),
          ordinal: 2,
          userMessageId: MessageId.make("newer-human-message"),
          status,
          completedAt: status === "running" ? null : run.completedAt,
        };
        yield* persist("newer:commit", [event(newer, "newer")]);
        const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
        yield* withReactor(
          Effect.gen(function* () {
            const reactor = yield* Reactor.RecoveryReactor;
            yield* reactor.sweep;
            assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
              sourceRunId: run.id,
              threadId: run.threadId,
              status: "decided",
              outcome: "not_retryable",
              reason: "superseded",
            });
            yield* reactor.sweep;
            assert.strictEqual((yield* History.readRecoveryOutcome(run.id))?.status, "decided");
            assert.strictEqual(yield* Queue.size(commands), 0);
          }),
          commands,
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "the committed non-MCP failure event produces a conclusion without a controller or clock tick",
    () =>
      Effect.gen(function* () {
        const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
        const observed = yield* Deferred.make<void>();
        yield* Effect.gen(function* () {
          yield* persist(
            "seed:non-mcp",
            recoveryEvents(run).map((item) =>
              item.type === "thread.created"
                ? { ...item, payload: { ...item.payload, creationSource: "web" as const } }
                : item,
            ),
          );
          yield* persist("failure:committed", [
            { ...event(run, "failure:committed"), type: "run.updated" },
          ]);
          yield* Deferred.await(observed);
          assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
            sourceRunId: run.id,
            threadId: run.threadId,
            status: "decided",
            outcome: "not_retryable",
            reason: "non_mcp",
          });
          assert.strictEqual(yield* (yield* Store.RecoveryStore).get(run.threadId), null);
          assert.strictEqual(yield* Queue.size(commands), 0);
        }).pipe(
          Effect.provide(
            Reactor.layer.pipe(
              Layer.provide(
                external(commands, Deferred.succeed(observed, undefined).pipe(Effect.asVoid)),
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(dependencies)),
  );

  it.effect("an archived failed run receives an automatic abandoned fact", () =>
    Effect.gen(function* () {
      yield* persist(
        "seed:archived",
        recoveryEvents(run).map((item) =>
          item.type === "thread.created"
            ? { ...item, payload: { ...item.payload, archivedAt: run.completedAt } }
            : item,
        ),
      );
      const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
      yield* withReactor(
        Effect.gen(function* () {
          yield* (yield* Reactor.RecoveryReactor).sweep;
          assert.deepStrictEqual(yield* History.readRecoveryOutcome(run.id), {
            sourceRunId: run.id,
            threadId: run.threadId,
            status: "decided",
            outcome: "abandoned",
            reason: "ineligible",
          });
          assert.strictEqual(yield* Queue.size(commands), 0);
        }),
        commands,
      );
    }).pipe(Effect.provide(dependencies)),
  );

  it.effect(
    "bounded fair batches exclude history without starving old failures behind newer work",
    () =>
      Effect.gen(function* () {
        yield* persist("seed:batch", [
          {
            id: EventId.make("batch:thread"),
            type: "thread.created",
            threadId: run.threadId,
            occurredAt: run.requestedAt,
            payload: threadFor(run),
          },
        ]);
        const events: OrchestrationV2DomainEvent[] = [];
        for (let index = 0; index < 130; index++) {
          const source = {
            ...run,
            id: RunId.make(`run:batch:${String(index).padStart(3, "0")}`),
            ordinal: index + 1,
          };
          events.push(event(source, `batch:${index}`), {
            id: EventId.make(`batch:error:${index}`),
            type: "turn-item.updated",
            threadId: source.threadId,
            occurredAt: source.requestedAt,
            payload: errorFor(source),
          });
        }
        const newer = {
          ...run,
          id: RunId.make("run:zz:newer"),
          ordinal: 131,
          status: "running" as const,
          completedAt: null,
        };
        events.push(event(newer, "batch:newer"));
        yield* persist("batch:commit", events);
        const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
        yield* withReactor(
          Effect.gen(function* () {
            const reactor = yield* Reactor.RecoveryReactor;
            const sql = yield* SqlClient.SqlClient;
            yield* reactor.sweep;
            assert.strictEqual(
              (yield* sql<{
                count: number;
              }>`SELECT count(*) AS count FROM fork_prism_recovery_outcomes`)[0]?.count,
              128,
            );
            yield* reactor.sweep;
            assert.strictEqual(
              (yield* sql<{
                count: number;
              }>`SELECT count(*) AS count FROM fork_prism_recovery_outcomes`)[0]?.count,
              130,
            );
            yield* reactor.sweep;
            assert.strictEqual(
              (yield* sql<{
                count: number;
              }>`SELECT count(*) AS count FROM fork_prism_recovery_outcomes`)[0]?.count,
              130,
            );
            assert.strictEqual(yield* Queue.size(commands), 0);
          }),
          commands,
        );
      }).pipe(Effect.provide(dependencies)),
  );
});
