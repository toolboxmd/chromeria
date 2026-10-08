// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RunId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import { continuationAdmission } from "../prism/continuationAdmission.ts";
import * as History from "../prism/RecoveryHistory.ts";
import { continuationRunFields } from "../prism/RecoveryHooks.ts";
import * as Store from "../prism/RecoveryStore.ts";
import { recoveryEvents, recoveryRun as run } from "../prism/recovery.testkit.ts";
import { followRun } from "./handoff.ts";

/** One server process on the database file; a new call is a restart. */
const inProcess = <A, E, R>(path: string, work: Effect.Effect<A, E, R>) => {
  const database = Persistence.layerFromPath(path).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provide(database)),
    Projection.layer.pipe(Layer.provide(database)),
  );
  return work.pipe(
    Effect.provide(
      Layer.mergeAll(
        stores,
        EventSink.layer.pipe(Layer.provide(stores)),
        Store.layer.pipe(Layer.provide(database)),
      ),
    ),
  );
};

const persist = (id: string, events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
  Effect.gen(function* () {
    yield* (yield* EventSink.EventSinkV2).commitCommand({
      commandId: CommandId.make(id),
      commandType: "fixture",
      threadId: run.threadId,
      acceptedAt: run.requestedAt,
      events,
      effects: [],
    });
  });

const runEvent = (
  type: "run.created" | "run.updated",
  payload: OrchestrationV2Run,
  suffix: string,
): OrchestrationV2DomainEvent => ({
  id: EventId.make(`event:${suffix}`),
  type,
  threadId: payload.threadId,
  occurredAt: payload.requestedAt,
  payload,
});

const resetAt = "2026-10-08T12:00:00Z";

it.effect(
  "a scheduled run's work follows #169's recovery state across opt-out, re-enable, admission and restarts",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "scheduled-recovery-"));
    const path = NodePath.join(directory, "state.sqlite");
    // The usage-limit reset continuation upstream dispatches once the user re-enables it.
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
    const successor: OrchestrationV2Run = {
      ...run,
      ...continuationRunFields(command),
      id: RunId.make("run:reset:successor"),
      ordinal: 2,
      userMessageId: command.messageId,
      status: "running",
      completedAt: null,
    };
    return Effect.gen(function* () {
      // Opted out at the usage limit: recovery concluded, so the scheduler decides.
      yield* inProcess(
        path,
        Effect.gen(function* () {
          yield* persist(
            "seed:optout",
            recoveryEvents(run).map((event) =>
              event.type === "thread.created"
                ? {
                    ...event,
                    payload: {
                      ...event.payload,
                      limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
                    },
                  }
                : event.type === "turn-item.updated" && event.payload.type === "error"
                  ? {
                      ...event,
                      payload: {
                        ...event.payload,
                        failure: { ...event.payload.failure, class: "usage_limit", resetAt },
                      },
                    }
                  : event,
            ),
          );
          yield* History.writeRecoveryOutcome({
            sourceRunId: run.id,
            threadId: run.threadId,
            status: "decided",
            outcome: "not_retryable",
            reason: "opted_out",
          });
          assert.deepEqual(yield* followRun(run.id), {
            kind: "ended",
            runId: run.id,
            started: false,
          });
        }),
      );
      // The user re-enables the reset: pending recovery holds over the recorded opt-out.
      yield* inProcess(
        path,
        Effect.gen(function* () {
          const thread = yield* (yield* Projection.ProjectionStoreV2).getThread(run.threadId);
          yield* persist("reenable", [
            {
              id: EventId.make("event:reenable"),
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
          assert.deepEqual(yield* followRun(run.id), { kind: "waiting" });
        }),
      );
      // A restart reads the same durable intent.
      yield* inProcess(
        path,
        Effect.gen(function* () {
          assert.deepEqual(yield* followRun(run.id), { kind: "waiting" });
          // Upstream admits the continuation with the run itself.
          const events = [runEvent("run.created", successor, "reset:successor")];
          yield* (yield* EventSink.EventSinkV2).commitCommand({
            commandId: command.commandId,
            commandType: command.type,
            threadId: run.threadId,
            acceptedAt: run.requestedAt,
            events,
            effects: [],
            forkPlans: [continuationAdmission(command, events)!],
          });
          // The admitted continuation is followed first; it is still running.
          assert.deepEqual(yield* followRun(run.id), { kind: "waiting" });
        }),
      );
      yield* inProcess(
        path,
        Effect.gen(function* () {
          assert.deepEqual(yield* followRun(run.id), { kind: "waiting" });
          yield* persist("successor:completed", [
            runEvent(
              "run.updated",
              { ...successor, status: "completed", completedAt: run.completedAt },
              "successor:completed",
            ),
          ]);
          // Completed through the exact chain; no provider turn was recorded for it.
          assert.deepEqual(yield* followRun(run.id), {
            kind: "completed",
            runId: successor.id,
            started: false,
          });
        }),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);
