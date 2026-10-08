import { expect, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as AppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Checkpoints from "../orchestration-v2/CheckpointService.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as IdAllocator from "../orchestration-v2/IdAllocator.ts";
import * as Ingestor from "../orchestration-v2/ProviderEventIngestor.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2SessionRuntime,
} from "../orchestration-v2/ProviderAdapter.ts";
import * as RunExecution from "../orchestration-v2/RunExecutionService.ts";
import * as ThreadCommands from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as Decorator from "./ProviderEventIngestor.ts";
import * as Stats from "./StreamStatsStore.ts";
import * as StreamClock from "./streamClock.ts";
import { recoveryRun, threadFor } from "./recovery.testkit.ts";

it.effect.each([
  { mode: "paragraph", unfinished: false },
  { mode: "turn", unfinished: false },
  { mode: "paragraph", unfinished: true },
] as const)(
  "measures original text and terminal-only completion: $mode unfinished=$unfinished",
  ({ mode, unfinished }) => {
    const interval = unfinished ? 60_000 : 1_000;
    return Effect.gen(function* () {
      const epoch = yield* Clock.currentTimeMillis;
      const at = DateTime.makeUnsafe(epoch);
      const driver = ProviderDriverKind.make("codex");
      const nodeId = NodeId.make("clock:root");
      const providerThreadId = ProviderThreadId.make("clock:native");
      const providerTurnId = ProviderTurnId.make("clock:turn");
      const providerSessionId = ProviderSessionId.make("clock:session");
      const attemptId = RunAttemptId.make("clock:attempt");
      const run = {
        ...recoveryRun,
        status: "running" as const,
        rootNodeId: nodeId,
        activeAttemptId: attemptId,
        providerThreadId,
        requestedAt: at,
        startedAt: at,
        completedAt: null,
      };
      const appThread = threadFor(run);
      const rootNode: OrchestrationV2ExecutionNode = {
        id: nodeId,
        threadId: run.threadId,
        runId: run.id,
        parentNodeId: null,
        rootNodeId: nodeId,
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: at,
        completedAt: null,
      };
      const attempt: OrchestrationV2RunAttempt = {
        id: attemptId,
        runId: run.id,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId: run.providerInstanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: at,
        completedAt: null,
      };
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: run.providerInstanceId,
        providerSessionId,
        appThreadId: run.threadId,
        ownerNodeId: nodeId,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: at,
        updatedAt: at,
      };
      const drained = yield* Deferred.make<void>();
      const database = Persistence.layerMemory;
      const stores = Layer.mergeAll(
        database,
        EventStore.layer.pipe(Layer.provide(database)),
        Projection.layer.pipe(Layer.provide(database)),
      );
      const sink = EventSink.layer.pipe(Layer.provide(stores));
      const clockLayer = StreamClock.layer.pipe(
        Layer.provide(Stats.layer),
        Layer.provide(database),
      );
      const ingestor = Decorator.layer.pipe(
        Layer.provide(
          Layer.merge(
            clockLayer,
            Ingestor.layer.pipe(
              Layer.provide(Layer.mergeAll(sink, stores, IdAllocator.layer, ThreadCommands.layer)),
            ),
          ),
        ),
      );
      const hooks = Layer.effect(
        StreamClock.StreamClockHooks,
        Effect.map(StreamClock.StreamClock, (clock) => ({
          ...clock,
          endAttempt: (...args: Parameters<typeof clock.endAttempt>) =>
            clock
              .endAttempt(...args)
              .pipe(Effect.andThen(Deferred.succeed(drained, undefined)), Effect.asVoid),
        })),
      ).pipe(Layer.provide(clockLayer));
      const execution = RunExecution.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            ingestor,
            hooks,
            sink,
            IdAllocator.layer,
            AppModelContext.layerEmpty,
            Settings.layerTest({ responseStreamingMode: mode }),
            Layer.mock(Checkpoints.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          ),
        ),
      );
      const testLayer = Layer.mergeAll(
        execution,
        ingestor,
        clockLayer,
        stores,
        sink,
        Stats.layer.pipe(Layer.provide(database)),
      );
      yield* Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const clock = yield* StreamClock.StreamClock;
        const base = { threadId: run.threadId, occurredAt: at };
        yield* eventSink.write({
          events: [
            {
              ...base,
              id: EventId.make("clock:thread"),
              type: "thread.created",
              payload: appThread,
            },
            { ...base, id: EventId.make("clock:run"), type: "run.created", payload: run },
            { ...base, id: EventId.make("clock:node"), type: "node.updated", payload: rootNode },
            {
              ...base,
              id: EventId.make("clock:attempt"),
              type: "run-attempt.updated",
              payload: attempt,
            },
            {
              ...base,
              id: EventId.make("clock:provider"),
              type: "provider-thread.updated",
              payload: providerThread,
            },
          ],
        });
        const message = (text: string): ProviderAdapterV2Event => ({
          type: "message.updated",
          driver,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make("clock:answer"),
            threadId: run.threadId,
            runId: run.id,
            nodeId,
            role: "assistant",
            text,
            attachments: [],
            streaming: true,
            createdAt: at,
            updatedAt: at,
          },
        });
        const events: ReadonlyArray<ProviderAdapterV2Event> = [
          {
            type: "provider_turn.updated",
            driver,
            threadId: run.threadId,
            providerTurn: {
              id: providerTurnId,
              providerThreadId,
              nodeId,
              runAttemptId: attemptId,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running",
              startedAt: at,
              completedAt: null,
            },
          },
          message("An unfinished paragraph"),
          message(
            unfinished
              ? "An unfinished paragraph keeps growing"
              : "First paragraph.\n\nAn unfinished paragraph",
          ),
          message(
            unfinished
              ? "An unfinished paragraph keeps growing for three minutes"
              : "First paragraph.\n\nAn even longer unfinished paragraph",
          ),
          {
            type: "turn.terminal",
            driver,
            providerThreadId,
            providerTurnId,
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        ];
        const stream = Stream.fromIterable(
          events.map((event, index) => ({ event, index })),
          { chunkSize: 1 },
        ).pipe(
          Stream.mapEffect(({ event, index }) =>
            Effect.gen(function* () {
              // The next source event is a drain boundary for the preceding ingest/tap.
              if (event.type === "turn.terminal") {
                expect(yield* clock.liveness).toMatchObject([
                  {
                    firstTokenAt: epoch + interval,
                    lastStreamAt: epoch + 3 * interval,
                    eventCount: 4,
                  },
                ]);
                const normalized = yield* (yield* Ingestor.ProviderEventIngestorV2).normalize({
                  threadId: run.threadId,
                  runId: run.id,
                  providerSessionId,
                  providerInstanceId: run.providerInstanceId,
                  event,
                });
                expect(normalized).toEqual([]);
              }
              yield* TestClock.setTime(epoch + index * interval);
              return event;
            }),
          ),
        );
        // The fake is only the external provider boundary; execution, filtering,
        // normalization, EventSink and SQLite are the production implementations.
        const session = {
          driver,
          events: stream,
          startTurn: () => Effect.void,
        } as unknown as ProviderAdapterV2SessionRuntime;
        yield* (yield* RunExecution.RunExecutionServiceV2).startRootRun({
          commandId: CommandId.make("clock:start"),
          appThread,
          providerSessionId,
          session,
          run,
          rootNode,
          checkpointScope: {
            id: CheckpointScopeId.make("clock:scope"),
            threadId: run.threadId,
            runId: run.id,
            nodeId,
            parentScopeId: null,
            providerThreadId,
            kind: "root_run",
            ordinalWithinParent: 0,
            advancesAppRunCount: true,
            cwd: process.cwd(),
            createdAt: at,
          },
          providerThread,
          attempt,
          attemptId,
          providerTurnOrdinal: 1,
          message: {
            messageId: run.userMessageId,
            text: "Work",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: run.modelSelection,
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
          },
        });
        yield* Deferred.await(drained);
        expect(yield* clock.liveness).toEqual([]);
        const sql = yield* SqlClient.SqlClient;
        expect(
          yield* sql`SELECT outcome, time_to_first_token_ms AS firstToken, event_count AS eventCount FROM fork_prism_stream_stats`,
        ).toEqual([{ outcome: "completed", firstToken: interval, eventCount: 5 }]);
        const projection = yield* Projection.ProjectionStoreV2;
        const records = yield* projection.getThreadRecords(run.threadId, ["messages"]);
        expect(records.messages.map((m) => m.text)).toEqual(
          mode === "turn" || unfinished ? [] : ["First paragraph.\n\n"],
        );
      }).pipe(Effect.provide(testLayer));
    });
  },
);
