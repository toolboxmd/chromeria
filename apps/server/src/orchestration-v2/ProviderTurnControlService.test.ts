import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  MessageId,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;

function makeProjection(input: {
  readonly now: DateTime.Utc;
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  readonly attemptId: RunAttemptId;
}): OrchestrationV2ThreadProjection {
  const runId = RunId.make("run:restart-session");
  const nodeId = NodeId.make("node:restart-session");
  return {
    thread: {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId: ProjectId.make("project:restart-session"),
      title: "Restart session",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: "/workspace",
      activeProviderThreadId: input.providerThread.id,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [
      {
        id: runId,
        threadId: input.threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId: input.providerThread.id,
        userMessageId: MessageId.make("message:restart-session"),
        rootNodeId: nodeId,
        activeAttemptId: RunAttemptId.make("attempt:replacement"),
        status: "starting",
        requestedAt: input.now,
        startedAt: input.now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    ],
    attempts: [
      {
        id: input.attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId: input.providerThread.id,
        providerTurnId: input.providerTurnId,
        reason: "initial",
        status: "superseded",
        startedAt: input.now,
        completedAt: input.now,
      },
    ],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [input.providerThread],
    providerTurns: [
      {
        id: input.providerTurnId,
        providerThreadId: input.providerThread.id,
        nodeId,
        runAttemptId: input.attemptId,
        nativeTurnRef: {
          driver,
          nativeId: "native-turn:restart-session",
          strength: "strong",
        },
        ordinal: 1,
        status: "running",
        startedAt: input.now,
        completedAt: null,
      },
    ],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: input.now,
  };
}

it.effect(
  "interrupts the historical session only for the exact committed restart replacement",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:restart-session");
      const oldSessionId = ProviderSessionId.make("provider-session:restart-session:old");
      const replacementSessionId = ProviderSessionId.make(
        "provider-session:restart-session:replacement",
      );
      const unrelatedSessionId = ProviderSessionId.make(
        "provider-session:restart-session:unrelated",
      );
      const providerThreadId = ProviderThreadId.make("provider-thread:restart-session");
      const providerTurnId = ProviderTurnId.make("provider-turn:restart-session");
      const attemptId = RunAttemptId.make("run-attempt:restart-session");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId: replacementSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "native-thread:restart-session",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const projection = yield* Ref.make(
        makeProjection({ now, threadId, providerThread, providerTurnId, attemptId }),
      );
      const interruptedThread = yield* Ref.make<OrchestrationV2ProviderThread | null>(null);
      const endedHold = yield* Ref.make<"cut-in" | "descendants" | undefined>(undefined);
      const interrupts = yield* Ref.make<
        ReadonlyArray<
          Parameters<ProviderAdapter.ProviderAdapterV2SessionRuntime["interruptTurn"]>[0]
        >
      >([]);
      const providerSession = {
        id: oldSessionId,
        driver,
        providerInstanceId,
        status: "running" as const,
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId,
        driver,
        providerSessionId: oldSessionId,
        providerSession,
        events: Stream.empty,
        endedTurnHold: () => Ref.get(endedHold),
        ensureThread: () => Effect.die("unused ensureThread"),
        resumeThread: () => Effect.die("unused resumeThread"),
        startTurn: () => Effect.die("unused startTurn"),
        steerTurn: () => Effect.die("unused steerTurn"),
        interruptTurn: (target) =>
          Effect.all(
            [
              Ref.set(interruptedThread, target.providerThread),
              Ref.update(interrupts, (current) => [...current, target]),
              Ref.update(projection, (current) => ({
                ...current,
                providerTurns: current.providerTurns.map((turn) =>
                  turn.id === providerTurnId
                    ? { ...turn, status: "interrupted" as const, completedAt: now }
                    : turn,
                ),
              })),
            ],
            { discard: true },
          ),
        respondToRuntimeRequest: () => Effect.die("unused respondToRuntimeRequest"),
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
      const layerProjection = Layer.succeed(
        ProjectionStore.ProjectionStoreV2,
        ProjectionStore.ProjectionStoreV2.of({
          apply: () => Effect.void,
          getLimitRecoveryCandidates: () => Effect.die("unused getLimitRecoveryCandidates"),
          getOpenRemoteDelegatedTasks: Effect.die("unused getOpenRemoteDelegatedTasks"),
          getShellSnapshot: () => Effect.die("unused getShellSnapshot"),
          listActiveSubagentThreadIds: () => Effect.die("unused listActiveSubagentThreadIds"),
          listOlderSettledThreads: () => Effect.die("unused listOlderSettledThreads"),
          readShellSnapshot: () => Effect.die("unused readShellSnapshot"),
          getThreadShell: () => Effect.die("unused getThreadShell"),
          findGuestShareThreadId: () => Effect.die("unused findGuestShareThreadId"),
          getThread: () => Ref.get(projection).pipe(Effect.map((state) => state.thread)),
          getSettlementCandidates: () => Effect.die("unused getSettlementCandidates"),
          getThreadsWithPullRequests: () => Effect.die("unused getThreadsWithPullRequests"),
          getThreadWorktreePaths: () => Effect.die("unused getThreadWorktreePaths"),
          getThreadProjection: () => Effect.die("control effects must not load transcript"),
          getTurnStartContext: () => Effect.die("unused"),
          getTurnStartHistory: () => Effect.die("unused"),
          getRuntimeRecoveryProjection: () => Ref.get(projection),
          getUnresolvedRestartCuts: () => Effect.die("unused getUnresolvedRestartCuts"),
          getPlan: () => Effect.die("unused"),
          hasUnpairedRunInterruptRequest: () => Effect.die("unused interrupt read"),
          getThreadAttachmentIds: () => Effect.die("Unused attachment lookup"),
          searchThread: () => Effect.die("unused"),
          searchThreadStream: () => Stream.empty,
          getThreadHistoryPage: () => Effect.die("unused"),
          getTimelinePage: () => Effect.die("Unused timeline read"),
          getMessageCount: () => Effect.die("unused message count"),
          getNextTurnItemOrdinal: () => Effect.die("unused ordinal read"),
          getTurnItem: () => Effect.die("unused turn item read"),
          getThreadRecords: () => Effect.die("unused record read"),
          getRuntimeRequest: () => Effect.die("unused getRuntimeRequest"),
          getRunningTurnContext: () => Effect.die("unused getRunningTurnContext"),
          getThreadProviderContext: () => Effect.die("unused getThreadProviderContext"),
          getRuntimeResponseContext: () => Effect.die("unused getRuntimeResponseContext"),
          getPendingNativeUserInputs: () => Effect.die("unused getPendingNativeUserInputs"),
          getProviderControlContext: (_threadId, target) =>
            Ref.get(projection).pipe(
              Effect.map((current) => ({
                providerThread: current.providerThreads.find(
                  (thread) => thread.id === target.providerThreadId,
                ),
                providerTurn: current.providerTurns.find(
                  (turn) => turn.id === target.providerTurnId,
                ),
                attempt: current.attempts.find((attempt) => attempt.id === target.attemptId),
                message: undefined,
                run: undefined,
              })),
            ),
          getCheckpointContext: () => Effect.die("not used"),
          getCheckpointRollbackContext: () => Effect.die("unused rollback context"),
          getCheckpointCaptureContext: () => Effect.die("not used"),
          getRunMessage: () => Effect.die("not used"),
          isNodeStored: () => Effect.die("not used"),
          canStartQueuedRun: () => Effect.die("not used"),
          getRecoveryThreadIds: () => Effect.die("unused getRecoveryThreadIds"),
          getUnreadableThreadIds: () => Effect.die("unused getUnreadableThreadIds"),
          getThreadSnapshot: () => Effect.die("unused getThreadSnapshot"),
          getThreadSnapshotWindow: () => Effect.die("unused getThreadSnapshotWindow"),
        }),
      );
      const layerSessionManager = Layer.succeed(
        ProviderSessionManager.ProviderSessionManagerV2,
        ProviderSessionManager.ProviderSessionManagerV2.of({
          shutdown: Effect.void,
          open: () => Effect.die("unused open"),
          get: (providerSessionId) =>
            Effect.succeed(
              providerSessionId === oldSessionId ? Option.some(runtime) : Option.none(),
            ),
          isLive: (providerSessionId) => Effect.succeed(providerSessionId === oldSessionId),
          close: () => Effect.void,
          closeInstance: () => Effect.void,
          release: () => Effect.void,
          detach: () => Effect.void,
        }),
      );
      const layerControl = ProviderTurnControlService.layer.pipe(
        Layer.provide(Layer.merge(layerProjection, layerSessionManager)),
      );

      const [ordinaryInterrupt, unrelatedRestart] = yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const ordinary = yield* Effect.exit(
          control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          }),
        );
        const unrelated = yield* Effect.exit(
          control.interruptAndAwaitTerminal({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: unrelatedSessionId,
            providerThreadId,
            providerTurnId,
            interruptedAttemptId: attemptId,
          }),
        );
        return [ordinary, unrelated] as const;
      }).pipe(Effect.provide(layerControl));

      assert.isTrue(Exit.isFailure(ordinaryInterrupt));
      assert.isTrue(Exit.isFailure(unrelatedRestart));
      assert.isNull(yield* Ref.get(interruptedThread));

      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interruptAndAwaitTerminal({
          threadId,
          providerSessionId: oldSessionId,
          replacementProviderSessionId: replacementSessionId,
          providerThreadId,
          providerTurnId,
          interruptedAttemptId: attemptId,
        });
      }).pipe(Effect.provide(layerControl));

      const interrupted = yield* Ref.get(interruptedThread);
      assert.isNotNull(interrupted);
      assert.equal(interrupted?.providerSessionId, oldSessionId);
      assert.equal(interrupted?.id, providerThreadId);
      assert.equal(interrupted?.nativeThreadRef?.nativeId, "native-thread:restart-session");

      // Ordinary restarts remain soft. Explicit cut-ins carry Stop scope and
      // wait for success before delivery; Stop itself carries no replacement.
      assert.isUndefined((yield* Ref.get(interrupts))[0]?.requestRuntimeRestart);
      assert.isUndefined((yield* Ref.get(interrupts))[0]?.replacementFollows);
      const restart = (interruption?: "stop") =>
        Effect.gen(function* () {
          const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
          yield* control.interruptAndAwaitTerminal({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: replacementSessionId,
            providerThreadId,
            providerTurnId,
            interruptedAttemptId: attemptId,
            ...(interruption === undefined ? {} : { interruption }),
          });
        }).pipe(Effect.provide(layerControl));
      const setTargetStatus = (status: "running" | "completed" | "failed" | "interrupted") =>
        Ref.update(projection, (current) => ({
          ...current,
          providerTurns: current.providerTurns.map((turn) =>
            turn.id === providerTurnId ? { ...turn, status } : turn,
          ),
        }));
      yield* setTargetStatus("running");
      yield* restart("stop");
      assert.equal((yield* Ref.get(interrupts)).length, 2);
      assert.isTrue((yield* Ref.get(interrupts))[1]?.requestRuntimeRestart);
      assert.isTrue((yield* Ref.get(interrupts))[1]?.replacementFollows);

      yield* setTargetStatus("completed");
      yield* restart();
      assert.equal((yield* Ref.get(interrupts)).length, 2);
      yield* restart("stop");
      assert.equal((yield* Ref.get(interrupts)).length, 3);
      assert.isTrue((yield* Ref.get(interrupts))[2]?.replacementFollows);

      // A native interruption has already retired the old ingestor. Never stop
      // a newer turn, or send an obsolete cut-in after its run stopped starting.
      yield* setTargetStatus("interrupted");
      yield* restart("stop");
      assert.equal((yield* Ref.get(interrupts)).length, 3);
      yield* setTargetStatus("completed");
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerTurns: [
          ...current.providerTurns.map((turn) => ({ ...turn, ordinal: 2 })),
          {
            ...current.providerTurns[0]!,
            id: ProviderTurnId.make("provider-turn:older-held"),
            runAttemptId: RunAttemptId.make("attempt:older-held"),
            ordinal: 1,
            status: "running" as const,
          },
        ],
      }));
      yield* restart("stop");
      assert.equal(
        (yield* Ref.get(interrupts)).length,
        4,
        "An older held turn must not hide the captured completed cut-in target.",
      );
      assert.equal((yield* Ref.get(interrupts)).at(-1)?.providerTurnId, providerTurnId);
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerTurns: [
          ...current.providerTurns.filter((turn) => turn.id === providerTurnId),
          {
            ...current.providerTurns[0]!,
            id: ProviderTurnId.make("provider-turn:newer"),
            runAttemptId: RunAttemptId.make("attempt:newer-unrelated"),
            ordinal: 3,
            status: "running" as const,
          },
        ],
      }));
      yield* setTargetStatus("completed");
      yield* restart("stop");
      assert.equal((yield* Ref.get(interrupts)).length, 4);
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerTurns: current.providerTurns.map((turn) =>
          turn.id === providerTurnId ? turn : { ...turn, runAttemptId: attemptId },
        ),
      }));
      yield* restart("stop");
      assert.equal(
        (yield* Ref.get(interrupts)).length,
        5,
        "The target attempt's own goal continuation belongs to the same stop scope.",
      );
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerTurns: current.providerTurns.filter((turn) => turn.id === providerTurnId),
        runs: current.runs.map((run) => ({ ...run, status: "interrupted" as const })),
      }));
      yield* restart("stop");
      assert.equal((yield* Ref.get(interrupts)).length, 5);

      // Restore the recorded binding for an ordinary Stop on the settled turn.
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerThreads: [{ ...providerThread, providerSessionId: oldSessionId }],
      }));
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interrupt({
          threadId,
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
        });
      }).pipe(Effect.provide(layerControl));
      assert.equal((yield* Ref.get(interrupts)).length, 6);
      assert.isTrue((yield* Ref.get(interrupts))[5]?.requestRuntimeRestart);
      assert.isUndefined((yield* Ref.get(interrupts))[5]?.replacementFollows);

      // A non-completed root may still own a live native cohort. Only the
      // runtime can distinguish that from a fully retired historical turn.
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerThreads: [providerThread],
        runs: current.runs.map((run) => ({ ...run, status: "starting" as const })),
      }));
      for (const status of ["failed", "interrupted"] as const) {
        for (const hold of [undefined, "descendants", "cut-in"] as const) {
          yield* setTargetStatus(status);
          yield* Ref.set(endedHold, hold);
          const before = (yield* Ref.get(interrupts)).length;
          yield* restart();
          assert.equal((yield* Ref.get(interrupts)).length, before, "routine restart stays soft");
          yield* restart("stop");
          assert.equal(
            (yield* Ref.get(interrupts)).length,
            before + (hold === undefined ? 0 : 1),
            `${status} root with ${hold ?? "no"} native owner`,
          );
        }
      }
      const stopCaptured = (replacement: ProviderSessionId) =>
        Effect.gen(function* () {
          const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
          yield* control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: replacement,
            providerThreadId,
            providerTurnId,
          });
        }).pipe(Effect.provide(layerControl));
      const beforeCapturedStop = (yield* Ref.get(interrupts)).length;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(stopCaptured(unrelatedSessionId))));
      assert.equal((yield* Ref.get(interrupts)).length, beforeCapturedStop);
      yield* stopCaptured(replacementSessionId);
      assert.equal((yield* Ref.get(interrupts)).length, beforeCapturedStop + 1);
      assert.equal(
        (yield* Ref.get(interrupts)).at(-1)?.providerThread.providerSessionId,
        oldSessionId,
      );
      assert.isUndefined((yield* Ref.get(interrupts)).at(-1)?.replacementFollows);
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerThreads: [{ ...providerThread, providerSessionId: unrelatedSessionId }],
      }));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(stopCaptured(replacementSessionId))));
      assert.equal((yield* Ref.get(interrupts)).length, beforeCapturedStop + 1);
    }),
);
