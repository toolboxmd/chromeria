import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it, vi } from "@effect/vitest";
import {
  CheckpointScopeId,
  ContextTransferId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as BareLaunch from "./BareLaunch.ts";
import * as ContextHandoff from "./ContextHandoffService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessions from "./ProviderSessionManager.ts";
import * as ProviderTurnStart from "./ProviderTurnStartService.ts";
import * as RunExecution from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuth from "../provider/ProviderAuthService.ts";
import * as ContextmdServing from "../contextmd/ContextmdServing.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";

/** External native-runtime and store ports, with the real turn-start owner and projector. */
function harness(
  options: {
    accepted?: boolean;
    rolledBack?: boolean;
    earlierDelivered?: boolean;
    resumeFailure?: boolean;
    missed?: "user" | "mixed";
    fork?: "before-carrier" | "after-carrier";
    driver?: "codex" | "grok";
    bare?: boolean;
    steeringSender?: ThreadId;
  } = {},
) {
  const now = DateTime.makeUnsafe("2026-10-14T00:00:00Z");
  const driver = ProviderDriverKind.make(options.driver ?? "codex");
  const instanceId = ProviderInstanceId.make("bare-history");
  const threadId = ThreadId.make("thread:bare-history");
  const runId = RunId.make("run:bare-history:current");
  const previousId = RunId.make("run:bare-history:previous");
  const attemptId = RunAttemptId.make("attempt:bare-history:current");
  const previousAttempt = RunAttemptId.make("attempt:bare-history:previous");
  const nodeId = NodeId.make("node:bare-history");
  const providerThreadId = ProviderThreadId.make("provider-thread:bare-history");
  const sessionId = ProviderSessionId.make("session:bare-history");
  const selection = { instanceId, model: "test" };
  const bare =
    options.bare === false
      ? undefined
      : {
          additions: [
            {
              type: "skill" as const,
              path: "/named/SKILL.md",
              sha256: "a".repeat(64),
              byteCount: 10,
            },
          ],
        };
  const nativeRef = { driver, nativeId: "bare-native", strength: "strong" as const };
  const providerThread = {
    id: providerThreadId,
    driver,
    providerInstanceId: instanceId,
    providerSessionId: sessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: options.accepted || options.resumeFailure ? nativeRef : null,
    nativeConversationHeadRef: null,
    status: "not_loaded" as const,
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const run = {
    id: runId,
    threadId,
    ordinal: 2,
    providerInstanceId: instanceId,
    modelSelection: selection,
    providerThreadId,
    userMessageId: MessageId.make("message:bare-history:current"),
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status: "starting" as const,
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const previousRun = {
    ...run,
    id: previousId,
    ordinal: 1,
    activeAttemptId: previousAttempt,
    status: options.rolledBack
      ? ("rolled_back" as const)
      : options.missed
        ? ("failed" as const)
        : ("completed" as const),
  };
  const attempt = {
    id: attemptId,
    runId,
    rootNodeId: nodeId,
    attemptOrdinal: 1,
    providerInstanceId: instanceId,
    providerThreadId,
    providerTurnId: null,
    reason:
      options.steeringSender === undefined ? ("initial" as const) : ("steering_restart" as const),
    ...(options.steeringSender === undefined
      ? {}
      : {
          requestedBy: {
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
            senderThreadId: options.steeringSender,
          },
        }),
    status: "pending" as const,
    startedAt: null,
    completedAt: null,
  };
  const previousTurn = {
    id: ProviderTurnId.make("turn:bare-history:previous"),
    providerThreadId,
    nodeId: options.earlierDelivered ? NodeId.make("node:earlier") : nodeId,
    runAttemptId: options.earlierDelivered
      ? RunAttemptId.make("attempt:bare-history:earlier")
      : previousAttempt,
    nativeTurnRef: { driver, nativeId: "previous-turn", strength: "strong" as const },
    ordinal: 1,
    status: "completed" as const,
    startedAt: now,
    completedAt: now,
  };
  const userItem = {
    id: TurnItemId.make("item:bare-history:missed"),
    threadId,
    runId: previousId,
    nodeId,
    providerThreadId,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed" as const,
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "user_message" as const,
    messageId: MessageId.make("message:missed"),
    inputIntent: "turn_start" as const,
    text: "Missed task exactly\n  keep spaces",
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
  };
  let projection: OrchestrationV2ThreadProjection = {
    thread: {
      id: threadId,
      projectId: ProjectId.make("project:bare-history"),
      title: "Bare history",
      providerInstanceId: instanceId,
      modelSelection: selection,
      ...(bare === undefined ? {} : { bare }),
      coOwners: [],
      createdBy: "user",
      creationSource: "web",
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    runs: [...(options.accepted || options.missed ? [previousRun] : []), run],
    attempts: [
      ...(options.accepted || options.missed
        ? [
            {
              ...attempt,
              id: previousAttempt,
              runId: previousId,
              attemptOrdinal: options.earlierDelivered ? 2 : 1,
              status: options.earlierDelivered ? ("failed" as const) : ("completed" as const),
              nativeThreadId: nativeRef.nativeId,
            },
          ]
        : []),
      ...(options.earlierDelivered
        ? [
            {
              ...attempt,
              id: RunAttemptId.make("attempt:bare-history:earlier"),
              runId: previousId,
              rootNodeId: NodeId.make("node:earlier"),
              nativeThreadId: nativeRef.nativeId,
              status: "completed" as const,
            },
          ]
        : []),
      attempt,
    ],
    nodes: [
      {
        id: nodeId,
        threadId,
        runId,
        rootNodeId: nodeId,
        parentNodeId: null,
        kind: "root_turn",
        status: "pending",
        countsForRun: true,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: CheckpointScopeId.make("scope:bare-history"),
        startedAt: null,
        completedAt: null,
      },
    ],
    providerThreads: [providerThread],
    providerTurns: options.accepted ? [previousTurn] : [],
    messages: [
      {
        id: run.userMessageId,
        threadId,
        runId,
        nodeId,
        role: "user",
        text: "Run the task.",
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    ],
    checkpointScopes: [
      {
        id: CheckpointScopeId.make("scope:bare-history"),
        threadId,
        runId,
        nodeId,
        parentScopeId: null,
        providerThreadId,
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/workspace",
        createdAt: now,
      },
    ],
    turnItems: options.missed
      ? [
          ...(options.earlierDelivered
            ? [
                {
                  ...userItem,
                  id: TurnItemId.make("item:earlier"),
                  nodeId: NodeId.make("node:earlier"),
                  text: "Already delivered",
                },
              ]
            : []),
          userItem,
          ...(options.missed === "mixed"
            ? [
                {
                  ...userItem,
                  id: TurnItemId.make("item:assistant:missed"),
                  type: "assistant_message" as const,
                  streaming: false,
                  text: "Not user input",
                },
              ]
            : []),
        ]
      : [],
    providerSessions: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    runtimeRequests: [],
    subagents: [],
    plans: [],
    checkpoints: [],
    updatedAt: now,
  };
  const sourceThreadId = ThreadId.make("thread:bare-history:source");
  const source: OrchestrationV2ThreadProjection = {
    ...projection,
    providerThreads: [
      {
        ...providerThread,
        appThreadId: sourceThreadId,
        nativeThreadRef: { ...nativeRef, nativeId: "source-native" },
      },
    ],
    runs: [previousRun],
    attempts: [
      { ...attempt, id: previousAttempt, runId: previousId, nativeThreadId: "source-native" },
    ],
    providerTurns: options.fork === "after-carrier" ? [previousTurn] : [],
  };
  if (options.fork !== undefined) {
    projection = {
      ...projection,
      contextTransfers: [
        {
          id: ContextTransferId.make("transfer:bare-history"),
          type: "fork",
          sourceThreadId,
          targetThreadId: threadId,
          sourcePoint: { threadId: sourceThreadId, runId: previousId },
          basePoint: null,
          sourceProviderInstanceId: instanceId,
          targetProviderInstanceId: instanceId,
          targetRunId: runId,
          status: "pending",
          resolution: null,
          createdBy: "user",
          error: null,
          createdAt: now,
          updatedAt: now,
          consumedAt: null,
        },
      ],
    };
  }
  const delivered: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const contextmd = {
    stageSessionInstructions: vi.fn(() =>
      Effect.succeed({
        reservation: "contextmd-reservation",
        text: "UNNAMED_CONTEXTMD_NATIVE_POINTER",
      }),
    ),
    recordInstallations: vi.fn(() => Effect.void),
    nativeSessionRegistered: vi.fn(() => Effect.void),
    attachSessionInstructions: vi.fn(() => Effect.void),
    recordNativeFork: vi.fn(() => Effect.void),
    releaseSessionInstructions: vi.fn(() => Effect.void),
  };
  const additions = vi.fn(() => Effect.succeed("NAMED_BODY"));
  const ensure = vi.fn(() => Effect.succeed({ ...providerThread, nativeThreadRef: nativeRef }));
  const resume = vi.fn(() =>
    options.resumeFailure
      ? Effect.fail(
          new ProviderAdapter.ProviderAdapterEnsureThreadError({
            driver,
            threadId,
            cause: "native session missing",
          }),
        )
      : Effect.succeed({ ...providerThread, nativeThreadRef: nativeRef }),
  );
  const session: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
    instanceId,
    driver,
    providerSessionId: sessionId,
    providerSession: {
      id: sessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: "/workspace",
      model: null,
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    },
    events: Stream.never,
    createdSessions: () => [
      {
        nativeSessionId: nativeRef.nativeId,
        text: "UNNAMED_CONTEXTMD_NATIVE_POINTER",
        site: "open",
      },
    ],
    ensureThread: ensure,
    resumeThread: resume,
    startTurn: (turn) =>
      Effect.sync(() => {
        delivered.push(turn);
      }),
    steerTurn: () => Effect.die("unexpected steering"),
    interruptTurn: () => Effect.die("unexpected interrupt"),
    respondToRuntimeRequest: () => Effect.die("unexpected approval"),
    readThreadSnapshot: () => Effect.die("unexpected snapshot"),
    rollbackThread: () => Effect.die("unexpected rollback"),
    forkThread: () => Effect.succeed({ ...providerThread, nativeThreadRef: nativeRef }),
  };
  const open = vi.fn<ProviderSessions.ProviderSessionManagerV2["Service"]["open"]>(() =>
    Effect.succeed(session),
  );
  const layer = ProviderTurnStart.layer.pipe(
    Layer.provide(NodePath.layer),
    Layer.provide(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-bare-turn-start-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
        ServerSettings.layerTest(),
        IdAllocator.layer,
        FileSystem.layerNoop({}),
        Layer.mock(BareLaunch.BareLaunch)({ deliveryText: additions }),
        Layer.mock(ContextmdServing.ContextmdServing)(contextmd),
        Layer.mock(ContextHandoff.ContextHandoffServiceV2)({}),
        Layer.mock(GitWorkflow.GitWorkflowService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(ProviderAuth.ProviderAuthService)({
          tryHandlePromptCommand: () => Effect.succeed(false),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: () =>
            Effect.succeed({
              ...(bare === undefined ? {} : { bare }),
              runtimeMode: "approval-required",
              interactionMode: "default",
              cwd: "/workspace",
            }),
        }),
        Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
          open,
        }),
        Layer.mock(EventSink.EventSinkV2)({
          writeIfRunCurrent: ({ events }) =>
            Effect.sync(() => {
              for (const event of events)
                projection = ProjectionStore.applyToProjection(projection, event);
              return { committed: true, storedEvents: [] };
            }),
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getTurnStartContext: () => Effect.succeed({ ...projection, hasConversation: true }),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          getTurnStartHistory: () => Effect.succeed(projection.turnItems),
          getThreadRecords: (requestedThreadId) =>
            Effect.succeed(requestedThreadId === threadId ? projection : source),
        }),
        Layer.mock(RunExecution.RunExecutionServiceV2)({
          startRootRun: (input) =>
            input.session
              .startTurn({
                appThread: input.appThread,
                threadId,
                runId,
                runOrdinal: input.run.ordinal,
                attemptId,
                rootNodeId: input.rootNode.id,
                providerThread: input.providerThread,
                message: input.message,
                modelSelection: input.modelSelection,
                runtimePolicy: input.runtimePolicy,
                providerTurnOrdinal: input.providerTurnOrdinal,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new RunExecution.RunExecutionStartError({
                      commandId: input.commandId,
                      runId,
                      cause,
                    }),
                ),
              ),
        }),
      ),
    ),
  );
  return {
    delivered,
    additions,
    ensure,
    resume,
    open,
    contextmd,
    projection: () => projection,
    start: Effect.gen(function* () {
      yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).start({ threadId, runId });
    }).pipe(Effect.provide(layer)),
  };
}

it.effect.each([true, false])(
  "urgent restart preserves the selected bare contract, bare=%s",
  (bare) =>
    Effect.gen(function* () {
      const h = harness({ bare, steeringSender: ThreadId.make("sender:urgent-bare-control") });
      yield* h.start;
      expect(h.delivered).toHaveLength(1);
      const text = h.delivered[0]!.message.text;
      if (bare) {
        expect(text).toBe("NAMED_BODY\n\nRun the task.");
        for (const effect of Object.values(h.contextmd)) expect(effect).not.toHaveBeenCalled();
      } else {
        expect(text).toContain(
          "the agent in T3 thread sender:urgent-bare-control interrupted your previous turn",
        );
        expect(text).toContain("were interrupted, not refused");
        expect(text.endsWith("\n\nRun the task.")).toBe(true);
        expect(h.additions).not.toHaveBeenCalled();
      }
      expect(h.projection().messages[0]?.text).toBe("Run the task.");
    }),
);

it.effect.each([
  { accepted: false, rolledBack: false, text: "NAMED_BODY\n\nRun the task." },
  { accepted: true, rolledBack: false, text: "Run the task." },
  { accepted: true, rolledBack: true, text: "NAMED_BODY\n\nRun the task." },
])(
  "bare additions follow the surviving native carrier, accepted=$accepted rolledBack=$rolledBack",
  (scenario) =>
    Effect.gen(function* () {
      const h = harness(scenario);
      yield* h.start;
      expect(h.delivered.map((turn) => turn.message.text)).toEqual([scenario.text]);
      expect(h.delivered[0]?.runtimePolicy.runtimeMode).toBe("approval-required");
      expect(h.projection().messages[0]?.text).toBe("Run the task.");
      expect(h.additions).toHaveBeenCalledTimes(scenario.accepted && !scenario.rolledBack ? 0 : 1);
    }),
);

it.effect(
  "bare recovery without accepted history creates a new native session and composes additions at final delivery",
  () =>
    Effect.gen(function* () {
      const h = harness({ resumeFailure: true });
      yield* h.start;
      expect(h.resume).toHaveBeenCalledOnce();
      expect(h.ensure).toHaveBeenCalledOnce();
      expect(h.delivered.map((turn) => turn.message.text)).toEqual(["NAMED_BODY\n\nRun the task."]);
      expect(h.projection().contextHandoffs).toEqual([]);
    }),
);

it.effect("bare recovery with accepted history refuses a summary substitute", () =>
  Effect.gen(function* () {
    const h = harness({ accepted: true, resumeFailure: true });
    yield* h.start;
    expect(h.projection().runs.at(-1)?.status).toBe("failed");
    expect(h.projection().turnItems.at(-1)).toMatchObject({
      type: "error",
      failure: { message: expect.stringContaining("will not substitute a summary") },
    });
    expect(h.ensure).not.toHaveBeenCalled();
    expect(h.delivered).toEqual([]);
  }),
);

it.effect("bare retry resends only missed user text in order and keeps whitespace", () =>
  Effect.gen(function* () {
    const h = harness({ missed: "user" });
    yield* h.start;
    expect(h.delivered.map((turn) => turn.message.text)).toEqual([
      "NAMED_BODY\n\nMissed task exactly\n  keep spaces\n\nRun the task.",
    ]);
  }),
);

it.effect("bare retry of a two-attempt run excludes the earlier delivered attempt", () =>
  Effect.gen(function* () {
    const h = harness({ missed: "user", accepted: true, earlierDelivered: true });
    yield* h.start;
    expect(h.delivered.map((turn) => turn.message.text)).toEqual([
      "Missed task exactly\n  keep spaces\n\nRun the task.",
    ]);
    expect(h.additions).not.toHaveBeenCalled();
  }),
);

it.effect("bare retry refuses mixed missed history before adapter delivery", () =>
  Effect.gen(function* () {
    const h = harness({ missed: "mixed" });
    const error = yield* h.start.pipe(Effect.flip);
    expect(error.cause).toMatchObject({
      _tag: "RunExecutionStartError",
      cause: { cause: { reason: expect.stringContaining("missed history is not user text") } },
    });
    expect(h.delivered).toEqual([]);
  }),
);

it.effect.each(["before-carrier", "after-carrier"] as const)(
  "native fork %s inherits the correct bare carrier boundary",
  (fork) =>
    Effect.gen(function* () {
      const h = harness({ fork });
      yield* h.start;
      expect(h.delivered.map((turn) => turn.message.text)).toEqual([
        fork === "after-carrier" ? "Run the task." : "NAMED_BODY\n\nRun the task.",
      ]);
      for (const effect of Object.values(h.contextmd)) expect(effect).not.toHaveBeenCalled();
    }),
);

it.effect.each([false, true])(
  "bare Grok bypasses ContextMD lifecycle with named skill body, recovery=%s",
  (resumeFailure) =>
    Effect.gen(function* () {
      const h = harness({ driver: "grok", resumeFailure });
      yield* h.start;
      expect(h.open).toHaveBeenCalledOnce();
      expect(h.open.mock.calls[0]?.[0]).not.toHaveProperty("newSessionInstructions");
      expect(h.delivered.map((turn) => turn.message.text)).toEqual(["NAMED_BODY\n\nRun the task."]);
      for (const effect of Object.values(h.contextmd)) expect(effect).not.toHaveBeenCalled();
    }),
);

it.effect("normal Grok retains native instructions and lifecycle accounting", () =>
  Effect.gen(function* () {
    const h = harness({ driver: "grok", bare: false });
    yield* h.start;
    expect(h.open.mock.calls[0]?.[0].newSessionInstructions).toBe(
      "UNNAMED_CONTEXTMD_NATIVE_POINTER",
    );
    expect(h.delivered.map((turn) => turn.message.text)).toEqual(["Run the task."]);
    expect(h.contextmd.stageSessionInstructions).toHaveBeenCalledOnce();
    expect(h.contextmd.recordInstallations).toHaveBeenCalledOnce();
    expect(h.contextmd.attachSessionInstructions).toHaveBeenCalledOnce();
    expect(h.contextmd.releaseSessionInstructions).toHaveBeenCalledWith("contextmd-reservation");
    expect(h.additions).not.toHaveBeenCalled();
  }),
);

it.effect("normal native forks retain ContextMD accounting", () =>
  Effect.gen(function* () {
    const h = harness({ bare: false, fork: "after-carrier" });
    yield* h.start;
    expect(h.contextmd.recordNativeFork).toHaveBeenCalledOnce();
    expect(h.contextmd.recordNativeFork).toHaveBeenCalledWith(
      expect.objectContaining({
        source: expect.objectContaining({
          nativeThreadRef: expect.objectContaining({ nativeId: "source-native" }),
        }),
        target: expect.objectContaining({
          nativeThreadRef: expect.objectContaining({ nativeId: "bare-native" }),
        }),
      }),
    );
    expect(h.delivered.map((turn) => turn.message.text)).toEqual(["Run the task."]);
  }),
);
