import { historyResponseItems } from "@t3tools/provider-core/server/handoffBudget";
import {
  makeProviderTextDeltaCoalescer,
  type ProviderTextDeltaUpdate,
} from "./ProviderTextDeltaCoalescer.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  CheckpointId,
  CodexSettings,
  EnvironmentId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProjectId,
  type ProviderApprovalDecision,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as CodexError from "effect-codex-app-server/errors";
import type * as CodexSchema from "effect-codex-app-server/schema";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as FileSystem from "effect/FileSystem";
import { MCP_APP_OUTPUT_KEY, readMcpAppReference } from "@t3tools/shared/mcpApp";
import { resolveAttachmentPathById } from "../../attachmentStore.ts";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as PlatformError from "effect/PlatformError";
import * as Stdio from "effect/Stdio";
import * as Sink from "effect/Sink";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { readProviderReplayTranscript } from "@t3tools/provider-testing/replayTranscript";
import packageJson from "../../../package.json" with { type: "json" };
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EffectWorker from "../EffectWorker.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProviderReplayHarness from "../testkit/ProviderReplayHarness.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";
import { withCodexReplayChildMetadata } from "./CodexAdapterV2.testkit.ts";
import * as CodexAdapterV2Testkit from "./CodexAdapterV2.testkit.ts";

const decodeNativeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const replayTranscriptJson = Schema.fromJsonString(CodexReplay.CodexAppServerReplayTranscript);
const encodeReplayTranscriptJson = Schema.encodeEffect(replayTranscriptJson);
const decodeReplayTranscriptJson = Schema.decodeUnknownEffect(replayTranscriptJson);
const encodeStringJson = Schema.encodeEffect(Schema.fromJsonString(Schema.String));
const isProviderAdapterEventStreamError = Schema.is(
  ProviderAdapter.ProviderAdapterEventStreamError,
);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

describe("CodexAdapterV2 context usage", () => {
  it("uses the current context rather than cumulative processed tokens", () => {
    const usage = CodexAdapterV2.codexProviderTurnTokenUsage(
      {
        total: {
          totalTokens: 180_000,
          inputTokens: 160_000,
          cachedInputTokens: 20_000,
          outputTokens: 20_000,
          reasoningOutputTokens: 5_000,
        },
        last: {
          totalTokens: 50_000,
          inputTokens: 45_000,
          cachedInputTokens: 10_000,
          outputTokens: 5_000,
          reasoningOutputTokens: 1_000,
        },
        modelContextWindow: 200_000,
      },
      "2026-08-29T00:00:00.000Z",
    );

    assert.deepEqual(usage, {
      usedTokens: 50_000,
      maxTokens: 200_000,
      inputTokens: 45_000,
      cachedInputTokens: 10_000,
      outputTokens: 5_000,
      reasoningOutputTokens: 1_000,
      updatedAt: "2026-08-29T00:00:00.000Z",
    });
  });
});

describe("CodexAdapterV2 assistant message streaming", () => {
  it.effect("flushes buffered text synchronously before item and turn completion", () =>
    Effect.gen(function* () {
      const updates = yield* Ref.make<ReadonlyArray<ProviderTextDeltaUpdate>>([]);
      const coalescer = yield* makeProviderTextDeltaCoalescer({
        flushIntervalMs: 50,
        emit: (update) => Ref.update(updates, (current) => [...current, update]),
      });

      yield* coalescer.append({ turnId: "turn-1", itemId: "message-1", delta: "item final" });
      const completedText = yield* coalescer.complete({
        turnId: "turn-1",
        itemId: "message-1",
      });
      yield* coalescer.append({ turnId: "turn-1", itemId: "message-2", delta: "turn final" });
      yield* coalescer.flushTurn("turn-1");

      assert.equal(completedText, "item final");
      assert.deepEqual(yield* Ref.get(updates), [
        { turnId: "turn-1", itemId: "message-1", text: "item final", completed: true },
        { turnId: "turn-1", itemId: "message-2", text: "turn final", completed: true },
      ]);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("50 millis");
      yield* Effect.yieldNow;
      assert.equal((yield* Ref.get(updates)).length, 2);
    }),
  );
});

describe("CodexAdapterV2 runtime policy", () => {
  it.effect.each([undefined, false, true])(
    "selects native collaboration modes without prompt overrides with hasT3Mcp=%s",
    (hasT3Mcp) =>
      Effect.gen(function* () {
        for (const interactionMode of ["plan", "default"] as const) {
          for (const reasoningEffort of [undefined, "high"] as const) {
            const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
              nativeThreadId: "native-mode-switch",
              codexInput: [{ type: "text", text: "test" }],
              runtimePolicy: { runtimeMode: "full-access", interactionMode, cwd: null },
              modelSelection: {
                instanceId: ProviderInstanceId.make("codex"),
                model: "gpt-5.4",
                ...(reasoningEffort === undefined
                  ? {}
                  : { options: [{ id: "reasoningEffort", value: reasoningEffort }] }),
              },
              ...(hasT3Mcp === undefined ? {} : { hasT3Mcp }),
            });

            assert.deepEqual(params.collaborationMode, {
              mode: interactionMode,
              settings: {
                model: "gpt-5.4",
                ...(reasoningEffort === undefined ? {} : { reasoning_effort: reasoningEffort }),
              },
            });
            assert.equal(params.effort, reasoningEffort);
            assert.equal(params.additionalContext !== undefined, hasT3Mcp === true);
          }
        }
      }),
  );

  it.effect("derives concrete Codex turn policies from every T3 runtime mode", () =>
    Effect.gen(function* () {
      const build = (
        runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access",
      ) =>
        CodexAdapterV2.buildCodexTurnStartParams({
          nativeThreadId: `native-${runtimeMode}`,
          codexInput: [{ type: "text", text: "test" }],
          runtimePolicy: {
            runtimeMode,
            interactionMode: "default",
            cwd: null,
          },
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5.4",
          },
        });

      const approvalRequired = yield* build("approval-required");
      const autoAcceptEdits = yield* build("auto-accept-edits");
      const auto = yield* build("auto");
      const fullAccess = yield* build("full-access");

      assert.equal(approvalRequired.approvalPolicy, "untrusted");
      assert.equal(approvalRequired.approvalsReviewer, "user");
      assert.equal(approvalRequired.sandboxPolicy?.type, "readOnly");
      assert.equal(autoAcceptEdits.approvalPolicy, "on-request");
      assert.equal(autoAcceptEdits.approvalsReviewer, "user");
      assert.equal(autoAcceptEdits.sandboxPolicy?.type, "workspaceWrite");
      assert.equal(auto.approvalPolicy, "on-request");
      assert.equal(auto.approvalsReviewer, "auto_review");
      assert.equal(auto.sandboxPolicy?.type, "workspaceWrite");
      assert.equal(fullAccess.approvalPolicy, "never");
      assert.equal(fullAccess.approvalsReviewer, "user");
      assert.equal(fullAccess.sandboxPolicy?.type, "dangerFullAccess");
    }),
  );

  it.effect("sends MCP app model context as untrusted Codex context", () =>
    Effect.gen(function* () {
      const policy = { runtimeMode: "full-access", interactionMode: "default", cwd: null } as const;
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
      const appContext = [{ key: "mcp_app_todos_list_todos_item-1", text: "Filtered to overdue" }];
      const alone = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-app-context",
        codexInput: [{ type: "text", text: "what's on my list?" }],
        runtimePolicy: policy,
        modelSelection,
        appContext,
      });
      assert.deepEqual(alone.additionalContext, {
        "mcp_app_todos_list_todos_item-1": { kind: "untrusted", value: "Filtered to overdue" },
      });
      const withT3 = yield* CodexAdapterV2.buildCodexTurnStartParams({
        nativeThreadId: "native-app-context",
        codexInput: [{ type: "text", text: "what's on my list?" }],
        runtimePolicy: policy,
        modelSelection,
        hasT3Mcp: true,
        appContext,
      });
      assert.deepEqual(withT3.additionalContext?.["mcp_app_todos_list_todos_item-1"], {
        kind: "untrusted",
        value: "Filtered to overdue",
      });
      assert.isDefined(withT3.additionalContext?.["t3_code_runtime"]);
    }),
  );
});

describe("CodexAdapterV2 native protocol logging", () => {
  it.effect("logs decoded app-server frames once with credentials redacted", () =>
    Effect.gen(function* () {
      const writes: Array<{
        readonly event: unknown;
        readonly threadId: ThreadId | null;
      }> = [];
      const logger: ProviderEventLoggers.EventNdjsonLogger = {
        filePath: "/tmp/events.log",
        write: (event, threadId) =>
          Effect.sync(() => {
            writes.push({ event, threadId });
          }),
        close: () => Effect.void,
      };
      const threadId = ThreadId.make("thread-1");
      const providerSessionId = ProviderSessionId.make("provider-session-1");
      const protocolLogger = CodexAdapterV2.makeCodexAppServerProtocolLogger({
        nativeEventLogger: logger,
        threadId,
        providerSessionId,
      });

      assert.notEqual(protocolLogger, undefined);
      if (protocolLogger === undefined) {
        return;
      }

      yield* protocolLogger({
        direction: "incoming",
        stage: "decoded",
        payload: {
          method: "thread/event",
          params: {
            id: "evt-1",
            http_headers: { Authorization: "Bearer secret-codex-token" },
            usage: { inputTokens: 12, outputTokens: 8, totalTokens: 20 },
          },
        },
      });
      yield* protocolLogger({
        direction: "incoming",
        stage: "raw",
        payload:
          '{"method":"thread/event","params":{"http_headers":{"Authorization":"Bearer secret-codex-token"}}}\n',
      });

      assert.equal(writes.length, 1);
      assert.equal(writes[0]?.threadId, threadId);
      assert.deepEqual(writes[0]?.event, {
        provider: "codex",
        protocol: "codex.app-server",
        kind: "protocol",
        providerSessionId,
        event: {
          direction: "incoming",
          stage: "decoded",
          payload: {
            method: "thread/event",
            params: {
              id: "evt-1",
              http_headers: { Authorization: "[REDACTED]" },
              usage: { inputTokens: 12, outputTokens: 8, totalTokens: 20 },
            },
          },
        },
      });
    }),
  );
});

describe("CodexAdapterV2 rollback mapping", () => {
  it.effect("derives native rollback count from durable provider turns", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const providerThreadId = ProviderThreadId.make("provider-thread-codex-rollback");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerSessionId: ProviderSessionId.make("provider-session-codex-rollback"),
        appThreadId: ThreadId.make("thread-codex-rollback"),
        ownerNodeId: null,
        nativeThreadRef: {
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeId: "native-thread-codex-rollback",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 3,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const providerTurn = (
        id: string,
        ordinal: number,
        status: OrchestrationV2ProviderTurn["status"],
      ): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(id),
        providerThreadId,
        nodeId: NodeId.make(`node-${id}`),
        runAttemptId: RunAttemptId.make(`run-attempt-${id}`),
        nativeTurnRef: {
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeId: `native-${id}`,
          strength: "strong",
        },
        ordinal,
        status,
        startedAt: now,
        completedAt: status === "running" || status === "pending" ? null : now,
      });
      const firstTurn = providerTurn("provider-turn-first", 1, "completed");
      const secondTurn = providerTurn("provider-turn-second", 2, "completed");
      const runningTurn = providerTurn("provider-turn-running", 3, "running");
      const interruptedTurn = providerTurn("provider-turn-interrupted", 4, "interrupted");
      const goalCommandTurn = {
        ...providerTurn("provider-turn-goal-command", 5, "completed"),
        nativeTurnRef: null,
      };

      const numTurns = yield* CodexAdapterV2.resolveCodexRollbackTurnCount({
        providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("checkpoint-first"),
          appRunOrdinal: 1,
          providerTurn: firstTurn,
        },
        providerThreadTurns: [goalCommandTurn, interruptedTurn, runningTurn, secondTurn, firstTurn],
      });

      assert.equal(numTurns, 2);
    }),
  );
});

describe("CodexAdapterV2 fork boundary", () => {
  const providerThreadId = ProviderThreadId.make("provider-thread-codex-fork-boundary");
  const makeProviderThread = (now: DateTime.Utc): OrchestrationV2ProviderThread => ({
    id: providerThreadId,
    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: ProviderSessionId.make("provider-session-codex-fork-boundary"),
    appThreadId: ThreadId.make("thread-codex-fork-boundary"),
    ownerNodeId: null,
    nativeThreadRef: {
      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
      nativeId: "native-thread-codex-fork-boundary",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: 1,
    lastRunOrdinal: 2,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  });
  const makeProviderTurn = (
    id: string,
    ordinal: number,
    nativeId: string | null,
    now: DateTime.Utc,
  ): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(id),
    providerThreadId,
    nodeId: NodeId.make(`node-${id}`),
    runAttemptId: RunAttemptId.make(`run-attempt-${id}`),
    nativeTurnRef:
      nativeId === null
        ? { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId: null, strength: "none" }
        : { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId, strength: "strong" },
    ordinal,
    status: "completed",
    startedAt: now,
    completedAt: now,
  });

  it.effect("resolves the selected provider turn to an inclusive native fork boundary", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, "native-turn-first", now);
      const secondTurn = makeProviderTurn("provider-turn-second", 2, "native-turn-second", now);

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        sourceProviderTurns: [firstTurn, secondTurn],
        providerTurnId: firstTurn.id,
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, {
        lastTurnId: "native-turn-first",
        rollbackTurnCount: 0,
      });
    }),
  );

  it.effect("keeps the rollback-count fallback when the boundary turn lacks a native id", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const firstTurn = makeProviderTurn("provider-turn-first", 1, null, now);
      const secondTurn = makeProviderTurn("provider-turn-second", 2, "native-turn-second", now);

      const boundary = yield* CodexAdapterV2.resolveCodexForkBoundary({
        sourceProviderThread: makeProviderThread(now),
        sourceProviderTurns: [firstTurn, secondTurn],
        providerTurnId: firstTurn.id,
        targetThreadId: ThreadId.make("thread-codex-fork-target"),
      });

      assert.deepEqual(boundary, { lastTurnId: undefined, rollbackTurnCount: 1 });
    }),
  );
});

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});
const decodeCodexSettings = Schema.decodeEffect(CodexSettings);
const CODEX_TEST_MODEL_SELECTION = {
  instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_TEST_RUNTIME_POLICY = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: "/workspace",
});

function makeCodexTestAppThread(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: input.threadId,
    projectId: ProjectId.make(`project-${input.threadId}`),
    title: "Codex continuation test",
    providerInstanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
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
  };
}

function makeCodexTestTurnInput(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
  readonly attemptId: RunAttemptId;
  readonly text: string;
}): ProviderAdapter.ProviderAdapterV2TurnInput {
  return {
    appThread: makeCodexTestAppThread(input),
    threadId: input.threadId,
    runId: RunId.make(`run-${input.attemptId}`),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: input.attemptId,
    rootNodeId: NodeId.make(`node-${input.attemptId}`),
    providerThread: input.providerThread,
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make(`message-${input.attemptId}`),
      text: input.text,
      attachments: [],
    },
    modelSelection: CODEX_TEST_MODEL_SELECTION,
    runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
  };
}

function makeCodexReplayTurn(input: {
  readonly id: string;
  readonly status: "inProgress" | "completed" | "interrupted" | "failed";
}): Record<string, unknown> {
  const terminal =
    input.status === "completed" || input.status === "interrupted" || input.status === "failed";
  return {
    id: input.id,
    items: [],
    itemsView: "notLoaded",
    status: input.status,
    error: null,
    startedAt: 1782622440,
    completedAt: terminal ? 1782622450 : null,
    durationMs: null,
  };
}

function codexReplayPreamble(input: {
  readonly nativeThreadId: string;
  readonly nativeTurnId: string;
  readonly prompt: string;
  readonly sentPrompt?: string;
}): Array<CodexReplay.CodexAppServerReplayEntry> {
  return [
    {
      type: "expect_outbound",
      label: "initialize",
      frame: {
        id: 1,
        method: "initialize",
        params: {
          clientInfo: { name: "T3 Code", title: "T3 Code", version: packageJson.version },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: ["turn/diff/updated"],
            extensions: {
              "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
            },
          },
        },
      },
    },
    {
      type: "emit_inbound",
      label: "initialize",
      frame: {
        id: 1,
        result: {
          userAgent: "T3 Code/0.156.1",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        },
      },
    },
    { type: "expect_outbound", label: "initialized", frame: { method: "initialized" } },
    {
      type: "expect_outbound",
      label: "thread/start",
      frame: {
        id: 2,
        method: "thread/start",
        params: { config: CodexAdapterV2.CODEX_THREAD_CONFIG },
      },
    },
    {
      type: "emit_inbound",
      label: "thread/start",
      frame: {
        id: 2,
        result: {
          thread: {
            id: input.nativeThreadId,
            sessionId: input.nativeThreadId,
            projectId: null,
            forkedFromId: null,
            preview: "",
            ephemeral: false,
            modelProvider: "openai",
            createdAt: 1782622440,
            updatedAt: 1782622440,
            status: { type: "idle" },
            path: `/tmp/${input.nativeThreadId}.jsonl`,
            cwd: "/workspace",
            cliVersion: "0.144.0",
            source: "vscode",
            threadSource: null,
            agentNickname: null,
            agentRole: null,
            gitInfo: null,
            name: null,
            turns: [],
          },
          model: "gpt-5.4",
          modelProvider: "openai",
          serviceTier: null,
          cwd: "/workspace",
          instructionSources: [],
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
          reasoningEffort: "medium",
        },
      },
    },
    {
      type: "expect_outbound",
      label: "turn/start",
      frame: {
        id: 3,
        method: "turn/start",
        params: {
          threadId: input.nativeThreadId,
          input: [{ type: "text", text: input.sentPrompt ?? input.prompt }],
          cwd: "/workspace",
          model: "gpt-5.4",
          approvalPolicy: "never",
          approvalsReviewer: "user",
          sandboxPolicy: { type: "dangerFullAccess" },
          summary: "detailed",
          collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
        },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/start",
      frame: {
        id: 3,
        result: { turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }) },
      },
    },
    {
      type: "emit_inbound",
      label: "turn/started",
      frame: {
        method: "turn/started",
        params: {
          threadId: input.nativeThreadId,
          turn: makeCodexReplayTurn({ id: input.nativeTurnId, status: "inProgress" }),
        },
      },
    },
  ];
}

function makeCodexReplayTranscript(input: {
  readonly scenario: string;
  readonly entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>;
}): CodexReplay.CodexAppServerReplayTranscript {
  return {
    provider: "codex",
    protocol: "codex.app-server",
    version: "0.144.0",
    scenario: input.scenario,
    entries: input.entries,
  };
}

function withReplayRequestId(
  entry: CodexReplay.CodexAppServerReplayEntry,
  id: number,
): CodexReplay.CodexAppServerReplayEntry {
  return entry.type !== "runtime_exit" && Predicate.isObject(entry.frame)
    ? { ...entry, frame: { ...entry.frame, id } }
    : entry;
}

describe("CodexAdapterV2 process exit", () => {
  it.effect.each([
    { exit: "numeric", active: false },
    { exit: "SIGKILL", active: false },
    { exit: "numeric", active: true },
    { exit: "SIGKILL", active: true },
  ] as const)("retires the shared session after $exit (active turn: $active)", ({ exit, active }) =>
    Effect.gen(function* () {
      const scenario = `process-exit-${exit}-${active}`;
      const nativeThreadId = `native-${scenario}`;
      const nativeTurnId = `turn-${scenario}`;
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const emit = (frame: unknown) =>
        Queue.offer(input, new TextEncoder().encode(JSON.stringify(frame) + "\n"));
      const outputEnded = yield* Deferred.make<void>();
      const turnStarted = yield* Deferred.make<void>();
      const turnCompleted = yield* Deferred.make<void>();
      const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      const preamble = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "work" });
      const terminationError =
        exit === "numeric"
          ? new CodexError.CodexAppServerProcessExitedError({ code: 17, pid: 52 })
          : new CodexError.CodexAppServerTransportError({
              operation: "read-process-exit-status",
              pid: 52,
              cause: PlatformError.systemError({
                _tag: "Unknown",
                module: "ChildProcess",
                method: "exitCode",
                cause: new Error("Process terminated by SIGKILL"),
              }),
            });
      const client = yield* CodexClient.make(
        Stdio.make({
          args: Effect.succeed([]),
          stdin: Stream.fromQueue(input),
          stdout: () =>
            Sink.forEach((chunk: string | Uint8Array) =>
              Effect.gen(function* () {
                const frame = JSON.parse(
                  typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk),
                ) as { readonly id?: number; readonly method: string };
                const response = preamble.find(
                  (entry) =>
                    entry.type === "emit_inbound" &&
                    entry.label === frame.method &&
                    Predicate.isObject(entry.frame) &&
                    "id" in entry.frame,
                );
                if (response?.type === "emit_inbound") {
                  yield* emit(response.frame);
                }
              }),
            ).pipe(Sink.ensuring(Deferred.succeed(outputEnded, undefined))),
          stderr: () => Sink.drain,
        }),
        {},
        Effect.succeed(terminationError),
      );
      const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        clientFactory: { open: () => Effect.succeed(client) },
        crypto: yield* Crypto.Crypto,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ProviderReplayHarness.makeReplayServerConfig(scenario).pipe(
          Effect.orDie,
        ),
      });
      const threadId = ThreadId.make(`thread-${scenario}`);
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make(`session-${scenario}`),
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      const eventReader = yield* runtime.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => events.push(event)).pipe(
            Effect.andThen(
              event.type === "provider_turn.updated" && event.providerTurn.status === "running"
                ? Deferred.succeed(turnStarted, undefined)
                : event.type === "turn.terminal"
                  ? Deferred.succeed(turnCompleted, undefined)
                  : Effect.void,
            ),
          ),
        ),
        Effect.forkScoped,
      );
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      yield* runtime.startTurn(
        makeCodexTestTurnInput({
          threadId,
          providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make(`attempt-${scenario}`),
          text: "work",
        }),
      );
      yield* emit({
        method: "turn/started",
        params: {
          threadId: nativeThreadId,
          turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
        },
      });
      yield* Deferred.await(turnStarted);
      if (!active) {
        yield* emit({
          method: "turn/completed",
          params: {
            threadId: nativeThreadId,
            turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
          },
        });
        yield* Deferred.await(turnCompleted);
        assert.isTrue(
          events.some((event) => event.type === "turn.terminal" && event.status === "completed"),
        );
      }

      yield* Queue.end(input);
      // Output closes only after the protocol records the process termination.
      yield* Deferred.await(outputEnded);
      const rpcError = yield* client.request("thread/list", {}).pipe(Effect.flip);
      assert.strictEqual(rpcError, terminationError);
      // Without the fix the stream never ends, so this waits until the project timeout.
      const readerExit = yield* Fiber.await(eventReader);
      // A failure, not a clean end, carries the Codex error into the session's lastError.
      assert.isTrue(
        Exit.isFailure(readerExit) &&
          isProviderAdapterEventStreamError(Cause.squash(readerExit.cause)),
        "the session manager must receive process death so it can retire the shared process",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );
});

describe("CodexAdapterV2 session initialize", () => {
  const openReplaySession = (
    transcript: CodexReplay.CodexAppServerReplayTranscript,
    beforeEmitInbound?: CodexReplay.CodexAppServerReplayDriver["beforeEmitInbound"],
  ) =>
    Effect.gen(function* () {
      const driver = yield* CodexReplay.makeReplayDriver(
        transcript,
        beforeEmitInbound === undefined ? {} : { beforeEmitInbound },
      );
      let initializeRequests = 0;
      const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        clientFactory: {
          open: (openInput) =>
            Layer.build(CodexReplay.layerReplayWithDriver(driver)).pipe(
              Effect.flatMap((context) =>
                Effect.service(CodexClient.CodexAppServerClient).pipe(Effect.provide(context)),
              ),
              Effect.map(
                (client) =>
                  ({
                    ...client,
                    request: (method, params) =>
                      Effect.sync(() => {
                        if (method === "initialize") initializeRequests++;
                      }).pipe(Effect.andThen(client.request(method, params))),
                  }) satisfies CodexClient.CodexAppServerClient["Service"],
              ),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterOpenSessionError({
                    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                    providerSessionId: openInput.providerSessionId,
                    cause,
                  }),
              ),
            ),
        },
        crypto: yield* Crypto.Crypto,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ProviderReplayHarness.makeReplayServerConfig(transcript.scenario).pipe(
          Effect.orDie,
        ),
      });
      const runtime = yield* adapter.openSession({
        threadId: ThreadId.make(`thread-${transcript.scenario}`),
        providerSessionId: ProviderSessionId.make(`provider-session-${transcript.scenario}`),
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      return {
        ensureThread: (threadId: string) =>
          runtime.ensureThread({
            threadId: ThreadId.make(threadId),
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          }),
        initializeRequests: () => initializeRequests,
      };
    });

  const replayPreamble = (nativeThreadId: string) =>
    codexReplayPreamble({ nativeThreadId, nativeTurnId: "unused", prompt: "unused" });

  it.effect("sends one initialize when two threads start on a fresh session at once", () =>
    Effect.gen(function* () {
      const initializeAwaitingResponse = yield* Deferred.make<void>();
      const releaseInitialize = yield* Deferred.make<void>();
      // Codex rejects a second initialize; the replay permits one handshake.
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({
          scenario: "concurrent-initialize",
          entries: [
            ...replayPreamble("concurrent-first").slice(0, 5),
            ...replayPreamble("concurrent-second")
              .slice(3, 5)
              .map((entry) => withReplayRequestId(entry, 3)),
          ],
        }),
        (entry) =>
          entry.label === "initialize"
            ? Deferred.succeed(initializeAwaitingResponse, undefined).pipe(
                Effect.andThen(Deferred.await(releaseInitialize)),
              )
            : Effect.void,
      );

      const first = yield* session
        .ensureThread("thread-concurrent-first")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(initializeAwaitingResponse);
      const second = yield* session
        .ensureThread("thread-concurrent-second")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseInitialize, undefined);

      const providerThreads = [yield* Fiber.join(first), yield* Fiber.join(second)];
      assert.equal(session.initializeRequests(), 1);
      assert.sameMembers(
        providerThreads.map((providerThread) => providerThread.nativeThreadRef?.nativeId),
        ["concurrent-first", "concurrent-second"],
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  // regression: #16260 one-shot initializers raced; a failed handshake must be retried
  it.effect("retries initialize after a failed handshake", () =>
    Effect.gen(function* () {
      const preamble = replayPreamble("initialize-retry");
      const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
        ...preamble.slice(0, 1),
        {
          type: "emit_inbound",
          label: "initialize",
          frame: { id: 1, error: { code: -32603, message: "Codex is not ready." } },
        },
        ...preamble.slice(0, 2).map((entry) => withReplayRequestId(entry, 2)),
        ...preamble.slice(2, 3),
        ...preamble.slice(3, 5).map((entry) => withReplayRequestId(entry, 3)),
      ];
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({ scenario: "initialize-retry", entries }),
      );

      const failure = yield* session.ensureThread("thread-initialize-retry").pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterEnsureThreadError");
      const providerThread = yield* session.ensureThread("thread-initialize-retry");
      assert.equal(providerThread.nativeThreadRef?.nativeId, "initialize-retry");
      assert.equal(session.initializeRequests(), 2);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect("completes the handshake after a caller is interrupted mid-initialize", () =>
    Effect.gen(function* () {
      const initializeAwaitingResponse = yield* Deferred.make<void>();
      const releaseInitialize = yield* Deferred.make<void>();
      const preamble = replayPreamble("initialize-interrupted");
      const session = yield* openReplaySession(
        makeCodexReplayTranscript({
          scenario: "initialize-interrupted",
          entries: [
            ...preamble.slice(0, 2),
            ...preamble.slice(0, 1).map((entry) => withReplayRequestId(entry, 2)),
            {
              type: "emit_inbound",
              label: "initialize-rejected",
              frame: { id: 2, error: { code: -32600, message: "Already initialized" } },
            },
            ...preamble.slice(2, 3),
            ...preamble.slice(3, 5).map((entry) => withReplayRequestId(entry, 3)),
          ],
        }),
        (entry) =>
          entry.label === "initialize"
            ? Deferred.succeed(initializeAwaitingResponse, undefined).pipe(
                Effect.andThen(Deferred.await(releaseInitialize)),
              )
            : Effect.void,
      );

      const interrupted = yield* session
        .ensureThread("thread-initialize-interrupted")
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(initializeAwaitingResponse);
      yield* Fiber.interrupt(interrupted);
      yield* Deferred.succeed(releaseInitialize, undefined);

      const providerThread = yield* session.ensureThread("thread-initialize-interrupted");
      assert.equal(providerThread.nativeThreadRef?.nativeId, "initialize-interrupted");
      assert.equal(session.initializeRequests(), 2);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );
});

describe("CodexAdapterV2 post-settle continuation", () => {
  const awaitUntil = (predicate: () => boolean, label: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      for (let attempt = 0; attempt < 5000; attempt++) {
        if (predicate()) {
          return;
        }
        yield* Effect.yieldNow;
      }
      return yield* Effect.die(`Timed out waiting for ${label}.`);
    });

  // The harness notifies after every recorded event or continuation, so a wait
  // re-checks exactly when the state it reads changes. Subscribing before the
  // first check means a change that lands in between is not lost.
  const makeChangeSignal = Effect.gen(function* () {
    const changes = yield* PubSub.unbounded<void>();
    const until = (predicate: () => boolean): Effect.Effect<void> =>
      Effect.scoped(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          while (!predicate()) yield* PubSub.take(subscription);
        }),
      );
    return { notify: PubSub.publish(changes, undefined), until };
  });

  const makeCodexReplayHarness = (
    transcript: CodexReplay.CodexAppServerReplayTranscript,
    onEvent: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<unknown> = () =>
      Effect.void,
    onRequest: (method: string, params: unknown) => Effect.Effect<void> = () => Effect.void,
    readChildMetadata?: Parameters<typeof withCodexReplayChildMetadata>[2],
  ) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const serverConfig = yield* ProviderReplayHarness.makeReplayServerConfig(
        transcript.scenario,
      ).pipe(Effect.orDie);
      const changes = yield* makeChangeSignal;
      const continuationRequests: Array<ProviderContinuationRequests.ProviderContinuationRequest> =
        [];
      const clientFactory: CodexAdapterV2.CodexAppServerClientFactoryShape = {
        open: (openInput) =>
          Layer.build(CodexReplay.layerReplay(transcript)).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterOpenSessionError({
                  driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                  providerSessionId: openInput.providerSessionId,
                  cause,
                }),
            ),
            Effect.flatMap((context) =>
              Effect.service(CodexClient.CodexAppServerClient).pipe(
                Effect.map((client) =>
                  withCodexReplayChildMetadata(client, transcript, readChildMetadata),
                ),
                Effect.map(
                  (client) =>
                    ({
                      ...client,
                      request: (method, params) =>
                        onRequest(method, params).pipe(
                          Effect.andThen(client.request(method, params)),
                        ),
                    }) satisfies CodexClient.CodexAppServerClient["Service"],
                ),
                Effect.provide(context),
              ),
            ),
          ),
      };
      const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
        instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
        settings: DEFAULT_CODEX_SETTINGS,
        environment: {},
        clientFactory,
        crypto: yield* Crypto.Crypto,
        fileSystem,
        idAllocator,
        serverConfig,
        continuationRequests: {
          offer: (request) =>
            Effect.sync(() => {
              continuationRequests.push(request);
            }).pipe(Effect.andThen(changes.notify)),
        },
      });
      const threadId = ThreadId.make(`thread-${transcript.scenario}`);
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make(`provider-session-${transcript.scenario}`),
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection: CODEX_TEST_MODEL_SELECTION,
        runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
      });
      const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      const firstTerminal = yield* Deferred.make<void>();
      yield* runtime.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }).pipe(
            Effect.andThen(
              event.type === "turn.terminal"
                ? Deferred.succeed(firstTerminal, undefined)
                : Effect.void,
            ),
            Effect.andThen(changes.notify),
            Effect.andThen(onEvent(event)),
          ),
        ),
        Effect.forkScoped,
      );
      if (runtime.hasPendingBackgroundWork === undefined) {
        return yield* Effect.die("Codex adapter runtime must expose hasPendingBackgroundWork.");
      }
      const hasPendingBackgroundWork = runtime.hasPendingBackgroundWork;
      const terminalEvents = () =>
        events.filter(
          (
            event,
          ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        );
      const subagentUpdates = () =>
        events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "subagent.updated" }
          > => event.type === "subagent.updated",
        );
      return {
        runtime,
        providerThread,
        threadId,
        serverConfig,
        events,
        continuationRequests,
        terminalEvents,
        subagentUpdates,
        hasPendingBackgroundWork,
        firstTerminal: Deferred.await(firstTerminal),
        until: changes.until,
        awaitTerminal: (count: number) => changes.until(() => terminalEvents().length >= count),
      };
    });

  it.effect.each(["supported", "unsupported", "invalid"] as const)(
    "delivers native history with %s app-server protocol",
    (response) =>
      Effect.gen(function* () {
        const nativeThreadId = `inject-${response}`;
        const prompt = "Only the current request";
        const history: ProviderAdapter.ProviderAdapterV2HistoricalContext = {
          context: "Historical conversation",
          messages: (["user", "assistant"] as const).map((role) => ({
            role,
            text:
              role === "user"
                ? "Original request\n" + "界".repeat(300)
                : "Partial interrupted work",
            threadId: ThreadId.make("source"),
            runId: RunId.make("source-run"),
            itemId: TurnItemId.make(`source-${role}`),
            providerThreadId: null,
            kind: `${role}_message`,
            status: "interrupted",
          })),
        };
        const items = historyResponseItems(history.messages, history.context);
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "current-turn",
          prompt,
        });
        const transcript = makeCodexReplayTranscript({
          scenario: `inject-${response}`,
          entries: [
            ...preamble.slice(0, -3),
            {
              type: "expect_outbound",
              label: "inject",
              frame: {
                id: 3,
                method: "thread/inject_items",
                params: { threadId: nativeThreadId, items },
              },
            },
            {
              type: "emit_inbound",
              label: "inject-result",
              frame:
                response === "supported"
                  ? { id: 3, result: {} }
                  : {
                      id: 3,
                      error: {
                        code: response === "unsupported" ? -32601 : -32602,
                        message: "Injection rejected",
                      },
                    },
            },
            ...(response === "invalid"
              ? []
              : preamble
                  .slice(-3)
                  .map((entry) =>
                    "frame" in entry && Predicate.isObject(entry.frame) && "id" in entry.frame
                      ? { ...entry, frame: { ...entry.frame, id: 4 } }
                      : entry,
                  )),
          ],
        });
        const requests: string[] = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        const injection = yield* harness.runtime.injectHistory!({
          providerThread: harness.providerThread,
          ...history,
        }).pipe(Effect.result);
        if (response === "invalid") {
          assert.equal(injection._tag, "Failure");
          if (injection._tag === "Failure") {
            assert.equal(injection.failure._tag, "ProviderAdapterProtocolError");
            assert.propertyVal(injection.failure.cause, "code", -32602);
            assert.notProperty(injection.failure, "payload");
          }
          assert.notInclude(requests, "turn/start");
          return;
        }
        assert.equal(injection._tag, "Success");
        if (injection._tag === "Success") assert.equal(injection.success, response === "supported");
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("inject-attempt"),
            text: prompt,
          }),
        );
        assert.equal(requests.filter((method) => method === "turn/start").length, 1);
        assert.isBelow(requests.indexOf("thread/inject_items"), requests.indexOf("turn/start"));
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  it.effect("adds the session MCP server without replacing launch-argument servers", () =>
    Effect.gen(function* () {
      const scenario = "launch-mcp-servers";
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("environment-codex-mcp"),
        threadId: ThreadId.make(`thread-${scenario}`),
        providerSessionId: `provider-session-${scenario}`,
        providerInstanceId: ProviderInstanceId.make("codex"),
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer secret-codex-token",
        browserToolsAvailable: true,
      });
      const requests: Array<unknown> = [];
      yield* makeCodexReplayHarness(
        makeCodexReplayTranscript({
          scenario,
          entries: codexReplayPreamble({
            nativeThreadId: "launch-mcp-thread",
            nativeTurnId: "unused",
            prompt: "unused",
          }).slice(0, 5),
        }),
        () => Effect.void,
        (method, params) =>
          Effect.sync(() => {
            if (method === "thread/start") requests.push(params);
          }),
      );
      assert.deepEqual(requests, [
        {
          cwd: "/workspace",
          model: "gpt-5.4",
          config: {
            "tools.update_plan.enabled": true,
            mcp_optional_startup_grace_ms: 10_000,
            "mcp_servers.t3-code": {
              url: "http://127.0.0.1:43123/mcp",
              tool_timeout_sec: 3_900,
              http_headers: { Authorization: "Bearer secret-codex-token" },
            },
          },
        },
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  // regression: #15839 launch-arg MCP servers survive the thread's t3-code server
  it.effect("replays historical and dotted MCP config while enforcing other thread config", () =>
    Effect.gen(function* () {
      const recordedServer = {
        url: "http://127.0.0.1:41000/mcp",
        http_headers: { Authorization: "Bearer recorded-token" },
      };
      const currentConfig = {
        "tools.update_plan.enabled": true,
        "mcp_servers.t3-code": {
          url: "http://127.0.0.1:42000/mcp",
          http_headers: { Authorization: "Bearer current-token" },
        },
        "mcp_servers.extra.command": "cat",
      };
      for (const recordedMcp of [
        { mcp_servers: { "t3-code": recordedServer } },
        { "mcp_servers.t3-code": recordedServer, "mcp_servers.extra.command": "echo" },
      ]) {
        const entries = codexReplayPreamble({
          nativeThreadId: "replayed-mcp-thread",
          nativeTurnId: "unused",
          prompt: "unused",
        })
          .slice(3, 5)
          .map((entry) => withReplayRequestId(entry, 1));
        entries[0] = {
          type: "expect_outbound",
          frame: {
            id: 1,
            method: "thread/start",
            params: { config: { "tools.update_plan.enabled": true, ...recordedMcp } },
          },
        };
        entries.push({
          ...entries[0],
          frame: {
            id: 2,
            method: "thread/start",
            params: { config: { "tools.update_plan.enabled": true, ...recordedMcp } },
          },
        });
        const driver = yield* CodexReplay.makeReplayDriver(
          makeCodexReplayTranscript({
            scenario: "mcp-config-normalization",
            entries,
          }),
        );
        yield* Effect.gen(function* () {
          const client = yield* CodexClient.CodexAppServerClient;
          const response = yield* client.request("thread/start", { config: currentConfig });
          assert.equal(response.thread.id, "replayed-mcp-thread");
          assert.isNull((yield* Ref.get(driver.state)).failure);
          yield* client
            .request("thread/start", {
              config: { ...currentConfig, "tools.update_plan.enabled": false },
            })
            .pipe(Effect.flip);
          assert.instanceOf(
            (yield* Ref.get(driver.state)).failure,
            CodexReplay.CodexAppServerReplayFrameMismatchError,
          );
        }).pipe(Effect.provide(CodexReplay.layerReplayWithDriver(driver)));
      }
    }).pipe(Effect.scoped),
  );

  it.effect("waits for native start before interrupting an acknowledged queued turn", () =>
    Effect.gen(function* () {
      const nativeThreadId = "early-stop-thread";
      const nativeTurnId = "early-stop-turn";
      const prompt = "Run a command.";
      const preamble = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt });
      const transcript = makeCodexReplayTranscript({
        scenario: "early-stop-await-native-start",
        entries: [
          ...preamble.slice(0, -2),
          {
            type: "emit_inbound",
            label: "turn/start/queued",
            frame: {
              id: 3,
              result: {
                turn: {
                  ...makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
                  startedAt: null,
                },
              },
            },
          },
          {
            type: "emit_inbound",
            label: "turn/started",
            afterMs: 1000,
            frame: {
              method: "turn/started",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
              },
            },
          },
          {
            type: "expect_outbound",
            label: "turn/interrupt",
            frame: {
              id: 4,
              method: "turn/interrupt",
              params: { threadId: nativeThreadId, turnId: nativeTurnId },
            },
          },
          { type: "emit_inbound", label: "turn/interrupt", frame: { id: 4, result: {} } },
          {
            type: "emit_inbound",
            label: "turn/completed",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: makeCodexReplayTurn({ id: nativeTurnId, status: "interrupted" }),
              },
            },
          },
        ],
      });
      let interruptSent = false;
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            if (method === "turn/interrupt") interruptSent = true;
          }),
      );
      yield* harness.runtime.startTurn(
        makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make("early-stop-attempt"),
          text: prompt,
        }),
      );
      const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: CodexAdapterV2.CODEX_DRIVER_KIND,
        nativeTurnId,
      });
      const interrupt = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("500 millis");
      assert.isFalse(interruptSent, "Stop must not reach Codex before native turn/started");
      yield* TestClock.adjust("500 millis");
      yield* Fiber.join(interrupt);
      yield* harness.firstTerminal;
      assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
      assert.lengthOf(harness.terminalEvents(), 1);
      assert.isFalse(yield* harness.hasPendingBackgroundWork);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  const assistantMessages = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
    events.filter(
      (
        event,
      ): event is Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "message.updated" }> =>
        event.type === "message.updated" && event.message.role === "assistant",
    );

  it.effect("keeps an asynchronous Codex question actionable after the turn completes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "async-question-thread";
        const nativeTurnId = "async-question-turn";
        const usage = {
          totalTokens: 15,
          inputTokens: 10,
          cachedInputTokens: 2,
          outputTokens: 5,
          reasoningOutputTokens: 1,
        };
        const transcript = makeCodexReplayTranscript({
          scenario: "async-question-and-billed-usage",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Continue while I decide.",
            }),
            {
              type: "emit_inbound",
              label: "question",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  item: {
                    type: "agentMessage",
                    id: "async-question-item",
                    text: "Which branch?",
                    delivery: "async",
                    questions: [{ title: "Which branch?", options: ["main", "dev"] }],
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "usage",
              frame: {
                method: "thread/tokenUsage/updated",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  tokenUsage: { total: usage, last: usage, modelContextWindow: 200_000 },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "complete",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("async-question-attempt"),
            text: "Continue while I decide.",
          }),
        );
        yield* harness.firstTerminal;
        const requests = harness.events.flatMap((event) =>
          event.type === "runtime_request.updated" ? [event.runtimeRequest] : [],
        );
        assert.lengthOf(requests, 1);
        assert.equal(requests[0]?.status, "pending");
        assert.deepEqual(requests[0]?.responseCapability, { type: "message" });
        const questionItem = harness.events.find(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
        );
        assert.equal(questionItem?.type, "turn_item.updated");
        if (
          questionItem?.type === "turn_item.updated" &&
          questionItem.turnItem.type === "user_input_request"
        ) {
          assert.deepEqual(
            questionItem.turnItem.questions[0]?.options.map((option) => option.label),
            ["main", "dev"],
          );
          assert.equal(questionItem.turnItem.responseMode, "message");
        }
        const questionNode = harness.events.find(
          (event) => event.type === "node.updated" && event.node.id === requests[0]?.nodeId,
        );
        assert.equal(
          questionNode?.type === "node.updated" && questionNode.node.countsForRun,
          false,
        );
        const contextReport = harness.events.find(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.tokenUsage !== undefined,
        );
        assert.equal(contextReport?.type, "provider_turn.updated");
        if (contextReport?.type === "provider_turn.updated") {
          assert.equal(
            contextReport.providerTurn.runAttemptId,
            RunAttemptId.make("async-question-attempt"),
          );
          assert.equal(contextReport.providerTurn.providerThreadId, harness.providerThread.id);
          assert.equal(contextReport.providerTurn.tokenUsage?.usedTokens, 15);
          assert.equal(contextReport.providerTurn.tokenUsage?.maxTokens, 200_000);
        }
        const completed = harness.events.find(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
        );
        assert.equal(completed?.type, "provider_turn.updated");
        if (completed?.type === "provider_turn.updated") {
          assert.deepEqual(completed.providerTurn.turnTokenUsage, {
            usageStatus: "complete",
            usageScope: "main_agent",
            hasSubagents: false,
            inputTokens: 10,
            cachedInputTokens: 2,
            outputTokens: 5,
            reasoningTokens: 1,
          });
        }
        assert.isEmpty(assistantMessages(harness.events));
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("stops asking for commands the user allowed for the session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "session-approval-thread";
        const turnIds = [
          "session-approval-turn-1",
          "session-approval-turn-2",
          "session-approval-turn-3",
        ];
        const turnPolicies = {
          "approval-required": {
            approvalPolicy: "untrusted",
            approvalsReviewer: "user",
            sandboxPolicy: { type: "readOnly" },
          },
          "full-access": {
            approvalPolicy: "never",
            approvalsReviewer: "user",
            sandboxPolicy: { type: "dangerFullAccess" },
          },
        } as const;
        const turnModes = ["approval-required", "full-access", "approval-required"] as const;
        const turnText = (index: number) => `Review round ${index + 1}.`;
        const startTurn = (index: number): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => [
          {
            type: "expect_outbound",
            label: `turn ${index + 1} turn/start`,
            frame: {
              id: 3 + index,
              method: "turn/start",
              params: {
                threadId: nativeThreadId,
                input: [{ type: "text", text: turnText(index) }],
                cwd: "/workspace",
                model: "gpt-5.4",
                ...turnPolicies[turnModes[index]!],
                summary: "detailed",
                collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
              },
            },
          },
          {
            type: "emit_inbound",
            label: `turn ${index + 1} turn/start`,
            frame: {
              id: 3 + index,
              result: { turn: makeCodexReplayTurn({ id: turnIds[index]!, status: "inProgress" }) },
            },
          },
          turnEvent("turn/started", turnIds[index]!),
        ];
        const approval = (input: {
          readonly id: number;
          readonly turnId: string;
          readonly kind: "commandExecution" | "fileChange";
          readonly itemId: string;
          readonly decision: string;
        }): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => [
          {
            type: "emit_inbound",
            label: `${input.itemId} approval`,
            frame: {
              id: input.id,
              method: `item/${input.kind}/requestApproval`,
              params: {
                threadId: nativeThreadId,
                turnId: input.turnId,
                itemId: input.itemId,
                startedAtMs: 1782622441000,
                ...(input.kind === "commandExecution" ? { command: `cat ${input.itemId}` } : {}),
              },
            },
          },
          {
            type: "expect_outbound",
            label: `${input.itemId} decision`,
            frame: { id: input.id, result: { decision: input.decision } },
          },
        ];
        const turnEvent = (
          method: "turn/started" | "turn/completed",
          turnId: string,
        ): CodexReplay.CodexAppServerReplayEntry => ({
          type: "emit_inbound",
          label: `${turnId} ${method}`,
          frame: {
            method,
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({
                id: turnId,
                status: method === "turn/started" ? "inProgress" : "completed",
              }),
            },
          },
        });
        const [patchRequest, patchDecision] = approval({
          id: 102,
          turnId: turnIds[0]!,
          kind: "fileChange",
          itemId: "first-patch",
          decision: "acceptForSession",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-session-approvals",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId: turnIds[0]!,
              prompt: turnText(0),
            }).slice(0, -3),
            ...startTurn(0),
            ...approval({
              id: 100,
              turnId: turnIds[0]!,
              kind: "commandExecution",
              itemId: "first-command",
              decision: "acceptForSession",
            }),
            patchRequest!,
            ...approval({
              id: 101,
              turnId: turnIds[0]!,
              kind: "commandExecution",
              itemId: "second-command",
              decision: "acceptForSession",
            }),
            turnEvent("turn/completed", turnIds[0]!),
            ...startTurn(1),
            patchDecision!,
            turnEvent("turn/completed", turnIds[1]!),
            ...startTurn(2),
            ...approval({
              id: 103,
              turnId: turnIds[2]!,
              kind: "commandExecution",
              itemId: "third-command",
              decision: "accept",
            }),
            ...approval({
              id: 104,
              turnId: turnIds[2]!,
              kind: "fileChange",
              itemId: "second-patch",
              decision: "accept",
            }),
            turnEvent("turn/completed", turnIds[2]!),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const requests = () => [
          ...new Map(
            harness.events.flatMap((event) =>
              event.type === "runtime_request.updated"
                ? [[event.runtimeRequest.id, event.runtimeRequest] as const]
                : [],
            ),
          ).values(),
        ];
        const answer = (
          count: number,
          kind: "command" | "file-change",
          decision: ProviderApprovalDecision,
        ) =>
          Effect.gen(function* () {
            yield* harness.until(() => requests().length === count);
            const request = requests()[count - 1]!;
            assert.equal(request.kind, kind);
            yield* harness.runtime.respondToRuntimeRequest({ requestId: request.id, decision });
          });

        const runTurn = (index: number) =>
          Effect.gen(function* () {
            yield* harness.runtime.startTurn({
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`session-approvals-${index + 1}`),
                text: turnText(index),
              }),
              runOrdinal: index + 1,
              providerTurnOrdinal: index + 1,
              runtimePolicy: { ...CODEX_TEST_RUNTIME_POLICY, runtimeMode: turnModes[index]! },
            });
          });

        yield* runTurn(0);
        yield* answer(1, "command", "acceptForSession");
        yield* harness.until(() => requests().length === 2);
        yield* harness.awaitTerminal(1);
        yield* runTurn(1);
        yield* answer(2, "file-change", "acceptForSession");
        yield* harness.awaitTerminal(2);
        yield* runTurn(2);
        yield* answer(3, "command", "accept");
        yield* answer(4, "file-change", "accept");
        yield* harness.awaitTerminal(3);
        assert.lengthOf(requests(), 4);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("preserves T3 context on the wire and restores it after compaction", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "context-thread";
        const nativeTurnId = "context-turn";
        const params = yield* CodexAdapterV2.buildCodexTurnStartParams({
          nativeThreadId,
          codexInput: [{ type: "text", text: "work" }],
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          hasT3Mcp: true,
        });
        assert.include(
          Object.entries(params.additionalContext ?? {})
            .filter(([key]) => key.startsWith("t3_code_orchestration_"))
            .map(([, entry]) => entry.value)
            .join("\n"),
          "delegate_task",
        );
        const entries = codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "work" });
        const transcript = makeCodexReplayTranscript({
          scenario: "restore-context",
          entries: [
            ...entries.slice(0, 5),
            {
              type: "expect_outbound",
              label: "context turn",
              frame: { id: 3, method: "turn/start", params },
            },
            ...entries.slice(6),
            {
              type: "emit_inbound",
              label: "compacted",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  item: { type: "contextCompaction", id: "compact-context" },
                },
              },
            },
            {
              type: "expect_outbound",
              label: "restore context",
              frame: {
                id: 4,
                method: "thread/inject_items",
                params: {
                  threadId: nativeThreadId,
                  items: Object.entries(params.additionalContext ?? {}).map(([key, entry]) => ({
                    type: "message",
                    role: "developer",
                    content: [{ type: "input_text", text: `<${key}>${entry.value}</${key}>` }],
                  })),
                },
              },
            },
            { type: "emit_inbound", label: "restored", frame: { id: 4, result: {} } },
            {
              type: "emit_inbound",
              label: "done",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* (yield* McpProviderSessions.McpProviderSessions).set({
          environmentId: EnvironmentId.make("test"),
          threadId: harness.threadId,
          providerSessionId: "context-session",
          providerInstanceId: ProviderInstanceId.make("codex"),
          endpoint: "http://127.0.0.1:43123/mcp",
          authorizationHeader: "Bearer test",
          browserToolsAvailable: true,
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("context-attempt"),
            text: "work",
          }),
        );
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("compacts Codex with the native RPC and completes the compaction turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "compact-thread";
        const nativeTurnId = "compact-turn";
        const item = { type: "contextCompaction", id: "compact-item" };
        const transcript = makeCodexReplayTranscript({
          scenario: "native-compaction",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "unused" }).slice(0, 5),
            {
              type: "expect_outbound",
              label: "compact",
              frame: {
                id: 3,
                method: "thread/compact/start",
                params: { threadId: nativeThreadId },
              },
            },
            { type: "emit_inbound", label: "compact", frame: { id: 3, result: {} } },
            {
              type: "emit_inbound",
              label: "start",
              frame: {
                method: "turn/started",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "inProgress" }),
                },
              },
            },
            ...(["item/started", "item/completed"] as const).map((method) => ({
              type: "emit_inbound" as const,
              label: method,
              frame: { method, params: { threadId: nativeThreadId, turnId: nativeTurnId, item } },
            })),
            {
              type: "emit_inbound",
              label: "complete",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        assert.isDefined(harness.runtime.compactThread);
        yield* harness.runtime.compactThread!(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("compact-attempt"),
            text: "/compact",
          }),
        );
        yield* harness.firstTerminal;
        const items = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "compaction"
            ? [event.turnItem]
            : [],
        );
        assert.deepEqual(
          items.map((entry) => entry.status),
          ["running", "completed"],
        );
        assert.equal(items[0]?.id, items[1]?.id);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each(["failed", "interrupted", "Stop"] as const)(
    "settles unfinished compaction on %s without changing completed compaction",
    (outcome) =>
      Effect.scoped(
        Effect.gen(function* () {
          const nativeThreadId = "compact-thread";
          const nativeTurnId = "compact-turn";
          const error = {
            message: "Remote compaction stream disconnected",
            codexErrorInfo: "other",
            additionalDetails: null,
          };
          const transcript = makeCodexReplayTranscript({
            scenario: `automatic-compaction-${outcome}`,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
              ...(
                [
                  { method: "item/started", id: "completed-item", startedAtMs: 1782622441000 },
                  { method: "item/completed", id: "completed-item", completedAtMs: 1782622442000 },
                  { method: "item/started", id: "unfinished-item", startedAtMs: 1782622443000 },
                ] as const
              ).map(({ method, id, ...timestamps }) => ({
                type: "emit_inbound" as const,
                label: `${method}/${id}`,
                frame: {
                  method,
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    item: { type: "contextCompaction", id },
                    ...timestamps,
                  },
                },
              })),
              ...(outcome === "Stop"
                ? [
                    {
                      type: "expect_outbound" as const,
                      label: "turn/interrupt",
                      frame: {
                        id: 4,
                        method: "turn/interrupt",
                        params: { threadId: nativeThreadId, turnId: nativeTurnId },
                      },
                    },
                    {
                      type: "emit_inbound" as const,
                      label: "turn/interrupt/failed",
                      frame: { id: 4, error: { code: -32_000, message: "Interrupt failed" } },
                    },
                  ]
                : [
                    {
                      type: "emit_inbound" as const,
                      label: "turn/completed",
                      frame: {
                        method: "turn/completed",
                        params: {
                          threadId: nativeThreadId,
                          turn: {
                            ...makeCodexReplayTurn({ id: nativeTurnId, status: outcome }),
                            error: outcome === "failed" ? error : null,
                          },
                        },
                      },
                    },
                  ]),
            ],
          });
          const compactionStarted = yield* Deferred.make<ProviderTurnId>();
          const harness = yield* makeCodexReplayHarness(transcript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "unfinished-item" &&
            event.turnItem.status === "running" &&
            event.turnItem.providerTurnId !== null
              ? Deferred.succeed(compactionStarted, event.turnItem.providerTurnId)
              : Effect.void,
          );
          yield* TestClock.setTime(1782622445000);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`compact-${outcome}-attempt`),
              text: "Continue.",
            }),
          );
          if (outcome === "Stop") {
            const providerTurnId = yield* Deferred.await(compactionStarted);
            const interrupt = yield* harness.runtime
              .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
              .pipe(Effect.exit);
            assert.equal(interrupt._tag, "Failure");
          }
          yield* harness.firstTerminal;
          const items = harness.events.flatMap((event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "compaction"
              ? [event.turnItem]
              : [],
          );
          const completed = items.filter(
            (item) => item.nativeItemRef?.nativeId === "completed-item",
          );
          const unfinished = items.filter(
            (item) => item.nativeItemRef?.nativeId === "unfinished-item",
          );
          const status = outcome === "failed" ? "failed" : "interrupted";
          assert.deepEqual(
            unfinished.map((item) => item.status),
            ["running", status],
          );
          assert.equal(unfinished[0]?.id, unfinished[1]?.id);
          assert.deepEqual(unfinished[1]?.startedAt, DateTime.makeUnsafe(1782622443000));
          assert.deepEqual(
            unfinished[1]?.completedAt,
            DateTime.makeUnsafe(outcome === "Stop" ? 1782622445000 : 1782622450000),
          );
          assert.deepEqual(
            completed.map((item) => item.status),
            ["running", "completed"],
          );
          assert.deepEqual(completed[1]?.startedAt, DateTime.makeUnsafe(1782622441000));
          assert.deepEqual(completed[1]?.completedAt, DateTime.makeUnsafe(1782622442000));
          assert.equal(harness.terminalEvents()[0]?.status, status);
          if (outcome === "failed") {
            assert.equal(harness.terminalEvents()[0]?.failure?.message, error.message);
          }
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("resumes a provider thread without requesting or decoding its history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-resume-metadata";
        const nativeThreadId = `native-${scenario}-thread`;
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "unused-turn",
          prompt: "unused-prompt",
        }).slice(0, 5);
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...preamble,
            {
              type: "expect_outbound",
              label: "thread/resume",
              frame: {
                id: 3,
                method: "thread/resume",
                params: {
                  threadId: nativeThreadId,
                  excludeTurns: true,
                  config: CodexAdapterV2.CODEX_THREAD_CONFIG,
                },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/resume",
              frame: { id: 3, result: { thread: { id: nativeThreadId, updatedAt: 1782622450 } } },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const resumed = yield* harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });

        assert.equal(resumed.nativeThreadRef?.nativeId, nativeThreadId);
        assert.equal(resumed.status, "idle");
        assert.equal(DateTime.toEpochMillis(resumed.updatedAt), 1782622450000);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  // regression: #15389 archived native Codex sessions resume via thread/unarchive
  it.effect.each(
    (
      [
        ["recovers archived-session errors", "session saved-thread is archived", 0, ""],
        ["preserves missing-thread errors", "thread not found", 3, "thread not found"],
        [
          "propagates unarchive missing-thread errors",
          "session saved-thread is archived",
          4,
          "thread not found",
        ],
        [
          "does not retry archived errors twice",
          "session saved-thread is archived",
          5,
          "session saved-thread is archived",
        ],
      ] as const
    ).map(([name, resumeError, failAt, finalError]) => ({ name, resumeError, failAt, finalError })),
  )("$name", ({ name, resumeError, failAt, finalError }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = "saved-thread";
        const params = {
          threadId: nativeThreadId,
          excludeTurns: true,
          cwd: CODEX_TEST_RUNTIME_POLICY.cwd,
          model: CODEX_TEST_MODEL_SELECTION.model,
          config: CodexAdapterV2.CODEX_THREAD_CONFIG,
        };
        const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
          ...codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: "unused-turn",
            prompt: "unused-prompt",
          }).slice(0, 5),
          {
            type: "expect_outbound",
            label: "resume archived thread",
            frame: { id: 3, method: "thread/resume", params },
          },
          {
            type: "emit_inbound",
            label: "resume error",
            frame: { id: 3, error: { code: -32600, message: resumeError } },
          },
        ];
        if (failAt !== 3) {
          entries.push(
            {
              type: "expect_outbound",
              label: "unarchive same thread",
              frame: { id: 4, method: "thread/unarchive", params: { threadId: nativeThreadId } },
            },
            {
              type: "emit_inbound",
              label: "unarchive result",
              frame:
                failAt === 4
                  ? { id: 4, error: { code: -32600, message: finalError } }
                  : { id: 4, result: { thread: { turns: [{ type: "unknown-history-item" }] } } },
            },
          );
        }
        if (failAt === 0 || failAt === 5) {
          entries.push(
            {
              type: "expect_outbound",
              label: "retry identical resume",
              frame: { id: 5, method: "thread/resume", params },
            },
            {
              type: "emit_inbound",
              label: "retry result",
              frame:
                failAt === 5
                  ? { id: 5, error: { code: -32600, message: finalError } }
                  : {
                      id: 5,
                      result: {
                        thread: {
                          id: nativeThreadId,
                          updatedAt: 1782622450,
                          turns: [{ type: "unknown-history-item" }],
                        },
                      },
                    },
            },
          );
        }
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario: name, entries }),
        );
        const resume = harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
        });
        if (failAt !== 0) {
          const error = yield* Effect.flip(resume);
          assert.equal(error._tag, "ProviderAdapterResumeThreadError");
          assert.nestedPropertyVal(error, "cause.errorMessage", finalError);
          assert.nestedPropertyVal(
            error,
            "cause.method",
            failAt === 4 ? "thread/unarchive" : "thread/resume",
          );
          assert.nestedPropertyVal(error, "cause.requestId", String(failAt));
          return;
        }
        const resumed = yield* resume;
        assert.equal(resumed.id, harness.providerThread.id);
        assert.equal(resumed.nativeThreadRef?.nativeId, nativeThreadId);
        assert.deepEqual(
          resumed.nativeConversationHeadRef,
          harness.providerThread.nativeConversationHeadRef,
        );
        assert.equal(resumed.status, "idle");
        assert.equal(DateTime.toEpochMillis(resumed.updatedAt), 1782622450000);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([
    "recover",
    "completion-only",
    "exhaust",
    "stop",
    "stop-continued",
    "stop-starting",
    "started-first",
    "completed-first",
    "new-run",
    "steer",
    "start-fails",
  ] as const)("handles terminal Codex capacity failures when retries %s", (outcome) =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = `capacity-${outcome}`;
        const nativeThreadId = `native-${scenario}`;
        const nativeTurnId = (attempt: number) => `${nativeThreadId}-${attempt}`;
        const prompt = "Continue the task.";
        const capacityError = {
          message: "Selected model is at capacity. Please try a different model.",
          codexErrorInfo: "serverOverloaded",
        };
        const failureEntries = (attempt: number): Array<CodexReplay.CodexAppServerReplayEntry> => [
          {
            type: "emit_inbound",
            frame: {
              method: "error",
              params: {
                threadId: nativeThreadId,
                turnId: nativeTurnId(attempt),
                willRetry: false,
                error: capacityError,
              },
            },
          },
          {
            type: "emit_inbound",
            frame: {
              method: "turn/completed",
              params: {
                threadId: nativeThreadId,
                turn: {
                  ...makeCodexReplayTurn({ id: nativeTurnId(attempt), status: "failed" }),
                  error: capacityError,
                },
              },
            },
          },
        ];
        const entries = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: nativeTurnId(0),
          prompt,
        });
        entries.push(
          ...(outcome === "completion-only" ? failureEntries(0).slice(1) : failureEntries(0)),
        );
        if (outcome === "completion-only") entries.push(...failureEntries(0).slice(1));
        const retries = outcome === "exhaust" ? 5 : outcome === "stop" ? 0 : 1;
        const steeredText = "Use the smaller change.";
        for (let attempt = 1; attempt <= retries; attempt++) {
          const start = codexReplayPreamble({
            nativeThreadId,
            nativeTurnId: nativeTurnId(attempt),
            prompt,
          }).slice(-3);
          if (
            outcome === "started-first" ||
            outcome === "stop-starting" ||
            outcome === "completed-first" ||
            outcome === "new-run"
          ) {
            start.splice(1, 0, ...start.splice(2, 1));
          }
          for (const entry of start) {
            if (entry.type === "expect_outbound") {
              entries.push({
                ...entry,
                frame: {
                  id: 3 + attempt,
                  method: "turn/start",
                  params: {
                    threadId: nativeThreadId,
                    input: [],
                    cwd: "/workspace",
                    model: "gpt-5.4",
                    approvalPolicy: "never",
                    approvalsReviewer: "user",
                    sandboxPolicy: { type: "dangerFullAccess" },
                    summary: "detailed",
                    collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                  },
                },
              });
            } else if (entry.type === "emit_inbound" && entry.label === "turn/start") {
              if (outcome === "completed-first" || outcome === "new-run")
                entries.push({
                  type: "emit_inbound",
                  frame: {
                    method: "turn/completed",
                    params: {
                      threadId: nativeThreadId,
                      turn: makeCodexReplayTurn({ id: nativeTurnId(attempt), status: "completed" }),
                    },
                  },
                });
              if (outcome === "new-run") {
                entries.push(
                  ...codexReplayPreamble({
                    nativeThreadId,
                    nativeTurnId: nativeTurnId(2),
                    prompt: "Next task.",
                  })
                    .slice(-3)
                    .map((next, index) => (index < 2 ? withReplayRequestId(next, 5) : next)),
                  ...failureEntries(2),
                );
              }
              entries.push(
                outcome === "start-fails"
                  ? {
                      type: "emit_inbound",
                      frame: {
                        id: 3 + attempt,
                        error: { code: -32603, message: "Start failed" },
                      },
                    }
                  : withReplayRequestId(entry, 3 + attempt),
              );
            } else if (outcome !== "start-fails") {
              entries.push(
                outcome === "stop-starting" && entry.type === "emit_inbound"
                  ? { ...entry, afterMs: 1_000 }
                  : entry,
              );
              if (outcome === "stop-starting") {
                entries.push(
                  {
                    type: "expect_outbound",
                    frame: {
                      id: 5,
                      method: "turn/interrupt",
                      params: { threadId: nativeThreadId, turnId: nativeTurnId(attempt) },
                    },
                  },
                  { type: "emit_inbound", frame: { id: 5, result: {} } },
                );
              }
            }
          }
          if (outcome === "exhaust") entries.push(...failureEntries(attempt));
          if (
            outcome === "recover" ||
            outcome === "completion-only" ||
            outcome === "started-first" ||
            outcome === "stop-starting"
          ) {
            entries.push({
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({
                    id: nativeTurnId(attempt),
                    status: outcome === "stop-starting" ? "interrupted" : "completed",
                  }),
                },
              },
            });
          }
          if (outcome === "stop-continued" || outcome === "steer") {
            const steering = outcome === "steer";
            entries.push(
              {
                type: "expect_outbound",
                frame: {
                  id: 5,
                  method: steering ? "turn/steer" : "turn/interrupt",
                  params: steering
                    ? {
                        threadId: nativeThreadId,
                        expectedTurnId: nativeTurnId(attempt),
                        input: [{ type: "text", text: steeredText }],
                      }
                    : { threadId: nativeThreadId, turnId: nativeTurnId(attempt) },
                },
              },
              {
                type: "emit_inbound",
                frame: { id: 5, result: steering ? { turnId: nativeTurnId(attempt) } : {} },
              },
              {
                type: "emit_inbound",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: makeCodexReplayTurn({
                      id: nativeTurnId(attempt),
                      status: steering ? "completed" : "interrupted",
                    }),
                  },
                },
              },
            );
          }
        }
        const milestones = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const starts: Array<unknown> = [];
        const retryStarted = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({ scenario, entries }),
          (event) =>
            event.type === "turn.terminal" ||
            (event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.status === "running")
              ? Queue.offer(milestones, event)
              : Effect.void,
          (method, params) =>
            Effect.gen(function* () {
              if (method === "turn/start") {
                starts.push(params);
                if (starts.length === 2) yield* Deferred.succeed(retryStarted, undefined);
              }
            }),
        );
        const turnInput = makeCodexTestTurnInput({
          threadId: harness.threadId,
          providerThread: harness.providerThread,
          now: yield* DateTime.now,
          attemptId: RunAttemptId.make(scenario),
          text: prompt,
        });
        yield* harness.runtime.startTurn(turnInput);
        for (let attempt = 1; attempt <= Math.max(retries, 1); attempt++) {
          const milestone = yield* Queue.take(milestones);
          assert.equal(milestone.type, "turn_item.updated");
          if (milestone.type !== "turn_item.updated" || milestone.turnItem.type !== "error") return;
          const providerTurnId = milestone.turnItem.providerTurnId;
          assert.isNotNull(providerTurnId);
          if (providerTurnId === null) return;
          assert.deepEqual(milestone.turnItem.retry, {
            attempt,
            maxAttempts: 5,
            retryDelayMs: Math.min(2_000 * 2 ** (attempt - 1), 30_000),
          });
          assert.isEmpty(harness.terminalEvents());
          if (outcome === "stop") {
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId,
            });
          }
          const steering =
            outcome === "steer"
              ? yield* harness.runtime
                  .steerTurn({
                    ...turnInput,
                    providerTurnId,
                    message: { ...turnInput.message, text: steeredText },
                  })
                  .pipe(Effect.forkScoped)
              : undefined;
          yield* TestClock.adjust(Math.min(2_000 * 2 ** (attempt - 1), 30_000));
          if (outcome === "stop-starting") {
            yield* Deferred.await(retryStarted);
            const stopping = yield* harness.runtime
              .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
              .pipe(Effect.forkScoped);
            yield* TestClock.adjust("1 second");
            yield* Fiber.join(stopping);
          }
          if (steering !== undefined) yield* Fiber.join(steering);
          if (outcome === "stop-continued") {
            yield* harness.until(() =>
              harness.events.some(
                (event) =>
                  event.type === "provider_turn.updated" &&
                  event.providerTurn.nativeTurnRef?.nativeId === nativeTurnId(1),
              ),
            );
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId,
            });
          }
        }
        yield* harness.awaitTerminal(1);
        if (outcome === "new-run") {
          assert.equal((yield* Queue.take(milestones)).type, "turn.terminal");
          yield* harness.runtime.startTurn({
            ...turnInput,
            attemptId: RunAttemptId.make("capacity-next-task"),
            message: { ...turnInput.message, text: "Next task." },
          });
          const nextRetry = yield* Queue.take(milestones);
          assert.equal(nextRetry.type, "turn_item.updated");
          if (nextRetry.type !== "turn_item.updated" || nextRetry.turnItem.type !== "error") return;
          assert.equal(nextRetry.turnItem.retry?.attempt, 1);
          return;
        }
        const terminal = harness.terminalEvents()[0];
        assert.equal(
          terminal?.status,
          outcome === "recover" ||
            outcome === "completion-only" ||
            outcome === "steer" ||
            outcome === "started-first" ||
            outcome === "completed-first"
            ? "completed"
            : outcome === "stop" || outcome === "stop-continued" || outcome === "stop-starting"
              ? "interrupted"
              : "failed",
        );
        assert.lengthOf(starts, 1 + retries);
        assert.lengthOf(harness.terminalEvents(), 1);
        assert.lengthOf(
          harness.events.filter(
            (event) =>
              event.type === "provider_turn.updated" && event.providerTurn.status === "running",
          ),
          outcome === "start-fails" ? 1 : 1 + retries,
        );
        const retryItems = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "error"
            ? [event.turnItem]
            : [],
        );
        assert.isTrue(
          ["completed", "failed", "interrupted"].includes(retryItems.at(-1)?.status ?? ""),
        );
        assert.lengthOf(new Set(retryItems.map((item) => item.id)), 1);

        if (outcome === "recover") {
          const fs = yield* FileSystem.FileSystem;
          const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-capacity-retry-" });
          const localTranscript = yield* decodeReplayTranscriptJson(
            (yield* encodeReplayTranscriptJson(
              makeCodexReplayTranscript({ scenario, entries }),
            )).replaceAll(yield* encodeStringJson("/workspace"), yield* encodeStringJson(cwd)),
          );
          const driver = yield* CodexReplay.makeReplayDriver(localTranscript);
          yield* Effect.gen(function* () {
            const orchestrator = yield* Orchestrator.OrchestratorV2;
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            const threadId = ThreadId.make("thread:capacity-retry");
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create-capacity-retry"),
              threadId,
              projectId: ProjectId.make("project:capacity-retry"),
              title: "Capacity retry",
              modelSelection: CODEX_TEST_MODEL_SELECTION,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const retrying = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "turn-item.updated" &&
                  event.payload.type === "error" &&
                  event.payload.retry != null &&
                  event.payload.status === "running",
              ),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("start-capacity-retry"),
              threadId,
              messageId: MessageId.make("message:capacity-retry"),
              text: prompt,
              attachments: [],
              createdBy: "user",
              creationSource: "web",
              dispatchMode: { type: "start_immediately" },
            });
            yield* worker.drain();
            yield* Fiber.join(retrying);
            assert.equal(
              (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)?.status,
              "running",
            );
            const completed = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) => event.type === "run.updated" && event.payload.status === "waiting",
              ),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
            yield* TestClock.adjust("2 seconds");
            yield* Fiber.join(completed);
            yield* worker.drain();
            const projection = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(projection.runs.at(-1)?.status, "completed");
            assert.deepEqual(
              projection.providerTurns.map((turn) => turn.status),
              ["failed", "completed"],
            );
            assert.lengthOf(
              projection.messages.filter((message) => message.role === "user"),
              1,
            );
            assert.isNull((yield* Ref.get(driver.state)).failure);
          }).pipe(
            Effect.provide(
              ProviderReplayHarness.layerWithRegistry(
                { name: "codex-capacity-retry", runtimePolicyOverride: { cwd } },
                CodexAdapterV2Testkit.layer({ transcript: localTranscript, driver }),
                { runEffectWorker: false },
              ),
            ),
          );
        }
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("resolves retryable app-server errors on resumed provider activity", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scenario = "codex-provider-api-retry";
        const nativeThreadId = `native-${scenario}-thread`;
        const nativeTurnId = `native-${scenario}-turn`;
        const transcript = makeCodexReplayTranscript({
          scenario,
          entries: [
            ...codexReplayPreamble({
              nativeThreadId,
              nativeTurnId,
              prompt: "Open github.com.",
            }),
            {
              type: "emit_inbound",
              label: "error/retry",
              frame: {
                method: "error",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  willRetry: true,
                  error: {
                    message: "Reconnecting... 2/5",
                    additionalDetails: "The response stream disconnected.",
                    codexErrorInfo: {
                      responseStreamDisconnected: { httpStatusCode: 529 },
                    },
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "item/started/after-retry",
              frame: {
                method: "item/started",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  startedAtMs: 1782622445000,
                  item: {
                    type: "commandExecution",
                    id: "command-after-provider-retry",
                    command: "pwd",
                    cwd: "/workspace",
                    processId: "42",
                    source: "unifiedExecStartup",
                    status: "inProgress",
                    commandActions: [{ type: "unknown", command: "pwd" }],
                    aggregatedOutput: null,
                    exitCode: null,
                    durationMs: null,
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "item/completed/after-retry",
              frame: {
                method: "item/completed",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622445010,
                  item: {
                    type: "commandExecution",
                    id: "command-after-provider-retry",
                    command: "pwd",
                    cwd: "/workspace",
                    processId: "42",
                    source: "unifiedExecStartup",
                    status: "completed",
                    commandActions: [{ type: "unknown", command: "pwd" }],
                    aggregatedOutput: "/workspace\n",
                    exitCode: 0,
                    durationMs: 10,
                  },
                },
              },
            },
            {
              type: "emit_inbound",
              label: "turn/completed",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-provider-api-retry"),
            text: "Open github.com.",
          }),
        );
        yield* harness.awaitTerminal(1);

        const retryItems = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.retry !== undefined
            ? [event.turnItem]
            : [],
        );
        assert.lengthOf(retryItems, 2);
        assert.equal(retryItems[0]?.status, "running");
        assert.equal(retryItems[0]?.failure.code, "responseStreamDisconnected");
        assert.equal(retryItems[0]?.failure.class, "transport_error");
        assert.equal(retryItems[0]?.failure.retryable, true);
        assert.deepEqual(retryItems[0]?.retry, {
          attempt: 2,
          maxAttempts: 5,
          retryDelayMs: null,
        });
        assert.equal(retryItems[1]?.id, retryItems[0]?.id);
        assert.equal(retryItems[1]?.status, "completed");
        assert.equal(retryItems[1]?.title, "Provider recovered");

        const recoveredIndex = harness.events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.status === "completed",
        );
        const resumedCommandIndex = harness.events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.input === "pwd",
        );
        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAtLeast(recoveredIndex, 0);
        assert.isAbove(resumedCommandIndex, recoveredIndex);
        assert.isAbove(terminalIndex, resumedCommandIndex);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each(["completed", "interrupted"] as const)(
    "retains Codex reasoning parts when the turn is %s",
    (terminalStatus) =>
      Effect.scoped(
        Effect.gen(function* () {
          const scenario = `codex-reasoning-${terminalStatus}`;
          const nativeThreadId = `native-${scenario}-thread`;
          const nativeTurnId = `native-${scenario}-turn`;
          const prompt = "Explain the check.";
          const transcript = makeCodexReplayTranscript({
            scenario,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
              ...[
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 0, delta: "Summary " },
                },
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 0, delta: "one" },
                },
                {
                  method: "item/reasoning/summaryTextDelta",
                  params: { itemId: "thought", summaryIndex: 1, delta: "Summary two" },
                },
                {
                  method: "item/reasoning/textDelta",
                  params: { itemId: "thought", contentIndex: 0, delta: "Raw trace" },
                },
                {
                  method: "item/completed",
                  params: {
                    item: {
                      type: "commandExecution",
                      id: "after-thought",
                      command: "pwd",
                      cwd: "/workspace",
                      processId: "42",
                      source: "unifiedExecStartup",
                      status: "completed",
                      commandActions: [{ type: "unknown", command: "pwd" }],
                      aggregatedOutput: "/workspace",
                      exitCode: 0,
                      durationMs: 1,
                    },
                  },
                },
                ...(terminalStatus === "completed"
                  ? [
                      {
                        method: "item/completed",
                        params: {
                          item: {
                            type: "reasoning",
                            id: "thought",
                            summary: ["Final summary one", "Summary two"],
                            content: ["Raw trace"],
                          },
                        },
                      },
                      {
                        method: "item/completed",
                        params: {
                          item: {
                            type: "reasoning",
                            id: "completion-only",
                            summary: ["Completion without deltas"],
                            content: [],
                          },
                        },
                      },
                      {
                        method: "item/reasoning/textDelta",
                        params: {
                          itemId: "delta-only",
                          contentIndex: 0,
                          delta: "Retained when completion omits content",
                        },
                      },
                      {
                        method: "item/completed",
                        params: {
                          item: { type: "reasoning", id: "delta-only", summary: [], content: [] },
                        },
                      },
                    ]
                  : []),
              ].map((event, index) => ({
                type: "emit_inbound" as const,
                label: `reasoning-${index}`,
                frame: {
                  method: event.method,
                  params: { threadId: nativeThreadId, turnId: nativeTurnId, ...event.params },
                },
              })),
              {
                type: "emit_inbound",
                label: "turn/completed",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: makeCodexReplayTurn({ id: nativeTurnId, status: terminalStatus }),
                  },
                },
              },
            ],
          });
          const harness = yield* makeCodexReplayHarness(transcript);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`attempt-${scenario}`),
              text: prompt,
            }),
          );
          yield* harness.firstTerminal;
          const latest = new Map(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "reasoning"
                ? [[event.turnItem.id, event.turnItem] as const]
                : [],
            ),
          );
          assert.deepEqual(
            [...latest.values()].map((item) => item.text),
            terminalStatus === "completed"
              ? [
                  "Final summary one",
                  "Summary two",
                  "Raw trace",
                  "Completion without deltas",
                  "Retained when completion omits content",
                ]
              : ["Summary one", "Summary two", "Raw trace"],
          );
          assert.isTrue([...latest.values()].every((item) => item.status === terminalStatus));
          assert.isTrue([...latest.values()].every((item) => item.streaming === false));
          assert.isTrue(
            [...latest.values()].every(
              (item) => item.runId !== null && item.providerTurnId !== null,
            ),
          );
          assert.equal(new Set([...latest.values()].map((item) => item.ordinal)).size, latest.size);
          const command = harness.events.find(
            (event) =>
              event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
          );
          assert.isDefined(command);
          if (command?.type === "turn_item.updated") {
            assert.isTrue(
              [...latest.values()]
                .slice(0, 3)
                .every((item) => item.ordinal < command.turnItem.ordinal),
            );
          }
          assert.deepEqual(assistantMessages(harness.events), []);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const finalAnswerTranscript = (
    scenario: string,
    answers: ReadonlyArray<{
      readonly id: string;
      readonly text: string;
      readonly phase?: "commentary" | "final_answer" | null;
      readonly omitPhase?: boolean;
      readonly streamed?: boolean;
      readonly completionDelayMs?: number;
    }>,
  ) => {
    const nativeThreadId = `native-${scenario}-thread`;
    const nativeTurnId = `native-${scenario}-turn`;
    const prompt = "Reply with the requested recovery marker.";
    return makeCodexReplayTranscript({
      scenario,
      entries: [
        ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
        ...answers.flatMap(
          (answer, index): ReadonlyArray<CodexReplay.CodexAppServerReplayEntry> => {
            const phase = answer.omitPhase
              ? {}
              : { phase: answer.phase === undefined ? ("final_answer" as const) : answer.phase };
            const completed: CodexReplay.CodexAppServerReplayEntry = {
              type: "emit_inbound",
              label: `item/completed/${answer.id}`,
              ...(answer.completionDelayMs === undefined
                ? {}
                : { afterMs: answer.completionDelayMs }),
              frame: {
                method: "item/completed",
                params: {
                  item: {
                    type: "agentMessage",
                    id: answer.id,
                    text: answer.text,
                    ...phase,
                    memoryCitation: null,
                  },
                  threadId: nativeThreadId,
                  turnId: nativeTurnId,
                  completedAtMs: 1782622441000 + index,
                },
              },
            };
            if (!answer.streamed) {
              return [completed];
            }
            return [
              {
                type: "emit_inbound",
                label: `item/started/${answer.id}`,
                frame: {
                  method: "item/started",
                  params: {
                    item: {
                      type: "agentMessage",
                      id: answer.id,
                      text: "",
                      ...phase,
                      memoryCitation: null,
                    },
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    startedAtMs: 1782622440500 + index,
                  },
                },
              },
              {
                type: "emit_inbound",
                label: `item/agentMessage/delta/${answer.id}`,
                frame: {
                  method: "item/agentMessage/delta",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    itemId: answer.id,
                    delta: answer.text,
                  },
                },
              },
              completed,
            ];
          },
        ),
        {
          type: "emit_inbound",
          label: "turn/completed",
          frame: {
            method: "turn/completed",
            params: {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: nativeTurnId, status: "completed" }),
            },
          },
        },
      ],
    });
  };

  it.effect("suppresses a trailing empty final answer after a non-empty final answer", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = finalAnswerTranscript("codex-redundant-empty-final", [
          { id: "answer-non-empty", text: "CODEX_RECOVERY_OK" },
          { id: "answer-empty", text: "" },
        ]);
        const harness = yield* makeCodexReplayHarness(transcript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-redundant-empty-final"),
            text: "Reply with the requested recovery marker.",
          }),
        );
        yield* harness.awaitTerminal(1);

        assert.deepEqual(
          assistantMessages(harness.events).map((event) => event.message.text),
          ["CODEX_RECOVERY_OK"],
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const BG_SCENARIO = "codex-bg-exec-wake";
  const BG_NATIVE_THREAD = "native-codex-bg-thread";
  const BG_NATIVE_TURN = "native-codex-bg-turn";
  const BG_COMMAND_ITEM = "call-codex-bg-command";
  const BG_COMMAND = "sleep 20 && echo CODEX_BG_WAKE_DONE";
  const BG_PROMPT = "Start the sleep in the background and reply STARTED.";

  const backgroundCommandItem = (status: "inProgress" | "completed"): Record<string, unknown> => ({
    type: "commandExecution",
    id: BG_COMMAND_ITEM,
    command: BG_COMMAND,
    cwd: "/workspace",
    processId: "4242",
    source: "unifiedExecStartup",
    status,
    commandActions: [{ type: "unknown", command: BG_COMMAND }],
    aggregatedOutput: status === "completed" ? "CODEX_BG_WAKE_DONE\n" : null,
    exitCode: status === "completed" ? 0 : null,
    durationMs: status === "completed" ? 25_000 : null,
  });

  const backgroundExecTranscript = makeCodexReplayTranscript({
    scenario: BG_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: BG_NATIVE_THREAD,
        nativeTurnId: BG_NATIVE_TURN,
        prompt: BG_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: backgroundCommandItem("inProgress"),
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/root-answer",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "agentMessage",
              id: "root-answer-bg",
              text: "STARTED",
              phase: "final_answer",
              memoryCitation: null,
            },
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: BG_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: BG_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-late",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: backgroundCommandItem("completed"),
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            completedAtMs: 1782622465500,
          },
        },
      },
    ],
  });

  it.effect.each([false, true])(
    "keeps commands interrupted after native thread closure (persistent tool: %s)",
    (retainContext) =>
      Effect.gen(function* () {
        const notification = (method: string, params: unknown) => ({
          type: "emit_inbound" as const,
          label: method,
          frame: { method, params },
        });
        const monitor = (status: "inProgress" | "completed") =>
          notification(status === "inProgress" ? "item/started" : "item/completed", {
            threadId: BG_NATIVE_THREAD,
            turnId: BG_NATIVE_TURN,
            item: {
              type: "dynamicToolCall",
              id: "persistent-monitor",
              namespace: "grok",
              tool: "monitor",
              status,
              arguments: { persistent: true, command: "tail -f" },
            },
          });
        // Replies fence notification handling on the same protocol reader.
        const unsubscribe = (id: number, notifications: ReturnType<typeof notification>[] = []) => [
          {
            type: "expect_outbound" as const,
            label: `unsubscribe-${id}`,
            frame: { id, method: "thread/unsubscribe", params: { threadId: BG_NATIVE_THREAD } },
          },
          ...notifications,
          {
            type: "emit_inbound" as const,
            label: `unsubscribe-result-${id}`,
            frame: { id, result: { status: "notSubscribed" } },
          },
        ];
        const closed = notification("thread/closed", { threadId: BG_NATIVE_THREAD });
        const transcript = makeCodexReplayTranscript({
          scenario: `codex-background-thread-closed-${retainContext}`,
          entries: [
            ...backgroundExecTranscript.entries.slice(0, -2),
            ...(retainContext ? [monitor("inProgress")] : []),
            backgroundExecTranscript.entries.at(-2)!,
            ...unsubscribe(4),
            ...unsubscribe(5, [notification("thread/closed", { threadId: "unrelated-thread" })]),
            ...unsubscribe(6, [closed]),
            ...unsubscribe(7, [
              closed,
              notification("item/completed", {
                threadId: BG_NATIVE_THREAD,
                turnId: BG_NATIVE_TURN,
                item: backgroundCommandItem("completed"),
              }),
              ...(retainContext ? [monitor("completed")] : []),
            ]),
          ],
        });
        const interrupted = yield* Deferred.make<void>();
        const monitorCompleted = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(transcript, (event) => {
          if (event.type !== "turn_item.updated") return Effect.void;
          if (
            event.turnItem.nativeItemRef?.nativeId === BG_COMMAND_ITEM &&
            event.turnItem.status === "interrupted"
          ) {
            return Deferred.succeed(interrupted, undefined);
          }
          return event.turnItem.nativeItemRef?.nativeId === "persistent-monitor" &&
            event.turnItem.status === "completed"
            ? Deferred.succeed(monitorCompleted, undefined)
            : Effect.void;
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-thread-closed"),
            text: BG_PROMPT,
          }),
        );
        yield* harness.firstTerminal;
        const unload = harness.runtime.unloadThread!({ providerThread: harness.providerThread });
        yield* unload;
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        yield* unload;
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        yield* unload;
        assert.equal(yield* harness.hasPendingBackgroundWork, retainContext);
        yield* Deferred.await(interrupted);
        yield* unload;
        if (retainContext) yield* Deferred.await(monitorCompleted);
        assert.deepEqual(
          {
            items: harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId === BG_COMMAND_ITEM
                ? [event.turnItem.status]
                : [],
            ),
            nodes: harness.events.flatMap((event) =>
              event.type === "node.updated" &&
              event.node.nativeItemRef?.nativeId === BG_COMMAND_ITEM
                ? [event.node.status]
                : [],
            ),
            roots: harness.terminalEvents().map((event) => event.status),
            continuations: harness.continuationRequests.length,
          },
          {
            items: ["running", "interrupted"],
            nodes: ["running", "interrupted"],
            roots: ["completed"],
            continuations: 0,
          },
        );
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.isFalse(
          yield* harness.runtime.hasPendingBackgroundWorkForThread!(harness.providerThread),
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const backgroundStopCases = [true, false, "still_running", "thread_unloaded"] as const;
  const makeBackgroundStopTranscript = (terminated: (typeof backgroundStopCases)[number]) => {
    const stillRunning = terminated === "still_running";
    return makeCodexReplayTranscript({
      scenario: `codex-bg-stop-${terminated}`,
      entries: [
        ...backgroundExecTranscript.entries.slice(0, -1),
        {
          type: "expect_outbound",
          label: "terminate-background-command",
          frame: {
            id: 4,
            method: "thread/backgroundTerminals/terminate",
            params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
          },
        },
        {
          type: "emit_inbound",
          label: "terminate-background-command",
          frame:
            terminated === "thread_unloaded"
              ? {
                  id: 4,
                  error: { code: -32600, message: `thread not found: ${BG_NATIVE_THREAD}` },
                }
              : { id: 4, result: { terminated: terminated === true } },
        },
        ...(terminated === false || stillRunning
          ? [
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 5,
                  method: "thread/backgroundTerminals/list",
                  params: { threadId: BG_NATIVE_THREAD },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: {
                  id: 5,
                  result: { data: stillRunning ? [{ processId: "4242" }] : [], nextCursor: null },
                },
              },
            ]
          : []),
        ...(stillRunning
          ? [
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 6,
                  method: "thread/backgroundTerminals/terminate",
                  params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 6, result: { terminated: false } },
              },
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 7,
                  method: "thread/backgroundTerminals/list",
                  params: { threadId: BG_NATIVE_THREAD },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 7, result: { data: [{ processId: "4242" }], nextCursor: null } },
              },
              {
                type: "expect_outbound" as const,
                frame: {
                  id: 8,
                  method: "thread/backgroundTerminals/terminate",
                  params: { threadId: BG_NATIVE_THREAD, processId: "4242" },
                },
              },
              {
                type: "emit_inbound" as const,
                frame: { id: 8, result: { terminated: true } },
              },
            ]
          : []),
        backgroundExecTranscript.entries.at(-1)!,
      ],
    });
  };

  it.effect.each(backgroundStopCases)(
    "stops a command after root completion when termination returns %s",
    (terminated) => {
      const stillRunning = terminated === "still_running";
      const transcript = makeBackgroundStopTranscript(terminated);
      return Effect.scoped(
        Effect.gen(function* () {
          const stopped = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(transcript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted"
              ? Deferred.succeed(stopped, undefined)
              : Effect.void,
          );
          const now = yield* DateTime.now;
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-bg-stop"),
              text: BG_PROMPT,
            }),
          );
          yield* harness.firstTerminal;
          const terminal = harness.terminalEvents()[0]!;
          assert.equal(terminal.status, "completed");
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          assert.isFalse(
            yield* harness.runtime.hasPendingBackgroundWorkForThread!({
              ...harness.providerThread,
              id: ProviderThreadId.make("unrelated-provider-thread"),
            }),
          );
          if (stillRunning) {
            const failed = yield* harness.runtime
              .interruptTurn({
                providerThread: harness.providerThread,
                providerTurnId: terminal.providerTurnId,
                requestRuntimeRestart: true,
              })
              .pipe(Effect.exit);
            assert.equal(failed._tag, "Failure");
            assert.isTrue(yield* harness.hasPendingBackgroundWork);
          }
          yield* harness.runtime.interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: stillRunning
              ? ProviderTurnId.make("later-completed-turn")
              : terminal.providerTurnId,
            requestRuntimeRestart: true,
          });
          yield* Deferred.await(stopped);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.equal(harness.terminalEvents()[0]?.status, "completed");
          assert.lengthOf(harness.continuationRequests, 0);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      );
    },
  );
  it.effect("stops a completed run's background command without a client-supplied run", () => {
    const transcript = makeBackgroundStopTranscript(true);
    return Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bg-stop-workspace-" });
        const localTranscript = yield* decodeReplayTranscriptJson(
          (yield* encodeReplayTranscriptJson(transcript)).replaceAll(
            yield* encodeStringJson("/workspace"),
            yield* encodeStringJson(cwd),
          ),
        );
        const replayDriver = yield* CodexReplay.makeReplayDriver(localTranscript);
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        assert.equal(
          Number(yield* spawner.exitCode(ChildProcess.make("git", ["init", "--quiet"], { cwd }))),
          0,
        );
        assert.equal(
          Number(
            yield* spawner.exitCode(
              ChildProcess.make(
                "git",
                [
                  "-c",
                  "user.name=Test",
                  "-c",
                  "user.email=test@example.com",
                  "commit",
                  "--allow-empty",
                  "--quiet",
                  "-m",
                  "Initial commit",
                ],
                { cwd },
              ),
            ),
          ),
          0,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make("thread:background-stop");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-background-stop"),
            threadId,
            projectId: ProjectId.make("project:background-stop"),
            title: "Background stop",
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const waiting = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) => event.type === "run.updated" && event.payload.status === "waiting",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-background-stop"),
            threadId,
            messageId: MessageId.make("message:background-stop"),
            text: BG_PROMPT,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* worker.drain();
          assert.isNull((yield* Ref.get(replayDriver.state)).failure);
          yield* Fiber.join(waiting);
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const run = projection.runs.at(-1)!;
          assert.equal(run.status, "completed");
          assert.equal(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks?.length,
            1,
          );
          const stopped = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.type === "command_execution" &&
                event.payload.status === "interrupted",
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop-background-command"),
            threadId,
          });
          yield* worker.drain();
          yield* Fiber.join(stopped);
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)?.status,
            "completed",
          );
          assert.deepEqual(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks,
            [],
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: "codex-background-stop", runtimePolicyOverride: { cwd } },
              CodexAdapterV2Testkit.layer({
                transcript: localTranscript,
                driver: replayDriver,
              }),
              { runEffectWorker: false },
            ),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    );
  });

  it.effect("Stop ends a background command no Codex process tracks any more", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bg-stale-workspace-" });
        const staleTranscript = makeCodexReplayTranscript({
          scenario: "codex-bg-stop-untracked",
          entries: [
            ...backgroundExecTranscript.entries.slice(0, -1),
            { type: "runtime_exit", status: "success" },
          ],
        });
        const localTranscript = yield* decodeReplayTranscriptJson(
          (yield* encodeReplayTranscriptJson(staleTranscript)).replaceAll(
            yield* encodeStringJson("/workspace"),
            yield* encodeStringJson(cwd),
          ),
        );
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        for (const args of [
          ["init", "--quiet"],
          [
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "Initial commit",
          ],
        ]) {
          assert.equal(Number(yield* spawner.exitCode(ChildProcess.make("git", args, { cwd }))), 0);
        }
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make("thread:background-stop-untracked");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create-background-stop-untracked"),
            threadId,
            projectId: ProjectId.make("project:background-stop-untracked"),
            title: "Background stop untracked",
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const settled = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                (event.payload.status === "waiting" || event.payload.status === "completed"),
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("start-background-stop-untracked"),
            threadId,
            messageId: MessageId.make("message:background-stop-untracked"),
            text: BG_PROMPT,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          });
          yield* worker.drain();
          yield* Fiber.join(settled);
          yield* worker.drain();
          const before = yield* orchestrator.getThreadShell(threadId);
          assert.deepEqual(
            before?.pendingBackgroundTasks?.map((task) => task.kind),
            ["command"],
            "the thread still shows the command the gone process never finished",
          );
          const run = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop-background-untracked"),
            threadId,
            runId: run.id,
            holdQueue: true,
          });
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            projection.turnItems.flatMap((item) =>
              item.type === "command_execution" ? [item.status] : [],
            ),
            ["interrupted"],
          );
          assert.equal(projection.runs.at(-1)?.status, "completed");
          assert.deepEqual(
            (yield* orchestrator.getThreadShell(threadId))?.pendingBackgroundTasks,
            [],
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: "codex-background-stop-untracked", runtimePolicyOverride: { cwd } },
              CodexAdapterV2Testkit.layer({ transcript: localTranscript }),
              { runEffectWorker: false },
            ),
          ),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
  );

  const INTERRUPT_SCENARIO = "codex-interrupt-mid-command";
  const INTERRUPT_NATIVE_THREAD = "native-codex-interrupt-thread";
  const INTERRUPT_NATIVE_TURN = "native-codex-interrupt-turn";
  const INTERRUPT_COMMAND_ITEM = "exec-codex-interrupt-command";
  const INTERRUPT_COMMAND_ITEM_TWO = "exec-codex-interrupt-command-two";
  const INTERRUPT_CHILD_COMMAND_ITEM = "exec-codex-interrupt-child-command";
  const INTERRUPT_CHILD_NATIVE_THREAD = "native-codex-interrupt-child-thread";
  const INTERRUPT_CHILD_NATIVE_TURN = "native-codex-interrupt-child-turn";
  const INTERRUPT_LATE_CHILD_NATIVE_TURN = "native-codex-interrupt-late-child-turn";
  const INTERRUPT_COMMAND = "bash -c 'sleep 30; echo SHOULD_NOT_FINISH_CMD_INTERRUPT_FIXTURE'";
  const INTERRUPT_COMMAND_TWO = "bash -c 'sleep 20; echo SECOND_COMMAND'";
  const INTERRUPT_PROMPT = "Run a long foreground command and wait until interrupted.";

  const interruptCommandItem = (status: "inProgress" | "completed"): Record<string, unknown> => ({
    type: "commandExecution",
    id: INTERRUPT_COMMAND_ITEM,
    command: INTERRUPT_COMMAND,
    cwd: "/workspace",
    processId: "57680",
    source: "unifiedExecStartup",
    status,
    commandActions: [{ type: "unknown", command: INTERRUPT_COMMAND }],
    aggregatedOutput: status === "completed" ? "SHOULD_NOT_FINISH_CMD_INTERRUPT_FIXTURE\n" : null,
    exitCode: status === "completed" ? 0 : null,
    durationMs: status === "completed" ? 30_000 : null,
  });

  const interruptMidCommandTranscript = makeCodexReplayTranscript({
    scenario: INTERRUPT_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: INTERRUPT_NATIVE_THREAD,
        nativeTurnId: INTERRUPT_NATIVE_TURN,
        prompt: INTERRUPT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: interruptCommandItem("inProgress"),
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt",
        frame: {
          id: 4,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt",
        frame: { id: 4, result: {} },
      },
      {
        type: "emit_inbound",
        label: "item/started/command-two-after-interrupt-response",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_COMMAND_ITEM_TWO,
              command: INTERRUPT_COMMAND_TWO,
              processId: "57681",
              commandActions: [{ type: "unknown", command: INTERRUPT_COMMAND_TWO }],
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            startedAtMs: 1782622440600,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_NATIVE_TURN,
              status: "interrupted",
            }),
          },
        },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/one",
        frame: {
          id: 5,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57680" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/one",
        frame: { id: 5, result: { terminated: false } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/list/after-false",
        frame: {
          id: 6,
          method: "thread/backgroundTerminals/list",
          params: { threadId: INTERRUPT_NATIVE_THREAD },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/list/after-false",
        frame: { id: 6, result: { data: [], nextCursor: null } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: {
          id: 7,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57681" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/two",
        frame: { id: 7, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "item/completed/command-late",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: interruptCommandItem("completed"),
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            completedAtMs: 1782622465500,
          },
        },
      },
    ],
  });

  it.effect("contains commands that start before and after the interrupt response", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptMidCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-mid-command"),
            text: INTERRUPT_PROMPT,
          }),
        );

        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.status === "running",
          ),
        );

        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");

        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAtLeast(terminalIndex, 0);

        let lastCommandBeforeTerminal:
          | Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn_item.updated" }>
          | undefined;
        for (let index = 0; index < terminalIndex; index++) {
          const event = harness.events[index];
          if (event?.type === "turn_item.updated" && event.turnItem.type === "command_execution") {
            lastCommandBeforeTerminal = event;
          }
        }
        assert.isDefined(lastCommandBeforeTerminal);
        assert.equal(lastCommandBeforeTerminal.turnItem.status, "interrupted");
        assert.isNotNull(lastCommandBeforeTerminal.turnItem.completedAt);

        const interruptedCommandsBeforeTerminal = harness.events
          .slice(0, terminalIndex)
          .flatMap((event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted"
              ? [event.turnItem.input]
              : [],
          )
          .sort();
        assert.deepEqual(
          interruptedCommandsBeforeTerminal,
          [INTERRUPT_COMMAND, INTERRUPT_COMMAND_TWO].sort(),
        );

        const interruptedCommandIndex = harness.events.findIndex(
          (event, index) =>
            index < terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "interrupted",
        );
        assert.isAbove(
          terminalIndex,
          interruptedCommandIndex,
          "command terminalization must precede turn.terminal",
        );

        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.continuationRequests, 0);

        yield* TestClock.adjust("30 seconds");
        for (let attempt = 0; attempt < 100; attempt++) {
          yield* Effect.yieldNow;
        }
        assert.lengthOf(harness.continuationRequests, 0);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);

        const postTerminalCommandUpdates = harness.events.filter(
          (event, index) =>
            index > terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution",
        );
        assert.lengthOf(
          postTerminalCommandUpdates,
          0,
          "late item/completed after interrupt must not project",
        );

        const commandUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > => event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isAtLeast(commandUpdates.length, 2, "start + interrupt terminalization");
        assert.equal(commandUpdates[commandUpdates.length - 1]?.turnItem.status, "interrupted");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const interruptSubagentCommandTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-subagent-command",
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: INTERRUPT_NATIVE_THREAD,
        nativeTurnId: INTERRUPT_NATIVE_TURN,
        prompt: INTERRUPT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/completed/subAgentActivity-started",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "subAgentActivity",
              id: "call-codex-interrupt-subagent",
              kind: "started",
              agentThreadId: INTERRUPT_CHILD_NATIVE_THREAD,
              agentPath: "/root/stop_hold",
            },
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/started/child",
        frame: {
          method: "turn/started",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_CHILD_NATIVE_TURN,
              status: "inProgress",
            }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/child-command",
        frame: {
          method: "item/started",
          params: {
            item: {
              ...interruptCommandItem("inProgress"),
              id: INTERRUPT_CHILD_COMMAND_ITEM,
              processId: "57682",
            },
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_CHILD_NATIVE_TURN,
            startedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt/root",
        frame: {
          id: 4,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turnId: INTERRUPT_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/root",
        frame: { id: 4, result: {} },
      },
      {
        type: "expect_outbound",
        label: "turn/interrupt/child",
        frame: {
          id: 5,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_CHILD_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/child",
        frame: { id: 5, result: {} },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/child",
        frame: {
          id: 6,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_CHILD_NATIVE_THREAD, processId: "57682" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/child",
        frame: { id: 6, result: { terminated: true } },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/root",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: INTERRUPT_NATIVE_TURN, status: "interrupted" }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/child-completed-race",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_CHILD_NATIVE_TURN,
              status: "completed",
            }),
          },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  const assertChildProviderTerminalBeforeRoot = (
    events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
    rootThreadId: ThreadId,
  ) => {
    const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
    const childProviderTurnIndex = events.findIndex(
      (event) =>
        event.type === "provider_turn.updated" &&
        event.threadId !== rootThreadId &&
        event.providerTurn.status === "interrupted",
    );
    const childProviderThreadIndex = events.findIndex(
      (event) =>
        event.type === "provider_thread.updated" &&
        event.providerThread.appThreadId !== rootThreadId &&
        event.providerThread.status === "idle",
    );
    assert.isAtLeast(childProviderTurnIndex, 0, "child provider turn must terminalize");
    assert.isAtLeast(childProviderThreadIndex, 0, "child provider thread must become idle");
    assert.isAbove(
      terminalIndex,
      childProviderTurnIndex,
      "child provider turn must terminalize before the root run",
    );
    assert.isAbove(
      terminalIndex,
      childProviderThreadIndex,
      "child provider thread must become idle before the root run",
    );
  };

  it.effect("contains descendant commands and keeps Stop authoritative", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptSubagentCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-subagent-command"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
              event.turnItem.status === "running",
          ),
        );
        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });

        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        const childCommandUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          > =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM,
        );
        assert.equal(childCommandUpdates.at(-1)?.turnItem.status, "interrupted");
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const rootCompletionIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) => entry.type === "emit_inbound" && entry.label === "turn/completed/root",
  );
  const interruptLateSubagentTurnTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-late-subagent-turn",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, rootCompletionIndex),
      {
        type: "emit_inbound",
        label: "turn/started/late-child",
        frame: {
          method: "turn/started",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: INTERRUPT_LATE_CHILD_NATIVE_TURN,
              status: "inProgress",
            }),
          },
        },
      },
      ...interruptSubagentCommandTranscript.entries.slice(rootCompletionIndex, -1),
      {
        type: "expect_outbound",
        label: "turn/interrupt/late-child",
        frame: {
          id: 7,
          method: "turn/interrupt",
          params: {
            threadId: INTERRUPT_CHILD_NATIVE_THREAD,
            turnId: INTERRUPT_LATE_CHILD_NATIVE_TURN,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/interrupt/late-child",
        frame: { id: 7, result: {} },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("interrupts descendants that start after the initial Stop snapshot", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(interruptLateSubagentTurnTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-interrupt-late-subagent-turn"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
              event.turnItem.status === "running",
          ),
        );
        const providerTurnId = harness.events.find(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);

        const interruptFiber = yield* harness.runtime
          .interruptTurn({ providerThread: harness.providerThread, providerTurnId })
          .pipe(Effect.forkScoped);
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
          ),
        );
        yield* Fiber.join(interruptFiber);

        yield* harness.awaitTerminal(1);
        const lateChildUpdates = harness.events.filter(
          (
            event,
          ): event is Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "provider_turn.updated" }
          > =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === INTERRUPT_LATE_CHILD_NATIVE_TURN,
        );
        assert.equal(lateChildUpdates.at(-1)?.providerTurn.status, "interrupted");
        assertChildProviderTerminalBeforeRoot(harness.events, harness.threadId);
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("keeps a cut-in's unacknowledged Codex turn observable for retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const running = yield* Deferred.make<void>();
        const tail = yield* Deferred.make<void>();
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-cut-in-request-failure",
          entries: [
            ...codexReplayPreamble({
              nativeThreadId: INTERRUPT_NATIVE_THREAD,
              nativeTurnId: INTERRUPT_NATIVE_TURN,
              prompt: INTERRUPT_PROMPT,
            }),
            {
              type: "emit_inbound",
              label: "command running",
              frame: {
                method: "item/started",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  turnId: INTERRUPT_NATIVE_TURN,
                  item: interruptCommandItem("inProgress"),
                },
              },
            },
            {
              type: "expect_outbound",
              label: "failed interrupt request",
              frame: {
                id: 4,
                method: "turn/interrupt",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  turnId: INTERRUPT_NATIVE_TURN,
                },
              },
            },
            {
              type: "emit_inbound",
              label: "native server rejects cancellation",
              frame: { id: 4, error: { code: -32000, message: "Cancellation unavailable" } },
            },
            {
              type: "emit_inbound",
              label: "work still running after rejected cancel",
              frame: {
                method: "item/completed",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  turnId: INTERRUPT_NATIVE_TURN,
                  item: {
                    id: "message-after-failed-cancel",
                    type: "agentMessage",
                    phase: "commentary",
                    text: "STILL_RUNNING_AFTER_FAILED_CANCEL",
                  },
                },
              },
            },
            {
              type: "expect_outbound",
              label: "retry interrupt",
              frame: {
                id: 5,
                method: "turn/interrupt",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  turnId: INTERRUPT_NATIVE_TURN,
                },
              },
            },
            { type: "emit_inbound", frame: { id: 5, result: {} } },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  turn: makeCodexReplayTurn({ id: INTERRUPT_NATIVE_TURN, status: "interrupted" }),
                },
              },
            },
            {
              type: "expect_outbound",
              label: "terminate retained terminal on retry",
              frame: {
                id: 6,
                method: "thread/backgroundTerminals/terminate",
                params: {
                  threadId: INTERRUPT_NATIVE_THREAD,
                  processId: "57680",
                },
              },
            },
            { type: "emit_inbound", frame: { id: 6, result: { terminated: true } } },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript, (event) => {
          if (
            event.type === "message.updated" &&
            event.message.text === "STILL_RUNNING_AFTER_FAILED_CANCEL"
          ) {
            return Deferred.succeed(tail, undefined);
          }
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "command_execution")
            return Effect.void;
          if (event.turnItem.status === "running") return Deferred.succeed(running, undefined);
          return Effect.void;
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-cut-in-request-failure"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* Deferred.await(running);
        const providerTurnId = harness.events.find(
          (event) => event.type === "provider_turn.updated",
        )?.providerTurn.id;
        assert.isDefined(providerTurnId);
        const input = {
          providerThread: harness.providerThread,
          providerTurnId,
          requestRuntimeRestart: true,
          replacementFollows: true,
        } as const;
        const rejected = yield* harness.runtime.interruptTurn(input).pipe(Effect.exit);
        assert.equal(rejected._tag, "Failure");
        assert.isTrue(
          yield* harness.hasPendingBackgroundWork,
          "failed native cancellation must retain its work owner",
        );
        assert.lengthOf(
          harness.terminalEvents(),
          0,
          "an unacknowledged stop is not a terminal outcome",
        );
        yield* Deferred.await(tail);
        yield* harness.runtime.interruptTurn(input);
        yield* harness.firstTerminal;
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.equal(
          harness.events.filter(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.status === "interrupted",
          ).length,
          1,
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const childInterruptResponseIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) => entry.type === "emit_inbound" && entry.label === "turn/interrupt/child",
  );
  const rootInterruptResponseIndex = interruptSubagentCommandTranscript.entries.findIndex(
    (entry) => entry.type === "emit_inbound" && entry.label === "turn/interrupt/root",
  );
  const interruptSubagentRequestFailureTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-subagent-request-failure",
    entries: [
      ...interruptSubagentCommandTranscript.entries.slice(0, rootInterruptResponseIndex + 1),
      {
        type: "emit_inbound",
        label: "turn/completed/root-before-child-interrupt-failure",
        frame: {
          method: "turn/completed",
          params: {
            threadId: INTERRUPT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: INTERRUPT_NATIVE_TURN, status: "interrupted" }),
          },
        },
      },
      ...interruptSubagentCommandTranscript.entries.slice(
        rootInterruptResponseIndex + 1,
        childInterruptResponseIndex,
      ),
      {
        type: "emit_inbound",
        label: "turn/interrupt/child",
        frame: {
          id: 5,
          error: { code: -32_000, message: "child interrupt request failed" },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  const interruptTerminationFailureTranscript = makeCodexReplayTranscript({
    scenario: "codex-interrupt-termination-failure",
    entries: [
      ...interruptMidCommandTranscript.entries
        .filter(
          (entry) => entry.type === "runtime_exit" || entry.label !== "item/completed/command-late",
        )
        .map((entry) =>
          entry.type === "emit_inbound" &&
          entry.label === "thread/backgroundTerminals/list/after-false"
            ? {
                ...entry,
                frame: {
                  id: 6,
                  result: {
                    data: [{ processId: "57680" }],
                    nextCursor: null,
                  },
                },
              }
            : entry,
        ),
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/terminate/one-retry",
        frame: {
          id: 8,
          method: "thread/backgroundTerminals/terminate",
          params: { threadId: INTERRUPT_NATIVE_THREAD, processId: "57680" },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/terminate/one-retry",
        frame: { id: 8, result: { terminated: false } },
      },
      {
        type: "expect_outbound",
        label: "thread/backgroundTerminals/list/after-false-retry",
        frame: {
          id: 9,
          method: "thread/backgroundTerminals/list",
          params: { threadId: INTERRUPT_NATIVE_THREAD },
        },
      },
      {
        type: "emit_inbound",
        label: "thread/backgroundTerminals/list/after-false-retry",
        frame: {
          id: 9,
          result: { data: [{ processId: "57680" }], nextCursor: null },
        },
      },
      { type: "runtime_exit", status: "success" },
    ],
  });

  it.effect("keeps the Codex root interruptible when a descendant rejects the cut-in", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const childRunning = yield* Deferred.make<void>();
        const childTail = yield* Deferred.make<void>();
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-cut-in-root-hold-on-child-failure",
          entries: [
            ...interruptSubagentRequestFailureTranscript.entries.filter(
              (entry) => entry.type !== "runtime_exit",
            ),
            {
              type: "emit_inbound",
              label: "unreached child continues after failed interrupt",
              frame: {
                method: "item/completed",
                params: {
                  threadId: INTERRUPT_CHILD_NATIVE_THREAD,
                  turnId: INTERRUPT_CHILD_NATIVE_TURN,
                  item: {
                    type: "agentMessage",
                    id: "child-after-failed-cut-in",
                    phase: "commentary",
                    text: "CHILD_STILL_RUNNING_AFTER_FAILED_CUT_IN",
                  },
                },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript, (event) => {
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === INTERRUPT_CHILD_COMMAND_ITEM &&
            event.turnItem.status === "running"
          )
            return Deferred.succeed(childRunning, undefined);
          if (
            event.type === "message.updated" &&
            event.message.text === "CHILD_STILL_RUNNING_AFTER_FAILED_CUT_IN"
          )
            return Deferred.succeed(childTail, undefined);
          return Effect.void;
        });
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-cut-in-child-failure"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* Deferred.await(childRunning);
        const root = harness.events.find(
          (event) => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        );
        if (root?.type !== "provider_turn.updated")
          return assert.fail("root provider turn missing");
        const rejected = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: root.providerTurn.id,
            requestRuntimeRestart: true,
            replacementFollows: true,
          })
          .pipe(Effect.exit);
        assert.equal(rejected._tag, "Failure");
        yield* Deferred.await(childTail);
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 0);
        const lastRoot = harness.events.findLast(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === root.providerTurn.id,
        );
        assert.equal(
          lastRoot?.type === "provider_turn.updated" && lastRoot.providerTurn.status,
          "running",
          "a retry and Stop must still find the old root while its child cancellation is unconfirmed",
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("holds the Codex root when retained terminal termination is unconfirmed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commandRunning = yield* Deferred.make<void>();
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-cut-in-retained-terminal-failure",
          entries: interruptTerminationFailureTranscript.entries.filter(
            (entry) => entry.type !== "runtime_exit",
          ),
        });
        const harness = yield* makeCodexReplayHarness(transcript, (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.nativeItemRef?.nativeId === INTERRUPT_COMMAND_ITEM &&
          event.turnItem.status === "running"
            ? Deferred.succeed(commandRunning, undefined)
            : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-cut-in-retained-terminal-failure"),
            text: INTERRUPT_PROMPT,
          }),
        );
        yield* Deferred.await(commandRunning);
        const root = harness.events.find(
          (event) => event.type === "provider_turn.updated" && event.threadId === harness.threadId,
        );
        if (root?.type !== "provider_turn.updated")
          return assert.fail("root provider turn missing");
        const rejected = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: root.providerTurn.id,
            requestRuntimeRestart: true,
            replacementFollows: true,
          })
          .pipe(Effect.exit);
        assert.equal(rejected._tag, "Failure");
        const lastRoot = harness.events.findLast(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.id === root.providerTurn.id,
        );
        assert.equal(
          lastRoot?.type === "provider_turn.updated" && lastRoot.providerTurn.status,
          "running",
          "a native root end does not confirm that a retained terminal stopped",
        );
        assert.lengthOf(harness.terminalEvents(), 0);
        assert.isTrue(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([
    {
      name: "usage",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "replacement",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "provider_error",
    },
    {
      name: "late-reset",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "matching-details",
      code: "usageLimitExceeded",
      notification: true,
      expectedClass: "usage_limit",
    },
    {
      name: "deferred-reset",
      code: "usageLimitExceeded",
      notification: false,
      expectedClass: "usage_limit",
    },
    { name: "retry", code: "usageLimitExceeded", notification: true, expectedClass: "usage_limit" },
    {
      name: "exhausted-429",
      code: "responseTooManyFailedAttempts",
      errorInfo: { responseTooManyFailedAttempts: { httpStatusCode: 429 } },
      notification: false,
      expectedClass: "usage_limit",
    },
    {
      name: "exhausted-500",
      code: "responseTooManyFailedAttempts",
      errorInfo: { responseTooManyFailedAttempts: { httpStatusCode: 500 } },
      notification: false,
      expectedClass: "provider_error",
    },
    {
      name: "chatgpt-workspace-limit",
      code: "other",
      details: "subscription_sharing_workplace_individual_usage_limit_exceeded",
      notification: true,
      expectedClass: "provider_error",
    },
  ] as const)("classifies Codex terminal failures from $name evidence", (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativeThreadId = `native-limit-${scenario.name}`;
        const nativeTurnId = `turn-limit-${scenario.name}`;
        const message = "Provider stopped this request.";
        const errorInfo = "errorInfo" in scenario ? scenario.errorInfo : scenario.code;
        const resetAt = "2033-05-19T07:20:00.000Z";
        const snapshot = {
          type: "emit_inbound" as const,
          label: "account/rateLimits/updated",
          frame: {
            method: "account/rateLimits/updated",
            params: {
              rateLimits: {
                limitId: "codex",
                primary: { usedPercent: 100, resetsAt: 2000100000, windowDurationMins: 300 },
              },
            },
          },
        };
        const transcript = makeCodexReplayTranscript({
          scenario: `codex-limit-${scenario.name}`,
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
            ...(scenario.name === "deferred-reset" ? [snapshot] : []),
            ...(scenario.name === "deferred-reset"
              ? [
                  {
                    type: "emit_inbound" as const,
                    label: "item/completed/subAgentActivity-started",
                    frame: {
                      method: "item/completed",
                      params: {
                        threadId: nativeThreadId,
                        turnId: nativeTurnId,
                        item: {
                          type: "subAgentActivity",
                          id: "limit-child-spawn",
                          kind: "started",
                          agentThreadId: "native-limit-child",
                          agentPath: "/root/limit_child",
                        },
                      },
                    },
                  },
                  {
                    type: "emit_inbound" as const,
                    label: "turn/started/child",
                    frame: {
                      method: "turn/started",
                      params: {
                        threadId: "native-limit-child",
                        turn: makeCodexReplayTurn({
                          id: "limit-child-turn",
                          status: "inProgress",
                        }),
                      },
                    },
                  },
                ]
              : []),
            ...(scenario.notification
              ? [
                  {
                    type: "emit_inbound" as const,
                    label: "error",
                    frame: {
                      method: "error",
                      params: {
                        threadId: nativeThreadId,
                        turnId: nativeTurnId,
                        willRetry: scenario.name === "retry",
                        error: {
                          message,
                          codexErrorInfo: errorInfo,
                          additionalDetails:
                            "details" in scenario
                              ? scenario.details
                              : scenario.name === "matching-details"
                                ? "Detailed provider allowance explanation."
                                : null,
                        },
                      },
                    },
                  },
                ]
              : []),
            {
              type: "emit_inbound",
              label: "turn/completed",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: nativeThreadId,
                  turn: {
                    ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                    error: {
                      message: scenario.name === "replacement" ? "A different failure." : message,
                      ...(scenario.notification &&
                      scenario.name !== "matching-details" &&
                      !("details" in scenario)
                        ? {}
                        : { codexErrorInfo: errorInfo }),
                    },
                  },
                },
              },
            },
            ...(scenario.name === "late-reset" ? [snapshot] : []),
            ...(scenario.name === "deferred-reset"
              ? [
                  {
                    ...snapshot,
                    frame: {
                      method: "account/rateLimits/updated",
                      params: {
                        rateLimits: {
                          limitId: "codex",
                          primary: {
                            usedPercent: 100,
                            resetsAt: 2000200000,
                            windowDurationMins: 300,
                          },
                        },
                      },
                    },
                  },
                  {
                    type: "emit_inbound" as const,
                    label: "turn/completed/child",
                    frame: {
                      method: "turn/completed",
                      params: {
                        threadId: "native-limit-child",
                        turn: makeCodexReplayTurn({
                          id: "limit-child-turn",
                          status: "completed",
                        }),
                      },
                    },
                  },
                ]
              : []),
          ],
        });
        const resetReceipt = yield* Deferred.make<void>();
        const harness = yield* makeCodexReplayHarness(transcript, (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.failure.resetAt === resetAt
            ? Deferred.succeed(resetReceipt, undefined)
            : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            text: "Continue.",
            attemptId: RunAttemptId.make(`attempt-limit-${scenario.name}`),
          }),
        );
        yield* harness.firstTerminal;
        const terminal = harness.terminalEvents()[0];
        assert.equal(terminal?.status, "failed");
        if (terminal?.status !== "failed") return;
        assert.equal(terminal.failure.class, scenario.expectedClass);
        assert.equal(terminal.threadDisposition, "reusable");
        if (scenario.name === "deferred-reset") assert.equal(terminal.failure.resetAt, resetAt);
        if (scenario.name === "matching-details")
          assert.equal(terminal.failure.message, "Detailed provider allowance explanation.");
        if (scenario.name === "late-reset") {
          yield* Deferred.await(resetReceipt);
          const item = harness.events.find(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.failure.resetAt === resetAt,
          );
          assert.isDefined(item);
        }
        if (scenario.name === "retry") assert.equal(terminal.retry?.attempt, 1);
        if (scenario.name === "chatgpt-workspace-limit") {
          assert.equal(terminal.failure.code, scenario.details);
          assert.include(terminal.failure.message, "workspace admin");
        }
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  // regression: #17501 Codex 429 retry exhaustion classifies as a usage limit
  it.effect.each([
    {
      notificationStatus: 500,
      completionStatus: 429,
      willRetry: false,
      expectedClass: "usage_limit",
    },
    {
      notificationStatus: 429,
      completionStatus: 500,
      willRetry: true,
      expectedClass: "provider_error",
    },
    {
      notificationStatus: 429,
      completionStatus: null,
      willRetry: false,
      expectedClass: "usage_limit",
    },
    {
      notificationStatus: 429,
      completionStatus: null,
      willRetry: true,
      expectedClass: "usage_limit",
    },
  ] as const)(
    "honors completion HTTP status from $notificationStatus to $completionStatus (retry: $willRetry)",
    ({ notificationStatus, completionStatus, expectedClass, willRetry }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const name = `${notificationStatus}-${completionStatus}-${willRetry}`;
          const nativeThreadId = `native-exhaustion-status-${name}`;
          const nativeTurnId = `turn-exhaustion-status-${name}`;
          const message = "Provider stopped this request.";
          const additionalDetails = "Detailed notification evidence.";
          const transcript = makeCodexReplayTranscript({
            scenario: `codex-exhaustion-status-${name}`,
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt: "Continue." }),
              {
                type: "emit_inbound",
                label: "error",
                frame: {
                  method: "error",
                  params: {
                    threadId: nativeThreadId,
                    turnId: nativeTurnId,
                    willRetry,
                    error: {
                      message,
                      codexErrorInfo: {
                        responseTooManyFailedAttempts: { httpStatusCode: notificationStatus },
                      },
                      additionalDetails,
                    },
                  },
                },
              },
              {
                type: "emit_inbound",
                label: "turn/completed",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: {
                      ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                      error: {
                        message,
                        codexErrorInfo: {
                          responseTooManyFailedAttempts: { httpStatusCode: completionStatus },
                        },
                      },
                    },
                  },
                },
              },
            ],
          });
          const harness = yield* makeCodexReplayHarness(transcript);
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              text: "Continue.",
              attemptId: RunAttemptId.make(`attempt-exhaustion-status-${name}`),
            }),
          );
          yield* harness.firstTerminal;
          const terminal = harness.terminalEvents()[0];
          assert.equal(terminal?.status, "failed");
          if (terminal?.status !== "failed") return;
          assert.equal(terminal.failure.class, expectedClass);
          assert.equal(terminal.failure.code, "responseTooManyFailedAttempts");
          assert.equal(terminal.threadDisposition, "reusable");
          const notificationClass = notificationStatus === 429 ? "usage_limit" : "provider_error";
          assert.equal(
            terminal.failure.message,
            expectedClass === notificationClass ? additionalDetails : message,
          );
          if (willRetry) assert.equal(terminal.retry?.attempt, 1);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const FAILED_SCENARIO = "codex-failed-mid-command";
  const FAILED_NATIVE_THREAD = "native-codex-failed-thread";
  const FAILED_NATIVE_TURN = "native-codex-failed-turn";
  const FAILED_COMMAND_ITEM = "exec-codex-failed-command";
  const FAILED_COMMAND = "sleep 30";
  const FAILED_PROMPT = "Run a command that will be abandoned when the turn fails.";

  const failedMidCommandTranscript = makeCodexReplayTranscript({
    scenario: FAILED_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: FAILED_NATIVE_THREAD,
        nativeTurnId: FAILED_NATIVE_TURN,
        prompt: FAILED_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/command",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "commandExecution",
              id: FAILED_COMMAND_ITEM,
              command: FAILED_COMMAND,
              cwd: "/workspace",
              processId: "99",
              source: "unifiedExecStartup",
              status: "inProgress",
              commandActions: [{ type: "unknown", command: FAILED_COMMAND }],
              aggregatedOutput: null,
              exitCode: null,
              durationMs: null,
            },
            threadId: FAILED_NATIVE_THREAD,
            turnId: FAILED_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: FAILED_NATIVE_THREAD,
            turn: {
              ...makeCodexReplayTurn({
                id: FAILED_NATIVE_TURN,
                status: "failed",
              }),
              error: { message: "provider failed mid-command" },
            },
          },
        },
      },
    ],
  });

  it.effect("terminalizes running command items before turn.terminal on failed turns", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(failedMidCommandTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-failed-mid-command"),
            text: FAILED_PROMPT,
          }),
        );
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "failed");

        const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
        const failedCommandIndex = harness.events.findIndex(
          (event, index) =>
            index < terminalIndex &&
            event.type === "turn_item.updated" &&
            event.turnItem.type === "command_execution" &&
            event.turnItem.status === "failed",
        );
        assert.isAtLeast(failedCommandIndex, 0);
        assert.isAbove(
          terminalIndex,
          failedCommandIndex,
          "failed-turn command terminalization must precede turn.terminal",
        );
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const MCP_APP_SCENARIO = "codex-mcp-app-capture";
  const MCP_APP_NATIVE_THREAD = "native-codex-mcp-app-thread";
  const MCP_APP_NATIVE_TURN = "native-codex-mcp-app-turn";
  const MCP_APP_ITEM = "mcp-weather-call";
  const MCP_APP_PROMPT = "Show the weather in Oslo.";
  const MCP_APP_RESOURCE = "ui://weather/dashboard";
  const MCP_APP_HTML = "<!doctype html><html><body><p>Weather</p><script>1</script></body></html>";
  const mcpAppToolItem = (status: "inProgress" | "completed") => ({
    type: "mcpToolCall",
    id: MCP_APP_ITEM,
    server: "weather",
    tool: "get_weather",
    status,
    arguments: { city: "Oslo" },
    mcpAppResourceUri: MCP_APP_RESOURCE,
    ...(status === "completed"
      ? { result: { content: [{ type: "text", text: "Sunny" }], structuredContent: { temp: 21 } } }
      : {}),
  });

  const mcpAppTranscript = makeCodexReplayTranscript({
    scenario: MCP_APP_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: MCP_APP_NATIVE_THREAD,
        nativeTurnId: MCP_APP_NATIVE_TURN,
        prompt: MCP_APP_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/app-tool",
        frame: {
          method: "item/started",
          params: {
            item: mcpAppToolItem("inProgress"),
            threadId: MCP_APP_NATIVE_THREAD,
            turnId: MCP_APP_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/app-tool",
        frame: {
          method: "item/completed",
          params: {
            item: mcpAppToolItem("completed"),
            threadId: MCP_APP_NATIVE_THREAD,
            turnId: MCP_APP_NATIVE_TURN,
            completedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: MCP_APP_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: MCP_APP_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      {
        type: "expect_outbound",
        label: "mcpServer/resource/read",
        frame: {
          id: 4,
          method: "mcpServer/resource/read",
          params: { threadId: MCP_APP_NATIVE_THREAD, server: "weather", uri: MCP_APP_RESOURCE },
        },
      },
      {
        type: "emit_inbound",
        label: "mcpServer/resource/read",
        frame: {
          id: 4,
          result: {
            contents: [
              {
                uri: MCP_APP_RESOURCE,
                mimeType: "text/html;profile=mcp-app",
                text: MCP_APP_HTML,
                _meta: { ui: { csp: { connectDomains: ["https://api.weather.test"] } } },
              },
            ],
          },
        },
      },
    ],
  });

  it.effect("captures an MCP app's resource and holds the turn open until it lands", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const appCaptured = yield* Deferred.make<OrchestrationV2TurnItem>();
        const harness = yield* makeCodexReplayHarness(mcpAppTranscript, (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.status === "completed"
            ? Deferred.succeed(appCaptured, event.turnItem)
            : Effect.void,
        );
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-mcp-app"),
            text: MCP_APP_PROMPT,
          }),
        );
        const item = yield* Deferred.await(appCaptured);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");

        const completedRows = harness.events.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "completed",
        );
        assert.lengthOf(completedRows, 1);

        assert.equal(item.type, "dynamic_tool");
        const output = item.type === "dynamic_tool" ? (item.output as Record<string, unknown>) : {};
        const reference = readMcpAppReference(output[MCP_APP_OUTPUT_KEY]);
        assert.deepEqual(
          { ...reference, attachmentId: undefined },
          {
            attachmentId: undefined,
            server: "weather",
            tool: "get_weather",
            resourceUri: MCP_APP_RESOURCE,
            csp: { connectDomains: ["https://api.weather.test"] },
          },
        );
        assert.deepEqual(output.result, {
          content: [{ type: "text", text: "Sunny" }],
          structuredContent: { temp: 21 },
        });

        const fileSystem = yield* FileSystem.FileSystem;
        const stored = yield* fileSystem.readFileString(
          resolveAttachmentPathById({
            attachmentsDir: harness.serverConfig.attachmentsDir,
            attachmentId: reference!.attachmentId,
          })!,
        );
        assert.include(stored, "connect-src https://api.weather.test");
        assert.isBelow(stored.indexOf("Content-Security-Policy"), stored.indexOf("<script>"));
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const ORPHAN_WAIT_SCENARIO = "codex-orphaned-dynamic-tool";
  const ORPHAN_WAIT_NATIVE_THREAD = "native-codex-orphan-wait-thread";
  const ORPHAN_WAIT_NATIVE_TURN = "native-codex-orphan-wait-turn";
  const ORPHAN_WAIT_ITEM = "exec-4669f3bb-78c9-4af1-b44e-daa340d2c538";
  const PERSISTENT_MONITOR_ITEM = "exec-persistent-monitor";
  const COMPLETED_WAIT_ITEM = "exec-completed-wait";
  const ORPHAN_WAIT_PROMPT = "Wait on two nested tasks, then finish.";

  const orphanedDynamicToolTranscript = makeCodexReplayTranscript({
    scenario: ORPHAN_WAIT_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: ORPHAN_WAIT_NATIVE_THREAD,
        nativeTurnId: ORPHAN_WAIT_NATIVE_TURN,
        prompt: ORPHAN_WAIT_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/started/completed-wait",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "mcpToolCall",
              id: COMPLETED_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "inProgress",
              arguments: { threadId: "thread:completed-wait", timeoutMs: 30000 },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622440500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/completed-wait",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "mcpToolCall",
              id: COMPLETED_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "completed",
              arguments: { threadId: "thread:completed-wait", timeoutMs: 30000 },
              result: { content: [{ type: "text", text: "idle" }] },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            completedAtMs: 1782622441500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/orphan-wait",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "mcpToolCall",
              id: ORPHAN_WAIT_ITEM,
              server: "t3-code",
              tool: "t3_thread_wait",
              status: "inProgress",
              arguments: {
                threadId:
                  "thread:delegated-task:command%3Amcp%3Aaafffab1-e811-458a-ae83-558e542c61ff%3Adelegate-task%3Areview-mobile-reconnect-opus-20260815",
                timeoutMs: 30000,
              },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622442500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/started/persistent-monitor",
        frame: {
          method: "item/started",
          params: {
            item: {
              type: "dynamicToolCall",
              id: PERSISTENT_MONITOR_ITEM,
              namespace: "grok",
              tool: "monitor",
              status: "inProgress",
              arguments: { persistent: true, command: "tail -f" },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            startedAtMs: 1782622443500,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed",
        frame: {
          method: "turn/completed",
          params: {
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turn: makeCodexReplayTurn({
              id: ORPHAN_WAIT_NATIVE_TURN,
              status: "completed",
            }),
          },
        },
      },
      {
        type: "emit_inbound",
        label: "item/completed/persistent-monitor",
        afterMs: 30_000,
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "dynamicToolCall",
              id: PERSISTENT_MONITOR_ITEM,
              namespace: "grok",
              tool: "monitor",
              status: "completed",
              arguments: { persistent: true, command: "tail -f" },
              result: { content: [{ type: "text", text: "stopped" }] },
            },
            threadId: ORPHAN_WAIT_NATIVE_THREAD,
            turnId: ORPHAN_WAIT_NATIVE_TURN,
            completedAtMs: 1782622473500,
          },
        },
      },
    ],
  });

  it.effect(
    "terminalizes leftover nonpersistent dynamic tools when a completed turn never closes them",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const monitorCompleted = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(orphanedDynamicToolTranscript, (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM &&
            event.turnItem.status === "completed"
              ? Deferred.succeed(monitorCompleted, undefined)
              : Effect.void,
          );
          const now = yield* DateTime.now;

          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-orphan-wait"),
              text: ORPHAN_WAIT_PROMPT,
            }),
          );
          yield* harness.firstTerminal;
          assert.equal(harness.terminalEvents()[0]?.status, "completed");

          const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");
          const cancelledWait = harness.events.find(
            (event, index) =>
              index < terminalIndex &&
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === ORPHAN_WAIT_ITEM &&
              event.turnItem.status === "cancelled",
          );
          assert.isDefined(cancelledWait);
          assert.isAbove(
            terminalIndex,
            harness.events.indexOf(cancelledWait!),
            "orphaned wait terminalization must precede turn.terminal",
          );

          const completedWaitStatuses = new Set(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === COMPLETED_WAIT_ITEM
                ? [event.turnItem.status]
                : [],
            ),
          );
          assert.isTrue(
            completedWaitStatuses.has("completed"),
            "the wait that received item/completed must stay completed",
          );
          assert.isFalse(
            completedWaitStatuses.has("cancelled"),
            "a completed wait must not be rewritten as cancelled",
          );

          const persistentMonitorStatuses = new Set(
            harness.events.flatMap((event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM
                ? [event.turnItem.status]
                : [],
            ),
          );
          assert.isTrue(persistentMonitorStatuses.has("running"));
          assert.isFalse(
            persistentMonitorStatuses.has("cancelled"),
            "persistent monitors must remain running after the root turn completes",
          );
          assert.isTrue(
            yield* harness.hasPendingBackgroundWork,
            "persistent dynamic tools must keep the session residency pin until they complete",
          );

          yield* TestClock.adjust("30 seconds");
          yield* Deferred.await(monitorCompleted);
          const lateMonitorUpdateIndex = harness.events.findIndex(
            (event, index) =>
              index > terminalIndex &&
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.nativeItemRef?.nativeId === PERSISTENT_MONITOR_ITEM &&
              event.turnItem.status === "completed",
          );
          assert.isAbove(
            lateMonitorUpdateIndex,
            terminalIndex,
            "persistent tool completion must follow turn.terminal",
          );
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const RESUME_SCENARIO = "codex-resume-subagent";
  const RESUME_NATIVE_THREAD = "native-codex-resume-thread";
  const RESUME_NATIVE_TURN = "native-codex-resume-root-turn";
  const RESUME_CHILD_THREAD = "native-codex-resume-child-thread";
  const RESUME_CHILD_TURN_1 = "native-codex-resume-child-turn-1";
  const RESUME_CHILD_TURN_2 = "native-codex-resume-child-turn-2";
  const RESUME_PROMPT = "Spawn a sub-agent, nudge it, and reply NUDGED.";

  const childAgentMessage = (input: {
    readonly id: string;
    readonly text: string;
    readonly turnId: string;
    readonly completedAtMs: number;
    readonly afterMs?: number;
    readonly omitPhase?: boolean;
  }): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `item/completed/${input.id}`,
    ...(input.afterMs === undefined ? {} : { afterMs: input.afterMs }),
    frame: {
      method: "item/completed",
      params: {
        item: {
          type: "agentMessage",
          id: input.id,
          text: input.text,
          ...(input.omitPhase ? {} : { phase: "final_answer" as const }),
          memoryCitation: null,
        },
        threadId: RESUME_CHILD_THREAD,
        turnId: input.turnId,
        completedAtMs: input.completedAtMs,
      },
    },
  });

  const childTurnStarted = (
    turnId: string,
    afterMs?: number,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `turn/started/${turnId}`,
    ...(afterMs === undefined ? {} : { afterMs }),
    frame: {
      method: "turn/started",
      params: {
        threadId: RESUME_CHILD_THREAD,
        turn: {
          ...makeCodexReplayTurn({ id: turnId, status: "inProgress" }),
          startedAt: turnId === RESUME_CHILD_TURN_2 ? 1782622470 : 1782622440,
        },
      },
    },
  });

  const childTurnCompleted = (
    turnId: string,
    afterMs?: number,
  ): CodexReplay.CodexAppServerReplayEntry => ({
    type: "emit_inbound",
    label: `turn/completed/${turnId}`,
    ...(afterMs === undefined ? {} : { afterMs }),
    frame: {
      method: "turn/completed",
      params: {
        threadId: RESUME_CHILD_THREAD,
        turn: makeCodexReplayTurn({ id: turnId, status: "completed" }),
      },
    },
  });

  const resumeSubagentTranscript = makeCodexReplayTranscript({
    scenario: RESUME_SCENARIO,
    entries: [
      ...codexReplayPreamble({
        nativeThreadId: RESUME_NATIVE_THREAD,
        nativeTurnId: RESUME_NATIVE_TURN,
        prompt: RESUME_PROMPT,
      }),
      {
        type: "emit_inbound",
        label: "item/completed/subAgentActivity-started",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "subAgentActivity",
              id: "call-codex-resume-spawn",
              kind: "started",
              agentThreadId: RESUME_CHILD_THREAD,
              agentPath: "/root/resume_agent",
            },
            threadId: RESUME_NATIVE_THREAD,
            turnId: RESUME_NATIVE_TURN,
            completedAtMs: 1782622441000,
          },
        },
      },
      childTurnStarted(RESUME_CHILD_TURN_1),
      childAgentMessage({
        id: "child-first-answer",
        text: "CODEX_FIRST_DONE",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442000,
      }),
      childAgentMessage({
        id: "child-first-answer-empty",
        text: "",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442001,
        omitPhase: true,
      }),
      childAgentMessage({
        id: "child-first-answer-duplicate",
        text: "CODEX_FIRST_DONE",
        turnId: RESUME_CHILD_TURN_1,
        completedAtMs: 1782622442002,
      }),
      childTurnCompleted(RESUME_CHILD_TURN_1, 100),
      {
        type: "emit_inbound",
        label: "item/completed/root-answer",
        frame: {
          method: "item/completed",
          params: {
            item: {
              type: "agentMessage",
              id: "root-answer-resume",
              text: "NUDGED",
              phase: "final_answer",
              memoryCitation: null,
            },
            threadId: RESUME_NATIVE_THREAD,
            turnId: RESUME_NATIVE_TURN,
            completedAtMs: 1782622443000,
          },
        },
      },
      {
        type: "emit_inbound",
        label: "turn/completed/root",
        frame: {
          method: "turn/completed",
          params: {
            threadId: RESUME_NATIVE_THREAD,
            turn: makeCodexReplayTurn({ id: RESUME_NATIVE_TURN, status: "completed" }),
          },
        },
      },
      childTurnStarted(RESUME_CHILD_TURN_2, 30_000),
      childAgentMessage({
        id: "child-resume-answer",
        text: "CODEX_RESUME_DONE",
        turnId: RESUME_CHILD_TURN_2,
        completedAtMs: 1782622480000,
        afterMs: 30_000,
      }),
      childTurnCompleted(RESUME_CHILD_TURN_2),
    ],
  });

  it.effect("wakes the parent when a native child finishes after the parent's turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entries = resumeSubagentTranscript.entries;
        const labelIndex = (label: string) =>
          entries.findIndex((entry) => entry.type === "emit_inbound" && entry.label === label);
        const rootCompleted = labelIndex("turn/completed/root");
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: RESUME_SCENARIO,
            entries: [
              ...entries.slice(0, labelIndex(`turn/started/${RESUME_CHILD_TURN_1}`) + 1),
              ...entries.slice(rootCompleted - 1, rootCompleted + 1),
              childAgentMessage({
                id: "child-late-answer",
                text: "NATIVE_CHILD_16900_DONE",
                turnId: RESUME_CHILD_TURN_1,
                completedAtMs: 1782622480000,
                afterMs: 60_000,
              }),
              childTurnCompleted(RESUME_CHILD_TURN_1),
            ],
          }),
        );

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-late-child"),
            text: RESUME_PROMPT,
          }),
        );
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        assert.lengthOf(harness.continuationRequests, 0);

        yield* TestClock.adjust("60 seconds");
        yield* harness.until(
          () => harness.subagentUpdates().at(-1)?.subagent.result === "NATIVE_CHILD_16900_DONE",
        );
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "completed");
        assert.lengthOf(harness.continuationRequests, 1);
        assert.include(harness.continuationRequests[0]?.detail, "NATIVE_CHILD_16900_DONE");
        assert.equal(harness.continuationRequests[0]?.delivery, "message_text");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  const lateChildPrefix = () => {
    const entries = resumeSubagentTranscript.entries;
    const labelIndex = (label: string) =>
      entries.findIndex((entry) => entry.type === "emit_inbound" && entry.label === label);
    const rootCompleted = labelIndex("turn/completed/root");
    return {
      spawned: entries.slice(0, labelIndex(`turn/started/${RESUME_CHILD_TURN_1}`) + 1),
      rootFinished: entries.slice(rootCompleted - 1, rootCompleted + 1),
    };
  };

  it.effect.each(["completed", "failed", "interrupted", "starting", "replied-last"] as const)(
    "handles a late native child after the capacity retry %s",
    (parentStatus) =>
      Effect.scoped(
        Effect.gen(function* () {
          const retriedTurnId = "native-capacity-recovered-parent";
          const retrying = yield* Deferred.make<void>();
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...lateChildPrefix().spawned,
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turn: {
                    ...makeCodexReplayTurn({ id: RESUME_NATIVE_TURN, status: "failed" }),
                    error: {
                      message: "Selected model is at capacity.",
                      codexErrorInfo: "serverOverloaded",
                    },
                  },
                },
              },
            },
            {
              type: "expect_outbound",
              frame: {
                id: 4,
                method: "turn/start",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  input: [],
                  cwd: "/workspace",
                  model: "gpt-5.4",
                  approvalPolicy: "never",
                  approvalsReviewer: "user",
                  sandboxPolicy: { type: "dangerFullAccess" },
                  summary: "detailed",
                  collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                },
              },
            },
            ...(parentStatus === "starting" ? [childTurnCompleted(RESUME_CHILD_TURN_1)] : []),
            {
              type: "emit_inbound",
              frame: {
                id: 4,
                result: {
                  turn: makeCodexReplayTurn({ id: retriedTurnId, status: "inProgress" }),
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/started",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turn: makeCodexReplayTurn({ id: retriedTurnId, status: "inProgress" }),
                },
              },
            },
            {
              type: "emit_inbound",
              frame: {
                method: "turn/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turn: {
                    ...makeCodexReplayTurn({
                      id: retriedTurnId,
                      status:
                        parentStatus === "starting" || parentStatus === "replied-last"
                          ? "completed"
                          : parentStatus,
                    }),
                    error:
                      parentStatus === "failed"
                        ? { message: "Recovery failed.", codexErrorInfo: "internalServerError" }
                        : null,
                  },
                },
              },
            },
            childAgentMessage({
              id: "child-after-capacity",
              text: "CAPACITY_CHILD_DONE",
              turnId: RESUME_CHILD_TURN_1,
              completedAtMs: 1782622480000,
              afterMs: 60_000,
            }),
            childTurnCompleted(RESUME_CHILD_TURN_1),
          ];
          if (parentStatus === "replied-last") {
            entries.push(...entries.splice(lateChildPrefix().spawned.length + 2, 1));
          }
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: "capacity-late-child", entries }),
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "error" &&
              event.turnItem.status === "running"
                ? Deferred.succeed(retrying, undefined)
                : Effect.void,
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("capacity-late-child"),
              text: RESUME_PROMPT,
            }),
          );
          yield* Deferred.await(retrying);
          yield* TestClock.adjust("2 seconds");
          if (parentStatus === "interrupted") assert.isEmpty(harness.terminalEvents());
          else {
            yield* harness.awaitTerminal(1);
            const terminal = harness.terminalEvents()[0];
            if (terminal?.status === "failed") assert.isTrue(terminal.childTurnsContinue);
          }
          yield* TestClock.adjust("60 seconds");
          yield* harness.until(
            () => harness.subagentUpdates().at(-1)?.subagent.status === "completed",
          );
          yield* harness.awaitTerminal(1);
          assert.lengthOf(
            harness.continuationRequests,
            parentStatus === "completed" || parentStatus === "replied-last" ? 1 : 0,
          );
          if (parentStatus === "completed" || parentStatus === "replied-last")
            assert.include(harness.continuationRequests[0]?.detail, "CAPACITY_CHILD_DONE");
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("stops native children when capacity retry startup fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const retrying = yield* Deferred.make<void>();
        const starting = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const interrupts: unknown[] = [];
        let starts = 0;
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "capacity-start-fails-stop-child",
            entries: [
              ...lateChildPrefix().spawned,
              {
                type: "emit_inbound",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: RESUME_NATIVE_THREAD,
                    turn: {
                      ...makeCodexReplayTurn({ id: RESUME_NATIVE_TURN, status: "failed" }),
                      error: { message: "At capacity", codexErrorInfo: "serverOverloaded" },
                    },
                  },
                },
              },
              {
                type: "expect_outbound",
                frame: {
                  id: 4,
                  method: "turn/start",
                  params: {
                    threadId: RESUME_NATIVE_THREAD,
                    input: [],
                    cwd: "/workspace",
                    model: "gpt-5.4",
                    approvalPolicy: "never",
                    approvalsReviewer: "user",
                    sandboxPolicy: { type: "dangerFullAccess" },
                    summary: "detailed",
                    collaborationMode: { mode: "default", settings: { model: "gpt-5.4" } },
                  },
                },
              },
              {
                type: "emit_inbound",
                frame: { id: 4, error: { code: -32603, message: "Start failed" } },
              },
              {
                type: "expect_outbound",
                frame: {
                  id: 5,
                  method: "turn/interrupt",
                  params: { threadId: RESUME_CHILD_THREAD, turnId: RESUME_CHILD_TURN_1 },
                },
              },
              { type: "emit_inbound", frame: { id: 5, result: {} } },
              {
                type: "emit_inbound",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: RESUME_CHILD_THREAD,
                    turn: makeCodexReplayTurn({ id: RESUME_CHILD_TURN_1, status: "interrupted" }),
                  },
                },
              },
            ],
          }),
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.status === "running"
              ? Deferred.succeed(retrying, undefined)
              : Effect.void,
          (method, params) =>
            Effect.gen(function* () {
              if (method === "turn/interrupt") interrupts.push(params);
              if (method === "turn/start" && ++starts === 2) {
                yield* Deferred.succeed(starting, undefined);
                yield* Deferred.await(release);
              }
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("capacity-start-fails-stop-child"),
            text: RESUME_PROMPT,
          }),
        );
        yield* Deferred.await(retrying);
        const root = harness.events.find((event) => event.type === "provider_turn.updated");
        assert.equal(root?.type, "provider_turn.updated");
        if (root?.type !== "provider_turn.updated") return;
        yield* TestClock.adjust("2 seconds");
        yield* Deferred.await(starting);
        const stopping = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: root.providerTurn.id,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(stopping);
        assert.lengthOf(interrupts, 1);
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "interrupted");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("names a late child spawned without a prompt in its wake", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { spawned, rootFinished } = lateChildPrefix();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: RESUME_SCENARIO,
            entries: [
              ...spawned.map((entry) =>
                entry.type === "emit_inbound" &&
                entry.label === "item/completed/subAgentActivity-started"
                  ? {
                      ...entry,
                      frame: {
                        method: "item/completed",
                        params: {
                          item: {
                            type: "collabAgentToolCall",
                            id: "call-codex-resume-spawn",
                            tool: "spawnAgent",
                            status: "completed",
                            senderThreadId: RESUME_NATIVE_THREAD,
                            receiverThreadIds: [RESUME_CHILD_THREAD],
                            agentsStates: {},
                          },
                          threadId: RESUME_NATIVE_THREAD,
                          turnId: RESUME_NATIVE_TURN,
                          completedAtMs: 1782622441000,
                        },
                      },
                    }
                  : entry,
              ),
              ...rootFinished,
              childAgentMessage({
                id: "child-late-answer",
                text: "UNPROMPTED_CHILD_DONE",
                turnId: RESUME_CHILD_TURN_1,
                completedAtMs: 1782622480000,
                afterMs: 60_000,
              }),
              childTurnCompleted(RESUME_CHILD_TURN_1),
            ],
          }),
        );

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-unprompted-child"),
            text: RESUME_PROMPT,
          }),
        );
        yield* harness.awaitTerminal(1);
        yield* TestClock.adjust("60 seconds");
        yield* harness.until(() => harness.continuationRequests.length > 0);
        const subagent = harness.subagentUpdates().at(-1)?.subagent;
        assert.equal(subagent?.prompt, "");
        assert.equal(
          harness.continuationRequests[0]?.detail,
          `Subagent completed: ${subagent?.id}\n\nResult:\nUNPROMPTED_CHILD_DONE`,
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("wakes the root only when its direct child finishes, not a grandchild", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const grandchildThread = "native-codex-resume-grandchild-thread";
        const grandchildTurn = "native-codex-resume-grandchild-turn";
        const { spawned, rootFinished } = lateChildPrefix();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: RESUME_SCENARIO,
            entries: [
              ...spawned,
              {
                type: "emit_inbound",
                label: "item/completed/grandchild-spawn",
                frame: {
                  method: "item/completed",
                  params: {
                    item: {
                      type: "subAgentActivity",
                      id: "call-codex-grandchild-spawn",
                      kind: "started",
                      agentThreadId: grandchildThread,
                      agentPath: "/root/resume_agent/grandchild",
                    },
                    threadId: RESUME_CHILD_THREAD,
                    turnId: RESUME_CHILD_TURN_1,
                    completedAtMs: 1782622441500,
                  },
                },
              },
              {
                type: "emit_inbound",
                label: "turn/started/grandchild",
                frame: {
                  method: "turn/started",
                  params: {
                    threadId: grandchildThread,
                    turn: makeCodexReplayTurn({ id: grandchildTurn, status: "inProgress" }),
                  },
                },
              },
              ...rootFinished,
              {
                type: "emit_inbound",
                label: "item/completed/grandchild-answer",
                afterMs: 30_000,
                frame: {
                  method: "item/completed",
                  params: {
                    item: {
                      type: "agentMessage",
                      id: "grandchild-answer",
                      text: "GRANDCHILD_DONE",
                      phase: "final_answer",
                      memoryCitation: null,
                    },
                    threadId: grandchildThread,
                    turnId: grandchildTurn,
                    completedAtMs: 1782622470000,
                  },
                },
              },
              {
                type: "emit_inbound",
                label: "turn/completed/grandchild",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: grandchildThread,
                    turn: makeCodexReplayTurn({ id: grandchildTurn, status: "completed" }),
                  },
                },
              },
              childAgentMessage({
                id: "child-late-answer",
                text: "CHILD_AFTER_GRANDCHILD",
                turnId: RESUME_CHILD_TURN_1,
                completedAtMs: 1782622500000,
                afterMs: 30_000,
              }),
              childTurnCompleted(RESUME_CHILD_TURN_1),
            ],
          }),
        );

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("attempt-codex-late-grandchild"),
            text: RESUME_PROMPT,
          }),
        );
        yield* harness.awaitTerminal(1);

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() =>
          harness.subagentUpdates().some((event) => event.subagent.result === "GRANDCHILD_DONE"),
        );
        assert.lengthOf(harness.continuationRequests, 0);

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() => harness.continuationRequests.length > 0);
        assert.lengthOf(harness.continuationRequests, 1);
        assert.include(harness.continuationRequests[0]?.detail, "CHILD_AFTER_GRANDCHILD");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect("leaves a late child's result to the root's next turn when one is running", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const secondPrompt = "Wait for the sub-agent.";
        const secondTurn = "native-codex-resume-root-turn-2";
        const { spawned, rootFinished } = lateChildPrefix();
        // Reuse the preamble's turn/start exchange under the session's next request id.
        const secondTurnStart = codexReplayPreamble({
          nativeThreadId: RESUME_NATIVE_THREAD,
          nativeTurnId: secondTurn,
          prompt: secondPrompt,
        })
          .slice(-3)
          .map((entry) =>
            entry.type !== "runtime_exit" &&
            typeof entry.frame === "object" &&
            entry.frame !== null &&
            "id" in entry.frame
              ? { ...entry, frame: { ...entry.frame, id: 4 } }
              : entry,
          );
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: RESUME_SCENARIO,
            entries: [
              ...spawned,
              ...rootFinished,
              ...secondTurnStart,
              childAgentMessage({
                id: "child-late-answer",
                text: "CHILD_DURING_SECOND_TURN",
                turnId: RESUME_CHILD_TURN_1,
                completedAtMs: 1782622480000,
              }),
              childTurnCompleted(RESUME_CHILD_TURN_1),
              {
                type: "emit_inbound",
                label: "turn/completed/root-2",
                frame: {
                  method: "turn/completed",
                  params: {
                    threadId: RESUME_NATIVE_THREAD,
                    turn: makeCodexReplayTurn({ id: secondTurn, status: "completed" }),
                  },
                },
              },
            ],
          }),
        );

        const turnInput = (index: number, text: string) =>
          Effect.gen(function* () {
            return {
              ...makeCodexTestTurnInput({
                threadId: harness.threadId,
                providerThread: harness.providerThread,
                now: yield* DateTime.now,
                attemptId: RunAttemptId.make(`attempt-codex-active-root-${index}`),
                text,
              }),
              runOrdinal: index,
              providerTurnOrdinal: index,
            };
          });
        yield* harness.runtime.startTurn(yield* turnInput(1, RESUME_PROMPT));
        yield* harness.awaitTerminal(1);
        yield* harness.runtime.startTurn(yield* turnInput(2, secondPrompt));
        yield* harness.awaitTerminal(2);
        assert.equal(harness.subagentUpdates().at(-1)?.subagent.result, "CHILD_DURING_SECOND_TURN");
        assert.lengthOf(harness.continuationRequests, 0);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each(["model/rerouted"] as const)(
    "keeps %s child metadata when an older lookup finishes later",
    (method) =>
      Effect.scoped(
        Effect.gen(function* () {
          const releaseMetadata = yield* Deferred.make<void>();
          const observed = yield* Deferred.make<void>();
          const model = "gpt-5.6-sol";
          const notification: CodexReplay.CodexAppServerReplayEntry = {
            type: "emit_inbound",
            frame: {
              method,
              params: {
                threadId: RESUME_CHILD_THREAD,
                turnId: RESUME_CHILD_TURN_1,
                fromModel: "gpt-6-astra",
                toModel: model,
                reason: "highRiskCyberActivity",
              },
            },
          };
          const initialSettings: CodexReplay.CodexAppServerReplayEntry = {
            type: "emit_inbound",
            frame: {
              method: "thread/settings/updated",
              params: {
                threadId: RESUME_CHILD_THREAD,
                threadSettings: {
                  model: "gpt-6-astra",
                  effort: "low",
                  serviceTier: "ultrafast",
                  modelProvider: "openai",
                  cwd: "/workspace",
                  approvalPolicy: "never",
                  approvalsReviewer: "auto_review",
                  collaborationMode: { mode: "default", settings: { model: "gpt-6-astra" } },
                  sandboxPolicy: { type: "dangerFullAccess" },
                },
              },
            },
          };
          const harness = yield* makeCodexReplayHarness(
            {
              ...resumeSubagentTranscript,
              entries: resumeSubagentTranscript.entries.flatMap((entry) =>
                entry.type === "emit_inbound" && entry.label === "turn/completed/root"
                  ? [entry, initialSettings, notification]
                  : [entry],
              ),
            },
            (event) =>
              event.type === "subagent.updated" && event.subagent.model === model
                ? Deferred.succeed(observed, undefined)
                : Effect.void,
            undefined,
            (threadId) =>
              Deferred.await(releaseMetadata).pipe(
                Effect.as({ thread: { id: threadId }, model: "gpt-6-astra" }),
              ),
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make("attempt-child-model-update"),
              text: RESUME_PROMPT,
            }),
          );
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(observed);
          assert.equal(harness.subagentUpdates().at(-1)?.subagent.status, "completed");
          yield* Deferred.succeed(releaseMetadata, undefined);
          yield* TestClock.adjust("30 seconds");
          assert.equal(harness.subagentUpdates().at(-1)?.subagent.model, model);
          assert.deepEqual(harness.subagentUpdates().at(-1)?.subagent.modelSelection?.options, [
            { id: "reasoningEffort", value: "low" },
            { id: "serviceTier", value: "ultrafast" },
          ]);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("preserves a subagent result across a trailing empty final and resume", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeCodexReplayHarness(resumeSubagentTranscript);
        const now = yield* DateTime.now;

        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("attempt-codex-resume"),
            text: RESUME_PROMPT,
          }),
        );
        yield* harness.until(() =>
          harness.subagentUpdates().some((event) => event.subagent.result === "CODEX_FIRST_DONE"),
        );
        assert.lengthOf(
          harness.subagentUpdates().filter((event) => event.subagent.result === "CODEX_FIRST_DONE"),
          1,
        );
        yield* TestClock.adjust("100 millis");
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "completed");
        const settledUpdates = harness.subagentUpdates();
        const firstCompletion = settledUpdates[settledUpdates.length - 1];
        assert.equal(firstCompletion?.subagent.status, "completed");
        assert.equal(firstCompletion?.subagent.result, "CODEX_FIRST_DONE");
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        const settledUpdateCount = settledUpdates.length;

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() => harness.subagentUpdates().length > settledUpdateCount);
        const reopened = harness.subagentUpdates()[settledUpdateCount];
        assert.equal(reopened?.subagent.status, "running");
        assert.equal(DateTime.toEpochMillis(reopened!.subagent.startedAt!), 1782622470000);
        assert.isNull(reopened!.subagent.completedAt);
        assert.isTrue(yield* harness.hasPendingBackgroundWork);

        yield* TestClock.adjust("30 seconds");
        yield* harness.until(() => {
          const updates = harness.subagentUpdates();
          const latest = updates[updates.length - 1];
          return (
            latest !== undefined &&
            latest.subagent.status === "completed" &&
            latest.subagent.result === "CODEX_RESUME_DONE"
          );
        });
        assert.isFalse(yield* harness.hasPendingBackgroundWork);
        assert.lengthOf(harness.terminalEvents(), 1);
        // The first child turn finished inside the root turn; only the resume wakes it.
        assert.lengthOf(harness.continuationRequests, 1);
        assert.include(harness.continuationRequests[0]?.detail, "CODEX_RESUME_DONE");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([
    ["shutdown", "cancelled"],
    ["activity-completed", "completed"],
    ["late-activity-completed", "completed"],
    ["stale-running", "completed"],
    ["duplicate-completed", "completed"],
  ] as const)(
    "normalizes subagent %s without losing its lifecycle",
    ([nativeStatus, expectedStatus]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const marker = yield* Deferred.make<void>();
          const firstCompletion = yield* Deferred.make<void>();
          const stateEntry = (
            status: string,
            id: string,
          ): Extract<CodexReplay.CodexAppServerReplayEntry, { type: "emit_inbound" }> => ({
            type: "emit_inbound",
            label: id,
            frame: {
              method: "item/completed",
              params: {
                threadId: RESUME_NATIVE_THREAD,
                turnId: RESUME_NATIVE_TURN,
                item: {
                  type: "collabAgentToolCall",
                  id,
                  tool: "listAgents",
                  status: "completed",
                  senderThreadId: RESUME_NATIVE_THREAD,
                  receiverThreadIds: [RESUME_CHILD_THREAD],
                  agentsStates: { [RESUME_CHILD_THREAD]: { status, message: null } },
                },
              },
            },
          });
          const entries: Array<CodexReplay.CodexAppServerReplayEntry> = [
            ...codexReplayPreamble({
              nativeThreadId: RESUME_NATIVE_THREAD,
              nativeTurnId: RESUME_NATIVE_TURN,
              prompt: RESUME_PROMPT,
            }),
            resumeSubagentTranscript.entries.find(
              (e) =>
                e.type === "emit_inbound" && e.label === "item/completed/subAgentActivity-started",
            )!,
          ];
          if (nativeStatus === "late-activity-completed") {
            entries.push(
              resumeSubagentTranscript.entries.find(
                (e) => e.type === "emit_inbound" && e.label === "turn/completed/root",
              )!,
            );
          }
          if (nativeStatus === "activity-completed" || nativeStatus === "late-activity-completed") {
            entries.push({
              type: "emit_inbound",
              label: "activity-done",
              frame: {
                method: "item/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turnId: RESUME_NATIVE_TURN,
                  item: {
                    type: "subAgentActivity",
                    id: "activity-done",
                    kind: "completed",
                    agentThreadId: RESUME_CHILD_THREAD,
                    agentPath: "/root/resume_agent",
                  },
                },
              },
            });
          } else if (nativeStatus === "stale-running" || nativeStatus === "duplicate-completed") {
            entries.push(stateEntry("completed", "child-completed"), {
              ...stateEntry(
                nativeStatus === "stale-running" ? "running" : "completed",
                "trailing-snapshot",
              ),
              afterMs: 100,
            });
          } else {
            entries.push(stateEntry(nativeStatus, "status-update"));
          }
          if (nativeStatus === "late-activity-completed") {
            entries.push({
              type: "emit_inbound",
              label: "late-marker",
              frame: {
                method: "turn/started",
                params: {
                  threadId: RESUME_CHILD_THREAD,
                  turn: makeCodexReplayTurn({ id: RESUME_CHILD_TURN_1, status: "inProgress" }),
                },
              },
            });
          } else {
            entries.push({
              type: "emit_inbound",
              label: "marker",
              frame: {
                method: "item/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turnId: RESUME_NATIVE_TURN,
                  item: {
                    type: "agentMessage",
                    id: "marker",
                    text: "LIFECYCLE_MARKER",
                    phase: "final_answer",
                    memoryCitation: null,
                  },
                },
              },
            });
          }
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({ scenario: `subagent-${nativeStatus}`, entries }),
            (event) =>
              (event.type === "message.updated" && event.message.text === "LIFECYCLE_MARKER") ||
              (nativeStatus === "late-activity-completed" &&
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === RESUME_CHILD_TURN_1)
                ? Deferred.succeed(marker, undefined)
                : event.type === "subagent.updated" && event.subagent.status === "completed"
                  ? Deferred.succeed(firstCompletion, undefined)
                  : Effect.void,
          );
          yield* harness.runtime.startTurn(
            makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now: yield* DateTime.now,
              attemptId: RunAttemptId.make(`subagent-${nativeStatus}`),
              text: RESUME_PROMPT,
            }),
          );
          if (nativeStatus === "stale-running" || nativeStatus === "duplicate-completed") {
            yield* Deferred.await(firstCompletion);
            yield* TestClock.adjust("100 millis");
          }
          yield* Deferred.await(marker);
          const latest = harness.subagentUpdates().at(-1)!.subagent;
          assert.equal(latest.status, expectedStatus);
          if (nativeStatus === "duplicate-completed") {
            const first = harness.subagentUpdates().find((e) => e.subagent.status === "completed")!;
            assert.equal(
              DateTime.toEpochMillis(latest.completedAt!),
              DateTime.toEpochMillis(first.subagent.completedAt!),
            );
          }
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  const codexReplayThreadResult = (input: {
    readonly nativeThreadId: string;
    readonly forkedFromId: string | null;
  }) => ({
    thread: {
      id: input.nativeThreadId,
      sessionId: input.nativeThreadId,
      forkedFromId: input.forkedFromId,
      preview: "",
      ephemeral: false,
      modelProvider: "openai",
      createdAt: 1782622440,
      updatedAt: 1782622440,
      status: { type: "idle" },
      path: `/tmp/${input.nativeThreadId}.jsonl`,
      cwd: "/workspace",
      cliVersion: "0.144.0",
      source: "vscode",
      threadSource: null,
      agentNickname: null,
      agentRole: null,
      gitInfo: null,
      name: null,
      turns: [],
    },
    model: "gpt-5.4",
    modelProvider: "openai",
    serviceTier: null,
    cwd: "/workspace",
    instructionSources: [],
    approvalPolicy: "on-request",
    approvalsReviewer: "user",
    sandbox: { type: "workspaceWrite", writableRoots: [], networkAccess: false },
    reasoningEffort: "medium",
  });

  const errorCauseChainText = (error: unknown): string =>
    error instanceof Error ? `${error.message} ${errorCauseChainText(error.cause)}` : String(error);

  it.effect.each(["existing", "managed"] as const)(
    "opens bare fork and resumed processes against the persisted native home owner: %s",
    (setupMode) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const serverConfig = yield* makeReplayServerConfig(`bare-owner-${setupMode}`);
        const sourceHome = yield* fs.makeTempDirectoryScoped();
        yield* fs.writeFileString(`${sourceHome}/auth.json`, "FAKE_TEST_CREDENTIAL");
        const settings = yield* decodeCodexSettings({
          setupMode,
          homePath: sourceHome,
        });
        const ownerId = ThreadId.make("bare-codex-owner");
        const forkId = ThreadId.make("bare-codex-fork");
        const chainId = ThreadId.make("bare-codex-fork-chain");
        const runtimePolicy = {
          ...CODEX_TEST_RUNTIME_POLICY,
          bare: { additions: [], nativeHomeOwnerThreadId: ownerId },
        };
        const home = `${serverConfig.stateDir}/bare/codex/codex/thread-9727934f3f9d2c89a76c8041c6b7e4b410f8345c0129f2185936004ea4316c9f`;
        const sourceNativeId = "native-bare-source";
        const forkNativeId = "native-bare-fork";
        const chainNativeId = "native-bare-fork-chain";
        const handshake = codexReplayPreamble({
          nativeThreadId: "unused",
          nativeTurnId: "unused",
          prompt: "unused",
        }).slice(0, 3);
        const params = CodexAdapterV2.codexThreadRuntimeParams({
          mcpSession: undefined,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy,
        });
        const nativeFork = (
          source: string,
          target: string,
        ): Array<CodexReplay.CodexAppServerReplayEntry> => [
          ...handshake,
          {
            type: "expect_outbound",
            frame: { id: 2, method: "thread/fork", params: { threadId: source, ...params } },
          },
          {
            type: "emit_inbound",
            frame: {
              id: 2,
              result: codexReplayThreadResult({ nativeThreadId: target, forkedFromId: source }),
            },
          },
        ];
        const transcripts = new Map<string, ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>>([
          ["source", []],
          [
            "fork",
            [
              ...nativeFork(sourceNativeId, forkNativeId),
              {
                type: "expect_outbound",
                frame: {
                  id: 3,
                  method: "mcpServerStatus/list",
                  params: { threadId: forkNativeId },
                },
              },
              { type: "emit_inbound", frame: { id: 3, result: { data: [], nextCursor: null } } },
              {
                type: "expect_outbound",
                frame: { id: 4, method: "skills/list", params: { cwds: ["/workspace"] } },
              },
              { type: "emit_inbound", frame: { id: 4, result: { data: [] } } },
            ],
          ],
          [
            "reopened",
            [
              ...handshake,
              {
                type: "expect_outbound",
                frame: {
                  id: 2,
                  method: "thread/resume",
                  params: { threadId: forkNativeId, excludeTurns: true, ...params },
                },
              },
              {
                type: "emit_inbound",
                frame: { id: 2, result: { thread: { id: forkNativeId, updatedAt: 1782622440 } } },
              },
            ],
          ],
          ["chain", nativeFork(forkNativeId, chainNativeId)],
        ]);
        const opened: Array<
          Parameters<CodexAdapterV2.CodexAppServerClientFactoryShape["open"]>[0]
        > = [];
        const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
          instanceId: CODEX_TEST_MODEL_SELECTION.instanceId,
          settings,
          environment: setupMode === "managed" ? { ACCESS_TOKEN: "FAKE_MANAGED_TOKEN" } : {},
          fileSystem: fs,
          crypto: yield* Crypto.Crypto,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          serverConfig,
          clientFactory: {
            open: (launch) => {
              opened.push(launch);
              const entries = transcripts.get(launch.providerSessionId);
              if (entries === undefined) return Effect.die("Unexpected app-server process");
              return Layer.build(
                CodexReplay.layerReplay(
                  makeCodexReplayTranscript({
                    scenario: `bare-${launch.providerSessionId}`,
                    entries,
                  }),
                ),
              ).pipe(
                Effect.flatMap((context) =>
                  Effect.service(CodexClient.CodexAppServerClient).pipe(Effect.provide(context)),
                ),
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterOpenSessionError({
                      driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                      providerSessionId: launch.providerSessionId,
                      cause,
                    }),
                ),
              );
            },
          },
        });
        const open = (threadId: ThreadId, id: string, initialNativeThreadId?: string) =>
          adapter.openSession({
            threadId,
            providerSessionId: ProviderSessionId.make(id),
            modelSelection: CODEX_TEST_MODEL_SELECTION,
            runtimePolicy,
            ...(initialNativeThreadId === undefined ? {} : { initialNativeThreadId }),
          });
        yield* open(ownerId, "source");
        const now = yield* DateTime.now;
        const source: OrchestrationV2ProviderThread = {
          id: ProviderThreadId.make("bare-source-provider-thread"),
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          providerInstanceId: CODEX_TEST_MODEL_SELECTION.instanceId,
          providerSessionId: ProviderSessionId.make("source"),
          appThreadId: ownerId,
          ownerNodeId: null,
          nativeThreadRef: {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: sourceNativeId,
            strength: "strong",
          },
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        };
        const forkRuntime = yield* open(forkId, "fork");
        const forked = yield* forkRuntime.forkThread({
          sourceProviderThread: source,
          targetThreadId: forkId,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy,
        });
        assert.equal(forked.nativeThreadRef?.nativeId, forkNativeId);
        assert.equal(forked.providerSessionId, ProviderSessionId.make("fork"));
        const getBareReceipt = forkRuntime.getBareReceipt;
        if (getBareReceipt === undefined) throw new Error("Bare Codex session has no receipt.");
        const receipt = yield* getBareReceipt(forked);
        assert.deepEqual(receipt.nativeRecord, {
          harness: "codex",
          root: `${home}/sessions`,
          sessionId: forkNativeId,
        });
        const reopened = yield* open(forkId, "reopened", forkNativeId);
        const resumed = yield* reopened.resumeThread({
          threadId: forkId,
          providerThread: forked,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy,
        });
        assert.equal(resumed.nativeThreadRef?.nativeId, forkNativeId);
        const chained = yield* (yield* open(chainId, "chain")).forkThread({
          sourceProviderThread: resumed,
          targetThreadId: chainId,
          modelSelection: CODEX_TEST_MODEL_SELECTION,
          runtimePolicy,
        });
        assert.equal(chained.nativeThreadRef?.nativeId, chainNativeId);
        assert.lengthOf(opened, 4);
        assert.deepEqual(
          opened.map((launch) => launch.threadId),
          [ownerId, forkId, forkId, chainId],
        );
        for (const launch of opened) {
          assert.equal(launch.settings.homePath, home);
          assert.equal(launch.environment.CODEX_HOME, home);
        }
        yield* fs.rename(home, `${home}-removed`);
        const missing = yield* open(forkId, "must-not-spawn", forkNativeId).pipe(Effect.flip);
        assert.include(errorCauseChainText(missing), "native home is missing");
        const missingSource = yield* open(ownerId, "must-not-spawn", sourceNativeId).pipe(
          Effect.flip,
        );
        assert.include(errorCauseChainText(missingSource), "native home is missing");
        assert.lengthOf(opened, 4);
        assert.isFalse(yield* fs.exists(home));
        assert.deepEqual(yield* fs.readDirectory(`${serverConfig.stateDir}/bare/codex/codex`), [
          "thread-9727934f3f9d2c89a76c8041c6b7e4b410f8345c0129f2185936004ea4316c9f-removed",
        ]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  const codexReplaySourceTurn = (input: {
    readonly id: string;
    readonly ordinal: number;
    readonly nativeId: string | null;
    readonly providerThreadId: ProviderThreadId;
    readonly now: DateTime.Utc;
  }): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(input.id),
    providerThreadId: input.providerThreadId,
    nodeId: NodeId.make(`node-${input.id}`),
    runAttemptId: RunAttemptId.make(`run-attempt-${input.id}`),
    nativeTurnRef:
      input.nativeId === null
        ? { driver: CodexAdapterV2.CODEX_DRIVER_KIND, nativeId: null, strength: "none" }
        : {
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeId: input.nativeId,
            strength: "strong",
          },
    ordinal: input.ordinal,
    status: "completed",
    startedAt: input.now,
    completedAt: input.now,
  });

  it.effect("fails honestly when rolling back a legacy Codex thread", () =>
    Effect.gen(function* () {
      const nativeThreadId = "legacy-rollback-thread";
      const preamble = codexReplayPreamble({
        nativeThreadId,
        nativeTurnId: "legacy-rollback-turn",
        prompt: "unused",
      });
      const transcript = makeCodexReplayTranscript({
        scenario: "codex-legacy-rollback",
        entries: [
          ...preamble.slice(0, 5),
          {
            type: "expect_outbound",
            label: "thread/read",
            frame: {
              id: 3,
              method: "thread/read",
              params: { threadId: nativeThreadId, includeTurns: false },
            },
          },
          {
            type: "emit_inbound",
            label: "thread/read",
            frame: {
              id: 3,
              result: { thread: { id: nativeThreadId, historyMode: "legacy" } },
            },
          },
        ],
      });
      const outbound: Array<string> = [];
      const harness = yield* makeCodexReplayHarness(
        transcript,
        () => Effect.void,
        (method) =>
          Effect.sync(() => {
            outbound.push(method);
          }),
      );
      const now = yield* DateTime.now;
      const firstTurn = codexReplaySourceTurn({
        id: "provider-turn-first",
        ordinal: 1,
        nativeId: "native-turn-first",
        providerThreadId: harness.providerThread.id,
        now,
      });
      const secondTurn = codexReplaySourceTurn({
        id: "provider-turn-second",
        ordinal: 2,
        nativeId: "native-turn-second",
        providerThreadId: harness.providerThread.id,
        now,
      });

      const error = yield* Effect.flip(
        harness.runtime.rollbackThread({
          providerThread: harness.providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-legacy-rollback"),
            appRunOrdinal: 1,
            providerTurn: firstTurn,
          },
          providerThreadTurns: [firstTurn, secondTurn],
        }),
      );

      assert.instanceOf(error, ProviderAdapter.ProviderAdapterRollbackThreadError);
      assert.include(
        errorCauseChainText(error),
        "legacy",
        "legacy rollback must surface an honest unsupported-history failure",
      );
      assert.notInclude(
        outbound,
        "thread/rollback",
        "thread/rollback must not be sent to a legacy Codex thread",
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
      ),
    ),
  );

  it.effect(
    "falls back to fork-local thread/revert on paginated history when the source turn lacks a native reference",
    () =>
      Effect.gen(function* () {
        const nativeThreadId = "fallback-source-thread";
        const forkThreadId = "fallback-fork-thread";
        const preamble = codexReplayPreamble({
          nativeThreadId,
          nativeTurnId: "fallback-source-turn",
          prompt: "unused",
        });
        const transcript = makeCodexReplayTranscript({
          scenario: "codex-fork-paginated-fallback",
          entries: [
            ...preamble.slice(0, 5),
            {
              type: "expect_outbound",
              label: "thread/fork",
              frame: {
                id: 3,
                method: "thread/fork",
                params: { threadId: nativeThreadId, config: CodexAdapterV2.CODEX_THREAD_CONFIG },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/fork",
              frame: {
                id: 3,
                result: codexReplayThreadResult({
                  nativeThreadId: forkThreadId,
                  forkedFromId: nativeThreadId,
                }),
              },
            },
            {
              type: "expect_outbound",
              label: "thread/read",
              frame: {
                id: 4,
                method: "thread/read",
                params: { threadId: forkThreadId, includeTurns: false },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/read",
              frame: {
                id: 4,
                result: { thread: { id: forkThreadId, historyMode: "paginated" } },
              },
            },
            {
              type: "expect_outbound",
              label: "thread/turns/list",
              frame: {
                id: 5,
                method: "thread/turns/list",
                params: {
                  threadId: forkThreadId,
                  cursor: null,
                  limit: 1,
                  sortDirection: "desc",
                  itemsView: "summary",
                },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/turns/list",
              frame: {
                id: 5,
                result: {
                  data: [{ id: "native-turn-second", items: [], status: "completed", error: null }],
                  nextCursor: null,
                },
              },
            },
            {
              type: "expect_outbound",
              label: "thread/revert",
              frame: {
                id: 6,
                method: "thread/revert",
                params: { threadId: forkThreadId, beforeTurnId: "native-turn-second" },
              },
            },
            {
              type: "emit_inbound",
              label: "thread/revert",
              frame: {
                id: 6,
                result: codexReplayThreadResult({
                  nativeThreadId: forkThreadId,
                  forkedFromId: null,
                }),
              },
            },
          ],
        });
        const outbound: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              outbound.push(method);
            }),
        );
        const now = yield* DateTime.now;
        const firstTurn = codexReplaySourceTurn({
          id: "provider-turn-first",
          ordinal: 1,
          nativeId: null,
          providerThreadId: harness.providerThread.id,
          now,
        });
        const secondTurn = codexReplaySourceTurn({
          id: "provider-turn-second",
          ordinal: 2,
          nativeId: "native-turn-second",
          providerThreadId: harness.providerThread.id,
          now,
        });

        const forkedProviderThread = yield* harness.runtime.forkThread({
          sourceProviderThread: harness.providerThread,
          sourceProviderTurns: [firstTurn, secondTurn],
          providerTurnId: firstTurn.id,
          targetThreadId: ThreadId.make("thread-fork-paginated-fallback-target"),
        });

        assert.equal(forkedProviderThread.nativeThreadRef?.nativeId, forkThreadId);
        assert.notEqual(forkedProviderThread.id, harness.providerThread.id);
        assert.equal(forkedProviderThread.forkedFrom?.providerTurnId, firstTurn.id);
        assert.deepEqual(outbound.slice(-2), ["thread/turns/list", "thread/revert"]);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
  );

  describe("native goals", () => {
    const nativeThreadId = "goal-thread";
    const objective = "Ship the feature";
    const codexGoal = (status: string, tokensUsed: number) => ({
      threadId: nativeThreadId,
      objective,
      status,
      tokenBudget: null,
      tokensUsed,
      timeUsedSeconds: 3,
      createdAt: 1782622440,
      updatedAt: 1782622450,
    });
    const notification = (
      label: string,
      method: string,
      params: unknown,
    ): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label,
      frame: { method, params },
    });
    const request = (id: number, method: string, params: unknown, result: unknown) =>
      [
        { type: "expect_outbound", label: method, frame: { id, method, params } },
        { type: "emit_inbound", label: method, frame: { id, result } },
      ] satisfies Array<CodexReplay.CodexAppServerReplayEntry>;
    const withReplayRequestId = (
      entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
      id: number,
    ) =>
      entries.map((entry) =>
        "frame" in entry && Predicate.isObject(entry.frame) && "id" in entry.frame
          ? { ...entry, frame: { ...entry.frame, id } }
          : entry,
      );
    const sessionStart = codexReplayPreamble({
      nativeThreadId,
      nativeTurnId: "unused",
      prompt: "unused",
    }).slice(0, 5);
    const goalTurnInput = (
      harness: {
        readonly threadId: ThreadId;
        readonly providerThread: OrchestrationV2ProviderThread;
      },
      text: string,
    ) =>
      DateTime.now.pipe(
        Effect.map((now) =>
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now,
            attemptId: RunAttemptId.make("goal-attempt"),
            text,
          }),
        ),
      );
    const providerGoals = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
      events.flatMap((event) =>
        event.type === "provider_thread.updated" ? [event.providerThread.goal?.status ?? null] : [],
      );

    it.effect("keeps a /goal run open across the turns Codex continues on its own", () =>
      Effect.gen(function* () {
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-continuation",
          entries: [
            ...sessionStart,
            ...request(3, "thread/goal/get", { threadId: nativeThreadId }, { goal: null }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, objective, status: "paused" },
              { goal: codexGoal("paused", 0) },
            ),
            ...withReplayRequestId(
              codexReplayPreamble({
                nativeThreadId,
                nativeTurnId: "goal-turn-1",
                prompt: objective,
                sentPrompt: `Start working toward this goal: ${objective}`,
              }).slice(5),
              5,
            ),
            ...request(
              6,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "active" },
              { goal: codexGoal("active", 0) },
            ),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-1",
              goal: codexGoal("active", 0),
            }),
            notification("first turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-1", status: "completed" }),
            }),
            notification("continuation", "turn/started", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-2", status: "inProgress" }),
            }),
            notification("goal complete", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-2",
              goal: codexGoal("complete", 1200),
            }),
            notification("continuation done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-2", status: "completed" }),
            }),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(yield* goalTurnInput(harness, `/goal ${objective}`));
        yield* harness.firstTerminal;

        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const providerTurnId = (nativeTurnId: string) =>
          idAllocator.derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId,
          });
        assert.deepEqual(
          harness.terminalEvents().map((event) => [event.providerTurnId, event.status]),
          [[providerTurnId("goal-turn-2"), "completed"]],
        );
        assert.deepEqual(
          harness.events.flatMap((event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running"
              ? [
                  [
                    event.providerTurn.id,
                    event.providerTurn.runAttemptId,
                    event.providerTurn.ordinal,
                  ],
                ]
              : [],
          ),
          [
            [providerTurnId("goal-turn-1"), RunAttemptId.make("goal-attempt"), 1],
            [providerTurnId("goal-turn-2"), RunAttemptId.make("goal-attempt"), 2],
          ],
        );
        // The goal is set paused until its first turn starts; only the result shows.
        assert.notInclude(providerGoals(harness.events), "paused");
        assert.equal(providerGoals(harness.events).at(-1), "complete");
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("settles a run held for the next goal turn when Stop arrives in between", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop-between-turns",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 10) },
            ),
            notification("goal paused", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("paused", 10),
            }),
          ],
        });
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-hold-attempt"),
            text: prompt,
          }),
        );
        const providerTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
          driver: CodexAdapterV2.CODEX_DRIVER_KIND,
          nativeTurnId: "goal-turn",
        });
        const turnStatuses = () =>
          harness.events.flatMap((event) =>
            event.type === "provider_turn.updated" && event.providerTurn.id === providerTurnId
              ? [event.providerTurn.status]
              : [],
          );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        assert.deepEqual(turnStatuses(), ["running"]);
        assert.lengthOf(harness.terminalEvents(), 0);

        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId,
        });
        yield* harness.firstTerminal;

        assert.notInclude(requests, "turn/interrupt");
        assert.deepEqual(turnStatuses(), ["running", "completed"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("moves a Stop that names an earlier goal turn to the turn Codex continued with", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop-after-continuation",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn-a", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-a",
              goal: codexGoal("active", 10),
            }),
            notification("first turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-a", status: "completed" }),
            }),
            notification("continuation", "turn/started", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-b", status: "inProgress" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn-b",
              goal: codexGoal("active", 11),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 11) },
            ),
            ...request(
              5,
              "turn/interrupt",
              { threadId: nativeThreadId, turnId: "goal-turn-b" },
              {},
            ),
            notification("continuation interrupted", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-b", status: "interrupted" }),
            }),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-successor-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId: "goal-turn-a",
          }),
          requestRuntimeRestart: true,
        });
        yield* harness.firstTerminal;
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("pauses a goal Codex does not continue before its run settles", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-continuation-expired",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 11) },
            ),
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-expired-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        assert.lengthOf(harness.terminalEvents(), 0);
        yield* TestClock.adjust("5 seconds");
        yield* harness.firstTerminal;

        assert.deepEqual(providerGoals(harness.events), ["active", "active", "paused"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("stops a turn Codex starts while the expiry pause is in flight", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-continuation-during-expiry",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
            {
              type: "expect_outbound",
              label: "pause",
              frame: {
                id: 4,
                method: "thread/goal/set",
                params: { threadId: nativeThreadId, status: "paused" },
              },
            },
            notification("goal paused", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("paused", 11),
            }),
            notification("late continuation", "turn/started", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn-2", status: "inProgress" }),
            }),
            ...request(
              5,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 11) },
            ),
            ...request(
              6,
              "turn/interrupt",
              { threadId: nativeThreadId, turnId: "goal-turn-2" },
              {},
            ),
            {
              type: "emit_inbound",
              label: "paused",
              frame: { id: 4, result: { goal: codexGoal("paused", 11) } },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-race-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        yield* TestClock.adjust("5 seconds");
        yield* harness.firstTerminal;

        assert.deepEqual(
          harness.terminalEvents().map((event) => [event.providerTurnId, event.status]),
          [
            [
              (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
                driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                nativeTurnId: "goal-turn",
              }),
              "completed",
            ],
          ],
        );
        assert.equal(providerGoals(harness.events).at(-1), "paused");
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("settles the run when Codex never answers the expiry pause", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-expiry-pause-unanswered",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            notification("turn done", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "completed" }),
            }),
            notification("goal accounted", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: null,
              goal: codexGoal("active", 11),
            }),
            {
              type: "expect_outbound",
              label: "pause",
              frame: {
                id: 4,
                method: "thread/goal/set",
                params: { threadId: nativeThreadId, status: "paused" },
              },
            },
          ],
        });
        const harness = yield* makeCodexReplayHarness(transcript);
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-unanswered-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() =>
          harness.events.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.goal?.tokensUsed === 11,
          ),
        );
        yield* TestClock.adjust("5 seconds");
        yield* TestClock.adjust("10 seconds");
        yield* harness.firstTerminal;

        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("Stop prevents a capacity retry while pausing its goal", () =>
      Effect.gen(function* () {
        const nativeTurnId = "goal-capacity-stop";
        const prompt = "Keep going";
        const retrying = yield* Deferred.make<void>();
        const pausing = yield* Deferred.make<void>();
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario: "goal-capacity-stop",
            entries: [
              ...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }),
              notification("goal active", "thread/goal/updated", {
                threadId: nativeThreadId,
                turnId: nativeTurnId,
                goal: codexGoal("active", 10),
              }),
              notification("capacity failure", "turn/completed", {
                threadId: nativeThreadId,
                turn: {
                  ...makeCodexReplayTurn({ id: nativeTurnId, status: "failed" }),
                  error: {
                    message: "Selected model is at capacity.",
                    codexErrorInfo: "serverOverloaded",
                  },
                },
              }),
              {
                type: "expect_outbound",
                frame: {
                  id: 4,
                  method: "thread/goal/set",
                  params: { threadId: nativeThreadId, status: "paused" },
                },
              },
              {
                type: "emit_inbound",
                afterMs: 3_000,
                frame: { id: 4, result: { goal: codexGoal("paused", 10) } },
              },
            ],
          }),
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "error" &&
            event.turnItem.status === "running"
              ? Deferred.succeed(retrying, undefined)
              : Effect.void,
          (method) =>
            Effect.gen(function* () {
              requests.push(method);
              if (method === "thread/goal/set") yield* Deferred.succeed(pausing, undefined);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-capacity-stop"),
            text: prompt,
          }),
        );
        yield* Deferred.await(retrying);
        const stopping = yield* harness.runtime
          .interruptTurn({
            providerThread: harness.providerThread,
            providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
              driver: CodexAdapterV2.CODEX_DRIVER_KIND,
              nativeTurnId,
            }),
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(pausing);
        yield* TestClock.adjust("3 seconds");
        assert.lengthOf(
          requests.filter((method) => method === "turn/start"),
          1,
        );
        yield* Fiber.join(stopping);
        yield* harness.awaitTerminal(1);
        assert.equal(harness.terminalEvents()[0]?.status, "interrupted");
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("pauses an active goal before Stop interrupts its turn", () =>
      Effect.gen(function* () {
        const prompt = "Keep going";
        const transcript = makeCodexReplayTranscript({
          scenario: "goal-stop",
          entries: [
            ...codexReplayPreamble({ nativeThreadId, nativeTurnId: "goal-turn", prompt }),
            notification("goal active", "thread/goal/updated", {
              threadId: nativeThreadId,
              turnId: "goal-turn",
              goal: codexGoal("active", 10),
            }),
            ...request(
              4,
              "thread/goal/set",
              { threadId: nativeThreadId, status: "paused" },
              { goal: codexGoal("paused", 10) },
            ),
            ...request(5, "turn/interrupt", { threadId: nativeThreadId, turnId: "goal-turn" }, {}),
            notification("interrupted", "turn/completed", {
              threadId: nativeThreadId,
              turn: makeCodexReplayTurn({ id: "goal-turn", status: "interrupted" }),
            }),
          ],
        });
        const requests: Array<string> = [];
        const harness = yield* makeCodexReplayHarness(
          transcript,
          () => Effect.void,
          (method) =>
            Effect.sync(() => {
              requests.push(method);
            }),
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make("goal-stop-attempt"),
            text: prompt,
          }),
        );
        yield* harness.until(() => providerGoals(harness.events).includes("active"));
        yield* harness.runtime.interruptTurn({
          providerThread: harness.providerThread,
          providerTurnId: (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
            driver: CodexAdapterV2.CODEX_DRIVER_KIND,
            nativeTurnId: "goal-turn",
          }),
        });
        yield* harness.firstTerminal;

        assert.deepEqual(requests.slice(-2), ["thread/goal/set", "turn/interrupt"]);
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["interrupted"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );
  });

  describe("Codex file-change approval paths", () => {
    const nativeThreadId = "file-approval-thread";
    const nativeTurnId = "file-approval-turn";
    const prompt = "Edit the files.";
    type FileUpdateChange = CodexSchema.V2ItemStartedNotification__FileUpdateChange;
    const addA: FileUpdateChange = { path: "/w/a.ts", kind: { type: "add" }, diff: "export {};" };
    const updateB: FileUpdateChange = {
      path: "/w/b.ts",
      kind: { type: "update", move_path: null },
      diff: "@@",
    };
    const renameC: FileUpdateChange = {
      path: "/w/c.ts",
      kind: { type: "update", move_path: "/w/d.ts" },
      diff: "@@",
    };

    const deleteE: FileUpdateChange = { path: "/w/e.ts", kind: { type: "delete" }, diff: "gone" };

    const fileChange = (
      phase: "started" | "completed",
      turnId: string,
      itemId: string,
      changes: ReadonlyArray<FileUpdateChange>,
    ): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label: `${phase} ${itemId}`,
      frame: {
        method: `item/${phase}`,
        params: {
          threadId: nativeThreadId,
          turnId,
          ...(phase === "started" ? { startedAtMs: 1 } : { completedAtMs: 3 }),
          item: {
            type: "fileChange",
            id: itemId,
            status: phase === "started" ? "inProgress" : "completed",
            changes,
          },
        },
      },
    });

    const approval = (input: {
      readonly id: number;
      readonly turnId: string;
      readonly itemId: string;
    }): Array<CodexReplay.CodexAppServerReplayEntry> => [
      {
        type: "emit_inbound",
        label: `approval ${input.itemId}`,
        frame: {
          id: input.id,
          method: "item/fileChange/requestApproval",
          params: {
            threadId: nativeThreadId,
            turnId: input.turnId,
            itemId: input.itemId,
            startedAtMs: 2,
            reason: null,
            grantRoot: null,
          },
        },
      },
      {
        type: "expect_outbound",
        label: `approval ${input.itemId}`,
        frame: { id: input.id, result: { decision: "accept" } },
      },
    ];

    const turnCompleted = (turnId: string): CodexReplay.CodexAppServerReplayEntry => ({
      type: "emit_inbound",
      label: `complete ${turnId}`,
      frame: {
        method: "turn/completed",
        params: {
          threadId: nativeThreadId,
          turn: makeCodexReplayTurn({ id: turnId, status: "completed" }),
        },
      },
    });

    const startApprovalTurn = (
      scenario: string,
      entries: ReadonlyArray<CodexReplay.CodexAppServerReplayEntry>,
    ) =>
      Effect.gen(function* () {
        const pending = yield* Queue.unbounded<OrchestrationV2RuntimeRequest>();
        const harness = yield* makeCodexReplayHarness(
          makeCodexReplayTranscript({
            scenario,
            entries: [...codexReplayPreamble({ nativeThreadId, nativeTurnId, prompt }), ...entries],
          }),
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending"
              ? Queue.offer(pending, event.runtimeRequest)
              : Effect.void,
        );
        yield* harness.runtime.startTurn(
          makeCodexTestTurnInput({
            threadId: harness.threadId,
            providerThread: harness.providerThread,
            now: yield* DateTime.now,
            attemptId: RunAttemptId.make(`${scenario}-attempt`),
            text: prompt,
          }),
        );
        return {
          harness,
          answerNext: Queue.take(pending).pipe(
            Effect.tap((request) =>
              harness.runtime.respondToRuntimeRequest({
                requestId: request.id,
                decision: "accept",
              }),
            ),
          ),
        };
      });

    const approvalItem = (
      events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
      request: OrchestrationV2RuntimeRequest,
    ) =>
      events.flatMap((event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "approval_request" &&
        event.turnItem.requestId === request.id
          ? [event.turnItem]
          : [],
      )[0];

    it.effect("answers without waiting for late paths or borrowing another item's paths", () =>
      Effect.gen(function* () {
        const { harness, answerNext } = yield* startApprovalTurn("file-approval-after", [
          fileChange("started", nativeTurnId, "other-patch", [updateB]),
          // Late details cannot arrive until the approval response has been sent.
          ...approval({ id: 0, turnId: nativeTurnId, itemId: "patch-a" }),
          fileChange("started", nativeTurnId, "patch-a", [addA]),
          fileChange("completed", nativeTurnId, "patch-a", [addA]),
          fileChange("completed", nativeTurnId, "other-patch", [updateB]),
          turnCompleted(nativeTurnId),
        ]);
        const request = yield* answerNext;
        yield* harness.firstTerminal;

        const items = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "approval_request" &&
          event.turnItem.requestId === request.id
            ? [event.turnItem]
            : [],
        );
        assert.equal(request.nativeRequestRef?.nativeId, "patch-a");
        assert.isNotEmpty(items);
        for (const item of items) {
          assert.equal(item.requestKind, "file-change");
          assert.isUndefined(item.prompt);
        }
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("matches file paths to items approved out of order", () =>
      Effect.gen(function* () {
        const { harness, answerNext } = yield* startApprovalTurn("file-approval-concurrent", [
          fileChange("started", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          fileChange("started", nativeTurnId, "patch-b", [updateB]),
          ...approval({ id: 0, turnId: nativeTurnId, itemId: "patch-b" }),
          fileChange("completed", nativeTurnId, "patch-b", [updateB]),
          ...approval({ id: 1, turnId: nativeTurnId, itemId: "patch-a" }),
          fileChange("completed", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          turnCompleted(nativeTurnId),
        ]);
        const first = yield* answerNext;
        const second = yield* answerNext;
        yield* harness.firstTerminal;

        assert.deepEqual(
          [first, second].map((request) => [
            request.nativeRequestRef?.nativeId,
            approvalItem(harness.events, request)?.prompt,
          ]),
          [
            ["patch-b", "update /w/b.ts"],
            ["patch-a", "add /w/a.ts\nupdate /w/c.ts -> /w/d.ts\ndelete /w/e.ts"],
          ],
        );
        assert.deepEqual(
          harness.terminalEvents().map((event) => event.status),
          ["completed"],
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );

    it.effect("keeps every path of a completed multi-file patch", () =>
      Effect.gen(function* () {
        const { harness } = yield* startApprovalTurn("file-change-multi-path", [
          fileChange("started", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          fileChange("completed", nativeTurnId, "patch-a", [renameC, deleteE, addA]),
          turnCompleted(nativeTurnId),
        ]);
        yield* harness.firstTerminal;

        const fileChanges = harness.events.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "file_change"
            ? [event.turnItem]
            : [],
        );
        assert.lengthOf(fileChanges, 1);
        assert.equal(fileChanges[0]?.fileName, "/w/c.ts");
        assert.deepEqual(fileChanges[0]?.changes, [
          { operation: "update", path: "/w/d.ts", oldPath: "/w/c.ts" },
          { operation: "delete", path: "/w/e.ts" },
          { operation: "add", path: "/w/a.ts" },
        ]);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      ),
    );
  });

  it.effect.each([false, true])(
    "rejects duplicate child starts across A/B parent runs and requires an exact resume call (observed=$0)",
    (observedResume) =>
      Effect.scoped(
        Effect.gen(function* () {
          const firstDone = yield* Deferred.make<void>();
          const resumed = yield* Deferred.make<void>();
          const secondTurn = "native-parent-resume-turn";
          const secondPrompt = "Resume the child.";
          const purposeA = {
            version: 1,
            scope: {
              type: "single",
              reference: {
                host: "github.com",
                repository: "example/repo",
                kind: "issue",
                number: 11,
              },
            },
            provenance: { type: "declared" },
          } as const;
          const purposeB = {
            ...purposeA,
            scope: { ...purposeA.scope, reference: { ...purposeA.scope.reference, number: 12 } },
          } as const;
          const entries = [...resumeSubagentTranscript.entries];
          const resumeIndex = entries.findIndex(
            (e) => e.type === "emit_inbound" && e.label === `turn/started/${RESUME_CHILD_TURN_2}`,
          );
          const suffix = entries.splice(resumeIndex);
          for (const entry of codexReplayPreamble({
            nativeThreadId: RESUME_NATIVE_THREAD,
            nativeTurnId: secondTurn,
            prompt: secondPrompt,
          }).slice(-3)) {
            entries.push(
              entry.type === "expect_outbound" || entry.type === "emit_inbound"
                ? {
                    ...entry,
                    frame:
                      Predicate.isObject(entry.frame) && "id" in entry.frame
                        ? { ...entry.frame, id: 4 }
                        : entry.frame,
                  }
                : entry,
            );
          }
          if (observedResume)
            entries.push({
              type: "emit_inbound",
              label: "exact-resume-call-b",
              frame: {
                method: "item/completed",
                params: {
                  threadId: RESUME_NATIVE_THREAD,
                  turnId: secondTurn,
                  item: {
                    type: "collabAgentToolCall",
                    id: "native-resume-call-b",
                    tool: "sendInput",
                    status: "completed",
                    senderThreadId: RESUME_NATIVE_THREAD,
                    receiverThreadIds: [RESUME_CHILD_THREAD],
                    agentsStates: { [RESUME_CHILD_THREAD]: { status: "running", message: null } },
                  },
                },
              },
            });
          entries.push(childTurnStarted(RESUME_CHILD_TURN_1));
          entries.push(...suffix);
          const harness = yield* makeCodexReplayHarness(
            makeCodexReplayTranscript({
              scenario: "codex-cross-run-resume",
              entries,
            }),
            (event) =>
              event.type === "turn.terminal"
                ? Deferred.succeed(firstDone, undefined)
                : event.type === "subagent.updated" &&
                    event.subagent.runId === "run-cross-run-second"
                  ? Deferred.succeed(resumed, undefined)
                  : Effect.void,
          );
          const now = yield* DateTime.now;
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("cross-run-first"),
              text: RESUME_PROMPT,
            }),
            purpose: purposeA,
          });
          yield* TestClock.adjust("100 millis");
          yield* Deferred.await(firstDone);
          yield* harness.runtime.startTurn({
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("cross-run-second"),
              text: secondPrompt,
            }),
            runOrdinal: 2,
            providerTurnOrdinal: 2,
            purpose: purposeB,
          });
          yield* TestClock.adjust("30 seconds");
          yield* Deferred.await(resumed);
          const row = harness
            .subagentUpdates()
            .find((e) => e.subagent.runId === "run-cross-run-second")?.subagent;
          assert.equal(row?.status, "running");
          assert.equal(row?.parentNodeId, "node-cross-run-second");
          assert.isNull(row?.completedAt);
          assert.isNotNull(row?.startedAt);
          assert.equal(DateTime.toEpochMillis(row!.startedAt!), 1782622470000);
          assert.deepEqual(row?.nativeSegments?.[0]?.purpose.scope, purposeA.scope);
          assert.deepEqual(row?.nativeSegments?.[0]?.boundary, {
            state: "native_turns",
            nativeTurnIds: [RESUME_CHILD_TURN_1],
          });
          assert.equal(row?.nativeSegments?.length, observedResume ? 2 : 1);
          if (observedResume) {
            assert.deepEqual(row?.nativeSegments?.[1]?.purpose.scope, purposeB.scope);
            assert.deepEqual(row?.nativeSegments?.[1]?.source, {
              type: "app_run",
              threadId: harness.threadId,
              runId: RunId.make("run-cross-run-second"),
            });
            assert.equal(row?.nativeSegments?.[1]?.boundary.state, "unresolved");
            assert.equal(row?.nativeSegments?.[1]?.usageFinality, "unreconciled");
          }
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect(
    "projects a post-settle background command completion and requests a continuation",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const backgroundEnded = yield* Deferred.make<void>();
          const backgroundDropped = yield* Deferred.make<void>();
          const harness = yield* makeCodexReplayHarness(backgroundExecTranscript, (event) =>
            Effect.gen(function* () {
              if (event.type !== "provider_thread.updated") return;
              for (const operation of event.providerThread.nativeOperations ?? []) {
                if (operation.nativeOperationId !== BG_COMMAND_ITEM) continue;
                if (operation.status === "ended")
                  yield* Deferred.succeed(backgroundEnded, undefined);
                if (operation.wakeDispositions.includes("dropped_disabled"))
                  yield* Deferred.succeed(backgroundDropped, undefined);
              }
            }),
          );
          const now = yield* DateTime.now;

          const turnInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
            ...makeCodexTestTurnInput({
              threadId: harness.threadId,
              providerThread: harness.providerThread,
              now,
              attemptId: RunAttemptId.make("attempt-codex-bg-wake"),
              text: BG_PROMPT,
            }),
            purpose: {
              version: 1,
              scope: {
                type: "single",
                reference: {
                  host: "github.com",
                  repository: "acme/work",
                  kind: "issue",
                  number: 1,
                },
              },
              provenance: { type: "declared" },
            },
          };
          yield* harness.runtime.startTurn(turnInput);
          yield* awaitUntil(() => harness.terminalEvents().length === 1, "root turn terminal");
          assert.equal(harness.terminalEvents()[0]?.status, "completed");
          assert.isTrue(yield* harness.hasPendingBackgroundWork);
          assert.isTrue(
            yield* harness.runtime.hasPendingBackgroundWorkForThread!(harness.providerThread),
          );
          assert.lengthOf(harness.continuationRequests, 0);
          const terminalIndex = harness.events.findIndex((event) => event.type === "turn.terminal");

          yield* TestClock.adjust("30 seconds");
          yield* awaitUntil(
            () => harness.continuationRequests.length === 1,
            "continuation request",
          );
          yield* Deferred.await(backgroundEnded);
          const request = harness.continuationRequests[0];
          const operation = harness.events
            .flatMap((event) =>
              event.type === "provider_thread.updated"
                ? (event.providerThread.nativeOperations ?? [])
                : [],
            )
            .findLast(
              (operation) =>
                operation.nativeOperationId === BG_COMMAND_ITEM && operation.status === "ended",
            );
          if (operation === undefined) return yield* Effect.die("Missing ended native operation.");
          assert.deepEqual(operation?.source, {
            type: "app_run",
            threadId: harness.threadId,
            runId: turnInput.runId,
          });
          assert.deepEqual(operation?.purpose, turnInput.purpose);
          assert.deepEqual(operation?.boundary, {
            state: "native_turns",
            nativeTurnIds: [BG_NATIVE_TURN],
          });
          assert.equal(operation?.status, "ended");
          assert.equal(operation?.usageFinality, "unreconciled");
          assert.deepEqual(operation.noticeIds, [BG_COMMAND_ITEM]);
          assert.deepEqual(request?.nativeCauses, [
            { operationId: operation.id, purpose: operation.purpose },
          ]);
          if (request?.recordDisposition === undefined)
            return yield* Effect.die("Missing native continuation disposition recorder.");
          yield* request.recordDisposition("dropped_disabled");
          yield* Deferred.await(backgroundDropped);
          assert.isTrue(
            harness.events.some(
              (event) =>
                event.type === "provider_thread.updated" &&
                event.providerThread.nativeOperations?.some(
                  (record) =>
                    record.id === operation.id &&
                    record.wakeDispositions.includes("dropped_disabled") &&
                    record.purpose.scope.type === "single" &&
                    record.purpose.scope.reference.number === 1,
                ),
            ),
          );
          assert.equal(request?.threadId, harness.threadId);
          assert.equal(request?.providerThreadId, harness.providerThread.id);
          assert.equal(request?.driver, CodexAdapterV2.CODEX_DRIVER_KIND);
          assert.deepEqual(request?.notification, {
            source: { kind: "command" },
            outcome: "completed",
            summary: `Command "${BG_COMMAND}" finished (exit 0)`,
            detail: BG_COMMAND,
          });
          assert.equal(
            request?.detail,
            `Background command completed (exit 0): ${BG_COMMAND}\n\n` +
              "Output tail:\nCODEX_BG_WAKE_DONE",
          );

          const lateCommandUpdateIndex = () =>
            harness.events.findIndex(
              (event, index) =>
                index > terminalIndex &&
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.status === "completed" &&
                event.turnItem.output === "CODEX_BG_WAKE_DONE\n" &&
                event.turnItem.exitCode === 0,
            );
          yield* awaitUntil(
            () => lateCommandUpdateIndex() > terminalIndex,
            "post-settle command projection",
          );
          assert.lengthOf(harness.terminalEvents(), 1);
          assert.isFalse(yield* harness.hasPendingBackgroundWork);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
          ),
        ),
      ),
  );
});

// These schedules run the production adapter and serial stdio client. Only the
// external app-server is replaced; requests and native notifications are gated.
describe("CodexAdapterV2 cut-in cohort", () => {
  type Event = ProviderAdapter.ProviderAdapterV2Event;
  type NativeRequest = {
    readonly id: string | number;
    readonly method: string;
    readonly params: Record<string, unknown>;
  };
  const decodeRecord = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));
  const nativeThreadId = "r7-root";
  const rootTurnId = "r7-root-turn";
  const childThreadId = "r7-child";
  const childTurnId = "r7-child-turn";
  const layers = Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer);
  const goal = (status: string) => ({
    threadId: nativeThreadId,
    objective: "Ship",
    status,
    tokenBudget: null,
    tokensUsed: 10,
    timeUsedSeconds: 3,
    createdAt: 1782622440,
    updatedAt: 1782622450,
  });
  const command = (id: string, processId: string, completed = false) => ({
    type: "commandExecution",
    id,
    command: `run ${id}`,
    cwd: "/workspace",
    processId,
    source: "unifiedExecStartup",
    status: completed ? "completed" : "inProgress",
    commandActions: [],
    aggregatedOutput: completed ? "REAL_NATIVE_OUTPUT" : "PARTIAL_NATIVE_OUTPUT",
    exitCode: completed ? 0 : null,
    durationMs: null,
  });
  const makeBoundary = Effect.fnUntraced(function* (
    options: {
      readonly interruptCleanup?: Effect.Effect<void>;
      readonly holdGoalActivation?: boolean;
    } = {},
  ) {
    const inbound = yield* Queue.unbounded<Uint8Array>();
    const controls = new Map<string, Queue.Queue<NativeRequest>>();
    for (const method of [
      "turn/interrupt",
      "turn/steer",
      "thread/backgroundTerminals/terminate",
      "thread/goal/set",
    ])
      controls.set(method, yield* Queue.unbounded<NativeRequest>());
    const requests: NativeRequest[] = [];
    const responses: Record<string, unknown>[] = [];
    const responseObserved = yield* Deferred.make<void>();
    const readerBarriers = new Map<number, Deferred.Deferred<void>>();
    let barrierOrdinal = 0;
    const emit = (frame: unknown) =>
      Queue.offer(inbound, new TextEncoder().encode(`${encodeUnknownJson(frame)}\n`)).pipe(
        Effect.asVoid,
      );
    const notify = (method: string, params: unknown) =>
      emit({
        method,
        params: {
          ...(method === "item/started" || method.endsWith("/requestApproval")
            ? { startedAtMs: 1782622440500 }
            : method === "item/completed"
              ? { completedAtMs: 1782622450000 }
              : {}),
          ...decodeRecord(params),
        },
      });
    const reply = (request: NativeRequest, result: unknown = {}) =>
      emit({ id: request.id, result });
    const reject = (request: NativeRequest) =>
      emit({ id: request.id, error: { code: -32000, message: "NATIVE_CANCELLATION_UNAVAILABLE" } });
    const recorded = yield* readProviderReplayTranscript(
      new URL("../testkit/fixtures/simple/codex_transcript.ndjson", import.meta.url),
    );
    const threadEntry = recorded.entries.find(
      (entry) => entry.type === "emit_inbound" && entry.label === "thread/start",
    );
    if (threadEntry?.type !== "emit_inbound")
      return yield* Effect.die("Missing native thread fixture");
    const recordedResult = decodeRecord(decodeRecord(threadEntry.frame).result);
    const threadResult = {
      ...recordedResult,
      thread: {
        ...decodeRecord(recordedResult.thread),
        id: nativeThreadId,
        sessionId: nativeThreadId,
      },
    };
    const write = (chunk: string | Uint8Array) =>
      Effect.gen(function* () {
        for (const line of (typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk))
          .trim()
          .split("\n")) {
          const frame = decodeRecord(yield* decodeNativeJson(line).pipe(Effect.orDie));
          if (typeof frame.method !== "string") {
            responses.push(frame);
            yield* Deferred.succeed(responseObserved, undefined);
            continue;
          }
          if (frame.method === "initialized") continue;
          if (typeof frame.id !== "number" && typeof frame.id !== "string")
            return yield* Effect.die("Request has no id");
          const request = {
            id: frame.id,
            method: frame.method,
            params: decodeRecord(frame.params ?? {}),
          };
          requests.push(request);
          switch (request.method) {
            case "initialize":
              yield* reply(request, {
                userAgent: "r7-test",
                codexHome: "/workspace/.codex",
                platformFamily: "unix",
                platformOs: "linux",
              });
              break;
            case "thread/start":
              yield* reply(request, threadResult);
              break;
            case "thread/read":
              yield* reply(request, {
                thread: {
                  ...decodeRecord(threadResult.thread),
                  id: request.params.threadId,
                  sessionId: request.params.threadId,
                },
              });
              break;
            case "turn/start": {
              const turn = makeCodexReplayTurn({ id: rootTurnId, status: "inProgress" });
              yield* reply(request, { turn });
              yield* notify("turn/started", { threadId: nativeThreadId, turn });
              break;
            }
            case "thread/goal/get":
              yield* reply(request, { goal: null });
              break;
            case "thread/goal/set":
              if (
                request.params.objective !== undefined ||
                (request.params.status === "active" && !options.holdGoalActivation)
              ) {
                yield* reply(request, { goal: goal(String(request.params.status)) });
                break;
              }
              yield* Queue.offer(controls.get(request.method)!, request);
              break;
            default: {
              const queue = controls.get(request.method);
              if (queue === undefined)
                return yield* Effect.die(`Unexpected native request ${request.method}`);
              yield* Queue.offer(queue, request);
            }
          }
        }
      });
    const events: Event[] = [];
    const watchers = new Map<Deferred.Deferred<Event>, (event: Event) => boolean>();
    const watch = (predicate: (event: Event) => boolean) =>
      Effect.suspend(() => {
        const existing = events.findLast(predicate);
        if (existing !== undefined) return Effect.succeed(existing);
        const received = Deferred.makeUnsafe<Event>();
        watchers.set(received, predicate);
        return Deferred.await(received).pipe(
          Effect.ensuring(Effect.sync(() => watchers.delete(received))),
        );
      });
    const adapter = yield* CodexAdapterV2.makeCodexAdapterV2({
      instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
      settings: DEFAULT_CODEX_SETTINGS,
      environment: {},
      clientFactory: {
        open: () =>
          Effect.gen(function* () {
            const client = yield* CodexClient.make(
              Stdio.make({
                args: Effect.succeed([]),
                stdin: Stream.fromQueue(inbound),
                stdout: () => Sink.forEach(write),
                stderr: () => Sink.drain,
              }),
            );
            yield* client.handleUnknownServerNotification((method, params) => {
              const id = decodeRecord(params).id;
              const barrier = typeof id === "number" ? readerBarriers.get(id) : undefined;
              return method === "r7/barrier" && barrier !== undefined
                ? Deferred.succeed(barrier, undefined).pipe(Effect.asVoid)
                : Effect.void;
            });
            return {
              ...client,
              raw: {
                ...client.raw,
                request: (method, params) =>
                  client.raw
                    .request(method, params)
                    .pipe(
                      Effect.onInterrupt(() =>
                        method === "thread/backgroundTerminals/terminate"
                          ? (options.interruptCleanup ?? Effect.void)
                          : Effect.void,
                      ),
                    ),
              },
            };
          }),
      },
      crypto: yield* Crypto.Crypto,
      fileSystem: yield* FileSystem.FileSystem,
      idAllocator: yield* IdAllocator.IdAllocatorV2,
      serverConfig: yield* makeReplayServerConfig("codex-cut-in-cohort").pipe(Effect.orDie),
    });
    const threadId = ThreadId.make("r7-app-thread");
    const runtime = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make("r7-session"),
      modelSelection: CODEX_TEST_MODEL_SELECTION,
      runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
    });
    const providerThread = yield* runtime.ensureThread({
      threadId,
      modelSelection: CODEX_TEST_MODEL_SELECTION,
      runtimePolicy: CODEX_TEST_RUNTIME_POLICY,
    });
    yield* runtime.events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          events.push(event);
          for (const [received, predicate] of watchers)
            if (predicate(event)) Deferred.doneUnsafe(received, Effect.succeed(event));
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
    );
    const barrier = Effect.suspend(() => {
      const id = ++barrierOrdinal;
      const received = Deferred.makeUnsafe<void>();
      readerBarriers.set(id, received);
      return notify("r7/barrier", { id }).pipe(
        Effect.andThen(Deferred.await(received)),
        Effect.ensuring(Effect.sync(() => readerBarriers.delete(id))),
      );
    });
    const turnUpdates = (id: string) =>
      events.flatMap((event) =>
        event.type === "provider_turn.updated" && event.providerTurn.nativeTurnRef?.nativeId === id
          ? [event.providerTurn]
          : [],
      );
    const itemUpdates = (id: string) =>
      events.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === id
          ? [event.turnItem]
          : [],
      );
    const end = (threadId: string, id: string, status: "completed" | "interrupted" | "failed") =>
      notify("turn/completed", {
        threadId,
        turn: {
          ...makeCodexReplayTurn({ id, status }),
          ...(status === "failed"
            ? { error: { message: "NATIVE_ROOT_FAILURE", codexErrorInfo: null } }
            : {}),
        },
      }).pipe(Effect.andThen(barrier));
    const item = (
      threadId: string,
      turnId: string,
      id: string,
      processId: string,
      completed = false,
    ) =>
      notify(completed ? "item/completed" : "item/started", {
        threadId,
        turnId,
        item: command(id, processId, completed),
      }).pipe(
        Effect.andThen(
          watch(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.nativeItemRef?.nativeId === id &&
              event.turnItem.status === (completed ? "completed" : "running"),
          ),
        ),
      );
    const child = Effect.gen(function* () {
      yield* notify("item/completed", {
        threadId: nativeThreadId,
        turnId: rootTurnId,
        item: {
          type: "subAgentActivity",
          id: "r7-spawn",
          kind: "started",
          agentThreadId: childThreadId,
          agentPath: "/root/r7-child",
        },
      });
      yield* notify("turn/started", {
        threadId: childThreadId,
        turn: makeCodexReplayTurn({ id: childTurnId, status: "inProgress" }),
      });
      yield* item(childThreadId, childTurnId, "child-command", "77");
    });
    const start = (text = "work") =>
      runtime
        .startTurn(
          makeCodexTestTurnInput({
            threadId,
            providerThread,
            now: DateTime.makeUnsafe(1782622440000),
            attemptId: RunAttemptId.make("r7-attempt"),
            text,
          }),
        )
        .pipe(
          Effect.andThen(
            watch(
              (event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.nativeTurnRef?.nativeId === rootTurnId,
            ),
          ),
          Effect.asVoid,
        );
    const rootId = () => {
      const id = turnUpdates(rootTurnId)[0]?.id;
      if (id === undefined) throw new Error("Missing root turn");
      return id;
    };
    const cutIn = Effect.suspend(() =>
      runtime.interruptTurn({
        providerThread,
        providerTurnId: rootId(),
        requestRuntimeRestart: true,
        replacementFollows: true,
      }),
    );
    const stop = Effect.suspend(() =>
      runtime.interruptTurn({
        providerThread,
        providerTurnId: rootId(),
        requestRuntimeRestart: true,
      }),
    );
    const hold = Effect.suspend(
      () =>
        runtime.endedTurnHold?.({ providerThread, providerTurnId: rootId() }) ??
        Effect.succeed(undefined),
    );
    const take = (method: string) => Queue.take(controls.get(method)!);
    const pending = runtime.hasPendingBackgroundWork ?? Effect.die("Missing pending work probe");
    const childProviderThread = () => {
      const event = events.find(
        (event) =>
          event.type === "provider_thread.updated" &&
          event.providerThread.nativeThreadRef?.nativeId === childThreadId,
      );
      if (event?.type !== "provider_thread.updated")
        throw new Error("Missing child provider thread");
      return event.providerThread;
    };
    const pendingChild = Effect.suspend(
      () =>
        runtime.hasPendingBackgroundWorkForThread?.(childProviderThread()) ??
        Effect.die("Missing thread pending work probe"),
    );
    const terminals = () => events.filter((event) => event.type === "turn.terminal");
    return {
      runtime,
      providerThread,
      threadId,
      events,
      requests,
      responses,
      responseObserved,
      emit,
      notify,
      reply,
      reject,
      watch,
      barrier,
      turnUpdates,
      itemUpdates,
      end,
      item,
      child,
      start,
      cutIn,
      stop,
      hold,
      take,
      pending,
      pendingChild,
      terminals,
    };
  });
  type Boundary = Effect.Success<ReturnType<typeof makeBoundary>>;
  const forkCall = (call: Boundary["cutIn"]) =>
    call.pipe(Effect.forkScoped({ startImmediately: true }));
  const takeBeforeExit = <A, E>(h: Boundary, fiber: Fiber.Fiber<A, E>, method: string) =>
    Effect.raceFirst(
      h.take(method),
      Fiber.await(fiber).pipe(Effect.andThen(Effect.die(`Call exited before ${method}`))),
    );
  const ackRoot = (h: Boundary) =>
    Effect.gen(function* () {
      const request = yield* h.take("turn/interrupt");
      assert.equal(request.params.turnId, rootTurnId);
      yield* h.reply(request);
      yield* h.end(nativeThreadId, rootTurnId, "interrupted");
    });
  const assertReleased = (h: Boundary, rootStatus = "interrupted") => {
    assert.equal(h.turnUpdates(rootTurnId).at(-1)?.status, rootStatus);
    assert.lengthOf(
      h.turnUpdates(rootTurnId).filter((turn) => turn.status !== "running"),
      1,
    );
    assert.lengthOf(h.terminals(), 1);
    const rootEnd = h.events.findIndex(
      (event) =>
        event.type === "provider_turn.updated" &&
        event.providerTurn.id === h.turnUpdates(rootTurnId)[0]?.id &&
        event.providerTurn.status !== "running",
    );
    const terminal = h.events.findIndex((event) => event.type === "turn.terminal");
    assert.isAbove(terminal, rootEnd);
  };

  it.effect.each([false, true])(
    "contains an answered silent child before release, retry=%s (N1/N2)",
    (retry) =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.item(nativeThreadId, rootTurnId, "root-command", "99");
        yield* h.child;
        let call = yield* forkCall(h.cutIn);
        yield* ackRoot(h);
        const child = yield* h.take("turn/interrupt");
        assert.equal(child.params.turnId, childTurnId);
        yield* h.reply(child);
        const rootTerminate = yield* h.take("thread/backgroundTerminals/terminate");
        assert.equal(rootTerminate.params.processId, "99");
        yield* h.reply(rootTerminate, { terminated: true });
        const terminate = yield* h.take("thread/backgroundTerminals/terminate");
        assert.equal(terminate.params.processId, "77");
        assert.equal(h.turnUpdates(rootTurnId).at(-1)?.status, "running");
        assert.equal(h.itemUpdates("child-command").at(-1)?.status, "running");
        if (retry) {
          yield* h.reject(terminate);
          yield* TestClock.adjust("10 seconds");
          yield* h.reject(yield* h.take("thread/backgroundTerminals/terminate"));
          const failed = yield* Fiber.await(call);
          assert.equal(failed._tag, "Failure");
          assert.isTrue(yield* h.pending);
          assert.lengthOf(h.terminals(), 0);
          assert.equal(h.turnUpdates(childTurnId).at(-1)?.status, "running");
          call = yield* forkCall(h.cutIn);
          const next = yield* takeBeforeExit(h, call, "thread/backgroundTerminals/terminate");
          yield* h.reply(next, { terminated: true });
        } else yield* h.reply(terminate, { terminated: true });
        yield* TestClock.adjust("10 seconds");
        yield* Fiber.join(call);
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h);
        assert.isFalse(yield* h.pending);
        assert.lengthOf(
          h.requests.filter(
            (request) =>
              request.method === "turn/interrupt" && request.params.turnId === childTurnId,
          ),
          1,
        );
        assert.deepEqual(
          h.itemUpdates("child-command").map((item) => item.status),
          ["running", "interrupted"],
        );
        assert.lengthOf(
          h.requests.filter(
            (request) =>
              request.method === "thread/backgroundTerminals/terminate" &&
              request.params.processId === "99",
          ),
          1,
        );
        assert.deepEqual(
          h.itemUpdates("root-command").map((item) => item.status),
          ["running", "interrupted"],
        );
        assert.lengthOf(
          h.turnUpdates(childTurnId).filter((turn) => turn.status === "interrupted"),
          1,
        );
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("bounds an unanswered interrupt and ignores its late response on retry (N3)", () =>
    Effect.gen(function* () {
      const h = yield* makeBoundary();
      yield* h.start();
      yield* h.child;
      const first = yield* forkCall(h.cutIn);
      yield* ackRoot(h);
      const unanswered = yield* h.take("turn/interrupt");
      yield* TestClock.adjust("10 seconds");
      const exit = first.pollUnsafe();
      assert.isDefined(exit, "the actual request bound must finish the call");
      if (exit?._tag !== "Failure") return assert.fail("Unanswered interrupt succeeded");
      assert.include(Cause.pretty(exit.cause), "did not acknowledge");
      assert.equal(h.turnUpdates(childTurnId).at(-1)?.status, "running");
      assert.lengthOf(h.terminals(), 0);
      yield* h.reply(unanswered);
      yield* h.barrier;
      const retry = yield* forkCall(h.cutIn);
      const child = yield* takeBeforeExit(h, retry, "turn/interrupt");
      assert.equal(child.params.turnId, childTurnId);
      yield* h.reply(child);
      yield* h.end(childThreadId, childTurnId, "interrupted");
      yield* h.reply(yield* h.take("thread/backgroundTerminals/terminate"), { terminated: true });
      yield* Fiber.join(retry);
      yield* h.watch((event) => event.type === "turn.terminal");
      assertReleased(h);
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect.each([false, true])(
    "keeps ordinary Stop's silent-child outcome, failed containment=%s (N4)",
    (failure) =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        const call = yield* forkCall(h.stop);
        yield* ackRoot(h);
        yield* h.reply(yield* h.take("turn/interrupt"));
        const terminate = yield* h.take("thread/backgroundTerminals/terminate");
        if (failure) yield* h.reject(terminate);
        else yield* h.reply(terminate, { terminated: true });
        yield* TestClock.adjust("10 seconds");
        if (failure) yield* h.reject(yield* h.take("thread/backgroundTerminals/terminate"));
        const result = yield* Fiber.await(call);
        assert.equal(result._tag, failure ? "Failure" : "Success");
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h);
        assert.isFalse(yield* h.pending);
        assert.equal(h.turnUpdates(childTurnId).at(-1)?.status, "interrupted");
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect(
    "adopts the native failed root that ends during goal pause without reopening it (N6)",
    () =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        yield* h.notify("thread/goal/updated", {
          threadId: nativeThreadId,
          turnId: rootTurnId,
          goal: goal("active"),
        });
        yield* h.barrier;
        const call = yield* forkCall(h.cutIn);
        const pause = yield* h.take("thread/goal/set");
        yield* h.end(nativeThreadId, rootTurnId, "failed");
        yield* h.watch(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "failed",
        );
        assert.equal(
          yield* h.hold,
          "cut-in",
          "the running pause call already owns first-Stop reach",
        );
        yield* h.reply(pause, { goal: goal("paused") });
        const child = yield* takeBeforeExit(h, call, "turn/interrupt");
        assert.equal(child.params.turnId, childTurnId);
        yield* h.reply(child);
        yield* h.end(childThreadId, childTurnId, "interrupted");
        yield* h.reply(yield* h.take("thread/backgroundTerminals/terminate"), { terminated: true });
        yield* Fiber.join(call);
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h, "failed");
        assert.deepEqual(
          h.turnUpdates(rootTurnId).map((turn) => turn.status),
          ["running", "failed"],
        );
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect.each(["completed", "failed"] as const)(
    "lets ordinary Stop reach a live child after a natural %s root end (#332)",
    (rootStatus) =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        yield* h.end(nativeThreadId, rootTurnId, rootStatus);
        const call = yield* forkCall(h.stop);
        const child = yield* takeBeforeExit(h, call, "turn/interrupt");
        assert.equal(child.params.turnId, childTurnId);
        yield* h.reply(child);
        yield* h.end(childThreadId, childTurnId, "interrupted");
        const terminate = yield* h.take("thread/backgroundTerminals/terminate");
        assert.equal(terminate.params.processId, "77");
        yield* h.reply(terminate, { terminated: true });
        yield* Fiber.join(call);
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h, rootStatus);
        assert.isFalse(yield* h.pending);
        assert.equal(h.turnUpdates(childTurnId).at(-1)?.status, "interrupted");
        assert.deepEqual(
          h.itemUpdates("child-command").map((item) => item.status),
          ["running", "interrupted"],
        );
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("keeps a failed root's process tracking for Stop while its child runs (#332)", () =>
    Effect.gen(function* () {
      const h = yield* makeBoundary();
      yield* h.start();
      yield* h.item(nativeThreadId, rootTurnId, "root-command", "99");
      yield* h.child;
      yield* h.end(nativeThreadId, rootTurnId, "failed");
      const call = yield* forkCall(h.stop);
      const child = yield* takeBeforeExit(h, call, "turn/interrupt");
      assert.equal(child.params.turnId, childTurnId);
      yield* h.reply(child);
      yield* h.end(childThreadId, childTurnId, "interrupted");
      const terminated = new Set<unknown>();
      for (let index = 0; index < 2; index++) {
        const terminate = yield* takeBeforeExit(h, call, "thread/backgroundTerminals/terminate");
        terminated.add(terminate.params.processId);
        yield* h.reply(terminate, { terminated: true });
      }
      assert.deepEqual([...terminated].sort(), ["77", "99"]);
      yield* Fiber.join(call);
      yield* h.watch((event) => event.type === "turn.terminal");
      assertReleased(h, "failed");
      assert.isFalse(yield* h.pending);
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("releases a failed root's process tracking with its deferred terminal (#332)", () =>
    Effect.gen(function* () {
      const h = yield* makeBoundary();
      yield* h.start();
      yield* h.item(nativeThreadId, rootTurnId, "root-command", "99");
      yield* h.child;
      yield* h.end(nativeThreadId, rootTurnId, "failed");
      assert.lengthOf(h.terminals(), 0);
      yield* h.item(childThreadId, childTurnId, "child-command", "77", true);
      yield* h.end(childThreadId, childTurnId, "completed");
      yield* h.watch((event) => event.type === "turn.terminal");
      assertReleased(h, "failed");
      assert.isFalse(yield* h.pending);
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("reports exact deferred and cut-in ownership without changing a native failure", () =>
    Effect.gen(function* () {
      const h = yield* makeBoundary();
      yield* h.start();
      yield* h.child;
      yield* h.end(nativeThreadId, rootTurnId, "failed");
      yield* h.watch(
        (event) => event.type === "provider_turn.updated" && event.providerTurn.status === "failed",
      );
      const count = h.requests.length;
      assert.equal(yield* h.hold, "descendants");
      assert.equal(yield* h.hold, "descendants");
      assert.equal(h.requests.length, count, "the view must perform no native I/O");
      const view = h.runtime.endedTurnHold;
      if (view === undefined) return assert.fail("Missing Codex hold view");
      assert.equal(
        yield* view({
          providerThread: h.providerThread,
          providerTurnId: ProviderTurnId.make("another-turn"),
        }),
        undefined,
      );
      const call = yield* forkCall(h.cutIn);
      const child = yield* takeBeforeExit(h, call, "turn/interrupt");
      assert.equal(child.params.turnId, childTurnId);
      yield* h.reject(child);
      assert.equal((yield* Fiber.await(call))._tag, "Failure");
      assert.equal(yield* h.hold, "cut-in");
      yield* h.item(childThreadId, childTurnId, "child-command", "77", true);
      yield* h.end(childThreadId, childTurnId, "completed");
      yield* h.watch((event) => event.type === "turn.terminal");
      assertReleased(h, "failed");
      assert.equal(yield* h.hold, undefined);
      assert.deepEqual(
        h.turnUpdates(rootTurnId).map((turn) => turn.status),
        ["running", "failed"],
      );
      assert.lengthOf(
        h.requests.filter((request) => request.method === "turn/interrupt"),
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect(
    "retains a completed late child's process 88 admitted during the final terminate reply (N9)",
    () =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        yield* h.end(nativeThreadId, rootTurnId, "completed");
        const call = yield* forkCall(h.cutIn);
        const first = yield* takeBeforeExit(h, call, "turn/interrupt");
        assert.equal(first.params.turnId, childTurnId);
        yield* h.reply(first);
        const initialTerminate = yield* h.take("thread/backgroundTerminals/terminate");
        assert.equal(yield* h.hold, "cut-in");
        const startChild = (id: string) =>
          h
            .notify("turn/started", {
              threadId: childThreadId,
              turn: makeCodexReplayTurn({ id, status: "inProgress" }),
            })
            .pipe(
              Effect.andThen(
                h.watch(
                  (event) =>
                    event.type === "provider_turn.updated" &&
                    event.providerTurn.nativeTurnRef?.nativeId === id,
                ),
              ),
            );
        // Each new turn follows its predecessor's native end. Turn 3 arrives
        // after the late-descendant snapshot, so only the final rescan reaches it.
        yield* h.end(childThreadId, childTurnId, "interrupted");
        yield* startChild("child-turn-2");
        yield* h.reject(initialTerminate);
        const second = yield* takeBeforeExit(h, call, "turn/interrupt");
        assert.equal(second.params.turnId, "child-turn-2");
        yield* h.end(childThreadId, "child-turn-2", "interrupted");
        yield* startChild("child-turn-3");
        yield* h.reply(second);
        // Keep 77 uncontained across the gap between turns 2 and 3. Otherwise
        // native completion legitimately releases the cohort before turn 3 exists.
        const beforeRescan = yield* takeBeforeExit(h, call, "thread/backgroundTerminals/terminate");
        assert.equal(beforeRescan.params.processId, "77");
        yield* h.reply(beforeRescan, { terminated: true });
        const third = yield* takeBeforeExit(h, call, "turn/interrupt");
        assert.equal(third.params.turnId, "child-turn-3");
        yield* h.item(childThreadId, "child-turn-3", "rescan-command", "99");
        yield* h.end(childThreadId, "child-turn-3", "interrupted");
        yield* h.reply(third);
        const finalTerminate = yield* takeBeforeExit(
          h,
          call,
          "thread/backgroundTerminals/terminate",
        );
        assert.equal(finalTerminate.params.processId, "99");
        yield* startChild("late-turn");
        yield* h.item(childThreadId, "late-turn", "late-command", "88");
        yield* h.end(childThreadId, "late-turn", "completed");
        yield* h.reply(finalTerminate, { terminated: true });
        // No native turn is active, but 88 is still running. An active-only
        // success tail would publish an invented command end and lose its owner.
        const result = yield* Fiber.await(call);
        assert.equal(result._tag, "Failure");
        assert.equal(h.itemUpdates("late-command").at(-1)?.status, "running");
        assert.equal(yield* h.hold, "cut-in");
        assert.isTrue(yield* h.pendingChild);
        const retry = yield* forkCall(h.cutIn);
        const lateTerminate = yield* takeBeforeExit(
          h,
          retry,
          "thread/backgroundTerminals/terminate",
        );
        assert.equal(lateTerminate.params.processId, "88");
        yield* h.reply(lateTerminate, { terminated: true });
        yield* Fiber.join(retry);
        yield* h.watch(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "late-command" &&
            event.turnItem.status === "interrupted",
        );
        assert.deepEqual(
          h.itemUpdates("late-command").map((item) => item.status),
          ["running", "interrupted"],
        );
        assert.deepEqual(
          h.turnUpdates(rootTurnId).map((turn) => turn.status),
          ["running", "completed"],
        );
        assert.isFalse(yield* h.pendingChild);
        assert.equal(yield* h.hold, undefined);
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect(
    "keeps publication owned when cancelled at native containment, then Stop releases it once (N10/N16)",
    () =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        const call = yield* forkCall(h.cutIn);
        yield* ackRoot(h);
        yield* h.reply(yield* h.take("turn/interrupt"));
        yield* h.end(childThreadId, childTurnId, "interrupted");
        const terminate = yield* h.take("thread/backgroundTerminals/terminate");
        assert.isTrue(yield* h.pendingChild);
        yield* Fiber.interrupt(call);
        yield* h.reply(terminate, { terminated: true });
        yield* h.barrier;
        assert.equal(yield* h.hold, "cut-in");
        assert.lengthOf(h.terminals(), 0);
        assert.equal(h.turnUpdates(rootTurnId).at(-1)?.status, "running");
        assert.equal(h.itemUpdates("child-command").at(-1)?.status, "running");
        const stop = yield* forkCall(h.stop);
        yield* h.reply(yield* takeBeforeExit(h, stop, "thread/backgroundTerminals/terminate"), {
          terminated: true,
        });
        yield* Fiber.join(stop);
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h);
        assert.lengthOf(
          h.itemUpdates("child-command").filter((item) => item.status === "interrupted"),
          1,
        );
        assert.isFalse(yield* h.pendingChild);
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect(
    "leaves later native work outside a cohort that released before its interrupt answer (L10)",
    () =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        yield* h.end(nativeThreadId, rootTurnId, "completed");
        const call = yield* forkCall(h.cutIn);
        const child = yield* takeBeforeExit(h, call, "turn/interrupt");
        // Codex's native end and the process's own completion settle the cohort
        // while the earlier interrupt request is still awaiting its answer.
        yield* h.item(childThreadId, childTurnId, "child-command", "77", true);
        yield* h.end(childThreadId, childTurnId, "completed");
        assert.isFalse(yield* h.pending);
        yield* h.notify("turn/started", {
          threadId: childThreadId,
          turn: makeCodexReplayTurn({ id: "after-release", status: "inProgress" }),
        });
        yield* h.item(childThreadId, "after-release", "after-release-command", "88");
        yield* h.reply(child);
        yield* Effect.raceFirst(
          Fiber.join(call),
          h
            .take("turn/interrupt")
            .pipe(Effect.andThen(Effect.die("A released cut-in reached later native work"))),
        );
        assert.equal(h.itemUpdates("after-release-command").at(-1)?.status, "running");
        assert.isTrue(yield* h.pendingChild);
        assert.equal(yield* h.hold, undefined);
        yield* h.item(childThreadId, "after-release", "after-release-command", "88", true);
        yield* h.end(childThreadId, "after-release", "completed");
        assert.isFalse(yield* h.pendingChild);
        assert.deepEqual(
          h.itemUpdates("after-release-command").map((item) => item.status),
          ["running", "completed"],
        );
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("joins an exiting cut-in before Stop installs its completion waiter (N13)", () =>
    Effect.gen(function* () {
      const cleanupStarted = yield* Deferred.make<void>();
      const finishCleanup = yield* Deferred.make<void>();
      const h = yield* makeBoundary({
        interruptCleanup: Deferred.succeed(cleanupStarted, undefined).pipe(
          Effect.andThen(Deferred.await(finishCleanup)),
        ),
      });
      yield* h.start();
      yield* h.child;
      const call = yield* forkCall(h.cutIn);
      yield* ackRoot(h);
      yield* h.reply(yield* h.take("turn/interrupt"));
      yield* h.take("thread/backgroundTerminals/terminate");
      const stop = yield* forkCall(h.stop);
      const cancelling = yield* Fiber.interrupt(call).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(cleanupStarted);
      // The serial reader remains live while cleanup is held. Let an actual
      // native frame pass before observing whether Stop has sent any I/O.
      yield* h.barrier;
      const terminationCountDuringCleanup = h.requests.filter(
        (request) => request.method === "thread/backgroundTerminals/terminate",
      ).length;
      const stopDuringCleanup = stop.pollUnsafe();
      yield* Deferred.succeed(finishCleanup, undefined);
      yield* Fiber.join(cancelling);
      assert.equal(
        terminationCountDuringCleanup,
        1,
        "Stop must join the old call's actual cleanup",
      );
      assert.equal(stopDuringCleanup, undefined);
      yield* h.reply(yield* takeBeforeExit(h, stop, "thread/backgroundTerminals/terminate"), {
        terminated: true,
      });
      yield* h.end(childThreadId, childTurnId, "interrupted");
      // No clock advance: Stop's real completion waiter must survive old cleanup.
      yield* Fiber.join(stop);
      yield* h.watch((event) => event.type === "turn.terminal");
      assertReleased(h);
      assert.lengthOf(
        h.requests.filter(
          (request) => request.method === "turn/interrupt" && request.params.turnId === childTurnId,
        ),
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect.each(["unanswered", "rejected"] as const)(
    "fails a %s goal pause before sending an interrupt and retains goal ownership (N12a)",
    (mode) =>
      Effect.gen(function* () {
        const h = yield* makeBoundary({ holdGoalActivation: true });
        const starting = yield* h
          .start("/goal Ship")
          .pipe(Effect.forkScoped({ startImmediately: true }));
        const activation = yield* h.take("thread/goal/set");
        assert.equal(activation.params.status, "active");
        yield* h.notify("thread/goal/updated", {
          threadId: nativeThreadId,
          turnId: rootTurnId,
          goal: goal("active"),
        });
        yield* h.barrier;
        const call = yield* forkCall(h.cutIn);
        const pause = yield* h.take("thread/goal/set");
        if (mode === "rejected") yield* h.reject(pause);
        else yield* TestClock.adjust("10 seconds");
        const result = call.pollUnsafe();
        if (mode === "rejected") {
          const failed = yield* Effect.raceFirst(
            Fiber.await(call),
            h
              .take("turn/interrupt")
              .pipe(
                Effect.andThen(Effect.die("A rejected goal pause must fail before any interrupt")),
              ),
          );
          assert.equal(failed._tag, "Failure");
        } else assert.isDefined(result, "the initial goal pause must be bounded");
        const exit = yield* Fiber.await(call);
        assert.equal(exit._tag, "Failure");
        if (exit._tag === "Failure")
          assert.include(Cause.pretty(exit.cause), "did not pause the goal within 10 seconds");
        assert.lengthOf(
          h.requests.filter((request) => request.method === "turn/interrupt"),
          0,
        );
        yield* h.reply(activation, { goal: goal("active") });
        // A failed cut-in must not mark the activation stopped and make the
        // original /goal caller pause it again when its response arrives.
        yield* Effect.raceFirst(
          Fiber.join(starting),
          h
            .take("thread/goal/set")
            .pipe(
              Effect.andThen(Effect.die("A failed cut-in stopped the pending goal activation")),
            ),
        );
        yield* h.end(nativeThreadId, rootTurnId, "completed");
        assert.lengthOf(h.terminals(), 0);
        yield* h.notify("turn/started", {
          threadId: nativeThreadId,
          turn: makeCodexReplayTurn({ id: "goal-next", status: "inProgress" }),
        });
        const next = yield* h.watch(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeTurnRef?.nativeId === "goal-next",
        );
        assert.equal(
          next.type === "provider_turn.updated" && next.providerTurn.runAttemptId,
          RunAttemptId.make("r7-attempt"),
        );
        assert.lengthOf(h.terminals(), 0);
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect.each([false, true])(
    "retains a held goal through failed cut-in and its factual continuation=%s (N10b/N12c-d)",
    (continuation) =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start("/goal Ship");
        yield* h.child;
        yield* h.end(nativeThreadId, rootTurnId, "completed");
        const root = h.turnUpdates(rootTurnId)[0];
        if (root === undefined) return assert.fail("Missing held goal root");
        const steering = continuation
          ? yield* h.runtime
              .steerTurn({
                ...makeCodexTestTurnInput({
                  threadId: h.threadId,
                  providerThread: h.providerThread,
                  now: yield* DateTime.now,
                  attemptId: RunAttemptId.make("r7-attempt"),
                  text: "update the goal",
                }),
                providerTurnId: root.id,
              })
              .pipe(Effect.forkScoped({ startImmediately: true }))
          : undefined;
        const call = yield* forkCall(h.cutIn);
        yield* h.reply(yield* h.take("thread/goal/set"), { goal: goal("paused") });
        yield* h.reject(yield* h.take("turn/interrupt"));
        const failure = yield* Fiber.await(call);
        assert.equal(failure._tag, "Failure");
        assert.equal(h.turnUpdates(rootTurnId).at(-1)?.status, "running");
        assert.lengthOf(h.terminals(), 0);
        yield* TestClock.adjust("5 seconds");
        assert.lengthOf(h.terminals(), 0);
        if (steering !== undefined)
          assert.equal(
            steering.pollUnsafe(),
            undefined,
            "hold.next must stay unresolved until a factual transition",
          );
        if (continuation) {
          yield* h.notify("turn/started", {
            threadId: nativeThreadId,
            turn: makeCodexReplayTurn({ id: "goal-next", status: "inProgress" }),
          });
          const next = yield* h.watch(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === "goal-next",
          );
          assert.equal(
            next.type === "provider_turn.updated" && next.providerTurn.runAttemptId,
            RunAttemptId.make("r7-attempt"),
          );
          const steer = yield* h.take("turn/steer");
          assert.equal(steer.params.expectedTurnId, "goal-next");
          yield* h.reply(steer, { turnId: "goal-next" });
          if (steering !== undefined) yield* Fiber.join(steering);
        }
        const retry = yield* forkCall(h.cutIn);
        const nextRequest = yield* takeBeforeExit(h, retry, "turn/interrupt");
        if (continuation) {
          assert.equal(nextRequest.params.turnId, "goal-next");
          yield* h.reply(nextRequest);
          yield* h.end(nativeThreadId, "goal-next", "interrupted");
          const child = yield* h.take("turn/interrupt");
          assert.equal(child.params.turnId, childTurnId);
          yield* h.reply(child);
        } else {
          assert.equal(nextRequest.params.turnId, childTurnId);
          yield* h.reply(nextRequest);
        }
        yield* h.end(childThreadId, childTurnId, "interrupted");
        yield* h.reply(yield* h.take("thread/backgroundTerminals/terminate"), { terminated: true });
        yield* Fiber.join(retry);
        yield* h.watch((event) => event.type === "turn.terminal");
        assert.lengthOf(h.terminals(), 1);
        assert.equal(h.terminals()[0]?.status, "interrupted");
        assert.deepEqual(
          h.turnUpdates(rootTurnId).map((turn) => turn.status),
          ["running", "completed"],
        );
        const commandEnd = h.events.findIndex(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.nativeItemRef?.nativeId === "child-command" &&
            event.turnItem.status === "interrupted",
        );
        const terminal = h.events.findIndex((event) => event.type === "turn.terminal");
        assert.isAbove(terminal, commandEnd);
        if (!continuation) {
          assert.deepEqual(
            h.events.slice(commandEnd + 1, terminal + 1).map((event) => event.type),
            ["provider_turn.updated", "turn.terminal"],
            "release publishes its kept command, held goal end and terminal contiguously",
          );
        }
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect(
    "keeps a child's approval answerable on the withheld root until true release (N15)",
    () =>
      Effect.gen(function* () {
        const h = yield* makeBoundary();
        yield* h.start();
        yield* h.child;
        yield* h.emit({
          id: "native-approval",
          method: "item/commandExecution/requestApproval",
          params: {
            threadId: childThreadId,
            turnId: childTurnId,
            itemId: "child-approval",
            command: "sensitive command",
            startedAtMs: 1782622440500,
          },
        });
        const approval = yield* h.watch((event) => event.type === "runtime_request.updated");
        if (approval.type !== "runtime_request.updated")
          return assert.fail("Missing native approval");
        assert.equal(approval.runtimeRequest.providerTurnId, h.turnUpdates(rootTurnId)[0]?.id);
        const call = yield* forkCall(h.cutIn);
        yield* ackRoot(h);
        yield* h.reject(yield* h.take("turn/interrupt"));
        assert.equal((yield* Fiber.await(call))._tag, "Failure");
        assert.equal(h.turnUpdates(rootTurnId).at(-1)?.status, "running");
        assert.lengthOf(h.terminals(), 0);
        yield* h.runtime.respondToRuntimeRequest({
          requestId: approval.runtimeRequest.id,
          decision: "accept",
        });
        yield* Deferred.await(h.responseObserved);
        assert.deepEqual(h.responses.find((frame) => frame.id === "native-approval")?.result, {
          decision: "accept",
        });
        yield* h.item(childThreadId, childTurnId, "child-command", "77", true);
        yield* h.end(childThreadId, childTurnId, "completed");
        yield* h.watch((event) => event.type === "turn.terminal");
        assertReleased(h);
        assert.equal(h.itemUpdates("child-command").at(-1)?.status, "completed");
        const completedCommand = h.itemUpdates("child-command").at(-1);
        assert.equal(
          completedCommand?.type === "command_execution" && completedCommand.output,
          "REAL_NATIVE_OUTPUT",
        );
      }).pipe(Effect.scoped, Effect.provide(layers)),
  );

  it.effect("keeps reading native frames while a late child's interrupt awaits its reply", () =>
    Effect.gen(function* () {
      const lateChildTurnId = "r7-late-child-turn";
      const h = yield* makeBoundary();
      yield* h.start();
      yield* h.child;
      yield* h.end(nativeThreadId, rootTurnId, "failed");
      yield* h.notify("turn/started", {
        threadId: childThreadId,
        turn: makeCodexReplayTurn({ id: lateChildTurnId, status: "inProgress" }),
      });
      const interrupt = yield* h.take("turn/interrupt");
      assert.equal(interrupt.params.turnId, lateChildTurnId);
      // The reply is parsed by the same reader that delivered turn/started, so
      // a frame behind the unanswered interrupt must pass with no clock advance.
      yield* h.barrier;
      yield* h.reply(interrupt);
      yield* h.end(childThreadId, lateChildTurnId, "interrupted");
      assert.lengthOf(h.turnUpdates(lateChildTurnId), 0);
      assert.lengthOf(
        h.requests.filter(
          (request) =>
            request.method === "turn/interrupt" && request.params.turnId === lateChildTurnId,
        ),
        1,
      );
    }).pipe(Effect.scoped, Effect.provide(layers)),
  );
});
