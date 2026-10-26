import * as FileSystem from "effect/FileSystem";
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { OpencodeClient, ToolPart } from "@opencode-ai/sdk/v2";
import {
  CheckpointId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  MessageId,
  ThreadId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import { OpenCodeSettings } from "../settings.ts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { OPEN_CODE_BARE_CONFIG } from "@t3tools/provider-core/server/BareProviderRuntime";

import {
  advanceOpenCodePromptAdmission,
  cancelOpenCodePromptAdmission,
  openCodeBoundaryAfterProviderTurn,
  openCodeChildPermissionRules,
  openCodePermissionRules,
  makeOpenCodeProtocolLogger,
  makeOpenCodeAdapterV2,
  OPENCODE_PROVIDER,
  reconcileOpenCodePromptAdmissionStatus,
} from "./adapter.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const OPEN_CODE_TEST_SETTINGS = Schema.decodeSync(OpenCodeSettings)({
  serverUrl: "http://test.invalid",
});

function promiseGate<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function asyncEventStream() {
  const values: Array<{ value: unknown; handled: ReturnType<typeof promiseGate<void>> }> = [];
  const waiters: Array<(value: IteratorResult<unknown>) => void> = [];
  let previousHandled: ReturnType<typeof promiseGate<void>> | undefined;
  let closed = false;
  return {
    close() {
      closed = true;
      for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
    },
    push(value: unknown) {
      const handled = promiseGate<void>();
      const waiter = waiters.shift();
      if (waiter) {
        previousHandled = handled;
        waiter({ done: false, value });
      } else values.push({ value, handled });
      return handled.promise;
    },
    stream: {
      [Symbol.asyncIterator]() {
        return {
          next: () => {
            previousHandled?.resolve();
            if (closed) return Promise.resolve({ done: true as const, value: undefined });
            const entry = values.shift();
            if (entry !== undefined) {
              previousHandled = entry.handled;
              return Promise.resolve({ done: false as const, value: entry.value });
            }
            return new Promise<IteratorResult<unknown>>((resolve) => waiters.push(resolve));
          },
        };
      },
    },
  };
}

const OPENCODE_TEST_SETTINGS = Schema.decodeSync(OpenCodeSettings)({});

function runtimePolicy(
  runtimeMode: ProviderAdapter.ProviderAdapterV2RuntimePolicy["runtimeMode"],
  override: Partial<ProviderAdapter.ProviderAdapterV2RuntimePolicy> = {},
): ProviderAdapter.ProviderAdapterV2RuntimePolicy {
  return ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
    runtimeMode,
    interactionMode: "default",
    cwd: null,
    ...override,
  });
}

function permissionAction(rules: ReturnType<typeof openCodePermissionRules>, permission: string) {
  return rules.findLast((rule) => rule.permission === "*" || rule.permission === permission)
    ?.action;
}

function providerTurn(input: {
  readonly id: string;
  readonly ordinal: number;
  readonly nativeId: string | null;
}): OrchestrationV2ProviderTurn {
  return {
    id: ProviderTurnId.make(input.id),
    providerThreadId: ProviderThreadId.make("provider-thread:opencode-test"),
    nodeId: NodeId.make(`node:${input.id}`),
    runAttemptId: null,
    nativeTurnRef:
      input.nativeId === null
        ? null
        : { driver: OPENCODE_PROVIDER, nativeId: input.nativeId, strength: "weak" },
    ordinal: input.ordinal,
    status: "completed",
    startedAt: null,
    completedAt: null,
  };
}

const makeOpenCodeRuntimeHarness = Effect.fn("makeOpenCodeRuntimeHarness")(function* (
  suffix: string,
  nativeSessionId: string,
  client: object,
  bareRoot?: string,
  initialAgent?: string,
  host: Partial<ProviderHost.ProviderHost["Service"]> = {},
) {
  const instanceId = ProviderInstanceId.make(`opencode-${suffix}`);
  const threadId = ThreadId.make(`thread-opencode-${suffix}`);
  const modelSelection = {
    instanceId,
    model: "anthropic/claude-sonnet",
    options: initialAgent === undefined ? [] : [{ id: "agent", value: initialAgent }],
  };
  const policy = runtimePolicy("full-access", {
    cwd: "/workspace",
    ...(bareRoot === undefined ? {} : { bare: { additions: [] } }),
  });
  const adapter = yield* makeOpenCodeAdapterV2({
    instanceId,
    settings: bareRoot === undefined ? OPEN_CODE_TEST_SETTINGS : OPENCODE_TEST_SETTINGS,
    environment: bareRoot === undefined ? {} : { HOME: bareRoot },
  }).pipe(
    Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, {
      connectToOpenCodeServer: () =>
        Effect.succeed({
          url: "http://test.invalid",
          external: bareRoot === undefined,
          exitCode: null,
        }),
      createOpenCodeSdkClient: () => client,
    } as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"]),
    Effect.provideService(ProviderHost.ProviderHost, {
      paths: { cwd: "/workspace", stateDir: bareRoot },
      resolveAttachmentPath: () => null,
      ...host,
    } as unknown as ProviderHost.ProviderHost["Service"]),
  );
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make(`session-opencode-${suffix}`),
    modelSelection,
    runtimePolicy: policy,
  });
  const providerThread = yield* runtime.ensureThread({
    threadId,
    modelSelection,
    runtimePolicy: policy,
  });
  const now = yield* DateTime.now;
  const startTurn = (
    text = "hello",
    startProviderThread: OrchestrationV2ProviderThread = providerThread,
    turnPolicyOrOverride: ProviderAdapter.ProviderAdapterV2RuntimePolicy | Partial<ProviderAdapter.ProviderAdapterV2TurnInput> = policy,
  ) =>
    runtime.startTurn({
      appThread: {
        id: threadId,
        projectId: ProjectId.make(`project-opencode-${suffix}`),
        title: suffix,
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThread.id,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
      threadId,
      runId: RunId.make(`run-opencode-${suffix}`),
      runOrdinal: 1,
      providerTurnOrdinal: 1,
      attemptId: RunAttemptId.make(`attempt-opencode-${suffix}`),
      rootNodeId: NodeId.make(`node-opencode-${suffix}`),
      providerThread: startProviderThread,
      message: {
        createdBy: "user",
        creationSource: "web",
        messageId: MessageId.make(`message-opencode-${suffix}`),
        text,
        attachments: [],
      },
      modelSelection,
      runtimePolicy: "runtimeMode" in turnPolicyOrOverride ? turnPolicyOrOverride : policy,
      ...("runtimeMode" in turnPolicyOrOverride ? {} : turnPolicyOrOverride),
    });
  return {
    nativeSessionId,
    now,
    policy,
    providerThread,
    runId: RunId.make(`run-opencode-${suffix}`),
    runtime,
    startTurn,
    threadId,
  };
});

describe("OpenCodeAdapterV2", () => {
  it.effect("classifies APIError 429 as a usage limit", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const harness = yield* makeOpenCodeRuntimeHarness("usage-limit", "root", {
        event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
        session: {
          create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
          promptAsync: async () => ({ data: true }),
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.error",
          properties: {
            sessionID: "root",
            error: {
              name: "APIError",
              data: {
                message: "Provider rejected request",
                statusCode: 429,
                isRetryable: true,
              },
            },
          },
        }),
      );
      const terminal = (yield* Fiber.join(received)).find(
        (event) => event.type === "turn.terminal",
      );
      assert.equal(terminal?.status, "failed");
      assert.equal(terminal?.failure?.class, "usage_limit");
    }).pipe(
      Effect.provide(Layer.merge(IdAllocator.layer, McpProviderSessions.layer)),
      Effect.scoped,
    ),
  );

  it.effect("applies a changed runtime policy to the session on resume and turn start", () =>
    Effect.gen(function* () {
      const updates: Array<{ sessionID: string; permission: unknown }> = [];
      const fullAccess = runtimePolicy("full-access", { cwd: "/workspace" });
      const supervised = runtimePolicy("approval-required", { cwd: "/workspace" });
      const harness = yield* makeOpenCodeRuntimeHarness("policy-change", "root", {
        event: { subscribe: async () => ({ stream: asyncEventStream().stream }) },
        session: {
          create: async (input: { permission: unknown }) => ({
            data: {
              id: "root",
              permission: input.permission,
              time: { created: 1, updated: 1 },
            },
          }),
          get: async () => ({
            data: {
              id: "root",
              permission: openCodePermissionRules(fullAccess),
              time: { created: 1, updated: 1 },
            },
          }),
          update: async (input: { sessionID: string; permission: unknown }) => {
            updates.push(input);
            return { data: { id: input.sessionID, time: { created: 1, updated: 2 } } };
          },
          promptAsync: async () => ({ data: true }),
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
        },
      });

      yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
        runtimePolicy: fullAccess,
      });
      assert.deepStrictEqual(updates, []);

      yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
        runtimePolicy: supervised,
      });
      assert.deepStrictEqual(updates, [
        { sessionID: "root", permission: openCodePermissionRules(supervised) },
      ]);

      yield* harness.startTurn("hello", harness.providerThread, fullAccess);
      assert.deepStrictEqual(updates.slice(1), [
        { sessionID: "root", permission: openCodePermissionRules(fullAccess) },
      ]);
    }).pipe(
      Effect.provide(Layer.merge(IdAllocator.layer, McpProviderSessions.layer)),
      Effect.scoped,
    ),
  );

  it.effect("applies a changed runtime policy to existing subagent sessions", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const push = (event: unknown) => Effect.promise(() => nativeEvents.push(event));
      const updates: Array<{
        sessionID: string;
        permission: ReturnType<typeof openCodePermissionRules>;
      }> = [];
      const fullAccess = runtimePolicy("full-access", { cwd: "/workspace" });
      const supervised = runtimePolicy("approval-required", { cwd: "/workspace" });
      const root = "ses_root";
      const child = "ses_child";
      const taskDeny = { permission: "task", pattern: "*", action: "deny" as const };
      const nativePermissions = new Map<string, (typeof updates)[number]["permission"]>([
        [root, openCodePermissionRules(fullAccess)],
        [child, [taskDeny]],
      ]);
      let failNextChildUpdate = false;
      const harness = yield* makeOpenCodeRuntimeHarness("child-policy-change", root, {
        event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
        session: {
          create: async () => ({ data: { id: root, time: { created: 1, updated: 1 } } }),
          get: async ({ sessionID }: { sessionID: string }) => ({
            data: {
              id: sessionID,
              ...(sessionID === child ? { parentID: root } : {}),
              permission: nativePermissions.get(sessionID),
              time: { created: 1, updated: 1 },
            },
          }),
          update: async (input: (typeof updates)[number]) => {
            if (input.sessionID === child && failNextChildUpdate) {
              failNextChildUpdate = false;
              throw new Error("child update failed");
            }
            updates.push(input);
            nativePermissions.set(input.sessionID, input.permission);
            return { data: { id: input.sessionID, time: { created: 1, updated: 2 } } };
          },
          promptAsync: async () => ({ data: true }),
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
        },
      });

      yield* harness.startTurn("hello", harness.providerThread, fullAccess);
      yield* push({
        type: "message.part.updated",
        properties: {
          sessionID: root,
          part: {
            id: "prt_task",
            sessionID: root,
            messageID: "msg_root_assistant",
            type: "tool",
            tool: "task",
            callID: "call_task",
            state: {
              status: "running",
              input: { description: "Explore", prompt: "look around", subagent_type: "general" },
              title: "Explore",
              metadata: { parentSessionId: root, sessionId: child },
              time: { start: 3 },
            },
          },
        },
      });
      const childUpdates = () => updates.filter((update) => update.sessionID === child);
      assert.equal(permissionAction(childUpdates().at(-1)!.permission, "bash"), "allow");

      yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
        runtimePolicy: supervised,
      });
      const childRules = childUpdates().at(-1)!.permission;
      assert.equal(childUpdates().length, 2);
      assert.equal(permissionAction(childRules, "bash"), "ask");
      assert.equal(permissionAction(childRules, "task"), "deny");
      assert.deepStrictEqual(childRules, openCodeChildPermissionRules(supervised, [taskDeny]));

      yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
        runtimePolicy: fullAccess,
      });
      assert.equal(permissionAction(nativePermissions.get(child)!, "bash"), "allow");

      failNextChildUpdate = true;
      const failed = yield* Effect.exit(
        harness.runtime.resumeThread({
          providerThread: harness.providerThread,
          runtimePolicy: supervised,
        }),
      );
      assert.isTrue(Exit.isFailure(failed));
      yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
        runtimePolicy: supervised,
      });
      assert.equal(permissionAction(nativePermissions.get(child)!, "bash"), "ask");
      assert.equal(permissionAction(nativePermissions.get(child)!, "task"), "deny");
    }).pipe(
      Effect.provide(Layer.merge(IdAllocator.layer, McpProviderSessions.layer)),
      Effect.scoped,
    ),
  );

  it.effect.each([
    "getaddrinfo ENOTFOUND api.provider.test",
    "connect ECONNRESET",
    "Invalid API key",
  ])("classifies session failure %s", (message) =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const harness = yield* makeOpenCodeRuntimeHarness("network", "root", {
        event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
        session: {
          create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
          promptAsync: async () => ({ data: true }),
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.error",
          properties: { sessionID: "root", error: { name: "UnknownError", data: { message } } },
        }),
      );
      const terminal = (yield* Fiber.join(received)).find(
        (event) => event.type === "turn.terminal",
      );
      assert.equal(terminal?.status, "failed");
      assert.equal(
        terminal?.failure?.class,
        message === "Invalid API key" ? "provider_error" : "transport_error",
      );
      assert.equal(terminal?.failure?.retryable, message === "Invalid API key" ? null : true);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );
  it.effect.each([
    { name: "default build", interactionMode: "default", agent: undefined },
    { name: "inherited plan after build resume", interactionMode: "plan", agent: undefined },
    { name: "explicit plan", interactionMode: "default", agent: "plan" },
    { name: "unsupported explicit agent", interactionMode: "default", agent: "explore" },
  ] as const)("bare $name keeps the effective permissions before inference", (choice) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped();
      const nativeEvents = asyncEventStream();
      yield* Effect.addFinalizer(() => Effect.sync(() => nativeEvents.close()));
      const order: string[] = [];
      const permissions: Array<ReturnType<typeof openCodePermissionRules>> = [];
      let prompt: Record<string, unknown> | undefined;
      const native = { id: "bare-plan", time: { created: 1, updated: 1 } };
      const opened = yield* makeOpenCodeRuntimeHarness(
        "bare-plan",
        native.id,
        {
          event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
          config: { get: async () => ({ data: OPEN_CODE_BARE_CONFIG }) },
          mcp: { status: async () => ({ data: {} }) },
          tool: { list: async () => ({ data: [] }) },
          session: {
            create: async () => ({ data: native }),
            get: async () => ({ data: native }),
            update: async (input: { permission: ReturnType<typeof openCodePermissionRules> }) => {
              order.push("permissions");
              permissions.push(input.permission);
              return { data: native };
            },
            promptAsync: async (input: Record<string, unknown>) => {
              order.push("prompt");
              prompt = input;
              return {};
            },
          },
        },
        root,
        choice.agent,
      ).pipe(Effect.result);
      if (choice.agent === "explore") {
        assert.strictEqual(opened._tag, "Failure");
        if (opened._tag === "Failure")
          assert.include(
            makeProviderFailure({ cause: opened.failure }).message,
            "agent explore is unsupported",
          );
        assert.deepStrictEqual(order, []);
        return;
      }
      assert.strictEqual(opened._tag, "Success");
      if (opened._tag === "Failure") return;
      const h = opened.success;
      const resumed = yield* h.runtime.resumeThread({
        providerThread: h.providerThread,
        runtimePolicy: h.policy,
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode-bare-plan"),
          model: "anthropic/claude-sonnet",
        },
      });
      yield* h.startTurn("Only the task", resumed, {
        runtimePolicy: { ...h.policy, interactionMode: choice.interactionMode },
      });
      assert.deepStrictEqual(order, ["permissions", "prompt"]);
      assert.strictEqual(
        permissions[0]!.findLast((r) => r.permission === "edit" && r.pattern === "*")?.action,
        choice.agent === "plan" || choice.interactionMode === "plan" ? "deny" : "allow",
      );
      assert.strictEqual(
        prompt?.agent,
        choice.agent ?? (choice.interactionMode === "plan" ? "plan" : undefined),
      );
      assert.notProperty(prompt, "system");
      assert.deepStrictEqual(prompt?.parts, [{ type: "text", text: "Only the task" }]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, IdAllocator.layer, McpProviderSessions.layer),
      ),
    ),
  );
  it.effect.each(["completed", "failed", "unavailable"] as const)(
    "normalizes OpenCode step usage for %s turns",
    (ending) =>
      Effect.gen(function* () {
        const nativeEvents = asyncEventStream();
        let promptId = "";
        const harness = yield* makeOpenCodeRuntimeHarness(`usage-${ending}`, "root", {
          event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
          session: {
            create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
            promptAsync: async (input: { messageID: string }) => {
              promptId = input.messageID;
              return { data: true };
            },
            abort: async () => ({ data: true }),
            children: async () => ({ data: [] }),
          },
        });
        yield* harness.startTurn();
        const received = yield* harness.runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.updated",
            properties: {
              sessionID: "root",
              info: { id: promptId, role: "user", time: { created: 1 } },
            },
          }),
        );
        const step = (id: string, messageID = "assistant") => ({
          type: "message.part.updated",
          properties: {
            part: {
              type: "step-finish",
              id,
              sessionID: "root",
              messageID,
              reason: "stop",
              cost: 0.01,
              tokens: { input: 10, output: 5, reasoning: 2, cache: { read: 3, write: 4 } },
            },
          },
        });
        if (ending !== "unavailable") {
          yield* Effect.promise(() => nativeEvents.push(step("one")));
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "message.updated",
              properties: {
                sessionID: "root",
                info: {
                  id: "assistant",
                  role: "assistant",
                  time: { created: 1 },
                  parentID: promptId,
                },
              },
            }),
          );
          yield* Effect.promise(() => nativeEvents.push(step("one")));
          yield* Effect.promise(() => nativeEvents.push(step("two")));
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "message.part.removed",
              properties: { sessionID: "root", messageID: "assistant", partID: "one" },
            }),
          );
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "message.updated",
              properties: {
                sessionID: "root",
                info: {
                  id: "old-assistant",
                  role: "assistant",
                  time: { created: 1 },
                  parentID: "old-prompt",
                },
              },
            }),
          );
          yield* Effect.promise(() => nativeEvents.push(step("old", "old-assistant")));
        }
        if (ending === "failed") {
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "session.error",
              properties: {
                sessionID: "root",
                error: { name: "UnknownError", data: { message: "failed" } },
              },
            }),
          );
        } else {
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "session.status",
              properties: { sessionID: "root", status: { type: "busy" } },
            }),
          );
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "session.status",
              properties: { sessionID: "root", status: { type: "idle" } },
            }),
          );
        }
        const events = yield* Fiber.join(received);
        const completed = events.findLast((event) => event.type === "provider_turn.updated");
        assert.deepEqual(
          completed?.providerTurn.turnTokenUsage,
          ending === "unavailable"
            ? { usageStatus: "unavailable", usageScope: "main_agent", hasSubagents: false }
            : {
                usageStatus: ending === "completed" ? "complete" : "partial",
                usageScope: "main_agent",
                inputTokens: 34,
                cachedInputTokens: 6,
                cacheCreationTokens: 8,
                outputTokens: 14,
                reasoningTokens: 4,
                hasSubagents: false,
              },
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
        ),
        Effect.scoped,
      ),
  );

  it.effect("preserves native multi-select and custom-answer flags through a live reply", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      let delivered: unknown;
      const harness = yield* makeOpenCodeRuntimeHarness("question-flags", "root", {
        event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
        session: {
          create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
          promptAsync: async () => ({ data: true }),
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
        },
        question: {
          reply: async (input: unknown) => {
            delivered = input;
            return { data: true };
          },
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.filter(
          (event) =>
            event.type === "runtime_request.updated" ||
            (event.type === "turn_item.updated" && event.turnItem.type === "user_input_request"),
        ),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "question.asked",
          properties: {
            id: "question",
            sessionID: "root",
            questions: [
              {
                header: "Drinks",
                question: "Which drinks?",
                multiple: true,
                custom: false,
                options: [
                  { label: "Tea", description: "Hot" },
                  { label: "Water", description: "Cold" },
                ],
              },
            ],
          },
        }),
      );
      const events = yield* Fiber.join(received);
      const item = events.find((event) => event.type === "turn_item.updated");
      const request = events.find((event) => event.type === "runtime_request.updated");
      assert.equal(item?.type, "turn_item.updated");
      if (
        item?.type !== "turn_item.updated" ||
        item.turnItem.type !== "user_input_request" ||
        request?.type !== "runtime_request.updated"
      )
        throw new Error("Native question missing");
      assert.equal(item.turnItem.questions[0]?.multiSelect, true);
      assert.equal(item.turnItem.questions[0]?.allowCustomAnswer, false);
      yield* harness.runtime.respondToRuntimeRequest({
        requestId: request.runtimeRequest.id,
        answers: { "question-0-drinks": ["Tea", "Water"] },
      });
      assert.deepInclude(delivered, { answers: [["Tea", "Water"]] });
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect(
    "preserves tool lifecycle, approval kinds, and late assistant text without cached tool payloads",
    () =>
      Effect.gen(function* () {
        const nativeEvents = asyncEventStream();
        const nativeSessionId = "native-opencode-tool-lifecycle";
        const harness = yield* makeOpenCodeRuntimeHarness("tool-lifecycle", nativeSessionId, {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
              options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
              return { stream: nativeEvents.stream };
            },
          },
          session: {
            create: async () => ({
              data: { id: nativeSessionId, time: { created: 1, updated: 1 } },
            }),
            promptAsync: async () => ({ data: true }),
          },
        });
        yield* harness.startTurn();
        const received = yield* harness.runtime.events.pipe(
          Stream.takeUntil(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
          ),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const states = [
          { status: "pending", input: { command: "pwd" }, raw: "" },
          {
            status: "running",
            input: { command: "pwd" },
            title: "Working directory",
            time: { start: 1 },
          },
          {
            status: "completed",
            input: { command: "pwd" },
            output: "/repo\n",
            title: "Working directory",
            metadata: {},
            time: { start: 1, end: 2 },
          },
          {
            status: "error",
            input: { command: "pwd" },
            error: "Command failed",
            time: { start: 3, end: 4 },
          },
        ] satisfies ReadonlyArray<ToolPart["state"]>;
        for (const state of states) {
          yield* Effect.promise(() =>
            nativeEvents.push({
              type: "message.part.updated",
              properties: {
                sessionID: nativeSessionId,
                part: {
                  id: state.status === "error" ? "part-failed" : "part-working",
                  sessionID: nativeSessionId,
                  messageID: "late-assistant",
                  type: "tool",
                  callID: state.status === "error" ? "call-failed" : "call-working",
                  tool: "bash",
                  state,
                },
              },
            }),
          );
        }
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.part.updated",
            properties: {
              sessionID: nativeSessionId,
              part: {
                id: "part-read",
                sessionID: nativeSessionId,
                messageID: "late-assistant",
                type: "tool",
                callID: "call-read",
                tool: "read",
                state: { status: "pending", input: { filePath: "/outside/file" }, raw: "" },
              },
            },
          }),
        );
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "permission.asked",
            properties: {
              id: "read-permission",
              sessionID: nativeSessionId,
              permission: "external_directory",
              patterns: ["/outside/*"],
              always: [],
              metadata: {},
              tool: { messageID: "late-assistant", callID: "call-read" },
            },
          }),
        );
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.part.updated",
            properties: {
              sessionID: nativeSessionId,
              part: {
                id: "part-text",
                sessionID: nativeSessionId,
                messageID: "late-assistant",
                type: "text",
                text: "Tool results received",
                time: { start: 4 },
              },
            },
          }),
        );
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.updated",
            properties: {
              sessionID: nativeSessionId,
              info: {
                id: "late-assistant",
                sessionID: nativeSessionId,
                role: "assistant",
                time: { created: 1, completed: 4 },
              },
            },
          }),
        );
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "session.compacted",
            properties: { sessionID: nativeSessionId },
          }),
        );
        const events = yield* Fiber.join(received);
        const items = events.flatMap((event) =>
          event.type === "turn_item.updated" ? [event.turnItem] : [],
        );
        const commands = items.filter((item) => item.type === "command_execution");
        assert.deepEqual(
          commands.map((item) => item.status),
          ["pending", "running", "completed", "failed"],
        );
        assert.deepEqual(
          commands.map((item) => item.input),
          ["pwd", "pwd", "pwd", "pwd"],
        );
        assert.equal(commands[2]?.output, "/repo\n");
        assert.equal(commands[3]?.output, "Command failed");
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.kind === "file-read",
          ),
        );
        const assistant = items.filter((item) => item.type === "assistant_message");
        assert.equal(assistant.at(-1)?.text, "Tool results received");
        assert.equal(assistant.at(-1)?.status, "completed");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
        ),
        Effect.scoped,
      ),
  );

  // OpenCode 1.18.32 background tasks complete at launch; children stay busy after root settlement.
  it.effect("reports a background task child as pending work after the root turn settles", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const push = (event: unknown) => Effect.promise(() => nativeEvents.push(event));
      const root = "ses_root";
      const child = "ses_child";
      let promptId = "";
      const harness = yield* makeOpenCodeRuntimeHarness("background-child", root, {
        event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
        session: {
          create: async () => ({ data: { id: root, time: { created: 1, updated: 1 } } }),
          get: async () => ({
            data: { id: child, parentID: root, permission: [], time: { created: 2, updated: 2 } },
          }),
          update: async () => ({ data: { id: child, parentID: root } }),
          promptAsync: async (input: { messageID: string }) => {
            promptId = input.messageID;
            return { data: true };
          },
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
        },
      });
      const hasPendingBackgroundWork = harness.runtime.hasPendingBackgroundWork;
      if (hasPendingBackgroundWork === undefined) {
        return yield* Effect.die("OpenCode runtime must expose hasPendingBackgroundWork.");
      }
      const purpose = {
        version: 1,
        scope: {
          type: "single",
          reference: { host: "github.com", repository: "example/repo", kind: "issue", number: 11 },
        },
        provenance: { type: "declared" },
      } as const;
      const captured: ProviderAdapter.ProviderAdapterV2Event[] = [];
      const childInputSeen = yield* Deferred.make<void>();
      yield* harness.runtime.events.pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            captured.push(event);
            if (
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeInputs?.some(
                (input) => input.nativeId === "msg_child_initial",
              )
            )
              yield* Deferred.succeed(childInputSeen, undefined);
          }),
        ),
        Effect.forkScoped,
      );
      yield* harness.startTurn("hello", harness.providerThread, { purpose });
      const terminal = yield* harness.runtime.events.pipe(
        Stream.filter((event) => event.type === "turn.terminal"),
        Stream.runHead,
        Effect.forkScoped,
      );
      const taskPart = (status: "running" | "completed") => ({
        type: "message.part.updated",
        properties: {
          sessionID: root,
          part: {
            id: "prt_task",
            sessionID: root,
            messageID: "msg_root_assistant",
            type: "tool",
            tool: "task",
            callID: "call_task",
            state: {
              status,
              input: {
                description: "Background sleep task",
                prompt: "sleep",
                subagent_type: "general",
              },
              title: "Background sleep task",
              metadata: { parentSessionId: root, sessionId: child, background: true },
              time: { start: 3, ...(status === "completed" ? { end: 3 } : {}) },
              ...(status === "completed" ? { output: "Background task started" } : {}),
            },
          },
        },
      });
      const status = (sessionID: string, type: "busy" | "idle") => ({
        type: "session.status",
        properties: { sessionID, status: { type } },
      });

      yield* push({
        type: "message.updated",
        properties: {
          sessionID: root,
          info: { id: promptId, sessionID: root, role: "user", time: { created: 1 } },
        },
      });
      yield* push(status(root, "busy"));
      yield* push(taskPart("running"));
      yield* push({
        type: "session.created",
        properties: {
          sessionID: child,
          info: { id: child, parentID: root, time: { created: 2, updated: 2 } },
        },
      });
      yield* push({
        type: "message.updated",
        properties: {
          sessionID: child,
          info: { id: "msg_child_initial", sessionID: child, role: "user", time: { created: 2 } },
        },
      });
      yield* Deferred.await(childInputSeen);
      yield* push(status(child, "busy"));
      yield* push(taskPart("completed"));
      yield* push(status(root, "idle"));
      assert.equal(Option.getOrUndefined(yield* Fiber.join(terminal))?.status, "completed");
      assert.isTrue(yield* hasPendingBackgroundWork, "the running child must pin idle release");

      yield* push(status(child, "idle"));
      yield* push({ type: "session.idle", properties: { sessionID: child } });
      assert.isFalse(yield* hasPendingBackgroundWork, "an idle child must not pin idle release");
      const nativeChild = captured.findLast((event) => event.type === "subagent.updated");
      assert.equal(nativeChild?.type, "subagent.updated");
      if (nativeChild?.type !== "subagent.updated") return;
      assert.deepEqual(nativeChild.subagent.nativeSegments?.[0]?.source, {
        type: "app_run",
        threadId: harness.threadId,
        runId: harness.runId,
      });
      assert.deepEqual(nativeChild.subagent.nativeSegments?.[0]?.purpose, purpose);
      assert.deepEqual(nativeChild.subagent.nativeSegments?.[0]?.boundary, {
        state: "native_input",
        nativeInputId: "msg_child_initial",
      });
      assert.equal(nativeChild.subagent.nativeSegments?.[0]?.usageFinality, "unreconciled");
      assert.isTrue(
        captured.some(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.nativeInputs?.some(
              (input) =>
                input.nativeId === promptId && input.validation === "opencode_admitted_message",
            ),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect.each([
    ["native-tail", "active-turn", "interrupt"],
    ["timeout", "active-turn", "interrupt"],
    ["native-tail", "attached-busy", "interrupt"],
    ["stale-idle-tail", "attached-busy", "interrupt"],
    ["timeout", "attached-busy", "interrupt"],
    ["native-tail", "related-busy", "interrupt"],
    ["abort-failure", "attached-busy", "cut-in-retry"],
    ["enumeration-failure", "attached-busy", "cut-in-retry"],
    ["enumeration-failure", "unobserved-child", "cut-in-retry"],
    ["sweep-timeout", "attached-busy", "cut-in-retry"],
    ["native-tail", "settled-root", "stop"],
    ["abort-failure", "attached-busy", "stop"],
    ["abort-failure", "attached-busy", "stop-tail"],
    ["enumeration-failure", "attached-busy", "stop"],
    ["sweep-timeout", "attached-busy", "stop"],
    ["abort-failure", "attached-busy", "interrupt"],
    ["abort-failure", "attached-busy", "cut-in-end"],
    ["enumeration-failure", "attached-busy", "cut-in-end"],
    ["sweep-timeout", "attached-busy", "cut-in-end"],
    ["abort-failure", "settled-root", "cut-in-retry"],
    ["abort-failure", "settled-root", "cut-in-end"],
    ["abort-failure", "attached-busy", "cut-in-stop"],
    ["enumeration-failure", "attached-busy", "cut-in-stop"],
  ] as const)(
    "keeps an interrupted root behind its aborted descendant (%s, %s, %s)",
    ([ending, admission, action]) =>
      Effect.gen(function* () {
        const nativeEvents = asyncEventStream();
        const push = (event: unknown) => Effect.promise(() => nativeEvents.push(event));
        let childAbortCalled = promiseGate<void>();
        const cleanupFailed = promiseGate<void>();
        const releaseChild = promiseGate<void>();
        const returned = yield* Deferred.make<void>();
        const rootRunning = yield* Deferred.make<ProviderTurnId>();
        const childRunning = yield* Deferred.make<void>();
        const rootTerminal = yield* Deferred.make<void>();
        const childTerminal = yield* Deferred.make<void>();
        const drained = yield* Deferred.make<void>();
        const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const root = "ses_interrupt_root";
        const child = "ses_interrupt_child";
        let promptId = "";
        let finished = false;
        let childStarted = false;
        let childOutputSent = false;
        let draining = false;
        let enumerations = 0;
        let rootAborts = 0;
        let childAborts = 0;
        let childAbortSignal: AbortSignal | undefined;
        const cleanupFails =
          ending === "abort-failure" ||
          ending === "enumeration-failure" ||
          ending === "sweep-timeout";
        const cutIn = action.startsWith("cut-in");
        const failureAttempts = action === "cut-in-end" ? 5 : 1;
        const childStatus = cleanupFails && action !== "cut-in-retry" ? "completed" : "interrupted";
        const status = (sessionID: string, type: "busy" | "idle") => ({
          type: "session.status",
          properties: { sessionID, status: { type } },
        });
        const childTool = (toolStatus: "running" | "completed") => ({
          type: "message.part.updated",
          properties: {
            sessionID: child,
            part: {
              id: "prt_child_command",
              sessionID: child,
              messageID: "msg_child_assistant",
              type: "tool",
              tool: "bash",
              callID: "call_child_command",
              state: {
                status: toolStatus,
                input: { command: "sleep 20" },
                title: "Held child command",
                time: { start: 3, ...(toolStatus === "completed" ? { end: 4 } : {}) },
                ...(toolStatus === "completed" ? { output: "CHILD_PARTIAL" } : {}),
              },
            },
          },
        });
        const task = (attached: boolean) => ({
          type: "message.part.updated",
          properties: {
            sessionID: root,
            part: {
              id: "prt_task",
              sessionID: root,
              messageID: "msg_root_assistant",
              type: "tool",
              tool: "task",
              callID: "call_task",
              state: {
                status: "running",
                input: { description: "Held child", prompt: "sleep", subagent_type: "general" },
                title: "Held child",
                metadata: attached ? { parentSessionId: root, sessionId: child } : {},
                time: { start: 2 },
              },
            },
          },
        });
        const admitChild = async () => {
          if (childStarted) return;
          await nativeEvents.push({
            type: "message.updated",
            properties: {
              sessionID: child,
              info: { id: "msg_child_user", sessionID: child, role: "user", time: { created: 2 } },
            },
          });
          await nativeEvents.push({
            type: "message.updated",
            properties: {
              sessionID: child,
              info: {
                id: "msg_child_assistant",
                parentID: "msg_child_user",
                sessionID: child,
                role: "assistant",
                time: { created: 3 },
              },
            },
          });
          await nativeEvents.push(childTool("running"));
          childStarted = true;
        };
        const completeChildTool = async () => {
          if (childOutputSent) return;
          await nativeEvents.push(childTool("completed"));
          childOutputSent = true;
        };
        const harness = yield* makeOpenCodeRuntimeHarness(
          `${ending}-${admission}-${action}`,
          root,
          {
            event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
            session: {
              create: async () => ({ data: { id: root, time: { created: 1, updated: 1 } } }),
              get: async () => ({
                data: {
                  id: child,
                  parentID: root,
                  permission: [],
                  time: { created: 2, updated: 2 },
                },
              }),
              update: async () => ({ data: { id: child, parentID: root } }),
              messages: async () => ({ data: [] }),
              children: async ({ sessionID }: { sessionID: string }) => {
                if (sessionID !== root || finished) return { data: [] };
                enumerations += 1;
                if (
                  ending === "enumeration-failure" &&
                  (enumerations <= failureAttempts || action === "cut-in-stop")
                ) {
                  cleanupFailed.resolve();
                  throw new Error("cannot enumerate descendants");
                }
                // Cleanup retry must still reach a previously attempted session
                // even if the next listing no longer returns it.
                if (ending === "abort-failure" && enumerations > 1) return { data: [] };
                if (admission === "related-busy" || admission === "unobserved-child") {
                  await nativeEvents.push(task(true));
                }
                return { data: [{ id: child }] };
              },
              promptAsync: async (input: { messageID: string }) => {
                promptId = input.messageID;
                return { data: true };
              },
              abort: async (
                { sessionID }: { sessionID: string },
                options: { signal: AbortSignal },
              ) => {
                if (finished) return { data: true };
                if (sessionID === root) {
                  rootAborts += 1;
                  await nativeEvents.push(status(root, "idle"));
                } else {
                  childAborts += 1;
                  childAbortSignal = options.signal;
                  childAbortCalled.resolve();
                  if (
                    ending === "abort-failure" &&
                    (childAborts <= failureAttempts || action === "cut-in-stop")
                  ) {
                    cleanupFailed.resolve();
                    throw { status: 500 };
                  }
                  if (ending === "sweep-timeout" && childAborts <= failureAttempts)
                    return new Promise(() => {});
                  if (ending !== "timeout" && ending !== "stale-idle-tail") {
                    await releaseChild.promise;
                    if (!finished) {
                      await admitChild();
                      await completeChildTool();
                      await nativeEvents.push(status(child, "idle"));
                    }
                  }
                }
                return { data: true };
              },
            },
          },
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            finished = true;
            releaseChild.resolve();
            nativeEvents.close();
          }),
        );
        yield* harness.runtime.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              events.push(event);
              if (
                draining &&
                event.type === "provider_thread.updated" &&
                event.providerThread.id === harness.providerThread.id
              )
                yield* Deferred.succeed(drained, undefined);
              if (event.type === "turn.terminal") yield* Deferred.succeed(rootTerminal, undefined);
              if (
                event.type === "provider_turn.updated" &&
                event.providerTurn.runAttemptId !== null &&
                event.providerTurn.status === "running"
              )
                yield* Deferred.succeed(rootRunning, event.providerTurn.id);
              if (
                event.type === "provider_turn.updated" &&
                event.providerTurn.runAttemptId === null &&
                event.providerTurn.status === "running"
              )
                yield* Deferred.succeed(childRunning, undefined);
              if (
                event.type === "provider_turn.updated" &&
                event.providerTurn.runAttemptId === null &&
                event.providerTurn.status !== "running"
              )
                yield* Deferred.succeed(childTerminal, undefined);
            }),
          ),
          Effect.forkScoped,
        );
        yield* harness.startTurn();
        yield* push({
          type: "message.updated",
          properties: {
            sessionID: root,
            info: { id: promptId, sessionID: root, role: "user", time: { created: 1 } },
          },
        });
        yield* push(status(root, "busy"));
        if (admission !== "unobserved-child") yield* push(task(admission !== "related-busy"));
        if (admission === "related-busy")
          yield* push({
            type: "session.created",
            properties: { info: { id: child, parentID: root } },
          });
        if (admission !== "unobserved-child") {
          yield* push(status(child, ending === "stale-idle-tail" ? "idle" : "busy"));
        }
        if (admission === "active-turn") {
          yield* Effect.promise(admitChild);
          yield* Deferred.await(childRunning);
        }
        if (admission === "settled-root") {
          yield* push(status(root, "idle"));
          yield* Deferred.await(rootTerminal);
        }
        const providerTurnId = yield* Deferred.await(rootRunning);
        const interruptInput = {
          providerThread: harness.providerThread,
          providerTurnId,
          ...(action !== "interrupt" ? { requestRuntimeRestart: true } : {}),
          ...(cutIn ? { replacementFollows: true as const } : {}),
        };
        const rootIsHeld = () =>
          Effect.gen(function* () {
            const held = yield* harness.runtime.readThreadSnapshot({
              providerThread: harness.providerThread,
            });
            assert.equal(
              held.providerTurns.at(-1)?.status,
              admission === "settled-root" ? "completed" : "running",
              "cleanup failure must not end the captured root",
            );
            if (admission !== "settled-root") assert.isFalse(yield* Deferred.isDone(rootTerminal));
            assert.isTrue(yield* harness.runtime.hasPendingBackgroundWork!);
            assert.isTrue(
              Exit.isFailure(yield* Effect.exit(harness.startTurn("must wait for cleanup"))),
              "an interruption blocks start even when its root already completed",
            );
          });
        const failedCleanup = (replacementFollows: true | undefined) =>
          Effect.gen(function* () {
            childAbortCalled = promiseGate<void>();
            const failedReturned = yield* Deferred.make<void>();
            const failedStop = yield* harness.runtime
              .interruptTurn({
                providerThread: harness.providerThread,
                providerTurnId,
                ...(action !== "interrupt" ? { requestRuntimeRestart: true } : {}),
                ...(replacementFollows ? { replacementFollows } : {}),
              })
              .pipe(
                Effect.result,
                Effect.tap(() => Deferred.succeed(failedReturned, undefined)),
                Effect.forkScoped,
              );
            if (ending === "sweep-timeout") {
              yield* Effect.promise(() => childAbortCalled.promise);
              yield* TestClock.adjust("15 seconds");
            } else yield* Effect.promise(() => cleanupFailed.promise);
            if (replacementFollows === undefined) {
              yield* TestClock.adjust("9999 millis");
              assert.isFalse(
                yield* Deferred.isDone(failedReturned),
                "ordinary Stop waits for the bounded native tail before reporting cleanup failure",
              );
              yield* rootIsHeld();
              if (action === "stop-tail") {
                yield* Effect.promise(admitChild);
                yield* Deferred.await(childRunning);
                yield* Effect.promise(completeChildTool);
                yield* push(status(child, "idle"));
                yield* Deferred.await(childTerminal);
              } else yield* TestClock.adjust("1 millis");
            }
            const failure = yield* Fiber.join(failedStop);
            assert.equal(failure._tag, "Failure");
            if (failure._tag === "Failure")
              assert.equal(failure.failure._tag, "ProviderAdapterInterruptError");
            if (ending === "sweep-timeout") assert.isTrue(childAbortSignal?.aborted);
          });
        if (cleanupFails) {
          for (let attempt = 0; attempt < failureAttempts; attempt += 1) {
            yield* failedCleanup(cutIn ? true : undefined);
            if (cutIn) yield* rootIsHeld();
          }
          if (admission === "unobserved-child") {
            yield* push(task(true));
            yield* push(status(child, "busy"));
          }
          yield* Effect.promise(admitChild);
          yield* Deferred.await(childRunning);
          yield* Effect.promise(completeChildTool);
          yield* push(status(root, "idle"));
          if (cutIn) yield* rootIsHeld();
          if (action === "cut-in-stop") yield* failedCleanup(undefined);
          if (action !== "cut-in-retry") {
            if (action !== "cut-in-end") {
              const stopped = yield* harness.runtime.readThreadSnapshot({
                providerThread: harness.providerThread,
              });
              assert.equal(
                stopped.providerTurns.at(-1)?.status,
                "interrupted",
                "Stop releases the root on cleanup failure",
              );
              if (action !== "stop-tail") {
                assert.isTrue(
                  yield* harness.runtime.hasPendingBackgroundWork!,
                  "unconfirmed busy work stays pending after Stop",
                );
                assert.isFalse(
                  yield* Deferred.isDone(childTerminal),
                  "Stop does not invent an end for an unreached child",
                );
              }
            }
            yield* push(status(child, "idle"));
            yield* Deferred.await(childTerminal);
            if (
              action === "cut-in-end" &&
              (ending === "enumeration-failure" || ending === "sweep-timeout")
            ) {
              // A known child's end cannot confirm an unread part of the native
              // session tree. A later Stop retries discovery before release.
              yield* rootIsHeld();
              yield* Effect.sync(() => releaseChild.resolve());
              yield* harness.runtime.interruptTurn({
                providerThread: harness.providerThread,
                providerTurnId,
                requestRuntimeRestart: true,
              });
            }
            const released = yield* harness.runtime.readThreadSnapshot({
              providerThread: harness.providerThread,
            });
            assert.equal(
              released.providerTurns.at(-1)?.status,
              admission === "settled-root" ? "completed" : "interrupted",
              "the native child end releases the retained root without another interrupt call",
            );
            assert.isFalse(
              yield* harness.runtime.hasPendingBackgroundWork!,
              "native completion releases the retry hold after the final failure",
            );
          }
        }
        if (!cleanupFails || action === "cut-in-retry") {
          const interrupt = yield* harness.runtime.interruptTurn(interruptInput).pipe(
            Effect.tap(() => Deferred.succeed(returned, undefined)),
            Effect.forkScoped,
          );
          yield* Effect.promise(() => childAbortCalled.promise);
          if (admission !== "settled-root") {
            const held = yield* harness.runtime.readThreadSnapshot({
              providerThread: harness.providerThread,
            });
            assert.equal(
              held.providerTurns.at(-1)?.status,
              "running",
              "root idle must wait for the aborted native id",
            );
          }
          if (ending === "stale-idle-tail") {
            // The HTTP abort acknowledges before SSE delivers this child's
            // current turn. Its earlier idle is not an acknowledgement for it.
            yield* TestClock.adjust("9999 millis");
            assert.isFalse(yield* Deferred.isDone(returned), "old idle cannot release a new abort");
            yield* Effect.promise(admitChild);
            yield* Effect.promise(completeChildTool);
            yield* push(status(child, "idle"));
          } else if (ending !== "timeout") yield* Effect.sync(() => releaseChild.resolve());
          else {
            yield* TestClock.adjust("9999 millis");
            assert.isFalse(yield* Deferred.isDone(returned), "the child wait has not elapsed");
            yield* TestClock.adjust("1 millis");
          }
          yield* Fiber.join(interrupt);
        }
        if (ending === "timeout" && admission === "attached-busy") {
          // There was no child turn to settle locally, but the actual aborted id
          // must still hold the root until the bound.
          assert.isFalse(yield* Deferred.isDone(childRunning));
        } else yield* Deferred.await(childTerminal);
        yield* Deferred.await(rootTerminal);
        yield* push({ type: "session.idle", properties: { sessionID: root } });
        draining = true;
        yield* push(status(root, "busy"));
        yield* Deferred.await(drained);
        const terminalIndex = events.findIndex((event) => event.type === "turn.terminal");
        const childTurns = events.filter(
          (event) =>
            event.type === "provider_turn.updated" &&
            event.providerTurn.runAttemptId === null &&
            event.providerTurn.status !== "running",
        );
        assert.equal(
          rootAborts,
          admission === "settled-root" ? 0 : 1,
          "a retained retry must not re-abort the root",
        );
        assert.equal(
          childAborts,
          ending === "enumeration-failure"
            ? action === "cut-in-retry" || action === "cut-in-end"
              ? 1
              : 0
            : cleanupFails
              ? action === "cut-in-end"
                ? failureAttempts + (ending === "sweep-timeout" ? 1 : 0)
                : action === "cut-in-retry" || action === "cut-in-stop"
                  ? 2
                  : 1
              : 1,
        );
        assert.lengthOf(childTurns, ending === "timeout" && admission === "attached-busy" ? 0 : 1);
        if (childTurns.length > 0) {
          const childEndIndex = events.findIndex((event) => event === childTurns[0]);
          if (
            admission !== "settled-root" &&
            (!cleanupFails ||
              action === "cut-in-retry" ||
              action === "cut-in-end" ||
              action === "stop-tail")
          )
            assert.isBelow(childEndIndex, terminalIndex);
          assert.equal(
            childTurns[0]?.type === "provider_turn.updated" && childTurns[0].providerTurn.status,
            childStatus,
          );
        }
        if (action === "cut-in-retry" || action === "cut-in-end") {
          const taskEnds = events.filter(
            (event) =>
              event.type === "subagent.updated" &&
              event.subagent.nativeTaskRef?.nativeId === "prt_task" &&
              event.subagent.status === childStatus,
          );
          assert.lengthOf(taskEnds, 1, "a stopped cohort must release its original task row once");
          if (admission !== "settled-root")
            assert.isBelow(events.indexOf(taskEnds[0]!), terminalIndex);
        }
        if (ending !== "timeout") {
          const toolEndIndex = events.findIndex(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.output === "CHILD_PARTIAL" &&
              event.turnItem.status === "completed",
          );
          assert.isAtLeast(toolEndIndex, 0);
          const childNodeEndIndex = events.findIndex(
            (event) =>
              event.type === "node.updated" &&
              event.node.kind === "root_turn" &&
              event.node.status === childStatus,
          );
          assert.isBelow(toolEndIndex, childNodeEndIndex);
          if (
            admission !== "settled-root" &&
            (!cleanupFails ||
              action === "cut-in-retry" ||
              action === "cut-in-end" ||
              action === "stop-tail")
          )
            assert.isBelow(childNodeEndIndex, terminalIndex);
          assert.lengthOf(
            events.filter(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "command_execution" &&
                event.turnItem.output === "CHILD_PARTIAL",
            ),
            1,
          );
        }
        assert.lengthOf(
          events.filter((event) => event.type === "turn.terminal"),
          1,
        );
        if (cleanupFails) yield* harness.startTurn("cleanup succeeded; accept the next turn");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
        ),
        Effect.scoped,
      ),
  );

  it.effect("projects OpenCode read output and search titles from native tool calls", () =>
    Effect.gen(function* () {
      const directoryOutput =
        "<path>/workspace/src</path>\n<type>directory</type>\n<entries>\nenv.ts\nlib/\n\n(2 entries)\n</entries>";
      const fileOutput =
        "<path>/workspace/src/env.ts</path>\n<type>file</type>\n<content>\n1: hello\n2: </content> literal\n3: world\n</content>";
      const fileWithInstructions =
        "<path>/workspace/src/env.ts</path>\n<type>file</type>\n<content>1: hello\n\n(End of file - total 1 lines)\n</content>\n\n<system-reminder>\nProject instructions\n</system-reminder>";
      const missingPath = directoryOutput.slice(directoryOutput.indexOf("<type>"));
      const missingClose = fileOutput.slice(0, fileOutput.lastIndexOf("\n</content>"));
      const nativeEvents = asyncEventStream();
      const nativeSessionId = "native-opencode-search";
      const harness = yield* makeOpenCodeRuntimeHarness("search-projection", nativeSessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        mcp: { status: async () => ({ data: {} }) },
        session: {
          create: async () => ({
            data: { id: nativeSessionId, time: { created: 1, updated: 1 } },
          }),
          promptAsync: async () => ({ data: true }),
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.takeUntil(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
        ),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const toolCalls = [
        ["read", { filePath: "src/env.ts" }, "---\nfile body"],
        ["read", { filePath: "/workspace/src" }, directoryOutput],
        ["read", { filePath: "/workspace/src/env.ts" }, fileOutput],
        ["read", { filePath: "/workspace/src/env.ts" }, fileWithInstructions],
        ["read", { filePath: "/workspace/src" }, missingPath],
        ["read", { filePath: "/workspace/src/env.ts" }, missingClose],
        ["custom_read", { filePath: "/workspace/src" }, directoryOutput],
        ["grep", { pattern: "TODO", path: "apps/web" }, "---\nfile body"],
        ["websearch", { query: "OpenCode documentation" }, "---\nfile body"],
        ["glob", { pattern: "missing", path: "apps/web" }, ""],
        ["codesearch", {}, " \n\t"],
      ] as const;
      for (const [index, [tool, input, output]] of toolCalls.entries()) {
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.part.updated",
            properties: {
              sessionID: nativeSessionId,
              part: {
                id: `part-${index}`,
                sessionID: nativeSessionId,
                messageID: "assistant-search",
                type: "tool",
                callID: `call-${index}`,
                tool,
                state: {
                  status: "completed",
                  input,
                  output,
                  title: tool,
                  metadata: {},
                  time: { start: 1, end: 2 },
                },
              },
            },
          }),
        );
      }
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.compacted",
          properties: { sessionID: nativeSessionId },
        }),
      );
      const items = (yield* Fiber.join(received)).flatMap((event) =>
        event.type === "turn_item.updated" ? [event.turnItem] : [],
      );
      const reads = items.filter((item) => item.type === "dynamic_tool");
      assert.equal(reads[0]?.title, "Read src/env.ts");
      assert.deepEqual(
        reads.map((item) => ({ toolName: item.toolName, output: item.output })),
        [
          { toolName: "read", output: "---\nfile body" },
          { toolName: "read", output: "env.ts\nlib/\n\n(2 entries)" },
          { toolName: "read", output: "1: hello\n2: </content> literal\n3: world" },
          {
            toolName: "read",
            output:
              "1: hello\n\n(End of file - total 1 lines)\n\n<system-reminder>\nProject instructions\n</system-reminder>",
          },
          { toolName: "read", output: missingPath },
          { toolName: "read", output: missingClose },
          { toolName: "custom_read", output: directoryOutput },
        ],
      );
      const grep = items.find((item) => item.type === "file_search");
      assert.equal(grep?.title, "Searched TODO in web");
      assert.equal(grep?.type === "file_search" ? grep.pattern : null, "TODO");
      assert.deepEqual(grep?.type === "file_search" ? grep.results : null, [
        { fileName: "apps/web", preview: "---\nfile body" },
      ]);
      const webSearch = items.find((item) => item.type === "web_search");
      assert.deepEqual(webSearch?.type === "web_search" ? webSearch.patterns : null, [
        "OpenCode documentation",
      ]);
      assert.deepEqual(webSearch?.type === "web_search" ? webSearch.results : null, [
        { snippet: "---\nfile body" },
      ]);
      const emptyFileSearch = items.find(
        (item) => item.type === "file_search" && item.pattern === "missing",
      );
      assert.ok(emptyFileSearch?.type === "file_search");
      assert.equal(emptyFileSearch.results, undefined);
      const emptyWebSearch = items.find(
        (item) => item.type === "web_search" && item.patterns === undefined,
      );
      assert.ok(emptyWebSearch?.type === "web_search");
      assert.equal(emptyWebSearch.results, undefined);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("drops cancelled OpenCode todos so a finished list completes", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const nativeSessionId = "native-opencode-todos";
      const harness = yield* makeOpenCodeRuntimeHarness("todos", nativeSessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        mcp: { status: async () => ({ data: {} }) },
        session: {
          create: async () => ({
            data: { id: nativeSessionId, time: { created: 1, updated: 1 } },
          }),
          promptAsync: async () => ({ data: true }),
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "plan.updated"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "todo.updated",
          properties: {
            sessionID: nativeSessionId,
            todos: [
              { content: "Write the fix", status: "completed", priority: "high" },
              { content: "Try another approach", status: "cancelled", priority: "low" },
              { content: "Add a test", status: "completed", priority: "medium" },
            ],
          },
        }),
      );
      const plan = (yield* Fiber.join(received)).flatMap((event) =>
        event.type === "plan.updated" ? [event.plan] : [],
      )[0];
      assert.ok(plan?.kind === "todo_list");
      assert.deepEqual(
        plan.steps.map((step) => ({ text: step.text, status: step.status })),
        [
          { text: "Write the fix", status: "completed" },
          { text: "Add a test", status: "completed" },
        ],
      );
      assert.equal(plan.status, "completed");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("presents OpenCode MCP calls without treating remote tools as local edits", () =>
    Effect.gen(function* () {
      const output =
        "<path>/workspace/src</path>\n<type>directory</type>\n<entries>\nenv.ts\nlib/\n\n(2 entries)\n</entries>";
      const nativeEvents = asyncEventStream();
      const nativeSessionId = "native-opencode-mcp";
      let statusReads = 0;
      const harness = yield* makeOpenCodeRuntimeHarness("mcp-presentation", nativeSessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        session: {
          create: async () => ({ data: { id: nativeSessionId, time: { created: 1, updated: 1 } } }),
          promptAsync: async () => ({ data: true }),
        },
        mcp: {
          status: async () => {
            statusReads++;
            return {
              data: {
                "my.server_with_underscores": { status: "connected" },
                ambiguous: { status: "connected" },
                ambiguous_server: { status: "connected" },
                code: { status: "connected" },
                apply: { status: "connected" },
              },
            };
          },
        },
      });
      yield* harness.startTurn();
      const received = yield* harness.runtime.events.pipe(
        Stream.takeUntil(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
        ),
        Stream.runCollect,
        Effect.forkScoped,
      );
      for (const status of ["running", "completed", "error"] as const) {
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.part.updated",
            properties: {
              sessionID: nativeSessionId,
              part: {
                id: "mcp-part",
                sessionID: nativeSessionId,
                messageID: "mcp-assistant",
                type: "tool",
                callID: "mcp-call",
                tool: "my_server_with_underscores_edit_document",
                state: {
                  status,
                  input: { document: "remote-doc" },
                  title: "Edit remote document",
                  metadata: {},
                  output,
                  error: "failed",
                  time: { start: 1, end: 2 },
                },
              },
            },
          }),
        );
      }
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.part.updated",
          properties: {
            sessionID: nativeSessionId,
            part: {
              id: "ambiguous-part",
              sessionID: nativeSessionId,
              messageID: "mcp-assistant",
              type: "tool",
              callID: "ambiguous-call",
              tool: "ambiguous_server_edit_document",
              state: {
                status: "completed",
                input: {},
                title: "Ambiguous tool",
                metadata: {},
                output: "",
                time: { start: 1, end: 2 },
              },
            },
          },
        }),
      );
      for (const tool of ["code_search", "apply_patch"]) {
        yield* Effect.promise(() =>
          nativeEvents.push({
            type: "message.part.updated",
            properties: {
              sessionID: nativeSessionId,
              part: {
                id: `native-${tool}`,
                sessionID: nativeSessionId,
                messageID: "mcp-assistant",
                type: "tool",
                callID: `native-${tool}`,
                tool,
                state: {
                  status: "completed",
                  input: { query: "needle", filePath: "local.ts" },
                  title: `Native ${tool}`,
                  metadata: {},
                  output: "done",
                  time: { start: 1, end: 2 },
                },
              },
            },
          }),
        );
      }
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.compacted",
          properties: { sessionID: nativeSessionId },
        }),
      );
      const allItems = (yield* Fiber.join(received)).flatMap((event) =>
        event.type === "turn_item.updated" ? [event.turnItem] : [],
      );
      assert.equal(
        allItems.find((item) => item.title === "Native code_search")?.type,
        "web_search",
      );
      assert.equal(
        allItems.find((item) => item.title === "Native apply_patch")?.type,
        "file_change",
      );
      const items = allItems.filter((item) => item.type === "dynamic_tool");
      assert.deepEqual(
        items.slice(0, 3).map((item) => item.status),
        ["running", "completed", "failed"],
      );
      assert.deepEqual(
        items.slice(0, 3).map((item) => item.title),
        ["Edit remote document", "Edit remote document", "edit document"],
      );
      for (const item of items.slice(0, 3)) {
        assert.deepEqual(item.toolSource, {
          key: "mcp:my.server_with_underscores",
          name: "my.server with underscores",
          kind: "integration",
        });
        assert.deepEqual(item.input, { document: "remote-doc" });
      }
      assert.equal(items[1]?.output, output);
      assert.equal(items[3]?.toolName, "ambiguous_server_edit_document");
      assert.isUndefined(items[3]?.toolSource);
      assert.equal(statusReads, 1);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("admits a native command on its user receipt before generation completes", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const release = promiseGate<void>();
      const calls = yield* Queue.unbounded<{
        messageID: string;
        command: string;
        arguments: string;
        model: string;
      }>();
      let promptCalls = 0;
      const sessionId = "native-command";
      const harness = yield* makeOpenCodeRuntimeHarness("native-command", sessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        command: { list: async () => ({ data: [{ name: "review" }] }) },
        session: {
          create: async () => ({ data: { id: sessionId, time: { created: 1, updated: 1 } } }),
          command: async (input: {
            messageID: string;
            command: string;
            arguments: string;
            model: string;
          }) => {
            Queue.offerUnsafe(calls, input);
            await release.promise;
            return { data: true };
          },
          promptAsync: async () => {
            promptCalls++;
            return { data: true };
          },
        },
      });
      const start = yield* harness.startTurn("/review staged changes").pipe(Effect.forkScoped);
      const call = yield* Queue.take(calls);
      assert.equal(call.command, "review");
      assert.equal(call.arguments, "staged changes");
      assert.equal(call.model, "anthropic/claude-sonnet");
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.updated",
          properties: {
            sessionID: sessionId,
            info: {
              id: call.messageID,
              sessionID: sessionId,
              role: "user",
              time: { created: DateTime.toEpochMillis(harness.now) },
            },
          },
        }),
      );
      yield* Fiber.join(start);
      assert.equal(promptCalls, 0);
      release.resolve();
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("compacts with the native summarize API and emits a completed compaction", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const summarizeCalls: Array<unknown> = [];
      let promptCalls = 0;
      const harness = yield* makeOpenCodeRuntimeHarness("compact", "native-opencode-compact", {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        session: {
          create: async () => ({
            data: { id: "native-opencode-compact", time: { created: 1, updated: 1 } },
          }),
          summarize: async (input: unknown) => {
            summarizeCalls.push(input);
            return { data: true };
          },
          promptAsync: async () => {
            promptCalls += 1;
            return { data: true };
          },
        },
      });
      yield* harness.startTurn("/compact");
      const events = yield* harness.runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
      );
      assert.deepEqual(summarizeCalls, [
        {
          sessionID: "native-opencode-compact",
          providerID: "anthropic",
          modelID: "claude-sonnet",
          auto: false,
        },
      ]);
      assert.equal(promptCalls, 0);
      assert.equal(
        events.filter(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "compaction" &&
            event.turnItem.status === "completed",
        ).length,
        1,
      );
      assert.isTrue(
        events.some((event) => event.type === "turn.terminal" && event.status === "completed"),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("ends a turn on the provider thread it started on", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const harness = yield* makeOpenCodeRuntimeHarness(
        "forked-row",
        "native-opencode-forked-row",
        {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
              options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
              return { stream: nativeEvents.stream };
            },
          },
          session: {
            create: async () => ({
              data: { id: "native-opencode-forked-row", time: { created: 1, updated: 1 } },
            }),
            summarize: async () => ({ data: true }),
          },
        },
      );
      const forkedRow = {
        ...harness.providerThread,
        id: ProviderThreadId.make("provider-thread:opencode-test:forked-run-row"),
      };
      yield* harness.startTurn("/compact", forkedRow);
      const terminal = yield* harness.runtime.events.pipe(
        Stream.filter(
          (event): event is Extract<typeof event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        ),
        Stream.runHead,
      );
      assert.equal(Option.getOrUndefined(terminal)?.providerThreadId, forkedRow.id);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("does not restore messages beyond OpenCode's persisted revert boundary", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const nativeSessionId = "native-opencode-reverted-snapshot";
      const messages = [
        {
          info: {
            id: "user-kept",
            sessionID: nativeSessionId,
            role: "user",
            time: { created: 1 },
          },
          parts: [{ type: "text", text: "keep this prompt" }],
        },
        {
          info: {
            id: "assistant-kept",
            sessionID: nativeSessionId,
            role: "assistant",
            time: { created: 2, completed: 3 },
          },
          parts: [{ type: "text", text: "keep this answer" }],
        },
        {
          info: {
            id: "user-reverted",
            sessionID: nativeSessionId,
            role: "user",
            time: { created: 4 },
          },
          parts: [{ type: "text", text: "remove this prompt" }],
        },
        {
          info: {
            id: "assistant-reverted",
            sessionID: nativeSessionId,
            role: "assistant",
            time: { created: 5, completed: 6 },
          },
          parts: [{ type: "text", text: "remove this answer" }],
        },
      ];
      const harness = yield* makeOpenCodeRuntimeHarness("reverted-snapshot", nativeSessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), {
              once: true,
            });
            return { stream: nativeEvents.stream };
          },
        },
        session: {
          create: async () => ({
            data: { id: nativeSessionId, time: { created: 1, updated: 1 } },
          }),
          get: async () => ({
            data: {
              id: nativeSessionId,
              time: { created: 1, updated: 6 },
              revert: { messageID: "user-reverted" },
            },
          }),
          messages: async () => ({ data: messages }),
        },
      });

      const snapshot = yield* harness.runtime.readThreadSnapshot({
        providerThread: harness.providerThread,
      });

      assert.deepEqual(
        snapshot.messages.map((message) => message.text),
        ["keep this prompt", "keep this answer"],
      );
      const providerPayload = snapshot.providerPayload as ReadonlyArray<{
        readonly info: { readonly id: string };
      }>;
      assert.deepEqual(
        providerPayload.map((entry) => entry.info.id),
        ["user-kept", "assistant-kept"],
      );
      assert.equal(snapshot.providerThread.nativeConversationHeadRef?.nativeId, "user-kept");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("keeps a newly admitted prompt alive across stale idle and delayed busy evidence", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const prompt = promiseGate<void>();
      const promptStarted = promiseGate<void>();
      const steerPrompt = promiseGate<void>();
      const steerPromptStarted = promiseGate<void>();
      const stopPromptStarted = promiseGate<void>();
      const statusCalled = promiseGate<void>();
      const steerStatusCalled = promiseGate<void>();
      const abortCalled = promiseGate<void>();
      let status: "idle" | "busy" = "busy";
      let promptCalls = 0;
      let statusCalls = 0;
      const promptInputs: Array<{ readonly messageID?: string }> = [];
      const client = {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        session: {
          create: async () => ({
            data: { id: "native-opencode-race", time: { created: 1, updated: 1 } },
          }),
          get: async () => ({
            data: { id: "native-opencode-race", time: { created: 1, updated: 1 } },
          }),
          promptAsync: async (
            input: { readonly messageID?: string },
            options?: { readonly signal?: AbortSignal },
          ) => {
            promptInputs.push(input);
            promptCalls += 1;
            if (promptCalls === 1) {
              promptStarted.resolve();
              await prompt.promise;
            } else if (promptCalls === 2) {
              steerPromptStarted.resolve();
              await steerPrompt.promise;
            } else {
              stopPromptStarted.resolve();
              await new Promise<void>((_resolve, reject) =>
                options?.signal?.addEventListener("abort", () => reject(options.signal!.reason), {
                  once: true,
                }),
              );
            }
            return { data: true };
          },
          status: async () => ({
            data: (() => {
              statusCalls += 1;
              if (statusCalls === 1) statusCalled.resolve();
              else steerStatusCalled.resolve();
              return { "native-opencode-race": { type: status } };
            })(),
          }),
          messages: async () => ({ data: [] }),
          children: async () => ({ data: [] }),
          abort: async () => {
            abortCalled.resolve();
            return { data: true };
          },
        },
        mcp: { add: async () => ({ data: true }) },
      };
      const adapter = yield* makeOpenCodeAdapterV2({
        instanceId: ProviderInstanceId.make("opencode-test"),
        settings: OPEN_CODE_TEST_SETTINGS,
        environment: {},
      }).pipe(
        Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, {
          connectToOpenCodeServer: () =>
            Effect.succeed({ url: "http://test.invalid", external: true }),
          createOpenCodeSdkClient: () => client,
        } as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"]),
        Effect.provideService(ProviderHost.ProviderHost, {
          paths: { cwd: "/workspace" },
          resolveAttachmentPath: () => null,
        } as unknown as ProviderHost.ProviderHost["Service"]),
      );
      const threadId = ThreadId.make("thread-opencode-admission-race");
      const providerSessionId = ProviderSessionId.make("session-opencode-admission-race");
      const modelSelection = {
        instanceId: ProviderInstanceId.make("opencode-test"),
        model: "anthropic/claude-sonnet",
        options: [],
      };
      const policy = runtimePolicy("full-access", { cwd: "/workspace" });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy: policy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: policy,
      });
      const now = yield* DateTime.now;
      const attemptId = RunAttemptId.make("attempt-opencode-admission-race");
      const runId = RunId.make("run-opencode-admission-race");
      const start = yield* runtime
        .startTurn({
          appThread: {
            id: threadId,
            projectId: ProjectId.make("project-opencode-admission-race"),
            title: "race",
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdBy: "user",
            creationSource: "web",
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId,
          rootNodeId: NodeId.make("node-opencode-admission-race"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("message-opencode-admission-race"),
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy: policy,
        })
        .pipe(Effect.forkScoped);

      yield* Effect.promise(() => promptStarted.promise);
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "idle" } },
        }),
      );
      prompt.resolve();
      yield* Fiber.join(start);
      assert.match(promptInputs[0]?.messageID ?? "", /^msg_[0-9a-f]{12}[0-9A-Za-z]{28}$/);
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "busy" } },
        }),
      );
      status = "busy";
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.updated",
          properties: {
            sessionID: "native-opencode-race",
            info: {
              id: "stale-user-message",
              sessionID: "native-opencode-race",
              role: "user",
              time: { created: DateTime.toEpochMillis(now) },
            },
          },
        }),
      );
      assert.equal(statusCalls, 0, "an unrelated user message must not release prompt admission");
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "idle" } },
        }),
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.updated",
          properties: {
            sessionID: "native-opencode-race",
            info: {
              id: promptInputs[0]!.messageID!,
              sessionID: "native-opencode-race",
              role: "user",
              time: { created: DateTime.toEpochMillis(now) },
            },
          },
        }),
      );

      yield* Effect.promise(() => statusCalled.promise);

      const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(snapshot.providerTurns.at(-1)?.status, "running");
      const activeTurn = snapshot.providerTurns.at(-1)!;
      const steer = yield* runtime
        .steerTurn({
          threadId,
          runId,
          providerThread,
          providerTurnId: activeTurn.id,
          message: {
            messageId: MessageId.make("message-opencode-steer-race"),
            text: "follow up",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
        })
        .pipe(Effect.forkScoped);
      yield* Effect.promise(() => steerPromptStarted.promise);
      assert.notEqual(promptInputs[1]?.messageID, promptInputs[0]?.messageID);
      steerPrompt.resolve();
      yield* Fiber.join(steer);
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "busy" } },
        }),
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.updated",
          properties: {
            sessionID: "native-opencode-race",
            info: {
              id: promptInputs[0]!.messageID!,
              sessionID: "native-opencode-race",
              role: "user",
              time: { created: DateTime.toEpochMillis(now) },
            },
          },
        }),
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "idle" } },
        }),
      );
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "message.updated",
          properties: {
            sessionID: "native-opencode-race",
            info: {
              id: promptInputs[1]!.messageID!,
              sessionID: "native-opencode-race",
              role: "user",
              time: { created: DateTime.toEpochMillis(now) },
            },
          },
        }),
      );
      yield* Effect.promise(() => steerStatusCalled.promise);
      const pendingSteer = yield* runtime
        .steerTurn({
          threadId,
          runId,
          providerThread,
          providerTurnId: activeTurn.id,
          message: {
            messageId: MessageId.make("message-opencode-stop-pending"),
            text: "pending when stopped",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
        })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* Effect.promise(() => stopPromptStarted.promise);
      const interrupt = yield* runtime
        .interruptTurn({ providerThread, providerTurnId: activeTurn.id })
        .pipe(Effect.forkScoped);
      yield* Effect.promise(() => abortCalled.promise);
      yield* Fiber.join(interrupt);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(pendingSteer)));
      const promptWhileStopping = yield* Effect.exit(
        runtime.steerTurn({
          threadId,
          runId,
          providerThread,
          providerTurnId: activeTurn.id,
          message: {
            messageId: MessageId.make("message-opencode-after-stop"),
            text: "must not pass the pending stop",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
        }),
      );
      assert.isTrue(Exit.isFailure(promptWhileStopping));
      assert.equal(promptCalls, 3);
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: { sessionID: "native-opencode-race", status: { type: "idle" } },
        }),
      );
      const interrupted = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(interrupted.providerTurns.at(-1)?.status, "interrupted");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("rejects steering when stop wins the extracted-text lookup", () =>
    Effect.gen(function* () {
      const events = asyncEventStream();
      const lookup = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let prompts = 0;
      const harness = yield* makeOpenCodeRuntimeHarness(
        "extraction-stop",
        "root",
        {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
              options.signal?.addEventListener("abort", () => events.close(), { once: true });
              return { stream: events.stream };
            },
          },
          session: {
            create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
            get: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
            promptAsync: async () => {
              prompts++;
              return { data: true };
            },
            status: async () => ({ data: { root: { type: "busy" } } }),
            messages: async () => ({ data: [] }),
            children: async () => ({ data: [] }),
            abort: async () => ({ data: true }),
          },
          mcp: { add: async () => ({ data: true }) },
        },
        {
          resolveAttachmentPath: () => "/workspace/report.docx",
          resolveExtractedTextPath: () =>
            Deferred.succeed(lookup, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as("/workspace/report.docx.txt"),
            ),
        },
      );
      yield* harness.startTurn();
      const snapshot = yield* harness.runtime.readThreadSnapshot({
        providerThread: harness.providerThread,
      });
      const turn = snapshot.providerTurns.at(-1)!;
      const steer = yield* harness.runtime
        .steerTurn({
          threadId: harness.threadId,
          runId: harness.runId,
          providerThread: harness.providerThread,
          providerTurnId: turn.id,
          message: {
            messageId: MessageId.make("steer-extraction-stop"),
            text: "read this",
            createdBy: "user",
            creationSource: "web",
            attachments: [
              {
                type: "file",
                id: "report-docx",
                name: "report.docx",
                mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                sizeBytes: 1,
              },
            ],
          },
        })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* Deferred.await(lookup);
      yield* harness.runtime.interruptTurn({
        providerThread: harness.providerThread,
        providerTurnId: turn.id,
      });
      yield* Deferred.succeed(release, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(steer)));
      assert.equal(prompts, 1);
      yield* Effect.promise(() =>
        events.push({
          type: "session.status",
          properties: { sessionID: "root", status: { type: "idle" } },
        }),
      );
      const stopped = yield* harness.runtime.readThreadSnapshot({
        providerThread: harness.providerThread,
      });
      assert.equal(stopped.providerTurns.at(-1)?.status, "interrupted");
    }).pipe(
      Effect.provide(Layer.merge(IdAllocator.layer, McpProviderSessions.layer)),
      Effect.scoped,
    ),
  );

  it.effect("interrupts an initial prompt while its SDK request is pending", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const promptStarted = promiseGate<void>();
      const abortCalled = promiseGate<void>();
      let abortCalls = 0;
      const client = {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
            return { stream: nativeEvents.stream };
          },
        },
        session: {
          create: async () => ({
            data: { id: "native-opencode-initial-stop", time: { created: 1, updated: 1 } },
          }),
          get: async () => ({
            data: { id: "native-opencode-initial-stop", time: { created: 1, updated: 1 } },
          }),
          promptAsync: async (_input: unknown, options?: { readonly signal?: AbortSignal }) => {
            promptStarted.resolve();
            await new Promise<void>((_resolve, reject) => {
              const signal = options?.signal;
              if (signal?.aborted) {
                reject(signal.reason);
                return;
              }
              signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
            });
            return { data: true };
          },
          status: async () => ({
            data: { "native-opencode-initial-stop": { type: "idle" as const } },
          }),
          messages: async () => ({ data: [] }),
          children: async () => ({ data: [] }),
          abort: async () => {
            abortCalls += 1;
            abortCalled.resolve();
            return { data: true };
          },
        },
        mcp: { add: async () => ({ data: true }) },
      };
      const adapter = yield* makeOpenCodeAdapterV2({
        instanceId: ProviderInstanceId.make("opencode-initial-stop-test"),
        settings: OPEN_CODE_TEST_SETTINGS,
        environment: {},
      }).pipe(
        Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, {
          connectToOpenCodeServer: () =>
            Effect.succeed({ url: "http://test.invalid", external: true }),
          createOpenCodeSdkClient: () => client,
        } as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"]),
        Effect.provideService(ProviderHost.ProviderHost, {
          paths: { cwd: "/workspace" },
          resolveAttachmentPath: () => null,
        } as unknown as ProviderHost.ProviderHost["Service"]),
      );
      const threadId = ThreadId.make("thread-opencode-initial-stop");
      const providerSessionId = ProviderSessionId.make("session-opencode-initial-stop");
      const modelSelection = {
        instanceId: ProviderInstanceId.make("opencode-initial-stop-test"),
        model: "anthropic/claude-sonnet",
        options: [],
      };
      const policy = runtimePolicy("full-access", { cwd: "/workspace" });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy: policy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: policy,
      });
      const terminalEvents = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const now = yield* DateTime.now;
      const start = yield* runtime
        .startTurn({
          appThread: {
            id: threadId,
            projectId: ProjectId.make("project-opencode-initial-stop"),
            title: "initial stop",
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdBy: "user",
            creationSource: "web",
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId,
          runId: RunId.make("run-opencode-initial-stop"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("attempt-opencode-initial-stop"),
          rootNodeId: NodeId.make("node-opencode-initial-stop"),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make("message-opencode-initial-stop"),
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy: policy,
        })
        .pipe(Effect.forkScoped);

      yield* Effect.promise(() => promptStarted.promise);
      const running = yield* runtime.readThreadSnapshot({ providerThread });
      const activeTurn = running.providerTurns.at(-1)!;
      const interrupt = yield* runtime
        .interruptTurn({ providerThread, providerTurnId: activeTurn.id })
        .pipe(Effect.forkScoped);

      yield* Effect.promise(() => abortCalled.promise);
      yield* Fiber.join(start);
      yield* Fiber.join(interrupt);
      yield* Effect.promise(() =>
        nativeEvents.push({
          type: "session.status",
          properties: {
            sessionID: "native-opencode-initial-stop",
            status: { type: "idle" },
          },
        }),
      );

      const events = Array.from(yield* Fiber.join(terminalEvents));
      const terminals = events.filter((event) => event.type === "turn.terminal");
      assert.equal(abortCalls, 1);
      assert.lengthOf(terminals, 1);
      assert.equal(terminals[0]?.status, "interrupted");
      assert.isNull(terminals[0]?.failure ?? null);
      assert.isFalse(
        events.some(
          (event) =>
            (event.type === "turn.terminal" && event.status === "failed") ||
            (event.type === "provider_session.updated" && event.providerSession.status === "error"),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("fails an active turn when the OpenCode event stream ends cleanly", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const harness = yield* makeOpenCodeRuntimeHarness(
        "clean-event-eof",
        "native-opencode-clean-event-eof",
        {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
              options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
              return { stream: nativeEvents.stream };
            },
          },
          session: {
            create: async () => ({
              data: { id: "native-opencode-clean-event-eof", time: { created: 1, updated: 1 } },
            }),
            promptAsync: async () => ({ data: true }),
          },
        },
      );
      yield* harness.startTurn();
      const terminalEvents = yield* harness.runtime.events.pipe(
        Stream.runCollect,
        Effect.forkScoped,
      );

      nativeEvents.close();
      const received = Array.from(yield* Fiber.join(terminalEvents));
      assert.isTrue(
        received.some(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "error",
        ),
      );
      const terminal = received.find((event) => event.type === "turn.terminal");
      assert.equal(terminal?.status, "failed");
      assert.equal(terminal?.failure?.class, "transport_error");
      assert.equal(terminal?.threadDisposition, "broken");
      assert.equal((yield* Effect.exit(harness.startTurn()))._tag, "Failure");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("fails compaction when its response races stream termination", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      const summarizeStarted = promiseGate<void>();
      const summarizeResult = promiseGate<{ data: boolean }>();
      const baseClock = yield* Clock.Clock;
      const eofClockRead = yield* Deferred.make<void>();
      const releaseEofClockRead = yield* Deferred.make<void>();
      let blockNextClockRead = false;
      const blockingClock: Clock.Clock = {
        ...baseClock,
        currentTimeMillis: Effect.suspend(() => {
          if (!blockNextClockRead) return baseClock.currentTimeMillis;
          blockNextClockRead = false;
          return Deferred.succeed(eofClockRead, undefined).pipe(
            Effect.andThen(Deferred.await(releaseEofClockRead)),
            Effect.andThen(baseClock.currentTimeMillis),
          );
        }),
      };
      const harness = yield* makeOpenCodeRuntimeHarness(
        "compaction-eof-race",
        "native-opencode-compaction-eof-race",
        {
          event: { subscribe: async () => ({ stream: nativeEvents.stream }) },
          session: {
            create: async () => ({
              data: { id: "native-opencode-compaction-eof-race", time: { created: 1, updated: 1 } },
            }),
            summarize: () => {
              summarizeStarted.resolve();
              return summarizeResult.promise;
            },
          },
        },
      ).pipe(Effect.provideService(Clock.Clock, blockingClock));
      const events = yield* harness.runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      const start = yield* harness.startTurn("/compact").pipe(Effect.forkScoped);
      yield* Effect.promise(() => summarizeStarted.promise);
      blockNextClockRead = true;
      nativeEvents.close();
      yield* Deferred.await(eofClockRead);
      summarizeResult.resolve({ data: true });
      yield* Fiber.join(start);
      yield* Deferred.succeed(releaseEofClockRead, undefined);
      const received = Array.from(yield* Fiber.join(events));
      const terminals = received.filter((event) => event.type === "turn.terminal");
      assert.lengthOf(terminals, 1);
      assert.equal(terminals[0]?.status, "failed");
      assert.equal(terminals[0]?.failure?.class, "transport_error");
      assert.isFalse(
        received.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "compaction" &&
            event.turnItem.status === "completed",
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it.effect("does not register a turn after the OpenCode event stream ends", () =>
    Effect.gen(function* () {
      const nativeEvents = asyncEventStream();
      let promptCalls = 0;
      const harness = yield* makeOpenCodeRuntimeHarness(
        "event-eof-start-race",
        "native-opencode-event-eof-start-race",
        {
          event: {
            subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
              options.signal?.addEventListener("abort", () => nativeEvents.close(), { once: true });
              return { stream: nativeEvents.stream };
            },
          },
          session: {
            create: async () => ({
              data: {
                id: "native-opencode-event-eof-start-race",
                time: { created: 1, updated: 1 },
              },
            }),
            promptAsync: async () => {
              promptCalls += 1;
              return { data: true };
            },
          },
        },
      );
      const baseClock = yield* Clock.Clock;
      const startClockRead = yield* Deferred.make<void>();
      const releaseStartClockRead = yield* Deferred.make<void>();
      let blockNextClockRead = true;
      const blockingClock: Clock.Clock = {
        ...baseClock,
        currentTimeMillis: Effect.suspend(() => {
          if (!blockNextClockRead) return baseClock.currentTimeMillis;
          blockNextClockRead = false;
          return Deferred.succeed(startClockRead, undefined).pipe(
            Effect.andThen(Deferred.await(releaseStartClockRead)),
            Effect.andThen(baseClock.currentTimeMillis),
          );
        }),
      };
      const start = yield* harness
        .startTurn()
        .pipe(Effect.provideService(Clock.Clock, blockingClock), Effect.exit, Effect.forkScoped);
      yield* Deferred.await(startClockRead);

      const events = yield* harness.runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      nativeEvents.close();
      yield* Fiber.join(events);
      yield* Deferred.succeed(releaseStartClockRead, undefined);

      assert.isTrue(Exit.isFailure(yield* Fiber.join(start)));
      assert.equal(promptCalls, 0);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
  );

  it("holds stale idle through prompt admission until the new user message is observed", () => {
    const admission = {
      admissionPending: true,
      admissionAccepted: false,
      admissionMessageObserved: false,
      idleDuringAdmission: false,
    };

    assert.equal(advanceOpenCodePromptAdmission(admission, "idle"), "hold");
    assert.equal(advanceOpenCodePromptAdmission(admission, "accepted"), "hold");
    assert.isTrue(admission.admissionPending);
    assert.equal(advanceOpenCodePromptAdmission(admission, "user-message"), "reconcile-idle");
    assert.isTrue(admission.admissionPending);
  });

  it("releases admission when an assistant completes under a provider-assigned message id", () => {
    const admission = {
      admissionPending: true,
      admissionAccepted: false,
      admissionMessageObserved: false,
      idleDuringAdmission: true,
    };

    assert.equal(advanceOpenCodePromptAdmission(admission, "assistant-completed"), "release");
    assert.isTrue(admission.admissionAccepted);
    assert.isTrue(admission.admissionMessageObserved);
    assert.isFalse(admission.admissionPending);
  });

  it("invalidates pending admission before aborting a turn", () => {
    const admission = {
      admissionGeneration: 4,
      admissionPending: true,
      admissionAccepted: false,
      admissionMessageObserved: false,
      idleDuringAdmission: false,
    };

    cancelOpenCodePromptAdmission(admission, 5);

    assert.equal(admission.admissionGeneration, 5);
    assert.isFalse(admission.admissionPending);
    assert.equal(advanceOpenCodePromptAdmission(admission, "accepted"), "release");
  });

  it.effect("keeps admission pending after a transient status failure", () =>
    Effect.gen(function* () {
      const admission = { admissionGeneration: 4, admissionPending: true };
      assert.equal(
        yield* reconcileOpenCodePromptAdmissionStatus(admission, 4, Effect.succeed("unknown")),
        "unknown",
      );
      assert.isTrue(admission.admissionPending);
      assert.equal(
        yield* reconcileOpenCodePromptAdmissionStatus(admission, 4, Effect.succeed("idle")),
        "idle",
      );
      assert.isFalse(admission.admissionPending);
    }),
  );

  it.effect("ignores a delayed status reply after steering starts a newer admission", () =>
    Effect.gen(function* () {
      const admission = { admissionGeneration: 4, admissionPending: true };
      const status = yield* Deferred.make<"idle" | "busy" | "unknown">();
      const fiber = yield* reconcileOpenCodePromptAdmissionStatus(
        admission,
        4,
        Deferred.await(status),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      admission.admissionGeneration = 5;
      admission.admissionPending = true;
      yield* Deferred.succeed(status, "idle");

      assert.equal(yield* Fiber.join(fiber), "stale");
      assert.equal(admission.admissionGeneration, 5);
      assert.isTrue(admission.admissionPending);
    }),
  );

  it.effect("logs bounded structural protocol diagnostics without native payload values", () =>
    Effect.gen(function* () {
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const records: Array<unknown> = [];
      const nativeEventLogger: ProviderEventLoggers.EventNdjsonLogger = {
        filePath: "/tmp/provider-native.ndjson",
        write: (event) => Effect.sync(() => void records.push(event)),
        close: () => Effect.void,
      };
      const logProtocolEvent = makeOpenCodeProtocolLogger({
        nativeEventLogger,
        idAllocator,
        providerInstanceId: ProviderInstanceId.make("opencode-test"),
        providerSessionId: ProviderSessionId.make("provider-session-opencode-test"),
        threadId: ThreadId.make("thread-opencode-test"),
      });
      const secret = "secret-opencode-prompt";

      yield* logProtocolEvent({
        direction: "outgoing",
        messageKind: "request",
        method: "session.prompt",
        payload: { prompt: secret, nested: { token: secret } },
      });

      const serialized = encodeUnknownJson(records);
      assert.notInclude(serialized, secret);
      assert.include(serialized, '"protocol":"opencode-sdk.sse"');
      assert.include(serialized, '"method":"session.prompt"');
      assert.include(serialized, '"fieldCount":2');
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
    ),
  );

  it.effect("adopts the handed-over provider thread identity on session create", () =>
    Effect.gen(function* () {
      let createCount = 0;
      const createInputs: Array<unknown> = [];
      const fakeClient = {
        event: {
          subscribe: async (_input?: unknown, options?: { readonly signal?: AbortSignal }) => ({
            stream: {
              [Symbol.asyncIterator]: () => ({
                next: () =>
                  new Promise<IteratorResult<never>>((resolve) => {
                    const done = () => resolve({ done: true, value: undefined });
                    if (options?.signal?.aborted) return done();
                    options?.signal?.addEventListener("abort", done, { once: true });
                  }),
              }),
            },
          }),
        },
        session: {
          create: async (input: unknown) => {
            createCount += 1;
            createInputs.push(input);
            return { data: { id: `ses_native_${createCount}`, time: { created: 1, updated: 1 } } };
          },
        },
      } as unknown as OpencodeClient;
      const unused = (operation: string) => () => Effect.die(`${operation} is not used`);
      const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
        startOpenCodeServerProcess: unused("startOpenCodeServerProcess"),
        connectToOpenCodeServer: () =>
          Effect.succeed({
            url: "test://opencode",
            version: "test",
            exitCode: null,
            external: true,
          }),
        runOpenCodeCommand: unused("runOpenCodeCommand"),
        createOpenCodeSdkClient: () => fakeClient,
        loadOpenCodeInventory: unused("loadOpenCodeInventory"),
        loadOpenCodeSkills: unused("loadOpenCodeSkills"),
      };
      const instanceId = ProviderInstanceId.make("opencode");
      const threadId = ThreadId.make("thread-opencode-adopt");
      const modelSelection = { instanceId, model: "default" };
      const adapter = yield* makeOpenCodeAdapterV2({
        instanceId,
        settings: OPENCODE_TEST_SETTINGS,
        environment: {},
      }).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-opencode-adopt"),
        modelSelection,
        runtimePolicy: runtimePolicy("full-access"),
      });
      const now = yield* DateTime.now;
      const placeholder: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("thread:provider:opencode:native-thread:pending:run:adopt:1"),
        driver: OPENCODE_PROVIDER,
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const adopted = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: runtimePolicy("full-access"),
        existingProviderThread: placeholder,
      });
      assert.equal(adopted.id, placeholder.id);
      assert.equal(adopted.nativeThreadRef?.nativeId, "ses_native_1");
      const minted = yield* session.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy: runtimePolicy("full-access"),
      });
      assert.notEqual(minted.id, placeholder.id);
      assert.equal(minted.nativeThreadRef?.nativeId, "ses_native_2");
      for (const input of createInputs) {
        assert.notProperty(input, "title");
      }
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          McpProviderSessions.layer,
          TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
          FileSystem.layerNoop({}),
        ),
      ),
    ),
  );

  it("maps runtime modes to safe OpenCode permission rules", () => {
    const approvalRequired = openCodePermissionRules(runtimePolicy("approval-required"));
    assert.equal(permissionAction(approvalRequired, "read"), "allow");
    assert.equal(permissionAction(approvalRequired, "edit"), "ask");
    assert.equal(permissionAction(approvalRequired, "bash"), "ask");
    assert.equal(permissionAction(approvalRequired, "doom_loop"), "ask");
    assert.equal(permissionAction(approvalRequired, "unknown_plugin_tool"), "ask");
    assert.equal(permissionAction(approvalRequired, "question"), "allow");

    const autoAcceptEdits = openCodePermissionRules(runtimePolicy("auto-accept-edits"));
    assert.equal(permissionAction(autoAcceptEdits, "edit"), "allow");
    assert.equal(permissionAction(autoAcceptEdits, "bash"), "ask");

    const fullAccess = openCodePermissionRules(runtimePolicy("full-access"));
    assert.equal(permissionAction(fullAccess, "bash"), "allow");
    assert.equal(permissionAction(fullAccess, "edit"), "allow");

    const granularApproval = openCodePermissionRules(
      runtimePolicy("full-access", {
        approvalPolicy: { granular: { request_permissions: true } },
      }),
    );
    assert.equal(permissionAction(granularApproval, "bash"), "ask");
    assert.equal(permissionAction(granularApproval, "read"), "allow");

    const approvalRequiredWorkspaceWrite = openCodePermissionRules(
      runtimePolicy("approval-required", {
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: ["/tmp/opencode-workspace"],
          networkAccess: false,
        },
      }),
    );
    assert.equal(permissionAction(approvalRequiredWorkspaceWrite, "edit"), "ask");
  });

  it("enforces non-interactive sandbox policy through OpenCode permissions", () => {
    const readOnly = openCodePermissionRules(
      runtimePolicy("full-access", {
        approvalPolicy: "never",
        sandboxPolicy: {
          type: "readOnly",
          access: { type: "fullAccess" },
          networkAccess: false,
        },
      }),
    );
    assert.equal(permissionAction(readOnly, "read"), "allow");
    assert.equal(permissionAction(readOnly, "edit"), "deny");
    assert.equal(permissionAction(readOnly, "bash"), "deny");
    assert.equal(permissionAction(readOnly, "webfetch"), "deny");
    assert.equal(permissionAction(readOnly, "doom_loop"), "deny");
    assert.equal(permissionAction(readOnly, "unknown_plugin_tool"), "deny");
    assert.equal(permissionAction(readOnly, "external_directory"), "allow");

    const workspaceWrite = openCodePermissionRules(
      runtimePolicy("auto-accept-edits", {
        approvalPolicy: "never",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: ["/tmp/opencode-workspace"],
          networkAccess: true,
        },
      }),
    );
    assert.equal(permissionAction(workspaceWrite, "edit"), "allow");
    assert.equal(permissionAction(workspaceWrite, "bash"), "deny");
    assert.equal(permissionAction(workspaceWrite, "webfetch"), "allow");
    assert.deepInclude(workspaceWrite, {
      permission: "external_directory",
      pattern: "/tmp/opencode-workspace/*",
      action: "allow",
    });
  });

  it("preserves OpenCode's recursion guard on task-created child sessions", () => {
    const childRules = openCodeChildPermissionRules(runtimePolicy("full-access"), [
      { permission: "task", pattern: "*", action: "deny" },
    ]);

    assert.equal(permissionAction(childRules, "read"), "allow");
    assert.equal(permissionAction(childRules, "bash"), "allow");
    assert.equal(permissionAction(childRules, "task"), "deny");

    const approvalRequiredPolicy = runtimePolicy("approval-required");
    const parentRules = openCodePermissionRules(approvalRequiredPolicy);
    const childApprovalRules = openCodeChildPermissionRules(approvalRequiredPolicy, [
      ...parentRules.filter((rule) => rule.action === "deny"),
      { permission: "task", pattern: "*", action: "deny" },
    ]);
    assert.equal(permissionAction(childApprovalRules, "bash"), "ask");
    assert.equal(permissionAction(childApprovalRules, "task"), "deny");

    const fullAccessChildRules = openCodeChildPermissionRules(runtimePolicy("full-access"), [
      { permission: "task", pattern: "*", action: "deny" },
    ]);
    const reappliedRules = openCodeChildPermissionRules(
      approvalRequiredPolicy,
      fullAccessChildRules,
      openCodePermissionRules(runtimePolicy("full-access")),
    );
    assert.equal(permissionAction(reappliedRules, "bash"), "ask");
    assert.equal(permissionAction(reappliedRules, "task"), "deny");
  });

  it("uses the next native user message as the exclusive fork and revert boundary", () => {
    const first = providerTurn({ id: "turn:first", ordinal: 1, nativeId: "msg-user-1" });
    const synthetic = providerTurn({ id: "turn:synthetic", ordinal: 2, nativeId: null });
    const third = providerTurn({ id: "turn:third", ordinal: 3, nativeId: "msg-user-3" });

    assert.equal(
      openCodeBoundaryAfterProviderTurn([third, first, synthetic], first.id),
      "msg-user-3",
    );
    assert.isUndefined(openCodeBoundaryAfterProviderTurn([first, synthetic, third], third.id));
  });
});

it.effect.each([false, true])(
  "OpenCode rewind forks history and validates the retained boundary, invalid=%s",
  (invalid) =>
    Effect.gen(function* () {
      const events = asyncEventStream();
      const nativeSessionId = "rewind-source";
      const forkId = "rewind-fork";
      const calls: string[] = [];
      const removed = {
        info: { id: "first-user", sessionID: nativeSessionId, role: "user", time: { created: 1 } },
        parts: [],
      };
      const harness = yield* makeOpenCodeRuntimeHarness("rewind-fork", nativeSessionId, {
        event: {
          subscribe: async (_input: unknown, options: { signal?: AbortSignal }) => {
            options.signal?.addEventListener("abort", () => events.close(), { once: true });
            return { stream: events.stream };
          },
        },
        session: {
          create: async () => ({ data: { id: nativeSessionId, time: { created: 1, updated: 1 } } }),
          get: async ({ sessionID }: { sessionID: string }) => ({
            data: { id: sessionID, time: { created: 1, updated: 2 } },
          }),
          messages: async ({ sessionID }: { sessionID: string }) => ({
            data: sessionID === nativeSessionId || invalid ? [removed] : [],
          }),
          fork: async ({ sessionID, messageID }: { sessionID: string; messageID: string }) => {
            assert.equal(sessionID, nativeSessionId);
            assert.equal(messageID, "first-user");
            calls.push("fork");
            return { data: { id: forkId, time: { created: 1, updated: 2 } } };
          },
          update: async () => {
            calls.push("permissions");
            return { data: {} };
          },
          revert: async () => {
            throw new Error("Native revert would change workspace files");
          },
        },
      });
      const effect = harness.runtime.rollbackThread({
        providerThread: harness.providerThread,
        target: {
          type: "thread_start",
          checkpointId: CheckpointId.make("rewind-checkpoint"),
          appRunOrdinal: 0,
        },
        providerThreadTurns: [],
      });
      if (invalid) {
        const error = yield* effect.pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterRollbackThreadError");
        assert.deepEqual(calls, ["fork"]);
      } else {
        const result = yield* effect;
        assert.equal(result.providerThread.nativeThreadRef?.nativeId, forkId);
        assert.equal(result.messages.length, 0);
        assert.deepEqual(calls, ["fork", "permissions"]);
      }
    }).pipe(
      Effect.provide(
        Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, FileSystem.layerNoop({})),
      ),
      Effect.scoped,
    ),
);
