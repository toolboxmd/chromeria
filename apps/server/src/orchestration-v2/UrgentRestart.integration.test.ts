import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CodexSettings,
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type ModelSelection,
  type OrchestrationV2ThreadProjection,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { GrokSettings } from "@t3tools/provider-grok/settings";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as CodexClient from "effect-codex-app-server/client";
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { describe, vi } from "vite-plus/test";

import {
  GROK_ACP_CANCEL_META,
  GROK_ACP_INITIALIZE_META,
  grokAcpRuntimeProcessOwnership,
} from "@t3tools/provider-grok/testing";
import { makeXAiPromptCompletionRuntime } from "@t3tools/provider-grok/testing";
import * as OpenCode2Client from "@t3tools/provider-opencode/server/v2/OpenCode2Client";
import * as OpenCode2Server from "@t3tools/provider-opencode/server/v2/OpenCode2Server";
import {
  makeAcpReplayRuntime,
  decodeAcpReplayTranscript,
} from "./Adapters/AcpAdapterV2.testkit.ts";
import * as CodexAdapter from "./Adapters/CodexAdapterV2.ts";
import * as GrokAdapter from "@t3tools/provider-grok/testing";
import {
  OpenCodeOrchestratorReplayHarness,
  OpenCodeReplayController,
} from "./Adapters/OpenCodeAdapterV2.testkit.ts";
import * as OpenCode2Adapter from "@t3tools/provider-opencode/server/v2/adapter";
import * as EffectOutbox from "./EffectOutbox.ts";
import type {
  ProviderAdapterV2,
  ProviderAdapterV2Event,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as EffectWorker from "./EffectWorker.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { readProviderReplayTranscript } from "@t3tools/provider-testing/replayTranscript";

const threadId = ThreadId.make("target-thread");
const projectId = ProjectId.make("project:urgent-restart");
const senderThreadId = ThreadId.make("sender-thread");
const urgentText = "Stop the current action. Use recipient list B instead.";
const codexSettings = Schema.decodeSync(CodexSettings)({});
const grokSettings = Schema.decodeSync(GrokSettings)({});
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRecord = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown));
// Validate fixtures through the package-owned client's native event decoder.
const decodeOpenCodeEvent = (frame: unknown) =>
  Effect.gen(function* () {
    const native = yield* OpenCode2Client.make.pipe(
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(`data: ${JSON.stringify(frame)}\n\n`, {
                headers: { "content-type": "text/event-stream" },
              }),
            ),
          ),
        ),
      ),
    );
    const api = yield* native.connect({
      baseUrl: "http://urgent-opencode2.replay",
      password: "replay",
    });
    const decoded = yield* api.client.event.subscribe().pipe(Stream.take(1), Stream.runCollect);
    assert.lengthOf(decoded, 1, "Invalid native OpenCode event fixture.");
    return decoded[0]!;
  }).pipe(Effect.orDie);
type WireRequest = { readonly method: string; readonly params: unknown };
type Delivery = "restart" | "human" | "mobile" | "auto" | "steer" | "queue";

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null) throw new Error("Expected a native object.");
  return decodeRecord(value);
}

function promptText(value: unknown): string {
  if (Array.isArray(value)) return value.map(promptText).join("\n");
  const text = record(value).text;
  return typeof text === "string" ? text : "";
}

function nativeRequests(promptMethod: string) {
  const requests: WireRequest[] = [];
  let prompts = 0;
  const receipts = new Map<number, Deferred.Deferred<void>>();
  const promptSent = (ordinal: number) => {
    let receipt = receipts.get(ordinal);
    if (receipt === undefined) {
      receipt = Deferred.makeUnsafe<void>();
      receipts.set(ordinal, receipt);
    }
    return receipt;
  };
  return {
    requests,
    promptSent,
    capture: (request: WireRequest) => {
      requests.push(request);
      if (request.method === promptMethod) {
        Deferred.doneUnsafe(promptSent(++prompts), Effect.void);
      }
    },
  };
}

const immediate = (entry: ProviderReplayEntry): ProviderReplayEntry => {
  if (entry.type !== "emit_inbound") return entry;
  const { afterMs: _afterMs, ...frame } = entry;
  return frame;
};
const fixture = (name: string, file: string) =>
  readProviderReplayTranscript(new URL(`./testkit/fixtures/${name}/${file}`, import.meta.url));
const outbound = (type: string, input: unknown): ProviderReplayEntry => ({
  type: "expect_outbound",
  frame: { type, input },
});

// Observe the real adapter stream once, where the session manager consumes it.
// Nothing here manufactures cancellation, child ownership or terminal events.
function observedAdapter(
  adapter: ProviderAdapterV2["Service"],
  events: ProviderAdapterV2Event[],
  observe: (event: ProviderAdapterV2Event) => Effect.Effect<void> = () => Effect.void,
  failures: string[] = [],
) {
  return {
    ...adapter,
    openSession: (input) =>
      adapter.openSession(input).pipe(
        Effect.map((runtime) => ({
          ...runtime,
          startTurn: (input) =>
            runtime
              .startTurn(input)
              .pipe(
                Effect.tapCause((cause) => Effect.sync(() => failures.push(Cause.pretty(cause)))),
              ),
          events: runtime.events.pipe(
            Stream.tap((event) =>
              Effect.sync(() => events.push(event)).pipe(Effect.andThen(observe(event))),
            ),
          ),
        })),
      ),
  } satisfies ProviderAdapterV2["Service"];
}

function observedRegistry(...input: Parameters<typeof observedAdapter>) {
  return ProviderAdapterRegistry.layerFromAdapters([observedAdapter(...input)]);
}

interface NativeBoundary<E = never> {
  readonly requests: WireRequest[];
  readonly nativeEvents: ProviderAdapterV2Event[];
  readonly nativeThreadId: string;
  readonly registry: Layer.Layer<
    ProviderAdapterRegistry.ProviderAdapterRegistryV2,
    E,
    McpProviderSessions.McpProviderSessions
  >;
  readonly modelSelection: ModelSelection;
  readonly ready: (event: OrchestrationV2DomainEvent) => boolean;
  readonly nativeFailures?: string[];
}

const setupScenario = Effect.fn("setupUrgentStopScenario")(function* (
  boundary: Omit<NativeBoundary, "registry">,
  cwd: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const management = yield* ThreadManagementService.ThreadManagementService;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const events: OrchestrationV2DomainEvent[] = [];
  yield* orchestrator.streamDomainEvents.pipe(
    Stream.runForEach((event) => Effect.sync(() => events.push(event))),
    Effect.forkScoped({ startImmediately: true }),
  );
  const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          predicate(event) || (event.type === "run.updated" && event.payload.status === "failed"),
      ),
      Stream.tap((event) =>
        event.type === "run.updated" && event.payload.status === "failed"
          ? Effect.die(
              `Provider failed before milestone: ${boundary.nativeFailures?.join("\n") ?? event.payload.id}`,
            )
          : Effect.void,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped({ startImmediately: true }),
    );
  for (const id of [threadId, senderThreadId])
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${id}`),
      threadId: id,
      projectId,
      title: id,
      modelSelection: boundary.modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
  const ready = yield* watch(boundary.ready);
  yield* management.sendToThread({
    commandId: CommandId.make("first"),
    threadId,
    projectId,
    messageId: MessageId.make("first"),
    text: "Start the original action.",
    attachments: [],
    mode: "auto",
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(ready);
  const send = Effect.fn("sendUrgentStopMessage")(function* (
    delivery: Delivery,
    key = "urgent",
    text = urgentText,
  ) {
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const active = projection.runs.findLast((run) => run.status === "running");
    if (active === undefined) return yield* Effect.die("Missing running target run.");
    if (delivery === "human" || delivery === "mobile")
      return yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(key),
        threadId,
        messageId: MessageId.make(key),
        text,
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: active.id },
        deliveryIntent: delivery === "mobile" ? "auto" : "steer",
        createdBy: "user",
        creationSource: delivery === "mobile" ? "mobile" : "web",
      });
    const result = yield* management.sendToThread({
      commandId: CommandId.make(key),
      threadId,
      projectId,
      messageId: MessageId.make(key),
      text,
      attachments: [],
      mode: delivery,
      createdBy: "agent",
      creationSource: "mcp",
      senderThreadId,
    });
    if (delivery === "restart") assert.equal(result.delivery, "restarted");
    return result;
  });
  const runtime = Effect.gen(function* () {
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const sessionId = projection.providerThreads.find(
      (thread) => thread.appThreadId === threadId,
    )?.providerSessionId;
    if (sessionId === undefined || sessionId === null) return yield* Effect.die("Missing session.");
    const session = yield* sessions.get(sessionId);
    if (Option.isNone(session)) return yield* Effect.die("Missing live adapter runtime.");
    return session.value;
  });
  return { orchestrator, worker, management, events, watch, send, runtime, outbox };
});
type Scenario = Effect.Success<ReturnType<typeof setupScenario>>;

const withScenario = <RegistryError, E, R>(
  boundary: NativeBoundary<RegistryError>,
  body: (scenario: Scenario) => Effect.Effect<void, E, R>,
) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("urgent-stop");
    return yield* setupScenario(boundary, cwd).pipe(
      Effect.flatMap(body),
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: "urgent-stop", runtimePolicyOverride: { cwd } },
          boundary.registry,
          { runEffectWorker: false },
        ),
      ),
    );
  });

function assertReplacement(
  before: OrchestrationV2ThreadProjection,
  after: OrchestrationV2ThreadProjection,
  request: WireRequest,
  delivery: Delivery,
  key = "urgent",
  text = urgentText,
) {
  const oldTurn = before.providerTurns.findLast((turn) => turn.status === "running")!;
  const oldAttempt = before.attempts.find((attempt) => attempt.id === oldTurn.runAttemptId)!;
  const next = after.attempts.find(
    (attempt) =>
      attempt.runId === oldAttempt.runId &&
      attempt.attemptOrdinal === oldAttempt.attemptOrdinal + 1,
  );
  assert.equal(after.providerTurns.find((turn) => turn.id === oldTurn.id)?.status, "interrupted");
  assert.equal(
    after.attempts.find((attempt) => attempt.id === oldAttempt.id)?.status,
    "superseded",
  );
  assert.equal(next?.reason, "steering_restart");
  assert.isTrue(
    after.providerTurns.some((turn) => turn.runAttemptId === next?.id && turn.status === "running"),
  );
  const person = delivery === "human" || delivery === "mobile";
  assert.deepEqual(
    next?.requestedBy,
    person
      ? { createdBy: "user", creationSource: delivery === "mobile" ? "mobile" : "web" }
      : { createdBy: "agent", creationSource: "mcp", senderThreadId },
  );
  const message = after.messages.find((message) => message.id === MessageId.make(key));
  assert.equal(message?.text, text);
  assert.equal(message?.createdBy, person ? "user" : "agent");
  assert.equal(
    message?.creationSource,
    person ? (delivery === "mobile" ? "mobile" : "web") : "mcp",
  );
  if (!person) assert.equal(message?.senderThreadId, senderThreadId);
  const input = record(request.params);
  const prompt =
    request.method === "turn/start"
      ? promptText(input.input)
      : request.method === "session/prompt"
        ? promptText(input.prompt)
        : request.method === "session.promptAsync"
          ? promptText(input.parts)
          : String(input.text);
  assert.include(prompt, text);
  if (!person) {
    const userRequest = prompt.includes("<user_request>")
      ? prompt.split("<user_request>")[1]!
      : prompt;
    assert.match(userRequest, /Note from T3 Code:.*agent.*sender-thread.*interrupted/s);
    assert.match(userRequest, /interrupted, not refused/i);
    assert.isBelow(userRequest.indexOf("sender-thread"), userRequest.indexOf(text));
    assert.equal(userRequest.split("Note from T3 Code:").length - 1, 1);
  }
}
const response = (operation: string, data: unknown): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.response", operation, data },
});
const nativeEvent = (type: string, data: unknown, generation: 1 | 2): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.event", event: { type, [generation === 1 ? "properties" : "data"]: data } },
});

// Only native I/O is replaced. The adapters, capabilities, session manager,
// management service, orchestrator, effect worker and persistence remain real.
const makeCodexBoundary = Effect.fn("makeCodexUrgentRestartBoundary")(function* (
  options: {
    readonly instanceId?: ProviderInstanceId;
    readonly child?: boolean;
    readonly retained?: boolean;
    readonly delegated?: boolean;
    readonly childInterruptFailures?: number;
    readonly overlap?: boolean;
    readonly holdLateChild?: boolean;
    readonly holdFirstChild?: boolean;
  } = {},
) {
  const scope = yield* Scope.Scope;
  const observer = nativeRequests("turn/start");
  const nativeEvents: ProviderAdapterV2Event[] = [];
  const inbound = yield* Queue.unbounded<Uint8Array>();
  const containmentEntered = yield* Deferred.make<void>();
  const releaseContainment = yield* Deferred.make<void>();
  const steerEntered = yield* Deferred.make<void>();
  const recorded = yield* fixture("simple", "codex_transcript.ndjson");
  const threadResponse = recorded.entries.find(
    (entry) => entry.type === "emit_inbound" && entry.label === "thread/start",
  );
  if (threadResponse?.type !== "emit_inbound") throw new Error("Missing Codex thread response.");
  const startedThread = record(record(threadResponse.frame).result);
  const nativeThreadId = String(record(startedThread.thread).id);
  const childId = "native-codex-child";
  const nextChildId = "native-codex-next-child";
  const lateChildId = childId;
  const childInterruptEntered = yield* Deferred.make<void>();
  let holdChildInterrupt = options.holdLateChild === true || options.holdFirstChild === true;
  let heldChildInterruptId: unknown;
  const childTurns = new Map([
    [childId, { turnId: "child-turn", itemId: "child-held-command" }],
    [nextChildId, { turnId: "next-child-turn", itemId: "next-child-command" }],
  ]);
  const appChildId = "native-app-delegated-child";
  const currentOrdinal = options.retained ? 2 : 1;
  const qualify = (id: string) =>
    options.instanceId === undefined ? id : `${options.instanceId}:${id}`;
  const rootTurnId = (ordinal: number) => qualify(`native-turn-${ordinal}`);
  const turn = (id: string, status: "inProgress" | "interrupted" | "completed" | "failed") => ({
    id,
    status,
    items: [],
    itemsView: "notLoaded",
    error:
      status === "failed"
        ? { message: "NATIVE_ROOT_FAILED_BEFORE_CUT_IN_EFFECT", codexErrorInfo: null }
        : null,
    startedAt: 1790295404,
    completedAt: status === "inProgress" ? null : 1790295405,
    durationMs: null,
  });
  const emit = (frame: unknown) =>
    Queue.offer(inbound, new TextEncoder().encode(`${encodeJson(frame)}\n`));
  const command = (id: string, processId: string | null, completed = false) => ({
    type: "commandExecution",
    id: qualify(id),
    command: "held foreground command",
    cwd: "/workspace",
    processId,
    source: "unifiedExecStartup",
    status: completed ? "completed" : "inProgress",
    commandActions: [],
    aggregatedOutput: completed ? "CHILD_PARTIAL" : null,
    exitCode: completed ? 0 : null,
    durationMs: null,
  });
  const completeChild = (status: "completed" | "interrupted", id = childId) =>
    Effect.gen(function* () {
      const child = childTurns.get(id);
      if (child === undefined) return yield* Effect.die("Unknown native child.");
      yield* emit({
        method: "item/completed",
        params: {
          threadId: id,
          turnId: child.turnId,
          item: command(child.itemId, null, true),
          completedAtMs: 1790295405100,
        },
      });
      yield* emit({
        method: "turn/completed",
        params: { threadId: id, turn: turn(child.turnId, status) },
      });
    });
  const spawnChild = (id: string, parentTurnId: string) =>
    Effect.gen(function* () {
      const child = childTurns.get(id);
      if (child === undefined) return yield* Effect.die("Unknown native child.");
      yield* emit({
        method: "item/completed",
        params: {
          threadId: nativeThreadId,
          turnId: parentTurnId,
          item: {
            type: "subAgentActivity",
            id: id === childId ? "native-spawn" : `native-spawn:${id}`,
            kind: "started",
            agentThreadId: id,
            agentPath: id === childId ? "/root/held-child" : `/root/${id}`,
          },
          completedAtMs: 1790295404100,
        },
      });
      yield* emit({
        method: "turn/started",
        params: { threadId: id, turn: turn(child.turnId, "inProgress") },
      });
      yield* emit({
        method: "item/started",
        params: {
          threadId: id,
          turnId: child.turnId,
          item: command(child.itemId, null),
          startedAtMs: 1790295404100,
        },
      });
    });
  let childInterruptFailures = options.childInterruptFailures ?? 0;
  let turnCount = 0;
  let threadCount = 0;
  const write = (chunk: string | Uint8Array) =>
    Effect.gen(function* () {
      for (const line of (typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk))
        .trim()
        .split("\n")) {
        const frame = record(decodeJson(line));
        if (typeof frame.method !== "string") continue;
        const params = frame.params;
        observer.capture({ method: frame.method, params });
        const reply = (result: unknown) => emit({ id: frame.id, result });
        switch (frame.method) {
          case "initialize":
            yield* reply({
              userAgent: "urgent-restart-test",
              codexHome: "/workspace/.codex",
              platformFamily: "unix",
              platformOs: "linux",
            });
            break;
          case "initialized":
            break;
          case "thread/start": {
            const id = options.delegated && ++threadCount > 1 ? appChildId : nativeThreadId;
            yield* reply({
              ...startedThread,
              thread: { ...record(startedThread.thread), id, sessionId: id },
            });
            break;
          }
          case "thread/resume":
            yield* reply(startedThread);
            break;
          case "thread/read":
            yield* reply({
              thread: {
                ...record(startedThread.thread),
                id: record(params).threadId,
                sessionId: record(params).threadId,
                turns: [],
              },
            });
            break;
          case "turn/start": {
            if (options.delegated && record(params).threadId === appChildId) {
              const nativeTurn = turn("app-child-turn", "inProgress");
              yield* reply({ turn: nativeTurn });
              yield* emit({
                method: "turn/started",
                params: { threadId: appChildId, turn: nativeTurn },
              });
              yield* emit({
                method: "item/started",
                params: {
                  threadId: appChildId,
                  turnId: nativeTurn.id,
                  item: command("app-owned-held-command", "171"),
                  startedAtMs: 1790295404100,
                },
              });
              break;
            }
            const nativeTurn = turn(rootTurnId(++turnCount), "inProgress");
            yield* reply({ turn: nativeTurn });
            yield* emit({
              method: "turn/started",
              params: { threadId: nativeThreadId, turn: nativeTurn },
            });
            if (turnCount <= currentOrdinal) {
              yield* emit({
                method: "item/started",
                params: {
                  threadId: nativeThreadId,
                  turnId: nativeTurn.id,
                  startedAtMs: 1790295404100,
                  item: command(
                    turnCount === currentOrdinal ? "held-command" : "retained-command",
                    turnCount === currentOrdinal ? "99" : "77",
                  ),
                },
              });
            }
            if (options.child && turnCount === currentOrdinal)
              yield* spawnChild(childId, nativeTurn.id);
            if (options.overlap && turnCount === currentOrdinal + 1)
              yield* spawnChild(nextChildId, nativeTurn.id);
            break;
          }
          case "turn/interrupt": {
            const targetThreadId = String(record(params).threadId);
            if (
              targetThreadId === lateChildId &&
              (record(params).turnId === "late-child-turn" || options.holdFirstChild === true) &&
              holdChildInterrupt
            ) {
              heldChildInterruptId = frame.id;
              yield* Deferred.succeed(childInterruptEntered, undefined);
              break;
            }
            if (childTurns.has(targetThreadId) && childInterruptFailures > 0) {
              childInterruptFailures--;
              yield* emit({
                id: frame.id,
                error: { code: -32000, message: "Child cancellation unavailable" },
              });
              break;
            }
            yield* reply({});
            if (record(params).threadId === appChildId) {
              yield* emit({
                method: "turn/completed",
                params: { threadId: appChildId, turn: turn("app-child-turn", "interrupted") },
              });
              break;
            }
            if (options.child) {
              if (record(params).threadId === nativeThreadId)
                yield* emit({
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: turn(String(record(params).turnId), "interrupted"),
                  },
                });
              else {
                assert.isTrue(childTurns.has(targetThreadId));
                yield* completeChild("interrupted", targetThreadId);
              }
            }
            break;
          }
          case "thread/backgroundTerminals/terminate": {
            yield* Deferred.succeed(containmentEntered, undefined);
            // The app-server can read another request while this reply waits.
            // Holding the stdio sink here would create a fake writer deadlock.
            yield* Effect.gen(function* () {
              yield* Deferred.await(releaseContainment);
              yield* reply({ terminated: true });
              if (record(params).processId === "99" && !options.child)
                yield* emit({
                  method: "turn/completed",
                  params: {
                    threadId: nativeThreadId,
                    turn: turn(rootTurnId(currentOrdinal), "interrupted"),
                  },
                });
            }).pipe(Effect.forkIn(scope, { startImmediately: true }));
            break;
          }
          case "thread/inject_items":
            yield* reply({});
            break;
          case "thread/unsubscribe":
            yield* reply({ status: "unsubscribed" });
            break;
          case "turn/steer":
            yield* Deferred.succeed(steerEntered, undefined);
            yield* reply({ turnId: rootTurnId(1) });
            break;
          default:
            return yield* Effect.die(`Unexpected native Codex request: ${frame.method}`);
        }
      }
    });
  const clientFactory = {
    open: () =>
      CodexClient.make(
        Stdio.make({
          args: Effect.succeed([]),
          stdin: Stream.fromQueue(inbound),
          stdout: () => Sink.forEach(write),
          stderr: () => Sink.drain,
        }),
      ),
  };
  const adapter = yield* CodexAdapter.makeCodexAdapterV2({
    instanceId: options.instanceId ?? CodexAdapter.CODEX_DEFAULT_INSTANCE_ID,
    settings: codexSettings,
    environment: {},
    clientFactory,
    crypto: yield* Crypto.Crypto,
    fileSystem: yield* FileSystem.FileSystem,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ProviderReplayHarness.makeReplayServerConfig("urgent-codex"),
  });
  return {
    ...observer,
    nativeEvents,
    nativeThreadId,
    childId,
    nextChildId,
    lateChildId,
    appChildId,
    emit,
    completeChild,
    spawnLateChild: () =>
      Effect.gen(function* () {
        // The known child's next turn starts after its first native end.
        // No post-root spawn notification is needed to establish its lineage.
        childTurns.set(lateChildId, { turnId: "late-child-turn", itemId: "late-child-command" });
        yield* emit({
          method: "turn/started",
          params: { threadId: lateChildId, turn: turn("late-child-turn", "inProgress") },
        });
        yield* emit({
          method: "item/started",
          params: {
            threadId: lateChildId,
            turnId: "late-child-turn",
            item: command("late-child-command", null),
            startedAtMs: 1790295405100,
          },
        });
      }),
    lateChildMessage: () =>
      emit({
        method: "item/completed",
        params: {
          threadId: lateChildId,
          turnId: "late-child-turn",
          item: { type: "agentMessage", id: "late-sentinel", text: "LATE_READER_PROGRESS" },
          completedAtMs: 1790295405100,
        },
      }),
    childInterruptEntered,
    answerHeldChildInterrupt: (outcome: "ack" | "reject" | "silent") =>
      Effect.gen(function* () {
        if (heldChildInterruptId === undefined)
          return yield* Effect.die("No held native interrupt.");
        holdChildInterrupt = false;
        if (outcome === "silent") return;
        if (outcome === "reject") {
          yield* emit({
            id: heldChildInterruptId,
            error: { code: -32000, message: "Late child cancellation unavailable" },
          });
          return;
        }
        yield* emit({ id: heldChildInterruptId, result: {} });
        yield* completeChild("interrupted", lateChildId);
      }),
    allowChildStop: () => Effect.sync(() => (childInterruptFailures = 0)),
    endRoot: (status: "completed" | "failed", ordinal = currentOrdinal) =>
      emit({
        method: "turn/completed",
        params: { threadId: nativeThreadId, turn: turn(rootTurnId(ordinal), status) },
      }),
    completeRootCommand: () =>
      emit({
        method: "item/completed",
        params: {
          threadId: nativeThreadId,
          turnId: rootTurnId(currentOrdinal),
          item: command("held-command", "99", true),
          completedAtMs: 1790295405100,
        },
      }),
    completeEarlier: () =>
      emit({
        method: "turn/completed",
        params: { threadId: nativeThreadId, turn: turn(rootTurnId(1), "completed") },
      }),
    registry: observedRegistry(adapter, nativeEvents),
    adapter: observedAdapter(adapter, nativeEvents),
    modelSelection: { instanceId: adapter.instanceId, model: "gpt-6-luna" },
    ready: (event: OrchestrationV2DomainEvent) =>
      event.type === "turn-item.updated" &&
      event.payload.nativeItemRef?.nativeId ===
        (options.retained
          ? "retained-command"
          : options.child
            ? "child-held-command"
            : "held-command") &&
      event.payload.status === "running",
    containment: { entered: containmentEntered, release: releaseContainment, steerEntered },
  };
});

const makeGrokBoundary = Effect.fn("makeGrokUrgentRestartBoundary")(function* () {
  const observer = nativeRequests("session/prompt");
  const nativeEvents: ProviderAdapterV2Event[] = [];
  const nativeThreadId = "00000000-0000-4000-8000-000000000001";
  const childId = "00000000-0000-4000-8000-000000000002";
  const processes: Array<{
    readonly pid: number;
    readonly exited: Deferred.Deferred<void>;
    readonly exitCode: ChildProcessSpawner.ChildProcessHandle["exitCode"];
    readonly isRunning: ChildProcessSpawner.ChildProcessHandle["isRunning"];
  }> = [];
  const loadedHistory: string[] = [];
  const recorded = yield* fixture("message_steering", "grok_transcript.ndjson");
  const firstPromptIndex = recorded.entries.findIndex(
    (entry) => entry.type === "expect_outbound" && record(entry.frame).method === "session/prompt",
  );
  const preamble = recorded.entries.slice(0, firstPromptIndex).map((entry) => {
    return immediate(entry);
  });
  const acpOut = (
    kind: "request" | "notification",
    method: string,
    params: unknown,
  ): ProviderReplayEntry => ({ type: "expect_outbound", frame: { kind, method, params } });
  const acpIn = (method: string, params: unknown): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "notification", method, params },
  });
  const acpReply = (method: string, result: unknown): ProviderReplayEntry => ({
    type: "emit_inbound",
    frame: { kind: "response", method, result },
  });
  const update = (value: unknown) =>
    acpIn("session/update", {
      sessionId: nativeThreadId,
      update: value,
      _meta: { promptId: "t3-xai-prompt-1" },
    });
  const nativeBackground: ProviderReplayEntry[] = [
    update({
      sessionUpdate: "tool_call",
      toolCallId: "spawn-call",
      title: "spawn_subagent",
      rawInput: {
        description: "Held native child",
        prompt: "Wait for cancellation.",
        background: true,
      },
    }),
    update({
      sessionUpdate: "tool_call_update",
      toolCallId: "spawn-call",
      status: "completed",
      rawOutput: {
        type: "Text",
        text: `Subagent started in background.\nsubagent_id: ${childId}\ndescription: Held native child`,
      },
    }),
    acpIn("_x.ai/session_notification", {
      sessionId: nativeThreadId,
      update: {
        sessionUpdate: "subagent_spawned",
        subagent_id: childId,
        attempt_id: "native-child-attempt",
        parent_session_id: nativeThreadId,
        parent_prompt_id: "t3-xai-prompt-1",
        child_session_id: childId,
        subagent_type: "general-purpose",
        description: "Held native child",
        model: "grok-4.7",
      },
    }),
    update({
      sessionUpdate: "tool_call",
      toolCallId: "monitor-call",
      title: "monitor",
      rawInput: { command: "held monitor", description: "Held monitor", variant: "Monitor" },
    }),
    acpIn("_x.ai/session_notification", {
      sessionId: nativeThreadId,
      update: {
        sessionUpdate: "background_tasks",
        tasks: [
          {
            task_id: "monitor-task",
            command: "held monitor",
            description: "Held monitor",
            cwd: "<workspace>",
            kind: "monitor",
            status: "running",
            started_at: "2026-10-14T12:00:00Z",
            output_file: "<workspace>/monitor.log",
          },
        ],
      },
    }),
    update({
      sessionUpdate: "tool_call_update",
      toolCallId: "monitor-call",
      status: "completed",
      rawOutput: {
        type: "Monitor",
        taskId: "monitor-task",
        timeoutMs: 36000000,
        persistent: false,
      },
    }),
    update({
      sessionUpdate: "tool_call",
      toolCallId: "held-command",
      title: "run_terminal_command",
      kind: "execute",
      status: "in_progress",
      rawInput: { command: "held foreground command" },
    }),
    update({
      sessionUpdate: "tool_call_update",
      toolCallId: "held-command",
      title: "Execute held foreground command",
      kind: "execute",
      status: "in_progress",
      rawInput: { variant: "Bash", command: "held foreground command" },
      rawOutput: {
        type: "Bash",
        command: "held foreground command",
        output: [],
        output_for_prompt: "",
        exit_code: 0,
      },
    }),
  ];
  const prompt = () =>
    acpOut("request", "session/prompt", {
      sessionId: nativeThreadId,
      prompt: "<any>",
      _meta: "<any>",
    });
  const cancel = () =>
    acpOut("notification", "session/cancel", {
      sessionId: nativeThreadId,
      _meta: { cancelTrigger: "ctrl_c" },
    });
  // This process can answer the old soft path as well as be killed on Stop.
  // A missing Stop flag therefore fails a behavior assertion, not a replay mismatch.
  const softTurns: ProviderReplayEntry[] = [];
  for (let ordinal = 1; ordinal <= 4; ordinal += 1) {
    softTurns.push(prompt());
    if (ordinal === 1) softTurns.push(...nativeBackground);
    softTurns.push(cancel(), acpReply("session/prompt", { stopReason: "cancelled" }));
  }
  softTurns.push(
    prompt(),
    acpReply("session/prompt", { stopReason: "end_turn" }),
    acpOut("request", "session/close", { sessionId: nativeThreadId }),
    acpReply("session/close", {}),
    { type: "runtime_exit", status: "success" },
  );
  const temporarySessionId = "00000000-0000-4000-8000-000000000003";
  const resumedPreamble = preamble.map((entry) =>
    entry.type === "runtime_exit"
      ? entry
      : {
          ...entry,
          frame: decodeJson(encodeJson(entry.frame).replaceAll(nativeThreadId, temporarySessionId)),
        },
  );
  const newSessionResponse = preamble.find(
    (entry) => entry.type === "emit_inbound" && record(entry.frame).method === "session/new",
  );
  if (newSessionResponse?.type !== "emit_inbound")
    return yield* Effect.die("Missing native Grok setup response.");
  const loadResponse = {
    ...record(record(newSessionResponse.frame).result),
    sessionId: nativeThreadId,
  };
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const replayDir = yield* fs.makeTempDirectory({ prefix: "urgent-stop-grok-" });
  yield* Effect.addFinalizer(() => fs.remove(replayDir, { recursive: true }).pipe(Effect.orDie));
  const realSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const hostPlatform = yield* HostProcess.Platform;
  const spawner = ChildProcessSpawner.make((command) =>
    realSpawner.spawn(command).pipe(
      Effect.tap((child) =>
        Effect.gen(function* () {
          const exited = yield* Deferred.make<void>();
          processes.push({
            pid: Number(child.pid),
            exited,
            exitCode: child.exitCode,
            isRunning: child.isRunning,
          });
        }),
      ),
    ),
  );
  let runtimeOrdinal = 0;
  const adapter = yield* GrokAdapter.makeGrokAdapterV2({
    instanceId: GrokAdapter.GROK_DEFAULT_INSTANCE_ID,
    settings: grokSettings,
    environment: {},
    hostPlatform: yield* HostProcess.Platform,
    selfInvocation: yield* resolveSelfInvocation(),
    continuationRequests: yield* ProviderContinuationRequests.ProviderContinuationRequests,
    makeRuntime: (input) =>
      Effect.gen(function* () {
        const ordinal = ++runtimeOrdinal;
        const transcript = yield* decodeAcpReplayTranscript(
          {
            ...recorded,
            scenario: `urgent-grok-process-${ordinal}`,
            entries:
              ordinal === 1
                ? [...preamble, ...softTurns]
                : [
                    ...resumedPreamble,
                    acpOut("request", "session/load", {
                      sessionId: nativeThreadId,
                      cwd: "<workspace>",
                      mcpServers: "<any>",
                    }),
                    acpReply("session/load", loadResponse),
                    acpIn("session/update", {
                      sessionId: nativeThreadId,
                      update: {
                        sessionUpdate: "user_message_chunk",
                        content: { type: "text", text: "Start the original action." },
                      },
                    }),
                    prompt(),
                    cancel(),
                    acpReply("session/prompt", { stopReason: "cancelled" }),
                    prompt(),
                    cancel(),
                    acpReply("session/prompt", { stopReason: "cancelled" }),
                    acpOut("request", "session/close", { sessionId: nativeThreadId }),
                    acpReply("session/close", {}),
                    { type: "runtime_exit", status: "success" },
                  ],
          },
          GrokAdapter.GROK_PROVIDER,
        ).pipe(Effect.orDie);
        const replay = makeAcpReplayRuntime({
          transcript,
          statusPath: path.join(replayDir, `process-${ordinal}.json`),
          scriptPath: yield* path
            .fromFileUrl(new URL("../../scripts/acp-replay-agent.ts", import.meta.url))
            .pipe(Effect.orDie),
          childProcessSpawner: spawner,
          fileSystem: fs,
          cancelMeta: GROK_ACP_CANCEL_META,
          initializeMeta: GROK_ACP_INITIALIZE_META,
        });
        const replayInput = {
          ...input,
          ...grokAcpRuntimeProcessOwnership(hostPlatform),
          protocolLogging: {
            logOutgoing: true,
            logIncoming: true,
            logger: (event) =>
              Effect.sync(() => {
                if (event.stage !== "raw" || typeof event.payload !== "string") return;
                for (const line of event.payload.trim().split("\n")) {
                  const frame = record(decodeJson(line));
                  if (event.direction === "outgoing" && typeof frame.method === "string")
                    observer.capture({ method: frame.method, params: frame.params });
                  if (event.direction === "incoming" && frame.method === "session/update") {
                    const content = record(record(frame.params).update);
                    if (content.sessionUpdate === "user_message_chunk")
                      loadedHistory.push(String(record(content.content).text));
                  }
                }
              }),
          },
        } satisfies Parameters<typeof replay>[0];
        const runtime = yield* replay(replayInput);
        if (runtime.terminateProcessGroup === undefined)
          return yield* Effect.die("Fake ACP process lacks real owned-group teardown.");
        const spawned = processes.at(-1)!;
        const wrapped = yield* makeXAiPromptCompletionRuntime({
          ...runtime,
          terminateProcessGroup: Effect.sync(() =>
            observer.capture({ method: "process-group/kill", params: { pid: spawned.pid } }),
          ).pipe(
            Effect.andThen(runtime.terminateProcessGroup),
            // A process killed by SIGTERM reports a failed exitCode effect.
            // Observe that exit, then independently confirm it is no longer running.
            Effect.andThen(spawned.exitCode.pipe(Effect.exit)),
            Effect.andThen(spawned.isRunning.pipe(Effect.orDie)),
            Effect.tap((running) => Effect.sync(() => assert.isFalse(running))),
            Effect.andThen(Deferred.succeed(spawned.exited, undefined)),
          ),
        });
        return wrapped;
      }),
  });
  return {
    ...observer,
    nativeEvents,
    nativeThreadId,
    childId,
    processes,
    loadedHistory,
    registry: observedRegistry(adapter, nativeEvents),
    modelSelection: { instanceId: adapter.instanceId, model: "grok-build" },
    ready: (event: OrchestrationV2DomainEvent) =>
      event.type === "turn-item.updated" &&
      event.payload.nativeItemRef?.nativeId?.endsWith("held-command") === true &&
      event.payload.status === "running",
  };
});

const makeOpenCodeBoundary = Effect.fn("makeOpenCode1UrgentRestartBoundary")(function* () {
  const observer = nativeRequests("session.promptAsync");
  const nativeEvents: ProviderAdapterV2Event[] = [];
  const nativeFailures: string[] = [];
  const original = OpenCodeReplayController.prototype.expectOutbound;
  const spy = vi
    .spyOn(OpenCodeReplayController.prototype, "expectOutbound")
    .mockImplementation(async function (this: OpenCodeReplayController, value: unknown) {
      await this.untilEventsDelivered();
      const frame = record(value);
      observer.capture({ method: String(frame.type), params: frame.input });
      return original.call(this, value);
    });
  yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
  const recorded = yield* fixture("opencode_subagent", "opencode_transcript.ndjson");
  const nativeThreadId = "ses_1235df8f8ffekByXP7gVnnXA3P";
  const childId = "ses_1235debc9ffe9MNk00UheV2j0q";
  const childUserIndex = recorded.entries.findIndex(
    (entry) => entry.type !== "runtime_exit" && entry.label === "child.user",
  );
  const prefix = recorded.entries.slice(0, childUserIndex).map((entry) => {
    const instant = immediate(entry);
    if (instant.type !== "expect_outbound" || record(instant.frame).type !== "session.promptAsync")
      return instant;
    const frame = record(instant.frame);
    return {
      ...instant,
      frame: {
        ...frame,
        input: {
          sessionID: nativeThreadId,
          messageID: record(frame.input).messageID,
          parts: "<any>",
        },
      },
    };
  });
  const captured = (label: string): ProviderReplayEntry => {
    const entry = recorded.entries.find(
      (entry) => entry.type !== "runtime_exit" && entry.label === label,
    );
    if (entry === undefined) throw new Error(`Missing OpenCode 1 frame: ${label}`);
    return immediate(entry);
  };
  const entries: ProviderReplayEntry[] = [
    ...prefix,
    // The task has attached its child thread, but no child user/turn exists yet.
    nativeEvent("session.status", { sessionID: childId, status: { type: "busy" } }, 1),
    outbound("session.abort", { sessionID: nativeThreadId }),
    response("session.abort", true),
    nativeEvent("session.status", { sessionID: nativeThreadId, status: { type: "idle" } }, 1),
    outbound("session.children", { sessionID: nativeThreadId }),
    response("session.children", [{ id: childId }]),
    outbound("session.abort", { sessionID: childId }),
    response("session.abort", true),
    outbound("session.children", { sessionID: childId }),
    response("session.children", []),
    captured("child.user"),
    captured("child.assistant"),
    nativeEvent(
      "message.part.updated",
      {
        sessionID: childId,
        part: {
          id: "child-held-tool",
          messageID: "msg_edca2143c002Eu46Ey4Vg2BbuM",
          sessionID: childId,
          type: "tool",
          tool: "bash",
          callID: "child-held-call",
          state: {
            status: "completed",
            input: { command: "held child command" },
            output: "CHILD_PARTIAL",
            title: "Held child",
            time: { start: 1781818070076, end: 1781818071615 },
          },
        },
        time: 1781818071615,
      },
      1,
    ),
    captured("child.idle"),
    // An idle echo must not end the same root twice.
    nativeEvent("session.idle", { sessionID: nativeThreadId }, 1),
    outbound("session.promptAsync", {
      sessionID: nativeThreadId,
      parts: "<any>",
      messageID: "replacement-native-message",
    }),
    response("session.promptAsync", null),
    nativeEvent("session.status", { sessionID: nativeThreadId, status: { type: "busy" } }, 1),
    outbound("session.abort", { sessionID: nativeThreadId }),
    response("session.abort", true),
    outbound("session.children", { sessionID: nativeThreadId }),
    response("session.children", []),
    { type: "runtime_exit", status: "success" },
  ];
  const registry = OpenCodeOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(
    yield* OpenCodeOrchestratorReplayHarness.decodeTranscript({ ...recorded, entries }),
  );
  // The replay registry constructs the actual 1.x adapter; observe it at openSession.
  const observed = Layer.unwrap(
    Effect.gen(function* () {
      const adapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const adapter = yield* adapters.get(ProviderInstanceId.make("opencode"));
      return observedRegistry(adapter, nativeEvents, () => Effect.void, nativeFailures);
    }),
  ).pipe(Layer.provide(registry));
  return {
    ...observer,
    nativeEvents,
    nativeFailures,
    nativeThreadId,
    childId,
    registry: observed,
    modelSelection: {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "openai/gpt-5.4-mini",
    },
    ready: (event: OrchestrationV2DomainEvent) =>
      event.type === "subagent.updated" &&
      event.payload.childThreadId !== null &&
      event.payload.status === "running",
  };
});

const makeOpenCode2Boundary = Effect.fn("makeOpenCode2UrgentStopBoundary")(function* (
  options: {
    readonly failure?: "http" | "timeout";
    readonly nested?: boolean;
    readonly holdChild?: boolean;
  } = {},
) {
  const observer = nativeRequests("session.prompt");
  const nativeEvents: ProviderAdapterV2Event[] = [];
  const nativeFailures: string[] = [];
  const nativeThreadId = "ses_f1485cda4ffeXuc6GjAU9vDiRb";
  const childId = "ses_f1485c529ffea4URrYruwEg0Ja";
  const nestedId = "ses_native_grandchild";
  const lateId = "ses_native_late_child";
  const input = yield* Queue.unbounded<unknown>();
  yield* Effect.addFinalizer(() => Queue.shutdown(input));
  const childInterruptEntered = yield* Deferred.make<void>();
  const releaseChildInterrupt = yield* Deferred.make<void>();
  let failChild = options.failure !== undefined;
  let prompts = 0;
  const recorded = yield* fixture("opencode2_background", "opencode_transcript.ndjson");
  const recordedResponse = (method: string) => {
    const entry = recorded.entries.find(
      (entry) => entry.type === "emit_inbound" && record(entry.frame).operation === method,
    );
    if (entry?.type !== "emit_inbound") throw new Error(`Missing OpenCode 2 response: ${method}`);
    return record(entry.frame).data;
  };
  let eventSequence = 0;
  const emit = (type: string, data: unknown) => {
    const sessionID = String(record(data).sessionID ?? nativeThreadId);
    return decodeOpenCodeEvent({
      id: `evt_urgent_${++eventSequence}`,
      created: 1790657051400,
      type,
      data: type === "session.execution.interrupted" ? { reason: "user", ...record(data) } : data,
      durable: {
        aggregateID: sessionID,
        seq: eventSequence,
        version: type === "session.tool.success" ? 2 : 1,
      },
    }).pipe(Effect.flatMap((frame) => Queue.offer(input, frame)));
  };
  const launchChild = Effect.fn("launchNativeOpenCodeChild")(function* (
    parent: string,
    child: string,
    call: string,
  ) {
    const tool = { sessionID: parent, assistantMessageID: `msg_assistant_${parent}`, id: call };
    yield* emit("session.tool.input.started", { ...tool, name: "subagent" });
    yield* emit("session.tool.called", {
      ...tool,
      input: {
        agent: "general",
        description: "Held native child",
        prompt: "Wait for cancellation.",
        background: true,
      },
      executed: false,
    });
    yield* emit("session.created", {
      sessionID: child,
      parentID: parent,
      title: "Held native child",
      agent: "general",
      slug: "held-native-child",
      version: "2.0.18",
      projectID: "global",
      location: { directory: "/work/opencode2" },
      subpath: "",
      model: { id: "big-pickle", providerID: "opencode", variant: "default" },
    });
    yield* emit("session.tool.progress", {
      ...tool,
      metadata: { sessionID: child, status: "running" },
    });
    yield* emit("session.execution.started", { sessionID: child });
    yield* emit("session.step.started", {
      sessionID: child,
      assistantMessageID: `msg_assistant_${child}`,
      agent: "general",
      model: { id: "big-pickle", providerID: "opencode", variant: "default" },
      started: 1790657051400,
    });
    yield* emit("session.tool.input.started", {
      sessionID: child,
      assistantMessageID: `msg_assistant_${child}`,
      id: `bash-${child}`,
      name: "bash",
    });
    yield* emit("session.tool.called", {
      sessionID: child,
      assistantMessageID: `msg_assistant_${child}`,
      id: `bash-${child}`,
      input: { command: "held child command" },
      executed: false,
    });
    yield* emit("session.text.delta", {
      sessionID: child,
      assistantMessageID: `msg_assistant_${child}`,
      ordinal: 0,
      delta: "Child is still working.",
    });
  });
  const permission = () =>
    emit("permission.asked", {
      id: "per_native_child",
      sessionID: childId,
      action: "shell",
      resources: ["held child command"],
      save: [],
      source: { type: "tool", messageID: `msg_assistant_${childId}`, id: `bash-${childId}` },
    });
  const question = () =>
    emit("form.created", {
      form: {
        id: "frm_native_child",
        sessionID: childId,
        title: "Questions",
        metadata: {
          kind: "question",
          tool: { messageID: `msg_assistant_${childId}`, id: `bash-${childId}` },
        },
        fields: [
          {
            key: "q0",
            title: "Continue?",
            description: "Keep waiting?",
            type: "string",
            options: [
              { value: "yes", label: "Yes" },
              { value: "no", label: "No" },
            ],
            custom: true,
          },
        ],
      },
    });
  const partial = () =>
    emit("session.tool.success", {
      sessionID: childId,
      assistantMessageID: `msg_assistant_${childId}`,
      id: `bash-${childId}`,
      content: [{ type: "text", text: "CHILD_PARTIAL" }],
      metadata: {},
      executed: false,
    });
  const lateFrames = Effect.gen(function* () {
    yield* partial();
    yield* emit("session.text.ended", {
      sessionID: childId,
      assistantMessageID: `msg_assistant_${childId}`,
      ordinal: 0,
      text: "LATE_OLD_CHILD_ANSWER",
    });
    yield* emit("session.execution.succeeded", { sessionID: childId });
    yield* emit("session.text.ended", {
      sessionID: nativeThreadId,
      assistantMessageID: "msg_replacement_marker",
      ordinal: 0,
      text: "OLD_FRAMES_DRAINED",
    });
  });
  const client = HttpClient.make((request, url) =>
    Effect.gen(function* () {
      const raw =
        request.body._tag === "Uint8Array"
          ? new TextDecoder().decode(request.body.body)
          : undefined;
      const body = raw === undefined ? {} : record(decodeJson(raw));
      const session = /^\/api\/session\/([^/]+)(\/.*)?$/.exec(url.pathname);
      const sessionID = session?.[1];
      const rest = session?.[2] ?? "";
      const reply = (value: unknown, status = 200) =>
        HttpClientResponse.fromWeb(
          request,
          value === null
            ? new Response(null, { status: 204 })
            : new Response(encodeJson(value), {
                status,
                headers: { "content-type": "application/json" },
              }),
        );
      if (request.method === "GET" && url.pathname === "/api/event") {
        const stream = yield* Stream.fromQueue(input).pipe(
          Stream.map((event) => new TextEncoder().encode(`data: ${encodeJson(event)}\n\n`)),
          (stream) => Stream.toReadableStreamEffect(stream),
        );
        observer.capture({ method: "event.subscribe", params: {} });
        return HttpClientResponse.fromWeb(
          request,
          new Response(stream, { headers: { "content-type": "text/event-stream" } }),
        );
      }
      if (request.method === "GET" && url.pathname === "/api/model")
        return reply(recordedResponse("model.list"));
      if (request.method === "POST" && url.pathname === "/api/session") {
        observer.capture({ method: "session.create", params: body });
        return reply(recordedResponse("session.create"));
      }
      if (request.method === "PUT" && url.pathname.includes("/instructions/entries/"))
        return reply(null);
      if (sessionID !== undefined && request.method === "PATCH" && rest === "") {
        observer.capture({ method: "session.update", params: { sessionID, ...body } });
        return reply(null);
      }
      if (sessionID !== undefined && request.method === "POST" && rest === "/prompt") {
        observer.capture({ method: "session.prompt", params: { sessionID, ...body } });
        prompts += 1;
        if (prompts === 1) {
          yield* emit("session.execution.started", { sessionID });
          yield* emit("session.step.started", {
            sessionID,
            assistantMessageID: `msg_assistant_${sessionID}`,
            agent: "build",
            model: { id: "big-pickle", providerID: "opencode", variant: "default" },
            started: 1790657051400,
          });
          yield* launchChild(nativeThreadId, childId, "call-background");
          if (options.nested) yield* launchChild(childId, nestedId, "call-nested-background");
          yield* permission();
          yield* question();
        } else yield* emit("session.execution.started", { sessionID });
        return reply({
          data: {
            id: `msg_native_${prompts}`,
            sessionID,
            type: "user",
            time: { created: 1790657051400 },
            payload: { text: String(body.text) },
            delivery: "steer",
          },
        });
      }
      if (sessionID !== undefined && request.method === "POST" && rest === "/interrupt") {
        observer.capture({ method: "session.interrupt", params: { sessionID } });
        if (sessionID === childId) {
          yield* Deferred.succeed(childInterruptEntered, undefined);
          if (options.holdChild) yield* Deferred.await(releaseChildInterrupt);
          if (failChild) {
            if (options.failure === "timeout") return yield* Effect.never;
            return reply({ _tag: "UnknownError", message: "interrupt failed" }, 500);
          }
        }
        // Native root interruption may arrive before cleanup. The real adapter
        // owns whether its public terminal can be emitted yet.
        yield* emit("session.execution.interrupted", { sessionID });
        return reply({ interrupted: true });
      }
      if (
        sessionID !== undefined &&
        (rest.includes("/permission/") || rest.includes("/form/") || rest === "/synthetic")
      ) {
        observer.capture({
          method: rest.includes("/permission/")
            ? "permission.reply"
            : rest.includes("/form/")
              ? "session.form.cancel"
              : "session.synthetic",
          params: { sessionID, ...body },
        });
        return reply(null);
      }
      return yield* Effect.die(
        `Unexpected native OpenCode 2 HTTP request: ${request.method} ${url.pathname}`,
      );
    }),
  );
  const registry = Layer.unwrap(
    Effect.gen(function* () {
      const native = yield* OpenCode2Client.make.pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      );
      const connection = {
        ...(yield* native.connect({
          baseUrl: "http://urgent-opencode2.replay",
          password: "replay",
        })),
        url: "http://urgent-opencode2.replay",
        version: "2.0.18",
        external: true,
      };
      const server = OpenCode2Server.OpenCode2Server.of({
        withConnection: (use) => use(connection),
      });
      const adapter = yield* OpenCode2Adapter.make(ProviderInstanceId.make("opencode")).pipe(
        Effect.provideService(OpenCode2Server.OpenCode2Server, server),
      );
      return observedRegistry(adapter, nativeEvents, () => Effect.void, nativeFailures);
    }),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        NodeServices.layer,
        TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
      ),
    ),
  );
  return {
    ...observer,
    nativeEvents,
    nativeFailures,
    nativeThreadId,
    childId,
    nestedId,
    lateId,
    registry,
    modelSelection: {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "opencode/big-pickle",
    },
    childInterruptEntered,
    releaseChildInterrupt,
    partial,
    lateFrames,
    launchChild,
    emit,
    allowChildStop: () =>
      Effect.sync(() => {
        failChild = false;
      }),
    ready: (event: OrchestrationV2DomainEvent) =>
      event.type === "runtime-request.updated" &&
      event.payload.nativeRequestRef?.nativeId?.endsWith("frm_native_child") === true &&
      event.payload.status === "pending",
  };
});

const provideTestServices = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);
const domainIndex = (
  events: OrchestrationV2DomainEvent[],
  predicate: (event: OrchestrationV2DomainEvent) => boolean,
) => {
  const index = events.findIndex(predicate);
  assert.isAtLeast(index, 0, "Expected persisted event is absent.");
  return index;
};
const stoppedChild = (events: OrchestrationV2DomainEvent[], id: string) => {
  const updates = events.filter(
    (event) =>
      event.type === "subagent.updated" &&
      event.payload.id === id &&
      event.payload.status === "interrupted",
  );
  assert.lengthOf(updates, 1, "A native child must end exactly once, with its original identity.");
};
const promptRequests = (boundary: Pick<NativeBoundary, "requests">, method: string) =>
  boundary.requests.filter((request) => request.method === method);
const watchReplacement = (scenario: Scenario, before: OrchestrationV2ThreadProjection) =>
  scenario.watch(
    (event) =>
      event.type === "provider-turn.updated" &&
      event.threadId === threadId &&
      event.payload.runAttemptId !==
        before.providerTurns.findLast((turn) => turn.status === "running")?.runAttemptId &&
      event.payload.status === "running",
  );

const codexRestart = (delivery: "restart" | "human", child = false) =>
  makeCodexBoundary({ child }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const originalChild = before.subagents.find((row) => row.origin === "provider_native");
          const childBefore =
            originalChild?.childThreadId === null || originalChild?.childThreadId === undefined
              ? undefined
              : yield* scenario.orchestrator.getThreadProjection(originalChild.childThreadId);
          const running = yield* watchReplacement(scenario, before);
          yield* scenario.send(delivery);
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Effect.raceFirst(
            Deferred.await(boundary.containment.entered),
            Deferred.await(boundary.containment.steerEntered),
          );
          assert.notInclude(
            boundary.requests.map((request) => request.method),
            "turn/steer",
          );
          assert.lengthOf(
            promptRequests(boundary, "turn/start"),
            1,
            "Replacement crossed the held foreground containment reply.",
          );
          yield* Deferred.succeed(boundary.containment.release, undefined);
          yield* Fiber.join(draining);
          yield* Fiber.join(running);
          const prompts = promptRequests(boundary, "turn/start");
          assert.lengthOf(prompts, 2);
          yield* Deferred.await(boundary.promptSent(2));
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, prompts[1]!, delivery);
          assert.deepEqual(
            boundary.requests.find(
              (request) => request.method === "thread/backgroundTerminals/terminate",
            )?.params,
            { threadId: boundary.nativeThreadId, processId: "99" },
          );
          assert.isBelow(
            boundary.requests.findIndex(
              (request) => request.method === "thread/backgroundTerminals/terminate",
            ),
            boundary.requests.indexOf(prompts[1]!),
          );
          const foreground = after.turnItems.find(
            (item) => item.nativeItemRef?.nativeId === "held-command",
          );
          assert.equal(foreground?.status, "interrupted");
          if (child) {
            assert.isDefined(originalChild);
            assert.isDefined(childBefore);
            const childAfter = yield* scenario.orchestrator.getThreadProjection(
              originalChild!.childThreadId!,
            );
            const tool = childAfter.turnItems.find(
              (item) => item.nativeItemRef?.nativeId === "child-held-command",
            );
            assert.equal(tool?.status, "completed");
            assert.equal(
              tool?.type === "command_execution" ? tool.output : undefined,
              "CHILD_PARTIAL",
            );
            assert.equal(childAfter.providerTurns[0]?.id, childBefore!.providerTurns[0]?.id);
            assert.equal(childAfter.providerTurns[0]?.status, "interrupted");
            assert.equal(
              childAfter.nodes.find((node) => node.kind === "root_turn")?.status,
              "interrupted",
            );
            stoppedChild(scenario.events, originalChild!.id);
            const childTail = domainIndex(
              scenario.events,
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === tool?.id &&
                event.payload.status === "completed",
            );
            const replacement = domainIndex(
              scenario.events,
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.runAttemptId === after.attempts.at(-1)?.id &&
                event.payload.status === "running",
            );
            assert.isBelow(childTail, replacement);
            const adapterTail = boundary.nativeEvents.findIndex(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.id === tool?.id &&
                event.turnItem.status === "completed",
            );
            const oldTerminal = boundary.nativeEvents.findIndex(
              (event) =>
                event.type === "turn.terminal" &&
                event.providerTurnId === before.providerTurns[0]?.id,
            );
            assert.isAtLeast(adapterTail, 0);
            assert.isAbove(
              oldTerminal,
              adapterTail,
              "Codex must defer its root terminal behind the known child's actual tool tail.",
            );
            assert.lengthOf(
              scenario.events.filter(
                (event) =>
                  event.type === "turn-item.updated" &&
                  event.payload.id === tool?.id &&
                  event.payload.status === "completed",
              ),
              1,
            );
          }
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const codexIncompleteStop = (
  delivery: "restart" | "human",
  finish: "retry" | "natural" | "stop" | "stop-before-retry",
  rootEnd?: "completed" | "failed",
) =>
  makeCodexBoundary({
    child: true,
    childInterruptFailures: finish === "retry" ? 1 : Number.POSITIVE_INFINITY,
  }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(boundary.containment.release, undefined);
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const oldTurn = before.providerTurns.find((turn) => turn.status === "running")!;
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          assert.isDefined(child);
          yield* scenario.send(delivery);
          if (rootEnd !== undefined) {
            const ended = yield* scenario.watch(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.id === oldTurn.id &&
                event.payload.status === rootEnd,
            );
            yield* boundary.endRoot(rootEnd);
            yield* Fiber.join(ended);
          }
          yield* scenario.worker.drain();
          assert.lengthOf(
            promptRequests(boundary, "turn/start"),
            1,
            "A child that rejected native cancellation must block replacement delivery.",
          );
          const effects = yield* scenario.outbox.listByCommandId(CommandId.make("urgent"));
          assert.equal(
            effects.find((effect) => effect.request.type === "provider-turn.restart")?.status,
            "pending",
          );
          const requestsTo = (nativeId: string) =>
            boundary.requests.filter(
              (request) =>
                request.method === "turn/interrupt" && record(request.params).threadId === nativeId,
            );
          assert.lengthOf(requestsTo(boundary.childId), 1);
          const held = yield* scenario.orchestrator.getThreadProjection(threadId);
          if (rootEnd === undefined)
            assert.equal(
              held.providerTurns.find((turn) => turn.id === oldTurn.id)?.status,
              "running",
              "Retry and Stop must retain the root while native child cancellation is unconfirmed.",
            );
          assert.equal(
            (yield* scenario.orchestrator.getThreadProjection(child.childThreadId!))
              .providerTurns[0]?.status,
            "running",
          );
          assert.isFalse(
            boundary.nativeEvents.some(
              (event) => event.type === "turn.terminal" && event.providerTurnId === oldTurn.id,
            ) && rootEnd !== "completed",
          );

          if (finish === "stop-before-retry") {
            const childEnded = yield* scenario.watch(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            );
            yield* boundary.allowChildStop();
            yield* scenario.orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("stop-completed-cut-in"),
              threadId,
              runId: before.runs[0]!.id,
              holdQueue: true,
            });
            yield* scenario.worker.drain();
            assert.lengthOf(
              requestsTo(boundary.childId),
              2,
              "The first Stop must reach the held child even when the old root completed before the cut-in effect.",
            );
            yield* Fiber.join(childEnded);
            assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
            assert.equal(
              (yield* scenario.outbox.listByCommandId(CommandId.make("urgent"))).find(
                (effect) => effect.request.type === "provider-turn.restart",
              )?.status,
              "cancelled",
            );
            yield* TestClock.adjust("1 second");
            yield* scenario.worker.drain();
            assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
            return;
          }

          if (finish === "retry") {
            const replacement = yield* watchReplacement(scenario, before);
            yield* TestClock.adjust("100 millis");
            yield* scenario.worker.drain();
            yield* Fiber.join(replacement);
            const after = yield* scenario.orchestrator.getThreadProjection(threadId);
            if (rootEnd === undefined)
              assertReplacement(
                before,
                after,
                promptRequests(boundary, "turn/start")[1]!,
                delivery,
              );
            assert.lengthOf(promptRequests(boundary, "turn/start"), 2);
            assert.lengthOf(requestsTo(boundary.nativeThreadId), rootEnd === undefined ? 1 : 0);
            assert.lengthOf(requestsTo(boundary.childId), 2);
            const childAfter = yield* scenario.orchestrator.getThreadProjection(
              child.childThreadId!,
            );
            const tool = childAfter.turnItems.find(
              (item) => item.nativeItemRef?.nativeId === "child-held-command",
            );
            assert.equal(tool?.status, "completed");
            assert.equal(
              tool?.type === "command_execution" ? tool.output : undefined,
              "CHILD_PARTIAL",
            );
            stoppedChild(scenario.events, child.id);
            const childEnded = domainIndex(
              scenario.events,
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            );
            const replacementStarted = domainIndex(
              scenario.events,
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.runAttemptId === after.attempts.at(-1)?.id &&
                event.payload.status === "running",
            );
            assert.isBelow(childEnded, replacementStarted);
            return;
          }

          for (const delay of [100, 200, 400, 800]) {
            yield* TestClock.adjust(`${delay} millis`);
            yield* scenario.worker.drain();
          }
          const failed = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.equal(failed.runs.find((run) => run.id === before.runs[0]?.id)?.status, "failed");
          assert.isTrue(
            failed.turnItems.some(
              (item) => item.type === "error" && item.title === "Message not delivered",
            ),
          );
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          assert.lengthOf(requestsTo(boundary.nativeThreadId), 1);
          assert.lengthOf(requestsTo(boundary.childId), 5);
          const rootEnded = yield* scenario.watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === oldTurn.id &&
              event.payload.status === "interrupted",
          );
          if (finish === "natural") {
            yield* boundary.completeRootCommand();
            yield* boundary.completeChild("completed");
          } else {
            yield* boundary.allowChildStop();
            yield* scenario.orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("stop-after-rejected-child"),
              threadId,
              runId: before.runs[0]!.id,
              holdQueue: true,
            });
            yield* scenario.worker.drain();
          }
          yield* Fiber.join(rootEnded);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          assert.lengthOf(requestsTo(boundary.childId), finish === "natural" ? 5 : 6);
          assert.lengthOf(
            scenario.events.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.id === oldTurn.id &&
                event.payload.status === "interrupted",
            ),
            1,
          );
          const finalChild = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          assert.equal(
            finalChild.providerTurns[0]?.status,
            finish === "natural" ? "completed" : "interrupted",
          );
          assert.equal(
            after.subagents.find((row) => row.id === child.id)?.status,
            finish === "natural" ? "completed" : "interrupted",
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const codexInstanceStop = (
  transition: "replace" | "detach",
  phase: "before" | "retry" | "failed" | "cancel" | "delivered",
  delivery: "human" | "restart" | "auto",
) =>
  Effect.gen(function* () {
    const original = yield* makeCodexBoundary({
      child: true,
      childInterruptFailures: phase === "delivered" ? 0 : Infinity,
    });
    const target = yield* makeCodexBoundary({ instanceId: ProviderInstanceId.make("codex-next") });
    const adapters = [original.adapter, target.adapter];
    const get = Effect.fnUntraced(function* (instanceId: ProviderInstanceId) {
      const adapter = adapters.find((adapter) => adapter.instanceId === instanceId);
      if (adapter === undefined)
        return yield* new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({
          instanceId,
        });
      return adapter;
    });
    // Metadata is the external instance configuration boundary. The actual
    // switch policy and both Codex adapters choose and execute the transition.
    const registry = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistryV2,
      ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
        get,
        list: () => Effect.succeed(adapters.map((adapter) => adapter.instanceId)),
        getMetadata: (instanceId) =>
          get(instanceId).pipe(
            Effect.flatMap((adapter) =>
              adapter.getCapabilities().pipe(
                Effect.map((capabilities) => ({
                  driver: adapter.driver,
                  continuationKey:
                    transition === "replace" ? "shared-codex-home" : String(instanceId),
                  enabled: true,
                  capabilities,
                })),
              ),
            ),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRegistry.ProviderAdapterRegistryMetadataError({
                  instanceId,
                  cause,
                }),
            ),
          ),
      }),
    );
    yield* withScenario({ ...original, registry }, (scenario) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(original.containment.release, undefined);
        yield* Deferred.succeed(target.containment.release, undefined);
        const before = yield* scenario.orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const oldTurn = before.providerTurns.find((turn) => turn.status === "running")!;
        const oldThread = before.providerThreads.find(
          (thread) => thread.id === oldTurn.providerThreadId,
        )!;
        const child = before.subagents.find((row) => row.origin === "provider_native")!;
        if (delivery === "human") {
          yield* scenario.orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("instance-cut-in"),
            threadId,
            messageId: MessageId.make("instance-cut-in"),
            text: urgentText,
            attachments: [],
            modelSelection: target.modelSelection,
            dispatchMode: { type: "steer_active", targetRunId: run.id },
            deliveryIntent: "steer",
            createdBy: "user",
            creationSource: "web",
          });
        } else {
          yield* scenario.management.sendToThread({
            commandId: CommandId.make("instance-cut-in"),
            threadId,
            projectId,
            messageId: MessageId.make("instance-cut-in"),
            text: urgentText,
            attachments: [],
            modelSelection: target.modelSelection,
            mode: delivery,
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId,
          });
        }
        const restart = (yield* scenario.outbox.listByCommandId(
          CommandId.make("instance-cut-in"),
        )).find((effect) => effect.request.type === "provider-turn.restart");
        assert.isDefined(restart);
        if (restart?.request.type !== "provider-turn.restart")
          return assert.fail("Missing restart capture");
        assert.equal(restart.request.interruption, delivery === "auto" ? undefined : "stop");
        assert.equal(restart.request.providerSessionId, oldThread.providerSessionId);
        assert.equal(restart.request.providerThreadId, oldTurn.providerThreadId);
        assert.equal(restart.request.sessionTransition?.type, transition);
        const rebound = yield* scenario.orchestrator.getThreadProjection(threadId);
        const nextThread = rebound.providerThreads.find(
          (thread) => thread.id === rebound.runs[0]?.providerThreadId,
        )!;
        if (transition === "replace") {
          assert.equal(nextThread.id, oldThread.id);
          assert.notEqual(nextThread.providerSessionId, oldThread.providerSessionId);
          assert.equal(
            restart.request.sessionTransition?.type === "replace" &&
              restart.request.sessionTransition.replacementProviderSessionId,
            nextThread.providerSessionId,
          );
        } else assert.notEqual(nextThread.id, oldThread.id);
        const delivered =
          phase === "delivered"
            ? yield* scenario.watch(
                (event) =>
                  event.type === "provider-turn.updated" &&
                  event.payload.runAttemptId === rebound.runs[0]?.activeAttemptId &&
                  event.payload.status === "running",
              )
            : undefined;
        if (phase === "retry" || phase === "failed" || phase === "delivered") {
          yield* scenario.worker.drain();
          if (delivered !== undefined) yield* Fiber.join(delivered);
          if (phase === "failed")
            for (const delay of [100, 200, 400, 800]) {
              yield* TestClock.adjust(`${delay} millis`);
              yield* scenario.worker.drain();
            }
          assert.equal(
            (yield* scenario.orchestrator.getThreadProjection(threadId)).runs[0]?.status,
            phase === "failed" ? "failed" : phase === "delivered" ? "running" : "starting",
          );
        }
        const interruptsToChild = () =>
          original.requests.filter(
            (request) =>
              request.method === "turn/interrupt" &&
              record(request.params).threadId === original.childId,
          );
        const previousChildInterrupts = interruptsToChild().length;
        if (phase === "cancel") {
          yield* scenario.outbox.cancelUnsettled({
            threadId,
            effectTypes: ["provider-turn.restart"],
            reason: "Cancel without requesting Stop",
          });
          yield* scenario.worker.drain();
          assert.lengthOf(
            original.requests.filter((request) => request.method === "turn/interrupt"),
            0,
          );
          assert.lengthOf(target.requests, 0);
          assert.equal(
            (yield* scenario.orchestrator.getThreadProjection(child.childThreadId!))
              .providerTurns[0]?.status,
            "running",
          );
          return;
        }
        yield* original.allowChildStop();
        const childEnded =
          phase === "delivered"
            ? undefined
            : yield* scenario.watch(
                (event) =>
                  event.type === "subagent.updated" &&
                  event.payload.id === child.id &&
                  event.payload.status === "interrupted",
              );
        yield* scenario.orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop-instance-cut-in"),
          threadId,
          runId: run.id,
          holdQueue: true,
        });
        const stop = (yield* scenario.outbox.listByCommandId(
          CommandId.make("stop-instance-cut-in"),
        )).filter((effect) => effect.request.type === "provider-turn.interrupt");
        assert.lengthOf(stop, 1, "Stop must not duplicate the handoff's old-thread interrupt");
        const request = stop[0]?.request;
        if (request?.type !== "provider-turn.interrupt") return assert.fail("Missing Stop capture");
        assert.equal(
          request.providerSessionId,
          phase === "delivered" ? nextThread.providerSessionId : oldThread.providerSessionId,
        );
        assert.equal(
          request.providerThreadId,
          phase === "delivered" ? nextThread.id : oldThread.id,
        );
        assert.equal(
          request.replacementProviderSessionId,
          transition === "replace" && phase !== "delivered"
            ? nextThread.providerSessionId
            : undefined,
        );
        if (phase !== "delivered") assert.equal(request.providerTurnId, oldTurn.id);
        yield* scenario.worker.drain();
        if (childEnded !== undefined) yield* Fiber.join(childEnded);
        if (phase === "delivered") {
          assert.lengthOf(interruptsToChild(), previousChildInterrupts);
          assert.lengthOf(promptRequests(target, "turn/start"), 1);
          assert.isTrue(target.requests.some((request) => request.method === "turn/interrupt"));
        } else {
          assert.lengthOf(interruptsToChild(), previousChildInterrupts + 1);
          assert.lengthOf(
            target.requests,
            0,
            "An unstarted replacement runtime cannot receive Stop",
          );
          assert.lengthOf(promptRequests(original, "turn/start"), 1);
          const current = Option.getOrUndefined(yield* scenario.outbox.get(restart.id));
          assert.equal(current?.status, phase === "failed" ? "failed" : "cancelled");
          yield* TestClock.adjust("1 second");
          yield* scenario.worker.drain();
          assert.lengthOf(target.requests, 0);
        }
        const after = yield* scenario.orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs[0]?.status, phase === "failed" ? "failed" : "interrupted");
        if (phase === "failed")
          assert.isTrue(
            after.turnItems.some(
              (item) => item.type === "error" && item.title === "Message not delivered",
            ),
          );
        assert.lengthOf(
          scenario.events.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === oldTurn.id &&
              event.payload.status === "interrupted",
          ),
          1,
        );
      }),
    );
  }).pipe(Effect.scoped, Effect.provide(provideTestServices));

const codexOverlappingHeldRoots = () =>
  makeCodexBoundary({ child: true, overlap: true, childInterruptFailures: Infinity }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(boundary.containment.release, undefined);
          const first = yield* scenario.orchestrator.getThreadProjection(threadId);
          const firstChild = first.subagents.find((row) => row.origin === "provider_native")!;
          yield* scenario.send("human");
          yield* scenario.worker.drain();
          for (const delay of [100, 200, 400, 800]) {
            yield* TestClock.adjust(`${delay} millis`);
            yield* scenario.worker.drain();
          }
          assert.equal(
            (yield* scenario.orchestrator.getThreadProjection(threadId)).runs[0]?.status,
            "failed",
          );
          const nextChildReady = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.nativeItemRef?.nativeId === "next-child-command" &&
              event.payload.status === "running",
          );
          yield* scenario.management.sendToThread({
            commandId: CommandId.make("next-root"),
            threadId,
            projectId,
            messageId: MessageId.make("next-root"),
            text: "Start a new run while the previous child is still held.",
            attachments: [],
            mode: "auto",
            createdBy: "user",
            creationSource: "web",
          });
          yield* scenario.worker.drain();
          yield* Fiber.join(nextChildReady);
          const second = yield* scenario.orchestrator.getThreadProjection(threadId);
          const secondTurn = second.providerTurns.findLast((turn) => turn.status === "running")!;
          const secondChild = second.subagents.find(
            (row) => row.origin === "provider_native" && row.id !== firstChild.id,
          )!;
          assert.isDefined(secondChild);
          yield* scenario.send("human", "second-urgent");
          const ended = yield* scenario.watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === secondTurn.id &&
              event.payload.status === "completed",
          );
          yield* boundary.endRoot("completed", 2);
          yield* Fiber.join(ended);
          yield* scenario.worker.drain();
          assert.lengthOf(
            promptRequests(boundary, "turn/start"),
            2,
            "The newer completed root must join the older unconfirmed cohort before delivery.",
          );
          const effects = yield* scenario.outbox.listByCommandId(CommandId.make("second-urgent"));
          assert.equal(
            effects.find((effect) => effect.request.type === "provider-turn.restart")?.status,
            "pending",
          );
          yield* boundary.allowChildStop();
          const replacement = yield* watchReplacement(scenario, second);
          yield* TestClock.adjust("100 millis");
          yield* scenario.worker.drain();
          yield* Fiber.join(replacement);
          assert.lengthOf(promptRequests(boundary, "turn/start"), 3);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.equal(
            after.providerTurns.find((turn) => turn.id === secondTurn.id)?.status,
            "completed",
          );
          const replacementStarted = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === after.attempts.at(-1)?.id &&
              event.payload.status === "running",
          );
          for (const child of [firstChild, secondChild]) {
            assert.equal(after.subagents.find((row) => row.id === child.id)?.status, "interrupted");
            assert.isBelow(
              domainIndex(
                scenario.events,
                (event) =>
                  event.type === "subagent.updated" &&
                  event.payload.id === child.id &&
                  event.payload.status === "interrupted",
              ),
              replacementStarted,
            );
          }
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const codexLateReaderProgress = (outcome: "ack" | "reject" | "silent") =>
  makeCodexBoundary({ child: true, holdLateChild: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const firstChild = before.subagents.find((row) => row.origin === "provider_native")!;
          const childEnded = yield* scenario.watch(
            (event) =>
              event.type === "subagent.updated" &&
              event.payload.id === firstChild.id &&
              event.payload.status === "interrupted",
          );
          yield* scenario.send("human");
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Fiber.join(childEnded);
          assert.equal(
            (yield* scenario.orchestrator.getThreadProjection(threadId)).providerTurns.find(
              (turn) => turn.runAttemptId === before.attempts[0]?.id,
            )?.status,
            "running",
            "The cohort owner must retain the root before testing late-reader progress.",
          );
          yield* Deferred.await(boundary.containment.entered);
          const sentinel = yield* scenario.watch(
            (event) =>
              event.type === "message.updated" && event.payload.text === "LATE_READER_PROGRESS",
          );
          yield* boundary.spawnLateChild();
          yield* boundary.lateChildMessage();
          const first = yield* Effect.raceFirst(
            Fiber.join(sentinel).pipe(Effect.as("persisted sentinel")),
            Deferred.await(boundary.childInterruptEntered).pipe(Effect.as("native interrupt")),
          );
          assert.equal(
            first,
            "persisted sentinel",
            "The serial reader must persist the late child's output before awaiting its interrupt reply.",
          );
          yield* Deferred.succeed(boundary.containment.release, undefined);
          yield* Deferred.await(boundary.childInterruptEntered);
          const late = yield* scenario.orchestrator.getThreadProjection(firstChild.childThreadId!);
          assert.equal(
            late.providerTurns.find((turn) => turn.nativeTurnRef?.nativeId === "late-child-turn")
              ?.status,
            "running",
            "The known child's new turn remains observable while cancellation is unconfirmed.",
          );
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          yield* boundary.answerHeldChildInterrupt(outcome);
          if (outcome === "silent") yield* TestClock.adjust("10 seconds");
          yield* Fiber.join(draining);
          if (outcome !== "ack") {
            assert.lengthOf(
              promptRequests(boundary, "turn/start"),
              1,
              "Unconfirmed late child cancellation cannot deliver a replacement.",
            );
            const effects = yield* scenario.outbox.listByCommandId(CommandId.make("urgent"));
            assert.equal(
              effects.find((effect) => effect.request.type === "provider-turn.restart")?.status,
              "pending",
            );
            yield* TestClock.adjust("100 millis");
            yield* scenario.worker.drain();
          }
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.lengthOf(promptRequests(boundary, "turn/start"), 2);
          assert.lengthOf(
            scenario.events.filter(
              (event) =>
                event.type === "message.updated" && event.payload.text === "LATE_READER_PROGRESS",
            ),
            1,
          );
          assert.equal(
            after.subagents.filter(
              (row) => row.origin === "provider_native" && row.status === "interrupted",
            ).length,
            1,
          );
          assert.isFalse(
            after.providerTurns.some(
              (turn) => turn.runAttemptId === before.attempts[0]?.id && turn.status === "running",
            ),
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const grokRestart = (delivery: Delivery, carryover = false) =>
  makeGrokBoundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const first = yield* scenario.orchestrator.getThreadProjection(threadId);
          const originalChild = first.subagents.find((child) => child.origin === "provider_native");
          assert.isDefined(
            originalChild,
            "The real Grok adapter must admit the native background child before interruption.",
          );
          assert.equal(originalChild?.status, "running");
          assert.isTrue(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
          let before = first;
          let promptOrdinal = 2;
          if (carryover) {
            const running = yield* watchReplacement(scenario, first);
            yield* scenario.send("auto", "routine", "Keep working; this is routine.");
            yield* scenario.worker.drain();
            yield* Fiber.join(running);
            yield* Deferred.await(boundary.promptSent(2));
            before = yield* scenario.orchestrator.getThreadProjection(threadId);
            assert.equal(
              before.subagents.find((row) => row.id === originalChild!.id)?.status,
              "running",
            );
            assert.lengthOf(boundary.processes, 1);
            assert.lengthOf(
              boundary.requests.filter((request) => request.method === "process-group/kill"),
              0,
            );
            assert.deepEqual(
              boundary.requests.find((request) => request.method === "session/cancel")?.params,
              { sessionId: boundary.nativeThreadId, _meta: { cancelTrigger: "ctrl_c" } },
            );
            promptOrdinal = 3;
          }
          const running = yield* watchReplacement(scenario, before);
          yield* scenario.send(delivery);
          yield* scenario.worker.drain();
          yield* Fiber.join(running);
          yield* Deferred.await(boundary.promptSent(promptOrdinal));
          const prompts = promptRequests(boundary, "session/prompt");
          assert.lengthOf(prompts, promptOrdinal);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, prompts.at(-1)!, delivery);
          const hard = delivery === "restart" || delivery === "human" || delivery === "mobile";
          if (!hard) {
            assert.lengthOf(
              boundary.processes,
              1,
              "Routine steering must keep the same ACP runtime.",
            );
            assert.lengthOf(
              boundary.requests.filter((request) => request.method === "process-group/kill"),
              0,
            );
            assert.deepEqual(
              boundary.requests.find((request) => request.method === "session/cancel")?.params,
              { sessionId: boundary.nativeThreadId, _meta: { cancelTrigger: "ctrl_c" } },
            );
            assert.equal(
              after.subagents.find((row) => row.id === originalChild!.id)?.status,
              "running",
            );
            assert.isTrue(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
            return;
          }
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "process-group/kill"),
            1,
            "An explicit message must use Grok's owned-process Stop path.",
          );
          assert.lengthOf(
            boundary.processes,
            2,
            "The urgent prompt must run on a respawned ACP runtime.",
          );
          yield* Deferred.await(boundary.processes[0]!.exited);
          assert.notEqual(boundary.processes[0]!.pid, boundary.processes[1]!.pid);
          const load = boundary.requests.find((request) => request.method === "session/load");
          assert.equal(record(load?.params).sessionId, boundary.nativeThreadId);
          assert.isBelow(
            boundary.requests.indexOf(load!),
            boundary.requests.indexOf(prompts.at(-1)!),
          );
          assert.include(boundary.loadedHistory, "Start the original action.");
          assert.lengthOf(
            after.messages.filter(
              (message) =>
                message.id === MessageId.make("first") &&
                message.text === "Start the original action.",
            ),
            1,
          );
          stoppedChild(scenario.events, originalChild!.id);
          const child = after.subagents.find((row) => row.id === originalChild!.id)!;
          assert.equal(child.runId, originalChild!.runId);
          assert.equal(child.providerThreadId, originalChild!.providerThreadId);
          assert.equal(child.nativeTaskRef?.nativeId, originalChild!.nativeTaskRef?.nativeId);
          assert.equal(child.status, "interrupted");
          const ended = domainIndex(
            scenario.events,
            (event) =>
              event.type === "subagent.updated" &&
              event.payload.id === originalChild!.id &&
              event.payload.status === "interrupted",
          );
          const replacement = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === after.attempts.at(-1)?.id &&
              event.payload.status === "running",
          );
          assert.isBelow(ended, replacement);
          const monitor = after.turnItems.find((item) =>
            item.nativeItemRef?.nativeId?.endsWith("monitor-call"),
          );
          assert.isDefined(monitor);
          assert.equal(monitor?.status, "interrupted");
          const originalMonitor = first.turnItems.find((item) =>
            item.nativeItemRef?.nativeId?.endsWith("monitor-call"),
          )!;
          assert.equal(monitor?.id, originalMonitor.id);
          assert.equal(monitor?.runId, originalMonitor.runId);
          assert.equal(monitor?.providerTurnId, originalMonitor.providerTurnId);
          const monitorEnd = (event: OrchestrationV2DomainEvent) =>
            event.type === "turn-item.updated" &&
            event.payload.id === originalMonitor.id &&
            event.payload.status === "interrupted";
          assert.lengthOf(scenario.events.filter(monitorEnd), 1);
          assert.isBelow(domainIndex(scenario.events, monitorEnd), replacement);
          if (carryover) {
            const againRunning = yield* watchReplacement(scenario, after);
            yield* scenario.send("human", "urgent-again", "Use recipient list C instead.");
            yield* scenario.worker.drain();
            yield* Fiber.join(againRunning);
            yield* Deferred.await(boundary.promptSent(4));
            assert.lengthOf(promptRequests(boundary, "session/prompt"), 4);
            stoppedChild(scenario.events, originalChild!.id);
            assert.lengthOf(scenario.events.filter(monitorEnd), 1);
            const again = yield* scenario.orchestrator.getThreadProjection(threadId);
            assertReplacement(
              after,
              again,
              promptRequests(boundary, "session/prompt").at(-1)!,
              "human",
              "urgent-again",
              "Use recipient list C instead.",
            );
          }
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const grokExistingStop = () =>
  makeGrokBoundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          const oldTurn = before.providerTurns.find((turn) => turn.status === "running")!;
          const ended = yield* scenario.watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === oldTurn.id &&
              event.payload.status === "interrupted",
          );
          yield* scenario.management.interruptThread({
            projectId,
            threadId,
            commandId: CommandId.make("ordinary-stop"),
          });
          yield* scenario.worker.drain();
          yield* Fiber.join(ended);
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "process-group/kill"),
            1,
          );
          yield* Deferred.await(boundary.processes[0]!.exited);
          stoppedChild(scenario.events, child.id);
          const monitor = before.turnItems.find((item) =>
            item.nativeItemRef?.nativeId?.endsWith("monitor-call"),
          )!;
          assert.equal(monitor.status, "running");
          const monitorEnd = (event: OrchestrationV2DomainEvent) =>
            event.type === "turn-item.updated" &&
            event.payload.id === monitor.id &&
            event.payload.status === "interrupted";
          assert.lengthOf(scenario.events.filter(monitorEnd), 1);
          const nativeMonitorEnd = boundary.nativeEvents.findIndex(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.id === monitor.id &&
              event.turnItem.status === "interrupted",
          );
          const oldTerminal = boundary.nativeEvents.findIndex(
            (event) => event.type === "turn.terminal" && event.providerTurnId === oldTurn.id,
          );
          assert.isAtLeast(nativeMonitorEnd, 0);
          assert.isBelow(nativeMonitorEnd, oldTerminal);
          const next = yield* scenario.watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id !== oldTurn.id &&
              event.threadId === threadId &&
              event.payload.status === "running",
          );
          yield* scenario.management.sendToThread({
            projectId,
            threadId,
            commandId: CommandId.make("after-stop"),
            messageId: MessageId.make("after-stop"),
            text: "Continue after Stop.",
            attachments: [],
            mode: "auto",
            createdBy: "user",
            creationSource: "web",
          });
          yield* scenario.worker.drain();
          yield* Fiber.join(next);
          yield* Deferred.await(boundary.promptSent(2));
          assert.lengthOf(boundary.processes, 2);
          const load = boundary.requests.find((request) => request.method === "session/load");
          assert.equal(record(load?.params).sessionId, boundary.nativeThreadId);
          assert.include(boundary.loadedHistory, "Start the original action.");
          assert.isBelow(
            boundary.requests.indexOf(load!),
            boundary.requests.indexOf(promptRequests(boundary, "session/prompt")[1]!),
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const openCode1Restart = (delivery: "restart" | "human") =>
  makeOpenCodeBoundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((child) => child.origin === "provider_native")!;
          assert.isDefined(child);
          const childBefore = yield* scenario.orchestrator.getThreadProjection(
            child.childThreadId!,
          );
          assert.lengthOf(
            childBefore.providerTurns,
            0,
            "The attached/busy child must have no active turn when root abort begins.",
          );
          const running = yield* watchReplacement(scenario, before);
          yield* scenario.send(delivery);
          yield* scenario.worker.drain();
          yield* Fiber.join(running);
          const prompts = promptRequests(boundary, "session.promptAsync");
          assert.lengthOf(prompts, 2);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, prompts[1]!, delivery);
          const childAfter = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          const tool = childAfter.turnItems.find(
            (item) => item.nativeItemRef?.nativeId === "child-held-tool",
          );
          assert.equal(
            tool?.status,
            "completed",
            "A late-admitted native child's real tail must persist instead of being invented by a cascade.",
          );
          assert.equal(
            tool?.type === "command_execution" ? tool.output : undefined,
            "CHILD_PARTIAL",
          );
          assert.equal(childAfter.providerTurns[0]?.status, "interrupted");
          assert.equal(
            childAfter.nodes.find((node) => node.kind === "root_turn")?.status,
            "interrupted",
          );
          stoppedChild(scenario.events, child.id);
          const tail = domainIndex(
            scenario.events,
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.id === tool?.id &&
              event.payload.status === "completed",
          );
          const replacement = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === after.attempts.at(-1)?.id &&
              event.payload.status === "running",
          );
          assert.isBelow(tail, replacement);
          const nativeTail = boundary.nativeEvents.findIndex(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.id === tool?.id &&
              event.turnItem.status === "completed",
          );
          const terminal = boundary.nativeEvents.findIndex(
            (event) =>
              event.type === "turn.terminal" &&
              event.providerTurnId === before.providerTurns[0]?.id,
          );
          assert.isAbove(terminal, nativeTail);
          assert.lengthOf(
            boundary.nativeEvents.filter(
              (event) =>
                event.type === "turn.terminal" &&
                event.providerTurnId === before.providerTurns[0]?.id,
            ),
            1,
          );
          assert.lengthOf(
            scenario.events.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === tool?.id &&
                event.payload.status === "completed",
            ),
            1,
          );
          const aborted = boundary.requests
            .filter((request) => request.method === "session.abort")
            .map((request) => record(request.params).sessionID);
          assert.includeMembers(aborted, [boundary.nativeThreadId, boundary.childId]);
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const openCode2Restart = (delivery: "restart" | "human") =>
  makeOpenCode2Boundary({ nested: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) =>
            row.nativeTaskRef?.nativeId?.includes("call-background"),
          )!;
          assert.isDefined(child);
          const childBefore = yield* scenario.orchestrator.getThreadProjection(
            child.childThreadId!,
          );
          const nested = childBefore.subagents.find((row) => row.origin === "provider_native")!;
          assert.isDefined(nested);
          const nestedBefore = yield* scenario.orchestrator.getThreadProjection(
            nested.childThreadId!,
          );
          assert.isTrue(
            childBefore.turnItems.some(
              (item) => item.type === "command_execution" && item.status === "running",
            ),
          );
          assert.isTrue(
            nestedBefore.turnItems.some(
              (item) => item.type === "command_execution" && item.status === "running",
            ),
          );
          const pending = [...before.runtimeRequests, ...childBefore.runtimeRequests].filter(
            (request) => request.status === "pending",
          );
          assert.isTrue(
            pending.some((request) => request.nativeRequestRef?.nativeId === "frm_native_child"),
          );
          assert.isTrue(
            pending.some((request) => request.nativeRequestRef?.nativeId === "per_native_child"),
          );
          const running = yield* watchReplacement(scenario, before);
          yield* scenario.send(delivery);
          yield* scenario.worker.drain();
          const cancelled = boundary.requests
            .filter((request) => request.method === "session.interrupt")
            .map((request) => record(request.params).sessionID);
          assert.sameMembers(
            cancelled,
            [boundary.nativeThreadId, boundary.childId, boundary.nestedId],
            "Explicit cut-in must reach the real native cohort.",
          );
          assert.equal(cancelled[0], boundary.nativeThreadId, "The native root stops first.");
          yield* Fiber.join(running);
          const prompts = promptRequests(boundary, "session.prompt");
          assert.lengthOf(prompts, 2);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, prompts[1]!, delivery);
          assert.isFalse(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
          const replacement = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === after.attempts.at(-1)?.id &&
              event.payload.status === "running",
          );
          const oldTurn = before.providerTurns.find((turn) => turn.status === "running")!;
          const oldEnd = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === oldTurn.id &&
              event.payload.status === "interrupted",
          );
          const nativeTerminal = boundary.nativeEvents.findIndex(
            (event) => event.type === "turn.terminal" && event.providerTurnId === oldTurn.id,
          );
          assert.isAtLeast(nativeTerminal, 0);
          for (const [original, projection] of [
            [child, childBefore],
            [nested, nestedBefore],
          ] as const) {
            const stopped = yield* scenario.orchestrator.getThreadProjection(
              original.childThreadId!,
            );
            assert.equal(stopped.providerTurns[0]?.id, projection.providerTurns[0]?.id);
            assert.equal(stopped.providerTurns[0]?.status, "interrupted");
            assert.equal(
              stopped.nodes.find((node) => node.kind === "root_turn")?.status,
              "interrupted",
            );
            const tool = projection.turnItems.find((item) => item.type === "command_execution")!;
            assert.equal(
              stopped.turnItems.find((item) => item.id === tool.id)?.status,
              "interrupted",
            );
            stoppedChild(scenario.events, original.id);
            assert.isBelow(
              domainIndex(
                scenario.events,
                (event) =>
                  event.type === "turn-item.updated" &&
                  event.payload.id === tool.id &&
                  event.payload.status === "interrupted",
              ),
              replacement,
            );
            assert.lengthOf(
              scenario.events.filter(
                (event) =>
                  event.type === "turn-item.updated" &&
                  event.payload.id === tool.id &&
                  event.payload.status === "interrupted",
              ),
              1,
            );
            const rootNode = projection.nodes.find((node) => node.kind === "root_turn")!;
            const nativeEnds = [
              (event: ProviderAdapterV2Event) =>
                event.type === "subagent.updated" &&
                event.subagent.id === original.id &&
                event.subagent.status === "interrupted",
              (event: ProviderAdapterV2Event) =>
                event.type === "provider_turn.updated" &&
                event.providerTurn.id === projection.providerTurns[0]!.id &&
                event.providerTurn.status === "interrupted",
              (event: ProviderAdapterV2Event) =>
                event.type === "node.updated" &&
                event.node.id === rootNode.id &&
                event.node.status === "interrupted",
              (event: ProviderAdapterV2Event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.id === tool.id &&
                event.turnItem.status === "interrupted",
            ];
            for (const matches of nativeEnds) {
              assert.lengthOf(
                boundary.nativeEvents.filter(matches),
                1,
                "The adapter must publish each real descendant end once.",
              );
              assert.isBelow(
                boundary.nativeEvents.findIndex(matches),
                nativeTerminal,
                "A descendant end must precede the old native root terminal.",
              );
            }
            for (const matches of [
              (event: OrchestrationV2DomainEvent) =>
                event.type === "subagent.updated" &&
                event.payload.id === original.id &&
                event.payload.status === "interrupted",
              (event: OrchestrationV2DomainEvent) =>
                event.type === "provider-turn.updated" &&
                event.payload.id === projection.providerTurns[0]!.id &&
                event.payload.status === "interrupted",
              (event: OrchestrationV2DomainEvent) =>
                event.type === "node.updated" &&
                event.payload.id === rootNode.id &&
                event.payload.status === "interrupted",
              (event: OrchestrationV2DomainEvent) =>
                event.type === "turn-item.updated" &&
                event.payload.id === tool.id &&
                event.payload.status === "interrupted",
            ]) {
              assert.lengthOf(scenario.events.filter(matches), 1);
              assert.isBelow(domainIndex(scenario.events, matches), oldEnd);
            }
          }
          for (const request of pending) {
            const owner = before.runtimeRequests.some((row) => row.id === request.id)
              ? threadId
              : child.childThreadId!;
            const rows = (yield* scenario.orchestrator.getThreadProjection(owner)).runtimeRequests;
            assert.equal(rows.find((row) => row.id === request.id)?.status, "cancelled");
            const ended = (event: OrchestrationV2DomainEvent) =>
              event.type === "runtime-request.updated" &&
              event.payload.id === request.id &&
              event.payload.status === "cancelled";
            assert.lengthOf(scenario.events.filter(ended), 1);
            assert.isBelow(domainIndex(scenario.events, ended), oldEnd);
            assert.isBelow(domainIndex(scenario.events, ended), replacement);
            const nativeEnd = (event: ProviderAdapterV2Event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === request.id &&
              event.runtimeRequest.status === "cancelled";
            assert.lengthOf(boundary.nativeEvents.filter(nativeEnd), 1);
            assert.isBelow(boundary.nativeEvents.findIndex(nativeEnd), nativeTerminal);
          }
          const drained = yield* scenario.watch(
            (event) =>
              event.type === "message.updated" && event.payload.text === "OLD_FRAMES_DRAINED",
          );
          yield* boundary.lateFrames;
          yield* Fiber.join(drained);
          const late = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          assert.lengthOf(late.providerTurns, childBefore.providerTurns.length);
          assert.equal(late.providerTurns[0]?.id, childBefore.providerTurns[0]?.id);
          assert.equal(late.providerTurns[0]?.status, "interrupted");
          const rootAfterLateFrames = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.equal(
            rootAfterLateFrames.providerTurns.find((turn) => turn.status === "running")?.id,
            after.providerTurns.find((turn) => turn.status === "running")?.id,
          );
          assert.isFalse(
            rootAfterLateFrames.messages.some(
              (message) => message.text === "LATE_OLD_CHILD_ANSWER",
            ),
          );
          const oldTool = childBefore.turnItems.find((item) => item.type === "command_execution")!;
          const lateTool = late.turnItems.find((item) => item.id === oldTool.id);
          assert.equal(lateTool?.status, "interrupted");
          assert.notEqual(
            lateTool?.type === "command_execution" ? lateTool.output : undefined,
            "CHILD_PARTIAL",
          );
          assert.isFalse(late.messages.some((message) => message.text === "LATE_OLD_CHILD_ANSWER"));
          stoppedChild(scenario.events, child.id);
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const openCode2IncompleteStop = (failure: "http" | "timeout") =>
  makeOpenCode2Boundary({ failure }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          assert.isDefined(child);
          const childBefore = yield* scenario.orchestrator.getThreadProjection(
            child.childThreadId!,
          );
          const pending = [...before.runtimeRequests, ...childBefore.runtimeRequests].filter(
            (request) => request.status === "pending",
          );
          assert.isTrue(
            pending.some((request) =>
              request.nativeRequestRef?.nativeId?.endsWith("frm_native_child"),
            ),
          );
          yield* scenario.send("human");
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Effect.raceFirst(
            Deferred.await(boundary.childInterruptEntered),
            Fiber.await(draining).pipe(Effect.asVoid),
          );
          assert.isTrue(
            boundary.requests.some(
              (request) =>
                request.method === "session.interrupt" &&
                record(request.params).sessionID === boundary.childId,
            ),
            "The failed child Stop must actually be attempted; a soft restart never reaches the 500/timeout guard.",
          );
          if (failure === "timeout") yield* TestClock.adjust("10 seconds");
          yield* Fiber.join(draining);
          assert.lengthOf(
            promptRequests(boundary, "session.prompt"),
            1,
            "Incomplete Stop must not admit the urgent replacement.",
          );
          const effects = yield* scenario.outbox.listByCommandId(CommandId.make("urgent"));
          const restart = effects.find((effect) => effect.request.type === "provider-turn.restart");
          assert.equal(restart?.status, "pending");
          assert.isString(restart?.lastError);
          assert.isNotEmpty(restart!.lastError!);
          const oldTurn = before.providerTurns.find((turn) => turn.status === "running")!;
          const heldRoot = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.equal(
            heldRoot.providerTurns.find((turn) => turn.id === oldTurn.id)?.status,
            "running",
          );
          assert.isFalse(
            boundary.nativeEvents.some(
              (event) => event.type === "turn.terminal" && event.providerTurnId === oldTurn.id,
            ),
          );
          assert.isTrue(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
          const stillRunning = yield* scenario.orchestrator.getThreadProjection(
            child.childThreadId!,
          );
          assert.equal(stillRunning.providerTurns[0]?.id, childBefore.providerTurns[0]?.id);
          assert.equal(stillRunning.providerTurns[0]?.status, "running");
          assert.isFalse(
            scenario.events.some(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            ),
          );
          for (const request of pending) {
            const owner = before.runtimeRequests.some((row) => row.id === request.id)
              ? threadId
              : child.childThreadId!;
            const held = (yield* scenario.orchestrator.getThreadProjection(
              owner,
            )).runtimeRequests.find((row) => row.id === request.id);
            assert.equal(held?.status, "pending");
            assert.deepEqual(held?.responseCapability, request.responseCapability);
            assert.equal(held?.responseCapability.type, "live");
          }
          const tail = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.threadId === child.childThreadId &&
              event.payload.type === "command_execution" &&
              event.payload.output === "CHILD_PARTIAL" &&
              event.payload.status === "completed",
          );
          yield* boundary.partial();
          yield* Fiber.join(tail);
          const partial = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          const tool = partial.turnItems.find(
            (item) => item.type === "command_execution" && item.output === "CHILD_PARTIAL",
          )!;
          assert.isDefined(tool);
          assert.lengthOf(
            scenario.events.filter(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.id === tool.id &&
                event.payload.status === "completed",
            ),
            1,
          );
          const replacement = yield* watchReplacement(scenario, before);
          yield* boundary.allowChildStop();
          // Advance exactly the existing effect worker's first retry delay.
          yield* TestClock.adjust("100 millis");
          yield* scenario.worker.drain();
          yield* Fiber.join(replacement);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, promptRequests(boundary, "session.prompt")[1]!, "human");
          const interrupts = boundary.requests.filter(
            (request) => request.method === "session.interrupt",
          );
          assert.lengthOf(
            interrupts.filter(
              (request) => record(request.params).sessionID === boundary.nativeThreadId,
            ),
            1,
            "A retry must retain the already stopped root and reach only the unconfirmed child.",
          );
          assert.lengthOf(
            interrupts.filter((request) => record(request.params).sessionID === boundary.childId),
            2,
          );
          const stopped = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          const stoppedTool = stopped.turnItems.find((item) => item.id === tool.id);
          assert.equal(stoppedTool?.status, "completed");
          assert.equal(
            stoppedTool?.type === "command_execution" ? stoppedTool.output : undefined,
            "CHILD_PARTIAL",
          );
          stoppedChild(scenario.events, child.id);
          for (const request of pending) {
            const owner = before.runtimeRequests.some((row) => row.id === request.id)
              ? threadId
              : child.childThreadId!;
            assert.equal(
              (yield* scenario.orchestrator.getThreadProjection(owner)).runtimeRequests.find(
                (row) => row.id === request.id,
              )?.status,
              "cancelled",
            );
          }
        }).pipe(Effect.ensuring(boundary.allowChildStop())),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const codexRetainedTerminal = () =>
  makeCodexBoundary({ retained: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(boundary.containment.release, undefined).pipe(Effect.asVoid),
          );
          const first = yield* scenario.orchestrator.getThreadProjection(threadId);
          const completed = yield* scenario.watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === first.providerTurns[0]?.id &&
              event.payload.status === "completed",
          );
          const waiting = yield* scenario.watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runs[0]?.id &&
              event.payload.status === "waiting",
          );
          yield* boundary.completeEarlier();
          yield* Fiber.join(completed);
          yield* Fiber.join(waiting);
          yield* scenario.worker.drain();
          const earlier = yield* scenario.orchestrator.getThreadProjection(threadId);
          const retained = earlier.turnItems.find(
            (item) => item.nativeItemRef?.nativeId === "retained-command",
          )!;
          assert.equal(retained.status, "running");
          const current = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.nativeItemRef?.nativeId === "held-command" &&
              event.payload.status === "running",
          );
          yield* scenario.orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("current"),
            threadId,
            messageId: MessageId.make("current"),
            text: "Start the current ordinary run.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* scenario.worker.drain();
          yield* Fiber.join(current);
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.notEqual(
            before.runs.find((run) => run.status === "running")?.id,
            earlier.runs[0]?.id,
          );
          const running = yield* watchReplacement(scenario, before);
          yield* scenario.send("human");
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Deferred.await(boundary.containment.entered);
          assert.lengthOf(promptRequests(boundary, "turn/start"), 2);
          yield* Deferred.succeed(boundary.containment.release, undefined);
          yield* Fiber.join(draining);
          yield* Fiber.join(running);
          const terminated = boundary.requests.filter(
            (request) => request.method === "thread/backgroundTerminals/terminate",
          );
          assert.sameMembers(
            terminated.map((request) => record(request.params).processId),
            ["77", "99"],
            "Explicit Stop scope must include the earlier retained terminal on this same native thread.",
          );
          assert.lengthOf(terminated, 2);
          yield* Deferred.await(boundary.promptSent(3));
          const nextPrompt = promptRequests(boundary, "turn/start")[2]!;
          for (const request of terminated)
            assert.isBelow(
              boundary.requests.indexOf(request),
              boundary.requests.indexOf(nextPrompt),
            );
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, nextPrompt, "human");
          const replacement = domainIndex(
            scenario.events,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.id ===
                after.providerTurns.find((turn) => turn.status === "running")?.id,
          );
          const currentItem = before.turnItems.find(
            (item) => item.nativeItemRef?.nativeId === "held-command",
          )!;
          for (const original of [retained, currentItem]) {
            const ended = after.turnItems.find((item) => item.id === original.id)!;
            assert.equal(ended.runId, original.runId);
            assert.equal(ended.providerTurnId, original.providerTurnId);
            assert.equal(ended.status, "interrupted");
            const matches = (event: OrchestrationV2DomainEvent) =>
              event.type === "turn-item.updated" &&
              event.payload.id === original.id &&
              event.payload.status === "interrupted";
            assert.lengthOf(scenario.events.filter(matches), 1);
            assert.isBelow(domainIndex(scenario.events, matches), replacement);
          }
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const openCode2LateAdmission = () =>
  makeOpenCode2Boundary({ holdChild: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          yield* scenario.send("human");
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Effect.raceFirst(
            Deferred.await(boundary.childInterruptEntered),
            Fiber.await(draining).pipe(Effect.asVoid),
          );
          assert.isTrue(
            boundary.requests.some(
              (request) =>
                request.method === "session.interrupt" &&
                record(request.params).sessionID === boundary.childId,
            ),
            "The actual child Stop must enter before testing admission during cancellation.",
          );
          assert.isTrue(
            boundary.requests.some(
              (request) =>
                request.method === "session.interrupt" &&
                record(request.params).sessionID === boundary.nativeThreadId,
            ),
            "The urgent root interrupt must not wait behind the held child response.",
          );
          assert.lengthOf(promptRequests(boundary, "session.prompt"), 1);
          // The root has ended natively; child A can still announce its own child B
          // while A's cancellation is in flight. No new root work is fabricated.
          const admitted = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.nativeItemRef?.nativeId?.includes(`bash-${boundary.lateId}`) === true &&
              event.payload.status === "running",
          );
          yield* boundary.launchChild(boundary.childId, boundary.lateId, "call-late-background");
          yield* Fiber.join(admitted);
          const firstChild = before.subagents.find((row) => row.origin === "provider_native")!;
          const childProjection = yield* scenario.orchestrator.getThreadProjection(
            firstChild.childThreadId!,
          );
          const lateChild = childProjection.subagents.find(
            (row) => row.origin === "provider_native",
          )!;
          assert.isDefined(lateChild);
          const running = yield* watchReplacement(scenario, before);
          yield* Deferred.succeed(boundary.releaseChildInterrupt, undefined);
          yield* Fiber.join(draining);
          yield* Fiber.join(running);
          assert.isTrue(
            boundary.requests.some(
              (request) =>
                request.method === "session.interrupt" &&
                record(request.params).sessionID === boundary.lateId,
            ),
            "The adapter's actual Stop cohort must cover the child admitted during cleanup.",
          );
          assert.isFalse(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assertReplacement(before, after, promptRequests(boundary, "session.prompt")[1]!, "human");
          const childAfter = yield* scenario.orchestrator.getThreadProjection(
            firstChild.childThreadId!,
          );
          assert.equal(
            childAfter.subagents.find((row) => row.id === lateChild.id)?.status,
            "interrupted",
          );
          stoppedChild(scenario.events, lateChild.id);
          for (const child of after.subagents.filter((row) => row.origin === "provider_native")) {
            assert.equal(child.status, "interrupted");
            stoppedChild(scenario.events, child.id);
          }
        }).pipe(Effect.ensuring(Deferred.succeed(boundary.releaseChildInterrupt, undefined))),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const cancelledCodexBeforeStop = () =>
  makeCodexBoundary({ child: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          yield* scenario.send("human");
          yield* scenario.outbox.cancelUnsettled({
            threadId,
            effectTypes: ["provider-turn.restart"],
            reason: "Cancel the unexecuted effect without a Stop command.",
          });
          const childEnded = yield* scenario.watch(
            (event) =>
              event.type === "subagent.updated" &&
              event.payload.id === child.id &&
              event.payload.status === "completed",
          );
          yield* boundary.completeChild("completed");
          yield* Fiber.join(childEnded);
          yield* scenario.worker.drain();
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "turn/interrupt"),
            0,
            "Cancelling an outbox effect must not invent a native Stop.",
          );
          const after = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          assert.equal(after.providerTurns[0]?.status, "completed");
          assert.equal(
            after.turnItems.find((item) => item.nativeItemRef?.nativeId === "child-held-command")
              ?.status,
            "completed",
          );
          assert.isFalse(
            scenario.events.some(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            ),
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const stopDuringCodexChildAnswer = () =>
  makeCodexBoundary({ child: true, holdFirstChild: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(boundary.containment.release, undefined);
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const root = before.providerTurns[0]!;
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          yield* scenario.send("human");
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Deferred.await(boundary.childInterruptEntered);
          const stopCommitted = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" && event.payload.type === "run_interrupt_request",
          );
          const stopping = yield* scenario.orchestrator
            .dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("stop-during-child-answer"),
              threadId,
              runId: before.runs[0]!.id,
              holdQueue: true,
            })
            .pipe(Effect.forkScoped);
          yield* Fiber.join(stopCommitted);
          const effects = yield* scenario.outbox.listByCommandId(
            CommandId.make("stop-during-child-answer"),
          );
          assert.isTrue(
            effects.some(
              (effect) =>
                effect.request.type === "provider-turn.interrupt" &&
                effect.request.providerTurnId === root.id,
            ),
            "First Stop must capture the owner while a cut-in call is exiting.",
          );
          const childStopped = yield* scenario.watch(
            (event) =>
              event.type === "subagent.updated" &&
              event.payload.id === child.id &&
              event.payload.status === "interrupted",
          );
          yield* boundary.answerHeldChildInterrupt("reject");
          yield* Fiber.join(stopping);
          yield* Fiber.join(draining);
          yield* scenario.worker.drain();
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          assert.lengthOf(
            boundary.requests.filter(
              (request) =>
                request.method === "turn/interrupt" &&
                record(request.params).threadId === boundary.childId,
            ),
            2,
            "The actual Stop, after the cancelled call exits, must reach the child once.",
          );
          yield* Fiber.join(childStopped);
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const cancelledBeforeStop = () =>
  makeOpenCode2Boundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          yield* scenario.send("restart");
          yield* scenario.outbox.cancelUnsettled({
            threadId,
            effectTypes: ["provider-turn.restart"],
            reason: "Cancel before native Stop executes.",
          });
          const tail = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.threadId === child.childThreadId &&
              event.payload.type === "command_execution" &&
              event.payload.output === "CHILD_PARTIAL",
          );
          yield* boundary.partial();
          yield* Fiber.join(tail);
          yield* scenario.worker.drain();
          assert.lengthOf(promptRequests(boundary, "session.prompt"), 1);
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "session.interrupt"),
            0,
          );
          const after = yield* scenario.orchestrator.getThreadProjection(child.childThreadId!);
          assert.equal(after.providerTurns[0]?.status, "running");
          const root = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.isTrue(
            [...root.runtimeRequests, ...after.runtimeRequests].some(
              (request) =>
                request.status === "pending" &&
                request.nativeRequestRef?.nativeId === "frm_native_child",
            ),
          );
          assert.isFalse(
            scenario.events.some(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === child.id &&
                event.payload.status === "interrupted",
            ),
          );
          assert.isTrue(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const appDelegatedTaskSurvives = () =>
  makeCodexBoundary({ delegated: true }).pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const parent = before.runs[0]!;
          const childRunning = yield* scenario.watch(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.nativeItemRef?.nativeId === "app-owned-held-command" &&
              event.payload.status === "running",
          );
          yield* scenario.orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("delegate-app-child"),
            parentThreadId: threadId,
            parentRunId: parent.id,
            parentNodeId: parent.rootNodeId!,
            task: "Separate app-owned work",
            modelSelection: boundary.modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "always",
            createdBy: "agent",
            creationSource: "mcp",
          });
          yield* scenario.worker.drain();
          yield* Fiber.join(childRunning);
          const attached = yield* scenario.orchestrator.getThreadProjection(threadId);
          const task = attached.subagents.find((row) => row.origin === "app_owned")!;
          assert.isDefined(task);
          const delegatedBefore = yield* scenario.orchestrator.getThreadProjection(
            task.childThreadId!,
          );
          assert.equal(delegatedBefore.runs[0]?.status, "running");
          assert.isTrue(
            delegatedBefore.turnItems.some(
              (item) =>
                item.nativeItemRef?.nativeId === "app-owned-held-command" &&
                item.status === "running",
            ),
          );
          const running = yield* watchReplacement(scenario, attached);
          yield* scenario.send("restart");
          const effects = yield* scenario.outbox.listByCommandId(CommandId.make("urgent"));
          assert.isFalse(effects.some((effect) => effect.request.type === "delegated-tasks.stop"));
          const draining = yield* scenario.worker.drain().pipe(Effect.forkScoped);
          yield* Deferred.await(boundary.containment.entered);
          yield* Deferred.succeed(boundary.containment.release, undefined);
          yield* Fiber.join(draining);
          yield* Fiber.join(running);
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          const delegatedAfter = yield* scenario.orchestrator.getThreadProjection(
            task.childThreadId!,
          );
          assert.equal(after.subagents.find((row) => row.id === task.id)?.status, "running");
          assert.equal(delegatedAfter.runs[0]?.id, delegatedBefore.runs[0]?.id);
          assert.equal(delegatedAfter.runs[0]?.status, "running");
          assert.equal(delegatedAfter.providerTurns[0]?.status, "running");
          assert.isTrue(
            delegatedAfter.turnItems.some(
              (item) =>
                item.nativeItemRef?.nativeId === "app-owned-held-command" &&
                item.status === "running",
            ),
          );
          assert.isFalse(
            boundary.requests.some(
              (request) =>
                request.method === "turn/interrupt" &&
                record(request.params).threadId === boundary.appChildId,
            ),
          );
          assert.isFalse(
            scenario.events.some(
              (event) =>
                event.type === "subagent.updated" &&
                event.payload.id === task.id &&
                event.payload.status !== "running" &&
                event.payload.status !== "pending",
            ),
          );
          const rootPrompts = promptRequests(boundary, "turn/start").filter(
            (request) => record(request.params).threadId === boundary.nativeThreadId,
          );
          assertReplacement(attached, after, rootPrompts[1]!, "restart");
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const routineCodexSteer = () =>
  makeCodexBoundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          yield* scenario.send("auto", "routine", "This update is routine.");
          yield* scenario.worker.drain();
          assert.lengthOf(promptRequests(boundary, "turn/start"), 1);
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "turn/steer"),
            1,
          );
          assert.lengthOf(
            boundary.requests.filter(
              (request) =>
                request.method === "turn/interrupt" ||
                request.method === "thread/backgroundTerminals/terminate",
            ),
            0,
          );
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.equal(after.attempts[0]?.id, before.attempts[0]?.id);
          assert.lengthOf(after.attempts, 1);
          assert.equal(
            after.turnItems.find((item) => item.nativeItemRef?.nativeId === "held-command")?.status,
            "running",
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const routineOpenCode2Steer = () =>
  makeOpenCode2Boundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          const child = before.subagents.find((row) => row.origin === "provider_native")!;
          yield* scenario.send("auto", "routine", "This update is routine.");
          yield* scenario.worker.drain();
          assert.lengthOf(promptRequests(boundary, "session.prompt"), 2);
          assert.lengthOf(
            boundary.requests.filter((request) => request.method === "session.interrupt"),
            0,
          );
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.lengthOf(after.attempts, 1);
          assert.equal(after.subagents.find((row) => row.id === child.id)?.status, "running");
          assert.isTrue(yield* (yield* scenario.runtime).hasPendingBackgroundWork!);
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

const grokQueue = () =>
  makeGrokBoundary().pipe(
    Effect.flatMap((boundary) =>
      withScenario(boundary, (scenario) =>
        Effect.gen(function* () {
          const before = yield* scenario.orchestrator.getThreadProjection(threadId);
          yield* scenario.send("queue", "queued", "Let the current work finish first.");
          yield* scenario.worker.drain();
          const after = yield* scenario.orchestrator.getThreadProjection(threadId);
          assert.isTrue(after.runs.some((run) => run.status === "queued"));
          assert.lengthOf(promptRequests(boundary, "session/prompt"), 1);
          assert.lengthOf(
            boundary.requests.filter(
              (request) =>
                request.method === "session/cancel" || request.method === "process-group/kill",
            ),
            0,
          );
          assert.equal(
            after.subagents.find((row) => row.id === before.subagents[0]?.id)?.status,
            "running",
          );
        }),
      ),
    ),
    Effect.scoped,
    Effect.provide(provideTestServices),
  );

describe("urgent restart through native adapters", { concurrent: false }, () => {
  it.effect(
    "explicit restart leaves a separately delegated app-owned session running",
    appDelegatedTaskSurvives,
  );
  it.effect(
    "routine Codex auto remains native steering with the foreground still running",
    routineCodexSteer,
  );
  it.effect("routine OpenCode 2 auto preserves its native background child", routineOpenCode2Steer);
  it.effect(
    "Grok existing Stop kills its replay process and reloads the original transcript",
    grokExistingStop,
  );
  it.effect("Grok Queue defers the message without any cancellation", grokQueue);
  it.effect("delivers MCP restart through the real Codex adapter", () => codexRestart("restart"));
  it.effect("a person's Codex Steer interrupts before delivering the replacement", () =>
    codexRestart("human"),
  );
  it.effect(
    "Codex explicit Steer ends earlier retained terminal 77 and current 99 once",
    codexRetainedTerminal,
  );
  it.effect(
    "Codex keeps a known child's real tail when the native root terminal arrives first",
    () => codexRestart("human", true),
  );
  it.effect.each(["restart", "human"] as const)(
    "Codex %s retries a rejected child before delivering one replacement",
    (delivery) => codexIncompleteStop(delivery, "retry"),
  );
  it.effect.each(["completed", "failed"] as const)(
    "Codex still stops the live child when its root %s before the cut-in effect",
    (rootEnd) => codexIncompleteStop("human", "retry", rootEnd),
  );
  it.effect.each(["natural", "stop"] as const)(
    "Codex retains failed cut-in ownership until %s releases the native child",
    (finish) => codexIncompleteStop("human", finish),
  );
  it.effect.each(["completed", "failed"] as const)(
    "Codex Stop reaches a held child on the first press after its root %s before the cut-in effect",
    (status) => codexIncompleteStop("human", "stop-before-retry", status),
  );
  it.effect(
    "Codex joins older held and newer completed roots before one replacement",
    codexOverlappingHeldRoots,
  );
  it.effect.each(["replace", "detach"] as const)(
    "Codex human Stop reaches the captured runtime before a production-policy %s",
    (transition) => codexInstanceStop(transition, "before", "human"),
  );
  it.effect.each(["replace", "detach"] as const)(
    "Codex human Stop reaches the captured runtime during a production-policy %s retry",
    (transition) => codexInstanceStop(transition, "retry", "human"),
  );
  it.effect.each(["replace", "detach"] as const)(
    "Codex agent restart retains first-Stop reach after a production-policy %s fails",
    (transition) => codexInstanceStop(transition, "failed", "restart"),
  );
  it.effect.each(["replace", "detach"] as const)(
    "Codex Stop cancels an unexecuted routine production-policy %s without losing old work",
    (transition) => codexInstanceStop(transition, "before", "auto"),
  );
  it.effect.each(["replace", "detach"] as const)(
    "Codex cancelling a production-policy %s alone preserves the old native observer",
    (transition) => codexInstanceStop(transition, "cancel", "human"),
  );
  it.effect(
    "Codex Stop reaches only the replacement after a successful cross-instance cut-in",
    () => codexInstanceStop("replace", "delivered", "human"),
  );
  it.effect(
    "a cancelled, unexecuted Codex cut-in preserves the native child observer",
    cancelledCodexBeforeStop,
  );
  it.effect(
    "Codex first Stop joins a cut-in held on its child's native answer",
    stopDuringCodexChildAnswer,
  );
  it.effect.each(["ack", "reject", "silent"] as const)(
    "Codex persists late-child reader progress before its interrupt %s outcome",
    codexLateReaderProgress,
  );
  it.effect.each(["restart", "mobile"] as const)(
    "Grok %s kills its owned replay process, ends native work and resumes the transcript",
    (delivery) => grokRestart(delivery),
  );
  it.effect.each(["auto", "steer"] as const)(
    "routine Grok %s keeps its native background child and ACP runtime",
    (delivery) => grokRestart(delivery),
  );
  it.effect(
    "Grok routine auto carries the same child into one hard Steer, without duplicate old ends on another",
    () => grokRestart("human", true),
  );
  it.effect.each(["restart", "human"] as const)(
    "OpenCode 1 %s persists an attached busy child's late tool tail before replacement",
    (delivery) => openCode1Restart(delivery),
  );
  it.effect.each(["http", "timeout"] as const)(
    "OpenCode 2 incomplete %s Stop retains its child tail and pending question until successful retry",
    openCode2IncompleteStop,
  );
  it.effect(
    "OpenCode 2 interrupts the root promptly and includes native child admission during cancellation",
    openCode2LateAdmission,
  );
  it.effect(
    "a cancelled, unexecuted OpenCode 2 Stop preserves the native child observer",
    cancelledBeforeStop,
  );
  it.effect.each(["restart", "human"] as const)(
    "OpenCode 2 %s stops native descendants and pending input before replacement",
    (delivery) => openCode2Restart(delivery),
  );
});
