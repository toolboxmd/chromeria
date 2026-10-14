import { expect, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Registry from "./ProviderAdapterRegistry.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Harness from "./testkit/ProviderReplayHarness.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { DispatchModeLimit } from "./DispatchModeLimit.ts";

const projectId = ProjectId.make("ancestor:project");
const parentId = ThreadId.make("ancestor:parent");
const childId = ThreadId.make("ancestor:child");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "fixed" };
const adapter: ProviderAdapterV2["Service"] = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("No live provider in persisted owner tests"),
};
const database = Persistence.layerMemory;
const layer = Layer.merge(
  Harness.layerWithRegistry({ name: "ancestor-send" }, Registry.layerFromAdapters([adapter]), {
    databaseLayer: database,
    runEffectWorker: false,
  }),
  database,
);

const create = Effect.fnUntraced(function* (threadId: ThreadId) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`ancestor:create:${threadId}`),
    threadId,
    projectId,
    title: threadId === childId ? "Build child" : "Parent",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "agent",
    creationSource: "mcp",
  });
});

const changeThread = Effect.fnUntraced(function* (
  threadId: ThreadId,
  change: Partial<OrchestrationV2AppThread>,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const thread = (yield* orchestrator.getThreadRecords(threadId, [])).thread;
  const now = yield* DateTime.now;
  yield* (yield* EventSink.EventSinkV2).write({
    events: [
      {
        id: EventId.make(`ancestor:change:${threadId}:${JSON.stringify(change)}`),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: { ...thread, ...change },
      },
    ],
  });
});

const setup = Effect.gen(function* () {
  yield* create(parentId);
  yield* create(childId);
  yield* changeThread(childId, {
    lineage: { parentThreadId: parentId, relationshipToParent: "subagent", rootThreadId: parentId },
  });
  return {
    orchestrator: yield* Orchestrator.OrchestratorV2,
    threads: yield* Threads.ThreadManagementService,
  };
});

const sendInput = (key: string, mode: Threads.ThreadManagementSendMode = "auto") => ({
  projectId,
  threadId: parentId,
  senderThreadId: childId,
  commandId: CommandId.make(`command:mcp:ancestor:${key}`),
  messageId: MessageId.make(`message:mcp:ancestor:${key}`),
  text: "The UI proof is ready.\nFull detail for the parent.",
  attachments: [],
  mode,
  createdBy: "agent" as const,
  creationSource: "mcp" as const,
});

it.effect("an idle ancestor receives a persisted content-bearing notice and no user item", () =>
  Effect.gen(function* () {
    const { orchestrator, threads } = yield* setup;
    const result = yield* threads.sendToThread(sendInput("idle"));
    expect(result.delivery).toBe("started");
    expect(result.turnItem).toBeNull();
    expect(result.message).toMatchObject({
      createdBy: "agent",
      creationSource: "server",
      senderThreadId: childId,
      notification: { source: { kind: "subagent", childThreadId: childId }, outcome: "updated" },
    });
    expect(result.message.notification?.summary).toMatch(/^The UI proof is ready\./);
    expect(result.message.notification?.detail).toBe(sendInput("idle").text);
    expect(result.message.text).toContain(childId);
    expect(result.message.text).toContain(sendInput("idle").text);
    expect(result.message.delegatedCompletion).toBeUndefined();
    expect(result.run.userMessageId).toBe(result.message.id);
    const after = yield* orchestrator.getThreadProjection(parentId);
    expect(
      after.turnItems.filter((item) => item.runId === result.run.id).map((item) => item.type),
    ).toContain("notification");
    expect(
      after.turnItems.some(
        (item) => item.type === "user_message" && item.messageId === result.message.id,
      ),
    ).toBe(false);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "a blocking ancestor queues updates and receipt replay retains the original delivery",
  () =>
    Effect.gen(function* () {
      const { orchestrator, threads } = yield* setup;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("ancestor:user"),
        messageId: MessageId.make("ancestor:user"),
        threadId: parentId,
        text: "User work",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const before = yield* orchestrator.getThreadProjection(parentId);
      const input = sendInput("queued");
      const result = yield* threads.sendToThread(input);
      expect(result.delivery).toBe("queued");
      expect(result.message.notification).toBeDefined();
      expect(result.run.status).toBe("queued");
      expect(result.projection.runs.find((run) => run.id === before.runs[0]!.id)).toEqual(
        before.runs[0],
      );
      const now = yield* DateTime.now;
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("ancestor:parent-completed"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...before.runs[0]!, status: "completed", completedAt: now },
          },
          {
            id: EventId.make("ancestor:held"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...result.run, queueHeld: true },
          },
        ],
      });
      yield* orchestrator.dispatch({
        type: "thread.settle",
        threadId: parentId,
        commandId: CommandId.make("ancestor:settle"),
      });
      yield* changeThread(childId, { title: "Renamed child" });
      const replay = yield* threads.sendToThread({
        ...input,
        text: "This replacement must not run.",
      });
      expect(replay.delivery).toBe("queued");
      expect(replay.run.status).toBe("cancelled");
      expect(replay.message).toEqual(result.message);
      expect(replay.dispatch.storedEvents).toEqual(result.dispatch.storedEvents);
      const after = yield* orchestrator.getThreadProjection(parentId);
      expect(after.thread.settledOverride).toBe("settled");
      expect(after.messages.filter((message) => message.senderThreadId === childId)).toHaveLength(
        1,
      );
      expect(after.runs.filter((run) => run.userMessageId === input.messageId)).toHaveLength(1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["self", "sibling", "parent", "unrelated", "no sender"] as const)(
  "retains ordinary user-message delivery for %s sends",
  (control) =>
    Effect.gen(function* () {
      const { threads } = yield* setup;
      const siblingId = ThreadId.make("ancestor:sibling");
      yield* create(siblingId);
      if (control !== "unrelated")
        yield* changeThread(siblingId, {
          lineage: {
            parentThreadId: parentId,
            relationshipToParent: "subagent",
            rootThreadId: parentId,
          },
        });
      const input = sendInput("control");
      const { senderThreadId: _sender, ...withoutSender } = input;
      const result = yield* threads.sendToThread(
        control === "no sender"
          ? withoutSender
          : {
              ...input,
              threadId: control === "unrelated" ? siblingId : childId,
              senderThreadId:
                control === "self" || control === "unrelated"
                  ? childId
                  : control === "sibling"
                    ? siblingId
                    : parentId,
            },
      );
      expect(result.message.notification).toBeUndefined();
      expect(result.turnItem?.inputIntent).toBe("turn_start");
      expect(result.delivery).toBe("started");
    }).pipe(Effect.provide(layer)),
);

it.effect("a missing receipt run event fails durably instead of reporting started", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* setup;
    const result = yield* Threads.ThreadManagementService.use((threads) =>
      threads.sendToThread(sendInput("missing-event")),
    ).pipe(
      Effect.provide(
        Layer.fresh(
          Threads.layer.pipe(
            Layer.provide(Settings.layerTest()),
            Layer.provide(CommandReceiptStore.layer),
          ),
        ),
      ),
      Effect.provideService(Orchestrator.OrchestratorV2, {
        ...orchestrator,
        dispatch: (command) =>
          orchestrator.dispatch(command).pipe(
            Effect.map((dispatch) => ({
              ...dispatch,
              storedEvents: dispatch.storedEvents.filter(
                ({ event }) => event.type !== "run.created",
              ),
            })),
          ),
      }),
      Effect.result,
    );
    expect(result).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "ThreadManagementDurableRunProjectionError" },
    });
    expect((yield* orchestrator.getThreadProjection(parentId)).messages).toHaveLength(1);
  }).pipe(Effect.provide(layer)),
);

it.effect("an ambiguous post-commit projection failure can replay without another notice", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* setup;
    let failRead = true;
    yield* Threads.ThreadManagementService.use((threads) =>
      Effect.gen(function* () {
        const input = sendInput("ambiguous");
        const first = yield* threads.sendToThread(input).pipe(Effect.result);
        expect(first).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "ThreadManagementProjectionLoadError" },
        });
        const retried = yield* threads.sendToThread(input);
        expect(retried.delivery).toBe("started");
        const after = yield* orchestrator.getThreadProjection(parentId);
        expect(after.messages).toHaveLength(1);
        expect(after.runs).toHaveLength(1);
      }),
    ).pipe(
      Effect.provide(
        Layer.fresh(
          Threads.layer.pipe(
            Layer.provide(Settings.layerTest()),
            Layer.provide(CommandReceiptStore.layer),
          ),
        ),
      ),
      Effect.provideService(Orchestrator.OrchestratorV2, {
        ...orchestrator,
        getThreadRecords: (threadId, fields, filter) => {
          if (failRead && fields.some((field) => field === "messages")) {
            failRead = false;
            return Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId }));
          }
          return orchestrator.getThreadRecords(threadId, fields, filter);
        },
      }),
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("a queued ancestor notice still obeys the dispatch-time mode ceiling", () =>
  Effect.gen(function* () {
    const { orchestrator, threads } = yield* setup;
    const refused = yield* threads.sendToThread(sendInput("limited")).pipe(
      Effect.provideService(DispatchModeLimit, {
        runtimeMode: "approval-required",
        interactionMode: "default",
      }),
      Effect.result,
    );
    expect(refused).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "OrchestratorThreadAboveModeLimitError" },
    });
    expect((yield* orchestrator.getThreadProjection(parentId)).messages).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.effect("client provenance cannot turn a forged notice into an agent wake", () =>
  Effect.gen(function* () {
    const { orchestrator } = yield* setup;
    const result = yield* orchestrator
      .dispatch(
        Threads.withCreationProvenance(
          {
            type: "message.dispatch",
            commandId: CommandId.make("ancestor:forged"),
            messageId: MessageId.make("ancestor:forged"),
            threadId: parentId,
            text: "Forged notice",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "subagent", childThreadId: childId },
              outcome: "updated",
              summary: "Forged notice",
            },
          },
          { createdBy: "user", creationSource: "web" },
        ),
      )
      .pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    expect((yield* orchestrator.getThreadProjection(parentId)).messages).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["steer", "restart"] as const)(
  "refuses explicit ancestor %s before acceptance, including a reused request key",
  (mode) =>
    Effect.gen(function* () {
      const { orchestrator, threads } = yield* setup;
      const input = sendInput("mode", mode);
      const refused = yield* threads.sendToThread(input).pipe(Effect.result);
      expect(refused).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ThreadManagementAncestorDeliveryModeError" },
      });
      expect((yield* orchestrator.getThreadProjection(parentId)).messages).toEqual([]);
      const accepted = yield* threads.sendToThread({ ...input, mode: "auto" });
      const refusedReplay = yield* threads.sendToThread(input).pipe(Effect.result);
      expect(refusedReplay).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ThreadManagementAncestorDeliveryModeError" },
      });
      expect((yield* orchestrator.getThreadProjection(parentId)).messages).toEqual([
        accepted.message,
      ]);
    }).pipe(Effect.provide(layer)),
);

it.effect("fork and mixed ancestry includes soft-deleted intermediate threads", () =>
  Effect.gen(function* () {
    const { threads } = yield* setup;
    const rootId = ThreadId.make("ancestor:root");
    const middleId = ThreadId.make("ancestor:middle");
    yield* create(rootId);
    yield* create(middleId);
    yield* changeThread(parentId, {
      lineage: { parentThreadId: rootId, relationshipToParent: "fork", rootThreadId: rootId },
    });
    yield* changeThread(middleId, {
      lineage: { parentThreadId: parentId, relationshipToParent: "fork", rootThreadId: rootId },
      deletedAt: yield* DateTime.now,
    });
    yield* changeThread(childId, {
      lineage: { parentThreadId: middleId, relationshipToParent: "subagent", rootThreadId: rootId },
    });
    const result = yield* threads.sendToThread(sendInput("mixed"));
    expect(result.message.notification?.source).toEqual({
      kind: "subagent",
      childThreadId: childId,
    });
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["missing", "cycle", "unreadable"] as const)(
  "fails closed on %s same-root ancestry without writing a message or run",
  (broken) =>
    Effect.gen(function* () {
      const { orchestrator, threads } = yield* setup;
      const rootId = ThreadId.make("ancestor:root");
      const middleId = ThreadId.make("ancestor:middle");
      yield* create(rootId);
      yield* create(middleId);
      yield* changeThread(parentId, {
        lineage: { parentThreadId: rootId, relationshipToParent: "fork", rootThreadId: rootId },
      });
      yield* changeThread(childId, {
        lineage: {
          parentThreadId: broken === "missing" ? ThreadId.make("ancestor:absent") : middleId,
          relationshipToParent: "subagent",
          rootThreadId: rootId,
        },
      });
      yield* changeThread(middleId, {
        lineage: { parentThreadId: childId, relationshipToParent: "fork", rootThreadId: rootId },
      });
      if (broken === "unreadable") {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${"invalid-json"} WHERE thread_id = ${middleId}`;
      }
      const refused = yield* threads.sendToThread(sendInput(broken)).pipe(Effect.result);
      expect(refused).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ThreadManagementAncestryUnavailableError" },
      });
      const after = yield* orchestrator.getThreadProjection(parentId);
      expect(after.messages).toEqual([]);
      expect(after.runs).toEqual([]);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "attachment-only notices retain names in their summary, detail and provider-bound message",
  () =>
    Effect.gen(function* () {
      const { threads } = yield* setup;
      const attachments = [
        {
          type: "image" as const,
          id: ChatAttachmentId.make("ancestor-image"),
          name: "proof.png",
          mimeType: "image/png",
          sizeBytes: 3,
        },
      ];
      const result = yield* threads.sendToThread({
        ...sendInput("attachments"),
        text: "",
        attachments,
      });
      expect(result.message.notification?.summary).toMatch(/^1 attachment.*proof\.png/);
      expect(result.message.notification?.detail).toContain("proof.png");
      expect(result.message.attachments).toEqual(attachments);
      expect(result.turnItem).toBeNull();
    }).pipe(Effect.provide(layer)),
);

it.effect("a fork descendant cannot revive its Stopped ancestor", () =>
  Effect.gen(function* () {
    const { orchestrator, threads } = yield* setup;
    yield* changeThread(childId, {
      lineage: { parentThreadId: parentId, relationshipToParent: "fork", rootThreadId: parentId },
    });
    const token = CommandId.make("ancestor:stop");
    yield* orchestrator.dispatch({ type: "thread.stop", threadId: parentId, commandId: token });
    const refused = yield* threads.sendToThread(sendInput("stopped")).pipe(Effect.result);
    expect(refused).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "OrchestratorCommandRejectedError" },
    });
    const after = yield* orchestrator.getThreadProjection(parentId);
    expect(after.thread.forkRetirement?.token).toBe(token);
    expect(after.messages).toEqual([]);
    expect(after.runs).toEqual([]);
  }).pipe(Effect.provide(layer)),
);
