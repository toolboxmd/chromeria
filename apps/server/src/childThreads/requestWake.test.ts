// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { forkV1SnapshotLayer } from "../persistence/forkV1Snapshot.testFixtures.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NonNegativeInt,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Layer from "effect/Layer";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import {
  layer as receiptLayer,
  CommandReceiptStoreV2,
} from "../orchestration-v2/CommandReceiptStore.ts";
import * as Option from "effect/Option";
import { EventSinkV2 } from "../orchestration-v2/EventSink.ts";
import { reconcileDelegatedTaskQuestion } from "../orchestration-v2/DelegatedTaskQuestions.ts";
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";

const databaseLayer = SqlitePersistence.layerMemory;
const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider worker in request recovery test"),
} satisfies ProviderAdapterV2["Service"];

it.effect(
  "startup reconciles descendant requests once and cancels queued notices when resolved or stopped",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const sink = yield* EventSinkV2;
      const now = yield* DateTime.now;
      const root = ThreadId.make("root");
      const child = ThreadId.make("child");
      const grandchild = ThreadId.make("grandchild");
      const unrelated = ThreadId.make("unrelated");
      for (const id of [root, child, grandchild, unrelated]) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${id}`),
          threadId: id,
          projectId: ProjectId.make("project:request-wake"),
          title: id,
          modelSelection: { instanceId, model: "gpt-5.1-codex" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
      }
      for (const [id, parentId] of [
        [child, root],
        [grandchild, child],
      ] as const) {
        const projection = yield* orchestrator.getThreadProjection(id);
        yield* sink.write({
          events: [
            {
              id: EventId.make(`lineage:${id}`),
              type: "thread.metadata-updated",
              threadId: id,
              providerInstanceId: instanceId,
              occurredAt: now,
              payload: {
                ...projection.thread,
                lineage: {
                  parentThreadId: parentId,
                  rootThreadId: root,
                  relationshipToParent: "subagent",
                },
              },
            },
          ],
        });
      }
      for (const id of [root, child]) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`work:${id}`),
          threadId: id,
          messageId: MessageId.make(`work:${id}`),
          text: "Work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
      }
      const request = {
        id: RuntimeRequestId.make("question"),
        nodeId: NodeId.make("request-node"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "user_input" as const,
        status: "pending" as const,
        responseCapability: { type: "message" as const },
        createdAt: now,
        resolvedAt: null,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("request:pending"),
            type: "runtime-request.updated",
            threadId: grandchild,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: request,
          },
        ],
      });
      yield* orchestrator.recoverDelegatedTasks;
      yield* orchestrator.recoverDelegatedTasks;
      for (const id of [root, child]) {
        const projection = yield* orchestrator.getThreadProjection(id);
        assert.lengthOf(
          projection.messages.filter((message) => message.text.includes("question answer")),
          1,
        );
        assert.lengthOf(
          projection.runs.filter((run) => run.status === "queued"),
          1,
        );
      }
      assert.lengthOf((yield* orchestrator.getThreadProjection(unrelated)).messages, 0);
      yield* sink.write({
        events: [
          {
            id: EventId.make("request:resolved"),
            type: "runtime-request.updated",
            threadId: grandchild,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: {
              ...request,
              status: "resolved",
              resolvedAt: now,
              answers: { choice: "continue" },
            },
          },
        ],
      });
      yield* orchestrator.recoverDelegatedTasks;
      for (const id of [root, child]) {
        assert.lengthOf(
          (yield* orchestrator.getThreadProjection(id)).runs.filter(
            (run) => run.status === "queued",
          ),
          0,
        );
      }
      const receipts = yield* CommandReceiptStoreV2;
      const rejectedId = CommandId.make(`command:child-request:${grandchild}:approval:${root}:0`);
      yield* receipts.upsert({
        commandId: rejectedId,
        threadId: root,
        commandType: "message.dispatch",
        acceptedAt: now,
        resultSequence: NonNegativeInt.make(0),
        status: "rejected",
        error: "Prior delivery rejected",
      });
      const approval = {
        ...request,
        id: RuntimeRequestId.make("approval"),
        kind: "command" as const,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("approval:pending"),
            type: "runtime-request.updated",
            threadId: grandchild,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: approval,
          },
        ],
      });
      yield* orchestrator.recoverDelegatedTasks;
      const retriedReceipt = yield* receipts.getByCommandId(
        CommandId.make(`command:child-request:${grandchild}:approval:${root}:1`),
      );
      assert.isTrue(Option.isSome(retriedReceipt));
      if (Option.isSome(retriedReceipt)) assert.equal(retriedReceipt.value.status, "accepted");
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop:root"),
        threadId: root,
      });
      yield* orchestrator.recoverDelegatedTasks;
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(root)).runs.filter(
          (run) => run.status === "queued",
        ),
        0,
      );
      yield* orchestrator.recoverDelegatedTasks;
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(root)).messages.filter((message) =>
          message.text.includes("an approval"),
        ),
        1,
      );
    }).pipe(
      Effect.provide(
        Layer.merge(
          Harness.layerWithRegistry(
            { name: "request-wake" },
            Registry.layerFromAdapters([adapter]),
            {
              runEffectWorker: false,
              databaseLayer,
            },
          ),
          receiptLayer.pipe(Layer.provide(databaseLayer)),
        ),
      ),
    ),
);

it.effect("live pending-request listener wakes an ancestor without invoking recovery", () =>
  Effect.gen(function* () {
    const orchestrator = yield* OrchestratorV2;
    const sink = yield* EventSinkV2;
    const receipts = yield* CommandReceiptStoreV2;
    const now = yield* DateTime.now;
    const root = ThreadId.make("live-root");
    const child = ThreadId.make("live-child");
    for (const id of [root, child]) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${id}`),
        threadId: id,
        projectId: ProjectId.make("project:request-live"),
        title: id,
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    }
    const childProjection = yield* orchestrator.getThreadProjection(child);
    yield* sink.write({
      events: [
        {
          id: EventId.make("live-lineage"),
          type: "thread.metadata-updated",
          threadId: child,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            ...childProjection.thread,
            lineage: { parentThreadId: root, rootThreadId: root, relationshipToParent: "subagent" },
          },
        },
      ],
    });
    const requestId = RuntimeRequestId.make("live-approval");
    const wakeCommandId = CommandId.make(`command:child-request:${child}:${requestId}:${root}:0`);
    const cursor = yield* sink.latestSequence();
    const observed = yield* sink.stream({ afterSequence: cursor }).pipe(
      Stream.filter(
        (stored) => stored.commandId === wakeCommandId && stored.event.type === "message.updated",
      ),
      Stream.runHead,
      Effect.forkChild,
    );
    yield* sink.write({
      events: [
        {
          id: EventId.make("live-request"),
          type: "runtime-request.updated",
          threadId: child,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: requestId,
            nodeId: NodeId.make("live-node"),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "command",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        },
      ],
    });
    assert.isTrue(Option.isSome(yield* Fiber.join(observed)));
    const receipt = yield* receipts.getByCommandId(wakeCommandId);
    assert.isTrue(Option.isSome(receipt));
    if (Option.isSome(receipt)) assert.equal(receipt.value.status, "accepted");
    const parent = yield* orchestrator.getThreadProjection(root);
    assert.lengthOf(parent.messages, 1);
    assert.include(parent.messages[0]!.text, "an approval");
  }).pipe(
    Effect.provide(
      Layer.merge(
        Harness.layerWithRegistry({ name: "request-live" }, Registry.layerFromAdapters([adapter]), {
          databaseLayer,
          runEffectWorker: false,
        }),
        receiptLayer.pipe(Layer.provide(databaseLayer)),
      ),
    ),
  ),
);

it.effect("committed ancestor resume wakes only its subtree, once", () =>
  Effect.gen(function* () {
    const orchestrator = yield* OrchestratorV2;
    const sink = yield* EventSinkV2;
    const receipts = yield* CommandReceiptStoreV2;
    const now = yield* DateTime.now;
    const root = ThreadId.make("resume-root");
    const child = ThreadId.make("resume-child");
    const controlRoot = ThreadId.make("control-root");
    const controlChild = ThreadId.make("control-child");
    const unrelatedRoot = ThreadId.make("unrelated-root");
    const unrelatedChild = ThreadId.make("unrelated-child");
    for (const id of [root, child, controlRoot, controlChild, unrelatedRoot, unrelatedChild]) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${id}`),
        threadId: id,
        projectId: ProjectId.make("project:request-resume"),
        title: id,
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    }
    // Pending before it becomes a subagent, so only an environment-wide sweep
    // would ever deliver it to its parent.
    const unrelatedRequestId = RuntimeRequestId.make("unrelated-approval");
    yield* sink.write({
      events: [
        {
          id: EventId.make(`request:${unrelatedRequestId}`),
          type: "runtime-request.updated",
          threadId: unrelatedChild,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: unrelatedRequestId,
            nodeId: NodeId.make(`node:${unrelatedRequestId}`),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "command",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        },
      ],
    });
    for (const [id, parentId] of [
      [child, root],
      [controlChild, controlRoot],
      [unrelatedChild, unrelatedRoot],
    ] as const) {
      const projection = yield* orchestrator.getThreadProjection(id);
      yield* sink.write({
        events: [
          {
            id: EventId.make(`lineage:${id}`),
            type: "thread.metadata-updated",
            threadId: id,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: {
              ...projection.thread,
              lineage: {
                parentThreadId: parentId,
                rootThreadId: parentId,
                relationshipToParent: "subagent",
              },
            },
          },
        ],
      });
    }
    const resume = (id: ThreadId, suffix: string) =>
      orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`resume:${suffix}`),
        messageId: MessageId.make(`resume:${suffix}`),
        threadId: id,
        text: "Resume",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
    const pending = (id: ThreadId, requestId: RuntimeRequestId) =>
      sink.write({
        events: [
          {
            id: EventId.make(`request:${requestId}`),
            type: "runtime-request.updated",
            threadId: id,
            providerInstanceId: instanceId,
            occurredAt: now,
            payload: {
              id: requestId,
              nodeId: NodeId.make(`node:${requestId}`),
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "command",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
        ],
      });
    // The listener is sequential. A later control request's durable wake proves
    // it has consumed all earlier events, without polling or manual recovery.
    const drainListener = Effect.fnUntraced(function* (suffix: string) {
      const requestId = RuntimeRequestId.make(`control:${suffix}`);
      const commandId = CommandId.make(
        `command:child-request:${controlChild}:${requestId}:${controlRoot}:0`,
      );
      const cursor = yield* sink.latestSequence();
      const observed = yield* sink.stream({ afterSequence: cursor }).pipe(
        Stream.filter(
          (stored) => stored.commandId === commandId && stored.event.type === "message.updated",
        ),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* pending(controlChild, requestId);
      assert.isTrue(Option.isSome(yield* Fiber.join(observed)));
    });
    yield* orchestrator.dispatch({
      type: "thread.stop",
      threadId: root,
      commandId: CommandId.make("stop:root"),
    });
    yield* resume(child, "child");
    const requestId = RuntimeRequestId.make("suppressed-approval");
    const wakeCommandId = CommandId.make(`command:child-request:${child}:${requestId}:${root}:0`);
    yield* pending(child, requestId);
    yield* drainListener("suppressed");
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(wakeCommandId)));
    const delivered = yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
      Stream.filter(
        (stored) => stored.commandId === wakeCommandId && stored.event.type === "message.updated",
      ),
      Stream.runHead,
      Effect.forkChild,
    );
    yield* resume(root, "root");
    assert.isTrue(Option.isSome(yield* Fiber.join(delivered)));
    const receipt = yield* receipts.getByCommandId(wakeCommandId);
    assert.isTrue(Option.isSome(receipt));
    if (Option.isSome(receipt)) assert.equal(receipt.value.status, "accepted");
    yield* resume(root, "root-again");
    // The resumed tokens stay on the thread; a later metadata update is not a resume.
    const resumed = yield* orchestrator.getThreadProjection(root);
    yield* sink.write({
      events: [
        {
          id: EventId.make("later-metadata"),
          type: "thread.metadata-updated",
          threadId: root,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: { ...resumed.thread, title: "Renamed" },
        },
      ],
    });
    yield* drainListener("deduplicated");
    assert.isTrue(
      Option.isNone(
        yield* receipts.getByCommandId(
          CommandId.make(
            `command:child-request:${unrelatedChild}:${unrelatedRequestId}:${unrelatedRoot}:0`,
          ),
        ),
      ),
    );
    const projection = yield* orchestrator.getThreadProjection(root);
    assert.lengthOf(
      projection.messages.filter(
        (message) => message.id === MessageId.make(`child-request:${child}:${requestId}:${root}`),
      ),
      1,
    );
  }).pipe(
    Effect.provide(
      Layer.merge(
        Harness.layerWithRegistry(
          { name: "request-resume" },
          Registry.layerFromAdapters([adapter]),
          { databaseLayer, runEffectWorker: false },
        ),
        receiptLayer.pipe(Layer.provide(databaseLayer)),
      ),
    ),
  ),
);

it.effect(
  "a recreated runtime recovers an offline pending request and a later boot deduplicates its accepted wake",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "chromeria-request-restart-"),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "chromeria-v2.sqlite");
      const runtime = () =>
        Harness.layerWithRegistry(
          { name: "request-restart" },
          Registry.layerFromAdapters([adapter]),
          {
            databaseLayer: SqlitePersistence.layerFromPath(filename).pipe(
              Layer.provide(NodeServices.layer),
            ),
            runEffectWorker: false,
          },
        );
      const root = ThreadId.make("restart-root");
      const child = ThreadId.make("restart-child");
      const now = yield* DateTime.now;
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const sink = yield* EventSinkV2;
        for (const id of [root, child]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${id}`),
            threadId: id,
            projectId: ProjectId.make("project:request-restart"),
            title: id,
            modelSelection: { instanceId, model: "gpt-5.1-codex" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
        }
        const projection = yield* orchestrator.getThreadProjection(child);
        yield* sink.write({
          events: [
            {
              id: EventId.make("restart-lineage"),
              type: "thread.metadata-updated",
              threadId: child,
              providerInstanceId: instanceId,
              occurredAt: now,
              payload: {
                ...projection.thread,
                lineage: {
                  parentThreadId: root,
                  rootThreadId: root,
                  relationshipToParent: "subagent",
                },
              },
            },
          ],
        });
      }).pipe(Effect.provide(runtime()));
      // Persist the request while no Orchestrator listener exists.
      yield* Effect.gen(function* () {
        const sink = yield* EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("offline-request"),
              type: "runtime-request.updated",
              threadId: child,
              providerInstanceId: instanceId,
              occurredAt: now,
              payload: {
                id: RuntimeRequestId.make("offline-question"),
                nodeId: NodeId.make("offline-node"),
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
          ],
        });
      }).pipe(Effect.provide(forkV1SnapshotLayer(filename)));
      yield* Effect.forEach(["first-recovery", "next-boot"], () =>
        Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          // Production startup invokes this recovery operation before workers.
          yield* orchestrator.recoverDelegatedTasks;
          const parent = yield* orchestrator.getThreadProjection(root);
          assert.lengthOf(parent.messages, 1);
          assert.include(parent.messages[0]!.text, "offline-question");
        }).pipe(Effect.provide(runtime())),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  "legacy-notice",
  "missing-native-wake",
  "failed-native-dispatch",
  "native-committed-before-legacy-cancel",
  "persisted-native-rejection",
] as const)("startup recovers a completed app-owned child's durable question after $0", (window) =>
  Effect.gen(function* () {
    const directory = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "chromeria-delegated-question-restart-"),
    );
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
    );
    const filename = NodePath.join(directory, "chromeria-v2.sqlite");
    const runtime = (providerAvailable = true) => {
      const database = SqlitePersistence.layerFromPath(filename).pipe(
        Layer.provide(NodeServices.layer),
      );
      return Layer.mergeAll(
        receiptLayer.pipe(Layer.provide(database)),
        Harness.layerWithRegistry(
          { name: `delegated-question-${window}` },
          Registry.layerFromAdapters(providerAvailable ? [adapter] : []),
          { databaseLayer: database, runEffectWorker: false },
        ),
      );
    };
    const root = ThreadId.make("restart-delegated-parent");
    const requestId = RuntimeRequestId.make("restart-delegated-question");
    const now = yield* DateTime.now;
    const { task, childRun } = yield* Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-delegated-parent"),
        threadId: root,
        projectId: ProjectId.make("project:delegated-question-restart"),
        title: "Parent",
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("start-delegated-parent"),
        threadId: root,
        messageId: MessageId.make("message:delegated-parent"),
        text: "Delegate the research",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const parentRun = (yield* orchestrator.getThreadProjection(root)).runs[0]!;
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("delegate-restart-question"),
        parentThreadId: root,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId!,
        task: "Research the sources",
        completionWake: "settled_only",
        modelSelection: { instanceId, model: "gpt-5.1-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
      });
      const task = (yield* orchestrator.getThreadProjection(root)).subagents[0]!;
      const childRun = (yield* orchestrator.getThreadProjection(task.childThreadId!)).runs[0]!;
      return { task, childRun };
    }).pipe(Effect.provide(runtime()));

    // Commit without live listeners: the child can finish while its message-answerable
    // question remains pending, or the process can die before the native wake commits.
    yield* Effect.gen(function* () {
      const sink = yield* EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("offline-delegated-question"),
            type: "runtime-request.updated",
            threadId: task.childThreadId!,
            runId: childRun.id,
            occurredAt: now,
            payload: {
              id: requestId,
              nodeId: childRun.rootNodeId!,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
          {
            id: EventId.make("offline-delegated-completed-run"),
            type: "run.updated",
            threadId: task.childThreadId!,
            runId: childRun.id,
            occurredAt: now,
            payload: { ...childRun, status: "completed", completedAt: now },
          },
          {
            id: EventId.make("offline-delegated-completed-task"),
            type: "subagent.updated",
            threadId: root,
            occurredAt: now,
            payload: { ...task, status: "completed", completedAt: now },
          },
        ],
      });
    }).pipe(Effect.provide(forkV1SnapshotLayer(filename)));

    const legacyMessageId = MessageId.make(
      `child-request:${task.childThreadId}:${requestId}:${root}`,
    );
    if (window !== "missing-native-wake" && window !== "persisted-native-rejection") {
      // The persisted pre-upgrade notice is queued behind the parent's active run.
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`command:${legacyMessageId}:0`),
          threadId: root,
          messageId: legacyMessageId,
          text: `Descendant ${task.childThreadId} needs an answer to ${requestId}. Read it with t3_pending_request_read and respond with t3_pending_request_respond.`,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "system",
          creationSource: "server",
        });
        const parent = yield* orchestrator.getThreadProjection(root);
        assert.equal(
          parent.runs.find((run) => run.userMessageId === legacyMessageId)?.status,
          "queued",
        );
      }).pipe(Effect.provide(runtime()));
    }

    if (window === "failed-native-dispatch" || window === "native-committed-before-legacy-cancel") {
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
        const request = child.runtimeRequests.find((request) => request.id === requestId)!;
        const result = yield* reconcileDelegatedTaskQuestion(task, request, (command) =>
          Effect.gen(function* () {
            if (
              (window === "failed-native-dispatch" && command.type === "message.dispatch") ||
              (window === "native-committed-before-legacy-cancel" &&
                command.type === "queued-run.cancel")
            )
              return yield* Effect.fail("simulated process loss during notice handoff");
            return yield* orchestrator.dispatch(command);
          }),
        ).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        const parent = yield* orchestrator.getThreadProjection(root);
        assert.equal(
          parent.runs.find((run) => run.userMessageId === legacyMessageId)?.status,
          "queued",
        );
        assert.equal(
          parent.messages.filter(
            (message) => message.notification?.source.kind === "delegated_task",
          ).length,
          window === "failed-native-dispatch" ? 0 : 1,
        );
      }).pipe(Effect.provide(runtime()));
    }

    if (window === "persisted-native-rejection") {
      // A real planning failure commits a rejected receipt. Restore the provider
      // in the next runtime, rather than throwing before the dispatcher runs.
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const receipts = yield* CommandReceiptStoreV2;
        const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
        const request = child.runtimeRequests.find((request) => request.id === requestId)!;
        const result = yield* reconcileDelegatedTaskQuestion(
          task,
          request,
          orchestrator.dispatch,
        ).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        const receipt = yield* receipts.getByCommandId(
          CommandId.make(`server:delegated-task-question:${root}:${requestId}`),
        );
        assert.isTrue(Option.isSome(receipt));
        if (Option.isSome(receipt)) assert.equal(receipt.value.status, "rejected");
        assert.isFalse(
          (yield* orchestrator.getThreadProjection(root)).messages.some((message) =>
            message.text.includes(requestId),
          ),
        );
      }).pipe(Effect.provide(runtime(false)));
    }

    for (const boot of ["first", "repeated"] as const) {
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        // The production persisted startup seam runs before the effect worker.
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        const parent = yield* orchestrator.getThreadProjection(root);
        const actionable = parent.messages.filter(
          (message) =>
            message.text.includes(requestId) &&
            parent.runs.some((run) => run.userMessageId === message.id && run.status === "queued"),
        );
        assert.lengthOf(
          actionable,
          1,
          `${window}: ${boot} startup must retain one actionable notice`,
        );
        assert.include(actionable[0]!.text, "t3_pending_request_read");
        assert.include(actionable[0]!.text, "t3_pending_request_respond");
        assert.equal(
          actionable[0]!.id,
          MessageId.make(`message:delegated-task-question:${root}:${requestId}`),
        );
        assert.lengthOf(
          parent.runs.filter((run) => run.userMessageId === actionable[0]!.id),
          1,
        );
        if (window === "persisted-native-rejection") {
          const receipts = yield* CommandReceiptStoreV2;
          const original = yield* receipts.getByCommandId(
            CommandId.make(`server:delegated-task-question:${root}:${requestId}`),
          );
          assert.isTrue(Option.isSome(original));
          if (Option.isSome(original)) assert.equal(original.value.status, "rejected");
        }
        assert.deepStrictEqual(actionable[0]!.notification?.source, {
          kind: "delegated_task",
          taskIds: [task.id],
          childThreadId: task.childThreadId!,
        });
        assert.isFalse(
          parent.runs.some(
            (run) => run.userMessageId === legacyMessageId && run.status === "queued",
          ),
        );
        const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
        assert.equal(
          child.runtimeRequests.find((request) => request.id === requestId)?.status,
          "pending",
        );
        assert.equal(child.runs.find((run) => run.id === childRun.id)?.status, "completed");
      }).pipe(Effect.provide(runtime()));
    }

    // Lifecycle changes are committed without listeners, too: the next startup
    // must discard the stale native notice instead of recovering it again.
    yield* Effect.gen(function* () {
      const sink = yield* EventSinkV2;
      const orchestrator = yield* OrchestratorV2;
      const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
      const request = child.runtimeRequests.find((request) => request.id === requestId)!;
      yield* sink.write({
        events: [
          window === "legacy-notice"
            ? {
                id: EventId.make("answered-delegated-question"),
                type: "runtime-request.updated",
                threadId: task.childThreadId!,
                runId: childRun.id,
                occurredAt: now,
                payload: { ...request, status: "resolved", resolvedAt: now },
              }
            : window === "native-committed-before-legacy-cancel"
              ? {
                  id: EventId.make("disposed-delegated-question"),
                  type: "subagent.updated",
                  threadId: root,
                  occurredAt: now,
                  payload: {
                    ...task,
                    completionDelivery: { state: "disposed", observedByRunId: null },
                  },
                }
              : {
                  id: EventId.make("retired-delegated-question"),
                  type: "thread.metadata-updated",
                  threadId: task.childThreadId!,
                  occurredAt: now,
                  payload:
                    window === "missing-native-wake"
                      ? { ...child.thread, archivedAt: now }
                      : {
                          ...child.thread,
                          forkRetirement: { token: CommandId.make("retired-question") },
                        },
                },
        ],
      });
    }).pipe(Effect.provide(runtime()));
    yield* Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      yield* orchestrator.recoverDelegatedTasks;
      yield* orchestrator.recoverDelegatedTasks;
      const parent = yield* orchestrator.getThreadProjection(root);
      assert.isFalse(
        parent.messages.some(
          (message) =>
            message.text.includes(requestId) &&
            parent.runs.some((run) => run.userMessageId === message.id && run.status === "queued"),
        ),
      );
    }).pipe(Effect.provide(runtime()));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
