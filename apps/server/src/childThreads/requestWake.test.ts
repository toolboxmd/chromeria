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
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
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
} as ProviderAdapterV2Shape;

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

it.effect("committed ancestor resume delivers one previously suppressed live request", () =>
  Effect.gen(function* () {
    const orchestrator = yield* OrchestratorV2;
    const sink = yield* EventSinkV2;
    const receipts = yield* CommandReceiptStoreV2;
    const now = yield* DateTime.now;
    const root = ThreadId.make("resume-root");
    const child = ThreadId.make("resume-child");
    const controlRoot = ThreadId.make("control-root");
    const controlChild = ThreadId.make("control-child");
    for (const id of [root, child, controlRoot, controlChild]) {
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
    for (const [id, parentId] of [
      [child, root],
      [controlChild, controlRoot],
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
    yield* resume(root, "root");
    yield* drainListener("resumed");
    const receipt = yield* receipts.getByCommandId(wakeCommandId);
    assert.isTrue(Option.isSome(receipt));
    if (Option.isSome(receipt)) assert.equal(receipt.value.status, "accepted");
    yield* resume(root, "root-again");
    yield* drainListener("deduplicated");
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
