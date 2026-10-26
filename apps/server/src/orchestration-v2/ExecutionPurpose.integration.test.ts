import { ProviderThreadId, type NativeExecutionSegment } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as EventSink from "./EventSink.ts";
import { EventId, NodeId, type OrchestrationV2Run } from "@t3tools/contracts";
import { withCreationProvenance } from "./ThreadManagementService.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type PurposeIntent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as BareLaunch from "./BareLaunch.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-6.1-sol" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Admission proof must never start a provider"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "execution-purpose" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);
const referenceA = {
  host: "github.com",
  repository: "example/repo",
  kind: "issue",
  number: 11,
} as const;
const referenceB = {
  host: "github.com",
  repository: "example/repo",
  kind: "issue",
  number: 12,
} as const;
const scopedA = { type: "single", reference: referenceA } as const;
const scopedB = { type: "single", reference: referenceB } as const;

const createThread = (threadId: ThreadId) =>
  Orchestrator.OrchestratorV2.pipe(
    Effect.flatMap((orchestrator) =>
      orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${threadId}`),
        threadId,
        projectId: ProjectId.make("project:purpose"),
        title: "Purpose admission fixture",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      }),
    ),
  );

function message(
  threadId: ThreadId,
  id: string,
  purposeIntent?: PurposeIntent,
): Extract<OrchestrationV2Command, { type: "message.dispatch" }> {
  return {
    type: "message.dispatch",
    commandId: CommandId.make(id),
    threadId,
    messageId: MessageId.make(`message:${id}`),
    text: "isolated purpose fixture",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    dispatchMode: { type: "defer_start" },
    ...(purposeIntent === undefined ? {} : { purposeIntent }),
  };
}

it.effect("pins A and B at admission; unnamed web/mobile Steer keeps A and the B selection", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    for (const source of ["web", "mobile"] as const) {
      const threadId = ThreadId.make(`thread:purpose-${source}`);
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, `first-${source}`, {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make(`select-b-${source}`),
        threadId,
        purposeSelection: { expectedRevision: 1, scope: scopedB },
      });
      yield* orchestrator.dispatch({
        ...message(threadId, `send-b-${source}`, {
          version: 1,
          intent: { type: "selection", expectedRevision: 2 },
        }),
        creationSource: source,
        deliveryIntent: "auto",
        dispatchMode: { type: "start_immediately" },
      });
      yield* orchestrator.dispatch({
        ...message(threadId, `steer-a-${source}`, {
          version: 1,
          intent: { type: "target" },
        }),
        creationSource: source,
        deliveryIntent: "steer",
        dispatchMode: { type: "start_immediately" },
      });
      const projection = yield* projections.getThreadProjection(threadId);
      assert.deepEqual(
        projection.runs.map((run) => [run.status, run.purpose?.scope]),
        [
          ["preparing", scopedA],
          ["queued", scopedB],
          ["queued", scopedA],
        ],
      );
      assert.deepEqual(projection.thread.purposeSelection, { revision: 2, scope: scopedB });
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect("late web/mobile Steer admits one ordinary next message after the run ends", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    for (const [source, legacy] of [
      ["web", false],
      ["mobile", false],
      ["web", true],
      ["mobile", true],
    ] as const) {
      const id = `${source}-${legacy ? "legacy" : "current"}`;
      const threadId = ThreadId.make(`thread:late-steer-${id}`);
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, `initial-${id}`, {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      const initial = (yield* projections.getThreadProjection(threadId)).runs[0]!;
      const lateSteer = {
        ...message(threadId, `late-steer-${id}`, {
          version: 1,
          intent: { type: "target" },
        }),
        creationSource: source,
        ...(legacy ? {} : { deliveryIntent: "steer" as const }),
        dispatchMode: legacy
          ? { type: "steer_active" as const, targetRunId: initial.id }
          : { type: "start_immediately" as const },
      };
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make(`late-steer-select-b-${id}`),
        threadId,
        purposeSelection: { expectedRevision: 1, scope: scopedB },
      });
      const now = yield* DateTime.now;
      yield* sink.write({
        commandId: CommandId.make(`fixture:late-steer-end-${id}`),
        events: [
          {
            id: EventId.make(`event:late-steer-end-${id}`),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: { ...initial, status: "completed", startedAt: now, completedAt: now },
          },
        ],
      });
      const admitted = yield* orchestrator.dispatch(lateSteer);
      assert.equal((yield* orchestrator.dispatch(lateSteer)).sequence, admitted.sequence);
      const projection = yield* projections.getThreadProjection(threadId);
      assert.equal(projection.messages.filter((m) => m.id === lateSteer.messageId).length, 1);
      const item = projection.turnItems.find(
        (item) => item.type === "user_message" && item.messageId === lateSteer.messageId,
      );
      assert.isDefined(item);
      assert.equal(item?.type === "user_message" ? item.inputIntent : undefined, "turn_start");
      assert.equal(projection.runs.length, 2);
      assert.equal(projection.runs[1]?.status, "starting");
      assert.deepEqual(projection.runs[1]?.purpose, {
        version: 1,
        scope: scopedB,
        provenance: { type: "selection", revision: 2 },
      });
      const effects = yield* sql<{ count: number }>`SELECT count(*) AS count
        FROM orchestration_v2_effect_outbox WHERE command_id = ${lateSteer.commandId}
        AND effect_type = 'provider-turn.start'`;
      assert.equal(effects[0]?.count, 1);
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect("an idle Steer starts a message but Restart still requires active work", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make("thread:idle-steer");
    yield* createThread(threadId);
    assert.equal(
      (yield* Effect.exit(
        orchestrator.dispatch({
          ...message(threadId, "idle-restart", { version: 1, intent: { type: "target" } }),
          deliveryIntent: "restart",
          dispatchMode: { type: "start_immediately" },
        }),
      ))._tag,
      "Failure",
    );
    const admitted = yield* orchestrator.dispatch({
      ...message(threadId, "idle-steer", { version: 1, intent: { type: "target" } }),
      creationSource: "mobile",
      deliveryIntent: "steer",
      dispatchMode: { type: "start_immediately" },
    });
    assert.deepEqual(admitted.admittedRun?.purpose, {
      version: 1,
      scope: { type: "unscoped", reason: "deliberate" },
      provenance: { type: "selection", revision: 0 },
    });
    assert.equal((yield* orchestrator.getThreadProjection(threadId)).runs.length, 1);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "explicit PR deliveries preserve outcome identity after linking, replay and unlinking",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:purpose-delivery");
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, "before-pr", {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      const before = yield* projections.getThreadProjection(threadId);
      const link = {
        type: "thread.pull-request.link" as const,
        commandId: CommandId.make("navigation-link"),
        threadId,
        host: "github.com",
        repository: "delivery/repo",
        number: 101,
        url: "https://github.com/delivery/repo/pull/101",
        source: "agent" as const,
      };
      yield* orchestrator.dispatch(link);
      assert.isUndefined(
        (yield* projections.getThreadProjection(threadId)).thread.purposeDeliveries,
        "navigation alone does not declare delivery or ownership",
      );
      for (const [id, purpose] of [
        ["deliver-a", referenceA],
        ["deliver-a-again", referenceA],
        ["deliver-b", referenceB],
      ] as const) {
        yield* orchestrator.dispatch({
          ...link,
          commandId: CommandId.make(id),
          purposeDelivery: { version: 1 as const, purpose },
        });
      }
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("unlink-delivery-navigation"),
        threadId,
        host: link.host,
        repository: link.repository,
        number: link.number,
      });
      const after = yield* projections.getThreadProjection(threadId);
      const declaredAt = DateTime.formatIso(yield* DateTime.now);
      const expected = [referenceA, referenceB].map((purpose) => ({
        version: 1 as const,
        purpose,
        pullRequest: {
          host: "github.com",
          repository: "delivery/repo",
          kind: "pr" as const,
          number: 101,
        },
        declaredAt,
      }));
      assert.deepEqual(after.thread.purposeDeliveries, expected);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.thread.purposeSelection, before.thread.purposeSelection);
      assert.deepEqual(after.thread.pullRequests, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "stale choice and incompatible target assertion commit no message, run or start effect",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:purpose-rejected");
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, "purpose-initial", {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      const before = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM orchestration_v2_effect_outbox`;
      for (const rejected of [
        message(threadId, "purpose-stale", {
          version: 1,
          intent: { type: "selection", expectedRevision: 0 },
        }),
        {
          ...message(threadId, "purpose-mismatch", {
            version: 1,
            intent: { type: "reference", reference: referenceB },
          }),
          deliveryIntent: "steer" as const,
          dispatchMode: { type: "start_immediately" as const },
        },
      ]) {
        assert.equal((yield* Effect.exit(orchestrator.dispatch(rejected)))._tag, "Failure");
      }
      const after = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM orchestration_v2_effect_outbox`;
      assert.deepEqual(after, before);
      const projection = yield* projections.getThreadProjection(threadId);
      assert.equal(projection.runs.length, 1);
      assert.equal(projection.messages.length, 1);
      assert.deepEqual(projection.thread.purposeSelection, { revision: 1, scope: scopedA });
    }).pipe(Effect.provide(layerTest)),
);

it.effect("legacy omission stays warned unscoped; a replay cannot change declared intent", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:purpose-legacy");
    yield* createThread(threadId);
    const first = message(threadId, "purpose-replay", {
      version: 1,
      intent: { type: "reference", reference: referenceA },
    });
    const original = yield* orchestrator.dispatch(first);
    assert.equal((yield* orchestrator.dispatch(first)).sequence, original.sequence);
    assert.equal(
      (yield* Effect.exit(
        orchestrator.dispatch({
          ...first,
          purposeIntent: { version: 1, intent: { type: "reference", reference: referenceB } },
        }),
      ))._tag,
      "Failure",
    );
    yield* orchestrator.dispatch(message(threadId, "legacy-omission"));
    const projection = yield* projections.getThreadProjection(threadId);
    assert.deepEqual(projection.runs[1]?.purpose?.scope, {
      type: "unscoped",
      reason: "legacy_no_intent",
    });
    assert.deepEqual(projection.thread.purposeSelection, { revision: 1, scope: scopedA });
    const idle = ThreadId.make("thread:legacy-mobile-idle");
    yield* createThread(idle);
    const legacyMobile = yield* orchestrator.dispatch({
      ...message(idle, "legacy-mobile-auto"),
      creationSource: "mobile",
      deliveryIntent: "auto",
    });
    assert.deepEqual(legacyMobile.admittedRun?.purpose.scope, {
      type: "unscoped",
      reason: "legacy_no_intent",
    });
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "a causal command replay retains its admitted source and refuses a changed source purpose",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:causal-purpose-replay");
      yield* createThread(threadId);
      const sourceA = { version: 1, scope: scopedA, provenance: { type: "declared" } } as const;
      const first = {
        ...message(threadId, "causal-purpose-replay", { version: 1, intent: { type: "inherit" } }),
        createdBy: "system" as const,
        creationSource: "server" as const,
        purposeSource: sourceA,
      };
      const original = yield* orchestrator.dispatch(first);
      const before = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM orchestration_v2_effect_outbox`;
      assert.deepEqual((yield* orchestrator.dispatch(first)).admittedRun, original.admittedRun);
      const changed = yield* Effect.exit(
        orchestrator.dispatch({
          ...first,
          purposeSource: { ...sourceA, scope: scopedB },
        }),
      );
      assert.equal(changed._tag, "Failure");
      const after = yield* sql<{
        count: number;
      }>`SELECT count(*) AS count FROM orchestration_v2_effect_outbox`;
      assert.deepEqual(after, before);
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).runs.map((run) => run.purpose?.scope),
        [scopedA],
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "saved A schedules ignore the B chip; old configurations fire legacy and new saves require intent",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const schedules = yield* ScheduledTasks.ScheduledTaskService;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:schedule-purpose");
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, "schedule-active-a", {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("schedule-select-b"),
        threadId,
        purposeSelection: { expectedRevision: 1, scope: scopedB },
      });
      const input = {
        title: "Isolated purpose schedule",
        prompt: "Scheduled fixture",
        enabled: true,
        schedule: { type: "interval", everyMs: 60_000 } as const,
        projectId: ProjectId.make("project:purpose"),
        threadId,
        workspaceStrategy: { type: "root" } as const,
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
      };
      assert.equal((yield* Effect.exit(schedules.upsert(input)))._tag, "Failure");
      assert.isEmpty((yield* schedules.list()).tasks);
      const { task } = yield* schedules.upsert({
        ...input,
        purposeIntent: { version: 1, intent: { type: "reference", reference: referenceA } },
      });
      const fired = yield* schedules.runNow({ id: task.id });
      assert.equal(fired.task.lastRunStatus, "succeeded");
      const first = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(first.runs[1]?.purpose, {
        version: 1,
        scope: scopedA,
        provenance: { type: "schedule", scheduledTaskId: task.id },
      });
      assert.deepEqual(first.thread.purposeSelection, { revision: 2, scope: scopedB });
      // A nullable column is the pre-contract persisted configuration, not a new save.
      yield* sql`UPDATE scheduled_tasks SET purpose_intent_json = NULL WHERE task_id = ${task.id}`;
      yield* TestClock.adjust(1);
      yield* schedules.runNow({ id: task.id });
      const legacy = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(legacy.runs[2]?.purpose?.scope, {
        type: "unscoped",
        reason: "legacy_no_intent",
      });
      yield* schedules.setEnabled({ id: task.id, enabled: false });
      assert.equal(
        (yield* Effect.exit(schedules.setEnabled({ id: task.id, enabled: true })))._tag,
        "Failure",
      );
      assert.equal(legacy.runs.length, 3);
    }).pipe(
      Effect.provide(
        ScheduledTasks.layer.pipe(
          Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
          Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
          Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
          Layer.provide(
            Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
              namedProjectsRoot: "/projects",
              isScratchProject: () => Effect.succeed(false),
            }),
          ),
          Layer.provide(Scheduler.layer),
          Layer.provide(NodeCrypto.layer),
          Layer.provideMerge(layerTest),
        ),
      ),
    ),
);

it.effect(
  "watch wakes retain saved purpose, legacy watches stay unscoped and purpose edits invalidate old reads",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      for (const legacy of [false, true]) {
        const threadId = ThreadId.make(`thread:watch-purpose-${legacy}`);
        yield* createThread(threadId);
        const watchCommand = {
          type: "thread.pull-request.watch",
          threadId,
          host: "github.com",
          repository: "example/repo",
          number: 350,
          watching: true,
          link: { url: "https://github.com/example/repo/pull/350", source: "agent" },
        } as const;
        // An old client or agent omits intent: the watch starts, warned legacy unscoped.
        yield* orchestrator.dispatch({
          ...watchCommand,
          commandId: CommandId.make(`watch-no-intent-${legacy}`),
        });
        const omitted = (yield* orchestrator.getThreadProjection(threadId)).thread.pullRequests![0]!
          .watch!;
        assert.isUndefined(omitted.purposeIntent);
        yield* orchestrator.dispatch({
          ...watchCommand,
          commandId: CommandId.make(`watch-save-a-${legacy}`),
          purposeIntent: { version: 1, intent: { type: "reference", reference: referenceA } },
        });
        let projection = yield* orchestrator.getThreadProjection(threadId);
        // Re-watching without intent keeps the saved purpose and its generation.
        const savedA = projection.thread.pullRequests![0]!.watch!;
        yield* orchestrator.dispatch({
          ...watchCommand,
          commandId: CommandId.make(`watch-again-no-intent-${legacy}`),
        });
        projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          projection.thread.pullRequests![0]!.watch!.purposeIntent,
          savedA.purposeIntent,
        );
        assert.equal(projection.thread.pullRequests![0]!.watch!.startedAt, savedA.startedAt);
        if (legacy) {
          const links = projection.thread.pullRequests!.map((link) => {
            const { purposeIntent: _intent, ...oldWatch } = link.watch!;
            return { ...link, watch: oldWatch };
          });
          yield* sink.write({
            events: [
              {
                id: EventId.make("legacy-watch-fixture"),
                type: "thread.metadata-updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...projection.thread, pullRequests: links },
              },
            ],
          });
          projection = yield* orchestrator.getThreadProjection(threadId);
        }
        const saved = projection.thread.pullRequests![0]!.watch!;
        const { purposeIntent: _savedIntent, ...progress } = saved;
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`watch-chip-b-${legacy}`),
          threadId,
          purposeSelection: { expectedRevision: 0, scope: scopedB },
        });
        const sync = {
          type: "thread.pull-request-watch.sync",
          threadId,
          host: "github.com",
          repository: "example/repo",
          number: 350,
          startedAt: saved.startedAt,
          watch: { ...progress, wakes: 1 },
          wake: {
            messageId: MessageId.make(`watch-wake-${legacy}`),
            text: "Watch fixture",
            notification: {
              source: { kind: "monitor" },
              outcome: "updated",
              summary: "Isolated watch",
            },
          },
        } as const;
        yield* orchestrator.dispatch({
          ...sync,
          commandId: CommandId.make(`watch-fire-${legacy}`),
        });
        const fired = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          fired.runs[0]?.purpose?.scope,
          legacy ? { type: "unscoped", reason: "legacy_no_intent" } : scopedA,
        );
        assert.deepEqual(fired.thread.purposeSelection, { revision: 1, scope: scopedB });
        assert.deepEqual(fired.thread.pullRequests![0]?.watch?.purposeIntent, saved.purposeIntent);
        if (!legacy) {
          yield* orchestrator.dispatch({
            ...watchCommand,
            commandId: CommandId.make("watch-save-b"),
            purposeIntent: { version: 1, intent: { type: "reference", reference: referenceB } },
          });
          const edited = yield* orchestrator.getThreadProjection(threadId);
          assert.notEqual(edited.thread.pullRequests![0]?.watch?.startedAt, saved.startedAt);
          assert.equal(
            (yield* Effect.exit(
              orchestrator.dispatch({
                ...sync,
                commandId: CommandId.make("watch-stale-read"),
                wake: { ...sync.wake, messageId: MessageId.make("watch-stale-message") },
              }),
            ))._tag,
            "Failure",
          );
          assert.equal((yield* orchestrator.getThreadProjection(threadId)).runs.length, 1);
        }
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a delegated child inherits a deliberately unscoped parent run exactly", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const parentId = ThreadId.make("thread:unscoped-inherit-parent");
    yield* createThread(parentId);
    yield* orchestrator.dispatch(
      message(parentId, "unscoped-inherit-parent", { version: 1, intent: { type: "unscoped" } }),
    );
    const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
    const parentNodeId = NodeId.make("node:unscoped-inherit-parent");
    yield* sink.write({
      commandId: CommandId.make("fixture:activate-unscoped-inherit-parent"),
      events: [
        {
          id: EventId.make("event:unscoped-inherit-parent-run"),
          type: "run.updated",
          threadId: parentId,
          occurredAt: now,
          payload: { ...parentRun, rootNodeId: parentNodeId, status: "running", startedAt: now },
        },
        {
          id: EventId.make("event:unscoped-inherit-parent-node"),
          type: "node.updated",
          threadId: parentId,
          occurredAt: now,
          payload: {
            id: parentNodeId,
            threadId: parentId,
            runId: parentRun.id,
            parentNodeId: null,
            rootNodeId: parentNodeId,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
      ],
    });
    yield* orchestrator.dispatch({
      type: "delegated_task.request",
      commandId: CommandId.make("unscoped-inherit-delegate"),
      parentThreadId: parentId,
      parentRunId: parentRun.id,
      parentNodeId,
      task: "isolated child fixture",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      createdBy: "agent",
      creationSource: "mcp",
      completionWake: "always",
      purposeIntent: { version: 1, intent: { type: "inherit" } },
    });
    const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
    const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
    assert.deepEqual(child.runs[0]?.purpose, {
      version: 1,
      scope: { type: "unscoped", reason: "deliberate" },
      provenance: { type: "parent_run", threadId: parentId, runId: parentRun.id },
    });
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "an unscoped parent's continuation keeps launch A; human B keeps the job and notifies with B",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const parentId = ThreadId.make("thread:purpose-parent");
      yield* createThread(parentId);
      yield* orchestrator.dispatch(
        message(parentId, "parent-unscoped", { version: 1, intent: { type: "unscoped" } }),
      );
      const parent = yield* orchestrator.getThreadProjection(parentId);
      const parentRun = parent.runs[0]!;
      const parentNodeId = NodeId.make("node:purpose-parent");
      yield* sink.write({
        commandId: CommandId.make("fixture:activate-purpose-parent"),
        events: [
          {
            id: EventId.make("event:purpose-parent-run"),
            type: "run.updated",
            threadId: parentId,
            occurredAt: now,
            payload: { ...parentRun, rootNodeId: parentNodeId, status: "running", startedAt: now },
          },
          {
            id: EventId.make("event:purpose-parent-node"),
            type: "node.updated",
            threadId: parentId,
            occurredAt: now,
            payload: {
              id: parentNodeId,
              threadId: parentId,
              runId: parentRun.id,
              parentNodeId: null,
              rootNodeId: parentNodeId,
              kind: "root_turn",
              status: "running",
              countsForRun: true,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: now,
              completedAt: null,
            },
          },
        ],
      });
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make("purpose-delegate-a"),
        parentThreadId: parentId,
        parentRunId: parentRun.id,
        parentNodeId,
        task: "isolated child fixture",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
        completionWake: "always",
        purposeIntent: { version: 1, intent: { type: "reference", reference: referenceA } },
      });
      const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
      assert.isNotNull(task.childThreadId);
      const childId = task.childThreadId!;
      const complete = (run: OrchestrationV2Run) =>
        sink.write({
          commandId: CommandId.make(`fixture:end:${run.id}`),
          events: [
            {
              id: EventId.make(`event:end:${run.id}`),
              type: "run.updated",
              threadId: childId,
              occurredAt: now,
              payload: { ...run, status: "completed", startedAt: now, completedAt: now },
            },
          ],
        });
      const initial = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
      assert.deepEqual(initial.purpose?.scope, scopedA);
      assert.equal(initial.delegatedTaskId, task.id);
      yield* complete(initial);
      yield* orchestrator.recoverDelegatedTask(childId, initial.id);
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("child-select-b"),
        threadId: childId,
        purposeSelection: { expectedRevision: 1, scope: scopedB },
      });
      yield* orchestrator.dispatch(
        message(childId, "human-b", {
          version: 1,
          intent: { type: "selection", expectedRevision: 2 },
        }),
      );
      const human = (yield* orchestrator.getThreadProjection(childId)).runs.at(-1)!;
      assert.deepEqual(human.purpose?.scope, scopedB);
      assert.equal(human.delegatedTaskId, task.id);
      yield* complete(human);
      yield* orchestrator.recoverDelegatedTask(childId, human.id);
      const notified = yield* orchestrator.getThreadProjection(parentId);
      assert.deepEqual(notified.subagents.find((row) => row.id === task.id)?.completionSource, {
        runId: human.id,
        purpose: human.purpose,
      });
      const delivery = notified.runs.find((run) => run.id === parentRun.id)?.delegatedCompletion
        ?.delivery;
      assert.isNotNull(delivery);
      yield* orchestrator.dispatch({
        ...message(parentId, "completion-b"),
        messageId: delivery!.messageId,
        createdBy: "agent",
        creationSource: "server",
        dispatchMode: { type: "queue_after_active" },
        delegatedCompletion: {
          parentRunId: parentRun.id,
          generation: delivery!.generation,
          taskIds: delivery!.taskIds,
        },
      });
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(parentId)).runs.at(-1)?.purpose?.scope,
        scopedB,
      );
      yield* orchestrator.dispatch({
        ...message(childId, "parent-continue-a"),
        createdBy: "agent",
        creationSource: "mcp",
        senderThreadId: parentId,
      });
      const continued = yield* orchestrator.getThreadProjection(childId);
      assert.deepEqual(continued.runs.at(-1)?.purpose?.scope, scopedA);
      assert.equal(continued.runs.at(-1)?.delegatedTaskId, task.id);
      assert.deepEqual(continued.thread.purposeSelection, { revision: 2, scope: scopedB });
      // A browser cannot spoof the authenticated parent continuation through senderThreadId.
      const forged = withCreationProvenance(
        {
          ...message(childId, "forged-parent", { version: 1, intent: { type: "inherit" } }),
          createdBy: "agent",
          creationSource: "mcp",
          senderThreadId: parentId,
        },
        { createdBy: "user", creationSource: "web" },
      );
      assert.equal((yield* Effect.exit(orchestrator.dispatch(forged)))._tag, "Failure");
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "memory replay and SQL projection reject a forged purpose change without committing it",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:purpose-immutable");
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, "immutable-a", {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      const original = yield* orchestrator.getThreadProjection(threadId);
      const run = original.runs[0]!;
      const event = {
        id: EventId.make("event:forged-purpose"),
        type: "run.updated" as const,
        threadId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...run,
          purpose: {
            version: 1 as const,
            scope: scopedB,
            provenance: { type: "declared" as const },
          },
        },
      };
      assert.throws(() => ProjectionStore.applyToProjection(original, event));
      assert.equal(
        (yield* Effect.exit(
          sink.write({ commandId: CommandId.make("forged-purpose"), events: [event] }),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).runs[0]?.purpose,
        run.purpose,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "retains native participation and serializes its timestamp after roster removal and stale updates",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:native-inventory");
      yield* createThread(threadId);
      yield* orchestrator.dispatch(
        message(threadId, "native-inventory-a", {
          version: 1,
          intent: { type: "reference", reference: referenceA },
        }),
      );
      const original = yield* orchestrator.getThreadProjection(threadId);
      const run = original.runs[0]!;
      const operation: NativeExecutionSegment = {
        version: 1,
        id: "native-session:command-1",
        nativeSessionId: "native-session",
        nativeOperationId: "command-1",
        kind: "background",
        source: { type: "app_run", threadId, runId: run.id },
        purpose: run.purpose!,
        boundary: { state: "unresolved", reason: "native_usage_absent" },
        status: "ended",
        usageFinality: "unreconciled",
        noticeIds: ["native-notice-1"],
        wakeDispositions: ["consumed_in_active"],
        observedAt: DateTime.formatIso(now),
      };
      const providerThread = {
        id: ProviderThreadId.make("provider:native-inventory"),
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "native-session",
          strength: "strong" as const,
        },
        nativeConversationHeadRef: null,
        status: "idle" as const,
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: now,
        updatedAt: now,
      };
      const first = {
        id: EventId.make("event:native-inventory"),
        type: "provider-thread.updated" as const,
        threadId,
        occurredAt: now,
        payload: { ...providerThread, nativeOperations: [operation] },
      };
      const stale = {
        ...first,
        id: EventId.make("event:native-inventory-stale"),
        payload: providerThread,
      };
      const staleRunning = {
        ...stale,
        id: EventId.make("event:native-inventory-stale-running"),
        payload: {
          ...providerThread,
          nativeOperations: [
            { ...operation, status: "running" as const, noticeIds: [], wakeDispositions: [] },
          ],
        },
      };
      const memory = ProjectionStore.applyToProjection(
        ProjectionStore.applyToProjection(
          ProjectionStore.applyToProjection(original, first),
          stale,
        ),
        staleRunning,
      );
      assert.deepEqual(
        memory.providerThreads.find((thread) => thread.id === providerThread.id)?.nativeOperations,
        [operation],
      );
      yield* sink.write({
        commandId: CommandId.make("native-inventory"),
        events: [first, stale, staleRunning],
      });
      const stored = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        stored.providerThreads.find((thread) => thread.id === providerThread.id)?.nativeOperations,
        [operation],
      );
      const rows = yield* sql<{
        observed: string;
      }>`SELECT json_extract(payload_json, '$.nativeOperations[0].observedAt') AS observed FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${providerThread.id}`;
      assert.equal(rows[0]?.observed, "1970-01-01T00:00:00.000Z");
      const forged = {
        ...first,
        id: EventId.make("event:native-inventory-forged"),
        payload: {
          ...providerThread,
          nativeOperations: [{ ...operation, purpose: { ...operation.purpose, scope: scopedB } }],
        },
      };
      assert.equal(
        (yield* Effect.exit(
          sink.write({ commandId: CommandId.make("native-inventory-forged"), events: [forged] }),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).providerThreads.find(
          (thread) => thread.id === providerThread.id,
        )?.nativeOperations,
        [operation],
      );
    }).pipe(Effect.provide(layerTest)),
);
