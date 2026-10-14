import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Settings from "../serverSettings.ts";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider worker"),
} satisfies ProviderAdapterV2["Service"];
const database = SqlitePersistence.layerMemory;
const layer = Harness.layerWithRegistry(
  { name: "cross-project" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
).pipe(Layer.provideMerge(Layer.merge(database, Receipts.layer.pipe(Layer.provide(database)))));

it.effect(
  "native cross-project admission refuses a disabled destination while preserving the enabled parent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const parentId = ThreadId.make("disabled-destination:parent");
      const parentProjectId = ProjectId.make("disabled-destination:parent-project");
      const destinationId = ProjectId.make("disabled-destination:project");
      const orchestrator = yield* OrchestratorV2;
      yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,default_thread_env_mode,created_at,updated_at)
      VALUES (${destinationId},'Destination','/tmp/destination','[]','worktree','2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.000Z')`;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("disabled-destination:create"),
        threadId: parentId,
        projectId: parentProjectId,
        title: "Parent",
        modelSelection,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("disabled-destination:parent-work"),
        messageId: MessageId.make("disabled-destination:parent-work"),
        threadId: parentId,
        text: "Parent work with its own model and kit.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const parentRun = (yield* orchestrator.getThreadProjection(parentId)).runs[0]!;
      const request = {
        type: "delegated_task.request" as const,
        commandId: CommandId.make("disabled-destination:delegate"),
        parentThreadId: parentId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId!,
        projectId: destinationId,
        task: "Destination work using the parent's selected kit.",
        modelSelection,
        runtimeMode: "approval-required" as const,
        interactionMode: "plan" as const,
        completionWake: "always" as const,
        createdBy: "agent" as const,
        creationSource: "mcp" as const,
      };
      const gate = Layer.fresh(Threads.layer).pipe(
        Layer.provideMerge(
          Settings.layerTest({
            providerInstances: { codex: { driver: "codex", enabled: true } },
            projectSettingsOverrides: {
              [destinationId]: { providerInstances: { codex: { enabled: false } } },
            },
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const threads = yield* Threads.ThreadManagementService;
        assert.isTrue(
          yield* threads.isProviderInstanceEnabledForProject({
            projectId: parentProjectId,
            instanceId,
          }),
        );
        assert.isFalse(
          yield* threads.isProviderInstanceEnabledForProject({
            projectId: destinationId,
            instanceId,
          }),
        );
        const refused = yield* threads.dispatch(request).pipe(Effect.flip);
        assert.instanceOf(refused, Threads.ThreadManagementProviderInstanceDisabledError);
        if (refused._tag !== "ThreadManagementProviderInstanceDisabledError") return;
        assert.strictEqual(refused.projectId, destinationId);
        assert.deepStrictEqual((yield* orchestrator.getThreadProjection(parentId)).subagents, []);
        assert.deepStrictEqual(
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${destinationId}`,
          [],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id=${request.commandId}`,
          [],
        );
      }).pipe(Effect.provide(gate));
      // Re-enabling the destination admits the unchanged request, with the parent's model and permission ceiling.
      yield* (yield* Threads.ThreadManagementService).dispatch(request);
      const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
      const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
      assert.strictEqual(child.thread.projectId, destinationId);
      assert.deepStrictEqual(child.thread.modelSelection, modelSelection);
      assert.strictEqual(child.thread.runtimeMode, "approval-required");
      assert.strictEqual(child.thread.lineage.parentThreadId, parentId);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "native cross-project delegation keeps ancestry and prepares the destination workspace, refusing missing projects",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const orchestrator = yield* OrchestratorV2;
      const parentId = ThreadId.make("parent");
      const destinationId = ProjectId.make("destination");
      yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, default_thread_env_mode, created_at, updated_at)
      VALUES (${destinationId}, 'Destination', '/tmp/destination', '[]', 'worktree', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:parent"),
        threadId: parentId,
        projectId: ProjectId.make("project:cross-project"),
        title: "Parent",
        modelSelection,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        branch: "parent-branch",
        worktreePath: "/tmp/parent-worktree",
        createdBy: "user",
        creationSource: "web",
        owner: "Fixture Owner",
      });
      yield* orchestrator.dispatch({
        type: "thread.share",
        commandId: CommandId.make("share:parent"),
        threadId: parentId,
        actor: "Fixture Owner",
        coOwner: "Fixture Collaborator",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("work:parent"),
        threadId: parentId,
        messageId: MessageId.make("work:parent"),
        text: "Work",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const parent = yield* orchestrator.getThreadProjection(parentId);
      const run = parent.runs[0]!;
      const common = {
        type: "delegated_task.request" as const,
        parentThreadId: parentId,
        parentRunId: run.id,
        parentNodeId: run.rootNodeId!,
        modelSelection,
        runtimeMode: "approval-required" as const,
        interactionMode: "plan" as const,
        completionWake: "always" as const,
        createdBy: "agent" as const,
        creationSource: "mcp" as const,
      };
      const missing = yield* orchestrator
        .dispatch({
          ...common,
          commandId: CommandId.make("missing"),
          task: "Missing project",
          projectId: ProjectId.make("missing"),
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(missing));
      assert.lengthOf((yield* orchestrator.getThreadProjection(parentId)).subagents, 0);
      yield* orchestrator.dispatch({
        ...common,
        commandId: CommandId.make("delegate:destination"),
        task: "Destination work",
        projectId: destinationId,
      });
      const task = (yield* orchestrator.getThreadProjection(parentId)).subagents[0]!;
      const child = yield* orchestrator.getThreadProjection(task.childThreadId!);
      assert.equal(child.thread.projectId, destinationId);
      assert.equal(child.thread.owner, "Fixture Owner");
      assert.deepEqual(child.thread.coOwners, []);
      assert.deepEqual(child.thread.lineage, {
        rootThreadId: parentId,
        parentThreadId: parentId,
        relationshipToParent: "subagent",
      });
      assert.equal(child.thread.runtimeMode, "approval-required");
      assert.equal(child.thread.interactionMode, "plan");
      assert.isNull(child.thread.worktreePath);
      assert.isNull(child.thread.branch);
      assert.equal(child.runs[0]!.status, "preparing");
      assert.deepEqual(child.runs[0]!.workspacePreparation, { type: "worktree", baseRef: "HEAD" });
      assert.equal(
        (yield* orchestrator.getThreadProjection(parentId)).thread.worktreePath,
        "/tmp/parent-worktree",
      );

      yield* orchestrator.dispatch({
        ...common,
        commandId: CommandId.make("delegate:root"),
        task: "Root work",
        projectId: destinationId,
        workspaceStrategy: { type: "root" },
      });
      const second = (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
        (task) => task.prompt === "Root work",
      )!;
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(second.childThreadId!)).runs[0]!
          .workspacePreparation,
        { type: "root" },
      );
      yield* orchestrator.dispatch({
        ...common,
        commandId: CommandId.make("delegate:default"),
        task: "Inherited workspace",
      });
      const inheritedTask = (yield* orchestrator.getThreadProjection(parentId)).subagents.find(
        (task) => task.prompt === "Inherited workspace",
      )!;
      const inherited = yield* orchestrator.getThreadProjection(inheritedTask.childThreadId!);
      assert.equal(inherited.thread.worktreePath, "/tmp/parent-worktree");
      assert.equal(inherited.thread.branch, "parent-branch");
      assert.equal(inherited.thread.projectId, parent.thread.projectId);
      assert.equal(inherited.thread.owner, "Fixture Owner");
      assert.deepEqual(inherited.thread.coOwners, []);
      const unchangedParent = (yield* orchestrator.getThreadProjection(parentId)).thread;
      assert.equal(unchangedParent.owner, "Fixture Owner");
      assert.deepEqual(unchangedParent.coOwners, ["Fixture Collaborator"]);
      assert.isUndefined(inherited.runs[0]!.workspacePreparation);
    }).pipe(Effect.provide(layer)),
);
