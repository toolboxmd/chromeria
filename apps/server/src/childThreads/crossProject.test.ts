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
import type { ProviderAdapterV2Shape } from "../orchestration-v2/ProviderAdapter.ts";
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
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const layer = Harness.layerWithRegistry(
  { name: "cross-project" },
  Registry.layerFromAdapters([adapter]),
  { databaseLayer: database, runEffectWorker: false },
).pipe(Layer.provideMerge(database));

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
      assert.isUndefined(inherited.runs[0]!.workspacePreparation);
    }).pipe(Effect.provide(layer)),
);
