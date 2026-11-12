import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ScheduledTaskUpsertInput,
  ThreadId,
  type ThreadBare,
} from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Git from "../git/GitWorkflowService.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Secrets from "../secrets/SecretRequests.ts";
import * as ScheduledTasks from "./ScheduledTaskService.ts";
import { readScheduledBare } from "./bareState.ts";

it.effect.each(["new_thread", "fresh_session", "continue"] as const)(
  "%s schedules persist bare intent and admitted reads across edits, service restart and repeated fires",
  (threadMode) =>
    Effect.gen(function* () {
      const seen: ThreadLaunch.ThreadLaunchInput[] = [];
      const sent: boolean[] = [];
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped();
      const readGrant = { type: "roots", roots: [root] } as const;
      const path = `${root}/input.txt`;
      yield* fs.writeFileString(path, "Named scheduled content");
      const threadId = ThreadId.make("thread:bound-bare");
      let content: ThreadBare | undefined;
      let bareLaunch: BareLaunch.BareLaunch["Service"];
      const adapter = {
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        bareSupport: Effect.succeed({ supported: true as const }),
        getCapabilities: () => Effect.die("not used"),
        planSelectionTransition: () => Effect.die("not used"),
        openSession: () => Effect.die("native execution disabled"),
      };
      const ports = Layer.mergeAll(
        Layer.merge(NodeServices.layer, NodeCrypto.layer),
        Scheduler.layer,
        Layer.mock(Threads.ThreadManagementService)({
          getThreadShell: () => Effect.succeed({ deletedAt: null } as never),
          getThreadRecords: () => Effect.succeed({ thread: { bare: content } } as never),
          sendToThread: (input) =>
            Effect.sync(() => sent.push(input.freshContext === true)).pipe(Effect.as({} as never)),
        }),
        Layer.mock(Secrets.SecretRequests)({}),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          namedProjectsRoot: "/projects",
          isScratchProject: () => Effect.succeed(false),
        }),
        Layer.mock(Providers.ProviderRegistry)({}),
        Layer.mock(Git.GitWorkflowService)({}),
        Layer.mock(Adapters.ProviderAdapterRegistryV2)({ get: () => Effect.succeed(adapter) }),
        Layer.mock(ThreadLaunch.ThreadLaunchService)({
          launch: (input) =>
            Effect.gen(function* () {
              seen.push(input);
              content = yield* bareLaunch
                .resolve({
                  request: input.bare!,
                  instanceId: input.modelSelection.instanceId,
                  cwd: root,
                  readGrant: input.bareReadGrant!,
                })
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new ThreadLaunch.ThreadLaunchError({
                        operation: "create-thread",
                        commandId: input.commandId,
                        projectId: input.projectId,
                        cause,
                      }),
                  ),
                );
              return { threadId, resumed: false } as never;
            }),
        }),
      );
      const dependencies = Layer.mergeAll(ports, BareLaunch.layer.pipe(Layer.provide(ports)));
      bareLaunch = yield* BareLaunch.BareLaunch.pipe(Effect.provide(dependencies));
      const layer = Layer.fresh(ScheduledTasks.layer).pipe(
        Layer.provide(ManagedPolicy.layerTest()),
        Layer.provide(dependencies),
      );
      const input = yield* decodeScheduledTaskUpsertInput({
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        id: "scheduled-task:bare",
        title: "Bare",
        prompt: "Do just this.",
        enabled: true,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId: "project:bare",
        threadId: null,
        threadMode,
        workspaceStrategy: { type: "root" },
        modelSelection: { instanceId: "codex", model: "test" },
        runtimeMode: "approval-required",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
        bare: { additions: [{ type: "file", path }] },
      });
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const denied = yield* tasks
          .upsert(input, { bareReadGrant: { type: "none" } })
          .pipe(Effect.flip);
        expect(denied.message).toContain("filesystem:read");
        expect((yield* tasks.list()).tasks).toEqual([]);
        const created = yield* tasks.upsert(input, { bareReadGrant: readGrant });
        expect(created.task.bare).toEqual(input.bare);
        const { bare: _intent, ...oldForm } = input;
        // A client without the new field can edit a title without clearing prior authority.
        yield* tasks.upsert(
          { ...oldForm, title: "Edited", requireExisting: true },
          { bareReadGrant: { type: "none" } },
        );
        expect((yield* tasks.list()).tasks[0]?.bare).toEqual(input.bare);
        expect((yield* readScheduledBare(created.task.id))?.readGrant).toEqual(readGrant);
      }).pipe(Effect.provide(layer));
      // A fresh service owns no ephemeral caller state, so execution must read the durable grant.
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const task = (yield* tasks.list()).tasks[0]!;
        yield* tasks.runNow({ id: task.id });
        const { bare: _request, ...stale } = input;
        yield* tasks.upsert({ ...stale, requireExisting: true });
      }).pipe(Effect.provide(Layer.fresh(layer)));
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const task = (yield* tasks.list()).tasks[0]!;
        yield* tasks.runNow({ id: task.id });
        expect(seen).toHaveLength(threadMode === "new_thread" ? 2 : 1);
        expect(sent).toEqual(threadMode === "new_thread" ? [] : [threadMode === "fresh_session"]);
        expect((yield* tasks.list()).tasks[0]?.bare).toEqual(input.bare);
        expect((yield* readScheduledBare(task.id))?.readGrant).toEqual(readGrant);
        expect(content?.additions[0]).toMatchObject({ type: "file", path, byteCount: 23 });
        for (const launch of seen) {
          expect(launch.bare).toEqual(input.bare);
          expect(launch.bareReadGrant).toEqual(readGrant);
          expect(launch.runtimeMode).toBe("approval-required");
          expect(launch.initialMessage?.text).toBe("Do just this.");
        }
        if (threadMode !== "new_thread") {
          const readDenied = yield* tasks
            .upsert(input, { bareReadGrant: { type: "none" } })
            .pipe(Effect.flip);
          expect(readDenied.message).toContain("filesystem:read");
          const changed = yield* tasks
            .upsert({
              ...input,
              bare: { additions: [{ type: "file", path: `${root}/other.txt` }] },
            })
            .pipe(Effect.flip);
          expect(changed.message).toContain("no new additions");
          const retained = content;
          content = { additions: [] };
          expect((yield* Effect.result(tasks.runNow({ id: task.id })))._tag).toBe("Failure");
          content = undefined;
          expect((yield* Effect.result(tasks.runNow({ id: task.id })))._tag).toBe("Failure");
          content = retained;
          expect(sent).toHaveLength(1);
        }
      }).pipe(Effect.provide(Layer.fresh(layer)));
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(Sqlite.layerMemory, NodeServices.layer))),
);

const decodeScheduledTaskUpsertInput = Schema.decodeEffect(ScheduledTaskUpsertInput);
