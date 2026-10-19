import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ScheduledTaskUpsertInput,
} from "@t3tools/contracts";
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
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Secrets from "../secrets/SecretRequests.ts";
import * as ScheduledTasks from "./ScheduledTaskService.ts";
import { readScheduledBare } from "./bareState.ts";

it.effect(
  "schedules persist bare intent and admitted reads across edits, service restart and each fresh fire",
  () =>
    Effect.gen(function* () {
      const seen: ThreadLaunch.ThreadLaunchInput[] = [];
      const adapter = {
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        bareSupport: Effect.succeed({ supported: true as const }),
        getCapabilities: () => Effect.die("not used"),
        planSelectionTransition: () => Effect.die("not used"),
        openSession: () => Effect.die("native execution disabled"),
      };
      const ports = Layer.mergeAll(
        NodeServices.layer,
        Scheduler.layer,
        Layer.mock(Threads.ThreadManagementService)({}),
        Layer.mock(Secrets.SecretRequests)({}),
        Layer.mock(Providers.ProviderRegistry)({}),
        Layer.mock(Git.GitWorkflowService)({}),
        Layer.mock(Adapters.ProviderAdapterRegistryV2)({ get: () => Effect.succeed(adapter) }),
        Layer.mock(ThreadLaunch.ThreadLaunchService)({
          launch: (input) =>
            Effect.sync(() => seen.push(input)).pipe(
              Effect.andThen(
                Effect.fail(
                  new ThreadLaunch.ThreadLaunchError({
                    commandId: input.commandId,
                    projectId: input.projectId,
                    operation: "create-thread",
                    cause: "native execution disabled",
                  }),
                ),
              ),
            ),
        }),
      );
      const dependencies = Layer.mergeAll(ports, BareLaunch.layer.pipe(Layer.provide(ports)));
      const layer = Layer.fresh(ScheduledTasks.layer).pipe(Layer.provide(dependencies));
      const input = yield* Schema.decodeUnknownEffect(ScheduledTaskUpsertInput)({
        purposeIntent: { version: 1, intent: { type: "unscoped" } },
        id: "scheduled-task:bare",
        title: "Bare",
        prompt: "Do just this.",
        enabled: true,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId: "project:bare",
        threadId: null,
        workspaceStrategy: { type: "root" },
        modelSelection: { instanceId: "codex", model: "test" },
        runtimeMode: "approval-required",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
        bare: { additions: [{ type: "file", path: "/named/input.txt" }] },
      });
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const denied = yield* tasks
          .upsert(input, { bareReadGrant: { type: "none" } })
          .pipe(Effect.flip);
        expect(denied.message).toContain("filesystem:read");
        expect((yield* tasks.list()).tasks).toEqual([]);
        const created = yield* tasks.upsert(input, { bareReadGrant: { type: "all" } });
        expect(created.task.bare).toEqual(input.bare);
        const { bare: _intent, ...oldForm } = input;
        // A client without the new field can edit a title without clearing prior authority.
        yield* tasks.upsert(
          { ...oldForm, title: "Edited", requireExisting: true },
          { bareReadGrant: { type: "none" } },
        );
        expect((yield* tasks.list()).tasks[0]?.bare).toEqual(input.bare);
        expect((yield* readScheduledBare(created.task.id))?.readGrant).toEqual({ type: "all" });
      }).pipe(Effect.provide(layer));
      // A fresh service owns no ephemeral caller state, so execution must read the durable grant.
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const task = (yield* tasks.list()).tasks[0]!;
        yield* tasks.runNow({ id: task.id }).pipe(Effect.ignore);
        yield* tasks.runNow({ id: task.id }).pipe(Effect.ignore);
        expect(seen).toHaveLength(2);
        for (const launch of seen) {
          expect(launch.bare).toEqual(input.bare);
          expect(launch.bareReadGrant).toEqual({ type: "all" });
          expect(launch.runtimeMode).toBe("approval-required");
          expect(launch.initialMessage?.text).toBe("Do just this.");
        }
      }).pipe(Effect.provide(Layer.fresh(ScheduledTasks.layer).pipe(Layer.provide(dependencies))));
    }).pipe(Effect.provide(Sqlite.layerMemory)),
);
