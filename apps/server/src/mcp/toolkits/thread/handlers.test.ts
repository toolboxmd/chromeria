import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as ThreadHandlers from "./handlers.ts";
import { ThreadToolkit } from "./tools.ts";

// Fork: thread people (toolboxmd/chromeria#170).
it.effect("an agent's fork belongs to its caller's owner, not the source thread's", () =>
  Effect.gen(function* () {
    const callerId = ThreadId.make("caller-thread");
    const sourceId = ThreadId.make("source-thread");
    const projectId = ProjectId.make("project");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const shell = (id: ThreadId, owner: string) =>
      ({
        id,
        projectId,
        providerInstanceId,
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        activeRunId: "active-run",
        archivedAt: null,
        deletedAt: null,
        owner,
        coOwners: [],
      }) as unknown as OrchestrationV2ThreadShell;
    const shells = new Map([
      [callerId, shell(callerId, "Pauli")],
      [sourceId, shell(sourceId, "Luke")],
    ]);
    const dispatched: Array<OrchestrationV2Command> = [];
    const layerDependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "session",
        thread: { threadId: callerId, providerSessionId: "session", providerInstanceId },
        client: undefined,
        issuedAt: 0,
        capabilities: new Set(["orchestration" as const]),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (threadId) => Effect.succeed(shells.get(threadId) ?? null),
        getProjectThreadRecords: ({ threadId }) =>
          Effect.succeed({ thread: shells.get(threadId) } as never),
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command);
            return { sequence: 1 } as never;
          }),
      }),
      Layer.mock(ThreadSearch.ThreadSearch)({}),
      Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
    );
    const toolkit = yield* ThreadToolkit.pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(ThreadHandlers.layer).pipe(
          Layer.provide(layerDependencies),
        ),
      ),
    );
    yield* toolkit
      .handle("t3_thread_fork", {
        threadId: sourceId,
        sourcePoint: { type: "run", runId: RunId.make("source-run") },
      })
      .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(layerDependencies));
    expect(dispatched).toHaveLength(1);
    expect(dispatched[0]).toMatchObject({
      type: "thread.fork",
      sourceThreadId: sourceId,
      owner: "Pauli",
    });
  }),
);
