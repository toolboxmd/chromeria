import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type OrchestratorMcpThreadSendResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as Mcp from "../mcp/OrchestratorMcpService.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Launch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Tasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as Secrets from "../secrets/SecretRequests.ts";
import * as Settings from "../serverSettings.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import { SpectrumMcpSend } from "./mcpSend.ts";
import { makeMessage, makeRun } from "./testFixtures.ts";

it.effect(
  "the Spectrum MCP hook follows capability, caller-liveness and both mode gates; normal sends retain their result",
  () =>
    Effect.gen(function* () {
      const parentId = ThreadId.make("parent"),
        spectrumId = ThreadId.make("registered-spectrum"),
        normalId = ThreadId.make("normal");
      let runtimeMode = "full-access" as "full-access" | "approval-required";
      let interactionMode = "default" as "default" | "plan";
      let live = true,
        forkCalls = 0,
        normalCalls = 0;
      const child = idleThreadProjection(liveThreadShell(normalId));
      const run = makeRun({
        threadId: normalId,
        commandId: CommandId.make("normal-command"),
        messageId: MessageId.make("normal-input"),
        runId: null,
        replyIds: null,
      });
      const result: OrchestratorMcpThreadSendResult = {
        threadId: normalId,
        messageId: makeMessage(run, "normal").id,
        runId: run.id,
        status: run.status,
        delivery: "started",
      };
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Settings.layerTest(),
        Layer.mock(Launch.ThreadLaunchService)({}),
        Layer.mock(Git.GitVcsDriver)({}),
        Layer.mock(Projects.ProjectService)({}),
        Layer.mock(Tasks.ScheduledTaskService)({}),
        Layer.mock(Secrets.SecretRequests)({}),
        Layer.mock(Providers.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(Adapters.ProviderAdapterRegistryV2)({ list: () => Effect.succeed([]) }),
        Layer.mock(Threads.ThreadManagementService)({
          getThreadShell: (id) => Effect.succeed(liveThreadShell(id)),
          getThreadRecords: () =>
            Effect.sync(() => ({
              ...idleThreadProjection(liveThreadShell(parentId, { runtimeMode, interactionMode })),
              runs: live ? [{ ...run, threadId: parentId, status: "running" as const }] : [],
            })),
          getProjectThreadRecords: ({ threadId }) =>
            Effect.succeed(idleThreadProjection(liveThreadShell(threadId))),
          sendToThread: () =>
            Effect.sync(() => {
              normalCalls++;
              return {
                dispatch: { sequence: 0, storedEvents: [] },
                projection: child,
                message: makeMessage(run, "normal"),
                run,
                turnItem: null,
                delivery: "started" as const,
              };
            }),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment"),
        requestNamespace: "caller:stable",
        thread: {
          threadId: parentId,
          providerSessionId: "session",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };
      yield* Effect.gen(function* () {
        const service = yield* Mcp.OrchestratorMcpService;
        const send = { threadId: spectrumId, message: "Join", clientRequestId: "stable-request" };
        for (const [change, code] of [
          [
            () => {
              live = false;
            },
            "parent_not_active",
          ],
          [
            () => {
              live = true;
              runtimeMode = "approval-required";
            },
            "runtime_mode_escalation_denied",
          ],
          [
            () => {
              runtimeMode = "full-access";
              interactionMode = "plan";
            },
            "interaction_mode_escalation_denied",
          ],
        ] as const) {
          change();
          assert.strictEqual(
            (yield* service.sendToThread(scope, send).pipe(Effect.flip)).code,
            code,
          );
        }
        assert.strictEqual(
          (yield* service
            .sendToThread({ ...scope, capabilities: new Set() }, send)
            .pipe(Effect.flip)).code,
          "capability_denied",
        );
        assert.strictEqual(forkCalls, 0);
        interactionMode = "default";
        const first = yield* service.sendToThread(scope, send);
        assert.deepStrictEqual(yield* service.sendToThread(scope, send), first);
        assert.strictEqual(first.delivery, "transcript");
        const normal = yield* service.sendToThread(scope, { ...send, threadId: normalId });
        assert.strictEqual(normal.delivery, result.delivery);
        assert.strictEqual(normal.runId, result.runId);
        assert.strictEqual(normalCalls, 1);
      }).pipe(
        Effect.provide(Mcp.layer.pipe(Layer.provide(dependencies))),
        Effect.provideService(SpectrumMcpSend, {
          send: (_threads, input, _commandId, messageId, senderThreadId) =>
            Effect.sync(() => {
              forkCalls++;
              assert.strictEqual(senderThreadId, parentId);
              return input.threadId === spectrumId
                ? {
                    threadId: spectrumId,
                    messageId,
                    runId: null,
                    status: "idle",
                    delivery: "transcript",
                  }
                : null;
            }),
        }),
      );
    }),
);
