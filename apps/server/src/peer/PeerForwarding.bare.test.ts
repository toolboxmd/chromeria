import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  BareLaunchRequest,
  ThreadToolsRequest,
  EnvironmentId,
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpFailure,
  ThreadId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";

const probe = Tool.make("bare_probe", {
  parameters: Schema.Struct({
    bare: Schema.optional(BareLaunchRequest),
    tools: Schema.optional(ThreadToolsRequest),
    threads: Schema.optional(
      Schema.Array(Schema.Struct({ bare: Schema.optional(BareLaunchRequest) })),
    ),
    colors: Schema.optional(
      Schema.Array(Schema.Struct({ bare: Schema.optional(BareLaunchRequest) })),
    ),
  }),
  success: Schema.Struct({ ok: Schema.Boolean }),
});
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("here"),
  requestNamespace: "bare-peer",
  thread: {
    threadId: ThreadId.make("caller"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "session",
  },
  capabilities: new Set(["orchestration"]),
  client: undefined,
  issuedAt: 1,
};

it.effect.each([{ bare: {} }, { threads: [{ bare: {} }] }, { colors: [{ bare: {} }] }])(
  "checks optional peer capability on every bare forwarding attempt: %j",
  (intent) =>
    Effect.gen(function* () {
      let supported: boolean | undefined;
      const called: string[] = [];
      const call: PeerMcpClient.PeerMcpClient["Service"]["call"] = (input) => {
        called.push(input.tool);
        const result =
          input.tool === "orchestrator_capabilities"
            ? OrchestratorMcpCapabilitiesResult.make({
                ...(supported === undefined
                  ? {}
                  : { bareLaunch: supported, toolAllowlist: supported }),
                parentThreadId: null,
                inheritedProviderInstanceId: null,
                inheritedModel: null,
                runtimeMode: "approval-required",
                interactionMode: "default",
                providers: [],
                features: {
                  appOwnedSubagents: true,
                  asyncPolling: true,
                  cancellation: true,
                  batchThreadCreation: true,
                  threadManagement: true,
                  incrementalThreadRead: true,
                  scheduledTasks: true,
                  maxBatchThreads: 20,
                },
              })
            : { ok: true };
        expect(input.limits.runtimeMode).toBe("approval-required");
        return Schema.decodeUnknownEffect(input.success)(result).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Invalid fixture response",
              }),
          ),
        );
      };
      const layer = PeerForwarding.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            NodeCrypto.layer,
            Layer.succeed(PeerMcpClient.PeerMcpClient, { call }),
            Layer.mock(PeerLinks.PeerLinks)({}),
            Layer.mock(ThreadManagement.ThreadManagementService)({
              getThreadShell: () =>
                Effect.succeed(
                  liveThreadShell(scope.thread!.threadId, { runtimeMode: "approval-required" }),
                ),
            }),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const forwarding = yield* PeerForwarding.PeerForwarding;
        const target = EnvironmentId.make("there");
        const missing = yield* forwarding.call(scope, probe, target, intent).pipe(Effect.flip);
        expect(missing.message).toContain("does not advertise bare launch");
        expect(called).toEqual(["orchestrator_capabilities"]);
        supported = true;
        expect(yield* forwarding.call(scope, probe, target, intent)).toEqual({ ok: true });
        supported = false;
        yield* forwarding.call(scope, probe, target, intent).pipe(Effect.flip);
        expect(called).toEqual([
          "orchestrator_capabilities",
          "orchestrator_capabilities",
          "bare_probe",
          "orchestrator_capabilities",
        ]);
        // Ordinary forwarding keeps the existing wire path and needs no new capability.
        expect(yield* forwarding.call(scope, probe, target, {})).toEqual({ ok: true });
        expect(called.at(-1)).toBe("bare_probe");
        const policy = { tools: { allow: ["mcp:outreach:read_venue"] } };
        const policyError = yield* forwarding.call(scope, probe, target, policy).pipe(Effect.flip);
        expect(policyError.message).toContain("does not advertise tool allowlists");
        supported = true;
        expect(yield* forwarding.call(scope, probe, target, policy)).toEqual({ ok: true });
      }).pipe(Effect.provide(layer));
    }),
);
