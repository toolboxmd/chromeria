import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  ThreadId,
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

const tool = (name: string) =>
  Tool.make(name, {
    parameters: Schema.Struct({ threadId: Schema.String }),
    success: Schema.Struct({ ok: Schema.Boolean }),
  });
const caller = ThreadId.make("caller");
const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("here"),
  requestNamespace: "promachos-peer",
  thread: {
    threadId: caller,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "session",
  },
  capabilities: new Set(["orchestration"]),
  client: undefined,
  issuedAt: 1,
};

// Fork: a linked environment learns its caller is the Promachos only when it answers (toolboxmd/chromeria#532).
it.effect.each([
  ["t3_pending_request_respond", "promachos", caller],
  ["t3_pending_request_respond", undefined, undefined],
  ["t3_pending_request_list", "promachos", caller],
  // A subagent child copies its parent's record, role included, but is not the Promachos.
  ["t3_pending_request_respond", "child", undefined],
  ["t3_thread_read", "promachos", undefined],
] as const)("forwards %s from a %s caller with Promachos thread %s", ([name, role, expected]) =>
  Effect.gen(function* () {
    const sent: Array<ThreadId | undefined> = [];
    const call: PeerMcpClient.PeerMcpClient["Service"]["call"] = (input) => {
      sent.push(input.promachosThreadId);
      // oxlint-disable-next-line t3code/no-inline-schema-compile -- The fake peer decodes the schema supplied by each call.
      return Schema.decodeEffect(input.success)({ ok: true }).pipe(
        Effect.mapError(
          () => new OrchestratorMcpFailure({ code: "orchestration_error", message: "fixture" }),
        ),
      );
    };
    const layer = PeerForwarding.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          NodeCrypto.layer,
          Layer.succeed(PeerMcpClient.PeerMcpClient, {
            call,
            requirePendingRequests: () => Effect.void,
            uploadAttachment: () => Effect.die("No attachment transfer in this focused fixture"),
            start: Effect.void,
          }),
          Layer.mock(PeerLinks.PeerLinks)({ enabled: Effect.succeed(true) }),
          Layer.mock(ThreadManagement.ThreadManagementService)({
            getThreadShell: () =>
              Effect.succeed({
                ...liveThreadShell(caller),
                ...(role === undefined ? {} : { prismRole: "promachos" as const }),
                ...(role === "child"
                  ? {
                      lineage: {
                        rootThreadId: ThreadId.make("parent"),
                        parentThreadId: ThreadId.make("parent"),
                        relationshipToParent: "subagent" as const,
                      },
                    }
                  : {}),
              }),
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const forwarding = yield* PeerForwarding.PeerForwarding;
      yield* forwarding.call(scope, tool(name), EnvironmentId.make("there"), { threadId: "t" });
    }).pipe(Effect.provide(layer));
    expect(sent).toEqual([expected]);
  }),
);
