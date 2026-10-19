import {
  EnvironmentId,
  ProjectId,
  OrchestratorMcpFailure,
  type AuthMcpOperation,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as Access from "./McpToolAccess.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";

// Real declaration boundary, with an observable handler effect after authorization.
const tool = (name: string) =>
  Tool.make(name, {
    parameters: Schema.Unknown,
    success: Schema.Boolean,
    failure: OrchestratorMcpFailure,
    dependencies: [Invocation.McpInvocationContext],
  });
const toolkit = Toolkit.make(
  tool("t3_thread_read"),
  tool("t3_thread_send"),
  tool("t3_thread_launch"),
  tool("t3_thread_organize"),
  tool("t3_thread_interrupt"),
  tool("t3_project_read"),
  tool("unknown_tool"),
);
const scope = (operations: ReadonlyArray<AuthMcpOperation>): Invocation.McpInvocationScope => ({
  environmentId: EnvironmentId.make("here"),
  requestNamespace: "client:test",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
  thread: undefined,
  client: {
    sessionId: "client",
    label: "outside",
    access: "full-access",
    limits: { projectId: ProjectId.make("wedding"), operations },
  },
});
const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
  getThreadShell: (id) =>
    Effect.succeed(
      id === "missing"
        ? null
        : {
            ...liveThreadShell(id),
            projectId: ProjectId.make(id === "venue" ? "wedding" : "other"),
          },
    ),
});

it.effect.each([
  ["t3_thread_read", { threadId: "venue" }, ["read"], true],
  ["t3_thread_read", { threadId: "other" }, ["read"], false],
  ["t3_thread_read", { threadId: "missing" }, ["read"], false],
  ["t3_thread_read", {}, ["read"], false],
  ["t3_thread_send", { threadId: "venue" }, ["read"], false],
  ["t3_thread_send", { threadId: "venue" }, ["send"], true],
  ["t3_thread_send", { threadId: "other" }, ["send"], false],
  ["t3_thread_interrupt", { threadId: "venue" }, ["interrupt"], true],
  ["t3_thread_interrupt", { threadId: "other" }, ["interrupt"], false],
  ["t3_thread_launch", { projectId: "wedding" }, ["launch"], true],
  ["t3_thread_launch", { projectId: "other" }, ["launch"], false],
  ["t3_thread_launch", { scratch: true }, ["launch"], false],
  ["t3_thread_read", { threadId: "venue", environmentId: "elsewhere" }, ["read"], false],
  ["t3_thread_read", { threadId: "venue", contextThreadId: "other" }, ["read"], false],
  ["t3_thread_organize", { threadId: "venue", action: "settle" }, ["settle"], true],
  ["t3_thread_organize", { threadId: "other", action: "settle" }, ["settle"], false],
  ["t3_thread_organize", { threadId: "venue", action: "archive" }, ["settle"], false],
  ["t3_project_read", { projectId: "other" }, ["read"], false],
  ["unknown_tool", {}, ["read", "launch", "send", "settle", "interrupt"], false],
  ["t3_thread_read", { threadId: "venue" }, [], false],
] as const)("enforces outside grant for %s %j with %j", ([name, params, operations, allowed]) =>
  Effect.gen(function* () {
    let ran = false;
    const handle = Access.reads(() =>
      Effect.sync(() => {
        ran = true;
        return true;
      }),
    );
    const handlers = Object.fromEntries(Object.keys(toolkit.tools).map((key) => [key, handle]));
    const checked = Access.toLayer(toolkit, handlers);
    const built = yield* toolkit.pipe(Effect.provide(Access.HandlersLayer.layer(checked)));
    const result = yield* built
      .handle(name, params)
      .pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.result,
        Effect.provideService(Invocation.McpInvocationContext, scope(operations)),
        Effect.provide(threads),
      );
    expect(ran).toBe(allowed);
    expect(result._tag).toBe(allowed ? "Success" : "Failure");
  }),
);
