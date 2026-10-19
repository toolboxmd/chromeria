import {
  OrchestratorMcpFailure,
  ProjectId,
  ThreadId,
  type AuthMcpOperation,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Invocation from "./McpInvocationContext.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";

const Target = Schema.Struct({
  projectId: Schema.optionalKey(ProjectId),
  threadId: Schema.optionalKey(ThreadId),
  environmentId: Schema.optionalKey(Schema.String),
  contextThreadId: Schema.optionalKey(Schema.String),
  action: Schema.optionalKey(Schema.String),
});
const decodeTarget = Schema.decodeUnknownOption(Target);
const operations: Readonly<Record<string, AuthMcpOperation>> = {
  t3_thread_launch: "launch",
  t3_thread_send: "send",
  t3_thread_read: "read",
  t3_thread_wait: "read",
  t3_project_read: "read",
  orchestrator_capabilities: "read",
  t3_thread_interrupt: "interrupt",
  t3_thread_organize: "settle",
};
const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "This call is outside the approved project or operations.",
  });

/** Fail closed before any handler, including reads and remote forwarding. */
export const checkOutsideGrant = (tool: string, params: unknown) =>
  Effect.gen(function* () {
    const context = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    const limits = Option.isSome(context) ? context.value.client?.limits : undefined;
    if (limits === undefined) return;
    const operation = operations[tool];
    const decoded = decodeTarget(params);
    if (operation === undefined || !limits.operations.includes(operation) || Option.isNone(decoded))
      return yield* denied();
    const target = decoded.value;
    // A project id is local to this environment. Never let forwarding or a borrowed caller escape it.
    if (target.environmentId !== undefined || target.contextThreadId !== undefined)
      return yield* denied();
    if (tool === "t3_thread_organize" && target.action !== "settle") return yield* denied();
    if (limits.projectId === undefined || tool === "orchestrator_capabilities") return;
    if (tool === "t3_thread_launch" || tool === "t3_project_read") {
      if (target.projectId !== limits.projectId) return yield* denied();
      return;
    }
    if (target.threadId === undefined) return yield* denied();
    const threads = yield* Effect.serviceOption(ThreadManagement.ThreadManagementService);
    if (Option.isNone(threads)) return yield* denied();
    const shell = yield* threads.value
      .getThreadShell(target.threadId)
      .pipe(Effect.mapError(denied));
    if (shell === null || shell.deletedAt !== null || shell.projectId !== limits.projectId)
      return yield* denied();
  });
