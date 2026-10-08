import { OrchestratorMcpFailure, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { readThread, unavailable, type Caller } from "../mcp/threadAccess.ts";

/** Native subagent edges alone grant ancestor authority, including across projects. */
export const isCallerAncestor = Effect.fn("childThreads.isCallerAncestor")(function* (
  context: Caller,
  targetId: ThreadId,
) {
  if (context.caller === undefined || context.caller.id === targetId) return false;
  const seen = new Set<ThreadId>();
  let id = targetId;
  while (!seen.has(id)) {
    seen.add(id);
    const shell = yield* context.threads.getThreadShell(id).pipe(Effect.mapError(unavailable));
    if (
      shell === null ||
      shell.lineage.relationshipToParent !== "subagent" ||
      shell.lineage.parentThreadId === null
    )
      return false;
    if (shell.lineage.parentThreadId === context.caller.id) return true;
    id = shell.lineage.parentThreadId;
  }
  return false;
});

export const readPendingRequest = Effect.fn("childThreads.readPendingRequest")(function* (input: {
  threadId?: ThreadId | undefined;
  requestId: import("@t3tools/contracts").RuntimeRequestId;
}) {
  const context = yield* readThread(input.threadId, ["runtimeRequests", "turnItems"]);
  const request = context.projection.runtimeRequests.find(
    (request) => request.id === input.requestId && request.status === "pending",
  );
  const item = context.projection.turnItems.find(
    (item) =>
      (item.type === "user_input_request" || item.type === "approval_request") &&
      item.requestId === input.requestId,
  );
  if (
    request === undefined ||
    item === undefined ||
    (item.type !== "user_input_request" && item.type !== "approval_request")
  ) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "The pending approval or user-input request was not found.",
    });
  }
  if (
    item.type === "approval_request" &&
    !(yield* isCallerAncestor(context, context.projection.thread.id))
  ) {
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Only an ancestor thread may answer a descendant approval.",
    });
  }
  return { ...context, request, item };
});
