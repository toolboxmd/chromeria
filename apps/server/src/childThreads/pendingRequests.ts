import { OrchestratorMcpFailure, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { PeerAuthority } from "../peer/PeerAuthority.ts";

import { readThread, unavailable, type Caller } from "../mcp/threadAccess.ts";

/** Verified subagent ancestry uses local edges and authenticated delegation pairs. */
export const isCallerAncestor = Effect.fn("childThreads.isCallerAncestor")(function* (
  context: Caller,
  targetId: ThreadId,
) {
  const authority = yield* Effect.serviceOption(PeerAuthority);
  if (Option.isSome(authority))
    return (yield* authority.value.pendingActor(context.scope, targetId)) !== undefined;
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

export const readPendingRequest = Effect.fn("childThreads.readPendingRequest")(function* (
  input: {
    threadId?: ThreadId | undefined;
    requestId: import("@t3tools/contracts").RuntimeRequestId;
  },
  includeResolved = false,
) {
  const context = yield* readThread(input.threadId, ["runtimeRequests", "turnItems"]);
  const request = context.projection.runtimeRequests.find(
    (request) =>
      request.id === input.requestId && (includeResolved || request.status === "pending"),
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
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const rows = Option.isSome(sql)
    ? yield* sql.value<{
        revision: number;
      }>`SELECT COALESCE(MAX(sequence),0) AS revision FROM orchestration_events WHERE stream_id=${context.projection.thread.id} AND ((event_type='runtime-request.updated' AND json_extract(payload_json,'$.id')=${input.requestId}) OR (event_type='turn-item.updated' AND json_extract(payload_json,'$.requestId')=${input.requestId}))`.pipe(
        Effect.mapError(unavailable),
      )
    : [];
  return {
    ...context,
    request,
    item,
    ...(rows[0] === undefined ? {} : { revision: rows[0].revision }),
  };
});
