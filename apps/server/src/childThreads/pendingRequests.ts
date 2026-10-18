import {
  isPromachosThread,
  OrchestratorMcpFailure,
  type OrchestrationV2RequestAnsweredBy,
  type OrchestrationV2ThreadShell,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import { PeerAuthority } from "../peer/PeerAuthority.ts";

import { clientModeCeiling } from "../mcp/McpInvocationContext.ts";
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
  // Caller/target resolution and legacy transcript preparation may do work outside
  // the native store. Never use that earlier content with a later event revision.
  const initial = yield* readThread(input.threadId, ["runtimeRequests", "turnItems"]);
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const snapshot = yield* Effect.gen(function* () {
    if (Option.isNone(sql)) return { projection: initial.projection };
    const store = yield* Effect.serviceOption(ProjectionStoreV2);
    if (Option.isNone(store)) return yield* unavailable();
    return yield* sql.value
      .withTransaction(
        Effect.gen(function* () {
          const projection = yield* store.value.getThreadRecords(
            initial.projection.thread.id,
            ["runtimeRequests", "turnItems"],
            { turnItemTypes: ["user_input_request", "approval_request"] },
          );
          const rows = yield* sql.value<{
            revision: number;
          }>`SELECT COALESCE(MAX(sequence),0) AS revision FROM orchestration_events WHERE stream_id=${projection.thread.id} AND ((event_type='runtime-request.updated' AND json_extract(payload_json,'$.id')=${input.requestId}) OR (event_type='turn-item.updated' AND json_extract(payload_json,'$.requestId')=${input.requestId}))`;
          const row = rows[0];
          if (row === undefined) return yield* unavailable();
          return { projection, revision: row.revision };
        }),
      )
      .pipe(Effect.mapError(unavailable));
  });
  const context = { ...initial, projection: snapshot.projection };
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
  const answerer = yield* requestAnswerer(context, context.projection.thread);
  if (item.type === "approval_request" && (answerer === undefined || answerer.kind === "own")) {
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message:
        "Only an ancestor thread, or the Promachos once no ancestor is active, may answer a descendant approval.",
    });
  }
  return {
    ...context,
    request,
    item,
    answerer,
    ...("revision" in snapshot ? { revision: snapshot.revision } : {}),
  };
});
