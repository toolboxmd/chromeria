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

import { clientModeCeiling } from "../mcp/McpInvocationContext.ts";
import { readThread, unavailable, type Caller } from "../mcp/threadAccess.ts";

/** Native subagent edges alone grant ancestor authority, including across projects. */
const isCallerAncestor = Effect.fn("childThreads.isCallerAncestor")(function* (
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

/**
 * An ancestor with running work, or with any activity this recently, keeps
 * first say over its descendants' requests; after that the Promachos may
 * answer them for the user (toolboxmd/chromeria#532).
 */
const ANCESTOR_STALE_AFTER = Duration.minutes(30);

type Target = Pick<OrchestrationV2ThreadShell, "lineage" | "delegatedFrom">;

const ancestorHasFirstSay = Effect.fn("childThreads.ancestorHasFirstSay")(function* (
  context: Caller,
  target: Target,
) {
  // A parent in another environment answers through its own link (toolboxmd/chromeria#369).
  if (target.delegatedFrom !== undefined) return true;
  const staleBefore =
    DateTime.toEpochMillis(yield* DateTime.now) - Duration.toMillis(ANCESTOR_STALE_AFTER);
  const seen = new Set<ThreadId>();
  let lineage = target.lineage;
  while (lineage.relationshipToParent === "subagent" && lineage.parentThreadId !== null) {
    if (seen.has(lineage.parentThreadId)) return false;
    seen.add(lineage.parentThreadId);
    const parent = yield* context.threads
      .getThreadShell(lineage.parentThreadId)
      .pipe(Effect.mapError(unavailable));
    if (parent === null) return false;
    // A deleted parent has no say, but an active ancestor above it still does.
    if (
      parent.deletedAt === null &&
      (parent.delegatedFrom !== undefined ||
        parent.activeRunId !== null ||
        DateTime.toEpochMillis(parent.updatedAt) > staleBefore)
    )
      return true;
    lineage = parent.lineage;
  }
  return false;
});

/** A linked environment's Promachos, when the link may act as the user here. */
const linkedPromachos = (context: Caller): OrchestrationV2RequestAnsweredBy | undefined => {
  const client = context.scope.client;
  if (context.caller !== undefined || client?.linked !== true) return undefined;
  if (client.promachosThreadId === undefined) return undefined;
  // This environment cannot see the peer's threads, so it trusts the link's claim. Only a
  // full-access link may make it: that link may already change this environment's settings
  // and scheduled tasks (`assertNotLinked`), which is more than answering a question.
  const ceiling = clientModeCeiling(client);
  if (ceiling.runtimeMode !== "full-access" || ceiling.interactionMode !== "default")
    return undefined;
  return { kind: "promachos", threadId: client.promachosThreadId, via: client.label };
};

/**
 * Who answers when the caller responds to `target`'s request: the thread
 * itself or a client outside any thread (questions only), an ancestor, or
 * the Promachos for the user once no ancestor has first say. Undefined
 * means the caller may not answer.
 */
export type RequestAnswerer =
  | { readonly kind: "own" }
  | { readonly kind: "ancestor" }
  | { readonly kind: "promachos"; readonly answeredBy: OrchestrationV2RequestAnsweredBy };

export const requestAnswerer = Effect.fn("childThreads.requestAnswerer")(function* (
  context: Caller,
  target: Target & { readonly id: ThreadId },
) {
  const caller = context.caller;
  if (caller !== undefined && caller.id === target.id) return { kind: "own" } as const;
  if (yield* isCallerAncestor(context, target.id)) return { kind: "ancestor" } as const;
  const promachos: OrchestrationV2RequestAnsweredBy | undefined =
    caller !== undefined && isPromachosThread(caller)
      ? { kind: "promachos", threadId: caller.id }
      : linkedPromachos(context);
  if (promachos !== undefined && !(yield* ancestorHasFirstSay(context, target)))
    return { kind: "promachos", answeredBy: promachos } as const;
  return caller === undefined ? ({ kind: "own" } as const) : undefined;
});

export const readPendingRequest = Effect.fn("childThreads.readPendingRequest")(function* (input: {
  threadId?: ThreadId | undefined;
  requestId: RuntimeRequestId;
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
  const answerer = yield* requestAnswerer(context, context.projection.thread);
  if (item.type === "approval_request" && (answerer === undefined || answerer.kind === "own")) {
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message:
        "Only an ancestor thread, or the Promachos once no ancestor is active, may answer a descendant approval.",
    });
  }
  return { ...context, request, item, answerer };
});
