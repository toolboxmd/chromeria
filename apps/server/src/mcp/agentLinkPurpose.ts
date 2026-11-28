import {
  OrchestratorMcpFailure,
  purposeSelectionOf,
  purposeScopesEqual,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { CurrentRequest, McpRequestReceipts } from "./McpRequestReceipts.ts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  PurposeAdmissionError,
  purposeAfterAgentLink,
  type AgentLinkChange,
} from "../orchestration-v2/ExecutionPurpose.ts";
import {
  ThreadManagementService,
  type ThreadManagementServiceShape,
} from "../orchestration-v2/ThreadManagementService.ts";
import { newCommandId } from "./threadAccess.ts";
import { McpInvocationContext } from "./McpInvocationContext.ts";

export const withAgentLinkLock = Effect.fn("Mcp.withAgentLinkLock")(function* <A, E, R>(
  target: ThreadId | undefined,
  effect: Effect.Effect<A, E, R>,
) {
  const caller = yield* McpInvocationContext;
  const own = caller.thread?.threadId;
  if (caller.client !== undefined || own === undefined || (target !== undefined && target !== own))
    return yield* effect;
  return yield* (yield* ThreadManagementService).withAgentLinkLock(own, effect);
});

/** Reserve before writing the link, then confirm after purpose is applied. */
export const resumeLinkPurpose = Effect.fn("Mcp.resumeLinkPurpose")(function* (
  threadId: ThreadId,
  previouslyApplied: boolean,
) {
  const request = yield* CurrentRequest;
  const caller = yield* McpInvocationContext;
  if (!request || caller.client !== undefined || caller.thread?.threadId !== threadId)
    return { pending: true, finish: Effect.void };
  const owner = yield* Effect.serviceOption(McpRequestReceipts);
  if (Option.isNone(owner))
    return yield* new OrchestratorMcpFailure({
      code: "orchestration_error",
      message: "The durable link receipt is unavailable.",
    });
  const phase = yield* owner.value.linkPurposePhase(request, previouslyApplied);
  const completed = yield* owner.value.replay(phase);
  return {
    pending: Option.isNone(completed),
    finish: owner.value.sqlMutation(phase, Effect.succeed(true)),
  };
});

const isPurposeAdmissionError = Schema.is(PurposeAdmissionError);

export const updateAgentLinkPurpose = Effect.fn("Mcp.updateAgentLinkPurpose")(function* (
  threads: Pick<ThreadManagementServiceShape, "dispatch" | "getThreadShell">,
  threadId: ThreadId,
  change: AgentLinkChange,
) {
  const caller = yield* McpInvocationContext;
  if (caller.client !== undefined || caller.thread?.threadId !== threadId) return;
  const update = Effect.gen(function* () {
    const thread = yield* threads.getThreadShell(threadId);
    if (thread === null) return;
    const selection = purposeSelectionOf(thread);
    const scope = purposeAfterAgentLink(selection.scope, change);
    if (purposeScopesEqual(scope, selection.scope)) return;
    yield* threads.dispatch({
      type: "thread.metadata.update",
      threadId,
      commandId: yield* newCommandId(),
      purposeSelection: { expectedRevision: selection.revision, scope },
    });
  });
  yield* update.pipe(
    Effect.catchIf(
      (error) =>
        error._tag === "OrchestratorDispatchError" &&
        isPurposeAdmissionError(error.cause) &&
        error.cause.reason === "selection_changed",
      () => update,
    ),
    Effect.mapError(
      (error) =>
        new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
    ),
  );
});
