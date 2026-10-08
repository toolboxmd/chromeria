import {
  CommandId,
  MessageId,
  type OrchestrationV2ServerCommand,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { CommandReceiptStoreV2 } from "../orchestration-v2/CommandReceiptStore.ts";

import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import { ThreadCommandExecutor } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { readRetirementState } from "./retirement.ts";

/** Acquires each recipient lock itself. Call outside thread locks and pass the
 * already-serialized dispatch path, which must not acquire that lock again. */
export const reconcileChildRequests = Effect.fn("childThreads.reconcileRequests")(function* <E>(
  childId: ThreadId,
  dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E>,
) {
  const projections = yield* ProjectionStoreV2;
  const serialization = yield* ThreadCommandExecutor;
  const receiptsStore = yield* CommandReceiptStoreV2;
  const child = yield* projections.getThreadRecords(childId, ["runtimeRequests"]);
  const ancestors: Array<ThreadId> = [];
  const seen = new Set<ThreadId>([childId]);
  let lineage = child.thread.lineage;
  while (lineage.relationshipToParent === "subagent" && lineage.parentThreadId !== null) {
    const parentId = lineage.parentThreadId;
    if (seen.has(parentId)) return;
    seen.add(parentId);
    const parent = yield* projections.getThreadShell(parentId);
    if (parent === null) return;
    ancestors.push(parentId);
    lineage = parent.lineage;
  }
  for (const parentId of ancestors) {
    yield* serialization
      .withLock(
        parentId,
        Effect.gen(function* () {
          const current = yield* projections.getThreadRecords(childId, ["runtimeRequests"]);
          const retirement = yield* readRetirementState(childId);
          const parent = yield* projections.getThreadRecords(parentId, ["runs", "messages"]);
          const parentRetirement = yield* readRetirementState(parentId);
          for (const request of current.runtimeRequests) {
            if (request.kind === "dynamic_tool_call" || request.kind === "auth_refresh") continue;
            const identity = `child-request:${childId}:${request.id}:${parentId}`;
            const messageId = MessageId.make(identity);
            const pending =
              request.status === "pending" && retirement.complete && !retirement.retired;
            if (!pending || parentRetirement.retired || !parentRetirement.complete) {
              for (const run of parent.runs) {
                if (run.userMessageId === messageId && run.status === "queued") {
                  yield* dispatch({
                    type: "queued-run.cancel",
                    commandId: CommandId.make(`${identity}:cancel:${run.id}`),
                    threadId: parentId,
                    runId: run.id,
                  });
                }
              }
              continue;
            }
            const prefix = `command:${identity}:`;
            let attempt = 0;
            let accepted = false;
            while (true) {
              const receipt = yield* receiptsStore.getByCommandId(
                CommandId.make(`${prefix}${attempt}`),
              );
              if (Option.isNone(receipt)) break;
              if (receipt.value.status === "accepted") {
                accepted = true;
                break;
              }
              attempt += 1;
            }
            if (accepted) continue;
            yield* dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`${prefix}${attempt}`),
              threadId: parentId,
              messageId,
              createdBy: "system",
              creationSource: "server",
              attachments: [],
              deliveryIntent: "auto",
              dispatchMode: { type: "queue_after_active" },
              text: `Descendant ${childId} is waiting for ${request.kind === "user_input" ? "a question answer" : "an approval"} (${request.id}). Read it with t3_pending_request_read and respond with t3_pending_request_respond. Preserve the child's mode.`,
            });
          }
        }),
      )
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to deliver descendant request", { childId, parentId, cause }),
        ),
      );
  }
});

export const reconcilePendingChildRequests = Effect.fn("childThreads.reconcilePendingRequests")(
  function* <E>(dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E>) {
    const projections = yield* ProjectionStoreV2;
    const snapshot = yield* projections.getShellSnapshot();
    for (const thread of snapshot.threads) {
      if (thread.lineage.relationshipToParent === "subagent")
        yield* reconcileChildRequests(thread.id, dispatch);
    }
  },
);
