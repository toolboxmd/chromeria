import {
  CommandId,
  MessageId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import { reportLabel } from "@t3tools/provider-core/server/notification";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { readRetirementState } from "../childThreads/retirement.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import { CommandReceiptStoreV2 } from "./CommandReceiptStore.ts";

/** Live delivery and persisted startup recovery share the native question identity and policy.
 * A serialized caller passes its dispatch path without acquiring the parent lock again. */
export const reconcileDelegatedTaskQuestion = Effect.fnUntraced(function* <E>(
  task: OrchestrationV2Subagent,
  request: OrchestrationV2RuntimeRequest,
  dispatch: (command: OrchestrationV2ServerCommand) => Effect.Effect<unknown, E>,
) {
  if (task.childThreadId === null) return;
  const projections = yield* ProjectionStoreV2;
  const parent = yield* projections.getThreadRecords(task.threadId, ["runs"]);
  const child = yield* projections.getThread(task.childThreadId);
  const retirement = yield* readRetirementState(task.childThreadId);
  const key = `${task.threadId}:${request.id}`;
  const messageId = MessageId.make(`message:delegated-task-question:${key}`);
  const legacyIdentity = `child-request:${task.childThreadId}:${request.id}:${task.threadId}`;
  const legacyMessageId = MessageId.make(legacyIdentity);
  const pending =
    request.status === "pending" &&
    task.completionDelivery?.state !== "disposed" &&
    child.archivedAt === null &&
    child.deletedAt === null &&
    parent.thread.archivedAt === null &&
    parent.thread.deletedAt === null &&
    retirement.complete &&
    !retirement.retired;

  if (pending) {
    // A blocking delegate_task wait delivers live questions itself. Durable message
    // questions still need a notice, even after the child has completed.
    if (
      task.completionWake === "settled_only" &&
      request.responseCapability.type === "live" &&
      parent.runs.some(
        (run) =>
          run.id === task.runId &&
          (run.status === "preparing" || run.status === "starting" || run.status === "running"),
      )
    ) {
      return;
    }
    const label = reportLabel(task.title?.trim() || task.prompt);
    const situation =
      request.responseCapability.type === "message"
        ? `Delegated task ${task.id} asked a question and is still working.`
        : `Delegated task ${task.id} is blocked on a question and cannot continue until it is answered.`;
    const receipts = yield* CommandReceiptStoreV2;
    const commandPrefix = `server:delegated-task-question:${key}`;
    let commandId = CommandId.make(commandPrefix);
    let attempt = 0;
    // Rejections are durable too: replaying one never plans again. Keep the
    // original identity for accepted wakes; only a rejected attempt gets a new ID.
    for (;;) {
      const receipt = yield* receipts.getByCommandId(commandId);
      if (Option.isNone(receipt) || receipt.value.status === "accepted") break;
      attempt += 1;
      commandId = CommandId.make(`server:delegated-task-question-retry:${attempt}:${key}`);
    }
    // Success means the command and its stable receipt committed. If dispatch fails,
    // leave the old queued notice intact so a later recovery can retry the handoff.
    yield* dispatch({
      type: "message.dispatch",
      commandId,
      threadId: task.threadId,
      messageId,
      text: `${situation} Read it with t3_pending_request_read using threadId ${task.childThreadId} and requestId ${request.id}. Answer it with t3_pending_request_respond when the answer follows from the task or the user's instructions; otherwise ask the user, then answer it. If it is no longer pending, it was answered or dismissed.`,
      notification: {
        source: { kind: "delegated_task", taskIds: [task.id], childThreadId: task.childThreadId },
        outcome: "updated",
        summary: `Delegated task${label === undefined ? "" : ` "${label}"`} has a question`,
      },
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
    });
  }

  for (const run of parent.runs) {
    if (run.status !== "queued") continue;
    if (run.userMessageId === legacyMessageId) {
      yield* dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make(`${legacyIdentity}:cancel:${run.id}`),
        threadId: task.threadId,
        runId: run.id,
      });
    } else if (!pending && run.userMessageId === messageId) {
      yield* dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make(`server:delegated-task-question-cancel:${key}`),
        threadId: task.threadId,
        runId: run.id,
      });
    }
  }
});
