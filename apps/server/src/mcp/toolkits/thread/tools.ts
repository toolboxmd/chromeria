import { ThreadTools } from "@t3tools/contracts";
import { ThreadBare } from "@t3tools/contracts";
import * as McpRouting from "../../McpRouting.ts";
import {
  PendingRequestInput,
  PendingRequestContent,
  ScheduledTaskId,
  McpManualRunReceipt,
  ScheduledTask,
  OrchestrationSearchThreadsInput,
  OrchestrationThreadSearchMatch,
  OrchestrationV2ThreadForkSourcePoint,
  OrchestrationV2ContextTransfer,
  TrimmedNonEmptyString,
  ModelSelection,
  RuntimeMode,
  ProviderInteractionMode,
  RuntimeRequestId,
  ProviderUserInputAnswers,
  ProviderApprovalDecision,
  IsoDateTime,
  OrchestratorMcpFailure,
  OrchestrationV2DispatchCommandResult,
  ThreadId,
  RunId,
  NonNegativeInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";

import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTaskService from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const ThreadOrganizeTool = Tool.make("t3_thread_organize", {
  description:
    "Pin, snooze, settle, archive, or mark a thread unread. Omit `threadId` for this thread. `snooze` needs `snoozedUntil`. Settling this thread applies at turn end. A failed turn or queued message leaves it active.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      threadId: Schema.optional(ThreadId),
      action: Schema.Literals([
        "pin",
        "unpin",
        "snooze",
        "unsnooze",
        "settle",
        "unsettle",
        "archive",
        "unarchive",
        "mark_unread",
      ]),
      snoozedUntil: Schema.optional(IsoDateTime),
    }),
  ),
  success: Schema.Union([
    OrchestrationV2DispatchCommandResult,
    Schema.Struct({ settlesWhenTurnEnds: Schema.Literal(true) }),
  ]),
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    Crypto.Crypto,
  ],
})
  .annotate(Tool.Title, "Organize a thread")
  .annotate(Tool.Destructive, true);

const queueTarget = { threadId: Schema.optional(ThreadId), queuedRunId: RunId };
const commandTool = {
  success: OrchestrationV2DispatchCommandResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    Crypto.Crypto,
  ],
};
const queueEntry = Schema.Struct({
  queuedRunId: RunId,
  // Older peers omit this field; absence means the hold state is unknown.
  held: Schema.optional(
    Schema.Boolean.annotate({
      description: "Whether this message is held. Absence means an older peer did not report it.",
    }),
  ),
  text: Schema.String,
  truncated: Schema.Boolean,
});
const QueueListTool = Tool.make("t3_queue_list", {
  ...commandTool,
  description:
    "List queued messages in send order, with whether each is held. Resume a held queue with `t3_queue_resume`. For full history, use `t3_thread_read`.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      threadId: Schema.optional(ThreadId),
      cursor: Schema.optional(NonNegativeInt),
      limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
    }),
  ),
  success: Schema.Struct({
    items: Schema.Array(queueEntry),
    nextCursor: Schema.NullOr(NonNegativeInt),
  }),
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
const QueueReadTool = Tool.make("t3_queue_read", {
  ...commandTool,
  description: "Read up to 16,000 chars of one queued message. Omit `threadId` for this thread.",
  parameters: McpRouting.parameters(Schema.Struct(queueTarget)),
  success: queueEntry,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
const QueueEditTool = Tool.make("t3_queue_edit", {
  ...commandTool,
  description:
    "Replace one queued message's text. Attachments stay. Fails if the run is no longer queued.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      ...queueTarget,
      text: Schema.String.check(Schema.isMaxLength(100000)),
    }),
  ),
}).annotate(Tool.Destructive, true);
const QueueCancelTool = Tool.make("t3_queue_cancel", {
  ...commandTool,
  description: "Cancel one queued run.",
  parameters: McpRouting.parameters(Schema.Struct(queueTarget)),
}).annotate(Tool.Destructive, true);
const QueueResumeTool = Tool.make("t3_queue_resume", {
  ...commandTool,
  description:
    "Explicitly resume a held queue using the existing queue.resume command. Omit threadId for this thread. Clears holds and delivers queued messages in their existing order when the thread can start another turn. The same thread mutation permissions and Stop/retirement rules apply.",
  parameters: McpRouting.parameters(Schema.Struct({ threadId: Schema.optional(ThreadId) })),
}).annotate(Tool.Destructive, true);
const QueueReorderTool = Tool.make("t3_queue_reorder", {
  ...commandTool,
  description: "Move one queued run before another, or to the end with `beforeRunId=null`.",
  parameters: McpRouting.parameters(
    Schema.Struct({ ...queueTarget, beforeRunId: Schema.NullOr(RunId) }),
  ),
}).annotate(Tool.Destructive, true);
const QueuePromoteTool = Tool.make("t3_queue_promote_to_steer", {
  ...commandTool,
  description:
    "Send one queued message now as steering to the given active run. An agent's promotion follows `t3_thread_send` steer rules: on Claude the message stays queued. For an urgent interruption use `t3_thread_send` with `mode:'restart'`.",
  parameters: McpRouting.parameters(Schema.Struct({ ...queueTarget, targetRunId: RunId })),
}).annotate(Tool.Destructive, true);

const PendingRequestTool = Tool.make("t3_pending_request", {
  ...commandTool,
  description:
    "List pending questions and descendant approvals, read one by requestId, or respond with answers for a question or decision for an approval. Omit threadId for this thread. Preserve the qualified target, route and revision returned by read.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      ...PendingRequestInput.fields,
      requestId: Schema.optional(RuntimeRequestId),
      action: Schema.Literals(["list", "read", "respond"]),
      answers: Schema.optional(ProviderUserInputAnswers),
      decision: Schema.optional(ProviderApprovalDecision),
    }),
  ),
  success: Schema.Union([
    Schema.Struct({ requestIds: Schema.Array(RuntimeRequestId) }),
    PendingRequestContent,
    OrchestrationV2DispatchCommandResult,
  ]),
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const pendingRequest = PendingRequestContent;
const PendingRequestListTool = Tool.make("t3_pending_request_list", {
  ...commandTool,
  description: "List pending questions and descendant approvals. Omit `threadId` for this thread.",
  parameters: McpRouting.parameters(Schema.Struct({ threadId: Schema.optional(ThreadId) })),
  success: Schema.Struct({ requestIds: Schema.Array(RuntimeRequestId) }),
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
export const PendingRequestReadTool = Tool.make("t3_pending_request_read", {
  ...commandTool,
  description:
    "Read a verified descendant question or approval. Nested remote work: the parent threadId plus requestTarget and requestRoute from the task observation or notice. Pass the returned revision as expectedRevision when responding.",
  parameters: McpRouting.parameters(PendingRequestInput),
  success: pendingRequest,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
export const PendingRequestRespondTool = Tool.make("t3_pending_request_respond", {
  ...commandTool,
  description:
    "Answer one verified descendant question (`answers`) or runtime approval (`decision`). Keep the qualified target and route from the read; pass expectedRevision and one stable clientRequestId per answer. Agents cannot claim a human actor. The child keeps its permission mode. A Promachos conversation may also answer when no ancestor can (no parent, or the parent is deleted or idle 30 minutes); the answer is recorded as the Promachos's.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      ...PendingRequestInput.fields,
      answers: Schema.optional(ProviderUserInputAnswers),
      decision: Schema.optional(ProviderApprovalDecision),
    }),
  ),
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const PendingRequestDismissTool = Tool.make("t3_pending_request_dismiss", {
  ...commandTool,
  description:
    "Dismiss a durable descendant question without starting another turn. Read its current revision first; live questions require an answer or Stop.",
  parameters: McpRouting.parameters(PendingRequestInput),
}).annotate(Tool.Destructive, true);

const ThreadConfigurationTool = Tool.make("t3_thread_configuration", {
  ...commandTool,
  description: "Read a thread's provider, model, and modes. Omit `threadId` for this thread.",
  parameters: McpRouting.parameters(Schema.Struct({ threadId: Schema.optional(ThreadId) })),
  success: Schema.Struct({
    bare: Schema.optional(ThreadBare),
    tools: Schema.optional(ThreadTools),
    threadId: ThreadId,
    modelSelection: ModelSelection,
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
  }),
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
const ThreadConfigureTool = Tool.make("t3_thread_configure", {
  ...commandTool,
  description:
    "Set a thread's provider, model, and options. Omit `threadId` for this thread. Permission modes stay. Values from `orchestrator_capabilities`.",
  success: Schema.Union([ThreadConfigurationTool.successSchema, commandTool.success]),
  parameters: McpRouting.parameters(
    Schema.Struct({
      threadId: Schema.optional(ThreadId),
      modelSelection: Schema.optional(ModelSelection),
    }),
  ),
}).annotate(Tool.Destructive, true);

const transferResult = Schema.Struct({ sequence: NonNegativeInt, targetThreadId: ThreadId });
const ThreadForkTool = Tool.make("t3_thread_fork", {
  ...commandTool,
  description:
    "Fork a thread from a stable run or checkpoint. Omit `threadId` for this thread. Keeps source setup. No turn has run yet.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      threadId: Schema.optional(ThreadId),
      sourcePoint: OrchestrationV2ThreadForkSourcePoint,
      title: Schema.optional(TrimmedNonEmptyString),
    }),
  ),
  success: transferResult,
}).annotate(Tool.Destructive, true);
const ThreadMergeBackTool = Tool.make("t3_thread_merge_back", {
  ...commandTool,
  description:
    "Merge context from one thread into a related thread in the same project. Omit `sourceThreadId` for this thread.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      sourceThreadId: Schema.optional(ThreadId),
      targetThreadId: ThreadId,
      sourcePoint: OrchestrationV2ThreadForkSourcePoint,
    }),
  ),
  success: transferResult,
}).annotate(Tool.Destructive, true);
const ThreadTransfersTool = Tool.make("t3_thread_transfers", {
  ...commandTool,
  description: "Read context transfer status for a thread. Omit `threadId` for this thread.",
  parameters: McpRouting.parameters(Schema.Struct({ threadId: Schema.optional(ThreadId) })),
  success: Schema.Struct({
    transfers: Schema.Array(
      Schema.Struct({
        id: OrchestrationV2ContextTransfer.fields.id,
        sourceThreadId: OrchestrationV2ContextTransfer.fields.sourceThreadId,
        targetThreadId: OrchestrationV2ContextTransfer.fields.targetThreadId,
        status: OrchestrationV2ContextTransfer.fields.status,
      }),
    ),
  }),
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);

const ThreadSearchTool = Tool.make("t3_thread_search", {
  ...commandTool,
  description:
    "Search active threads by title, branch, PR, project, then message text. Skips subagent threads. `projectId`: one project, else yours, else all outside a thread. Not exhaustive.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      ...OrchestrationSearchThreadsInput.fields,
      projectId: Schema.optional(ProjectId),
    }),
  ),
  success: Schema.Struct({
    matches: Schema.Array(
      Schema.Struct({
        threadId: OrchestrationThreadSearchMatch.fields.threadId,
        projectId: OrchestrationThreadSearchMatch.fields.projectId,
        title: Schema.String,
        link: Schema.optional(Schema.String),
        source: Schema.Literals(["title", "user", "assistant"]),
        snippet: Schema.optional(OrchestrationThreadSearchMatch.fields.snippet),
        messageCreatedAt: Schema.optional(OrchestrationThreadSearchMatch.fields.messageCreatedAt),
      }),
    ),
  }),
  dependencies: [...commandTool.dependencies, ThreadSearch.ThreadSearch],
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);

const ScheduledTaskRunTool = Tool.make("run_scheduled_task_now", {
  ...commandTool,
  description:
    "Run one scheduled task now. Needs a full-access or default caller. Without clientRequestId each call is a new run; a negotiated request key recovers the original admission instead of dispatching again. The receipt stage covers admission or queued native acceptance, never provider completion.",
  parameters: McpRouting.parameters(Schema.Struct({ taskId: ScheduledTaskId })),
  success: Schema.Struct({
    taskId: ScheduledTaskId,
    manualReceipt: Schema.optional(McpManualRunReceipt),
    threadId: ScheduledTask.fields.threadId,
    lastRunStatus: ScheduledTask.fields.lastRunStatus,
    runCount: NonNegativeInt,
    nextRunAt: ScheduledTask.fields.nextRunAt,
  }),
  dependencies: [...commandTool.dependencies, ScheduledTaskService.ScheduledTaskService],
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

export const ThreadToolkit = Toolkit.make(
  ScheduledTaskRunTool,
  ThreadSearchTool,
  ThreadForkTool,
  ThreadMergeBackTool,
  ThreadTransfersTool,
  ThreadConfigurationTool,
  ThreadConfigureTool,
  PendingRequestTool,
  PendingRequestListTool,
  PendingRequestReadTool,
  PendingRequestRespondTool,
  PendingRequestDismissTool,
  ThreadOrganizeTool,
  QueueListTool,
  QueueReadTool,
  QueueEditTool,
  QueueCancelTool,
  QueueResumeTool,
  QueueReorderTool,
  QueuePromoteTool,
);
