import { RemoteSecretHandle } from "./remoteSecret.ts";
import { ThreadToolsRequest } from "./threadTools.ts";
import { McpScheduledMutationReceipt } from "./mcpRequest.ts";
import { PeerTaskResult, PeerTaskSnapshot, PeerTaskStop } from "./peerTask.ts";
import { BareLaunchRequest, BareSupport } from "./bareLaunch.ts";
import {
  ExecutionPurpose,
  ExecutionPurposeDelivery,
  PurposeIntent,
  PurposeSelection,
  RemotePurposeContinuation,
  SavedPurposeIntent,
} from "./executionPurpose.ts";
import { PRISM_DELEGATE_ROLE_GUIDE, PrismDelegateRoleInput, PrismLane } from "./prism.ts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { AuthMcpClientAccess } from "./auth.ts";
import {
  ContextTransferId,
  EnvironmentId,
  IsoDateTime,
  MessageId,
  NodeId,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  RunId,
  ScheduledTaskId,
  SecretRef,
  ThreadId,
  TrimmedNonEmptyString,
  TurnItemId,
} from "./baseSchemas.ts";
import {
  ScheduledTaskRunStatus,
  ScheduledTaskSchedule,
  ScheduledTaskUpsertSchedule,
} from "./scheduledTask.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";
import * as ScheduledTaskFork from "./scheduledTaskChecks.ts";
import { ThreadLinkedPullRequest } from "./threadPullRequest.ts";
import { SecretRequestField, SecretRequestFieldName } from "./secretRequest.ts";
import { ThreadTitleRegeneration } from "./threadTitle.ts";
import {
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  OrchestrationV2Actor,
  OrchestrationV2CreationSource,
  OrchestrationV2RunStatus,
  OrchestrationV2RestartCutView,
  OrchestrationV2TurnItemStatus,
} from "./orchestrationV2.ts";
import {
  ProviderOptionDescriptor,
  ProviderOptionSelection,
  ProviderOptionSelectionValue,
} from "./model.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";

const OrchestratorMcpPrompt = TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)).annotate({
  description: "Complete task or message text for the target agent.",
});
const OrchestratorMcpTitle = TrimmedNonEmptyString.check(Schema.isMaxLength(512)).annotate({
  description: "Optional concise display title.",
});
export const OrchestratorMcpClientRequestId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(256),
).annotate({ description: "Stable idempotency key to reuse when retrying this mutation." });

/**
 * OpenCode 1.15 has been observed serializing nested MCP union objects as JSON
 * strings. Keep the structured object as the documented form while accepting
 * that compatibility shape at the tool boundary and decoding it immediately.
 */
const OrchestratorMcpScheduleFromJsonString = Schema.fromJsonString(
  ScheduledTaskUpsertSchedule,
).annotate({
  description:
    "Compatibility-only JSON encoding of the schedule object. Prefer a structured object.",
});

const OrchestratorMcpSchedule = Schema.Union([
  ScheduledTaskUpsertSchedule,
  OrchestratorMcpScheduleFromJsonString,
]).annotate({
  description:
    "Trigger object: {type:'interval', everyMs}, {type:'fixed_time', timeOfDay, weekdays?}, or {type:'webhook'} to run on each request to a generated URL. Never stringify it unless the provider requires the compatibility form.",
});

const OrchestratorMcpTargetOptionsFromRecord = Schema.Record(
  Schema.String,
  ProviderOptionSelectionValue,
).pipe(
  Schema.decodeTo(
    Schema.Array(ProviderOptionSelection),
    SchemaTransformation.transformEffect({
      decode: (record) =>
        Effect.succeed(Object.entries(record).map(([id, value]) => ({ id, value }))),
      encode: (selections: ReadonlyArray<ProviderOptionSelection>) =>
        Effect.succeed(
          Object.fromEntries(selections.map((selection) => [selection.id, selection.value])),
        ),
    }),
  ),
);

export const OrchestratorMcpTargetOptions = Schema.Union([
  Schema.Array(ProviderOptionSelection),
  OrchestratorMcpTargetOptionsFromRecord,
]);
export type OrchestratorMcpTargetOptions = typeof OrchestratorMcpTargetOptions.Type;

export const OrchestratorMcpTarget = Schema.Struct({
  providerInstanceId: Schema.optional(
    ProviderInstanceId.annotate({
      description: "Configured provider instance id from orchestrator_capabilities.",
    }),
  ),
  driverKind: Schema.optional(
    ProviderDriverKind.annotate({
      description: "Provider driver kind; prefer providerInstanceId when available.",
    }),
  ),
  model: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "Model id advertised for the selected provider instance.",
    }),
  ),
  options: Schema.optional(
    OrchestratorMcpTargetOptions.annotate({
      description: "Model option selections advertised by orchestrator_capabilities.",
    }),
  ),
  environmentId: Schema.optional(
    EnvironmentId.annotate({
      description:
        "Run the task in a linked environment (t3_environment_links). It uses this thread's provider and model unless providerInstanceId and model name others from orchestrator_capabilities with this environmentId.",
    }),
  ),
  projectId: Schema.optional(
    ProjectId.annotate({
      description:
        "With environmentId: that environment's project to run in. Omit to use the one with the same repository as this thread's project.",
    }),
  ),
});
export type OrchestratorMcpTarget = typeof OrchestratorMcpTarget.Type;

export const OrchestratorMcpRuntimeMode = Schema.Union([Schema.Literal("inherit"), RuntimeMode]);
export type OrchestratorMcpRuntimeMode = typeof OrchestratorMcpRuntimeMode.Type;

export const OrchestratorMcpInteractionMode = Schema.Union([
  Schema.Literal("inherit"),
  ProviderInteractionMode,
]);
export type OrchestratorMcpInteractionMode = typeof OrchestratorMcpInteractionMode.Type;

export const OrchestratorMcpTaskRole = Schema.Literals([
  "implementation",
  "research",
  "review",
  "design",
  "test",
  "general",
]);
export type OrchestratorMcpTaskRole = typeof OrchestratorMcpTaskRole.Type;

export const OrchestratorMcpDelegatedTaskStatus = Schema.Literals([
  "queued",
  "running",
  "waiting",
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);
export type OrchestratorMcpDelegatedTaskStatus = typeof OrchestratorMcpDelegatedTaskStatus.Type;

export const OrchestratorMcpTerminalDelegatedTaskStatus = Schema.Literals([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);
export type OrchestratorMcpTerminalDelegatedTaskStatus =
  typeof OrchestratorMcpTerminalDelegatedTaskStatus.Type;

export const OrchestratorMcpDelegateTaskInput = Schema.Struct({
  purposeIntent: Schema.optional(PurposeIntent),
  bare: Schema.optional(BareLaunchRequest),
  tools: Schema.optional(ThreadToolsRequest),
  projectId: Schema.optional(ProjectId),
  workspaceStrategy: Schema.optional(OrchestrationV2ThreadLaunchWorkspaceStrategy),
  lane: Schema.optional(
    PrismLane.annotate({
      description:
        "Worker lane: easy, medium (default) or hard for difficulty, or ui for UI and frontend work. A ui task runs only on the ui lane's Colors, an explicit target must be one of them, and it fails visibly instead of falling back to another model.",
    }),
  ),
  prismRole: Schema.optional(
    PrismDelegateRoleInput.annotate({
      description: `Explicit Prism role kit: ${PRISM_DELEGATE_ROLE_GUIDE}. Omit to map role: review to reviewer, research to researcher, design to designer, and everything else to worker. dispatcher, retry and escalation are removed and work only to retry a request an older build accepted.`,
    }),
  ),
  task: OrchestratorMcpPrompt.annotate({
    description: "Self-contained task for one delegated child agent/subagent.",
  }),
  target: Schema.optional(OrchestratorMcpTarget),
  title: Schema.optional(OrchestratorMcpTitle),
  role: Schema.optional(OrchestratorMcpTaskRole),
  isolation: Schema.optional(
    Schema.Literals(["shared", "worktree"]).annotate({
      description:
        "Defaults to shared: use the parent's checkout. Worktree creates a separate checkout from the parent's current local commit before starting the child. Uncommitted edits are not copied.",
    }),
  ),
  mode: Schema.optional(
    Schema.Literals(["async", "wait"]).annotate({
      description:
        "Defaults to async. Use wait only when this turn needs the child's result before you can continue.",
    }),
  ),
  timeoutMs: Schema.optional(Schema.Number).annotate({
    description:
      "Wait budget for mode=wait only. Default 10 minutes. Elapsing it returns waitTimedOut=true on that call and does not cancel the child.",
  }),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
  runtimeMode: Schema.optional(
    OrchestratorMcpRuntimeMode.annotate({
      description:
        "Defaults to inherit; omit it unless the user asks for a supervised child. A narrower mode is not a read-only switch: on most providers, approval-required and auto-accept-edits pause the child at each action that needs approval. An ancestor thread may answer exposed descendant requests through t3_pending_request_respond; a thread cannot approve its own request. To limit what the child does, say so in the task. A mode ranked above this thread's is rejected; that ranking does not promise the child's provider permissions are narrower than this thread's.",
    }),
  ),
  interactionMode: Schema.optional(OrchestratorMcpInteractionMode),
});
export type OrchestratorMcpDelegateTaskInput = typeof OrchestratorMcpDelegateTaskInput.Type;

export const OrchestratorMcpDelegateTaskResult = Schema.Struct({
  purpose: Schema.optional(ExecutionPurpose),
  latestTerminalPurpose: Schema.optional(ExecutionPurpose),
  warnings: Schema.optional(Schema.Array(Schema.String)),
  remoteObservation: Schema.optional(
    Schema.Struct({
      availability: Schema.Literals(["current", "stale", "unsupported"]),
      snapshot: Schema.optional(PeerTaskSnapshot),
      originalRunId: Schema.optional(RunId),
      latestResult: Schema.optional(PeerTaskResult),
      stop: Schema.optional(PeerTaskStop),
    }),
  ),
  taskId: NodeId,
  /** Null for a task that runs in a linked environment; see `remoteChild`. */
  childThreadId: Schema.NullOr(ThreadId),
  /** The destination environment; threadId is absent until its launch is attached. */
  remoteChild: Schema.optional(
    Schema.Struct({
      environmentId: EnvironmentId,
      threadId: Schema.optional(ThreadId),
      label: Schema.String,
      launchError: Schema.optional(Schema.String),
    }),
  ),
  childRunId: Schema.NullOr(RunId),
  childNodeId: NodeId,
  status: OrchestratorMcpDelegatedTaskStatus,
  workState: Schema.Literals(["working", "waiting_for_children", "result_available"]),
  hasPendingChildRuns: Schema.Boolean,
  latestTerminalRunId: Schema.NullOr(RunId),
  latestTerminalStatus: Schema.NullOr(OrchestratorMcpTerminalDelegatedTaskStatus),
  latestTerminalSummary: Schema.NullOr(Schema.String),
  latestTerminalResultContextTransferId: Schema.NullOr(ContextTransferId),
  providerInstanceId: ProviderInstanceId,
  model: Schema.NullOr(Schema.String),
  summary: Schema.NullOr(Schema.String),
  resultContextTransferId: Schema.NullOr(ContextTransferId),
  hasPendingApproval: Schema.optional(Schema.Boolean).annotate({
    description:
      "An eligible descendant approval ends mode=wait. Read the qualified request before answering; the task keeps its permission mode.",
  }),
  hasPendingQuestion: Schema.Boolean.annotate({
    description:
      "Blocking question ends mode=wait. Local work uses childThreadId; remote work uses the source parent threadId and qualified requestTarget/requestRoute from remoteObservation.snapshot or its notice. Read the current request before responding; task completion still wakes this thread.",
  }),
  waitTimedOut: Schema.Boolean.annotate({
    description:
      "True only on that mode=wait call when timeoutMs elapsed. The timeout does not cancel the child. Later task_status reads return false and use status for liveness.",
  }),
});
export type OrchestratorMcpDelegateTaskResult = typeof OrchestratorMcpDelegateTaskResult.Type;

/**
 * A Run group: the tasks one delegation started on several Colors of a
 * read-only role, in Color order. `taskId` reads and cancels each member;
 * `groupId` reads and cancels them all.
 */
export const OrchestratorMcpRunGroupResult = Schema.Struct({
  groupId: NodeId,
  status: Schema.Literals(["running", "completed"]).annotate({
    description:
      "completed once every member and its nested work is terminal; each member's own status says how it ended.",
  }),
  tasks: Schema.Array(OrchestratorMcpDelegateTaskResult),
  waitTimedOut: Schema.Boolean.annotate({
    description:
      "True only on that mode=wait call when timeoutMs elapsed before every member was terminal.",
  }),
});
export type OrchestratorMcpRunGroupResult = typeof OrchestratorMcpRunGroupResult.Type;

/** One task, or the Run group a Run N role started. */
export const OrchestratorMcpTaskOrRunGroupResult = Schema.Union([
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpRunGroupResult,
]);
export type OrchestratorMcpTaskOrRunGroupResult = typeof OrchestratorMcpTaskOrRunGroupResult.Type;

export const OrchestratorMcpTaskStatusInput = Schema.Struct({
  taskId: NodeId.annotate({ description: "A taskId, or a Run group's groupId." }),
});
export type OrchestratorMcpTaskStatusInput = typeof OrchestratorMcpTaskStatusInput.Type;

export const OrchestratorMcpTaskCancelInput = Schema.Struct({
  taskId: NodeId.annotate({ description: "A taskId, or a Run group's groupId." }),
  reason: Schema.optional(Schema.String.check(Schema.isMaxLength(2_000))),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
});
export type OrchestratorMcpTaskCancelInput = typeof OrchestratorMcpTaskCancelInput.Type;

export const OrchestratorMcpTaskCancelResult = Schema.Struct({
  stopState: Schema.optional(PeerTaskStop.fields.state),
  taskId: NodeId,
  status: Schema.Literals(["cancel_requested", "completed", "failed", "cancelled", "interrupted"]),
});
export type OrchestratorMcpTaskCancelResult = typeof OrchestratorMcpTaskCancelResult.Type;

export const OrchestratorMcpRunGroupCancelResult = Schema.Struct({
  groupId: NodeId,
  tasks: Schema.Array(OrchestratorMcpTaskCancelResult),
});
export type OrchestratorMcpRunGroupCancelResult = typeof OrchestratorMcpRunGroupCancelResult.Type;

export const OrchestratorMcpTaskOrRunGroupCancelResult = Schema.Union([
  OrchestratorMcpTaskCancelResult,
  OrchestratorMcpRunGroupCancelResult,
]);
export type OrchestratorMcpTaskOrRunGroupCancelResult =
  typeof OrchestratorMcpTaskOrRunGroupCancelResult.Type;

export const OrchestratorMcpCreateThreadRequest = Schema.Struct({
  purposeIntent: Schema.optional(PurposeIntent),
  bare: Schema.optional(BareLaunchRequest),
  prompt: Schema.optional(OrchestratorMcpPrompt),
  title: Schema.optional(OrchestratorMcpTitle),
  target: Schema.optional(OrchestratorMcpTarget),
  runtimeMode: Schema.optional(OrchestratorMcpRuntimeMode),
  interactionMode: Schema.optional(OrchestratorMcpInteractionMode),
});
export type OrchestratorMcpCreateThreadRequest = typeof OrchestratorMcpCreateThreadRequest.Type;

export const OrchestratorMcpCreateThreadsInput = Schema.Struct({
  threads: Schema.Array(OrchestratorMcpCreateThreadRequest).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(20),
  ),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
});
export type OrchestratorMcpCreateThreadsInput = typeof OrchestratorMcpCreateThreadsInput.Type;

export const OrchestratorMcpCreatedThreadStatus = Schema.Union([
  Schema.Literal("idle"),
  Schema.Literal("preparing"),
  Schema.Literal("starting"),
  OrchestratorMcpDelegatedTaskStatus,
  Schema.Literal("rolled_back"),
]);
export type OrchestratorMcpCreatedThreadStatus = typeof OrchestratorMcpCreatedThreadStatus.Type;

export const OrchestratorMcpCreatedThread = Schema.Struct({
  threadId: ThreadId,
  runId: Schema.NullOr(RunId),
  status: OrchestratorMcpCreatedThreadStatus,
  title: Schema.String,
  createdBy: OrchestrationV2Actor,
  creationSource: OrchestrationV2CreationSource,
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
});
export type OrchestratorMcpCreatedThread = typeof OrchestratorMcpCreatedThread.Type;

export const OrchestratorMcpCreateThreadsResult = Schema.Struct({
  threads: Schema.Array(OrchestratorMcpCreatedThread),
});
export type OrchestratorMcpCreateThreadsResult = typeof OrchestratorMcpCreateThreadsResult.Type;

export const OrchestratorMcpThreadStatus = Schema.Union([
  Schema.Literal("idle"),
  OrchestrationV2RunStatus,
]);
export type OrchestratorMcpThreadStatus = typeof OrchestratorMcpThreadStatus.Type;

const OrchestratorMcpProjectTarget = Schema.optional(
  ProjectId.annotate({
    description:
      "Project to act on. Omit for the calling thread's project; required when the caller is not a T3 thread.",
  }),
);

/**
 * A linked environment to act in instead of this one. The tools that take it
 * forward the call there, where the link's access and the caller's own modes
 * both apply.
 */
export const OrchestratorMcpEnvironmentTarget = Schema.optional(
  EnvironmentId.annotate({
    description:
      "A linked environment to do this in; t3_environment_links lists them. Omit for this environment. Ids (projects, threads, runs) then belong to that environment.",
  }),
);

export const OrchestratorMcpThreadListInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  projectId: OrchestratorMcpProjectTarget,
  statuses: Schema.optional(
    Schema.Array(OrchestratorMcpThreadStatus).check(Schema.isMaxLength(10)),
  ),
  titleContains: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(256))),
  settled: Schema.optional(Schema.Boolean),
  snoozed: Schema.optional(Schema.Boolean),
  includeSubagents: Schema.optional(Schema.Boolean),
  cursor: Schema.optional(NonNegativeInt),
  limit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))),
});
export type OrchestratorMcpThreadListInput = typeof OrchestratorMcpThreadListInput.Type;

export const OrchestratorMcpThreadListItem = Schema.Struct({
  threadId: ThreadId,
  title: Schema.String,
  createdBy: OrchestrationV2Actor,
  creationSource: OrchestrationV2CreationSource,
  status: OrchestratorMcpThreadStatus,
  latestRunId: Schema.NullOr(RunId),
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  linkedPullRequest: Schema.NullOr(ThreadLinkedPullRequest),
  settled: Schema.Boolean,
  settledAt: Schema.NullOr(IsoDateTime),
  snoozed: Schema.Boolean,
  snoozedUntil: Schema.NullOr(IsoDateTime),
  parentThreadId: Schema.NullOr(ThreadId),
  relationshipToParent: Schema.NullOr(Schema.Literals(["fork", "subagent"])),
  itemCount: NonNegativeInt,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type OrchestratorMcpThreadListItem = typeof OrchestratorMcpThreadListItem.Type;

export const OrchestratorMcpThreadListResult = Schema.Struct({
  projectId: ProjectId,
  currentThreadId: Schema.NullOr(ThreadId),
  threads: Schema.Array(OrchestratorMcpThreadListItem),
  nextCursor: Schema.NullOr(NonNegativeInt),
  total: NonNegativeInt,
});
export type OrchestratorMcpThreadListResult = typeof OrchestratorMcpThreadListResult.Type;

export const OrchestratorMcpThreadReadInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: ThreadId,
  itemId: Schema.optional(TurnItemId),
  textOffset: Schema.optional(NonNegativeInt),
  view: Schema.optional(Schema.Literals(["messages", "activity"])),
  afterPosition: Schema.optional(NonNegativeInt),
  limit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))),
  runLimit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(50))),
  maxCharsPerItem: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(50_000))),
});
export type OrchestratorMcpThreadReadInput = typeof OrchestratorMcpThreadReadInput.Type;

export const OrchestratorMcpThreadDetail = Schema.Struct({
  purposeSelection: Schema.optional(PurposeSelection),
  purposeDeliveries: Schema.optional(Schema.Array(ExecutionPurposeDelivery)),
  threadId: ThreadId,
  projectId: ProjectId,
  title: Schema.String,
  createdBy: OrchestrationV2Actor,
  creationSource: OrchestrationV2CreationSource,
  status: OrchestratorMcpThreadStatus,
  latestRunId: Schema.NullOr(RunId),
  activeRunId: Schema.NullOr(RunId),
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  linkedPullRequest: Schema.NullOr(ThreadLinkedPullRequest),
  titleRegeneration: Schema.NullOr(ThreadTitleRegeneration),
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  parentThreadId: Schema.NullOr(ThreadId),
  relationshipToParent: Schema.NullOr(Schema.Literals(["fork", "subagent"])),
  runCount: NonNegativeInt,
  itemCount: NonNegativeInt,
  pendingRequestCount: NonNegativeInt,
  archived: Schema.Boolean,
  settled: Schema.Boolean,
  settledAt: Schema.NullOr(IsoDateTime),
  snoozed: Schema.Boolean,
  snoozedUntil: Schema.NullOr(IsoDateTime),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type OrchestratorMcpThreadDetail = typeof OrchestratorMcpThreadDetail.Type;

const OrchestratorMcpRunRecovery = Schema.Union([
  Schema.Struct({ state: Schema.Literal("none") }),
  Schema.Struct({ state: Schema.Literal("waiting_on_quota"), resetAt: IsoDateTime }),
  Schema.Struct({
    state: Schema.Literal("recovery_scheduled"),
    reason: Schema.Literal("retry"),
    expectedStartAt: Schema.NullOr(IsoDateTime).annotate({
      description:
        "Expected start time, or null for an immediate retry with no persisted scheduled instant.",
    }),
  }),
  Schema.Struct({ state: Schema.Literal("continued_by"), successorRunId: RunId }),
  Schema.Struct({ state: Schema.Literal("given_up"), reason: Schema.String }),
]);

export const OrchestratorMcpThreadRun = Schema.Struct({
  purpose: Schema.optional(ExecutionPurpose),
  runId: RunId,
  ordinal: PositiveInt,
  status: OrchestrationV2RunStatus,
  providerInstanceId: ProviderInstanceId,
  model: Schema.String,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  restartCut: Schema.optional(OrchestrationV2RestartCutView),
  // Older linked environments omit this field; absence means unknown, not no recovery.
  recovery: Schema.optional(OrchestratorMcpRunRecovery),
});
export type OrchestratorMcpThreadRun = typeof OrchestratorMcpThreadRun.Type;

export const OrchestratorMcpThreadTimelineItem = Schema.Struct({
  position: NonNegativeInt,
  visibility: Schema.Literals(["local", "inherited", "synthetic"]),
  sourceThreadId: ThreadId,
  itemId: TurnItemId,
  subagentId: Schema.optional(NodeId),
  runId: Schema.NullOr(RunId),
  messageId: Schema.NullOr(MessageId),
  createdBy: Schema.NullOr(OrchestrationV2Actor),
  creationSource: Schema.NullOr(OrchestrationV2CreationSource),
  type: Schema.String,
  status: OrchestrationV2TurnItemStatus,
  title: Schema.NullOr(Schema.String),
  text: Schema.NullOr(Schema.String),
  textTruncated: Schema.Boolean,
  nextTextOffset: Schema.optional(Schema.NullOr(NonNegativeInt)),
  updatedAt: IsoDateTime,
});
export type OrchestratorMcpThreadTimelineItem = typeof OrchestratorMcpThreadTimelineItem.Type;

export const OrchestratorMcpThreadReadResult = Schema.Struct({
  thread: OrchestratorMcpThreadDetail,
  recentRuns: Schema.Array(OrchestratorMcpThreadRun),
  items: Schema.Array(OrchestratorMcpThreadTimelineItem),
  nextPosition: Schema.NullOr(NonNegativeInt),
  hasMore: Schema.Boolean,
});
export type OrchestratorMcpThreadReadResult = typeof OrchestratorMcpThreadReadResult.Type;

export const OrchestratorMcpThreadSendInput = Schema.Struct({
  purposeIntent: Schema.optional(PurposeIntent),
  /** Generated by the origin; ordinary callers cannot claim a forwarded continuation. */
  delegatedContinuation: Schema.optional(RemotePurposeContinuation),
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: ThreadId,
  message: OrchestratorMcpPrompt,
  mode: Schema.optional(Schema.Literals(["auto", "queue", "steer", "restart"])),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
});
export type OrchestratorMcpThreadSendInput = typeof OrchestratorMcpThreadSendInput.Type;

export const OrchestratorMcpThreadSendResult = Schema.Union([
  Schema.Struct({
    purpose: Schema.optional(ExecutionPurpose),
    warnings: Schema.optional(Schema.Array(Schema.String)),
    threadId: ThreadId,
    messageId: MessageId,
    runId: Schema.Null,
    taskId: NodeId,
    status: Schema.Literal("queued"),
    /** Persisted on the child's environment; parent intake has not acknowledged it yet. */
    delivery: Schema.Literal("pending"),
  }),
  Schema.Struct({
    threadId: ThreadId,
    messageId: MessageId,
    runId: RunId,
    purpose: Schema.optional(ExecutionPurpose),
    warnings: Schema.optional(Schema.Array(Schema.String)),
    taskId: Schema.optional(NodeId),
    status: OrchestrationV2RunStatus,
    delivery: Schema.Literals(["started", "queued", "steered", "restarted"]),
  }),
  Schema.Struct({
    threadId: ThreadId,
    messageId: MessageId,
    runId: Schema.Null,
    status: Schema.Literal("idle"),
    delivery: Schema.Literal("transcript"),
  }),
]);
export type OrchestratorMcpThreadSendResult = typeof OrchestratorMcpThreadSendResult.Type;

export const OrchestratorMcpThreadWaitInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: ThreadId,
  runId: Schema.optional(RunId),
  timeoutMs: Schema.optional(Schema.Number),
});
export type OrchestratorMcpThreadWaitInput = typeof OrchestratorMcpThreadWaitInput.Type;

export const OrchestratorMcpThreadWaitResult = Schema.Struct({
  threadId: ThreadId,
  runId: Schema.NullOr(RunId),
  status: OrchestratorMcpThreadStatus,
  timedOut: Schema.Boolean,
});
export type OrchestratorMcpThreadWaitResult = typeof OrchestratorMcpThreadWaitResult.Type;

export const OrchestratorMcpThreadInterruptInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
  threadId: ThreadId,
  runId: Schema.optional(RunId),
  reason: Schema.optional(Schema.String.check(Schema.isMaxLength(2_000))),
  /**
   * Stop the thread as its Stop button does instead: interrupt its running
   * turn whichever run that is, hold its queued messages, end its pull request
   * watches and stop every task it delegated. `runId` is then not used.
   */
  stop: Schema.optional(Schema.Boolean),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
});
export type OrchestratorMcpThreadInterruptInput = typeof OrchestratorMcpThreadInterruptInput.Type;

export const OrchestratorMcpThreadInterruptResult = Schema.Struct({
  threadId: ThreadId,
  runId: Schema.NullOr(RunId),
  status: Schema.Union([
    Schema.Literal("interrupt_requested"),
    Schema.Literal("no_active_run"),
    Schema.Literals(["completed", "failed", "cancelled", "interrupted", "rolled_back"]),
  ]),
});
export type OrchestratorMcpThreadInterruptResult = typeof OrchestratorMcpThreadInterruptResult.Type;

export const OrchestratorMcpProviderCapability = Schema.Struct({
  bare: Schema.optional(BareSupport),
  providerInstanceId: ProviderInstanceId,
  driverKind: ProviderDriverKind,
  displayName: Schema.NullOr(Schema.String),
  models: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      label: Schema.NullOr(Schema.String),
      options: Schema.optional(Schema.Array(ProviderOptionDescriptor)),
    }),
  ),
  runtimeModes: Schema.optional(Schema.Array(RuntimeMode)),
  canRunChildTask: Schema.Boolean,
  canRunCrossProviderChildTask: Schema.Boolean,
  constraints: Schema.Array(Schema.String),
});
export type OrchestratorMcpProviderCapability = typeof OrchestratorMcpProviderCapability.Type;

export const OrchestratorMcpCapabilitiesInput = Schema.Struct({
  environmentId: OrchestratorMcpEnvironmentTarget,
});
export type OrchestratorMcpCapabilitiesInput = typeof OrchestratorMcpCapabilitiesInput.Type;

export const OrchestratorMcpEnvironmentLink = Schema.Struct({
  environmentId: EnvironmentId,
  label: Schema.String,
  /** Whether it answered just now. An expired link must be linked again there. */
  status: Schema.Literals(["reachable", "unreachable", "expired"]),
  /** The most the linked environment lets this one's agents do there. */
  access: AuthMcpClientAccess,
  expiresAt: IsoDateTime,
  lastError: Schema.NullOr(Schema.String),
});
export type OrchestratorMcpEnvironmentLink = typeof OrchestratorMcpEnvironmentLink.Type;

export const OrchestratorMcpEnvironmentLinksResult = Schema.Struct({
  environmentId: EnvironmentId,
  links: Schema.Array(OrchestratorMcpEnvironmentLink),
});
export type OrchestratorMcpEnvironmentLinksResult =
  typeof OrchestratorMcpEnvironmentLinksResult.Type;

export const OrchestratorMcpCapabilitiesResult = Schema.Struct({
  executionPurposeVersions: Schema.optional(Schema.Array(Schema.Literal(1))),
  peerAttachments: Schema.optional(Schema.Literal(1)),
  peerRequests: Schema.optional(Schema.Literal(1)),
  bareLaunch: Schema.optional(Schema.Boolean),
  toolAllowlist: Schema.optional(Schema.Boolean),
  /** The calling thread, or null when the caller is not a T3 thread. */
  parentThreadId: Schema.NullOr(ThreadId),
  inheritedProviderInstanceId: Schema.NullOr(ProviderInstanceId),
  inheritedModel: Schema.NullOr(Schema.String),
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  providers: Schema.Array(OrchestratorMcpProviderCapability),
  features: Schema.Struct({
    appOwnedSubagents: Schema.Boolean,
    asyncPolling: Schema.Boolean,
    cancellation: Schema.Boolean,
    batchThreadCreation: Schema.Boolean,
    threadManagement: Schema.Boolean,
    incrementalThreadRead: Schema.Boolean,
    scheduledTasks: Schema.Boolean,
    maxBatchThreads: Schema.Number,
  }),
});
export type OrchestratorMcpCapabilitiesResult = typeof OrchestratorMcpCapabilitiesResult.Type;

export const OrchestratorMcpScheduleTaskInput = Schema.Struct({
  purposeIntent: Schema.optional(SavedPurposeIntent),
  bare: Schema.optional(BareLaunchRequest),
  projectId: OrchestratorMcpProjectTarget,
  prompt: OrchestratorMcpPrompt.annotate({
    description: "Prompt executed on every scheduled run.",
  }),
  schedule: OrchestratorMcpSchedule,
  title: Schema.optional(OrchestratorMcpTitle),
  enabled: Schema.optional(
    Schema.Boolean.annotate({ description: "Whether the schedule starts enabled; defaults true." }),
  ),
  bindToCurrentThread: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "When omitted, a once run within 55 minutes, an interval of 55 minutes or less, or a webhook posts into this thread; a later once, a longer interval, fixed_time, or weekly starts a fresh thread. True binds to this thread; false starts a fresh thread per run.",
    }),
  ),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId),
  ...ScheduledTaskFork.ScheduledTaskOutcomeCheckCreateFields,
});
export type OrchestratorMcpScheduleTaskInput = typeof OrchestratorMcpScheduleTaskInput.Type;

export const OrchestratorMcpScheduledTask = Schema.Struct({
  purposeIntent: Schema.optional(SavedPurposeIntent),
  bare: Schema.optional(BareLaunchRequest),
  scheduledTaskId: ScheduledTaskId,
  title: Schema.String,
  prompt: Schema.String,
  enabled: Schema.Boolean,
  projectId: ProjectId,
  boundThreadId: Schema.NullOr(ThreadId),
  schedule: ScheduledTaskSchedule,
  nextRunAt: Schema.NullOr(IsoDateTime),
  lastRunStatus: ScheduledTaskRunStatus,
  webhookUrl: Schema.optional(Schema.String).annotate({
    description:
      "Public URL to give the sender. Absent when this environment has no T3 Connect managed tunnel; the user must enable T3 Connect remote access first.",
  }),
  webhookSignature: Schema.optional(Schema.Literals(["none", "set"])).annotate({
    description: "Whether requests must carry a valid signature.",
  }),
  ...ScheduledTaskFork.ScheduledTaskForkFields,
});
export type OrchestratorMcpScheduledTask = typeof OrchestratorMcpScheduledTask.Type;

export const OrchestratorMcpScheduleTaskResult = Schema.Union([
  OrchestratorMcpScheduledTask,
  McpScheduledMutationReceipt,
]);
export type OrchestratorMcpScheduleTaskResult = typeof OrchestratorMcpScheduleTaskResult.Type;

export const OrchestratorMcpListScheduledTasksInput = Schema.Struct({
  projectId: Schema.optional(
    ProjectId.annotate({
      description:
        "Only list this project's tasks. Omit for the calling thread's project, or for every project when the caller is not a T3 thread.",
    }),
  ),
});
export type OrchestratorMcpListScheduledTasksInput =
  typeof OrchestratorMcpListScheduledTasksInput.Type;

export const OrchestratorMcpListScheduledTasksResult = Schema.Struct({
  tasks: Schema.Array(OrchestratorMcpScheduledTask),
});
export type OrchestratorMcpListScheduledTasksResult =
  typeof OrchestratorMcpListScheduledTasksResult.Type;

export const OrchestratorMcpUpdateScheduledTaskInput = Schema.Struct({
  purposeIntent: Schema.optional(SavedPurposeIntent),
  bare: Schema.optional(BareLaunchRequest),
  scheduledTaskId: ScheduledTaskId,
  prompt: Schema.optional(OrchestratorMcpPrompt),
  title: Schema.optional(OrchestratorMcpTitle),
  schedule: Schema.optional(OrchestratorMcpSchedule),
  enabled: Schema.optional(Schema.Boolean),
  bindToCurrentThread: Schema.optional(Schema.Boolean),
  ...ScheduledTaskFork.ScheduledTaskOutcomeCheckUpdateFields,
});
export type OrchestratorMcpUpdateScheduledTaskInput =
  typeof OrchestratorMcpUpdateScheduledTaskInput.Type;

export const OrchestratorMcpRequestSecretInput = Schema.Struct({
  environmentId: Schema.optional(EnvironmentId).annotate({
    description: "Destination for a single webhook signing value. Omit for private local files.",
  }),
  projectId: Schema.optional(ProjectId).annotate({
    description:
      "Required with a remote destination; the request can only serve a webhook in this project.",
  }),
  label: TrimmedNonEmptyString.annotate({
    description: "What you need, shown as the card's title, e.g. 'GitHub webhook secret'.",
  }),
  reason: TrimmedNonEmptyString.annotate({
    description:
      "One or two sentences on what it is for and where the user gets or also enters it.",
  }),
  placeholder: Schema.optional(TrimmedNonEmptyString).annotate({
    description:
      "Hint inside the input, e.g. 'Paste your GitHub token'. Single-field requests only; each entry in fields has its own.",
  }),
  fields: Schema.optional(
    Schema.Array(SecretRequestField).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(5),
      Schema.makeFilter(
        (fields) => new Set(fields.map((field) => field.name)).size === fields.length,
        { expected: "fields with unique names" },
      ),
    ),
  ).annotate({
    description:
      "Ask for several values on one card, e.g. [{name:'client_id', label:'Client ID'}, {name:'client_secret', label:'Client secret'}]. 1-5 entries; names are unique, lowercase [a-z0-9_-], and become the keys of files. Omit for one value.",
  }),
  timeoutMs: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1_000, maximum: 60 * 60 * 1_000 })),
  ).annotate({ description: "How long to wait for the user. Default 10 minutes." }),
  clientRequestId: Schema.optional(OrchestratorMcpClientRequestId).annotate({
    description:
      "Reuse when retrying a call that lost its result, so the user sees one card and its answer is returned again. Use a new id to ask again after timed_out or cancelled.",
  }),
});
export type OrchestratorMcpRequestSecretInput = typeof OrchestratorMcpRequestSecretInput.Type;

export const OrchestratorMcpRequestSecretResult = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("saved"),
    secretRequest: RemoteSecretHandle.annotate({
      description:
        "Destination-owned one-use request. Pass to schedule_task signature.secretRequest in the same destination/project; contains no value and grants no authority.",
    }),
  }),
  Schema.Struct({
    status: Schema.Literal("saved").annotate({
      description: "files holds the values; secretRef too for a single-field request.",
    }),
    files: Schema.Record(SecretRequestFieldName, TrimmedNonEmptyString).annotate({
      description:
        "Absolute path of a private file per field name ('secret' without fields), holding exactly that value. Use it without printing it, e.g. \"$(cat path)\" or stdin, and delete the file when done. It expires after 24 hours.",
    }),
    secretRef: Schema.optional(SecretRef).annotate({
      description:
        "Single-field requests only. Pass it to a tool that accepts a secretRef (schedule_task's signature.secretRef); it works once.",
    }),
  }),
  Schema.Struct({
    status: Schema.Literals(["declined", "cancelled", "timed_out"]).annotate({
      description:
        "declined: the user chose not to. cancelled: the request ended with the run. timed_out: the user did not answer in time; the card is closed, so ask again with a new clientRequestId if still needed.",
    }),
  }),
]);
export type OrchestratorMcpRequestSecretResult = typeof OrchestratorMcpRequestSecretResult.Type;

export const OrchestratorMcpDeleteScheduledTaskInput = Schema.Struct({
  scheduledTaskId: ScheduledTaskId,
});
export type OrchestratorMcpDeleteScheduledTaskInput =
  typeof OrchestratorMcpDeleteScheduledTaskInput.Type;

export const OrchestratorMcpDeleteScheduledTaskResult = Schema.Union([
  Schema.Struct({
    scheduledTaskId: ScheduledTaskId,
    deleted: Schema.Boolean,
  }),
  McpScheduledMutationReceipt,
]);
export type OrchestratorMcpDeleteScheduledTaskResult =
  typeof OrchestratorMcpDeleteScheduledTaskResult.Type;

export class OrchestratorMcpFailure extends Schema.TaggedError<OrchestratorMcpFailure>()(
  "OrchestratorMcpFailure",
  {
    code: Schema.Literals([
      "capability_denied",
      "outcome_unknown",
      "parent_not_active",
      "provider_unavailable",
      "research_host_unavailable",
      "research_budget_exceeded",
      "session_unavailable",
      "session_expired",
      "session_in_use",
      "session_warning",
      "model_unavailable",
      "runtime_mode_escalation_denied",
      "interaction_mode_escalation_denied",
      "task_not_found",
      "task_not_cancellable",
      "thread_not_found",
      "run_not_found",
      "thread_not_sendable",
      "thread_not_interruptible",
      "invalid_request",
      "orchestration_error",
      "thread_credential_required",
      "target_required",
    ]),
    message: Schema.String,
    requestId: Schema.optional(Schema.String),
    lastConfirmedStage: Schema.optional(
      Schema.Literals(["dispatch_started", "native_binding_accepted"]),
    ),
  },
) {}
