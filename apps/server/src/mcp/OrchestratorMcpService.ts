import * as ThreadMcpConfig from "../orchestration-v2/ThreadMcpConfig.ts";
import { toolPolicyRejection, toolAllowlistsEqual } from "@t3tools/provider-core/server/toolPolicy";

import { interactiveAuthority } from "../peer/PeerAuthorityRecord.ts";
import {
  executionPurposeOf,
  executionPurposesEqual,
  purposeIntentsEqual,
  type ExecutionPurpose,
} from "@t3tools/contracts";
import { executionPurposeWarnings } from "@t3tools/contracts";
import * as ScheduleMutation from "../scheduledTasks/mutationReceipts.ts";
import { requestNamespace } from "./requestIdentity.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import type { BareReadGrant } from "../orchestration-v2/bareReadGrant.ts";
import * as BareRuntime from "@t3tools/provider-core/server/bareLaunch";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as RequestReceipts from "./McpRequestReceipts.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { callerLinkOrigin, assertSameLink } from "./linkOrigin.ts";
import { SpectrumMcpInterrupt } from "../spectrum/mcpInterrupt.ts";
import { SpectrumMcpSend } from "../spectrum/mcpSend.ts";
import * as Prism from "../prism/PrismService.ts";
import {
  CommandId,
  type RunId,
  isProviderAvailable,
  MessageId,
  type ModelSelection,
  NodeId,
  TurnItemId,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadLaunchWorkspaceStrategy,
  type OrchestrationV2TurnItem,
  OrchestratorMcpFailure,
  type OrchestratorMcpCapabilitiesResult,
  type OrchestratorMcpCreateThreadsInput,
  type OrchestratorMcpCreateThreadsResult,
  type OrchestratorMcpCreatedThread,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpDelegateTaskResult,
  type OrchestratorMcpDeleteScheduledTaskInput,
  type OrchestratorMcpRequestSecretInput,
  type OrchestratorMcpRequestSecretResult,
  type OrchestratorMcpDeleteScheduledTaskResult,
  type OrchestratorMcpListScheduledTasksResult,
  type OrchestratorMcpScheduledTask,
  type OrchestratorMcpScheduleTaskInput,
  type OrchestratorMcpScheduleTaskResult,
  type OrchestratorMcpTarget,
  type OrchestratorMcpTaskCancelInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestratorMcpTaskOrRunGroupCancelResult,
  type OrchestratorMcpTaskOrRunGroupResult,
  type OrchestratorMcpRunGroupResult,
  type OrchestratorMcpUpdateScheduledTaskInput,
  type OrchestratorMcpListScheduledTasksInput,
  type ProjectId,
  type OrchestratorMcpThreadDetail,
  type OrchestratorMcpThreadInterruptInput,
  type OrchestratorMcpThreadInterruptResult,
  type OrchestratorMcpThreadListInput,
  type OrchestratorMcpThreadListItem,
  type OrchestratorMcpThreadListResult,
  type OrchestratorMcpThreadReadInput,
  type OrchestratorMcpThreadReadResult,
  type OrchestratorMcpThreadRun,
  type OrchestratorMcpThreadSendInput,
  type OrchestratorMcpThreadSendResult,
  type OrchestratorMcpThreadTimelineItem,
  type OrchestratorMcpThreadWaitInput,
  type OrchestratorMcpThreadWaitResult,
  type ProviderInteractionMode,
  type ProviderOptionDescriptor,
  type ProviderOptionSelection,
  type RuntimeMode,
  type ScheduledTask,
  type ScheduledTaskError,
  type ScheduledTaskUpsertInput,
  type ServerProvider,
  ThreadId,
  threadOwner,
  admitDelegateRole,
  PRISM_DELEGATE_ROLE_ERROR,
} from "@t3tools/contracts";
import { runGroupMembers } from "../orchestration-v2/RunGroups.ts";
import { PurposeAdmissionError } from "../orchestration-v2/ExecutionPurpose.ts";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import {
  subagentResultForRun,
  restartResultRunIds,
  delegatedTaskProgress,
} from "@t3tools/provider-core/server/subagentProjection";
import { restartCutView } from "@t3tools/shared/restartCut";
import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../orchestration-v2/DispatchModeLimit.ts";
import { runOutcome } from "../orchestration-v2/RestartBackgroundNote.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import { isSnoozed } from "../orchestration-v2/ThreadSettlementService.ts";
import * as FileSystem from "effect/FileSystem";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { assertProjectWorktree } from "../childThreads/workspaceAccess.ts";
import {
  delegatedTaskRuns,
  delegatedResultTransfers,
  latestDelegatedResultTransfer,
} from "../childThreads/delegatedFollowups.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ScheduledTaskChecks from "../scheduledTaskChecks/ScheduledTaskChecks.ts";
import {
  clientModeCeiling,
  isLiveCallerShell,
  intersectModes,
  type McpInvocationScope,
  type McpThreadInvocationScope,
  requireThreadScope,
} from "./McpInvocationContext.ts";
import * as Metrics from "../observability/Metrics.ts";
import {
  interactionModeRank,
  resolveInteractionMode,
  resolveRuntimeMode,
  runtimeModeRank,
  taskPrompt,
} from "./delegatedTaskModes.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";

const isMcpFailure = Schema.is(OrchestratorMcpFailure);

const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_WAIT_TIMEOUT_MS = 60 * 60 * 1_000;
const TASK_WAKE_EVENTS = [
  { thread: "parent", eventType: "subagent.updated" },
  { thread: "child", eventType: "run.updated" },
  { thread: "child", eventType: "subagent.updated" },
  { thread: "child", eventType: "provider-thread.updated" },
  { thread: "child", eventType: "runtime-request.updated" },
] as const;
const SECRET_REQUEST_POLL_INTERVAL_MS = 500;
const DEFAULT_THREAD_LIST_LIMIT = 50;
const DEFAULT_THREAD_READ_LIMIT = 50;
const DEFAULT_THREAD_RUN_LIMIT = 10;
const DEFAULT_THREAD_ITEM_MAX_CHARS = 20_000;

interface ResolvedTarget {
  readonly modelSelection: ModelSelection;
  readonly supportedRuntimeModes: ReadonlyArray<RuntimeMode> | undefined;
}

type TerminalTaskStatus = Extract<
  OrchestratorMcpDelegateTaskResult["status"],
  "completed" | "failed" | "cancelled" | "interrupted"
>;

export interface OrchestratorMcpServiceShape {
  readonly capabilities: (
    scope: McpInvocationScope,
  ) => Effect.Effect<OrchestratorMcpCapabilitiesResult, OrchestratorMcpFailure>;
  readonly delegateTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpDelegateTaskInput,
  ) => Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure>;
  readonly taskStatus: (
    scope: McpInvocationScope,
    taskId: NodeId,
  ) => Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure>;
  readonly cancelTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpTaskCancelInput,
  ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
  /** `delegate_task`: one task, or a Run group when the role's Run is above 1 (#402). */
  readonly delegateTaskOrRunGroup: (
    scope: McpInvocationScope,
    input: OrchestratorMcpDelegateTaskInput,
  ) => Effect.Effect<OrchestratorMcpTaskOrRunGroupResult, OrchestratorMcpFailure>;
  /** `task_status`: a task, or every member of the Run group `taskId` names. */
  readonly taskOrRunGroupStatus: (
    scope: McpInvocationScope,
    taskId: NodeId,
  ) => Effect.Effect<OrchestratorMcpTaskOrRunGroupResult, OrchestratorMcpFailure>;
  /** `task_cancel`: a task, or every member of the Run group `taskId` names. */
  readonly cancelTaskOrRunGroup: (
    scope: McpInvocationScope,
    input: OrchestratorMcpTaskCancelInput,
  ) => Effect.Effect<OrchestratorMcpTaskOrRunGroupCancelResult, OrchestratorMcpFailure>;
  readonly createThreads: (
    scope: McpInvocationScope,
    input: OrchestratorMcpCreateThreadsInput,
  ) => Effect.Effect<OrchestratorMcpCreateThreadsResult, OrchestratorMcpFailure>;
  readonly scheduleTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpScheduleTaskInput,
  ) => Effect.Effect<OrchestratorMcpScheduleTaskResult, OrchestratorMcpFailure>;
  readonly listScheduledTasks: (
    scope: McpInvocationScope,
    input: OrchestratorMcpListScheduledTasksInput,
  ) => Effect.Effect<OrchestratorMcpListScheduledTasksResult, OrchestratorMcpFailure>;
  readonly updateScheduledTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpUpdateScheduledTaskInput,
  ) => Effect.Effect<OrchestratorMcpScheduleTaskResult, OrchestratorMcpFailure>;
  readonly deleteScheduledTask: (
    scope: McpInvocationScope,
    input: OrchestratorMcpDeleteScheduledTaskInput,
  ) => Effect.Effect<OrchestratorMcpDeleteScheduledTaskResult, OrchestratorMcpFailure>;
  /**
   * The value never reaches the agent: the result is a status.
   */
  readonly requestSecret: (
    scope: McpInvocationScope,
    input: OrchestratorMcpRequestSecretInput,
  ) => Effect.Effect<OrchestratorMcpRequestSecretResult, OrchestratorMcpFailure>;
  readonly listThreads: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadListInput,
  ) => Effect.Effect<OrchestratorMcpThreadListResult, OrchestratorMcpFailure>;
  readonly readThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadReadInput,
  ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>;
  readonly sendToThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadSendInput,
  ) => Effect.Effect<OrchestratorMcpThreadSendResult, OrchestratorMcpFailure>;
  readonly waitForThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadWaitInput,
  ) => Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>;
  readonly interruptThread: (
    scope: McpInvocationScope,
    input: OrchestratorMcpThreadInterruptInput,
  ) => Effect.Effect<OrchestratorMcpThreadInterruptResult, OrchestratorMcpFailure>;
}

export class OrchestratorMcpService extends Context.Service<
  OrchestratorMcpService,
  OrchestratorMcpServiceShape
>()("t3/mcp/OrchestratorMcpService") {}

const isThreadManagementError = Schema.is(ThreadManagementService.ThreadManagementError);

function failure(code: OrchestratorMcpFailure["code"], message: string): OrchestratorMcpFailure {
  return new OrchestratorMcpFailure({ code, message });
}

function threadManagementFailure(error: unknown): OrchestratorMcpFailure {
  if (!isThreadManagementError(error)) return failure("orchestration_error", errorMessage(error));
  switch (error._tag) {
    case "ThreadManagementThreadNotFoundError":
      return failure("thread_not_found", error.message);
    case "ThreadManagementRunNotFoundError":
      return failure("run_not_found", error.message);
    case "ThreadManagementAncestorDeliveryModeError":
      return failure("invalid_request", error.message);
    case "ThreadManagementAncestryUnavailableError":
      return failure("orchestration_error", error.message);
    case "ThreadManagementThreadArchivedError":
    case "ThreadManagementNoSteerableRunError":
      return failure("thread_not_sendable", error.message);
    case "ThreadManagementThreadNotInterruptibleError":
      return failure("thread_not_interruptible", error.message);
    case "ThreadManagementProjectionLoadError":
    case "ThreadManagementProjectThreadsListError":
    case "ThreadManagementDurableRunProjectionError":
      return failure("orchestration_error", error.message);
    case "ThreadManagementProviderInstanceDisabledError":
      return failure("invalid_request", error.message);
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function scheduledTaskWorkspaceStrategy(
  boundToThread: boolean,
): ScheduledTask["workspaceStrategy"] {
  return boundToThread
    ? { type: "root" }
    : { type: "worktree", baseRef: "main", startFromOrigin: true };
}

/**
 * Provider prompt caches last at most an hour. Five minutes of slack keeps a
 * run that is still inside that window from reloading the calling thread cold.
 */
const SCHEDULE_BIND_WINDOW_MS = 55 * 60 * 1_000;

/** A near once, a short interval, or a webhook stays on the caller. A later run starts fresh. */
function scheduleBindsToCallerByDefault(
  schedule: ScheduledTaskUpsertInput["schedule"],
  nowMs: number,
): boolean {
  switch (schedule.type) {
    case "webhook":
      return true;
    case "interval":
      return schedule.everyMs <= SCHEDULE_BIND_WINDOW_MS;
    case "once":
      return Date.parse(schedule.at) - nowMs <= SCHEDULE_BIND_WINDOW_MS;
    case "fixed_time":
    case "weekly":
      return false;
  }
}

function scheduledTaskSummary(task: ScheduledTask, mayRun: boolean): OrchestratorMcpScheduledTask {
  return {
    ...(task.purposeIntent === undefined ? {} : { purposeIntent: task.purposeIntent }),
    ...(task.bare === undefined ? {} : { bare: task.bare }),
    scheduledTaskId: task.id,
    title: task.title,
    prompt: task.prompt,
    enabled: task.enabled,
    projectId: task.projectId,
    boundThreadId: task.threadId,
    schedule: task.schedule,
    nextRunAt: task.nextRunAt,
    lastRunStatus: task.lastRunStatus,
    ...(task.webhook?.url == null || !mayRun ? {} : { webhookUrl: task.webhook.url }),
    ...(task.webhook === undefined
      ? {}
      : { webhookSignature: task.webhook.hasSecret ? "set" : "none" }),
    ...ScheduledTaskChecks.forkSummaryFields(task),
  };
}

function providerConstraints(
  provider: ServerProvider | undefined,
  supportsOrchestrationV2: boolean,
): ReadonlyArray<string> {
  const constraints: Array<string> = [];
  if (!supportsOrchestrationV2) {
    constraints.push("No V2 provider adapter is registered.");
  }
  if (provider === undefined) return constraints;
  if (!provider.enabled) constraints.push("Provider instance is disabled.");
  if (!provider.installed) constraints.push("Provider executable is not installed.");
  if (!isProviderAvailable(provider)) {
    constraints.push(provider.unavailableReason ?? "Provider driver is unavailable.");
  }
  if (provider.status === "error" || provider.status === "disabled") {
    constraints.push(provider.message ?? `Provider status is ${provider.status}.`);
  }
  if (provider.auth.status === "unauthenticated") {
    constraints.push("Provider is not authenticated.");
  }
  return constraints;
}

/**
 * Models without descriptors skip the descriptor checks (mirroring how model slugs are only
 * validated when the provider advertises models), but duplicate ids always fail: downstream
 * consumers disagree on whether the first or last value of a duplicated id wins.
 */
function invalidOptionSelections(
  selections: ReadonlyArray<ProviderOptionSelection>,
  descriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined,
): ReadonlyArray<string> {
  const problems: Array<string> = [];
  const seen = new Set<string>();
  for (const selection of selections) {
    if (seen.has(selection.id)) {
      problems.push(`Option ${selection.id} was specified more than once.`);
      continue;
    }
    seen.add(selection.id);
    if (descriptors === undefined) continue;
    const descriptor = descriptors.find((candidate) => candidate.id === selection.id);
    if (descriptor === undefined) {
      const known = descriptors.map((candidate) => candidate.id).join(", ");
      problems.push(`Unknown option ${selection.id}; supported options: ${known || "none"}.`);
      continue;
    }
    if (descriptor.type === "boolean" && typeof selection.value !== "boolean") {
      problems.push(`Option ${selection.id} expects a boolean value.`);
      continue;
    }
    if (
      descriptor.type === "select" &&
      !descriptor.options.some((choice) => choice.id === selection.value)
    ) {
      const choices = descriptor.options.map((choice) => choice.id).join(", ");
      problems.push(`Option ${selection.id} must be one of: ${choices}.`);
    }
  }
  return problems;
}

function taskStatusForRun(
  run: Pick<OrchestrationV2Run, "status" | "restartCancelledBackgroundWork"> | undefined,
): OrchestratorMcpDelegateTaskResult["status"] {
  switch (run === undefined ? undefined : runOutcome(run)) {
    case "queued":
      return "queued";
    case "waiting":
      return "waiting";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
    case "rolled_back":
      return "cancelled";
    case "interrupted":
      return "interrupted";
    case "preparing":
    case "starting":
    case "running":
    case undefined:
      return "running";
  }
}

export function delegatedTaskRun(
  childProjection: Pick<OrchestrationV2ThreadProjection, "runs" | "contextTransfers"> &
    Partial<Pick<OrchestrationV2ThreadProjection, "thread">>,
  task: OrchestrationV2Subagent,
): OrchestrationV2Run | undefined {
  const boundRun = childProjection.runs
    .filter((run) => run.delegatedTaskId === task.id)
    .toSorted((a, b) => a.ordinal - b.ordinal)[0];
  if (
    childProjection.thread?.forkedFrom?.type === "node" &&
    childProjection.thread.forkedFrom.nodeId !== task.id
  ) {
    return boundRun;
  }
  const spawnTransfer = childProjection.contextTransfers.find(
    (transfer) =>
      transfer.type === "subagent_spawn" &&
      transfer.sourceThreadId === task.threadId &&
      transfer.targetThreadId === task.childThreadId,
  );
  if (spawnTransfer === undefined) {
    // The first binding remains the original run when the same job takes more work.
    return boundRun ?? childProjection.runs.toSorted((a, b) => a.ordinal - b.ordinal)[0];
  }
  return spawnTransfer.targetRunId === null
    ? undefined
    : childProjection.runs.find((run) => run.id === spawnTransfer.targetRunId);
}

export function hasPendingChildRuns(
  childProjection: Pick<OrchestrationV2ThreadProjection, "runs"> &
    Partial<Pick<OrchestrationV2ThreadProjection, "messages">>,
  delegatedRun: OrchestrationV2Run | undefined,
): boolean {
  // Held queued runs wait for the user to resume the child; task_cancel holds them.
  const monitorIds = new Set(
    (childProjection.messages ?? [])
      .filter((message) => message.notification?.source.kind === "monitor")
      .map((message) => message.runId),
  );
  return childProjection.runs.some(
    (run) =>
      !ThreadManagementService.isTerminalRunStatus(run.status) &&
      !monitorIds.has(run.id) &&
      !(run.status === "queued" && run.queueHeld === true) &&
      (delegatedRun === undefined || run.ordinal > delegatedRun.ordinal),
  );
}

function isTerminalTaskStatus(
  status: OrchestratorMcpDelegateTaskResult["status"],
): status is TerminalTaskStatus {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

function directAppOwnedChildTasks(
  parent: Pick<OrchestrationV2ThreadProjection, "thread" | "subagents">,
  target: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
  returnedRunIds: ReadonlySet<RunId | null>,
): ReadonlyArray<OrchestrationV2Subagent> {
  if (
    target.thread.lineage.parentThreadId !== parent.thread.id ||
    target.thread.lineage.relationshipToParent !== "subagent"
  ) {
    return [];
  }
  const original = parent.subagents.find(
    (task) =>
      task.origin === "app_owned" &&
      task.threadId === parent.thread.id &&
      task.id ===
        (target.thread.forkedFrom?.type === "node" ? target.thread.forkedFrom.nodeId : undefined) &&
      task.childThreadId === target.thread.id,
  );
  const taskIds = new Set(
    target.runs
      .filter((run) => returnedRunIds.has(run.id))
      .map((run) => run.delegatedTaskId ?? original?.id),
  );
  return parent.subagents.filter(
    (task) =>
      taskIds.has(task.id) &&
      task.origin === "app_owned" &&
      task.childThreadId === target.thread.id,
  );
}

function taskResultTransfer(
  parent: Pick<OrchestrationV2ThreadProjection, "thread" | "contextTransfers">,
  child: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "contextTransfers">,
  task: OrchestrationV2Subagent,
) {
  const ordinals = new Map(child.runs.map((run) => [run.id, run.ordinal]));
  const ordinal = (transfer: (typeof parent.contextTransfers)[number]) =>
    transfer.sourcePoint.runId === undefined ? 0 : (ordinals.get(transfer.sourcePoint.runId) ?? 0);
  return delegatedResultTransfers(
    child,
    task.id,
    parent.contextTransfers,
    parent.thread.id,
  ).toSorted((a, b) => ordinal(a) - ordinal(b))[0];
}

function pageIncludesTerminalTaskResult(input: {
  readonly parent: Pick<OrchestrationV2ThreadProjection, "thread" | "contextTransfers">;
  readonly page: ReadonlyArray<OrchestrationV2ThreadProjection["visibleTurnItems"][number]>;
  readonly task: OrchestrationV2Subagent;
  readonly target: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "contextTransfers" | "messages" | "turnItems"
  >;
  readonly maxChars: number;
}): boolean {
  const transfer = latestDelegatedResultTransfer(
    input.target,
    input.task.id,
    input.parent.contextTransfers,
    input.parent.thread.id,
  );
  if (transfer === undefined) return false;
  const run =
    transfer.sourcePoint.runId === undefined
      ? delegatedTaskRun(input.target, input.task)
      : input.target.runs.find((run) => run.id === transfer.sourcePoint.runId);
  if (run === undefined || !isTerminalTaskStatus(taskStatusForRun(run))) return false;

  const result = subagentResultForRun(input.target, run);
  // Reading old partial output does not acknowledge the restart explanation.
  if (result.restartNote !== null) return false;
  if (result.messageId === null && result.turnItemId === null) return false;

  return input.page.some((row) => {
    if (row.sourceThreadId !== input.target.thread.id) return false;
    const matchesResult =
      (result.turnItemId !== null && row.sourceItemId === result.turnItemId) ||
      (result.messageId !== null &&
        row.item.type === "assistant_message" &&
        row.item.messageId === result.messageId);
    if (!matchesResult) return false;

    const text = turnItemText(row.item);
    return text !== null && text.length <= input.maxChars;
  });
}

function latestTerminalResultRun(
  projection: Pick<OrchestrationV2ThreadProjection, "messages" | "runs">,
  delegatedRun: OrchestrationV2Run | undefined,
): OrchestrationV2Run | undefined {
  const monitorRunIds = new Set(
    projection.messages
      .filter((message) => message.notification?.source.kind === "monitor")
      .map((message) => message.runId),
  );
  return projection.runs
    .filter(
      (run) =>
        ThreadManagementService.isTerminalRunStatus(run.status) &&
        !monitorRunIds.has(run.id) &&
        run.status !== "rolled_back" &&
        (run.id === delegatedRun?.id ||
          run.startedAt !== null ||
          run.restartContinuationOfRunId !== undefined ||
          (run.status === "cancelled" && run.restartCut !== undefined)),
    )
    .reduce<OrchestrationV2Run | undefined>(
      (latest, run) => (latest === undefined || runRanAfter(run, latest) ? run : latest),
      undefined,
    );
}

function canExposeTaskRunResult(run: OrchestrationV2Run | undefined): run is OrchestrationV2Run {
  return (
    run !== undefined && run.status !== "rolled_back" && isTerminalTaskStatus(taskStatusForRun(run))
  );
}

function stablePart(value: string): string {
  return encodeURIComponent(value);
}

function stableCommandId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly operation: string;
  readonly index?: number;
}): CommandId {
  return CommandId.make(
    [
      "command",
      "mcp",
      stablePart(requestNamespace(input.scope)),
      stablePart(input.operation),
      stablePart(input.requestKey),
      ...(input.index === undefined ? [] : [String(input.index)]),
    ].join(":"),
  );
}

function stableThreadId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly index: number;
}): ThreadId {
  return ThreadId.make(
    [
      "thread",
      "mcp",
      stablePart(requestNamespace(input.scope)),
      stablePart(input.requestKey),
      String(input.index),
    ].join(":"),
  );
}

function stableMessageId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly index: number;
}): MessageId {
  return MessageId.make(
    [
      "message",
      "mcp",
      stablePart(requestNamespace(input.scope)),
      stablePart(input.requestKey),
      String(input.index),
    ].join(":"),
  );
}

function stableOperationMessageId(input: {
  readonly scope: McpInvocationScope;
  readonly requestKey: string;
  readonly operation: string;
}): MessageId {
  return MessageId.make(
    [
      "message",
      "mcp",
      stablePart(requestNamespace(input.scope)),
      stablePart(input.operation),
      stablePart(input.requestKey),
    ].join(":"),
  );
}

function threadTitle(input: {
  readonly parentTitle: string;
  readonly prompt: string | undefined;
  readonly title: string | undefined;
  readonly index: number;
}): string {
  const detail = input.title?.trim() || input.prompt?.trim();
  if (!detail) return `${input.parentTitle} thread ${input.index + 1}`;
  return detail.length > 80 ? `${detail.slice(0, 77)}...` : detail;
}

function threadSettlement(
  thread: Pick<OrchestrationV2ThreadShell, "settledOverride" | "settledAt">,
): Pick<OrchestratorMcpThreadListItem, "settled" | "settledAt"> {
  const settled = thread.settledOverride === "settled";
  return {
    settled,
    settledAt: settled && thread.settledAt !== null ? DateTime.formatIso(thread.settledAt) : null,
  };
}

function threadSnooze(
  thread: Parameters<typeof isSnoozed>[0],
  nowMs: number,
): Pick<OrchestratorMcpThreadListItem, "snoozed" | "snoozedUntil"> {
  const snoozed = isSnoozed(thread, nowMs);
  return {
    snoozed,
    snoozedUntil:
      snoozed && thread.snoozedUntil != null ? DateTime.formatIso(thread.snoozedUntil) : null,
  };
}

function listItemFromShell(
  shell: OrchestrationV2ThreadShell,
  nowMs: number,
): OrchestratorMcpThreadListItem {
  return {
    threadId: shell.id,
    title: shell.title,
    createdBy: shell.createdBy,
    creationSource: shell.creationSource,
    status: shell.activityRunStatus ?? shell.status,
    latestRunId: shell.latestRunId,
    providerInstanceId: shell.modelSelection.instanceId,
    model: shell.modelSelection.model,
    runtimeMode: shell.runtimeMode,
    interactionMode: shell.interactionMode,
    linkedPullRequest: shell.linkedPullRequest ?? null,
    ...threadSettlement(shell),
    ...threadSnooze(shell, nowMs),
    parentThreadId: shell.lineage.parentThreadId,
    relationshipToParent: shell.lineage.relationshipToParent,
    itemCount: shell.visibleItemCount,
    createdAt: DateTime.formatIso(shell.createdAt),
    updatedAt: DateTime.formatIso(shell.updatedAt),
  };
}

function threadDetail(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "runtimeRequests">,
  itemCount: number,
  shell: OrchestrationV2ThreadShell,
  nowMs: number,
): OrchestratorMcpThreadDetail {
  const latest = ThreadManagementService.latestRun(projection);
  const active = ThreadManagementService.latestActiveRun(projection);
  return {
    ...(projection.thread.purposeSelection === undefined
      ? {}
      : { purposeSelection: projection.thread.purposeSelection }),
    ...(projection.thread.purposeDeliveries === undefined
      ? {}
      : { purposeDeliveries: projection.thread.purposeDeliveries }),
    threadId: projection.thread.id,
    projectId: projection.thread.projectId,
    title: projection.thread.title,
    createdBy: projection.thread.createdBy,
    creationSource: projection.thread.creationSource,
    status: active?.status ?? latest?.status ?? "idle",
    latestRunId: latest?.id ?? null,
    activeRunId: active?.id ?? null,
    providerInstanceId: projection.thread.modelSelection.instanceId,
    model: projection.thread.modelSelection.model,
    runtimeMode: projection.thread.runtimeMode,
    interactionMode: projection.thread.interactionMode,
    linkedPullRequest: projection.thread.linkedPullRequest ?? null,
    titleRegeneration:
      projection.thread.titleRegeneration === undefined ||
      projection.thread.titleRegeneration === null
        ? null
        : {
            requestId: projection.thread.titleRegeneration.requestId,
            startedAt: DateTime.formatIso(projection.thread.titleRegeneration.startedAt),
          },
    branch: projection.thread.branch,
    worktreePath: projection.thread.worktreePath,
    parentThreadId: projection.thread.lineage.parentThreadId,
    relationshipToParent: projection.thread.lineage.relationshipToParent,
    runCount: projection.runs.length,
    itemCount,
    pendingRequestCount: projection.runtimeRequests.filter(
      (request) => request.status === "pending",
    ).length,
    archived: projection.thread.archivedAt !== null,
    ...threadSettlement(projection.thread),
    ...threadSnooze(shell, nowMs),
    createdAt: DateTime.formatIso(projection.thread.createdAt),
    updatedAt: DateTime.formatIso(projection.thread.updatedAt),
  };
}

function threadRun(
  run: OrchestrationV2Run,
  cut: ReturnType<typeof restartCutView>,
  recovery: OrchestratorMcpThreadRun["recovery"],
): OrchestratorMcpThreadRun {
  return {
    purpose: executionPurposeOf(run),
    runId: run.id,
    recovery,
    ordinal: run.ordinal,
    status: run.status,
    providerInstanceId: run.modelSelection.instanceId,
    model: run.modelSelection.model,
    requestedAt: DateTime.formatIso(run.requestedAt),
    startedAt: run.startedAt === null ? null : DateTime.formatIso(run.startedAt),
    completedAt: run.completedAt === null ? null : DateTime.formatIso(run.completedAt),
    ...(cut === undefined ? {} : { restartCut: cut }),
  };
}

function jsonText(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function turnItemText(item: OrchestrationV2TurnItem): string | null {
  switch (item.type) {
    case "notification":
      return [item.summary, item.detail].filter((part) => part !== undefined).join("\n");
    case "user_message":
    case "assistant_message":
    case "reasoning":
      return item.text;
    case "proposed_plan":
      return item.markdown;
    case "todo_list":
      return [item.explanation, ...item.steps.map((step) => `[${step.status}] ${step.text}`)]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "user_input_request":
      return jsonText(item.questions);
    case "file_change":
      return [
        item.fileName,
        item.additions === undefined && item.deletions === undefined
          ? undefined
          : `+${item.additions ?? 0} -${item.deletions ?? 0}`,
        item.diffStr ?? item.newStr,
      ]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "command_execution":
      return [`$ ${item.input}`, item.output]
        .filter((line): line is string => line !== undefined)
        .join("\n");
    case "file_search":
      return jsonText({ pattern: item.pattern, results: item.results });
    case "web_search":
      return jsonText({ patterns: item.patterns, results: item.results });
    case "approval_request":
      return item.prompt ?? item.requestKind;
    case "checkpoint":
      return jsonText(item.files);
    case "run_interrupt_request":
    case "run_interrupt_result":
    case "system_notice":
      return item.message;
    case "error":
      return item.failure.message;
    case "compaction":
      return item.summary ?? null;
    case "handoff":
      return item.summary ?? `${item.strategy} handoff to ${item.toProviderInstanceId}`;
    case "fork":
      return `Forked to thread ${item.targetThreadId}.`;
    case "thread_created":
      return `Created thread ${item.targetThreadId} with ${item.targetProviderInstanceId} (${item.targetModel}).`;
    case "secret_request":
      return `Asked the user for ${item.label}: ${item.secretStatus}.`;
    case "subagent":
      return item.result ?? item.progress ?? item.prompt;
    case "dynamic_tool":
      return jsonText({ toolName: item.toolName, input: item.input, output: item.output });
  }
}

function timelineItem(input: {
  readonly row: OrchestrationV2ThreadProjection["visibleTurnItems"][number];
  readonly maxChars: number;
  readonly textOffset?: number;
  readonly messagesByThreadId: ReadonlyMap<ThreadId, OrchestrationV2ThreadProjection["messages"]>;
}): OrchestratorMcpThreadTimelineItem {
  const text = turnItemText(input.row.item);
  const offset = input.textOffset ?? 0;
  const end = offset + input.maxChars;
  const textTruncated = text !== null && text.length > end;
  const messageId =
    input.row.item.type === "user_message" || input.row.item.type === "assistant_message"
      ? input.row.item.messageId
      : null;
  const message =
    messageId === null
      ? undefined
      : input.messagesByThreadId
          .get(input.row.sourceThreadId)
          ?.find((candidate) => candidate.id === messageId);
  return {
    position: input.row.position,
    visibility: input.row.visibility,
    sourceThreadId: input.row.sourceThreadId,
    itemId: input.row.sourceItemId,
    ...(input.row.item.type === "subagent" ? { subagentId: input.row.item.subagentId } : {}),
    runId: input.row.item.runId,
    messageId,
    createdBy: message?.createdBy ?? null,
    creationSource: message?.creationSource ?? null,
    type: input.row.item.type,
    status: input.row.item.status,
    title: input.row.item.title,
    text: text === null ? null : text.slice(offset, end),
    textTruncated,
    nextTextOffset: textTruncated ? end : null,
    updatedAt: DateTime.formatIso(input.row.item.updatedAt),
  };
}

const make = Effect.gen(function* () {
  const threadMcpConfig = yield* Effect.serviceOption(ThreadMcpConfig.ThreadMcpConfig);
  const crypto = yield* Crypto.Crypto;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const peerAuthority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const attachmentStaging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
  const requestReceipts = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);
  const bareLaunch = yield* BareLaunch.BareLaunch;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const providerAdapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const scheduledTasks = yield* ScheduledTaskService.ScheduledTaskService;
  // Fork (#174): outcome checks and command tasks; refused where the service is absent.
  const forkTasks = ScheduledTaskChecks.orRefuse(
    yield* Effect.serviceOption(ScheduledTaskChecks.ScheduledTaskChecks),
  );
  const projects = yield* ProjectService.ProjectService;
  const threadLaunch = yield* ThreadLaunchService.ThreadLaunchService;
  const gitDriver = yield* GitVcsDriver.GitVcsDriver;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const fileSystem = yield* FileSystem.FileSystem;
  const remoteDelegation = yield* RemoteDelegation.RemoteDelegation;

  const bareTarget = Effect.fnUntraced(function* (selection: ModelSelection) {
    const adapter = yield* providerAdapters
      .get(selection.instanceId)
      .pipe(Effect.mapError(() => failure("provider_unavailable", "Bare adapter unavailable.")));
    const support =
      adapter.bareSupport === undefined
        ? BareRuntime.unsupportedBare(adapter.driver)
        : yield* adapter.bareSupport;
    if (!support.supported) return yield* failure("provider_unavailable", support.reason);
    return selection;
  });

  /** A caller-named project, which must exist before anything is recorded against it. */
  const requireProject = (projectId: ProjectId) =>
    projects.getById(projectId).pipe(
      Effect.mapError((error) =>
        failure("orchestration_error", `Unable to read project ${projectId}: ${error.message}`),
      ),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(failure("invalid_request", `Project ${projectId} was not found.`)),
          onSome: Effect.succeed,
        }),
      ),
    );

  const scheduledBareGrant = Effect.fn("OrchestratorMcp.scheduledBareGrant")(function* (
    parent: Pick<OrchestrationV2ThreadProjection, "thread"> | undefined,
    mode: RuntimeMode,
  ): Effect.fn.Return<BareReadGrant, OrchestratorMcpFailure> {
    if (mode === "full-access") return { type: "all" };
    if (parent === undefined) return { type: "roots", roots: [] };
    return {
      type: "roots",
      roots: [
        parent.thread.worktreePath ??
          (yield* requireProject(parent.thread.projectId)).workspaceRoot,
      ],
    };
  });

  /** A client has no thread to inherit a model from, so it falls back to the project default. */
  const projectDefaultModelSelection = (
    project: Effect.Success<ReturnType<typeof requireProject>>,
  ) =>
    project.defaultModelSelection === null
      ? Effect.fail(
          failure(
            "invalid_request",
            `Project ${project.id} has no default model, so this caller cannot pick one for it.`,
          ),
        )
      : Effect.succeed(project.defaultModelSelection);
  const secretRequests = yield* SecretRequests.SecretRequests;

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("orchestration")
      ? Effect.void
      : Effect.fail(
          failure(
            "capability_denied",
            "This MCP credential does not grant orchestration capabilities.",
          ),
        );

  const loadProjection = (threadId: ThreadId) =>
    threadManagement
      .getThreadRecords(
        threadId,
        [
          "runs",
          "messages",
          "subagents",
          "providerThreads",
          "providerTurns",
          "runtimeRequests",
          "contextTransfers",
          "turnItems",
        ],
        { turnItemTypes: [], messageRoles: ["user"] },
      )
      .pipe(
        Effect.mapError((error) =>
          failure(
            "orchestration_error",
            `Unable to read thread ${threadId}: ${errorMessage(error)}`,
          ),
        ),
      );

  const loadProjectThread = (
    projectId: OrchestrationV2ThreadProjection["thread"]["projectId"],
    threadId: ThreadId,
  ) =>
    threadManagement
      .getProjectThreadRecords(
        { projectId, threadId },
        [
          "runs",
          "messages",
          "subagents",
          "providerThreads",
          "providerTurns",
          "runtimeRequests",
          "contextTransfers",
          "turnItems",
        ],
        { turnItemTypes: [], messageRoles: ["user"] },
      )
      .pipe(Effect.mapError(threadManagementFailure));

  const loadCaller = (scope: McpInvocationScope) =>
    Effect.gen(function* () {
      yield* requireCapability(scope);
      if (scope.thread === undefined) {
        return {
          parent: undefined,
          limits: intersectModes(
            clientModeCeiling(scope.client),
            scope.authority?.ceiling ?? scope.legacyPeerCeiling,
          ),
        } as const;
      }
      const parent = yield* loadProjection(scope.thread.threadId);
      if (scope.client !== undefined) {
        const ceiling = clientModeCeiling(scope.client);
        yield* resolveRuntimeMode(ceiling.runtimeMode, parent.thread.runtimeMode);
        yield* resolveInteractionMode(ceiling.interactionMode, parent.thread.interactionMode);
      }
      return {
        parent,
        limits: intersectModes(parent.thread, scope.authority?.ceiling ?? scope.legacyPeerCeiling),
      } as const;
    });

  const loadThreadCaller = (scope: McpInvocationScope, operation: string) =>
    Effect.gen(function* () {
      yield* requireCapability(scope);
      const threadScope = yield* requireThreadScope(scope, operation);
      const parent = yield* loadProjection(threadScope.thread.threadId);
      if (scope.client !== undefined) {
        const ceiling = clientModeCeiling(scope.client);
        yield* resolveRuntimeMode(ceiling.runtimeMode, parent.thread.runtimeMode);
        yield* resolveInteractionMode(ceiling.interactionMode, parent.thread.interactionMode);
      }
      const ceiling = scope.authority?.ceiling ?? scope.legacyPeerCeiling;
      const bounded =
        ceiling === undefined
          ? parent
          : { ...parent, thread: { ...parent.thread, ...intersectModes(parent.thread, ceiling) } };
      return { scope: threadScope, parent: bounded } as const;
    });

  const resolveProjectTarget = (
    parent: Pick<OrchestrationV2ThreadProjection, "thread"> | undefined,
    projectId: ProjectId | undefined,
  ) =>
    projectId !== undefined
      ? Effect.succeed(projectId)
      : parent !== undefined
        ? Effect.succeed(parent.thread.projectId)
        : Effect.fail(
            failure(
              "target_required",
              "Pass projectId: this MCP client is not running inside a T3 thread.",
            ),
          );

  const loadTargetThread = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const shell = yield* threadManagement
        .getThreadShell(threadId)
        .pipe(Effect.mapError(threadManagementFailure));
      if (shell === null || shell.deletedAt !== null) {
        return yield* failure("thread_not_found", `Thread ${threadId} was not found.`);
      }
      return yield* loadProjectThread(shell.projectId, threadId);
    });

  const loadScopedThread = (scope: McpInvocationScope, threadId: ThreadId) =>
    Effect.gen(function* () {
      const caller = yield* loadCaller(scope);
      const target =
        caller.parent !== undefined && threadId === caller.parent.thread.id
          ? caller.parent
          : yield* loadTargetThread(threadId);
      return { ...caller, target } as const;
    });

  /**
   * A thread caller writes to another thread only while its own run is live,
   * so a credential that outlived its session cannot reach across threads.
   */
  const assertLiveCallerForOtherThread = (
    scope: McpInvocationScope,
    parent: Pick<OrchestrationV2ThreadProjection, "thread" | "runs"> | undefined,
    target: Pick<OrchestrationV2ThreadProjection, "thread">,
  ) =>
    parent === undefined || target.thread.id === parent.thread.id
      ? Effect.void
      : assertLiveCaller(scope, parent);

  const assertLiveCallerForOtherProject = (
    scope: McpInvocationScope,
    parent: Pick<OrchestrationV2ThreadProjection, "thread" | "runs"> | undefined,
    projectId: ProjectId,
  ) =>
    parent === undefined || projectId === parent.thread.projectId
      ? Effect.void
      : assertLiveCaller(scope, parent);

  const assertLiveCaller = (
    scope: McpInvocationScope,
    parent: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
  ) => {
    const notActive = failure(
      "parent_not_active",
      "The calling provider no longer owns an active thread run.",
    );
    const activeRun = ThreadManagementService.latestActiveRun(parent);
    if (parent.thread.archivedAt === null && activeRun === undefined) {
      // Decide from this one fresh snapshot: a run may have started since `parent` was read.
      return threadManagement.getThreadShell(parent.thread.id).pipe(
        Effect.mapError(threadManagementFailure),
        Effect.flatMap((shell) =>
          isLiveCallerShell(shell, scope.thread?.providerInstanceId)
            ? Effect.void
            : Effect.fail(notActive),
        ),
      );
    }
    return parent.thread.archivedAt !== null ||
      activeRun === undefined ||
      activeRun.providerInstanceId !== scope.thread?.providerInstanceId
      ? Effect.fail(notActive)
      : Effect.void;
  };

  const loadReadableThread = (scope: McpInvocationScope, threadId: ThreadId) =>
    Effect.gen(function* () {
      const { parent } = yield* loadCaller(scope);
      const shell = yield* threadManagement
        .getThreadShell(threadId)
        .pipe(Effect.mapError(threadManagementFailure));
      if (shell === null || shell.deletedAt !== null) {
        return yield* failure("thread_not_found", `Thread ${threadId} is no longer available.`);
      }
      const target = yield* threadManagement
        .getProjectThreadRecords({ projectId: shell.projectId, threadId }, [
          "runs",
          "runtimeRequests",
          "contextTransfers",
        ])
        .pipe(Effect.mapError(threadManagementFailure));
      return { parent, target, shell } as const;
    });

  const prism = yield* Prism.PrismService;
  const loadProviders = providerRegistry.getProviders;

  const loadOrchestrationCapableInstanceIds = () =>
    providerAdapters.list().pipe(Effect.map((instanceIds) => new Set(instanceIds)));

  /**
   * Provider snapshots only re-probe while a client is in the foreground, so an
   * unattended agent can see a provider as unavailable after it was fixed.
   * Re-probe the requested instance once before refusing it.
   */
  const assertTargetEnabledForProject = (
    parent: Pick<OrchestrationV2ThreadProjection, "thread">,
    commandId: CommandId,
    target: ResolvedTarget,
  ) =>
    threadManagement
      .assertProviderInstanceEnabled({
        projectId: parent.thread.projectId,
        commandId,
        instanceId: target.modelSelection.instanceId,
      })
      .pipe(Effect.mapError(threadManagementFailure));

  const resolveTargetRechecking = (input: Parameters<typeof resolveTarget>[0]) => {
    const instanceId =
      input.target?.providerInstanceId ??
      (input.target?.driverKind === undefined
        ? input.parent.thread.modelSelection.instanceId
        : undefined);
    const resolved = resolveTarget(input);
    if (instanceId === undefined) return resolved;
    return resolved.pipe(
      Effect.catchIf(
        (error) => error.code === "provider_unavailable",
        () =>
          providerRegistry
            .refreshInstance(instanceId)
            .pipe(Effect.flatMap((providers) => resolveTarget({ ...input, providers }))),
      ),
    );
  };

  const resolveTarget = (input: {
    readonly parent: Pick<OrchestrationV2ThreadProjection, "thread">;
    readonly projectId?: ProjectId | undefined;
    readonly target: OrchestratorMcpTarget | undefined;
    readonly providers: ReadonlyArray<ServerProvider>;
    /**
     * Set only when the command that will dispatch this target already has a receipt: that
     * dispatch can only replay the recorded result, never start anything new, so filtering
     * candidates by the project's current settings would just refuse a retry that does no new
     * work. Never set this for a resolution whose dispatch uses a different command id than the
     * one checked for a receipt.
     */
    readonly skipProjectEnablementFilter?: boolean;
  }): Effect.Effect<ResolvedTarget, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const requestedInstanceId = input.target?.providerInstanceId;
      const requestedDriver = input.target?.driverKind;
      const orchestrationCapableInstanceIds = yield* loadOrchestrationCapableInstanceIds();
      let instanceId = requestedInstanceId;

      if (instanceId === undefined && requestedDriver !== undefined) {
        const candidates = input.providers.filter(
          (provider) =>
            provider.driver === requestedDriver &&
            orchestrationCapableInstanceIds.has(provider.instanceId),
        );
        if (candidates.length === 0) {
          return yield* failure(
            "provider_unavailable",
            `No V2 provider adapter is registered for driver ${requestedDriver}.`,
          );
        }
        const healthyCandidates = candidates.filter(
          (candidate) => providerConstraints(candidate, true).length === 0,
        );
        const inheritedCandidate = healthyCandidates.find(
          (candidate) => candidate.instanceId === input.parent.thread.modelSelection.instanceId,
        );
        const orderedCandidates = [
          ...(inheritedCandidate === undefined ? [] : [inheritedCandidate]),
          ...healthyCandidates.filter((candidate) => candidate !== inheritedCandidate),
        ];
        for (const candidate of orderedCandidates) {
          const enabledForProject =
            input.skipProjectEnablementFilter === true ||
            (yield* threadManagement.isProviderInstanceEnabledForProject({
              projectId: input.projectId ?? input.parent.thread.projectId,
              instanceId: candidate.instanceId,
            }));
          if (enabledForProject) {
            instanceId = candidate.instanceId;
            break;
          }
        }
        if (instanceId === undefined) {
          return yield* failure(
            "provider_unavailable",
            healthyCandidates.length === 0
              ? `No available V2 provider instance for driver ${requestedDriver}.`
              : `No instance of driver ${requestedDriver} is enabled for this project.`,
          );
        }
      }
      instanceId ??= input.parent.thread.modelSelection.instanceId;

      const provider = input.providers.find((candidate) => candidate.instanceId === instanceId);
      if (provider === undefined) {
        return yield* failure(
          "provider_unavailable",
          `Provider instance ${instanceId} is not registered.`,
        );
      }
      if (requestedDriver !== undefined && provider.driver !== requestedDriver) {
        return yield* failure(
          "invalid_request",
          `Provider instance ${instanceId} uses driver ${provider.driver}, not ${requestedDriver}.`,
        );
      }
      const constraints = providerConstraints(
        provider,
        orchestrationCapableInstanceIds.has(provider.instanceId),
      );
      if (constraints.length > 0) {
        return yield* failure(
          "provider_unavailable",
          `Provider ${instanceId} cannot run a child task: ${constraints.join(" ")}`,
        );
      }

      const inheritedSelection = input.parent.thread.modelSelection;
      const requestedModel = input.target?.model;
      const model =
        requestedModel ??
        (instanceId === inheritedSelection.instanceId
          ? inheritedSelection.model
          : provider?.models[0]?.slug);
      if (model === undefined) {
        return yield* failure(
          "model_unavailable",
          `Provider ${instanceId} has no model available for inheritance.`,
        );
      }
      if (
        requestedModel !== undefined &&
        provider !== undefined &&
        provider.models.length > 0 &&
        !provider.models.some((candidate) => candidate.slug === requestedModel)
      ) {
        return yield* failure(
          "model_unavailable",
          `Model ${requestedModel} is not advertised by provider ${instanceId}.`,
        );
      }

      const requestedOptions = input.target?.options;
      if (requestedOptions !== undefined) {
        const descriptors = provider.models.find((candidate) => candidate.slug === model)
          ?.capabilities?.optionDescriptors;
        const invalid = invalidOptionSelections(requestedOptions, descriptors);
        if (invalid.length > 0) {
          return yield* failure(
            "invalid_request",
            `Model ${model} on provider ${instanceId} rejected options: ${invalid.join(" ")}`,
          );
        }
      }

      return {
        modelSelection:
          instanceId === inheritedSelection.instanceId &&
          model === inheritedSelection.model &&
          requestedOptions === undefined
            ? inheritedSelection
            : requestedOptions === undefined
              ? { instanceId, model }
              : { instanceId, model, options: requestedOptions },
        supportedRuntimeModes: provider.supportedRuntimeModes,
      };
    });

  const requestKey = (clientRequestId: string | undefined): Effect.Effect<string> =>
    clientRequestId === undefined
      ? crypto.randomUUIDv4().pipe(Effect.orDie)
      : Effect.succeed(clientRequestId);

  const readTask = (
    scope: McpThreadInvocationScope,
    taskId: NodeId,
    waitTimedOut = false,
    acknowledgeTerminal = false,
    acknowledgementOperation = "task-status-acknowledge",
  ): Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      yield* requireCapability(scope);
      const parentProjection = yield* loadProjection(scope.thread.threadId);
      const task = parentProjection.subagents.find(
        (candidate) =>
          candidate.id === taskId &&
          candidate.origin === "app_owned" &&
          candidate.threadId === scope.thread.threadId,
      );
      if (task === undefined || (task.childThreadId === null && task.remoteChild === undefined)) {
        return yield* failure(
          "task_not_found",
          `Delegated task ${taskId} does not belong to thread ${scope.thread.threadId}.`,
        );
      }
      if (task.childThreadId === null) {
        return yield* readRemoteTask(scope, task, waitTimedOut, acknowledgeTerminal);
      }
      const childControls = yield* threadManagement
        .getThreadRecords(
          task.childThreadId,
          [
            "runs",
            "messages",
            "contextTransfers",
            "subagents",
            "providerThreads",
            "runtimeRequests",
          ],
          { messageRoles: ["user"] },
        )
        .pipe(Effect.mapError(threadManagementFailure));
      const childRun = delegatedTaskRun(childControls, task);
      const terminalRun = latestTerminalResultRun(childControls, childRun);
      const progress = delegatedTaskProgress(childControls);
      const isOriginalTask =
        childControls.thread.forkedFrom?.type === "node" &&
        childControls.thread.forkedFrom.nodeId === task.id;
      const taskResultRun =
        !isOriginalTask && childRun?.delegatedTaskId === task.id
          ? delegatedTaskRuns(childControls, task.id)
              .filter((run) => !(run.status === "queued" && run.queueHeld === true))
              .toSorted((a, b) => b.ordinal - a.ordinal)[0]
          : progress.resultRun;
      const taskWorkState =
        !isOriginalTask && childRun?.delegatedTaskId === task.id
          ? isTerminalTaskStatus(taskStatusForRun(taskResultRun))
            ? "result_available"
            : "working"
          : progress.state;
      const resultRunIds = restartResultRunIds(
        childControls.runs,
        [taskResultRun?.id, terminalRun?.id].filter((id): id is RunId => id !== undefined),
      );
      const resultRecords = yield* threadManagement
        .getThreadRecords(task.childThreadId, ["messages", "turnItems"], {
          messageRoles: ["assistant"],
          messageRunIds: resultRunIds,
          turnItemRunIds: resultRunIds,
          turnItemTypes: ["assistant_message", "error"],
        })
        .pipe(Effect.mapError(threadManagementFailure));
      const childProjection = {
        ...childControls,
        messages: [...childControls.messages, ...resultRecords.messages],
        turnItems: resultRecords.turnItems,
      };
      const resultForRun = (run: OrchestrationV2Run) =>
        threadManagement.readRestartCutView(run, childControls.runs).pipe(
          Effect.mapError(threadManagementFailure),
          Effect.map((cut) =>
            subagentResultForRun(childProjection, { ...run, status: runOutcome(run) }, cut),
          ),
        );
      // A restart cut the child's run and its continuation has not settled, or
      // the child started working again after this read.
      const heldForRestart =
        task.result === null &&
        taskWorkState === "result_available" &&
        (yield* threadManagement
          .delegatedTaskResultPending(task.childThreadId, childRun?.delegatedTaskId)
          .pipe(Effect.mapError(threadManagementFailure)));
      const workState =
        task.result !== null ? "result_available" : heldForRestart ? "working" : taskWorkState;
      const status =
        task.result !== null
          ? taskStatusForRun(
              task.status === "completed" ||
                task.status === "failed" ||
                task.status === "cancelled" ||
                task.status === "interrupted"
                ? { status: task.status }
                : childRun,
            )
          : workState === "result_available"
            ? taskStatusForRun(taskResultRun ?? childRun)
            : taskStatusForRun(taskResultRun ?? childRun) === "queued"
              ? "queued"
              : "running";
      const derivedResult =
        task.result !== null
          ? task.result
          : taskResultRun !== undefined && isTerminalTaskStatus(status)
            ? (yield* resultForRun(taskResultRun)).text
            : null;
      const resultTransfers = parentProjection.contextTransfers.filter(
        (transfer) =>
          transfer.type === "subagent_result" &&
          transfer.sourceThreadId === task.childThreadId &&
          transfer.targetThreadId === scope.thread.threadId,
      );
      const resultTransferForRun = (run: OrchestrationV2Run | undefined) =>
        !canExposeTaskRunResult(run)
          ? null
          : (resultTransfers.find((transfer) => transfer.sourcePoint.runId === run.id) ??
            (run.id === childRun?.id
              ? resultTransfers.find((transfer) => transfer.sourcePoint.runId === undefined)
              : undefined) ??
            null);
      const resultTransfer = taskResultTransfer(parentProjection, childProjection, task) ?? null;
      const terminalStatus = terminalRun === undefined ? null : taskStatusForRun(terminalRun);
      const workRuns = delegatedTaskRuns(childControls, task.id);
      const continuedSources = new Set(workRuns.map((run) => run.restartContinuationOfRunId));
      const pendingRestart =
        workRuns.some(
          (run) =>
            (run.status === "cancelled" || run.restartCancelledBackgroundWork !== undefined) &&
            !continuedSources.has(run.id) &&
            (childRun === undefined || run.ordinal > childRun.ordinal),
        ) &&
        (yield* threadManagement
          .delegatedTaskResultPending(task.childThreadId, task.id)
          .pipe(Effect.mapError(threadManagementFailure)));
      const terminalResult = canExposeTaskRunResult(terminalRun)
        ? yield* resultForRun(terminalRun)
        : undefined;
      const response = {
        taskId: task.id,
        childThreadId: task.childThreadId,
        childRunId: childRun?.id ?? null,
        purpose: executionPurposeOf(task),
        warnings: executionPurposeWarnings(executionPurposeOf(task)),
        ...(terminalRun === undefined
          ? {}
          : { latestTerminalPurpose: executionPurposeOf(terminalRun) }),
        childNodeId: task.id,
        status,
        workState,
        hasPendingChildRuns: hasPendingChildRuns(childProjection, childRun) || pendingRestart,
        providerInstanceId: task.providerInstanceId,
        model: task.model,
        summary: derivedResult,
        resultContextTransferId: resultTransfer?.id ?? null,
        latestTerminalRunId: terminalRun?.id ?? null,
        latestTerminalStatus:
          terminalStatus !== null && isTerminalTaskStatus(terminalStatus) ? terminalStatus : null,
        latestTerminalSummary:
          terminalResult !== undefined
            ? terminalRun?.id === childRun?.id && terminalResult.restartNote === null
              ? derivedResult
              : terminalResult.text
            : null,
        latestTerminalResultContextTransferId: resultTransferForRun(terminalRun)?.id ?? null,
        hasPendingApproval:
          !isTerminalTaskStatus(status) &&
          childControls.runtimeRequests.some(
            (request) =>
              request.status === "pending" &&
              request.responseCapability.type === "live" &&
              request.kind !== "user_input" &&
              request.kind !== "auth_refresh" &&
              request.kind !== "dynamic_tool_call",
          ),
        // A finished task has no question of its own; later child runs belong to other tasks.
        hasPendingQuestion:
          !isTerminalTaskStatus(status) &&
          childControls.runtimeRequests.some(
            (request) =>
              request.kind === "user_input" &&
              request.status === "pending" &&
              request.responseCapability.type === "live",
          ),
        waitTimedOut,
      } satisfies OrchestratorMcpDelegateTaskResult;
      if (
        acknowledgeTerminal &&
        isTerminalTaskStatus(status) &&
        task.completionDelivery?.state !== "acknowledged" &&
        task.completionDelivery?.state !== "disposed"
      ) {
        const observingRun = ThreadManagementService.latestActiveRun(parentProjection);
        const acknowledgementRequestKey = yield* requestKey(undefined);
        yield* threadManagement
          .dispatch({
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: stableCommandId({
              scope,
              requestKey: acknowledgementRequestKey,
              operation: acknowledgementOperation,
            }),
            parentThreadId: scope.thread.threadId,
            taskId,
            observedByRunId:
              observingRun?.providerInstanceId === scope.thread.providerInstanceId
                ? observingRun.id
                : null,
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "orchestration_error",
                `Unable to acknowledge delegated task ${taskId}: ${errorMessage(error)}`,
              ),
            ),
          );
      }
      return response;
    });

  /** `mode=wait` on a task already recorded, as `delegateTask` does for its own. */
  const awaitTask = (
    scope: McpThreadInvocationScope,
    taskId: NodeId,
    requestedTimeoutMs: number | undefined,
  ): Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const timeoutMs = Math.min(
        MAX_WAIT_TIMEOUT_MS,
        Math.max(1, requestedTimeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
      );
      const waited = yield* waitForTask(scope, taskId, timeoutMs);
      if (Option.isSome(waited)) return waited.value;
      // As for a local task: once the blocking wait ends, a later result wakes the parent.
      yield* threadManagement
        .dispatch({
          type: "delegated_task.wake-policy",
          commandId: stableCommandId({
            scope,
            requestKey: taskId,
            operation: "delegate-task-wake-policy",
          }),
          parentThreadId: scope.thread.threadId,
          taskId,
          completionWake: "always",
        })
        .pipe(Effect.ignoreCause({ log: true }));
      return yield* readTask(scope, taskId, true, true);
    });

  /**
   * A task that runs in a linked environment, answered from its row here:
   * the follower records its result once its thread there ends.
   */
  const readRemoteTask = (
    scope: McpThreadInvocationScope,
    task: OrchestrationV2Subagent,
    waitTimedOut: boolean,
    acknowledgeTerminal: boolean,
  ): Effect.Effect<OrchestratorMcpDelegateTaskResult, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const status: OrchestratorMcpDelegateTaskResult["status"] =
        task.result === null
          ? "running"
          : task.status === "completed" ||
              task.status === "failed" ||
              task.status === "cancelled" ||
              task.status === "interrupted"
            ? task.status
            : "completed";
      const terminal = isTerminalTaskStatus(status);
      if (
        acknowledgeTerminal &&
        terminal &&
        task.completionDelivery?.state !== "acknowledged" &&
        task.completionDelivery?.state !== "disposed"
      ) {
        const parentProjection = yield* loadProjection(scope.thread.threadId);
        const observingRun = ThreadManagementService.latestActiveRun(parentProjection);
        yield* threadManagement
          .dispatch({
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: stableCommandId({
              scope,
              requestKey: yield* requestKey(undefined),
              operation: "task-status-acknowledge",
            }),
            parentThreadId: scope.thread.threadId,
            taskId: task.id,
            observedByRunId:
              observingRun?.providerInstanceId === scope.thread.providerInstanceId
                ? observingRun.id
                : null,
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "orchestration_error",
                `Unable to acknowledge delegated task ${task.id}: ${errorMessage(error)}`,
              ),
            ),
          );
      }
      return {
        taskId: task.id,
        childThreadId: null,
        ...(task.remoteChild === undefined
          ? {}
          : {
              remoteChild: {
                environmentId: task.remoteChild.environmentId,
                label: task.remoteChild.label,
                ...(task.remoteChild.launchError === undefined
                  ? {}
                  : { launchError: task.remoteChild.launchError }),
                ...(task.remoteChild.threadId === undefined
                  ? {}
                  : { threadId: task.remoteChild.threadId }),
              },
            }),
        remoteObservation: {
          availability: task.remoteChild?.availability ?? "stale",
          ...(task.remoteChild?.snapshot === undefined
            ? {}
            : { snapshot: task.remoteChild.snapshot }),
          ...(task.remoteChild?.originalResult === undefined
            ? {}
            : { originalRunId: task.remoteChild.originalResult.runId }),
          ...(task.remoteChild?.latestResult === undefined
            ? {}
            : { latestResult: task.remoteChild.latestResult }),
          ...(task.remoteChild?.stop === undefined ? {} : { stop: task.remoteChild.stop }),
        },
        childRunId: null,
        purpose: executionPurposeOf(task),
        warnings: executionPurposeWarnings(executionPurposeOf(task)),
        ...(task.completionSource === undefined
          ? {}
          : { latestTerminalPurpose: task.completionSource.purpose }),
        childNodeId: task.id,
        status,
        workState:
          task.remoteChild?.snapshot?.workState ?? (terminal ? "result_available" : "working"),
        hasPendingChildRuns:
          (task.remoteChild?.snapshot?.pendingChildren ?? 0) > 0 ||
          (task.remoteChild?.snapshot?.queuedRuns ?? 0) > 0 ||
          task.remoteChild?.snapshot?.recoveryWaiting === true,
        hasPendingApproval:
          task.remoteChild?.availability === "current" &&
          (task.remoteChild.snapshot?.requests.some(
            (request) =>
              request.responseType === "live" &&
              request.kind !== "user_input" &&
              request.kind !== "auth_refresh" &&
              request.kind !== "dynamic_tool_call",
          ) ??
            false),
        // Question counts come from qualified destination observations.
        hasPendingQuestion: (task.remoteChild?.snapshot?.pendingQuestions ?? 0) > 0,
        providerInstanceId: task.providerInstanceId,
        model: task.model,
        summary: task.result,
        resultContextTransferId: null,
        latestTerminalRunId: task.remoteChild?.latestResult?.runId ?? null,
        latestTerminalStatus: task.remoteChild?.latestResult?.status ?? null,
        latestTerminalSummary: task.remoteChild?.latestResult?.text ?? null,
        latestTerminalResultContextTransferId: null,
        waitTimedOut,
      } satisfies OrchestratorMcpDelegateTaskResult;
    });

  // Re-read the task only when an event on the parent or child thread can
  // change its status, instead of polling the projections every 50 ms.
  const waitForTask = (scope: McpThreadInvocationScope, taskId: NodeId, timeoutMs: number) =>
    Effect.gen(function* () {
      const streamError = (error: unknown) =>
        failure(
          "orchestration_error",
          `Unable to watch delegated task ${taskId}: ${errorMessage(error)}`,
        );
      // Sequences are global, so one cursor taken before the first read
      // replays anything either thread records after it.
      const afterSequence = yield* threadManagement
        .getThreadEventSequence(scope.thread.threadId)
        .pipe(Effect.mapError(streamError));
      const initial = yield* readTask(scope, taskId, false, true);
      if (
        initial.hasPendingQuestion ||
        initial.hasPendingApproval ||
        isTerminalTaskStatus(initial.status)
      ) {
        return Option.some(initial);
      }
      // One stream per event type, so transcript events never fill a buffer.
      // A remote task has no child here; its completion lands on the parent.
      const childThreadId = initial.childThreadId;
      const wakeEvents =
        childThreadId === null
          ? TASK_WAKE_EVENTS.filter(({ thread }) => thread === "parent")
          : TASK_WAKE_EVENTS;
      return yield* Stream.mergeAll(
        wakeEvents.map(({ thread, eventType }) =>
          threadManagement.streamStoredEventsFrom({
            threadId:
              thread === "parent" || childThreadId === null ? scope.thread.threadId : childThreadId,
            afterSequence,
            eventType,
          }),
        ),
        { concurrency: "unbounded" },
      ).pipe(
        Stream.mapError(streamError),
        Stream.mapEffect(() => readTask(scope, taskId, false, true)),
        Stream.filter(
          (result) =>
            result.hasPendingQuestion ||
            result.hasPendingApproval === true ||
            isTerminalTaskStatus(result.status),
        ),
        Stream.runHead,
      );
    }).pipe(Effect.timeoutOption(Duration.millis(timeoutMs)), Effect.map(Option.flatten));

  /** A Run group's members on this thread, in Color order; empty when `groupId` names none. */
  const loadRunGroup = (scope: McpThreadInvocationScope, groupId: NodeId) =>
    loadProjection(scope.thread.threadId).pipe(
      Effect.map((parent) =>
        runGroupMembers(parent.subagents, groupId).filter(
          (task) => task.threadId === scope.thread.threadId,
        ),
      ),
    );

  const readRunGroup = (
    scope: McpThreadInvocationScope,
    groupId: NodeId,
    members: ReadonlyArray<OrchestrationV2Subagent>,
    waitTimedOut = false,
  ): Effect.Effect<OrchestratorMcpRunGroupResult, OrchestratorMcpFailure> =>
    Effect.forEach(members, (member) => readTask(scope, member.id, false, true)).pipe(
      Effect.map((tasks) => ({
        groupId,
        status: tasks.every((task) => isTerminalTaskStatus(task.status))
          ? ("completed" as const)
          : ("running" as const),
        tasks,
        waitTimedOut,
      })),
    );

  /**
   * `mode=wait` on a Run group: until every member is terminal, one asks a
   * blocking question, or the budget runs out. A wait that ends early leaves
   * the group to wake the parent once it finishes, as for one task.
   */
  const awaitRunGroup = (
    scope: McpThreadInvocationScope,
    groupId: NodeId,
    members: ReadonlyArray<OrchestrationV2Subagent>,
    requestedTimeoutMs: number | undefined,
  ) =>
    Effect.gen(function* () {
      const timeoutMs = Math.min(
        MAX_WAIT_TIMEOUT_MS,
        Math.max(1, requestedTimeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
      );
      const deadline = (yield* Clock.currentTimeMillis) + timeoutMs;
      for (const member of members) {
        const remaining = deadline - (yield* Clock.currentTimeMillis);
        const waited =
          remaining <= 0 ? Option.none() : yield* waitForTask(scope, member.id, remaining);
        if (Option.isSome(waited) && isTerminalTaskStatus(waited.value.status)) continue;
        for (const pending of members)
          yield* threadManagement
            .dispatch({
              type: "delegated_task.wake-policy",
              commandId: stableCommandId({
                scope,
                requestKey: pending.id,
                operation: "delegate-task-wake-policy",
              }),
              parentThreadId: scope.thread.threadId,
              taskId: pending.id,
              completionWake: "always",
            })
            .pipe(Effect.ignoreCause({ log: true }));
        return yield* readRunGroup(scope, groupId, members, Option.isNone(waited));
      }
      return yield* readRunGroup(scope, groupId, members);
    });

  const scheduledTaskRunModes = (task: ScheduledTask) =>
    Effect.gen(function* () {
      const modes = [{ runtimeMode: task.runtimeMode, interactionMode: task.interactionMode }];
      if (task.threadId === null) return modes;
      const bound = yield* threadManagement
        .getThreadShell(task.threadId)
        .pipe(Effect.mapError(threadManagementFailure));
      return bound === null || bound.deletedAt !== null ? modes : [...modes, bound];
    });

  const withinLimits = (
    limits: {
      readonly runtimeMode: RuntimeMode;
      readonly interactionMode: ProviderInteractionMode;
    },
    modes: { readonly runtimeMode: RuntimeMode; readonly interactionMode: ProviderInteractionMode },
  ) =>
    runtimeModeRank(modes.runtimeMode) <= runtimeModeRank(limits.runtimeMode) &&
    interactionModeRank(modes.interactionMode) <= interactionModeRank(limits.interactionMode);

  /**
   * Its webhook URL starts runs, so only a caller that may start one sees it: never a client
   * approved for read-only access, and a thread caller only with a live turn, at modes covering
   * every mode the task runs at.
   */
  const summarizeScheduledTask = (
    scope: McpInvocationScope,
    caller: {
      readonly parent: Pick<OrchestrationV2ThreadProjection, "thread" | "runs"> | undefined;
      readonly limits: {
        readonly runtimeMode: RuntimeMode;
        readonly interactionMode: ProviderInteractionMode;
      };
    },
    task: ScheduledTask,
  ) =>
    Effect.gen(function* () {
      const live =
        caller.parent === undefined
          ? scope.client?.access !== "read-only"
          : Exit.isSuccess(yield* Effect.exit(assertLiveCaller(scope, caller.parent)));
      const owns = Exit.isSuccess(
        yield* Effect.exit(assertSameLink({ scope, caller: caller.parent?.thread }, task)),
      );
      // This runs after a save, so a failed lookup of the bound thread hides
      // the webhook URL instead of reporting a saved task as an error.
      const modes = yield* scheduledTaskRunModes(task).pipe(Effect.option);
      return scheduledTaskSummary(
        task,
        live &&
          owns &&
          Option.isSome(modes) &&
          modes.value.every((mode) => withinLimits(caller.limits, mode)),
      );
    });

  /**
   * A scheduled task the caller may change: one whose runs execute at modes no
   * broader than the caller's own, so editing its prompt cannot run work above
   * the caller's limits.
   */
  const loadScheduledTask = (
    scheduledTaskId: ScheduledTask["id"],
    limits: {
      readonly runtimeMode: RuntimeMode;
      readonly interactionMode: ProviderInteractionMode;
    },
  ): Effect.Effect<ScheduledTask, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const { tasks } = yield* scheduledTasks
        .list()
        .pipe(
          Effect.mapError((error) =>
            failure("orchestration_error", `Could not load scheduled task: ${error.message}`),
          ),
        );
      const task = tasks.find((candidate) => candidate.id === scheduledTaskId);
      if (task === undefined) {
        return yield* failure("task_not_found", `Scheduled task ${scheduledTaskId} was not found.`);
      }
      for (const modes of yield* scheduledTaskRunModes(task)) {
        yield* resolveRuntimeMode(limits.runtimeMode, modes.runtimeMode);
        yield* resolveInteractionMode(limits.interactionMode, modes.interactionMode);
      }
      return task;
    });

  type RunGroupMethods = "delegateTaskOrRunGroup" | "taskOrRunGroupStatus" | "cancelTaskOrRunGroup";

  /**
   * Run groups (#402): a read-only role whose Run is above 1 starts that many
   * of its Colors on one task. Everything else is the single-task method.
   */
  const runGroupMethods = (
    single: Omit<OrchestratorMcpServiceShape, RunGroupMethods>,
  ): Pick<OrchestratorMcpServiceShape, RunGroupMethods> => ({
    delegateTaskOrRunGroup: (callerScope, input) =>
      Effect.gen(function* () {
        const role =
          input.prismRole === undefined
            ? Prism.delegatedPrismRole(input.role)
            : admitDelegateRole(input.prismRole, true);
        const target = input.target;
        // A named provider or model is one Color; so is a bare launch.
        const explicit =
          target !== undefined &&
          (target.model !== undefined ||
            target.providerInstanceId !== undefined ||
            target.driverKind !== undefined);
        if (role === undefined || explicit || input.bare !== undefined || input.tools !== undefined)
          return yield* single.delegateTask(callerScope, input);
        const scope = yield* requireThreadScope(callerScope, "delegate_task");
        const caller = yield* loadProjection(scope.thread.threadId);
        const key = yield* requestKey(input.clientRequestId);
        const commandId = stableCommandId({
          scope,
          requestKey: key,
          operation: "delegate-task-group",
        });
        const groupId = NodeId.make(`run-group:${commandId}`);
        // A retry returns the group its first call committed, never a fuller one.
        const committed = yield* loadRunGroup(scope, groupId);
        if (committed.length > 0) {
          if (
            committed.some(
              (member) => !purposeIntentsEqual(member.purposeIntent, input.purposeIntent),
            )
          )
            return yield* failure(
              "orchestration_error",
              new PurposeAdmissionError({ reason: "intent_reused" }).message,
            );
          return yield* input.mode === "wait"
            ? awaitRunGroup(scope, groupId, committed, input.timeoutMs)
            : readRunGroup(scope, groupId, committed);
        }
        // A retry of a request accepted as one task stays one task.
        const acceptedAsTask = yield* threadManagement.hasReceipt(
          stableCommandId({ scope, requestKey: key, operation: "delegate-task" }),
        );
        const run = acceptedAsTask
          ? 1
          : yield* prism.run({ projectId: caller.thread.projectId, role });
        if (run === 1) return yield* single.delegateTask(callerScope, input);
        if (
          target !== undefined &&
          PeerForwarding.remoteTarget(callerScope, target.environmentId) !== undefined
        )
          return yield* failure(
            "invalid_request",
            `Prism role ${role} runs ${run} Colors, and a Run group cannot run in a linked environment. Name target.model to run one Color there.`,
          );
        if (
          input.isolation === "worktree" ||
          input.projectId !== undefined ||
          input.workspaceStrategy !== undefined
        )
          return yield* failure(
            "invalid_request",
            `Prism role ${role} runs ${run} Colors that share this thread's checkout. Omit projectId, workspaceStrategy and isolation, or name target.model to run one Color.`,
          );
        const { parent } = yield* loadThreadCaller(callerScope, "delegate_task");
        const parentRun = ThreadManagementService.latestActiveRun(parent);
        if (
          parentRun === undefined ||
          parentRun.rootNodeId === null ||
          parentRun.providerInstanceId !== scope.thread.providerInstanceId
        )
          return yield* failure(
            "parent_not_active",
            "Delegated tasks require an active run owned by this MCP provider session.",
          );
        const providers = yield* loadProviders;
        const targets: Array<Effect.Success<ReturnType<typeof resolveTargetRechecking>>> = [];
        // Prism walks the role's Colors in order; one that cannot start yields to the next.
        const colors = yield* prism.resolve({
          projectId: parent.thread.projectId,
          role,
          lane: input.lane,
          inherited: parent.thread.modelSelection,
          count: run,
          validate: (selection) =>
            resolveTargetRechecking({
              parent,
              projectId: undefined,
              providers,
              target: {
                providerInstanceId: selection.instanceId,
                model: selection.model,
                options: selection.options,
              },
              skipProjectEnablementFilter: false,
            }).pipe(
              Effect.map((resolved) => {
                targets.push(resolved);
                return resolved.modelSelection;
              }),
            ),
        });
        const [first, ...rest] = yield* Effect.forEach(
          colors.modelSelections,
          (modelSelection, index) =>
            resolveRuntimeMode(parent.thread.runtimeMode, input.runtimeMode, targets[index]).pipe(
              Effect.map((runtimeMode) => ({ modelSelection, runtimeMode })),
            ),
        );
        yield* threadManagement
          .dispatch({
            type: "delegated_task.group.request",
            ...(input.purposeIntent === undefined ? {} : { purposeIntent: input.purposeIntent }),
            createdBy: "agent",
            creationSource: "mcp",
            commandId,
            parentThreadId: scope.thread.threadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId,
            groupId,
            task: [colors.kitText, taskPrompt(input)].filter(Boolean).join("\n\n"),
            ...(input.title === undefined ? {} : { title: input.title }),
            members: [first!, ...rest],
            interactionMode: yield* resolveInteractionMode(
              parent.thread.interactionMode,
              input.interactionMode,
            ),
            // As for one task: a wait delivers through this call unless it ends first.
            completionWake: input.mode === "wait" ? "settled_only" : "always",
          })
          .pipe(
            Effect.mapError((error) =>
              error._tag === "ThreadManagementProviderInstanceDisabledError"
                ? threadManagementFailure(error)
                : failure(
                    "orchestration_error",
                    `Unable to create Run group: ${errorMessage(error)}`,
                  ),
            ),
          );
        const members = yield* loadRunGroup(scope, groupId);
        return yield* input.mode === "wait"
          ? awaitRunGroup(scope, groupId, members, input.timeoutMs)
          : readRunGroup(scope, groupId, members);
      }),
    taskOrRunGroupStatus: (callerScope, taskId) =>
      Effect.gen(function* () {
        const scope = yield* requireThreadScope(callerScope, "task_status");
        const members = yield* loadRunGroup(scope, taskId);
        return members.length === 0
          ? yield* single.taskStatus(callerScope, taskId)
          : yield* readRunGroup(scope, taskId, members);
      }),
    cancelTaskOrRunGroup: (callerScope, input) =>
      Effect.gen(function* () {
        const scope = yield* requireThreadScope(callerScope, "task_cancel");
        const members = yield* loadRunGroup(scope, input.taskId);
        if (members.length === 0) return yield* single.cancelTask(callerScope, input);
        const key = yield* requestKey(input.clientRequestId);
        // Cancelling the whole group gives up its wake. The group cancel is
        // recorded on every member before any is stopped, so a member ending in
        // any order stays quiet; every member is asked to stop before the first
        // refusal is reported, and a refused member keeps its wake.
        const groupCancelId = stableCommandId({
          scope,
          requestKey: key,
          operation: "cancel-run-group",
        });
        yield* threadManagement
          .dispatch({
            type: "delegated_task.group-cancel",
            commandId: groupCancelId,
            parentThreadId: scope.thread.threadId,
            groupId: input.taskId,
          })
          .pipe(
            Effect.mapError((error) =>
              failure(
                "orchestration_error",
                `Unable to cancel Run group ${input.taskId}: ${errorMessage(error)}`,
              ),
            ),
          );
        const outcomes = yield* Effect.forEach(members, (member, index) =>
          Effect.result(
            single
              .cancelTask(callerScope, {
                ...input,
                taskId: member.id,
                clientRequestId: `${key}:${index}`,
              })
              .pipe(
                Effect.tapError(() =>
                  threadManagement
                    .dispatch({
                      type: "delegated_task.group-cancel.release",
                      commandId: stableCommandId({
                        scope,
                        requestKey: `${key}:${index}`,
                        operation: "cancel-run-group-release",
                      }),
                      parentThreadId: scope.thread.threadId,
                      taskId: member.id,
                      groupCancelId,
                    })
                    .pipe(
                      Effect.mapError((error) =>
                        failure(
                          "orchestration_error",
                          `Unable to restore delegated task ${member.id} completion delivery: ${errorMessage(error)}`,
                        ),
                      ),
                    ),
                ),
                Effect.tap(() =>
                  threadManagement
                    .dispatch({
                      type: "delegated_task.completion-delivery.dispose",
                      commandId: stableCommandId({
                        scope,
                        requestKey: `${key}:${index}`,
                        operation: "cancel-run-group-delivery",
                      }),
                      parentThreadId: scope.thread.threadId,
                      taskId: member.id,
                    })
                    .pipe(
                      Effect.mapError((error) =>
                        failure(
                          "orchestration_error",
                          `Unable to dispose delegated task ${member.id} completion delivery: ${errorMessage(error)}`,
                        ),
                      ),
                    ),
                ),
              ),
          ),
        );
        const tasks: Array<OrchestratorMcpTaskCancelResult> = [];
        for (const outcome of outcomes) {
          if (Result.isFailure(outcome)) return yield* outcome.failure;
          tasks.push(outcome.success);
        }
        return { groupId: input.taskId, tasks };
      }),
  });

  const scheduleRequest = (scope: McpInvocationScope) =>
    Effect.gen(function* () {
      const identity = yield* RequestReceipts.CurrentRequest;
      if (
        identity === undefined ||
        (scope.client?.linked === true &&
          interactiveAuthority(scope.authority)?.envelope.requestOwner === undefined)
      )
        return undefined;
      if (scheduledTasks.mutationReceipt === undefined)
        return yield* failure(
          "capability_denied",
          "This environment does not support durable scheduler mutations.",
        );
      return identity;
    });
  const scheduleReceipt = (request: RequestReceipts.RequestIdentity) =>
    scheduledTasks.mutationReceipt === undefined
      ? Effect.fail(
          failure(
            "capability_denied",
            "This environment does not support durable scheduler mutations.",
          ),
        )
      : scheduledTasks.mutationReceipt(request);
  const scheduleReplay = (
    scope: McpInvocationScope,
    request: RequestReceipts.RequestIdentity,
    projectId?: ProjectId,
  ) =>
    Option.isSome(peerAuthority)
      ? peerAuthority.value.historicalScheduleReceipt(scope, request, projectId)
      : Effect.fail(
          failure(
            "capability_denied",
            "Historical scheduler authority is unavailable; automatic replay is unsupported.",
          ),
        );
  const scheduleRecheck = (scope: McpInvocationScope, taskId?: ScheduledTask["id"]) =>
    Effect.gen(function* () {
      const caller = yield* loadCaller(scope);
      if (Option.isSome(peerAuthority) && scope.thread !== undefined)
        yield* peerAuthority.value.checkThread(scope.thread.threadId);
      if (taskId !== undefined) {
        const task = yield* loadScheduledTask(taskId, caller.limits);
        yield* assertSameLink({ scope, caller: caller.parent?.thread }, task);
        yield* assertLiveCallerForOtherProject(scope, caller.parent, task.projectId);
      }
    });
  const scheduleError = (error: ScheduledTaskError | OrchestratorMcpFailure) =>
    error._tag === "OrchestratorMcpFailure"
      ? error
      : isMcpFailure(error.cause)
        ? error.cause
        : failure("orchestration_error", `Could not mutate scheduled task: ${error.message}`);

  const service: Omit<OrchestratorMcpServiceShape, RunGroupMethods> = {
    scheduleTask: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits } = yield* loadCaller(scope);
        const projectId = yield* resolveProjectTarget(parent, input.projectId);
        yield* assertLiveCallerForOtherProject(scope, parent, projectId);
        const project = yield* requireProject(projectId);
        const request = yield* scheduleRequest(scope);
        if (request !== undefined) {
          const prior = yield* scheduleReplay(scope, request, projectId);
          if (Option.isSome(prior)) return prior.value;
        }

        const callerCanBind =
          input.bare === undefined && parent !== undefined && parent.thread.projectId === projectId;
        const bindToCurrentThread =
          input.bindToCurrentThread !== undefined
            ? input.bindToCurrentThread
            : callerCanBind &&
              scheduleBindsToCallerByDefault(input.schedule, yield* Clock.currentTimeMillis);
        if (
          bindToCurrentThread &&
          (parent === undefined || parent.thread.projectId !== projectId)
        ) {
          return yield* failure(
            "invalid_request",
            parent === undefined
              ? "bindToCurrentThread needs an agent running inside a T3 thread."
              : "bindToCurrentThread binds to this thread, which belongs to a different project.",
          );
        }
        if (
          input.bare !== undefined &&
          bindToCurrentThread &&
          (parent?.thread.bare === undefined || (input.bare.additions?.length ?? 0) > 0)
        )
          return yield* failure(
            "invalid_request",
            "A bound bare schedule needs a bare thread and no new additions.",
          );
        const modelSelection =
          parent?.thread.modelSelection ?? (yield* projectDefaultModelSelection(project));
        const derivedTitle = input.prompt.split("\n")[0]?.trim() ?? "";
        const title =
          input.title ?? (derivedTitle.length > 0 ? derivedTitle.slice(0, 80) : "Scheduled task");
        const linkOrigin = callerLinkOrigin({ scope, caller: parent?.thread });
        const upsertInput: ScheduledTaskUpsertInput = {
          purposeIntent: input.purposeIntent,
          ...(input.bare === undefined ? {} : { bare: input.bare }),
          ...(linkOrigin === undefined ? {} : { linkOrigin }),
          title,
          prompt: input.prompt,
          enabled: input.enabled ?? true,
          schedule: input.schedule,
          projectId,
          threadId: bindToCurrentThread && parent !== undefined ? parent.thread.id : null,
          workspaceStrategy: scheduledTaskWorkspaceStrategy(bindToCurrentThread),
          modelSelection,
          runtimeMode: limits.runtimeMode,
          interactionMode: limits.interactionMode,
          createdBy: "agent",
          creationSource: "mcp",
          // Scope the idempotency key by authenticated caller so two callers
          // reusing the same clientRequestId cannot collide on one task row.
          ...(input.clientRequestId === undefined
            ? {}
            : {
                commandId: stableCommandId({
                  scope,
                  requestKey: input.clientRequestId,
                  operation: "schedule-task",
                }),
              }),
        };
        const bareReadGrant =
          input.bare === undefined
            ? undefined
            : yield* scheduledBareGrant(parent, limits.runtimeMode);
        const { task } = yield* forkTasks
          .schedule(
            {
              input,
              projectId,
              parent,
              bindToCurrentThread,
              scope: { requestNamespace: requestNamespace(scope) },
            },
            bareReadGrant === undefined
              ? scheduledTasks.upsert
              : (request) => scheduledTasks.upsert(request, { bareReadGrant }),
          )(upsertInput)
          .pipe(
            Effect.provideService(
              ScheduleMutation.CurrentMutation,
              request === undefined
                ? undefined
                : {
                    request,
                    recheck: scheduleRecheck(scope),
                    consumesSecret:
                      input.schedule?.type === "webhook" &&
                      input.schedule.signature?.secretRef !== undefined,
                  },
            ),
            Effect.mapError(scheduleError),
          );
        if (request !== undefined) {
          const accepted = yield* scheduleReceipt(request);
          if (Option.isNone(accepted))
            return yield* failure(
              "orchestration_error",
              "Native scheduler acceptance was not recorded.",
            );
          return accepted.value;
        }
        return yield* summarizeScheduledTask(scope, { parent, limits }, task);
      }),
    listScheduledTasks: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits } = yield* loadCaller(scope);
        const projectId = input.projectId ?? parent?.thread.projectId;
        const { tasks } = yield* scheduledTasks
          .list()
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Could not list scheduled tasks: ${error.message}`),
            ),
          );
        return {
          tasks: yield* Effect.forEach(
            tasks.filter((task) => projectId === undefined || task.projectId === projectId),
            (task) => summarizeScheduledTask(scope, { parent, limits }, task),
          ),
        };
      }),
    updateScheduledTask: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits } = yield* loadCaller(scope);
        const request = yield* scheduleRequest(scope);
        if (request !== undefined) {
          const prior = yield* scheduleReplay(scope, request);
          if (Option.isSome(prior)) return prior.value;
        }
        const existing = yield* loadScheduledTask(input.scheduledTaskId, limits);
        yield* assertSameLink({ scope, caller: parent?.thread }, existing);
        yield* assertLiveCallerForOtherProject(scope, parent, existing.projectId);
        const { scheduledTaskId: _id, enabled, ...definition } = input;
        if (
          enabled !== undefined &&
          Object.values(definition).every((value) => value === undefined)
        ) {
          const { task } = yield* scheduledTasks
            .setEnabled({ id: existing.id, enabled })
            .pipe(
              Effect.mapError((error) =>
                failure("orchestration_error", `Could not update scheduled task: ${error.message}`),
              ),
            );
          return yield* summarizeScheduledTask(scope, { parent, limits }, task);
        }
        if (
          input.bindToCurrentThread === true &&
          (parent === undefined || parent.thread.projectId !== existing.projectId)
        ) {
          return yield* failure(
            "invalid_request",
            parent === undefined
              ? "bindToCurrentThread needs an agent running inside a T3 thread."
              : "bindToCurrentThread binds to this thread, which belongs to a different project.",
          );
        }
        const threadId =
          input.bindToCurrentThread === undefined
            ? existing.threadId
            : input.bindToCurrentThread && parent !== undefined
              ? parent.thread.id
              : null;
        // Rebinding changes where runs execute, so the workspace strategy must
        // follow: unbinding a root-strategy task would otherwise run loose
        // prompts in the shared project checkout.
        const workspaceStrategy =
          input.bindToCurrentThread === undefined
            ? existing.workspaceStrategy
            : scheduledTaskWorkspaceStrategy(input.bindToCurrentThread);
        const upsertInput: ScheduledTaskUpsertInput = {
          purposeIntent: input.purposeIntent,
          id: existing.id,
          ...((input.bare ?? existing.bare) === undefined
            ? {}
            : { bare: input.bare ?? existing.bare }),
          title: input.title ?? existing.title,
          prompt: input.prompt ?? existing.prompt,
          enabled: input.enabled ?? existing.enabled,
          schedule: input.schedule ?? existing.schedule,
          projectId: existing.projectId,
          threadId,
          workspaceStrategy,
          modelSelection: existing.modelSelection,
          runtimeMode: existing.runtimeMode,
          interactionMode: existing.interactionMode,
          createdBy: existing.createdBy,
          creationSource: existing.creationSource,
        };
        const bareReadGrant =
          input.bare === undefined
            ? undefined
            : yield* scheduledBareGrant(parent, limits.runtimeMode);
        const { task } = yield* forkTasks
          .update(
            { input, existing, threadId, parent },
            bareReadGrant === undefined
              ? scheduledTasks.upsert
              : (request) => scheduledTasks.upsert(request, { bareReadGrant }),
          )(upsertInput)
          .pipe(
            Effect.provideService(
              ScheduleMutation.CurrentMutation,
              request === undefined
                ? undefined
                : {
                    request,
                    recheck: scheduleRecheck(scope, existing.id),
                    consumesSecret:
                      input.schedule?.type === "webhook" &&
                      input.schedule.signature?.secretRef !== undefined,
                  },
            ),
            Effect.mapError(scheduleError),
          );
        if (request !== undefined) {
          const accepted = yield* scheduleReceipt(request);
          if (Option.isNone(accepted))
            return yield* failure(
              "orchestration_error",
              "Native scheduler acceptance was not recorded.",
            );
          return accepted.value;
        }
        return yield* summarizeScheduledTask(scope, { parent, limits }, task);
      }),
    deleteScheduledTask: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits } = yield* loadCaller(scope);
        const request = yield* scheduleRequest(scope);
        if (request !== undefined) {
          const prior = yield* scheduleReplay(scope, request);
          if (Option.isSome(prior)) return prior.value;
        }
        const existing = yield* loadScheduledTask(input.scheduledTaskId, limits);
        yield* assertSameLink({ scope, caller: parent?.thread }, existing);
        yield* assertLiveCallerForOtherProject(scope, parent, existing.projectId);
        yield* forkTasks
          .delete(
            { existing, parent },
            scheduledTasks.delete,
          )({ id: existing.id })
          .pipe(
            Effect.provideService(
              ScheduleMutation.CurrentMutation,
              request === undefined
                ? undefined
                : { request, recheck: scheduleRecheck(scope, existing.id), consumesSecret: false },
            ),
            Effect.mapError(scheduleError),
          );
        if (request !== undefined) {
          const accepted = yield* scheduleReceipt(request);
          if (Option.isNone(accepted))
            return yield* failure(
              "orchestration_error",
              "Native scheduler acceptance was not recorded.",
            );
          return accepted.value;
        }
        return { scheduledTaskId: existing.id, deleted: true };
      }),
    requestSecret: (scope, input) =>
      Effect.gen(function* () {
        const { scope: threadScope, parent } = yield* loadThreadCaller(scope, "request_secret");
        if (input.fields !== undefined && input.placeholder !== undefined) {
          return yield* failure(
            "invalid_request",
            "placeholder is for single-field requests; give each entry in fields its own.",
          );
        }
        const threadId = threadScope.thread.threadId;
        const run = ThreadManagementService.latestActiveRun(parent);
        if (
          run === undefined ||
          run.rootNodeId === null ||
          run.providerInstanceId !== threadScope.thread.providerInstanceId
        ) {
          return yield* failure(
            "parent_not_active",
            "Asking for a secret requires an active run owned by this MCP provider session.",
          );
        }
        const runId = run.id;
        const nodeId = run.rootNodeId;
        const key = yield* requestKey(input.clientRequestId);
        const turnItemId = TurnItemId.make(
          `turn-item:secret-request:${stablePart(threadId)}:${stablePart(key)}`,
        );
        const record = (secretStatus: "pending" | "cancelled") =>
          threadManagement
            .dispatch({
              type: "secret_request.record",
              commandId: stableCommandId({
                scope,
                requestKey: key,
                operation: `secret-${secretStatus}`,
              }),
              threadId: threadId,
              runId,
              nodeId,
              turnItemId,
              label: input.label,
              reason: input.reason,
              ...(input.placeholder === undefined ? {} : { placeholder: input.placeholder }),
              ...(input.fields === undefined ? {} : { fields: input.fields }),
              secretStatus,
            })
            .pipe(
              Effect.mapError((error) =>
                failure(
                  "orchestration_error",
                  `Could not record the secret request: ${errorMessage(error)}`,
                ),
              ),
            );
        yield* record("pending");
        // Only this call can hand the agent its ref, so the card must not
        // outlive it: a timeout, a failed wait or an aborted call closes it as
        // cancelled.
        const closeCard = record("cancelled").pipe(
          Effect.catch((error) =>
            Effect.logWarning("Could not close a secret request card", { error: error.message }),
          ),
        );

        const answered = yield* Effect.gen(function* () {
          while (true) {
            const projection = yield* threadManagement
              .getThreadRecords(threadId, ["runs", "turnItems"], {
                turnItemTypes: ["secret_request"],
                messageRoles: [],
              })
              .pipe(
                Effect.mapError((error) =>
                  failure(
                    "orchestration_error",
                    `Unable to read the secret request: ${errorMessage(error)}`,
                  ),
                ),
              );
            const item = projection.turnItems.find((candidate) => candidate.id === turnItemId);
            if (item?.type === "secret_request" && item.secretStatus !== "pending") {
              return item.secretStatus;
            }
            const current = projection.runs.find((candidate) => candidate.id === runId);
            if (
              current === undefined ||
              ThreadManagementService.isTerminalRunStatus(current.status)
            ) {
              yield* record("cancelled");
              return "cancelled" as const;
            }
            yield* Effect.sleep(Duration.millis(SECRET_REQUEST_POLL_INTERVAL_MS));
          }
        }).pipe(
          Effect.timeoutOption(
            Duration.millis(
              Math.min(input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS, MAX_WAIT_TIMEOUT_MS),
            ),
          ),
          Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : closeCard)),
        );
        if (Option.isNone(answered)) yield* closeCard;
        const status = Option.isSome(answered)
          ? answered.value
          : yield* threadManagement
              .getThreadRecords(threadId, ["turnItems"], {
                turnItemTypes: ["secret_request"],
                messageRoles: [],
              })
              .pipe(
                Effect.map((records) => {
                  const item = records.turnItems.find((candidate) => candidate.id === turnItemId);
                  return item?.type === "secret_request" &&
                    (item.secretStatus === "saved" || item.secretStatus === "declined")
                    ? item.secretStatus
                    : ("timed_out" as const);
                }),
                Effect.orElseSucceed(() => "timed_out" as const),
              );
        yield* Effect.annotateCurrentSpan({ "secret_request.status": status });
        yield* Metrics.increment(Metrics.secretRequestsTotal, { status });
        if (status !== "saved") return { status };
        // Saved means the files were written before the card said so; none
        // left means a storage fault, or the agent already used and deleted them.
        const saved = yield* secretRequests.saved({ threadId: threadId, turnItemId });
        if (Option.isNone(saved)) {
          return yield* failure(
            "orchestration_error",
            "The user saved the secret, but it could not be read. Ask again with a new clientRequestId.",
          );
        }
        return { status, ...saved.value };
      }).pipe(Effect.withSpan("OrchestratorMcpService.requestSecret")),

    capabilities: (scope) =>
      Effect.gen(function* () {
        const { parent, limits } = yield* loadCaller(scope);
        const providers = yield* loadProviders;
        const orchestrationCapableInstanceIds = yield* loadOrchestrationCapableInstanceIds();
        const modelPreferences = yield* serverSettings.getSettings.pipe(
          Effect.map((settings) => settings.providerModelPreferences ?? {}),
          Effect.mapError(() => failure("orchestration_error", "Unable to read server settings.")),
        );
        const bareSupports = new Map(
          yield* Effect.forEach(providers, (provider) =>
            Effect.gen(function* () {
              const adapter = yield* providerAdapters.get(provider.instanceId).pipe(Effect.option);
              return [
                provider.instanceId,
                Option.isSome(adapter) && adapter.value.bareSupport !== undefined
                  ? yield* adapter.value.bareSupport
                  : BareRuntime.unsupportedBare(provider.driver),
              ] as const;
            }),
          ),
        );
        return {
          bareLaunch: true,
          toolAllowlist: true,
          executionPurposeVersions: [1],
          ...(Option.isSome(attachmentStaging) ? { peerAttachments: 1 as const } : {}),
          ...(Option.isSome(requestReceipts) ? { peerRequests: 1 as const } : {}),
          parentThreadId: parent?.thread.id ?? null,
          inheritedProviderInstanceId: parent?.thread.modelSelection.instanceId ?? null,
          inheritedModel: parent?.thread.modelSelection.model ?? null,
          runtimeMode: limits.runtimeMode,
          interactionMode: limits.interactionMode,
          providers: providers.map((provider) => {
            const constraints = providerConstraints(
              provider,
              orchestrationCapableInstanceIds.has(provider.instanceId),
            );
            return {
              providerInstanceId: provider.instanceId,
              driverKind: provider.driver,
              bare: bareSupports.get(provider.instanceId),
              displayName: provider?.displayName ?? null,
              models:
                provider?.models
                  .filter(
                    (model) =>
                      model.isCustom ||
                      !modelPreferences[provider.instanceId]?.hiddenModels.includes(model.slug),
                  )
                  .map((model) => ({
                    id: model.slug,
                    label: model.name ?? null,
                    ...(model.capabilities?.optionDescriptors === undefined
                      ? {}
                      : { options: model.capabilities.optionDescriptors }),
                  })) ?? [],
              ...(provider.supportedRuntimeModes === undefined ||
              provider.supportedRuntimeModes.length === 0
                ? {}
                : { runtimeModes: [...provider.supportedRuntimeModes] }),
              canRunChildTask: constraints.length === 0,
              canRunCrossProviderChildTask: constraints.length === 0,
              constraints: [...constraints],
            };
          }),
          features: {
            appOwnedSubagents: true,
            asyncPolling: true,
            cancellation: true,
            batchThreadCreation: true,
            threadManagement: true,
            incrementalThreadRead: true,
            scheduledTasks: true,
            maxBatchThreads: 20,
          },
        };
      }),
    delegateTask: (callerScope, input) =>
      Effect.gen(function* () {
        const environmentId =
          input.target === undefined
            ? undefined
            : PeerForwarding.remoteTarget(callerScope, input.target.environmentId);
        if (input.target !== undefined && environmentId !== undefined) {
          yield* requireCapability(callerScope);
          const scope = yield* requireThreadScope(callerScope, "delegate_task");
          // The task's child runs as an ordinary thread there; this thread keeps the task.
          const { taskId } = yield* remoteDelegation.delegate(scope, {
            ...input,
            target: { ...input.target, environmentId },
          });
          return yield* input.mode === "wait"
            ? awaitTask(scope, taskId, input.timeoutMs)
            : readTask(scope, taskId, false, true);
        }
        const { scope, parent } = yield* loadThreadCaller(callerScope, "delegate_task");
        if (
          input.target?.projectId !== undefined &&
          input.target.projectId !== parent.thread.projectId
        ) {
          return yield* failure(
            "invalid_request",
            "A local task runs in this thread's project. Omit target.projectId, or pass target.environmentId to run it in that environment's project.",
          );
        }
        const parentRun = parent.runs
          .filter(ThreadManagementService.isActiveRun)
          .toSorted((left, right) => right.ordinal - left.ordinal)[0];
        if (
          parentRun === undefined ||
          parentRun.rootNodeId === null ||
          parentRun.providerInstanceId !== scope.thread.providerInstanceId
        ) {
          return yield* failure(
            "parent_not_active",
            "Delegated tasks require an active run owned by this MCP provider session.",
          );
        }
        if (input.projectId !== undefined || input.workspaceStrategy !== undefined) {
          const destination = yield* requireProject(input.projectId ?? parent.thread.projectId);
          if (input.workspaceStrategy?.type === "existing_worktree") {
            yield* assertProjectWorktree(
              destination.workspaceRoot,
              input.workspaceStrategy.worktreePath,
            ).pipe(
              Effect.provideService(FileSystem.FileSystem, fileSystem),
              Effect.provideService(GitVcsDriver.GitVcsDriver, gitDriver),
            );
          }
        }
        const providers = yield* loadProviders;
        const key = yield* requestKey(input.clientRequestId);
        const commandId = stableCommandId({
          scope,
          requestKey: key,
          operation: "delegate-task",
        });
        const isRetryOfAcceptedDelegation = yield* threadManagement.hasReceipt(commandId);
        const prismRole =
          input.prismRole === undefined
            ? Prism.delegatedPrismRole(input.role)
            : admitDelegateRole(input.prismRole, isRetryOfAcceptedDelegation);
        if (prismRole === undefined)
          return yield* failure("invalid_request", PRISM_DELEGATE_ROLE_ERROR);
        const explicitTarget =
          input.target === undefined
            ? undefined
            : yield* resolveTargetRechecking({
                parent,
                projectId: input.projectId,
                target: input.target,
                providers,
                skipProjectEnablementFilter: isRetryOfAcceptedDelegation,
              });
        let runtimeTarget = explicitTarget;
        const target = yield* prism.resolve({
          bare: input.bare !== undefined,
          projectId: parent.thread.projectId,
          role: prismRole,
          lane: input.lane,
          explicit: explicitTarget?.modelSelection,
          inherited: parent.thread.modelSelection,
          validate:
            explicitTarget === undefined
              ? (selection) =>
                  resolveTargetRechecking({
                    parent,
                    projectId: input.projectId,
                    providers,
                    target: {
                      providerInstanceId: selection.instanceId,
                      model: selection.model,
                      options: selection.options,
                    },
                    skipProjectEnablementFilter: isRetryOfAcceptedDelegation,
                  }).pipe(
                    Effect.map((resolved) => {
                      runtimeTarget = resolved;
                      return resolved.modelSelection;
                    }),
                    Effect.flatMap((selection) =>
                      input.bare === undefined ? Effect.succeed(selection) : bareTarget(selection),
                    ),
                  )
              : undefined,
        });
        if (input.bare !== undefined && !isRetryOfAcceptedDelegation)
          yield* bareTarget(target.modelSelection);
        const runtimeMode = yield* resolveRuntimeMode(
          parent.thread.runtimeMode,
          input.runtimeMode,
          runtimeTarget,
        );
        const interactionMode = yield* resolveInteractionMode(
          parent.thread.interactionMode,
          input.interactionMode,
        );
        let resolvedTools: import("@t3tools/contracts").ThreadTools | undefined;
        if (input.tools !== undefined && !isRetryOfAcceptedDelegation) {
          const adapter = yield* providerAdapters
            .get(target.modelSelection.instanceId)
            .pipe(
              Effect.mapError(() =>
                failure("invalid_request", "Tool allowlist provider is unavailable."),
              ),
            );
          const rejection = toolPolicyRejection(adapter.driver, input.tools, {
            runtimeMode,
            interactionMode,
          });
          if (rejection !== undefined) return yield* failure("invalid_request", rejection);
          if (Option.isNone(threadMcpConfig))
            return yield* failure(
              "invalid_request",
              "Thread MCP configuration service is unavailable.",
            );
          const configs = threadMcpConfig.value;
          resolvedTools = yield* configs
            .prepare(input.tools)
            .pipe(Effect.mapError((error) => failure("invalid_request", error.message)));
        }
        let workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy | undefined;
        if (input.isolation === "worktree" && !isRetryOfAcceptedDelegation) {
          const projectId = input.projectId ?? parent.thread.projectId;
          const project = yield* requireProject(projectId);
          const sameProject = projectId === parent.thread.projectId;
          const base = yield* gitDriver
            .resolveCommit({
              cwd: sameProject
                ? (parent.thread.worktreePath ?? project.workspaceRoot)
                : project.workspaceRoot,
              revision: "HEAD",
            })
            .pipe(
              Effect.mapError(() =>
                failure(
                  "invalid_request",
                  sameProject
                    ? "Unable to resolve the parent checkout commit."
                    : "Unable to resolve the destination project checkout commit. Worktree isolation requires a Git repository with a HEAD commit.",
                ),
              ),
            );
          workspaceStrategy = { type: "worktree", baseRef: base.commitSha, startFromOrigin: false };
        }
        const bareDestination =
          input.bare === undefined || isRetryOfAcceptedDelegation
            ? undefined
            : yield* requireProject(input.projectId ?? parent.thread.projectId);
        const preparedBare =
          input.bare === undefined || bareDestination === undefined
            ? undefined
            : yield* bareLaunch
                .prepare({
                  request: input.bare,
                  instanceId: target.modelSelection.instanceId,
                  projectRoot: bareDestination.workspaceRoot,
                  workspaceStrategy:
                    workspaceStrategy ??
                    input.workspaceStrategy ??
                    (input.projectId !== undefined && input.projectId !== parent.thread.projectId
                      ? bareDestination.defaultThreadEnvMode === "worktree"
                        ? { type: "worktree", baseRef: "HEAD" }
                        : { type: "root" }
                      : parent.thread.worktreePath === null
                        ? { type: "root", branch: parent.thread.branch ?? undefined }
                        : {
                            type: "existing_worktree",
                            worktreePath: parent.thread.worktreePath,
                            branch: parent.thread.branch ?? undefined,
                          }),
                  readGrant:
                    parent.thread.runtimeMode === "full-access"
                      ? { type: "all" }
                      : {
                          type: "roots",
                          roots: [
                            parent.thread.worktreePath ??
                              (yield* requireProject(parent.thread.projectId)).workspaceRoot,
                          ],
                        },
                })
                .pipe(Effect.mapError((error) => failure("invalid_request", error.message)));
        // ThreadManagementService.dispatch is the chokepoint for this command
        // type's own enablement check, using the same command id and its
        // receipt-replay exemption, so no separate assert runs here.
        const result = yield* threadManagement
          .dispatch({
            type: "delegated_task.request",
            ...(resolvedTools === undefined ? {} : { tools: resolvedTools }),
            ...(input.purposeIntent === undefined ? {} : { purposeIntent: input.purposeIntent }),
            createdBy: "agent",
            creationSource: "mcp",
            commandId,
            parentThreadId: scope.thread.threadId,
            parentRunId: parentRun.id,
            parentNodeId: parentRun.rootNodeId,
            ...(input.projectId === undefined ? {} : { projectId: input.projectId }),
            ...(input.workspaceStrategy === undefined
              ? {}
              : { workspaceStrategy: input.workspaceStrategy }),
            ...(preparedBare === undefined
              ? {}
              : { bare: preparedBare.bare, workspaceStrategy: preparedBare.workspaceStrategy }),
            task: [target.kitText, taskPrompt(input)].filter(Boolean).join("\n\n"),
            ...(input.title === undefined ? {} : { title: input.title }),
            modelSelection: target.modelSelection,
            runtimeMode,
            interactionMode,
            ...(preparedBare === undefined && workspaceStrategy !== undefined
              ? { workspaceStrategy }
              : {}),
            // Async delegations wake the parent on every child terminal; wait
            // delegations deliver through the blocking tool call, so a wake is
            // only needed if the parent settled first (timeout, disconnect).
            completionWake: input.mode === "wait" ? "settled_only" : "always",
          })
          .pipe(
            Effect.mapError((error) =>
              error._tag === "ThreadManagementProviderInstanceDisabledError"
                ? threadManagementFailure(error)
                : failure(
                    "orchestration_error",
                    `Unable to create delegated task: ${errorMessage(error)}`,
                  ),
            ),
          );
        const taskEvent = result.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.origin === "app_owned",
        );
        if (taskEvent?.event.type !== "subagent.updated") {
          return yield* failure(
            "orchestration_error",
            "Delegated task command did not produce a task projection.",
          );
        }
        const taskId = taskEvent.event.payload.id;
        const childThreadId = taskEvent.event.payload.childThreadId;
        if (
          (input.bare !== undefined || input.tools !== undefined) &&
          isRetryOfAcceptedDelegation &&
          childThreadId !== null
        ) {
          const child = yield* threadManagement
            .getThreadRecords(childThreadId, [])
            .pipe(Effect.mapError(threadManagementFailure));
          if (input.tools !== undefined && !toolAllowlistsEqual(input.tools, child.thread.tools))
            return yield* failure("invalid_request", "Delegation tool allowlist changed.");
          if ((input.bare !== undefined) !== (child.thread.bare !== undefined))
            return yield* failure("invalid_request", "Delegation bare intent changed.");
        }
        const childRunEvent = result.storedEvents.find(
          (stored) =>
            stored.event.type === "run.created" && stored.event.threadId === childThreadId,
        );
        if (
          childThreadId !== null &&
          childRunEvent?.event.type === "run.created" &&
          childRunEvent.event.payload.workspacePreparation !== undefined
        ) {
          yield* threadLaunch.prepareRun({
            commandId,
            threadId: childThreadId,
            runId: childRunEvent.event.payload.id,
          });
        }

        if (input.mode !== "wait") {
          return yield* readTask(scope, taskId, false, true);
        }
        const timeoutMs = Math.min(
          MAX_WAIT_TIMEOUT_MS,
          Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
        );
        const waited = yield* waitForTask(scope, taskId, timeoutMs);
        if (Option.isSome(waited) && isTerminalTaskStatus(waited.value.status)) {
          return waited.value;
        }
        // An incomplete wait gives up delivery: upgrade so a later terminal
        // wakes the parent even mid-turn.
        yield* threadManagement
          .dispatch({
            type: "delegated_task.wake-policy",
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "delegate-task-wake-policy",
            }),
            parentThreadId: scope.thread.threadId,
            taskId,
            completionWake: "always",
          })
          .pipe(
            Effect.catch((error) =>
              Effect.logWarning("orchestrator-mcp.delegate-task.wake-policy-failed", {
                taskId,
                outcome:
                  error._tag === "OrchestratorCommandPreviouslyRejectedError"
                    ? "previously_rejected"
                    : "dispatch_failed",
                error,
              }),
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestrator-mcp.delegate-task.wake-policy-failed", {
                taskId,
                outcome: "defect",
                cause,
              }),
            ),
          );
        return yield* readTask(scope, taskId, Option.isNone(waited), true);
      }),
    taskStatus: (callerScope, taskId) =>
      Effect.gen(function* () {
        const scope = yield* requireThreadScope(callerScope, "task_status");
        return yield* readTask(scope, taskId, false, true);
      }),
    cancelTask: (callerScope, input) =>
      Effect.gen(function* () {
        const scope = yield* requireThreadScope(callerScope, "task_cancel");
        const current = yield* readTask(scope, input.taskId);
        const childThreadId = current.childThreadId;
        if (childThreadId === null) {
          // The task runs in a linked environment: its thread there is stopped through the link.
          const { subagents } = yield* loadProjection(scope.thread.threadId);
          const remoteTask = subagents.find(
            (task) =>
              task.id === input.taskId &&
              task.origin === "app_owned" &&
              task.remoteChild !== undefined,
          );
          if (remoteTask === undefined) {
            return yield* failure(
              "task_not_found",
              `Delegated task ${input.taskId} does not belong to thread ${scope.thread.threadId}.`,
            );
          }
          return yield* remoteDelegation.cancel(scope, remoteTask, input);
        }
        const key = yield* requestKey(input.clientRequestId);
        const { parent: parentProjection } = yield* loadThreadCaller(scope, "task_cancel");
        // Cancelling stops the child and every task under it, each a write to a
        // thread its user may have raised above the parent's modes since it was
        // delegated. All of them are checked before anything is stopped.
        const assertStoppable = (threadId: ThreadId): Effect.Effect<void, OrchestratorMcpFailure> =>
          Effect.gen(function* () {
            const shell = yield* threadManagement
              .getThreadShell(threadId)
              .pipe(Effect.mapError(threadManagementFailure));
            if (shell !== null && shell.deletedAt === null) {
              yield* resolveRuntimeMode(parentProjection.thread.runtimeMode, shell.runtimeMode);
              yield* resolveInteractionMode(
                parentProjection.thread.interactionMode,
                shell.interactionMode,
              );
            }
            const { subagents } = yield* threadManagement
              .getThreadRecords(threadId, ["subagents"])
              .pipe(Effect.mapError(threadManagementFailure));
            for (const task of subagents) {
              if (task.origin === "app_owned" && task.childThreadId !== null) {
                yield* assertStoppable(task.childThreadId);
              }
            }
          });
        yield* assertStoppable(childThreadId);
        // Fork: a Spectrum task has no run of its own; its Stop cancels every Color. Once
        // its report exists that report is the caller's wake, so nothing is stopped.
        const spectrum = yield* (yield* SpectrumMcpInterrupt).stop(
          threadManagement,
          { threadId: childThreadId, reason: input.reason, keepReported: true },
          stableCommandId({ scope, requestKey: key, operation: "cancel-task" }),
        );
        if (spectrum !== null) {
          // The report may have settled after `current` was read; its row then reads terminal.
          const latest = spectrum === "reported" ? yield* readTask(scope, input.taskId) : current;
          return {
            taskId: input.taskId,
            status:
              spectrum === "reported" && isTerminalTaskStatus(latest.status)
                ? latest.status
                : "cancel_requested",
          } satisfies OrchestratorMcpTaskCancelResult;
        }
        const parentTask = parentProjection.subagents.find(
          (task) => task.id === input.taskId && task.origin === "app_owned",
        );
        // A Run group member's result stays owed to the group's one wake.
        const disposeCompletionDelivery =
          (parentTask?.completionDelivery?.state === "disposed" &&
            parentTask.completionDelivery.disposedBy === undefined) ||
          parentTask?.runGroupId !== undefined
            ? Effect.void
            : threadManagement
                .dispatch({
                  type: "delegated_task.completion-delivery.dispose",
                  commandId: stableCommandId({
                    scope,
                    requestKey: key,
                    operation: "cancel-task-completion-delivery",
                  }),
                  parentThreadId: scope.thread.threadId,
                  taskId: input.taskId,
                })
                .pipe(
                  Effect.asVoid,
                  Effect.mapError((error) =>
                    failure(
                      "orchestration_error",
                      `Unable to dispose delegated task ${input.taskId} completion delivery: ${errorMessage(error)}`,
                    ),
                  ),
                );
        const stopChild = Effect.gen(function* () {
          const commandId = stableCommandId({ scope, requestKey: key, operation: "cancel-task" });
          const reason = input.reason;
          yield* threadManagement
            .dispatch({
              type: "thread.stop",
              commandId,
              threadId: childThreadId,
              ...(reason === undefined ? {} : { reason }),
            })
            .pipe(
              Effect.mapError((error) =>
                failure(
                  "task_not_cancellable",
                  `Unable to stop delegated task ${input.taskId}: ${errorMessage(error)}`,
                ),
              ),
            );
          yield* threadManagement
            .stopDelegatedTasks({ threadId: childThreadId, commandId, reason })
            .pipe(
              Effect.mapError((error) =>
                failure(
                  "task_not_cancellable",
                  `Stopped delegated task ${input.taskId}, but not every task it delegated: ${errorMessage(error)}`,
                ),
              ),
            );
        });
        // Checked above; a user raising one of these threads meanwhile is
        // caught again under that thread's lock, which leaves it running.
        const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
        const stopWithinLimit = stopChild.pipe(
          Effect.provideService(DispatchModeLimit, {
            runtimeMode: parentProjection.thread.runtimeMode,
            interactionMode: parentProjection.thread.interactionMode,
            refused,
          }),
          Effect.catch((error) =>
            Effect.flatMap(Ref.get(refused), (refusal) =>
              Effect.fail(
                refusal === undefined
                  ? error
                  : failure(
                      refusal.mode === "runtime"
                        ? "runtime_mode_escalation_denied"
                        : "interaction_mode_escalation_denied",
                      `Thread ${refusal.threadId} now runs in ${refusal.runtimeMode}/${refusal.interactionMode} mode, above this thread's; its user changed it while the task was being cancelled.`,
                    ),
              ),
            ),
          ),
        );
        if (isTerminalTaskStatus(current.status)) {
          yield* stopWithinLimit;
          yield* disposeCompletionDelivery;
          return {
            taskId: input.taskId,
            status: current.status,
          } satisfies OrchestratorMcpTaskCancelResult;
        }
        // A task waiting on its own delegates has work to stop below it. One with neither
        // a running turn nor delegates (such as one awaiting a restart) keeps its delivery.
        const child = yield* loadProjection(childThreadId);
        if (
          current.workState !== "waiting_for_children" &&
          ThreadManagementService.latestActiveRun(child) === undefined
        ) {
          return yield* failure(
            "task_not_cancellable",
            `Delegated task ${input.taskId} has no interruptible child run.`,
          );
        }
        yield* stopWithinLimit;
        yield* disposeCompletionDelivery.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestrator-mcp.cancel-task.delivery-dispose-failed", {
              taskId: input.taskId,
              cause,
            }),
          ),
        );
        return {
          taskId: input.taskId,
          status: "cancel_requested",
        };
      }),
    createThreads: (callerScope, input) =>
      Effect.gen(function* () {
        const { scope, parent } = yield* loadThreadCaller(callerScope, "create_threads");
        const parentRun = ThreadManagementService.latestActiveRun(parent);
        if (
          parentRun === undefined ||
          parentRun.rootNodeId === null ||
          parentRun.providerInstanceId !== scope.thread.providerInstanceId
        ) {
          return yield* failure(
            "parent_not_active",
            "Thread creation requires an active run owned by this MCP provider session.",
          );
        }
        const parentNodeId = parentRun.rootNodeId;
        const providers = yield* loadProviders;
        const key = yield* requestKey(input.clientRequestId);
        const created = yield* Effect.forEach(
          input.threads,
          (request, index) =>
            Effect.gen(function* () {
              const target = yield* resolveTargetRechecking({
                parent,
                target: request.target,
                providers,
              });
              const runtimeMode = yield* resolveRuntimeMode(
                parent.thread.runtimeMode,
                request.runtimeMode,
                target,
              );
              const interactionMode = yield* resolveInteractionMode(
                parent.thread.interactionMode,
                request.interactionMode,
              );
              const threadId = stableThreadId({
                scope,
                requestKey: key,
                index,
              });
              if (Option.isSome(peerAuthority)) {
                const inherited = yield* peerAuthority.value.forThread(parent.thread.id);
                if (inherited !== undefined) yield* peerAuthority.value.bind(inherited, threadId);
              }
              yield* assertTargetEnabledForProject(
                parent,
                stableCommandId({ scope, requestKey: key, operation: "create-thread", index }),
                target,
              );
              const title = threadTitle({
                parentTitle: parent.thread.title,
                prompt: request.prompt,
                title: request.title,
                index,
              });
              const wasAccepted =
                request.bare !== undefined &&
                (yield* threadManagement.hasReceipt(
                  stableCommandId({ scope, requestKey: key, operation: "create-thread", index }),
                ));
              const storedBare = wasAccepted
                ? (yield* threadManagement
                    .getThreadRecords(threadId, [])
                    .pipe(Effect.mapError(threadManagementFailure))).thread.bare
                : undefined;
              if (wasAccepted && (request.bare !== undefined) !== (storedBare !== undefined))
                return yield* failure("invalid_request", "Thread bare intent changed.");
              const bare = wasAccepted
                ? storedBare
                : request.bare === undefined
                  ? undefined
                  : yield* bareLaunch
                      .resolve({
                        request: request.bare,
                        instanceId: target.modelSelection.instanceId,
                        cwd:
                          parent.thread.worktreePath ??
                          (yield* requireProject(parent.thread.projectId)).workspaceRoot,
                        readGrant:
                          parent.thread.runtimeMode === "full-access"
                            ? { type: "all" }
                            : {
                                type: "roots",
                                roots: [
                                  parent.thread.worktreePath ??
                                    (yield* requireProject(parent.thread.projectId)).workspaceRoot,
                                ],
                              },
                      })
                      .pipe(Effect.mapError((error) => failure("invalid_request", error.message)));
              yield* threadManagement
                .dispatch({
                  type: "thread.create",
                  ...(bare === undefined ? {} : { bare }),
                  createdBy: "agent",
                  creationSource: "mcp",
                  // Fork: an agent's thread belongs to its caller's owner (toolboxmd/chromeria#170).
                  owner: threadOwner(parent.thread),
                  commandId: stableCommandId({
                    scope,
                    requestKey: key,
                    operation: "create-thread",
                    index,
                  }),
                  threadId,
                  projectId: parent.thread.projectId,
                  title,
                  modelSelection: target.modelSelection,
                  runtimeMode,
                  interactionMode,
                  branch: parent.thread.branch,
                  worktreePath: parent.thread.worktreePath,
                  ...(parent.thread.linkOrigin === undefined
                    ? {}
                    : { linkOrigin: parent.thread.linkOrigin }),
                })
                .pipe(
                  Effect.mapError((error) =>
                    failure(
                      "orchestration_error",
                      `Unable to create thread ${index + 1}: ${errorMessage(error)}`,
                    ),
                  ),
                );
              if (request.prompt !== undefined) {
                yield* threadManagement
                  .dispatch({
                    type: "message.dispatch",
                    ...(request.purposeIntent === undefined
                      ? {}
                      : { purposeIntent: request.purposeIntent }),
                    ...(parentRun === undefined || request.purposeIntent?.intent.type !== "inherit"
                      ? {}
                      : {
                          purposeSource: {
                            version: 1,
                            scope: executionPurposeOf(parentRun).scope,
                            provenance: {
                              type: "parent_run",
                              threadId: parent.thread.id,
                              runId: parentRun.id,
                            },
                          } satisfies ExecutionPurpose,
                        }),
                    createdBy: "agent",
                    creationSource: "mcp",
                    commandId: stableCommandId({
                      scope,
                      requestKey: key,
                      operation: "dispatch-thread",
                      index,
                    }),
                    threadId,
                    senderThreadId: scope.thread.threadId,
                    messageId: stableMessageId({
                      scope,
                      requestKey: key,
                      index,
                    }),
                    text: request.prompt,
                    attachments: [],
                    modelSelection: target.modelSelection,
                    dispatchMode: { type: "start_immediately" },
                  })
                  .pipe(
                    Effect.mapError((error) =>
                      failure(
                        "orchestration_error",
                        `Unable to start thread ${index + 1}: ${errorMessage(error)}`,
                      ),
                    ),
                  );
              }
              const projection = yield* loadProjection(threadId);
              const run = projection.runs.at(-1);
              yield* threadManagement
                .dispatch({
                  type: "thread.created.record",
                  commandId: stableCommandId({
                    scope,
                    requestKey: key,
                    operation: "record-created-thread",
                    index,
                  }),
                  parentThreadId: scope.thread.threadId,
                  parentRunId: parentRun.id,
                  parentNodeId,
                  targetThreadId: threadId,
                  targetRunId: run?.id ?? null,
                })
                .pipe(
                  Effect.mapError((error) =>
                    failure(
                      "orchestration_error",
                      `Unable to record thread ${index + 1} in the parent timeline: ${errorMessage(error)}`,
                    ),
                  ),
                );
              return {
                threadId,
                runId: run?.id ?? null,
                status: run?.status ?? "idle",
                title: projection.thread.title,
                createdBy: projection.thread.createdBy,
                creationSource: projection.thread.creationSource,
                providerInstanceId: target.modelSelection.instanceId,
                model: target.modelSelection.model,
              } satisfies OrchestratorMcpCreatedThread;
            }),
          { concurrency: 1 },
        );
        return { threads: created };
      }),
    listThreads: (scope, input) =>
      Effect.gen(function* () {
        const { parent } = yield* loadCaller(scope);
        const projectId = yield* resolveProjectTarget(parent, input.projectId);
        const projectThreads = yield* threadManagement
          .listProjectThreads({
            projectId,
            includeSubagents: input.includeSubagents !== false,
          })
          .pipe(
            Effect.mapError((error) =>
              failure("orchestration_error", `Unable to list threads: ${errorMessage(error)}`),
            ),
          );
        const nowMs = yield* Clock.currentTimeMillis;
        const statuses = input.statuses === undefined ? null : new Set(input.statuses);
        const titleContains = input.titleContains?.toLocaleLowerCase();
        const filtered = projectThreads
          .filter(
            (thread) =>
              statuses === null || statuses.has(thread.activityRunStatus ?? thread.status),
          )
          .filter(
            (thread) =>
              input.settled === undefined || threadSettlement(thread).settled === input.settled,
          )
          .filter(
            (thread) => input.snoozed === undefined || isSnoozed(thread, nowMs) === input.snoozed,
          )
          .filter(
            (thread) =>
              titleContains === undefined ||
              thread.title.toLocaleLowerCase().includes(titleContains),
          );
        const cursor = input.cursor ?? 0;
        const limit = input.limit ?? DEFAULT_THREAD_LIST_LIMIT;
        const page = filtered.slice(cursor, cursor + limit);
        const nextCursor = cursor + page.length < filtered.length ? cursor + page.length : null;
        return {
          projectId,
          currentThreadId: parent?.thread.id ?? null,
          threads: page.map((shell) => listItemFromShell(shell, nowMs)),
          nextCursor,
          total: filtered.length,
        } satisfies OrchestratorMcpThreadListResult;
      }),
    readThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent, target, shell } = yield* loadReadableThread(scope, input.threadId);
        const view = input.view ?? "messages";
        const afterPosition = input.afterPosition ?? -1;
        const limit = input.limit ?? DEFAULT_THREAD_READ_LIMIT;
        const maxChars = input.maxCharsPerItem ?? DEFAULT_THREAD_ITEM_MAX_CHARS;
        const timeline = yield* threadManagement
          .getTimelinePage(input.threadId, {
            afterPosition,
            limit,
            view,
            ...(input.itemId === undefined ? {} : { itemId: input.itemId }),
          })
          .pipe(Effect.mapError(threadManagementFailure));
        const page = timeline.items;
        const messageIdsByThread = new Map<ThreadId, Array<MessageId>>();
        for (const row of page) {
          if (row.item.type !== "user_message" && row.item.type !== "assistant_message") continue;
          const ids = messageIdsByThread.get(row.sourceThreadId) ?? [];
          ids.push(row.item.messageId);
          messageIdsByThread.set(row.sourceThreadId, ids);
        }
        const sourceMessages = yield* Effect.forEach(
          [...messageIdsByThread],
          ([threadId, messageIds]) =>
            threadManagement.getThreadRecords(threadId, ["messages"], { messageIds }).pipe(
              Effect.map((records) => [threadId, records.messages] as const),
              Effect.mapError(threadManagementFailure),
            ),
          { concurrency: 1 },
        );
        const messagesByThreadId = new Map(sourceMessages);
        const returnedRunIds = new Set(
          page
            .filter((row) => row.sourceThreadId === target.thread.id)
            .map((row) => row.item.runId),
        );
        const tasks =
          parent === undefined ? [] : directAppOwnedChildTasks(parent, target, returnedRunIds);
        if (
          parent !== undefined &&
          scope.thread !== undefined &&
          tasks.length > 0 &&
          (input.textOffset ?? 0) === 0
        ) {
          for (const task of tasks) {
            const transfer = latestDelegatedResultTransfer(
              target,
              task.id,
              parent.contextTransfers,
              parent.thread.id,
            );
            const resultRunId = transfer?.sourcePoint.runId ?? delegatedTaskRun(target, task)?.id;
            const resultRunIds = restartResultRunIds(
              target.runs,
              resultRunId === undefined ? [] : [resultRunId],
            );
            const resultRecords = yield* threadManagement
              .getThreadRecords(target.thread.id, ["messages", "turnItems"], {
                messageRoles: ["assistant"],
                messageRunIds: resultRunIds,
                turnItemRunIds: resultRunIds,
                turnItemTypes: ["assistant_message", "error"],
              })
              .pipe(Effect.mapError(threadManagementFailure));
            if (
              pageIncludesTerminalTaskResult({
                parent,
                page,
                task,
                target: { ...target, ...resultRecords },
                maxChars,
              })
            ) {
              yield* readTask(
                scope as McpThreadInvocationScope,
                task.id,
                false,
                true,
                "thread-read-acknowledge",
              );
            }
          }
        }
        return {
          thread: threadDetail(target, timeline.totalItems, shell, yield* Clock.currentTimeMillis),
          recentRuns: yield* Effect.forEach(
            target.runs
              .toSorted((left, right) => right.ordinal - left.ordinal)
              .slice(0, input.runLimit ?? DEFAULT_THREAD_RUN_LIMIT),
            (run) =>
              threadManagement.readRestartCutView(run, target.runs).pipe(
                Effect.mapError(threadManagementFailure),
                Effect.flatMap((cut) =>
                  threadManagement.readRunRecovery(run).pipe(
                    Effect.map((recovery) => threadRun(run, cut, recovery)),
                    Effect.mapError(() =>
                      failure("orchestration_error", "Unable to read run recovery."),
                    ),
                  ),
                ),
              ),
            { concurrency: 1 },
          ),
          items: page.map((row) =>
            timelineItem({
              row,
              maxChars,
              messagesByThreadId,
              ...(input.itemId === undefined ? {} : { textOffset: input.textOffset ?? 0 }),
            }),
          ),
          nextPosition: page.at(-1)?.position ?? null,
          hasMore: timeline.hasMore,
        } satisfies OrchestratorMcpThreadReadResult;
      }),
    sendToThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits, target } = yield* loadScopedThread(scope, input.threadId);
        yield* assertLiveCallerForOtherThread(scope, parent, target);
        yield* resolveRuntimeMode(limits.runtimeMode, target.thread.runtimeMode);
        yield* resolveInteractionMode(limits.interactionMode, target.thread.interactionMode);

        const mode = input.mode ?? "auto";
        let purposeSource: ExecutionPurpose | undefined;
        if (input.delegatedContinuation !== undefined) {
          const evidence = input.delegatedContinuation;
          const original = target.thread.delegatedFrom?.purposeLaunch;
          if (
            scope.client?.linked !== true ||
            target.thread.linkOrigin?.sessionId !== scope.client.sessionId ||
            original === undefined ||
            original.environmentId !== evidence.environmentId ||
            original.threadId !== evidence.threadId ||
            original.runId !== evidence.runId ||
            original.launchId !== evidence.launchId ||
            !executionPurposesEqual(original.purpose, evidence.purpose)
          ) {
            return yield* failure(
              "invalid_request",
              "The continuation does not match the original remote purpose launch.",
            );
          }
          purposeSource = {
            version: 1,
            scope: original.purpose.scope,
            provenance: {
              type: "remote_launch",
              environmentId: original.environmentId,
              threadId: original.threadId,
              runId: original.runId,
              launchId: original.launchId,
            },
          };
        } else if (
          input.purposeIntent?.intent.type === "inherit" &&
          !(
            target.thread.lineage.parentThreadId === parent?.thread.id &&
            target.thread.lineage.relationshipToParent === "subagent"
          )
        ) {
          const source =
            parent === undefined ? undefined : ThreadManagementService.latestActiveRun(parent);
          if (source === undefined || executionPurposeOf(source).scope.type !== "single") {
            return yield* failure(
              "invalid_request",
              "Purpose inheritance requires the exact scoped caller run.",
            );
          }
          purposeSource = {
            version: 1,
            scope: executionPurposeOf(source).scope,
            provenance: {
              type: "parent_run",
              threadId: source.threadId,
              runId: source.id,
            },
          };
        }
        const key = yield* requestKey(input.clientRequestId);
        const messageId = stableOperationMessageId({
          scope,
          requestKey: key,
          operation: "thread-send",
        });
        const transcript = yield* (yield* SpectrumMcpSend).send(
          threadManagement,
          input,
          stableCommandId({ scope, requestKey: key, operation: "thread-send" }),
          messageId,
          parent?.thread.id,
        );
        if (transcript !== null) return transcript;
        const result = yield* threadManagement
          .sendToThread({
            purposeIntent: input.purposeIntent,
            purposeSource,
            projectId: target.thread.projectId,
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "thread-send",
            }),
            threadId: input.threadId,
            ...(parent === undefined ? {} : { senderThreadId: parent.thread.id }),
            messageId,
            text: input.message,
            attachments: [],
            mode,
            createdBy: "agent",
            creationSource: "mcp",
          })
          .pipe(
            Effect.mapError((error) =>
              isThreadManagementError(error)
                ? threadManagementFailure(error)
                : failure(
                    "orchestration_error",
                    `Unable to send to thread ${input.threadId}: ${errorMessage(error)}`,
                  ),
            ),
          );
        return {
          threadId: input.threadId,
          messageId,
          runId: result.run.id,
          purpose: executionPurposeOf(result.run),
          warnings: executionPurposeWarnings(executionPurposeOf(result.run)),
          ...(result.run.delegatedTaskId === undefined
            ? {}
            : { taskId: result.run.delegatedTaskId }),
          status: result.run.status,
          delivery: result.delivery,
        } satisfies OrchestratorMcpThreadSendResult;
      }),
    waitForThread: (scope, input) =>
      Effect.gen(function* () {
        const { target } = yield* loadScopedThread(scope, input.threadId);
        const result = yield* threadManagement
          .waitForThread({
            projectId: target.thread.projectId,
            threadId: input.threadId,
            ...(input.runId === undefined ? {} : { runId: input.runId }),
            timeoutMs: Math.min(
              MAX_WAIT_TIMEOUT_MS,
              Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
            ),
          })
          .pipe(Effect.mapError(threadManagementFailure));
        return {
          threadId: input.threadId,
          runId: result.run?.id ?? null,
          status: result.run === null ? "idle" : runOutcome(result.run),
          timedOut: result.timedOut,
        } satisfies OrchestratorMcpThreadWaitResult;
      }),
    interruptThread: (scope, input) =>
      Effect.gen(function* () {
        const { parent, limits, target } = yield* loadScopedThread(scope, input.threadId);
        yield* assertLiveCallerForOtherThread(scope, parent, target);
        yield* resolveRuntimeMode(limits.runtimeMode, target.thread.runtimeMode);
        yield* resolveInteractionMode(limits.interactionMode, target.thread.interactionMode);
        const key = yield* requestKey(input.clientRequestId);
        const spectrum = yield* (yield* SpectrumMcpInterrupt).interrupt(
          threadManagement,
          input,
          stableCommandId({ scope, requestKey: key, operation: "thread-interrupt" }),
        );
        if (spectrum !== null) return spectrum;
        // A peer from before `stop` drops it when decoding, so it interrupts
        // the run instead, as this call did then.
        if (input.stop === true) {
          const commandId = stableCommandId({ scope, requestKey: key, operation: "thread-stop" });
          // The cause goes to the log; the caller learns only that the stop failed.
          const stopFailed = (error: unknown) =>
            Effect.logWarning("orchestrator-mcp.thread-stop-failed", {
              threadId: input.threadId,
              error,
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  failure("orchestration_error", `Unable to stop thread ${input.threadId}.`),
                ),
              ),
            );
          const stopped = yield* threadManagement
            .dispatch({
              type: "thread.stop",
              commandId,
              threadId: input.threadId,
              ...(input.reason === undefined ? {} : { reason: input.reason }),
            })
            .pipe(Effect.catch(stopFailed));
          // A retry with the same clientRequestId repeats only the stops that failed.
          yield* threadManagement
            .stopDelegatedTasks({ threadId: input.threadId, commandId, reason: input.reason })
            .pipe(Effect.catch(stopFailed));
          // The run this stop interrupted, from its own events, so a retry
          // reports the same run even if another has started since.
          const interrupted = stopped.storedEvents.find(
            (stored) => stored.event.type === "run.updated",
          )?.event.runId;
          return {
            threadId: input.threadId,
            runId: interrupted ?? null,
            status: interrupted === undefined ? "no_active_run" : "interrupt_requested",
          } satisfies OrchestratorMcpThreadInterruptResult;
        }
        const result = yield* threadManagement
          .interruptThread({
            projectId: target.thread.projectId,
            commandId: stableCommandId({
              scope,
              requestKey: key,
              operation: "thread-interrupt",
            }),
            threadId: input.threadId,
            ...(input.runId === undefined ? {} : { runId: input.runId }),
            ...(input.reason === undefined ? {} : { reason: input.reason }),
          })
          .pipe(
            Effect.mapError((error) =>
              isThreadManagementError(error)
                ? threadManagementFailure(error)
                : failure(
                    "orchestration_error",
                    `Unable to interrupt thread ${input.threadId}: ${errorMessage(error)}`,
                  ),
            ),
          );
        if (result.type === "no_active_run") {
          return {
            threadId: input.threadId,
            runId: null,
            status: "no_active_run",
          } satisfies OrchestratorMcpThreadInterruptResult;
        }
        return {
          threadId: input.threadId,
          runId: result.run.id,
          status: result.type === "already_terminal" ? result.run.status : "interrupt_requested",
        } satisfies OrchestratorMcpThreadInterruptResult;
      }),
  };
  return OrchestratorMcpService.of({ ...service, ...runGroupMethods(service) });
});

export const layer: Layer.Layer<
  OrchestratorMcpService,
  never,
  | BareLaunch.BareLaunch
  | Crypto.Crypto
  | FileSystem.FileSystem
  | ThreadManagementService.ThreadManagementService
  | ThreadLaunchService.ThreadLaunchService
  | GitVcsDriver.GitVcsDriver
  | ProviderRegistry.ProviderRegistry
  | ProviderAdapterRegistry.ProviderAdapterRegistryV2
  | ScheduledTaskService.ScheduledTaskService
  | ProjectService.ProjectService
  | SecretRequests.SecretRequests
  | ServerSettings.ServerSettingsService
  | RemoteDelegation.RemoteDelegation
> = Layer.effect(OrchestratorMcpService, make).pipe(Layer.provide(Prism.layer));
