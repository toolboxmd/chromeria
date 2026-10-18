import * as McpRouting from "../../McpRouting.ts";
import {
  OrchestratorMcpCapabilitiesInput,
  OrchestratorMcpCapabilitiesResult,
  OrchestrationV2StopWorkflowInput,
  OrchestratorMcpEnvironmentLinksResult,
  OrchestratorMcpCreateThreadsInput,
  OrchestratorMcpCreateThreadsResult,
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDeleteScheduledTaskInput,
  OrchestratorMcpDeleteScheduledTaskResult,
  OrchestratorMcpRequestSecretInput,
  OrchestratorMcpRequestSecretResult,
  OrchestratorMcpFailure,
  OrchestratorMcpListScheduledTasksInput,
  OrchestratorMcpListScheduledTasksResult,
  OrchestratorMcpScheduleTaskInput,
  OrchestratorMcpScheduleTaskResult,
  OrchestratorMcpTaskCancelInput,
  OrchestratorMcpTaskOrRunGroupCancelResult,
  OrchestratorMcpTaskOrRunGroupResult,
  OrchestratorMcpUpdateScheduledTaskInput,
  OrchestratorMcpTaskStatusInput,
  OrchestratorMcpThreadInterruptInput,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadListInput,
  OrchestratorMcpThreadListResult,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadSendInput,
  OrchestratorMcpThreadSendResult,
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpThreadWaitResult,
  ThreadMetadataMcpUpdateInput,
  ThreadMetadataMcpUpdateResult,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import * as Schema from "effect/Schema";

import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as OrchestratorMcpService from "../../OrchestratorMcpService.ts";
import * as ThreadMetadataMcpService from "../../ThreadMetadataMcpService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  OrchestratorMcpService.OrchestratorMcpService,
];
/** Tools that also run in a linked environment, when `environmentId` names one. */
const forwardingDependencies = [...dependencies, PeerForwarding.PeerForwarding];
const threadMetadataDependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  ThreadMetadataMcpService.ThreadMetadataMcpService,
];

const OrchestratorCapabilitiesTool = Tool.make("orchestrator_capabilities", {
  description:
    "List the V2 provider instances and their current models from the same live catalog as the composer, including configured custom models, inherited runtime settings, and app-owned orchestration features available to this caller. For a separate top-level thread in a new or existing worktree, use t3_thread_launch with workspaceStrategy. Pass environmentId to list a linked environment's providers instead.",
  parameters: McpRouting.parameters(OrchestratorMcpCapabilitiesInput),
  success: OrchestratorMcpCapabilitiesResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "Get orchestration capabilities")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

export const DelegateTaskTool = Tool.make("delegate_task", {
  description:
    "Needs an agent running inside a T3 thread. Delegate one task to a T3-owned child agent/subagent of THIS thread and run it with only the supplied task prompt, without copying parent conversation history. Set target.environmentId to run on a linked environment, with target.projectId to select its project. The target Prism chooses provider, model and effort from the configured role kit and usage headroom. Omit target unless the user chose a provider/model explicitly; an explicit target wins. Optional lane chooses worker difficulty; prismRole chooses an explicit kit: planner, researcher, designer, worker, roast, reviewer or judge. Without prismRole, role review runs as reviewer, research as researcher, design as designer and everything else as worker. A read-only role (researcher, designer, roast, reviewer, judge) whose Run is above 1 in Prism settings starts that many of its Colors on this same task, sharing this thread's checkout, and returns { groupId, tasks }: a Run group. Its members do not wake this thread one by one; it wakes once after every member and its nested work is terminal, listing each member's status and result. task_status and task_cancel take the groupId or a member's taskId; mode='wait' waits for the whole group. An explicit target model, a bare delegation or a linked environment runs one Color, and a linked environment without target.model refuses a Run above 1. Use visible T3 delegation for same-provider and cross-provider work instead of native or shell agent runners. For every T3 delegated review round, call delegate_task again with the original brief, prior findings, responses, and unresolved objections in the task prompt. Track each round by its own taskId and use a distinct clientRequestId per round, stable across retries of that round. The childThreadId is backing storage, not the target for starting another delegated review round through t3_thread_send. Unconfigured roles inherit the parent model. Runtime mode and interaction mode retain the upstream ceiling. Prefer mode='async' for long work; mode='wait' blocks until completion, a blocking question, or timeout. If hasPendingQuestion on a remote task, inspect the qualified identities in remoteObservation.snapshot.requests and use remoteChild.environmentId/threadId; local tasks use threadId=childThreadId. Use t3_pending_request_list, t3_pending_request_read (with requestId) and t3_pending_request_respond, or ask the user; the task's completion still wakes this thread. timeoutMs on mode=wait is only the parent's wait budget and does not cancel the child. waitTimedOut on that wait call means the timeout fired; keep that taskId and read status on later task_status. An async child's completion wakes this thread through a notification, steered into active turns where supported or queued otherwise, so end the turn instead of polling or spawning watchers; use task_status only when the result is needed mid-turn.",
  parameters: McpRouting.parameters(OrchestratorMcpDelegateTaskInput),
  success: OrchestratorMcpTaskOrRunGroupResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Delegate a child task")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const TaskStatusTool = Tool.make("task_status", {
  description:
    "Needs an agent running inside a T3 thread. Read a T3-owned delegated task created by this parent thread. childRunId identifies the original delegated run. workState distinguishes working, waiting_for_children, and result_available; a completed turn with live nested work is not a completed task. summary is the final task result, including provider errors on failure, and remains stable after publication. hasPendingChildRuns reports later queued or executing turns in the backing child thread, even after the task is terminal; it does not reopen the task, and task_cancel stops those turns too. Turns held in a stopped queue wait for the user and do not count. latestTerminal* provides later non-monitor turn results. Reading a terminal result acknowledges its automatic parent delivery. Remote tasks keep local child/transfer lookup ids null: remoteChild and remoteObservation qualify the destination, original run, latest real result, freshness and pending request identities. Continue a remote job with remoteChild.environmentId and remoteChild.threadId while keeping this source taskId for status and cancellation. A Spectrum task from start_spectrum has no childRunId; its summary is the Spectrum report. A Run group's groupId returns { groupId, status, tasks } with every member.",
  parameters: McpRouting.parameters(OrchestratorMcpTaskStatusInput),
  success: OrchestratorMcpTaskOrRunGroupResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Get delegated task status")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const TaskCancelTool = Tool.make("task_cancel", {
  description:
    "Needs an agent running inside a T3 thread. Stop a T3-owned delegated task and dispose its automatic parent delivery. Its child thread stops like a user Stop: the running turn is interrupted, queued turns are held, pull request watches end, and the tasks it delegated stop too. This includes later child-thread runs, even after the task is terminal. A local terminal task returns its existing status, and published task results remain available. Remote jobs return cancel_requested with stopState: pending/unconfirmed is not proof of destination cancellation, acknowledged means Stop was accepted, confirmed means observed execution settled, and not_started means the launch was never sent. A missing legacy generation cannot authorize a replacement Stop target. On a Spectrum task it runs the Spectrum's Stop, which cancels every Color. A Run group's groupId stops every member and disposes the group's wake; a member's taskId stops that member, and the group's wake still lists it.",
  parameters: McpRouting.parameters(OrchestratorMcpTaskCancelInput),
  success: OrchestratorMcpTaskOrRunGroupCancelResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Cancel delegated task")
  .annotate(Tool.Destructive, true);

const WorkflowStopTool = Tool.make("t3_workflow_stop", {
  description:
    "Stop one running Claude workflow in a T3 thread. Use the workflow coordinator's subagentId returned by t3_thread_read with view='activity'. Stops the whole workflow, including its phases and agents. Does not stop the parent thread or other workflows. Individual workflow phases and agents cannot be stopped separately.",
  parameters: McpRouting.parameters(OrchestrationV2StopWorkflowInput),
  success: Schema.Struct({}),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Stop a workflow")
  .annotate(Tool.Destructive, true);

export const ScheduleTaskTool = Tool.make("schedule_task", {
  description:
    "Negotiated keyed calls return only a bounded historical confirmed-operation receipt with scheduledTaskId. It does not assert that the task still exists, is unchanged, or has executed. Use list_scheduled_tasks for its current cadence, nextRunAt and webhookUrl. Create persistent work in the app scheduler that runs even when no turn is active. Pass schedule as a STRUCTURED OBJECT, never JSON text. Timers: {type:'interval', everyMs:3600000} is hourly; {type:'fixed_time', timeOfDay:'09:00', weekdays:[1,2,3,4,5]} is weekday mornings; report the current nextRunAt from list_scheduled_tasks after acceptance. Webhooks: {type:'webhook'} runs once per request to a generated URL. The run sees the request ONLY through prompt placeholders: {{body.path}} (e.g. {{body.action}}, {{body.release.tag_name}}), {{headers.name}}, {{query.name}}, {{body}}, or {{request}} (method, headers with credentials redacted, and body). Each value arrives framed as untrusted request data, and $skill, @file or /command mentions inside it do not act; without a signature, anyone with the URL can trigger a run. For a sender that signs requests, first call request_secret so the user enters the secret privately (never ask for it in chat or invent one), then set signature with the returned secretRef, e.g. GitHub: {type:'webhook', signature:{header:'x-hub-signature-256', encoding:'hex', prefix:'sha256=', secretRef}}. Read the current webhookUrl through list_scheduled_tasks after acceptance; it is the public URL to give the user; if it is absent, this environment has no T3 Connect managed tunnel, so tell the user to enable T3 Connect remote access rather than sharing a path. Omit projectId for this thread's project. In this thread's project, a once run due within 55 minutes, an interval of 55 minutes or less, or a webhook posts into this thread by default. A once run more than 55 minutes away, a longer interval, fixed_time, or weekly starts a fresh thread, so write a self-contained prompt (Issue links, exact commands, what to report and where). Set bindToCurrentThread=true only when this thread will still be active, for example while you are coordinating live work; bindToCurrentThread=false forces a fresh thread. Elsewhere each run launches a fresh thread. Provider, model, and runtime settings inherit from this thread, or from the project default when there is no calling thread.",
  parameters: McpRouting.parameters(OrchestratorMcpScheduleTaskInput),
  success: OrchestratorMcpScheduleTaskResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Schedule a recurring task")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const ListScheduledTasksTool = Tool.make("list_scheduled_tasks", {
  parameters: McpRouting.parameters(OrchestratorMcpListScheduledTasksInput),
  description:
    "List recurring scheduled tasks in a project (omit projectId for the calling thread's project, or every project when there is no calling thread), including their id, schedule, prompt, enabled state, bound thread, next run time, and last run status. Use the returned scheduledTaskId with update_scheduled_task or delete_scheduled_task.",
  success: OrchestratorMcpListScheduledTasksResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "List scheduled tasks")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const UpdateScheduledTaskTool = Tool.make("update_scheduled_task", {
  description:
    "Negotiated keyed calls return only the historical confirmed-operation receipt; read list_scheduled_tasks for the current cadence, nextRunAt and webhookUrl. Update an existing scheduled task by scheduledTaskId (from list_scheduled_tasks). Only the provided fields change; omit a field to leave it as-is. Use enabled=false to pause a task without deleting it. Set bindToCurrentThread to move the task between posting into this thread and launching a fresh thread per run.",
  parameters: McpRouting.parameters(OrchestratorMcpUpdateScheduledTaskInput),
  success: OrchestratorMcpScheduleTaskResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Update a scheduled task")
  .annotate(Tool.Destructive, true);

const DeleteScheduledTaskTool = Tool.make("delete_scheduled_task", {
  description:
    "Negotiated keyed calls confirm only original deletion acceptance, not current absence after later recreation. Permanently delete a scheduled task by scheduledTaskId (from list_scheduled_tasks). The task stops running immediately. To keep it but stop runs, use update_scheduled_task with enabled=false instead.",
  parameters: McpRouting.parameters(OrchestratorMcpDeleteScheduledTaskInput),
  success: OrchestratorMcpDeleteScheduledTaskResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Delete a scheduled task")
  .annotate(Tool.Destructive, true);

const RequestSecretTool = Tool.make("request_secret", {
  description:
    "Ask the user for ANY secret (a token, API key, password, login, signing secret) through a private card in this thread, and wait for them to answer. Use fields to ask for several values on one card, e.g. an ID and a key. The value is never in the result or the transcript: when saved, files maps each field name ('secret' without fields) to a private file on this environment holding exactly that value. Use it without printing it or reading it into your reply, e.g. TOKEN=\"$(cat <path>)\" cmd, bao kv put <path> <field>=- < <path>, or rbw add (reads stdin), then delete the files with rm. Unused files expire after 24 hours. A single-field request also returns a secretRef for a tool that accepts one (schedule_task's signature.secretRef); it works once. Never ask for secrets in chat, and never invent one.",
  parameters: McpRouting.parameters(OrchestratorMcpRequestSecretInput),
  success: OrchestratorMcpRequestSecretResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Request a secret from the user")
  .annotate(Tool.Destructive, false);

export const CreateThreadsTool = Tool.make("create_threads", {
  description:
    "Needs an agent running inside a T3 thread. Create one or more ORDINARY TOP-LEVEL T3 conversations. This is not delegation and does not create child agents/subagents. For delegated work, choose models from orchestrator_capabilities. Prefer native subagents only when they support the chosen model; otherwise call delegate_task, including for same-provider work. Use create_threads for a batch of separate top-level threads sharing this checkout. Prefer t3_thread_launch for a single thread. Both require the user to request separate/new/top-level threads or conversations. Each entry may override provider, model, options, runtime mode, and interaction mode; omitted settings inherit. Project, branch, and worktree always inherit and cannot be overridden here. For independent implementation or a PR stack in its own worktree, use t3_thread_launch with workspaceStrategy instead of asking the agent to create a worktree in its prompt.",
  parameters: McpRouting.parameters(OrchestratorMcpCreateThreadsInput),
  success: OrchestratorMcpCreateThreadsResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Create T3 threads")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const ThreadListTool = Tool.make("t3_thread_list", {
  description:
    "List T3 threads in a project, newest first. Omit projectId for the calling thread's project. Pass environmentId to list a linked environment's threads; projectId is then required. Filter by durable run status, title, or settled state (settled=true lists threads the user or auto-settlement moved out of the active list), or snoozed state, and paginate with the returned cursor. A snoozed thread wakes early when it asks for something, fails, or completes. To link a thread for the user, write `[title](t3-thread://v1/<threadId>)` with the threadId exactly as returned, not URL-encoded; T3 Code shows the thread's current title.",
  parameters: McpRouting.parameters(OrchestratorMcpThreadListInput),
  success: OrchestratorMcpThreadListResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "List T3 threads")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const ThreadReadTool = Tool.make("t3_thread_read", {
  description:
    "Read durable state and a paginated timeline from any T3 thread in this environment, or in a linked one with environmentId. The default messages view returns user messages, assistant messages, and proposed plans; activity returns all summarized timeline items. Reading an untruncated terminal assistant result from this parent thread's direct app-owned child acknowledges that child's automatic completion delivery. Continue with afterPosition=nextPosition. Recover long item text with itemId and textOffset=nextTextOffset until nextTextOffset is null; offsets count UTF-16 code units. The thread also reports its snooze state. To link a thread for the user, write `[title](t3-thread://v1/<threadId>)` with the threadId exactly as returned, not URL-encoded; T3 Code shows the thread's current title.",
  parameters: McpRouting.parameters(OrchestratorMcpThreadReadInput),
  success: OrchestratorMcpThreadReadResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "Read a T3 thread")
  .annotate(Tool.Strict, true)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

export const ThreadUpdateTool = Tool.make("t3_thread_update", {
  description:
    "Update metadata for a thread. Omit threadId to update this thread. Use action='rename' with title, action='regenerate_title' with no extra field, action='link_pull_request' with pullRequest, or action='unlink_pull_request'. Workspace and branch changes are intentionally not supported. clientRequestId makes retries idempotent.",
  parameters: McpRouting.parameters(ThreadMetadataMcpUpdateInput),
  success: ThreadMetadataMcpUpdateResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: threadMetadataDependencies,
})
  .annotate(Tool.Title, "Update T3 thread metadata")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

const ThreadSendTool = Tool.make("t3_thread_send", {
  description:
    "Send a message to any T3 thread in this environment, or in a linked one with environmentId. The target cannot have broader permission modes than the caller. Do not use a delegated task's childThreadId to start another review round here; use delegate_task with the full review context and a new clientRequestId for that round. To continue the same delegated job after it stopped, send here to its childThreadId, or use remoteChild.environmentId and remoteChild.threadId for a remote task; the finished turn wakes the parent like the first result and appears in task_status latestTerminal*. Thread messages do not create a new delegated task or reopen a completed task. Spectrum transcript threads return runId=null, status=idle and delivery=transcript; they never start a provider on the transcript thread. For a descendant agent sending to an ordinary ancestor in the same environment, auto and queue deliver a child notice behind active work without steering; explicit steer and restart are refused, and Stopped ancestors stay stopped. For other sends, mode='auto' starts an idle thread; into a running turn it steers natively on Codex and OpenCode, queues on Claude, and restarts on restart-only harnesses such as Grok and Cursor. Use queue for a separate follow-up turn. steer follows these rules but requires a running turn. Use restart for an urgent course change that must interrupt running work: on a harness supporting it, T3 requests that harness's native Stop cancellation, including background work within its scope. Only after that succeeds does T3 deliver your message as the next prompt, naming you as the requester and explaining that stopped calls were interrupted rather than refused; a bare thread gets your message alone. If T3 cannot stop the work, the message is not delivered and the target's run shows why. Background cancellation on ACP and Antigravity agents depends on the agent, and a tool shown interrupted may not have exited yet. Separately delegated T3 tasks keep running; cancel those explicitly. restart needs a running turn (a thread only waiting on background work rejects it; use t3_thread_interrupt), rejects messages starting with '/' unless the target is bare, and cannot undo completed effects. delivery='restarted' acknowledges dispatch, not completed cancellation or delivery. clientRequestId makes retries idempotent.",
  parameters: McpRouting.parameters(OrchestratorMcpThreadSendInput),
  success: OrchestratorMcpThreadSendResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "Send to a T3 thread")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.OpenWorld, true);

const ThreadWaitTool = Tool.make("t3_thread_wait", {
  description:
    "Wait for a T3 thread run to reach a terminal durable state, here or in a linked environment with environmentId. Without runId, the latest run at call time is selected; an idle thread returns immediately. Timeout does not interrupt work, so call again or use t3_thread_read/list after timedOut=true. Waiting reports status only and does not acknowledge a delegated result.",
  parameters: McpRouting.parameters(OrchestratorMcpThreadWaitInput),
  success: OrchestratorMcpThreadWaitResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "Wait for a T3 thread")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const ThreadInterruptTool = Tool.make("t3_thread_interrupt", {
  description:
    "Request interruption of a running turn in any T3 thread in this environment, or in a linked one with environmentId. Without runId, the newest interruptible run is selected. Spectrum threads stop and drain their owned Drafters without a runId. Normal terminal runs and threads without an active turn return without another side effect. stop=true stops the thread, holds queued messages, ends pull request watches and stops its delegated tasks. clientRequestId makes retries idempotent.",
  parameters: McpRouting.parameters(OrchestratorMcpThreadInterruptInput),
  success: OrchestratorMcpThreadInterruptResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "Interrupt a T3 thread")
  .annotate(Tool.Destructive, true);

const EnvironmentLinksTool = Tool.make("t3_environment_links", {
  parameters: Schema.Struct(McpRouting.fields),
  description:
    "List the other T3 Code environments this one is linked to, such as the user's other machines, and whether each answers now. Pass one's environmentId to t3_project_list, t3_thread_launch, t3_thread_list, t3_thread_read, t3_thread_send, t3_thread_wait, t3_thread_interrupt, or orchestrator_capabilities to act there. A linked environment lets this one's agents do at most its access there, and never more than the calling agent may do here. An expired link has to be linked again from a new pairing code.",
  success: OrchestratorMcpEnvironmentLinksResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: forwardingDependencies,
})
  .annotate(Tool.Title, "List linked environments")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

export const OrchestratorToolkit = Toolkit.make(
  OrchestratorCapabilitiesTool,
  DelegateTaskTool,
  TaskStatusTool,
  TaskCancelTool,
  WorkflowStopTool,
  ScheduleTaskTool,
  ListScheduledTasksTool,
  UpdateScheduledTaskTool,
  DeleteScheduledTaskTool,
  RequestSecretTool,
  CreateThreadsTool,
  ThreadListTool,
  ThreadReadTool,
  ThreadUpdateTool,
  ThreadSendTool,
  ThreadWaitTool,
  ThreadInterruptTool,
  EnvironmentLinksTool,
);
