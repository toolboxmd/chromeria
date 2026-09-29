import {
  PrismLane,
  PrismRole,
  PrismRoleName,
  RuntimeMode,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { ALWAYS_LOAD_META } from "../../alwaysLoad.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [McpInvocationContext.McpInvocationContext];

export class ThreadsToolError extends Schema.TaggedError<ThreadsToolError>()("ThreadsToolError", {
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

export const SubagentStatus = Schema.Literals(["starting", "running", "idle", "failed", "stopped"]);
export type SubagentStatus = typeof SubagentStatus.Type;

export const ThreadScope = Schema.Literals(["children", "project"]);
export type ThreadScope = typeof ThreadScope.Type;

const threadScope = ThreadScope.pipe(
  Schema.withDecodingDefault(Effect.succeed("children" as const)),
);
const includeSettled = Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false)));

export const SpawnThreadInput = Schema.Struct({
  task: TrimmedNonEmptyString.annotate({
    description: "The first message the child thread receives: its whole task.",
  }),
  role: Schema.optional(
    PrismRoleName.annotate({
      description:
        "Prism role for the child: dispatcher, reviewer, worker, retry, escalation (or planner); correction and recovery are the old names of retry and escalation. Applies that role's kit from Prism settings: its instructions, skills, permissions, thread-tool scope, and the first eligible model of its model list unless model is named.",
    }),
  ),
  lane: Schema.optional(
    PrismLane.annotate({
      description:
        "With role worker: which of its model lists to use, easy, medium (default) or hard, by how difficult the task is. Other roles have one list. Later entries in a list are fallbacks.",
    }),
  ),
  instanceId: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Provider instance to run the child on, for example claudeAgent, codex, opencode or grok. Defaults to the role's preferred model, else this thread's provider instance.",
    }),
  ),
  model: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Model id on that instance. Defaults to the first eligible model of the role's list, else this thread's model.",
    }),
  ),
  effort: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Reasoning effort (Claude effort, Codex/Grok reasoningEffort, OpenCode variant).",
    }),
  ),
  title: Schema.optional(TrimmedNonEmptyString),
  runtimeMode: Schema.optional(RuntimeMode),
  reportBack: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "When true (default), each time the child finishes a turn its final reply is sent to this thread as a message.",
    }),
  ),
});

export const SpawnThreadResult = Schema.Struct({
  threadId: Schema.String,
  role: Schema.optional(PrismRole),
  lane: Schema.optional(PrismLane),
  parentThreadId: Schema.String,
  instanceId: Schema.String,
  model: Schema.String,
});

export const MessageThreadInput = Schema.Struct({
  threadId: TrimmedNonEmptyString.annotate({ description: "A thread in the selected scope." }),
  text: TrimmedNonEmptyString,
  scope: threadScope,
});

export const MessageThreadResult = Schema.Struct({
  threadId: Schema.String,
  statusBefore: SubagentStatus,
  delivery: Schema.Literals(["new-turn", "steer"]).annotate({
    description:
      "new-turn: the child was idle and starts a turn. steer: the message joins the child's running turn.",
  }),
});

export const ThreadSummary = Schema.Struct({
  threadId: Schema.String,
  id: Schema.String,
  title: Schema.String,
  status: SubagentStatus,
  instanceId: Schema.NullOr(Schema.String),
  provider: Schema.NullOr(Schema.String),
  model: Schema.String,
  parentId: Schema.NullOr(Schema.String),
  lastError: Schema.NullOr(Schema.String),
  lastAssistantMessage: Schema.NullOr(Schema.String),
  userMessageCount: Schema.Int,
});

const SpawnThreadTool = Tool.make("spawn_thread", {
  description:
    "Use when another agent should do one bounded task, such as a review, a second opinion, a check or a small fix, or when a specific model and effort is wanted. Use it instead of starting codex exec, claude -p, opencode run or grok in the shell: the user cannot see those runs, while a child thread shows in this thread's Agents panel. Pass role (reviewer, worker) to apply that Prism role's kit and model, or name instance, model and effort. For a job that should end in one PR, use prism_submit. Follow up with read_thread and message_thread.",
  parameters: SpawnThreadInput,
  success: SpawnThreadResult,
  failure: ThreadsToolError,
  dependencies,
})
  .annotate(Tool.Title, "Spawn child thread")
  .annotate(Tool.Meta, ALWAYS_LOAD_META)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const MessageThreadTool = Tool.make("message_thread", {
  description:
    "Send a message to a child thread by default, or any thread in this project with scope: project, once it has started working or gone idle. A thread that is still starting refuses with a retryable error; retry in a few seconds.",
  parameters: MessageThreadInput,
  success: MessageThreadResult,
  failure: ThreadsToolError,
  dependencies,
})
  .annotate(Tool.Title, "Message child thread")
  .annotate(Tool.Meta, ALWAYS_LOAD_META)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const ReadThreadTool = Tool.make("read_thread", {
  description:
    "Read a thread's status and its latest assistant reply. The default scope is this thread's children; use scope: project for any thread in this project, including settled threads.",
  parameters: Schema.Struct({ threadId: TrimmedNonEmptyString, scope: threadScope }),
  success: ThreadSummary,
  failure: ThreadsToolError,
  dependencies,
})
  .annotate(Tool.Title, "Read child thread")
  .annotate(Tool.Meta, ALWAYS_LOAD_META)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ListChildThreadsTool = Tool.make("list_child_threads", {
  description: "List this thread's child threads and their status.",
  success: Schema.Struct({ threads: Schema.Array(ThreadSummary) }),
  failure: ThreadsToolError,
  dependencies,
})
  .annotate(Tool.Title, "List child threads")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ListThreadsTool = Tool.make("list_threads", {
  description:
    "List active threads in this thread's children by default, or all non-archived threads in this project with scope: project. Set includeSettled: true to include settled threads.",
  parameters: Schema.Struct({ scope: threadScope, includeSettled }),
  success: Schema.Struct({ threads: Schema.Array(ThreadSummary) }),
  failure: ThreadsToolError,
  dependencies,
})
  .annotate(Tool.Title, "List threads")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const ThreadsToolkit = Toolkit.make(
  SpawnThreadTool,
  MessageThreadTool,
  ReadThreadTool,
  ListChildThreadsTool,
  ListThreadsTool,
);
