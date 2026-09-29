import { PrismLane, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { ALWAYS_LOAD_META } from "../../alwaysLoad.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [McpInvocationContext.McpInvocationContext];

export class PrismToolError extends Schema.TaggedError<PrismToolError>()("PrismToolError", {
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

const RequestId = TrimmedNonEmptyString.annotate({
  description: "The Prism job's request id, as prism_submit returned it.",
});

/** Who answers a Prism job's decision points; the router defaults to luna. */
export const PrismDispatcher = Schema.Literals(["luna", "planner"]);
export type PrismDispatcher = typeof PrismDispatcher.Type;

export const PrismSubmitInput = Schema.Struct({
  task: TrimmedNonEmptyString.annotate({
    description:
      "The whole task packet: outcome, Issue, acceptance criteria, non-goals, proof, and delivery (branch, one PR, no merge).",
  }),
  workspace: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Absolute path of the checkout the job works in. Defaults to this thread's worktree, else its project root.",
    }),
  ),
  lane: Schema.optional(
    PrismLane.annotate({
      description:
        "How difficult the job is: easy for mechanical work, hard for difficult work, medium (default) otherwise. Selects each role's model list for that lane.",
    }),
  ),
  dispatcher: Schema.optional(
    PrismDispatcher.annotate({
      description:
        "Who answers the job's decision points: luna (default) for a dispatcher child thread, or planner for this thread itself.",
    }),
  ),
  requestId: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description: "A unique job id. Generated when omitted.",
    }),
  ),
  handoffSummary: Schema.optional(TrimmedNonEmptyString),
});

/** The router's own JSON reply, passed through. */
export const PrismResult = Schema.Struct({
  requestId: Schema.String,
  router: Schema.Unknown,
});

const PrismSubmitTool = Tool.make("prism_submit", {
  description:
    "Use when an authorized job should end in one pushed branch and one PR with proof and review. Use it instead of coordinating workers yourself or starting agent CLIs in the shell. Prism (Model Router) picks models, falls back when capacity fails and recovers stalled work; this thread becomes the planner, woken only for judgment, and receives the final state: ready with the PR URL, or blocked, failed or cancelled with the reason. workspace defaults to this thread's current directory; set it for work in another repository or worktree. For one bounded task, use spawn_thread.",
  parameters: PrismSubmitInput,
  success: PrismResult,
  failure: PrismToolError,
  dependencies,
})
  .annotate(Tool.Title, "Submit Prism job")
  .annotate(Tool.Meta, ALWAYS_LOAD_META)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

const PrismStatusTool = Tool.make("prism_status", {
  description:
    "Read a Prism job's state: status, route, launches, open questions and recent events. Use it when the user asks how a job is going, or when a job blocks, loops or acts against Model Router's RUNNER.md; the final state arrives in this thread by itself. For such a defect, also read ~/.local/share/durable-runner/outputs/<requestId>/, then open or update a toolboxmd/model-router Issue with the request id, that evidence and the expected behavior; do not silently work around it. Fix your own packet mistakes, such as a missing proof or workspace, in the packet; they are not router defects.",
  parameters: Schema.Struct({ requestId: RequestId }),
  success: PrismResult,
  failure: PrismToolError,
  dependencies,
})
  .annotate(Tool.Title, "Prism job status")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const PrismQuestionsTool = Tool.make("prism_questions", {
  description:
    "List a Prism job's questions for its planner: pending ones by default, all with includeAnswered. Answer each with prism_answer.",
  parameters: Schema.Struct({
    requestId: RequestId,
    includeAnswered: Schema.optional(Schema.Boolean),
  }),
  success: PrismResult,
  failure: PrismToolError,
  dependencies,
})
  .annotate(Tool.Title, "Prism job questions")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const PrismAnswerTool = Tool.make("prism_answer", {
  description:
    "Answer one of a Prism job's pending questions; the dispatcher continues with the answer. Decide within your authority; take Human Gates to the user first.",
  parameters: Schema.Struct({
    requestId: RequestId,
    qid: TrimmedNonEmptyString.annotate({ description: "The question id from prism_questions." }),
    answer: TrimmedNonEmptyString,
  }),
  success: PrismResult,
  failure: PrismToolError,
  dependencies,
})
  .annotate(Tool.Title, "Answer Prism question")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

const PrismCancelTool = Tool.make("prism_cancel", {
  description:
    "Cancel a Prism job this thread started: Prism records the cancellation, stops the job's workers and child threads, and returns the final state (cancelled, or still cancelling when something could not be stopped yet; call again to retry). Use it only when the user asks to stop the job or the job is clearly stuck.",
  parameters: Schema.Struct({ requestId: RequestId }),
  success: PrismResult,
  failure: PrismToolError,
  dependencies,
})
  .annotate(Tool.Title, "Cancel Prism job")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const PrismToolkit = Toolkit.make(
  PrismSubmitTool,
  PrismStatusTool,
  PrismQuestionsTool,
  PrismAnswerTool,
  PrismCancelTool,
);
