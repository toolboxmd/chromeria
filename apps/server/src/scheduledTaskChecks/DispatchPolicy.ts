import type { ScheduledTask } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

/**
 * What upstream's `runTask` does with one fire, decided before the task is
 * marked running (toolboxmd/chromeria#174).
 * - `upstream`: dispatch exactly as upstream would.
 * - `skip`: a scheduled fire while the task's run is unfinished; the policy
 *   already consumed the slot, so upstream records no run.
 * - `fork`: the fork already recorded its run and dispatches it.
 */
export type ScheduledTaskDispatchDecision =
  | { readonly _tag: "upstream" }
  | { readonly _tag: "skip" }
  | { readonly _tag: "fork"; readonly dispatch: Effect.Effect<void, ScheduledTaskDispatchFailed> };

export class ScheduledTaskDispatchRefused extends Data.TaggedError("ScheduledTaskDispatchRefused")<{
  readonly message: string;
}> {}

export class ScheduledTaskDispatchFailed extends Data.TaggedError("ScheduledTaskDispatchFailed")<{
  readonly message: string;
}> {}

export interface ScheduledTaskDispatchPolicyShape {
  readonly decide: (input: {
    readonly task: ScheduledTask;
    readonly trigger: "scheduled" | "manual" | "webhook";
    readonly startedAt: DateTime.DateTime;
  }) => Effect.Effect<ScheduledTaskDispatchDecision, ScheduledTaskDispatchRefused>;
}

const upstreamDecision: ScheduledTaskDispatchDecision = { _tag: "upstream" };

export class ScheduledTaskDispatchPolicy extends Context.Reference<ScheduledTaskDispatchPolicyShape>(
  "t3/scheduledTaskChecks/ScheduledTaskDispatchPolicy",
  { defaultValue: () => ({ decide: () => Effect.succeed(upstreamDecision) }) },
) {}

/**
 * Upstream's launch or send for one fire, or the fork's own dispatch when the
 * policy took the fire; wraps the upstream call in place so its arguments and
 * fallback stay upstream's.
 */
export const dispatchVia =
  <I, A, E>(decision: ScheduledTaskDispatchDecision, upstream: (input: I) => Effect.Effect<A, E>) =>
  (input: I): Effect.Effect<A | void, E | ScheduledTaskDispatchFailed> =>
    decision._tag === "fork" ? decision.dispatch : upstream(input);
