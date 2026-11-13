import * as Context from "effect/Context";
import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";

/** Server-owned command admission; provide only around one dispatch effect. Never persisted. */
export class ForkDispatchPlans extends Context.Reference<ReadonlyArray<ForkCommitPlan>>(
  "t3/fork/ForkDispatchPlans/ForkDispatchPlans",
  { defaultValue: () => [] },
) {}
