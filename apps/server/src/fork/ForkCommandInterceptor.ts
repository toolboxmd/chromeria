import { ThreadId, type OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Projection from "../orchestration-v2/ProjectionStore.ts";
import type { EventSinkV2Shape } from "../orchestration-v2/EventSink.ts";

type Commit = Parameters<EventSinkV2Shape["commitCommand"]>[0];
export type ForkCommandPlan = Pick<
  Commit,
  "events" | "effects" | "forkPlans" | "cancelUnsettledEffects"
>;
export class ForkCommandPlanError extends Schema.TaggedError<ForkCommandPlanError>()(
  "ForkCommandPlanError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}
/** Plans only. The common serialized dispatch owns admission, commit, receipts and effects. */
export class ForkCommandInterceptor extends Context.Reference<{
  readonly plan: (
    command: OrchestrationV2ServerCommand,
  ) => Effect.Effect<
    ForkCommandPlan | null,
    ForkCommandPlanError,
    SqlClient.SqlClient | Projection.ProjectionStoreV2
  >;
}>("t3/fork/ForkCommandInterceptor", {
  defaultValue: () => ({ plan: () => Effect.succeed(null) }),
}) {}
