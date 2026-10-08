import type { OrchestrationV2DomainEvent, OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Effect from "effect/Effect";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as History from "./RecoveryHistory.ts";
import { sameModelSelection } from "./recoveryPolicy.ts";

/** Binds a source to the actual planned run, in the same event/receipt transaction. */
export function continuationAdmission(
  command: OrchestrationV2ServerCommand,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
): ForkCommitPlan | null {
  if (command.type !== "message.dispatch") return null;
  const sourceRunId = command.forkPrismRetryOfRunId ?? command.usageLimitContinuationOfRunId;
  if (sourceRunId === undefined) return null;
  const creations = events.filter((event) => event.type === "run.created");
  // Upstream may accept a stale reset command with metadata only. It did not create a continuation.
  if (creations.length === 0) return null;
  const successor = creations[0]?.payload;
  const reject = (kind: "invalid_transition" | "storage_failure") =>
    new ForkCommitGuardRejected({ threadId: command.threadId, kind });
  const guard = Effect.gen(function* () {
    if (successor === undefined) return yield* reject("invalid_transition");
    const sql = yield* SqlClient.SqlClient;
    const existing =
      yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id=${successor.id}`;
    if (existing.length > 0) return yield* reject("invalid_transition");
    const projections = yield* Projection.ProjectionStoreV2;
    const records = yield* projections.getThreadRecords(command.threadId, ["runs"], {
      runIds: [sourceRunId],
    });
    const source = records.runs[0];
    const previous = yield* History.readRecoveryState(sourceRunId);
    if (
      creations.length !== 1 ||
      successor === undefined ||
      source === undefined ||
      command.creationSource !== "server" ||
      (command.forkPrismRetryOfRunId !== undefined && command.createdBy !== "system") ||
      (command.forkPrismRetryOfRunId !== undefined &&
        command.usageLimitContinuationOfRunId !== undefined &&
        command.forkPrismRetryOfRunId !== command.usageLimitContinuationOfRunId) ||
      successor.threadId !== command.threadId ||
      source.threadId !== command.threadId ||
      source.status !== "failed" ||
      successor.id === sourceRunId ||
      successor.forkPrismContinuationSourceRunId !== sourceRunId ||
      !sameModelSelection(source.modelSelection, successor.modelSelection) ||
      (previous.decision?.status === "decided" &&
        previous.decision.outcome === "retried" &&
        previous.decision.successorRunId !== successor.id) ||
      previous.admittedContinuations.some((link) => link.successorRunId !== successor.id)
    )
      return yield* reject("invalid_transition");
  }).pipe(
    Effect.mapError((error) =>
      error._tag === "ForkCommitGuardRejected" ? error : reject("storage_failure"),
    ),
  );
  const mutation = Effect.gen(function* () {
    if (successor === undefined) return yield* reject("invalid_transition");
    // The run/event itself is the admission link. A prior conclusive decision remains immutable.
    yield* History.writeRecoveryOutcome({
      sourceRunId,
      threadId: command.threadId,
      status: "decided",
      outcome: "retried",
      successorRunId: successor.id,
    });
  }).pipe(
    Effect.mapError((error) =>
      error._tag === "ForkCommitGuardRejected" ? error : reject("storage_failure"),
    ),
  );
  return { guards: [guard], mutations: [mutation] };
}
