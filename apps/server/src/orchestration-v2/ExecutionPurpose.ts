import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import {
  type ExecutionPurpose,
  type ExecutionPurposeScope,
  type OrchestrationV2RuntimeRequest,
  type ThreadId,
  LEGACY_EXECUTION_PURPOSE,
  executionPurposeOf,
  normalizeTrackingReference,
  type PurposeIntent,
  PurposeSelection,
  purposeScopeLabel,
  purposeScopesEqual,
  purposeSelectionOf,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class PurposeAdmissionError extends Schema.TaggedError<PurposeAdmissionError>()(
  "PurposeAdmissionError",
  {
    currentSelection: Schema.optional(PurposeSelection),
    reason: Schema.Literals([
      "selection_changed",
      "inherit_source_missing",
      "target_missing",
      "target_mismatch",
      "delegated_launch_missing",
      "remote_launch_mismatch",
      "intent_reused",
    ]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "selection_changed":
        return this.currentSelection === undefined
          ? "The purpose selection changed. Refresh it before sending."
          : `The purpose selection changed to ${purposeScopeLabel(this.currentSelection.scope)} (revision ${this.currentSelection.revision}). Refresh it before sending.`;
      case "inherit_source_missing":
        return "Purpose inheritance requires the exact owning execution.";
      case "target_missing":
        return "There is no active run to steer or restart.";
      case "target_mismatch":
        return "The asserted purpose differs from the target run's immutable purpose.";
      case "delegated_launch_missing":
        return "The original delegated launch has no validated purpose evidence.";
      case "remote_launch_mismatch":
        return "The continuation does not match the original remote purpose launch.";
      case "intent_reused":
        return "This command id was already admitted with a different purpose intent.";
    }
  }
}

export function normalizePurposeScope(scope: ExecutionPurposeScope): ExecutionPurposeScope {
  return scope.type === "single"
    ? { type: "single", reference: normalizeTrackingReference(scope.reference) }
    : scope;
}

/** Resolve caller intent before capability-based delivery can join an active run. */
export const resolvePurposeIntent = Effect.fnUntraced(function* (input: {
  readonly intent?: PurposeIntent | undefined;
  readonly selection?: PurposeSelection | undefined;
  readonly inherited?: ExecutionPurpose | undefined;
  readonly target?: ExecutionPurpose | undefined;
}) {
  const choice = input.intent?.intent;
  let purpose: ExecutionPurpose;
  let select: ExecutionPurposeScope | undefined;
  switch (choice?.type) {
    case undefined:
      purpose = LEGACY_EXECUTION_PURPOSE;
      break;
    case "reference":
      select = { type: "single", reference: normalizeTrackingReference(choice.reference) };
      purpose = { version: 1, scope: select, provenance: { type: "declared" } };
      break;
    case "unscoped":
      select = { type: "unscoped", reason: "deliberate" };
      purpose = { version: 1, scope: select, provenance: { type: "declared" } };
      break;
    case "selection": {
      const selection = purposeSelectionOf({ purposeSelection: input.selection });
      if (selection.revision !== choice.expectedRevision) {
        return yield* new PurposeAdmissionError({
          reason: "selection_changed",
          currentSelection: selection,
        });
      }
      purpose = {
        version: 1,
        scope: selection.scope,
        provenance: { type: "selection", revision: selection.revision },
      };
      break;
    }
    case "inherit":
      if (input.inherited === undefined) {
        return yield* new PurposeAdmissionError({ reason: "inherit_source_missing" });
      }
      purpose = input.inherited;
      break;
    case "target":
      if (input.target === undefined) {
        return yield* new PurposeAdmissionError({ reason: "target_missing" });
      }
      purpose = input.target;
      break;
  }
  if (input.target !== undefined) {
    if (choice !== undefined && !purposeScopesEqual(input.target.scope, purpose.scope)) {
      return yield* new PurposeAdmissionError({ reason: "target_mismatch" });
    }
    purpose = input.target;
    select = undefined;
  }
  return { purpose, select };
});

/** Validated system causes keep one owner only when every cause agrees. */
export function purposeFromCauses(
  causes: ReadonlyArray<ExecutionPurpose | undefined>,
  operationIds: ReadonlyArray<string>,
  type: "native_operation" | "delegated_completion" = "native_operation",
): ExecutionPurpose {
  let scope: ExecutionPurposeScope = { type: "unscoped", reason: "unknown_cause" };
  const first = causes[0];
  if (first !== undefined && causes.length > 0) {
    scope = first.scope;
    for (const cause of causes) {
      if (cause === undefined) {
        scope = { type: "unscoped", reason: "unknown_cause" };
        break;
      }
      if (!purposeScopesEqual(first.scope, cause.scope))
        scope = { type: "unscoped", reason: "mixed" };
    }
  }
  return {
    version: 1,
    scope,
    provenance: { type, operationIds },
  };
}

/** A question wake follows its exact request node, including human follow-ups in a delegated job. */
export const purposeForRuntimeRequest = Effect.fnUntraced(function* (
  threadId: ThreadId,
  request: OrchestrationV2RuntimeRequest,
) {
  const projections = yield* ProjectionStoreV2;
  const records = yield* projections.getThreadRecords(threadId, ["nodes", "runs"]);
  const node = records.nodes.find((node) => node.id === request.nodeId);
  const run = records.runs.find((run) => run.id === node?.runId);
  return run === undefined
    ? purposeFromCauses([undefined], [request.id])
    : {
        version: 1 as const,
        scope: executionPurposeOf(run).scope,
        provenance: { type: "source_run" as const, threadId, runId: run.id },
      };
});
