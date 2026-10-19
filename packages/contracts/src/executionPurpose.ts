import * as Schema from "effect/Schema";

import {
  EnvironmentId,
  IsoDateTime,
  NodeId,
  NonNegativeInt,
  PositiveInt,
  RunId,
  ScheduledTaskId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { parseIssueUrl } from "./issueLinks.ts";

/** An outcome's identity. Navigation links and delivery PRs do not change it. */
export const TrackingReference = Schema.Struct({
  host: TrimmedNonEmptyString.check(
    Schema.isPattern(/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?$/iu),
  ),
  repository: TrimmedNonEmptyString.check(Schema.isPattern(/^[a-z0-9_.-]+\/[a-z0-9_.-]+$/iu)),
  kind: Schema.Literals(["issue", "pr"]),
  number: PositiveInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
});
export type TrackingReference = typeof TrackingReference.Type;
const isTrackingReference = Schema.is(TrackingReference);

/** A delivery declaration is separate from navigation and execution ownership. */
export const PurposeDeliveryIntent = Schema.Struct({
  version: Schema.Literal(1),
  purpose: TrackingReference,
});
export type PurposeDeliveryIntent = typeof PurposeDeliveryIntent.Type;

export const ExecutionPurposeDelivery = Schema.Struct({
  ...PurposeDeliveryIntent.fields,
  pullRequest: Schema.Struct({ ...TrackingReference.fields, kind: Schema.Literal("pr") }),
  declaredAt: IsoDateTime,
});
export type ExecutionPurposeDelivery = typeof ExecutionPurposeDelivery.Type;

export const ExecutionPurposeScope = Schema.Union([
  Schema.Struct({ type: Schema.Literal("single"), reference: TrackingReference }),
  Schema.Struct({
    type: Schema.Literal("unscoped"),
    reason: Schema.Literals(["deliberate", "legacy_no_intent", "mixed", "unknown_cause"]),
  }),
]);
export type ExecutionPurposeScope = typeof ExecutionPurposeScope.Type;

export const ExecutionPurposeProvenance = Schema.Union([
  Schema.Struct({ type: Schema.Literal("declared") }),
  Schema.Struct({ type: Schema.Literal("selection"), revision: NonNegativeInt }),
  Schema.Struct({ type: Schema.Literal("legacy") }),
  Schema.Struct({
    type: Schema.Literals(["parent_run", "target_run", "source_run"]),
    threadId: ThreadId,
    runId: RunId,
  }),
  Schema.Struct({
    type: Schema.Literal("delegated_launch"),
    threadId: ThreadId,
    runId: RunId,
    taskId: NodeId,
  }),
  Schema.Struct({
    type: Schema.Literal("remote_launch"),
    environmentId: EnvironmentId,
    threadId: ThreadId,
    runId: RunId,
    launchId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    type: Schema.Literals(["native_operation", "delegated_completion"]),
    operationIds: Schema.Array(TrimmedNonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("schedule"), scheduledTaskId: ScheduledTaskId }),
  Schema.Struct({
    type: Schema.Literal("pull_request_watch"),
    reference: TrackingReference,
    startedAt: TrimmedNonEmptyString,
  }),
]);

/** Resolved before app execution and immutable for the lifetime of a run. */
export const ExecutionPurpose = Schema.Struct({
  version: Schema.Literal(1),
  scope: ExecutionPurposeScope,
  provenance: ExecutionPurposeProvenance,
});
export type ExecutionPurpose = typeof ExecutionPurpose.Type;
const encodeExecutionPurpose = Schema.encodeSync(Schema.fromJsonString(ExecutionPurpose));

export function executionPurposesEqual(
  left: ExecutionPurpose | undefined,
  right: ExecutionPurpose | undefined,
): boolean {
  return left === undefined || right === undefined
    ? left === right
    : encodeExecutionPurpose(left) === encodeExecutionPurpose(right);
}

export const PurposeSelection = Schema.Struct({
  revision: NonNegativeInt,
  scope: ExecutionPurposeScope,
});
export type PurposeSelection = typeof PurposeSelection.Type;

/** Absence of this envelope is the legacy protocol, regardless of the served tool schema. */
export const PurposeIntent = Schema.Struct({
  version: Schema.Literal(1),
  intent: Schema.Union([
    Schema.Struct({
      type: Schema.Literal("reference"),
      reference: TrackingReference,
      expectedRevision: Schema.optional(Schema.Never),
    }),
    Schema.Struct({
      type: Schema.Literal("selection"),
      expectedRevision: NonNegativeInt,
      reference: Schema.optional(Schema.Never),
    }),
    Schema.Struct({
      type: Schema.Literal("inherit"),
      reference: Schema.optional(Schema.Never),
      expectedRevision: Schema.optional(Schema.Never),
    }),
    Schema.Struct({
      type: Schema.Literal("unscoped"),
      reference: Schema.optional(Schema.Never),
      expectedRevision: Schema.optional(Schema.Never),
    }),
    Schema.Struct({
      type: Schema.Literal("target"),
      reference: Schema.optional(Schema.Never),
      expectedRevision: Schema.optional(Schema.Never),
    }),
  ]),
}).annotate({
  description:
    "Version 1 execution purpose. Choose reference, inherit the exact caller run, selection with its revision, deliberate unscoped, or target for Steer/Restart. Omission is legacy unscoped with a warning.",
});
export type PurposeIntent = typeof PurposeIntent.Type;
const encodePurposeIntent = Schema.encodeSync(Schema.fromJsonString(PurposeIntent));

export function purposeIntentsEqual(
  left: PurposeIntent | undefined,
  right: PurposeIntent | undefined,
): boolean {
  return left === undefined || right === undefined
    ? left === right
    : encodePurposeIntent(left) === encodePurposeIntent(right);
}

/** A saved trigger chooses a stable reference or deliberately unscoped work. */
export const SavedPurposeIntent = Schema.Struct({
  version: Schema.Literal(1),
  intent: Schema.Union([
    Schema.Struct({
      type: Schema.Literal("reference"),
      reference: TrackingReference,
      expectedRevision: Schema.optional(Schema.Never),
    }),
    Schema.Struct({
      type: Schema.Literal("unscoped"),
      reference: Schema.optional(Schema.Never),
      expectedRevision: Schema.optional(Schema.Never),
    }),
  ]),
});
export type SavedPurposeIntent = typeof SavedPurposeIntent.Type;

export function savedPurposeIntentForScope(scope: ExecutionPurposeScope): SavedPurposeIntent {
  return {
    version: 1,
    intent:
      scope.type === "single"
        ? { type: "reference", reference: normalizeTrackingReference(scope.reference) }
        : { type: "unscoped" },
  };
}

export function purposeScopeOfSavedIntent(
  intent: SavedPurposeIntent | undefined,
): ExecutionPurposeScope {
  return intent === undefined
    ? { type: "unscoped", reason: "legacy_no_intent" }
    : intent.intent.type === "reference"
      ? { type: "single", reference: normalizeTrackingReference(intent.intent.reference) }
      : { type: "unscoped", reason: "deliberate" };
}

/** Origin-generated evidence, checked against the original destination launch and link. */
export const RemotePurposeContinuation = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  runId: RunId,
  launchId: TrimmedNonEmptyString,
  purpose: ExecutionPurpose,
});
export type RemotePurposeContinuation = typeof RemotePurposeContinuation.Type;

export const LEGACY_EXECUTION_PURPOSE: ExecutionPurpose = {
  version: 1,
  scope: { type: "unscoped", reason: "legacy_no_intent" },
  provenance: { type: "legacy" },
};

export function executionPurposeOf(value: {
  readonly purpose?: ExecutionPurpose | undefined;
}): ExecutionPurpose {
  return value.purpose ?? LEGACY_EXECUTION_PURPOSE;
}

export function purposeSelectionOf(value: {
  readonly purposeSelection?: PurposeSelection | undefined;
}): PurposeSelection {
  return (
    value.purposeSelection ?? { revision: 0, scope: { type: "unscoped", reason: "deliberate" } }
  );
}

export function normalizeTrackingReference(reference: TrackingReference): TrackingReference {
  return {
    host: reference.host.toLowerCase(),
    repository: reference.repository.toLowerCase(),
    kind: reference.kind,
    number: reference.number,
  };
}

export function purposeScopesEqual(
  left: ExecutionPurposeScope,
  right: ExecutionPurposeScope,
): boolean {
  if (left.type === "unscoped" || right.type === "unscoped") {
    return (
      left.type === right.type &&
      left.type === "unscoped" &&
      right.type === "unscoped" &&
      left.reason === right.reason
    );
  }
  return (
    left.reference.kind === right.reference.kind &&
    left.reference.number === right.reference.number &&
    left.reference.host.toLowerCase() === right.reference.host.toLowerCase() &&
    left.reference.repository.toLowerCase() === right.reference.repository.toLowerCase()
  );
}

export function trackingReferenceUrl(reference: TrackingReference): string {
  return `https://${reference.host}/${reference.repository}/${reference.kind === "issue" ? "issues" : "pull"}/${reference.number}`;
}

export function parseTrackingReferenceUrl(url: string): TrackingReference | null {
  const issue = parseIssueUrl(url);
  if (issue !== null) {
    const candidate = { ...issue, kind: "issue" };
    return isTrackingReference(candidate) ? normalizeTrackingReference(candidate) : null;
  }
  try {
    const parsed = new URL(url.trim());
    if (!/^\/[^/]+\/[^/]+\/pull\/[0-9]+\/?$/u.test(parsed.pathname)) return null;
    parsed.pathname = parsed.pathname.replace("/pull/", "/issues/");
    const pr = parseIssueUrl(parsed.href);
    const candidate = pr === null ? null : { ...pr, kind: "pr" };
    return isTrackingReference(candidate) ? normalizeTrackingReference(candidate) : null;
  } catch {
    return null;
  }
}

export function purposeScopeLabel(scope: ExecutionPurposeScope): string {
  return scope.type === "single"
    ? `${scope.reference.repository} ${scope.reference.kind === "issue" ? "Issue" : "PR"} #${scope.reference.number}`
    : scope.reason === "legacy_no_intent"
      ? "Unscoped (legacy caller)"
      : scope.reason === "mixed"
        ? "Unscoped (mixed causes)"
        : scope.reason === "unknown_cause"
          ? "Unscoped (unknown cause)"
          : "Unscoped";
}

export function executionPurposeWarnings(purpose: ExecutionPurpose): ReadonlyArray<string> {
  return purpose.scope.type === "unscoped" && purpose.scope.reason === "legacy_no_intent"
    ? [
        "Legacy caller omitted purpose intent. This execution is unscoped and cannot establish exact outcome accounting.",
      ]
    : [];
}
