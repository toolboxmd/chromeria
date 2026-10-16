import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";

import { EnvironmentAuthorizationError } from "./auth.ts";
import { IsoDateTime, PositiveInt, ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { IssueOperationError, IssueState } from "./issues.ts";

/**
 * Fork: a project's feature map (toolboxmd/chromeria#235). The structure is `docs/map.md` on the
 * project's default branch, one Mermaid flowchart whose `%% ID: refs` comment lines bind each part
 * to its Issues and pull requests; the facts are those refs as GitHub answers them now.
 */

export const FeatureMapReadInput = Schema.Struct({ projectId: ProjectId });
export type FeatureMapReadInput = typeof FeatureMapReadInput.Type;

/** An Issue or pull request a part names, on the project's GitHub host. */
export const FeatureMapRef = Schema.Struct({
  kind: Schema.Literals(["issue", "pull"]),
  repository: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
});
export type FeatureMapRef = typeof FeatureMapRef.Type;

/** A declared part: tracked by its primary ref, asserted live, or other-project context. */
export const FeatureMapPart = Schema.Struct({
  id: TrimmedNonEmptyString,
  name: Schema.String,
  context: Schema.Boolean,
  /** An existing capability asserted by the map author; absent in older snapshots. */
  live: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(false))),
  /** The first token of its ref line could not be read, so no ref may stand in as primary. */
  primaryUnreadable: Schema.Boolean,
  refs: Schema.Array(FeatureMapRef),
});
export type FeatureMapPart = typeof FeatureMapPart.Type;

/** A ref line that names no declared part, or a ref token the parser could not read. */
export const FeatureMapRefLineProblem = Schema.Struct({
  id: TrimmedNonEmptyString,
  text: Schema.String,
});
export type FeatureMapRefLineProblem = typeof FeatureMapRefLineProblem.Type;

/** A head commit's combined check result; null where the head has no checks. */
export const FeatureMapChecks = Schema.NullOr(Schema.Literals(["passing", "failing", "pending"]));
export type FeatureMapChecks = typeof FeatureMapChecks.Type;

export const FeatureMapPullRequestFact = Schema.Struct({
  kind: Schema.Literal("pull"),
  url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "merged", "closed"]),
  isDraft: Schema.Boolean,
  /** Merged into its base repository's default branch, not into another branch. */
  mergedIntoDefault: Schema.Boolean,
  checks: FeatureMapChecks,
});
export type FeatureMapPullRequestFact = typeof FeatureMapPullRequestFact.Type;

export const FeatureMapIssueFact = Schema.Struct({
  kind: Schema.Literal("issue"),
  url: TrimmedNonEmptyString,
  state: IssueState,
  labels: Schema.Array(Schema.String),
  /** Every label and closing pull request was read; false when GitHub held more than one page. */
  complete: Schema.Boolean,
  /** Open pull requests that close it, with their head checks. */
  openClosingPullRequests: Schema.Array(
    Schema.Struct({
      repository: TrimmedNonEmptyString,
      number: PositiveInt,
      url: TrimmedNonEmptyString,
      isDraft: Schema.Boolean,
      checks: FeatureMapChecks,
    }),
  ),
});
export type FeatureMapIssueFact = typeof FeatureMapIssueFact.Type;

/** What GitHub answered for one ref; `fact` is null when it answered nothing (private, deleted). */
export const FeatureMapRefFact = Schema.Struct({
  repository: TrimmedNonEmptyString,
  number: PositiveInt,
  fact: Schema.NullOr(Schema.Union([FeatureMapIssueFact, FeatureMapPullRequestFact])),
});
export type FeatureMapRefFact = typeof FeatureMapRefFact.Type;

const MapLocation = {
  host: TrimmedNonEmptyString,
  repository: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  commit: TrimmedNonEmptyString,
  fetchedAt: IsoDateTime,
};

/** One coherent read: the map at one commit of the default branch and its refs' facts. */
export const FeatureMapSnapshot = Schema.Union([
  Schema.TaggedStruct("NoMap", {
    ...MapLocation,
    /** No `docs/map.md`, or one without a fenced `mermaid` block. */
    reason: Schema.Literals(["missing", "no-diagram"]),
  }),
  Schema.TaggedStruct("Map", {
    ...MapLocation,
    /** The flowchart, without its fence. */
    source: Schema.String,
    parts: Schema.Array(FeatureMapPart),
    undeclaredRefLines: Schema.Array(FeatureMapRefLineProblem),
    unreadableRefs: Schema.Array(FeatureMapRefLineProblem),
    facts: Schema.Array(FeatureMapRefFact),
    openIssues: Schema.Array(
      Schema.Struct({ number: PositiveInt, title: Schema.String, url: TrimmedNonEmptyString }),
    ),
    /** The open-Issue read stopped at its cap, so more may exist. */
    openIssuesPartial: Schema.Boolean,
  }),
]);
export type FeatureMapSnapshot = typeof FeatureMapSnapshot.Type;

export const FEATURE_MAP_WS_METHODS = {
  read: "featureMap.read",
} as const;

export const FeatureMapRpcs = [
  Rpc.make(FEATURE_MAP_WS_METHODS.read, {
    payload: FeatureMapReadInput,
    success: FeatureMapSnapshot,
    error: Schema.Union([IssueOperationError, EnvironmentAuthorizationError]),
  }),
] as const;
