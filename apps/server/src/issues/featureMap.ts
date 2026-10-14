import type {
  FeatureMapChecks,
  FeatureMapPart,
  FeatureMapRef,
  FeatureMapRefFact,
  FeatureMapRefLineProblem,
} from "@t3tools/contracts";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import {
  aliasedGraphQlDocument,
  type GraphQlDocument,
} from "@t3tools/source-control-github/server/githubGraphQl";
import { issueStateOf, repositoryParts } from "./gitHubIssues.ts";

/** Where a project keeps its feature map, on its default branch. */
export const FEATURE_MAP_PATH = "docs/map.md";
/** Open Issues read per page for the "open Issues on no part" gap. */
export const FEATURE_MAP_OPEN_ISSUE_PAGE = 100;
/** Open-Issue pages read before the gap is marked partial (500 Issues). */
export const FEATURE_MAP_OPEN_ISSUE_PAGES = 5;
/** Refs read per request. */
export const FEATURE_MAP_REFS_PER_READ = 100;
/** Closing pull requests read per Issue; more marks the Issue's facts incomplete. */
const CLOSING_PULL_REQUEST_PAGE = 10;
/** Labels read per Issue, GitHub's page maximum; labels add no cost per page size. */
const LABEL_PAGE = 100;

const FENCE = /^```mermaid[ \t]*\r?\n([\s\S]*?)\r?\n```[ \t]*$/m;
const PART = /^\s*([A-Za-z]\w*)\s*\["([^"]*)"\]\s*;?\s*$/;
// `%%{init}%%` directives start with a brace, so they never match an ID.
const REF_LINE = /^\s*%%\s*([A-Za-z]\w*)\s*:(.*)$/;
const REPOSITORY = /^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/;
const NUMBER = /^#(\d+)$/;
const REPOSITORY_NUMBER = /^([A-Za-z0-9._-]+\/[A-Za-z0-9._-]+)#(\d+)$/;

export interface ParsedFeatureMap {
  readonly source: string;
  readonly parts: ReadonlyArray<FeatureMapPart>;
  readonly undeclaredRefLines: ReadonlyArray<FeatureMapRefLineProblem>;
  readonly unreadableRefs: ReadonlyArray<FeatureMapRefLineProblem>;
}

function refOf(
  host: string,
  kind: FeatureMapRef["kind"],
  repository: string,
  number: string,
): FeatureMapRef | null {
  const value = Number(number);
  if (!Number.isSafeInteger(value) || value < 1) return null;
  return {
    kind,
    repository,
    number: value,
    url: `https://${host}/${repository}/${kind === "issue" ? "issues" : "pull"}/${value}`,
  };
}

/**
 * The refs on one `%% ID:` line: `#7`, `PR #38`, `owner/repo#231` or `owner/repo PR #227`, all on
 * `host`; anything else is reported, never guessed.
 */
function readRefs(text: string, at: { readonly host: string; readonly repository: string }) {
  const tokens = text.trim().split(/\s+/).filter(Boolean);
  const refs: Array<FeatureMapRef> = [];
  const unreadable: Array<string> = [];
  let firstUnreadable = false;
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    let repository = at.repository;
    let cursor = index;
    if (REPOSITORY.test(token) && !token.includes("#")) {
      repository = token;
      cursor++;
    }
    const pull = tokens[cursor] === "PR";
    if (pull) cursor++;
    const qualified = !pull && cursor === index ? REPOSITORY_NUMBER.exec(token) : null;
    const number = qualified?.[2] ?? NUMBER.exec(tokens[cursor] ?? "")?.[1];
    const ref =
      number === undefined
        ? null
        : refOf(at.host, pull ? "pull" : "issue", qualified?.[1] ?? repository, number);
    if (ref === null) {
      if (refs.length === 0 && unreadable.length === 0) firstUnreadable = true;
      unreadable.push(token);
      continue;
    }
    refs.push(ref);
    index = cursor;
  }
  return { refs, unreadable, firstUnreadable };
}

/**
 * The first fenced `mermaid` block of `docs/map.md`: each `ID["Name"]` line declares a part, and
 * each `%% ID: refs` line binds it (first ref primary) or marks it `context`. Edges and other lines
 * stay plain Mermaid. Null when the file holds no such block.
 */
export function parseFeatureMapFile(
  markdown: string,
  at: { readonly host: string; readonly repository: string },
): ParsedFeatureMap | null {
  const source = FENCE.exec(markdown)?.[1];
  if (source === undefined) return null;
  const parts = new Map<string, { name: string }>();
  const lines: Array<{ id: string; text: string }> = [];
  for (const line of source.split(/\r?\n/)) {
    const part = PART.exec(line);
    if (part !== null) {
      if (!parts.has(part[1]!)) parts.set(part[1]!, { name: part[2]! });
      continue;
    }
    const refLine = REF_LINE.exec(line);
    if (refLine !== null) lines.push({ id: refLine[1]!, text: refLine[2]!.trim() });
  }
  const bound = new Map<
    string,
    { context: boolean; primaryUnreadable: boolean; refs: Array<FeatureMapRef> }
  >();
  const undeclaredRefLines: Array<FeatureMapRefLineProblem> = [];
  const unreadableRefs: Array<FeatureMapRefLineProblem> = [];
  for (const { id, text } of lines) {
    if (!parts.has(id)) {
      undeclaredRefLines.push({ id, text });
      continue;
    }
    const binding = bound.get(id) ?? { context: false, primaryUnreadable: false, refs: [] };
    if (text === "context") {
      binding.context = true;
    } else {
      const { refs, unreadable, firstUnreadable } = readRefs(text, at);
      // The first token is the primary ref; a later ref must not take its place.
      if (binding.refs.length === 0 && firstUnreadable) binding.primaryUnreadable = true;
      binding.refs.push(...refs);
      unreadableRefs.push(...unreadable.map((token) => ({ id, text: token })));
    }
    bound.set(id, binding);
  }
  return {
    source,
    parts: [...parts].map(([id, { name }]) => ({
      id,
      name,
      context: bound.get(id)?.context ?? false,
      primaryUnreadable: bound.get(id)?.primaryUnreadable ?? false,
      refs: bound.get(id)?.refs ?? [],
    })),
    undeclaredRefLines,
    unreadableRefs,
  };
}

/** Each ref once, in first-seen order. */
export function featureMapRefsOf(parts: ReadonlyArray<FeatureMapPart>): Array<FeatureMapRef> {
  const seen = new Map<string, FeatureMapRef>();
  for (const part of parts) {
    for (const ref of part.refs) {
      const key = `${ref.repository.toLowerCase()}#${ref.number}`;
      if (!seen.has(key)) seen.set(key, ref);
    }
  }
  return [...seen.values()];
}

/** The default branch and its head commit. */
export const FEATURE_MAP_HEAD_QUERY = `query FeatureMapHead($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef { name target { oid } }
  }
}`;

export const decodeFeatureMapHeadJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({
      data: Schema.Struct({
        repository: Schema.NullOr(
          Schema.Struct({
            defaultBranchRef: Schema.NullOr(
              Schema.Struct({ name: Schema.String, target: Schema.Struct({ oid: Schema.String }) }),
            ),
          }),
        ),
      }),
    }),
  ),
);

/** `docs/map.md` at exactly that commit; a missing file answers null, not an error. */
export const FEATURE_MAP_FILE_QUERY = `query FeatureMapFile($owner: String!, $name: String!, $expression: String!) {
  repository(owner: $owner, name: $name) {
    object(expression: $expression) { ... on Blob { text } }
  }
}`;

export const decodeFeatureMapFileJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({
      data: Schema.Struct({
        repository: Schema.NullOr(
          Schema.Struct({
            object: Schema.NullOr(
              Schema.Struct({ text: Schema.optional(Schema.NullOr(Schema.String)) }),
            ),
          }),
        ),
      }),
    }),
  ),
);

const HEAD_CHECKS = "commits(last: 1) { nodes { commit { statusCheckRollup { state } } } }";

// Both fragments, so a ref written as an Issue that is a pull request (or the reverse) still reads.
const REF_FIELDS = `__typename
      ... on Issue {
        url state stateReason
        labels(first: ${LABEL_PAGE}) { pageInfo { hasNextPage } nodes { name } }
        closedByPullRequestsReferences(first: ${CLOSING_PULL_REQUEST_PAGE}) {
          pageInfo { hasNextPage }
          nodes { number url state isDraft repository { nameWithOwner } ${HEAD_CHECKS} }
        }
      }
      ... on PullRequest {
        url state isDraft baseRefName
        baseRepository { defaultBranchRef { name } }
        ${HEAD_CHECKS}
      }`;

const OPEN_ISSUES = `  repository(owner: $owner, name: $name) {
    issues(states: OPEN, first: ${FEATURE_MAP_OPEN_ISSUE_PAGE}, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes { number title url }
    }
  }`;

/**
 * Up to `FEATURE_MAP_REFS_PER_READ` refs, each through `resource(url:)` (null for a private or
 * deleted item instead of failing the request), plus one page of the repository's open Issues.
 */
export function featureMapFactsDocument(input: {
  readonly refs: ReadonlyArray<FeatureMapRef>;
  readonly repository: string;
  readonly after: string | null;
}): GraphQlDocument {
  const { owner, name } = repositoryParts(input.repository);
  const shared = {
    owner: ["String!", owner],
    name: ["String!", name],
    after: ["String", input.after],
  } as const;
  return (
    aliasedGraphQlDocument({
      operation: "query",
      name: "FeatureMapFacts",
      alias: "ref",
      items: input.refs,
      variables: (ref) => ({ url: ["URI!", ref.url] }),
      field: ({ url }) => `resource(url: ${url}) {\n      ${REF_FIELDS}\n    }`,
      shared,
      within: (fields) => `${fields}\n${OPEN_ISSUES}`,
    }) ?? {
      query: `query FeatureMapFacts($owner: String!, $name: String!, $after: String) {\n${OPEN_ISSUES}\n}`,
      variables: { owner, name, after: input.after },
    }
  );
}

const Checks = Schema.Struct({
  nodes: Schema.Array(
    Schema.NullOr(
      Schema.Struct({
        commit: Schema.Struct({
          statusCheckRollup: Schema.NullOr(Schema.Struct({ state: Schema.String })),
        }),
      }),
    ),
  ),
});

const IssueAnswer = Schema.Struct({
  __typename: Schema.Literal("Issue"),
  url: Schema.String,
  state: Schema.String,
  stateReason: Schema.NullOr(Schema.String),
  labels: Schema.Struct({
    pageInfo: Schema.Struct({ hasNextPage: Schema.Boolean }),
    nodes: Schema.Array(Schema.NullOr(Schema.Struct({ name: Schema.String }))),
  }),
  closedByPullRequestsReferences: Schema.Struct({
    pageInfo: Schema.Struct({ hasNextPage: Schema.Boolean }),
    nodes: Schema.Array(
      Schema.NullOr(
        Schema.Struct({
          number: Schema.Number,
          url: Schema.String,
          state: Schema.String,
          isDraft: Schema.Boolean,
          repository: Schema.Struct({ nameWithOwner: Schema.String }),
          commits: Checks,
        }),
      ),
    ),
  }),
});

const PullRequestAnswer = Schema.Struct({
  __typename: Schema.Literal("PullRequest"),
  url: Schema.String,
  state: Schema.String,
  isDraft: Schema.Boolean,
  baseRefName: Schema.String,
  baseRepository: Schema.NullOr(
    Schema.Struct({ defaultBranchRef: Schema.NullOr(Schema.Struct({ name: Schema.String })) }),
  ),
  commits: Checks,
});

// Anything else (null, `{}` for another type) reads as no answer.
const decodeRefAnswer = Schema.decodeUnknownOption(Schema.Union([IssueAnswer, PullRequestAnswer]));

const FactsJson = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(
      Schema.Struct({
        issues: Schema.Struct({
          pageInfo: Schema.Struct({
            hasNextPage: Schema.Boolean,
            endCursor: Schema.NullOr(Schema.String),
          }),
          nodes: Schema.Array(
            Schema.NullOr(
              Schema.Struct({ number: Schema.Number, title: Schema.String, url: Schema.String }),
            ),
          ),
        }),
      }),
    ),
  }),
});
const decodeFactsJson = Schema.decodeUnknownResult(Schema.fromJsonString(FactsJson));
const decodeAliases = Schema.decodeUnknownResult(
  Schema.fromJsonString(Schema.Struct({ data: Schema.Record(Schema.String, Schema.Unknown) })),
);

/** The open-Issue page and every alias (`ref<N>`, read by `featureMapFactsOf`) of one answer. */
export const decodeFeatureMapFactsJson = (raw: string) =>
  Result.flatMap(decodeFactsJson(raw), (typed) =>
    Result.map(decodeAliases(raw), (aliases) => ({
      openIssues: typed.data.repository?.issues ?? null,
      aliases: aliases.data,
    })),
  );

function checksOf(commits: typeof Checks.Type): FeatureMapChecks {
  switch (commits.nodes.at(-1)?.commit.statusCheckRollup?.state) {
    case "SUCCESS":
      return "passing";
    case "FAILURE":
    case "ERROR":
      return "failing";
    case "PENDING":
    case "EXPECTED":
      return "pending";
    default:
      return null;
  }
}

function pullRequestStateOf(state: string) {
  return state === "MERGED" ? "merged" : state === "OPEN" ? "open" : "closed";
}

/** Each ref's fact from the answer's `ref<N>` aliases, by position. */
export function featureMapFactsOf(
  refs: ReadonlyArray<FeatureMapRef>,
  data: Readonly<Record<string, unknown>>,
): Array<FeatureMapRefFact> {
  return refs.map((ref, index) => {
    const answer = decodeRefAnswer(data[`ref${index}`]);
    const base = { repository: ref.repository, number: ref.number };
    if (answer._tag === "None") return { ...base, fact: null };
    const node = answer.value;
    if (node.__typename === "PullRequest") {
      const state = pullRequestStateOf(node.state);
      const defaultBranch = node.baseRepository?.defaultBranchRef?.name ?? null;
      return {
        ...base,
        fact: {
          kind: "pull",
          url: node.url,
          state,
          isDraft: node.isDraft,
          mergedIntoDefault: state === "merged" && node.baseRefName === defaultBranch,
          checks: checksOf(node.commits),
        },
      };
    }
    return {
      ...base,
      fact: {
        kind: "issue",
        url: node.url,
        state: issueStateOf(node),
        labels: node.labels.nodes.flatMap((label) => (label === null ? [] : [label.name])),
        complete:
          !node.labels.pageInfo.hasNextPage &&
          !node.closedByPullRequestsReferences.pageInfo.hasNextPage,
        openClosingPullRequests: node.closedByPullRequestsReferences.nodes.flatMap((pullRequest) =>
          pullRequest === null || pullRequest.state !== "OPEN"
            ? []
            : [
                {
                  repository: pullRequest.repository.nameWithOwner,
                  number: pullRequest.number,
                  url: pullRequest.url,
                  isDraft: pullRequest.isDraft,
                  checks: checksOf(pullRequest.commits),
                },
              ],
        ),
      },
    };
  });
}
