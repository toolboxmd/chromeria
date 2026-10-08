import type {
  IssueDetail,
  IssueLink,
  IssueListSort,
  IssueListState,
  IssuePullRequest,
  IssueReviewStatus,
  IssueRef,
  IssueState,
  IssueStateEntry,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  aliasedGraphQlDocument,
  type GraphQlDocument,
  type GraphQlVariables,
} from "../sourceControl/githubGraphQl.ts";

/** GitHub's ceiling on a search page. */
export const ISSUE_SEARCH_MAX_ROWS = 100;
/** Children read per Issue; `subIssueCount` says when there are more. */
const SUB_ISSUE_PAGE = 50;
/** Newest comments the side panel shows. */
const COMMENT_PAGE = 100;
/** Closing pull requests read per Issue; more than a few is rare. */
const CLOSING_PULL_REQUEST_PAGE = 10;
/** Thread-linked pull requests read with one search's first page. */
export const LINKED_PULL_REQUEST_MAX = 50;
/** The commit status AgentsMD's independent review posts on a pull request's head. */
const REVIEW_MARK_CONTEXT = "review/independent";

// The three search helpers below are the pull request search's own (`GitHubPullRequestApi.ts`),
// which keeps them private; the Issues search quotes typed text exactly the same way.

/** What a repository selector may hold before it goes into a search as itself. */
const SEARCH_REPOSITORY = /^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/;

/** The reader's words as one literal phrase, so nothing typed becomes a qualifier. */
function searchPhrase(query: string): string {
  return `"${query.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

/** A qualifier value, quoted; a double quote (never in a label or milestone) is dropped. */
function qualifierValue(value: string): string {
  return `"${value.replaceAll('"', "").trim()}"`;
}

/** `owner/name` as GraphQL's separate owner and name arguments. */
export function repositoryParts(repository: string): {
  readonly owner: string;
  readonly name: string;
} {
  const parts = repository.trim().split("/").filter(Boolean);
  return { owner: parts.at(-2) ?? "", name: parts.at(-1) ?? "" };
}

/**
 * The Issues search, built the way the pull request search is (`searchQuery` in
 * GitHubPullRequestCli): typed text is one quoted phrase and label and milestone values are
 * quoted, so nothing a reader types can widen the search. Null when a repository is not a plain
 * `owner/name`.
 */
export function issueSearchQuery(input: {
  readonly repositories: ReadonlyArray<string>;
  readonly state: IssueListState;
  readonly sort?: IssueListSort | undefined;
  readonly query?: string | undefined;
  readonly labels?: ReadonlyArray<string> | undefined;
  readonly milestone?: string | undefined;
}): string | null {
  if (input.repositories.length === 0) return null;
  const repositories = input.repositories.map((repository) => repository.trim());
  if (!repositories.every((repository) => SEARCH_REPOSITORY.test(repository))) return null;
  const query = input.query?.trim() ?? "";
  return [
    "is:issue",
    ...(input.state === "open" ? ["is:open"] : []),
    ...(input.state === "closed" ? ["is:closed"] : []),
    ...(query.length === 0 ? [] : [searchPhrase(query)]),
    ...(input.labels ?? []).map((label) => `label:${qualifierValue(label)}`),
    ...(input.milestone === undefined ? [] : [`milestone:${qualifierValue(input.milestone)}`]),
    // Number order is creation order on GitHub; the page sorts the rows it holds exactly.
    input.sort === "created" || input.sort === "number" ? "sort:created-desc" : "sort:updated-desc",
    ...repositories.map((repository) => `repo:${repository}`),
  ].join(" ");
}

const LINK_FIELDS = "number title url state stateReason repository { nameWithOwner }";

/** A commit's review mark and who posted it; trust is decided against the query's viewer. */
const COMMIT_REVIEW_FIELDS = `oid status { context(name: "${REVIEW_MARK_CONTEXT}") { state creator { login } } }`;

const PULL_REQUEST_FIELDS = `number url state isDraft headRefName headRefOid repository { nameWithOwner }
            headRef { target { ... on Commit { ${COMMIT_REVIEW_FIELDS} } } }`;

/** An Issue's or pull request's URL, or null when a part could not name a real one. */
function resourceUrl(host: string, repository: string, kind: "issues" | "pull", number: number) {
  return /^[a-z0-9.-]+(?::\d+)?$/iu.test(host) &&
    SEARCH_REPOSITORY.test(repository) &&
    Number.isSafeInteger(number) &&
    number >= 1
    ? `https://${host}/${repository}/${kind}/${number}`
    : null;
}

/**
 * Each item that names a real resource, keeping its position: an answer is read back by the
 * alias its position gives it (`linked3`, `issue3`), so a skipped item leaves a gap, not a shift.
 */
function resourceItems(
  items: ReadonlyArray<{ readonly repository: string; readonly number: number }>,
  host: string,
  kind: "issues" | "pull",
): ReadonlyArray<{ readonly position: number; readonly url: string }> {
  return items.flatMap(({ repository, number }, position) => {
    const url = resourceUrl(host, repository, kind, number);
    return url === null ? [] : [{ position, url }];
  });
}

/**
 * A query reading each item through `resource(url:)` under its own alias, plus `rest` (other
 * top-level fields) and its `shared` variables. Every URL travels as a variable.
 * `resource(url:)` answers null for a deleted or invisible item where `repository.pullRequest`
 * would fail the whole request.
 */
function resourceDocument(input: {
  readonly name: string;
  readonly alias: string;
  readonly items: ReadonlyArray<{ readonly position: number; readonly url: string }>;
  readonly type: "Issue" | "PullRequest";
  readonly fields: string;
  readonly rest?: string;
  readonly shared?: GraphQlVariables;
}): GraphQlDocument | null {
  const rest = input.rest ?? "";
  const document = aliasedGraphQlDocument({
    operation: "query",
    name: input.name,
    alias: input.alias,
    key: (item) => item.position,
    items: input.items,
    variables: (item) => ({ url: ["URI!", item.url] }),
    field: ({ url }) => `resource(url: ${url}) { ... on ${input.type} { ${input.fields} } }`,
    ...(input.shared === undefined ? {} : { shared: input.shared }),
    within: (fields) => (rest.length === 0 ? fields : `${fields}\n${rest}`),
  });
  if (document !== null || rest.length === 0) return document;
  // No item to alias: the rest alone, with its shared variables.
  const shared = Object.entries(input.shared ?? {});
  const parameters =
    shared.length === 0
      ? ""
      : `(${shared.map(([name, [type]]) => `$${name}: ${type}`).join(", ")})`;
  return {
    query: `query ${input.name}${parameters} {\n${rest}\n}`,
    variables: Object.fromEntries(shared.map(([name, [, value]]) => [name, value])),
  };
}

function linkedPullRequestItems(
  linkedPullRequests: ReadonlyArray<{ readonly repository: string; readonly number: number }>,
  host: string,
) {
  return resourceItems(linkedPullRequests.slice(0, LINKED_PULL_REQUEST_MAX), host, "pull");
}

/**
 * Each Issue on `host` as the thread's links panel shows it, one alias each: state, title, author
 * and update time. Null when none names a real Issue.
 */
export function issueStatesGraphQlDocument(
  issues: ReadonlyArray<{ readonly repository: string; readonly number: number }>,
  host: string,
): GraphQlDocument | null {
  return resourceDocument({
    name: "IssueStates",
    alias: "issue",
    items: resourceItems(issues, host, "issues"),
    type: "Issue",
    fields: STATE_FIELDS,
  });
}

const STATE_FIELDS = "state stateReason title updatedAt author { login avatarUrl }";

// A resource that is not an Issue answers `{}`, which reads as no state.
const StateNode = Schema.NullOr(
  Schema.Struct({
    state: Schema.optional(Schema.String),
    stateReason: Schema.optional(Schema.NullOr(Schema.String)),
    title: Schema.optional(Schema.String),
    updatedAt: Schema.optional(Schema.String),
    author: Schema.optional(
      Schema.NullOr(
        Schema.Struct({ login: Schema.String, avatarUrl: Schema.optional(Schema.String) }),
      ),
    ),
  }),
);

export const decodeIssueStatesJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({ data: Schema.Record(Schema.String, Schema.optional(StateNode)) }),
  ),
);

/** Each asked Issue by its position, state null and nothing else where GitHub returned none. */
export function issueStatesOf<I extends { readonly repository: string; readonly number: number }>(
  issues: ReadonlyArray<I>,
  data: Readonly<Record<string, typeof StateNode.Type | undefined>>,
): Array<I & Omit<IssueStateEntry, keyof IssueRef>> {
  return issues.map((issue, index) => {
    const node = data[`issue${index}`];
    if (node?.state === undefined) return { ...issue, state: null };
    const login = node.author?.login;
    return {
      ...issue,
      state: issueStateOf({ state: node.state, stateReason: node.stateReason ?? null }),
      ...(node.title === undefined ? {} : { title: node.title }),
      ...(node.updatedAt === undefined ? {} : { updatedAt: node.updatedAt }),
      // A deleted account answers a null author, shown as GitHub's own "ghost".
      author: login ? { login, name: null, avatarUrl: node.author?.avatarUrl || null } : null,
    };
  });
}

/**
 * Only the thread-linked pull requests and the viewer, for a server whose repositories this list
 * does not search (a repository filter names none of them).
 */
export function linkedPullRequestsGraphQlDocument(
  linkedPullRequests: ReadonlyArray<{ readonly repository: string; readonly number: number }>,
  host: string,
): GraphQlDocument {
  return resourceDocument({
    name: "IssueLinkedPullRequests",
    alias: "linked",
    items: linkedPullRequestItems(linkedPullRequests, host),
    type: "PullRequest",
    fields: PULL_REQUEST_FIELDS,
    rest: "  viewer { login }",
  })!;
}

export const decodeViewerJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({ data: Schema.Struct({ viewer: Schema.Struct({ login: Schema.String }) }) }),
  ),
);

/**
 * The search page, plus each given pull request on `host` as its own alias in the same request.
 * The search text and every pull request's URL travel as variables.
 */
export function issueSearchGraphQlDocument(input: {
  readonly rows: number;
  readonly q: string;
  readonly after?: string | undefined;
  readonly linkedPullRequests?: ReadonlyArray<{
    readonly repository: string;
    readonly number: number;
  }>;
  readonly host?: string;
}): GraphQlDocument {
  const first = Math.min(Math.max(Math.trunc(input.rows), 1), ISSUE_SEARCH_MAX_ROWS);
  return resourceDocument({
    name: "IssueSearch",
    alias: "linked",
    items: linkedPullRequestItems(input.linkedPullRequests ?? [], input.host ?? "github.com"),
    type: "PullRequest",
    fields: PULL_REQUEST_FIELDS,
    shared: { q: ["String!", input.q], after: ["String", input.after ?? null] },
    rest: `  viewer { login }
  search(query: $q, type: ISSUE, first: ${first}, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on Issue {
        ${LINK_FIELDS}
        createdAt
        updatedAt
        author { login }
        labels(first: 20) { nodes { name color } }
        milestone { title }
        comments { totalCount }
        parent { ${LINK_FIELDS} }
        subIssues(first: ${SUB_ISSUE_PAGE}) { totalCount nodes { ${LINK_FIELDS} } }
        issueDependenciesSummary { blockedBy }
        closedByPullRequestsReferences(first: ${CLOSING_PULL_REQUEST_PAGE}, includeClosedPrs: true) {
          nodes { ${PULL_REQUEST_FIELDS} }
        }
      }
    }
  }`,
  })!;
}

/** One Issue for the side panel; repository and number are variables. */
export const ISSUE_DETAIL_GRAPHQL_QUERY = `query IssueDetail($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      ${LINK_FIELDS}
      body
      createdAt
      updatedAt
      locked
      viewerCanClose
      viewerCanReopen
      author { login }
      closedByPullRequestsReferences(first: ${CLOSING_PULL_REQUEST_PAGE}, includeClosedPrs: true) {
        nodes { ${PULL_REQUEST_FIELDS} }
      }
      comments(last: ${COMMENT_PAGE}) {
        totalCount
        nodes { id url body createdAt author { login } }
      }
    }
  }
}`;

const Actor = Schema.NullOr(Schema.Struct({ login: Schema.String }));

const LinkNode = Schema.Struct({
  number: Schema.Number,
  title: Schema.String,
  url: Schema.String,
  state: Schema.String,
  stateReason: Schema.NullOr(Schema.String),
  repository: Schema.Struct({ nameWithOwner: Schema.String }),
});
type LinkNode = typeof LinkNode.Type;

// A non-commit target answers `{}`, which this reads as no mark.
const ReviewCommit = Schema.Struct({
  oid: Schema.optional(Schema.String),
  status: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        context: Schema.NullOr(Schema.Struct({ state: Schema.String, creator: Actor })),
      }),
    ),
  ),
});
export type GitHubReviewCommit = typeof ReviewCommit.Type;

const ClosingPullRequestNode = Schema.Struct({
  number: Schema.Number,
  url: Schema.String,
  state: Schema.String,
  isDraft: Schema.Boolean,
  headRefName: Schema.String,
  headRefOid: Schema.String,
  repository: Schema.Struct({ nameWithOwner: Schema.String }),
  // Null once the branch is deleted; the pull request is then closed or merged anyway.
  headRef: Schema.NullOr(Schema.Struct({ target: Schema.NullOr(ReviewCommit) })),
});
type ClosingPullRequestNode = typeof ClosingPullRequestNode.Type;

const SearchNode = Schema.Struct({
  ...LinkNode.fields,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  author: Actor,
  labels: Schema.Struct({
    nodes: Schema.Array(Schema.Struct({ name: Schema.String, color: Schema.String })),
  }),
  milestone: Schema.NullOr(Schema.Struct({ title: Schema.String })),
  comments: Schema.Struct({ totalCount: Schema.Number }),
  parent: Schema.NullOr(LinkNode),
  subIssues: Schema.Struct({ totalCount: Schema.Number, nodes: Schema.Array(LinkNode) }),
  issueDependenciesSummary: Schema.Struct({ blockedBy: Schema.Number }),
  closedByPullRequestsReferences: Schema.Struct({
    nodes: Schema.Array(Schema.NullOr(ClosingPullRequestNode)),
  }),
});
export type GitHubIssueSearchNode = typeof SearchNode.Type;

const IssueSearchJson = Schema.Struct({
  data: Schema.Struct({
    viewer: Schema.Struct({ login: Schema.String }),
    search: Schema.Struct({
      pageInfo: Schema.Struct({
        hasNextPage: Schema.Boolean,
        endCursor: Schema.NullOr(Schema.String),
      }),
      // `is:issue` keeps pull requests out, so every node is an Issue.
      nodes: Schema.Array(SearchNode),
    }),
  }),
});
export type GitHubIssueSearchJson = typeof IssueSearchJson.Type;

export const decodeIssueSearchJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(IssueSearchJson),
);

const decodeAnswerFields = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ data: Schema.Record(Schema.String, Schema.Unknown) })),
);
// A resource that is not a pull request answers `{}` and fails to decode, like a missing one.
const decodeLinkedAlias = Schema.decodeUnknownOption(ClosingPullRequestNode);

/** The thread-linked pull requests a search answer carries; missing or unreadable ones drop. */
export function linkedPullRequestsOf(host: string, raw: string): ReadonlyArray<IssuePullRequest> {
  const answer = decodeAnswerFields(raw);
  if (answer._tag === "None") return [];
  return Object.entries(answer.value.data).flatMap(([alias, value]) => {
    if (!alias.startsWith("linked")) return [];
    const linked = decodeLinkedAlias(value);
    return linked._tag === "Some" ? [pullRequestOf(host, linked.value)] : [];
  });
}

const DetailNode = Schema.Struct({
  ...LinkNode.fields,
  body: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  locked: Schema.Boolean,
  viewerCanClose: Schema.Boolean,
  viewerCanReopen: Schema.Boolean,
  author: Actor,
  closedByPullRequestsReferences: Schema.Struct({
    nodes: Schema.Array(Schema.NullOr(ClosingPullRequestNode)),
  }),
  comments: Schema.Struct({
    totalCount: Schema.Number,
    nodes: Schema.Array(
      Schema.Struct({
        id: Schema.String,
        url: Schema.String,
        body: Schema.String,
        createdAt: Schema.String,
        author: Actor,
      }),
    ),
  }),
});
export type GitHubIssueDetailNode = typeof DetailNode.Type;

export const decodeIssueDetailJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({
      data: Schema.Struct({
        repository: Schema.NullOr(Schema.Struct({ issue: Schema.NullOr(DetailNode) })),
      }),
    }),
  ),
);

export function issueStateOf(node: Pick<LinkNode, "state" | "stateReason">): IssueState {
  if (node.state === "OPEN") return "open";
  return node.stateReason === "NOT_PLANNED" || node.stateReason === "DUPLICATE"
    ? "not-planned"
    : "done";
}

export function issueLinkOf(host: string, node: LinkNode): IssueLink {
  return {
    host,
    repository: node.repository.nameWithOwner,
    number: node.number,
    title: node.title,
    url: node.url,
    state: issueStateOf(node),
  };
}

/**
 * The head commit's `review/independent` status and who posted it; clients decide trust. GitHub
 * keeps only the newest status per context, so an untrusted status posted after a trusted one
 * hides it until the trusted account posts again.
 */
export function reviewStatusOf(
  commit: GitHubReviewCommit | null | undefined,
): IssueReviewStatus | null {
  const context = commit?.status?.context ?? null;
  if (context === null) return null;
  const creator = context.creator?.login || null;
  switch (context.state) {
    case "SUCCESS":
      return { state: "success", creator };
    case "FAILURE":
    case "ERROR":
      return { state: "failure", creator };
    case "PENDING":
    case "EXPECTED":
      return { state: "pending", creator };
    default:
      return null;
  }
}

export function pullRequestOf(host: string, node: ClosingPullRequestNode): IssuePullRequest {
  // The branch can have moved past the head GitHub last synced; only the head's own mark counts.
  const target = node.headRef?.target ?? null;
  const head = target?.oid === node.headRefOid ? target : null;
  return {
    host,
    repository: node.repository.nameWithOwner,
    number: node.number,
    url: node.url,
    state: node.state === "MERGED" ? "merged" : node.state === "OPEN" ? "open" : "closed",
    isDraft: node.isDraft,
    headRefName: node.headRefName,
    headSha: node.headRefOid || null,
    review: reviewStatusOf(head),
  };
}

/** The side panel's Issue, closing pull requests included; null nodes (no access) drop. */
export function issueDetailOf(host: string, issue: GitHubIssueDetailNode): IssueDetail {
  return {
    ...issueLinkOf(host, issue),
    author: issue.author?.login || null,
    body: issue.body,
    createdAt: issue.createdAt,
    updatedAt: issue.updatedAt,
    comments: issue.comments.nodes.map((comment) => ({
      id: comment.id,
      author: comment.author?.login || null,
      body: comment.body,
      createdAt: comment.createdAt,
      url: comment.url,
    })),
    commentCount: issue.comments.totalCount,
    locked: issue.locked,
    viewerCanClose: issue.viewerCanClose,
    viewerCanReopen: issue.viewerCanReopen,
    closingPullRequests: issue.closedByPullRequestsReferences.nodes.flatMap((pullRequest) =>
      pullRequest === null ? [] : [pullRequestOf(host, pullRequest)],
    ),
  };
}
