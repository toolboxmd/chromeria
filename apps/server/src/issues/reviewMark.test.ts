import { describe, expect, it } from "@effect/vitest";

import {
  decodeIssueDetailJson,
  decodeIssueSearchJson,
  issueDetailOf,
  issueSearchGraphQlDocument,
  issueStatesGraphQlDocument,
  linkedPullRequestsGraphQlDocument,
  linkedPullRequestsOf,
  pullRequestCheckVerdictsOf,
  pullRequestChecksGraphQlDocument,
  pullRequestOf,
  reviewStatusOf,
  unsettledCheckUrlsOf,
} from "./gitHubIssues.ts";

const commit = (state: string, creator: string | null) => ({
  oid: "abc123",
  status: { context: { state, creator: creator === null ? null : { login: creator } } },
});

describe("reviewStatusOf", () => {
  it.each([
    ["SUCCESS", "success"],
    ["FAILURE", "failure"],
    ["ERROR", "failure"],
    ["PENDING", "pending"],
    ["EXPECTED", "pending"],
  ] as const)("reads %s as %s with its poster", (state, expected) => {
    expect(reviewStatusOf(commit(state, "LukeMaj"))).toEqual({
      state: expected,
      creator: "LukeMaj",
    });
  });

  it.each([
    ["an unknown state", commit("SOMETHING_NEW", "lukemaj")],
    ["no status", { oid: "abc123", status: null }],
    ["no review context", { oid: "abc123", status: { context: null } }],
    ["no commit", null],
  ])("reads nothing from %s", (_label, head) => {
    expect(reviewStatusOf(head)).toBeNull();
  });

  it("keeps a deleted poster as no creator", () => {
    expect(reviewStatusOf(commit("SUCCESS", null))).toEqual({ state: "success", creator: null });
  });
});

const run = (status: string) => ({ __typename: "CheckRun", status });
const status = (state: string) => ({ __typename: "StatusContext", state });

const node = {
  number: 7,
  url: "https://github.com/toolboxmd/t3code/pull/7",
  state: "OPEN",
  isDraft: true,
  headRefName: "feat/29-issue-status",
  headRefOid: "abc123",
  repository: { nameWithOwner: "toolboxmd/t3code" },
  headRef: { target: commit("PENDING", "lukemaj") },
};

describe("pullRequestOf", () => {
  it("reads an open pull request with its head's review status", () => {
    expect(pullRequestOf("github.com", node)).toEqual({
      host: "github.com",
      repository: "toolboxmd/t3code",
      number: 7,
      url: "https://github.com/toolboxmd/t3code/pull/7",
      state: "open",
      isDraft: true,
      headRefName: "feat/29-issue-status",
      headSha: "abc123",
      review: { state: "pending", creator: "lukemaj" },
    });
  });

  it("counts only the status on the head GitHub reports", () => {
    const moved = { ...node, headRef: { target: { ...commit("SUCCESS", "lukemaj"), oid: "def" } } };
    expect(pullRequestOf("github.com", moved).review).toBeNull();
  });

  it.each([
    ["all passed", { state: "SUCCESS" }, "success"],
    ["one still to arrive", { state: "PENDING" }, "pending"],
    ["one expected", { state: "EXPECTED" }, "pending"],
    ["a head nothing reported on", null, "none"],
  ] as const)("reads the head's checks, %s, as %s", (_label, statusCheckRollup, expected) => {
    const target = { ...commit("SUCCESS", "lukemaj"), statusCheckRollup };
    expect(pullRequestOf("github.com", { ...node, headRef: { target } }).checks).toBe(expected);
  });

  it.each([
    // GitHub reports failure once one check fails, while others may still run.
    ["a failed rollup", { statusCheckRollup: { state: "FAILURE" } }, "abc123"],
    ["an errored rollup", { statusCheckRollup: { state: "ERROR" } }, "abc123"],
    ["a state it does not know", { statusCheckRollup: { state: "SOMETHING_NEW" } }, "abc123"],
    ["a branch that moved past the head", { statusCheckRollup: { state: "SUCCESS" } }, "def"],
    ["a target that is no commit", {}, "abc123"],
  ])("leaves the checks unknown for %s", (_label, rollup, oid) => {
    const target = { ...commit("SUCCESS", "lukemaj"), ...rollup, oid };
    expect(pullRequestOf("github.com", { ...node, headRef: { target } })).not.toHaveProperty(
      "checks",
    );
  });

  it.each([
    ["MERGED", "merged"],
    ["CLOSED", "closed"],
  ] as const)("reads %s without a head branch", (state, expected) => {
    expect(pullRequestOf("github.com", { ...node, state, headRef: null })).toMatchObject({
      state: expected,
      headSha: "abc123",
      review: null,
    });
  });
});

describe("a failed rollup", () => {
  const reviewed = (number: number, overrides: object = {}, mark = "SUCCESS") => ({
    ...node,
    number,
    url: `https://github.com/toolboxmd/t3code/pull/${number}`,
    isDraft: false,
    headRef: { target: { ...commit(mark, "lukemaj"), statusCheckRollup: { state: "FAILURE" } } },
    ...overrides,
  });
  const page = (closing: ReadonlyArray<object | null>, linked: Record<string, object> = {}) => {
    const issue = {
      number: 1,
      title: "Issue",
      url: "https://github.com/toolboxmd/t3code/issues/1",
      state: "OPEN",
      stateReason: null,
      repository: { nameWithOwner: "toolboxmd/t3code" },
      createdAt: "2026-09-20T10:00:00Z",
      updatedAt: "2026-09-21T10:00:00Z",
      author: null,
      labels: { nodes: [] },
      milestone: null,
      comments: { totalCount: 0 },
      parent: null,
      subIssues: { totalCount: 0, nodes: [] },
      issueDependenciesSummary: { blockedBy: 0 },
      closedByPullRequestsReferences: { nodes: closing },
    };
    const raw = JSON.stringify({
      data: {
        viewer: { login: "lukemaj" },
        search: {
          issueCount: 1,
          pageInfo: { hasNextPage: false, endCursor: null },
          nodes: [issue],
        },
        ...linked,
      },
    });
    const rows = decodeIssueSearchJson(raw);
    if (rows._tag !== "Success") throw new Error("the search fixture did not decode");
    return unsettledCheckUrlsOf(rows.success, raw);
  };

  it("is looked into only where it can decide whose move it is", () => {
    expect(
      page(
        [
          reviewed(1),
          reviewed(2, { isDraft: true }),
          reviewed(3, { state: "MERGED" }),
          reviewed(4, {}, "PENDING"),
          reviewed(5, { headRefOid: "moved" }),
          null,
        ],
        // A thread-linked pull request counts like a closing one, once.
        { linked0: reviewed(6), linked1: reviewed(1) },
      ),
    ).toEqual([
      "https://github.com/toolboxmd/t3code/pull/1",
      "https://github.com/toolboxmd/t3code/pull/6",
    ]);
  });

  it("reads the contexts of at most 20 pull requests a page", () => {
    expect(page(Array.from({ length: 25 }, (_, index) => reviewed(index + 1)))).toHaveLength(20);
  });

  it("asks for one page of contexts per pull request, by variable only", () => {
    const document = pullRequestChecksGraphQlDocument(["https://github.com/a/b/pull/1"]);
    expect(document?.query).toContain("checks0: resource(url: $checks0_url)");
    expect(document?.query).toContain("contexts(first: 100)");
    expect(document?.variables).toEqual({ checks0_url: "https://github.com/a/b/pull/1" });
    expect(pullRequestChecksGraphQlDocument([])).toBeNull();
  });

  it.each([
    ["a check run still in progress", [run("COMPLETED"), run("IN_PROGRESS")], 2, "pending"],
    ["a commit status still to arrive", [run("COMPLETED"), status("PENDING")], 2, "pending"],
    ["every context ended", [run("COMPLETED"), status("FAILURE"), status("SUCCESS")], 3, "failure"],
    // One still runs among those read: that much is known whatever the rest are.
    ["one running among more than a page", [run("QUEUED")], 140, "pending"],
    ["more contexts than the page holds", [run("COMPLETED")], 140, undefined],
    ["a context GitHub did not return", [run("COMPLETED"), null], 2, undefined],
  ] as const)("settles %s", (_label, nodes, totalCount, expected) => {
    const urls = ["https://github.com/a/b/pull/1"];
    const answer = (oid: string) => ({
      checks0: {
        headRefOid: "abc123",
        headRef: { target: { oid, statusCheckRollup: { contexts: { totalCount, nodes } } } },
      },
    });
    expect(pullRequestCheckVerdictsOf(urls, answer("abc123")).get(urls[0]!)).toBe(expected);
    // A head that moved since the page was read settles nothing.
    expect(pullRequestCheckVerdictsOf(urls, answer("def")).size).toBe(0);
    expect(pullRequestCheckVerdictsOf(urls, { checks0: null }).size).toBe(0);
  });
});

describe("linkedPullRequestsOf", () => {
  it("reads the aliased pull requests and skips the rest of the answer", () => {
    const raw = JSON.stringify({
      data: {
        viewer: { login: "lukemaj" },
        search: { issueCount: 0, pageInfo: { hasNextPage: false, endCursor: null }, nodes: [] },
        linked0: node,
        // Gone, or not a pull request.
        linked1: null,
        linked2: {},
      },
    });
    expect(linkedPullRequestsOf("github.com", raw).map((pr) => pr.number)).toEqual([7]);
  });
});

describe("issueSearchGraphQlDocument", () => {
  it("reads each valid linked pull request under its own alias, by variable only", () => {
    const document = issueSearchGraphQlDocument({
      rows: 10,
      q: 'is:issue "typed text"',
      linkedPullRequests: [
        { repository: "toolboxmd/t3code", number: 36 },
        { repository: 'evil") { x } #', number: 1 },
        { repository: "toolboxmd/t3code", number: 0 },
      ],
      host: "github.com",
    });
    expect(document.query).toContain("linked0: resource(url: $linked0_url) { ... on PullRequest {");
    expect(document.query).not.toContain("linked1");
    expect(document.query).not.toContain("linked2");
    expect(document.query).not.toContain("typed text");
    expect(document.variables).toEqual({
      q: 'is:issue "typed text"',
      after: null,
      linked0_url: "https://github.com/toolboxmd/t3code/pull/36",
    });
  });

  it("reads at most the newest 50 linked pull requests", () => {
    const document = issueSearchGraphQlDocument({
      rows: 10,
      q: "is:issue",
      linkedPullRequests: Array.from({ length: 60 }, (_, index) => ({
        repository: "toolboxmd/t3code",
        number: index + 1,
      })),
    });
    expect(document.query).toContain("linked49: resource(url: $linked49_url)");
    expect(document.query).not.toContain("linked50");
    expect(
      Object.keys(document.variables).filter((name) => name.startsWith("linked")),
    ).toHaveLength(50);
  });

  it("is the search alone without linked pull requests", () => {
    const document = issueSearchGraphQlDocument({ rows: 500, q: "is:issue", after: "cursor" });
    expect(document.query).toContain("query IssueSearch($q: String!, $after: String) {");
    expect(document.query).toContain("first: 100, after: $after");
    expect(document.variables).toEqual({ q: "is:issue", after: "cursor" });
  });
});

describe("linkedPullRequestsGraphQlDocument", () => {
  it("reads only the viewer and the linked pull requests", () => {
    const document = linkedPullRequestsGraphQlDocument(
      [{ repository: "toolboxmd/t3code", number: 36 }],
      "github.com",
    );
    expect(document.query).toContain("viewer { login }");
    expect(document.query).toContain("linked0: resource(url: $linked0_url)");
    expect(document.query).not.toContain("search(");
    expect(document.variables).toEqual({
      linked0_url: "https://github.com/toolboxmd/t3code/pull/36",
    });
  });
});

describe("issueStatesGraphQlDocument", () => {
  it("keeps each Issue's position as its alias, skipping ones it cannot name", () => {
    const document = issueStatesGraphQlDocument(
      [
        { repository: "toolboxmd/t3code", number: 3 },
        { repository: "not a repository", number: 4 },
        { repository: "toolboxmd/t3code", number: 5 },
      ],
      "github.com",
    );
    expect(document?.query).toContain("issue0: resource(url: $issue0_url) { ... on Issue {");
    expect(document?.query).not.toContain("issue1");
    expect(document?.variables).toEqual({
      issue0_url: "https://github.com/toolboxmd/t3code/issues/3",
      issue2_url: "https://github.com/toolboxmd/t3code/issues/5",
    });
    expect(issueStatesGraphQlDocument([{ repository: "x", number: 1 }], "github.com")).toBeNull();
  });
});

describe("issueDetailOf", () => {
  it("decodes the detail read and maps its closing pull requests, dropping null nodes", () => {
    const raw = JSON.stringify({
      data: {
        repository: {
          issue: {
            number: 26,
            title: "Prism settings",
            url: "https://github.com/toolboxmd/t3code/issues/26",
            state: "CLOSED",
            stateReason: "COMPLETED",
            repository: { nameWithOwner: "toolboxmd/t3code" },
            body: "Body",
            createdAt: "2026-09-20T10:00:00Z",
            updatedAt: "2026-09-21T10:00:00Z",
            locked: false,
            viewerCanClose: true,
            viewerCanReopen: true,
            author: { login: "lukemaj" },
            // A pull request in a repository the viewer cannot see answers null.
            closedByPullRequestsReferences: { nodes: [{ ...node, state: "MERGED" }, null] },
            comments: { totalCount: 0, nodes: [] },
          },
        },
      },
    });
    const decoded = decodeIssueDetailJson(raw);
    if (decoded._tag !== "Success") throw new Error("the detail fixture did not decode");
    const detail = issueDetailOf("github.com", decoded.success.data.repository!.issue!);
    expect(detail.state).toBe("done");
    expect(detail.closingPullRequests).toEqual([
      {
        host: "github.com",
        repository: "toolboxmd/t3code",
        number: 7,
        url: "https://github.com/toolboxmd/t3code/pull/7",
        state: "merged",
        isDraft: true,
        headRefName: "feat/29-issue-status",
        headSha: "abc123",
        review: { state: "pending", creator: "lukemaj" },
      },
    ]);
  });
});
