import { describe, expect, it } from "@effect/vitest";

import {
  decodeIssueDetailJson,
  issueDetailOf,
  issueSearchGraphQlDocument,
  issueStatesGraphQlDocument,
  linkedPullRequestsGraphQlDocument,
  linkedPullRequestsOf,
  pullRequestOf,
  reviewStatusOf,
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

describe("linkedPullRequestsOf", () => {
  it("reads the aliased pull requests and skips the rest of the answer", () => {
    const raw = JSON.stringify({
      data: {
        viewer: { login: "lukemaj" },
        search: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [] },
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
