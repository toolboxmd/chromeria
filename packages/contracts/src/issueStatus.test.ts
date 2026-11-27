import { describe, expect, it } from "vite-plus/test";

import {
  type IssueGroupInput,
  type IssueGroupPullRequest,
  type IssueGrouping,
  type IssueStatus,
  type IssueStatusInput,
  type IssueStatusPullRequest,
  issueGroupOf,
  issueStatusOf,
  trustedReviewMark,
} from "./issueStatus.ts";

const pr = (
  state: IssueStatusPullRequest["state"],
  reviewMark: IssueStatusPullRequest["reviewMark"] = null,
  isDraft = false,
): IssueStatusPullRequest => ({ state, reviewMark, isDraft });
const draft = (reviewMark: IssueStatusPullRequest["reviewMark"] = null) =>
  pr("open", reviewMark, true);

const input = (overrides: Partial<IssueStatusInput> = {}): IssueStatusInput => ({
  state: "open",
  openBlockerCount: 0,
  pullRequests: [],
  hasTaskBranch: false,
  linkedThreadCount: 0,
  workingNow: false,
  ...overrides,
});

describe("issueStatusOf: every row of the status table", () => {
  it.each<[IssueStatus, Partial<IssueStatusInput>]>([
    ["done", { state: "done" }],
    ["not-planned", { state: "not-planned" }],
    ["in-review", { pullRequests: [pr("open", "pending")] }],
    ["in-progress", { hasTaskBranch: true, linkedThreadCount: 1, workingNow: true }],
    ["in-progress", { pullRequests: [pr("open")], linkedThreadCount: 1, workingNow: true }],
    ["waiting-for-merge", { pullRequests: [pr("open", "success")] }],
    ["changes-requested", { pullRequests: [pr("open", "failure")] }],
    ["waiting-for-review", { pullRequests: [pr("open")] }],
    ["paused", { hasTaskBranch: true, linkedThreadCount: 1 }],
    ["blocked", { openBlockerCount: 2 }],
    ["discussion", { linkedThreadCount: 1 }],
    ["to-do", {}],
  ])("%s", (expected, overrides) => {
    expect(issueStatusOf(input(overrides))).toBe(expected);
  });
});

describe("issueStatusOf: first match wins", () => {
  const everything: Partial<IssueStatusInput> = {
    openBlockerCount: 1,
    pullRequests: [pr("open", "pending"), pr("open", "success"), pr("open", "failure"), pr("open")],
    hasTaskBranch: true,
    linkedThreadCount: 2,
    workingNow: true,
  };

  it.each<[string, IssueStatus, Partial<IssueStatusInput>]>([
    ["Done over every other signal", "done", { ...everything, state: "done" }],
    ["Not planned over every other signal", "not-planned", { ...everything, state: "not-planned" }],
    ["In review over In progress", "in-review", everything],
    [
      "In progress over Waiting for merge",
      "in-progress",
      { ...everything, pullRequests: [pr("open", "success")] },
    ],
    [
      "Waiting for merge over Changes requested",
      "waiting-for-merge",
      { pullRequests: [pr("open", "failure"), pr("open", "success")] },
    ],
    [
      "Changes requested over Waiting for review",
      "changes-requested",
      { pullRequests: [pr("open"), pr("open", "failure")] },
    ],
    [
      "Waiting for review over Paused",
      "waiting-for-review",
      { pullRequests: [pr("open")], hasTaskBranch: true, linkedThreadCount: 1 },
    ],
    [
      "Paused over Blocked",
      "paused",
      { hasTaskBranch: true, linkedThreadCount: 1, openBlockerCount: 1 },
    ],
    ["Blocked over Discussion", "blocked", { openBlockerCount: 1, linkedThreadCount: 1 }],
    ["Discussion over To do", "discussion", { linkedThreadCount: 1 }],
  ])("%s", (_label, expected, overrides) => {
    expect(issueStatusOf(input(overrides))).toBe(expected);
  });
});

describe("issueStatusOf: mixed review marks across pull requests", () => {
  it.each<[string, ReadonlyArray<IssueStatusPullRequest>, IssueStatus]>([
    [
      "pending, success and failure",
      [pr("open", "failure"), pr("open", "success"), pr("open", "pending")],
      "in-review",
    ],
    ["pending and failure", [pr("open", "failure"), pr("open", "pending")], "in-review"],
    ["pending and success", [pr("open", "success"), pr("open", "pending")], "in-review"],
    ["success and failure", [pr("open", "failure"), pr("open", "success")], "waiting-for-merge"],
    ["failure and none", [pr("open"), pr("open", "failure")], "changes-requested"],
  ])("%s: %s", (_label, pullRequests, expected) => {
    expect(issueStatusOf(input({ pullRequests }))).toBe(expected);
  });
});

describe("issueStatusOf: which pull requests count", () => {
  it("ignores merged pull requests while the Issue stays open", () => {
    // A component PR merged into an integration branch; the final PR into main closes the Issue.
    expect(issueStatusOf(input({ pullRequests: [pr("merged", "success")] }))).toBe("to-do");
    expect(issueStatusOf(input({ pullRequests: [pr("merged")], linkedThreadCount: 1 }))).toBe(
      "discussion",
    );
  });

  it("ignores closed pull requests and their marks", () => {
    expect(issueStatusOf(input({ pullRequests: [pr("closed", "pending")] }))).toBe("to-do");
  });

  it("reads an open draft with nobody working as Paused, not Waiting for review", () => {
    expect(issueStatusOf(input({ pullRequests: [draft()], linkedThreadCount: 1 }))).toBe("paused");
    // Even over an open blocker: the draft is the work.
    expect(issueStatusOf(input({ pullRequests: [draft()], openBlockerCount: 1 }))).toBe("paused");
  });

  it("keeps an open draft with a working thread In progress", () => {
    expect(
      issueStatusOf(input({ pullRequests: [draft()], linkedThreadCount: 1, workingNow: true })),
    ).toBe("in-progress");
  });

  it("still reads a review mark on a draft", () => {
    expect(issueStatusOf(input({ pullRequests: [draft("pending")] }))).toBe("in-review");
  });

  it("waits for review once any open pull request is ready", () => {
    expect(issueStatusOf(input({ pullRequests: [draft(), pr("open")] }))).toBe(
      "waiting-for-review",
    );
  });

  it("keeps a working thread without a branch or pull request in Discussion", () => {
    expect(issueStatusOf(input({ linkedThreadCount: 1, workingNow: true }))).toBe("discussion");
  });

  it("never shows the review statuses without review marks", () => {
    const review = new Set<IssueStatus>(["in-review", "waiting-for-merge", "changes-requested"]);
    for (const state of ["open", "done", "not-planned"] as const) {
      for (const pullRequests of [[], [pr("open")], [draft()], [pr("merged")], [pr("closed")]]) {
        for (const hasTaskBranch of [false, true]) {
          for (const workingNow of [false, true]) {
            for (const openBlockerCount of [0, 1]) {
              const status = issueStatusOf(
                input({
                  state,
                  pullRequests,
                  hasTaskBranch,
                  workingNow,
                  openBlockerCount,
                  linkedThreadCount: 1,
                }),
              );
              expect(review.has(status)).toBe(false);
            }
          }
        }
      }
    }
  });
});

type Grouping = IssueGrouping<string, number>;
type GroupInput = IssueGroupInput<string, number>;

/** A pull request named by its number, which is also what a target opens. */
const groupPr = (
  ref: number,
  overrides: Partial<IssueGroupPullRequest<number>> = {},
): IssueGroupPullRequest<number> => ({
  ref,
  state: "open",
  isDraft: false,
  reviewMark: null,
  ...overrides,
});
const reviewed = (ref: number, checks?: IssueGroupPullRequest<number>["checks"]) =>
  groupPr(ref, { reviewMark: "success", checks });
const linked = (ref: string, overrides: Partial<GroupInput["threads"][number]> = {}) => ({
  ref,
  archived: false,
  waitsOnUser: null,
  ...overrides,
});
const groupInput = (overrides: Partial<GroupInput> = {}): GroupInput => ({
  state: "open",
  openBlockerCount: 0,
  labels: [],
  threads: [],
  pullRequests: [],
  hasTaskBranch: false,
  workingNow: false,
  ...overrides,
});
/** Group, reason and target as one row: `status:<name>` for a status reason, `+N` for blockers. */
const read = ({ group, reason, target }: Grouping) => [
  group,
  reason === null
    ? null
    : reason.kind === "status"
      ? `status:${reason.status}${reason.openBlockerCount > 0 ? `+${reason.openBlockerCount}` : ""}`
      : reason.kind === "blocked"
        ? `blocked:${reason.openBlockerCount}`
        : reason.kind,
  target.kind === "issue"
    ? "issue"
    : target.kind === "thread"
      ? `thread:${target.thread}`
      : `pr:${target.pullRequest}`,
];

describe("issueGroupOf: every rule of the group table", () => {
  it.each<[string, Partial<GroupInput>, ReturnType<typeof read>]>([
    ["Done stays GitHub's own state", { state: "done" }, ["done", null, "issue"]],
    ["Not planned likewise", { state: "not-planned" }, ["not-planned", null, "issue"]],
    ["1: the needs-user label", { labels: ["Needs-User"] }, ["your-move", "needs-user", "issue"]],
    [
      "2: a linked thread waits on the user",
      { threads: [linked("idle"), linked("asks", { waitsOnUser: "asks" })] },
      ["your-move", "thread-waits-on-user", "thread:asks"],
    ],
    [
      "2: a thread it spawned holds the request, and that thread opens",
      { threads: [linked("parent", { waitsOnUser: "child" })] },
      ["your-move", "thread-waits-on-user", "thread:child"],
    ],
    ["3: the waiting label", { labels: ["bug", "waiting"] }, ["waiting", "waiting", "issue"]],
    [
      "4: a linked thread works on a branch",
      { threads: [linked("t")], hasTaskBranch: true, workingNow: true },
      ["agents-working", "status:in-progress", "issue"],
    ],
    [
      "4: work in flight keeps its blockers on the row",
      { threads: [linked("t")], hasTaskBranch: true, workingNow: true, openBlockerCount: 2 },
      ["agents-working", "status:in-progress+2", "issue"],
    ],
    [
      "4: work without a branch on a blocked Issue names the blockers once",
      { threads: [linked("t")], workingNow: true, openBlockerCount: 2 },
      ["agents-working", "blocked:2", "issue"],
    ],
    [
      "5: a pull request in review, draft or not",
      { pullRequests: [groupPr(7, { isDraft: true, reviewMark: "pending" })] },
      ["agents-working", "status:in-review", "issue"],
    ],
    [
      "6: reviewed while its checks run",
      { pullRequests: [reviewed(7, "pending")] },
      ["agents-working", "reviewed-checks-running", "pr:7"],
    ],
    [
      "7: reviewed and its checks passed",
      { pullRequests: [reviewed(7, "success")] },
      ["your-move", "reviewed-not-merged", "pr:7"],
    ],
    [
      "7: reviewed and its checks failed",
      { pullRequests: [reviewed(7, "failure")] },
      ["your-move", "reviewed-not-merged", "pr:7"],
    ],
    [
      "7: reviewed, and its head has no checks at all",
      { pullRequests: [reviewed(7, "none")] },
      ["your-move", "reviewed-not-merged", "pr:7"],
    ],
    ["8: open native blockers", { openBlockerCount: 3 }, ["blocked", "blocked:3", "issue"]],
    [
      "9: review found problems; its pull request opens",
      { threads: [linked("t")], pullRequests: [groupPr(7, { reviewMark: "failure" })] },
      ["stalled", "status:changes-requested", "pr:7"],
    ],
    [
      "9: a pull request nobody reviewed",
      { pullRequests: [groupPr(6, { isDraft: true }), groupPr(7)] },
      ["stalled", "status:waiting-for-review", "pr:7"],
    ],
    [
      "9: a task branch with its thread idle",
      { threads: [linked("old", { archived: true }), linked("t")], hasTaskBranch: true },
      ["stalled", "status:paused", "thread:t"],
    ],
    [
      "9: a draft with no thread to resume",
      { pullRequests: [groupPr(7, { isDraft: true })] },
      ["stalled", "status:paused", "pr:7"],
    ],
    [
      "9: a thread that only discussed it",
      { threads: [linked("t")] },
      ["stalled", "status:discussion", "thread:t"],
    ],
    [
      "9: a task branch on its own, with no thread and no pull request held",
      { hasTaskBranch: true },
      ["stalled", "status:paused", "issue"],
    ],
    [
      "9: an archived-only linked thread",
      { threads: [linked("gone", { archived: true })], hasTaskBranch: true },
      ["stalled", "last-thread-archived", "thread:gone"],
    ],
    ["10: nothing started", {}, ["nobody-on-it", null, "issue"]],
    [
      "10: a pull request closed without merging counts as none",
      { pullRequests: [groupPr(7, { state: "closed", reviewMark: "success" })] },
      ["nobody-on-it", null, "issue"],
    ],
    [
      "10: a merged component pull request counts as none",
      { pullRequests: [groupPr(7, { state: "merged", reviewMark: "success" })] },
      ["nobody-on-it", null, "issue"],
    ],
  ])("%s", (_label, overrides, expected) => {
    expect(read(issueGroupOf(groupInput(overrides)))).toEqual(expected);
  });
});

describe("issueGroupOf: first match wins", () => {
  /** Every later rule's signal at once; each row adds the one signal that outranks them. */
  const stalled: Partial<GroupInput> = {
    threads: [linked("t")],
    hasTaskBranch: true,
    pullRequests: [groupPr(9)],
  };
  const everything: Partial<GroupInput> = {
    ...stalled,
    labels: ["waiting", "needs-user"],
    threads: [linked("t", { waitsOnUser: "t" })],
    workingNow: true,
    openBlockerCount: 1,
    pullRequests: [
      groupPr(9),
      reviewed(8, "success"),
      reviewed(7, "pending"),
      groupPr(6, { reviewMark: "pending" }),
    ],
  };

  it.each<[string, Partial<GroupInput>, ReturnType<typeof read>]>([
    ["Done over every mark", { ...everything, state: "done" }, ["done", null, "issue"]],
    ["needs-user whatever else is true", everything, ["your-move", "needs-user", "issue"]],
    [
      "a pending request over the waiting label",
      { ...everything, labels: ["waiting"] },
      ["your-move", "thread-waits-on-user", "thread:t"],
    ],
    [
      "waiting even while a thread works on it",
      { ...stalled, labels: ["waiting"], workingNow: true },
      ["waiting", "waiting", "issue"],
    ],
    [
      "work in flight over a pull request that is reviewed and held",
      { ...stalled, workingNow: true, pullRequests: [reviewed(8, "success")] },
      ["agents-working", "status:in-progress", "issue"],
    ],
    [
      "in review over reviewed, with two pull requests in different states",
      { pullRequests: [reviewed(8, "success"), groupPr(6, { reviewMark: "pending" })] },
      ["agents-working", "status:in-review", "issue"],
    ],
    [
      "checks running on one reviewed pull request over another that is held",
      { pullRequests: [reviewed(8, "success"), reviewed(7, "pending")] },
      ["agents-working", "reviewed-checks-running", "pr:7"],
    ],
    [
      "a reviewed pull request over the Issue's blockers",
      { ...stalled, openBlockerCount: 1, pullRequests: [reviewed(8, "failure")] },
      ["your-move", "reviewed-not-merged", "pr:8"],
    ],
    [
      "a blocked Issue with an open pull request is Blocked, not Stalled",
      { ...stalled, openBlockerCount: 2 },
      ["blocked", "blocked:2", "issue"],
    ],
    ["Stalled over Nobody on it", stalled, ["stalled", "status:waiting-for-review", "pr:9"]],
  ])("%s", (_label, overrides, expected) => {
    expect(read(issueGroupOf(groupInput(overrides)))).toEqual(expected);
  });
});

describe("issueGroupOf: what never makes it the user's move", () => {
  it.each<[string, Partial<GroupInput>, ReturnType<typeof read>]>([
    [
      "a draft with a review mark success, whatever its checks: paused work, not a merge to wait for",
      { pullRequests: [groupPr(7, { isDraft: true, reviewMark: "success", checks: "success" })] },
      ["stalled", "status:paused", "pr:7"],
    ],
    [
      "a reviewed pull request whose checks are unknown, as from an older server",
      { pullRequests: [reviewed(7)] },
      ["stalled", "status:waiting-for-merge", "pr:7"],
    ],
    [
      "a reviewed draft beside a reviewed pull request whose checks are unknown: the ready one is named and opened",
      {
        pullRequests: [
          groupPr(6, { isDraft: true, reviewMark: "success", checks: "success" }),
          reviewed(7),
        ],
      },
      ["stalled", "status:waiting-for-merge", "pr:7"],
    ],
    [
      "unknown checks beside a pull request whose checks finished: the finished one decides",
      { pullRequests: [reviewed(7), reviewed(8, "failure")] },
      ["your-move", "reviewed-not-merged", "pr:8"],
    ],
    [
      "a pending request on an archived thread",
      { threads: [linked("gone", { archived: true, waitsOnUser: "gone" })] },
      ["stalled", "last-thread-archived", "thread:gone"],
    ],
    [
      "a mark the caller did not trust",
      { pullRequests: [groupPr(7, { reviewMark: null, checks: "success" })] },
      ["stalled", "status:waiting-for-review", "pr:7"],
    ],
  ])("%s", (_label, overrides, expected) => {
    expect(read(issueGroupOf(groupInput(overrides)))).toEqual(expected);
  });

  it("names the status issueStatusOf gives the same inputs", () => {
    const input = groupInput({ threads: [linked("t")], pullRequests: [groupPr(7)] });
    expect(issueGroupOf(input).status).toBe(
      issueStatusOf({ ...input, linkedThreadCount: input.threads.length }),
    );
  });
});

describe("trustedReviewMark", () => {
  const trusted = new Set(["lukemaj", "other-machine"]);

  it.each([
    ["the account of a server that listed it", { state: "success", creator: "LukeMaj" }, "success"],
    ["another listing server's account", { state: "pending", creator: "other-machine" }, "pending"],
    ["anyone else", { state: "success", creator: "github-actions[bot]" }, null],
    ["a deleted account", { state: "failure", creator: null }, null],
    ["no mark", null, null],
  ] as const)("counts a mark from %s", (_label, review, expected) => {
    expect(trustedReviewMark(review, trusted)).toBe(expected);
  });
});
