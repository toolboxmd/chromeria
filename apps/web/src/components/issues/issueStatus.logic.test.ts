import {
  EnvironmentId,
  type IssueGroup,
  type IssuePullRequest,
  issueGroupOf,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  groupIssues,
  issueGroupInputOf,
  issuePanelPullRequests,
  issueThreadTargets,
  matchesIssueGroupFilters,
  mergeIssueRowThreads,
  isThreadShellWorking,
  waitingThreadKeysOf,
  waitingThreadsOf,
  workingThreadKeysOf,
  type IssueRowThread,
} from "./issueStatus.logic";

const LOCAL = EnvironmentId.make("local");
const REMOTE = EnvironmentId.make("remote");
const REPOSITORY = "toolboxmd/t3code";

function pullRequest(number: number, overrides: Partial<IssuePullRequest> = {}): IssuePullRequest {
  return {
    host: "github.com",
    repository: REPOSITORY,
    number,
    url: `https://github.com/${REPOSITORY}/pull/${number}`,
    state: "open",
    isDraft: false,
    headRefName: `feat/${number}-x`,
    headSha: "abc",
    review: null,
    ...overrides,
  };
}

function thread(id: string, overrides: Partial<IssueRowThread> = {}): IssueRowThread {
  return {
    environmentId: LOCAL,
    id: ThreadId.make(id),
    title: id,
    archivedAt: null,
    sources: ["manual"],
    pullRequests: [],
    ...overrides,
  };
}

const openIssue = {
  state: "open" as const,
  openBlockerCount: 0,
  closingPullRequests: [],
  labels: [],
};
const label = (name: string) => ({ name, color: "ededed" });
const context = (overrides: Partial<Parameters<typeof issueGroupInputOf>[2]> = {}) => ({
  working: new Set<string>(),
  waiting: new Map<string, ThreadId>(),
  linkedPullRequests: new Map<string, IssuePullRequest>(),
  trustedLogins: new Set(["lukemaj"]),
  ...overrides,
});
const groupingOf = (...args: Parameters<typeof issueGroupInputOf>) =>
  issueGroupOf(issueGroupInputOf(...args));
const statusOf = (...args: Parameters<typeof issueGroupInputOf>) => groupingOf(...args).status;

describe("matchesIssueGroupFilters", () => {
  it.each([
    [{}, "nobody-on-it", 0, true],
    [{ groups: [] }, "stalled", 1, true],
    [{ groups: ["stalled", "blocked"] }, "blocked", 0, true],
    [{ groups: ["stalled"] }, "blocked", 0, false],
    [{ linked: "linked" }, "your-move", 1, true],
    [{ linked: "linked" }, "nobody-on-it", 0, false],
    [{ linked: "unlinked" }, "waiting", 0, true],
    [{ linked: "unlinked" }, "stalled", 2, false],
    [{ groups: ["blocked"], linked: "linked" }, "blocked", 0, false],
  ] as const)("%j keeps %s with %i threads: %s", (filters, group, linkedThreadCount, kept) => {
    expect(matchesIssueGroupFilters({ group, linkedThreadCount }, filters)).toBe(kept);
  });
});

describe("groupIssues", () => {
  it("orders groups by whose move it is and keeps each group's row order", () => {
    const rows: Array<[string, IssueGroup]> = [
      ["a", "nobody-on-it"],
      ["b", "done"],
      ["c", "stalled"],
      ["d", "nobody-on-it"],
      ["e", "waiting"],
      ["f", "agents-working"],
      ["g", "your-move"],
      ["h", "blocked"],
    ];
    const groups = groupIssues(rows, ([, group]) => group);
    expect(groups.map((group) => [group.group, group.entries.map(([name]) => name)])).toEqual([
      ["your-move", ["g"]],
      ["agents-working", ["f"]],
      ["waiting", ["e"]],
      ["blocked", ["h"]],
      ["stalled", ["c"]],
      ["nobody-on-it", ["a", "d"]],
      ["done", ["b"]],
    ]);
  });
});

describe("issueGroupInputOf: pull requests", () => {
  it("counts a thread's PR into a non-default branch, which closes nothing on GitHub", () => {
    const linked = new Map([["github.com toolboxmd/t3code#36", pullRequest(36)]]);
    const threads = [
      thread("t", { pullRequests: [{ host: "github.com", repository: REPOSITORY, number: 36 }] }),
    ];
    expect(statusOf(openIssue, threads, context())).toBe("discussion");
    expect(statusOf(openIssue, threads, context({ linkedPullRequests: linked }))).toBe(
      "waiting-for-review",
    );
  });

  it("counts a pull request both closing and thread-linked once, closing read first", () => {
    const closing = pullRequest(7, { review: { state: "failure", creator: "lukemaj" } });
    const input = issueGroupInputOf(
      { ...openIssue, closingPullRequests: [closing] },
      [
        thread("t", {
          pullRequests: [{ host: "GitHub.com", repository: "Toolboxmd/T3code", number: 7 }],
        }),
      ],
      context({
        linkedPullRequests: new Map([
          ["github.com toolboxmd/t3code#7", pullRequest(7, { review: null })],
        ]),
      }),
    );
    expect(input.pullRequests).toEqual([
      { ref: closing, state: "open", isDraft: false, reviewMark: "failure", checks: undefined },
    ]);
  });

  it("trusts a mark from any server that listed the Issue, and no one else", () => {
    const marked = pullRequest(7, { review: { state: "success", creator: "Other-Machine" } });
    const issue = { ...openIssue, closingPullRequests: [marked] };
    expect(statusOf(issue, [], context())).toBe("waiting-for-review");
    expect(
      statusOf(issue, [], context({ trustedLogins: new Set(["lukemaj", "other-machine"]) })),
    ).toBe("waiting-for-merge");
  });

  it("keeps a reviewed pull request with the agents while its head's checks run", () => {
    const reviewed = (checks?: IssuePullRequest["checks"]) =>
      pullRequest(7, {
        review: { state: "success", creator: "lukemaj" },
        ...(checks === undefined ? {} : { checks }),
      });
    const read = (closing: IssuePullRequest) => {
      const { group, reason, target } = groupingOf(
        { ...openIssue, closingPullRequests: [closing] },
        [],
        context(),
      );
      return [group, reason?.kind, target.kind === "pull-request" ? target.pullRequest : null];
    };
    const running = reviewed("pending");
    expect(read(running)).toEqual(["agents-working", "reviewed-checks-running", running]);
    // Checks an older server did not read are unknown, not running: the fact still stands.
    const unread = reviewed();
    expect(read(unread)).toEqual(["your-move", "reviewed-not-merged", unread]);
  });
});

describe("issueGroupInputOf: labels", () => {
  it("reads the two marks from the row's labels, over what its threads do", () => {
    const working = new Set(["local:t"]);
    const threads = [thread("t", { sources: ["branch"] })];
    const groupOf = (...names: ReadonlyArray<string>) =>
      groupingOf({ ...openIssue, labels: names.map(label) }, threads, context({ working })).group;
    expect(groupOf("bug")).toBe("agents-working");
    expect(groupOf("bug", "waiting")).toBe("waiting");
    expect(groupOf("waiting", "needs-user")).toBe("your-move");
  });
});

describe("issueThreadTargets", () => {
  it("asks every server that keeps links about every row, in batches of 100", () => {
    const closing = pullRequest(40, { state: "merged" });
    const entries = [
      { host: "github.com", repository: REPOSITORY, number: 1, closingPullRequests: [closing] },
      ...Array.from({ length: 150 }, (_, index) => ({
        host: "github.com",
        repository: REPOSITORY,
        number: index + 2,
        closingPullRequests: [],
      })),
    ];
    const targets = issueThreadTargets(entries, new Set([REMOTE, LOCAL]));
    expect(targets.map((target) => [target.environmentId, target.input.issues.length])).toEqual([
      [LOCAL, 100],
      [LOCAL, 51],
      [REMOTE, 100],
      [REMOTE, 51],
    ]);
    expect(targets[0]!.input.issues[0]).toEqual({
      host: "github.com",
      repository: REPOSITORY,
      number: 1,
      closingPullRequests: [{ repository: REPOSITORY, number: 40 }],
    });
    expect(issueThreadTargets(entries, new Set())).toEqual([]);
  });
});

describe("mergeIssueRowThreads", () => {
  it("keys threads by Issue across servers, each thread once", () => {
    const linked = {
      id: ThreadId.make("t1"),
      projectId: ProjectId.make("p"),
      title: "Build it",
      archivedAt: null,
      sources: ["branch" as const],
      pullRequests: [],
    };
    const answer = (repository: string) => ({
      issues: [{ host: "github.com", repository, number: 5, threads: [linked] }],
    });
    const merged = mergeIssueRowThreads([
      [LOCAL, answer("Toolboxmd/T3code")],
      [LOCAL, answer(REPOSITORY)],
      [REMOTE, answer(REPOSITORY)],
    ]);
    expect(merged.get("github.com toolboxmd/t3code#5")?.map((row) => row.environmentId)).toEqual([
      LOCAL,
      REMOTE,
    ]);
  });
});

type WorkingShell = Parameters<typeof workingThreadKeysOf>[0][number];

function shell(
  id: string,
  overrides: Partial<WorkingShell> & { readonly parent?: string; readonly fork?: boolean } = {},
): WorkingShell {
  const { parent, fork, ...rest } = overrides;
  return {
    environmentId: LOCAL,
    id: ThreadId.make(id),
    archivedAt: null,
    runtime: null,
    pendingBackgroundTasks: [],
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    lineage: {
      parentThreadId: parent === undefined ? null : ThreadId.make(parent),
      relationshipToParent: parent === undefined ? null : fork ? "fork" : "subagent",
      rootThreadId: ThreadId.make(parent ?? id),
    },
    ...rest,
  };
}

describe("waiting on the user", () => {
  const asking = { hasPendingApprovals: true };

  it("names the thread that holds the request for itself and for every thread that spawned it", () => {
    expect(
      waitingThreadKeysOf([
        shell("own", { hasPendingUserInput: true }),
        shell("parent"),
        shell("child", { parent: "parent" }),
        shell("grandchild", { parent: "child", ...asking }),
        shell("idle"),
      ]).split("\n"),
    ).toEqual([
      "local:child\tgrandchild",
      "local:grandchild\tgrandchild",
      "local:own\town",
      "local:parent\tgrandchild",
    ]);
    expect(waitingThreadKeysOf([shell("idle")])).toBe("");
  });

  it("opens a thread that waits itself rather than one it spawned", () => {
    const waiting = waitingThreadsOf(
      waitingThreadKeysOf([
        shell("parent", asking),
        shell("a-child", { parent: "parent", ...asking }),
      ]),
    );
    expect(waiting.get("local:parent")).toBe("parent");
  });

  it("counts no archived thread's request, and a fork's request for the fork alone", () => {
    expect(
      waitingThreadKeysOf([
        shell("gone", { ...asking, archivedAt: "2026-09-25T10:00:00Z" }),
        shell("original"),
        shell("fork", { parent: "original", fork: true, ...asking }),
      ]),
    ).toBe("local:fork\tfork");
  });

  // The Issue's repository is listed by two servers; its row is the local server's, while the
  // thread working on it, and the thread that one spawned, live on the remote one.
  it("moves an Issue to Your move for a request on a remote server's spawned thread", () => {
    const waiting = waitingThreadsOf(
      waitingThreadKeysOf([
        shell("parent", { environmentId: REMOTE }),
        shell("job", { environmentId: REMOTE, parent: "parent", ...asking }),
      ]),
    );
    const linked = thread("parent", { sources: ["branch"] });
    // The same thread id on the local server asks nothing.
    expect(groupingOf(openIssue, [linked], context({ waiting })).group).toBe("stalled");
    const { group, reason, target } = groupingOf(
      openIssue,
      [linked, { ...linked, environmentId: REMOTE }],
      context({ waiting }),
    );
    expect([group, reason, target]).toEqual([
      "your-move",
      { kind: "thread-waits-on-user" },
      { kind: "thread", thread: { environmentId: REMOTE, id: "job" } },
    ]);
  });

  it("opens the thread to resume for a stalled row, and the archived one when none is left", () => {
    const targetOf = (...threads: ReadonlyArray<IssueRowThread>) => {
      const { reason, target } = groupingOf(openIssue, threads, context());
      return [reason?.kind, target.kind === "thread" ? target.thread.id : null];
    };
    const archived = thread("old", { archivedAt: "2026-09-25T10:00:00Z" });
    expect(targetOf(archived, thread("live"))).toEqual(["status", "live"]);
    expect(targetOf(archived)).toEqual(["last-thread-archived", "old"]);
  });
});

describe("working now", () => {
  const runtime = (status: NonNullable<WorkingShell["runtime"]>["status"]) => ({
    status,
    activeRunId: null,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerName: "Codex",
    lastError: null,
    updatedAt: "2026-09-25T10:00:00.000Z",
  });
  const running = runtime("running");
  const task = (kind: "subagent" | "command" | "monitor" | "background_task") =>
    ({ kind }) as unknown as WorkingShell["pendingBackgroundTasks"][number];
  const keysOf = (shells: ReadonlyArray<WorkingShell>) =>
    new Set(
      workingThreadKeysOf(shells)
        .split("\n")
        .filter((key) => key.length > 0),
    );

  it("counts a thread whose own run is in flight, or whose spawned thread works", () => {
    expect(
      workingThreadKeysOf([
        shell("own", { runtime: running }),
        shell("parent"),
        shell("child", { parent: "parent" }),
        shell("grandchild", { parent: "child", runtime: running }),
        shell("idle"),
      ]).split("\n"),
    ).toEqual(["local:child", "local:grandchild", "local:own", "local:parent"]);
    expect(workingThreadKeysOf([shell("idle")])).toBe("");
  });

  it("does not count a thread for work in its conversation fork", () => {
    expect(
      keysOf([
        shell("original"),
        shell("fork", { parent: "original", fork: true, runtime: running }),
      ]),
    ).toEqual(new Set(["local:fork"]));
  });

  it("reads working from the run and the agents it left running, not from waiting on the user", () => {
    for (const status of ["preparing", "starting", "running", "waiting"] as const) {
      expect(isThreadShellWorking(shell("t", { runtime: runtime(status) }))).toBe(true);
    }
    for (const status of ["queued", "completed", "failed", "idle"] as const) {
      expect(isThreadShellWorking(shell("t", { runtime: runtime(status) }))).toBe(false);
    }
    expect(isThreadShellWorking(shell("t", { pendingBackgroundTasks: [task("subagent")] }))).toBe(
      true,
    );
    expect(
      isThreadShellWorking(shell("t", { pendingBackgroundTasks: [task("background_task")] })),
    ).toBe(true);
    expect(
      isThreadShellWorking(
        shell("t", { pendingBackgroundTasks: [task("monitor"), task("command")] }),
      ),
    ).toBe(false);
    expect(isThreadShellWorking(shell("t", { runtime: running, hasPendingApprovals: true }))).toBe(
      false,
    );
    expect(
      isThreadShellWorking(shell("t", { runtime: running, archivedAt: "2026-09-25T10:00:00Z" })),
    ).toBe(false);
  });

  it("feeds the status from whichever server the linked thread lives on", () => {
    const working = keysOf([
      shell("parent", { environmentId: REMOTE }),
      shell("job", { environmentId: REMOTE, parent: "parent", runtime: running }),
    ]);
    const branchThread = thread("parent", { sources: ["branch"] });
    // Only the remote machine's thread works; its row counts once the remote server answers.
    expect(statusOf(openIssue, [branchThread], context({ working }))).toBe("paused");
    expect(
      statusOf(
        openIssue,
        [branchThread, { ...branchThread, environmentId: REMOTE }],
        context({ working }),
      ),
    ).toBe("in-progress");
  });
});

describe("issuePanelPullRequests", () => {
  it("lists closing PRs, then thread PRs once each, with state where the list read it", () => {
    const threads = [
      thread("a", {
        pullRequests: [
          { host: "github.com", repository: "Toolboxmd/T3code", number: 7 },
          { host: "github.com", repository: REPOSITORY, number: 36 },
        ],
      }),
      thread("b", { pullRequests: [{ host: "github.com", repository: REPOSITORY, number: 50 }] }),
    ];
    const listed = issuePanelPullRequests(
      [pullRequest(7, { state: "merged" })],
      threads,
      new Map([["github.com toolboxmd/t3code#36", pullRequest(36, { isDraft: true })]]),
    );
    expect(listed.map((pr) => [pr.number, pr.state, pr.isDraft, pr.url])).toEqual([
      [7, "merged", false, "https://github.com/toolboxmd/t3code/pull/7"],
      [36, "open", true, "https://github.com/toolboxmd/t3code/pull/36"],
      // Not read by the list (e.g. opened from a thread's link): shown without a state.
      [50, null, false, "https://github.com/toolboxmd/t3code/pull/50"],
    ]);
  });
});
