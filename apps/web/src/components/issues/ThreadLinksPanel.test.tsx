import {
  EnvironmentId,
  type IssueStatesResult,
  ThreadId,
  type ThreadIssueLink,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";

import { ThreadLinksPanel } from "./ThreadLinksPanel";

const testState = vi.hoisted(() => ({
  // The states queries' results, one per chunk of at most 50 linked Issues.
  results: [] as ReadonlyArray<AsyncResult.AsyncResult<IssueStatesResult, unknown>>,
}));

vi.mock("@effect/atom-react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@effect/atom-react")>()),
  useAtomValue: () => testState.results,
}));

const environmentId = EnvironmentId.make("local");
const threadRef = { environmentId, threadId: ThreadId.make("thread-1") };

const issue = (number: number): ThreadIssueLink => ({
  host: "github.com",
  repository: "acme/web",
  number,
  url: `https://github.com/acme/web/issues/${number}`,
  sources: ["agent"],
  linkedAt: "2026-09-29T09:00:00.000Z",
});
// 1 open, 2 closed as done, 3 closed as not planned, 4 missing from GitHub's answer.
const issues = [issue(1), issue(2), issue(3), issue(4)];

const openPullRequest: ThreadPullRequestLink = {
  host: "github.com",
  repository: "acme/web",
  number: 70,
  url: "https://github.com/acme/web/pull/70",
  source: "created",
  linkedAt: "2026-09-29T09:00:00.000Z",
  stack: null,
  snapshot: {
    state: "open",
    title: "Change",
    headBranch: "fix/69-state",
    baseBranch: "main",
    isDraft: false,
    updatedAt: "2026-09-29T09:00:00.000Z",
    syncedAt: "2026-09-29T09:00:00.000Z",
  },
};

vi.mock("~/state/entities", () => ({
  useThreadShell: () => ({ branch: "fix/69-state", pullRequests: [openPullRequest] }),
  useServerConfigs: () =>
    new Map([["local", { environment: { capabilities: { threadPullRequests: true } } }]]),
  useProjects: () => [],
}));
vi.mock("~/state/threadDescendants", () => ({ useDescendantThreadShells: () => [] }));
vi.mock("./useThreadIssueLinks", () => ({
  useThreadIssueLinks: () => ({ links: issues, error: null }),
}));
vi.mock("./useStartThreadFromIssue", () => ({
  useStartThreadFromIssue: () => ({
    resolve: () => ({ reason: "Not in this test" }),
    start: vi.fn(),
  }),
}));
vi.mock("./useOpenIssueOrPullRequestLink", () => ({ useOpenIssueInIssuesView: () => vi.fn() }));
vi.mock("~/state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("~/lib/openPullRequestLink", () => ({ useOpenPrLink: () => vi.fn() }));

/** Each Issue row's state icon label by Issue number, and the footer's counts. */
function renderPanel() {
  const markup = renderToStaticMarkup(<ThreadLinksPanel threadRef={threadRef} issueLinks />);
  const rows = new Map(
    markup
      .split("group/issue-row")
      .slice(1)
      .map((row) => {
        const icon = /aria-label="([^"]+)"/u.exec(row)?.[1];
        const number = /aria-label="Open acme\/web#(\d+) in the right panel"/u.exec(row)?.[1];
        return [Number(number), icon] as const;
      }),
  );
  const footer = /(\d+) open · (\d+) linked/u.exec(markup.replaceAll("<!-- -->", ""));
  return { rows, footer: footer?.slice(1).map(Number) };
}

const read = (entries: IssueStatesResult["issues"]) => AsyncResult.success({ issues: entries });

describe("ThreadLinksPanel Issue states", () => {
  it("shows every Issue as not read yet before the first read, and counts none open", () => {
    testState.results = [AsyncResult.initial(true)];
    const { rows, footer } = renderPanel();
    expect([...rows.values()]).toEqual(Array(4).fill("State not read yet"));
    // Only the open pull request counts; the four Issues are linked but not known open.
    expect(footer).toEqual([1, 5]);
  });

  it("shows every Issue as unknown when the read fails, and counts none open", () => {
    testState.results = [AsyncResult.failure(Cause.fail(new Error("API rate limit exceeded")))];
    const { rows, footer } = renderPanel();
    for (const icon of rows.values()) expect(icon).toMatch(/^State unknown/u);
    expect(footer).toEqual([1, 5]);
  });

  it("shows GitHub's state per Issue, a missing Issue as unknown, and counts open ones", () => {
    testState.results = [
      read([
        { host: "github.com", repository: "acme/web", number: 1, state: "open" },
        { host: "github.com", repository: "acme/web", number: 2, state: "done" },
        { host: "github.com", repository: "acme/web", number: 3, state: "not-planned" },
        { host: "github.com", repository: "acme/web", number: 4, state: null },
      ]),
    ];
    const { rows, footer } = renderPanel();
    expect(rows.get(1)).toBe("Open");
    expect(rows.get(2)).toBe("Done");
    expect(rows.get(3)).toBe("Not planned");
    expect(rows.get(4)).toMatch(/^State unknown/u);
    expect(footer).toEqual([2, 5]);
  });

  it("shows an Issue closed after linking as closed once the next read says so", () => {
    testState.results = [
      read(
        issues.map(({ host, repository, number }) => ({ host, repository, number, state: "open" })),
      ),
    ];
    const before = renderPanel();
    expect(before.rows.get(2)).toBe("Open");
    expect(before.footer).toEqual([5, 5]);

    testState.results = [
      read(
        issues.map(({ host, repository, number }) => ({
          host,
          repository,
          number,
          state: number === 2 ? "done" : "open",
        })),
      ),
    ];
    const after = renderPanel();
    expect(after.rows.get(2)).toBe("Done");
    expect(after.footer).toEqual([4, 5]);
  });
});
