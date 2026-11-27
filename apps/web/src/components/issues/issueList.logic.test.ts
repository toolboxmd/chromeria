import type {
  EnvironmentId,
  GitHubIssueCountsResult,
  IssueLink,
  GitHubIssueListEntry,
  GitHubIssueListResult,
  ProjectId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildIssueTree,
  collectIssueFacets,
  issueCountCertainty,
  issueCountsTotal,
  issueCountTargets,
  issueKey,
  issueListingOf,
  issueListingScopes,
  issueListPages,
  issueNumbersLine,
  matchesIssueFilters,
  mergeIssueLists,
  repositoryKey,
  sortIssues,
  staleIssueCount,
  type EnvironmentIssueEntry,
  type IssueListing,
  type IssueListingScope,
  type IssueListTarget,
  type IssueTreeNode,
  environmentIdsWithCapability,
} from "./issueList.logic";

const ENV_A = "env-a" as EnvironmentId;
const ENV_B = "env-b" as EnvironmentId;

function link(number: number, overrides: Partial<IssueLink> = {}): IssueLink {
  const repository = overrides.repository ?? "toolboxmd/t3code";
  return {
    host: "github.com",
    repository,
    number,
    title: `Issue ${number}`,
    url: `https://github.com/${repository}/issues/${number}`,
    state: "open",
    ...overrides,
  };
}

function entry(
  number: number,
  overrides: Partial<EnvironmentIssueEntry> = {},
): EnvironmentIssueEntry {
  return {
    ...link(number, overrides),
    environmentId: ENV_A,
    projectId: "project-1" as ProjectId,
    projectTitle: "t3code",
    author: "lukemaj",
    labels: [],
    milestone: null,
    commentCount: 0,
    createdAt: `2026-09-${String(number).padStart(2, "0")}T10:00:00Z`,
    updatedAt: "2026-09-25T10:00:00Z",
    parent: null,
    subIssues: [],
    subIssueCount: 0,
    openBlockerCount: 0,
    closingPullRequests: [],
    ...overrides,
  };
}

function result(
  entries: ReadonlyArray<GitHubIssueListEntry>,
  overrides: Partial<GitHubIssueListResult> = {},
): GitHubIssueListResult {
  return {
    repositories: [
      {
        host: "github.com",
        repository: "toolboxmd/t3code",
        projectId: "project-1" as ProjectId,
        projectTitle: "t3code",
      },
    ],
    unsupported: [],
    viewers: [{ host: "github.com", login: "lukemaj" }],
    linkedPullRequests: [],
    errors: [],
    entries,
    nextCursors: {},
    ...overrides,
  };
}

const strip = ({ environmentId: _environmentId, ...rest }: EnvironmentIssueEntry) => rest;

describe("mergeIssueLists", () => {
  it("is null before any server answered", () => {
    expect(mergeIssueLists([])).toBeNull();
  });

  it("keeps the first server's copy of an Issue two servers both list", () => {
    const merged = mergeIssueLists([
      [ENV_A, result([strip(entry(1)), strip(entry(2))])],
      [ENV_B, result([strip(entry(2)), strip(entry(3))])],
    ]);
    expect(merged?.entries.map((row) => [row.number, row.environmentId])).toEqual([
      [1, ENV_A],
      [2, ENV_A],
      [3, ENV_B],
    ]);
    expect(merged?.repositories).toHaveLength(1);
  });

  it("walks a server's continuation forward and drops it once exhausted", () => {
    const listing = { environmentId: ENV_A, input: { state: "open", limit: 1 } } as const;
    const continuation = {
      environmentId: ENV_A,
      input: { state: "open", limit: 1, cursors: { "github.com#0": "c1" } },
    } as const;
    const matchCounts = { "github.com#0": 2 };
    const pageOne = result([strip(entry(3))], {
      nextCursors: { "github.com#0": "c1" },
      matchCounts,
    });
    const pageTwo = result([strip(entry(2))], { nextCursors: {}, matchCounts });
    const answers = new Map([[JSON.stringify(listing), pageOne]]);
    const answerOf = (target: unknown) => answers.get(JSON.stringify(target)) ?? null;

    // The first page asks for its continuation unprompted; until that answers, rows are missing.
    expect(issueListPages([listing], answerOf)).toEqual({
      targets: [listing, continuation],
      rows: "pending",
    });
    const first = mergeIssueLists([[ENV_A, pageOne]]);
    expect(first?.nextCursors.get(ENV_A)).toEqual({ "github.com#0": "c1" });

    answers.set(JSON.stringify(continuation), pageTwo);
    expect(issueListPages([listing], answerOf)).toEqual({
      targets: [listing, continuation],
      rows: "complete",
    });
    const second = mergeIssueLists([
      [ENV_A, pageOne],
      [ENV_A, pageTwo],
    ]);
    expect(second?.entries.map((row) => row.number)).toEqual([3, 2]);
    expect(second?.nextCursors.has(ENV_A)).toBe(false);
  });

  it("reads no cursor of a search twice, and a search that repeats one is not complete", () => {
    const listingA = { environmentId: ENV_A, input: { state: "open" } } as const;
    const listingB = { environmentId: ENV_B, input: { state: "open" } } as const;
    // ENV_A's first search answers every page with the cursor it was read from while its second
    // search advances once and ends; ENV_B has one page and is done.
    const cursorsOf = (target: IssueListTarget) =>
      target.environmentId === ENV_B
        ? {}
        : target.input.cursors === undefined
          ? { "github.com#0": "stuck", "github.com#1": "next" }
          : { "github.com#0": "stuck" };
    const pages = issueListPages([listingA, listingB], (target) =>
      result([], { nextCursors: cursorsOf(target), matchCounts: {} }),
    );
    expect(pages.targets).toEqual([
      listingA,
      listingB,
      {
        environmentId: ENV_A,
        input: { state: "open", cursors: { "github.com#0": "stuck", "github.com#1": "next" } },
      },
    ]);
    expect(pages.rows).toBe("truncated");
  });

  it("calls a list truncated only when GitHub matched more Issues than it returned", () => {
    const listing = { environmentId: ENV_A, input: { state: "all", limit: 100 } } as const;
    const rows = (count: number) => Array.from({ length: count }, () => strip(entry(1)));
    // One search read page by page: `sizes` rows per page, against GitHub's match count.
    const walk = (sizes: ReadonlyArray<number>, matchCounts: Record<string, number> | undefined) =>
      issueListPages([listing], (target) => {
        const page = Number(target.input.cursors?.["github.com#0"] ?? 0);
        return result(rows(sizes[page] ?? 0), {
          nextCursors: page + 1 < sizes.length ? { "github.com#0": String(page + 1) } : {},
          ...(matchCounts === undefined ? {} : { matchCounts }),
        });
      }).rows;
    const full = (pages: number) => Array.from({ length: pages }, () => 100);

    // 901 matches end on a short tenth page: all read.
    expect(walk([...full(9), 1], { "github.com#0": 901 })).toBe("complete");
    // GitHub stops after 1,000 rows however many match.
    expect(walk(full(10), { "github.com#0": 1500 })).toBe("truncated");
    // The same cut-off search beside a second search read in full: its 50 rows hide nothing.
    expect(walk([150, ...full(9)], { "github.com#0": 1500, "github.com#1": 50 })).toBe("truncated");
    expect(walk([150, ...full(9)], { "github.com#0": 1000, "github.com#1": 50 })).toBe("complete");
    // A server that predates match counts cannot say either way.
    expect(walk([100, 20], undefined)).toBe("unknown");
  });

  it("counts each repository on another forge once, across pages and servers", () => {
    const merged = mergeIssueLists([
      [ENV_A, result([], { unsupported: [{ host: "gitlab.com", repository: "a/one" }] })],
      [ENV_A, result([], { unsupported: [{ host: "gitlab.com", repository: "a/one" }] })],
      [
        ENV_B,
        result([], {
          unsupported: [
            { host: "GitLab.com", repository: "A/One" },
            { host: "gitlab.com", repository: "a/two" },
          ],
        }),
      ],
    ]);
    expect(merged?.unsupported).toEqual([{ host: "gitlab.com", repositoryCount: 2 }]);
  });
});

describe("the line of numbers", () => {
  const listed = (repository: string) => ({
    host: "github.com",
    repository,
    projectId: "project-1" as ProjectId,
    projectTitle: repository,
  });
  const counted = (open: number, filed: number, closed: number): GitHubIssueCountsResult => ({
    open,
    filed,
    closed,
  });
  const SHARED = "github.com acme/shared";
  const ONLY_A = "github.com acme/only-a";
  const ONLY_B = "github.com acme/only-b";
  /** Both servers list Shared; each lists one repository more. */
  const answered = (environmentId: EnvironmentId, repositories: ReadonlyArray<string>) =>
    ({ environmentId, state: "answered", repositories }) as const;
  const scope = (
    environmentId: EnvironmentId,
    repositories: ReadonlyArray<string> | null,
    answering = true,
  ) => ({ environmentId, repositories, answering });
  const BOTH = [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, [ONLY_B, SHARED])];
  const target = (environmentId: EnvironmentId, ...repositories: ReadonlyArray<string>) => ({
    environmentId,
    input: { repositories },
  });

  it.each([
    [
      "a connected server that answered",
      {
        connected: true,
        answer: { repositories: [listed("acme/Web"), listed("acme/api")], errors: [] },
      },
      { state: "answered", repositories: ["github.com acme/api", "github.com acme/web"] },
    ],
    [
      // The RPC succeeds and still names the repositories; the view shows the refusal with Retry.
      "a server whose answer carries GitHub's refusal of a search",
      {
        connected: true,
        answer: {
          repositories: [listed("acme/api")],
          errors: [{ host: "github.com", message: "Bad credentials" }],
        },
      },
      { state: "failed", repositories: ["github.com acme/api"] },
    ],
    ["a connected server yet to answer", { connected: true }, { state: "pending" }],
    [
      "a failed read with no answer before it",
      { connected: true, failed: true },
      { state: "failed" },
    ],
    [
      "a failed reread that left its answer behind",
      { connected: true, failed: true, answer: { repositories: [listed("acme/api")], errors: [] } },
      { state: "failed", repositories: ["github.com acme/api"] },
    ],
    [
      // Its reads wait on the connection with their last answer, so the listing itself still
      // looks answered.
      "a server that dropped its connection after answering",
      { connected: false, answer: { repositories: [listed("acme/api")], errors: [] } },
      { repositories: ["github.com acme/api"] },
    ],
    ["a server that is not connected and never answered", { connected: false, failed: true }, {}],
  ] as const)("reads the listing of %s", (_name, input, expected) => {
    expect(issueListingOf({ environmentId: ENV_A, answer: null, failed: false, ...input })).toEqual(
      { environmentId: ENV_A, state: "disconnected", repositories: null, ...expected },
    );
  });

  const pendingB = { environmentId: ENV_B, state: "pending", repositories: null } as const;
  const failedB = { environmentId: ENV_B, state: "failed", repositories: null } as const;
  const disconnectedB = {
    environmentId: ENV_B,
    state: "disconnected",
    repositories: null,
  } as const;
  it.each([
    [
      "each answering server's own listing",
      [answered(ENV_A, [ONLY_A, SHARED]), answered(ENV_B, [ONLY_B, SHARED])],
      [],
      BOTH,
    ],
    [
      // A new sort, search, filter or State reads the rows again, and the slower server's
      // listing says nothing for a while.
      "the scope held for a server whose listing is being read again",
      [answered(ENV_A, [ONLY_A, SHARED]), pendingB],
      BOTH,
      BOTH,
    ],
    [
      "unknown for a connected server that has never answered",
      [answered(ENV_A, [ONLY_A, SHARED]), pendingB],
      [],
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, null, false)],
    ],
    [
      "unknown for a connected server whose only read failed",
      [answered(ENV_A, [ONLY_A, SHARED]), failedB],
      [],
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, null, false)],
    ],
    [
      // The listing of a new row query has no answer of its own to keep.
      "the repositories a server that stopped answering last listed, and that it cannot be asked",
      [answered(ENV_A, [ONLY_A, SHARED]), disconnectedB],
      BOTH,
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, [ONLY_B, SHARED], false)],
    ],
    [
      // A machine switched off since the page opened adds no rows, so nothing of it is expected.
      "none for a server that is not connected and has listed nothing",
      [answered(ENV_A, [ONLY_A, SHARED]), disconnectedB],
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, null, false)],
      [scope(ENV_A, [ONLY_A, SHARED])],
    ],
    [
      // GitHub refuses the first server's searches. It still lists Shared, and cannot count it.
      "the repositories of a server whose listing carries a GitHub error, and that it cannot be asked",
      [
        { environmentId: ENV_A, state: "failed", repositories: [SHARED] },
        answered(ENV_B, [ONLY_B, SHARED]),
      ],
      [],
      [scope(ENV_A, [SHARED], false), scope(ENV_B, [ONLY_B, SHARED])],
    ],
    [
      "a new answer in place of the scope held, and no server that stopped listing",
      [answered(ENV_B, [SHARED])],
      BOTH,
      [scope(ENV_B, [SHARED])],
    ],
  ] as const)("a server's scope is %s", (_name, listings, held, expected) => {
    const scopes = issueListingScopes(listings, held);
    expect(scopes).toEqual(expected);
    // Worked out again from itself it does not move, so holding it settles.
    expect(issueListingScopes(listings, scopes)).toEqual(scopes);
  });

  it("gives a repository to the healthy server when GitHub refuses the first server's listing", () => {
    const refused = (repositories: ReadonlyArray<string>) =>
      issueListingOf({
        environmentId: ENV_A,
        connected: true,
        failed: false,
        answer: {
          repositories: repositories.map((key) => listed(key.split(" ")[1]!)),
          errors: [{ host: "github.com", message: "Bad credentials" }],
        },
      });
    const b = answered(ENV_B, [ONLY_B, SHARED]);
    // Both list Shared; the first, which would own it, cannot search GitHub.
    expect(issueCountTargets(issueListingScopes([refused([SHARED]), b], []))).toEqual([
      target(ENV_B, ONLY_B, SHARED),
    ]);
    // A repository only the refused server lists has nobody to count it.
    expect(issueCountTargets(issueListingScopes([refused([ONLY_A, SHARED]), b], []))).toBeNull();
  });

  it("counts what is listed while a server is switched off, and never keeps that total once it lists more", () => {
    const fromA = [ENV_A, counted(100, 9, 4)] as const;
    const fromB = [ENV_B, counted(13, 3, 5)] as const;
    const a = answered(ENV_A, [ONLY_A, SHARED]);
    /** The scopes, the reads they ask for, and the total those reads' answers give. */
    const step = (
      listings: ReadonlyArray<IssueListing>,
      held: ReadonlyArray<IssueListingScope>,
      answers: ReadonlyArray<readonly [EnvironmentId, GitHubIssueCountsResult]>,
    ) => {
      const scopes = issueListingScopes(listings, held);
      const targets = issueCountTargets(scopes);
      // A server whose read has no answer yet is being read.
      const reading = (targets ?? []).length > answers.length;
      return {
        scopes,
        targets,
        total: issueCountsTotal({ targets, answers, failed: false, reading }),
      };
    };

    // The second server has been off since the page opened: the first one's repositories are
    // the listed ones, and their counts are exact.
    const off = step([a, disconnectedB], [], [fromA]);
    expect(off.targets).toEqual([target(ENV_A, ONLY_A, SHARED)]);
    expect(off.total).toEqual({ ...counted(100, 9, 4), exact: true });

    // It connects: until its listing answers, what it lists is unknown, and so are the numbers.
    const connecting = step([a, pendingB], off.scopes, [fromA]);
    expect(connecting.total).toBeNull();

    // It lists a repository only it has: unknown until its own counts answer, then the larger
    // total. The first server is asked for nothing new.
    const b = answered(ENV_B, [ONLY_B, SHARED]);
    const listing = step([a, b], connecting.scopes, [fromA]);
    expect(listing.targets).toEqual([target(ENV_A, ONLY_A, SHARED), target(ENV_B, ONLY_B)]);
    expect(listing.total).toBeNull();
    expect(step([a, b], listing.scopes, [fromA, fromB]).total).toEqual({
      ...counted(113, 12, 9),
      exact: true,
    });
  });

  it.each([
    [
      "each repository of the first answering server that lists it",
      BOTH,
      [target(ENV_A, ONLY_A, SHARED), target(ENV_B, ONLY_B)],
    ],
    [
      "nothing of a server whose every repository another counts",
      [scope(ENV_A, [SHARED]), scope(ENV_B, [SHARED])],
      [target(ENV_A, SHARED)],
    ],
    [
      // An offline laptop that lists what an always-on machine lists holds nothing back.
      "the answering server alone when an unreachable one only duplicates it",
      [scope(ENV_A, [SHARED], false), scope(ENV_B, [ONLY_B, SHARED])],
      [target(ENV_B, ONLY_B, SHARED)],
    ],
    [
      "nothing at all while an unreachable server lists a repository no other does",
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, [ONLY_B, SHARED], false)],
      null,
    ],
    [
      "nothing at all while a server's repositories are unknown",
      [scope(ENV_A, [ONLY_A, SHARED]), scope(ENV_B, null, false)],
      null,
    ],
  ] as const)("the counts ask %s", (_name, scopes, expected) => {
    expect(issueCountTargets(scopes)).toEqual(expected);
    expect(issueCountTargets(scopes.toReversed())).toEqual(expected);
  });

  const targets = [target(ENV_A, ONLY_A), target(ENV_B, ONLY_B)];
  const answers = [
    [ENV_A, counted(100, 9, 4)],
    [ENV_B, counted(13, 3, 5)],
  ] as const;
  const read = { targets, answers, failed: false, reading: false };
  it.each([
    ["sums every server's answer", read, { ...counted(113, 12, 9), exact: true }],
    [
      // Refresh, or a stale view opened again: the answers shown are being replaced.
      "is not exact while a read is in flight over answers already in hand",
      { ...read, reading: true },
      { ...counted(113, 12, 9), exact: false },
    ],
    ["is unknown while the reads cannot cover the view", { ...read, targets: null }, null],
    [
      "is unknown while a server's counts have no answer, not the sum of the others",
      { ...read, answers: answers.slice(0, 1), reading: true },
      null,
    ],
    [
      // An older server without the query, a deadline passed, or a refresh that failed after an
      // answer: the answers a failed read leaves behind are not shown as current.
      "is unknown after a failed counts read, whatever answers remain",
      { ...read, failed: true },
      null,
    ],
    [
      "is zero, not unknown, when every counted repository has none",
      {
        ...read,
        answers: [
          [ENV_A, counted(0, 0, 0)],
          [ENV_B, counted(0, 0, 0)],
        ],
      },
      { ...counted(0, 0, 0), exact: true },
    ],
  ] as const)("the counts total %s", (_name, input, expected) => {
    expect(issueCountsTotal(input)).toEqual(expected);
  });

  const final = (count: number) => ({ count, certainty: "final" }) as const;
  const floor = (count: number) => ({ count, certainty: "floor" }) as const;
  const unsettled = (count: number) => ({ count, certainty: "unsettled" }) as const;
  const line = (overrides: Partial<Parameters<typeof issueNumbersLine>[0]>) =>
    issueNumbersLine({
      state: "open",
      searched: false,
      shown: final(113),
      loadedOpen: 113,
      rows: "complete",
      counted: final(113),
      ...overrides,
    });
  it.each([
    [
      "the whole open list counts its own rows, even past a counts read a moment older",
      line({ counted: final(112) }),
      { kind: "open", count: final(113) },
    ],
    [
      // Refresh reads the rows and the counts again together.
      "the whole open list being read again is not final",
      line({ rows: "refreshing", counted: unsettled(113) }),
      { kind: "open", count: unsettled(113) },
    ],
    [
      // The counts read failed or passed its deadline while a server that is away keeps its
      // pages waiting: no total is claimed, and none stays marked as being read.
      "rows being read again with no counted total claim none",
      line({ rows: "refreshing", counted: null }),
      { kind: "listed", count: final(113), open: null },
    ],
    [
      // An unreachable server keeps its pages waiting; the counts of a server that answers stand.
      "rows being read again give way to an exact counted total",
      line({ rows: "refreshing" }),
      { kind: "open", count: final(113) },
    ],
    [
      "rows short of their end that reach the counted total read as that total",
      line({ shown: floor(113), rows: "incomplete" }),
      { kind: "open", count: final(113) },
    ],
    [
      "a filter over the loaded rows reads as a part of them",
      line({ shown: final(5) }),
      { kind: "subset", shown: final(5), total: final(113) },
    ],
    [
      // The rows are the matches of a search, so only the counts read holds the total.
      "text, labels, a milestone or a repository read as a part of the counted total",
      line({ searched: true, shown: final(5), loadedOpen: 5 }),
      { kind: "subset", shown: final(5), total: final(113) },
    ],
    [
      "a counted total being read again is not final",
      line({ searched: true, shown: final(5), loadedOpen: 5, counted: unsettled(113) }),
      { kind: "subset", shown: final(5), total: unsettled(113) },
    ],
    [
      "a search that matches every open Issue still reads against the total",
      line({ searched: true }),
      { kind: "subset", shown: final(113), total: final(113) },
    ],
    [
      // Two Issues were opened since the counts read: never "114 of 113".
      "a search matching more open Issues than were counted claims no total",
      line({ searched: true, shown: final(114), loadedOpen: 114 }),
      { kind: "listed", count: final(114), open: null },
    ],
    [
      "a search with the total unknown claims no total",
      line({ searched: true, shown: final(5), loadedOpen: 5, counted: null }),
      { kind: "listed", count: final(5), open: null },
    ],
    [
      "rows still arriving read against the counted total",
      line({ shown: floor(100), loadedOpen: 100, rows: "incomplete", counted: final(350) }),
      { kind: "subset", shown: floor(100), total: final(350) },
    ],
    [
      "rows still arriving past the counted total are a floor of their own",
      line({ shown: floor(114), loadedOpen: 114, rows: "incomplete" }),
      { kind: "open", count: floor(114) },
    ],
    [
      "rows still arriving are a floor when the total is unknown",
      line({ shown: floor(100), loadedOpen: 100, rows: "incomplete", counted: null }),
      { kind: "open", count: floor(100) },
    ],
    [
      "another State lists its own rows and keeps the open total apart",
      line({ state: "closed", shown: final(42), loadedOpen: 0 }),
      { kind: "listed", count: final(42), open: final(113) },
    ],
    [
      "another State holding more open Issues than were counted shows no total",
      line({ state: "all", shown: final(700), loadedOpen: 114 }),
      { kind: "listed", count: final(700), open: null },
    ],
    [
      "another State with the total unknown shows none",
      line({ state: "all", shown: final(42), loadedOpen: 30, counted: null }),
      { kind: "listed", count: final(42), open: null },
    ],
  ] as const)("%s", (_name, actual, expected) => {
    expect(actual).toEqual(expected);
  });

  it("counts open Issues nobody updated for 30 days", () => {
    const now = Date.parse("2026-11-26T12:00:00Z");
    const updated = (updatedAt: string, state: GitHubIssueListEntry["state"] = "open") =>
      entry(1, { updatedAt, state });
    expect(
      staleIssueCount(
        [
          updated("2026-10-27T12:00:00Z"),
          updated("2026-10-27T12:00:01Z"),
          updated("2026-09-01T00:00:00Z"),
          updated("2026-09-01T00:00:00Z", "done"),
        ],
        now,
      ),
    ).toBe(2);
  });

  it.each([
    // A count of rows only grows while a page is missing, as after a failed further page.
    [{ rowsComplete: false, threadsRead: true, byGroup: false }, "floor"],
    [{ rowsComplete: true, threadsRead: false, byGroup: false }, "final"],
    // A later page or thread read can move a row already shown into another group.
    [{ rowsComplete: false, threadsRead: true, byGroup: true }, "unsettled"],
    [{ rowsComplete: true, threadsRead: false, byGroup: true }, "unsettled"],
    [{ rowsComplete: true, threadsRead: true, byGroup: true }, "final"],
  ] as const)("trusts a count under %j as %s", (input, expected) => {
    expect(issueCountCertainty(input)).toBe(expected);
  });
});

describe("matchesIssueFilters", () => {
  const parent = link(25);
  const row = entry(27, {
    labels: [
      { name: "Bug", color: "d73a4a" },
      { name: "web", color: "000000" },
    ],
    milestone: "M1",
    parent,
  });

  it("narrows by repository", () => {
    expect(
      matchesIssueFilters(row, { repository: repositoryKey("github.com", "toolboxmd/t3code") }),
    ).toBe(true);
    expect(
      matchesIssueFilters(row, { repository: repositoryKey("github.com", "toolboxmd/other") }),
    ).toBe(false);
  });

  it("requires every chosen label, ignoring case", () => {
    expect(matchesIssueFilters(row, { labels: ["bug", "WEB"] })).toBe(true);
    expect(matchesIssueFilters(row, { labels: ["bug", "mobile"] })).toBe(false);
  });

  it("narrows by milestone", () => {
    expect(matchesIssueFilters(row, { milestone: "m1" })).toBe(true);
    expect(matchesIssueFilters(row, { milestone: "M2" })).toBe(false);
    expect(matchesIssueFilters(entry(1), { milestone: "M1" })).toBe(false);
  });

  it("keeps top-level Issues or the children of one parent", () => {
    expect(matchesIssueFilters(row, { parent: "none" })).toBe(false);
    expect(matchesIssueFilters(entry(1), { parent: "none" })).toBe(true);
    expect(matchesIssueFilters(row, { parent: issueKey(parent) })).toBe(true);
    expect(matchesIssueFilters(row, { parent: issueKey(link(26)) })).toBe(false);
  });
});

describe("sortIssues", () => {
  const rows = [
    entry(3, { updatedAt: "2026-09-20T00:00:00Z" }),
    entry(1, { updatedAt: "2026-09-25T00:00:00Z" }),
    entry(2, { updatedAt: "2026-09-22T00:00:00Z", repository: "toolboxmd/b" }),
    entry(2, { updatedAt: "2026-09-22T00:00:00Z", repository: "toolboxmd/a" }),
  ];

  it("orders by last update, newest first, stable on ties", () => {
    expect(sortIssues(rows, "updated").map((row) => `${row.repository}#${row.number}`)).toEqual([
      "toolboxmd/t3code#1",
      "toolboxmd/a#2",
      "toolboxmd/b#2",
      "toolboxmd/t3code#3",
    ]);
  });

  it("orders by creation and by number, newest first", () => {
    expect(sortIssues(rows, "created").map((row) => row.number)).toEqual([3, 2, 2, 1]);
    expect(sortIssues(rows, "number").map((row) => row.number)).toEqual([3, 2, 2, 1]);
  });
});

describe("collectIssueFacets", () => {
  it("offers labels by use, milestones and the parents of loaded rows", () => {
    const facets = collectIssueFacets([
      entry(1, { labels: [{ name: "bug", color: "red" }], milestone: "M1", parent: link(9) }),
      entry(2, {
        labels: [
          { name: "Bug", color: "red" },
          { name: "docs", color: "blue" },
        ],
        parent: link(9),
      }),
    ]);
    expect(facets.labels.map((label) => [label.name, label.count])).toEqual([
      ["bug", 2],
      ["docs", 1],
    ]);
    expect(facets.milestones).toEqual(["M1"]);
    expect(facets.parents.map((parent) => parent.number)).toEqual([9]);
  });
});

describe("buildIssueTree", () => {
  const projects = new Set([repositoryKey("github.com", "toolboxmd/t3code")]);
  const shape = (nodes: ReadonlyArray<IssueTreeNode>): unknown =>
    nodes.map((node) => ({
      key: `${node.link.repository}#${node.link.number}`,
      loaded: node.entry !== null,
      ...(node.outsideProjects ? { outside: true } : {}),
      ...(node.unlistedChildCount > 0 ? { more: node.unlistedChildCount } : {}),
      ...(node.children.length > 0 ? { children: shape(node.children) } : {}),
    }));

  it("nests loaded children under their parent and keeps orphans at the root", () => {
    const parent = entry(25, { subIssues: [link(27), link(28)], subIssueCount: 2 });
    const tree = buildIssueTree(
      [
        parent,
        entry(27, { parent: link(25) }),
        entry(28, { parent: link(25) }),
        entry(40, { parent: link(99) }),
      ],
      projects,
    );
    expect(shape(tree)).toEqual([
      {
        key: "toolboxmd/t3code#25",
        loaded: true,
        children: [
          { key: "toolboxmd/t3code#27", loaded: true },
          { key: "toolboxmd/t3code#28", loaded: true },
        ],
      },
      // Its parent is not on the page, so it stays a root.
      { key: "toolboxmd/t3code#40", loaded: true },
    ]);
  });

  it("shows children in other repositories as link rows marked outside the projects", () => {
    const tree = buildIssueTree(
      [
        entry(3, {
          subIssues: [
            link(1, { repository: "toolboxmd/agent-observer" }),
            link(5, { state: "done" }),
          ],
          subIssueCount: 4,
        }),
      ],
      projects,
    );
    expect(shape(tree)).toEqual([
      {
        key: "toolboxmd/t3code#3",
        loaded: true,
        more: 2,
        children: [
          { key: "toolboxmd/agent-observer#1", loaded: false, outside: true },
          // Not loaded (filtered out or on a later page) but inside the projects: unmarked.
          { key: "toolboxmd/t3code#5", loaded: false },
        ],
      },
    ]);
  });

  it("attaches a loaded child its parent's first page left out, and survives a cycle", () => {
    const tree = buildIssueTree(
      [
        entry(1, { subIssues: [link(2)], subIssueCount: 60, parent: null }),
        entry(2, { parent: link(1), subIssues: [link(1)], subIssueCount: 1 }),
        entry(70, { parent: link(1) }),
      ],
      projects,
    );
    expect(shape(tree)).toEqual([
      {
        key: "toolboxmd/t3code#1",
        loaded: true,
        more: 59,
        children: [
          { key: "toolboxmd/t3code#2", loaded: true },
          { key: "toolboxmd/t3code#70", loaded: true },
        ],
      },
    ]);
  });
});

describe("environmentIdsWithCapability", () => {
  const server = (id: string, capabilities: { issues?: boolean; issueLinks?: boolean } | null) => ({
    environmentId: id as EnvironmentId,
    serverConfig: capabilities === null ? null : { environment: { capabilities } },
  });
  const environments = [
    server("upstream", {}),
    server("fork-b", { issues: true, issueLinks: true }),
    server("connecting", null),
    server("fork-a", { issues: true }),
  ];

  it("keeps only servers that advertise the capability, sorted", () => {
    // Upstream and older servers would answer `issues.list` with an unknown-tag error.
    expect(environmentIdsWithCapability(environments, "issues")).toEqual(["fork-a", "fork-b"]);
    expect(environmentIdsWithCapability(environments, "issueLinks")).toEqual(["fork-b"]);
  });
});
