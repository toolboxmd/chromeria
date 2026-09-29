import { ThreadId, type ThreadPullRequestLink } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  type DescendantShell,
  descendantThreads,
  linkedByOf,
  rollUpThreadPullRequests,
} from "./threadDescendants.logic";

const PLANNER = "4979d148-e778-4239-8540-b3718dc55c56";
const CHILD = `sub.${PLANNER}.053f467833fd`;
const GRANDCHILD = `sub.${CHILD}.4bfa18c0144a`;

const pullRequest = (
  number: number,
  source: ThreadPullRequestLink["source"] = "agent",
): ThreadPullRequestLink => ({
  host: "github.com",
  repository: "toolboxmd/agentsmd",
  number,
  url: `https://github.com/toolboxmd/agentsmd/pull/${number}`,
  source,
  linkedAt: "2026-09-29T13:16:49.000Z",
  snapshot: null,
  stack: null,
});

const shell = (
  id: string,
  createdAt: string,
  pullRequests: ReadonlyArray<ThreadPullRequestLink>,
): DescendantShell => ({
  id: ThreadId.make(id),
  title: id,
  branch: null,
  createdAt,
  pullRequests,
});

describe("descendantThreads", () => {
  it("finds children and grandchildren in spawn order, and nothing else", () => {
    const shells = [
      shell(GRANDCHILD, "2026-09-29T15:00:00.000Z", []),
      shell(CHILD, "2026-09-29T13:00:00.000Z", []),
      shell(PLANNER, "2026-09-29T12:00:00.000Z", []),
      shell(`sub.other-${PLANNER}.1`, "2026-09-29T13:00:00.000Z", []),
      shell("sub.another-planner.1", "2026-09-29T13:00:00.000Z", []),
    ];
    expect(descendantThreads(shells, PLANNER).map((entry) => entry.id)).toEqual([
      CHILD,
      GRANDCHILD,
    ]);
    expect(descendantThreads(shells, CHILD).map((entry) => entry.id)).toEqual([GRANDCHILD]);
  });
});

describe("rollUpThreadPullRequests", () => {
  it("adds a pull request only a grandchild linked, marked with the grandchild", () => {
    const grandchild = shell(GRANDCHILD, "2026-09-29T15:00:00.000Z", [pullRequest(168)]);
    const rolledUp = rollUpThreadPullRequests(
      [pullRequest(12, "manual")],
      [shell(CHILD, "2026-09-29T13:00:00.000Z", []), grandchild],
    );
    expect(rolledUp.links.map((link) => link.number)).toEqual([12, 168]);
    expect(linkedByOf(rolledUp, rolledUp.links[0]!)).toBeNull();
    expect(linkedByOf(rolledUp, rolledUp.links[1]!)).toBe(grandchild);
  });

  it("shows each pull request once: the thread's own first, then the earliest holder's", () => {
    const child = shell(CHILD, "2026-09-29T13:00:00.000Z", [pullRequest(12), pullRequest(168)]);
    const grandchild = shell(GRANDCHILD, "2026-09-29T15:00:00.000Z", [pullRequest(168)]);
    const rolledUp = rollUpThreadPullRequests([pullRequest(12, "manual")], [child, grandchild]);
    expect(rolledUp.links.map((link) => [link.number, link.source])).toEqual([
      [12, "manual"],
      [168, "agent"],
    ]);
    expect(linkedByOf(rolledUp, rolledUp.links[1]!)).toBe(child);
  });

  it("keeps what the thread or its child dismissed hidden", () => {
    const rolledUp = rollUpThreadPullRequests(
      [pullRequest(7, "stack-dismissed")],
      [
        shell(CHILD, "2026-09-29T13:00:00.000Z", [
          pullRequest(7),
          pullRequest(8, "stack-dismissed"),
        ]),
      ],
    );
    expect(rolledUp.links).toEqual([]);
  });
});
