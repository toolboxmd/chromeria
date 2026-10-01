import { describe, expect, it } from "vite-plus/test";

import {
  childThreadsByParent,
  threadBreadcrumbLinks,
  threadShellStatus,
  toggleExpandedThread,
} from "./AgentThreadTree.logic";

const PLANNER = "7b0736c2-3dec-4adb-aa18-6d93d15db108";

function shell(id: string, createdAt: string, extra: { archivedAt?: string | null } = {}) {
  return { id, title: `title ${id}`, createdAt, archivedAt: extra.archivedAt ?? null };
}

describe("child thread tree", () => {
  it("nests live children under their parent in spawn order", () => {
    const children = childThreadsByParent([
      shell(PLANNER, "2026-09-25T10:00:00Z"),
      shell(`sub.${PLANNER}.b`, "2026-09-25T10:02:00Z"),
      shell(`sub.${PLANNER}.a`, "2026-09-25T10:01:00Z"),
      shell(`sub.${PLANNER}.gone`, "2026-09-25T10:03:00Z", { archivedAt: "2026-09-25T11:00:00Z" }),
      shell(`sub.sub.${PLANNER}.a.x`, "2026-09-25T10:04:00Z"),
    ]);
    expect(children.get(PLANNER)?.map((s) => s.id)).toEqual([
      `sub.${PLANNER}.a`,
      `sub.${PLANNER}.b`,
    ]);
    expect(children.get(`sub.${PLANNER}.a`)?.map((s) => s.id)).toEqual([`sub.sub.${PLANNER}.a.x`]);
  });

  it("starts collapsed and toggles only the clicked row", () => {
    let expanded: ReadonlySet<string> = new Set();
    expanded = toggleExpandedThread(expanded, "a");
    expanded = toggleExpandedThread(expanded, "b");
    expect([...expanded]).toEqual(["a", "b"]);
    expanded = toggleExpandedThread(expanded, "a");
    expect([...expanded]).toEqual(["b"]);
  });

  it("reads a shell's status like the Agents panel does", () => {
    expect(threadShellStatus({ session: null, latestTurn: null })).toBe("pending");
    expect(threadShellStatus({ session: null, latestTurn: { state: "completed" } } as never)).toBe(
      "idle",
    );
    expect(threadShellStatus({ session: { status: "running" }, latestTurn: null } as never)).toBe(
      "running",
    );
    expect(threadShellStatus({ session: null, latestTurn: { state: "error" } } as never)).toBe(
      "failed",
    );
  });

  it("shows a stopped thread as stopped even when its last turn never finished", () => {
    for (const state of ["running", "error"]) {
      expect(
        threadShellStatus({
          session: { status: "interrupted" },
          latestTurn: { state },
        } as never),
      ).toBe("interrupted");
    }
  });

  it("shows a restarted thread by its current session, not its stopped turn", () => {
    for (const status of ["running", "starting"]) {
      expect(
        threadShellStatus({
          session: { status },
          latestTurn: { state: "interrupted" },
        } as never),
      ).toBe("running");
    }
    expect(
      threadShellStatus({
        session: { status: "ready" },
        latestTurn: { state: "completed" },
      } as never),
    ).toBe("idle");
  });
});

describe("breadcrumb links", () => {
  const shells = [
    shell(PLANNER, "2026-09-25T10:00:00Z"),
    shell(`sub.${PLANNER}.a`, "2026-09-25T10:01:00Z"),
    shell(`sub.${PLANNER}.b`, "2026-09-25T10:02:00Z"),
  ];

  it("links a child thread to its parent and siblings", () => {
    const links = threadBreadcrumbLinks(`sub.${PLANNER}.b`, shells);
    expect(links?.parent.id).toBe(PLANNER);
    expect(links?.siblings.map((s) => s.id)).toEqual([`sub.${PLANNER}.a`, `sub.${PLANNER}.b`]);
  });

  it("prefers a real parentThreadId over the id convention", () => {
    const links = threadBreadcrumbLinks("child", [
      ...shells,
      { ...shell("child", "2026-09-25T10:05:00Z"), parentThreadId: PLANNER },
    ]);
    expect(links?.parent.id).toBe(PLANNER);
  });

  it("shows no crumbs for user threads or children whose parent is gone", () => {
    expect(threadBreadcrumbLinks(PLANNER, shells)).toBeNull();
    expect(
      threadBreadcrumbLinks("sub.missing.a", [shell("sub.missing.a", "2026-09-25T10:00:00Z")]),
    ).toBeNull();
  });
});
