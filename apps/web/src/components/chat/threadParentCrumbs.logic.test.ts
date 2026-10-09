import { describe, expect, it } from "vite-plus/test";

import { childParentThreadId, siblingThreads } from "./threadParentCrumbs.logic";

function shell(
  id: string,
  options: {
    parent?: string | null;
    relationship?: "fork" | "subagent" | null;
    environmentId?: string;
    createdAt?: string;
    archivedAt?: string | null;
  } = {},
) {
  return {
    id,
    environmentId: options.environmentId ?? "env-1",
    createdAt: options.createdAt ?? "2026-10-09T10:00:00Z",
    archivedAt: options.archivedAt ?? null,
    lineage: {
      parentThreadId: options.parent ?? null,
      relationshipToParent: options.relationship ?? (options.parent ? "subagent" : null),
    },
  };
}

describe("childParentThreadId", () => {
  it("returns the parent of a child thread", () => {
    expect(childParentThreadId(shell("child", { parent: "parent" }))).toBe("parent");
  });

  it("ignores forks and top-level threads", () => {
    expect(childParentThreadId(shell("fork", { parent: "source", relationship: "fork" }))).toBe(
      null,
    );
    expect(childParentThreadId(shell("top"))).toBe(null);
    expect(childParentThreadId(null)).toBe(null);
  });
});

describe("siblingThreads", () => {
  it("lists live siblings in the same environment, oldest first, without the thread itself", () => {
    const shells = [
      shell("self", { parent: "p", createdAt: "2026-10-09T10:02:00Z" }),
      shell("later", { parent: "p", createdAt: "2026-10-09T10:03:00Z" }),
      shell("earlier", { parent: "p", createdAt: "2026-10-09T10:01:00Z" }),
      shell("archived", { parent: "p", archivedAt: "2026-10-09T11:00:00Z" }),
      shell("other-env", { parent: "p", environmentId: "env-2" }),
      shell("fork", { parent: "p", relationship: "fork" }),
      shell("cousin", { parent: "q" }),
    ];
    expect(siblingThreads(shells, "env-1", "p", "self").map((s) => s.id)).toEqual([
      "earlier",
      "later",
    ]);
  });
});
