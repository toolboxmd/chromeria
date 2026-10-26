// @vitest-environment jsdom
import { presentThreadShell } from "@t3tools/client-runtime/state/models";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { buildThreadActionItems } from "./CommandPalette.logic";
import { makeThreadFixture } from "../test-fixtures";
import { legacySidebarVisibleThreads } from "./LegacySidebar";

it("legacy rendered rows and jump candidates omit remote children but retain user forks", async () => {
  const destination = EnvironmentId.make("destination");
  const child = presentThreadShell(destination, {
    ...makeThreadFixture({ title: "Remote child" }).source,
    delegatedFrom: {
      environmentId: EnvironmentId.make("offline-parent"),
      threadId: ThreadId.make("parent"),
      title: "Parent",
    },
  });
  const fork = {
    ...child,
    id: ThreadId.make("fork"),
    lineage: { ...child.lineage, parentThreadId: child.id, relationshipToParent: "fork" as const },
  };
  const ordinary = makeThreadFixture({ environmentId: destination, id: ThreadId.make("ordinary") });
  const archived = {
    ...fork,
    id: ThreadId.make("archived"),
    archivedAt: "2026-01-02T00:00:00.000Z",
  };
  const localChild = {
    ...child,
    id: ThreadId.make("local"),
    lineage: { ...child.lineage, relationshipToParent: "subagent" as const },
  };
  const all = [child, fork, ordinary, archived, localChild];
  expect(new Set(legacySidebarVisibleThreads(all, "created_at"))).toEqual(
    new Set([fork, ordinary]),
  );
  const selected: string[][] = [];
  const actions = buildThreadActionItems({
    threads: all,
    projectByKey: new Map(),
    icon: null,
    runThread: async (thread) => {
      selected.push([thread.environmentId, thread.id]);
    },
  });
  const searchResult = actions.find(
    (item) => item.searchTerms.includes("Remote child") && item.value === `thread:${child.id}`,
  );
  expect(searchResult).toBeDefined();
  await searchResult!.run();
  expect(selected).toEqual([[destination, child.id]]);
  expect(all).toContain(child);
  expect(fork.delegatedFrom).toBe(child.delegatedFrom);
});
