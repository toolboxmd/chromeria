import { presentThreadShell } from "@t3tools/client-runtime/state/models";
import { makeThreadFixture } from "../../test-fixtures";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { promachosConversations, promachosHomeRefs } from "./promachosConversations";

const home = { environmentId: EnvironmentId.make("env-a"), projectId: ProjectId.make("home") };
const homeOnB = { environmentId: EnvironmentId.make("env-b"), projectId: ProjectId.make("home-b") };

function thread(
  id: string,
  overrides: Partial<{
    environmentId: string;
    projectId: string;
    archivedAt: string | null;
    latestUserMessageAt: string | null;
    lineage: { parentThreadId: ThreadId | null; relationshipToParent: "fork" | "subagent" | null };
    updatedAt: string;
  }> = {},
) {
  return {
    id: ThreadId.make(id),
    lineage: {
      rootThreadId: ThreadId.make("root"),
      ...(overrides.lineage ?? { parentThreadId: null, relationshipToParent: null }),
    },
    environmentId: EnvironmentId.make(overrides.environmentId ?? "env-a"),
    projectId: ProjectId.make(overrides.projectId ?? "home"),
    archivedAt: overrides.archivedAt ?? null,
    latestUserMessageAt: overrides.latestUserMessageAt ?? null,
    updatedAt: overrides.updatedAt ?? "2026-10-01T08:00:00.000Z",
  };
}

describe("promachosConversations", () => {
  it("hides destination children in the home but keeps inherited-provenance user forks", () => {
    const child = presentThreadShell(home.environmentId, {
      ...makeThreadFixture({ projectId: home.projectId }).source,
      delegatedFrom: {
        environmentId: EnvironmentId.make("offline-parent"),
        threadId: ThreadId.make("parent"),
        title: "Parent",
      },
    });
    const fork = {
      ...child,
      id: ThreadId.make("user-fork"),
      lineage: {
        ...child.lineage,
        parentThreadId: child.id,
        relationshipToParent: "fork" as const,
      },
    };
    const otherMachine = {
      ...fork,
      environmentId: homeOnB.environmentId,
      projectId: homeOnB.projectId,
    };
    const archived = {
      ...fork,
      id: ThreadId.make("archived"),
      archivedAt: "2026-01-02T00:00:00.000Z",
    };
    const localChild = {
      ...child,
      id: ThreadId.make("local-child"),
      lineage: { ...child.lineage, relationshipToParent: "subagent" as const },
    };
    const all = [child, fork, otherMachine, archived, localChild];
    const visible = promachosConversations(all, [home, homeOnB]);
    expect(visible.map((entry) => [entry.environmentId, entry.id])).toEqual([
      [home.environmentId, fork.id],
      [homeOnB.environmentId, fork.id],
    ]);
    expect(fork.delegatedFrom).toBe(child.delegatedFrom);
    expect(all).toContain(child);
  });

  it("lists the home's own top-level threads on every machine it spans", () => {
    const threads = [
      thread("chat"),
      thread("other-machine", { environmentId: "env-b", projectId: "home-b" }),
      thread("elsewhere", { projectId: "satrapy" }),
      thread("same-id-other-machine", { environmentId: "env-b" }),
      thread("archived", { archivedAt: "2026-10-01T09:00:00.000Z" }),
      thread("native-child", {
        lineage: { parentThreadId: ThreadId.make("chat"), relationshipToParent: "subagent" },
      }),
    ];
    expect(promachosConversations(threads, [home, homeOnB]).map((entry) => entry.id)).toEqual([
      "chat",
      "other-machine",
    ]);
  });

  it("puts the latest conversation first", () => {
    const threads = [
      thread("older", { latestUserMessageAt: "2026-10-01T08:00:00.000Z" }),
      thread("newer", { latestUserMessageAt: "2026-10-01T09:00:00.000Z" }),
      thread("never-messaged", { updatedAt: "2026-10-01T08:30:00.000Z" }),
    ];
    expect(promachosConversations(threads, [home]).map((entry) => entry.id)).toEqual([
      "newer",
      "never-messaged",
      "older",
    ]);
  });
});

describe("promachosHomeRefs", () => {
  const identity = (canonicalKey: string) => ({
    canonicalKey,
    locator: { source: "git-remote" as const, remoteName: "origin", remoteUrl: canonicalKey },
  });
  const project = (
    environmentId: string,
    id: string,
    workspaceRoot: string,
    canonicalKey: string | null,
  ) => ({
    environmentId: EnvironmentId.make(environmentId),
    id: ProjectId.make(id),
    workspaceRoot,
    repositoryIdentity: canonicalKey === null ? null : identity(canonicalKey),
  });
  const grouped = {
    sidebarProjectGroupingMode: "repository" as const,
    sidebarProjectGroupingOverrides: {},
  };

  it("spans the same repository on every machine, as the sidebar merges it", () => {
    const projects = [
      project("env-a", "home", "/Users/me/dev/promachos", "github.com/me/promachos"),
      project("env-b", "home-b", "/home/me/dev/promachos", "github.com/me/promachos"),
      project("env-b", "satrapy", "/home/me/dev/satrapy", "github.com/me/satrapy"),
    ];
    expect(promachosHomeRefs(projects, home, grouped)).toEqual([home, homeOnB]);
  });

  it("stays on one machine when the sidebar keeps projects separate", () => {
    const projects = [
      project("env-a", "home", "/Users/me/dev/promachos", "github.com/me/promachos"),
      project("env-b", "home-b", "/home/me/dev/promachos", "github.com/me/promachos"),
    ];
    expect(
      promachosHomeRefs(projects, home, { ...grouped, sidebarProjectGroupingMode: "separate" }),
    ).toEqual([home]);
  });
});
