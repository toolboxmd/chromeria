import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { promachosConversations } from "./promachosConversations";

const home = { environmentId: EnvironmentId.make("env-a"), projectId: ProjectId.make("home") };

function thread(
  id: string,
  overrides: Partial<{
    environmentId: string;
    projectId: string;
    archivedAt: string | null;
    latestUserMessageAt: string | null;
    updatedAt: string;
  }> = {},
) {
  return {
    id: ThreadId.make(id),
    environmentId: EnvironmentId.make(overrides.environmentId ?? "env-a"),
    projectId: ProjectId.make(overrides.projectId ?? "home"),
    archivedAt: overrides.archivedAt ?? null,
    latestUserMessageAt: overrides.latestUserMessageAt ?? null,
    updatedAt: overrides.updatedAt ?? "2026-10-01T08:00:00.000Z",
  };
}

describe("promachosConversations", () => {
  it("lists only the home's own top-level threads on the home's environment", () => {
    const threads = [
      thread("chat"),
      thread("elsewhere", { projectId: "satrapy" }),
      thread("same-id-other-machine", { environmentId: "env-b" }),
      thread("archived", { archivedAt: "2026-10-01T09:00:00.000Z" }),
      thread("sub.chat.drafter"),
    ];
    expect(promachosConversations(threads, home).map((entry) => entry.id)).toEqual(["chat"]);
  });

  it("puts the latest conversation first", () => {
    const threads = [
      thread("older", { latestUserMessageAt: "2026-10-01T08:00:00.000Z" }),
      thread("newer", { latestUserMessageAt: "2026-10-01T09:00:00.000Z" }),
      thread("never-messaged", { updatedAt: "2026-10-01T08:30:00.000Z" }),
    ];
    expect(promachosConversations(threads, home).map((entry) => entry.id)).toEqual([
      "newer",
      "never-messaged",
      "older",
    ]);
  });
});
