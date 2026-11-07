import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type IssueLinkedThread,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import { render } from "../../test/render";
import { openCommandPalette } from "../commandPaletteBus";
import { useCommandPaletteStore } from "../commandPaletteStore";
import { makeThreadFixture } from "../test-fixtures";
import type { Project, Thread } from "../types";
import { CommandPalette } from "./CommandPalette";

const entities = vi.hoisted(() => ({
  projects: [] as ReadonlyArray<Project>,
  threads: [] as ReadonlyArray<Thread>,
  issueThreads: [] as ReadonlyArray<{
    environmentId: EnvironmentId;
    threads: ReadonlyArray<IssueLinkedThread>;
  }>,
  loadedRefs: [] as ReadonlyArray<ScopedThreadRef>,
  hiddenKeys: new Set<string>(),
  hiding: false,
}));
vi.mock("../state/entities", async (original) => ({
  ...(await original<typeof import("../state/entities")>()),
  useProjects: () => entities.projects,
  useVisibleThreadShells: () => entities.threads,
  useThreadRefs: () => entities.loadedRefs,
  useHiddenThreadKeys: () => entities.hiddenKeys,
  useAnyEnvironmentHiding: () => entities.hiding,
}));

vi.mock("./issues/issuePaletteThreads", () => ({
  usePaletteIssueThreads: () => entities.issueThreads,
}));

function setup(state: { projects?: ReadonlyArray<Project>; threads?: ReadonlyArray<Thread> } = {}) {
  useCommandPaletteStore.setState(useCommandPaletteStore.getInitialState(), true);
  entities.projects = state.projects ?? [];
  entities.threads = state.threads ?? [];
  entities.issueThreads = [];
  entities.loadedRefs = [];
  entities.hiddenKeys = new Set();
  entities.hiding = false;
}

describe("CommandPalette open intents", () => {
  it("opens New thread in... with focus in the search input, not the Back button", async () => {
    setup();
    await using _screen = await render(<CommandPalette>{null}</CommandPalette>);

    openCommandPalette({ open: "new-thread-in" });

    await expect.element(page.getByPlaceholder("Search...")).toHaveFocus();
  });

  it("opens a search intent with its query in the focused input", async () => {
    setup();
    await using _screen = await render(<CommandPalette>{null}</CommandPalette>);

    openCommandPalette({ query: "acme" });

    const input = page.getByPlaceholder("Search commands, projects, and threads...");
    await expect.element(input).toHaveFocus();
    await expect.element(input).toHaveValue("acme");
  });

  it("opens a searched project's latest listed thread, not a newer subagent", async () => {
    const environmentId = EnvironmentId.make("local");
    const projectId = ProjectId.make("acme");
    const parentId = ThreadId.make("parent");
    setup({
      projects: [
        {
          id: projectId,
          environmentId,
          title: "Acme",
          workspaceRoot: "/work/acme",
          repositoryIdentity: null,
          defaultModelSelection: null,
          createdAt: "2026-03-01T00:00:00.000Z",
          updatedAt: "2026-03-01T00:00:00.000Z",
          scripts: [],
        },
      ],
      threads: [
        makeThreadFixture({
          id: parentId,
          environmentId,
          projectId,
          latestUserMessageAt: "2026-03-02T00:00:00.000Z",
        }),
        makeThreadFixture({
          id: ThreadId.make("subagent"),
          environmentId,
          projectId,
          updatedAt: "2026-03-03T00:00:00.000Z",
          lineage: {
            rootThreadId: parentId,
            parentThreadId: parentId,
            relationshipToParent: "subagent",
          },
        }),
      ],
    });
    await using screen = await render(<CommandPalette>{null}</CommandPalette>);

    openCommandPalette({ query: "Acme" });
    await page.getByRole("option", { name: /Acme/ }).first().click();

    await vi.waitFor(() => expect(screen.router?.state.location.pathname).toBe("/local/parent"));
  });
});

describe.each(["owner/repo#29", "https://github.com/owner/repo/issues/29", "#29"])(
  "Issue-linked thread search for %s",
  (query) => {
    it.each([true, false])(
      "keeps environment-qualified visibility when hiding is %s",
      async (hiding) => {
        setup();
        const local = EnvironmentId.make("local");
        const remote = EnvironmentId.make("remote");
        const sharedId = ThreadId.make("shared");
        const visibleId = ThreadId.make("visible");
        const unloadedId = ThreadId.make("unloaded");
        const elsewhereId = ThreadId.make("elsewhere");
        const linkedThread = (id: ThreadId, title: string): IssueLinkedThread => ({
          id,
          projectId: ProjectId.make("project"),
          title,
          archivedAt: null,
          sources: [],
          pullRequests: [],
        });
        entities.hiding = hiding;
        entities.hiddenKeys = new Set(hiding ? ["local\u0000shared"] : []);
        entities.loadedRefs = [
          { environmentId: local, threadId: sharedId },
          { environmentId: local, threadId: visibleId },
          { environmentId: remote, threadId: sharedId },
          { environmentId: remote, threadId: elsewhereId },
        ];
        entities.issueThreads = [
          {
            environmentId: local,
            threads: [
              linkedThread(sharedId, "Private local linked thread"),
              linkedThread(visibleId, "Visible local linked thread"),
              linkedThread(unloadedId, "Unloaded local linked thread"),
              linkedThread(elsewhereId, "Loaded only elsewhere local link"),
            ],
          },
          {
            environmentId: remote,
            threads: [
              linkedThread(sharedId, "Visible remote shared thread"),
              linkedThread(elsewhereId, "Visible remote loaded elsewhere"),
            ],
          },
        ];
        await using screen = await render(<CommandPalette>{null}</CommandPalette>);
        openCommandPalette({ query });
        await expect
          .element(page.getByRole("option", { name: /Visible local linked thread/ }))
          .toBeVisible();
        await expect
          .element(page.getByRole("option", { name: /Visible remote shared thread/ }))
          .toBeVisible();
        await expect
          .element(page.getByRole("option", { name: /Visible remote loaded elsewhere/ }))
          .toBeVisible();
        for (const title of [
          "Private local linked thread",
          "Unloaded local linked thread",
          "Loaded only elsewhere local link",
        ]) {
          const row = page.getByRole("option", { name: new RegExp(title) });
          if (hiding) await expect.element(row).not.toBeInTheDocument();
          else await expect.element(row).toBeVisible();
        }
        await page.getByRole("option", { name: /Visible remote shared thread/ }).click();
        await vi.waitFor(() =>
          expect(screen.router?.state.location.pathname).toBe("/remote/shared"),
        );
      },
    );
  },
);
