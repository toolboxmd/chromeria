import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";

// The phone's state modules, replaced by writable atoms the test moves from cache to live.
const project = vi.hoisted(() => (environmentId: string, id: string) => ({
  environmentId,
  id,
  workspaceRoot: `/${environmentId}/${id}`,
  repositoryIdentity: {
    canonicalKey: "github.com/me/promachos",
    locator: { source: "git-remote", remoteName: "origin", remoteUrl: "github.com/me/promachos" },
  },
}));
const fake = vi.hoisted(async () => {
  const { Atom } = await import("effect/reactivity");
  const perMachine = <A>(value: A) => ({ "env-a": Atom.make(value), "env-b": Atom.make(value) });
  return {
    catalog: Atom.make({
      isReady: true,
      entries: new Map([
        ["env-a", { enabled: true }],
        ["env-b", { enabled: true }],
      ]),
    }),
    configs: Atom.make(new Map<string, { settings: { promachosHome: string | null } }>()),
    configLive: perMachine(false),
    shellStatus: perMachine("cached"),
    projects: Atom.make([project("env-a", "home"), project("env-b", "home-b")]),
  };
});
vi.mock("../../connection/catalog", async () => ({
  environmentCatalog: { catalogValueAtom: (await fake).catalog },
}));
vi.mock("../../state/server", async () => {
  const state = await fake;
  return {
    environmentServerConfigsAtom: state.configs,
    serverEnvironment: {
      configLiveAtom: (id: "env-a" | "env-b") => state.configLive[id],
    },
  };
});
vi.mock("../../state/shell", async () => {
  const state = await fake;
  return { environmentShell: { statusAtom: (id: "env-a" | "env-b") => state.shellStatus[id] } };
});
vi.mock("../../state/projects", async () => ({
  environmentProjects: { projectsAtom: (await fake).projects },
}));
vi.mock("../../state/threads", async () => {
  const { Atom } = await import("effect/reactivity");
  const shell = Atom.make({ projectId: "home" });
  return { environmentThreadShells: { threadShellAtom: () => shell } };
});
vi.mock("../../state/preferences", async () => {
  const { AsyncResult, Atom } = await import("effect/reactivity");
  return { mobilePreferencesAtom: Atom.make(AsyncResult.success({})) };
});

import { promachosConversationMembershipAtom } from "./promachosLaunch";

it("keeps a conversation until every machine answers live, then follows the homes they name", async () => {
  const state = await fake;
  const registry = AtomRegistry.make();
  const membership = promachosConversationMembershipAtom({
    environmentId: EnvironmentId.make("env-a"),
    threadId: ThreadId.make("chat"),
  });
  const names = (a: string | null, b: string | null) =>
    registry.set(
      state.configs,
      new Map([
        ["env-a", { settings: { promachosHome: a === null ? null : ProjectId.make(a) } }],
        ["env-b", { settings: { promachosHome: b === null ? null : ProjectId.make(b) } }],
      ]),
    );
  const goLive = (machine: "env-a" | "env-b") => {
    registry.set(state.configLive[machine], true);
    registry.set(state.shellStatus[machine], "live");
  };

  // A answers live with its home cleared; B still shows its launch cache, from before B named one.
  names(null, null);
  goLive("env-a");
  expect(registry.get(membership)).toBe("unknown");

  // B answers live naming the same repository: A's conversation is still in the home.
  names(null, "home-b");
  goLive("env-b");
  expect(registry.get(membership)).toBe("in");

  // B names a new project of the same repository before that project reaches the phone.
  names(null, "home-b-new");
  expect(registry.get(membership)).toBe("unknown");
  registry.set(state.projects, [...registry.get(state.projects), project("env-b", "home-b-new")]);
  expect(registry.get(membership)).toBe("in");

  // Both machines now say there is no home: the conversation left it.
  names(null, null);
  expect(registry.get(membership)).toBe("out");
});
