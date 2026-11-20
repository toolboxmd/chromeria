import type { EnvironmentId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  electron: false,
  desktop: false,
  primary: null as string | null,
  serverBrowser: new Set<string>(),
}));

vi.mock("~/env", () => ({
  get isElectron() {
    return state.electron;
  },
}));
vi.mock("~/previewStateStore", () => ({ isPreviewSupportedInRuntime: () => state.desktop }));
vi.mock("~/rpc/atomRegistry", () => ({ appAtomRegistry: { get: () => state.primary } }));
vi.mock("~/state/primaryEnvironment", () => ({ primaryEnvironmentIdAtom: {} }));
vi.mock("~/state/entities", () => ({
  readEnvironmentSupportsServerBrowser: (id: string) => state.serverBrowser.has(id),
  useEnvironmentSupportsServerBrowser: () => false,
}));

import { rendersServerTabNatively } from "./previewRuntime";

const local = "local" as EnvironmentId;
const remote = "remote" as EnvironmentId;

afterEach(() => {
  state.electron = false;
  state.desktop = false;
  state.primary = null;
  state.serverBrowser = new Set();
});

describe("rendersServerTabNatively", () => {
  it("hosts a person's local server tab in the desktop and streams a server page", () => {
    state.electron = true;

    expect(rendersServerTabNatively(local, local, { runtime: "server" })).toBe(true);
    expect(rendersServerTabNatively(local, local, { runtime: "server", serverPage: true })).toBe(
      false,
    );
    expect(rendersServerTabNatively(remote, local, { runtime: "server" })).toBe(false);
  });
});
