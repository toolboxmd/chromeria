import { RegistryContext } from "@effect/atom-react";
import { EnvironmentId, ThreadId, type PreviewViewerResource } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";

import { fakeTimers, render } from "../../test/render";

const state = vi.hoisted(() => ({ opens: 0, closes: 0 }));
vi.mock("../connection/runtime", () => ({ connectionAtomRuntime: {} }));
vi.mock("../rpc/atomRegistry", () => ({ appAtomRegistry: {} }));
vi.mock("./session", () => ({ environmentSession: {} }));
vi.mock("./preview", () => ({
  previewEnvironment: {
    viewerOpen: { permissionAtom: () => permissionAtom },
    viewerAccess: () => accessAtom,
  },
}));

import { usePreviewStreamAccessState } from "./previewStream";

const permissionAtom = Atom.make(true);
const accessAtom = Atom.make((get) => {
  state.opens++;
  get.addFinalizer(() => state.closes++);
  return AsyncResult.failure(Cause.fail(new Error("Offline")));
}).pipe(Atom.setIdleTTL(0));
const environmentId = EnvironmentId.make("source");
const resource: PreviewViewerResource = {
  type: "preview",
  environmentId,
  threadId: ThreadId.make("thread"),
  tabId: "tab",
  serverEpoch: "epoch",
};

function Viewer({ visible }: { visible: boolean }) {
  usePreviewStreamAccessState(environmentId, resource, visible);
  return null;
}

it("retries repeated access failures only while the resource viewer is visible", async () => {
  state.opens = state.closes = 0;
  const registry = AtomRegistry.make();
  using _registry = { [Symbol.dispose]: () => registry.dispose() };
  const viewer = (visible: boolean) => (
    <RegistryContext value={registry}>
      <Viewer visible={visible} />
    </RegistryContext>
  );
  await using screen = await render(null);
  using _timers = fakeTimers();
  await screen.rerender(viewer(true));
  expect(state.opens).toBe(1);
  await vi.advanceTimersByTimeAsync(10_000);
  expect(state.opens).toBe(2);
  await vi.advanceTimersByTimeAsync(10_000);
  expect(state.opens).toBe(3);
  await screen.rerender(viewer(false));
  const stopped = state.opens;
  await vi.advanceTimersByTimeAsync(10_000);
  expect(state.opens).toBe(stopped);
  expect(state.closes).toBe(stopped);
  await screen.rerender(viewer(true));
  expect(state.opens).toBe(stopped + 1);
});
