import type { DeviceStreamEvents, DeviceStreamTarget } from "@t3tools/client-runtime/device/stream";
import { EnvironmentId } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  clients: [] as Array<{
    target: DeviceStreamTarget;
    events: DeviceStreamEvents;
    stop: () => void;
  }>,
}));
vi.mock("@t3tools/client-runtime/device/frame", () => ({ createCanvasFrameSink: () => ({}) }));
vi.mock("@t3tools/client-runtime/device/stream", () => ({
  createDeviceStreamClient: (
    target: DeviceStreamTarget,
    _sink: unknown,
    events: DeviceStreamEvents,
  ) => {
    const stop = vi.fn();
    state.clients.push({ target, events, stop });
    return { start: vi.fn(), stop };
  },
}));

import { deviceStreamScreenAtom, deviceStreamSessionAtom } from "./deviceStreamSession";

it("keeps each viewer's stream, dimensions and scoped access renewal independent", () => {
  state.clients = [];
  const tasks: Array<() => void> = [];
  const flush = () => {
    for (let task = tasks.shift(); task !== undefined; task = tasks.shift()) task();
  };
  const registry = AtomRegistry.make({
    scheduleTask: (task) => {
      tasks.push(task);
      return () => {};
    },
  });
  using _registry = { [Symbol.dispose]: () => registry.dispose() };
  const refreshAccess = vi.fn();
  const key = {
    environmentId: EnvironmentId.make("source"),
    hostId: "simlock",
    platform: "android" as const,
    deviceId: "device",
    access: {
      httpBase: "https://source.test/hub",
      wsBase: "wss://source.test/hub",
      query: { viewerId: "viewer" },
      credentials: true,
    },
    serveSimOnly: true,
    refreshAccess,
  };
  const first = deviceStreamSessionAtom({ ...key, viewId: "panel" });
  const second = deviceStreamSessionAtom({ ...key, viewId: "floating" });
  const releaseFirst = registry.mount(first);
  const releaseSecond = registry.mount(second);
  using _second = {
    [Symbol.dispose]: () => {
      releaseSecond();
      flush();
    },
  };
  using _panelScreen = { [Symbol.dispose]: registry.mount(deviceStreamScreenAtom("panel")) };
  using _floatingScreen = { [Symbol.dispose]: registry.mount(deviceStreamScreenAtom("floating")) };
  const panel = state.clients[0]!;
  const floating = state.clients[1]!;
  expect(panel.target).toMatchObject({ access: key.access, serveSimOnly: true });
  const screen = { width: 400, height: 800, orientation: "portrait" as const };
  panel.events.onScreen(screen);
  floating.events.onScreen(screen);
  panel.events.onUnauthorized();
  expect(refreshAccess).toHaveBeenCalledTimes(1);
  releaseFirst();
  flush();
  expect(panel.stop).toHaveBeenCalledTimes(1);
  expect(floating.stop).not.toHaveBeenCalled();
  expect(registry.get(deviceStreamScreenAtom("panel"))).toBeNull();
  expect(registry.get(deviceStreamScreenAtom("floating"))).toEqual(screen);
});
