import { EnvironmentId, ThreadId, type DeviceServiceState } from "@t3tools/contracts";
import { act, useSyncExternalStore } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import "~/index.css";

const fixture = vi.hoisted(() => ({
  snapshot: null as unknown as { state: DeviceServiceState; loaded: boolean },
  listeners: new Set<() => void>(),
  command: vi.fn().mockResolvedValue({ _tag: "Success", value: undefined }),
}));
const subscribe = (listener: () => void) => {
  fixture.listeners.add(listener);
  return () => fixture.listeners.delete(listener);
};
vi.mock(import("~/state/device"), async (original) => ({
  ...(await original()),
  useDeviceState: () => useSyncExternalStore(subscribe, () => fixture.snapshot),
}));
vi.mock(import("~/state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: (() => fixture.command) as never,
}));
vi.mock(import("~/state/session"), async (original) => ({
  ...(await original()),
  useEnvironmentScope: () => true,
  readEnvironmentScope: () => true,
}));

import { DevicePanel } from "./DevicePanel";
import { PlatformStatus, platformSetupStatus } from "./DeviceSetup";

const environmentId = EnvironmentId.make("remote-device-environment");
const threadId = ThreadId.make("device-diagnostic-thread");
const warning =
  "Could not list Android virtual devices (emulator -list-avds, exit code 127). Discovered devices remain available.";
const partialState = (): DeviceServiceState => ({
  hosts: [
    {
      id: "local",
      label: "Linux workstation",
      kind: "local",
      platforms: [
        { platform: "ios", available: false, reason: "Xcode is unavailable." },
        { platform: "android", available: false, reason: "Android tooling is unavailable." },
      ],
      hubInstalled: false,
      agentDeviceInstalled: false,
    },
    {
      id: "remote",
      label: "Remote Mac",
      kind: "ssh",
      platforms: [{ platform: "android", available: true }],
      hubInstalled: true,
      agentDeviceInstalled: true,
    },
  ],
  hostStatus: "idle",
  hostStatuses: { remote: { status: "ready", detail: warning, androidDiscoveryIncomplete: true } },
  devices: [],
  sessions: [],
  supportsHostRetry: true,
  onboardingCompleted: true,
  agentAccessEnabled: false,
  hubBasePath: "/api/device-hub",
  revision: 1,
});
const phone = {
  hostId: "remote",
  id: "phone",
  name: "Pixel phone",
  platform: "android" as const,
  version: "Android 16",
  booted: true,
  physical: true,
};
let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  fixture.snapshot = { state: partialState(), loaded: true };
  fixture.command.mockClear();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
const panel = () => (
  <DevicePanel
    mode="embedded"
    threadRef={{ environmentId, threadId }}
    surface={{ kind: "device", id: "device" }}
    visible
    onDismissSetup={() => {}}
  />
);
const publish = (state: DeviceServiceState) => {
  fixture.snapshot = { state, loaded: true };
  for (const listener of fixture.listeners) listener();
};

it("distinguishes an empty partial remote scan and clears the labeled warning after recovery", async () => {
  await act(async () => root.render(panel()));
  expect(container.textContent).toContain("incomplete");
  expect(container.textContent).not.toContain("No simulators or emulators were found");
  const diagnostic = [...container.querySelectorAll('[role="alert"]')].find((element) =>
    element.textContent?.includes(warning),
  );
  expect(diagnostic?.textContent).toContain("Remote Mac");
  const refresh = page.getByRole("button", { name: "Refresh devices" });
  await expect.element(refresh).toBeEnabled();
  await act(async () =>
    publish({ ...partialState(), hostStatuses: { remote: { status: "ready" } }, revision: 2 }),
  );
  expect(container.textContent).not.toContain(warning);
  expect(container.textContent).not.toContain("incomplete");
  expect(container.textContent).toContain("No simulators or emulators were found");
  await expect.element(refresh).toBeEnabled();
});

it("keeps the known phone actionable alongside the remote discovery warning", async () => {
  fixture.snapshot = { state: { ...partialState(), devices: [phone] }, loaded: true };
  await act(async () => root.render(panel()));
  await expect.element(page.getByRole("button", { name: "Open Pixel phone" })).toBeEnabled();
  expect(container.textContent).toContain(warning);
  expect(container.textContent).toContain("Remote Mac");
});

it("keeps setup progression available while showing incomplete Android discovery with a phone", async () => {
  fixture.snapshot = {
    state: { ...partialState(), devices: [phone], onboardingCompleted: false },
    loaded: true,
  };
  await act(async () => root.render(panel()));
  const dialog = page.getByRole("dialog");
  await expect.element(dialog).toMatchTextContent("may be incomplete");
  await expect.element(dialog).not.toMatchTextContent("SDK and Emulator are available");
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await expect.element(dialog).toMatchTextContent("Allow agent control");
});

it("shows uncertainty in the compact Integrations platform status until discovery recovers", async () => {
  const state = { ...partialState(), devices: [phone] };
  await act(async () =>
    root.render(
      <PlatformStatus compact platform="Android" status={platformSetupStatus(state, "android")} />,
    ),
  );
  expect(container.textContent).toContain("may be incomplete");
  expect(container.textContent).not.toContain("Ready");
  expect(container.textContent).not.toContain("SDK and Emulator are available");
  await act(async () =>
    root.render(
      <PlatformStatus
        compact
        platform="Android"
        status={platformSetupStatus(
          { ...state, hostStatuses: { remote: { status: "ready" } } },
          "android",
        )}
      />,
    ),
  );
  expect(container.textContent).toContain("Ready");
  expect(container.textContent).not.toContain("incomplete");
});
