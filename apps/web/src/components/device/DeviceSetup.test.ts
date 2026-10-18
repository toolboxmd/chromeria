import type { DeviceServiceState } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { deviceHubReadiness, platformSetupStatus } from "./DeviceSetup";

const deviceState = (overrides: Partial<DeviceServiceState> = {}): DeviceServiceState => ({
  hosts: [
    {
      id: "local",
      kind: "local",
      label: "This machine",
      hubInstalled: false,
      agentDeviceInstalled: false,
      platforms: [
        { platform: "ios", available: true },
        { platform: "android", available: true },
      ],
    },
  ],
  hostStatus: "ready",
  hostStatuses: {},
  devices: [],
  sessions: [],
  onboardingCompleted: false,
  agentAccessEnabled: false,
  hubBasePath: "/api/device-hub",
  revision: 0,
  ...overrides,
});

describe("device setup guidance", () => {
  it("waits for complete Android discovery before advising creation of an AVD", () => {
    const state = deviceState({
      hostStatus: "idle",
      hosts: [{ ...deviceState().hosts[0]!, id: "remote", kind: "ssh" }],
      hostStatuses: { remote: { status: "ready", androidDiscoveryIncomplete: true } },
    });
    expect(platformSetupStatus(state, "android").message).toContain("may be incomplete");
    expect(platformSetupStatus(state, "ios").message).toContain("Xcode Settings");
    expect(
      platformSetupStatus({ ...state, hostStatuses: { remote: { status: "ready" } } }, "android")
        .message,
    ).toContain("Device Manager");
  });

  it.each([true, false])(
    "keeps Android discovery uncertain with a known device (physical=%s)",
    (physical) => {
      const device = {
        hostId: "remote",
        id: "phone",
        name: "Known Android device",
        platform: "android" as const,
        version: "Android 16",
        booted: true,
        physical,
      };
      const state = deviceState({
        hostStatus: "idle",
        hosts: [
          {
            id: "remote",
            label: "Remote Mac",
            kind: "ssh",
            hubInstalled: true,
            agentDeviceInstalled: true,
            platforms: [{ platform: "android", available: true }],
          },
        ],
        hostStatuses: { remote: { status: "ready", androidDiscoveryIncomplete: true } },
        devices: [device],
      });
      const partial = platformSetupStatus(state, "android");
      expect(partial.ready).toBe(false);
      expect(partial.message).toContain("may be incomplete");
      expect(partial.message).not.toContain("SDK and Emulator are available");
      expect(deviceHubReadiness(state).ready).toBe(true);
      expect(state.devices).toEqual([device]);
      const recovered = { ...state, hostStatuses: { remote: { status: "ready" as const } } };
      expect(platformSetupStatus(recovered, "android").ready).toBe(true);
      expect(platformSetupStatus(recovered, "android").message).not.toContain("incomplete");
    },
  );

  it("preserves a specific missing-tool explanation from the server", () => {
    const state = deviceState({
      hosts: [
        {
          ...deviceState().hosts[0]!,
          platforms: [
            { platform: "ios", available: true },
            { platform: "android", available: false, reason: "Android Emulator is missing." },
          ],
        },
      ],
    });
    expect(platformSetupStatus(state, "android").message).toBe("Android Emulator is missing.");
  });

  it("counts a remote device host when this machine has no simulator tooling", () => {
    const state = deviceState({
      hostStatus: "idle",
      hostStatuses: { macmini: { status: "ready" } },
      hosts: [
        {
          ...deviceState().hosts[0]!,
          platforms: [
            { platform: "ios", available: false, reason: "Xcode was not found." },
            { platform: "android", available: false, reason: "Android SDK was not found." },
          ],
        },
        {
          id: "macmini",
          kind: "ssh",
          label: "Mac Mini",
          hubInstalled: true,
          agentDeviceInstalled: true,
          platforms: [
            { platform: "ios", available: true },
            { platform: "android", available: false, reason: "Android SDK was not found." },
          ],
        },
      ],
      devices: [
        {
          hostId: "macmini",
          id: "iphone",
          platform: "ios",
          name: "iPhone 16 Pro",
          version: "iOS 27.0",
          booted: true,
          physical: false,
        },
      ],
    });
    expect(deviceHubReadiness(state).ready).toBe(true);
    expect(platformSetupStatus(state, "ios")).toEqual({
      ready: true,
      message: "Xcode and iOS Simulator are available on Mac Mini.",
    });
    expect(platformSetupStatus(state, "android").ready).toBe(false);
  });
});
