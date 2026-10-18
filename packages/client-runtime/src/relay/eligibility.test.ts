import { EnvironmentId } from "@t3tools/contracts";
import type { RelayClientEnvironmentRecord } from "@t3tools/contracts/relay";
import { describe, expect, it } from "vite-plus/test";

import { isAddableRelayEnvironment } from "./eligibility.ts";

describe("isAddableRelayEnvironment", () => {
  it.each([
    ["cloudflare_tunnel", true],
    ["manual", false],
    ["t3_relay", false],
  ] as const)("offers %s as a relay: %s", (providerKind, expected) => {
    const environment: RelayClientEnvironmentRecord = {
      environmentId: EnvironmentId.make("environment"),
      label: "Environment",
      endpoint: {
        providerKind,
        httpBaseUrl: "https://environment.example.test",
        wsBaseUrl: "wss://environment.example.test/ws",
      },
      linkedAt: "2026-10-18T00:00:00.000Z",
    };

    expect(isAddableRelayEnvironment(environment)).toBe(expected);
  });

  it("does not offer an environment missing from discovery", () => {
    expect(isAddableRelayEnvironment(undefined)).toBe(false);
  });
});
