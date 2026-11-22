import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { autoBalanceMigrationPatch, resolveAutoBalance } from "./loadBalancing.ts";
import { applyServerSettingsPatch } from "./serverSettings.ts";

describe("Auto balance migration", () => {
  const legacy = { enabled: true, weights: { "environment-bestia": 0 } };

  it("moves a client's choice, Manual only included, onto a server that has none", () => {
    const patch = autoBalanceMigrationPatch(DEFAULT_SERVER_SETTINGS.autoBalance, legacy);
    expect(applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, patch!).autoBalance).toEqual(legacy);
  });

  it("never overwrites a policy the server already has", () => {
    const established = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
      autoBalance: { enabled: false, weights: {} },
    });
    expect(autoBalanceMigrationPatch(established.autoBalance, legacy)).toBeNull();
    // Another client's migration that raced the first write is ignored by the server.
    expect(
      applyServerSettingsPatch(established, { migrateAutoBalance: legacy }).autoBalance,
    ).toEqual({ enabled: false, weights: {} });
  });

  it("lets a client with default choices claim nothing", () => {
    expect(autoBalanceMigrationPatch(null, { enabled: false, weights: {} })).toBeNull();
  });
});

describe("the Auto balance a client acts on", () => {
  const legacy = { enabled: true, weights: { "environment-bestia": 0 } };
  const server = { enabled: false, weights: {} };

  it.each([
    ["a client without a primary uses its own", false, undefined, legacy, true],
    [
      "an unknown primary reads as off until it arrives",
      true,
      undefined,
      { enabled: false, weights: {} },
      false,
    ],
    ["a primary that never set it shows the client's until migrated", true, null, legacy, true],
    ["a primary's own setting wins", true, server, server, true],
  ] as const)("%s", (_name, hasPrimary, primary, autoBalance, ready) => {
    expect(resolveAutoBalance({ hasPrimary, primary, legacy })).toEqual({ autoBalance, ready });
  });
});
