import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { autoBalanceMigrationPatch } from "./loadBalancing.ts";
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
