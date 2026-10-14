import { describe, expect, it } from "vite-plus/test";

import {
  filterAvailableSettingsSearchItems,
  getSettingsSearchTargetScope,
  isSettingsSearchScopeAvailable,
  searchSettings,
  type SettingsSearchAvailability,
} from "./settingsSearch";

const available = {
  hasCloudPublicConfig: false,
  hasEnvironment: true,
  hasProviderSettingsEnvironment: true,
  hasMacProviderSettingsEnvironment: false,
  canManageLocalBackend: false,
  isWslSettingsRowVisible: false,
  hasThreadAutoSettlement: false,
} satisfies SettingsSearchAvailability;

describe("provider update settings search", () => {
  it("opens both rows on Providers when that page can show Advanced", () => {
    const items = filterAvailableSettingsSearchItems(available);
    for (const [query, id] of [
      ["provider update checks", "provider-update-checks"],
      ["update providers automatically", "auto-update-providers"],
    ] as const) {
      expect(searchSettings(query, items)[0]).toMatchObject({
        id,
        to: "/settings/providers",
        scope: "environment-defaults",
        providerSettingsOnly: true,
      });
    }
    const checks = getSettingsSearchTargetScope("provider-update-checks")!;
    expect(isSettingsSearchScopeAvailable(checks.scope, "project")).toBe(false);
    expect(isSettingsSearchScopeAvailable(checks.scope, "environment")).toBe(true);
  });

  it("hides both rows when no provider settings environment can render them", () => {
    const ids = filterAvailableSettingsSearchItems({
      ...available,
      hasProviderSettingsEnvironment: false,
    }).map((item) => item.id);
    expect(ids).not.toContain("provider-update-checks");
    expect(ids).not.toContain("auto-update-providers");
  });
});
