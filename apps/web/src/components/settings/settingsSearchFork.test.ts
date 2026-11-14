import { describe, expect, it } from "vite-plus/test";

import { SETTINGS_SEARCH_ITEMS } from "./settingsSearch";

describe("fork-added search entries", () => {
  it("flags exactly the fork-added entries for the Chromeria mark", () => {
    const flagged = SETTINGS_SEARCH_ITEMS.filter(
      (item) => "forkAdded" in item && item.forkAdded === true,
    )
      .map((item) => item.id)
      .sort();
    expect(flagged).toEqual([
      "auto-update-providers",
      "prism-capacity",
      "prism-roles",
      "promachos-colors",
      "spectrum-defaults",
    ]);
  });
});
