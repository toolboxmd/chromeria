import { expect, it } from "vite-plus/test";
import { DEFAULT_SERVER_SETTINGS, ThreadId } from "@t3tools/contracts";
import { applyServerSettingsPatch } from "./serverSettings.ts";

it("enables, replaces and disables one Wight activation without changing another", () => {
  const first = ThreadId.make("first");
  const second = ThreadId.make("second");
  const activation = { enabledAt: "2026-10-08T17:00:00.000Z", expiresAt: 1_900_000_000_000 };
  const enabled = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
    wightModes: { [first]: activation, [second]: activation },
  });
  const replaced = applyServerSettingsPatch(enabled, {
    wightModes: { [first]: { ...activation, expiresAt: null } },
  });
  expect(replaced.wightModes[first]?.expiresAt).toBeNull();
  expect(replaced.wightModes[second]).toEqual(activation);
  const disabled = applyServerSettingsPatch(replaced, { wightModes: { [first]: null } });
  expect(disabled.wightModes).toEqual({ [second]: activation });
  expect(applyServerSettingsPatch(disabled, { autoUpdateProviders: true }).wightModes).toEqual(
    disabled.wightModes,
  );
});
