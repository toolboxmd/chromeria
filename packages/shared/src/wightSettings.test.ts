import { expect, it } from "vite-plus/test";
import { DEFAULT_SERVER_SETTINGS, ThreadId } from "@t3tools/contracts";
import { applyServerSettingsPatch } from "./serverSettings.ts";

it("sets, replaces and clears one thread's idle mode without changing another", () => {
  const first = ThreadId.make("first");
  const second = ThreadId.make("second");
  const wight = {
    type: "wight" as const,
    enabledAt: "2026-10-08T17:00:00.000Z",
    expiresAt: 1_900_000_000_000,
  };
  const enabled = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
    threadIdleModes: { [first]: wight, [second]: wight },
  });
  const warm = {
    type: "keepWarm" as const,
    enabledAt: "2026-10-08T18:00:00.000Z",
    lastPingAt: null,
    stopped: null,
  };
  const replaced = applyServerSettingsPatch(enabled, { threadIdleModes: { [first]: warm } });
  expect(replaced.threadIdleModes[first]).toEqual(warm);
  expect(replaced.threadIdleModes[second]).toEqual(wight);
  const disabled = applyServerSettingsPatch(replaced, { threadIdleModes: { [first]: null } });
  expect(disabled.threadIdleModes).toEqual({ [second]: wight });
  expect(applyServerSettingsPatch(disabled, { autoUpdateProviders: true }).threadIdleModes).toEqual(
    disabled.threadIdleModes,
  );
});
