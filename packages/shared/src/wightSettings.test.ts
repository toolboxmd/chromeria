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
    hiddenRequestAt: null,
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

it("a conditional idle-mode write applies only to the activation it read", () => {
  const thread = ThreadId.make("thread");
  const warm = {
    type: "keepWarm" as const,
    enabledAt: "2026-11-23T10:00:00.000Z",
    hiddenRequestAt: null,
    stopped: null,
  };
  const settings = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
    threadIdleModes: { [thread]: warm },
  });
  const pinged = { ...warm, hiddenRequestAt: "2026-11-23T10:55:00.000Z" };
  // Wight turned on with the same timestamp is a different activation.
  const wight = { type: "wight" as const, enabledAt: warm.enabledAt, expiresAt: null };
  expect(
    applyServerSettingsPatch(settings, {
      threadIdleModes: { [thread]: wight },
      threadIdleModesIfCurrent: {
        [thread]: { expected: { type: "keepWarm", enabledAt: warm.enabledAt }, next: pinged },
      },
    }).threadIdleModes[thread],
  ).toEqual(wight);
  expect(
    applyServerSettingsPatch(settings, {
      threadIdleModesIfCurrent: {
        [thread]: { expected: { type: "keepWarm", enabledAt: warm.enabledAt }, next: pinged },
      },
    }).threadIdleModes[thread],
  ).toEqual(pinged);
  // The user turned it off, or chose Wight, after the loop read the entry.
  const off = applyServerSettingsPatch(settings, { threadIdleModes: { [thread]: null } });
  expect(
    applyServerSettingsPatch(off, {
      threadIdleModesIfCurrent: {
        [thread]: { expected: { type: "keepWarm", enabledAt: warm.enabledAt }, next: pinged },
      },
    }).threadIdleModes,
  ).toEqual({});
});
