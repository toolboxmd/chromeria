import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "./settings.ts";
import { WightMode } from "./wight.ts";

const decodeSettings = Schema.decodeUnknownSync(ServerSettings);
const encodeSettings = Schema.encodeSync(ServerSettings);
const decodePatch = Schema.decodeUnknownSync(ServerSettingsPatch);
const decodeMode = Schema.decodeUnknownSync(WightMode);

const activation = { enabledAt: "2026-10-08T17:00:00.000Z", expiresAt: null };

describe("Wight settings", () => {
  it("keeps instance quota limits and one idle mode per thread through unrelated writes", () => {
    const input = {
      threadIdleModes: {
        wight: { type: "wight", ...activation },
        warm: {
          type: "keepWarm",
          enabledAt: activation.enabledAt,
          lastPingAt: null,
          stopped: null,
        },
      },
      providerInstances: { codex: { driver: "codex", wightLimitPercent: 35 } },
    };
    const decoded = decodeSettings(input);
    expect(encodeSettings({ ...decoded, autoUpdateProviders: true })).toMatchObject(input);
    expect(decodeSettings({}).threadIdleModes).toEqual({});
    expect(decodePatch({ threadIdleModes: { wight: null } })).toEqual({
      threadIdleModes: { wight: null },
    });
    expect(() =>
      decodePatch({ threadIdleModes: { both: { ...activation, lastPingAt: null } } }),
    ).toThrow();
  });

  it.each([-1, 101, 1.5, Number.POSITIVE_INFINITY])("rejects invalid quota %s", (limit) => {
    expect(() =>
      decodePatch({
        providerInstances: { codex: { driver: "codex", wightLimitPercent: limit } },
      }),
    ).toThrow();
  });

  it.each([-1, Number.POSITIVE_INFINITY])("rejects invalid deadline %s", (expiresAt) => {
    expect(() => decodeMode({ ...activation, expiresAt })).toThrow();
  });
});
