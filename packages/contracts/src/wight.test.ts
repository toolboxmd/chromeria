import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "./settings.ts";
import { WightMode } from "./wight.ts";

const activation = { enabledAt: "2026-10-08T17:00:00.000Z", expiresAt: null };

describe("Wight settings", () => {
  it("preserves v1 activations and instance quota limits through unrelated settings writes", () => {
    const input = {
      wightModes: { existing: activation },
      providerInstances: { codex: { driver: "codex", wightLimitPercent: 35 } },
    };
    const decoded = Schema.decodeUnknownSync(ServerSettings)(input);
    expect(
      Schema.encodeSync(ServerSettings)({ ...decoded, autoUpdateProviders: true }),
    ).toMatchObject(input);
    expect(Schema.decodeUnknownSync(ServerSettings)({}).wightModes).toEqual({});
    expect(
      Schema.decodeUnknownSync(ServerSettingsPatch)({ wightModes: { existing: null } }),
    ).toEqual({
      wightModes: { existing: null },
    });
  });

  it.each([-1, 101, 1.5, Number.POSITIVE_INFINITY])("rejects invalid quota %s", (limit) => {
    expect(() =>
      Schema.decodeUnknownSync(ServerSettingsPatch)({
        providerInstances: { codex: { driver: "codex", wightLimitPercent: limit } },
      }),
    ).toThrow();
  });

  it.each([-1, Number.POSITIVE_INFINITY])("rejects invalid deadline %s", (expiresAt) => {
    expect(() => Schema.decodeUnknownSync(WightMode)({ ...activation, expiresAt })).toThrow();
  });
});
