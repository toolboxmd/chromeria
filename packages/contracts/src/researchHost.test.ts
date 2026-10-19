import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { EnvironmentId } from "./baseSchemas.ts";
import { AuthSettingsWriteScope } from "./auth.ts";
import {
  ServerSettings,
  ServerSettingsPatch,
  requiredScopesForServerSettingsPatch,
} from "./settings.ts";

const decode = Schema.decodeUnknownSync(ServerSettings);
const patch = Schema.decodeUnknownSync(ServerSettingsPatch);
const encode = Schema.encodeSync(ServerSettings);

describe("environment research host settings", () => {
  it("defaults to local, round-trips a host, and permits clearing it", () => {
    expect(decode({}).webResearch).toMatchObject({
      host: null,
      searxngUrl: "https://bestia.napoleon-sun.ts.net:8888",
    });
    const selected = { webResearch: { host: "bestia" } };
    expect(encode(decode(selected))).toMatchObject(selected);
    expect(patch(selected)).toEqual(selected);
    expect(patch({ webResearch: { host: null } })).toEqual({ webResearch: { host: null } });
  });

  it("rejects malformed hosts and requires settings permission to select or clear one", () => {
    for (const host of ["", " ", 42, {}]) {
      expect(() => decode({ webResearch: { host } })).toThrow();
      expect(() => patch({ webResearch: { host } })).toThrow();
    }
    for (const host of [null, EnvironmentId.make("bestia")]) {
      expect(requiredScopesForServerSettingsPatch({ webResearch: { host } })).toEqual([
        AuthSettingsWriteScope,
      ]);
    }
  });
  it("validates the configured provider endpoint and keeps its writes under settings permission", () => {
    for (const searxngUrl of [
      "file:///etc/passwd",
      "https://user:secret@example.com",
      "https://example.com/?token=secret",
      "https://example.com/#secret",
      "",
      42,
    ]) {
      expect(() => decode({ webResearch: { searxngUrl } })).toThrow();
      expect(() => patch({ webResearch: { searxngUrl } })).toThrow();
    }
    const selected = { webResearch: { searxngUrl: "http://configured-search.internal:8888/base" } };
    expect(patch(selected)).toEqual(selected);
    expect(encode(decode(selected))).toMatchObject(selected);
    expect(requiredScopesForServerSettingsPatch(patch(selected))).toEqual([AuthSettingsWriteScope]);
  });
});
