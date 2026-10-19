import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  AuthCookieMirrorWriteScope,
  AuthStandardClientScopes,
  AuthGrantScope,
  sessionGrantsScope,
} from "./auth.ts";
import {
  CookieMirrorBatch,
  CookieMirrorConnectionStatus,
  CookieMirrorConnectionInput,
} from "./browserCookieMirror.ts";

const decode = Schema.decodeUnknownOption(CookieMirrorBatch);
const decodeStatus = Schema.decodeUnknownSync(CookieMirrorConnectionStatus);
const decodeConnection = Schema.decodeUnknownSync(CookieMirrorConnectionInput);

describe("cookie mirror contracts", () => {
  it("accepts only an environment id from renderer connect IPC, stripping destinations and credentials", () => {
    expect(
      decodeConnection({
        action: "connect",
        environmentId: "env",
        httpBaseUrl: "https://attacker.test",
        headers: { authorization: "synthetic-renderer-token" },
      }),
    ).toEqual({ action: "connect", environmentId: "env" });
  });
  it("excludes Incognito and requires a complete partition key including a false ancestor", () => {
    const batch = {
      environmentId: "env",
      sequence: 0,
      profile: { id: "default", name: "Default", kind: "persistent" },
      mutations: [
        {
          type: "remove",
          key: {
            name: "login",
            domain: "example.test",
            path: "/",
            partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: false },
          },
        },
      ],
    };
    expect(decode(batch)._tag).toBe("Some");
    expect(decode({ ...batch, profile: { ...batch.profile, id: "incognito" } })._tag).toBe("None");
    expect(
      decode({
        ...batch,
        mutations: [
          {
            type: "remove",
            key: { ...batch.mutations[0]!.key, partitionKey: { topLevelSite: "https://top.test" } },
          },
        ],
      })._tag,
    ).toBe("None");
  });

  it("requires an explicit dedicated credential, outside standard client grants", () => {
    expect(
      (AuthStandardClientScopes as ReadonlyArray<string>).includes(AuthCookieMirrorWriteScope),
    ).toBe(false);
    expect(
      (AuthGrantScope.literals as ReadonlyArray<string>).includes(AuthCookieMirrorWriteScope),
    ).toBe(true);
    expect(
      sessionGrantsScope(
        { authenticated: true, scopes: ["access:write", "orchestration:operate"] },
        AuthCookieMirrorWriteScope,
      ),
    ).toBe(false);
  });

  it("returns connection and profile metadata to the renderer without cookie or grant fields", () => {
    const status = decodeStatus({
      connected: true,
      generation: 1,
      profiles: [],
      limitations: [{ host: "accounts.google.com", reason: "device-bound, not mirrored" }],
      cookies: ["synthetic-private-value"],
      ticket: "synthetic-private-ticket",
    });
    expect(status.limitations).toEqual([
      { host: "accounts.google.com", reason: "device-bound, not mirrored" },
    ]);
    expect(JSON.stringify(status)).not.toContain("synthetic-private");
  });
});
