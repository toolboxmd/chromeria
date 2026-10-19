import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  AuthCookieMirrorWriteScope,
  AuthStandardClientScopes,
  AuthGrantScope,
  sessionGrantsScope,
} from "./auth.ts";
import {
  COOKIE_MIRROR_ENABLE_RPC,
  CookieMirrorBatch,
  CookieMirrorConnectionStatus,
} from "./browserCookieMirror.ts";
import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";

const decode = Schema.decodeUnknownOption(CookieMirrorBatch);
const decodeStatus = Schema.decodeUnknownSync(CookieMirrorConnectionStatus);

describe("cookie mirror contracts", () => {
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

  it("keeps the dedicated write scope out of standard and human-issued grants and guards desktop registration", () => {
    expect(
      (AuthStandardClientScopes as ReadonlyArray<string>).includes(AuthCookieMirrorWriteScope),
    ).toBe(false);
    expect(
      (AuthGrantScope.literals as ReadonlyArray<string>).includes(AuthCookieMirrorWriteScope),
    ).toBe(false);
    expect(clientRpcRequiredScopes(COOKIE_MIRROR_ENABLE_RPC, {})).toEqual(["access:write"]);
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
