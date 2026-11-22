import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { PeerCeiling, PeerGrant, PeerGrantSummary, peerCapabilities } from "./peerAuthority.ts";

describe("PeerGrant", () => {
  it("decodes a grant stored before Share browser logins existed as not sharing them", () => {
    const decode = Schema.decodeUnknownSync(Schema.fromJsonString(PeerGrant));
    const legacy = decode(
      '{"sessionId":"session-1","resourceScope":"created-work","onward":false,"preview":true,"device":false}',
    );
    expect(legacy.browserExport).toBe(false);
    expect(decode(JSON.stringify({ ...legacy, browserExport: true })).browserExport).toBe(true);
  });

  it.each([
    ["PeerGrant", PeerGrant, {}],
    ["PeerGrantSummary", PeerGrantSummary, { environmentId: "env-1", label: "Rocky" }],
  ] as const)(
    "decodes a %s without Use credential vault as following Browser access",
    (_, schema, extra) => {
      const decode = Schema.decodeUnknownSync(Schema.fromJsonString(schema));
      const stored = (preview: boolean, vault?: boolean) =>
        decode(
          JSON.stringify({
            sessionId: "session-1",
            resourceScope: "created-work",
            onward: false,
            preview,
            device: false,
            ...extra,
            ...(vault === undefined ? {} : { vault }),
          }),
        ).vault;
      expect(stored(true)).toBe(true);
      expect(stored(false)).toBe(false);
      expect(stored(true, false)).toBe(false);
      expect(stored(false, true)).toBe(true);
    },
  );
});

describe("PeerCeiling", () => {
  // Every ceiling a server forwards, records or answers in a validate frame uses this schema.
  it("keeps the peer capability set older environments decode, dropping local-only capabilities", () => {
    const decode = Schema.decodeUnknownExit(PeerCeiling);
    const ceiling = {
      runtimeMode: "full-access",
      interactionMode: "default",
      resourceScope: "environment-resources",
      onward: true,
    };
    const local = ["orchestration", "worktree", "pull-requests", "issues", "preview", "device"];
    expect(decode({ ...ceiling, capabilities: local })._tag).toBe("Failure");
    expect(decode({ ...ceiling, capabilities: peerCapabilities(local) })._tag).toBe("Success");
  });
});
