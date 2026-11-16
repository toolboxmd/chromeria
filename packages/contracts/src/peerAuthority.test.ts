import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { PeerGrant } from "./peerAuthority.ts";

describe("PeerGrant", () => {
  it("decodes a grant stored before Share browser logins existed as not sharing them", () => {
    const decode = Schema.decodeUnknownSync(Schema.fromJsonString(PeerGrant));
    const legacy = decode(
      '{"sessionId":"session-1","resourceScope":"created-work","onward":false,"preview":true,"device":false}',
    );
    expect(legacy.browserExport).toBe(false);
    expect(decode(JSON.stringify({ ...legacy, browserExport: true })).browserExport).toBe(true);
  });
});
