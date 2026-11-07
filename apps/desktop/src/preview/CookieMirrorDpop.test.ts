// @effect-diagnostics nodeBuiltinImport:off - Runs the app-window signing script against synthetic IndexedDB and a real WebCrypto key.
import * as NodeVM from "node:vm";
import { describe, expect, it, vi } from "vite-plus/test";
import { computeDpopJwkThumbprint, verifyDpopProof, DpopPublicJwk } from "@t3tools/shared/dpop";
import * as Schema from "effect/Schema";
import { cookieMirrorDpopProof } from "./CookieMirrorDpop.ts";
const decodePublicJwk = Schema.decodeUnknownSync(DpopPublicJwk);

describe("saved relay connection proof", () => {
  it("signs only the main-selected URL and parent token with the existing non-extractable key", async () => {
    const nowEpochSeconds = 2_000_000_000;
    const generated = await crypto.subtle.generateKey(
      { name: "ECDSA", namedCurve: "P-256" },
      true,
      ["sign", "verify"],
    );
    const publicJwk = decodePublicJwk(await crypto.subtle.exportKey("jwk", generated.publicKey));
    const privateKey = await crypto.subtle.importKey(
      "jwk",
      await crypto.subtle.exportKey("jwk", generated.privateKey),
      { name: "ECDSA", namedCurve: "P-256" },
      false,
      ["sign"],
    );
    const thumbprint = computeDpopJwkThumbprint(publicJwk);
    const complete = (result: unknown) => {
      const request = { result, onsuccess: () => {} };
      queueMicrotask(() => request.onsuccess());
      return request;
    };
    const get = vi.fn(() => complete({ privateKey, publicJwk, thumbprint }));
    const close = vi.fn();
    const indexedDB = {
      open: vi.fn(() => complete({ transaction: () => ({ objectStore: () => ({ get }) }), close })),
    };
    const url = "https://saved.test/api/browser-cookie-mirror/grant";
    const token = 'synthetic-parent-";untrusted-text';
    const proof = await cookieMirrorDpopProof({
      execute: (script) =>
        NodeVM.runInNewContext(script, {
          indexedDB,
          crypto,
          URL,
          TextEncoder,
          Uint8Array,
          btoa,
          Date: { now: () => nowEpochSeconds * 1000 },
        }),
      method: "POST",
      url,
      token,
      thumbprint,
      nowEpochSeconds,
    });
    expect(privateKey.extractable).toBe(false);
    expect(
      verifyDpopProof({
        proof,
        method: "POST",
        url,
        expectedAccessToken: token,
        expectedThumbprint: thumbprint,
        nowEpochSeconds,
      }).ok,
    ).toBe(true);
    expect(
      verifyDpopProof({
        proof,
        method: "POST",
        url: "https://attacker.test",
        expectedAccessToken: token,
        nowEpochSeconds,
      }).ok,
    ).toBe(false);
    expect(get).toHaveBeenCalledExactlyOnceWith("relay-dpop-proof-key");
    expect(close).toHaveBeenCalledOnce();
    await expect(
      cookieMirrorDpopProof({
        execute: async () => proof,
        method: "POST",
        url,
        token: "wrong-token",
        thumbprint,
        nowEpochSeconds,
      }),
    ).rejects.toThrow("Invalid desktop connection proof.");
  });
});
