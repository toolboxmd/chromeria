import { verifyDpopProof } from "@t3tools/shared/dpop";
import * as Schema from "effect/Schema";

const decode = Schema.decodeUnknownSync(Schema.String);

/** Uses the app window's existing non-extractable key to sign a main-selected destination. */
export async function cookieMirrorDpopProof(input: {
  readonly execute: (script: string) => Promise<unknown>;
  readonly method: string;
  readonly url: string;
  readonly token: string;
  readonly thumbprint: string;
  readonly nowEpochSeconds: number;
}) {
  const proof = decode(
    await input.execute(`(async () => {
    const [method, url, token] = ${JSON.stringify([input.method, input.url, input.token])};
    const database = await new Promise((resolve, reject) => {
      const request = indexedDB.open("t3code:cloud-auth", 1);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(new Error("Key unavailable"));
    });
    let key;
    try { key = await new Promise((resolve, reject) => {
      const request = database.transaction("keys", "readonly").objectStore("keys").get("relay-dpop-proof-key");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(new Error("Key unavailable"));
    }); } finally { database.close(); }
    if (!key) throw new Error("Key unavailable");
    const encode = bytes => btoa(String.fromCharCode(...new Uint8Array(bytes))).replace(/=/g, "").replace(/\\+/g, "-").replace(/\\//g, "_");
    const text = value => new TextEncoder().encode(JSON.stringify(value));
    const normalized = new URL(url); normalized.search = ""; normalized.hash = "";
    const header = encode(text({ typ: "dpop+jwt", alg: "ES256", jwk: key.publicJwk }));
    const payload = encode(text({ htm: method, htu: normalized.href, jti: crypto.randomUUID(), iat: Math.floor(Date.now()/1000), ath: encode(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token))) }));
    const bytes = header + "." + payload;
    return bytes + "." + encode(await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, key.privateKey, new TextEncoder().encode(bytes)));
  })()`),
  );
  if (
    !verifyDpopProof({
      proof,
      method: input.method,
      url: input.url,
      nowEpochSeconds: input.nowEpochSeconds,
      expectedThumbprint: input.thumbprint,
      expectedAccessToken: input.token,
    }).ok
  )
    throw new Error("Invalid desktop connection proof.");
  return proof;
}
