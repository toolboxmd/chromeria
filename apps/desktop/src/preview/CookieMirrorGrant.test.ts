import { EnvironmentId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { requestCookieMirrorGrant } from "./CookieMirrorGrant.ts";

const input = {
  action: "connect",
  environmentId: EnvironmentId.make("synthetic-environment"),
  httpBaseUrl: "https://synthetic.test/relay/",
  headers: { authorization: "DPoP synthetic-parent-token", dpop: "synthetic-proof" },
} as const;
const resolve = async () => ({ httpBaseUrl: input.httpBaseUrl, headers: input.headers });
afterEach(() => vi.unstubAllGlobals());

describe("desktop main mirror grant", () => {
  it("ignores a renderer-supplied attacker URL and uses only main's saved authenticated destination", async () => {
    const fetch = vi.fn(async () =>
      Response.json({ ticket: "synthetic-ticket", environmentId: input.environmentId }),
    );
    vi.stubGlobal("fetch", fetch);
    const resolver = vi.fn(async () => ({
      httpBaseUrl: "https://trusted.test/",
      headers: { authorization: "Bearer main-parent" },
    }));
    const attackerInput = { ...input, httpBaseUrl: "https://attacker.test/" };
    const socketUrl = await requestCookieMirrorGrant(attackerInput, resolver);
    expect(fetch).toHaveBeenCalledWith(
      "https://trusted.test/api/browser-cookie-mirror/grant",
      expect.objectContaining({ headers: { authorization: "Bearer main-parent" } }),
    );
    expect(socketUrl).toBe(
      "wss://trusted.test/api/browser-cookie-mirror?wsTicket=synthetic-ticket",
    );
  });

  it("makes no network request for an environment absent from main's authenticated catalog", async () => {
    const fetch = vi.fn(async () =>
      Response.json({ ticket: "synthetic-ticket", environmentId: input.environmentId }),
    );
    vi.stubGlobal("fetch", fetch);
    await expect(
      requestCookieMirrorGrant(input, async () => {
        throw new Error("Unknown environment");
      }),
    ).rejects.toThrow("Could not authorize browser cookie mirroring.");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("uses the existing authenticated environment endpoint and keeps its grant in main", async () => {
    const fetch = vi.fn(async () =>
      Response.json({ ticket: "synthetic-one-time-ticket", environmentId: input.environmentId }),
    );
    vi.stubGlobal("fetch", fetch);
    const socketUrl = await requestCookieMirrorGrant(input, resolve);
    expect(fetch.mock.calls[0]).toEqual([
      "https://synthetic.test/api/browser-cookie-mirror/grant",
      expect.objectContaining({ method: "POST", headers: input.headers, redirect: "error" }),
    ]);
    expect(socketUrl).toBe(
      "wss://synthetic.test/api/browser-cookie-mirror?wsTicket=synthetic-one-time-ticket",
    );
  });

  it("rejects wrong-destination and malformed grants without exposing the response or credentials", async () => {
    for (const response of [
      { ticket: "synthetic-private-sentinel", environmentId: "wrong-environment" },
      { invalid: "synthetic-private-sentinel" },
    ]) {
      vi.stubGlobal("fetch", async () => Response.json(response));
      await expect(requestCookieMirrorGrant(input, resolve)).rejects.toThrow(
        "Could not authorize browser cookie mirroring.",
      );
    }
    vi.stubGlobal("fetch", async () => {
      throw new Error("synthetic-private-sentinel");
    });
    const error = await requestCookieMirrorGrant(input, resolve).catch((error: unknown) => error);
    expect(String(error)).not.toContain("synthetic-private-sentinel");
  });
});
