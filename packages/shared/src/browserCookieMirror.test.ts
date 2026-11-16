import { describe, expect, it, vi } from "vite-plus/test";
import { type MirrorCookie } from "@t3tools/contracts";

import { cdpCookieMirrorStore } from "./browserCookieMirror.ts";

const cookie: MirrorCookie = {
  name: "synthetic-login",
  value: "synthetic-only",
  domain: "example.test",
  path: "/",
  secure: true,
  httpOnly: true,
  sameSite: "unspecified",
};

describe("CDP mirror boundary", () => {
  it("binds Electron observations and writes to its partition's target", async () => {
    const send = vi.fn(async (method: string) =>
      method === "Network.getAllCookies" ? { cookies: [] } : {},
    );
    const store = cdpCookieMirrorStore({ send }, "target");
    expect(await store.read()).toEqual([]);
    await store.apply([{ type: "set", cookie }]);
    expect(send.mock.calls.map(([method]) => method)).toEqual([
      "Network.getAllCookies",
      "Network.setCookies",
    ]);
  });
  it("does not invent SameSite, a domain scope or an expiry on a host-only session cookie", async () => {
    const send = vi.fn(async () => ({}));
    await cdpCookieMirrorStore({ send }).apply([{ type: "set", cookie }]);
    expect(send).toHaveBeenCalledWith("Storage.setCookies", {
      cookies: [
        {
          name: "synthetic-login",
          value: "synthetic-only",
          url: "https://example.test/",
          path: "/",
          secure: true,
          httpOnly: true,
        },
      ],
    });
  });

  it("uses both partition-key fields when writing and removing, including false ancestors", async () => {
    const send = vi.fn(async () => ({}));
    const partitionKey = { topLevelSite: "https://top.test", hasCrossSiteAncestor: false };
    const domainCookie = {
      ...cookie,
      domain: ".example.test",
      sameSite: "strict" as const,
      expires: 2_000_000_000,
      partitionKey,
    };
    const store = cdpCookieMirrorStore({ send });
    await store.apply([
      { type: "set", cookie: domainCookie },
      {
        type: "remove",
        key: { name: cookie.name, domain: ".example.test", path: "/", partitionKey },
      },
    ]);
    expect(send.mock.calls).toEqual([
      [
        "Storage.setCookies",
        {
          cookies: [
            {
              name: cookie.name,
              value: cookie.value,
              url: "https://example.test/",
              domain: ".example.test",
              path: "/",
              secure: true,
              httpOnly: true,
              sameSite: "Strict",
              expires: 2_000_000_000,
              partitionKey,
            },
          ],
        },
      ],
      [
        "Network.deleteCookies",
        { name: cookie.name, domain: ".example.test", path: "/", partitionKey },
      ],
    ]);
  });

  it("redacts failed reads and writes even when the browser error contains a cookie payload", async () => {
    const sentinel = "synthetic-cookie-do-not-log";
    const send = vi.fn(async () => {
      throw new Error(sentinel);
    });
    const store = cdpCookieMirrorStore({ send });
    const readError = await store.read().catch((error: unknown) => error);
    const writeError = await store
      .apply([{ type: "set", cookie }])
      .catch((error: unknown) => error);
    expect(String(readError)).toBe("Error: Could not read browser mirror cookies.");
    expect(String(writeError)).toBe("Error: Could not apply browser mirror cookies.");
    expect(JSON.stringify([readError, writeError])).not.toContain(sentinel);
  });

  it("rejects an opaque partition and malformed browser data as a failed whole observation", async () => {
    const browserCookie = { ...cookie, session: true, expires: -1, partitionKeyOpaque: true };
    const store = cdpCookieMirrorStore({ send: async () => ({ cookies: [browserCookie] }) });
    await expect(store.read()).rejects.toThrow("Could not read browser mirror cookies.");
    const invalid = cdpCookieMirrorStore({
      send: async () => ({ cookies: [{ value: "synthetic-schema-sentinel" }] }),
    });
    await expect(invalid.read()).rejects.toThrow("Could not read browser mirror cookies.");
  });
});
