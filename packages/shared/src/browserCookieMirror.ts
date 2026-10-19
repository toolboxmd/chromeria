import {
  cookieMirrorKey,
  type CookieMirrorKey,
  type CookieMirrorMutation,
  type MirrorCookie,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const CdpCookie = Schema.Struct({
  name: Schema.String,
  value: Schema.String,
  domain: Schema.String,
  path: Schema.String,
  secure: Schema.Boolean,
  httpOnly: Schema.Boolean,
  session: Schema.Boolean,
  expires: Schema.Number,
  sameSite: Schema.optionalKey(Schema.Literals(["None", "Lax", "Strict"])),
  partitionKey: Schema.optionalKey(
    Schema.Struct({ topLevelSite: Schema.String, hasCrossSiteAncestor: Schema.Boolean }),
  ),
  partitionKeyOpaque: Schema.optionalKey(Schema.Boolean),
});
const decodeCookies = Schema.decodeUnknownSync(Schema.Struct({ cookies: Schema.Array(CdpCookie) }));

export interface CookieMirrorCdp {
  send(method: "Storage.getCookies" | "Network.getAllCookies"): Promise<unknown>;
  send(
    method: "Storage.setCookies" | "Network.setCookies",
    params: {
      cookies: Array<{
        name: string;
        value: string;
        url: string;
        domain?: string;
        path: string;
        secure: boolean;
        httpOnly: boolean;
        sameSite?: "None" | "Lax" | "Strict";
        expires?: number;
        partitionKey?: NonNullable<CookieMirrorKey["partitionKey"]>;
      }>;
    },
  ): Promise<unknown>;
  send(method: "Network.deleteCookies", params: CookieMirrorKey): Promise<unknown>;
}

export interface CookieMirrorStore {
  readonly read: () => Promise<ReadonlyArray<MirrorCookie>>;
  readonly apply: (mutations: ReadonlyArray<CookieMirrorMutation>) => Promise<void>;
}

const sameSiteToCdp = { no_restriction: "None", lax: "Lax", strict: "Strict" } as const;
// CDP's seconds round trip through Chromium's millisecond timestamp conversion.
const expiryMillis = (expires: number) => Math.round(expires * 1000);

const cookieMirrorUrl = (cookie: CookieMirrorKey, secure: boolean) => {
  const host = cookie.domain.replace(/^\./, "");
  const authority = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
  return `${secure ? "https" : "http"}://${authority}${cookie.path}`;
};

/** A private CDP connection keeps partition keys and unspecified SameSite intact. */
export function cdpCookieMirrorStore(
  cdp: CookieMirrorCdp,
  scope: "browser" | "target" = "browser",
): CookieMirrorStore {
  return {
    read: async () => {
      try {
        const { cookies } = decodeCookies(
          await cdp.send(scope === "browser" ? "Storage.getCookies" : "Network.getAllCookies"),
        );
        return cookies.map((cookie): MirrorCookie => {
          if (cookie.partitionKeyOpaque) throw new Error("Opaque partition");
          return {
            name: cookie.name,
            value: cookie.value,
            domain: cookie.domain,
            path: cookie.path,
            secure: cookie.secure,
            httpOnly: cookie.httpOnly,
            sameSite:
              cookie.sameSite === "None"
                ? "no_restriction"
                : cookie.sameSite === "Lax"
                  ? "lax"
                  : cookie.sameSite === "Strict"
                    ? "strict"
                    : "unspecified",
            ...(!cookie.session && cookie.expires > 0
              ? { expires: expiryMillis(cookie.expires) / 1000 }
              : {}),
            ...(cookie.partitionKey ? { partitionKey: cookie.partitionKey } : {}),
          };
        });
      } catch {
        // Protocol and schema errors may embed the cookie payload.
        throw new Error("Could not read browser mirror cookies.");
      }
    },
    apply: async (mutations) => {
      try {
        for (const mutation of mutations) {
          if (mutation.type === "remove") {
            await cdp.send("Network.deleteCookies", { ...mutation.key });
          } else {
            const cookie = mutation.cookie;
            await cdp.send(scope === "browser" ? "Storage.setCookies" : "Network.setCookies", {
              cookies: [
                {
                  name: cookie.name,
                  value: cookie.value,
                  url: cookieMirrorUrl(cookie, cookie.secure),
                  ...(cookie.domain.startsWith(".") ? { domain: cookie.domain } : {}),
                  path: cookie.path,
                  secure: cookie.secure,
                  httpOnly: cookie.httpOnly,
                  ...(cookie.sameSite === "unspecified"
                    ? {}
                    : { sameSite: sameSiteToCdp[cookie.sameSite] }),
                  ...(cookie.expires === undefined
                    ? {}
                    : { expires: expiryMillis(cookie.expires) / 1000 }),
                  ...(cookie.partitionKey === undefined
                    ? {}
                    : { partitionKey: cookie.partitionKey }),
                },
              ],
            });
          }
        }
      } catch {
        throw new Error("Could not apply browser mirror cookies.");
      }
    },
  };
}

const signature = (cookie: MirrorCookie) =>
  JSON.stringify([
    cookie.value,
    cookie.secure,
    cookie.httpOnly,
    cookie.sameSite,
    cookie.expires === undefined ? null : expiryMillis(cookie.expires),
  ]);

export function diffMirrorCookies(
  previous: ReadonlyMap<string, MirrorCookie>,
  current: ReadonlyArray<MirrorCookie>,
  allowSessionRemovals = true,
): ReadonlyArray<CookieMirrorMutation> {
  const next = new Map(current.map((cookie) => [cookieMirrorKey(cookie), cookie]));
  const mutations: CookieMirrorMutation[] = [];
  for (const [key, cookie] of next) {
    const old = previous.get(key);
    if (old === undefined || signature(old) !== signature(cookie))
      mutations.push({ type: "set", cookie });
  }
  for (const [key, cookie] of previous) {
    if (!next.has(key) && (allowSessionRemovals || cookie.expires !== undefined)) {
      const { name, domain, path, partitionKey } = cookie;
      mutations.push({
        type: "remove",
        key: { name, domain, path, ...(partitionKey ? { partitionKey } : {}) },
      });
    }
  }
  return mutations;
}

export function applyMirrorMutations(
  cookies: Map<string, MirrorCookie>,
  mutations: ReadonlyArray<CookieMirrorMutation>,
): void {
  for (const mutation of mutations) {
    if (mutation.type === "set") cookies.set(cookieMirrorKey(mutation.cookie), mutation.cookie);
    else cookies.delete(cookieMirrorKey(mutation.key));
  }
}
