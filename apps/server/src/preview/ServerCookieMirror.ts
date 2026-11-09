import { cookieMirrorKey, type CookieMirrorBatch } from "@t3tools/contracts";
import { cdpCookieMirrorStore } from "@t3tools/shared/browserCookieMirror";
import * as Schema from "effect/Schema";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import type { BrowserContext, CDPSession } from "playwright-core";
import type { CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

const decodeCurrent = Schema.decodeUnknownSync(
  Schema.Struct({
    cookies: Schema.Array(
      Schema.Struct({
        name: Schema.String,
        value: Schema.String,
        domain: Schema.String,
        path: Schema.String,
        partitionKey: Schema.optionalKey(
          Schema.Struct({ topLevelSite: Schema.String, hasCrossSiteAncestor: Schema.Boolean }),
        ),
        partitionKeyOpaque: Schema.optionalKey(Schema.Boolean),
      }),
    ),
  }),
);

/**
 * Borrows each persistent context's reserved page: it owns only its CDP
 * session, never the page, and re-attaches after the page or session goes.
 */
export class ServerCookieMirror {
  private readonly stores = new WeakMap<
    BrowserContext,
    { readonly cdp: CDPSession; readonly store: CookieMirrorStore }
  >();
  /** Last value mirrored per profile and cookie key; outlives idle context restarts. */
  private readonly mirrored = new Map<string, Map<string, string>>();
  private readonly contexts: ServerBrowserContexts;
  constructor(contexts: ServerBrowserContexts) {
    this.contexts = contexts;
  }

  async apply(batch: CookieMirrorBatch) {
    const context = await this.contexts.contextFor(batch.profile.id);
    let attached = this.stores.get(context);
    if (!attached) {
      const page = await this.contexts.reservedPage(context);
      const cdp = await context.newCDPSession(page);
      const current = { cdp, store: cdpCookieMirrorStore(cdp) };
      cdp.on("close", () => {
        if (this.stores.get(context) === current) this.stores.delete(context);
      });
      this.stores.set(context, current);
      attached = current;
    }
    let mirrored = this.mirrored.get(batch.profile.id);
    if (!mirrored) {
      mirrored = new Map();
      this.mirrored.set(batch.profile.id, mirrored);
    }
    try {
      // Reconnects and resyncs replay the desktop's values. A replay must not
      // clobber a value the profile changed itself, such as an agent-tab login.
      const current = new Map(
        decodeCurrent(await attached.cdp.send("Storage.getCookies"))
          .cookies.filter((cookie) => !cookie.partitionKeyOpaque)
          .map((cookie) => [cookieMirrorKey(cookie), cookie.value]),
      );
      const mutations = batch.mutations.filter((mutation) => {
        const key = cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key);
        const last = mirrored.get(key);
        const changedHere = last !== undefined && current.has(key) && current.get(key) !== last;
        return !changedHere || (mutation.type === "set" && mutation.cookie.value !== last);
      });
      await attached.store.apply(mutations);
      for (const mutation of mutations)
        if (mutation.type === "set")
          mirrored.set(cookieMirrorKey(mutation.cookie), mutation.cookie.value);
        else mirrored.delete(cookieMirrorKey(mutation.key));
    } catch (error) {
      this.stores.delete(context);
      await attached.cdp.detach().catch(() => undefined);
      throw error;
    }
    return context;
  }
}
