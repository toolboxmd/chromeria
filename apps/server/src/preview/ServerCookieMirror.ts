import { cookieMirrorKey, type CookieMirrorBatch } from "@t3tools/contracts";
import {
  cdpCookieMirrorStore,
  cdpMirrorCookies,
  mirrorCookieSignature,
} from "@t3tools/shared/browserCookieMirror";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import type { BrowserContext, CDPSession } from "playwright-core";
import type { CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

/**
 * Borrows each persistent context's reserved page: it owns only its CDP
 * session, never the page, and re-attaches after the page or session goes.
 */
export class ServerCookieMirror {
  private readonly stores = new WeakMap<
    BrowserContext,
    { readonly cdp: CDPSession; readonly store: CookieMirrorStore }
  >();
  /**
   * Per profile and cookie key: the signature the desktop last sent and the one
   * the profile stored for it. Outlives context restarts, not server restarts.
   */
  private readonly mirrored = new Map<
    string,
    Map<string, { readonly sent: string; readonly stored: string | undefined }>
  >();
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
      // Reconnects and resyncs replay the desktop's cookies. A replay must not
      // clobber a cookie the profile changed itself, such as an agent-tab login.
      const read = async () =>
        new Map(
          cdpMirrorCookies(await attached.cdp.send("Storage.getCookies"), "skip").map((cookie) => [
            cookieMirrorKey(cookie),
            mirrorCookieSignature(cookie),
          ]),
        );
      const current = await read();
      const mutations = batch.mutations.filter((mutation) => {
        const key = cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key);
        const last = mirrored.get(key);
        const changedHere =
          last !== undefined && current.has(key) && current.get(key) !== last.stored;
        return (
          !changedHere ||
          (mutation.type === "set" && mirrorCookieSignature(mutation.cookie) !== last.sent)
        );
      });
      await attached.store.apply(mutations);
      const stored = mutations.length > 0 ? await read() : current;
      for (const mutation of mutations)
        if (mutation.type === "set") {
          const key = cookieMirrorKey(mutation.cookie);
          mirrored.set(key, {
            sent: mirrorCookieSignature(mutation.cookie),
            stored: stored.get(key),
          });
        } else mirrored.delete(cookieMirrorKey(mutation.key));
    } catch (error) {
      this.stores.delete(context);
      await attached.cdp.detach().catch(() => undefined);
      throw error;
    }
    return context;
  }
}
