import type { CookieMirrorBatch } from "@t3tools/contracts";
import { cdpCookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

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
    try {
      await attached.store.apply(batch.mutations);
    } catch (error) {
      this.stores.delete(context);
      await attached.cdp.detach().catch(() => undefined);
      throw error;
    }
    return context;
  }
}
