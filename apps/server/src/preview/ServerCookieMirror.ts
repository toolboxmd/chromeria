import type { CookieMirrorBatch } from "@t3tools/contracts";
import { cdpCookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import type { BrowserContext } from "playwright-core";
import type { CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

export class ServerCookieMirror {
  private readonly stores = new WeakMap<BrowserContext, CookieMirrorStore>();
  private readonly contexts: ServerBrowserContexts;
  constructor(contexts: ServerBrowserContexts) {
    this.contexts = contexts;
  }

  async apply(batch: CookieMirrorBatch) {
    const context = await this.contexts.contextFor(batch.profile.id);
    let store = this.stores.get(context);
    if (!store) {
      const page = await context.newPage();
      try {
        store = cdpCookieMirrorStore(await context.newCDPSession(page));
      } catch (error) {
        await page.close();
        throw error;
      }
      // Closing the final page ends Chromium's session and discards session cookies.
      // The blank target belongs to this context and closes with it.
      this.stores.set(context, store);
    }
    await store.apply(batch.mutations);
    return context;
  }
}
