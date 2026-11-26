import {
  cookieMirrorKey,
  type CookieMirrorBatch,
  type CookieMirrorMutation,
  type MirrorCookie,
} from "@t3tools/contracts";
import {
  cdpCookieMirrorStore,
  cdpMirrorCookies,
  isAntiBotVerdictCookie,
  mirrorCookieSignature,
} from "@t3tools/shared/browserCookieMirror";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import type { BrowserContext, CDPSession } from "playwright-core";
import type { CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

/** A stored signature no real cookie has: the profile removed this desktop cookie itself. */
const REMOVED_HERE = "\u0000removed-here";

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
  /** Verdict keys whose mirrored copy was already withdrawn, per profile. Process memory. */
  private readonly withdrawn = new Map<string, Set<string>>();
  private readonly contexts: ServerBrowserContexts;
  /** Mirror batches and login imports touch the same jars, so they run one at a time. */
  private queue: Promise<unknown> = Promise.resolve();
  constructor(contexts: ServerBrowserContexts) {
    this.contexts = contexts;
  }

  private serialized<A>(run: () => Promise<A>): Promise<A> {
    const next = this.queue.then(run, run);
    this.queue = next.catch(() => undefined);
    return next;
  }

  private async attach(context: BrowserContext) {
    const existing = this.stores.get(context);
    if (existing) return existing;
    const page = await this.contexts.reservedPage(context);
    const cdp = await context.newCDPSession(page);
    const current = { cdp, store: cdpCookieMirrorStore(cdp) };
    cdp.on("close", () => {
      if (this.stores.get(context) === current) this.stores.delete(context);
    });
    this.stores.set(context, current);
    return current;
  }

  private async detachAfter(context: BrowserContext, cdp: CDPSession) {
    this.stores.delete(context);
    await cdp.detach().catch(() => undefined);
  }

  private async readAttached(context: BrowserContext, cdp: CDPSession) {
    try {
      return cdpMirrorCookies(await cdp.send("Storage.getCookies"), "skip");
    } catch {
      await this.detachAfter(context, cdp);
      // Protocol and schema errors may embed cookie values.
      throw new Error("Could not read browser profile cookies.");
    }
  }

  /** The profile's cookies, opaque partitions left out. Callers check the profile id first. */
  readProfile(profileId: string): Promise<ReadonlyArray<MirrorCookie>> {
    return this.serialized(async () => {
      const context = await this.contexts.contextFor(profileId);
      const attached = await this.attach(context);
      return this.readAttached(context, attached.cdp);
    });
  }

  /**
   * Writes a login import computed from `plan(current)`. The desktop's last-sent
   * signatures stay as they were, so the replay guard treats the import as a
   * change the profile made itself: a replay keeps it, a new desktop value wins.
   */
  writeProfile<A>(
    profileId: string,
    plan: (current: ReadonlyArray<MirrorCookie>) => {
      readonly mutations: ReadonlyArray<CookieMirrorMutation>;
      readonly result: A;
    },
  ): Promise<A> {
    return this.serialized(async () => {
      const context = await this.contexts.contextFor(profileId);
      const attached = await this.attach(context);
      const { mutations, result } = plan(await this.readAttached(context, attached.cdp));
      try {
        await attached.store.apply(mutations);
      } catch (error) {
        await this.detachAfter(context, attached.cdp);
        throw error;
      }
      // An absent cookie normally lets a replay restore it; one the import deleted must stay gone.
      const mirrored = this.mirrored.get(profileId);
      for (const mutation of mutations) {
        if (mutation.type !== "remove") continue;
        const key = cookieMirrorKey(mutation.key);
        const last = mirrored?.get(key);
        if (last !== undefined) mirrored!.set(key, { sent: last.sent, stored: REMOVED_HERE });
      }
      return result;
    });
  }

  apply(batch: CookieMirrorBatch) {
    return this.serialized(() => this.applyBatch(batch));
  }

  private async applyBatch(batch: CookieMirrorBatch) {
    const context = await this.contexts.contextFor(batch.profile.id);
    const attached = await this.attach(context);
    let withdrawn = this.withdrawn.get(batch.profile.id);
    if (!withdrawn) {
      withdrawn = new Set();
      this.withdrawn.set(batch.profile.id, withdrawn);
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
        // A desktop built before verdicts stopped travelling still sends them.
        if (mutation.type === "set" && isAntiBotVerdictCookie(mutation.cookie.name)) return false;
        // The one-time withdrawal of a mirrored verdict is applied once; a replay after a lost
        // acknowledgement must not delete a verdict the profile earned since.
        if (mutation.type === "remove" && isAntiBotVerdictCookie(mutation.key.name)) {
          const key = cookieMirrorKey(mutation.key);
          if (withdrawn.has(key)) return false;
          withdrawn.add(key);
          return true;
        }
        const key = cookieMirrorKey(mutation.type === "set" ? mutation.cookie : mutation.key);
        const last = mirrored.get(key);
        const changedHere =
          last !== undefined &&
          (current.has(key) ? current.get(key) !== last.stored : last.stored === REMOVED_HERE);
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
      await this.detachAfter(context, attached.cdp);
      throw error;
    }
    return context;
  }
}
