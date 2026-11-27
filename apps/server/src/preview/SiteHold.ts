import type { PreviewSiteHold, PreviewSiteHoldVendor } from "@t3tools/contracts";

import { registrableDomain } from "./CredentialFill.ts";

export interface AntiBotVerdict {
  readonly vendor: PreviewSiteHoldVendor;
  readonly kind: PreviewSiteHold["kind"];
}

export interface MainDocument {
  readonly status: number;
  /** Lower-case names, as Playwright reports them. */
  readonly headers: Readonly<Record<string, string>>;
  readonly html: string;
}

/**
 * Whether a main-frame document is a vendor's challenge or block page. Vendors serve
 * those as errors, so a 2xx page with an embedded widget is never one; only Cloudflare
 * labels its challenge in a header. Ordinary 403s carry no vendor marker.
 */
export const classifyAntiBotPage = (page: MainDocument): AntiBotVerdict | undefined => {
  const { status, headers, html } = page;
  if (headers["cf-mitigated"] === "challenge") return { vendor: "cloudflare", kind: "challenge" };
  if (status < 400) return undefined;
  if (/captcha-delivery\.com/.test(html))
    // DataDome's page config marks a ban as `'t':'bv'`; anything else can be passed.
    return {
      vendor: "datadome",
      kind: /['"]t['"]\s*:\s*['"]bv['"]/.test(html) ? "blocked" : "challenge",
    };
  // `_pxAppId` is on every HUMAN-protected page, so only the captcha itself counts.
  if (/px-captcha|captcha\.px-cdn\.net/.test(html)) return { vendor: "human", kind: "challenge" };
  if (/_Incapsula_Resource|Incapsula incident ID/.test(html))
    return {
      vendor: "imperva",
      kind: /Request unsuccessful\. Incapsula incident ID/.test(html) ? "blocked" : "challenge",
    };
  if (/sec-if-cpt-container|sec-cpt-if|_sec\/cp_challenge/.test(html))
    return { vendor: "akamai", kind: "challenge" };
  if (
    /^AkamaiGHost/.test(headers.server ?? "") &&
    /Access Denied/.test(html) &&
    /errors\.edgesuite\.net/.test(html)
  )
    return { vendor: "akamai", kind: "blocked" };
  if (headers.server === "cloudflare") {
    if (/cf-chl-|challenge-platform|_cf_chl_opt/.test(html))
      return { vendor: "cloudflare", kind: "challenge" };
    // Cloudflare's 5xx pages share `cf-error-details`; only the firewall block says this.
    if (status === 403 && /cf-error-details/.test(html) && /you have been blocked/i.test(html))
      return { vendor: "cloudflare", kind: "blocked" };
  }
  return undefined;
};

/** Holds bind to the registrable domain, so a check on www.example.com covers shop.example.com. */
export const siteOf = (url: string): string | undefined => {
  let host: string;
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return undefined;
    host = parsed.hostname.toLowerCase();
  } catch {
    return undefined;
  }
  return registrableDomain(host) ?? host;
};

/** A hold names the tab and thread that met the check, so clearing it can tell them. */
export interface HeldSite extends PreviewSiteHold {
  readonly threadId: string;
  readonly tabId: string;
  /** When any covered tab last met a check on the site; a release needs a newer document. */
  metAt: number;
}

/**
 * Site holds per browser scope (one cookie jar): one per site, kept in memory. A
 * restart forgets them, and the next challenge page creates the hold again.
 */
export class SiteHolds {
  private readonly holds = new Map<string, Map<string, HeldSite>>();
  // Events are ordered by one counter, not the clock, so two in one millisecond keep their order.
  private sequence = 0;
  // When a person last ended a site's hold, by browser scope and by thread: a response that
  // arrived before that is part of what the person dismissed.
  private readonly cleared = new Map<string, number>();
  // Responses that may be checks and are still being classified, whatever holds exist yet.
  private readonly classifying = new Set<{
    readonly scope: string;
    readonly threadId: string;
    readonly site: string;
  }>();

  /** A response that may be a check holds back every release on its site until it is classified. */
  receive(scope: string, threadId: string, site: string) {
    const entry = { scope, threadId, site };
    this.classifying.add(entry);
    return {
      at: ++this.sequence,
      done: () => {
        this.classifying.delete(entry);
      },
    };
  }

  /** Whether a response received at `at` came before a person ended this site's hold. */
  dismissed(scope: string, threadId: string, site: string, at: number) {
    return (
      at <
      Math.max(
        this.cleared.get(`${scope}|${site}`) ?? 0,
        this.cleared.get(`thread:${threadId}|${site}`) ?? 0,
      )
    );
  }

  /** Whether a response that could hold this site again is still being classified. */
  classifyingUnder(scope: string, hold: HeldSite) {
    return [...this.classifying].some(
      (each) =>
        each.site === hold.site && (each.scope === scope || each.threadId === hold.threadId),
    );
  }

  get(scope: string, site: string | undefined): HeldSite | undefined {
    return site === undefined ? undefined : this.holds.get(scope)?.get(site);
  }

  /** Any hold this thread's tabs met on the site, whatever the cookie jar. */
  forThread(threadId: string, site: string | undefined): HeldSite | undefined {
    if (site === undefined) return undefined;
    for (const sites of this.holds.values()) {
      const hold = sites.get(site);
      if (hold?.threadId === threadId) return hold;
    }
    return undefined;
  }

  /** Every hold on the site that a tab in this scope and thread falls under. */
  covering(scope: string, threadId: string, site: string): ReadonlyArray<HeldSite> {
    return [...this.holds].flatMap(([each, sites]) => {
      const hold = sites.get(site);
      return hold && (each === scope || hold.threadId === threadId) ? [hold] : [];
    });
  }

  heldBy(tabId: string): ReadonlyArray<readonly [scope: string, hold: HeldSite]> {
    return [...this.holds].flatMap(([scope, sites]) =>
      [...sites.values()]
        .filter((hold) => hold.tabId === tabId)
        .map((hold) => [scope, hold] as const),
    );
  }

  /** The first detection wins, so two tabs on one site make one hold. */
  create(scope: string, hold: HeldSite): boolean {
    let sites = this.holds.get(scope);
    if (!sites) {
      sites = new Map();
      this.holds.set(scope, sites);
    }
    if (sites.has(hold.site)) return false;
    sites.set(hold.site, hold);
    return true;
  }

  /** Ends a hold; a person's clear also dismisses the responses received before it. */
  clear(scope: string, hold: HeldSite, byPerson = false): boolean {
    const sites = this.holds.get(scope);
    if (sites?.get(hold.site) !== hold) return false;
    sites.delete(hold.site);
    if (sites.size === 0) this.holds.delete(scope);
    if (byPerson) {
      const at = ++this.sequence;
      this.cleared.set(`${scope}|${hold.site}`, at);
      this.cleared.set(`thread:${hold.threadId}|${hold.site}`, at);
    }
    return true;
  }
}
