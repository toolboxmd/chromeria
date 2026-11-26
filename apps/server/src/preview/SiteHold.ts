import type { PreviewSiteHold, PreviewSiteHoldVendor } from "@t3tools/contracts";

import { registrableDomain } from "./CredentialFill.ts";

export interface AntiBotVerdict {
  readonly vendor: PreviewSiteHoldVendor;
  readonly kind: PreviewSiteHold["kind"];
}

export interface MainDocument {
  /** Undefined when the response was not observed, such as after a reconnect: judge by content. */
  readonly status: number | undefined;
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
  if (status !== undefined && status < 400) return undefined;
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
  if (headers.server === "cloudflare" || status === undefined) {
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
}

/**
 * Site holds per browser scope (one cookie jar): one per site, kept in memory. A
 * restart forgets them, and the next challenge page creates the hold again.
 */
export class SiteHolds {
  private readonly holds = new Map<string, Map<string, HeldSite>>();

  get(scope: string, site: string | undefined): HeldSite | undefined {
    return site === undefined ? undefined : this.holds.get(scope)?.get(site);
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

  clear(scope: string, hold: HeldSite): boolean {
    const sites = this.holds.get(scope);
    if (sites?.get(hold.site) !== hold) return false;
    sites.delete(hold.site);
    if (sites.size === 0) this.holds.delete(scope);
    return true;
  }
}
