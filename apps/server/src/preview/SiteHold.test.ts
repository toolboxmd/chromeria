import { describe, expect, it } from "vite-plus/test";

import { classifyAntiBotPage, SiteHolds, siteOf } from "./SiteHold.ts";

const page = (status: number, html: string, headers: Record<string, string> = {}) =>
  classifyAntiBotPage({ status, headers, html });

describe("classifyAntiBotPage", () => {
  it.each([
    [
      "DataDome captcha",
      page(
        403,
        `<script>var dd={'rt':'c','cid':'x','t':'fe'}</script>
        <iframe src="https://geo.captcha-delivery.com/captcha/?initialCid=x"></iframe>`,
      ),
      { vendor: "datadome", kind: "challenge" },
    ],
    [
      "DataDome ban",
      page(
        403,
        `<script>var dd={'rt':'c','cid':'x','t':'bv'}</script>
        <script src="https://ct.captcha-delivery.com/c.js"></script>`,
      ),
      { vendor: "datadome", kind: "blocked" },
    ],
    [
      "Cloudflare managed challenge header",
      page(403, "<title>Just a moment...</title>", { "cf-mitigated": "challenge" }),
      { vendor: "cloudflare", kind: "challenge" },
    ],
    [
      "Cloudflare challenge without the header",
      page(503, `<script>window._cf_chl_opt={}</script>`, { server: "cloudflare" }),
      { vendor: "cloudflare", kind: "challenge" },
    ],
    [
      "Cloudflare block",
      page(403, `<div id="cf-error-details">Sorry, you have been blocked</div>`, {
        server: "cloudflare",
      }),
      { vendor: "cloudflare", kind: "blocked" },
    ],
    [
      "HUMAN press and hold",
      page(403, `<div id="px-captcha"></div><script>window._pxAppId='PX1'</script>`),
      { vendor: "human", kind: "challenge" },
    ],
    [
      "Akamai challenge",
      page(428, `<div id="sec-if-cpt-container"></div>`),
      { vendor: "akamai", kind: "challenge" },
    ],
    [
      "Akamai block",
      page(
        403,
        `<H1>Access Denied</H1> Reference #18.1 <a href="https://errors.edgesuite.net/18.1">`,
        { server: "AkamaiGHost" },
      ),
      { vendor: "akamai", kind: "blocked" },
    ],
    [
      "Imperva challenge",
      page(403, `<script src="/_Incapsula_Resource?SWJIYLWA=1"></script>`),
      { vendor: "imperva", kind: "challenge" },
    ],
    [
      "Imperva block",
      page(403, `Request unsuccessful. Incapsula incident ID: 1-2`),
      { vendor: "imperva", kind: "blocked" },
    ],
  ])("recognizes a %s", (_name, verdict, expected) => {
    expect(verdict).toEqual(expected);
  });

  it.each([
    [
      "a 200 form with an embedded Turnstile, DataDome or HUMAN widget",
      page(
        200,
        `<script src="https://challenges.cloudflare.com/turnstile/v0/api.js"></script>
         <iframe src="https://geo.captcha-delivery.com/captcha/"></iframe>
         <div id="px-captcha"></div>`,
        { server: "cloudflare" },
      ),
    ],
    ["an ordinary 403", page(403, "<title>Access denied</title><p>Forbidden</p>")],
    [
      "a HUMAN-protected 404 that carries the sitewide snippet",
      page(404, "<title>Not found</title><script>window._pxAppId='PX1'</script>"),
    ],
    [
      "a Cloudflare 502 error page",
      page(502, `<div id="cf-error-details">Bad gateway</div>`, { server: "cloudflare" }),
    ],
    ["an ordinary Cloudflare 404", page(404, "<title>Not found</title>", { server: "cloudflare" })],
    [
      "a DataDome-protected 404",
      page(404, "<title>Not found</title>", { "x-datadome": "protected" }),
    ],
  ])("ignores %s", (_name, verdict) => {
    expect(verdict).toBeUndefined();
  });
});

describe("siteOf", () => {
  it("holds a registrable domain, so subdomains share one hold", () => {
    expect(siteOf("https://www.allegro.pl/listing?string=x")).toBe("allegro.pl");
    expect(siteOf("https://shop.example.co.uk/")).toBe("example.co.uk");
    expect(siteOf("http://127.0.0.1:3000/")).toBe("127.0.0.1");
    expect(siteOf("about:blank")).toBeUndefined();
  });
});

describe("SiteHolds.covering", () => {
  it("finds every hold a tab falls under, by scope or by thread", () => {
    const holds = new SiteHolds();
    const hold = (threadId: string, tabId: string) => ({
      site: "shop.test",
      vendor: "datadome" as const,
      kind: "challenge" as const,
      url: "https://shop.test/",
      since: "2026-11-27T00:00:00.000Z",
      threadId,
      tabId,
      metAt: 0,
    });
    const profile = hold("thread-a", "tab-1");
    const isolated = hold("thread-a", "tab-2");
    const other = hold("thread-b", "tab-3");
    holds.create("profile:default", profile);
    holds.create("isolated:thread-a:tab-2", isolated);
    holds.create("profile:work", other);
    expect(holds.covering("profile:default", "thread-a", "shop.test")).toEqual([profile, isolated]);
    expect(holds.covering("profile:work", "thread-c", "shop.test")).toEqual([other]);
    expect(holds.covering("profile:default", "thread-a", "other.test")).toEqual([]);
  });
});

describe("SiteHolds ordering", () => {
  const hold = (threadId: string, tabId: string) => ({
    site: "shop.test",
    vendor: "datadome" as const,
    kind: "challenge" as const,
    url: "https://shop.test/",
    since: "2026-11-27T00:00:00.000Z",
    threadId,
    tabId,
    metAt: 0,
  });

  it("holds back a release while a response from before the hold is still classified", () => {
    const holds = new SiteHolds();
    // B's headers arrive first; A's check is classified first and raises the hold.
    const earlier = holds.receive("profile:default", "thread-a", "shop.test", "tab-x");
    const held = hold("thread-a", "tab-a");
    holds.create("profile:default", held);
    expect(holds.classifyingUnder("profile:default", held)).toBe(true);
    earlier.done();
    expect(holds.classifyingUnder("profile:default", held)).toBe(false);
    // The thread's isolated tab counts against the profile hold too.
    const isolated = holds.receive("isolated:thread-a:tab-b", "thread-a", "shop.test", "tab-x");
    expect(holds.classifyingUnder("profile:default", held)).toBe(true);
    isolated.done();
    expect(holds.classifyingUnder("profile:default", held)).toBe(false);
  });

  it("dismisses only responses received before a person's clear, in order not by clock", () => {
    const holds = new SiteHolds();
    const held = hold("thread-a", "tab-a");
    holds.create("profile:default", held);
    const before = holds.receive("profile:default", "thread-a", "shop.test", "tab-x");
    const isolatedBefore = holds.receive(
      "isolated:thread-a:tab-b",
      "thread-a",
      "shop.test",
      "tab-x",
    );
    holds.clear("profile:default", held, true);
    const after = holds.receive("profile:default", "thread-a", "shop.test", "tab-x");
    expect(holds.dismissed("profile:default", "thread-a", "shop.test", before.at)).toBe(true);
    expect(
      holds.dismissed("isolated:thread-a:tab-b", "thread-a", "shop.test", isolatedBefore.at),
      "the thread's other tab dismissed it too",
    ).toBe(true);
    expect(holds.dismissed("profile:default", "thread-a", "shop.test", after.at)).toBe(false);
    // Clearing the thread's incognito hold leaves a check its profile is still classifying.
    const incognito = hold("thread-a", "tab-b");
    holds.create("isolated:thread-a:tab-b", incognito);
    const profilePending = holds.receive("profile:default", "thread-a", "shop.test", "tab-x");
    holds.clear("isolated:thread-a:tab-b", incognito, true);
    expect(holds.dismissed("profile:default", "thread-a", "shop.test", profilePending.at)).toBe(
      false,
    );
    // A verified release is no person's dismissal.
    const again = hold("thread-a", "tab-a");
    holds.create("profile:default", again);
    const pending = holds.receive("profile:default", "thread-a", "shop.test", "tab-x");
    holds.clear("profile:default", again);
    expect(holds.dismissed("profile:default", "thread-a", "shop.test", pending.at)).toBe(false);
  });

  it("lets a person's clear release the responses it dismissed, so they stop fencing the tab", async () => {
    const holds = new SiteHolds();
    const held = hold("thread-a", "tab-a");
    holds.create("profile:default", held);
    holds.receive("profile:default", "thread-a", "shop.test", "tab-a");
    expect(holds.classifyingOn("tab-a")).toBe(true);
    const changed = holds.changed();
    holds.clear("profile:default", held, true);
    await changed;
    expect(holds.classifyingOn("tab-a")).toBe(false);
    // A response after the clear is classified as usual.
    const later = holds.receive("profile:default", "thread-a", "shop.test", "tab-a");
    expect(holds.classifyingOn("tab-a")).toBe(true);
    later.done();
    expect(holds.classifyingOn("tab-a")).toBe(false);
  });
});

describe("SiteHolds fence", () => {
  it("stops a response that never finishes from holding the tab back after its fence", () => {
    let now = 0;
    const holds = new SiteHolds(10_000, () => now);
    holds.receive("profile:default", "thread-a", "shop.test", "tab-a");
    expect(holds.classifyingOn("tab-a")).toBe(true);
    now = 10_001;
    expect(holds.classifyingOn("tab-a"), "a stalled body lapses").toBe(false);
  });
});
