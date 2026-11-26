import { describe, expect, it } from "vite-plus/test";

import { classifyAntiBotPage, siteOf } from "./SiteHold.ts";

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

describe("classifyAntiBotPage without a status", () => {
  it("judges a page whose response was not observed by its content", () => {
    expect(
      classifyAntiBotPage({
        status: undefined,
        headers: {},
        html: `<iframe src="https://geo.captcha-delivery.com/captcha/"></iframe>`,
      }),
    ).toEqual({ vendor: "datadome", kind: "challenge" });
    expect(
      classifyAntiBotPage({ status: undefined, headers: {}, html: "<p>Results</p>" }),
    ).toBeUndefined();
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
