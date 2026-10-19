import { describe, expect, it } from "vite-plus/test";

import { chromeIdentity, chromeRequestHeaders, usesChromeIdentity } from "./ChromeUserAgent.ts";

const host = { arch: "arm64", platformVersion: "15.7.1" };
const native =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chromeria/0.1.0 Chrome/152.0.7977.130 Electron/44.4.5 Safari/537.36";

describe("Chrome identity host list", () => {
  it.each([
    "https://web.whatsapp.com/",
    "https://WEB.WHATSAPP.COM:443/login",
    "wss://dyn.web.whatsapp.com/ws",
    "https://static.whatsapp.net/rsrc.php/script.js",
    "https://mmg.whatsapp.net/media",
  ])("admits WhatsApp and its asset/media hosts: %s", (url) => {
    expect(usesChromeIdentity(url)).toBe(true);
  });

  it.each([
    "https://dash.cloudflare.com/login",
    "https://instagram.com/",
    "https://facebook.com/",
    "https://whatsapp.com/",
    "https://evilwhatsapp.net/",
    "https://web.whatsapp.com.evil.test/",
    "https://web.whatsapp.com@evil.test/",
    "http://web.whatsapp.com/",
    "file:///web.whatsapp.com",
    "not a URL",
  ])("keeps native identity outside the secure host list: %s", (url) => {
    expect(usesChromeIdentity(url)).toBe(false);
  });
});

describe("chromeIdentity", () => {
  it.each([
    ["Macintosh; Intel Mac OS X 10_15_7", "macOS"],
    ["X11; Linux x86_64", "Linux"],
    ["Windows NT 10.0; Win64; x64", "Windows"],
  ])("pairs the reduced UA with real engine metadata on %s", (platformToken, platform) => {
    const prefix = `Mozilla/5.0 (${platformToken}) AppleWebKit/537.36 (KHTML, like Gecko)`;
    for (const products of ["Chromeria/0.1.0", "T3Code(Alpha)/0.0.33", "custom-app/1.0"]) {
      const identity = chromeIdentity(
        `${prefix} ${products} Chrome/152.0.7977.130 Electron/44.4.5 Safari/537.36`,
        host,
      );
      expect(identity.userAgent).toBe(`${prefix} Chrome/152.0.0.0 Safari/537.36`);
      expect(identity.userAgentMetadata).toEqual({
        brands: [
          { brand: "Not A(Brand", version: "99" },
          { brand: "Chromium", version: "152" },
          { brand: "Google Chrome", version: "152" },
        ],
        fullVersionList: [
          { brand: "Not A(Brand", version: "99.0.0.0" },
          { brand: "Chromium", version: "152.0.7977.130" },
          { brand: "Google Chrome", version: "152.0.7977.130" },
        ],
        fullVersion: "152.0.7977.130",
        platform,
        platformVersion: "15.7.1",
        architecture: "arm",
        bitness: "64",
        mobile: false,
        model: "",
        wow64: false,
      });
    }
  });

  it("uses the native x86 architecture without changing the platform token", () => {
    const identity = chromeIdentity(native, { arch: "x64", platformVersion: "15.7.1" });
    expect(identity.userAgentMetadata.architecture).toBe("x86");
    expect(identity.userAgentMetadata.bitness).toBe("64");
  });

  it("rejects input without a Chromium identity instead of inventing a version", () => {
    expect(() => chromeIdentity("Mozilla/5.0 Electron/44.4.5 Chromeria/0.1.0", host)).toThrow(
      "Preview user agent is missing the desktop Chromium identity.",
    );
  });

  it("uses the same brands and versions for headers and JS metadata", () => {
    const headers = chromeRequestHeaders(
      {
        "User-Agent": native,
        "Sec-CH-UA": '"Electron";v="44"',
        "Sec-CH-UA-Full-Version-List": '"Electron";v="44.4.5"',
        "Sec-CH-UA-Arch": '"x86"',
        "Accept-Language": "pl-PL,pl;q=0.9",
        Cookie: "session=private",
      },
      chromeIdentity(native, host),
    );
    expect(headers["User-Agent"]).toContain("Chrome/152.0.0.0 Safari/537.36");
    expect(headers["User-Agent"]).not.toContain("Electron");
    expect(headers["Sec-CH-UA"]).toBe(
      '"Not A(Brand";v="99", "Chromium";v="152", "Google Chrome";v="152"',
    );
    expect(headers["Sec-CH-UA-Full-Version-List"]).toBe(
      '"Not A(Brand";v="99.0.0.0", "Chromium";v="152.0.7977.130", "Google Chrome";v="152.0.7977.130"',
    );
    expect(headers["Sec-CH-UA-Arch"]).toBe('"arm"');
    expect(headers["sec-ch-ua-platform"]).toBe('"macOS"');
    expect(headers["Accept-Language"]).toBe("pl-PL,pl;q=0.9");
    expect(headers.Cookie).toBe("session=private");
  });

  it("does not send high entropy hints without the site's opt-in", () => {
    const headers = chromeRequestHeaders({}, chromeIdentity(native, host));
    expect(headers["sec-ch-ua-full-version-list"]).toBeUndefined();
    expect(headers["sec-ch-ua-arch"]).toBeUndefined();
  });
});
