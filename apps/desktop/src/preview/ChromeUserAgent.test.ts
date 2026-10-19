import { describe, expect, it } from "vite-plus/test";

import { chromeIdentity, usesChromeIdentity } from "./ChromeUserAgent.ts";

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
          { brand: "Chromium", version: "152" },
          { brand: "Not?A_Brand", version: "24" },
          { brand: "Google Chrome", version: "152" },
        ],
        fullVersionList: [
          { brand: "Chromium", version: "152.0.7977.130" },
          { brand: "Not?A_Brand", version: "24.0.0.0" },
          { brand: "Google Chrome", version: "152.0.7977.130" },
        ],
        fullVersion: "152.0.7977.130",
        platform,
        platformVersion: platform === "Linux" ? "" : "15.7.1",
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

  it("does not report a Linux kernel version as platformVersion", () => {
    const linux = native.replace("Macintosh; Intel Mac OS X 10_15_7", "X11; Linux x86_64");
    expect(
      chromeIdentity(linux, { arch: "x64", platformVersion: "6.14.0-generic" }).userAgentMetadata
        .platformVersion,
    ).toBe("");
  });

  it.each([
    [150, ["Not;A=Brand", "Chromium", "Google Chrome"], "8"],
    [151, ["Not=A?Brand", "Google Chrome", "Chromium"], "99"],
    [152, ["Chromium", "Not?A_Brand", "Google Chrome"], "24"],
    [153, ["Google Chrome", "Not_A Brand", "Chromium"], "8"],
    [154, ["Chromium", "Google Chrome", "Not A(Brand"], "99"],
    [155, ["Google Chrome", "Chromium", "Not(A:Brand"], "24"],
  ] as const)(
    "uses Chromium's GREASE and permutation for major %i",
    (major, brands, greaseVersion) => {
      const agent = native.replace("152.0.7977.130", `${major}.0.1.2`);
      const metadata = chromeIdentity(agent, host).userAgentMetadata;
      expect(metadata.brands.map(({ brand }) => brand)).toEqual(brands);
      expect(metadata.brands.find(({ brand }) => brand.startsWith("Not"))?.version).toBe(
        greaseVersion,
      );
      expect(metadata.fullVersionList.find(({ brand }) => brand.startsWith("Not"))?.version).toBe(
        `${greaseVersion}.0.0.0`,
      );
    },
  );
});
