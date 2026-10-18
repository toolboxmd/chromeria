import { describe, expect, it } from "vite-plus/test";

import { chromeUserAgent } from "./ChromeUserAgent.ts";

describe("chromeUserAgent", () => {
  it.each([
    "Macintosh; Intel Mac OS X 10_15_7",
    "X11; Linux x86_64",
    "Windows NT 10.0; Win64; x64",
  ])("preserves the native platform and Chromium version on %s", (platform) => {
    const prefix = `Mozilla/5.0 (${platform}) AppleWebKit/537.36 (KHTML, like Gecko)`;
    const chrome = `${prefix} Chrome/152.0.7935.16 Safari/537.36`;
    for (const input of [
      `${prefix} Chromeria/0.1.0 Chrome/152.0.7935.16 Electron/44.4.5 Safari/537.36`,
      `${prefix} T3Code(Alpha)/0.0.33 Chrome/152.0.7935.16 Electron/44.4.5 Safari/537.36`,
      `${prefix} Chrome/152.0.7935.16 Safari/537.36 Electron/44.4.5 custom-app/1.0`,
      chrome,
    ]) {
      expect(chromeUserAgent(input)).toBe(chrome);
    }
  });

  it("rejects input without a Chromium identity instead of inventing a version", () => {
    expect(() => chromeUserAgent("Mozilla/5.0 Electron/44.4.5 Chromeria/0.1.0")).toThrow(
      "Preview user agent is missing the desktop Chromium identity.",
    );
  });
});
