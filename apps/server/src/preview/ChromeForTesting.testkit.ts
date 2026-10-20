// @effect-diagnostics nodeBuiltinImport:off - Resolved once at test import, outside any Effect.
import * as NodePath from "node:path";
import type { Page } from "playwright-core";
import { expect } from "vite-plus/test";

const CHROME_FOR_TESTING = new Set(["chrome", "chrome.exe", "Google Chrome for Testing"]);

/**
 * The pinned Chrome for Testing that real-browser suites launch, from
 * `CHROMERIA_TEST_CHROMIUM`. Without it suites skip locally and fail under CI,
 * and a headless shell or other Chromium is refused, so a green run proves the
 * browser T3 ships.
 */
export const testChromium = ((): string | undefined => {
  const executable = process.env.CHROMERIA_TEST_CHROMIUM;
  if (!executable) {
    if (process.env.CI) {
      throw new Error("CI must set CHROMERIA_TEST_CHROMIUM to the pinned Chrome for Testing.");
    }
    return undefined;
  }
  if (!CHROME_FOR_TESTING.has(NodePath.basename(executable))) {
    throw new Error(`CHROMERIA_TEST_CHROMIUM is not Chrome for Testing: ${executable}`);
  }
  return executable;
})();

export const SKIP_WITHOUT_CHROMIUM =
  "Set CHROMERIA_TEST_CHROMIUM to the pinned Chrome for Testing executable.";

/** chrome-headless-shell has no plugins; full Chrome lists its PDF viewers even headless. */
export const expectFullChrome = async (page: Page) => {
  expect(await page.evaluate<number>("navigator.plugins.length")).toBeGreaterThan(0);
};
