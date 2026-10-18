import { describe, expect, it } from "vite-plus/test";

import { PREVIEW_WEBVIEW_PREFERENCES } from "./WebviewPreferences.ts";

function parseWebPreferences(input: string): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const pair of input.split(",")) {
    if (pair !== pair.trim()) {
      // Electron's parser doesn't trim; surface the bug as undefined-key.
      out[pair] = pair.split("=")[1];
      continue;
    }
    const [key, value] = pair.split("=");
    if (!key) continue;
    out[key] = value;
  }
  return out;
}

describe("PREVIEW_WEBVIEW_PREFERENCES", () => {
  const parsed = parseWebPreferences(PREVIEW_WEBVIEW_PREFERENCES);

  it("uses canonical JS-boolean string literals for security preferences", () => {
    for (const key of ["contextIsolation", "nodeIntegration", "sandbox", "plugins"]) {
      expect(parsed[key]).toMatch(/^(true|false)$/);
    }
  });

  it("disables the Blink automation marker only in preview guests", () => {
    expect(parsed["disableBlinkFeatures"]).toBe("AutomationControlled");
  });

  it("disables context isolation (so react-grab can see the page's React DevTools hook)", () => {
    expect(parsed["contextIsolation"]).toBe("false");
  });

  it("keeps the renderer sandbox enabled (so the page cannot reach Node APIs)", () => {
    expect(parsed["sandbox"]).toBe("true");
  });

  it("disables nodeIntegration (defense in depth — page never gets Node)", () => {
    expect(parsed["nodeIntegration"]).toBe("false");
  });

  it("enables native plugins for Chrome-compatible PDF and MIME support", () => {
    expect(parsed["plugins"]).toBe("true");
  });

  it("contains no whitespace (Electron's parser does not trim)", () => {
    expect(PREVIEW_WEBVIEW_PREFERENCES).not.toMatch(/\s/);
  });
});
