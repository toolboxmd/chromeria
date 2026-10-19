import type {
  CallbackResponse,
  OnBeforeRequestListenerDetails,
  WebContents,
  WebRequest,
} from "electron";
import { describe, expect, it, vi } from "vite-plus/test";

import { installChromeIdentity } from "./ChromeIdentitySession.ts";
import { chromeIdentity } from "./ChromeUserAgent.ts";

const native =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chromeria/0.1.0 Chrome/152.0.7977.130 Electron/44.4.5 Safari/537.36";
const host = { arch: "x64", platformVersion: "" };

const harness = (configure = vi.fn(async () => {})) => {
  const onBeforeRequest = vi.fn<WebRequest["onBeforeRequest"]>();
  const onBeforeSendHeaders = vi.fn<WebRequest["onBeforeSendHeaders"]>();
  const contents = {} as WebContents;
  installChromeIdentity(
    { getUserAgent: () => native, webRequest: { onBeforeRequest, onBeforeSendHeaders } },
    host,
    configure,
  );
  const beforeRequest = onBeforeRequest.mock.calls[0]?.[0];
  const beforeHeaders = onBeforeSendHeaders.mock.calls[0]?.[0];
  if (typeof beforeRequest !== "function" || typeof beforeHeaders !== "function") {
    throw new Error("Missing request policy");
  }
  const details = (url: string): OnBeforeRequestListenerDetails => ({
    id: 1,
    url,
    method: "GET",
    webContents: contents,
    resourceType: "mainFrame",
    referrer: "",
    timestamp: 0,
    uploadData: [],
  });
  const navigate = (url: string) =>
    new Promise<CallbackResponse>((resolve) => beforeRequest(details(url), resolve));
  const headers = (url: string, requestHeaders: Record<string, string>) => {
    const callback = vi.fn();
    beforeHeaders({ ...details(url), resourceType: "script", requestHeaders }, callback);
    return callback.mock.calls[0]?.[0]?.requestHeaders as Record<string, string>;
  };
  return { configure, contents, navigate, headers };
};

describe("preview site identity policy", () => {
  it("leaves a normal site untouched, including Turnstile, without CDP commands", async () => {
    const test = harness();
    expect(await test.navigate("https://dash.cloudflare.com/login")).toEqual({});
    const original = { "User-Agent": native, "Sec-CH-UA": '"Chromium";v="152"', Cookie: "private" };
    expect(test.headers("https://dash.cloudflare.com/login", original)).toBe(original);
    expect(test.configure).not.toHaveBeenCalled();
  });

  it("holds the first request until the paired UA and metadata update completes", async () => {
    let release = () => {};
    const started = new Promise<void>((resolve) => {
      release = resolve;
    });
    let finish = () => {};
    const configured = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const configure = vi.fn(async () => {
      release();
      await configured;
    });
    const test = harness(configure);
    const resumed = vi.fn();
    const navigation = test.navigate("https://web.whatsapp.com/").then(resumed);
    await started;
    expect(resumed).not.toHaveBeenCalled();
    expect(configure).toHaveBeenCalledWith(test.contents, chromeIdentity(native, host));
    finish();
    await navigation;
    expect(resumed).toHaveBeenCalledWith({ cancel: false });
  });

  it("clears the override on a redirect away and reapplies it on return", async () => {
    const test = harness();
    await test.navigate("https://web.whatsapp.com/");
    await test.navigate("https://dash.cloudflare.com/login");
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
    const nativeHeaders = { "User-Agent": native };
    expect(test.headers("https://dash.cloudflare.com/login", nativeHeaders)).toBe(nativeHeaders);
    await test.navigate("https://web.whatsapp.com/");
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, chromeIdentity(native, host));
  });

  it("serializes navigation away while the Chrome update is still pending", async () => {
    let finish = () => {};
    const configured = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const configure = vi.fn(async () => {
      await configured;
    });
    const test = harness(configure);
    const first = test.navigate("https://web.whatsapp.com/");
    const second = test.navigate("https://example.com/");
    finish();
    await Promise.all([first, second]);
    expect(configure.mock.calls).toEqual([
      [test.contents, chromeIdentity(native, host)],
      [test.contents, null],
    ]);
  });

  it("cancels navigation when CDP setup fails", async () => {
    const test = harness(
      vi.fn(async () => {
        throw new Error("debugger unavailable");
      }),
    );
    expect(await test.navigate("https://web.whatsapp.com/")).toEqual({ cancel: true });
  });

  it("resets a possibly applied override after a failed transport call", async () => {
    const configure = vi.fn(async () => {});
    configure.mockRejectedValueOnce(new Error("transport disconnected"));
    const test = harness(configure);
    const listed = test.navigate("https://web.whatsapp.com/");
    const nativeNavigation = test.navigate("https://example.com/");
    expect(await listed).toEqual({ cancel: true });
    expect(await nativeNavigation).toEqual({ cancel: false });
    expect(configure).toHaveBeenLastCalledWith(test.contents, null);
  });

  it("routes asset requests by destination without changing unrelated headers", async () => {
    const test = harness();
    await test.navigate("https://web.whatsapp.com/");
    const chrome = test.headers("https://static.whatsapp.net/script.js", {
      "User-Agent": native,
      Cookie: "private",
    });
    expect(chrome["User-Agent"]).toContain("Chrome/152.0.0.0 Safari/537.36");
    expect(chrome["sec-ch-ua"]).toContain('"Google Chrome";v="152"');
    const thirdParty = test.headers("https://example.com/script.js", chrome);
    expect(thirdParty["User-Agent"]).toBe(native);
    expect(thirdParty["sec-ch-ua"]).not.toContain("Google Chrome");
    expect(thirdParty.Cookie).toBe("private");
  });
});
