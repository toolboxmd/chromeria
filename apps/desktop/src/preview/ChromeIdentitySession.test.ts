import type {
  CallbackResponse,
  OnBeforeRequestListenerDetails,
  WebContents,
  WebRequest,
} from "electron";
import { describe, expect, it, vi } from "vite-plus/test";

import { installChromeIdentity } from "./ChromeIdentitySession.ts";
import { chromeIdentity, type ChromeIdentity } from "./ChromeUserAgent.ts";

const native =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chromeria/0.1.0 Chrome/152.0.7977.130 Electron/44.4.5 Safari/537.36";
const host = { arch: "x64", platformVersion: "" };
const listed = "https://web.whatsapp.com/";

const harness = (
  configure = vi.fn<(contents: WebContents, identity: ChromeIdentity | null) => Promise<void>>(
    async () => {},
  ),
) => {
  const onBeforeRequest = vi.fn<WebRequest["onBeforeRequest"]>();
  const onBeforeSendHeaders = vi.fn<WebRequest["onBeforeSendHeaders"]>();
  const listeners = new Map<string, (...args: unknown[]) => unknown>();
  const contents = {
    on: (event: string, listener: (...args: unknown[]) => unknown) =>
      listeners.set(event, listener),
    off: (event: string) => listeners.delete(event),
    isDestroyed: () => false,
  } as unknown as WebContents;
  const webRequest = { onBeforeRequest, onBeforeSendHeaders };
  installChromeIdentity({ getUserAgent: () => native, webRequest }, host, configure);
  const beforeRequest = onBeforeRequest.mock.calls[0]?.[0];
  if (typeof beforeRequest !== "function") throw new Error("Missing request observation");
  const request = (
    url = listed,
    resourceType: OnBeforeRequestListenerDetails["resourceType"] = "mainFrame",
  ) => {
    const callback = vi.fn<(response: CallbackResponse) => void>();
    beforeRequest(
      {
        id: 1,
        url,
        method: "GET",
        webContents: contents,
        resourceType,
        referrer: "",
        timestamp: 0,
        uploadData: [],
      },
      callback,
    );
    return callback;
  };
  const emit = (event: string, ...args: unknown[]) => listeners.get(event)?.(...args);
  const commit = (url = listed, isMainFrame = true) =>
    emit("did-frame-navigate", {}, url, 200, "OK", isMainFrame, 1, 1);
  return { configure, contents, request, commit, emit, onBeforeSendHeaders, listeners };
};

describe("preview main-frame identity lifecycle", () => {
  it("never registers a header rewriter or switches identity for the provisional request", () => {
    const test = harness();
    const resumed = test.request();
    expect(resumed).toHaveBeenCalledWith({});
    expect(test.configure).not.toHaveBeenCalled();
    expect(test.onBeforeSendHeaders).not.toHaveBeenCalled();
  });

  it("sets the paired identity only after a listed main frame commits", async () => {
    const test = harness();
    test.request();
    await test.commit();
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(
      test.contents,
      chromeIdentity(native, host),
    );
  });

  it("clears both UA and metadata on a non-listed commit, then sets them on return", async () => {
    const test = harness();
    test.request();
    await test.commit();
    test.request("https://dash.cloudflare.com/login");
    await test.commit("https://dash.cloudflare.com/login");
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
    await test.commit();
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, chromeIdentity(native, host));
  });

  it("explicitly clears non-listed main-frame commits to native identity", async () => {
    const test = harness();
    test.request("https://dash.cloudflare.com/login");
    await test.commit("https://dash.cloudflare.com/login");
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(test.contents, null);
  });

  it("does not switch listed iframes or workers under a native top-level page", async () => {
    const test = harness();
    test.request("https://example.com/");
    await test.commit("https://example.com/");
    test.configure.mockClear();
    test.request(listed, "subFrame");
    await test.commit(listed, false);
    test.request("https://static.whatsapp.net/worker.js", "other");
    expect(test.configure).not.toHaveBeenCalled();
    expect(test.onBeforeSendHeaders).not.toHaveBeenCalled();
  });

  it("does not restore third-party headers or switch child frames under a Chrome main frame", async () => {
    const test = harness();
    test.request();
    await test.commit();
    test.request("https://example.com/frame", "subFrame");
    await test.commit("https://example.com/frame", false);
    test.request("https://example.com/challenge.js", "script");
    expect(test.configure).toHaveBeenCalledTimes(1);
    expect(test.onBeforeSendHeaders).not.toHaveBeenCalled();
  });

  it.each(["did-fail-load", "did-fail-provisional-load"])(
    "clears an uncommitted listed navigation on %s, including cancellation",
    async (event) => {
      const test = harness();
      test.request();
      await test.emit(event, {}, -3, "ERR_ABORTED", listed, true, 1, 1);
      expect(test.configure).toHaveBeenCalledExactlyOnceWith(test.contents, null);
    },
  );

  it("clears a listed navigation stopped before commit even without a failure event", async () => {
    const test = harness();
    test.request();
    await test.emit("did-stop-loading");
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(test.contents, null);
  });

  it("does not clear a successful commit or react to a child-frame failure", async () => {
    const test = harness();
    test.request();
    await test.emit("did-fail-load", {}, -3, "ERR_ABORTED", listed, false, 1, 2);
    expect(test.configure).not.toHaveBeenCalled();
    await test.commit();
    await test.emit("did-stop-loading");
    expect(test.configure).toHaveBeenCalledTimes(1);
  });

  it("can clear after a previous CDP command rejects", async () => {
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {});
    configure.mockRejectedValueOnce(new Error("debugger unavailable"));
    const test = harness(configure);
    test.request();
    await expect(test.commit()).rejects.toThrow("debugger unavailable");
    await test.commit("https://example.com/");
    expect(configure).toHaveBeenLastCalledWith(test.contents, null);
  });

  it("removes lifecycle observers when the WebContents is destroyed", () => {
    const test = harness();
    test.request();
    test.emit("destroyed");
    expect(test.listeners.size).toBe(0);
  });

  it("serializes an abort clear after a command that is still in flight", async () => {
    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = () => {};
    const beginning = new Promise<void>((resolve) => {
      started = resolve;
    });
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {
      started();
      await blocked;
    });
    const test = harness(configure);
    test.request();
    const committed = test.commit();
    await beginning;
    test.request();
    const stopped = test.emit("did-stop-loading");
    release();
    await Promise.all([committed, stopped]);
    expect(configure.mock.calls).toEqual([
      [test.contents, chromeIdentity(native, host)],
      [test.contents, null],
    ]);
  });
});
