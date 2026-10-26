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
const ordinary = "https://dash.cloudflare.com/login";

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
    getURL: () => ordinary,
  } as unknown as WebContents;
  const webRequest = { onBeforeRequest, onBeforeSendHeaders };
  const restore = installChromeIdentity(
    { getUserAgent: () => native, webRequest },
    host,
    configure,
  );
  const beforeRequest = onBeforeRequest.mock.calls[0]?.[0];
  if (typeof beforeRequest !== "function") throw new Error("Missing request policy");
  const request = (
    url = listed,
    resourceType: OnBeforeRequestListenerDetails["resourceType"] = "mainFrame",
  ) => {
    const resumed = vi.fn();
    const completed = new Promise<CallbackResponse>((resolve) =>
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
        (response) => {
          resumed(response);
          resolve(response);
        },
      ),
    );
    return { resumed, completed };
  };
  const emit = (event: string, ...args: unknown[]) => listeners.get(event)?.(...args);
  const commit = (url = listed, isMainFrame = true) =>
    emit("did-frame-navigate", {}, url, 200, "OK", isMainFrame, 1, 1);
  // Identity commands chain only microtasks when fakes settle immediately, so flushing
  // microtasks runs them to completion without any timer.
  const drain = async () => {
    for (let turn = 0; turn < 32; turn += 1) await Promise.resolve();
  };
  return {
    configure,
    contents,
    request,
    commit,
    emit,
    drain,
    onBeforeSendHeaders,
    listeners,
    restore,
  };
};

describe("preview main-frame identity lifecycle", () => {
  it("resumes main-frame requests while Chromium queues the identity reply behind the navigation", async () => {
    // Chromium answers Emulation.setUserAgentOverride only after the navigation finishes.
    const finishNavigation: Array<() => void> = [];
    const test = harness(
      vi.fn(() => new Promise<void>((resolve) => finishNavigation.push(resolve))),
    );
    const whatsapp = test.request(listed);
    expect(whatsapp.resumed).toHaveBeenCalledWith({});
    await test.drain();
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(
      test.contents,
      chromeIdentity(native, host),
    );
    const committed = test.commit(listed);
    for (const finish of finishNavigation) finish();
    await committed;
    expect(test.configure).toHaveBeenCalledOnce();
    expect(test.onBeforeSendHeaders).not.toHaveBeenCalled();
  });

  it("loads non-listed pages in a fresh tab without touching the debugger", async () => {
    const test = harness(vi.fn(() => new Promise<void>(() => {})));
    const page = test.request("https://example.com/");
    expect(page.resumed).toHaveBeenCalledWith({});
    await test.commit("https://example.com/");
    await test.emit("did-stop-loading");
    expect(test.configure).not.toHaveBeenCalled();
  });

  it("keeps the committed WhatsApp pair after an aborted WhatsApp reload", async () => {
    const test = harness();
    await test.request().completed;
    await test.commit();
    await test.request().completed;
    await test.emit("did-stop-loading");
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(
      test.contents,
      chromeIdentity(native, host),
    );
  });

  it.each(["did-fail-load", "did-fail-provisional-load"])(
    "keeps committed WhatsApp after a second listed navigation fails via %s",
    async (event) => {
      const test = harness();
      await test.request().completed;
      await test.commit();
      await test.request("https://web.whatsapp.com/second").completed;
      await test.emit(event, {}, -3, "ERR_ABORTED", "https://web.whatsapp.com/second", true, 1, 1);
      expect(test.configure).toHaveBeenCalledExactlyOnceWith(
        test.contents,
        chromeIdentity(native, host),
      );
    },
  );

  it.each(["did-fail-load", "did-fail-provisional-load", "did-stop-loading"])(
    "restores a native committed document when the listed load ends via %s",
    async (event) => {
      const test = harness();
      await test.request().completed;
      await test.emit(event, {}, -3, "ERR_ABORTED", listed, true, 1, 1);
      expect(test.configure.mock.calls).toEqual([
        [test.contents, chromeIdentity(native, host)],
        [test.contents, null],
      ]);
    },
  );

  it("clears when leaving WhatsApp and restores it if that load aborts", async () => {
    const test = harness();
    await test.request().completed;
    await test.commit();
    expect(await test.request(ordinary).completed).toEqual({});
    await test.drain();
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
    await test.emit("did-stop-loading");
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, chromeIdentity(native, host));
  });

  it("remembers the newly committed native page and keeps it on a later abort", async () => {
    const test = harness();
    await test.request().completed;
    await test.commit();
    await test.request(ordinary).completed;
    await test.commit(ordinary);
    await test.request().completed;
    await test.emit("did-stop-loading");
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
  });

  it("clears UA and metadata for a listed HTTP redirect to a non-listed host and stays native", async () => {
    let finishClear = () => {};
    const clearing = new Promise<void>((resolve) => {
      finishClear = resolve;
    });
    const test = harness(
      vi.fn(async (_contents: WebContents, identity: ChromeIdentity | null) => {
        if (identity === null) await clearing;
      }),
    );
    expect(await test.request("https://web.whatsapp.com/").completed).toEqual({});
    // The redirect is another main-frame request with the same id, without a listed commit.
    expect(await test.request(ordinary).completed).toEqual({});
    await test.drain();
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
    const committed = test.commit(ordinary);
    finishClear();
    await committed;
    await test.emit("did-stop-loading");
    expect(test.configure.mock.calls).toEqual([
      [test.contents, chromeIdentity(native, host)],
      [test.contents, null],
    ]);
  });

  it("re-applies the Chrome pair on a same-host reload after a renderer crash", async () => {
    const test = harness();
    await test.request(listed).completed;
    await test.commit(listed);
    test.configure.mockClear();
    test.emit("render-process-gone", {}, { reason: "crashed", exitCode: 1 });
    expect(await test.request(listed).completed).toEqual({});
    await test.drain();
    expect(test.configure).toHaveBeenCalledExactlyOnceWith(
      test.contents,
      chromeIdentity(native, host),
    );
    await test.commit(listed);
  });

  it("does not switch iframes, workers or third-party headers", async () => {
    const test = harness();
    await test.request(ordinary).completed;
    await test.commit(ordinary);
    test.configure.mockClear();
    expect(await test.request(listed, "subFrame").completed).toEqual({});
    await test.commit(listed, false);
    await test.request("https://static.whatsapp.net/worker.js", "other").completed;
    expect(test.configure).not.toHaveBeenCalled();
    await test.request().completed;
    await test.commit();
    await test.request("https://example.com/challenge.js", "script").completed;
    await test.drain();
    expect(test.configure).toHaveBeenCalledTimes(1);
    expect(test.onBeforeSendHeaders).not.toHaveBeenCalled();
  });

  it("ignores a stale failure from a different pending URL", async () => {
    const test = harness();
    await test.request().completed;
    await test.request(ordinary).completed;
    await test.emit("did-fail-provisional-load", {}, -3, "ERR_ABORTED", listed, true, 1, 1);
    await test.drain();
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
    expect(test.configure).toHaveBeenCalledTimes(2);
  });

  it("re-applies committed identity on restoration even when it was cached as applied", async () => {
    const test = harness();
    await test.request().completed;
    await test.commit();
    await test.restore(test.contents);
    expect(test.configure).toHaveBeenCalledTimes(2);
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, chromeIdentity(native, host));
  });

  it("remembers a commit whose command failed while DevTools was open", async () => {
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {});
    const test = harness(configure);
    await test.request(ordinary).completed;
    await test.commit(ordinary);
    test.emit("devtools-opened");
    configure.mockRejectedValueOnce(new Error("DevTools open"));
    await expect(test.commit()).rejects.toThrow("DevTools open");
    await test.restore(test.contents);
    expect(configure).toHaveBeenLastCalledWith(test.contents, chromeIdentity(native, host));
  });

  it("still loads a listed page whose identity setup fails", async () => {
    const test = harness(
      vi.fn(async () => {
        throw new Error("debugger unavailable");
      }),
    );
    expect(await test.request().completed).toEqual({});
  });

  // A held command (here a debugger restore) queues the navigations that follow it.
  const holdNext = () => {
    let release = () => {};
    let held: Promise<void> | undefined = new Promise<void>((resolve) => {
      release = resolve;
    });
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {
      const wait = held;
      held = undefined;
      await wait;
    });
    return { configure, release: () => release() };
  };

  it("never applies a superseded WhatsApp identity to the native page that replaced it", async () => {
    const { configure, release } = holdNext();
    const test = harness(configure);
    await test.request(ordinary).completed;
    await test.commit(ordinary);
    const restored = test.restore(test.contents);
    await test.drain();
    expect(await test.request(listed).completed).toEqual({});
    expect(await test.request(ordinary).completed).toEqual({});
    const committed = test.commit(ordinary);
    release();
    await restored;
    await committed;
    expect(configure.mock.calls).toEqual([[test.contents, null]]);
  });

  it("never clears the identity of a WhatsApp page that replaced a native navigation", async () => {
    const test = harness();
    await test.request(listed).completed;
    await test.commit(listed);
    const { configure, release } = holdNext();
    test.configure.mockImplementation(configure);
    test.configure.mockClear();
    const restored = test.restore(test.contents);
    await test.drain();
    expect(await test.request(ordinary).completed).toEqual({});
    expect(await test.request("https://web.whatsapp.com/second").completed).toEqual({});
    const committed = test.commit("https://web.whatsapp.com/second");
    release();
    await restored;
    await committed;
    expect(test.configure.mock.calls).toEqual([[test.contents, chromeIdentity(native, host)]]);
  });

  it("restores the old document after stopping while the provisional command is in flight", async () => {
    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let begin = () => {};
    const beginning = new Promise<void>((resolve) => {
      begin = resolve;
    });
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {
      begin();
      await blocked;
    });
    const test = harness(configure);
    const request = test.request();
    await beginning;
    const stopped = test.emit("did-stop-loading");
    release();
    await stopped;
    expect(await request.completed).toEqual({});
    expect(configure).toHaveBeenLastCalledWith(test.contents, null);
  });

  it("does not let a concurrent control-session restore overwrite the pending destination", async () => {
    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let begin = () => {};
    const beginning = new Promise<void>((resolve) => {
      begin = resolve;
    });
    const configure = vi.fn<
      (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>
    >(async () => {
      begin();
      await blocked;
    });
    const test = harness(configure);
    const request = test.request();
    await beginning;
    const restored = test.restore(test.contents);
    release();
    expect(await request.completed).toEqual({});
    await restored;
    expect(
      configure.mock.calls.every(([, identity]) =>
        identity?.userAgent.includes("Chrome/152.0.0.0"),
      ),
    ).toBe(true);
  });

  it("re-applies identity after DevTools detached an in-flight command", async () => {
    let release = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let begin = () => {};
    const beginning = new Promise<void>((resolve) => {
      begin = resolve;
    });
    const test = harness(
      vi.fn(async () => {
        begin();
        await blocked;
      }),
    );
    const request = test.request();
    await beginning;
    test.emit("devtools-opened");
    release();
    expect(await request.completed).toEqual({});
    await test.emit("did-stop-loading");
    await test.restore(test.contents);
    expect(test.configure).toHaveBeenCalledTimes(3);
    expect(test.configure).toHaveBeenLastCalledWith(test.contents, null);
  });

  it("removes lifecycle observers when the WebContents is destroyed", async () => {
    const test = harness();
    await test.request().completed;
    test.emit("destroyed");
    expect(test.listeners.size).toBe(0);
  });
});
