import * as NodeEvents from "node:events";

import { INCOGNITO_BROWSER_PROFILE_ID } from "@t3tools/contracts";
import { chromium, type Browser, type BrowserContext, type BrowserType } from "playwright-core";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { ServerBrowserContexts } from "./ServerBrowserContexts.ts";

const launches = vi.hoisted(() => ({
  launch: vi.fn<BrowserType["launch"]>(),
  persistent: vi.fn<BrowserType["launchPersistentContext"]>(),
  mkdir: vi.fn().mockResolvedValue(undefined),
  rm: vi.fn().mockResolvedValue(undefined),
}));
vi.spyOn(chromium, "launch").mockImplementation(launches.launch);
vi.spyOn(chromium, "launchPersistentContext").mockImplementation(launches.persistent);
vi.mock("node:fs/promises", () => ({ mkdir: launches.mkdir, rm: launches.rm }));

const makePage = () => {
  const events = new NodeEvents.EventEmitter();
  return {
    once: vi.fn((event: string, listener: () => void) => events.once(event, listener)),
    close: vi.fn(async () => void events.emit("close")),
  };
};

const makeContext = (product = "HeadlessChrome/154.0.8037.92") => {
  const events = new NodeEvents.EventEmitter();
  const pages: ReturnType<typeof makePage>[] = [makePage()];
  let closed = false;
  return {
    opened: pages,
    addInitScript: vi.fn(async () => {}),
    on: vi.fn((event: string, listener: () => void) => events.on(event, listener)),
    once: vi.fn((event: string, listener: () => void) => events.once(event, listener)),
    pages: vi.fn(() => [...pages]),
    newPage: vi.fn(async () => {
      const page = makePage();
      pages.push(page);
      return page;
    }),
    newCDPSession: vi.fn(async () => ({
      send: vi.fn(async () => ({ product })),
      detach: vi.fn(async () => {}),
    })),
    close: vi.fn(async () => {
      if (!closed) {
        closed = true;
        // Closing a reserved page can add its replacement while this runs.
        for (const page of pages.slice()) await page.close();
        events.emit("close");
      }
    }),
  };
};

const makeBrowser = () => {
  const events = new NodeEvents.EventEmitter();
  const contexts: ReturnType<typeof makeContext>[] = [];
  return {
    contexts,
    version: () => "154.0.8037.92",
    on: vi.fn((event: string, listener: () => void) => events.on(event, listener)),
    newContext: vi.fn(async () => {
      const context = makeContext();
      contexts.push(context);
      return context as unknown as BrowserContext;
    }),
    close: vi.fn(async () => {
      for (const context of contexts) await context.close();
      events.emit("disconnected");
    }),
  };
};

const options = () => ({
  profilesDir: "/test/profiles",
  executable: vi.fn(async () => "/test/chromium"),
  version: "154.0.8037.92",
  host: { platform: "linux" as const, arch: "x64" },
  env: {},
});

beforeEach(() => {
  launches.launch.mockReset();
  launches.persistent.mockReset();
  launches.mkdir.mockClear();
  launches.rm.mockClear();
});

describe("ServerBrowserContexts", () => {
  it("closes a context whose WebAuthn setup fails and retries on the next request", async () => {
    const failed = makeContext();
    failed.addInitScript.mockRejectedValueOnce(new Error("script registration failed"));
    launches.persistent
      .mockResolvedValueOnce(failed as unknown as BrowserContext)
      .mockResolvedValue(makeContext() as unknown as BrowserContext);
    const pool = new ServerBrowserContexts(options());
    await expect(pool.contextFor("default")).rejects.toThrow("script registration failed");
    expect(failed.close).toHaveBeenCalledTimes(1);
    expect(failed.newPage).not.toHaveBeenCalled();
    expect(await pool.contextFor("default")).not.toBe(failed);
    await pool.close();
  });

  it("lazily shares one browser across concurrent isolated sessions and reuses each context", async () => {
    const browser = makeBrowser();
    const launch = Promise.withResolvers<Browser>();
    launches.launch.mockReturnValue(launch.promise);
    const configuration = options();
    const pool = new ServerBrowserContexts(configuration);
    expect(configuration.executable).not.toHaveBeenCalled();
    const first = pool.contextFor("default", "agent-a");
    const again = pool.contextFor("default", "agent-a");
    const second = pool.contextFor("default", "agent-b");
    const incognito = pool.contextFor(INCOGNITO_BROWSER_PROFILE_ID);
    launch.resolve(browser as unknown as Browser);
    const [a, reused, b, privateContext] = await Promise.all([first, again, second, incognito]);
    expect(a).toBe(reused);
    expect(new Set([a, b, privateContext]).size).toBe(3);
    expect(launches.launch).toHaveBeenCalledTimes(1);
    expect(launches.persistent).not.toHaveBeenCalled();
    expect(browser.newContext).toHaveBeenCalledWith({
      viewport: { width: 1280, height: 800 },
      deviceScaleFactor: 2,
    });
    await pool.close();
    expect(browser.close).toHaveBeenCalledTimes(1);
    for (const context of browser.contexts) expect(context.close).toHaveBeenCalled();
  });

  it("keeps human profiles persistent without sharing their storage with agents", async () => {
    const human = makeContext();
    const browser = makeBrowser();
    launches.persistent.mockResolvedValue(human as unknown as BrowserContext);
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    const pool = new ServerBrowserContexts(options());
    const persistent = await pool.contextFor("work/team");
    expect(await pool.contextFor("work/team")).toBe(persistent);
    expect(await pool.contextFor("work/team", "agent")).not.toBe(persistent);
    expect(launches.persistent).toHaveBeenCalledExactlyOnceWith(
      "/test/profiles/work%2Fteam",
      expect.objectContaining({ chromiumSandbox: true, executablePath: "/test/chromium" }),
    );
    expect(launches.mkdir).toHaveBeenCalledWith("/test/profiles/work%2Fteam", { recursive: true });
    await pool.close();
    expect(human.close).toHaveBeenCalledTimes(1);
  });

  it("clears a profile by closing its open context before deleting its storage", async () => {
    const human = makeContext();
    const browser = makeBrowser();
    launches.persistent.mockResolvedValue(human as unknown as BrowserContext);
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    const onContextClose = vi.fn();
    const pool = new ServerBrowserContexts({ ...options(), onContextClose });
    const persistent = await pool.contextFor("work/team");
    const agent = await pool.contextFor("work/team", "agent");
    launches.rm.mockImplementationOnce(async () => {
      expect(human.close).toHaveBeenCalledTimes(1);
    });
    await pool.clearProfile("work/team");
    expect(onContextClose).toHaveBeenCalledExactlyOnceWith(persistent);
    expect(launches.rm).toHaveBeenCalledExactlyOnceWith("/test/profiles/work%2Fteam", {
      recursive: true,
      force: true,
    });
    expect(await pool.contextFor("work/team", "agent")).toBe(agent);
    expect(browser.contexts[0]?.close).not.toHaveBeenCalled();
    launches.persistent.mockResolvedValue(makeContext() as unknown as BrowserContext);
    expect(await pool.contextFor("work/team")).not.toBe(persistent);
    await pool.clearProfile("never-opened");
    expect(launches.rm).toHaveBeenLastCalledWith("/test/profiles/never-opened", {
      recursive: true,
      force: true,
    });
    await pool.close();
  });

  it("opens a profile requested during its clear only after the storage is deleted", async () => {
    launches.persistent.mockImplementation(async () => makeContext() as unknown as BrowserContext);
    const configuration = options();
    const pool = new ServerBrowserContexts(configuration);
    await pool.contextFor("work/team");
    const removing = Promise.withResolvers<void>();
    const removed = Promise.withResolvers<void>();
    let deleted = false;
    launches.rm.mockImplementationOnce(async () => {
      removing.resolve();
      await removed.promise;
      deleted = true;
    });
    const launchedAfterDelete: Array<boolean> = [];
    configuration.executable.mockImplementation(async () => {
      launchedAfterDelete.push(deleted);
      return "/test/chromium";
    });
    const clearing = pool.clearProfile("work/team");
    const reopening = pool.contextFor("work/team");
    await removing.promise;
    removed.resolve();
    await Promise.all([clearing, reopening]);
    expect(launchedAfterDelete).toEqual([true]);
    await pool.close();
  });

  it.each([undefined, "default"])(
    "never retries a failed %s launch with the sandbox disabled",
    async (profile) => {
      const launch = profile ? launches.persistent : launches.launch;
      launch.mockRejectedValue(new Error("sandbox unavailable"));
      const pool = new ServerBrowserContexts(options());
      await expect(pool.contextFor(profile ?? INCOGNITO_BROWSER_PROFILE_ID)).rejects.toThrow(
        "sandbox unavailable",
      );
      expect(launch).toHaveBeenCalledTimes(1);
      expect(
        profile ? launches.persistent.mock.calls[0]?.[1] : launches.launch.mock.calls[0]?.[0],
      ).toMatchObject({ chromiumSandbox: true });
      await pool.close();
    },
  );

  it("allows only an explicit sandbox opt-out", async () => {
    const browser = makeBrowser();
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    const pool = new ServerBrowserContexts({
      ...options(),
      env: { T3CODE_SERVER_BROWSER_SANDBOX: "0" },
    });
    await pool.contextFor(INCOGNITO_BROWSER_PROFILE_ID);
    expect(launches.launch).toHaveBeenCalledWith(
      expect.objectContaining({ chromiumSandbox: false }),
    );
    await pool.close();
  });

  it("retries a failed startup on a later request with sandboxing still enabled", async () => {
    const browser = makeBrowser();
    launches.launch
      .mockRejectedValueOnce(new Error("executable unavailable"))
      .mockResolvedValue(browser as unknown as Browser);
    const pool = new ServerBrowserContexts(options());
    await expect(pool.contextFor("default", "agent")).rejects.toThrow("executable unavailable");
    await pool.contextFor("default", "agent");
    expect(launches.launch).toHaveBeenCalledTimes(2);
    expect(launches.launch).toHaveBeenLastCalledWith(
      expect.objectContaining({ chromiumSandbox: true }),
    );
    await pool.close();
  });

  it("evicts a closed context without closing other agents or relaunching chromium", async () => {
    const browser = makeBrowser();
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    const onContextClose = vi.fn();
    const pool = new ServerBrowserContexts({ ...options(), onContextClose });
    const a = await pool.contextFor("default", "agent-a");
    const b = await pool.contextFor("default", "agent-b");
    await a.close();
    expect(onContextClose).toHaveBeenCalledWith(a);
    expect(await pool.contextFor("default", "agent-a")).not.toBe(a);
    expect(await pool.contextFor("default", "agent-b")).toBe(b);
    expect(browser.close).not.toHaveBeenCalled();
    expect(launches.launch).toHaveBeenCalledTimes(1);
    await pool.close();
  });

  it("cleans up a browser whose launch finishes during shutdown and rejects future work", async () => {
    const browser = makeBrowser();
    const launch = Promise.withResolvers<Browser>();
    launches.launch.mockReturnValue(launch.promise);
    const pool = new ServerBrowserContexts(options());
    const opening = expect(pool.contextFor("default", "agent")).rejects.toThrow("closed");
    const closing = pool.close();
    launch.resolve(browser as unknown as Browser);
    await Promise.all([opening, closing]);
    await expect(pool.contextFor("default", "agent")).rejects.toThrow("closed");
    await pool.close();
    expect(browser.close).toHaveBeenCalledTimes(1);
    expect(browser.contexts[0]?.close).toHaveBeenCalled();
  });
  it("launches with the reduced Chrome user agent its pages present, and the GPU on", async () => {
    launches.persistent.mockResolvedValue(makeContext() as unknown as BrowserContext);
    const pool = new ServerBrowserContexts(options());
    await pool.contextFor("default");
    const args = launches.persistent.mock.calls[0]?.[1]?.args ?? [];
    expect(pool.identity.userAgent).toBe(
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36",
    );
    expect(args).toContain(`--user-agent=${pool.identity.userAgent}`);
    expect(args).toContain("--use-angle=vulkan");
    expect(args).not.toContain("--disable-gpu");
    expect(pool.identity.userAgentMetadata.fullVersionList).toContainEqual({
      brand: "Google Chrome",
      version: "154.0.8037.92",
    });
    await pool.close();
    const mac = new ServerBrowserContexts({
      ...options(),
      host: { platform: "darwin", arch: "arm64" },
    });
    launches.persistent.mockResolvedValue(makeContext() as unknown as BrowserContext);
    await mac.contextFor("default");
    expect(launches.persistent.mock.calls[1]?.[1]?.args).not.toContain("--use-angle=vulkan");
    expect(mac.identity.userAgent).toContain("(Macintosh; Intel Mac OS X 10_15_7)");
    await mac.close();
  });

  it("refuses a browser that is not the pinned version", async () => {
    const human = makeContext("HeadlessChrome/155.0.1.2");
    launches.persistent.mockResolvedValue(human as unknown as BrowserContext);
    const pool = new ServerBrowserContexts(options());
    await expect(pool.contextFor("default")).rejects.toMatchObject({
      name: "Error",
      expected: "154.0.8037.92",
      actual: "155.0.1.2",
    });
    expect(human.close).toHaveBeenCalled();
    const browser = { ...makeBrowser(), version: () => "153.0.1.1" };
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    await expect(pool.contextFor("default", "agent")).rejects.toMatchObject({
      actual: "153.0.1.1",
    });
    expect(browser.close).toHaveBeenCalled();
    await pool.close();
  });

  it("never caches a persistent context that closes while it starts", async () => {
    const crashing = makeContext();
    crashing.newCDPSession.mockImplementationOnce(async () => ({
      send: vi.fn(async () => ({ product: "HeadlessChrome/154.0.8037.92" })),
      // Chrome exits after answering, while the session detaches.
      detach: vi.fn(async () => {
        await crashing.close();
      }),
    }));
    launches.persistent
      .mockResolvedValueOnce(crashing as unknown as BrowserContext)
      .mockResolvedValue(makeContext() as unknown as BrowserContext);
    const pool = new ServerBrowserContexts(options());
    await expect(pool.contextFor("default")).rejects.toThrow("closed while it started");
    const reopened = await pool.contextFor("default");
    expect(reopened).not.toBe(crashing);
    expect(launches.persistent).toHaveBeenCalledTimes(2);
    await pool.close();
  });

  it("keeps a persistent context's first page as its reserved page and recreates it", async () => {
    const human = makeContext();
    launches.persistent.mockResolvedValue(human as unknown as BrowserContext);
    const pool = new ServerBrowserContexts(options());
    const context = await pool.contextFor("default");
    const first = human.opened[0]!;
    expect(await pool.reservedPage(context)).toBe(first);
    expect(first.close).not.toHaveBeenCalled();
    await first.close();
    const replacement = await pool.reservedPage(context);
    expect(replacement).not.toBe(first);
    expect(human.newPage).toHaveBeenCalledTimes(1);
    await pool.close();
    expect(human.newPage).toHaveBeenCalledTimes(1);
    const browser = makeBrowser();
    launches.launch.mockResolvedValue(browser as unknown as Browser);
    const isolated = new ServerBrowserContexts(options());
    await expect(
      isolated.reservedPage(await isolated.contextFor("default", "agent")),
    ).rejects.toThrow("no reserved page");
    await isolated.close();
  });
});
