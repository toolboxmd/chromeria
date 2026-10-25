// @effect-diagnostics nodeBuiltinImport:off - Owns Playwright resources outside the Effect runtime.
import { INCOGNITO_BROWSER_PROFILE_ID } from "@t3tools/contracts";
import { constVoid } from "effect/Function";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { Browser, BrowserContext, CDPSession, Page } from "playwright-core";

import { sandboxDisabled } from "./PreviewBrowserHost.ts";

// Playwright needs its files on disk. createRequire also resolves it from a Node SEA executable.
const requirePlaywright = NodeModule.createRequire(import.meta.url);
const loadPlaywright = () =>
  requirePlaywright("playwright-core") as typeof import("playwright-core");

interface Options {
  readonly profilesDir: string;
  readonly executable: () => Promise<string>;
  /** The pinned full version the executable must report, such as `154.0.8037.92`. */
  readonly version: string;
  readonly host: { readonly platform: NodeJS.Platform; readonly arch: string };
  readonly diagnose?: (executable: string, cause: unknown) => Promise<unknown>;
  readonly env?: NodeJS.ProcessEnv;
  readonly onContextClose?: (context: BrowserContext) => void;
}

const UA_PLATFORMS: Partial<Record<NodeJS.Platform, string>> = {
  darwin: "macOS",
  linux: "Linux",
  win32: "Windows",
};

/** Chrome's reduced user agent freezes everything but the major version. */
const REDUCED_UA_PLATFORMS: Partial<Record<NodeJS.Platform, string>> = {
  darwin: "Macintosh; Intel Mac OS X 10_15_7",
  linux: "X11; Linux x86_64",
  win32: "Windows NT 10.0; Win64; x64",
};

/** The full version in a `Browser.getVersion` product or user agent, with or without Headless. */
const chromeVersion = (text: string) => /(?:Headless)?Chrome\/(\d+\.[\d.]+)/.exec(text)?.[1];

class ChromeVersionMismatchError extends Error {
  readonly expected: string;
  readonly actual: string | undefined;
  constructor(expected: string, actual: string | undefined) {
    super(`The preview browser reports Chrome ${actual ?? "unknown"}, not the pinned ${expected}.`);
    this.expected = expected;
    this.actual = actual;
  }
}

/**
 * One identity for the `--user-agent` launch flag, which also reaches workers,
 * and the per-page override that adds matching `Sec-CH-UA`: sites block a user
 * agent that disagrees with its client hints.
 */
export const chromeIdentity = (
  fullVersion: string,
  host: { readonly platform: NodeJS.Platform; readonly arch: string; readonly release: string },
) => {
  const major = fullVersion.split(".", 1)[0]!;
  const brands = (names: (version: string) => string) => [
    { brand: "Chromium", version: names(major) },
    { brand: "Google Chrome", version: names(major) },
    { brand: "Not A(Brand", version: names("99") },
  ];
  return {
    userAgent: `Mozilla/5.0 (${REDUCED_UA_PLATFORMS[host.platform] ?? REDUCED_UA_PLATFORMS.linux}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${major}.0.0.0 Safari/537.36`,
    userAgentMetadata: {
      brands: brands((value) => value),
      fullVersionList: brands((value) => (value === major ? fullVersion : `${value}.0.0.0`)),
      platform: UA_PLATFORMS[host.platform] ?? "Unknown",
      platformVersion: host.platform === "linux" ? host.release : "",
      architecture: host.arch.startsWith("arm") ? "arm" : "x86",
      bitness: host.arch.endsWith("64") ? "64" : "32",
      model: "",
      mobile: false,
      wow64: false,
    },
  };
};

export type ChromeIdentity = ReturnType<typeof chromeIdentity>;

export const presentAsChrome = async (cdp: CDPSession, identity: ChromeIdentity) => {
  await cdp.send("Emulation.setUserAgentOverride", identity);
};

export class ServerBrowserContexts {
  /** A research render owns its browser and fresh storage, with no route to human profiles. */
  static async publicContext(input: {
    readonly executable: string;
    readonly proxyPort: number;
    readonly env: NodeJS.ProcessEnv;
  }) {
    const { chromium } = loadPlaywright();
    const browser = await chromium.launch({
      executablePath: input.executable,
      env: input.env,
      headless: true,
      chromiumSandbox: !sandboxDisabled(input.env),
      timeout: 10_000,
      proxy: { server: `socks5://127.0.0.1:${input.proxyPort}`, bypass: "<-loopback>" },
      args: [
        "--disable-gpu",
        "--disable-quic",
        "--block-new-web-contents",
        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
        // All destination DNS belongs to the proxy, including speculation and worker traffic.
        "--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1",
      ],
    });
    try {
      const context = await browser.newContext({
        viewport: { width: 1280, height: 800 },
        serviceWorkers: "block",
        acceptDownloads: false,
        userAgent: "ChromeriaResearch/1.0 (+https://github.com/toolboxmd/chromeria)",
      });
      return { context, close: () => browser.close() };
    } catch (cause) {
      await browser.close().catch(constVoid);
      throw cause;
    }
  }

  private readonly options: Options;
  readonly identity: ChromeIdentity;
  private readonly contexts = new Map<string, Promise<BrowserContext>>();
  private readonly clearing = new Map<string, Promise<void>>();
  /**
   * One page per persistent context that no tab or tool ever uses: a headed
   * persistent context ends with its last page (pingdotgg/t3code#20867), and
   * session cookies with it.
   */
  private readonly reserved = new WeakMap<BrowserContext, Promise<Page>>();
  private readonly endingContexts = new WeakSet<BrowserContext>();
  private browser: Promise<Browser> | undefined;
  private closing: Promise<void> | undefined;

  constructor(options: Options) {
    this.options = options;
    this.identity = chromeIdentity(options.version, {
      ...options.host,
      release: NodeOS.release(),
    });
  }

  private async launchOptions() {
    const executablePath = await this.options.executable();
    const env = this.options.env ?? process.env;
    return {
      executablePath,
      env,
      args: [
        `--user-agent=${this.identity.userAgent}`,
        "--force-device-scale-factor=2",
        "--disable-blink-features=AutomationControlled",
        // New headless picks a hardware GL backend on its own on macOS; Linux needs Vulkan named.
        ...(this.options.host.platform === "linux" ? ["--use-angle=vulkan"] : []),
      ],
      ignoreDefaultArgs: ["--enable-automation"],
      headless: true,
      // Only an explicit operator opt-out disables sandboxing. Launch errors never do.
      chromiumSandbox: !sandboxDisabled(env),
    };
  }

  private async launch<A>(options: { readonly executablePath: string }, start: () => Promise<A>) {
    try {
      return await start();
    } catch (cause) {
      throw (await this.options.diagnose?.(options.executablePath, cause)) ?? cause;
    }
  }

  private checkVersion(product: string) {
    const actual = chromeVersion(product);
    if (actual !== this.options.version) {
      throw new ChromeVersionMismatchError(this.options.version, actual);
    }
  }

  private sharedBrowser() {
    if (!this.browser) {
      const launched = this.launchOptions().then(async (options) => {
        const { chromium } = loadPlaywright();
        const browser = await this.launch(options, () => chromium.launch(options));
        browser.on("disconnected", () => {
          if (this.browser === launched) this.browser = undefined;
        });
        try {
          this.checkVersion(`Chrome/${browser.version()}`);
        } catch (error) {
          await browser.close().catch(constVoid);
          throw error;
        }
        return browser;
      });
      this.browser = launched;
      void launched.catch(() => {
        if (this.browser === launched) this.browser = undefined;
      });
    }
    return this.browser;
  }

  contextFor(profileId: string, isolationKey?: string): Promise<BrowserContext> {
    if (this.closing) return Promise.reject(new Error("The preview browser is closed."));
    const key = JSON.stringify([profileId, isolationKey ?? null]);
    const cached = this.contexts.get(key);
    if (cached) return cached;
    const cleared = this.clearing.get(key)?.catch(constVoid) ?? Promise.resolve();
    const pending = cleared
      .then(() => this.createContext(profileId, isolationKey))
      .then(async (context) => {
        try {
          // No viewing client can answer a server tab's native authenticator prompt.
          // A context init script reaches every navigation and child frame before site code.
          await context.addInitScript(`(() => {
            if (typeof CredentialsContainer !== "undefined") {
              for (const method of ["get", "create"]) {
                const original = CredentialsContainer.prototype[method];
                CredentialsContainer.prototype[method] = function (...args) {
                  if (args[0]?.publicKey !== undefined) {
                    return Promise.reject(new DOMException(
                      "Authenticators are unavailable in server browser tabs.", "NotAllowedError"
                    ));
                  }
                  return Reflect.apply(original, this, args);
                };
              }
            }
            if (typeof PublicKeyCredential !== "undefined") {
              PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable = () => Promise.resolve(false);
            }
          })()`);
        } catch (error) {
          this.endingContexts.add(context);
          await context.close().catch(constVoid);
          throw error;
        }
        context.on("close", () => {
          this.endingContexts.add(context);
          if (this.contexts.get(key) === pending) this.contexts.delete(key);
          this.options.onContextClose?.(context);
        });
        if (this.closing) {
          await context.close().catch(constVoid);
          throw new Error("The preview browser is closed.");
        }
        return context;
      });
    this.contexts.set(key, pending);
    void pending.catch(() => {
      if (this.contexts.get(key) === pending) this.contexts.delete(key);
    });
    return pending;
  }

  private async createContext(profileId: string, isolationKey?: string) {
    const contextOptions = { viewport: { width: 1280, height: 800 }, deviceScaleFactor: 2 };
    if (isolationKey !== undefined || profileId === INCOGNITO_BROWSER_PROFILE_ID) {
      const browser = await this.sharedBrowser();
      return browser.newContext(contextOptions);
    }
    const directory = this.profileDirectory(profileId);
    const options = await this.launchOptions();
    const { chromium } = loadPlaywright();
    await NodeFSP.mkdir(directory, { recursive: true });
    const context = await this.launch(options, () =>
      chromium.launchPersistentContext(directory, { ...options, ...contextOptions }),
    );
    // The caller listens for close only once this returns, so a close meanwhile fails the start.
    let closed = false;
    context.once("close", () => {
      closed = true;
    });
    try {
      const [first, ...rest] = context.pages();
      for (const page of rest) await page.close().catch(constVoid);
      const page = await this.keepReserved(context, first ?? (await context.newPage()));
      const cdp = await context.newCDPSession(page);
      try {
        this.checkVersion((await cdp.send("Browser.getVersion")).product);
      } finally {
        await cdp.detach().catch(constVoid);
      }
      if (closed) throw new Error("The preview browser closed while it started.");
      return context;
    } catch (error) {
      this.endingContexts.add(context);
      await context.close().catch(constVoid);
      throw error;
    }
  }

  private keepReserved(context: BrowserContext, page: Page): Promise<Page> {
    const reserved = Promise.resolve(page);
    this.reserved.set(context, reserved);
    page.once("close", () => {
      if (this.reserved.get(context) !== reserved || this.endingContexts.has(context)) return;
      const replacement = context.newPage().then((next) => this.keepReserved(context, next));
      this.reserved.set(context, replacement);
      void replacement.catch(constVoid);
    });
    return reserved;
  }

  /**
   * The persistent context's reserved page, recreated if something closed it.
   * Borrowers may attach to it but never close it; isolated contexts have none.
   */
  async reservedPage(context: BrowserContext): Promise<Page> {
    const reserved = this.reserved.get(context);
    if (!reserved) throw new Error("This browser context has no reserved page.");
    return reserved;
  }

  private profileDirectory(profileId: string) {
    const encoded = encodeURIComponent(profileId);
    return NodePath.join(
      this.options.profilesDir,
      profileId === "." || profileId === ".." ? encoded.replaceAll(".", "%2E") : encoded,
    );
  }

  async scratchPage() {
    if (this.closing) throw new Error("The preview browser is closed.");
    const context = await (await this.sharedBrowser()).newContext();
    const page = await context.newPage();
    page.once("close", () => void context.close().catch(constVoid));
    return page;
  }

  async connectDesktopPage(endpoint: string) {
    const { chromium } = loadPlaywright();
    const browser = await chromium.connectOverCDP(endpoint, { timeout: 15_000 });
    const page = browser.contexts().flatMap((context) => context.pages())[0];
    if (!page) {
      await browser.close().catch(constVoid);
      throw new Error("The desktop tab has no page.");
    }
    return { browser, page };
  }

  /**
   * Closes one context and waits until it has; a request for its profile meanwhile
   * waits too, then opens a fresh context from the same directory.
   */
  async closeContext(context: BrowserContext) {
    for (const [key, pending] of this.contexts) {
      if ((await pending.catch(() => undefined)) !== context) continue;
      if (this.contexts.get(key) === pending) this.contexts.delete(key);
      this.endingContexts.add(context);
      const closing = context.close();
      this.clearing.set(key, closing);
      try {
        await closing;
      } finally {
        if (this.clearing.get(key) === closing) this.clearing.delete(key);
      }
      return;
    }
    this.endingContexts.add(context);
    await context.close();
  }

  async clearProfile(profileId: string) {
    if (profileId === INCOGNITO_BROWSER_PROFILE_ID) return;
    const key = JSON.stringify([profileId, null]);
    const clearing = (async () => {
      const pending = this.contexts.get(key);
      if (pending) {
        this.contexts.delete(key);
        const context = await pending.catch(() => undefined);
        if (context) this.endingContexts.add(context);
        await context?.close();
      }
      await NodeFSP.rm(this.profileDirectory(profileId), { recursive: true, force: true });
    })();
    this.clearing.set(key, clearing);
    try {
      await clearing;
    } finally {
      if (this.clearing.get(key) === clearing) this.clearing.delete(key);
    }
  }

  close() {
    this.closing ??= this.dispose();
    return this.closing;
  }

  private async dispose() {
    await Promise.allSettled(
      [...this.contexts.values()].map(async (pending) => {
        const context = await pending;
        this.endingContexts.add(context);
        await context.close();
      }),
    );
    const browser = await this.browser?.catch(() => undefined);
    await browser?.close().catch(constVoid);
    this.contexts.clear();
  }
}
