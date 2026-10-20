// @effect-diagnostics nodeBuiltinImport:off - Drives real Chrome for Testing profiles and a local fixture server.
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { EnvironmentId } from "@t3tools/contracts";
import { chromium, type BrowserContext } from "playwright-core";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";

import {
  expectFullChrome,
  testChromium,
  testChromiumVersion,
  testHeadlessShell,
  testHost,
} from "./ChromeForTesting.testkit.ts";
import { presentAsChrome, ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";

const PAGE = `<!doctype html><script src="/app.js"></script><script>
  window.results = (async () => {
    const reply = (port) => new Promise((resolve) => (port.onmessage = (event) => resolve(event.data)));
    // Listen before any await: a worker's first message is lost without a listener.
    const dedicated = reply(new Worker("/dedicated.js"));
    const shared = reply(new SharedWorker("/shared.js").port);
    const service = new Promise((resolve) => (navigator.serviceWorker.onmessage = (event) => resolve(event.data)));
    await navigator.serviceWorker.register("/service.js");
    (await navigator.serviceWorker.ready).active.postMessage("ua");
    const high = await navigator.userAgentData.getHighEntropyValues(["fullVersionList"]);
    return {
      page: navigator.userAgent,
      brands: navigator.userAgentData.brands.map((brand) => brand.brand),
      fullVersionList: high.fullVersionList,
      dedicated: await dedicated,
      shared: await shared,
      service: await service,
    };
  })();
</script>`;
const SCRIPTS: Record<string, string> = {
  "/app.js": "window.appLoaded = true;",
  "/dedicated.js": "postMessage(navigator.userAgent);",
  "/shared.js": "onconnect = (event) => event.ports[0].postMessage(navigator.userAgent);",
  "/service.js":
    "self.addEventListener('message', (event) => event.source.postMessage(navigator.userAgent));",
};

let server: NodeHttp.Server;
let origin: string;
const requests: Array<{ readonly path: string; readonly headers: NodeHttp.IncomingHttpHeaders }> =
  [];
let root: string;

beforeAll(async () => {
  root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "chromeria-cft-contexts-"));
  server = NodeHttp.createServer((request, response) => {
    const path = request.url ?? "/";
    requests.push({ path, headers: request.headers });
    const script = SCRIPTS[path];
    response.setHeader("Content-Type", script ? "text/javascript" : "text/html");
    response.end(script ?? PAGE);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("The fixture did not bind.");
  // localhost is a secure context, so client hints and service workers are on.
  origin = `http://localhost:${address.port}`;
});

afterAll(async () => {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  await NodeFSP.rm(root, { recursive: true, force: true });
});

const makePool = (profilesDir: string) =>
  new ServerBrowserContexts({
    profilesDir,
    executable: async () => testChromium!,
    version: testChromiumVersion,
    host: testHost,
  });

const withPool = async (name: string, run: (pool: ServerBrowserContexts) => Promise<void>) => {
  const pool = makePool(NodePath.join(root, name));
  try {
    await run(pool);
  } finally {
    await pool.close();
  }
};

const sessionCookies = async (context: BrowserContext) =>
  (await context.cookies(origin)).map((cookie) => `${cookie.name}=${cookie.value}`);

describe.skipIf(!testChromium)("ServerBrowserContexts on Chrome for Testing", () => {
  it("presents one Chrome identity to the page, its workers and every request", async () => {
    await withPool("identity", async (pool) => {
      const context = await pool.contextFor("default");
      const page = await context.newPage();
      await presentAsChrome(await context.newCDPSession(page), pool.identity);
      requests.length = 0;
      await page.goto(origin);
      await expectFullChrome(page);
      const seen = await page.evaluate<{
        page: string;
        brands: string[];
        fullVersionList: Array<{ brand: string; version: string }>;
        dedicated: string;
        shared: string;
        service: string;
      }>("window.results");
      const userAgent = pool.identity.userAgent;
      expect(userAgent).toMatch(/ Chrome\/\d+\.0\.0\.0 Safari\/537\.36$/);
      expect(seen).toMatchObject({
        page: userAgent,
        dedicated: userAgent,
        shared: userAgent,
        service: userAgent,
      });
      expect(seen.brands).toContain("Google Chrome");
      expect(seen.fullVersionList).toContainEqual({
        brand: "Google Chrome",
        version: testChromiumVersion,
      });
      expect(JSON.stringify(seen)).not.toContain("Headless");
      const paths = requests.map((request) => request.path);
      expect(paths).toEqual(expect.arrayContaining(["/", "/app.js", ...Object.keys(SCRIPTS)]));
      for (const request of requests) {
        expect(request.headers["user-agent"], request.path).toBe(userAgent);
      }
      const document = requests.find((request) => request.path === "/")!;
      expect(document.headers["sec-ch-ua"]).toContain('"Google Chrome";v="154"');
      expect(document.headers["sec-ch-ua"]).not.toContain("Headless");
    });
  }, 60_000);

  it("keeps a profile's session alive while its tabs close, and recreates the reserved page", async () => {
    await withPool("keep-alive", async (pool) => {
      const context = await pool.contextFor("default");
      const reserved = await pool.reservedPage(context);
      const tab = await context.newPage();
      await tab.goto(origin);
      await tab.evaluate("document.cookie = 'session=kept; path=/'");
      await tab.close();
      expect(context.pages()).toEqual([reserved]);
      expect(await pool.contextFor("default")).toBe(context);
      expect(await sessionCookies(context)).toContain("session=kept");

      const mirror = new ServerCookieMirror(pool);
      const batch = (sequence: number, value: string) => ({
        environmentId: EnvironmentId.make("synthetic-environment"),
        sequence,
        profile: { id: "default", name: "Default", kind: "persistent" as const },
        mutations: [
          {
            type: "set" as const,
            cookie: {
              name: "mirrored",
              value,
              domain: "localhost",
              path: "/",
              secure: false,
              httpOnly: false,
              sameSite: "lax" as const,
            },
          },
        ],
      });
      await mirror.apply(batch(0, "first"));
      // Closing the last page ends the session on Linux, which is why nothing but
      // this test ever closes it; the profile itself must still work afterwards.
      await reserved.close();
      const replacement = await pool.reservedPage(context);
      expect(replacement).not.toBe(reserved);
      expect(replacement.isClosed()).toBe(false);
      // The mirror's session went with the old page; it attaches to the new one.
      await mirror.apply(batch(1, "second"));
      expect(await sessionCookies(context)).toContain("mirrored=second");
      const reopened = await context.newPage();
      await reopened.goto(origin);
      expect(await reopened.evaluate<string>("document.cookie")).toContain("mirrored=second");
    });
  }, 60_000);

  it.skipIf(!testHeadlessShell)(
    "opens a profile the headless shell wrote with its cookies intact",
    async () => {
      const profilesDir = NodePath.join(root, "upgrade");
      const legacy = await chromium.launchPersistentContext(NodePath.join(profilesDir, "default"), {
        executablePath: testHeadlessShell!,
        headless: true,
      });
      await legacy.addCookies([
        {
          name: "remembered",
          value: "from-the-shell",
          url: origin,
          // 2100-01-01: a persistent cookie, not a session one.
          expires: 4_102_444_800,
        },
      ]);
      await legacy.close();
      await withPool("upgrade", async (pool) => {
        const context = await pool.contextFor("default");
        expect(await sessionCookies(context)).toContain("remembered=from-the-shell");
      });
    },
    60_000,
  );
});
