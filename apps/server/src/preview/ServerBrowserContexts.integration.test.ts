// @effect-diagnostics nodeBuiltinImport:off - Drives real Chrome for Testing profiles and a local fixture server.
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { EnvironmentId, INCOGNITO_BROWSER_PROFILE_ID } from "@t3tools/contracts";
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
  it.each([
    { name: "persistent", profile: "default" },
    { name: "incognito", profile: INCOGNITO_BROWSER_PROFILE_ID },
    { name: "isolated", profile: "default", isolation: "agent" },
  ])(
    "rejects WebAuthn in $name tabs on every navigation and in child frames",
    async ({ name, profile, isolation }) => {
      await withPool(`webauthn-${name}`, async (pool) => {
        const context = await pool.contextFor(profile, isolation);
        const document = `<!doctype html><script>
        window.results = (async () => {
          const start = performance.now();
          const result = (promise) => Promise.race([
            promise.then(() => "resolved", (error) => error instanceof DOMException ? error.name : "not a DOMException"),
            new Promise((resolve) => setTimeout(() => resolve("pending"), 2000)),
          ]);
          const get = await result(navigator.credentials.get({publicKey: {challenge: new Uint8Array(32)}}));
          const conditional = await result(navigator.credentials.get({mediation: "conditional", publicKey: {challenge: new Uint8Array(32)}}));
          const create = await result(navigator.credentials.create({publicKey: {
            challenge: new Uint8Array(32), rp: {name: "Test"},
            user: {id: new Uint8Array([1]), name: "test", displayName: "Test"},
            pubKeyCredParams: [{type: "public-key", alg: -7}],
          }}));
          return {
            get, conditional, create, elapsed: performance.now() - start,
            uvpaa: await PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable(),
            unsupported: await result(navigator.credentials.create({})),
          };
        })();
      </script>`;
        await context.route("https://**.test/**", (route) =>
          route.fulfill({
            contentType: "text/html",
            body: document,
          }),
        );
        const page = await context.newPage();
        // An empty virtual environment reproduces an authenticator that never answers.
        await (await context.newCDPSession(page)).send("WebAuthn.enable", { enableUI: false });
        for (const path of ["first", "second"]) {
          await page.goto(`https://main.example.test/${path}`);
          await page.evaluate(`(() => {
          for (const host of ["main.example", "child.other"]) {
            const frame = document.createElement("iframe");
            frame.allow = "publickey-credentials-get *; publickey-credentials-create *";
            frame.src = "https://" + host + ".test/frame";
            document.body.append(frame);
          }
        })()`);
          await page.waitForLoadState("load");
          // Frame results are published by their first page script, before any interaction.
          await Promise.all(
            page.frames().map(async (frame) => {
              await frame.waitForFunction("window.results !== undefined");
              const seen = await frame.evaluate<{ elapsed: number }>("window.results");
              expect(seen).toMatchObject({
                get: "NotAllowedError",
                conditional: "NotAllowedError",
                create: "NotAllowedError",
                uvpaa: false,
                unsupported: "NotSupportedError",
              });
              expect(seen.elapsed).toBeLessThan(2000);
            }),
          );
          expect(page.frames()).toHaveLength(3);
          expect(
            await page.evaluate('navigator.credentials.get({password: true, mediation: "silent"})'),
          ).toBeNull();
        }
      });
    },
  );

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
