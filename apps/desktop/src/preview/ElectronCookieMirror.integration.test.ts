// @effect-diagnostics nodeBuiltinImport:off - Builds and launches a disposable Electron main process for adapter proof.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import { build } from "vite-plus";
import { describe, expect, it } from "vite-plus/test";

const executable = process.env.CHROMERIA_TEST_ELECTRON;
const execute = NodeUtil.promisify(NodeChildProcess.execFile);

describe.skipIf(!executable)("real Electron cookie mirror adapter", () => {
  it("preserves host scope, session cookies and exact partitioned-cookie siblings in Electron 44", async () => {
    const dir = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "chromeria-electron-mirror-adapter-"),
    );
    const adapter = NodeURL.fileURLToPath(new URL("./ElectronCookieMirror.ts", import.meta.url));
    const entry = NodePath.join(dir, "entry.ts");
    const bundle = NodePath.join(dir, "fixture.cjs");
    try {
      await NodeFSP.writeFile(
        entry,
        `
import { app, session } from "electron";
import assert from "node:assert/strict";
import { electronCookieMirrorStore } from ${JSON.stringify(adapter)};
app.setPath("userData", ${JSON.stringify(NodePath.join(dir, "user-data"))});
app.disableHardwareAcceleration();
app.whenReady().then(async () => {
  let store;
  let stage = "runtime";
  try {
    assert.equal(process.versions.electron?.split(".")[0], "44");
    const browserSession = session.fromPartition("persist:synthetic-mirror-test");
    store = electronCookieMirrorStore(browserSession);
    stage = "write";
    const cookie = { name: "__Host-login", value: "synthetic-session", domain: "login.example.test", path: "/", secure: true, httpOnly: true, sameSite: "unspecified" };
    const partitioned = { name: "partitioned", value: "synthetic-partition", domain: ".example.test", path: "/account", secure: true, httpOnly: true, sameSite: "no_restriction", expires: Math.floor(Date.now()/1000) + 86400, partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: false } };
    const sibling = { ...partitioned, value: "synthetic-sibling", partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: true } };
    const expiryBase = Math.floor(Date.now()/1000) + 86400;
    const fractional = { ...cookie, name: "fractional", expires: expiryBase + 0.123456 };
    await store.apply([cookie, partitioned, sibling, fractional].map(cookie => ({ type: "set", cookie })));
    stage = "ordinary-attributes";
    const ordinary = (await browserSession.cookies.get({ name: "__Host-login" }))[0];
    stage = "ordinary-present";
    assert.ok(ordinary);
    stage = "ordinary-hostOnly";
    assert.equal(ordinary.hostOnly, true);
    stage = "ordinary-session";
    assert.equal(ordinary.session, true);
    stage = "ordinary-sameSite";
    assert.equal(ordinary.sameSite, "unspecified");
    assert.equal(ordinary.httpOnly, true);
    assert.equal(ordinary.secure, true);
    const observed = await store.read();
    stage = "cdp-attributes";
    assert.deepEqual(await session.defaultSession.cookies.get({}), []);
    assert.deepEqual(observed.find(item => item.name === "__Host-login"), cookie);
    stage = "fractional-expiry";
    assert.deepEqual(observed.find(item => item.name === "fractional"), { ...fractional, expires: expiryBase + 0.123 });
    assert.deepEqual(observed.filter(item => item.name === "partitioned").sort((a,b) => Number(a.partitionKey.hasCrossSiteAncestor)-Number(b.partitionKey.hasCrossSiteAncestor)), [partitioned, sibling]);
    await store.apply([{ type: "remove", key: { name: "partitioned", domain: ".example.test", path: "/account", partitionKey: partitioned.partitionKey } }]);
    stage = "exact-removal";
    assert.deepEqual((await store.read()).filter(item => item.name === "partitioned"), [sibling]);
    store.close();
    process.stdout.write("COOKIE_MIRROR_ELECTRON_44_PASS\\n");
    app.exit(0);
  } catch {
    store?.close();
    process.stderr.write("COOKIE_MIRROR_ELECTRON_44_FAILED: " + stage + "\\n");
    app.exit(1);
  }
});
`,
      );
      await build({
        configFile: false,
        logLevel: "silent",
        build: {
          lib: { entry, formats: ["cjs"], fileName: () => "fixture.cjs" },
          outDir: dir,
          emptyOutDir: false,
          minify: false,
          target: "node24",
          rollupOptions: { external: ["electron", /^node:/] },
        },
      });
      const environment = { ...process.env };
      delete environment.ELECTRON_RUN_AS_NODE;
      const result = await execute(
        executable!,
        ["--no-sandbox", "--ozone-platform=headless", "--disable-gpu", bundle],
        { env: environment, timeout: 30_000, maxBuffer: 1024 * 1024 },
      );
      expect(result.stdout).toContain("COOKIE_MIRROR_ELECTRON_44_PASS");
    } finally {
      await NodeFSP.rm(dir, { recursive: true, force: true });
    }
  });
});
