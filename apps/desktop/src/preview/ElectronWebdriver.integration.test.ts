// @effect-diagnostics nodeBuiltinImport:off - Builds and launches a disposable Electron main process for browser proof.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import { build } from "vite-plus";
import { describe, expect, it } from "vite-plus/test";

const executable = process.env.CHROMERIA_TEST_ELECTRON;
const execute = NodeUtil.promisify(NodeChildProcess.execFile);

describe.skipIf(!executable)("real Electron desktop tab identity", () => {
  it("reports navigator.webdriver false and no Headless marker while a debugger drives the tab", async () => {
    const dir = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "chromeria-electron-webdriver-"),
    );
    const entry = NodePath.join(dir, "entry.ts");
    const bundle = NodePath.join(dir, "fixture.cjs");
    try {
      // The relay attaches the webContents debugger and Playwright enables Runtime through it.
      await NodeFSP.writeFile(
        entry,
        `
import { app, BrowserWindow } from "electron";
app.setPath("userData", ${JSON.stringify(NodePath.join(dir, "user-data"))});
app.disableHardwareAcceleration();
app.whenReady().then(async () => {
  try {
    const window = new BrowserWindow({ show: false });
    await window.loadURL("data:text/html,<title>tab</title>");
    window.webContents.debugger.attach("1.3");
    await window.webContents.debugger.sendCommand("Runtime.enable");
    const { result } = await window.webContents.debugger.sendCommand("Runtime.evaluate", {
      expression: "JSON.stringify({ webdriver: navigator.webdriver, ua: navigator.userAgent })",
      returnByValue: true,
    });
    process.stdout.write("WEBDRIVER_RESULT " + result.value + "\\n");
    app.exit(0);
  } catch (error) {
    process.stderr.write("WEBDRIVER_FAILED " + String(error) + "\\n");
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
      const line = result.stdout.split("\n").find((each) => each.startsWith("WEBDRIVER_RESULT "));
      const seen = JSON.parse(line!.slice("WEBDRIVER_RESULT ".length)) as {
        webdriver: boolean;
        ua: string;
      };
      expect(seen.webdriver).toBe(false);
      expect(seen.ua).not.toContain("Headless");
    } finally {
      await NodeFSP.rm(dir, { recursive: true, force: true });
    }
  });
});
