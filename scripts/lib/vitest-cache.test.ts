// @effect-diagnostics nodeBuiltinImport:off - Exercises the installed runner lifecycle in an isolated subprocess.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import { expect, test } from "vite-plus/test";

const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));

// A separate runner exercises the installed Vitest lifecycle, including the
// forks pool's on-disk transforms, without sharing this suite's Vite server.
async function runCacheProbe(project: boolean, kill: boolean) {
  const sandbox = await NodeFSP.mkdtemp(NodePath.join(repoRoot, ".vitest-cache-proof-"));
  const systemTemp = NodePath.join(sandbox, "system-tmp");
  await NodeFSP.mkdir(systemTemp);
  await NodeFSP.writeFile(
    NodePath.join(sandbox, "basic.test.js"),
    'import { test, expect } from "vite-plus/test"; test("transforms a module", () => expect(42).toBe(42));',
  );
  const script = `
    import { createVitest } from "vite-plus/test/node";
    import { existsSync, readdirSync } from "node:fs";
    const root = ${JSON.stringify(sandbox)};
    // Vitest 5.0.5 persists transforms in fsModuleCache by default and only makes these
    // temp copies without it, so the probe turns it off to exercise the copies.
    const test = { root, include: ["basic.test.js"], exclude: [], pool: "forks", watch: false, fsModuleCache: false };
    const vitest = await createVitest({
      ...test, configFile: false, reporters: [{}],
      ${project ? 'projects: [{ test: { ...test, name: "child" } }],' : ""}
    });
    await vitest.start();
    if (vitest.state.getUnhandledErrors().length || process.exitCode) throw new Error("probe test failed");
    const paths = [vitest._tmpDir, ...vitest.projects.map(p => p.tmpDir)];
    const populated = paths.filter(p => existsSync(p) && readdirSync(p).includes("ssr"));
    process.send({ paths, populated });
    ${
      kill
        ? "await new Promise(() => {});"
        : `
      await vitest.close();
      await vitest.close();
      process.send({ remaining: paths.filter(existsSync) });
      process.disconnect();
    `
    }
  `;
  const child = NodeChildProcess.spawn(process.execPath, ["--input-type=module", "-e", script], {
    cwd: repoRoot,
    env: { ...process.env, TMPDIR: systemTemp, TEMP: systemTemp, TMP: systemTemp },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  let output = "";
  child.stdout?.on("data", (chunk) => {
    output += chunk;
  });
  child.stderr?.on("data", (chunk) => {
    output += chunk;
  });
  const messages: { paths?: string[]; populated?: string[]; remaining?: string[] }[] = [];
  child.on("message", (message) => {
    messages.push(message as (typeof messages)[number]);
    // This exact PID was captured at spawn. No other process is touched.
    if (kill) child.kill("SIGKILL");
  });
  try {
    const result = await new Promise<{ code: number | null; signal: string | null }>(
      (resolve, reject) => {
        child.on("error", reject);
        child.on("close", (code, signal) => resolve({ code, signal }));
      },
    );
    expect(result, output).toEqual(
      kill ? { code: null, signal: "SIGKILL" } : { code: 0, signal: null },
    );
    expect(messages[0]?.populated?.length, output).toBeGreaterThan(0);
    const cacheRoot = NodePath.join(sandbox, "node_modules", ".cache", "vitest");
    for (const path of messages[0]!.paths!) {
      expect(NodePath.dirname(NodePath.resolve(path))).toBe(cacheRoot);
      expect(NodePath.basename(path)).toMatch(/^vitest-\d+-/);
    }
    expect(await NodeFSP.readdir(systemTemp)).toEqual([]);
    if (!kill) expect(messages[1]?.remaining).toEqual([]);
    else {
      for (const path of messages[0]!.populated!) {
        expect(await NodeFSP.readdir(NodePath.join(path, "ssr"))).not.toEqual([]);
      }
    }
  } finally {
    await NodeFSP.rm(sandbox, { recursive: true, force: true });
  }
}

test("root transform copies are removed on close without using system temp", () =>
  runCacheProbe(false, false));
test("project transform copies are removed on close without using system temp", () =>
  runCacheProbe(true, false));
test("a killed forks run leaves identifiable copies only in its worktree cache", () =>
  runCacheProbe(true, true));
