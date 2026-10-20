// @effect-diagnostics nodeBuiltinImport:off - Exercises the dependency-free CI check in disposable Git repositories.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

const script = NodePath.join(import.meta.dirname, "browser-doc-check.mjs");
const repository = NodePath.join(import.meta.dirname, "..");
const guide = "docs/user/browser.md";
const roots: Array<string> = [];
const git = (root: string, ...args: Array<string>) =>
  NodeChildProcess.execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
const write = (root: string, path: string, contents: string) => {
  NodeFS.mkdirSync(NodePath.dirname(NodePath.join(root, path)), { recursive: true });
  NodeFS.writeFileSync(NodePath.join(root, path), contents);
};
const commit = (root: string) => {
  git(root, "add", "-A");
  git(root, "commit", "-qm", "fixture");
  return git(root, "rev-parse", "HEAD");
};
const fixture = () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "browser-doc-check-"));
  roots.push(root);
  git(root, "init", "-q", "-b", "work");
  git(root, "config", "user.email", "browser-doc-check@test");
  git(root, "config", "user.name", "browser-doc-check");
  write(root, guide, "Browser guide\n");
  const base = commit(root);
  return { root, base };
};
const check = (root: string, args: Array<string> = [], event?: unknown) => {
  const eventPath = NodePath.join(root, ".git", "event.json");
  if (event !== undefined) NodeFS.writeFileSync(eventPath, JSON.stringify(event));
  const result = NodeChildProcess.spawnSync(process.execPath, [script, ...args], {
    cwd: root,
    encoding: "utf8",
    env: {
      ...process.env,
      GITHUB_EVENT_NAME: event === undefined ? "" : "pull_request",
      GITHUB_EVENT_PATH: eventPath,
    },
  });
  return { status: result.status, output: result.stdout + result.stderr };
};
const pr = (root: string, base: string, body: string | null = null) => ({
  pull_request: { base: { sha: base }, head: { sha: git(root, "rev-parse", "HEAD") }, body },
});

afterEach(() => {
  for (const root of roots.splice(0)) NodeFS.rmSync(root, { recursive: true, force: true });
});

describe("browser documentation PR guard", () => {
  it.each([
    "apps/server/src/preview/ServerBrowser.ts",
    "apps/web/src/browser/BrowserSurface.tsx",
    "apps/desktop/src/preview/CookieMirror.ts",
    "apps/server/src/mcp/toolkits/preview/tools.ts",
    "apps/server/src/mcp/toolkits/previewControls/tools.ts",
    "apps/server/src/mcp/PreviewAutomationBroker.ts",
  ])("rejects %s without a guide change or PR-body declaration", (path) => {
    const { root, base } = fixture();
    write(root, path, "browser change\n");
    commit(root);
    const result = check(root, [], pr(root, base));
    expect(result.status, result.output).toBe(1);
    expect(result.output).toContain("browser doc: no user-visible change");
  });

  it("accepts a guide update or the exact standalone declaration, not a passing mention", () => {
    const { root, base } = fixture();
    write(root, "apps/server/src/preview/ServerBrowser.ts", "browser change\n");
    commit(root);
    expect(
      check(root, [], pr(root, base, "Added browser doc: no user-visible change here")).status,
    ).toBe(1);
    expect(
      check(root, [], pr(root, base, "Summary\r\nbrowser doc: no user-visible change\r\nProof"))
        .status,
    ).toBe(0);
    write(root, guide, "Updated browser guide\n");
    commit(root);
    expect(check(root, [], pr(root, base)).status).toBe(0);
  });

  it("does not accept deleting the guide as an update", () => {
    const { root, base } = fixture();
    write(root, "apps/web/src/browser/example.ts", "browser change\n");
    NodeFS.rmSync(NodePath.join(root, guide));
    commit(root);
    expect(check(root, [], pr(root, base)).status).toBe(1);
  });

  it("includes deleted browser paths when checking a rename out of the browser", () => {
    const { root } = fixture();
    const path = "apps/server/src/preview/example.ts";
    write(root, path, "original\n");
    const base = commit(root);
    git(root, "mv", path, "moved.ts");
    commit(root);
    expect(check(root, [], pr(root, base)).status).toBe(1);
  });

  it("compares PR trees across an upstream absorption, not the new upstream merge base", () => {
    const { root } = fixture();
    const path = "apps/server/src/preview/example.ts";
    write(root, path, "original\n");
    const upstream = commit(root);
    write(root, "fork.txt", "carried fork change\n");
    const base = commit(root);
    git(root, "checkout", "-qb", "absorption", upstream);
    write(root, path, "new upstream behavior\n");
    const newUpstream = commit(root);
    write(root, "fork.txt", "carried fork change\n");
    commit(root);
    expect(git(root, "diff", "--name-only", newUpstream, "HEAD", "--", path)).toBe("");
    expect(check(root, [], pr(root, base)).status).toBe(1);
    expect(check(root, [], pr(root, base, "browser doc: no user-visible change")).status).toBe(0);
  });

  it("allows unrelated PRs and local runs but rejects incomplete PR context or the wrong head", () => {
    const { root, base } = fixture();
    write(root, "unrelated.txt", "change\n");
    commit(root);
    expect(check(root).status).toBe(0);
    expect(check(root, [], pr(root, base)).status).toBe(0);
    expect(check(root, [], {}).status).toBe(1);
    expect(
      check(root, [], { pull_request: { base: { sha: base }, head: { sha: base }, body: null } })
        .status,
    ).toBe(1);
  });
});

describe("documented browser numbers", () => {
  it("matches the real idle timeout, tab limits and pinned Chrome major", () => {
    const result = check(repository, ["--numbers"]);
    expect(result.status, result.output).toBe(0);
  });

  it.each([
    { pattern: /after (\d+) minutes unused/, label: "Idle minutes" },
    { pattern: /up to (\d+) tabs per agent session/, label: "Per-agent tab limit" },
    { pattern: /and (\d+) per environment/, label: "Per-environment tab limit" },
    { pattern: /Chrome for Testing (\d+)/, label: "Chrome major" },
  ])("rejects drift in $label", ({ pattern, label }) => {
    const { root } = fixture();
    for (const path of [
      guide,
      "apps/server/src/preview/ServerBrowser.ts",
      "apps/server/src/preview/PreviewBrowser.ts",
    ]) {
      write(root, path, NodeFS.readFileSync(NodePath.join(repository, path), "utf8"));
    }
    const text = NodeFS.readFileSync(NodePath.join(root, guide), "utf8");
    expect(text).toMatch(pattern);
    write(
      root,
      guide,
      text.replace(pattern, (match, number: string) =>
        match.replace(number, String(Number(number) + 1)),
      ),
    );
    const result = check(root, ["--numbers"]);
    expect(result.status, result.output).toBe(1);
    expect(result.output).toContain(label);
  });
});
