// Fork maintenance routine: drives the real scripts against tiny git fixtures.
// @effect-diagnostics nodeBuiltinImport:off - Drives the real bash/sh routines against throwaway git fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";

const scriptsDir = import.meta.dirname;
const forkCheck = NodePath.join(scriptsDir, "fork-check.sh");
const forkFeatures = NodePath.join(scriptsDir, "fork-features.mjs");
const forkConsolidate = NodePath.join(scriptsDir, "fork-consolidate.mjs");
const forkRebase = NodePath.join(scriptsDir, "fork-rebase.sh");

function git(cwd: string, ...args: Array<string>): string {
  return NodeChildProcess.execFileSync("git", args, { cwd, encoding: "utf8" });
}

function writeMap(root: string, upstreamFiles = ["scripts/fork-upstream-edits.txt"], extra = {}) {
  const feature = {
    id: "fixture",
    purpose: "Protect the fixture capability.",
    issues: ["https://github.com/toolboxmd/t3code/issues/20"],
    prs: ["https://github.com/toolboxmd/t3code/pull/9"],
    newFiles: ["new-capability.txt", "docs/*"],
    upstreamFiles,
    sharedFiles: ["watched.txt"],
    keywords: ["child thread", "parentThreadId"],
    ...extra,
  };
  NodeFS.writeFileSync(
    NodePath.join(root, "docs/fork-features.md"),
    "# Features\n\n```json\n" + JSON.stringify(feature) + "\n```\n",
  );
}

const fixtures: Array<string> = [];
afterEach(() => {
  for (const root of fixtures.splice(0)) NodeFS.rmSync(root, { recursive: true, force: true });
});

function makeFixture(): string {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-fork-check-"));
  fixtures.push(root);
  git(root, "init", "-q", "-b", "work");
  git(root, "config", "user.email", "fork-check@test");
  git(root, "config", "user.name", "fork-check");
  NodeFS.mkdirSync(NodePath.join(root, "docs"), { recursive: true });
  NodeFS.mkdirSync(NodePath.join(root, "scripts"), { recursive: true });
  NodeFS.writeFileSync(NodePath.join(root, "docs", "fork.md"), "# Fork maintenance\n");
  NodeFS.writeFileSync(
    NodePath.join(root, "scripts", "fork-upstream-edits.txt"),
    "# allowlist\nscripts/fork-upstream-edits.txt\n",
  );
  NodeFS.writeFileSync(NodePath.join(root, "upstream-owned.txt"), "base\n");
  writeMap(root);
  git(root, "add", "-A");
  git(root, "commit", "-qm", "base");
  return root;
}

function runCheck(cwd: string, ...args: Array<string>): { status: number; output: string } {
  try {
    const output = NodeChildProcess.execFileSync("bash", [forkCheck, ...args], {
      cwd,
      encoding: "utf8",
      env: { ...process.env, GITHUB_EVENT_NAME: "", GITHUB_EVENT_PATH: "" },
    });
    return { status: 0, output };
  } catch (error) {
    const result = error as { status?: number; stdout?: string; stderr?: string };
    return {
      status: result.status ?? 1,
      output: `${result.stdout ?? ""}${result.stderr ?? ""}`,
    };
  }
}

function baseOf(root: string): string {
  return git(root, "rev-parse", "HEAD").trim();
}

describe("fork-check", () => {
  it("requires upstream/main instead of falling back to origin/main", () => {
    const root = makeFixture();
    const base = baseOf(root);
    git(root, "update-ref", "refs/remotes/origin/main", base);
    const absent = runCheck(root);
    expect(absent.status).toBe(1);
    expect(absent.output).toContain("upstream/main is missing");
    git(root, "update-ref", "refs/remotes/upstream/main", base);
    expect(runCheck(root).status).toBe(0);
  });

  it("passes for fork-only new files on top of the base", () => {
    const root = makeFixture();
    const base = baseOf(root);
    NodeFS.writeFileSync(NodePath.join(root, "docs", "new-fork-doc.md"), "fork only\n");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: new file");
    const result = runCheck(root, "--base", base);
    expect(result.status).toBe(0);
    expect(result.output).toContain("fork-check: OK.");
  });

  it("fails on unallowlisted upstream edits and passes once allowlisted", () => {
    const root = makeFixture();
    const base = baseOf(root);
    NodeFS.writeFileSync(NodePath.join(root, "upstream-owned.txt"), "forked\n");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: touch upstream file");
    const denied = runCheck(root, "--base", base);
    expect(denied.status).toBe(1);
    expect(denied.output).toContain("not allowlisted: upstream-owned.txt");

    NodeFS.appendFileSync(
      NodePath.join(root, "scripts", "fork-upstream-edits.txt"),
      "upstream-owned.txt\ndocs/fork-features.md\n",
    );
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: allowlist the edit");
    writeMap(root, [
      "scripts/fork-upstream-edits.txt",
      "upstream-owned.txt",
      "docs/fork-features.md",
    ]);
    git(root, "add", "-A");
    git(root, "commit", "-qm", "register owner");
    const allowed = runCheck(root, "--base", base);
    expect(allowed.status).toBe(0);
    expect(allowed.output).toContain("modified (allowlisted): upstream-owned.txt");
  });

  it("accepts early matching paths in a long allowlist", () => {
    const root = makeFixture();
    const base = baseOf(root);
    const paths = [
      "docs/fork-features.md",
      "scripts/fork-upstream-edits.txt",
      "upstream-owned.txt",
      ...Array.from({ length: 4096 }, (_, index) => `known-upstream-file-${index}.txt`),
    ];
    NodeFS.writeFileSync(NodePath.join(root, "upstream-owned.txt"), "forked\n");
    NodeFS.writeFileSync(
      NodePath.join(root, "scripts/fork-upstream-edits.txt"),
      paths.join("\n") + "\n",
    );
    writeMap(root, paths);
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: retain a large upstream-edit inventory");

    const result = runCheck(root, "--base", base);
    expect(result.status).toBe(0);
    expect(result.output).toContain("modified (allowlisted): upstream-owned.txt");
  });

  it("requires the removed upstream path to be allowlisted for a rename", () => {
    const root = makeFixture();
    const base = baseOf(root);
    git(root, "mv", "upstream-owned.txt", "new-capability.txt");
    git(root, "commit", "-qm", "fork: rename upstream file");
    // Confirm this is a similarity-detected rename, not just an ordinary edit.
    expect(git(root, "diff", "--name-status", "-M", base, "HEAD")).toContain("R100");
    const denied = runCheck(root, "--base", base);
    expect(denied.status).toBe(1);
    expect(denied.output).toContain("not allowlisted: upstream-owned.txt");
    NodeFS.appendFileSync(
      NodePath.join(root, "scripts/fork-upstream-edits.txt"),
      "upstream-owned.txt\ndocs/fork-features.md\n",
    );
    writeMap(root, [
      "scripts/fork-upstream-edits.txt",
      "upstream-owned.txt",
      "docs/fork-features.md",
    ]);
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: own the removed upstream path");
    const allowed = runCheck(root, "--base", base);
    expect(allowed.status, allowed.output).toBe(0);
  });

  it("fails on merge commits in the stack", () => {
    const root = makeFixture();
    const base = baseOf(root);
    git(root, "checkout", "-qb", "side");
    NodeFS.writeFileSync(NodePath.join(root, "side.txt"), "side\n");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "side");
    git(root, "checkout", "-q", "work");
    NodeFS.writeFileSync(NodePath.join(root, "work.txt"), "work\n");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "work");
    git(root, "merge", "--no-ff", "-m", "merge side", "side");
    const result = runCheck(root, "--base", base);
    expect(result.status).toBe(1);
    expect(result.output).toContain("merge commits in the fork stack");
  });

  it("fails when docs/fork.md is missing", () => {
    const root = makeFixture();
    const base = baseOf(root);
    NodeFS.rmSync(NodePath.join(root, "docs", "fork.md"));
    git(root, "add", "-A");
    git(root, "commit", "-qm", "fork: drop the doc");
    const result = runCheck(root, "--base", base);
    expect(result.status).toBe(1);
    expect(result.output).toContain("docs/fork.md is missing");
  });
});

describe("fork-rebase safety rules", () => {
  it("refuses to push without proof in the same run", () => {
    const root = makeFixture();
    try {
      NodeChildProcess.execFileSync("bash", [forkRebase, "--push"], {
        cwd: root,
        encoding: "utf8",
      });
      expect.unreachable("fork-rebase --push without --proof must fail");
    } catch (error) {
      const result = error as { status?: number; stderr?: string };
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("refusing to push without proof");
    }
  });

  it("refuses to push main directly", () => {
    const root = makeFixture();
    git(root, "checkout", "-qb", "main");
    try {
      NodeChildProcess.execFileSync("bash", [forkRebase, "--proof", "--push"], {
        cwd: root,
        encoding: "utf8",
      });
      expect.unreachable("fork-rebase --push on main must fail");
    } catch (error) {
      const result = error as { status?: number; stderr?: string };
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("refusing to push main directly");
    }
  });
});

describe("fork feature ownership", () => {
  it("rejects an Issue URL in prs instead of accepting any nonempty string", () => {
    const root = makeFixture();
    writeMap(root, undefined, { prs: ["https://github.com/toolboxmd/chromeria/issues/374"] });
    const result = runCheck(root, "--base", baseOf(root));
    expect(result.status).toBe(1);
    expect(result.output).toContain("invalid prs URL");
  });

  it("rejects an allowlisted path without an owner", () => {
    const root = makeFixture();
    writeMap(root, []);
    const result = runCheck(root, "--base", baseOf(root));
    expect(result.status).toBe(1);
    expect(result.output).toContain("expected exactly one feature owner, found 0");
  });

  it("rejects two owners and duplicate feature ids", () => {
    const root = makeFixture();
    const path = NodePath.join(root, "docs/fork-features.md");
    const original = NodeFS.readFileSync(path, "utf8");
    NodeFS.appendFileSync(path, original.replace('"fixture"', '"second"'));
    expect(runCheck(root, "--base", baseOf(root)).output).toContain("found 2");
    NodeFS.writeFileSync(path, original + original);
    expect(runCheck(root, "--base", baseOf(root)).output).toContain("duplicate feature id");
  });

  it("rejects missing or malformed maps and stale ownership", () => {
    const root = makeFixture();
    const path = NodePath.join(root, "docs/fork-features.md");
    NodeFS.rmSync(path);
    expect(runCheck(root, "--base", baseOf(root)).status).toBe(1);
    NodeFS.writeFileSync(path, "```json\n{broken}\n```\n");
    expect(runCheck(root, "--base", baseOf(root)).status).toBe(1);
    writeMap(root, undefined, { id: undefined });
    expect(runCheck(root, "--base", baseOf(root)).output).toContain(
      "invalid or duplicate feature id",
    );
    writeMap(root, ["scripts/fork-upstream-edits.txt", "stale.txt"]);
    expect(runCheck(root, "--base", baseOf(root)).output).toContain(
      "path not allowlisted: stale.txt",
    );
  });
});

function overlap(root: string, base: string, target = "HEAD"): string {
  return NodeChildProcess.execFileSync("node", [forkFeatures, "report", base, target], {
    cwd: root,
    encoding: "utf8",
  });
}

function commitFile(root: string, path: string, content: string, title: string): string {
  NodeFS.writeFileSync(NodePath.join(root, path), content);
  git(root, "add", "-A");
  git(root, "commit", "-qm", title);
  return baseOf(root);
}

describe("upstream overlap report", () => {
  it("reports complete diffs larger than the former 64 MiB limit", () => {
    const root = makeFixture();
    const base = baseOf(root);
    const sha = commitFile(
      root,
      "large.txt",
      "x".repeat(65 * 1024 * 1024) + "parentThreadId\n",
      "large patch",
    );
    const report = overlap(root, base);
    expect(report).toContain(sha);
    expect(report).toContain("keyword (diff): parentThreadId");
  });

  it("matches titles, changed paths and diffs while excluding old and unrelated commits", () => {
    const root = makeFixture();
    commitFile(root, "old.txt", "old", "old child thread");
    const base = baseOf(root);
    const title = commitFile(root, "one.txt", "plain", "CHILD THREAD support");
    const path = commitFile(root, "watched.txt", "plain", "touch watched file");
    const diff = commitFile(root, "two.txt", "parentThreadId", "add linkage");
    const added = commitFile(root, "new-capability.txt", "plain", "new capability");
    const unrelated = commitFile(root, "other.txt", "plain", "unrelated");
    const report = overlap(root, base);
    for (const sha of [title, path, diff, added]) expect(report).toContain(sha);
    expect(report).toContain("keyword (title): child thread");
    expect(report).toContain("file: watched.txt");
    expect(report).toContain("keyword (diff): parentThreadId");
    expect(report).not.toContain("old child thread");
    expect(report).not.toContain(`${unrelated} unrelated`);
    expect(report).toContain("PENDING");
    expect(report).toContain("adopt upstream and delete ours");
    expect(overlap(root, base, base)).toContain("No matches.");
  });

  it("matches deleted/renamed paths and merge changes and groups shared files", () => {
    const root = makeFixture();
    commitFile(root, "watched.txt", "original", "seed");
    const base = baseOf(root);
    const map = NodePath.join(root, "docs/fork-features.md");
    const original = NodeFS.readFileSync(map, "utf8");
    writeMap(root, [], { id: "second" });
    NodeFS.appendFileSync(map, original);
    git(root, "mv", "watched.txt", "renamed.txt");
    git(root, "commit", "-qam", "rename capability");
    const renamed = baseOf(root);
    git(root, "checkout", "-qb", "side");
    const side = commitFile(root, "side.txt", "parentThreadId", "side linkage");
    git(root, "checkout", "-q", "work");
    commitFile(root, "main.txt", "plain", "advance");
    git(root, "merge", "--no-ff", "-qm", "merge feature", "side");
    const merged = baseOf(root);
    const report = overlap(root, base);
    expect(report).toContain("## second");
    expect(report).toContain("## fixture");
    for (const sha of [renamed, side, merged]) expect(report).toContain(sha);
    expect(report).toContain("file: watched.txt");
  });

  it("prints the report before rebase and leaves HEAD unchanged in a dry run", () => {
    const root = makeFixture();
    const base = baseOf(root);
    git(root, "checkout", "-qb", "main");
    commitFile(root, "feature.txt", "parentThreadId", "upstream capability");
    git(root, "checkout", "-q", "work");
    git(root, "remote", "add", "upstream", root);
    const report = NodeChildProcess.execFileSync("bash", [forkRebase, "--dry-run"], {
      cwd: root,
      encoding: "utf8",
    });
    expect(report).toContain("# Upstream overlap report");
    expect(report).toContain("upstream capability");
    expect(report.indexOf("# Upstream overlap report")).toBeLessThan(
      report.indexOf("dry run; nothing changed"),
    );
    expect(baseOf(root)).toBe(base);
  });
});

describe("the real fork documentation", () => {
  it("names the registry and links both fork guides without joining the app map", () => {
    const root = NodePath.join(scriptsDir, "..");
    const registry = NodeFS.readFileSync(NodePath.join(root, "docs/fork-features.md"), "utf8");
    const guide = NodeFS.readFileSync(NodePath.join(root, "docs/fork.md"), "utf8");
    const map = NodeFS.readFileSync(NodePath.join(root, "docs/map.md"), "utf8");
    expect(registry.startsWith("# Fork carried-edit registry\n")).toBe(true);
    expect(registry).toMatch(/\[[^\]]+\]\(fork\.md\)/);
    expect(guide).toMatch(/\[[^\]]+\]\(fork-features\.md\)/);
    expect(map).not.toMatch(/\]\(fork(?:-features)?\.md\)/);
  });
});

it("keeps the current live base in one subgraph, with every live part connected", () => {
  const map = NodeFS.readFileSync(NodePath.join(scriptsDir, "../docs/map.md"), "utf8");
  // Editable fixture for the agreed base, not a permanent inventory of all map parts.
  const expectedLiveIds = ["T3C", "T3S", "T3P", "T3T", "T3R", "T3M"];
  const liveIds = [...map.matchAll(/^\s*%% (\w+): live\s*$/gm)].map((match) => match[1]);
  expect(liveIds.sort()).toEqual([...expectedLiveIds].sort());
  const base = /subgraph T3\["Live T3 base"\]([\s\S]*?)\bend\b/.exec(map)?.[1];
  expect(base).toBeDefined();
  const edges = map.split("\n").filter((line) => /-->|---|-.->/.test(line));
  for (const id of expectedLiveIds) {
    expect(base).toContain(`%% ${id}: live`);
    expect(edges.some((line) => new RegExp(`\\b${id}\\b`).test(line))).toBe(true);
  }
});

function consolidate(
  root: string,
  command: string,
  base: string,
  source: string,
  ...args: Array<string>
) {
  return NodeChildProcess.spawnSync("node", [forkConsolidate, command, base, source, ...args], {
    cwd: root,
    encoding: "utf8",
  });
}

describe("fork consolidation", () => {
  it("requires exclusive ownership of every changed path, ignoring shared watchers", () => {
    const root = makeFixture();
    const base = baseOf(root);
    writeMap(root, undefined, { id: "direction" });
    NodeFS.writeFileSync(NodePath.join(root, "watched.txt"), "unowned");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "unowned watched file");
    const unowned = consolidate(root, "check", base, "HEAD");
    expect(unowned.status).toBe(1);
    expect(unowned.stderr).toContain("watched.txt: expected exactly one feature owner, found 0");
    const originalSource = baseOf(root);
    writeMap(root, undefined, { id: "direction", newFiles: ["docs/*", "watched.txt"] });
    git(root, "add", "-A");
    git(root, "commit", "-qm", "complete ownership");
    const dryRun = consolidate(root, "generate", base, originalSource, "--registry", "HEAD");
    expect(dryRun.status, dryRun.stderr).toBe(0);
    const candidate = (JSON.parse(dryRun.stdout) as { candidate: string }).candidate;
    expect(git(root, "diff", originalSource, candidate)).toBe("");
    const path = NodePath.join(root, "docs/fork-features.md");
    const first = NodeFS.readFileSync(path, "utf8");
    writeMap(root, [], { id: "glossary", newFiles: ["watched.txt"] });
    NodeFS.appendFileSync(path, first);
    git(root, "add", "-A");
    git(root, "commit", "-qm", "duplicate watched ownership");
    const duplicate = consolidate(root, "generate", base, "HEAD");
    expect(duplicate.status).toBe(1);
    expect(duplicate.stderr).toContain("watched.txt: expected exactly one feature owner, found 2");
    expect(baseOf(root)).not.toBe(base);
  });

  it("copies frozen tree entries into disjoint nonempty owner commits without changing refs or the index", () => {
    const root = makeFixture();
    NodeFS.writeFileSync(NodePath.join(root, "removed.txt"), "remove me");
    NodeFS.writeFileSync(NodePath.join(root, "rename.txt"), "rename me");
    NodeFS.writeFileSync(NodePath.join(root, "executable"), "#!/bin/sh\necho base\n");
    git(root, "add", "-A");
    git(root, "commit", "-qm", "upstream entries");
    const base = baseOf(root);
    const upstream = ["upstream-owned.txt", "removed.txt", "rename.txt", "executable"];
    writeMap(root, upstream, {
      id: "direction",
      newFiles: ["assets/*", "renamed.txt", "link", "module"],
    });
    const registry = NodePath.join(root, "docs/fork-features.md");
    const direction = NodeFS.readFileSync(registry, "utf8");
    writeMap(root, ["scripts/fork-upstream-edits.txt", "docs/fork-features.md"], {
      id: "fork-maintenance",
      newFiles: [],
    });
    NodeFS.appendFileSync(registry, direction);
    NodeFS.writeFileSync(
      NodePath.join(root, "scripts/fork-upstream-edits.txt"),
      [...upstream, "scripts/fork-upstream-edits.txt", "docs/fork-features.md"].join("\n") + "\n",
    );
    NodeFS.writeFileSync(NodePath.join(root, "upstream-owned.txt"), "final contents");
    NodeFS.rmSync(NodePath.join(root, "removed.txt"));
    git(root, "mv", "rename.txt", "renamed.txt");
    NodeFS.chmodSync(NodePath.join(root, "executable"), 0o755);
    NodeFS.symlinkSync("renamed.txt", NodePath.join(root, "link"));
    NodeFS.mkdirSync(NodePath.join(root, "assets"));
    NodeFS.writeFileSync(NodePath.join(root, "assets/binary\nname"), Buffer.from([0, 255, 1, 2]));
    git(root, "add", "-A");
    git(root, "update-index", "--add", "--cacheinfo", "160000", base, "module");
    git(root, "commit", "-qm", "frozen fork source");
    const source = baseOf(root);
    NodeFS.writeFileSync(NodePath.join(root, "upstream-owned.txt"), "uncommitted staged change");
    git(root, "add", "upstream-owned.txt");
    const index = git(root, "write-tree");
    const status = git(root, "status", "--porcelain");
    const result = consolidate(root, "generate", base, source);
    expect(result.status, result.stderr).toBe(0);
    const proof = JSON.parse(result.stdout) as {
      candidate: string;
      commits: Array<{ owner: string; commit: string }>;
    };
    expect(git(root, "rev-parse", proof.candidate + "^{tree}")).toBe(
      git(root, "rev-parse", source + "^{tree}"),
    );
    expect(git(root, "diff", "--no-renames", source, proof.candidate)).toBe("");
    expect(git(root, "ls-tree", "-rz", proof.candidate)).toBe(git(root, "ls-tree", "-rz", source));
    expect(git(root, "merge-base", "--is-ancestor", base, proof.candidate)).toBe("");
    expect(git(root, "rev-list", "--merges", base + ".." + proof.candidate)).toBe("");
    expect(proof.commits.map((commit) => commit.owner)).toEqual(["direction", "fork-maintenance"]);
    const changed = proof.commits.map(({ commit, owner }) => {
      expect(git(root, "show", "-s", "--format=%B", commit)).toContain("Fork-owner: " + owner);
      return git(root, "diff", "--no-renames", "--name-only", "-z", commit + "^", commit)
        .split("\0")
        .filter(Boolean);
    });
    expect(changed.every((paths) => paths.length > 0)).toBe(true);
    expect(new Set(changed.flat()).size).toBe(changed.flat().length);
    expect(changed.flat().sort()).toEqual(
      git(root, "diff", "--no-renames", "--name-only", "-z", base, source)
        .split("\0")
        .filter(Boolean)
        .sort(),
    );
    expect(baseOf(root)).toBe(source);
    expect(git(root, "write-tree")).toBe(index);
    expect(git(root, "status", "--porcelain")).toBe(status);
  });
});
