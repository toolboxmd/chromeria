// Dependency-free: CI checks ownership before installing packages.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { parseFeatures } from "./fork-features.mjs";

const order = [
  "direction",
  "glossary",
  "data-foundation",
  "branding",
  "query-interrupt-retry",
  "partial-clone-remotes",
  "macos-temp-realpath-tests",
  "secret-store-atomic-create",
  "launcher-test-fixtures",
  "legacy-terminal-labels",
  "login-shell-capture",
  "grok-reasoning-effort",
  "grok-background-command-end",
  "provider-auto-update",
  "bounded-stored-event-replay",
  "settings-mark",
  "thread-people",
  "issues-browse",
  "issues-links",
  "issues-status",
  "issues-open-links",
  "no-project-sidebar",
  "rejected-connect-link",
  "paired-session-renewal",
  "network-routes",
  "prism-stream-clock",
  "prism-stale-turn-detector",
  "prism-settings",
  "child-threads",
  "child-follow-up-wake",
  "claude-steer-queue",
  "urgent-steering-interruption",
  "contextmd-serving",
  "single-run-status",
  "unchanged-turn-items",
  "parent-run-observers",
  "bare-launch",
  "execution-purpose",
  "prism-toolkit",
  "run-groups",
  "peer-links",
  "scheduled-tasks",
  "spectrum",
  "wight-mode",
  "promachos-mode",
  "thread-tool-policy",
  "thread-parent-crumbs",
  "feature-map",
  "menu-escape-focus",
  "preview-chrome-identity",
  "chrome-login-mirror",
  "usable-secret-requests",
  "outside-agent-oauth-grants",
  "preview-credential-fill",
  "preview-chrome-for-testing",
  "agent-web-research",
  "remote-secret-requests",
  "preview-tab-settlement",
  "page-subscriptions",
  "tool-instructions",
  "nested-cli-startup",
  "stacked-merge-worktree-cleanup",
  "project-restore-guard",
  "fork-maintenance",
];

function git(args, options = {}) {
  return NodeChildProcess.execFileSync("git", args, {
    encoding: "utf8",
    maxBuffer: 1024 ** 3,
    ...options,
  });
}

function matches(path, pattern) {
  return pattern.endsWith("/*") ? path.startsWith(pattern.slice(0, -1)) : path === pattern;
}

function inventory(baseRef, sourceRef, registryRef = sourceRef) {
  const base = git(["rev-parse", "--verify", "--end-of-options", `${baseRef}^{commit}`]).trim();
  const source = git(["rev-parse", "--verify", "--end-of-options", `${sourceRef}^{commit}`]).trim();
  git(["merge-base", "--is-ancestor", base, source]);
  const registry = git([
    "rev-parse",
    "--verify",
    "--end-of-options",
    `${registryRef}^{commit}`,
  ]).trim();
  const features = parseFeatures(
    git(["show", `${registry}:docs/fork-features.md`]),
    git(["show", `${registry}:scripts/fork-upstream-edits.txt`]),
  );
  const paths = git(["diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", base, source])
    .split("\0")
    .filter(Boolean);
  const groups = new Map(features.map((feature) => [feature.id, []]));
  const failures = [];
  for (const path of paths) {
    const owners = features.filter((feature) =>
      [...feature.newFiles, ...feature.upstreamFiles].some((pattern) => matches(path, pattern)),
    );
    if (owners.length !== 1) {
      failures.push(
        `${path}: expected exactly one feature owner, found ${owners.length} (${owners.map((f) => f.id).join(", ")})`,
      );
    } else {
      groups.get(owners[0].id).push(path);
    }
  }
  if (failures.length) throw new Error(failures.join("\n"));
  return { base, source, registry, paths, groups };
}

function generate({ base, source, registry, paths, groups }) {
  for (const owner of groups.keys()) {
    if (!order.includes(owner)) throw new Error(`${owner}: missing dependency order`);
  }
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fork-consolidate-"));
  const env = { ...process.env, GIT_INDEX_FILE: NodePath.join(directory, "index") };
  const indexGit = (args, input) => git(args, { env, input });
  const entries = new Map(
    git(["ls-tree", "-rz", "--full-tree", source])
      .split("\0")
      .filter(Boolean)
      .map((entry) => [entry.slice(entry.indexOf("\t") + 1), entry]),
  );
  const commits = [];
  let parent = base;
  try {
    indexGit(["read-tree", base]);
    for (const owner of order) {
      const owned = groups.get(owner) ?? [];
      if (!owned.length) continue;
      // Remove owned entries before adding their replacements.
      indexGit(["update-index", "--force-remove", "-z", "--stdin"], owned.join("\0") + "\0");
      const additions = owned.filter((path) => entries.has(path)).map((path) => entries.get(path));
      if (additions.length)
        indexGit(["update-index", "-z", "--index-info"], additions.join("\0") + "\0");
      const tree = indexGit(["write-tree"]).trim();
      if (tree === git(["rev-parse", `${parent}^{tree}`]).trim())
        throw new Error(`${owner}: empty commit`);
      parent = git(["commit-tree", tree, "-p", parent], {
        input: `chore(fork): consolidate ${owner}\n\nFork-owner: ${owner}\nFork-source: ${source}\nFork-base: ${base}\nFork-registry: ${registry}\n`,
      }).trim();
      const changed = git(["diff", "--no-renames", "--name-only", "-z", `${parent}^`, parent])
        .split("\0")
        .filter(Boolean)
        .sort();
      if (JSON.stringify(changed) !== JSON.stringify([...owned].sort())) {
        throw new Error(`${owner}: tree-entry update changed another owner’s paths`);
      }
      commits.push({ owner, commit: parent, paths: owned.length });
    }
    const tree = git(["rev-parse", `${parent}^{tree}`]).trim();
    if (tree !== git(["rev-parse", `${source}^{tree}`]).trim())
      throw new Error("candidate tree differs from source");
    git(["diff", "--quiet", source, parent]);
    return { base, source, registry, candidate: parent, tree, paths: paths.length, commits };
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
}

try {
  const [command, base, source, ...extra] = process.argv.slice(2);
  if (
    !["check", "generate"].includes(command) ||
    !base ||
    !source ||
    (extra.length && (extra.length !== 2 || extra[0] !== "--registry"))
  ) {
    throw new Error(
      "usage: node scripts/fork-consolidate.mjs <check|generate> <base> <source> [--registry <ref>]",
    );
  }
  const data = inventory(base, source, extra[1]);
  console.log(
    JSON.stringify(
      command === "generate"
        ? generate(data)
        : {
            base: data.base,
            source: data.source,
            registry: data.registry,
            paths: data.paths.length,
            owners: [...data.groups]
              .filter(([, paths]) => paths.length)
              .map(([owner, paths]) => ({ owner, paths: paths.length })),
          },
      null,
      2,
    ),
  );
} catch (error) {
  console.error(`fork-consolidate: FAIL: ${error.message}`);
  process.exitCode = 1;
}
