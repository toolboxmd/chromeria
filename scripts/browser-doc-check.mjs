// Dependency-free: fork CI runs before installing workspace packages.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const guide = "docs/user/browser.md";
const waiver = "browser doc: no user-visible change";
const browserPaths = [
  "apps/server/src/preview/",
  "apps/web/src/browser/",
  "apps/desktop/src/preview/",
  "apps/server/src/mcp/toolkits/preview/",
  "apps/server/src/mcp/toolkits/previewControls/",
];
const git = (...args) =>
  NodeChildProcess.execFileSync("git", args, {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  }).trim();

function checkPullRequest() {
  if (process.env.GITHUB_EVENT_NAME !== "pull_request") {
    console.log("browser-doc-check: PR guard skipped outside a pull_request event.");
    return;
  }
  const event = JSON.parse(NodeFS.readFileSync(process.env.GITHUB_EVENT_PATH, "utf8"));
  const pr = event.pull_request;
  const base = pr?.base?.sha;
  const head = pr?.head?.sha;
  if (
    typeof base !== "string" ||
    typeof head !== "string" ||
    !/^[a-f0-9]{40}$/.test(base) ||
    !/^[a-f0-9]{40}$/.test(head) ||
    (pr.body !== null && typeof pr.body !== "string")
  ) {
    throw new Error("Missing or invalid pull request base, head or body.");
  }
  if (git("rev-parse", "HEAD") !== head) {
    throw new Error("Check the PR head, not a merge commit or another checkout.");
  }
  // Compare trees, not an upstream merge base: absorption rebases rewrite the fork stack.
  const paths = git("diff", "--no-renames", "--name-only", "-z", base, head).split("\0");
  const changedBrowser = paths.some(
    (path) =>
      browserPaths.some((prefix) => path.startsWith(prefix)) ||
      /^apps\/server\/src\/mcp\/PreviewAutomationBroker\./.test(path),
  );
  if (!changedBrowser) {
    console.log("browser-doc-check: no browser code changed.");
    return;
  }
  const updatedGuide = git(
    "diff",
    "--no-renames",
    "--name-only",
    "--diff-filter=AM",
    "-z",
    base,
    head,
    "--",
    guide,
  )
    .split("\0")
    .includes(guide);
  const optedOut = (pr.body ?? "").split(/\r?\n/).some((line) => line.trim() === waiver);
  if (!updatedGuide && !optedOut) {
    throw new Error(`Browser code changed: update ${guide} or add this PR-body line: ${waiver}`);
  }
  console.log(
    `browser-doc-check: ${updatedGuide ? "guide updated" : "PR declares no user-visible change"}.`,
  );
}

function checkNumbers() {
  const read = (path) => NodeFS.readFileSync(NodePath.resolve(path), "utf8");
  const doc = read(guide);
  const server = read("apps/server/src/preview/ServerBrowser.ts");
  const chrome = read("apps/server/src/preview/PreviewBrowser.ts");
  const capture = (text, pattern, label) => {
    const value = pattern.exec(text)?.[1];
    if (value === undefined)
      throw new Error(`Cannot read ${label}; update the browser number check.`);
    return value;
  };
  const constant = (name) => {
    const expression = capture(server, new RegExp(`const ${name} = ([\\d *]+);`), name);
    return expression.split("*").reduce((product, factor) => product * Number(factor.trim()), 1);
  };
  const equal = (documented, actual, label) => {
    if (Number(documented) !== actual) {
      throw new Error(`${label}: guide says ${documented}, code says ${actual}.`);
    }
  };
  equal(
    capture(doc, /after (\d+) minutes unused/, "idle minutes"),
    constant("AGENT_TAB_IDLE_MS") / 60_000,
    "Idle minutes",
  );
  equal(
    capture(doc, /up to (\d+) tabs per agent session/, "agent tab limit"),
    constant("AGENT_TAB_LIMIT"),
    "Per-agent tab limit",
  );
  equal(
    capture(doc, /and (\d+) per environment/, "environment tab limit"),
    constant("SERVER_TAB_LIMIT"),
    "Per-environment tab limit",
  );
  equal(
    capture(doc, /Chrome for Testing (\d+)/, "Chrome major"),
    Number(capture(chrome, /const VERSION = "(\d+)\./, "pinned Chrome major")),
    "Chrome major",
  );
  console.log("browser-doc-check: documented numbers match code constants.");
}

try {
  if (process.argv.length === 2) checkPullRequest();
  else if (process.argv.length === 3 && process.argv[2] === "--numbers") checkNumbers();
  else throw new Error("Usage: node scripts/browser-doc-check.mjs [--numbers]");
} catch (error) {
  console.error(`browser-doc-check: FAIL: ${error.message}`);
  process.exitCode = 1;
}
