const assert = require("node:assert/strict");
const { test } = require("node:test");
const { gateBlock, prBlock, reconcile, squashMerge } = require("./merge-reviewed.cjs");

const sha = "a".repeat(40);
const pr = {
  number: 42,
  state: "open",
  draft: false,
  title: "Fix tests",
  labels: [],
  base: { ref: "main" },
  head: { ref: "fix/tests", sha, repo: { id: 1 } },
};
const green = { status: "completed", conclusion: "success" };
const runs = ["ci", "fork"].map((name) => ({
  ...green,
  path: `.github/workflows/${name}.yml`,
  event: "pull_request",
  head_branch: pr.head.ref,
  head_repository: pr.head.repo,
}));
const review = { context: "review/independent", state: "success", creator: { login: "lukemaj" } };

for (const [name, change] of Object.entries({
  closed: { state: "closed" },
  draft: { draft: true },
  base: { base: { ref: "release" } },
  pushed: { head: { ...pr.head, sha: "b".repeat(40) } },
  absorption: { head: { ...pr.head, ref: "candidate/937" } },
  title: { title: "DO NOT MERGE: proof for #937" },
  label: { labels: [{ name: "do-not-merge" }] },
})) {
  test(`blocks ${name}`, () => assert.ok(prBlock({ ...pr, ...change }, sha)));
}

for (const state of ["pending", "failure", "error"]) {
  test(`blocks latest ${state} review or other status`, () => {
    assert.ok(gateBlock(pr, runs, [green], [{ ...review, state }, review]));
    assert.ok(gateBlock(pr, runs, [green], [review, { context: "external", state }]));
  });
}
test("requires the latest exact-head review to be from lukemaj", () => {
  assert.ok(gateBlock(pr, runs, [green], []));
  assert.ok(gateBlock(pr, runs, [green], [{ ...review, creator: { login: "other" } }, review]));
});
test("requires both PR workflows and completed green checks", () => {
  for (const run of runs) assert.ok(gateBlock(pr, [run], [green], [review]));
  assert.ok(gateBlock(pr, [...runs, { status: "queued" }], [green], [review]));
  for (const conclusion of ["failure", "cancelled", "neutral", "timed_out", null]) {
    assert.ok(gateBlock(pr, runs, [{ ...green, conclusion }], [review]));
  }
  assert.ok(gateBlock(pr, runs, [{ status: "in_progress" }], [review]));
  assert.ok(gateBlock(pr, runs, [], [review]));
  assert.equal(
    gateBlock(
      pr,
      runs,
      [green, { ...green, conclusion: "skipped" }],
      [review, { ...review, state: "failure" }],
    ),
    undefined,
  );
});

for (const [name, payload] of Object.entries({
  status: { sha },
  workflow: { workflow_run: { head_sha: sha } },
  rerun: { workflow_run: { head_sha: sha, run_attempt: 2 } },
  external: { check_run: { head_sha: sha } },
})) {
  test(`${name} completion reconciles current state and binds the write to the reviewed head`, async () => {
    const rest = {
      actions: { listWorkflowRunsForRepo: "runs" },
      checks: { listForRef: "checks" },
      repos: {
        listCommitStatusesForRef: "statuses",
      },
      pulls: { list: "candidates", get: async () => ({ data: pr }) },
    };
    const pages = {
      runs,
      checks: [green],
      statuses: [review],
      candidates: [{ ...pr, head: { ...pr.head, sha: "b".repeat(40) } }, pr],
    };
    const github = {
      rest,
      paginate(method, args) {
        if (method === "candidates") {
          assert.equal(args.state, "open");
          assert.equal(args.base, "main");
        } else assert.equal(args.head_sha ?? args.ref, sha);
        return pages[method];
      },
    };
    const inputs = {
      github,
      context: { payload, repo: { owner: "toolboxmd", repo: "chromeria" } },
      core: { info() {} },
    };
    const writes = [];
    const merge = async (...args) => {
      writes.push(args);
      return { merged: true };
    };
    await reconcile(inputs, merge);
    assert.deepEqual(writes, [[pr.number, sha]]);
    let reads = 0;
    rest.pulls.get = async () => ({ data: ++reads === 1 ? pr : { ...pr, draft: true } });
    await reconcile(inputs, merge);
    assert.equal(writes.length, 1);
  });
}

test("a push after the last read makes the squash request fail closed", async (t) => {
  const request = t.mock.method(globalThis, "fetch", async (url, init) => {
    assert.equal(url, "https://api.github.com/repos/toolboxmd/chromeria/pulls/42/merge");
    assert.equal(init.method, "PUT");
    assert.deepEqual(JSON.parse(init.body), { sha, merge_method: "squash" });
    return { ok: false, status: 409 };
  });
  await assert.rejects(squashMerge(pr.number, sha, ""), /Missing CHROMERIA_MERGE_TOKEN/);
  await assert.rejects(squashMerge(pr.number, sha, "fixture"), /HTTP 409/);
  assert.equal(request.mock.callCount(), 1);
});
