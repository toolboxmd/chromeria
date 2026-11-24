const hold = /\bdo[\s_-]+not[\s_-]+merge\b/i;

function prBlock(pr, sha) {
  if (pr.state !== "open" || pr.draft !== false) return "closed or draft";
  if (pr.base.ref !== "main" || pr.head.sha !== sha) return "base or head changed";
  if (!pr.head.repo) return "head repository unavailable";
  if (pr.head.ref.startsWith("candidate/")) return "absorption candidate";
  if (hold.test(pr.title) || pr.labels.some((label) => hold.test(label.name))) return "merge hold";
}

function gateBlock(pr, runs, checks, statuses) {
  // A queued workflow can exist before GitHub publishes its first check run.
  for (const path of [".github/workflows/ci.yml", ".github/workflows/fork.yml"]) {
    if (
      !runs.some(
        (run) =>
          run.path === path &&
          run.event === "pull_request" &&
          run.head_branch === pr.head.ref &&
          run.head_repository?.id === pr.head.repo.id &&
          run.status === "completed" &&
          run.conclusion === "success",
      )
    )
      return "waiting for " + path;
  }
  if (runs.length >= 1000 || checks.length >= 1000) return "too many results to verify";
  if (
    runs.some(
      (run) => run.status !== "completed" || !["success", "skipped"].includes(run.conclusion),
    )
  )
    return "non-green workflow";
  if (
    !checks.length ||
    checks.some(
      (check) => check.status !== "completed" || !["success", "skipped"].includes(check.conclusion),
    )
  )
    return "missing or non-green check";

  // GitHub returns individual statuses newest first, including their creator.
  const latest = new Map();
  for (const status of statuses) {
    const context = status.context.toLowerCase();
    if (!latest.has(context)) latest.set(context, status);
  }
  const review = latest.get("review/independent");
  if (
    review?.context !== "review/independent" ||
    review.state !== "success" ||
    review.creator?.login !== "lukemaj"
  )
    return "waiting for independent review by lukemaj";
  if ([...latest.values()].some((status) => status.state !== "success"))
    return "non-green commit status";
}

async function squashMerge(number, sha, token = process.env.MERGE_TOKEN) {
  if (!token) throw new Error("Missing CHROMERIA_MERGE_TOKEN");
  const response = await fetch(
    `https://api.github.com/repos/toolboxmd/chromeria/pulls/${number}/merge`,
    {
      method: "PUT",
      redirect: "error",
      headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
      body: JSON.stringify({ sha, merge_method: "squash" }),
    },
  );
  if (!response.ok) throw new Error(`GitHub declined merge of #${number}: HTTP ${response.status}`);
  return response.json();
}

async function reconcile({ github, context, core }, merge = squashMerge) {
  const sha =
    context.payload.workflow_run?.head_sha ??
    context.payload.check_run?.head_sha ??
    context.payload.sha;
  if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error("Missing event head SHA");
  const { repos, actions, checks, pulls } = github.rest;
  const page = (method, args) =>
    github.paginate(method, { ...context.repo, per_page: 100, ...args });
  // Commit-to-PR associations can be empty for commits from a fork.
  const open = await page(pulls.list, { state: "open", base: "main" });
  const candidates = open.filter((pr) => pr.head.sha === sha);
  for (const candidate of candidates) {
    const args = { ...context.repo, pull_number: candidate.number };
    const read = async () => (await pulls.get(args)).data;
    const pr = await read();
    let reason = prBlock(pr, sha);
    if (!reason) {
      const [runs, checkRuns, statuses] = await Promise.all([
        page(actions.listWorkflowRunsForRepo, { head_sha: sha }),
        page(checks.listForRef, { ref: sha, filter: "latest" }),
        page(repos.listCommitStatusesForRef, { ref: sha }),
      ]);
      reason = gateBlock(pr, runs, checkRuns, statuses) || prBlock(await read(), sha);
    }
    if (reason) {
      core.info(`Skip #${pr.number} at ${sha}: ${reason}`);
      continue;
    }
    const data = await merge(pr.number, sha);
    if (!data.merged) throw new Error(`GitHub declined to merge #${pr.number}`);
    core.info(`Merged #${pr.number} at ${sha} as ${data.sha}`);
  }
  if (!candidates.length) core.info(`No open PR to main at ${sha}`);
}

module.exports = { reconcile, gateBlock, prBlock, squashMerge };
