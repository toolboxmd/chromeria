import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";

const git = (operation: string, cwd: string, args: ReadonlyArray<string>) =>
  GitVcsDriver.GitVcsDriver.pipe(
    Effect.flatMap((driver) =>
      driver.execute({ operation, cwd, args, allowNonZeroExit: true, timeoutMs: 120_000 }),
    ),
    Effect.map((result) => (result.exitCode === 0 ? result.stdout.trim() : null)),
    Effect.orElseSucceed(() => null),
  );

/** Ends a destination's refusal of a pinned commit none of its remotes has; the parent names its remote. */
export const MISSING_BASE_REFUSAL = "No remote of this checkout has that commit.";

const notPushed = (detail: string) =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message: `Isolation "worktree" in a linked environment starts the child at this checkout's commit, which that machine fetches from a remote, but ${detail}. Push the branch with an upstream (git push -u), or omit isolation to work in that project's checkout.`,
  });

/**
 * This checkout's HEAD, refused unless it is on the branch's upstream: another machine can
 * fetch only pushed commits, and a newer or older commit there is not the parent's.
 */
export const resolveBase = Effect.fn("DelegatedWorktreeBase.resolveBase")(function* (cwd: string) {
  const commitSha = yield* git("DelegatedWorktreeBase.head", cwd, [
    "rev-parse",
    "--verify",
    "--quiet",
    "HEAD^{commit}",
  ]);
  if (commitSha === null)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "Unable to resolve the parent checkout commit.",
    });
  const headRef = yield* git("DelegatedWorktreeBase.branch", cwd, ["symbolic-ref", "-q", "HEAD"]);
  if (headRef === null) return yield* notPushed("this checkout has no branch");
  const [trackingRef = "", remoteName = ""] = (
    (yield* git("DelegatedWorktreeBase.upstream", cwd, [
      "for-each-ref",
      "--format=%(upstream)%09%(upstream:remotename)",
      headRef,
    ])) ?? ""
  ).split("\t");
  // A remote of "." tracks another local branch, which no other machine can fetch.
  if (!trackingRef.startsWith("refs/remotes/"))
    return yield* notPushed("its branch has no upstream branch");
  const pushed = yield* git("DelegatedWorktreeBase.pushed", cwd, [
    "merge-base",
    "--is-ancestor",
    commitSha,
    trackingRef,
  ]);
  if (pushed === null) return yield* notPushed(`HEAD ${commitSha} is not on ${remoteName}`);
  return commitSha;
});

/**
 * Makes the exact commit `commitSha` present in `cwd` by fetching it from this checkout's
 * remotes, which serve reachable commits by id. False when it is still missing; a base that
 * is not a full commit id is left to launch preflight.
 */
export const fetchBase = Effect.fn("DelegatedWorktreeBase.fetchBase")(function* (
  cwd: string,
  commitSha: string,
) {
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(commitSha)) return true;
  const present = git("DelegatedWorktreeBase.present", cwd, [
    "cat-file",
    "-e",
    `${commitSha}^{commit}`,
  ]).pipe(Effect.map((result) => result !== null));
  if (yield* present) return true;
  const remotes = ((yield* git("DelegatedWorktreeBase.remotes", cwd, ["remote"])) ?? "")
    .split("\n")
    .filter(Boolean);
  for (const remote of remotes) {
    yield* git("DelegatedWorktreeBase.fetch", cwd, [
      "fetch",
      "--quiet",
      "--no-tags",
      "--",
      remote,
      commitSha,
    ]);
    if (yield* present) return true;
  }
  return false;
});

/** The remote this checkout's branch is pushed to, by name and URL without credentials. */
export const upstreamRemote = Effect.fn("DelegatedWorktreeBase.upstreamRemote")(function* (
  cwd: string,
) {
  const headRef = yield* git("DelegatedWorktreeBase.branch", cwd, ["symbolic-ref", "-q", "HEAD"]);
  if (headRef === null) return undefined;
  const remoteName = yield* git("DelegatedWorktreeBase.upstream", cwd, [
    "for-each-ref",
    "--format=%(upstream:remotename)",
    headRef,
  ]);
  if (remoteName === null || remoteName === "" || remoteName === ".") return undefined;
  const url = yield* git("DelegatedWorktreeBase.remoteUrl", cwd, [
    "remote",
    "get-url",
    "--",
    remoteName,
  ]);
  if (url === null) return undefined;
  return `${remoteName} (${url.replace(/^(https?:\/\/)[^/@]*@/, "$1")})`;
});
