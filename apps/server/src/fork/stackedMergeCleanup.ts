import type * as GitManager from "../git/GitManager.ts";

/**
 * Merge cleanup also accepts a pull request merged into a stack parent or another
 * non-default base. Upstream requires the default branch, so stacked worktrees
 * stay forever after their parent squash-merges. Requiring the PR to name the
 * worktree's exact head keeps the guarantee that every commit is on the remote.
 */
export function stackedPullRequestMerged(
  pullRequest: Pick<GitManager.GitBranchPullRequest, "state" | "headRef" | "headSha"> | null,
  worktree: { readonly branch: string; readonly headSha: string },
): boolean {
  return (
    pullRequest?.state === "merged" &&
    pullRequest.headRef === worktree.branch &&
    pullRequest.headSha === worktree.headSha
  );
}
