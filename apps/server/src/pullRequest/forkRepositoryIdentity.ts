import type { RepositoryIdentity } from "@t3tools/contracts";

/** PR consumers target the checkout's fork; project grouping retains upstream identity. */
export function forkPullRequestIdentity(identity: RepositoryIdentity | null | undefined) {
  if (!identity?.origin) return identity;
  const repository =
    identity.origin.displayName ?? identity.origin.canonicalKey.split("/").slice(1).join("/");
  const segments = repository.split("/");
  return {
    ...identity,
    canonicalKey: identity.origin.canonicalKey,
    displayName: repository,
    owner: segments.slice(0, -1).join("/"),
    name: segments.at(-1)!,
  };
}
