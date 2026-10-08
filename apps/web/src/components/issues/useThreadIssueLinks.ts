import type { ScopedThreadRef } from "@t3tools/contracts";
import { visibleThreadPullRequests } from "@t3tools/shared/threadPullRequests";
import { useEffect, useRef } from "react";

import { useThreadShell } from "~/state/entities";
import { issueLinkEnvironment } from "~/state/issueLinks";
import { useEnvironmentQuery } from "~/state/query";
import { useDescendantThreadShells } from "~/state/threadDescendants";

/**
 * A server thread's Issue links, with those its child threads hold. Only the links panel calls
 * this, so a thread view that does not show them reads nothing. Stored links refresh on the
 * server's change push; derived ones follow the thread's branch and pull requests and its
 * descendants' branches, which the thread shells already stream, so a change there rereads.
 */
export function useThreadIssueLinks(threadRef: ScopedThreadRef | null) {
  const thread = useThreadShell(threadRef);
  const descendants = useDescendantThreadShells(threadRef);
  const query = useEnvironmentQuery(
    threadRef === null
      ? null
      : issueLinkEnvironment.forThread({
          environmentId: threadRef.environmentId,
          input: { threadId: threadRef.threadId, includeDescendants: true },
        }),
  );
  const derivationKey =
    thread === null
      ? null
      : JSON.stringify([
          thread.branch,
          visibleThreadPullRequests(thread.pullRequests).map((link) => [
            link.host,
            link.repository,
            link.number,
            link.snapshot?.updatedAt ?? null,
          ]),
          descendants.map((descendant) => [descendant.id, descendant.branch]),
        ]);
  const lastDerivationKey = useRef(derivationKey);
  const { refresh } = query;
  useEffect(() => {
    if (lastDerivationKey.current === derivationKey) return;
    lastDerivationKey.current = derivationKey;
    refresh();
  }, [derivationKey, refresh]);
  return { links: query.data?.links ?? [], error: query.error };
}
