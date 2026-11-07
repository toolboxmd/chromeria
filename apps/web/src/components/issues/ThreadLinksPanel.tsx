import type { ScopedThreadRef } from "@t3tools/contracts";

import { useThreadShell } from "~/state/entities";
import { useLinkedIssueStates } from "~/state/issues";
import { ThreadPullRequestsPanel } from "../pullRequest/ThreadPullRequestsPanel";
import { openLinkedIssueCount } from "./issueLinks.logic";
import { ThreadIssueLinks } from "./ThreadIssueLinks";
import { useThreadIssueLinks } from "./useThreadIssueLinks";

/**
 * The thread's right-panel links: Issues above pull requests. Issues show only where the server
 * advertises `issueLinks`; elsewhere this is the pull request panel alone.
 */
export function ThreadLinksPanel({
  threadRef,
  issueLinks,
}: {
  threadRef: ScopedThreadRef;
  issueLinks: boolean;
}) {
  const { links } = useThreadIssueLinks(issueLinks ? threadRef : null);
  const thread = useThreadShell(threadRef);
  // Each pull request sync rereads the Issues' states too, so a closed Issue shows closed with it.
  const syncKey =
    thread?.pullRequests.reduce<string | null>((latest, link) => {
      const at = link.snapshot?.syncedAt ?? null;
      return at !== null && (latest === null || at > latest) ? at : latest;
    }, null) ?? null;
  const { states: issueStates, entries: issueEntries } = useLinkedIssueStates(
    issueLinks ? threadRef.environmentId : null,
    links,
    syncKey,
  );
  return (
    <div className="flex h-full min-h-0 flex-col">
      {issueLinks ? (
        <ThreadIssueLinks threadRef={threadRef} states={issueStates} entries={issueEntries} />
      ) : null}
      <div className="min-h-0 flex-1">
        <ThreadPullRequestsPanel
          threadRef={threadRef}
          issues={
            issueLinks
              ? { open: openLinkedIssueCount(links, issueStates), linked: links.length }
              : undefined
          }
        />
      </div>
    </div>
  );
}
