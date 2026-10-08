import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type EnvironmentId, type IssuePullRequest, parseIssueUrl } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useMemo } from "react";

import { useProjects } from "~/state/entities";
import { useEnvironments } from "~/state/environments";
import { buildThreadRouteParams } from "~/threadRoutes";

import { IssueDetailPanel } from "./IssueDetailPanel";
import { issueMarkdownCwd, resolveIssuePanelEnvironment } from "./issueLinks.logic";
import { environmentIdsWithCapability, issueKey } from "./issueList.logic";
import { useStartThreadFromIssue } from "./useStartThreadFromIssue";

const NO_LINKED_PULL_REQUESTS: ReadonlyMap<string, IssuePullRequest> = new Map();

/**
 * An Issue in a thread's right panel, opened from a link in the thread or its linked Issues.
 * The same detail the Issues page shows; the surface tab closes it.
 */
export function ThreadIssuePanel({ environmentId, url }: { environmentId: string; url: string }) {
  const navigate = useNavigate();
  const projects = useProjects();
  const { environments } = useEnvironments();
  const { resolve: resolveStart, start: startFromIssue } = useStartThreadFromIssue();
  const linkEnvironments = useMemo(
    () => new Set(environmentIdsWithCapability(environments, "issueLinks")),
    [environments],
  );
  const reference = parseIssueUrl(url);
  // Read through the thread's server when it reads Issues, else the one the Issues page would use.
  const readEnvironmentId =
    reference === null
      ? null
      : resolveIssuePanelEnvironment(reference, environmentId as EnvironmentId, projects, {
          issues: environmentIdsWithCapability(environments, "issues"),
          issueLinks: [...linkEnvironments],
        });
  const cwd =
    reference === null || readEnvironmentId === null
      ? null
      : issueMarkdownCwd(projects, readEnvironmentId, reference);
  if (reference === null || readEnvironmentId === null || cwd === null) {
    return (
      <div className="flex flex-1 items-center justify-center p-6 text-sm text-muted-foreground">
        No connected server reads this Issue.
      </div>
    );
  }
  const startTarget = resolveStart(reference);
  return (
    <IssueDetailPanel
      key={`${readEnvironmentId} ${issueKey(reference)}`}
      className="w-full flex-1 border-l-0"
      environmentId={readEnvironmentId}
      reference={reference}
      cwd={cwd}
      // The panel reads the Issue again itself; there is no list here to refresh.
      onChanged={() => undefined}
      startDisabledReason={"reason" in startTarget ? startTarget.reason : null}
      linkEnvironments={linkEnvironments}
      linkedPullRequests={NO_LINKED_PULL_REQUESTS}
      onOpenThread={(thread) =>
        void navigate({
          to: "/$environmentId/$threadId",
          params: buildThreadRouteParams(scopeThreadRef(thread.environmentId, thread.id)),
        })
      }
      onStart={(detail) =>
        void startFromIssue({
          ...reference,
          url: detail.url,
          title: detail.title,
          body: detail.body,
        })
      }
    />
  );
}
