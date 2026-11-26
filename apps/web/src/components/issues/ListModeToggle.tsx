import { useNavigate } from "@tanstack/react-router";
import { T, useGT } from "gt-react";
import { CircleDotIcon } from "lucide-react";

import { useEnvironments } from "~/state/environments";

import { PullRequestGlyph } from "../pullRequest/pullRequestIcons";
import { readPullRequestListPreferences } from "../pullRequest/pullRequestListPreferences";
import { Toggle, ToggleGroup } from "../ui/toggle-group";

export type ListMode = "pull-requests" | "issues";

/**
 * The Pull Requests page's two lists. Both live on one route; Issues is `view=issues`. Hidden on
 * the PR list while no connected server lists Issues.
 */
export function ListModeToggle({ mode }: { mode: ListMode }) {
  const gt = useGT();
  const navigate = useNavigate();
  const { environments } = useEnvironments();
  const issuesSupported = environments.some(
    (environment) => environment.serverConfig?.environment.capabilities.githubIssues === true,
  );
  if (mode === "pull-requests" && !issuesSupported) return null;
  return (
    <ToggleGroup
      aria-label={gt("List", { $context: "which list the page shows" })}
      className="shrink-0"
      variant="segmented"
      value={[mode]}
      onValueChange={(value) => {
        const next = value[0];
        if (next === mode) return;
        if (next === "issues") {
          void navigate({
            to: "/pull-requests",
            search: { ...readPullRequestListPreferences(), view: "issues" },
          });
        }
        if (next === "pull-requests") {
          void navigate({ to: "/pull-requests", search: readPullRequestListPreferences() });
        }
      }}
    >
      <Toggle aria-label={gt("Pull requests")} value="pull-requests">
        <PullRequestGlyph.pullRequest className="size-3.5" />
        <span>
          <T>PRs</T>
        </span>
      </Toggle>
      <Toggle aria-label={gt("Issues")} value="issues">
        <CircleDotIcon className="size-3.5" />
        <span>
          <T>Issues</T>
        </span>
      </Toggle>
    </ToggleGroup>
  );
}
