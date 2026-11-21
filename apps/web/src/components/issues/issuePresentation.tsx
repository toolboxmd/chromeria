import type { GitHubIssueState, IssueStatus } from "@t3tools/contracts";
import {
  BanIcon,
  CircleAlertIcon,
  CircleCheckIcon,
  CircleDotDashedIcon,
  CircleDotIcon,
  CircleIcon,
  CirclePauseIcon,
  CircleSlashIcon,
  EyeIcon,
  MessagesSquareIcon,
} from "lucide-react";
import { msg, useMessages } from "gt-react";

import { cn } from "~/lib/utils";
import { PullRequestGlyph } from "../pullRequest/pullRequestIcons";

/** GitHub's own three states, in its colors: open green, done purple, not planned grey. */
export const ISSUE_STATE_PRESENTATION = {
  open: {
    label: msg("Open", { $context: "issue status" }),
    toneClassName: "text-emerald-600 dark:text-emerald-300/90",
    Icon: CircleDotIcon,
  },
  done: {
    label: msg("Done", { $context: "issue status" }),
    toneClassName: "text-violet-600 dark:text-violet-300/90",
    Icon: CircleCheckIcon,
  },
  "not-planned": {
    label: msg("Not planned", { $context: "issue status" }),
    toneClassName: "text-zinc-500 dark:text-zinc-400/80",
    Icon: CircleSlashIcon,
  },
} as const satisfies Record<GitHubIssueState, unknown>;

export function IssueStateGlyph({
  state,
  className,
}: {
  state: GitHubIssueState;
  className?: string;
}) {
  const { Icon, label, toneClassName } = ISSUE_STATE_PRESENTATION[state];
  const messages = useMessages();
  return (
    <Icon
      aria-label={messages(label)}
      className={cn("size-4 shrink-0", toneClassName, className)}
    />
  );
}

/** The computed status (#29); Done and Not planned keep GitHub's own look. */
export const ISSUE_STATUS_PRESENTATION = {
  done: ISSUE_STATE_PRESENTATION.done,
  "not-planned": ISSUE_STATE_PRESENTATION["not-planned"],
  "in-review": {
    label: msg("In review", { $context: "issue status" }),
    toneClassName: "text-amber-600 dark:text-amber-300/90",
    Icon: EyeIcon,
  },
  "in-progress": {
    label: msg("In progress", { $context: "issue status" }),
    toneClassName: "text-sky-600 dark:text-sky-300/90",
    Icon: CircleDotDashedIcon,
  },
  "waiting-for-merge": {
    label: msg("Waiting for merge", { $context: "issue status" }),
    toneClassName: "text-emerald-600 dark:text-emerald-300/90",
    Icon: PullRequestGlyph.merged,
  },
  "changes-requested": {
    label: msg("Changes requested", { $context: "issue status" }),
    toneClassName: "text-red-600 dark:text-red-300/90",
    Icon: CircleAlertIcon,
  },
  "waiting-for-review": {
    label: msg("Waiting for review", { $context: "issue status" }),
    toneClassName: "text-amber-600 dark:text-amber-300/90",
    Icon: PullRequestGlyph.pullRequest,
  },
  paused: {
    label: msg("Paused", { $context: "issue status" }),
    toneClassName: "text-zinc-500 dark:text-zinc-400/80",
    Icon: CirclePauseIcon,
  },
  blocked: {
    label: msg("Blocked", { $context: "issue status" }),
    toneClassName: "text-red-600 dark:text-red-300/90",
    Icon: BanIcon,
  },
  discussion: {
    label: msg("Discussion", { $context: "issue status" }),
    toneClassName: "text-sky-600 dark:text-sky-300/90",
    Icon: MessagesSquareIcon,
  },
  "to-do": {
    label: msg("To do", { $context: "issue status" }),
    toneClassName: "text-emerald-600 dark:text-emerald-300/90",
    Icon: CircleIcon,
  },
} as const satisfies Record<
  IssueStatus,
  { readonly label: string; readonly toneClassName: string; readonly Icon: unknown }
>;

export function IssueStatusGlyph({
  status,
  className,
}: {
  status: IssueStatus;
  className?: string;
}) {
  const { Icon, label, toneClassName } = ISSUE_STATUS_PRESENTATION[status];
  const messages = useMessages();
  return (
    <Icon
      aria-label={messages(label)}
      className={cn("size-4 shrink-0", toneClassName, className)}
    />
  );
}
