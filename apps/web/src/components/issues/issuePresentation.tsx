import type {
  GitHubIssueState,
  IssueGroup,
  IssueGroupReason,
  IssueStatus,
} from "@t3tools/contracts";
import {
  BanIcon,
  BotIcon,
  CircleAlertIcon,
  CircleCheckIcon,
  CircleDotDashedIcon,
  CircleDotIcon,
  CircleIcon,
  CirclePauseIcon,
  CircleSlashIcon,
  EyeIcon,
  HandIcon,
  HourglassIcon,
  MessagesSquareIcon,
} from "lucide-react";
import { msg, useMessages } from "gt-react";

import type { MessageResolver } from "~/i18n/messages";
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
const ISSUE_STATUS_PRESENTATION = {
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

/** Whose move it is; Done and Not planned keep GitHub's own look. */
export const ISSUE_GROUP_PRESENTATION = {
  "your-move": {
    label: msg("Your move", { $context: "issue group: the user has to act next" }),
    toneClassName: "text-amber-600 dark:text-amber-300/90",
    Icon: HandIcon,
  },
  "agents-working": {
    label: msg("Agents working", { $context: "issue group: agents act next" }),
    toneClassName: "text-sky-600 dark:text-sky-300/90",
    Icon: BotIcon,
  },
  waiting: {
    label: msg("Waiting", { $context: "issue group: held by a date or a third party" }),
    toneClassName: "text-zinc-500 dark:text-zinc-400/80",
    Icon: HourglassIcon,
  },
  blocked: {
    label: msg("Blocked", { $context: "issue group: blocked by other open Issues" }),
    toneClassName: "text-red-600 dark:text-red-300/90",
    Icon: BanIcon,
  },
  stalled: {
    label: msg("Stalled", { $context: "issue group: started, and nobody is moving it" }),
    toneClassName: "text-orange-600 dark:text-orange-300/90",
    Icon: CirclePauseIcon,
  },
  "nobody-on-it": {
    label: msg("Nobody on it", { $context: "issue group: free to start" }),
    toneClassName: "text-emerald-600 dark:text-emerald-300/90",
    Icon: CircleIcon,
  },
  done: ISSUE_STATE_PRESENTATION.done,
  "not-planned": ISSUE_STATE_PRESENTATION["not-planned"],
} as const satisfies Record<
  IssueGroup,
  { readonly label: string; readonly toneClassName: string; readonly Icon: unknown }
>;

export function IssueGroupGlyph({ group, className }: { group: IssueGroup; className?: string }) {
  const { Icon, toneClassName } = ISSUE_GROUP_PRESENTATION[group];
  return <Icon aria-hidden className={cn("size-4 shrink-0", toneClassName, className)} />;
}

const ISSUE_REASON_LABEL = {
  "needs-user": msg("Needs you", { $context: "why an Issue waits on the user" }),
  "thread-waits-on-user": msg("A thread is waiting for your answer"),
  waiting: msg("Waiting", { $context: "why an Issue cannot move: a date or a third party" }),
  "reviewed-checks-running": msg("Reviewed, checks running", {
    $context: "state of an Issue's pull request",
  }),
  "reviewed-not-merged": msg("Reviewed, not merged", {
    $context: "state of an Issue's pull request",
  }),
  "last-thread-archived": msg("Last linked thread archived"),
} as const;

/**
 * Why a row sits in its group, as the row's second line says it: facts, never duties. A function
 * of the row's own resolver, not a component, so a list of hundreds of rows mounts no more for it.
 */
export function issueReasonLabel(reason: IssueGroupReason, resolve: MessageResolver): string {
  const blockedBy = (count: number) =>
    resolve(
      msg(
        "{count, plural, one {Blocked by {count} open Issue} other {Blocked by {count} open Issues}}",
        { count },
      ),
    );
  switch (reason.kind) {
    case "blocked":
      return blockedBy(reason.openBlockerCount);
    case "status": {
      const status = resolve(ISSUE_STATUS_PRESENTATION[reason.status].label);
      return reason.openBlockerCount > 0
        ? `${status} · ${blockedBy(reason.openBlockerCount)}`
        : status;
    }
    default:
      return resolve(ISSUE_REASON_LABEL[reason.kind]);
  }
}
