import type { RunId } from "@t3tools/contracts";

import {
  isFeedbackRequestActivityGroup,
  isLinkRequestActivityGroup,
  isSecretRequestActivityGroup,
  type ThreadFeedEntry,
} from "../../lib/threadActivity";

/** The one status row a Promachos chat shows while he works. */
export const PROMACHOS_STATUS_ROW_ID = "promachos-status";

type StatusRow = Extract<ThreadFeedEntry, { readonly type: "thinking" }>;
type ActivityGroup = Extract<ThreadFeedEntry, { readonly type: "activity-group" }>;

function isRequest(group: ActivityGroup) {
  return (
    isSecretRequestActivityGroup(group) ||
    isFeedbackRequestActivityGroup(group) ||
    isLinkRequestActivityGroup(group)
  );
}

/**
 * The conversation and what asks the user for something, with his technical activity collapsed
 * into one status row at the end while he works; desktop's `promachosTimelineRows` on the phone.
 * The status names his latest live step, so the line changes as the work changes.
 */
export function promachosFeedRows(rows: ReadonlyArray<ThreadFeedEntry>): ThreadFeedEntry[] {
  const conversation: ThreadFeedEntry[] = [];
  let status: StatusRow | null = null;
  for (const row of rows) {
    switch (row.type) {
      case "message":
      case "sent-message":
      case "html-render":
      case "mcp-app":
        conversation.push(row);
        break;
      case "activity-group": {
        if (isRequest(row)) {
          conversation.push(row);
          break;
        }
        const errors = row.activities.filter(
          (activity) => activity.projectedItem.item.type === "error",
        );
        if (errors.length > 0) conversation.push({ ...row, activities: errors });
        break;
      }
      case "work-toggle":
        if (row.live) status = statusRow(row, row.summary, status);
        break;
      case "run-fold":
        if (row.workingSince !== undefined) status = statusRow(row, undefined, status);
        break;
      case "thinking":
        status = statusRow(row, row.label ?? row.providerStartingLabel, status);
        break;
      default:
        break;
    }
  }
  return status === null ? conversation : [...conversation, status];
}

function statusRow(row: ThreadFeedEntry, label: string | undefined, previous: StatusRow | null) {
  const shown = label ?? previous?.label;
  return {
    type: "thinking",
    id: PROMACHOS_STATUS_ROW_ID,
    createdAt: row.createdAt,
    runId: "runId" in row ? row.runId : row.message.runId,
    ...(shown === undefined ? {} : { label: shown }),
  } satisfies StatusRow;
}

/** Every turn in the feed, so each is open and no reply hides inside a fold. */
export function promachosRunIds(feed: ReadonlyArray<ThreadFeedEntry>): ReadonlySet<RunId> {
  return new Set(
    feed.flatMap((entry) => {
      const runId = "runId" in entry ? entry.runId : entry.message.runId;
      return runId === null ? [] : [runId];
    }),
  );
}
