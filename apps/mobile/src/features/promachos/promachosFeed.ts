import type { RunId } from "@t3tools/contracts";

import {
  isFeedbackRequestActivityGroup,
  isLinkRequestActivityGroup,
  isSecretRequestActivityGroup,
  type ThreadFeedEntry,
} from "../../lib/threadActivity";

/** The one status row a Promachos chat shows while he works. */
export const PROMACHOS_STATUS_ROW_ID = "promachos-status";
/** The status row while his reply is being written; the reply shows once it is done. */
export const PROMACHOS_TYPING_ROW_ID = "promachos-typing";
/** Centered gray lines: a time between groups, or something that happened without a message. */
export const PROMACHOS_TIME_ROW_PREFIX = "promachos-time:";
export const PROMACHOS_NOTICE_ROW_PREFIX = "promachos-notice:";

// Events recorded in the conversation that read as one line rather than a message.
const NOTICE_ITEM_TYPES = new Set([
  "subagent",
  "handoff",
  "compaction",
  "notification",
  "system_notice",
  "fork",
]);
// A pause this long starts a new group under its own time line.
const TIME_GAP_MS = 60 * 60 * 1000;

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
  let lastAtMs = Number.NEGATIVE_INFINITY;
  const push = (row: ThreadFeedEntry) => {
    const atMs = Date.parse(row.createdAt);
    if (!Number.isNaN(atMs)) {
      if (atMs - lastAtMs >= TIME_GAP_MS)
        conversation.push(lineRow(row, PROMACHOS_TIME_ROW_PREFIX));
      lastAtMs = atMs;
    }
    conversation.push(row);
  };
  for (const row of rows) {
    switch (row.type) {
      case "message":
        if (row.message.role === "assistant" && row.message.streaming) {
          status = { ...statusRow(row, undefined, status), id: PROMACHOS_TYPING_ROW_ID };
          break;
        }
        push(row);
        break;
      case "sent-message":
      case "html-render":
      case "mcp-app":
        push(row);
        break;
      case "activity-group": {
        if (isRequest(row)) {
          push(row);
          break;
        }
        const errors = row.activities.filter(
          (activity) => activity.projectedItem.item.type === "error",
        );
        if (errors.length > 0) {
          push({ ...row, activities: errors });
          break;
        }
        const live = row.activities.findLast((activity) => activity.live === true);
        if (live !== undefined) status = statusRow(row, live.summary, status);
        for (const activity of row.activities) {
          if (NOTICE_ITEM_TYPES.has(activity.projectedItem.item.type) && activity.summary) {
            push(lineRow(row, PROMACHOS_NOTICE_ROW_PREFIX, activity.summary, activity.id));
          }
        }
        break;
      }
      case "work-toggle":
        if (row.live) status = statusRow(row, row.summary, status);
        break;
      case "run-fold":
        if (row.workingSince !== undefined) status = statusRow(row, undefined, status);
        break;
      case "thinking":
        status = statusRow(row, row.label, status);
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

/** A centered line, carried as a labelled status row; the label is the notice's text. */
function lineRow(row: ThreadFeedEntry, prefix: string, label?: string, key?: string): StatusRow {
  return {
    type: "thinking",
    id: `${prefix}${key ?? row.id}`,
    createdAt: row.createdAt,
    runId: "runId" in row ? row.runId : row.message.runId,
    ...(label === undefined ? {} : { label }),
  };
}
