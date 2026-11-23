import { RunAttemptId, RunId } from "@t3tools/contracts";

import {
  timelineTurnFoldRunIdsByEntryId,
  type MessagesTimelineRow,
  type deriveMessagesTimelineRows,
} from "../chat/MessagesTimeline.logic";

type FoldInput = Pick<
  Parameters<typeof deriveMessagesTimelineRows>[0],
  "timelineEntries" | "latestRun" | "isWorking" | "runlessWorkActive" | "runningRunId"
>;
/** The one status row while he works; its label names his latest live step. */
export const PROMACHOS_STATUS_ROW_ID = "promachos-status";
/** The status row while his reply is being written; the reply shows once it is done. */
export const PROMACHOS_TYPING_ROW_ID = "promachos-typing";
/** Centered gray lines: a time between groups, or something that happened without a message. */
export const PROMACHOS_TIME_ROW_PREFIX = "promachos-time:";
export const PROMACHOS_NOTICE_ROW_PREFIX = "promachos-notice:";

// Events recorded in the conversation that read as one line rather than a message.
const NOTICE_ITEM_TYPES = new Set(["subagent", "handoff", "notification", "system_notice", "fork"]);
// A pause this long starts a new group under its own time line.
const TIME_GAP_MS = 60 * 60 * 1000;

/** The conversation and outputs, with technical activity collapsed into one status row. */
export function promachosTimelineRows(rows: MessagesTimelineRow[]): MessagesTimelineRow[] {
  const conversation: MessagesTimelineRow[] = [];
  let lastAtMs = Number.NEGATIVE_INFINITY;
  const push = (row: MessagesTimelineRow) => {
    const atMs = row.createdAt === null ? Number.NaN : Date.parse(row.createdAt);
    if (!Number.isNaN(atMs)) {
      if (atMs - lastAtMs >= TIME_GAP_MS) {
        conversation.push({
          kind: "thinking",
          id: `${PROMACHOS_TIME_ROW_PREFIX}${row.id}`,
          createdAt: row.createdAt,
        });
      }
      lastAtMs = atMs;
    }
    conversation.push(row);
  };
  let working = false;
  let typing = false;
  let label: string | undefined;
  for (const row of rows) {
    switch (row.kind) {
      case "message":
        if (row.message.role === "assistant" && row.message.streaming) {
          typing = true;
          break;
        }
        push(row);
        break;
      case "proposed-plan":
      case "worktree-setup":
      case "html-render":
      case "mcp-app":
        push(row);
        break;
      case "event": {
        const item = row.projectedItem.item;
        if (item.type === "error") {
          push(row);
          break;
        }
        const label = item.type === "system_notice" ? item.message : item.title;
        if (NOTICE_ITEM_TYPES.has(item.type) && label) {
          push({
            kind: "thinking",
            id: `${PROMACHOS_NOTICE_ROW_PREFIX}${row.id}`,
            createdAt: row.createdAt,
            label,
          });
        }
        break;
      }
      case "work": {
        const errors = row.groupedEntries.filter(
          (entry) => entry.itemType === "error" || entry.sourceActivityKind === "runtime.error",
        );
        if (errors.length > 0) push({ ...row, groupedEntries: errors });
        break;
      }
      case "work-live":
        if (row.active) {
          working = true;
          label = row.entry.toolTitle ?? row.entry.label;
        }
        break;
      case "working":
        working = true;
        break;
      case "thinking":
        working = true;
        label = row.label ?? label;
        break;
      default:
        break;
    }
  }
  if (!working && !typing) return conversation;
  return [
    ...conversation,
    {
      kind: "thinking",
      id: typing ? PROMACHOS_TYPING_ROW_ID : PROMACHOS_STATUS_ROW_ID,
      createdAt: null,
      ...(label === undefined ? {} : { label }),
    },
  ];
}

/** Imported V1 turns use synthetic fold keys; attempt folds can also hide interim replies. */
export function promachosTimelineDisclosures(input: FoldInput) {
  return {
    runs: new Set(timelineTurnFoldRunIdsByEntryId(input).values()),
    attempts: new Set(
      input.timelineEntries.flatMap((entry) => (entry.attempt ? [entry.attempt.id] : [])),
    ),
  };
}

/** Stable sets keep streamed text from invalidating the timeline's disclosure fast path. */
export function resolvePromachosTimelineDisclosures(input: FoldInput, enabled: boolean) {
  const disclosures = enabled ? promachosTimelineDisclosures(input) : null;
  const runsKey = disclosures ? [...disclosures.runs].join("\n") : null;
  const attemptsKey = disclosures ? [...disclosures.attempts].join("\n") : null;
  return runsKey === null || attemptsKey === null
    ? null
    : {
        runs: new Set(
          runsKey
            .split("\n")
            .filter(Boolean)
            .map((id) => RunId.make(id)),
        ),
        attempts: new Set(
          attemptsKey
            .split("\n")
            .filter(Boolean)
            .map((id) => RunAttemptId.make(id)),
        ),
      };
}
