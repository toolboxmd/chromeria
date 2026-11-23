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

/** The conversation and outputs, with technical activity collapsed into one status row. */
export function promachosTimelineRows(rows: MessagesTimelineRow[]): MessagesTimelineRow[] {
  const conversation: MessagesTimelineRow[] = [];
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
        conversation.push(row);
        break;
      case "proposed-plan":
      case "worktree-setup":
      case "html-render":
      case "mcp-app":
        conversation.push(row);
        break;
      case "event":
        if (row.projectedItem.item.type === "error") conversation.push(row);
        break;
      case "work": {
        const errors = row.groupedEntries.filter(
          (entry) => entry.itemType === "error" || entry.sourceActivityKind === "runtime.error",
        );
        if (errors.length > 0) conversation.push({ ...row, groupedEntries: errors });
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
