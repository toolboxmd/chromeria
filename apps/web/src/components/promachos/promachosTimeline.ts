import { RunAttemptId, RunId } from "@t3tools/contracts";
import { useMemo } from "react";
import {
  timelineTurnFoldRunIdsByEntryId,
  type MessagesTimelineRow,
  type deriveMessagesTimelineRows,
} from "../chat/MessagesTimeline.logic";

type FoldInput = Pick<
  Parameters<typeof deriveMessagesTimelineRows>[0],
  "timelineEntries" | "latestRun" | "isWorking" | "runlessWorkActive" | "runningRunId"
>;
const WORKING_ROW: MessagesTimelineRow = {
  kind: "working",
  id: "promachos-working",
  createdAt: null,
};

/** The conversation and outputs, with technical activity collapsed into one static indicator. */
export function promachosTimelineRows(rows: MessagesTimelineRow[]): MessagesTimelineRow[] {
  const conversation: MessagesTimelineRow[] = [];
  let working = false;
  for (const row of rows) {
    switch (row.kind) {
      case "message":
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
        working ||= row.active;
        break;
      case "working":
      case "thinking":
        working = true;
        break;
      default:
        break;
    }
  }
  return working ? [...conversation, WORKING_ROW] : conversation;
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
export function usePromachosTimelineDisclosures(input: FoldInput, enabled: boolean) {
  const disclosures = enabled ? promachosTimelineDisclosures(input) : null;
  const runsKey = disclosures ? [...disclosures.runs].join("\n") : null;
  const attemptsKey = disclosures ? [...disclosures.attempts].join("\n") : null;
  return useMemo(
    () =>
      runsKey === null || attemptsKey === null
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
          },
    [runsKey, attemptsKey],
  );
}
