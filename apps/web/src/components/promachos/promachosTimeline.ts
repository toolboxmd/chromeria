import { TurnId } from "@t3tools/contracts";
import { useMemo } from "react";

import type { TimelineEntry } from "../../session-logic";
import type { MessagesTimelineRow } from "../chat/MessagesTimeline.logic";

const WORKING_ROW: Extract<MessagesTimelineRow, { kind: "working" }> = {
  kind: "working",
  id: "working-indicator-row",
  createdAt: null,
};

/**
 * The timeline as a Promachos chat: the conversation, plans and setup cards,
 * with tool activity, reasoning and turn folds collapsed into one working
 * indicator after the latest message while the turn runs.
 */
export function promachosTimelineRows(rows: MessagesTimelineRow[]): MessagesTimelineRow[] {
  const conversation: MessagesTimelineRow[] = [];
  const queued: MessagesTimelineRow[] = [];
  let working = false;
  for (const row of rows) {
    switch (row.kind) {
      case "message":
        if (row.message.role !== "reasoning") conversation.push(row);
        break;
      case "proposed-plan":
      case "worktree-setup":
        conversation.push(row);
        break;
      case "work":
        // Sharing changes stay in the chat (toolboxmd/chromeria#121).
        if (row.groupedEntries.every((entry) => entry.sourceActivityKind === "thread.sharing")) {
          conversation.push(row);
        }
        break;
      case "queued-message":
        queued.push(row);
        break;
      case "working":
      case "thinking":
        working = true;
        break;
      default:
        break;
    }
  }
  return working ? [...conversation, WORKING_ROW, ...queued] : [...conversation, ...queued];
}

function entryTurnId(entry: TimelineEntry): TurnId | null {
  if (entry.kind === "message") return entry.message.turnId ?? null;
  if (entry.kind === "proposed-plan") return entry.proposedPlan.turnId;
  return entry.entry.turnId ?? null;
}

/**
 * Every turn in the timeline, so a Promachos chat never folds a finished
 * turn: his interim replies stay where they were written. Keyed by the ids,
 * not the entries, so streamed text keeps the timeline's fast path.
 */
export function usePromachosExpandedTurnIds(
  timelineEntries: ReadonlyArray<TimelineEntry>,
  enabled: boolean,
): ReadonlySet<TurnId> | null {
  const key = enabled
    ? [...new Set(timelineEntries.map(entryTurnId).filter((id) => id !== null))].join("\n")
    : null;
  return useMemo(
    () =>
      key === null
        ? null
        : new Set(
            key
              .split("\n")
              .filter(Boolean)
              .map((id) => TurnId.make(id)),
          ),
    [key],
  );
}
