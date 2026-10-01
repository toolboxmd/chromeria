import { EventId, MessageId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { deriveTimelineEntries, deriveWorkLogEntries } from "../../session-logic";
import type { ChatMessage } from "../../types";
import { deriveMessagesTimelineRows } from "../chat/MessagesTimeline.logic";
import { promachosTimelineRows } from "../promachos/promachosTimeline";

const turnId = TurnId.make("turn-1");

function toolActivity(id: string, createdAt: string): OrchestrationThreadActivity {
  return {
    id: EventId.make(id),
    kind: "tool.completed",
    summary: "Ran command",
    tone: "tool",
    turnId,
    createdAt,
    payload: { itemType: "command_execution", detail: "ls" },
  };
}

const sharing: OrchestrationThreadActivity = {
  id: EventId.make("sharing:command-1"),
  kind: "thread.sharing",
  summary: "Luke shared this thread with Pauli",
  tone: "info",
  turnId: null,
  createdAt: "2026-10-01T00:00:02.000Z",
  payload: { action: "shared", actor: "Luke", coOwners: ["Pauli"] },
};

const userMessage: ChatMessage = {
  id: MessageId.make("message-1"),
  role: "user",
  text: "Look around",
  turnId,
  createdAt: "2026-10-01T00:00:00.000Z",
  updatedAt: "2026-10-01T00:00:00.000Z",
  streaming: false,
};

// A share made while the turn runs lands between that turn's tool calls.
function rowsDuringRunningTurn() {
  const work = deriveWorkLogEntries([
    toolActivity("tool-1", "2026-10-01T00:00:01.000Z"),
    sharing,
    toolActivity("tool-2", "2026-10-01T00:00:03.000Z"),
  ]);
  return deriveMessagesTimelineRows({
    timelineEntries: deriveTimelineEntries([userMessage], [], work),
    isWorking: true,
    runningTurnId: turnId,
    activeTurnStartedAt: userMessage.createdAt,
    turnDiffSummaries: [],
    supportsConversationRollback: false,
  });
}

function sharingLabels(rows: ReturnType<typeof deriveMessagesTimelineRows>) {
  return rows.flatMap((row) =>
    row.kind === "work"
      ? row.groupedEntries
          .filter((entry) => entry.sourceActivityKind === "thread.sharing")
          .map((entry) => entry.label)
      : [],
  );
}

describe("sharing activity in the timeline", () => {
  it("shows the server's summary as its own row, not folded into the turn's tool work", () => {
    expect(sharingLabels(rowsDuringRunningTurn())).toEqual(["Luke shared this thread with Pauli"]);
  });

  it("stays in a Promachos chat, which hides tool work", () => {
    const rows = promachosTimelineRows(rowsDuringRunningTurn());
    expect(sharingLabels(rows)).toEqual(["Luke shared this thread with Pauli"]);
    expect(rows.some((row) => row.kind === "work-live" || row.kind === "work-toggle")).toBe(false);
  });
});
