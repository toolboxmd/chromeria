import {
  MessageId,
  RunId,
  RunAttemptId,
  NodeId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import { deriveTimelineEntriesFromVisibleTurnItems, type TimelineEntry } from "../../session-logic";
import {
  deriveMessagesTimelineRows,
  type MessagesTimelineRow,
} from "../chat/MessagesTimeline.logic";
import { promachosTimelineDisclosures, promachosTimelineRows } from "./promachosTimeline";

const at = (n: number) => `2026-01-01T00:00:${String(n).padStart(2, "0")}Z`;
function message(
  id: string,
  role: "user" | "assistant",
  n: number,
  runId: RunId | null = null,
): TimelineEntry {
  return {
    id,
    kind: "message",
    createdAt: at(n),
    message: {
      id: MessageId.make(id),
      role,
      text: id,
      runId,
      createdAt: at(n),
      updatedAt: at(n),
      streaming: false,
    },
  };
}
function chat(input: Parameters<typeof deriveMessagesTimelineRows>[0]) {
  const disclosures = promachosTimelineDisclosures(input);
  return promachosTimelineRows(
    deriveMessagesTimelineRows({
      ...input,
      expandedRunIds: disclosures.runs,
      expandedAttemptIds: disclosures.attempts,
    }),
  );
}
const base = {
  latestRun: null,
  isWorking: false,
  turnDiffSummaries: [],
  supportsConversationRollback: false,
};
describe("Promachos V2 timeline", () => {
  it("keeps imported runless interim replies and native completed-run replies unfolded", () => {
    const runId = RunId.make("native");
    const timelineEntries = [
      message("prompt", "user", 0),
      message("interim", "assistant", 2),
      {
        id: "tool",
        kind: "work" as const,
        createdAt: at(3),
        entry: {
          id: "tool",
          createdAt: at(3),
          runId: null,
          label: "Ran git",
          tone: "tool" as const,
        },
      },
      message("answer", "assistant", 4),
      message("native-prompt", "user", 5, runId),
      message("native-interim", "assistant", 6, runId),
      message("native-answer", "assistant", 8, runId),
    ];
    const input = {
      ...base,
      timelineEntries,
      latestRun: { runId, status: "completed" as const, startedAt: at(5), completedAt: at(8) },
    };
    const rows = chat(input);
    expect(rows.filter((row) => row.kind === "message").map((row) => row.message.text)).toEqual([
      "prompt",
      "interim",
      "answer",
      "native-prompt",
      "native-interim",
      "native-answer",
    ]);
    expect(rows.some((row) => row.kind === "turn-fold" || row.kind === "work")).toBe(false);
  });
  it("preserves superseded attempt replies rather than silently dropping them", () => {
    const runId = RunId.make("retry");
    const old = {
      id: RunAttemptId.make("old"),
      runId,
      attemptOrdinal: 1,
      rootNodeId: NodeId.make("old-root"),
      status: "superseded" as const,
    };
    const current = {
      ...old,
      id: RunAttemptId.make("current"),
      attemptOrdinal: 2,
      status: "running" as const,
    };
    const entries = [
      { ...message("prompt", "user", 0, runId), attempt: old },
      { ...message("old-reply", "assistant", 2, runId), attempt: old },
      { ...message("steer", "user", 3, runId), attempt: current },
      { ...message("current-reply", "assistant", 5, runId), attempt: current },
    ];
    const rows = chat({
      ...base,
      timelineEntries: entries,
      latestRun: { runId, status: "running", startedAt: at(0), completedAt: null },
    });
    expect(rows.filter((row) => row.kind === "message").map((row) => row.message.text)).toContain(
      "old-reply",
    );
    expect(rows.some((row) => row.kind === "attempt-fold")).toBe(false);
  });
  it.each(["provider_error", "usage_limit"] as const)(
    "keeps real projected %s notices visible outside technical work",
    (failureClass) => {
      const now = DateTime.makeUnsafe(at(3));
      const error = {
        id: TurnItemId.make("error"),
        threadId: ThreadId.make("chat"),
        runId: RunId.make("failed"),
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "failed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "error",
        failure: { class: failureClass, message: "Provider stopped", code: null, retryable: false },
      } satisfies OrchestrationV2TurnItem;
      const timelineEntries = deriveTimelineEntriesFromVisibleTurnItems({
        visibleTurnItems: [
          {
            item: error,
            position: 0,
            visibility: "local",
            sourceThreadId: error.threadId,
            sourceItemId: error.id,
          },
        ],
        optimisticMessages: [],
      });
      const rows = chat({ ...base, timelineEntries });
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        kind: "work",
        groupedEntries: [{ projectedItem: { item: error } }],
      });
    },
  );
  it("preserves outputs while collapsing active technical work", () => {
    const outputRows = [
      { kind: "html-render", id: "html", createdAt: at(1) },
      { kind: "mcp-app", id: "app", createdAt: at(2) },
    ] as MessagesTimelineRow[];
    const rows = promachosTimelineRows([
      ...outputRows,
      { kind: "thinking", id: "thought", createdAt: null },
      { kind: "working", id: "work", createdAt: null },
    ]);
    expect(rows.map((row) => row.id)).toEqual(["html", "app", "promachos-working"]);
    expect(promachosTimelineRows(outputRows).map((row) => row.id)).toEqual(["html", "app"]);
  });
});
