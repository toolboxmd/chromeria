import { describe, expect, it } from "vite-plus/test";

import type { ThreadFeedEntry } from "../../lib/threadActivity";
import {
  PROMACHOS_STATUS_ROW_ID,
  PROMACHOS_TYPING_ROW_ID,
  promachosFeedRows,
} from "./promachosFeed";

// Only the fields the presentation reads; the rest of each row is irrelevant here.
const row = (fields: Record<string, unknown>) =>
  ({
    createdAt: "2026-11-23T14:00:00.000Z",
    runId: "run-1",
    ...fields,
  }) as unknown as ThreadFeedEntry;
const activity = (type: string) => ({ projectedItem: { item: { type } } });

describe("promachosFeedRows", () => {
  it("keeps the conversation and errors, and collapses his work into one status row", () => {
    const user = row({ type: "message", id: "user", message: { role: "user", streaming: false } });
    const reply = row({
      type: "message",
      id: "reply",
      message: { role: "assistant", streaming: false },
    });
    const rows = promachosFeedRows([
      user,
      row({ type: "run-fold", id: "fold", workingSince: "2026-11-23T14:00:00.000Z" }),
      row({ type: "activity-group", id: "tools", activities: [activity("dynamic_tool")] }),
      row({ type: "work-toggle", id: "read", live: false, summary: "Read 2 files" }),
      row({
        type: "activity-group",
        id: "mixed",
        activities: [activity("dynamic_tool"), activity("error")],
      }),
      reply,
      row({ type: "work-toggle", id: "search", live: true, summary: "Searching the web" }),
      row({ type: "thinking", id: "live" }),
    ]);
    expect(rows.map((entry) => entry.id)).toEqual([
      "user",
      "mixed",
      "reply",
      PROMACHOS_STATUS_ROW_ID,
    ]);
    expect(rows[1]).toMatchObject({ activities: [activity("error")] });
    expect(rows.at(-1)).toMatchObject({ type: "thinking", label: "Searching the web" });
  });

  it("holds his reply back while it is written, showing the typing status instead", () => {
    const reply = row({
      type: "message",
      id: "reply",
      message: { role: "assistant", streaming: true },
    });
    expect(promachosFeedRows([row({ type: "thinking", id: "live" }), reply])).toEqual([
      expect.objectContaining({ id: PROMACHOS_TYPING_ROW_ID }),
    ]);
  });

  it("keeps requests for the user and shows no status once he is done", () => {
    const request = row({
      type: "activity-group",
      id: "secret",
      activities: [activity("secret_request")],
    });
    expect(
      promachosFeedRows([request, row({ type: "work-toggle", id: "done", live: false })]),
    ).toEqual([request]);
  });
});
