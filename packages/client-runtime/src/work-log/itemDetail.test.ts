import { describe, expect, it } from "vite-plus/test";
import {
  EnvironmentId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { turnItemHasDetail, turnItemOutputImages, turnItemOutputText } from "./itemDetail.ts";

function toolItem(output: unknown): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make("images"),
    threadId: ThreadId.make("thread"),
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: null,
    startedAt: null,
    completedAt: null,
    updatedAt: DateTime.makeUnsafe("2026-10-01T00:00:00Z"),
    type: "dynamic_tool",
    toolName: "screenshots",
    input: {},
    output,
  };
}

describe("turn item detail", () => {
  const subagentItem = (overrides: Record<string, unknown>) =>
    ({
      id: TurnItemId.make("subagent-remote"),
      type: "subagent",
      threadId: ThreadId.make("thread-1"),
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "running",
      title: "Remote roast",
      subagentId: "node-remote",
      childThreadId: null,
      startedAt: DateTime.makeUnsafe("2026-10-05T00:00:00.000Z"),
      completedAt: null,
      updatedAt: DateTime.makeUnsafe("2026-10-05T00:00:01.000Z"),
      ...overrides,
    }) as never;

  it("offers detail for an attached remote subagent but not an unattached one", () => {
    expect(
      turnItemHasDetail(
        subagentItem({
          remoteChild: {
            environmentId: EnvironmentId.make("environment-cavallo"),
            threadId: ThreadId.make("thread-remote"),
            label: "cavallo",
          },
        }),
      ),
    ).toBe(true);
    expect(
      turnItemHasDetail(
        subagentItem({
          remoteChild: {
            environmentId: EnvironmentId.make("environment-cavallo"),
            label: "cavallo",
          },
        }),
      ),
    ).toBe(false);
    expect(turnItemHasDetail(subagentItem({}))).toBe(false);
  });
});

describe("tool image output parsing", () => {
  it.each(["array", "content"] as const)(
    "keeps markers for images beyond the served positions in %s output",
    (shape) => {
      // Detail responses retain image metadata after the server strips image bytes.
      const image = { type: "image", mimeType: "image/png" };
      const blocks = Array.from({ length: 8 }, () => ({ ...image }));
      const wrap = (content: unknown[]) => (shape === "array" ? content : { content });
      const withinLimit = toolItem(wrap(blocks));
      expect(turnItemOutputText(withinLimit)).toBeNull();
      expect(turnItemOutputImages(withinLimit)).toHaveLength(8);

      const overflow = toolItem(
        wrap([
          ...blocks,
          { type: "text", text: "Two additional screenshots:" },
          { ...image, sizeBytes: 100 },
          { type: "image", source: { type: "base64", media_type: "image/jpeg" } },
        ]),
      );
      expect(turnItemOutputImages(overflow)).toHaveLength(8);
      expect(turnItemOutputText(overflow)).toBe("Two additional screenshots:\n[image]\n[image]");
    },
  );
});
