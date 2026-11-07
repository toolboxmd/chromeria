import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { previewStreamDocument, previewStreamMessage } from "./preview-stream-document";

describe("native browser resource messages", () => {
  const resource = {
    type: "preview",
    environmentId: EnvironmentId.make("destination-b"),
    threadId: ThreadId.make("b-thread"),
    tabId: "popup",
    serverEpoch: "epoch-b",
  };
  it("retains the qualified popup and refuses malformed or differently named resources", () => {
    expect(
      previewStreamMessage(JSON.stringify({ type: "popup", tabId: "popup", resource })),
    ).toEqual({ type: "popup", tabId: "popup", resource });
    expect(
      previewStreamMessage(
        JSON.stringify({
          type: "popup",
          tabId: "popup",
          resource: { ...resource, tabId: "rival" },
        }),
      ),
    ).toBeNull();
    expect(
      previewStreamMessage(
        JSON.stringify({
          type: "popup",
          tabId: "popup",
          resource: { ...resource, ticket: "private" },
        }),
      ),
    ).toBeNull();
    expect(
      previewStreamMessage(
        JSON.stringify({
          type: "popup",
          tabId: "popup",
          resource: { ...resource, serverEpoch: "" },
        }),
      ),
    ).toBeNull();
    expect(previewStreamMessage(JSON.stringify({ type: "popup", tabId: "" }))).toBeNull();
    expect(previewStreamMessage('{"type":"popup","tabId":"legacy-popup"}')).toEqual({
      type: "popup",
      tabId: "legacy-popup",
    });
    expect(previewStreamMessage('{"type":"gone"}')).toEqual({ type: "gone" });
  });
  it("embeds resource/session values as data without introducing a new script", () => {
    const configuration = JSON.stringify({
      resource,
      access: { query: { viewerTicket: "</script><script>private</script>" } },
    });
    const document = previewStreamDocument(configuration, "var T3PreviewStream={start(){}};");
    expect(document.match(/<script>/g)).toHaveLength(1);
    expect(document).not.toContain("<script>private");
    expect(document).toContain("destination-b");
  });
});
