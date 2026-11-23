import { describe, expect, it } from "vite-plus/test";

import { resolveWorkspaceLaunch } from "./workspace-launch";

const conversation = { environmentId: "environment-1", threadId: "thread-1" };

describe("resolveWorkspaceLaunch", () => {
  it("reopens the tab used last, and the Promachos tab its last conversation", () => {
    expect(
      resolveWorkspaceLaunch({
        launchIntent: false,
        lastTab: "issues",
        promachosConversation: conversation,
      }),
    ).toEqual({ tab: "issues", promachosConversation: null });
    expect(
      resolveWorkspaceLaunch({
        launchIntent: false,
        lastTab: "promachos",
        promachosConversation: conversation,
      }),
    ).toEqual({ tab: "promachos", promachosConversation: conversation });
    expect(
      resolveWorkspaceLaunch({
        launchIntent: false,
        lastTab: "promachos",
        promachosConversation: undefined,
      }),
    ).toEqual({ tab: "promachos", promachosConversation: null });
  });

  it("leaves a link, notification or quick action launch and an unknown tab on the default", () => {
    const none = { tab: undefined, promachosConversation: null };
    expect(
      resolveWorkspaceLaunch({
        launchIntent: true,
        lastTab: "promachos",
        promachosConversation: conversation,
      }),
    ).toEqual(none);
    expect(
      resolveWorkspaceLaunch({
        launchIntent: false,
        lastTab: "settings",
        promachosConversation: undefined,
      }),
    ).toEqual(none);
    expect(
      resolveWorkspaceLaunch({
        launchIntent: false,
        lastTab: undefined,
        promachosConversation: undefined,
      }),
    ).toEqual(none);
  });
});
