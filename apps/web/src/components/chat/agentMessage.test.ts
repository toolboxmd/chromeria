import { describe, expect, it } from "vite-plus/test";

import { parseAgentMessage } from "./agentMessage";

describe("parseAgentMessage", () => {
  it("parses a child report", () => {
    expect(
      parseAgentMessage(
        "[Subagent Fix: repo identity (thread sub.abc.1) finished a turn]\n\n\nPR #138 is ready.\nMore detail.",
      ),
    ).toEqual({
      kind: "report",
      title: "Fix: repo identity",
      threadId: "sub.abc.1",
      body: "\nPR #138 is ready.\nMore detail.",
      preview: "PR #138 is ready.",
    });
  });

  it("parses a thread-to-thread message", () => {
    expect(
      parseAgentMessage("[Message from Live check (thread t-42)]\n\nCan you confirm?"),
    ).toEqual({
      kind: "message",
      title: "Live check",
      threadId: "t-42",
      body: "Can you confirm?",
      preview: "Can you confirm?",
    });
  });

  it("keeps parentheses and brackets inside titles", () => {
    expect(
      parseAgentMessage("[Message from Fix (thread x) [v2] (draft) (thread t-1)]\n\nhi"),
    ).toMatchObject({ title: "Fix (thread x) [v2] (draft)", threadId: "t-1" });
    expect(
      parseAgentMessage("[Subagent Retry (2) (thread sub.p.2) finished a turn]\n\nok"),
    ).toMatchObject({ kind: "report", title: "Retry (2)", threadId: "sub.p.2" });
  });

  it("accepts a header with an empty body", () => {
    expect(parseAgentMessage("[Message from A (thread t-1)]")).toMatchObject({
      body: "",
      preview: "",
    });
  });

  it("leaves user messages alone", () => {
    expect(parseAgentMessage("yes I agree")).toBeNull();
    expect(parseAgentMessage("[Message from A (thread t-1)] trailing\n\nbody")).toBeNull();
    expect(parseAgentMessage("hello\n[Message from A (thread t-1)]\n\nbody")).toBeNull();
    expect(parseAgentMessage("[Subagent A (thread t-1) started]\n\nbody")).toBeNull();
    expect(parseAgentMessage("[Message from  (thread t-1)]\n\nbody")).toBeNull();
  });
});
