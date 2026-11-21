import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkThreadTarget } from "./backgroundWorkTarget";

describe("backgroundWorkThreadTarget", () => {
  const parent = EnvironmentId.make("parent");
  const remote = EnvironmentId.make("remote");
  const threadId = ThreadId.make("child");
  const noLink = () => undefined;
  const link = (id: EnvironmentId) => (id === remote ? "Remote box" : undefined);

  it("opens a local child here and has nothing to open without a thread", () => {
    const local = { childThreadId: threadId, childEnvironmentId: undefined };
    expect(backgroundWorkThreadTarget(local, parent, () => undefined, noLink)).toEqual({
      environmentId: parent,
      threadId,
    });
    expect(
      backgroundWorkThreadTarget(
        { childThreadId: undefined, childEnvironmentId: undefined },
        parent,
        () => undefined,
        noLink,
      ),
    ).toBeNull();
  });

  it("opens a remote child in its own environment when connected, else read-only through the link", () => {
    const item = { childThreadId: threadId, childEnvironmentId: remote };
    expect(backgroundWorkThreadTarget(item, parent, () => ({ connected: true }), link)).toEqual({
      environmentId: remote,
      threadId,
    });
    expect(backgroundWorkThreadTarget(item, parent, () => undefined, link)).toEqual({
      environmentId: remote,
      threadId,
      linked: { viaEnvironmentId: parent, label: "Remote box" },
    });
  });
});
