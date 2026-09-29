import * as Context from "effect/Context";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { DeviceToolkit } from "./toolkits/device/tools.ts";
import { IssuesToolkit } from "./toolkits/issues/tools.ts";
import { PreviewToolkit } from "./toolkits/preview/tools.ts";
import { PrismToolkit } from "./toolkits/prism/tools.ts";
import { PullRequestsToolkit } from "./toolkits/pullRequests/tools.ts";
import { ThreadsToolkit } from "./toolkits/threads/tools.ts";

const DELEGATION_TOOLS = ["spawn_thread", "read_thread", "message_thread", "prism_submit"];

const allTools = [
  DeviceToolkit,
  IssuesToolkit,
  PreviewToolkit,
  PrismToolkit,
  PullRequestsToolkit,
  ThreadsToolkit,
].flatMap((toolkit) => Object.values(toolkit.tools) as ReadonlyArray<Tool.Any>);

const alwaysLoaded = (tool: Tool.Any) =>
  Context.getOrUndefined(tool.annotations, Tool.Meta)?.["anthropic/alwaysLoad"] === true;

describe("t3-code tool loading", () => {
  it("keeps exactly the delegation tools out of Claude Code's deferred tool search", () => {
    const loaded = allTools.filter(alwaysLoaded).map((tool) => tool.name);
    expect([...new Set(loaded)].toSorted()).toEqual([...DELEGATION_TOOLS].toSorted());
  });

  it.each(["spawn_thread", "prism_submit"])(
    "%s says when to use it, names the shell alternative, and stays short",
    (name) => {
      const tool = allTools.find((candidate) => candidate.name === name);
      const description = tool ? (Tool.getDescription(tool) ?? "") : "";
      expect(description.startsWith("Use when")).toBe(true);
      expect(description).toMatch(/in the shell/);
      expect(description.length).toBeLessThan(600);
    },
  );
});
