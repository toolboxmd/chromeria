import * as Context from "effect/Context";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import { DeviceToolkit } from "./toolkits/device/tools.ts";
import { IssuesToolkit } from "./toolkits/issues/tools.ts";
import { PreviewToolkit } from "./toolkits/preview/tools.ts";
import { PullRequestsToolkit } from "./toolkits/pullRequests/tools.ts";
import { ThreadsToolkit } from "./toolkits/threads/tools.ts";

const deferredTools = [DeviceToolkit, IssuesToolkit, PreviewToolkit, PullRequestsToolkit].flatMap(
  (toolkit) => Object.values(toolkit.tools) as ReadonlyArray<Tool.Any>,
);
const threadTools = Object.values(ThreadsToolkit.tools) as ReadonlyArray<Tool.Any>;
const allTools = [...deferredTools, ...threadTools];

const alwaysLoaded = (tool: Tool.Any) =>
  Context.getOrUndefined(tool.annotations, Tool.Meta)?.["anthropic/alwaysLoad"] === true;

describe("t3-code tool loading", () => {
  it("makes parent delegation and blocked-child response tools immediately available", () => {
    // Spectrum needs visible barriers, and a blocked child needs a response path
    // without first requiring the parent to discover another tool.
    for (const name of [
      "spawn_thread",
      "read_thread",
      "message_thread",
      "start_spectrum",
      "pending_request_respond",
    ]) {
      const tool = threadTools.find((candidate) => candidate.name === name);
      expect(tool, name).toBeDefined();
      expect(tool && alwaysLoaded(tool), name).toBe(true);
    }
  });

  it("keeps tools outside thread delegation in deferred tool search", () => {
    expect(deferredTools.filter(alwaysLoaded).map((tool) => tool.name)).toEqual([]);
  });

  it("spawn_thread says when to use it, names the shell alternative, and stays short", () => {
    const tool = allTools.find((candidate) => candidate.name === "spawn_thread");
    const description = tool ? (Tool.getDescription(tool) ?? "") : "";
    expect(description.startsWith("Use when")).toBe(true);
    expect(description).toMatch(/in the shell/);
    expect(description.length).toBeLessThan(600);
  });
});
