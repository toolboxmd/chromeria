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

  it("prism_submit says workspace defaults to this thread's directory", () => {
    const tool = allTools.find((candidate) => candidate.name === "prism_submit");
    expect(tool ? Tool.getDescription(tool) : "").toContain(
      "workspace defaults to this thread's current directory; set it for work in another repository or worktree.",
    );
  });

  it("prism_status says how to confirm, report and fix a router defect and what is not one", () => {
    const tool = allTools.find((candidate) => candidate.name === "prism_status");
    const description = tool ? (Tool.getDescription(tool) ?? "") : "";
    expect(description.startsWith("Read a Prism job's state")).toBe(true);
    expect(description).toContain(
      "when a job blocks, loops or acts against Model Router's RUNNER.md",
    );
    expect(description).toContain("~/.local/share/durable-runner/outputs/<requestId>/");
    expect(description).toContain(
      "once a spawn_thread reviewer confirms it, open or update a toolboxmd/model-router Issue with the request id, that evidence and expected behavior, and start a fix worker.",
    );
    expect(description).toContain(
      "Fix your own packet mistakes, like a missing proof or workspace, in the packet; they are not router defects.",
    );
    expect(description.length).toBeLessThan(600);
  });
});
