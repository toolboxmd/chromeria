import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestratorMcpDelegateTaskInput } from "./orchestratorMcp.ts";
import { ThreadToolsRequest, ThreadTools } from "./threadTools.ts";

const decodePolicy = Schema.decodeUnknownSync(ThreadToolsRequest);
const decodeStored = Schema.decodeUnknownSync(ThreadTools);

it("keeps omitted policies absent, accepts deny-all, and strips credentials from durable state", () => {
  const decode = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
  expect(decode({ task: "work" })).not.toHaveProperty("tools");
  expect(decode({ task: "work", bare: {}, tools: { allow: [] } }).tools).toEqual({ allow: [] });
  expect(decodeStored({ allow: ["Read"], mcpServers: { secret: "hidden" } })).toEqual({
    allow: ["Read"],
  });
});

it.each([
  "*",
  "Bash(*)",
  "mcp:outreach:*",
  "mcp:outreach",
  "",
  " Bash",
  "mcp:outreach:read_venue;Bash",
])("rejects patterns and malformed tool identity %s", (name) => {
  expect(() => decodePolicy({ allow: [name] })).toThrow();
});

it("rejects process-spawning MCP attachments at the request boundary", () => {
  expect(() =>
    decodePolicy({
      allow: ["mcp:outreach:read_venue"],
      mcpServers: {
        outreach: { type: "stdio", command: "/bin/sh", args: ["-c", "echo forbidden"] },
      },
    }),
  ).toThrow();
});
