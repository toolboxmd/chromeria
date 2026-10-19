import { expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import { makeClaudeQueryOptions } from "./ClaudeAdapterV2.ts";

const input = {
  modelSelection: {
    instanceId: ProviderInstanceId.make("claudeAgent"),
    model: "claude-sonnet-4-6",
  },
  nativeThreadId: "allowlist-test",
  resume: false,
  cwd: "/workspace",
  permissionMode: "bypassPermissions" as const,
  allowedTools: ["mcp__t3-code__*"],
  mcpServers: { "t3-code": { type: "http" as const, url: "http://localhost/mcp" } },
};

it("enforces exactly two MCP tools even under full access and bare mode", () => {
  const options = makeClaudeQueryOptions({
    ...input,
    bare: { additions: [] },
    toolPolicy: { allow: ["mcp:outreach:read_venue", "mcp:outreach:submit_result"] },
    threadMcpServers: { outreach: { type: "http", url: "http://localhost/outreach" } },
  });
  expect(options.tools).toEqual([]);
  expect(options.permissionMode).toBe("dontAsk");
  expect(options.allowedTools).toEqual([
    "mcp__outreach__read_venue",
    "mcp__outreach__submit_result",
  ]);
  expect(Object.keys(options.mcpServers ?? {})).toEqual(["outreach"]);
  expect(options.allowDangerouslySkipPermissions).not.toBe(true);
});

it("permits only explicitly listed built-ins and treats an empty list as no tools", () => {
  expect(makeClaudeQueryOptions({ ...input, toolPolicy: { allow: ["Read"] } }).tools).toEqual([
    "Read",
  ]);
  expect(makeClaudeQueryOptions({ ...input, toolPolicy: { allow: [] } }).tools).toEqual([]);
});

it("keeps full-access defaults unchanged when the option is absent", () => {
  const options = makeClaudeQueryOptions(input);
  expect(options.permissionMode).toBe("bypassPermissions");
  expect(options.allowedTools).toEqual(["mcp__t3-code__*"]);
});

it("does not raise a read-only sandbox's native tool inventory", () => {
  const options = makeClaudeQueryOptions({
    ...input,
    permissionMode: "default",
    tools: ["Read", "Glob", "Grep"],
    toolPolicy: { allow: ["Bash", "Read", "Write"] },
  });
  expect(options.tools).toEqual(["Read"]);
  expect(options.allowedTools).not.toContain("Bash");
});
