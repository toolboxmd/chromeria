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

it("rejects a read-only sandbox rather than overriding its admitted native mode", () => {
  expect(() =>
    makeClaudeQueryOptions({
      ...input,
      permissionMode: "default",
      tools: ["Read", "Glob", "Grep"],
      toolPolicy: { allow: ["Bash", "Read"] },
    }),
  ).toThrow(/full-access/);
});

it.each(["default", "acceptEdits", "auto", "plan", "dontAsk"] as const)(
  "rejects an allowlist under native mode %s instead of attaching unusable tools",
  (permissionMode) => {
    expect(() =>
      makeClaudeQueryOptions({
        ...input,
        permissionMode,
        toolPolicy: { allow: ["mcp:outreach:read_venue"] },
        threadMcpServers: { outreach: { type: "http", url: "http://localhost/outreach" } },
      }),
    ).toThrow(/full-access/);
  },
);
