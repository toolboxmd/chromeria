import { assert, it } from "@effect/vitest";
import type { OpenCodeClient, Permission } from "@opencode/client/effect";
import * as Effect from "effect/Effect";

import * as OpenCode2Bare from "./bare.ts";

const rule = (action: string, effect: Permission.Rule["effect"]): Permission.Rule => ({
  action,
  resource: "*",
  effect,
});

// OpenCode 2.3.2 lists a tool unless the last rule matching its permission is a "*" deny,
// and appends agent defaults such as `skill browser deny` before session rules.
const AGENT_DEFAULTS: ReadonlyArray<Permission.Rule> = [
  rule("*", "allow"),
  { action: "skill", resource: "browser", effect: "deny" },
];
const glob = (pattern: string) =>
  new RegExp(`^${pattern.replaceAll(/[.+?^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*")}$`);
const listed = (permission: string, session: ReadonlyArray<Permission.Rule>) => {
  const last = [...AGENT_DEFAULTS, ...session].findLast((entry) =>
    glob(entry.action).test(permission),
  );
  return !(last?.resource === "*" && last.effect === "deny");
};
// Tool name, then the permission OpenCode checks to list it.
const TOOLS = [
  ["shell", "shell"],
  ["read", "read"],
  ["write", "edit"],
  ["edit", "edit"],
  ["glob", "glob"],
  ["grep", "grep"],
  ["opencode_worktree_remove", "edit"],
  ["skill", "skill"],
  ["subagent", "subagent"],
  ["question", "question"],
  ["webfetch", "webfetch"],
  ["execute", "execute"],
  ["t3-code-mine_t3_thread_read", "t3-code-mine_t3_thread_read"],
  ["t3-code-other_t3_thread_read", "t3-code-other_t3_thread_read"],
] as const;
const visible = (session: ReadonlyArray<Permission.Rule>) =>
  TOOLS.filter(([, permission]) => listed(permission, session)).map(([name]) => name);

const NATIVE = ["shell", "read", "write", "edit", "glob", "grep", "opencode_worktree_remove"];
const t3Rules = [rule("t3-code-*", "deny"), rule("t3-code-mine_*", "allow")];

it.each([
  ["full access", [rule("*", "allow"), ...t3Rules]],
  [
    "supervised",
    [rule("shell", "ask"), rule("edit", "ask"), rule("external_directory", "ask"), ...t3Rules],
  ],
  ["plan", [rule("shell", "ask"), rule("edit", "ask"), rule("edit", "deny"), ...t3Rules]],
] as const)("bare session rules list only native tools in %s mode", (mode, rules) => {
  const unnamed = OpenCode2Bare.sessionRules(rules, { additions: [] }, "t3-code-mine");
  const named = OpenCode2Bare.sessionRules(
    rules,
    { additions: [{ type: "mcp", server: "t3-code" }] },
    "t3-code-mine",
  );
  const native =
    mode === "plan"
      ? NATIVE.filter((tool) => !["write", "edit", "opencode_worktree_remove"].includes(tool))
      : NATIVE;
  assert.deepStrictEqual(visible(unnamed), native);
  assert.deepStrictEqual(visible(named), [...native, "t3-code-mine_t3_thread_read"]);
  assert.include(visible(rules), "skill", "normal rules keep tools bare must hide");
});

const inventory = (input: {
  readonly system?: string;
  readonly plugins?: ReadonlyArray<unknown>;
  readonly mcp?: ReadonlyArray<string>;
  readonly instructions?: ReadonlyArray<string>;
}) =>
  ({
    agent: {
      get: ({ agentID }: { readonly agentID: string }) =>
        Effect.succeed({ data: { id: agentID, system: input.system ?? "." } }),
    },
    plugin: {
      list: () =>
        Effect.succeed({
          data: [{ id: "opencode.skill", source: { type: "builtin" } }, ...(input.plugins ?? [])],
        }),
    },
    mcp: { list: () => Effect.succeed({ data: (input.mcp ?? []).map((name) => ({ name })) }) },
    skill: { list: () => Effect.succeed({ data: [] }) },
    // OpenCode 2 unions `instructions` over every document, so bare's empty list clears none.
    config: {
      get: () =>
        Effect.succeed([
          { type: "document", info: { instructions: [] } },
          { type: "directory", path: "/home/.config/opencode" },
          { type: "document", info: { instructions: input.instructions ?? [] } },
        ]),
    },
  }) as unknown as OpenCodeClient;

it.effect("bare inspection accepts only built-in plugins and T3 Code MCP servers", () =>
  Effect.gen(function* () {
    const evidence = yield* OpenCode2Bare.inspect(inventory({ mcp: ["t3-code-thread"] }), "/work");
    assert.deepStrictEqual(
      evidence.map((entry) => [entry.category, entry.status]),
      [
        ["system_text", "observed_retained"],
        ["plugins", "observed_clean"],
        ["mcp", "observed_clean"],
        ["skills", "observed_clean"],
        ["instructions", "observed_clean"],
      ],
    );
    for (const unexpected of [
      inventory({ system: "You are OpenCode" }),
      inventory({ plugins: [{ source: { type: "package", target: "oh-my-opencode" } }] }),
      inventory({ mcp: ["github"] }),
      inventory({ instructions: ["/Library/Application Support/opencode/AGENTS.md"] }),
    ]) {
      const error = yield* Effect.flip(OpenCode2Bare.inspect(unexpected, "/work"));
      assert.strictEqual(error.operation, "bare.inspect");
    }
  }),
);
