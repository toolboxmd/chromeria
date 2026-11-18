import { Agent, type OpenCodeClient, type Permission } from "@opencode/client/effect";
import type { BareEvidence, RuntimeMode, ThreadBare } from "@t3tools/contracts";
import {
  bareInventoryEvidence,
  bareReceipt,
  namesT3Mcp,
} from "@t3tools/provider-core/server/bareLaunch";
import * as Effect from "effect/Effect";

import { OPENCODE_PROVIDER } from "../adapter.ts";
import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";

/** OpenCode 2 has no `--pure`: the private server's config strips prompts, instructions and plugins. */
export const OPEN_CODE_2_BARE_CONFIG = {
  instructions: [],
  plugins: [],
  mcp: {},
  policies: [{ action: "skill.use", resource: "*", effect: "deny" }],
  agents: {
    build: { system: "." },
    plan: { system: "." },
    title: { disabled: true },
  },
};

const NATIVE_ACTIONS = ["shell", "read", "edit", "glob", "grep", "external_directory"];
// `write` and `opencode_worktree_remove` ask the edit permission, so edit keeps them listed.
const NATIVE_TOOLS = ["shell", "read", "write", "edit", "glob", "grep", "opencode_worktree_remove"];

const allow = (action: string, resource = "*"): Permission.Rule => ({
  action,
  resource,
  effect: "allow",
});

/**
 * OpenCode 2 lists a tool unless the last rule matching its permission denies "*",
 * and appends agent defaults after config, so only session rules can hide `skill`.
 */
export const sessionRules = (
  rules: ReadonlyArray<Permission.Rule>,
  bare: ThreadBare,
  mcpServerName: string | null,
): ReadonlyArray<Permission.Rule> => {
  const native = [
    ...NATIVE_ACTIONS,
    ...(mcpServerName !== null && namesT3Mcp(bare) ? [`${mcpServerName}_*`] : []),
  ];
  return [
    { action: "*", resource: "*", effect: "deny" },
    allow("read"),
    allow("glob"),
    allow("grep"),
    // OpenCode's own defaults, which the leading deny would otherwise drop.
    { action: "read", resource: "*.env", effect: "ask" },
    { action: "read", resource: "*.env.*", effect: "ask" },
    allow("read", "*.env.example"),
    ...rules.flatMap((rule) =>
      rule.action === "*"
        ? native.map((action) => ({ ...rule, action }))
        : native.includes(rule.action)
          ? [rule]
          : [],
    ),
  ];
};

/** Reads what the private server would load before any inference; extra context fails the open. */
export const inspect = Effect.fn("OpenCode2Bare.inspect")(function* (
  client: OpenCodeClient,
  directory: string,
) {
  const location = { directory };
  const { agents, plugins, mcp, skills } = yield* Effect.all(
    {
      agents: Effect.forEach(["build", "plan"], (id) =>
        client.agent.get({ agentID: Agent.ID.make(id), location }),
      ),
      plugins: client.plugin.list({ location }),
      mcp: client.mcp.list({ location }),
      skills: client.skill.list({ location }),
    },
    { concurrency: "unbounded" },
  ).pipe(
    Effect.mapError(
      (cause) =>
        new OpenCodeRuntime.OpenCodeRuntimeError({
          operation: "bare.inspect",
          detail: "Could not read the bare OpenCode server's inventory.",
          cause,
        }),
    ),
  );
  // T3 Code's per-thread servers stay hidden by each session's rules.
  const servers = mcp.data
    .map((server) => server.name)
    .filter((name) => !name.startsWith("t3-code-"));
  const evidence: ReadonlyArray<BareEvidence> = [
    bareInventoryEvidence(
      "system_text",
      agents.map((agent) => `${agent.data.id}: ${JSON.stringify(agent.data.system ?? null)}`),
      ['build: "."', 'plan: "."'],
      "OpenCode agent.get before inference",
      true,
    ),
    bareInventoryEvidence(
      "plugins",
      plugins.data.flatMap(({ id, source }) =>
        source.type === "builtin"
          ? []
          : [
              id ??
                (source.type === "package"
                  ? source.target
                  : source.type === "local"
                    ? source.path
                    : source.type),
            ],
      ),
      [],
      "OpenCode plugin.list before inference; built-in plugins are part of the harness",
    ),
    bareInventoryEvidence("mcp", servers, [], "OpenCode mcp.list before inference"),
    bareInventoryEvidence(
      "skills",
      skills.data.map((skill) => skill.id),
      [],
      "OpenCode skill.list before inference",
    ),
  ];
  const unexpected = evidence.filter((entry) => entry.status === "observed_unexpected");
  if (unexpected.length > 0)
    return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
      operation: "bare.inspect",
      detail: `OpenCode bare server loaded unexpected context: ${unexpected
        .map((entry) => `${entry.category} ${entry.identities.join(", ")}`)
        .join("; ")}.`,
    });
  return evidence;
});

export const receipt = (input: {
  readonly bare: ThreadBare;
  readonly mode: RuntimeMode;
  readonly agent: string;
  readonly mcpServerName: string | null;
  readonly root: string;
  readonly sessionId: string | null;
  readonly evidence: ReadonlyArray<BareEvidence>;
}) =>
  bareReceipt({
    driver: OPENCODE_PROVIDER,
    bare: input.bare,
    mode: input.mode,
    effective: `OpenCode agent ${input.agent}; ${input.mode}; native plan restrictions preserved`,
    controls: [
      "own private OpenCode 2 server",
      "shadow XDG_CONFIG_HOME",
      "project and Claude Code discovery disabled",
      "build/plan system '.'",
      "skills denied by policy",
      "session rules deny all but native tools",
      "title disabled",
    ],
    retained: [
      "system text '.'",
      "model identity, date and environment block",
      "worktree session_move hint",
      "opencode_worktree_remove under the edit permission",
    ],
    root: input.root,
    sessionId: input.sessionId,
    evidence: [
      {
        category: "tools",
        status: "unobserved",
        identities: [
          ...NATIVE_TOOLS,
          ...(input.mcpServerName !== null && namesT3Mcp(input.bare)
            ? [`${input.mcpServerName}_*`]
            : []),
        ],
        source:
          "OpenCode 2 session rules; it has no tool listing API, outbound tool request unobserved",
      },
      ...input.evidence,
    ],
  });
