// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { DEFAULT_PRISM_ROLE_KITS, type ServerProvider, ThreadId } from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import { describe, expect, it } from "vite-plus/test";

import {
  isProviderBlocked,
  pickRoleModel,
  prismRoleSuffix,
  roleTaskMessage,
  threadRoleOf,
} from "./roles.ts";
import {
  callTool,
  createParent,
  dispatchAll,
  PARENT_ID,
  session,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";
import { makeSubagentThreadId, parentThreadIdOf } from "./subagentThreadId.ts";
import { SpawnThreadInput, ThreadsToolkit } from "./tools.ts";

const decodeSpawnInput = Schema.decodeUnknownSync(SpawnThreadInput);
const child = (role: string) => makeSubagentThreadId("planner-1", `${role}-abc123`);

describe("thread roles", () => {
  it("treats a thread the user started as the planner", () => {
    expect(threadRoleOf("planner-1")).toBe("planner");
  });

  it("reads a role spawned child's role from its id", () => {
    expect(threadRoleOf(makeSubagentThreadId("planner-1", prismRoleSuffix("reviewer", "a1")))).toBe(
      "reviewer",
    );
    expect(threadRoleOf(child("dispatcher"))).toBe("dispatcher");
    expect(threadRoleOf(makeSubagentThreadId(child("dispatcher"), "worker-9f"))).toBe("worker");
  });

  it("spawns Retry and Escalation under their names and reads legacy ids as them", () => {
    const spawnedRole = (role: string) => {
      const input = decodeSpawnInput({ task: "Fix it.", role });
      return threadRoleOf(makeSubagentThreadId("planner-1", prismRoleSuffix(input.role!, "a1")));
    };
    expect(spawnedRole("retry")).toBe("retry");
    expect(spawnedRole("escalation")).toBe("escalation");
    expect(() => decodeSpawnInput({ task: "Fix it.", role: "correction" })).toThrow();
    expect(threadRoleOf(child("correction"))).toBe("retry");
    expect(threadRoleOf(child("recovery"))).toBe("escalation");
  });

  it("leaves children without a role prefix unassigned", () => {
    expect(threadRoleOf(makeSubagentThreadId("planner-1", "0123456789ab"))).toBe("unassigned");
    expect(threadRoleOf(makeSubagentThreadId("planner-1", "tester-1"))).toBe("unassigned");
  });
});

describe("thread tools per role", () => {
  effectIt.effect("lets a thread in every role use every thread tool", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-thread-roles-");
      const reached = yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          const reached: Record<string, unknown> = {};
          for (const role of ["dispatcher", "worker", "reviewer", "retry", "escalation"] as const) {
            const spawned = yield* callTool("spawn_thread", {
              task: `Act as ${role}.`,
              role,
              reportBack: false,
            });
            const caller = spawned.threadId;
            expect(threadRoleOf(caller)).toBe(role);
            const grandchild = yield* callTool(
              "spawn_thread",
              { task: "Check it.", reportBack: false },
              caller,
            );
            yield* dispatchAll([session(ThreadId.make(grandchild.threadId), "ready", null)]);
            const children = yield* callTool("list_child_threads", {} as never, caller);
            const read = yield* callTool(
              "read_thread",
              { threadId: grandchild.threadId, scope: "children" },
              caller,
            );
            const message = yield* callTool(
              "message_thread",
              { threadId: grandchild.threadId, text: "Continue.", scope: "children" },
              caller,
            );
            const peers = yield* callTool(
              "list_threads",
              { scope: "project", includeSettled: false },
              caller,
            );
            reached[role] = {
              spawnedUnder: parentThreadIdOf(grandchild.threadId),
              children: children.threads.map((thread) => thread.threadId),
              read: read.threadId,
              delivery: message.delivery,
              seesPlanner: peers.threads.some((thread) => thread.threadId === PARENT_ID),
            };
            expect(reached[role]).toEqual({
              spawnedUnder: caller,
              children: [grandchild.threadId],
              read: grandchild.threadId,
              delivery: "new-turn",
              seesPlanner: true,
            });
          }
          return reached;
        }),
      );
      expect(Object.keys(reached)).toHaveLength(5);
    }).pipe(Effect.scoped),
  );
});

const provider = (overrides: Partial<ServerProvider> = {}): ServerProvider =>
  ({
    instanceId: "opencode",
    driver: "opencode",
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-09-25T00:00:00.000Z",
    models: [{ slug: "opencode/muse", name: "Muse", isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
    ...overrides,
  }) as ServerProvider;

const window = (usedPercent: number, resetsAt?: string) => ({
  id: "five_hour",
  kind: "session" as const,
  label: "5h",
  usedPercent,
  ...(resetsAt ? { resetsAt } : {}),
});

describe("role model eligibility", () => {
  const now = Date.parse("2026-09-25T12:00:00.000Z");
  const muse = { instanceId: "opencode", model: "opencode/muse", effort: "high" } as never;
  const luna = { instanceId: "codex", model: "gpt-5.6-luna" } as never;

  it("picks the first preference enabled in Providers", () => {
    expect(pickRoleModel([luna, muse], [provider()], now)).toEqual({ pick: muse });
  });

  it("skips a disabled instance and a model the instance does not offer", () => {
    const result = pickRoleModel(
      [muse, { instanceId: "opencode", model: "opencode/other" } as never],
      [provider({ enabled: false })],
      now,
    );
    expect(result).toEqual({
      refusal:
        "No eligible model for this role and lane: opencode/opencode/muse (instance disabled), opencode/opencode/other (instance disabled).",
    });
    expect(
      pickRoleModel([{ instanceId: "opencode", model: "x" } as never], [provider()], now),
    ).toEqual({
      refusal: "No eligible model for this role and lane: opencode/x (model not offered).",
    });
  });

  it("blocks an instance at 100 % until its window resets", () => {
    const exhausted = provider({
      usageLimits: {
        checkedAt: "2026-09-25T11:59:00.000Z",
        windows: [window(100, "2026-09-25T13:00:00.000Z")],
      },
    });
    expect(isProviderBlocked(exhausted, now)).toBe(true);
    expect(isProviderBlocked(exhausted, Date.parse("2026-09-25T13:00:01.000Z"))).toBe(false);
    expect("refusal" in pickRoleModel([muse], [exhausted], now)).toBe(true);
  });

  it("falls back down a lane list, with one model at two efforts as separate entries", () => {
    const opusMedium = {
      instanceId: "claudeAgent",
      model: "claude-opus-5-5",
      effort: "medium",
    } as never;
    const opusXhigh = { ...(opusMedium as object), effort: "xhigh" } as never;
    const claude = provider({
      instanceId: "claudeAgent" as never,
      driver: "claudeAgent" as never,
      models: [{ slug: "claude-opus-5-5", name: "Opus", isCustom: false, capabilities: null }],
      usageLimits: {
        checkedAt: "2026-09-25T11:59:00.000Z",
        windows: [window(100, "2026-09-25T13:00:00.000Z")],
      },
    });
    expect(pickRoleModel([opusXhigh, opusMedium], [claude], now)).toEqual({
      refusal:
        "No eligible model for this role and lane: claudeAgent/claude-opus-5-5 (usage limit reached), claudeAgent/claude-opus-5-5 (usage limit reached).",
    });
    expect(pickRoleModel([opusXhigh, muse], [claude, provider()], now)).toEqual({ pick: muse });
    expect(
      pickRoleModel([opusXhigh, opusMedium], [{ ...claude, usageLimits: undefined }], now),
    ).toEqual({ pick: opusXhigh });
  });

  it("blocks a full window without resetsAt until the next reading, not below 100 %", () => {
    const noReset = provider({
      usageLimits: { checkedAt: "2026-09-25T11:59:00.000Z", windows: [window(100)] },
    });
    expect(isProviderBlocked(noReset, now)).toBe(true);
    const busy = provider({
      usageLimits: { checkedAt: "2026-09-25T11:59:00.000Z", windows: [window(85)] },
    });
    expect(isProviderBlocked(busy, now)).toBe(false);
  });
});

describe("role task message", () => {
  it("puts the kit's instructions and skills before the task", () => {
    const kit = {
      ...DEFAULT_PRISM_ROLE_KITS.reviewer,
      instructions: "Review only; do not edit.",
      skills: ["code-review"],
    };
    expect(roleTaskMessage(kit, "Review PR 12.")).toBe(
      "Review only; do not edit.\n\nUse these skills: code-review.\n\nReview PR 12.",
    );
  });

  it("sends the bare task for an empty kit", () => {
    expect(roleTaskMessage(DEFAULT_PRISM_ROLE_KITS.worker, "Fix it.")).toBe("Fix it.");
  });
});

describe("spawn_thread runtime mode", () => {
  it("exposes explicit runtime mode while leaving the default to the handler", () => {
    const schema = JSON.stringify(Tool.getJsonSchema(ThreadsToolkit.tools.spawn_thread));
    expect(schema).toContain("runtimeMode");
    expect(decodeSpawnInput({ task: "x", runtimeMode: "approval-required" })).toHaveProperty(
      "runtimeMode",
      "approval-required",
    );
    expect(decodeSpawnInput({ task: "x" })).not.toHaveProperty("runtimeMode");
  });
});
