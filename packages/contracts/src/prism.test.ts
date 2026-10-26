import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { OrchestratorMcpDelegateTaskInput, OrchestrationV2RemoteTaskLaunch } from "./index.ts";
import {
  admitDelegateRole,
  DEFAULT_PRISM_ROLE_KITS,
  PRISM_DELEGATE_ROLE_ERROR,
  PRISM_DELEGATE_ROLES,
  PrismRole,
  PrismRoleKits,
  PrismRoleKitsPatch,
  StoredPrismRole,
} from "./prism.ts";
import { ProjectSettingsOverrides, ServerSettings } from "./settings.ts";

const encodeServerSettings = Schema.encodeSync(ServerSettings);
const decodeServerSettings = Schema.decodeSync(ServerSettings);
const decodeProjectOverrides = Schema.decodeSync(ProjectSettingsOverrides);
const decodeKitsPatch = Schema.decodeSync(PrismRoleKitsPatch);
const decodeUnknownKitsPatch = Schema.decodeUnknownSync(PrismRoleKitsPatch);
const encodeKits = Schema.encodeSync(PrismRoleKits);
const decodeUnknownServerSettings = Schema.decodeUnknownSync(ServerSettings);
const decodeStored = Schema.decodeUnknownSync(StoredPrismRole);
const decodeRole = Schema.decodeUnknownSync(PrismRole);
const decodeRemoteLaunch = Schema.decodeUnknownSync(OrchestrationV2RemoteTaskLaunch);
const decodeDelegate = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);

describe("Prism role kits", () => {
  it("enables Chromeria reset recovery by default and preserves a saved opt-out through patches", () => {
    expect(decodeServerSettings({}).autoResumeLimitedThreads).toBe(true);
    const saved = decodeServerSettings({ autoResumeLimitedThreads: false });
    expect(saved.autoResumeLimitedThreads).toBe(false);
    const updated = decodeServerSettings({
      ...encodeServerSettings(saved),
      prismRoles: { planner: { instructions: "Plan." } },
    });
    expect(updated.autoResumeLimitedThreads).toBe(false);
  });
  it("keeps other roles without preferred models", () => {
    for (const [role, kit] of Object.entries(DEFAULT_PRISM_ROLE_KITS)) {
      if (role === "computer-use") continue;
      if ("models" in kit) expect(kit.models).toEqual([]);
    }
    expect(DEFAULT_PRISM_ROLE_KITS.promachos.models).toEqual([]);
    expect(DEFAULT_PRISM_ROLE_KITS.worker.lanes).toEqual({
      easy: [],
      medium: [],
      hard: [],
      ui: [{ instanceId: "claudeAgent", model: "claude-opus-5-5", effort: "medium" }],
    });
  });

  it("defaults computer use to Claude Haiku in new and older saved settings", () => {
    const models = [{ instanceId: "claudeAgent", model: "claude-haiku-5-5" }];
    expect(DEFAULT_PRISM_ROLE_KITS["computer-use"].models).toEqual(models);
    const old = decodeServerSettings({ prismRoles: { planner: { instructions: "Lead." } } });
    expect(old.prismRoles["computer-use"].models).toEqual(models);
    expect(decodeProjectOverrides({ prismRoles: { planner: { models: [] } } }).prismRoles).toEqual({
      planner: { models: [] },
    });
    expect(
      decodeServerSettings({ prismRoles: { "computer-use": { models: [] } } }).prismRoles[
        "computer-use"
      ].models,
    ).toEqual([]);
    expect(decodeKitsPatch({ "computer-use": { models: [] } })).toEqual({
      "computer-use": { models: [] },
    });
  });

  it("preserves the Promachos preference order and effort through saved settings", () => {
    const models = [
      { instanceId: "codex", model: "gpt-6.1-sol", effort: "high" },
      { instanceId: "claudeAgent", model: "claude-opus-5-5" },
    ];
    const settings = decodeServerSettings({ prismRoles: { promachos: { models } } });
    expect(encodeKits(settings.prismRoles).promachos).toMatchObject({ models });
    expect(decodeKitsPatch({ promachos: { models } })).toEqual({ promachos: { models } });
    expect(
      decodeProjectOverrides({ prismRoles: { promachos: { models } } }).prismRoles?.promachos
        ?.models,
    ).toEqual(models);
  });

  it("loads older settings without a Promachos kit and permits clearing its preferences", () => {
    const old = decodeServerSettings({ prismRoles: { planner: { instructions: "Lead." } } });
    expect(old.prismRoles.promachos.models).toEqual([]);
    expect(old.prismRoles.planner.instructions).toBe("Lead.");
    expect(decodeKitsPatch({ promachos: { models: [] } })).toEqual({ promachos: { models: [] } });
  });

  it("ship no role instructions or skills by default", () => {
    for (const kit of Object.values(DEFAULT_PRISM_ROLE_KITS)) {
      expect(kit.instructions).toBe("");
      expect(kit.skills).toEqual([]);
    }
  });

  it("load saved settings that still carry a thread-tool scope, dropping it", () => {
    const settings = decodeUnknownServerSettings({
      prismRoles: {
        planner: { instructions: "Lead.", threadTools: "children" },
        worker: { threadTools: "none", lanes: { easy: [] } },
        reviewer: { threadTools: "none" },
      },
    });
    expect(settings.prismRoles.planner.instructions).toBe("Lead.");
    const encoded = encodeKits(settings.prismRoles) as Record<string, object>;
    for (const role of ["planner", "worker", "reviewer"] as const) {
      expect("threadTools" in settings.prismRoles[role]).toBe(false);
      expect("threadTools" in encoded[role]!).toBe(false);
    }
  });

  it("drop removed role kits from environment settings and project overrides, keeping the rest", () => {
    const opus = { instanceId: "claudeAgent", model: "claude-opus-5-5", effort: "high" };
    const luna = { instanceId: "codex", model: "gpt-6-luna" };
    const grok = { instanceId: "grok", model: "grok-4.7" };
    const removed = {
      dispatcher: { models: [opus], instructions: "Lead." },
      retry: { models: [luna], enabled: false },
      escalation: { models: [opus] },
      correction: { models: [luna] },
      recovery: { enabled: false },
    };
    const kept = {
      planner: { models: [opus] },
      reviewer: { models: [luna, grok] },
      promachos: { models: [grok] },
      worker: { lanes: { easy: [luna], hard: [opus] } },
    };
    const settings = decodeServerSettings({ prismRoles: { ...removed, ...kept } });
    const encoded = encodeKits(settings.prismRoles) as Record<string, unknown>;
    for (const role of Object.keys(removed)) expect(encoded).not.toHaveProperty(role);
    expect(settings.prismRoles.planner.models).toEqual([opus]);
    expect(settings.prismRoles.reviewer.models).toEqual([luna, grok]);
    expect(settings.prismRoles.promachos.models).toEqual([grok]);
    expect(settings.prismRoles.worker.lanes).toMatchObject({
      easy: [luna],
      medium: [],
      hard: [opus],
    });

    // A project override keeps only the kept roles it set; an empty list stays a real value.
    const overrides = decodeProjectOverrides({
      prismRoles: { ...removed, reviewer: { models: [] }, worker: { lanes: { hard: [opus] } } },
    });
    expect(overrides.prismRoles).toEqual({
      reviewer: { models: [] },
      worker: { lanes: { hard: [opus] } },
    });
    expect(decodeProjectOverrides({ prismRoles: removed }).prismRoles).toEqual({});
  });

  it("read removed role names in stored records as the role that now does that work", () => {
    expect(decodeStored("dispatcher")).toBe("planner");
    expect(decodeStored("retry")).toBe("worker");
    expect(decodeStored("escalation")).toBe("worker");
    expect(decodeStored("researcher")).toBe("researcher");
    expect(() => decodeStored("nonsense")).toThrow();
    const launch = decodeRemoteLaunch({
      requestNamespace: "ns",
      clientRequestId: "id",
      linkIdentity: "link",
      projectId: "project",
      title: "Old launch",
      prismRole: "escalation",
      delegatedFrom: { environmentId: "env", threadId: "thread", title: "Parent" },
    });
    expect(launch.prismRole).toBe("worker");
    expect(() => decodeRole("dispatcher")).toThrow();
  });

  it("accept the delegated roles and reject Promachos and unknown names with the list", () => {
    for (const prismRole of PRISM_DELEGATE_ROLES) {
      expect(decodeDelegate({ task: "Do it.", prismRole }).prismRole).toBe(prismRole);
    }
    for (const prismRole of ["promachos", "nonsense"]) {
      expect(() => decodeDelegate({ task: "Do it.", prismRole })).toThrow(
        PRISM_DELEGATE_ROLE_ERROR,
      );
    }
    for (const role of PRISM_DELEGATE_ROLES) {
      expect(PRISM_DELEGATE_ROLE_ERROR).toContain(role);
    }
  });

  it("admit a removed delegated role only when replaying a request an older build accepted", () => {
    for (const [removed, successor] of [
      ["dispatcher", "planner"],
      ["retry", "worker"],
      ["escalation", "worker"],
    ] as const) {
      const prismRole = decodeDelegate({ task: "Do it.", prismRole: removed }).prismRole!;
      expect(admitDelegateRole(prismRole, false)).toBeUndefined();
      expect(admitDelegateRole(prismRole, true)).toBe(successor);
    }
    expect(admitDelegateRole("judge", false)).toBe("judge");
  });

  it("load saved settings that still name a role runtime mode, dropping it", () => {
    const settings = decodeServerSettings({
      prismRoles: { reviewer: { instructions: "Read.", runtimeMode: "approval-required" } },
    });
    expect(settings.prismRoles.reviewer.instructions).toBe("Read.");
    expect("runtimeMode" in settings.prismRoles.reviewer).toBe(false);
  });

  it("keep a saved per-lane list as the single list of a role other than the worker", () => {
    const opus = { instanceId: "claudeAgent", model: "claude-opus-5-5", effort: "high" };
    const luna = { instanceId: "codex", model: "gpt-5.6-luna" };
    const settings = decodeServerSettings({
      prismRoles: {
        planner: { instructions: "Lead.", lanes: { easy: [luna], medium: [opus], hard: [] } },
        reviewer: { lanes: { hard: [opus] } },
        judge: { models: [luna], lanes: { medium: [opus] } },
      },
    });
    expect(settings.prismRoles.planner.models).toEqual([opus]);
    expect(settings.prismRoles.planner.instructions).toBe("Lead.");
    expect("lanes" in settings.prismRoles.planner).toBe(false);
    expect(settings.prismRoles.reviewer.models).toEqual([]);
    expect(settings.prismRoles.judge.models).toEqual([luna]);
    expect(encodeKits(settings.prismRoles).planner).toEqual({
      instructions: "Lead.",
      skills: [],
      models: [opus],
    });
  });

  it("fill missing roles and fields when a settings file names one role", () => {
    const settings = decodeServerSettings({
      prismRoles: {
        worker: {
          lanes: {
            medium: [
              { instanceId: "claudeAgent", model: "claude-opus-5-5", effort: "medium" },
              { instanceId: "opencode", model: "opencode/muse", effort: "medium" },
            ],
            hard: [{ instanceId: "claudeAgent", model: "claude-opus-5-5", effort: "xhigh" }],
          },
        },
      },
    });
    expect(settings.prismRoles.worker.lanes.medium.map((entry) => entry.model)).toEqual([
      "claude-opus-5-5",
      "opencode/muse",
    ]);
    expect(settings.prismRoles.worker.lanes.hard[0]?.effort).toBe("xhigh");
    expect(settings.prismRoles.worker.lanes.easy).toEqual([]);
    expect(settings.prismRoles.roast.models).toEqual([]);
    expect(settings.prismRoles.reviewer.instructions).toBe("");
  });

  it("keep a project override partial: only the roles it set, an empty list kept", () => {
    const luna = { instanceId: "codex", model: "gpt-5.6-luna" };
    const overrides = decodeProjectOverrides({
      prismRoles: { reviewer: { models: [luna] }, planner: { models: [] } },
    });
    expect(overrides.prismRoles).toEqual({ reviewer: { models: [luna] }, planner: { models: [] } });
  });

  it("keep a project override's legacy medium lane as the role's list, without defaults", () => {
    const opus = { instanceId: "claudeAgent", model: "claude-opus-5-5" };
    const luna = { instanceId: "codex", model: "gpt-5.6-luna" };
    expect(
      decodeProjectOverrides({
        prismRoles: {
          reviewer: { lanes: { medium: [opus], hard: [luna] } },
          judge: { models: [luna], lanes: { medium: [opus] } },
          worker: { lanes: { medium: [luna] } },
        },
      }).prismRoles,
    ).toEqual({
      reviewer: { models: [opus] },
      judge: { models: [luna] },
      worker: { lanes: { medium: [luna] } },
    });
  });

  it("patch one lane of the worker or one field of another role without defaults", () => {
    expect(decodeKitsPatch({ worker: { lanes: { hard: [] } } })).toEqual({
      worker: { lanes: { hard: [] } },
    });
    expect(decodeKitsPatch({ roast: { instructions: "Attack." } })).toEqual({
      roast: { instructions: "Attack." },
    });
  });

  it("run read-only roles once by default and accept a Run above 1 only for them", () => {
    const kits = decodeServerSettings({}).prismRoles;
    for (const role of ["researcher", "designer", "roast", "reviewer", "judge"] as const)
      expect(kits[role].run).toBe(1);
    expect(decodeKitsPatch({ roast: { run: 2 } })).toEqual({ roast: { run: 2 } });
    expect(() => decodeKitsPatch({ roast: { run: 0 } })).toThrow();
    expect(() => decodeUnknownKitsPatch({ worker: { run: 2 } })).toThrow(
      "Run above 1 is only for read-only roles.",
    );
    expect(() => decodeUnknownKitsPatch({ planner: { run: 2 } })).toThrow(
      "Run above 1 is only for read-only roles.",
    );
    expect(decodeKitsPatch({ planner: { run: 1 } })).toEqual({ planner: { run: 1 } });
  });
});
