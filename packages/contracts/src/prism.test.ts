import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  DEFAULT_PRISM_ROLE_KITS,
  PrismRoleKits,
  PrismRoleKitsPatch,
  PrismLiveness,
} from "./prism.ts";
import { ProjectSettingsOverrides, ServerSettings } from "./settings.ts";

const decodeServerSettings = Schema.decodeSync(ServerSettings);
const decodeProjectOverrides = Schema.decodeSync(ProjectSettingsOverrides);
const decodeKitsPatch = Schema.decodeSync(PrismRoleKitsPatch);
const encodeKits = Schema.encodeSync(PrismRoleKits);

const decodeLiveness = Schema.decodeUnknownSync(PrismLiveness);
const encodeLiveness = Schema.encodeSync(PrismLiveness);

describe("Prism liveness", () => {
  const live = {
    threadId: "sub.parent.child",
    now: "2026-09-28T12:00:00.000Z",
    lastStreamAt: "2026-09-28T11:58:00.000Z",
    silenceMs: 120_000,
    openTool: null,
    turn: {
      turnId: "turn-1",
      provider: "opencode",
      model: "muse",
      startedAt: "2026-09-28T11:57:00.000Z",
      eventCount: 42,
    },
    stale: true,
    staleSince: "2026-09-28T11:59:53.000Z",
    thresholdMs: 90_000,
    thresholdSource: "default",
    reason: "silence",
  };

  it("round-trips the stale decision alongside the existing clock fields", () => {
    const decoded = decodeLiveness(live);
    expect(encodeLiveness(decoded)).toEqual(live);
    expect(
      decodeLiveness({
        ...live,
        stale: false,
        staleSince: null,
        thresholdSource: "measured",
        reason: null,
      }),
    ).toMatchObject({ stale: false, staleSince: null, thresholdSource: "measured", reason: null });
  });

  it("requires a complete decision and rejects unknown reasons and threshold sources", () => {
    const { stale: _stale, ...missing } = live;
    expect(() => decodeLiveness(missing)).toThrow();
    expect(() => decodeLiveness({ ...live, reason: "timeout" })).toThrow();
    expect(() => decodeLiveness({ ...live, thresholdSource: "fixed" })).toThrow();
  });
});

describe("Prism role kits", () => {
  it("default the six roles to no preferred models", () => {
    expect(Object.keys(DEFAULT_PRISM_ROLE_KITS).toSorted()).toEqual([
      "dispatcher",
      "escalation",
      "planner",
      "retry",
      "reviewer",
      "worker",
    ]);
    expect(DEFAULT_PRISM_ROLE_KITS.worker.lanes).toEqual({ easy: [], medium: [], hard: [] });
    expect(DEFAULT_PRISM_ROLE_KITS.dispatcher.models).toEqual([]);
    expect(DEFAULT_PRISM_ROLE_KITS.retry.enabled).toBe(true);
    expect(DEFAULT_PRISM_ROLE_KITS.escalation.enabled).toBe(true);
    expect("enabled" in DEFAULT_PRISM_ROLE_KITS.reviewer).toBe(false);
  });

  it("ship no role instructions or skills by default", () => {
    for (const kit of Object.values(DEFAULT_PRISM_ROLE_KITS)) {
      expect(kit.instructions).toBe("");
      expect(kit.skills).toEqual([]);
    }
  });

  it("load saved settings that still carry a thread-tool scope, dropping it", () => {
    const settings = decodeServerSettings({
      prismRoles: {
        dispatcher: { instructions: "Lead.", threadTools: "children" },
        worker: { threadTools: "none", lanes: { easy: [] } },
        correction: { threadTools: "none" },
      },
    });
    expect(settings.prismRoles.dispatcher.instructions).toBe("Lead.");
    const encoded = encodeKits(settings.prismRoles) as Record<string, object>;
    for (const role of ["dispatcher", "worker", "retry"] as const) {
      expect("threadTools" in settings.prismRoles[role]).toBe(false);
      expect("threadTools" in encoded[role]!).toBe(false);
    }
  });

  it("load Retry and Escalation saved under their legacy keys", () => {
    const luna = { instanceId: "codex", model: "gpt-5.6-luna" };
    const settings = decodeServerSettings({
      prismRoles: {
        correction: { models: [luna], enabled: false },
        recovery: { instructions: "Escalate." },
      },
    });
    expect(settings.prismRoles.retry).toMatchObject({ models: [luna], enabled: false });
    expect(settings.prismRoles.escalation).toMatchObject({
      instructions: "Escalate.",
      enabled: true,
    });
    expect(Object.keys(encodeKits(settings.prismRoles))).not.toContain("correction");
    // A kit saved under the new key wins over one left under the legacy key.
    const both = decodeServerSettings({
      prismRoles: { retry: { enabled: true }, correction: { enabled: false } },
    });
    expect(both.prismRoles.retry.enabled).toBe(true);
    const overrides = decodeProjectOverrides({ prismRoles: { recovery: { enabled: false } } });
    expect(overrides.prismRoles?.escalation.enabled).toBe(false);
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
        dispatcher: { instructions: "Lead.", lanes: { easy: [luna], medium: [opus], hard: [] } },
        reviewer: { lanes: { hard: [opus] } },
        retry: { models: [luna], lanes: { medium: [opus] }, enabled: false },
      },
    });
    expect(settings.prismRoles.dispatcher.models).toEqual([opus]);
    expect(settings.prismRoles.dispatcher.instructions).toBe("Lead.");
    expect("lanes" in settings.prismRoles.dispatcher).toBe(false);
    expect(settings.prismRoles.reviewer.models).toEqual([]);
    expect(settings.prismRoles.retry).toMatchObject({ models: [luna], enabled: false });
    expect(encodeKits(settings.prismRoles).dispatcher).toEqual({
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
    expect(settings.prismRoles.escalation.models).toEqual([]);
    expect(settings.prismRoles.reviewer.instructions).toBe("");
  });

  it("accept a project override of the whole kit set", () => {
    const overrides = decodeProjectOverrides({
      prismRoles: {
        reviewer: { models: [{ instanceId: "codex", model: "gpt-5.6-luna" }] },
      },
    });
    expect(overrides.prismRoles?.reviewer.models[0]?.model).toBe("gpt-5.6-luna");
    expect(overrides.prismRoles?.planner.models).toEqual([]);
  });

  it("patch one lane of the worker or one field of another role without defaults", () => {
    expect(decodeKitsPatch({ worker: { lanes: { hard: [] } } })).toEqual({
      worker: { lanes: { hard: [] } },
    });
    expect(decodeKitsPatch({ escalation: { enabled: false } })).toEqual({
      escalation: { enabled: false },
    });
  });
});
