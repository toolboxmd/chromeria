import { describe, expect, it } from "vite-plus/test";
import { threadIdleChoice, wightThreadWait, wightUsageStatus } from "./threadIdleMode.ts";

const thread = {
  archivedAt: null,
  pendingRuntimeRequest: null,
  hasActionableProposedPlan: false,
  status: "idle" as const,
  lastErrorClass: null,
  limitRecovery: null,
};

describe("Wight user status", () => {
  it("explains the quota line and missing provider readings", () => {
    expect(wightUsageStatus(undefined, 80).paused).toBe(true);
    expect(wightUsageStatus({ enabled: true }, 80)).toEqual({
      paused: false,
      text: "This provider reports no usage windows, so the Wight limit cannot be enforced.",
    });
    expect(wightUsageStatus({ enabled: true }, 0).paused).toBe(true);
  });
  it("explains reset recovery and manual recovery while allowing completed work", () => {
    expect(
      wightThreadWait({ ...thread, status: "failed", lastErrorClass: "usage_limit" }),
    ).toContain("reset");
    expect(
      wightThreadWait({ ...thread, status: "failed", lastErrorClass: "provider_error" }),
    ).toContain("failed");
    expect(wightThreadWait({ ...thread, status: "interrupted" })).toContain("interrupted");
    expect(wightThreadWait({ ...thread, status: "completed" })).toBeNull();
    expect(wightThreadWait({ ...thread, hasActionableProposedPlan: true })).toContain("plan");
  });
});

describe("threadIdleChoice", () => {
  it("reads an ended Wight timer and a stopped keep-warm as off", () => {
    const enabledAt = "2026-11-23T10:00:00.000Z";
    expect(threadIdleChoice(undefined, 0)).toBe("off");
    expect(threadIdleChoice({ type: "wight", enabledAt, expiresAt: 5 }, 4)).toBe("wight");
    expect(threadIdleChoice({ type: "wight", enabledAt, expiresAt: 5 }, 5)).toBe("off");
    const warm = { type: "keepWarm" as const, enabledAt, hiddenRequestAt: null, stopped: null };
    expect(threadIdleChoice(warm, 0)).toBe("keepWarm");
    expect(threadIdleChoice({ ...warm, stopped: { at: enabledAt, reason: "userAway" } }, 0)).toBe(
      "off",
    );
  });
});
