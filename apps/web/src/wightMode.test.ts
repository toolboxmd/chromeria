import { describe, expect, it } from "vite-plus/test";
import { wightThreadWait, wightUsageStatus } from "./wightMode";

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
