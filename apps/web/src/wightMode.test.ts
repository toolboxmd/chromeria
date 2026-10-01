import { describe, expect, it } from "vite-plus/test";

import { wightThreadWait, wightUsageStatus } from "./wightMode";

const window = (label: string, usedPercent: number) => ({
  id: label,
  kind: "session" as const,
  label,
  usedPercent,
});
const provider = (windows: ReturnType<typeof window>[], enabled = true) => ({
  enabled,
  usageLimits: { checkedAt: "2026-10-01T00:00:00.000Z", windows },
});

describe("wightUsageStatus", () => {
  it("pauses when any window reaches the limit and names the highest", () => {
    const status = wightUsageStatus(provider([window("Weekly", 40), window("5h", 85)]), 80);
    expect(status.paused).toBe(true);
    expect(status.text).toContain("5h at 85%");
    expect(wightUsageStatus(provider([window("5h", 80)]), 80).paused).toBe(true);
  });

  it("resumes below the limit", () => {
    expect(wightUsageStatus(provider([window("5h", 79.4)]), 80)).toEqual({
      paused: false,
      text: "5h at 79%, limit 80%.",
    });
  });

  it("runs unenforced without usage windows, but pauses a missing or disabled instance", () => {
    expect(wightUsageStatus(provider([]), 80).paused).toBe(false);
    expect(wightUsageStatus({ enabled: true }, 80).text).toContain("cannot be enforced");
    expect(wightUsageStatus(undefined, 80).paused).toBe(true);
    expect(wightUsageStatus(provider([window("5h", 1)], false), 80).paused).toBe(true);
  });

  it("treats a 0% limit as paused even without windows", () => {
    expect(wightUsageStatus(provider([]), 0).paused).toBe(true);
  });
});

describe("wightThreadWait", () => {
  const ready = {
    archivedAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    session: null,
  };
  const session = (
    status: "ready" | "running" | "interrupted" | "error",
    lastError: string | null,
  ) => ({ status, lastError });

  it("is ready for a nudge when idle or working", () => {
    expect(wightThreadWait(ready)).toBeNull();
    expect(wightThreadWait({ ...ready, session: session("running", null) })).toBeNull();
  });

  it("waits on the user for approvals, questions, plans and interrupts", () => {
    expect(wightThreadWait({ ...ready, hasPendingApprovals: true })).toContain("approval");
    expect(wightThreadWait({ ...ready, hasPendingUserInput: true })).toContain("question");
    expect(wightThreadWait({ ...ready, hasActionableProposedPlan: true })).toContain("plan");
    expect(wightThreadWait({ ...ready, session: session("interrupted", null) })).toContain(
      "interrupted",
    );
  });

  it("leaves usage-limit failures to usage-limit resume but waits on other failures", () => {
    expect(
      wightThreadWait({ ...ready, session: session("error", "You've hit your usage limit.") }),
    ).toBeNull();
    expect(wightThreadWait({ ...ready, session: session("error", "Process exited") })).toContain(
      "failed",
    );
  });
});
