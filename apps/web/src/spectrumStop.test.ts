import type { ThreadRuntimeSummary } from "@t3tools/client-runtime/state/shell";
import { ProviderInstanceId, RunId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { deriveCanStopThread } from "./spectrumStop";

const runtime = (
  status: ThreadRuntimeSummary["status"],
  activeRunId: RunId | null = null,
): ThreadRuntimeSummary => ({
  status,
  activeRunId,
  providerInstanceId: ProviderInstanceId.make("claude-default"),
  providerName: null,
  lastError: null,
  updatedAt: "2026-10-09T00:00:00.000Z",
});

describe("deriveCanStopThread", () => {
  it("offers Stop on an idle thread while its Spectrum runs", () => {
    expect(deriveCanStopThread(true, runtime("idle"), true)).toBe(true);
    expect(deriveCanStopThread(true, null, true)).toBe(true);
  });

  it("keeps Stop hidden on an idle thread without a running Spectrum", () => {
    expect(deriveCanStopThread(true, runtime("idle"), false)).toBe(false);
    expect(deriveCanStopThread(true, runtime("completed"), false)).toBe(false);
  });

  it("still offers Stop for an interruptible run, Spectrum or not", () => {
    const running = runtime("running", RunId.make("run-1"));
    expect(deriveCanStopThread(true, running, false)).toBe(true);
    expect(deriveCanStopThread(true, running, true)).toBe(true);
  });

  it("never offers Stop without an active thread", () => {
    expect(deriveCanStopThread(false, runtime("idle"), true)).toBe(false);
  });
});
