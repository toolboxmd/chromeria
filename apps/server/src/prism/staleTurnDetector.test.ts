import { describe, expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { makeStaleTurnDetectorState } from "./staleTurnDetector.ts";
import type { ThreadLiveness } from "./streamClock.ts";

const live: ThreadLiveness = {
  threadId: ThreadId.make("child"),
  runId: RunId.make("run"),
  runOrdinal: 1,
  attemptId: RunAttemptId.make("attempt"),
  attemptOrdinal: 1,
  providerThreadId: ProviderThreadId.make("native"),
  providerTurnId: ProviderTurnId.make("turn"),
  provider: ProviderDriverKind.make("codex"),
  model: "fixed",
  startedAt: 0,
  firstTokenAt: null,
  lastStreamAt: 0,
  lastEventKind: "attempt.started",
  openTool: null,
  eventCount: 0,
};
describe("silence detector", () => {
  it("uses default confirmed thresholds and reports once per turn", () => {
    const detector = makeStaleTurnDetectorState();
    expect(detector.check(live, 150_000, [])).toBeNull();
    expect(detector.check(live, 150_001, [])).toMatchObject({
      thresholdMs: 120_000,
      thresholdSource: "default",
    });
    detector.notified(live);
    expect(
      detector.check({ ...live, firstTokenAt: 160_000, lastStreamAt: 160_000 }, 400_000, []),
    ).toBeNull();
    expect(
      detector.check(
        { ...live, providerTurnId: ProviderTurnId.make("next"), lastStreamAt: 200_000 },
        400_000,
        [],
      ),
    ).not.toBeNull();
  });
  it("uses persisted healthy samples only after twenty matching turns", () => {
    const detector = makeStaleTurnDetectorState();
    const sample = {
      provider: live.provider,
      model: live.model,
      count: 20,
      maxTimeToFirstTokenMs: 4_000,
      maxGapMs: 2_000,
    };
    expect(detector.check(live, 7_500, [sample])).toBeNull();
    expect(detector.check(live, 7_501, [sample])).toMatchObject({
      thresholdMs: 6_000,
      thresholdSource: "measured",
    });
    expect(makeStaleTurnDetectorState().check(live, 7_501, [{ ...sample, count: 19 }])).toBeNull();
    expect(
      makeStaleTurnDetectorState().check(live, 7_501, [{ ...sample, model: "other" }]),
    ).toBeNull();
    expect(
      makeStaleTurnDetectorState().check({ ...live, firstTokenAt: 1_000 }, 3_751, [sample]),
    ).toMatchObject({ thresholdMs: 3_000 });
  });
  it("does not treat host suspension or a busy event loop as provider silence", () => {
    const detector = makeStaleTurnDetectorState();
    detector.tick(0, 0, [live]);
    detector.tick(300_000, 1_000, [live]);
    expect(detector.check(live, 300_000, [])).toBeNull();
    detector.tick(600_000, 301_000, [live]);
    expect(detector.check(live, 600_000, [])).toBeNull();
    expect(detector.check(live, 750_001, [])).not.toBeNull();
  });
  it("suspends silence during open tools and requires a fresh window afterwards", () => {
    const detector = makeStaleTurnDetectorState();
    const tool = { ...live, firstTokenAt: 1_000, openTool: { itemId: "tool", startedAt: 2_000 } };
    detector.tick(300_000, 300_000, [tool]);
    expect(detector.check(tool, 300_000, [])).toBeNull();
    const closed = { ...tool, openTool: null, lastStreamAt: 300_001 };
    expect(detector.check(closed, 400_000, [])).toBeNull();
    expect(detector.check(closed, 412_502, [])).not.toBeNull();
  });
});
