import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type { ProviderAdapterV2Event } from "../orchestration-v2/ProviderAdapter.ts";
import { makeStreamClockState } from "./streamClock.ts";

const identity = {
  threadId: ThreadId.make("child"),
  runId: RunId.make("run"),
  runOrdinal: 1,
  attemptId: RunAttemptId.make("attempt"),
  attemptOrdinal: 1,
  providerThreadId: ProviderThreadId.make("native"),
  provider: ProviderDriverKind.make("codex"),
  model: "fixed",
};
const turnId = ProviderTurnId.make("turn");
const now = DateTime.makeUnsafe(0);
const running: ProviderAdapterV2Event = {
  type: "provider_turn.updated",
  driver: identity.provider,
  threadId: identity.threadId,
  providerTurn: {
    id: turnId,
    providerThreadId: identity.providerThreadId,
    nodeId: NodeId.make("node"),
    runAttemptId: identity.attemptId,
    nativeTurnRef: null,
    ordinal: 1,
    status: "running",
    startedAt: now,
    completedAt: null,
  },
};
const text: ProviderAdapterV2Event = {
  type: "message.updated",
  driver: identity.provider,
  message: {
    createdBy: "agent",
    creationSource: "provider",
    id: MessageId.make("answer"),
    threadId: identity.threadId,
    runId: identity.runId,
    nodeId: NodeId.make("node"),
    role: "assistant",
    text: "An unfinished paragraph",
    attachments: [],
    streaming: true,
    createdAt: now,
    updatedAt: now,
  },
};
const terminal: ProviderAdapterV2Event = {
  type: "turn.terminal",
  driver: identity.provider,
  providerThreadId: identity.providerThreadId,
  providerTurnId: turnId,
  runOrdinal: 1,
  status: "completed",
  failure: null,
  threadDisposition: "reusable",
};
const input = (event: ProviderAdapterV2Event) => ({
  threadId: identity.threadId,
  runId: identity.runId,
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerSessionId: ProviderSessionId.make("session"),
  event,
});

describe("stream clock ownership and turn measurements", () => {
  it("records a terminal-only completion once and releases its liveness", () => {
    const clock = makeStreamClockState();
    clock.beginAttempt(identity, 1_000);
    clock.observe(input(running), 1_000, identity.attemptId);
    clock.observe(input(text), 1_500, identity.attemptId);
    clock.observe(input(text), 2_000, identity.attemptId);
    expect(clock.observe(input(terminal), 3_000, identity.attemptId)).toMatchObject({
      outcome: "completed",
      timeToFirstTokenMs: 500,
      maxGapMs: 1_000,
      eventCount: 4,
    });
    expect(clock.observe(input(terminal), 4_000, identity.attemptId)).toBeNull();
    expect(
      clock.endAttempt(identity.threadId, identity.runId, identity.attemptId, 5_000),
    ).toBeNull();
    expect(clock.liveness()).toEqual([]);
  });

  it("fences old attempt text and cleanup after same-run recovery", () => {
    const clock = makeStreamClockState();
    clock.beginAttempt(identity, 1_000);
    clock.observe(input(running), 1_000, identity.attemptId);
    const next = { ...identity, attemptId: RunAttemptId.make("retry"), attemptOrdinal: 2 };
    expect(clock.beginAttempt(next, 2_000)).toMatchObject({
      runId: identity.runId,
      providerTurnId: turnId,
      outcome: "aborted",
      endedAt: 2_000,
    });
    expect(clock.beginAttempt(identity, 2_100)).toBeNull();
    clock.observe(input(text), 10_000, identity.attemptId);
    clock.observe(input(running), 10_000, identity.attemptId);
    expect(
      clock.endAttempt(identity.threadId, identity.runId, identity.attemptId, 10_000),
    ).toBeNull();
    expect(clock.liveness()).toMatchObject([
      { attemptId: next.attemptId, firstTokenAt: null, lastStreamAt: 2_000, eventCount: 0 },
    ]);
    clock.observe(input(text), 2_100, next.attemptId);
    expect(clock.liveness()).toMatchObject([{ firstTokenAt: 2_100, eventCount: 1 }]);
  });

  it("does not count child-provider text or another run as root activity", () => {
    const clock = makeStreamClockState();
    clock.beginAttempt(identity, 1_000);
    clock.observe(input(running), 1_100, identity.attemptId);
    clock.observe({ ...input(text), runId: RunId.make("other") }, 9_000, identity.attemptId);
    clock.observe(
      input({
        ...text,
        message: { ...text.message, threadId: ThreadId.make("native-child"), runId: null },
      }),
      9_000,
      identity.attemptId,
    );
    expect(clock.liveness()).toMatchObject([
      { firstTokenAt: null, lastStreamAt: 1_100, eventCount: 1 },
    ]);
  });

  it("measures each provider turn separately when one attempt continues", () => {
    const clock = makeStreamClockState();
    clock.beginAttempt(identity, 1_000);
    clock.observe(input(running), 1_000, identity.attemptId);
    clock.observe(input(text), 1_500, identity.attemptId);
    expect(clock.observe(input(terminal), 2_000, identity.attemptId)).toMatchObject({
      providerTurnId: turnId,
      startedAt: 1_000,
      timeToFirstTokenMs: 500,
    });
    const nextTurnId = ProviderTurnId.make("next-turn");
    clock.observe(
      input({ ...running, providerTurn: { ...running.providerTurn, id: nextTurnId, ordinal: 2 } }),
      3_000,
      identity.attemptId,
    );
    clock.observe(input(text), 3_100, identity.attemptId);
    expect(
      clock.observe(input({ ...terminal, providerTurnId: nextTurnId }), 4_000, identity.attemptId),
    ).toMatchObject({
      providerTurnId: nextTurnId,
      startedAt: 3_000,
      timeToFirstTokenMs: 100,
    });
    expect(
      clock.endAttempt(identity.threadId, identity.runId, identity.attemptId, 5_000),
    ).toBeNull();
  });

  it("tracks overlapping tools and excludes their execution time from healthy gaps", () => {
    const clock = makeStreamClockState();
    clock.beginAttempt(identity, 1_000);
    clock.observe(input(running), 1_000, identity.attemptId);
    const tool = (id: string, status: "running" | "completed"): ProviderAdapterV2Event => ({
      type: "turn_item.updated",
      driver: identity.provider,
      turnItem: {
        id: TurnItemId.make(id),
        threadId: identity.threadId,
        runId: identity.runId,
        nodeId: NodeId.make("node"),
        providerThreadId: identity.providerThreadId,
        providerTurnId: turnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status,
        title: null,
        startedAt: now,
        completedAt: status === "completed" ? now : null,
        updatedAt: now,
        type: "command_execution",
        input: "work",
      },
    });
    clock.observe(input(text), 1_100, identity.attemptId);
    clock.observe(input(tool("first", "running")), 1_200, identity.attemptId);
    clock.observe(input(tool("second", "running")), 100_000, identity.attemptId);
    clock.observe(input(tool("first", "completed")), 200_000, identity.attemptId);
    expect(clock.liveness()).toMatchObject([{ openTool: { itemId: "second" } }]);
    clock.observe(input(tool("second", "completed")), 300_000, identity.attemptId);
    expect(clock.liveness()).toMatchObject([{ openTool: null }]);
    expect(clock.observe(input(terminal), 300_100, identity.attemptId)).toMatchObject({
      maxGapMs: 100,
    });
  });
});
