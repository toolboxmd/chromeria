import {
  EventId,
  PRISM_STREAM_STATS_ACTIVITY_KIND,
  ThreadId,
  type OrchestrationThreadShell,
  type OrchestrationThreadActivity,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";

import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import { make } from "./streamClock.ts";
import { StaleTurnDetector, staleTurnDetectorLayer } from "./staleTurnDetector.ts";
import { startStaleTurnMonitor } from "./staleTurnMonitor.ts";

const PARENT = ThreadId.make("parent");
const CHILD = ThreadId.make("sub.parent.child");
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
type Command = Parameters<OrchestrationEngineService["Service"]["dispatch"]>[0];
const parent = {
  id: PARENT,
  runtimeMode: "full-access",
  interactionMode: "default",
} as OrchestrationThreadShell;
const history: OrchestrationThreadActivity[] = Array.from({ length: 20 }, (_, i) => ({
  id: EventId.make(`sample-${i}`),
  kind: PRISM_STREAM_STATS_ACTIVITY_KIND,
  tone: "info",
  summary: "Stream completed",
  turnId: null,
  createdAt: "2026-09-28T00:00:00.000Z",
  payload: {
    provider: "opencode",
    model: "muse",
    outcome: "completed",
    timeToFirstTokenMs: 10_000,
    maxGapMs: 18_000,
  },
}));

const fixture = (title = "worker") =>
  Effect.gen(function* () {
    const messages = yield* Queue.unbounded<Command>();
    const clock = yield* make;
    const detector = yield* StaleTurnDetector;
    const child = { ...parent, id: CHILD, title };
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE projection_thread_activities (activity_id TEXT, kind TEXT, payload_json TEXT)`;
    for (const activity of history) {
      yield* sql`INSERT INTO projection_thread_activities VALUES (${activity.id}, ${activity.kind}, ${encodeJson(activity.payload)})`;
    }
    const layers = Layer.mergeAll(
      Layer.mock(ProjectionSnapshotQuery)({
        getThreadShellById: (id) => Effect.succeedSome(id === PARENT ? parent : child),
      }),
      Layer.mock(ProviderService)({
        listSessions: () =>
          Effect.succeed([{ threadId: CHILD, status: "running" } as ProviderSession]),
      }),
      Layer.mock(OrchestrationEngineService)({
        dispatch: (command) => Queue.offer(messages, command).pipe(Effect.as({ sequence: 1 })),
      }),
    );
    yield* startStaleTurnMonitor(Clock.currentTimeMillis).pipe(Effect.provide(layers));
    const send = (type: string, payload: unknown, turnId = "turn-1") =>
      clock.canonical({
        type,
        threadId: CHILD,
        provider: "opencode",
        turnId,
        eventId: type,
        createdAt: "2026-09-28T00:00:00.000Z",
        payload,
      } as ProviderRuntimeEvent);
    yield* send("turn.started", { model: "muse" });
    return { messages, detector, send, clock };
  });

it.effect(
  "loads saved stats, ticks each second and messages the parent only once per stale turn",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      expect(f.detector.state.status(CHILD)).toMatchObject({
        thresholdSource: "measured",
        thresholdMs: 15_000,
      });
      yield* TestClock.adjust("18 seconds");
      expect(yield* Queue.size(f.messages)).toBe(0);
      yield* TestClock.adjust("1 second");
      const message = yield* Queue.take(f.messages);
      expect(message.type).toBe("thread.turn.start");
      if (message.type !== "thread.turn.start") return;
      expect(message.threadId).toBe(PARENT);
      expect(message.message.text).toContain("sub.parent.child");
      expect(message.message.text).toContain("muse (opencode)");
      expect(message.message.text).toContain("1970-01-01T00:00:00.000Z (turn.started)");
      expect(message.message.text).toContain("Silence: 19000 ms; threshold: 15000 ms (measured)");
      yield* TestClock.adjust("30 seconds");
      yield* f.clock.native(CHILD, "native:reasoning.delta");
      yield* TestClock.adjust("30 seconds");
      yield* f.send("runtime.error", { message: "lost provider" });
      yield* TestClock.adjust("1 second");
      expect(yield* Queue.size(f.messages)).toBe(0);
      yield* f.send("turn.started", { model: "muse" }, "turn-2");
      yield* TestClock.adjust("19 seconds");
      expect(yield* Queue.take(f.messages)).toMatchObject({
        type: "thread.turn.start",
        threadId: PARENT,
      });
      expect(yield* Queue.size(f.messages)).toBe(0);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          staleTurnDetectorLayer,
          NodeCrypto.layer,
          NodeSqliteClient.layer({ filename: ":memory:" }),
        ),
      ),
    ),
);

it.effect("notifies the parent of every stale child, whatever its title", () =>
  Effect.gen(function* () {
    const f = yield* fixture("model-router job-62 worker seq 1");
    yield* TestClock.adjust("20 seconds");
    expect(f.detector.state.status(CHILD).stale).toBe(true);
    expect(yield* Queue.take(f.messages)).toMatchObject({
      type: "thread.turn.start",
      threadId: PARENT,
    });
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        staleTurnDetectorLayer,
        NodeCrypto.layer,
        NodeSqliteClient.layer({ filename: ":memory:" }),
      ),
    ),
  ),
);
