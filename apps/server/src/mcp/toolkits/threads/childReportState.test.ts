// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { EventId, ThreadId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { describe, expect, it } from "@effect/vitest";

import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { childReportStatesFrom, unrecordedChildReportState } from "./childReportState.ts";
import {
  assistantReply,
  callTool,
  createParent,
  dispatchAll,
  dispatchUntil,
  PARENT_ID,
  parentActivity,
  parentMessages,
  session,
  taskStarted,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";

const activity = (kind: string, payload: unknown): OrchestrationThreadActivity => ({
  id: EventId.make(`activity-${kind}-${JSON.stringify(payload)}`),
  tone: "info",
  kind,
  summary: kind,
  payload,
  turnId: null,
  createdAt: "2026-01-01T00:00:00.000Z",
});

describe("childReportStatesFrom", () => {
  it("restores each child from its newest row carrying report state", () => {
    const states = childReportStatesFrom([
      activity("task.started", { taskId: "sub.p.a", reportBack: false }),
      activity("task.started", { taskId: "sub.p.b", reportBack: true }),
      activity("task.progress", { taskId: "sub.p.b", reportBack: true, reportedMessageId: "m1" }),
      activity("task.updated", { taskId: "sub.p.b", status: "running" }),
      activity("task.progress", { taskId: "sub.p.b", reportBack: true, reportedMessageId: "m2" }),
      activity("task.progress", { taskId: "sub.p.c", reportBack: false }),
    ]);
    expect(Object.fromEntries(states)).toEqual({
      "sub.p.a": { reportBack: false, lastReported: null },
      "sub.p.b": { reportBack: true, lastReported: "m2" },
      "sub.p.c": { reportBack: false, lastReported: null },
    });
  });

  it("omits children whose rows carry no report state", () => {
    expect(childReportStatesFrom([activity("task.started", { taskId: "sub.p.old" })]).size).toBe(0);
  });

  it("treats an unrecorded child's existing reply as already reported", () => {
    expect(unrecordedChildReportState("m9")).toEqual({ reportBack: true, lastReported: "m9" });
    expect(unrecordedChildReportState(null)).toEqual({ reportBack: true, lastReported: null });
  });
});

const spawnChild = (reportBack: boolean) =>
  callTool("spawn_thread", { task: "Remember HERON.", reportBack });

const reportsOf = (messages: ReadonlyArray<{ role: string; text: string }>) =>
  messages.filter((message) => message.role === "user" && message.text.includes("finished a turn"));

describe("spawned child runtime mode", () => {
  it.effect("starts every child in full access, whatever mode the parent runs in", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-child-mode-");
      const childMode = yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory, "approval-required");
          const { result } = yield* dispatchUntil(spawnChild(false), taskStarted);
          const snapshots = yield* ProjectionSnapshotQuery;
          const child = yield* snapshots.getThreadDetailById(ThreadId.make(result.threadId));
          return Option.getOrThrow(child).runtimeMode;
        }),
      );
      expect(childMode).toBe("full-access");
    }).pipe(Effect.scoped),
  );
});

describe("child report-back across a server restart", () => {
  it.effect("keeps reporting a child's finished turns to its parent, once each", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-child-restart-");
      const databasePath = NodePath.join(directory, "state.sqlite");

      // First process: the parent spawns one child that reports back and one that does not.
      const [childId, quietId] = yield* withServer(
        databasePath,
        Effect.gen(function* () {
          yield* createParent(directory);
          const loud = yield* dispatchUntil(spawnChild(true), taskStarted);
          const quiet = yield* dispatchUntil(spawnChild(false), taskStarted);
          return [loud.result.threadId, quiet.result.threadId] as const;
        }),
      );
      const child = ThreadId.make(childId);
      const quietChild = ThreadId.make(quietId);

      // Second process: both children finish a turn after the restart. The
      // quiet child goes first; the bridge handles events in order, so a
      // report from it would reach the parent before the loud child's.
      const { quietIdle, report } = yield* withServer(
        databasePath,
        Effect.gen(function* () {
          const { event: quietIdle } = yield* dispatchUntil(
            dispatchAll([
              session(quietChild, "running", "quiet-turn-1"),
              ...assistantReply(quietChild, "quiet-reply-1", "QUIET"),
              session(quietChild, "ready", null),
            ]),
            parentActivity(quietId, "task.progress", "idle"),
          );
          const { event: report } = yield* dispatchUntil(
            dispatchAll([
              session(child, "running", "turn-1"),
              ...assistantReply(child, "reply-1", "HERON"),
              session(child, "ready", null),
            ]),
            (event) =>
              event.type === "thread.message-sent" &&
              event.aggregateId === PARENT_ID &&
              event.payload.text.includes("finished a turn"),
          );
          return { quietIdle, report };
        }),
      );
      expect(
        quietIdle.type === "thread.activity-appended" && quietIdle.payload.activity.payload,
      ).toMatchObject({ reportBack: false });
      expect(report.type === "thread.message-sent" && report.payload.text).toContain("HERON");

      // Third process: an idle transition without a new reply is not reported
      // again, and the quiet child was never reported.
      const messages = yield* withServer(
        databasePath,
        Effect.gen(function* () {
          yield* dispatchUntil(
            dispatchAll([session(child, "running", "turn-2"), session(child, "ready", null)]),
            parentActivity(childId, "task.progress", "idle"),
          );
          // The bridge handles events in order, so once this row lands any report
          // for the idle transition above has already been dispatched.
          yield* dispatchUntil(
            dispatchAll([session(child, "running", "turn-3")]),
            parentActivity(childId, "task.updated", "running"),
          );
          return yield* parentMessages;
        }),
      );
      expect(reportsOf(messages).map((message) => message.text)).toEqual([
        expect.stringContaining("HERON"),
      ]);
    }).pipe(Effect.scoped),
  );
});
