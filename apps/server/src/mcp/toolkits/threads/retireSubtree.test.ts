// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import { foldSubagentActivities } from "../../../../../../packages/client-runtime/src/state/subagentRuntime.ts";
import { CommandId, MessageId, ProjectId, ThreadId } from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { INTERRUPT_SETTLE_TIMEOUT } from "./handlers.ts";
import { describe, expect } from "vite-plus/test";

import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { blocksQueuedStart, RETIREMENT_KIND } from "../../../orchestration/ThreadRetirement.ts";
import {
  assistantReply,
  callTool,
  commandId,
  createParent,
  dispatchAll,
  NOW,
  session,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";

const start = (threadId: ThreadId, id: string) => ({
  type: "thread.turn.start" as const,
  commandId: CommandId.make(id),
  threadId,
  message: {
    messageId: MessageId.make(id),
    role: "user" as const,
    text: "Continue",
    attachments: [],
  },
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  createdAt: NOW,
});

const shell = (id: string) =>
  Effect.gen(function* () {
    const snapshots = yield* ProjectionSnapshotQuery;
    return Option.getOrThrow(yield* snapshots.getThreadShellById(ThreadId.make(id)));
  });

const setup = Effect.gen(function* () {
  const root = ThreadId.make((yield* callTool("spawn_thread", { task: "Root" })).threadId);
  yield* dispatchAll([session(root, "ready", null)]);
  const otherProject = ProjectId.make("other-project");
  yield* dispatchAll([
    {
      type: "project.create",
      commandId: commandId(),
      projectId: otherProject,
      title: "Other project",
      workspaceRoot: "/tmp/retirement-other-project",
      createdAt: NOW,
    },
  ]);
  const childResult = yield* callTool(
    "spawn_thread",
    { task: "Child in another project", projectId: otherProject, mode: "child" },
    root,
  );
  expect(childResult.parentThreadId).toBe(root);
  const child = ThreadId.make(childResult.threadId);
  yield* dispatchAll([session(child, "ready", null)]);
  const grandchild = ThreadId.make(
    (yield* callTool(
      "spawn_thread",
      { task: "Grandchild", projectId: (yield* shell(root)).projectId, mode: "child" },
      child,
    )).threadId,
  );
  yield* dispatchAll([
    session(root, "running", "root-turn"),
    session(child, "running", "child-turn"),
    session(grandchild, "starting", null),
  ]);
  return { root, child, grandchild };
});

describe("durable subtree retirement", () => {
  it.effect("reports pending honestly when physical stop is not acknowledged", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-retire-pending-");
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          const child = ThreadId.make(
            (yield* callTool("spawn_thread", { task: "Work", reportBack: false })).threadId,
          );
          yield* dispatchAll([session(child, "running", "still-running")]);
          const engine = yield* OrchestrationEngineService;
          const events = yield* engine.subscribeDomainEvents;
          const stopping = yield* callTool("interrupt_thread", {
            threadId: child,
            scope: "children",
            retireSubtree: true,
          }).pipe(Effect.forkScoped);
          yield* events.pipe(
            Stream.filter(
              (event) =>
                event.type === "thread.activity-appended" &&
                event.payload.activity.kind === "thread.subtree-retire-requested",
            ),
            Stream.runHead,
          );
          yield* TestClock.adjust(INTERRUPT_SETTLE_TIMEOUT);
          expect(yield* Fiber.join(stopping)).toEqual({
            threadId: child,
            turnId: "still-running",
            status: "interrupt_requested",
            statusAfter: "running",
          });
          expect(
            (yield* callTool("read_thread", { threadId: child, scope: "children" })).status,
          ).toBe("running");
          expect((yield* engine.getThreadRetirement(child))?.pendingStop).toBe(true);
        }),
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    "retires three levels atomically, rejects late reports/spawns/resumes, and recovers only a named thread across restart",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-retirement-");
        const database = NodePath.join(directory, "state.sqlite");
        const ids = yield* withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const ids = yield* setup;
            const independent = yield* callTool(
              "spawn_thread",
              {
                task: "Independent top-level work",
                mode: "top-level",
                projectId: ProjectId.make("other-project"),
              },
              ids.root,
            );
            expect(independent.parentThreadId).toBeNull();
            const independentId = ThreadId.make(independent.threadId);
            yield* dispatchAll([session(independentId, "running", "independent-turn")]);
            expect((yield* shell(ids.child)).projectId).not.toBe(
              (yield* shell(ids.root)).projectId,
            );
            expect((yield* shell(ids.grandchild)).projectId).toBe(
              (yield* shell(ids.root)).projectId,
            );
            const engine = yield* OrchestrationEngineService;
            const queued = yield* engine.dispatch(
              start(ids.child, "server:mcp-threads-turn:queued"),
            );
            const events = yield* engine.subscribeDomainEvents;
            const call = yield* callTool("interrupt_thread", {
              threadId: ids.root,
              scope: "children",
              retireSubtree: true,
            }).pipe(Effect.forkScoped);
            const requested = yield* events.pipe(
              Stream.takeUntil(
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.payload.activity.kind === "thread.subtree-retire-requested",
              ),
              Stream.runCollect,
            );
            expect(call.pollUnsafe()).toBeUndefined();
            expect((yield* shell(ids.root)).session?.status).toBe("running");
            for (const id of Object.values(ids)) {
              const state = yield* engine.getThreadRetirement(id);
              expect(state?.pendingStop).toBe(true);
              yield* engine.dispatch({
                ...session(id, "interrupted", null),
                commandId: CommandId.make(state!.stopAckCommandId),
              });
            }
            expect((yield* Fiber.join(call)).status).toBe("interrupted");
            expect(yield* engine.getThreadRetirement(independentId)).toBeUndefined();
            expect((yield* shell(independentId)).session?.status).toBe("running");
            expect((yield* shell(independentId)).session?.activeTurnId).toBe("independent-turn");
            yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.aggregateId === ids.child &&
                  event.payload.activity.kind === "task.updated" &&
                  (event.payload.activity.payload as { status?: string }).status === "interrupted",
              ),
              Stream.runHead,
            );
            const snapshots = yield* ProjectionSnapshotQuery;
            const childDetail = Option.getOrThrow(yield* snapshots.getThreadDetailById(ids.child));
            expect(
              foldSubagentActivities(childDetail.activities).find(
                (agent) => agent.id === ids.grandchild,
              )?.status,
            ).toBe("interrupted");
            // A dispatch receipt means the entire subtree's projection is committed.
            for (const id of Object.values(ids)) {
              expect((yield* shell(id)).session?.status).toBe("interrupted");
              expect((yield* shell(id)).session?.activeTurnId).toBeNull();
            }
            const state = yield* engine.getThreadRetirement(ids.child);
            expect(blocksQueuedStart(state, queued.sequence)).toBe(true);
            const seen = requested;
            expect(
              seen
                .filter((event) => event.type === "thread.session-stop-requested")
                .map((event) => event.aggregateId)
                .sort(),
            ).toEqual(Object.values(ids).sort());
            expect(
              (yield* callTool("spawn_thread", { task: "Late spawn" }, ids.child).pipe(Effect.flip))
                .message,
            ).toContain("retired");
            const childShell = yield* shell(ids.child);
            expect(
              (yield* engine
                .dispatch({
                  type: "thread.create",
                  commandId: commandId(),
                  threadId: ThreadId.make(`sub.${ids.child}.queued`),
                  projectId: childShell.projectId,
                  title: "Queued spawn",
                  modelSelection: childShell.modelSelection,
                  runtimeMode: childShell.runtimeMode,
                  interactionMode: childShell.interactionMode,
                  branch: null,
                  worktreePath: null,
                  createdAt: NOW,
                })
                .pipe(Effect.flip)).message,
            ).toContain("retired");
            for (const command of [
              "server:mcp-threads-report:late",
              "server:mcp-threads-turn:usage-resume",
            ]) {
              expect(
                (yield* engine.dispatch(start(ids.root, command)).pipe(Effect.flip)).message,
              ).toContain("retired");
            }
            // A late ready/error provider write must preserve stopped shell state.
            yield* dispatchAll([
              ...assistantReply(ids.grandchild, "late-reply", "Done"),
              session(ids.grandchild, "ready", null),
            ]);
            expect((yield* shell(ids.grandchild)).session?.status).toBe("interrupted");
            return ids;
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            for (const id of Object.values(ids)) {
              expect((yield* engine.getThreadRetirement(id))?.retired).toBe(true);
              expect((yield* shell(id)).session?.status).toBe("interrupted");
            }
            expect(
              (yield* engine
                .dispatch(start(ids.root, "server:mcp-threads-report:after-restart"))
                .pipe(Effect.flip)).message,
            ).toContain("retired");
            expect(
              (yield* engine
                .dispatch(start(ids.root, `server:mcp-threads-message:${ids.child}:in-flight`))
                .pipe(Effect.flip)).message,
            ).toContain("Calling thread");
            yield* callTool("message_thread", {
              threadId: ids.root,
              text: "Explicit recovery",
              scope: "children",
            });
            expect((yield* engine.getThreadRetirement(ids.root))?.retired).toBe(false);
            expect(
              (yield* engine
                .dispatch(
                  start(ids.child, `server:mcp-threads-message:${ids.root}:0:stale-before-retire`),
                )
                .pipe(Effect.flip)).message,
            ).toContain("Calling thread");
            expect((yield* engine.getThreadRetirement(ids.child))?.retired).toBe(true);
            expect((yield* engine.getThreadRetirement(ids.grandchild))?.retired).toBe(true);
            // Recovery never makes an already queued pre-retirement turn runnable.
            expect(blocksQueuedStart(yield* engine.getThreadRetirement(ids.root), 1)).toBe(true);
            const snapshots = yield* ProjectionSnapshotQuery;
            const markers = yield* snapshots.listActivitiesByKind(RETIREMENT_KIND);
            expect(
              markers.filter((row) => (row.payload as { threadId: string }).threadId === ids.root),
            ).toHaveLength(1);
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            expect((yield* engine.getThreadRetirement(ids.root))?.retired).toBe(false);
            expect((yield* engine.getThreadRetirement(ids.child))?.retired).toBe(true);
            // A human/client turn is another explicit named-thread recovery path.
            yield* engine.dispatch(start(ids.child, "client-manual-recovery"));
            expect((yield* engine.getThreadRetirement(ids.child))?.retired).toBe(false);
            expect((yield* engine.getThreadRetirement(ids.grandchild))?.retired).toBe(true);
            const previous = yield* engine.getThreadRetirement(ids.root);
            const events = yield* engine.subscribeDomainEvents;
            const stop = yield* callTool("interrupt_thread", {
              threadId: ids.root,
              scope: "children",
              retireSubtree: true,
            }).pipe(Effect.forkScoped);
            yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.payload.activity.kind === "thread.subtree-retire-requested",
              ),
              Stream.runHead,
            );
            for (const id of Object.values(ids)) {
              const retired = yield* engine.getThreadRetirement(id);
              expect(retired?.retired).toBe(true);
              expect(retired!.cutoffSequence).toBeGreaterThan(previous!.cutoffSequence);
              yield* engine.dispatch({
                ...session(id, "interrupted", null),
                commandId: CommandId.make(retired!.stopAckCommandId),
              });
            }
            yield* Fiber.join(stop);
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            for (const id of Object.values(ids))
              expect((yield* engine.getThreadRetirement(id))?.retired).toBe(true);
            // A recovered child reports honestly without waking its still-retired parent.
            yield* engine.dispatch(start(ids.child, "client-recover-child-only"));
            yield* dispatchAll([
              session(ids.child, "running", "recovered-turn"),
              ...assistantReply(ids.child, "recovered-reply", "Recovered child done"),
            ]);
            const events = yield* engine.subscribeDomainEvents;
            yield* dispatchAll([session(ids.child, "ready", null)]);
            yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.aggregateId === ids.root &&
                  event.payload.activity.kind === "task.progress" &&
                  (event.payload.activity.payload as { status?: string }).status === "idle",
              ),
              Stream.runHead,
            );
            const snapshots = yield* ProjectionSnapshotQuery;
            const parent = Option.getOrThrow(yield* snapshots.getThreadDetailById(ids.root));
            expect(
              parent.messages.some((message) => message.text.includes("Recovered child done")),
            ).toBe(false);
            const rows = yield* snapshots.listActivitiesByKind("task.progress");
            expect(
              rows.some(
                (row) =>
                  (row.payload as { reportedMessageId?: string }).reportedMessageId ===
                  "recovered-reply",
              ),
            ).toBe(true);
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            yield* engine.dispatch({
              type: "thread.archive",
              commandId: commandId(),
              threadId: ids.grandchild,
            });
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            expect((yield* engine.getThreadRetirement(ids.grandchild))?.retired).toBe(true);
            const snapshots = yield* ProjectionSnapshotQuery;
            const parent = Option.getOrThrow(yield* snapshots.getThreadDetailById(ids.root));
            expect(
              parent.messages.some((message) => message.text.includes("Recovered child done")),
            ).toBe(false);
          }),
        );
      }).pipe(Effect.scoped),
  );
});
