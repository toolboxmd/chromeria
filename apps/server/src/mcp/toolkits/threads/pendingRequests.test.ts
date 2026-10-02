// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import {
  ApprovalRequestId,
  EventId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationCommand,
  type RuntimeMode,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import {
  callTool,
  commandId,
  createParent,
  dispatchAll,
  dispatchUntil,
  NOW,
  PARENT_ID,
  parentMessages,
  session,
  taskStarted,
  temporaryDirectory,
  withEngineOnly,
  withServer,
  parentActivity,
} from "./handlers.testFixtures.ts";

const questions = [
  {
    id: "color",
    header: "Color",
    question: "Which color?",
    options: [
      { label: "Blue", description: "Use blue" },
      { label: "Red", description: "Use red" },
    ],
    multiSelect: false,
  },
];
const activity = (
  threadId: string,
  kind: string,
  requestId: string,
  extra: Record<string, unknown> = {},
): OrchestrationCommand => ({
  type: "thread.activity.append",
  commandId: commandId(),
  threadId: ThreadId.make(threadId),
  activity: {
    id: EventId.make(commandId()),
    tone: "approval",
    kind,
    summary: kind,
    payload: { requestId, ...extra },
    turnId: null,
    createdAt: NOW,
  },
  createdAt: NOW,
});
const approval = (id: string, requestId = "approve-1") =>
  activity(id, "approval.requested", requestId, {
    requestKind: "command",
    detail: "Run printf HERON",
    options: [
      { decision: "accept", label: "Allow once" },
      { decision: "decline", label: "Decline" },
    ],
  });
const input = (id: string, requestId = "question-1") =>
  activity(id, "user-input.requested", requestId, { questions });
const notice = (id: string, requestId: string) => (event: OrchestrationEvent) =>
  event.type === "thread.message-sent" &&
  event.aggregateId === PARENT_ID &&
  event.payload.text.includes(`thread ${id}) waiting`) &&
  event.payload.text.includes(`request ${requestId}`);
const notices = (messages: ReadonlyArray<{ text: string }>) =>
  messages.filter((message) => message.text.includes(") waiting on "));
const spawn = (
  runtimeMode: RuntimeMode = "approval-required",
  reportBack = true,
  caller = PARENT_ID,
) => callTool("spawn_thread", { task: "Wait for my request", runtimeMode, reportBack }, caller);
const read = (id: string) => callTool("read_thread", { threadId: id, scope: "children" });

/** A bridge barrier, using the existing parent activity receipt instead of sleeps. */
const barrier = (id: string) =>
  dispatchUntil(
    dispatchAll([
      session(ThreadId.make(id), "running", "barrier"),
      session(ThreadId.make(id), "ready", null),
    ]),
    parentActivity(id, "task.progress", "idle"),
  );

describe("parent pending child requests", () => {
  for (const mode of ["approval-required", "auto-accept-edits", "auto", "full-access"] as const) {
    it.effect(`notifies approvals and input with full detail in ${mode}`, () =>
      Effect.gen(function* () {
        const dir = yield* temporaryDirectory("t3-pending-");
        yield* withServer(
          NodePath.join(dir, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(dir);
            const { result: child } = yield* dispatchUntil(spawn(mode), taskStarted);
            yield* dispatchAll([session(ThreadId.make(child.threadId), "running", "request-turn")]);
            const { event: approved } = yield* dispatchUntil(
              dispatchAll([approval(child.threadId)]),
              notice(child.threadId, "approve-1"),
            );
            expect(approved.type === "thread.message-sent" && approved.payload.text).toContain(
              "Run printf HERON",
            );
            const { event: asked } = yield* dispatchUntil(
              dispatchAll([input(child.threadId)]),
              notice(child.threadId, "question-1"),
            );
            expect(asked.type === "thread.message-sent" && asked.payload.text).toContain(
              "Which color?",
            );
            expect(asked.type === "thread.message-sent" && asked.payload.text).toContain("Blue");
            const waiting = yield* read(child.threadId);
            expect(waiting.status).toBe("waiting");
            expect(waiting.pendingRequests).toMatchObject([
              {
                requestId: ApprovalRequestId.make("approve-1"),
                kind: "approval",
                detail: { detail: "Run printf HERON" },
              },
              {
                requestId: ApprovalRequestId.make("question-1"),
                kind: "user-input",
                detail: { questions },
              },
            ]);
            expect(notices(yield* parentMessages)).toHaveLength(2);
          }),
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect(
    "catches up unseen requests and never repeats them after restart, including a crash before recording delivery",
    () =>
      Effect.gen(function* () {
        const dir = yield* temporaryDirectory("t3-pending-restart-");
        const db = NodePath.join(dir, "state.sqlite");
        const child = yield* withServer(
          db,
          Effect.gen(function* () {
            yield* createParent(dir);
            return (yield* dispatchUntil(spawn(), taskStarted)).result.threadId;
          }),
        );
        yield* withEngineOnly(
          db,
          dispatchAll([
            session(ThreadId.make(child), "running", "turn"),
            approval(child),
            input(child),
          ]),
        );
        yield* withServer(
          db,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const events = yield* engine.subscribeDomainEvents;
            if (notices(yield* parentMessages).length < 2)
              yield* events.pipe(Stream.filter(notice(child, "question-1")), Stream.runHead);
            expect(notices(yield* parentMessages)).toHaveLength(2);
          }),
        );
        yield* withServer(
          db,
          Effect.gen(function* () {
            yield* barrier(child);
            expect(notices(yield* parentMessages)).toHaveLength(2);
            expect((yield* read(child)).status).toBe("waiting");
          }),
        );
      }).pipe(Effect.scoped),
  );

  it.effect("does not notify reportBack false children on restart", () =>
    Effect.gen(function* () {
      const dir = yield* temporaryDirectory("t3-pending-quiet-");
      const db = NodePath.join(dir, "state.sqlite");
      const child = yield* withServer(
        db,
        Effect.gen(function* () {
          yield* createParent(dir);
          return (yield* dispatchUntil(spawn("full-access", false), taskStarted)).result.threadId;
        }),
      );
      yield* withEngineOnly(
        db,
        dispatchAll([
          approval(child),
          activity(child, "approval.resolved", "approve-1"),
          input(child),
        ]),
      );
      yield* withServer(
        db,
        Effect.gen(function* () {
          yield* barrier(child);
          expect(notices(yield* parentMessages)).toHaveLength(0);
          expect((yield* read(child)).pendingRequests.map((r) => r.requestId)).toEqual([
            "question-1",
          ]);
        }),
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps an old pending request actionable and clears resolved or stale requests on restart",
    () =>
      Effect.gen(function* () {
        const dir = yield* temporaryDirectory("t3-pending-window-");
        const db = NodePath.join(dir, "state.sqlite");
        const child = yield* withServer(
          db,
          Effect.gen(function* () {
            yield* createParent(dir);
            return (yield* dispatchUntil(spawn(), taskStarted)).result.threadId;
          }),
        );
        yield* withEngineOnly(
          db,
          dispatchAll([
            approval(child),
            input(child),
            activity(child, "user-input.resolved", "question-1", { answers: { color: "Blue" } }),
            ...Array.from({ length: 510 }, (_, i) => activity(child, "noise", `noise-${i}`)),
          ]),
        );
        yield* withServer(
          db,
          Effect.gen(function* () {
            yield* barrier(child);
            const waiting = yield* read(child);
            expect(waiting.status).toBe("waiting");
            expect(waiting.pendingRequests.map((r) => r.requestId)).toEqual(["approve-1"]);
            expect(notices(yield* parentMessages)).toHaveLength(1);
            yield* dispatchAll([
              activity(child, "provider.approval.respond.failed", "approve-1", {
                detail: "Unknown pending approval request: approve-1",
              }),
            ]);
            expect((yield* read(child)).pendingRequests).toEqual([]);
          }),
        );
        yield* withServer(
          db,
          Effect.gen(function* () {
            yield* barrier(child);
            expect(notices(yield* parentMessages)).toHaveLength(1);
            expect((yield* read(child)).pendingRequests).toEqual([]);
          }),
        );
      }).pipe(Effect.scoped),
  );

  for (const decision of ["accept", "decline"] as const) {
    it.effect(`dispatches ${decision} through the existing approval response path`, () =>
      Effect.gen(function* () {
        const dir = yield* temporaryDirectory("t3-pending-answer-");
        yield* withServer(
          NodePath.join(dir, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(dir);
            const { result: child } = yield* dispatchUntil(spawn(), taskStarted);
            yield* dispatchUntil(
              dispatchAll([approval(child.threadId)]),
              notice(child.threadId, "approve-1"),
            );
            const { event } = yield* dispatchUntil(
              callTool("pending_request_respond", {
                threadId: child.threadId,
                requestId: ApprovalRequestId.make("approve-1"),
                decision,
              }),
              (e) => e.type === "thread.approval-response-requested",
            );
            expect(
              event.type === "thread.approval-response-requested" && event.payload,
            ).toMatchObject({
              threadId: child.threadId,
              requestId: ApprovalRequestId.make("approve-1"),
              decision,
            });
            yield* dispatchAll([
              activity(child.threadId, "approval.resolved", "approve-1", { decision }),
              session(ThreadId.make(child.threadId), "running", "continued"),
            ]);
            expect((yield* read(child.threadId)).status).toBe("running");
            expect((yield* read(child.threadId)).pendingRequests).toEqual([]);
            expect(
              (yield* Effect.flip(
                callTool("pending_request_respond", {
                  threadId: child.threadId,
                  requestId: ApprovalRequestId.make("approve-1"),
                  decision,
                }),
              )).reason,
            ).toMatch(/unknown or already resolved/);
          }),
        );
      }).pipe(Effect.scoped),
    );
  }

  it.effect(
    "answers a descendant's question and refuses unrelated, unknown and mismatched requests",
    () =>
      Effect.gen(function* () {
        const dir = yield* temporaryDirectory("t3-pending-descendant-");
        yield* withServer(
          NodePath.join(dir, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(dir);
            const { result: child } = yield* dispatchUntil(spawn(), taskStarted);
            const grandchild = yield* spawn(
              "approval-required",
              false,
              ThreadId.make(child.threadId),
            );
            yield* dispatchAll([input(grandchild.threadId)]);
            const answers = { color: "Blue" };
            const { event } = yield* dispatchUntil(
              callTool("pending_request_respond", {
                threadId: grandchild.threadId,
                requestId: ApprovalRequestId.make("question-1"),
                answers,
              }),
              (e) => e.type === "thread.user-input-response-requested",
            );
            expect(
              event.type === "thread.user-input-response-requested" && event.payload,
            ).toMatchObject({
              threadId: grandchild.threadId,
              requestId: ApprovalRequestId.make("question-1"),
              answers,
            });
            for (const [params, caller, error] of [
              [
                { threadId: PARENT_ID, requestId: ApprovalRequestId.make("question-1"), answers },
                child.threadId,
                /not a descendant/,
              ],
              [
                { threadId: child.threadId, requestId: ApprovalRequestId.make("missing"), answers },
                PARENT_ID,
                /unknown or already resolved/,
              ],
              [
                {
                  threadId: grandchild.threadId,
                  requestId: ApprovalRequestId.make("question-1"),
                  decision: "accept" as const,
                },
                PARENT_ID,
                /require answers/,
              ],
            ] as const) {
              expect(
                (yield* Effect.flip(callTool("pending_request_respond", params, caller))).reason,
              ).toMatch(error);
            }
            yield* dispatchAll([approval(child.threadId)]);
            expect(
              (yield* Effect.flip(
                callTool("pending_request_respond", {
                  threadId: child.threadId,
                  requestId: ApprovalRequestId.make("approve-1"),
                  answers,
                }),
              )).reason,
            ).toMatch(/require decision/);
            yield* dispatchAll([
              activity(grandchild.threadId, "user-input.resolved", "question-1", { answers }),
            ]);
            expect(
              (yield* Effect.flip(
                callTool("pending_request_respond", {
                  threadId: grandchild.threadId,
                  requestId: ApprovalRequestId.make("question-1"),
                  answers,
                }),
              )).reason,
            ).toMatch(/unknown or already resolved/);
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadShellById(ThreadId.make(grandchild.threadId)))
                .hasPendingUserInput,
            ).toBe(false);
          }),
        );
      }).pipe(Effect.scoped),
  );
});
