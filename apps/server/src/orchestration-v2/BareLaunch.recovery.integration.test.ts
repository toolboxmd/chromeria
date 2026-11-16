import { expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

it.live.each(["stop", "open-failure", "restart-open-failure"] as const)(
  "bare recovery after %s keeps undelivered text without status prose or earlier delivered input",
  (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `bare-recovery-${scenario}`;
        const cwd = yield* checkpointWorkspace(name);
        const threadId = ThreadId.make(name);
        const instanceId = ProviderInstanceId.make(name);
        const driver = ProviderDriverKind.make("codex");
        const delivered: string[] = [];
        let active: ProviderAdapter.ProviderAdapterV2TurnInput | undefined;
        let failedOpen = false;
        const capabilities = {
          ...CodexProviderCapabilitiesV2,
          sessions: {
            ...CodexProviderCapabilitiesV2.sessions,
            supportsModelSwitchInSession: false,
            supportsMultipleProviderThreadsPerSession: false,
          },
        };
        const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
          instanceId,
          driver,
          bareSupport: Effect.succeed({ supported: true }),
          getCapabilities: () => Effect.succeed(capabilities),
          planSelectionTransition: () => Effect.succeed({ type: "restart_session" }),
          openSession: (input) =>
            Effect.gen(function* () {
              if (input.modelSelection.model === "fail-open" && !failedOpen) {
                failedOpen = true;
                return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
                  driver,
                  providerSessionId: input.providerSessionId,
                  cause: "session open failed before provider start",
                });
              }
              const now = yield* DateTime.now;
              const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
              const publish = (
                turn: ProviderAdapter.ProviderAdapterV2TurnInput,
                status: "running" | "interrupted" | "completed",
              ) =>
                Effect.gen(function* () {
                  const occurredAt = yield* DateTime.now;
                  const providerTurnId = ProviderTurnId.make(`turn:${turn.attemptId}`);
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: providerTurnId,
                      providerThreadId: turn.providerThread.id,
                      nodeId: turn.rootNodeId,
                      runAttemptId: turn.attemptId,
                      nativeTurnRef: { driver, nativeId: providerTurnId, strength: "strong" },
                      ordinal: turn.providerTurnOrdinal,
                      status,
                      startedAt: occurredAt,
                      completedAt: status === "running" ? null : occurredAt,
                    },
                  });
                  if (status !== "running")
                    yield* Queue.offer(events, {
                      type: "turn.terminal",
                      driver,
                      providerThreadId: turn.providerThread.id,
                      providerTurnId,
                      runOrdinal: turn.runOrdinal,
                      status,
                      failure: null,
                      threadDisposition: "reusable",
                    });
                });
              return {
                instanceId,
                driver,
                providerSessionId: input.providerSessionId,
                providerSession: {
                  id: input.providerSessionId,
                  driver,
                  providerInstanceId: instanceId,
                  status: "ready",
                  cwd,
                  model: input.modelSelection.model,
                  capabilities,
                  createdAt: now,
                  updatedAt: now,
                  lastError: null,
                },
                events: Stream.fromQueue(events),
                ensureThread: () =>
                  Effect.succeed({
                    id: ProviderThreadId.make(`provider:${name}`),
                    driver,
                    providerInstanceId: instanceId,
                    providerSessionId: input.providerSessionId,
                    appThreadId: threadId,
                    ownerNodeId: null,
                    nativeThreadRef: { driver, nativeId: name, strength: "strong" as const },
                    nativeConversationHeadRef: null,
                    status: "idle" as const,
                    firstRunOrdinal: null,
                    lastRunOrdinal: null,
                    handoffIds: [],
                    forkedFrom: null,
                    createdAt: now,
                    updatedAt: now,
                  }),
                resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
                startTurn: (turn) =>
                  Effect.gen(function* () {
                    delivered.push(turn.message.text);
                    active = turn;
                    yield* publish(
                      turn,
                      turn.modelSelection.model === "running" ? "running" : "completed",
                    );
                  }),
                interruptTurn: () =>
                  active === undefined
                    ? Effect.die("no active turn")
                    : publish(active, "interrupted"),
                steerTurn: () => Effect.die("unexpected steer"),
                respondToRuntimeRequest: () => Effect.die("unexpected approval"),
                readThreadSnapshot: () => Effect.die("unexpected snapshot"),
                rollbackThread: () => Effect.die("unexpected rollback"),
                forkThread: () => Effect.die("unexpected fork"),
              };
            }),
        };
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${name}:create`),
            threadId,
            projectId: ProjectId.make(name),
            title: name,
            modelSelection: { instanceId, model: "complete" },
            bare: { additions: [] },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const dispatch = (step: string, text: string, model: string, restart = false) =>
            Effect.gen(function* () {
              const previous = yield* orchestrator.getThreadProjection(threadId);
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(`${name}:${step}`),
                threadId,
                messageId: MessageId.make(`${name}:${step}`),
                text,
                attachments: [],
                modelSelection: { instanceId, model },
                dispatchMode: restart
                  ? { type: "restart_active", targetRunId: previous.runs[0]!.id }
                  : { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
            });
          if (scenario === "restart-open-failure") {
            const running = yield* sink
              .stream({ threadId, afterSequence: yield* sink.latestSequence({ threadId }) })
              .pipe(
                Stream.filter(
                  ({ event }) =>
                    event.type === "provider-turn.updated" && event.payload.status === "running",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
            yield* dispatch("delivered", "Already delivered", "running");
            yield* worker.drain();
            expect(delivered).toEqual(["Already delivered"]);
            yield* Fiber.join(running);
          }
          yield* dispatch(
            "missed",
            "Missed task exactly\n  keep spaces",
            scenario === "stop" ? "complete" : "fail-open",
            scenario === "restart-open-failure",
          );
          if (scenario === "stop") {
            const starting = yield* orchestrator.getThreadProjection(threadId);
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make(`${name}:stop`),
              threadId,
              runId: starting.runs[0]!.id,
            });
          }
          yield* worker.drain();
          if (scenario !== "stop") expect(failedOpen).toBe(true);
          const stopped = yield* orchestrator.getThreadProjection(threadId);
          expect(stopped.runs[0]?.status).toBe(scenario === "stop" ? "interrupted" : "failed");
          expect(
            stopped.turnItems.some(
              (item) => item.type === (scenario === "stop" ? "run_interrupt_result" : "error"),
            ),
          ).toBe(true);
          expect(stopped.providerTurns).toHaveLength(scenario === "restart-open-failure" ? 1 : 0);
          if (scenario === "restart-open-failure") expect(stopped.attempts).toHaveLength(2);
          const completed = yield* sink
            .stream({ threadId, afterSequence: yield* sink.latestSequence({ threadId }) })
            .pipe(
              Stream.filter(
                ({ event }) => event.type === "run.updated" && event.payload.status === "waiting",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
          yield* dispatch("next", "Next task", "complete");
          yield* worker.drain();
          expect(delivered).toEqual([
            ...(scenario === "restart-open-failure" ? ["Already delivered"] : []),
            "Missed task exactly\n  keep spaces\n\nNext task",
          ]);
          yield* Fiber.join(completed);
          yield* worker.drain();
          yield* dispatch("following", "Following task", "complete");
          yield* worker.drain();
          expect(delivered.at(-1)).toBe("Following task");
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name },
              ProviderAdapterRegistry.layerSingle(adapter),
              { runEffectWorker: false, effectWorkerOptions: { maxAttempts: 1 } },
            ),
          ),
        );
      }),
    ),
);
