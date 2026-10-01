// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  CommandId,
  MessageId,
  type ServerSettings,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import { makeWightMode } from "./wightMode.ts";
import * as Option from "effect/Option";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { OrchestrationCommandReceiptRepository } from "../../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  createParent,
  temporaryDirectory,
  withEngineOnly,
  PARENT_ID,
  NOW,
  commandId,
  session,
} from "./handlers.testFixtures.ts";

const later = "2026-01-01T00:10:00.000Z";
const readShell = Effect.gen(function* () {
  const query = yield* ProjectionSnapshotQuery;
  return Option.getOrThrow(yield* query.getThreadShellById(PARENT_ID));
});
const nudge = (shell: Effect.Success<typeof readShell>, sequence: number, at = later) => ({
  type: "thread.turn.start" as const,
  commandId: CommandId.make(
    `server:wight:${PARENT_ID}:${NOW}:${shell.updatedAt}:${shell.latestTurn?.turnId ?? "initial"}:${sequence}`,
  ),
  threadId: PARENT_ID,
  idleGuard: { latestTurnId: shell.latestTurn?.turnId ?? null, updatedAt: shell.updatedAt },
  message: {
    messageId: MessageId.make(`wight-message-${sequence}`),
    role: "user" as const,
    text: "Continue.",
    attachments: [],
  },
  runtimeMode: shell.runtimeMode,
  interactionMode: shell.interactionMode,
  createdAt: at,
});

describe("automatic idle turn admission on real SQLite engine", () => {
  it.effect(
    "queued nudges recheck toggle, slider, usage and timer at admission without burning retries",
    () =>
      Effect.gen(function* () {
        for (const change of ["toggle", "slider", "usage", "timer"] as const) {
          const directory = yield* temporaryDirectory("t3-wight-capacity-");
          yield* withEngineOnly(
            NodePath.join(directory, "state.sqlite"),
            Effect.gen(function* () {
              yield* createParent(directory);
              const engine = yield* OrchestrationEngineService;
              const receipts = yield* OrchestrationCommandReceiptRepository;
              const shell = yield* readShell;
              const query = yield* ProjectionSnapshotQuery;
              const instance = shell.modelSelection.instanceId;
              const enabled: ServerSettings = {
                ...DEFAULT_SERVER_SETTINGS,
                wightModes: { [PARENT_ID]: { enabledAt: NOW, expiresAt: null } },
              };
              let settings = enabled;
              let usage = 20;
              const admitting = yield* Deferred.make<void>();
              const release = yield* Deferred.make<void>();
              let gate = true;
              const ids: CommandId[] = [];
              const runtime = yield* makeWightMode({
                settings: Effect.sync(() => settings),
                providers: Effect.sync(() => [
                  {
                    instanceId: instance,
                    enabled: true,
                    usageLimits: {
                      checkedAt: NOW,
                      windows: [
                        { id: "session", kind: "session", label: "Session", usedPercent: usage },
                      ],
                    },
                  } as unknown as ServerProvider,
                ]),
                thread: () =>
                  readShell.pipe(
                    Effect.provideService(ProjectionSnapshotQuery, query),
                    Effect.orDie,
                  ),
                resume: (thread, _text, _activation, admission) =>
                  Effect.gen(function* () {
                    const command = nudge(thread, yield* engine.latestSequence);
                    ids.push(command.commandId);
                    yield* engine.dispatch(command, {
                      idleAdmission: Effect.gen(function* () {
                        // This executes only after Queue.offer, inside the serialized engine worker.
                        if (gate) {
                          yield* Deferred.succeed(admitting, undefined);
                          yield* Deferred.await(release);
                        }
                        return yield* admission;
                      }),
                    });
                  }).pipe(Effect.orDie),
              });
              const queued = yield* Effect.forkChild(runtime.reconcile());
              yield* Deferred.await(admitting);
              if (change === "toggle") settings = DEFAULT_SERVER_SETTINGS;
              if (change === "slider")
                settings = {
                  ...enabled,
                  providerInstances: {
                    [instance]: { driver: ProviderDriverKind.make("codex"), wightLimitPercent: 10 },
                  },
                };
              if (change === "usage") usage = 90;
              if (change === "timer")
                settings = {
                  ...enabled,
                  wightModes: {
                    [PARENT_ID]: { enabledAt: NOW, expiresAt: 0 },
                  },
                };
              yield* Deferred.succeed(release, undefined);
              yield* Fiber.join(queued);
              expect(
                Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
                change,
              ).toHaveLength(0);
              expect(
                Option.isNone(yield* receipts.getByCommandId({ commandId: ids[0]! })),
                change,
              ).toBe(true);
              settings = enabled;
              usage = 20;
              gate = false;
              yield* runtime.reconcile();
              yield* runtime.reconcile();
              expect(ids[1], change).toBe(ids[0]);
              expect(
                Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
                change,
              ).toHaveLength(1);
            }),
          );
        }
      }).pipe(Effect.scoped),
  );

  it.effect(
    "a stale no-op receipt leaves the next idle revision eligible for exactly one nudge",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-wight-admission-");
        yield* withEngineOnly(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            const receipts = yield* OrchestrationCommandReceiptRepository;
            const stale = yield* readShell;
            yield* engine.dispatch({
              type: "thread.meta.update",
              commandId: commandId(),
              threadId: PARENT_ID,
              title: "New title",
            });
            // Production reads the shell, then samples sequence in resume after this mutation.
            const staleCommand = nudge(stale, yield* engine.latestSequence);
            yield* engine.dispatch(staleCommand);
            expect(
              Option.getOrThrow(
                yield* receipts.getByCommandId({ commandId: staleCommand.commandId }),
              ).status,
            ).toBe("accepted");
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
            ).toHaveLength(0);
            const fresh = nudge(yield* readShell, yield* engine.latestSequence);
            expect(fresh.commandId).not.toBe(staleCommand.commandId);
            yield* engine.dispatch(fresh);
            yield* engine.dispatch(fresh);
            const detail = Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID));
            expect(detail.messages.map((message) => message.text)).toEqual(["Continue."]);
          }),
        );
      }).pipe(Effect.scoped),
  );

  it.effect(
    "pending starts block nudges even after the two-minute heuristic expires, including across restart",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-wight-pending-");
        const db = NodePath.join(directory, "state.sqlite");
        yield* withEngineOnly(
          db,
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            const { idleGuard: _guard, ...manual } = nudge(
              yield* readShell,
              yield* engine.latestSequence,
              NOW,
            );
            yield* engine.dispatch(manual);
            const current = yield* readShell;
            yield* engine.dispatch(nudge(current, yield* engine.latestSequence, later));
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
            ).toHaveLength(1);
          }),
        );
        yield* withEngineOnly(
          db,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            yield* engine.dispatch({
              type: "thread.meta.update",
              commandId: commandId(),
              threadId: PARENT_ID,
              title: "Restarted",
            });
            yield* engine.dispatch(nudge(yield* readShell, yield* engine.latestSequence, later));
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
            ).toHaveLength(1);
          }),
        );
      }).pipe(Effect.scoped),
  );

  it.effect("running turns reject stale automatic starts", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-wight-running-");
      yield* withEngineOnly(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          const engine = yield* OrchestrationEngineService;
          const stale = yield* readShell;
          yield* engine.dispatch(session(PARENT_ID, "running", "turn-active"));
          yield* engine.dispatch(nudge(stale, yield* engine.latestSequence));
          yield* engine.dispatch(nudge(yield* readShell, (yield* engine.latestSequence) + 1));
          const query = yield* ProjectionSnapshotQuery;
          expect(
            Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
          ).toHaveLength(0);
        }),
      );
    }).pipe(Effect.scoped),
  );
});
