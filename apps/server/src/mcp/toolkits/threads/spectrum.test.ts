// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import {
  CheckpointRef,
  CommandId,
  EventId,
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSession,
  ThreadId,
  TurnId,
  type OrchestrationEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ProviderCommandReactor } from "../../../orchestration/Services/ProviderCommandReactor.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
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
  temporaryDirectory,
  withEngineOnly,
  withServer,
} from "./handlers.testFixtures.ts";
import { makeSpectrum, SpectrumState } from "./spectrum.ts";
import { StartSpectrumInput } from "./spectrumTools.ts";

const decodeInput = Schema.decodeUnknownSync(StartSpectrumInput);
const decodeState = Schema.decodeUnknownSync(SpectrumState);

const input = (mode: "council" | "free", limit?: number) =>
  decodeInput({
    question: "Which option works?",
    mode,
    limit,
    colors: [
      { label: "Blue", model: "blue-model" },
      { label: "Red", model: "red-model" },
    ],
  });
const state = (id: string) =>
  Effect.gen(function* () {
    const query = yield* ProjectionSnapshotQuery;
    const rows = yield* query.listActivitiesByKind("spectrum.state");
    const values = rows.map((row) => decodeState(row.payload)).filter((value) => value.id === id);
    return values.toSorted((a, b) => b.revision - a.revision)[0]!;
  });
const threadShell = (id: string) =>
  Effect.gen(function* () {
    const query = yield* ProjectionSnapshotQuery;
    return Option.getOrThrow(yield* query.getThreadShellById(ThreadId.make(id)));
  });
const detail = (id: string) =>
  Effect.gen(function* () {
    const query = yield* ProjectionSnapshotQuery;
    return Option.getOrThrow(yield* query.getThreadDetailById(ThreadId.make(id)));
  });
const isState =
  (id: string, test: (value: SpectrumState) => boolean) => (event: OrchestrationEvent) =>
    event.aggregateId === id &&
    event.type === "thread.activity-appended" &&
    event.payload.activity.kind === "spectrum.state" &&
    test(decodeState(event.payload.activity.payload));
const nextTurn = (id: string) => (event: OrchestrationEvent) =>
  event.type === "thread.turn-start-requested" && event.aggregateId === id;
const settled = (id: string) => (event: OrchestrationEvent) =>
  event.type === "thread.settled" && event.aggregateId === id;
const shared = (id: string, suffix: string) => (event: OrchestrationEvent) =>
  event.type === "thread.message-sent" &&
  event.aggregateId === id &&
  !event.payload.streaming &&
  event.payload.messageId.endsWith(suffix);

const bind = (threadId: string, messageId: string, turnId: string) =>
  ({
    type: "thread.activity.append",
    commandId: commandId(),
    threadId: ThreadId.make(threadId),
    activity: {
      id: EventId.make(`bind:${messageId}:${turnId}`),
      kind: "spectrum.turn-bound",
      summary: "Turn bound",
      tone: "info",
      turnId: TurnId.make(turnId),
      payload: { messageId, turnId },
      createdAt: NOW,
    },
    createdAt: NOW,
  }) as const;
const reply = (threadId: string, turnId: string, messageId: string, text: string) =>
  [
    {
      type: "thread.message.assistant.delta",
      commandId: commandId(),
      threadId: ThreadId.make(threadId),
      turnId: TurnId.make(turnId),
      messageId: MessageId.make(messageId),
      delta: text,
      createdAt: NOW,
    },
    {
      type: "thread.message.assistant.complete",
      commandId: commandId(),
      threadId: ThreadId.make(threadId),
      turnId: TurnId.make(turnId),
      messageId: MessageId.make(messageId),
      createdAt: NOW,
    },
  ] as const;
const answer = (pending: SpectrumState["pending"][number], turnId: string, texts: string[]) =>
  dispatchAll([
    bind(pending.threadId, pending.messageId, turnId),
    session(ThreadId.make(pending.threadId), "running", turnId),
    ...texts.flatMap((text, index) => reply(pending.threadId, turnId, `${turnId}-m${index}`, text)),
    session(ThreadId.make(pending.threadId), "ready", null),
  ]);
const sendUser = (id: string, text: string) =>
  dispatchAll([
    {
      type: "thread.turn.start",
      commandId: commandId(),
      threadId: ThreadId.make(id),
      message: {
        messageId: MessageId.make(`user-${commandId()}`),
        role: "user",
        text,
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: NOW,
    },
  ]);
const scenario = <E>(body: (directory: string, database: string) => Effect.Effect<void, E>) =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory("spectrum-");
    yield* body(directory, NodePath.join(directory, "state.sqlite"));
  }).pipe(Effect.scoped);

describe("Spectrum server orchestration", () => {
  it.effect(
    "relays every full message, waits for all Colors in two rounds, then synthesizes without a Spectrum provider",
    () =>
      scenario((directory, database) =>
        withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const started = yield* callTool("start_spectrum", input("council"));
            const id = started.threadId;
            const long = `BEGIN\n${"verbatim 🟦\n".repeat(700)}END`;
            let current = yield* state(id);
            const blue = current.pending[0]!;
            yield* dispatchUntil(
              answer(blue, "blue-first", [long, "Second separate message"]),
              shared(id, "blue-first-m1"),
            );
            current = yield* state(id);
            expect(current.step).toBe(0);
            expect(current.pending[1]!.done).toBe(false);
            expect(
              (yield* detail(current.pending[1]!.threadId)).messages.filter(
                (message) => message.role === "user",
              ),
            ).toHaveLength(1);
            yield* dispatchUntil(
              answer(current.pending[1]!, "red-first", ["Red independent"]),
              nextTurn(blue.threadId),
            );
            current = yield* state(id);
            expect(current.step).toBe(1);
            const prompt = (yield* detail(blue.threadId)).messages.find(
              (message) => message.id === current.pending[0]!.messageId,
            )!.text;
            expect(prompt).toContain(long);
            expect(prompt).toContain("Second separate message");
            expect(prompt).toContain("[Color: Red]\nRed independent");
            for (const round of [1, 2]) {
              current = yield* state(id);
              yield* dispatchUntil(
                answer(current.pending[0]!, `blue-round-${round}`, [`Blue round ${round}`]),
                shared(id, `blue-round-${round}-m0`),
              );
              expect((yield* state(id)).step).toBe(round);
              yield* dispatchUntil(
                answer(current.pending[1]!, `red-round-${round}`, [`Red round ${round}`]),
                nextTurn(blue.threadId),
              );
            }
            current = yield* state(id);
            expect(current.step).toBe(3);
            expect(current.pending).toHaveLength(1);
            const synthesisPrompt = (yield* detail(blue.threadId)).messages.find(
              (message) => message.id === current.pending[0]!.messageId,
            )!.text;
            expect(synthesisPrompt).toContain("Synthesize");
            expect(synthesisPrompt).toContain("Red round 2");
            yield* dispatchUntil(
              answer(current.pending[0]!, "synthesis", [long, "Final conclusion"]),
              settled(id),
            );
            const spectrum = yield* detail(id);
            expect(spectrum.session).toBeNull();
            expect(spectrum.latestTurn).toBeNull();
            expect(spectrum.settledAt).not.toBeNull();
            expect(
              spectrum.messages.some((message) => message.text === `[Color: Blue]\n${long}`),
            ).toBe(true);
            const reports = (yield* parentMessages).filter((message) =>
              message.text.startsWith("[Spectrum"),
            );
            expect(reports).toHaveLength(1);
            expect(reports[0]!.text).toContain(long);
            expect(reports[0]!.text).toContain("Final conclusion");
          }),
        ),
      ),
  );

  it.effect("rejects stale/wrong turn replies and out-of-order free speakers", () =>
    scenario((directory, database) =>
      withServer(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const started = yield* callTool("start_spectrum", input("free", 3));
          let current = yield* state(started.threadId);
          const blue = current.pending[0]!;
          const red = started.participants[1]!.threadId;
          yield* dispatchUntil(
            dispatchAll([
              session(ThreadId.make(blue.threadId), "running", "stale"),
              ...reply(blue.threadId, "stale", "stale-msg", "STALE"),
              bind(blue.threadId, "wrong-request", "stale"),
              session(ThreadId.make(blue.threadId), "ready", null),
              session(ThreadId.make(red), "running", "red-too-soon"),
              ...reply(red, "red-too-soon", "wrong-order", "OUT OF ORDER"),
              session(ThreadId.make(red), "ready", null),
              // A state receipt from a user send proves all earlier events have been considered.
            ]).pipe(Effect.andThen(sendUser(started.threadId, "Keep going"))),
            isState(started.threadId, (value) => value.users.length === 1),
          );
          current = yield* state(started.threadId);
          expect(current.step).toBe(0);
          expect(current.pending[0]!.done).toBe(false);
          expect(
            (yield* detail(started.threadId)).messages.some((message) =>
              /STALE|OUT OF ORDER/.test(message.text),
            ),
          ).toBe(false);
          yield* dispatchUntil(answer(current.pending[0]!, "blue-0", ["Blue zero"]), nextTurn(red));
          current = yield* state(started.threadId);
          expect(current.phase).toBe("broadcast");
          for (const [index, pending] of current.pending.entries()) {
            yield* dispatchUntil(
              answer(pending, `broadcast-${index}`, ["Acknowledged"]),
              index === 0 ? shared(started.threadId, `broadcast-${index}-m0`) : nextTurn(red),
            );
          }
          current = yield* state(started.threadId);
          expect(current.step).toBe(1);
          expect(current.pending[0]!.threadId).toBe(red);
          yield* dispatchUntil(
            answer(current.pending[0]!, "red-1", ["Red one"]),
            nextTurn(blue.threadId),
          );
          current = yield* state(started.threadId);
          expect(current.step).toBe(2);
          expect(current.pending[0]!.threadId).toBe(blue.threadId);
          yield* dispatchUntil(
            answer(current.pending[0]!, "blue-2", ["Blue two"]),
            settled(started.threadId),
          );
          expect((yield* state(started.threadId)).status).toBe("settled");
        }),
      ),
    ),
  );

  it.effect(
    "broadcasts User to every provider after the barrier, preserves it when reopening council",
    () =>
      scenario((directory, database) =>
        withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const started = yield* callTool("start_spectrum", input("council"));
            let current = yield* state(started.threadId);
            yield* dispatchUntil(
              callTool("message_thread", {
                threadId: started.threadId,
                text: "USER EXACT\nChoose the safe option.",
                scope: "project",
              }),
              isState(started.threadId, (value) => value.users.length === 1),
            );
            expect((yield* state(started.threadId)).step).toBe(0);
            expect((yield* state(started.threadId)).generation).toBe(1);
            yield* dispatchUntil(
              answer(current.pending[0]!, "b0", ["Blue first"]),
              shared(started.threadId, "b0-m0"),
            );
            yield* dispatchUntil(
              answer(current.pending[1]!, "r0", ["Red first"]),
              nextTurn(current.pending[1]!.threadId),
            );
            current = yield* state(started.threadId);
            expect(current.phase).toBe("broadcast");
            for (const pending of current.pending) {
              const prompt = (yield* detail(pending.threadId)).messages.find(
                (message) => message.id === pending.messageId,
              )!.text;
              expect(prompt).toContain("[User]\n[Message from Parent");
              expect(prompt).toContain("USER EXACT\nChoose the safe option.");
            }
            for (const [index, pending] of current.pending.entries())
              yield* dispatchUntil(
                answer(pending, `ack-${index}`, ["User received"]),
                index === 0
                  ? shared(started.threadId, `ack-${index}-m0`)
                  : nextTurn(current.pending[1]!.threadId),
              );
            for (const round of [1, 2]) {
              current = yield* state(started.threadId);
              yield* dispatchUntil(
                answer(current.pending[0]!, `b${round}`, ["Blue"]),
                shared(started.threadId, `b${round}-m0`),
              );
              yield* dispatchUntil(
                answer(current.pending[1]!, `r${round}`, ["Red"]),
                nextTurn(current.pending[0]!.threadId),
              );
            }
            current = yield* state(started.threadId);
            yield* dispatchUntil(
              answer(current.pending[0]!, "final", ["Prior synthesis"]),
              settled(started.threadId),
            );
            yield* dispatchUntil(
              sendUser(started.threadId, "Now consider costs."),
              nextTurn(current.pending[1]?.threadId ?? started.participants[1]!.threadId),
            );
            const reopened = yield* state(started.threadId);
            expect(reopened.cycle).toBe(1);
            expect(reopened.status).toBe("active");
            const prompt = (yield* detail(reopened.pending[0]!.threadId)).messages.find(
              (message) => message.id === reopened.pending[0]!.messageId,
            )!.text;
            expect(prompt).toContain("Prior synthesis");
            expect(prompt).toContain("Now consider costs.");
            expect((yield* detail(started.threadId)).latestTurn).toBeNull();
          }),
        ),
      ),
  );

  it.effect("settles and reports a participant failure instead of hanging", () =>
    scenario((directory, database) =>
      withServer(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const started = yield* callTool("start_spectrum", input("free", 2));
          const pending = (yield* state(started.threadId)).pending[0]!;
          yield* dispatchUntil(
            dispatchAll([
              {
                type: "thread.activity.append",
                commandId: commandId(),
                threadId: ThreadId.make(pending.threadId),
                activity: {
                  id: EventId.make("start-failed"),
                  kind: "provider.turn.start.failed",
                  summary: "Failed",
                  tone: "error",
                  turnId: null,
                  payload: { requestId: pending.messageId, detail: "No capacity" },
                  createdAt: NOW,
                },
                createdAt: NOW,
              },
            ]),
            settled(started.threadId),
          );
          expect((yield* state(started.threadId)).status).toBe("settled");
          expect(
            (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
          ).toHaveLength(1);
          expect((yield* parentMessages).at(-1)!.text).toContain("No capacity");
        }),
      ),
    ),
  );

  it.effect(
    "puts Prism kit instructions in the actual first provider prompt and reserves User attribution",
    () =>
      scenario((directory, database) =>
        withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const settings = yield* ServerSettingsService;
            yield* settings.updateSettings({
              prismRoles: { reviewer: { instructions: "KIT EXACT", skills: ["proof-skill"] } },
            });
            const value = input("free", 2);
            const started = yield* callTool("start_spectrum", {
              ...value,
              colors: [
                { label: "Review", role: "reviewer", model: "review-model" },
                { label: "Fallback", role: "reviewer" },
              ],
            });
            const pending = (yield* state(started.threadId)).pending[0]!;
            const prompt = (yield* detail(pending.threadId)).messages.find(
              (message) => message.id === pending.messageId,
            )!.text;
            expect(prompt).toContain("KIT EXACT");
            expect(prompt).toContain("proof-skill");
            expect(started.participants[1]!.model).toBe("gpt-5");
            yield* dispatchUntil(
              answer(pending, "first-kit-turn", ["First Color"]),
              nextTurn(started.participants[1]!.threadId),
            );
            const second = (yield* state(started.threadId)).pending[0]!;
            const secondPrompt = (yield* detail(second.threadId)).messages.find(
              (message) => message.id === second.messageId,
            )!.text;
            expect(secondPrompt).toContain("KIT EXACT");
            expect(secondPrompt).toContain("proof-skill");
            const rejected = yield* Effect.exit(
              callTool("start_spectrum", {
                ...value,
                colors: [{ label: "User" }, value.colors[1]!],
              }),
            );
            expect(rejected._tag).toBe("Failure");
          }),
        ),
      ),
  );

  it.effect("recovers a crash after initial state, before participant creation or scheduling", () =>
    scenario((directory, database) =>
      Effect.gen(function* () {
        const id = "spectrum.initial-crash";
        yield* withEngineOnly(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const parent = yield* detail(PARENT_ID);
            const selection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" };
            const engine = yield* OrchestrationEngineService;
            const create = (threadId: string) => ({
              type: "thread.create" as const,
              commandId: CommandId.make(`create:${threadId}`),
              threadId: ThreadId.make(threadId),
              projectId: parent.projectId,
              title: "Crash test",
              modelSelection: selection,
              runtimeMode: "full-access" as const,
              interactionMode: "default" as const,
              branch: null,
              worktreePath: null,
              createdAt: NOW,
            });
            yield* engine.dispatch(create(id));
            const initial: SpectrumState = {
              id,
              callerId: PARENT_ID,
              question: "Crash question",
              mode: "council",
              limit: 2,
              moderator: 0,
              cycle: 0,
              step: -1,
              revision: 1,
              cursor: yield* engine.latestSequence,
              status: "active",
              phase: "discussion",
              generation: 0,
              deliveredUsers: 0,
              participants: [0, 1].map((index) => ({
                threadId: `sub.${id}.${index}`,
                label: `Color ${index}`,
                selection,
                instructions: "",
              })),
              pending: [],
              transcript: [],
              users: [],
              outbox: [create(`sub.${id}.0`), create(`sub.${id}.1`)],
            };
            yield* engine.dispatch({
              type: "thread.activity.append",
              commandId: CommandId.make("crash-state"),
              threadId: ThreadId.make(id),
              activity: {
                id: EventId.make("crash-state"),
                kind: "spectrum.state",
                summary: "Initialized",
                payload: initial,
                tone: "info",
                turnId: null,
                createdAt: NOW,
              },
              createdAt: NOW,
            });
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            yield* dispatchUntil(Effect.void, nextTurn(`sub.${id}.1`));
            const recovered = yield* state(id);
            expect(recovered.step).toBe(0);
            expect(recovered.pending).toHaveLength(2);
            expect(recovered.status).toBe("active");
            expect(
              (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
            ).toHaveLength(0);
            expect((yield* detail(id)).settledAt).toBeNull();
          }),
        );
      }),
    ),
  );

  it.effect(
    "waits for every completed message and can bind after a fast turn has already ended",
    () =>
      scenario((directory, database) =>
        withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const started = yield* callTool("start_spectrum", input("free", 1));
            const pending = (yield* state(started.threadId)).pending[0]!;
            yield* dispatchAll([
              session(ThreadId.make(pending.threadId), "running", "fast"),
              ...reply(pending.threadId, "fast", "z-first-complete", "First"),
              {
                type: "thread.message.assistant.delta",
                commandId: commandId(),
                threadId: ThreadId.make(pending.threadId),
                turnId: TurnId.make("fast"),
                messageId: MessageId.make("a-second-open"),
                delta: "Second full",
                createdAt: NOW,
              },
              session(ThreadId.make(pending.threadId), "ready", null),
            ]);
            yield* dispatchUntil(
              dispatchAll([bind(pending.threadId, pending.messageId, "fast")]),
              isState(started.threadId, (value) => value.pending[0]!.turnId === "fast"),
            );
            expect((yield* state(started.threadId)).status).toBe("active");
            yield* dispatchUntil(
              dispatchAll([
                {
                  type: "thread.message.assistant.complete",
                  commandId: commandId(),
                  threadId: ThreadId.make(pending.threadId),
                  turnId: TurnId.make("fast"),
                  messageId: MessageId.make("a-second-open"),
                  createdAt: NOW,
                },
              ]),
              settled(started.threadId),
            );
            const report = (yield* parentMessages).find((message) =>
              message.text.startsWith("[Spectrum"),
            )!;
            expect(report.text).toContain("First");
            expect(report.text).toContain("Second full");
            expect(report.text.indexOf("First")).toBeLessThan(report.text.indexOf("Second full"));
          }),
        ),
      ),
  );

  it.effect(
    "delivers User arriving during synthesis to all Colors and synthesizes again before reporting",
    () =>
      scenario((directory, database) =>
        withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const started = yield* callTool("start_spectrum", input("council"));
            for (const round of [0, 1, 2]) {
              const current = yield* state(started.threadId);
              yield* dispatchUntil(
                answer(current.pending[0]!, `late-blue-${round}`, ["Blue answer"]),
                shared(started.threadId, `late-blue-${round}-m0`),
              );
              yield* dispatchUntil(
                answer(current.pending[1]!, `late-red-${round}`, ["Red answer"]),
                nextTurn(current.pending[0]!.threadId),
              );
            }
            const synthesis = yield* state(started.threadId);
            expect(synthesis.step).toBe(3);
            yield* dispatchUntil(
              sendUser(started.threadId, "Late User constraint"),
              isState(started.threadId, (value) => value.users.length === 1),
            );
            yield* dispatchUntil(
              answer(synthesis.pending[0]!, "old-synthesis", ["OUTDATED SYNTHESIS"]),
              nextTurn(started.participants[1]!.threadId),
            );
            const broadcast = yield* state(started.threadId);
            expect(broadcast.phase).toBe("broadcast");
            expect(
              (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
            ).toHaveLength(0);
            for (const [index, pending] of broadcast.pending.entries())
              yield* dispatchUntil(
                answer(pending, `late-ack-${index}`, ["Received late User constraint"]),
                index === 0
                  ? shared(started.threadId, `late-ack-${index}-m0`)
                  : nextTurn(started.participants[0]!.threadId),
              );
            const repeated = yield* state(started.threadId);
            expect(repeated.phase).toBe("discussion");
            expect(repeated.step).toBe(3);
            expect(repeated.pending[0]!.messageId).not.toBe(synthesis.pending[0]!.messageId);
            const prompt = (yield* detail(repeated.pending[0]!.threadId)).messages.find(
              (message) => message.id === repeated.pending[0]!.messageId,
            )!.text;
            expect(prompt).toContain("[User]\nLate User constraint");
            yield* dispatchUntil(
              answer(repeated.pending[0]!, "new-synthesis", ["UPDATED SYNTHESIS"]),
              settled(started.threadId),
            );
            const report = (yield* parentMessages).find((message) =>
              message.text.startsWith("[Spectrum"),
            )!;
            expect(report.text).toContain("UPDATED SYNTHESIS");
            expect(report.text).not.toContain("OUTDATED SYNTHESIS");
          }),
        ),
      ),
  );

  it.effect("manual reopening retains the transcript and reports the next bounded cycle once", () =>
    scenario((directory, database) =>
      withServer(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const started = yield* callTool("start_spectrum", input("free", 1));
          const pending = (yield* state(started.threadId)).pending[0]!;
          yield* dispatchUntil(
            answer(pending, "cycle-zero", ["ORIGINAL TRANSCRIPT"]),
            settled(started.threadId),
          );
          yield* dispatchUntil(
            dispatchAll([
              {
                type: "thread.unsettle",
                commandId: commandId(),
                threadId: ThreadId.make(started.threadId),
                reason: "user",
              },
            ]),
            nextTurn(pending.threadId),
          );
          const reopened = yield* state(started.threadId);
          expect(reopened.cycle).toBe(1);
          const prompt = (yield* detail(pending.threadId)).messages.find(
            (message) => message.id === reopened.pending[0]!.messageId,
          )!.text;
          expect(prompt).toContain("ORIGINAL TRANSCRIPT");
          yield* dispatchUntil(
            answer(reopened.pending[0]!, "cycle-one", ["CONTINUED TRANSCRIPT"]),
            settled(started.threadId),
          );
          expect(
            (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
          ).toHaveLength(2);
          const sharedMessages = (yield* detail(started.threadId)).messages;
          expect(
            sharedMessages.some((message) => message.text === "[Color: Blue]\nORIGINAL TRANSCRIPT"),
          ).toBe(true);
          expect(
            sharedMessages.some(
              (message) => message.text === "[Color: Blue]\nCONTINUED TRANSCRIPT",
            ),
          ).toBe(true);
        }),
      ),
    ),
  );

  it.effect("recovers exact turn identity and relays a finished turn once across restarts", () =>
    scenario((directory, database) =>
      Effect.gen(function* () {
        const started = yield* withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const started = yield* callTool("start_spectrum", input("free", 1));
            const pending = (yield* state(started.threadId)).pending[0]!;
            yield* dispatchUntil(
              dispatchAll([bind(pending.threadId, pending.messageId, "real-turn")]),
              isState(started.threadId, (value) => value.pending[0]!.turnId === "real-turn"),
            );
            return { ...started, pending };
          }),
        );
        yield* withEngineOnly(
          database,
          dispatchAll([
            session(ThreadId.make(started.pending.threadId), "running", "real-turn"),
            ...reply(started.pending.threadId, "wrong-turn", "wrong-final", "WRONG"),
            ...reply(started.pending.threadId, "real-turn", "real-a", "A".repeat(5001)),
            ...reply(started.pending.threadId, "real-turn", "real-b", "Second"),
            session(ThreadId.make(started.pending.threadId), "ready", null),
          ]),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            yield* dispatchUntil(Effect.void, settled(started.threadId));
            expect(
              (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
            ).toHaveLength(1);
            expect(
              (yield* detail(started.threadId)).messages.some((message) =>
                message.text.includes("WRONG"),
              ),
            ).toBe(false);
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            // A fresh tool call is unrelated to Spectrum and gives its startup recovery a receipt barrier.
            yield* dispatchUntil(
              callTool("spawn_thread", { task: "Normal child", reportBack: false }),
              (event) =>
                event.type === "thread.turn-start-requested" &&
                event.aggregateId.startsWith(`sub.${PARENT_ID}.`),
            );
            expect(
              (yield* parentMessages).filter((message) => message.text.startsWith("[Spectrum")),
            ).toHaveLength(1);
            expect(
              (yield* detail(started.threadId)).messages.filter((message) =>
                message.text.includes("A".repeat(5001)),
              ),
            ).toHaveLength(1);
          }),
        );
      }),
    ),
  );
});

// Drive committed engine events explicitly: no fake projections or timing waits.
const feed = (spectrum: Effect.Success<ReturnType<typeof makeSpectrum>>, from: number) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const events = yield* engine.readEvents(from, 100_000).pipe(Stream.runCollect);
    for (const event of events) yield* spectrum.onEvent(event);
  });
const checkpoint = (threadId: string, turnId: string, count: number) => ({
  type: "thread.turn.diff.complete" as const,
  commandId: commandId(),
  threadId: ThreadId.make(threadId),
  turnId: TurnId.make(turnId),
  completedAt: NOW,
  checkpointRef: CheckpointRef.make(`refs/checkpoints/${count}`),
  status: "ready" as const,
  files: [],
  checkpointTurnCount: count,
  createdAt: NOW,
});

describe("Spectrum lifecycle regressions", () => {
  it.effect("collects an exact finished turn despite a late previous-turn checkpoint", () =>
    scenario((directory, database) =>
      withEngineOnly(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const engine = yield* OrchestrationEngineService;
          const spectrum = yield* makeSpectrum(() => "reasoningEffort");
          const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("free", 1));
          yield* feed(spectrum, 0);
          const pending = (yield* state(started.threadId)).pending[0]!;
          const from = yield* engine.latestSequence;
          yield* dispatchAll([
            bind(pending.threadId, pending.messageId, "current"),
            session(ThreadId.make(pending.threadId), "running", "current"),
            checkpoint(pending.threadId, "previous", 1),
            ...reply(pending.threadId, "current", "current-message", "Full current reply"),
            session(ThreadId.make(pending.threadId), "ready", null),
          ]);
          expect((yield* detail(pending.threadId)).latestTurn?.turnId).toBe("previous");
          yield* feed(spectrum, from);
          expect((yield* state(started.threadId)).status).toBe("settled");
          expect(
            (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
          ).toHaveLength(1);
          const head = yield* engine.latestSequence;
          yield* dispatchAll([checkpoint(pending.threadId, "current", 2)]);
          yield* feed(spectrum, head);
          expect(
            (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
          ).toHaveLength(1);
        }),
      ),
    ),
  );

  it.effect(
    "stops an uncertain request-to-binding restart without duplicating the provider request",
    () =>
      scenario((directory, database) =>
        Effect.gen(function* () {
          const started = yield* withEngineOnly(
            database,
            Effect.gen(function* () {
              yield* createParent(directory);
              const spectrum = yield* makeSpectrum(() => "reasoningEffort");
              const started = yield* spectrum.start(
                yield* threadShell(PARENT_ID),
                input("free", 1),
              );
              yield* feed(spectrum, 0);
              expect((yield* state(started.threadId)).pending[0]).toMatchObject({
                requested: true,
                turnId: null,
              });
              return started;
            }),
          );
          yield* withEngineOnly(
            database,
            Effect.gen(function* () {
              const engine = yield* OrchestrationEngineService;
              const spectrum = yield* makeSpectrum(() => "reasoningEffort");
              yield* spectrum.recover;
              expect((yield* state(started.threadId)).status).toBe("settled");
              const events = yield* engine.readEvents(0, 100_000).pipe(Stream.runCollect);
              expect(
                events.filter(
                  (e) =>
                    e.type === "thread.turn-start-requested" &&
                    e.aggregateId === started.participants[0]!.threadId,
                ),
              ).toHaveLength(1);
              expect(
                (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
              ).toHaveLength(1);
              yield* spectrum.recover;
              expect(
                (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
              ).toHaveLength(1);
            }),
          );
        }),
      ),
  );
});

const retire = (id: string) =>
  dispatchAll([
    {
      type: "thread.activity.append",
      commandId: CommandId.make(`server:mcp-threads-retire:${commandId()}`),
      threadId: ThreadId.make(id),
      activity: {
        id: EventId.make(`retire:${commandId()}`),
        kind: "thread.subtree-retire-requested",
        summary: "Retire",
        payload: {},
        tone: "info",
        turnId: null,
        createdAt: NOW,
      },
      createdAt: NOW,
    },
  ]);

it.effect.each(["participant", "spectrum", "caller"] as const)(
  "stops scheduling after %s retirement, including late replies and recovery",
  (target) =>
    scenario((directory, database) =>
      Effect.gen(function* () {
        const started = yield* withEngineOnly(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            const spectrum = yield* makeSpectrum(() => "reasoningEffort");
            const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("council"));
            yield* feed(spectrum, 0);
            const pending = (yield* state(started.threadId)).pending[0]!;
            const from = yield* engine.latestSequence;
            yield* dispatchAll([
              bind(pending.threadId, pending.messageId, "retiring-turn"),
              session(ThreadId.make(pending.threadId), "running", "retiring-turn"),
            ]);
            yield* retire(
              target === "participant"
                ? pending.threadId
                : target === "spectrum"
                  ? started.threadId
                  : PARENT_ID,
            );
            yield* feed(spectrum, from);
            expect((yield* state(started.threadId)).status).toBe("retired");
            const late = yield* engine.latestSequence;
            yield* dispatchAll(
              reply(pending.threadId, "retiring-turn", "late-retired", "Late reply"),
            );
            yield* feed(spectrum, late);
            expect((yield* state(started.threadId)).status).toBe("retired");
            expect((yield* detail(started.threadId)).settledAt).not.toBeNull();
            expect(
              (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
            ).toHaveLength(target === "participant" ? 1 : 0);
            const next = yield* engine.latestSequence;
            yield* engine.dispatch({
              type: "thread.unsettle",
              commandId: commandId(),
              threadId: ThreadId.make(started.threadId),
              reason: "user",
            });
            yield* feed(spectrum, next);
            expect((yield* state(started.threadId)).status).toBe("retired");
            return started;
          }),
        );
        yield* withEngineOnly(
          database,
          Effect.gen(function* () {
            const spectrum = yield* makeSpectrum(() => "reasoningEffort");
            yield* spectrum.recover;
            expect((yield* state(started.threadId)).status).toBe("retired");
            expect(
              (yield* parentMessages).filter((m) => m.text.startsWith("[Spectrum")),
            ).toHaveLength(target === "participant" ? 1 : 0);
          }),
        );
      }),
    ),
);

it.effect.each(["participant", "caller", "spectrum"] as const)(
  "rejects a delayed scheduler command after %s retirement and explicit recovery",
  (target) =>
    scenario((directory, database) =>
      withEngineOnly(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const engine = yield* OrchestrationEngineService;
          const spectrum = yield* makeSpectrum(() => "reasoningEffort");
          const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("free", 1));
          const child = started.participants[0]!.threadId;
          const targetId =
            target === "participant" ? child : target === "caller" ? PARENT_ID : started.threadId;
          yield* retire(targetId);
          const retirement = (yield* engine.getThreadRetirement(ThreadId.make(targetId)))!;
          yield* dispatchAll([
            {
              ...session(ThreadId.make(targetId), "interrupted", null),
              commandId: CommandId.make(retirement.stopAckCommandId),
            },
          ]);
          yield* sendUser(targetId, "Explicit recovery");
          expect((yield* engine.getThreadRetirement(ThreadId.make(targetId)))?.retired).toBe(false);
          const delayed = {
            type: "thread.turn.start" as const,
            commandId: CommandId.make(
              `server:spectrum:${started.threadId}:0:${PARENT_ID}:0:0:delayed`,
            ),
            threadId: ThreadId.make(child),
            message: {
              messageId: MessageId.make("delayed"),
              role: "user" as const,
              text: "Old cycle",
              attachments: [],
            },
            runtimeMode: "full-access" as const,
            interactionMode: "default" as const,
            createdAt: NOW,
          };
          const result = yield* engine.dispatch(delayed).pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          const events = yield* engine.readEvents(0, 100_000).pipe(Stream.runCollect);
          expect(
            events.some(
              (e) => e.type === "thread.turn-start-requested" && e.payload.messageId === "delayed",
            ),
          ).toBe(false);
        }),
      ),
    ),
);

it.effect("requires explicit recovery of retired Colors before explicitly reopening Spectrum", () =>
  scenario((directory, database) =>
    withEngineOnly(
      database,
      Effect.gen(function* () {
        yield* createParent(directory);
        const engine = yield* OrchestrationEngineService;
        const spectrum = yield* makeSpectrum(() => "reasoningEffort");
        const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("free", 1));
        yield* feed(spectrum, 0);
        const from = yield* engine.latestSequence;
        yield* retire(started.threadId);
        yield* feed(spectrum, from);
        for (const id of [
          started.threadId,
          ...started.participants.map((child) => child.threadId),
        ]) {
          const retired = (yield* engine.getThreadRetirement(ThreadId.make(id)))!;
          yield* dispatchAll([
            {
              ...session(ThreadId.make(id), "interrupted", null),
              commandId: CommandId.make(retired.stopAckCommandId),
            },
          ]);
        }
        const reopen = yield* engine.latestSequence;
        yield* sendUser(started.threadId, "First explicit reopen");
        yield* feed(spectrum, reopen);
        expect((yield* state(started.threadId)).status).toBe("retired");
        for (const child of started.participants) {
          expect((yield* engine.getThreadRetirement(ThreadId.make(child.threadId)))?.retired).toBe(
            true,
          );
          yield* sendUser(child.threadId, "Explicit Color recovery");
        }
        const second = yield* engine.latestSequence;
        yield* sendUser(started.threadId, "Second explicit reopen");
        yield* feed(spectrum, second);
        const current = yield* state(started.threadId);
        expect(current.status).toBe("active");
        expect(current.cycle).toBe(2);
        expect(current.pending).toHaveLength(1);
        const prompt = (yield* detail(current.pending[0]!.threadId)).messages.find(
          (message) => message.id === current.pending[0]!.messageId,
        )!.text;
        expect(prompt).toContain("First explicit reopen");
        expect(prompt).toContain("Second explicit reopen");
      }),
    ),
  ),
);

it.effect(
  "caller retirement cancels an in-flight Spectrum send before its turn binding exists",
  () =>
    scenario((directory, database) =>
      Effect.gen(function* () {
        const sending = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const sessions: ProviderSession[] = [];
        yield* withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            const reactor = yield* ProviderCommandReactor;
            yield* reactor.start();
            const started = yield* callTool("start_spectrum", input("free", 1));
            yield* Deferred.await(sending);
            yield* dispatchUntil(
              retire(PARENT_ID),
              isState(started.threadId, (value) => value.status === "retired"),
            );
            yield* Deferred.await(cancelled);
            yield* reactor.drain;
            expect((yield* state(started.threadId)).status).toBe("retired");
            const engine = yield* OrchestrationEngineService;
            expect(
              (yield* engine.getThreadRetirement(ThreadId.make(started.participants[0]!.threadId)))
                ?.pendingStop,
            ).toBe(false);
            expect(
              (yield* parentMessages).some((message) => message.text.startsWith("[Spectrum")),
            ).toBe(false);
          }).pipe(Effect.scoped),
          undefined,
          {
            provider: {
              listSessions: () => Effect.succeed(sessions),
              getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
              startSession: (threadId, input) =>
                Effect.sync(() => {
                  const session: ProviderSession = {
                    threadId,
                    provider: ProviderDriverKind.make("codex"),
                    providerInstanceId: ProviderInstanceId.make("codex"),
                    runtimeMode: input.runtimeMode,
                    status: "ready",
                    cwd: input.cwd,
                    model: input.modelSelection?.model,
                    createdAt: NOW,
                    updatedAt: NOW,
                  };
                  sessions.push(session);
                  return session;
                }),
              sendTurn: (request) =>
                Deferred.succeed(sending, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as({ threadId: request.threadId, turnId: TurnId.make("must-not-bind") }),
                  Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined)),
                ),
              stopSession: () => Effect.void,
            },
          },
        );
      }),
    ),
);

it.effect(
  "retires cleanly when the caller retires between Spectrum creation and Color creation",
  () =>
    scenario((directory, database) =>
      withEngineOnly(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const engine = yield* OrchestrationEngineService;
          let retired = false;
          const spectrum = yield* makeSpectrum(() => "reasoningEffort").pipe(
            Effect.provideService(OrchestrationEngineService, {
              ...engine,
              dispatch: (command) =>
                Effect.gen(function* () {
                  if (
                    !retired &&
                    command.type === "thread.create" &&
                    command.threadId.startsWith("sub.spectrum.")
                  ) {
                    retired = true;
                    yield* retire(PARENT_ID).pipe(
                      Effect.provideService(OrchestrationEngineService, engine),
                    );
                  }
                  return yield* engine.dispatch(command);
                }),
            }),
          );
          const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("free", 1));
          expect((yield* state(started.threadId)).status).toBe("retired");
          expect((yield* detail(started.threadId)).settledAt).not.toBeNull();
          expect(
            (yield* parentMessages).some((message) => message.text.startsWith("[Spectrum")),
          ).toBe(false);
          const query = yield* ProjectionSnapshotQuery;
          for (const child of started.participants)
            expect(
              Option.isNone(yield* query.getThreadShellById(ThreadId.make(child.threadId))),
            ).toBe(true);
          yield* spectrum.recover;
          expect((yield* state(started.threadId)).status).toBe("retired");
        }),
      ),
    ),
);

it.effect("replays explicit retirement recovery sent while the Spectrum worker was offline", () =>
  scenario((directory, database) =>
    Effect.gen(function* () {
      const started = yield* withEngineOnly(
        database,
        Effect.gen(function* () {
          yield* createParent(directory);
          const spectrum = yield* makeSpectrum(() => "reasoningEffort");
          const started = yield* spectrum.start(yield* threadShell(PARENT_ID), input("free", 1));
          yield* feed(spectrum, 0);
          return started;
        }),
      );
      yield* withEngineOnly(
        database,
        Effect.gen(function* () {
          const engine = yield* OrchestrationEngineService;
          yield* retire(started.threadId);
          for (const id of [
            started.threadId,
            ...started.participants.map((child) => child.threadId),
          ]) {
            const retired = (yield* engine.getThreadRetirement(ThreadId.make(id)))!;
            yield* dispatchAll([
              {
                ...session(ThreadId.make(id), "interrupted", null),
                commandId: CommandId.make(retired.stopAckCommandId),
              },
            ]);
          }
          for (const child of started.participants)
            yield* sendUser(child.threadId, "Explicit offline Color recovery");
          yield* sendUser(started.threadId, "Explicit offline Spectrum recovery");
        }),
      );
      yield* withEngineOnly(
        database,
        Effect.gen(function* () {
          const spectrum = yield* makeSpectrum(() => "reasoningEffort");
          yield* spectrum.recover;
          const current = yield* state(started.threadId);
          expect(current.status).toBe("active");
          expect(current.cycle).toBe(1);
          const engine = yield* OrchestrationEngineService;
          for (const child of started.participants)
            expect(
              (yield* engine.getThreadRetirement(ThreadId.make(child.threadId)))?.retired,
            ).toBe(false);
          const prompt = (yield* detail(current.pending[0]!.threadId)).messages.find(
            (message) => message.id === current.pending[0]!.messageId,
          )!.text;
          expect(prompt).toContain("Explicit offline Spectrum recovery");
        }),
      );
    }),
  ),
);
