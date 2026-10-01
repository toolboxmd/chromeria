import {
  CommandId,
  EventId,
  MessageId,
  OrchestrationCommand,
  ProviderInstanceId,
  ThreadId,
  DEFAULT_PRISM_LANE,
  DEFAULT_RUNTIME_MODE,
  prismRoleModels,
  type OrchestrationEvent,
  type OrchestrationThreadShell,
  ModelSelection,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import { ProviderService } from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { pickRoleModel, roleTaskMessage } from "./roles.ts";
import type { StartSpectrumInput } from "./spectrumTools.ts";
import { ThreadsToolError } from "./tools.ts";

const Participant = Schema.Struct({
  threadId: Schema.String,
  label: Schema.String,
  selection: ModelSelection,
  instructions: Schema.String,
  retirementGeneration: Schema.optional(Schema.Number),
});
const Pending = Schema.Struct({
  threadId: Schema.String,
  messageId: Schema.String,
  previousTurnId: Schema.NullOr(Schema.String),
  turnId: Schema.NullOr(Schema.String),
  requestSequence: Schema.Number,
  requested: Schema.Boolean,
  done: Schema.Boolean,
});
const Entry = Schema.Struct({ id: Schema.String, label: Schema.String, formatted: Schema.Boolean });
export const SpectrumState = Schema.Struct({
  id: Schema.String,
  callerId: Schema.String,
  question: Schema.String,
  callerGeneration: Schema.optional(Schema.Number),
  retirementGeneration: Schema.optional(Schema.Number),
  mode: Schema.Literals(["council", "free"]),
  limit: Schema.Number,
  moderator: Schema.Number,
  phase: Schema.Literals(["discussion", "broadcast"]),
  generation: Schema.Number,
  deliveredUsers: Schema.Number,
  cycle: Schema.Number,
  step: Schema.Number,
  revision: Schema.Number,
  cursor: Schema.Number,
  status: Schema.Literals(["active", "settled", "retired"]),
  participants: Schema.Array(Participant),
  pending: Schema.Array(Pending),
  transcript: Schema.Array(Entry),
  users: Schema.Array(Schema.String),
  outbox: Schema.Array(OrchestrationCommand),
});
export type SpectrumState = typeof SpectrumState.Type;
const TurnBinding = Schema.Struct({ messageId: Schema.String, turnId: Schema.String });
const StartFailure = Schema.Struct({
  requestId: Schema.String,
  detail: Schema.optional(Schema.String),
});
const decodeBinding = Schema.decodeUnknownEffect(TurnBinding);
const decodeFailure = Schema.decodeUnknownEffect(StartFailure);
const decodeState = Schema.decodeUnknownEffect(SpectrumState);
const STATE_KIND = "spectrum.state";
const fail = (reason: string) => Effect.fail(new ThreadsToolError({ reason }));
const spectrumTranscript = (
  entries: ReadonlyArray<{ readonly label: string; readonly text: string }>,
) =>
  entries
    .map((entry) => `[${entry.label === "User" ? "User" : `Color: ${entry.label}`}]\n${entry.text}`)
    .join("\n\n");

/** A persisted outbox precedes every effect. Command receipts make replay safe even mid-transition. */
export const makeSpectrum = Effect.fn("Spectrum.make")(function* (
  effortOptionId: (driverKind: string) => string,
) {
  const engine = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  const settings = yield* ServerSettingsService;
  const providers = yield* ProviderService;
  const registry = yield* ProviderRegistry;
  const crypto = yield* Crypto.Crypto;
  const lock = yield* Semaphore.make(1);
  const states = new Map<string, SpectrumState>();
  const now = Effect.map(DateTime.now, DateTime.formatIso);
  const shell = (id: string) =>
    snapshots.getThreadShellById(ThreadId.make(id)).pipe(Effect.map(Option.getOrThrow));
  const detail = (id: string) =>
    snapshots.getThreadDetailById(ThreadId.make(id)).pipe(Effect.map(Option.getOrThrow));
  // Keep payloads out of state snapshots. Exact-id projection reads retain every byte even
  // after messages fall outside the client's thread-detail window.
  const transcript = Effect.fnUntraced(function* (
    state: SpectrumState,
    entries: SpectrumState["transcript"] = state.transcript,
  ) {
    return spectrumTranscript(
      yield* Effect.forEach(entries, (entry) =>
        Effect.gen(function* () {
          const result = Option.getOrThrow(
            yield* snapshots.getTurnStartMessage({
              threadId: ThreadId.make(state.id),
              messageId: MessageId.make(entry.id),
            }),
          );
          const prefix = `[${entry.label === "User" ? "User" : `Color: ${entry.label}`}]\n`;
          return {
            label: entry.label,
            text: entry.formatted ? result.message.text.slice(prefix.length) : result.message.text,
          };
        }),
      ),
    );
  });
  const dispatch = (command: OrchestrationCommand) =>
    engine
      .dispatch(command)
      .pipe(
        Effect.catch((error) => fail(`Spectrum command ${command.type} failed: ${String(error)}`)),
      );
  const key = (state: SpectrumState, suffix: string) =>
    `spectrum:${state.id}:${state.cycle}:${state.step}:${state.phase}:${state.generation}:${suffix}`;
  const append = (
    id: string,
    text: string,
    messageId: string,
    createdAt: string,
  ): OrchestrationCommand[] => [
    {
      type: "thread.message.assistant.delta",
      threadId: ThreadId.make(id),
      commandId: CommandId.make(`${messageId}:delta`),
      messageId: MessageId.make(messageId),
      delta: text,
      createdAt,
    },
    {
      type: "thread.message.assistant.complete",
      threadId: ThreadId.make(id),
      commandId: CommandId.make(`${messageId}:complete`),
      messageId: MessageId.make(messageId),
      createdAt,
    },
  ];
  const turn = (
    thread: OrchestrationThreadShell,
    text: string,
    id: string,
    createdAt: string,
  ): OrchestrationCommand => ({
    type: "thread.turn.start",
    commandId: CommandId.make(`${id}:start`),
    threadId: thread.id,
    message: { messageId: MessageId.make(id), role: "user", text, attachments: [] },
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
    createdAt,
  });
  const retirementReason = Effect.fnUntraced(function* (state: SpectrumState) {
    for (const id of [
      state.id,
      state.callerId,
      ...state.participants.map((child) => child.threadId),
    ]) {
      const retirement = yield* engine.getThreadRetirement(ThreadId.make(id));
      if (retirement?.retired) return `Thread ${id} was retired.`;
      if (
        id === state.id &&
        (retirement?.cutoffSequence ?? 0) !== (state.retirementGeneration ?? 0)
      )
        return `Spectrum ${id} retirement generation changed.`;
      const participant = state.participants.find((child) => child.threadId === id);
      if (
        participant &&
        (retirement?.cutoffSequence ?? 0) !== (participant.retirementGeneration ?? 0)
      )
        return `Color ${participant.label} retirement generation changed.`;
      if (
        id === state.callerId &&
        (retirement?.cutoffSequence ?? 0) !== (state.callerGeneration ?? 0)
      )
        return `Calling thread ${id} retirement generation changed.`;
    }
    return undefined;
  });
  const flush = Effect.fnUntraced(function* (state: SpectrumState) {
    for (const command of state.outbox) {
      if (command.type === "thread.turn.start" || command.type === "thread.create") {
        const source = yield* engine.getThreadRetirement(ThreadId.make(state.id));
        const caller = yield* engine.getThreadRetirement(ThreadId.make(state.callerId));
        const target = yield* engine.getThreadRetirement(command.threadId);
        const targetGeneration =
          command.threadId === state.callerId
            ? (state.callerGeneration ?? 0)
            : command.threadId === state.id
              ? (state.retirementGeneration ?? 0)
              : (state.participants.find((child) => child.threadId === command.threadId)
                  ?.retirementGeneration ?? 0);
        if (
          source?.retired ||
          caller?.retired ||
          target?.retired ||
          (source?.cutoffSequence ?? 0) !== (state.retirementGeneration ?? 0) ||
          (caller?.cutoffSequence ?? 0) !== (state.callerGeneration ?? 0) ||
          (target?.cutoffSequence ?? 0) !== targetGeneration
        )
          continue;
        // The engine rechecks both source generations atomically with the decision.
        const guarded = {
          ...command,
          commandId: CommandId.make(
            `server:spectrum:${state.id}:${state.retirementGeneration ?? 0}:${state.callerId}:${state.callerGeneration ?? 0}:${targetGeneration}:${command.commandId}`,
          ),
        };
        yield* engine
          .dispatch(guarded)
          .pipe(
            Effect.catch((error) =>
              error._tag === "OrchestrationCommandInvariantError" ||
              error._tag === "OrchestrationCommandPreviouslyRejectedError"
                ? retirementReason(state).pipe(
                    Effect.flatMap((reason) => (reason ? Effect.void : fail(String(error)))),
                  )
                : fail(String(error)),
            ),
          );
      } else
        yield* dispatch({ ...command, commandId: CommandId.make(`server:${command.commandId}`) });
    }
  });
  const save = Effect.fnUntraced(function* (value: SpectrumState) {
    const state = { ...value, revision: value.revision + 1 };
    const createdAt = yield* now;
    const id = `spectrum:${state.id}:state:${state.revision}`;
    yield* dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(id),
      threadId: ThreadId.make(state.id),
      activity: {
        id: EventId.make(id),
        kind: STATE_KIND,
        summary: `Spectrum ${state.status}, cycle ${state.cycle}, step ${state.step}`,
        payload: state,
        tone: "info",
        turnId: null,
        createdAt,
      },
      createdAt,
    });
    states.set(state.id, state);
    yield* flush(state);
    return state;
  });
  const schedule = Effect.fnUntraced(function* (
    previous: SpectrumState,
    extra: OrchestrationCommand[] = [],
  ) {
    if (previous.status === "retired") return previous;
    const reason = yield* retirementReason(previous);
    if (reason) return yield* finish(previous, reason, true);
    const state = { ...previous, generation: previous.generation + 1 };
    const broadcast = state.phase === "broadcast";
    const synthesis = state.mode === "council" && state.step >= state.limit + 1;
    const indexes = broadcast
      ? state.participants.map((_, index) => index)
      : state.mode === "free"
        ? [state.step % state.participants.length]
        : synthesis
          ? [state.moderator]
          : state.participants.map((_, index) => index);
    const createdAt = yield* now;
    const prompt = broadcast
      ? `User broadcast. Read these messages from the user and acknowledge them before discussion continues.\n\n${yield* transcript(
          state,
          state.transcript.filter(
            (entry) => entry.label === "User" && state.users.includes(entry.id),
          ),
        )}`
      : state.mode === "council" && state.step === 0
        ? `Answer independently. Do not consult other Drafters for this first answer.\n\n${state.question}${state.cycle > 0 ? `\n\nPrevious discussion and new user context:\n${yield* transcript(state)}` : ""}`
        : `${
            synthesis
              ? "You are the moderator. Synthesize the council's answers and disagreements into the final result."
              : state.mode === "council"
                ? `Council relay round ${state.step} of ${state.limit}. Respond to the other Colors.`
                : `Free discussion turn ${state.step + 1} of ${state.limit}. Respond to the discussion.`
          }\n\nQuestion: ${state.question}\n\n${yield* transcript(state)}`;
    const pending: SpectrumState["pending"][number][] = [];
    const outbox = [...extra];
    for (const index of indexes) {
      const participant = state.participants[index]!;
      const child = yield* shell(participant.threadId);
      const id = key(state, `turn:${index}`);
      pending.push({
        threadId: child.id,
        messageId: id,
        previousTurnId: child.latestTurn?.turnId ?? null,
        turnId: null,
        requestSequence: 0,
        requested: false,
        done: false,
      });
      outbox.push(
        turn(
          child,
          participant.instructions &&
            (child.latestTurn === null || (state.step === 0 && state.phase === "discussion"))
            ? `${participant.instructions}\n\n${prompt}`
            : prompt,
          id,
          createdAt,
        ),
      );
    }
    return yield* save({ ...state, pending, outbox });
  });
  const finish = Effect.fnUntraced(function* (
    state: SpectrumState,
    failure?: string,
    retired = false,
  ) {
    const createdAt = yield* now;
    const caller = yield* shell(state.callerId);
    const result = failure
      ? `Spectrum stopped: ${failure}\n\n${yield* transcript(state)}`
      : state.mode === "council"
        ? yield* transcript(
            state,
            state.transcript.filter((entry) => entry.id.startsWith(key(state, "reply:"))),
          )
        : yield* transcript(state);
    const outbox: OrchestrationCommand[] = [
      ...append(
        state.id,
        failure ? `Spectrum stopped: ${failure}` : "Spectrum complete.",
        key(state, "completion"),
        createdAt,
      ),
      ...((yield* engine.getThreadRetirement(caller.id))?.retired ||
      (yield* engine.getThreadRetirement(ThreadId.make(state.id)))?.retired
        ? []
        : [
            turn(
              caller,
              `[Spectrum ${state.id}, ${state.mode}, cycle ${state.cycle}]\n\n${result}`,
              key(state, "report"),
              createdAt,
            ),
          ]),
      {
        type: "thread.settle",
        threadId: ThreadId.make(state.id),
        commandId: CommandId.make(key(state, "settle")),
      },
    ];
    // Failure ends scheduling immediately. Interrupt only the exact active turns, never a successor.
    if (failure) {
      for (const participant of state.participants) {
        const childOption = yield* snapshots.getThreadShellById(
          ThreadId.make(participant.threadId),
        );
        // Retirement can reject a child's creation after the initial state was saved.
        if (Option.isNone(childOption)) continue;
        const child = childOption.value;
        const pending = state.pending.find((entry) => entry.threadId === child.id && !entry.done);
        const childRetirement = yield* engine.getThreadRetirement(child.id);
        if (childRetirement?.retired) continue;
        if (retired) {
          // Explicit recovery belongs to the newer generation, outside this cycle.
          if ((childRetirement?.cutoffSequence ?? 0) !== (participant.retirementGeneration ?? 0))
            continue;
          // Use the retirement owner's cancellation and stop acknowledgement path,
          // including a send that has not returned its provider turn id yet.
          outbox.push({
            type: "thread.activity.append",
            threadId: child.id,
            commandId: CommandId.make(
              `mcp-threads-retire:spectrum:${key(state, `stop:${child.id}`)}`,
            ),
            activity: {
              id: EventId.make(key(state, `retire:${child.id}`)),
              kind: "thread.subtree-retire-requested",
              summary: "Spectrum stopped after retirement",
              payload: {},
              tone: "info",
              turnId: null,
              createdAt,
            },
            createdAt,
          });
          continue;
        }
        if (pending?.turnId && child.session?.activeTurnId === pending.turnId)
          outbox.push({
            type: "thread.turn.interrupt",
            threadId: child.id,
            turnId: child.session.activeTurnId,
            commandId: CommandId.make(key(state, `stop:${child.id}`)),
            createdAt,
          });
        else if (pending?.requested && pending.turnId === null) {
          const messages = (yield* detail(child.id)).messages;
          if (messages.findLast((message) => message.role === "user")?.id === pending.messageId)
            outbox.push({
              type: "thread.session.stop",
              threadId: child.id,
              commandId: CommandId.make(key(state, `stop-unbound:${child.id}`)),
              createdAt,
            });
        }
      }
    }
    return yield* save({ ...state, status: retired ? "retired" : "settled", outbox });
  });
  const advance = Effect.fnUntraced(function* (
    state: SpectrumState,
    extra: OrchestrationCommand[],
  ) {
    const last =
      state.mode === "council" ? state.step >= state.limit + 1 : state.step === state.limit - 1;
    if (state.users.length > state.deliveredUsers) {
      return yield* schedule(
        { ...state, phase: "broadcast", deliveredUsers: state.users.length },
        extra,
      );
    }
    if (state.phase === "broadcast") {
      if (last && state.mode === "council")
        return yield* schedule({ ...state, phase: "discussion" }, extra);
      if (!last)
        return yield* schedule({ ...state, step: state.step + 1, phase: "discussion" }, extra);
    }
    if (last) {
      const stored = yield* save({ ...state, outbox: extra });
      return yield* finish(stored);
    }
    return yield* schedule({ ...state, step: state.step + 1, phase: "discussion" }, extra);
  });
  const handle = Effect.fnUntraced(function* (event: OrchestrationEvent, id: string) {
    const state = states.get(id);
    if (!state || event.sequence <= state.cursor) return;
    if (event.type === "thread.activity-appended" && event.payload.activity.kind === STATE_KIND)
      return;
    if (state.status === "active") {
      const reason = yield* retirementReason(state);
      if (reason) {
        yield* finish({ ...state, cursor: event.sequence }, reason, true);
        return;
      }
    }
    if (
      event.type === "thread.message-sent" &&
      event.aggregateId === state.id &&
      event.payload.role === "user"
    ) {
      if (state.users.includes(event.payload.messageId)) return;
      const entry = { id: event.payload.messageId, label: "User", formatted: false };
      const next = {
        ...state,
        cursor: event.sequence,
        users: [...state.users, entry.id],
        transcript: [...state.transcript, entry],
        outbox: [],
      };
      if (state.status === "settled" || state.status === "retired")
        return void (yield* schedule({
          ...next,
          cycle: state.cycle + 1,
          step: 0,
          phase: "discussion",
          status: "active",
          retirementGeneration:
            (yield* engine.getThreadRetirement(ThreadId.make(state.id)))?.cutoffSequence ?? 0,
          callerGeneration:
            (yield* engine.getThreadRetirement(ThreadId.make(state.callerId)))?.cutoffSequence ?? 0,
          participants: yield* Effect.forEach(state.participants, (child) =>
            Effect.gen(function* () {
              return {
                ...child,
                retirementGeneration:
                  (yield* engine.getThreadRetirement(ThreadId.make(child.threadId)))
                    ?.cutoffSequence ?? 0,
              };
            }),
          ),
        }));
      // Broadcast durably without starting/superseding any participant's barrier turn.
      const createdAt = yield* now;
      const outbox: OrchestrationCommand[] = state.participants.map((child) => ({
        type: "thread.message.user.append",
        commandId: CommandId.make(`spectrum:${state.id}:user:${entry.id}:${child.threadId}`),
        threadId: ThreadId.make(child.threadId),
        message: {
          messageId: MessageId.make(`spectrum:user:${entry.id}`),
          text: `[User]\n${event.payload.text}`,
          attachments: [],
        },
        createdAt,
      }));
      yield* save({ ...next, outbox });
      return;
    }
    if (
      event.type === "thread.unsettled" &&
      event.aggregateId === state.id &&
      event.payload.reason === "user" &&
      state.status === "settled"
    ) {
      yield* schedule({
        ...state,
        cursor: event.sequence,
        cycle: state.cycle + 1,
        step: 0,
        phase: "discussion",
        status: "active",
      });
      return;
    }
    if (state.status !== "active") return;
    const index = state.pending.findIndex(
      (entry) => entry.threadId === event.aggregateId && !entry.done,
    );
    if (index < 0) return;
    const pending = state.pending[index]!;
    const replace = (value: SpectrumState["pending"][number]) =>
      state.pending.map((entry, i) => (i === index ? value : entry));
    if (
      event.type === "thread.turn-start-requested" &&
      event.payload.messageId === pending.messageId
    ) {
      yield* save({
        ...state,
        cursor: event.sequence,
        pending: replace({ ...pending, requested: true, requestSequence: event.sequence }),
        outbox: [],
      });
      return;
    }
    if (!pending.requested) return;
    let bound = pending;
    let current = state;
    if (
      event.type === "thread.activity-appended" &&
      event.payload.activity.kind === "spectrum.turn-bound"
    ) {
      const binding = yield* decodeBinding(event.payload.activity.payload);
      if (binding.messageId !== pending.messageId || binding.turnId === pending.previousTurnId)
        return;
      bound = { ...pending, turnId: binding.turnId };
      current = yield* save({
        ...state,
        cursor: event.sequence,
        pending: replace(bound),
        outbox: [],
      });
    } else if (
      event.type === "thread.activity-appended" &&
      event.payload.activity.kind === "provider.turn.start.failed"
    ) {
      const failure = yield* decodeFailure(event.payload.activity.payload);
      if (failure.requestId === pending.messageId)
        yield* finish(
          { ...state, cursor: event.sequence },
          failure.detail ?? "Provider turn start failed.",
        );
      return;
    } else if (
      event.type !== "thread.session-set" &&
      !(event.type === "thread.message-sent" && !event.payload.streaming) &&
      !(event.type === "thread.turn-diff-completed" && event.payload.turnId === pending.turnId)
    )
      return;
    if (bound.turnId === null) return;
    const child = yield* detail(pending.threadId);
    const session = child.session;
    if (!session) return;
    if (
      session.status === "error" ||
      session.status === "interrupted" ||
      session.status === "stopped"
    ) {
      yield* finish(
        { ...current, cursor: event.sequence },
        `${state.participants.find((participant) => participant.threadId === pending.threadId)!.label}: ${session.lastError ?? session.status}`,
      );
      return;
    }
    if (
      !["ready", "idle"].includes(session.status) ||
      (session.activeTurnId !== null && session.activeTurnId !== bound.turnId)
    )
      return;
    const requestExists = child.messages.some((message) => message.id === pending.messageId);
    const events = yield* engine
      .readThreadEvents({
        threadId: ThreadId.make(pending.threadId),
        fromSequenceExclusive: pending.requestSequence,
        toSequenceInclusive: yield* engine.latestSequence,
        limit: 100_000,
      })
      .pipe(Stream.runCollect);
    const latestRunning = events.findLast(
      (item) =>
        item.type === "thread.session-set" &&
        item.payload.session.status === "running" &&
        item.payload.session.activeTurnId !== null,
    );
    if (
      !latestRunning ||
      latestRunning.type !== "thread.session-set" ||
      latestRunning.payload.session.activeTurnId !== bound.turnId
    )
      return;
    const messageOrder = new Map<string, number>();
    for (const item of events) {
      if (
        item.type === "thread.message-sent" &&
        item.payload.role === "assistant" &&
        item.payload.turnId === bound.turnId &&
        !messageOrder.has(item.payload.messageId)
      ) {
        messageOrder.set(item.payload.messageId, item.sequence);
      }
    }
    const messages = child.messages
      .filter((message) => message.role === "assistant" && message.turnId === bound.turnId)
      .toSorted(
        (left, right) =>
          (messageOrder.get(left.id) ?? Infinity) - (messageOrder.get(right.id) ?? Infinity),
      );
    if (messages.some((message) => message.streaming)) return;
    if (!requestExists || messages.length === 0) {
      yield* finish(
        { ...state, cursor: event.sequence },
        `${pending.threadId} ended its turn without a completed reply.`,
      );
      return;
    }
    const label = state.participants.find(
      (participant) => participant.threadId === pending.threadId,
    )!.label;
    const entries = messages.map((message) => ({
      id: key(state, `reply:${pending.threadId}:${message.id}`),
      label,
      formatted: true,
    }));
    const createdAt = yield* now;
    const outbox = entries.flatMap((entry, index) =>
      append(
        state.id,
        spectrumTranscript([{ label, text: messages[index]!.text }]),
        entry.id,
        createdAt,
      ),
    );
    const next = {
      ...current,
      cursor: event.sequence,
      pending: replace({ ...bound, done: true }),
      transcript: [...state.transcript, ...entries],
    };
    const stored = yield* save({ ...next, outbox });
    if (stored.pending.every((entry) => entry.done)) yield* advance(stored, []);
  });
  const recover = lock.withPermit(
    Effect.gen(function* () {
      const rows = yield* snapshots.listActivitiesByKind(STATE_KIND);
      for (const row of rows) {
        const state = yield* decodeState(row.payload);
        if ((states.get(state.id)?.revision ?? -1) < state.revision) states.set(state.id, state);
      }
      const head = yield* engine.latestSequence;
      for (const state of states.values()) {
        if (state.status === "active") {
          const reason = yield* retirementReason(state);
          if (reason) {
            // Finish any persisted transcript appends, while flush suppresses all starts.
            yield* flush(state);
            yield* finish(state, reason, true);
          } else yield* flush(state);
        } else yield* flush(state);
        if (state.step === -1 && states.get(state.id)?.status === "active")
          yield* schedule({ ...state, step: 0 });
        const events = yield* Effect.forEach(
          [state.id, state.callerId, ...state.participants.map((child) => child.threadId)],
          (id) =>
            engine
              .readThreadEvents({
                threadId: ThreadId.make(id),
                fromSequenceExclusive: state.cursor,
                toSequenceInclusive: head,
                limit: 100_000,
              })
              .pipe(Stream.runCollect),
        );
        for (const event of events.flat().toSorted((a, b) => a.sequence - b.sequence))
          yield* handle(event, state.id);
        const current = states.get(state.id)!;
        if (
          current.status === "active" &&
          current.pending.some(
            (entry) =>
              entry.requested &&
              entry.requestSequence <= head &&
              entry.turnId === null &&
              !entry.done,
          )
        ) {
          // sendTurn has no persisted idempotency key. Its result may have been lost;
          // resending could duplicate real provider work. End this cycle explicitly.
          yield* finish(
            current,
            "Provider start outcome was lost during restart; send an explicit message to reopen.",
          );
          continue;
        }
        // A crash after saving the final replies but before saving completion still finishes once.
        if (
          current.status === "active" &&
          current.pending.length > 0 &&
          current.pending.every((entry) => entry.done)
        )
          yield* advance(current, []);
      }
    }),
  );
  const start = (parent: OrchestrationThreadShell, input: StartSpectrumInput) =>
    lock
      .withPermit(
        Effect.gen(function* () {
          const callerRetirement = yield* engine.getThreadRetirement(parent.id);
          if (callerRetirement?.retired)
            return yield* fail(`Calling thread ${parent.id} is retired.`);
          if (input.colors.some((color) => color.label.toLowerCase() === "user"))
            return yield* fail("User is reserved for user attribution.");
          if (new Set(input.colors.map((color) => color.label)).size !== input.colors.length)
            return yield* fail("Colors must have unique labels.");
          const limit = input.limit ?? (input.mode === "council" ? 2 : input.colors.length * 3);
          if (input.mode === "council" && limit < 2)
            return yield* fail("Council requires at least two relay rounds.");
          const moderator = input.moderator ?? 0;
          if (moderator >= input.colors.length)
            return yield* fail("Moderator must name a Color index.");
          const kits = resolveProjectSettings(yield* settings.getSettings, parent.projectId)
            .settings.prismRoles;
          const resolved = yield* Effect.forEach(input.colors, (color) =>
            Effect.gen(function* () {
              const kit = color.role ? kits[color.role] : undefined;
              let preferred;
              if (color.role && !color.model && !color.instanceId) {
                const models = prismRoleModels(kits, color.role, color.lane ?? DEFAULT_PRISM_LANE);
                if (models.length > 0) {
                  const picked = pickRoleModel(
                    models,
                    yield* registry.getProviders,
                    yield* Clock.currentTimeMillis,
                  );
                  if ("refusal" in picked) return yield* fail(picked.refusal);
                  preferred = picked.pick;
                }
              }
              const instanceId = ProviderInstanceId.make(
                color.instanceId ?? preferred?.instanceId ?? parent.modelSelection.instanceId,
              );
              const model =
                color.model ??
                preferred?.model ??
                (instanceId === parent.modelSelection.instanceId
                  ? parent.modelSelection.model
                  : undefined);
              if (!model) return yield* fail(`Pass model for Color ${color.label}.`);
              const info = yield* providers.getInstanceInfo(instanceId);
              if (!info.enabled) return yield* fail(`Provider instance ${instanceId} is disabled.`);
              const effort = color.effort ?? preferred?.effort;
              const selection: ModelSelection = {
                instanceId,
                model,
                ...(effort
                  ? { options: [{ id: effortOptionId(info.driverKind), value: effort }] }
                  : {}),
              };
              return { color, selection, kit };
            }),
          );
          const id = `spectrum.${yield* crypto.randomUUIDv4}`;
          const createdAt = yield* now;
          const create = (
            threadId: string,
            title: string,
            selection: ModelSelection,
          ): OrchestrationCommand => ({
            type: "thread.create",
            commandId: CommandId.make(
              `server:mcp-threads-create:${parent.id}:${callerRetirement?.cutoffSequence ?? 0}:spectrum:${id}:create:${threadId}`,
            ),
            threadId: ThreadId.make(threadId),
            projectId: parent.projectId,
            title,
            modelSelection: selection,
            runtimeMode: DEFAULT_RUNTIME_MODE,
            interactionMode: "default",
            branch: parent.branch,
            worktreePath: parent.worktreePath,
            createdAt,
          });
          yield* dispatch(
            create(
              id,
              input.title ?? `Spectrum: ${input.question.slice(0, 60)}`,
              parent.modelSelection,
            ),
          );
          const participants = resolved.map(({ color, selection, kit }, index) => ({
            threadId: `sub.${id}.${index}`,
            label: color.label,
            selection,
            instructions: kit
              ? roleTaskMessage(kit, "Participate as a Drafter in this Spectrum.")
              : "",
          }));
          const creates = participants.map((child, index) =>
            create(child.threadId, `Color: ${child.label}`, resolved[index]!.selection),
          );
          let state: SpectrumState = {
            id,
            callerId: parent.id,
            callerGeneration: callerRetirement?.cutoffSequence ?? 0,
            retirementGeneration: 0,
            question: input.question,
            mode: input.mode,
            limit,
            moderator,
            phase: "discussion",
            generation: 0,
            deliveredUsers: 0,
            cycle: 0,
            step: -1,
            revision: 0,
            cursor: yield* engine.latestSequence,
            status: "active",
            participants,
            pending: [],
            transcript: [{ id: `spectrum:${id}:question`, label: "User", formatted: true }],
            users: [],
            outbox: creates,
          };
          state = yield* save({
            ...state,
            outbox: [
              ...creates,
              ...append(id, `[User]\n${input.question}`, `spectrum:${id}:question`, createdAt),
            ],
          });
          yield* schedule({ ...state, step: 0 });
          return {
            threadId: id,
            callerThreadId: parent.id,
            mode: input.mode,
            participants: participants.map((child, index) => ({
              threadId: child.threadId,
              label: child.label,
              instanceId: resolved[index]!.selection.instanceId,
              model: resolved[index]!.selection.model,
            })),
          };
        }),
      )
      .pipe(Effect.catchCause((cause) => fail(`Could not start Spectrum: ${String(cause)}`)));
  return {
    start,
    recover,
    onEvent: (event: OrchestrationEvent) =>
      lock.withPermit(
        Effect.gen(function* () {
          for (const state of states.values()) {
            if (
              state.id === event.aggregateId ||
              state.callerId === event.aggregateId ||
              state.participants.some((child) => child.threadId === event.aggregateId)
            )
              yield* handle(event, state.id);
          }
        }),
      ),
    statusOf: (id: string) =>
      states.get(id)?.status === "active"
        ? ("running" as const)
        : states.get(id)?.status === "retired"
          ? ("stopped" as const)
          : ("idle" as const),
  };
});
