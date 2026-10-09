import {
  CommandId,
  MessageId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
  type SpectrumTranscriptAppend,
} from "@t3tools/contracts";
import { ThreadCommandExecutor } from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { readRetirementState } from "../childThreads/retirement.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Events from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import { followRun } from "../scheduledTaskChecks/handoff.ts";
import { acceptReply, barrierComplete, nextRound } from "./barrier.ts";
import { ownedActiveRuns, spectrumCancellationPlan } from "./cancellationPlan.ts";
import * as Reports from "./ReportService.ts";
import * as Rounds from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import { lifecycleEvents, lifecyclePlan } from "./lifecycle.ts";
import { readSpectrum } from "./store.ts";
import type { SpectrumState } from "./state.ts";

export class SpectrumControllerError extends Schema.TaggedError<SpectrumControllerError>()(
  "SpectrumControllerError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}
export class SpectrumController extends Context.Service<
  SpectrumController,
  {
    readonly resume: (threadId: ThreadId) => Effect.Effect<SpectrumState, SpectrumControllerError>;
    readonly sweep: Effect.Effect<void>;
  }
>()("t3/spectrum/Controller/SpectrumController") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const locks = yield* ThreadCommandExecutor;
  const sink = yield* Sink.EventSinkV2;
  const events = yield* Events.EventStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const rounds = yield* Rounds.SpectrumRoundService;
  const transcript = yield* Transcript.SpectrumTranscriptService;
  const reports = yield* Reports.SpectrumReportService;
  const read = (id: ThreadId) =>
    readSpectrum(id).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.map(Option.getOrThrow),
    );
  const save = Effect.fn("SpectrumController.save")(function* (
    old: SpectrumState,
    next: SpectrumState,
    key: string,
  ) {
    const now = yield* DateTime.now;
    const thread = yield* projections.getThread(old.threadId);
    const lifecycle = lifecycleEvents(thread, next, key, now);
    yield* sink.commitCommand({
      commandId: CommandId.make(key),
      threadId: old.threadId,
      commandType: "spectrum.controller",
      acceptedAt: now,
      events: lifecycle,
      effects: [],
      forkPlans: [
        ...(lifecycle.length === 0 ? [] : [lifecyclePlan(thread)]),
        spectrumPlan({
          expectedRevision: old.revision,
          expectedGeneration: old.generation,
          state: next,
        }),
      ],
    });
    return yield* read(old.threadId);
  });
  const append = Effect.fn("SpectrumController.append")(function* (
    state: SpectrumState,
    id: MessageId,
    text: string,
    role: "user" | "assistant",
    key: string,
  ) {
    const revision = state.revision + 1;
    const command: SpectrumTranscriptAppend = {
      type: "spectrum.transcript.append",
      commandId: CommandId.make(key),
      threadId: state.threadId,
      generation: state.generation,
      revision,
      messageId: id,
      role,
      text,
    };
    yield* save(
      state,
      { ...state, revision, outbox: [...state.outbox, command] },
      `${key}:enqueue`,
    );
    yield* transcript.dispatch(state.threadId, command.commandId);
    return yield* read(state.threadId);
  });
  const cancelOwned = Effect.fn("SpectrumController.cancelOwned")(function* (
    state: SpectrumState,
    key: string,
  ) {
    // Lineage-only Colors are not delegated task rows. Enumerate their full owned lineage explicitly.
    const owned = yield* ownedActiveRuns(state.threadId).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const generation = state.generation + 1;
    const commands: OrchestrationV2Command[] = owned.map((run) => {
      const identity = {
        commandId: CommandId.make(`${key}:cancel:${run.run_id}`),
        threadId: ThreadId.make(run.thread_id),
        runId: RunId.make(run.run_id),
      };
      return run.status === "queued"
        ? { ...identity, type: "queued-run.cancel" }
        : { ...identity, type: "run.interrupt", holdQueue: false, reason: "Spectrum stopped" };
    });
    return yield* save(
      state,
      {
        ...state,
        generation,
        revision: state.revision + 1,
        status: "retired",
        round: null,
        outbox: commands,
      },
      key,
    );
  });
  const ownedWorkDrained = Effect.fn("SpectrumController.ownedWorkDrained")(function* (
    id: ThreadId,
  ) {
    const pending = yield* sql`WITH RECURSIVE owned(thread_id) AS (
      SELECT thread_id FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.lineage.parentThreadId')=${id}
        AND json_extract(payload_json,'$.lineage.relationshipToParent')='subagent'
      UNION SELECT t.thread_id FROM orchestration_v2_projection_threads t JOIN owned p ON json_extract(t.payload_json,'$.lineage.parentThreadId')=p.thread_id
        WHERE json_extract(t.payload_json,'$.lineage.relationshipToParent')='subagent'
    ) SELECT 1 FROM orchestration_v2_effect_outbox e JOIN owned o ON e.thread_id=o.thread_id
      WHERE e.status IN('pending','running') AND json_extract(e.payload_json,'$.type') IN('provider-turn.start','provider-turn.restart','provider-turn.interrupt') LIMIT 1`;
    return (
      pending.length === 0 &&
      (yield* ownedActiveRuns(id).pipe(Effect.provideService(SqlClient.SqlClient, sql))).length ===
        0
    );
  });
  const reconcileRetired = Effect.fn("SpectrumController.reconcileRetired")(function* (
    state: SpectrumState,
  ) {
    const owned = yield* ownedActiveRuns(state.threadId).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const commands: OrchestrationV2Command[] = owned.map((run) => {
      const type = run.status === "queued" ? "queued-run.cancel" : "run.interrupt";
      const existing = state.outbox.find(
        (command) => command.type === type && command.runId === run.run_id,
      );
      if (existing !== undefined && existing.type !== "spectrum.transcript.append") return existing;
      const identity = {
        commandId: CommandId.make(
          `spectrum:${state.threadId}:${state.generation}:drain:${run.run_id}:${run.status}`,
        ),
        threadId: ThreadId.make(run.thread_id),
        runId: RunId.make(run.run_id),
      };
      return run.status === "queued"
        ? { ...identity, type: "queued-run.cancel" }
        : { ...identity, type: "run.interrupt", holdQueue: false, reason: "Spectrum stopped" };
    });
    const other = state.outbox.filter(
      (command) => command.type !== "run.interrupt" && command.type !== "queued-run.cancel",
    );
    const outbox = [...commands, ...other];
    if (
      outbox.length === state.outbox.length &&
      outbox.every((command, index) => command.commandId === state.outbox[index]?.commandId)
    )
      return state;
    // Stop's snapshot may predate a Color launch commit. Keep its generation/report and persist exact cancellations before delivery.
    return yield* save(
      state,
      { ...state, revision: state.revision + 1, outbox },
      `spectrum:${state.threadId}:${state.generation}:drain:${state.revision}`,
    );
  });
  const drain = Effect.fn("SpectrumController.drain")(function* (id: ThreadId) {
    let state = yield* read(id);
    for (const command of state.outbox) {
      if (command.type === "spectrum.transcript.append") {
        yield* transcript.dispatch(id, command.commandId);
      } else if (command.type === "run.interrupt" || command.type === "queued-run.cancel") {
        yield* orchestrator
          .dispatch(command)
          .pipe(
            Effect.provideService(ForkDispatchPlans, [
              spectrumCancellationPlan(id, state.generation, command),
            ]),
          );
        state = yield* read(id);
        yield* save(
          state,
          {
            ...state,
            revision: state.revision + 1,
            outbox: state.outbox.filter((pending) => pending.commandId !== command.commandId),
          },
          `${command.commandId}:drained`,
        );
      } else if (state.report?.commandId === command.commandId) {
        if (state.status !== "retired" || (yield* ownedWorkDrained(id))) yield* reports.deliver(id);
      } else if (command.type === "message.dispatch") {
        yield* rounds.dispatch(id, command.commandId);
      }
      state = yield* read(id);
    }
    return state;
  });
  const pump = Effect.fn("SpectrumController.resume")(function* (id: ThreadId) {
    let state = yield* read(id);
    const shell = yield* projections.getThread(id);
    if (shell.forkSpectrumRunning !== (state.status === "active")) {
      state = yield* save(
        state,
        { ...state, revision: state.revision + 1 },
        `spectrum:${id}:lifecycle:${state.revision}`,
      );
    }
    const retirement = yield* readRetirementState(id).pipe(
      Effect.provideService(Projections.ProjectionStoreV2, projections),
    );
    if (state.status === "active" && (retirement.retired || !retirement.complete)) {
      state = yield* cancelOwned(state, `spectrum:${id}:${state.generation}:retirement`);
      yield* reports.settle(id, "Spectrum stopped.", true);
    }
    for (let step = 0; step < 128; step++) {
      if (state.status === "retired") state = yield* reconcileRetired(state);
      state = yield* drain(id);
      if (state.status !== "active") {
        if (state.status !== "retired" || (yield* ownedWorkDrained(id))) yield* reports.deliver(id);
        return yield* read(id);
      }
      // Consume raw stored rows. Wire projections are deliberately excluded from durable recovery.
      const batch = yield* events
        .read({ afterSequence: state.cursor, limit: 256 })
        .pipe(Stream.runCollect);
      const last = batch.at(-1);
      if (last !== undefined)
        state = yield* save(
          state,
          { ...state, revision: state.revision + 1, cursor: last.sequence },
          `spectrum:${id}:${state.generation}:cursor:${last.sequence}`,
        );
      if (state.round === null) {
        if (state.transcript.length === 0)
          state = yield* append(
            state,
            MessageId.make(`spectrum:${id}:question`),
            `[User]\n${state.question}`,
            "user",
            `spectrum:${id}:question`,
          );
        yield* rounds.prepare(id);
        continue;
      }
      let accepted = false;
      for (const slot of state.round.slots) {
        if (slot.runId === null || slot.replyIds !== null) continue;
        const end = yield* followRun(slot.runId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        if (end.kind === "waiting") continue;
        const records = yield* projections.getThreadRecords(slot.threadId, ["runs", "messages"]);
        const run = records.runs.find((candidate) => candidate.id === end.runId);
        if (run === undefined) continue;
        // followRun validates every immutable restart/Prism source link before selecting this exact run.
        const linked = {
          ...state.round,
          slots: state.round.slots.map((current) =>
            current.commandId === slot.commandId ? { ...current, runId: run.id } : current,
          ),
        };
        const reply = acceptReply(linked, {
          generation: state.generation,
          run,
          messages: records.messages,
          recoveryPending: false,
        });
        if (reply.type === "failed") {
          state = yield* cancelOwned(
            state,
            `spectrum:${id}:${state.generation}:failed:${slot.commandId}`,
          );
          yield* reports.settle(id, `Spectrum could not finish: ${reply.reason}.`, true);
          return yield* drain(id);
        }
        if (reply.type !== "accepted") continue;
        const key = `${slot.commandId}:reply`;
        const revision = state.revision + 1;
        const label = state.participants.find((color) => color.threadId === slot.threadId)!.label;
        const command: SpectrumTranscriptAppend = {
          type: "spectrum.transcript.append",
          commandId: CommandId.make(key),
          threadId: id,
          generation: state.generation,
          revision,
          messageId: MessageId.make(key),
          role: "assistant",
          text: `[${label}]\n${reply.messages.map((message) => message.text).join("\n\n")}`,
        };
        state = yield* save(
          state,
          { ...state, revision, round: reply.round, outbox: [command] },
          `${key}:enqueue`,
        );
        state = yield* drain(id);
        accepted = true;
        break;
      }
      if (accepted) continue;
      if (state.round !== null && barrierComplete(state.round)) {
        if (nextRound(state) !== null) {
          yield* rounds.prepare(id);
          continue;
        }
        const records = yield* projections.getThreadRecords(id, ["messages"]);
        const text = records.messages
          .filter(
            (message) => state.transcript.includes(message.id) && message.role === "assistant",
          )
          .map((message) => message.text)
          .join("\n\n");
        yield* reports.settle(id, `Spectrum completed.\n\n${text}`);
        return yield* drain(id);
      }
      return state;
    }
    return yield* read(id);
  });
  const guarded = <A, E>(id: ThreadId, effect: Effect.Effect<A, E>) =>
    locks
      .withLock(id, effect)
      .pipe(Effect.mapError((cause) => new SpectrumControllerError({ threadId: id, cause })));
  const resume = (id: ThreadId) => guarded(id, pump(id));
  return SpectrumController.of({
    resume,
    sweep: Effect.gen(function* () {
      const rows = yield* sql<{ thread_id: string }>`SELECT thread_id FROM fork_spectra`;
      for (const row of rows)
        yield* resume(ThreadId.make(row.thread_id)).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Spectrum reconciliation failed", { threadId: row.thread_id, cause }),
          ),
        );
    }).pipe(Effect.catchCause((cause) => Effect.logWarning("Spectrum sweep failed", { cause }))),
  });
});
export const layer = Layer.effect(SpectrumController, make);
