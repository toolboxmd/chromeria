import {
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { readRetirementThreadIds } from "../childThreads/retirement.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import { RecoveryWaitSignals } from "../peer/recoveryWaitSignals.ts";
import { forkParked } from "../serverActivation.ts";
import { ownedActiveRuns, ownedThreads } from "./cancellationPlan.ts";
import { SpectrumController } from "./Controller.ts";
import { attemptOutcome, reportAttempt, reportDrainProof, reportNeedsYou } from "./reportPolicy.ts";
import type { SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

/** `all` wakes on every event and recovery signal; `shell` only on thread metadata, which carries retirement. */
type Interest = "all" | "shell";

interface SpectrumWatch {
  readonly threads: ReadonlyMap<ThreadId, Interest>;
  /** Unsettled effects whose settlement may emit no event, such as a declined native restart. */
  readonly effects: ReadonlyArray<string>;
}

/** Delivered, held for a person, or an abandoned report whose chains drained. */
const reportConcluded = Effect.fnUntraced(function* (state: SpectrumState) {
  const report = state.report;
  if (report === null || reportAttempt(state) === null) return true;
  if (state.reportAbandonment?.commandId === report.commandId)
    return (yield* reportDrainProof(state)) !== null;
  if (state.outbox.some((command) => command.commandId === report.commandId)) return false;
  return (
    (yield* attemptOutcome(state.callerThreadId, report)).kind === "delivered" ||
    (yield* reportNeedsYou(state)) !== null
  );
});

/**
 * What can still move a Spectrum besides its own thread: owned lineage, the
 * caller while a report is out, ancestors' retirement and unsettled effects.
 * Null once nothing but an event on the Spectrum thread, such as reopen, can.
 */
const readSpectrumWatch = Effect.fn("Spectrum.readWatch")(function* (state: SpectrumState) {
  const sql = yield* SqlClient.SqlClient;
  const owned = yield* ownedThreads(state.threadId);
  const reportOut = !(yield* reportConcluded(state));
  const followed = reportOut ? [...owned, state.callerThreadId] : owned;
  const effects =
    followed.length === 0
      ? []
      : (yield* sql<{
          readonly effect_id: string;
        }>`SELECT effect_id FROM orchestration_v2_effect_outbox
          WHERE thread_id IN ${sql.in(followed)} AND status IN ('pending','running')`).map(
          (row) => row.effect_id,
        );
  if (
    state.status !== "active" &&
    state.outbox.length === 0 &&
    !reportOut &&
    effects.length === 0 &&
    (yield* ownedActiveRuns(state.threadId)).length === 0
  )
    return null;
  const threads = new Map<ThreadId, Interest>();
  for (const id of yield* readRetirementThreadIds(state.threadId))
    if (id !== state.threadId) threads.set(id, "shell");
  for (const id of followed) threads.set(id, "all");
  return { threads, effects } satisfies SpectrumWatch;
});

const RECONCILE_INTERVAL = Duration.seconds(60);

/** Progress that cannot settle anything; the event that ends it wakes the Spectrum. */
const settlesNothing = (event: OrchestrationV2DomainEvent) =>
  (event.type === "message.updated" && event.payload.streaming) ||
  (event.type === "turn-item.updated" && event.payload.status === "running");

/**
 * Spectrums advance on events (#404). Every Spectrum thread is known; a live
 * Spectrum also follows its watch. Wakes for one Spectrum coalesce into one
 * pending resume, and each resume re-reads its watch. A slow tick wakes every
 * active or live Spectrum, the fallback for any lost wake; with none it costs
 * one indexed read.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const controller = yield* SpectrumController;
    const sink = yield* EventSink.EventSinkV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const signals = yield* RecoveryWaitSignals;
    const sql = yield* SqlClient.SqlClient;
    const projections = yield* Projections.ProjectionStoreV2;
    const scope = yield* Effect.scope;

    const spectra = new Set<ThreadId>();
    const watches = new Map<ThreadId, SpectrumWatch>();
    const watchers = new Map<ThreadId, Map<ThreadId, Interest>>();
    const waiters = new Map<ThreadId, Map<string, Fiber.Fiber<void>>>();
    const queued = new Set<ThreadId>();
    const ready = yield* Queue.unbounded<ThreadId>();

    const wake = (id: ThreadId) =>
      Effect.sync(() => {
        if (queued.has(id)) return;
        queued.add(id);
        Queue.offerUnsafe(ready, id);
      });
    const wakeWatchers = (threadId: ThreadId, shell: boolean) =>
      Effect.forEach(
        watchers.get(threadId) ?? [],
        ([id, interest]) => (interest === "all" || shell ? wake(id) : Effect.void),
        { discard: true },
      );

    /** True when the watch gained a thread or effect, whose earlier events this index missed. */
    const index = Effect.fnUntraced(function* (id: ThreadId, watch: SpectrumWatch | null) {
      const old = watches.get(id);
      for (const threadId of old?.threads.keys() ?? []) {
        const entry = watchers.get(threadId);
        entry?.delete(id);
        if (entry?.size === 0) watchers.delete(threadId);
      }
      let grew = false;
      if (watch === null) watches.delete(id);
      else watches.set(id, watch);
      for (const [threadId, interest] of watch?.threads ?? []) {
        if (old?.threads.get(threadId) !== interest) grew = true;
        const entry = watchers.get(threadId) ?? new Map<ThreadId, Interest>();
        entry.set(id, interest);
        watchers.set(threadId, entry);
      }
      const current = waiters.get(id) ?? new Map<string, Fiber.Fiber<void>>();
      const wanted = new Set(watch?.effects ?? []);
      for (const [effectId, fiber] of current) {
        if (wanted.has(effectId)) continue;
        current.delete(effectId);
        yield* Fiber.interrupt(fiber);
      }
      for (const effectId of wanted) {
        if (current.has(effectId)) continue;
        grew = true;
        const fiber = yield* outbox
          .awaitSettled(effectId)
          .pipe(
            Effect.ignoreCause,
            Effect.andThen(Effect.sync(() => current.delete(effectId))),
            Effect.andThen(wake(id)),
            Effect.forkIn(scope),
          );
        current.set(effectId, fiber);
      }
      if (current.size === 0) waiters.delete(id);
      else waiters.set(id, current);
      return grew;
    });

    const catchUp = Effect.gen(function* () {
      const rows = yield* sql<{ readonly thread_id: string }>`SELECT thread_id FROM fork_spectra`;
      for (const row of rows) {
        const id = ThreadId.make(row.thread_id);
        spectra.add(id);
        yield* wake(id);
      }
      yield* Effect.logInfo("Spectrum catch-up", { woken: rows.length });
    }).pipe(Effect.catchCause((cause) => Effect.logWarning("Spectrum catch-up failed", { cause })));

    const refresh = (id: ThreadId) =>
      Effect.gen(function* () {
        const found = yield* readSpectrum(id);
        const watch = Option.isNone(found) ? null : yield* readSpectrumWatch(found.value);
        if (yield* index(id, watch)) yield* wake(id);
      }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.provideService(Projections.ProjectionStoreV2, projections),
        Effect.catchCause((cause) =>
          Effect.logWarning("Spectrum watch refresh failed", { threadId: id, cause }),
        ),
      );

    const route = (stored: OrchestrationV2StoredEvent) =>
      Effect.gen(function* () {
        const event = stored.event;
        if (settlesNothing(event)) return;
        if (event.type === "thread.created") {
          const thread = event.payload;
          // Registration commits the row with the Spectrum thread's creation.
          if (
            !spectra.has(thread.id) &&
            (yield* sql`SELECT 1 FROM fork_spectra WHERE thread_id = ${thread.id}`).length > 0
          )
            spectra.add(thread.id);
          // A new descendant joins the owner's watch on that owner's next resume.
          const parent =
            thread.lineage.relationshipToParent === "subagent"
              ? thread.lineage.parentThreadId
              : null;
          if (parent !== null) {
            if (spectra.has(parent)) yield* wake(parent);
            yield* wakeWatchers(parent, false);
          }
        }
        if (spectra.has(event.threadId)) yield* wake(event.threadId);
        yield* wakeWatchers(event.threadId, event.type === "thread.metadata-updated");
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Spectrum event routing failed", {
            sequence: stored.sequence,
            cause,
          }),
        ),
      );

    const work = Effect.gen(function* () {
      const id = yield* Queue.take(ready);
      queued.delete(id);
      yield* controller
        .resume(id)
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Spectrum resume failed", { threadId: id, cause }),
          ),
        );
      yield* refresh(id);
    }).pipe(Effect.forever);

    // Subscribe before the catch-up: the stream replays from this sequence and signals buffer.
    let cursor = yield* sink.latestSequence().pipe(Effect.orDie);
    const decisions = yield* signals.subscribe;
    const follow = () =>
      sink.stream({ afterSequence: cursor }).pipe(
        Stream.chunks,
        Stream.runForEach((chunk) =>
          Effect.forEach(
            chunk,
            (stored) =>
              route(stored).pipe(Effect.andThen(Effect.sync(() => (cursor = stored.sequence)))),
            { discard: true },
          ),
        ),
      );
    // A lost subscription resumes from the last routed event, then catches up for what routing dropped.
    const followForever = Effect.gen(function* () {
      for (let failures = 1; ; failures = Math.min(failures + 1, 6)) {
        const seen = cursor;
        const cause = yield* follow().pipe(
          Effect.matchCause({ onFailure: (cause) => cause, onSuccess: () => "ended" }),
        );
        if (typeof cause !== "string" && Cause.hasInterruptsOnly(cause)) return;
        if (cursor > seen) failures = 1;
        yield* Effect.logWarning("Spectrum event stream failed; resubscribing", { cause });
        yield* Effect.sleep(Duration.seconds(2 ** (failures - 1)));
        yield* catchUp;
      }
    });
    yield* forkParked(followForever);
    yield* forkParked(
      decisions.pipe(
        Stream.runForEach((threadId) =>
          (spectra.has(threadId) ? wake(threadId) : Effect.void).pipe(
            Effect.andThen(wakeWatchers(threadId, false)),
          ),
        ),
      ),
    );
    yield* forkParked(catchUp.pipe(Effect.andThen(work)));
    // Read and resume failures are logged where they occur and retried on the next tick.
    yield* forkParked(
      Effect.gen(function* () {
        const rows = yield* sql<{ readonly thread_id: string }>`SELECT thread_id FROM fork_spectra
          WHERE json_extract(payload_json, '$.status') = 'active'`.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Spectrum reconcile read failed", { cause }).pipe(Effect.as([])),
          ),
        );
        for (const row of rows) {
          const id = ThreadId.make(row.thread_id);
          spectra.add(id);
          yield* wake(id);
        }
        for (const id of watches.keys()) yield* wake(id);
      }).pipe(Effect.delay(RECONCILE_INTERVAL), Effect.forever),
    );
  }),
);
