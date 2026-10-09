import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { SpectrumState, type SpectrumMutation } from "./state.ts";

export class SpectrumStoreError extends Schema.TaggedError<SpectrumStoreError>()(
  "SpectrumStoreError",
  {
    threadId: ThreadId,
    cause: Schema.Defect(),
  },
) {}

export class SpectrumMutationRejected extends Schema.TaggedError<SpectrumMutationRejected>()(
  "SpectrumMutationRejected",
  {
    threadId: ThreadId,
    kind: Schema.Literals(["missing", "stale", "invalid-transition"]),
  },
) {}

const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(SpectrumState));
const encode = Schema.encodeEffect(Schema.fromJsonString(SpectrumState));

/** Fork schema initialization; wired by the foundation hook during integration. */
export const ensureSpectrumSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_spectra (
    thread_id TEXT PRIMARY KEY,
    caller_thread_id TEXT NOT NULL,
    scheduler_run_id TEXT,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS fork_spectra_caller ON fork_spectra(caller_thread_id, scheduler_run_id)`;
});

export const readSpectrum = Effect.fn("Spectrum.read")(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM fork_spectra WHERE thread_id = ${threadId}`.pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId, cause })),
  );
  if (rows.length === 0) return Option.none<SpectrumState>();
  const state = yield* decode(rows[0]!.payload_json).pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId, cause })),
  );
  return Option.some(state);
});

/** Registration must share the thread creation transaction in the runtime. */
export const insertSpectrum = Effect.fn("Spectrum.insert")(function* (state: SpectrumState) {
  const sql = yield* SqlClient.SqlClient;
  const payload = yield* encode(state).pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId: state.threadId, cause })),
  );
  yield* sql`INSERT INTO fork_spectra (thread_id, caller_thread_id, scheduler_run_id, generation, revision, payload_json)
    VALUES (${state.threadId}, ${state.callerThreadId}, ${state.schedulerRunId}, ${state.generation}, ${state.revision}, ${payload})`.pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId: state.threadId, cause })),
  );
});

/** Rechecked in the shared fork commit plan, never trusted from command planning. */
export const checkSpectrumMutation = Effect.fn("Spectrum.checkMutation")(function* (
  mutation: SpectrumMutation,
) {
  const threadId = mutation.state.threadId;
  const found = yield* readSpectrum(threadId);
  if (Option.isNone(found))
    return yield* new SpectrumMutationRejected({ threadId, kind: "missing" });
  const current = found.value;
  if (
    current.revision !== mutation.expectedRevision ||
    current.generation !== mutation.expectedGeneration
  ) {
    return yield* new SpectrumMutationRejected({ threadId, kind: "stale" });
  }
  const next = mutation.state;
  if (
    next.revision !== current.revision + 1 ||
    next.cursor < current.cursor ||
    next.callerThreadId !== current.callerThreadId ||
    next.callerRunId !== current.callerRunId ||
    next.scheduledTaskId !== current.scheduledTaskId ||
    next.schedulerRunId !== current.schedulerRunId ||
    next.generation < current.generation ||
    next.generation > current.generation + 1 ||
    (current.status === "retired" &&
      next.generation === current.generation &&
      next.status !== "retired")
  )
    return yield* new SpectrumMutationRejected({ threadId, kind: "invalid-transition" });
});

/** The caller's transaction also commits events and receipts, or rolls everything back. */
export const applySpectrumMutation = Effect.fn("Spectrum.applyMutation")(function* (
  mutation: SpectrumMutation,
) {
  yield* checkSpectrumMutation(mutation);
  const sql = yield* SqlClient.SqlClient;
  const next = mutation.state;
  const payload = yield* encode(next).pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId: next.threadId, cause })),
  );
  const updated = yield* sql<{ thread_id: string }>`UPDATE fork_spectra
    SET generation = ${next.generation}, revision = ${next.revision}, payload_json = ${payload}
    WHERE thread_id = ${next.threadId} AND generation = ${mutation.expectedGeneration}
      AND revision = ${mutation.expectedRevision} RETURNING thread_id`.pipe(
    Effect.mapError((cause) => new SpectrumStoreError({ threadId: next.threadId, cause })),
  );
  if (updated.length !== 1)
    return yield* new SpectrumMutationRejected({ threadId: next.threadId, kind: "stale" });
});
