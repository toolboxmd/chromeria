import {
  McpScheduledMutationReceipt,
  OrchestratorMcpFailure,
  ScheduledTaskId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { isSqlError } from "effect/sql/SqlError";
import * as SqlClient from "effect/sql/SqlClient";
import type { RequestIdentity } from "../mcp/McpRequestReceipts.ts";

/** Set only after negotiated caller and actual resource preflight. */
export class CurrentMutation extends Context.Reference<
  | {
      readonly request: RequestIdentity;
      readonly consumesSecret: boolean;
      readonly recheck: Effect.Effect<void, OrchestratorMcpFailure>;
    }
  | undefined
>("t3/scheduledTasks/CurrentMutation", { defaultValue: () => undefined }) {}
class InAcceptance extends Context.Reference<boolean>("t3/scheduledTasks/InAcceptance", {
  defaultValue: () => false,
}) {}
const unavailable = (_cause: unknown) =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The native schedule acceptance receipt is unavailable.",
  });
const unknown = (request: RequestIdentity) =>
  new OrchestratorMcpFailure({
    code: "outcome_unknown",
    requestId: request.clientRequestId,
    lastConfirmedStage: "dispatch_started",
    message:
      "The one-use secret may have been consumed before schedule acceptance. Inspect the original request; it will not be consumed again.",
  });
const receiptJson = Schema.fromJsonString(McpScheduledMutationReceipt);
const decode = Schema.decodeUnknownEffect(receiptJson);
const encode = Schema.encodeEffect(receiptJson);
interface Row {
  request_id: string;
  generation: string;
  input_digest: string;
  task_id: string;
  incarnation: string | null;
  stage: string;
  result_json: string | null;
}
export const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_mutation_receipts (request_id TEXT PRIMARY KEY, generation TEXT NOT NULL, input_digest TEXT NOT NULL, task_id TEXT NOT NULL, incarnation TEXT, stage TEXT NOT NULL, result_json TEXT)`;
});
const read = (request: RequestIdentity) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const row =
      (yield* sql<Row>`SELECT * FROM scheduled_mutation_receipts WHERE request_id=${request.id}`.pipe(
        Effect.mapError(unavailable),
      ))[0];
    if (
      row !== undefined &&
      (row.generation !== request.generation || row.input_digest !== request.inputDigest)
    )
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "The schedule acceptance belongs to its original verified intent and grant generation.",
      });
    return row;
  });
export const confirmed = (request: RequestIdentity) =>
  Effect.gen(function* () {
    const row = yield* read(request);
    if (row === undefined || row.result_json === null)
      return Option.none<McpScheduledMutationReceipt>();
    return Option.some(yield* decode(row.result_json).pipe(Effect.mapError(unavailable)));
  });

/** Called under the existing check owner's task lock, before its SQL transaction.
 * Native row, check state and confirmation share this transaction. Secret consumption
 * alone has a committed started stage outside it, since its owner is not SQLite.
 */
export const accept = <A, E, R>(
  taskId: ScheduledTaskId,
  perform: Effect.Effect<A, E, R>,
  replay: Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const mutation = yield* CurrentMutation;
    if (mutation === undefined || (yield* InAcceptance)) return yield* perform;
    const sql = yield* SqlClient.SqlClient;
    const { request } = mutation;
    yield* mutation.recheck;
    const before = yield* read(request);
    if (before?.stage === "confirmed") return yield* replay;
    if (before !== undefined) return yield* unknown(request);
    if (mutation.consumesSecret) {
      const started =
        yield* sql`INSERT INTO scheduled_mutation_receipts (request_id,generation,input_digest,task_id,stage) VALUES (${request.id},${request.generation},${request.inputDigest},${taskId},'secret-started') ON CONFLICT DO NOTHING RETURNING request_id`.pipe(
          Effect.mapError(unavailable),
        );
      if (started.length === 0) {
        const existing = yield* read(request);
        return existing?.stage === "confirmed" ? yield* replay : yield* unknown(request);
      }
    }
    const transaction = sql
      .withTransaction(
        Effect.gen(function* () {
          const current = yield* read(request);
          if (current?.stage === "confirmed") return yield* replay;
          // SQL serialization owns concurrent ordinary mutations. A secret-started row
          // can enter here only in the fiber that won its committed reservation above.
          const incarnation = (yield* sql<{
            incarnation: string;
          }>`SELECT incarnation FROM scheduled_task_request_incarnations WHERE task_id=${taskId}`)[0]
            ?.incarnation;
          yield* mutation.recheck;
          const result = yield* perform.pipe(Effect.provideService(InAcceptance, true));
          const acceptedIncarnation =
            incarnation ??
            (yield* sql<{
              incarnation: string;
            }>`SELECT incarnation FROM scheduled_task_request_incarnations WHERE task_id=${taskId}`)[0]
              ?.incarnation;
          if (acceptedIncarnation === undefined) return yield* unavailable(undefined);
          const receipt: McpScheduledMutationReceipt = {
            type: "confirmed-operation",
            applied: true,
            requestId: request.clientRequestId,
            operationId: request.id,
            stage: "confirmed",
            scheduledTaskId: taskId,
          };
          const encoded = yield* encode(receipt).pipe(Effect.mapError(unavailable));
          yield* sql`INSERT INTO scheduled_mutation_receipts (request_id,generation,input_digest,task_id,incarnation,stage,result_json) VALUES (${request.id},${request.generation},${request.inputDigest},${taskId},${acceptedIncarnation},'confirmed',${encoded}) ON CONFLICT(request_id) DO UPDATE SET incarnation=excluded.incarnation,stage=excluded.stage,result_json=excluded.result_json`;
          return result;
        }),
      )
      .pipe(Effect.catchIf(isSqlError, (error) => Effect.fail(unavailable(error))));
    return yield* mutation.consumesSecret
      ? transaction.pipe(
          Effect.catch(() =>
            Effect.gen(function* () {
              const accepted = yield* read(request);
              if (accepted?.stage === "confirmed") return yield* replay;
              return yield* unknown(request);
            }),
          ),
        )
      : transaction;
  });
