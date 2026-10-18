import {
  McpScheduledMutationReceipt,
  OrchestratorMcpFailure,
  ScheduledTaskId,
  ProjectId,
  RuntimeMode,
  ProviderInteractionMode,
  OrchestrationV2LinkOrigin,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { isSqlError } from "effect/sql/SqlError";
import * as SqlClient from "effect/sql/SqlClient";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { requestNamespace } from "../mcp/requestIdentity.ts";
import { requestDigest, type RequestIdentity } from "../mcp/McpRequestReceipts.ts";

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
  resource_json: string | null;
}
export const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_mutation_receipts (request_id TEXT PRIMARY KEY, generation TEXT NOT NULL, input_digest TEXT NOT NULL, task_id TEXT NOT NULL, incarnation TEXT, stage TEXT NOT NULL, result_json TEXT, resource_json TEXT)`;
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(scheduled_mutation_receipts)`).some(
      (column) => column.name === "resource_json",
    )
  )
    yield* sql`ALTER TABLE scheduled_mutation_receipts ADD COLUMN resource_json TEXT`;
});
const RunMode = Schema.Struct({
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
const ResourceEvidence = Schema.Struct({
  owner: Schema.String,
  projectId: ProjectId,
  incarnation: Schema.String.check(Schema.isPattern(/^[0-9a-f]{32}$/)),
  runModes: Schema.Array(RunMode).check(Schema.isMinLength(1), Schema.isMaxLength(2)),
  linkSessionId: Schema.optional(Schema.String),
  authorityRecordId: Schema.optional(Schema.String),
});
export type ResourceEvidence = typeof ResourceEvidence.Type;
const evidenceJson = Schema.fromJsonString(ResourceEvidence);
const encodeEvidence = Schema.encodeEffect(evidenceJson);
const decodeEvidence = Schema.decodeUnknownEffect(evidenceJson);
const decodeOrigin = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2LinkOrigin));
const decodeModes = Schema.decodeUnknownEffect(Schema.Array(RunMode));
const resourceRow = (taskId: ScheduledTaskId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return (yield* sql<{
      project_id: string;
      thread_id: string | null;
      runtime_mode: string;
      interaction_mode: string;
      link_origin_json: string | null;
    }>`SELECT project_id,thread_id,runtime_mode,interaction_mode,link_origin_json FROM scheduled_tasks WHERE task_id=${taskId}`)[0];
  });
const captureEvidence = (
  taskId: ScheduledTaskId,
  incarnation: string,
  row: Effect.Success<ReturnType<typeof resourceRow>>,
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isNone(scope) || row === undefined) return null;
    const sql = yield* SqlClient.SqlClient;
    const bound =
      row.thread_id === null
        ? []
        : yield* sql<{
            runtime_mode: string;
            interaction_mode: string;
          }>`SELECT runtime_mode,interaction_mode FROM orchestration_v2_projection_threads WHERE thread_id=${row.thread_id} AND deleted_at IS NULL`;
    if (row.thread_id !== null && bound.length !== 1) return null;
    const runModes = yield* decodeModes(
      [row, ...bound].map((mode) => ({
        runtimeMode: mode.runtime_mode,
        interactionMode: mode.interaction_mode,
      })),
    ).pipe(Effect.mapError(unavailable));
    return yield* encodeEvidence({
      owner: requestNamespace(scope.value),
      projectId: ProjectId.make(row.project_id),
      incarnation,
      runModes,
      ...(row.link_origin_json === null
        ? {}
        : {
            linkSessionId: (yield* decodeOrigin(row.link_origin_json).pipe(
              Effect.mapError(unavailable),
            )).sessionId,
          }),
      ...(scope.value.authority === undefined
        ? {}
        : { authorityRecordId: scope.value.authority.id }),
    }).pipe(Effect.mapError(unavailable));
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

/** Old/imported confirmations have no retroactively inferred access proof. */
export const acceptance = (request: RequestIdentity) =>
  Effect.gen(function* () {
    const row = yield* read(request);
    if (row?.stage !== "confirmed" || row.result_json === null)
      return Option.none<{ receipt: McpScheduledMutationReceipt; resource: ResourceEvidence }>();
    if (row.resource_json === null)
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "Historical scheduler acceptance has no verified resource ownership evidence; automatic replay is unsupported.",
      });
    const resource = yield* decodeEvidence(row.resource_json).pipe(Effect.mapError(unavailable));
    const receipt = yield* decode(row.result_json).pipe(Effect.mapError(unavailable));
    if (
      resource.incarnation !== row.incarnation ||
      receipt.scheduledTaskId !== row.task_id ||
      receipt.operationId !== request.id ||
      receipt.requestId !== request.clientRequestId ||
      resource.runModes.length === 0
    )
      return yield* unavailable(undefined);
    return Option.some({ receipt, resource });
  });

/** Capture only local authorization-relevant binding and incarnation. Live peer
 * validation must finish outside SQLite so its reverse mailbox can commit.
 * This guard is one admission's value, not an offline authority cache.
 */
export const captureResourceFence = (taskId: ScheduledTaskId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const readBinding = () =>
      sql`SELECT t.task_id,t.project_id,t.thread_id,t.model_selection_json,t.runtime_mode,t.interaction_mode,t.bare_json,t.link_origin_json,i.incarnation,b.runtime_mode AS bound_runtime_mode,b.interaction_mode AS bound_interaction_mode,b.deleted_at AS bound_deleted_at FROM scheduled_tasks t LEFT JOIN scheduled_task_request_incarnations i ON i.task_id=t.task_id LEFT JOIN orchestration_v2_projection_threads b ON b.thread_id=t.thread_id WHERE t.task_id=${taskId}`.pipe(
        Effect.mapError(unavailable),
      );
    const expected = requestDigest(yield* readBinding());
    return Effect.gen(function* () {
      if (requestDigest(yield* readBinding()) !== expected)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The schedule binding changed during authorization; no mutation was accepted.",
        });
    });
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
    const localFence = yield* captureResourceFence(taskId);
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
          yield* localFence;
          const originalResource = yield* resourceRow(taskId);
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
          const resourceJson = yield* captureEvidence(
            taskId,
            acceptedIncarnation,
            (yield* resourceRow(taskId)) ?? originalResource,
          );
          const encoded = yield* encode(receipt).pipe(Effect.mapError(unavailable));
          yield* sql`INSERT INTO scheduled_mutation_receipts (request_id,generation,input_digest,task_id,incarnation,stage,result_json,resource_json) VALUES (${request.id},${request.generation},${request.inputDigest},${taskId},${acceptedIncarnation},'confirmed',${encoded},${resourceJson}) ON CONFLICT(request_id) DO UPDATE SET incarnation=excluded.incarnation,stage=excluded.stage,result_json=excluded.result_json,resource_json=excluded.resource_json`;
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
