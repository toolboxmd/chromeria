import { interactiveAuthority } from "../peer/PeerAuthorityRecord.ts";
import { isSqlError } from "effect/sql/SqlError";
import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { McpInvocationScope } from "./McpInvocationContext.ts";
import { requestGeneration, requestNamespace } from "./requestIdentity.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ value: Schema.optional(Schema.Unknown) })),
);

/** Semantic object order is immaterial; array order and every supplied value remain significant. */
export function canonicalRequest(value: unknown): string {
  const normalize = (input: unknown): unknown => {
    if (Array.isArray(input)) return input.map(normalize);
    if (typeof input !== "object" || input === null) return input;
    return Object.fromEntries(
      Object.entries(input)
        .filter(([, v]) => v !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, normalize(v)]),
    );
  };
  return json(normalize(value));
}
export const requestDigest = (value: unknown) =>
  Hex.encode(sha256(new TextEncoder().encode(canonicalRequest(value))));

export interface RequestIdentity {
  readonly id: string;
  readonly generation: string;
  readonly inputDigest: string;
  readonly clientRequestId: string;
}

export class CurrentRequest extends Context.Reference<RequestIdentity | undefined>(
  "t3/mcp/CurrentRequest",
  { defaultValue: () => undefined },
) {}
export class CommandIndex extends Context.Reference<Ref.Ref<number> | undefined>(
  "t3/mcp/CommandIndex",
  { defaultValue: () => undefined },
) {}
export class CurrentOperation extends Context.Reference<
  { readonly tool: string; readonly input: unknown } | undefined
>("t3/mcp/CurrentOperation", { defaultValue: () => undefined }) {}

const NotDispatched = Context.Reference<Effect.Effect<void> | undefined>("t3/mcp/NotDispatched", {
  defaultValue: () => undefined,
});
/** Only an actual delivery owner may prove that this attempt reached no adapter.
 * The callback is scoped to the winning attempt, never decoded from public errors.
 */
export const confirmNotDispatched = Effect.gen(function* () {
  const confirm = yield* NotDispatched;
  if (confirm !== undefined) yield* confirm;
});

const unavailable = (_cause: unknown) =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The durable request receipt is unavailable; no new action was authorized.",
  });
const unknownOutcome = (identity: RequestIdentity) =>
  new OrchestratorMcpFailure({
    code: "outcome_unknown",
    message:
      "The external action may have happened. Inspect its destination before choosing a new request key.",
    requestId: identity.clientRequestId,
    lastConfirmedStage: "dispatch_started",
  });

/** Binds intent, but leaves app command acceptance to the existing orchestrator. */
export class McpRequestReceipts extends Context.Service<
  McpRequestReceipts,
  {
    readonly identify: (
      scope: McpInvocationScope,
      tool: string,
      input: unknown,
      key?: string,
    ) => Effect.Effect<RequestIdentity, OrchestratorMcpFailure>;
    readonly destination: (
      identity: RequestIdentity,
      operation: string,
    ) => Effect.Effect<
      Option.Option<{ environmentId: string; generation: string }>,
      OrchestratorMcpFailure
    >;
    readonly bindDestination: (
      identity: RequestIdentity,
      operation: string,
      environmentId: string,
      generation: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly replay: <A>(
      identity: RequestIdentity,
    ) => Effect.Effect<Option.Option<A>, OrchestratorMcpFailure>;
    /** Only for mutations executed by this same SQLite owner and transaction. */
    readonly sqlMutation: <A, E, R>(
      identity: RequestIdentity,
      perform: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | OrchestratorMcpFailure, R>;
    readonly external: <A, E, R>(
      identity: RequestIdentity,
      perform: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | OrchestratorMcpFailure, R>;
  }
>()("t3/mcp/McpRequestReceipts") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  yield* sql`CREATE TABLE IF NOT EXISTS mcp_request_receipts (
    id TEXT PRIMARY KEY, generation TEXT NOT NULL, input_digest TEXT NOT NULL,
    state TEXT NOT NULL, result_json TEXT, attempt_id TEXT)`.pipe(Effect.orDie);
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(mcp_request_receipts)`).some(
      (column) => column.name === "attempt_id",
    )
  )
    yield* sql`ALTER TABLE mcp_request_receipts ADD COLUMN attempt_id TEXT`.pipe(Effect.orDie);
  yield* sql`CREATE TABLE IF NOT EXISTS mcp_request_destinations (request_id TEXT NOT NULL, operation TEXT NOT NULL, environment_id TEXT NOT NULL, generation TEXT NOT NULL, PRIMARY KEY(request_id,operation))`.pipe(
    Effect.orDie,
  );
  // Imported receipts lack a verified caller/input binding. An exact legacy
  // operation/key match is evidence of possible prior acceptance, never proof
  // that a different namespace can safely launch again. Keep it unsupported.
  const tables = new Set(
    (yield* sql<{ name: string }>`SELECT name FROM sqlite_master WHERE type='table'`).map(
      (row) => row.name,
    ),
  );
  const legacyConflict = (scope: McpInvocationScope, tool: string, input: unknown, key: string) =>
    Effect.gen(function* () {
      const encoded = encodeURIComponent(key);
      if (tables.has("orchestration_command_receipts")) {
        const candidates = yield* sql<{
          command_id: string;
        }>`SELECT command_id FROM orchestration_command_receipts
        WHERE instr(command_id,${`:${encoded}`}) > 0`;
        const operations: Readonly<Record<string, ReadonlyArray<string>>> = {
          create_threads: ["create-thread"],
          delegate_task: ["delegate-task"],
          t3_thread_send: ["thread-send"],
          t3_thread_interrupt: ["thread-stop", "thread-interrupt"],
          task_cancel: ["cancel-task"],
        };
        for (const { command_id: id } of candidates) {
          const parts = id.split(":");
          const launch =
            tool === "t3_thread_launch" &&
            parts[0] === "mcp" &&
            parts[1] === "launch" &&
            parts[3] === encoded;
          const spectrum =
            tool === "start_spectrum" &&
            parts[0] === "spectrum" &&
            parts[2] === encoded &&
            parts[3] === "register";
          const target =
            typeof input === "object" &&
            input !== null &&
            "threadId" in input &&
            typeof input.threadId === "string"
              ? input.threadId
              : scope.thread?.threadId;
          const metadata =
            target !== undefined &&
            parts[4] === encodeURIComponent(target) &&
            tool === "t3_thread_update" &&
            parts[0] === "command" &&
            parts[1] === "mcp" &&
            parts[3] === "thread-update" &&
            parts[6] === encoded;
          const command =
            parts[0] === "command" &&
            parts[1] === "mcp" &&
            parts[4] === encoded &&
            operations[tool]?.includes(parts[3]!);
          if (!launch && !spectrum && !metadata && !command) continue;
          const namespace = parts[spectrum ? 1 : 2]!;
          // A verified modern caller owns its namespace, never this raw key in
          // every other namespace. Missing ownership remains safe unsupported.
          let decoded: string;
          try {
            decoded = decodeURIComponent(namespace);
          } catch {
            return true;
          }
          const modernId = requestDigest([decoded, tool, key]);
          if ((yield* sql`SELECT id FROM mcp_request_receipts WHERE id=${modernId}`).length === 0)
            return true;
        }
      }
      if (tool === "schedule_task" && tables.has("scheduled_tasks")) {
        const rows = yield* sql<{
          task_id: string;
        }>`SELECT task_id FROM scheduled_tasks WHERE instr(task_id,${`:${key}`}) > 0 OR instr(task_id,${`:${encoded}`}) > 0`;
        for (const { task_id: id } of rows) {
          if (!id.endsWith(`:${key}`) && !id.endsWith(`:schedule-task:${encoded}`)) continue;
          const hasScheduleReceipts =
            tables.has("scheduled_mutation_receipts") ||
            (yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='scheduled_mutation_receipts'`)
              .length > 0;
          const owned = hasScheduleReceipts
            ? yield* sql`SELECT r.id FROM scheduled_mutation_receipts s JOIN mcp_request_receipts r ON r.id=s.request_id WHERE s.task_id=${id} AND s.stage='confirmed'`
            : [];
          if (owned.length === 0) return true;
        }
      }
      return false;
    });
  const identify: McpRequestReceipts["Service"]["identify"] = (scope, tool, input, key) =>
    Effect.gen(function* () {
      const clientRequestId = key ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie));
      const id = requestDigest([requestNamespace(scope), tool, clientRequestId]);
      const identity = {
        id,
        clientRequestId,
        generation: requestGeneration(scope),
        // Capability transport metadata is authenticated independently. Its set
        // order or renewal cannot turn the same authorized intent into a conflict.
        inputDigest: requestDigest(
          typeof input === "object" && input !== null && !Array.isArray(input)
            ? Object.fromEntries(
                Object.entries(input).filter(([key]) => key !== "contextCapabilities"),
              )
            : input,
        ),
      };
      const existing = yield* sql`SELECT id FROM mcp_request_receipts WHERE id=${id}`;
      if (existing.length === 0 && (yield* legacyConflict(scope, tool, input, clientRequestId)))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "A legacy operation with this request key has no verifiable input/owner binding. Inspect its original result; automatic replay is unsupported.",
        });
      yield* sql`INSERT INTO mcp_request_receipts (id,generation,input_digest,state)
        VALUES (${id},${identity.generation},${identity.inputDigest},'bound') ON CONFLICT DO NOTHING`;
      const rows = yield* sql<{
        generation: string;
        input_digest: string;
      }>`SELECT generation,input_digest FROM mcp_request_receipts WHERE id=${id}`;
      const row = rows[0];
      if (row === undefined) return yield* unavailable(undefined);
      if (row.generation !== identity.generation)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "This request belongs to its original grant generation. Replacement cannot replay it or create duplicate work.",
        });
      if (row.input_digest !== identity.inputDigest)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The request key was already bound to different input.",
        });
      return identity;
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure" ? error : unavailable(error),
      ),
    );
  const replay = <A>(
    identity: RequestIdentity,
  ): Effect.Effect<Option.Option<A>, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        state: string;
        result_json: string | null;
      }>`SELECT state,result_json FROM mcp_request_receipts WHERE id=${identity.id}
      AND generation=${identity.generation} AND input_digest=${identity.inputDigest}`.pipe(
        Effect.mapError(unavailable),
      );
      const row = rows[0];
      if (row?.state === "bound") return Option.none();
      if (row?.state === "completed" && row.result_json !== null)
        return Option.some(
          (yield* decodeJson(row.result_json).pipe(Effect.mapError(unavailable))).value as A,
        );
      return yield* unknownOutcome(identity);
    });
  const external = <A, E, R>(
    identity: RequestIdentity,
    perform: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | OrchestratorMcpFailure, R> =>
    Effect.gen(function* () {
      const attempt = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const notDispatched = yield* Ref.make(false);
      const changed =
        yield* sql`UPDATE mcp_request_receipts SET state='started',attempt_id=${attempt}
        WHERE id=${identity.id} AND generation=${identity.generation} AND input_digest=${identity.inputDigest}
          AND state='bound' RETURNING id`.pipe(Effect.mapError(unavailable));
      if (changed.length === 0) {
        const result = yield* replay<A>(identity);
        return Option.isSome(result) ? result.value : yield* unknownOutcome(identity);
      }
      const result = yield* perform.pipe(
        Effect.provideService(NotDispatched, Ref.set(notDispatched, true)),
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (yield* Ref.get(notDispatched)) {
              const reset =
                yield* sql`UPDATE mcp_request_receipts SET state='bound',attempt_id=NULL WHERE id=${identity.id} AND generation=${identity.generation} AND input_digest=${identity.inputDigest} AND state='started' AND attempt_id=${attempt} RETURNING id`.pipe(
                  Effect.mapError(() => unknownOutcome(identity)),
                );
              if (reset.length === 1) return yield* Effect.failCause(cause);
            }
            return yield* unknownOutcome(identity);
          }),
        ),
      );
      const encoded = json({ value: result });
      // Large media remains at its artifact owner, never in a generic receipt row.
      if (encoded.length > 64_000) return yield* unknownOutcome(identity);
      yield* sql`UPDATE mcp_request_receipts SET state='completed', result_json=${encoded}
        WHERE id=${identity.id} AND generation=${identity.generation} AND input_digest=${identity.inputDigest}`.pipe(
        Effect.mapError(() => unknownOutcome(identity)),
      );
      return result;
    });
  const sqlMutation = <A, E, R>(
    identity: RequestIdentity,
    perform: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | OrchestratorMcpFailure, R> =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const existing = yield* replay<A>(identity);
          if (Option.isSome(existing)) return existing.value;
          const value = yield* perform;
          const encoded = json({ value });
          if (encoded.length > 64_000) return yield* unavailable(undefined);
          yield* sql`UPDATE mcp_request_receipts SET state='completed',result_json=${encoded} WHERE id=${identity.id} AND generation=${identity.generation} AND input_digest=${identity.inputDigest} AND state='bound'`;
          return value;
        }),
      )
      .pipe(Effect.catchIf(isSqlError, (error) => Effect.fail(unavailable(error))));
  const bindDestination: McpRequestReceipts["Service"]["bindDestination"] = (
    identity,
    operation,
    environmentId,
    generation,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO mcp_request_destinations (request_id,operation,environment_id,generation)
      SELECT id,${operation},${environmentId},${generation} FROM mcp_request_receipts
      WHERE id=${identity.id} AND generation=${identity.generation} AND input_digest=${identity.inputDigest}
      ON CONFLICT DO NOTHING`;
          const row = (yield* sql<{
            environment_id: string;
            generation: string;
          }>`SELECT environment_id,generation FROM mcp_request_destinations WHERE request_id=${identity.id} AND operation=${operation}`)[0];
          if (row === undefined) return yield* unavailable(undefined);
          if (row.environment_id !== environmentId || row.generation !== generation)
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message:
                "This request belongs to its original destination grant generation. Replacement cannot replay it or create duplicate work.",
            });
        }),
      )
      .pipe(Effect.catchIf(isSqlError, (error) => Effect.fail(unavailable(error))));
  const destination: McpRequestReceipts["Service"]["destination"] = (identity, operation) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        environment_id: string;
        generation: string;
      }>`SELECT d.environment_id,d.generation FROM mcp_request_destinations d JOIN mcp_request_receipts r ON r.id=d.request_id WHERE r.id=${identity.id} AND r.generation=${identity.generation} AND r.input_digest=${identity.inputDigest} AND d.operation=${operation}`.pipe(
        Effect.mapError(unavailable),
      );
      return rows[0] === undefined
        ? Option.none()
        : Option.some({ environmentId: rows[0].environment_id, generation: rows[0].generation });
    });
  return McpRequestReceipts.of({
    identify,
    replay,
    external,
    sqlMutation,
    bindDestination,
    destination,
  });
});

export const layer = Layer.effect(McpRequestReceipts, make);

/** Called at the actual external dispatch boundary, after resource/consent checks. */
export const externalDispatch = <A, E, R>(perform: Effect.Effect<A, E, R>, partition?: unknown) =>
  Effect.gen(function* () {
    const operation = yield* CurrentOperation;
    if (operation === undefined) return yield* perform;
    const receipts = yield* Effect.serviceOption(McpRequestReceipts);
    const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isNone(receipts) || Option.isNone(scope)) return yield* unavailable(undefined);
    const parent = yield* CurrentRequest;
    const identity =
      partition === undefined
        ? (parent ?? (yield* receipts.value.identify(scope.value, operation.tool, operation.input)))
        : yield* receipts.value.identify(
            scope.value,
            operation.tool,
            { input: operation.input, partition },
            parent === undefined ? undefined : requestDigest([parent.clientRequestId, partition]),
          );
    return yield* receipts.value.external(identity, perform);
  });

/** Read a prior outcome after caller authorization, before requiring a new external host. */
export const externalReplay = <A>() =>
  Effect.gen(function* () {
    const operation = yield* CurrentOperation;
    const identity = yield* CurrentRequest;
    if (operation === undefined || identity === undefined) return Option.none<A>();
    const receipts = yield* Effect.serviceOption(McpRequestReceipts);
    if (Option.isNone(receipts)) return yield* unavailable(undefined);
    return yield* receipts.value.replay<A>(identity);
  });

/** Direct SQL owners call this inside their mutation, after actual caller/resource preflight. */
export const ownedSqlMutation = <A, E, R>(perform: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const identity = yield* CurrentRequest;
    const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (
      identity === undefined ||
      Option.isNone(scope) ||
      (scope.value.client?.linked === true &&
        interactiveAuthority(scope.value.authority)?.envelope.requestOwner === undefined)
    )
      return yield* perform;
    const receipts = yield* Effect.serviceOption(McpRequestReceipts);
    if (Option.isNone(receipts)) return yield* unavailable(undefined);
    return yield* receipts.value.sqlMutation(identity, perform);
  });
