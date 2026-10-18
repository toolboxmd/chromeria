import * as ScheduleReceipts from "../scheduledTasks/mutationReceipts.ts";
import type { RequestIdentity } from "../mcp/McpRequestReceipts.ts";
import type { McpScheduledMutationReceipt } from "@t3tools/contracts";
import { Authority, PeerSession, intersect } from "./PeerAuthorityRecord.ts";
export { Authority, PeerSession } from "./PeerAuthorityRecord.ts";
import { readStopDescendants } from "../childThreads/stopDescendants.ts";
import { initializePeerEffectAuthority } from "./peerEffectAuthority.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import { exceededDispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import {
  AuthSessionId,
  EnvironmentId,
  MessageId,
  OrchestratorMcpFailure,
  OrchestratorMcpThreadSendResult,
  OrchestratorMcpThreadSendInput,
  PeerAuthorityEnvelope,
  PeerCeiling,
  PeerGrant,
  PeerGrantSummary,
  PeerReturnPair,
  type PeerTaskLifecycleRequest,
  ThreadId,
  RunId,
  ScheduledTaskId,
  OrchestrationV2LinkOrigin,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as PeerNoticeDelivery from "./PeerNoticeDelivery.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as ServerSettings from "../serverSettings.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { requestNamespace, requestGeneration } from "../mcp/requestIdentity.ts";
import { requestDigest } from "../mcp/McpRequestReceipts.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Links from "./PeerLinks.ts";

export type ResourceTarget =
  | { readonly type: "thread"; readonly threadId: ThreadId }
  | { readonly type: "schedule"; readonly taskId: ScheduledTaskId }
  | { readonly type: "environment" };
type ResourceBinding =
  | { readonly type: "thread"; readonly thread: OrchestrationV2ThreadShell }
  | {
      readonly type: "schedule";
      readonly linkOrigin: Pick<OrchestrationV2LinkOrigin, "sessionId"> | undefined;
    }
  | { readonly type: "environment" };

export const AUTHORITY_HEADER = "t3-peer-authority";
export const LINK_HEADER = "t3-peer-link";
const StopControl = Schema.Struct({
  pair: PeerReturnPair,
  requestId: Schema.String,
  reason: Schema.optional(Schema.String),
});
type StopControl = typeof StopControl.Type;
type StopValidation = StopControl & { readonly upstream?: StopControl };
const Validation = Schema.Struct({
  type: Schema.Literal("validate"),
  envelope: PeerAuthorityEnvelope,
  stop: Schema.optional(StopControl),
});
const ReturnNotice = Schema.Struct({
  type: Schema.Literal("return"),
  pair: PeerReturnPair,
  senderRunId: RunId,
  requestId: Schema.String,
  text: OrchestratorMcpThreadSendInput.fields.message,
  mode: Schema.Literals(["auto", "queue"]),
});
const PendingActor = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("thread"),
    environmentId: EnvironmentId,
    threadId: ThreadId,
  }),
  Schema.Struct({
    type: Schema.Literal("client"),
    environmentId: EnvironmentId,
    sessionId: AuthSessionId,
  }),
]);
type PendingActor = typeof PendingActor.Type;
/** Private service-owned markers; no MCP parameter supplies either value. */
export const PendingRequestRelay = Context.Reference<boolean>("t3/peer/PendingRequestRelay", {
  defaultValue: () => false,
});
export const PendingClientSession = Context.Reference<
  { readonly sessionId: string; readonly readOnly: boolean } | undefined
>("t3/peer/PendingClientSession", { defaultValue: () => undefined });
const Frame = Schema.Struct({
  id: Schema.String,
  body: Schema.Union([
    Validation,
    ReturnNotice,
    Schema.Struct({ type: Schema.Literal("request-actor"), envelope: PeerAuthorityEnvelope }),
    Schema.Struct({
      type: Schema.Literal("request-ancestry"),
      pair: PeerReturnPair,
      actor: PendingActor,
      remaining: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
    }),
    Schema.Struct({
      type: Schema.Literal("pair"),
      envelope: PeerAuthorityEnvelope,
      pair: PeerReturnPair,
    }),
  ]),
});
type Frame = typeof Frame.Type;
const Answer = Schema.Struct({
  id: Schema.String,
  result: Schema.Union([
    Schema.Struct({ ok: Schema.Literal(true), value: Schema.Unknown }),
    Schema.Struct({ ok: Schema.Literal(false), message: Schema.String }),
  ]),
});
export const Exchange = Schema.Struct({
  ...PeerSession.fields,
  answers: Schema.Array(Answer),
  after: Schema.optionalKey(Schema.Number),
  wait: Schema.optionalKey(Schema.Boolean),
});
export const ExchangeResult = Schema.Struct({ frames: Schema.Array(Frame), cursor: Schema.Number });
export type Exchange = typeof Exchange.Type;
const ReturnReceipt = OrchestratorMcpThreadSendResult;
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeAuthority = Schema.decodeUnknownEffect(Schema.fromJsonString(Authority));
const decodeFrame = Schema.decodeUnknownEffect(Schema.fromJsonString(Frame));
const decodeAnswer = Schema.decodeUnknownEffect(Schema.fromJsonString(Answer.fields.result));
const decodeCeiling = Schema.decodeUnknownEffect(PeerCeiling);
const decodeReceipt = Schema.decodeUnknownEffect(ReturnReceipt);
const decodeStoredCeiling = Schema.decodeUnknownEffect(Schema.fromJsonString(PeerCeiling));
const decodeGrant = Schema.decodeUnknownEffect(Schema.fromJsonString(PeerGrant));
const decodePair = Schema.decodeUnknownEffect(Schema.fromJsonString(PeerReturnPair));
const decodeLinkOrigin = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2LinkOrigin),
);
const decodeExpiry = Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString);
const encodeGrant = Schema.encodeEffect(Schema.fromJsonString(PeerGrant));
const denied = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Peer authority is unavailable; no new action was authorized.",
  });

/** Durable authority and a bounded reverse mailbox. The dialer drains it concurrently with tools. */
export class PeerAuthority extends Context.Service<
  PeerAuthority,
  {
    readonly historicalScheduleReceipt: (
      scope: Invocation.McpInvocationScope,
      request: RequestIdentity,
      expectedProjectId?: string,
    ) => Effect.Effect<Option.Option<McpScheduledMutationReceipt>, OrchestratorMcpFailure>;
    readonly checkRecord: (id: string) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly recheckInvocation: (
      scope: Invocation.McpThreadInvocationScope,
      capability: "device" | "preview",
      threadId: ThreadId,
    ) => Effect.Effect<Invocation.McpThreadInvocationScope, OrchestratorMcpFailure>;
    readonly authorizeStopEffect: (
      effectId: string,
      threadId: ThreadId,
    ) => Effect.Effect<string | undefined, OrchestratorMcpFailure>;
    readonly authorizeTaskStop: (
      scope: Invocation.McpInvocationScope,
      input: PeerTaskLifecycleRequest & {
        operation: Extract<PeerTaskLifecycleRequest["operation"], { type: "stop" }>;
      },
    ) => Effect.Effect<Authority, OrchestratorMcpFailure>;
    /** Authenticates only an already committed receipt, never a new operation. */
    readonly verifyTaskReceipt: (
      scope: Invocation.McpInvocationScope,
      input: PeerTaskLifecycleRequest,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly verifyTask: (
      scope: Invocation.McpInvocationScope,
      input: PeerTaskLifecycleRequest,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly pendingWithinTask: (
      ancestor: { readonly environmentId: EnvironmentId; readonly threadId: ThreadId },
      target: ThreadId,
    ) => Effect.Effect<boolean, OrchestratorMcpFailure>;
    readonly pendingActor: (
      scope: Invocation.McpInvocationScope,
      target: ThreadId,
    ) => Effect.Effect<"ancestor" | "client" | undefined, OrchestratorMcpFailure>;
    readonly enroll: (
      scope: Invocation.McpInvocationScope,
      peer: typeof PeerSession.Type,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly grants: Effect.Effect<ReadonlyArray<PeerGrantSummary>, OrchestratorMcpFailure>;
    readonly setGrant: (grant: PeerGrant) => Effect.Effect<PeerGrant, OrchestratorMcpFailure>;
    readonly exchange: (
      scope: Invocation.McpInvocationScope,
      input: Exchange,
    ) => Effect.Effect<typeof ExchangeResult.Type, OrchestratorMcpFailure>;
    readonly process: (
      peer: EnvironmentId,
      generation: string,
      frame: Frame,
    ) => Effect.Effect<typeof Answer.Type>;
    readonly prepare: (
      scope: Invocation.McpInvocationScope,
      peer: EnvironmentId,
      generation: string,
      tool: string,
      args: unknown,
      limits: Invocation.ClientModes,
      pair?: Authority["pair"],
      readOnly?: boolean,
      stableRequests?: boolean,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly accept: (
      scope: Invocation.McpInvocationScope,
      envelope: PeerAuthorityEnvelope,
    ) => Effect.Effect<Authority, OrchestratorMcpFailure>;
    readonly checkPayload: (
      authority: Authority,
      tool: string,
      args: unknown,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly bind: (
      authority: Authority,
      threadId: ThreadId,
      registerReturn?: true,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly checkEffect: (
      effectId: string,
      threadId: ThreadId,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly checkThread: (
      threadId: ThreadId,
    ) => Effect.Effect<Authority | undefined, OrchestratorMcpFailure>;
    readonly legacyCeiling: (
      threadId: ThreadId,
    ) => Effect.Effect<PeerCeiling | undefined, OrchestratorMcpFailure>;
    readonly checkTarget: (
      scope: Invocation.McpInvocationScope,
      target: ResourceTarget,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly forThread: (
      threadId: ThreadId,
    ) => Effect.Effect<Authority | undefined, OrchestratorMcpFailure>;
    readonly returnNotice: (
      scope: Invocation.McpInvocationScope,
      environmentId: EnvironmentId,
      input: { threadId: ThreadId; text: string; mode?: string; clientRequestId?: string },
    ) => Effect.Effect<typeof ReturnReceipt.Type | undefined, OrchestratorMcpFailure>;
    readonly registerPair: (
      envelope: PeerAuthorityEnvelope,
      pair: PeerReturnPair,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerAuthority") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const authSessions = yield* AuthSessions.AuthSessionRepository;
  const crypto = yield* Crypto.Crypto;
  const threads = yield* ProjectionStore.ProjectionStoreV2;
  const links = yield* Links.PeerLinks;
  const here = yield* Environment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  yield* sql`CREATE TABLE IF NOT EXISTS peer_authority_sessions (session_id TEXT PRIMARY KEY, environment_id TEXT NOT NULL, generation TEXT NOT NULL, grant_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_authority_records (id TEXT PRIMARY KEY, record_json TEXT NOT NULL)`;
  yield* initializePeerEffectAuthority;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_legacy_authority (thread_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, ceiling_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_thread_authority (thread_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_return_pairs (task_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, pair_json TEXT NOT NULL)`;
  const controlSchema = `CREATE TABLE IF NOT EXISTS peer_control_frames (cursor INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL, frame_json TEXT NOT NULL, answer_json TEXT)`;
  yield* sql.unsafe(controlSchema);
  const schema = yield* sql<{
    sql: string;
  }>`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'peer_control_frames'`;
  if (!schema[0]?.sql.includes("AUTOINCREMENT")) {
    // Preserve old cursors and receipts. Removing transient frames must never
    // reuse a cursor already observed by an outbound-only stream.
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`ALTER TABLE peer_control_frames RENAME TO peer_control_frames_previous`;
        yield* sql.unsafe(controlSchema);
        yield* sql`INSERT INTO peer_control_frames (cursor, id, session_id, frame_json, answer_json) SELECT rowid, id, session_id, frame_json, answer_json FROM peer_control_frames_previous ORDER BY rowid`;
        yield* sql`DROP TABLE peer_control_frames_previous`;
      }),
    );
  }
  yield* sql`CREATE INDEX IF NOT EXISTS peer_control_pending ON peer_control_frames (session_id) WHERE answer_json IS NULL`;
  yield* sql`CREATE INDEX IF NOT EXISTS peer_return_by_record ON peer_return_pairs (record_id)`;
  // Validation requests have a live waiter, unlike durable pair/return receipts.
  // A restarted server cannot resume that prior process's interactive request.
  yield* sql`DELETE FROM peer_control_frames WHERE json_extract(frame_json, '$.body.type') IN ('validate','request-actor','request-ancestry')`;
  // Signals only avoid idle queries. Frames and answers are durable and replay after restart.
  const signals = new Map<string, Set<Deferred.Deferred<void>>>();
  const wake = (key: string) =>
    Effect.gen(function* () {
      const waiting = signals.get(key);
      // Remove before completing: completion may immediately register a new waiter.
      signals.delete(key);
      if (waiting) yield* Effect.forEach(waiting, (signal) => Deferred.succeed(signal, undefined));
    });
  const signalFor = (key: string) =>
    Effect.gen(function* () {
      const signal = yield* Deferred.make<void>();
      // Read and update together, after the allocation yield. Each concurrent
      // waiter owns its registration and cannot replace or release another's.
      const waiting = signals.get(key) ?? new Set<Deferred.Deferred<void>>();
      waiting.add(signal);
      signals.set(key, waiting);
      return signal;
    });
  const releaseSignal = (key: string, signal: Deferred.Deferred<void>) =>
    Effect.sync(() => {
      const waiting = signals.get(key);
      waiting?.delete(signal);
      if (waiting?.size === 0) signals.delete(key);
    });
  const wait = (key: string, signal: Deferred.Deferred<void>, millis: number) =>
    Deferred.await(signal).pipe(
      Effect.timeoutOption(millis),
      Effect.ensuring(releaseSignal(key, signal)),
    );

  const hash = (value: unknown) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(json(value)))
      .pipe(Effect.map(Hex.encode), Effect.orDie);
  const read = (id: string): Effect.Effect<Authority, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM peer_authority_records WHERE id = ${id}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0]) return yield* denied("Unknown peer authority record.");
      return yield* decodeAuthority(rows[0].record_json).pipe(Effect.mapError(unavailable));
    });
  const save = (record: Authority) =>
    sql`INSERT INTO peer_authority_records (id, record_json) VALUES (${record.id}, ${json(record)}) ON CONFLICT DO NOTHING`.pipe(
      Effect.asVoid,
      Effect.mapError(unavailable),
    );
  const permanentForThread: PeerAuthority["Service"]["forThread"] = (threadId) =>
    Effect.gen(function* () {
      const seen = new Set<ThreadId>();
      let current: ThreadId | null = threadId;
      let legacyOrigin: OrchestrationV2ThreadShell | undefined;
      while (current !== null && !seen.has(current)) {
        seen.add(current);
        const rows = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_thread_authority WHERE thread_id = ${current}`.pipe(
          Effect.mapError(unavailable),
        );
        if (rows[0]) return yield* read(rows[0].record_id);
        const shell: OrchestrationV2ThreadShell | null = yield* threads
          .getThreadShell(current)
          .pipe(Effect.mapError(unavailable));
        if (shell?.linkOrigin !== undefined && legacyOrigin === undefined) legacyOrigin = shell;
        current = shell?.lineage.parentThreadId ?? null;
      }
      if (legacyOrigin?.linkOrigin !== undefined) {
        // Pre-protocol work has no verifiable remote action. Preserve its local
        // origin and modes without manufacturing a return pair or wider grant.
        const ceiling: PeerCeiling = {
          runtimeMode: legacyOrigin.runtimeMode,
          interactionMode: legacyOrigin.interactionMode,
          resourceScope: "created-work",
          onward: false,
          capabilities: ["orchestration", "worktree", "pull-requests"],
        };
        yield* sql`INSERT INTO peer_legacy_authority (thread_id, session_id, ceiling_json) VALUES (${threadId}, ${legacyOrigin.linkOrigin.sessionId}, ${json(ceiling)}) ON CONFLICT DO NOTHING`.pipe(
          Effect.mapError(unavailable),
        );
      }
      return undefined;
    });
  // Creation ancestry is durable. An otherwise local thread adopts only its
  // own active run, never an ancestor's current run. An existing job and a
  // different incoming run constrain each other without replacing either.
  const forThread: PeerAuthority["Service"]["forThread"] = (threadId) =>
    Effect.gen(function* () {
      const created = yield* permanentForThread(threadId);
      const shell = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
      if (shell?.activeRunId == null) return created;
      const rows = yield* sql<{
        record_id: string;
      }>`SELECT record_id FROM peer_run_authority WHERE run_id = ${shell.activeRunId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0]) return created;
      const run = yield* read(rows[0].record_id);
      if (created === undefined) {
        const imported = yield* legacy(threadId);
        if (imported === undefined) return run;
        if (
          run.context?.type === "legacy-thread" &&
          run.context.threadId === threadId &&
          run.resourceSessionId === imported.sessionId
        )
          return run;
        const context = {
          type: "legacy-thread" as const,
          threadId,
          runRecordId: run.id,
        };
        const id = `context:${yield* hash(context)}`;
        yield* save({
          ...run,
          id,
          context,
          resourceSessionId: AuthSessionId.make(imported.sessionId),
          ceiling: intersect(run.ceiling, imported.ceiling),
          envelope: { ...run.envelope, recordId: id, actionId: id, digest: id },
        });
        return yield* read(id);
      }
      if (
        created.id === run.id ||
        (run.context?.type === "owned-thread" && run.context.creationRecordId === created.id)
      )
        return run;
      const context = {
        type: "owned-thread" as const,
        threadId,
        creationRecordId: created.id,
        runRecordId: run.id,
      };
      const id = `context:${yield* hash(context)}`;
      yield* save({
        ...created,
        id,
        context,
        ceiling: intersect(created.ceiling, run.ceiling),
        // This derived record is not an issuer's original action. Readers that
        // do not understand local intersections must fail its issuer lookup,
        // rather than silently validate just one of the two owners.
        envelope: { ...created.envelope, recordId: id, actionId: id, digest: id },
      });
      return yield* read(id);
    });
  const legacy = (threadId: ThreadId) =>
    Effect.gen(function* () {
      yield* permanentForThread(threadId);
      const rows = yield* sql<{
        session_id: string;
        ceiling_json: string;
      }>`SELECT session_id, ceiling_json FROM peer_legacy_authority WHERE thread_id = ${threadId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0]) return undefined;
      return {
        sessionId: rows[0].session_id,
        ceiling: yield* decodeStoredCeiling(rows[0].ceiling_json).pipe(
          Effect.mapError(unavailable),
        ),
      };
    });
  const activeSession = (sessionId: string) =>
    Effect.gen(function* () {
      const session = yield* authSessions
        .getById({ sessionId: AuthSessionId.make(sessionId) })
        .pipe(Effect.mapError(unavailable));
      const now = yield* DateTime.now;
      if (
        Option.isNone(session) ||
        session.value.revokedAt !== null ||
        DateTime.isLessThanOrEqualTo(session.value.expiresAt, now)
      )
        return yield* denied("The peer grant expired or was revoked.");
    });
  const grant = (sessionId: string) =>
    Effect.gen(function* () {
      yield* activeSession(sessionId);
      const rows = yield* sql<{
        grant_json: string;
      }>`SELECT grant_json FROM peer_authority_sessions WHERE session_id = ${sessionId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0]) return yield* denied("The link has not negotiated peer authority.");
      return yield* decodeGrant(rows[0].grant_json).pipe(Effect.mapError(unavailable));
    });
  const enroll: PeerAuthority["Service"]["enroll"] = (scope, peer) =>
    Effect.gen(function* () {
      if (scope.client?.linked !== true)
        return yield* denied("Only an authenticated peer can open a control channel.");
      const sessionId = scope.client.sessionId;
      yield* activeSession(sessionId);
      if (peer.environmentId === here)
        return yield* denied("A peer cannot bind this environment to itself.");
      const initial: PeerGrant = {
        sessionId: AuthSessionId.make(sessionId),
        resourceScope: "created-work",
        onward: false,
        preview: false,
        device: false,
      };
      yield* sql`INSERT INTO peer_authority_sessions (session_id, environment_id, generation, grant_json) VALUES (${sessionId}, ${peer.environmentId}, ${peer.generation}, ${json(initial)}) ON CONFLICT DO NOTHING`.pipe(
        Effect.mapError(unavailable),
      );
      const rows = yield* sql<{
        environment_id: string;
        generation: string;
      }>`SELECT environment_id, generation FROM peer_authority_sessions WHERE session_id = ${sessionId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (rows[0]?.environment_id !== peer.environmentId || rows[0]?.generation !== peer.generation)
        return yield* denied(
          "The authenticated link identity does not match its durable generation.",
        );
    });
  const ask = (sessionId: string, frame: Frame, pendingReceipt?: unknown) =>
    Effect.gen(function* () {
      yield* activeSession(sessionId);
      const responseSignal = yield* signalFor(`frame:${frame.id}`);
      return yield* Effect.gen(function* () {
        yield* sql`INSERT INTO peer_control_frames (id, session_id, frame_json)
        SELECT ${frame.id}, ${sessionId}, ${json(frame)}
        WHERE (SELECT COUNT(*) FROM peer_control_frames WHERE session_id = ${sessionId} AND answer_json IS NULL) < 32
        ON CONFLICT DO NOTHING`.pipe(Effect.mapError(unavailable));
        const stored = yield* sql<{
          frame_json: string;
        }>`SELECT frame_json FROM peer_control_frames WHERE id = ${frame.id} AND session_id = ${sessionId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (stored[0] === undefined) return yield* unavailable();
        if (stored[0].frame_json !== json(frame))
          return yield* denied("The control request identity was reused with changed input.");
        yield* wake(`session:${sessionId}`);
        const answer = () =>
          sql<{
            answer_json: string | null;
          }>`SELECT answer_json FROM peer_control_frames WHERE id = ${frame.id} AND session_id = ${sessionId}`.pipe(
            Effect.mapError(unavailable),
          );
        let rows = yield* answer();
        if (rows[0]?.answer_json == null) {
          yield* wait(
            `frame:${frame.id}`,
            responseSignal,
            pendingReceipt === undefined ? 25_000 : 1_000,
          );
          rows = yield* answer();
        }
        if (rows[0]?.answer_json == null)
          return pendingReceipt === undefined ? yield* unavailable() : pendingReceipt;
        yield* activeSession(sessionId);
        const result = yield* decodeAnswer(rows[0].answer_json).pipe(Effect.mapError(unavailable));
        if (!result.ok) return yield* denied(result.message);
        return result.value;
      }).pipe(
        Effect.ensuring(releaseSignal(`frame:${frame.id}`, responseSignal)),
        Effect.ensuring(
          frame.body.type === "validate" ||
            frame.body.type === "request-actor" ||
            frame.body.type === "request-ancestry"
            ? sql`DELETE FROM peer_control_frames WHERE id = ${frame.id} AND session_id = ${sessionId}`.pipe(
                Effect.orDie,
              )
            : Effect.void,
        ),
      );
    });
  const exchange: PeerAuthority["Service"]["exchange"] = (scope, input) =>
    Effect.gen(function* () {
      if (input.answers.length > 32) return yield* denied("Too many peer control answers.");
      yield* enroll(scope, input);
      if (scope.client?.linked !== true) return yield* denied("An authenticated peer is required.");
      const sessionId = scope.client.sessionId;
      for (const answer of input.answers) {
        yield* sql`UPDATE peer_control_frames SET answer_json = ${json(answer.result)} WHERE id = ${answer.id} AND session_id = ${sessionId} AND answer_json IS NULL`.pipe(
          Effect.mapError(unavailable),
        );
        yield* wake(`frame:${answer.id}`);
      }
      const pending = () =>
        sql<{
          frame_json: string;
          cursor: number;
        }>`SELECT rowid AS cursor, frame_json FROM peer_control_frames WHERE session_id = ${sessionId} AND answer_json IS NULL AND rowid > ${input.after ?? 0} ORDER BY rowid LIMIT 32`.pipe(
          Effect.mapError(unavailable),
        );
      const pendingSignal =
        input.wait === false ? undefined : yield* signalFor(`session:${sessionId}`);
      return yield* Effect.gen(function* () {
        let rows = yield* pending();
        if (rows.length === 0 && pendingSignal !== undefined) {
          yield* wait(`session:${sessionId}`, pendingSignal, 20_000);
          rows = yield* pending();
        }
        yield* activeSession(sessionId);
        return {
          frames: yield* Effect.forEach(rows, (row) =>
            decodeFrame(row.frame_json).pipe(Effect.mapError(unavailable)),
          ),
          cursor: rows.at(-1)?.cursor ?? input.after ?? 0,
        };
      }).pipe(
        Effect.ensuring(
          pendingSignal === undefined
            ? Effect.void
            : releaseSignal(`session:${sessionId}`, pendingSignal),
        ),
      );
    });
  const validateStopIntent = (control: StopControl, peer: EnvironmentId, generation: string) =>
    Effect.gen(function* () {
      const pair = control.pair;
      if (pair.parentEnvironmentId !== here || pair.childEnvironmentId !== peer)
        return yield* denied("The stop obligation belongs to another audience.");
      const rows = yield* sql<{
        record_id: string;
        pair_json: string;
      }>`SELECT record_id,pair_json FROM peer_return_pairs WHERE task_id=${pair.taskId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (rows[0]?.pair_json !== json(pair)) return yield* denied("Unknown stop obligation pair.");
      const launch = yield* read(rows[0]!.record_id);
      if (launch.peer.environmentId !== peer || launch.peer.generation !== generation)
        return yield* denied("The stop obligation belongs to another generation.");
      yield* validateReturn(launch);
      const parent = yield* threads
        .getThreadRecords(pair.parentThreadId, ["subagents"])
        .pipe(Effect.mapError(unavailable));
      const task = parent.subagents.find((t) => t.id === pair.taskId);
      if (
        parent.thread.deletedAt !== null ||
        parent.thread.archivedAt !== null ||
        task?.remoteChild?.threadId !== pair.childThreadId ||
        task.remoteChild.environmentId !== peer ||
        task.remoteChild.launch?.linkIdentity !== generation
      )
        return yield* denied("The stop relationship was retired.");
      const intents = yield* sql<{
        reason: string | null;
      }>`SELECT DISTINCT json_extract(payload_json,'$.remoteChild.stop.reason') AS reason FROM orchestration_events WHERE stream_id=${pair.parentThreadId} AND event_type='subagent.updated' AND json_extract(payload_json,'$.id')=${pair.taskId} AND json_extract(payload_json,'$.remoteChild.stop.requestId')= ${control.requestId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (intents.length === 0) return yield* denied("No durable stop obligation was accepted.");
      for (const intent of intents) {
        if (intent.reason !== (control.reason ?? null))
          return yield* denied("The stop request identity has conflicting semantics.");
      }
      return launch;
    });
  const currentCapabilities = (
    capabilities: PeerCeiling["capabilities"],
    projectId: OrchestrationV2ThreadShell["projectId"],
  ) =>
    Effect.gen(function* () {
      const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      if (Option.isNone(settings)) return capabilities;
      const current = yield* settings.value.getSettings.pipe(Effect.mapError(unavailable));
      const effective = resolveProjectSettings(current, projectId).settings;
      return capabilities.filter((name) =>
        name === "preview"
          ? effective.enableAgentBrowserAccess
          : name === "device"
            ? effective.enableAgentDeviceAccess
            : true,
      );
    });
  const validate = (
    record: Authority,
    stop?: StopValidation,
  ): Effect.Effect<PeerCeiling, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (record.context?.type === "task-stop") {
        const control = record.context;
        let ceiling = record.ceiling;
        for (const id of control.recordIds)
          ceiling = intersect(ceiling, yield* validate(yield* read(id), control));
        for (const target of control.targets) {
          const binding = yield* loadResource({ type: "thread", threadId: target.threadId });
          if (binding.type !== "thread" || binding.thread.archivedAt !== null)
            return yield* denied("The stop target was retired.");
          for (const id of control.recordIds) {
            const owner = yield* read(id);
            yield* checkResource(owner, yield* validate(owner, control), binding, control);
          }
          for (const id of target.recordIds) {
            const owner = yield* read(id);
            const current = yield* validate(owner, control);
            yield* checkResource(owner, current, binding, control);
            ceiling = intersect(ceiling, current);
          }
          if (exceededDispatchModeLimit(ceiling, binding.thread) !== undefined)
            return yield* denied("The stop exceeds a current destination mode.");
        }
        return ceiling;
      }
      if (record.context?.type === "run-inputs") {
        let ceiling = record.ceiling;
        for (const id of record.context.recordIds)
          ceiling = intersect(ceiling, yield* validate(yield* read(id), stop));
        return ceiling;
      }
      if (record.context !== undefined) {
        const contextThread = yield* threads
          .getThreadShell(record.context.threadId)
          .pipe(Effect.mapError(unavailable));
        if (contextThread === null || contextThread.deletedAt !== null)
          return yield* denied("The authority's destination context is no longer available.");
        let ceiling = record.ceiling;
        if (record.context.type === "legacy-thread") {
          const imported = yield* legacy(record.context.threadId);
          if (imported === undefined || imported.sessionId !== record.resourceSessionId)
            return yield* denied("The legacy context is no longer available.");
          yield* activeSession(imported.sessionId);
          ceiling = intersect(ceiling, imported.ceiling);
        }
        const owners =
          record.context.type === "owned-thread"
            ? [record.context.creationRecordId, record.context.runRecordId]
            : [record.context.runRecordId];
        // Keep the immutable owners and their live chains. Resource eligibility
        // belongs to the actual operation target, which may be a new joint child.
        for (const id of owners) {
          const owner = yield* read(id);
          const current = yield* validate(owner, stop);
          ceiling = intersect(ceiling, current);
        }
        return ceiling;
      }
      if (record.sessionId !== null) {
        const current = yield* grant(record.sessionId);
        const upstream = yield* ask(record.sessionId, {
          id: `validate:${record.id}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
          body: {
            type: "validate",
            envelope: record.envelope,
            ...(stop === undefined ||
            record.envelope.environmentId !== stop.pair.parentEnvironmentId ||
            here !== stop.pair.childEnvironmentId
              ? {}
              : {
                  stop: {
                    pair: stop.pair,
                    requestId: stop.requestId,
                    ...(stop.reason === undefined ? {} : { reason: stop.reason }),
                  },
                }),
          },
        });
        return intersect(
          record.ceiling,
          intersect(yield* decodeCeiling(upstream).pipe(Effect.mapError(unavailable)), {
            ...record.ceiling,
            resourceScope: current.resourceScope,
            onward: current.onward,
            capabilities: record.ceiling.capabilities.filter(
              (c) => (c !== "preview" && c !== "device") || current[c],
            ),
          }),
        );
      }
      const link = yield* links.get(record.peer.environmentId).pipe(Effect.mapError(unavailable));
      if (
        Option.isNone(link) ||
        link.value.launchIdentity !== record.peer.generation ||
        DateTime.toEpochMillis(link.value.expiresAt) <= (yield* Clock.currentTimeMillis)
      )
        return yield* denied("The originating link generation is no longer authorized.");
      if (record.localThreadId === null) {
        if (record.originClientId === null)
          return yield* denied("A forwarding action requires its originating actor.");
        yield* activeSession(record.originClientId);
        if (record.requestClientSessionId !== undefined)
          yield* requireClient(record.requestClientSessionId, record.readOnly);
        if (record.parentId === null) return record.ceiling;
        const parent = yield* validate(yield* read(record.parentId));
        if (!parent.onward)
          return yield* denied("The originating grant does not permit onward delegation.");
        return intersect(record.ceiling, parent);
      }
      const thread = yield* threads
        .getThreadShell(record.localThreadId)
        .pipe(Effect.mapError(unavailable));
      if (thread === null || thread.deletedAt !== null)
        return yield* denied("The originating thread is no longer available.");
      const stoppedOrigin =
        stop !== undefined &&
        record.localThreadId === stop.pair.parentThreadId &&
        record.peer.environmentId === stop.pair.childEnvironmentId &&
        (record.pair === undefined || record.pair.taskId === stop.pair.taskId);
      if (record.pair !== undefined) {
        const parent = yield* threads
          .getThreadRecords(record.pair.parentThreadId, ["subagents"])
          .pipe(Effect.mapError(unavailable));
        const task = parent.subagents.find((task) => task.id === record.pair?.taskId);
        if (
          task === undefined ||
          (!stoppedOrigin && (task.status === "cancelled" || task.status === "interrupted"))
        )
          return yield* denied("The originating delegated job was stopped or retired.");
      } else if (
        !stoppedOrigin &&
        !record.readOnly &&
        (record.originRunId === null ||
          thread.activeRunId !== record.originRunId ||
          thread.archivedAt !== null)
      ) {
        return yield* denied("The originating run is no longer active.");
      }
      const current = {
        ...record.ceiling,
        capabilities: yield* currentCapabilities(record.ceiling.capabilities, thread.projectId),
        runtimeMode: thread.runtimeMode,
        interactionMode: thread.interactionMode,
      };
      const parent = record.parentId === null ? undefined : yield* read(record.parentId);
      const ceiling =
        parent === undefined ? record.ceiling : yield* validate(parent, stop?.upstream ?? stop);
      if (parent !== undefined) {
        if (!ceiling.onward)
          return yield* denied("The originating grant does not permit onward delegation.");
        // A forwarded action still depends on permission for its actual source
        // thread, even when its owners were inherited from an older context.
        yield* checkResource(parent, ceiling, { type: "thread", thread }, stop?.upstream ?? stop);
      }
      return intersect(current, intersect(record.ceiling, ceiling));
    });
  const resourceSession = (record: Authority) => record.resourceSessionId ?? record.sessionId;
  // Creation membership is a union; an acting authority is a conjunction.
  // Only permanent birth bindings use membership, never an existing target's run.
  const hasResourceSession = (
    record: Authority,
    sessionId: string,
  ): Effect.Effect<boolean, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const context = record.context;
      if (context === undefined) return resourceSession(record) === sessionId;
      if (context.type === "legacy-thread" && record.resourceSessionId === sessionId) return true;
      const ids =
        context.type === "run-inputs" || context.type === "task-stop"
          ? context.recordIds
          : context.type === "owned-thread"
            ? [context.creationRecordId, context.runRecordId]
            : [context.runRecordId];
      for (const id of ids) if (yield* hasResourceSession(yield* read(id), sessionId)) return true;
      return false;
    });
  const checkCreated = (
    sessionId: string,
    target: ResourceBinding,
  ): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (target.type === "schedule" && target.linkOrigin?.sessionId === sessionId) return;
      if (target.type === "thread") {
        if (target.thread.linkOrigin?.sessionId === sessionId) return;
        const created = yield* permanentForThread(target.thread.id);
        if (created !== undefined && (yield* hasResourceSession(created, sessionId))) return;
      }
      return yield* denied("The destination is outside the link's current resource scope.");
    });
  const checkResource = (
    record: Authority,
    ceiling: PeerCeiling,
    target: ResourceBinding,
    stop?: StopValidation,
  ): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const context = record.context;
      if (context?.type === "task-stop") {
        if (target.type !== "thread")
          return yield* denied("A stop obligation only controls its exact threads.");
        const entry = context.targets.find((item) => item.threadId === target.thread.id);
        if (entry === undefined)
          return yield* denied("The effect is outside the recorded stop target set.");
        for (const id of [...context.recordIds, ...entry.recordIds]) {
          const owner = yield* read(id);
          yield* checkResource(owner, yield* validate(owner, context), target, context);
        }
        return;
      }
      if (context !== undefined) {
        // Preserve each owner's predicate on the actual target, including the
        // legacy creation owner. A copied session or scope enum is not this proof.
        if (context.type === "legacy-thread") {
          const imported = yield* legacy(context.threadId);
          if (imported === undefined || imported.sessionId !== record.resourceSessionId)
            return yield* denied("The legacy context is no longer available.");
          yield* activeSession(imported.sessionId);
          yield* checkCreated(imported.sessionId, target);
        }
        const ids =
          context.type === "run-inputs"
            ? context.recordIds
            : context.type === "owned-thread"
              ? [context.creationRecordId, context.runRecordId]
              : [context.runRecordId];
        for (const id of ids) {
          const owner = yield* read(id);
          yield* checkResource(owner, yield* validate(owner, stop), target, stop);
        }
        return;
      }
      const sessionId = resourceSession(record);
      if (sessionId !== null && ceiling.resourceScope === "created-work")
        yield* checkCreated(sessionId, target);
    });
  const loadResource = (
    target: ResourceTarget,
  ): Effect.Effect<ResourceBinding, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (target.type === "environment") return target;
      if (target.type === "thread") {
        const thread = yield* threads
          .getThreadShell(target.threadId)
          .pipe(Effect.mapError(unavailable));
        if (thread === null || thread.deletedAt !== null)
          return yield* denied("The destination thread is no longer available.");
        return { type: "thread", thread };
      }
      const rows = yield* sql<{
        link_origin_json: string | null;
      }>`SELECT link_origin_json FROM scheduled_tasks WHERE task_id = ${target.taskId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (rows[0] === undefined)
        return yield* denied("The destination schedule is no longer available.");
      const linkOrigin =
        rows[0].link_origin_json === null
          ? undefined
          : yield* decodeLinkOrigin(rows[0].link_origin_json).pipe(Effect.mapError(unavailable));
      return { type: "schedule", linkOrigin };
    });
  const checkThread: PeerAuthority["Service"]["checkThread"] = (threadId) =>
    Effect.gen(function* () {
      const record = yield* forThread(threadId);
      if (record !== undefined) {
        const ceiling = yield* validate(record);
        const target = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
        if (target === null || target.deletedAt !== null)
          return yield* denied("The destination thread is no longer available.");
        yield* checkResource(record, ceiling, { type: "thread", thread: target });
        return { ...record, ceiling };
      }
      const migrated = yield* legacy(threadId);
      if (migrated !== undefined) yield* activeSession(migrated.sessionId);
      return undefined;
    });
  const prepare: PeerAuthority["Service"]["prepare"] = (
    scope,
    peer,
    generation,
    tool,
    args,
    limits,
    pair,
    readOnly = false,
    stableRequests = false,
  ) =>
    Effect.gen(function* () {
      if (!scope.thread && !scope.client)
        return yield* denied("Peer forwarding requires an authenticated authority owner.");
      const parent =
        scope.client?.linked === true
          ? scope.authority
          : scope.thread === undefined
            ? undefined
            : yield* checkThread(scope.thread.threadId);
      const ceiling: PeerCeiling = {
        ...limits,
        resourceScope: parent?.ceiling.resourceScope ?? "environment-resources",
        onward: parent?.ceiling.onward ?? true,
        capabilities: [...scope.capabilities],
      };
      if (parent && !parent.ceiling.onward)
        return yield* denied("This child's grant does not permit onward delegation.");
      const route =
        scope.client !== undefined && parent ? [...parent.envelope.route, here] : [here];
      if (route.includes(peer))
        return yield* denied("Peer transport route repeats an environment.");
      const origin =
        scope.thread === undefined
          ? null
          : yield* threads.getThreadShell(scope.thread.threadId).pipe(Effect.mapError(unavailable));
      const digest = stableRequests ? requestDigest({ tool, args }) : yield* hash({ tool, args });
      const id = yield* hash({
        actor:
          scope.thread === undefined
            ? { client: scope.client?.sessionId }
            : { thread: scope.thread.threadId },
        peer,
        generation,
        tool,
        args,
        pair,
        action: pair === undefined ? yield* crypto.randomUUIDv4().pipe(Effect.orDie) : undefined,
      });
      const envelope: PeerAuthorityEnvelope = {
        version: 1,
        environmentId: here,
        generation,
        recordId: id,
        actionId: id,
        digest,
        route,
        descendant: parent !== undefined || scope.client?.linked === true,
        ...(stableRequests
          ? {
              requestOwner: {
                namespace: requestNamespace(scope),
                generation: `${requestGeneration(scope)}:${generation}`,
              },
            }
          : {}),
        ...(pair ? { pair } : {}),
      };
      const clientSession = yield* PendingClientSession;
      const record: Authority = {
        id,
        requestActorThreadId:
          scope.client?.linked === true ? null : (scope.thread?.threadId ?? null),
        ...(clientSession === undefined
          ? {}
          : { requestClientSessionId: AuthSessionId.make(clientSession.sessionId) }),
        peer: { environmentId: peer, generation },
        sessionId: null,
        localThreadId: scope.thread?.threadId ?? null,
        originClientId:
          scope.thread === undefined && scope.client !== undefined
            ? AuthSessionId.make(scope.client.sessionId)
            : null,
        parentId: parent?.id ?? null,
        ceiling: parent ? intersect(ceiling, parent.ceiling) : ceiling,
        envelope,
        tool,
        readOnly,
        originRunId: origin?.activeRunId ?? null,
        ...(pair ? { pair } : {}),
      };
      yield* save(record);
      yield* validate(record);
      return envelope;
    });
  const accept: PeerAuthority["Service"]["accept"] = (scope, envelope) =>
    Effect.gen(function* () {
      yield* enroll(scope, envelope);
      if (
        envelope.route.length === 0 ||
        envelope.route.length > 16 ||
        new Set(envelope.route).size !== envelope.route.length ||
        envelope.route.includes(here) ||
        envelope.route.at(-1) !== envelope.environmentId
      )
        return yield* denied("Invalid peer transport route.");
      if (scope.client?.linked !== true) return yield* denied("An authenticated peer is required.");
      const sessionId = scope.client.sessionId;
      const remote = yield* ask(sessionId, {
        id: `validate:${envelope.actionId}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
        body: { type: "validate", envelope },
      });
      const ceiling = yield* decodeCeiling(remote).pipe(Effect.mapError(unavailable));
      const current = yield* grant(sessionId);
      const modes = Invocation.clientModeCeiling(scope.client);
      const record: Authority = {
        id: `in:${sessionId}:${envelope.recordId}`,
        peer: { environmentId: envelope.environmentId, generation: envelope.generation },
        sessionId: AuthSessionId.make(sessionId),
        localThreadId: null,
        originClientId: null,
        parentId: null,
        ceiling: intersect(ceiling, {
          ...modes,
          resourceScope: current.resourceScope,
          onward: current.onward,
          capabilities: ceiling.capabilities.filter(
            (c) => (c !== "preview" && c !== "device") || current[c],
          ),
        }),
        envelope,
        tool: "",
        readOnly: false,
        originRunId: null,
      };
      yield* save(record);
      const frozen = yield* read(record.id);
      return { ...frozen, ceiling: intersect(frozen.ceiling, record.ceiling) };
    });
  const checkPayload: PeerAuthority["Service"]["checkPayload"] = (authority, tool, args) =>
    Effect.gen(function* () {
      const digest =
        authority.envelope.requestOwner === undefined
          ? yield* hash({ tool, args })
          : requestDigest({ tool, args });
      if (digest !== authority.envelope.digest)
        return yield* denied("The peer action does not match its authenticated authority.");
    });
  const bind: PeerAuthority["Service"]["bind"] = (authority, threadId, registerReturn) =>
    Effect.gen(function* () {
      yield* sql`INSERT INTO peer_thread_authority (thread_id, record_id) VALUES (${threadId}, ${authority.id}) ON CONFLICT DO NOTHING`.pipe(
        Effect.mapError(unavailable),
      );
      const parent = authority.envelope.pair;
      if (registerReturn === true && parent && authority.sessionId !== null) {
        const pair: PeerReturnPair = {
          parentEnvironmentId: authority.peer.environmentId,
          parentThreadId: parent.parentThreadId,
          taskId: parent.taskId,
          childEnvironmentId: here,
          childThreadId: threadId,
          childRunId: null,
        };
        yield* sql`INSERT INTO peer_return_pairs (task_id, record_id, pair_json) VALUES (${parent.taskId}, ${authority.id}, ${json(pair)}) ON CONFLICT DO NOTHING`.pipe(
          Effect.mapError(unavailable),
        );
        yield* ask(authority.sessionId, {
          id: `pair:${authority.id}:${threadId}`,
          body: { type: "pair", envelope: authority.envelope, pair },
        });
      }
    });
  const registerPair: PeerAuthority["Service"]["registerPair"] = (envelope, pair) =>
    Effect.gen(function* () {
      const record = yield* read(envelope.recordId);
      if (
        record.sessionId !== null ||
        record.pair?.taskId !== pair.taskId ||
        record.pair.parentThreadId !== pair.parentThreadId ||
        pair.parentEnvironmentId !== here ||
        pair.childEnvironmentId !== record.peer.environmentId
      )
        return yield* denied("The return pair does not match its parent-local task.");
      yield* sql`INSERT INTO peer_return_pairs (task_id, record_id, pair_json) VALUES (${pair.taskId}, ${record.id}, ${json({ ...pair, childRunId: null })}) ON CONFLICT DO NOTHING`.pipe(
        Effect.mapError(unavailable),
      );
      const stored = yield* sql<{
        record_id: string;
        pair_json: string;
      }>`SELECT record_id, pair_json FROM peer_return_pairs WHERE task_id = ${pair.taskId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (
        stored[0]?.record_id !== record.id ||
        stored[0]?.pair_json !== json({ ...pair, childRunId: null })
      )
        return yield* denied("The parent task already belongs to another return pair.");
    });
  const validateReturn = (record: Authority): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (record.sessionId !== null) {
        yield* grant(record.sessionId);
        return;
      }
      const link = yield* links.get(record.peer.environmentId).pipe(Effect.mapError(unavailable));
      if (
        Option.isNone(link) ||
        link.value.launchIdentity !== record.peer.generation ||
        DateTime.toEpochMillis(link.value.expiresAt) <= (yield* Clock.currentTimeMillis)
      )
        return yield* denied("The return link generation was revoked or expired.");
      if (record.parentId !== null) yield* validateReturn(yield* read(record.parentId));
    });
  const decodePendingActor = Schema.decodeUnknownEffect(PendingActor);
  const requireClient = (sessionId: string, readOnly = false) =>
    Effect.gen(function* () {
      yield* activeSession(sessionId);
      const session = yield* authSessions
        .getById({ sessionId: AuthSessionId.make(sessionId) })
        .pipe(Effect.mapError(unavailable));
      if (
        Option.isNone(session) ||
        !session.value.scopes.includes(readOnly ? "orchestration:read" : "orchestration:operate")
      )
        return yield* denied("The source client cannot answer requests.");
    });
  const actorForRecord = (record: Authority): Effect.Effect<PendingActor, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      yield* validate(record);
      if (record.sessionId !== null) {
        return yield* decodePendingActor(
          yield* ask(record.sessionId, {
            id: `request-actor:${record.id}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
            body: { type: "request-actor", envelope: record.envelope },
          }),
        ).pipe(Effect.mapError(unavailable));
      }
      if (record.requestClientSessionId !== undefined) {
        yield* requireClient(record.requestClientSessionId, record.readOnly);
        return { type: "client", environmentId: here, sessionId: record.requestClientSessionId };
      }
      if (record.requestActorThreadId != null)
        return { type: "thread", environmentId: here, threadId: record.requestActorThreadId };
      if (record.requestActorThreadId === null && record.parentId !== null)
        return yield* actorForRecord(yield* read(record.parentId));
      return yield* denied("The request has no verified originating thread or client.");
    });
  // Walk real local edges, then ask the owner of the exact authenticated pair.
  // Environments may repeat in ancestry; the finite hop budget is not a transport route.
  const ancestor = (
    actor: PendingActor,
    target: ThreadId,
    remaining = 16,
  ): Effect.Effect<boolean, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const seen = new Set<ThreadId>();
      let id = target;
      while (!seen.has(id) && seen.size < 256) {
        seen.add(id);
        const shell = yield* threads.getThreadShell(id).pipe(Effect.mapError(unavailable));
        if (shell === null || shell.deletedAt !== null || shell.archivedAt !== null) return false;
        if (actor.type === "thread" && actor.environmentId === here && actor.threadId === id)
          return true;
        if (
          shell.lineage.relationshipToParent === "subagent" &&
          shell.lineage.parentThreadId !== null
        ) {
          id = shell.lineage.parentThreadId;
          continue;
        }
        if (remaining <= 1) return false;
        const rows = yield* sql<{
          record_id: string;
          pair_json: string;
        }>`SELECT record_id,pair_json FROM peer_return_pairs WHERE json_extract(pair_json,'$.childEnvironmentId')=${here} AND json_extract(pair_json,'$.childThreadId')=${id}`.pipe(
          Effect.mapError(unavailable),
        );
        if (rows.length !== 1) return false;
        const pair = yield* decodePair(rows[0]!.pair_json).pipe(Effect.mapError(unavailable));
        const owner = yield* read(rows[0]!.record_id);
        if (owner.sessionId === null || owner.peer.environmentId !== pair.parentEnvironmentId)
          return false;
        yield* validateReturn(owner);
        const result = yield* ask(owner.sessionId, {
          id: `request-ancestry:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
          body: { type: "request-ancestry", pair, actor, remaining: remaining - 1 },
        });
        return result === true;
      }
      return false;
    });
  const pendingActor: PeerAuthority["Service"]["pendingActor"] = (scope, target) =>
    Effect.gen(function* () {
      const clientSession = yield* PendingClientSession;
      if (clientSession !== undefined) {
        yield* requireClient(clientSession.sessionId, clientSession.readOnly);
        return "client";
      }
      const actor: PendingActor =
        scope.client?.linked === true && scope.authority !== undefined
          ? yield* actorForRecord(scope.authority)
          : scope.thread !== undefined
            ? { type: "thread", environmentId: here, threadId: scope.thread.threadId }
            : yield* denied(
                "Only a verified ancestor or authenticated client may answer a request.",
              );
      if (actor.type === "client") return "client";
      if (actor.environmentId === here && actor.threadId === target) return undefined;
      return (yield* ancestor(actor, target)) ? "ancestor" : undefined;
    });
  const process: PeerAuthority["Service"]["process"] = (peer, generation, frame) =>
    Effect.gen(function* () {
      if (frame.body.type === "request-actor") {
        const record = yield* read(frame.body.envelope.recordId);
        if (
          record.sessionId !== null ||
          record.peer.environmentId !== peer ||
          record.peer.generation !== generation ||
          json(record.envelope) !== json(frame.body.envelope)
        )
          return yield* denied("The request actor belongs to another action or link.");
        return yield* actorForRecord(record);
      }
      if (frame.body.type === "request-ancestry") {
        const { pair, actor, remaining } = frame.body;
        const rows = yield* sql<{
          record_id: string;
          pair_json: string;
        }>`SELECT record_id,pair_json FROM peer_return_pairs WHERE task_id=${pair.taskId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (!rows[0] || rows[0].pair_json !== json(pair))
          return yield* denied("Unknown request ancestry pair.");
        const record = yield* read(rows[0].record_id);
        if (
          record.sessionId !== null ||
          record.peer.environmentId !== peer ||
          record.peer.generation !== generation ||
          pair.parentEnvironmentId !== here ||
          pair.childEnvironmentId !== peer
        )
          return yield* denied("The request ancestry belongs to another generation.");
        yield* validateReturn(record);
        const parent = yield* threads
          .getThreadRecords(pair.parentThreadId, ["subagents"])
          .pipe(Effect.mapError(unavailable));
        const task = parent.subagents.find((task) => task.id === pair.taskId);
        if (
          !task ||
          task.completionDelivery?.state === "disposed" ||
          task.remoteChild?.threadId !== pair.childThreadId ||
          ["cancelled", "interrupted"].includes(task.status)
        )
          return false;
        return yield* ancestor(actor, pair.parentThreadId, remaining);
      }
      if (frame.body.type === "pair") {
        const record = yield* read(frame.body.envelope.recordId);
        if (record.peer.environmentId !== peer || record.peer.generation !== generation)
          return yield* denied("Pair registration belongs to another link.");
        if (json(record.envelope) !== json(frame.body.envelope))
          return yield* denied("Pair registration belongs to another action.");
        yield* validate(record);
        yield* registerPair(frame.body.envelope, frame.body.pair);
        return { registered: true };
      }
      if (frame.body.type === "validate") {
        const envelope = frame.body.envelope;
        const record = yield* read(envelope.recordId);
        if (
          record.sessionId !== null ||
          record.peer.environmentId !== peer ||
          record.peer.generation !== generation ||
          record.envelope.digest !== envelope.digest ||
          record.envelope.actionId !== envelope.actionId ||
          json(record.envelope) !== json(envelope)
        )
          return yield* denied("The validation frame belongs to another action or link.");
        if (frame.body.stop !== undefined) {
          const control = frame.body.stop;
          yield* validateStopIntent(control, peer, generation);
          if (record.localThreadId !== control.pair.parentThreadId)
            return yield* denied("The stop cannot borrow another source actor.");
          const inherited = yield* sql<{
            record_id: string;
          }>`SELECT record_id FROM peer_task_stop_authority WHERE task_id=${control.pair.taskId} AND request_id=${control.requestId}`.pipe(
            Effect.mapError(unavailable),
          );
          const inheritedRecord =
            inherited[0] === undefined ? undefined : yield* read(inherited[0].record_id);
          const upstream =
            inheritedRecord?.context?.type === "task-stop" ? inheritedRecord.context : undefined;
          const requesterCeiling =
            inheritedRecord === undefined ? undefined : yield* validate(inheritedRecord);
          let ceiling = yield* validate(record, {
            ...control,
            ...(upstream === undefined ? {} : { upstream }),
          });
          const parent = yield* threads
            .getThreadShell(control.pair.parentThreadId)
            .pipe(Effect.mapError(unavailable));
          if (parent === null) return yield* denied("The stop source disappeared.");
          if (inheritedRecord !== undefined && requesterCeiling !== undefined) {
            yield* checkResource(inheritedRecord, requesterCeiling, {
              type: "thread",
              thread: parent,
            });
            ceiling = intersect(ceiling, requesterCeiling);
          }

          // Stop has ended the active run. Keep its actual accepted input owners,
          // plus creation ownership, rather than falling back to raw thread modes.
          const latest = yield* sql<{
            record_id: string;
          }>`SELECT owner.record_id FROM orchestration_v2_projection_runs AS run JOIN peer_run_authority AS owner ON owner.run_id=run.run_id WHERE run.run_id=(SELECT latest.run_id FROM orchestration_v2_projection_runs AS latest WHERE latest.thread_id=${control.pair.parentThreadId} ORDER BY json_extract(latest.payload_json,'$.ordinal') DESC LIMIT 1)`.pipe(
            Effect.mapError(unavailable),
          );
          const current = yield* forThread(control.pair.parentThreadId);
          for (const id of new Set([
            ...(current === undefined ? [] : [current.id]),
            ...latest.map((row) => row.record_id),
          ])) {
            const owner = yield* read(id);
            const live = yield* validate(owner, upstream);
            yield* checkResource(owner, live, { type: "thread", thread: parent }, upstream);
            ceiling = intersect(ceiling, live);
          }
          return ceiling;
        }
        return yield* validate(record);
      }
      const input = frame.body;
      const rows = yield* sql<{
        record_id: string;
        pair_json: string;
      }>`SELECT record_id, pair_json FROM peer_return_pairs WHERE task_id = ${input.pair.taskId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0] || rows[0].pair_json !== json(input.pair))
        return yield* denied("Unknown parent return pair.");
      const record = yield* read(rows[0].record_id);
      if (record.peer.environmentId !== peer || record.peer.generation !== generation)
        return yield* denied("Return notice belongs to another link generation.");
      yield* validateReturn(record);
      const parent = yield* threads
        .getThreadRecords(input.pair.parentThreadId, ["subagents"])
        .pipe(Effect.mapError(unavailable));
      const task = parent.subagents.find((task) => task.id === input.pair.taskId);
      if (
        !task ||
        task.status === "cancelled" ||
        task.status === "interrupted" ||
        task.remoteChild?.environmentId !== peer ||
        (task.remoteChild.threadId !== undefined &&
          task.remoteChild.threadId !== input.pair.childThreadId) ||
        task.remoteChild.launchStarted !== true
      )
        return yield* denied("The delegated return relationship was retired.");
      const id = yield* hash({ pair: input.pair, requestId: input.requestId });
      const delivery = yield* Effect.serviceOption(PeerNoticeDelivery.PeerNoticeDelivery);
      if (Option.isNone(delivery)) return yield* unavailable();
      const delivered = yield* delivery.value.deliver(
        input.pair,
        input.senderRunId,
        input.text,
        input.mode,
        id,
      );
      return delivered;
    }).pipe(
      Effect.map((value) => ({ id: frame.id, result: { ok: true as const, value } })),
      Effect.catch((error) =>
        Effect.succeed({ id: frame.id, result: { ok: false as const, message: error.message } }),
      ),
    );
  const returnNotice: PeerAuthority["Service"]["returnNotice"] = (scope, environmentId, input) =>
    Effect.gen(function* () {
      if (!scope.thread) return undefined;
      const record = yield* forThread(scope.thread.threadId);
      if (!record || record.sessionId === null || record.peer.environmentId !== environmentId)
        return undefined;
      if (input.mode !== undefined && input.mode !== "auto" && input.mode !== "queue")
        return yield* denied("A child may only queue a notice to its parent.");
      // The destination stores only a verified pair returned by the source owner.
      // Returns retain the actual creation pair, not a newer contributor's
      // command identity. Its exact child thread is still checked below.
      let pairOwner = record;
      while (pairOwner.context?.type === "owned-thread")
        pairOwner = yield* read(pairOwner.context.creationRecordId);
      if (pairOwner.context?.type === "legacy-thread" || pairOwner.context?.type === "run-inputs")
        return undefined;
      const rows = yield* sql<{
        pair_json: string;
      }>`SELECT pair_json FROM peer_return_pairs WHERE record_id = ${pairOwner.id}`.pipe(
        Effect.mapError(unavailable),
      );
      const pair = rows[0]
        ? yield* decodePair(rows[0].pair_json).pipe(Effect.mapError(unavailable))
        : undefined;
      if (
        !pair ||
        pair.parentThreadId !== input.threadId ||
        pair.childThreadId !== scope.thread.threadId
      )
        return undefined;
      const sender = yield* threads
        .getThreadShell(scope.thread.threadId)
        .pipe(Effect.mapError(unavailable));
      if (sender?.activeRunId == null)
        return yield* denied("The returning child has no active run.");
      const senderRunId = sender.activeRunId;
      const requestId = input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie));
      const noticeIdentity = yield* hash({ pair, requestId });
      const id = `return:${noticeIdentity}`;
      const pending = {
        threadId: pair.parentThreadId,
        messageId: MessageId.make(`peer-return:${noticeIdentity}`),
        taskId: pair.taskId,
        runId: null,
        status: "queued",
        delivery: "pending",
      };
      const value = yield* ask(
        record.sessionId,
        {
          id,
          body: {
            type: "return",
            pair,
            senderRunId,
            requestId,
            text: input.text,
            mode: input.mode ?? "auto",
          },
        },
        pending,
      );
      return yield* decodeReceipt(value).pipe(Effect.mapError(unavailable));
    });
  const verifyTask = (
    scope: Invocation.McpInvocationScope,
    input: PeerTaskLifecycleRequest,
    receiptOnly = false,
  ): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      yield* enroll(scope, input);
      if (scope.client?.linked !== true) return yield* denied("An authenticated peer is required.");
      const rows = yield* sql<{
        record_id: string;
        pair_json: string;
      }>`SELECT record_id, pair_json FROM peer_return_pairs WHERE task_id = ${input.pair.taskId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0] || rows[0].pair_json !== json(input.pair))
        return yield* denied("Unknown delegated lifecycle pair.");
      const record = yield* read(rows[0].record_id);
      if (
        record.sessionId !== scope.client.sessionId ||
        record.peer.environmentId !== input.environmentId ||
        record.peer.generation !== input.generation ||
        input.pair.parentEnvironmentId !== input.environmentId ||
        input.pair.childEnvironmentId !== here
      )
        return yield* denied("The lifecycle pair belongs to another grant generation.");
      yield* validateReturn(record);
      if (receiptOnly) {
        if (input.operation.type !== "stop")
          return yield* denied("Only an existing stop receipt can be reconciled here.");
        return;
      }
      const child = yield* threads
        .getThreadShell(input.pair.childThreadId)
        .pipe(Effect.mapError(unavailable));
      if (child === null || child.deletedAt !== null || child.archivedAt !== null)
        return yield* denied("The delegated lifecycle pair was retired.");
      const current = yield* grant(record.sessionId);
      if (!record.ceiling.capabilities.includes("orchestration"))
        return yield* denied("This pair does not permit lifecycle observation.");
      yield* checkResource(
        record,
        {
          ...record.ceiling,
          resourceScope:
            record.ceiling.resourceScope === "created-work"
              ? "created-work"
              : current.resourceScope,
        },
        { type: "thread", thread: child },
      );
      if (input.operation.type === "view" && input.operation.resource !== undefined) {
        if (input.operation.resource.environmentId !== here)
          return yield* denied("The lifecycle resource belongs to another destination.");
        yield* checkResource(
          record,
          {
            ...record.ceiling,
            resourceScope:
              record.ceiling.resourceScope === "created-work"
                ? "created-work"
                : current.resourceScope,
          },
          yield* loadResource({ type: "thread", threadId: input.operation.resource.threadId }),
        );
      }
    });
  const checkEffect: PeerAuthority["Service"]["checkEffect"] = (effectId, threadId) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        record_id: string;
      }>`SELECT record_id FROM peer_effect_authority WHERE effect_id = ${effectId}`.pipe(
        Effect.mapError(unavailable),
      );
      const created = yield* permanentForThread(threadId);
      const record = rows[0] === undefined ? created : yield* read(rows[0].record_id);
      const migrated = created === undefined ? yield* legacy(threadId) : undefined;
      if (record?.context?.type === "task-stop") {
        const effects = yield* sql<{
          effect_type: string;
        }>`SELECT effect_type FROM orchestration_v2_effect_outbox WHERE effect_id=${effectId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (
          !effects[0] ||
          !["provider-turn.interrupt", "provider-session.detach", "delegated-tasks.stop"].includes(
            effects[0].effect_type,
          )
        )
          return yield* denied("A stop obligation cannot authorize general work.");
      }
      if (record === undefined && migrated === undefined) return;
      if (migrated !== undefined) yield* activeSession(migrated.sessionId);
      const ceiling =
        record !== undefined
          ? yield* validate(record)
          : migrated !== undefined
            ? migrated.ceiling
            : yield* unavailable();
      const target = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
      if (target === null || target.deletedAt !== null)
        return yield* denied("The destination thread is no longer available.");
      if (record !== undefined)
        yield* checkResource(record, ceiling, { type: "thread", thread: target });
      let effective = migrated === undefined ? ceiling : intersect(ceiling, migrated.ceiling);
      if (created !== undefined && created.id !== record?.id) {
        const creationCeiling = yield* validate(
          created,
          record?.context?.type === "task-stop" ? record.context : undefined,
        );
        yield* checkResource(
          created,
          creationCeiling,
          { type: "thread", thread: target },
          record?.context?.type === "task-stop" ? record.context : undefined,
        );
        effective = intersect(effective, creationCeiling);
      }
      if (exceededDispatchModeLimit(effective, target) !== undefined)
        return yield* denied(
          "The destination effect exceeds the current originating authority ceiling.",
        );
    });
  const checkBinding = (
    scope: Invocation.McpInvocationScope,
    bindingEffect: Effect.Effect<ResourceBinding, OrchestratorMcpFailure>,
  ) =>
    Effect.gen(function* () {
      const record =
        scope.client === undefined && scope.thread !== undefined
          ? yield* checkThread(scope.thread.threadId)
          : scope.authority === undefined
            ? undefined
            : yield* read(scope.authority.id);
      const migrated =
        record === undefined && scope.thread !== undefined
          ? yield* legacy(scope.thread.threadId)
          : undefined;
      const sessionId =
        migrated?.sessionId ?? (scope.client?.linked === true ? scope.client.sessionId : undefined);
      if (record === undefined && sessionId === undefined)
        return scope.client === undefined ? undefined : Invocation.clientModeCeiling(scope.client);
      const binding = yield* bindingEffect;
      const ceiling = record === undefined ? undefined : yield* validate(record);
      if (record === undefined) {
        if (sessionId === undefined) return yield* unavailable();
        yield* activeSession(sessionId);
      }
      const modes =
        scope.client === undefined && ceiling !== undefined
          ? ceiling
          : Invocation.intersectModes(Invocation.clientModeCeiling(scope.client), ceiling);
      const directFull =
        scope.thread === undefined &&
        scope.client?.linked === true &&
        record?.envelope.descendant !== true &&
        modes.runtimeMode === "full-access" &&
        modes.interactionMode === "default";
      // Preserve the legacy direct exception for environment operations and
      // unstamped schedules. It never covers descendants or other-link threads.
      if (
        directFull &&
        (binding.type === "environment" ||
          (binding.type === "schedule" && binding.linkOrigin === undefined))
      )
        return modes;
      if (record !== undefined && ceiling !== undefined)
        yield* checkResource(record, ceiling, binding);
      else if (sessionId !== undefined) yield* checkCreated(sessionId, binding);
      return modes;
    });
  const recheckInvocation: PeerAuthority["Service"]["recheckInvocation"] = (
    scope,
    capability,
    threadId,
  ) =>
    Effect.gen(function* () {
      // The incoming action remains the owner even when its selected destination
      // thread has a different creation/run authority.
      const record =
        scope.authority !== undefined
          ? yield* read(scope.authority.id)
          : scope.client?.linked === true
            ? yield* denied("The peer has no verified invocation authority.")
            : yield* checkThread(scope.thread.threadId);
      const target = yield* loadResource({ type: "thread", threadId });
      if (target.type !== "thread" || target.thread.archivedAt !== null)
        return yield* denied("The destination context is no longer available.");
      let ceiling = record === undefined ? undefined : yield* validate(record);
      if (record !== undefined && ceiling !== undefined) {
        if (scope.authority !== undefined) ceiling = intersect(ceiling, scope.authority.ceiling);
        yield* checkResource(record, ceiling, target);
      }
      let modes: Invocation.ClientModes | undefined = ceiling ?? scope.legacyPeerCeiling;
      if (scope.client === undefined) {
        const caller = yield* threads
          .getThreadShell(scope.thread.threadId)
          .pipe(Effect.mapError(unavailable));
        if (
          caller === null ||
          caller.deletedAt !== null ||
          caller.archivedAt !== null ||
          caller.activeRunId === null ||
          caller.providerInstanceId !== scope.thread.providerInstanceId
        )
          return yield* denied("The original caller no longer owns a live run.");
        modes = Invocation.intersectModes(caller, modes);
      } else modes = Invocation.intersectModes(Invocation.clientModeCeiling(scope.client), modes);
      if (exceededDispatchModeLimit(modes, target.thread) !== undefined)
        return yield* denied(
          "The device or browser action exceeds current originating or destination modes.",
        );
      const capabilities = new Set(
        (yield* currentCapabilities([...scope.capabilities], target.thread.projectId)).filter(
          (name) => ceiling === undefined || ceiling.capabilities.includes(name),
        ),
      );
      if (!capabilities.has(capability))
        return yield* denied("The original invocation no longer has the required capability.");
      return {
        ...scope,
        capabilities,
        ...(record === undefined || ceiling === undefined
          ? {}
          : { authority: { ...record, ceiling } }),
      };
    });
  return PeerAuthority.of({
    recheckInvocation,
    pendingWithinTask: (resource, target) => ancestor({ type: "thread", ...resource }, target),
    checkRecord: (id) => Effect.flatMap(read(id), validate).pipe(Effect.asVoid),
    authorizeTaskStop: (scope, input) =>
      Effect.gen(function* () {
        yield* verifyTask(scope, input);
        const rows = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_return_pairs WHERE task_id=${input.pair.taskId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (!rows[0]) return yield* denied("Unknown stop pair.");
        const original = yield* read(rows[0].record_id);
        const control = {
          pair: input.pair,
          requestId: input.operation.requestId,
          ...(input.operation.reason === undefined ? {} : { reason: input.operation.reason }),
        };
        const targets: Array<{ threadId: ThreadId; recordIds: Array<string> }> = [];
        const queue = [input.pair.childThreadId];
        const seen = new Set<ThreadId>();
        while (queue.length) {
          const id = queue.shift()!;
          if (seen.has(id)) continue;
          seen.add(id);
          if (seen.size > 256) return yield* denied("The stop descendant bound was exceeded.");
          const records = yield* threads
            .getThreadRecords(id, ["subagents"])
            .pipe(Effect.mapError(unavailable));
          const owner = yield* forThread(id);
          targets.push({ threadId: id, recordIds: owner === undefined ? [] : [owner.id] });
          queue.push(
            ...records.subagents.flatMap((task) =>
              task.origin === "app_owned" && task.childThreadId !== null
                ? [task.childThreadId]
                : [],
            ),
          );
          const shells = yield* threads.getShellSnapshot().pipe(Effect.mapError(unavailable));
          const descendants = yield* readStopDescendants(
            id,
            [...shells.threads, ...shells.archivedThreads],
            (threadId) =>
              threads
                .getThreadShell(threadId)
                .pipe(Effect.map(Option.fromNullishOr), Effect.mapError(unavailable)),
          );
          queue.push(...descendants);
        }
        const context = {
          type: "task-stop" as const,
          ...control,
          recordIds: [original.id],
          targets,
        };
        const id = `task-stop:${yield* hash(context)}`;
        const record: Authority = { ...original, id, context };
        yield* save(record);
        const ceiling = yield* validate(record);
        if (!ceiling.capabilities.includes("orchestration"))
          return yield* denied("The stop has no orchestration grant.");
        return { ...record, ceiling };
      }),
    pendingActor,
    verifyTask,
    verifyTaskReceipt: (scope, input) => verifyTask(scope, input, true),
    checkEffect,
    authorizeStopEffect: (effectId, threadId) =>
      Effect.gen(function* () {
        const rows = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_effect_authority WHERE effect_id=${effectId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (!rows[0]) return undefined;
        const record = yield* read(rows[0].record_id);
        if (record.context?.type !== "task-stop") return undefined;
        yield* checkEffect(effectId, threadId);
        return record.id;
      }),
    legacyCeiling: (threadId) => legacy(threadId).pipe(Effect.map((record) => record?.ceiling)),
    checkThread,
    enroll,
    exchange,
    process,
    prepare,
    accept,
    checkPayload,
    bind,
    forThread,
    checkTarget: (scope, target) => checkBinding(scope, loadResource(target)).pipe(Effect.asVoid),
    historicalScheduleReceipt: (scope, request, expectedProjectId) =>
      Effect.gen(function* () {
        const accepted = yield* ScheduleReceipts.acceptance(request).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        if (Option.isNone(accepted)) return Option.none<McpScheduledMutationReceipt>();
        const { receipt, resource } = accepted.value;
        if (
          resource.owner !== requestNamespace(scope) ||
          request.generation !== requestGeneration(scope) ||
          !scope.capabilities.has("orchestration") ||
          (expectedProjectId !== undefined && expectedProjectId !== resource.projectId)
        )
          return yield* denied(
            "Historical scheduler acceptance belongs to its original authenticated owner and project.",
          );
        const binding: ResourceBinding = {
          type: "schedule",
          linkOrigin:
            resource.linkSessionId === undefined
              ? undefined
              : { sessionId: resource.linkSessionId },
        };
        let modes = yield* checkBinding(scope, Effect.succeed(binding));
        if (scope.thread !== undefined) {
          const caller = yield* threads
            .getThreadShell(scope.thread.threadId)
            .pipe(Effect.mapError(unavailable));
          if (
            caller === null ||
            caller.deletedAt !== null ||
            caller.archivedAt !== null ||
            caller.activeRunId === null
          )
            return yield* denied("The historical receipt's caller is no longer eligible.");
          modes = modes === undefined ? caller : Invocation.intersectModes(modes, caller);
        }
        if (modes === undefined)
          return yield* denied("An authenticated historical receipt caller is required.");
        if (resource.authorityRecordId !== undefined) {
          const original = yield* read(resource.authorityRecordId);
          const current = yield* validate(original);
          if (!current.capabilities.includes("orchestration"))
            return yield* denied(
              "The original scheduler authority no longer permits receipt access.",
            );
          yield* checkResource(original, current, binding);
          modes = Invocation.intersectModes(modes, current);
        }
        for (const acceptedMode of resource.runModes)
          if (exceededDispatchModeLimit(modes, acceptedMode) !== undefined)
            return yield* denied(
              "Historical scheduler acceptance exceeds the caller's current modes.",
            );
        return Option.some(receipt);
      }),
    returnNotice,
    registerPair,
    grants: Effect.gen(function* () {
      const now = yield* DateTime.now;
      const rows = yield* sql<{
        grant_json: string;
        environment_id: string;
        client_label: string | null;
        expires_at: string;
      }>`SELECT p.grant_json, p.environment_id, a.client_label, a.expires_at FROM peer_authority_sessions p JOIN auth_sessions a ON a.session_id = p.session_id WHERE a.revoked_at IS NULL`.pipe(
        Effect.mapError(unavailable),
      );
      const live = yield* Effect.filter(rows, (row) =>
        decodeExpiry(row.expires_at).pipe(
          Effect.map((expiresAt) => DateTime.isGreaterThan(expiresAt, now)),
          Effect.mapError(unavailable),
        ),
      );
      return yield* Effect.forEach(live, (row) =>
        decodeGrant(row.grant_json).pipe(
          Effect.map((access) => ({
            ...access,
            environmentId: EnvironmentId.make(row.environment_id),
            label: row.client_label ?? row.environment_id,
          })),
          Effect.mapError(unavailable),
        ),
      );
    }),
    setGrant: (input) =>
      Effect.gen(function* () {
        yield* activeSession(input.sessionId);
        const encoded = yield* encodeGrant(input).pipe(Effect.mapError(unavailable));
        const rows =
          yield* sql`UPDATE peer_authority_sessions SET grant_json = ${encoded} WHERE session_id = ${input.sessionId} RETURNING session_id`.pipe(
            Effect.mapError(unavailable),
          );
        if (rows.length !== 1) return yield* denied("This session has no authenticated peer link.");
        return input;
      }),
  });
});
export const layer = Layer.effect(PeerAuthority, make).pipe(Layer.provide(AuthSessions.layer));
