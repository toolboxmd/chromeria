import * as RemoteSecretPersistence from "../secrets/remoteSecretPersistence.ts";
import {
  RemoteSecretOrigin,
  type RemoteSecretControl,
  remoteSecretIntent,
} from "@t3tools/contracts";
import { rejectMcpClientAudience } from "../auth/EnvironmentAuth.ts";
import type {
  DeviceViewerOpenInput,
  PreviewViewerOpenInput,
  QualifiedAssetResource,
} from "@t3tools/contracts";
import * as LinkEvents from "./PeerLinkEvents.ts";
import * as ScheduleReceipts from "../scheduledTasks/mutationReceipts.ts";
import type { RequestIdentity } from "../mcp/McpRequestReceipts.ts";
import type { McpScheduledMutationReceipt } from "@t3tools/contracts";
import {
  Authority,
  Authority as InteractiveAuthority,
  ExecutionRecord,
  conjunction,
  PeerSession,
  intersect,
} from "./PeerAuthorityRecord.ts";
export { ExecutionRecord as Authority, PeerSession } from "./PeerAuthorityRecord.ts";
import { readStopDescendants } from "../childThreads/stopDescendants.ts";
import { initializePeerEffectAuthority, NativeAuthorityValidation } from "./peerEffectAuthority.ts";
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
import * as Automation from "../scheduledTasks/DestinationAutomationAuthority.ts";

export type ResourceTarget =
  | { readonly type: "thread"; readonly threadId: ThreadId }
  | { readonly type: "schedule"; readonly taskId: ScheduledTaskId }
  | { readonly type: "environment" };
type ResourceBinding =
  | { readonly type: "thread"; readonly thread: OrchestrationV2ThreadShell }
  | {
      readonly type: "schedule";
      readonly taskId: ScheduledTaskId;
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
const AutomationCapture = Schema.Struct({
  type: Schema.Literal("capture-automation"),
  envelope: PeerAuthorityEnvelope,
  certificateId: Schema.String,
  audience: EnvironmentId,
  binding: Automation.AutomationBinding,
  remaining: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
});
type AutomationCapture = typeof AutomationCapture.Type;
const LegacyAutomationCapture = Schema.Struct({
  type: Schema.Literal("capture-legacy-automation"),
  certificateId: Schema.String,
  audience: EnvironmentId,
  binding: Automation.AutomationBinding,
  contributor: Automation.LegacyContributor,
});
const LegacyAutomationAttestation = Schema.Struct({
  dependency: Automation.AutomationDependency,
  expiresAt: Schema.Number,
});
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
    Schema.Struct({ type: Schema.Literal("secret-origin"), envelope: PeerAuthorityEnvelope }),
    AutomationCapture,
    LegacyAutomationCapture,
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
const decodeSecretOrigin = Schema.decodeUnknownEffect(RemoteSecretOrigin);
const decodeAcceptedGraph = Schema.decodeUnknownEffect(Automation.AcceptedGraph);
const decodeLegacyAttestation = Schema.decodeUnknownEffect(LegacyAutomationAttestation);
const decodeAuthority = Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionRecord));
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
    readonly secretOrigin: (
      recordId: string,
    ) => Effect.Effect<RemoteSecretOrigin, OrchestratorMcpFailure>;
    readonly secretClient: (recordId: string) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly prepareSecretAnswer: (
      sessionId: AuthSessionId,
      peer: EnvironmentId,
      generation: string,
      input: RemoteSecretControl,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly nativePreflight: NativeAuthorityValidation["Service"]["preflight"];
    readonly notifyAutomationChanges: Effect.Effect<void>;
    readonly nextAutomationDisableDeadline: Effect.Effect<number | null, OrchestratorMcpFailure>;
    readonly pendingAutomationDisables: Effect.Effect<
      ReadonlyArray<{
        readonly dependency: Automation.AutomationDependency;
        readonly environmentId: EnvironmentId;
        readonly generation: string;
      }>,
      OrchestratorMcpFailure
    >;
    readonly acknowledgeAutomationDisable: (
      acknowledgement: Automation.AutomationDisableAck,
      environmentId: EnvironmentId,
      generation: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly receiveAutomationDisable: (
      input: Automation.AutomationDisable,
      credentialIdentity: string,
    ) => Effect.Effect<Automation.AutomationDisableAck | undefined, OrchestratorMcpFailure>;
    readonly captureAutomation: (
      scope: Invocation.McpInvocationScope | undefined,
      binding: Automation.AutomationBinding,
      action: RequestIdentity | { readonly nativeCommandId: string },
      limits: PeerCeiling,
    ) => Effect.Effect<Automation.ValidatedAcceptance | undefined, OrchestratorMcpFailure>;
    readonly automationAdmission: (
      value: Automation.ValidatedAcceptance,
      binding: Automation.AutomationBinding,
      actionId: string,
    ) => Effect.Effect<{
      readonly record: ExecutionRecord;
      readonly commit: Effect.Effect<void, OrchestratorMcpFailure>;
    }>;
    readonly automationExecution: (
      certificateId: string,
      actionId: string,
    ) => Effect.Effect<ExecutionRecord, OrchestratorMcpFailure>;
    readonly historicalScheduleReceipt: (
      scope: Invocation.McpInvocationScope,
      request: RequestIdentity,
      expectedProjectId?: string,
    ) => Effect.Effect<Option.Option<McpScheduledMutationReceipt>, OrchestratorMcpFailure>;
    readonly checkRecord: (id: string) => Effect.Effect<void, OrchestratorMcpFailure>;
    /** Only the native authenticated client ingress supplies this session id. */
    readonly prepareAttachmentRead: (
      sessionId: AuthSessionId,
      peer: EnvironmentId,
      generation: string,
      resource: QualifiedAssetResource,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly recheckAttachmentRead: (
      scope: Invocation.McpInvocationScope,
      resource: QualifiedAssetResource,
      capability: "device" | "preview",
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly prepareDeviceViewer: (
      sessionId: AuthSessionId,
      peer: EnvironmentId,
      generation: string,
      input: DeviceViewerOpenInput,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly recheckDeviceViewer: (
      scope: Invocation.McpInvocationScope,
      input: DeviceViewerOpenInput,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly preparePreviewViewer: (
      sessionId: AuthSessionId,
      peer: EnvironmentId,
      generation: string,
      input: PreviewViewerOpenInput,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly recheckPreviewViewer: (
      scope: Invocation.McpInvocationScope,
      input: PreviewViewerOpenInput,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly checkInteractiveRecord: (id: string) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly recheckInvocation: (
      scope: Invocation.McpThreadInvocationScope,
      intent: "device" | "preview" | "thread-resource",
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
    ) => Effect.Effect<InteractiveAuthority, OrchestratorMcpFailure>;
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
      credentialIdentity?: string,
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
      pair?: InteractiveAuthority["pair"],
      readOnly?: boolean,
      stableRequests?: boolean,
    ) => Effect.Effect<PeerAuthorityEnvelope, OrchestratorMcpFailure>;
    readonly accept: (
      scope: Invocation.McpInvocationScope,
      envelope: PeerAuthorityEnvelope,
    ) => Effect.Effect<InteractiveAuthority, OrchestratorMcpFailure>;
    readonly checkPayload: (
      authority: InteractiveAuthority,
      tool: string,
      args: unknown,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly bind: (
      authority: ExecutionRecord,
      threadId: ThreadId,
      registerReturn?: true,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly checkEffect: (
      effectId: string,
      threadId: ThreadId,
    ) => Effect.Effect<ExecutionRecord | undefined, OrchestratorMcpFailure>;
    readonly checkThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ExecutionRecord | undefined, OrchestratorMcpFailure>;
    readonly legacyCeiling: (
      threadId: ThreadId,
    ) => Effect.Effect<PeerCeiling | undefined, OrchestratorMcpFailure>;
    readonly checkTarget: (
      scope: Invocation.McpInvocationScope,
      target: ResourceTarget,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly forThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ExecutionRecord | undefined, OrchestratorMcpFailure>;
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
  const automation = yield* Automation.DestinationAutomationAuthority;
  const linkEvents = yield* Effect.serviceOption(LinkEvents.PeerLinkEvents);
  const policySettings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const wakeDisables = Option.isSome(linkEvents) ? linkEvents.value.automationChanged : Effect.void;
  const authSessions = yield* AuthSessions.AuthSessionRepository;
  const crypto = yield* Crypto.Crypto;
  const threads = yield* ProjectionStore.ProjectionStoreV2;
  const links = yield* Links.PeerLinks;
  const here = yield* Environment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  yield* sql`CREATE TABLE IF NOT EXISTS peer_authority_sessions (session_id TEXT PRIMARY KEY, environment_id TEXT NOT NULL, generation TEXT NOT NULL, grant_json TEXT NOT NULL)`;
  const sessionColumns = yield* sql<{ name: string }>`PRAGMA table_info(peer_authority_sessions)`;
  if (!sessionColumns.some((column) => column.name === "automation_version"))
    yield* sql`ALTER TABLE peer_authority_sessions ADD COLUMN automation_version INTEGER NOT NULL DEFAULT 0`;
  if (!sessionColumns.some((column) => column.name === "credential_identity"))
    yield* sql`ALTER TABLE peer_authority_sessions ADD COLUMN credential_identity TEXT`;
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
  yield* sql`DELETE FROM peer_control_frames WHERE json_extract(frame_json, '$.body.type') IN ('validate','capture-automation','capture-legacy-automation','request-actor','request-ancestry','secret-origin')`;
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
  const read = (id: string): Effect.Effect<ExecutionRecord, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM peer_authority_records WHERE id = ${id}`.pipe(
        Effect.mapError(unavailable),
      );
      if (!rows[0]) return yield* denied("Unknown peer authority record.");
      return yield* decodeAuthority(rows[0].record_json).pipe(Effect.mapError(unavailable));
    });
  const save = (record: ExecutionRecord) =>
    sql`INSERT INTO peer_authority_records (id, record_json) VALUES (${record.id}, ${json(record)}) ON CONFLICT DO NOTHING`.pipe(
      Effect.asVoid,
      Effect.mapError(unavailable),
    );
  const readInteractive = (id: string) =>
    Effect.gen(function* () {
      const record = yield* read(id);
      return "envelope" in record
        ? record
        : yield* denied("This operation requires an original interactive authority record.");
    });
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
          "resourceSessionId" in run &&
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
          ...conjunction(id, context, [run], AuthSessionId.make(imported.sessionId)),
          ceiling: intersect(run.ceiling, imported.ceiling),
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
      yield* save(conjunction(id, context, [created, run]));
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
  const nativeViewerSession = (
    sessionId: AuthSessionId,
    readOnly: boolean,
    capability: "device" | "preview" = "device",
  ) =>
    Effect.gen(function* () {
      yield* activeSession(sessionId);
      const session = yield* authSessions.getById({ sessionId }).pipe(Effect.mapError(unavailable));
      if (Option.isNone(session))
        return yield* denied("The original native client is unavailable.");
      yield* rejectMcpClientAudience(session.value).pipe(
        Effect.mapError(() => denied("An MCP agent cannot claim native human viewing authority.")),
      );
      if (
        !session.value.scopes.includes("orchestration:read") ||
        (!readOnly &&
          !session.value.scopes.includes(
            capability === "preview" ? "preview:operate" : "orchestration:operate",
          ))
      )
        return yield* denied("The original native client no longer permits this viewer operation.");
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
  const enroll: PeerAuthority["Service"]["enroll"] = (scope, peer, credentialIdentity) =>
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
      if (peer.automationVersion === 1 && credentialIdentity !== undefined) {
        const enrolled = yield* sql<{
          credential_identity: string | null;
        }>`SELECT credential_identity FROM peer_authority_sessions WHERE session_id=${sessionId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (
          enrolled[0]?.credential_identity !== null &&
          enrolled[0]?.credential_identity !== credentialIdentity
        )
          return yield* denied("The recovery credential does not match its original session.");
        yield* sql`UPDATE peer_authority_sessions SET automation_version=1,credential_identity=${credentialIdentity} WHERE session_id=${sessionId}`.pipe(
          Effect.mapError(unavailable),
        );
      }
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
            frame.body.type === "capture-automation" ||
            frame.body.type === "capture-legacy-automation" ||
            frame.body.type === "request-actor" ||
            frame.body.type === "request-ancestry" ||
            frame.body.type === "secret-origin"
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
      const launch = yield* readInteractive(rows[0]!.record_id);
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
      if (Option.isNone(policySettings)) return capabilities;
      const current = yield* policySettings.value.getSettings.pipe(Effect.mapError(unavailable));
      const effective = resolveProjectSettings(current, projectId).settings;
      return capabilities.filter((name) =>
        name === "preview"
          ? effective.enableAgentBrowserAccess
          : name === "device"
            ? effective.enableAgentDeviceAccess
            : true,
      );
    });
  type Coverage = ReadonlyMap<string, Automation.AcceptedGraph["records"][number]> & {
    readonly executions: ReadonlySet<string>;
  };
  const emptyCoverage = (): Coverage =>
    Object.assign(new Map<string, Automation.AcceptedGraph["records"][number]>(), {
      executions: new Set<string>(),
    });
  const coverageFor = (record: ExecutionRecord): Effect.Effect<Coverage, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const covered = new Map<string, Automation.AcceptedGraph["records"][number]>();
      const pending = [record];
      const seen = new Set<string>();
      const certificates: Extract<ExecutionRecord, { readonly certificateId: string }>[] = [];
      const executions = new Set<string>();
      while (pending.length !== 0) {
        const next = pending.pop()!;
        if (seen.has(next.id)) continue;
        seen.add(next.id);
        if (seen.size > 256) return yield* denied("The automation contributor bound was exceeded.");
        if ("certificateId" in next) {
          if (next.kind === "automation-execution") certificates.push(next);
        } else if (next.context !== undefined) {
          const context = next.context;
          const ids =
            context.type === "run-inputs" || context.type === "task-stop"
              ? [
                  ...context.recordIds,
                  ...(context.type === "task-stop"
                    ? context.targets.flatMap((target) => target.recordIds)
                    : []),
                ]
              : context.type === "owned-thread"
                ? [context.creationRecordId, context.runRecordId]
                : [context.runRecordId];
          for (const id of ids) pending.push(yield* read(id));
        }
      }
      const failures: {
        record: Extract<ExecutionRecord, { readonly certificateId: string }>;
        error: OrchestratorMcpFailure;
      }[] = [];
      for (const next of certificates) {
        const claimed = yield* Effect.result(automation.claim(next.certificateId));
        if (claimed._tag === "Failure") {
          failures.push({ record: next, error: claimed.failure });
          continue;
        }
        const certificate = claimed.success;
        executions.add(certificate.id);
        if (certificate.audience !== here || certificate.binding.projectId !== next.projectId)
          return yield* denied("The automation authority belongs to another destination.");
        for (const node of certificate.graph.records) {
          if (node.environmentId !== here) continue;
          const previous = covered.get(node.record.id);
          if (previous !== undefined && json(previous.record) !== json(node.record))
            return yield* denied("Automation contributors have conflicting immutable facts.");
          covered.set(
            node.record.id,
            previous === undefined
              ? node
              : {
                  ...node,
                  ceiling: intersect(previous.ceiling, node.ceiling),
                  expiresAt:
                    previous.expiresAt === null
                      ? node.expiresAt
                      : node.expiresAt === null
                        ? previous.expiresAt
                        : Math.min(previous.expiresAt, node.expiresAt),
                },
          );
        }
      }
      for (const failure of failures) {
        const frozen = covered.get(failure.record.id);
        if (frozen === undefined || json(frozen.record) !== json(failure.record))
          return yield* failure.error;
      }
      return Object.assign(covered, { executions });
    });
  const validate = (
    record: ExecutionRecord,
    stop?: StopValidation,
    coverage?: Coverage,
    purpose: "local" | "interactive" = "local",
    replacements: ReadonlyMap<string, Automation.AutomationCertificate> = new Map(),
    capabilityView: "operation" | "contributor" = "operation",
  ): Effect.Effect<PeerCeiling, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (
        "envelope" in record &&
        (record.tool === "device_viewer" ||
          record.tool === "preview_viewer" ||
          record.tool === "attachment_read")
      ) {
        if (
          record.sessionId !== null ||
          record.originClientId === null ||
          record.localThreadId !== null ||
          record.parentId !== null
        )
          return yield* denied(
            "Native viewing requires its original authenticated client session.",
          );
        yield* nativeViewerSession(
          record.originClientId,
          record.readOnly,
          record.tool === "preview_viewer" ? "preview" : "device",
        );
      }
      const accepted =
        coverage ??
        (purpose === "local"
          ? yield* coverageFor(record)
          : Object.assign(new Map<string, Automation.AcceptedGraph["records"][number]>(), {
              executions: new Set<string>(),
            }));
      if ("certificateId" in record && record.kind === "automation-birth")
        yield* automation.birth(record.certificateId);
      const frozen = accepted.get(record.id);
      if (frozen !== undefined) {
        if (json(frozen.record) !== json(record))
          return yield* denied("The accepted contributor identity changed.");
        if (frozen.expiresAt !== null && frozen.expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* denied("An accepted automation contributor has expired.");
        if ("envelope" in record && record.sessionId !== null) {
          const current = yield* grant(record.sessionId);
          return intersect(frozen.ceiling, {
            ...frozen.ceiling,
            resourceScope: current.resourceScope,
            onward: current.onward,
            capabilities: frozen.ceiling.capabilities.filter(
              (capability) =>
                (capability !== "preview" && capability !== "device") || current[capability],
            ),
          });
        }
        return frozen.ceiling;
      }
      if ("certificateId" in record) {
        const nextPurpose =
          record.kind === "automation-birth" && !accepted.executions.has(record.certificateId)
            ? "interactive"
            : purpose;
        const certificate =
          replacements.get(record.certificateId) ??
          (yield* record.kind === "automation-birth"
            ? automation.birth(record.certificateId)
            : automation.claim(record.certificateId));
        if (certificate.audience !== here || certificate.binding.projectId !== record.projectId)
          return yield* denied("The automation authority belongs to another destination.");
        let ceiling = certificate.ceiling;
        const historical = new Map(replacements);
        if (nextPurpose === "interactive" && certificate.binding.kind === "spectrum") {
          for (const node of certificate.graph.records) {
            if (node.environmentId !== here || !("certificateId" in node.record)) continue;
            const prior = yield* automation.birth(node.record.certificateId);
            if (
              prior.binding.kind === "spectrum" &&
              prior.binding.resourceId === certificate.binding.resourceId &&
              prior.binding.workflowGeneration < certificate.binding.workflowGeneration
            )
              historical.set(prior.id, prior);
          }
        }
        // B's capability availability belongs to the operation, after local birth composition.
        for (const id of certificate.recordIds)
          ceiling = intersect(
            ceiling,
            yield* validate(
              yield* read(id),
              stop,
              accepted,
              nextPurpose,
              historical,
              "contributor",
            ),
          );
        if (
          nextPurpose === "interactive" &&
          certificate.graph.legacy.some((owner) => owner.environmentId !== here)
        ) {
          for (const id of certificate.recordIds) {
            const live = yield* captureRecord(
              yield* read(id),
              {
                certificateId: certificate.id,
                audience: certificate.audience,
                binding: certificate.binding,
                remaining: 16,
              },
              new Set(),
              undefined,
              historical,
            );
            for (const contributor of live.legacy)
              ceiling = intersect(ceiling, contributor.ceiling);
          }
        }
        for (const owner of certificate.graph.legacy) {
          if (owner.environmentId !== here) continue;
          yield* activeSession(owner.sessionId);
          if (nextPurpose === "interactive") {
            const live = yield* captureLegacy(owner, {
              certificateId: certificate.id,
              audience: certificate.audience,
              binding: certificate.binding,
              remaining: 16,
            });
            for (const contributor of live.legacy)
              ceiling = intersect(ceiling, contributor.ceiling);
          }
        }
        const current = {
          ...ceiling,
          capabilities:
            capabilityView === "operation"
              ? yield* currentCapabilities(ceiling.capabilities, record.projectId)
              : ceiling.capabilities,
        };
        if (
          record.kind === "automation-birth" &&
          nextPurpose === "interactive" &&
          json(intersect(certificate.ceiling, ceiling)) !== json(certificate.ceiling)
        )
          return yield* denied(
            "An original automation birth contributor was narrowed by current policy.",
          );
        return current;
      }
      if (record.context?.type === "task-stop") {
        const control = record.context;
        let ceiling = record.ceiling;
        for (const id of control.recordIds)
          ceiling = intersect(
            ceiling,
            yield* validate(
              yield* read(id),
              control,
              accepted,
              purpose,
              replacements,
              capabilityView,
            ),
          );
        for (const target of control.targets) {
          const binding = yield* loadResource({ type: "thread", threadId: target.threadId });
          if (binding.type !== "thread" || binding.thread.archivedAt !== null)
            return yield* denied("The stop target was retired.");
          for (const id of control.recordIds) {
            const owner = yield* read(id);
            yield* checkResource(
              owner,
              yield* validate(owner, control, accepted, purpose, replacements, capabilityView),
              binding,
              control,
              accepted,
            );
          }
          for (const id of target.recordIds) {
            const owner = yield* read(id);
            const current = yield* validate(
              owner,
              control,
              accepted,
              purpose,
              replacements,
              capabilityView,
            );
            yield* checkResource(owner, current, binding, control, accepted);
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
          ceiling = intersect(
            ceiling,
            yield* validate(yield* read(id), stop, accepted, purpose, replacements, capabilityView),
          );
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
          const current = yield* validate(
            owner,
            stop,
            accepted,
            purpose,
            replacements,
            capabilityView,
          );
          ceiling = intersect(ceiling, current);
        }
        return ceiling;
      }
      if (!("envelope" in record)) return yield* denied("Invalid local authority context.");
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
        const parent = yield* validate(
          yield* read(record.parentId),
          stop,
          accepted,
          purpose,
          replacements,
          capabilityView,
        );
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
        capabilities:
          capabilityView === "operation"
            ? yield* currentCapabilities(record.ceiling.capabilities, thread.projectId)
            : record.ceiling.capabilities,
        runtimeMode: thread.runtimeMode,
        interactionMode: thread.interactionMode,
      };
      const parent = record.parentId === null ? undefined : yield* read(record.parentId);
      const ceiling =
        parent === undefined
          ? record.ceiling
          : yield* validate(
              parent,
              stop?.upstream ?? stop,
              accepted,
              purpose,
              replacements,
              capabilityView,
            );
      if (parent !== undefined) {
        if (!ceiling.onward)
          return yield* denied("The originating grant does not permit onward delegation.");
        // A forwarded action still depends on permission for its actual source
        // thread, even when its owners were inherited from an older context.
        yield* checkResource(
          parent,
          ceiling,
          { type: "thread", thread },
          stop?.upstream ?? stop,
          accepted,
        );
      }
      return intersect(current, intersect(record.ceiling, ceiling));
    });
  const resourceSession = (record: ExecutionRecord) =>
    "envelope" in record
      ? (record.resourceSessionId ?? record.sessionId)
      : "resourceSessionId" in record
        ? (record.resourceSessionId ?? null)
        : null;
  // Creation membership is a union; an acting authority is a conjunction.
  // Only permanent birth bindings use membership, never an existing target's run.
  const hasResourceSession = (
    record: ExecutionRecord,
    sessionId: string,
    coverage?: Coverage,
  ): Effect.Effect<boolean, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if ("certificateId" in record) {
        const certificate = coverage?.has(record.id)
          ? yield* automation.facts(record.certificateId)
          : yield* record.kind === "automation-birth"
              ? automation.birth(record.certificateId)
              : automation.claim(record.certificateId);
        return (
          certificate.graph.records.some(
            (node) => node.environmentId === here && resourceSession(node.record) === sessionId,
          ) ||
          certificate.graph.legacy.some(
            (node) => node.environmentId === here && node.sessionId === sessionId,
          )
        );
      }
      const context = record.context;
      if (context === undefined) return resourceSession(record) === sessionId;
      if (context.type === "legacy-thread" && record.resourceSessionId === sessionId) return true;
      const ids =
        context.type === "run-inputs" || context.type === "task-stop"
          ? context.recordIds
          : context.type === "owned-thread"
            ? [context.creationRecordId, context.runRecordId]
            : [context.runRecordId];
      for (const id of ids)
        if (yield* hasResourceSession(yield* read(id), sessionId, coverage)) return true;
      return false;
    });
  const checkCreated = (
    sessionId: string,
    target: ResourceBinding,
    coverage?: Coverage,
  ): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (target.type === "schedule" && target.linkOrigin?.sessionId === sessionId) return;
      if (target.type === "thread") {
        if (target.thread.linkOrigin?.sessionId === sessionId) return;
        const created = yield* permanentForThread(target.thread.id);
        if (created !== undefined && (yield* hasResourceSession(created, sessionId, coverage)))
          return;
      }
      return yield* denied("The destination is outside the link's current resource scope.");
    });
  const checkResource = (
    record: ExecutionRecord,
    ceiling: PeerCeiling,
    target: ResourceBinding,
    stop?: StopValidation,
    coverage?: Coverage,
  ): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      const accepted = coverage ?? (yield* coverageFor(record));
      if ("certificateId" in record) {
        const certificate = accepted.has(record.id)
          ? yield* automation.facts(record.certificateId)
          : yield* record.kind === "automation-birth"
              ? automation.birth(record.certificateId)
              : automation.claim(record.certificateId);
        if (target.type === "thread") {
          if (target.thread.projectId !== certificate.binding.projectId)
            return yield* denied("The automation cannot move to another project.");
          if (
            target.thread.id !== certificate.binding.threadId &&
            !(
              certificate.binding.kind === "spectrum" &&
              (certificate.binding.callerThreadId === target.thread.id ||
                certificate.binding.memberThreadIds.includes(target.thread.id))
            )
          ) {
            const pending: ExecutionRecord[] = [];
            const birth = yield* permanentForThread(target.thread.id);
            if (birth !== undefined) pending.push(birth);
            const seen = new Set<string>();
            let belongs = false;
            while (pending.length !== 0 && seen.size < 256) {
              const owner = pending.pop()!;
              if (seen.has(owner.id)) continue;
              seen.add(owner.id);
              if ("certificateId" in owner && owner.certificateId === certificate.id) {
                belongs = true;
                break;
              }
              if ("certificateId" in owner) {
                const inherited = yield* owner.kind === "automation-birth"
                  ? automation.birth(owner.certificateId)
                  : automation.claim(owner.certificateId);
                if (
                  inherited.audience !== here ||
                  inherited.binding.projectId !== target.thread.projectId
                )
                  return yield* denied("The descendant automation belongs to another destination.");
                for (const id of inherited.recordIds) pending.push(yield* read(id));
              }
              if (owner.context !== undefined) {
                const context = owner.context;
                const ids =
                  context.type === "run-inputs" || context.type === "task-stop"
                    ? context.recordIds
                    : context.type === "owned-thread"
                      ? [context.creationRecordId, context.runRecordId]
                      : [context.runRecordId];
                for (const id of ids) pending.push(yield* read(id));
              }
            }
            if (!belongs) return yield* denied("The thread is outside this accepted automation.");
          }
        } else if (
          target.type === "schedule" &&
          (certificate.binding.kind !== "schedule" ||
            target.taskId !== certificate.binding.resourceId)
        )
          return yield* denied("The schedule is outside this accepted automation.");
        for (const owner of certificate.graph.legacy)
          if (owner.environmentId === here) {
            yield* activeSession(owner.sessionId);
            if (owner.ceiling.resourceScope === "created-work")
              yield* checkCreated(owner.sessionId, target, accepted);
          }
        for (const id of certificate.recordIds) {
          const owner = yield* read(id);
          yield* checkResource(
            owner,
            yield* validate(owner, stop, accepted),
            target,
            stop,
            accepted,
          );
        }
        return;
      }
      const context = record.context;
      if (context?.type === "task-stop") {
        if (target.type !== "thread")
          return yield* denied("A stop obligation only controls its exact threads.");
        const entry = context.targets.find((item) => item.threadId === target.thread.id);
        if (entry === undefined)
          return yield* denied("The effect is outside the recorded stop target set.");
        for (const id of [...context.recordIds, ...entry.recordIds]) {
          const owner = yield* read(id);
          yield* checkResource(
            owner,
            yield* validate(owner, context, accepted),
            target,
            context,
            accepted,
          );
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
          yield* checkCreated(imported.sessionId, target, accepted);
        }
        const ids =
          context.type === "run-inputs"
            ? context.recordIds
            : context.type === "owned-thread"
              ? [context.creationRecordId, context.runRecordId]
              : [context.runRecordId];
        for (const id of ids) {
          const owner = yield* read(id);
          yield* checkResource(
            owner,
            yield* validate(owner, stop, accepted),
            target,
            stop,
            accepted,
          );
        }
        return;
      }
      const sessionId = resourceSession(record);
      if (sessionId !== null && ceiling.resourceScope === "created-work")
        yield* checkCreated(sessionId, target, accepted);
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
      return { type: "schedule", taskId: target.taskId, linkOrigin };
    });
  const checkThread: PeerAuthority["Service"]["checkThread"] = (threadId) =>
    Effect.gen(function* () {
      const record = yield* forThread(threadId);
      if (record !== undefined) {
        const coverage = yield* coverageFor(record);
        const ceiling = yield* validate(record, undefined, coverage);
        const target = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
        if (target === null || target.deletedAt !== null)
          return yield* denied("The destination thread is no longer available.");
        yield* checkResource(
          record,
          ceiling,
          { type: "thread", thread: target },
          undefined,
          coverage,
        );
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
        scope.client !== undefined && parent && "envelope" in parent
          ? [...parent.envelope.route, here]
          : [here];
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
      yield* validate(record, undefined, undefined, "interactive");
      return envelope;
    });
  const prepareAttachmentRead: PeerAuthority["Service"]["prepareAttachmentRead"] = (
    sessionId,
    peer,
    generation,
    resource,
  ) =>
    Effect.gen(function* () {
      if (
        (resource._tag === "peer-attachment"
          ? resource.reference.environmentId
          : resource.resource.environmentId) !== peer ||
        peer === here
      )
        return yield* denied("The attachment must address its exact linked environment.");
      yield* nativeViewerSession(sessionId, true);
      const id = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const envelope: PeerAuthorityEnvelope = {
        version: 1,
        environmentId: here,
        generation,
        recordId: id,
        actionId: id,
        digest: yield* hash({ tool: "attachment_read", args: resource }),
        route: [here],
        descendant: false,
      };
      const record: Authority = {
        id,
        peer: { environmentId: peer, generation },
        sessionId: null,
        localThreadId: null,
        originClientId: sessionId,
        parentId: null,
        requestActorThreadId: null,
        envelope,
        tool: "attachment_read",
        readOnly: true,
        originRunId: null,
        // The publisher's persisted provenance chooses the required capability at
        // the destination. Current grants and ALL resource owners narrow this set.
        ceiling: {
          runtimeMode: "full-access",
          interactionMode: "default",
          resourceScope: "environment-resources",
          onward: false,
          capabilities: ["device", "preview"],
        },
      };
      yield* save(record);
      yield* validate(record);
      return envelope;
    });
  const recheckAttachmentRead: PeerAuthority["Service"]["recheckAttachmentRead"] = (
    scope,
    resource,
    capability,
  ) =>
    Effect.gen(function* () {
      if (
        scope.client?.linked !== true ||
        scope.authority === undefined ||
        (resource._tag === "peer-attachment"
          ? resource.reference.environmentId
          : resource.resource.environmentId) !== here
      )
        return yield* denied(
          "Attachment access requires its original authenticated destination authority.",
        );
      const record = yield* read(scope.authority.id);
      if (!("envelope" in record))
        return yield* denied("Native resource access requires its original interactive authority.");
      // The authenticated payload digest distinguishes the original MCP read
      // from a native viewer read. It cannot convert either actor into the other.
      const acquisition =
        record.envelope.requestOwner !== undefined &&
        record.envelope.digest === requestDigest({ tool: "attachment_acquire", args: resource });
      yield* checkPayload(record, acquisition ? "attachment_acquire" : "attachment_read", resource);
      const ceiling = intersect(scope.authority.ceiling, yield* validate(record));
      const target = yield* loadResource({
        type: "thread",
        threadId:
          resource._tag === "peer-attachment" ? resource.threadId : resource.resource.threadId,
      });
      if (target.type !== "thread" || target.thread.archivedAt !== null)
        return yield* denied("The attachment context is no longer available.");
      yield* checkResource(record, ceiling, target);
      const capabilities = yield* currentCapabilities(
        ceiling.capabilities,
        target.thread.projectId,
      );
      if (
        !scope.capabilities.has(capability) ||
        !capabilities.includes(capability) ||
        exceededDispatchModeLimit(ceiling, target.thread) !== undefined
      )
        return yield* denied("The original attachment reader no longer permits this resource.");
    });
  const prepareViewer = (
    sessionId: AuthSessionId,
    peer: EnvironmentId,
    generation: string,
    input: DeviceViewerOpenInput | PreviewViewerOpenInput,
  ) =>
    Effect.gen(function* () {
      const capability = input.resource.type;
      const tool = capability === "device" ? "device_viewer" : "preview_viewer";
      if (input.resource.environmentId !== peer || peer === here)
        return yield* denied("The device viewer must address its exact linked environment.");
      yield* nativeViewerSession(sessionId, input.operation === "view", capability);
      const id = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const envelope: PeerAuthorityEnvelope = {
        version: 1,
        environmentId: here,
        generation,
        recordId: id,
        actionId: id,
        digest: yield* hash({ tool, args: input }),
        route: [here],
        descendant: false,
      };
      const record: Authority = {
        id,
        peer: { environmentId: peer, generation },
        sessionId: null,
        localThreadId: null,
        originClientId: sessionId,
        parentId: null,
        requestActorThreadId: null,
        ceiling: {
          runtimeMode: "full-access",
          interactionMode: "default",
          resourceScope: "environment-resources",
          onward: false,
          capabilities: [capability],
        },
        envelope,
        tool,
        readOnly: input.operation === "view",
        originRunId: null,
      };
      yield* save(record);
      yield* validate(record);
      return envelope;
    });
  const recheckViewer = (
    scope: Invocation.McpInvocationScope,
    input: DeviceViewerOpenInput | PreviewViewerOpenInput,
  ) =>
    Effect.gen(function* () {
      const capability = input.resource.type;
      if (
        scope.client?.linked !== true ||
        scope.authority === undefined ||
        input.resource.environmentId !== here
      )
        return yield* denied("Device viewing needs its authenticated original peer authority.");
      const record = yield* read(scope.authority.id);
      if (!("envelope" in record))
        return yield* denied("Native resource access requires its original interactive authority.");
      yield* checkPayload(
        record,
        capability === "device" ? "device_viewer" : "preview_viewer",
        input,
      );
      const ceiling = intersect(scope.authority.ceiling, yield* validate(record));
      if (!scope.capabilities.has(capability) || !ceiling.capabilities.includes(capability))
        return yield* denied("The original device viewing capability was retired.");
      const target = yield* loadResource({ type: "thread", threadId: input.resource.threadId });
      if (target.type !== "thread" || target.thread.archivedAt !== null)
        return yield* denied("The device viewing context is no longer available.");
      yield* checkResource(record, ceiling, target);
      if (
        input.operation === "control" &&
        exceededDispatchModeLimit(ceiling, target.thread) !== undefined
      )
        return yield* denied(
          "Device control exceeds the current originating or destination modes.",
        );
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
      const frozen = yield* readInteractive(record.id);
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
      if (!("envelope" in authority)) return;
      yield* sql`INSERT INTO peer_thread_authority (thread_id, record_id) VALUES (${threadId}, ${authority.id}) ON CONFLICT DO NOTHING`.pipe(
        Effect.mapError(unavailable),
      );
      if (!("envelope" in authority)) return;
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
      const record = yield* readInteractive(envelope.recordId);
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
  const validateReturn = (record: ExecutionRecord): Effect.Effect<void, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if ("certificateId" in record) {
        const certificate = yield* record.kind === "automation-birth"
          ? automation.birth(record.certificateId)
          : automation.claim(record.certificateId);
        for (const id of certificate.recordIds) yield* validateReturn(yield* read(id));
        return;
      }
      if (!("envelope" in record)) {
        const context = record.context;
        const ids =
          context.type === "run-inputs" || context.type === "task-stop"
            ? context.recordIds
            : context.type === "owned-thread"
              ? [context.creationRecordId, context.runRecordId]
              : [context.runRecordId];
        for (const id of ids) yield* validateReturn(yield* read(id));
        return;
      }
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
        return yield* actorForRecord(yield* readInteractive(record.parentId));
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
        const owner = yield* readInteractive(rows[0]!.record_id);
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
      const actor: PendingActor | undefined =
        scope.client?.linked === true && scope.authority !== undefined
          ? yield* actorForRecord(yield* readInteractive(scope.authority.id))
          : scope.thread !== undefined
            ? { type: "thread", environmentId: here, threadId: scope.thread.threadId }
            : undefined;
      if (actor === undefined) return undefined;
      if (actor.type === "client") return "client";
      if (actor.environmentId === here && actor.threadId === target) return undefined;
      return (yield* ancestor(actor, target)) ? "ancestor" : undefined;
    });
  const mergeGraphs = (graphs: ReadonlyArray<Automation.AcceptedGraph>) =>
    Effect.gen(function* () {
      const records = new Map<string, Automation.AcceptedGraph["records"][number]>();
      const legacyOwners = new Map<string, Automation.AcceptedGraph["legacy"][number]>();
      const dependencies = new Map<string, Automation.AutomationDependency>();
      for (const graph of graphs) {
        for (const node of graph.records) {
          const key = `${node.environmentId}:${node.record.id}`;
          const prior = records.get(key);
          if (prior !== undefined && json(prior.record) !== json(node.record))
            return yield* denied("An automation contributor changed during capture.");
          records.set(
            key,
            prior === undefined
              ? node
              : {
                  ...node,
                  ceiling: intersect(prior.ceiling, node.ceiling),
                  expiresAt:
                    prior.expiresAt === null
                      ? node.expiresAt
                      : node.expiresAt === null
                        ? prior.expiresAt
                        : Math.min(prior.expiresAt, node.expiresAt),
                },
          );
        }
        for (const node of graph.legacy) {
          const key = `${node.environmentId}:${node.threadId === null ? `schedule:${node.taskId}` : `thread:${node.threadId}`}:${node.sessionId}`;
          const prior = legacyOwners.get(key);
          legacyOwners.set(
            key,
            prior === undefined
              ? node
              : {
                  ...node,
                  ceiling: intersect(prior.ceiling, node.ceiling),
                  expiresAt: Math.min(prior.expiresAt, node.expiresAt),
                },
          );
        }
        for (const dependency of graph.dependencies)
          dependencies.set(requestDigest(dependency), dependency);
      }
      if (records.size > 256 || legacyOwners.size > 256 || dependencies.size > 256)
        return yield* denied("The automation contributor bound was exceeded.");
      return {
        records: [...records.values()],
        legacy: [...legacyOwners.values()],
        dependencies: [...dependencies.values()],
      };
    });
  // Session identity alone cannot cover a later, unrelated input from the same owner.
  type BirthFacts = { readonly records: ReadonlySet<string>; readonly legacy: ReadonlySet<string> };
  const noBirthFacts = (): BirthFacts => ({ records: new Set(), legacy: new Set() });
  const addBirthFacts = (
    facts: BirthFacts,
    certificate: Automation.AutomationCertificate,
  ): BirthFacts => ({
    records: new Set([
      ...facts.records,
      ...certificate.graph.records
        .filter((node) => node.environmentId === here)
        .map((node) => json(node.record)),
    ]),
    legacy: new Set([
      ...facts.legacy,
      ...certificate.graph.legacy
        .filter((node) => node.environmentId === here)
        .map((node) => json(node)),
    ]),
  });
  const dependencyGrant = (sessionId: string, covered: boolean): Automation.DependencyGrant =>
    covered ? { kind: "automation-birth", sessionId } : sessionId;
  const captureLegacy = (
    owner: Automation.LegacyContributor,
    input: Omit<AutomationCapture, "type" | "envelope">,
    relay?: { readonly environmentId: EnvironmentId; readonly generation: string },
    birthFacts: BirthFacts = noBirthFacts(),
  ): Effect.Effect<Automation.AcceptedGraph, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (owner.environmentId !== here)
        return yield* denied("Legacy capture requires its actual resource owner.");
      yield* activeSession(owner.sessionId);
      const channel = (yield* sql<{
        environment_id: EnvironmentId;
        generation: string;
        automation_version: number;
        credential_identity: string | null;
      }>`SELECT environment_id,generation,automation_version,credential_identity FROM peer_authority_sessions WHERE session_id=${owner.sessionId}`.pipe(
        Effect.mapError(unavailable),
      ))[0];
      if (
        channel === undefined ||
        channel.automation_version !== 1 ||
        channel.credential_identity === null
      )
        return yield* denied(
          "The legacy source does not support destination automation authority v1.",
        );
      const dependency: Automation.AutomationDependency = {
        sourceEnvironmentId: channel.environment_id,
        destinationEnvironmentId: input.audience,
        certificateId: input.certificateId,
        recordId: owner.sessionId,
        grantGeneration: channel.generation,
        resourceId: input.binding.resourceId,
        definitionGeneration: input.binding.definitionGeneration,
        definitionDigest: input.binding.definitionDigest,
      };
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* automation.registerRecovery(
              dependency,
              owner.sessionId,
              channel.credential_identity!,
            );
            if (relay !== undefined)
              yield* automation.registerDependency(
                dependency,
                relay,
                [dependencyGrant(owner.sessionId, birthFacts.legacy.has(json(owner)))],
                false,
              );
          }),
        )
        .pipe(
          Effect.mapError(unavailable),
          Effect.tap(() => (relay === undefined ? Effect.void : wakeDisables)),
        );
      const attestation = yield* decodeLegacyAttestation(
        yield* ask(owner.sessionId, {
          id: `capture-legacy:${input.certificateId}:${owner.sessionId}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
          body: {
            type: "capture-legacy-automation",
            certificateId: input.certificateId,
            audience: input.audience,
            binding: input.binding,
            contributor: owner,
          },
        }),
      ).pipe(Effect.mapError(unavailable));
      if (json(attestation.dependency) !== json(dependency))
        return yield* denied("The legacy attestation changed its exact dependency.");
      return {
        records: [],
        legacy: [{ ...owner, expiresAt: Math.min(owner.expiresAt, attestation.expiresAt) }],
        dependencies: [dependency],
      };
    });
  const captureRecord = (
    record: ExecutionRecord,
    input: Omit<AutomationCapture, "type" | "envelope">,
    ancestors: ReadonlySet<string> = new Set(),
    relay?: { readonly environmentId: EnvironmentId; readonly generation: string },
    replacements: ReadonlyMap<string, Automation.AutomationCertificate> = new Map(),
    inheritedBirthFacts: BirthFacts = noBirthFacts(),
  ): Effect.Effect<Automation.AcceptedGraph, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
      if (input.remaining < 1 || ancestors.has(record.id) || ancestors.size >= 256)
        return yield* denied("The automation contributor graph is cyclic or too deep.");
      const ceiling = yield* validate(record, undefined, undefined, "interactive", replacements);
      const children: Automation.AcceptedGraph[] = [];
      const legacyOwners: Automation.AcceptedGraph["legacy"][number][] = [];
      const sessionIds = new Set<string>();
      let expiresAt: number | null = null;
      const nextAncestors = new Set([...ancestors, record.id]);
      const childIds: string[] = [];
      let birthFacts = inheritedBirthFacts;
      if (record.context !== undefined) {
        const context = record.context;
        childIds.push(
          ...(context.type === "run-inputs" || context.type === "task-stop"
            ? [
                ...context.recordIds,
                ...(context.type === "task-stop"
                  ? context.targets.flatMap((target) => target.recordIds)
                  : []),
              ]
            : context.type === "owned-thread"
              ? [context.creationRecordId, context.runRecordId]
              : [context.runRecordId]),
        );
      }
      if ("certificateId" in record) {
        const previous =
          replacements.get(record.certificateId) ??
          (yield* record.kind === "automation-birth"
            ? automation.birth(record.certificateId)
            : automation.claim(record.certificateId));
        if (record.kind === "automation-birth") birthFacts = addBirthFacts(birthFacts, previous);
        expiresAt = previous.expiresAt;
        if (expiresAt !== null && expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* denied("The original automation contributor expired.");
        childIds.push(...previous.recordIds);
        legacyOwners.push(...previous.graph.legacy.filter((owner) => owner.environmentId === here));
      }
      if ("envelope" in record && record.context === undefined) {
        if (record.parentId !== null) childIds.push(record.parentId);
        for (const sessionId of [
          record.sessionId,
          record.originClientId,
          record.requestClientSessionId,
          record.resourceSessionId,
        ]) {
          if (sessionId === null || sessionId === undefined) continue;
          yield* activeSession(sessionId);
          const session = yield* authSessions
            .getById({ sessionId: AuthSessionId.make(sessionId) })
            .pipe(Effect.mapError(unavailable));
          if (Option.isNone(session)) return yield* unavailable();
          sessionIds.add(sessionId);
          const expiry = DateTime.toEpochMillis(session.value.expiresAt);
          expiresAt = expiresAt === null ? expiry : Math.min(expiresAt, expiry);
        }
        if (record.sessionId !== null) {
          const channel = (yield* sql<{
            automation_version: number;
            credential_identity: string | null;
          }>`SELECT automation_version,credential_identity FROM peer_authority_sessions WHERE session_id=${record.sessionId}`.pipe(
            Effect.mapError(unavailable),
          ))[0];
          if (channel?.automation_version !== 1 || channel.credential_identity === null)
            return yield* denied(
              "The source does not support destination automation authority v1.",
            );
          if (input.remaining <= 1)
            return yield* denied("The automation capture hop bound was exceeded.");
          const expected: Automation.AutomationDependency = {
            sourceEnvironmentId: record.envelope.environmentId,
            destinationEnvironmentId: input.audience,
            certificateId: input.certificateId,
            recordId: record.envelope.recordId,
            grantGeneration: record.peer.generation,
            resourceId: input.binding.resourceId,
            definitionGeneration: input.binding.definitionGeneration,
            definitionDigest: input.binding.definitionDigest,
          };
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                yield* automation.registerRecovery(
                  expected,
                  record.sessionId!,
                  channel.credential_identity!,
                );
                if (relay !== undefined)
                  yield* automation.registerDependency(
                    expected,
                    relay,
                    [
                      dependencyGrant(
                        record.sessionId!,
                        inheritedBirthFacts.records.has(json(record)),
                      ),
                    ],
                    false,
                  );
              }),
            )
            .pipe(
              Effect.mapError(unavailable),
              Effect.tap(() => (relay === undefined ? Effect.void : wakeDisables)),
            );
          const upstream = yield* decodeAcceptedGraph(
            yield* ask(record.sessionId, {
              id: `capture:${input.certificateId}:${record.id}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
              body: {
                type: "capture-automation",
                envelope: record.envelope,
                ...input,
                remaining: input.remaining - 1,
              },
            }),
          ).pipe(Effect.mapError(unavailable));
          if (upstream.records.some((node) => node.environmentId === here))
            return yield* denied(
              "A peer cannot supply this destination's local automation authority.",
            );
          for (const dependency of upstream.dependencies)
            if (
              dependency.certificateId !== input.certificateId ||
              dependency.destinationEnvironmentId !== input.audience ||
              dependency.resourceId !== input.binding.resourceId ||
              dependency.definitionGeneration !== input.binding.definitionGeneration ||
              dependency.definitionDigest !== input.binding.definitionDigest
            )
              return yield* denied(
                "The upstream disable dependency does not match this exact automation capture.",
              );
          for (const dependency of upstream.dependencies)
            yield* automation.registerRecovery(
              dependency,
              record.sessionId,
              channel.credential_identity,
            );
          children.push(upstream);
        } else {
          const link = yield* links
            .get(record.peer.environmentId)
            .pipe(Effect.mapError(unavailable));
          if (Option.isNone(link) || link.value.launchIdentity !== record.peer.generation)
            return yield* denied("The source link changed during automation capture.");
          const expiry = DateTime.toEpochMillis(link.value.expiresAt);
          expiresAt = expiresAt === null ? expiry : Math.min(expiresAt, expiry);
        }
      }
      for (const id of new Set(childIds))
        children.push(
          yield* captureRecord(
            yield* read(id),
            input,
            nextAncestors,
            relay,
            replacements,
            birthFacts,
          ),
        );
      const threadId =
        record.context !== undefined && record.context.type !== "task-stop"
          ? record.context.threadId
          : "localThreadId" in record
            ? record.localThreadId
            : null;
      if (threadId !== null) {
        const imported = yield* legacy(threadId);
        if (imported !== undefined) {
          yield* activeSession(imported.sessionId);
          const session = yield* authSessions
            .getById({ sessionId: AuthSessionId.make(imported.sessionId) })
            .pipe(Effect.mapError(unavailable));
          if (Option.isNone(session)) return yield* unavailable();
          sessionIds.add(imported.sessionId);
          legacyOwners.push({
            environmentId: here,
            threadId,
            sessionId: AuthSessionId.make(imported.sessionId),
            ceiling: imported.ceiling,
            expiresAt: DateTime.toEpochMillis(session.value.expiresAt),
          });
        }
      }
      for (const owner of legacyOwners)
        children.push(yield* captureLegacy(owner, input, relay, birthFacts));
      const graph = yield* mergeGraphs([
        ...children,
        {
          records: [{ environmentId: here, record, ceiling, expiresAt }],
          legacy: [],
          dependencies: [],
        },
      ]);
      return graph;
    });
  const captureAutomation: PeerAuthority["Service"]["captureAutomation"] = (
    scope,
    binding,
    action,
    limits,
  ) =>
    Effect.gen(function* () {
      const grantFence = yield* sql<{
        session_id: string;
        generation: string;
        grant_json: string;
      }>`SELECT session_id,generation,grant_json FROM peer_authority_sessions`.pipe(
        Effect.mapError(unavailable),
      );
      const targetThreadId =
        binding.kind === "spectrum" ? binding.callerThreadId : binding.threadId;
      const roots: ExecutionRecord[] = [];
      if (scope?.authority !== undefined) roots.push(yield* read(scope.authority.id));
      if (targetThreadId !== null) {
        const owner = yield* forThread(targetThreadId);
        if (owner !== undefined) roots.push(owner);
      }
      const retainedLegacy: Automation.AcceptedGraph["legacy"][number][] = [];
      let existingSchedule: ResourceBinding | undefined;
      if (binding.kind === "schedule") {
        const found =
          yield* sql`SELECT task_id FROM scheduled_tasks WHERE task_id=${binding.resourceId}`.pipe(
            Effect.mapError(unavailable),
          );
        if (found.length !== 0)
          existingSchedule = yield* loadResource({
            type: "schedule",
            taskId: ScheduledTaskId.make(binding.resourceId),
          });
        const previous = yield* automation.current("schedule", binding.resourceId);
        if (Option.isSome(previous)) {
          for (const id of previous.value.recordIds) roots.push(yield* read(id));
          retainedLegacy.push(
            ...previous.value.graph.legacy.filter((owner) => owner.environmentId === here),
          );
        } else if (
          existingSchedule?.type === "schedule" &&
          existingSchedule.linkOrigin !== undefined
        ) {
          const sessionId = AuthSessionId.make(existingSchedule.linkOrigin.sessionId);
          yield* activeSession(sessionId);
          const original = (yield* sql<{
            runtime_mode: PeerCeiling["runtimeMode"];
            interaction_mode: PeerCeiling["interactionMode"];
          }>`SELECT runtime_mode,interaction_mode FROM scheduled_tasks WHERE task_id=${binding.resourceId}`.pipe(
            Effect.mapError(unavailable),
          ))[0];
          const session = yield* authSessions
            .getById({ sessionId })
            .pipe(Effect.mapError(unavailable));
          if (original === undefined || Option.isNone(session)) return yield* unavailable();
          retainedLegacy.push({
            environmentId: here,
            threadId: null,
            taskId: ScheduledTaskId.make(binding.resourceId),
            sessionId,
            ceiling: {
              runtimeMode: original.runtime_mode,
              interactionMode: original.interaction_mode,
              resourceScope: "created-work",
              onward: false,
              capabilities: ["orchestration", "worktree", "pull-requests"],
            },
            expiresAt: DateTime.toEpochMillis(session.value.expiresAt),
          });
        }
      }
      const replacements = new Map<string, Automation.AutomationCertificate>();
      if (binding.kind === "spectrum" && binding.workflowGeneration > 0) {
        const current = yield* automation.current("spectrum", binding.resourceId);
        if (Option.isSome(current)) {
          if (current.value.binding.kind !== "spectrum")
            return yield* denied("The accepted automation is not a Spectrum.");
          const native = (yield* sql<{
            generation: number;
            status: string;
            certificate_id: string | null;
          }>`SELECT generation,json_extract(payload_json,'$.status') AS status,json_extract(payload_json,'$.state.automationCertificateId') AS certificate_id FROM fork_spectra WHERE thread_id=${binding.resourceId}`.pipe(
            Effect.mapError(unavailable),
          ))[0];
          if (
            native === undefined ||
            native.status === "active" ||
            native.generation + 1 !== binding.workflowGeneration ||
            native.certificate_id !== current.value.id
          )
            return yield* denied("The Spectrum changed before fresh reopen capture.");
          const previous = current.value;
          const priorBinding = previous.binding;
          if (
            priorBinding.kind !== "spectrum" ||
            previous.audience !== here ||
            priorBinding.workflowGeneration >= binding.workflowGeneration ||
            priorBinding.threadId !== binding.threadId ||
            priorBinding.projectId !== binding.projectId ||
            priorBinding.callerThreadId !== binding.callerThreadId ||
            requestDigest(priorBinding.memberThreadIds) !== requestDigest(binding.memberThreadIds)
          )
            return yield* denied("The Spectrum reopen changed its accepted ownership.");
          const pending = [previous];
          while (pending.length !== 0) {
            const prior = pending.pop()!;
            if (replacements.has(prior.id)) continue;
            if (replacements.size >= 256)
              return yield* denied("The Spectrum generation bound was exceeded.");
            if (
              prior.audience !== here ||
              prior.binding.kind !== "spectrum" ||
              prior.binding.resourceId !== binding.resourceId ||
              prior.binding.projectId !== binding.projectId ||
              prior.binding.workflowGeneration >= binding.workflowGeneration
            )
              return yield* denied("The Spectrum birth contributor belongs to another automation.");
            replacements.set(prior.id, prior);
            for (const node of prior.graph.records)
              if (
                node.environmentId === here &&
                "certificateId" in node.record &&
                !replacements.has(node.record.certificateId)
              ) {
                const ancestor = yield* automation.facts(node.record.certificateId);
                if (
                  ancestor.binding.kind === "spectrum" &&
                  ancestor.binding.resourceId === binding.resourceId
                )
                  pending.push(ancestor);
              }
          }
          for (const threadId of [binding.threadId, ...binding.memberThreadIds]) {
            const birth = yield* permanentForThread(threadId);
            if (birth !== undefined) roots.push(birth);
          }
        }
      }
      const id = `automation:${"id" in action ? action.id : action.nativeCommandId}`;
      const input = { certificateId: id, audience: here, binding, remaining: 16 };
      let ceiling = limits;
      const graphs: Automation.AcceptedGraph[] = [];
      for (const root of roots) {
        const live = yield* validate(root, undefined, undefined, "interactive", replacements);
        ceiling = intersect(ceiling, live);
        if (existingSchedule !== undefined)
          yield* checkResource(root, live, existingSchedule, undefined, emptyCoverage());
        if (
          targetThreadId !== null &&
          !("certificateId" in root && replacements.has(root.certificateId))
        )
          yield* checkResource(
            root,
            live,
            yield* loadResource({ type: "thread", threadId: targetThreadId }),
            undefined,
            emptyCoverage(),
          );
        graphs.push(yield* captureRecord(root, input, new Set(), undefined, replacements));
      }
      if (
        scope?.client?.linked === true &&
        roots.length === 0 &&
        !(binding.kind === "schedule" && scope.authority === undefined)
      )
        return yield* denied(
          "Imported authority cannot create autonomous execution without live provenance.",
        );
      const legacyOwners: Automation.AcceptedGraph["legacy"][number][] = [];
      if (
        binding.kind === "schedule" &&
        scope?.client?.linked === true &&
        scope.authority === undefined
      ) {
        yield* checkBinding(
          scope,
          existingSchedule === undefined
            ? Effect.succeed({ type: "environment" })
            : Effect.succeed(existingSchedule),
        );
        const sessionId = AuthSessionId.make(scope.client.sessionId);
        const session = yield* authSessions
          .getById({ sessionId })
          .pipe(Effect.mapError(unavailable));
        if (Option.isNone(session)) return yield* unavailable();
        const direct =
          scope.thread === undefined &&
          scope.client.access === "full-access" &&
          Invocation.clientModeCeiling(scope.client).runtimeMode === "full-access" &&
          Invocation.clientModeCeiling(scope.client).interactionMode === "default" &&
          (existingSchedule === undefined ||
            (existingSchedule.type === "schedule" && existingSchedule.linkOrigin === undefined));
        legacyOwners.push({
          environmentId: here,
          threadId: null,
          taskId: ScheduledTaskId.make(binding.resourceId),
          sessionId,
          ceiling: {
            ...limits,
            resourceScope: direct ? "environment-resources" : "created-work",
            onward: (yield* grant(sessionId)).onward,
            capabilities: limits.capabilities.filter(
              (capability) => capability !== "preview" && capability !== "device",
            ),
          },
          expiresAt: DateTime.toEpochMillis(session.value.expiresAt),
        });
      }
      for (const owner of retainedLegacy) {
        yield* activeSession(owner.sessionId);
        ceiling = intersect(ceiling, owner.ceiling);
        if (owner.expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* denied("An original schedule contributor expired.");
        if (existingSchedule !== undefined && owner.ceiling.resourceScope === "created-work")
          yield* checkCreated(owner.sessionId, existingSchedule);
        legacyOwners.push(owner);
      }
      for (const threadId of new Set([
        ...(targetThreadId === null ? [] : [targetThreadId]),
        ...(scope?.thread === undefined ? [] : [scope.thread.threadId]),
      ])) {
        const imported = yield* legacy(threadId);
        if (imported === undefined) continue;
        yield* activeSession(imported.sessionId);
        const session = yield* authSessions
          .getById({ sessionId: AuthSessionId.make(imported.sessionId) })
          .pipe(Effect.mapError(unavailable));
        if (Option.isNone(session)) return yield* unavailable();
        if (targetThreadId !== null)
          yield* checkCreated(
            imported.sessionId,
            yield* loadResource({ type: "thread", threadId: targetThreadId }),
          );
        ceiling = intersect(ceiling, imported.ceiling);
        legacyOwners.push({
          environmentId: here,
          threadId,
          sessionId: AuthSessionId.make(imported.sessionId),
          ceiling: imported.ceiling,
          expiresAt: DateTime.toEpochMillis(session.value.expiresAt),
        });
      }
      for (const owner of legacyOwners) graphs.push(yield* captureLegacy(owner, input));
      const graph = yield* mergeGraphs(graphs);
      if (
        graph.records.length === 0 &&
        graph.legacy.length === 0 &&
        graph.dependencies.length === 0
      )
        return undefined;
      for (const node of graph.records) ceiling = intersect(ceiling, node.ceiling);
      for (const owner of graph.legacy) ceiling = intersect(ceiling, owner.ceiling);
      if (
        ceiling.runtimeMode !== limits.runtimeMode ||
        ceiling.interactionMode !== limits.interactionMode
      )
        return yield* denied(
          "The automation definition exceeds an original contributor's current mode ceiling.",
        );
      const expiries = [
        ...graph.records.flatMap((node) => (node.expiresAt === null ? [] : [node.expiresAt])),
        ...graph.legacy.map((node) => node.expiresAt),
      ];
      const localSessions = new Set<string>();
      for (const node of graph.records) {
        if (node.environmentId !== here) continue;
        if ("envelope" in node.record) {
          for (const session of [
            node.record.sessionId,
            node.record.originClientId,
            node.record.requestClientSessionId,
            node.record.resourceSessionId,
          ])
            if (session !== null && session !== undefined) localSessions.add(session);
        } else if (
          "resourceSessionId" in node.record &&
          node.record.resourceSessionId !== undefined
        )
          localSessions.add(node.record.resourceSessionId);
      }
      for (const owner of graph.legacy)
        if (owner.environmentId === here) localSessions.add(owner.sessionId);
      return Automation.ValidatedAcceptance.afterLiveCapture(
        {
          version: 1,
          id,
          audience: here,
          binding,
          recordIds: [...new Set(roots.map((root) => root.id))],
          graph,
          ceiling,
          ...(scope === undefined ? {} : { namespace: requestNamespace(scope) }),
          generation: "generation" in action ? action.generation : "local",
          acceptedAt: yield* Clock.currentTimeMillis,
          expiresAt: expiries.length === 0 ? null : Math.min(...expiries),
        },
        grantFence.filter((row) => localSessions.has(row.session_id)),
      );
    });
  const process: PeerAuthority["Service"]["process"] = (peer, generation, frame) =>
    Effect.gen(function* () {
      if (frame.body.type === "capture-legacy-automation") {
        const input = frame.body;
        if (input.contributor.environmentId !== peer)
          return yield* denied("The legacy contributor belongs to another destination.");
        const link = yield* links.get(peer).pipe(Effect.mapError(unavailable));
        const now = yield* Clock.currentTimeMillis;
        if (
          Option.isNone(link) ||
          link.value.launchIdentity !== generation ||
          DateTime.toEpochMillis(link.value.expiresAt) <= now ||
          input.contributor.expiresAt <= now
        )
          return yield* denied("The original legacy source generation retired before capture.");
        if (
          link.value.access === "read-only" ||
          Invocation.intersectModes(
            { runtimeMode: link.value.access, interactionMode: "default" },
            input.contributor.ceiling,
          ).runtimeMode !== input.contributor.ceiling.runtimeMode
        )
          return yield* denied(
            "The legacy contributor exceeds the original source link's current access.",
          );
        const expiresAt = Math.min(
          input.contributor.expiresAt,
          DateTime.toEpochMillis(link.value.expiresAt),
        );
        const dependency: Automation.AutomationDependency = {
          sourceEnvironmentId: here,
          destinationEnvironmentId: input.audience,
          certificateId: input.certificateId,
          recordId: input.contributor.sessionId,
          grantGeneration: generation,
          resourceId: input.binding.resourceId,
          definitionGeneration: input.binding.definitionGeneration,
          definitionDigest: input.binding.definitionDigest,
        };
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const current = yield* links.get(peer).pipe(Effect.mapError(unavailable));
              if (
                Option.isNone(current) ||
                current.value.launchIdentity !== generation ||
                DateTime.toEpochMillis(current.value.expiresAt) <= (yield* Clock.currentTimeMillis)
              )
                return yield* denied(
                  "The legacy source link retired before dependency registration.",
                );
              yield* automation.registerDependency(
                dependency,
                { environmentId: peer, generation },
                [],
                true,
                expiresAt,
              );
            }),
          )
          .pipe(
            Effect.catchTags({ SqlError: () => Effect.fail(unavailable()) }),
            Effect.tap(() => wakeDisables),
          );
        return { dependency, expiresAt };
      }
      if (frame.body.type === "capture-automation") {
        const { certificateId, audience, binding, remaining, envelope } = frame.body;
        const input = { certificateId, audience, binding, remaining };
        const record = yield* readInteractive(envelope.recordId);
        if (
          record.sessionId !== null ||
          record.peer.environmentId !== peer ||
          record.peer.generation !== generation ||
          json(record.envelope) !== json(envelope)
        )
          return yield* denied("The automation capture belongs to another action or link.");
        const next = { environmentId: peer, generation };
        const graph = yield* captureRecord(record, input, new Set(), next);
        const dependency: Automation.AutomationDependency = {
          sourceEnvironmentId: here,
          destinationEnvironmentId: input.audience,
          certificateId: input.certificateId,
          recordId: record.id,
          grantGeneration: generation,
          resourceId: input.binding.resourceId,
          definitionGeneration: input.binding.definitionGeneration,
          definitionDigest: input.binding.definitionDigest,
        };
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              let birthFacts = noBirthFacts();
              for (const node of graph.records)
                if (
                  node.environmentId === here &&
                  "certificateId" in node.record &&
                  node.record.kind === "automation-birth"
                )
                  birthFacts = addBirthFacts(
                    birthFacts,
                    yield* automation.birth(node.record.certificateId),
                  );
              const sessionIds = new Map<string, boolean>();
              const includeSession = (sessionId: string, covered: boolean) =>
                sessionIds.set(sessionId, (sessionIds.get(sessionId) ?? true) && covered);
              for (const node of graph.records) {
                if (node.environmentId !== here || !("envelope" in node.record)) continue;
                for (const sessionId of [
                  node.record.sessionId,
                  node.record.originClientId,
                  node.record.requestClientSessionId,
                  node.record.resourceSessionId,
                ]) {
                  if (sessionId === null || sessionId === undefined) continue;
                  yield* activeSession(sessionId);
                  includeSession(sessionId, birthFacts.records.has(json(node.record)));
                }
                if (node.record.sessionId === null) {
                  const link = yield* links
                    .get(node.record.peer.environmentId)
                    .pipe(Effect.mapError(unavailable));
                  if (
                    Option.isNone(link) ||
                    link.value.launchIdentity !== node.record.peer.generation ||
                    DateTime.toEpochMillis(link.value.expiresAt) <= (yield* Clock.currentTimeMillis)
                  )
                    return yield* denied("The source link retired before capture registration.");
                }
              }
              for (const node of graph.legacy)
                if (node.environmentId === here) {
                  yield* activeSession(node.sessionId);
                  includeSession(node.sessionId, birthFacts.legacy.has(json(node)));
                }
              const expiries = [
                ...graph.records.flatMap((node) =>
                  node.expiresAt === null ? [] : [node.expiresAt],
                ),
                ...graph.legacy.map((node) => node.expiresAt),
              ];
              const certificates = graph.records.flatMap((node) =>
                node.environmentId === here && "certificateId" in node.record ? [node.record] : [],
              );
              const grants = [...sessionIds].map(([sessionId, covered]) =>
                dependencyGrant(sessionId, covered),
              );
              yield* automation.registerDependency(
                dependency,
                next,
                grants,
                true,
                expiries.length === 0 ? null : Math.min(...expiries),
                certificates,
              );
              for (const upstream of graph.dependencies)
                yield* automation.registerDependency(
                  upstream,
                  next,
                  grants,
                  false,
                  undefined,
                  certificates,
                );
            }),
          )
          .pipe(
            Effect.catchTags({ SqlError: () => Effect.fail(unavailable()) }),
            Effect.tap(() => wakeDisables),
          );
        return yield* mergeGraphs([graph, { records: [], legacy: [], dependencies: [dependency] }]);
      }
      if (frame.body.type === "secret-origin") {
        const record = yield* readInteractive(frame.body.envelope.recordId);
        if (
          record.sessionId !== null ||
          record.peer.environmentId !== peer ||
          record.peer.generation !== generation ||
          json(record.envelope) !== json(frame.body.envelope)
        )
          return yield* denied("The secret request belongs to another action or link.");
        yield* validate(record, undefined, undefined, "interactive");
        if (
          record.localThreadId === null ||
          record.originRunId === null ||
          record.envelope.requestOwner === undefined
        )
          return yield* denied("The secret request needs its original live run.");
        return {
          threadId: record.localThreadId,
          runId: record.originRunId,
          ...record.envelope.requestOwner,
        };
      }
      if (frame.body.type === "request-actor") {
        const record = yield* readInteractive(frame.body.envelope.recordId);
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
        const record = yield* readInteractive(rows[0].record_id);
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
        const record = yield* readInteractive(frame.body.envelope.recordId);
        if (record.peer.environmentId !== peer || record.peer.generation !== generation)
          return yield* denied("Pair registration belongs to another link.");
        if (json(record.envelope) !== json(frame.body.envelope))
          return yield* denied("Pair registration belongs to another action.");
        yield* validate(record, undefined, undefined, "interactive");
        yield* registerPair(frame.body.envelope, frame.body.pair);
        return { registered: true };
      }
      if (frame.body.type === "validate") {
        const envelope = frame.body.envelope;
        const record = yield* readInteractive(envelope.recordId);
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
            inheritedRecord === undefined
              ? undefined
              : yield* validate(inheritedRecord, undefined, undefined, "interactive");
          let ceiling = yield* validate(
            record,
            {
              ...control,
              ...(upstream === undefined ? {} : { upstream }),
            },
            undefined,
            "interactive",
          );
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
            const live = yield* validate(owner, upstream, undefined, "interactive");
            yield* checkResource(owner, live, { type: "thread", thread: parent }, upstream);
            ceiling = intersect(ceiling, live);
          }
          return ceiling;
        }
        return yield* validate(record, undefined, undefined, "interactive");
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
      const record = yield* readInteractive(rows[0].record_id);
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
      const rows = yield* sql<{
        record_id: string;
        pair_json: string;
      }>`SELECT record_id,pair_json FROM peer_return_pairs WHERE json_extract(pair_json,'$.childEnvironmentId')=${here} AND json_extract(pair_json,'$.childThreadId')=${scope.thread.threadId} AND json_extract(pair_json,'$.parentEnvironmentId')=${environmentId} AND json_extract(pair_json,'$.parentThreadId')=${input.threadId}`.pipe(
        Effect.mapError(unavailable),
      );
      if (rows.length !== 1) return undefined;
      const record = yield* readInteractive(rows[0]!.record_id);
      if (record.sessionId === null || record.peer.environmentId !== environmentId)
        return undefined;
      if (input.mode !== undefined && input.mode !== "auto" && input.mode !== "queue")
        return yield* denied("A child may only queue a notice to its parent.");
      const pair = yield* decodePair(rows[0]!.pair_json).pipe(Effect.mapError(unavailable));
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
      const record = yield* readInteractive(rows[0].record_id);
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
      const coverage = record === undefined ? undefined : yield* coverageFor(record);
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
          ? yield* validate(record, undefined, coverage)
          : migrated !== undefined
            ? migrated.ceiling
            : yield* unavailable();
      const target = yield* threads.getThreadShell(threadId).pipe(Effect.mapError(unavailable));
      if (target === null || target.deletedAt !== null)
        return yield* denied("The destination thread is no longer available.");
      if (record !== undefined)
        yield* checkResource(
          record,
          ceiling,
          { type: "thread", thread: target },
          undefined,
          coverage,
        );
      let effective = migrated === undefined ? ceiling : intersect(ceiling, migrated.ceiling);
      if (created !== undefined && created.id !== record?.id) {
        const creationCeiling = yield* validate(
          created,
          record?.context?.type === "task-stop" ? record.context : undefined,
          coverage,
        );
        yield* checkResource(
          created,
          creationCeiling,
          { type: "thread", thread: target },
          record?.context?.type === "task-stop" ? record.context : undefined,
          coverage,
        );
        effective = intersect(effective, creationCeiling);
      }
      if (exceededDispatchModeLimit(effective, target) !== undefined)
        return yield* denied(
          "The destination effect exceeds the current originating authority ceiling.",
        );
      if (record === undefined) return undefined;
      let owner = record;
      if (created !== undefined && created.id !== record.id) {
        const context = {
          type: "owned-thread" as const,
          threadId,
          creationRecordId: created.id,
          runRecordId: record.id,
        };
        owner = conjunction(`context:${yield* hash(context)}`, context, [created, record]);
        yield* save(owner);
      } else if (migrated !== undefined && record.context?.type !== "legacy-thread") {
        const context = { type: "legacy-thread" as const, threadId, runRecordId: record.id };
        owner = conjunction(
          `context:${yield* hash(context)}`,
          context,
          [record],
          AuthSessionId.make(migrated.sessionId),
        );
        yield* save(owner);
      }
      return { ...owner, ceiling: effective };
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
        (record === undefined || ("envelope" in record && record.envelope.descendant !== true)) &&
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
    intent,
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
          !Invocation.isLiveCallerShell(caller, scope.thread.providerInstanceId)
        )
          return yield* denied("The original caller no longer owns a live run.");
        modes = Invocation.intersectModes(caller, modes);
      } else modes = Invocation.intersectModes(Invocation.clientModeCeiling(scope.client), modes);
      if (exceededDispatchModeLimit(modes, target.thread) !== undefined)
        return yield* denied(
          intent === "thread-resource"
            ? "The resource action exceeds current originating or destination modes."
            : "The device or browser action exceeds current originating or destination modes.",
        );
      const capabilities = new Set(
        (yield* currentCapabilities([...scope.capabilities], target.thread.projectId)).filter(
          (name) => ceiling === undefined || ceiling.capabilities.includes(name),
        ),
      );
      if (intent !== "thread-resource" && !capabilities.has(intent))
        return yield* denied("The original invocation no longer has the required capability.");
      return {
        ...scope,
        capabilities,
        ...(record === undefined || ceiling === undefined
          ? {}
          : { authority: { ...record, ceiling } }),
      };
    });
  const executionRecord = (
    certificate: Automation.AutomationCertificate,
    actionId: string,
  ): ExecutionRecord => ({
    kind: "automation-execution",
    id: `automation-execution:${certificate.id}:${actionId}`,
    certificateId: certificate.id,
    actionId,
    ceiling: certificate.ceiling,
    generation: certificate.generation,
    projectId: certificate.binding.projectId,
  });
  const nativePreflight: NativeAuthorityValidation["Service"]["preflight"] = (
    ownerIds,
    threadIds,
  ) =>
    Effect.gen(function* () {
      const grants = () =>
        sql`SELECT session_id,generation,grant_json FROM peer_authority_sessions ORDER BY session_id`.pipe(
          Effect.mapError(unavailable),
        );
      const expected = requestDigest(yield* grants());
      const roots: ExecutionRecord[] = [];
      for (const id of ownerIds) {
        const rows = yield* sql<{
          record_json: string;
        }>`SELECT record_json FROM peer_authority_records WHERE id=${id}`.pipe(
          Effect.mapError(unavailable),
        );
        // New fork-plan owners are persisted in the atomic commit and checked there.
        if (rows[0] !== undefined)
          roots.push(
            yield* decodeAuthority(rows[0].record_json).pipe(Effect.mapError(unavailable)),
          );
      }
      const pending = [...roots];
      const seen = new Set<string>();
      const births = new Set<string>();
      while (pending.length !== 0) {
        const record = pending.pop()!;
        if (seen.has(record.id)) continue;
        seen.add(record.id);
        if (seen.size > 256)
          return yield* denied("The native birth contributor bound was exceeded.");
        if ("certificateId" in record) {
          if (record.kind === "automation-birth") births.add(record.id);
          continue;
        }
        const context = record.context;
        if (context === undefined || context.type === "task-stop") continue;
        for (const id of context.type === "run-inputs"
          ? context.recordIds
          : context.type === "owned-thread"
            ? [context.creationRecordId, context.runRecordId]
            : [context.runRecordId])
          pending.push(yield* read(id));
      }
      if (births.size !== 0) {
        const covered = new Map<string, Automation.AcceptedGraph["records"][number]>();
        const executions = new Set<string>();
        for (const root of roots) {
          const coverage = yield* coverageFor(root);
          for (const [id, node] of coverage) covered.set(id, node);
          for (const id of coverage.executions) executions.add(id);
        }
        const coverage = Object.assign(covered, { executions });
        for (const root of roots) {
          const ceiling = yield* validate(root, undefined, coverage);
          for (const threadId of threadIds) {
            const thread = yield* threads
              .getThreadShell(threadId)
              .pipe(Effect.mapError(unavailable));
            if (thread !== null) {
              yield* checkResource(root, ceiling, { type: "thread", thread }, undefined, coverage);
              if (exceededDispatchModeLimit(ceiling, thread) !== undefined)
                return yield* denied("The native follow-up exceeds current destination modes.");
            }
          }
        }
      }
      return {
        births,
        check: Effect.gen(function* () {
          if (births.size !== 0 && requestDigest(yield* grants()) !== expected)
            return yield* denied("An original contributor changed before native commit.");
        }),
      };
    });
  return PeerAuthority.of({
    prepareAttachmentRead,
    recheckAttachmentRead,
    prepareDeviceViewer: prepareViewer,
    recheckDeviceViewer: recheckViewer,
    preparePreviewViewer: prepareViewer,
    recheckPreviewViewer: recheckViewer,
    nativePreflight,
    notifyAutomationChanges: wakeDisables,
    nextAutomationDisableDeadline: Effect.gen(function* () {
      const automationDeadline = yield* automation.nextDisableDeadline;
      yield* RemoteSecretPersistence.schema.pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(unavailable),
      );
      const [secret] = yield* sql<{
        deadline: number | null;
      }>`SELECT min(min(r.expires_at,coalesce(round(unixepoch(p.expires_at,'subsec')*1000),r.expires_at))) AS deadline FROM remote_secret_obligations r LEFT JOIN peer_environment_links p ON p.environment_id=r.environment_id WHERE r.pending=0 AND r.ack_json IS NULL`.pipe(
        Effect.mapError(unavailable),
      );
      const deadlines = [automationDeadline, secret?.deadline ?? null].filter(
        (value): value is number => value !== null,
      );
      return deadlines.length === 0 ? null : Math.min(...deadlines);
    }),
    pendingAutomationDisables: automation.pending,
    acknowledgeAutomationDisable: (acknowledgement, peer, generation) =>
      automation
        .acknowledge(acknowledgement, peer, generation)
        .pipe(Effect.tap(() => wakeDisables)),
    receiveAutomationDisable: (input, identity) =>
      automation.receiveDisable(input, identity).pipe(Effect.tap(() => wakeDisables)),
    captureAutomation,
    automationAdmission: (value, binding, actionId) =>
      Effect.sync(() => {
        const record = executionRecord(value.certificate, actionId);
        return {
          record,
          commit: automation.accept(value, binding).pipe(Effect.andThen(save(record))),
        };
      }),
    automationExecution: (certificateId, actionId) =>
      Effect.gen(function* () {
        const certificate = yield* automation.claim(certificateId);
        const record = executionRecord(certificate, actionId);
        yield* save(record);
        yield* validate(record);
        return record;
      }),
    recheckInvocation,
    pendingWithinTask: (resource, target) => ancestor({ type: "thread", ...resource }, target),
    checkRecord: (id) => Effect.flatMap(read(id), validate).pipe(Effect.asVoid),
    checkInteractiveRecord: (id) =>
      Effect.flatMap(read(id), (record) =>
        validate(record, undefined, undefined, "interactive"),
      ).pipe(Effect.asVoid),
    authorizeTaskStop: (scope, input) =>
      Effect.gen(function* () {
        yield* verifyTask(scope, input);
        const rows = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_return_pairs WHERE task_id=${input.pair.taskId}`.pipe(
          Effect.mapError(unavailable),
        );
        if (!rows[0]) return yield* denied("Unknown stop pair.");
        const original = yield* readInteractive(rows[0].record_id);
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
    secretOrigin: (recordId) =>
      Effect.gen(function* () {
        const record = yield* readInteractive(recordId);
        if (record.sessionId !== null)
          return yield* decodeSecretOrigin(
            yield* ask(record.sessionId, {
              id: `secret-origin:${record.id}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
              body: { type: "secret-origin", envelope: record.envelope },
            }),
          ).pipe(Effect.mapError(unavailable));
        yield* validate(record, undefined, undefined, "interactive");
        if (
          record.localThreadId === null ||
          record.originRunId === null ||
          record.envelope.requestOwner === undefined
        )
          return yield* denied("The secret request needs its original live run.");
        return {
          threadId: record.localThreadId,
          runId: record.originRunId,
          ...record.envelope.requestOwner,
        };
      }),
    secretClient: (recordId) =>
      Effect.gen(function* () {
        const actor = yield* actorForRecord(yield* readInteractive(recordId));
        if (actor.type !== "client")
          return yield* denied("Only an authenticated human client can answer a secret request.");
      }),
    prepareSecretAnswer: (sessionId, peer, generation, input) =>
      Effect.gen(function* () {
        yield* requireClient(sessionId, false);
        const client = yield* authSessions
          .getById({ sessionId })
          .pipe(Effect.mapError(unavailable));
        if (Option.isNone(client))
          return yield* denied("The original human client is unavailable.");
        yield* rejectMcpClientAudience(client.value).pipe(
          Effect.mapError(() => denied("An MCP agent cannot claim a human secret answer.")),
        );
        return yield* prepare(
          {
            environmentId: here,
            thread: undefined,
            issuedAt: yield* Clock.currentTimeMillis,
            requestNamespace: `client:${here}:${sessionId}`,
            client: { sessionId, label: "Secret request client", access: "full-access" },
            capabilities: new Set(["orchestration"]),
          },
          peer,
          generation,
          "remote_secret",
          remoteSecretIntent(input),
          { runtimeMode: "full-access", interactionMode: "default" },
          undefined,
          false,
          true,
        ).pipe(Effect.provideService(PendingClientSession, { sessionId, readOnly: false }));
      }),
    bind,
    forThread: (threadId) =>
      Effect.gen(function* () {
        const record = yield* forThread(threadId);
        return record !== undefined && !("envelope" in record)
          ? yield* checkThread(threadId)
          : record;
      }),
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
          taskId: receipt.scheduledTaskId,
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
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* activeSession(input.sessionId);
            const encoded = yield* encodeGrant(input).pipe(Effect.mapError(unavailable));
            const previous = yield* grant(input.sessionId);
            const priorEncoded = yield* encodeGrant(previous).pipe(Effect.mapError(unavailable));
            if (priorEncoded !== encoded)
              yield* Automation.invalidateAutomationSessions([input.sessionId], "retired").pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.mapError(unavailable),
              );
            const rows =
              yield* sql`UPDATE peer_authority_sessions SET grant_json=${encoded} WHERE session_id=${input.sessionId} RETURNING session_id`.pipe(
                Effect.mapError(unavailable),
              );
            if (rows.length !== 1)
              return yield* denied("This session has no authenticated peer link.");
            return input;
          }),
        )
        .pipe(
          Effect.catchTags({ SqlError: () => Effect.fail(unavailable()) }),
          Effect.tap(() => wakeDisables),
        ),
  });
});
export const layer = Layer.effect(
  NativeAuthorityValidation,
  Effect.map(PeerAuthority, (authority) => ({ preflight: authority.nativePreflight })),
).pipe(
  Layer.provideMerge(
    Layer.effect(PeerAuthority, make).pipe(
      Layer.provide(AuthSessions.layer),
      Layer.provide(Automation.layer),
    ),
  ),
);
