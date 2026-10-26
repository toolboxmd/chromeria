import * as RemoteSecretPersistence from "../secrets/remoteSecretPersistence.ts";
import * as DateTime from "effect/DateTime";
import {
  AuthSessionId,
  EnvironmentId,
  OrchestratorMcpFailure,
  PeerCeiling,
  ProjectId,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { requestDigest } from "../mcp/McpRequestReceipts.ts";
import { ExecutionRecord } from "../peer/PeerAuthorityRecord.ts";

const bindingFields = {
  resourceId: Schema.String,
  projectId: ProjectId,
  definitionGeneration: Schema.String,
  definitionDigest: Schema.String,
  incarnation: Schema.NullOr(Schema.String),
};
export const AutomationBinding = Schema.Union([
  Schema.Struct({
    ...bindingFields,
    kind: Schema.Literal("schedule"),
    threadId: Schema.NullOr(ThreadId),
  }),
  Schema.Struct({
    ...bindingFields,
    kind: Schema.Literal("spectrum"),
    threadId: ThreadId,
    callerThreadId: ThreadId,
    memberThreadIds: Schema.NonEmptyArray(ThreadId).check(Schema.isMaxLength(9)),
    workflowGeneration: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
]);
export type AutomationBinding = typeof AutomationBinding.Type;

export const AutomationDependency = Schema.Struct({
  sourceEnvironmentId: EnvironmentId,
  destinationEnvironmentId: EnvironmentId,
  certificateId: Schema.String,
  recordId: Schema.String,
  grantGeneration: Schema.String,
  resourceId: Schema.String,
  definitionGeneration: Schema.String,
  definitionDigest: Schema.String,
});
export type AutomationDependency = typeof AutomationDependency.Type;

const legacyFields = {
  environmentId: EnvironmentId,
  sessionId: AuthSessionId,
  ceiling: PeerCeiling,
  expiresAt: Schema.Number,
};
export const LegacyContributor = Schema.Union([
  Schema.Struct({ ...legacyFields, threadId: ThreadId }),
  Schema.Struct({ ...legacyFields, threadId: Schema.Null, taskId: ScheduledTaskId }),
]);
export type LegacyContributor = typeof LegacyContributor.Type;

export const AcceptedGraph = Schema.Struct({
  records: Schema.Array(
    Schema.Struct({
      environmentId: EnvironmentId,
      record: ExecutionRecord,
      ceiling: PeerCeiling,
      expiresAt: Schema.NullOr(Schema.Number),
    }),
  ).check(Schema.isMaxLength(256)),
  legacy: Schema.Array(LegacyContributor).check(Schema.isMaxLength(256)),
  dependencies: Schema.Array(AutomationDependency).check(Schema.isMaxLength(256)),
});
export type AcceptedGraph = typeof AcceptedGraph.Type;

export const AutomationCertificate = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  audience: EnvironmentId,
  binding: AutomationBinding,
  recordIds: Schema.Array(Schema.String).check(Schema.isMaxLength(256)),
  graph: AcceptedGraph,
  ceiling: PeerCeiling,
  namespace: Schema.optionalKey(Schema.String),
  generation: Schema.String,
  acceptedAt: Schema.Number,
  expiresAt: Schema.NullOr(Schema.Number),
});
export type AutomationCertificate = typeof AutomationCertificate.Type;

/** Constructed only by live capture; a failed admission cannot be reused for another save. */
export class ValidatedAcceptance {
  private consumed = false;
  readonly certificate: AutomationCertificate;
  readonly grantFence: ReadonlyArray<{
    readonly session_id: string;
    readonly generation: string;
    readonly grant_json: string;
  }>;
  private constructor(
    certificate: AutomationCertificate,
    grantFence: ValidatedAcceptance["grantFence"],
  ) {
    this.certificate = certificate;
    this.grantFence = grantFence;
  }

  static afterLiveCapture(
    certificate: AutomationCertificate,
    grantFence: ValidatedAcceptance["grantFence"],
  ) {
    return new ValidatedAcceptance(certificate, grantFence);
  }

  consume() {
    if (this.consumed) return false;
    this.consumed = true;
    return true;
  }
}

export class CurrentAutomationAcceptance extends Context.Service<
  CurrentAutomationAcceptance,
  ValidatedAcceptance
>()("t3/scheduledTasks/DestinationAutomationAuthority/CurrentAutomationAcceptance") {}

export const AutomationDisable = Schema.Struct({
  version: Schema.Literal(1),
  dependency: AutomationDependency,
});
export type AutomationDisable = typeof AutomationDisable.Type;
export const AutomationDisableAck = Schema.Struct({
  version: Schema.Literal(1),
  dependency: AutomationDependency,
  disabled: Schema.Literal(true),
});
export type AutomationDisableAck = typeof AutomationDisableAck.Type;

const denied = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });
const unavailable = (_cause: unknown) =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Destination automation authority storage is unavailable.",
  });
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeExecutionRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(ExecutionRecord));
const decodeCertificate = Schema.decodeUnknownEffect(Schema.fromJsonString(AutomationCertificate));
const decodeDependency = Schema.decodeUnknownEffect(Schema.fromJsonString(AutomationDependency));
const decodeAck = Schema.decodeUnknownEffect(Schema.fromJsonString(AutomationDisableAck));

type CertificateContributor = Pick<
  Extract<typeof ExecutionRecord.Type, { readonly certificateId: string }>,
  "kind" | "certificateId"
>;

export const ensureAutomationAuthoritySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_certificates (id TEXT PRIMARY KEY, certificate_json TEXT NOT NULL, disabled INTEGER NOT NULL DEFAULT 0)`;
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(fork_automation_certificates)`).some(
      (column) => column.name === "revoked",
    )
  ) {
    yield* sql`ALTER TABLE fork_automation_certificates ADD COLUMN revoked INTEGER NOT NULL DEFAULT 0`;
    // Historical disabled rows have no trustworthy reason; preserve their denial.
    yield* sql`UPDATE fork_automation_certificates SET revoked=disabled`;
  }
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_bindings (kind TEXT NOT NULL,resource_id TEXT NOT NULL,certificate_id TEXT NOT NULL,PRIMARY KEY(kind,resource_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_recovery (dependency_id TEXT PRIMARY KEY,dependency_json TEXT NOT NULL,session_id TEXT NOT NULL,credential_identity TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_disable_acks (dependency_id TEXT PRIMARY KEY,certificate_id TEXT NOT NULL,ack_json TEXT NOT NULL)`;
  yield* sql`CREATE INDEX IF NOT EXISTS fork_automation_disable_by_certificate ON fork_automation_disable_acks(certificate_id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_obligations (dependency_id TEXT PRIMARY KEY,dependency_json TEXT NOT NULL,next_environment_id TEXT NOT NULL,next_generation TEXT NOT NULL,originating INTEGER NOT NULL,pending INTEGER NOT NULL DEFAULT 0,ack_json TEXT)`;
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(fork_automation_obligations)`).some(
      (column) => column.name === "expires_at",
    )
  )
    yield* sql`ALTER TABLE fork_automation_obligations ADD COLUMN expires_at REAL`;
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(fork_automation_obligations)`).some(
      (column) => column.name === "selection_order",
    )
  )
    yield* sql`ALTER TABLE fork_automation_obligations ADD COLUMN selection_order INTEGER NOT NULL DEFAULT 0`;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_dependency_grants (dependency_id TEXT NOT NULL,session_id TEXT NOT NULL,PRIMARY KEY(dependency_id,session_id))`;
  if (
    !(yield* sql<{ name: string }>`PRAGMA table_info(fork_automation_dependency_grants)`).some(
      (column) => column.name === "grant_kind",
    )
  )
    yield* sql`ALTER TABLE fork_automation_dependency_grants ADD COLUMN grant_kind TEXT NOT NULL DEFAULT 'automation-execution'`;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_automation_dependency_certificates (dependency_id TEXT NOT NULL,certificate_id TEXT NOT NULL,PRIMARY KEY(dependency_id,certificate_id))`;
  if (
    !(yield* sql<{
      name: string;
    }>`PRAGMA table_info(fork_automation_dependency_certificates)`).some(
      (column) => column.name === "certificate_kind",
    )
  )
    yield* sql`ALTER TABLE fork_automation_dependency_certificates ADD COLUMN certificate_kind TEXT NOT NULL DEFAULT 'automation-execution'`;
});

/** Persist downstream obligations before retiring an exact local generation. */
const invalidateAutomationCertificates = Effect.fn(
  "DestinationAutomationAuthority.invalidateAutomationCertificates",
)(function* (certificateIds: ReadonlyArray<string>, reason: "retired" | "revoked") {
  if (certificateIds.length === 0) return;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND dependency_id IN (SELECT dependency_id FROM fork_automation_dependency_certificates WHERE ${sql.in("certificate_id", certificateIds)} AND (${reason}='revoked' OR certificate_kind='automation-execution'))`;
  yield* sql`UPDATE fork_automation_certificates SET disabled=1,revoked=CASE WHEN ${reason}='revoked' THEN 1 ELSE revoked END WHERE ${sql.in("id", certificateIds)}`;
});

/** SQL-only: call in the revocation owner's transaction, before invalidating credentials. */
export const invalidateAutomationSessions = Effect.fn(
  "DestinationAutomationAuthority.invalidateAutomationSessions",
)(function* (sessionIds: ReadonlyArray<string>, reason: "retired" | "revoked" = "revoked") {
  if (sessionIds.length === 0) return;
  const sql = yield* SqlClient.SqlClient;
  yield* ensureAutomationAuthoritySchema;
  yield* RemoteSecretPersistence.invalidateSessions(sessionIds);
  yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND dependency_id IN (SELECT dependency_id FROM fork_automation_dependency_grants WHERE ${sql.in("session_id", sessionIds)} AND (${reason}='revoked' OR grant_kind='automation-execution'))`;
  const affected = yield* sql<{
    id: string;
  }>`SELECT id FROM fork_automation_certificates WHERE id IN (SELECT json_extract(dependency_json,'$.certificateId') FROM fork_automation_recovery WHERE ${sql.in("session_id", sessionIds)}) OR EXISTS (SELECT 1 FROM json_each(certificate_json,'$.graph.records') node JOIN json_each(${json(sessionIds)}) requested WHERE json_extract(node.value,'$.environmentId')=json_extract(certificate_json,'$.audience') AND requested.value IN (json_extract(node.value,'$.record.sessionId'),json_extract(node.value,'$.record.originClientId'),json_extract(node.value,'$.record.requestClientSessionId'),json_extract(node.value,'$.record.resourceSessionId'))) OR EXISTS (SELECT 1 FROM json_each(certificate_json,'$.graph.legacy') node JOIN json_each(${json(sessionIds)}) requested WHERE json_extract(node.value,'$.environmentId')=json_extract(certificate_json,'$.audience') AND json_extract(node.value,'$.sessionId')=requested.value)`;
  yield* invalidateAutomationCertificates(
    affected.map((row) => row.id),
    reason,
  );
});

export const invalidateAutomationLink = Effect.fn(
  "DestinationAutomationAuthority.invalidateAutomationLink",
)(function* (environmentId: string, generation: string) {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureAutomationAuthoritySchema;
  yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND next_environment_id=${environmentId} AND next_generation=${generation}`;
});

/** Historical callers and index rows retain strict execution retirement. */
export type DependencyGrant =
  | string
  | {
      readonly kind: "automation-birth";
      readonly sessionId: string;
    };

export class DestinationAutomationAuthority extends Context.Service<
  DestinationAutomationAuthority,
  {
    readonly accept: (
      value: ValidatedAcceptance,
      binding: AutomationBinding,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly facts: (id: string) => Effect.Effect<AutomationCertificate, OrchestratorMcpFailure>;
    readonly birth: (id: string) => Effect.Effect<AutomationCertificate, OrchestratorMcpFailure>;
    readonly claim: (id: string) => Effect.Effect<AutomationCertificate, OrchestratorMcpFailure>;
    readonly current: (
      kind: AutomationBinding["kind"],
      resourceId: string,
    ) => Effect.Effect<Option.Option<AutomationCertificate>, OrchestratorMcpFailure>;
    readonly disableLocal: (
      kind: AutomationBinding["kind"],
      resourceId: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly registerRecovery: (
      dependency: AutomationDependency,
      sessionId: string,
      credentialIdentity: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly registerDependency: (
      dependency: AutomationDependency,
      next: { readonly environmentId: EnvironmentId; readonly generation: string },
      sessionIds: ReadonlyArray<DependencyGrant>,
      originating: boolean,
      knownExpiry?: number | null,
      certificates?: ReadonlyArray<CertificateContributor>,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly receiveDisable: (
      input: AutomationDisable,
      credentialIdentity: string,
    ) => Effect.Effect<AutomationDisableAck | undefined, OrchestratorMcpFailure>;
    readonly nextDisableDeadline: Effect.Effect<number | null, OrchestratorMcpFailure>;
    readonly pending: Effect.Effect<
      ReadonlyArray<{
        readonly dependency: AutomationDependency;
        readonly environmentId: EnvironmentId;
        readonly generation: string;
      }>,
      OrchestratorMcpFailure
    >;
    readonly acknowledge: (
      acknowledgement: AutomationDisableAck,
      peer: EnvironmentId,
      generation: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/scheduledTasks/DestinationAutomationAuthority") {}

const readCertificate = Effect.fn("DestinationAutomationAuthority.readCertificate")(function* (
  id: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const row = (yield* sql<{
    certificate_json: string;
    disabled: number;
    revoked: number;
  }>`SELECT * FROM fork_automation_certificates WHERE id=${id}`.pipe(
    Effect.mapError(unavailable),
  ))[0];
  if (row === undefined) return yield* denied("Unknown destination automation authority.");
  const certificate = yield* decodeCertificate(row.certificate_json).pipe(
    Effect.mapError(unavailable),
  );
  return { certificate, disabled: row.disabled !== 0, revoked: row.revoked !== 0 };
});

const checkDestinationSessions = Effect.fn(
  "DestinationAutomationAuthority.checkDestinationSessions",
)(function* (certificate: AutomationCertificate) {
  const sql = yield* SqlClient.SqlClient;
  const sessionIds = new Set<string>();
  for (const node of certificate.graph.records)
    if (node.environmentId === certificate.audience && "envelope" in node.record)
      for (const id of [
        node.record.sessionId,
        node.record.originClientId,
        node.record.requestClientSessionId,
        node.record.resourceSessionId,
      ])
        if (id !== null && id !== undefined) sessionIds.add(id);
  for (const node of certificate.graph.legacy)
    if (node.environmentId === certificate.audience) sessionIds.add(node.sessionId);
  const now = DateTime.formatIso(yield* DateTime.now);
  for (const id of sessionIds) {
    const valid =
      yield* sql`SELECT 1 FROM auth_sessions WHERE session_id=${id} AND revoked_at IS NULL AND expires_at>${now}`.pipe(
        Effect.mapError(unavailable),
      );
    if (valid.length !== 1)
      return yield* denied("An accepted destination grant expired or was revoked.");
  }
});

const claimCertificate: (
  id: string,
  ancestors: ReadonlySet<string>,
  purpose?: "execution" | "birth",
) => Effect.Effect<AutomationCertificate, OrchestratorMcpFailure, SqlClient.SqlClient> = Effect.fn(
  "DestinationAutomationAuthority.claimCertificate",
)(function* (
  id: string,
  ancestors: ReadonlySet<string>,
  purpose: "execution" | "birth" = "execution",
) {
  if (ancestors.has(id) || ancestors.size >= 256)
    return yield* denied("The local automation contributor graph is cyclic or too deep.");
  const visited = new Set([...ancestors, id]);
  const sql = yield* SqlClient.SqlClient;
  const { certificate, disabled, revoked } = yield* readCertificate(id);
  const current = (yield* sql<{
    certificate_id: string;
  }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind=${certificate.binding.kind} AND resource_id=${certificate.binding.resourceId}`.pipe(
    Effect.mapError(unavailable),
  ))[0];
  const tombstones =
    yield* sql`SELECT 1 FROM fork_automation_disable_acks WHERE certificate_id=${id} LIMIT 1`.pipe(
      Effect.mapError(unavailable),
    );
  if (
    revoked ||
    tombstones.length !== 0 ||
    (purpose === "execution" && (disabled || current?.certificate_id !== id))
  )
    return yield* denied("Destination automation authority is disabled or superseded.");
  if (certificate.expiresAt !== null && certificate.expiresAt <= (yield* Clock.currentTimeMillis))
    return yield* denied("Destination automation authority has expired.");
  yield* checkDestinationSessions(certificate);
  for (const node of certificate.graph.records) {
    if (node.environmentId !== certificate.audience || !("certificateId" in node.record)) continue;
    const prior = (yield* readCertificate(node.record.certificateId)).certificate;
    const retainedBirth =
      certificate.binding.kind === "spectrum" &&
      prior.binding.kind === "spectrum" &&
      prior.binding.resourceId === certificate.binding.resourceId &&
      prior.binding.projectId === certificate.binding.projectId &&
      prior.binding.workflowGeneration < certificate.binding.workflowGeneration;
    yield* claimCertificate(
      prior.id,
      visited,
      node.record.kind === "automation-birth" || retainedBirth ? "birth" : purpose,
    );
  }
  if (purpose === "birth") return certificate;
  if (certificate.binding.kind === "schedule") {
    const row = (yield* sql<{
      project_id: string;
      thread_id: string | null;
      enabled: number;
      incarnation: string;
      certificate_id: string | null;
    }>`SELECT t.project_id,t.thread_id,t.enabled,i.incarnation,json_extract(t.schedule_json,'$.certificateId') AS certificate_id FROM scheduled_tasks t JOIN scheduled_task_request_incarnations i ON i.task_id=t.task_id WHERE t.task_id=${certificate.binding.resourceId}`.pipe(
      Effect.mapError(unavailable),
    ))[0];
    if (
      row === undefined ||
      row.enabled !== 1 ||
      row.project_id !== certificate.binding.projectId ||
      row.thread_id !== certificate.binding.threadId ||
      row.incarnation !== certificate.binding.incarnation ||
      row.certificate_id !== certificate.id
    )
      return yield* denied(
        "The current schedule no longer matches its accepted automation definition.",
      );
  } else {
    const row = (yield* sql<{
      generation: number;
      status: string;
      certificate_id: string | null;
    }>`SELECT generation,json_extract(payload_json,'$.status') AS status,json_extract(payload_json,'$.state.automationCertificateId') AS certificate_id FROM fork_spectra WHERE thread_id=${certificate.binding.resourceId}`.pipe(
      Effect.mapError(unavailable),
    ))[0];
    if (
      row === undefined ||
      row.status !== "active" ||
      row.generation !== certificate.binding.workflowGeneration ||
      row.certificate_id !== certificate.id
    )
      return yield* denied(
        "The current Spectrum no longer matches its accepted launch generation.",
      );
  }
  return certificate;
});

const claimBirthCertificate = (id: string) => claimCertificate(id, new Set(), "birth");

export const claimAutomationCertificate = (id: string) => claimCertificate(id, new Set());

export const currentAutomationCertificate = Effect.fn(
  "DestinationAutomationAuthority.currentAutomationCertificate",
)(function* (kind: AutomationBinding["kind"], resourceId: string) {
  const sql = yield* SqlClient.SqlClient;
  const row = (yield* sql<{
    certificate_id: string;
  }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind=${kind} AND resource_id=${resourceId}`.pipe(
    Effect.mapError(unavailable),
  ))[0];
  return row === undefined ? undefined : (yield* readCertificate(row.certificate_id)).certificate;
});

/** Native acceptance calls this inside the event/receipt transaction; no peer I/O is permitted. */
export const checkAutomationCommit = Effect.fn(
  "DestinationAutomationAuthority.checkAutomationCommit",
)(function* (recordIds: ReadonlyArray<string>, liveBirths: ReadonlySet<string> = new Set()) {
  const sql = yield* SqlClient.SqlClient;
  const pending = [...recordIds];
  const seen = new Set<string>();
  const certificates: Extract<typeof ExecutionRecord.Type, { readonly certificateId: string }>[] =
    [];
  const sessions = new Set<string>();
  while (pending.length !== 0) {
    const id = pending.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    if (seen.size > 256)
      return yield* denied("The native automation contributor bound was exceeded.");
    const row = (yield* sql<{
      record_json: string;
    }>`SELECT record_json FROM peer_authority_records WHERE id=${id}`.pipe(
      Effect.mapError(unavailable),
    ))[0];
    if (row === undefined) return yield* denied("The native authority owner is unavailable.");
    const record = yield* decodeExecutionRecord(row.record_json).pipe(Effect.mapError(unavailable));
    if ("envelope" in record) {
      for (const sessionId of [
        record.sessionId,
        record.originClientId,
        record.requestClientSessionId,
        record.resourceSessionId,
      ])
        if (sessionId !== null && sessionId !== undefined) sessions.add(sessionId);
    } else if ("resourceSessionId" in record && record.resourceSessionId !== undefined)
      sessions.add(record.resourceSessionId);
    if ("certificateId" in record) {
      certificates.push(record);
    } else if (record.context !== undefined) {
      const context = record.context;
      if (context.type === "task-stop") continue;
      pending.push(
        ...(context.type === "run-inputs"
          ? context.recordIds
          : context.type === "owned-thread"
            ? [context.creationRecordId, context.runRecordId]
            : [context.runRecordId]),
      );
    } else if (record.parentId !== null) pending.push(record.parentId);
  }
  if (certificates.length !== 0) {
    const now = DateTime.formatIso(yield* DateTime.now);
    for (const sessionId of sessions) {
      const active =
        yield* sql`SELECT 1 FROM auth_sessions WHERE session_id=${sessionId} AND revoked_at IS NULL AND expires_at>${now}`.pipe(
          Effect.mapError(unavailable),
        );
      if (active.length !== 1)
        return yield* denied(
          "A native automation contributor expired or was revoked before commit.",
        );
    }
  }
  const covered = new Map<string, typeof ExecutionRecord.Type>();
  const failures: { record: (typeof certificates)[number]; error: OrchestratorMcpFailure }[] = [];
  for (const record of certificates) {
    const result = yield* Effect.result(
      record.kind === "automation-birth"
        ? claimBirthCertificate(record.certificateId)
        : claimAutomationCertificate(record.certificateId),
    );
    if (result._tag === "Failure") {
      if (record.kind === "automation-birth") return yield* result.failure;
      failures.push({ record, error: result.failure });
      continue;
    }
    const certificate = result.success;
    if (
      record.kind === "automation-birth" &&
      !liveBirths.has(record.id) &&
      !certificates.some(
        (owner) =>
          owner.kind === "automation-execution" && owner.certificateId === record.certificateId,
      )
    ) {
      failures.push({
        record,
        error: denied("A native birth requires live original-contributor admission."),
      });
      continue;
    }
    if (certificate.binding.projectId !== record.projectId)
      return yield* denied("The native automation project changed.");
    if (record.kind === "automation-birth") continue;
    for (const node of certificate.graph.records)
      if (node.environmentId === certificate.audience) {
        const prior = covered.get(node.record.id);
        if (prior !== undefined && requestDigest(prior) !== requestDigest(node.record))
          return yield* denied("The native automation contributor changed.");
        covered.set(node.record.id, node.record);
      }
  }
  for (const failure of failures) {
    const frozen = covered.get(failure.record.id);
    if (frozen === undefined || requestDigest(frozen) !== requestDigest(failure.record))
      return yield* failure.error;
  }
});

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureAutomationAuthoritySchema.pipe(Effect.mapError(unavailable));
  const read = (id: string) =>
    readCertificate(id).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const claim = (id: string) =>
    claimAutomationCertificate(id).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const accept: DestinationAutomationAuthority["Service"]["accept"] = Effect.fn(
    "DestinationAutomationAuthority.accept",
  )(function* (value, binding) {
    const certificate = value.certificate;
    if (
      !value.consume() ||
      requestDigest({ ...certificate.binding, incarnation: binding.incarnation }) !==
        requestDigest(binding)
    )
      return yield* denied("The automation acceptance does not match this definition.");
    if (certificate.expiresAt !== null && certificate.expiresAt <= (yield* Clock.currentTimeMillis))
      return yield* denied("The captured automation authority expired before acceptance.");
    for (const expected of value.grantFence) {
      const rows = yield* sql<{
        generation: string;
        grant_json: string;
      }>`SELECT generation,grant_json FROM peer_authority_sessions WHERE session_id=${expected.session_id}`.pipe(
        Effect.mapError(unavailable),
      );
      if (
        rows.length !== 1 ||
        rows[0]!.generation !== expected.generation ||
        rows[0]!.grant_json !== expected.grant_json
      )
        return yield* denied("A destination contributor grant changed during automation capture.");
    }
    yield* checkDestinationSessions(certificate).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const tombstone =
      yield* sql`SELECT 1 FROM fork_automation_disable_acks WHERE certificate_id=${certificate.id} LIMIT 1`.pipe(
        Effect.mapError(unavailable),
      );
    if (tombstone.length !== 0)
      return yield* denied("The automation was disabled before acceptance.");
    const previous = yield* sql<{
      certificate_id: string;
    }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind=${binding.kind} AND resource_id=${binding.resourceId} AND certificate_id<>${certificate.id}`.pipe(
      Effect.mapError(unavailable),
    );
    yield* invalidateAutomationCertificates(
      previous.map((row) => row.certificate_id),
      "retired",
    ).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.mapError(unavailable));
    const encoded = json({ ...certificate, binding });
    yield* sql`INSERT INTO fork_automation_certificates(id,certificate_json) VALUES (${certificate.id},${encoded}) ON CONFLICT DO NOTHING`.pipe(
      Effect.mapError(unavailable),
    );
    const stored = yield* read(certificate.id);
    if (
      requestDigest(stored.certificate) !== requestDigest({ ...certificate, binding }) ||
      stored.disabled
    )
      return yield* denied("The automation acceptance identity was already consumed.");
    yield* sql`INSERT INTO fork_automation_bindings VALUES (${binding.kind},${binding.resourceId},${certificate.id}) ON CONFLICT(kind,resource_id) DO UPDATE SET certificate_id=excluded.certificate_id`.pipe(
      Effect.mapError(unavailable),
    );
  });
  const current: DestinationAutomationAuthority["Service"]["current"] = Effect.fn(
    "DestinationAutomationAuthority.current",
  )(function* (kind, resourceId) {
    const rows = yield* sql<{
      certificate_id: string;
    }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind=${kind} AND resource_id=${resourceId}`.pipe(
      Effect.mapError(unavailable),
    );
    return rows[0] === undefined
      ? Option.none()
      : Option.some((yield* read(rows[0].certificate_id)).certificate);
  });
  const disableLocal: DestinationAutomationAuthority["Service"]["disableLocal"] = (
    kind,
    resourceId,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const ids = yield* sql<{
            certificate_id: string;
          }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind=${kind} AND resource_id=${resourceId}`;
          yield* invalidateAutomationCertificates(
            ids.map((row) => row.certificate_id),
            "retired",
          ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
        }),
      )
      .pipe(Effect.mapError(unavailable));
  const registerRecovery: DestinationAutomationAuthority["Service"]["registerRecovery"] = Effect.fn(
    "DestinationAutomationAuthority.registerRecovery",
  )(function* (dependency, sessionId, credentialIdentity) {
    const key = requestDigest(dependency);
    yield* sql`INSERT INTO fork_automation_recovery VALUES (${key},${json(dependency)},${sessionId},${credentialIdentity}) ON CONFLICT DO NOTHING`.pipe(
      Effect.mapError(unavailable),
    );
    const stored = (yield* sql<{
      session_id: string;
      credential_identity: string;
    }>`SELECT session_id,credential_identity FROM fork_automation_recovery WHERE dependency_id=${key}`.pipe(
      Effect.mapError(unavailable),
    ))[0];
    if (stored?.session_id !== sessionId || stored.credential_identity !== credentialIdentity)
      return yield* denied("The automation recovery identity is already bound.");
  });
  const registerDependency: DestinationAutomationAuthority["Service"]["registerDependency"] = (
    dependency,
    next,
    sessionIds,
    originating,
    knownExpiry,
    certificates = [],
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const key = requestDigest(dependency);
          yield* sql`INSERT INTO fork_automation_obligations(dependency_id,dependency_json,next_environment_id,next_generation,originating,selection_order) VALUES (${key},${json(dependency)},${next.environmentId},${next.generation},${originating ? 1 : 0},(SELECT coalesce(max(selection_order),0) FROM fork_automation_obligations)) ON CONFLICT DO NOTHING`;
          const stored = (yield* sql<{
            next_environment_id: string;
            next_generation: string;
            originating: number;
          }>`SELECT next_environment_id,next_generation,originating FROM fork_automation_obligations WHERE dependency_id=${key}`)[0];
          if (
            stored?.next_environment_id !== next.environmentId ||
            stored.next_generation !== next.generation ||
            stored.originating !== (originating ? 1 : 0)
          )
            return yield* denied("The original automation disable route is already bound.");
          if (knownExpiry !== undefined && knownExpiry !== null)
            yield* sql`UPDATE fork_automation_obligations SET expires_at=CASE WHEN expires_at IS NULL THEN ${knownExpiry} ELSE min(expires_at,${knownExpiry}) END WHERE dependency_id=${key}`;
          for (const contributor of sessionIds) {
            const sessionId = typeof contributor === "string" ? contributor : contributor.sessionId;
            const kind =
              typeof contributor === "string" ? "automation-execution" : contributor.kind;
            yield* sql`INSERT INTO fork_automation_dependency_grants(dependency_id,session_id,grant_kind) VALUES (${key},${sessionId},${kind}) ON CONFLICT(dependency_id,session_id) DO UPDATE SET grant_kind=CASE WHEN grant_kind='automation-execution' OR excluded.grant_kind='automation-execution' THEN 'automation-execution' ELSE grant_kind END`;
          }
          for (const certificate of certificates) {
            yield* certificate.kind === "automation-birth"
              ? claimBirthCertificate(certificate.certificateId).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                )
              : claim(certificate.certificateId);
            yield* sql`INSERT INTO fork_automation_dependency_certificates(dependency_id,certificate_id,certificate_kind) VALUES (${key},${certificate.certificateId},${certificate.kind}) ON CONFLICT(dependency_id,certificate_id) DO UPDATE SET certificate_kind=CASE WHEN certificate_kind='automation-execution' OR excluded.certificate_kind='automation-execution' THEN 'automation-execution' ELSE certificate_kind END`;
          }
        }),
      )
      .pipe(Effect.catchTags({ SqlError: (cause) => Effect.fail(unavailable(cause)) }));
  const receiveDisable: DestinationAutomationAuthority["Service"]["receiveDisable"] = (
    input,
    credentialIdentity,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const key = requestDigest(input.dependency);
          const recovery = (yield* sql<{
            credential_identity: string;
          }>`SELECT credential_identity FROM fork_automation_recovery WHERE dependency_id=${key}`)[0];
          if (recovery?.credential_identity !== credentialIdentity)
            return yield* denied("The disable does not match its registered recovery identity.");
          const prior = (yield* sql<{
            ack_json: string;
          }>`SELECT ack_json FROM fork_automation_disable_acks WHERE dependency_id=${key}`)[0];
          if (prior !== undefined)
            return yield* decodeAck(prior.ack_json).pipe(Effect.mapError(unavailable));
          const relay = (yield* sql<{
            originating: number;
            ack_json: string | null;
          }>`SELECT originating,ack_json FROM fork_automation_obligations WHERE dependency_id=${key}`)[0];
          if (relay !== undefined && relay.originating === 0) {
            yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE dependency_id=${key} AND ack_json IS NULL`;
            return relay.ack_json === null
              ? undefined
              : yield* decodeAck(relay.ack_json).pipe(Effect.mapError(unavailable));
          }
          const acknowledgement: AutomationDisableAck = {
            version: 1,
            dependency: input.dependency,
            disabled: true,
          };
          yield* sql`INSERT INTO fork_automation_disable_acks VALUES (${key},${input.dependency.certificateId},${json(acknowledgement)})`;
          yield* invalidateAutomationCertificates([input.dependency.certificateId], "revoked").pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          return acknowledgement;
        }),
      )
      .pipe(Effect.catchTags({ SqlError: (cause) => Effect.fail(unavailable(cause)) }));
  const nextDisableDeadline = Effect.gen(function* () {
    const rows = yield* sql<{ deadline: number | null }>`
      SELECT min(deadline) AS deadline FROM (
        SELECT o.expires_at AS deadline FROM fork_automation_obligations o
          WHERE o.ack_json IS NULL AND o.pending=0
        UNION ALL
        SELECT json_extract(c.certificate_json,'$.expiresAt') AS deadline
          FROM fork_automation_dependency_certificates d
          JOIN fork_automation_certificates c ON c.id=d.certificate_id
          JOIN fork_automation_obligations o ON o.dependency_id=d.dependency_id
          WHERE o.ack_json IS NULL AND o.pending=0
        UNION ALL
        SELECT round(unixepoch(a.expires_at,'subsec')*1000) AS deadline
          FROM fork_automation_dependency_grants d
          JOIN auth_sessions a ON a.session_id=d.session_id
          JOIN fork_automation_obligations o ON o.dependency_id=d.dependency_id
          WHERE o.ack_json IS NULL AND o.pending=0
      )
    `.pipe(Effect.mapError(unavailable));
    if (rows[0] === undefined) return yield* unavailable(undefined);
    return rows[0].deadline;
  });
  const pending: DestinationAutomationAuthority["Service"]["pending"] = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND expires_at IS NOT NULL AND expires_at<=${now}`.pipe(
      Effect.mapError(unavailable),
    );
    yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND dependency_id IN (SELECT d.dependency_id FROM fork_automation_dependency_certificates d JOIN fork_automation_certificates c ON c.id=d.certificate_id WHERE c.revoked=1 OR (d.certificate_kind='automation-execution' AND c.disabled=1) OR (json_extract(c.certificate_json,'$.expiresAt') IS NOT NULL AND json_extract(c.certificate_json,'$.expiresAt')<=${now}))`.pipe(
      Effect.mapError(unavailable),
    );
    yield* sql`UPDATE fork_automation_obligations SET pending=1 WHERE ack_json IS NULL AND dependency_id IN (SELECT d.dependency_id FROM fork_automation_dependency_grants d JOIN auth_sessions a ON a.session_id=d.session_id WHERE a.revoked_at IS NOT NULL OR round(unixepoch(a.expires_at,'subsec')*1000)<=${now})`.pipe(
      Effect.mapError(unavailable),
    );
    const rows = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const batch = yield* sql<{
            dependency_id: string;
            dependency_json: string;
            next_environment_id: string;
            next_generation: string;
          }>`SELECT dependency_id,dependency_json,next_environment_id,next_generation FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL ORDER BY selection_order,dependency_id LIMIT 32`;
          if (batch.length !== 0)
            yield* sql`UPDATE fork_automation_obligations SET selection_order=(SELECT coalesce(max(selection_order),0)+1 FROM fork_automation_obligations) WHERE ${sql.in(
              "dependency_id",
              batch.map((row) => row.dependency_id),
            )}`;
          return batch;
        }),
      )
      .pipe(Effect.mapError(unavailable));
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        return {
          dependency: yield* decodeDependency(row.dependency_json).pipe(
            Effect.mapError(unavailable),
          ),
          environmentId: EnvironmentId.make(row.next_environment_id),
          generation: row.next_generation,
        };
      }),
    );
  });
  const acknowledge: DestinationAutomationAuthority["Service"]["acknowledge"] = Effect.fn(
    "DestinationAutomationAuthority.acknowledge",
  )(function* (acknowledgement, peer, generation) {
    const rows =
      yield* sql`UPDATE fork_automation_obligations SET ack_json=${json(acknowledgement)},pending=0 WHERE dependency_id=${requestDigest(acknowledgement.dependency)} AND next_environment_id=${peer} AND next_generation=${generation} RETURNING dependency_id`.pipe(
        Effect.mapError(unavailable),
      );
    if (rows.length !== 1)
      return yield* denied("The disable acknowledgement does not match its obligation.");
  });
  return DestinationAutomationAuthority.of({
    accept,
    claim,
    birth: (id) => claimBirthCertificate(id).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    facts: (id) => read(id).pipe(Effect.map((value) => value.certificate)),
    current,
    disableLocal,
    registerRecovery,
    registerDependency,
    receiveDisable,
    nextDisableDeadline,
    pending,
    acknowledge,
  });
});

export const layer = Layer.effect(DestinationAutomationAuthority, make);
