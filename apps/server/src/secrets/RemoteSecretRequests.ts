import * as PeerLinkEvents from "../peer/PeerLinkEvents.ts";
import { forkParked } from "../serverActivation.ts";
import * as PubSub from "effect/PubSub";
import { PeerGrant } from "@t3tools/contracts";
import * as MutationReceipts from "../scheduledTasks/mutationReceipts.ts";
import {
  PeerAuthorityEnvelope,
  type McpScheduledMutationReceipt,
  type RemoteSecretReceiptQuery,
} from "@t3tools/contracts";
import {
  RemoteSecretReceiptIdentity,
  OrchestratorMcpFailure,
  RemoteSecretRetirementAck,
  type RemoteSecretControl,
  type RemoteSecretHandle,
  type RemoteSecretResult,
  type RemoteSecretRetirement,
  type ProjectId,
  remoteSecretIntent,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import {
  canonicalRequest,
  requestDigest,
  type RequestIdentity,
} from "../mcp/McpRequestReceipts.ts";
import { requestNamespace } from "../mcp/requestIdentity.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import { interactiveAuthority } from "../peer/PeerAuthorityRecord.ts";
import * as Persistence from "./remoteSecretPersistence.ts";

const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "The secret request does not permit this actor, intent or state.",
  });
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The protected secret request could not be confirmed.",
  });
const decodeConsumer = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ request: RemoteSecretReceiptIdentity })),
);
const decodeEnvelope = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ envelope: PeerAuthorityEnvelope })),
);
const decodeGrant = Schema.decodeUnknownEffect(Schema.fromJsonString(PeerGrant));
const nameFor = (row: { request_id: string; attempt_id: string | null }) =>
  `secret-remote-${row.request_id}-${row.attempt_id}`;
interface Row {
  request_id: string;
  environment_id: string;
  generation: string;
  namespace: string;
  origin_json: string;
  source_record_id: string;
  session_id: string;
  credential_identity: string;
  key_digest: string;
  input_digest: string;
  project_id: string;
  family: string;
  status: RemoteSecretResult["status"];
  attempt_id: string | null;
  consumer_json: string | null;
  expires_at: number;
}
export class RemoteSecretRequests extends Context.Service<
  RemoteSecretRequests,
  {
    readonly receipt: (
      scope: McpInvocationScope,
      query: RemoteSecretReceiptQuery,
    ) => Effect.Effect<Option.Option<McpScheduledMutationReceipt>, OrchestratorMcpFailure>;
    readonly control: (
      scope: McpInvocationScope,
      input: RemoteSecretControl,
    ) => Effect.Effect<RemoteSecretResult, OrchestratorMcpFailure>;
    readonly reserve: (
      scope: McpInvocationScope,
      handle: RemoteSecretHandle,
      projectId: ProjectId,
      request: RequestIdentity,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    /** Runs under the scheduler's OUTERMOST acceptance transaction. */
    readonly take: (
      handle: RemoteSecretHandle,
      request: RequestIdentity,
    ) => Effect.Effect<string, OrchestratorMcpFailure>;
    readonly finalize: (handle: RemoteSecretHandle) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly retire: (
      input: RemoteSecretRetirement,
      credentialIdentity: string,
    ) => Effect.Effect<typeof RemoteSecretRetirementAck.Type, OrchestratorMcpFailure>;
    readonly reconcile: Effect.Effect<void, OrchestratorMcpFailure>;
  }
>()("t3/secrets/RemoteSecretRequests") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ServerSecretStore.ServerSecretStore;
  const authority = yield* PeerAuthority.PeerAuthority;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const linkEvents = yield* Effect.serviceOption(PeerLinkEvents.PeerLinkEvents);
  const here = yield* environment.getEnvironmentId;
  yield* Persistence.schema;
  const lock = yield* Semaphore.make(1);
  const read = (id: string) =>
    sql<Row>`SELECT * FROM remote_secret_requests WHERE request_id=${id}`.pipe(
      Effect.map((rows) => rows[0]),
      Effect.mapError(unavailable),
    );
  const remove = (row: Row) =>
    row.attempt_id === null
      ? Effect.void
      : store.remove(nameFor(row)).pipe(
          Effect.mapError(unavailable),
          Effect.tapError(() =>
            Option.isSome(linkEvents) ? linkEvents.value.automationChanged : Effect.void,
          ),
        );
  const owned = (scope: McpInvocationScope, row: Row) => {
    const record = interactiveAuthority(scope.authority);
    return (
      record !== undefined &&
      scope.client?.linked === true &&
      record.peer.environmentId === row.environment_id &&
      record.peer.generation === row.generation
    );
  };
  const eligible = (row: Row) =>
    Effect.gen(function* () {
      if (row.expires_at <= (yield* Clock.currentTimeMillis)) {
        yield* sql`UPDATE remote_secret_requests SET status='retired' WHERE request_id=${row.request_id} AND status NOT IN ('consumed','declined','retired')`.pipe(
          Effect.mapError(unavailable),
        );
        yield* remove(row);
        return yield* denied();
      }
      yield* authority
        .checkInteractiveRecord(row.source_record_id)
        .pipe(
          Effect.tapError((error) =>
            error.code === "capability_denied"
              ? sql`UPDATE remote_secret_requests SET status='retired' WHERE request_id=${row.request_id} AND status NOT IN ('consumed','declined','retired')`.pipe(
                  Effect.mapError(unavailable),
                  Effect.andThen(remove(row)),
                )
              : Effect.void,
          ),
        );
    });
  const result = (row: Row): RemoteSecretResult => ({
    handle: { environmentId: here, requestId: row.request_id },
    status: row.status,
  });
  const control: RemoteSecretRequests["Service"]["control"] = (scope, input) =>
    Effect.gen(function* () {
      const record = interactiveAuthority(scope.authority);
      if (record === undefined || record.sessionId === null || scope.client?.linked !== true)
        return yield* denied();
      yield* authority.checkPayload(record, "remote_secret", remoteSecretIntent(input));
      if (input.type === "create") {
        yield* authority.checkTarget(scope, { type: "environment" });
        if (
          record.ceiling.runtimeMode !== "full-access" ||
          record.ceiling.interactionMode !== "default" ||
          record.ceiling.resourceScope !== "environment-resources" ||
          !record.ceiling.capabilities.includes("orchestration")
        )
          return yield* denied();
        const projects =
          yield* sql`SELECT 1 FROM projection_projects WHERE project_id=${input.projectId} AND deleted_at IS NULL`.pipe(
            Effect.mapError(unavailable),
          );
        if (projects.length !== 1) return yield* denied();
        const origin = yield* authority.secretOrigin(record.id);
        const inputDigest = requestDigest(input);
        const keyDigest = requestDigest(input.key);
        const existing =
          (yield* sql<Row>`SELECT * FROM remote_secret_requests WHERE environment_id=${record.peer.environmentId} AND generation=${record.peer.generation} AND namespace=${origin.namespace} AND key_digest=${keyDigest}`.pipe(
            Effect.mapError(unavailable),
          ))[0];
        if (existing !== undefined) {
          if (
            existing.input_digest !== inputDigest ||
            existing.origin_json !== canonicalRequest(origin)
          )
            return yield* denied();
          return result(existing);
        }
        const channel = (yield* sql<{
          credential_identity: string | null;
        }>`SELECT credential_identity FROM peer_authority_sessions WHERE session_id=${record.sessionId}`.pipe(
          Effect.mapError(unavailable),
        ))[0];
        if (channel?.credential_identity == null) return yield* denied();
        const id = `rsr_${Hex.encode(yield* crypto.randomBytes(16).pipe(Effect.mapError(unavailable)))}`;
        const expiry = (yield* Clock.currentTimeMillis) + 24 * 60 * 60 * 1000;
        yield* sql`INSERT INTO remote_secret_requests (request_id,environment_id,generation,namespace,origin_json,source_record_id,session_id,credential_identity,key_digest,input_digest,project_id,family,status,expires_at) VALUES (${id},${record.peer.environmentId},${record.peer.generation},${origin.namespace},${canonicalRequest(origin)},${record.id},${record.sessionId},${channel.credential_identity},${keyDigest},${inputDigest},${input.projectId},${input.family},'pending',${expiry})`.pipe(
          Effect.mapError(unavailable),
        );
        const created = yield* read(id);
        if (created === undefined) return yield* unavailable();
        if (Option.isSome(linkEvents)) yield* linkEvents.value.automationChanged;
        return result(created);
      }
      const row = yield* read(input.requestId);
      if (row === undefined || !owned(scope, row)) return yield* denied();
      if (input.type === "status") {
        if (requestNamespace(scope) !== row.namespace) return yield* denied();
        yield* eligible(row);
        return result(row);
      }
      yield* authority.secretClient(record.id);
      yield* eligible(row);
      if (input.answer === "decline") {
        const changed =
          yield* sql`UPDATE remote_secret_requests SET status='declined' WHERE request_id=${row.request_id} AND status='pending' RETURNING request_id`.pipe(
            Effect.mapError(unavailable),
          );
        if (changed.length === 0 && row.status !== "declined") return yield* denied();
        return { ...result(row), status: "declined" as const };
      }
      if (input.value === undefined || input.value.trim().length === 0) return yield* denied();
      // Durable attempt before filesystem publication. No attempt ever replaces another's bytes.
      if (row.status === "pending") {
        const won =
          yield* sql`UPDATE remote_secret_requests SET status='saving',attempt_id=${input.attemptId} WHERE request_id=${row.request_id} AND status='pending' RETURNING request_id`.pipe(
            Effect.mapError(unavailable),
          );
        if (won.length !== 1) return yield* denied();
      } else if (
        row.attempt_id !== input.attemptId ||
        !["saving", "saved", "reserved", "consumed"].includes(row.status)
      )
        return yield* denied();
      const attempt = { ...row, attempt_id: input.attemptId };
      const bytes = new TextEncoder().encode(input.value);
      const prior = yield* store.get(nameFor(attempt)).pipe(Effect.mapError(unavailable));
      if (Option.isSome(prior)) {
        if (new TextDecoder().decode(prior.value) !== input.value) return yield* denied();
      } else {
        if (row.status !== "pending" && row.status !== "saving") return yield* denied();
        yield* store.create(nameFor(attempt), bytes).pipe(Effect.mapError(unavailable));
      }
      yield* eligible(attempt);
      yield* sql`UPDATE remote_secret_requests SET status='saved' WHERE request_id=${row.request_id} AND status='saving' AND attempt_id=${input.attemptId}`.pipe(
        Effect.mapError(unavailable),
      );
      const current = yield* read(row.request_id);
      if (current === undefined || !["saved", "reserved", "consumed"].includes(current.status)) {
        yield* remove(attempt);
        return yield* denied();
      }
      return result(current);
    }).pipe(lock.withPermits(1));
  const reserve: RemoteSecretRequests["Service"]["reserve"] = (scope, handle, projectId, request) =>
    Effect.gen(function* () {
      if (handle.environmentId !== here) return yield* denied();
      const row = yield* read(handle.requestId);
      const record = interactiveAuthority(scope.authority);
      if (
        row === undefined ||
        record === undefined ||
        !owned(scope, row) ||
        row.namespace !== requestNamespace(scope) ||
        row.project_id !== projectId ||
        row.family !== "schedule.webhook-signature"
      )
        return yield* denied();
      const currentOrigin = yield* authority.secretOrigin(record.id);
      if (canonicalRequest(currentOrigin) !== row.origin_json) return yield* denied();
      yield* eligible(row);
      const identity = canonicalRequest({
        request,
        namespace: row.namespace,
        origin: currentOrigin,
        projectId,
        family: row.family,
      });
      if (row.status === "reserved" && row.consumer_json === identity) return;
      const changed =
        yield* sql`UPDATE remote_secret_requests SET status='reserved',consumer_json=${identity} WHERE request_id=${row.request_id} AND status='saved' AND expires_at>${yield* Clock.currentTimeMillis} RETURNING request_id`.pipe(
          Effect.mapError(unavailable),
        );
      if (changed.length !== 1) return yield* denied();
    }).pipe(lock.withPermits(1));
  const take: RemoteSecretRequests["Service"]["take"] = (handle, request) =>
    Effect.gen(function* () {
      if (handle.environmentId !== here) return yield* denied();
      const row = yield* read(handle.requestId);
      if (
        row === undefined ||
        row.status !== "reserved" ||
        row.consumer_json === null ||
        row.expires_at <= (yield* Clock.currentTimeMillis)
      )
        return yield* denied();
      // Compare the complete verified operation, not a caller-chosen reservation token.
      const consumer = yield* decodeConsumer(row.consumer_json).pipe(Effect.mapError(unavailable));
      if (canonicalRequest(consumer.request) !== canonicalRequest(request)) return yield* denied();
      const admitted =
        yield* sql`SELECT 1 FROM peer_authority_sessions p JOIN auth_sessions a ON a.session_id=p.session_id
      WHERE p.session_id=${row.session_id} AND p.generation=${row.generation} AND a.revoked_at IS NULL
      AND round(unixepoch(a.expires_at,'subsec')*1000)>${yield* Clock.currentTimeMillis}
      AND json_extract(p.grant_json,'$.resourceScope')='environment-resources'`.pipe(
          Effect.mapError(unavailable),
        );
      if (admitted.length !== 1) return yield* denied();
      const value = yield* store.get(nameFor(row)).pipe(Effect.mapError(unavailable));
      if (Option.isNone(value)) return yield* unavailable();
      const changed =
        yield* sql`UPDATE remote_secret_requests SET status='consumed' WHERE request_id=${row.request_id} AND status='reserved' AND consumer_json=${row.consumer_json} AND expires_at>${yield* Clock.currentTimeMillis} RETURNING request_id`.pipe(
          Effect.mapError(unavailable),
        );
      if (changed.length !== 1) return yield* denied();
      return new TextDecoder().decode(value.value);
    });
  const finalize: RemoteSecretRequests["Service"]["finalize"] = (handle) =>
    Effect.gen(function* () {
      if (handle.environmentId !== here) return yield* denied();
      const row = yield* read(handle.requestId);
      if (row !== undefined && ["consumed", "declined", "retired"].includes(row.status))
        yield* remove(row);
    });
  const reconcileConsumer = (row: Row) =>
    Effect.gen(function* () {
      if (row.consumer_json === null) return false;
      const consumer = yield* decodeConsumer(row.consumer_json).pipe(Effect.mapError(unavailable));
      const confirmed = yield* MutationReceipts.acceptance(consumer.request).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      if (Option.isNone(confirmed)) return false;
      if (
        confirmed.value.resource.owner !== row.namespace ||
        confirmed.value.resource.projectId !== row.project_id
      )
        return yield* denied();
      yield* sql`UPDATE remote_secret_requests SET status='consumed' WHERE request_id=${row.request_id} AND consumer_json=${row.consumer_json}`.pipe(
        Effect.mapError(unavailable),
      );
      return true;
    });
  const retire: RemoteSecretRequests["Service"]["retire"] = (input, credentialIdentity) =>
    Effect.gen(function* () {
      const row = yield* read(input.requestId);
      if (
        row === undefined ||
        input.environmentId !== row.environment_id ||
        input.generation !== row.generation ||
        canonicalRequest(input.origin) !== row.origin_json ||
        credentialIdentity !== row.credential_identity
      )
        return yield* denied();
      yield* reconcileConsumer(row);
      // Receipt reconciliation precedes retirement; recovery cannot authorize a missing operation.
      yield* sql`UPDATE remote_secret_requests SET status='retired' WHERE request_id=${row.request_id} AND status NOT IN ('consumed','declined','retired')`.pipe(
        Effect.mapError(unavailable),
      );
      yield* remove(row);
      return { ...input, retired: true as const };
    }).pipe(lock.withPermits(1));
  const reconcileBatch = Effect.gen(function* () {
    let cleanupPending = false;
    yield* sql`UPDATE remote_secret_requests SET status='retired' WHERE status NOT IN ('consumed','declined','retired') AND session_id IN (SELECT session_id FROM auth_sessions WHERE revoked_at IS NOT NULL OR round(unixepoch(expires_at,'subsec')*1000)<=${yield* Clock.currentTimeMillis})`.pipe(
      Effect.mapError(unavailable),
    );
    const rows = yield* sql<Row>`SELECT * FROM remote_secret_requests`.pipe(
      Effect.mapError(unavailable),
    );
    for (const row of rows) {
      if (row.consumer_json !== null) yield* reconcileConsumer(row);
      if (row.status === "saving") {
        const value = yield* store.get(nameFor(row)).pipe(Effect.mapError(unavailable));
        if (Option.isSome(value))
          yield* sql`UPDATE remote_secret_requests SET status='saved' WHERE request_id=${row.request_id} AND status='saving' AND attempt_id=${row.attempt_id}`.pipe(
            Effect.mapError(unavailable),
          );
      }
      if (row.expires_at <= (yield* Clock.currentTimeMillis))
        yield* sql`UPDATE remote_secret_requests SET status='retired' WHERE request_id=${row.request_id} AND status NOT IN ('consumed','declined','retired')`.pipe(
          Effect.mapError(unavailable),
        );
      const current = yield* read(row.request_id);
      if (current !== undefined && ["consumed", "declined", "retired"].includes(current.status))
        yield* remove(current).pipe(
          Effect.catch(() => {
            cleanupPending = true;
            return Effect.logWarning("Remote secret protected copy cleanup remains pending.", {
              requestId: current.request_id,
            });
          }),
        );
    }
    if (store.directory !== undefined) {
      const entries = yield* fs.readDirectory(store.directory).pipe(
        Effect.catch(() => {
          cleanupPending = true;
          return Effect.logWarning("Remote secret temporary copy scan remains pending.").pipe(
            Effect.as([] as Array<string>),
          );
        }),
      );
      for (const name of entries) {
        if (!/^secret-remote-rsr_[0-9a-f]{32}-[0-9a-f]{32}\.bin\.[0-9a-f-]+\.tmp$/.test(name))
          continue;
        yield* fs.remove(path.join(store.directory, name), { force: true }).pipe(
          Effect.catch(() => {
            cleanupPending = true;
            return Effect.logWarning("Remote secret temporary copy cleanup remains pending.");
          }),
        );
      }
    }
    return cleanupPending;
  }).pipe(lock.withPermits(1));
  const reconcile = reconcileBatch.pipe(
    Effect.flatMap((pending) => (pending ? unavailable() : Effect.void)),
  );
  // Only cleanup leaves may defer startup; schema and state/authority failures still fail construction.
  let retryCleanup = yield* reconcileBatch;
  if (Option.isSome(linkEvents)) {
    const changes = yield* linkEvents.value.subscribeAutomationChanges;
    yield* forkParked(
      Effect.forever(
        Effect.gen(function* () {
          if (retryCleanup) yield* Effect.sleep("2 seconds");
          else {
            const [next] = yield* sql<{
              deadline: number | null;
            }>`SELECT min(min(r.expires_at,round(unixepoch(a.expires_at,'subsec')*1000))) AS deadline FROM remote_secret_requests r JOIN auth_sessions a ON a.session_id=r.session_id WHERE r.status IN ('pending','saving','saved','reserved')`.pipe(
              Effect.mapError(unavailable),
            );
            if (next?.deadline == null) yield* PubSub.take(changes);
            else
              yield* Effect.raceFirst(
                PubSub.take(changes),
                Effect.sleep(Math.max(0, next.deadline - (yield* Clock.currentTimeMillis))),
              );
          }
          retryCleanup = yield* reconcileBatch;
        }).pipe(
          Effect.catch(() => {
            retryCleanup = true;
            return Effect.logWarning("Remote secret reconciliation remains pending.");
          }),
        ),
      ),
    );
  }
  return RemoteSecretRequests.of({
    control,
    reserve,
    take,
    finalize,
    retire,
    reconcile,
    receipt: (scope, query) =>
      Effect.gen(function* () {
        if (scope.client?.linked !== true || query.handle.environmentId !== here)
          return yield* denied();
        const row = yield* read(query.handle.requestId);
        if (
          row === undefined ||
          row.session_id !== scope.client.sessionId ||
          row.project_id !== query.projectId ||
          query.envelope.environmentId !== row.environment_id ||
          query.envelope.generation !== row.generation ||
          query.envelope.requestOwner?.namespace !== row.namespace ||
          query.envelope.requestOwner.generation !== query.request.generation
        )
          return yield* denied();
        // No fresh action, source validation, capture or reservation is possible on this route.
        const acceptance = yield* MutationReceipts.acceptance(query.request).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        if (Option.isNone(acceptance)) return Option.none();
        const { resource, receipt } = acceptance.value;
        const recordId = `in:${row.session_id}:${query.envelope.recordId}`;
        if (
          resource.owner !== row.namespace ||
          resource.projectId !== query.projectId ||
          resource.authorityRecordId !== recordId ||
          row.status !== "consumed"
        )
          return yield* denied();
        const original = (yield* sql<{
          record_json: string;
        }>`SELECT record_json FROM peer_authority_records WHERE id=${recordId}`.pipe(
          Effect.mapError(unavailable),
        ))[0];
        if (original === undefined) return yield* denied();
        const saved = yield* decodeEnvelope(original.record_json).pipe(
          Effect.mapError(unavailable),
        );
        if (canonicalRequest(saved.envelope) !== canonicalRequest(query.envelope))
          return yield* denied();
        const current = (yield* sql<{
          generation: string;
          grant_json: string;
          revoked_at: string | null;
          expires_at: string;
        }>`SELECT p.generation,p.grant_json,a.revoked_at,a.expires_at FROM peer_authority_sessions p JOIN auth_sessions a ON a.session_id=p.session_id WHERE p.session_id=${row.session_id}`.pipe(
          Effect.mapError(unavailable),
        ))[0];
        if (
          current === undefined ||
          current.generation !== row.generation ||
          current.revoked_at !== null ||
          Date.parse(current.expires_at) <= (yield* Clock.currentTimeMillis)
        )
          return yield* denied();
        const grant = yield* decodeGrant(current.grant_json).pipe(Effect.mapError(unavailable));
        if (grant.resourceScope !== "environment-resources") return yield* denied();
        return Option.some(receipt);
      }),
  });
});
export const layer = Layer.effect(RemoteSecretRequests, make);
