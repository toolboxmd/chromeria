import {
  OrchestratorMcpScheduleTaskInput,
  RemoteSecretReceiptQuery,
  type McpScheduledMutationReceipt,
} from "@t3tools/contracts";
import { requestNamespace } from "../mcp/requestIdentity.ts";
import { requestInputDigest } from "../mcp/McpRequestReceipts.ts";
import {
  AuthSessionId,
  OrchestratorMcpFailure,
  remoteSecretIntent,
  type EnvironmentId,
  type RemoteSecretCreate,
  type RemoteSecretDestination,
  type RemoteSecretControl,
  type RemoteSecretHandle,
  type SecretRequestAnswerInput,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import { canonicalRequest } from "../mcp/McpRequestReceipts.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Links from "../peer/PeerLinks.ts";
import * as Peers from "../peer/PeerMcpClient.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Persistence from "./remoteSecretPersistence.ts";

const decodeSchedule = Schema.decodeUnknownOption(OrchestratorMcpScheduleTaskInput);
const decodeReceiptQuery = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteSecretReceiptQuery),
);
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "The remote secret owner is unavailable.",
  });
const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "The secret request belongs to another run or retired generation.",
  });
interface Obligation {
  request_id: string;
  environment_id: EnvironmentId;
  generation: string;
  thread_id: ThreadId;
  run_id: string;
  turn_item_id: string;
  origin_json: string;
  create_json: string;
  authority_json: string;
  attempt_id: string;
  pending: number;
  ack_json: string | null;
}
export class RemoteSecretRelay extends Context.Service<
  RemoteSecretRelay,
  {
    readonly recover: (
      scope: McpInvocationScope,
      input: unknown,
    ) => Effect.Effect<Option.Option<McpScheduledMutationReceipt>, OrchestratorMcpFailure>;
    readonly create: (
      scope: McpInvocationScope,
      destination: EnvironmentId,
      input: RemoteSecretCreate,
      turnItemId: string,
      limits: import("../peer/PeerMcpClient.ts").PeerCallLimits,
    ) => Effect.Effect<RemoteSecretDestination, OrchestratorMcpFailure>;
    readonly answer: (
      input: SecretRequestAnswerInput,
      sessionId: AuthSessionId,
    ) => Effect.Effect<boolean, OrchestratorMcpFailure>;
    readonly retire: (
      threadId: ThreadId,
      turnItemId: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly handle: (
      threadId: ThreadId,
      turnItemId: string,
    ) => Effect.Effect<RemoteSecretHandle, OrchestratorMcpFailure>;
  }
>()("t3/secrets/RemoteSecretRelay") {}
const make = Effect.gen(function* () {
  const authority = yield* PeerAuthority.PeerAuthority;
  const links = yield* Links.PeerLinks;
  const peers = yield* Peers.PeerMcpClient;
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  yield* Persistence.schema;
  const control = (
    environmentId: EnvironmentId,
    generation: string,
    input?: {
      authority: import("@t3tools/contracts").PeerAuthorityEnvelope;
      body: RemoteSecretControl;
    },
  ) =>
    peers.remoteSecret === undefined
      ? Effect.fail(unavailable())
      : peers.remoteSecret(environmentId, generation, input);
  const owned = (threadId: ThreadId, turnItemId: string) =>
    sql<Obligation>`SELECT * FROM remote_secret_obligations WHERE thread_id=${threadId} AND turn_item_id=${turnItemId}`.pipe(
      Effect.map((rows) => rows[0]),
      Effect.mapError(unavailable),
    );
  return RemoteSecretRelay.of({
    recover: Effect.fn("RemoteSecretRelay.recover")(function* (scope, raw) {
      const decoded = decodeSchedule(raw);
      if (
        Option.isNone(decoded) ||
        scope.thread === undefined ||
        !scope.capabilities.has("orchestration")
      )
        return Option.none();
      const input = decoded.value;
      if (
        input.schedule?.type !== "webhook" ||
        input.schedule.signature?.secretRequest === undefined ||
        input.clientRequestId === undefined
      )
        return Option.none();
      const row = (yield* sql<{
        input_digest: string;
        environment_id: EnvironmentId;
        generation: string;
        query_json: string;
      }>`SELECT * FROM remote_secret_consumers WHERE namespace=${requestNamespace(scope)} AND client_key=${input.clientRequestId}`.pipe(
        Effect.mapError(unavailable),
      ))[0];
      if (row === undefined) return Option.none();
      if (row.input_digest !== requestInputDigest(raw)) return yield* denied();
      if (peers.remoteSecretReceipt === undefined) return yield* unavailable();
      const query = yield* decodeReceiptQuery(row.query_json).pipe(Effect.mapError(unavailable));
      return yield* peers.remoteSecretReceipt(row.environment_id, row.generation, query);
    }),
    create: Effect.fn("RemoteSecretRelay.create")(
      function* (scope, destination, input, turnItemId, limits) {
        const link = yield* links.get(destination).pipe(Effect.mapError(unavailable));
        if (Option.isNone(link) || scope.thread === undefined) return yield* denied();
        yield* control(destination, link.value.launchIdentity);
        const envelope = yield* authority.prepare(
          scope,
          destination,
          link.value.launchIdentity,
          "remote_secret",
          remoteSecretIntent(input),
          limits,
          undefined,
          false,
          true,
        );
        const origin = yield* authority.secretOrigin(envelope.recordId);
        const reply = yield* control(destination, link.value.launchIdentity, {
          authority: envelope,
          body: input,
        });
        if (reply === undefined || reply.handle.environmentId !== destination)
          return yield* unavailable();
        const existing = yield* owned(scope.thread.threadId, turnItemId);
        if (existing !== undefined) {
          if (
            existing.request_id !== reply.handle.requestId ||
            existing.origin_json !== canonicalRequest(origin) ||
            existing.create_json !== canonicalRequest(input) ||
            existing.pending !== 0
          )
            return yield* denied();
        } else {
          const attemptId = Hex.encode(
            yield* crypto.randomBytes(16).pipe(Effect.mapError(unavailable)),
          );
          yield* sql`INSERT INTO remote_secret_obligations (request_id,environment_id,generation,thread_id,run_id,turn_item_id,origin_json,create_json,authority_json,attempt_id,expires_at) VALUES (${reply.handle.requestId},${destination},${link.value.launchIdentity},${scope.thread.threadId},${origin.runId},${turnItemId},${canonicalRequest(origin)},${canonicalRequest(input)},${canonicalRequest(envelope)},${attemptId},${(yield* Clock.currentTimeMillis) + 24 * 60 * 60 * 1000})`.pipe(
            Effect.mapError(unavailable),
          );
        }
        yield* authority.notifyAutomationChanges;
        return { ...reply.handle, environmentLabel: link.value.label, projectId: input.projectId };
      },
    ),
    answer: Effect.fn("RemoteSecretRelay.answer")(function* (input, sessionId) {
      const row = yield* owned(input.threadId, input.turnItemId);
      if (row === undefined) return false;
      if (row.pending !== 0 || row.ack_json !== null) return yield* denied();
      const values =
        input.answer.type === "decline"
          ? []
          : "secret" in input.answer
            ? [input.answer.secret]
            : Object.values(input.answer.secrets);
      if (input.answer.type === "save" && values.length !== 1) return yield* denied();
      const body: RemoteSecretControl = {
        type: "answer",
        requestId: row.request_id,
        attemptId: row.attempt_id,
        answer: input.answer.type,
        ...(values[0] === undefined ? {} : { value: values[0] }),
      };
      const envelope = yield* authority.prepareSecretAnswer(
        sessionId,
        row.environment_id,
        row.generation,
        body,
      );
      const reply = yield* control(row.environment_id, row.generation, {
        authority: envelope,
        body,
      });
      if (
        reply === undefined ||
        !["saved", "reserved", "consumed", "declined"].includes(reply.status)
      )
        return yield* denied();
      return true;
    }),
    retire: Effect.fn("RemoteSecretRelay.retire")(function* (threadId, turnItemId) {
      yield* sql`UPDATE remote_secret_obligations SET pending=1 WHERE thread_id=${threadId} AND turn_item_id=${turnItemId} AND ack_json IS NULL`.pipe(
        Effect.mapError(unavailable),
      );
      yield* authority.notifyAutomationChanges;
    }),
    handle: Effect.fn("RemoteSecretRelay.handle")(function* (threadId, turnItemId) {
      const row = yield* owned(threadId, turnItemId);
      if (row === undefined || row.pending !== 0) return yield* denied();
      return { environmentId: row.environment_id, requestId: row.request_id };
    }),
  });
});
export const layer = Layer.effect(RemoteSecretRelay, make);
