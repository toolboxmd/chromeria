import { McpScheduledMutationReceipt, type RemoteSecretReceiptQuery } from "@t3tools/contracts";
import * as RemoteSecretPersistence from "../secrets/remoteSecretPersistence.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { RemoteSecretRetirementAck, RemoteSecretOrigin } from "@t3tools/contracts";
import {
  RemoteSecretSupport,
  RemoteSecretResult,
  type RemoteSecretControl,
} from "@t3tools/contracts";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import {
  DeviceViewerAccess,
  DeviceViewerProtocol,
  type DeviceViewerOpenInput,
  PreviewViewerAccess,
  PreviewViewerProtocol,
  type PreviewViewerOpenInput,
  type PeerAttachmentAssetResource,
  type NativeBrowserDownloadAssetResource,
} from "@t3tools/contracts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import {
  PeerSavedPreviewAuthorizationRequest,
  PeerSavedPreviewAuthorizationResult,
  PeerSavedPreviewAuthorizationSupport,
} from "@t3tools/contracts";
import { PeerTaskLifecycleRequest, PeerTaskLifecycleResult } from "@t3tools/contracts";
import * as PeerLinkEvents from "./PeerLinkEvents.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Stream from "effect/Stream";
import { forkParked } from "../serverActivation.ts";
import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type PeerLinkError,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as PubSub from "effect/PubSub";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as HttpBody from "effect/http/HttpBody";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerNoticeDelivery from "./PeerNoticeDelivery.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerAuthority from "./PeerAuthority.ts";
import { PeerAuthorityEnvelope } from "@t3tools/contracts";
import { PeerRequestProtocol } from "@t3tools/contracts";

/** The only MCP protocol version a T3 Code `/mcp` registers. */
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRequestProtocol = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerRequestProtocol),
);
const decodeSavedPreviewSupport = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerSavedPreviewAuthorizationSupport),
);
const decodeSavedPreviewResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerSavedPreviewAuthorizationResult),
);

/** Credential replies are small JSON, never an unbounded media/body transport. */
const deviceViewerReply = (response: HttpClientResponse.HttpClientResponse) =>
  response.stream.pipe(
    Stream.runFoldEffect(
      () => ({ size: 0, chunks: new Array<Uint8Array>() }),
      (state, chunk) => {
        if (state.size + chunk.byteLength > 16 * 1024)
          return Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The device viewer credential reply exceeded its bound.",
            }),
          );
        return Effect.sync(() => {
          state.size += chunk.byteLength;
          state.chunks.push(chunk);
          return state;
        });
      },
    ),
    Effect.map((state) => {
      const bytes = new Uint8Array(state.size);
      let offset = 0;
      for (const chunk of state.chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return new TextDecoder().decode(bytes);
    }),
  );
const decodeAutomationDisableAck = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AutomationAuthority.AutomationDisableAck),
);
const decodeAutomationSupport = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ version: Schema.Literal(1) })),
);

const decodeSecretEnvelope = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerAuthorityEnvelope),
);
const decodeSecretOrigin = Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteSecretOrigin));
const decodeSecretRetirementAck = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteSecretRetirementAck),
);
const decodeSecretReceipt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(McpScheduledMutationReceipt),
);
const decodeSecretSupport = Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteSecretSupport));
const decodeSecretResult = Schema.decodeUnknownEffect(Schema.fromJsonString(RemoteSecretResult));
const PROTOCOL_VERSION = "2025-06-18";

/** The limits a call carries to the peer: the calling agent's modes here. */
export interface PeerCallLimits {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

/**
 * Calls a linked environment's T3 MCP tools as this environment's outside
 * agent. Each call carries the calling agent's limits in `T3-Mode-Limit`, so
 * the peer enforces the narrower of those and the link's own access.
 */
export class PeerMcpClient extends Context.Service<
  PeerMcpClient,
  {
    readonly remoteSecretReceipt?: (
      environmentId: EnvironmentId,
      generation: string,
      query: RemoteSecretReceiptQuery,
    ) => Effect.Effect<
      Option.Option<import("@t3tools/contracts").McpScheduledMutationReceipt>,
      OrchestratorMcpFailure
    >;
    readonly remoteSecret?: (
      environmentId: EnvironmentId,
      generation: string,
      input?: { readonly authority: PeerAuthorityEnvelope; readonly body: RemoteSecretControl },
    ) => Effect.Effect<RemoteSecretResult | undefined, OrchestratorMcpFailure>;
    /** Server-private access. The destination ticket never crosses the source client boundary. */
    readonly closeDeviceViewer?: (input: {
      readonly environmentId: EnvironmentId;
      readonly generation: string;
      readonly viewerId: string;
    }) => Effect.Effect<boolean, OrchestratorMcpFailure>;
    readonly openDeviceViewer?: (input: {
      request: DeviceViewerOpenInput;
      authority: PeerAuthorityEnvelope;
    }) => Effect.Effect<
      { readonly origin: string; readonly access: DeviceViewerAccess },
      OrchestratorMcpFailure
    >;
    readonly openPreviewViewer?: (input: {
      request: PreviewViewerOpenInput;
      authority: PeerAuthorityEnvelope;
    }) => Effect.Effect<
      { readonly origin: string; readonly access: PreviewViewerAccess },
      OrchestratorMcpFailure
    >;
    readonly closePreviewViewer?: (input: {
      readonly environmentId: EnvironmentId;
      readonly generation: string;
      readonly viewerId: string;
    }) => Effect.Effect<boolean, OrchestratorMcpFailure>;
    readonly readNativeBrowserDownload?: (input: {
      readonly resource: NativeBrowserDownloadAssetResource;
      readonly authority: PeerAuthorityEnvelope;
      readonly authorize: Effect.Effect<void, OrchestratorMcpFailure>;
    }) => Effect.Effect<
      {
        readonly authorize: Effect.Effect<void, OrchestratorMcpFailure>;
        readonly readBlock: (offset: number) => Effect.Effect<Uint8Array, OrchestratorMcpFailure>;
      },
      OrchestratorMcpFailure
    >;
    readonly readAttachment?: (input: {
      readonly resource: PeerAttachmentAssetResource;
      readonly authority: PeerAuthorityEnvelope;
      readonly authorize: Effect.Effect<void, OrchestratorMcpFailure>;
    }) => Effect.Effect<
      {
        readonly bytes: Uint8Array;
        readonly producer: "device" | "preview";
        readonly authorize: Effect.Effect<void, OrchestratorMcpFailure>;
      },
      OrchestratorMcpFailure
    >;
    readonly authorizeSavedPreview?: (input: {
      request: PeerSavedPreviewAuthorizationRequest;
      authority: PeerAuthorityEnvelope;
      limits: PeerCallLimits;
    }) => Effect.Effect<PeerSavedPreviewAuthorizationResult, OrchestratorMcpFailure>;
    readonly requireRequestProtocol?: (
      environmentId: EnvironmentId,
      generation?: string,
    ) => Effect.Effect<PeerRequestProtocol, OrchestratorMcpFailure>;
    readonly uploadAttachment: (input: {
      readonly environmentId: EnvironmentId;
      readonly generation: string;
      readonly relativeUrl: string;
      readonly bytes: Stream.Stream<Uint8Array, unknown>;
      readonly sizeBytes: number;
    }) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
    readonly drainAutomationDisables?: Effect.Effect<void, OrchestratorMcpFailure>;
    readonly requirePendingRequests?: (
      environmentId: EnvironmentId,
      generation?: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly requireTaskLifecycle?: (
      environmentId: EnvironmentId,
      generation?: string,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly taskLifecycle?: (
      environmentId: EnvironmentId,
      generation: string,
      input: Omit<PeerTaskLifecycleRequest, "environmentId" | "generation">,
    ) => Effect.Effect<PeerTaskLifecycleResult, OrchestratorMcpFailure>;
    /** Calls `tool` on the peer and decodes its structured result with `success`. */
    readonly call: <S extends Schema.Decoder<unknown>>(input: {
      readonly environmentId: EnvironmentId;
      readonly tool: string;
      readonly arguments: Record<string, unknown>;
      readonly limits: PeerCallLimits;
      readonly success: S;
      /** Refuse a launch replay through a replacement OAuth grant. */
      readonly expectedLinkIdentity?: string;
      /** The calling Promachos thread here, when it answers a request there. */
      readonly promachosThreadId?: ThreadId;
      readonly authority?: PeerAuthorityEnvelope;
    }) => Effect.Effect<S["Type"], OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerMcpClient") {}

const ToolCallResponse = Schema.Struct({
  result: Schema.optional(
    Schema.Struct({
      content: Schema.Array(
        Schema.Struct({
          type: Schema.String,
          text: Schema.optional(Schema.String),
          data: Schema.optional(Schema.String),
          mimeType: Schema.optional(Schema.String),
        }),
      ),
      structuredContent: Schema.optional(Schema.Unknown),
      isError: Schema.optional(Schema.Boolean),
    }),
  ),
  error: Schema.optional(Schema.Struct({ code: Schema.Number, message: Schema.String })),
});
const decodeExchange = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerAuthority.ExchangeResult),
);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeToolCallResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(ToolCallResponse));
const decodeFailure = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      code: OrchestratorMcpFailure.fields.code,
      message: Schema.String,
      requestId: OrchestratorMcpFailure.fields.requestId,
      lastConfirmedStage: OrchestratorMcpFailure.fields.lastConfirmedStage,
    }),
  ),
);

/** A peer link problem, as the agent that asked hears it. */
const fromLinkError = (error: PeerLinkError) =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "unknown_link"
        ? "invalid_request"
        : error.reason === "expired" || error.reason === "incompatible"
          ? "capability_denied"
          : "orchestration_error",
    message: error.message,
  });

/** Not final, so a task already running there resumes once the beta is on there again. */
const linkedEnvironmentsOffThere = new OrchestratorMcpFailure({
  code: "orchestration_error",
  message:
    "The linked environment has Linked environments turned off. Turn it on in its Settings → Labs.",
});

/** The body of an MCP response: JSON, or a single server-sent event carrying JSON. */
const responseJson = (contentType: string | undefined, text: string) =>
  contentType?.includes("text/event-stream") === true
    ? text
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice("data:".length).trim())
        .join("")
    : text;

const make = Effect.gen(function* () {
  const secretSql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const links = yield* PeerLinks.PeerLinks;
  const httpClient = yield* HttpClient.HttpClient;
  const crypto = yield* Crypto.Crypto;
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const lifetime = yield* Scope.Scope;
  const delivery = yield* Effect.serviceOption(PeerNoticeDelivery.PeerNoticeDelivery);
  const channels = new Set<string>();
  const environment = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
  const changes = yield* Effect.serviceOption(PeerLinkEvents.PeerLinkEvents);
  // One MCP session per peer and address, reused across calls. The peer keeps
  // sessions in memory, so a restart there answers 404 and we start a new one.
  const sessions = yield* Ref.make<ReadonlyMap<string, string>>(new Map());

  const post = (url: string, token: string, body: unknown, headers: Record<string, string>) =>
    httpClient.execute(
      HttpClientRequest.post(`${url}/mcp`).pipe(
        HttpClientRequest.setHeaders({
          accept: "application/json, text/event-stream",
          authorization: `Bearer ${token}`,
          ...headers,
        }),
        HttpClientRequest.setBody(HttpBody.text(JSON.stringify(body), "application/json")),
      ),
    );

  const openSession = Effect.fn("PeerMcpClient.openSession")(function* (
    url: string,
    token: string,
    limitHeader: Record<string, string>,
  ) {
    const initialized = yield* post(
      url,
      token,
      {
        jsonrpc: "2.0",
        id: 0,
        method: "initialize",
        params: {
          protocolVersion: PROTOCOL_VERSION,
          capabilities: {},
          clientInfo: { name: "t3code-peer", version: "1" },
        },
        limitHeader,
      );
      const sessionId = initialized.headers["mcp-session-id"];
      if (initialized.status === 409 && limitHeader[PeerAuthority.LINK_HEADER] !== undefined)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The destination does not support peer authority v1; no action was forwarded.",
        });
      if (initialized.status !== 200 || sessionId === undefined) {
        const refusal = decodeFailure(yield* initialized.text);
        if (Option.isSome(refusal)) return yield* new OrchestratorMcpFailure(refusal.value);
        return yield* new OrchestratorMcpFailure({
          code: initialized.status === 401 ? "capability_denied" : "orchestration_error",
          message:
            initialized.status === 401
              ? "The linked environment no longer accepts this link. It may have been revoked there; link it again."
              : `The linked environment refused to start an MCP session (HTTP ${initialized.status}).`,
        });
      }
      if (
        limitHeader[PeerAuthority.LINK_HEADER] !== undefined &&
        initialized.headers[PeerAuthority.AUTHORITY_HEADER] !== "1"
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The destination does not support peer authority v1; no action was forwarded.",
        });
      yield* post(
        url,
        token,
        { jsonrpc: "2.0", method: "notifications/initialized" },
        { ...limitHeader, "mcp-session-id": sessionId, "mcp-protocol-version": PROTOCOL_VERSION },
      );
      return sessionId;
    });

  const ensureChannel = (resolved: PeerLinks.ResolvedPeerLink, self: EnvironmentId) =>
    Effect.gen(function* () {
      const key = `${resolved.link.environmentId}:${resolved.link.launchIdentity}`;
      if (Option.isNone(authority) || channels.has(key)) return;
      channels.add(key);
      const peerHeader = {
        environmentId: self,
        generation: resolved.link.launchIdentity,
        automationVersion: 1,
      };
      const handleFrame = (
        current: PeerLinks.ResolvedPeerLink,
        frame: (typeof PeerAuthority.ExchangeResult.Type)["frames"][number],
      ) =>
        Effect.gen(function* () {
          const answer = yield* authority.value
            .process(current.link.environmentId, current.link.launchIdentity, frame)
            .pipe(
              Effect.provideService(
                PeerNoticeDelivery.PeerNoticeDelivery,
                Option.isSome(delivery) ? delivery.value : PeerNoticeDelivery.unavailable,
              ),
            );
          const acknowledged = yield* httpClient.execute(
            HttpClientRequest.post(`${current.url}/mcp/peer-control`).pipe(
              HttpClientRequest.setHeaders({
                authorization: `Bearer ${current.token}`,
                [PeerAuthority.LINK_HEADER]: json(peerHeader),
              }),
              HttpClientRequest.setBody(
                HttpBody.text(
                  json({ ...peerHeader, answers: [answer], wait: false }),
                  "application/json",
                ),
              ),
            ),
          );
          if (acknowledged.status !== 200)
            return yield* new OrchestratorMcpFailure({
              code:
                acknowledged.status === 401 || acknowledged.status === 403
                  ? "capability_denied"
                  : "orchestration_error",
              message: "Peer control acknowledgement failed.",
            });
          yield* acknowledged.text;
        });
      // Intake keeps reading while earlier frames validate through another hop.
      // A batch boundary must never become the only reverse channel's lock.
      const polling = Effect.suspend(() => {
        let cursor = 0;
        return Stream.fromEffectRepeat(
          Effect.gen(function* () {
            const current = yield* links
              .resolve(resolved.link.environmentId)
              .pipe(Effect.mapError(fromLinkError));
            if (current.link.launchIdentity !== resolved.link.launchIdentity)
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "The link control generation was replaced.",
              });
            const response = yield* httpClient.execute(
              HttpClientRequest.post(`${current.url}/mcp/peer-control`).pipe(
                HttpClientRequest.setHeaders({
                  authorization: `Bearer ${current.token}`,
                  [PeerAuthority.LINK_HEADER]: json(peerHeader),
                }),
                HttpClientRequest.setBody(
                  HttpBody.text(
                    json({ ...peerHeader, answers: [], after: cursor }),
                    "application/json",
                  ),
                ),
              ),
            );
            if (response.status !== 200)
              return yield* new OrchestratorMcpFailure({
                code:
                  response.status === 401 || response.status === 403 || response.status === 409
                    ? "capability_denied"
                    : "orchestration_error",
                message: "The peer control channel is unavailable.",
              });
            const batch = yield* decodeExchange(yield* response.text);
            cursor = batch.cursor;
            return batch.frames.map((frame) => ({ current, frame }));
          }),
        ).pipe(
          Stream.flattenIterable,
          Stream.mapEffect(({ current, frame }) => handleFrame(current, frame), {
            concurrency: 8,
            unordered: true,
          }),
          Stream.runDrain,
        );
      });
      const streaming = Effect.gen(function* () {
        const current = yield* links
          .resolve(resolved.link.environmentId)
          .pipe(Effect.mapError(fromLinkError));
        if (current.link.launchIdentity !== resolved.link.launchIdentity)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The link control generation was replaced.",
          });
        const response = yield* httpClient
          .execute(
            HttpClientRequest.get(`${current.url}/mcp/peer-control/stream`).pipe(
              HttpClientRequest.setHeaders({
                authorization: `Bearer ${current.token}`,
                [PeerAuthority.LINK_HEADER]: json(peerHeader),
                accept: "text/event-stream",
              }),
            ),
          )
          .pipe(Effect.timeout("5 seconds"));
        if (
          response.status !== 200 ||
          response.headers["content-type"]?.includes("text/event-stream") !== true
        )
          return yield* new OrchestratorMcpFailure({
            code:
              response.status === 401 || response.status === 403
                ? "capability_denied"
                : "orchestration_error",
            message: "Peer streaming is unavailable.",
          });
        yield* response.stream.pipe(
          Stream.timeout("30 seconds"),
          Stream.decodeText(),
          Stream.splitLines,
          Stream.filter((line) => line.startsWith("data: ")),
          Stream.mapEffect((line) => decodeExchange(line.slice(6))),
          Stream.flatMap((batch) => Stream.fromIterable(batch.frames)),
          Stream.mapEffect((frame) => handleFrame(current, frame), {
            concurrency: 8,
            unordered: true,
          }),
          Stream.runDrain,
        );
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "Peer control stream closed.",
        });
      });
      yield* streaming.pipe(
        Effect.catch((error) =>
          error._tag === "SchemaError" ||
          (error._tag === "OrchestratorMcpFailure" && error.code === "capability_denied")
            ? Effect.fail(error)
            : polling,
        ),
        Effect.retry({
          schedule: Schedule.spaced("1 second"),
          while: (error) =>
            error._tag === "HttpClientError" ||
            (error._tag === "OrchestratorMcpFailure" && error.code === "orchestration_error"),
        }),
        Effect.ensuring(Effect.sync(() => channels.delete(key))),
        Effect.ignoreCause({ log: true }),
        Effect.forkIn(lifetime),
      );
    });
  const deliverSecretRetirements = Effect.gen(function* () {
    if (Option.isNone(secretSql) || Option.isNone(environment)) return false;
    const sql = secretSql.value;
    const initialized = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE name='remote_secret_obligations'`;
    if (initialized.length === 0) return false;
    yield* RemoteSecretPersistence.reconcileObligations.pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    if (Option.isSome(authority)) {
      const active = yield* sql<{
        request_id: string;
        authority_json: string;
      }>`SELECT request_id,authority_json FROM remote_secret_obligations WHERE pending=0 AND ack_json IS NULL`;
      for (const row of active) {
        const envelope = yield* decodeSecretEnvelope(row.authority_json);
        yield* authority.value
          .checkInteractiveRecord(envelope.recordId)
          .pipe(
            Effect.catch((error) =>
              error.code === "capability_denied"
                ? sql`UPDATE remote_secret_obligations SET pending=1 WHERE request_id=${row.request_id} AND ack_json IS NULL`.pipe(
                    Effect.asVoid,
                  )
                : Effect.void,
            ),
          );
      }
    }
    const pending = yield* sql<{
      request_id: string;
      environment_id: EnvironmentId;
      generation: string;
      origin_json: string;
    }>`SELECT request_id,environment_id,generation,origin_json FROM remote_secret_obligations WHERE pending=1 AND ack_json IS NULL`;
    for (const row of pending)
      yield* Effect.gen(function* () {
        if (links.resolveAutomationRecovery === undefined) return;
        const resolved = yield* links
          .resolveAutomationRecovery(row.environment_id, row.generation)
          .pipe(Effect.mapError(fromLinkError));
        const input = {
          version: 1 as const,
          environmentId: yield* environment.value.getEnvironmentId,
          generation: row.generation,
          requestId: row.request_id,
          origin: yield* decodeSecretOrigin(row.origin_json),
        };
        const response = yield* httpClient.execute(
          HttpClientRequest.post(`${resolved.url}/mcp/peer-control/secrets/retire`).pipe(
            HttpClientRequest.setHeaders({ authorization: `Bearer ${resolved.token}` }),
            HttpClientRequest.setBody(HttpBody.text(json(input), "application/json")),
          ),
        );
        if (response.status !== 200) return;
        const ack = yield* decodeSecretRetirementAck(yield* response.text);
        const { retired: _retired, ...identity } = ack;
        if (json(identity) !== json(input)) return;
        yield* sql`UPDATE remote_secret_obligations SET ack_json=${json(ack)} WHERE request_id=${row.request_id} AND generation=${row.generation} AND pending=1`;
      }).pipe(
        Effect.timeout("10 seconds"),
        Effect.catch(() => Effect.logWarning("Remote secret retirement remains unconfirmed.")),
      );
    return pending.length !== 0;
  });
  const deliverAutomationDisableBatch = Effect.gen(function* () {
    if (Option.isNone(authority)) return false;
    const secretPending = yield* deliverSecretRetirements.pipe(
      Effect.catch(() => Effect.succeed(true)),
    );
    const batch = yield* authority.value.pendingAutomationDisables;
    yield* Effect.forEach(
      batch,
      (pending) =>
        Effect.gen(function* () {
          if (links.resolveAutomationRecovery === undefined)
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The original disable-only recovery resolver is unavailable.",
            });
          const resolved = yield* links
            .resolveAutomationRecovery(pending.environmentId, pending.generation)
            .pipe(Effect.mapError(fromLinkError));
          const response = yield* httpClient.execute(
            HttpClientRequest.post(`${resolved.url}/mcp/peer-control/automation-disable`).pipe(
              HttpClientRequest.setHeaders({ authorization: `Bearer ${resolved.token}` }),
              HttpClientRequest.setBody(
                HttpBody.text(
                  json({ version: 1, dependency: pending.dependency }),
                  "application/json",
                ),
              ),
            ),
          );
          if (response.status !== 200) return;
          const acknowledgement = yield* decodeAutomationDisableAck(yield* response.text);
          if (json(acknowledgement.dependency) !== json(pending.dependency))
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message:
                "The automation acknowledgement does not match its exact original dependency.",
            });
          yield* authority.value.acknowledgeAutomationDisable(
            acknowledgement,
            pending.environmentId,
            pending.generation,
          );
        }).pipe(Effect.timeout("10 seconds"), Effect.ignoreCause({ log: true })),
      { concurrency: 4, discard: true },
    );
    return batch.length !== 0 || secretPending;
  });
  const drainAutomationDisables = deliverAutomationDisableBatch.pipe(Effect.asVoid);
  const disableWakeups = yield* Queue.sliding<void>(1);
  const recoverAutomationDisables = Queue.take(disableWakeups).pipe(
    Effect.andThen(
      Effect.forever(
        Effect.gen(function* () {
          if (yield* deliverAutomationDisableBatch) {
            yield* Effect.sleep("2 seconds");
            return;
          }
          const deadline = Option.isSome(authority)
            ? yield* authority.value.nextAutomationDisableDeadline
            : null;
          if (deadline === null) yield* Queue.take(disableWakeups);
          else
            yield* Effect.raceFirst(
              Queue.take(disableWakeups),
              Effect.sleep(Math.max(0, deadline - (yield* Clock.currentTimeMillis))),
            );
        }).pipe(
          Effect.catch(() =>
            Effect.logWarning("Pending automation disable recovery is unavailable; retrying.").pipe(
              Effect.andThen(Effect.sleep("2 seconds")),
            ),
          ),
        ),
      ),
    ),
  );

  const reconnect = (id: EnvironmentId) =>
    Effect.gen(function* () {
      if (Option.isNone(environment) || Option.isNone(authority)) return;
      const resolved = yield* links.resolve(id).pipe(Effect.mapError(fromLinkError));
      const self = yield* environment.value.getEnvironmentId;
      const peerHeader = {
        environmentId: self,
        generation: resolved.link.launchIdentity,
        automationVersion: 1,
      };
      yield* openSession(resolved.url, resolved.token, {
        [PeerAuthority.LINK_HEADER]: json(peerHeader),
      });
      yield* ensureChannel(resolved, self);
    });
  const start = Option.isNone(authority)
    ? Effect.void
    : forkParked(
        Effect.gen(function* () {
          if (Option.isSome(changes)) {
            const automationChanges = yield* changes.value.subscribeAutomationChanges;
            yield* Effect.forever(
              PubSub.take(automationChanges).pipe(
                Effect.andThen(Queue.offer(disableWakeups, undefined)),
              ),
            ).pipe(Effect.forkIn(lifetime));
          }
          yield* Queue.offer(disableWakeups, undefined);
          yield* recoverAutomationDisables.pipe(Effect.forkIn(lifetime));
          const stored = yield* links.list;
          const updates = Option.isSome(changes) ? changes.value.changes : Stream.empty;
          yield* Stream.merge(
            Stream.fromIterable(stored.map((link) => link.environmentId)),
            updates,
          ).pipe(
            Stream.runForEach((id) =>
              Queue.offer(disableWakeups, undefined)
                .pipe(Effect.andThen(reconnect(id)))
                .pipe(
                  Effect.retry({
                    schedule: Schedule.spaced("2 seconds"),
                    while: (error) =>
                      error._tag === "HttpClientError" ||
                      (error._tag === "OrchestratorMcpFailure" &&
                        error.code === "orchestration_error"),
                  }),
                  Effect.ignoreCause({ log: true }),
                  Effect.forkIn(lifetime),
                ),
            ),
          );
        }).pipe(Effect.ignoreCause({ log: true })),
      );

  const call: PeerMcpClient["Service"]["call"] = (input) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (
        input.expectedLinkIdentity !== undefined &&
        resolved.link.launchIdentity !== input.expectedLinkIdentity
      ) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "The destination was linked again. Its original launch identity cannot be replayed through the replacement link.",
        });
      }
      const peerHeader = {
        environmentId: input.authority?.environmentId,
        generation: resolved.link.launchIdentity,
        automationVersion: 1,
      };
      const limitHeader: Record<string, string> = {
        [McpInvocationContext.MODE_LIMIT_HEADER]: `${input.limits.runtimeMode}/${input.limits.interactionMode}`,
        ...(input.authority === undefined
          ? {}
          : {
              [PeerAuthority.LINK_HEADER]: json(peerHeader),
            }),
      };
      if (["schedule_task", "update_scheduled_task", "start_spectrum"].includes(input.tool)) {
        const support = yield* httpClient.execute(
          HttpClientRequest.get(`${resolved.url}/mcp/peer-control/automation`).pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${resolved.token}`,
              ...limitHeader,
            }),
          ),
        );
        if (support.status !== 200)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "The destination does not support destination automation authority v1; no definition was accepted.",
          });
        yield* decodeAutomationSupport(yield* support.text).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "The destination automation authority protocol is unsupported.",
              }),
          ),
        );
      }
      const sessionKey = `${input.environmentId} ${resolved.url} ${resolved.link.launchIdentity} ${input.authority === undefined ? "legacy" : "authority-v1"}`;
      const callOnce = (sessionId: string) =>
        post(
          resolved.url,
          resolved.token,
          {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: { name: input.tool, arguments: input.arguments },
          },
          {
            ...limitHeader,
            ...(input.promachosThreadId === undefined
              ? {}
              : { [McpInvocationContext.PROMACHOS_THREAD_HEADER]: input.promachosThreadId }),
            ...(input.authority === undefined
              ? {}
              : { [PeerAuthority.AUTHORITY_HEADER]: json(input.authority) }),
            "mcp-session-id": sessionId,
            "mcp-protocol-version": PROTOCOL_VERSION,
          },
        );
      const existing = (yield* Ref.get(sessions)).get(sessionKey);
      const sessionId = existing ?? (yield* openSession(resolved.url, resolved.token, limitHeader));
      if (input.authority !== undefined)
        yield* ensureChannel(resolved, input.authority.environmentId);

      let response = yield* callOnce(sessionId);
      if (response.status === 404) {
        const renewed = yield* openSession(resolved.url, resolved.token, limitHeader);
        yield* Ref.update(sessions, (current) => new Map(current).set(sessionKey, renewed));
        response = yield* callOnce(renewed);
      } else if (existing === undefined) {
        yield* Ref.update(sessions, (current) => new Map(current).set(sessionKey, sessionId));
      }
      if (response.status === 401) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
        });
      }
      if (response.status === 403) return yield* linkedEnvironmentsOffThere;
      if (response.status !== 200) {
        const refusal = decodeFailure(yield* response.text);
        if (Option.isSome(refusal)) return yield* new OrchestratorMcpFailure(refusal.value);
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: `The linked environment answered HTTP ${response.status}.`,
        });
      }
      const body = yield* decodeToolCallResponse(
        responseJson(response.headers["content-type"], yield* response.text),
      ).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The linked environment answered in an unexpected shape.",
            }),
        ),
      );
      if (body.error !== undefined || body.result === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: body.error?.message ?? "The linked environment returned no result.",
        });
      }
      const text = body.result.content.map((part) => part.text ?? "").join("");
      if (body.result.isError === true) {
        // A refusal keeps the peer's own code, so the agent sees why it was refused.
        const refused = decodeFailure(text);
        return yield* refused._tag === "Some"
          ? new OrchestratorMcpFailure(refused.value)
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: text || "The linked environment refused the call.",
            });
      }
      const success = Schema.fromJsonString(input.success);
      return yield* Schema.decodeEffect(success)(text).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message:
                "The linked environment's answer did not match this version. Update both environments.",
            }),
        ),
      );
    },
    (effect, input) =>
      effect.pipe(
        Effect.tapError((error) =>
          links.recordOutcome(input.environmentId, { error: error.message }),
        ),
        Effect.tap(() => links.recordOutcome(input.environmentId, { error: null })),
        // Recorded above with the transport's own message; the agent hears it plainly.
        Effect.catchTags({
          HttpClientError: () =>
            Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The linked environment stopped answering.",
              }),
            ),
        }),
      ),
  );

  const lifecycleRequest = (
    environmentId: EnvironmentId,
    generation?: string,
    input?: Omit<PeerTaskLifecycleRequest, "environmentId" | "generation">,
    pendingRequests = false,
  ) =>
    Effect.gen(function* () {
      const resolved = yield* links.resolve(environmentId).pipe(Effect.mapError(fromLinkError));
      if (generation !== undefined && generation !== resolved.link.launchIdentity)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The delegated task belongs to another link generation.",
        });
      if (Option.isNone(environment))
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The local environment identity is unavailable.",
        });
      const self = yield* environment.value.getEnvironmentId;
      const peer = { environmentId: self, generation: resolved.link.launchIdentity };
      // Stop validates the durable source obligation over this same authenticated
      // channel, including a first control operation after source restart.
      if (input?.operation.type === "stop") yield* ensureChannel(resolved, self);
      const request =
        input === undefined
          ? HttpClientRequest.get(
              `${resolved.url}/mcp/peer-control/${pendingRequests ? "pending-requests" : "tasks"}`,
            )
          : HttpClientRequest.post(`${resolved.url}/mcp/peer-control/tasks`).pipe(
              HttpClientRequest.setBody(
                HttpBody.text(json({ ...peer, ...input }), "application/json"),
              ),
            );
      const response = yield* httpClient.execute(
        request.pipe(HttpClientRequest.setHeaders({ authorization: `Bearer ${resolved.token}` })),
      );
      if (response.status === 404 || response.status === 409)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: pendingRequests
            ? "The destination does not support verified pending request actions. Update both environments; no request action was accepted."
            : "The destination does not support delegated lifecycle v1. Update both environments; no lifecycle operation was accepted.",
        });
      if (response.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code:
            response.status === 401 || response.status === 403
              ? "capability_denied"
              : "orchestration_error",
          message: "The delegated lifecycle operation could not be confirmed.",
        });
      const text = yield* response.text;
      const current = yield* links.get(environmentId).pipe(Effect.mapError(fromLinkError));
      if (Option.isNone(current) || current.value.launchIdentity !== resolved.link.launchIdentity)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "The delegated lifecycle reply belongs to a link generation that is no longer current.",
        });
      return text;
    }).pipe(
      Effect.timeout("30 seconds"),
      Effect.catchTags({
        TimeoutError: () =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The delegated lifecycle operation timed out without confirmation.",
            }),
          ),
        HttpClientError: () =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The delegated execution environment is unreachable.",
            }),
          ),
      }),
    );
  const decodeLifecycleVersion = Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Struct({ version: Schema.Literal(1) })),
  );
  const requireTaskLifecycle: NonNullable<PeerMcpClient["Service"]["requireTaskLifecycle"]> = (
    environmentId,
    generation,
  ) =>
    lifecycleRequest(environmentId, generation).pipe(
      Effect.flatMap(decodeLifecycleVersion),
      Effect.asVoid,
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "The destination does not support delegated lifecycle v1; no delegated work was created.",
            }),
      ),
    );
  const decodeLifecycle = Schema.decodeUnknownEffect(
    Schema.fromJsonString(PeerTaskLifecycleResult),
  );
  const taskLifecycle: NonNullable<PeerMcpClient["Service"]["taskLifecycle"]> = (
    environmentId,
    generation,
    input,
  ) =>
    lifecycleRequest(environmentId, generation, input).pipe(
      Effect.flatMap(decodeLifecycle),
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The destination lifecycle response is incompatible.",
            }),
      ),
    );
  const uploadAttachment: PeerMcpClient["Service"]["uploadAttachment"] = (input) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (
        resolved.link.launchIdentity !== input.generation ||
        !/^\/api\/attachments\/upload\/[A-Za-z0-9_.-]+$/.test(input.relativeUrl)
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The attachment transfer does not match its destination grant or upload route.",
        });
      const response = yield* httpClient
        .execute(
          HttpClientRequest.post(`${resolved.url}${input.relativeUrl}`).pipe(
            HttpClientRequest.setBody(
              HttpBody.stream(input.bytes, "application/octet-stream", input.sizeBytes),
            ),
          ),
        )
        .pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message:
                  "The attachment transfer response was lost; retry the original request key.",
              }),
          ),
        );
      if (response.status !== 204)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The destination did not accept the attachment bytes.",
        });
    });
  const requireRequestProtocol: NonNullable<PeerMcpClient["Service"]["requireRequestProtocol"]> = (
    environmentId,
    generation,
  ) =>
    Effect.gen(function* () {
      const resolved = yield* links.resolve(environmentId).pipe(Effect.mapError(fromLinkError));
      if (generation !== undefined && resolved.link.launchIdentity !== generation)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The original request grant was replaced.",
        });
      const response = yield* httpClient
        .execute(
          HttpClientRequest.get(`${resolved.url}/mcp/peer-control/requests`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${resolved.token}`),
          ),
        )
        .pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The request protocol could not be confirmed.",
              }),
          ),
        );
      if (response.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "The destination does not support durable requests v1; no mutation was forwarded.",
        });
      return yield* decodeRequestProtocol(
        yield* response.text.pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The request protocol response was lost.",
              }),
          ),
        ),
      ).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "The destination request protocol is incompatible; no mutation was forwarded.",
            }),
        ),
      );
    });
  const openViewer = (input: {
    request: DeviceViewerOpenInput | PreviewViewerOpenInput;
    authority: PeerAuthorityEnvelope;
  }) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.request.resource.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (resolved.link.launchIdentity !== input.authority.generation)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The device viewer link generation was replaced.",
        });
      const purpose = input.request.resource.type === "device" ? "device-viewer" : "preview-viewer";
      const endpoint = `${resolved.url}/mcp/peer-control/${purpose}`;
      const support = yield* httpClient.get(endpoint, {
        headers: { authorization: `Bearer ${resolved.token}` },
      });
      if (support.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code:
            support.status === 401 || support.status === 403
              ? "capability_denied"
              : "invalid_request",
          message:
            "The destination does not support authenticated device viewing; no viewer was admitted.",
        });
      yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(
          input.request.resource.type === "device" ? DeviceViewerProtocol : PreviewViewerProtocol,
        ),
      )(yield* deviceViewerReply(support)).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The destination device viewer protocol is unsupported.",
            }),
        ),
      );
      yield* ensureChannel(resolved, input.authority.environmentId);
      const response = yield* httpClient.execute(
        HttpClientRequest.post(endpoint).pipe(
          HttpClientRequest.setHeaders({
            authorization: `Bearer ${resolved.token}`,
            [PeerAuthority.LINK_HEADER]: json({
              environmentId: input.authority.environmentId,
              generation: input.authority.generation,
            }),
            [PeerAuthority.AUTHORITY_HEADER]: json(input.authority),
          }),
          HttpClientRequest.setBody(HttpBody.text(json(input.request), "application/json")),
        ),
      );
      const text = yield* deviceViewerReply(response);
      if (response.status !== 200) {
        const refusal = decodeFailure(text);
        if (Option.isSome(refusal)) return yield* new OrchestratorMcpFailure(refusal.value);
        return yield* new OrchestratorMcpFailure({
          code:
            response.status === 401 || response.status === 403
              ? "capability_denied"
              : "orchestration_error",
          message: "The destination refused the device viewer.",
        });
      }
      return { origin: resolved.url, text };
    }).pipe(
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
      Effect.mapError((error) =>
        Schema.is(OrchestratorMcpFailure)(error)
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The destination device viewer is unavailable.",
            }),
      ),
    );
  const closeViewer = (
    input: { environmentId: EnvironmentId; generation: string; viewerId: string },
    purpose: "device-viewer" | "preview-viewer",
  ) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (resolved.link.launchIdentity !== input.generation) return false;
      const response = yield* httpClient.execute(
        HttpClientRequest.post(`${resolved.url}/mcp/peer-control/${purpose}/close`).pipe(
          HttpClientRequest.setHeader("authorization", `Bearer ${resolved.token}`),
          HttpClientRequest.setBody(
            HttpBody.text(json({ viewerId: input.viewerId }), "application/json"),
          ),
        ),
      );
      if (response.status !== 200) return false;
      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Boolean))(
        yield* deviceViewerReply(response),
      );
    }).pipe(
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The destination viewer could not be released.",
          }),
      ),
    );
  const openDeviceViewer: NonNullable<PeerMcpClient["Service"]["openDeviceViewer"]> = (input) =>
    openViewer(input).pipe(
      Effect.flatMap(({ origin, text }) =>
        Schema.decodeUnknownEffect(Schema.fromJsonString(DeviceViewerAccess))(text).pipe(
          Effect.map((access) => ({ origin, access })),
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The destination device viewer response is invalid.",
              }),
          ),
        ),
      ),
    );
  const openPreviewViewer: NonNullable<PeerMcpClient["Service"]["openPreviewViewer"]> = (input) =>
    openViewer(input).pipe(
      Effect.flatMap(({ origin, text }) =>
        Schema.decodeUnknownEffect(Schema.fromJsonString(PreviewViewerAccess))(text).pipe(
          Effect.map((access) => ({ origin, access })),
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The destination browser viewer response is invalid.",
              }),
          ),
        ),
      ),
    );
  const closeDeviceViewer: NonNullable<PeerMcpClient["Service"]["closeDeviceViewer"]> = (input) =>
    closeViewer(input, "device-viewer");
  const closePreviewViewer: NonNullable<PeerMcpClient["Service"]["closePreviewViewer"]> = (input) =>
    closeViewer(input, "preview-viewer");
  const authorizeSavedPreview: NonNullable<PeerMcpClient["Service"]["authorizeSavedPreview"]> = (
    input,
  ) =>
    Effect.gen(function* () {
      const resolved = yield* links
        .resolve(input.request.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (resolved.link.launchIdentity !== input.request.generation)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The original saved-preview destination grant was replaced.",
        });
      const endpoint = `${resolved.url}/mcp/peer-control/saved-preview`;
      const support = yield* httpClient.execute(
        HttpClientRequest.get(endpoint).pipe(
          HttpClientRequest.setHeader("authorization", `Bearer ${resolved.token}`),
        ),
      );
      if (support.status === 404 || support.status === 409)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message:
            "The destination does not support saved-preview authorization v1; no renderer or save was requested.",
        });
      if (support.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code:
            support.status === 401 || support.status === 403
              ? "capability_denied"
              : "orchestration_error",
          message: "Saved-preview authority is unavailable.",
        });
      yield* decodeSavedPreviewSupport(yield* support.text).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The saved-preview authorization protocol is unsupported.",
            }),
        ),
      );
      yield* ensureChannel(resolved, input.authority.environmentId);
      const response = yield* httpClient.execute(
        HttpClientRequest.post(endpoint).pipe(
          HttpClientRequest.setHeaders({
            authorization: `Bearer ${resolved.token}`,
            [McpInvocationContext.MODE_LIMIT_HEADER]: `${input.limits.runtimeMode}/${input.limits.interactionMode}`,
            [PeerAuthority.LINK_HEADER]: json({
              environmentId: input.authority.environmentId,
              generation: input.request.generation,
            }),
            [PeerAuthority.AUTHORITY_HEADER]: json(input.authority),
          }),
          HttpClientRequest.setBody(HttpBody.text(json(input.request), "application/json")),
        ),
      );
      const text = yield* response.text;
      if (response.status !== 200) {
        const refusal = decodeFailure(text);
        if (Option.isSome(refusal)) return yield* new OrchestratorMcpFailure(refusal.value);
        return yield* new OrchestratorMcpFailure({
          code:
            response.status === 401 || response.status === 403
              ? "capability_denied"
              : "orchestration_error",
          message: "Saved-preview authority could not be confirmed.",
        });
      }
      const result = yield* decodeSavedPreviewResult(text).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The saved-preview authorization result is invalid.",
            }),
        ),
      );
      if (
        result.environmentId !== input.request.environmentId ||
        result.generation !== input.request.generation ||
        result.contextThreadId !== input.request.contextThreadId ||
        result.operationId !== input.request.operationId
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Saved-preview authorization belongs to a different context.",
        });
      return result;
    }).pipe(
      Effect.catchTags({
        HttpClientError: () =>
          Effect.fail(
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message:
                "Saved-preview authority is unavailable; no protected replay was authorized.",
            }),
          ),
      }),
    );
  const readNativeBrowserDownload: NonNullable<
    PeerMcpClient["Service"]["readNativeBrowserDownload"]
  > = (input) => {
    const refused = () =>
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The native browser download is unavailable to its original reader.",
      });
    const mapFailure = () => refused();
    const request = (offset?: number) =>
      Effect.gen(function* () {
        yield* input.authorize;
        const current = yield* links
          .resolve(input.resource.resource.environmentId)
          .pipe(Effect.mapError(fromLinkError));
        if (current.link.launchIdentity !== input.authority.generation) return yield* refused();
        const response = yield* httpClient.execute(
          HttpClientRequest.post(`${current.url}/mcp/peer-control/attachment`).pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${current.token}`,
              [PeerAuthority.LINK_HEADER]: json({
                environmentId: input.authority.environmentId,
                generation: input.authority.generation,
              }),
              [PeerAuthority.AUTHORITY_HEADER]: json(input.authority),
              ...(offset === undefined
                ? { "t3-attachment-check-only": "1" }
                : { "t3-download-offset": String(offset) }),
            }),
            HttpClientRequest.setBody(HttpBody.text(json(input.resource), "application/json")),
          ),
        );
        if (
          response.status !== (offset === undefined ? 204 : 200) ||
          response.headers["t3-attachment-producer"] !== "preview" ||
          response.headers["t3-download-digest"] !== input.resource.digest ||
          response.headers["t3-download-size"] !== String(input.resource.sizeBytes)
        )
          return yield* refused();
        return response;
      });
    return Effect.gen(function* () {
      yield* input.authorize;
      const resolved = yield* links
        .resolve(input.resource.resource.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (resolved.link.launchIdentity !== input.authority.generation) return yield* refused();
      const support = yield* httpClient.execute(
        HttpClientRequest.get(`${resolved.url}/mcp/peer-control/attachment`).pipe(
          HttpClientRequest.setHeader("authorization", `Bearer ${resolved.token}`),
        ),
      );
      if (support.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Native browser download streaming is unsupported by this peer.",
        });
      yield* deviceViewerReply(support).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.fromJsonString(
              Schema.Struct({
                version: Schema.Literal(1),
                purpose: Schema.Literal("attachment-read"),
                nativeBrowserDownloads: Schema.Literal(1),
              }),
            ),
          ),
        ),
      );
      yield* ensureChannel(resolved, input.authority.environmentId);
      const authorize = request().pipe(
        Effect.asVoid,
        Effect.mapError(mapFailure),
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
      );
      yield* authorize;
      return {
        authorize,
        readBlock: (offset: number) =>
          Effect.gen(function* () {
            if (
              !Number.isSafeInteger(offset) ||
              offset < 0 ||
              offset >= input.resource.sizeBytes ||
              offset % (1024 * 1024) !== 0
            )
              return yield* refused();
            const response = yield* request(offset);
            const expected = Math.min(1024 * 1024, input.resource.sizeBytes - offset);
            const bytes = new Uint8Array(expected);
            const size = yield* response.stream.pipe(
              Stream.runFoldEffect(
                () => 0,
                (size, chunk) => {
                  if (size + chunk.byteLength > expected) return Effect.fail(refused());
                  return input.authorize.pipe(
                    Effect.andThen(
                      Effect.sync(() => {
                        bytes.set(chunk, size);
                        return size + chunk.byteLength;
                      }),
                    ),
                  );
                },
              ),
            );
            if (
              size !== expected ||
              (yield* crypto.digest("SHA-256", bytes).pipe(Effect.map(Hex.encode))) !==
                response.headers["t3-download-block-digest"]
            )
              return yield* refused();
            yield* authorize;
            return bytes;
          }).pipe(
            Effect.mapError(mapFailure),
            Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
          ),
      };
    }).pipe(
      Effect.mapError(mapFailure),
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
    );
  };
  const readAttachment: NonNullable<PeerMcpClient["Service"]["readAttachment"]> = (input) =>
    Effect.gen(function* () {
      yield* input.authorize;
      const resolved = yield* links
        .resolve(input.resource.reference.environmentId)
        .pipe(Effect.mapError(fromLinkError));
      if (resolved.link.launchIdentity !== input.authority.generation)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The attachment destination generation was replaced.",
        });
      const endpoint = `${resolved.url}/mcp/peer-control/attachment`;
      const support = yield* httpClient.execute(
        HttpClientRequest.get(endpoint).pipe(
          HttpClientRequest.setHeader("authorization", `Bearer ${resolved.token}`),
        ),
      );
      if (support.status !== 200)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Qualified attachment access is unsupported by this peer.",
        });
      yield* deviceViewerReply(support).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.fromJsonString(
              Schema.Struct({
                version: Schema.Literal(1),
                purpose: Schema.Literal("attachment-read"),
              }),
            ),
          ),
        ),
      );
      yield* ensureChannel(resolved, input.authority.environmentId);
      yield* input.authorize;
      const response = yield* httpClient.execute(
        HttpClientRequest.post(endpoint).pipe(
          HttpClientRequest.setHeaders({
            authorization: `Bearer ${resolved.token}`,
            [PeerAuthority.LINK_HEADER]: json({
              environmentId: input.authority.environmentId,
              generation: input.authority.generation,
            }),
            [PeerAuthority.AUTHORITY_HEADER]: json(input.authority),
          }),
          HttpClientRequest.setBody(HttpBody.text(json(input.resource), "application/json")),
        ),
      );
      if (response.status !== 200) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The qualified attachment is unavailable to its current reader.",
        });
      }
      const producer = yield* Schema.decodeUnknownEffect(Schema.Literals(["device", "preview"]))(
        response.headers["t3-attachment-producer"],
      );
      const state = yield* response.stream.pipe(
        Stream.runFoldEffect(
          () => ({ size: 0, chunks: new Array<Uint8Array>() }),
          (state, chunk) => {
            if (state.size + chunk.byteLength > input.resource.reference.sizeBytes)
              return Effect.fail(
                new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "The attachment exceeded its verified byte count.",
                }),
              );
            return input.authorize.pipe(
              Effect.andThen(
                Effect.sync(() => {
                  state.size += chunk.byteLength;
                  state.chunks.push(chunk);
                  return state;
                }),
              ),
            );
          },
        ),
      );
      const bytes = new Uint8Array(state.size);
      let offset = 0;
      for (const chunk of state.chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      const digest = yield* crypto.digest("SHA-256", bytes).pipe(Effect.map(Hex.encode));
      if (
        state.size !== input.resource.reference.sizeBytes ||
        digest !== input.resource.reference.digest
      )
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The attachment bytes do not match their original manifest.",
        });
      const authorize = Effect.gen(function* () {
        yield* input.authorize;
        const current = yield* links
          .resolve(input.resource.reference.environmentId)
          .pipe(Effect.mapError(fromLinkError));
        if (current.link.launchIdentity !== input.authority.generation)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The attachment destination generation was replaced.",
          });
        const checked = yield* httpClient.execute(
          HttpClientRequest.post(`${current.url}/mcp/peer-control/attachment`).pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${current.token}`,
              [PeerAuthority.LINK_HEADER]: json({
                environmentId: input.authority.environmentId,
                generation: input.authority.generation,
              }),
              [PeerAuthority.AUTHORITY_HEADER]: json(input.authority),
              "t3-attachment-check-only": "1",
            }),
            HttpClientRequest.setBody(HttpBody.text(json(input.resource), "application/json")),
          ),
        );
        if (checked.status !== 204 || checked.headers["t3-attachment-producer"] !== producer)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The attachment reader was retired before publication.",
          });
      }).pipe(
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
        Effect.mapError((error) =>
          error._tag === "OrchestratorMcpFailure"
            ? error
            : new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Current attachment authority is unavailable.",
              }),
        ),
      );
      yield* authorize;
      return { bytes, authorize, producer };
    }).pipe(
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Qualified attachment transport is unavailable.",
            }),
      ),
    );
  return PeerMcpClient.of({
    remoteSecretReceipt: (environmentId, generation, query) =>
      Effect.gen(function* () {
        const resolved = yield* links.resolve(environmentId).pipe(Effect.mapError(fromLinkError));
        if (resolved.link.launchIdentity !== generation || Option.isNone(environment))
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Receipt recovery requires its original link generation.",
          });
        const response = yield* httpClient.execute(
          HttpClientRequest.post(`${resolved.url}/mcp/peer-control/secrets/receipt`).pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${resolved.token}`,
              [PeerAuthority.LINK_HEADER]: json({
                environmentId: yield* environment.value.getEnvironmentId,
                generation,
                automationVersion: 1,
              }),
            }),
            HttpClientRequest.setBody(HttpBody.text(json(query), "application/json")),
          ),
        );
        if (response.status === 204) return Option.none();
        if (response.status !== 200)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The original secret consumer receipt could not be recovered.",
          });
        return Option.some(
          yield* decodeSecretReceipt(yield* deviceViewerReply(response)).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "The recovered receipt was invalid.",
                }),
            ),
          ),
        );
      }).pipe(
        Effect.catchTags({
          HttpClientError: () =>
            Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The receipt owner is unreachable.",
              }),
            ),
        }),
      ),
    remoteSecret: (environmentId, generation, input) =>
      Effect.gen(function* () {
        if (Option.isNone(secretSql))
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Remote secret control requires its durable source retirement owner.",
          });
        const resolved = yield* links.resolve(environmentId).pipe(Effect.mapError(fromLinkError));
        if (resolved.link.launchIdentity !== generation || Option.isNone(environment))
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The secret request link generation changed.",
          });
        const self = yield* environment.value.getEnvironmentId;
        // Intake is independent of the first POST, so reverse validation can complete while it is open.
        yield* ensureChannel(resolved, self);
        const request =
          input === undefined
            ? HttpClientRequest.get(`${resolved.url}/mcp/peer-control/secrets`)
            : HttpClientRequest.post(`${resolved.url}/mcp/peer-control/secrets`).pipe(
                HttpClientRequest.setBody(HttpBody.text(json(input.body), "application/json")),
              );
        const response = yield* httpClient.execute(
          request.pipe(
            HttpClientRequest.setHeaders({
              authorization: `Bearer ${resolved.token}`,
              [PeerAuthority.LINK_HEADER]: json({
                environmentId: self,
                generation,
                automationVersion: 1,
              }),
              ...(input === undefined
                ? {}
                : { [PeerAuthority.AUTHORITY_HEADER]: json(input.authority) }),
            }),
          ),
        );
        if (response.status !== 200)
          return yield* new OrchestratorMcpFailure({
            code:
              response.status === 404 || response.status === 409
                ? "invalid_request"
                : response.status === 403 || response.status === 401
                  ? "capability_denied"
                  : "orchestration_error",
            message:
              "The destination could not confirm remote secret v1; no fallback prompt was created.",
          });
        const reply = yield* deviceViewerReply(response);
        if (input === undefined) {
          const support = yield* decodeSecretSupport(reply).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "The destination does not support remote secret v1.",
                }),
            ),
          );
          if (
            !support.families.includes("schedule.webhook-signature") ||
            !support.consumers.includes("schedule_task")
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The destination does not support this secret consumer.",
            });
          return undefined;
        }
        const current = yield* links.get(environmentId).pipe(Effect.mapError(fromLinkError));
        if (Option.isNone(current) || current.value.launchIdentity !== generation)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The secret reply belongs to a retired link generation.",
          });
        return yield* decodeSecretResult(reply).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The secret control reply was invalid.",
              }),
          ),
        );
      }).pipe(
        Effect.catchTags({
          HttpClientError: () =>
            Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The secret destination is unreachable.",
              }),
            ),
        }),
      ),
    readAttachment,
    readNativeBrowserDownload,
    call,
    start,
    drainAutomationDisables,
    requirePendingRequests: (environmentId, generation) =>
      lifecycleRequest(environmentId, generation, undefined, true).pipe(
        Effect.flatMap(decodeLifecycleVersion),
        Effect.asVoid,
        Effect.mapError((error) =>
          error._tag === "OrchestratorMcpFailure"
            ? error
            : new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "The peer does not support verified pending request actions.",
              }),
        ),
      ),
    requireTaskLifecycle,
    taskLifecycle,
    uploadAttachment,
    requireRequestProtocol,
    authorizeSavedPreview,
    openDeviceViewer,
    closeDeviceViewer,
    openPreviewViewer,
    closePreviewViewer,
  });
});

export const layer = Layer.effect(PeerMcpClient, make);

/**
 * The client built over the runtime services that own the environment identity
 * and the threads parent notices land in. The client reads both as optional
 * services, so built below the runtime it still type-checks and fails only on
 * its first remote call.
 */
export const layerOver = <ROut, E, RIn>(
  runtime: Layer.Layer<
    ROut | ServerEnvironment.ServerEnvironment | Threads.ThreadManagementService,
    E,
    RIn
  >,
) => layer.pipe(Layer.provide(PeerNoticeDelivery.layer), Layer.provideMerge(runtime));
