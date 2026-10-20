import type * as Scope from "effect/Scope";
import {
  AuthSessionId,
  AuthMcpGrantLimits,
  DeviceViewerResource,
  PreviewViewerResource,
  AuthStandardClientScopes,
  AuthEnvironmentScopes,
  type AuthClientMetadata,
  type AuthClientSession,
  type AuthEnvironmentScope,
  AuthWebSocketTicketUse,
  RuntimeMode,
  type ServerAuthSessionMethod,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Option from "effect/Option";

import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SessionRenewal from "./SessionRenewal.ts";
import { watchCrossProcessRevocations } from "./forkCrossProcessRevocation.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import {
  REUSABLE_DEV_SESSION_EXPIRES_AT,
  REUSABLE_DEV_SESSION_PREFIX,
  resolveReusableDevAuth,
} from "./ReusableDevAuth.ts";
import {
  base64UrlDecodeUtf8,
  base64UrlEncode,
  resolveLegacySessionCookieName,
  resolveSessionCookieName,
  signPayload,
  timingSafeEqualBase64Url,
} from "./utils.ts";

export interface IssuedSession {
  readonly sessionId: AuthSessionId;
  readonly token: string;
  readonly method: ServerAuthSessionMethod;
  readonly client: AuthClientMetadata;
  readonly expiresAt: DateTime.DateTime;
  readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
  readonly proofKeyThumbprint?: string;
  readonly renewAfter?: number;
}

export interface VerifiedSession {
  readonly sessionId: AuthSessionId;
  readonly token: string;
  readonly method: ServerAuthSessionMethod;
  readonly client: AuthClientMetadata;
  readonly expiresAt?: DateTime.DateTime;
  readonly subject: string;
  readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
  readonly proofKeyThumbprint?: string;
  readonly runtimeModeCeiling?: RuntimeMode;
  /** An MCP client that is another T3 Code environment's peer link. */
  readonly peerLink?: true;
  readonly mcpLimits?: AuthMcpGrantLimits;
}

export interface VerifiedWebSocketTicket extends VerifiedSession {
  readonly audience?: AuthWebSocketTicketUse;
}

export type SessionCredentialChange =
  | {
      readonly type: "clientUpserted";
      readonly clientSession: AuthClientSession;
    }
  | {
      readonly type: "clientRemoved";
      readonly sessionId: AuthSessionId;
    };

export class MalformedSessionTokenError extends Schema.TaggedError<MalformedSessionTokenError>()(
  "MalformedSessionTokenError",
  {},
) {
  override get message(): string {
    return "Malformed session token.";
  }
}

export class InvalidSessionTokenSignatureError extends Schema.TaggedError<InvalidSessionTokenSignatureError>()(
  "InvalidSessionTokenSignatureError",
  {},
) {
  override get message(): string {
    return "Invalid session token signature.";
  }
}

export class InvalidSessionTokenPayloadError extends Schema.TaggedError<InvalidSessionTokenPayloadError>()(
  "InvalidSessionTokenPayloadError",
  {
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Invalid session token payload.";
  }
}

export class SessionTokenExpiredError extends Schema.TaggedError<SessionTokenExpiredError>()(
  "SessionTokenExpiredError",
  {
    sessionId: AuthSessionId,
    expiresAt: Schema.DateTimeUtc,
    observedAt: Schema.DateTimeUtc,
  },
) {
  override get message(): string {
    return "Session token expired.";
  }
}

export class UnknownSessionTokenError extends Schema.TaggedError<UnknownSessionTokenError>()(
  "UnknownSessionTokenError",
  {
    sessionId: AuthSessionId,
  },
) {
  override get message(): string {
    return "Unknown session token.";
  }
}

export class SessionTokenRevokedError extends Schema.TaggedError<SessionTokenRevokedError>()(
  "SessionTokenRevokedError",
  {
    sessionId: AuthSessionId,
    revokedAt: Schema.DateTimeUtc,
  },
) {
  override get message(): string {
    return "Session token revoked.";
  }
}

export class InvalidSessionExpirationClaimError extends Schema.TaggedError<InvalidSessionExpirationClaimError>()(
  "InvalidSessionExpirationClaimError",
  {
    sessionId: AuthSessionId,
    expirationClaim: Schema.Number,
  },
) {
  override get message(): string {
    return "Invalid `exp` claim";
  }
}

export class MalformedWebSocketTokenError extends Schema.TaggedError<MalformedWebSocketTokenError>()(
  "MalformedWebSocketTokenError",
  {},
) {
  override get message(): string {
    return "Malformed websocket token.";
  }
}

export class InvalidWebSocketTokenSignatureError extends Schema.TaggedError<InvalidWebSocketTokenSignatureError>()(
  "InvalidWebSocketTokenSignatureError",
  {},
) {
  override get message(): string {
    return "Invalid websocket token signature.";
  }
}

export class InvalidWebSocketTokenPayloadError extends Schema.TaggedError<InvalidWebSocketTokenPayloadError>()(
  "InvalidWebSocketTokenPayloadError",
  {
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Invalid websocket token payload.";
  }
}

export class WebSocketTokenExpiredError extends Schema.TaggedError<WebSocketTokenExpiredError>()(
  "WebSocketTokenExpiredError",
  {
    sessionId: AuthSessionId,
    expiresAt: Schema.DateTimeUtc,
    observedAt: Schema.DateTimeUtc,
  },
) {
  override get message(): string {
    return "Websocket token expired.";
  }
}

export class UnknownWebSocketSessionError extends Schema.TaggedError<UnknownWebSocketSessionError>()(
  "UnknownWebSocketSessionError",
  {
    sessionId: AuthSessionId,
  },
) {
  override get message(): string {
    return "Unknown websocket session.";
  }
}

export class WebSocketSessionExpiredError extends Schema.TaggedError<WebSocketSessionExpiredError>()(
  "WebSocketSessionExpiredError",
  {
    sessionId: AuthSessionId,
    expiresAt: Schema.DateTimeUtc,
    observedAt: Schema.DateTimeUtc,
  },
) {
  override get message(): string {
    return "Websocket session expired.";
  }
}

export class WebSocketSessionRevokedError extends Schema.TaggedError<WebSocketSessionRevokedError>()(
  "WebSocketSessionRevokedError",
  {
    sessionId: AuthSessionId,
    revokedAt: Schema.DateTimeUtc,
  },
) {
  override get message(): string {
    return "Websocket session revoked.";
  }
}

export const SessionCredentialInvalidError = Schema.Union([
  MalformedSessionTokenError,
  InvalidSessionTokenSignatureError,
  InvalidSessionTokenPayloadError,
  SessionTokenExpiredError,
  UnknownSessionTokenError,
  SessionTokenRevokedError,
  InvalidSessionExpirationClaimError,
  MalformedWebSocketTokenError,
  InvalidWebSocketTokenSignatureError,
  InvalidWebSocketTokenPayloadError,
  WebSocketTokenExpiredError,
  UnknownWebSocketSessionError,
  WebSocketSessionExpiredError,
  WebSocketSessionRevokedError,
]);
export type SessionCredentialInvalidError = typeof SessionCredentialInvalidError.Type;
export const isSessionCredentialInvalidError = Schema.is(SessionCredentialInvalidError);

const sessionCredentialInternalErrorContext = {
  cause: Schema.Defect(),
};

export class SessionClaimsEncodingError extends Schema.TaggedError<SessionClaimsEncodingError>()(
  "SessionClaimsEncodingError",
  {
    sessionId: AuthSessionId,
    operation: Schema.Literals(["encode_session_claims", "encode_websocket_claims"]),
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to encode claims";
  }
}

export class SessionCredentialIssueError extends Schema.TaggedError<SessionCredentialIssueError>()(
  "SessionCredentialIssueError",
  {
    sessionId: Schema.optional(AuthSessionId),
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to issue session credential.";
  }
}

export class SessionCredentialVerificationError extends Schema.TaggedError<SessionCredentialVerificationError>()(
  "SessionCredentialVerificationError",
  {
    sessionId: AuthSessionId,
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to verify session credential.";
  }
}

export class WebSocketTokenIssueError extends Schema.TaggedError<WebSocketTokenIssueError>()(
  "WebSocketTokenIssueError",
  {
    sessionId: AuthSessionId,
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to issue websocket token.";
  }
}

export class WebSocketTokenVerificationError extends Schema.TaggedError<WebSocketTokenVerificationError>()(
  "WebSocketTokenVerificationError",
  {
    sessionId: AuthSessionId,
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to verify websocket token.";
  }
}

export class ActiveSessionsListError extends Schema.TaggedError<ActiveSessionsListError>()(
  "ActiveSessionsListError",
  {
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to list active sessions.";
  }
}

export class SessionRevocationError extends Schema.TaggedError<SessionRevocationError>()(
  "SessionRevocationError",
  {
    sessionId: AuthSessionId,
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to revoke session.";
  }
}

export class OtherSessionsRevocationError extends Schema.TaggedError<OtherSessionsRevocationError>()(
  "OtherSessionsRevocationError",
  {
    currentSessionId: AuthSessionId,
    ...sessionCredentialInternalErrorContext,
  },
) {
  override get message(): string {
    return "Failed to revoke other sessions.";
  }
}

export const SessionCredentialInternalError = Schema.Union([
  SessionClaimsEncodingError,
  SessionCredentialIssueError,
  SessionCredentialVerificationError,
  WebSocketTokenIssueError,
  WebSocketTokenVerificationError,
  ActiveSessionsListError,
  SessionRevocationError,
  OtherSessionsRevocationError,
]);
export type SessionCredentialInternalError = typeof SessionCredentialInternalError.Type;

export const SessionCredentialError = Schema.Union([
  SessionCredentialInvalidError,
  SessionCredentialInternalError,
]);
export type SessionCredentialError = typeof SessionCredentialError.Type;

const ViewerResource = Schema.Union([DeviceViewerResource, PreviewViewerResource]);
export type ViewerResource = typeof ViewerResource.Type;
export type ViewerOperation = "view" | "control";
type AuthenticatedViewer = {
  readonly session: VerifiedSession;
  readonly viewerId: string;
  readonly expiresAt: DateTime.DateTime;
  readonly resource: ViewerResource;
  readonly operation: ViewerOperation;
  readonly generation: string;
};

const ViewerClaims = Schema.Struct({
  v: Schema.Literal(1),
  kind: Schema.Literal("viewer"),
  sid: AuthSessionId,
  viewerId: Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/)),
  resource: ViewerResource,
  operation: Schema.Literals(["view", "control"]),
  generation: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  exp: Schema.Number,
});
const viewerClaimsJson = Schema.fromJsonString(ViewerClaims);
const decodeViewerClaims = Schema.decodeUnknownEffect(viewerClaimsJson);
const encodeViewerClaims = Schema.encodeEffect(viewerClaimsJson);
const encodeViewerResource = Schema.encodeSync(Schema.fromJsonString(ViewerResource));

export class SessionStore extends Context.Service<
  SessionStore,
  {
    readonly cookieName: string;
    readonly legacyCookieName: string | undefined;
    readonly issue: (input?: {
      readonly renewalCommitment?: string;
      readonly ttl?: Duration.Duration;
      /** Absolute authority deadline, used for outside grants that last until revoked. */
      readonly expiresAt?: DateTime.DateTime;
      readonly subject?: string;
      readonly method?: ServerAuthSessionMethod;
      readonly scopes?: ReadonlyArray<AuthEnvironmentScope>;
      readonly client?: AuthClientMetadata;
      readonly proofKeyThumbprint?: string;
      readonly runtimeModeCeiling?: RuntimeMode;
      readonly peerLink?: boolean;
      readonly mcpLimits?: AuthMcpGrantLimits;
      /**
       * Atomically revoke active sessions with the same subject and method
       * before storing this session.
       */
      readonly replaceActiveForSubjectAndMethod?: boolean;
      readonly replaceSessionId?: AuthSessionId;
    }) => Effect.Effect<IssuedSession, SessionCredentialInternalError>;
    readonly issueMcpOAuthTokens: (
      token: string,
      clientId: string,
      resource: string,
    ) => Effect.Effect<
      { token: string; refreshToken: string; expiresAt: DateTime.DateTime },
      SessionCredentialError
    >;
    readonly refreshMcpOAuthToken: (
      token: string,
      clientId: string,
      resource: string,
    ) => Effect.Effect<
      {
        token: string;
        refreshToken: string;
        expiresAt: DateTime.DateTime;
        scopes: ReadonlyArray<AuthEnvironmentScope>;
      },
      SessionCredentialError
    >;
    readonly verify: (token: string) => Effect.Effect<VerifiedSession, SessionCredentialError>;
    readonly renewSession: (input: { secret: string; next: string }) => Effect.Effect<
      {
        token: string;
        method: ServerAuthSessionMethod;
        expiresAt: DateTime.DateTime;
        generation: number;
        outcome: "current" | "rotated";
        renewAfter: number;
      },
      | SessionRenewal.RenewalRejected
      | SessionRenewal.RenewalStale
      | SessionRenewal.RenewalPending
      | SessionRenewal.RenewalStorageError
      | SessionCredentialInternalError
    >;

    /** Resource owners authorize first. This credential authenticates only that exact viewer. */
    readonly issueViewerToken: (input: {
      readonly viewerId?: string;
      readonly sessionId: AuthSessionId;
      readonly resource: ViewerResource;
      readonly operation: ViewerOperation;
      readonly generation: string;
    }) => Effect.Effect<
      {
        readonly token: string;
        readonly viewerId: string;
        readonly expiresAt: DateTime.DateTime;
      },
      SessionCredentialError
    >;
    readonly closeViewer: (sessionId: AuthSessionId, viewerId: string) => Effect.Effect<boolean>;
    readonly awaitViewerInvalidation: (
      viewerId: string,
    ) => Effect.Effect<void, SessionCredentialVerificationError>;
    readonly authenticateViewerToken: (
      token: string,
    ) => Effect.Effect<AuthenticatedViewer, SessionCredentialError>;
    /** Admission expires quickly; an admitted transport keeps its current session until scope release. */
    readonly admitViewerToken: (
      token: string,
    ) => Effect.Effect<
      AuthenticatedViewer & { readonly recheck: Effect.Effect<void, SessionCredentialError> },
      SessionCredentialError,
      Scope.Scope
    >;
    /** Never accepted by ordinary HTTP or websocket session authentication. */
    readonly verifyViewerToken: (input: {
      readonly token: string;
      readonly resource: ViewerResource;
      readonly operation: ViewerOperation;
      readonly generation: string;
    }) => Effect.Effect<
      {
        readonly session: VerifiedSession;
        readonly viewerId: string;
        readonly expiresAt: DateTime.DateTime;
      },
      SessionCredentialError
    >;
    readonly issueWebSocketToken: (
      sessionId: AuthSessionId,
      input?: {
        readonly ttl?: Duration.Duration;
        readonly audience?: AuthWebSocketTicketUse;
        readonly proofKeyThumbprint?: string;
      },
    ) => Effect.Effect<
      {
        readonly token: string;
        readonly expiresAt: DateTime.DateTime;
      },
      SessionCredentialInternalError
    >;
    readonly verifyWebSocketToken: (
      token: string,
    ) => Effect.Effect<VerifiedWebSocketTicket, SessionCredentialError>;
    readonly getPerson: (
      sessionId: AuthSessionId,
    ) => Effect.Effect<string | null, SessionCredentialInternalError>;
    readonly setPerson: (
      sessionId: AuthSessionId,
      person: string | null,
    ) => Effect.Effect<boolean, SessionCredentialInternalError>;
    readonly listActive: () => Effect.Effect<
      ReadonlyArray<AuthClientSession>,
      SessionCredentialInternalError
    >;
    readonly streamChanges: Stream.Stream<SessionCredentialChange>;
    readonly awaitInvalidation: (
      sessionId: AuthSessionId,
    ) => Effect.Effect<void, SessionCredentialVerificationError>;
    readonly revoke: (
      sessionId: AuthSessionId,
    ) => Effect.Effect<boolean, SessionCredentialInternalError>;
    readonly revokeAllExcept: (
      sessionId: AuthSessionId,
    ) => Effect.Effect<number, SessionCredentialInternalError>;
    readonly markConnected: (sessionId: AuthSessionId) => Effect.Effect<void, never>;
    readonly markDisconnected: (sessionId: AuthSessionId) => Effect.Effect<void, never>;
  }
>()("t3/auth/SessionStore") {}

const SIGNING_SECRET_NAME = "server-signing-key";
const DEFAULT_SESSION_TTL = Duration.days(30);
const DEFAULT_WEBSOCKET_TOKEN_TTL = Duration.minutes(5);

const SessionClaims = Schema.Struct({
  v: Schema.Literals([1, 2]),
  kind: Schema.Literal("session"),
  sid: AuthSessionId,
  sub: Schema.String,
  scopes: AuthEnvironmentScopes,
  method: Schema.Literals(["browser-session-cookie", "bearer-access-token", "dpop-access-token"]),
  jkt: Schema.optionalKey(Schema.String),
  rtc: Schema.optionalKey(RuntimeMode),
  /** Signed, so a client cannot shed its peer link's limits. */
  plk: Schema.optionalKey(Schema.Literal(1)),
  mcp: Schema.optionalKey(AuthMcpGrantLimits),
  iat: Schema.Number,
  exp: Schema.Number,
});
type SessionClaims = typeof SessionClaims.Type;

const WebSocketClaims = Schema.Struct({
  v: Schema.Literal(1),
  kind: Schema.Literal("websocket"),
  sid: AuthSessionId,
  aud: Schema.optionalKey(AuthWebSocketTicketUse),
  jkt: Schema.optionalKey(Schema.String),
  iat: Schema.Number,
  exp: Schema.Number,
});
type WebSocketClaims = typeof WebSocketClaims.Type;

const decodeSessionClaims = Schema.decodeUnknownEffect(Schema.fromJsonString(SessionClaims));
const decodeWebSocketClaims = Schema.decodeUnknownEffect(Schema.fromJsonString(WebSocketClaims));

function createDefaultClientMetadata(): AuthClientMetadata {
  return {
    deviceType: "unknown",
  };
}

function toClientMetadata(record: {
  readonly label: string | null;
  readonly ipAddress: string | null;
  readonly userAgent: string | null;
  readonly deviceType: AuthClientMetadata["deviceType"];
  readonly os: string | null;
  readonly browser: string | null;
}): AuthClientMetadata {
  return {
    ...(record.label ? { label: record.label } : {}),
    ...(record.ipAddress ? { ipAddress: record.ipAddress } : {}),
    ...(record.userAgent ? { userAgent: record.userAgent } : {}),
    deviceType: record.deviceType,
    ...(record.os ? { os: record.os } : {}),
    ...(record.browser ? { browser: record.browser } : {}),
  };
}

function toAuthClientSession(input: Omit<AuthClientSession, "current">): AuthClientSession {
  return {
    ...input,
    current: false,
  };
}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const serverEnvironment = yield* ServerEnvironment.ServerEnvironmentIdentity;
  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  const authSessions = yield* AuthSessions.AuthSessionRepository;
  const sql = yield* SqlClient.SqlClient;
  const renewal = yield* SessionRenewal.SessionRenewal;
  const signingSecret = yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32);
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`CREATE TABLE IF NOT EXISTS auth_mcp_refresh (
      session_id TEXT PRIMARY KEY REFERENCES auth_sessions(session_id),
      token_hash TEXT NOT NULL, previous_hash TEXT, previous_valid_until INTEGER
    )`;
      const columns = yield* sql<{ name: string }>`PRAGMA table_info(auth_mcp_refresh)`;
      if (!columns.some((column) => column.name === "previous_hash"))
        yield* sql`ALTER TABLE auth_mcp_refresh ADD COLUMN previous_hash TEXT`;
      if (!columns.some((column) => column.name === "previous_valid_until"))
        yield* sql`ALTER TABLE auth_mcp_refresh ADD COLUMN previous_valid_until INTEGER`;
    }),
  );
  const connectedSessionsRef = yield* Ref.make(new Map<AuthSessionId, number>());
  // Sessions with a running awaitInvalidation; each registers before its first row read.
  const watchedSessionsRef = yield* SubscriptionRef.make<ReadonlyMap<AuthSessionId, number>>(
    new Map(),
  );
  const changesPubSub = yield* PubSub.unbounded<SessionCredentialChange>();
  const cookieInput = {
    mode: serverConfig.mode,
    port: serverConfig.port,
    host: serverConfig.host,
    instanceKey: serverConfig.stateDir,
    environmentId: yield* serverEnvironment.getEnvironmentId,
    development: serverConfig.devUrl !== undefined,
  } as const;
  const cookieName = resolveSessionCookieName(cookieInput);
  const legacyCookieName = resolveLegacySessionCookieName(cookieInput);
  const devAuth = resolveReusableDevAuth(serverConfig);
  if (devAuth) {
    yield* authSessions
      .createIfAbsent({
        sessionId: devAuth.sessionId,
        subject: "reusable-dev-token",
        scopes: AuthStandardClientScopes,
        method: "browser-session-cookie",
        client: {
          label: "Reusable dev token",
          ipAddress: null,
          userAgent: null,
          deviceType: "unknown",
          os: null,
          browser: null,
        },
        issuedAt: yield* DateTime.now,
        expiresAt: REUSABLE_DEV_SESSION_EXPIRES_AT,
      })
      .pipe(
        Effect.mapError(
          (cause) => new SessionCredentialIssueError({ sessionId: devAuth.sessionId, cause }),
        ),
      );
  }

  const emitUpsert = (clientSession: AuthClientSession) =>
    PubSub.publish(changesPubSub, {
      type: "clientUpserted",
      clientSession,
    }).pipe(Effect.asVoid);

  const emitRemoved = (sessionId: AuthSessionId) =>
    PubSub.publish(changesPubSub, {
      type: "clientRemoved",
      sessionId,
    }).pipe(Effect.asVoid);

  // Reads run on the WAL reader so a lock held by another process cannot stall the writer.
  yield* watchCrossProcessRevocations({
    source: {
      dataVersion: sql<{ readonly data_version: number }>`PRAGMA data_version`.pipe(
        Effect.map((rows) => rows[0]?.data_version ?? 0),
        NodeSqliteClient.readOnly,
      ),
      listActive: (sessionIds) =>
        sql<{ readonly sessionId: AuthSessionId }>`
          SELECT session_id AS "sessionId"
          FROM auth_sessions
          WHERE ${sql.in("session_id", sessionIds)} AND revoked_at IS NULL
        `.pipe(
          Effect.map((rows) => rows.map((row) => row.sessionId)),
          NodeSqliteClient.readOnly,
        ),
    },
    watchedSessions: watchedSessionsRef,
    invalidate: emitRemoved,
  }).pipe(Effect.forkScoped);

  const loadActiveSession = Effect.fn("SessionStore.loadActiveSession")(function* (
    sessionId: AuthSessionId,
  ) {
    const row = yield* authSessions.getById({ sessionId });
    if (Option.isNone(row) || row.value.revokedAt !== null) {
      return Option.none<AuthClientSession>();
    }

    const connectedSessions = yield* Ref.get(connectedSessionsRef);
    const connected = connectedSessions.has(row.value.sessionId);
    const now = yield* DateTime.now;
    if (!connected && row.value.expiresAt.epochMilliseconds <= now.epochMilliseconds) {
      return Option.none<AuthClientSession>();
    }
    return Option.some(
      toAuthClientSession({
        sessionId: row.value.sessionId,
        subject: row.value.subject,
        ...(row.value.person === null ? {} : { person: row.value.person }),
        scopes: row.value.scopes,
        method: row.value.method,
        client: toClientMetadata(row.value.client),
        issuedAt: row.value.issuedAt,
        expiresAt: row.value.expiresAt,
        lastConnectedAt: row.value.lastConnectedAt,
        connected,
      }),
    );
  });

  const markConnected: SessionStore["Service"]["markConnected"] = (sessionId) =>
    Ref.modify(connectedSessionsRef, (current) => {
      const next = new Map(current);
      const wasDisconnected = !next.has(sessionId);
      next.set(sessionId, (next.get(sessionId) ?? 0) + 1);
      return [wasDisconnected, next] as const;
    }).pipe(
      Effect.flatMap((wasDisconnected) =>
        wasDisconnected
          ? DateTime.now.pipe(
              Effect.flatMap((lastConnectedAt) =>
                authSessions.setLastConnectedAt({
                  sessionId,
                  lastConnectedAt,
                }),
              ),
            )
          : Effect.void,
      ),
      Effect.flatMap(() => loadActiveSession(sessionId)),
      Effect.flatMap((session) =>
        Option.isSome(session) ? emitUpsert(session.value) : Effect.void,
      ),
      Effect.catchCause((cause) =>
        Effect.logError("Failed to publish connected-session auth update.").pipe(
          Effect.annotateLogs({
            sessionId,
            cause,
          }),
        ),
      ),
      Effect.withSpan("SessionStore.markConnected"),
    );

  const markDisconnected: SessionStore["Service"]["markDisconnected"] = (sessionId) =>
    Ref.update(connectedSessionsRef, (current) => {
      const next = new Map(current);
      const remaining = (next.get(sessionId) ?? 0) - 1;
      if (remaining > 0) {
        next.set(sessionId, remaining);
      } else {
        next.delete(sessionId);
      }
      return next;
    }).pipe(
      Effect.flatMap(() => loadActiveSession(sessionId)),
      Effect.flatMap((session) =>
        Option.isSome(session) ? emitUpsert(session.value) : emitRemoved(sessionId),
      ),
      Effect.catchCause((cause) =>
        Effect.logError("Failed to publish disconnected-session auth update.").pipe(
          Effect.annotateLogs({
            sessionId,
            cause,
          }),
        ),
      ),
      Effect.withSpan("SessionStore.markDisconnected"),
    );

  const encodeClaims = Schema.encodeEffect(Schema.fromJsonString(SessionClaims));
  const issue: SessionStore["Service"]["issue"] = Effect.fn("SessionStore.issue")(
    function* (input) {
      const sessionId = AuthSessionId.make(
        yield* crypto
          .randomUUIDv4()
          .pipe(Effect.mapError((cause) => new SessionCredentialIssueError({ cause }))),
      );
      const issuedAt = yield* DateTime.now;
      // Beyond year 9999 ISO strings acquire a leading '+', which sorts before
      // ordinary dates in persisted active-session and revocation predicates.
      const expiresAt = DateTime.min(
        input?.expiresAt === undefined
          ? DateTime.add(issuedAt, {
              milliseconds: Duration.toMillis(input?.ttl ?? DEFAULT_SESSION_TTL),
            })
          : DateTime.toUtc(input.expiresAt),
        REUSABLE_DEV_SESSION_EXPIRES_AT,
      );
      const claims: SessionClaims = {
        v: 1,
        kind: "session",
        sid: sessionId,
        sub: input?.subject ?? "browser",
        scopes: input?.scopes ?? AuthStandardClientScopes,
        method: input?.method ?? "browser-session-cookie",
        ...(input?.proofKeyThumbprint ? { jkt: input.proofKeyThumbprint } : {}),
        ...(input?.runtimeModeCeiling ? { rtc: input.runtimeModeCeiling } : {}),
        ...(input?.peerLink === true ? { plk: 1 as const } : {}),
        ...(input?.mcpLimits === undefined ? {} : { mcp: input.mcpLimits }),
        iat: issuedAt.epochMilliseconds,
        exp: expiresAt.epochMilliseconds,
      };

      const encodedPayload = yield* encodeClaims(claims).pipe(
        Effect.map(base64UrlEncode),
        Effect.mapError(
          (cause) =>
            new SessionCredentialIssueError({
              sessionId,
              cause: new SessionClaimsEncodingError({
                sessionId,
                operation: "encode_session_claims",
                cause,
              }),
            }),
        ),
      );
      const signature = signPayload(encodedPayload, signingSecret);
      const client = input?.client ?? createDefaultClientMetadata();
      const sessionRecord = {
        sessionId,
        subject: claims.sub,
        scopes: claims.scopes,
        method: claims.method,
        client: {
          label: client.label ?? null,
          ipAddress: client.ipAddress ?? null,
          userAgent: client.userAgent ?? null,
          deviceType: client.deviceType,
          os: client.os ?? null,
          browser: client.browser ?? null,
        },
        issuedAt,
        expiresAt,
      } satisfies AuthSessions.CreateAuthSessionInput;
      const replacedSessionIds = yield* sql
        .withTransaction(
          (input?.replaceSessionId !== undefined || input?.replaceActiveForSubjectAndMethod
            ? authSessions.createReplacingActive({
                session: sessionRecord,
                revokedAt: issuedAt,
                ...(input.replaceSessionId !== undefined
                  ? { replaceSessionId: input.replaceSessionId }
                  : {}),
              })
            : authSessions.create(sessionRecord).pipe(Effect.as([] as ReadonlyArray<AuthSessionId>))
          ).pipe(
            Effect.tap(() =>
              input?.renewalCommitment === undefined
                ? Effect.void
                : renewal.enroll({
                    sessionId,
                    commitment: input.renewalCommitment,
                    approved: true,
                  }),
            ),
          ),
        )
        .pipe(Effect.mapError((cause) => new SessionCredentialIssueError({ sessionId, cause })));

      if (replacedSessionIds.length > 0) {
        yield* Ref.update(connectedSessionsRef, (current) => {
          const next = new Map(current);
          for (const replacedSessionId of replacedSessionIds) {
            next.delete(replacedSessionId);
          }
          return next;
        });
        yield* Effect.forEach(replacedSessionIds, emitRemoved, {
          concurrency: "unbounded",
          discard: true,
        });
      }
      yield* emitUpsert(
        toAuthClientSession({
          sessionId,
          subject: claims.sub,
          scopes: claims.scopes,
          method: claims.method,
          client,
          issuedAt,
          expiresAt,
          lastConnectedAt: null,
          connected: false,
        }),
      );

      return {
        sessionId,
        token: `${encodedPayload}.${signature}`,
        method: claims.method,
        client,
        expiresAt: expiresAt,
        scopes: claims.scopes,
        ...(input?.renewalCommitment === undefined
          ? {}
          : { renewAfter: (issuedAt.epochMilliseconds + expiresAt.epochMilliseconds) / 2 }),
        ...(claims.jkt ? { proofKeyThumbprint: claims.jkt } : {}),
      } satisfies IssuedSession;
    },
  );

  const verify: SessionStore["Service"]["verify"] = Effect.fn("SessionStore.verify")(
    function* (token) {
      if (devAuth?.matches(token)) {
        const row = yield* authSessions
          .getById({ sessionId: devAuth.sessionId })
          .pipe(
            Effect.mapError(
              (cause) =>
                new SessionCredentialVerificationError({ sessionId: devAuth.sessionId, cause }),
            ),
          );
        if (Option.isNone(row)) {
          return yield* new UnknownSessionTokenError({ sessionId: devAuth.sessionId });
        }
        if (row.value.revokedAt !== null) {
          return yield* new SessionTokenRevokedError({
            sessionId: devAuth.sessionId,
            revokedAt: row.value.revokedAt,
          });
        }
        const observedAt = yield* DateTime.now;
        if (row.value.expiresAt.epochMilliseconds <= observedAt.epochMilliseconds) {
          return yield* new SessionTokenExpiredError({
            sessionId: devAuth.sessionId,
            expiresAt: row.value.expiresAt,
            observedAt,
          });
        }
        return {
          sessionId: row.value.sessionId,
          token,
          method: row.value.method,
          client: toClientMetadata(row.value.client),
          expiresAt: row.value.expiresAt,
          subject: row.value.subject,
          scopes: row.value.scopes,
        } satisfies VerifiedSession;
      }
      const [encodedPayload, signature] = token.split(".");
      if (!encodedPayload || !signature) {
        return yield* new MalformedSessionTokenError({});
      }

      const expectedSignature = signPayload(encodedPayload, signingSecret);
      if (!timingSafeEqualBase64Url(signature, expectedSignature)) {
        return yield* new InvalidSessionTokenSignatureError({});
      }

      const claims = yield* decodeSessionClaims(base64UrlDecodeUtf8(encodedPayload)).pipe(
        Effect.mapError((cause) => new InvalidSessionTokenPayloadError({ cause })),
      );

      const observedAt = yield* DateTime.now;
      const expiresAt = DateTime.make(claims.exp);
      if (Option.isNone(expiresAt)) {
        return yield* new InvalidSessionExpirationClaimError({
          sessionId: claims.sid,
          expirationClaim: claims.exp,
        });
      }
      if (claims.exp <= observedAt.epochMilliseconds) {
        return yield* new SessionTokenExpiredError({
          sessionId: claims.sid,
          expiresAt: expiresAt.value,
          observedAt,
        });
      }

      const row = yield* authSessions
        .getById({ sessionId: claims.sid })
        .pipe(
          Effect.mapError(
            (cause) => new SessionCredentialVerificationError({ sessionId: claims.sid, cause }),
          ),
        );
      if (Option.isNone(row)) {
        return yield* new UnknownSessionTokenError({ sessionId: claims.sid });
      }
      if (row.value.revokedAt !== null) {
        return yield* new SessionTokenRevokedError({
          sessionId: claims.sid,
          revokedAt: row.value.revokedAt,
        });
      }

      return {
        sessionId: claims.sid,
        token,
        method: claims.method,
        client: toClientMetadata(row.value.client),
        expiresAt: expiresAt.value,
        subject: claims.sub,
        scopes: claims.scopes,
        ...(claims.jkt ? { proofKeyThumbprint: claims.jkt } : {}),
        ...(claims.rtc ? { runtimeModeCeiling: claims.rtc } : {}),
        ...(claims.plk === 1 ? { peerLink: true as const } : {}),
        ...(claims.mcp === undefined ? {} : { mcpLimits: claims.mcp }),
      } satisfies VerifiedSession;
    },
  );

  // OAuth refresh authority has a separate signing domain and is never a bearer session.
  const RefreshClaims = Schema.Struct({
    clientId: Schema.String,
    resource: Schema.String,
    nonce: Schema.String,
    claims: SessionClaims,
  });
  const decodeRefresh = Schema.decodeUnknownEffect(Schema.fromJsonString(RefreshClaims));
  const encodeRefresh = Schema.encodeEffect(Schema.fromJsonString(RefreshClaims));
  const signRefresh = (payload: string) => signPayload(`mcp-refresh.${payload}`, signingSecret);
  const accessTokens = (
    claims: SessionClaims,
    clientId: string,
    resource: string,
    nextNonce?: string,
  ) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const expiresAt = DateTime.add(now, { hours: 1 });
      const payload = yield* encodeClaims({
        ...claims,
        iat: now.epochMilliseconds,
        exp: expiresAt.epochMilliseconds,
      }).pipe(
        Effect.map(base64UrlEncode),
        Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
      );
      const nonce =
        nextNonce ??
        (yield* crypto
          .randomUUIDv4()
          .pipe(Effect.mapError((cause) => new SessionCredentialIssueError({ cause }))));
      const refreshPayload = yield* encodeRefresh({ claims, clientId, resource, nonce }).pipe(
        Effect.map(base64UrlEncode),
        Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
      );
      const refreshToken = `${refreshPayload}.${signRefresh(refreshPayload)}`;
      return {
        token: `${payload}.${signPayload(payload, signingSecret)}`,
        refreshToken,
        expiresAt,
      };
    });
  const tokenHash = (token: string) => signPayload(`mcp-refresh-hash.${token}`, signingSecret);
  const issueMcpOAuthTokens: SessionStore["Service"]["issueMcpOAuthTokens"] = (
    token,
    clientId,
    resource,
  ) =>
    Effect.gen(function* () {
      const verified = yield* verify(token);
      if (verified.subject !== "mcp-client") return yield* new MalformedSessionTokenError({});
      const claims = yield* decodeSessionClaims(base64UrlDecodeUtf8(token.split(".")[0]!)).pipe(
        Effect.mapError((cause) => new InvalidSessionTokenPayloadError({ cause })),
      );
      const result = yield* accessTokens(claims, clientId, resource);
      yield* sql`INSERT INTO auth_mcp_refresh (session_id, token_hash) VALUES (${claims.sid}, ${tokenHash(result.refreshToken)})`.pipe(
        Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
      );
      return result;
    });
  const refreshMcpOAuthToken: SessionStore["Service"]["refreshMcpOAuthToken"] = (
    token,
    clientId,
    resource,
  ) =>
    Effect.gen(function* () {
      const [payload, signature, extra] = token.split(".");
      if (
        !payload ||
        !signature ||
        extra !== undefined ||
        !timingSafeEqualBase64Url(signature, signRefresh(payload))
      )
        return yield* new MalformedSessionTokenError({});
      const refresh = yield* decodeRefresh(base64UrlDecodeUtf8(payload)).pipe(
        Effect.mapError((cause) => new InvalidSessionTokenPayloadError({ cause })),
      );
      if (
        refresh.clientId !== clientId ||
        refresh.resource !== resource ||
        refresh.claims.sub !== "mcp-client"
      )
        return yield* new MalformedSessionTokenError({});
      const authorityPayload = yield* encodeClaims(refresh.claims).pipe(
        Effect.map(base64UrlEncode),
        Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
      );
      yield* verify(`${authorityPayload}.${signPayload(authorityPayload, signingSecret)}`);
      // A concurrent SDK request or a lost response must get the same successor,
      // without persisting the refresh secret. Only the immediate predecessor can retry.
      const result = yield* accessTokens(
        refresh.claims,
        clientId,
        resource,
        signPayload(`mcp-refresh-nonce.${token}`, signingSecret),
      );
      const now = (yield* DateTime.now).epochMilliseconds;
      const updated =
        yield* sql`UPDATE auth_mcp_refresh SET token_hash = ${tokenHash(result.refreshToken)},
          previous_hash = ${tokenHash(token)}, previous_valid_until = ${now + 5_000}
        WHERE session_id = ${refresh.claims.sid} AND token_hash = ${tokenHash(token)} RETURNING session_id`.pipe(
          Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
        );
      if (updated.length !== 1) {
        const retry = yield* sql`SELECT session_id FROM auth_mcp_refresh
          WHERE session_id = ${refresh.claims.sid} AND token_hash = ${tokenHash(result.refreshToken)}
            AND previous_hash = ${tokenHash(token)} AND previous_valid_until > ${now}`.pipe(
          Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
        );
        if (retry.length !== 1) {
          // Valid bound authority reused after the retry window, or after another rotation.
          yield* revoke(refresh.claims.sid);
          return yield* new MalformedSessionTokenError({});
        }
      }
      return { ...result, scopes: refresh.claims.scopes };
    });

  const encodeWsClaims = Schema.encodeEffect(Schema.fromJsonString(WebSocketClaims));
  const issueWebSocketToken: SessionStore["Service"]["issueWebSocketToken"] = Effect.fn(
    "SessionStore.issueWebSocketToken",
  )(function* (sessionId, input) {
    const issuedAt = yield* DateTime.now;
    const expiresAt = DateTime.add(issuedAt, {
      milliseconds: Duration.toMillis(input?.ttl ?? DEFAULT_WEBSOCKET_TOKEN_TTL),
    });
    const claims: WebSocketClaims = {
      v: 1,
      kind: "websocket",
      sid: sessionId,
      ...(input?.audience ? { aud: input.audience } : {}),
      ...(input?.proofKeyThumbprint ? { jkt: input.proofKeyThumbprint } : {}),
      iat: issuedAt.epochMilliseconds,
      exp: expiresAt.epochMilliseconds,
    };
    const encodedPayload = yield* encodeWsClaims(claims).pipe(
      Effect.map(base64UrlEncode),
      Effect.mapError(
        (cause) =>
          new WebSocketTokenIssueError({
            sessionId,
            cause: new SessionClaimsEncodingError({
              sessionId,
              operation: "encode_websocket_claims",
              cause,
            }),
          }),
      ),
    );
    const signature = signPayload(encodedPayload, signingSecret);
    return {
      token: `${encodedPayload}.${signature}`,
      expiresAt,
    };
  });

  const verifyWebSocketToken: SessionStore["Service"]["verifyWebSocketToken"] = Effect.fn(
    "SessionStore.verifyWebSocketToken",
  )(function* (token) {
    const [encodedPayload, signature] = token.split(".");
    if (!encodedPayload || !signature) {
      return yield* new MalformedWebSocketTokenError({});
    }

    const expectedSignature = signPayload(encodedPayload, signingSecret);
    if (!timingSafeEqualBase64Url(signature, expectedSignature)) {
      return yield* new InvalidWebSocketTokenSignatureError({});
    }

    const claims = yield* decodeWebSocketClaims(base64UrlDecodeUtf8(encodedPayload)).pipe(
      Effect.mapError((cause) => new InvalidWebSocketTokenPayloadError({ cause })),
    );
    if (claims.sid.startsWith(REUSABLE_DEV_SESSION_PREFIX) && claims.sid !== devAuth?.sessionId) {
      return yield* new UnknownWebSocketSessionError({ sessionId: claims.sid });
    }

    const observedAt = yield* DateTime.now;
    const expiresAt = DateTime.make(claims.exp);
    if (Option.isNone(expiresAt)) {
      return yield* new InvalidSessionExpirationClaimError({
        sessionId: claims.sid,
        expirationClaim: claims.exp,
      });
    }
    if (claims.exp <= observedAt.epochMilliseconds) {
      return yield* new WebSocketTokenExpiredError({
        sessionId: claims.sid,
        expiresAt: expiresAt.value,
        observedAt,
      });
    }

    const row = yield* authSessions
      .getById({ sessionId: claims.sid })
      .pipe(
        Effect.mapError(
          (cause) => new WebSocketTokenVerificationError({ sessionId: claims.sid, cause }),
        ),
      );
    if (Option.isNone(row)) {
      return yield* new UnknownWebSocketSessionError({ sessionId: claims.sid });
    }
    if (row.value.expiresAt.epochMilliseconds <= observedAt.epochMilliseconds) {
      return yield* new WebSocketSessionExpiredError({
        sessionId: claims.sid,
        expiresAt: row.value.expiresAt,
        observedAt,
      });
    }
    if (row.value.revokedAt !== null) {
      return yield* new WebSocketSessionRevokedError({
        sessionId: claims.sid,
        revokedAt: row.value.revokedAt,
      });
    }

    return {
      sessionId: row.value.sessionId,
      token,
      method: row.value.method,
      client: toClientMetadata(row.value.client),
      expiresAt: row.value.expiresAt,
      subject: row.value.subject,
      scopes: row.value.scopes,
      ...(claims.aud ? { audience: claims.aud } : {}),
      ...(claims.jkt ? { proofKeyThumbprint: claims.jkt } : {}),
    } satisfies VerifiedWebSocketTicket;
  });

  const viewers = new Map<
    string,
    {
      readonly sessionId: AuthSessionId;
      readonly expiresAt: DateTime.DateTime;
      readonly closed: Deferred.Deferred<void>;
      readonly active: Set<symbol>;
      readonly resource: ViewerResource;
      readonly operation: ViewerOperation;
      readonly generation: string;
    }
  >();
  const closeViewer: SessionStore["Service"]["closeViewer"] = (sessionId, viewerId) =>
    Effect.gen(function* () {
      const viewer = viewers.get(viewerId);
      if (!viewer || viewer.sessionId !== sessionId) return false;
      viewers.delete(viewerId);
      yield* Deferred.succeed(viewer.closed, undefined);
      return true;
    });
  const awaitViewerInvalidation: SessionStore["Service"]["awaitViewerInvalidation"] = (viewerId) =>
    Effect.gen(function* () {
      const viewer = viewers.get(viewerId);
      if (!viewer) return;
      const now = yield* DateTime.now;
      const admissionExpired = Effect.sleep(
        Duration.millis(Math.max(0, viewer.expiresAt.epochMilliseconds - now.epochMilliseconds)),
      ).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            viewer.active.size === 0 ? Effect.void : Deferred.await(viewer.closed),
          ),
        ),
      );
      yield* Effect.raceFirst(
        awaitInvalidation(viewer.sessionId),
        Effect.raceFirst(Deferred.await(viewer.closed), admissionExpired),
      );
    });
  const viewerSession = (sessionId: AuthSessionId, token: string) =>
    Effect.gen(function* () {
      const row = yield* authSessions
        .getById({ sessionId })
        .pipe(
          Effect.mapError((cause) => new SessionCredentialVerificationError({ sessionId, cause })),
        );
      if (Option.isNone(row)) return yield* new UnknownSessionTokenError({ sessionId });
      if (row.value.revokedAt !== null)
        return yield* new SessionTokenRevokedError({ sessionId, revokedAt: row.value.revokedAt });
      const observedAt = yield* DateTime.now;
      if (row.value.expiresAt.epochMilliseconds <= observedAt.epochMilliseconds)
        return yield* new SessionTokenExpiredError({
          sessionId,
          expiresAt: row.value.expiresAt,
          observedAt,
        });
      return {
        sessionId,
        token,
        method: row.value.method,
        client: toClientMetadata(row.value.client),
        expiresAt: row.value.expiresAt,
        subject: row.value.subject,
        scopes: row.value.scopes,
      } satisfies VerifiedSession;
    });
  const issueViewerToken: SessionStore["Service"]["issueViewerToken"] = Effect.fn(
    "SessionStore.issueViewerToken",
  )(function* (input) {
    const session = yield* viewerSession(input.sessionId, "");
    const existing = input.viewerId === undefined ? undefined : viewers.get(input.viewerId);
    if (
      input.viewerId !== undefined &&
      (existing === undefined ||
        existing.sessionId !== input.sessionId ||
        existing.active.size === 0 ||
        encodeViewerResource(existing.resource) !== encodeViewerResource(input.resource) ||
        existing.operation !== input.operation ||
        existing.generation !== input.generation)
    )
      return yield* new SessionCredentialIssueError({
        sessionId: input.sessionId,
        cause: "The exact active viewer is unavailable.",
      });
    const now = yield* DateTime.now;
    for (const [id, viewer] of viewers) {
      if (viewer.active.size === 0 && viewer.expiresAt.epochMilliseconds <= now.epochMilliseconds)
        yield* closeViewer(viewer.sessionId, id);
    }
    if (existing === undefined && viewers.size >= 256)
      return yield* new SessionCredentialIssueError({ cause: "Too many active viewers." });
    const expiresAt = DateTime.min(DateTime.add(now, { seconds: 60 }), session.expiresAt);
    const viewerId =
      input.viewerId ??
      (yield* crypto
        .randomUUIDv4()
        .pipe(
          Effect.mapError(
            (cause) => new SessionCredentialIssueError({ sessionId: input.sessionId, cause }),
          ),
        ));
    const payload = yield* encodeViewerClaims({
      v: 1,
      kind: "viewer",
      sid: input.sessionId,
      viewerId,
      resource: input.resource,
      operation: input.operation,
      generation: input.generation,
      exp: expiresAt.epochMilliseconds,
    }).pipe(
      Effect.map(base64UrlEncode),
      Effect.mapError(
        (cause) => new SessionCredentialIssueError({ sessionId: input.sessionId, cause }),
      ),
    );
    if (existing !== undefined) yield* viewerSession(input.sessionId, "");
    if (
      existing !== undefined &&
      (viewers.get(viewerId) !== existing || existing.active.size === 0)
    )
      return yield* new SessionCredentialIssueError({
        sessionId: input.sessionId,
        cause: "The active viewer ended during renewal.",
      });
    // Renewed admission must not extend an abandoned viewer beyond its original lifetime.
    if (existing === undefined)
      viewers.set(viewerId, {
        sessionId: input.sessionId,
        expiresAt,
        closed: yield* Deferred.make<void>(),
        active: new Set(),
        resource: input.resource,
        operation: input.operation,
        generation: input.generation,
      });
    return { token: `${payload}.${signPayload(payload, signingSecret)}`, viewerId, expiresAt };
  });
  const authenticateViewerToken: SessionStore["Service"]["authenticateViewerToken"] = Effect.fn(
    "SessionStore.authenticateViewerToken",
  )(function* (token) {
    const [payload, signature, extra] = token.split(".");
    if (!payload || !signature || extra !== undefined)
      return yield* new MalformedWebSocketTokenError({});
    if (!timingSafeEqualBase64Url(signature, signPayload(payload, signingSecret)))
      return yield* new InvalidWebSocketTokenSignatureError({});
    const claims = yield* decodeViewerClaims(base64UrlDecodeUtf8(payload)).pipe(
      Effect.mapError((cause) => new InvalidWebSocketTokenPayloadError({ cause })),
    );
    const now = yield* DateTime.now;
    const expiresAt = DateTime.makeUnsafe(claims.exp);
    if (expiresAt.epochMilliseconds <= now.epochMilliseconds)
      return yield* new WebSocketTokenExpiredError({
        sessionId: claims.sid,
        expiresAt,
        observedAt: now,
      });
    const active = viewers.get(claims.viewerId);
    if (active?.sessionId !== claims.sid)
      return yield* new UnknownWebSocketSessionError({ sessionId: claims.sid });
    const session = yield* viewerSession(claims.sid, token);
    const admittedAt = yield* DateTime.now;
    if (expiresAt.epochMilliseconds <= admittedAt.epochMilliseconds)
      return yield* new WebSocketTokenExpiredError({
        sessionId: claims.sid,
        expiresAt,
        observedAt: admittedAt,
      });
    if (
      viewers.get(claims.viewerId) !== active ||
      Deferred.isDoneUnsafe(active.closed) ||
      (active.active.size === 0 &&
        active.expiresAt.epochMilliseconds <= admittedAt.epochMilliseconds)
    )
      return yield* new UnknownWebSocketSessionError({ sessionId: claims.sid });
    return {
      session,
      viewerId: claims.viewerId,
      expiresAt,
      resource: claims.resource,
      operation: claims.operation,
      generation: claims.generation,
    };
  });
  const admitViewerToken: SessionStore["Service"]["admitViewerToken"] = (token) =>
    Effect.acquireRelease(
      Effect.gen(function* () {
        const viewer = yield* authenticateViewerToken(token);
        const retained = viewers.get(viewer.viewerId);
        if (retained === undefined)
          return yield* new UnknownWebSocketSessionError({ sessionId: viewer.session.sessionId });
        const transport = Symbol();
        retained.active.add(transport);
        const recheck = Effect.gen(function* () {
          if (viewers.get(viewer.viewerId) !== retained || !retained.active.has(transport))
            return yield* new UnknownWebSocketSessionError({ sessionId: retained.sessionId });
          yield* viewerSession(retained.sessionId, token);
          if (viewers.get(viewer.viewerId) !== retained || !retained.active.has(transport))
            return yield* new UnknownWebSocketSessionError({ sessionId: retained.sessionId });
        });
        return { viewer: { ...viewer, recheck }, retained, transport };
      }),
      ({ retained, transport }) =>
        Effect.gen(function* () {
          retained.active.delete(transport);
          const now = yield* DateTime.now;
          if (
            retained.active.size === 0 &&
            retained.expiresAt.epochMilliseconds <= now.epochMilliseconds
          ) {
            yield* Deferred.succeed(retained.closed, undefined);
          }
        }),
    ).pipe(Effect.map(({ viewer }) => viewer));
  const verifyViewerToken: SessionStore["Service"]["verifyViewerToken"] = Effect.fn(
    "SessionStore.verifyViewerToken",
  )(function* (input) {
    const viewer = yield* authenticateViewerToken(input.token);
    if (
      encodeViewerResource(viewer.resource) !== encodeViewerResource(input.resource) ||
      viewer.generation !== input.generation ||
      (input.operation === "control" && viewer.operation !== "control")
    )
      return yield* new InvalidWebSocketTokenPayloadError({
        cause: "Viewer resource or operation mismatch.",
      });
    return viewer;
  });

  const listActive: SessionStore["Service"]["listActive"] = Effect.fn("SessionStore.listActive")(
    function* () {
      const now = yield* DateTime.now;
      const connectedSessions = yield* Ref.get(connectedSessionsRef);
      const rows = yield* authSessions.listActive({
        now,
        connectedSessionIds: Array.from(connectedSessions.keys()),
      });

      return rows.map((row) =>
        toAuthClientSession({
          sessionId: row.sessionId,
          subject: row.subject,
          ...(row.person === null ? {} : { person: row.person }),
          scopes: row.scopes,
          method: row.method,
          client: toClientMetadata(row.client),
          issuedAt: row.issuedAt,
          expiresAt: row.expiresAt,
          lastConnectedAt: row.lastConnectedAt,
          connected: connectedSessions.has(row.sessionId),
        }),
      );
    },
    Effect.mapError((cause) => new ActiveSessionsListError({ cause })),
  );

  const revoke: SessionStore["Service"]["revoke"] = Effect.fn("SessionStore.revoke")(
    function* (sessionId) {
      const revokedAt = yield* DateTime.now;
      const revoked = yield* authSessions
        .revoke({
          sessionId,
          revokedAt,
        })
        .pipe(Effect.mapError((cause) => new SessionRevocationError({ sessionId, cause })));
      if (revoked) {
        yield* Ref.update(connectedSessionsRef, (current) => {
          const next = new Map(current);
          next.delete(sessionId);
          return next;
        });
        yield* emitRemoved(sessionId);
      }
      return revoked;
    },
  );

  const revokeAllExcept: SessionStore["Service"]["revokeAllExcept"] = Effect.fn(
    "SessionStore.revokeAllExcept",
  )(function* (sessionId) {
    const revokedAt = yield* DateTime.now;
    const revokedSessionIds = yield* authSessions
      .revokeAllExcept({
        currentSessionId: sessionId,
        revokedAt,
      })
      .pipe(
        Effect.mapError(
          (cause) => new OtherSessionsRevocationError({ currentSessionId: sessionId, cause }),
        ),
      );
    if (revokedSessionIds.length > 0) {
      yield* Ref.update(connectedSessionsRef, (current) => {
        const next = new Map(current);
        for (const revokedSessionId of revokedSessionIds) {
          next.delete(revokedSessionId);
        }
        return next;
      });
      yield* Effect.forEach(
        revokedSessionIds,
        (revokedSessionId) => emitRemoved(revokedSessionId),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
    }
    return revokedSessionIds.length;
  });

  const awaitInvalidation = Effect.fn(function* (sessionId: AuthSessionId) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        // Registered before the first read, so a revocation committed after that read
        // is covered by the cross-process check's next snapshot.
        yield* Effect.acquireRelease(
          SubscriptionRef.update(watchedSessionsRef, (current) =>
            new Map(current).set(sessionId, (current.get(sessionId) ?? 0) + 1),
          ),
          () =>
            SubscriptionRef.update(watchedSessionsRef, (current) => {
              const next = new Map(current);
              const remaining = (next.get(sessionId) ?? 0) - 1;
              if (remaining > 0) next.set(sessionId, remaining);
              else next.delete(sessionId);
              return next;
            }),
        );
        const subscription = yield* PubSub.subscribe(changesPubSub);
        while (true) {
          const row = yield* authSessions
            .getById({ sessionId })
            .pipe(
              Effect.mapError(
                (cause) => new SessionCredentialVerificationError({ sessionId, cause }),
              ),
            );
          if (Option.isNone(row) || row.value.revokedAt !== null) return;
          const now = yield* DateTime.now;
          const remaining = row.value.expiresAt.epochMilliseconds - now.epochMilliseconds;
          if (remaining <= 0) return;
          // Both a change and the old deadline require a fresh authoritative read.
          yield* Effect.raceFirst(
            Stream.fromSubscription(subscription).pipe(
              Stream.filter((change) =>
                change.type === "clientRemoved"
                  ? change.sessionId === sessionId
                  : change.clientSession.sessionId === sessionId,
              ),
              Stream.runHead,
            ),
            Effect.sleep(Duration.millis(remaining)),
          );
        }
      }),
    );
  });
  const renewSession: SessionStore["Service"]["renewSession"] = Effect.fn(
    "SessionStore.renewSession",
  )(function* (input) {
    const renewed = yield* renewal
      .renew(input)
      .pipe(
        Effect.tapError((error) =>
          error._tag === "RenewalRejected" && error.revokedSessionId !== undefined
            ? emitRemoved(error.revokedSessionId)
            : Effect.void,
        ),
      );
    const row = renewed.session;
    const payload = yield* encodeClaims({
      v: 1,
      kind: "session",
      sid: row.sessionId,
      sub: row.subject,
      scopes: row.scopes,
      method: row.method,
      iat: row.issuedAt.epochMilliseconds,
      exp: row.expiresAt.epochMilliseconds,
    }).pipe(
      Effect.map(base64UrlEncode),
      Effect.mapError((cause) => new SessionCredentialIssueError({ cause })),
    );
    const current = yield* loadActiveSession(row.sessionId).pipe(
      Effect.mapError(
        (cause) => new SessionCredentialVerificationError({ sessionId: row.sessionId, cause }),
      ),
    );
    if (Option.isSome(current)) yield* emitUpsert(current.value);
    return {
      token: `${payload}.${signPayload(payload, signingSecret)}`,
      method: row.method,
      expiresAt: row.expiresAt,
      generation: renewed.generation,
      outcome: renewed.outcome,
      renewAfter: renewed.renewAfter,
    };
  });
  const getPerson: SessionStore["Service"]["getPerson"] = (sessionId) =>
    authSessions.getById({ sessionId }).pipe(
      Effect.map((row) => (Option.isSome(row) ? row.value.person : null)),
      Effect.mapError((cause) => new ActiveSessionsListError({ cause })),
    );
  const setPerson: SessionStore["Service"]["setPerson"] = Effect.fn("SessionStore.setPerson")(
    function* (sessionId, person) {
      const current = yield* loadActiveSession(sessionId);
      if (Option.isNone(current)) return false;
      yield* authSessions.setPerson({ sessionId, person });
      const updated = yield* loadActiveSession(sessionId);
      if (Option.isSome(updated)) yield* emitUpsert(updated.value);
      return true;
    },
    Effect.mapError((cause) => new ActiveSessionsListError({ cause })),
  );

  return SessionStore.of({
    getPerson,
    setPerson,
    cookieName,
    legacyCookieName,
    issue,
    renewSession,
    issueMcpOAuthTokens,
    refreshMcpOAuthToken,
    verify,
    issueViewerToken,
    verifyViewerToken,
    authenticateViewerToken,
    admitViewerToken,
    closeViewer,
    awaitViewerInvalidation,
    issueWebSocketToken,
    verifyWebSocketToken,
    listActive,
    awaitInvalidation,
    get streamChanges() {
      return Stream.fromPubSub(changesPubSub);
    },
    revoke,
    revokeAllExcept,
    markConnected,
    markDisconnected,
  });
});

export const layer = Layer.effect(SessionStore, make).pipe(
  Layer.provideMerge(AuthSessions.layer),
  Layer.provideMerge(SessionRenewal.layer),
);
