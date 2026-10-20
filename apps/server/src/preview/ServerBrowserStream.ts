import * as NodeHttpServerRequest from "@effect/platform-node/NodeHttpServerRequest";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import {
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  PREVIEW_STREAM_HOST_SETUP_CLOSE_CODE,
  PreviewStreamHostSetup,
  AuthSessionId,
  OrchestratorMcpFailure,
  PreviewViewerOpenInput,
  PreviewViewerClientRpcGroup,
  PreviewViewerUnavailableError,
  PreviewStreamInput,
  WS_METHODS,
  type PreviewViewerAccess,
  type PeerAuthorityEnvelope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import {
  HttpPlatform,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
  Multipart,
} from "effect/http";
import * as Socket from "effect/socket/Socket";

import { authenticateMediaRequest } from "../auth/http.ts";
import { assetFileResponse, nativeDownloadResponse } from "../http.ts";
import { openMediaFile } from "../assets/MediaFile.ts";
import * as HttpBody from "effect/http/HttpBody";
import * as PreviewBrowserHost from "./PreviewBrowserHost.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import type * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

const PREVIEW_STREAM_ROUTE_PREFIX = "/api/preview-stream";

type PreviewViewerBinding = {
  readonly input: PreviewViewerOpenInput;
  readonly generation: string;
} & (
  | {
      readonly type: "native";
      readonly authorize: Effect.Success<
        ReturnType<typeof EnvironmentAuth.nativeSessionAuthorization>
      >;
    }
  | { readonly type: "incoming"; readonly scope: Invocation.McpInvocationScope }
  | {
      readonly type: "outgoing";
      readonly origin: string;
      access: PreviewViewerAccess;
      readonly envelope: PeerAuthorityEnvelope;
    }
);
const viewerRefused = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });
const viewerUnsupported = () =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message: "Authenticated resource-bound browser viewing is unsupported by this environment.",
  });
const decodeViewerInput = Schema.decodeUnknownEffect(PreviewViewerOpenInput);

export class PreviewViewers extends Context.Service<
  PreviewViewers,
  {
    readonly open: (
      session: EnvironmentAuth.AuthenticatedSession,
      input: PreviewViewerOpenInput,
    ) => Effect.Effect<PreviewViewerAccess, OrchestratorMcpFailure>;
    readonly accept: (
      scope: Invocation.McpInvocationScope,
      input: PreviewViewerOpenInput,
    ) => Effect.Effect<PreviewViewerAccess, OrchestratorMcpFailure>;
    readonly close: (sessionId: AuthSessionId, viewerId: string) => Effect.Effect<boolean>;
    readonly resolve: (
      viewerId: string,
      input: PreviewViewerOpenInput,
      generation: string,
    ) => Effect.Effect<
      | { readonly type: "local" }
      | { readonly type: "peer"; readonly origin: string; readonly ticket: string },
      OrchestratorMcpFailure
    >;
  }
>()("t3/preview/ServerBrowserStream/PreviewViewers") {}

export const clientHandlers = (
  session: EnvironmentAuth.AuthenticatedSession,
  viewers: PreviewViewers["Service"],
) =>
  PreviewViewerClientRpcGroup.of({
    [WS_METHODS.previewViewerOpen]: (input) =>
      viewers.open(session, input).pipe(
        Effect.mapError(
          (error) =>
            new PreviewViewerUnavailableError({
              reason:
                error.code === "invalid_request"
                  ? "unsupported"
                  : error.code === "capability_denied"
                    ? "denied"
                    : "unavailable",
            }),
        ),
      ),
    [WS_METHODS.previewViewerClose]: (input) => viewers.close(session.sessionId, input.viewerId),
  });

/** Source sessions own admissions. Destination credentials and routing stay server-private. */
export const layerViewers = Layer.effect(
  PreviewViewers,
  Effect.gen(function* () {
    const sessions = yield* SessionStore.SessionStore;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const here = yield* environment.getEnvironmentId;
    const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    const peers = yield* Effect.serviceOption(PeerMcpClient.PeerMcpClient);
    const links = yield* Effect.serviceOption(PeerLinks.PeerLinks);
    const nativeSessions = yield* AuthSessions.AuthSessionRepository;
    const lifetime = yield* Scope.Scope;
    const bindings = new Map<
      string,
      {
        readonly binding: PreviewViewerBinding;
        readonly sessionId: AuthSessionId;
        retiring: boolean;
        readonly retired: Deferred.Deferred<boolean>;
      }
    >();
    const closeRemote = (binding: PreviewViewerBinding) =>
      binding.type !== "outgoing"
        ? Effect.succeed(true)
        : Option.isSome(peers) && peers.value.closePreviewViewer !== undefined
          ? peers.value
              .closePreviewViewer({
                environmentId: binding.input.resource.environmentId,
                generation: binding.generation,
                viewerId: binding.access.viewerId,
              })
              .pipe(
                Effect.timeout("5 seconds"),
                Effect.orElseSucceed(() => false),
              )
          : Effect.succeed(false);
    const retire = (id: string): Effect.Effect<boolean> =>
      Effect.suspend(() => {
        const entry = bindings.get(id);
        if (entry === undefined) return Effect.succeed(true);
        if (entry.retiring) return Deferred.await(entry.retired);
        entry.retiring = true;
        return closeRemote(entry.binding).pipe(
          Effect.tap((closed) => Deferred.succeed(entry.retired, closed)),
          Effect.ensuring(Effect.sync(() => bindings.delete(id))),
        );
      });
    const resource = (input: PreviewViewerOpenInput) =>
      Effect.gen(function* () {
        if (Option.isNone(browser) || browser.value.authorizeViewer === undefined)
          return yield* viewerUnsupported();
        yield* browser.value
          .authorizeViewer(input.resource)
          .pipe(
            Effect.mapError(() =>
              viewerRefused("This exact browser opening is no longer available."),
            ),
          );
      });
    const issue = (sessionId: AuthSessionId, binding: PreviewViewerBinding) =>
      Effect.gen(function* () {
        const issued = yield* sessions
          .issueViewerToken({ sessionId, ...binding.input, generation: binding.generation })
          .pipe(
            Effect.mapError(() => viewerRefused("The source browser viewer could not be created.")),
          );
        bindings.set(issued.viewerId, {
          binding,
          sessionId,
          retiring: false,
          retired: yield* Deferred.make<boolean>(),
        });
        yield* Effect.forkIn(
          sessions
            .awaitViewerInvalidation(issued.viewerId)
            .pipe(Effect.ignore, Effect.andThen(retire(issued.viewerId))),
          lifetime,
        );
        const expiresAt =
          binding.type === "outgoing" &&
          binding.access.expiresAt < DateTime.formatIso(issued.expiresAt)
            ? binding.access.expiresAt
            : DateTime.formatIso(issued.expiresAt);
        return {
          viewerId: issued.viewerId,
          ticket: issued.token,
          expiresAt,
          streamBasePath: PREVIEW_STREAM_ROUTE_PREFIX,
        } satisfies PreviewViewerAccess;
      });
    const renew = (
      sessionId: AuthSessionId,
      input: PreviewViewerOpenInput,
      scope?: Invocation.McpInvocationScope,
    ) =>
      Effect.gen(function* () {
        const entry = input.viewerId === undefined ? undefined : bindings.get(input.viewerId);
        const binding = entry?.binding;
        if (
          entry === undefined ||
          entry.retiring ||
          entry.sessionId !== sessionId ||
          binding === undefined ||
          binding.input.resource.environmentId !== input.resource.environmentId ||
          binding.input.resource.threadId !== input.resource.threadId ||
          binding.input.resource.tabId !== input.resource.tabId ||
          binding.input.resource.serverEpoch !== input.resource.serverEpoch ||
          binding.input.operation !== input.operation ||
          (scope !== undefined &&
            (binding.type !== "incoming" || scope.authority?.id !== binding.scope.authority?.id))
        )
          return yield* viewerRefused("The exact original browser viewer cannot be renewed.");
        const check = Effect.gen(function* () {
          if (bindings.get(input.viewerId!) !== entry || entry.retiring)
            return yield* viewerRefused("The browser viewer was retired.");
          if (binding.type === "native")
            yield* binding.authorize.pipe(
              Effect.mapError(() => viewerRefused("The original native client was retired.")),
            );
          else {
            if (Option.isNone(authority)) return yield* viewerUnsupported();
            if (binding.type === "incoming")
              yield* authority.value.recheckPreviewViewer(binding.scope, binding.input);
            else yield* authority.value.checkRecord(binding.envelope.recordId);
          }
          if (binding.type !== "outgoing") yield* resource(binding.input);
        });
        yield* check;
        if (binding.type === "outgoing") {
          if (Option.isNone(peers) || peers.value.openPreviewViewer === undefined)
            return yield* viewerUnsupported();
          const remote = yield* peers.value.openPreviewViewer({
            request: { ...binding.input, viewerId: binding.access.viewerId },
            authority: binding.envelope,
          });
          yield* check;
          if (
            remote.access.viewerId !== binding.access.viewerId ||
            remote.origin !== binding.origin
          )
            return yield* viewerRefused("The destination changed the original browser viewer.");
          binding.access = remote.access;
        }
        const issued = yield* sessions
          .issueViewerToken({
            sessionId,
            ...binding.input,
            viewerId: input.viewerId!,
            generation: binding.generation,
          })
          .pipe(
            Effect.mapError(() => viewerRefused("The active browser viewer could not be renewed.")),
          );
        yield* check;
        return {
          viewerId: issued.viewerId,
          ticket: issued.token,
          expiresAt:
            binding.type === "outgoing" &&
            binding.access.expiresAt < DateTime.formatIso(issued.expiresAt)
              ? binding.access.expiresAt
              : DateTime.formatIso(issued.expiresAt),
          streamBasePath: PREVIEW_STREAM_ROUTE_PREFIX,
        } satisfies PreviewViewerAccess;
      });
    return PreviewViewers.of({
      open: (session, untrusted) =>
        Effect.gen(function* () {
          const input = yield* decodeViewerInput(untrusted).pipe(
            Effect.mapError(() => viewerRefused("Invalid browser viewer resource.")),
          );
          if (input.viewerId !== undefined) return yield* renew(session.sessionId, input);
          const read = yield* EnvironmentAuth.nativeSessionAuthorization(
            session,
            AuthOrchestrationReadScope,
          ).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, nativeSessions));
          const operate =
            input.operation === "control"
              ? yield* EnvironmentAuth.nativeSessionAuthorization(
                  session,
                  AuthPreviewOperateScope,
                ).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, nativeSessions))
              : Effect.void;
          const authorize = read.pipe(Effect.andThen(operate));
          yield* authorize.pipe(
            Effect.mapError(() =>
              viewerRefused("The original native client no longer permits this browser operation."),
            ),
          );
          if (input.resource.environmentId === here) {
            yield* resource(input);
            return yield* issue(session.sessionId, {
              type: "native",
              input,
              generation: input.resource.serverEpoch,
              authorize,
            });
          }
          if (
            Option.isNone(authority) ||
            Option.isNone(peers) ||
            Option.isNone(links) ||
            peers.value.openPreviewViewer === undefined
          )
            return yield* viewerUnsupported();
          const link = yield* links.value
            .get(input.resource.environmentId)
            .pipe(Effect.mapError(() => viewerRefused("The browser environment is not linked.")));
          if (Option.isNone(link))
            return yield* viewerRefused("The browser environment is not linked.");
          const envelope = yield* authority.value.preparePreviewViewer(
            session.sessionId,
            input.resource.environmentId,
            link.value.launchIdentity,
            input,
          );
          const remote = yield* peers.value.openPreviewViewer({
            request: input,
            authority: envelope,
          });
          const binding: PreviewViewerBinding = {
            type: "outgoing",
            input,
            generation: envelope.generation,
            envelope,
            ...remote,
          };
          return yield* authority.value.checkRecord(envelope.recordId).pipe(
            Effect.andThen(issue(session.sessionId, binding)),
            Effect.onExit((exit) =>
              Exit.isFailure(exit) ? closeRemote(binding).pipe(Effect.asVoid) : Effect.void,
            ),
          );
        }),
      accept: (scope, input) =>
        Effect.gen(function* () {
          if (
            Option.isNone(authority) ||
            scope.client?.linked !== true ||
            scope.authority === undefined ||
            !("envelope" in scope.authority)
          )
            return yield* viewerRefused(
              "Browser viewing needs its original authenticated peer authority.",
            );
          if (input.viewerId !== undefined)
            return yield* renew(AuthSessionId.make(scope.client.sessionId), input, scope);
          yield* authority.value.recheckPreviewViewer(scope, input);
          yield* resource(input);
          yield* authority.value.recheckPreviewViewer(scope, input);
          return yield* issue(AuthSessionId.make(scope.client.sessionId), {
            type: "incoming",
            input,
            generation: scope.authority.peer.generation,
            scope,
          });
        }),
      close: (sessionId, id) =>
        sessions
          .closeViewer(sessionId, id)
          .pipe(Effect.flatMap((closed) => (closed ? retire(id) : Effect.succeed(false)))),
      resolve: (id, input, generation) =>
        Effect.gen(function* () {
          const binding = bindings.get(id)?.binding;
          if (
            binding === undefined ||
            binding.generation !== generation ||
            binding.input.resource.environmentId !== input.resource.environmentId ||
            binding.input.resource.threadId !== input.resource.threadId ||
            binding.input.resource.tabId !== input.resource.tabId ||
            binding.input.resource.serverEpoch !== input.resource.serverEpoch ||
            binding.input.operation !== input.operation
          )
            return yield* viewerRefused("The exact browser viewer binding is unavailable.");
          if (binding.type === "native") {
            yield* binding.authorize.pipe(
              Effect.mapError(() =>
                viewerRefused(
                  "The original native client no longer permits this browser operation.",
                ),
              ),
            );
            yield* resource(input);
            return { type: "local" as const };
          }
          if (Option.isNone(authority)) return yield* viewerUnsupported();
          if (binding.type === "incoming") {
            yield* authority.value.recheckPreviewViewer(binding.scope, input);
            yield* resource(input);
            yield* authority.value.recheckPreviewViewer(binding.scope, input);
            return { type: "local" as const };
          }
          yield* authority.value.checkRecord(binding.envelope.recordId);
          return { type: "peer" as const, origin: binding.origin, ticket: binding.access.ticket };
        }),
    });
  }),
);
/** Matches `PREVIEW_STREAM_TAB_GONE_CODE` in the client. */
const TAB_GONE_CODE = 4404;

const DEFAULT_QUALITY = 70;
const MAX_SOCKET_BUFFER_BYTES = 8 * 1024 * 1024;
const textDecoder = new TextDecoder();
const decodeViewerMessage = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Union([
      PreviewStreamInput,
      Schema.StructWithRest(Schema.Struct({ type: Schema.Literal("ack") }), [
        Schema.Record(Schema.String, Schema.Unknown),
      ])
        .check(
          Schema.makeFilter(
            (input) => Object.keys(input).length === 1 || "Invalid frame acknowledgement.",
          ),
        )
        .pipe(Schema.decodeTo(Schema.Struct({ type: Schema.Literal("ack") }))),
    ]),
  ),
);

const validViewerMessage = (chunk: string | Uint8Array, canOperate: boolean) => {
  if (typeof chunk !== "string" || Buffer.byteLength(chunk) > 64 * 1024) return false;
  const decoded = decodeViewerMessage(chunk);
  return Option.isSome(decoded) && (decoded.value.type === "ack" || canOperate);
};

type NativeRecheck = Effect.Success<ReturnType<typeof EnvironmentAuth.nativeSessionAuthorization>>;
type PreviewRecheck = Effect.Effect<void, OrchestratorMcpFailure | Effect.Error<NativeRecheck>>;
type PreviewInvalidation = Effect.Effect<void, SessionStore.SessionCredentialVerificationError>;

const relaySocket = (
  request: HttpServerRequest.HttpServerRequest,
  endpoint: string,
  recheck: PreviewRecheck,
  invalidated: PreviewInvalidation,
  canOperate: boolean,
) =>
  Effect.gen(function* () {
    yield* recheck;
    const incoming = NodeHttpServerRequest.toIncomingMessage(request);
    delete incoming.headers["sec-websocket-extensions"];
    const client = yield* request.upgrade;
    const raw = yield* Effect.acquireRelease(
      Effect.sync(
        () =>
          new NodeSocket.NodeWS.WebSocket(endpoint, {
            perMessageDeflate: false,
            maxPayload: MAX_SOCKET_BUFFER_BYTES,
          }),
      ),
      (socket) => Effect.sync(() => socket.close()),
    );
    const upstream = yield* Socket.fromWebSocket(Effect.succeed(raw), {
      openTimeout: "10 seconds",
      highWaterMark: MAX_SOCKET_BUFFER_BYTES,
    });
    const fromPeer = yield* upstream.reader;
    const fromClient = yield* client.reader;
    const toClient = yield* client.writer;
    const flush = Effect.callback<void, Socket.SocketError>((resume) => {
      incoming.socket.write(new Uint8Array(0), (cause) =>
        resume(
          cause
            ? Effect.fail(
                new Socket.SocketError({ reason: new Socket.SocketWriteError({ cause }) }),
              )
            : Effect.void,
        ),
      );
    });
    const toPeer = (chunk: string | Uint8Array) =>
      Effect.callback<void, Socket.SocketError>((resume) => {
        raw.send(chunk, (cause) =>
          resume(
            cause
              ? Effect.fail(
                  new Socket.SocketError({ reason: new Socket.SocketWriteError({ cause }) }),
                )
              : Effect.void,
          ),
        );
      });
    const pump = (
      reader: Socket.Reader,
      write: (chunk: string | Uint8Array) => Effect.Effect<void, Socket.SocketError>,
      buffered: () => number,
      input: boolean,
    ) =>
      Effect.gen(function* () {
        while (true) {
          const chunks = yield* reader.pull;
          for (const chunk of chunks) {
            const bytes = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.byteLength;
            if (
              buffered() + bytes + 14 > MAX_SOCKET_BUFFER_BYTES ||
              (input && !validViewerMessage(chunk, canOperate))
            )
              return yield* Effect.interrupt;
            yield* recheck;
            yield* write(chunk);
          }
        }
      });
    yield* Effect.raceFirst(
      invalidated,
      Effect.raceFirst(
        pump(
          fromPeer,
          (chunk) => toClient.write(chunk).pipe(Effect.andThen(flush)),
          () => incoming.socket.writableLength,
          false,
        ).pipe(
          Effect.catchTags({
            SocketError: (error) =>
              error.reason._tag === "SocketCloseError" && error.reason.code === 1013
                ? toClient
                    .write(new Socket.CloseEvent(1013, "browser input backlog"))
                    .pipe(Effect.andThen(flush), Effect.andThen(Effect.interrupt))
                : Effect.fail(error),
          }),
        ),
        pump(fromClient, toPeer, () => raw.bufferedAmount, true),
      ),
    );
    return HttpServerResponse.empty();
  }).pipe(
    Effect.scoped,
    Effect.catchCause(() => Effect.succeed(HttpServerResponse.empty())),
  );

const relayFile = (
  request: HttpServerRequest.HttpServerRequest,
  endpoint: string,
  recheck: PreviewRecheck,
  invalidated: PreviewInvalidation,
) =>
  Effect.gen(function* () {
    yield* recheck;
    const clientOwner = yield* Effect.serviceOption(HttpClient.HttpClient);
    if (Option.isNone(clientOwner)) return yield* viewerUnsupported();
    const client = HttpClient.withScope(clientOwner.value);
    const headers: Record<string, string> = {};
    for (const key of ["range", "if-range", "content-type"])
      if (request.headers[key] !== undefined) headers[key] = request.headers[key];
    let bytes = 0;
    const body = request.stream.pipe(
      Stream.tap((chunk) => {
        bytes += chunk.byteLength;
        return bytes > UPLOAD_MAX_FILE_BYTES
          ? Effect.fail(viewerRefused("The browser upload exceeds its byte limit."))
          : recheck;
      }),
      Stream.interruptWhen(invalidated),
    );
    const response = yield* client
      .execute(
        HttpClientRequest.make(request.method)(endpoint).pipe(
          HttpClientRequest.setHeaders(headers),
          request.method === "POST"
            ? HttpClientRequest.bodyStream(body, { contentType: request.headers["content-type"] })
            : (self) => self,
        ),
      )
      .pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }));
    if (response.status >= 300 && response.status < 400)
      return HttpServerResponse.text("Browser redirect refused", { status: 502 });
    const outgoing: Record<string, string> = { "cache-control": "no-store, no-transform" };
    for (const key of [
      "content-type",
      "etag",
      "content-length",
      "content-range",
      "accept-ranges",
      "content-disposition",
      "content-security-policy",
      "x-content-type-options",
    ])
      if (response.headers[key] !== undefined) outgoing[key] = response.headers[key];
    yield* recheck;
    return HttpServerResponse.stream(
      response.stream.pipe(
        Stream.tap(() => recheck),
        Stream.interruptWhen(invalidated),
      ),
      { status: response.status, headers: outgoing },
    );
  });

const viewerRequest = (url: URL) =>
  Effect.gen(function* () {
    const sessionOwner = yield* Effect.serviceOption(SessionStore.SessionStore);
    const viewerOwner = yield* Effect.serviceOption(PreviewViewers);
    if (Option.isNone(sessionOwner) || Option.isNone(viewerOwner))
      return yield* viewerUnsupported();
    const sessions = sessionOwner.value;
    const viewers = viewerOwner.value;
    const ticket = url.searchParams.get("viewerTicket")!;
    const authenticate = sessions
      .admitViewerToken(ticket)
      .pipe(Effect.mapError(() => viewerRefused("The browser viewer expired or was retired.")));
    const viewer = yield* authenticate;
    if (
      viewer.resource.type !== "preview" ||
      url.searchParams.get("threadId") !== viewer.resource.threadId ||
      url.searchParams.get("tabId") !== viewer.resource.tabId
    )
      return yield* viewerRefused("The browser viewer names another resource.");
    const allowed = new Set(["viewerTicket", "threadId", "tabId"]);
    if (url.pathname.endsWith("/ws"))
      for (const key of ["maxWidth", "maxHeight", "quality", "interactive"]) allowed.add(key);
    if (url.pathname.endsWith("/download")) allowed.add("id");
    if (url.pathname.endsWith("/upload")) allowed.add("chooser");
    for (const key of url.searchParams.keys())
      if (!allowed.has(key) || url.searchParams.getAll(key).length !== 1)
        return yield* viewerRefused("Invalid browser resource selector.");
    if (url.pathname.endsWith("/upload") && viewer.operation !== "control")
      return yield* viewerRefused("This browser viewer is read-only.");
    const input = { resource: viewer.resource, operation: viewer.operation };
    const resolve = viewers.resolve(viewer.viewerId, input, viewer.generation);
    const resolved = yield* resolve;
    return {
      viewerId: viewer.viewerId,
      resolved,
      canOperate: viewer.operation === "control" && url.searchParams.get("interactive") !== "false",
      recheck: viewer.recheck.pipe(
        Effect.mapError(() => viewerRefused("The admitted browser viewer was retired.")),
        Effect.andThen(resolve),
        Effect.asVoid,
      ),
      invalidated: sessions.awaitViewerInvalidation(viewer.viewerId),
    };
  });

const intParam = (params: URLSearchParams, name: string, fallback: number, max: number) => {
  const value = Number(params.get(name));
  return Number.isFinite(value) && value > 0 ? Math.min(Math.round(value), max) : fallback;
};

type Frame = Extract<ServerBrowser.ServerBrowserViewerOutput, { readonly _tag: "frame" }>;

const isAck = (message: unknown) =>
  typeof message === "object" && message !== null && "type" in message && message.type === "ack";

const parseMessage = (chunk: Uint8Array | string): unknown => {
  try {
    return JSON.parse(typeof chunk === "string" ? chunk : textDecoder.decode(chunk));
  } catch {
    return null;
  }
};

const makeHandler = (browser: ServerBrowser.ServerBrowser["Service"] | undefined) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = HttpServerRequest.toURL(request);
    if (Option.isNone(url)) return HttpServerResponse.text("Bad Request", { status: 400 });
    const kind =
      url.value.pathname === `${PREVIEW_STREAM_ROUTE_PREFIX}/ws`
        ? "ws"
        : url.value.pathname === `${PREVIEW_STREAM_ROUTE_PREFIX}/download`
          ? "download"
          : url.value.pathname === `${PREVIEW_STREAM_ROUTE_PREFIX}/upload`
            ? "upload"
            : undefined;
    if (kind === undefined) return HttpServerResponse.text("Not Found", { status: 404 });
    if (
      (kind === "ws" &&
        (request.method !== "GET" || request.headers.upgrade?.toLowerCase() !== "websocket")) ||
      (kind === "download" && request.method !== "GET" && request.method !== "HEAD") ||
      (kind === "upload" && request.method !== "POST")
    )
      return HttpServerResponse.text("Method Not Allowed", { status: 405 });
    const params = url.value.searchParams;
    const access = params.has("viewerTicket") ? yield* viewerRequest(url.value) : undefined;
    if (access?.resolved.type === "peer") {
      const upstream = new URLSearchParams(params);
      upstream.set("viewerTicket", access.resolved.ticket);
      const endpoint = `${access.resolved.origin}${PREVIEW_STREAM_ROUTE_PREFIX}/${kind}?${upstream}`;
      return kind === "ws"
        ? yield* relaySocket(
            request,
            endpoint.replace(/^http/, "ws"),
            access.recheck,
            access.invalidated,
            access.canOperate,
          )
        : yield* relayFile(request, endpoint, access.recheck, access.invalidated);
    }
    if (browser === undefined)
      return HttpServerResponse.text("The local browser service is unavailable", { status: 503 });
    if (kind === "download")
      return yield* serveDownload(browser, params, access?.recheck, access?.invalidated);
    if (kind === "upload")
      return yield* receiveUpload(browser, params, access?.recheck, access?.viewerId);
    const viewing =
      access ??
      (yield* Effect.gen(function* () {
        const session = yield* authenticateMediaRequest(AuthOrchestrationReadScope);
        const canOperate =
          session.scopes.includes(AuthPreviewOperateScope) && params.get("interactive") !== "false";
        const read = yield* EnvironmentAuth.nativeSessionAuthorization(
          session,
          AuthOrchestrationReadScope,
        );
        const operate = canOperate
          ? yield* EnvironmentAuth.nativeSessionAuthorization(session, AuthPreviewOperateScope)
          : Effect.void;
        const sessionOwner = yield* Effect.serviceOption(SessionStore.SessionStore);
        return {
          viewerId: session.sessionId,
          canOperate,
          recheck: read.pipe(Effect.andThen(operate)),
          invalidated: Option.isSome(sessionOwner)
            ? sessionOwner.value.awaitInvalidation(session.sessionId)
            : Effect.never,
        };
      }));
    const { canOperate } = viewing;
    const recheck: PreviewRecheck = viewing.recheck;
    const invalidated: PreviewInvalidation = viewing.invalidated;
    yield* recheck;
    const threadId = params.get("threadId") ?? "";
    const tabId = params.get("tabId") ?? "";
    if (threadId.length === 0 || tabId.length === 0) {
      return HttpServerResponse.text("Not Found", { status: 404 });
    }
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const attached = yield* browser
          .attachViewer({
            threadId,
            tabId,
            canOperate,
            authorize: () => Effect.runPromise(recheck),
            resourceViewerId: viewing.viewerId,
            maxWidth: intParam(params, "maxWidth", 1280, 7680),
            maxHeight: intParam(params, "maxHeight", 800, 4320),
            quality: intParam(params, "quality", DEFAULT_QUALITY, 100),
          })
          .pipe(
            Effect.map((viewer) => ({ _tag: "attached" as const, viewer })),
            Effect.catchTags({
              ServerBrowserTabNotFoundError: () => Effect.succeed({ _tag: "gone" as const }),
              ServerBrowserLaunchError: (error) => {
                const setup = hostSetup(error.cause);
                return setup === undefined
                  ? Effect.fail(error)
                  : Effect.succeed({ _tag: "hostSetup" as const, reason: encodeHostSetup(setup) });
              },
            }),
          );
        const incoming = NodeHttpServerRequest.toIncomingMessage(request);
        // JPEGs are already compressed. Disabling deflate also keeps all
        // pending writes in the socket buffer we bound below, not a zlib queue.
        delete incoming.headers["sec-websocket-extensions"];
        const transport = incoming.socket;
        const socket = yield* request.upgrade;
        const reader = yield* socket.reader;
        const writer = yield* socket.writer;
        // A refused upgrade reads as an auth failure to ticket clients, so a
        // missing tab is a close code they stop on.
        const gone = writer.write(new Socket.CloseEvent(TAB_GONE_CODE, "tab closed"));
        if (attached._tag === "gone") {
          yield* gone;
          return HttpServerResponse.empty();
        }
        if (attached._tag === "hostSetup") {
          yield* writer.write(
            new Socket.CloseEvent(PREVIEW_STREAM_HOST_SETUP_CLOSE_CODE, attached.reason),
          );
          return HttpServerResponse.empty();
        }
        const viewer = attached.viewer;
        // `write` returns once the frame is queued, so Chromium's ack for each
        // frame waits for the viewer's `ack` message instead. Only one frame
        // is on the wire and only the newest waits behind it, so replies never
        // queue behind stale images on a slow link.
        let inFlight: Effect.Effect<void> | null = null;
        let pending: Frame | null = null;
        const disconnectSlowViewer = Effect.sync(() => transport.destroy()).pipe(
          Effect.andThen(Effect.interrupt),
        );
        const write = (data: Uint8Array | string) =>
          Effect.suspend(() => {
            const bytes = typeof data === "string" ? Buffer.byteLength(data) : data.byteLength;
            if (transport.writableLength + bytes + 14 > MAX_SOCKET_BUFFER_BYTES) {
              return disconnectSlowViewer;
            }
            return recheck.pipe(Effect.andThen(writer.write(data)));
          });
        const sendFrame = (frame: Frame) => {
          inFlight = frame.ack;
          return write(frame.data);
        };
        const sendOutput = Queue.take(viewer.output).pipe(
          Effect.flatMap((output) => {
            switch (output._tag) {
              case "frame": {
                if (inFlight === null) return sendFrame(output);
                const superseded = pending;
                pending = output;
                // Never sent, but Chromium still waits for its ack.
                return superseded ? Effect.forkScoped(superseded.ack) : Effect.void;
              }
              case "viewport":
              case "control":
              case "clipboard":
              case "download":
              case "fileChooser":
              case "fileChooserClosed":
              case "popup":
              case "pointer":
              case "probe": {
                const { _tag: type, ...data } = output;
                return write(JSON.stringify({ type, ...data }));
              }
              case "gone":
                return gone.pipe(Effect.andThen(Effect.interrupt));
              case "reconnect":
                return writer
                  .write(new Socket.CloseEvent(1012, "reconnect"))
                  .pipe(Effect.andThen(Effect.interrupt));
            }
          }),
        );
        const receive = (chunk: Uint8Array | string) => {
          if (access !== undefined && !validViewerMessage(chunk, canOperate))
            return Effect.interrupt;
          const message = parseMessage(chunk);
          // Ownership serializes actions in the service. Keep reading so dialog
          // replies and takeover can unblock an action already waiting on the page.
          if (!isAck(message))
            return recheck.pipe(
              Effect.andThen(
                canOperate
                  ? viewer.enqueueInput(message).pipe(
                      Effect.catchTags({
                        BrowserInputOverloaded: () =>
                          writer
                            .write(new Socket.CloseEvent(1013, "browser input backlog"))
                            .pipe(Effect.andThen(Effect.interrupt)),
                      }),
                    )
                  : Effect.void,
              ),
            );
          const ack = inFlight;
          const next = pending;
          inFlight = null;
          pending = null;
          // Forked: Chromium acks are paced and must not hold up input.
          return Effect.andThen(
            ack ? Effect.forkScoped(ack) : Effect.void,
            next ? sendFrame(next) : Effect.void,
          );
        };
        const receiveInput = reader.pull.pipe(
          Effect.flatMap((chunks) => Effect.forEach(chunks, receive, { discard: true })),
        );
        return yield* Effect.raceFirst(
          invalidated.pipe(Effect.andThen(Effect.interrupt)),
          Effect.raceFirst(Effect.forever(sendOutput), Effect.forever(receiveInput)),
        );
      }),
    ).pipe(
      Effect.catchTags({
        ServerBrowserLaunchError: (error) =>
          Effect.logWarning("server preview browser failed to start", { cause: error.cause }).pipe(
            Effect.as(HttpServerResponse.text("Service Unavailable", { status: 503 })),
          ),
      }),
      Effect.catch(() => Effect.succeed(HttpServerResponse.empty())),
    );
  }).pipe(
    Effect.catchTags({
      OrchestratorMcpFailure: () =>
        Effect.succeed(
          HttpServerResponse.text("Current browser viewer authority refused", { status: 403 }),
        ),
    }),
  );

const hostSetup = (cause: unknown): PreviewStreamHostSetup | undefined =>
  isSandboxError(cause)
    ? { need: "sandbox", command: cause.setupCommand }
    : isLibrariesError(cause)
      ? { need: "libraries", command: cause.setupCommand }
      : undefined;
const isSandboxError = Schema.is(PreviewBrowserHost.PreviewBrowserSandboxError);
const isLibrariesError = Schema.is(PreviewBrowserHost.PreviewBrowserLibrariesError);
const encodeHostSetup = Schema.encodeSync(Schema.fromJsonString(PreviewStreamHostSetup));

/** `GET /api/preview-stream/download?threadId&tabId&id`: a file a server tab downloaded. */
const serveDownload = (
  browser: ServerBrowser.ServerBrowser["Service"],
  params: URLSearchParams,
  authorization?: PreviewRecheck,
  invalidated: PreviewInvalidation = Effect.never,
) =>
  Effect.gen(function* () {
    const recheck =
      authorization ??
      (yield* Effect.gen(function* () {
        const session = yield* authenticateMediaRequest(AuthOrchestrationReadScope);
        return yield* EnvironmentAuth.nativeSessionAuthorization(
          session,
          AuthOrchestrationReadScope,
        );
      }));
    yield* recheck;
    const download = yield* browser.openDownload({
      threadId: params.get("threadId") ?? "",
      tabId: params.get("tabId") ?? "",
      downloadId: params.get("id") ?? "",
    });
    if (Option.isNone(download)) return HttpServerResponse.text("Not Found", { status: 404 });
    yield* recheck;
    if (download.value.resource !== undefined) {
      if (browser.inspectDownload === undefined || browser.readDownloadBlock === undefined)
        return yield* viewerUnsupported();
      const resource = download.value.resource;
      const current = recheck.pipe(
        Effect.mapError(() => viewerRefused("The native download reader was retired.")),
      );
      const authorize = browser.inspectDownload(resource, current);
      const read = browser.readDownloadBlock;
      const request = yield* HttpServerRequest.HttpServerRequest;
      const response = yield* nativeDownloadResponse(
        resource,
        { authorize, readBlock: (offset) => read(resource, offset, current) },
        request.method,
        request.headers.range,
        request.headers["if-range"],
      );
      return response.body._tag === "Stream"
        ? HttpServerResponse.setBody(
            response,
            HttpBody.stream(
              response.body.stream.pipe(Stream.interruptWhen(invalidated)),
              response.body.contentType,
              response.body.contentLength,
            ),
          )
        : response;
    }
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    // Canonicalize parent aliases while retaining the final entry's no-follow check.
    const directory = yield* fs.realPath(path.dirname(download.value.path));
    const filePath = path.join(directory, path.basename(download.value.path));
    yield* recheck;
    const file = yield* openMediaFile(filePath);
    if (file === null) return HttpServerResponse.text("Not Found", { status: 404 });
    const request = yield* HttpServerRequest.HttpServerRequest;
    const response = yield* assetFileResponse(
      { ...download.value, path: filePath, file, download: true },
      request.headers.range,
      request.headers["if-range"],
      request.method === "HEAD" ? "HEAD" : "GET",
    );
    yield* recheck;
    return response.body._tag === "Stream"
      ? HttpServerResponse.setBody(
          response,
          HttpBody.stream(
            response.body.stream.pipe(
              Stream.tap(() => recheck),
              Stream.interruptWhen(invalidated),
            ),
            response.body.contentType,
            response.body.contentLength,
          ),
        )
      : response;
  });

const UPLOAD_MAX_FILE_BYTES = 100 * 1024 * 1024;
const UPLOAD_MAX_FILES = 20;

/**
 * `POST /api/preview-stream/upload?threadId&tabId&chooser` with multipart `file`
 * parts: answers the page's open file picker. No parts cancels it.
 */
const receiveUpload = (
  browser: ServerBrowser.ServerBrowser["Service"],
  params: URLSearchParams,
  authorization?: PreviewRecheck,
  resourceViewerId?: string,
) =>
  Effect.gen(function* () {
    const recheck =
      authorization ??
      (yield* Effect.gen(function* () {
        const session = yield* authenticateMediaRequest(AuthPreviewOperateScope);
        resourceViewerId = session.sessionId;
        return yield* EnvironmentAuth.nativeSessionAuthorization(session, AuthPreviewOperateScope);
      }));
    yield* recheck;
    const request = yield* HttpServerRequest.HttpServerRequest;
    const fs = yield* FileSystem.FileSystem;
    const parts = yield* request.multipart.pipe(
      Effect.provideContext(
        Multipart.limitsServices({
          maxParts: UPLOAD_MAX_FILES,
          maxFileSize: UPLOAD_MAX_FILE_BYTES,
          maxTotalSize: UPLOAD_MAX_FILE_BYTES,
        }),
      ),
    );
    yield* recheck;
    const files = yield* Effect.forEach(
      Object.values(parts).flatMap((value) =>
        Array.isArray(value) ? value.filter(Multipart.isPersistedFile) : [],
      ),
      (file) =>
        fs.readFile(file.path).pipe(
          Effect.tap(() => recheck),
          Effect.map((bytes) => ({
            name: file.name,
            mimeType: file.contentType,
            buffer: Buffer.from(bytes),
          })),
        ),
    );
    const answered = yield* browser.answerFileChooser({
      threadId: params.get("threadId") ?? "",
      tabId: params.get("tabId") ?? "",
      chooserId: params.get("chooser") ?? "",
      files,
      authorize: () => Effect.runPromise(recheck),
      ...(resourceViewerId === undefined ? {} : { resourceViewerId }),
    });
    yield* recheck;
    return answered
      ? HttpServerResponse.empty({ status: 204 })
      : HttpServerResponse.text("The page's file picker is no longer open.", { status: 409 });
  }).pipe(Effect.scoped);

export const routeLayer = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
    const nativeSessions = yield* AuthSessions.AuthSessionRepository;
    const sessions = yield* Effect.serviceOption(SessionStore.SessionStore);
    const viewers = yield* Effect.serviceOption(PreviewViewers);
    const client = yield* Effect.serviceOption(HttpClient.HttpClient);
    const platform = yield* Effect.context<
      HttpPlatform.HttpPlatform | FileSystem.FileSystem | Path.Path
    >();
    let handler = makeHandler(Option.getOrUndefined(browser)).pipe(
      Effect.provideContext(platform),
      Effect.provideService(AuthSessions.AuthSessionRepository, nativeSessions),
    );
    if (Option.isSome(sessions))
      handler = handler.pipe(Effect.provideService(SessionStore.SessionStore, sessions.value));
    if (Option.isSome(viewers))
      handler = handler.pipe(Effect.provideService(PreviewViewers, viewers.value));
    if (Option.isSome(client))
      handler = handler.pipe(Effect.provideService(HttpClient.HttpClient, client.value));
    yield* router.add("GET", `${PREVIEW_STREAM_ROUTE_PREFIX}/*`, handler);
    yield* router.add("POST", `${PREVIEW_STREAM_ROUTE_PREFIX}/upload`, handler);
  }),
);
