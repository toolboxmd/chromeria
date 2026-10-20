import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import type * as Invocation from "../mcp/McpInvocationContext.ts";
/**
 * The hub binds loopback and is never reachable directly: serve-sim exposes a shell-exec route
 * and serve-emu's action routes are unauthenticated, so the only way to a device stream is
 * through this route, which requires an environment session with read scope (operate scope for
 * input and tuning).
 */
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  DeviceHostUnavailableError,
  DeviceOperationError,
  OrchestratorMcpFailure,
  AuthSessionId,
  DeviceViewerOpenInput,
  DeviceViewerClientRpcGroup,
  WS_METHODS,
  closedDeviceCommand,
  type DeviceViewerAccess,
  type DeviceError,
  EnvironmentHttpApi,
  EnvironmentOperationForbiddenError,
  EnvironmentAuthenticatedPrincipal,
  type DeviceViewerResource,
  type EnvironmentAuthInvalidError,
  type EnvironmentInternalError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as DateTime from "effect/DateTime";
import * as ByteSize from "effect/ByteSize";
import * as HttpIncomingMessage from "effect/http/HttpIncomingMessage";
import * as Stream from "effect/Stream";
import * as SessionStore from "../auth/SessionStore.ts";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Option from "effect/Option";
import {
  HttpClient,
  HttpClientRequest,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import * as Socket from "effect/socket/Socket";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeHttpServerRequest from "@effect/platform-node/NodeHttpServerRequest";

import {
  authenticateMediaRequest,
  failEnvironmentAuthInvalid,
  failEnvironmentInternal,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as DeviceService from "./DeviceService.ts";
import { repeatStillFrame } from "./mjpegStillFrame.ts";

type ViewerAccessError =
  | OrchestratorMcpFailure
  | DeviceOperationError
  | DeviceHostUnavailableError
  | EnvironmentAuthInvalidError
  | EnvironmentInternalError;
const decodeViewerInput = Schema.decodeUnknownEffect(DeviceViewerOpenInput, {
  onExcessProperty: "error",
});
/** The native transport supplies the session; no public field supplies an actor. */
const issueViewer = Effect.fn("DeviceHubProxy.issueViewer")(function* (
  session: Pick<AuthenticatedSession, "sessionId" | "scopes">,
  untrusted: DeviceViewerOpenInput,
): Effect.fn.Return<
  DeviceViewerAccess,
  DeviceError,
  DeviceService.DeviceService | ServerEnvironment.ServerEnvironment | SessionStore.SessionStore
> {
  const input = yield* decodeViewerInput(untrusted).pipe(
    Effect.mapError(
      (cause) =>
        new DeviceHostUnavailableError({
          hostId: "local",
          reason: "Invalid device viewer request.",
          cause,
        }),
    ),
  );
  if (
    !session.scopes.includes(AuthOrchestrationReadScope) ||
    (input.operation === "control" && !session.scopes.includes(AuthOrchestrationOperateScope))
  )
    return yield* new DeviceHostUnavailableError({
      hostId: input.resource.hostId,
      reason: "This source session cannot perform that viewer operation.",
    });
  const here = yield* ServerEnvironment.ServerEnvironment.pipe(
    Effect.flatMap((owner) => owner.getEnvironmentId),
  );
  if (input.resource.environmentId !== here)
    return yield* new DeviceHostUnavailableError({
      hostId: input.resource.hostId,
      reason: "The linked environment does not yet support resource-bound device viewing.",
    });
  const devices = yield* DeviceService.DeviceService;
  yield* devices.authorizeViewer(input.resource);
  const sessions = yield* SessionStore.SessionStore;
  const issued = yield* sessions
    .issueViewerToken({
      sessionId: session.sessionId,
      ...input,
      generation: input.resource.toolSessionId,
    })
    .pipe(
      Effect.mapError(
        (cause) =>
          new DeviceHostUnavailableError({
            hostId: input.resource.hostId,
            reason: "Viewer access could not be issued.",
            cause,
          }),
      ),
    );
  return {
    viewerId: issued.viewerId,
    ticket: issued.token,
    expiresAt: DateTime.formatIso(issued.expiresAt),
    hubBasePath: DeviceService.DEVICE_HUB_ROUTE_PREFIX,
  };
});

type ResolvedDeviceViewer = { readonly origin: string; readonly ticket?: string };
type DeviceViewerBinding =
  | {
      readonly type: "incoming";
      readonly scope: Invocation.McpInvocationScope;
      readonly input: DeviceViewerOpenInput;
      readonly generation: string;
    }
  | {
      readonly type: "outgoing";
      readonly origin: string;
      access: DeviceViewerAccess;
      readonly input: DeviceViewerOpenInput;
      readonly envelope: import("@t3tools/contracts").PeerAuthorityEnvelope;
    };
export class DeviceViewers extends Context.Service<
  DeviceViewers,
  {
    readonly open: (
      session: Pick<AuthenticatedSession, "sessionId" | "scopes">,
      input: DeviceViewerOpenInput,
    ) => Effect.Effect<DeviceViewerAccess, DeviceError | OrchestratorMcpFailure>;
    readonly close: (sessionId: AuthSessionId, viewerId: string) => Effect.Effect<boolean>;
    readonly accept: (
      scope: Invocation.McpInvocationScope,
      input: DeviceViewerOpenInput,
    ) => Effect.Effect<DeviceViewerAccess, DeviceError | OrchestratorMcpFailure>;
    readonly resolve: (
      viewerId: string,
      input: DeviceViewerOpenInput,
      generation: string,
    ) => Effect.Effect<
      ResolvedDeviceViewer,
      DeviceHostUnavailableError | DeviceOperationError | OrchestratorMcpFailure
    >;
  }
>()("t3/device/DeviceHubProxy/DeviceViewers") {}

/** Native RPC admission shares the viewer owner with HTTP and media transport. */
export const clientHandlers = (session: AuthenticatedSession, viewers: DeviceViewers["Service"]) =>
  DeviceViewerClientRpcGroup.of({
    [WS_METHODS.deviceViewerOpen]: (input) =>
      viewers.open(session, input).pipe(
        Effect.mapError(
          (cause) =>
            new DeviceHostUnavailableError({
              hostId: input.resource.hostId,
              reason: "The device viewer is unavailable or no longer authorized.",
              cause,
            }),
        ),
      ),
    [WS_METHODS.deviceViewerClose]: (input) => viewers.close(session.sessionId, input.viewerId),
  });

export const layerViewers = Layer.effect(
  DeviceViewers,
  Effect.gen(function* () {
    const devices = yield* DeviceService.DeviceService;
    const sessions = yield* SessionStore.SessionStore;
    const environment = yield* ServerEnvironment.ServerEnvironment;
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    const links = yield* Effect.serviceOption(PeerLinks.PeerLinks);
    const peers = yield* Effect.serviceOption(PeerMcpClient.PeerMcpClient);
    const lifetime = yield* Scope.Scope;
    const here = yield* environment.getEnvironmentId;
    // The session store owns credentials and expiry. These bindings hold only
    // server-private routing and the original verified invocation for that viewer.
    const bindings = new Map<
      string,
      {
        binding: DeviceViewerBinding;
        readonly sessionId: AuthSessionId;
        retiring: boolean;
        retired: Deferred.Deferred<boolean>;
      }
    >();
    const closeRemote = (binding: DeviceViewerBinding) =>
      binding.type === "outgoing"
        ? Option.isSome(peers) && peers.value.closeDeviceViewer !== undefined
          ? peers.value
              .closeDeviceViewer({
                environmentId: binding.input.resource.environmentId,
                generation: binding.envelope.generation,
                viewerId: binding.access.viewerId,
              })
              .pipe(
                Effect.timeout("5 seconds"),
                Effect.orElseSucceed(() => false),
              )
          : Effect.succeed(false)
        : Effect.succeed(true);
    const retire = (viewerId: string): Effect.Effect<boolean> =>
      Effect.suspend(() => {
        const entry = bindings.get(viewerId);
        if (entry === undefined) return Effect.succeed(true);
        if (entry.retiring) return Deferred.await(entry.retired);
        entry.retiring = true;
        const binding = entry.binding;
        return closeRemote(binding).pipe(
          Effect.tap((closed) => Deferred.succeed(entry.retired, closed)),
          Effect.ensuring(
            Effect.sync(() => {
              bindings.delete(viewerId);
            }),
          ),
        );
      });
    const issue = (
      sessionId: AuthSessionId,
      input: DeviceViewerOpenInput,
      generation: string,
      binding: DeviceViewerBinding,
    ) =>
      Effect.gen(function* () {
        const issued = yield* sessions.issueViewerToken({ sessionId, ...input, generation }).pipe(
          Effect.mapError(
            (cause) =>
              new DeviceHostUnavailableError({
                hostId: input.resource.hostId,
                reason: "The source viewer could not be created.",
                cause,
              }),
          ),
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
        return {
          viewerId: issued.viewerId,
          ticket: issued.token,
          expiresAt: DateTime.formatIso(issued.expiresAt),
          hubBasePath: DeviceService.DEVICE_HUB_ROUTE_PREFIX,
        } satisfies DeviceViewerAccess;
      });
    const renew = (
      sessionId: AuthSessionId,
      input: DeviceViewerOpenInput,
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
          binding.input.resource.hostId !== input.resource.hostId ||
          binding.input.resource.deviceId !== input.resource.deviceId ||
          binding.input.resource.platform !== input.resource.platform ||
          binding.input.resource.toolSessionId !== input.resource.toolSessionId ||
          binding.input.operation !== input.operation ||
          (scope !== undefined &&
            (binding.type !== "incoming" || scope.authority?.id !== binding.scope.authority?.id))
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The exact original device viewer cannot be renewed.",
          });
        const check = Effect.gen(function* () {
          if (bindings.get(input.viewerId!) !== entry || entry.retiring || Option.isNone(authority))
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The device viewer was retired.",
            });
          if (binding.type === "incoming") {
            yield* authority.value.recheckDeviceViewer(binding.scope, binding.input);
            yield* devices.authorizeViewer(binding.input.resource);
            yield* authority.value.recheckDeviceViewer(binding.scope, binding.input);
          } else yield* authority.value.checkRecord(binding.envelope.recordId);
        });
        yield* check;
        if (binding.type === "outgoing") {
          if (Option.isNone(peers) || peers.value.openDeviceViewer === undefined)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Current device admission is unsupported.",
            });
          const remote = yield* peers.value.openDeviceViewer({
            request: { ...binding.input, viewerId: binding.access.viewerId },
            authority: binding.envelope,
          });
          yield* check;
          if (
            remote.access.viewerId !== binding.access.viewerId ||
            remote.origin !== binding.origin
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The destination changed the original device viewer.",
            });
          binding.access = remote.access;
        }
        const issued = yield* sessions
          .issueViewerToken({
            sessionId,
            ...binding.input,
            viewerId: input.viewerId!,
            generation:
              binding.type === "incoming" ? binding.generation : binding.envelope.generation,
          })
          .pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "capability_denied",
                  message: "The active device viewer could not be renewed.",
                }),
            ),
          );
        yield* check;
        return {
          viewerId: issued.viewerId,
          ticket: issued.token,
          expiresAt: DateTime.formatIso(issued.expiresAt),
          hubBasePath: DeviceService.DEVICE_HUB_ROUTE_PREFIX,
        } satisfies DeviceViewerAccess;
      });
    return DeviceViewers.of({
      open: (session, input) =>
        Effect.gen(function* () {
          if (input.viewerId !== undefined && input.resource.environmentId !== here)
            return yield* renew(session.sessionId, input);
          if (input.resource.environmentId === here)
            return yield* issueViewer(session, input).pipe(
              Effect.provideService(DeviceService.DeviceService, devices),
              Effect.provideService(SessionStore.SessionStore, sessions),
              Effect.provideService(ServerEnvironment.ServerEnvironment, environment),
            );
          if (
            !session.scopes.includes(AuthOrchestrationReadScope) ||
            (input.operation === "control" &&
              !session.scopes.includes(AuthOrchestrationOperateScope))
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "This native client cannot perform that device viewer operation.",
            });
          if (
            Option.isNone(authority) ||
            Option.isNone(links) ||
            Option.isNone(peers) ||
            peers.value.openDeviceViewer === undefined
          )
            return yield* new DeviceHostUnavailableError({
              hostId: input.resource.hostId,
              reason: "Authenticated peer device viewing is unsupported.",
            });
          const link = yield* links.value.get(input.resource.environmentId).pipe(
            Effect.mapError(
              () =>
                new DeviceHostUnavailableError({
                  hostId: input.resource.hostId,
                  reason: "The device viewer link is unavailable.",
                }),
            ),
          );
          if (Option.isNone(link))
            return yield* new DeviceHostUnavailableError({
              hostId: input.resource.hostId,
              reason: "The device viewer environment is not linked.",
            });
          const envelope = yield* authority.value.prepareDeviceViewer(
            session.sessionId,
            input.resource.environmentId,
            link.value.launchIdentity,
            input,
          );
          const remote = yield* peers.value.openDeviceViewer({
            request: input,
            authority: envelope,
          });
          const binding: DeviceViewerBinding = { type: "outgoing", ...remote, input, envelope };
          return yield* authority.value.checkRecord(envelope.recordId).pipe(
            Effect.andThen(issue(session.sessionId, input, envelope.generation, binding)),
            Effect.onExit((exit) =>
              Exit.isFailure(exit) ? closeRemote(binding).pipe(Effect.asVoid) : Effect.void,
            ),
          );
        }),
      close: (sessionId, viewerId) =>
        Effect.gen(function* () {
          if (!(yield* sessions.closeViewer(sessionId, viewerId))) return false;
          return yield* retire(viewerId);
        }),
      accept: (scope, input) =>
        Effect.gen(function* () {
          if (
            Option.isNone(authority) ||
            scope.client?.linked !== true ||
            scope.authority === undefined ||
            !("envelope" in scope.authority)
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The device viewer needs its original authenticated peer authority.",
            });
          if (input.viewerId !== undefined)
            return yield* renew(AuthSessionId.make(scope.client.sessionId), input, scope);
          yield* authority.value.recheckDeviceViewer(scope, input);
          yield* devices.authorizeViewer(input.resource);
          yield* authority.value.recheckDeviceViewer(scope, input);
          const generation = scope.authority.peer.generation;
          return yield* issue(AuthSessionId.make(scope.client.sessionId), input, generation, {
            type: "incoming",
            scope,
            input,
            generation,
          });
        }),
      resolve: (viewerId, input, generation) =>
        Effect.gen(function* () {
          const binding = bindings.get(viewerId)?.binding;
          if (binding === undefined) {
            if (
              input.resource.environmentId !== here ||
              generation !== input.resource.toolSessionId
            )
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "The original device viewer binding is unavailable.",
              });
            return { origin: (yield* devices.authorizeViewer(input.resource)).hub.origin };
          }
          if (Option.isNone(authority))
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "Current device viewer authority is unavailable.",
            });
          if (binding.type === "incoming") {
            if (generation !== binding.generation)
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "The device viewer generation changed.",
              });
            yield* authority.value.recheckDeviceViewer(binding.scope, binding.input);
            const ready = yield* devices.authorizeViewer(binding.input.resource);
            yield* authority.value.recheckDeviceViewer(binding.scope, binding.input);
            return { origin: ready.hub.origin };
          }
          if (generation !== binding.envelope.generation)
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The device viewer generation changed.",
            });
          yield* authority.value.checkRecord(binding.envelope.recordId);
          return {
            origin: `${binding.origin}${DeviceService.DEVICE_HUB_ROUTE_PREFIX}`,
            ticket: binding.access.ticket,
          };
        }),
    });
  }),
);

/** Resource credentials use the same authenticated HTTP owner as media session credentials. */
export const layerApi = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "devices",
  Effect.fnUntraced(function* (handlers) {
    const viewers = yield* DeviceViewers;
    return handlers
      .handle("viewerOpen", ({ payload }) =>
        Effect.gen(function* () {
          const principal = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          if (payload.operation === "control")
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* viewers
            .open({ sessionId: principal.sessionId, scopes: [...principal.scopes] }, payload)
            .pipe(
              Effect.catch((error) =>
                Effect.currentParentSpan.pipe(
                  Effect.map((span) => span.traceId),
                  Effect.orElseSucceed(() => "unavailable"),
                  Effect.flatMap(
                    (traceId) =>
                      new EnvironmentOperationForbiddenError({
                        code: "operation_forbidden",
                        reason:
                          Schema.is(OrchestratorMcpFailure)(error) &&
                          error.code === "invalid_request"
                            ? "device_viewer_unsupported"
                            : "device_viewer_unavailable",
                        traceId,
                      }),
                  ),
                ),
              ),
            );
        }),
      )
      .handle("viewerClose", ({ payload }) =>
        Effect.gen(function* () {
          const principal = yield* EnvironmentAuthenticatedPrincipal;
          return yield* viewers.close(principal.sessionId, payload.viewerId);
        }),
      );
  }),
);

const ALLOWED_PATHS: ReadonlyArray<RegExp> = [
  /^\/api\/devices$/,
  /^\/vendor\/serve-sim\/api$/,
  /^\/vendor\/serve-sim\/api\/screenshot$/,
  /^\/vendor\/serve-sim\/api\/event-log(\/events)?$/,
  /^\/vendor\/serve-sim\/helper\/[^/]+\/(stream\.mjpeg|stream\.avcc|config|health|ax|foreground)$/,
  /^\/vendor\/serve-sim\/helper\/[^/]+\/panel\/(1|3)\/stream\.avcc$/,
  /^\/vendor\/serve-sim\/helper\/[^/]+\/webrtc\/(offer|close)$/,
  /^\/vendor\/serve-sim\/appstate$/,
  /^\/vendor\/serve-emu\/api\/(devices|screenshot|stream-mode|stream-settings|accessibility|fold)$/,
  /^\/vendor\/serve-emu\/health$/,
];

const MUTABLE_PATHS: ReadonlyArray<RegExp> = [
  /^\/vendor\/serve-sim\/api\/screenshot$/,
  // WebRTC signalling: opening and closing a view of the stream, so read scope.
  /^\/vendor\/serve-sim\/helper\/[^/]+\/webrtc\/(offer|close)$/,
  /^\/vendor\/serve-emu\/api\/(screenshot|stream-mode|stream-settings)$/,
  /^\/vendor\/serve-emu\/api\/fold$/,
];

const ALLOWED_WS_PATHS: ReadonlyArray<RegExp> = [
  /^\/api\/devices\/ws$/,
  /^\/vendor\/serve-sim\/helper\/ws$/,
  /^\/vendor\/serve-emu\/ws$/,
];

/** Query selectors are part of the route, not arbitrary hub configuration. */
const hasAllowedQuery = (path: string, params: URLSearchParams): boolean => {
  const allowed = new Set(["hostId", "wsTicket", "viewerTicket"]);
  if (path === "/api/devices") allowed.add("booted");
  if (
    /^\/vendor\/serve-(sim|emu)\/api(?:\/|$)/.test(path) ||
    path === "/vendor/serve-sim/appstate" ||
    path === "/vendor/serve-sim/helper/ws" ||
    path === "/vendor/serve-emu/ws"
  )
    allowed.add("device");
  if (/^\/vendor\/serve-sim\/api\/event-log(?:\/events)?$/.test(path)) {
    allowed.add("limit");
    allowed.add("since");
  }
  if (path === "/vendor/serve-emu/ws") {
    allowed.add("frame-meta");
    allowed.add("video");
  }
  const seen = new Set<string>();
  for (const [name, value] of params) {
    if (!allowed.has(name) || seen.has(name) || value.length === 0 || value.length > 4096)
      return false;
    seen.add(name);
  }
  return true;
};

/** Hop-by-hop and origin headers that must not cross the proxy. */
const DROPPED_REQUEST_HEADERS = new Set([
  "host",
  "connection",
  "upgrade",
  "sec-websocket-key",
  "sec-websocket-version",
  "sec-websocket-extensions",
  "sec-websocket-protocol",
  "cookie",
  "authorization",
  "dpop",
  "content-length",
  "accept-encoding",
  // The hub derives its own loopback scheme from this header before checking Origin.
  "x-forwarded-proto",
]);

const isWebSocketUpgrade = (request: HttpServerRequest.HttpServerRequest) =>
  request.headers.upgrade?.toLowerCase() === "websocket";

const forwardHeaders = (request: HttpServerRequest.HttpServerRequest, origin: string) => {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(request.headers)) {
    if (DROPPED_REQUEST_HEADERS.has(name) || value === undefined) continue;
    headers[name] = value;
  }
  // serve-emu refuses mutations whose Origin differs from the request origin.
  if (request.headers.origin !== undefined) headers.origin = origin;
  return headers;
};

// Effect drops its listeners before closing a socket that is still connecting, and `ws` then
// emits an `error` nobody handles, which exits the server.
export const upstreamWebSocketConstructor = Layer.effect(
  Socket.WebSocketConstructor,
  Effect.gen(function* () {
    const make = yield* Socket.WebSocketConstructor;
    return (url, options) => {
      const socket = make(url, options);
      socket.addEventListener("error", () => undefined);
      return socket;
    };
  }),
).pipe(Layer.provide(NodeSocket.layerWebSocketConstructor));

const unit = Schema.Number.check(Schema.isFinite(), Schema.isBetween({ minimum: 0, maximum: 1 }));
const inputAck = { ack: Schema.optional(Schema.Boolean), record: Schema.optional(Schema.Boolean) };
const keyAction = Schema.optional(Schema.Literals(["down", "up"]));
const decodeAndroidInput = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Union([
      closedDeviceCommand({
        type: Schema.Literal("reset-video"),
        ack: Schema.optional(Schema.Boolean),
      }),
      closedDeviceCommand({ type: Schema.Literal("tap"), x: unit, y: unit, ...inputAck }),
      closedDeviceCommand({
        type: Schema.Literal("swipe"),
        x1: unit,
        y1: unit,
        x2: unit,
        y2: unit,
        durationMs: Schema.optional(
          Schema.Number.check(Schema.isFinite(), Schema.isBetween({ minimum: 0, maximum: 10_000 })),
        ),
        ...inputAck,
      }),
      closedDeviceCommand({
        type: Schema.Literal("touch"),
        action: Schema.Literals(["down", "move", "up"]),
        x: unit,
        y: unit,
        pointerId: Schema.optional(
          Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
        ),
        ...inputAck,
      }),
      closedDeviceCommand({
        type: Schema.Literal("key"),
        keycode: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10_000 })),
        action: keyAction,
        metaState: Schema.optional(
          Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 0x7fffffff })),
        ),
        ...inputAck,
      }),
      closedDeviceCommand({
        type: Schema.Literal("text"),
        text: Schema.String.check(Schema.isMaxLength(16_384)),
        ...inputAck,
      }),
      closedDeviceCommand({
        type: Schema.Literals(["back", "home", "recents", "power"]),
        ...inputAck,
      }),
    ]),
  ),
);
const orientation = Schema.Literals([
  "portrait",
  "portrait_upside_down",
  "landscape_left",
  "landscape_right",
]);
const iosTouch = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    closedDeviceCommand({
      type: Schema.Literals(["begin", "move", "end"]),
      x: unit,
      y: unit,
    }),
  ),
);
const iosButton = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    closedDeviceCommand({
      button: Schema.Literals(["home", "app_switcher", "lock"]),
    }),
  ),
);
const iosKey = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    closedDeviceCommand({
      type: Schema.Literals(["down", "up"]),
      usage: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65_535 })),
    }),
  ),
);
const iosOrientation = Schema.decodeUnknownOption(
  Schema.fromJsonString(closedDeviceCommand({ orientation })),
);
const iosHinge = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    closedDeviceCommand({
      requestId: Schema.Int.check(
        Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
      ),
      command: Schema.Union([
        closedDeviceCommand({
          control: Schema.Literal("angle"),
          value: Schema.Number.check(
            Schema.isFinite(),
            Schema.isBetween({ minimum: 0, maximum: 180 }),
          ),
        }),
        closedDeviceCommand({
          control: Schema.Literal("pose"),
          value: Schema.Literals(["closed", "book", "open", "laptop", "tent"]),
        }),
        closedDeviceCommand({ control: Schema.Literal("table"), value: Schema.Boolean }),
        closedDeviceCommand({
          control: Schema.Literal("physical"),
          value: Schema.Literals(["faceup", "facedown"]),
        }),
        closedDeviceCommand({ control: Schema.Literal("orientation"), value: orientation }),
      ]),
    }),
  ),
);
const inputDecoder = new TextDecoder();
const MAX_SOCKET_BUFFER_BYTES = 8 * 1024 * 1024;
const MAX_INPUT_BYTES = 64 * 1024;
type ViewerInput = {
  readonly resource: DeviceViewerResource;
  readonly operation: "view" | "control";
};
const validViewerInput = (viewer: ViewerInput | undefined, chunk: Uint8Array | string) => {
  if (viewer === undefined) return true;
  const bytes = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.byteLength;
  if (bytes > MAX_INPUT_BYTES) return false;
  if (viewer.resource.platform === "ios") {
    if (viewer.operation !== "control" || typeof chunk === "string" || chunk.length < 2)
      return false;
    const payload = inputDecoder.decode(chunk.subarray(1));
    switch (chunk[0]) {
      case 0x03:
        return Option.isSome(iosTouch(payload));
      case 0x04:
        return Option.isSome(iosButton(payload));
      case 0x06:
        return Option.isSome(iosKey(payload));
      case 0x07:
        return Option.isSome(iosOrientation(payload));
      case 0x10:
        return Option.isSome(iosHinge(payload));
      default:
        return false;
    }
  }
  if (typeof chunk !== "string") return false;
  const decoded = decodeAndroidInput(chunk);
  return (
    Option.isSome(decoded) &&
    (viewer.operation === "control" || decoded.value.type === "reset-video")
  );
};

/**
 * Pipe a client WebSocket to the hub's with no framing changes. Frames are
 * unchanged: H.264 access units one way, validated input packets the other.
 */
const proxyWebSocket = Effect.fn("DeviceHubProxy.proxyWebSocket")(function* (
  request: HttpServerRequest.HttpServerRequest,
  upstreamUrl: string,
  protocols: ReadonlyArray<string> = [],
  headers: Readonly<Record<string, string>> = {},
  recheck: Effect.Effect<void, ViewerAccessError> = Effect.void,
  invalidated: Effect.Effect<void, ViewerAccessError> = Effect.never,
  viewerInput?: ViewerInput,
) {
  yield* recheck;
  const incoming = NodeHttpServerRequest.toIncomingMessage(request);
  delete incoming.headers["sec-websocket-extensions"];
  const transport = incoming.socket;
  const client = yield* request.upgrade;
  const rawUpstream = yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new NodeSocket.NodeWS.WebSocket(upstreamUrl, [...protocols], {
          perMessageDeflate: false,
          maxPayload: MAX_SOCKET_BUFFER_BYTES,
        }),
    ),
    (socket) => Effect.sync(() => socket.close()),
  );
  const upstream = yield* Socket.fromWebSocket(Effect.succeed(rawUpstream), {
    openTimeout: "10 seconds",
    ...(protocols.length > 0 ? { protocols: [...protocols] } : {}),
    ...(Object.keys(headers).length > 0 ? { headers: { ...headers } } : {}),
    highWaterMark: MAX_SOCKET_BUFFER_BYTES,
  }).pipe(Effect.provide(upstreamWebSocketConstructor));
  yield* Effect.scoped(
    Effect.gen(function* () {
      // Open the hub before admitting source input. A synchronous client may
      // send its first frame as soon as its own upgrade completes.
      const upstreamReader = yield* upstream.reader;
      const clientReader = yield* client.reader;
      const writeToClient = yield* client.writer;
      const flushClient = Effect.callback<void, Socket.SocketError>((resume) => {
        transport.write(new Uint8Array(0), (cause) =>
          resume(
            cause
              ? Effect.fail(
                  new Socket.SocketError({ reason: new Socket.SocketWriteError({ cause }) }),
                )
              : Effect.void,
          ),
        );
      });
      // Socket.Writer.write queues synchronously. Wait for the actual Node write
      // completion before pulling another frame; neither hop grows an extra queue.
      const clientSink = {
        write: (chunk: Uint8Array | string) =>
          writeToClient.write(chunk).pipe(Effect.andThen(flushClient)),
      };
      const upstreamSink = {
        write: (chunk: Uint8Array | string) =>
          Effect.callback<void, Socket.SocketError>((resume) => {
            rawUpstream.send(chunk, (cause) =>
              resume(
                cause
                  ? Effect.fail(
                      new Socket.SocketError({ reason: new Socket.SocketWriteError({ cause }) }),
                    )
                  : Effect.void,
              ),
            );
          }),
      };
      // Whichever side closes first ends the other via scope teardown: a close
      // fails the pull with a SocketError, which loses the race.
      return yield* Effect.raceFirst(
        Effect.raceFirst(
          pumpFrames(upstreamReader, clientSink, recheck, () => transport.writableLength),
          invalidated,
        ),
        pumpFrames(
          clientReader,
          upstreamSink,
          recheck,
          () => rawUpstream.bufferedAmount,
          viewerInput,
        ),
      );
    }),
  ).pipe(Effect.ignoreCause);
  return HttpServerResponse.empty();
});

const pumpFrames =
  /* Frame pumping runs continuously; a span per frame stream adds no diagnostic value. */
  Effect.fnUntraced(function* (
    source: Socket.Reader,
    sink: {
      readonly write: (chunk: Uint8Array | string) => Effect.Effect<void, Socket.SocketError>;
    },
    recheck: Effect.Effect<void, ViewerAccessError>,
    bufferedBytes: () => number,
    viewerInput?: ViewerInput,
  ) {
    const { pull } = source;
    while (true) {
      const chunks = yield* pull;
      for (const chunk of chunks) {
        const bytes = typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.byteLength;
        if (bufferedBytes() + bytes + 14 > MAX_SOCKET_BUFFER_BYTES) return yield* Effect.interrupt;
        if (!validViewerInput(viewerInput, chunk)) return yield* Effect.interrupt;
        yield* recheck;
        yield* sink.write(chunk);
      }
    }
  });

const proxyHttp = Effect.fn("DeviceHubProxy.proxyHttp")(function* (
  request: HttpServerRequest.HttpServerRequest,
  upstreamUrl: string,
  extraHeaders: Readonly<Record<string, string>> = {},
  recheck: Effect.Effect<void, ViewerAccessError> = Effect.void,
  invalidated: Effect.Effect<void, ViewerAccessError> = Effect.never,
  viewerInput?: ViewerInput,
) {
  yield* recheck;
  const httpClient = HttpClient.withScope(yield* HttpClient.HttpClient);
  const method = request.method;
  let upstreamRequest = HttpClientRequest.make(method)(upstreamUrl).pipe(
    HttpClientRequest.setHeaders({
      ...forwardHeaders(request, new URL(upstreamUrl).origin),
      ...extraHeaders,
    }),
  );
  if (method !== "GET" && method !== "HEAD") {
    if (viewerInput !== undefined) {
      // The pinned hub caps these JSON controls at 8 KiB. Finish the bounded
      // body wait before checking current authority and sending any mutation.
      const length = request.headers["content-length"];
      if (length !== undefined && Number(length) > 8 * 1024)
        return HttpServerResponse.text("Oversized device control body", { status: 413 });
      const body = yield* request.arrayBuffer.pipe(
        Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(8 * 1024)),
        Effect.result,
      );
      if (body._tag === "Failure")
        return HttpServerResponse.text("Invalid or oversized device control body", { status: 413 });
      yield* recheck;
      upstreamRequest = upstreamRequest.pipe(
        HttpClientRequest.bodyUint8Array(
          new Uint8Array(body.success),
          request.headers["content-type"],
        ),
      );
    } else {
      upstreamRequest = upstreamRequest.pipe(
        HttpClientRequest.bodyStream(request.stream, {
          contentType: request.headers["content-type"],
        }),
      );
    }
  }
  const response = yield* httpClient
    .execute(upstreamRequest)
    .pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }));
  if (response.status >= 300 && response.status < 400)
    return HttpServerResponse.text("Device hub redirect refused", { status: 502 });
  const headers: Record<string, string> = {};
  for (const name of [
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "etag",
    "last-modified",
  ]) {
    const value = response.headers[name];
    if (value !== undefined) headers[name] = value;
  }
  // Long-lived MJPEG and AVCC responses must not be buffered by compression.
  headers["cache-control"] = "no-store, no-transform";
  const body = headers["content-type"]?.startsWith("multipart/x-mixed-replace")
    ? repeatStillFrame(response.stream)
    : response.stream;
  return HttpServerResponse.stream(
    body.pipe(
      Stream.tap(() => recheck),
      Stream.interruptWhen(invalidated),
    ),
    {
      status: response.status,
      headers,
      ...(headers["content-type"] ? { contentType: headers["content-type"] } : {}),
    },
  );
});

/** The path and selector must name the signed resource, never a hub default. */
const matchesViewerResource = (
  path: string,
  params: URLSearchParams,
  resource: DeviceViewerResource,
) => {
  const hostId = params.get("hostId");
  if (hostId !== null && hostId !== resource.hostId) return false;
  if (resource.platform === "ios") {
    const helper = path.match(
      /^\/vendor\/serve-sim\/helper\/([^/]+)\/(?:stream\.(?:avcc|mjpeg)|config|health|ax|foreground|panel\/(?:1|3)\/stream\.avcc)$/,
    );
    if (helper) return helper[1] === encodeURIComponent(resource.deviceId);
    return (
      [
        "/vendor/serve-sim/helper/ws",
        "/vendor/serve-sim/api/screenshot",
        "/vendor/serve-sim/api/event-log",
        "/vendor/serve-sim/api/event-log/events",
        "/vendor/serve-sim/appstate",
      ].includes(path) && params.get("device") === resource.deviceId
    );
  }
  return (
    /^\/vendor\/serve-emu\/(?:ws|api\/(?:screenshot|stream-mode|stream-settings|accessibility|fold))$/.test(
      path,
    ) && params.get("device") === resource.deviceId
  );
};

const handler = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) {
    return HttpServerResponse.text("Bad Request", { status: 400 });
  }
  const hubPath = url.value.pathname.slice(DeviceService.DEVICE_HUB_ROUTE_PREFIX.length) || "/";
  const upgrade = isWebSocketUpgrade(request);
  const allowed = (upgrade ? ALLOWED_WS_PATHS : ALLOWED_PATHS).some((pattern) =>
    pattern.test(hubPath),
  );
  if (!allowed) {
    return HttpServerResponse.text("Not Found", { status: 404 });
  }
  if (!hasAllowedQuery(hubPath, url.value.searchParams)) {
    return HttpServerResponse.text("Bad Request", { status: 400 });
  }
  const readOnly = request.method === "GET" || request.method === "HEAD";
  if (
    (upgrade && request.method !== "GET") ||
    (!upgrade &&
      !readOnly &&
      (hubPath === "/vendor/serve-emu/api/stream-mode"
        ? request.method !== "PUT"
        : hubPath === "/vendor/serve-emu/api/stream-settings"
          ? request.method !== "PATCH"
          : request.method !== "POST" || !MUTABLE_PATHS.some((pattern) => pattern.test(hubPath))))
  ) {
    return HttpServerResponse.text("Method Not Allowed", { status: 405 });
  }
  const controlsDevice =
    (upgrade && hubPath !== "/api/devices/ws") ||
    (!readOnly && /\/api\/(stream-(mode|settings)|fold)$/.test(hubPath));
  const devices = yield* DeviceService.DeviceService;
  const viewerTicket = url.value.searchParams.get("viewerTicket");
  let recheck: Effect.Effect<void, ViewerAccessError> = Effect.void;
  let invalidated: Effect.Effect<void, ViewerAccessError> = Effect.never;
  let ready: Pick<DeviceService.DeviceReadiness, "hostId" | "hub" | "upstream"> | null;
  let peerTicket: string | undefined;
  let viewerInput: ViewerInput | undefined;
  if (viewerTicket !== null) {
    const sessionOwner = yield* Effect.serviceOption(SessionStore.SessionStore);
    const environmentOwner = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
    if (Option.isNone(sessionOwner) || Option.isNone(environmentOwner))
      return HttpServerResponse.text("Resource-bound viewing is unavailable", { status: 503 });
    const sessions = sessionOwner.value;
    const authenticate = sessions
      .admitViewerToken(viewerTicket)
      .pipe(
        Effect.catch(
          (error): Effect.Effect<never, EnvironmentAuthInvalidError | EnvironmentInternalError> =>
            SessionStore.isSessionCredentialInvalidError(error)
              ? failEnvironmentAuthInvalid("invalid_credential")
              : failEnvironmentInternal("internal_error", error),
        ),
      );
    const viewer = yield* authenticate;
    if (
      viewer.resource.type !== "device" ||
      !matchesViewerResource(hubPath, url.value.searchParams, viewer.resource) ||
      (controlsDevice &&
        viewer.operation !== "control" &&
        !(upgrade && viewer.resource.platform === "android"))
    )
      return HttpServerResponse.text("Forbidden", { status: 403 });
    const resource = viewer.resource;
    viewerInput = { resource, operation: viewer.operation };
    const viewerOwner = yield* Effect.serviceOption(DeviceViewers);
    if (Option.isNone(viewerOwner))
      return HttpServerResponse.text("Resource-bound viewing is unavailable", { status: 503 });
    const resolve = viewerOwner.value.resolve(viewer.viewerId, viewerInput, viewer.generation);
    recheck = viewer.recheck.pipe(
      Effect.catch(
        (error): Effect.Effect<never, EnvironmentAuthInvalidError | EnvironmentInternalError> =>
          SessionStore.isSessionCredentialInvalidError(error)
            ? failEnvironmentAuthInvalid("invalid_credential")
            : failEnvironmentInternal("internal_error", error),
      ),
      Effect.andThen(resolve),
      Effect.asVoid,
    );
    const resolved = yield* resolve;
    ready = { hostId: resource.hostId, hub: { origin: resolved.origin } };
    peerTicket = resolved.ticket;
    invalidated = sessions
      .awaitViewerInvalidation(viewer.viewerId)
      .pipe(Effect.catch((error) => failEnvironmentInternal("internal_error", error)));
  } else {
    yield* authenticateMediaRequest(
      controlsDevice ? AuthOrchestrationOperateScope : AuthOrchestrationReadScope,
    );
    ready = yield* devices.currentReadiness(url.value.searchParams.get("hostId") ?? undefined);
  }
  if (!ready) {
    return HttpServerResponse.text("Device hub is not running", { status: 503 });
  }
  // The ticket authenticates here and must not travel on to the hub.
  const upstreamSearch = new URLSearchParams(url.value.search);
  upstreamSearch.delete("wsTicket");
  upstreamSearch.delete("hostId");
  upstreamSearch.delete("viewerTicket");
  if (peerTicket !== undefined) upstreamSearch.set("viewerTicket", peerTicket);
  const search = upstreamSearch.size > 0 ? `?${upstreamSearch.toString()}` : "";
  // Hosts with several upstreams (one per cloud session) route and authenticate requests themselves.
  const routed = ready.upstream ? yield* ready.upstream(hubPath, upstreamSearch) : undefined;
  if (routed === null) {
    return HttpServerResponse.text("Not Found", { status: 404 });
  }
  const upstreamUrl = routed?.url ?? `${ready.hub.origin}${hubPath}${search}`;
  if (upgrade) {
    return yield* proxyWebSocket(
      request,
      upstreamUrl.replace(/^http/, "ws"),
      routed?.protocols,
      routed?.headers,
      recheck,
      invalidated,
      viewerInput,
    );
  }
  return yield* proxyHttp(request, upstreamUrl, routed?.headers, recheck, invalidated, viewerInput);
}).pipe(
  Effect.catchTags({
    OrchestratorMcpFailure: () =>
      Effect.succeed(
        HttpServerResponse.text("Current device viewer authority refused", { status: 403 }),
      ),
    DeviceHostUnavailableError: () =>
      Effect.succeed(HttpServerResponse.text("Device opening is unavailable", { status: 409 })),
    DeviceOperationError: () =>
      Effect.succeed(HttpServerResponse.text("Device service is unavailable", { status: 503 })),
  }),
);

export const layer = HttpRouter.use((router) =>
  Effect.gen(function* () {
    // Optional lookup happens at registration, where the native owners are composed.
    // Minimal legacy servers retain their local proxy and refuse resource viewers explicitly.
    const devices = yield* DeviceService.DeviceService;
    const client = yield* Effect.serviceOption(HttpClient.HttpClient);
    const sessions = yield* Effect.serviceOption(SessionStore.SessionStore);
    const environment = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
    const viewers = yield* Effect.serviceOption(DeviceViewers);
    let serve = handler.pipe(Effect.provideService(DeviceService.DeviceService, devices));
    if (Option.isSome(client))
      serve = serve.pipe(Effect.provideService(HttpClient.HttpClient, client.value));
    if (Option.isSome(sessions))
      serve = serve.pipe(Effect.provideService(SessionStore.SessionStore, sessions.value));
    if (Option.isSome(environment))
      serve = serve.pipe(
        Effect.provideService(ServerEnvironment.ServerEnvironment, environment.value),
      );
    if (Option.isSome(viewers))
      serve = serve.pipe(Effect.provideService(DeviceViewers, viewers.value));
    yield* router.add("*", `${DeviceService.DEVICE_HUB_ROUTE_PREFIX}/*`, serve);
  }),
);
