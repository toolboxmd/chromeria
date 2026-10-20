import { threadRequestNamespace } from "./requestIdentity.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Option from "effect/Option";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";

export interface McpCredentialRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  /** False omits the preview capability wherever the token is honored. Defaults to full access. */
  readonly browserToolsAvailable?: boolean;
  readonly authority?: PeerAuthority.Authority;
  readonly capabilities?: ReadonlySet<McpInvocationContext.McpCapability>;
  readonly subagent?: boolean;
}

export interface McpIssuedCredential {
  readonly config: McpProviderSession.McpProviderSessionConfig;
}

export interface McpSessionRegistryShape {
  readonly issue: (request: McpCredentialRequest) => Effect.Effect<McpIssuedCredential>;
  readonly resolve: (
    rawToken: string,
  ) => Effect.Effect<McpInvocationContext.McpThreadInvocationScope | undefined>;
  /** The newest unexpired provider scope, without exposing its credential. */
  readonly scopeForThread: (
    threadId: ThreadId,
  ) => Effect.Effect<McpInvocationContext.McpThreadInvocationScope | undefined>;
  /**
   * Records a sign of life for every credential bound to `threadId`. Provider
   * turns call this so that a session which is plainly alive keeps its
   * credential even when it goes a long time without touching an MCP tool.
   */
  readonly touch: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly revokeThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeAll: Effect.Effect<void>;
}

export class McpSessionRegistry extends Context.Service<
  McpSessionRegistry,
  McpSessionRegistryShape
>()("t3/mcp/McpSessionRegistry") {}

interface CredentialRecord {
  readonly tokenHash: string;
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly lastAliveAt: number;
}

interface RegistryState {
  readonly records: ReadonlyMap<string, CredentialRecord>;
}

export interface McpSessionRegistryOptions {
  readonly livenessWindowMs?: number;
  readonly now?: () => number;
}

/**
 * `/mcp` is outside environment auth, so idle tokens stop authenticating.
 * Keep expired records until explicit revocation: a trusted provider turn can
 * refresh the token its subprocess still holds; MCP requests cannot.
 */
const DEFAULT_LIVENESS_WINDOW_MS = 24 * 60 * 60 * 1_000;

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const tokenFromBytes = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64url");

const getHttpMcpEndpointHost = (address: NetAddress.IpAddress): string =>
  NetAddress.isUnspecified(address)
    ? "127.0.0.1"
    : NetAddress.formatUrlHostString(NetAddress.formatIp(address));

const makeWithOptions = Effect.fn("McpSessionRegistry.make")(function* (
  options: McpSessionRegistryOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const httpServer = yield* HttpServer.HttpServer;
  const state = yield* SynchronizedRef.make<RegistryState>({ records: new Map() });
  const currentTimeMillis = options.now ? Effect.sync(options.now) : Clock.currentTimeMillis;
  const livenessWindowMs = options.livenessWindowMs ?? DEFAULT_LIVENESS_WINDOW_MS;
  const endpoint = NetAddress.isInetAddress(httpServer.address)
    ? `http://${getHttpMcpEndpointHost(httpServer.address.address)}:${httpServer.address.port}/mcp`
    : "http://127.0.0.1/mcp";

  const hashToken = (token: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(token))
      .pipe(Effect.map(bytesToHex), Effect.orDie);

  const currentScope = (
    scope: McpInvocationContext.McpThreadInvocationScope,
    fallback?: PeerAuthority.Authority,
  ) =>
    Effect.gen(function* () {
      const frozen = Option.isSome(authority)
        ? yield* authority.value.forThread(scope.thread.threadId).pipe(Effect.orDie)
        : (fallback ?? scope.authority);
      const legacy =
        frozen === undefined && Option.isSome(authority)
          ? yield* authority.value.legacyCeiling(scope.thread.threadId).pipe(Effect.orDie)
          : undefined;
      const ceiling = frozen?.ceiling ?? legacy;
      const { authority: _oldAuthority, legacyPeerCeiling: _oldLegacy, ...credential } = scope;
      return {
        ...credential,
        ...(frozen === undefined ? {} : { authority: frozen }),
        ...(legacy === undefined ? {} : { legacyPeerCeiling: legacy }),
        capabilities:
          ceiling === undefined
            ? scope.capabilities
            : new Set(
                [...scope.capabilities].filter((capability) =>
                  ceiling.capabilities.includes(capability),
                ),
              ),
      };
    });

  const issue: McpSessionRegistryShape["issue"] = Effect.fn("McpSessionRegistry.issue")(
    function* (request) {
      const issuedAt = yield* currentTimeMillis;
      const providerSessionId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
      const tokenHash = yield* hashToken(rawToken);
      const browserToolsAvailable = request.browserToolsAvailable ?? true;
      const scope: McpInvocationContext.McpThreadInvocationScope = {
        environmentId,
        requestNamespace: threadRequestNamespace(environmentId, request.threadId),
        thread: {
          threadId: ThreadId.make(request.threadId),
          providerSessionId,
          providerInstanceId: ProviderInstanceId.make(request.providerInstanceId),
          subagent: request.subagent === true,
        },
        client: undefined,
        capabilities: new Set<McpInvocationContext.McpCapability>([
          "orchestration",
          "worktree",
          "pull-requests",
          ...(request.capabilities ?? (browserToolsAvailable ? (["preview"] as const) : [])),
        ]),
        issuedAt,
      };
      const bounded = yield* currentScope(scope, request.authority);
      yield* SynchronizedRef.update(state, ({ records }) => {
        const next = new Map(records);
        next.set(tokenHash, { tokenHash, scope: bounded, lastAliveAt: issuedAt });
        return { records: next };
      });
      return {
        config: {
          environmentId,
          threadId: scope.thread.threadId,
          providerSessionId,
          providerInstanceId: scope.thread.providerInstanceId,
          endpoint,
          authorizationHeader: `Bearer ${rawToken}`,
          browserToolsAvailable: bounded.capabilities.has("preview"),
          capabilities: bounded.capabilities,
        },
      };
    },
  );

  const resolve: McpSessionRegistryShape["resolve"] = Effect.fn("McpSessionRegistry.resolve")(
    function* (rawToken) {
      if (rawToken.length === 0) return undefined;
      const tokenHash = yield* hashToken(rawToken);
      const timestamp = yield* currentTimeMillis;
      const scope = yield* SynchronizedRef.modify(state, ({ records }) => {
        const record = records.get(tokenHash);
        if (!record || timestamp - record.lastAliveAt > livenessWindowMs) {
          return [undefined, { records }] as const;
        }
        const next = new Map(records);
        next.set(tokenHash, { ...record, lastAliveAt: timestamp });
        return [record.scope, { records: next }] as const;
      });
      return scope === undefined ? undefined : yield* currentScope(scope);
    },
  );

  const touch: McpSessionRegistryShape["touch"] = Effect.fn("McpSessionRegistry.touch")(
    function* (threadId) {
      const timestamp = yield* currentTimeMillis;
      yield* SynchronizedRef.update(state, ({ records }) => {
        const next = new Map(records);
        for (const [tokenHash, record] of records) {
          if (record.scope.thread.threadId === threadId) {
            next.set(tokenHash, { ...record, lastAliveAt: timestamp });
          }
        }
        return { records: next };
      });
    },
  );

  const revokeWhere = (predicate: (record: CredentialRecord) => boolean) =>
    SynchronizedRef.update(state, ({ records }) => ({
      records: new Map(Array.from(records).filter(([, record]) => !predicate(record))),
    }));

  return McpSessionRegistry.of({
    issue,
    resolve,
    scopeForThread: (threadId) =>
      Effect.gen(function* () {
        const timestamp = yield* currentTimeMillis;
        const { records } = yield* SynchronizedRef.get(state);
        const scope = Array.from(records.values()).findLast(
          (record) =>
            record.scope.thread.threadId === threadId &&
            timestamp - record.lastAliveAt <= livenessWindowMs,
        )?.scope;
        return scope === undefined ? undefined : yield* currentScope(scope);
      }),
    touch,
    revokeProviderSession: Effect.fn("McpSessionRegistry.revokeProviderSession")(
      function* (providerSessionId) {
        yield* revokeWhere((record) => record.scope.thread.providerSessionId === providerSessionId);
      },
    ),
    revokeThread: Effect.fn("McpSessionRegistry.revokeThread")(function* (threadId) {
      yield* revokeWhere((record) => record.scope.thread.threadId === threadId);
    }),
    revokeAll: SynchronizedRef.set(state, { records: new Map() }),
  });
});

let activeMcpSessionRegistry: McpSessionRegistryShape | undefined;

const make = Effect.acquireRelease(
  makeWithOptions().pipe(
    Effect.tap((registry) =>
      Effect.sync(() => {
        activeMcpSessionRegistry = registry;
      }),
    ),
  ),
  (registry) =>
    Effect.sync(() => {
      if (activeMcpSessionRegistry === registry) {
        activeMcpSessionRegistry = undefined;
      }
    }),
);

export const layer = Layer.effect(McpSessionRegistry, make);

export const touchActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.touch(threadId) : Effect.void;

export const __testing = {
  make: makeWithOptions,
};
