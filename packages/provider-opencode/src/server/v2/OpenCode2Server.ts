import { resolveProviderProbeTimeoutMs } from "@t3tools/provider-core/server/snapshotProbe";
import type { OpenCodeClient } from "@opencode/client/effect";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Base64Url from "effect/encoding/Base64Url";
import * as P from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as HttpClientError from "effect/http/HttpClientError";

import * as OpenCodeRuntime from "../OpenCodeRuntime.ts";
import * as OpenCodeServerOwner from "../OpenCodeServerOwner.ts";
import * as OpenCode2Client from "./OpenCode2Client.ts";

const INFO_TIMEOUT_MS = 5_000;

export interface OpenCode2Connection extends OpenCode2Client.OpenCode2Api {
  readonly url: string;
  readonly version: string;
  readonly external: boolean;
  /** Only owned processes have an authoritative lifetime signal. */
  readonly processExit?: Effect.Effect<void>;
}

export interface OpenCode2SessionServer {
  readonly withConnection: <A, E, R>(
    use: (connection: OpenCode2Connection) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | OpenCodeRuntime.OpenCodeRuntimeError, R>;
}

export class OpenCode2Server extends Context.Service<
  OpenCode2Server,
  OpenCode2SessionServer & {
    /** Drops a configured server's verified connection, so the next use reads its version again. */
    readonly forget: Effect.Effect<void>;
    /**
     * A private process for one provider session, stopped when the session's scope or the
     * instance closes. Identical context across threads then hits the model's prefix cache.
     * Absent when sessions share one server: a configured `serverUrl`, or bare's own server.
     */
    readonly forSession?: (
      threadId: string,
    ) => Effect.Effect<OpenCode2SessionServer, never, Scope.Scope>;
  }
>()("@t3tools/provider-opencode/server/v2/OpenCode2Server") {}

/**
 * OpenCode 2 always requires one and prints a generated one to stdout
 * otherwise, so T3 supplies its own and keeps it in memory.
 */
const generatePassword = Effect.gen(function* () {
  const random = yield* SecureRandom.SecureRandom;
  const bytes = yield* random.randomBytes(32);
  return Redacted.make(Base64Url.encode(bytes), { label: "OPENCODE_PASSWORD" });
});

/** OPENCODE_PASSWORD overrides OPENCODE_SERVER_PASSWORD; drop the inherited 1.x variable. */
export const serverEnvironment = (
  environment: NodeJS.ProcessEnv,
  password: Redacted.Redacted,
): NodeJS.ProcessEnv => {
  const { OPENCODE_SERVER_PASSWORD: _inherited, ...rest } = environment;
  return { ...rest, OPENCODE_PASSWORD: Redacted.value(password) };
};

export const httpFailureOf = (cause: unknown): HttpClientError.HttpClientError | undefined => {
  if (HttpClientError.isHttpClientError(cause)) return cause;
  if (
    P.isTagged(cause, "ClientError") &&
    P.hasProperty(cause, "cause") &&
    HttpClientError.isHttpClientError(cause.cause)
  ) {
    return cause.cause;
  }
  return undefined;
};

// Empty 401s still reject passwords; undecodable 2xx may be 1.x HTML, not OpenCode 2.
const describeInfoFailure = (cause: unknown) => {
  const failure = httpFailureOf(cause);
  if (failure?.reason._tag === "TransportError") {
    return "Could not reach the OpenCode server.";
  }
  const status = failure?.response?.status;
  if (status === 401) return "The OpenCode server rejected the server password.";
  if (status !== undefined && (status < 200 || status >= 300)) {
    return `The OpenCode server returned HTTP ${status}.`;
  }
  return "The server is not an OpenCode 2 server.";
};

/** Only server info proves readiness; keep credential-bearing failure details out of client text. */
export const verifyServer = (client: OpenCodeClient, environment?: NodeJS.ProcessEnv) =>
  client.server.info().pipe(
    Effect.timeoutOrElse({
      duration: resolveProviderProbeTimeoutMs("opencode", INFO_TIMEOUT_MS, environment),
      orElse: () =>
        Effect.fail(
          new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: "Timed out waiting for the OpenCode server.",
          }),
        ),
    }),
    Effect.catchTags({
      UnauthorizedError: (cause) =>
        Effect.fail(
          new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: "The OpenCode server rejected the server password.",
            cause,
          }),
        ),
    }),
    Effect.mapError((cause) =>
      OpenCodeRuntime.isOpenCodeRuntimeError(cause)
        ? cause
        : new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "server.info",
            detail: describeInfoFailure(cause),
            cause,
          }),
    ),
    Effect.map((info) => info.version),
  );

export const make = Effect.fn("OpenCode2Server.make")(function* (input: {
  readonly binaryPath: string;
  readonly serverUrl: string;
  readonly serverPassword: string;
  readonly directory: string;
  readonly environment: NodeJS.ProcessEnv;
}) {
  const opencode = yield* OpenCode2Client.OpenCode2Client;
  const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
  const random = yield* SecureRandom.SecureRandom;
  const instanceScope = yield* Effect.scope;
  const connectTo = Effect.fn("OpenCode2Server.connectTo")(function* (
    url: string,
    password: Redacted.Redacted,
    external: boolean,
  ) {
    const api = yield* opencode.connect({ baseUrl: url, password });
    const version = yield* verifyServer(api.client, input.environment);
    return { ...api, url, version, external } satisfies OpenCode2Connection;
  });

  const serverUrl = input.serverUrl.trim();
  if (serverUrl.length > 0) {
    let latest: OpenCode2Connection | undefined;
    const connect = connectTo(serverUrl, Redacted.make(input.serverPassword), true).pipe(
      Effect.tap((connection) =>
        Effect.sync(() => {
          latest = connection;
        }),
      ),
    );
    return OpenCode2Server.of({
      withConnection: Effect.fn("OpenCode2Server.withConnection")(function (use) {
        return Effect.suspend(() => (latest === undefined ? connect : Effect.succeed(latest))).pipe(
          Effect.flatMap(use),
        );
      }),
      forget: Effect.sync(() => {
        latest = undefined;
      }),
    });
  }

  const owned = Effect.fn("OpenCode2Server.owned")(function* (threadId?: string) {
    const password = yield* generatePassword;
    let latest: OpenCode2Connection | undefined;
    const owner = yield* OpenCodeServerOwner.make({
      binaryPath: input.binaryPath,
      directory: input.directory,
      environment: serverEnvironment(input.environment, password),
      verify: (url) =>
        connectTo(url, password, false).pipe(
          Effect.map((connection) => {
            latest = connection;
            return connection.version;
          }),
        ),
      ...(threadId === undefined ? {} : { threadId }),
    });
    return {
      withConnection: Effect.fn("OpenCode2Server.withConnection")(function (use) {
        return owner.withServer((server) =>
          latest?.url === server.url
            ? use({ ...latest, processExit: server.exitCode.pipe(Effect.asVoid) })
            : Effect.die(new Error("OpenCode 2 server was lent before verification.")),
        );
      }),
    } satisfies OpenCode2SessionServer;
  });
  const shared = yield* owned();
  return OpenCode2Server.of({
    ...shared,
    forget: Effect.void,
    forSession: Effect.fn("OpenCode2Server.forSession")(function* (threadId) {
      // Forked from the instance so disabling or replacing it stops sessions still open.
      const scope = yield* Scope.fork(instanceScope);
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      return yield* owned(threadId).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime),
        Effect.provideService(SecureRandom.SecureRandom, random),
      );
    }),
  });
});
