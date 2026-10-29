import {
  ORCHESTRATION_PROTOCOL_VERSION,
  type AuthRenewalLocalRecord,
  EnvironmentAuthInvalidError,
  EnvironmentInternalError,
  EnvironmentId,
  type ExecutionEnvironmentDescriptor,
  type AuthBrowserSessionResult,
  type AuthCreatePairingCredentialInput,
  type AuthSessionState,
  type DesktopBridge,
} from "@t3tools/contracts";
import { SessionRenewal } from "@t3tools/client-runtime/authorization";
import { createBrowserHistory } from "@tanstack/react-router";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import { ConnectionTransientError, RenewalRecordStore } from "@t3tools/client-runtime/connection";
import { layerRemoteHttpClient } from "@t3tools/client-runtime/rpc";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { installEnvironmentHttpTest } from "../test/environmentHttpTest";
import {
  __setPrimaryHttpRunnerForTests,
  __setRenewalRunnerForTests,
  runRenewal,
} from "./lib/runtime";

type TestWindow = {
  location: URL;
  history: {
    state: unknown;
    replaceState: (_data: unknown, _unused: string, url?: string) => void;
  };
  desktopBridge?: DesktopBridge;
};

const LOOPBACK_AUTH = {
  policy: "loopback-browser",
  bootstrapMethods: ["one-time-token"],
  sessionMethods: ["browser-session-cookie"],
  sessionCookieName: "t3_session",
} as const;

const DESKTOP_AUTH = {
  policy: "desktop-managed-local",
  bootstrapMethods: ["desktop-bootstrap"],
  sessionMethods: ["browser-session-cookie"],
  sessionCookieName: "t3_session",
} as const;

const SESSION_EXPIRES_AT = DateTime.makeUnsafe("2026-04-05T00:00:00.000Z");
const TEST_ENVIRONMENT: ExecutionEnvironmentDescriptor = {
  environmentId: EnvironmentId.make("environment-test"),
  label: "Test environment",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  capabilities: { repositoryIdentity: true },
};
const unauthenticatedSession = (auth: AuthSessionState["auth"]): AuthSessionState => ({
  authenticated: false,
  auth,
});

const authenticatedSession = (auth: AuthSessionState["auth"]): AuthSessionState => ({
  authenticated: true,
  auth,
  sessionMethod: "browser-session-cookie",
  expiresAt: SESSION_EXPIRES_AT,
});

const browserSession = (scopes: AuthBrowserSessionResult["scopes"]): AuthBrowserSessionResult => ({
  authenticated: true,
  scopes,
  sessionMethod: "browser-session-cookie",
  expiresAt: SESSION_EXPIRES_AT,
});

function installTestBrowser(url: string) {
  const testWindow: TestWindow = {
    location: new URL(url),
    history: {
      state: null,
      replaceState: (data, _unused, nextUrl) => {
        testWindow.history.state = data;
        if (nextUrl !== undefined) {
          testWindow.location = new URL(nextUrl, testWindow.location.href);
        }
      },
    },
  };

  vi.stubGlobal("window", testWindow);
  vi.stubGlobal("document", { title: "T3 Code" });

  return testWindow;
}

function sequence<A>(...values: ReadonlyArray<A>) {
  let index = 0;
  return () => values[Math.min(index++, values.length - 1)]!;
}

function createSignal() {
  let resolve = () => {};
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

let disposeHttpTest: (() => Promise<void>) | undefined;

async function installAuthApi(input: {
  readonly renewalLayer?: Layer.Layer<SessionRenewal.SessionRenewal>;
  readonly descriptor?: () => Effect.Effect<ExecutionEnvironmentDescriptor>;
  readonly renewal?: SessionRenewal.SessionRenewal["Service"]["ensure"];
  readonly session?: () => AuthSessionState;
  readonly browserSession?: (
    credential: string,
  ) => Effect.Effect<AuthBrowserSessionResult, EnvironmentAuthInvalidError>;
  readonly pairingCredential?: (payload: AuthCreatePairingCredentialInput) => Effect.Effect<{
    readonly id: string;
    readonly credential: string;
    readonly label?: string;
    readonly expiresAt: DateTime.Utc;
  }>;
}) {
  const testApi = await installEnvironmentHttpTest({
    descriptor: input.descriptor ?? (() => Effect.succeed(TEST_ENVIRONMENT)),
    ...(input.renewalLayer ? { renewalLayer: input.renewalLayer } : {}),
    ...(input.renewal ? { renewal: input.renewal } : {}),
    ...(input.session ? { session: () => Effect.succeed(input.session!()) } : {}),
    ...(input.browserSession
      ? { browserSession: (payload) => input.browserSession!(payload.credential) }
      : {}),
    ...(input.pairingCredential
      ? { pairingCredential: (payload) => input.pairingCredential!(payload) }
      : {}),
  });
  disposeHttpTest = testApi.dispose;
  return testApi;
}

describe("resolveInitialServerAuthGateState", () => {
  it("does not consume a new browser pairing code when its descriptor is unavailable", async () => {
    let unavailable = true;
    const testApi = await installAuthApi({
      descriptor: () =>
        unavailable
          ? Effect.die(
              new EnvironmentInternalError({
                code: "internal_error",
                reason: "internal_error",
                traceId: "descriptor-failure",
              }),
            )
          : Effect.succeed(TEST_ENVIRONMENT),
      browserSession: () => Effect.succeed(browserSession(["orchestration:read"])),
      session: () => authenticatedSession(LOOPBACK_AUTH),
    });
    const { submitServerAuthCredential } = await import("./environments/primary/auth");
    await expect(submitServerAuthCredential("valid-new-code")).rejects.toBeDefined();
    expect(testApi.calls.browserSession).toEqual([]);
    unavailable = false;
    await expect(submitServerAuthCredential("valid-new-code")).resolves.toBeUndefined();
    expect(testApi.calls.browserSession).toMatchObject([
      {
        credential: "valid-new-code",
        renewal_commitment: "66d34fba71f8f450f7e45598853e53bfc23bbd129027cbb131a2f4ffd7878cd0",
      },
    ]);
  });
  it("recovers the primary cookie before presenting a pairing gate", async () => {
    installTestBrowser("http://localhost/");
    const keys: string[] = [];
    const testApi = await installAuthApi({
      session: sequence(unauthenticatedSession(LOOPBACK_AUTH), authenticatedSession(LOOPBACK_AUTH)),
      renewal: (key) => {
        keys.push(key);
        return Effect.succeed({
          _tag: "Renewed",
          result: {
            outcome: "current",
            generation: 0,
            expiresAt: 1000,
            renewAfter: 500,
          },
        });
      },
    });
    const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
    expect(await resolveInitialServerAuthGateState()).toEqual({ status: "authenticated" });
    expect(keys).toEqual(["cookie:http://localhost"]);
    expect(testApi.calls.session).toBe(2);
  });
  it.each(["network", "HTTP 500", "storage"])(
    "retains renewal authority and recovers startup after a transient %s failure",
    async (failure) => {
      let unavailable = false;
      let authenticated = false;
      let record: AuthRenewalLocalRecord | null = null;
      const readRecord = () => record;
      const storage = Layer.succeed(RenewalRecordStore.RenewalRecordStore, {
        read: () =>
          unavailable && failure === "storage"
            ? Effect.fail(
                new ConnectionTransientError({ reason: "transport", detail: "Store unavailable" }),
              )
            : Effect.sync(() => record),
        compareAndSet: (_key, revision, next) =>
          Effect.sync(() => {
            if ((record?.revision ?? null) !== revision) return false;
            record = next;
            return true;
          }),
      });
      const http = layerRemoteHttpClient((input) => {
        if (String(input).endsWith("/.well-known/t3/environment")) {
          return Promise.resolve(
            Response.json({
              ...TEST_ENVIRONMENT,
              orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
            }),
          );
        }
        if (unavailable) {
          return failure === "network"
            ? Promise.reject(new TypeError("Network unavailable"))
            : Promise.resolve(new Response("Unavailable", { status: 500 }));
        }
        authenticated = true;
        return Promise.resolve(
          Response.json({ outcome: "current", generation: 0, expiresAt: 1000, renewAfter: 500 }),
        );
      });
      const crypto = Layer.succeed(
        Crypto.Crypto,
        Crypto.make({
          randomBytes: (size) => new Uint8Array(size).fill(1),
          digest: (_algorithm, data) => Effect.succeed(data),
        }),
      );
      const testApi = await installAuthApi({
        renewalLayer: SessionRenewal.layer.pipe(
          Layer.provide(Layer.mergeAll(storage, http, crypto)),
        ),
        session: () =>
          authenticated
            ? authenticatedSession(LOOPBACK_AUTH)
            : unauthenticatedSession(LOOPBACK_AUTH),
      });
      const slot = await runRenewal(
        SessionRenewal.SessionRenewal.pipe(
          Effect.flatMap((renewal) =>
            renewal.stage({
              key: "cookie:http://localhost",
              environmentId: TEST_ENVIRONMENT.environmentId,
              origin: "http://localhost",
              kind: "cookie",
            }),
          ),
        ),
      );
      await runRenewal(
        SessionRenewal.SessionRenewal.pipe(
          Effect.flatMap((renewal) =>
            renewal.complete("cookie:http://localhost", slot.epoch, { expiresAt: 1 }),
          ),
        ),
      );
      unavailable = true;
      const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
      await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
        status: "retryable",
        retryAfterMs: 500,
      });
      expect(readRecord()?.active?.current).toBe(slot.current);
      unavailable = false;
      await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
        status: "authenticated",
      });
      expect(testApi.calls.browserSession).toEqual([]);
    },
  );

  it("requires pairing when the durable renewal secret is rejected", async () => {
    await installAuthApi({
      session: () => unauthenticatedSession(LOOPBACK_AUTH),
      renewal: () => Effect.fail(new SessionRenewal.RenewalRejected({})),
    });
    const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
    });
  });

  it("shares a transient startup renewal attempt across concurrent callers", async () => {
    let attempts = 0;
    await installAuthApi({
      session: () => unauthenticatedSession(LOOPBACK_AUTH),
      renewal: () => {
        attempts += 1;
        return Effect.fail(
          new ConnectionTransientError({ reason: "transport", detail: "Unavailable" }),
        );
      },
    });
    const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
    const results = await Promise.all([
      resolveInitialServerAuthGateState(),
      resolveInitialServerAuthGateState(),
    ]);
    expect(results).toEqual([
      { status: "retryable", retryAfterMs: 500 },
      { status: "retryable", retryAfterMs: 500 },
    ]);
    expect(attempts).toBe(1);
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "retryable",
      retryAfterMs: 1000,
    });
  });

  it("preserves explicit pairing intent while startup renewal is retryable", async () => {
    let attempts = 0;
    await installAuthApi({
      session: () => unauthenticatedSession(LOOPBACK_AUTH),
      renewal: () => {
        attempts += 1;
        return Effect.fail(
          new ConnectionTransientError({ reason: "transport", detail: "Unavailable" }),
        );
      },
    });
    const testWindow = installTestBrowser("http://localhost/");
    const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "retryable",
      retryAfterMs: 500,
    });
    testWindow.location = new URL("http://localhost/pair#token=replacement");
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
    });
    expect(testWindow.location.hash).toBe("#token=replacement");
    expect(attempts).toBe(1);
  });

  it("caps startup retry backoff and stops retrying on definitive rejection", async () => {
    let rejected = false;
    await installAuthApi({
      session: () => unauthenticatedSession(LOOPBACK_AUTH),
      renewal: () =>
        rejected
          ? Effect.fail(new SessionRenewal.RenewalRejected({}))
          : Effect.fail(
              new ConnectionTransientError({ reason: "transport", detail: "Unavailable" }),
            ),
    });
    const { resolveInitialServerAuthGateState } = await import("./environments/primary/auth");
    for (const retryAfterMs of [500, 1000, 2000, 4000, 5000, 5000]) {
      await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
        status: "retryable",
        retryAfterMs,
      });
    }
    rejected = true;
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
    });
    rejected = false;
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "retryable",
      retryAfterMs: 500,
    });
  });

  beforeEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    installTestBrowser("http://localhost/");
  });

  afterEach(async () => {
    __setRenewalRunnerForTests();
    await disposeHttpTest?.();
    disposeHttpTest = undefined;
    const { __resetServerAuthBootstrapForTests } = await import("./environments/primary");
    __resetServerAuthBootstrapForTests();
    __setPrimaryHttpRunnerForTests();
    vi.unstubAllEnvs();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("reuses an in-flight silent bootstrap attempt", async () => {
    const nextSession = sequence(
      unauthenticatedSession(DESKTOP_AUTH),
      authenticatedSession(DESKTOP_AUTH),
    );
    const testApi = await installAuthApi({
      session: nextSession,
      browserSession: () => Effect.succeed(browserSession(["orchestration:read", "access:write"])),
    });

    const testWindow = installTestBrowser("http://localhost/");
    testWindow.desktopBridge = {
      getLocalEnvironmentEnabled: () => true,
      getLocalEnvironmentBootstraps: () => [
        {
          id: "primary",
          label: "Windows",
          httpBaseUrl: "http://localhost:3773",
          wsBaseUrl: "ws://localhost:3773",
          bootstrapToken: "desktop-bootstrap-token",
        },
      ],
    } as unknown as DesktopBridge;

    const { resolveInitialServerAuthGateState } = await import("./environments/primary");

    await expect(
      Promise.all([resolveInitialServerAuthGateState(), resolveInitialServerAuthGateState()]),
    ).resolves.toEqual([{ status: "authenticated" }, { status: "authenticated" }]);

    expect(testApi.calls.session).toBe(2);
    expect(testApi.calls.browserSession).toMatchObject([{ credential: "desktop-bootstrap-token" }]);
  });

  it("takes a pairing token from the location hash and strips it immediately", async () => {
    const testWindow = installTestBrowser("http://localhost/#token=pairing-token");
    const { takePairingTokenFromUrl } = await import("./environments/primary");

    expect(takePairingTokenFromUrl()).toBe("pairing-token");
    expect(testWindow.location.hash).toBe("");
    expect(testWindow.location.searchParams.get("token")).toBeNull();
  });

  it.each([
    { suffix: "#token=replacement-token", cached: false },
    { suffix: "#token=replacement-token", cached: true },
    { suffix: "?token=replacement-token", cached: true },
    { suffix: "/#token=replacement-token", cached: true },
  ])(
    "re-pairs an authenticated browser with $suffix when cached=$cached",
    async ({ suffix, cached }) => {
      let scopes: AuthBrowserSessionResult["scopes"] = ["orchestration:read"];
      const testApi = await installAuthApi({
        session: () => ({ ...authenticatedSession(LOOPBACK_AUTH), scopes }),
        browserSession: () =>
          Effect.sync(() => {
            scopes = ["orchestration:read", "orchestration:operate"];
            return browserSession(scopes);
          }),
      });
      const testWindow = installTestBrowser("http://localhost/");
      const {
        resolveInitialServerAuthGateState,
        submitServerAuthCredential,
        takePairingTokenFromUrl,
      } = await import("./environments/primary");
      const { fetchSessionState } = await import("./environments/primary/auth");

      if (cached) {
        await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
          status: "authenticated",
        });
      }

      testWindow.location = new URL(`http://localhost/pair${suffix}`);
      await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
        status: "requires-auth",
        auth: LOOPBACK_AUTH,
      });
      expect(testApi.calls.browserSession).toEqual([]);

      const token = takePairingTokenFromUrl();
      expect(token).toBe("replacement-token");
      await submitServerAuthCredential(token!);

      expect(testApi.calls.browserSession).toMatchObject([{ credential: "replacement-token" }]);
      await expect(fetchSessionState()).resolves.toMatchObject({
        authenticated: true,
        scopes: ["orchestration:read", "orchestration:operate"],
      });
      await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
        status: "authenticated",
      });
    },
  );

  it.each([false, true])(
    "keeps rejected replacement pairing open when cached=%s",
    async (cached) => {
      const existingSession = {
        ...authenticatedSession(LOOPBACK_AUTH),
        scopes: ["orchestration:read"] as const,
      };
      const exchangeStarted = createSignal();
      const finishExchange = createSignal();
      const testApi = await installAuthApi({
        session: () => existingSession,
        browserSession: () =>
          Effect.gen(function* () {
            exchangeStarted.resolve();
            yield* Effect.promise(() => finishExchange.promise);
            return yield* new EnvironmentAuthInvalidError({
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "trace-invalid-replacement",
            });
          }),
      });
      const testWindow = installTestBrowser("http://localhost/");
      const {
        resolveInitialServerAuthGateState,
        submitServerAuthCredential,
        takePairingTokenFromUrl,
      } = await import("./environments/primary");
      const { fetchSessionState } = await import("./environments/primary/auth");
      if (cached) {
        await resolveInitialServerAuthGateState();
      }
      testWindow.location = new URL("http://localhost/pair#token=invalid-replacement");
      const history = createBrowserHistory({
        window: {
          get location() {
            return testWindow.location;
          },
          history: testWindow.history,
          addEventListener() {},
          removeEventListener() {},
        },
      });
      const gateLoads: Array<ReturnType<typeof resolveInitialServerAuthGateState>> = [];
      const unsubscribe = history.subscribe(() => {
        gateLoads.push(resolveInitialServerAuthGateState());
      });
      try {
        const requiresAuth = { status: "requires-auth", auth: LOOPBACK_AUTH };
        await expect(resolveInitialServerAuthGateState()).resolves.toEqual(requiresAuth);

        // Token removal synchronously reloads the TanStack auth gate via replaceState.
        const token = takePairingTokenFromUrl();
        expect(gateLoads).toHaveLength(1);
        await expect(gateLoads[0]).resolves.toEqual(requiresAuth);
        const rejected = expect(submitServerAuthCredential(token!)).rejects.toMatchObject({
          _tag: "PrimaryEnvironmentPairingCredentialRejectedError",
          message: "Invalid pairing token. Check the token and try again.",
        });
        await exchangeStarted.promise;
        await expect(resolveInitialServerAuthGateState()).resolves.toEqual(requiresAuth);
        finishExchange.resolve();
        await rejected;
        await expect(resolveInitialServerAuthGateState()).resolves.toEqual(requiresAuth);
        expect(testApi.calls.browserSession).toMatchObject([{ credential: "invalid-replacement" }]);
        await expect(fetchSessionState()).resolves.toEqual(existingSession);

        testWindow.history.replaceState({}, "", "/");
        await expect(gateLoads[1]).resolves.toEqual({ status: "authenticated" });
        testWindow.history.replaceState({}, "", "/pair");
        await expect(gateLoads[2]).resolves.toEqual({ status: "authenticated" });
      } finally {
        finishExchange.resolve();
        unsubscribe();
        history.destroy();
      }
    },
  );

  it("makes later callers wait for a URL token that arrives during bootstrap", async () => {
    let releaseExchange!: () => void;
    const exchangeRelease = new Promise<void>((resolve) => {
      releaseExchange = resolve;
    });
    let markExchangeStarted!: () => void;
    const exchangeStarted = new Promise<void>((resolve) => {
      markExchangeStarted = resolve;
    });
    const nextSession = sequence(
      authenticatedSession(LOOPBACK_AUTH),
      authenticatedSession(LOOPBACK_AUTH),
    );
    const testApi = await installAuthApi({
      session: nextSession,
      browserSession: () => {
        markExchangeStarted();
        return Effect.promise(() => exchangeRelease).pipe(
          Effect.andThen(
            Effect.fail(
              new EnvironmentAuthInvalidError({
                code: "auth_invalid",
                reason: "invalid_credential",
                traceId: "trace-rejected-queued-credential",
              }),
            ),
          ),
        );
      },
    });
    const testWindow = installTestBrowser("http://localhost/");
    const { resolveInitialServerAuthGateState } = await import("./environments/primary");

    const initialBootstrap = resolveInitialServerAuthGateState();
    testWindow.location = new URL("http://localhost/#token=reusable-token");
    const explicitPairing = resolveInitialServerAuthGateState();
    const laterCaller = resolveInitialServerAuthGateState();
    let laterCallerSettled = false;
    void laterCaller.then(() => {
      laterCallerSettled = true;
    });

    try {
      await expect(initialBootstrap).resolves.toEqual({ status: "authenticated" });
      await exchangeStarted;
      expect(laterCallerSettled).toBe(false);
    } finally {
      releaseExchange();
    }

    const rejectedState = {
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
      errorMessage: "Invalid pairing token. Check the token and try again.",
    } as const;
    await expect(explicitPairing).resolves.toEqual(rejectedState);
    await expect(laterCaller).resolves.toEqual(rejectedState);
    expect(testApi.calls.browserSession).toMatchObject([{ credential: "reusable-token" }]);
    expect(testApi.calls.session).toBe(2);
  });

  it("reports a rejected URL token without caching false success", async () => {
    const cause = new EnvironmentAuthInvalidError({
      code: "auth_invalid",
      reason: "invalid_credential",
      traceId: "trace-invalid-url-credential",
    });
    const nextSession = sequence(
      authenticatedSession(LOOPBACK_AUTH),
      unauthenticatedSession(LOOPBACK_AUTH),
    );
    const testApi = await installAuthApi({
      session: nextSession,
      browserSession: () => Effect.fail(cause),
    });
    installTestBrowser("http://localhost/#token=rejected-token");
    const { resolveInitialServerAuthGateState } = await import("./environments/primary");

    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
      errorMessage: "Invalid pairing token. Check the token and try again.",
    });
    await expect(resolveInitialServerAuthGateState()).resolves.toEqual({
      status: "requires-auth",
      auth: LOOPBACK_AUTH,
    });
    expect(testApi.calls.browserSession).toMatchObject([{ credential: "rejected-token" }]);
  });
});
