import { AuthStandardClientScopes } from "@t3tools/contracts";
import * as Sessions from "../auth/SessionStore.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as TestClock from "effect/testing/TestClock";
import * as Config from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Auth from "../auth/EnvironmentAuth.ts";
import * as OAuth from "../auth/McpOAuth.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import { REUSABLE_DEV_SESSION_EXPIRES_AT } from "../auth/ReusableDevAuth.ts";

const dependencies = Layer.mergeAll(
  Sqlite.layerMemory,
  Secrets.layer,
  Environment.layerIdentity,
).pipe(
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "t3-peer-permanent-" })),
  Layer.provideMerge(NodeServices.layer),
);
const layer = OAuth.layer.pipe(Layer.provideMerge(Auth.layer), Layer.provide(dependencies));
const request = HttpServerRequest.fromWeb(new Request("http://localhost/mcp"));
const urls = { issuer: "http://localhost", resource: "http://localhost/mcp" };
const redirectUri = "http://localhost/callback";
const verifier = "a".repeat(43) + "-verifier-for-tests";
const challenge = "DeB41nTVkPwpbbYecrnqtVq7VXLezustdHAK4SWt13c";

it.effect.each([true, false])(
  "pairing-code lifetime and immediate revoke (peerLink=%s)",
  (peerLink) =>
    Effect.gen(function* () {
      const auth = yield* Auth.EnvironmentAuth;
      const oauth = yield* OAuth.McpOAuth;
      const client = yield* oauth.register({
        client_name: "Peer",
        redirect_uris: [redirectUri],
        ...(peerLink ? { software_id: "t3code-peer-link" } : {}),
      });
      const authorization = yield* oauth.validateAuthorization({
        urls,
        request: {
          response_type: "code",
          client_id: client.clientId,
          redirect_uri: redirectUri,
          code_challenge: challenge,
          code_challenge_method: "S256",
          resource: urls.resource,
        },
      });
      const pairing = yield* auth.issuePairingCredential();
      const redirect = yield* oauth.approve({
        request,
        authorization,
        decision: { _tag: "pairing-code", access: "full-access", code: pairing.credential },
      });
      const token = yield* oauth.exchangeCode({
        request,
        urls,
        token: {
          grant_type: "authorization_code",
          code: new URL(redirect).searchParams.get("code")!,
          redirect_uri: redirectUri,
          client_id: client.clientId,
          code_verifier: verifier,
          resource: urls.resource,
        },
      });
      const bearer = HttpServerRequest.fromWeb(
        new Request("http://localhost/mcp", {
          headers: { authorization: `Bearer ${token.access_token}` },
        }),
      );
      const session = yield* auth.authenticateMcpClient(bearer);
      if (peerLink) {
        const listed = yield* auth.listSessions();
        assert.equal(
          listed.find((entry) => entry.sessionId === session.sessionId)?.expiresAt
            ?.epochMilliseconds,
          REUSABLE_DEV_SESSION_EXPIRES_AT.epochMilliseconds,
        );
      }
      yield* TestClock.adjust(Duration.days(31));
      if (!peerLink) {
        assert.isTrue(
          (yield* auth.authenticateMcpClient(bearer).pipe(Effect.result))._tag === "Failure",
        );
        return;
      }
      assert.equal((yield* auth.authenticateMcpClient(bearer)).sessionId, session.sessionId);
      yield* TestClock.adjust(Duration.days(3653));
      assert.equal((yield* auth.authenticateMcpClient(bearer)).sessionId, session.sessionId);
      yield* auth.revokeSession(session.sessionId);
      assert.isTrue(
        (yield* auth.authenticateMcpClient(bearer).pipe(Effect.result))._tag === "Failure",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("browser-approved peer links keep the 30-day expiry", () =>
  Effect.gen(function* () {
    const auth = yield* Auth.EnvironmentAuth;
    const oauth = yield* OAuth.McpOAuth;
    const sessions = yield* Sessions.SessionStore;
    const browser = yield* sessions.issue({
      method: "browser-session-cookie",
      scopes: AuthStandardClientScopes,
    });
    const browserRequest = HttpServerRequest.fromWeb(
      new Request("http://localhost/mcp", {
        headers: { cookie: `${sessions.cookieName}=${browser.token}` },
      }),
    );
    const client = yield* oauth.register({
      client_name: "Peer browser",
      redirect_uris: [redirectUri],
      software_id: "t3code-peer-link",
    });
    const authorization = yield* oauth.validateAuthorization({
      urls,
      request: {
        response_type: "code",
        client_id: client.clientId,
        redirect_uri: redirectUri,
        code_challenge: challenge,
        code_challenge_method: "S256",
        resource: urls.resource,
      },
    });
    const approval = yield* oauth.approvingBrowserSession(browserRequest, authorization);
    assert.isDefined(approval);
    const redirect = yield* oauth.approve({
      request: browserRequest,
      authorization,
      decision: { _tag: "browser-session", access: "full-access", csrfToken: approval!.csrfToken },
    });
    const token = yield* oauth.exchangeCode({
      request,
      urls,
      token: {
        grant_type: "authorization_code",
        code: new URL(redirect).searchParams.get("code")!,
        redirect_uri: redirectUri,
        client_id: client.clientId,
        code_verifier: verifier,
        resource: urls.resource,
      },
    });
    const bearer = HttpServerRequest.fromWeb(
      new Request("http://localhost/mcp", {
        headers: { authorization: `Bearer ${token.access_token}` },
      }),
    );
    assert.isTrue((yield* auth.authenticateMcpClient(bearer)).peerLink);
    assert.equal(token.expires_in, 30 * 24 * 60 * 60);
    yield* TestClock.adjust(Duration.days(31));
    assert.isTrue(
      (yield* auth.authenticateMcpClient(bearer).pipe(Effect.result))._tag === "Failure",
    );
  }).pipe(Effect.provide(layer)),
);
