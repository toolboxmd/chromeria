import * as GitHubAppSignIn from "../../../../packages/source-control-github/src/server/GitHubAppSignIn.ts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse, type HttpClientRequest } from "effect/http";
import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";
import * as GitHubRest from "@t3tools/source-control-github/server/GitHubRest";
import * as GitHubQuota from "@t3tools/source-control-github/server/GitHubQuota";
import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";

import * as IssueGitHubApi from "./IssueGitHubApi.ts";
import { issueStatesGraphQlDocument } from "./gitHubIssues.ts";

const jsonCodec = Schema.fromJsonString(Schema.Unknown);
const encodeResponse = Schema.encodeSync(jsonCodec);
const decodeRequest = Schema.decodeUnknownEffect(jsonCodec);

function harness(answer: () => unknown) {
  const requests: Array<HttpClientRequest.HttpClientRequest> = [];
  const dependencies = Layer.mergeAll(
    Layer.mock(GitHubAppSignIn.GitHubAppSignIn)({
      enabled: false,
      token: () => Effect.succeedNone,
    }),
    Layer.mock(GitHubCredentials.GitHubCredentials)({
      get: (host) =>
        Effect.succeed({
          host,
          token: Redacted.make(`token-${host}`),
          source: "gh" as const,
          fingerprint: host,
        }),
      saved: () => Effect.succeedNone,
      fingerprint: (host) => Effect.succeed(host),
      invalidate: () => Effect.void,
    }),
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) => {
        requests.push(request);
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(encodeResponse(answer()), {
              headers: { "content-type": "application/json" },
            }),
          ),
        );
      }),
    ),
    GitHubQuota.layer,
    SourceControlRateLimit.layer,
  );
  const layer = IssueGitHubApi.layer.pipe(
    Layer.provideMerge(GitHubRest.layer.pipe(Layer.provideMerge(dependencies))),
  );
  return { layer, requests };
}

describe("IssueGitHubApi", () => {
  it.effect(
    "preserves Issue batch aliases and variables through the native authenticated client",
    () => {
      const document = issueStatesGraphQlDocument(
        [
          { repository: "toolboxmd/chromeria", number: 411 },
          { repository: "invalid repository", number: 2 },
          { repository: "toolboxmd/contextmd", number: 34 },
        ],
        "github.com",
      )!;
      const data = { issue0: { state: "OPEN" }, issue2: { state: "CLOSED" } };
      const { layer, requests } = harness(() => ({ data }));
      return Effect.gen(function* () {
        const api = yield* IssueGitHubApi.IssueGitHubApi;
        const raw = yield* api.graphql({
          host: "github.com",
          operation: "issueStates",
          ...document,
        });
        expect(yield* decodeRequest(raw)).toEqual({ data });
        expect(requests).toHaveLength(1);
        expect(requests[0]!.url).toBe("https://api.github.com/graphql");
        expect(requests[0]!.headers.authorization).toBe("Bearer token-github.com");
        const body = requests[0]!.body;
        if (body._tag !== "Uint8Array") throw new Error("Expected a JSON GraphQL request");
        expect(yield* decodeRequest(new TextDecoder().decode(body.body))).toMatchObject({
          operationName: "IssueStates",
          query: document.query,
          variables: {
            issue0_url: "https://github.com/toolboxmd/chromeria/issues/411",
            issue2_url: "https://github.com/toolboxmd/contextmd/issues/34",
          },
        });
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect("rejects GraphQL errors even when an answer contains partial data", () => {
    const { layer } = harness(() => ({
      data: { viewer: null },
      errors: [{ message: "The viewer could not be read" }],
    }));
    return Effect.gen(function* () {
      const api = yield* IssueGitHubApi.IssueGitHubApi;
      const error = yield* api
        .graphql({
          host: "github.com",
          operation: "viewer",
          query: "query IssueViewer { viewer { login } }",
        })
        .pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "GitHubApiResponseError",
        githubErrors: ["The viewer could not be read"],
      });
    }).pipe(Effect.provide(layer));
  });
});
