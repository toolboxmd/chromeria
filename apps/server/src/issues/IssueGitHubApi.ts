import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  GraphQL,
  GraphQLClient,
  GraphQLClientError,
  GraphQLGroup,
  GraphQLProtocol,
} from "effect/graphql";

import * as GitHubRest from "@t3tools/source-control-github/server/GitHubRest";
import * as Policy from "@t3tools/source-control-github/server/GitHubRequestPolicy";
import * as GitHubGraphQl from "@t3tools/source-control-github/server/gitHubGraphQlClient";

export {
  GitHubApiAuthenticationError,
  GitHubApiNotFoundError,
  GitHubApiRateLimitError,
  GitHubApiRequestError,
  GitHubApiResponseError,
  PinnedGitHubCredential,
} from "@t3tools/source-control-github/server/GitHubRequestPolicy";
export type GitHubApiError = Policy.GitHubApiError | GraphQLClientError.GraphQLClientError;

export interface GitHubGraphQlInput {
  readonly host: string;
  readonly operation: string;
  readonly query: string;
  readonly variables?: Readonly<Record<string, unknown>>;
}

const encodeResponse = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const make = Effect.gen(function* () {
  const rest = yield* GitHubRest.GitHubRest;
  const protocol = yield* GraphQLProtocol.GraphQLProtocol;
  const auth = yield* GitHubGraphQl.GitHubGraphQlAuth;

  // Issue and map batches have a variable number of aliases. Their existing
  // decoders own the result shape; the native client owns transport and errors.
  const graphql = Effect.fn("IssueGitHubApi.graphql")(function* (input: GitHubGraphQlInput) {
    const name = /^\s*query\s+(\w+)/u.exec(input.query)?.[1];
    if (name === undefined) {
      return yield* new Policy.GitHubApiRequestError({
        host: input.host,
        operation: input.operation,
        cause: new Error("An Issue read needs a named GraphQL query."),
      });
    }
    const operation = GraphQL.query(name, {
      document: input.query,
      variables: Schema.Record(Schema.String, Schema.Unknown),
      result: Schema.Unknown,
    }).middleware(GitHubGraphQl.GitHubGraphQlAuth);
    const client = yield* GraphQLClient.make(GraphQLGroup.make(operation));
    const data = yield* client[name]!(input.variables ?? {}).pipe(
      GitHubGraphQl.call({ host: input.host, operation: input.operation }),
    );
    return yield* encodeResponse({ data }).pipe(
      Effect.mapError(
        (cause) =>
          new Policy.GitHubApiRequestError({ host: input.host, operation: input.operation, cause }),
      ),
    );
  });

  return {
    ...rest,
    graphql: (input: Parameters<typeof graphql>[0]) =>
      graphql(input).pipe(
        Effect.provideService(GraphQLProtocol.GraphQLProtocol, protocol),
        Effect.provideService(GitHubGraphQl.GitHubGraphQlAuth, auth),
      ),
  };
});

export class IssueGitHubApi extends Context.Service<IssueGitHubApi, Effect.Success<typeof make>>()(
  "t3/issues/IssueGitHubApi",
) {}

export const layer = Layer.effect(IssueGitHubApi, make).pipe(Layer.provide(GitHubGraphQl.layer));

export const layerWithDependencies = layer.pipe(
  Layer.provideMerge(GitHubRest.layerWithDependencies),
);
