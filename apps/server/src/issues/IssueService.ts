import {
  gitHubRepositoryOf,
  type IssueCommentInput,
  type IssueDetail,
  type IssueListEntry,
  type IssueListInput,
  type IssueListRepository,
  type IssueListResult,
  IssueOperationError,
  type IssuePullRequest,
  type IssueRef,
  type IssueSetStateInput,
  type IssueStatesInput,
  type IssueStatesResult,
  pullRequestHostOf,
  type SourceControlProviderKind,
} from "@t3tools/contracts";
import { sourceControlRepositorySelector } from "@t3tools/shared/sourceControl";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";

import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as GitHubApi from "../sourceControl/GitHubApi.ts";
import { type GraphQlDocument, readGraphQlPages } from "../sourceControl/githubGraphQl.ts";
import {
  decodeIssueDetailJson,
  decodeIssueStatesJson,
  issueDetailOf,
  decodeIssueSearchJson,
  decodeViewerJson,
  type GitHubIssueSearchJson,
  ISSUE_DETAIL_GRAPHQL_QUERY,
  ISSUE_SEARCH_MAX_ROWS,
  issueLinkOf,
  issueSearchGraphQlDocument,
  issueSearchQuery,
  issueStatesGraphQlDocument,
  issueStatesOf,
  LINKED_PULL_REQUEST_MAX,
  linkedPullRequestsGraphQlDocument,
  linkedPullRequestsOf,
  pullRequestOf,
  repositoryParts,
} from "./gitHubIssues.ts";

/** Repositories named in one search, as the pull request listing chunks them. */
const REPOSITORY_SEARCH_CHUNK = 100;
const DEFAULT_LIMIT = 50;
const SEARCH_CONCURRENCY = 4;

export class IssueService extends Context.Service<
  IssueService,
  {
    readonly list: (input: IssueListInput) => Effect.Effect<IssueListResult, IssueOperationError>;
    readonly detail: (input: IssueRef) => Effect.Effect<IssueDetail, IssueOperationError>;
    /** Only the current state of each Issue, one request per host; for the thread links panel. */
    readonly states: (
      input: IssueStatesInput,
    ) => Effect.Effect<IssueStatesResult, IssueOperationError>;
    readonly comment: (input: IssueCommentInput) => Effect.Effect<void, IssueOperationError>;
    readonly setState: (input: IssueSetStateInput) => Effect.Effect<void, IssueOperationError>;
  }
>()("t3/issues/IssueService") {}

interface Workspace {
  readonly repositories: ReadonlyArray<IssueListRepository>;
  readonly unsupported: ReadonlyArray<{ readonly host: string; readonly repository: string }>;
}

type WorkspaceRepository = Workspace["repositories"][number];

interface Search {
  readonly key: string;
  readonly host: string;
  readonly chunk: ReadonlyArray<WorkspaceRepository>;
}

interface SearchAnswer {
  readonly search: Search;
  readonly rows: GitHubIssueSearchJson | null;
  readonly linked: ReadonlyArray<IssuePullRequest>;
  readonly error: IssueOperationError | null;
}

const failure = (operation: string, cause: unknown, fallback: string) =>
  new IssueOperationError({
    operation,
    detail:
      cause instanceof Error && cause.message.trim().length > 0 ? cause.message.trim() : fallback,
  });

/** A repository as it may appear in a REST path: `owner/name` with each part encoded. */
const restRepository = (repository: string) => {
  const { owner, name } = repositoryParts(repository);
  return `${encodeURIComponent(owner)}/${encodeURIComponent(name)}`;
};

const make = Effect.gen(function* () {
  const api = yield* GitHubApi.GitHubApi;
  const projects = yield* ProjectService.ProjectService;
  const issueLinks = yield* IssueLinks.IssueLinks;

  /**
   * Pull requests of threads that link to an Issue on `host`, newest link first, at most
   * `LINKED_PULL_REQUEST_MAX`; the rest are left out and logged.
   */
  const linkedPullRequestCandidates = (host: string) =>
    issueLinks.pullRequestsOfIssueThreads(host).pipe(
      Effect.tap((candidates) =>
        candidates.length > LINKED_PULL_REQUEST_MAX
          ? Effect.logWarning("Reading only the newest thread-linked pull requests", {
              host,
              candidates: candidates.length,
              read: LINKED_PULL_REQUEST_MAX,
            })
          : Effect.void,
      ),
      Effect.map((candidates) => candidates.slice(0, LINKED_PULL_REQUEST_MAX)),
      Effect.catch((cause) =>
        Effect.logWarning("Could not read thread-linked pull requests", cause).pipe(
          Effect.as<ReadonlyArray<{ repository: string; number: number }>>([]),
        ),
      ),
    );

  const workspace = projects.listShells().pipe(
    Effect.mapError((cause) =>
      failure("listProjects", cause, "The project list could not be read."),
    ),
    Effect.map((shells): Workspace => {
      const repositories: Array<IssueListRepository> = [];
      const unsupported: Array<{ host: string; repository: string }> = [];
      const seen = new Set<string>();
      for (const project of shells) {
        const identity = project.repositoryIdentity;
        if (!identity) continue;
        // A fork checkout lists its own repository's Issues, not the upstream it tracks.
        const gitHub = gitHubRepositoryOf(identity);
        const selector = sourceControlRepositorySelector(identity);
        const host =
          gitHub?.host ??
          pullRequestHostOf(identity, identity.provider as SourceControlProviderKind);
        const repository = gitHub?.repository ?? selector;
        if (repository === null) continue;
        // Worktrees of one repository are separate projects; count and list the repository once.
        const key = `${host} ${repository.toLowerCase()}`;
        if (seen.has(key)) continue;
        seen.add(key);
        if (gitHub === null) {
          unsupported.push({ host, repository });
          continue;
        }
        repositories.push({
          host,
          repository,
          projectId: project.id,
          projectTitle: project.title,
        });
      }
      return { repositories, unsupported };
    }),
  );

  const graphqlRead = <A>(input: {
    readonly host: string;
    readonly operation: string;
    readonly document: GraphQlDocument;
    readonly decode: (raw: string) => Result.Result<A, unknown>;
  }) =>
    api
      .graphql({
        host: input.host,
        operation: input.operation,
        query: input.document.query,
        variables: input.document.variables,
      })
      .pipe(
        Effect.mapError((cause) => failure(input.operation, cause, "GitHub could not be read.")),
        Effect.flatMap((raw) => {
          const decoded = input.decode(raw.trim());
          return Result.isSuccess(decoded)
            ? Effect.succeed(decoded.success)
            : Effect.fail(
                new IssueOperationError({
                  operation: input.operation,
                  detail: "GitHub answered in an unexpected shape.",
                }),
              );
        }),
      );

  /**
   * Only hosts of this environment's GitHub projects are read or written: a request names its
   * host itself, and the server's GitHub credential must never go to a host the client chose.
   */
  const requireHost = (host: string, operation: string) =>
    workspace.pipe(
      Effect.flatMap(({ repositories }) =>
        repositories.some((candidate) => candidate.host === host.toLowerCase())
          ? Effect.succeed(host.toLowerCase())
          : Effect.fail(
              new IssueOperationError({
                operation,
                detail: `No project on ${host} to read Issues through.`,
              }),
            ),
      ),
    );

  /**
   * One credential per host for the whole list, read once and pinned on each of its requests: a
   * `gh auth switch` mid-list cannot mix two accounts' rows, nor credit one account's review
   * marks to the other's viewer.
   */
  const pinnedCredentials = (hosts: ReadonlyArray<string>) =>
    Effect.forEach(hosts, (host) =>
      api.credential(host).pipe(
        Effect.map((held) => ({
          host,
          token: held.token,
          credentialFingerprint: held.fingerprint,
        })),
        Effect.mapError((cause) => failure("searchIssues", cause, "GitHub could not be read.")),
        Effect.cached,
        Effect.map((pinned) => [host, pinned] as const),
      ),
    ).pipe(
      Effect.map(
        (pins) =>
          <A, E, R>(host: string, read: Effect.Effect<A, E, R>) =>
            pins
              .find(([pinnedHost]) => pinnedHost === host)![1]
              .pipe(
                Effect.flatMap((pinned) =>
                  read.pipe(Effect.provideService(GitHubApi.PinnedGitHubCredential, pinned)),
                ),
              ),
      ),
    );

  const list: IssueService["Service"]["list"] = (input) =>
    Effect.gen(function* () {
      const { repositories, unsupported } = yield* workspace;
      const wanted =
        input.repositories === undefined
          ? repositories
          : repositories.filter((repository) =>
              input.repositories!.some(
                (key) =>
                  key.toLowerCase() === `${repository.host} ${repository.repository}`.toLowerCase(),
              ),
            );
      const byHost = new Map<string, Array<WorkspaceRepository>>();
      for (const repository of wanted) {
        const group = byHost.get(repository.host) ?? [];
        group.push(repository);
        byHost.set(repository.host, group);
      }
      const searches = [...byHost].flatMap(([host, group]) => {
        const chunks: Array<Search> = [];
        for (let start = 0; start < group.length; start += REPOSITORY_SEARCH_CHUNK) {
          const chunk = group.slice(start, start + REPOSITORY_SEARCH_CHUNK);
          chunks.push({ key: `${host}#${start / REPOSITORY_SEARCH_CHUNK}`, host, chunk });
        }
        return chunks;
      });
      // A continuation reads only the searches it names.
      const reads =
        input.cursors === undefined
          ? searches
          : searches.filter((search) => input.cursors![search.key] !== undefined);
      const limit = Math.min(input.limit ?? DEFAULT_LIMIT, ISSUE_SEARCH_MAX_ROWS);
      // A server whose repositories the list does not search still reads its thread-linked pull
      // requests: an Issue another server lists can be linked to a thread here.
      const searchedHosts = new Set(searches.map((search) => search.host));
      const linkedOnlyHosts =
        input.cursors === undefined
          ? [...new Set(repositories.map((repository) => repository.host))].filter(
              (host) => !searchedHosts.has(host),
            )
          : [];
      const pinned = yield* pinnedCredentials([
        ...new Set([...reads.map((search) => search.host), ...linkedOnlyHosts]),
      ]);
      const answers = yield* Effect.forEach(
        reads,
        (search): Effect.Effect<SearchAnswer> => {
          const q = issueSearchQuery({
            repositories: search.chunk.map((repository) => repository.repository),
            state: input.state,
            sort: input.sort,
            query: input.query,
            labels: input.labels,
            milestone: input.milestone,
          });
          if (q === null) return Effect.succeed({ search, rows: null, linked: [], error: null });
          const after = input.cursors?.[search.key];
          // A host's thread-linked pull requests ride along with its first search's first page.
          const withLinked = after === undefined && search.key === `${search.host}#0`;
          return (withLinked ? linkedPullRequestCandidates(search.host) : Effect.succeed([])).pipe(
            Effect.flatMap((candidates) =>
              pinned(
                search.host,
                // One page per call, from the client's cursor: "Load more" asks for the next.
                readGraphQlPages(
                  (from) =>
                    graphqlRead({
                      host: search.host,
                      operation: "searchIssues",
                      document: issueSearchGraphQlDocument({
                        rows: limit,
                        q,
                        after: from ?? undefined,
                        linkedPullRequests: candidates,
                        host: search.host,
                      }),
                      decode: (raw) =>
                        Result.map(decodeIssueSearchJson(raw), (rows) => ({
                          rows,
                          linked:
                            candidates.length === 0 ? [] : linkedPullRequestsOf(search.host, raw),
                        })),
                    }),
                  {
                    from: after ?? null,
                    maxPages: 1,
                    nextCursor: ({ rows }) =>
                      rows.data.search.pageInfo.hasNextPage
                        ? rows.data.search.pageInfo.endCursor
                        : null,
                  },
                ).pipe(Effect.map(({ pages }) => pages[0]!)),
              ),
            ),
            Effect.map(({ rows, linked }) => ({ search, rows, linked, error: null })),
            Effect.catch((error) => Effect.succeed({ search, rows: null, linked: [], error })),
          );
        },
        { concurrency: SEARCH_CONCURRENCY },
      );
      const linkedOnly = yield* Effect.forEach(
        linkedOnlyHosts,
        (host) =>
          linkedPullRequestCandidates(host).pipe(
            Effect.flatMap((candidates) =>
              candidates.length === 0
                ? Effect.succeed(null)
                : pinned(
                    host,
                    graphqlRead({
                      host,
                      operation: "readLinkedPullRequests",
                      document: linkedPullRequestsGraphQlDocument(candidates, host),
                      decode: (raw) =>
                        Result.map(decodeViewerJson(raw), (answer) => ({
                          host,
                          viewer: answer.data.viewer.login,
                          linked: linkedPullRequestsOf(host, raw),
                        })),
                    }),
                  ),
            ),
            // Only status inputs are missing then; the list itself is complete.
            Effect.catch((error) =>
              Effect.logWarning("Could not read thread-linked pull requests", error).pipe(
                Effect.as(null),
              ),
            ),
          ),
        { concurrency: SEARCH_CONCURRENCY },
      );

      const entries: Array<IssueListEntry> = [];
      const errors: Array<{ host: string; message: string }> = [];
      const nextCursors: Record<string, string> = {};
      const viewers = new Map<string, string>();
      const linkedPullRequests: Array<IssuePullRequest> = [];
      for (const { search, rows, linked, error } of answers) {
        if (error !== null) errors.push({ host: search.host, message: error.detail });
        if (rows === null) continue;
        const page = rows.data.search;
        viewers.set(search.host, rows.data.viewer.login);
        linkedPullRequests.push(...linked);
        if (page.pageInfo.hasNextPage && page.pageInfo.endCursor !== null) {
          nextCursors[search.key] = page.pageInfo.endCursor;
        }
        for (const node of page.nodes) {
          const owner = search.chunk.find(
            (repository) =>
              repository.repository.toLowerCase() === node.repository.nameWithOwner.toLowerCase(),
          );
          if (owner === undefined) continue;
          entries.push({
            ...issueLinkOf(search.host, node),
            projectId: owner.projectId,
            projectTitle: owner.projectTitle,
            author: node.author?.login || null,
            labels: node.labels.nodes.filter((label) => label.name.trim().length > 0),
            milestone: node.milestone?.title.trim() || null,
            commentCount: node.comments.totalCount,
            createdAt: node.createdAt,
            updatedAt: node.updatedAt,
            parent: node.parent === null ? null : issueLinkOf(search.host, node.parent),
            subIssues: node.subIssues.nodes.map((child) => issueLinkOf(search.host, child)),
            subIssueCount: node.subIssues.totalCount,
            openBlockerCount: node.issueDependenciesSummary.blockedBy,
            closingPullRequests: node.closedByPullRequestsReferences.nodes.flatMap((pullRequest) =>
              pullRequest === null ? [] : [pullRequestOf(search.host, pullRequest)],
            ),
          });
        }
      }
      for (const answer of linkedOnly) {
        if (answer === null) continue;
        viewers.set(answer.host, answer.viewer);
        linkedPullRequests.push(...answer.linked);
      }
      return {
        repositories,
        unsupported,
        errors,
        entries,
        viewers: [...viewers].map(([host, login]) => ({ host, login })),
        linkedPullRequests,
        nextCursors,
      };
    });

  const detail: IssueService["Service"]["detail"] = (input) =>
    Effect.gen(function* () {
      const host = yield* requireHost(input.host, "issueDetail");
      const { owner, name } = repositoryParts(input.repository);
      const answer = yield* graphqlRead({
        host,
        operation: "issueDetail",
        document: {
          query: ISSUE_DETAIL_GRAPHQL_QUERY,
          variables: { owner, name, number: input.number },
        },
        decode: decodeIssueDetailJson,
      });
      const issue = answer.data.repository?.issue ?? null;
      if (issue === null) {
        return yield* new IssueOperationError({
          operation: "issueDetail",
          detail: `${input.repository}#${input.number} was not found.`,
        });
      }
      return issueDetailOf(host, issue);
    });

  const states: IssueService["Service"]["states"] = (input) =>
    Effect.gen(function* () {
      const byHost = new Map<string, Array<IssueRef>>();
      for (const issue of input.issues) {
        const host = issue.host.toLowerCase();
        byHost.set(host, [...(byHost.get(host) ?? []), issue]);
      }
      const read = yield* Effect.forEach(
        [...byHost],
        ([host, issues]) =>
          Effect.gen(function* () {
            const document = issueStatesGraphQlDocument(issues, host);
            if (document === null) return issueStatesOf(issues, {});
            const answer = yield* graphqlRead({
              host: yield* requireHost(host, "issueStates"),
              operation: "issueStates",
              document,
              decode: decodeIssueStatesJson,
            });
            return issueStatesOf(issues, answer.data);
          }),
        { concurrency: SEARCH_CONCURRENCY },
      );
      return { issues: read.flat() };
    });

  /** A user's change to one Issue, through GitHub's REST API. */
  const issueWrite = (
    operation: string,
    input: IssueRef,
    request: {
      readonly method: "POST" | "PATCH";
      readonly path: string;
      readonly body: Record<string, unknown>;
    },
  ) =>
    requireHost(input.host, operation).pipe(
      Effect.flatMap((host) =>
        api
          .rest({
            host,
            operation,
            method: request.method,
            path: `repos/${restRepository(input.repository)}/issues/${input.number}${request.path}`,
            body: request.body,
            // A user acting on an Issue is not refused because a background read spent the quota.
            allowReserve: true,
          })
          .pipe(
            Effect.mapError((cause) =>
              failure(operation, cause, `GitHub refused to ${operation} the Issue.`),
            ),
          ),
      ),
      Effect.asVoid,
    );

  const comment: IssueService["Service"]["comment"] = (input) =>
    input.body.trim().length === 0
      ? Effect.fail(
          new IssueOperationError({ operation: "comment", detail: "The comment is empty." }),
        )
      : issueWrite("comment", input, {
          method: "POST",
          path: "/comments",
          body: { body: input.body },
        });

  const setState: IssueService["Service"]["setState"] = (input) =>
    input.action === "reopen"
      ? issueWrite("reopen", input, { method: "PATCH", path: "", body: { state: "open" } })
      : issueWrite("close", input, {
          method: "PATCH",
          path: "",
          body: {
            state: "closed",
            state_reason: input.action === "close-completed" ? "completed" : "not_planned",
          },
        });

  return IssueService.of({ list, detail, states, comment, setState });
});

export const layer = Layer.effect(IssueService, make);
