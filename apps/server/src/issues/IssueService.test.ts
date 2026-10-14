import { describe, expect, it } from "@effect/vitest";
import { type OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";

import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import { gitHubIdentity } from "../issueLinks/IssueLinks.testFixtures.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as GitHubApi from "@t3tools/source-control-github/server/GitHubApi";
import * as IssueService from "./IssueService.ts";

/** More repositories than one search names, so the host is read in two searches. */
const projects: ReadonlyArray<OrchestrationProjectShell> = Array.from(
  { length: 101 },
  (_, index) => ({
    id: ProjectId.make(`project-${index}`),
    title: `repo-${index}`,
    workspaceRoot: `/work/repo-${index}`,
    repositoryIdentity: gitHubIdentity(`acme/repo-${index}`),
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
  }),
);

const emptyPage = (viewer: string) =>
  JSON.stringify({
    data: {
      viewer: { login: viewer },
      search: { pageInfo: { hasNextPage: false, endCursor: null }, nodes: [] },
    },
  });

const page = (endCursor: string | null) =>
  JSON.stringify({
    data: {
      viewer: { login: "viewer" },
      search: { pageInfo: { hasNextPage: endCursor !== null, endCursor }, nodes: [] },
    },
  });

/** Resolves no folder to a repository; the shells carry their identity. */
const noIdentities: RepositoryIdentityResolver.RepositoryIdentityResolver["Service"]["resolve"] =
  () => Effect.succeed(null);

const resolverLayer = (
  resolve: RepositoryIdentityResolver.RepositoryIdentityResolver["Service"]["resolve"],
) => Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({ resolve });

/** The service over a fake GitHub whose GraphQL answers come from `graphql`. */
const serviceWith = (
  shells: ReadonlyArray<OrchestrationProjectShell>,
  graphql: GitHubApi.GitHubApi["Service"]["graphql"],
  resolve = noIdentities,
) =>
  Effect.provide(
    IssueService.IssueService,
    IssueService.layer.pipe(
      Layer.provide(
        Layer.mock(GitHubApi.GitHubApi)({
          credential: () =>
            Effect.succeed({ token: Redacted.make("token"), fingerprint: "account" }),
          graphql,
        }),
      ),
      Layer.provide(
        Layer.mock(ProjectService.ProjectService)({ listShells: () => Effect.succeed(shells) }),
      ),
      Layer.provide(resolverLayer(resolve)),
      Layer.provide(
        Layer.mock(IssueLinks.IssueLinks)({
          pullRequestsOfIssueThreads: () => Effect.succeed([]),
        }),
      ),
    ),
  );

describe("IssueService.list", () => {
  it.effect("reads one search page per call and carries on from the cursor it returned", () =>
    Effect.gen(function* () {
      const requests = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const issues = yield* serviceWith(projects.slice(0, 1), (input) =>
        Ref.updateAndGet(requests, (all) => [...all, input.variables?.after]).pipe(
          Effect.map((all) => page(all.length === 1 ? "cursor-1" : null)),
        ),
      );

      const first = yield* issues.list({ state: "open" });
      expect(yield* Ref.get(requests)).toEqual([null]);
      expect(first.nextCursors).toEqual({ "github.com#0": "cursor-1" });

      const next = yield* issues.list({ state: "open", cursors: first.nextCursors });
      expect(yield* Ref.get(requests)).toEqual([null, "cursor-1"]);
      expect(next.nextCursors).toEqual({});
    }),
  );

  it.effect("reads every search of a host under the one credential it pinned", () =>
    Effect.gen(function* () {
      // Every credential lookup returns the next account, as a `gh auth switch` mid-list would.
      const lookups = yield* Ref.make(0);
      const searches = yield* Ref.make<ReadonlyArray<string | null>>([]);
      const api = Layer.mock(GitHubApi.GitHubApi)({
        credential: () =>
          Ref.updateAndGet(lookups, (count) => count + 1).pipe(
            Effect.map((count) => ({
              token: Redacted.make(`token-${count}`),
              fingerprint: `account-${count}`,
            })),
          ),
        graphql: () =>
          Effect.gen(function* () {
            const pinned = yield* GitHubApi.PinnedGitHubCredential;
            yield* Ref.update(searches, (all) => [...all, pinned?.credentialFingerprint ?? null]);
            return emptyPage(pinned?.credentialFingerprint ?? "unpinned");
          }),
      });
      const issues = yield* Effect.provide(
        IssueService.IssueService,
        IssueService.layer.pipe(
          Layer.provide(api),
          Layer.provide(
            Layer.mock(ProjectService.ProjectService)({
              listShells: () => Effect.succeed(projects),
            }),
          ),
          Layer.provide(resolverLayer(noIdentities)),
          Layer.provide(
            Layer.mock(IssueLinks.IssueLinks)({
              pullRequestsOfIssueThreads: () => Effect.succeed([]),
            }),
          ),
        ),
      );

      const result = yield* issues.list({ state: "open" });
      expect(yield* Ref.get(lookups)).toBe(1);
      expect(yield* Ref.get(searches)).toEqual(["account-1", "account-1"]);
      expect(result.viewers).toEqual([{ host: "github.com", login: "account-1" }]);
      expect(result.errors).toEqual([]);
    }),
  );
});

describe("IssueService.states", () => {
  // A project shell carries its identity only while the one-minute enrichment cache holds it;
  // the links panel rereads every minute, so its reads often arrive just after it expired.
  it.effect("reads linked Issues while a project's identity is not cached", () =>
    Effect.gen(function* () {
      const uncached: OrchestrationProjectShell = {
        ...projects[0]!,
        workspaceRoot: "/work/contextmd",
        repositoryIdentity: null,
      };
      const issues = yield* serviceWith(
        [uncached],
        () =>
          Effect.succeed(
            JSON.stringify({
              data: {
                issue0: {
                  state: "OPEN",
                  stateReason: null,
                  title: "Capture: extract and judge one machine's sessions",
                  updatedAt: "2026-10-13T10:57:29Z",
                  author: { login: "lukemaj", avatarUrl: "https://avatars.example/lukemaj" },
                },
              },
            }),
          ),
        (cwd) =>
          Effect.succeed(cwd === "/work/contextmd" ? gitHubIdentity("toolboxmd/contextmd") : null),
      );

      const result = yield* issues.states({
        issues: [{ host: "github.com", repository: "toolboxmd/contextmd", number: 7 }],
      });
      expect(result.issues.map((entry) => [entry.number, entry.state, entry.title])).toEqual([
        [7, "open", "Capture: extract and judge one machine's sessions"],
      ]);
    }),
  );
});

describe("IssueService.readFeatureMap", () => {
  const contextmd: OrchestrationProjectShell = {
    ...projects[0]!,
    id: ProjectId.make("contextmd"),
    workspaceRoot: "/work/contextmd",
    repositoryIdentity: gitHubIdentity("toolboxmd/contextmd"),
  };
  const map = [
    "# Map",
    "",
    "```mermaid",
    "flowchart TD",
    '    C["Capture"]',
    "    %% C: #7 PR #22",
    '    SR["Serve report"]',
    "    %% SR: PR #38 #15",
    "    C --> SR",
    "```",
  ].join("\n");
  const head = JSON.stringify({
    data: { repository: { defaultBranchRef: { name: "main", target: { oid: "3f2a1c0abc" } } } },
  });
  const blob = (text: string | null) =>
    JSON.stringify({ data: { repository: { object: text === null ? null : { text } } } });
  const issuesPage = (numbers: ReadonlyArray<number>, endCursor: string | null) => ({
    repository: {
      issues: {
        pageInfo: { hasNextPage: endCursor !== null, endCursor },
        nodes: numbers.map((number) => ({
          number,
          title: `Issue ${number}`,
          url: `https://github.com/toolboxmd/contextmd/issues/${number}`,
        })),
      },
    },
  });
  const openIssue = {
    __typename: "Issue",
    url: "https://github.com/toolboxmd/contextmd/issues/7",
    state: "OPEN",
    stateReason: null,
    labels: { pageInfo: { hasNextPage: false }, nodes: [] },
    closedByPullRequestsReferences: { pageInfo: { hasNextPage: false }, nodes: [] },
  };

  it.effect(
    "reads the map at the default branch's head commit, then every ref and open Issue",
    () =>
      Effect.gen(function* () {
        const requests = yield* Ref.make<ReadonlyArray<GitHubApi.GitHubGraphQlInput>>([]);
        const issues = yield* serviceWith([contextmd], (input) =>
          Ref.updateAndGet(requests, (all) => [...all, input]).pipe(
            Effect.map(() => {
              if (input.operation === "featureMapHead") return head;
              if (input.operation === "featureMapFile") return blob(map);
              const after = input.variables?.after ?? null;
              return JSON.stringify({
                data:
                  after === null
                    ? {
                        ref0: openIssue,
                        ref1: null,
                        ref2: null,
                        ref3: null,
                        ...issuesPage([7, 9], "c1"),
                      }
                    : issuesPage([15], null),
              });
            }),
          ),
        );

        const snapshot = yield* issues.readFeatureMap({ projectId: contextmd.id });
        const sent = yield* Ref.get(requests);
        expect(sent.map((input) => input.operation)).toEqual([
          "featureMapHead",
          "featureMapFile",
          "featureMapFacts",
          "featureMapFacts",
        ]);
        // The file is read at the commit the head read returned, not at a moving branch name.
        expect(sent[1]?.variables).toMatchObject({ expression: "3f2a1c0abc:docs/map.md" });
        if (snapshot._tag !== "Map") throw new Error(`expected a map, got ${snapshot._tag}`);
        expect(snapshot).toMatchObject({
          host: "github.com",
          repository: "toolboxmd/contextmd",
          branch: "main",
          commit: "3f2a1c0abc",
          openIssuesPartial: false,
        });
        expect(snapshot.parts.map((part) => part.id)).toEqual(["C", "SR"]);
        expect(snapshot.facts.map((fact) => [fact.number, fact.fact?.kind ?? null])).toEqual([
          [7, "issue"],
          [22, null],
          [38, null],
          [15, null],
        ]);
        expect(snapshot.openIssues.map((issue) => issue.number)).toEqual([7, 9, 15]);
      }),
  );

  it.effect("answers NoMap when the default branch has no docs/map.md", () =>
    Effect.gen(function* () {
      const issues = yield* serviceWith([contextmd], (input) =>
        Effect.succeed(input.operation === "featureMapHead" ? head : blob(null)),
      );
      const snapshot = yield* issues.readFeatureMap({ projectId: contextmd.id });
      expect(snapshot).toMatchObject({ _tag: "NoMap", reason: "missing", commit: "3f2a1c0abc" });
    }),
  );

  it.effect("stops reading open Issues at the cap and marks the gap partial", () =>
    Effect.gen(function* () {
      const facts = yield* Ref.make(0);
      const issues = yield* serviceWith([contextmd], (input) => {
        if (input.operation === "featureMapHead") return Effect.succeed(head);
        if (input.operation === "featureMapFile") return Effect.succeed(blob(map));
        return Ref.updateAndGet(facts, (count) => count + 1).pipe(
          Effect.map((count) => JSON.stringify({ data: issuesPage([count], `cursor-${count}`) })),
        );
      });
      const snapshot = yield* issues.readFeatureMap({ projectId: contextmd.id });
      expect(yield* Ref.get(facts)).toBe(5);
      expect(snapshot).toMatchObject({ _tag: "Map", openIssuesPartial: true });
    }),
  );

  it.effect("fails, rather than answering an empty map, when GitHub cannot be read", () =>
    Effect.gen(function* () {
      const issues = yield* serviceWith([contextmd], (input) =>
        Effect.fail(
          new GitHubApi.GitHubApiRequestError({
            host: input.host,
            operation: input.operation,
            cause: new Error("offline"),
          }),
        ),
      );
      const error = yield* issues.readFeatureMap({ projectId: contextmd.id }).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "IssueOperationError",
        detail: "Could not reach GitHub at github.com.",
      });
    }),
  );

  it.effect("refuses a project that is not on GitHub before any read", () =>
    Effect.gen(function* () {
      let reads = 0;
      const issues = yield* serviceWith([contextmd], () => {
        reads++;
        return Effect.succeed(head);
      });
      const error = yield* issues
        .readFeatureMap({ projectId: ProjectId.make("elsewhere") })
        .pipe(Effect.flip);
      expect(error._tag).toBe("IssueOperationError");
      expect(reads).toBe(0);
    }),
  );
});
