import { describe, expect, it } from "@effect/vitest";
import { type OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";

import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import { gitHubIdentity } from "../issueLinks/IssueLinks.testFixtures.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as GitHubApi from "../sourceControl/GitHubApi.ts";
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

/** The service over a fake GitHub whose GraphQL answers come from `graphql`. */
const serviceWith = (
  shells: ReadonlyArray<OrchestrationProjectShell>,
  graphql: GitHubApi.GitHubApi["Service"]["graphql"],
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
