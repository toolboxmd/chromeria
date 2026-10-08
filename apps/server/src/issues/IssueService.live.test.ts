/**
 * Live suite: the real GitHub API with the GitHub CLI's credential, nothing faked between them.
 * Skipped unless `T3CODE_ISSUES_LIVE=1` and `gh` is signed in to github.com.
 *
 *   T3CODE_ISSUES_LIVE=1 vp test run src/issues/IssueService.live.test.ts
 *
 * Reads use existing toolboxmd Issues and pull requests. Comment and close/reopen run only when
 * `T3CODE_ISSUES_LIVE_FIXTURE` names a disposable Issue number in toolboxmd/chromeria (labelled
 * `test-fixture`); the suite leaves it closed as completed.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { type OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient } from "effect/http";

import * as ClosingReferences from "../issueLinks/closingReferences.ts";
import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import { gitHubIdentity } from "../issueLinks/IssueLinks.testFixtures.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitHubApi from "../sourceControl/GitHubApi.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as IssueService from "./IssueService.ts";

const live = process.env.T3CODE_ISSUES_LIVE === "1";
const fixtureNumber = Number(process.env.T3CODE_ISSUES_LIVE_FIXTURE ?? "");
const hasFixture = Number.isInteger(fixtureNumber) && fixtureNumber > 0;

const project = (
  id: string,
  identity: OrchestrationProjectShell["repositoryIdentity"],
): OrchestrationProjectShell => ({
  id: ProjectId.make(id),
  title: id,
  workspaceRoot: `/work/${id}`,
  repositoryIdentity: identity,
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
});

const projects = [
  // A checkout of the fork tracking its upstream lists the fork's Issues.
  project("chromeria", gitHubIdentity("pingdotgg/t3code", "toolboxmd/chromeria")),
  // A worktree of the same repository is listed once.
  project("chromeria-worktree", gitHubIdentity("pingdotgg/t3code", "toolboxmd/chromeria")),
  project("model-router", gitHubIdentity("toolboxmd/model-router")),
  project("gitlab", {
    canonicalKey: "gitlab.com/someone/elsewhere",
    locator: { source: "git-remote", remoteName: "origin", remoteUrl: "https://gitlab.com/x.git" },
    provider: "gitlab",
    owner: "someone",
    name: "elsewhere",
  }),
];

const github = GitHubApi.layerWithDependencies.pipe(
  Layer.provide(ServerSettings.layerTest({})),
  Layer.provide(VcsProcess.layer),
  Layer.provide(FetchHttpClient.layer),
  Layer.provide(NodeServices.layer),
);

const layer = IssueService.layer.pipe(
  Layer.provide(
    Layer.mock(ProjectService.ProjectService)({ listShells: () => Effect.succeed(projects) }),
  ),
  // Thread-linked pull requests ride along with the first search page: two merged ones here.
  Layer.provide(
    Layer.mock(IssueLinks.IssueLinks)({
      pullRequestsOfIssueThreads: () =>
        Effect.succeed([
          { repository: "toolboxmd/chromeria", number: 160 },
          { repository: "toolboxmd/chromeria", number: 156 },
        ]),
    }),
  ),
  Layer.provideMerge(ClosingReferences.layer),
  Layer.provideMerge(github),
);

describe.skipIf(!live)("IssueService (live GitHub)", () => {
  it.layer(layer, { timeout: 120_000 })("reads", (it) => {
    it.effect("lists every project repository, once, and reports other forges", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const result = yield* issues.list({ state: "all", limit: 10 });
        expect(result.errors).toEqual([]);
        expect(result.repositories.map((repository) => repository.repository).toSorted()).toEqual([
          "toolboxmd/chromeria",
          "toolboxmd/model-router",
        ]);
        expect(result.unsupported).toEqual([
          { host: "gitlab.com", repository: "someone/elsewhere" },
        ]);
        expect(result.entries.length).toBeGreaterThan(0);
        for (const entry of result.entries) {
          expect(["toolboxmd/chromeria", "toolboxmd/model-router"]).toContain(entry.repository);
          expect(entry.url).toContain(`/${entry.repository}/issues/${entry.number}`);
        }
        expect(result.viewers.map((viewer) => viewer.host)).toEqual(["github.com"]);
        // The aliased thread-linked pull requests came back with GitHub's own state.
        expect(
          result.linkedPullRequests.map((pullRequest) => [pullRequest.number, pullRequest.state]),
        ).toEqual([
          [160, "merged"],
          [156, "merged"],
        ]);
      }),
    );

    it.effect("pages through a repository with its continuation", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const input = {
          state: "all",
          sort: "created",
          repositories: ["github.com toolboxmd/chromeria"],
          limit: 3,
        } as const;
        const first = yield* issues.list(input);
        expect(first.entries).toHaveLength(3);
        expect(Object.keys(first.nextCursors)).toEqual(["github.com#0"]);
        const second = yield* issues.list({ ...input, cursors: first.nextCursors });
        expect(second.entries).toHaveLength(3);
        const firstNumbers = first.entries.map((entry) => entry.number);
        for (const entry of second.entries) expect(firstNumbers).not.toContain(entry.number);
        // Created order, newest first, carries across the page boundary.
        expect(Math.min(...firstNumbers)).toBeGreaterThan(
          Math.max(...second.entries.map((entry) => entry.number)),
        );
      }),
    );

    it.effect("reads an Issue's parent and sub-Issues", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const result = yield* issues.list({
          state: "all",
          repositories: ["github.com toolboxmd/chromeria"],
          query: "Issues in Chromeria",
          limit: 50,
        });
        const spec = result.entries.find((entry) => entry.number === 25);
        expect(spec?.subIssues.map((child) => child.number)).toEqual(
          expect.arrayContaining([27, 28, 29]),
        );
        const child = result.entries.find((entry) => entry.number === 27);
        if (child !== undefined) expect(child.parent).toMatchObject({ number: 25 });
      }),
    );

    it.effect("reads an Issue with its body and comments", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const detail = yield* issues.detail({
          host: "github.com",
          repository: "toolboxmd/chromeria",
          number: 166,
        });
        expect(detail.title).toContain("Absorb upstream");
        expect(detail.body).toContain("## Acceptance criteria");
        expect(detail.comments.length).toBeGreaterThan(0);
        expect(detail.comments.length).toBeLessThanOrEqual(detail.commentCount);
      }),
    );

    it.effect("reports a missing Issue as an operation error", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const error = yield* issues
          .detail({ host: "github.com", repository: "toolboxmd/chromeria", number: 999_999 })
          .pipe(Effect.flip);
        expect(error._tag).toBe("IssueOperationError");
      }),
    );

    it.effect("reads linked Issues' states, a missing one as none", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const { issues: states } = yield* issues.states({
          issues: [166, 999_999].map((number) => ({
            host: "github.com",
            repository: "toolboxmd/chromeria",
            number,
          })),
        });
        expect(states.map((entry) => [entry.number, entry.state])).toEqual([
          [166, "open"],
          [999_999, null],
        ]);
        expect(states[0]?.title).toContain("Absorb upstream");
      }),
    );

    it.effect("reads the Issues a pull request closes", () =>
      Effect.gen(function* () {
        const closing = yield* ClosingReferences.IssueClosingReferences;
        expect(
          yield* closing.issuesClosedBy({
            pullRequest: { host: "github.com", repository: "toolboxmd/chromeria", number: 160 },
            version: null,
          }),
        ).toEqual([{ host: "github.com", repository: "toolboxmd/chromeria", number: 159 }]);
      }),
    );

    it.effect("refuses a host none of the projects is on", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const error = yield* issues
          .detail({ host: "example.com", repository: "toolboxmd/chromeria", number: 1 })
          .pipe(Effect.flip);
        expect(error.detail).toBe("No project on example.com to read Issues through.");
      }),
    );
  });

  it.layer(layer, { timeout: 180_000 })("fixture actions", (it) => {
    it.effect.skipIf(!hasFixture)("comments, closes as not planned, reopens and closes", () =>
      Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const ref = {
          host: "github.com",
          repository: "toolboxmd/chromeria",
          number: fixtureNumber,
        };
        const marker = `Live suite comment ${DateTime.formatIso(yield* DateTime.now)}`;
        yield* issues.comment({ ...ref, body: marker });
        const commented = yield* issues.detail(ref);
        expect(commented.comments.map((comment) => comment.body)).toContain(marker);

        yield* issues.setState({ ...ref, action: "close-not-planned" });
        expect((yield* issues.detail(ref)).state).toBe("not-planned");
        yield* issues.setState({ ...ref, action: "reopen" });
        expect((yield* issues.detail(ref)).state).toBe("open");
        // Left closed, as a fixture should be.
        yield* issues.setState({ ...ref, action: "close-completed" });
        expect((yield* issues.detail(ref)).state).toBe("done");

        const empty = yield* issues.comment({ ...ref, body: "   " }).pipe(Effect.flip);
        expect(empty.detail).toBe("The comment is empty.");
      }),
    );
  });
});
