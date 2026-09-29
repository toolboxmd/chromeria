import { describe, expect, it } from "@effect/vitest";
import { type OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as IssueLinks from "../issueLinks/IssueLinks.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as GitHubCli from "../sourceControl/GitHubCli.ts";
import * as GitHubGraphQlBudget from "../sourceControl/githubGraphQlBudget.ts";
import * as IssueService from "./IssueService.ts";

const project: OrchestrationProjectShell = {
  id: ProjectId.make("web"),
  title: "acme/web",
  workspaceRoot: "/work/web",
  repositoryIdentity: {
    canonicalKey: "github.com/acme/web",
    locator: {
      source: "git-remote",
      remoteName: "origin",
      remoteUrl: "https://github.com/acme/web.git",
    },
    provider: "github",
    owner: "acme",
    name: "web",
  },
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
};

/** A service whose `gh` answers each GraphQL request with `answer`, recording what it was sent. */
function serviceLayer(answer: (query: string) => Effect.Effect<string, GitHubCli.GitHubCliError>) {
  const sent: Array<string> = [];
  const github = {
    execute: (input: { readonly stdin?: string }) => {
      const query = (JSON.parse(input.stdin ?? "{}") as { query: string }).query;
      sent.push(query);
      return answer(query).pipe(
        Effect.map((stdout) => ({
          exitCode: 0,
          stdout,
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
        })),
      );
    },
  } as unknown as GitHubCli.GitHubCli["Service"];
  const layer = IssueService.layer.pipe(
    Layer.provide(Layer.succeed(GitHubCli.GitHubCli, github)),
    Layer.provide(
      Layer.succeed(GitHubGraphQlBudget.GitHubGraphQlBudget, {
        query: (_host, document) => Effect.succeed(document),
        observe: () => Effect.void,
      }),
    ),
    Layer.provide(
      Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getProjectShells: () => Effect.succeed([project]),
      } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]),
    ),
    Layer.provide(
      Layer.succeed(IssueLinks.IssueLinks, {} as unknown as IssueLinks.IssueLinks["Service"]),
    ),
  );
  return { layer, sent };
}

const open12 = { host: "github.com", repository: "acme/web", number: 12 };
const closed13 = { host: "github.com", repository: "acme/web", number: 13 };
const gone14 = { host: "github.com", repository: "acme/web", number: 14 };

describe("IssueService.states", () => {
  it.effect(
    "reads each Issue's state and row fields in one request, and marks missing ones null",
    () => {
      const { layer, sent } = serviceLayer(() =>
        Effect.succeed(
          JSON.stringify({
            data: {
              issue0: {
                state: "OPEN",
                stateReason: null,
                title: "Rows read like pull requests",
                updatedAt: "2026-09-29T09:00:00Z",
                author: { login: "octo", avatarUrl: "https://avatars.example/octo" },
              },
              issue1: { state: "CLOSED", stateReason: "COMPLETED", author: null },
              issue2: null,
            },
          }),
        ),
      );
      return Effect.gen(function* () {
        const issues = yield* IssueService.IssueService;
        const result = yield* issues.states({ issues: [open12, closed13, gone14] });
        expect(result.issues).toEqual([
          {
            ...open12,
            state: "open",
            title: "Rows read like pull requests",
            updatedAt: "2026-09-29T09:00:00Z",
            author: { login: "octo", name: null, avatarUrl: "https://avatars.example/octo" },
          },
          // A deleted author reads as no author, so the row shows GitHub's "ghost".
          { ...closed13, state: "done", author: null },
          { ...gone14, state: null },
        ]);
        expect(sent).toHaveLength(1);
        expect(sent[0]).toContain('resource(url: "https://github.com/acme/web/issues/13")');
        expect(sent[0]).toContain(
          "... on Issue { state stateReason title updatedAt author { login avatarUrl } }",
        );
        // No body, comments or closing references: the panel's rows need none of them.
        expect(sent[0]).not.toMatch(/body|comments|closedByPullRequestsReferences/);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect("fails the read when GitHub cannot be reached", () => {
    const { layer } = serviceLayer(() =>
      Effect.fail(
        new GitHubCli.GitHubCliCommandError({
          command: "gh",
          cwd: "/work/web",
          cause: new Error("API rate limit exceeded"),
        }),
      ),
    );
    return Effect.gen(function* () {
      const issues = yield* IssueService.IssueService;
      const error = yield* Effect.flip(issues.states({ issues: [open12] }));
      expect(error._tag).toBe("IssueOperationError");
      expect(error.operation).toBe("issueStates");
    }).pipe(Effect.provide(layer));
  });

  it.effect("asks GitHub nothing for no Issues", () => {
    const { layer, sent } = serviceLayer(() => Effect.die("unexpected read"));
    return Effect.gen(function* () {
      const issues = yield* IssueService.IssueService;
      expect(yield* issues.states({ issues: [] })).toEqual({ issues: [] });
      expect(sent).toEqual([]);
    }).pipe(Effect.provide(layer));
  });
});
