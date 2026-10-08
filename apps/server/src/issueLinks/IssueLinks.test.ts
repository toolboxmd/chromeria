import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  type IssueKey,
  ProviderInstanceId,
  ThreadId,
  type ThreadIssueLink,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import type { Tool } from "effect/ai";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as IssuesHandlers from "../mcp/toolkits/issues/handlers.ts";
import { IssuesToolkit } from "../mcp/toolkits/issues/tools.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ClosingReferences from "./closingReferences.ts";
import * as IssueLinks from "./IssueLinks.ts";
import {
  deleteThread,
  gitHubIdentity,
  insertProject,
  pullRequestLink,
  readLayer,
  restoreThread,
  updateThread,
  v2Stores,
  writeThread,
} from "./IssueLinks.testFixtures.ts";
import { combineThreadIssueLinks } from "./threadIssueLinks.ts";

const IDENTITIES = {
  "/work/acme-web": gitHubIdentity("acme/web"),
  "/work/acme-api": gitHubIdentity("acme/api"),
};
const THREAD = ThreadId.make("thread-web");

/** GitHub's closing references, by pull request; nothing here reads GitHub. */
const CLOSES: Readonly<Record<string, ReadonlyArray<IssueKey>>> = {
  "github.com/acme/web#60": [{ host: "github.com", repository: "acme/web", number: 61 }],
};
const closingReferences = Layer.succeed(ClosingReferences.IssueClosingReferences, {
  issuesClosedBy: ({ pullRequest }) =>
    Effect.succeed(
      CLOSES[`${pullRequest.host}/${pullRequest.repository}#${pullRequest.number}`] ?? [],
    ),
});

const servicesOver = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  IssueLinks.layer.pipe(
    Layer.provide(closingReferences),
    Layer.provideMerge(readLayer(IDENTITIES)),
    Layer.provideMerge(v2Stores(database)),
  );
const services = servicesOver(SqlitePersistence.layerMemory);

const seed = Effect.gen(function* () {
  yield* insertProject("project-web", "/work/acme-web");
  yield* insertProject("project-api", "/work/acme-api");
  yield* writeThread({ id: THREAD, projectId: "project-web", branch: "feat/28-issue-links" });
  yield* writeThread({
    id: "thread-web-older",
    projectId: "project-web",
    branch: "fix/280-other-issue",
    updatedAt: "2026-09-01T00:00:00.000Z",
  });
  yield* writeThread({ id: "thread-plain", projectId: "project-web", branch: "release/2026-09" });
  yield* writeThread({ id: "thread-api", projectId: "project-api", branch: "feat/28-api-side" });
  yield* writeThread({ id: "thread-deleted", projectId: "project-web", branch: "feat/28-gone" });
  yield* deleteThread("thread-deleted", "2026-09-02T00:00:00.000Z");
});

const issue = (number: number, repository = "acme/web") => ({
  host: "github.com",
  repository,
  number,
});

const linksOf = (links: ReadonlyArray<ThreadIssueLink>) =>
  links.map((link) => ({ issue: `${link.repository}#${link.number}`, sources: link.sources }));

/** Thread ids and sources per Issue, for Issues with no closing pull requests. */
const threadsFor = (...issues: ReadonlyArray<ReturnType<typeof issue>>) =>
  Effect.flatMap(IssueLinks.IssueLinks, (links) =>
    links.threadsForIssues({
      issues: issues.map((key) => ({ ...key, closingPullRequests: [] })),
    }),
  ).pipe(
    Effect.map((results) =>
      results.map((result) => result.threads.map((thread) => [thread.id, thread.sources])),
    ),
  );

const storedRows = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) => sql<{ readonly threadId: string; readonly number: number; readonly source: string }>`
    SELECT thread_id AS "threadId", number, source FROM fork_thread_issue_links
    ORDER BY thread_id, number
  `,
);

describe("IssueLinks", () => {
  it.effect("links manually by number in the thread's repository, once", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      const first = yield* links.link({
        threadId: THREAD,
        target: { number: 12 },
        source: "manual",
      });
      expect(first).toEqual({ link: issue(12), alreadyLinked: false });
      const again = yield* links.link({
        threadId: THREAD,
        target: { url: "https://github.com/Acme/Web/issues/12" },
        source: "manual",
      });
      expect(again.alreadyLinked).toBe(true);

      const forThread = yield* links.forThread(THREAD);
      expect(linksOf(forThread)).toEqual([
        { issue: "acme/web#12", sources: ["manual"] },
        { issue: "acme/web#28", sources: ["branch"] },
      ]);
      expect(forThread[0]).toMatchObject({ url: "https://github.com/acme/web/issues/12" });
      expect(forThread[0]?.linkedAt).not.toBeNull();
      expect(yield* threadsFor(issue(12))).toEqual([[[THREAD, ["manual"]]]]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("derives branch links only from task branches in the project's repository", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      // One batched read for several Issues keeps the caller's order; unknown Issues are empty.
      expect(
        yield* threadsFor(issue(28), issue(28, "acme/api"), issue(280), issue(2026), issue(9)),
      ).toEqual([
        [[THREAD, ["branch"]]],
        [["thread-api", ["branch"]]],
        [["thread-web-older", ["branch"]]],
        [],
        [],
      ]);
      expect(linksOf(yield* links.forThread(ThreadId.make("thread-api")))).toEqual([
        { issue: "acme/api#28", sources: ["branch"] },
      ]);
      expect(yield* links.forThread(ThreadId.make("thread-plain"))).toEqual([]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("names a fork checkout's own repository for branch links", () =>
    Effect.gen(function* () {
      yield* insertProject("project-fork", "/work/fork");
      yield* writeThread({ id: "thread-fork", projectId: "project-fork", branch: "feat/171-x" });
      const forked = IssueLinks.layer.pipe(
        Layer.provide(closingReferences),
        Layer.provideMerge(
          readLayer({ "/work/fork": gitHubIdentity("pingdotgg/t3code", "toolboxmd/chromeria") }),
        ),
      );
      const links = yield* Effect.provide(IssueLinks.IssueLinks, forked);
      expect(linksOf(yield* links.forThread(ThreadId.make("thread-fork")))).toEqual([
        { issue: "toolboxmd/chromeria#171", sources: ["branch"] },
      ]);
    }).pipe(Effect.provide(v2Stores(SqlitePersistence.layerMemory))),
  );

  it.effect("derives links from the Issues the thread's pull requests close", () =>
    Effect.gen(function* () {
      yield* seed;
      yield* writeThread({
        id: "thread-closer",
        projectId: "project-web",
        pullRequests: [pullRequestLink("acme/web", 60)],
      });
      const links = yield* IssueLinks.IssueLinks;
      expect(linksOf(yield* links.forThread(ThreadId.make("thread-closer")))).toEqual([
        { issue: "acme/web#61", sources: ["closing-reference"] },
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("rolls up the links child threads hold, at any depth, only when asked", () =>
    Effect.gen(function* () {
      yield* seed;
      const child = "thread-web-child";
      const grandchild = "thread-web-grandchild";
      yield* writeThread({ id: child, projectId: "project-web", parent: { id: THREAD } });
      // A worker in another repository: its branch names that repository's Issue.
      yield* writeThread({
        id: grandchild,
        projectId: "project-api",
        branch: "feat/7-worker",
        parent: { id: child, root: THREAD },
      });
      // Not a descendant, though its id contains the thread's.
      yield* writeThread({ id: `sub.other-${THREAD}.c`, projectId: "project-web" });
      const links = yield* IssueLinks.IssueLinks;
      const link = (threadId: string, number: number, repository?: string) =>
        links.link({
          threadId: ThreadId.make(threadId),
          target: { number, repository },
          source: "agent",
        });
      yield* link(THREAD, 12);
      yield* link(child, 12);
      yield* link(child, 41);
      yield* links.unlink({ threadId: THREAD, issue: issue(41) });
      yield* link(grandchild, 40, "acme/web");
      yield* link(`sub.other-${THREAD}.c`, 99);

      expect(linksOf(yield* links.forThread(THREAD))).toEqual([
        { issue: "acme/web#12", sources: ["agent"] },
        { issue: "acme/web#28", sources: ["branch"] },
      ]);
      const rolledUp = yield* links.forThread(THREAD, { includeDescendants: true });
      expect(
        rolledUp.map((entry) => [`${entry.repository}#${entry.number}`, entry.linkedByThreadId]),
      ).toEqual([
        ["acme/web#12", undefined],
        ["acme/web#28", undefined],
        ["acme/web#40", grandchild],
        ["acme/api#7", grandchild],
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("rolls up only through unbroken spawned-thread links, never across a fork", () =>
    Effect.gen(function* () {
      yield* seed;
      const spawned = (id: string, parent: string, relationshipToParent: "subagent" | "fork") =>
        writeThread({
          id,
          projectId: "project-web",
          parent: { id: parent, root: THREAD },
          relationshipToParent,
        });
      // root -> fork -> subagent: the fork's worker is not the root's.
      yield* spawned("fork", THREAD, "fork");
      yield* spawned("fork-worker", "fork", "subagent");
      // root -> subagent -> fork -> subagent: the walk stops at the fork.
      yield* spawned("worker", THREAD, "subagent");
      yield* spawned("worker-fork", "worker", "fork");
      yield* spawned("worker-fork-worker", "worker-fork", "subagent");
      const links = yield* IssueLinks.IssueLinks;
      for (const [threadId, number] of [
        ["fork", 70],
        ["fork-worker", 71],
        ["worker", 72],
        ["worker-fork", 73],
        ["worker-fork-worker", 74],
      ] as const) {
        yield* links.link({
          threadId: ThreadId.make(threadId),
          target: { number },
          source: "agent",
        });
      }

      const rolledUp = yield* links.forThread(THREAD, { includeDescendants: true });
      expect(
        rolledUp.map((entry) => [`${entry.repository}#${entry.number}`, entry.linkedByThreadId]),
      ).toEqual([
        ["acme/web#28", undefined],
        ["acme/web#72", "worker"],
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("reports an Issue its branch already links as linked, without storing it", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      expect(
        yield* links.link({ threadId: THREAD, target: { number: 28 }, source: "manual" }),
      ).toEqual({ link: issue(28), alreadyLinked: true });
      expect(yield* storedRows).toEqual([]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("keeps a removed derived link removed until it is linked again", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      expect(yield* links.unlink({ threadId: THREAD, issue: issue(28) })).toEqual({
        wasLinked: true,
      });
      expect(yield* links.forThread(THREAD)).toEqual([]);
      expect(yield* threadsFor(issue(28))).toEqual([[]]);

      yield* links.link({ threadId: THREAD, target: { number: 28 }, source: "manual" });
      expect(linksOf(yield* links.forThread(THREAD))).toEqual([
        { issue: "acme/web#28", sources: ["manual", "branch"] },
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("records an explicit unlink even when nothing looked linked", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      const plain = ThreadId.make("thread-plain");
      expect(yield* links.unlink({ threadId: plain, issue: issue(50) })).toEqual({
        wasLinked: false,
      });
      // The branch later names #50; the explicit unlink still holds.
      yield* updateThread(plain, (thread) => ({ ...thread, branch: "feat/50-later" }));
      expect(yield* links.forThread(plain)).toEqual([]);
      expect(yield* threadsFor(issue(50))).toEqual([[]]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("unlinks a stored link and leaves other threads alone", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      yield* links.link({ threadId: THREAD, target: { number: 7 }, source: "manual" });
      yield* links.link({
        threadId: ThreadId.make("thread-web-older"),
        target: { number: 7 },
        source: "manual",
      });
      yield* links.unlink({ threadId: THREAD, issue: issue(7) });
      expect(yield* threadsFor(issue(7))).toEqual([[["thread-web-older", ["manual"]]]]);
    }).pipe(Effect.provide(services)),
  );

  it.effect(
    "links a draft started from an Issue, and moves the link when the draft is reused",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const links = yield* IssueLinks.IssueLinks;
        const draft = ThreadId.make("thread-draft");
        const start = (number: number) =>
          links.link({
            threadId: draft,
            target: { url: `https://github.com/acme/web/issues/${number}` },
            source: "started",
          });
        yield* start(99);
        // Invisible until the draft's first send creates the thread.
        expect(yield* threadsFor(issue(99))).toEqual([[]]);
        // The empty draft is reused for another Issue.
        yield* start(98);
        yield* writeThread({ id: draft, projectId: "project-web" });
        expect(yield* threadsFor(issue(99), issue(98))).toEqual([[], [[draft, ["started"]]]]);
      }).pipe(Effect.provide(services)),
  );

  it.effect("prunes only week-old deletions and drafts never sent within a week", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-09-01T00:00:00.000Z"));
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      const linkTo = (threadId: string, number: number, source: "manual" | "started") =>
        links.link({
          threadId: ThreadId.make(threadId),
          target: { number, repository: "acme/web" },
          source,
        });
      for (const id of ["thread-doomed", "thread-retried", "thread-just-deleted"]) {
        yield* writeThread({ id, projectId: "project-web" });
      }
      yield* linkTo("thread-doomed", 1, "manual");
      yield* linkTo("thread-abandoned-draft", 2, "started");
      yield* linkTo("thread-retried", 4, "started");
      yield* linkTo("thread-web-older", 5, "manual");
      yield* linkTo("thread-just-deleted", 6, "started");
      yield* deleteThread("thread-doomed", "2026-09-01T12:00:00.000Z");
      // A failed first-send bootstrap deletes the thread; the retry creates the same id again.
      yield* deleteThread("thread-retried", "2026-09-01T12:00:00.000Z");
      yield* restoreThread("thread-retried", "2026-09-01T12:00:01.000Z");

      yield* TestClock.adjust("8 days");
      yield* deleteThread("thread-just-deleted", "2026-09-08T12:00:00.000Z");
      yield* linkTo("thread-pending-draft", 3, "started");
      yield* threadsFor(issue(1));
      expect(yield* storedRows).toEqual([
        { threadId: "thread-just-deleted", number: 6, source: "started" },
        { threadId: "thread-pending-draft", number: 3, source: "started" },
        { threadId: "thread-retried", number: 4, source: "started" },
        { threadId: "thread-web-older", number: 5, source: "manual" },
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("links Issues to threads whose pull requests close them, from caller data", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      yield* writeThread({
        id: "thread-closer",
        projectId: "project-web",
        pullRequests: [pullRequestLink("acme/web", 30)],
      });
      yield* writeThread({
        id: "thread-dismissed-pr",
        projectId: "project-web",
        pullRequests: [pullRequestLink("acme/web", 30, "stack-dismissed")],
      });
      yield* writeThread({
        id: "thread-unlinked",
        projectId: "project-web",
        pullRequests: [pullRequestLink("acme/web", 31)],
      });
      yield* writeThread({
        id: "thread-cross",
        projectId: "project-api",
        pullRequests: [pullRequestLink("acme/api", 5)],
      });
      yield* links.unlink({ threadId: ThreadId.make("thread-unlinked"), issue: issue(11) });

      const result = yield* links.threadsForIssues({
        issues: [
          {
            host: "GitHub.com",
            repository: "ACME/Web",
            number: 11,
            closingPullRequests: [
              { repository: "Acme/Web", number: 30 },
              { repository: "acme/web", number: 31 },
              // A pull request in another repository closes this Issue too.
              { repository: "Acme/API", number: 5 },
            ],
          },
        ],
      });
      expect(result).toMatchObject([{ host: "github.com", repository: "acme/web", number: 11 }]);
      expect(result[0]!.threads.map((thread) => [thread.id, thread.sources]).toSorted()).toEqual([
        ["thread-closer", ["closing-reference"]],
        ["thread-cross", ["closing-reference"]],
      ]);
      // Each thread carries its own pull requests (#29 counts them toward the Issue's status).
      expect(
        result[0]!.threads.map((thread) => [thread.id, thread.pullRequests]).toSorted(),
      ).toEqual([
        ["thread-closer", [{ host: "github.com", repository: "acme/web", number: 30 }]],
        ["thread-cross", [{ host: "github.com", repository: "acme/api", number: 5 }]],
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect(
    "reads the pull requests of threads that link to an Issue, whatever their last state",
    () =>
      Effect.gen(function* () {
        const links = yield* IssueLinks.IssueLinks;
        yield* insertProject("project-web", "/work/acme-web");
        const thread = (
          id: string,
          pullRequests: ReadonlyArray<ReturnType<typeof pullRequestLink>>,
          branch: string | null = null,
        ) => writeThread({ id, projectId: "project-web", branch, pullRequests });
        const linkTo = (threadId: string, url: string) =>
          links.link({ threadId: ThreadId.make(threadId), target: { url }, source: "manual" });

        yield* thread("stored", [
          pullRequestLink("acme/web", 1),
          // Last synced as merged: GitHub's answer in the list's own read decides, not this one.
          {
            ...pullRequestLink("acme/web", 8),
            snapshot: {
              state: "merged",
              title: "Merged",
              headBranch: "feat/8-x",
              baseBranch: "main",
              isDraft: false,
              updatedAt: null,
              syncedAt: "2026-09-01T00:00:00.000Z",
            },
          },
          pullRequestLink("acme/web", 6, "stack-dismissed"),
        ]);
        yield* linkTo("stored", "https://github.com/acme/web/issues/1");
        // Component threads usually link only through their task branch.
        yield* thread("task-branch", [pullRequestLink("acme/web", 2)], "feat/29-issue-status");
        // The same pull request through an unlinked thread still counts once, via the linked one.
        yield* thread(
          "plain-branch",
          [pullRequestLink("acme/web", 3), pullRequestLink("acme/web", 1)],
          "release/2026-09",
        );
        yield* thread("dismissed", [pullRequestLink("acme/web", 4)]);
        yield* linkTo("dismissed", "https://github.com/acme/web/issues/4");
        yield* links.unlink({ threadId: ThreadId.make("dismissed"), issue: issue(4) });
        yield* thread("deleted", [pullRequestLink("acme/web", 5)], "feat/5-gone");
        yield* deleteThread("deleted", "2026-09-02T00:00:00.000Z");
        yield* thread("other-host", [pullRequestLink("acme/web", 7)]);
        yield* linkTo("other-host", "https://ghe.example.com/acme/web/issues/7");

        const read = yield* links.pullRequestsOfIssueThreads("GitHub.com");
        expect(read.map((candidate) => candidate.number).toSorted()).toEqual([1, 2, 8]);
        expect(yield* links.pullRequestsOfIssueThreads("ghe.example.com")).toEqual([]);
      }).pipe(Effect.provide(services)),
  );

  it.effect("refuses targets it cannot place", () =>
    Effect.gen(function* () {
      yield* seed;
      const links = yield* IssueLinks.IssueLinks;
      const pullRequestUrl = yield* links
        .link({
          threadId: THREAD,
          target: { url: "https://github.com/acme/web/pull/3" },
          source: "manual",
        })
        .pipe(Effect.flip);
      expect(pullRequestUrl).toMatchObject({ _tag: "IssueLinkError" });
      const noProject = yield* links
        .link({ threadId: ThreadId.make("missing"), target: { number: 3 }, source: "manual" })
        .pipe(Effect.flip);
      expect(noProject.detail).toContain("no GitHub repository");
    }).pipe(Effect.provide(services)),
  );

  it.effect("tells subscribers which thread and Issues changed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* seed;
        const links = yield* IssueLinks.IssueLinks;
        const changes = yield* links.subscribeChanges;
        yield* links.link({ threadId: THREAD, target: { number: 5 }, source: "manual" });
        yield* links.unlink({ threadId: THREAD, issue: issue(5) });
        expect(yield* changes.pipe(Stream.take(2), Stream.runCollect)).toEqual([
          { threadId: THREAD, issues: [issue(5)] },
          { threadId: THREAD, issues: [issue(5)] },
        ]);
      }),
    ).pipe(Effect.provide(services)),
  );

  it.effect("keeps stored links across a server restart", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dbPath = path.join(yield* fs.makeTempDirectoryScoped(), "chromeria-v2.sqlite");
      const persistence = SqlitePersistence.layerFromPath(dbPath);

      yield* Effect.gen(function* () {
        yield* seed;
        const links = yield* IssueLinks.IssueLinks;
        yield* links.link({ threadId: THREAD, target: { number: 12 }, source: "manual" });
        yield* links.unlink({ threadId: THREAD, issue: issue(28) });
      }).pipe(Effect.provide(servicesOver(persistence)));

      const afterRestart = yield* IssueLinks.IssueLinks.pipe(
        Effect.flatMap((links) => links.forThread(THREAD)),
        Effect.provide(servicesOver(persistence)),
      );
      expect(linksOf(afterRestart)).toEqual([{ issue: "acme/web#12", sources: ["manual"] }]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("issue MCP tools", () => {
  const invocation = (
    capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
    thread: ThreadId | null,
  ): McpInvocationContext.McpInvocationScope => ({
    environmentId: EnvironmentId.make("environment-1"),
    requestNamespace: "provider-session-1",
    thread:
      thread === null
        ? undefined
        : {
            threadId: thread,
            providerSessionId: "provider-session-1",
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
    client: undefined,
    capabilities: new Set(capabilities),
    issuedAt: 1,
  });

  const call = <Name extends keyof typeof IssuesToolkit.tools>(
    name: Name,
    params: Tool.Parameters<(typeof IssuesToolkit.tools)[Name]>,
    capabilities: ReadonlyArray<McpInvocationContext.McpCapability> = ["pull-requests"],
    /** Null for an MCP client signed in from outside a T3 thread. */
    thread: ThreadId | null = THREAD,
  ) =>
    IssuesToolkit.pipe(
      Effect.flatMap((toolkit) => toolkit.handle(name, params as never)),
      Stream.unwrap,
      Stream.runCollect,
      Effect.map(
        (chunk) => chunk.at(-1)!.result as Tool.Success<(typeof IssuesToolkit.tools)[Name]>,
      ),
      Effect.provideService(
        McpInvocationContext.McpInvocationContext,
        invocation(capabilities, thread),
      ),
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(IssuesHandlers.layer).pipe(
          Layer.provideMerge(McpToolAccessTestkit.liveThreadsLayer),
        ),
      ),
    );

  it.effect("links, lists and unlinks for the calling thread with source agent", () =>
    Effect.gen(function* () {
      yield* seed;
      expect(yield* call("link_issue", { number: 41 })).toEqual({
        ...issue(41),
        alreadyLinked: false,
      });
      expect(yield* call("link_issue", { url: "https://github.com/acme/web/issues/41" })).toEqual({
        ...issue(41),
        alreadyLinked: true,
      });
      const listed = yield* call("list_thread_issues", {});
      expect(linksOf(listed.issues)).toEqual([
        { issue: "acme/web#41", sources: ["agent"] },
        { issue: "acme/web#28", sources: ["branch"] },
      ]);
      expect(yield* call("unlink_issue", { number: 41 })).toEqual({
        ...issue(41),
        wasLinked: true,
      });
      const links = yield* IssueLinks.IssueLinks;
      expect(linksOf(yield* links.forThread(THREAD))).toEqual([
        { issue: "acme/web#28", sources: ["branch"] },
      ]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("refuses a credential without the pull-requests capability", () =>
    Effect.gen(function* () {
      yield* seed;
      const error = yield* call("link_issue", { number: 1 }, ["preview"]).pipe(Effect.flip);
      expect(error).toMatchObject({
        _tag: "McpCapabilityUnavailableError",
        capability: "pull-requests",
      });
      expect(yield* storedRows).toEqual([]);
    }).pipe(Effect.provide(services)),
  );

  it.effect("refuses a client signed in from outside a T3 thread", () =>
    Effect.gen(function* () {
      yield* seed;
      const error = yield* call("list_thread_issues", {}, ["pull-requests"], null).pipe(
        Effect.flip,
      );
      expect(error).toMatchObject({
        _tag: "OrchestratorMcpFailure",
        code: "thread_credential_required",
      });
    }).pipe(Effect.provide(services)),
  );
});

describe("combineThreadIssueLinks", () => {
  const key = (number: number) => issue(number);
  it("merges sources, keeps stored order first and drops dismissed Issues", () => {
    expect(
      combineThreadIssueLinks(
        [
          {
            key: key(3),
            url: "https://github.com/acme/web/issues/3",
            source: "agent",
            linkedAt: "2026-09-02T00:00:00.000Z",
          },
          {
            key: key(2),
            url: "https://github.com/acme/web/issues/2",
            source: "manual",
            linkedAt: "2026-09-01T00:00:00.000Z",
          },
          {
            key: key(9),
            url: "https://github.com/acme/web/issues/9",
            source: "dismissed",
            linkedAt: "2026-09-03T00:00:00.000Z",
          },
        ],
        [
          { key: key(3), source: "branch" },
          { key: { host: "GitHub.com", repository: "Acme/Web", number: 4 }, source: "branch" },
          { key: key(9), source: "branch" },
        ],
      ).map((link) => [link.number, link.sources, link.url]),
    ).toEqual([
      [2, ["manual"], "https://github.com/acme/web/issues/2"],
      [3, ["agent", "branch"], "https://github.com/acme/web/issues/3"],
      [4, ["branch"], "https://github.com/acme/web/issues/4"],
    ]);
  });
});
