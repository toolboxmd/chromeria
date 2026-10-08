import {
  type IssueKey,
  type IssueLinkChange,
  IssueLinkError,
  type IssueLinkedThread,
  type IssueTarget,
  ThreadLinkedPullRequest,
  type ThreadId,
  type ThreadIssueLink,
  ThreadPullRequestLink,
  type ThreadsForIssuesInput,
  type ThreadsForIssuesResult,
  gitHubRepositoryOf,
  issueKeyString,
  issueKeysEqual,
  issueNumberFromBranch,
  issueUrlFor,
  parseIssueUrl,
} from "@t3tools/contracts";
import {
  normalizeThreadPullRequestKey,
  threadPullRequestsOf,
  visibleThreadPullRequests,
} from "@t3tools/shared/threadPullRequests";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ClosingReferences from "./closingReferences.ts";
import {
  type DerivedIssueLink,
  type StoredIssueLink,
  type StoredIssueLinkSource,
  combineThreadIssueLinks,
  rollUpDescendantIssueLinks,
} from "./threadIssueLinks.ts";

export type IssueLinkWriteSource = Exclude<StoredIssueLinkSource, "dismissed">;

/**
 * Thread ↔ GitHub Issue links. Stored links live in the fork-owned `fork_thread_issue_links`
 * table, outside upstream migrations and the event log; branch and closing-reference links are
 * derived on every read. Change subscribers hear each stored change with the thread and Issues it
 * touched; clients already see branch and pull request changes on the thread and reread for those.
 */
export class IssueLinks extends Context.Service<
  IssueLinks,
  {
    /** With `includeDescendants`, also the links its child threads hold, marked with the holder. */
    readonly forThread: (
      threadId: ThreadId,
      options?: { readonly includeDescendants?: boolean | undefined },
    ) => Effect.Effect<ReadonlyArray<ThreadIssueLink>, IssueLinkError>;
    /** Threads for a page of Issues; closing pull requests are the caller's, so no GitHub read. */
    readonly threadsForIssues: (
      input: ThreadsForIssuesInput,
    ) => Effect.Effect<ThreadsForIssuesResult["issues"], IssueLinkError>;
    readonly link: (input: {
      readonly threadId: ThreadId;
      readonly target: IssueTarget;
      readonly source: IssueLinkWriteSource;
    }) => Effect.Effect<
      { readonly link: IssueKey; readonly alreadyLinked: boolean },
      IssueLinkError
    >;
    readonly unlink: (input: {
      readonly threadId: ThreadId;
      readonly issue: IssueKey;
    }) => Effect.Effect<{ readonly wasLinked: boolean }, IssueLinkError>;
    /**
     * Pull requests on `host` linked to threads that link to an Issue there (a stored link, or a
     * task branch naming one), newest link first, each once. The Issues list reads their real
     * state from GitHub, so no last-synced state filters them here.
     */
    readonly pullRequestsOfIssueThreads: (
      host: string,
    ) => Effect.Effect<
      ReadonlyArray<{ readonly repository: string; readonly number: number }>,
      IssueLinkError
    >;
    /** Resolve an agent's or user's target against the thread's project repository. */
    readonly resolveTarget: (
      threadId: ThreadId,
      target: IssueTarget,
    ) => Effect.Effect<IssueKey & { readonly url: string }, IssueLinkError>;
    /** Subscribes at once, so nothing published after this returns is missed. */
    readonly subscribeChanges: Effect.Effect<Stream.Stream<IssueLinkChange>, never, Scope.Scope>;
  }
>()("t3/issueLinks/IssueLinks") {}

interface StoredRow {
  readonly host: string;
  readonly repository: string;
  readonly number: number;
  readonly url: string;
  readonly source: StoredIssueLinkSource;
  readonly linkedAt: string;
}

/**
 * A live (not deleted) thread as Issue links read it, straight from the V2 thread projection the
 * way upstream's `pullRequest/linkedThreads.ts` reads it: one pass over the table instead of a
 * shell snapshot per read.
 */
interface ThreadRow {
  readonly threadId: string;
  readonly projectId: string;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly updatedAt: string;
  readonly branch: string | null;
  readonly parentThreadId: string | null;
  readonly relationshipToParent: string | null;
  readonly pullRequestsJson: string | null;
  readonly linkedPullRequestJson: string | null;
}

const decodePullRequests = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Array(ThreadPullRequestLink)),
);
const decodeLinkedPullRequest = Schema.decodeUnknownOption(
  Schema.fromJsonString(ThreadLinkedPullRequest),
);

/** A thread's pull request keys, normalized and without dismissed stack members. */
function pullRequestKeysOf(
  row: ThreadRow,
): ReadonlyArray<IssueKey & { readonly linkedAt: string }> {
  const links = threadPullRequestsOf({
    ...(row.pullRequestsJson === null
      ? {}
      : { pullRequests: Option.getOrElse(decodePullRequests(row.pullRequestsJson), () => []) }),
    linkedPullRequest:
      row.linkedPullRequestJson === null
        ? null
        : Option.getOrNull(decodeLinkedPullRequest(row.linkedPullRequestJson)),
  });
  return visibleThreadPullRequests(links).map((link) => ({
    ...normalizeThreadPullRequestKey(link),
    linkedAt: link.linkedAt,
  }));
}

const isIssueLinkError = Schema.is(IssueLinkError);

/** Reports storage and projection failures as one `IssueLinkError`, keeping ones already typed. */
const failWith = (detail: string) =>
  Effect.catch((cause: unknown) =>
    isIssueLinkError(cause)
      ? Effect.fail(cause)
      : Effect.logWarning(detail, cause).pipe(
          Effect.andThen(Effect.fail(new IssueLinkError({ detail }))),
        ),
  );

const PRUNE_INTERVAL_MS = 10 * 60_000;
const STALE_LINK_TTL_MS = 7 * 24 * 60 * 60_000;

const normalizeIssueKey = (key: IssueKey): IssueKey => ({
  host: key.host.toLowerCase(),
  repository: key.repository.toLowerCase(),
  number: key.number,
});

function groupBy<A>(items: ReadonlyArray<A>, keyOf: (item: A) => string): Map<string, A[]> {
  const groups = new Map<string, A[]>();
  for (const item of items) {
    const key = keyOf(item);
    groups.set(key, [...(groups.get(key) ?? []), item]);
  }
  return groups;
}

const storedOf = (row: StoredRow): StoredIssueLink => ({
  key: { host: row.host, repository: row.repository, number: row.number },
  url: row.url,
  source: row.source,
  linkedAt: row.linkedAt,
});

/**
 * Threads `threadId` spawned, directly or through other spawned threads, in spawn order. Only
 * unbroken subagent links count: a conversation fork and anything below it are not the thread's
 * work. This is the V1 fork's rule, which rolled up only `sub.<parent>.<suffix>` threads the
 * threads toolkit spawned (`isDescendantThreadId`); V1 had no conversation forks.
 */
function descendantRowsOf(rows: ReadonlyArray<ThreadRow>, threadId: string): Array<ThreadRow> {
  const children = groupBy(
    rows.filter((row) => row.relationshipToParent === "subagent" && row.parentThreadId !== null),
    (row) => row.parentThreadId!,
  );
  const descendants: ThreadRow[] = [];
  const seen = new Set([threadId]);
  const queue = [threadId];
  while (queue.length > 0) {
    for (const child of children.get(queue.shift()!) ?? []) {
      if (seen.has(child.threadId)) continue;
      seen.add(child.threadId);
      descendants.push(child);
      queue.push(child.threadId);
    }
  }
  // Rows arrive in creation order, so this keeps spawn order across depths.
  const order = new Map(rows.map((row, index) => [row.threadId, index]));
  return descendants.toSorted(
    (left, right) => order.get(left.threadId)! - order.get(right.threadId)!,
  );
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const closing = yield* ClosingReferences.IssueClosingReferences;
  const pubsub = yield* PubSub.unbounded<IssueLinkChange>();

  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_thread_issue_links (
      thread_id TEXT NOT NULL,
      host TEXT NOT NULL,
      repository TEXT NOT NULL,
      number INTEGER NOT NULL,
      url TEXT NOT NULL,
      source TEXT NOT NULL,
      linked_at TEXT NOT NULL,
      PRIMARY KEY (thread_id, host, repository, number)
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_fork_thread_issue_links_issue
    ON fork_thread_issue_links(host, repository, number)
  `;

  const storedForThread = (threadId: ThreadId) =>
    sql<StoredRow>`
      SELECT host, repository, number, url, source, linked_at AS "linkedAt"
      FROM fork_thread_issue_links
      WHERE thread_id = ${threadId}
    `.pipe(Effect.map((rows) => rows.map(storedOf)));

  /** Every live thread, archived ones included, oldest first. */
  const liveThreads = sql<ThreadRow>`
    SELECT thread_id AS "threadId", project_id AS "projectId", title,
      archived_at AS "archivedAt", updated_at AS "updatedAt",
      json_extract(payload_json, '$.branch') AS branch,
      json_extract(payload_json, '$.lineage.parentThreadId') AS "parentThreadId",
      json_extract(payload_json, '$.lineage.relationshipToParent') AS "relationshipToParent",
      json_extract(payload_json, '$.pullRequests') AS "pullRequestsJson",
      json_extract(payload_json, '$.linkedPullRequest') AS "linkedPullRequestJson"
    FROM orchestration_v2_projection_threads
    WHERE deleted_at IS NULL
    ORDER BY created_at, thread_id
  `;

  /** Each active project's GitHub repository (a fork checkout's own), by project id. */
  const projectRepositories = projects
    .listShells()
    .pipe(
      Effect.map(
        (shells) =>
          new Map(
            shells.map((project) => [
              project.id as string,
              gitHubRepositoryOf(project.repositoryIdentity),
            ]),
          ),
      ),
    );

  const projectContext = Effect.fn("IssueLinks.projectContext")(function* (threadId: ThreadId) {
    const thread = yield* orchestrator.getThreadShell(threadId);
    if (thread === null) return null;
    const project = Option.getOrNull(yield* projects.getShell(thread.projectId));
    return {
      thread,
      project,
      repository: gitHubRepositoryOf(project?.repositoryIdentity),
    };
  });

  /**
   * Stored and branch links of the thread's descendants, in spawn order. Closing references are
   * left out: they need a GitHub read per pull request, and the panel lists those pull requests.
   */
  const descendantLinks = Effect.fn("IssueLinks.descendantLinks")(function* (threadId: ThreadId) {
    const descendants = descendantRowsOf(yield* liveThreads, threadId);
    if (descendants.length === 0) return [];
    const storedRows = yield* sql<StoredRow & { readonly threadId: string }>`
      SELECT thread_id AS "threadId", host, repository, number, url, source,
        linked_at AS "linkedAt"
      FROM fork_thread_issue_links
      WHERE ${sql.in(
        "thread_id",
        descendants.map((row) => row.threadId),
      )}
    `;
    const storedByThread = groupBy(storedRows, (row) => row.threadId);
    const repositories = yield* projectRepositories;
    return descendants.map((row) => {
      const repository = repositories.get(row.projectId) ?? null;
      const branchIssue = issueNumberFromBranch(row.branch);
      const derived: DerivedIssueLink[] =
        repository !== null && branchIssue !== null
          ? [{ key: { ...repository, number: branchIssue }, source: "branch" }]
          : [];
      const stored = (storedByThread.get(row.threadId) ?? []).map(storedOf);
      return {
        threadId: row.threadId as ThreadId,
        links: combineThreadIssueLinks(stored, derived),
      };
    });
  });

  const forThread = Effect.fn("IssueLinks.forThread")(function* (
    threadId: ThreadId,
    options?: { readonly includeDescendants?: boolean | undefined },
  ) {
    const context = yield* projectContext(threadId);
    if (context === null) {
      return yield* new IssueLinkError({ detail: `Thread ${threadId} was not found.` });
    }
    const stored = yield* storedForThread(threadId);
    const derived: DerivedIssueLink[] = [];
    const { repository, thread } = context;
    const branchIssue = issueNumberFromBranch(thread.branch);
    if (repository !== null && branchIssue !== null) {
      derived.push({ key: { ...repository, number: branchIssue }, source: "branch" });
    }
    if (repository !== null) {
      const pullRequests = visibleThreadPullRequests(threadPullRequestsOf(thread)).filter(
        (link) => normalizeThreadPullRequestKey(link).host === repository.host,
      );
      const closed = yield* Effect.forEach(
        pullRequests,
        (link) =>
          closing.issuesClosedBy({
            pullRequest: normalizeThreadPullRequestKey(link),
            version: link.snapshot?.updatedAt ?? null,
          }),
        { concurrency: 4 },
      );
      for (const key of closed.flat()) derived.push({ key, source: "closing-reference" });
    }
    const own = combineThreadIssueLinks(stored, derived);
    if (options?.includeDescendants !== true) return own;
    return rollUpDescendantIssueLinks(
      own,
      stored.filter((link) => link.source === "dismissed").map((link) => link.key),
      yield* descendantLinks(threadId),
    );
  }, failWith("Could not read the thread's Issue links."));

  // Pruned on read, at most every ten minutes: rows of threads deleted over a week ago, and of
  // drafts never sent within a week. Recent deletions stay: a first send whose bootstrap fails
  // deletes its thread, and the retry creates the same id again.
  const lastPruneAt = yield* Ref.make(0);
  const pruneIfDue = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const due = yield* Ref.modify(lastPruneAt, (last) =>
      now - last >= PRUNE_INTERVAL_MS ? [true, now] : [false, last],
    );
    if (!due) return;
    const cutoff = DateTime.formatIso(DateTime.makeUnsafe(now - STALE_LINK_TTL_MS));
    yield* sql`
      DELETE FROM fork_thread_issue_links
      WHERE thread_id IN (
          SELECT thread_id FROM orchestration_v2_projection_threads
          WHERE deleted_at IS NOT NULL AND deleted_at < ${cutoff}
        )
        OR (linked_at < ${cutoff}
          AND thread_id NOT IN (SELECT thread_id FROM orchestration_v2_projection_threads))
    `;
  });

  const threadsForIssues = Effect.fn("IssueLinks.threadsForIssues")(function* (
    input: ThreadsForIssuesInput,
  ) {
    yield* pruneIfDue;
    const issues = input.issues.map((issue) => ({
      key: normalizeIssueKey(issue),
      closingPullRequests: issue.closingPullRequests.map((pullRequest) => ({
        repository: pullRequest.repository.toLowerCase(),
        number: pullRequest.number,
      })),
    }));
    type Linked = { row: ThreadRow; stored: StoredIssueLink[]; derived: DerivedIssueLink[] };
    const byIssue = new Map<string, Map<string, Linked>>(
      issues.map(({ key }) => [issueKeyString(key), new Map()]),
    );
    const entry = (issue: IssueKey, row: ThreadRow) => {
      const threads = byIssue.get(issueKeyString(issue));
      if (threads === undefined) return null;
      const existing = threads.get(row.threadId);
      if (existing !== undefined) return existing;
      const created: Linked = { row, stored: [], derived: [] };
      threads.set(row.threadId, created);
      return created;
    };

    const threads = yield* liveThreads;
    const threadsById = new Map(threads.map((row) => [row.threadId, row]));

    const repositories = groupBy(issues, ({ key }) => `${key.host}\0${key.repository}`);
    for (const group of repositories.values()) {
      const { host, repository } = group[0]!.key;
      const numbers = group.map(({ key }) => key.number);
      const storedRows = yield* sql<StoredRow & { readonly threadId: string }>`
        SELECT thread_id AS "threadId", host, repository, number, url, source,
          linked_at AS "linkedAt"
        FROM fork_thread_issue_links
        WHERE host = ${host} AND repository = ${repository} AND ${sql.in("number", numbers)}
      `;
      for (const stored of storedRows) {
        const row = threadsById.get(stored.threadId);
        if (row === undefined) continue;
        entry({ host, repository, number: stored.number }, row)?.stored.push(storedOf(stored));
      }
    }

    const repositoryProjects = new Map<string, { host: string; repository: string }>();
    for (const [projectId, repository] of yield* projectRepositories) {
      if (repository !== null && repositories.has(`${repository.host}\0${repository.repository}`)) {
        repositoryProjects.set(projectId, repository);
      }
    }
    const pullRequestsByThread = new Map<string, ReadonlyArray<IssueKey>>();
    const closingByPullRequest = groupBy(
      issues.flatMap(({ key, closingPullRequests }) =>
        closingPullRequests.map((pullRequest) => ({ issue: key, host: key.host, ...pullRequest })),
      ),
      (pullRequest) => `${pullRequest.host}/${pullRequest.repository}#${pullRequest.number}`,
    );
    for (const row of threads) {
      const project = repositoryProjects.get(row.projectId);
      const number = project === undefined ? null : issueNumberFromBranch(row.branch);
      if (project !== undefined && number !== null) {
        const key = { host: project.host, repository: project.repository, number };
        entry(key, row)?.derived.push({ key, source: "branch" });
      }
      const pullRequests = pullRequestKeysOf(row);
      pullRequestsByThread.set(row.threadId, pullRequests);
      for (const pullRequest of pullRequests) {
        const closed = closingByPullRequest.get(
          `${pullRequest.host}/${pullRequest.repository}#${pullRequest.number}`,
        );
        for (const { issue } of closed ?? []) {
          entry(issue, row)?.derived.push({ key: issue, source: "closing-reference" });
        }
      }
    }

    return issues.map(({ key }) => ({
      ...key,
      threads: [...(byIssue.get(issueKeyString(key))?.values() ?? [])]
        .toSorted(
          (left, right) =>
            right.row.updatedAt.localeCompare(left.row.updatedAt) ||
            left.row.threadId.localeCompare(right.row.threadId),
        )
        .flatMap(({ row, stored, derived }): IssueLinkedThread[] => {
          const [link] = combineThreadIssueLinks(stored, derived);
          // Each linked thread's own pull requests: a component PR into a non-default branch makes
          // no closing reference, so the Issue's status reaches it only through the thread.
          return link === undefined
            ? []
            : [
                {
                  id: row.threadId as IssueLinkedThread["id"],
                  projectId: row.projectId as IssueLinkedThread["projectId"],
                  title: row.title,
                  archivedAt: row.archivedAt,
                  sources: link.sources,
                  pullRequests: (pullRequestsByThread.get(row.threadId) ?? []).map(
                    ({ host, repository, number }) => ({ host, repository, number }),
                  ),
                },
              ];
        }),
    }));
  }, failWith("Could not read the Issues' linked threads."));

  const pullRequestsOfIssueThreads = Effect.fn("IssueLinks.pullRequestsOfIssueThreads")(function* (
    host: string,
  ) {
    const normalizedHost = host.toLowerCase();
    const storedThreads = new Set(
      (yield* sql<{ readonly threadId: string }>`
        SELECT DISTINCT thread_id AS "threadId"
        FROM fork_thread_issue_links
        WHERE host = ${normalizedHost} AND source != 'dismissed'
      `).map((row) => row.threadId),
    );
    const candidates = (yield* liveThreads).flatMap((row) =>
      storedThreads.has(row.threadId) || issueNumberFromBranch(row.branch) !== null
        ? pullRequestKeysOf(row).filter((pullRequest) => pullRequest.host === normalizedHost)
        : [],
    );
    const seen = new Set<string>();
    return candidates
      .toSorted(
        (left, right) =>
          right.linkedAt.localeCompare(left.linkedAt) ||
          left.repository.localeCompare(right.repository) ||
          left.number - right.number,
      )
      .flatMap(({ repository, number }) => {
        const key = `${repository}#${number}`;
        if (seen.has(key)) return [];
        seen.add(key);
        return [{ repository, number }];
      });
  }, failWith("Could not read the pull requests of Issue threads."));

  const resolveTarget = Effect.fn("IssueLinks.resolveTarget")(function* (
    threadId: ThreadId,
    target: IssueTarget,
  ) {
    if (target.url !== undefined) {
      const key = parseIssueUrl(target.url);
      if (key === null) {
        return yield* new IssueLinkError({
          detail: "This is not a GitHub Issue URL. Pass repository and number instead.",
        });
      }
      return { ...key, url: issueUrlFor(key) };
    }
    if (target.number === undefined) {
      return yield* new IssueLinkError({ detail: "Pass either url, or number." });
    }
    const context = yield* projectContext(threadId);
    const repository = target.repository?.toLowerCase() ?? context?.repository?.repository;
    const host = target.host?.toLowerCase() ?? context?.repository?.host ?? "github.com";
    if (repository === undefined || !repository.includes("/")) {
      return yield* new IssueLinkError({
        detail: "This thread's project has no GitHub repository. Pass repository or url.",
      });
    }
    const key = { host, repository, number: target.number };
    return { ...key, url: issueUrlFor(key) };
  }, failWith("Could not read the thread's project."));

  const notify = (threadId: ThreadId, issues: ReadonlyArray<IssueKey>) =>
    PubSub.publish(pubsub, { threadId, issues }).pipe(Effect.asVoid);

  /** The thread's visible links; a thread not created yet (a draft) has only stored ones. */
  const currentLinks = (threadId: ThreadId) =>
    forThread(threadId).pipe(
      Effect.catchTags({
        IssueLinkError: () =>
          storedForThread(threadId).pipe(
            Effect.map((stored) => combineThreadIssueLinks(stored, [])),
          ),
      }),
    );

  const link = Effect.fn("IssueLinks.link")(function* (input: {
    readonly threadId: ThreadId;
    readonly target: IssueTarget;
    readonly source: IssueLinkWriteSource;
  }) {
    const target = yield* resolveTarget(input.threadId, input.target);
    const { url, ...key } = target;
    const touched: IssueKey[] = [];
    if (input.source === "started") {
      // A draft starts from one Issue. A reused draft drops the Issue it was started from before.
      const previous = yield* sql<StoredRow>`
        DELETE FROM fork_thread_issue_links
        WHERE thread_id = ${input.threadId} AND source = 'started'
          AND NOT (host = ${key.host} AND repository = ${key.repository} AND number = ${key.number})
        RETURNING host, repository, number, url, source, linked_at AS "linkedAt"
      `;
      touched.push(...previous.map((row) => storedOf(row).key));
    }
    const alreadyLinked = (yield* currentLinks(input.threadId)).some((current) =>
      issueKeysEqual(current, key),
    );
    if (!alreadyLinked) {
      const linkedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`
        INSERT OR REPLACE INTO fork_thread_issue_links
          (thread_id, host, repository, number, url, source, linked_at)
        VALUES (${input.threadId}, ${key.host}, ${key.repository}, ${key.number},
          ${url}, ${input.source}, ${linkedAt})
      `;
      touched.push(key);
    }
    if (touched.length > 0) yield* notify(input.threadId, touched);
    return { link: key, alreadyLinked };
  }, failWith("Could not link the Issue."));

  const unlink = Effect.fn("IssueLinks.unlink")(function* (input: {
    readonly threadId: ThreadId;
    readonly issue: IssueKey;
  }) {
    const issue = normalizeIssueKey(input.issue);
    const wasLinked = (yield* currentLinks(input.threadId)).some((current) =>
      issueKeysEqual(current, issue),
    );
    // Always a tombstone, not a delete, and written even when nothing looked linked: a branch or
    // closing reference (possibly unreadable right now) would otherwise put the link back.
    const linkedAt = DateTime.formatIso(yield* DateTime.now);
    yield* sql`
      INSERT OR REPLACE INTO fork_thread_issue_links
        (thread_id, host, repository, number, url, source, linked_at)
      VALUES (${input.threadId}, ${issue.host}, ${issue.repository}, ${issue.number},
        ${issueUrlFor(issue)}, 'dismissed', ${linkedAt})
    `;
    yield* notify(input.threadId, [issue]);
    return { wasLinked };
  }, failWith("Could not unlink the Issue."));

  return IssueLinks.of({
    forThread,
    threadsForIssues,
    link,
    unlink,
    pullRequestsOfIssueThreads,
    resolveTarget,
    subscribeChanges: PubSub.subscribe(pubsub).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const layer = Layer.effect(IssueLinks, make);

export const layerLive = layer.pipe(Layer.provide(ClosingReferences.layer));
