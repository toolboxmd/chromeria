// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId, issueKeyString } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { runForkV1Backfills } from "../persistence/forkV1Backfills.ts";
import {
  forkV1SnapshotLayer,
  vacuumForkV1Snapshot,
} from "../persistence/forkV1Snapshot.testFixtures.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as ClosingReferences from "./closingReferences.ts";
import * as IssueLinks from "./IssueLinks.ts";
import { gitHubIdentity, readLayer } from "./IssueLinks.testFixtures.ts";

/**
 * Stored Issue links outlive the V1 to V2 move unchanged: the V2 database starts as a copy of
 * V1's, `fork_thread_issue_links` rides along, and imported threads keep their ids. These read
 * the copied rows through the real importer and the V2 reads `IssueLinks` serves from.
 */
const issueLinksOver = (snapshotPath: string, identities: Parameters<typeof readLayer>[0]) =>
  IssueLinks.layer.pipe(
    // Closing references need GitHub; carryover is about the stored rows.
    Layer.provide(
      Layer.succeed(ClosingReferences.IssueClosingReferences, {
        issuesClosedBy: () => Effect.succeed([]),
      }),
    ),
    Layer.provideMerge(readLayer(identities)),
    Layer.provideMerge(ProjectStore.layer),
    Layer.provideMerge(forkV1SnapshotLayer(snapshotPath)),
  );

const AT = "2026-09-01T00:00:00.000Z";

/** Every persisted field of the stored links of live V2 threads, in a stable order. */
const storedLiveRows = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) => sql`
    SELECT thread_id AS "threadId", host, repository, number, url, source, linked_at AS "linkedAt"
    FROM fork_thread_issue_links
    WHERE thread_id IN (
      SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL
    )
    ORDER BY 1, 2, 3, 4
  `,
);

/**
 * Exactly equal, without printing either side: real rows name private repositories, so a mismatch
 * reports only the counts and digests.
 */
const assertSame = (label: string, actual: unknown, expected: unknown) => {
  const digest = (value: unknown) =>
    NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 12);
  const size = (value: unknown) => (Array.isArray(value) ? value.length : -1);
  assert.isTrue(
    NodeUtil.isDeepStrictEqual(actual, expected),
    `${label} differ: ${size(actual)} vs ${size(expected)} entries, ${digest(actual)} vs ${digest(expected)}`,
  );
};
const LONG_AGO = "2026-08-01T00:00:00.000Z";

it.effect("carries stored Issue links through the V1 import into V2 reads", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-09-10T00:00:00.000Z"));
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-issue-links-"));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
    );
    const sourcePath = NodePath.join(directory, "state.sqlite");
    const snapshotPath = NodePath.join(directory, "chromeria-v2.sqlite");

    // A V1 database as a Chromeria build before the port left it.
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 52 });
      yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES ('project', 'Project', '/work/acme-web', '[]', ${AT}, ${AT})`;
      const thread = (id: string, branch: string | null, deletedAt: string | null = null) =>
        sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, branch, created_at, updated_at, deleted_at)
          VALUES (${id}, 'project', ${id}, '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default', ${branch}, ${AT}, ${AT}, ${deletedAt})`;
      yield* thread("thread-linked", "feat/28-task");
      yield* thread("thread-closer", null);
      yield* thread("thread-gone", null, LONG_AGO);
      yield* sql`INSERT INTO projection_thread_pull_requests (thread_id, host, repository, number, url, source, linked_at)
        VALUES ('thread-closer', 'github.com', 'acme/web', 30, 'https://github.com/acme/web/pull/30', 'manual', ${AT})`;
      // The table exactly as the V1 fork created it.
      yield* sql`CREATE TABLE fork_thread_issue_links (
        thread_id TEXT NOT NULL, host TEXT NOT NULL, repository TEXT NOT NULL,
        number INTEGER NOT NULL, url TEXT NOT NULL, source TEXT NOT NULL, linked_at TEXT NOT NULL,
        PRIMARY KEY (thread_id, host, repository, number))`;
      const link = (threadId: string, number: number, source: string, linkedAt = AT) =>
        sql`INSERT INTO fork_thread_issue_links VALUES (${threadId}, 'github.com', 'acme/web', ${number},
          ${`https://github.com/acme/web/issues/${number}`}, ${source}, ${linkedAt})`;
      yield* link("thread-linked", 12, "agent");
      // The user removed the branch's own link; the tombstone must keep it removed.
      yield* link("thread-linked", 28, "dismissed");
      yield* link("thread-gone", 13, "manual", LONG_AGO);
      yield* link("draft-never-sent", 14, "started", LONG_AGO);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: sourcePath })));
    const original = NodeFS.readFileSync(sourcePath);
    yield* Effect.promise(() =>
      vacuumForkV1Snapshot({ sourcePath, destinationPath: snapshotPath }),
    );

    yield* Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const sql = yield* SqlClient.SqlClient;
      assert.equal((yield* importer.reconcileShells).importedThreadCount, 3);
      // Unchanged rows need no backfill; startup still runs the hook after the import.
      yield* runForkV1Backfills();

      const links = yield* IssueLinks.IssueLinks;
      const read = Effect.gen(function* () {
        const linked = yield* links.forThread(ThreadId.make("thread-linked"));
        const threads = yield* links.threadsForIssues({
          issues: [12, 28, 11].map((number) => ({
            host: "github.com",
            repository: "acme/web",
            number,
            closingPullRequests: number === 11 ? [{ repository: "acme/web", number: 30 }] : [],
          })),
        });
        return {
          linked: linked.map((entry): [string, ReadonlyArray<string>] => [
            issueKeyString(entry),
            entry.sources,
          ]),
          threads: threads.map((entry) =>
            entry.threads.map((thread): [string, ReadonlyArray<string>] => [
              thread.id,
              thread.sources,
            ]),
          ),
          stored: yield* sql`
            SELECT thread_id AS "threadId", host, repository, number, url, source,
              linked_at AS "linkedAt"
            FROM fork_thread_issue_links ORDER BY 1, 2, 3, 4`,
        };
      });
      const first = yield* read;
      assert.deepEqual(first, {
        linked: [["github.com/acme/web#12", ["agent"]]],
        threads: [[["thread-linked", ["agent"]]], [], [["thread-closer", ["closing-reference"]]]],
        // The read pruned the week-old deletion's and the abandoned draft's rows, nothing else.
        stored: [
          {
            threadId: "thread-linked",
            host: "github.com",
            repository: "acme/web",
            number: 12,
            url: "https://github.com/acme/web/issues/12",
            source: "agent",
            linkedAt: AT,
          },
          {
            threadId: "thread-linked",
            host: "github.com",
            repository: "acme/web",
            number: 28,
            url: "https://github.com/acme/web/issues/28",
            source: "dismissed",
            linkedAt: AT,
          },
        ],
      });
      // Every startup imports and runs the hook again; the links stay exactly as they were.
      assert.equal((yield* importer.reconcileShells).importedThreadCount, 0);
      yield* runForkV1Backfills();
      assert.deepEqual(yield* read, first);
    }).pipe(
      Effect.provide(
        issueLinksOver(snapshotPath, { "/work/acme-web": gitHubIdentity("acme/web") }),
      ),
    );
    assert.deepEqual(NodeFS.readFileSync(sourcePath), original);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const realSource = process.env.CHROMERIA_V1_SNAPSHOT_SOURCE;
(realSource ? it.effect : it.effect.skip)(
  "reads every stored Issue link of a read-only VACUUM INTO snapshot of real V1 data",
  () =>
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "chromeria-real-issue-links-"),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const snapshotPath = NodePath.join(directory, "chromeria-v2.sqlite");
      yield* Effect.promise(() =>
        vacuumForkV1Snapshot({ sourcePath: realSource!, destinationPath: snapshotPath }),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        // The V1 rows of live threads, every persisted field, before anything V2 runs.
        const before = yield* sql<{
          readonly threadId: string;
          readonly host: string;
          readonly repository: string;
          readonly number: number;
          readonly url: string;
          readonly source: string;
          readonly linkedAt: string;
        }>`SELECT link.thread_id AS "threadId", link.host, link.repository, link.number, link.url,
            link.source, link.linked_at AS "linkedAt"
          FROM fork_thread_issue_links AS link
          JOIN projection_threads AS thread ON thread.thread_id = link.thread_id
          WHERE thread.deleted_at IS NULL
          ORDER BY 1, 2, 3, 4`;
        assert.isAbove(before.length, 0);
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();

        const links = yield* IssueLinks.IssueLinks;
        const stored = (source: string) =>
          before
            .filter((row) => row.source === source)
            .map((row) => `${row.threadId} ${issueKeyString(row)}`)
            .toSorted();
        const read = Effect.gen(function* () {
          const visible: string[] = [];
          for (const threadId of new Set(before.map((row) => row.threadId))) {
            for (const link of yield* links.forThread(ThreadId.make(threadId))) {
              if (
                link.sources.some((source) => source !== "branch" && source !== "closing-reference")
              )
                visible.push(`${threadId} ${issueKeyString(link)}`);
            }
          }
          const found: string[] = [];
          for (const row of before.filter((entry) => entry.source !== "dismissed")) {
            const [answer] = yield* links.threadsForIssues({
              issues: [{ ...row, closingPullRequests: [] }],
            });
            if (answer!.threads.some((thread) => thread.id === row.threadId)) {
              found.push(`${row.threadId} ${issueKeyString(row)}`);
            }
          }
          return {
            visible: visible.toSorted(),
            found: found.toSorted(),
            stored: yield* storedLiveRows,
          };
        });
        const first = yield* read;
        const shown = [...stored("manual"), ...stored("agent"), ...stored("started")].toSorted();
        // Every stored link of a live thread shows on it and its Issue finds the thread; no
        // dismissed one shows.
        assertSame("visible links", first.visible, shown);
        assertSame("threads found by Issue", first.found, shown);
        // The live threads' rows are exactly V1's, every field, after the prune those reads ran.
        assertSame("stored rows", first.stored, before);
        // Every startup imports and runs the hook again; the links stay exactly as they were.
        yield* importer.reconcileShells;
        yield* runForkV1Backfills();
        const repeated = yield* read;
        assertSame("repeated visible links", repeated.visible, first.visible);
        assertSame("repeated threads found by Issue", repeated.found, first.found);
        assertSame("repeated stored rows", repeated.stored, first.stored);
      }).pipe(Effect.provide(issueLinksOver(snapshotPath, {})));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
