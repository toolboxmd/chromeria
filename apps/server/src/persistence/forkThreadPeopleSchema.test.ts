import { AuthSessionId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Migrator from "effect/unstable/sql/Migrator";

import * as AuthSessions from "./AuthSessions.ts";
import { ensureThreadPeopleSchema } from "./forkThreadPeopleSchema.ts";
import { ProjectionThreadRepositoryLive } from "./Layers/ProjectionThreads.ts";
import { SqlitePersistenceMemory } from "./Layers/Sqlite.ts";
import { migrationManifest, runMigrations } from "./Migrations.ts";
import { ProjectionThreadRepository } from "./Services/ProjectionThreads.ts";

const NOW = "2026-01-01T00:00:00.000Z";
const repositories = Layer.merge(AuthSessions.layer, ProjectionThreadRepositoryLive).pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
);

it.effect("adds each people column once and preserves legacy rows and later labels on rerun", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at) VALUES ('legacy-thread', 'project', 'Legacy', '{"instanceId":"codex","model":"gpt-5"}', 'full-access', ${NOW}, ${NOW})`;
    yield* sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at) VALUES ('legacy-session', 'device', '[]', 'bearer-access-token', ${NOW}, '2027-01-01T00:00:00.000Z')`;
    yield* ensureThreadPeopleSchema;
    assert.deepEqual(yield* sql`SELECT title, owner, co_owners_json FROM projection_threads`, [
      { title: "Legacy", owner: null, co_owners_json: "[]" },
    ]);
    assert.deepEqual(yield* sql`SELECT subject, person FROM auth_sessions`, [
      { subject: "device", person: null },
    ]);
    yield* sql`UPDATE auth_sessions SET person = 'Pauli'`;
    yield* sql`UPDATE projection_threads SET owner = 'Pauli', co_owners_json = '["Luke"]'`;
    yield* ensureThreadPeopleSchema;
    const sessions = yield* sql<{ readonly name: string }>`PRAGMA table_info(auth_sessions)`;
    const threads = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
    assert.lengthOf(
      sessions.filter((column) => column.name === "person"),
      1,
    );
    for (const name of ["owner", "co_owners_json"]) {
      assert.lengthOf(
        threads.filter((column) => column.name === name),
        1,
      );
    }
    assert.deepEqual(yield* sql`SELECT title, owner, co_owners_json FROM projection_threads`, [
      { title: "Legacy", owner: "Pauli", co_owners_json: '["Luke"]' },
    ]);
    assert.deepEqual(yield* sql`SELECT subject, person FROM auth_sessions`, [
      { subject: "device", person: "Pauli" },
    ]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("SQLite setup makes people fields usable by auth and projection repositories", () =>
  Effect.gen(function* () {
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const threads = yield* ProjectionThreadRepository;
    const sessionId = AuthSessionId.make("session");
    yield* sessions.create({
      sessionId,
      subject: "device",
      scopes: ["access:read"],
      method: "browser-session-cookie",
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "desktop",
        os: null,
        browser: null,
      },
      issuedAt: DateTime.makeUnsafe(NOW),
      expiresAt: DateTime.makeUnsafe("2027-01-01T00:00:00.000Z"),
    });
    yield* sessions.setPerson({ sessionId, person: "Pauli" });
    assert.equal(Option.getOrThrow(yield* sessions.getById({ sessionId })).person, "Pauli");

    const threadId = ThreadId.make("thread");
    yield* threads.upsert({
      threadId,
      projectId: ProjectId.make("project"),
      title: "Owned thread",
      owner: "Pauli",
      coOwners: ["Luke"],
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      latestTurnId: null,
      createdAt: NOW,
      updatedAt: NOW,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      unsettledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      latestUserMessageAt: null,
      pendingApprovalCount: 0,
      pendingUserInputCount: 0,
      hasActionableProposedPlan: 0,
      deletedAt: null,
    });
    const thread = Option.getOrThrow(yield* threads.getById({ threadId }));
    assert.equal(thread.owner, "Pauli");
    assert.deepEqual(thread.coOwners, ["Luke"]);
  }).pipe(Effect.provide(repositories)),
);

it.effect("leaves migration IDs to upstream and executes a future upstream migration 55", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly id: number; readonly name: string }>`
      SELECT migration_id AS id, name FROM effect_sql_migrations ORDER BY migration_id
    `;
    assert.deepEqual(
      rows.map(({ id, name }) => [id, name]),
      migrationManifest.map(([id, name]) => [id, name]),
    );
    assert.isFalse(rows.some(({ name }) => name === "ThreadPeople"));
    assert.equal(
      Math.max(...rows.map(({ id }) => id)),
      Math.max(...migrationManifest.map(([id]) => id)),
    );

    // The recorded upstream entries are already applied; only the new entry can run.
    const future = Effect.gen(function* () {
      yield* sql`CREATE TABLE future_upstream_marker (value TEXT NOT NULL)`;
      yield* sql`INSERT INTO future_upstream_marker (value) VALUES ('executed')`;
    });
    const executed = yield* Migrator.make({})({
      loader: Migrator.fromRecord({
        ...Object.fromEntries(
          migrationManifest.map(([id, name]) => [`${id}_${name}`, Effect.void]),
        ),
        "55_FutureUpstream": future,
      }),
    });
    assert.deepEqual(executed, [[55, "FutureUpstream"]]);
    assert.deepEqual(yield* sql`SELECT value FROM future_upstream_marker`, [{ value: "executed" }]);
    assert.deepEqual(yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 55`, [
      { name: "FutureUpstream" },
    ]);
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);
