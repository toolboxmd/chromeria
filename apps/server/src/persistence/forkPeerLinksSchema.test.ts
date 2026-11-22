import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "./Sqlite.ts";
import { ensurePeerLinksSchema } from "./forkPeerLinksSchema.ts";

it.effect(
  "preserves upstream's exact migrated peer_environment_links schema and data without a fork migration",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const migrations = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id IN (70, 71) ORDER BY migration_id`,
        [
          { migration_id: 70, name: "PeerLinks" },
          { migration_id: 71, name: "PeerLinkFence" },
        ],
      );
      const schema = yield* sql`PRAGMA table_info(peer_environment_links)`;
      yield* sql`
        INSERT INTO peer_environment_links
          (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
        VALUES ('peer', 'peer-token', 'Peer', '[]', 'read', '2026-10-29', '2026-10-30')
      `;
      assert.deepEqual(yield* sql`SELECT fenced FROM peer_environment_links`, [{ fenced: 0 }]);
      yield* sql`UPDATE peer_environment_links SET fenced = 1 WHERE environment_id = 'peer'`;
      const links = yield* sql`SELECT * FROM peer_environment_links`;
      yield* ensurePeerLinksSchema;
      yield* ensurePeerLinksSchema;
      assert.deepEqual(yield* sql`PRAGMA table_info(peer_environment_links)`, [
        { cid: 0, name: "environment_id", type: "TEXT", notnull: 0, dflt_value: null, pk: 1 },
        ...["secret_name", "label", "urls_json", "access", "linked_at", "expires_at"].map(
          (name, index) => ({
            cid: index + 1,
            name,
            type: "TEXT",
            notnull: 1,
            dflt_value: null,
            pk: 0,
          }),
        ),
        { cid: 7, name: "last_reached_at", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
        { cid: 8, name: "last_error", type: "TEXT", notnull: 0, dflt_value: null, pk: 0 },
        { cid: 9, name: "fenced", type: "INTEGER", notnull: 1, dflt_value: "0", pk: 0 },
      ]);
      assert.deepEqual(yield* sql`PRAGMA table_info(peer_environment_links)`, schema);
      assert.deepEqual(yield* sql`SELECT * FROM peer_environment_links`, links);
      assert.isEmpty(
        yield* sql`SELECT * FROM effect_sql_migrations WHERE name = 'PeerEnvironmentLinks'`,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        migrations,
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
);
