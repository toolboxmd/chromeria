import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "./Sqlite.ts";
import { ensurePeerLinksSchema } from "./forkPeerLinksSchema.ts";

it.effect(
  "creates upstream's exact peer_environment_links schema idempotently without migration 061",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
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
      ]);
      assert.isEmpty(yield* sql`SELECT * FROM effect_sql_migrations WHERE migration_id = 61`);
    }).pipe(Effect.provide(Sqlite.layerMemory)),
);
