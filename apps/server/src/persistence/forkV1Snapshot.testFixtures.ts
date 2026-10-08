// @effect-diagnostics nodeBuiltinImport:off
import * as NodeSqlite from "node:sqlite";

import * as Layer from "effect/Layer";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as SqlitePersistence from "./Sqlite.ts";

/** Copy committed V1 data into a new private file, never opening the source writable. */
export async function vacuumForkV1Snapshot(input: {
  readonly sourcePath: string;
  readonly destinationPath: string;
}): Promise<void> {
  const source = new NodeSqlite.DatabaseSync(input.sourcePath, { readOnly: true });
  try {
    // SQLite refuses an existing destination. Binding avoids interpreting a path as SQL.
    source.prepare("VACUUM INTO ?").run(input.destinationPath);
  } finally {
    source.close();
  }
}

/** Run real migrations/import against the private snapshot, without a server or providers. */
export function forkV1SnapshotLayer(snapshotPath: string) {
  const database = SqlitePersistence.layerFromPath(snapshotPath);
  const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
    Layer.provideMerge(database),
  );
  const sink = EventSink.layer.pipe(Layer.provideMerge(stores));
  return LegacyV1ThreadImporter.layer.pipe(Layer.provideMerge(sink));
}
