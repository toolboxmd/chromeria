import type { ServerProvider } from "@t3tools/contracts";
import { unsupportedBare } from "@t3tools/provider-core/server/bareLaunch";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import type { ProviderSnapshotSource } from "./builtInProviderCatalog.ts";

/** Clients offer a bare launch only where this instance's adapter would accept one. */
export function withBareSupport(
  instance: ProviderInstance,
  source: ProviderSnapshotSource,
): ProviderSnapshotSource {
  const stamp = (snapshot: ServerProvider) =>
    Effect.map(
      instance.orchestrationAdapter.bareSupport ??
        Effect.succeed(unsupportedBare(instance.driverKind)),
      (bare): ServerProvider => ({ ...snapshot, bare }),
    );
  return {
    ...source,
    getSnapshot: Effect.flatMap(source.getSnapshot, stamp),
    refresh: Effect.flatMap(source.refresh, stamp),
    streamChanges: Stream.mapEffect(source.streamChanges, stamp),
  };
}
