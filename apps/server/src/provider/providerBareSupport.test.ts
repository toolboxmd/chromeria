import { describe, expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type BareSupport,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { withBareSupport } from "./providerBareSupport.ts";

const driverKind = ProviderDriverKind.make("codex");
const snapshot = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: driverKind,
  enabled: true,
  installed: true,
  version: "1.0.0",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-11-18T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
} as const satisfies ServerProvider;

const source = (bareSupport: Effect.Effect<BareSupport> | undefined) =>
  withBareSupport(
    {
      driverKind,
      orchestrationAdapter: { bareSupport },
    } as unknown as ProviderInstance,
    {
      instanceId: snapshot.instanceId,
      driverKind,
      getSnapshot: Effect.succeed(snapshot),
      refresh: Effect.succeed(snapshot),
      streamChanges: Stream.make(snapshot),
    },
  );

describe("withBareSupport", () => {
  it.effect("stamps every snapshot path with the adapter's bare support", () =>
    Effect.gen(function* () {
      const reason = "Codex native keyring-only auth is unsupported for bare.";
      const unsupported = source(Effect.succeed({ supported: false, reason }));
      const expected = { supported: false, reason };
      expect((yield* unsupported.getSnapshot).bare).toEqual(expected);
      expect((yield* unsupported.refresh).bare).toEqual(expected);
      const streamed = yield* Stream.runCollect(unsupported.streamChanges);
      expect(Array.from(streamed).map((provider) => provider.bare)).toEqual([expected]);

      const supported = source(Effect.succeed({ supported: true }));
      expect((yield* supported.getSnapshot).bare).toEqual({ supported: true });
    }),
  );

  it.effect("reports an adapter without bare support as unsupported", () =>
    Effect.gen(function* () {
      const bare = (yield* source(undefined).getSnapshot).bare;
      expect(bare?.supported).toBe(false);
    }),
  );
});
