import type { ProviderConsumeResetCreditOutcome, ProviderInstanceId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

/** Prism's capacity query reads this table, so its layer creates it too. */
export const initializeResetRedemptions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_prism_reset_redemptions (
    provider_instance_id TEXT PRIMARY KEY, started_at TEXT NOT NULL
  )`;
});

/**
 * Confirmed reset-credit redemptions, newest per provider instance. A confirmed reset voids the
 * instance's usage-limit failures recorded before the redemption started.
 */
export class ResetRedemptions extends Context.Service<
  ResetRedemptions,
  {
    /** Runs a redemption and records its start time when the provider confirms a reset. */
    readonly redeem: <E, R>(
      instanceId: ProviderInstanceId,
      consume: Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>,
    ) => Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>;
  }
>()("t3/prism/ResetRedemptions") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* initializeResetRedemptions;
  const record = (instanceId: ProviderInstanceId, startedAt: string) =>
    sql`INSERT INTO fork_prism_reset_redemptions (provider_instance_id, started_at)
      VALUES (${instanceId}, ${startedAt})
      ON CONFLICT(provider_instance_id) DO UPDATE SET started_at = excluded.started_at
      WHERE julianday(excluded.started_at) > julianday(started_at)`.pipe(
      Effect.asVoid,
      // The reset happened either way; without the record the old block holds until it expires.
      Effect.catchCause((cause) =>
        Effect.logWarning("Unable to record a confirmed reset-credit redemption.", {
          instanceId,
          cause,
        }),
      ),
    );
  const redeem = Effect.fn("ResetRedemptions.redeem")(function* <E, R>(
    instanceId: ProviderInstanceId,
    consume: Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>,
  ) {
    // Taken before the call: a failure recorded while the reset is confirmed stays in force.
    const startedAt = DateTime.formatIso(yield* DateTime.now);
    const outcome = yield* consume;
    if (outcome === "reset") yield* record(instanceId, startedAt);
    return outcome;
  });
  return ResetRedemptions.of({ redeem });
});

export const layer = Layer.effect(ResetRedemptions, make);
