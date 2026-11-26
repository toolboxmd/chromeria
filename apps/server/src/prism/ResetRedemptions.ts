import type {
  ProviderClearRedeemedResetResult,
  ProviderConsumeResetCreditOutcome,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProvider,
  ServerSettingsError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

/** Prism's capacity query reads this table, so its layer creates it too. */
export const initializeResetRedemptions = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_prism_reset_voided_failures (
    turn_item_id TEXT NOT NULL, provider_instance_id TEXT NOT NULL, driver TEXT NOT NULL,
    redeemed_at TEXT NOT NULL, PRIMARY KEY (turn_item_id, provider_instance_id, driver)
  )`;
});

/** A usage window Prism treats as closed: fully used and not yet reset. */
export function exhaustedWindow(provider: Pick<ServerProvider, "usageLimits">, now: number) {
  return (provider.usageLimits?.windows ?? []).find(
    (window) =>
      window.usedPercent >= 100 &&
      (window.resetsAt === undefined || Date.parse(window.resetsAt) > now),
  );
}

export interface UsageLimitFailure {
  readonly turn_item_id: string;
  readonly provider_instance_id: string;
  readonly driver: string | null;
  readonly run_id: string;
  readonly failed_at: string;
  readonly reset_at: string;
}

/**
 * Unexpired usage-limit failures that still block their instance: each failed run's latest root
 * error, by the executing run's instance, minus those a confirmed reset voided.
 */
export const blockingUsageLimitFailures = Effect.fn("ResetRedemptions.blockingFailures")(function* (
  now: DateTime.DateTime,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<UsageLimitFailure>`
      SELECT item.turn_item_id, r.provider_instance_id, thread.driver, r.run_id,
        item.updated_at AS failed_at,
        json_extract(item.payload_json, '$.failure.resetAt') AS reset_at
      FROM orchestration_v2_projection_turn_items item
      INNER JOIN orchestration_v2_projection_runs r ON r.run_id = item.run_id
      LEFT JOIN orchestration_v2_projection_provider_threads thread
        ON thread.provider_thread_id = r.provider_thread_id
      WHERE r.status = 'failed' AND r.provider_instance_id IS NOT NULL
        AND item.type = 'error' AND item.status = 'failed'
        AND item.turn_item_id = (
          SELECT error.turn_item_id FROM orchestration_v2_projection_turn_items error
          WHERE error.thread_id = r.thread_id AND error.run_id = r.run_id
            AND error.type = 'error' AND error.status = 'failed'
            AND error.node_id IS json_extract(r.payload_json, '$.rootNodeId')
          ORDER BY error.updated_at DESC, error.ordinal DESC, error.turn_item_id DESC
          LIMIT 1
        )
        AND json_extract(item.payload_json, '$.failure.class') = 'usage_limit'
        AND julianday(json_extract(item.payload_json, '$.failure.resetAt')) > julianday(${DateTime.formatIso(now)})
        AND NOT EXISTS (
          SELECT 1 FROM fork_prism_reset_voided_failures voided
          WHERE voided.turn_item_id = item.turn_item_id
            AND voided.provider_instance_id = r.provider_instance_id
            AND voided.driver IS thread.driver
        )
    `;
});

/**
 * A confirmed reset-credit redemption voids exactly the failures that blocked its instance when
 * it started, by identity, so clocks and later rewrites of those failures do not matter.
 */
export class ResetRedemptions extends Context.Service<
  ResetRedemptions,
  {
    readonly redeem: <E, R>(
      instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
      consume: Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>,
    ) => Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>;
    /**
     * A reset redeemed for this login elsewhere: refresh the instance's usage, then void the
     * failures that block it now. Refreshing first keeps a stale window from admitting it.
     * Runs to its end once started, even if the caller stops waiting; the provider's own refresh
     * timeout bounds it.
     */
    readonly clearRedeemed: (
      instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
      usage: {
        readonly getSnapshot: Effect.Effect<ServerProvider>;
        readonly refreshUsage?: Effect.Effect<void, ServerSettingsError> | undefined;
      },
    ) => Effect.Effect<ProviderClearRedeemedResetResult>;
  }
>()("t3/prism/ResetRedemptions") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* initializeResetRedemptions;
  // The reset happened either way; without the record the old block holds until it expires.
  const unrecorded = (instanceId: ProviderInstanceId) => (cause: unknown) =>
    Effect.logWarning("Unable to record a confirmed reset-credit redemption.", {
      instanceId,
      cause,
    });
  const blockingOn = (
    instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
    at: DateTime.DateTime,
  ) =>
    blockingUsageLimitFailures(at).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.map((failures) =>
        failures.filter(
          (f) => f.provider_instance_id === instance.instanceId && f.driver === instance.driver,
        ),
      ),
    );
  const voidFailures = (
    instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
    failures: ReadonlyArray<UsageLimitFailure>,
    redeemedAt: DateTime.DateTime,
  ) =>
    Effect.forEach(
      failures,
      (failure) =>
        sql`INSERT OR IGNORE INTO fork_prism_reset_voided_failures
          (turn_item_id, provider_instance_id, driver, redeemed_at)
          VALUES (${failure.turn_item_id}, ${instance.instanceId}, ${instance.driver},
            ${DateTime.formatIso(redeemedAt)})`,
      { discard: true },
    );
  const redeem = Effect.fn("ResetRedemptions.redeem")(function* <E, R>(
    instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
    consume: Effect.Effect<ProviderConsumeResetCreditOutcome, E, R>,
  ) {
    const started = yield* DateTime.now;
    // Read before the call: a failure recorded while the reset is confirmed is not in the set.
    const blocking: ReadonlyArray<UsageLimitFailure> = yield* blockingOn(instance, started).pipe(
      Effect.catchCause((cause) =>
        unrecorded(instance.instanceId)(cause).pipe(
          Effect.as<ReadonlyArray<UsageLimitFailure>>([]),
        ),
      ),
    );
    const outcome = yield* consume;
    if (outcome === "reset")
      yield* voidFailures(instance, blocking, started).pipe(
        Effect.catchCause(unrecorded(instance.instanceId)),
      );
    return outcome;
  });
  const clearRedeemed = Effect.fn("ResetRedemptions.clearRedeemed")(function* (
    instance: { readonly instanceId: ProviderInstanceId; readonly driver: ProviderDriverKind },
    usage: {
      readonly getSnapshot: Effect.Effect<ServerProvider>;
      readonly refreshUsage?: Effect.Effect<void, ServerSettingsError> | undefined;
    },
  ) {
    const refreshed = yield* (usage.refreshUsage ?? Effect.void).pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        Effect.logWarning("Unable to refresh usage after a reset redeemed elsewhere.", {
          instanceId: instance.instanceId,
          cause,
        }).pipe(Effect.as(false)),
      ),
    );
    const now = yield* DateTime.now;
    const blocking = yield* blockingOn(instance, now).pipe(
      Effect.tap((failures) => voidFailures(instance, failures, now)),
      Effect.catchCause((cause) =>
        unrecorded(instance.instanceId)(cause).pipe(
          Effect.as<ReadonlyArray<UsageLimitFailure>>([]),
        ),
      ),
    );
    const snapshot = yield* usage.getSnapshot;
    const readFailed = !refreshed || snapshot.usageLimits?.unavailable?.reason === "probeFailed";
    return {
      voided: blocking.length,
      usage: !exhaustedWindow(snapshot, DateTime.toEpochMillis(now))
        ? "headroom"
        : readFailed
          ? "unread"
          : "exhausted",
    } satisfies ProviderClearRedeemedResetResult;
  }, Effect.uninterruptible);
  return ResetRedemptions.of({ redeem, clearRedeemed });
});

export const layer = Layer.effect(ResetRedemptions, make);
