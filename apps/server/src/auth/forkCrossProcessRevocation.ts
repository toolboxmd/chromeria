import type { AuthSessionId } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

/**
 * Upper bound, besides the read itself, between another process committing a
 * revocation (such as `t3 auth session revoke`) and this server noticing it.
 */
export const CROSS_PROCESS_REVOCATION_POLL_INTERVAL = Duration.seconds(2);

export interface CrossProcessRevocationSource<E> {
  /** `PRAGMA data_version`: changes whenever another connection commits. */
  readonly dataVersion: Effect.Effect<number, E>;
  /** The given sessions that are neither revoked nor deleted. */
  readonly listActive: (
    sessionIds: ReadonlyArray<AuthSessionId>,
  ) => Effect.Effect<ReadonlyArray<AuthSessionId>, E>;
}

/**
 * Same-process revocation already reaches sockets through the store's PubSub;
 * this one loop covers commits from other processes for every open socket.
 * A failed read invalidates nothing and is retried on the next tick, because
 * the CLI and server share SQLite and a lock is an ordinary transient state.
 */
export const watchCrossProcessRevocations = <E>(options: {
  readonly source: CrossProcessRevocationSource<E>;
  /** Sessions with a running invalidation watcher, each registered before its first row read. */
  readonly watchedSessions: SubscriptionRef.SubscriptionRef<ReadonlyMap<AuthSessionId, number>>;
  readonly invalidate: (sessionId: AuthSessionId) => Effect.Effect<void>;
}): Effect.Effect<never> =>
  Effect.suspend(() => {
    let checkedVersion: number | undefined;
    const openSessionIds = SubscriptionRef.get(options.watchedSessions).pipe(
      Effect.map((watched) => Array.from(watched.keys())),
    );
    // Idle until something is watched, so no timer runs on a server without sockets.
    const untilWatched = SubscriptionRef.changes(options.watchedSessions).pipe(
      Stream.filter((watched) => watched.size > 0),
      Stream.runHead,
    );
    const tick = Effect.gen(function* () {
      if ((yield* openSessionIds).length === 0) return;
      const version = yield* options.source.dataVersion;
      if (version === checkedVersion) return;
      // Snapshot after the version read. Callers register a session before its first
      // row read, so one registering later reads after every commit this version covers.
      const open = yield* openSessionIds;
      const active = new Set(yield* options.source.listActive(open));
      yield* Effect.forEach(
        open.filter((sessionId) => !active.has(sessionId)),
        options.invalidate,
        { discard: true },
      );
      checkedVersion = version;
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Cross-process revocation check failed; retrying.", { cause }),
      ),
    );
    return Effect.forever(
      untilWatched.pipe(
        Effect.andThen(Effect.sleep(CROSS_PROCESS_REVOCATION_POLL_INTERVAL)),
        Effect.andThen(tick),
      ),
    );
  });
