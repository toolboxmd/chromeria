import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { PendingOrchestrationEffectV2 } from "../orchestration-v2/EffectOutbox.ts";

/** Set only by authenticated tool intake. The event sink persists it with the effect intent. */
export const PeerDispatchAuthority = Context.Reference<string | undefined>(
  "t3/peer/PeerDispatchAuthority",
  { defaultValue: () => undefined },
);

/** Initialized with the event sink so local-only services need no peer runtime. */
export const initializePeerEffectAuthority = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_effect_authority (effect_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_run_authority (run_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
});

export const recordPeerEffectAuthority = (
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
  effects: ReadonlyArray<PendingOrchestrationEffectV2>,
) =>
  Effect.gen(function* () {
    const authority = yield* PeerDispatchAuthority;
    const sql = yield* SqlClient.SqlClient;
    // A queued command has no effect yet. Its run owns the accepted authority,
    // independently of the target thread's owner and of the later promoter.
    for (const event of events) {
      if (event.type !== "run.created") continue;
      const sourceRunId =
        event.payload.restartContinuationOfRunId ?? event.payload.forkPrismContinuationSourceRunId;
      const inherited =
        sourceRunId === undefined
          ? []
          : yield* sql<{
              record_id: string;
            }>`SELECT record_id FROM peer_run_authority WHERE run_id = ${sourceRunId}`;
      const owner = inherited[0]?.record_id ?? authority;
      if (owner !== undefined) {
        yield* sql`INSERT INTO peer_run_authority (run_id, record_id) VALUES (${event.payload.id}, ${owner}) ON CONFLICT DO NOTHING`;
      }
    }
    for (const effect of effects) {
      const runId =
        "runId" in effect.request
          ? effect.request.runId
          : "sourceRunId" in effect.request
            ? effect.request.sourceRunId
            : undefined;
      const rows =
        runId === undefined
          ? []
          : yield* sql<{
              record_id: string;
            }>`SELECT record_id FROM peer_run_authority WHERE run_id = ${runId}`;
      // Promotion can happen inside a different caller's command. A local
      // queued start must never inherit that promoter's peer authority.
      const owner =
        rows[0]?.record_id ??
        (effect.request.type === "provider-turn.start" ? undefined : authority);
      if (owner !== undefined) {
        yield* sql`INSERT INTO peer_effect_authority (effect_id, record_id) VALUES (${effect.id}, ${owner}) ON CONFLICT DO NOTHING`;
      }
    }
  });
