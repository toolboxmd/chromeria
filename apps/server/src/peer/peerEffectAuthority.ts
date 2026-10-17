import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { PendingOrchestrationEffectV2 } from "../orchestration-v2/EffectOutbox.ts";

/** Set only by authenticated tool intake. The event sink persists it with the effect intent. */
export const PeerDispatchAuthority = Context.Reference<string | undefined>(
  "t3/peer/PeerDispatchAuthority",
  { defaultValue: () => undefined },
);

export const recordPeerEffectAuthority = (effects: ReadonlyArray<PendingOrchestrationEffectV2>) =>
  Effect.gen(function* () {
    const authority = yield* PeerDispatchAuthority;
    if (authority === undefined || effects.length === 0) return;
    const sql = yield* SqlClient.SqlClient;
    for (const effect of effects) {
      yield* sql`INSERT INTO peer_effect_authority (effect_id, record_id) VALUES (${effect.id}, ${authority}) ON CONFLICT DO NOTHING`;
    }
  });
