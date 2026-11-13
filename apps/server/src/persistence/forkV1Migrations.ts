import * as Effect from "effect/Effect";

// Chromeria retains passive legacy data. Keep upstream migration 63's ledger
// identity without deleting existing tables or recreating already absent ones.
export const retainLegacyProjections = Effect.void;
