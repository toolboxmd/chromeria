import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";

import { backfillImportedLineage, lineageBackfillId } from "../childThreads/lineageBackfill.ts";
import type * as EventSink from "../orchestration-v2/EventSink.ts";
import { backfillThreadPeople } from "./forkThreadPeopleBackfill.ts";
import {
  backfillScheduledTasks,
  scheduledTasksBackfillId,
} from "../scheduledTaskChecks/v1Import.ts";
import { ensureSpectrumSchema } from "../spectrum/store.ts";

export interface ForkV1Backfill {
  readonly id: string;
  readonly run: Effect.Effect<
    void,
    SqlError | EventSink.EventSinkV2Error | ForkV1BackfillStepError,
    SqlClient.SqlClient | EventSink.EventSinkV2
  >;
}

export class ForkV1BackfillStepError extends Schema.TaggedError<ForkV1BackfillStepError>()(
  "ForkV1BackfillStepError",
  { backfillId: Schema.String },
) {}

export class ForkV1BackfillError extends Schema.TaggedError<ForkV1BackfillError>()(
  "ForkV1BackfillError",
  { backfillId: Schema.String },
) {
  override get message() {
    return `Fork V1 backfill ${this.backfillId} failed; startup cannot continue.`;
  }
}

/**
 * Register feature-owned, idempotent backfills here. They run on every startup
 * after V2 shells are imported, before recovery and workers start. Read only
 * imported V2 shells and frozen V1 data: transcripts have NOT been hydrated.
 * Features own their recovery markers in fork tables or supported V2 event IDs,
 * outside the upstream migration ledger. No provider work may start here.
 */
export const forkV1Backfills: ReadonlyArray<ForkV1Backfill> = [
  { id: "spectrum-schema", run: ensureSpectrumSchema },
  {
    id: "thread-people",
    run: backfillThreadPeople.pipe(
      Effect.catchTags({
        ThreadPeopleBackfillPayloadError: () =>
          Effect.fail(new ForkV1BackfillStepError({ backfillId: "thread-people" })),
      }),
    ),
  },
  { id: lineageBackfillId, run: backfillImportedLineage() },
  {
    id: scheduledTasksBackfillId,
    run: backfillScheduledTasks.pipe(
      Effect.catchTags({
        SchemaError: () =>
          Effect.fail(new ForkV1BackfillStepError({ backfillId: scheduledTasksBackfillId })),
      }),
    ),
  },
];

export const runForkV1Backfills = Effect.fn("forkV1Backfills.run")(function* (
  backfills: ReadonlyArray<ForkV1Backfill> = forkV1Backfills,
) {
  for (const backfill of backfills) {
    yield* Effect.matchCauseEffect(backfill.run, {
      onFailure: (cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Effect.fail(new ForkV1BackfillError({ backfillId: backfill.id })),
      onSuccess: () => Effect.void,
    });
  }
});
