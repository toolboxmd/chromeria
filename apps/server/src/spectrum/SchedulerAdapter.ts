import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { type BoundSpectrum, ScheduledTaskSpectra } from "../scheduledTaskChecks/handoff.ts";
import { reportNeedsYou } from "./reportPolicy.ts";
import { SpectrumState } from "./state.ts";

const decode = Schema.decodeUnknownOption(Schema.fromJsonString(SpectrumState));

// An unreadable row holds its scheduler run like an unsettled Spectrum, never releasing it by absence.
const unreadable: BoundSpectrum = {
  status: "active",
  report: null,
  reportAbandonment: null,
  reportNeedsYou: null,
};

/**
 * #174's port over Spectrum's own rows (D32, D40). Reads through the caller's
 * SqlClient, so the scheduler's fence sees one transaction; needs-you is derived
 * from the current attempt, never stored.
 */
export const boundTo = Effect.fn("SpectrumScheduler.boundTo")(function* (schedulerRunId: string) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly payload_json: string }>`
    SELECT payload_json FROM fork_spectra WHERE scheduler_run_id = ${schedulerRunId}
    ORDER BY thread_id
  `;
  return yield* Effect.forEach(rows, (row) =>
    Effect.gen(function* () {
      const decoded = decode(row.payload_json);
      if (Option.isNone(decoded)) return unreadable;
      const state = decoded.value;
      const report = state.report;
      return {
        status: state.status,
        report:
          report === null
            ? null
            : {
                commandId: report.commandId,
                messageId: report.messageId,
                threadId: report.threadId,
                inOutbox: state.outbox.some((command) => command.commandId === report.commandId),
              },
        reportAbandonment:
          state.reportAbandonment === null
            ? null
            : { commandId: state.reportAbandonment.commandId },
        reportNeedsYou: yield* reportNeedsYou(state),
      } satisfies BoundSpectrum;
    }),
  );
});

export const layer = Layer.succeed(ScheduledTaskSpectra, { boundTo });
