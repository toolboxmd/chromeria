import { CommandId, MessageId, OrchestrationV2Command, type ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import type { BoundSpectrum } from "./handoff.ts";

/**
 * A fixture of #176's published Spectrum storage (toolboxmd/chromeria
 * 7cb115d306, `spectrum/store.ts` and `spectrum/state.ts`): its exact table,
 * and payloads written as its encoder writes them. #176 owns the real adapter;
 * this fixture maps the same rows onto the fence's port. Needs-you is not
 * stored: #176's adapter derives it from its bounded attempts and recovery
 * facts, so a test supplies it per report command.
 */
export const ensureSpectraFixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS fork_spectra (
    thread_id TEXT PRIMARY KEY,
    caller_thread_id TEXT NOT NULL,
    scheduler_run_id TEXT,
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS fork_spectra_caller ON fork_spectra(caller_thread_id, scheduler_run_id)`;
});

const FixtureReport = OrchestrationV2Command.pipe(
  Schema.refine(
    (command): command is Extract<OrchestrationV2Command, { type: "message.dispatch" }> =>
      command.type === "message.dispatch",
  ),
);
/** The fields of #176's `SpectrumState` that the fence reads; missing report fields decode null. */
const decodeFixtureSpectrum = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      status: Schema.Literals(["active", "settled", "retired"]),
      outbox: Schema.Array(Schema.Struct({ commandId: CommandId })),
      report: Schema.NullOr(FixtureReport).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
      reportAbandonment: Schema.NullOr(
        Schema.Struct({
          commandId: CommandId,
          person: Schema.String,
          abandonedAt: Schema.DateTimeUtcFromString,
        }),
      ).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
    }),
  ),
);

/** The port over fixture rows; `needsYou` maps a report command to the reason #176 would derive. */
export const fixtureSpectra = (needsYou: ReadonlyMap<string, string> = new Map()) => ({
  boundTo: (schedulerRunId: string) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM fork_spectra WHERE scheduler_run_id = ${schedulerRunId}
      `;
      return rows.map((row): BoundSpectrum => {
        const state = decodeFixtureSpectrum(row.payload_json);
        const report = state.report;
        const reason = report === null ? undefined : needsYou.get(report.commandId);
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
          reportNeedsYou:
            report === null || reason === undefined
              ? null
              : { commandId: report.commandId, reason },
        };
      });
    }),
});

const encodeCommand = Schema.encodeSync(OrchestrationV2Command);
const toJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** A report attempt as Spectrum dispatches it to the caller's thread. */
const reportCommand = (threadId: ThreadId, id: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(`report:${id}`),
  threadId,
  messageId: MessageId.make(`report-message:${id}`),
  createdBy: "system" as const,
  creationSource: "server" as const,
  text: "Spectrum report",
  attachments: [],
  dispatchMode: { type: "queue_after_active" as const },
});

let spectra = 0;
/** Binds one Spectrum to a scheduler run, as a row in #176's full payload shape. */
export const bindSpectrum = (input: {
  readonly callerThreadId: ThreadId;
  readonly schedulerRunId: string;
  readonly status: "active" | "settled" | "retired";
  readonly report?: string;
  readonly inOutbox?: boolean;
  readonly abandoned?: string;
  /** A row written before the report fields existed. */
  readonly legacy?: boolean;
  /** Replaces this Spectrum's row instead of adding one. */
  readonly spectrumThreadId?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    spectra += 1;
    const spectrumThreadId = input.spectrumThreadId ?? `spectrum:${spectra}`;
    const report =
      input.report === undefined
        ? null
        : encodeCommand(reportCommand(input.callerThreadId, input.report));
    const selection = { instanceId: "codex", model: "test-model" };
    const payload = {
      version: 1,
      threadId: spectrumThreadId,
      callerThreadId: input.callerThreadId,
      callerRunId: "run:caller",
      scheduledTaskId: "task:bound",
      schedulerRunId: input.schedulerRunId,
      question: "What should we build?",
      mode: "council",
      limit: 2,
      moderator: 1,
      participants: [
        { threadId: `${spectrumThreadId}:blue`, label: "Blue", selection },
        { threadId: `${spectrumThreadId}:red`, label: "Red", selection },
      ],
      generation: 0,
      revision: 0,
      cursor: 0,
      status: input.status,
      cycle: 0,
      round: null,
      transcript: [],
      inbox: [],
      outbox: report !== null && input.inOutbox === true ? [report] : [],
      ...(input.legacy === true
        ? {}
        : {
            report,
            reportAbandonment:
              input.abandoned === undefined
                ? null
                : {
                    commandId: `report:${input.abandoned}`,
                    person: "user",
                    abandonedAt: "2026-10-08T12:30:00.000Z",
                  },
          }),
    };
    yield* sql`INSERT OR REPLACE INTO fork_spectra ${sql.insert({
      thread_id: spectrumThreadId,
      caller_thread_id: input.callerThreadId,
      scheduler_run_id: input.schedulerRunId,
      generation: 0,
      revision: 0,
      payload_json: toJson(payload),
    })}`;
    return spectrumThreadId;
  });

/** The orchestrator's receipt for a report attempt's dispatch, through the real receipt store. */
export const reportReceipt = (threadId: ThreadId, id: string, status: "accepted" | "rejected") =>
  Effect.gen(function* () {
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    yield* receipts
      .insertIfAbsent({
        commandId: CommandId.make(`report:${id}`),
        threadId,
        commandType: "message.dispatch",
        acceptedAt: DateTime.makeUnsafe("2026-10-08T12:00:00.000Z"),
        resultSequence: 1,
        status,
        error: status === "rejected" ? "The report was refused." : null,
      })
      .pipe(Effect.orDie);
  }).pipe(Effect.provide(CommandReceiptStore.layer));
