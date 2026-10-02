import { ThreadId, type OrchestrationEvent, type TaskRun } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  SpectrumState,
  SCHEDULER_REPORT_KIND,
  spectrumReportQueueId,
} from "../mcp/toolkits/threads/spectrum.ts";

const decodeState = Schema.decodeUnknownEffect(Schema.fromJsonString(SpectrumState));
const decodeReceiptKeys = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
const decodeQueuedReport = Schema.decodeUnknownEffect(
  Schema.Struct({ runId: Schema.String, text: Schema.String }),
);
const receiptMarker = "\nScheduler report receipts JSON:\n";
export type RunReport = { readonly key: string; readonly text: string };
export const appendRunReports = (text: string, reports: ReadonlyArray<RunReport>) =>
  reports.length === 0
    ? text
    : `${text}\nRun Drafter reports/errors:\n${reports.map((report) => report.text).join("\n\n")}${receiptMarker}${JSON.stringify(reports.map((report) => report.key))}`;

/** Spectrum handoffs and child activities are the durable queue. Only accepted own turns consume it. */
export const makeRunReports = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return Effect.fnUntraced(function* (run: TaskRun, events: ReadonlyArray<OrchestrationEvent>) {
    const acceptedMessages = new Set(
      events.flatMap((event) =>
        event.type === "thread.turn-start-requested" &&
        event.commandId?.startsWith(`server:scheduler-turn:${run.id}:`)
          ? [event.payload.messageId]
          : [],
      ),
    );
    const delivered = new Set<string>();
    for (const event of events) {
      if (
        event.type !== "thread.message-sent" ||
        event.payload.role !== "user" ||
        !acceptedMessages.has(event.payload.messageId)
      )
        continue;
      const index = event.payload.text.lastIndexOf(receiptMarker);
      if (index < 0) continue;
      const keys = decodeReceiptKeys(event.payload.text.slice(index + receiptMarker.length));
      if (keys._tag === "Some") for (const key of keys.value) delivered.add(key);
    }
    const queuedReports: RunReport[] = [];
    const handoffs = new Set<string>();
    const childReports = new Map<string, RunReport>();
    for (const event of events) {
      if (event.type !== "thread.activity-appended") continue;
      const activity = event.payload.activity;
      if (activity.kind === SCHEDULER_REPORT_KIND) {
        const report = yield* decodeQueuedReport(activity.payload);
        handoffs.add(activity.id);
        const key = `spectrum:${activity.id}`;
        if (report.runId === run.id && !delivered.has(key))
          queuedReports.push({ key, text: report.text });
        continue;
      }
      if (
        !/^task\.(progress|updated|completed)$/.test(activity.kind) ||
        !Predicate.isObject(activity.payload) ||
        typeof activity.payload.taskId !== "string"
      )
        continue;
      if (
        activity.payload.status !== "idle" &&
        activity.payload.status !== "failed" &&
        activity.kind !== "task.completed"
      )
        continue;
      if (activity.payload.status === "idle" && activity.payload.reportBack === false) continue;
      const summary =
        typeof activity.payload.summary === "string" ? activity.payload.summary : activity.summary;
      childReports.set(activity.payload.taskId, {
        key:
          activity.payload.status === "idle" &&
          typeof activity.payload.reportedMessageId === "string"
            ? `child:${activity.payload.taskId}:${activity.payload.reportedMessageId}`
            : `child:${event.sequence}`,
        text: `[Drafter ${activity.payload.taskId}, ${String(activity.payload.status ?? activity.kind)}] ${summary}`,
      });
    }
    // These are run-scoped reads from the existing append-only log, never live full-history payloads.
    const rows = yield* sql<{
      payload: string;
    }>`SELECT json_extract(payload_json, '$.activity.payload') AS payload FROM orchestration_events
      WHERE event_type = 'thread.activity-appended' AND json_extract(payload_json, '$.activity.kind') = 'spectrum.state'
      AND json_extract(payload_json, '$.activity.payload.callerId') = ${run.threadId} AND sequence > ${run.originSequence}
      AND sequence IN (SELECT MAX(sequence) FROM orchestration_events WHERE event_type = 'thread.activity-appended'
        AND json_extract(payload_json, '$.activity.kind') = 'spectrum.state'
        AND json_extract(payload_json, '$.activity.payload.callerId') = ${run.threadId} AND sequence > ${run.originSequence} GROUP BY stream_id)`;
    const states = yield* Effect.forEach(rows, (row) => decodeState(row.payload));
    const participants = states.flatMap((state) =>
      state.participants.map((child) => ThreadId.make(child.threadId)),
    );
    // A settled state is persisted before flush hands its report off. Keep that crash/race window pending.
    const pending = states.some(
      (state) =>
        state.status === "active" ||
        state.outbox.some(
          (command) =>
            command.type === "thread.turn.start" &&
            command.threadId === run.threadId &&
            !handoffs.has(spectrumReportQueueId(state, command.commandId)),
        ),
    );
    const reports = [...childReports.values()].filter((report) => !delivered.has(report.key));
    reports.push(...queuedReports);
    return { reports, participants, pending };
  });
});
