// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  COMMAND_OUTPUTS_KEPT,
  CommandId,
  EnvironmentId,
  OrchestrationEvent,
  ProjectId,
  ProviderInstanceId,
  ScheduledTask,
  ScheduledTaskView,
  SchedulerError,
  ThreadId,
  commandFailureToNotify,
  isCommandTask,
  isSettledRun,
  threadOwner,
  type CommandTaskDefinition,
  type TaskRun,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";
import { ServerConfig } from "../config.ts";
import { McpInvocationContext, type McpCapability } from "../mcp/McpInvocationContext.ts";
import { SchedulerToolkitHandlersLive } from "../mcp/toolkits/scheduler/handlers.ts";
import { SchedulerToolkit } from "../mcp/toolkits/scheduler/tools.ts";
import {
  NOW,
  PARENT_ID,
  commandId,
  createParent,
  temporaryDirectory,
} from "../mcp/toolkits/threads/handlers.testFixtures.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadPlanProgress from "../orchestration/ThreadPlanProgress.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { makeProviderRegistryLayer } from "../provider/testUtils/providerRegistryMock.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { COMMAND_OUTPUT_BYTES } from "./CommandRunner.ts";
import { makeSchedulerRpcHandlers } from "./rpcHandlers.ts";
import { Scheduler, makeLiveScheduler } from "./Service.ts";

const PROJECT_ID = ProjectId.make("project-threads");
const layer = (directory: string) =>
  Layer.mergeAll(
    OrchestrationLayerLive,
    ServerSettingsService.layerTest(),
    makeProviderRegistryLayer([]),
    ProcessRunner.layer,
  ).pipe(
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(makeSqlitePersistenceLive(NodePath.join(directory, "state.sqlite"))),
    Layer.provideMerge(ServerConfig.layerTest(directory, { prefix: "scheduler-command-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
/** One server lifetime over the directory's SQLite file; closing it is a server stop. */
const within = <A, E, R>(directory: string, effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect.pipe(Effect.provide(layer(directory))));
/** A second server process over the same SQLite file, running next to the current one. */
const alongside = <A, E, R>(directory: string, effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect.pipe(Effect.provide(Layer.fresh(layer(directory)))));
const command = (text: string, minutes = 60): CommandTaskDefinition => ({
  kind: "command",
  title: "Export",
  projectId: PROJECT_ID,
  command: text,
  schedule: { kind: "interval", minutes },
});
/** The side-effect oracle: every execution appends its run id. */
const RECORD = `printf '%s\\n' '{run_id}' >> runs.log`;
const executions = (directory: string) =>
  Effect.promise(() =>
    NodeFSP.readFile(NodePath.join(directory, "runs.log"), "utf8").catch(() => ""),
  ).pipe(Effect.map((text) => text.split("\n").filter(Boolean)));
const fifo = (path: string) => NodeChildProcess.execFileSync("mkfifo", [path]);
const only = (scheduler: Effect.Success<typeof makeLiveScheduler>) =>
  scheduler.list.pipe(Effect.map((tasks) => tasks[0]!));
/** The engine command that moves `task` to its next revision with `patch` applied. */
const stateCommand = (task: ScheduledTask, patch: Partial<ScheduledTask>, id = commandId()) => ({
  type: "scheduler.state.set" as const,
  commandId: id,
  threadId: ThreadId.make(task.id),
  expectedRevision: task.revision,
  createdAt: NOW,
  task: { ...task, ...patch, revision: task.revision + 1 },
});
const forge = (task: ScheduledTask, patch: Partial<ScheduledTask>) =>
  Effect.flatMap(OrchestrationEngineService, (engine) =>
    engine.dispatch(stateCommand(task, patch)).pipe(Effect.exit),
  );
const encodeTask = Schema.encodeSync(ScheduledTask);
const eventJson = Schema.encodeSync(Schema.fromJsonString(OrchestrationEvent));
const decodeTask = Schema.decodeUnknownSync(ScheduledTask);
const listJson = Schema.encodeSync(Schema.fromJsonString(Schema.Array(ScheduledTaskView)));
const reportJson = Schema.encodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);
const decodeJsonTask = Schema.decodeUnknownSync(Schema.fromJsonString(ScheduledTask));
const encodeJsonTask = Schema.encodeSync(Schema.fromJsonString(ScheduledTask));
/** The task exactly as the state store holds it, without the computed view fields. */
const persisted = (task: ScheduledTask) => decodeTask(encodeTask(task));
const CHATTY_RUNS = 25;
/** A five-minute interval runs 288 times a day. */
const perDay = (total: number) => Math.round((total / CHATTY_RUNS) * 288);
const sum = (values: ReadonlyArray<number>) => values.reduce((total, value) => total + value, 0);
// Set SCHEDULER_SIZE_REPORT to a path prefix to keep the size measurements.
const reportPrefix = process.env.SCHEDULER_SIZE_REPORT;
const writeReport = (name: string, report: Record<string, number>) =>
  reportPrefix === undefined
    ? Effect.void
    : Effect.promise(() => NodeFSP.writeFile(`${reportPrefix}${name}.json`, reportJson(report)));
/** Allocated pages of each table and its indexes, page overhead included. Reports only. */
const tablePages = Effect.gen(function* () {
  if (reportPrefix === undefined) return {} as Record<string, number>;
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ name: string; bytes: number }>`
    SELECT m.tbl_name AS name, SUM(d.pgsize) AS bytes
    FROM dbstat AS d JOIN sqlite_master AS m ON m.name = d.name
    WHERE ${sql.in("m.tbl_name", ["orchestration_events", "orchestration_command_receipts", "fork_scheduler_task_state"])}
    GROUP BY m.tbl_name`;
  return Object.fromEntries(rows.map((row) => [row.name, row.bytes]));
});
/**
 * Runs a five-minute command 25 times with 20 KB of output each; `fails` decides each exit from
 * the run number `n`. Measures what the task adds to the database, projected to a day by
 * repeating the measured runs: its permanent state events as stored column bytes and as
 * serialized events, its command receipts as stored column bytes, integers counted as 8.
 */
const chattyCommand = (directory: string, fails: string) =>
  Effect.gen(function* () {
    yield* createParent(directory);
    const scheduler = yield* makeLiveScheduler;
    const engine = yield* OrchestrationEngineService;
    const sql = yield* SqlClient.SqlClient;
    const before = yield* tablePages;
    const created = yield* scheduler.create(
      command(
        `${RECORD}; head -c 20000 /dev/zero | tr '\\0' x; n=$(wc -l < runs.log); ${fails}`,
        5,
      ),
      "user:creator",
    );
    for (let index = 0; index < CHATTY_RUNS; index++) {
      yield* TestClock.adjust("5 minutes");
      yield* scheduler.reconcile();
      yield* scheduler.drainCommands;
    }
    const task = yield* only(scheduler);
    const events = yield* sql<{ payload: number; columns: number; runs: number }>`
      SELECT length(CAST(payload_json AS BLOB)) AS payload,
        json_array_length(payload_json, '$.runs') AS runs,
        length(CAST(event_id AS BLOB)) + length(CAST(aggregate_kind AS BLOB))
          + length(CAST(stream_id AS BLOB)) + length(CAST(event_type AS BLOB))
          + length(CAST(occurred_at AS BLOB)) + coalesce(length(CAST(command_id AS BLOB)), 0)
          + coalesce(length(CAST(causation_event_id AS BLOB)), 0)
          + coalesce(length(CAST(correlation_id AS BLOB)), 0) + length(CAST(actor_kind AS BLOB))
          + length(CAST(payload_json AS BLOB)) + length(CAST(metadata_json AS BLOB)) + 16 AS columns
      FROM orchestration_events
      WHERE event_type = 'scheduler.state-set' AND stream_id = ${created.id}`;
    const serialized = (yield* Stream.runCollect(engine.readEvents(0)))
      .filter((event) => event.type === "scheduler.state-set" && event.aggregateId === created.id)
      .map((event) => Buffer.byteLength(eventJson(event)));
    const receipts = yield* sql<{ columns: number }>`
      SELECT length(CAST(command_id AS BLOB)) + length(CAST(aggregate_kind AS BLOB))
        + length(CAST(aggregate_id AS BLOB)) + length(CAST(accepted_at AS BLOB))
        + length(CAST(status AS BLOB)) + coalesce(length(CAST(error AS BLOB)), 0) + 8 AS columns
      FROM orchestration_command_receipts WHERE aggregate_id = ${created.id}`;
    const after = yield* tablePages;
    const growth = (table: string) => (after[table] ?? 0) - (before[table] ?? 0);
    const payload = sum(events.map((event) => event.payload));
    const eventColumns = sum(events.map((event) => event.columns));
    const receiptColumns = sum(receipts.map((receipt) => receipt.columns));
    const report = {
      intervalMinutes: 5,
      runs: CHATTY_RUNS,
      outputBytesPerRun: 20_000,
      retainedRuns: task.runs.length,
      retainedFailedRuns: task.runs.filter((run) => run.status === "needs-you").length,
      retainedOutputs: task.runs.filter((run) => run.commandResult?.output !== undefined).length,
      stateEvents: events.length,
      stateEventRuns: sum(events.map((event) => event.runs)),
      projectedStateEventsPerDay: perDay(events.length),
      largestStateEventPayloadBytes: Math.max(0, ...events.map((event) => event.payload)),
      totalStateEventPayloadBytes: payload,
      projectedStateEventPayloadBytesPerDay: perDay(payload),
      totalStateEventColumnBytes: eventColumns,
      projectedStateEventColumnBytesPerDay: perDay(eventColumns),
      largestSerializedStateEventBytes: Math.max(0, ...serialized),
      totalSerializedStateEventBytes: sum(serialized),
      projectedSerializedStateEventBytesPerDay: perDay(sum(serialized)),
      receipts: receipts.length,
      largestReceiptColumnBytes: Math.max(0, ...receipts.map((receipt) => receipt.columns)),
      totalReceiptColumnBytes: receiptColumns,
      projectedReceiptsPerDay: perDay(receipts.length),
      projectedReceiptColumnBytesPerDay: perDay(receiptColumns),
      projectedPermanentColumnBytesPerDay: perDay(eventColumns + receiptColumns),
      ...(reportPrefix === undefined
        ? {}
        : {
            eventTablePageBytesGrowth: growth("orchestration_events"),
            receiptTablePageBytesGrowth: growth("orchestration_command_receipts"),
            runtimeTablePageBytes: growth("fork_scheduler_task_state"),
          }),
    };
    return { scheduler, task, report };
  });
/** The task's permanent state events, oldest first. */
const stateEvents = (taskId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ payload: string }>`
      SELECT payload_json AS payload FROM orchestration_events
      WHERE event_type = 'scheduler.state-set' AND stream_id = ${taskId} ORDER BY sequence`;
    return rows.map((row) => decodeJsonTask(row.payload));
  });
/** The task's runtime row as stored. */
const runtimeRow = (taskId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ bytes: number; state: string }>`
      SELECT length(task_id) + length(state_json) + 8 AS bytes, state_json AS state
      FROM fork_scheduler_task_state WHERE task_id = ${taskId}`;
    return {
      rows: rows.length,
      bytes: rows[0]?.bytes ?? 0,
      task: rows[0] === undefined ? undefined : decodeJsonTask(rows[0].state),
    };
  });

describe("scheduled command tasks", () => {
  it.effect(
    "runs once per slot with its result; a failure needs you until a later run passes",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-");
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const created = yield* scheduler.create(
              command(
                `${RECORD}; for i in $(seq 1 600); do echo "line-$i"; done; echo boom >&2; exit 3`,
              ),
              "user:creator",
            );
            expect(created).toMatchObject({
              createdBy: "user:creator",
              owner: "Luke",
              checks: [],
              runs: [],
              nextRunAt: "2026-01-01T01:00:00.000Z",
            });
            // Creating a command task runs nothing.
            expect(yield* executions(directory)).toEqual([]);
            yield* TestClock.adjust("60 minutes");
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const failed = yield* only(scheduler);
            const first = failed.runs[0]!;
            expect(first).toMatchObject({
              id: `${created.id}:2026-01-01T01:00:00.000Z`,
              status: "needs-you",
              threadId: null,
              dispatchedAt: "2026-01-01T01:00:00.000Z",
              error: "The command exited with code 3.",
              commandResult: { exitCode: 3, timedOut: false, endedAt: "2026-01-01T01:00:00.000Z" },
            });
            expect(first.checkVersion).toBeUndefined();
            const output = first.commandResult!.output!;
            expect(Buffer.byteLength(output)).toBeLessThanOrEqual(COMMAND_OUTPUT_BYTES);
            expect(output).toContain("line-600\n");
            expect(output).toContain("boom\n");
            expect(output).not.toMatch(/^line-1$/m);
            expect(failed).toMatchObject({
              failureStreak: 1,
              lastError: "The command exited with code 3.",
              nextRunAt: "2026-01-01T02:00:00.000Z",
            });
            expect(yield* executions(directory)).toEqual([first.id]);
            expect(commandFailureToNotify(failed)).toEqual(first);
            const snapshots = yield* ProjectionSnapshotQuery;
            expect(
              Option.isNone(
                yield* snapshots.getThreadShellById(ThreadId.make(`scheduled-thread-${first.id}`)),
              ),
            ).toBe(true);
            // Reconciling again in the same slot neither repeats nor resumes the failed run.
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            expect(yield* executions(directory)).toEqual([first.id]);
            // A failing task keeps its schedule; the next failure extends the streak without a new alert.
            yield* TestClock.adjust("60 minutes");
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const failing = yield* only(scheduler);
            expect(failing.runs.map((run) => run.status)).toEqual(["needs-you", "needs-you"]);
            expect(failing.failureStreak).toBe(2);
            expect(commandFailureToNotify(failing)).toBeNull();

            yield* scheduler.edit(
              { taskId: created.id, definition: command(`${RECORD}; printf fixed`) },
              "user:creator",
            );
            yield* TestClock.adjust("60 minutes");
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const healed = yield* only(scheduler);
            expect(healed.runs[0]).toEqual(first);
            expect(healed.runs[2]).toMatchObject({
              id: `${created.id}:2026-01-01T03:00:00.000Z`,
              status: "done",
              error: null,
              commandResult: { exitCode: 0, output: "fixed", timedOut: false },
            });
            expect(healed).toMatchObject({ failureStreak: 0, lastError: null });
            expect(commandFailureToNotify(healed)).toBeNull();
            expect(yield* executions(directory)).toEqual(healed.runs.map((run) => run.id));
          }),
        );
      }),
  );

  it.effect(
    "never runs an admitted command again after a restart; the latest missed slot runs once",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-restart-");
        const started = NodePath.join(directory, "started.fifo");
        fifo(started);
        let admitted: TaskRun | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            yield* scheduler.create(
              command(
                `${RECORD}; if [ -p started.fifo ]; then echo started > started.fifo; exec sleep 600; fi`,
              ),
              "user:creator",
            );
            yield* TestClock.adjust("60 minutes");
            yield* scheduler.reconcile();
            // The process reports it is running. Its admission is already durable state.
            yield* Effect.promise(() => NodeFSP.readFile(started, "utf8"));
            admitted = (yield* only(scheduler)).runs[0];
            expect(admitted).toMatchObject({
              status: "running",
              dispatchedAt: "2026-01-01T01:00:00.000Z",
            });
            expect(admitted!.commandResult).toBeUndefined();
          }),
        );
        yield* Effect.promise(() => NodeFSP.rm(started));
        yield* within(
          directory,
          Effect.gen(function* () {
            const scheduler = yield* makeLiveScheduler;
            const replayed = yield* only(scheduler);
            expect(replayed.runs).toEqual([admitted]);
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const interrupted = yield* only(scheduler);
            expect(interrupted.runs).toHaveLength(1);
            expect(interrupted.runs[0]).toMatchObject({ id: admitted!.id, status: "needs-you" });
            expect(interrupted.runs[0]!.error).toContain("may or may not have finished");
            expect(interrupted.runs[0]!.commandResult).toBeUndefined();
            expect(interrupted.failureStreak).toBe(1);
            expect(yield* executions(directory)).toEqual([admitted!.id]);

            yield* TestClock.adjust("180 minutes");
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const latest = `${interrupted.id}:2026-01-01T04:00:00.000Z`;
            const caughtUp = yield* only(scheduler);
            expect(caughtUp.runs.map((run) => [run.id, run.status])).toEqual([
              [admitted!.id, "needs-you"],
              [latest, "done"],
            ]);
            expect(caughtUp).toMatchObject({ failureStreak: 0, lastError: null });
            expect(yield* executions(directory)).toEqual([admitted!.id, latest]);
          }),
        );
      }),
  );

  it.effect("manages a running command like an agent task, without a check or a kind change", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-manage-");
      const gate = NodePath.join(directory, "gate.fifo");
      fifo(gate);
      const release = Effect.promise(() => NodeFSP.writeFile(gate, "go"));
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const created = yield* scheduler.create(
            command(`cat gate.fifo > /dev/null; ${RECORD}`, 1440),
            "user:creator",
          );
          const running = yield* scheduler.runNow(created.id);
          expect(running.runs[0]).toMatchObject({ status: "running", threadId: null });
          for (const refused of [
            scheduler.delete(created.id, "user:creator"),
            scheduler.runNow(created.id),
            scheduler.edit(
              { taskId: created.id, checkCommand: "true", checkReason: "Not for commands" },
              "user:creator",
            ),
            scheduler.edit(
              {
                taskId: created.id,
                definition: {
                  title: "Agent",
                  prompt: "Work",
                  target: { kind: "new-thread", projectId: PROJECT_ID },
                  role: "worker",
                  schedule: { kind: "interval", minutes: 1440 },
                },
              },
              "user:creator",
            ),
          ])
            expect(yield* Effect.flip(refused)).toBeInstanceOf(SchedulerError);
          // Pausing keeps the running command; its result is still recorded.
          expect((yield* scheduler.pause(created.id, true, "user:creator")).nextRunAt).toBeNull();
          yield* release;
          yield* scheduler.drainCommands;
          const paused = yield* only(scheduler);
          expect(paused).toMatchObject({ paused: true, runs: [{ status: "done" }] });
          expect(yield* Effect.flip(scheduler.runNow(created.id))).toBeInstanceOf(SchedulerError);
          yield* scheduler.pause(created.id, false, "user:creator");
          // Run now claims the current instant; a later instant is a new run.
          yield* TestClock.adjust("1 minute");
          yield* scheduler.runNow(created.id);
          yield* release;
          yield* scheduler.drainCommands;
          expect((yield* only(scheduler)).runs.map((run) => run.status)).toEqual(["done", "done"]);
          expect(yield* executions(directory)).toHaveLength(2);
          yield* scheduler.delete(created.id, "user:creator");
          expect(yield* scheduler.list).toEqual([]);
        }),
      );
    }),
  );

  it.effect("records the last passing run durably, so a pass between two samples is visible", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-success-");
      let kept: ScheduledTask | undefined;
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const created = yield* scheduler.create(command("exit 3"), "user:creator");
          const runAs = (text: string) =>
            Effect.gen(function* () {
              if (text !== "") {
                yield* scheduler.edit(
                  { taskId: created.id, definition: command(text) },
                  "user:creator",
                );
              }
              yield* TestClock.adjust("60 minutes");
              yield* scheduler.reconcile();
              yield* scheduler.drainCommands;
              return yield* only(scheduler);
            });
          // A first-ever failure has no passing run.
          const firstFailure = yield* runAs("");
          expect(firstFailure.failureStreak).toBe(1);
          expect(firstFailure.lastSuccessfulRunId).toBeUndefined();
          const pass = (yield* runAs("true")).runs.at(-1)!;
          const failedAgain = yield* runAs("exit 3");
          // Pass then fail: the streak is 1 again, as before the pass; only the marker shows it.
          expect(failedAgain.failureStreak).toBe(firstFailure.failureStreak);
          expect(failedAgain.lastSuccessfulRunId).toBe(pass.id);
          // Failures, pause and edits keep the marker; compact responses carry it.
          yield* runAs("");
          yield* scheduler.pause(created.id, true, "user:creator");
          yield* scheduler.pause(created.id, false, "user:creator");
          yield* scheduler.edit(
            { taskId: created.id, definition: command("exit 4") },
            "user:creator",
          );
          const failing = yield* only(scheduler);
          expect(failing).toMatchObject({ failureStreak: 2, lastSuccessfulRunId: pass.id });
          expect((yield* scheduler.listCompact)[0]!.lastSuccessfulRunId).toBe(pass.id);

          const later = (yield* runAs("true")).runs.at(-1)!;
          const current = yield* only(scheduler);
          expect(current.lastSuccessfulRunId).toBe(later.id);
          const { lastSuccessfulRunId: _, ...cleared } = persisted(current);
          const failedRun = current.runs.find((run) => run.status === "needs-you")!;
          // Only the server's settle of a passing run may move it: not clearing, not a failed
          // run, not an earlier pass, not an unknown id.
          for (const forged of [
            { ...cleared, revision: current.revision },
            { ...persisted(current), lastSuccessfulRunId: failedRun.id },
            { ...persisted(current), lastSuccessfulRunId: pass.id },
            { ...persisted(current), lastSuccessfulRunId: "invented" },
          ])
            expect(Exit.isFailure(yield* forge(forged, {}))).toBe(true);
          kept = persisted(yield* only(scheduler));
        }),
      );
      // A restart restores the marker from the runtime row.
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          expect(persisted(yield* only(scheduler))).toEqual(kept);
          expect(kept!.lastSuccessfulRunId).toBeDefined();
        }),
      );
    }),
  );

  it.effect(
    "keeps output for the newest runs only and appends no events for a chatty five-minute command",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-retention-");
        yield* within(
          directory,
          Effect.gen(function* () {
            // Every fourth run fails.
            const { scheduler, task, report } = yield* chattyCommand(
              directory,
              "[ $((n % 4)) -ne 0 ]",
            );
            expect(yield* executions(directory)).toHaveLength(CHATTY_RUNS);
            expect(task.runs).toHaveLength(20);
            const kept = task.runs.slice(-COMMAND_OUTPUTS_KEPT);
            const older = task.runs.slice(0, -COMMAND_OUTPUTS_KEPT);
            for (const run of kept)
              expect(Buffer.byteLength(run.commandResult!.output!)).toBe(COMMAND_OUTPUT_BYTES);
            for (const run of older) {
              expect(run.commandResult!.output).toBeUndefined();
              expect(run.commandResult!.exitCode).toBe(run.status === "done" ? 0 : 1);
              expect(run.commandResult!.endedAt).toBe(run.dispatchedAt);
            }
            expect(task.runs.filter((run) => run.status === "needs-you")).toHaveLength(5);
            // The user's budget: well under 1 MB of permanent event payload per day.
            expect(report.projectedStateEventPayloadBytesPerDay).toBeLessThan(100_000);
            // Admissions, results and health live in the task's runtime row; only creation is audited.
            expect(report.stateEvents).toBe(1);
            expect(report.stateEventRuns).toBe(0);
            expect(report.projectedPermanentColumnBytesPerDay).toBeLessThan(1_000_000);
            const row = yield* runtimeRow(task.id);
            expect(row.task).toEqual(persisted(task));
            // Twenty runs of metadata plus three 4 KiB tails, not twenty tails.
            expect(row.bytes).toBeLessThan(40_000);

            // The RPC chooses the view; compact is a response copy and leaves stored state alone.
            const api = makeSchedulerRpcHandlers(
              scheduler,
              { subject: "creator", sessionId: AuthSessionId.make("command-sizes") },
              { getPerson: () => Effect.succeed(null) },
              (_, effect) => effect,
            );
            const full = yield* api["scheduler.list"]({});
            const compact = yield* api["scheduler.list"]({ compact: true });
            const { output: _newestOutput, ...newestMetadata } = task.runs.at(-1)!.commandResult!;
            expect(full).toEqual([task]);
            expect(compact).toEqual([
              {
                ...task,
                checks: [],
                runs: [{ ...task.runs.at(-1)!, commandResult: newestMetadata }],
              },
            ]);
            expect(yield* only(scheduler)).toEqual(task);
            const fullBytes = Buffer.byteLength(listJson(full));
            const compactBytes = Buffer.byteLength(listJson(compact));
            yield* writeReport("every-fourth-fails", {
              ...report,
              runtimeRows: row.rows,
              runtimeRowBytes: row.bytes,
              fullListBytesPerTask: fullBytes,
              compactListBytesPerTask: compactBytes,
              fullListBytesPerTaskPerDayEvery15s: fullBytes * 5_760,
              compactListBytesPerTaskPerDayEvery60s: compactBytes * 1_440,
            });
            expect(fullBytes).toBeLessThan(40_000);
            expect(compactBytes).toBeLessThan(3_000);

            // The decider accepts only the scheduler's own trimming of settled runs.
            const newest = task.runs.at(-1)!;
            const oldest = task.runs[0]!;
            const { output: _, ...withoutOutput } = newest.commandResult!;
            for (const patch of [
              { runs: [...task.runs.slice(0, -1), { ...newest, commandResult: withoutOutput }] },
              {
                runs: [
                  { ...oldest, commandResult: { ...oldest.commandResult!, exitCode: 9 } },
                  ...task.runs.slice(1),
                ],
              },
              { runs: [...task.runs.slice(0, -1), { ...newest, status: "needs-you" as const }] },
              { createdBy: "user:someone-else" },
              {
                checks: [
                  {
                    version: 1,
                    command: "true",
                    actor: "user:creator",
                    reason: "Commands have no check",
                    createdAt: NOW,
                    revertedFrom: null,
                  },
                ],
              },
            ])
              expect(Exit.isFailure(yield* forge(persisted(task), patch))).toBe(true);
            const failedRun = task.runs.findLast((run) => run.status === "needs-you")!;
            expect(
              Exit.isFailure(
                yield* forge(persisted(task), {
                  runs: task.runs.map((run) =>
                    run.id === failedRun.id ? { ...run, status: "done" as const } : run,
                  ),
                }),
              ),
            ).toBe(true);
            // Evicting old settled runs stays allowed.
            expect(Exit.isSuccess(yield* forge(persisted(task), { runs: kept }))).toBe(true);
          }),
        );
      }),
  );

  it.effect("alternating failures and passes alert each time and append no health events", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-alternating-");
      yield* within(
        directory,
        Effect.gen(function* () {
          // Odd runs fail, even runs pass.
          const { task, report } = yield* chattyCommand(directory, "[ $((n % 2)) -eq 0 ]");
          // The newest 20 of 25 runs are runs 6 to 25.
          expect(task.runs.map((run) => run.status)).toEqual(
            Array.from({ length: 20 }, (_, index) => (index % 2 === 0 ? "done" : "needs-you")),
          );
          expect(task).toMatchObject({
            failureStreak: 1,
            lastSuccessfulRunId: task.runs.at(-2)!.id,
          });
          expect(commandFailureToNotify(task)).toEqual(task.runs.at(-1));
          expect(report.projectedStateEventPayloadBytesPerDay).toBeLessThan(100_000);
          expect(report.stateEvents).toBe(1);
          expect(report.stateEventRuns).toBe(0);
          expect(report.projectedPermanentColumnBytesPerDay).toBeLessThan(1_000_000);
          const row = yield* runtimeRow(task.id);
          expect(row.task).toEqual(persisted(task));
          expect(row.bytes).toBeLessThan(40_000);
          yield* writeReport("alternating", {
            ...report,
            runtimeRows: row.rows,
            runtimeRowBytes: row.bytes,
          });
        }),
      );
    }),
  );

  it.effect("agents create command tasks through MCP as themselves", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-mcp-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory, "full-access", "Pauli");
          const scheduler = yield* makeLiveScheduler;
          const result = yield* Effect.gen(function* () {
            const toolkit = yield* SchedulerToolkit;
            return yield* toolkit
              .handle("create_scheduled_command", {
                title: "Agent-made export",
                projectId: PROJECT_ID,
                command: RECORD,
                schedule: { kind: "interval", minutes: 60 },
                createdBy: "user:forged",
              } as never)
              .pipe(Stream.unwrap, Stream.runCollect);
          }).pipe(
            Effect.provide(SchedulerToolkitHandlersLive),
            Effect.provideService(Scheduler, scheduler),
            Effect.provideService(McpInvocationContext, {
              environmentId: EnvironmentId.make("isolated-mcp"),
              threadId: PARENT_ID,
              providerSessionId: "test",
              providerInstanceId: ProviderInstanceId.make("codex"),
              capabilities: new Set<McpCapability>(),
              issuedAt: 1,
            }),
          );
          expect(result.at(-1)!.result).toMatchObject({
            definition: { kind: "command", command: RECORD },
            createdBy: PARENT_ID,
            owner: "Pauli",
            checks: [],
          });
          expect(yield* executions(directory)).toEqual([]);
        }),
      );
    }),
  );

  it("reads tasks stored before command tasks existed unchanged", () => {
    const legacy = {
      checkCwd: "/work",
      id: "scheduled-legacy",
      revision: 3,
      definition: {
        title: "Nightly",
        prompt: "Do the work",
        target: { kind: "new-thread", projectId: "project" },
        role: "worker",
        schedule: { kind: "interval", minutes: 60 },
      },
      createdAt: NOW,
      updatedAt: NOW,
      paused: false,
      deleted: false,
      checks: [
        {
          version: 1,
          command: "test -f done",
          actor: "user:legacy",
          reason: "Proof",
          createdAt: NOW,
          revertedFrom: null,
        },
      ],
      choices: [],
      consumedSlot: NOW,
      runs: [
        {
          definition: {
            title: "Nightly",
            prompt: "Do the work",
            target: { kind: "new-thread", projectId: "project" },
            role: "worker",
            schedule: { kind: "interval", minutes: 60 },
          },
          checkCwd: "/work",
          id: `scheduled-legacy:${NOW}`,
          slot: NOW,
          checkVersion: 1,
          threadId: "scheduled-thread-legacy",
          status: "needs-you",
          processId: "old",
          originSequence: 4,
          sendIndex: 1,
          attempt: 5,
          hasWork: true,
          leaseUntil: 0,
          retryAt: null,
          dispatchedAt: NOW,
          observedTurnId: null,
          error: "Ladder exhausted",
          check: { version: 1, passed: false, output: "missing", checkedAt: NOW },
          drafterIds: [],
        },
      ],
      failureStreak: 5,
      lastError: "Ladder exhausted",
    };
    const task = decodeJsonTask(JSON.stringify(legacy));
    expect(isCommandTask(task.definition)).toBe(false);
    expect(task.createdBy).toBeUndefined();
    expect(task.runs[0]!.checkVersion).toBe(1);
    // An exhausted agent run still holds its task and resumes on the next slot.
    expect(isSettledRun(task.runs[0]!)).toBe(false);
    expect(JSON.parse(encodeJsonTask(task))).toEqual(legacy);
  });
});

describe("scheduled command task runtime state", () => {
  it.effect(
    "audits configuration without runs and keeps routine state, deletion included, in the row",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-audit-");
        let kept: ScheduledTask | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const created = yield* scheduler.create(
              command(`${RECORD}; printf out; exit 3`),
              "user:creator",
            );
            const runOnce = Effect.gen(function* () {
              yield* TestClock.adjust("60 minutes");
              yield* scheduler.reconcile();
              yield* scheduler.drainCommands;
            });
            yield* runOnce;
            yield* scheduler.edit(
              { taskId: created.id, definition: command(`${RECORD}; printf ok`) },
              "user:creator",
            );
            yield* runOnce;
            yield* scheduler.pause(created.id, true, "user:creator");
            yield* scheduler.pause(created.id, false, "user:creator");
            kept = persisted(yield* only(scheduler));
            expect(kept.runs.map((run) => [run.status, run.commandResult?.output])).toEqual([
              ["needs-you", "out"],
              ["done", "ok"],
            ]);
            // Creation, the edit, pause and resume are audited; the four run writes are not.
            expect(
              (yield* stateEvents(created.id)).map((event) => [
                isCommandTask(event.definition) ? event.definition.command : null,
                event.paused,
                event.runs.length,
              ]),
            ).toEqual([
              [`${RECORD}; printf out; exit 3`, false, 0],
              [`${RECORD}; printf ok`, false, 0],
              [`${RECORD}; printf ok`, true, 0],
              [`${RECORD}; printf ok`, false, 0],
            ]);
            expect((yield* runtimeRow(created.id)).task).toEqual(kept);
          }),
        );
        yield* within(
          directory,
          Effect.gen(function* () {
            const scheduler = yield* makeLiveScheduler;
            expect(persisted(yield* only(scheduler))).toEqual(kept);
            yield* scheduler.delete(kept!.id, "user:creator");
            expect(yield* scheduler.list).toEqual([]);
            expect((yield* stateEvents(kept!.id)).at(-1)).toMatchObject({
              deleted: true,
              runs: [],
            });
            // The tombstone keeps its creator and its recent runs, outputs included.
            expect((yield* runtimeRow(kept!.id)).task).toMatchObject({
              deleted: true,
              createdBy: "user:creator",
              runs: kept!.runs,
            });
          }),
        );
        yield* within(
          directory,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const scheduler = yield* makeLiveScheduler;
            expect(yield* scheduler.list).toEqual([]);
            expect(
              (yield* engine.getScheduledTasks!).find((task) => task.id === kept!.id),
            ).toMatchObject({ deleted: true, runs: kept!.runs });
            yield* TestClock.adjust("60 minutes");
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            expect(yield* executions(directory)).toHaveLength(2);
          }),
        );
      }),
  );

  it.effect(
    "answers a repeated runtime write from its receipt and refuses stale or colliding ones",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-receipts-");
        let write: ReturnType<typeof stateCommand> | undefined;
        let first: { sequence: number } | undefined;
        let latest: ScheduledTask | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const created = persisted(yield* scheduler.create(command("true"), "user:creator"));
            write = stateCommand(created, { consumedSlot: NOW }, CommandId.make("runtime-write"));
            first = yield* engine.dispatch(write);
            expect(yield* engine.dispatch(write)).toEqual(first);
            expect((yield* runtimeRow(created.id)).task).toEqual(write.task);
            // A later write moves on; replaying the earlier command does not rewind it.
            const later = stateCommand(write.task, { consumedSlot: "2026-01-01T00:05:00.000Z" });
            yield* engine.dispatch(later);
            expect(yield* engine.dispatch(write)).toEqual(first);
            latest = later.task;
            expect((yield* runtimeRow(created.id)).task).toEqual(latest);
            expect(persisted(yield* only(scheduler))).toEqual(latest);
            expect(yield* stateEvents(created.id)).toHaveLength(1);
            // The same command id aimed at another task is a conflict, not a replay.
            const other = {
              ...write,
              threadId: ThreadId.make("scheduled-other"),
              task: { ...write.task, id: "scheduled-other" },
            };
            expect(yield* Effect.flip(engine.dispatch(other))).toMatchObject({
              _tag: "OrchestrationCommandIdConflictError",
            });
            // A stale revision is refused, remembered as refused, and changes nothing.
            const stale = stateCommand(
              write.task,
              { failureStreak: 7 },
              CommandId.make("runtime-stale"),
            );
            expect(yield* Effect.flip(engine.dispatch(stale))).toMatchObject({
              _tag: "OrchestrationCommandInvariantError",
            });
            expect(yield* Effect.flip(engine.dispatch(stale))).toMatchObject({
              _tag: "OrchestrationCommandPreviouslyRejectedError",
            });
            expect((yield* runtimeRow(created.id)).task).toEqual(latest);
          }),
        );
        // After a restart, as after a crash between commit and response, a retry is still a replay.
        yield* within(
          directory,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            expect(yield* engine.dispatch(write!)).toEqual(first);
            expect((yield* runtimeRow(latest!.id)).task).toEqual(latest);
          }),
        );
      }),
  );

  it.effect("rolls back the row, its audit event and its receipt together", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-rollback-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const engine = yield* OrchestrationEngineService;
          const sql = yield* SqlClient.SqlClient;
          const task = persisted(yield* scheduler.create(command("true"), "user:creator"));
          const fails = (patch: Partial<ScheduledTask>, id: string) =>
            engine
              .dispatch(stateCommand(task, patch, CommandId.make(id)))
              .pipe(Effect.exit, Effect.map(Exit.isFailure));
          const unchanged = Effect.gen(function* () {
            expect(yield* stateEvents(task.id)).toHaveLength(1);
            expect((yield* runtimeRow(task.id)).task).toEqual(task);
            expect(persisted(yield* only(scheduler))).toEqual(task);
            expect(
              yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id LIKE 'rollback-%'`,
            ).toEqual([]);
          });
          // The receipt is written last, after the row and any audit event.
          yield* sql`CREATE TRIGGER fail_receipt BEFORE INSERT ON orchestration_command_receipts
            WHEN NEW.command_id LIKE 'rollback-%'
            BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END`;
          expect(yield* fails({ consumedSlot: NOW }, "rollback-routine")).toBe(true);
          expect(yield* fails({ paused: true }, "rollback-config")).toBe(true);
          yield* unchanged;
          yield* sql`DROP TRIGGER fail_receipt`;
          // A failed row write takes the audit event with it.
          yield* sql`CREATE TRIGGER fail_row BEFORE UPDATE ON fork_scheduler_task_state
            BEGIN SELECT RAISE(ABORT, 'injected row failure'); END`;
          expect(yield* fails({ paused: true }, "rollback-row")).toBe(true);
          yield* unchanged;
          yield* sql`DROP TRIGGER fail_row`;
          // The revision is still free.
          yield* engine.dispatch(stateCommand(task, { paused: true }));
          expect((yield* only(scheduler)).paused).toBe(true);
          expect(yield* stateEvents(task.id)).toHaveLength(2);
        }),
      );
    }),
  );

  it.effect("refuses a write from another engine that missed a runtime-only write", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-engines-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const first = yield* OrchestrationEngineService;
          const task = persisted(yield* scheduler.create(command("true"), "user:creator"));
          const ours = stateCommand(task, { consumedSlot: NOW });
          yield* alongside(
            directory,
            Effect.gen(function* () {
              const second = yield* OrchestrationEngineService;
              expect((yield* second.getScheduledTasks!).map(persisted)).toEqual([task]);
              yield* first.dispatch(ours);
              // Only the runtime row holds the first engine's write; the revision check reads it.
              expect(yield* stateEvents(task.id)).toHaveLength(1);
              const theirs = stateCommand(task, { consumedSlot: "2026-01-01T00:05:00.000Z" });
              expect(yield* Effect.flip(second.dispatch(theirs))).toMatchObject({
                _tag: "OrchestrationCommandInvariantError",
                detail: "Durable scheduled task claim lost its revision race.",
              });
            }),
          );
          expect((yield* runtimeRow(task.id)).task).toEqual(ours.task);
        }),
      );
    }),
  );

  it.effect("lets a newer audit event defeat an older runtime row at startup", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-stale-row-");
      let older: { revision: number; state: string } | undefined;
      let taskId = "";
      // An older snapshot next to a newer audit event, as a lost row write would leave it.
      const restoreOlderRow = within(
        directory,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE fork_scheduler_task_state
            SET revision = ${older!.revision}, state_json = ${older!.state} WHERE task_id = ${taskId}`;
        }),
      );
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const sql = yield* SqlClient.SqlClient;
          taskId = (yield* scheduler.create(command(RECORD), "user:creator")).id;
          yield* TestClock.adjust("60 minutes");
          yield* scheduler.reconcile();
          yield* scheduler.drainCommands;
          [older] = yield* sql<{ revision: number; state: string }>`
            SELECT revision, state_json AS state FROM fork_scheduler_task_state
            WHERE task_id = ${taskId}`;
          yield* scheduler.edit(
            { taskId, definition: command(`${RECORD}; printf edited`) },
            "user:creator",
          );
        }),
      );
      yield* restoreOlderRow;
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          const task = yield* only(scheduler);
          expect(task.definition).toEqual(command(`${RECORD}; printf edited`));
          expect(task.revision).toBe(older!.revision + 1);
          yield* scheduler.delete(taskId, "user:creator");
        }),
      );
      yield* restoreOlderRow;
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          expect(yield* scheduler.list).toEqual([]);
          yield* TestClock.adjust("60 minutes");
          yield* scheduler.reconcile();
          yield* scheduler.drainCommands;
          expect(yield* executions(directory)).toHaveLength(1);
        }),
      );
    }),
  );

  it.effect("continues a task stored only as full events by a build without runtime rows", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-legacy-");
      let legacy: ScheduledTask | undefined;
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const sql = yield* SqlClient.SqlClient;
          const created = yield* scheduler.create(command(RECORD), "user:creator");
          yield* TestClock.adjust("60 minutes");
          yield* scheduler.reconcile();
          yield* scheduler.drainCommands;
          legacy = persisted(yield* only(scheduler));
          // What that build stored: the full task, runs included, in its newest event, and no row.
          yield* sql`
            INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version,
              event_type, occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
              payload_json, metadata_json)
            SELECT 'legacy-event', aggregate_kind, stream_id, stream_version + 1, event_type,
              occurred_at, 'legacy-command', NULL, NULL, actor_kind, ${encodeJsonTask(legacy)},
              metadata_json
            FROM orchestration_events WHERE stream_id = ${created.id}`;
          yield* sql`DELETE FROM fork_scheduler_task_state`;
        }),
      );
      let continued: ScheduledTask | undefined;
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          expect(persisted(yield* only(scheduler))).toEqual(legacy);
          yield* TestClock.adjust("60 minutes");
          yield* scheduler.reconcile();
          yield* scheduler.drainCommands;
          continued = persisted(yield* only(scheduler));
          expect(continued.runs.slice(0, 1)).toEqual(legacy!.runs);
          expect(continued.runs).toHaveLength(2);
          expect((yield* runtimeRow(continued.id)).task).toEqual(continued);
        }),
      );
      // The new row now outranks the legacy event.
      yield* within(
        directory,
        Effect.gen(function* () {
          const scheduler = yield* makeLiveScheduler;
          expect(persisted(yield* only(scheduler))).toEqual(continued);
          expect(yield* executions(directory)).toHaveLength(2);
        }),
      );
    }),
  );

  it.effect("refuses kind and creator changes without touching the runtime row", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(NOW));
      const directory = yield* temporaryDirectory("scheduler-command-identity-");
      yield* within(
        directory,
        Effect.gen(function* () {
          yield* createParent(directory);
          const scheduler = yield* makeLiveScheduler;
          const engine = yield* OrchestrationEngineService;
          const agent = persisted(
            yield* scheduler.create(
              {
                title: "Agent",
                prompt: "Work",
                target: { kind: "new-thread", projectId: PROJECT_ID },
                role: "worker",
                schedule: { kind: "interval", minutes: 60 },
                checkCommand: "test -f never",
                checkReason: "Proof",
              },
              "user:creator",
            ),
          );
          const asCommand = { definition: command("true"), checks: [] };
          // An agent task cannot become a command task, nor be created over as one.
          expect(Exit.isFailure(yield* forge(agent, asCommand))).toBe(true);
          const overwrite = {
            ...stateCommand(agent, asCommand),
            expectedRevision: 0,
            task: { ...agent, ...asCommand, revision: 1 },
          };
          expect(Exit.isFailure(yield* engine.dispatch(overwrite).pipe(Effect.exit))).toBe(true);
          expect((yield* runtimeRow(agent.id)).rows).toBe(0);
          const task = persisted(yield* scheduler.create(command("true"), "user:creator"));
          for (const patch of [{ createdBy: PARENT_ID }, { owner: "Pauli" }])
            expect(Exit.isFailure(yield* forge(task, { consumedSlot: NOW, ...patch }))).toBe(true);
          expect((yield* runtimeRow(task.id)).task).toEqual(task);
          const listed = (yield* scheduler.list).map(persisted);
          expect(listed).toHaveLength(2);
          expect(listed).toContainEqual(agent);
          expect(listed).toContainEqual(task);
        }),
      );
    }),
  );

  it.effect(
    "never runs an admission committed just before a crash; replaying it changes nothing",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-crash-");
        const slot = "2026-01-01T01:00:00.000Z";
        let admit: ReturnType<typeof stateCommand> | undefined;
        let committed: { sequence: number } | undefined;
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const engine = yield* OrchestrationEngineService;
            const task = persisted(yield* scheduler.create(command(RECORD), "user:creator"));
            const run: TaskRun = {
              owner: threadOwner(task),
              definition: task.definition,
              checkCwd: task.checkCwd,
              id: `${task.id}:${slot}`,
              slot,
              threadId: null,
              status: "running",
              processId: "crashed-process",
              originSequence: 0,
              sendIndex: 0,
              attempt: 0,
              hasWork: false,
              leaseUntil: Date.parse(slot) + 120_000,
              retryAt: null,
              dispatchedAt: slot,
              observedTurnId: null,
              error: null,
              check: null,
              drafterIds: [],
            };
            admit = stateCommand(
              task,
              { consumedSlot: slot, runs: [run] },
              CommandId.make("crash-admission"),
            );
            committed = yield* engine.dispatch(admit);
            // The process stops here: before the scheduler hears back and before anything spawns.
          }),
        );
        yield* TestClock.adjust("60 minutes");
        yield* within(
          directory,
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const scheduler = yield* makeLiveScheduler;
            expect(yield* engine.dispatch(admit!)).toEqual(committed);
            expect(persisted(yield* only(scheduler))).toEqual(admit!.task);
            yield* scheduler.reconcile();
            yield* scheduler.drainCommands;
            const settled = yield* only(scheduler);
            expect(settled.runs).toHaveLength(1);
            expect(settled.runs[0]).toMatchObject({
              id: `${admit!.task.id}:${slot}`,
              status: "needs-you",
            });
            expect(settled.runs[0]!.error).toContain("may or may not have finished");
            expect(settled.runs[0]!.commandResult).toBeUndefined();
            // Replaying the admission cannot reopen it.
            expect(yield* engine.dispatch(admit!)).toEqual(committed);
            expect(yield* only(scheduler)).toEqual(settled);
            expect(yield* executions(directory)).toEqual([]);
          }),
        );
      }),
  );
});
