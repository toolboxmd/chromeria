// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  COMMAND_OUTPUTS_KEPT,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ScheduledTask,
  ScheduledTaskView,
  SchedulerError,
  ThreadId,
  commandFailureToNotify,
  isCommandTask,
  isSettledRun,
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
const forge = (task: ScheduledTask, patch: Partial<ScheduledTask>) =>
  Effect.flatMap(OrchestrationEngineService, (engine) =>
    engine
      .dispatch({
        type: "scheduler.state.set",
        commandId: commandId(),
        threadId: ThreadId.make(task.id),
        expectedRevision: task.revision,
        createdAt: NOW,
        task: { ...task, ...patch, revision: task.revision + 1 },
      })
      .pipe(Effect.exit),
  );
const encodeTask = Schema.encodeSync(ScheduledTask);
const decodeTask = Schema.decodeUnknownSync(ScheduledTask);
const listJson = Schema.encodeSync(Schema.fromJsonString(Schema.Array(ScheduledTaskView)));
const reportJson = Schema.encodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);
const decodeJsonTask = Schema.decodeUnknownSync(Schema.fromJsonString(ScheduledTask));
const encodeJsonTask = Schema.encodeSync(Schema.fromJsonString(ScheduledTask));
/** The task exactly as the state store holds it, without the computed view fields. */
const persisted = (task: ScheduledTask) => decodeTask(encodeTask(task));

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
      // A restart replays the marker from the event log.
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
    "keeps output for the newest runs only and bounds a chatty five-minute command's state",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(NOW));
        const directory = yield* temporaryDirectory("scheduler-command-retention-");
        yield* within(
          directory,
          Effect.gen(function* () {
            yield* createParent(directory);
            const scheduler = yield* makeLiveScheduler;
            const sql = yield* SqlClient.SqlClient;
            // 20 KB of output per run; every fourth run fails.
            const created = yield* scheduler.create(
              command(
                `${RECORD}; head -c 20000 /dev/zero | tr '\\0' x; n=$(wc -l < runs.log); [ $((n % 4)) -ne 0 ]`,
                5,
              ),
              "user:creator",
            );
            const RUNS = 25;
            for (let index = 0; index < RUNS; index++) {
              yield* TestClock.adjust("5 minutes");
              yield* scheduler.reconcile();
              yield* scheduler.drainCommands;
            }
            const task = yield* only(scheduler);
            expect(yield* executions(directory)).toHaveLength(RUNS);
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

            const rows = yield* sql<{ bytes: number }>`
              SELECT length(payload_json) AS bytes FROM orchestration_events
              WHERE event_type = 'scheduler.state-set' AND stream_id = ${created.id}`;
            const largest = Math.max(...rows.map((row) => row.bytes));
            const total = rows.reduce((sum, row) => sum + row.bytes, 0);
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
            const report = {
              intervalMinutes: 5,
              runs: RUNS,
              outputBytesPerRun: 20_000,
              stateEvents: rows.length,
              largestStateEventBytes: largest,
              totalStateEventBytes: total,
              projectedStateBytesPerDay: Math.round((total / RUNS) * 288),
              fullListBytesPerTask: fullBytes,
              compactListBytesPerTask: compactBytes,
              fullListBytesPerTaskPerDayEvery15s: fullBytes * 5_760,
              compactListBytesPerTaskPerDayEvery60s: compactBytes * 1_440,
            };
            // Set SCHEDULER_SIZE_REPORT to a file path to keep these measurements.
            const reportPath = process.env.SCHEDULER_SIZE_REPORT;
            if (reportPath)
              yield* Effect.promise(() => NodeFSP.writeFile(reportPath, reportJson(report)));
            // Twenty runs of metadata plus three 4 KiB tails, not twenty tails.
            expect(largest).toBeLessThan(40_000);
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
