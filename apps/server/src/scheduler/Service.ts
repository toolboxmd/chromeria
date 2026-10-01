import {
  CommandId,
  DEFAULT_RUNTIME_MODE,
  MessageId,
  SchedulerError,
  TaskCheckVersion,
  ThreadId,
  prismRoleModels,
  type CheckHistoryInput,
  type ScheduledTask,
  type TaskRun,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import { ProcessRunner } from "../processRunner.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { StreamClock } from "../prism/streamClock.ts";
import { makeThreadTurnSender } from "../mcp/toolkits/threads/sendThreadTurn.ts";
import { effortOptionId, pickRoleModel, roleTaskMessage } from "../mcp/toolkits/threads/roles.ts";
import { isUsageLimitError, usageLimitResetAt } from "../mcp/toolkits/threads/usageLimitResume.ts";
import { threadHasQueuedTurnStart } from "../orchestration/ThreadSettlementPolicy.ts";
import { makeScheduler, type RunObservation } from "./Scheduler.ts";
import { iso } from "./Schedule.ts";
import { ServerRuntimeStartup } from "../serverRuntimeStartup.ts";

const decodeCheck = Schema.decodeUnknownEffect(Schema.fromJsonString(TaskCheckVersion));
const error = (cause: unknown) => new SchedulerError({ detail: String(cause) });
export const schedulerIdle = (thread: OrchestrationThreadShell, now: string) =>
  thread.archivedAt === null &&
  !thread.hasPendingApprovals &&
  !thread.hasPendingUserInput &&
  !thread.hasActionableProposedPlan &&
  thread.session?.status !== "running" &&
  thread.session?.status !== "starting" &&
  thread.session?.status !== "stopped" &&
  thread.latestTurn?.state !== "running" &&
  !threadHasQueuedTurnStart(thread, now);
export const CHECK_OUTPUT_BYTES = 16_384;
export const truncateCheckOutput = (text: string) => {
  let output = Buffer.from(text).subarray(0, CHECK_OUTPUT_BYTES).toString("utf8");
  while (Buffer.byteLength(output) > CHECK_OUTPUT_BYTES) output = output.slice(0, -1);
  return output;
};

export const makeLiveScheduler = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  const registry = yield* ProviderRegistry;
  const settings = yield* ServerSettingsService;
  const process = yield* ProcessRunner;
  const clock = yield* Effect.serviceOption(StreamClock);
  const sql = yield* SqlClient.SqlClient;
  const uuid = (yield* Crypto.Crypto).randomUUIDv4.pipe(Effect.orDie);
  const processId = yield* uuid;
  const wakes = yield* Queue.unbounded<void>();
  const wake = Queue.offer(wakes, undefined).pipe(Effect.asVoid);
  const getThread = (id: string) =>
    snapshots
      .getThreadShellById(ThreadId.make(id))
      .pipe(Effect.map(Option.getOrUndefined), Effect.mapError(error));
  const checkHistory = (input: CheckHistoryInput) =>
    Effect.gen(function* () {
      // Versions remain in the append-only log; health carries a bounded window plus pinned judges.
      const rows = yield* sql<{ payload: string }>`
      SELECT checks.value AS payload FROM orchestration_events, json_each(payload_json, '$.checks') AS checks
      WHERE event_type = 'scheduler.state-set' AND stream_id = ${input.taskId}
        AND json_extract(checks.value, '$.version') < ${input.beforeVersion ?? Number.MAX_SAFE_INTEGER}
      GROUP BY json_extract(checks.value, '$.version')
      ORDER BY json_extract(checks.value, '$.version') DESC LIMIT ${input.limit ?? 50}
    `.pipe(Effect.mapError(error));
      return yield* Effect.forEach(rows, (row) =>
        decodeCheck(row.payload).pipe(Effect.mapError(error)),
      );
    });
  const sender = makeThreadTurnSender({
    dispatch: engine.dispatch,
    commandId: uuid.pipe(Effect.map((id) => CommandId.make(`server:scheduler:${id}`))),
    messageId: uuid.pipe(Effect.map(MessageId.make)),
    now: Clock.currentTimeMillis.pipe(Effect.map(iso)),
  });
  const observe = (task: ScheduledTask, run: TaskRun) =>
    Effect.gen(function* (): Effect.fn.Return<RunObservation, SchedulerError> {
      const now = yield* Clock.currentTimeMillis;
      if (run.threadId === null)
        return {
          active: false,
          idle: true,
          retired: false,
          hasWork: false,
          pendingDrafters: false,
          drafterIds: [],
          turnId: null,
          error: null,
          usageLimited: false,
          resetAt: null,
          stalled: false,
        };
      const thread = yield* getThread(run.threadId);
      if (!thread)
        return {
          active: false,
          idle: false,
          retired: run.dispatchedAt !== null,
          hasWork: false,
          pendingDrafters: false,
          drafterIds: [],
          turnId: null,
          error: null,
          usageLimited: false,
          resetAt: null,
          stalled: false,
        };
      const head = yield* engine.latestSequence;
      const events = yield* engine
        .readThreadEvents({
          threadId: run.threadId,
          fromSequenceExclusive: run.originSequence,
          toSequenceInclusive: head,
          limit: Number.MAX_SAFE_INTEGER,
        })
        .pipe(Stream.runCollect, Effect.mapError(error));
      let hasWork = run.hasWork;
      const children = new Map<string, string>();
      for (const event of events) {
        if (
          event.type === "thread.message-sent" &&
          event.payload.role === "assistant" &&
          event.payload.text.length > 0
        )
          hasWork = true;
        if (event.type !== "thread.activity-appended") continue;
        const activity = event.payload.activity;
        if (/^(tool\.|task\.|turn\.plan\.)/.test(activity.kind)) hasWork = true;
        if (
          /^task\.(started|progress|updated|completed)$/.test(activity.kind) &&
          Predicate.isObject(activity.payload) &&
          typeof activity.payload.taskId === "string"
        ) {
          children.set(
            activity.payload.taskId,
            activity.kind === "task.completed"
              ? "idle"
              : typeof activity.payload.status === "string"
                ? activity.payload.status
                : "running",
          );
        }
      }
      const drafterIds = [...new Set([...run.drafterIds, ...children.keys()])].map((id) =>
        ThreadId.make(id),
      );
      let pendingDrafters = false;
      for (const id of drafterIds) {
        const child = yield* getThread(id);
        if (
          child &&
          child.archivedAt === null &&
          !(yield* engine.getThreadRetirement(id))?.retired &&
          ((child.session === null && children.get(id) !== "idle") ||
            child.session?.status === "running" ||
            child.session?.status === "starting" ||
            child.backgroundLiveness === "working" ||
            child.hasPendingApprovals ||
            child.hasPendingUserInput ||
            child.hasActionableProposedPlan ||
            isUsageLimitError(child.session?.lastError) ||
            threadHasQueuedTurnStart(child, iso(now)))
        )
          pendingDrafters = true;
      }
      const provider = (yield* registry.getProviders).find(
        (entry) =>
          entry.instanceId ===
          (thread.session?.providerInstanceId ?? thread.modelSelection.instanceId),
      );
      const lastError = thread.session?.lastError ?? null;
      const exhausted =
        provider?.usageLimits?.windows.some((window) => window.usedPercent >= 100) ?? false;
      const limited = isUsageLimitError(lastError) && (exhausted || provider === undefined);
      const liveness = Option.isSome(clock)
        ? yield* clock.value.liveness(run.threadId)
        : { openTool: null, turn: null, lastStreamAt: null };
      hasWork ||=
        liveness.openTool !== null ||
        (liveness.turn?.firstTokenAt !== null && liveness.turn?.firstTokenAt !== undefined);
      const active = thread.session?.status === "running" || thread.session?.status === "starting";
      const stalled =
        active &&
        now - (liveness.lastStreamAt ?? Date.parse(run.dispatchedAt ?? iso(now))) >=
          (liveness.openTool === null ? 300_000 : 3_600_000);
      return {
        active,
        idle: schedulerIdle(thread, iso(now)),
        retired:
          thread.archivedAt !== null ||
          (yield* engine.getThreadRetirement(thread.id))?.retired === true ||
          thread.session?.status === "stopped",
        hasWork,
        drafterIds,
        pendingDrafters,
        turnId: thread.latestTurn?.turnId ?? null,
        error: lastError,
        usageLimited: limited,
        resetAt: limited ? usageLimitResetAt(provider, now) : null,
        stalled,
      };
    });
  const core = yield* makeScheduler({
    processId,
    tasks: engine.getScheduledTasks ?? Effect.succeed([]),
    uuid,
    sequence: engine.latestSequence,
    wake,
    historyVersion: (id, version) =>
      checkHistory({ taskId: id, beforeVersion: version + 1, limit: 1 }).pipe(
        Effect.map((versions) => versions.find((entry) => entry.version === version)),
      ),
    save: (previous, task) =>
      Effect.gen(function* () {
        return yield* engine
          .dispatch({
            type: "scheduler.state.set",
            commandId: CommandId.make(
              `server:scheduler-state:${task.id}:${task.revision}:${yield* uuid}`,
            ),
            threadId: ThreadId.make(task.id),
            expectedRevision: previous?.revision ?? 0,
            task,
            createdAt: task.updatedAt,
          })
          .pipe(Effect.asVoid, Effect.mapError(error));
      }),
    check: (command, variables) => {
      const expanded = command.replace(/\{(date|run_id|task_id)\}/g, (_, key: string) =>
        key === "date" ? variables.date : key === "run_id" ? variables.runId : variables.taskId,
      );
      return process
        .run({
          command: "/bin/sh",
          args: ["-c", expanded],
          cwd: variables.cwd,
          timeout: "30 seconds",
          maxOutputBytes: CHECK_OUTPUT_BYTES,
          outputMode: "truncate",
          timeoutBehavior: "timedOutResult",
        })
        .pipe(
          Effect.map((result) => ({
            passed: result.code === 0 && !result.timedOut,
            output: truncateCheckOutput(
              `${result.stdout}${result.stderr}${result.timedOut ? "\nOutcome check timed out." : ""}`,
            ),
          })),
          Effect.mapError(error),
        );
    },
    validateTarget: (target) =>
      Effect.gen(function* () {
        const thread = target.kind === "thread" ? yield* getThread(target.threadId) : undefined;
        const projectId = target.kind === "new-thread" ? target.projectId : thread?.projectId;
        if (!projectId) return yield* new SchedulerError({ detail: "Target thread not found." });
        const project = yield* snapshots
          .getProjectShellById(projectId)
          .pipe(Effect.mapError(error));
        if (Option.isNone(project))
          return yield* new SchedulerError({ detail: "Target project not found." });
        return thread?.worktreePath ?? project.value.workspaceRoot;
      }),
    observe,
    prepare: (task, run) =>
      Effect.gen(function* () {
        const id = run.threadId ?? ThreadId.make(`scheduled-thread-${run.id}`);
        const existing = yield* getThread(id);
        if (run.hasWork && existing && run.attempt === 0) return id;
        const projectId =
          task.definition.target.kind === "new-thread"
            ? task.definition.target.projectId
            : existing?.projectId;
        if (!projectId)
          return yield* Effect.fail(new SchedulerError({ detail: "Target project unavailable." }));
        const config = resolveProjectSettings(
          yield* settings.getSettings.pipe(Effect.mapError(error)),
          projectId,
        ).settings;
        const models = prismRoleModels(
          config.prismRoles,
          task.definition.role,
          task.definition.lane ?? "medium",
        );
        const providers = yield* registry.getProviders;
        const rotated = models.length
          ? [
              ...models.slice(run.attempt % models.length),
              ...models.slice(0, run.attempt % models.length),
            ]
          : models;
        const picked = pickRoleModel(rotated, providers, yield* Clock.currentTimeMillis);
        if ("refusal" in picked)
          return yield* Effect.fail(new SchedulerError({ detail: picked.refusal }));
        const driver = providers.find(
          (provider) => provider.instanceId === picked.pick.instanceId,
        )!.driver;
        const modelSelection = {
          instanceId: picked.pick.instanceId,
          model: picked.pick.model,
          ...(picked.pick.effort
            ? { options: [{ id: effortOptionId(driver), value: picked.pick.effort }] }
            : {}),
        };
        if (!existing)
          yield* engine
            .dispatch({
              type: "thread.create",
              commandId: CommandId.make(`server:scheduler-create:${run.id}`),
              threadId: id,
              projectId,
              title: task.definition.title,
              modelSelection,
              runtimeMode: DEFAULT_RUNTIME_MODE,
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              createdAt: iso(yield* Clock.currentTimeMillis),
            })
            .pipe(Effect.mapError(error));
        else if (task.definition.target.kind === "new-thread" || run.attempt > 0)
          yield* engine
            .dispatch({
              type: "thread.meta.update",
              commandId: CommandId.make(`server:scheduler-color:${run.id}:${run.attempt}`),
              threadId: id,
              modelSelection,
            })
            .pipe(Effect.mapError(error));
        return id;
      }),
    send: (task, run, text) =>
      Effect.gen(function* () {
        if (!run.threadId) return false;
        const thread = yield* getThread(run.threadId);
        if (!thread) return false;
        const admission = Effect.gen(function* () {
          const current = (yield* engine.getScheduledTasks ?? Effect.succeed([])).find(
            (entry) => entry.id === task.id,
          );
          if (
            !current ||
            current.deleted ||
            current.paused ||
            !current.runs.some((entry) => entry.id === run.id && entry.status === "running")
          )
            return false;
          if ((yield* engine.getThreadRetirement(thread.id))?.retired) return false;
          const latest = yield* getThread(thread.id).pipe(Effect.orDie);
          if (!latest || !schedulerIdle(latest, iso(yield* Clock.currentTimeMillis))) return false;
          const provider = (yield* registry.getProviders).find(
            (entry) => entry.instanceId === latest.modelSelection.instanceId,
          );
          return (
            provider !== undefined &&
            provider.enabled &&
            provider.availability !== "unavailable" &&
            !(provider.usageLimits?.windows.some((window) => window.usedPercent >= 100) ?? false)
          );
        });
        const projectSettings = resolveProjectSettings(
          yield* settings.getSettings.pipe(Effect.mapError(error)),
          thread.projectId,
        ).settings;
        const reportEvents = yield* engine
          .readThreadEvents({
            threadId: thread.id,
            fromSequenceExclusive: run.originSequence,
            toSequenceInclusive: yield* engine.latestSequence,
            limit: Number.MAX_SAFE_INTEGER,
          })
          .pipe(Stream.runCollect, Effect.mapError(error));
        const reports = new Map<string, string>();
        for (const event of reportEvents) {
          if (event.type !== "thread.activity-appended") continue;
          const activity = event.payload.activity;
          if (
            !activity.kind.startsWith("task.") ||
            !Predicate.isObject(activity.payload) ||
            typeof activity.payload.taskId !== "string"
          )
            continue;
          const summary =
            typeof activity.payload.summary === "string"
              ? activity.payload.summary
              : activity.summary;
          reports.set(
            activity.payload.taskId,
            `[Drafter ${activity.payload.taskId}, ${String(activity.payload.status ?? activity.kind)}] ${summary.slice(0, 4000)}`,
          );
        }
        const withReports = `${text}${reports.size ? `\nRun Drafter reports/errors:\n${[...reports.values()].join("\n")}` : ""}`;
        const commandId = CommandId.make(`server:scheduler-turn:${run.id}:${run.sendIndex}`);
        yield* sender(
          thread,
          run.hasWork
            ? withReports
            : roleTaskMessage(projectSettings.prismRoles[task.definition.role], withReports),
          commandId,
          true,
          admission,
        ).pipe(Effect.mapError(error));
        const events = yield* engine
          .readThreadEvents({
            threadId: thread.id,
            fromSequenceExclusive: run.originSequence,
            toSequenceInclusive: yield* engine.latestSequence,
            limit: Number.MAX_SAFE_INTEGER,
          })
          .pipe(Stream.runCollect, Effect.mapError(error));
        return events.some(
          (event) => event.type === "thread.turn-start-requested" && event.commandId === commandId,
        );
      }),
    interrupt: (run) =>
      run.threadId === null
        ? Effect.void
        : engine
            .dispatch({
              type: "thread.turn.interrupt",
              commandId: CommandId.make(`server:scheduler-stall:${run.id}:${run.attempt}`),
              threadId: run.threadId,
              createdAt: iso(Date.parse(run.slot)),
            })
            .pipe(Effect.asVoid, Effect.mapError(error)),
  });
  // Authorization consults durable attribution, including judges older than the health window.
  const authorAllowed = (taskId: string, actor: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        found: number;
      }>`SELECT 1 AS found FROM orchestration_events, json_each(payload_json, '$.runs') AS runs
      WHERE event_type = 'scheduler.state-set' AND stream_id = ${taskId}
      AND json_extract(runs.value, '$.threadId') = ${actor} LIMIT 1`.pipe(Effect.mapError(error));
      if (rows.length)
        return yield* new SchedulerError({
          detail: "A judged thread cannot edit its own scheduled task or outcome check.",
        });
    });
  const start = Effect.gen(function* () {
    const domain = yield* engine.subscribeDomainEvents;
    yield* Stream.runForEach(domain, (event) =>
      event.type === "scheduler.state-set" ? Effect.void : wake,
    ).pipe(Effect.forkScoped);
    yield* Stream.runForEach(registry.streamChanges, () => wake).pipe(Effect.forkScoped);
    yield* Stream.runForEach(yield* settings.subscribeChanges, () => wake).pipe(Effect.forkScoped);
    yield* Effect.gen(function* () {
      const startup = yield* Effect.serviceOption(ServerRuntimeStartup);
      if (Option.isSome(startup)) yield* startup.value.awaitCommandReady.pipe(Effect.orDie);
      for (;;) {
        const deadline = yield* core.reconcile();
        if (deadline === null) yield* Queue.take(wakes);
        else
          yield* Effect.raceFirst(
            Queue.take(wakes),
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) => Effect.sleep(Math.max(0, deadline - now))),
            ),
          );
        yield* Queue.takeAll(wakes);
      }
    }).pipe(Effect.forkScoped);
  });
  return {
    ...core,
    checkHistory,
    inspectRun: observe,
    start,
    edit: (input: Parameters<typeof core.edit>[0], actor: string) =>
      authorAllowed(input.taskId, actor).pipe(Effect.andThen(core.edit(input, actor))),
    pause: (id: string, paused: boolean, actor: string) =>
      authorAllowed(id, actor).pipe(Effect.andThen(core.pause(id, paused, actor))),
    delete: (id: string, actor: string) =>
      authorAllowed(id, actor).pipe(Effect.andThen(core.delete(id, actor))),
  };
});
export class Scheduler extends Context.Service<
  Scheduler,
  Effect.Success<typeof makeLiveScheduler>
>()("t3/scheduler/Service/Scheduler") {
  static readonly layer = Layer.effect(
    Scheduler,
    makeLiveScheduler.pipe(Effect.tap((scheduler) => scheduler.start)),
  );
}
