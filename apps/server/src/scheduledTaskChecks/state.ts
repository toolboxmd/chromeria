import {
  CommandId,
  IsoDateTime,
  MessageId,
  ModelSelection,
  NonNegativeInt,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
  PositiveInt,
  PrismLane,
  PrismRole,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  ScheduledTaskId,
  ScheduledTaskRunStage,
  ThreadId,
  TrimmedNonEmptyString,
  type ScheduledTaskCommand,
  type ScheduledTaskOutcomeCheck,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/** Pinned check versions: a run is judged by the version it started with. */
export const CheckVersion = Schema.Struct({
  version: PositiveInt,
  command: TrimmedNonEmptyString,
  actor: TrimmedNonEmptyString,
  reason: TrimmedNonEmptyString,
  createdAt: IsoDateTime,
  revertedFrom: Schema.NullOr(PositiveInt),
});
export type CheckVersion = typeof CheckVersion.Type;

export const CheckResult = Schema.Struct({
  version: PositiveInt,
  passed: Schema.Boolean,
  output: Schema.String,
  checkedAt: IsoDateTime,
});
export type CheckResult = typeof CheckResult.Type;

/**
 * Everything a send dispatches, fixed when the send is recorded, so a resend
 * after a crash repeats it exactly even if the task or Prism's pick changed.
 */
export const SendPayload = Schema.Struct({
  text: Schema.String,
  /** Prism's pick, for a new thread only; posts keep the thread's model and effort. */
  modelSelection: Schema.NullOr(ModelSelection),
  /** Set when this send launches the run's thread. */
  launch: Schema.NullOr(
    Schema.Struct({
      projectId: ProjectId,
      title: TrimmedNonEmptyString,
      modelSelection: ModelSelection,
      runtimeMode: RuntimeMode,
      interactionMode: ProviderInteractionMode,
      workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy,
    }),
  ),
  projectId: ProjectId,
});
export type SendPayload = typeof SendPayload.Type;

/** One message the scheduler sent for a run, with its deterministic identity. */
export const RunSend = Schema.Struct({
  index: NonNegativeInt,
  commandId: CommandId,
  messageId: MessageId,
  /** First sends launch or post the task prompt; later ones continue the same thread. */
  kind: Schema.Literals(["start", "continue"]),
  createdAt: IsoDateTime,
  /** Kept until a newer send supersedes it; superseded sends keep only their identity. */
  payload: Schema.optional(SendPayload),
});
export type RunSend = typeof RunSend.Type;

/** A finished command run. It started when its run was recorded, before the spawn. */
export const CommandResult = Schema.Struct({
  /** Null when the process timed out, was stopped by a signal or never started. */
  exitCode: Schema.NullOr(Schema.Int),
  timedOut: Schema.Boolean,
  endedAt: IsoDateTime,
  /** The last bytes of stdout and stderr in arrival order; only the newest runs keep it. */
  output: Schema.optional(Schema.String),
});
export type CommandResult = typeof CommandResult.Type;

export const CheckedRun = Schema.Struct({
  id: TrimmedNonEmptyString,
  slot: IsoDateTime,
  /** The pinned outcome check of an agent run; command runs have none. */
  checkVersion: Schema.NullOr(PositiveInt),
  threadId: Schema.NullOr(ThreadId),
  /** Pinned at the first check from the run's thread workspace. */
  checkCwd: Schema.NullOr(TrimmedNonEmptyString),
  stage: ScheduledTaskRunStage,
  attempt: NonNegativeInt,
  retryAt: Schema.NullOr(IsoDateTime),
  hasWork: Schema.Boolean,
  sends: Schema.Array(RunSend),
  error: Schema.NullOr(Schema.String),
  check: Schema.NullOr(CheckResult),
  /** Present once a command run's process ended. */
  commandResult: Schema.optional(CommandResult),
  /**
   * Its next check waits for its bound Spectrum reports to release: set when a
   * check passed while they were pending, or when a report needed the user.
   */
  awaitingReports: Schema.optional(Schema.Boolean),
  /** Imported from Chromeria v1 as history: never driven, never resumed. */
  imported: Schema.optional(Schema.Struct({ from: Schema.Literal("v1"), status: Schema.String })),
});
export type CheckedRun = typeof CheckedRun.Type;

/**
 * A fork task's state, one row per upstream task id. `agent` tasks keep at
 * least one pinned check version; `command` tasks run `command` and have none.
 */
export const CheckState = Schema.Struct({
  version: Schema.Literal(1),
  taskId: ScheduledTaskId,
  revision: NonNegativeInt,
  kind: Schema.Literals(["agent", "command"]),
  command: Schema.NullOr(TrimmedNonEmptyString),
  role: Schema.NullOr(PrismRole),
  lane: Schema.NullOr(PrismLane),
  checks: Schema.Array(CheckVersion),
  runs: Schema.Array(CheckedRun),
  failureStreak: NonNegativeInt,
  lastError: Schema.NullOr(Schema.String),
  /** The command run that most recently passed; never cleared. */
  lastSuccessfulRunId: Schema.NullOr(TrimmedNonEmptyString),
});
export type CheckState = typeof CheckState.Type;

/** Delays before the same thread is asked to continue after a failed check or run. */
export const RECOVERY_DELAYS_MS = [30_000, 60_000, 300_000, 900_000, 3_600_000] as const;
/** Command runs keep their output tail only for this many newest runs. */
export const COMMAND_OUTPUTS_KEPT = 3;
/** Settled runs kept per task; unfinished runs and their pinned versions are always kept. */
export const RUNS_KEPT = 20;
export const CHECK_VERSIONS_KEPT = 20;
export const CHECK_OUTPUT_BYTES = 16_384;

export const activeCheck = (state: CheckState) => state.checks.at(-1)!;

/**
 * Whether a run still holds its task. An agent run holds it until its check
 * passed; a command run only while its process runs, since one that failed
 * never resumes. Imported history never holds a task.
 */
export const holdsTask = (state: Pick<CheckState, "kind">, run: CheckedRun) =>
  run.imported === undefined &&
  (state.kind === "command" ? run.stage === "running" : run.stage !== "done");

export const unfinishedRun = (state: CheckState): CheckedRun | undefined =>
  state.runs.find((run) => holdsTask(state, run));

/** Bounded history: unfinished runs and the versions they pin always survive. */
export const compact = (state: CheckState): CheckState => {
  const kept = state.runs
    .filter((run, index) => holdsTask(state, run) || index >= state.runs.length - RUNS_KEPT)
    .map((run, index, all) => {
      // Only an unfinished run's latest send can still be resent.
      const keep = holdsTask(state, run) ? run.sends.length - 1 : -1;
      const sends = run.sends.every((send, at) => at === keep || send.payload === undefined)
        ? run.sends
        : run.sends.map((send, at) => {
            if (at === keep || send.payload === undefined) return send;
            const { payload: _, ...identity } = send;
            return identity;
          });
      // Every write rewrites the row, so only the newest command outputs stay.
      const output = run.commandResult?.output;
      const dropOutput = output !== undefined && index < all.length - COMMAND_OUTPUTS_KEPT;
      if (sends === run.sends && !dropOutput) return run;
      if (!dropOutput) return { ...run, sends };
      const { output: _, ...result } = run.commandResult!;
      return { ...run, sends, commandResult: result };
    });
  const pinned = new Set(
    kept.flatMap((run) => (run.checkVersion === null ? [] : [run.checkVersion])),
  );
  const checks = state.checks.filter(
    (check, index) =>
      index >= state.checks.length - CHECK_VERSIONS_KEPT || pinned.has(check.version),
  );
  return { ...state, runs: kept, checks };
};

export const truncateOutput = (text: string) => {
  let output = Buffer.from(text).subarray(0, CHECK_OUTPUT_BYTES).toString("utf8");
  while (Buffer.byteLength(output) > CHECK_OUTPUT_BYTES) output = output.slice(0, -1);
  return output;
};

/** The read-model summary of an agent task's outcome check. */
export function outcomeCheckSummary(state: CheckState): ScheduledTaskOutcomeCheck {
  const check = activeCheck(state);
  const run = unfinishedRun(state) ?? state.runs.at(-1);
  const verdict = state.runs.toReversed().find((entry) => entry.check !== null)?.check ?? null;
  return {
    version: check.version,
    command: check.command,
    role: state.role,
    lane: state.lane,
    run:
      run === undefined
        ? null
        : {
            id: run.id,
            stage: run.stage,
            checkVersion: run.checkVersion ?? check.version,
            attempt: run.attempt,
            error: run.error,
            imported: run.imported !== undefined,
            resumable: run.imported === undefined && run.stage === "needs-you",
          },
    lastVerdict:
      verdict === null
        ? null
        : { version: verdict.version, passed: verdict.passed, checkedAt: verdict.checkedAt },
  };
}

/** The read-model summary of a command task. */
export function commandSummary(state: CheckState): ScheduledTaskCommand {
  const run = state.runs.at(-1);
  return {
    command: state.command!,
    run:
      run === undefined
        ? null
        : {
            id: run.id,
            stage:
              run.stage === "running" ? "running" : run.stage === "done" ? "done" : "needs-you",
            exitCode: run.commandResult?.exitCode ?? null,
            timedOut: run.commandResult?.timedOut ?? false,
            endedAt: run.commandResult?.endedAt ?? null,
            error: run.error,
            ...(run.commandResult?.output === undefined
              ? {}
              : { output: run.commandResult.output }),
            imported: run.imported !== undefined,
          },
    failureStreak: state.failureStreak,
    lastSuccessfulRunId: state.lastSuccessfulRunId,
  };
}

/**
 * Upstream records a run as succeeded once its dispatch returns. With an
 * outcome check that is not completion, so the read model reports the
 * checked run instead: unfinished is running, needs-you is failed, and only
 * a passed check is succeeded.
 */
export function checkedRunStatus(
  state: CheckState,
): { readonly status: "running" | "succeeded" | "failed"; readonly error: string | null } | null {
  const run = unfinishedRun(state) ?? state.runs.at(-1);
  if (run === undefined) return null;
  if (run.imported !== undefined)
    return {
      status: "failed",
      error: `Imported from Chromeria v1 (${run.imported.status}); it is not resumed here. Run now starts a new run.`,
    };
  if (run.stage === "done") return { status: "succeeded", error: null };
  if (run.stage === "needs-you")
    return { status: "failed", error: `Needs you: ${run.error ?? "the run stopped."}` };
  return { status: "running", error: null };
}
