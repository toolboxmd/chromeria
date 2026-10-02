import { SchedulerError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

/**
 * Reversible default: long enough for backups and syncs, short enough that a hung command
 * frees its task for later slots.
 */
export const COMMAND_TIMEOUT_MS = 30 * 60_000;
/** Every retained run repeats in each scheduler state event, so the tail stays small. */
export const COMMAND_OUTPUT_BYTES = 4_096;
/** How long to read output a finished or stopped process left in its pipe. */
const DRAIN_MS = 2_000;

/** The last `limit` bytes appended, never starting in the middle of a UTF-8 character. */
export const makeOutputTail = (limit: number) => {
  let buffer = Buffer.alloc(0);
  let cut = false;
  return {
    append: (chunk: Uint8Array) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.byteLength > limit) {
        buffer = Buffer.from(buffer.subarray(buffer.byteLength - limit));
        cut = true;
      }
    },
    text: () => {
      let start = 0;
      if (cut) while (start < buffer.length && (buffer[start]! & 0xc0) === 0x80) start++;
      return buffer.subarray(start).toString("utf8");
    },
  };
};

// Server-generated values only. Inert inside single quotes, double quotes or none at all.
const SAFE_VALUE = /^[A-Za-z0-9._:-]+$/;

/** Fills `{date}`, `{run_id}` and `{task_id}`; refuses any value a shell could interpret. */
export const expandTemplate = (
  command: string,
  variables: { readonly date: string; readonly runId: string; readonly taskId: string },
): Effect.Effect<string, SchedulerError> =>
  [variables.date, variables.runId, variables.taskId].every((value) => SAFE_VALUE.test(value))
    ? Effect.succeed(
        command.replace(/\{(date|run_id|task_id)\}/g, (_, key: string) =>
          key === "date" ? variables.date : key === "run_id" ? variables.runId : variables.taskId,
        ),
      )
    : Effect.fail(
        new SchedulerError({ detail: "Refusing to substitute a value with shell syntax." }),
      );

export type ShellCommandOutcome = {
  readonly exitCode: number | null;
  readonly output: string;
  readonly timedOut: boolean;
  /** Why there is no exit code when the process never started or a signal stopped it. */
  readonly failure: string | null;
};

/**
 * Runs `command` with /bin/sh in `cwd` with no stdin. When `deadline` completes first the
 * process group is stopped. The output tail keeps what the process wrote either way.
 */
export const runShellCommand = Effect.fnUntraced(function* (input: {
  readonly command: string;
  readonly cwd: string;
  readonly deadline: Effect.Effect<void>;
}): Effect.fn.Return<ShellCommandOutcome, never, ChildProcessSpawner.ChildProcessSpawner> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const tail = makeOutputTail(COMMAND_OUTPUT_BYTES);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* spawner.spawn(
        ChildProcess.make("/bin/sh", ["-c", input.command], {
          cwd: input.cwd,
          // Its own process group, so stopping it also stops everything it started.
          detached: true,
          stdin: "ignore",
          forceKillAfter: "5 seconds",
        }),
      );
      const reader = yield* Stream.runForEach(handle.all, (chunk) =>
        Effect.sync(() => tail.append(chunk)),
      ).pipe(Effect.ignore, Effect.forkScoped);
      const finished = yield* Effect.raceFirst(
        handle.exitCode.pipe(Effect.exit, Effect.asSome),
        input.deadline.pipe(Effect.as(Option.none())),
      );
      if (Option.isNone(finished)) yield* handle.kill({ forceKillAfter: "5 seconds" });
      // A background child may keep the pipe open after the shell ends; read only briefly.
      yield* Fiber.join(reader).pipe(Effect.timeoutOption(DRAIN_MS));
      if (Option.isNone(finished))
        return { exitCode: null, output: tail.text(), timedOut: true, failure: null };
      return Exit.isSuccess(finished.value)
        ? { exitCode: finished.value.value, output: tail.text(), timedOut: false, failure: null }
        : {
            exitCode: null,
            output: tail.text(),
            timedOut: false,
            failure: "The command was stopped by a signal.",
          };
    }),
  ).pipe(
    Effect.catch((error) =>
      Effect.succeed({
        exitCode: null,
        output: tail.text(),
        timedOut: false,
        failure: `The command could not start: ${error.message}`,
      }),
    ),
  );
});
