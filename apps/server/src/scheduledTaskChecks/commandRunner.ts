import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import type { CommandOutcome } from "./engine.ts";

/**
 * Shell command tasks (toolboxmd/chromeria#174, ported from Chromeria v1 #149).
 * Long enough for backups and syncs, short enough that a hung command frees
 * its task for later slots.
 */
export const COMMAND_TIMEOUT_MS = 30 * 60_000;
/** Every write of a task's state rewrites its kept tails, so each stays small. */
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

/**
 * Starts `command` with /bin/sh in `cwd` with no stdin, in the caller's scope,
 * and returns once the process has spawned or could not start; the returned
 * effect waits for it to end. When `deadline` completes first the process
 * group is stopped. The output tail keeps what the process wrote either way,
 * stdout and stderr together in arrival order.
 */
export const startShellCommand = Effect.fnUntraced(function* (input: {
  readonly command: string;
  readonly cwd: string;
  readonly deadline: Effect.Effect<void>;
}): Effect.fn.Return<
  Effect.Effect<CommandOutcome>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const tail = makeOutputTail(COMMAND_OUTPUT_BYTES);
  const notStarted = (error: { readonly message: string }): CommandOutcome => ({
    exitCode: null,
    output: tail.text(),
    timedOut: false,
    failure: `The command could not start: ${error.message}`,
  });
  return yield* spawner
    .spawn(
      ChildProcess.make("/bin/sh", ["-c", input.command], {
        cwd: input.cwd,
        // Its own process group, so stopping it also stops everything it started.
        detached: true,
        stdin: "ignore",
        forceKillAfter: "5 seconds",
      }),
    )
    .pipe(
      Effect.flatMap((handle) =>
        Effect.gen(function* () {
          const reader = yield* Stream.runForEach(handle.all, (chunk) =>
            Effect.sync(() => tail.append(chunk)),
          ).pipe(Effect.ignore, Effect.forkScoped);
          return Effect.gen(function* () {
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
              ? {
                  exitCode: finished.value.value,
                  output: tail.text(),
                  timedOut: false,
                  failure: null,
                }
              : {
                  exitCode: null,
                  output: tail.text(),
                  timedOut: false,
                  failure: "The command was stopped by a signal.",
                };
          }).pipe(Effect.catch((error) => Effect.succeed(notStarted(error))));
        }),
      ),
      Effect.catch((error) => Effect.succeed(Effect.succeed(notStarted(error)))),
    );
});

/** Runs `command` to its end, as `startShellCommand` starts it. */
export const runShellCommand = (input: {
  readonly command: string;
  readonly cwd: string;
  readonly deadline: Effect.Effect<void>;
}) => startShellCommand(input).pipe(Effect.flatten, Effect.scoped);
