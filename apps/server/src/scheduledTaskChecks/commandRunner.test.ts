// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { COMMAND_OUTPUT_BYTES, makeOutputTail, runShellCommand } from "./commandRunner.ts";
import { ScheduledTaskCheckError } from "./engine.ts";
import { expandCheckCommand } from "./ScheduledTaskChecks.ts";

const temporaryDirectory = (prefix: string) =>
  Effect.acquireRelease(
    Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix))),
    (directory) => Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
  );
const fifo = (path: string) => NodeChildProcess.execFileSync("mkfifo", [path]);
/** Alive means signalable and not a zombie waiting to be reaped. */
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
  } catch {
    return false;
  }
  return !NodeChildProcess.execFileSync("ps", ["-o", "stat=", "-p", String(pid)], {
    encoding: "utf8",
  })
    .trim()
    .startsWith("Z");
};
const run = (command: string, cwd: string, deadline: Effect.Effect<void> = Effect.never) =>
  runShellCommand({ command, cwd, deadline }).pipe(Effect.provide(NodeServices.layer));

describe("scheduled command execution", () => {
  it("keeps only the last bytes, never starting mid-character", () => {
    const chunks = ["é".repeat(1_500), "line one\n", "😀".repeat(700), "ü tail end\n"];
    const tail = makeOutputTail(COMMAND_OUTPUT_BYTES);
    for (const chunk of chunks) tail.append(Buffer.from(chunk));
    const full = chunks.join("");
    const text = tail.text();
    assert.isAtMost(Buffer.byteLength(text), COMMAND_OUTPUT_BYTES);
    assert.isAbove(Buffer.byteLength(text), COMMAND_OUTPUT_BYTES - 4);
    assert.isTrue(full.endsWith(text));
    assert.notInclude(text, "�");
    const short = makeOutputTail(COMMAND_OUTPUT_BYTES);
    short.append(Buffer.from("small ü"));
    assert.equal(short.text(), "small ü");
  });

  it.effect("reports the exit code with both streams, and a start failure without one", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduled-command-exit-");
      const outcome = yield* run(
        "printf 'to stdout\\n'; printf 'to stderr\\n' >&2; exit 7",
        directory,
      );
      assert.equal(outcome.exitCode, 7);
      assert.isFalse(outcome.timedOut);
      assert.isNull(outcome.failure);
      assert.include(outcome.output, "to stdout\n");
      assert.include(outcome.output, "to stderr\n");
      const missing = yield* run("true", NodePath.join(directory, "missing"));
      assert.isNull(missing.exitCode);
      assert.isFalse(missing.timedOut);
      assert.match(missing.failure ?? "", /^The command could not start: /);
    }),
  );

  it.effect("on timeout keeps the output tail and stops every process the command started", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduled-command-timeout-");
      const ready = NodePath.join(directory, "ready.fifo");
      fifo(ready);
      // The deadline fires only after the command reports, through the FIFO, that it wrote.
      const outcome = yield* run(
        "sleep 600 & echo $! > child.pid; printf 'stdout before timeout\\n'; printf 'stderr before timeout\\n' >&2; echo ready > ready.fifo; wait",
        directory,
        Effect.promise(() => NodeFSP.readFile(ready, "utf8")).pipe(Effect.asVoid),
      );
      assert.isNull(outcome.exitCode);
      assert.isTrue(outcome.timedOut);
      assert.include(outcome.output, "stdout before timeout\n");
      assert.include(outcome.output, "stderr before timeout\n");
      const child = Number(
        yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(directory, "child.pid"), "utf8"),
        ),
      );
      assert.isAbove(child, 0);
      assert.isFalse(alive(child));
    }),
  );

  it.effect("substitutes generated values in any quoting and refuses shell syntax unexecuted", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduled-command-template-");
      const variables = {
        date: "2026-01-01",
        runId: "scheduled-task:abc:2026-01-01T01:00:00.000Z",
        taskId: "scheduled-task:abc",
      };
      const template =
        "printf '%s|%s|%s\\n' '{date}' \"{run_id}\" {task_id} > values.txt; touch ran";
      const expanded = yield* expandCheckCommand(template, variables);
      assert.equal((yield* run(expanded, directory)).exitCode, 0);
      assert.equal(
        yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(directory, "values.txt"), "utf8"),
        ),
        "2026-01-01|scheduled-task:abc:2026-01-01T01:00:00.000Z|scheduled-task:abc\n",
      );
      for (const hostile of [
        "x'; touch pwned; '",
        'x"; touch pwned; "',
        "x$(touch pwned)",
        "x y",
      ]) {
        const refused = yield* expandCheckCommand(template, { ...variables, runId: hostile }).pipe(
          Effect.flatMap((command) => run(command, directory)),
          Effect.flip,
        );
        assert.instanceOf(refused, ScheduledTaskCheckError);
      }
      // A value no placeholder references never blocks the command.
      assert.equal(
        yield* expandCheckCommand("touch plain", { ...variables, taskId: "x y" }),
        "touch plain",
      );
      const files = yield* Effect.promise(() => NodeFSP.readdir(directory));
      assert.notInclude(files, "pwned");
    }),
  );
});
