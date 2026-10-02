// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import { SchedulerError } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import { temporaryDirectory } from "../mcp/toolkits/threads/handlers.testFixtures.ts";
import {
  COMMAND_OUTPUT_BYTES,
  expandTemplate,
  makeOutputTail,
  runShellCommand,
} from "./CommandRunner.ts";

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
    expect(Buffer.byteLength(text)).toBeLessThanOrEqual(COMMAND_OUTPUT_BYTES);
    expect(Buffer.byteLength(text)).toBeGreaterThan(COMMAND_OUTPUT_BYTES - 4);
    expect(full.endsWith(text)).toBe(true);
    expect(text).not.toContain("�");
    const short = makeOutputTail(COMMAND_OUTPUT_BYTES);
    short.append(Buffer.from("small ü"));
    expect(short.text()).toBe("small ü");
  });

  it.effect("reports the exit code with both streams, and a start failure without one", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-command-exit-");
      const outcome = yield* run(
        "printf 'to stdout\\n'; printf 'to stderr\\n' >&2; exit 7",
        directory,
      );
      expect(outcome).toMatchObject({ exitCode: 7, timedOut: false, failure: null });
      expect(outcome.output).toContain("to stdout\n");
      expect(outcome.output).toContain("to stderr\n");
      const missing = yield* run("true", NodePath.join(directory, "missing"));
      expect(missing).toMatchObject({ exitCode: null, timedOut: false });
      expect(missing.failure).toMatch(/^The command could not start: /);
    }),
  );

  it.effect("on timeout keeps the output tail and stops every process the command started", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-command-timeout-");
      const ready = NodePath.join(directory, "ready.fifo");
      fifo(ready);
      // The deadline fires only after the command reports, through the FIFO, that it wrote.
      const outcome = yield* run(
        "sleep 600 & echo $! > child.pid; printf 'stdout before timeout\\n'; printf 'stderr before timeout\\n' >&2; echo ready > ready.fifo; wait",
        directory,
        Effect.promise(() => NodeFSP.readFile(ready, "utf8")).pipe(Effect.asVoid),
      );
      expect(outcome).toMatchObject({ exitCode: null, timedOut: true, failure: null });
      expect(outcome.output).toContain("stdout before timeout\n");
      expect(outcome.output).toContain("stderr before timeout\n");
      const child = Number(
        yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(directory, "child.pid"), "utf8"),
        ),
      );
      expect(child).toBeGreaterThan(0);
      expect(alive(child)).toBe(false);
    }),
  );

  it.effect("substitutes generated values in any quoting and refuses shell syntax unexecuted", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("scheduler-command-template-");
      const variables = {
        date: "2026-01-01",
        runId: "scheduled-abc:2026-01-01T01:00:00.000Z",
        taskId: "scheduled-abc",
      };
      const template =
        "printf '%s|%s|%s\\n' '{date}' \"{run_id}\" {task_id} > values.txt; touch ran";
      const expanded = yield* expandTemplate(template, variables);
      expect((yield* run(expanded, directory)).exitCode).toBe(0);
      expect(
        yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(directory, "values.txt"), "utf8"),
        ),
      ).toBe("2026-01-01|scheduled-abc:2026-01-01T01:00:00.000Z|scheduled-abc\n");
      for (const hostile of [
        "x'; touch pwned; '",
        'x"; touch pwned; "',
        "x$(touch pwned)",
        "x y",
      ]) {
        const refused = yield* expandTemplate(template, { ...variables, runId: hostile }).pipe(
          Effect.flatMap((command) => run(command, directory)),
          Effect.flip,
        );
        expect(refused).toBeInstanceOf(SchedulerError);
      }
      const files = yield* Effect.promise(() => NodeFSP.readdir(directory));
      expect(files).not.toContain("pwned");
    }),
  );
});
