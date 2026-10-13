/**
 * Which files a tool call read, so a pointer rule can lift once its target is read.
 *
 * The rule AgentsMD's read-before-action gate already uses in daily use: a
 * Read-like tool's path arguments, or any path token in a shell command that
 * runs a reader (`cat`, `sed`, `head`, `rg` ...), resolved against `cwd` and any
 * `cd` in the command. Only files that exist count, which also drops tokens
 * that are not paths. The serving state records a read at the tool result, and
 * only when the call did not fail. Only a full read lifts a pointer rule: a
 * shell command is partial when it runs any reader that shows part of a file
 * (`grep`, `rg`, `head`, `tail`, `sed`, `awk`, also after a pipe), and a Read
 * tool call is partial when it names a line offset or limit.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { expandHomePathWith } from "../pathExpansion.ts";
import type { Read } from "./Selection.ts";

const READERS = [
  "cat",
  "sed",
  "head",
  "tail",
  "less",
  "more",
  "bat",
  "batcat",
  "nl",
  "awk",
  "rg",
  "grep",
  "view",
];
const PARTIAL = new Set(["grep", "rg", "head", "tail", "sed", "awk"]);
const RANGE_FIELDS = [
  "offset",
  "limit",
  "start_line",
  "end_line",
  "startLine",
  "endLine",
  "view_range",
];
const READ_CMD = new RegExp(`(?:^|[\\s;&|(\`"'])(${READERS.join("|")})\\s`, "gu");
const PATH_TOKEN = /[^\s'"`;|&()<>{}=,]+/gu;
const CD = /(?:^|[;&|(]\s*)cd\s+(\S+)/gu;
const READ_TOOL = /read|view|open/iu;

const strings = (value: unknown): string[] => {
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap(strings);
  if (typeof value === "object" && value !== null) return Object.values(value).flatMap(strings);
  return [];
};

const existing = Effect.fn("contextmd.existingFiles")(function* (
  candidates: ReadonlyArray<string>,
  bases: ReadonlyArray<string>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const out: string[] = [];
  for (const candidate of candidates) {
    for (const base of bases) {
      const resolved = path.resolve(base, expandHomePathWith(candidate, path));
      const info = yield* fs.stat(resolved).pipe(Effect.option);
      if (Option.isNone(info) || info.value.type !== "File") continue;
      const real = yield* fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
      if (!out.includes(real)) out.push(real);
    }
  }
  return out;
});

/** The files one tool call read, with how it read them. */
export const reads = Effect.fn("contextmd.reads")(function* (
  tool: string,
  toolInput: unknown,
  cwd: string | null,
  shellTools: ReadonlyArray<string>,
) {
  const path = yield* Path.Path;
  const base = cwd ?? "/";
  if (shellTools.includes(tool)) {
    const command =
      typeof toolInput === "object" && toolInput !== null
        ? (toolInput as Record<string, unknown>)["command"]
        : undefined;
    if (typeof command !== "string") return [] as Read[];
    const used = [...command.matchAll(READ_CMD)].map((m) => m[1]!);
    if (used.length === 0) return [] as Read[];
    const kind = used.some((r) => PARTIAL.has(r)) ? "shell_partial" : "shell_full";
    const bases = [
      base,
      ...[...command.matchAll(CD)].map((m) =>
        path.resolve(base, expandHomePathWith(m[1]!.replace(/^['"]|['"]$/gu, ""), path)),
      ),
    ];
    const files = yield* existing(
      [...command.matchAll(PATH_TOKEN)].map((m) => m[0]),
      bases,
    );
    return files.map((p): Read => ({ path: p, kind }));
  }
  if (READ_TOOL.test(tool.split("__").at(-1) ?? "")) {
    const input = (typeof toolInput === "object" && toolInput !== null ? toolInput : {}) as Record<
      string,
      unknown
    >;
    const kind = RANGE_FIELDS.some((f) => input[f] !== undefined && input[f] !== null)
      ? "read_partial"
      : "read_tool";
    const files = yield* existing(strings(toolInput), [base]);
    return files.map((p): Read => ({ path: p, kind }));
  }
  return [] as Read[];
});
