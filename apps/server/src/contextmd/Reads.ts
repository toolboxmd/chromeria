/**
 * Which files a tool call read, so a pointer rule can lift once its target is read in full.
 *
 * A Read-like tool's path arguments count, and in a shell command the operands of
 * a reader (`cat`, `sed`, `head`, `rg` ...) in its own segment, resolved against
 * `cwd` and any earlier `cd`. Only files that exist count. The serving state
 * records a read at the tool result, and only when the call did not fail.
 *
 * Only a full read lifts a pointer rule (#225). A shell read is full only when a
 * whole-file reader (`cat`, `nl`, `bat`, `less`, `more`, `view`) runs without a
 * range option and its output is neither piped, redirected nor fed by a
 * here-string; a Read-like call is full only without an offset, limit or line
 * range. Either is partial when the result shows it was cut (a truncation marker
 * in the output, or fewer lines returned than the file has). Unclear shapes count
 * as partial: a missed full read only costs the deny cap.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { expandHomePathWith } from "../pathExpansion.ts";
import type { Read } from "./Selection.ts";

const FULL_READERS = new Set(["cat", "less", "more", "bat", "batcat", "nl", "view"]);
const PARTIAL_READERS = new Set(["sed", "head", "tail", "awk", "rg", "grep"]);
const RANGE_FIELDS = [
  "offset",
  "limit",
  "start_line",
  "end_line",
  "startLine",
  "endLine",
  "view_range",
];
/** Reader options that show only part of a file: `bat -r 1:5`, `less +40`. */
const RANGE_OPTION = /^(?:-r|--line-range(?:=.*)?|\+.*)$/u;
/** Stdout leaves the agent's view: `> file`, `>> file`, `&> file`, `>&2`; `2>` is fine. */
const REDIRECT = /(?:^|[^2&])>|&>/u;
/** How harnesses mark a cut result: Codex, Claude Code, OpenCode, Grok. */
const TRUNCATED =
  /\b\d+ (?:tokens|chars|characters|lines|bytes) (?:truncated|omitted)\b|omitted \d+ of \d+ lines|\[\+\d+ chars\]|File has more lines|Output (?:was )?truncated/iu;
const SEGMENT = /(\|\||&&|;|\n|\|)/u;
const WORD = /'([^']*)'|"([^"]*)"|(\S+)/gu;
const WRAPPERS = new Set(["command", "exec", "nohup", "time", "env", "sudo"]);
const READ_TOOL = /read|view|open/iu;

const strings = (value: unknown): string[] => {
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap(strings);
  if (typeof value === "object" && value !== null) return Object.values(value).flatMap(strings);
  return [];
};

/** Fewer lines returned than the file has (Claude Code's Read result: `file.numLines`). */
const cutByMetadata = (value: unknown): boolean => {
  if (Array.isArray(value)) return value.some(cutByMetadata);
  if (typeof value !== "object" || value === null) return false;
  const o = value as Record<string, unknown>;
  const { numLines, totalLines, startLine } = o;
  if (typeof numLines === "number" && typeof totalLines === "number" && numLines < totalLines)
    return true;
  if (typeof startLine === "number" && startLine > 1) return true;
  return Object.values(o).some(cutByMetadata);
};

const words = (segment: string) =>
  [...segment.matchAll(WORD)].map((m) => m[1] ?? m[2] ?? m[3] ?? "");

const existing = Effect.fn("contextmd.existingFiles")(function* (
  candidates: ReadonlyArray<string>,
  base: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const out: string[] = [];
  for (const candidate of candidates) {
    const resolved = path.resolve(base, expandHomePathWith(candidate, path));
    const info = yield* fs.stat(resolved).pipe(Effect.option);
    if (Option.isNone(info) || info.value.type !== "File") continue;
    const real = yield* fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
    if (!out.includes(real)) out.push(real);
  }
  return out;
});

/** Reader operands per shell segment, each with whether that segment showed whole files. */
const shellReads = Effect.fn("contextmd.shellReads")(function* (command: string, cwd: string) {
  const path = yield* Path.Path;
  const parts = command.split(SEGMENT);
  let base = cwd;
  const out: Read[] = [];
  for (let i = 0; i < parts.length; i += 2) {
    const segment = parts[i]!;
    const piped = parts[i + 1] === "|";
    const ws = words(segment);
    while (ws.length > 0 && (WRAPPERS.has(ws[0]!) || /^[A-Za-z_][A-Za-z0-9_]*=/u.test(ws[0]!)))
      ws.shift();
    const name = (ws[0] ?? "").slice((ws[0] ?? "").lastIndexOf("/") + 1);
    if (name === "cd" && ws[1] !== undefined) {
      base = path.resolve(base, expandHomePathWith(ws[1], path));
      continue;
    }
    if (!FULL_READERS.has(name) && !PARTIAL_READERS.has(name)) continue;
    const args = ws.slice(1);
    const full =
      FULL_READERS.has(name) &&
      !piped &&
      !segment.includes("<<<") &&
      !REDIRECT.test(segment) &&
      !args.some((a) => RANGE_OPTION.test(a));
    const operands = args.filter((a) => !a.startsWith("-") && !a.startsWith("+"));
    for (const file of yield* existing(operands, base)) {
      out.push({ path: file, kind: full ? "shell_full" : "shell_partial" });
    }
  }
  return out;
});

/** The files one tool call read, with how it read them; `response` is the tool's result. */
export const reads = Effect.fn("contextmd.reads")(function* (
  tool: string,
  toolInput: unknown,
  cwd: string | null,
  shellTools: ReadonlyArray<string>,
  response: unknown = undefined,
) {
  const base = cwd ?? "/";
  const input = (typeof toolInput === "object" && toolInput !== null ? toolInput : {}) as Record<
    string,
    unknown
  >;
  let found: Read[] = [];
  if (shellTools.includes(tool)) {
    if (typeof input["command"] !== "string") return [] as Read[];
    found = yield* shellReads(input["command"], base);
  } else if (READ_TOOL.test(tool.split("__").at(-1) ?? "")) {
    const kind = RANGE_FIELDS.some((f) => input[f] !== undefined && input[f] !== null)
      ? "read_partial"
      : "read_tool";
    found = (yield* existing(strings(toolInput), base)).map((p): Read => ({ path: p, kind }));
  }
  const cut = cutByMetadata(response) || strings(response).some((s) => TRUNCATED.test(s));
  // A file read in full by one segment stays full even if another segment only grepped it.
  const byPath = new Map<string, Read>();
  for (const r of found) {
    const kind = !cut
      ? r.kind
      : r.kind === "shell_full"
        ? "shell_partial"
        : r.kind === "read_tool"
          ? "read_partial"
          : r.kind;
    const prior = byPath.get(r.path);
    if (prior === undefined || kind === "shell_full" || kind === "read_tool")
      byPath.set(r.path, { path: r.path, kind });
  }
  return [...byPath.values()];
});
