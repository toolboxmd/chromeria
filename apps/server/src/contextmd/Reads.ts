/**
 * Which files a tool call read, so a pointer rule can lift once its target is read in full.
 *
 * A file-reading tool's file argument counts, and in a shell command the operands of
 * a reader (`cat`, `sed`, `head`, `rg` ...) in its own segment, resolved against
 * `cwd` and any earlier `cd`. Only files that exist count. The serving state
 * records a read at the tool result, and only when the call did not fail.
 *
 * Only a full read lifts a pointer rule (#225). A shell command is a full read
 * only when the whole command is one plain whole-file reader call: optional
 * `cd <dir> &&` steps, then `cat`, `nl`, `bat`, `less`, `more` or `view` with
 * known whole-file options and file operands, at most `2>/dev/null`, and no
 * other shell syntax. Shell is open-ended, so every other shape is partial: its
 * reader operands are still logged, but never lift. A file-reading tool (Claude
 * Code `Read`, OpenCode `read`, Grok `read_file`) counts only for its file field,
 * and is full only without an offset, limit, line or page range. Either is partial when the result
 * shows it was cut (a truncation marker, or fewer lines returned than the file
 * has). A missed full read only costs the deny cap.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
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
  "pages",
];
/** File-reading tools and the input field naming their file: Claude Code, OpenCode, Grok. */
const READ_TOOLS: Record<string, string> = {
  Read: "file_path",
  read: "filePath",
  read_file: "target_file",
};
/** Options that keep a reader's output whole; any other option makes the read partial. */
const WHOLE_OPTIONS: Record<string, ReadonlySet<string>> = {
  cat: new Set(["-n", "-b", "-s", "-v", "-A", "-e", "-t"]),
  nl: new Set(["-ba"]),
  bat: new Set(["-p", "-pp", "--plain", "--paging=never", "--color=never", "--style=plain"]),
  batcat: new Set(["-p", "-pp", "--plain", "--paging=never", "--color=never", "--style=plain"]),
  less: new Set(),
  more: new Set(),
  view: new Set(),
};
/** Shell syntax that can hide, condition, redirect or substitute a read. */
const SHELL_SYNTAX = /[;|&<>`$(){}[\]*?!\\\n'"]/u;
/** Operands a full read refuses: `..` after a symlink or `cd -` can name another file. */
const UNSAFE_PATH = /(?:^|\/)\.\.(?:\/|$)|^-$/u;
/**
 * How harnesses mark a cut result, checked against live hook payloads on 2026-10-13:
 * Claude Code (`[N characters truncated]`, `Output too large`), Codex (`Warning: truncated
 * output (original token count: N)`, `N tokens truncated`, `N bytes omitted`), Grok (`[+N
 * chars]`, `[truncated: showing first/last ...]`, `(output truncated)`), and the OpenCode
 * and Grok read tools (`(Showing lines A-B` with or without `of N`, `(Output capped at 50 KB`,
 * `(line truncated to N chars)`).
 */
const TRUNCATED = new RegExp(
  [
    String.raw`\b\d+ (?:tokens|chars|characters|lines|bytes) (?:truncated|omitted)\b`,
    String.raw`omitted \d+ of \d+ lines`,
    String.raw`\[\+\d+ chars\]`,
    String.raw`\[\.\.\. truncated \(\d+ chars total\)\]`,
    String.raw`Output (?:was )?truncated`,
    String.raw`\(output truncated\)`,
    String.raw`\(Showing lines \d+-\d+`,
    String.raw`Output capped at \d+`,
    String.raw`truncated output \(original token count`,
    String.raw`\[truncated: showing`,
    String.raw`Output too large`,
    String.raw`line truncated to \d+ chars`,
    String.raw`File has more lines`,
  ].join("|"),
  "iu",
);
const SEGMENT = /(\|\||&&|;|\n|\|)/u;
const WORD = /'([^']*)'|"([^"]*)"|(\S+)/gu;
const WRAPPERS = new Set(["command", "exec", "nohup", "time", "env", "sudo"]);

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
  // Claude Code's Read also says so outright, even when the line counts match.
  if (Object.entries(o).some(([k, v]) => /truncat/iu.test(k) && v === true)) return true;
  // Claude Code saved a long shell output to a file and showed only its start.
  if (typeof o["persistedOutputPath"] === "string" && o["persistedOutputPath"] !== "") return true;
  // Grok's read_file: fewer lines returned than the file has, with no footer in the hook.
  const { total_lines: total, raw_output: raw } = o;
  if (typeof total === "number" && typeof raw === "string" && raw.split("\n").length < total)
    return true;
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
  const home = yield* HostProcess.HomeDirectory;
  const out: string[] = [];
  for (const candidate of candidates) {
    const resolved = path.resolve(base, expandHomePath(candidate, home));
    const info = yield* fs.stat(resolved).pipe(Effect.option);
    if (Option.isNone(info) || info.value.type !== "File") continue;
    const real = yield* fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
    if (!out.includes(real)) out.push(real);
  }
  return out;
});

const readerName = (word: string | undefined) =>
  (word ?? "").slice((word ?? "").lastIndexOf("/") + 1);

/** The files of a command that is exactly `[cd <dir> && ...] <reader> [options] <files>`. */
const fullRead = Effect.fn("contextmd.fullRead")(function* (command: string, cwd: string) {
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const steps = command.replace(/\s+2>\/dev\/null\s*$/u, "").split("&&");
  if (steps.some((s) => SHELL_SYNTAX.test(s))) return null;
  let base = cwd;
  for (const step of steps.slice(0, -1)) {
    const ws = words(step);
    if (ws.length !== 2 || ws[0] !== "cd" || UNSAFE_PATH.test(ws[1]!)) return null;
    base = path.resolve(base, expandHomePath(ws[1]!, home));
  }
  const ws = words(steps.at(-1)!);
  const allowed = WHOLE_OPTIONS[readerName(ws[0])];
  if (allowed === undefined || ws.length < 2) return null;
  const args = ws.slice(1);
  if (args.some((a) => (a.startsWith("-") || a.startsWith("+")) && !allowed.has(a))) return null;
  const operands = args.filter((a) => !a.startsWith("-"));
  if (operands.some((a) => UNSAFE_PATH.test(a))) return null;
  const files = yield* existing(operands, base);
  // Every operand must be a file that exists: anything else is not a plain read.
  return files.length === operands.length ? files : null;
});

/** Reader operands anywhere in a command, for the log; these never lift. */
const partialReads = Effect.fn("contextmd.partialReads")(function* (command: string, cwd: string) {
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const parts = command.split(SEGMENT);
  let base = cwd;
  const out: string[] = [];
  for (let i = 0; i < parts.length; i += 2) {
    const ws = words(parts[i]!);
    while (ws.length > 0 && (WRAPPERS.has(ws[0]!) || /^[A-Za-z_][A-Za-z0-9_]*=/u.test(ws[0]!)))
      ws.shift();
    const name = readerName(ws[0]);
    if (name === "cd" && ws[1] !== undefined) {
      base = path.resolve(base, expandHomePath(ws[1], home));
      continue;
    }
    if (!FULL_READERS.has(name) && !PARTIAL_READERS.has(name)) continue;
    const operands = ws.slice(1).filter((a) => !a.startsWith("-") && !a.startsWith("+"));
    for (const file of yield* existing(operands, base)) if (!out.includes(file)) out.push(file);
  }
  return out;
});

const shellReads = Effect.fn("contextmd.shellReads")(function* (command: string, cwd: string) {
  const full = yield* fullRead(command.trim(), cwd);
  if (full !== null) return full.map((p): Read => ({ path: p, kind: "shell_full" }));
  return (yield* partialReads(command, cwd)).map((p): Read => ({ path: p, kind: "shell_partial" }));
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
  } else if (READ_TOOLS[tool] !== undefined) {
    const target = input[READ_TOOLS[tool]];
    if (typeof target !== "string") return [] as Read[];
    // `..` after a symlinked directory names another file than the collapsed path (#225).
    if (target.split("/").includes("..")) return [] as Read[];
    const kind = RANGE_FIELDS.some((f) => input[f] !== undefined && input[f] !== null)
      ? "read_partial"
      : "read_tool";
    found = (yield* existing([target], base)).map((p): Read => ({ path: p, kind }));
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
