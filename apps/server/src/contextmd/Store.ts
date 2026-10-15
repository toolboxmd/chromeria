/** Flat Libraries share one YAML header parser and exact-key/Cue projections. */
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Hex from "effect/encoding/Hex";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { normalizeError, type EntryKeys } from "./Matcher.ts";

export const MAX_ENTRY_BYTES = 1800;
const FILE_CONCURRENCY = 8;
const MAX_ID_CHARS = 120;
const KEY_FIELDS = ["command", "path", "error"];
const FIELDS = ["id", ...KEY_FIELDS, "must-know-before", "name", "applies", "target"];
// oxlint-disable-next-line no-control-regex -- the file boundary rejects these characters in YAML headers.
const CONTROL = /[\x00-\x08\x0a-\x1f\x7f-\x9f]/u;
const PLAIN_INVALID = /[\uFEFF\uFFFE\uFFFF]/u;
// oxlint-disable no-control-regex -- one explicit shared blank set rejects empty error signatures.
const ERROR_BLANK =
  /^[\u0009-\u000d\u001c-\u001f\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+$/u;
// oxlint-enable no-control-regex
const NUMBER =
  /^(?:0o[0-7]+|[-+]?[0-9]+|0x[0-9a-fA-F]+|[-+]?\.(?:inf|Inf|INF)|\.nan|\.NaN|\.NAN|[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)[eE][-+]?[0-9]+|[-+]?(?:\.[0-9]+|[0-9]+\.[0-9]*))$/u;
type Value = string | boolean | number | null | ReadonlyArray<Value>;

export class EntryError extends Error {
  readonly code: string;
  readonly reason: string;
  readonly line: number | null;
  readonly field: string | null;
  constructor(
    code: string,
    reason: string,
    line: number | null = null,
    field: string | null = null,
  ) {
    super(`${line === null ? "" : `line ${line}: `}${reason}`);
    this.code = code;
    this.reason = reason;
    this.line = line;
    this.field = field;
  }
}

export interface Entry extends EntryKeys {
  readonly id: string;
  readonly version: string;
  readonly text: string;
  readonly mustKnowBefore: boolean;
  readonly file: string;
  readonly self: string;
  readonly target: string | null;
  readonly long: boolean;
  readonly legacy: string | null;
}
export interface CueEntry {
  readonly id: string;
  readonly version: string;
  readonly name: string;
  readonly applies: string;
  readonly text: string;
  readonly file: string;
  readonly self: string;
  readonly target: string | null;
  readonly long: boolean;
  readonly legacy: string | null;
}
export interface Source {
  readonly directory: string;
  readonly registered: boolean;
}
export interface Library {
  readonly entries: ReadonlyArray<Entry>;
  readonly cue: ReadonlyArray<CueEntry>;
  readonly owners: Record<string, string | null>;
  readonly errors: ReadonlyArray<string>;
  readonly legacy: ReadonlyArray<string>;
}
interface Parsed {
  readonly id: string;
  readonly version: string;
  readonly fields: Readonly<Record<string, Value>>;
  readonly body: string;
  readonly long: boolean;
  readonly commands: ReadonlyArray<ReadonlyArray<string>>;
  readonly paths: ReadonlyArray<string>;
  readonly rawErrors: ReadonlyArray<string>;
  readonly mustKnowBefore: boolean;
  readonly name: string | null;
  readonly applies: string | null;
  readonly target: string | null;
  readonly exact: boolean;
  readonly legacy: string | null;
}
export type ParseCache = Map<
  string,
  { readonly stamp: string; strict?: Parsed | EntryError; tolerant?: Parsed | EntryError }
>;
const trim = (v: string) => v.replace(/^[ \t]+|[ \t]+$/gu, "");

const scalar = (value: string, line: number, tolerant = false): Value => {
  if (value === "") return null;
  let out: string;
  if (value.startsWith("'")) {
    out = "";
    let closed = false;
    for (let i = 1; i < value.length; i++) {
      if (value[i] === "'") {
        if (value[i + 1] === "'") {
          out += "'";
          i++;
          continue;
        }
        if (trim(value.slice(i + 1)) !== "")
          throw new EntryError("syntax", "unexpected text after quoted value", line);
        closed = true;
        break;
      }
      out += value[i];
    }
    if (!closed) throw new EntryError("syntax", "unclosed quoted value", line);
  } else if (value.startsWith('"')) {
    try {
      // YAML permits raw tabs. Preserve existing escapes so backslash + raw tab stays invalid.
      const decoded: unknown = JSON.parse(
        value.replace(/\\[\s\S]|\t/gu, (part) => (part === "\t" ? "\\t" : part)),
      );
      if (typeof decoded !== "string") throw new Error("not a string");
      out = decoded;
    } catch {
      throw new EntryError("syntax", "invalid quoted value", line);
    }
  } else {
    if (PLAIN_INVALID.test(value))
      throw new EntryError("syntax", "invalid character in plain value or comment", line);
    if (/[ \t]#/u.test(value))
      throw new EntryError("syntax", "trailing comments are unsupported; quote the value", line);
    const legacy = "*@`%".includes(value[0]!) || /:[ \t]|:$/u.test(value);
    if ("-?:,[]{}#&*!|>'\"%@`".includes(value[0]!) || legacy) {
      if (
        !(tolerant && legacy && !"['\"".includes(value[0]!) && !"-?:,]{}#&!|>".includes(value[0]!))
      )
        throw new EntryError("syntax", "quote the value", line);
    }
    if (!legacy) {
      if (["~", "null", "Null", "NULL"].includes(value)) return null;
      if (["true", "True", "TRUE", "false", "False", "FALSE"].includes(value))
        return value.toLowerCase() === "true";
      if (NUMBER.test(value)) return 0; // Numeric fields are rejected by the typed header.
    }
    out = value;
  }
  if (CONTROL.test(out) || /[\uD800-\uDFFF]/u.test(out))
    throw new EntryError("syntax", "control character or lone surrogate in value", line);
  return out;
};

const fieldValue = (value: string, line: number, tolerant: boolean): Value => {
  if (!value.startsWith("[")) return scalar(value, line, tolerant);
  if (!value.endsWith("]")) throw new EntryError("syntax", "unclosed flow list", line);
  const inner = value.slice(1, -1);
  if (trim(inner) === "") return [];
  const parts: string[] = [];
  let start = 0,
    quote: string | null = null;
  for (let i = 0; i < inner.length; i++) {
    const c = inner[i]!;
    if (quote !== null) {
      if (quote === '"' && c === "\\") {
        i++;
        continue;
      }
      if (c === quote) {
        if (quote === "'" && inner[i + 1] === "'") {
          i++;
          continue;
        }
        quote = null;
      }
    } else if ((c === "'" || c === '"') && trim(inner.slice(start, i)) === "") quote = c;
    else if ("[]{}".includes(c))
      throw new EntryError("syntax", "nested collections are unsupported", line);
    else if (c === ",") {
      parts.push(trim(inner.slice(start, i)));
      start = i + 1;
    }
  }
  parts.push(trim(inner.slice(start)));
  if (quote !== null || parts.some((p) => p === ""))
    throw new EntryError("syntax", "invalid flow list", line);
  return parts.map((p) => scalar(p, line));
};

const strict = (fileName: string, text: string, version: string, tolerant: boolean): Parsed => {
  text = text.replace(/^\uFEFF/u, "").replace(/\r\n/gu, "\n");
  if (text.includes("\r"))
    throw new EntryError(
      "encoding",
      "bare carriage return",
      text.slice(0, text.indexOf("\r")).split("\n").length,
    );
  const lines = text.split("\n");
  if (lines[0]!.replace(/[ \t]+$/u, "") !== "---")
    throw new EntryError("no_header", "missing front matter");
  const end = lines.findIndex((l, i) => i > 0 && l.replace(/[ \t]+$/u, "") === "---");
  if (end < 0) throw new EntryError("no_header", "front matter is not closed");
  const fields = new Map<string, Value>(),
    locations = new Map<string, number>();
  for (let i = 1; i < end; i++) {
    const line = lines[i]!;
    if (CONTROL.test(line)) throw new EntryError("encoding", "control character in header", i + 1);
    if (trim(line) === "") continue;
    if (/^ *#/u.test(line)) {
      if (PLAIN_INVALID.test(line))
        throw new EntryError("syntax", "invalid character in plain value or comment", i + 1);
      continue;
    }
    const m = /^([A-Za-z_][A-Za-z0-9_-]*):(?:[ \t]+(.*))?$/su.exec(line);
    if (m === null) throw new EntryError("syntax", "expected a field at column zero", i + 1);
    const name = m[1]!,
      value = trim(m[2] ?? "");
    if (fields.has(name) && !(tolerant && KEY_FIELDS.includes(name)))
      throw new EntryError("duplicate_field", "repeated field; use a flow list", i + 1, name);
    const parsed = fieldValue(value, i + 1, tolerant);
    if (fields.has(name)) {
      const old = fields.get(name)!;
      fields.set(name, [
        ...(Array.isArray(old) ? old : [old]),
        ...(Array.isArray(parsed) ? parsed : [parsed]),
      ]);
    } else {
      fields.set(name, parsed);
      locations.set(name, i + 1);
    }
  }
  const unknown = [...fields.keys()].filter((k) => !FIELDS.includes(k)).sort();
  if (unknown.length > 0)
    throw new EntryError("unknown_field", `unknown fields: ${unknown.join(", ")}`);
  for (const name of FIELDS) {
    if (!fields.has(name)) continue;
    const value = fields.get(name)!;
    if (value === null || value === "" || (Array.isArray(value) && value.length === 0))
      throw new EntryError("empty_value", "empty field value", locations.get(name), name);
    const values: ReadonlyArray<Value> =
      Array.isArray(value) && KEY_FIELDS.includes(name) ? value : [value];
    for (const item of values) {
      if (item === null || item === "")
        throw new EntryError("empty_value", "empty field value", locations.get(name), name);
      if (name === "must-know-before" ? typeof item !== "boolean" : typeof item !== "string")
        throw new EntryError("wrong_type", "wrong field type", locations.get(name), name);
      if (name === "command" && typeof item === "string" && trim(item) === "")
        throw new EntryError("empty_value", "empty command key", locations.get(name), name);
      if (name === "error" && typeof item === "string" && ERROR_BLANK.test(item))
        throw new EntryError("empty_value", "empty error key", locations.get(name), name);
    }
  }
  const string = (name: string): string | null => {
    const v = fields.get(name);
    return typeof v === "string" ? v : null;
  };
  const values = (name: string): ReadonlyArray<string> => {
    const v = fields.get(name);
    if (typeof v === "string") return [v];
    return Array.isArray(v) ? v.filter((item): item is string => typeof item === "string") : [];
  };
  const id = string("id") ?? fileName.slice(0, -3);
  if (id === "") throw new EntryError("empty_value", "empty entry id", null, "id");
  if ([...id].length > MAX_ID_CHARS)
    throw new EntryError(
      "id_too_long",
      `id is ${[...id].length} characters, over ${MAX_ID_CHARS}`,
      null,
      "id",
    );
  const body = lines
    .slice(end + 1)
    .join("\n")
    .replace(/^[ \t\n]+|[ \t\n]+$/gu, "");
  if (body === "") throw new EntryError("empty_body", "empty body");
  const exact = KEY_FIELDS.some((k) => fields.has(k)),
    cue = fields.has("name") || fields.has("applies");
  if (!exact && !cue)
    throw new EntryError("no_matcher", "no command, path or error key or Cue fields");
  if (cue && !(fields.has("name") && fields.has("applies")))
    throw new EntryError("cue_incomplete", `no ${fields.has("name") ? "applies" : "name"}`);
  if (fields.get("must-know-before") === true && !exact)
    throw new EntryError("must_know_without_key", "must-know-before needs an exact key");
  return {
    id,
    version,
    fields: Object.fromEntries(fields),
    body,
    long: Buffer.byteLength(body, "utf8") > MAX_ENTRY_BYTES,
    commands: values("command").map((c) => trim(c).split(/[ \t]+/u)),
    paths: values("path"),
    rawErrors: values("error"),
    mustKnowBefore: fields.get("must-know-before") === true,
    name: string("name"),
    applies: string("applies"),
    target: string("target"),
    exact,
    legacy: null,
  };
};

/** Pure parsing stays separate from the filesystem-dependent active targets. */
const parseText = (fileName: string, text: string, version: string, tolerant: boolean): Parsed => {
  try {
    return strict(fileName, text, version, false);
  } catch (error) {
    if (!(error instanceof EntryError) || !tolerant) throw error;
    return { ...strict(fileName, text, version, true), legacy: error.message };
  }
};

export const parseBytes = (
  fileName: string,
  raw: Uint8Array,
  version: string,
  tolerant: boolean,
) => {
  try {
    let text: string;
    try {
      text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw);
    } catch {
      throw new EntryError("encoding", "not UTF-8");
    }
    return parseText(fileName, text, version, tolerant);
  } catch (error) {
    if (!(error instanceof EntryError)) throw error;
    return error;
  }
};

/** The commit checked out in the repository holding `directory`, read without git. */
export const headCommit = Effect.fn("contextmd.headCommit")(function* (directory: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const read = (p: string) =>
    fs.readFileString(p).pipe(
      Effect.map((t) => t.trim()),
      Effect.option,
    );
  let d = yield* fs.realPath(directory).pipe(Effect.orElseSucceed(() => path.resolve(directory)));
  while (true) {
    let dot = path.join(d, ".git");
    const info = yield* fs.stat(dot).pipe(Effect.option);
    if (Option.isSome(info) && info.value.type === "File") {
      const pointer = yield* read(dot);
      if (Option.isNone(pointer)) return null;
      dot = path.resolve(d, pointer.value.split(":").slice(1).join(":").trim());
    }
    if (Option.isSome(info)) {
      const head = yield* read(path.join(dot, "HEAD"));
      if (Option.isNone(head)) return null;
      if (!head.value.startsWith("ref: ")) return head.value;
      const ref = head.value.slice(5);
      const commondir = yield* read(path.join(dot, "commondir"));
      const common = Option.isSome(commondir) ? path.resolve(dot, commondir.value) : dot;
      for (const root of [dot, common]) {
        const sha = yield* read(path.join(root, ref));
        if (Option.isSome(sha)) return sha.value;
      }
      const packed = yield* read(path.join(common, "packed-refs"));
      if (Option.isSome(packed)) {
        for (const line of packed.value.split("\n")) {
          const [sha, name] = line.split(" ");
          if (name === ref && sha) return sha;
        }
      }
      return null;
    }
    const parent = path.dirname(d);
    if (parent === d) return null;
    d = parent;
  }
});

/** Discover the checkout containing the joined thread directory, never the hook's cwd. */
export const libraryDir = Effect.fn("contextmd.libraryDir")(function* (threadDir: string) {
  const home = yield* HostProcess.HomeDirectory;
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path;
  const original = yield* fs.realPath(expandHomePath(threadDir, home)).pipe(Effect.option);
  if (Option.isNone(original)) return null;
  const info = yield* fs.stat(original.value).pipe(Effect.option);
  if (Option.isNone(info) || info.value.type !== "Directory") return null;
  let root = original.value;
  for (let d = original.value; ; d = path.dirname(d)) {
    const dot = yield* fs.stat(path.join(d, ".git")).pipe(Effect.option);
    if (Option.isSome(dot) && (dot.value.type === "File" || dot.value.type === "Directory")) {
      root = d;
      break;
    }
    if (path.dirname(d) === d) break;
  }
  const library = path.join(root, "library"),
    stat = yield* fs.stat(library).pipe(Effect.option);
  return Option.isSome(stat) && stat.value.type === "Directory"
    ? yield* fs.realPath(library).pipe(Effect.orElseSucceed(() => library))
    : null;
});

/** One load returns both matcher views; cached parses never freeze active source or target identity. */
export const load = Effect.fn("contextmd.loadStore")(function* (
  sources: ReadonlyArray<Source | string>,
  cache: ParseCache,
) {
  const home = yield* HostProcess.HomeDirectory;
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    crypto = yield* Crypto.Crypto;
  const normalized = yield* Effect.forEach(
    sources,
    (s) => {
      const source = typeof s === "string" ? { directory: s, registered: true } : s;
      const lexical = path.resolve(expandHomePath(source.directory, home));
      return fs.realPath(lexical).pipe(
        Effect.orElseSucceed(() => lexical),
        Effect.map((directory) => ({ ...source, directory })),
      );
    },
    { concurrency: FILE_CONCURRENCY },
  );
  const registered = new Set(normalized.filter((s) => s.registered).map((s) => s.directory));
  const entries: Entry[] = [],
    cue: CueEntry[] = [],
    owners: Record<string, string | null> = {},
    errors: string[] = [],
    legacy: string[] = [];
  const roots = new Set<string>(),
    exactIds = new Map<string, string>(),
    cueIds = new Map<string, string>();
  const unique = normalized.filter((source) => {
    if (roots.has(source.directory)) return false;
    roots.add(source.directory);
    return true;
  });
  const directories = yield* Effect.forEach(
    unique,
    Effect.fnUntraced(function* (source) {
      const root = source.directory;
      const info = yield* fs.stat(root).pipe(Effect.option);
      if (Option.isNone(info) || info.value.type !== "Directory") {
        return { type: "missing" as const, source };
      }
      const owner = yield* headCommit(root);
      const listed = yield* fs.readDirectory(root).pipe(Effect.option);
      return { type: "directory" as const, source, owner, listed };
    }),
    { concurrency: FILE_CONCURRENCY },
  );
  for (const directory of directories) {
    const { source } = directory;
    const root = source.directory;
    if (directory.type === "missing") {
      if (source.registered) errors.push(`${root}: not a directory`);
      continue;
    }
    owners[root] = directory.owner;
    const listed = directory.listed;
    if (Option.isNone(listed)) {
      errors.push(`${root}: unreadable directory`);
      continue;
    }
    const names = listed.value
      .filter((n) => n.endsWith(".md") && n.toLowerCase() !== "readme.md")
      .sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
    // Process independent files together so a busy event loop does not serialize every callback.
    // Keep results in name order: completion order must never decide which duplicate id wins.
    const loaded = yield* Effect.forEach(
      names,
      Effect.fnUntraced(function* (name) {
        const file = path.join(root, name),
          stat = yield* fs.stat(file).pipe(Effect.option);
        if (Option.isNone(stat)) {
          return { type: "error" as const, file, reason: "unreadable" };
        }
        if (stat.value.type !== "File") return null;
        const self = yield* fs.realPath(file).pipe(Effect.orElseSucceed(() => file));
        const stamp = `${Option.match(stat.value.mtime, { onNone: () => 0, onSome: (d) => d.getTime() })}:${stat.value.size}:${self}`;
        let hit = cache.get(file);
        if (hit === undefined || hit.stamp !== stamp) {
          hit = { stamp };
          cache.set(file, hit);
        }
        const mode = registered.has(root) ? "tolerant" : "strict";
        let result = hit[mode];
        if (result === undefined) {
          const raw = yield* fs.readFile(file).pipe(Effect.option);
          if (Option.isNone(raw)) {
            return { type: "error" as const, file, reason: "unreadable" };
          }
          const version = Hex.encode(yield* crypto.digest("SHA-256", raw.value)).slice(0, 12);
          result = parseBytes(name, raw.value, version, mode === "tolerant");
          hit[mode] = result;
        }
        if (result instanceof EntryError) {
          return { type: "error" as const, file, reason: result.message };
        }
        return { type: "entry" as const, file, self, result };
      }),
      { concurrency: FILE_CONCURRENCY },
    );
    const accepted = loaded.flatMap((item) => {
      if (item === null) return [];
      if (item.type === "error") {
        errors.push(`${item.file}: ${item.reason}`);
        return [];
      }
      const { file, result } = item;
      const prior =
        (result.exact ? exactIds.get(result.id) : undefined) ??
        (result.name !== null ? cueIds.get(result.id) : undefined);
      if (prior !== undefined) {
        errors.push(`${file}: id '${result.id}' already used by ${prior}`);
        return [];
      }
      if (result.exact) exactIds.set(result.id, file);
      if (result.name !== null) cueIds.set(result.id, file);
      return [item];
    });
    // Resolve only accepted targets, with the same fresh identity checks as entry files.
    const resolved = yield* Effect.forEach(
      accepted,
      Effect.fnUntraced(function* ({ file, self, result }) {
        let target: string | null = null;
        if (result.target !== null) {
          const lexical = path.resolve(path.dirname(file), expandHomePath(result.target, home));
          target = yield* fs.realPath(lexical).pipe(Effect.orElseSucceed(() => lexical));
        }
        const common = {
          id: result.id,
          version: result.version,
          text: result.body,
          file,
          self,
          target,
          long: result.long,
          legacy: result.legacy,
        };
        return { result, common };
      }),
      { concurrency: FILE_CONCURRENCY },
    );
    for (const { result, common } of resolved) {
      if (result.exact) {
        entries.push({
          ...common,
          commands: result.commands,
          paths: result.paths,
          errors: result.rawErrors.map(normalizeError),
          mustKnowBefore: result.mustKnowBefore,
        });
      }
      if (result.name !== null && result.applies !== null) {
        cue.push({ ...common, name: result.name, applies: result.applies });
      }
      if (result.legacy !== null) legacy.push(`${common.file}: ${result.legacy}`);
    }
  }
  return { entries, cue, owners, errors, legacy } satisfies Library;
});
