/**
 * ContextMD's exact-key stores: Markdown entries keyed by command, path or error.
 *
 * Ported from ContextMD's `store.py` (PR #23); the entries stay data files in
 * ContextMD's store directories, and `contextmd-serve check` validates them
 * with the same rules. Each entry is one `.md` file:
 *
 *     ---
 *     id: pr-template
 *     command: gh pr create
 *     must-know-before: true
 *     target: procedures/pull-requests.md
 *     ---
 *     Answer every prompt of the PR template before opening a PR.
 *
 * Cue entries (`loadCue`) live in their own directories, never inside an exact-key directory, because
 * each loader rejects the other's fields:
 *
 *     ---
 *     id: agentsmd-research
 *     name: Research
 *     applies: The request asks a question whose answer is facts beyond what is at hand. Not: ...
 *     target: ../skills/operations/procedures/research/index.md
 *     ---
 *     AgentsMD: follow the research procedure.
 *
 * Cue scores `<name>: <applies>`, the text it was trained on. `target` is optional.
 *
 * Keys may repeat; any one key matching serves the entry. A must-know-before
 * entry with `target` is a pointer rule: its deny names the file and lifts once
 * the agent is seen reading it. Without `target` it is an inline rule. The
 * version is a hash of the file bytes, so an edited entry is new. Stores load
 * on every moment; unchanged files are not parsed again.
 */
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Hex from "effect/encoding/Hex";

import { expandHomePathWith } from "../pathExpansion.ts";
import { normalizeError, type EntryKeys } from "./Matcher.ts";

/** One entry fits the smallest harness preview (Claude Code shows 2 KB of larger context). */
export const MAX_ENTRY_BYTES = 1800;
const MAX_ID_CHARS = 120;
const KEY_FIELDS = new Set(["command", "path", "error"]);
const OTHER_FIELDS = new Set(["id", "must-know-before", "target"]);
const CUE_FIELDS = new Set(["id", "name", "applies", "target"]);

export interface Entry extends EntryKeys {
  readonly id: string;
  readonly version: string;
  readonly text: string;
  readonly mustKnowBefore: boolean;
  readonly file: string;
  readonly target: string | null;
}

export interface Store {
  readonly entries: ReadonlyArray<Entry>;
  readonly owners: Record<string, string | null>;
  readonly errors: ReadonlyArray<string>;
}

export interface CueEntry {
  readonly id: string;
  readonly version: string;
  readonly name: string;
  readonly applies: string;
  readonly text: string;
  readonly file: string;
  readonly target: string | null;
}

export interface CueStore {
  readonly entries: ReadonlyArray<CueEntry>;
  readonly owners: Record<string, string | null>;
  readonly errors: ReadonlyArray<string>;
}

/**
 * Parsed entries by file, kept while the file's size and modification time are unchanged. Each loader
 * (`load`, `loadCue`) needs a cache of its own.
 */
export type ParseCache = Map<
  string,
  { readonly stamp: string; readonly result: Entry | CueEntry | string }
>;

class EntryError extends Error {}

/** An entry file's front-matter fields and body; throws `EntryError` on a malformed entry. */
const frontMatter = (text: string, allowed: ReadonlySet<string>) => {
  const lines = text.split("\n");
  if (lines.length === 0 || lines[0]!.trim() !== "---")
    throw new EntryError("missing front matter");
  const end = lines.indexOf("---", 1);
  if (end < 0) throw new EntryError("front matter is not closed");
  const fields = new Map<string, string[]>();
  for (const line of lines.slice(1, end)) {
    if (line.trim() === "") continue;
    const sep = line.indexOf(":");
    if (sep < 0) throw new EntryError(`not a 'name: value' line: '${line}'`);
    const name = line.slice(0, sep).trim();
    fields.set(name, [...(fields.get(name) ?? []), line.slice(sep + 1).trim()]);
  }
  const unknown = [...fields.keys()].filter((f) => !allowed.has(f));
  if (unknown.length > 0) throw new EntryError(`unknown fields: ${unknown.sort().join(", ")}`);
  const body = lines
    .slice(end + 1)
    .join("\n")
    .trim();
  if (body === "") throw new EntryError("empty body");
  const bytes = Buffer.byteLength(body, "utf8");
  if (bytes > MAX_ENTRY_BYTES)
    throw new EntryError(`body is ${bytes} UTF-8 bytes, over ${MAX_ENTRY_BYTES}`);
  const last = (name: string) => fields.get(name)?.at(-1);
  return { fields, body, last };
};

const entryId = (last: (name: string) => string | undefined, file: string, path: Path.Path) => {
  const id = last("id") ?? path.basename(file, ".md");
  if (id.length > MAX_ID_CHARS)
    throw new EntryError(`id is ${id.length} characters, over ${MAX_ID_CHARS}`);
  return id;
};

const parseText = (file: string, text: string, version: string, path: Path.Path): Entry => {
  const { fields, body, last } = frontMatter(text, new Set([...KEY_FIELDS, ...OTHER_FIELDS]));
  const flag = (last("must-know-before") ?? "false").toLowerCase();
  if (flag !== "true" && flag !== "false")
    throw new EntryError("must-know-before is not true or false");
  let target = last("target") ?? null;
  if (target !== null) {
    if (flag !== "true") throw new EntryError("target is only for must-know-before entries");
    target = path.resolve(path.dirname(file), expandHomePathWith(target, path));
  }
  const entry: Entry = {
    id: entryId(last, file, path),
    version,
    text: body,
    mustKnowBefore: flag === "true",
    file,
    target,
    commands: (fields.get("command") ?? [])
      .map((c) => c.split(/\s+/u).filter(Boolean))
      .filter((c) => c.length > 0),
    paths: (fields.get("path") ?? []).filter(Boolean),
    errors: (fields.get("error") ?? []).filter(Boolean).map(normalizeError),
  };
  if (entry.commands.length + entry.paths.length + entry.errors.length === 0) {
    throw new EntryError("no command, path or error key");
  }
  return entry;
};

const parseCueText = (file: string, text: string, version: string, path: Path.Path): CueEntry => {
  const { body, last } = frontMatter(text, CUE_FIELDS);
  const name = last("name") ?? "";
  const applies = last("applies") ?? "";
  if (name === "") throw new EntryError("no name");
  if (applies === "") throw new EntryError("no applies");
  const target = last("target");
  return {
    id: entryId(last, file, path),
    version,
    name,
    applies,
    text: body,
    file,
    target: target ? path.resolve(path.dirname(file), expandHomePathWith(target, path)) : null,
  };
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

const loadWith = <E extends Entry | CueEntry>(
  directories: ReadonlyArray<string>,
  cache: ParseCache,
  parse: (file: string, text: string, version: string, path: Path.Path) => E,
  isEntry: (result: Entry | CueEntry | string) => result is E,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const entries: E[] = [];
    const owners: Record<string, string | null> = {};
    const errors: string[] = [];
    const seen = new Map<string, string>();
    for (const directory of directories) {
      const root = expandHomePathWith(directory, path);
      const info = yield* fs.stat(root).pipe(Effect.option);
      if (Option.isNone(info) || info.value.type !== "Directory") {
        errors.push(`${root}: not a directory`);
        continue;
      }
      owners[root] = yield* headCommit(root);
      const names = yield* fs
        .readDirectory(root, { recursive: true })
        .pipe(Effect.orElseSucceed(() => []));
      const files = names
        .filter((n) => n.endsWith(".md"))
        .filter((n) => !n.split("/").includes(".git"))
        .filter((n) => path.basename(n).toLowerCase() !== "readme.md")
        .map((n) => path.join(root, n))
        .sort();
      for (const file of files) {
        const stat = yield* fs.stat(file).pipe(Effect.option);
        if (Option.isNone(stat) || stat.value.type !== "File") continue;
        const mtime = Option.match(stat.value.mtime, {
          onNone: () => 0,
          onSome: (d) => d.getTime(),
        });
        const stamp = `${mtime}:${stat.value.size}`;
        let hit = cache.get(file);
        if (hit === undefined || hit.stamp !== stamp) {
          const raw = yield* fs.readFile(file);
          const version = Hex.encode(yield* crypto.digest("SHA-256", raw)).slice(0, 12);
          let result = yield* Effect.try({
            try: (): E | string =>
              parse(file, new TextDecoder("utf-8", { fatal: true }).decode(raw), version, path),
            catch: (cause) => (cause instanceof EntryError ? cause.message : "not UTF-8"),
          }).pipe(
            Effect.match({
              onFailure: (reason): E | string => reason,
              onSuccess: (entry) => entry,
            }),
          );
          // A target resolves through symlinks, as reads do; a missing target stays as written.
          if (typeof result !== "string" && result.target !== null) {
            const target = result.target;
            result = {
              ...result,
              target: yield* fs.realPath(target).pipe(Effect.orElseSucceed(() => target)),
            };
          }
          hit = { stamp, result };
          cache.set(file, hit);
        }
        const result = hit.result;
        if (typeof result === "string") {
          errors.push(`${file}: ${result}`);
          continue;
        }
        if (!isEntry(result)) continue;
        const prior = seen.get(result.id);
        if (prior !== undefined) {
          errors.push(`${file}: id '${result.id}' already used by ${prior}`);
          continue;
        }
        seen.set(result.id, file);
        entries.push(result);
      }
    }
    return { entries, owners, errors };
  });

const isExactKey = (r: Entry | CueEntry | string): r is Entry =>
  typeof r !== "string" && "mustKnowBefore" in r;
const isCue = (r: Entry | CueEntry | string): r is CueEntry =>
  typeof r !== "string" && "applies" in r;

/** Load every entry of the exact-key store directories. */
export const load = Effect.fn("contextmd.loadStore")(function* (
  directories: ReadonlyArray<string>,
  cache: ParseCache,
) {
  return (yield* loadWith(directories, cache, parseText, isExactKey)) satisfies Store;
});

/** Load every entry of the Cue entry directories. */
export const loadCue = Effect.fn("contextmd.loadCue")(function* (
  directories: ReadonlyArray<string>,
  cache: ParseCache,
) {
  return (yield* loadWith(directories, cache, parseCueText, isCue)) satisfies CueStore;
});
