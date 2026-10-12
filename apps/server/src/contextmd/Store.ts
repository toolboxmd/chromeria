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

/** Parsed entries by file, kept while the file's size and modification time are unchanged. */
export type ParseCache = Map<string, { readonly stamp: string; readonly result: Entry | string }>;

class EntryError extends Error {}

const parseText = (file: string, text: string, version: string, path: Path.Path): Entry => {
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
  const unknown = [...fields.keys()].filter((f) => !KEY_FIELDS.has(f) && !OTHER_FIELDS.has(f));
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
  const flag = (last("must-know-before") ?? "false").toLowerCase();
  if (flag !== "true" && flag !== "false")
    throw new EntryError("must-know-before is not true or false");
  let target = last("target") ?? null;
  if (target !== null) {
    if (flag !== "true") throw new EntryError("target is only for must-know-before entries");
    target = path.resolve(path.dirname(file), expandHomePathWith(target, path));
  }
  const id = last("id") ?? path.basename(file, ".md");
  if (id.length > MAX_ID_CHARS)
    throw new EntryError(`id is ${id.length} characters, over ${MAX_ID_CHARS}`);
  const entry: Entry = {
    id,
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

/** Load every entry of the store directories. */
export const load = Effect.fn("contextmd.loadStore")(function* (
  directories: ReadonlyArray<string>,
  cache: ParseCache,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const entries: Entry[] = [];
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
      const mtime = Option.match(stat.value.mtime, { onNone: () => 0, onSome: (d) => d.getTime() });
      const stamp = `${mtime}:${stat.value.size}`;
      let hit = cache.get(file);
      if (hit === undefined || hit.stamp !== stamp) {
        const raw = yield* fs.readFile(file);
        const version = Hex.encode(yield* crypto.digest("SHA-256", raw)).slice(0, 12);
        let result = yield* Effect.try({
          try: (): Entry | string =>
            parseText(file, new TextDecoder("utf-8", { fatal: true }).decode(raw), version, path),
          catch: (cause) => (cause instanceof EntryError ? cause.message : "not UTF-8"),
        }).pipe(
          Effect.match({
            onFailure: (reason): Entry | string => reason,
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
      if (typeof hit.result === "string") {
        errors.push(`${file}: ${hit.result}`);
        continue;
      }
      const prior = seen.get(hit.result.id);
      if (prior !== undefined) {
        errors.push(`${file}: id '${hit.result.id}' already used by ${prior}`);
        continue;
      }
      seen.set(hit.result.id, file);
      entries.push(hit.result);
    }
  }
  return { entries, owners, errors } satisfies Store;
});
