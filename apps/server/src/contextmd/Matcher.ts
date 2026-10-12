/**
 * Exact keys from structured tool input: command words, paths and error text.
 *
 * Ported unchanged from ContextMD's `keys.py` (PR #23). A shell command is not
 * parsed. It is cut into words at whitespace, quotes, backslashes and shell
 * punctuation, and a command key matches wherever its words appear in that
 * order, including inside quotes, heredocs, `bash -c` strings and
 * substitutions. The first word of a key also matches an executable path
 * (`/usr/bin/git`). Words that only appear as data therefore also match: a rare
 * false deny costs one model round and still delivers the rule, while modeling
 * shell syntax missed commands that ran.
 */

const WORD_BREAK = /[\s;&|()<>`$"'\\{}]+/u;
const PATH_FIELDS = ["file_path", "notebook_path", "path", "filePath", "target_file"] as const;

/** Tool input whose shape the exact-key matcher cannot read. */
export class Unsupported extends Error {}

export interface Keys {
  readonly words: ReadonlyArray<string>;
  /** Absolute paths, before they are made repository-relative. */
  readonly paths: ReadonlyArray<string>;
  readonly error: string | null;
}

export const words = (command: string) => command.split(WORD_BREAK).filter((w) => w !== "");

const basename = (word: string) => word.slice(word.lastIndexOf("/") + 1);

/** `key` appears as consecutive words; its first word may be an executable path. */
export const hasCommand = (have: ReadonlyArray<string>, key: ReadonlyArray<string>) =>
  have.some(
    (w, i) =>
      (w === key[0] || basename(w) === key[0]) &&
      key.slice(1).every((k, j) => have[i + 1 + j] === k),
  );

/** Error signature: lowercase, digits collapsed, whitespace collapsed. */
export const normalizeError = (text: string) =>
  text.toLowerCase().replace(/\d+/gu, "#").replace(/\s+/gu, " ").trim();

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Keys for one tool call; throws `Unsupported` for shapes it cannot read. */
export const extract = (
  tool: string,
  toolInput: unknown,
  error: string | null,
  shellTools: ReadonlyArray<string>,
): Keys => {
  if (!isObject(toolInput)) throw new Unsupported(`${tool}: input is not an object`);
  const signature = error ? normalizeError(error) : null;
  if (shellTools.includes(tool)) {
    const command = toolInput["command"];
    if (typeof command !== "string") throw new Unsupported(`${tool}: no command`);
    return { words: words(command), paths: [], error: signature };
  }
  const paths = PATH_FIELDS.map((f) => toolInput[f]).filter(
    (v): v is string => typeof v === "string" && v !== "",
  );
  if (paths.length === 0 && signature === null) {
    throw new Unsupported(`${tool}: no command or path`);
  }
  return { words: [], paths, error: signature };
};

const escape = (c: string) => c.replace(/[.*+?^${}()|[\]\\/]/gu, "\\$&");

/** `**` spans directories, `*` and `?` stay inside one path part. */
export const globRegex = (pattern: string) => {
  let out = "";
  let i = 0;
  while (i < pattern.length) {
    if (pattern.startsWith("**/", i)) {
      out += "(?:.*/)?";
      i += 3;
    } else if (pattern.startsWith("**", i)) {
      out += ".*";
      i += 2;
    } else if (pattern[i] === "*") {
      out += "[^/]*";
      i += 1;
    } else if (pattern[i] === "?") {
      out += "[^/]";
      i += 1;
    } else {
      out += escape(pattern[i]!);
      i += 1;
    }
  }
  return new RegExp(`^${out}$`, "u");
};

export interface EntryKeys {
  readonly commands: ReadonlyArray<ReadonlyArray<string>>;
  readonly paths: ReadonlyArray<string>;
  readonly errors: ReadonlyArray<string>;
}

/** The first key of an entry that matches, as `kind:value`, else null. */
export const firstMatch = (entry: EntryKeys, keys: Keys, relativePaths: ReadonlyArray<string>) => {
  for (const prefix of entry.commands) {
    if (hasCommand(keys.words, prefix)) return `command:${prefix.join(" ")}`;
  }
  for (const pattern of entry.paths) {
    const rx = globRegex(pattern);
    if (relativePaths.some((p) => rx.test(p))) return `path:${pattern}`;
  }
  if (keys.error !== null) {
    for (const signature of entry.errors) {
      if (keys.error.includes(signature)) return `error:${signature}`;
    }
  }
  return null;
};
