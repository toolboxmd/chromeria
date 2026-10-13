/**
 * Exact keys from structured tool input: command words, paths and error text.
 *
 * A shell command is not parsed in general. Command keys match on the parts that
 * run (`visibleCommand`, #232): quoted strings and heredoc bodies are only
 * mentions and are skipped, while `sh -c` strings, unescaped `$(...)` and
 * backticks inside double quotes or an unquoted-marker heredoc, and a quoted
 * command word still count. The remaining text is cut into words at
 * whitespace and shell punctuation, and a key matches where its words appear
 * in order; the first word may be an executable path (`/usr/bin/git`), and an
 * unquoted argument still matches a bare key. Path keys see every word (#234).
 * contextmd's `keys.py` follows the same rule.
 */

const WORD_BREAK = /[\s;&|()<>`$"'\\{}]+/u;
const PATH_FIELDS = ["file_path", "notebook_path", "path", "filePath", "target_file"] as const;

/**
 * Words of a shell command that name a file, so `path:` keys match files reached through
 * the shell (#234): anything with a `/` or a leading `~`, a `file:` URI without its query,
 * the value of `VAR=path`, and a bare name with an extension (relative to the cwd). No
 * general shell parsing: a word is a path by its shape. Web URLs are not paths.
 */
const shellPaths = (ws: ReadonlyArray<string>) => {
  const out: string[] = [];
  for (const word of ws) {
    let w = word.replace(/^[A-Za-z_][A-Za-z0-9_]*=/u, "");
    if (w.startsWith("file:")) w = w.replace(/^file:(?:\/\/)?/u, "").replace(/[?#].*$/u, "");
    else if (w.includes("://") || w.startsWith("-")) continue;
    const shaped = w.includes("/") || w.startsWith("~") || /^[^/]+\.[A-Za-z0-9]{1,10}$/u.test(w);
    if (shaped && w !== "" && !out.includes(w)) out.push(w);
  }
  return out;
};

const HEREDOC_START = /^<<-?[ \t]*(['"]?)(\w+)\1/u;
// An escaped `\$(` or `\`` is literal text, not a substitution.
const SUBSTITUTION = /(?<!\\)\$\(([^()]*)\)|(?<!\\)`([^`]*)`/gu;
const substitutions = (text: string) =>
  [...text.matchAll(SUBSTITUTION)].map((m) => visibleCommand(m[1] ?? m[2] ?? "")).join(" ; ");
/** Where a new command starts: the line so far ends at a separator, or holds only a word being built. */
const SEPARATOR = /(?:^|[;&|(\n]|\$\()[ \t]*\S*$/u;
const SHELLS = new Set(["sh", "bash", "zsh", "dash"]);

/**
 * The parts of a shell command that run, for command keys (#232): quoted strings and heredoc
 * bodies are dropped, because a key named there is only mentioned. What bash still runs is
 * kept: the string after `sh -c` (`bash`, `zsh -lc`); `$(...)` and backticks inside double
 * quotes or an unquoted-marker heredoc, unless escaped; and a quoted word in command position
 * (`'gh' pr create`). Unquoted arguments stay, so a bare key still matches a file argument.
 */
const visibleCommand = (command: string): string => {
  const text = command;
  let out = "";
  let i = 0;
  const heredocs: Array<{ word: string; expands: boolean }> = [];
  while (i < text.length) {
    const ch = text[i]!;
    // An unquoted `<<WORD` opens a heredoc: its body, from the next line to WORD, is data.
    const opening =
      ch === "<" && text[i + 1] === "<" && text[i + 2] !== "<"
        ? HEREDOC_START.exec(text.slice(i))
        : null;
    if (opening !== null) {
      heredocs.push({ word: opening[2]!, expands: opening[1] === "" });
      out += " ";
      i += opening[0].length;
      continue;
    }
    if (ch === "\n" && heredocs.length > 0) {
      // Skip each pending heredoc body up to its terminator line.
      let j = i + 1;
      let kept = "";
      for (const { word, expands } of heredocs) {
        const end = new RegExp(`(^|\\n)[ \\t]*${word}[ \\t]*(?=\\n|$)`, "u").exec(text.slice(j));
        const stop = end === null ? text.length : j + end.index + end[0].length;
        // An unquoted marker expands the body: its substitutions run.
        if (expands)
          kept += ` ${substitutions(text.slice(j, end === null ? text.length : j + end.index))} `;
        j = stop;
      }
      heredocs.length = 0;
      out += `${kept}\n`;
      i = j;
      continue;
    }
    if (ch === "\\") {
      out += text.slice(i, i + 2);
      i += 2;
      continue;
    }
    if (ch !== "'" && ch !== '"') {
      out += ch;
      i += 1;
      continue;
    }
    let j = i + 1;
    while (j < text.length && text[j] !== ch) j += ch === '"' && text[j] === "\\" ? 2 : 1;
    const body = text.slice(i + 1, j);
    const before = out.trimEnd().split(/\s+/u);
    const shell = (before.at(-2) ?? "").slice((before.at(-2) ?? "").lastIndexOf("/") + 1);
    if (SHELLS.has(shell) && /^-[a-z]*c$/u.test(before.at(-1) ?? "")) {
      out += ` ${visibleCommand(body)} `;
    } else if (SEPARATOR.test(out) && !/\s/u.test(body)) {
      // A quoted word where a command starts is that command's name: bash runs it.
      out += body;
    } else if (ch === '"') {
      out += ` ${substitutions(body)} `;
    } else {
      out += " ";
    }
    i = j + 1;
  }
  return out;
};

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
    return {
      words: words(visibleCommand(command)),
      paths: shellPaths(words(command)),
      error: signature,
    };
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
