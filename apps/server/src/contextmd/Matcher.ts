/**
 * Exact keys from structured tool input: command words, paths and error text.
 * Command keys keep unquoted words and quoted words whose dequoted value has no
 * whitespace, drop quoted prose and literal heredoc bodies, and scan executed
 * substitutions and dequoted shell `-c` scripts (#232).
 * Shell names followed by `-c` options count anywhere, including a printed invocation
 * (`echo bash -c '...'`); ContextMD's `keys.py` follows this rule and paths see raw words (#234).
 */

// ContextMD's keys.py uses the same ASCII whitespace set.
const WHITESPACE = " \t\n\r\v\f";
const HAS_WHITESPACE = new RegExp(`[${WHITESPACE}]`, "u");
const WHITESPACE_RUN = new RegExp(`[${WHITESPACE}]+`, "gu");
const WHITESPACE_EDGE = new RegExp(`^[${WHITESPACE}]+|[${WHITESPACE}]+$`, "gu");
const WORD_BREAK = new RegExp("[" + WHITESPACE + ";&|()<>`$\"'\\\\{}]+", "u");
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

// Literal markers use the same ASCII shell boundaries in both languages, including Unicode.
const HEREDOC_START = new RegExp(
  String.raw`^<<(-?)[${WHITESPACE.slice(0, 2)}]*(['"]?)([^${WHITESPACE};&|()<>'"\\]+)\2(?=$|[${WHITESPACE};&|()<>])`,
  "u",
);
const SHELL_BREAK = new RegExp(`[${WHITESPACE};&|()<>]`, "u");
const REDIRECTION = /^(?:[0-9]+)?(<<<|[<>][&<>|]?)/u;

/** A quote inside an expansion belongs to that expansion, not the enclosing string. */
const quotedEnd = (text: string, start: number): number => {
  const quote = text[start];
  const ansi = quote === "'" && text[start - 1] === "$";
  let i = start + 1;
  while (i < text.length) {
    if (text[i] === quote) return i;
    if ((quote === '"' || ansi) && text[i] === "\\") {
      i += 2;
      continue;
    }
    const expansion = quote === '"' ? expansionAt(text, i) : null;
    i = expansion === null ? i + 1 : expansion.end;
  }
  return i;
};

/** Find a balanced expansion boundary, ignoring delimiters in quotes or nested expansions. */
const expansionEnd = (text: string, start: number, close: string): number => {
  let i = start;
  let depth = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\") {
      i += 2;
      continue;
    }
    if (depth === 0 && text.startsWith(close, i)) return i;
    // Legacy backtick boundaries are found before the body is parsed as shell code.
    if (close === "`") {
      i += 1;
      continue;
    }
    if (ch === "'" || ch === '"') {
      i = quotedEnd(text, i) + 1;
      continue;
    }
    const expansion = expansionAt(text, i);
    if (expansion !== null) {
      i = expansion.end;
      continue;
    }
    if (ch === (close === "}" ? "{" : "(")) depth += 1;
    else if (depth > 0 && ch === close[0]) depth -= 1;
    i += 1;
  }
  return text.length;
};

const expansionAt = (
  text: string,
  start: number,
): { body: string; end: number; command: boolean } | null => {
  const backtick = text[start] === "`";
  const arithmetic = text.startsWith("$((", start);
  const parameter = text.startsWith("${", start);
  if (!backtick && !parameter && !text.startsWith("$(", start)) return null;
  const width = backtick ? 1 : arithmetic ? 3 : 2;
  const close = backtick ? "`" : parameter ? "}" : arithmetic ? "))" : ")";
  const stop = expansionEnd(text, start + width, close);
  let body = text.slice(start + width, stop);
  // Backticks remove one escape layer before the shell reads their command body.
  if (backtick) body = body.replace(/\\([$`\\])/gu, "$1");
  return {
    body,
    end: Math.min(stop + close.length, text.length),
    command: !arithmetic && !parameter,
  };
};

/** Keep executable substitutions in expanded data; skip escape pairs so parity is preserved. */
const substitutions = (text: string): string => {
  const out: string[] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] === "\\") {
      i += 2;
      continue;
    }
    const expansion = expansionAt(text, i);
    if (expansion === null) {
      i += 1;
      continue;
    }
    out.push(expansion.command ? visibleCommand(expansion.body) : substitutions(expansion.body));
    i = expansion.end;
  }
  return out.join(" ; ");
};
const SHELLS = new Set(["sh", "bash", "zsh", "dash", "ksh"]);
const ANSI_ESCAPES: Record<string, string> = {
  a: "\x07",
  b: "\b",
  e: "\x1b",
  E: "\x1b",
  f: "\f",
  n: "\n",
  r: "\r",
  t: "\t",
  v: "\v",
  "\\": "\\",
  "'": "'",
  '"': '"',
  "?": "?",
};
const ANSI_NUMBER =
  /^\\(?:([0-7]{1,3})|x([A-Fa-f0-9]{1,2})|u([A-Fa-f0-9]{1,4})|U([A-Fa-f0-9]{1,8}))/u;

/** One shell word: dequote its value, retaining unquoted fragments and executed substitutions. */
const shellWord = (text: string, start: number) => {
  let i = start;
  let quote = "";
  let quoted = false;
  let ansiStart = 0;
  let value = "";
  let plain = "";
  const runs: string[] = [];
  while (i < text.length) {
    const ch = text[i]!;
    if (quote === "" && SHELL_BREAK.test(ch)) break;
    if (ch === quote || (quote === "$'" && ch === "'")) {
      if (quote === "$'") {
        // Bash cannot keep a NUL in a word; only this ANSI-C segment ends there.
        const nul = value.indexOf("\0", ansiStart);
        if (nul >= 0) value = value.slice(0, nul);
      }
      quote = "";
      i += 1;
      continue;
    }
    if (quote === "" && text.startsWith("$'", i)) {
      quote = "$'";
      quoted = true;
      ansiStart = value.length;
      i += 2;
      continue;
    }
    if (quote === "" && (ch === "'" || ch === '"')) {
      quote = ch;
      quoted = true;
      i += 1;
      continue;
    }
    if (ch === "\\" && quote !== "'") {
      const next = text[i + 1] ?? "";
      if (quote === "$'") {
        const numeric = ANSI_NUMBER.exec(text.slice(i));
        if (numeric !== null) {
          const digits = numeric[1] ?? numeric[2] ?? numeric[3] ?? numeric[4]!;
          const code = Number.parseInt(digits, numeric[1] === undefined ? 16 : 8);
          value += code <= 0x10ffff ? String.fromCodePoint(code) : numeric[0];
          i += numeric[0].length;
        } else {
          value += ANSI_ESCAPES[next] ?? ch + next;
          i += 2;
        }
        continue;
      }
      if (quote === "" || '$`"\\\n'.includes(next)) {
        if (next !== "\n") value += next;
      } else value += ch + next;
      if (quote === "" && next !== "\n") plain += next;
      i += 2;
      continue;
    }
    const expansion = quote === "'" || quote === "$'" ? null : expansionAt(text, i);
    if (expansion !== null) {
      const raw = text.slice(i, expansion.end);
      value += raw;
      if (quote === "" && !expansion.command) plain += raw;
      runs.push(expansion.command ? visibleCommand(expansion.body) : substitutions(expansion.body));
      i = expansion.end;
      continue;
    }
    value += ch;
    if (quote === "") plain += ch;
    i += 1;
  }
  return { value, plain, quoted, runs: runs.join(" ; "), end: i };
};

/**
 * Keep single quoted tokens anywhere; only quoted text with whitespace is a mention.
 * Shell names count anywhere; only the first script word after their `-c` options is scanned.
 */
const visibleCommand = (command: string): string => {
  const text = command;
  let out = "";
  let i = 0;
  let shell: "none" | "options" | "script" = "none";
  const heredocs: Array<{ word: string; expands: boolean; stripTabs: boolean }> = [];
  while (i < text.length) {
    const ch = text[i]!;
    const redirection = REDIRECTION.exec(text.slice(i));
    const operator = redirection === null ? i : i + redirection[0].length - redirection[1]!.length;
    // An unquoted `<<WORD` opens a heredoc: its body, from the next line to WORD, is data.
    const opening = redirection?.[1] === "<<" ? HEREDOC_START.exec(text.slice(operator)) : null;
    if (opening !== null) {
      heredocs.push({
        word: opening[3]!,
        expands: opening[2] === "",
        stripTabs: opening[1] === "-",
      });
      out += " ";
      i = operator + opening[0].length;
      continue;
    }
    if (ch === "\n" && heredocs.length > 0) {
      // Skip each pending heredoc body up to its terminator line.
      let j = i + 1;
      let kept = "";
      for (const { word, expands, stripTabs } of heredocs) {
        const end = new RegExp(
          `(^|\\n)${stripTabs ? "\\t*" : ""}${escape(word)}(?=\\n|$)`,
          "u",
        ).exec(text.slice(j));
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
    if (HAS_WHITESPACE.test(ch) || /[;&|()]/u.test(ch)) {
      if (/[\n;&|()]/u.test(ch)) shell = "none";
      out += ch;
      i += 1;
      continue;
    }
    if (redirection !== null) {
      // A here-string consumes one word, without opening a body on following lines.
      out += redirection[0];
      i += redirection[0].length;
      continue;
    }
    const word = shellWord(text, i);
    let kept = word.quoted ? (HAS_WHITESPACE.test(word.value) ? "" : word.value) : word.plain;
    if (shell === "script") {
      kept = visibleCommand(word.value);
      shell = "none";
    } else if (
      (!word.quoted || !HAS_WHITESPACE.test(word.value)) &&
      SHELLS.has(word.value.slice(word.value.lastIndexOf("/") + 1))
    )
      shell = "options";
    else if (shell === "options" && /^-[A-Za-z]*c[A-Za-z]*$/u.test(word.value)) shell = "script";
    out += ` ${kept} ${word.runs} `;
    i = word.end;
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
  text
    .toLowerCase()
    .replace(/\d+/gu, "#")
    .replace(WHITESPACE_RUN, " ")
    .replace(WHITESPACE_EDGE, "");

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
