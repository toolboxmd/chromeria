/**
 * Selection: one raw harness hook event in, the moment and its matching entries
 * out. Holds no serving state; `ServingState` decides what to serve.
 *
 * The stores and the session-start files are ContextMD's data, named by
 * ContextMD's serving config (`~/.config/contextmd/serving.json`, or the file in
 * `CONTEXTMD_SERVING_CONFIG`), and are read on every moment, so a knowledge edit
 * applies to the next moment without a release.
 */
import * as Config from "effect/Config";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Hex from "effect/encoding/Hex";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { HARNESSES, type Harness } from "./Harnesses.ts";
import { extract, firstMatch, Unsupported } from "./Matcher.ts";
import { reads } from "./Reads.ts";
import type { Candidate, MomentKind, Note, Profile, Selection } from "./Selection.ts";
import * as Store from "./Store.ts";

// Served text says where it comes from: models treat unexplained text as data
// (obedience test, 2026-10-12: OpenCode doubted a rule that arrived as tool output).
const DENY_HEAD =
  "ContextMD, the user's own rule store, stopped this call: follow this rule from the user, then run the same call again.";
const CONTEXT_HEAD = "ContextMD, the user's own rule store, applies this rule from the user here:";

/** The event name a reply answers, per moment kind. */
const EVENT_OUT: Partial<Record<MomentKind, string>> = {
  session_start: "SessionStart",
  user_message: "UserPromptSubmit",
  pre_tool: "PreToolUse",
  tool_result: "PostToolUse",
  tool_failure: "PostToolUseFailure",
  before_done: "Stop",
};

/** A non-zero exit the command printed itself (`cmd; echo "exit=$?"`), so the call succeeded. */
const EXIT_PRINTED =
  /\bexit(?:[ _-]?(?:code|status))?\s*[=:]\s*([1-9][0-9]{0,2})\b|\bexit[ _-]?(?:code|status)\s+([1-9][0-9]{0,2})\b/iu;

export class BadEvent extends Schema.TaggedError<BadEvent>()("ContextmdBadEvent", {
  reason: Schema.String,
}) {}

export class BadConfig extends Schema.TaggedError<BadConfig>()("ContextmdBadConfig", {
  reason: Schema.String,
}) {}

const ServingConfigFile = Schema.fromJsonString(
  Schema.Struct({
    exact_keys: Schema.optional(Schema.Array(Schema.String)),
    session_start: Schema.optional(
      Schema.Array(
        Schema.Struct({ path: Schema.String, max_chars: Schema.optional(Schema.Number) }),
      ),
    ),
    cue: Schema.optional(Schema.Array(Schema.String)),
    cue_url: Schema.optional(Schema.String),
    great_library: Schema.optional(Schema.String),
  }),
);
const decodeServingConfig = Schema.decodeUnknownEffect(ServingConfigFile, {
  onExcessProperty: "error",
});

export interface ServingConfig {
  readonly exactKeys: ReadonlyArray<string>;
  readonly sessionStart: ReadonlyArray<{ readonly path: string; readonly maxChars: number }>;
  /** Registered Libraries, alongside exactKeys; fields choose each entry's matchers. */
  readonly cue: ReadonlyArray<string>;
  readonly cueUrl: string | null;
  readonly greatLibrary?: string;
}

const GREAT_LIBRARY_PREFIX =
  "The Great Library holds knowledge shared across projects and personal knowledge outside any project. Read ";
const GREAT_LIBRARY_SUFFIX = " in full.";

export const greatLibraryLine = (path: string) =>
  `${GREAT_LIBRARY_PREFIX}${path}${GREAT_LIBRARY_SUFFIX}`;

/** Recover the path from the exact creation receipt, without trimming its Unicode or spaces. */
export const greatLibraryEntry = (text: string) => {
  const line = text.startsWith(`${CONTEXT_HEAD}\n\n`) ? text.slice(CONTEXT_HEAD.length + 2) : text;
  return line.startsWith(GREAT_LIBRARY_PREFIX) && line.endsWith(GREAT_LIBRARY_SUFFIX)
    ? line.slice(GREAT_LIBRARY_PREFIX.length, -GREAT_LIBRARY_SUFFIX.length)
    : null;
};

export const greatLibraryText = (line: string) => `${CONTEXT_HEAD}\n\n${line}`;

/** The configured line, without opening the target. Epoch retention uses this identity. */
export const configuredGreatLibrary = Effect.fn("contextmd.configuredGreatLibrary")(function* (
  config: ServingConfig,
) {
  if (config.greatLibrary === undefined) return null;
  const crypto = yield* Crypto.Crypto;
  const line = greatLibraryLine(config.greatLibrary);
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(line)).pipe(Effect.orDie);
  return {
    matcher: "great_library",
    entry: config.greatLibrary,
    version: Hex.encode(digest).slice(0, 12),
    key: null,
    text: line,
    must_know: false,
    rule: null,
    target: null,
  } satisfies Candidate;
});

/** Only an owed delivery opens the index. A directory or an unreadable target is a miss. */
export const prepareGreatLibrary = Effect.fn("contextmd.prepareGreatLibrary")(function* (
  config: ServingConfig,
) {
  const candidate = yield* configuredGreatLibrary(config);
  if (candidate === null) return null;
  const fs = yield* FileSystem.FileSystem;
  const stat = yield* fs.stat(candidate.entry).pipe(Effect.option);
  return Option.isSome(stat) && stat.value.type === "File" ? candidate : null;
});

/** Where ContextMD's serving config lives: `CONTEXTMD_SERVING_CONFIG`, else the default. */
export const configFile = Config.String("CONTEXTMD_SERVING_CONFIG").pipe(
  Config.withDefault("~/.config/contextmd/serving.json"),
  Effect.orElseSucceed(() => "~/.config/contextmd/serving.json"),
);

/** ContextMD's serving config, read on every moment; a missing file means the defaults. */
export const loadConfig = Effect.fn("contextmd.loadConfig")(function* (file: string) {
  const fs = yield* FileSystem.FileSystem;
  const home = yield* HostProcess.HomeDirectory;
  const text = yield* fs.readFileString(expandHomePath(file, home)).pipe(Effect.option);
  if (Option.isNone(text)) {
    return {
      exactKeys: ["~/.local/share/contextmd/exact-keys"],
      sessionStart: [],
      cue: [],
      cueUrl: null,
    } satisfies ServingConfig;
  }
  const raw = yield* decodeServingConfig(text.value).pipe(
    Effect.mapError(() => new BadConfig({ reason: `${file} is not a valid serving config` })),
  );
  let greatLibrary: string | undefined;
  if (raw.great_library !== undefined) {
    const value = raw.great_library;
    if (!/^(?:\/|~\/)/u.test(value)) {
      return yield* new BadConfig({
        reason: "great_library must be an absolute path or start with ~/",
      });
    }
    // Like Python expanduser, substitute the home without normalizing the user's path.
    greatLibrary = value.startsWith("~/") ? `${home.replace(/\/+$/u, "")}${value.slice(1)}` : value;
    if (/[\p{Cc}\p{Surrogate}\u2028\u2029]/u.test(greatLibrary)) {
      return yield* new BadConfig({
        reason:
          "great_library must be an absolute path without control characters or lone surrogates",
      });
    }
    if (Buffer.byteLength(greatLibrary, "utf8") > 1024) {
      return yield* new BadConfig({
        reason: "great_library expanded path exceeds 1024 UTF-8 bytes",
      });
    }
  }
  return {
    exactKeys: raw.exact_keys ?? ["~/.local/share/contextmd/exact-keys"],
    sessionStart: (raw.session_start ?? []).map((f) => ({
      path: f.path,
      maxChars: f.max_chars ?? 4000,
    })),
    cue: raw.cue ?? [],
    cueUrl: raw.cue_url ?? null,
    ...(greatLibrary === undefined ? {} : { greatLibrary }),
  } satisfies ServingConfig;
});

const at = (raw: unknown, field: string | ReadonlyArray<string> | undefined): unknown => {
  if (field === undefined) return undefined;
  for (const dotted of typeof field === "string" ? [field] : field) {
    let value: unknown = raw;
    for (const part of dotted.split(".")) {
      value =
        typeof value === "object" && value !== null && !Array.isArray(value)
          ? (value as Record<string, unknown>)[part]
          : undefined;
    }
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
};

const text = (value: unknown): string | null =>
  value === undefined || value === null
    ? null
    : typeof value === "string"
      ? value
      : JSON.stringify(value);

const nativeField = (value: unknown) => (typeof value === "string" ? value : null);

/** Every string in a tool output, joined: Claude's {stdout, stderr}, a plain string, a list. */
const outputText = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(outputText).join("\n");
  if (typeof value === "object" && value !== null)
    return Object.values(value).map(outputText).join("\n");
  return "";
};

/** A pointer always recovers the entry's own knowledge when its body is omitted. */
const pointer = (e: Store.Entry | Store.CueEntry) =>
  e.target ? `${e.self} and ${e.target}` : e.self;
const contextText = (e: Store.Entry | Store.CueEntry, room: number) => {
  const text = e.text + (e.target ? ` Read ${e.target} in full.` : "");
  return !e.long && Buffer.byteLength(text, "utf8") <= room ? text : `Read ${pointer(e)} in full.`;
};

const cutDisplay = (value: string, bytes: number) => {
  if (Buffer.byteLength(value, "utf8") <= bytes) return value;
  if (bytes < 6) return "";
  let out = "";
  for (const c of value) {
    if (Buffer.byteLength(out + c, "utf8") > bytes - 6) break;
    out += c;
  }
  return out.replace(/\s+\S*$/u, "") + " [...]";
};
const cueText = (e: Store.CueEntry, room: number) => {
  const chars = [...e.applies];
  let applies =
    chars.length <= 600
      ? e.applies
      : chars
          .slice(0, 595)
          .join("")
          .replace(/\s+\S*$/u, "") + " [...]";
  let name = e.name;
  let body = contextText(e, room);
  const render = () =>
    `${name} applies when: ${applies}\nCue judged that this holds now; if it does not, ignore this. ${body}`;
  applies = cutDisplay(
    applies,
    Math.max(0, Buffer.byteLength(applies, "utf8") + room - Buffer.byteLength(render(), "utf8")),
  );
  name = cutDisplay(
    name,
    Math.max(0, Buffer.byteLength(name, "utf8") + room - Buffer.byteLength(render(), "utf8")),
  );
  if (Buffer.byteLength(render(), "utf8") > room) body = `Read ${pointer(e)} in full.`;
  return render();
};

/** Display metadata may shrink; the document sent to Cue keeps its complete scored text. */
export const cueCandidate = (e: Store.CueEntry, profile: Profile): Candidate => ({
  matcher: "intent",
  entry: e.id,
  version: e.version,
  key: null,
  text: cueText(e, profile.limit_bytes - Buffer.byteLength(profile.heads.context, "utf8") - 2),
  must_know: false,
  rule: null,
  target: e.target,
});

const denyCandidate = (e: Store.Entry, harness: Harness) => {
  if (!e.mustKnowBefore) return { rule: null, target: e.target, deny_text: null };
  const room =
    Math.min(harness.limitBytes, harness.denyLimitBytes ?? harness.limitBytes) -
    Buffer.byteLength(harness.denyHead ?? DENY_HEAD, "utf8") -
    2;
  const full = e.target
    ? `Read ${e.target} in full, then run the same call again.\n${e.text}`
    : e.text;
  if (e.long || (!e.target && Buffer.byteLength(full, "utf8") > room)) {
    return {
      rule: "entry" as const,
      target: e.self,
      opening: `Read ${pointer(e)} in full`,
      deny_text: `Read ${pointer(e)} in full, then run the same call again.`,
    };
  }
  if (e.target) {
    const opening = `Read ${Buffer.byteLength(full, "utf8") <= room ? e.target : `${e.target} and ${e.self}`} in full`;
    return {
      rule: "pointer" as const,
      target: e.target,
      opening,
      deny_text:
        Buffer.byteLength(full, "utf8") <= room
          ? full
          : `Read ${e.target} and ${e.self} in full, then run the same call again.`,
    };
  }
  return { rule: "inline" as const, target: null, deny_text: full };
};

/** A path relative to the repository holding it, else the absolute path. */
const repoRelative = Effect.fn("contextmd.repoRelative")(function* (
  file: string,
  cwd: string | null,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const absolute = path.resolve(cwd ?? "/", expandHomePath(file, home));
  let d = path.dirname(absolute);
  while (true) {
    if (yield* fs.exists(path.join(d, ".git")).pipe(Effect.orElseSucceed(() => false))) {
      return path.relative(d, absolute);
    }
    const parent = path.dirname(d);
    if (parent === d) return absolute;
    d = parent;
  }
});

/**
 * What a hook event says about itself, read before any file is touched: the harness, the moment's
 * kind (a reported or printed failure turns a result into `tool_failure`), the session and the call.
 */
export const identify = Effect.fn("contextmd.identify")(function* (
  harnessName: string,
  raw: unknown,
) {
  const harness: Harness | undefined = HARNESSES[harnessName];
  if (harness === undefined)
    return yield* new BadEvent({ reason: `unknown harness ${harnessName}` });
  const f = harness.fields;
  const event = at(raw, f.event);
  const declared = typeof event === "string" ? harness.events[event] : undefined;
  if (declared === undefined) {
    return yield* new BadEvent({ reason: `${harnessName}: unsupported event ${text(event)}` });
  }
  const session = at(raw, f.session);
  if (typeof session !== "string" || session === "") {
    return yield* new BadEvent({ reason: `${harnessName}: no session id` });
  }
  const output = outputText(at(raw, f.output));
  const notes: Note[] = [];
  let kind = declared;
  let failed: boolean | null = kind === "tool_failure" ? true : null;
  if (kind === "tool_result") {
    if (harness.failureBy === "exit_code") {
      const code = at(raw, f.exitCode);
      failed = typeof code === "number" && code !== 0;
      if (failed) kind = "tool_failure";
    } else if (harness.failureBy === "event") {
      failed = false;
    }
    if (failed !== true && EXIT_PRINTED.test(output)) {
      // The output shows a failure the harness did not report, so error keys and the failure
      // seam apply. It does not decide whether files were read: printed text can be file
      // content, so `failed` keeps the harness's own report.
      kind = "tool_failure";
      notes.push({ matcher: "failure", outcome: "failed_by_output" });
    }
  }
  return {
    harness,
    declared,
    kind,
    failed,
    session,
    toolUseId: text(at(raw, f.toolUseId)),
    output,
    notes,
  };
});

export const select = Effect.fn("contextmd.select")(function* (
  harnessName: string,
  raw: unknown,
  config: ServingConfig,
  library: Store.Library,
) {
  const { harness, declared, kind, failed, session, toolUseId, output, notes } = yield* identify(
    harnessName,
    raw,
  );
  const f = harness.fields;
  const tool = text(at(raw, f.tool));
  const cwd = text(at(raw, f.cwd));
  const candidates: Candidate[] = [];
  const owners: Record<string, string | null> = { ...library.owners };
  const toolKind = kind === "pre_tool" || kind === "tool_result" || kind === "tool_failure";
  const toolInput = at(raw, f.toolInput);
  // A read counts only after a successful call; without a failure signal (Codex) the serving
  // state holds it until the projected command item shows exit code 0.
  const momentReads =
    kind === "tool_result" || kind === "tool_failure"
      ? // The whole event goes in: harnesses keep output and truncation marks in different fields.
        yield* reads(tool ?? "", toolInput, cwd, harness.shellTools, raw)
      : [];

  if (kind === "session_start") {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = yield* HostProcess.HomeDirectory;
    const crypto = yield* Crypto.Crypto;
    for (const file of config.sessionStart) {
      const p = expandHomePath(file.path, home);
      owners[path.dirname(p)] = yield* Store.headCommit(path.dirname(p));
      const content = yield* fs.readFileString(p).pipe(Effect.option);
      if (Option.isNone(content)) {
        notes.push({ matcher: "session_start", entry: p, outcome: "not_on_machine" });
        continue;
      }
      const body = content.value.trim();
      if (body.length > file.maxChars) {
        notes.push({ matcher: "session_start", entry: p, outcome: "cut_budget", reason: "file" });
        continue;
      }
      const digest = yield* crypto
        .digest("SHA-256", new TextEncoder().encode(body))
        .pipe(Effect.orDie);
      candidates.push({
        matcher: "session_start",
        entry: p,
        version: Hex.encode(digest).slice(0, 12),
        key: null,
        text: body,
        must_know: false,
        rule: null,
        target: null,
      });
    }
  } else if (toolKind) {
    // Error keys match the failure text and the output of every call: agents print failures
    // of calls that succeed (`cmd; echo "exit=$?"`), and Codex reports no failure at all.
    const errorText =
      kind === "pre_tool"
        ? null
        : [text(at(raw, f.error)), output].filter((t) => t).join("\n") || null;
    const keys = yield* Effect.try({
      try: () => extract(tool ?? "", toolInput, errorText, harness.shellTools),
      catch: (cause) => (cause instanceof Unsupported ? cause.message : String(cause)),
    }).pipe(
      Effect.catch((reason) =>
        Effect.sync(() => {
          notes.push({ matcher: "exact_key", outcome: "unsupported_key", reason });
          return null;
        }),
      ),
    );
    if (keys !== null) {
      const relative = yield* Effect.forEach(keys.paths, (p) => repoRelative(p, cwd));
      for (const e of library.entries) {
        const key = firstMatch(e, keys, relative);
        if (key === null) continue;
        candidates.push({
          matcher: "exact_key",
          entry: e.id,
          version: e.version,
          key,
          text: contextText(e, harness.limitBytes - Buffer.byteLength(CONTEXT_HEAD, "utf8") - 2),
          must_know: e.mustKnowBefore,
          ...denyCandidate(e, harness),
        });
      }
    }
  }

  return {
    moment: {
      kind,
      event: EVENT_OUT[declared] ?? null,
      session,
      tool_use_id: toolUseId,
      tool,
      source: text(at(raw, f.source)),
      transcript_path: nativeField(at(raw, f.transcript)),
      native_turn: nativeField(at(raw, f.turn)),
      failed,
      reads: momentReads,
      call: toolKind ? `${tool ?? ""}: ${text(toolInput) ?? ""}`.slice(0, 2000) : null,
      output:
        kind === "tool_failure"
          ? [text(at(raw, f.error)), output]
              .filter((t) => t)
              .join("\n")
              .slice(-2000)
          : null,
    },
    candidates,
    notes,
    owners,
    profile: {
      harness: harness.name,
      delivering: harness.delivering,
      carry_to: harness.carryTo,
      limit_bytes: harness.limitBytes,
      settle_within_ms: harness.settleWithinMs,
      session_start_limit_bytes: harness.sessionStartLimitBytes,
      new_epoch_sources: harness.newEpochSources,
      templates: harness.templates,
      heads: { deny: harness.denyHead ?? DENY_HEAD, context: CONTEXT_HEAD },
      deny_evidence: harness.denyEvidence,
      ...(harness.denyLimitBytes === undefined ? {} : { deny_limit_bytes: harness.denyLimitBytes }),
    },
  } satisfies Selection;
});
