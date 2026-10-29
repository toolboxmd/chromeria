/**
 * ContextMD serving state: a hook posts each moment here; the service matches entries, joins the
 * running Chromeria run, decides what to serve or deny, and reserves it until the hook settles. State
 * and the serving log live in `<stateDir>/contextmd.sqlite`; see `ContextmdRoute` and `Cue.ts`.
 */
import * as NodeSqlite from "node:sqlite";

import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ServerActivation from "../serverActivation.ts";
import {
  COLLECT_MS,
  CueClient,
  PROBE_SECONDS,
  httpTransport,
  transcriptItems,
  type CueReply,
  type TranscriptRow,
} from "./Cue.ts";
import * as Select from "./Select.ts";
import { HARNESSES } from "./Harnesses.ts";
import * as Shown from "./Shown.ts";
import type { Selection } from "./Selection.ts";
import * as Store from "./Store.ts";
import type { ParseCache } from "./Store.ts";
import {
  type CueMoment,
  ServingState,
  type ReadCheck,
  type Reply,
  type RunJoin,
  type Unserved,
  type ShownCheck,
  type SessionInstructions,
} from "./ServingState.ts";

export const MAX_BODY_BYTES = 1_048_576;
/**
 * Every moment replies within this many milliseconds of its arrival, whatever its steps cost. The hook
 * cuts the server off at 2 s; its spawn, connect, queueing and transfer took up to 600 ms at p99
 * (Grok, rocky, 2026-10-13), and 200 ms stay spare.
 */
export const MOMENT_BUDGET_MS = 1200;
/**
 * Kept back from the budget for the reply after the moment's work: the decision itself, and a timer
 * firing late while the event loop is busy (up to 236 ms seen on rocky's Cue collects).
 */
const REPLY_RESERVE_MS = 250;
/** A Cue wait ends this long before the moment's work is cut, so a late score still leaves it served. */
const CUE_MARGIN_MS = 50;
/** Bound authenticated startup payloads that outlive their hook, including outside sessions. */
const STARTUP_TTL_SECONDS = 10;
const STARTUP_LIMIT = 32;
const STARTUP_BYTES = 4 * MAX_BODY_BYTES;
/** Cue's window reads the last user messages, the replies between them and the work since the last one. */
const TRANSCRIPT_USERS = 6;
const TRANSCRIPT_REPLIES = 100;
const TRANSCRIPT_SINCE = 100;
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

type MomentPhase =
  | "authorization"
  | "off_flag"
  | "request"
  | "body"
  | "identity"
  | "config"
  | "run_join"
  | "project_directory"
  | "satrapy_discovery"
  | "library_load"
  | "selection"
  | "pointer_configured"
  | "pointer_prepare"
  | "deny_checks"
  | "read_checks"
  | "cue_window"
  | "cue_wait";

export interface MomentRequest {
  /** Whether the hook's credential may read; checked inside the moment's budget. */
  readonly authorized: Effect.Effect<boolean>;
  readonly harness: string | undefined;
  readonly delivery: string | undefined;
  readonly pointerDelivery?: string | undefined;
  readonly contentLength: number | undefined;
  /** The request body, or null when it exceeds `MAX_BODY_BYTES` or cannot be read. */
  readonly body: Effect.Effect<string | null>;
}

export class StateOpenError extends Schema.TaggedError<StateOpenError>()(
  "ContextmdStateOpenError",
  {
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to open the ContextMD serving state file.";
  }
}

export type MomentResponse = Reply | { readonly status: 400 } | { readonly status: 401 };

export class SessionInstructionsError extends Schema.TaggedError<SessionInstructionsError>()(
  "ContextmdSessionInstructionsError",
  {
    operation: Schema.Literals(["stage", "record", "attach"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to account for native session instructions.";
  }
}

export class PointerDeliveryError extends Schema.TaggedError<PointerDeliveryError>()(
  "ContextmdPointerDeliveryError",
  {
    delivery: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to move the ContextMD pointer delivery.";
  }
}

interface InstructionsStage {
  readonly reservation: string | null;
  readonly text: string | undefined;
}

interface InstallationReceipt {
  readonly nativeSessionId: string;
  readonly text: string;
  readonly site: "open" | "fallback" | "rollback";
}

interface NativeForkRequest {
  readonly driver: string;
  readonly threadId: string;
  readonly runId: string;
  readonly sourceThreadId: string;
  readonly source: OrchestrationV2ProviderThread;
  readonly target: OrchestrationV2ProviderThread;
  readonly boundaryAt: DateTime.Utc;
}

interface PendingStartup {
  readonly session: string;
  readonly request: MomentRequest;
  readonly bytes: number;
  readonly receivedAt: number;
  readonly finished: Deferred.Deferred<void>;
  registeredRunId: string | null;
  phase: "waiting" | "recovering";
}

export class ContextmdServing extends Context.Service<
  ContextmdServing,
  {
    readonly moment: (request: MomentRequest) => Effect.Effect<MomentResponse>;
    /** Called only after the running attempt's native identity has committed. */
    readonly nativeSessionRegistered: (input: {
      readonly nativeSessionId: string;
      readonly runId: string;
    }) => Effect.Effect<void>;
    readonly settle: (delivery: string) => Effect.Effect<{ readonly status: 204 | 404 }>;
    /** The hook dropped a reserved reply before the model saw it. */
    readonly release: (delivery: string) => Effect.Effect<{ readonly status: 204 | 404 }>;
    readonly checkShown: Effect.Effect<void>;
    readonly pointerApplying: (
      delivery: string,
    ) => Effect.Effect<{ readonly status: 204 | 409 }, PointerDeliveryError>;
    readonly pointerSettle: (
      delivery: string,
    ) => Effect.Effect<{ readonly status: 204 | 409 }, PointerDeliveryError>;
    readonly pointerRelease: (
      delivery: string,
      reason: string,
    ) => Effect.Effect<{ readonly status: 204 | 409 }, PointerDeliveryError>;
    readonly currentInstructions: (
      driver: string,
    ) => Effect.Effect<string | undefined, SessionInstructionsError>;
    readonly stageSessionInstructions: (input: {
      readonly driver: string;
      readonly threadId: string;
      readonly runId: string;
    }) => Effect.Effect<InstructionsStage, SessionInstructionsError>;
    readonly recordInstallations: (
      receipts: ReadonlyArray<InstallationReceipt>,
      input: { readonly threadId: string },
    ) => Effect.Effect<void, SessionInstructionsError>;
    readonly attachSessionInstructions: (input: {
      readonly driver: string;
      readonly threadId: string;
      readonly runId: string;
      readonly nativeSessionId: string;
      readonly reservation: string | null;
    }) => Effect.Effect<void, SessionInstructionsError>;
    readonly releaseSessionInstructions: (reservation: string | null) => Effect.Effect<void>;
    readonly recordNativeFork: (fork: NativeForkRequest) => Effect.Effect<void>;
    /** Log a hook call the transport refused for its credential. */
    readonly rejected: (request: {
      readonly harness: string | undefined;
      readonly delivery: string | undefined;
    }) => Effect.Effect<void>;
  }
>()("t3/contextmd/ContextmdServing") {}

/** The self-check: the run-attempt contract this module reads must be present. */
const checkContract = Effect.fn("ContextmdServing.checkContract")(
  function* (sql: SqlClient.SqlClient) {
    const columns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info('orchestration_v2_projection_run_attempts')`;
    const names = new Set(columns.map((c) => c.name));
    if (!["run_id", "thread_id", "status", "payload_json"].every((c) => names.has(c))) return false;
    const [counts] = yield* sql<{ readonly total: number; readonly native: number }>`
      SELECT count(*) AS total,
        count(json_extract(payload_json, '$.nativeThreadId')) AS native
      FROM (SELECT payload_json FROM orchestration_v2_projection_run_attempts ORDER BY rowid DESC LIMIT 50)`;
    return counts === undefined || counts.total === 0 || counts.native > 0;
  },
  Effect.orElseSucceed(() => false),
);

/** The field of each projected item type that holds what the model was shown for the call. */
const RESULT_FIELD: Readonly<Record<string, string>> = {
  command_execution: "output",
  dynamic_tool: "output",
  file_change: "diffStr",
};
/** Item statuses after which the projected result no longer changes. */
const TERMINAL = new Set(["completed", "failed", "cancelled", "interrupted"]);

/**
 * Every string in a finished projected turn item's result field: the text the model was
 * shown for the call, never its input. Null while the item is unfinished or has no result
 * field the module knows, so the deny stays pending.
 */
const shownOutput = (row: {
  readonly type: string;
  readonly status: string;
  readonly payload_json: string;
}) => {
  const field = RESULT_FIELD[row.type];
  if (field === undefined || !TERMINAL.has(row.status)) return null;
  const item = decodeJson(row.payload_json);
  const output =
    Option.isSome(item) && typeof item.value === "object" && item.value !== null
      ? (item.value as Record<string, unknown>)[field]
      : undefined;
  const strings = (value: unknown): string[] =>
    typeof value === "string"
      ? [value]
      : Array.isArray(value)
        ? value.flatMap(strings)
        : typeof value === "object" && value !== null
          ? Object.values(value).flatMap(strings)
          : [];
  return strings(output);
};

const make = Effect.gen(function* () {
  const serviceScope = yield* Effect.scope;
  const config = yield* ServerConfig.ServerConfig;
  const sql = yield* SqlClient.SqlClient;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const dir = path.join(config.stateDir, "contextmd");
  // Serving must never stop Chromeria from starting: a state file that cannot be opened
  // (unwritable, corrupt, from an incompatible build) disables serving instead.
  const opened = yield* fileSystem.makeDirectory(dir, { recursive: true }).pipe(
    Effect.andThen(
      Effect.try({
        try: () => {
          const db = new NodeSqlite.DatabaseSync(path.join(config.stateDir, "contextmd.sqlite"));
          try {
            return { db, state: new ServingState(db) };
          } catch (cause) {
            db.close();
            throw cause;
          }
        },
        catch: (cause) => new StateOpenError({ cause }),
      }),
    ),
    Effect.asSome,
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD serving is disabled: its state file could not be opened", {
        cause,
      }).pipe(Effect.as(Option.none<{ db: NodeSqlite.DatabaseSync; state: ServingState }>())),
    ),
  );
  if (Option.isNone(opened)) {
    return ContextmdServing.of({
      moment: Effect.fn("ContextmdServing.moment")(() => Effect.succeed({ status: 204 } as const)),
      nativeSessionRegistered: Effect.fn("ContextmdServing.nativeSessionRegistered")(
        () => Effect.void,
      ),
      settle: Effect.fn("ContextmdServing.settle")(() => Effect.succeed({ status: 404 } as const)),
      release: Effect.fn("ContextmdServing.release")(() =>
        Effect.succeed({ status: 404 } as const),
      ),
      pointerApplying: Effect.fn("ContextmdServing.pointerApplying")(() =>
        Effect.succeed({ status: 409 } as const),
      ),
      pointerSettle: Effect.fn("ContextmdServing.pointerSettle")(() =>
        Effect.succeed({ status: 409 } as const),
      ),
      pointerRelease: Effect.fn("ContextmdServing.pointerRelease")(() =>
        Effect.succeed({ status: 409 } as const),
      ),
      currentInstructions: Effect.fn("ContextmdServing.currentInstructions")(() =>
        Effect.sync(() => undefined),
      ),
      stageSessionInstructions: Effect.fn("ContextmdServing.stageSessionInstructions")(() =>
        Effect.succeed({ reservation: null, text: undefined }),
      ),
      recordInstallations: Effect.fn("ContextmdServing.recordInstallations")(function* (receipts) {
        if (receipts.length > 0)
          return yield* new SessionInstructionsError({
            operation: "record",
            cause: new Error("Serving state unavailable"),
          });
      }),
      attachSessionInstructions: Effect.fn("ContextmdServing.attachSessionInstructions")(
        function* (input) {
          if (input.driver === "grok")
            return yield* new SessionInstructionsError({
              operation: "attach",
              cause: new Error("Serving state unavailable"),
            });
        },
      ),
      releaseSessionInstructions: Effect.fn("ContextmdServing.releaseSessionInstructions")(
        () => Effect.void,
      ),
      recordNativeFork: Effect.fn("ContextmdServing.recordNativeFork")(() => Effect.void),
      rejected: Effect.fn("ContextmdServing.rejected")(() => Effect.void),
      checkShown: Effect.void,
    });
  }
  const { db, state } = opened.value;
  const clock = yield* Clock.Clock;
  const parsed: ParseCache = new Map();
  const startups = new Map<string, PendingStartup>();
  const recovering = new Map<
    string,
    { readonly runId: string; readonly ready: Deferred.Deferred<void> }
  >();
  // One client per Cue service URL, so its availability is shared by every moment on this machine.
  const cueClients = new Map<string, CueClient>();
  const cueClient = (url: string) => {
    let client = cueClients.get(url);
    if (client === undefined) {
      // A previous process may already have seen this URL fail. Start skipped until health returns.
      const nowMs = () => clock.currentTimeMillisUnsafe();
      client = new CueClient(httpTransport(url), nowMs, !state.cueDown(url), (available) => {
        if (available) state.clearCueDown(url);
        else state.markCueDown(url, nowMs() / 1000);
      });
      cueClients.set(url, client);
    }
    return client;
  };
  const servingConfig = yield* Select.configFile;
  const mapped = yield* checkContract(sql);
  if (!mapped) {
    yield* Effect.logWarning("ContextMD serving is disabled: the run-attempt contract changed");
  }
  yield* Effect.addFinalizer(() => Effect.sync(() => db.close()));

  const nowSeconds = Clock.currentTimeMillis.pipe(Effect.map((ms) => ms / 1000));
  const canServe = () =>
    fileSystem.exists(path.join(dir, "off")).pipe(Effect.map((off) => mapped && !off));
  const withFiles = <A, E>(
    work: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path | Crypto.Crypto>,
  ) =>
    work.pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.provideService(Crypto.Crypto, crypto),
    );
  const instructions = Effect.fn("ContextmdServing.instructions")(function* (
    candidate: NonNullable<Selection["greatLibrary"]>,
  ) {
    const text = Select.greatLibraryText(candidate.text);
    return {
      entry: candidate.entry,
      version: candidate.version,
      text,
      bytes: Buffer.byteLength(text, "utf8"),
      owners: {
        [path.dirname(candidate.entry)]: yield* Store.headCommit(path.dirname(candidate.entry)),
      },
    } satisfies SessionInstructions;
  });
  const currentConfigured = Effect.fn("ContextmdServing.currentConfigured")(function* () {
    if (!(yield* canServe())) return null;
    return yield* Select.loadConfig(servingConfig).pipe(
      Effect.flatMap(Select.configuredGreatLibrary),
    );
  }, withFiles);
  const instructionsError =
    (operation: SessionInstructionsError["operation"]) => (cause: unknown) =>
      new SessionInstructionsError({ operation, cause });

  const currentInstructions: ContextmdServing["Service"]["currentInstructions"] = Effect.fn(
    "ContextmdServing.currentInstructions",
  )(
    function* (driver) {
      if (driver !== "grok" || !(yield* canServe())) return undefined;
      const candidate = yield* Select.loadConfig(servingConfig).pipe(
        Effect.flatMap(Select.prepareGreatLibrary),
      );
      return candidate === null ? undefined : Select.greatLibraryText(candidate.text);
    },
    withFiles,
    Effect.catchCause((cause) => Effect.fail(instructionsError("stage")(cause))),
  );

  const stageSessionInstructions: ContextmdServing["Service"]["stageSessionInstructions"] =
    Effect.fn("ContextmdServing.stageSessionInstructions")(
      function* (input) {
        if (input.driver !== "grok" || !(yield* canServe()))
          return { reservation: null, text: undefined };
        const config = yield* Select.loadConfig(servingConfig);
        const candidate = yield* Select.prepareGreatLibrary(config);
        const current = candidate === null ? null : yield* instructions(candidate);
        const now = yield* nowSeconds;
        state.releaseStale(now);
        const delivery = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
        const reservation = state.stageSessionInstructions({
          ...input,
          delivery,
          current,
          now,
          ...(candidate === null && config.greatLibrary !== undefined
            ? { missing: config.greatLibrary }
            : {}),
        });
        return { reservation, text: reservation === null ? undefined : current?.text };
      },
      withFiles,
      Effect.catchCause((cause) => Effect.fail(instructionsError("stage")(cause))),
    );

  const recordInstallations: ContextmdServing["Service"]["recordInstallations"] = Effect.fn(
    "ContextmdServing.recordInstallations",
  )(
    function* (receipts, input) {
      for (const receipt of receipts) {
        const entry = Select.greatLibraryEntry(receipt.text);
        if (
          entry === null ||
          receipt.text !== Select.greatLibraryText(Select.greatLibraryLine(entry))
        ) {
          return yield* new SessionInstructionsError({
            operation: "record",
            cause: new Error("Invalid native instructions receipt"),
          });
        }
        const candidate = yield* Select.configuredGreatLibrary({
          exactKeys: [],
          cue: [],
          sessionStart: [],
          cueUrl: null,
          greatLibrary: entry,
        });
        const installed = yield* instructions(candidate!);
        state.recordInstallation({
          ...installed,
          session: receipt.nativeSessionId,
          threadId: input.threadId,
          site: receipt.site,
          now: yield* nowSeconds,
        });
      }
    },
    withFiles,
    Effect.catchCause((cause) => Effect.fail(instructionsError("record")(cause))),
  );

  const attachSessionInstructions: ContextmdServing["Service"]["attachSessionInstructions"] =
    Effect.fn("ContextmdServing.attachSessionInstructions")(
      function* (input) {
        if (input.driver !== "grok") return;
        const candidate = yield* currentConfigured();
        const current = candidate === null ? null : yield* instructions(candidate);
        state.attachSessionInstructions({
          ...input,
          session: input.nativeSessionId,
          current,
          delivery: yield* crypto.randomUUIDv4().pipe(Effect.orDie),
          now: yield* nowSeconds,
        });
      },
      withFiles,
      Effect.catchCause((cause) => Effect.fail(instructionsError("attach")(cause))),
    );

  const releaseSessionInstructions: ContextmdServing["Service"]["releaseSessionInstructions"] = (
    reservation,
  ) =>
    Effect.sync(() => {
      if (reservation !== null) state.releaseReservation(reservation);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("ContextMD session stage release failed", { cause }),
      ),
    );

  const recordNativeFork: ContextmdServing["Service"]["recordNativeFork"] = Effect.fn(
    "ContextmdServing.recordNativeFork",
  )(
    function* (fork) {
      if ((fork.driver !== "codex" && fork.driver !== "claudeAgent") || !(yield* canServe()))
        return;
      const harness = fork.driver === "claudeAgent" ? "claude" : fork.driver;
      const source = fork.source.nativeThreadRef?.nativeId;
      const target = fork.target.nativeThreadRef?.nativeId;
      if (source == null || target == null) return;
      const line = yield* Select.loadConfig(servingConfig).pipe(
        Effect.flatMap(Select.prepareGreatLibrary),
      );
      if (line === null) return;
      const reverted = new Set(
        (yield* sql<{ readonly run_id: string }>`SELECT run_id FROM orchestration_v2_projection_runs
      WHERE thread_id = ${fork.sourceThreadId} AND status = 'rolled_back'`).map(
          (row) => row.run_id,
        ),
      );
      const compactions = yield* sql<{
        readonly run_id: string | null;
        readonly started_at: string | null;
      }>`
      SELECT run_id, json_extract(payload_json, '$.startedAt') AS started_at FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${fork.sourceThreadId} AND provider_thread_id = ${fork.source.id} AND type = 'compaction'`;
      const boundaryAt = DateTime.toEpochMillis(fork.boundaryAt) / 1000;
      state.recordNativeFork({
        harness,
        source,
        target,
        threadId: fork.threadId,
        runId: fork.runId,
        line,
        boundaryAt,
        compactions: compactions.map((row) => ({
          runId: row.run_id,
          startedAt: row.started_at === null ? boundaryAt : Date.parse(row.started_at) / 1000,
        })),
        reverted,
        now: yield* nowSeconds,
        delivery: yield* crypto.randomUUIDv4().pipe(Effect.orDie),
      });
    },
    withFiles,
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD native fork record failed", { cause }),
    ),
  );

  const maxRead = 64 * 1024 * 1024;
  const decodeSeen = Schema.decodeUnknownOption(
    Schema.fromJsonString(Schema.Struct({ seen: Schema.String })),
  );
  const recordEvidence = (
    check: ShownCheck,
    evidence: Shown.Evidence,
    checkedAt: number,
    seen?: string,
    retry = false,
  ) => {
    const { fact, ...detail } = evidence;
    state.recordShown(
      check.delivery,
      fact,
      JSON.stringify({
        v: 1,
        ...detail,
        checked_at: checkedAt,
        ...(seen === undefined ? {} : { seen }),
      }),
      retry
        ? checkedAt + Shown.backoff(checkedAt - check.createdAt)
        : Shown.nextCheck(evidence, checkedAt - check.createdAt, checkedAt),
    );
  };
  const checkShown = Effect.gen(function* () {
    const now = yield* nowSeconds;
    const groups = new Map<string, ShownCheck[]>();
    for (const check of state.dueShown(now, 50)) {
      const key = `${check.harness}\0${check.path ?? ""}`;
      const group = groups.get(key);
      if (group === undefined) groups.set(key, [check]);
      else group.push(check);
    }
    let remaining = maxRead;
    for (const group of groups.values()) {
      const source = HARNESSES[group[0]!.harness]?.shownEvidence ?? "none";
      if (source === "none") {
        for (const check of group)
          recordEvidence(check, Shown.unknown(null, "no_evidence_source"), now);
        continue;
      }
      const checks = group.filter((check) => {
        if (Shown.validPath(source, check.path, check.session, path)) return true;
        recordEvidence(check, Shown.unknown(source, "no_transcript"), now);
        return false;
      });
      if (checks.length === 0) continue;
      const file = checks[0]!.path!;
      const stat = yield* fileSystem.stat(file).pipe(Effect.option);
      if (Option.isNone(stat) || stat.value.type !== "File") {
        for (const check of checks)
          recordEvidence(check, Shown.unknown(source, "transcript_unreadable"), now);
        continue;
      }
      const size = Number(stat.value.size);
      if (size > maxRead) {
        for (const check of checks)
          recordEvidence(check, Shown.unknown(source, "transcript_too_large"), now);
        continue;
      }
      const stamp = Option.isSome(stat.value.mtime)
        ? `${Shown.RULE[source]}:${size}:${stat.value.mtime.value.getTime()}`
        : undefined;
      const changed = checks.filter((check) => {
        const seen = check.shownJson === null ? Option.none() : decodeSeen(check.shownJson);
        if (stamp === undefined || Option.isNone(seen) || seen.value.seen !== stamp) return true;
        state.deferShown(check.delivery, now + Shown.backoff(now - check.createdAt));
        return false;
      });
      if (changed.length === 0) continue;
      if (size > remaining) break; // Unread groups stay oldest due for the next tick.
      remaining -= size; // Reserve the complete stat size before I/O, including failed reads.
      let reading = true;
      yield* Effect.gen(function* () {
        const decoder = new TextDecoder("utf-8", { fatal: true });
        const lines: string[] = [];
        let pending = "";
        let bytes = 0;
        yield* fileSystem.stream(file, { bytesToRead: size, chunkSize: 1024 * 1024 }).pipe(
          Stream.runForEach((chunk) =>
            Effect.gen(function* () {
              bytes += chunk.byteLength;
              pending += decoder.decode(chunk, { stream: true });
              const parts = pending.split("\n");
              pending = parts.pop()!;
              lines.push(...parts);
              yield* Effect.yieldNow;
            }),
          ),
        );
        if (bytes !== size) return yield* Effect.fail("incomplete transcript read");
        // A trailing line without a newline is still being written, so it never decides.
        decoder.decode();
        reading = false;
        const claude = source === "claude_request_replay" ? lines.map(Shown.decodeClaudeLine) : [];
        const codex = source === "codex_rollout" ? lines.map(Shown.decodeCodexLine) : [];
        const rivals = new Map<string, Shown.Served[]>();
        for (const check of changed) {
          let sessionRivals = rivals.get(check.session);
          if (sessionRivals === undefined) {
            sessionRivals = state.shownRivals(check.harness, check.session).flatMap((r) => {
              const parsed = Shown.served(r);
              return parsed === null ? [] : [parsed];
            });
            rivals.set(check.session, sessionRivals);
          }
          const delivery = Shown.served(check);
          const result = yield* Effect.try(() =>
            delivery === null
              ? Shown.unknown(source, "unrecognized_format")
              : source === "claude_request_replay"
                ? Shown.claudeEvidence(claude, delivery, sessionRivals)
                : Shown.codexEvidence(codex, delivery, sessionRivals),
          ).pipe(Effect.option);
          const checkedAt = yield* nowSeconds;
          if (Option.isNone(result)) {
            recordEvidence(
              check,
              Shown.unknown(source, "unrecognized_format"),
              checkedAt,
              undefined,
              true,
            );
            yield* Effect.logWarning("ContextMD shown rule failed", { source });
          } else {
            const evidence = result.value;
            recordEvidence(
              check,
              evidence.fact === "shown" && evidence.consumed.at > checkedAt
                ? Shown.unknown(source, "unrecognized_format", evidence.record)
                : evidence,
              checkedAt,
              stamp,
            );
          }
        }
      }).pipe(
        Effect.catchCause(() =>
          Effect.gen(function* () {
            for (const check of changed)
              recordEvidence(
                check,
                Shown.unknown(source, reading ? "transcript_unreadable" : "unrecognized_format"),
                now,
                undefined,
                true,
              );
            yield* Effect.logWarning("ContextMD shown group failed", {
              source,
              checks: changed.length,
            });
          }),
        ),
      );
    }
  }).pipe(Effect.catchCause(() => Effect.logWarning("ContextMD shown sweep failed")));

  yield* ServerActivation.forkParked(checkShown.pipe(Effect.repeat(Schedule.spaced("30 seconds"))));

  yield* ServerActivation.forkParked(
    Effect.gen(function* () {
      yield* sql`CREATE INDEX IF NOT EXISTS contextmd_running_native_thread_idx
        ON orchestration_v2_projection_run_attempts (json_extract(payload_json, '$.nativeThreadId'))
        WHERE status = 'running'`.pipe(
        Effect.catchCause((cause) => Effect.logWarning("ContextMD index not created", { cause })),
      );
      yield* nowSeconds.pipe(
        Effect.flatMap((now) =>
          Effect.sync(() => {
            state.heartbeat(now);
            state.releaseStale(now);
          }),
        ),
        Effect.repeat(Schedule.spaced("30 seconds")),
      );
    }),
  );
  // An unavailable Cue service is probed until it answers again.
  yield* ServerActivation.forkParked(
    Effect.suspend(() =>
      Effect.forEach(
        [...cueClients.values()].filter((c) => !c.isAvailable()),
        (c) => Effect.promise(() => c.probe()),
        { discard: true },
      ),
    ).pipe(
      Effect.catchCause((cause) => Effect.logWarning("ContextMD Cue probe failed", { cause })),
      Effect.repeat(Schedule.spaced(`${PROBE_SECONDS} seconds`)),
    ),
  );

  const resolveRun = (session: string) =>
    sql<{ readonly run_id: string; readonly thread_id: string }>`
      SELECT DISTINCT run_id, thread_id FROM orchestration_v2_projection_run_attempts
      WHERE status = 'running' AND json_extract(payload_json, '$.nativeThreadId') = ${session}`.pipe(
      Effect.map((rows): RunJoin => {
        if (rows.length === 0) return { type: "unjoined" };
        if (rows.length > 1) return { type: "ambiguous" };
        return { type: "run", runId: rows[0]!.run_id, threadId: rows[0]!.thread_id };
      }),
      Effect.orElseSucceed((): RunJoin => ({ type: "unjoined" })),
    );

  const pruneStartups = (now: number) =>
    Effect.sync(() => {
      for (const [delivery, entry] of startups) {
        if (now - entry.receivedAt >= STARTUP_TTL_SECONDS) {
          startups.delete(delivery);
        }
      }
    });
  yield* ServerActivation.forkParked(
    nowSeconds.pipe(Effect.flatMap(pruneStartups), Effect.repeat(Schedule.spaced("1 second"))),
  );

  const threadDirectory = (threadId: string) =>
    sql<{ readonly directory: string | null }>`
      SELECT COALESCE(json_extract(t.payload_json, '$.worktreePath'), p.workspace_root) AS directory
      FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.thread_id = ${threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL`.pipe(
      Effect.map((rows) => rows[0]?.directory ?? null),
      Effect.orElseSucceed(() => null),
    );

  /**
   * Look up the projected turn item of each settled deny in this session: its output is
   * the text the model saw for that call (Claude Code, Grok). Another hook's deny on the
   * same call replaces ours there.
   */
  const checkDenies = Effect.fn("ContextmdServing.checkDenies")(
    function* (harness: string, session: string, threadId: string) {
      const pending = state.pendingChecks(harness, session);
      if (pending.length === 0) return;
      const results = yield* Effect.forEach(pending, (check) =>
        sql<{ readonly type: string; readonly status: string; readonly payload_json: string }>`
          SELECT type, status, payload_json FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId}
            AND json_extract(payload_json, '$.nativeItemRef.nativeId') = ${check.toolUseId}
          LIMIT 1`.pipe(
          Effect.map((rows) => ({
            claim: check.claim,
            output: rows[0] === undefined ? null : shownOutput(rows[0]),
          })),
        ),
      );
      state.applyChecks(results, yield* nowSeconds);
    },
    Effect.catchCause((cause) => Effect.logWarning("ContextMD deny check failed", { cause })),
  );

  /**
   * The projected exit status of earlier calls whose reads wait for proof (Codex reports no
   * failure to its hook): a command item finished with exit code 0 proves them.
   */
  const checkReads = (harness: string, session: string, threadId: string) =>
    Effect.forEach(state.pendingReads(harness, session), (toolUseId) =>
      sql<{ readonly status: string; readonly exit_code: number | null }>`
        SELECT status, json_extract(payload_json, '$.exitCode') AS exit_code
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND type = 'command_execution'
          AND json_extract(payload_json, '$.nativeItemRef.nativeId') = ${toolUseId}
        LIMIT 1`.pipe(
        Effect.map((rows): ReadCheck => {
          const item = rows[0];
          if (item === undefined || !TERMINAL.has(item.status)) return { toolUseId, ok: null };
          // Success needs an explicit exit code 0; a finished item without one proves nothing.
          if (item.status === "completed" && item.exit_code === null)
            return { toolUseId, ok: null };
          return { toolUseId, ok: item.status === "completed" && item.exit_code === 0 };
        }),
      ),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("ContextMD read check failed", { cause }).pipe(
          Effect.as([] as ReadCheck[]),
        ),
      ),
    );

  /**
   * The thread's items Cue's window is built from, oldest first. The window needs the last few user
   * messages and the agent's replies between them, then the agent's work since the current message, so
   * each part is read with its own bound: however long a turn runs, its user message stays in.
   */
  const transcript = Effect.fn("ContextmdServing.transcript")(function* (threadId: string) {
    const users = yield* sql<TranscriptRow>`
        SELECT ordinal, type, payload_json FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND type = 'user_message'
        ORDER BY ordinal DESC LIMIT ${TRANSCRIPT_USERS}`;
    if (users.length === 0) return [];
    const first = users.at(-1)!.ordinal;
    const current = users[0]!.ordinal;
    const replies = yield* sql<TranscriptRow>`
        SELECT ordinal, type, payload_json FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND type = 'assistant_message'
          AND ordinal > ${first} AND ordinal < ${current}
        ORDER BY ordinal DESC LIMIT ${TRANSCRIPT_REPLIES}`;
    const since = yield* sql<TranscriptRow>`
        SELECT ordinal, type, payload_json FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND ordinal > ${current} AND type IN ('assistant_message',
          'command_execution', 'dynamic_tool', 'file_change', 'file_search', 'web_search')
        ORDER BY ordinal DESC LIMIT ${TRANSCRIPT_SINCE}`;
    return transcriptItems(
      [...users, ...replies, ...since].toSorted((a, b) => a.ordinal - b.ordinal),
    );
  });

  /** Ask Cue about this moment, when Cue is configured and the moment is one it scores at. */
  const askCue = Effect.fn("ContextmdServing.askCue")(
    function* (
      harness: string,
      selection: Selection,
      threadId: string,
      config: Select.ServingConfig,
      library: Store.Library,
      waitUntil: number,
      enterPhase: (phase: "cue_window" | "cue_wait") => void,
    ) {
      const m = selection.moment;
      if (config.cueUrl === null || library.cue.length === 0) return null;
      if (!["user_message", "pre_tool", "tool_result", "tool_failure"].includes(m.kind))
        return null;
      if (m.kind !== "user_message" && m.tool_use_id === null) return null;
      const client = cueClient(config.cueUrl);
      const key = `${harness}\0${m.session}\0${m.tool_use_id ?? ""}`;
      const request = (kind: string) =>
        Effect.suspend(() => {
          enterPhase("cue_window");
          return transcript(threadId);
        }).pipe(
          Effect.map((items) => ({
            items,
            moment: {
              kind,
              ...(m.call ? { call: m.call } : {}),
              ...(m.output ? { output: m.output } : {}),
            },
            entries: library.cue.map((e) => ({ id: e.id, doc: `${e.name}: ${e.applies}` })),
          })),
        );
      // The wait is whatever the moment's budget still leaves.
      const left = Effect.map(nowSeconds, (now) =>
        Math.max(0, Math.round((waitUntil - now) * 1000)),
      );
      let reply: CueReply;
      if (m.kind === "tool_result") {
        const wait = Math.min(COLLECT_MS, yield* left);
        enterPhase("cue_wait");
        reply = yield* Effect.promise(() => client.collect(key, wait));
      } else {
        if (m.kind === "tool_failure") client.drop(key);
        const req = yield* request(m.kind);
        if (!req.items.some(([kind]) => kind === "user")) {
          // Nothing to score: the thread has no user message (a session started outside Chromeria).
          reply = { status: "no_window", ms: null, waitMs: 0 };
        } else if (m.kind === "pre_tool") {
          enterPhase("cue_wait");
          reply = client.start(key, req);
        } else {
          const wait = yield* left;
          enterPhase("cue_wait");
          reply = yield* Effect.promise(() => client.score(req, wait));
        }
      }
      return {
        moment: {
          reply,
          candidates: library.cue.map((e) => Select.cueCandidate(e, selection.profile)),
        } satisfies CueMoment,
        owners: library.owners,
      };
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD Cue request failed", { cause }).pipe(
        Effect.as({
          moment: {
            reply: { status: "error", ms: null, waitMs: null },
            candidates: [],
          } satisfies CueMoment,
          owners: {},
        }),
      ),
    ),
  );

  const serveMoment: (
    request: MomentRequest,
    mode?:
      | { readonly type: "hook" }
      | {
          readonly type: "retained_startup";
          readonly runId: string;
          readonly receivedAt: number;
        },
  ) => Effect.Effect<MomentResponse> = Effect.fn("ContextmdServing.serveMoment")(
    function* (
      request: MomentRequest,
      mode:
        | { readonly type: "hook" }
        | {
            readonly type: "retained_startup";
            readonly runId: string;
            readonly receivedAt: number;
          } = { type: "hook" },
    ) {
      let startup: PendingStartup | undefined;
      let waitedForRecovery = false;
      yield* Effect.addFinalizer(() =>
        startup === undefined ? Effect.void : Deferred.succeed(startup.finished, undefined),
      );
      const startedAt = yield* nowSeconds;
      const phaseClock = yield* Clock.Clock;
      let phase: MomentPhase = "authorization";
      let phaseStarted = phaseClock.monotonicTimeNanosUnsafe();
      const completed: Partial<Record<MomentPhase, number>> = {};
      // Phase intervals include scheduling and local work until the next operation starts. The
      // timeout reason reports the pending operation, not whether its delay was I/O or queueing.
      const enterPhase = (next: MomentPhase) => {
        if (next === phase) return;
        const now = phaseClock.monotonicTimeNanosUnsafe();
        completed[phase] = (completed[phase] ?? 0) + Number(now - phaseStarted) / 1_000_000;
        phase = next;
        phaseStarted = now;
      };
      const during = <A, E, R>(next: MomentPhase, effect: Effect.Effect<A, E, R>) =>
        Effect.suspend(() => {
          enterPhase(next);
          return effect;
        });
      const unserved = (status: Unserved["status"], extra: Partial<Unserved> = {}) =>
        nowSeconds.pipe(
          Effect.map((now) => {
            state.logUnserved({
              harness: request.harness ?? null,
              delivery: request.delivery ?? null,
              status,
              now,
              startedAt,
              ...extra,
            });
          }),
        );
      // Everything from arrival to the decision, authentication included, must leave the reply its
      // reserve; past that the moment is cut and not served.
      const workBy = startedAt + (MOMENT_BUDGET_MS - REPLY_RESERVE_MS) / 1000;
      const workLeft = Math.max(0, Math.round((workBy - (yield* nowSeconds)) * 1000));
      // What the moment knew of itself when it was cut, so its log row reads like any other.
      let known: Partial<Unserved> = {};
      const prepared = yield* Effect.gen(function* () {
        if (!(yield* during("authorization", request.authorized))) {
          yield* unserved("unauthorized");
          return { reply: { status: 401 } as const };
        }
        const { delivery, harness } = request;
        if (delivery === undefined || harness === undefined) {
          yield* unserved("bad_request");
          return { reply: { status: 400 } as const };
        }
        if (request.pointerDelivery === delivery || request.pointerDelivery === "") {
          yield* unserved("bad_request");
          return { reply: { status: 400 } as const };
        }
        if (yield* during("off_flag", fileSystem.exists(path.join(dir, "off"))))
          return { reply: { status: 204 } as const };
        enterPhase("request");
        if (request.contentLength !== undefined && request.contentLength > MAX_BODY_BYTES) {
          yield* unserved("oversize");
          return { reply: { status: 204 } as const };
        }
        const replay = state.replayed(delivery, request.pointerDelivery);
        if (replay !== undefined) return { reply: replay };
        state.releaseStale(startedAt);
        if (!mapped) {
          yield* unserved("unmapped");
          return { reply: { status: 204 } as const };
        }
        const body = yield* during("body", request.body);
        if (body === null || Buffer.byteLength(body, "utf8") > MAX_BODY_BYTES) {
          yield* unserved("oversize");
          return { reply: { status: 204 } as const };
        }
        const raw = decodeJson(body);
        if (Option.isNone(raw)) {
          yield* unserved("bad_event", { reason: "json" });
          return { reply: { status: 204 } as const };
        }
        // Known before any file is read; an event it rejects is reported by selection below.
        const identity = yield* during(
          "identity",
          Effect.option(Select.identify(harness, raw.value)),
        );
        if (Option.isSome(identity)) {
          const { kind, session, toolUseId } = identity.value;
          known = { kind, session, toolUseId };
        }
        const selected = yield* during("config", Select.loadConfig(servingConfig)).pipe(
          Effect.flatMap((config) =>
            Effect.gen(function* () {
              const identity = yield* Select.identify(harness, raw.value);
              const join = yield* during(
                "run_join",
                Effect.gen(function* () {
                  let join = yield* resolveRun(identity.session);
                  if (
                    mode.type !== "hook" ||
                    join.type !== "unjoined" ||
                    (identity.kind !== "session_start" && identity.kind !== "user_message")
                  )
                    return join;
                  const starting = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
                  WHERE status = 'starting' LIMIT 1`;
                  if (starting.length === 0) return yield* resolveRun(identity.session);
                  yield* pruneStartups(startedAt);
                  startup = startups.get(delivery);
                  if (startup === undefined) {
                    const bytes = Buffer.byteLength(body, "utf8");
                    while (
                      startups.size >= STARTUP_LIMIT ||
                      [...startups.values()].reduce((sum, e) => sum + e.bytes, bytes) >
                        STARTUP_BYTES
                    ) {
                      const oldest = startups.entries().next().value;
                      if (oldest === undefined) break;
                      startups.delete(oldest[0]);
                    }
                    startup = {
                      session: identity.session,
                      bytes,
                      receivedAt: startedAt,
                      phase: "waiting",
                      registeredRunId: null,
                      request: {
                        ...request,
                        authorized: Effect.succeed(true),
                        body: Effect.succeed(body),
                      },
                      finished: yield* Deferred.make<void>(),
                    };
                    startups.set(delivery, startup);
                  }
                  // Retain before rechecking so a concurrent registration cannot be missed.
                  // No exact identity means we cannot distinguish an outside hook from startup.
                  join = yield* resolveRun(identity.session);
                  return join;
                }),
              );
              if (startup !== undefined && join.type === "unjoined")
                return {
                  type: "retained" as const,
                  status: "unjoined" as const,
                  reason: "awaiting_registration",
                };
              // Registration returns before recovery. The first subsequent hook waits here,
              // inside its own budget; neither the original hook nor recovery waits on itself.
              if (mode.type === "hook" && startup === undefined && join.type === "run") {
                const recovery = recovering.get(identity.session);
                if (recovery?.runId === join.runId) {
                  waitedForRecovery = true;
                  yield* Deferred.await(recovery.ready);
                }
              }
              const directory =
                join.type === "run"
                  ? yield* during("project_directory", threadDirectory(join.threadId))
                  : null;
              const project =
                directory === null
                  ? null
                  : yield* during("satrapy_discovery", Store.libraryDir(directory));
              const sources = [
                ...(project === null ? [] : [{ directory: project, registered: false }]),
                ...config.exactKeys.map((directory) => ({ directory, registered: true })),
                ...config.cue.map((directory) => ({ directory, registered: true })),
              ];
              const library = yield* during("library_load", Store.load(sources, parsed));
              const selection = yield* during(
                "selection",
                Select.select(harness, raw.value, config, library),
              );
              return { type: "selected" as const, selection, config, library, join };
            }),
          ),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.catchTags({
            ContextmdBadConfig: (e) =>
              Effect.succeed({
                type: "error" as const,
                status: "bad_config" as const,
                reason: e.reason,
              }),
            ContextmdBadEvent: (e) =>
              Effect.succeed({
                type: "error" as const,
                status: "bad_event" as const,
                reason: e.reason,
              }),
          }),
        );
        if (selected.type !== "selected") {
          yield* unserved(selected.status, { ...known, reason: selected.reason });
          return { reply: { status: 204 } as const, retainStartup: selected.status === "unjoined" };
        }
        let selection: Selection = selected.selection;
        let join = selected.join;
        const configured = yield* during(
          "pointer_configured",
          withFiles(Select.configuredGreatLibrary(selected.config)),
        );
        if (configured !== null) {
          const m = selection.moment;
          const epoch = state.epochAfter(harness, selection);
          const eligible =
            (m.kind === "user_message" && selection.profile.delivering.includes(m.kind)) ||
            (m.kind === "session_start" &&
              m.source !== null &&
              selection.profile.new_epoch_sources.includes(m.source)) ||
            (m.kind === "compaction" && selection.profile.templates.pointer !== undefined);
          let candidate: Selection["greatLibrary"] = null;
          if (harness === "grok" && m.kind === "compaction") candidate = configured;
          else if (
            join.type === "run" &&
            eligible &&
            state.greatLibraryOwed(harness, m.session, epoch)
          ) {
            candidate = yield* during(
              "pointer_prepare",
              withFiles(Select.prepareGreatLibrary(selected.config)),
            );
            if (candidate === null)
              selection = {
                ...selection,
                notes: [
                  ...selection.notes,
                  {
                    matcher: "great_library",
                    entry: configured.entry,
                    outcome: "not_on_machine",
                  },
                ],
              };
          }
          selection = {
            ...selection,
            greatLibrary: candidate,
            greatLibraryBytes: Buffer.byteLength(Select.greatLibraryText(configured.text), "utf8"),
          };
        }
        if (join.type === "run") known = { ...known, threadId: join.threadId, runId: join.runId };
        let readChecks: ReadonlyArray<ReadCheck> = [];
        let cue: CueMoment | null = null;
        if (join.type === "run") {
          yield* during(
            "deny_checks",
            checkDenies(harness, selection.moment.session, join.threadId),
          );
          readChecks = yield* during(
            "read_checks",
            checkReads(harness, selection.moment.session, join.threadId),
          );
          const asked = yield* askCue(
            harness,
            selection,
            join.threadId,
            selected.config,
            selected.library,
            workBy - CUE_MARGIN_MS / 1000,
            enterPhase,
          ).pipe(
            Effect.provideService(FileSystem.FileSystem, fileSystem),
            Effect.provideService(Path.Path, path),
            Effect.provideService(Crypto.Crypto, crypto),
          );
          if (asked !== null) {
            cue = asked.moment;
            selection = { ...selection, owners: { ...selection.owners, ...asked.owners } };
          }
        }
        if (mode.type === "retained_startup" || startup !== undefined || waitedForRecovery) {
          const current = yield* during("run_join", resolveRun(selection.moment.session));
          const now = yield* nowSeconds;
          if (
            join.type !== "run" ||
            current.type !== "run" ||
            current.runId !== join.runId ||
            (mode.type === "retained_startup" &&
              (current.runId !== mode.runId || now - mode.receivedAt >= STARTUP_TTL_SECONDS)) ||
            (startup?.registeredRunId != null && current.runId !== startup.registeredRunId)
          ) {
            if (mode.type === "retained_startup") return { reply: { status: 204 } as const };
            join = { type: "unjoined" };
          }
        }
        return { work: { delivery, harness, selection, join, readChecks, cue } };
      }).pipe(Effect.timeoutOption(Duration.millis(workLeft)));
      if (Option.isNone(prepared)) {
        yield* unserved("over_budget", {
          ...known,
          reason: encodeJson({
            phase,
            active_ms: Math.round(
              Number(phaseClock.monotonicTimeNanosUnsafe() - phaseStarted) / 1_000_000,
            ),
            completed_ms: Object.fromEntries(
              Object.entries(completed).map(([name, ms]) => [name, Math.round(ms)]),
            ),
          }),
        });
        return { status: 204 } as const;
      }
      if ("reply" in prepared.value) {
        if (
          startup !== undefined &&
          request.delivery !== undefined &&
          !("retainStartup" in prepared.value && prepared.value.retainStartup)
        )
          startups.delete(request.delivery);
        return prepared.value.reply;
      }
      const { delivery, harness, selection, join, readChecks, cue } = prepared.value.work;
      const now = yield* nowSeconds;
      const reply = state.decide({
        harness,
        delivery,
        ...(request.pointerDelivery === undefined
          ? {}
          : { pointerDelivery: request.pointerDelivery }),
        selection,
        join,
        readChecks,
        cue,
        now,
        startedAt: mode.type === "hook" ? startedAt : mode.receivedAt,
        deliveryMode: mode.type,
      });
      startups.delete(delivery);
      return reply;
    },
    NodeSqliteClient.readOnly,
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD moment failed", { cause }).pipe(
        Effect.as({ status: 204 } as const),
      ),
    ),
    Effect.scoped,
  );

  const recoverStartups: ContextmdServing["Service"]["nativeSessionRegistered"] = Effect.fn(
    "ContextmdServing.recoverStartups",
  )(
    function* (input) {
      yield* pruneStartups(yield* nowSeconds);
      if (startups.size === 0) return;
      const join = yield* resolveRun(input.nativeSessionId);
      if (join.type !== "run" || join.runId !== input.runId) return;
      const pending = [...startups.entries()].filter(
        ([, entry]) =>
          entry.session === input.nativeSessionId &&
          (entry.registeredRunId === null || entry.registeredRunId === input.runId),
      );
      for (const [, entry] of pending) {
        entry.registeredRunId = input.runId;
      }
      for (const [delivery, entry] of pending) {
        // Let the original hook finish before recovering its retained payload.
        yield* Deferred.await(entry.finished);
        if (startups.get(delivery) !== entry || entry.phase === "recovering") continue;
        entry.phase = "recovering";
        yield* serveMoment(entry.request, {
          type: "retained_startup",
          runId: input.runId,
          receivedAt: entry.receivedAt,
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              entry.phase = "waiting";
            }),
          ),
        );
      }
    },
    NodeSqliteClient.readOnly,
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD startup registration failed", { cause }),
    ),
  );

  const nativeSessionRegistered: ContextmdServing["Service"]["nativeSessionRegistered"] = Effect.fn(
    "ContextmdServing.nativeSessionRegistered",
  )(function* (input) {
    if (![...startups.values()].some((entry) => entry.session === input.nativeSessionId)) return;
    if (recovering.get(input.nativeSessionId)?.runId === input.runId) return;
    const recovery = { runId: input.runId, ready: yield* Deferred.make<void>() };
    // Publish the barrier synchronously, before dispatch can produce another hook.
    recovering.set(input.nativeSessionId, recovery);
    yield* recoverStartups(input).pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          if (recovering.get(input.nativeSessionId) === recovery)
            recovering.delete(input.nativeSessionId);
          yield* Deferred.succeed(recovery.ready, undefined);
        }),
      ),
      Effect.forkIn(serviceScope),
    );
  });

  const settle: ContextmdServing["Service"]["settle"] = Effect.fn("ContextmdServing.settle")(
    function* (delivery) {
      const now = yield* nowSeconds;
      return state.settle(delivery, now) ? ({ status: 204 } as const) : ({ status: 404 } as const);
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("ContextMD settle failed", { cause }).pipe(
        Effect.as({ status: 404 } as const),
      ),
    ),
  );

  const release: ContextmdServing["Service"]["release"] = (delivery) =>
    Effect.sync(() =>
      state.releaseReservation(delivery) ? ({ status: 204 } as const) : ({ status: 404 } as const),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("ContextMD release failed", { cause }).pipe(
          Effect.as({ status: 404 } as const),
        ),
      ),
    );

  const rejected: ContextmdServing["Service"]["rejected"] = (request) =>
    nowSeconds.pipe(
      Effect.map((now) =>
        state.logUnserved({
          harness: request.harness ?? null,
          delivery: request.delivery ?? null,
          status: "unauthorized",
          now,
          startedAt: now,
        }),
      ),
      Effect.ignoreCause,
    );

  const pointerTransition = (delivery: string, transition: (now: number) => boolean) =>
    nowSeconds.pipe(
      Effect.map((now) => (transition(now) ? { status: 204 as const } : { status: 409 as const })),
      Effect.catchCause((cause) =>
        Effect.logWarning("ContextMD pointer transition failed", { delivery, cause }).pipe(
          Effect.andThen(Effect.fail(new PointerDeliveryError({ delivery, cause }))),
        ),
      ),
    );
  return ContextmdServing.of({
    moment: Effect.fn("ContextmdServing.moment")((request) => serveMoment(request)),
    nativeSessionRegistered,
    settle,
    release,
    rejected,
    checkShown,
    pointerApplying: Effect.fn("ContextmdServing.pointerApplying")((delivery) =>
      pointerTransition(delivery, (now) => state.markPointerApplying(delivery, now)),
    ),
    pointerSettle: Effect.fn("ContextmdServing.pointerSettle")((delivery) =>
      pointerTransition(delivery, (now) => state.settlePointer(delivery, now)),
    ),
    pointerRelease: Effect.fn("ContextmdServing.pointerRelease")((delivery, reason) =>
      pointerTransition(delivery, () => state.releasePointer(delivery, reason)),
    ),
    currentInstructions,
    stageSessionInstructions,
    recordInstallations,
    attachSessionInstructions,
    releaseSessionInstructions,
    recordNativeFork,
  });
});

export const layer = Layer.effect(ContextmdServing, make);
