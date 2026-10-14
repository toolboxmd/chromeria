/**
 * ContextmdServing - ContextMD serving state for this environment.
 *
 * A thin harness hook posts each moment (the raw hook event) here. The
 * service reads what the event is and which of ContextMD's stored entries
 * match (`Select`), joins it to the running Chromeria run, decides what to
 * serve or deny, and reserves it until the hook settles. State and the serving log
 * live in `<stateDir>/contextmd.sqlite`, on a connection of its own.
 *
 * Hooks authenticate as an ordinary Chromeria session (see `ContextmdRoute`),
 * so a hook installed for one base directory never serves through another
 * environment. After activation the service writes a heartbeat every 30 seconds
 * that marks outage windows in the log. If the run-attempt columns it reads are
 * missing, it serves nothing and logs every moment `unmapped`.
 *
 * With Cue configured (`cue` entry directories and `cue_url`), a joined moment also asks the Cue service
 * (`Cue.ts`): a user message and a failed call score at once, a pre-tool moment starts a score its tool
 * result collects. The window items come from the thread's projected turn items. Cue never holds the
 * exact-key lane up: an unreachable service is skipped machine-wide until a probe sees its health
 * document again. The skip is stored in the serving database, so a restarted server does not call
 * Cue on every new process while the service is down.
 *
 * @module ContextmdServing
 */
import * as NodeSqlite from "node:sqlite";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
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
} from "./ServingState.ts";

export const MAX_BODY_BYTES = 1_048_576;
/**
 * Every moment replies within this many milliseconds of its arrival, whatever its steps cost. The hook
 * cuts the server off at 2 s (#248); its spawn, connect, queueing and transfer took up to 600 ms at p99
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
/** Cue's window reads the last user messages, the replies between them and the work since the last one. */
const TRANSCRIPT_USERS = 6;
const TRANSCRIPT_REPLIES = 100;
const TRANSCRIPT_SINCE = 100;
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

export interface MomentRequest {
  /** Whether the hook's credential may read; checked inside the moment's budget. */
  readonly authorized: Effect.Effect<boolean>;
  readonly harness: string | undefined;
  readonly delivery: string | undefined;
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

export class ContextmdServing extends Context.Service<
  ContextmdServing,
  {
    readonly moment: (request: MomentRequest) => Effect.Effect<MomentResponse>;
    readonly settle: (delivery: string) => Effect.Effect<{ readonly status: 204 | 404 }>;
    /** The hook dropped a reserved reply before the model saw it. */
    readonly release: (delivery: string) => Effect.Effect<{ readonly status: 204 | 404 }>;
    /** Log a hook call the transport refused for its credential. */
    readonly rejected: (request: {
      readonly harness: string | undefined;
      readonly delivery: string | undefined;
    }) => Effect.Effect<void>;
  }
>()("t3/contextmd/ContextmdServing") {}

/** The self-check: the run-attempt contract this module reads must be present. */
const checkContract = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const columns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info('orchestration_v2_projection_run_attempts')`;
    const names = new Set(columns.map((c) => c.name));
    if (!["run_id", "thread_id", "status", "payload_json"].every((c) => names.has(c))) return false;
    const [counts] = yield* sql<{ readonly total: number; readonly native: number }>`
      SELECT count(*) AS total,
        count(json_extract(payload_json, '$.nativeThreadId')) AS native
      FROM (SELECT payload_json FROM orchestration_v2_projection_run_attempts ORDER BY rowid DESC LIMIT 50)`;
    return counts === undefined || counts.total === 0 || counts.native > 0;
  }).pipe(Effect.orElseSucceed(() => false));

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
      moment: () => Effect.succeed({ status: 204 } as const),
      settle: () => Effect.succeed({ status: 404 } as const),
      release: () => Effect.succeed({ status: 404 } as const),
      rejected: () => Effect.void,
    });
  }
  const { db, state } = opened.value;
  const clock = yield* Clock.Clock;
  const parsed: ParseCache = new Map();
  const cueParsed: ParseCache = new Map();
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

  /**
   * Look up the projected turn item of each settled deny in this session: its output is
   * the text the model saw for that call (Claude Code, Grok). Another hook's deny on the
   * same call replaces ours there.
   */
  const checkDenies = (harness: string, session: string, threadId: string) =>
    Effect.gen(function* () {
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
    }).pipe(
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
  const transcript = (threadId: string) =>
    Effect.gen(function* () {
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
  const askCue = (
    harness: string,
    selection: Selection,
    threadId: string,
    config: Select.ServingConfig,
    waitUntil: number,
  ) =>
    Effect.gen(function* () {
      const m = selection.moment;
      if (config.cueUrl === null || config.cue.length === 0) return null;
      if (!["user_message", "pre_tool", "tool_result", "tool_failure"].includes(m.kind))
        return null;
      if (m.kind !== "user_message" && m.tool_use_id === null) return null;
      const store = yield* Store.loadCue(config.cue, cueParsed);
      if (store.entries.length === 0) return null;
      const client = cueClient(config.cueUrl);
      const key = `${harness}\0${m.session}\0${m.tool_use_id ?? ""}`;
      const request = (kind: string) =>
        transcript(threadId).pipe(
          Effect.map((items) => ({
            items,
            moment: {
              kind,
              ...(m.call ? { call: m.call } : {}),
              ...(m.output ? { output: m.output } : {}),
            },
            entries: store.entries.map((e) => ({ id: e.id, doc: `${e.name}: ${e.applies}` })),
          })),
        );
      // The wait is whatever the moment's budget still leaves.
      const left = Effect.map(nowSeconds, (now) =>
        Math.max(0, Math.round((waitUntil - now) * 1000)),
      );
      let reply: CueReply;
      if (m.kind === "tool_result") {
        const wait = Math.min(COLLECT_MS, yield* left);
        reply = yield* Effect.promise(() => client.collect(key, wait));
      } else {
        if (m.kind === "tool_failure") client.drop(key);
        const req = yield* request(m.kind);
        if (!req.items.some(([kind]) => kind === "user")) {
          // Nothing to score: the thread has no user message (a session started outside Chromeria).
          reply = { status: "no_window", ms: null, waitMs: 0 };
        } else if (m.kind === "pre_tool") {
          reply = client.start(key, req);
        } else {
          const wait = yield* left;
          reply = yield* Effect.promise(() => client.score(req, wait));
        }
      }
      return {
        moment: {
          reply,
          candidates: store.entries.map(Select.cueCandidate),
        } satisfies CueMoment,
        owners: store.owners,
      };
    }).pipe(
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

  const moment: ContextmdServing["Service"]["moment"] = (request) =>
    Effect.gen(function* () {
      const startedAt = yield* nowSeconds;
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
        if (!(yield* request.authorized)) {
          yield* unserved("unauthorized");
          return { reply: { status: 401 } as const };
        }
        const { delivery, harness } = request;
        if (delivery === undefined || harness === undefined) {
          yield* unserved("bad_request");
          return { reply: { status: 400 } as const };
        }
        if (request.contentLength !== undefined && request.contentLength > MAX_BODY_BYTES) {
          yield* unserved("oversize");
          return { reply: { status: 204 } as const };
        }
        const replay = state.replayed(delivery);
        if (replay !== undefined) return { reply: replay };
        state.releaseStale(startedAt);
        if (!mapped) {
          yield* unserved("unmapped");
          return { reply: { status: 204 } as const };
        }
        const body = yield* request.body;
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
        const identity = yield* Effect.option(Select.identify(harness, raw.value));
        if (Option.isSome(identity)) {
          const { kind, session, toolUseId } = identity.value;
          known = { kind, session, toolUseId };
        }
        const selected = yield* Select.loadConfig(servingConfig).pipe(
          Effect.flatMap((config) =>
            Select.select(harness, raw.value, config, parsed).pipe(
              Effect.map((selection) => ({ selection, config })),
            ),
          ),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.catchTags({
            ContextmdBadConfig: (e) =>
              Effect.succeed({ status: "bad_config" as const, reason: e.reason }),
            ContextmdBadEvent: (e) =>
              Effect.succeed({ status: "bad_event" as const, reason: e.reason }),
          }),
        );
        if (!("selection" in selected)) {
          yield* unserved(selected.status, { reason: selected.reason });
          return { reply: { status: 204 } as const };
        }
        let selection = selected.selection;
        const m = selection.moment;
        const join = yield* resolveRun(m.session);
        if (join.type === "run") known = { ...known, threadId: join.threadId, runId: join.runId };
        let readChecks: ReadonlyArray<ReadCheck> = [];
        let cue: CueMoment | null = null;
        if (join.type === "run") {
          yield* checkDenies(harness, selection.moment.session, join.threadId);
          readChecks = yield* checkReads(harness, selection.moment.session, join.threadId);
          const asked = yield* askCue(
            harness,
            selection,
            join.threadId,
            selected.config,
            workBy - CUE_MARGIN_MS / 1000,
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
        return { work: { delivery, harness, selection, join, readChecks, cue } };
      }).pipe(Effect.timeoutOption(Duration.millis(workLeft)));
      if (Option.isNone(prepared)) {
        yield* unserved("over_budget", known);
        return { status: 204 } as const;
      }
      if ("reply" in prepared.value) return prepared.value.reply;
      const { delivery, harness, selection, join, readChecks, cue } = prepared.value.work;
      const now = yield* nowSeconds;
      return state.decide({
        harness,
        delivery,
        selection,
        join,
        readChecks,
        cue,
        now,
        startedAt,
      });
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("ContextMD moment failed", { cause }).pipe(
          Effect.as({ status: 204 } as const),
        ),
      ),
    );

  const settle: ContextmdServing["Service"]["settle"] = (delivery) =>
    Effect.gen(function* () {
      const now = yield* nowSeconds;
      return state.settle(delivery, now) ? ({ status: 204 } as const) : ({ status: 404 } as const);
    }).pipe(
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

  return ContextmdServing.of({ moment, settle, release, rejected });
});

export const layer = Layer.effect(ContextmdServing, make);
