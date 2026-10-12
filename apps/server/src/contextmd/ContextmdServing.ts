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
 * @module ContextmdServing
 */
import * as NodeSqlite from "node:sqlite";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
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
import * as Select from "./Select.ts";
import type { ParseCache } from "./Store.ts";
import {
  ServingState,
  type ReadCheck,
  type Reply,
  type RunJoin,
  type Unserved,
} from "./ServingState.ts";

export const MAX_BODY_BYTES = 1_048_576;
const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

export interface MomentRequest {
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

export type MomentResponse = Reply | { readonly status: 400 };

export class ContextmdServing extends Context.Service<
  ContextmdServing,
  {
    readonly moment: (request: MomentRequest) => Effect.Effect<MomentResponse>;
    readonly settle: (delivery: string) => Effect.Effect<{ readonly status: 204 | 404 }>;
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
      rejected: () => Effect.void,
    });
  }
  const { db, state } = opened.value;
  const parsed: ParseCache = new Map();
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
      if (request.delivery === undefined || request.harness === undefined) {
        yield* unserved("bad_request");
        return { status: 400 } as const;
      }
      if (request.contentLength !== undefined && request.contentLength > MAX_BODY_BYTES) {
        yield* unserved("oversize");
        return { status: 204 } as const;
      }
      const replay = state.replayed(request.delivery);
      if (replay !== undefined) return replay;
      state.releaseStale(startedAt);
      if (!mapped) {
        yield* unserved("unmapped");
        return { status: 204 } as const;
      }
      const body = yield* request.body;
      if (body === null || Buffer.byteLength(body, "utf8") > MAX_BODY_BYTES) {
        yield* unserved("oversize");
        return { status: 204 } as const;
      }
      const raw = decodeJson(body);
      if (Option.isNone(raw)) {
        yield* unserved("bad_event", { reason: "json" });
        return { status: 204 } as const;
      }
      const harness = request.harness;
      const selected = yield* Select.loadConfig(servingConfig).pipe(
        Effect.flatMap((config) => Select.select(harness, raw.value, config, parsed)),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, path),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.map((selection) => ({ selection })),
        Effect.catchTags({
          ContextmdBadConfig: (e) =>
            Effect.succeed({ status: "bad_config" as const, reason: e.reason }),
          ContextmdBadEvent: (e) =>
            Effect.succeed({ status: "bad_event" as const, reason: e.reason }),
        }),
      );
      if (!("selection" in selected)) {
        yield* unserved(selected.status, { reason: selected.reason });
        return { status: 204 } as const;
      }
      const selection = selected.selection;
      const join = yield* resolveRun(selection.moment.session);
      let readChecks: ReadonlyArray<ReadCheck> = [];
      if (join.type === "run") {
        yield* checkDenies(harness, selection.moment.session, join.threadId);
        readChecks = yield* checkReads(harness, selection.moment.session, join.threadId);
      }
      const now = yield* nowSeconds;
      return state.decide({
        harness,
        delivery: request.delivery,
        selection,
        join,
        readChecks,
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

  return ContextmdServing.of({ moment, settle, rejected });
});

export const layer = Layer.effect(ContextmdServing, make);
