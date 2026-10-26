/**
 * One environment's ContextMD serving state in `<base>/userdata/contextmd.sqlite`, per ContextMD
 * Spec #3. Each method is one short synchronous transaction, so hooks serialize on the event loop.
 * Budgets key by run; replies stay reserved until settled; must-know denies lift on a full read.
 */
import type * as NodeSqlite from "node:sqlite";

import type { CueReply } from "./Cue.ts";
import type { Candidate, Selection } from "./Selection.ts";

export const SERVE_BUDGET_BYTES = 8_000;
export const DENY_BUDGET_BYTES = 2_000;
const DENY_CAP = 3;
/** A denied call is projected within seconds; after this a deny without its item stays unproven. */
const CHECK_GIVE_UP_SECONDS = 60;
/** Read kinds that lift a pointer rule: the agent saw the whole target. */
const FULL_READS: ReadonlySet<string> = new Set(["read_tool", "shell_full"]);

const SCHEMA = `
CREATE TABLE IF NOT EXISTS sessions (harness TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL,
  PRIMARY KEY (harness, session));
CREATE TABLE IF NOT EXISTS budgets (run_id TEXT PRIMARY KEY, serve_used INTEGER NOT NULL DEFAULT 0,
  deny_used INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS deliveries (delivery TEXT PRIMARY KEY, harness TEXT NOT NULL, moment_id INTEGER,
  stage TEXT NOT NULL, status INTEGER NOT NULL, body TEXT, run_id TEXT, serve_bytes INTEGER NOT NULL DEFAULT 0,
  deny_bytes INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, expires_at REAL NOT NULL, settled_at REAL);
CREATE INDEX IF NOT EXISTS deliveries_reserved ON deliveries(expires_at) WHERE stage = 'reserved';
CREATE TABLE IF NOT EXISTS claims (delivery TEXT NOT NULL, kind TEXT NOT NULL, harness TEXT NOT NULL,
  session TEXT NOT NULL, epoch INTEGER NOT NULL, run_id TEXT, entry TEXT NOT NULL, version TEXT NOT NULL,
  rule TEXT, whole INTEGER NOT NULL, settled INTEGER NOT NULL DEFAULT 0, candidate_id INTEGER NOT NULL,
  evidence TEXT, tool_use_id TEXT, probe TEXT, checked TEXT);
CREATE INDEX IF NOT EXISTS claims_delivery ON claims(delivery);
CREATE INDEX IF NOT EXISTS claims_session_epoch ON claims(harness, session, epoch);
CREATE INDEX IF NOT EXISTS claims_entry ON claims(run_id, epoch, entry, version) WHERE settled = 1;
CREATE TABLE IF NOT EXISTS deny_state (harness TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL,
  entry TEXT NOT NULL, version TEXT NOT NULL, rule_kind TEXT NOT NULL, target TEXT,
  denies INTEGER NOT NULL DEFAULT 0, lifted INTEGER NOT NULL DEFAULT 0, lift_outcome TEXT,
  first_denied_at REAL, lifted_at REAL, PRIMARY KEY (harness, session, epoch, entry, version));
CREATE TABLE IF NOT EXISTS reads (harness TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL,
  path TEXT NOT NULL, kind TEXT NOT NULL, moment_id INTEGER NOT NULL, read_at REAL NOT NULL,
  pending_tool_use_id TEXT);
CREATE INDEX IF NOT EXISTS reads_path ON reads(harness, session, epoch, path);
CREATE TABLE IF NOT EXISTS carries (id INTEGER PRIMARY KEY, harness TEXT NOT NULL, session TEXT NOT NULL,
  run_id TEXT NOT NULL, entry TEXT NOT NULL, version TEXT NOT NULL, text TEXT NOT NULL,
  candidate_id INTEGER NOT NULL, state TEXT NOT NULL, delivery TEXT, created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS carries_session ON carries(harness, session, state);
CREATE TABLE IF NOT EXISTS moments (id INTEGER PRIMARY KEY, received_at REAL NOT NULL, harness TEXT,
  kind TEXT, session TEXT, thread_id TEXT, run_id TEXT, tool_use_id TEXT, epoch INTEGER,
  holdout INTEGER, status TEXT NOT NULL, reason TEXT, owners_json TEXT,
  cue_adapter TEXT, server_ms REAL, delivery TEXT);
CREATE INDEX IF NOT EXISTS moments_thread ON moments(thread_id, received_at);
CREATE INDEX IF NOT EXISTS moments_session ON moments(harness, session, received_at);
CREATE TABLE IF NOT EXISTS candidates (id INTEGER PRIMARY KEY, moment_id INTEGER NOT NULL,
  matcher TEXT NOT NULL, entry TEXT, version TEXT, key TEXT, seam TEXT, stage TEXT, outcome TEXT NOT NULL,
  reason TEXT, score REAL, bytes INTEGER, extra_json TEXT, served_at REAL);
CREATE INDEX IF NOT EXISTS candidates_moment ON candidates(moment_id);
CREATE INDEX IF NOT EXISTS candidates_entry ON candidates(entry, served_at);
CREATE TABLE IF NOT EXISTS heartbeats (at REAL PRIMARY KEY);
CREATE TABLE IF NOT EXISTS cue_down (url TEXT PRIMARY KEY, since REAL NOT NULL);
CREATE TABLE IF NOT EXISTS installations (
  harness TEXT NOT NULL, session TEXT NOT NULL, thread_id TEXT, site TEXT NOT NULL,
  text TEXT NOT NULL, entry TEXT NOT NULL, version TEXT NOT NULL, bytes INTEGER NOT NULL,
  owners_json TEXT, created_at REAL NOT NULL, consumed_run TEXT, consumed_candidate INTEGER,
  PRIMARY KEY (harness, session));
`;

/** Columns added after a table first shipped: created on open when an older state file lacks them. */
const ADDED_COLUMNS: ReadonlyArray<readonly [table: string, column: string, type: string]> = [
  ["moments", "cue_status", "TEXT"],
  ["moments", "cue_ms", "REAL"],
  ["moments", "cue_wait_ms", "REAL"],
  ["moments", "cue_json", "TEXT"],
  ["moments", "source", "TEXT"],
  ["carries", "matcher", "TEXT"],
  ["carries", "score", "REAL"],
  ["deliveries", "transcript_path", "TEXT"],
  ["deliveries", "native_turn", "TEXT"],
  ["deliveries", "shown", "TEXT"],
  ["deliveries", "shown_json", "TEXT"],
  ["deliveries", "shown_next", "REAL"],
  ["candidates", "delivery", "TEXT"],
];

/** Cue's part of one moment: its reply, and the candidate each scored entry would be. */
export interface CueMoment {
  readonly reply: CueReply;
  readonly candidates: ReadonlyArray<Candidate>;
}

/** How the moment joined to Chromeria: a running run, or why it did not. */
export type RunJoin =
  | { readonly type: "run"; readonly runId: string; readonly threadId: string }
  | { readonly type: "unjoined" }
  | { readonly type: "ambiguous" };

export interface MomentInput {
  readonly deliveryMode?: "hook" | "retained_startup";
  readonly harness: string;
  readonly delivery: string;
  readonly pointerDelivery?: string;
  readonly selection: Selection;
  readonly join: RunJoin;
  /** Projected exit status of earlier calls whose reads are pending (Codex). */
  readonly readChecks?: ReadonlyArray<ReadCheck>;
  /** Cue's reply for this moment, when Cue was asked. */
  readonly cue?: CueMoment | null;
  readonly now: number;
  readonly startedAt: number;
}

export type Reply = { readonly status: 200; readonly body: string } | { readonly status: 204 };

/** Immutable bytes installed into a native Grok session, rather than a transient hook reply. */
export interface SessionInstructions {
  readonly entry: string;
  readonly version: string;
  readonly text: string;
  readonly bytes: number;
  readonly owners: Readonly<Record<string, string | null>>;
}

interface NativeForkInput {
  readonly harness: "codex" | "claude";
  readonly source: string;
  readonly target: string;
  readonly threadId: string;
  readonly runId: string;
  readonly line: Candidate;
  readonly boundaryAt: number;
  readonly compactions: ReadonlyArray<{
    readonly runId: string | null;
    readonly startedAt: number;
  }>;
  readonly reverted: ReadonlySet<string>;
  readonly now: number;
  readonly delivery: string;
}

interface InstallationRow {
  readonly entry: string;
  readonly version: string;
  readonly text: string;
  readonly bytes: number;
  readonly owners_json: string | null;
  readonly consumed_run: string | null;
}

/** A moment that was not served: why, and what of it was known by then, for the log. */
export interface Unserved {
  readonly harness: string | null;
  readonly delivery: string | null;
  readonly status:
    | "unjoined"
    | "unmapped"
    | "bad_config"
    | "bad_event"
    | "oversize"
    | "unauthorized"
    | "bad_request"
    | "over_budget";
  readonly reason?: string;
  readonly kind?: string;
  readonly session?: string;
  readonly toolUseId?: string | null;
  readonly threadId?: string | null;
  readonly runId?: string | null;
  readonly now: number;
  readonly startedAt: number;
}

const utf8 = (text: string) => Buffer.byteLength(text, "utf8");
const nativeField = (value: string | null | undefined) =>
  value && utf8(value) <= 4096 ? value : null;

/** A harness's cut mark: Grok ends a clipped hook reason with `… [+N chars]`. */
const CUT_MARK = /…\s*\[\+\d+ chars\]/u;
/**
 * The model saw the start of this deny, cut by the harness: before the cut mark the text
 * holds the probe's opening and then only more of the probe.
 */
const shownCut = (text: string, probe: string) => {
  const mark = CUT_MARK.exec(text);
  if (mark === null) return false;
  const seen = text.slice(0, mark.index);
  const start = seen.indexOf(probe.slice(0, Math.min(40, probe.length)));
  return start >= 0 && probe.startsWith(seen.slice(start));
};

/** The longest prefix of `text` within `bytes` UTF-8 bytes, never splitting a character. */
const cutBytes = (text: string, bytes: number) => {
  const buf = Buffer.from(text, "utf8").subarray(0, Math.max(0, bytes));
  return buf.toString("utf8").replace(/�$/u, "");
};

/** 10% of sessions are the Cue holdout arm, fixed per harness and session (FNV-1a). */
export const isHoldout = (harness: string, session: string) => {
  let hash = 0x811c9dc5;
  for (const byte of Buffer.from(`${harness}\0${session}`, "utf8")) {
    hash = Math.imul(hash ^ byte, 0x01000193) >>> 0;
  }
  return hash % 10 === 0;
};

/** Fill a profile template: string leaves `{{text}}` and `{{event}}` take the values. */
const renderTemplate = (template: unknown, values: { text: string; event: string }): unknown => {
  if (template === "{{text}}") return values.text;
  if (template === "{{event}}") return values.event;
  if (Array.isArray(template)) return template.map((t) => renderTemplate(t, values));
  if (template !== null && typeof template === "object") {
    return Object.fromEntries(
      Object.entries(template).map(([k, v]) => [k, renderTemplate(v, values)]),
    );
  }
  return template;
};

interface CandidateRow {
  readonly delivery?: string;
  readonly matcher: string;
  readonly entry?: string | null;
  readonly version?: string | null;
  readonly key?: string | null;
  readonly seam?: string | null;
  readonly stage?: string | null;
  readonly outcome: string;
  readonly reason?: string | null;
  readonly score?: number | null;
  readonly bytes?: number | null;
  readonly extra?: Record<string, unknown>;
}

/** Collects the parts of one output under the per-output byte limit and one budget. */
class Composer {
  readonly parts: Array<{ text: string; claim: Claim; startup: boolean }> = [];
  private head: string;
  private readonly limit: number;
  private budgetLeft: number;
  private used: number;
  private startupUsed = 0;
  private readonly startupLimit: number;
  constructor(head: string, limit: number, budgetLeft: number, startupLimit = limit) {
    this.head = head;
    this.limit = limit;
    this.startupLimit = Math.max(limit, startupLimit);
    this.budgetLeft = budgetLeft;
    this.used = utf8(head);
  }
  private cost = (text: string) => utf8(text) + 2;
  private outputLimit = (startup: boolean) =>
    startup || this.startupUsed > 0 ? this.startupLimit : this.limit;
  private budgetCost = (text: string) =>
    this.cost(text) + (this.parts.some((p) => !p.startup) ? 0 : utf8(this.head));
  fits = (text: string, startup = false) =>
    !this.overLimit(text, startup) && (startup || this.budgetCost(text) <= this.budgetLeft);
  overLimit = (text: string, startup = false) =>
    this.used + this.cost(text) > this.outputLimit(startup) ||
    (!startup && this.used - this.startupUsed + this.cost(text) > this.limit);
  exceedsLimitAlone = (text: string, startup = false) =>
    utf8(this.head) + this.cost(text) > (startup ? this.startupLimit : this.limit);
  add = (text: string, claim: Claim, startup = false) => {
    if (startup) this.startupUsed += this.cost(text);
    else this.budgetLeft -= this.budgetCost(text);
    this.used += this.cost(text);
    this.parts.push({ text, claim, startup });
  };
  /** Startup files keep their budget exemption even when carried to a later hook. */
  budgetBytes = () => {
    const normal = this.parts.filter((p) => !p.startup);
    if (normal.length === 0) return 0;
    return utf8((this.head ? this.head + "\n\n" : "") + normal.map((p) => p.text).join("\n\n"));
  };
  /** Room left for one more part under the output limit. */
  room = () => this.limit - this.used - 2;
  /** Ordinary text leaves the configured whole pointer's headroom intact. */
  keepBack = (bytes: number) => {
    this.budgetLeft -= bytes;
  };
  /** Give up the head for a first part that cannot fit beside it. */
  dropHead = () => {
    if (this.parts.length > 0) return;
    this.used -= utf8(this.head);
    this.head = "";
  };
  text = () => {
    if (this.parts.length === 0) return null;
    const body = this.parts.map((p) => p.text).join("\n\n");
    return this.head ? `${this.head}\n\n${body}` : body;
  };
}

/** Whether a call with pending reads finished successfully: null while unknown. */
export interface ReadCheck {
  readonly toolUseId: string;
  readonly ok: boolean | null;
}

/** What the projection showed for one settled deny. */
export interface DenyCheck {
  readonly claim: number;
  readonly toolUseId: string;
  readonly probe: string;
}

/** A delivery and its moment, independent of claims that release may remove. */
export interface ShownCheck {
  readonly delivery: string;
  readonly harness: string;
  readonly session: string;
  readonly body: string;
  readonly call: string | null;
  readonly turn: string | null;
  readonly path: string | null;
  readonly createdAt: number;
  readonly settledAt: number | null;
  readonly shownJson: string | null;
}

const SHOWN_SELECT = `SELECT d.delivery, d.harness, m.session, d.body, m.tool_use_id AS call,
  d.native_turn AS turn, d.transcript_path AS path, d.created_at AS createdAt,
  d.settled_at AS settledAt, d.shown_json AS shownJson
  FROM deliveries d JOIN moments m ON m.id = d.moment_id`;

interface Claim {
  readonly kind: "serve" | "deny" | "start";
  readonly entry: string;
  readonly version: string;
  readonly rule: string | null;
  readonly whole: boolean;
  candidateId: number;
  readonly carryId?: number;
  readonly probe?: string;
}

export class ServingState {
  private readonly db: NodeSqlite.DatabaseSync;
  constructor(db: NodeSqlite.DatabaseSync) {
    this.db = db;
    db.exec("PRAGMA journal_mode = WAL");
    db.exec("PRAGMA busy_timeout = 100");
    db.exec(SCHEMA);
    for (const [table, column, type] of ADDED_COLUMNS) {
      const has = (
        db.prepare(`SELECT name FROM pragma_table_info('${table}')`).all() as Array<{
          name: string;
        }>
      ).some((c) => c.name === column);
      if (!has) db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
    }
    db.exec(`CREATE INDEX IF NOT EXISTS deliveries_shown_due
      ON deliveries(shown_next) WHERE shown_next IS NOT NULL`);
    // Before user_version 1, moments.server_ms held seconds; convert those rows once.
    this.tx(() => {
      const { user_version } = this.get<{ user_version: number }>("PRAGMA user_version")!;
      if (user_version < 1) {
        db.exec("UPDATE moments SET server_ms = server_ms * 1000");
        db.exec("PRAGMA user_version = 1");
      }
    });
  }

  private tx<A>(f: () => A): A {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const a = f();
      this.db.exec("COMMIT");
      return a;
    } catch (cause) {
      this.db.exec("ROLLBACK");
      throw cause;
    }
  }

  private get<T>(sql: string, ...params: Array<string | number | null>): T | undefined {
    return this.db.prepare(sql).get(...params) as T | undefined;
  }

  private all<T>(sql: string, ...params: Array<string | number | null>): T[] {
    return this.db.prepare(sql).all(...params) as T[];
  }

  private run(sql: string, ...params: Array<string | number | null>) {
    return this.db.prepare(sql).run(...params);
  }

  /** The stored reply of a delivery id already handled, so a repeated delivery is not charged twice. */
  replayed(delivery: string, pointerDelivery?: string): Reply | undefined {
    const row = this.get<{ status: number; body: string | null }>(
      "SELECT status, body FROM deliveries WHERE delivery = ?",
      delivery,
    );
    if (!row) return undefined;
    const ordinary: Reply =
      row.status === 200 && row.body !== null ? { status: 200, body: row.body } : { status: 204 };
    const pointer =
      pointerDelivery === undefined
        ? undefined
        : this.get<{ status: number; body: string | null }>(
            "SELECT status, body FROM deliveries WHERE delivery = ?",
            pointerDelivery,
          );
    return pointer?.status === 200 && pointer.body !== null
      ? {
          status: 200,
          body: JSON.stringify({
            ...(ordinary.status === 200 ? JSON.parse(ordinary.body) : {}),
            ...JSON.parse(pointer.body),
          }),
        }
      : ordinary;
  }

  heartbeat(now: number) {
    this.run("INSERT OR IGNORE INTO heartbeats (at) VALUES (?)", now);
  }

  /** Whether this machine last saw Cue's connect fail and has not had a health document since. */
  cueDown(url: string): boolean {
    return this.get<{ url: string }>("SELECT url FROM cue_down WHERE url = ?", url) !== undefined;
  }

  /** Remember a connect failure so the next process skips Cue until health returns. */
  markCueDown(url: string, now: number) {
    this.run("INSERT OR REPLACE INTO cue_down (url, since) VALUES (?, ?)", url, now);
  }

  clearCueDown(url: string) {
    this.run("DELETE FROM cue_down WHERE url = ?", url);
  }

  /** Log a moment that was not served for a reason outside selection. */
  logUnserved(u: Unserved) {
    this.tx(() => {
      this.insertMoment({
        harness: u.harness,
        status: u.status,
        delivery: u.delivery,
        reason: u.reason ?? null,
        now: u.now,
        startedAt: u.startedAt,
        ...(u.kind === undefined ? {} : { kind: u.kind }),
        ...(u.session === undefined ? {} : { session: u.session }),
        toolUseId: u.toolUseId ?? null,
        threadId: u.threadId ?? null,
        runId: u.runId ?? null,
      });
    });
  }

  /**
   * The hook dropped this reply before the model saw it. Release that one
   * reservation now, the same way a missed settle deadline does. A settled,
   * already released, or unknown delivery is left alone.
   */
  releaseReservation(delivery: string): boolean {
    return this.tx(() => {
      const d = this.get<{ stage: string }>(
        "SELECT stage FROM deliveries WHERE delivery = ?",
        delivery,
      );
      if (d?.stage !== "reserved" || this.isPointerDelivery(delivery)) return false;
      this.release(delivery);
      return true;
    });
  }

  /** Release every reservation past its settle deadline, as `delivery_unverified`. */
  releaseStale(now: number) {
    this.tx(() => {
      const stale = this.all<{ delivery: string }>(
        "SELECT delivery FROM deliveries WHERE stage = 'reserved' AND expires_at < ?",
        now,
      );
      for (const { delivery } of stale) this.release(delivery);
      this.expireChecks(now);
    });
  }

  /**
   * A settled deny whose evidence never arrived (no projected item, an unknown item type,
   * no tool-use id) stops waiting after `CHECK_GIVE_UP_SECONDS`: logged `unverified` and
   * counted toward the cap, so an agent is never denied forever.
   */
  private expireChecks(now: number) {
    const overdue = this.all<{ claim: number; candidate_id: number }>(
      `SELECT c.rowid AS claim, c.candidate_id FROM claims c JOIN deliveries d ON d.delivery = c.delivery
       WHERE c.kind = 'deny' AND c.settled = 1 AND c.evidence = 'projection' AND c.checked IS NULL
         AND d.created_at < ?`,
      now - CHECK_GIVE_UP_SECONDS,
    );
    for (const o of overdue) {
      if (!this.adjudicate(o.claim, "unverified")) continue;
      this.run("UPDATE candidates SET outcome = 'unverified' WHERE id = ?", o.candidate_id);
      this.denyShown(o.claim, now, { lift: false });
    }
  }

  private release(delivery: string) {
    const d = this.get<{
      run_id: string | null;
      serve_bytes: number;
      deny_bytes: number;
      moment_id: number;
    }>(
      "SELECT run_id, serve_bytes, deny_bytes, moment_id FROM deliveries WHERE delivery = ?",
      delivery,
    );
    if (!d) return;
    this.run("UPDATE deliveries SET stage = 'released' WHERE delivery = ?", delivery);
    if (d.run_id !== null) {
      this.run(
        "UPDATE budgets SET serve_used = serve_used - ?, deny_used = deny_used - ? WHERE run_id = ?",
        d.serve_bytes,
        d.deny_bytes,
        d.run_id,
      );
    }
    this.run("DELETE FROM claims WHERE delivery = ?", delivery);
    this.run("UPDATE carries SET state = 'waiting', delivery = NULL WHERE delivery = ?", delivery);
    this.run(
      `UPDATE candidates SET stage = 'released', outcome = 'delivery_unverified'
       WHERE coalesce(delivery, (SELECT delivery FROM moments WHERE id = moment_id)) = ? AND stage = 'reserved'`,
      delivery,
    );
  }

  /**
   * Settle a delivery after the hook wrote the reply. Returns false when the
   * delivery is unknown, has nothing reserved, or was already released; a settle
   * that arrives after the deadline releases the reservation instead.
   */
  settle(delivery: string, now: number): boolean {
    return this.tx(() => {
      const d = this.get<{ stage: string; moment_id: number; expires_at: number }>(
        "SELECT stage, moment_id, expires_at FROM deliveries WHERE delivery = ?",
        delivery,
      );
      if (!d || d.stage !== "reserved" || this.isPointerDelivery(delivery)) return false;
      if (d.expires_at < now) {
        this.release(delivery);
        return false;
      }
      this.run(
        "UPDATE deliveries SET stage = 'settled', settled_at = ? WHERE delivery = ?",
        now,
        delivery,
      );
      this.run("UPDATE claims SET settled = 1 WHERE delivery = ?", delivery);
      this.run("UPDATE carries SET state = 'printed' WHERE delivery = ?", delivery);
      this.run(
        `UPDATE candidates SET stage = 'settled', served_at = ?,
           outcome = CASE WHEN outcome = 'reserved_truncated' THEN 'truncated' ELSE 'injected' END
         WHERE coalesce(delivery, (SELECT delivery FROM moments WHERE id = moment_id)) = ? AND stage = 'reserved'`,
        now,
        delivery,
      );
      this.run(
        `UPDATE deliveries SET shown = 'unknown', shown_json = ?, shown_next = ?
         WHERE delivery = ? AND EXISTS (SELECT 1 FROM candidates c WHERE c.moment_id = ?
           AND c.stage = 'settled' AND c.seam IN ('context', 'carry_print'))`,
        '{"v":1,"source":null,"reason":"not_checked"}',
        now,
        delivery,
        d.moment_id,
      );
      // Without projection evidence the hook's settle is the proof the model was shown the
      // deny: it counts toward the cap, and an inline rule delivered whole was read. Where
      // the projection can show what the model saw, both wait for that evidence instead,
      // because another hook denying the same call can replace this reason (obedience test).
      const shown = this.all<{ rowid: number }>(
        "SELECT rowid FROM claims WHERE delivery = ? AND kind = 'deny' AND coalesce(evidence, 'none') = 'none'",
        delivery,
      );
      for (const c of shown) this.denyShown(c.rowid, now);
      this.run(
        `UPDATE candidates SET outcome = 'awaiting_evidence' WHERE id IN
           (SELECT candidate_id FROM claims WHERE delivery = ? AND kind = 'deny' AND evidence = 'projection')`,
        delivery,
      );
      return true;
    });
  }

  dueShown(now: number, limit: number): ShownCheck[] {
    return this.all<ShownCheck>(
      `${SHOWN_SELECT} WHERE d.shown = 'unknown' AND d.shown_next IS NOT NULL
        AND d.shown_next <= ? ORDER BY d.shown_next, d.created_at, d.rowid LIMIT ?`,
      now,
      limit,
    );
  }

  /** Every 200 reply, including released and reserved rivals outside the current batch. */
  shownRivals(harness: string, session: string): ShownCheck[] {
    return this.all<ShownCheck>(
      `${SHOWN_SELECT} WHERE d.status = 200 AND d.harness = ? AND m.session = ?`,
      harness,
      session,
    );
  }

  recordShown(delivery: string, fact: "shown" | "unknown", json: string, next: number | null) {
    return (
      Number(
        this.run(
          `UPDATE deliveries SET shown = ?, shown_json = ?, shown_next = ?
       WHERE delivery = ? AND shown = 'unknown'`,
          fact,
          json,
          fact === "shown" ? null : next,
          delivery,
        ).changes,
      ) === 1
    );
  }

  deferShown(delivery: string, next: number) {
    this.run(
      "UPDATE deliveries SET shown_next = ? WHERE delivery = ? AND shown = 'unknown'",
      next,
      delivery,
    );
  }

  /** Independent OpenCode pointer receipts cannot be moved by generic context closures. */
  isPointerDelivery(delivery: string): boolean {
    return (
      this.get(
        "SELECT 1 FROM candidates WHERE delivery = ? AND matcher = 'great_library'",
        delivery,
      ) !== undefined
    );
  }

  markPointerApplying(delivery: string, now: number): boolean {
    return this.tx(() => {
      if (!this.isPointerDelivery(delivery)) return false;
      const d = this.get<{ stage: string; expires_at: number }>(
        "SELECT stage, expires_at FROM deliveries WHERE delivery = ?",
        delivery,
      );
      if (d?.stage === "applying") return true;
      if (d?.stage !== "reserved" || d.expires_at < now) return false;
      this.run("UPDATE deliveries SET stage = 'applying' WHERE delivery = ?", delivery);
      this.run(
        "UPDATE candidates SET stage = 'applying', outcome = 'applying' WHERE delivery = ?",
        delivery,
      );
      return true;
    });
  }

  settlePointer(delivery: string, now: number): boolean {
    return this.tx(() => {
      if (!this.isPointerDelivery(delivery)) return false;
      const d = this.get<{ stage: string; expires_at: number }>(
        "SELECT stage, expires_at FROM deliveries WHERE delivery = ?",
        delivery,
      );
      if (d?.stage === "settled") return true;
      if (d?.stage !== "applying" && !(d?.stage === "reserved" && d.expires_at >= now))
        return false;
      this.run(
        "UPDATE deliveries SET stage = 'settled', settled_at = ? WHERE delivery = ?",
        now,
        delivery,
      );
      this.run("UPDATE claims SET settled = 1 WHERE delivery = ?", delivery);
      this.run(
        "UPDATE candidates SET stage = 'settled', outcome = 'injected', served_at = ? WHERE delivery = ?",
        now,
        delivery,
      );
      return true;
    });
  }

  releasePointer(delivery: string, reason: string): boolean {
    return this.tx(() => {
      if (!this.isPointerDelivery(delivery)) return false;
      const d = this.get<{ stage: string }>(
        "SELECT stage FROM deliveries WHERE delivery = ?",
        delivery,
      );
      if (d?.stage === "released") return true;
      if (d?.stage !== "reserved" && d?.stage !== "applying") return false;
      this.release(delivery);
      this.run(
        "UPDATE candidates SET stage = 'released', outcome = 'released', reason = ? WHERE delivery = ?",
        reason,
        delivery,
      );
      return true;
    });
  }

  greatLibraryOwed(harness: string, session: string, epoch: number): boolean {
    return (
      this.get(
        `SELECT 1 FROM claims c JOIN candidates k ON k.id = c.candidate_id
      WHERE c.harness = ? AND c.session = ? AND c.epoch = ? AND k.matcher = 'great_library'`,
        harness,
        session,
        epoch,
      ) === undefined
    );
  }

  /** Reserve before opening a Grok session. Its native id does not exist yet, so no claim is written. */
  stageSessionInstructions(input: {
    readonly threadId: string;
    readonly runId: string;
    readonly delivery: string;
    readonly current: SessionInstructions | null;
    readonly now: number;
    readonly missing?: string;
  }): string | null {
    return this.tx(() => {
      const { current, now } = input;
      if (current === null && input.missing === undefined) return null;
      const moment = this.insertMoment({
        harness: "grok",
        kind: "session_start",
        threadId: input.threadId,
        runId: input.runId,
        delivery: input.delivery,
        status: "ok",
        now,
        startedAt: now,
        ...(current === null ? {} : { owners: current.owners }),
      });
      if (current === null) {
        this.insertCandidate(moment, {
          matcher: "great_library",
          entry: input.missing ?? null,
          outcome: "not_on_machine",
        });
        return null;
      }
      if (current.bytes > this.budgetLeft(input.runId).serve) {
        this.insertCandidate(moment, {
          matcher: "great_library",
          entry: current.entry,
          version: current.version,
          seam: "context",
          outcome: "cut_budget",
          reason: "turn",
        });
        return null;
      }
      this.insertCandidate(moment, {
        matcher: "great_library",
        entry: current.entry,
        version: current.version,
        seam: "context",
        stage: "reserved",
        outcome: "reserved",
        bytes: current.bytes,
      });
      this.charge(input.runId, current.bytes, 0);
      this.run(
        `INSERT INTO deliveries (delivery, harness, moment_id, stage, status, body, run_id, serve_bytes, created_at, expires_at)
        VALUES (?, 'grok', ?, 'reserved', 200, ?, ?, ?, ?, ?)`,
        input.delivery,
        moment,
        current.text,
        input.runId,
        current.bytes,
        now,
        now + 120,
      );
      return input.delivery;
    });
  }

  recordInstallation(
    input: SessionInstructions & {
      readonly session: string;
      readonly threadId: string;
      readonly site: string;
      readonly now: number;
    },
  ) {
    this.tx(() => {
      const existing = this.get<InstallationRow>(
        "SELECT * FROM installations WHERE harness = 'grok' AND session = ?",
        input.session,
      );
      if (existing !== undefined) {
        if (existing.text !== input.text)
          throw new Error("Conflicting immutable Grok installation receipt");
        return;
      }
      this.run(
        `INSERT INTO installations (harness, session, thread_id, site, text, entry, version, bytes, owners_json, created_at)
        VALUES ('grok', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        input.session,
        input.threadId,
        input.site,
        input.text,
        input.entry,
        input.version,
        input.bytes,
        JSON.stringify(input.owners),
        input.now,
      );
    });
  }

  /** Charge the actual immutable head once, before a run can persist its running state or prompt. */
  attachSessionInstructions(input: {
    readonly session: string;
    readonly threadId: string;
    readonly runId: string;
    readonly reservation: string | null;
    readonly delivery: string;
    readonly current: SessionInstructions | null;
    readonly now: number;
  }) {
    this.tx(() => {
      const { session, runId, now, current, reservation } = input;
      const inst = this.get<InstallationRow>(
        "SELECT * FROM installations WHERE harness = 'grok' AND session = ?",
        session,
      );
      const stage =
        reservation === null
          ? undefined
          : this.get<{ stage: string; moment_id: number; expires_at: number }>(
              "SELECT stage, moment_id, expires_at FROM deliveries WHERE delivery = ? AND harness = 'grok' AND run_id = ?",
              reservation,
              runId,
            );
      const live = stage?.stage === "reserved" && stage.expires_at >= now;
      const release = () => {
        if (stage?.stage === "reserved" && reservation !== null) this.release(reservation);
      };
      if (inst?.consumed_run != null) {
        release();
        return;
      }
      const epoch =
        this.get<{ epoch: number }>(
          "SELECT epoch FROM sessions WHERE harness = 'grok' AND session = ?",
          session,
        )?.epoch ?? 0;
      if (inst === undefined) {
        release();
        this.run(
          "INSERT OR IGNORE INTO sessions (harness, session, epoch) VALUES ('grok', ?, ?)",
          session,
          epoch,
        );
        if (
          current === null ||
          this.get(
            `SELECT 1 FROM candidates c JOIN moments m ON m.id = c.moment_id
          WHERE m.harness = 'grok' AND m.session = ? AND m.epoch = ? AND c.matcher = 'great_library' AND c.outcome = 'not_installed'`,
            session,
            epoch,
          )
        )
          return;
        const moment = this.insertMoment({
          harness: "grok",
          session,
          epoch,
          kind: "session_start",
          threadId: input.threadId,
          runId,
          delivery: input.delivery,
          status: "ok",
          now,
          startedAt: now,
          owners: current.owners,
        });
        this.insertCandidate(moment, {
          matcher: "great_library",
          entry: current.entry,
          version: current.version,
          outcome: "not_installed",
          bytes: 0,
        });
        return;
      }
      const same = current?.version === inst.version;
      const confirm = same && live;
      if (!confirm) release();
      if (!confirm && inst.bytes > this.budgetLeft(runId).serve)
        throw new Error("Grok installation exceeds the attaching run's serve budget");
      const delivery = confirm ? reservation! : input.delivery;
      const moment =
        stage?.moment_id ??
        this.insertMoment({
          harness: "grok",
          session,
          epoch,
          kind: "session_start",
          threadId: input.threadId,
          runId,
          delivery,
          status: "ok",
          now,
          startedAt: now,
        });
      this.run(
        "UPDATE moments SET session = ?, epoch = ?, owners_json = ? WHERE id = ?",
        session,
        epoch,
        inst.owners_json,
        moment,
      );
      let candidate: number;
      if (confirm) {
        candidate = this.get<{ id: number }>(
          "SELECT id FROM candidates WHERE moment_id = ? AND matcher = 'great_library' AND stage = 'reserved'",
          moment,
        )!.id;
        this.run(
          "UPDATE candidates SET outcome = 'injected', stage = 'settled', served_at = ? WHERE id = ?",
          now,
          candidate,
        );
        this.run(
          "UPDATE deliveries SET stage = 'settled', settled_at = ? WHERE delivery = ?",
          now,
          delivery,
        );
      } else {
        candidate = this.insertCandidate(moment, {
          matcher: "great_library",
          entry: inst.entry,
          version: inst.version,
          delivery,
          seam: "context",
          stage: "settled",
          outcome: same ? "injected" : "installed_stale",
          bytes: inst.bytes,
          ...(same ? {} : { extra: { current_version: current?.version ?? null } }),
        });
        this.run("UPDATE candidates SET served_at = ? WHERE id = ?", now, candidate);
        this.charge(runId, inst.bytes, 0);
        this.run(
          `INSERT INTO deliveries (delivery, harness, moment_id, stage, status, run_id, serve_bytes, created_at, expires_at, settled_at)
          VALUES (?, 'grok', ?, 'settled', 204, ?, ?, ?, ?, ?)`,
          delivery,
          moment,
          runId,
          inst.bytes,
          now,
          now,
          now,
        );
      }
      if (same) {
        this.writeClaims(delivery, "grok", session, epoch, runId, [
          {
            kind: "serve",
            entry: inst.entry,
            version: inst.version,
            rule: null,
            whole: true,
            candidateId: candidate,
          },
        ]);
        this.run("UPDATE claims SET settled = 1 WHERE delivery = ?", delivery);
      }
      this.run(
        "INSERT OR IGNORE INTO sessions (harness, session, epoch) VALUES ('grok', ?, ?)",
        session,
        epoch,
      );
      this.run(
        "UPDATE installations SET consumed_run = ?, consumed_candidate = ? WHERE harness = 'grok' AND session = ?",
        runId,
        candidate,
        session,
      );
    });
  }

  private retainInstallation(
    sel: Selection,
    epoch: number,
    moment: number,
    runId: string,
    delivery: string,
    now: number,
  ) {
    const current = sel.greatLibrary;
    if (current == null || !this.greatLibraryOwed("grok", sel.moment.session, epoch)) return;
    const inst = this.get<InstallationRow>(
      "SELECT * FROM installations WHERE harness = 'grok' AND session = ?",
      sel.moment.session,
    );
    if (inst === undefined) {
      this.insertCandidate(moment, { ...this.base(current), outcome: "not_installed", bytes: 0 });
      return;
    }
    const same = current.version === inst.version;
    this.run("UPDATE moments SET owners_json = ? WHERE id = ?", inst.owners_json, moment);
    const candidate = this.insertCandidate(moment, {
      matcher: "great_library",
      entry: inst.entry,
      version: inst.version,
      seam: "context",
      stage: same ? "settled" : null,
      outcome: same ? "retained" : "retained_stale",
      bytes: 0,
      extra: {
        owners: inst.owners_json === null ? {} : JSON.parse(inst.owners_json),
        ...(same ? {} : { current_version: current.version }),
      },
    });
    if (same) {
      this.run("UPDATE candidates SET served_at = ? WHERE id = ?", now, candidate);
      this.writeClaims(delivery, "grok", sel.moment.session, epoch, runId, [
        {
          kind: "serve",
          entry: inst.entry,
          version: inst.version,
          rule: null,
          whole: true,
          candidateId: candidate,
        },
      ]);
      this.run("UPDATE claims SET settled = 1 WHERE candidate_id = ?", candidate);
    }
  }

  /** A native fork inherits only settled current evidence in its surviving prefix. */
  recordNativeFork(fork: NativeForkInput) {
    return this.tx(() => {
      if (
        this.get(
          "SELECT 1 FROM moments WHERE harness = ? AND session = ?",
          fork.harness,
          fork.target,
        )
      )
        return "skipped";
      const delivered = this.all<{
        delivery: string;
        version: string;
        bytes: number;
        outcome: string;
        received_at: number;
        run_id: string;
        moment_id: number;
        epoch: number;
      }>(
        `SELECT c.delivery, c.version, k.bytes, k.outcome, m.received_at, m.run_id, m.id AS moment_id, m.epoch FROM claims c
         JOIN candidates k ON k.id = c.candidate_id AND k.matcher = 'great_library'
         JOIN moments m ON m.id = k.moment_id
         WHERE c.harness = ? AND c.session = ? AND c.settled = 1 AND m.received_at <= ?`,
        fork.harness,
        fork.source,
        fork.boundaryAt,
      ).filter((row) => row.outcome === "inherited" || !fork.reverted.has(row.run_id));
      const compactions = fork.compactions.filter(
        (c) => c.startedAt <= fork.boundaryAt && (c.runId === null || !fork.reverted.has(c.runId)),
      );
      const compactedAt =
        compactions.length === 0 ? null : Math.max(...compactions.map((c) => c.startedAt));
      const epochMoments =
        fork.harness === "claude"
          ? this.all<{
              id: number;
              epoch: number;
              run_id: string | null;
              source: string;
            }>(
              `SELECT id, epoch, run_id, source FROM moments
          WHERE harness = ? AND session = ? AND kind = 'session_start' AND source IN ('compact', 'clear')
            AND epoch IS NOT NULL AND received_at <= ? ORDER BY id`,
              fork.harness,
              fork.source,
              fork.boundaryAt,
            ).filter((m) => m.run_id === null || !fork.reverted.has(m.run_id))
          : [];
      const sourceEpoch = epochMoments.at(-1)?.epoch;
      const latest = compactions.filter((c) => c.startedAt === compactedAt);
      const compactRun = latest[0]?.runId;
      let compactHookId: number | null = null;
      if (compactRun != null && latest.every((c) => c.runId === compactRun)) {
        const hooks = epochMoments.filter((m) => m.run_id === compactRun && m.source === "compact");
        // Claude emits SessionStart(compact) before compact_boundary reaches its SDK stream.
        // Pair the last hook with the last surviving compaction only when that run's counts agree.
        // A missing or failed last re-serve cannot borrow an older epoch's settled copy.
        if (hooks.length === compactions.filter((c) => c.runId === compactRun).length)
          compactHookId = hooks.at(-1)?.id ?? null;
      }
      const retained = delivered.filter(
        (row) =>
          (sourceEpoch === undefined || row.epoch === sourceEpoch) &&
          (compactedAt === null ||
            row.received_at > compactedAt ||
            row.moment_id === compactHookId),
      );
      const from = retained.find((row) => row.version === fork.line.version);
      const reason =
        from !== undefined
          ? null
          : retained.length > 0
            ? "stale_version"
            : delivered.length > 0
              ? "compacted"
              : "no_delivery";
      const moment = this.insertMoment({
        harness: fork.harness,
        session: fork.target,
        epoch: 0,
        kind: "native_fork",
        threadId: fork.threadId,
        runId: fork.runId,
        delivery: fork.delivery,
        status: "none",
        now: fork.now,
        startedAt: fork.now,
      });
      const candidate = this.insertCandidate(moment, {
        ...this.base(fork.line),
        seam: "context",
        outcome: from === undefined ? "not_inherited" : "inherited",
        stage: from === undefined ? null : "settled",
        reason,
        bytes: 0,
        extra: {
          source: fork.source,
          source_delivery: from?.delivery ?? null,
          source_bytes: from?.bytes ?? null,
          boundary_at: fork.boundaryAt,
          compacted_at: compactedAt,
          compact_hook_moment_id: from?.moment_id === compactHookId ? compactHookId : null,
        },
      });
      this.run(
        `INSERT INTO deliveries (delivery, harness, moment_id, stage, status, run_id, created_at, expires_at)
        VALUES (?, ?, ?, 'none', 204, ?, ?, ?)`,
        fork.delivery,
        fork.harness,
        moment,
        fork.runId,
        fork.now,
        fork.now,
      );
      this.run(
        "INSERT OR IGNORE INTO sessions (harness, session, epoch) VALUES (?, ?, 0)",
        fork.harness,
        fork.target,
      );
      if (from !== undefined) {
        this.writeClaims(fork.delivery, fork.harness, fork.target, 0, fork.runId, [
          {
            kind: "serve",
            entry: fork.line.entry,
            version: fork.line.version,
            rule: null,
            whole: true,
            candidateId: candidate,
          },
        ]);
        this.run("UPDATE claims SET settled = 1 WHERE candidate_id = ?", candidate);
        this.run("UPDATE candidates SET served_at = ? WHERE id = ?", fork.now, candidate);
      }
      return from === undefined ? reason! : "inherited";
    });
  }

  /**
   * Settled denies of this session whose delivery the projection can still prove or
   * disprove: the caller looks up each denied call's projected turn item.
   */
  pendingChecks(harness: string, session: string): DenyCheck[] {
    return this.all<{ claim: number; tool_use_id: string; probe: string }>(
      `SELECT rowid AS claim, tool_use_id, probe FROM claims
       WHERE harness = ? AND session = ? AND kind = 'deny' AND settled = 1 AND evidence = 'projection'
         AND checked IS NULL AND tool_use_id IS NOT NULL AND probe IS NOT NULL
       ORDER BY rowid DESC LIMIT 8`,
      harness,
      session,
    ).map((r) => ({ claim: r.claim, toolUseId: r.tool_use_id, probe: r.probe }));
  }

  /**
   * Records what the projection showed for settled denies: `shown` lifts an inline rule; `hidden`
   * (another hook's deny replaced it) keeps the rule armed without counting toward the cap or dedupe.
   * A deny with no projected item after `CHECK_GIVE_UP_SECONDS` stays armed, logged `unverified`.
   */
  applyChecks(
    results: ReadonlyArray<{
      readonly claim: number;
      /** Every string in the projected item's `output`, or null when there is no item yet. */
      readonly output: ReadonlyArray<string> | null;
    }>,
    now: number,
  ) {
    this.tx(() => {
      for (const r of results) {
        const c = this.get<{ probe: string; candidate_id: number; created_at: number }>(
          `SELECT c.probe, c.candidate_id, d.created_at FROM claims c
           JOIN deliveries d ON d.delivery = c.delivery WHERE c.rowid = ? AND c.checked IS NULL`,
          r.claim,
        );
        if (!c) continue; // already adjudicated by a concurrent moment
        if (r.output === null) continue; // no finished item yet; `expireChecks` ends the wait
        // The whole delivered rule must be in what the model saw; the call's input does not count.
        if (r.output.some((text) => text.includes(c.probe))) {
          if (this.adjudicate(r.claim, "shown")) {
            this.run(
              `UPDATE candidates SET outcome = CASE WHEN (SELECT whole FROM claims WHERE rowid = ?) = 1
                 THEN 'injected' ELSE 'truncated' END WHERE id = ?`,
              r.claim,
              c.candidate_id,
            );
            this.denyShown(r.claim, now);
          }
          continue;
        }
        // Grok shows a deny reason only up to a length, then `… [+N chars]`: the model
        // saw the rule cut, so the deny counts toward the cap but the rule is not read.
        if (r.output.some((text) => shownCut(text, c.probe))) {
          if (this.adjudicate(r.claim, "shown")) {
            this.run("UPDATE claims SET whole = 0 WHERE rowid = ?", r.claim);
            this.run("UPDATE candidates SET outcome = 'truncated' WHERE id = ?", c.candidate_id);
            this.denyShown(r.claim, now);
          }
          continue;
        }
        if (this.adjudicate(r.claim, "hidden")) {
          this.run("UPDATE claims SET settled = 0 WHERE rowid = ?", r.claim);
          this.run("UPDATE candidates SET outcome = 'hidden' WHERE id = ?", c.candidate_id);
        }
      }
    });
  }

  /** Claim the one verdict a deny gets; only the winning transition may change counts or lifts. */
  private adjudicate(claim: number, verdict: "shown" | "hidden" | "unverified") {
    return (
      Number(
        this.run(
          "UPDATE claims SET checked = ? WHERE rowid = ? AND checked IS NULL",
          verdict,
          claim,
        ).changes,
      ) === 1
    );
  }

  /**
   * A deny the model was shown counts toward the cap, and an inline rule shown whole is
   * read. An unproven deny also counts (`lift: false`), so an agent is never denied forever.
   */
  private denyShown(claim: number, now: number, options: { lift: boolean } = { lift: true }) {
    const c = this.get<{
      harness: string;
      session: string;
      epoch: number;
      entry: string;
      version: string;
      rule: string | null;
      whole: number;
    }>(
      "SELECT harness, session, epoch, entry, version, rule, whole FROM claims WHERE rowid = ?",
      claim,
    );
    if (!c) return;
    const key = [c.harness, c.session, c.epoch, c.entry, c.version] as const;
    this.run(
      `UPDATE deny_state SET denies = denies + 1
       WHERE harness = ? AND session = ? AND epoch = ? AND entry = ? AND version = ?`,
      ...key,
    );
    if (options.lift && c.rule === "inline" && c.whole === 1) {
      this.run(
        `UPDATE deny_state SET lifted = 1, lift_outcome = 'delivered_inline', lifted_at = ?
         WHERE harness = ? AND session = ? AND epoch = ? AND entry = ? AND version = ? AND lifted = 0`,
        now,
        ...key,
      );
    }
  }

  /** Decide and reserve the reply to one selected moment. */
  decide(input: MomentInput): Reply {
    return this.tx(() => this.decideTx(input));
  }

  private insertMoment(m: {
    harness: string | null;
    status: string;
    delivery: string | null;
    reason?: string | null;
    now: number;
    startedAt: number;
    kind?: string;
    source?: string | null;
    session?: string;
    threadId?: string | null;
    runId?: string | null;
    toolUseId?: string | null;
    epoch?: number | null;
    holdout?: boolean | null;
    owners?: Record<string, string | null>;
    cue?: CueReply | null;
  }) {
    const cue = m.cue ?? null;
    const result = this.run(
      `INSERT INTO moments (received_at, harness, kind, session, thread_id, run_id, tool_use_id, epoch, holdout,
         status, reason, owners_json, cue_adapter, server_ms, delivery, cue_status, cue_ms, cue_wait_ms, cue_json, source)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      m.now,
      m.harness,
      m.kind ?? null,
      m.session ?? null,
      m.threadId ?? null,
      m.runId ?? null,
      m.toolUseId ?? null,
      m.epoch ?? null,
      m.holdout === undefined || m.holdout === null ? null : m.holdout ? 1 : 0,
      m.status,
      m.reason ?? null,
      m.owners ? JSON.stringify(m.owners) : null,
      cue?.adapter ?? null,
      (m.now - m.startedAt) * 1000,
      m.delivery,
      cue?.status ?? null,
      cue?.ms ?? null,
      cue?.waitMs ?? null,
      cue?.detail ? JSON.stringify(cue.detail) : null,
      m.source ?? null,
    );
    return Number(result.lastInsertRowid);
  }

  private insertCandidate(momentId: number, c: CandidateRow) {
    const result = this.run(
      `INSERT INTO candidates (moment_id, matcher, entry, version, key, seam, stage, outcome, reason, score, bytes, extra_json, delivery)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      momentId,
      c.matcher,
      c.entry ?? null,
      c.version ?? null,
      c.key ?? null,
      c.seam ?? null,
      c.stage ?? null,
      c.outcome,
      c.reason ?? null,
      c.score ?? null,
      c.bytes ?? null,
      c.extra ? JSON.stringify(c.extra) : null,
      c.delivery ?? null,
    );
    return Number(result.lastInsertRowid);
  }

  epochAfter(harness: string, sel: Selection): number {
    const row = this.get<{ epoch: number }>(
      "SELECT epoch FROM sessions WHERE harness = ? AND session = ?",
      harness,
      sel.moment.session,
    );
    const bump =
      row !== undefined &&
      ((sel.profile.new_epoch_sources.length === 0 && sel.moment.kind === "compaction") ||
        (sel.moment.kind === "session_start" &&
          sel.moment.source !== null &&
          sel.profile.new_epoch_sources.includes(sel.moment.source)));
    const epoch = row === undefined ? 0 : row.epoch + (bump ? 1 : 0);
    return epoch;
  }

  private epochFor(harness: string, sel: Selection): number {
    const epoch = this.epochAfter(harness, sel);
    this.run(
      "INSERT INTO sessions (harness, session, epoch) VALUES (?, ?, ?) ON CONFLICT (harness, session) DO UPDATE SET epoch = excluded.epoch",
      harness,
      sel.moment.session,
      epoch,
    );
    return epoch;
  }

  private budgetLeft(runId: string) {
    const row = this.get<{ serve_used: number; deny_used: number }>(
      "SELECT serve_used, deny_used FROM budgets WHERE run_id = ?",
      runId,
    );
    return {
      serve: SERVE_BUDGET_BYTES - (row?.serve_used ?? 0),
      deny: DENY_BUDGET_BYTES - (row?.deny_used ?? 0),
    };
  }

  private settledIn(runId: string, epoch: number, entry: string, version: string) {
    return (
      this.get(
        "SELECT 1 FROM claims WHERE run_id = ? AND epoch = ? AND entry = ? AND version = ? AND settled = 1",
        runId,
        epoch,
        entry,
        version,
      ) !== undefined
    );
  }

  private decideTx(input: MomentInput): Reply {
    // A delivery id decided while this moment was selecting gets that same reply.
    const replay = this.replayed(input.delivery, input.pointerDelivery);
    if (replay !== undefined) return replay;
    const { harness, now } = input;
    let sel = input.selection;
    const m = sel.moment;
    const epoch = this.epochFor(harness, sel);
    const holdout = isHoldout(harness, m.session);
    const joined = input.join.type === "run" ? input.join : null;
    const status = input.join.type === "run" ? "ok" : input.join.type;
    const momentId = this.insertMoment({
      harness,
      status,
      delivery: input.delivery,
      now,
      startedAt: input.startedAt,
      kind: m.kind,
      source: m.source,
      session: m.session,
      threadId: joined?.threadId ?? null,
      runId: joined?.runId ?? null,
      toolUseId: m.tool_use_id,
      epoch,
      holdout,
      owners: sel.owners,
      cue: input.cue?.reply ?? null,
    });
    for (const note of sel.notes) {
      this.insertCandidate(momentId, {
        matcher: note.matcher,
        entry: note.entry ?? null,
        outcome: note.outcome,
        reason: note.reason ?? null,
      });
    }
    const finish = (reply: Reply, reserved?: { runId: string; serve: number; deny: number }) => {
      this.run(
        `INSERT INTO deliveries (delivery, harness, moment_id, stage, status, body, run_id, serve_bytes, deny_bytes,
           created_at, expires_at, transcript_path, native_turn)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        input.delivery,
        harness,
        momentId,
        reply.status === 200 ? "reserved" : "none",
        reply.status,
        reply.status === 200 ? reply.body : null,
        reserved?.runId ?? null,
        reserved?.serve ?? 0,
        reserved?.deny ?? 0,
        now,
        now + sel.profile.settle_within_ms / 1000,
        reply.status === 200 ? nativeField(m.transcript_path) : null,
        reply.status === 200 ? nativeField(m.native_turn) : null,
      );
      return this.replayed(input.delivery, input.pointerDelivery)!;
    };
    if (joined === null) {
      for (const c of sel.candidates) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: status });
      }
      return finish({ status: 204 });
    }
    const fires = this.cueFires(input.cue ?? null, holdout, momentId);
    if (fires.length > 0) sel = { ...sel, candidates: [...sel.candidates, ...fires] };
    this.expireCarries(harness, m.session, joined.runId);
    if (input.deliveryMode === "retained_startup") {
      if (
        sel.profile.delivering.includes(m.kind) ||
        (m.kind === "user_message" && sel.profile.carry_to !== null)
      ) {
        this.carry(
          harness,
          {
            ...sel,
            candidates: [
              ...(sel.greatLibrary == null ? [] : [sel.greatLibrary]),
              ...(m.kind === "user_message"
                ? this.dropAfterAction(sel, momentId).candidates
                : sel.candidates),
            ],
          },
          epoch,
          momentId,
          joined.runId,
          now,
        );
      } else {
        for (const c of sel.candidates)
          this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
      }
      // This hook already replied. Account for its join without reserving an undeliverable reply.
      return finish({ status: 204 });
    }
    if (harness === "grok" && m.kind === "compaction")
      this.retainInstallation(sel, epoch, momentId, joined.runId, input.delivery, now);
    if (
      sel.profile.templates.pointer !== undefined &&
      (m.kind === "user_message" || m.kind === "compaction") &&
      sel.greatLibrary !== null &&
      sel.greatLibrary !== undefined &&
      this.greatLibraryOwed(harness, m.session, epoch)
    ) {
      if (input.pointerDelivery === undefined) {
        this.insertCandidate(momentId, { matcher: "great_library", outcome: "client_unsupported" });
      } else {
        this.reservePointer(sel, epoch, momentId, joined.runId, input.pointerDelivery, now);
      }
    }
    this.applyReadChecks(harness, m.session, input.readChecks ?? [], momentId, now);
    if ((m.kind === "tool_result" || m.kind === "tool_failure") && m.failed !== true) {
      this.recordReads(harness, sel, epoch, momentId, now);
    }
    const delivering = sel.profile.delivering.includes(m.kind);
    switch (m.kind) {
      case "session_start":
        return finish(
          ...this.sessionStart(sel, epoch, momentId, joined.runId, input.delivery, delivering),
        );
      case "pre_tool":
        return finish(
          ...this.preTool(harness, sel, epoch, momentId, joined.runId, input.delivery, now),
        );
      case "user_message":
      case "tool_result":
      case "tool_failure": {
        const after = this.dropAfterAction(sel, momentId);
        if (
          after.candidates.length === 0 &&
          this.eligiblePointer(after, epoch) === null &&
          !this.hasCarries(harness, m.session, joined.runId)
        )
          return finish({ status: 204 });
        if (!delivering && after.profile.carry_to !== null) {
          this.carry(harness, after, epoch, momentId, joined.runId, now);
          return finish({ status: 204 });
        }
        if (!delivering) {
          for (const c of after.candidates) {
            this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
          }
          return finish({ status: 204 });
        }
        return finish(...this.serveContext(after, epoch, momentId, joined.runId, input.delivery));
      }
      default:
        if (delivering && sel.profile.carry_to === null)
          return finish(
            ...this.printCarries(harness, sel, epoch, momentId, joined.runId, input.delivery),
          );
        return finish({ status: 204 });
    }
  }

  /**
   * A must-know rule matched by a command or path key governs that very call, which has
   * now run: serving it as context would remind the agent after the action. Rules
   * keyed on an error only exist after the call and stay.
   */
  private dropAfterAction(sel: Selection, momentId: number): Selection {
    const late = (c: Candidate) =>
      c.must_know && c.key !== null && /^(?:command|path):/u.test(c.key);
    for (const c of sel.candidates.filter(late)) {
      this.insertCandidate(momentId, {
        ...this.base(c),
        outcome: "suppressed",
        reason: "after_action",
      });
    }
    return { ...sel, candidates: sel.candidates.filter((c) => !late(c)) };
  }

  private base(c: Candidate) {
    return {
      matcher: c.matcher,
      entry: c.entry,
      version: c.version,
      key: c.key,
      score: c.score ?? null,
    };
  }

  /**
   * Log every entry Cue scored and return the ones to serve: at or above the adapter's threshold, outside
   * the holdout. A holdout session's would-fires are logged `holdout` and go no further.
   */
  private cueFires(cue: CueMoment | null, holdout: boolean, momentId: number): Candidate[] {
    const reply = cue?.reply;
    if (cue === null || reply === undefined || reply.status !== "ok") return [];
    const threshold = reply.threshold ?? Number.POSITIVE_INFINITY;
    const fires: Candidate[] = [];
    for (const c of cue.candidates) {
      const score = reply.scores?.[c.entry];
      if (score === undefined) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: "not_scored" });
        continue;
      }
      const scored = { ...c, score };
      if (score < threshold) {
        this.insertCandidate(momentId, { ...this.base(scored), outcome: "below_threshold" });
      } else if (holdout) {
        this.insertCandidate(momentId, { ...this.base(scored), outcome: "holdout" });
      } else {
        fires.push(scored);
      }
    }
    return fires;
  }

  /**
   * Record the files a successful call read. Where the harness reports no failure (Codex),
   * a read waits for proof: it stays pending until the projected command item shows the
   * call finished with exit code 0 (`pendingReads`, `applyReadChecks`).
   */
  private recordReads(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    now: number,
  ) {
    // Without a failure signal a read waits for the projected exit code, which needs the
    // call's tool-use id; a read that can never be checked does not count at all.
    if (sel.moment.failed === null && sel.moment.tool_use_id === null) return;
    const pending = sel.moment.failed === null ? sel.moment.tool_use_id : null;
    for (const read of sel.moment.reads) {
      this.run(
        `INSERT INTO reads (harness, session, epoch, path, kind, moment_id, read_at, pending_tool_use_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        harness,
        sel.moment.session,
        epoch,
        read.path,
        read.kind,
        momentId,
        now,
        pending,
      );
      if (pending === null)
        this.liftOnRead(harness, sel.moment.session, epoch, read, momentId, now);
    }
  }

  /** Lift every pointer rule waiting on this file in this session and epoch, on a full read. */
  private liftOnRead(
    harness: string,
    session: string,
    epoch: number,
    read: { readonly path: string; readonly kind: string },
    momentId: number,
    now: number,
    readAt = now,
  ) {
    if (!FULL_READS.has(read.kind)) return;
    const waiting = this.all<{
      entry: string;
      version: string;
      denies: number;
      first_denied_at: number | null;
    }>(
      `SELECT entry, version, denies, first_denied_at FROM deny_state
       WHERE harness = ? AND session = ? AND epoch = ? AND target = ? AND lifted = 0
       AND (rule_kind != 'entry' OR first_denied_at < ?)`,
      harness,
      session,
      epoch,
      read.path,
      readAt,
    );
    for (const w of waiting) {
      this.run(
        `UPDATE deny_state SET lifted = 1, lift_outcome = 'read', lifted_at = ?
         WHERE harness = ? AND session = ? AND epoch = ? AND entry = ? AND version = ?`,
        now,
        harness,
        session,
        epoch,
        w.entry,
        w.version,
      );
      const calls = this.get<{ n: number }>(
        `SELECT count(*) AS n FROM moments WHERE harness = ? AND session = ? AND kind = 'pre_tool' AND received_at > ?`,
        harness,
        session,
        w.first_denied_at ?? now,
      );
      this.insertCandidate(momentId, {
        matcher: "exact_key",
        entry: w.entry,
        version: w.version,
        outcome: "lifted",
        reason: "read",
        extra: { denies: w.denies, calls: calls?.n ?? 0, read_kind: read.kind },
      });
    }
  }

  /** Calls whose reads wait for the projected exit status, newest first. */
  pendingReads(harness: string, session: string): string[] {
    return this.all<{ id: string }>(
      `SELECT DISTINCT pending_tool_use_id AS id FROM reads
       WHERE harness = ? AND session = ? AND pending_tool_use_id IS NOT NULL
       ORDER BY rowid DESC LIMIT 8`,
      harness,
      session,
    ).map((r) => r.id);
  }

  /**
   * Settle pending reads from the projected command items: a call that finished with exit
   * code 0 proves its reads (and lifts their rules); a call that failed discards them.
   * `ok: null` (not finished, or no item) keeps them waiting for up to `CHECK_GIVE_UP_SECONDS`.
   */
  private applyReadChecks(
    harness: string,
    session: string,
    results: ReadonlyArray<ReadCheck>,
    momentId: number,
    now: number,
  ) {
    {
      for (const r of results) {
        if (r.ok === false) {
          this.run(
            "DELETE FROM reads WHERE harness = ? AND session = ? AND pending_tool_use_id = ?",
            harness,
            session,
            r.toolUseId,
          );
          continue;
        }
        if (r.ok === null) continue;
        const proven = this.all<{ path: string; kind: string; epoch: number; read_at: number }>(
          `SELECT path, kind, epoch, read_at FROM reads
           WHERE harness = ? AND session = ? AND pending_tool_use_id = ?`,
          harness,
          session,
          r.toolUseId,
        );
        this.run(
          "UPDATE reads SET pending_tool_use_id = NULL WHERE harness = ? AND session = ? AND pending_tool_use_id = ?",
          harness,
          session,
          r.toolUseId,
        );
        for (const read of proven)
          this.liftOnRead(harness, session, read.epoch, read, momentId, now, read.read_at);
      }
      this.run(
        "DELETE FROM reads WHERE pending_tool_use_id IS NOT NULL AND read_at < ?",
        now - CHECK_GIVE_UP_SECONDS,
      );
    }
  }

  private wasRead(harness: string, session: string, epoch: number, target: string) {
    return (
      this.get(
        `SELECT 1 FROM reads WHERE harness = ? AND session = ? AND epoch = ? AND path = ?
         AND pending_tool_use_id IS NULL AND kind IN (${[...FULL_READS].map((k) => `'${k}'`).join(", ")})`,
        harness,
        session,
        epoch,
        target,
      ) !== undefined
    );
  }

  private sessionStart(
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
    delivering: boolean,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    const pointer = this.eligiblePointer(sel, epoch);
    if (!delivering && pointer === null) {
      for (const c of sel.candidates)
        this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
      return [{ status: 204 }];
    }
    const limit = delivering ? sel.profile.session_start_limit_bytes : sel.profile.limit_bytes;
    const carryOutput = new Composer("", limit, Math.max(this.budgetLeft(runId).serve, 0));
    let charged = 0;
    const carried: Claim[] = [];
    this.appendCarries(
      sel.profile.harness,
      sel,
      epoch,
      momentId,
      runId,
      carryOutput,
      carried,
      delivering ? sel.candidates.filter((c) => c.matcher === "session_start") : [],
    );
    const carriedBytes = carryOutput.budgetBytes();
    if (carriedBytes > 0) {
      this.charge(runId, carriedBytes, 0);
      charged += carriedBytes;
    }
    this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, carried);
    // Files share the output limit; only ordinary carries spend the serve budget.
    const out = new Composer("", limit, Number.POSITIVE_INFINITY);
    for (const part of carryOutput.parts) out.add(part.text, part.claim, part.startup);
    if (
      pointer !== null &&
      !carried.some((claim) => claim.entry === pointer.entry && claim.version === pointer.version)
    ) {
      const text = this.pointerText(sel, pointer);
      const bytes = utf8(text);
      if (bytes > this.budgetLeft(runId).serve) {
        this.insertCandidate(momentId, {
          ...this.base(pointer),
          seam: "context",
          outcome: "cut_budget",
          reason: "turn",
        });
      } else if (!out.fits(text)) {
        this.insertCandidate(momentId, {
          ...this.base(pointer),
          seam: "context",
          outcome: "cut_budget",
          reason: "moment",
        });
      } else {
        const claim = this.pointerClaim(pointer, momentId, bytes);
        out.add(text, claim);
        this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, [claim]);
        this.charge(runId, bytes, 0);
        charged += bytes;
      }
    }
    for (const c of sel.candidates) {
      if (carried.some((claim) => claim.entry === c.entry && claim.version === c.version)) continue;
      if (!delivering) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
        continue;
      }
      if (!out.fits(c.text)) {
        this.insertCandidate(momentId, {
          ...this.base(c),
          outcome: "cut_budget",
          reason: "moment",
        });
        continue;
      }
      const id = this.insertCandidate(momentId, {
        ...this.base(c),
        seam: "context",
        stage: "reserved",
        outcome: "reserved",
        bytes: utf8(c.text),
      });
      const carry = this.get<{ id: number }>(
        "SELECT id FROM carries WHERE harness = ? AND session = ? AND run_id = ? AND entry = ? AND version = ? AND state = 'waiting'",
        sel.profile.harness,
        sel.moment.session,
        runId,
        c.entry,
        c.version,
      );
      const claim: Claim = {
        kind: "start",
        entry: c.entry,
        version: c.version,
        rule: null,
        whole: true,
        candidateId: id,
        ...(carry === undefined ? {} : { carryId: carry.id }),
      };
      out.add(c.text, claim);
      // Use the fresh file's exemption and the existing carry reservation/release lifecycle.
      if (carry !== undefined)
        this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, [claim]);
    }
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    return [this.render(sel, "context", text), { runId, serve: charged, deny: 0 }];
  }

  private render(sel: Selection, template: "context" | "deny" | "pointer", text: string): Reply {
    const body = renderTemplate(sel.profile.templates[template], {
      text,
      event: sel.moment.event ?? "",
    });
    return { status: 200, body: JSON.stringify(body) };
  }

  private pointerText(sel: Selection, pointer: Candidate) {
    return `${sel.profile.heads.context}\n\n${pointer.text}`;
  }

  private pointerClaim(c: Candidate, momentId: number, bytes: number, delivery?: string): Claim {
    const candidateId = this.insertCandidate(momentId, {
      ...this.base(c),
      seam: "context",
      stage: "reserved",
      outcome: "reserved",
      bytes,
      ...(delivery === undefined ? {} : { delivery }),
    });
    return {
      kind: "serve",
      entry: c.entry,
      version: c.version,
      rule: null,
      whole: true,
      candidateId,
    };
  }

  private eligiblePointer(sel: Selection, epoch: number): Candidate | null {
    const m = sel.moment;
    const eligible =
      (m.kind === "user_message" && sel.profile.delivering.includes(m.kind)) ||
      (m.kind === "session_start" &&
        m.source !== null &&
        sel.profile.new_epoch_sources.includes(m.source));
    return eligible &&
      sel.profile.templates.pointer === undefined &&
      sel.greatLibrary != null &&
      this.greatLibraryOwed(sel.profile.harness, m.session, epoch)
      ? sel.greatLibrary
      : null;
  }

  private reservePointer(
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
    now: number,
  ) {
    const c = sel.greatLibrary!;
    const text = this.pointerText(sel, c);
    const bytes = utf8(text);
    if (bytes > this.budgetLeft(runId).serve || bytes > sel.profile.limit_bytes) {
      this.insertCandidate(momentId, {
        ...this.base(c),
        seam: "context",
        outcome: "cut_budget",
        reason: bytes > sel.profile.limit_bytes ? "moment" : "turn",
      });
      return;
    }
    const claim = this.pointerClaim(c, momentId, bytes, delivery);
    this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, [claim]);
    this.charge(runId, bytes, 0);
    const reply = this.render(sel, "pointer", text);
    this.run(
      `INSERT INTO deliveries (delivery, harness, moment_id, stage, status, body, run_id, serve_bytes, created_at, expires_at)
      VALUES (?, ?, ?, 'reserved', 200, ?, ?, ?, ?, ?)`,
      delivery,
      sel.profile.harness,
      momentId,
      reply.status === 200 ? reply.body : null,
      runId,
      bytes,
      now,
      now + sel.profile.settle_within_ms / 1000,
    );
  }

  private writeClaims(
    delivery: string,
    harness: string,
    session: string,
    epoch: number,
    runId: string,
    claims: Claim[],
    evidence: { readonly kind: string; readonly toolUseId: string | null } = {
      kind: "none",
      toolUseId: null,
    },
  ) {
    for (const c of claims) {
      this.run(
        `INSERT INTO claims (delivery, kind, harness, session, epoch, run_id, entry, version, rule, whole, candidate_id,
           evidence, tool_use_id, probe)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        delivery,
        c.kind,
        harness,
        session,
        epoch,
        runId,
        c.entry,
        c.version,
        c.rule,
        c.whole ? 1 : 0,
        c.candidateId,
        c.kind === "deny" ? evidence.kind : "none",
        evidence.toolUseId,
        c.probe ?? null,
      );
      if (c.carryId !== undefined) {
        this.run(
          "UPDATE carries SET state = 'reserved', delivery = ? WHERE id = ?",
          delivery,
          c.carryId,
        );
      }
    }
  }

  private charge(runId: string, serve: number, deny: number) {
    this.run(
      `INSERT INTO budgets (run_id, serve_used, deny_used) VALUES (?, ?, ?)
       ON CONFLICT (run_id) DO UPDATE SET serve_used = serve_used + excluded.serve_used,
         deny_used = deny_used + excluded.deny_used`,
      runId,
      serve,
      deny,
    );
  }

  private preTool(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
    now: number,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    const session = sel.moment.session;
    const denyQueue: Candidate[] = [];
    for (const c of sel.candidates) {
      if (!c.must_know) {
        this.insertCandidate(momentId, {
          ...this.base(c),
          outcome: "selected",
          seam: "tool_result",
        });
        continue;
      }
      const state = this.get<{ denies: number; lifted: number; lift_outcome: string | null }>(
        "SELECT denies, lifted, lift_outcome FROM deny_state WHERE harness = ? AND session = ? AND epoch = ? AND entry = ? AND version = ?",
        harness,
        session,
        epoch,
        c.entry,
        c.version,
      );
      if (state?.lifted === 1) {
        this.insertCandidate(momentId, {
          ...this.base(c),
          outcome: "passed",
          reason: state.lift_outcome,
        });
        continue;
      }
      if (
        c.rule === "pointer" &&
        c.target !== null &&
        this.wasRead(harness, session, epoch, c.target)
      ) {
        this.liftWithout(harness, session, epoch, c, "read", now);
        this.insertCandidate(momentId, {
          ...this.base(c),
          outcome: "lifted",
          reason: "read",
          extra: { denies: state?.denies ?? 0 },
        });
        continue;
      }
      if ((state?.denies ?? 0) >= DENY_CAP) {
        this.liftWithout(harness, session, epoch, c, "not_read_capped", now);
        this.insertCandidate(momentId, {
          ...this.base(c),
          outcome: "lifted",
          reason: "not_read_capped",
          extra: { denies: state?.denies ?? 0 },
        });
        continue;
      }
      denyQueue.push(c);
    }
    if (denyQueue.length > 0) {
      return this.deny(harness, sel, epoch, momentId, runId, delivery, denyQueue, now);
    }
    return this.printCarries(harness, sel, epoch, momentId, runId, delivery);
  }

  private liftWithout(
    harness: string,
    session: string,
    epoch: number,
    c: Candidate,
    outcome: string,
    now: number,
  ) {
    this.run(
      `INSERT INTO deny_state (harness, session, epoch, entry, version, rule_kind, target, denies, lifted, lift_outcome, lifted_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?)
       ON CONFLICT (harness, session, epoch, entry, version) DO UPDATE SET lifted = 1,
         lift_outcome = excluded.lift_outcome, lifted_at = excluded.lifted_at`,
      harness,
      session,
      epoch,
      c.entry,
      c.version,
      c.rule ?? "inline",
      c.target,
      outcome,
      now,
    );
  }

  /**
   * A deny carries every whole matching rule that fits the deny budget and the
   * output limit, and always the first, cut only when it alone exceeds the
   * output limit. The others stay `deferred` and deny their next matching call.
   */
  private deny(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
    queue: Candidate[],
    now: number,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    const left = this.budgetLeft(runId);
    const out = new Composer(
      sel.profile.heads.deny,
      Math.min(sel.profile.limit_bytes, sel.profile.deny_limit_bytes ?? sel.profile.limit_bytes),
      Math.max(left.deny, 0),
    );
    const claims: Claim[] = [];
    queue.forEach((c, i) => {
      const text = c.deny_text ?? c.text;
      let part = text;
      let whole = true;
      if (i === 0) {
        if (out.overLimit(text)) {
          // A pointer's deny opens with "Read <target> in full", which must never be cut:
          // cut what follows, then give up the head; if that still cannot fit, deny nothing.
          // Built from the entry's own target, never found by searching the text: a path may
          // itself contain " in full".
          const opening =
            (c.rule === "pointer" || c.rule === "entry") && c.target !== null
              ? (c.opening ?? `Read ${c.target} in full`)
              : "";
          const readLine = text.startsWith(opening) ? opening : "";
          if (utf8(readLine) > out.room()) out.dropHead();
          if (utf8(readLine) > out.room()) {
            this.insertCandidate(momentId, {
              ...this.base(c),
              seam: "deny",
              outcome: "cut_budget",
              reason: "target",
            });
            return;
          }
          part = cutBytes(text, out.room());
          whole = part === text;
        }
      } else if (!out.fits(text)) {
        this.insertCandidate(momentId, {
          ...this.base(c),
          seam: "deny",
          outcome: "deferred",
          reason: out.overLimit(text) ? "moment" : "turn",
        });
        return;
      }
      const id = this.insertCandidate(momentId, {
        ...this.base(c),
        seam: "deny",
        stage: "reserved",
        outcome: whole ? "reserved" : "reserved_truncated",
        reason: c.rule,
        bytes: utf8(part),
      });
      const claim: Claim = {
        kind: "deny",
        entry: c.entry,
        version: c.version,
        rule: c.rule,
        whole,
        candidateId: id,
        probe: part,
      };
      out.add(part, claim);
      claims.push(claim);
      // A deny counts toward the cap once shown (on settle or projection evidence), so
      // outstanding and dropped deliveries never spend it and parallel calls stay denied.
      this.run(
        `INSERT INTO deny_state (harness, session, epoch, entry, version, rule_kind, target, denies, first_denied_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
         ON CONFLICT (harness, session, epoch, entry, version) DO UPDATE
         SET rule_kind = excluded.rule_kind, target = excluded.target
         WHERE deny_state.lifted = 0 AND excluded.rule_kind = 'entry'`,
        harness,
        sel.moment.session,
        epoch,
        c.entry,
        c.version,
        c.rule ?? "inline",
        c.target,
        now,
      );
    });
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    const bytes = utf8(text);
    this.charge(runId, 0, bytes);
    this.writeClaims(delivery, harness, sel.moment.session, epoch, runId, claims, {
      kind: sel.profile.deny_evidence ?? "none",
      toolUseId: sel.moment.tool_use_id,
    });
    return [this.render(sel, "deny", text), { runId, serve: 0, deny: bytes }];
  }

  /** Serve matching entries as context: first fit in the serve budget and the output limit. */
  private serveContext(
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    const left = this.budgetLeft(runId);
    const out = new Composer(
      sel.profile.heads.context,
      sel.profile.limit_bytes,
      Math.max(left.serve, 0),
      sel.profile.session_start_limit_bytes,
    );
    const claims: Claim[] = [];
    const pointer = this.eligiblePointer(sel, epoch);
    if (pointer !== null) {
      if (out.fits(pointer.text)) {
        const claim = this.pointerClaim(pointer, momentId, utf8(this.pointerText(sel, pointer)));
        out.add(pointer.text, claim);
        claims.push(claim);
      } else {
        this.insertCandidate(momentId, {
          ...this.base(pointer),
          seam: "context",
          outcome: "cut_budget",
          reason: out.overLimit(pointer.text) ? "moment" : "turn",
        });
      }
    }
    out.keepBack(sel.greatLibraryBytes ?? 0);
    this.appendCarries(sel.profile.harness, sel, epoch, momentId, runId, out, claims);
    for (const c of sel.candidates) {
      if (claims.some((claim) => claim.entry === c.entry && claim.version === c.version)) continue;
      if (this.settledIn(runId, epoch, c.entry, c.version)) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: "deduped" });
        continue;
      }
      if (!out.fits(c.text)) {
        this.insertCandidate(momentId, {
          ...this.base(c),
          seam: "context",
          outcome: "cut_budget",
          reason: out.overLimit(c.text) ? "moment" : "turn",
        });
        continue;
      }
      const id = this.insertCandidate(momentId, {
        ...this.base(c),
        seam: "context",
        stage: "reserved",
        outcome: "reserved",
        bytes: utf8(c.text),
      });
      const claim: Claim = {
        kind: "serve",
        entry: c.entry,
        version: c.version,
        rule: null,
        whole: true,
        candidateId: id,
      };
      out.add(c.text, claim);
      claims.push(claim);
    }
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    const bytes = out.budgetBytes();
    this.charge(runId, bytes, 0);
    this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, claims);
    return [this.render(sel, "context", text), { runId, serve: bytes, deny: 0 }];
  }

  /** Keep context until the session's next applicable hook, including startup hooks that timed out. */
  private carry(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    now: number,
  ) {
    for (const c of sel.candidates) {
      if (this.settledIn(runId, epoch, c.entry, c.version)) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: "deduped" });
        continue;
      }
      const older = this.all<{ id: number; candidate_id: number; version: string }>(
        "SELECT id, candidate_id, version FROM carries WHERE harness = ? AND session = ? AND entry = ? AND state = 'waiting'",
        harness,
        sel.moment.session,
        c.entry,
      );
      if (older.some((o) => o.version === c.version)) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: "deduped", reason: "carried" });
        continue;
      }
      for (const o of older) {
        this.run("UPDATE carries SET state = 'superseded' WHERE id = ?", o.id);
        this.run("UPDATE candidates SET outcome = 'superseded' WHERE id = ?", o.candidate_id);
      }
      const id = this.insertCandidate(momentId, {
        ...this.base(c),
        seam: "carry",
        outcome: "carried",
      });
      this.run(
        `INSERT INTO carries (harness, session, run_id, entry, version, text, candidate_id, state, created_at,
           matcher, score)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting', ?, ?, ?)`,
        harness,
        sel.moment.session,
        runId,
        c.entry,
        c.version,
        c.text,
        id,
        now,
        c.matcher,
        c.score ?? null,
      );
    }
  }

  private expireCarries(harness: string, session: string, runId: string) {
    const stale = this.all<{ id: number; candidate_id: number }>(
      "SELECT id, candidate_id FROM carries WHERE harness = ? AND session = ? AND state = 'waiting' AND run_id != ?",
      harness,
      session,
      runId,
    );
    for (const s of stale) {
      this.run("UPDATE carries SET state = 'expired' WHERE id = ?", s.id);
      this.run("UPDATE candidates SET outcome = 'carry_expired' WHERE id = ?", s.candidate_id);
    }
  }

  private printCarries(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    delivery: string,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    const left = this.budgetLeft(runId);
    const out = new Composer(
      sel.profile.heads.context,
      sel.profile.limit_bytes,
      Math.max(left.serve - (sel.greatLibraryBytes ?? 0), 0),
      sel.profile.session_start_limit_bytes,
    );
    const claims: Claim[] = [];
    this.appendCarries(harness, sel, epoch, momentId, runId, out, claims);
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    const bytes = out.budgetBytes();
    this.charge(runId, bytes, 0);
    this.writeClaims(delivery, harness, sel.moment.session, epoch, runId, claims);
    return [this.render(sel, "context", text), { runId, serve: bytes, deny: 0 }];
  }

  private hasCarries(harness: string, session: string, runId: string) {
    return (
      this.get(
        "SELECT 1 FROM carries WHERE harness = ? AND session = ? AND run_id = ? AND state = 'waiting' LIMIT 1",
        harness,
        session,
        runId,
      ) !== undefined
    );
  }

  private appendCarries(
    harness: string,
    sel: Selection,
    epoch: number,
    momentId: number,
    runId: string,
    out: Composer,
    claims: Claim[],
    freshFiles: ReadonlyArray<Candidate> = [],
  ) {
    const waiting = this.all<{
      id: number;
      entry: string;
      version: string;
      text: string;
      matcher: string | null;
      score: number | null;
    }>(
      "SELECT id, entry, version, text, matcher, score FROM carries WHERE harness = ? AND session = ? AND run_id = ? AND state = 'waiting' ORDER BY id",
      harness,
      sel.moment.session,
      runId,
    );
    for (const w of waiting) {
      if (freshFiles.some((c) => c.entry === w.entry && c.version === w.version)) continue;
      if (claims.some((c) => c.entry === w.entry && c.version === w.version)) continue;
      if (
        this.settledIn(runId, epoch, w.entry, w.version) ||
        (w.matcher === "great_library" &&
          !this.greatLibraryOwed(harness, sel.moment.session, epoch))
      ) {
        this.run("UPDATE carries SET state = 'expired' WHERE id = ?", w.id);
        continue;
      }
      const base = {
        matcher: w.matcher ?? "exact_key",
        entry: w.entry,
        version: w.version,
        score: w.score,
      };
      const startup = w.matcher === "session_start" && sel.profile.session_start_limit_bytes > 0;
      if (
        startup &&
        out.exceedsLimitAlone(w.text, true) &&
        utf8(w.text) + 2 <= sel.profile.session_start_limit_bytes
      )
        out.dropHead();
      if (!out.fits(w.text, startup)) {
        this.insertCandidate(momentId, {
          ...base,
          seam: "carry_print",
          outcome: "cut_budget",
          reason: out.overLimit(w.text, startup) ? "moment" : "turn",
        });
        // Startup files have no heading of their own. Other hook content can only defer them.
        if (
          startup
            ? utf8(w.text) + 2 > sel.profile.session_start_limit_bytes
            : out.exceedsLimitAlone(w.text)
        ) {
          this.run("UPDATE carries SET state = 'cut_budget' WHERE id = ?", w.id);
          this.run(
            "UPDATE candidates SET outcome = 'cut_budget', reason = 'moment' WHERE id = (SELECT candidate_id FROM carries WHERE id = ?)",
            w.id,
          );
        }
        continue;
      }
      const id = this.insertCandidate(momentId, {
        ...base,
        seam: "carry_print",
        stage: "reserved",
        outcome: "reserved",
        bytes: utf8(w.text),
      });
      const claim: Claim = {
        kind: "serve",
        entry: w.entry,
        version: w.version,
        rule: null,
        whole: true,
        candidateId: id,
        carryId: w.id,
      };
      out.add(w.text, claim, startup);
      claims.push(claim);
    }
  }
}
