/**
 * The serving state of one Chromeria environment: run budgets, context epochs,
 * must-know deny state, observed reads, Grok carries, reservations and the
 * serving log, in `<base>/userdata/contextmd.sqlite`.
 *
 * Every method is synchronous and runs in one short transaction on the
 * module's own connection, so concurrent hooks are serialized by the event
 * loop and no transaction spans a network await. Rules (ContextMD Spec #3,
 * "Design: serving state in Chromeria"):
 *
 * - The budget key is the Chromeria run: an 8,000-byte serve budget and a
 *   2,000-byte deny budget, in UTF-8 bytes. Session-start files sit outside both.
 * - A reply is `reserved` until the hook settles it. Only settled claims count
 *   for dedupe and deny lifts; a reservation not settled within
 *   the harness's settle deadline is released as `delivery_unverified`, with its bytes
 *   and claims returned.
 * - A must-know entry denies every matching call until it lifts: a pointer
 *   rule on an observed successful read of its target in the same session and
 *   epoch, an inline rule on a settled whole delivery. After `DENY_CAP`
 *   denies the next matching call passes, logged `not_read_capped`.
 * - Compaction starts a new context epoch, which re-arms denies and resets
 *   dedupe.
 */
import type * as NodeSqlite from "node:sqlite";

import type { Candidate, Selection } from "./Selection.ts";

export const SERVE_BUDGET_BYTES = 8_000;
export const DENY_BUDGET_BYTES = 2_000;
const DENY_CAP = 3;
/** A denied call is projected within seconds; after this a deny without its item stays unproven. */
const CHECK_GIVE_UP_SECONDS = 60;

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
`;

/** How the moment joined to Chromeria: a running run, or why it did not. */
export type RunJoin =
  | { readonly type: "run"; readonly runId: string; readonly threadId: string }
  | { readonly type: "unjoined" }
  | { readonly type: "ambiguous" };

export interface MomentInput {
  readonly harness: string;
  readonly delivery: string;
  readonly selection: Selection;
  readonly join: RunJoin;
  /** Projected exit status of earlier calls whose reads are pending (Codex). */
  readonly readChecks?: ReadonlyArray<ReadCheck>;
  readonly now: number;
  readonly startedAt: number;
}

export type Reply = { readonly status: 200; readonly body: string } | { readonly status: 204 };

/** A moment that never reached selection: why, for the log. */
export interface Unserved {
  readonly harness: string | null;
  readonly delivery: string | null;
  readonly status:
    | "unmapped"
    | "bad_config"
    | "bad_event"
    | "oversize"
    | "unauthorized"
    | "bad_request";
  readonly reason?: string;
  readonly now: number;
  readonly startedAt: number;
}

const utf8 = (text: string) => Buffer.byteLength(text, "utf8");

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
  readonly matcher: string;
  readonly entry?: string | null;
  readonly version?: string | null;
  readonly key?: string | null;
  readonly seam?: string | null;
  readonly stage?: string | null;
  readonly outcome: string;
  readonly reason?: string | null;
  readonly bytes?: number | null;
  readonly extra?: Record<string, unknown>;
}

/** Collects the parts of one output under the per-output byte limit and one budget. */
class Composer {
  readonly parts: Array<{ text: string; claim: Claim }> = [];
  private readonly head: string;
  private readonly limit: number;
  private budgetLeft: number;
  private used: number;
  constructor(head: string, limit: number, budgetLeft: number) {
    this.head = head;
    this.limit = limit;
    this.budgetLeft = budgetLeft;
    this.used = utf8(head);
  }
  private cost = (text: string) => utf8(text) + 2;
  fits = (text: string) =>
    this.used + this.cost(text) <= this.limit &&
    this.cost(text) + (this.parts.length ? 0 : utf8(this.head)) <= this.budgetLeft;
  overLimit = (text: string) => this.used + this.cost(text) > this.limit;
  add = (text: string, claim: Claim) => {
    const cost = this.cost(text) + (this.parts.length ? 0 : utf8(this.head));
    this.used += this.cost(text);
    this.budgetLeft -= cost;
    this.parts.push({ text, claim });
  };
  /** Room left for one more part under the output limit. */
  room = () => this.limit - this.used - 2;
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
  replayed(delivery: string): Reply | undefined {
    const row = this.get<{ status: number; body: string | null }>(
      "SELECT status, body FROM deliveries WHERE delivery = ?",
      delivery,
    );
    if (!row) return undefined;
    return row.status === 200 && row.body !== null
      ? { status: 200, body: row.body }
      : { status: 204 };
  }

  heartbeat(now: number) {
    this.run("INSERT OR IGNORE INTO heartbeats (at) VALUES (?)", now);
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
        serverMs: u.now - u.startedAt,
      });
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
      "UPDATE candidates SET stage = 'released', outcome = 'delivery_unverified' WHERE moment_id = ? AND stage = 'reserved'",
      d.moment_id,
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
      if (!d || d.stage !== "reserved") return false;
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
         WHERE moment_id = ? AND stage = 'reserved'`,
        now,
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
   * Record what the projection showed for settled denies. `shown`: the model saw this
   * rule, so an inline rule lifts. `hidden`: the call's projected output lacks it,
   * because another hook's deny replaced it; the rule stays armed, that deny does not
   * count toward the cap, and it no longer dedupes the rule. A deny with no projected
   * item after `CHECK_GIVE_UP_SECONDS` stays armed, logged `unverified`.
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
    serverMs: number;
    kind?: string;
    session?: string;
    threadId?: string | null;
    runId?: string | null;
    toolUseId?: string | null;
    epoch?: number | null;
    holdout?: boolean | null;
    owners?: Record<string, string | null>;
  }) {
    const result = this.run(
      `INSERT INTO moments (received_at, harness, kind, session, thread_id, run_id, tool_use_id, epoch, holdout,
         status, reason, owners_json, cue_adapter, server_ms, delivery)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
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
      m.serverMs,
      m.delivery,
    );
    return Number(result.lastInsertRowid);
  }

  private insertCandidate(momentId: number, c: CandidateRow) {
    const result = this.run(
      `INSERT INTO candidates (moment_id, matcher, entry, version, key, seam, stage, outcome, reason, score, bytes, extra_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
      momentId,
      c.matcher,
      c.entry ?? null,
      c.version ?? null,
      c.key ?? null,
      c.seam ?? null,
      c.stage ?? null,
      c.outcome,
      c.reason ?? null,
      c.bytes ?? null,
      c.extra ? JSON.stringify(c.extra) : null,
    );
    return Number(result.lastInsertRowid);
  }

  private epochFor(harness: string, sel: Selection): number {
    const row = this.get<{ epoch: number }>(
      "SELECT epoch FROM sessions WHERE harness = ? AND session = ?",
      harness,
      sel.moment.session,
    );
    const bump =
      row !== undefined &&
      (sel.moment.kind === "compaction" ||
        (sel.moment.kind === "session_start" &&
          sel.moment.source !== null &&
          sel.profile.new_epoch_sources.includes(sel.moment.source)));
    const epoch = row === undefined ? 0 : row.epoch + (bump ? 1 : 0);
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
    const replay = this.replayed(input.delivery);
    if (replay !== undefined) return replay;
    const { harness, selection: sel, now } = input;
    const m = sel.moment;
    const epoch = this.epochFor(harness, sel);
    const joined = input.join.type === "run" ? input.join : null;
    const status = input.join.type === "run" ? "ok" : input.join.type;
    const momentId = this.insertMoment({
      harness,
      status,
      delivery: input.delivery,
      now,
      serverMs: now - input.startedAt,
      kind: m.kind,
      session: m.session,
      threadId: joined?.threadId ?? null,
      runId: joined?.runId ?? null,
      toolUseId: m.tool_use_id,
      epoch,
      holdout: isHoldout(harness, m.session),
      owners: sel.owners,
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
           created_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      );
      return reply;
    };
    if (joined === null) {
      for (const c of sel.candidates) {
        this.insertCandidate(momentId, { ...this.base(c), outcome: status });
      }
      return finish({ status: 204 });
    }
    this.expireCarries(harness, m.session, joined.runId);
    this.applyReadChecks(harness, m.session, input.readChecks ?? [], momentId, now);
    if ((m.kind === "tool_result" || m.kind === "tool_failure") && m.failed !== true) {
      this.recordReads(harness, sel, epoch, momentId, now);
    }
    const delivering = sel.profile.delivering.includes(m.kind);
    switch (m.kind) {
      case "session_start":
        return finish(...this.sessionStart(sel, momentId, joined.runId, delivering));
      case "pre_tool":
        return finish(
          ...this.preTool(harness, sel, epoch, momentId, joined.runId, input.delivery, now),
        );
      case "tool_result":
      case "tool_failure": {
        if (!delivering && sel.profile.carry_to !== null) {
          this.carry(harness, sel, epoch, momentId, joined.runId, now);
          return finish({ status: 204 });
        }
        if (!delivering) {
          for (const c of sel.candidates) {
            this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
          }
          return finish({ status: 204 });
        }
        return finish(...this.serveContext(sel, epoch, momentId, joined.runId, input.delivery));
      }
      default:
        return finish({ status: 204 });
    }
  }

  private base(c: Candidate) {
    return { matcher: c.matcher, entry: c.entry, version: c.version, key: c.key };
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

  /** Lift every pointer rule waiting on this file in this session and epoch. */
  private liftOnRead(
    harness: string,
    session: string,
    epoch: number,
    read: { readonly path: string; readonly kind: string },
    momentId: number,
    now: number,
  ) {
    const waiting = this.all<{
      entry: string;
      version: string;
      denies: number;
      first_denied_at: number | null;
    }>(
      `SELECT entry, version, denies, first_denied_at FROM deny_state
       WHERE harness = ? AND session = ? AND epoch = ? AND target = ? AND lifted = 0`,
      harness,
      session,
      epoch,
      read.path,
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
        const proven = this.all<{ path: string; kind: string; epoch: number }>(
          `SELECT path, kind, epoch FROM reads
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
          this.liftOnRead(harness, session, read.epoch, read, momentId, now);
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
        "SELECT 1 FROM reads WHERE harness = ? AND session = ? AND epoch = ? AND path = ? AND pending_tool_use_id IS NULL",
        harness,
        session,
        epoch,
        target,
      ) !== undefined
    );
  }

  private sessionStart(
    sel: Selection,
    momentId: number,
    runId: string,
    delivering: boolean,
  ): [Reply, { runId: string; serve: number; deny: number }?] {
    if (!delivering) {
      for (const c of sel.candidates)
        this.insertCandidate(momentId, { ...this.base(c), outcome: "no_seam" });
      return [{ status: 204 }];
    }
    const out = new Composer("", sel.profile.session_start_limit_bytes, Number.POSITIVE_INFINITY);
    for (const c of sel.candidates) {
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
      out.add(c.text, {
        kind: "start",
        entry: c.entry,
        version: c.version,
        rule: null,
        whole: true,
        candidateId: id,
      });
    }
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    return [this.render(sel, "context", text), { runId, serve: 0, deny: 0 }];
  }

  private render(sel: Selection, template: "context" | "deny", text: string): Reply {
    const body = renderTemplate(sel.profile.templates[template], {
      text,
      event: sel.moment.event ?? "",
    });
    return { status: 200, body: JSON.stringify(body) };
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
    if (sel.profile.carry_to === "pre_tool") {
      return this.printCarries(harness, sel, epoch, momentId, runId, delivery);
    }
    return [{ status: 204 }];
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
      sel.profile.limit_bytes,
      Math.max(left.deny, 0),
    );
    const claims: Claim[] = [];
    queue.forEach((c, i) => {
      const text = c.deny_text ?? c.text;
      let part = text;
      let whole = true;
      if (i === 0) {
        if (out.overLimit(text)) {
          part = cutBytes(text, out.room());
          whole = false;
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
         ON CONFLICT (harness, session, epoch, entry, version) DO NOTHING`,
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
    const text = out.text()!;
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
    );
    const claims: Claim[] = [];
    for (const c of sel.candidates) {
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
    const bytes = utf8(text);
    this.charge(runId, bytes, 0);
    this.writeClaims(delivery, sel.profile.harness, sel.moment.session, epoch, runId, claims);
    return [this.render(sel, "context", text), { runId, serve: bytes, deny: 0 }];
  }

  /** Grok: a serve selected where the harness discards hook output waits for the next PreToolUse. */
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
        `INSERT INTO carries (harness, session, run_id, entry, version, text, candidate_id, state, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting', ?)`,
        harness,
        sel.moment.session,
        runId,
        c.entry,
        c.version,
        c.text,
        id,
        now,
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
    const waiting = this.all<{ id: number; entry: string; version: string; text: string }>(
      "SELECT id, entry, version, text FROM carries WHERE harness = ? AND session = ? AND run_id = ? AND state = 'waiting' ORDER BY id",
      harness,
      sel.moment.session,
      runId,
    );
    if (waiting.length === 0) return [{ status: 204 }];
    const left = this.budgetLeft(runId);
    const out = new Composer(
      sel.profile.heads.context,
      sel.profile.limit_bytes,
      Math.max(left.serve, 0),
    );
    const claims: Claim[] = [];
    for (const w of waiting) {
      const base = { matcher: "exact_key", entry: w.entry, version: w.version };
      if (!out.fits(w.text)) {
        this.insertCandidate(momentId, {
          ...base,
          seam: "carry_print",
          outcome: "cut_budget",
          reason: out.overLimit(w.text) ? "moment" : "turn",
        });
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
      out.add(w.text, claim);
      claims.push(claim);
    }
    const text = out.text();
    if (text === null) return [{ status: 204 }];
    const bytes = utf8(text);
    this.charge(runId, bytes, 0);
    this.writeClaims(delivery, harness, sel.moment.session, epoch, runId, claims);
    return [this.render(sel, "context", text), { runId, serve: bytes, deny: 0 }];
  }
}
