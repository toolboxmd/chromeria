import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import { AuthSessionId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../persistence/AuthSessions.ts";

export class RenewalRejected extends Schema.TaggedError<RenewalRejected>()("RenewalRejected", {
  revokedSessionId: Schema.optional(AuthSessionId),
}) {}
export class RenewalPending extends Schema.TaggedError<RenewalPending>()("RenewalPending", {}) {}
export class RenewalConflict extends Schema.TaggedError<RenewalConflict>()("RenewalConflict", {}) {}
export class RenewalStale extends Schema.TaggedError<RenewalStale>()("RenewalStale", {}) {}
export class RenewalStorageError extends Schema.TaggedError<RenewalStorageError>()(
  "RenewalStorageError",
  { cause: Schema.Defect() },
) {}

const isRejected = Schema.is(RenewalRejected);
const isConflict = Schema.is(RenewalConflict);
const isStale = Schema.is(RenewalStale);
const isPending = Schema.is(RenewalPending);
export const commitment = renewalCommitment;

export interface Enrollment {
  readonly enrollmentId: string;
  readonly sessionId: AuthSessionId;
  readonly fingerprint: string;
  readonly requestedAt: string;
  readonly lastSeenAt: string;
  readonly route: string | null;
  readonly ipAddress: string | null;
  readonly client: string | null;
  readonly deviceType: string;
  readonly os: string | null;
  readonly expiresAt: string;
  readonly lastConnectedAt: string | null;
  readonly state: string;
}

export class SessionRenewal extends Context.Service<
  SessionRenewal,
  {
    readonly enroll: (input: {
      sessionId: AuthSessionId;
      commitment: string;
      approved: boolean;
    }) => Effect.Effect<void, RenewalRejected | RenewalStorageError>;
    readonly requestEnrollment: (input: {
      sessionId: AuthSessionId;
      commitment: string;
      route: string | null;
      ipAddress: string | null;
    }) => Effect.Effect<
      { enrollmentId: string; fingerprint: string; state: "pending_approval" | "approved" },
      RenewalRejected | RenewalConflict | RenewalStorageError
    >;
    readonly listEnrollments: () => Effect.Effect<ReadonlyArray<Enrollment>, RenewalStorageError>;
    readonly approveEnrollment: (
      id: string,
      expectedCommitment: string,
    ) => Effect.Effect<void, RenewalRejected | RenewalConflict | RenewalStorageError>;
    readonly rejectEnrollment: (id: string) => Effect.Effect<void, RenewalStorageError>;
    readonly renew: (input: { secret: string; next: string }) => Effect.Effect<
      {
        session: AuthSessions.AuthSessionRecord;
        generation: number;
        outcome: "current" | "rotated";
        renewAfter: number;
      },
      RenewalRejected | RenewalStale | RenewalPending | RenewalStorageError
    >;
  }
>()("t3/auth/SessionRenewal") {}

const eligible = (row: AuthSessions.AuthSessionRecord) =>
  (row.method === "browser-session-cookie" || row.method === "bearer-access-token") &&
  !["desktop-bootstrap", "reusable-dev-token", "mcp-client", "managed-relay-bootstrap"].includes(
    row.subject,
  );

/** @public Canonical Effect service construction. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  yield* sql`CREATE TABLE IF NOT EXISTS auth_session_renewal (
    session_id TEXT PRIMARY KEY REFERENCES auth_sessions(session_id),
    current_hash TEXT NOT NULL UNIQUE, previous_hash TEXT, previous_next_hash TEXT,
    generation INTEGER NOT NULL DEFAULT 0, renew_after TEXT NOT NULL,
    approved INTEGER NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS auth_session_renewal_spent (
    hash TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES auth_sessions(session_id),
    successor_hash TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS auth_session_renewal_enrollment (
    enrollment_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES auth_sessions(session_id),
    commitment_hash TEXT NOT NULL UNIQUE, requested_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
    route TEXT, ip_address TEXT, state TEXT NOT NULL DEFAULT 'pending', UNIQUE(session_id, commitment_hash)
  )`;
  const active = Effect.fnUntraced(function* (sessionId: AuthSessionId) {
    const row = yield* sessions.getById({ sessionId });
    const now = yield* DateTime.now;
    if (
      Option.isNone(row) ||
      row.value.revokedAt !== null ||
      row.value.expiresAt.epochMilliseconds <= now.epochMilliseconds ||
      !eligible(row.value)
    ) {
      return yield* new RenewalRejected({});
    }
    return row.value;
  });
  const enroll = Effect.fn("SessionRenewal.enroll")(
    function* (input: { sessionId: AuthSessionId; commitment: string; approved: boolean }) {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const row = yield* active(input.sessionId);
          const renewAfter = DateTime.makeUnsafe(
            (row.issuedAt.epochMilliseconds + row.expiresAt.epochMilliseconds) / 2,
          );
          yield* sql`INSERT INTO auth_session_renewal (session_id, current_hash, renew_after, approved)
        VALUES (${input.sessionId}, ${input.commitment}, ${DateTime.formatIso(renewAfter)}, ${input.approved ? 1 : 0})
        ON CONFLICT(session_id) DO NOTHING`;
        }),
      );
    },
    Effect.mapError((cause) => (isRejected(cause) ? cause : new RenewalStorageError({ cause }))),
  );

  const requestEnrollment: SessionRenewal["Service"]["requestEnrollment"] = Effect.fn(
    "SessionRenewal.requestEnrollment",
  )(function* (input) {
    const id = yield* crypto
      .randomUUIDv4()
      .pipe(Effect.mapError((cause) => new RenewalStorageError({ cause })));
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* active(input.sessionId);
          const existing = yield* sql<{
            enrollmentId: string;
            state: string;
          }>`SELECT enrollment_id AS "enrollmentId", state
          FROM auth_session_renewal_enrollment WHERE session_id = ${input.sessionId} AND commitment_hash = ${input.commitment}`;
          if (existing[0]?.state === "rejected") return yield* new RenewalRejected({});
          if (existing.length === 0) {
            const enrolled =
              yield* sql`SELECT 1 FROM auth_session_renewal WHERE session_id = ${input.sessionId}`;
            if (enrolled.length > 0) return yield* new RenewalConflict({});
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`INSERT INTO auth_session_renewal_enrollment
          (enrollment_id, session_id, commitment_hash, requested_at, last_seen_at, route, ip_address)
          VALUES (${id}, ${input.sessionId}, ${input.commitment}, ${now}, ${now}, ${input.route}, ${input.ipAddress})
          ON CONFLICT(session_id, commitment_hash) DO UPDATE SET last_seen_at = excluded.last_seen_at,
            route = excluded.route, ip_address = excluded.ip_address`;
          return {
            enrollmentId: existing[0]?.enrollmentId ?? id,
            fingerprint: input.commitment,
            state:
              existing[0]?.state === "approved"
                ? ("approved" as const)
                : ("pending_approval" as const),
          };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isRejected(cause) || isConflict(cause) ? cause : new RenewalStorageError({ cause }),
        ),
      );
  });
  const listEnrollments = Effect.fn("SessionRenewal.listEnrollments")(
    function* () {
      return yield* sql<Enrollment>`SELECT e.enrollment_id AS "enrollmentId", e.session_id AS "sessionId",
      e.commitment_hash AS fingerprint, e.requested_at AS "requestedAt", e.last_seen_at AS "lastSeenAt",
      e.route, e.ip_address AS "ipAddress", s.client_label AS client, s.client_device_type AS "deviceType",
      s.client_os AS os, s.expires_at AS "expiresAt", s.last_connected_at AS "lastConnectedAt",
      CASE WHEN s.revoked_at IS NOT NULL THEN 'revoked' ELSE e.state END AS state
      FROM auth_session_renewal_enrollment e JOIN auth_sessions s ON s.session_id = e.session_id
      ORDER BY e.requested_at DESC`;
    },
    Effect.mapError((cause) => new RenewalStorageError({ cause })),
  );
  const approveEnrollment: SessionRenewal["Service"]["approveEnrollment"] = Effect.fn(
    "SessionRenewal.approveEnrollment",
  )(function* (id, expectedCommitment) {
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            sessionId: string;
            commitment: string;
            state: string;
          }>`SELECT session_id AS "sessionId",
          commitment_hash AS commitment, state FROM auth_session_renewal_enrollment WHERE enrollment_id = ${id}`;
          const entry = rows[0];
          if (
            !entry ||
            entry.commitment !== expectedCommitment ||
            (entry.state !== "pending" && entry.state !== "approved")
          )
            return yield* new RenewalRejected({});
          const sessionId = AuthSessionId.make(entry.sessionId);
          yield* active(sessionId);
          if (entry.state === "approved") return;
          const enrolled =
            yield* sql`SELECT 1 FROM auth_session_renewal WHERE session_id = ${sessionId}`;
          if (enrolled.length > 0) return yield* new RenewalConflict({});
          yield* sql`INSERT INTO auth_session_renewal (session_id, current_hash, renew_after, approved)
          VALUES (${sessionId}, ${entry.commitment}, ${DateTime.formatIso(yield* DateTime.now)}, 1)`;
          yield* sql`UPDATE auth_session_renewal_enrollment SET state = 'rejected' WHERE session_id = ${sessionId} AND enrollment_id <> ${id} AND state = 'pending'`;
          yield* sql`UPDATE auth_session_renewal_enrollment SET state = 'approved' WHERE enrollment_id = ${id}`;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isRejected(cause) || isConflict(cause) ? cause : new RenewalStorageError({ cause }),
        ),
      );
  });
  const rejectEnrollment = Effect.fn("SessionRenewal.rejectEnrollment")(
    function* (id: string) {
      yield* sql`UPDATE auth_session_renewal_enrollment SET state = 'rejected' WHERE enrollment_id = ${id} AND state = 'pending'`;
    },
    Effect.mapError((cause) => new RenewalStorageError({ cause })),
  );

  const renew = Effect.fn("SessionRenewal.renew")(
    function* (input: { secret: string; next: string }) {
      // Commit reuse revocation before returning its authentication error.
      const result = yield* sql.withTransaction(
        Effect.gen(function* () {
          const hash = commitment(input.secret);
          const rows = yield* sql<{
            sessionId: string;
            currentHash: string;
            previousHash: string | null;
            previousNextHash: string | null;
            generation: number;
            renewAfter: string;
            approved: number;
          }>`SELECT session_id AS "sessionId", current_hash AS "currentHash", previous_hash AS "previousHash",
          previous_next_hash AS "previousNextHash", generation, renew_after AS "renewAfter", approved
        FROM auth_session_renewal WHERE current_hash = ${hash} OR previous_hash = ${hash}
        OR session_id IN (SELECT session_id FROM auth_session_renewal_spent WHERE hash = ${hash})`;
          const renewal = rows[0];
          if (!renewal) {
            const pending =
              yield* sql`SELECT 1 FROM auth_session_renewal_enrollment e JOIN auth_sessions s
          ON s.session_id = e.session_id WHERE e.commitment_hash = ${hash} AND e.state = 'pending'
          AND s.revoked_at IS NULL AND s.expires_at > ${DateTime.formatIso(yield* DateTime.now)}`;
            if (pending.length > 0) return { kind: "pending" } as const;
            return { kind: "rejected" } as const;
          }
          if (renewal.approved !== 1) return { kind: "rejected" } as const;
          const sessionId = AuthSessionId.make(renewal.sessionId);
          const row = yield* active(sessionId);
          const now = yield* DateTime.now;
          if (renewal.currentHash !== hash) {
            const spent = yield* sql<{
              successorHash: string;
            }>`SELECT successor_hash AS "successorHash"
          FROM auth_session_renewal_spent WHERE hash = ${hash} AND session_id = ${sessionId}`;
            const successor =
              renewal.previousHash === hash ? renewal.previousNextHash : spent[0]?.successorHash;
            if (successor !== input.next) {
              yield* sessions.revoke({ sessionId, revokedAt: now });
              return { kind: "rejected", revokedSessionId: sessionId } as const;
            }
            if (renewal.previousHash !== hash) return { kind: "stale" } as const;
            return {
              kind: "success",
              session: row,
              generation: renewal.generation,
              outcome: "rotated",
              renewAfter: DateTime.makeUnsafe(renewal.renewAfter).epochMilliseconds,
            } as const;
          }
          if (now.epochMilliseconds < DateTime.makeUnsafe(renewal.renewAfter).epochMilliseconds) {
            return {
              kind: "success",
              session: row,
              generation: renewal.generation,
              outcome: "current",
              renewAfter: DateTime.makeUnsafe(renewal.renewAfter).epochMilliseconds,
            } as const;
          }
          const reused =
            yield* sql`SELECT 1 FROM auth_session_renewal WHERE current_hash = ${input.next} OR previous_hash = ${input.next}
        UNION ALL SELECT 1 FROM auth_session_renewal_spent WHERE hash = ${input.next}`;
          if (reused.length > 0) return { kind: "rejected" } as const;
          const lifetime = row.expiresAt.epochMilliseconds - row.issuedAt.epochMilliseconds;
          const expiresAt = DateTime.makeUnsafe(now.epochMilliseconds + lifetime);
          const renewAfter = DateTime.makeUnsafe(now.epochMilliseconds + lifetime / 2);
          if (renewal.previousHash !== null && renewal.previousNextHash !== null) {
            yield* sql`INSERT INTO auth_session_renewal_spent (hash, session_id, successor_hash)
          VALUES (${renewal.previousHash}, ${sessionId}, ${renewal.previousNextHash})`;
          }
          yield* sql`UPDATE auth_session_renewal SET previous_hash = current_hash, previous_next_hash = ${input.next},
        current_hash = ${input.next}, generation = generation + 1, renew_after = ${DateTime.formatIso(renewAfter)}
        WHERE session_id = ${sessionId} AND current_hash = ${hash}`;
          const updated =
            yield* sql`UPDATE auth_sessions SET issued_at = ${DateTime.formatIso(now)}, expires_at = ${DateTime.formatIso(expiresAt)}
        WHERE session_id = ${sessionId} AND revoked_at IS NULL AND expires_at > ${DateTime.formatIso(now)}
        RETURNING session_id`;
          if (updated.length !== 1) return yield* new RenewalRejected({});
          const persisted = yield* sessions.getById({ sessionId });
          if (Option.isNone(persisted)) return yield* new RenewalRejected({});
          return {
            kind: "success",
            session: persisted.value,
            generation: renewal.generation + 1,
            outcome: "rotated",
            renewAfter: renewAfter.epochMilliseconds,
          } as const;
        }),
      );
      if (result.kind === "rejected")
        return yield* new RenewalRejected(
          "revokedSessionId" in result ? { revokedSessionId: result.revokedSessionId } : {},
        );
      if (result.kind === "pending") return yield* new RenewalPending({});
      if (result.kind === "stale") return yield* new RenewalStale({});
      return result;
    },
    Effect.mapError((cause) =>
      isRejected(cause) || isStale(cause) || isPending(cause)
        ? cause
        : new RenewalStorageError({ cause }),
    ),
  );
  return SessionRenewal.of({
    enroll,
    renew,
    requestEnrollment,
    listEnrollments,
    approveEnrollment,
    rejectEnrollment,
  });
});

export const layer = Layer.effect(SessionRenewal, make).pipe(Layer.provide(AuthSessions.layer));
