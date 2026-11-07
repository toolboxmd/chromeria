import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import type {
  OrchestrationV2DomainEvent,
  ThreadId,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ExecutionRecord as Authority, conjunction } from "./PeerAuthorityRecord.ts";
import type { PendingOrchestrationEffectV2 } from "../orchestration-v2/EffectOutbox.ts";

/** Set only by authenticated tool intake. The event sink persists it with the accepted input. */
export const PeerDispatchAuthority = Context.Reference<string | undefined>(
  "t3/peer/PeerDispatchAuthority",
  { defaultValue: () => undefined },
);

/** A destination claim accompanies an incoming caller; neither replaces the other. */
export const PeerAcceptedAutomation = Context.Reference<string | undefined>(
  "t3/peer/PeerAcceptedAutomation",
  { defaultValue: () => undefined },
);

export class NativeAuthorityValidation extends Context.Service<
  NativeAuthorityValidation,
  {
    readonly preflight: (
      ownerIds: ReadonlyArray<string>,
      threadIds: ReadonlyArray<ThreadId>,
    ) => Effect.Effect<
      {
        readonly births: ReadonlySet<string>;
        readonly check: Effect.Effect<void, OrchestratorMcpFailure>;
      },
      OrchestratorMcpFailure
    >;
  }
>()("t3/peer/peerEffectAuthority/NativeAuthorityValidation") {}

export const nativeAuthorityOwners = Effect.fn("peerEffectAuthority.nativeAuthorityOwners")(
  function* (
    explicit: ReadonlyArray<string>,
    threadIds: ReadonlyArray<ThreadId>,
    runIds: ReadonlyArray<string>,
  ) {
    const sql = yield* SqlClient.SqlClient;
    const births =
      threadIds.length === 0
        ? []
        : yield* sql<{
            record_id: string;
          }>`SELECT record_id FROM peer_thread_authority WHERE ${sql.in("thread_id", threadIds)}`;
    const runs =
      runIds.length === 0
        ? []
        : yield* sql<{
            record_id: string;
          }>`SELECT record_id FROM peer_run_authority WHERE ${sql.in("run_id", runIds)}`;
    return [
      ...new Set([
        ...explicit,
        ...births.map((row) => row.record_id),
        ...runs.map((row) => row.record_id),
      ]),
    ].sort();
  },
);

/** Initialized with the event sink so local-only services need no peer runtime. */
export const initializePeerEffectAuthority = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_task_stop_authority (task_id TEXT NOT NULL,request_id TEXT NOT NULL,record_id TEXT NOT NULL,PRIMARY KEY(task_id,request_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_effect_authority (effect_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_run_authority (run_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_thread_authority (thread_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_message_authority (message_id TEXT PRIMARY KEY, record_id TEXT NOT NULL)`;
  // Existing queued inputs already have a run owner. Preserve that owner if
  // their stable message is subsequently promoted into another active run.
  const projections =
    yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orchestration_v2_projection_runs'`;
  if (projections.length > 0) {
    yield* sql`INSERT INTO peer_message_authority (message_id, record_id)
      SELECT json_extract(run.payload_json, '$.userMessageId'), owner.record_id
      FROM orchestration_v2_projection_runs AS run JOIN peer_run_authority AS owner ON owner.run_id = run.run_id
      WHERE json_extract(run.payload_json, '$.userMessageId') IS NOT NULL
      ON CONFLICT DO NOTHING`;
  }
});

const decodeRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(Authority));
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeRecord = Schema.encodeSync(Schema.fromJsonString(Authority));

export const recordPeerEffectAuthority = Effect.fn("peerEffectAuthority.recordPeerEffectAuthority")(
  function* (
    events: ReadonlyArray<OrchestrationV2DomainEvent>,
    effects: ReadonlyArray<PendingOrchestrationEffectV2>,
  ) {
    const authority = yield* PeerDispatchAuthority;
    const automation = yield* PeerAcceptedAutomation;
    const sql = yield* SqlClient.SqlClient;
    const read = Effect.fn("peerEffectAuthority.read")(function* (id: string) {
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM peer_authority_records WHERE id = ${id}`;
      if (rows[0] === undefined)
        return yield* Effect.die(new Error("Missing accepted peer authority record"));
      return yield* decodeRecord(rows[0].record_json);
    });
    const combine = Effect.fn("peerEffectAuthority.combine")(function* (
      threadId: ThreadId,
      first: string | undefined,
      next: string | undefined,
    ) {
      if (first === undefined) return next;
      if (next === undefined || next === first) return first;
      const a = yield* read(first);
      const b = yield* read(next);
      const ids = [
        ...new Set(
          [a, b].flatMap((record) =>
            record.context?.type === "run-inputs" && record.context.threadId === threadId
              ? record.context.recordIds
              : [record.id],
          ),
        ),
      ].sort();
      const [head, ...tail] = ids;
      if (head === undefined)
        return yield* Effect.die(new Error("Empty accepted peer authority set"));
      const recordIds: [string, ...Array<string>] = [head, ...tail];
      const context = { type: "run-inputs" as const, threadId, recordIds };
      const records = yield* Effect.forEach(recordIds, read);
      const base = records[0];
      if (base === undefined)
        return yield* Effect.die(new Error("Empty accepted peer authority set"));
      const digest = sha256(new TextEncoder().encode(json(context)));
      const id = `context:${Hex.encode(digest)}`;
      const record = conjunction(id, context, [base, ...records.slice(1)]);
      yield* sql`INSERT INTO peer_authority_records (id, record_json) VALUES (${id}, ${encodeRecord(record)}) ON CONFLICT DO NOTHING`;
      return id;
    });
    const birthOwner: (id: string, seen?: ReadonlySet<string>) => Effect.Effect<string, never> =
      Effect.fn("peerEffectAuthority.birthOwner")(function* (
        id: string,
        seen: ReadonlySet<string> = new Set(),
      ) {
        if (seen.has(id) || seen.size >= 256)
          return yield* Effect.die(new Error("Cyclic native birth owner"));
        const visited = new Set([...seen, id]);
        const record = yield* read(id).pipe(Effect.orDie);
        if ("certificateId" in record) {
          if (record.kind === "automation-birth") return id;
          const birth = { ...record, kind: "automation-birth" as const, id: `birth:${record.id}` };
          yield* sql`INSERT INTO peer_authority_records (id,record_json) VALUES (${birth.id},${encodeRecord(birth)}) ON CONFLICT DO NOTHING`.pipe(
            Effect.orDie,
          );
          return birth.id;
        }
        const context = record.context;
        if (context === undefined || context.type === "task-stop") return id;
        const old =
          context.type === "run-inputs"
            ? context.recordIds
            : context.type === "owned-thread"
              ? [context.creationRecordId, context.runRecordId]
              : [context.runRecordId];
        const ids = yield* Effect.forEach(old, (key) => birthOwner(key, visited));
        if (ids.every((key, index) => key === old[index])) return id;
        const [first, ...rest] = yield* Effect.forEach(ids, (key) => read(key).pipe(Effect.orDie));
        if (first === undefined)
          return yield* Effect.die(new Error("Missing native birth contributor"));
        const recordIds: [string, ...Array<string>] = [
          first.id,
          ...rest.map((record) => record.id),
        ];
        const birthContext =
          context.type === "run-inputs"
            ? { ...context, recordIds }
            : context.type === "owned-thread"
              ? { ...context, creationRecordId: recordIds[0], runRecordId: recordIds[1]! }
              : { ...context, runRecordId: recordIds[0] };
        const birth = conjunction(
          `birth:${id}`,
          birthContext,
          [first, ...rest],
          "resourceSessionId" in record ? record.resourceSessionId : undefined,
        );
        yield* sql`INSERT INTO peer_authority_records (id,record_json) VALUES (${birth.id},${encodeRecord(birth)}) ON CONFLICT DO NOTHING`.pipe(
          Effect.orDie,
        );
        return birth.id;
      });
    const runOwner = (runId: string) =>
      sql<{
        record_id: string;
      }>`SELECT record_id FROM peer_run_authority WHERE run_id = ${runId}`.pipe(
        Effect.map((rows) => rows[0]?.record_id),
      );
    const attach = Effect.fn("peerEffectAuthority.attach")(function* (
      runId: string,
      threadId: ThreadId,
      contributor: string | undefined,
    ) {
      const owner = yield* combine(threadId, yield* runOwner(runId), contributor);
      if (owner !== undefined)
        yield* sql`INSERT INTO peer_run_authority (run_id, record_id) VALUES (${runId}, ${owner}) ON CONFLICT (run_id) DO UPDATE SET record_id = excluded.record_id`;
    });
    // An inherited native Stop is authority for this exact next obligation,
    // never general authority for the now-stopped intermediate thread.
    if (authority !== undefined) {
      const owner = yield* read(authority);
      if (owner.context?.type === "task-stop") {
        for (const effect of effects)
          if (
            ![
              "provider-turn.interrupt",
              "provider-session.detach",
              "delegated-tasks.stop",
            ].includes(effect.request.type) ||
            !owner.context.targets.some((target) => target.threadId === effect.threadId)
          )
            return yield* Effect.die(
              new Error(
                "A stop obligation only authorizes its named native Stop effects and targets",
              ),
            );
      }
      for (const event of events) {
        if (
          event.type === "subagent.updated" &&
          event.payload.remoteChild?.stop?.state === "pending"
        ) {
          if (
            owner.context?.type === "task-stop" &&
            !owner.context.targets.some((target) => target.threadId === event.threadId)
          )
            return yield* Effect.die(
              new Error("A stop obligation cannot own an unrelated source intent"),
            );
          const stop = event.payload.remoteChild.stop;
          yield* sql`INSERT INTO peer_task_stop_authority (task_id,request_id,record_id) VALUES (${event.payload.id},${stop.requestId},${authority}) ON CONFLICT DO NOTHING`;
        }
      }
    }
    const continuations = new Set<string>();
    const promotedMessages = new Set(
      events.flatMap((event) =>
        event.type === "turn-item.updated" &&
        event.payload.type === "user_message" &&
        event.payload.inputIntent === "promoted_queued_to_steer"
          ? [event.payload.messageId]
          : [],
      ),
    );
    for (const event of events) {
      if (event.type === "thread.created") {
        const execution = yield* combine(event.threadId, authority, automation);
        const owner = execution === undefined ? undefined : yield* birthOwner(execution);
        if (owner !== undefined)
          yield* sql`INSERT INTO peer_thread_authority (thread_id, record_id) VALUES (${event.threadId}, ${owner}) ON CONFLICT DO NOTHING`;
      }
      if (event.type !== "run.created") continue;
      const sourceRunId =
        event.payload.restartContinuationOfRunId ?? event.payload.forkPrismContinuationSourceRunId;
      if (sourceRunId !== undefined) continuations.add(event.payload.id);
      // A continuation is owned by its source, including an unowned local
      // source. An unrelated caller cannot become the recovery's authority.
      const owner =
        sourceRunId === undefined
          ? yield* combine(event.threadId, authority, automation)
          : yield* runOwner(sourceRunId);
      yield* attach(event.payload.id, event.threadId, owner);
      if (owner !== undefined)
        yield* sql`INSERT INTO peer_message_authority (message_id, record_id) VALUES (${event.payload.userMessageId}, ${owner}) ON CONFLICT DO NOTHING`;
    }
    for (const event of events) {
      if (
        event.type !== "message.updated" ||
        event.payload.role !== "user" ||
        event.payload.runId === null
      )
        continue;
      const message = event.payload;
      const runId = event.payload.runId;
      const saved = yield* sql<{
        record_id: string;
      }>`SELECT record_id FROM peer_message_authority WHERE message_id = ${message.id}`;
      // Promotion reattaches an existing input, not a new instruction from
      // the actor moving it. Its committed message owner is authoritative.
      const contributor =
        continuations.has(runId) || promotedMessages.has(message.id)
          ? undefined
          : yield* combine(event.threadId, authority, automation);
      const owner = yield* combine(event.threadId, saved[0]?.record_id, contributor);
      if (owner !== undefined)
        yield* sql`INSERT INTO peer_message_authority (message_id, record_id) VALUES (${message.id}, ${owner}) ON CONFLICT (message_id) DO UPDATE SET record_id = excluded.record_id`;
      yield* attach(runId, event.threadId, owner);
    }
    for (const effect of effects) {
      const request = effect.request;
      const runId =
        "runId" in request
          ? request.runId
          : "sourceRunId" in request
            ? request.sourceRunId
            : request.type === "provider-turn.steer"
              ? events.find(
                  (event) =>
                    event.type === "message.updated" && event.payload.id === request.messageId,
                )?.runId
              : undefined;
      const acceptedRun = runId == null ? undefined : yield* runOwner(runId);
      // Execution uses accepted input ownership, never a promoter's context.
      // Other operations keep their own authenticated effect authority too.
      const executesInput =
        request.type === "provider-turn.start" ||
        request.type === "provider-turn.steer" ||
        request.type === "provider-turn.restart" ||
        request.type === "provider-runtime.continue";
      const owner = executesInput
        ? acceptedRun
        : yield* combine(
            effect.threadId,
            acceptedRun,
            yield* combine(effect.threadId, authority, automation),
          );
      if (owner !== undefined) {
        yield* sql`INSERT INTO peer_effect_authority (effect_id, record_id) VALUES (${effect.id}, ${owner}) ON CONFLICT DO NOTHING`;
      }
    }
  },
);
