import {
  threadPullRequestKeyOf,
  threadPullRequestKeysEqual,
} from "@t3tools/shared/threadPullRequests";
import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2CheckpointJson,
  OrchestrationV2CheckpointScopeJson,
  OrchestrationV2ContextHandoffJson,
  OrchestrationV2ContextTransferJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2PlanArtifact,
  OrchestrationV2ProviderSessionJson,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2ProviderTurnJson,
  OrchestrationV2RunAttemptJson,
  OrchestrationV2RunJson,
  OrchestrationV2RuntimeRequestJson,
  OrchestrationV2RemoteTaskChild,
  OrchestrationV2SubagentJson,
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemJson,
  ThreadPullRequestKey,
  ThreadPullRequestLink,
  StoredPrismRole,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaAST from "effect/SchemaAST";
import * as Option from "effect/Option";
import { PersistenceSqlError, toPersistenceSqlError } from "./Errors.ts";
import type * as SqlClient from "effect/sql/SqlClient";

// Task-only rollback source: newer product work stays in storage until roll-forward.
const turnItemTypes: ReadonlySet<string> = new Set(
  OrchestrationV2TurnItem.members.map((member) => member.fields.type.literal),
);
export const knownTurnItemTypes = [...turnItemTypes];
const jsonObject = Schema.Record(Schema.String, Schema.Unknown);
const decodeObject = Schema.decodeUnknownSync(jsonObject);
const decodeObjectEffect = Schema.decodeUnknownEffect(jsonObject);
const decodeStoredPrismRole = Schema.decodeUnknownOption(StoredPrismRole);
const retiredSetting = (path: readonly string[], key: string) => {
  const role = path.at(-1) === "prismRoles" ? decodeStoredPrismRole(key) : Option.none();
  return Option.isSome(role) && role.value !== key;
};
const validatePullRequestLink = Schema.decodeUnknownEffect(ThreadPullRequestLink);
const decodePullRequestKey = Schema.decodeUnknownSync(ThreadPullRequestKey);
const parseJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeKnownRemote = Schema.encodeUnknownEffect(
  Schema.fromJsonString(Schema.toEncoded(OrchestrationV2RemoteTaskChild)),
);
const decodeTurnItem = Schema.decodeUnknownEffect(OrchestrationV2TurnItemJson);

export const isUnknownTurnItem = (value: unknown): boolean =>
  typeof value === "object" &&
  value !== null &&
  "type" in value &&
  typeof value.type === "string" &&
  value.type.trim().length > 0 &&
  !turnItemTypes.has(value.type);

// The enclosing event/projection operation owns tracing for these per-row codecs.
export const decodeReadableTurnItem = Effect.fnUntraced(function* (json: string) {
  const value = yield* parseJson(json);
  return isUnknownTurnItem(value) ? null : yield* decodeTurnItem(value);
});

const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const guards = new WeakMap<SchemaAST.AST, (value: unknown) => boolean>();
function accepts(ast: SchemaAST.AST, value: unknown): boolean {
  let guard = guards.get(ast);
  if (guard === undefined) {
    guard = Schema.is(Schema.make<Schema.Codec<unknown>>(ast));
    guards.set(ast, guard);
  }
  return guard(value);
}

function arrayIdentity(value: unknown, path: readonly string[]): string | undefined {
  if (!object(value)) return undefined;
  switch (path.at(-1)) {
    case "pullRequests":
      return threadPullRequestKeyOf(decodePullRequestKey(value));
    case "agents":
      return JSON.stringify([value.index, value.attempt ?? 0, value.agentId ?? null]);
    case "restartCancelledBackgroundWork":
      return JSON.stringify([value.kind, value.id ?? value.label]);
    case "options":
      return typeof value.label === "string" ? value.label : undefined;
    case "pendingBackgroundTasks":
      return JSON.stringify([value.taskId, value.kind]);
    default:
      return typeof value.id === "string" ? value.id : undefined;
  }
}

function sameOwner(
  stored: Record<string, unknown>,
  next: Record<string, unknown>,
  path: readonly string[],
) {
  for (const key of ["id", "type", "kind"]) {
    if (key in stored && key in next && stored[key] !== next[key]) return false;
  }
  if (path.at(-1) === "linkOrigin") return stored.sessionId === next.sessionId;
  if (path.at(-1) === "remoteChild") {
    return stored.environmentId === next.environmentId && stored.threadId === next.threadId;
  }
  if (path.at(-1) === "workflow") {
    const before = stored.runHandles;
    const after = next.runHandles;
    return !object(before) || !object(after) || before.runId === after.runId;
  }
  return true;
}

function retainWatch(
  stored: Record<string, unknown>,
  next: Record<string, unknown>,
  retained: Record<string, unknown>,
  at: unknown,
) {
  if (!object(next.watch))
    return object(stored.watch) ? { ...retained, endedWatch: stored.watch } : retained;
  const { endedWatch: _ended, ...active } = retained;
  const watch = decodeObject(active.watch);
  if (!object(stored.watch)) {
    if (!object(stored.endedWatch)) return active;
    const resumed = new Map(Object.entries(watch));
    for (const key of [
      "headSha",
      "failedChecks",
      "passed",
      "passedChecks",
      "conflicting",
      "owedChecks",
      "checksAwaitedSince",
    ]) {
      if (Object.hasOwn(stored.endedWatch, key)) resumed.set(key, stored.endedWatch[key]);
    }
    return { ...active, watch: Object.fromEntries(resumed) };
  }
  if (stored.watch.startedAt !== next.watch.startedAt) {
    const { owedChecks: _owed, checksAwaitedSince: _since, ...fresh } = watch;
    return { ...active, watch: fresh };
  }
  if (stored.watch.headSha !== next.watch.headSha) {
    const hasChecks =
      (Array.isArray(next.watch.failedChecks) && next.watch.failedChecks.length > 0) ||
      (Array.isArray(next.watch.passedChecks) && next.watch.passedChecks.length > 0) ||
      (object(next.snapshot) &&
        next.snapshot.checksState != null &&
        next.snapshot.checksState !== "none");
    return {
      ...active,
      watch: {
        ...watch,
        owedChecks: [],
        ...(Object.hasOwn(stored.watch, "checksAwaitedSince")
          ? { checksAwaitedSince: hasChecks ? null : at }
          : {}),
      },
    };
  }
  return active;
}

export function retainNewerFields(
  ast: SchemaAST.AST,
  stored: unknown,
  next: unknown,
  path: readonly string[] = [],
  at?: unknown,
): unknown {
  if (path.at(-1) === "nativeMetadata" && next == null && object(stored))
    return unknownFieldsOnly(ast, stored) ?? next;
  if (ast._tag === "Suspend") return retainNewerFields(ast.thunk(), stored, next, path, at);
  if (ast._tag === "Union") {
    const member = ast.types.find((type) => accepts(type, next));
    return member === undefined ? next : retainNewerFields(member, stored, next, path, at);
  }
  if (ast._tag === "Arrays" && Array.isArray(stored) && Array.isArray(next)) {
    return next.map((value, index) => {
      const itemAst = ast.elements[index] ?? ast.rest[0];
      if (itemAst === undefined) return value;
      const key = arrayIdentity(value, path);
      const candidates =
        key === undefined ? [] : stored.filter((old) => arrayIdentity(old, path) === key);
      if (candidates.length > 1)
        throw new PersistenceSqlError({
          operation: "rollback.array-identity",
          detail: "Ambiguous persisted array identity; write deferred until roll-forward",
        });
      return candidates.length !== 1
        ? value
        : retainNewerFields(itemAst, candidates[0], value, [...path, "[]"], at);
    });
  }
  if (ast._tag !== "Objects" || !object(stored) || !object(next) || !sameOwner(stored, next, path))
    return next;
  const properties = new Map(
    ast.propertySignatures.map((property) => [String(property.name), property.type]),
  );
  const result = new Map(Object.entries(next));
  for (const [key, value] of Object.entries(stored)) {
    if (retiredSetting(path, key)) continue;
    const child =
      properties.get(key) ??
      ast.indexSignatures.find((signature) => accepts(signature.parameter, key))?.type;
    if (child === undefined) {
      if (!Object.hasOwn(next, key)) result.set(key, value);
    } else if (Object.hasOwn(next, key)) {
      result.set(key, retainNewerFields(child, value, next[key], [...path, key], at));
    }
  }
  return path.at(-1) === "[]" && path.at(-2) === "pullRequests"
    ? retainWatch(stored, next, Object.fromEntries(result), at)
    : Object.fromEntries(result);
}

const entity = (table: string, key: string, schema: Schema.Top) => {
  const ast = SchemaAST.toEncoded(schema.ast);
  return {
    table: `orchestration_v2_projection_${table}`,
    key,
    ast,
    decode: Schema.decodeUnknownEffect(Schema.make<Schema.Codec<unknown>>(ast)),
  };
};
const entities = {
  thread: entity("threads", "thread_id", OrchestrationV2AppThreadJson),
  run: entity("runs", "run_id", OrchestrationV2RunJson),
  "run-attempt": entity("run_attempts", "attempt_id", OrchestrationV2RunAttemptJson),
  node: entity("nodes", "node_id", OrchestrationV2ExecutionNodeJson),
  subagent: entity("subagents", "subagent_id", OrchestrationV2SubagentJson),
  "provider-session": entity(
    "provider_sessions",
    "provider_session_id",
    OrchestrationV2ProviderSessionJson,
  ),
  "provider-thread": entity(
    "provider_threads",
    "provider_thread_id",
    OrchestrationV2ProviderThreadJson,
  ),
  "provider-turn": entity("provider_turns", "provider_turn_id", OrchestrationV2ProviderTurnJson),
  "runtime-request": entity(
    "runtime_requests",
    "runtime_request_id",
    OrchestrationV2RuntimeRequestJson,
  ),
  message: entity("messages", "message_id", OrchestrationV2ConversationMessageJson),
  plan: entity("plans", "plan_id", OrchestrationV2PlanArtifact),
  "turn-item": entity("turn_items", "turn_item_id", OrchestrationV2TurnItemJson),
  "checkpoint-scope": entity("checkpoint_scopes", "scope_id", OrchestrationV2CheckpointScopeJson),
  checkpoint: entity("checkpoints", "checkpoint_id", OrchestrationV2CheckpointJson),
  "context-handoff": entity(
    "context_handoffs",
    "context_handoff_id",
    OrchestrationV2ContextHandoffJson,
  ),
  "context-transfer": entity(
    "context_transfers",
    "context_transfer_id",
    OrchestrationV2ContextTransferJson,
  ),
};
type Entity = (typeof entities)[keyof typeof entities];
const entitiesByName: ReadonlyMap<string, Entity> = new Map(Object.entries(entities));
function eventEntity(type: string): Entity | undefined {
  return entitiesByName.get(type.split(".")[0] ?? "");
}

const retainValue = (
  ast: SchemaAST.AST,
  stored: unknown,
  next: unknown,
  path: readonly string[] = [],
  at?: unknown,
) =>
  Effect.try({
    try: () => retainNewerFields(ast, stored, next, path, at),
    catch: toPersistenceSqlError("rollback.retention"),
  });

export const makeRetainedPayloads = (sql: SqlClient.SqlClient) => {
  const batch = new Map<string, unknown>();
  const read = Effect.fnUntraced(function* (owner: Entity, id: string) {
    const key = `${owner.table}:${id}`;
    if (batch.has(key)) return batch.get(key);
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM ${sql(owner.table)} WHERE ${sql(owner.key)} = ${id}`;
    const value = rows[0] === undefined ? undefined : yield* parseJson(rows[0].payload_json);
    batch.set(key, value);
    return value;
  });
  return Effect.fnUntraced(function* (
    type: string,
    encoded: string,
    context: {
      readonly threadId?: string;
      readonly eventId?: string;
      readonly occurredAt?: string;
    } = {},
  ) {
    const { threadId, eventId } = context;
    const owner = eventEntity(type);
    const next = yield* decodeObjectEffect(yield* parseJson(encoded));
    if (owner === undefined) return encoded;
    const id =
      type === "thread.pull-request-link-updated"
        ? threadId
        : typeof next.id === "string"
          ? next.id
          : typeof next.runId === "string"
            ? next.runId
            : undefined;
    if (id === undefined) return encoded;
    const stored = yield* read(owner, id);
    if (type === "turn-item.updated" && isUnknownTurnItem(stored)) {
      return yield* Effect.fail(
        new PersistenceSqlError({
          operation: "rollback.write-fence",
          detail: "Rollback cannot overwrite a newer turn item",
        }),
      );
    }
    // Retention examines raw prior facts; unrelated obsolete known fields may be replaced.
    if (typeof next.id === "string") yield* owner.decode(next);
    if (
      object(stored) &&
      object(stored.remoteChild) &&
      Object.hasOwn(stored.remoteChild, "stopCount")
    ) {
      if (
        !object(next.remoteChild) ||
        (yield* encodeKnownRemote(stored.remoteChild)) !==
          (yield* encodeKnownRemote(next.remoteChild))
      )
        return yield* Effect.fail(
          new PersistenceSqlError({
            operation: "rollback.remote-stop-fence",
            detail: "Newer remote stop correlation is deferred until roll-forward",
          }),
        );
    }
    // Rebuild has no previous projection carrying fields introduced by a partial event.
    const durableRows =
      eventId === undefined
        ? []
        : yield* sql<{ event_type: string; payload_json: string }>`
      SELECT event_type, payload_json FROM orchestration_events WHERE event_id = ${eventId}
        AND (event_type = ${type} OR (${type === "thread.pull-request-synced" ? 1 : 0} = 1 AND event_type = 'thread.pull-request-link-updated'))
        AND stream_id = ${threadId ?? ""} AND application_event_version = 2`;
    const durable =
      durableRows[0] === undefined ? undefined : yield* parseJson(durableRows[0].payload_json);
    const expandedPartial =
      durableRows[0]?.event_type === "thread.pull-request-link-updated" &&
      type === "thread.pull-request-synced";
    let baseline = durable ?? stored;
    if (expandedPartial) {
      yield* validatePullRequestLink(durable);
      if (!object(stored) || !Array.isArray(next.pullRequests))
        return yield* new PersistenceSqlError({
          operation: "rollback.partial-projection",
          detail: "Missing thread for a partial link projection",
        });
      const key = decodePullRequestKey(durable);
      const previous = Array.isArray(stored.pullRequests) ? stored.pullRequests : [];
      baseline = {
        ...stored,
        pullRequests: next.pullRequests.map((link) => {
          const nextKey = decodePullRequestKey(link);
          if (threadPullRequestKeysEqual(nextKey, key)) return durable;
          return (
            previous.find((candidate) =>
              threadPullRequestKeysEqual(decodePullRequestKey(candidate), nextKey),
            ) ?? link
          );
        }),
      };
    } else if (
      durable !== undefined &&
      type !== "run.background-work-cancelled" &&
      type !== "thread.pull-request-link-updated"
    ) {
      yield* owner.decode(durable);
    }
    if (
      type === "thread.pull-request-link-updated" &&
      object(stored) &&
      Array.isArray(stored.pullRequests)
    ) {
      const key = decodePullRequestKey(next);
      const links = stored.pullRequests.map((link) => {
        const existing = decodePullRequestKey(link);
        return threadPullRequestKeysEqual(existing, key) ? next : link;
      });
      const updated = decodeObject(
        yield* retainValue(
          owner.ast,
          stored,
          { ...stored, pullRequests: links },
          [],
          context.occurredAt ?? stored.updatedAt,
        ),
      );
      batch.set(`${owner.table}:${id}`, updated);
      const retainedLinks = updated.pullRequests;
      if (Array.isArray(retainedLinks)) {
        const retained = retainedLinks.find((link) => {
          const existing = decodePullRequestKey(link);
          return threadPullRequestKeysEqual(existing, key);
        });
        return retained === undefined ? encoded : yield* encodeJson(retained);
      }
      return encoded;
    }
    if (type === "run.background-work-cancelled") {
      if (!object(stored)) return encoded;
      let work = next.restartCancelledBackgroundWork;
      if (object(durable)) {
        work = yield* retainValue(
          SchemaAST.toEncoded(OrchestrationV2RunJson.fields.restartCancelledBackgroundWork.ast),
          durable.restartCancelledBackgroundWork,
          work,
        );
      } else if (Array.isArray(work) && typeof stored.providerThreadId === "string") {
        const rosterRows = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${stored.providerThreadId}`;
        const roster =
          rosterRows[0] === undefined ? undefined : yield* parseJson(rosterRows[0].payload_json);
        if (object(roster) && Array.isArray(roster.pendingBackgroundTasks)) {
          const tasks = roster.pendingBackgroundTasks;
          work = work.map((entry) =>
            !object(entry) ||
            !tasks.some(
              (task) =>
                object(task) &&
                task.awaited === true &&
                (task.parentTaskId ?? task.taskId) === entry.id &&
                (task.parentTaskId !== undefined
                  ? "subagent"
                  : task.kind === "command"
                    ? "shell"
                    : task.kind === "background_task"
                      ? "task"
                      : task.kind) === entry.kind,
            )
              ? entry
              : { ...entry, awaited: true },
          );
        }
      }
      const updated = decodeObject(
        yield* retainValue(
          owner.ast,
          stored,
          { ...stored, restartCancelledBackgroundWork: work },
          [],
          context.occurredAt ?? next.updatedAt,
        ),
      );
      batch.set(`${owner.table}:${id}`, updated);
      return yield* encodeJson({
        ...next,
        restartCancelledBackgroundWork: updated.restartCancelledBackgroundWork,
      });
    }
    if (typeof next.id !== "string") return encoded;
    // Resume also restores known PR fields; project the committed link state after old decoding.
    const incoming =
      durable !== undefined &&
      type.startsWith("thread.") &&
      object(baseline) &&
      Array.isArray(baseline.pullRequests)
        ? { ...next, pullRequests: baseline.pullRequests }
        : next;
    let retained = decodeObject(
      yield* retainValue(owner.ast, baseline, incoming, [], context.occurredAt ?? next.updatedAt),
    );
    if (type.startsWith("thread.") && (next.archivedAt != null || next.deletedAt != null))
      retained = { ...retained, guestShare: null };
    batch.set(`${owner.table}:${id}`, retained);
    return yield* encodeJson(retained);
  });
};

function unknownFieldsOnly(
  ast: SchemaAST.AST,
  stored: unknown,
  path: readonly string[] = [],
): unknown {
  if (ast._tag === "Suspend") return unknownFieldsOnly(ast.thunk(), stored, path);
  if (ast._tag === "Union") {
    const member = ast.types.find((type) => accepts(type, stored));
    return member === undefined ? undefined : unknownFieldsOnly(member, stored, path);
  }
  if (ast._tag !== "Objects" || !object(stored)) return undefined;
  const result = new Map<string, unknown>();
  for (const [key, value] of Object.entries(stored)) {
    if (retiredSetting(path, key)) continue;
    const child =
      ast.propertySignatures.find((property) => property.name === key)?.type ??
      ast.indexSignatures.find((signature) => accepts(signature.parameter, key))?.type;
    const retained = child === undefined ? value : unknownFieldsOnly(child, value, [...path, key]);
    if (retained !== undefined) result.set(key, retained);
  }
  return result.size === 0 ? undefined : Object.fromEntries(result);
}

export function retainNewerSettings(ast: SchemaAST.AST, stored: unknown, next: unknown): unknown {
  const retained = retainNewerFields(ast, stored, next);
  if (!object(retained)) return retained;
  const unknownOnly = unknownFieldsOnly(ast, stored);
  if (!object(unknownOnly)) return retained;
  const fill = (
    previous: Record<string, unknown>,
    current: Record<string, unknown>,
  ): Record<string, unknown> => {
    const result = new Map(Object.entries(current));
    for (const [key, value] of Object.entries(previous)) {
      if (!Object.hasOwn(current, key)) result.set(key, value);
      else if (object(value) && object(current[key])) result.set(key, fill(value, current[key]));
    }
    return Object.fromEntries(result);
  };
  return fill(unknownOnly, retained);
}
