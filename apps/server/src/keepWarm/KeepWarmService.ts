import {
  CommandId,
  KEEP_WARM_PING_TEXT,
  keepWarmPingMessageId,
  MessageId,
  supportsKeepWarm,
  type KeepWarmMode,
  type KeepWarmStopReason,
  type ServerSettingsError,
  type ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { isThreadRetired } from "../childThreads/retirement.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import { readWightThread } from "../wight/admission.ts";
import { makeKeepWarm, type KeepWarmThread } from "./keepWarm.ts";

export class KeepWarmUnsupportedError extends Schema.TaggedError<KeepWarmUnsupportedError>()(
  "KeepWarmUnsupportedError",
  { threadId: Schema.String, driver: Schema.NullOr(Schema.String) },
) {
  override get message(): string {
    return "Keep cache warm works only for Claude threads.";
  }
}

type KeepWarmStatus = {
  readonly enabled: boolean;
  readonly stoppedReason?: KeepWarmStopReason;
};

/** A reference so tool fixtures without the runtime read every thread as off. */
export class KeepWarm extends Context.Reference<{
  readonly reconcile: Effect.Effect<void>;
  readonly read: (threadId: ThreadId) => Effect.Effect<KeepWarmStatus, ServerSettingsError>;
  /** Turns keep-warm on (replacing Wight) or off for one thread. */
  readonly set: (
    threadId: ThreadId,
    enabled: boolean,
  ) => Effect.Effect<
    void,
    KeepWarmUnsupportedError | ServerSettingsError | ProjectionStore.ProjectionStoreV2Error
  >;
}>("t3/keepWarm/KeepWarmService/KeepWarm", {
  defaultValue: () => ({
    reconcile: Effect.void,
    read: () => Effect.succeed({ enabled: false }),
    set: () => Effect.die("The keep-warm runtime is not running."),
  }),
}) {}

const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const isoOrNull = (value: DateTime.Utc | null | undefined) =>
  value == null ? null : DateTime.formatIso(value);

const make = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;

  const readThread = Effect.fn("KeepWarm.readThread")(function* (id: ThreadId) {
    const thread = yield* readWightThread(id).pipe(
      Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    if (thread === undefined) return undefined;
    // Runs the shell hides (pings, idle compactions) still refresh the cache, so read them all.
    const [latest] = yield* sql<{
      readonly requestAt: string | null;
      readonly pingFailureClass: string | null;
    }>`
      SELECT
        coalesce(json_extract(r.payload_json, '$.startedAt'), r.requested_at) AS "requestAt",
        CASE WHEN r.status = 'failed'
          AND json_extract(r.payload_json, '$.userMessageId') LIKE 'keep-warm:%'
        THEN (
          SELECT json_extract(i.payload_json, '$.failure.class')
          FROM orchestration_v2_projection_turn_items i
          WHERE i.run_id = r.run_id AND i.type = 'error'
          ORDER BY i.updated_at DESC LIMIT 1
        ) END AS "pingFailureClass"
      FROM orchestration_v2_projection_runs r
      WHERE r.thread_id = ${id}
        AND r.status <> 'queued'
        AND NOT (r.status = 'cancelled' AND json_extract(r.payload_json, '$.startedAt') IS NULL)
      ORDER BY r.ordinal DESC
      LIMIT 1
    `;
    return {
      ...thread,
      latestRunAt: latest?.requestAt ?? null,
      latestUserMessageAt: isoOrNull(thread.latestUserAuthoredMessageAt),
      latestPingHitUsageLimit: latest?.pingFailureClass === "usage_limit",
    } satisfies KeepWarmThread;
  });

  /** When the ping's run asked the provider, for clients that see no pings in the shell. */
  const pingRequestAt = Effect.fn("KeepWarm.pingRequestAt")(function* (
    id: ThreadId,
    messageId: string,
  ) {
    const [run] = yield* sql<{ readonly requestAt: string | null }>`
      SELECT coalesce(json_extract(payload_json, '$.startedAt'), requested_at) AS "requestAt"
      FROM orchestration_v2_projection_runs
      WHERE thread_id = ${id} AND json_extract(payload_json, '$.userMessageId') = ${messageId}
      LIMIT 1
    `;
    return run?.requestAt ?? null;
  });

  /**
   * Applied under the settings write lock, only while the entry is still the activation read, so
   * a ping or stop never undoes the user's choice. Only this serialized loop changes an entry.
   */
  const replaceCurrent = Effect.fn("KeepWarm.replaceCurrent")(function* (
    id: ThreadId,
    mode: KeepWarmMode,
    next: KeepWarmMode | null,
  ) {
    yield* settings.updateSettings({
      threadIdleModesIfCurrent: { [id]: { enabledAt: mode.enabledAt, next } },
    });
  });

  const runtime = yield* makeKeepWarm<
    ServerSettingsError | ProjectionStore.ProjectionStoreV2Error | SqlError,
    never
  >({
    settings: settings.getSettings,
    providers: registry.getProviders,
    retired: (id) =>
      isThreadRetired(id).pipe(
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
      ),
    thread: readThread,
    end: Effect.fn("KeepWarm.end")(function* (id, mode, reason) {
      const at = iso(yield* Clock.currentTimeMillis);
      yield* replaceCurrent(
        id,
        mode,
        reason === null ? null : { ...mode, stopped: { at, reason } },
      );
    }),
    ping: Effect.fn("KeepWarm.ping")(function* (thread, mode, dueMs) {
      const messageId = keepWarmPingMessageId(thread.id, mode, dueMs);
      const sent = yield* threads
        .dispatch({
          type: "message.dispatch",
          threadId: thread.id,
          commandId: CommandId.make(`server:${messageId}`),
          messageId: MessageId.make(messageId),
          text: KEEP_WARM_PING_TEXT,
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "agent",
          creationSource: "server",
          idleAdmission: {
            mode: "keepWarm",
            latestRunId: thread.latestRunId,
            updatedAt: DateTime.toEpochMillis(thread.updatedAt),
            providerInstanceId: thread.providerInstanceId,
            enabledAt: mode.enabledAt,
          },
        })
        .pipe(
          Effect.as(true),
          Effect.catchCause((cause) =>
            Effect.logDebug("Keep-warm ping refused", { threadId: thread.id, cause }).pipe(
              Effect.as(false),
            ),
          ),
        );
      if (!sent) return;
      const lastPingAt =
        (yield* pingRequestAt(thread.id, messageId)) ?? iso(yield* Clock.currentTimeMillis);
      yield* replaceCurrent(thread.id, mode, { ...mode, lastPingAt });
    }),
  });
  const reconcile = runtime
    .reconcile()
    .pipe(
      Effect.catchCause((cause) => Effect.logWarning("Keep-warm reconciliation failed", { cause })),
    );

  const set = Effect.fn("KeepWarm.set")(function* (threadId: ThreadId, enabled: boolean) {
    const current = (yield* settings.getSettings).threadIdleModes[threadId];
    if (!enabled) {
      if (current?.type === "keepWarm")
        yield* settings.updateSettings({ threadIdleModes: { [threadId]: null } });
      return;
    }
    if (current?.type === "keepWarm" && current.stopped === null) return;
    const shell = yield* projections.getThreadShell(threadId);
    const provider = (yield* registry.getProviders).find(
      (entry) => entry.instanceId === shell?.providerInstanceId,
    );
    if (!supportsKeepWarm(provider?.driver))
      return yield* new KeepWarmUnsupportedError({
        threadId,
        driver: provider?.driver ?? null,
      });
    const enabledAt = iso(yield* Clock.currentTimeMillis);
    yield* settings.updateSettings({
      threadIdleModes: {
        [threadId]: { type: "keepWarm", enabledAt, lastPingAt: null, stopped: null },
      },
    });
  });

  const read = Effect.fn("KeepWarm.read")(function* (
    threadId: ThreadId,
  ): Effect.fn.Return<KeepWarmStatus, ServerSettingsError> {
    const mode = (yield* settings.getSettings).threadIdleModes[threadId];
    if (mode?.type !== "keepWarm") return { enabled: false };
    return mode.stopped === null
      ? { enabled: true }
      : { enabled: false, stoppedReason: mode.stopped.reason };
  });

  const changes = yield* settings.subscribeChanges;
  yield* forkParked(
    Stream.mergeAll(
      [
        changes.pipe(Stream.map(() => undefined)),
        registry.streamChanges.pipe(Stream.map(() => undefined)),
        threads.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" ||
              event.type === "run.created" ||
              event.type === "thread.archived" ||
              event.type === "thread.deleted" ||
              event.type === "thread.settled" ||
              event.type === "thread.provider-switched" ||
              event.type === "runtime-request.updated",
          ),
          Stream.map(() => undefined),
        ),
      ],
      { concurrency: "unbounded" },
    ).pipe(Stream.runForEach(() => reconcile)),
  );
  yield* forkParked(reconcile);
  return { reconcile, read, set };
});

export const layer = Layer.effect(KeepWarm, make);
