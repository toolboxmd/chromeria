import * as PeerLinkEvents from "../peer/PeerLinkEvents.ts";
import { checkAutomationCommit } from "../scheduledTasks/DestinationAutomationAuthority.ts";
import {
  PeerDispatchAuthority,
  PeerAcceptedAutomation,
  NativeAuthorityValidation,
  nativeAuthorityOwners,
} from "../peer/peerEffectAuthority.ts";
import {
  initializePeerEffectAuthority,
  recordPeerEffectAuthority,
} from "../peer/peerEffectAuthority.ts";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";
import {
  CommandId,
  EventId,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type TurnItemId,
  OrchestrationV2RunJson,
  OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  OrchestrationV2StoredEvent,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  NodeId,
  type ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Semaphore from "effect/Semaphore";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import { readStopDescendants } from "../childThreads/stopDescendants.ts";
import type { RestartCutReason } from "@t3tools/shared/restartCut";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";

import { replayAndBufferProjectedLiveEvents } from "./LiveStreamBudget.ts";
import type { UnsequencedProjectEvent } from "../persistence/OrchestrationEventStore.ts";
import { projectDomainEventForWire } from "./WireProjection.ts";
import { makeRunOutcomeRecorder, type RunOutcome } from "./RunMetrics.ts";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { forkCommitSequence, validateForkStateOnlyCommand } from "../fork/commitSequence.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

const isForkCommitGuardRejected = Schema.is(ForkCommitGuardRejected);

/**
 * ERRORS
 */
export class EventSinkWriteError extends Schema.TaggedError<EventSinkWriteError>()(
  "EventSinkWriteError",
  {
    eventCount: Schema.Number,
    commandId: Schema.optional(CommandId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to write ${this.eventCount} orchestration V2 event(s).`;
  }
}

export class EventSinkStreamError extends Schema.TaggedError<EventSinkStreamError>()(
  "EventSinkStreamError",
  {
    threadId: Schema.optional(ThreadId),
    afterSequence: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? "Failed to stream orchestration V2 events."
      : `Failed to stream orchestration V2 events for thread ${this.threadId}.`;
  }
}

export const EventSinkV2Error = Schema.Union([EventSinkWriteError, EventSinkStreamError]);
export type EventSinkV2Error = typeof EventSinkV2Error.Type;

export interface EventSinkV2Shape {
  readonly write: (input: {
    /** Return no events if another thread owns this native session at commit time. */
    readonly unlessNativeThreadOwned?: Pick<
      OrchestrationV2ProviderThread,
      "driver" | "providerInstanceId" | "appThreadId" | "nativeThreadRef"
    >;
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeWithEffects: (input: {
    /**
     * Rebuilds each run.updated from the stored row with only its delegatedCompletion replaced.
     * The batch must not also change that run's lifecycle.
     */
    readonly updateDelegatedCompletionOnly?: boolean;
    readonly unlessNativeThreadOwned?: Pick<
      OrchestrationV2ProviderThread,
      "driver" | "providerInstanceId" | "appThreadId" | "nativeThreadRef"
    >;
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, EventSinkV2Error>;
  readonly writeIfRunCurrent: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedStatus: OrchestrationV2Run["status"];
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects?: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the expected attempt and ordinal still own the provider thread, protecting newer attempts. */
  readonly writeIfProviderThreadOwner: (input: {
    readonly guardPendingUserInputCancellations?: boolean;
    readonly commandId?: CommandId;
    readonly providerThreadId: ProviderThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedLastRunOrdinal: number;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  /** Commits only if the full provider row equals expected; some writers do not advance updatedAt. */
  readonly writeIfProviderThreadUnchanged: (input: {
    readonly expected: OrchestrationV2ProviderThread;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  }) => Effect.Effect<
    {
      readonly committed: boolean;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    },
    EventSinkV2Error
  >;
  readonly commitCommand: (input: {
    readonly forkPlans?: ReadonlyArray<ForkCommitPlan>;
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
    readonly restartCutDecline?: {
      readonly runId: RunId;
      readonly eventId: EventId;
      readonly reason: RestartCutReason;
    };
    readonly cancelUnsettledEffects?: {
      readonly includeSubagentDescendants?: boolean;
      readonly drain?: boolean;
      readonly effectTypes: ReadonlyArray<EffectOutbox.OrchestrationEffectRequestV2["type"]>;
      readonly reason: string;
    };
    /** Like writeWithEffects' updateDelegatedCompletionOnly, for this run's run.updated only. */
    readonly updateDelegatedCompletionOnlyRunId?: RunId;
  }) => Effect.Effect<
    {
      readonly receipt: CommandReceiptStore.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      readonly committed: boolean;
      readonly cancelledEffectCount: number;
    },
    EventSinkV2Error | ForkCommitGuardRejected
  >;
  readonly commitRejectedCommand: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.CommandReceiptV2, EventSinkV2Error>;
  readonly commitProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
    readonly event: UnsequencedProjectEvent;
  }) => Effect.Effect<
    { readonly receipt: CommandReceiptStore.ProjectCommandReceiptV2; readonly committed: boolean },
    EventSinkV2Error
  >;
  readonly commitRejectedProjectCommand: (input: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
    readonly rejectedAt: DateTime.Utc;
    readonly error: string;
  }) => Effect.Effect<CommandReceiptStore.ProjectCommandReceiptV2, EventSinkV2Error>;
  readonly stream: (input: {
    /** Required. `0` is a deliberate full replay of the matched range. */
    readonly afterSequence: number;
    readonly threadId?: ThreadId;
    /** Filter before queuing live events so a busy worker retains only the events it handles. */
    readonly eventType?: OrchestrationV2DomainEvent["type"];
    /** Bound RPC subscribers; internal workers must not drop their subscription under load. */
    readonly bounded?: boolean;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
  readonly latestSequence: (input?: {
    readonly threadId?: ThreadId;
  }) => Effect.Effect<number, EventSinkV2Error>;
  readonly readByCommandId: (input: {
    readonly commandId: CommandId;
  }) => Stream.Stream<OrchestrationV2StoredEvent, EventSinkV2Error>;
}

export class EventSinkV2 extends Context.Service<EventSinkV2, EventSinkV2Shape>()(
  "t3/orchestration-v2/EventSink/EventSinkV2",
) {}

const layerBase: Layer.Layer<
  EventSinkV2,
  never,
  | CommandReceiptStore.CommandReceiptStoreV2
  | EffectOutbox.EffectOutboxV2
  | EventStore.EventStoreV2
  | ProjectionStore.ProjectionStoreV2
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | TurnItemPositionStore.TurnItemPositionStoreV2
> = Layer.effect(
  EventSinkV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const nativeAuthority = yield* Effect.serviceOption(NativeAuthorityValidation);
    const peerLinkEvents = yield* Effect.serviceOption(PeerLinkEvents.PeerLinkEvents);
    yield* initializePeerEffectAuthority.pipe(Effect.orDie);
    const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const liveEvents = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
    const liveEventsByType = new Map<
      OrchestrationV2DomainEvent["type"],
      PubSub.PubSub<OrchestrationV2StoredEvent>
    >();
    const publishLiveEvents =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (events: ReadonlyArray<OrchestrationV2StoredEvent>) {
        yield* PubSub.publishAll(liveEvents, events);
        for (const [type, pubsub] of liveEventsByType) {
          yield* PubSub.publishAll(
            pubsub,
            events.filter((stored) => stored.event.type === type),
          );
        }
      });
    const runOutcomes = makeRunOutcomeRecorder((threadId) =>
      // One payload row; unlike the shell, it still resolves a deleted child thread.
      projectionStore
        .getThread(threadId)
        .pipe(Effect.map((thread) => thread.lineage.relationshipToParent === "subagent")),
    );
    // Recorded before publication, so an interrupted publish cannot lose a committed outcome.
    const publishStoredEvents = (
      events: ReadonlyArray<OrchestrationV2StoredEvent>,
      outcomes: ReadonlyArray<RunOutcome>,
    ) =>
      runOutcomes
        .record(outcomes)
        .pipe(
          Effect.andThen(eventStore.publishCommitted(events)),
          Effect.andThen(publishLiveEvents(events)),
        );

    // Serialize commit and publish; reordered publication makes clients drop earlier sequences.
    const publishLane = yield* Semaphore.make(1);
    const commitThenPublish = <A, E, R>(
      transaction: Effect.Effect<A, E, R>,
      publish: (committed: A) => Effect.Effect<void>,
    ) =>
      Effect.uninterruptibleMask((restore) => {
        let holdsLane = false;
        const takeLane = publishLane.take(1).pipe(
          Effect.andThen(
            Effect.sync(() => {
              holdsLane = true;
            }),
          ),
          Effect.uninterruptible,
        );
        // Cancellation may roll back the transaction's work. Once it commits,
        // every subscriber must hear about it before this writer can exit.
        // Otherwise a failed child run can leave its parent's task running.
        return sql
          .withTransaction(Effect.tap(restore(transaction), () => takeLane))
          .pipe(
            Effect.tap(publish),
            Effect.ensuring(
              Effect.suspend(() => (holdsLane ? publishLane.release(1) : Effect.void)),
            ),
          );
      });

    // A user can answer after terminal normalization reads the pending request.
    // Recheck inside the write transaction so stale cleanup cannot erase answers.
    const guardUserInputCancellations =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (events: ReadonlyArray<OrchestrationV2DomainEvent>) {
        const staleRequests = new Set<RuntimeRequestId>();
        const staleNodes = new Set<NodeId>();
        for (const event of events) {
          if (
            event.type !== "runtime-request.updated" ||
            event.payload.kind !== "user_input" ||
            event.payload.status !== "cancelled"
          )
            continue;
          const current = yield* projectionStore.getRuntimeRequest(
            event.threadId,
            event.payload.id,
          );
          if (
            current?.status !== "pending" ||
            current.kind !== "user_input" ||
            current.providerTurnId !== event.payload.providerTurnId ||
            current.responseCapability.type === "message"
          ) {
            staleRequests.add(event.payload.id);
            staleNodes.add(event.payload.nodeId);
          }
        }
        return events.filter((event) => {
          switch (event.type) {
            case "runtime-request.updated":
              return event.payload.status !== "cancelled" || !staleRequests.has(event.payload.id);
            case "node.updated":
              return event.payload.status !== "cancelled" || !staleNodes.has(event.payload.id);
            case "turn-item.updated":
              return (
                event.payload.type !== "user_input_request" ||
                event.payload.status !== "cancelled" ||
                !staleRequests.has(event.payload.requestId)
              );
            default:
              return true;
          }
        });
      });

    const normalizeEvents = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      const runOrdinals = new Map(
        events.flatMap((event) =>
          event.type === "run.created" || event.type === "run.updated"
            ? [[event.payload.id, event.payload.ordinal] as const]
            : [],
        ),
      );
      return Effect.gen(function* () {
        const normalized: Array<OrchestrationV2DomainEvent> = [];
        const pendingItems = new Map<TurnItemId, OrchestrationV2TurnItem>();
        for (const event of events) {
          if (event.type !== "turn-item.updated") {
            normalized.push(event);
            continue;
          }
          const payload = yield* turnItemPositions.normalize(
            event.payload,
            event.payload.runId === null ? undefined : runOrdinals.get(event.payload.runId),
          );
          // Compare after position allocation, inside the write transaction.
          // Independent deliveries and commands can repeat a stored snapshot.
          const previous =
            pendingItems.get(payload.id) ??
            (yield* projectionStore.getTurnItem({
              threadId: payload.threadId,
              itemId: payload.id,
            }));
          if (previous !== null && previous !== undefined) {
            const unchanged =
              (yield* ProjectionStore.encodeTurnItemPayload(previous)) ===
              (yield* ProjectionStore.encodeTurnItemPayload(payload));
            if (unchanged) continue;
          }
          pendingItems.set(payload.id, payload);
          normalized.push({ ...event, payload });
        }
        return normalized;
      });
    };

    const applyStoredEvents =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) {
        yield* Effect.forEach(storedEvents, (stored) => projectionStore.apply(stored.event), {
          concurrency: 1,
        });
        const sequence = storedEvents.at(-1)?.sequence;
        if (sequence !== undefined) {
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${sequence},
              ${now}
            )
            ON CONFLICT(projection_name)
            DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }
        // Read lineage here, on the connection this transaction holds; see makeRunOutcomeRecorder.
        return yield* runOutcomes.prepare(storedEvents);
      });

    // The enclosing write or commit span already traces this per-event projection merge.
    const updateDelegatedCompletionOnly = Effect.fnUntraced(function* (
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      includesRun: (runId: RunId) => boolean,
    ) {
      return yield* Effect.forEach(events, (event) =>
        Effect.gen(function* () {
          if (event.type !== "run.updated" || !includesRun(event.payload.id)) return event;
          const { runs } = yield* projectionStore.getThreadRecords(event.threadId, ["runs"], {
            runIds: [event.payload.id],
          });
          const current = runs[0];
          return current === undefined
            ? event
            : {
                ...event,
                payload: { ...current, delegatedCompletion: event.payload.delegatedCompletion },
              };
        }),
      );
    });

    const writeEffect = Effect.fn("orchestrationV2.EventSink.write")(function* (
      input: Parameters<EventSinkV2Shape["writeWithEffects"]>[0],
    ) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.thread_id": input.events[0]?.threadId ?? null,
      });

      const { committed } = yield* commitThenPublish(
        Effect.gen(function* () {
          // Check ownership in the same transaction that claims the native session.
          const imported = input.unlessNativeThreadOwned;
          if (imported?.nativeThreadRef != null) {
            const owners = yield* sql`
              SELECT provider_thread_id
              FROM orchestration_v2_projection_provider_threads
              WHERE driver = ${imported.driver}
                AND provider_instance_id = ${imported.providerInstanceId}
                AND thread_id IS NOT ${imported.appThreadId}
                AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${imported.nativeThreadRef.nativeId}
              LIMIT 1
            `;
            if (owners.length > 0) return { committed: [], outcomes: [] };
          }
          // Checkpoint capture can finish after a cohort writer reads its parent run.
          // Merge in this transaction so the event log and projection keep that completion.
          const events = input.updateDelegatedCompletionOnly
            ? yield* updateDelegatedCompletionOnly(input.events, () => true)
            : input.events;
          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(events)
              : events,
          );
          const committed = yield* eventStore.append({
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            events: normalized,
          });
          const outcomes = yield* applyStoredEvents(committed);
          yield* recordPeerEffectAuthority(input.events, input.effects).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          yield* effectOutbox.enqueue(input.effects);
          return { committed, outcomes };
        }).pipe(Effect.withTracerEnabled(false)),
        ({ committed, outcomes }) =>
          Effect.gen(function* () {
            if (input.effects.length > 0) {
              yield* effectOutbox.notifyAvailable(input.effects.length);
            }
            yield* publishStoredEvents(committed, outcomes);
          }),
      );
      return committed;
    });

    const writeIfRunCurrentEffect = Effect.fn("orchestrationV2.EventSink.writeIfRunCurrent")(
      function* (input: Parameters<EventSinkV2Shape["writeIfRunCurrent"]>[0]) {
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.command_id": input.commandId ?? null,
          "orchestration_v2.event_count": input.events.length,
          "orchestration_v2.run_id": input.runId,
          "orchestration_v2.thread_id": input.threadId,
        });

        return yield* commitThenPublish(
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly status: string;
              readonly active_attempt_id: string | null;
            }>`
            SELECT
              status,
              json_extract(payload_json, '$.activeAttemptId') AS active_attempt_id
            FROM orchestration_v2_projection_runs
            WHERE run_id = ${input.runId}
              AND thread_id = ${input.threadId}
            LIMIT 1
          `;
            const current = rows[0];
            if (
              current === undefined ||
              current.status !== input.expectedStatus ||
              current.active_attempt_id !== input.activeAttemptId
            ) {
              return {
                committed: false as const,
                storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
              };
            }

            const normalized = yield* normalizeEvents(
              input.guardPendingUserInputCancellations === true
                ? yield* guardUserInputCancellations(input.events)
                : input.events,
            );
            const storedEvents = yield* eventStore.append({
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              events: normalized,
            });
            const outcomes = yield* applyStoredEvents(storedEvents);
            yield* recordPeerEffectAuthority(input.events, input.effects ?? []).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            yield* effectOutbox.enqueue(input.effects ?? []);
            return { committed: true as const, storedEvents, outcomes };
          }),
          (result) =>
            Effect.gen(function* () {
              if (!result.committed) return;
              if (input.effects !== undefined && input.effects.length > 0) {
                yield* effectOutbox.notifyAvailable(input.effects.length);
              }
              yield* publishStoredEvents(result.storedEvents, result.outcomes);
            }),
        );
      },
    );

    const writeIfProviderThreadOwnerEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadOwner",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadOwner"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.command_id": input.commandId ?? null,
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.providerThreadId,
        "orchestration_v2.run_id": input.runId,
        "orchestration_v2.active_attempt_id": input.activeAttemptId,
        "orchestration_v2.expected_last_run_ordinal": input.expectedLastRunOrdinal,
      });

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* sql<{
            readonly active_attempt_id: string | null;
            readonly last_run_ordinal: number | null;
          }>`
            SELECT
              json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id,
              p.last_run_ordinal
            FROM orchestration_v2_projection_provider_threads p
            JOIN orchestration_v2_projection_runs r
              ON r.run_id = ${input.runId}
             AND r.thread_id = p.thread_id
            WHERE p.provider_thread_id = ${input.providerThreadId}
            LIMIT 1
          `;
          const current = rows[0];
          if (
            current === undefined ||
            current.active_attempt_id !== input.activeAttemptId ||
            current.last_run_ordinal !== input.expectedLastRunOrdinal
          ) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const normalized = yield* normalizeEvents(
            input.guardPendingUserInputCancellations === true
              ? yield* guardUserInputCancellations(input.events)
              : input.events,
          );
          const storedEvents = yield* eventStore.append({
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            events: normalized,
          });
          const outcomes = yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents, outcomes };
        }),
        (result) =>
          result.committed
            ? publishStoredEvents(result.storedEvents, result.outcomes)
            : Effect.void,
      );
    });

    const writeIfProviderThreadUnchangedEffect = Effect.fn(
      "orchestrationV2.EventSink.writeIfProviderThreadUnchanged",
    )(function* (input: Parameters<EventSinkV2Shape["writeIfProviderThreadUnchanged"]>[0]) {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.event_count": input.events.length,
        "orchestration_v2.provider_thread_id": input.expected.id,
      });
      const expectedJson = yield* ProjectionStore.encodeProviderThreadPayload(input.expected);

      return yield* commitThenPublish(
        Effect.gen(function* () {
          const rows = yield* sql<{ readonly payload_json: string }>`
            SELECT payload_json
            FROM orchestration_v2_projection_provider_threads
            WHERE provider_thread_id = ${input.expected.id}
            LIMIT 1
          `;
          if (rows[0]?.payload_json !== expectedJson) {
            return {
              committed: false as const,
              storedEvents: [] as ReadonlyArray<OrchestrationV2StoredEvent>,
            };
          }

          const normalized = yield* normalizeEvents(input.events);
          const storedEvents = yield* eventStore.append({ events: normalized });
          const outcomes = yield* applyStoredEvents(storedEvents);
          return { committed: true as const, storedEvents, outcomes };
        }),
        (result) =>
          result.committed
            ? publishStoredEvents(result.storedEvents, result.outcomes)
            : Effect.void,
      );
    });

    const decodeRestartCutRun = Schema.decodeUnknownEffect(
      Schema.fromJsonString(OrchestrationV2RunJson),
    );
    const patchRestartCutDecline = Effect.fnUntraced(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      const decline = input.restartCutDecline;
      if (decline === undefined) return undefined;
      const rows = yield* sql<{ readonly payload_json: string }>`
        UPDATE orchestration_v2_projection_runs AS source
        SET payload_json = json_set(
          payload_json, '$.restartCut', json_object(
            'continuation', 'declined',
            'cutAt', coalesce(
              json_extract(payload_json, '$.restartCut.cutAt'),
              json_extract(payload_json, '$.completedAt')
            ),
            'reason', ${decline.reason}
          )
        )
        WHERE source.run_id = ${decline.runId} AND source.thread_id = ${input.threadId}
          AND source.status = 'cancelled'
          AND coalesce(json_extract(payload_json, '$.restartCut.cutAt'), json_extract(payload_json, '$.completedAt')) IS NOT NULL
          AND (
            json_extract(payload_json, '$.restartCut.continuation') = 'scheduled'
            OR (
              json_type(payload_json, '$.restartCut') IS NULL
              AND (
                EXISTS (
                  SELECT 1 FROM orchestration_v2_effect_outbox AS intent
                  WHERE intent.effect_id = 'effect:restart-continuation:' || source.run_id
                  AND intent.thread_id = source.thread_id
                  AND intent.effect_type = 'provider-runtime.continue'
                  AND json_extract(intent.payload_json, '$.type') = 'provider-runtime.continue'
                  AND json_extract(intent.payload_json, '$.sourceRunId') = source.run_id
                )
                OR (
                  ${decline.reason === RESTART_CUT_REASONS.TASK_SETTLED ? 1 : 0}
                  AND EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_provider_turns AS turn
                    WHERE turn.run_attempt_id = json_extract(source.payload_json, '$.activeAttemptId')
                      AND turn.status = 'cancelled'
                      AND json_extract(turn.payload_json, '$.completedAt') = source.completed_at
                  )
                  AND EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_subagents AS task
                    WHERE task.child_thread_id = source.thread_id
                      AND json_extract(task.payload_json, '$.origin') = 'app_owned'
                      AND json_extract(task.payload_json, '$.result') IS NOT NULL
                  )
                )
              )
            )
          )
          AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_projection_runs AS replacement
            WHERE replacement.thread_id = source.thread_id
              AND json_extract(replacement.payload_json, '$.restartContinuationOfRunId') = source.run_id
          )
        RETURNING payload_json
      `;
      const row = rows[0];
      if (row === undefined) return undefined;
      const patched = yield* decodeRestartCutRun(row.payload_json);
      return {
        id: decline.eventId,
        type: "run.updated",
        threadId: input.threadId,
        runId: patched.id,
        providerInstanceId: patched.providerInstanceId,
        ...(patched.rootNodeId === null ? {} : { nodeId: patched.rootNodeId }),
        occurredAt: input.acceptedAt,
        payload: patched,
      } satisfies OrchestrationV2DomainEvent;
    });

    const existingCommandResult =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(function* (commandId: CommandId) {
        const existing = yield* commandReceipts.getByCommandId(commandId);
        if (Option.isNone(existing)) {
          return yield* Effect.die(
            new Error(`Command receipt ${commandId} disappeared during its transaction.`),
          );
        }
        const storedEvents = yield* eventStore.readByCommandId({ commandId }).pipe(
          Stream.runCollect,
          Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
        );
        return { receipt: existing.value, storedEvents };
      });

    const commitCommandEffect = Effect.fn("orchestrationV2.EventSink.commitCommand")(function* (
      input: Parameters<EventSinkV2Shape["commitCommand"]>[0],
    ) {
      let drained: Effect.Effect<void> = Effect.void;
      const execution = input.effects.some((effect) =>
        [
          "provider-turn.start",
          "provider-runtime.continue",
          "provider-turn.steer",
          "provider-turn.restart",
          "runtime-request.respond",
          "provider-thread.rollback",
        ].includes(effect.request.type),
      );
      const dispatch = yield* PeerDispatchAuthority;
      const automation = yield* PeerAcceptedAutomation;
      const explicit = [dispatch, automation].filter((id) => id !== undefined);
      const threadIds = [
        ...new Set([input.threadId, ...input.events.map((event) => event.threadId)]),
      ];
      const runIds = input.events.flatMap((event) => [
        ...(event.runId == null ? [] : [event.runId]),
        ...(event.type === "run.created"
          ? [
              event.payload.restartContinuationOfRunId,
              event.payload.forkPrismContinuationSourceRunId,
            ].filter((id) => id !== undefined)
          : []),
      ]);
      runIds.push(
        ...input.effects.flatMap((effect) =>
          effect.request.type === "provider-runtime.continue"
            ? [effect.request.sourceRunId]
            : "runId" in effect.request
              ? [effect.request.runId]
              : [],
        ),
      );
      const guarded = dispatch !== undefined || automation !== undefined || execution;
      const capturedOwners = guarded
        ? yield* nativeAuthorityOwners(explicit, threadIds, runIds).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          )
        : [];
      const admission =
        guarded && Option.isSome(nativeAuthority)
          ? yield* nativeAuthority.value.preflight(capturedOwners, threadIds).pipe(
              Effect.mapError(
                () =>
                  new ForkCommitGuardRejected({
                    threadId: input.threadId,
                    kind: "invalid_transition",
                  }),
              ),
            )
          : undefined;
      const result = yield* commitThenPublish(
        Effect.gen(function* () {
          let standaloneDeclineEvent: OrchestrationV2DomainEvent | undefined;
          if (
            input.restartCutDecline !== undefined &&
            input.events.length === 0 &&
            input.effects.length === 0 &&
            (input.forkPlans?.length ?? 0) === 0 &&
            input.cancelUnsettledEffects === undefined
          ) {
            if (Option.isSome(yield* commandReceipts.getByCommandId(input.commandId))) {
              const existing = yield* existingCommandResult(input.commandId);
              return {
                ...existing,
                committed: false as const,
                cancelledEffectIds: [],
                enqueuedEffectCount: 0,
              };
            }
            standaloneDeclineEvent = yield* patchRestartCutDecline(input);
            if (standaloneDeclineEvent === undefined) {
              // No receipt is persisted: an early guarded no-op must remain retryable.
              const receipt: CommandReceiptStore.CommandReceiptV2 = {
                commandId: input.commandId,
                threadId: input.threadId,
                commandType: input.commandType,
                acceptedAt: input.acceptedAt,
                resultSequence: yield* eventStore.latestSequence({ threadId: input.threadId }),
                status: "accepted",
                error: null,
              };
              return {
                receipt,
                storedEvents: [],
                committed: false as const,
                cancelledEffectIds: [],
                enqueuedEffectCount: 0,
              };
            }
          }
          const reserved = yield* commandReceipts.insertIfAbsent({
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: 0,
            status: "accepted",
            error: null,
          });
          if (!reserved) {
            const existing = yield* existingCommandResult(input.commandId);
            return {
              ...existing,
              committed: false as const,
              cancelledEffectIds: [],
              enqueuedEffectCount: 0,
            };
          }

          yield* validateForkStateOnlyCommand({
            ...input,
            events: standaloneDeclineEvent === undefined ? input.events : [standaloneDeclineEvent],
          });
          for (const plan of input.forkPlans ?? []) {
            for (const guard of plan.guards) {
              const decision = yield* guard.pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.provideService(ProjectionStore.ProjectionStoreV2, projectionStore),
              );
              if (decision === "accept_noop") {
                const receipt: CommandReceiptStore.CommandReceiptV2 = {
                  commandId: input.commandId,
                  threadId: input.threadId,
                  commandType: input.commandType,
                  acceptedAt: input.acceptedAt,
                  resultSequence: yield* eventStore.latestSequence({ threadId: input.threadId }),
                  status: "accepted",
                  error: null,
                };
                yield* commandReceipts.upsert(receipt);
                return {
                  receipt,
                  storedEvents: [],
                  outcomes: [],
                  committed: true as const,
                  cancelledEffectIds: [],
                  enqueuedEffectCount: 0,
                };
              }
            }
          }
          for (const plan of input.forkPlans ?? []) {
            for (const mutation of plan.mutations)
              yield* mutation.pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.provideService(ProjectionStore.ProjectionStoreV2, projectionStore),
              );
          }
          if (guarded) {
            const owners = yield* nativeAuthorityOwners(explicit, threadIds, runIds).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            if (admission !== undefined) {
              yield* admission.check.pipe(
                Effect.mapError(
                  () =>
                    new ForkCommitGuardRejected({
                      threadId: input.threadId,
                      kind: "invalid_transition",
                    }),
                ),
              );
              const actual = yield* nativeAuthorityOwners(explicit, threadIds, runIds).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              );
              if (
                actual.length !== capturedOwners.length ||
                actual.some((id, index) => id !== capturedOwners[index])
              )
                return yield* new ForkCommitGuardRejected({
                  threadId: input.threadId,
                  kind: "invalid_transition",
                });
            }
            yield* checkAutomationCommit(owners, admission?.births).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
              Effect.mapError(
                () =>
                  new ForkCommitGuardRejected({
                    threadId: input.threadId,
                    kind: "invalid_transition",
                  }),
              ),
            );
          }

          const runId = input.updateDelegatedCompletionOnlyRunId;
          const normalized = yield* normalizeEvents(
            runId === undefined
              ? input.events
              : yield* updateDelegatedCompletionOnly(input.events, (id) => id === runId),
          );
          let storedEvents = yield* eventStore.append({
            commandId: input.commandId,
            events: normalized,
          });
          const outcomes = [...(yield* applyStoredEvents(storedEvents))];
          const declineEvent = standaloneDeclineEvent ?? (yield* patchRestartCutDecline(input));
          if (declineEvent !== undefined) {
            const appended = yield* eventStore.append({
              commandId: input.commandId,
              events: [declineEvent],
            });
            outcomes.push(...(yield* applyStoredEvents(appended)));
            storedEvents = [...storedEvents, ...appended];
          }
          // A Stop that changes nothing can still queue the stop of its delegated tasks.
          const sequence = yield* forkCommitSequence(
            input,
            storedEvents,
            eventStore.latestSequence({ threadId: input.threadId }),
          );
          yield* recordPeerEffectAuthority(
            storedEvents.map((stored) => stored.event),
            input.effects,
          ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
          yield* effectOutbox.enqueue(input.effects);
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.acceptedAt,
            resultSequence: sequence,
            status: "accepted",
            error: null,
          };
          yield* commandReceipts.upsert(receipt);
          const cancelledEffectIds: string[] = [];
          if (input.cancelUnsettledEffects !== undefined) {
            const threadIds = [
              input.threadId,
              ...(input.cancelUnsettledEffects.includeSubagentDescendants
                ? yield* readStopDescendants(
                    input.threadId,
                    yield* projectionStore
                      .getShellSnapshot()
                      .pipe(
                        Effect.map((snapshot) => [
                          ...snapshot.threads,
                          ...snapshot.archivedThreads,
                        ]),
                      ),
                    (id) =>
                      projectionStore.getThread(id).pipe(
                        Effect.map(Option.some),
                        Effect.catchTags({
                          ProjectionStoreThreadNotFoundError: () => Effect.succeedNone,
                        }),
                      ),
                  )
                : []),
            ];
            for (const threadId of threadIds) {
              cancelledEffectIds.push(
                ...(yield* effectOutbox.cancelUnsettled({
                  threadId,
                  ...input.cancelUnsettledEffects,
                })),
              );
            }
          }
          return {
            receipt,
            storedEvents,
            outcomes,
            committed: true as const,
            cancelledEffectIds,
            enqueuedEffectCount: input.effects.length,
          };
        }),
        (result) =>
          Effect.gen(function* () {
            if (input.cancelUnsettledEffects?.drain === true)
              drained = yield* effectOutbox.drains(result.cancelledEffectIds);
            yield* effectOutbox.signalCancellations(result.cancelledEffectIds);
            if (result.enqueuedEffectCount > 0) {
              yield* effectOutbox.notifyAvailable(result.enqueuedEffectCount);
            }
            if (result.committed) {
              if ((input.forkPlans?.length ?? 0) !== 0 && Option.isSome(peerLinkEvents))
                yield* peerLinkEvents.value.automationChanged;
              yield* publishStoredEvents(result.storedEvents, result.outcomes);
            }
          }),
      );
      // A stopped start may still commit what it did, which takes the publish
      // lane; waiting for it while holding that lane deadlocks every commit (#511).
      yield* drained;
      return {
        receipt: result.receipt,
        storedEvents: result.storedEvents,
        committed: result.committed,
        cancelledEffectCount: result.cancelledEffectIds.length,
      };
    });

    const commitRejectedCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedCommand"]>[0]) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const sequence = yield* eventStore.latestSequence({ threadId: input.threadId });
          const receipt: CommandReceiptStore.CommandReceiptV2 = {
            commandId: input.commandId,
            threadId: input.threadId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: sequence,
            status: "rejected",
            error: input.error,
          };
          const inserted = yield* commandReceipts.insertIfAbsent(receipt);
          if (inserted) {
            return receipt;
          }
          const existing = yield* commandReceipts.getByCommandId(input.commandId);
          return Option.getOrElse(existing, () => receipt);
        }),
      );
    });

    const existingProjectReceipt = (commandId: CommandId) =>
      commandReceipts.getProjectByCommandId(commandId).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(`Command ${commandId} was already used by a thread command.` as const),
            onSome: Effect.succeed,
          }),
        ),
      );

    const commitProjectCommandEffect = Effect.fn("orchestrationV2.EventSink.commitProjectCommand")(
      function* (input: Parameters<EventSinkV2Shape["commitProjectCommand"]>[0]) {
        const result = yield* commitThenPublish(
          Effect.gen(function* () {
            const reserved: CommandReceiptStore.ProjectCommandReceiptV2 = {
              commandId: input.commandId,
              projectId: input.projectId,
              commandType: input.commandType,
              acceptedAt: input.acceptedAt,
              resultSequence: 0,
              status: "accepted",
              error: null,
            };
            if (!(yield* commandReceipts.insertIfAbsent(reserved))) {
              return { receipt: yield* existingProjectReceipt(input.commandId), event: undefined };
            }
            const event = yield* eventStore.appendProjectEvent(input.event);
            yield* projectStore.apply(event);
            const receipt = { ...reserved, resultSequence: event.sequence };
            yield* commandReceipts.upsert(receipt);
            return { receipt, event };
          }),
          (result) =>
            result.event === undefined ? Effect.void : eventStore.publishCommitted([result.event]),
        );
        return { receipt: result.receipt, committed: result.event !== undefined };
      },
    );

    const commitRejectedProjectCommandEffect = Effect.fn(
      "orchestrationV2.EventSink.commitRejectedProjectCommand",
    )(function* (input: Parameters<EventSinkV2Shape["commitRejectedProjectCommand"]>[0]) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const receipt: CommandReceiptStore.ProjectCommandReceiptV2 = {
            commandId: input.commandId,
            projectId: input.projectId,
            commandType: input.commandType,
            acceptedAt: input.rejectedAt,
            resultSequence: yield* eventStore.latestApplicationSequence,
            status: "rejected",
            error: input.error,
          };
          return (yield* commandReceipts.insertIfAbsent(receipt))
            ? receipt
            : yield* existingProjectReceipt(input.commandId);
        }),
      );
    });

    const stream = (input: Parameters<EventSinkV2Shape["stream"]>[0]) => {
      // The parameter stays required. A runtime caller can still omit it.
      // Read fields only after this confirms an object, so `stream()` returns
      // a stream error instead of throwing.
      const provided = input as typeof input | null | undefined;
      // `readAgentEvents` turns a missing cursor into `0` and sends any other
      // number through as `sequence > cursor`. Reject an omitted argument and
      // every unusable cursor here, before subscribe or read, so both `stream`
      // and `streamStoredEventsFrom` fail closed. `0` stays a deliberate full
      // replay of the matched range.
      if (
        provided === undefined ||
        provided === null ||
        !Number.isSafeInteger(provided.afterSequence) ||
        provided.afterSequence < 0
      ) {
        const received =
          provided === undefined || provided === null ? undefined : provided.afterSequence;
        return Stream.fail(
          new EventSinkStreamError({
            ...(provided === undefined || provided === null || provided.threadId === undefined
              ? {}
              : { threadId: provided.threadId }),
            // NaN and Infinity are not schema numbers. Omit them so the
            // rejection itself is the stream error.
            ...(typeof received === "number" && Number.isFinite(received)
              ? { afterSequence: received }
              : {}),
            cause: new Error(
              `afterSequence must be a non-negative safe integer, received ${String(received)}`,
            ),
          }),
        );
      }
      const afterSequence = provided.afterSequence;
      const streamError = (cause: unknown) =>
        new EventSinkStreamError({
          ...(provided.threadId === undefined ? {} : { threadId: provided.threadId }),
          ...(Number.isFinite(afterSequence) ? { afterSequence } : {}),
          cause,
        });
      const matches = (stored: OrchestrationV2StoredEvent) =>
        (provided.threadId === undefined || stored.event.threadId === provided.threadId) &&
        (provided.eventType === undefined || stored.event.type === provided.eventType);
      // Historical replay pages are released as they are consumed. The live
      // subscription stays unbounded and can retain events published during replay.
      const replay = (throughSequence: number) =>
        eventStore
          .read({
            afterSequence,
            throughSequence,
            ...(provided.threadId === undefined ? {} : { threadId: provided.threadId }),
            ...(provided.eventType === undefined ? {} : { eventType: provided.eventType }),
          })
          .pipe(Stream.filter(matches));
      return Stream.unwrap(
        Effect.gen(function* () {
          let pubsub = liveEvents;
          if (provided.eventType !== undefined) {
            const existing = liveEventsByType.get(provided.eventType);
            if (existing !== undefined) {
              pubsub = existing;
            } else {
              const created = yield* PubSub.unbounded<OrchestrationV2StoredEvent>();
              pubsub = liveEventsByType.get(provided.eventType) ?? created;
              liveEventsByType.set(provided.eventType, pubsub);
            }
          }
          if (provided.bounded === true) {
            return replayAndBufferProjectedLiveEvents({
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: eventStore.latestSequence(),
              afterSequence,
              filter: matches,
              replay,
              project: (stored) => ({ ...stored, event: projectDomainEventForWire(stored.event) }),
            });
          }
          const subscription = yield* PubSub.subscribe(pubsub);
          const highWater = yield* eventStore.latestSequence();
          const live = Stream.fromSubscription(subscription).pipe(
            Stream.filter((stored) => stored.sequence > Math.max(highWater, afterSequence)),
            Stream.filter(matches),
          );
          return Stream.concat(replay(highWater), live);
        }),
      ).pipe(Stream.mapError(streamError));
    };

    return EventSinkV2.of({
      write: (input) =>
        writeEffect({ ...input, effects: [] }).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeWithEffects: (input) =>
        writeEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfRunCurrent: (input) =>
        writeIfRunCurrentEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadOwner: (input) =>
        writeIfProviderThreadOwnerEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                eventCount: input.events.length,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                cause,
              }),
          ),
        ),
      writeIfProviderThreadUnchanged: (input) =>
        writeIfProviderThreadUnchangedEffect(input).pipe(
          Effect.mapError(
            (cause) => new EventSinkWriteError({ eventCount: input.events.length, cause }),
          ),
        ),
      commitCommand: (input) =>
        commitCommandEffect(input).pipe(
          Effect.mapError((cause) =>
            isForkCommitGuardRejected(cause)
              ? cause
              : new EventSinkWriteError({
                  commandId: input.commandId,
                  eventCount: input.events.length,
                  cause,
                }),
          ),
        ),
      commitRejectedCommand: (input) =>
        commitRejectedCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({
                commandId: input.commandId,
                eventCount: 0,
                cause,
              }),
          ),
        ),
      commitProjectCommand: (input) =>
        commitProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 1, cause }),
          ),
        ),
      commitRejectedProjectCommand: (input) =>
        commitRejectedProjectCommandEffect(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkWriteError({ commandId: input.commandId, eventCount: 0, cause }),
          ),
        ),
      stream: (input) => stream(input),
      latestSequence: (input) =>
        eventStore.latestSequence(input).pipe(
          Effect.mapError(
            (cause) =>
              new EventSinkStreamError({
                ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
                cause,
              }),
          ),
        ),
      readByCommandId: (input) =>
        eventStore.readByCommandId(input).pipe(
          Stream.mapError(
            (cause) =>
              new EventSinkStreamError({
                cause,
              }),
          ),
        ),
    } satisfies EventSinkV2Shape);
  }),
);

/** Share the worker's outbox instance: enqueue wakeups are in-memory. */
export const layerFromStores = layerBase;

export const layer: Layer.Layer<
  EventSinkV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = layerBase.pipe(
  Layer.provide(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
    ),
  ),
);
