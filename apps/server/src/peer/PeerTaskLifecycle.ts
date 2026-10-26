import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import { PROCESS_BOUND_EFFECT_TYPES, EffectOutboxV2 } from "../orchestration-v2/EffectOutbox.ts";
import {
  executionPurposeOf,
  OrchestratorMcpFailure,
  type PeerTaskLifecycleRequest,
  type OrchestrationV2Run,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2TurnItem,
  type OrchestrationV2RuntimeRequest,
  type ThreadId,
  type PeerTaskLifecycleResult,
  type PeerTaskSnapshot,
  type PeerTaskResult,
  type OrchestrationV2DomainEvent,
  EventId,
  CommandId,
} from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import { PeerDispatchAuthority } from "./peerEffectAuthority.ts";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import {
  delegatedTaskProgress,
  subagentResultForRun,
} from "@t3tools/provider-core/server/subagentProjection";
import { restartCutView } from "@t3tools/shared/restartCut";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Events from "../orchestration-v2/EventStore.ts";
import { RecoveryWaitState } from "./recoveryWait.ts";

export class PeerTaskLifecycle extends Context.Service<
  PeerTaskLifecycle,
  {
    readonly execute: (
      scope: Parameters<PeerAuthority.PeerAuthority["Service"]["verifyTask"]>[0],
      input: PeerTaskLifecycleRequest,
    ) => Effect.Effect<PeerTaskLifecycleResult, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerTaskLifecycle") {}

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const lifecycleTypes = [
  "run.created",
  "run.updated",
  "run.background-work-cancelled",
  "subagent.updated",
  "provider-thread.updated",
  "provider-turn.updated",
  "runtime-request.updated",
  "turn-item.updated",
  "thread.created",
  "thread.metadata-updated",
  "thread.archived",
  "thread.unarchived",
  "thread.deleted",
  "thread.settled",
  "thread.unsettled",
  "thread.runtime-mode-updated",
  "thread.interaction-mode-updated",
  "thread.model-selection-updated",
  "thread.provider-switched",
] as const satisfies ReadonlyArray<OrchestrationV2DomainEvent["type"]>;
const executionEffects = [
  ...PROCESS_BOUND_EFFECT_TYPES,
  "provider-runtime.continue",
  "provider-session.detach",
  "delegated-tasks.stop",
] as const;
const lifecycleTypeSet = new Set<string>(lifecycleTypes);
const requestItemTypes = ["user_input_request", "approval_request"] as const;

const requestDetail = (requestId: string, items: ReadonlyArray<OrchestrationV2TurnItem>) => {
  const item = items.find(
    (item) =>
      (item.type === "user_input_request" || item.type === "approval_request") &&
      item.requestId === requestId,
  );
  const text =
    item?.type === "user_input_request"
      ? json(item.questions)
      : item?.type === "approval_request"
        ? json({
            requestKind: item.requestKind,
            prompt: item.prompt,
            appName: item.appName,
            options: item.options,
          })
        : undefined;
  return text === undefined
    ? {}
    : {
        detail:
          text.length <= 4096 ? text : `${text.slice(0, 4048)} [truncated; read the full request]`,
      };
};

const make = Effect.gen(function* () {
  const authority = yield* PeerAuthority.PeerAuthority;
  const threads = yield* Threads.ThreadManagementService;
  const events = yield* Events.EventStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const outbox = yield* EffectOutboxV2;
  const recovery = yield* RecoveryWaitState;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_task_stop_receipts (command_id TEXT PRIMARY KEY, semantic_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS peer_task_observation_ack (task_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL)`;
  const unavailable = () =>
    new OrchestratorMcpFailure({
      code: "orchestration_error",
      message: "The delegated lifecycle observation is unavailable.",
    });
  // Result pages belong to a terminal commit, not today's mutable message projection.
  const resultAt = Effect.fn("PeerTaskLifecycle.resultAt")(function* (
    threadId: ThreadId,
    runId: OrchestrationV2Run["id"],
    sequence: number,
    commandId: string | null,
  ) {
    const committed =
      commandId === null
        ? sequence
        : ((yield* sql<{
            through: number;
          }>`SELECT COALESCE(MAX(sequence),${sequence}) AS through FROM orchestration_events WHERE command_id=${commandId}`)[0]
            ?.through ?? sequence);
    const runs = new Map<string, OrchestrationV2Run>();
    const messages = new Map<string, OrchestrationV2ConversationMessage>();
    const turnItems = new Map<string, OrchestrationV2TurnItem>();
    yield* events.read({ threadId, throughSequence: committed }).pipe(
      Stream.runForEach((stored) =>
        Effect.sync(() => {
          const event = stored.event;
          if (event.type === "run.created" || event.type === "run.updated")
            runs.set(event.payload.id, event.payload);
          else if (event.type === "message.updated") messages.set(event.payload.id, event.payload);
          else if (event.type === "turn-item.updated")
            turnItems.set(event.payload.id, event.payload);
        }),
      ),
    );
    const run = runs.get(runId);
    if (run === undefined) return yield* unavailable();
    return subagentResultForRun(
      {
        runs: [...runs.values()],
        messages: [...messages.values()],
        turnItems: [...turnItems.values()],
      },
      run,
    ).text;
  });
  const execute: PeerTaskLifecycle["Service"]["execute"] = (scope, input) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { pair, operation } = input;
        if (operation.type === "stop") {
          // The command identity excludes semantic inputs, so a conflicting replay
          // cannot obtain another stop on a new run under the old request key.
          const key = Hex.encode(
            sha256(new TextEncoder().encode(json([pair, operation.requestId]))),
          );
          const commandId = CommandId.make(`peer-task-stop:${key}`);
          const semantic = json(input);
          yield* authority.verifyTaskReceipt(scope, input);
          const prior = yield* sql<{
            semantic_json: string;
          }>`SELECT semantic_json FROM peer_task_stop_receipts WHERE command_id=${commandId}`;
          if (prior[0] && prior[0].semantic_json !== semantic)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The stop request identity has conflicting semantics.",
            });
          const existing = yield* receipts.getByCommandId(commandId);
          if (Option.isSome(existing)) {
            if (
              existing.value.threadId !== pair.childThreadId ||
              existing.value.commandType !== "thread.stop" ||
              existing.value.status !== "accepted"
            )
              return yield* unavailable();
            return { type: "stop", commandId } as const;
          }
          const owner = yield* authority.authorizeTaskStop(scope, { ...input, operation });
          yield* sql`INSERT INTO peer_task_stop_receipts (command_id,semantic_json) VALUES (${commandId},${semantic}) ON CONFLICT DO NOTHING`;
          const stored = yield* sql<{
            semantic_json: string;
          }>`SELECT semantic_json FROM peer_task_stop_receipts WHERE command_id=${commandId}`;
          if (stored[0]?.semantic_json !== semantic)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The stop request identity has conflicting semantics.",
            });
          yield* threads
            .dispatch({
              type: "thread.stop",
              threadId: pair.childThreadId,
              commandId,
              ...(operation.reason === undefined ? {} : { reason: operation.reason }),
            })
            .pipe(Effect.provideService(PeerDispatchAuthority, owner.id));
          // The committed native outbox owns descendant stops and recovery.
          return { type: "stop", commandId } as const;
        }
        yield* authority.verifyTask(scope, input);
        if (operation.type === "acknowledge") {
          const latest = yield* events.latestSequence();
          if (operation.through > latest)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The acknowledgement is ahead of the destination event log.",
            });
          yield* sql`INSERT INTO peer_task_observation_ack (task_id, cursor) VALUES (${pair.taskId}, ${operation.through}) ON CONFLICT(task_id) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)`;
          return { type: "acknowledged", through: operation.through } as const;
        }
        if (operation.type === "resultText") {
          const stored = yield* events
            .read({
              threadId: pair.childThreadId,
              afterSequence: operation.sequence - 1,
              throughSequence: operation.sequence,
              limit: 1,
            })
            .pipe(Stream.runHead);
          if (
            Option.isNone(stored) ||
            stored.value.event.id !== operation.eventId ||
            stored.value.event.type !== "run.updated" ||
            stored.value.event.payload.id !== operation.runId ||
            !["completed", "failed", "cancelled", "interrupted"].includes(
              stored.value.event.payload.status,
            )
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The result page does not name a terminal event of this delegated thread.",
            });
          const text = yield* resultAt(
            pair.childThreadId,
            operation.runId,
            operation.sequence,
            stored.value.commandId,
          );
          const part = text.slice(operation.textOffset, operation.textOffset + 50_000);
          return {
            type: "resultText",
            text: part,
            nextTextOffset:
              operation.textOffset + part.length < text.length
                ? operation.textOffset + part.length
                : null,
          } as const;
        }
        let dependencyThreads = new Set([pair.childThreadId]);
        let observedThrough = 0;
        let pendingStopEffectIds: ReadonlyArray<string> = [];
        const changes = yield* recovery.subscribe;
        const read = () =>
          sql.withTransaction(
            Effect.gen(function* () {
              const sequence = yield* events.latestSequence();
              observedThrough = sequence;
              const rootRevisions = yield* sql<{
                sequence: number;
              }>`SELECT COALESCE(MAX(sequence),0) AS sequence FROM orchestration_events WHERE stream_id=${pair.childThreadId} AND sequence<=${sequence} AND event_type IN ${sql.in(lifecycleTypes)} AND (event_type!='turn-item.updated' OR json_extract(payload_json,'$.type') IN ${sql.in(requestItemTypes)})`;
              const rootSequence = rootRevisions[0]?.sequence ?? 0;
              const child = yield* threads.getThreadRecords(
                pair.childThreadId,
                [
                  "runs",
                  "messages",
                  "turnItems",
                  "subagents",
                  "providerThreads",
                  "providerTurns",
                  "runtimeRequests",
                ],
                { turnItemTypes: requestItemTypes },
              );
              const monitors = new Set(
                child.messages
                  .filter((message) => message.notification?.source.kind === "monitor")
                  .map((message) => message.runId),
              );
              const progress = delegatedTaskProgress(child);
              let recoveryWaiting = false;
              for (const run of child.runs) {
                if (!["completed", "failed", "interrupted", "cancelled"].includes(run.status))
                  continue;
                const state = yield* recovery.read(pair.childThreadId, run.id);
                if (
                  state.pendingRecovery !== null ||
                  state.decision?.status === "pending" ||
                  restartCutView(run, child.runs)?.state === "continuation_pending"
                )
                  recoveryWaiting = true;
              }
              const revisions = yield* sql<{
                request_id: string;
                revision: number;
              }>`SELECT CASE WHEN event_type='runtime-request.updated' THEN json_extract(payload_json, '$.id') ELSE json_extract(payload_json, '$.requestId') END AS request_id, MAX(sequence) AS revision FROM orchestration_events WHERE stream_id = ${pair.childThreadId} AND sequence <= ${sequence} AND (event_type = 'runtime-request.updated' OR (event_type='turn-item.updated' AND json_extract(payload_json,'$.requestId') IS NOT NULL)) GROUP BY request_id`;
              const revisionById = new Map(revisions.map((row) => [row.request_id, row.revision]));
              let pendingChildren = 0;
              let queuedRuns = child.runs.filter(
                (run) => run.status === "queued" && run.queueHeld !== true && !monitors.has(run.id),
              ).length;
              let incomplete = false;
              const pendingDetail = (
                request: OrchestrationV2RuntimeRequest,
                items: ReadonlyArray<OrchestrationV2TurnItem>,
              ) => {
                const detail = requestDetail(request.id, items);
                // Providers can commit a runtime request before its content item.
                if (
                  request.kind !== "auth_refresh" &&
                  request.kind !== "dynamic_tool_call" &&
                  (request.kind === "user_input" || request.responseCapability.type === "live") &&
                  detail.detail === undefined
                )
                  incomplete = true;
                return detail;
              };
              let omittedRequests = 0;
              let omittedQuestions = 0;
              let executionSettled = !child.providerTurns.some((turn) =>
                ["pending", "running"].includes(turn.status),
              );
              const requests: Array<PeerTaskSnapshot["requests"][number]> = child.runtimeRequests
                .filter((request) => request.status === "pending")
                .map((request) => ({
                  environmentId: pair.childEnvironmentId,
                  threadId: pair.childThreadId,
                  requestId: request.id,
                  revision: revisionById.get(request.id) ?? sequence,
                  kind: request.kind,
                  responseType: request.responseCapability.type,
                  ...pendingDetail(request, child.turnItems),
                }));
              const seen = new Set([pair.childThreadId]);
              const queue = [...child.subagents];
              const executionThreads = [pair.childThreadId];
              for (let index = 0; index < queue.length; index++) {
                if (seen.size >= 256) {
                  incomplete = true;
                  break;
                }
                const task = queue[index]!;
                if (
                  ["pending", "running", "waiting"].includes(task.status) ||
                  task.completionDelivery?.state === "pending" ||
                  task.completionDelivery?.state === "claimed"
                )
                  pendingChildren++;
                if (task.childThreadId === null) {
                  if (task.remoteChild !== undefined) {
                    if (
                      task.remoteChild.stop !== undefined &&
                      !["confirmed", "not_started"].includes(task.remoteChild.stop.state)
                    ) {
                      executionSettled = false;
                      pendingChildren++;
                    }
                    if (
                      task.remoteChild.availability !== "current" ||
                      task.remoteChild.snapshot?.incomplete !== false
                    )
                      incomplete = true;
                    if (task.remoteChild.snapshot !== undefined) {
                      const observed = task.remoteChild.snapshot;
                      if (task.result !== null && observed.workState !== "result_available")
                        pendingChildren++;
                      pendingChildren += observed.pendingChildren;
                      queuedRuns += observed.queuedRuns;
                      recoveryWaiting ||= observed.recoveryWaiting;
                      if (!observed.executionSettled) executionSettled = false;
                      requests.push(
                        ...observed.requests.map((request) => ({
                          ...request,
                          route: [
                            {
                              environmentId: task.remoteChild!.environmentId,
                              threadId: task.remoteChild!.threadId!,
                            },
                            ...(request.route ?? []),
                          ],
                        })),
                      );
                      omittedRequests += Math.max(
                        0,
                        observed.requestCount - observed.requests.length,
                      );
                      omittedQuestions += Math.max(
                        0,
                        observed.pendingQuestions -
                          observed.requests.filter((request) => request.kind === "user_input")
                            .length,
                      );
                    }
                  }
                  continue;
                }
                if (seen.has(task.childThreadId)) continue;
                seen.add(task.childThreadId);
                executionThreads.push(task.childThreadId);
                const nested = yield* threads.getThreadRecords(
                  task.childThreadId,
                  [
                    "runs",
                    "messages",
                    "subagents",
                    "providerThreads",
                    "providerTurns",
                    "runtimeRequests",
                    "turnItems",
                  ],
                  {
                    messageRoles: ["user"],
                    turnItemTypes: requestItemTypes,
                  },
                );
                if (nested.thread.archivedAt !== null || nested.thread.deletedAt !== null) continue;
                const nestedProgress = delegatedTaskProgress(nested);
                queuedRuns += nested.runs.filter(
                  (run) =>
                    run.status === "queued" &&
                    run.queueHeld !== true &&
                    !nested.messages.some(
                      (message) =>
                        message.runId === run.id && message.notification?.source.kind === "monitor",
                    ),
                ).length;
                for (const run of nested.runs) {
                  if (!["completed", "failed", "interrupted", "cancelled"].includes(run.status))
                    continue;
                  const state = yield* recovery.read(task.childThreadId, run.id);
                  if (
                    state.pendingRecovery !== null ||
                    state.decision?.status === "pending" ||
                    restartCutView(run, nested.runs)?.state === "continuation_pending"
                  )
                    recoveryWaiting = true;
                }
                const nestedRevisions = yield* sql<{
                  request_id: string;
                  revision: number;
                }>`SELECT CASE WHEN event_type='runtime-request.updated' THEN json_extract(payload_json, '$.id') ELSE json_extract(payload_json, '$.requestId') END AS request_id, MAX(sequence) AS revision FROM orchestration_events WHERE stream_id = ${task.childThreadId} AND sequence <= ${sequence} AND (event_type = 'runtime-request.updated' OR (event_type='turn-item.updated' AND json_extract(payload_json,'$.requestId') IS NOT NULL)) GROUP BY request_id`;
                const nestedRevisionById = new Map(
                  nestedRevisions.map((row) => [row.request_id, row.revision]),
                );
                if (task.result !== null && nestedProgress.state !== "result_available")
                  pendingChildren++;
                if (
                  nested.providerTurns.some((turn) => ["pending", "running"].includes(turn.status))
                )
                  executionSettled = false;
                for (const request of nested.runtimeRequests) {
                  if (request.status === "pending")
                    requests.push({
                      environmentId: pair.childEnvironmentId,
                      threadId: task.childThreadId,
                      requestId: request.id,
                      revision: nestedRevisionById.get(request.id) ?? sequence,
                      kind: request.kind,
                      responseType: request.responseCapability.type,
                      ...pendingDetail(request, nested.turnItems),
                    });
                }
                queue.push(...nested.subagents);
              }
              dependencyThreads = new Set(executionThreads);
              incomplete ||= requests.length + omittedRequests > 100;
              const pendingEffects = yield* sql<{
                count: number;
              }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE thread_id IN ${sql.in(executionThreads)} AND status IN ('pending','running') AND effect_type IN ${sql.in(executionEffects)}`;
              executionSettled =
                executionSettled &&
                pendingEffects[0]?.count === 0 &&
                !incomplete &&
                pendingChildren === 0 &&
                progress.state === "result_available" &&
                !recoveryWaiting;
              const revisionsThrough = yield* sql<{
                sequence: number;
              }>`SELECT COALESCE(MAX(sequence),0) AS sequence FROM orchestration_events WHERE stream_id IN ${sql.in(executionThreads)} AND sequence<=${sequence} AND event_type IN ${sql.in(lifecycleTypes)} AND (event_type!='turn-item.updated' OR json_extract(payload_json,'$.type') IN ${sql.in(requestItemTypes)})`;
              let stopExecution: PeerTaskSnapshot["stopExecution"];
              if (
                (operation.type === "observe" || operation.type === "snapshot") &&
                operation.stopCommandId !== undefined
              ) {
                const receipt = yield* receipts.getByCommandId(operation.stopCommandId);
                if (
                  Option.isNone(receipt) ||
                  receipt.value.threadId !== pair.childThreadId ||
                  receipt.value.commandType !== "thread.stop" ||
                  receipt.value.status !== "accepted"
                )
                  return yield* new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message:
                      "This Stop receipt does not belong to the delegated destination thread.",
                  });
                const prefix = `${operation.stopCommandId}:stop:`;
                const effects = yield* sql<{
                  status: string;
                  effect_id: string;
                }>`SELECT effect_id,status FROM orchestration_v2_effect_outbox WHERE (command_id=${operation.stopCommandId} OR substr(command_id,1,${prefix.length})=${prefix}) AND effect_type IN ${sql.in(executionEffects)}`;
                pendingStopEffectIds = effects
                  .filter((effect) => effect.status === "pending" || effect.status === "running")
                  .map((effect) => effect.effect_id);
                stopExecution = {
                  commandId: operation.stopCommandId,
                  state: effects.some(
                    (effect) => effect.status === "failed" || effect.status === "cancelled",
                  )
                    ? "failed"
                    : effects.some(
                          (effect) => effect.status === "pending" || effect.status === "running",
                        )
                      ? "pending"
                      : effects.length === 0
                        ? "unknown"
                        : "settled",
                };
              }
              const snapshot: PeerTaskSnapshot = {
                sequence: revisionsThrough[0]?.sequence ?? 0,
                observedAt: DateTime.formatIso(yield* DateTime.now),
                title: child.thread.title,
                workState: recoveryWaiting
                  ? "working"
                  : (incomplete || pendingChildren > 0) && progress.state === "result_available"
                    ? "waiting_for_children"
                    : progress.state,
                pendingChildren,
                queuedRuns,
                recoveryWaiting,
                incomplete,
                executionSettled,
                ...(stopExecution === undefined ? {} : { stopExecution }),
                pendingQuestions:
                  requests.filter((request) => request.kind === "user_input").length +
                  omittedQuestions,
                requestCount: requests.length + omittedRequests,
                requests: requests.slice(0, 100),
              };
              if (operation.type === "snapshot") return { type: "snapshot", snapshot } as const;
              if (operation.type === "view") {
                const resource = operation.resource ?? {
                  environmentId: pair.childEnvironmentId,
                  threadId: pair.childThreadId,
                };
                if (
                  resource.environmentId !== pair.childEnvironmentId ||
                  !seen.has(resource.threadId)
                )
                  return yield* new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message:
                      "This resource is outside the delegated job's observed local descendants.",
                  });
                const viewed =
                  resource.threadId === pair.childThreadId
                    ? child
                    : yield* threads.getThreadRecords(resource.threadId, ["messages", "subagents"]);
                if (viewed.thread.deletedAt != null || viewed.thread.archivedAt != null)
                  return yield* new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message: "The selected descendant was retired.",
                  });
                const ordered = viewed.messages.toSorted(
                  (a, b) =>
                    DateTime.toEpochMillis(a.createdAt) - DateTime.toEpochMillis(b.createdAt),
                );
                const messages: Array<{
                  id: string;
                  runId: (typeof ordered)[number]["runId"];
                  role: (typeof ordered)[number]["role"];
                  text: string;
                }> = [];
                let budget = 50_000;
                let index = operation.after;
                let nextTextOffset: number | null = null;
                for (; index < ordered.length && messages.length < 20 && budget > 0; index++) {
                  const message = ordered[index]!;
                  const offset = index === operation.after ? (operation.textOffset ?? 0) : 0;
                  const text = message.text.slice(offset, offset + budget);
                  messages.push({ id: message.id, runId: message.runId, role: message.role, text });
                  budget -= text.length;
                  if (offset + text.length < message.text.length) {
                    nextTextOffset = offset + text.length;
                    break;
                  }
                }
                return {
                  type: "view",
                  view: {
                    taskId: pair.taskId,
                    environmentId: pair.childEnvironmentId,
                    threadId: resource.threadId,
                    snapshot,
                    childrenTruncated: viewed.subagents.length > 100,
                    children: viewed.subagents.slice(0, 100).map((task) => ({
                      taskId: task.id,
                      environmentId: task.remoteChild?.environmentId ?? pair.childEnvironmentId,
                      threadId: task.childThreadId ?? task.remoteChild?.threadId ?? null,
                      title: task.title,
                      status: task.status,
                    })),
                    messages,
                    nextCursor: index < ordered.length ? index : null,
                    nextTextOffset,
                  },
                } as const;
              }
              // Terminal transitions are durable identities. Checkpoint-only rewrites of
              // the same terminal status cannot manufacture another completion.
              const rows = yield* sql<{
                sequence: number;
                run_id: string;
                status: "completed" | "failed" | "cancelled" | "interrupted";
              }>`WITH transitions AS (SELECT sequence,json_extract(payload_json,'$.id') AS run_id,json_extract(payload_json,'$.status') AS status,LAG(json_extract(payload_json,'$.status')) OVER (PARTITION BY json_extract(payload_json,'$.id') ORDER BY sequence) AS previous_status FROM orchestration_events WHERE stream_id=${pair.childThreadId} AND sequence<=${sequence} AND event_type IN ('run.created','run.updated')) SELECT sequence,run_id,status FROM transitions WHERE sequence>${operation.after} AND status IN ('completed','failed','cancelled','interrupted') AND (previous_status IS NULL OR previous_status<>status) ORDER BY sequence LIMIT 32`;

              const predecessors = new Set(child.runs.map((run) => run.restartContinuationOfRunId));
              let cursor = operation.after;
              let result: PeerTaskResult | undefined;
              for (const row of rows) {
                const run = child.runs.find((run) => run.id === row.run_id);
                if (
                  run === undefined ||
                  monitors.has(run.id) ||
                  predecessors.has(run.id) ||
                  run.status === "rolled_back" ||
                  (run.startedAt === null &&
                    run.ordinal !== 1 &&
                    run.restartContinuationOfRunId === undefined &&
                    run.restartCut === undefined)
                ) {
                  cursor = row.sequence;
                  continue;
                }
                if (!["completed", "failed", "interrupted", "cancelled"].includes(run.status))
                  break;
                const state = yield* recovery.read(pair.childThreadId, run.id);
                if (
                  state.admittedContinuations.some(
                    (entry) => entry.threadId === pair.childThreadId,
                  ) ||
                  (state.decision?.status === "decided" && state.decision.outcome === "retried")
                ) {
                  cursor = row.sequence;
                  continue;
                }
                if (
                  state.pendingRecovery !== null ||
                  state.decision?.status === "pending" ||
                  restartCutView(run, child.runs)?.state === "continuation_pending"
                )
                  break;
                // The original job keeps the same whole-child barrier as local delegation.
                if (run.ordinal === 1 && snapshot.workState !== "result_available") break;
                const stored = yield* events
                  .read({
                    threadId: pair.childThreadId,
                    afterSequence: row.sequence - 1,
                    throughSequence: row.sequence,
                    limit: 1,
                  })
                  .pipe(Stream.runHead);
                if (Option.isNone(stored)) return yield* unavailable();
                const text = yield* resultAt(
                  pair.childThreadId,
                  run.id,
                  row.sequence,
                  stored.value.commandId,
                );
                result = {
                  environmentId: pair.childEnvironmentId,
                  threadId: pair.childThreadId,
                  runId: run.id,
                  purposeScope: executionPurposeOf(run).scope,
                  eventId: EventId.make(stored.value.event.id),
                  sequence: row.sequence,
                  status: row.status,
                  text: text.slice(0, 50_000),
                  truncated: text.length > 50_000,
                };
                cursor = row.sequence;
                break;
              }
              if (rows.length === 0) cursor = Math.max(operation.after, rootSequence);
              return {
                type: "observation",
                observation: { cursor, snapshot, ...(result === undefined ? {} : { result }) },
              } as const;
            }),
          );
        let result = yield* read();
        const observedSnapshot =
          result.type === "snapshot"
            ? result.snapshot
            : result.type === "observation"
              ? result.observation.snapshot
              : undefined;
        const sameStopState =
          (operation.type !== "observe" && operation.type !== "snapshot") ||
          operation.stopCommandId === undefined ||
          observedSnapshot?.stopExecution?.state === operation.afterStopExecution;

        if (
          (operation.type === "observe" || operation.type === "snapshot") &&
          operation.wait &&
          sameStopState &&
          ((result.type === "observation" &&
            result.observation.cursor === operation.after &&
            result.observation.result === undefined &&
            operation.type === "observe" &&
            result.observation.snapshot.sequence <= (operation.afterSnapshot ?? operation.after)) ||
            (result.type === "snapshot" && result.snapshot.sequence <= operation.after))
        ) {
          yield* threads
            .streamStoredEventsFrom({
              afterSequence: observedThrough,
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  dependencyThreads.has(stored.event.threadId) &&
                  lifecycleTypeSet.has(stored.event.type) &&
                  (stored.event.type !== "turn-item.updated" ||
                    stored.event.payload.type === "user_input_request" ||
                    stored.event.payload.type === "approval_request"),
              ),
              Stream.runHead,
              Effect.raceFirst(
                changes.pipe(
                  Stream.filter((id) => dependencyThreads.has(id)),
                  Stream.runHead,
                ),
              ),
              Effect.raceFirst(
                pendingStopEffectIds.length === 0
                  ? Effect.never
                  : Effect.forEach(pendingStopEffectIds, (id) => outbox.awaitSettled(id), {
                      concurrency: "unbounded",
                      discard: true,
                    }),
              ),
              Effect.timeoutOption("20 seconds"),
            );
          yield* authority.verifyTask(scope, input);
          result = yield* read();
        }
        return result;
      }),
    ).pipe(
      Effect.tapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? Effect.void
          : Effect.logWarning("Delegated lifecycle read failed", error),
      ),
      Effect.mapError((error) => (error._tag === "OrchestratorMcpFailure" ? error : unavailable())),
    );
  return PeerTaskLifecycle.of({ execute });
});
export const layer = Layer.effect(PeerTaskLifecycle, make).pipe(
  Layer.provide(Layer.merge(Events.layer, Receipts.layer)),
);
