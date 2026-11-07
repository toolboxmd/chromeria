import * as Predicate from "effect/Predicate";
import { CurrentRequest } from "../../McpRequestReceipts.ts";
import {
  type CommandId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type RunId,
  OrchestratorMcpFailure,
  type OrchestrationV2Command,
  threadOwner,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as PendingRequests from "../../../childThreads/PendingRequestService.ts";
import { modelSelectionCommandType } from "@t3tools/shared/model";
import { formatThreadLink } from "@t3tools/shared/threadLinks";

import { requestAnswerer } from "../../../childThreads/pendingRequests.ts";
import { assertSameLink } from "../../linkOrigin.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import {
  dispatchFailure,
  newCommandId,
  readCaller,
  readThread,
  unavailable,
} from "../../threadAccess.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import { queuedRunsInDeliveryOrder } from "../../../orchestration-v2/QueuedRunOrder.ts";
import { ThreadToolkit } from "./tools.ts";

const pendingRequestEffect = (input: unknown) =>
  Predicate.isObject(input) && "action" in input && input.action !== "respond"
    ? ("read" as const)
    : ("owned" as const);

function queueEntry(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
  runId: RunId,
  limit: number,
) {
  const run = projection.runs.find((run) => run.id === runId && run.status === "queued");
  const message = projection.messages.find((message) => message.id === run?.userMessageId);
  if (run === undefined || message === undefined) return undefined;
  const characters = Array.from(message.text);
  return {
    queuedRunId: run.id,
    held: run.queueHeld === true,
    text: characters.slice(0, limit).join(""),
    truncated: characters.length > limit,
  };
}
const dispatch = Effect.fn("mcp.dispatchThreadCommand")(function* (
  threadId: ThreadId | undefined,
  command: (common: { commandId: CommandId; threadId: ThreadId }) => OrchestrationV2Command,
) {
  const { threads, projection } = yield* readThread(threadId);
  const result = yield* threads
    .dispatch(command({ commandId: yield* newCommandId(), threadId: projection.thread.id }))
    .pipe(Effect.mapError(dispatchFailure));
  return { sequence: result.sequence };
});

const writesThread = <P extends { readonly threadId?: ThreadId | undefined }, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
) => McpToolAccess.writesThreads((params: P) => [params.threadId], handle);

export const layer = McpToolAccess.toLayer(
  ThreadToolkit,
  Effect.gen(function* () {
    const pending = yield* PendingRequests.PendingRequests;
    const listPendingRequests = Effect.fn("listPendingRequests")(function* (input: {
      readonly threadId?: ThreadId | undefined;
    }) {
      const context = yield* readThread(input.threadId, ["runtimeRequests"]);
      const { projection } = context;
      const answerer = yield* requestAnswerer(context, projection.thread);
      const descendant = answerer !== undefined && answerer.kind !== "own";
      return {
        requestIds: projection.runtimeRequests
          .filter(
            (request) =>
              request.status === "pending" &&
              (request.kind === "user_input" ||
                (descendant &&
                  request.kind !== "dynamic_tool_call" &&
                  request.kind !== "auth_refresh")),
          )
          .map((request) => request.id),
      };
    });
    return {
      run_scheduled_task_now: McpToolAccess.writesEnvironment((input, check) =>
        Effect.gen(function* () {
          const scheduler = yield* ScheduledTasks.ScheduledTaskService;
          const { tasks } = yield* scheduler.list().pipe(Effect.mapError(unavailable));
          const selected = tasks.find((task) => task.id === input.taskId);
          if (selected === undefined)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The scheduled task was not found.",
            });
          yield* assertSameLink(yield* readCaller(), selected);
          const request = yield* CurrentRequest;
          // The native owner captures its local binding fence before this live check.
          // Re-authorize the actual current target, not the handler's earlier snapshot.
          const recheck = check.pipe(
            Effect.andThen(
              Effect.gen(function* () {
                const current = (yield* scheduler
                  .list()
                  .pipe(Effect.mapError(unavailable))).tasks.find(
                  (task) => task.id === input.taskId,
                );
                if (current === undefined)
                  return yield* new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: "The scheduled task was not found.",
                  });
                yield* assertSameLink(yield* readCaller(), current);
              }),
            ),
          );
          const context = yield* Effect.context<Effect.Services<typeof recheck>>();
          const operation =
            request === undefined
              ? scheduler.runNow({ id: input.taskId })
              : scheduler.runNow(
                  { id: input.taskId },
                  { request, recheck: recheck.pipe(Effect.provideContext(context), Effect.asVoid) },
                );
          const { task, threadId, manualReceipt } = yield* operation.pipe(
            Effect.mapError((error) =>
              error._tag === "OrchestratorMcpFailure"
                ? error
                : new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: error.message,
                  }),
            ),
          );
          return {
            taskId: task.id,
            ...(manualReceipt === undefined ? {} : { manualReceipt }),
            threadId,
            lastRunStatus: task.lastRunStatus,
            runCount: task.runCount,
            nextRunAt: task.nextRunAt,
          };
        }),
      ),
      t3_thread_search: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const { caller, scope } = yield* readCaller();
          // Like the other project tools, an omitted project means the caller's own; a client
          // outside a thread searches every project.
          const threadSearch = yield* ThreadSearch.ThreadSearch;
          const { matches } = yield* threadSearch
            .searchThreads({ ...input, projectId: input.projectId ?? caller?.projectId })
            .pipe(Effect.mapError(unavailable));
          const environmentId = scope.environmentId;
          return {
            matches: matches.map((match) => ({
              ...match,
              link: formatThreadLink({ environmentId, threadId: match.threadId }, match.title),
            })),
          };
        }),
      ),
      t3_thread_fork: writesThread((input) =>
        Effect.gen(function* () {
          const { threads, projection, caller } = yield* readThread(input.threadId);
          const commandId = yield* newCommandId();
          const targetThreadId = ThreadId.make(`${commandId}:fork`);
          const result = yield* threads
            .dispatch({
              type: "thread.fork",
              commandId,
              sourceThreadId: projection.thread.id,
              targetThreadId,
              sourcePoint: input.sourcePoint,
              ...(input.title === undefined ? {} : { title: input.title }),
              createdBy: "agent",
              creationSource: "mcp",
              // Fork: an agent's fork belongs to its caller's owner (toolboxmd/chromeria#170).
              ...(caller === undefined ? {} : { owner: threadOwner(caller) }),
            })
            .pipe(Effect.mapError(dispatchFailure));
          return { sequence: result.sequence, targetThreadId };
        }),
      ),
      t3_thread_merge_back: McpToolAccess.writesThreads(
        (input) => [input.targetThreadId, input.sourceThreadId],
        (input) =>
          Effect.gen(function* () {
            const context = yield* readThread(input.targetThreadId);
            const source = yield* readThread(input.sourceThreadId);
            const result = yield* context.threads
              .dispatch({
                type: "thread.merge_back",
                commandId: yield* newCommandId(),
                sourceThreadId: source.projection.thread.id,
                targetThreadId: input.targetThreadId,
                sourcePoint: input.sourcePoint,
                createdBy: "agent",
                creationSource: "mcp",
              })
              .pipe(Effect.mapError(dispatchFailure));
            return { sequence: result.sequence, targetThreadId: input.targetThreadId };
          }),
      ),
      t3_thread_transfers: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const { projection } = yield* readThread(input.threadId, ["contextTransfers"]);
          return {
            transfers: projection.contextTransfers.map(
              ({ id, sourceThreadId, targetThreadId, status }) => ({
                id,
                sourceThreadId,
                targetThreadId,
                status,
              }),
            ),
          };
        }),
      ),
      t3_thread_configuration: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const {
            projection: { thread },
          } = yield* readThread(input.threadId);
          return {
            ...(thread.bare === undefined ? {} : { bare: thread.bare }),
            ...(thread.tools === undefined ? {} : { tools: thread.tools }),
            threadId: thread.id,
            modelSelection: thread.modelSelection,
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
          };
        }),
      ),
      t3_thread_configure: writesThread((input) =>
        Effect.gen(function* () {
          const {
            threads,
            projection: { thread },
          } = yield* readThread(input.threadId);
          if (input.modelSelection === undefined)
            return {
              threadId: thread.id,
              modelSelection: thread.modelSelection,
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
            };
          const type = modelSelectionCommandType(thread.providerInstanceId, input.modelSelection);
          const result = yield* threads
            .dispatch({
              type,
              threadId: thread.id,
              commandId: yield* newCommandId(),
              modelSelection: input.modelSelection,
            })
            .pipe(Effect.mapError(dispatchFailure));
          return { sequence: result.sequence };
        }),
      ),
      t3_pending_request: McpToolAccess.either(
        (input) => input.action !== "respond",
        McpToolAccess.reads((input) =>
          Effect.gen(function* () {
            if (input.action === "list") return yield* listPendingRequests(input);
            if (input.requestId === undefined)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Pass requestId.",
              });
            return yield* pending.read({ ...input, requestId: input.requestId });
          }),
        ),
        McpToolAccess.writesThreads(
          () => [],
          (input) =>
            Effect.gen(function* () {
              if (input.requestId === undefined)
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "Pass requestId.",
                });
              return yield* pending.respond(
                { ...input, requestId: input.requestId },
                { type: "respond", answers: input.answers, decision: input.decision },
              );
            }),
        ),
        pendingRequestEffect,
      ),
      t3_pending_request_list: McpToolAccess.reads(listPendingRequests),
      t3_pending_request_read: McpToolAccess.reads((input) => pending.read(input)),
      t3_pending_request_respond: McpToolAccess.writesThreads(
        () => [],
        (input) =>
          pending.respond(input, {
            type: "respond",
            answers: input.answers,
            decision: input.decision,
          }),
      ),
      t3_pending_request_dismiss: McpToolAccess.writesThreads(
        () => [],
        (input) => pending.respond(input, { type: "dismiss" }),
      ),
      t3_queue_list: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const { projection } = yield* readThread(input.threadId, ["runs", "messages"]);
          const runs = queuedRunsInDeliveryOrder(projection);
          const cursor = input.cursor ?? 0;
          const end = cursor + (input.limit ?? 20);
          return {
            items: runs.slice(cursor, end).flatMap((run) => {
              const entry = queueEntry(projection, run.id, 1000);
              return entry === undefined ? [] : [entry];
            }),
            nextCursor: end < runs.length ? end : null,
          };
        }),
      ),
      t3_queue_read: McpToolAccess.reads((input) =>
        Effect.gen(function* () {
          const { projection } = yield* readThread(input.threadId, ["runs", "messages"]);
          const entry = queueEntry(projection, input.queuedRunId, 16000);
          return (
            entry ??
            (yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The queued message was not found.",
            }))
          );
        }),
      ),
      t3_queue_edit: writesThread((input) =>
        dispatch(input.threadId, (common) => ({
          ...common,
          type: "queued-run.edit",
          runId: input.queuedRunId,
          text: input.text,
        })),
      ),
      t3_queue_cancel: writesThread((input) =>
        dispatch(input.threadId, (common) => ({
          ...common,
          type: "queued-run.cancel",
          runId: input.queuedRunId,
        })),
      ),
      t3_queue_resume: writesThread((input) =>
        dispatch(input.threadId, (common) => ({ ...common, type: "queue.resume" })),
      ),
      t3_queue_reorder: writesThread((input) =>
        dispatch(input.threadId, (common) => ({
          ...common,
          type: "queued-run.reorder",
          runId: input.queuedRunId,
          beforeRunId: input.beforeRunId,
        })),
      ),
      t3_queue_promote_to_steer: writesThread((input) =>
        Effect.gen(function* () {
          const { caller } = yield* readCaller();
          return yield* dispatch(input.threadId, (common) => ({
            ...common,
            type: "queued-message.promote-to-steer",
            queuedRunId: input.queuedRunId,
            targetRunId: input.targetRunId,
            createdBy: "agent",
            creationSource: "mcp",
            ...(caller === undefined ? {} : { senderThreadId: caller.id }),
          }));
        }),
      ),
      t3_thread_organize: writesThread((input) =>
        Effect.gen(function* () {
          const { threads, projection, caller } = yield* readThread(input.threadId);
          const common = { commandId: yield* newCommandId(), threadId: projection.thread.id };
          if (input.action === "settle") {
            return yield* threads
              .settleThread({ ...common, byOwnAgent: caller?.id === projection.thread.id })
              .pipe(Effect.mapError(dispatchFailure));
          }
          let command: OrchestrationV2Command;
          switch (input.action) {
            case "snooze":
              if (input.snoozedUntil === undefined) {
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "snooze requires snoozedUntil.",
                });
              }
              command = { ...common, type: "thread.snooze", snoozedUntil: input.snoozedUntil };
              break;
            case "unsnooze":
            case "unsettle":
              command = { ...common, type: `thread.${input.action}`, reason: "user" };
              break;
            case "mark_unread":
              command = { ...common, type: "thread.mark-unread" };
              break;
            default:
              command = { ...common, type: `thread.${input.action}` };
          }
          const result = yield* threads.dispatch(command).pipe(Effect.mapError(dispatchFailure));
          return { sequence: result.sequence };
        }),
      ),
    } satisfies McpToolAccess.Handlers<typeof ThreadToolkit.tools>;
  }).pipe(Effect.provide(PendingRequests.layer)),
);
