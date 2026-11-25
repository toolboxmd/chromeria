import * as PeerLinkEvents from "../peer/PeerLinkEvents.ts";
import * as RemoteSecretPersistence from "../secrets/remoteSecretPersistence.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Option from "effect/Option";
import {
  CommandId,
  MessageId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as KeyedLock from "@t3tools/shared/KeyedLock";

import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export class DelegatedTaskQuestionReactor extends Context.Service<
  DelegatedTaskQuestionReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly handle: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/DelegatedTaskQuestionReactor") {}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const links = yield* Effect.serviceOption(PeerLinkEvents.PeerLinkEvents);
  const projections = yield* ProjectionStore.ProjectionStoreV2;

  const wakeParent = Effect.fnUntraced(function* (
    task: OrchestrationV2Subagent,
    request: OrchestrationV2RuntimeRequest,
  ) {
    yield* engine.reconcileDelegatedTaskQuestion(task.threadId, task.id, request.id);
  });

  const cancelQueuedWakes = Effect.fnUntraced(function* (
    parentThreadId: ThreadId,
    requestIds: ReadonlyArray<RuntimeRequestId>,
  ) {
    const parent = yield* projections.getThreadRecords(parentThreadId, ["runs"]);
    for (const requestId of requestIds) {
      const key = `${parentThreadId}:${requestId}`;
      const messageId = MessageId.make(`message:delegated-task-question:${key}`);
      const queued = parent.runs.find(
        (run) => run.status === "queued" && run.userMessageId === messageId,
      );
      if (queued === undefined) continue;
      yield* engine
        .dispatch({
          type: "queued-run.cancel",
          commandId: CommandId.make(`server:delegated-task-question-cancel:${key}`),
          threadId: parentThreadId,
          runId: queued.id,
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("cancelling a delegated task question wake failed", {
              threadId: parentThreadId,
              requestId,
              cause,
            }),
          ),
        );
    }
  });

  // Runs for every domain event; most events return before any work is needed.
  const handle: DelegatedTaskQuestionReactor["Service"]["handle"] = Effect.fnUntraced(
    function* (event) {
      if (
        ((event.type === "run.updated" &&
          !["preparing", "starting", "running"].includes(event.payload.status)) ||
          event.type === "thread.archived" ||
          (event.type === "turn-item.updated" &&
            event.payload.type === "secret_request" &&
            event.payload.secretStatus === "cancelled")) &&
        Option.isSome(sql)
      ) {
        yield* RemoteSecretPersistence.reconcileObligations.pipe(
          Effect.provideService(SqlClient.SqlClient, sql.value),
          Effect.catchCause((cause) =>
            Effect.logWarning("Remote secret retirement reconciliation remains pending", { cause }),
          ),
        );
        if (Option.isSome(links)) yield* links.value.automationChanged;
      }
      if (event.type === "run.updated") {
        if (!["completed", "failed", "waiting"].includes(event.payload.status)) return;
        const parent = yield* projections.getThreadRecords(event.threadId, ["subagents"]);
        if (parent.thread.archivedAt !== null || parent.thread.deletedAt !== null) return;
        for (const task of parent.subagents) {
          if (
            task.origin !== "app_owned" ||
            task.runId !== event.payload.id ||
            task.completionWake !== "settled_only" ||
            task.completionDelivery?.state === "disposed"
          ) {
            continue;
          }
          if (task.childThreadId === null) {
            const request = task.remoteChild?.snapshot?.requests.find(
              (request) =>
                request.responseType === "live" &&
                request.kind !== "auth_refresh" &&
                request.kind !== "dynamic_tool_call",
            );
            if (request !== undefined)
              yield* engine.reconcileDelegatedTaskQuestion(
                task.threadId,
                task.id,
                request.requestId,
              );
            continue;
          }
          if (["completed", "failed", "cancelled", "interrupted"].includes(task.status)) {
            continue;
          }
          const child = yield* projections.getThreadRecords(task.childThreadId, [
            "runtimeRequests",
          ]);
          for (const request of child.runtimeRequests) {
            if (
              request.kind !== "user_input" ||
              request.status !== "pending" ||
              request.responseCapability.type !== "live"
            ) {
              continue;
            }
            yield* wakeParent(task, request).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("catching up a delegated task question failed", {
                  taskId: task.id,
                  requestId: request.id,
                  cause,
                }),
              ),
            );
          }
        }
        return;
      }
      if (event.type !== "runtime-request.updated" || event.payload.kind !== "user_input") {
        return;
      }
      const child = yield* projections.getThread(event.threadId);
      const parentThreadId = child.lineage.parentThreadId;
      if (parentThreadId === null) return;
      const request = yield* projections.getRuntimeRequest(event.threadId, event.payload.id);
      if (event.payload.status !== "pending" || request?.status !== "pending") {
        yield* cancelQueuedWakes(parentThreadId, [event.payload.id]);
        return;
      }
      const parent = yield* projections.getThreadRecords(parentThreadId, ["subagents", "runs"]);
      const { runs } = yield* projections.getThreadRecords(child.id, ["runs"]);
      const run =
        runs.find((run) => run.id === event.runId) ??
        runs.toSorted((a, b) => b.ordinal - a.ordinal)[0];
      const taskId =
        run?.delegatedTaskId ??
        (child.forkedFrom?.type === "node" ? child.forkedFrom.nodeId : undefined);
      const task = parent.subagents.find(
        (candidate) => candidate.origin === "app_owned" && candidate.id === taskId,
      );
      if (
        task === undefined ||
        parent.thread.archivedAt !== null ||
        parent.thread.deletedAt !== null
      ) {
        return;
      }
      yield* wakeParent(task, request);
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("waking the parent of a delegated task question failed", {
            threadId: event.threadId,
            cause,
          }),
        ),
      ),
  );

  // Every wake and cancel goes to the parent, so keying by it keeps a question's wake ahead of
  // its task's cancel while a busy parent's lock delays only that parent's reactions.
  const parentOf = (event: OrchestrationV2DomainEvent) => {
    switch (event.type) {
      case "run.updated":
        return Effect.succeed(event.threadId);
      case "subagent.updated":
        return Effect.succeed(event.payload.threadId);
      case "runtime-request.updated":
        return event.payload.kind === "user_input"
          ? projections
              .getThread(event.threadId)
              .pipe(Effect.map((child) => child.lineage.parentThreadId))
          : Effect.succeed(null);
      // Remote secret retirement reconciles on these; they have no parent to key by.
      case "thread.archived":
        return Effect.succeed(event.threadId);
      case "turn-item.updated":
        return Effect.succeed(
          event.payload.type === "secret_request" && event.payload.secretStatus === "cancelled"
            ? event.threadId
            : null,
        );
      default:
        return Effect.succeed(null);
    }
  };

  const start: DelegatedTaskQuestionReactor["Service"]["start"] = Effect.fn(
    "DelegatedTaskQuestionReactor.start",
  )(function* () {
    const parentReactions = yield* KeyedLock.makeRunner<ThreadId>();
    yield* forkParked(
      Stream.runForEach(engine.streamDomainEvents, (event) =>
        parentOf(event).pipe(
          Effect.flatMap((parentThreadId) =>
            parentThreadId === null
              ? Effect.void
              : parentReactions.run(parentThreadId, handle(event)),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("finding the parent of a delegated task question failed", {
              threadId: event.threadId,
              cause,
            }),
          ),
        ),
      ).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("delegated task question event stream failed", { cause }),
        ),
      ),
    );
  });

  return { start, handle } satisfies DelegatedTaskQuestionReactor["Service"];
});

export const layer = Layer.effect(DelegatedTaskQuestionReactor, make);
