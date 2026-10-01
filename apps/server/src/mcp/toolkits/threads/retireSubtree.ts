import { CommandId, EventId, ThreadId } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { RETIRE_SUBTREE_KIND, retirementFrom } from "../../../orchestration/ThreadRetirement.ts";
import { isDescendantThreadId } from "./subagentThreadId.ts";

/** The command receipt installs the guard; provider stop acknowledgements settle the call. */
export const retireSubtree = Effect.fn("ThreadsToolkit.retireSubtree")(function* (
  threadId: ThreadId,
  commandId: CommandId,
  eventId: EventId,
  createdAt: string,
) {
  const engine = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const subtree = (id: string) => id === threadId || isDescendantThreadId(id, threadId);
      const events = yield* engine.subscribeDomainEvents;
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId,
        threadId,
        activity: {
          id: eventId,
          kind: RETIRE_SUBTREE_KIND,
          tone: "info",
          summary: "Retired subtree",
          payload: {},
          turnId: null,
          createdAt,
        },
        createdAt,
      });
      // Sample after the receipt: a spawn decided before retirement is included atomically.
      const pending = new Map<string, string>();
      let hadActiveRun = false;
      let turnId: string | null = null;
      for (const thread of (yield* snapshots.getCommandReadModel()).threads.filter((thread) =>
        subtree(thread.id),
      )) {
        const state = yield* engine.getThreadRetirement(thread.id);
        hadActiveRun ||= state?.hadActiveRun ?? false;
        if (thread.id === threadId) turnId = state?.activeTurnId ?? null;
        if (state?.pendingStop) pending.set(thread.id, state.stopAckCommandId);
      }
      const settled =
        pending.size === 0 ||
        Option.isSome(
          yield* events.pipe(
            Stream.filter((event) => {
              if (event.type !== "thread.activity-appended") return false;
              const state = retirementFrom(event.payload.activity);
              if (!state || Option.isNone(state) || state.value.pendingStop) return false;
              if (pending.get(event.aggregateId) === state.value.stopAckCommandId)
                pending.delete(event.aggregateId);
              return pending.size === 0;
            }),
            Stream.runHead,
            Effect.timeoutOption(Duration.seconds(30)),
            Effect.map(Option.flatten),
          ),
        );
      const target = Option.getOrUndefined(yield* snapshots.getThreadShellById(threadId));
      return {
        threadId,
        turnId,
        status: settled
          ? hadActiveRun
            ? ("interrupted" as const)
            : ("no_active_run" as const)
          : ("interrupt_requested" as const),
        session: target?.session ?? null,
      };
    }),
  );
});
