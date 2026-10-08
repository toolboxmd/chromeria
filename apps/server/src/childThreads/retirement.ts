import type {
  CommandId,
  OrchestrationV2AppThread,
  OrchestrationV2Command,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "./ForkCommitPlan.ts";

export type RetirementThread = Pick<
  OrchestrationV2AppThread,
  "id" | "lineage" | "forkRetirement" | "forkResumedRetirements"
>;
export type RetirementState = {
  readonly tokens: ReadonlyArray<CommandId>;
  readonly retired: boolean;
  readonly complete: boolean;
};

/** Missing ancestors are distinct from retirement; no unknown token is ever acknowledged. */
export function retirementState(
  thread: RetirementThread,
  ancestors: ReadonlyMap<ThreadId, RetirementThread>,
): RetirementState {
  const tokens: CommandId[] = [];
  const seen = new Set<ThreadId>();
  let current: RetirementThread | undefined = thread;
  let complete = true;
  while (current !== undefined) {
    if (seen.has(current.id)) {
      complete = false;
      break;
    }
    seen.add(current.id);
    if (current.forkRetirement !== undefined) tokens.push(current.forkRetirement.token);
    if (
      current.lineage.relationshipToParent !== "subagent" ||
      current.lineage.parentThreadId === null
    )
      break;
    current = ancestors.get(current.lineage.parentThreadId);
    if (current === undefined) complete = false;
  }
  return {
    tokens: [...new Set(tokens)],
    retired: tokens.some((token) => !thread.forkResumedRetirements?.includes(token)),
    complete,
  };
}

export const readRetirementState = Effect.fn("childThreads.readRetirementState")(function* (
  threadId: ThreadId,
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const thread = yield* projections.getThread(threadId);
  const ancestors = new Map<ThreadId, RetirementThread>();
  let current = thread;
  const seen = new Set<ThreadId>([thread.id]);
  while (
    current.lineage.relationshipToParent === "subagent" &&
    current.lineage.parentThreadId !== null
  ) {
    const parentId = current.lineage.parentThreadId;
    if (seen.has(parentId)) break;
    seen.add(parentId);
    const parent = yield* projections
      .getThread(parentId)
      .pipe(
        Effect.map(Option.some),
        Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeedNone }),
      );
    if (parent._tag === "None") break;
    current = parent.value;
    ancestors.set(current.id, current);
  }
  return retirementState(thread, ancestors);
});

export const isThreadRetired = Effect.fn("childThreads.isThreadRetired")(function* (
  threadId: ThreadId,
) {
  const state = yield* readRetirementState(threadId);
  return state.retired || !state.complete;
});

export function explicitMessage(
  command: Extract<OrchestrationV2Command, { type: "message.dispatch" }>,
) {
  return (
    (command.creationSource === "mcp" ||
      ((command.creationSource === "web" || command.creationSource === "mobile") &&
        command.createdBy === "user")) &&
    command.notification === undefined &&
    command.delegatedCompletion === undefined &&
    command.usageLimitContinuationOfRunId === undefined &&
    command.restartContinuationOfRunId === undefined
  );
}
export function humanMessage(
  command: Extract<OrchestrationV2Command, { type: "message.dispatch" }>,
) {
  return (
    explicitMessage(command) &&
    command.createdBy === "user" &&
    (command.creationSource === "web" || command.creationSource === "mobile")
  );
}

/** The constructor captures immutable admission data only, resolves projection inside the transaction. */
export function retirementAdmission(input: {
  readonly threadId: ThreadId;
  readonly expectedTokens?: ReadonlyArray<CommandId>;
  readonly resume?: boolean;
  readonly humanOverride?: boolean;
  readonly admittedHumanRunId?: RunId;
}): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const state = yield* readRetirementState(input.threadId).pipe(
          Effect.mapError(
            () =>
              new ForkCommitGuardRejected({ threadId: input.threadId, kind: "storage_failure" }),
          ),
        );
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const thread = yield* projections
          .getThread(input.threadId)
          .pipe(
            Effect.mapError(
              () =>
                new ForkCommitGuardRejected({ threadId: input.threadId, kind: "storage_failure" }),
            ),
          );
        const admittedHuman =
          input.admittedHumanRunId !== undefined &&
          thread.forkLineageOverride?.runId === input.admittedHumanRunId;
        if (!state.complete && !input.humanOverride && !admittedHuman)
          return yield* new ForkCommitGuardRejected({
            threadId: input.threadId,
            kind: "lineage_incomplete",
          });
        if (
          input.expectedTokens !== undefined &&
          (state.tokens.length !== input.expectedTokens.length ||
            state.tokens.some((token) => !input.expectedTokens?.includes(token)))
        )
          return yield* new ForkCommitGuardRejected({
            threadId: input.threadId,
            kind: "state_conflict",
          });
        if (state.retired && !input.resume)
          return yield* new ForkCommitGuardRejected({ threadId: input.threadId, kind: "retired" });
      }),
    ],
    mutations: [],
  };
}

export type PropagatedStop = {
  readonly ancestorThreadId: ThreadId;
  readonly originalToken: CommandId;
};

/** A delayed internal Stop cannot revoke a later explicit admission or replace a newer Stop. */
export function propagatedStopAdmission(
  threadId: ThreadId,
  provenance: PropagatedStop,
): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const ancestor = yield* projections
          .getThread(provenance.ancestorThreadId)
          .pipe(
            Effect.map(Option.some),
            Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeedNone }),
          );
        const child = yield* projections.getThread(threadId);
        const state = yield* readRetirementState(threadId);
        if (
          Option.isNone(ancestor) ||
          ancestor.value.forkRetirement?.token !== provenance.originalToken ||
          !state.tokens.includes(provenance.originalToken) ||
          child.forkResumedRetirements?.includes(provenance.originalToken)
        )
          return "accept_noop" as const;
      }).pipe(
        Effect.mapError(() => new ForkCommitGuardRejected({ threadId, kind: "storage_failure" })),
      ),
    ],
    mutations: [],
  };
}

export function subagentDescendants(
  threadId: ThreadId,
  threads: ReadonlyArray<Pick<RetirementThread, "id" | "lineage">>,
): ReadonlyArray<ThreadId> {
  const found = new Set<ThreadId>([threadId]);
  let added = true;
  while (added) {
    added = false;
    for (const thread of threads) {
      if (
        thread.lineage.relationshipToParent === "subagent" &&
        thread.lineage.parentThreadId !== null &&
        found.has(thread.lineage.parentThreadId) &&
        !found.has(thread.id)
      ) {
        found.add(thread.id);
        added = true;
      }
    }
  }
  found.delete(threadId);
  return [...found];
}
