import type {
  OrchestrationV2Run,
  OrchestrationV2StoredEvent,
  RunId,
  ThreadId,
  ServerSettingsError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import type { ProjectionStoreV2Error } from "../orchestration-v2/ProjectionStore.ts";
import * as History from "../prism/RecoveryHistory.ts";
import * as Hooks from "../prism/RecoveryHooks.ts";

import { RecoveryWaitSignals } from "./recoveryWaitSignals.ts";

export interface RecoveryWaitAccess {
  readonly read: (
    threadId: ThreadId,
    runId: RunId,
  ) => Effect.Effect<
    History.RecoveryState,
    SqlError | ServerSettingsError | ProjectionStoreV2Error
  >;
  readonly subscribe: Effect.Effect<Stream.Stream<ThreadId>, never, Scope.Scope>;
}

export class RecoveryWaitState extends Context.Reference<RecoveryWaitAccess>(
  "t3/peer/RecoveryWaitState",
  {
    defaultValue: () => ({
      read: () =>
        Effect.succeed({ decision: null, admittedContinuations: [], pendingRecovery: null }),
      subscribe: Effect.succeed(Stream.never),
    }),
  },
) {}

export const layer = Layer.effect(
  RecoveryWaitState,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const hooks = yield* Hooks.RecoveryHooks;
    const signals = yield* RecoveryWaitSignals;
    return {
      subscribe: signals.subscribe,
      read: Effect.fn("RecoveryWaitState.read")((threadId: ThreadId, runId: RunId) =>
        hooks
          .holdsFinalization(threadId, runId)
          .pipe(
            Effect.andThen(
              History.readRecoveryState(runId).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
            ),
            Effect.provideService(RecoveryWaitSignals, signals),
          ),
      ),
    };
  }),
);

const terminal = (run: OrchestrationV2Run) =>
  run.status === "completed" ||
  run.status === "failed" ||
  run.status === "cancelled" ||
  run.status === "interrupted" ||
  run.status === "rolled_back";

/** One budget follows only recorded recovery successors, including an already-completed chain. */
export const waitForRecovery = <E>(
  input: {
    readonly threadId: ThreadId;
    readonly runId?: RunId;
    readonly timeoutMs: number;
    readonly readRuns: Effect.Effect<ReadonlyArray<OrchestrationV2Run>, E>;
    readonly latest: (runs: ReadonlyArray<OrchestrationV2Run>) => OrchestrationV2Run | undefined;
    readonly events: Stream.Stream<OrchestrationV2StoredEvent, E>;
    readonly missing: (runId: RunId) => E;
    readonly loadError: (cause: unknown) => E;
    readonly result?: Effect.Effect<OrchestrationV2Run | undefined, E>;
  },
  recovery: RecoveryWaitAccess,
  restart?: {
    readonly latest: (
      runs: ReadonlyArray<OrchestrationV2Run>,
      run: OrchestrationV2Run,
    ) => OrchestrationV2Run;
    readonly pending: (run: OrchestrationV2Run) => Effect.Effect<boolean, E>;
    readonly await: (run: OrchestrationV2Run) => Effect.Effect<void, E>;
  },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      // Subscribe before reading so a decision-only change cannot fall in a gap.
      const changes = yield* recovery.subscribe;
      const runs = yield* input.readRuns;
      const selected =
        input.runId === undefined ? input.latest(runs) : runs.find((run) => run.id === input.runId);
      if (selected === undefined) {
        if (input.runId !== undefined) return yield* Effect.fail(input.missing(input.runId));
        return { threadId: input.threadId, run: null, timedOut: false };
      }
      let selectedRunId = selected.id;
      const resolve = Effect.fn("recoveryWait.resolve")(function* (
        snapshot?: ReadonlyArray<OrchestrationV2Run>,
        resultRunId?: RunId,
      ) {
        let current = snapshot ?? (yield* input.readRuns);
        let currentRunId = resultRunId ?? selectedRunId;
        const seen = new Set<RunId>();
        for (;;) {
          let run = current.find((candidate) => candidate.id === currentRunId);
          if (run === undefined) return yield* Effect.fail(input.missing(currentRunId));
          run = restart?.latest(current, run) ?? run;
          currentRunId = run.id;
          if (resultRunId === undefined) selectedRunId = currentRunId;
          if (restart !== undefined && terminal(run) && (yield* restart.pending(run)))
            return { type: "native-continuation-pending" as const, run };
          if (run.status !== "failed" && run.status !== "interrupted")
            return { type: terminal(run) ? ("done" as const) : ("pending" as const), run };
          const state = yield* recovery
            .read(input.threadId, run.id)
            .pipe(Effect.mapError(input.loadError));
          const successor =
            state.admittedContinuations.find((entry) => entry.threadId === input.threadId)
              ?.successorRunId ??
            (state.decision?.status === "decided" && state.decision.outcome === "retried"
              ? state.decision.successorRunId
              : undefined);
          if (successor !== undefined) {
            if (seen.has(successor))
              return yield* Effect.fail(input.loadError("Recovery successor cycle"));
            seen.add(run.id);
            let next = current.find((candidate) => candidate.id === successor);
            if (next === undefined) {
              current = yield* input.readRuns;
              next = current.find((candidate) => candidate.id === successor);
            }
            if (next === undefined) return yield* Effect.fail(input.missing(successor));
            currentRunId = next.id;
            if (resultRunId === undefined) selectedRunId = currentRunId;
            continue;
          }
          return {
            run,
            type:
              state.pendingRecovery === null && state.decision?.status !== "pending"
                ? ("done" as const)
                : ("pending" as const),
          };
        }
      });
      const evaluate = Effect.fn("recoveryWait.evaluate")(function* (
        snapshot?: ReadonlyArray<OrchestrationV2Run>,
      ) {
        const current = yield* resolve(snapshot);
        if (current.type !== "done" || input.result === undefined)
          return { ...current, resultRun: undefined, waitRun: current.run };
        const candidate = yield* input.result;
        if (candidate === undefined)
          return {
            run: current.run,
            type: "pending" as const,
            resultRun: undefined,
            waitRun: current.run,
          };
        const result = yield* resolve(undefined, candidate.id);
        return {
          run: current.run,
          type: result.type,
          resultRun: result.type === "done" ? result.run : undefined,
          waitRun: result.run,
        };
      });
      const answer = (state: Effect.Success<ReturnType<typeof evaluate>>) => ({
        threadId: input.threadId,
        run: state.run,
        timedOut: state.type !== "done",
        ...(state.resultRun === undefined ? {} : { resultRun: state.resultRun }),
      });
      const initial = yield* evaluate(runs);
      if (initial.type === "done") return answer(initial);
      const waited = Option.flatten(
        yield* Stream.merge(
          Stream.make(initial),
          Stream.merge(
            input.events.pipe(
              Stream.filter((stored) => {
                const event = stored.event;
                return (
                  input.result !== undefined ||
                  (event.type !== "run.updated" && event.type !== "run.created") ||
                  event.payload.id === selectedRunId ||
                  event.payload.restartContinuationOfRunId === selectedRunId ||
                  event.payload.forkPrismContinuationSourceRunId === selectedRunId
                );
              }),
            ),
            changes.pipe(Stream.filter((threadId) => threadId === input.threadId)),
          ).pipe(Stream.mapEffect(() => evaluate())),
        ).pipe(
          Stream.switchMap((state) =>
            state.type === "native-continuation-pending" && restart !== undefined
              ? Stream.merge(
                  Stream.make(state),
                  Stream.fromEffect(
                    restart.await(state.waitRun).pipe(Effect.andThen(() => evaluate())),
                  ),
                )
              : Stream.make(state),
          ),
          Stream.filter((result) => result.type === "done"),
          Stream.runHead,
          Effect.timeoutOption(Duration.millis(Math.max(1, input.timeoutMs))),
        ),
      );
      if (Option.isSome(waited)) return answer(waited.value);
      return answer(yield* evaluate());
    }),
  );
