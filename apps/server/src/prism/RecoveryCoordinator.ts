import {
  type OrchestrationV2ThreadProjection,
  type ServerSettingsError,
  type ThreadId,
} from "@t3tools/contracts";
import {
  latestExecutedRun,
  latestRootProviderFailure,
} from "@t3tools/shared/orchestrationV2ThreadError";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { SqlError } from "effect/sql/SqlError";
import * as Settings from "../serverSettings.ts";
import * as Store from "./RecoveryStore.ts";
import { sameModelSelection, type RecoveryRecord } from "./recoveryPolicy.ts";

export type RecoveryProjection = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "runs" | "turnItems" | "runtimeRequests"
>;

/** Retirement is observed by the caller and admitted atomically by #168's commit guard. */
export class RecoveryCoordinator extends Context.Service<
  RecoveryCoordinator,
  {
    readonly observe: (
      projection: RecoveryProjection,
      retiredOrIncomplete: boolean,
    ) => Effect.Effect<RecoveryRecord | null, SqlError | ServerSettingsError>;
    readonly holdsResult: (threadId: ThreadId) => Effect.Effect<boolean, SqlError>;
  }
>()("t3/prism/RecoveryCoordinator") {}

const make = Effect.gen(function* () {
  const store = yield* Store.RecoveryStore;
  const settings = yield* Settings.ServerSettingsService;
  const observe: RecoveryCoordinator["Service"]["observe"] = Effect.fn(
    "RecoveryCoordinator.observe",
  )(function* (projection, retiredOrIncomplete) {
    const run = latestExecutedRun(projection.runs);
    if (!run || projection.thread.creationSource !== "mcp") return null;
    const failure = latestRootProviderFailure(run, projection.turnItems);
    const stopped = projection.turnItems.some(
      (item) => item.type === "run_interrupt_request" && item.runId === run.id,
    );
    const blocked =
      retiredOrIncomplete ||
      stopped ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      projection.thread.settledOverride === "settled" ||
      projection.runtimeRequests.some((request) => request.status === "pending") ||
      !sameModelSelection(projection.thread.modelSelection, run.modelSelection);
    const recovery = projection.thread.limitRecovery;
    const persistedResetChoice =
      recovery?.runId === run.id && recovery.resetAt === failure?.resetAt
        ? recovery.autoResume
        : undefined;
    return yield* store.reconcile({
      previous: null,
      run,
      failure,
      stoppedOrRetired: blocked,
      autoResume: persistedResetChoice ?? (yield* settings.getSettings).autoResumeLimitedThreads,
    });
  });
  const holdsResult = Effect.fn("RecoveryCoordinator.holdsResult")(function* (threadId: ThreadId) {
    const record = yield* store.get(threadId);
    return record !== null && record.state !== "closed";
  });
  return RecoveryCoordinator.of({ observe, holdsResult });
});
export const layer = Layer.effect(RecoveryCoordinator, make).pipe(Layer.provide(Store.layer));
