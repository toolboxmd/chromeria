import { OrchestrationV2Command, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { readSpectrum } from "./store.ts";

const sameCommand = Schema.toEquivalence(OrchestrationV2Command);
/** Exact run cancellation never holds a newer run's queue. Admission is rebuilt on replay. */
export function spectrumCancellationPlan(
  threadId: ThreadId,
  generation: number,
  command: OrchestrationV2Command,
): ForkCommitPlan {
  return {
    guards: [
      Effect.gen(function* () {
        const found = yield* readSpectrum(threadId);
        if (
          Option.isNone(found) ||
          found.value.generation !== generation ||
          (command.type !== "run.interrupt" && command.type !== "queued-run.cancel") ||
          (command.type === "run.interrupt" && command.holdQueue === true) ||
          !found.value.outbox.some(
            (pending) =>
              pending.type !== "spectrum.transcript.append" && sameCommand(pending, command),
          )
        )
          return yield* new ForkCommitGuardRejected({ threadId, kind: "state_conflict" });
      }).pipe(
        Effect.mapError((error) =>
          error._tag === "ForkCommitGuardRejected"
            ? error
            : new ForkCommitGuardRejected({ threadId, kind: "storage_failure" }),
        ),
      ),
    ],
    mutations: [],
  };
}
