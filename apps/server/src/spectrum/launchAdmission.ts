import { OrchestrationV2Command, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ForkCommitGuardRejected, type ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { retirementAdmission } from "../childThreads/retirement.ts";
import { readSpectrum } from "./store.ts";

const sameCommand = Schema.toEquivalence(OrchestrationV2Command);

/** Rebuilt from durable data for every dispatch/replay, including after process restart. */
export function spectrumLaunchAdmission(
  threadId: ThreadId,
  generation: number,
  command: OrchestrationV2Command,
): ForkCommitPlan {
  return {
    guards: [
      ...retirementAdmission({ threadId }).guards,
      Effect.gen(function* () {
        const found = yield* readSpectrum(threadId);
        if (
          Option.isNone(found) ||
          found.value.status !== "active" ||
          found.value.generation !== generation ||
          found.value.round?.generation !== generation ||
          !found.value.round.slots.some((slot) =>
            command.type === "message.dispatch"
              ? slot.commandId === command.commandId &&
                slot.messageId === command.messageId &&
                slot.threadId === command.threadId &&
                slot.runId === null
              : command.type === "run.interrupt" &&
                slot.threadId === command.threadId &&
                slot.runId === command.runId,
          ) ||
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
