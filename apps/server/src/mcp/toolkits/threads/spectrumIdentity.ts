import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type { OrchestrationEngineShape } from "../../../orchestration/Services/OrchestrationEngine.ts";

/** Spectrum transcripts have no provider; only their hidden Drafters run turns. */
export const isSpectrumThreadId = (id: string) => id.startsWith("spectrum.");
export const isSpectrumParticipantId = (id: string) => id.startsWith("sub.spectrum.");

/** Names the one Drafter request, by its event sequence, that a Spectrum stop may end. */
export const spectrumStopSuffix = (threadId: string, requestSequence: number) =>
  `stop-unbound:${threadId}:${requestSequence}`;

// `server:` + Spectrum's step key + the suffix above. Earlier builds omitted the sequence.
const SPECTRUM_STOP =
  /^server:spectrum:spectrum\.[^:]+:\d+:-?\d+:(?:discussion|broadcast):\d+:stop-unbound:([^:]+)(?::(\d+))?$/;

/**
 * Whether the engine must refuse a Spectrum stop: its Drafter has a newer request than the one
 * it names. Commands are decided one at a time, so no request can persist between this read and
 * the stop's event, and the reactor handles a later request only after the stop has finished.
 * A stop without a named request cannot tell an old send from a successor and is always refused.
 * Every other stop returns false.
 */
export const spectrumStopSuperseded = (
  engine: Pick<OrchestrationEngineShape, "readThreadEvents">,
  command: { readonly commandId: string; readonly threadId: string },
) => {
  const match = SPECTRUM_STOP.exec(command.commandId);
  if (!match || match[1] !== command.threadId) return Effect.succeed(false);
  if (match[2] === undefined) return Effect.succeed(true);
  return engine
    .readThreadEvents({
      threadId: ThreadId.make(command.threadId),
      fromSequenceExclusive: Number(match[2]),
      toSequenceInclusive: Number.MAX_SAFE_INTEGER,
      limit: Number.MAX_SAFE_INTEGER,
    })
    .pipe(
      Stream.filter((item) => item.type === "thread.turn-start-requested"),
      Stream.runHead,
      Effect.map(Option.isSome),
    );
};
