import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProviderEventIngestor from "../orchestration-v2/ProviderEventIngestor.ts";
import * as StreamClock from "./streamClock.ts";

/** Accepted writes refresh liveness. Routed terminals may intentionally produce no writes. */
export const layer = Layer.effect(
  ProviderEventIngestor.ProviderEventIngestorV2,
  Effect.gen(function* () {
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const clock = yield* StreamClock.StreamClock;
    return ProviderEventIngestor.ProviderEventIngestorV2.of({
      normalize: ingestor.normalize,
      ingestNormalized: (input) =>
        ingestor
          .ingestNormalized(input)
          .pipe(
            Effect.tap((stored) =>
              stored.length > 0 || input.event.type === "turn.terminal"
                ? Effect.flatMap(StreamClock.StreamClockAttempt, (attemptId) =>
                    clock.observe(input, attemptId),
                  )
                : Effect.void,
            ),
          ),
    });
  }),
);
