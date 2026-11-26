import {
  AuthOrchestrationOperateScope,
  EnvironmentAuthorizationError,
  SpectrumStopError,
  type SpectrumStopInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import { OrchestratorV2 } from "../orchestration-v2/Orchestrator.ts";
import { readSpectrum } from "./store.ts";

/** Narrow authenticated GUI bridge: registration is read from durable state, never the client flag. */
export const stopSpectrum = Effect.fn("Spectrum.stopHuman")(function* (
  session: AuthenticatedSession | undefined,
  input: SpectrumStopInput,
) {
  if (
    session === undefined ||
    session.subject === "mcp-client" ||
    !session.scopes.includes(AuthOrchestrationOperateScope)
  )
    return yield* new EnvironmentAuthorizationError({
      requiredScope: AuthOrchestrationOperateScope,
      message:
        "Stopping a Spectrum requires an authenticated human session with permission to operate threads.",
    });
  return yield* Effect.gen(function* () {
    if (Option.isNone(yield* readSpectrum(input.threadId)))
      return yield* new SpectrumStopError({ message: "This thread is not a Spectrum." });
    const orchestrator = yield* OrchestratorV2;
    // Do not reject retired rows: an exact-id replay must reach the common receipt dedupe.
    const result = yield* orchestrator.dispatch({ type: "thread.stop", ...input });
    return { sequence: result.sequence };
  }).pipe(
    Effect.mapError((cause) =>
      cause._tag === "SpectrumStopError"
        ? cause
        : new SpectrumStopError({ message: "Could not stop the Spectrum. Refresh and try again." }),
    ),
  );
});
