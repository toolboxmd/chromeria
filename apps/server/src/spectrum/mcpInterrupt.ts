import {
  OrchestratorMcpFailure,
  type CommandId,
  type OrchestratorMcpThreadInterruptInput,
  type OrchestratorMcpThreadInterruptResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type { ThreadManagementServiceShape } from "../orchestration-v2/ThreadManagementService.ts";
import { readSpectrum } from "./store.ts";

/** Invoked only after the common MCP scope, caller-liveness and mode checks. */
export class SpectrumMcpInterrupt extends Context.Reference<{
  readonly interrupt: (
    threads: Pick<ThreadManagementServiceShape, "dispatch">,
    input: OrchestratorMcpThreadInterruptInput,
    commandId: CommandId,
  ) => Effect.Effect<OrchestratorMcpThreadInterruptResult | null, OrchestratorMcpFailure>;
}>("t3/spectrum/McpInterrupt", {
  defaultValue: () => ({ interrupt: () => Effect.succeed(null) }),
}) {}

export const layer = Layer.effect(
  SpectrumMcpInterrupt,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      interrupt: (
        threads: Pick<ThreadManagementServiceShape, "dispatch">,
        input: OrchestratorMcpThreadInterruptInput,
        commandId: CommandId,
      ) =>
        Effect.gen(function* () {
          const found = yield* readSpectrum(input.threadId).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          if (Option.isNone(found)) return null;
          if (input.runId !== undefined)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "Spectrum transcript threads have no provider run. Omit runId to stop their Drafters.",
            });
          yield* threads.dispatch({ type: "thread.stop", commandId, threadId: input.threadId });
          return { threadId: input.threadId, runId: null, status: "interrupt_requested" } as const;
        }).pipe(
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure"
              ? error
              : new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Unable to stop the Spectrum and its Drafters.",
                }),
          ),
        ),
    };
  }),
);
