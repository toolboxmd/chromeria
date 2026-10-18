import {
  OrchestratorMcpFailure,
  type CommandId,
  type ThreadId,
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
  /** Stops a Spectrum task's transcript, which has no run of its own; false for any other thread. */
  readonly stop: (
    threads: Pick<ThreadManagementServiceShape, "dispatch">,
    input: { readonly threadId: ThreadId; readonly reason?: string | undefined },
    commandId: CommandId,
  ) => Effect.Effect<boolean, OrchestratorMcpFailure>;
}>("t3/spectrum/McpInterrupt", {
  defaultValue: () => ({
    interrupt: () => Effect.succeed(null),
    stop: () => Effect.succeed(false),
  }),
}) {}

export const layer = Layer.effect(
  SpectrumMcpInterrupt,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const failure = () =>
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "Unable to stop the Spectrum and its Drafters.",
      });
    const stop = (
      threads: Pick<ThreadManagementServiceShape, "dispatch">,
      input: { readonly threadId: ThreadId; readonly reason?: string | undefined },
      commandId: CommandId,
    ) =>
      Effect.gen(function* () {
        const found = yield* readSpectrum(input.threadId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        if (Option.isNone(found)) return false;
        yield* threads.dispatch({
          type: "thread.stop",
          commandId,
          threadId: input.threadId,
          ...(input.reason === undefined ? {} : { reason: input.reason }),
        });
        return true;
      }).pipe(Effect.mapError(failure));
    return {
      stop,
      interrupt: (
        threads: Pick<ThreadManagementServiceShape, "dispatch">,
        input: OrchestratorMcpThreadInterruptInput,
        commandId: CommandId,
      ) =>
        Effect.gen(function* () {
          if (input.runId !== undefined) {
            const found = yield* readSpectrum(input.threadId).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
              Effect.mapError(failure),
            );
            if (Option.isNone(found)) return null;
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "Spectrum transcript threads have no provider run. Omit runId to stop their Drafters.",
            });
          }
          return (yield* stop(threads, { threadId: input.threadId }, commandId))
            ? ({ threadId: input.threadId, runId: null, status: "interrupt_requested" } as const)
            : null;
        }),
    };
  }),
);
