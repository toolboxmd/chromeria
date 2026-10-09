import {
  OrchestratorMcpFailure,
  type CommandId,
  type MessageId,
  type ThreadId,
  type OrchestratorMcpThreadSendInput,
  type OrchestratorMcpThreadSendResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type { ThreadManagementServiceShape } from "../orchestration-v2/ThreadManagementService.ts";
import { readSpectrum } from "./store.ts";

export class SpectrumMcpSend extends Context.Reference<{
  readonly send: (
    threads: Pick<ThreadManagementServiceShape, "dispatch">,
    input: OrchestratorMcpThreadSendInput,
    commandId: CommandId,
    messageId: MessageId,
    senderThreadId?: ThreadId,
  ) => Effect.Effect<OrchestratorMcpThreadSendResult | null, OrchestratorMcpFailure>;
}>("t3/spectrum/McpSend", { defaultValue: () => ({ send: () => Effect.succeed(null) }) }) {}

export const layer = Layer.effect(
  SpectrumMcpSend,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      send: (
        threads: Pick<ThreadManagementServiceShape, "dispatch">,
        input: OrchestratorMcpThreadSendInput,
        commandId: CommandId,
        messageId: MessageId,
        senderThreadId?: ThreadId,
      ) =>
        Effect.gen(function* () {
          const found = yield* readSpectrum(input.threadId).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          if (Option.isNone(found)) return null;
          if (input.mode === "steer" || input.mode === "restart") {
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Spectrum transcript threads have no provider turn to steer or restart.",
            });
          }
          yield* threads.dispatch({
            type: "message.dispatch",
            commandId,
            threadId: input.threadId,
            messageId,
            text: input.message,
            attachments: [],
            ...(senderThreadId === undefined ? {} : { senderThreadId }),
            createdBy: "agent",
            creationSource: "mcp",
            dispatchMode: {
              type: input.mode === "queue" ? "queue_after_active" : "start_immediately",
            },
          });
          return {
            threadId: input.threadId,
            messageId,
            runId: null,
            status: "idle",
            delivery: "transcript",
          } as const;
        }).pipe(
          Effect.mapError((error) =>
            error._tag === "OrchestratorMcpFailure"
              ? error
              : new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Unable to append to the Spectrum transcript.",
                }),
          ),
        ),
    };
  }),
);
