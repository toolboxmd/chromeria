import {
  type CommandId,
  type MessageId,
  type OrchestrationCommand,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { OrchestrationEngineShape } from "../../../orchestration/Services/OrchestrationEngine.ts";

/**
 * Normal server-origin turn dispatch, preserving thread modes and optional serialized idle admission.
 * Automatic callers keep server: IDs; server:mcp-threads-message: is reserved for explicit recovery.
 */
export const makeThreadTurnSender =
  <E, R>(deps: {
    readonly dispatch: (
      command: OrchestrationCommand,
      options?: Parameters<OrchestrationEngineShape["dispatch"]>[1],
    ) => Effect.Effect<unknown, E, R>;
    readonly commandId: Effect.Effect<CommandId>;
    readonly messageId: Effect.Effect<MessageId>;
    readonly now: Effect.Effect<string>;
  }) =>
  (
    thread: OrchestrationThreadShell,
    text: string,
    commandId?: CommandId,
    idleOnly = false,
    idleAdmission?: Effect.Effect<boolean>,
  ) =>
    Effect.gen(function* () {
      const createdAt = yield* deps.now;
      yield* deps.dispatch(
        {
          type: "thread.turn.start",
          ...(idleOnly
            ? {
                idleGuard: {
                  latestTurnId: thread.latestTurn?.turnId ?? null,
                  updatedAt: thread.updatedAt,
                },
              }
            : {}),
          commandId: commandId ?? (yield* deps.commandId),
          threadId: thread.id,
          message: { messageId: yield* deps.messageId, role: "user", text, attachments: [] },
          modelSelection: thread.modelSelection,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
          createdAt,
        },
        idleAdmission === undefined ? undefined : { idleAdmission },
      );
    });
