import {
  CommandId,
  MessageId,
  OrchestratorMcpFailure,
  OrchestratorMcpThreadSendResult,
  type RunId,
  type PeerReturnPair,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";

/** The authority owner verifies the pair before entering ordinary thread intake. */
export class PeerNoticeDelivery extends Context.Service<
  PeerNoticeDelivery,
  {
    readonly deliver: (
      pair: PeerReturnPair,
      runId: RunId,
      text: string,
      mode: "auto" | "queue",
      identity: string,
    ) => Effect.Effect<OrchestratorMcpThreadSendResult, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerNoticeDelivery") {}

export const unavailable: PeerNoticeDelivery["Service"] = {
  deliver: () =>
    Effect.fail(
      new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The parent notice service is unavailable.",
      }),
    ),
};

export const layer = Layer.effect(
  PeerNoticeDelivery,
  Effect.gen(function* () {
    const threads = yield* Threads.ThreadManagementService;
    return PeerNoticeDelivery.of({
      deliver: Effect.fn("PeerNoticeDelivery.deliver")(
        function* (pair, runId, text, mode, identity) {
          const parent = yield* threads.getThreadShell(pair.parentThreadId);
          if (!parent)
            return yield* new OrchestratorMcpFailure({
              code: "thread_not_found",
              message: "The parent thread no longer exists.",
            });
          const sent = yield* threads.sendToThread({
            projectId: parent.projectId,
            threadId: parent.id,
            commandId: CommandId.make(`peer-return:${identity}`),
            messageId: MessageId.make(`peer-return:${identity}`),
            text,
            attachments: [],
            mode,
            createdBy: "agent",
            creationSource: "mcp",
            remoteDescendant: {
              taskId: pair.taskId,
              environmentId: pair.childEnvironmentId,
              threadId: pair.childThreadId,
              runId,
            },
          });
          return {
            threadId: parent.id,
            messageId: sent.message.id,
            runId: sent.run.id,
            status: sent.run.status,
            delivery: sent.delivery,
          };
        },
        Effect.mapError((error) =>
          error._tag === "OrchestratorMcpFailure"
            ? error
            : new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The parent notice was not accepted.",
              }),
        ),
      ),
    });
  }),
);
