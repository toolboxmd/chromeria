import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, OrchestratorMcpThreadSendResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Plans from "./commandPlan.ts";
import * as Launch from "./LaunchService.ts";
import * as Mcp from "./mcpSend.ts";
import { base, input, setup } from "./controllerTestkit.ts";

const decodeSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult);
const runtime = Layer.fresh(
  Layer.mergeAll(
    Plans.layer,
    Mcp.layer.pipe(Layer.provide(base)),
    Launch.layer.pipe(Layer.provideMerge(base)),
  ),
);
it.effect(
  "MCP Spectrum sends retain provenance and stable identities without a provider run, including replay",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threads = { dispatch: orchestrator.dispatch };
      const send = yield* Mcp.SpectrumMcpSend;
      const request = {
        projectId: (yield* (yield* Projection.ProjectionStoreV2).getThread(state.threadId))
          .projectId,
        commandId: CommandId.make("mcp:send"),
        messageId: MessageId.make("mcp:message"),
        threadId: state.threadId,
        senderThreadId: input.callerThreadId,
        text: "Agent joins verbatim",
        attachments: [],
        mode: "auto" as const,
        createdBy: "agent" as const,
        creationSource: "mcp" as const,
      };
      const expected = {
        threadId: state.threadId,
        messageId: request.messageId,
        runId: null,
        status: "idle" as const,
        delivery: "transcript" as const,
      };
      assert.deepStrictEqual(
        yield* send.send(
          threads,
          { threadId: request.threadId, message: request.text, mode: request.mode },
          request.commandId,
          request.messageId,
          request.senderThreadId,
        ),
        expected,
      );
      assert.deepStrictEqual(
        yield* send.send(
          threads,
          { threadId: request.threadId, message: request.text, mode: request.mode },
          request.commandId,
          request.messageId,
          request.senderThreadId,
        ),
        expected,
      );
      assert.deepStrictEqual(yield* decodeSendResult(expected), expected);
      const rows = yield* (yield* Projection.ProjectionStoreV2).getThreadRecords(state.threadId, [
        "messages",
        "runs",
      ]);
      assert.deepStrictEqual(rows.runs, []);
      assert.strictEqual(
        rows.messages.filter((message) => message.id === request.messageId).length,
        1,
      );
      assert.strictEqual(rows.messages[0]!.creationSource, "mcp");
      assert.strictEqual(rows.messages[0]!.senderThreadId, input.callerThreadId);
      // The fork does not intercept normal targets, and cannot invent a steerable transcript run.
      assert.isNull(
        yield* send.send(
          threads,
          { threadId: input.callerThreadId, message: request.text },
          request.commandId,
          request.messageId,
          request.senderThreadId,
        ),
      );
      const refused = yield* send
        .send(
          threads,
          { threadId: request.threadId, message: request.text, mode: "steer" },
          request.commandId,
          request.messageId,
          request.senderThreadId,
        )
        .pipe(Effect.flip);
      assert.strictEqual(refused.code, "invalid_request");
    }).pipe(Effect.provide(runtime)),
);
