import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import { assert, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  CommandId,
  EnvironmentId,
  MessageId,
  OrchestratorMcpThreadSendResult,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer } from "effect/ai";
import * as AssetSigner from "../assets/AssetSigner.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Plans from "./commandPlan.ts";
import * as Launch from "./LaunchService.ts";
import * as Mcp from "./mcpSend.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { readSpectrum } from "./store.ts";

const decodeSendResult = Schema.decodeUnknownEffect(OrchestratorMcpThreadSendResult);
const runtime = Layer.fresh(
  Layer.mergeAll(
    Plans.layer.pipe(Layer.provide(base)),
    Mcp.layer.pipe(Layer.provide(base)),
    Launch.layer.pipe(
      Layer.provide(Layer.mock(BareLaunch.BareLaunch)({})),
      Layer.provideMerge(base),
    ),
  ),
);

it.effect.each(["spectrum", "ordinary"] as const)(
  "%s attachment intake acknowledges persisted bytes",
  (target) =>
    Effect.gen(function* () {
      yield* setup;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const threadId = target === "spectrum" ? state.threadId : input.callerThreadId;
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      const id = ChatAttachmentId.make(createPendingAttachmentId()!);
      const bytes = new Uint8Array([1, 2, 3]);
      yield* fs.writeFile(`${config.attachmentsDir}/${id}.png`, bytes);
      const server = yield* McpServer.McpServer;
      const client = McpSchema.McpServerClient.of({
        clientId: 1,
        protocolVersion: "2025-06-18",
        clientCapabilities: {},
        clientInfo: { name: "spectrum-attachments", version: "1" },
        initializePayload: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "spectrum-attachments", version: "1" },
        },
        getClient: Effect.die("unused"),
      });
      const scope: Invocation.McpInvocationScope = {
        environmentId: EnvironmentId.make("spectrum-attachment-environment"),
        requestNamespace: "spectrum-attachment-client",
        capabilities: new Set(["orchestration"]),
        issuedAt: 0,
        thread: undefined,
        client: { sessionId: "spectrum-attachment-client", label: "Test", access: "full-access" },
      };
      const send = (key: string, invocation = scope) =>
        server
          .callTool({
            name: "t3_thread_send_attachments",
            arguments: {
              threadId,
              message: "Inspect the attached proof",
              attachments: [
                { type: "image", id, name: "proof.png", mimeType: "image/png", sizeBytes: 3 },
              ],
              clientRequestId: key,
            },
          })
          .pipe(
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.provideService(Invocation.McpInvocationContext, invocation),
          );
      const denied = yield* send("denied", {
        ...scope,
        client: { ...scope.client!, access: "read-only" },
      });
      assert.isTrue(denied.isError);
      const projections = yield* Projection.ProjectionStoreV2;
      assert.deepStrictEqual(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages,
        [],
      );
      const call = yield* send("spectrum-attachment");
      const rows = yield* projections.getThreadRecords(threadId, [
        "messages",
        "runs",
        "providerSessions",
        "providerTurns",
      ]);
      assert.strictEqual(rows.messages.length, 1);
      const message = rows.messages[0]!;
      assert.strictEqual(message.text, "Inspect the attached proof");
      assert.strictEqual(message.attachments.length, 1);
      assert.deepStrictEqual(message.attachments[0], {
        type: "image",
        id: message.attachments[0]!.id,
        name: "proof.png",
        mimeType: "image/png",
        sizeBytes: 3,
      });
      assert.notStrictEqual(message.attachments[0]!.id, id);
      const path = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment: message.attachments[0]!,
      });
      assert.isNotNull(path);
      assert.deepStrictEqual(Array.from(yield* fs.readFile(path!)), Array.from(bytes));
      assert.notStrictEqual(call.isError, true, JSON.stringify(call));
      assert.deepStrictEqual(call.structuredContent, {
        threadId,
        messageId: message.id,
        ...(target === "spectrum"
          ? { runId: null, status: "idle", delivery: "transcript" }
          : { runId: rows.runs[0]!.id, status: rows.runs[0]!.status }),
      });
      assert.deepStrictEqual(rows.providerSessions, []);
      assert.deepStrictEqual(rows.providerTurns, []);
      if (target === "spectrum") {
        yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
        assert.deepStrictEqual(rows.runs, []);
        yield* (yield* Threads.ThreadManagementService).dispatch({
          type: "thread.stop",
          threadId,
          commandId: CommandId.make("stop:spectrum-attachments"),
        });
        const stopped = yield* readSpectrum(threadId);
        assert.strictEqual(stopped._tag, "Some");
        if (stopped._tag === "Some") assert.strictEqual(stopped.value.status, "retired");
        assert.isTrue((yield* send("after-stop")).isError);
        assert.deepStrictEqual(yield* readSpectrum(threadId), stopped);
      }
      const replay = yield* send("spectrum-attachment");
      assert.deepStrictEqual(replay.structuredContent, call.structuredContent);
      assert.strictEqual(
        (yield* projections.getThreadRecords(threadId, ["messages"])).messages.length,
        1,
      );
    }).pipe(
      Effect.provide(
        McpHttpServer.layerAttachmentToolkit.pipe(
          Layer.provideMerge(runtime),
          Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
          Layer.provideMerge(AssetSigner.layer.pipe(Layer.provideMerge(ServerSecretStore.layer))),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "spectrum-attachments-" }),
          ),
          Layer.provideMerge(
            DocumentText.layer.pipe(
              Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
            ),
          ),
        ),
      ),
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
