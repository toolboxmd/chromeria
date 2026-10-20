import { McpSchema, McpServer } from "effect/ai";
import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as Broker from "../../PreviewAutomationBroker.ts";
import * as Receipts from "../../McpRequestReceipts.ts";
import * as Threads from "../../../orchestration-v2/ThreadManagementService.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as PreviewHandlers from "./handlers.ts";
import { PreviewStandardToolkit } from "./tools.ts";
import { PreviewTabId, SERVER_BROWSER_AUTOMATION_CLIENT_ID } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationRequest,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import {
  createPendingAttachmentId,
  parseThreadSegmentFromAttachmentId,
} from "../../../attachmentStore.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import * as Secrets from "../../../auth/ServerSecretStore.ts";
import * as Staging from "../../../assets/AttachmentStaging.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as ServerConfig from "../../../config.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadsLayer } from "../../McpToolAccess.testkit.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import { claimPreviewRecording, layerStandard } from "./handlers.ts";
import { PreviewStandardToolkit } from "./tools.ts";

it.effect("preview_open navigates the opened tab to a dev server in one tool call", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
      const connected = yield* Deferred.make<void>();
      const tabId = PreviewTabId.make("opened-tab");
      const target = {
        kind: "environment-port" as const,
        port: 3000,
        path: "/settings?tab=account",
      };
      const requests: PreviewAutomationRequest[] = [];
      const status = {
        available: true,
        visible: false,
        tabId,
        url: "http://localhost:3000/settings?tab=account",
        title: "Settings",
        loading: false,
      };
      const events = yield* broker.connect({
        clientId: "client-1",
        environmentId: scope.environmentId,
      });
      yield* Stream.runForEach(events, (event) => {
        if (event.type === "connected") return Deferred.succeed(connected, undefined);
        requests.push(event.request);
        return broker.respond({
          clientId: "client-1",
          connectionId: event.connectionId,
          requestId: event.request.requestId,
          ok: true,
          result: event.request.operation === "open" ? { ...status, url: "about:blank" } : status,
        });
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(connected);
      const toolkit = yield* PreviewStandardToolkit.pipe(
        Effect.provide(McpToolAccess.HandlersLayer.layer(layerStandard)),
      );
      const results = yield* toolkit
        .handle("preview_open", {
          url: "http://localhost:3000",
          target,
          open: false,
          reuseExistingTab: false,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      expect(results.at(-1)?.result).toEqual(status);
      expect(requests.map(({ operation, input }) => ({ operation, input }))).toEqual([
        { operation: "open", input: { open: false, show: false, reuseExistingTab: false } },
        { operation: "navigate", input: { target } },
      ]);
      expect(requests[1]?.tabId).toBe(tabId);
      expect(requests[1]?.tabIdExplicit).toBe(true);
    }).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-1"),
        requestNamespace: "provider-session-1",
        thread: {
          threadId: ThreadId.make("thread-1"),
          providerSessionId: "provider-session-1",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
        client: undefined,
        capabilities: new Set(["preview"] as const),
        issuedAt: 1,
      }),
      Effect.provide(
        Layer.mergeAll(PreviewAutomationBroker.layer, liveThreadsLayer).pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  ),
);

const recordingScope: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("preview-test"),
  requestNamespace: "native-recording",
  issuedAt: 0,
  capabilities: new Set(["preview"]),
  client: undefined,
  thread: {
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "native-recording",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
};
const recordingLayer = Staging.layer.pipe(
  Layer.provideMerge(
    Sqlite.layerMemory.pipe(
      Layer.provideMerge(Secrets.layer),
      Layer.provideMerge(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-preview-recording-" }),
      ),
      Layer.provideMerge(NodeServices.layer),
    ),
  ),
);


describe("claimPreviewRecording", () => {
  it.effect("overlapping and repeated claims return the same retained recording", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const uploadedAttachmentId = createPendingAttachmentId(".webm");
      const pendingPath = path.join(config.attachmentsDir, `${uploadedAttachmentId}.webm`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFileString(pendingPath, "video!");
      const response = {
        id: "desktop-recording",
        tabId: "tab-1",
        path: "/desktop/recording.webm",
        mimeType: "video/webm",
        sizeBytes: 6,
        createdAt: "2026-09-07T00:00:00.000Z",
        uploadedAttachmentId,
      };
      const claim = claimPreviewRecording(ThreadId.make("thread-1"), response);
      const [first, second] = yield* Effect.all([claim, claim], { concurrency: "unbounded" });
      expect(first).toEqual(second);
      expect(yield* claim).toEqual(first);
      expect(yield* fileSystem.readFileString(first.path)).toBe("video!");
      expect(yield* fileSystem.exists(pendingPath)).toBe(true);
      const wrongThread = yield* claimPreviewRecording(ThreadId.make("thread-2"), response).pipe(
        Effect.result,
      );
      expect(wrongThread._tag).toBe("Failure");
      const wrongPath = yield* claimPreviewRecording(ThreadId.make("thread-1"), {
        ...response,
        uploadedAttachmentId: `../${uploadedAttachmentId}`,
      }).pipe(Effect.result);
      expect(wrongPath._tag).toBe("Failure");
    }).pipe(
      Effect.provide(
        recordingLayer.pipe(
          Layer.provideMerge(Layer.succeed(Invocation.McpInvocationContext, recordingScope)),
        ),
      ),
    ),
  );

  it.effect.each([6, 5])(
    "claims only a complete uploaded recording (reported bytes: %s)",
    (sizeBytes) =>
      Effect.gen(function* () {
        const config = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const uploadedAttachmentId = createPendingAttachmentId(".webm");
        const pendingPath = path.join(config.attachmentsDir, `${uploadedAttachmentId}.webm`);
        yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
        yield* fileSystem.writeFileString(pendingPath, "video!");
        const response = {
          id: "desktop-recording",
          tabId: "tab-1",
          path: "/desktop/recording.webm",
          mimeType: "video/webm",
          sizeBytes,
          createdAt: "2026-09-07T00:00:00.000Z",
          uploadedAttachmentId,
        };
        const result = yield* claimPreviewRecording(ThreadId.make("thread-1"), response).pipe(
          Effect.result,
        );
        if (sizeBytes === 6) {
          expect(result._tag).toBe("Success");
          if (result._tag !== "Success") return;
          expect(result.success.path).not.toBe(response.path);
          expect(parseThreadSegmentFromAttachmentId(result.success.id)).toBe("thread-1");
          expect(yield* fileSystem.readFileString(result.success.path)).toBe("video!");
          expect(yield* fileSystem.exists(pendingPath)).toBe(true);

          yield* fileSystem.writeFileString(result.success.path, "later!");
          const retry = yield* claimPreviewRecording(ThreadId.make("thread-1"), response).pipe(
            Effect.result,
          );
          expect(retry._tag).toBe("Failure");
          expect(yield* fileSystem.readFileString(result.success.path)).toBe("later!");
        } else {
          expect(result._tag).toBe("Failure");
          if (result._tag !== "Failure") return;
          expect(result.failure._tag).toBe("PreviewAutomationRecordingTransferError");
          expect(yield* fileSystem.exists(pendingPath)).toBe(true);
        }
      }).pipe(
        Effect.provide(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-preview-recording-" }).pipe(
            Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );
});

const vaultMcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "vault-receipt-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "vault-receipt-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

it.effect.each([
  ["preview_fill_credential", "completed"],
  ["preview_end_login", "completed"],
  ["preview_fill_credential", "unresolved"],
  ["preview_end_login", "unresolved"],
  ["preview_fill_credential", "legacy"],
  ["preview_end_login", "legacy"],
] as const)("vault379: registered %s preserves %s boundaries", ([name, scenario]) => {
  let live = true;
  const database = Sqlite.layerMemory.pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-vault-receipts-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  const services = Layer.mergeAll(
    Receipts.layer,
    Layer.effect(Broker.PreviewAutomationBroker, Broker.make),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.succeed(liveThreadShell(id, live ? {} : { activeRunId: null })),
    }),
  ).pipe(Layer.provideMerge(database));
  const registered = McpHttpServer.toolkitRegistration(
    PreviewStandardToolkit,
    PreviewHandlers.layerStandard,
  ).pipe(Layer.provideMerge(McpServer.McpServer.layer), Layer.provideMerge(services));
  return Effect.scoped(
    Effect.gen(function* () {
      const broker = yield* Broker.PreviewAutomationBroker;
      const server = yield* McpServer.McpServer;
      const ready = yield* Deferred.make<void>();
      const tabId = PreviewTabId.make("vault-tab");
      const operation = name === "preview_fill_credential" ? "fillCredential" : "endLogin";
      const result =
        name === "preview_fill_credential"
          ? { filled: true, item: "synthetic-item", field: "password", site: "example.test", tabId }
          : { closed: [tabId] };
      let effects = 0;
      let loseReply = false;
      yield* Stream.runForEach(
        yield* broker.connect({
          clientId: SERVER_BROWSER_AUTOMATION_CLIENT_ID,
          environmentId: recordingScope.environmentId,
          supportedOperations: [operation, "status"],
        }),
        (event) =>
          Effect.gen(function* () {
            if (event.type === "connected") {
              yield* Deferred.succeed(ready, undefined);
              return;
            }
            const request = event.request;
            if (request.operation === operation) effects++;
            yield* broker.respond({
              clientId: SERVER_BROWSER_AUTOMATION_CLIENT_ID,
              connectionId: event.connectionId,
              requestId: request.requestId,
              ...(request.operation === operation && loseReply
                ? {
                    ok: false,
                    error: {
                      _tag: "PreviewAutomationExecutionError",
                      message: "synthetic reply lost after effect",
                    },
                  }
                : {
                    ok: true,
                    result: request.operation === "status" ? { available: true } : result,
                  }),
            });
          }),
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(ready);
      const call = (key: string, scope: Invocation.McpInvocationScope = recordingScope) =>
        server
          .callTool({
            name,
            arguments: {
              tabId,
              ...(name === "preview_fill_credential"
                ? { item: "synthetic-item", field: "password", locator: "#password" }
                : {}),
              clientRequestId: key,
            },
          })
          .pipe(
            Effect.provideService(Invocation.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, vaultMcpClient),
          );
      if (scenario === "completed") {
        const first = yield* call("completed");
        expect(first.isError, JSON.stringify(first)).not.toBe(true);
        expect(first.structuredContent).toMatchObject(result);
        expect((yield* call("completed")).structuredContent).toEqual(first.structuredContent);
        expect(effects).toBe(1);
        live = false;
        const revoked = yield* call("completed");
        expect(JSON.stringify(revoked)).toContain("parent_not_active");
        expect(effects).toBe(1);
      } else if (scenario === "legacy") {
        const legacy = yield* call("legacy", {
          ...recordingScope,
          client: {
            sessionId: "legacy-peer",
            label: "Legacy",
            access: "full-access",
            linked: true,
          },
        });
        expect(JSON.stringify(legacy)).toContain("capability_denied");
        expect(effects).toBe(0);
      } else {
        loseReply = true;
        const uncertain = yield* call("unresolved");
        expect(JSON.stringify(uncertain)).toContain("outcome_unknown");
        expect(JSON.stringify(uncertain)).toContain("dispatch_started");
        expect(effects).toBe(1);
        expect(JSON.stringify(yield* call("unresolved"))).toContain("outcome_unknown");
        expect(effects).toBe(1);
      }
    }),
  ).pipe(Effect.provide(registered));
});
