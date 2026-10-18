import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as Config from "../config.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as Invocation from "./McpInvocationContext.ts";

it.effect(
  "a registered research read returns bounded content with drained effects and no research events",
  () =>
    Effect.gen(function* () {
      const requests = yield* Queue.unbounded<string>();
      const runtime = Replay.layerWithRegistry(
        { name: "research-read" },
        Registry.layerFromAdapters([]),
        { runEffectWorker: false },
      );
      const reader = WebResearch.layer.pipe(
        Layer.provide(PageCache.layer),
        Layer.provide(
          Layer.succeed(PublicHttp.PublicHttp, {
            get: (url) =>
              Queue.offer(requests, url.pathname).pipe(
                Effect.as({
                  status: 200,
                  headers: { "content-type": "text/html" },
                  body:
                    url.pathname === "/robots.txt"
                      ? "User-agent: *\nAllow: /"
                      : `<h1>Research fixture</h1><p>${"Full text stays in the page cache. ".repeat(1000)}</p>`,
                }),
              ),
          }),
        ),
        Layer.provide(Config.layerTest(process.cwd(), { prefix: "research-integration-" })),
        Layer.provide(NodeServices.layer),
      );
      const layer = McpHttpServer.layerWebResearchToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(reader),
        Layer.provideMerge(runtime),
        Layer.provide(NodeCrypto.layer),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const server = yield* McpServer.McpServer;
        const threadId = ThreadId.make("research-integration-thread");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("research:create"),
          threadId,
          projectId: ProjectId.make("research-project"),
          title: "Research",
          branch: null,
          worktreePath: null,
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        const before = yield* orchestrator.getThreadProjection(threadId);
        const client = McpSchema.McpServerClient.of({
          clientId: 1,
          protocolVersion: "2025-06-18",
          clientCapabilities: {},
          clientInfo: { name: "research-proof", version: "1" },
          initializePayload: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "research-proof", version: "1" },
          },
          getClient: Effect.die("unused"),
        });
        const fiber = yield* Effect.forkScoped(
          server
            .callTool({ name: "web_read", arguments: { url: "https://research.example/" } })
            .pipe(
              Effect.provideService(McpSchema.McpServerClient, client),
              Effect.provideService(Invocation.McpInvocationContext, {
                environmentId: EnvironmentId.make("research-environment"),
                requestNamespace: "research-agent",
                issuedAt: 0,
                thread: {
                  threadId,
                  providerSessionId: "research-session",
                  providerInstanceId: ProviderInstanceId.make("codex"),
                },
                client: undefined,
                capabilities: new Set<Invocation.McpCapability>(),
              }),
            ),
        );
        expect(yield* Queue.take(requests)).toBe("/robots.txt");
        yield* TestClock.adjust(3000);
        expect(yield* Queue.take(requests)).toBe("/");
        const result = yield* Fiber.join(fiber);
        yield* worker.drain();
        expect(result.isError, JSON.stringify(result.content)).not.toBe(true);
        expect(result.structuredContent).toMatchObject({ status: "ok", nextOffset: 20_000 });
        expect(JSON.stringify(result).length).toBeLessThan(45_000);
        expect(yield* orchestrator.getThreadProjection(threadId)).toEqual(before);
      }).pipe(Effect.provide(layer));
    }),
);
