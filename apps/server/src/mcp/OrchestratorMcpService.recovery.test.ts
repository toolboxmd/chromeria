import { NodeHttpServer } from "@effect/platform-node";
import { HttpBody, HttpClient, HttpRouter } from "effect/http";
import * as Schema from "effect/Schema";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as Metadata from "./ThreadMetadataMcpService.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as Handlers from "./toolkits/orchestrator/handlers.ts";
import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";
import { OrchestratorMcpThreadReadResult } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  RunId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Store from "../prism/RecoveryStore.ts";
import * as History from "../prism/RecoveryHistory.ts";
import { recoveryRun as run, recoveryEvents, errorFor } from "../prism/recovery.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Projects from "../project/ProjectService.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as Mcp from "./OrchestratorMcpService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

const decodeReadResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      result: Schema.Struct({ structuredContent: OrchestratorMcpThreadReadResult }),
    }),
  ),
);

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("recovery-test"),
  requestNamespace: "outside-service",
  thread: undefined,
  client: { sessionId: "outside-service", label: "service", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};
const persist = (key: string, events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
  Effect.gen(function* () {
    yield* (yield* EventSink.EventSinkV2).commitCommand({
      commandId: CommandId.make(key),
      commandType: "fixture",
      threadId: run.threadId,
      acceptedAt: run.requestedAt,
      events,
      effects: [],
    });
  });
const read = Effect.scoped(
  Effect.gen(function* () {
    const serverLayer = McpHttpServer.toolkitRegistration(OrchestratorToolkit, Handlers.layer).pipe(
      Layer.provide(Metadata.layer),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provideMerge(McpHttpServer.layerMcpTransport),
      Layer.provide(
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({
          resolve: () => Effect.succeed(undefined),
        }),
      ),
      Layer.provide(
        Layer.succeed(McpHttpServer.McpClientAuthenticator, {
          authenticate: (request) =>
            Effect.succeed(
              request.headers.authorization === "Bearer test-client" ? scope : undefined,
            ),
        }),
      ),
    );
    yield* HttpRouter.serve(serverLayer, { disableListenLog: true, disableLogger: true }).pipe(
      Layer.build,
    );
    const client = yield* HttpClient.HttpClient;
    const headers = {
      accept: "application/json, text/event-stream",
      authorization: "Bearer test-client",
    };
    const initialize = yield* client.post("/mcp", {
      headers,
      body: HttpBody.text(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "initialize",
          params: {
            protocolVersion: "2025-06-18",
            capabilities: {},
            clientInfo: { name: "outside-service", version: "1" },
          },
        }),
        "application/json",
      ),
    });
    assert.strictEqual(initialize.status, 200);
    const sessionId = initialize.headers["mcp-session-id"];
    if (sessionId === undefined) throw new Error("MCP session was not created");
    const response = yield* client.post("/mcp", {
      headers: { ...headers, "mcp-session-id": sessionId, "mcp-protocol-version": "2025-06-18" },
      body: HttpBody.text(
        JSON.stringify({
          jsonrpc: "2.0",
          id: 2,
          method: "tools/call",
          params: { name: "t3_thread_read", arguments: { threadId: run.threadId } },
        }),
        "application/json",
      ),
    });
    assert.strictEqual(response.status, 200);
    const body = yield* decodeReadResponse(yield* response.text);
    return body.result.structuredContent;
  }),
).pipe(Effect.provide(NodeHttpServer.layerTest));
const processLayer = (path: string) => {
  const database = Persistence.layerFromPath(path).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provide(database)),
    Projection.layer.pipe(Layer.provide(database)),
    Store.layer.pipe(Layer.provide(database)),
  );
  const management = Layer.unwrap(
    Effect.gen(function* () {
      const projections = yield* Projection.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      return Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (id) => projections.getThreadShell(id).pipe(Effect.orDie),
        getProjectThreadRecords: ({ threadId }, fields, options) =>
          projections.getThreadRecords(threadId, fields, options).pipe(Effect.orDie),
        getTimelinePage: (id, options) =>
          projections.getTimelinePage(id, options).pipe(Effect.orDie),
        readRunRecovery: (source) =>
          History.readRunRecovery(source).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        readRestartCutView: () => Effect.succeed(undefined),
      });
    }),
  ).pipe(Layer.provide(stores));
  const service = Mcp.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        stores,
        management,
        NodeServices.layer,
        Settings.layerTest(),
        Layer.mock(BareLaunch.BareLaunch)({}),
        Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(Providers.ProviderRegistry)({}),
        Layer.mock(Adapters.ProviderAdapterRegistryV2)({}),
        Layer.mock(Projects.ProjectService)({}),
        Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      ),
    ),
  );
  return Layer.mergeAll(
    stores,
    management,
    EventSink.layer.pipe(Layer.provide(stores)),
    service,
  ).pipe(Layer.provideMerge(NodeServices.layer));
};
const resetAt = "2026-10-08T12:00:00Z";
const quotaEvents = (autoResume: boolean) =>
  recoveryEvents(run).map((event) =>
    event.type === "thread.created"
      ? {
          ...event,
          payload: {
            ...event.payload,
            limitRecovery: { runId: run.id, resetAt, autoResume, snooze: false },
          },
        }
      : event.type === "turn-item.updated" && event.payload.type === "error"
        ? {
            ...event,
            payload: {
              ...event.payload,
              failure: { ...event.payload.failure, class: "usage_limit" as const, resetAt },
            },
          }
        : event,
  );

describe("outside-agent thread read recovery", () => {
  it.effect.each([
    "none",
    "waiting_on_quota",
    "controller_quota",
    "recovery_scheduled",
    "continued_by",
    "given_up",
    "opted_out",
    "stopped",
    "reenabled",
    "stale_retry",
  ] as const)("%s survives closing and reopening the database", (state) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "mcp-recovery-read-"));
    const path = NodePath.join(directory, "state.sqlite");
    const inProcess = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.provide(processLayer(path)));
    const expected =
      state === "waiting_on_quota" || state === "reenabled" || state === "controller_quota"
        ? { state: "waiting_on_quota", resetAt }
        : state === "recovery_scheduled"
          ? { state, reason: "retry", expectedStartAt: null }
          : state === "continued_by"
            ? { state, successorRunId: "run:successor" }
            : state === "given_up"
              ? { state, reason: "exhausted" }
              : state === "opted_out"
                ? { state: "given_up", reason: "opted_out" }
                : state === "stopped"
                  ? { state: "given_up", reason: "stopped_or_retired" }
                  : state === "stale_retry"
                    ? { state: "none" }
                    : { state };
    return Effect.gen(function* () {
      yield* inProcess(
        Effect.gen(function* () {
          yield* persist(
            "seed",
            state === "controller_quota"
              ? quotaEvents(true).map((event) =>
                  event.type === "thread.created"
                    ? { ...event, payload: { ...event.payload, limitRecovery: undefined } }
                    : event,
                )
              : state === "waiting_on_quota" || state === "reenabled"
                ? quotaEvents(true)
                : state === "opted_out"
                  ? quotaEvents(false)
                  : recoveryEvents(run),
          );
          if (
            state === "recovery_scheduled" ||
            state === "stale_retry" ||
            state === "controller_quota"
          ) {
            const error = errorFor(run);
            if (error.type !== "error") throw new Error("fixture");
            yield* (yield* Store.RecoveryStore).reconcile({
              previous: null,
              run,
              failure:
                state === "controller_quota"
                  ? { ...error.failure, class: "usage_limit", resetAt }
                  : error.failure,
              stoppedOrRetired: false,
              autoResume: true,
            });
          }
          if (state === "continued_by") {
            yield* persist("successor", [
              {
                id: EventId.make("successor"),
                type: "run.created",
                threadId: run.threadId,
                occurredAt: run.requestedAt,
                payload: {
                  ...run,
                  id: RunId.make("run:successor"),
                  ordinal: 2,
                  status: "failed",
                  forkPrismContinuationSourceRunId: run.id,
                },
              },
              {
                id: EventId.make("latest"),
                type: "run.created",
                threadId: run.threadId,
                occurredAt: run.requestedAt,
                payload: {
                  ...run,
                  id: RunId.make("run:latest"),
                  ordinal: 3,
                  status: "running",
                  completedAt: null,
                  forkPrismContinuationSourceRunId: RunId.make("run:successor"),
                },
              },
            ]);
          }
          if (state === "stale_retry") {
            const thread = yield* (yield* Projection.ProjectionStoreV2).getThread(run.threadId);
            yield* persist("stop", [
              {
                id: EventId.make("stop"),
                type: "thread.metadata-updated",
                threadId: run.threadId,
                occurredAt: run.requestedAt,
                payload: { ...thread, settledOverride: "settled" },
              },
            ]);
          }
          if (state === "opted_out" || state === "reenabled")
            yield* History.writeRecoveryOutcome({
              sourceRunId: run.id,
              threadId: run.threadId,
              status: "decided",
              outcome: "not_retryable",
              reason: "opted_out",
            });
          if (state === "stopped")
            yield* History.writeRecoveryOutcome({
              sourceRunId: run.id,
              threadId: run.threadId,
              status: "decided",
              outcome: "abandoned",
              reason: "stopped_or_retired",
            });
          if (state === "given_up")
            yield* History.writeRecoveryOutcome({
              sourceRunId: run.id,
              threadId: run.threadId,
              status: "decided",
              outcome: "not_retryable",
              reason: "exhausted",
            });
          assert.containSubset(
            (yield* read).recentRuns.find((entry) => entry.runId === run.id),
            { recovery: expected },
          );
        }),
      );
      yield* inProcess(
        Effect.gen(function* () {
          const result = yield* read;
          if (state === "continued_by") {
            assert.containSubset(result.recentRuns, [
              {
                runId: "run:original",
                recovery: { state: "continued_by", successorRunId: "run:successor" },
              },
              {
                runId: "run:successor",
                recovery: { state: "continued_by", successorRunId: "run:latest" },
              },
              { runId: "run:latest", recovery: { state: "none" } },
            ]);
          }
          assert.containSubset(
            result.recentRuns.find((entry) => entry.runId === run.id),
            { recovery: expected },
          );
        }),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  });
});
