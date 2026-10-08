import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  NonNegativeInt,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { McpSchema, McpServer } from "effect/ai";

import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import { McpInvocationContext, type McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import { liveThreadShell, idleThreadProjection } from "../mcp/McpToolAccess.testkit.ts";
import { layer as handlers } from "../mcp/toolkits/thread/handlers.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";

const root = ThreadId.make("root");
const child = ThreadId.make("child");
const grandchild = ThreadId.make("grandchild");
const unrelated = ThreadId.make("unrelated");
const fork = ThreadId.make("fork");
const requestId = RuntimeRequestId.make("request");
const now = DateTime.makeUnsafe("2026-01-01T00:00:00.000Z");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "requests", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "requests", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const scope = (id: ThreadId | undefined): McpInvocationScope => ({
  environmentId: EnvironmentId.make("env"),
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  requestNamespace: "request",
  thread:
    id === undefined
      ? undefined
      : {
          threadId: id,
          providerSessionId: "session",
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
  client:
    id === undefined ? { sessionId: "oauth", label: "external", access: "full-access" } : undefined,
});

it.effect.each([
  [root, grandchild, "approval", "accept", undefined, true],
  [root, grandchild, "question", undefined, { choice: "continue" }, true],
  [unrelated, grandchild, "approval", "accept", undefined, false],
  [unrelated, grandchild, "question", undefined, { choice: "continue" }, false],
  [undefined, grandchild, "approval", "accept", undefined, false],
  [root, fork, "approval", "accept", undefined, false],
  [grandchild, grandchild, "approval", "accept", undefined, false],
] as const)(
  "respond authorizes %s -> %s %s",
  ([caller, target, kind, decision, answers, accepted]) =>
    Effect.gen(function* () {
      const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
      const shells = new Map(
        [root, child, grandchild, unrelated, fork].map((id) => [
          id,
          {
            ...liveThreadShell(id, {
              runtimeMode: id === root ? "full-access" : "approval-required",
            }),
            lineage:
              id === child
                ? {
                    rootThreadId: root,
                    parentThreadId: root,
                    relationshipToParent: "subagent" as const,
                  }
                : id === grandchild
                  ? {
                      rootThreadId: root,
                      parentThreadId: child,
                      relationshipToParent: "subagent" as const,
                    }
                  : id === fork
                    ? {
                        rootThreadId: root,
                        parentThreadId: root,
                        relationshipToParent: "fork" as const,
                      }
                    : { rootThreadId: id, parentThreadId: null, relationshipToParent: null },
          },
        ]),
      );
      const threads = Layer.mock(ThreadManagementService)({
        getThreadShell: (id) => Effect.succeed(shells.get(id) ?? null),
        getProjectThreadRecords: (({ threadId }) => {
          const shell = shells.get(threadId)!;
          const base = {
            id: TurnItemId.make("item"),
            threadId,
            runId: null,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: NonNegativeInt.make(0),
            status: "in_progress" as const,
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
          };
          return Effect.succeed({
            ...idleThreadProjection(shell),
            runtimeRequests: [
              {
                id: requestId,
                nodeId: NodeId.make("node"),
                providerTurnId: null,
                nativeRequestRef: null,
                kind: kind === "approval" ? ("command" as const) : ("user_input" as const),
                status: "pending" as const,
                responseCapability: { type: "message" as const },
                createdAt: now,
                resolvedAt: null,
              },
            ],
            turnItems: [
              kind === "approval"
                ? {
                    ...base,
                    type: "approval_request" as const,
                    requestId,
                    requestKind: "command" as const,
                    prompt: "Run command?",
                  }
                : { ...base, type: "user_input_request" as const, requestId, questions: [] },
            ],
          });
        }) as ThreadManagementService["Service"]["getProjectThreadRecords"],
        dispatch: (command) =>
          Ref.update(commands, (current) => [...current, command]).pipe(
            Effect.as({ sequence: NonNegativeInt.make(1), storedEvents: [] }),
          ),
      });
      const server = McpHttpServer.toolkitRegistration(ThreadToolkit, handlers).pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(threads),
        Layer.provideMerge(NodeServices.layer),
      );
      const result = yield* McpServer.McpServer.pipe(
        Effect.flatMap((service) =>
          service.callTool({
            name: "t3_pending_request_respond",
            arguments: {
              threadId: target,
              requestId,
              ...(decision === undefined ? {} : { decision }),
              ...(answers === undefined ? {} : { answers }),
            },
          }),
        ),
        Effect.provideService(McpInvocationContext, scope(caller)),
        Effect.provideService(McpSchema.McpServerClient, client),
        Effect.provide(server),
      );
      assert.equal(result.isError === true, !accepted);
      const dispatched = yield* Ref.get(commands);
      assert.lengthOf(dispatched, accepted ? 1 : 0);
      if (accepted) {
        assert.equal(dispatched[0]?.type, "runtime-request.respond");
        assert.equal(shells.get(target)?.runtimeMode, "approval-required");
      }
    }).pipe(Effect.scoped),
);
