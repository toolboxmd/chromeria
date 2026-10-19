import { clientHandlers as pendingRequestClientHandlers } from "../childThreads/PendingRequestIngress.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcServer, RpcSerialization } from "effect/rpc";
import { PendingRequestClientRpcGroup, WS_METHODS } from "@t3tools/contracts";
import * as RuntimeRequests from "../orchestration-v2/RuntimeRequestService.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import { AuthSessionId, RuntimeRequestId } from "@t3tools/contracts";
import * as Queue from "effect/Queue";
import * as Fiber from "effect/Fiber";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import { vi } from "vite-plus/test";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  OrchestratorMcpFailure,
  EventId,
  NodeId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import {
  activate,
  caller,
  decodePending369ClientResult,
  decodePending369Content,
  decodePending369LocalTask,
  decodePendingWsReply,
  decodeTyped383Open,
  delegate,
  driver,
  instanceId,
  json,
  makeNode,
  nativeCall,
  nativeInvalidParams,
  seedParent,
  seedPending369,
  selection,
  typed383Pair,
} from "./PeerAuthority.workflow.fixture.ts";

it.live.each(["question", "approval"] as const)(
  "pending369 notice: peer observation carries %s content into one parent notice",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("notice-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("notice-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("notice-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "notice-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const pending = yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
          "turnItems",
          "providerThreads",
        ]);
        const item = pending.turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === requestId,
        );
        if (item?.type !== "user_input_request") return yield* Effect.die("Expected question item");
        const now = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("notice:request-content"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: now,
              payload:
                kind === "question"
                  ? {
                      ...item,
                      questions: [
                        {
                          id: "color",
                          header: "Color",
                          question: "Which color?",
                          options: [{ label: "Blue", description: "Use blue" }],
                        },
                      ],
                    }
                  : {
                      ...item,
                      type: "approval_request",
                      requestKind: "command",
                      prompt: "npm run build",
                      options: [{ decision: "accept", label: "Approve build" }],
                    },
            },
            ...(kind === "approval"
              ? [
                  {
                    id: EventId.make("notice:approval-runtime"),
                    type: "runtime-request.updated" as const,
                    threadId: child.childId,
                    occurredAt: now,
                    payload: {
                      ...pending.runtimeRequests.find((request) => request.id === requestId)!,
                      kind: "command" as const,
                      responseCapability: {
                        type: "live" as const,
                        providerSessionId: pending.providerThreads[0]!.providerSessionId!,
                      },
                    },
                  },
                ]
              : []),
          ],
        });
        const detail = kind === "question" ? "Which color?" : "npm run build";
        const option = kind === "question" ? "Blue" : "Approve build";
        const task = (yield* a.threads.getThreadRecords(parent, ["subagents"])).subagents.find(
          (task) => task.id === child.taskId,
        )!;
        const connection = yield* a.reconnect();
        const snapshot = yield* connection.peers.taskLifecycle!(
          b.environmentId,
          task.remoteChild!.launch!.linkIdentity,
          {
            pair: {
              parentEnvironmentId: a.environmentId,
              parentThreadId: parent,
              taskId: task.id,
              childEnvironmentId: b.environmentId,
              childThreadId: child.childId,
              childRunId: null,
            },
            operation: { type: "snapshot", after: 0, wait: false },
          },
        );
        expect(snapshot.type).toBe("snapshot");
        if (snapshot.type !== "snapshot") return yield* Effect.die("Expected snapshot");
        expect(JSON.stringify(snapshot.snapshot.requests)).toContain(detail);
        expect(JSON.stringify(snapshot.snapshot.requests)).toContain(option);
        yield* a.threads.dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make("notice:peer-snapshot"),
          parentThreadId: parent,
          taskId: task.id,
          availability: "current",
          observation: { cursor: 0, snapshot: snapshot.snapshot },
        });
        const messages = (yield* a.threads.getThreadRecords(parent, ["messages"])).messages.filter(
          (message) =>
            message.notification?.outcome === "updated" && message.text.includes(requestId),
        );
        expect(messages).toHaveLength(1);
        expect(messages[0]!.text).toContain(detail);
        expect(messages[0]!.text).toContain(option);
        expect(messages[0]!.notification?.source).not.toHaveProperty("childThreadId");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);


it.live(
  "pending369: authenticated native HTTP proves pair ancestry and replays one native answer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("pending369-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("pending369-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        expect(content.kind).toBe("user-input");
        expect(content.revision).toBeGreaterThan(0);
        const answer = {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Blue" },
          clientRequestId: "pending369-answer",
        };
        const concurrent = yield* Effect.all(
          [
            nativeCall(a, parent, "t3_pending_request_respond", answer),
            nativeCall(a, parent, "t3_pending_request_respond", answer),
          ],
          { concurrency: "unbounded" },
        );
        const response = concurrent[0];
        expect(concurrent[1]?.structuredContent).toEqual(response.structuredContent);
        expect(response.isError, json(response.content)).not.toBe(true);
        const replay = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(replay.isError, json(replay.content)).not.toBe(true);
        expect(replay.structuredContent).toEqual(response.structuredContent);
        // Rebuild the destination's actual disk request runtime and authenticated
        // transport. Renewed source credentials still recover native acceptance.
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).structuredContent,
        ).toEqual(response.structuredContent);
        const changed = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...answer,
          answers: { color: "Red" },
        });
        expect(changed.isError).toBe(true);
        const records = yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
          "messages",
        ]);
        expect(records.runtimeRequests.find((r) => r.id === requestId)?.status).toBe("resolved");
        expect(records.messages.filter((m) => m.text === "Which color?\nBlue")).toHaveLength(1);
        const selfRequestId = yield* seedPending369(b, child.childId, "self");
        const self = yield* nativeCall(b, child.childId, "t3_pending_request_read", {
          threadId: child.childId,
          requestId: selfRequestId,
        });
        expect(self.isError).toBe(true);
        const dismissArgs = { ...args, requestId: selfRequestId };
        const dismissRead = yield* nativeCall(a, parent, "t3_pending_request_read", dismissArgs);
        const dismissContent = yield* decodePending369Content(dismissRead.structuredContent);
        const dismissed = yield* nativeCall(a, parent, "t3_pending_request_dismiss", {
          ...dismissArgs,
          expectedRevision: dismissContent.revision,
          clientRequestId: "explicit-dismiss",
        });
        expect(dismissed.isError, json(dismissed.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, [
            "runtimeRequests",
          ])).runtimeRequests.find((request) => request.id === selfRequestId),
        ).toMatchObject({ status: "resolved", decision: "cancel" });
        const shapeRequest = yield* seedPending369(b, child.childId, "shape-revision");
        const shapeArgs = { ...args, requestId: shapeRequest };
        const shapeRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", shapeArgs)).structuredContent,
        );
        const shapeItem = (yield* b.threads.getThreadRecords(child.childId, [
          "turnItems",
        ])).turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === shapeRequest,
        );
        if (shapeItem?.type !== "user_input_request")
          return yield* Effect.die("Expected current question shape");
        const shapeNow = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-question-shape-update"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: shapeNow,
              payload: {
                ...shapeItem,
                questions: shapeItem.questions.map((question) => ({
                  ...question,
                  question: "Which updated color?",
                })),
              },
            },
          ],
        });
        const staleShape = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...shapeArgs,
          expectedRevision: shapeRead.revision,
          answers: { color: "White" },
          clientRequestId: "pending369-old-shape",
        });
        expect(staleShape.isError, json(staleShape.content)).toBe(true);
        const freshShape = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", shapeArgs)).structuredContent,
        );
        expect(freshShape.revision).toBeGreaterThan(shapeRead.revision!);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...shapeArgs,
            expectedRevision: freshShape.revision,
            answers: { color: "White" },
            clientRequestId: "pending369-new-shape",
          })).isError,
        ).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which updated color?\nWhite",
          ),
        ).toHaveLength(1);
        const protectedRequest = yield* seedPending369(b, child.childId, "current-policy");
        const protectedArgs = { ...args, requestId: protectedRequest };
        const protectedRead = yield* nativeCall(
          a,
          parent,
          "t3_pending_request_read",
          protectedArgs,
        );
        const protectedContent = yield* decodePending369Content(protectedRead.structuredContent);
        const protectedAnswer = {
          ...protectedArgs,
          expectedRevision: protectedContent.revision,
          answers: { color: "Black" },
          clientRequestId: "pending369-current-policy",
        };
        const beforePolicy = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("pending369-narrow-mode"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", protectedAnswer)).isError,
        ).toBe(true);
        expect((yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).isError).toBe(
          true,
        );
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("pending369-restore-mode"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_read", protectedArgs)).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", protectedAnswer)).isError,
        ).toBe(true);
        expect((yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).isError).toBe(
          true,
        );
        const afterPolicy = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        expect(afterPolicy.messages).toEqual(beforePolicy.messages);
        expect(afterPolicy.runtimeRequests).toEqual(beforePolicy.runtimeRequests);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: grandparent routes A-B-C without destination profiles and denies unrelated or stale actions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-grand-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-grand-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const c = yield* makeNode(
          descriptorOf("pending369-grand-c", "C", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("pending369-grandparent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const ab = yield* delegate(a, b, parent, "pending369-ab");
        yield* activate(b, ab.childId);
        const bc = yield* delegate(b, c, ab.childId, "pending369-bc");
        yield* activate(c, bc.childId);
        const requestId = yield* seedPending369(c, bc.childId);
        const requestTarget = { environmentId: c.environmentId, threadId: bc.childId };
        const requestRoute = [
          { environmentId: b.environmentId, threadId: ab.childId },
          requestTarget,
        ];
        const args = { threadId: parent, requestId, requestTarget, requestRoute };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        const unrelated = ThreadId.make("pending369-unrelated");
        yield* seedParent(a, unrelated);
        expect(
          (yield* nativeCall(a, unrelated, "t3_pending_request_respond", {
            ...args,
            expectedRevision: content.revision,
            answers: { color: "Red" },
            clientRequestId: "unrelated",
          })).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: 0,
            answers: { color: "Red" },
            clientRequestId: "stale",
          })).isError,
        ).toBe(true);
        const good = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Green" },
          clientRequestId: "grandparent",
        });
        expect(good.isError, json(good.content)).not.toBe(true);
        expect(
          (yield* c.threads.getThreadRecords(bc.childId, ["messages"])).messages.filter(
            (m) => m.text === "Which color?\nGreen",
          ),
        ).toHaveLength(1);
        expect(yield* c.links.list).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: source client derives its actor from a real current session and preserves native action identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-human-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-human-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-human-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const initialGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...initialGrant, onward: true });
        const child = yield* delegate(a, b, parent, "pending369-human-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const sessionId = AuthSessionId.make("pending369-real-client-session");
        const now = yield* DateTime.now;
        const auth = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(a.database)),
          Layer.build,
        );
        const sessions = Context.get(auth, AuthSessions.AuthSessionRepository);
        yield* sessions.create({
          sessionId,
          subject: "isolated-client",
          method: "browser-session-cookie",
          scopes: ["orchestration:read", "orchestration:operate"],
          client: {
            label: "Isolated source client",
            ipAddress: null,
            userAgent: null,
            deviceType: "unknown",
            os: null,
            browser: null,
          },
          issuedAt: now,
          expiresAt: DateTime.add(now, { hours: 1 }),
        });
        const base = {
          parentThreadId: parent,
          taskId: child.taskId,
          requestId,
          requestTarget: { environmentId: b.environmentId, threadId: child.childId },
        };
        const unrelated = ThreadId.make("pending369-unrelated-destination");
        yield* seedParent(b, unrelated);
        const unrelatedRequest = yield* seedPending369(b, unrelated);
        const unrelatedRead = yield* a.pendingRequests
          .fromTask(sessionId, {
            ...base,
            requestId: unrelatedRequest,
            requestTarget: { environmentId: b.environmentId, threadId: unrelated },
            operation: { type: "read" },
          })
          .pipe(Effect.result);
        expect(unrelatedRead._tag).toBe("Failure");
        const read = yield* a.pendingRequests.fromTask(sessionId, {
          ...base,
          operation: { type: "read" },
        });
        if (!("kind" in read) || read.revision === undefined)
          return yield* Effect.die("Missing qualified read");
        const action = {
          ...base,
          operation: {
            type: "respond" as const,
            clientRequestId: "human369-answer",
            expectedRevision: read.revision,
            answers: { color: "Orange" },
          },
        };
        const accepted = yield* a.pendingRequests.fromTask(sessionId, action);
        expect(yield* a.pendingRequests.fromTask(sessionId, action)).toEqual(accepted);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (m) => m.text === "Which color?\nOrange",
          ),
        ).toHaveLength(1);
        yield* linkTo(b.links, a, "full-access");
        const sourceGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...sourceGrant, onward: true });
        const back = yield* delegate(
          b,
          a,
          child.childId,
          "pending369-return-to-earlier-environment",
        );
        yield* activate(a, back.childId);
        const backRequest = yield* seedPending369(a, back.childId);
        const nativeRead = yield* nativeCall(a, parent, "t3_pending_request_read", {
          threadId: back.childId,
          requestId: backRequest,
        });
        expect(nativeRead.isError, json(nativeRead.content)).not.toBe(true);
        const backBase = {
          ...base,
          requestId: backRequest,
          requestTarget: { environmentId: a.environmentId, threadId: back.childId },
          requestRoute: [{ environmentId: a.environmentId, threadId: back.childId }],
        };
        const backRead = yield* a.pendingRequests.fromTask(sessionId, {
          ...backBase,
          operation: { type: "read" },
        });
        if (!("kind" in backRead) || backRead.revision === undefined)
          return yield* Effect.die("Missing returned descendant request");
        const backAnswer = {
          ...backBase,
          operation: {
            type: "respond" as const,
            expectedRevision: backRead.revision,
            clientRequestId: "human369-back",
            answers: { color: "Silver" },
          },
        };
        const backAccepted = yield* a.pendingRequests.fromTask(sessionId, backAnswer);
        expect(yield* a.pendingRequests.fromTask(sessionId, backAnswer)).toEqual(backAccepted);
        expect(
          (yield* a.threads.getThreadRecords(back.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nSilver",
          ),
        ).toHaveLength(1);
        yield* sessions.revoke({ sessionId, revokedAt: now });
        const denied = yield* a.pendingRequests.fromTask(sessionId, action).pipe(Effect.result);
        expect(denied._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: local-equivalent remote approval commits once and the real native effect calls its provider once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-approve-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-approve-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-approve-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-approve-child");
        yield* activate(b, child.childId);
        const projection = yield* b.orchestrator.getThreadProjection(child.childId);
        const providerSessionId = projection.providerThreads[0]!.providerSessionId!;
        const now = yield* DateTime.now;
        const providerSession = {
          id: providerSessionId,
          driver,
          providerInstanceId: instanceId,
          status: "running" as const,
          cwd: process.cwd(),
          model: selection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-approve-session"),
              type: "provider-session.attached",
              threadId: child.childId,
              occurredAt: now,
              payload: providerSession,
            },
          ],
        });
        const requestId = RuntimeRequestId.make("pending369-approval");
        const nodeId = NodeId.make("pending369-approval-node");
        const common = { threadId: child.childId, nodeId, occurredAt: now };
        yield* b.sink.write({
          events: [
            {
              ...common,
              id: EventId.make("pending369-approval-node"),
              type: "node.updated",
              payload: {
                id: nodeId,
                threadId: child.childId,
                runId: null,
                parentNodeId: null,
                rootNodeId: nodeId,
                kind: "approval_request",
                status: "waiting",
                countsForRun: false,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                runtimeRequestId: requestId,
                checkpointScopeId: null,
                startedAt: now,
                completedAt: null,
              },
            },
            {
              ...common,
              id: EventId.make("pending369-approval-request"),
              type: "runtime-request.updated",
              payload: {
                id: requestId,
                nodeId,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "command",
                status: "pending",
                responseCapability: { type: "live", providerSessionId },
                createdAt: now,
                resolvedAt: null,
              },
            },
            {
              ...common,
              id: EventId.make("pending369-approval-item"),
              type: "turn-item.updated",
              payload: {
                id: TurnItemId.make("pending369-approval-item"),
                type: "approval_request",
                threadId: child.childId,
                runId: null,
                nodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 1,
                status: "waiting",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                requestId,
                requestKind: "command",
                prompt: "Run the requested command?",
                options: [
                  { decision: "accept", label: "Approve" },
                  { decision: "decline", label: "Deny" },
                ],
              },
            },
          ],
        });
        yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parent,
            afterSequence: 0,
            eventType: "subagent.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "subagent.updated" &&
                stored.event.payload.id === child.taskId &&
                (stored.event.payload.remoteChild?.snapshot?.requests.some(
                  (request) => request.requestId === requestId,
                ) ??
                  false),
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect((yield* a.threads.getThreadShell(parent))?.remotePendingRequest).toBe("approval");
        const status = yield* nativeCall(a, parent, "task_status", { taskId: child.taskId });
        expect(status.isError, json(status.content)).not.toBe(true);
        expect(status.structuredContent).toMatchObject({
          hasPendingApproval: true,
          childThreadId: null,
        });
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        expect(content.kind).toBe("approval");
        const malformed = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          decision: "cancel",
          clientRequestId: "bad-shape",
        });
        expect(malformed.isError).toBe(true);
        const answer = {
          ...args,
          expectedRevision: content.revision,
          decision: "accept",
          clientRequestId: "native-approval",
        };
        yield* Ref.set(a.dropCommittedToolReply, "t3_pending_request_respond");
        const lost = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(lost.isError).toBe(true);
        const accepted = yield* nativeCall(a, parent, "t3_pending_request_respond", answer);
        expect(accepted.isError, json(accepted.content)).not.toBe(true);
        expect(
          (yield* nativeCall(a, parent, "t3_pending_request_respond", answer)).structuredContent,
        ).toEqual(accepted.structuredContent);
        const callbacks: Array<Adapter.ProviderAdapterV2RuntimeRequestResponseInput> = [];
        const sessions = Context.get(b.runtime, ProviderSessionManager.ProviderSessionManagerV2);
        const spy = vi.spyOn(sessions, "get").mockReturnValue(
          Effect.succeed(
            Option.some({
              providerSession,
              instanceId,
              driver,
              providerSessionId,
              events: Stream.empty,
              ensureThread: () => Effect.die("No external provider process in this proof"),
              resumeThread: () => Effect.die("No external provider process in this proof"),
              startTurn: () => Effect.die("No external provider process in this proof"),
              steerTurn: () => Effect.die("No external provider process in this proof"),
              interruptTurn: () => Effect.die("No external provider process in this proof"),
              readThreadSnapshot: () => Effect.die("No external provider process in this proof"),
              rollbackThread: () => Effect.die("No external provider process in this proof"),
              forkThread: () => Effect.die("No external provider process in this proof"),
              respondToRuntimeRequest: (
                input: Adapter.ProviderAdapterV2RuntimeRequestResponseInput,
              ) =>
                Effect.sync(() => {
                  callbacks.push(input);
                }),
            } satisfies Adapter.ProviderAdapterV2SessionRuntime),
          ),
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
        const native = yield* RuntimeRequests.layer.pipe(
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.build,
        );
        const runtimeRequests = Context.get(native, RuntimeRequests.RuntimeRequestServiceV2);
        const worker = yield* EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(
          Layer.provide(
            Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
              execute: (effect) =>
                effect.request.type === "runtime-request.respond"
                  ? runtimeRequests
                      .respond({
                        threadId: effect.threadId,
                        providerSessionId: effect.request.providerSessionId,
                        requestId: effect.request.requestId,
                        ...(effect.request.decision === undefined
                          ? {}
                          : { decision: effect.request.decision }),
                        ...(effect.request.answers === undefined
                          ? {}
                          : { answers: effect.request.answers }),
                      })
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new EffectWorker.OrchestrationEffectExecutionError({
                              effectId: effect.id,
                              effectType: effect.request.type,
                              cause,
                            }),
                        ),
                      )
                  : Effect.void,
            }),
          ),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.build,
        );
        const drain = Context.get(worker, EffectWorker.OrchestrationEffectWorkerV2).drain();
        yield* drain;
        yield* drain;
        expect(callbacks).toEqual([{ requestId, decision: "accept" }]);
        const self = yield* nativeCall(b, child.childId, "t3_pending_request_respond", {
          threadId: child.childId,
          requestId,
          decision: "accept",
          actor: "human",
          human: true,
          clientRequestId: "spoof",
        });
        expect(self.isError).toBe(true);
        expect(callbacks).toHaveLength(1);
        const stoppingRequest = yield* seedPending369(b, child.childId, "live-stop");
        const stoppingRecords = yield* b.threads.getThreadRecords(child.childId, [
          "nodes",
          "runtimeRequests",
          "turnItems",
        ]);
        const stoppedRunId = (yield* b.threads.getThreadShell(child.childId))?.activeRunId;
        if (stoppedRunId == null)
          return yield* Effect.die("Expected a live native run before Stop");
        const stoppingNode = stoppingRecords.nodes.find(
          (node) => node.runtimeRequestId === stoppingRequest,
        )!;
        const stoppingRuntimeRequest = stoppingRecords.runtimeRequests.find(
          (request) => request.id === stoppingRequest,
        )!;
        const stoppingItem = stoppingRecords.turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === stoppingRequest,
        );
        if (stoppingItem?.type !== "user_input_request")
          return yield* Effect.die("Expected live question item");
        const stopNow = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-live-stop-node"),
              type: "node.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: { ...stoppingNode, runId: stoppedRunId },
            },
            {
              id: EventId.make("pending369-live-stop-request"),
              type: "runtime-request.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: {
                ...stoppingRuntimeRequest,
                responseCapability: { type: "live", providerSessionId },
              },
            },
            {
              id: EventId.make("pending369-live-stop-item"),
              type: "turn-item.updated",
              threadId: child.childId,
              runId: stoppedRunId,
              nodeId: stoppingNode.id,
              occurredAt: stopNow,
              payload: { ...stoppingItem, runId: stoppedRunId, responseMode: undefined },
            },
          ],
        });
        const stopArgs = { ...args, requestId: stoppingRequest };
        const stopRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", stopArgs)).structuredContent,
        );
        const stopped = yield* nativeCall(a, parent, "t3_thread_interrupt", {
          environmentId: b.environmentId,
          threadId: child.childId,
          clientRequestId: "pending369-stop-before-answer",
        });
        expect(stopped.isError, json(stopped.content)).not.toBe(true);
        const tooLate = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...stopArgs,
          expectedRevision: stopRead.revision,
          answers: { color: "Late" },
          clientRequestId: "pending369-after-stop",
        });
        expect(tooLate.isError, json(tooLate.content)).toBe(true);
        const stopSql = Context.get(b.database, SqlClient.SqlClient);
        const previousEffects = yield* stopSql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE effect_type='runtime-request.respond' AND json_extract(payload_json,'$.requestId')=${requestId}`;
        expect(previousEffects[0]?.count).toBe(1);
        const lateEffects = yield* stopSql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE effect_type='runtime-request.respond' AND json_extract(payload_json,'$.requestId')=${stoppingRequest}`;
        expect(lateEffects[0]?.count).toBe(0);
        expect(callbacks).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("pending369: asynchronous answer and dismiss race in the native serialized owner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* makeNode(
        descriptorOf("pending369-race-a", "A", { peerAuthority: 1 }),
        "disk",
        { attachmentRequests: true },
      );
      const b = yield* makeNode(
        descriptorOf("pending369-race-b", "B", { peerAuthority: 1 }),
        "disk",
        { attachmentRequests: true },
      );
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("pending369-race-parent");
      yield* seedParent(a, parent);
      const child = yield* delegate(a, b, parent, "pending369-race-child");
      yield* activate(b, child.childId);
      const requestId = yield* seedPending369(b, child.childId);
      const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
      const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
      const content = yield* decodePending369Content(read.structuredContent);
      const outside = yield* Context.get(a.services, PeerMcpClient.PeerMcpClient)
        .call({
          environmentId: b.environmentId,
          tool: "t3_pending_request_read",
          arguments: args,
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          success: ThreadToolkit.tools.t3_pending_request_read.successSchema,
        })
        .pipe(Effect.result);
      expect(outside._tag).toBe("Failure");
      const raced = yield* Effect.all(
        [
          nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: content.revision,
            answers: { color: "Purple" },
            clientRequestId: "answer-race",
          }),
          nativeCall(a, parent, "t3_pending_request_dismiss", {
            ...args,
            expectedRevision: content.revision,
            clientRequestId: "dismiss-race",
          }),
        ],
        { concurrency: "unbounded" },
      );
      expect(raced.filter((result) => result.isError !== true)).toHaveLength(1);
      const records = yield* b.threads.getThreadRecords(child.childId, [
        "runtimeRequests",
        "messages",
      ]);
      expect(records.runtimeRequests.find((request) => request.id === requestId)?.status).toBe(
        "resolved",
      );
      expect(
        records.messages.filter((message) => message.text === "Which color?\nPurple"),
      ).toHaveLength(raced[0]?.isError === true ? 0 : 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: real authenticated source WebSocket invokes the production request handlers and rejects agent credentials",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-ws-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-ws-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-ws-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-ws-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const server = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
        const route = HttpRouter.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const authenticated = yield* a.auth
              .authenticateWebSocketUpgrade(request)
              .pipe(Effect.result);
            if (authenticated._tag === "Failure") return HttpServerResponse.empty({ status: 403 });
            const session = authenticated.success;
            const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
            yield* RpcServer.make(PendingRequestClientRpcGroup, {
              disableTracing: true,
              disableFatalDefects: true,
            }).pipe(
              Effect.provideService(RpcServer.Protocol, protocol),
              Effect.provide(
                Layer.merge(
                  PendingRequestClientRpcGroup.toLayer(
                    pendingRequestClientHandlers(session, a.pendingRequests),
                  ),
                  RpcAuthorization.layer(session.scopes),
                ),
              ),
              Effect.forkScoped,
            );
            return yield* httpEffect;
          }),
        );
        yield* HttpRouter.serve(route, { disableListenLog: true, disableLogger: true }).pipe(
          Layer.provide(Layer.succeedContext(server)),
          Layer.provide(RpcSerialization.layerJson),
          Layer.fresh,
          Layer.build,
        );
        const address = Context.get(server, HttpServer.HttpServer).address;
        if (address._tag === "UnixPathAddress") return yield* Effect.die("Expected test socket");
        const messages = yield* Queue.unbounded<string>();
        const connect = (ticket: string) =>
          Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                new Promise<WebSocket>((resolve, reject) => {
                  const socket = new WebSocket(
                    `ws://127.0.0.1:${address.port}/ws?wsTicket=${encodeURIComponent(ticket)}`,
                  );
                  socket.addEventListener("message", (event) =>
                    Queue.offerUnsafe(messages, String(event.data)),
                  );
                  socket.addEventListener("open", () => resolve(socket), { once: true });
                  socket.addEventListener(
                    "error",
                    () => reject(new Error("Unauthenticated source socket refused")),
                    { once: true },
                  );
                }),
              catch: () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Isolated source WebSocket failed",
                }),
            }),
            (socket) => Effect.sync(() => socket.close()),
          );
        const pairing = yield* a.auth.issuePairingCredential({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const token = yield* a.auth.exchangeBootstrapCredentialForAccessToken(
          pairing.credential,
          undefined,
          { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
        );
        const session = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${token.access_token}` } }),
          ),
        );
        const ticket = yield* a.auth.issueWebSocketTicket(session);
        const socket = yield* connect(ticket.ticket);
        let ordinal = 0;
        const rpc = (tag: string, payload: unknown, client = socket) =>
          Effect.gen(function* () {
            const id = String(++ordinal);
            client.send(json({ _tag: "Request", id, tag, payload, headers: [] }));
            const reply = yield* decodePendingWsReply(yield* Queue.take(messages));
            expect(reply.requestId).toBe(id);
            return reply;
          });
        const base = {
          parentThreadId: parent,
          taskId: child.taskId,
          requestId,
          requestTarget: { environmentId: b.environmentId, threadId: child.childId },
        };
        const read = yield* rpc(WS_METHODS.remoteTaskRequestRead, {
          ...base,
          operation: { type: "read" },
        });
        expect(read.exit?._tag).toBe("Success");
        const content = yield* decodePending369ClientResult(read.exit?.value);
        if (!("kind" in content) || content.revision === undefined)
          return yield* Effect.die("Missing native qualified request");
        const answer = {
          ...base,
          operation: {
            type: "respond",
            clientRequestId: "ws369-answer",
            expectedRevision: content.revision,
            answers: { color: "Teal" },
          },
          actor: "human",
        };
        const accepted = yield* rpc(WS_METHODS.remoteTaskRequest, answer);
        expect(accepted.exit?._tag).toBe("Success");
        expect((yield* rpc(WS_METHODS.remoteTaskRequest, answer)).exit).toEqual(accepted.exit);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nTeal",
          ),
        ).toHaveLength(1);
        const peerSession = yield* a.auth.issueMcpClientSession({
          label: "Outside OAuth",
          access: "full-access",
          peerLink: false,
          client: {
            deviceType: "desktop",
            os: "fixture",
            browser: "fixture",
            ipAddress: "127.0.0.1",
          },
        });
        const impersonation = yield* a.auth
          .authenticateWebSocketUpgrade(
            HttpServerRequest.fromWeb(
              new Request(a.url, { headers: { authorization: `Bearer ${peerSession.token}` } }),
            ),
          )
          .pipe(Effect.result);
        expect(impersonation._tag).toBe("Failure");
        const readPairing = yield* a.auth.issuePairingCredential({
          scopes: ["orchestration:read"],
        });
        const readToken = yield* a.auth.exchangeBootstrapCredentialForAccessToken(
          readPairing.credential,
          undefined,
          { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
        );
        const readSession = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${readToken.access_token}` } }),
          ),
        );
        const readTicket = yield* a.auth.issueWebSocketTicket(readSession);
        const readSocket = yield* connect(readTicket.ticket);
        const secondRequestId = yield* seedPending369(b, child.childId, "read-only");
        const secondRead = yield* rpc(
          WS_METHODS.remoteTaskRequestRead,
          { ...base, requestId: secondRequestId, operation: { type: "read" } },
          readSocket,
        );
        expect(secondRead.exit?._tag).toBe("Success");
        expect(
          (yield* rpc(
            WS_METHODS.remoteTaskRequest,
            { ...answer, requestId: secondRequestId },
            readSocket,
          )).exit?._tag,
        ).toBe("Failure");
        const authRepository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(a.database)),
          Layer.build,
        );
        yield* Context.get(authRepository, AuthSessions.AuthSessionRepository).revoke({
          sessionId: session.sessionId,
          revokedAt: yield* DateTime.now,
        });
        expect((yield* rpc(WS_METHODS.remoteTaskRequest, answer)).exit?._tag).toBe("Failure");
        expect((yield* connect("unproved-agent-token").pipe(Effect.result))._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: native intake resolving after planning wins before the response transaction commits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-intake-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-intake-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true, productionKernel: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("pending369-intake-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "pending369-intake-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* nativeCall(a, parent, "t3_pending_request_read", args);
        const content = yield* decodePending369Content(read.structuredContent);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const commit = b.sink.commitCommand;
        const held = vi
          .spyOn(b.sink, "commitCommand")
          .mockImplementation((input) =>
            input.commandType !== "runtime-request.respond"
              ? commit(input)
              : Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(commit(input)),
                ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => held.mockRestore()));
        const pending = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Obsolete" },
          clientRequestId: "intake-response",
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const request = (yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
        ])).runtimeRequests.find((request) => request.id === requestId)!;
        const now = yield* DateTime.now;
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("pending369-intake-cancel"),
              type: "runtime-request.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: { ...request, status: "cancelled", resolvedAt: now },
            },
          ],
        });
        yield* Deferred.succeed(release, undefined);
        const refused = yield* Fiber.join(pending);
        expect(refused.isError, json(refused.content)).toBe(true);
        const records = yield* b.threads.getThreadRecords(child.childId, [
          "runtimeRequests",
          "messages",
        ]);
        expect(records.runtimeRequests.find((request) => request.id === requestId)?.status).toBe(
          "cancelled",
        );
        expect(records.messages.some((message) => message.text === "Which color?\nObsolete")).toBe(
          false,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369: a native local grandparent crosses a real local child and an authenticated pair",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("pending369-local-a", "A", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("pending369-local-b", "B", { peerAuthority: 1 }),
          "disk",
          { attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const root = ThreadId.make("pending369-local-root");
        yield* seedParent(a, root);
        const launched = yield* nativeCall(a, root, "delegate_task", {
          task: "Own local intermediate work",
          target: {
            projectId: a.project.id,
            providerInstanceId: instanceId,
            model: selection.model,
          },
          mode: "async",
          clientRequestId: "pending369-local-child",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const local = yield* decodePending369LocalTask(launched.structuredContent);
        if (!("childThreadId" in local) || local.childThreadId === null)
          return yield* Effect.die("Expected a real native local child");
        expect((yield* a.threads.getThreadShell(local.childThreadId))?.lineage).toMatchObject({
          parentThreadId: root,
          relationshipToParent: "subagent",
        });
        yield* activate(a, local.childThreadId);
        const remote = yield* delegate(a, b, local.childThreadId, "pending369-local-pair");
        yield* activate(b, remote.childId);
        const requestId = yield* seedPending369(b, remote.childId);
        const args = { environmentId: b.environmentId, threadId: remote.childId, requestId };
        const read = yield* nativeCall(a, root, "t3_pending_request_read", args);
        expect(read.isError, json(read.content)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        const answered = yield* nativeCall(a, root, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Amber" },
          clientRequestId: "pending369-local-grandparent",
        });
        expect(answered.isError, json(answered.content)).not.toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(remote.childId, ["messages"])).messages.filter(
            (message) => message.text === "Which color?\nAmber",
          ),
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "pending369 R1: read content and revision describe one snapshot",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("r1-read-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("r1-read-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r1-read-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "r1-read-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const original = b.threads.getProjectThreadRecords;
        let once = true;
        // Scheduling barrier only: the real native owner executes and its exact result is retained.
        // No authority decision, projection value or SQL operation is replaced.
        const barrier = vi
          .spyOn(b.threads, "getProjectThreadRecords")
          .mockImplementation((...input) =>
            original(...input).pipe(
              Effect.flatMap((records) => {
                if (
                  !once ||
                  input[0].threadId !== child.childId ||
                  !input[1]?.includes("turnItems")
                )
                  return Effect.succeed(records);
                once = false;
                return Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(records),
                );
              }),
            ),
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => barrier.mockRestore()));
        const reading = yield* nativeCall(a, parent, "t3_pending_request_read", args).pipe(
          Effect.forkScoped,
        );
        yield* Deferred.await(entered);
        const records = yield* b.threads.getThreadRecords(child.childId, ["turnItems"]);
        const item = records.turnItems.find(
          (i) => i.type === "user_input_request" && i.requestId === requestId,
        );
        if (item?.type !== "user_input_request") return yield* Effect.die("Missing native item");
        const changed = "Which UPDATED color?";
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-read-change"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...item,
                questions: item.questions.map((q) => ({ ...q, question: changed })),
              },
            },
          ],
        });
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(reading);
        expect(result.isError, json(result)).not.toBe(true);
        const content = yield* decodePending369Content(result.structuredContent);
        const accepted = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Blue" },
          clientRequestId: "r1-old-content-answer",
        });
        const after = yield* b.threads.getThreadRecords(child.childId, [
          "messages",
          "runtimeRequests",
        ]);
        yield* Effect.log(
          "R1 READ RACE EVIDENCE",
          json({
            content,
            accepted,
            updatedMessage: after.messages.find((m) => m.text.includes(changed)),
            status: after.runtimeRequests.find((r) => r.id === requestId)?.status,
          }),
        );
        // Either read must return updated content, or its stale revision must reject the answer.
        expect(
          (content.kind === "user-input" && content.questions[0]?.question === changed) ||
            accepted.isError === true,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30000,
);

it.live(
  "pending369 R1: advertised empty live option retains local-equivalent response",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("r1-empty-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("r1-empty-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r1-empty-parent");
        yield* seedParent(a, parent);
        const child = yield* delegate(a, b, parent, "r1-empty-child");
        yield* activate(b, child.childId);
        const requestId = yield* seedPending369(b, child.childId);
        const projection = yield* b.orchestrator.getThreadProjection(child.childId);
        const providerSessionId = projection.providerThreads[0]!.providerSessionId!;
        const now = yield* DateTime.now;
        const request = projection.runtimeRequests.find((r) => r.id === requestId)!;
        const item = projection.turnItems.find(
          (i) => i.type === "user_input_request" && i.requestId === requestId,
        );
        if (item?.type !== "user_input_request")
          return yield* Effect.die("Missing real pending item");
        // This is the exact input/select empty-value shape produced by piQuestion
        // in packages/provider-pi/src/server/adapter.ts. No invented optional flag.
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-empty-session"),
              type: "provider-session.attached",
              threadId: child.childId,
              occurredAt: now,
              payload: {
                id: providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "running",
                cwd: process.cwd(),
                model: selection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
            },
            {
              id: EventId.make("r1-empty-request"),
              type: "runtime-request.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: { ...request, responseCapability: { type: "live", providerSessionId } },
            },
            {
              id: EventId.make("r1-empty-item"),
              type: "turn-item.updated",
              threadId: child.childId,
              nodeId: request.nodeId,
              occurredAt: now,
              payload: {
                ...item,
                responseMode: undefined,
                questions: [
                  {
                    id: "color",
                    header: "Input",
                    question: "Value?",
                    options: [
                      {
                        label: "Submit empty value",
                        description: "Send an empty string to the extension.",
                        value: "",
                      },
                    ],
                  },
                ],
              },
            },
          ],
        });
        const args = { environmentId: b.environmentId, threadId: child.childId, requestId };
        const read = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", args)).structuredContent,
        );
        for (const [index, answers] of [{}, { color: [""] }].entries()) {
          const rejected = yield* nativeCall(a, parent, "t3_pending_request_respond", {
            ...args,
            expectedRevision: read.revision,
            answers,
            clientRequestId: `r1-empty-invalid-${index}`,
          });
          expect(rejected.isError).toBe(true);
        }
        const remote = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...args,
          expectedRevision: read.revision,
          answers: { color: "" },
          clientRequestId: "r1-empty-answer",
        });
        const after = yield* b.threads.getThreadRecords(child.childId, ["runtimeRequests"]);
        expect(remote.isError, json(remote)).not.toBe(true);
        expect(after.runtimeRequests.find((r) => r.id === requestId)).toMatchObject({
          status: "resolved",
          answers: { color: "" },
        });
        const fixedId = yield* seedPending369(b, child.childId, "fixed-choice-control");
        const fixedItem = (yield* b.threads.getThreadRecords(child.childId, [
          "turnItems",
        ])).turnItems.find(
          (entry) => entry.type === "user_input_request" && entry.requestId === fixedId,
        );
        if (fixedItem?.type !== "user_input_request")
          return yield* Effect.die("Missing fixed choice control");
        yield* b.sink.write({
          events: [
            {
              id: EventId.make("r1-fixed-shape"),
              type: "turn-item.updated",
              threadId: child.childId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...fixedItem,
                questions: [
                  {
                    id: "color",
                    header: "Choice",
                    question: "Choose?",
                    allowCustomAnswer: false,
                    options: [{ label: "Blue", description: "Allowed" }],
                  },
                ],
              },
            },
          ],
        });
        const fixedArgs = { ...args, requestId: fixedId };
        const fixedRead = yield* decodePending369Content(
          (yield* nativeCall(a, parent, "t3_pending_request_read", fixedArgs)).structuredContent,
        );
        const invalidOption = yield* nativeCall(a, parent, "t3_pending_request_respond", {
          ...fixedArgs,
          expectedRevision: fixedRead.revision,
          answers: { color: "not offered" },
          clientRequestId: "r1-fixed-invalid",
        });
        expect(invalidOption.isError).toBe(true);
        expect(
          (yield* b.threads.getThreadRecords(child.childId, [
            "runtimeRequests",
          ])).runtimeRequests.find((entry) => entry.id === fixedId)?.status,
        ).toBe("pending");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30000,
);

it.live(
  "review383-read-capture: readonly capture rechecks after external readiness",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair("review383-read-capture");
        const opened = yield* open("read-capture-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const held = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
        );
        const running = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "held-click",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(held);
        yield* Effect.gen(function* () {
          const input = { ...route, hostId: "local", deviceId: "fixture-device382" };
          const lawful = yield* nativeCall(a, parent, "device_screenshot", input);
          expect(lawful.isError, json(lawful)).not.toBe(true);
          const before = yield* Ref.get(b.deviceScreenshotCalls);
          const grant = (yield* b.authority.grants)[0]!;
          const armed = yield* Ref.make(true);
          yield* Ref.set(
            b.deviceReadinessHook,
            Effect.gen(function* () {
              if (!(yield* Ref.getAndSet(armed, false))) return;
              yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
            }),
          );
          const revoked = yield* nativeCall(a, parent, "device_screenshot", input);
          expect(yield* Ref.get(armed)).toBe(false);
          expect((yield* b.authority.grants)[0]!.device).toBe(false);
          expect(revoked.isError, "revoked read must deny before capture").toBe(true);
          expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(before);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(running);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "review383-local-auto: keyed local opening preserves existing capability policy",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const n = yield* makeNode(
          descriptorOf("review383-local", "local", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        const parent = ThreadId.make("review383-local-parent");
        yield* seedParent(n, parent);
        const args = { deviceId: "fixture-device382", platform: "android" };
        const full = yield* nativeCall(n, parent, "device_open", {
          ...args,
          clientRequestId: "full",
        });
        expect(full.isError, json(full)).not.toBe(true);
        yield* n.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("review383-auto"),
          threadId: parent,
          runtimeMode: "auto",
        });
        const keyed = yield* nativeCall(n, parent, "device_open", {
          ...args,
          clientRequestId: "auto",
        });
        expect(keyed.isError, json(keyed)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-literals: registered empty/CR fill and app deep link preserve exact bound intent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-literals");
        const opened = yield* open("literal-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const texts = [
          "",
          "abc\r",
          "\n",
          "\r\n",
          "first\nsecond",
          "--config /rival",
          '"quoted" Ω🦊',
        ];
        for (const [index, text] of texts.entries()) {
          const input = {
            ...route,
            target,
            command: { type: "fill", ref: "@e1", text },
            clientRequestId: `literal-${index}`,
          };
          const reply = yield* nativeCall(a, parent, "device_execute", input);
          expect(reply.structuredContent).toEqual({ type: "action", target, status: "dispatched" });
          const call = b.deviceProcessCalls.at(-1)!;
          expect(call.args[1]).toBe("fill");
          const args = call.args.slice(call.args.indexOf("--") + 1);
          expect(args).toEqual(text === "" ? ["@e1", ""] : ["@e1"]);
          expect(call.stdin).toBe(text === "" ? undefined : text + "\r\n");
          const beforeReplay = yield* footprint();
          expect((yield* nativeCall(a, parent, "device_execute", input)).structuredContent).toEqual(
            reply.structuredContent,
          );
          expect(yield* footprint()).toEqual(beforeReplay);
        }
        const command = { type: "open", app: "com.example.app", url: "example://settings?tab=one" };
        const input = { ...route, target, command, clientRequestId: "deep-link" };
        expect((yield* nativeCall(a, parent, "device_execute", input)).structuredContent).toEqual({
          type: "action",
          target,
          status: "dispatched",
        });
        const call = b.deviceProcessCalls.at(-1)!;
        expect(call.args[1]).toBe("open");
        expect(call.args.slice(call.args.indexOf("--") + 1)).toEqual([command.app, command.url]);
        const beforeInvalid = yield* footprint();
        for (const url of ["--config", "example://bad\nlink", "x:" + "a".repeat(8192)])
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", {
              ...input,
              command: { ...command, url },
            })).error.code,
          ).toBe(-32602);
        expect(yield* footprint()).toEqual(beforeInvalid);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each(["android", "ios"] as const)(
  "typed383-shutdown: %s readiness-time grant retirement denies shutdown outside stopped-device recovery",
  (platform) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route } = yield* typed383Pair(`typed383-shutdown-${platform}`);
        yield* Ref.set(b.deviceHubPlatform, platform);
        yield* Ref.update(b.deviceCapabilities, (c) => ({
          ...c,
          device: { ...c.device, platform, kind: platform === "ios" ? "simulator" : "emulator" },
        }));
        const open = (key: string) =>
          nativeCall(a, parent, "device_open", {
            ...route,
            deviceId: "fixture-device382",
            platform,
            clientRequestId: key,
          });
        const close = (key: string) =>
          nativeCall(a, parent, "device_close", {
            ...route,
            hostId: "local",
            deviceId: "fixture-device382",
            shutdown: true,
            clientRequestId: key,
          });
        const firstOpen = yield* open("lawful-open");
        expect(firstOpen.isError, json(firstOpen)).not.toBe(true);
        const lawful = yield* close("lawful-close");
        expect(lawful.isError, json(lawful)).not.toBe(true);
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        const nextOpen = yield* open("revoked-open");
        expect(nextOpen.isError, json(nextOpen)).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        const armed = yield* Ref.make(true);
        yield* Ref.set(
          b.deviceReadinessHook,
          Effect.gen(function* () {
            if (!(yield* Ref.getAndSet(armed, false))) return;
            yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
            // A refusal caught by the iOS already-stopped fallback must not become success.
            yield* Ref.set(b.deviceHubBooted, false);
          }),
        );
        const revoked = yield* close("revoked-close");
        expect(yield* Ref.get(armed)).toBe(false);
        expect((yield* b.authority.grants)[0]!.device).toBe(false);
        expect(
          yield* Ref.get(b.deviceShutdownCalls),
          "no new shutdown after observed grant retirement",
        ).toBe(1);
        expect(
          revoked.isError,
          "retired grant must deny shutdown even when the hub reports stopped",
        ).toBe(true);
        expect(json(revoked)).toContain("outcome_unknown");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const unknown = yield* sql`SELECT id FROM mcp_request_receipts WHERE state='started'`;
        expect(unknown.length).toBeGreaterThan(0);
        yield* b.authority.setGrant({ ...grant, device: true });
        expect(json(yield* close("revoked-close"))).toContain("outcome_unknown");
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        yield* Ref.set(b.deviceHubBooted, true);
        const later = yield* open("later-open");
        expect(later.isError, json(later)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(later.structuredContent);
        expect(json(yield* close("revoked-close"))).toContain("outcome_unknown");
        expect(yield* Ref.get(b.deviceShutdownCalls)).toBe(1);
        const click = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "later-click",
        });
        expect(click.isError, json(click)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "pending532: actual source role and destination policy compose with native ancestry and concurrent answers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("pending532-a", "A", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        const b = yield* makeNode(descriptorOf("pending532-b", "B", { peerAuthority: 1 }), "disk", {
          attachmentRequests: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const promachos = ThreadId.make("pending532-promachos");
        const worker = ThreadId.make("pending532-worker");
        const target = ThreadId.make("pending532-target");
        yield* seedParent(a, promachos, undefined, "promachos");
        yield* seedParent(a, worker);
        yield* seedParent(b, target);
        const requestId = yield* seedPending369(b, target);
        const args = {
          environmentId: b.environmentId,
          threadId: target,
          requestId,
          requestTarget: { environmentId: b.environmentId, threadId: target },
        };
        // A full runtime mode does not widen the default created-work resource grant.
        expect((yield* nativeCall(a, promachos, "t3_pending_request_read", args)).isError).toBe(
          true,
        );
        const grant = (yield* b.authority.grants)[0];
        if (grant === undefined) return yield* Effect.die("Missing fixture grant");
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        expect((yield* nativeCall(a, worker, "t3_pending_request_read", args)).isError).toBe(true);
        const read = yield* nativeCall(a, promachos, "t3_pending_request_read", args);
        expect(read.isError, json(read)).not.toBe(true);
        const content = yield* decodePending369Content(read.structuredContent);
        const answer = {
          ...args,
          expectedRevision: content.revision,
          answers: { color: "Gold" },
          clientRequestId: "pending532-answer",
        };
        // Current source mode is revalidated instead of borrowing the destination's full access.
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("pending532-plan"),
          threadId: promachos,
          interactionMode: "plan",
        });
        expect(
          (yield* nativeCall(a, promachos, "t3_pending_request_respond", answer)).isError,
        ).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("pending532-default"),
          threadId: promachos,
          interactionMode: "default",
        });
        const concurrent = yield* Effect.all(
          [
            nativeCall(a, promachos, "t3_pending_request_respond", answer),
            nativeCall(a, promachos, "t3_pending_request_respond", {
              ...answer,
              clientRequestId: "pending532-rival",
              answers: { color: "Silver" },
            }),
          ],
          { concurrency: "unbounded" },
        );
        expect(concurrent.filter((result) => result.isError !== true)).toHaveLength(1);
        const records = yield* b.threads.getThreadRecords(target, [
          "runtimeRequests",
          "turnItems",
          "messages",
        ]);
        const answeredBy = { kind: "promachos", threadId: promachos, via: "T3 Code · A" };
        expect(records.runtimeRequests.find((request) => request.id === requestId)).toMatchObject({
          status: "resolved",
          answeredBy,
        });
        expect(
          records.turnItems.find(
            (item) => item.type === "user_input_request" && item.requestId === requestId,
          ),
        ).toMatchObject({ answeredBy });
        expect(
          records.messages.filter((message) => message.text.startsWith("Which color?\n")),
        ).toHaveLength(1);
        // A remote delegated child stays with its actual parent, even for a real Promachos.
        const delegated = yield* delegate(a, b, worker, "pending532-owned");
        yield* activate(b, delegated.childId);
        const childRequest = yield* seedPending369(b, delegated.childId);
        const childArgs = {
          environmentId: b.environmentId,
          threadId: delegated.childId,
          requestId: childRequest,
        };
        expect(
          (yield* nativeCall(a, promachos, "t3_pending_request_read", childArgs)).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, worker, "t3_pending_request_read", childArgs)).isError,
        ).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);
