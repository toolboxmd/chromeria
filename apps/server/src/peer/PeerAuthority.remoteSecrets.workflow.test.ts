import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as TestClock from "effect/testing/TestClock";
import * as RemoteRequests from "../secrets/RemoteSecretRequests.ts";
import { CommandId, MessageId, remoteSecretIntent } from "@t3tools/contracts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as RemoteSecretRequests from "../secrets/RemoteSecretRequests.ts";
import * as DateTime from "effect/DateTime";
import { EventId } from "@t3tools/contracts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Independent HMAC oracle for isolated webhook bytes.
import * as NodeCrypto from "node:crypto";
import * as Ref from "effect/Ref";
import { NodeHttpServer } from "@effect/platform-node";
import {
  SecretRequestClientRpcGroup,
  WS_METHODS,
  OrchestratorMcpRequestSecretResult,
  McpScheduledMutationReceipt,
  RemoteSecretReceiptIdentity,
} from "@t3tools/contracts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcServer, RpcSerialization } from "effect/rpc";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as SecretRequestIngress from "../secrets/SecretRequestIngress.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as Queue from "effect/Queue";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ThreadId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as QuestionReactor from "../orchestration-v2/DelegatedTaskQuestionReactor.ts";
import { RuntimeRequestId, ProviderSessionId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import {
  caller,
  makeNode,
  nativeCall,
  seedParent,
  json,
  decodePendingWsReply,
  automationWorkspace,
  activate,
  traceAutomationValidation,
} from "./PeerAuthority.workflow.fixture.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
const decodeSecretResult = Schema.decodeUnknownEffect(OrchestratorMcpRequestSecretResult);
const decodeMutationReceipt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(McpScheduledMutationReceipt),
);
const decodeSecretConsumer = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ request: RemoteSecretReceiptIdentity })),
);

it.live(
  "secret385 cold-link first control POST validates concurrently without a destination thread",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* makeNode(descriptorOf("secret385-b", "B", { peerAuthority: 1 }), "disk", {
          remoteSecrets: true,
          productionKernel: true,
        });
        const a = yield* makeNode(descriptorOf("secret385-a", "A", { peerAuthority: 1 }), "disk", {
          remoteSecrets: true,
          productionKernel: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const link = Option.getOrThrow(yield* a.links.get(b.environmentId));
        const parent = ThreadId.make("secret385-parent");
        yield* seedParent(a, parent);
        const peers = Context.get(a.services, PeerMcpClient.PeerMcpClient);
        if (peers.remoteSecret === undefined)
          return yield* Effect.die("Missing native secret route");
        // Version/family/consumer handshake enrolls the cold link, without a tool call or delegation.
        yield* peers.remoteSecret(b.environmentId, link.launchIdentity);
        const [grant] = yield* b.authority.grants;
        if (grant === undefined) return yield* Effect.die("Missing isolated link grant");
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const input = {
          type: "create" as const,
          projectId: b.project.id,
          key: "cold-secret",
          family: "schedule.webhook-signature" as const,
          label: "Fixture signature",
          reason: "Native cold-link proof",
        };
        const envelope = yield* a.authority.prepare(
          caller(a, parent),
          b.environmentId,
          link.launchIdentity,
          "remote_secret",
          input,
          { runtimeMode: "full-access", interactionMode: "default" },
          undefined,
          false,
          true,
        );
        const result = yield* peers.remoteSecret(b.environmentId, link.launchIdentity, {
          authority: envelope,
          body: input,
        });
        expect(result?.status).toBe("pending");
        expect(result?.handle.environmentId).toBe(b.environmentId);
        const bSql = Context.get(b.database, SqlClient.SqlClient);
        const requests = yield* bSql<{
          source_record_id: string;
          origin_json: string;
        }>`SELECT source_record_id,origin_json FROM remote_secret_requests`;
        expect(requests).toHaveLength(1);
        expect(requests[0]?.origin_json).toContain("secret385-parent");
        expect(yield* bSql`SELECT thread_id FROM orchestration_v2_projection_threads`).toHaveLength(
          0,
        );
        expect(yield* bSql`SELECT run_id FROM orchestration_v2_projection_runs`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const pair = (id: string) =>
  Effect.gen(function* () {
    const options = {
      remoteSecrets: true,
      productionKernel: true,
      attachmentRequests: true,
      ownerWorkspace: yield* automationWorkspace,
    };
    const b = yield* makeNode(descriptorOf(`${id}-b`, "B", { peerAuthority: 1 }), "disk", options);
    const a = yield* makeNode(descriptorOf(`${id}-a`, "A", { peerAuthority: 1 }), "disk", options);
    yield* linkTo(a.links, b, "full-access");
    const link = Option.getOrThrow(yield* a.links.get(b.environmentId));
    const peers = Context.get(a.services, PeerMcpClient.PeerMcpClient);
    if (peers.remoteSecret === undefined)
      return yield* Effect.die("Missing remote secret protocol");
    yield* peers.remoteSecret(b.environmentId, link.launchIdentity);
    const [grant] = yield* b.authority.grants;
    if (grant === undefined) return yield* Effect.die("Missing fixture grant");
    yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources", onward: true });
    const parent = ThreadId.make(`${id}-parent`);
    yield* seedParent(a, parent);
    return { a, b, parent, link, peers };
  });

const privateClient = (a: import("./PeerAuthority.workflow.fixture.ts").Node) =>
  Effect.gen(function* () {
    const server = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
    const secretService = Option.getOrThrow(
      Context.getOption(a.nativeSecrets, SecretRequests.SecretRequests),
    );
    const route = HttpRouter.add(
      "GET",
      "/ws",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const authenticated = yield* a.auth
          .authenticateWebSocketUpgrade(request)
          .pipe(Effect.result);
        if (authenticated._tag === "Failure") return HttpServerResponse.empty({ status: 403 });
        const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
        yield* RpcServer.make(SecretRequestClientRpcGroup, {
          disableTracing: true,
          disableFatalDefects: true,
        }).pipe(
          Effect.provideService(RpcServer.Protocol, protocol),
          Effect.provide(
            Layer.merge(
              SecretRequestClientRpcGroup.toLayer(
                SecretRequestIngress.clientHandlers(authenticated.success, secretService),
              ).pipe(Layer.provideMerge(Layer.succeedContext(a.secretOwners))),
              RpcAuthorization.layer(authenticated.success.scopes),
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
    if (address._tag === "UnixPathAddress")
      return yield* Effect.die("Expected native client fixture socket");
    const credential = yield* a.auth.issuePairingCredential({
      scopes: ["orchestration:read", "orchestration:operate"],
    });
    const token = yield* a.auth.exchangeBootstrapCredentialForAccessToken(
      credential.credential,
      undefined,
      { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
    );
    const session = yield* a.auth.authenticateHttpRequest(
      HttpServerRequest.fromWeb(
        new Request(a.url, { headers: { authorization: `Bearer ${token.access_token}` } }),
      ),
    );
    const ticket = yield* a.auth.issueWebSocketTicket(session);
    const messages = yield* Queue.unbounded<string>();
    const socket = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          new Promise<WebSocket>((resolve, reject) => {
            const connection = new WebSocket(
              `ws://127.0.0.1:${address.port}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`,
            );
            connection.addEventListener("message", (event) =>
              Queue.offerUnsafe(messages, String(event.data)),
            );
            connection.addEventListener("open", () => resolve(connection), { once: true });
            connection.addEventListener(
              "error",
              () => reject(new Error("Private fixture connection failed")),
              { once: true },
            );
          }),
        catch: () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Private fixture connection failed",
          }),
      }),
      (connection) => Effect.sync(() => connection.close()),
    );
    let requestOrdinal = 0;
    const answer = (input: import("@t3tools/contracts").SecretRequestAnswerInput) =>
      Effect.gen(function* () {
        socket.send(
          json({
            _tag: "Request",
            id: String(++requestOrdinal),
            tag: WS_METHODS.secretsAnswerRequest,
            payload: input,
            headers: [],
          }),
        );
        return yield* decodePendingWsReply(yield* Queue.take(messages));
      });
    return Object.assign(answer, { sessionId: session.sessionId });
  });

it.live.each([false, true])(
  "secret385 private answer, receipt-first recovery and signed idle-B delivery (lost reply: %s)",
  (loseReply) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent } = yield* pair("secret385-private");
        const reply = yield* nativeCall(a, parent, "request_secret", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          label: "Fixture signing value",
          reason: "Sign a fixture delivery",
          clientRequestId: "private-one",
        }).pipe(Effect.forkScoped);
        const cardEvent = yield* a.orchestrator
          .streamStoredEventsFrom({
            threadId: parent,
            afterSequence: 0,
            eventType: "turn-item.updated",
          })
          .pipe(
            Stream.map((stored) => stored.event),
            Stream.filter(
              (event) =>
                event.type === "turn-item.updated" && event.payload.type === "secret_request",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        if (cardEvent.type !== "turn-item.updated" || cardEvent.payload.type !== "secret_request")
          return yield* Effect.die("Missing source card");
        expect(cardEvent.payload.destination?.environmentId).toBe(b.environmentId);
        const answer = yield* privateClient(a);
        const sentinel = "fixture385-signing-material-7f20";
        const saved = yield* answer({
          threadId: parent,
          turnItemId: cardEvent.payload.id,
          answer: { type: "save", secret: sentinel },
        });
        expect(saved.exit?._tag).toBe("Success");
        const result = yield* Fiber.join(reply);
        expect(result.isError).not.toBe(true);
        const content = yield* decodeSecretResult(result.structuredContent);
        if (content.status !== "saved" || !("secretRequest" in content))
          return yield* Effect.die("Missing destination request handle");
        expect(json(content)).not.toContain("secretRef");
        expect(json(content)).not.toContain(sentinel);
        const input = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          prompt: "Accept fixture delivery",
          purposeIntent: { version: 1, intent: { type: "unscoped", reason: "deliberate" } },
          schedule: {
            type: "webhook",
            signature: {
              header: "x-signature",
              encoding: "hex",
              prefix: "sha256=",
              secretRequest: content.secretRequest,
            },
          },
          clientRequestId: "private-schedule",
        };
        if (loseReply) yield* Ref.set(a.dropCommittedToolReply, "schedule_task");
        const accepted = yield* nativeCall(a, parent, "schedule_task", input);
        expect(accepted.isError === true).toBe(loseReply);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const [confirmation] = yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM scheduled_mutation_receipts WHERE stage='confirmed'`;
        if (confirmation === undefined)
          return yield* Effect.die("Missing committed consumer receipt");
        const receipt = yield* decodeMutationReceipt(confirmation.result_json);
        const tasks = yield* sql<{
          task_id: string;
          webhook_secret: string;
        }>`SELECT task_id,webhook_secret FROM scheduled_tasks`;
        expect(tasks).toHaveLength(1);
        expect(tasks[0]?.webhook_secret === sentinel).toBe(true);
        expect(receipt.scheduledTaskId).toBe(tasks[0]?.task_id);
        expect(yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads`).toHaveLength(
          0,
        );
        expect(
          yield* sql`SELECT status FROM remote_secret_requests WHERE status='consumed'`,
        ).toHaveLength(1);
        const files = yield* FileSystem.FileSystem;
        expect(
          (yield* files.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
        expect(
          (yield* files.readDirectory(a.config.secretsDir)).filter(
            (name) => name.startsWith("secret-remote-") || name.startsWith("secret-request-"),
          ),
        ).toHaveLength(1); // local owner salt only
        expect(
          json(yield* sql`SELECT result_json,resource_json FROM scheduled_mutation_receipts`),
        ).not.toContain(sentinel);
        // The task's protected webhook_secret is authorized storage. Every other SQLite owner,
        // including event/card projections, control frames and receipts, must remain sanitized.
        for (const node of [a, b]) {
          const database = Context.get(node.database, SqlClient.SqlClient);
          const tables = yield* database<{
            name: string;
          }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='scheduled_tasks'`;
          for (const table of tables) {
            const rows = yield* database.unsafe(
              `SELECT * FROM "${table.name.replaceAll('"', '""')}"`,
            );
            expect(
              json(rows).includes(sentinel),
              `Forbidden value owner: ${node.environmentId}/${table.name}`,
            ).toBe(false);
          }
          const projectedTasks = yield* node.scheduled.list();
          expect(json(projectedTasks)).not.toContain(sentinel);
          for (const suffix of ["", "-wal"]) {
            const file = `${node.databasePath}${suffix}`;
            if (!(yield* files.exists(file))) continue;
            const bytes = yield* files.readFile(file);
            const containsFixtureValue = new TextDecoder().decode(bytes).includes(sentinel);
            if (node === a) expect(containsFixtureValue, `Source SQLite${suffix}`).toBe(false);
            yield* Effect.log("secret385 retained SQLite evidence", {
              owner:
                node === a ? "source forbidden storage" : "destination authorized webhook storage",
              suffix: suffix || "database",
              sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
              byteLength: bytes.byteLength,
              containsFixtureValue,
            });
          }
        }
        const attempts = yield* traceAutomationValidation(b);
        yield* endRun(a, parent, "secret385-source-ended");
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        if (replay.isError) yield* Effect.log(json(replay));
        expect(replay.isError).not.toBe(true);
        expect(replay.structuredContent).toEqual(receipt);
        expect(yield* attempts).toHaveLength(0);
        const absent = yield* nativeCall(a, parent, "schedule_task", {
          ...input,
          clientRequestId: "no-confirmed-receipt",
        });
        expect(absent.isError).toBe(true);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(1);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === receipt.scheduledTaskId,
        );
        if (task?.webhook?.url == null)
          return yield* Effect.die("Missing isolated webhook address");
        yield* a.stopConnections;
        const body = "{}";
        const delivered = yield* b.scheduled.triggerWebhook({
          hookId: task.id,
          token: new URL(task.webhook.url).pathname.split("/").at(-1)!,
          method: "POST",
          path: "/fixture385",
          query: "",
          headers: {
            "x-signature": `sha256=${NodeCrypto.createHmac("sha256", sentinel).update(body).digest("hex")}`,
          },
          bodyText: body,
          body: new TextEncoder().encode(body),
          relayDeliveryId: "secret385-signed-delivery",
        });
        expect(delivered._tag).toBe("accepted");
        expect(yield* attempts).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

const endRun = (
  node: import("./PeerAuthority.workflow.fixture.ts").Node,
  threadId: import("@t3tools/contracts").ThreadId,
  label: string,
) =>
  Effect.gen(function* () {
    const records = yield* node.threads.getThreadRecords(threadId, ["runs"]);
    const run = records.runs.at(-1);
    if (run === undefined) return yield* Effect.die("Missing fixture run");
    const now = yield* DateTime.now;
    yield* node.sink.write({
      events: [
        {
          id: EventId.make(label),
          type: "run.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
    return { ...run, status: "completed" as const, completedAt: now };
  });

const openRequest = (
  setup: Effect.Success<ReturnType<typeof pair>>,
  key: string,
  oneField = false,
  timeoutMs?: number,
) =>
  Effect.gen(function* () {
    const { a, b, parent } = setup;
    const priorCards = new Set(
      (yield* a.threads.getThreadRecords(parent, ["turnItems"])).turnItems.map((item) => item.id),
    );
    const pending = yield* nativeCall(a, parent, "request_secret", {
      environmentId: b.environmentId,
      projectId: b.project.id,
      label: "Fixture secret",
      reason: "Owner lifetime proof",
      clientRequestId: key,
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
      ...(oneField ? { fields: [{ name: "signature", label: "Signing value" }] } : {}),
    }).pipe(Effect.forkScoped);
    const item = yield* a.orchestrator
      .streamStoredEventsFrom({
        threadId: parent,
        afterSequence: 0,
        eventType: "turn-item.updated",
      })
      .pipe(
        Stream.map((stored) => stored.event),
        Stream.filter(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "secret_request" &&
            event.payload.secretStatus === "pending" &&
            !priorCards.has(event.payload.id),
        ),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
    if (item.type !== "turn-item.updated" || item.payload.type !== "secret_request")
      return yield* Effect.die("Missing source private request");
    const answer = yield* privateClient(a);
    const value = "fixture385-private-owner-1af9";
    const input = {
      threadId: parent,
      turnItemId: item.payload.id,
      answer: oneField
        ? { type: "save" as const, secrets: { signature: value } }
        : { type: "save" as const, secret: value },
    };
    return { pending, item, answer, input, value };
  });
const saveRequest = (
  setup: Effect.Success<ReturnType<typeof pair>>,
  key: string,
  oneField = false,
  afterPublicationFault = false,
) =>
  Effect.gen(function* () {
    const { b } = setup;
    const { pending, item, answer, input, value } = yield* openRequest(setup, key, oneField);
    if (afterPublicationFault) yield* Ref.set(b.remoteSecretFault, "after-save");
    const first = yield* answer(input);
    if (afterPublicationFault) {
      expect(first.exit?._tag).toBe("Failure");
      const bSql = Context.get(b.database, SqlClient.SqlClient);
      expect(
        yield* bSql`SELECT request_id FROM remote_secret_requests WHERE status='saving'`,
      ).toHaveLength(1);
      yield* b.rebuildSecretOwner;
      expect(
        yield* bSql`SELECT request_id FROM remote_secret_requests WHERE status='saved'`,
      ).toHaveLength(1);
      expect((yield* answer(input)).exit?._tag).toBe("Success");
    } else expect(first.exit?._tag).toBe("Success");
    const response = yield* Fiber.join(pending);
    expect(response.isError).not.toBe(true);
    const result = yield* decodeSecretResult(response.structuredContent);
    if (result.status !== "saved" || !("secretRequest" in result))
      return yield* Effect.die("Missing saved handle");
    return { handle: result.secretRequest, value, card: item.payload, answer, input };
  });
const consumerInput = (
  setup: Effect.Success<ReturnType<typeof pair>>,
  handle: import("@t3tools/contracts").RemoteSecretHandle,
  key: string,
) => ({
  environmentId: setup.b.environmentId,
  projectId: setup.b.project.id,
  prompt: "Fixture signed task",
  purposeIntent: { version: 1, intent: { type: "unscoped", reason: "deliberate" } },
  schedule: {
    type: "webhook",
    signature: { header: "x-signature", encoding: "hex", prefix: "sha256=", secretRequest: handle },
  },
  clientRequestId: key,
});

it.live(
  "secret385 published-save interruption reconciles the immutable attempt and one-field mapping",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-save-interrupt");
        const saved = yield* saveRequest(setup, "save-interrupt", true, true);
        const owner = Option.getOrThrow(
          Context.getOption(setup.b.secretOwners, RemoteSecretRequests.RemoteSecretRequests),
        );
        const result = yield* nativeCall(
          setup.a,
          setup.parent,
          "schedule_task",
          consumerInput(setup, saved.handle, "consume-interrupted-save"),
        );
        expect(result.isError).not.toBe(true);
        yield* owner.reconcile;
        const sql = Context.get(setup.b.database, SqlClient.SqlClient);
        expect(
          (yield* sql<{ webhook_secret: string }>`SELECT webhook_secret FROM scheduled_tasks`)[0]
            ?.webhook_secret === saved.value,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 outer SQL rollback preserves the reservation and postcommit cleanup can recover",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-outer-rollback");
        const { a, b, parent } = setup;
        const saved = yield* saveRequest(setup, "outer-rollback");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* sql`CREATE TRIGGER fixture385_abort_receipt BEFORE INSERT ON scheduled_mutation_receipts WHEN NEW.stage='confirmed' BEGIN SELECT RAISE(ABORT,'fixture385 outer receipt interruption'); END`;
        const input = consumerInput(setup, saved.handle, "atomic-consumer");
        const failed = yield* nativeCall(a, parent, "schedule_task", input);
        expect(failed.isError).toBe(true);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(0);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='reserved'`,
        ).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(1);
        const source = Context.get(a.database, SqlClient.SqlClient);
        const [obligation] = yield* source<{
          attempt_id: string;
        }>`SELECT attempt_id FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId}`;
        if (obligation === undefined || setup.peers.remoteSecret === undefined)
          return yield* Effect.die("Missing original save attempt transport");
        const remoteSecret = setup.peers.remoteSecret;
        const replayAnswer = (value: string) =>
          Effect.gen(function* () {
            const body = {
              type: "answer" as const,
              requestId: saved.handle.requestId,
              attemptId: obligation.attempt_id,
              answer: "save" as const,
              value,
            };
            const envelope = yield* a.authority.prepareSecretAnswer(
              saved.answer.sessionId,
              b.environmentId,
              setup.link.launchIdentity,
              body,
            );
            return yield* remoteSecret(b.environmentId, setup.link.launchIdentity, {
              authority: envelope,
              body,
            });
          });
        expect((yield* replayAnswer("fixture385-different-value").pipe(Effect.result))._tag).toBe(
          "Failure",
        );
        expect((yield* replayAnswer(saved.value))?.status).toBe("reserved");
        yield* sql`DROP TRIGGER fixture385_abort_receipt`;
        yield* Ref.set(b.remoteSecretFault, "cleanup");
        const accepted = yield* nativeCall(a, parent, "schedule_task", input);
        expect(accepted.isError).not.toBe(true);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='consumed'`,
        ).toHaveLength(1);
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(1);
        yield* Ref.set(b.remoteSecretFault, undefined);
        yield* b.rebuildSecretOwner;
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
        yield* endRun(a, parent, "secret385-rollback-source-ended");
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replay.isError).not.toBe(true);
        expect(replay.structuredContent).toEqual(accepted.structuredContent);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 source run exit after the returned handle drains exact retirement without another secret call",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-run-retirement");
        const saved = yield* saveRequest(setup, "retire-on-exit");
        const sql = Context.get(setup.b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='saved'`,
        ).toHaveLength(1);
        yield* endRun(setup.a, setup.parent, "secret385-retirement-run-ended");
        if (setup.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing native retirement drain");
        yield* setup.peers.drainAutomationDisables;
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE request_id=${saved.handle.requestId} AND status='retired'`,
        ).toHaveLength(1);
        const source = Context.get(setup.a.database, SqlClient.SqlClient);
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId} AND pending=1 AND ack_json IS NOT NULL`,
        ).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 unlink while unreachable persists retirement across source restart and preserves the new generation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-offline-retirement");
        const { a, b } = setup;
        const old = yield* saveRequest(setup, "old-generation");
        yield* a.links.unlink(b.environmentId);
        const source = Context.get(a.database, SqlClient.SqlClient);
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE pending=1 AND ack_json IS NULL`,
        ).toHaveLength(1);
        const unavailableTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) =>
              request.url.endsWith("/mcp/peer-control/secrets/retire")
                ? Effect.succeed(
                    HttpClientResponse.fromWeb(request, new Response(null, { status: 503 })),
                  )
                : effect,
            ),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        const unavailable = yield* a.reconstructAutomationWith(unavailableTransport);
        if (unavailable.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing reconstructed native retirement owner");
        yield* unavailable.peers.drainAutomationDisables;
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE pending=1 AND ack_json IS NULL`,
        ).toHaveLength(1);
        const destination = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${old.handle.requestId} AND status='saved'`,
        ).toHaveLength(1);
        yield* linkTo(a.links, b, "full-access");
        const nextLink = Option.getOrThrow(yield* a.links.get(b.environmentId));
        expect(nextLink.launchIdentity).not.toBe(setup.link.launchIdentity);
        if (setup.peers.remoteSecret === undefined)
          return yield* Effect.die("Missing negotiated secret owner");
        yield* setup.peers.remoteSecret(b.environmentId, nextLink.launchIdentity);
        const sessions = yield* destination<{
          session_id: string;
        }>`SELECT session_id FROM peer_authority_sessions WHERE generation=${nextLink.launchIdentity}`;
        const newGrant = (yield* b.authority.grants).find(
          (grant) => grant.sessionId === sessions[0]?.session_id,
        );
        if (newGrant === undefined) return yield* Effect.die("Missing isolated successor grant");
        yield* b.authority.setGrant({
          ...newGrant,
          resourceScope: "environment-resources",
          onward: true,
        });
        const current = yield* saveRequest({ ...setup, link: nextLink }, "new-generation");
        const restarted = yield* a.reconstructAutomation;
        if (restarted.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing restarted retirement owner");
        yield* restarted.peers.drainAutomationDisables;
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE request_id=${old.handle.requestId} AND ack_json IS NOT NULL`,
        ).toHaveLength(1);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${old.handle.requestId} AND status='retired'`,
        ).toHaveLength(1);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${current.handle.requestId} AND status='saved'`,
        ).toHaveLength(1);
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE request_id=${current.handle.requestId} AND pending=0 AND ack_json IS NULL`,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 same create key has immutable intent and origin across concurrent retries", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const setup = yield* pair("secret385-create-identity");
      const { a, b, parent, link, peers } = setup;
      if (peers.remoteSecret === undefined)
        return yield* Effect.die("Missing native secret transport");
      const input = {
        type: "create" as const,
        projectId: b.project.id,
        family: "schedule.webhook-signature" as const,
        key: "immutable-create",
        label: "Fixture purpose",
        reason: "Identity proof",
      };
      const prepare = (body: typeof input) =>
        a.authority.prepare(
          caller(a, parent),
          b.environmentId,
          link.launchIdentity,
          "remote_secret",
          body,
          { runtimeMode: "full-access", interactionMode: "default" },
          undefined,
          false,
          true,
        );
      const envelope = yield* prepare(input);
      const results = yield* Effect.all(
        Array.from({ length: 3 }, () =>
          peers.remoteSecret!(b.environmentId, link.launchIdentity, {
            authority: envelope,
            body: input,
          }),
        ),
        { concurrency: "unbounded" },
      );
      expect(new Set(results.map((result) => result?.handle.requestId)).size).toBe(1);
      const sql = Context.get(b.database, SqlClient.SqlClient);
      const before =
        yield* sql`SELECT origin_json,source_record_id,input_digest FROM remote_secret_requests`;
      const changed = { ...input, reason: "Changed intent" };
      const refused = yield* peers
        .remoteSecret(b.environmentId, link.launchIdentity, {
          authority: yield* prepare(changed),
          body: changed,
        })
        .pipe(Effect.result);
      expect(refused._tag).toBe("Failure");
      yield* endRun(a, parent, "secret385-create-old-run-ended");
      yield* a.threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("secret385-next-run"),
        threadId: parent,
        messageId: MessageId.make("secret385-next-run-message"),
        text: "New isolated run",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* activate(a, parent);
      const laterRun = yield* peers
        .remoteSecret(b.environmentId, link.launchIdentity, {
          authority: yield* prepare(input),
          body: input,
        })
        .pipe(Effect.result);
      expect(laterRun._tag).toBe("Failure");
      expect(
        yield* sql`SELECT origin_json,source_record_id,input_digest FROM remote_secret_requests`,
      ).toEqual(before);
      expect(yield* sql`SELECT request_id FROM remote_secret_requests`).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 wrong thread, project, family and claimed human cannot use the saved request",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-bindings");
        const { a, b, parent, link, peers } = setup;
        const saved = yield* saveRequest(setup, "bound-save");
        const wrongThread = ThreadId.make("secret385-unrelated-source");
        yield* seedParent(a, wrongThread);
        const input = consumerInput(setup, saved.handle, "bound-consumer");
        expect((yield* nativeCall(a, wrongThread, "schedule_task", input)).isError).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "schedule_task", {
            ...input,
            projectId: `${b.project.id}-wrong`,
            clientRequestId: "wrong-project",
          })).isError,
        ).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "update_scheduled_task", {
            ...input,
            scheduledTaskId: "unknown-task",
            clientRequestId: "wrong-family",
          })).isError,
        ).toBe(true);
        const body = {
          type: "answer" as const,
          requestId: saved.handle.requestId,
          attemptId: "0".repeat(32),
          answer: "save" as const,
          value: "fixture385-forged-answer",
        };
        const envelope = yield* a.authority.prepare(
          caller(a, parent),
          b.environmentId,
          link.launchIdentity,
          "remote_secret",
          remoteSecretIntent(body),
          { runtimeMode: "full-access", interactionMode: "default" },
          undefined,
          false,
          true,
        );
        if (peers.remoteSecret === undefined)
          return yield* Effect.die("Missing native private transport");
        expect(
          (yield* peers
            .remoteSecret(b.environmentId, link.launchIdentity, { authority: envelope, body })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='saved'`,
        ).toHaveLength(1);
        const valid = yield* nativeCall(a, parent, "schedule_task", input);
        expect(valid.isError).not.toBe(true);
        expect(
          (yield* sql<{ webhook_secret: string }>`SELECT webhook_secret FROM scheduled_tasks`)[0]
            ?.webhook_secret === saved.value,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 rival keys and concurrent duplicate retries accept exactly one consumer identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-concurrent-consumers");
        const saved = yield* saveRequest(setup, "concurrent-save");
        const first = consumerInput(setup, saved.handle, "consumer-one");
        const rival = { ...first, clientRequestId: "consumer-two" };
        const outcomes = yield* Effect.all(
          [
            nativeCall(setup.a, setup.parent, "schedule_task", first),
            nativeCall(setup.a, setup.parent, "schedule_task", first),
            nativeCall(setup.a, setup.parent, "schedule_task", rival),
          ],
          { concurrency: "unbounded" },
        );
        const successful = outcomes.filter((result) => result.isError !== true);
        expect(successful.length).toBeGreaterThan(0);
        expect(
          successful.every(
            (result) => json(result.structuredContent) === json(successful[0]?.structuredContent),
          ),
        ).toBe(true);
        const sql = Context.get(setup.b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(1);
        expect(
          yield* sql`SELECT request_id FROM scheduled_mutation_receipts WHERE stage='confirmed'`,
        ).toHaveLength(1);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='consumed'`,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 narrowed grants and broader fields fail before any source card", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { a, b, parent } = yield* pair("secret385-preprompt");
      const input = {
        environmentId: b.environmentId,
        projectId: b.project.id,
        label: "Fixture",
        reason: "Private fixture",
        clientRequestId: "preprompt",
      };
      expect(
        (yield* nativeCall(a, parent, "request_secret", {
          ...input,
          fields: [
            { name: "first", label: "First" },
            { name: "second", label: "Second" },
          ],
        })).isError,
      ).toBe(true);
      const [grant] = yield* b.authority.grants;
      if (grant === undefined) return yield* Effect.die("Missing native grant");
      yield* b.authority.setGrant({ ...grant, resourceScope: "created-work" });
      expect((yield* nativeCall(a, parent, "request_secret", input)).isError).toBe(true);
      for (const node of [a, b]) {
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT request_id FROM remote_secret_requests`).toHaveLength(0);
        expect(
          yield* sql`SELECT turn_item_id FROM orchestration_v2_projection_turn_items WHERE type='secret_request'`,
        ).toHaveLength(0);
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 an old generic peer request is rejected by the new destination before effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, link, peers } = yield* pair("secret385-old-generic");
        const input = {
          label: "Fixture",
          reason: "Old generic request",
          clientRequestId: "old-generic",
        };
        const limits = { runtimeMode: "full-access" as const, interactionMode: "default" as const };
        const authority = yield* a.authority.prepare(
          caller(a, parent),
          b.environmentId,
          link.launchIdentity,
          "request_secret",
          input,
          limits,
        );
        const response = yield* peers
          .call({
            environmentId: b.environmentId,
            tool: "request_secret",
            arguments: input,
            limits,
            authority,
            success: OrchestratorMcpRequestSecretResult,
          })
          .pipe(Effect.result);
        expect(response._tag).toBe("Failure");
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT request_id FROM remote_secret_requests`).toHaveLength(0);
        expect(
          yield* sql`SELECT turn_item_id FROM orchestration_v2_projection_turn_items WHERE type='secret_request'`,
        ).toHaveLength(0);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter(
            (name) => name.startsWith("secret-request-") || name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["grant", "link-expiry"] as const)(
  "secret385 %s retirement uses the existing recovery owner",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair(`secret385-retire-${mode}`);
        const saved = yield* saveRequest(setup, "retire-source");
        const destination = Context.get(setup.b.database, SqlClient.SqlClient);
        if (mode === "grant") {
          const [grant] = yield* setup.b.authority.grants;
          if (grant === undefined) return yield* Effect.die("Missing native grant");
          yield* setup.b.authority.setGrant({ ...grant, resourceScope: "created-work" });
          yield* Option.getOrThrow(
            Context.getOption(setup.b.secretOwners, RemoteSecretRequests.RemoteSecretRequests),
          ).reconcile;
        } else {
          const source = Context.get(setup.a.database, SqlClient.SqlClient);
          yield* source`UPDATE peer_environment_links SET expires_at='2000-01-01T00:00:00.000Z'`;
          if (setup.peers.drainAutomationDisables === undefined)
            return yield* Effect.die("Missing native drain");
          yield* setup.peers.drainAutomationDisables;
          expect(
            yield* source`SELECT request_id FROM remote_secret_obligations WHERE ack_json IS NOT NULL`,
          ).toHaveLength(1);
        }
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${saved.handle.requestId} AND status='retired'`,
        ).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["grant", "source-exit"] as const)(
  "secret385 a %s racing protected publication cannot publish or consume a stale save",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair(`secret385-save-race-${mode}`);
        const opened = yield* openRequest(setup, "race-save");
        yield* Ref.set(setup.b.remoteSecretFault, "hold-save");
        const answer = yield* opened.answer(opened.input).pipe(Effect.forkScoped);
        yield* Deferred.await(setup.b.secretPublished);
        const destination = Context.get(setup.b.database, SqlClient.SqlClient);
        const [saving] = yield* destination<{
          request_id: string;
        }>`SELECT request_id FROM remote_secret_requests WHERE status='saving'`;
        if (saving === undefined) return yield* Effect.die("Missing durable save attempt");
        const consumer = yield* nativeCall(
          setup.a,
          setup.parent,
          "schedule_task",
          consumerInput(
            setup,
            { environmentId: setup.b.environmentId, requestId: saving.request_id },
            "race-consumer",
          ),
        ).pipe(Effect.forkScoped);
        if (mode === "grant") {
          const [grant] = yield* setup.b.authority.grants;
          if (grant === undefined) return yield* Effect.die("Missing native grant");
          yield* setup.b.authority.setGrant({ ...grant, resourceScope: "created-work" });
        } else yield* endRun(setup.a, setup.parent, "secret385-race-ended");
        yield* Deferred.succeed(setup.b.releaseSecretPublication, undefined);
        expect((yield* Fiber.join(answer)).exit?._tag).toBe("Failure");
        expect((yield* Fiber.join(consumer)).isError).toBe(true);
        if (setup.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing recovery owner");
        yield* setup.peers.drainAutomationDisables;
        yield* Option.getOrThrow(
          Context.getOption(setup.b.secretOwners, RemoteSecretRequests.RemoteSecretRequests),
        ).reconcile;
        expect(yield* destination`SELECT task_id FROM scheduled_tasks`).toHaveLength(0);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE status='retired'`,
        ).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 an older peer fails negotiation before a source card", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* makeNode(
        descriptorOf("secret385-new-source", "A", { peerAuthority: 1 }),
        "disk",
        { remoteSecrets: true, productionKernel: true },
      );
      const b = yield* makeNode(
        descriptorOf("secret385-old-destination", "B", { peerAuthority: 1 }),
        "disk",
        { productionKernel: true },
      );
      yield* linkTo(a.links, b, "full-access");
      const parent = ThreadId.make("secret385-old-peer-parent");
      yield* seedParent(a, parent);
      expect(
        (yield* nativeCall(a, parent, "request_secret", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          label: "Fixture",
          reason: "Unsupported peer",
          clientRequestId: "old-peer",
        })).isError,
      ).toBe(true);
      const source = Context.get(a.database, SqlClient.SqlClient);
      expect(
        yield* source`SELECT turn_item_id FROM orchestration_v2_projection_turn_items WHERE type='secret_request'`,
      ).toHaveLength(0);
      expect(yield* source`SELECT request_id FROM remote_secret_obligations`).toHaveLength(0);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 pending SQL failure and protected temporary orphan reconcile before retry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const setup = yield* pair("secret385-pending-orphan");
      const opened = yield* openRequest(setup, "pending-orphan");
      const sql = Context.get(setup.b.database, SqlClient.SqlClient);
      yield* sql`CREATE TRIGGER fixture385_abort_saving BEFORE UPDATE ON remote_secret_requests WHEN NEW.status='saving' BEGIN SELECT RAISE(ABORT,'isolated saving interruption'); END`;
      expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Failure");
      expect(
        yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='pending' AND attempt_id IS NULL`,
      ).toHaveLength(1);
      const fs = yield* FileSystem.FileSystem;
      expect(
        (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
          name.startsWith("secret-remote-"),
        ),
      ).toHaveLength(0);
      const orphan = `${setup.b.config.secretsDir}/secret-remote-rsr_${"a".repeat(32)}-${"b".repeat(32)}.bin.00000000-0000-0000-0000-000000000000.tmp`;
      yield* fs.writeFileString(orphan, "fixture385-protected-temporary", { mode: 0o600 });
      yield* setup.b.rebuildSecretOwner;
      expect(yield* fs.exists(orphan)).toBe(false);
      yield* sql`DROP TRIGGER fixture385_abort_saving`;
      expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Success");
      const response = yield* Fiber.join(opened.pending);
      expect(response.isError).not.toBe(true);
      const result = yield* decodeSecretResult(response.structuredContent);
      if (result.status !== "saved" || !("secretRequest" in result))
        return yield* Effect.die("Missing recovered handle");
      expect(
        (yield* nativeCall(
          setup.a,
          setup.parent,
          "schedule_task",
          consumerInput(setup, result.secretRequest, "consume-pending-recovery"),
        )).isError,
      ).not.toBe(true);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 a rolled-back reservation without a receipt cannot recover after source exit",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-no-receipt");
        const saved = yield* saveRequest(setup, "no-receipt-save");
        const sql = Context.get(setup.b.database, SqlClient.SqlClient);
        yield* sql`CREATE TRIGGER fixture385_abort_receipt BEFORE INSERT ON scheduled_mutation_receipts BEGIN SELECT RAISE(ABORT,'isolated outer rollback'); END`;
        const input = consumerInput(setup, saved.handle, "no-receipt-consumer");
        expect((yield* nativeCall(setup.a, setup.parent, "schedule_task", input)).isError).toBe(
          true,
        );
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='reserved'`,
        ).toHaveLength(1);
        expect(yield* sql`SELECT request_id FROM scheduled_mutation_receipts`).toHaveLength(0);
        yield* sql`DROP TRIGGER fixture385_abort_receipt`;
        yield* endRun(setup.a, setup.parent, "secret385-no-receipt-ended");
        expect((yield* nativeCall(setup.a, setup.parent, "schedule_task", input)).isError).toBe(
          true,
        );
        if (setup.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing native drain");
        yield* setup.peers.drainAutomationDisables;
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='retired'`,
        ).toHaveLength(1);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(0);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 a failed active retirement hook preserves ownership, delivers the question and retries through recovery",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-hook-retry");
        const { a, b, parent } = setup;
        const saved = yield* saveRequest(setup, "hook-retained-save");
        const fs = yield* FileSystem.FileSystem;
        const protectedFiles = yield* fs.readDirectory(b.config.secretsDir);
        const protectedName = protectedFiles.find((name) => name.startsWith("secret-remote-"));
        if (protectedName === undefined)
          return yield* Effect.die("Missing protected request value");
        const protectedDigest = () =>
          fs
            .readFile(`${b.config.secretsDir}/${protectedName}`)
            .pipe(
              Effect.map((bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex")),
            );
        const originalProtectedDigest = yield* protectedDigest();
        const source = Context.get(a.database, SqlClient.SqlClient);
        const destination = Context.get(b.database, SqlClient.SqlClient);
        const original =
          yield* source`SELECT * FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId}`;
        const parentRun = (yield* a.threads.getThreadRecords(parent, ["runs"])).runs.at(-1);
        if (parentRun?.rootNodeId == null || a.project.defaultModelSelection === null)
          return yield* Effect.die("Missing source run");
        yield* a.orchestrator.dispatch({
          type: "delegated_task.request",
          commandId: CommandId.make("secret385-hook-child"),
          parentThreadId: parent,
          parentRunId: parentRun.id,
          parentNodeId: parentRun.rootNodeId,
          task: "Isolated pending question",
          completionWake: "settled_only",
          modelSelection: a.project.defaultModelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "agent",
          creationSource: "mcp",
        });
        const task = (yield* a.threads.getThreadRecords(parent, ["subagents"])).subagents.at(-1);
        if (task?.childThreadId == null) return yield* Effect.die("Missing real delegated child");
        yield* activate(a, task.childThreadId);
        const childRun = (yield* a.threads.getThreadRecords(task.childThreadId, ["runs"])).runs.at(
          -1,
        );
        if (childRun?.rootNodeId == null) return yield* Effect.die("Missing child run");
        const now = yield* DateTime.now;
        const requestId = RuntimeRequestId.make("secret385-hook-question");
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("secret385-hook-question-event"),
              type: "runtime-request.updated",
              threadId: task.childThreadId,
              runId: childRun.id,
              occurredAt: now,
              payload: {
                id: requestId,
                nodeId: childRun.rootNodeId,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: {
                  type: "live",
                  providerSessionId: ProviderSessionId.make("secret385-hook-session"),
                },
                createdAt: now,
                resolvedAt: null,
              },
            },
          ],
        });
        const services = yield* QuestionReactor.layer.pipe(
          Layer.provide(Layer.succeedContext(Context.mergeAll(a.runtime, a.database, a.services))),
          Layer.build,
        );
        const reactor = Context.get(services, QuestionReactor.DelegatedTaskQuestionReactor);
        const logs: string[] = [];
        const logger = Logger.make(({ message }) => logs.push(String(message)));
        const completed = yield* endRun(a, parent, "secret385-hook-parent-ended");
        yield* source`CREATE TRIGGER fixture385_retirement_failure BEFORE UPDATE ON remote_secret_obligations BEGIN SELECT RAISE(ABORT,'isolated retirement failure'); END`;
        yield* reactor
          .handle({
            id: EventId.make("secret385-hook-parent-handle"),
            type: "run.updated",
            threadId: parent,
            runId: completed.id,
            occurredAt: now,
            payload: completed,
          })
          .pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
        expect(
          logs.some((message) =>
            message.includes("Remote secret retirement reconciliation remains pending"),
          ),
        ).toBe(true);
        expect(logs.join("\n")).not.toContain(saved.value);
        const notices = (yield* a.threads.getThreadRecords(parent, ["messages"])).messages.filter(
          (message) => message.notification !== undefined && message.text.includes(requestId),
        );
        expect(notices).toHaveLength(1);
        expect(
          yield* source`SELECT * FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId}`,
        ).toEqual(original);
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE ack_json IS NOT NULL`,
        ).toHaveLength(0);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${saved.handle.requestId} AND status='saved'`,
        ).toHaveLength(1);
        expect(
          (yield* nativeCall(
            a,
            parent,
            "schedule_task",
            consumerInput(setup, saved.handle, "hook-stale-consumer"),
          )).isError,
        ).toBe(true);
        expect(yield* destination`SELECT task_id FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* protectedDigest()).toBe(originalProtectedDigest);
        const recoveryTime = yield* Clock.currentTimeMillis;
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(recoveryTime);
          const clock = yield* Clock.Clock;
          const retries = yield* Queue.unbounded<void>();
          const observed = {
            ...clock,
            sleep: (duration: Duration.Duration) =>
              (Duration.toMillis(duration) === 2000
                ? Queue.offer(retries, undefined)
                : Effect.void
              ).pipe(Effect.andThen(clock.sleep(duration))),
          };
          const recovered = yield* a.reconstructAutomation.pipe(
            Effect.provideService(Clock.Clock, observed),
          );
          yield* recovered.peers.start.pipe(Effect.provideService(Clock.Clock, observed));
          yield* Queue.take(retries);
          expect(
            yield* source`SELECT * FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId}`,
          ).toEqual(original);
          yield* source`DROP TRIGGER fixture385_retirement_failure`;
          yield* TestClock.adjust("2 seconds");
          yield* Queue.take(retries);
        }).pipe(Effect.provide(TestClock.layer()));
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE request_id=${saved.handle.requestId} AND pending=1 AND ack_json IS NOT NULL`,
        ).toHaveLength(1);
        expect(
          yield* destination`SELECT request_id FROM remote_secret_requests WHERE request_id=${saved.handle.requestId} AND status='retired'`,
        ).toHaveLength(1);
        expect(
          yield* destination`SELECT thread_id FROM orchestration_v2_projection_threads`,
        ).toHaveLength(0);
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 r1 timeout returns the saved winner from the final pending poll", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const setup = yield* pair("secret385-r1-timeout");
      const now = yield* Clock.currentTimeMillis;
      yield* Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const clock = yield* Clock.Clock;
        const polling = yield* Queue.unbounded<void>();
        yield* Ref.set(setup.a.secretRequestClock, {
          ...clock,
          sleep: (duration: Duration.Duration) =>
            (Duration.toMillis(duration) === 500
              ? Queue.offer(polling, undefined)
              : Effect.void
            ).pipe(Effect.andThen(clock.sleep(duration))),
        });
        const opened = yield* openRequest(setup, "r1-timeout", false, 1200);
        yield* Queue.take(polling);
        yield* TestClock.adjust(1000);
        yield* Queue.take(polling);
        yield* Queue.take(polling);
        expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Success");
        yield* TestClock.adjust(200);
        const reply = yield* Fiber.join(opened.pending);
        expect(reply.isError).not.toBe(true);
        const result = yield* decodeSecretResult(reply.structuredContent);
        expect(result.status).toBe("saved");
        if (result.status !== "saved" || !("secretRequest" in result))
          return yield* Effect.die("Missing saved winner");
        const source = Context.get(setup.a.database, SqlClient.SqlClient);
        expect(
          yield* source`SELECT request_id FROM remote_secret_obligations WHERE pending=0 AND ack_json IS NULL`,
        ).toHaveLength(1);
        expect(
          (yield* nativeCall(
            setup.a,
            setup.parent,
            "schedule_task",
            consumerInput(setup, result.secretRequest, "r1-timeout-consume"),
          )).isError,
        ).not.toBe(true);
      }).pipe(Effect.provide(TestClock.layer()));
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 r1 terminal deletion retries automatically on idle B", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const setup = yield* pair("secret385-r1-idle");
      const { a, b, parent } = setup;
      const saved = yield* saveRequest(setup, "r1-idle");
      const now = yield* Clock.currentTimeMillis;
      yield* Effect.gen(function* () {
        yield* TestClock.setTime(now);
        const retryArmed = yield* Deferred.make<void>();
        const clock = yield* Clock.Clock;
        const observed = {
          ...clock,
          sleep: (duration: Duration.Duration) =>
            (Duration.toMillis(duration) === 2000
              ? Deferred.succeed(retryArmed, undefined)
              : Effect.void
            ).pipe(Effect.andThen(clock.sleep(duration))),
        };
        yield* Ref.set(b.secretCleanupClock, observed);
        yield* Ref.set(b.remoteSecretFault, "cleanup");
        expect(
          (yield* nativeCall(
            a,
            parent,
            "schedule_task",
            consumerInput(setup, saved.handle, "r1-idle-consume"),
          )).isError,
        ).not.toBe(true);
        expect((yield* Queue.take(b.secretRemovals)).failed).toBe(true);
        expect((yield* Queue.take(b.secretRemovals)).failed).toBe(true);
        yield* Deferred.await(retryArmed);
        yield* Ref.set(b.remoteSecretFault, undefined);
        yield* TestClock.adjust("2 seconds");
        expect((yield* Queue.take(b.secretRemovals)).failed).toBe(false);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(0);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='consumed'`,
        ).toHaveLength(1);
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(1);
      }).pipe(Effect.provide(TestClock.layer()));
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "secret385 r1 startup isolates a failed terminal copy and keeps the protected owner available",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair("secret385-r1-startup");
        const { a, b, parent } = setup;
        const first = yield* saveRequest(setup, "r1-startup-first");
        const second = yield* saveRequest(setup, "r1-startup-second");
        yield* b.stopSecretOwner;
        yield* Ref.set(b.remoteSecretFault, "cleanup");
        for (const [saved, key] of [
          [first, "first"],
          [second, "second"],
        ] as const)
          expect(
            (yield* nativeCall(
              a,
              parent,
              "schedule_task",
              consumerInput(setup, saved.handle, `r1-startup-${key}-consume`),
            )).isError,
          ).not.toBe(true);
        const fs = yield* FileSystem.FileSystem;
        const retained = (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
          name.startsWith("secret-remote-"),
        );
        expect(retained).toHaveLength(2);
        yield* Ref.set(
          b.secretCleanupName,
          retained
            .find((name) => name.startsWith(`secret-remote-${first.handle.requestId}-`))!
            .replace(/\.bin$/, ""),
        );
        const result = yield* b.rebuildSecretOwner.pipe(Effect.result);
        expect(result._tag).toBe("Success");
        if (result._tag !== "Success")
          return yield* Effect.die("Protected owner construction failed");
        const freshOwner = Context.get(result.success, RemoteRequests.RemoteSecretRequests);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const [terminal] = yield* sql<{
          consumer_json: string;
        }>`SELECT consumer_json FROM remote_secret_requests WHERE request_id=${first.handle.requestId}`;
        if (terminal === undefined) return yield* Effect.die("Missing committed consumer");
        const consumer = yield* decodeSecretConsumer(terminal.consumer_json);
        expect(
          (yield* freshOwner.take(first.handle, consumer.request).pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(
          (yield* fs.readDirectory(b.config.secretsDir)).filter((name) =>
            name.startsWith("secret-remote-"),
          ),
        ).toHaveLength(1);
        expect(
          (yield* nativeCall(
            a,
            parent,
            "schedule_task",
            consumerInput(setup, first.handle, "r1-startup-forbidden-reuse"),
          )).isError,
        ).toBe(true);
        const current = yield* nativeCall(a, parent, "orchestrator_capabilities", {
          environmentId: b.environmentId,
        });
        expect(current.isError).not.toBe(true);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["success", "timeout", "cancel", "startup", "budget"] as const)(
  "secret385 r1 secret-origin frames are transient (%s)",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair(`secret385-r1-frame-${variant}`);
        const saved = yield* saveRequest(setup, `r1-frame-${variant}`);
        const sql = Context.get(setup.b.database, SqlClient.SqlClient);
        const [row] = yield* sql<{
          source_record_id: string;
        }>`SELECT source_record_id FROM remote_secret_requests WHERE request_id=${saved.handle.requestId}`;
        if (row === undefined) return yield* Effect.die("Missing real source record");
        const frames = sql<{
          id: string;
          session_id: string;
          frame_json: string;
        }>`SELECT id,session_id,frame_json FROM peer_control_frames WHERE json_extract(frame_json,'$.body.type')='secret-origin'`;
        if (variant === "success") {
          expect(yield* frames).toHaveLength(0);
          return;
        }
        yield* setup.a.stopConnections;
        const now = yield* Clock.currentTimeMillis;
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(now);
          const clock = yield* Clock.Clock;
          const armed = yield* Queue.unbounded<void>();
          const observed = {
            ...clock,
            sleep: (duration: Duration.Duration) =>
              Queue.offer(armed, undefined).pipe(Effect.andThen(clock.sleep(duration))),
          };
          for (let index = 0; index < (variant === "budget" ? 33 : 1); index++) {
            const waiting = yield* setup.b.authority
              .secretOrigin(row.source_record_id)
              .pipe(Effect.provideService(Clock.Clock, observed), Effect.result, Effect.forkScoped);
            yield* Queue.take(armed);
            const pending = (yield* frames).find(
              (frame) =>
                frame.id.startsWith(`secret-origin:${row.source_record_id}:`) &&
                JSON.parse(frame.frame_json).id === frame.id,
            );
            if (pending === undefined)
              return yield* Effect.die("Missing actual in-flight secret-origin frame");
            if (variant === "timeout") {
              yield* TestClock.adjust("25 seconds");
              const result = yield* Fiber.join(waiting);
              expect(result._tag).toBe("Failure");
              if (result._tag === "Failure")
                expect(result.failure.code).toBe("orchestration_error");
            } else yield* Fiber.interrupt(waiting);
            if (variant === "startup") {
              yield* sql`INSERT INTO peer_control_frames (id,session_id,frame_json) VALUES (${pending.id},${pending.session_id},${pending.frame_json}) ON CONFLICT DO NOTHING`;
              yield* setup.b.reconstructAutomation;
            }
            expect(yield* frames).toHaveLength(0);
          }
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["saved", "reserved"] as const)(
  "secret385 r1 unavailable source validation preserves %s material and the original consumer",
  (state) =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair(`secret385-r1-transient-${state}`);
        const { a, b, parent } = setup;
        const saved = yield* saveRequest(setup, `r1-transient-${state}`);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const input = consumerInput(setup, saved.handle, `r1-transient-${state}-consume`);
        if (state === "reserved") {
          yield* sql`CREATE TRIGGER fixture385_transient_rollback BEFORE INSERT ON scheduled_mutation_receipts WHEN NEW.stage='confirmed' BEGIN SELECT RAISE(ABORT,'isolated outer receipt interruption'); END`;
          expect((yield* nativeCall(a, parent, "schedule_task", input)).isError).toBe(true);
          yield* sql`DROP TRIGGER fixture385_transient_rollback`;
        }
        const original = yield* sql<{
          source_record_id: string;
          attempt_id: string;
          status: string;
          consumer_json: string | null;
        }>`SELECT source_record_id,attempt_id,status,consumer_json FROM remote_secret_requests WHERE request_id=${saved.handle.requestId}`;
        expect(original[0]?.status).toBe(state);
        const row = original[0]!;
        const repeatAnswer = Effect.gen(function* () {
          const body = {
            type: "answer" as const,
            requestId: saved.handle.requestId,
            attemptId: row.attempt_id,
            answer: "save" as const,
            value: saved.value,
          };
          const envelope = yield* a.authority.prepareSecretAnswer(
            saved.answer.sessionId,
            b.environmentId,
            setup.link.launchIdentity,
            body,
          );
          if (setup.peers.remoteSecret === undefined)
            return yield* Effect.die("Missing native secret control");
          return yield* setup.peers.remoteSecret(b.environmentId, setup.link.launchIdentity, {
            authority: envelope,
            body,
          });
        });
        const fs = yield* FileSystem.FileSystem;
        const file = `${b.config.secretsDir}/secret-remote-${saved.handle.requestId}-${row.attempt_id}.bin`;
        const digest = fs
          .readFile(file)
          .pipe(Effect.map((bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex")));
        const before = yield* digest;
        const blocked = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Ref.set(a.secretControlReplyGate, (ids) =>
          ids.some((id) => id.startsWith(`validate:${row.source_record_id}:`))
            ? Deferred.succeed(blocked, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
        );
        const now = yield* Clock.currentTimeMillis;
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(now);
          yield* Ref.set(b.secretValidationClock, yield* Clock.Clock);
          const attempted = yield* repeatAnswer.pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(blocked);
          yield* TestClock.adjust("25 seconds");
          expect((yield* Fiber.join(attempted))._tag).toBe("Failure");
          const error = yield* Queue.take(b.secretValidationErrors);
          expect(error).toEqual({ id: row.source_record_id, code: "orchestration_error" });
          expect(
            yield* sql`SELECT source_record_id,attempt_id,status,consumer_json FROM remote_secret_requests WHERE request_id=${saved.handle.requestId}`,
          ).toEqual(original);
          expect(yield* digest).toBe(before);
        }).pipe(Effect.provide(TestClock.layer()));
        yield* Ref.set(b.secretValidationClock, undefined);
        yield* Ref.set(a.secretControlReplyGate, () => Effect.void);
        yield* Deferred.succeed(release, undefined);
        expect((yield* repeatAnswer)?.status).toBe(state);
        const accepted = yield* nativeCall(a, parent, "schedule_task", input);
        expect(accepted.isError).not.toBe(true);
        expect((yield* nativeCall(a, parent, "schedule_task", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(yield* sql`SELECT task_id FROM scheduled_tasks`).toHaveLength(1);
        expect(
          yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='consumed'`,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["timeout", "aborted"] as const)(
  "secret385 r1 a caller without a returned handle retires its exact request (%s)",
  (reason) =>
    Effect.scoped(
      Effect.gen(function* () {
        const setup = yield* pair(`secret385-r1-close-${reason}`);
        const now = yield* Clock.currentTimeMillis;
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(now);
          const clock = yield* Clock.Clock;
          const polling = yield* Deferred.make<void>();
          const abort = yield* Deferred.make<void>();
          if (reason === "aborted") yield* Ref.set(setup.a.secretRequestAbort, abort);
          yield* Ref.set(setup.a.secretRequestClock, {
            ...clock,
            sleep: (duration: Duration.Duration) =>
              (Duration.toMillis(duration) === 500
                ? Deferred.succeed(polling, undefined)
                : Effect.void
              ).pipe(Effect.andThen(clock.sleep(duration))),
          });
          const opened = yield* openRequest(setup, `r1-close-${reason}`, false, 1200);
          yield* Deferred.await(polling);
          if (reason === "aborted") {
            expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Success");
            yield* Deferred.succeed(abort, undefined);
            yield* Queue.take(setup.a.secretRequestExits);
            yield* Fiber.interrupt(opened.pending);
          } else {
            yield* TestClock.adjust(1200);
            const reply = yield* Fiber.join(opened.pending);
            expect(reply.isError).not.toBe(true);
            expect((yield* decodeSecretResult(reply.structuredContent)).status).toBe("timed_out");
          }
          const source = Context.get(setup.a.database, SqlClient.SqlClient);
          expect(
            yield* source`SELECT request_id FROM remote_secret_obligations WHERE pending=1 AND ack_json IS NULL`,
          ).toHaveLength(1);
          if (setup.peers.drainAutomationDisables === undefined)
            return yield* Effect.die("Missing native retirement drain");
          yield* setup.peers.drainAutomationDisables;
          expect(
            yield* source`SELECT request_id FROM remote_secret_obligations WHERE pending=1 AND ack_json IS NOT NULL`,
          ).toHaveLength(1);
          const destination = Context.get(setup.b.database, SqlClient.SqlClient);
          expect(
            yield* destination`SELECT request_id FROM remote_secret_requests WHERE status='retired'`,
          ).toHaveLength(1);
          const fs = yield* FileSystem.FileSystem;
          expect(
            (yield* fs.readDirectory(setup.b.config.secretsDir)).filter((name) =>
              name.startsWith("secret-remote-"),
            ),
          ).toHaveLength(0);
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("secret385 r1 startup does not hide a protected state transition SQL failure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const setup = yield* pair("secret385-r1-startup-sql");
      const opened = yield* openRequest(setup, "r1-startup-sql");
      yield* setup.b.stopSecretOwner;
      yield* Ref.set(setup.b.remoteSecretFault, "after-save");
      expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Failure");
      const sql = Context.get(setup.b.database, SqlClient.SqlClient);
      expect(
        yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='saving'`,
      ).toHaveLength(1);
      yield* sql`CREATE TRIGGER fixture385_startup_transition BEFORE UPDATE ON remote_secret_requests WHEN NEW.status='saved' BEGIN SELECT RAISE(ABORT,'isolated startup transition failure'); END`;
      expect((yield* setup.b.rebuildSecretOwner.pipe(Effect.result))._tag).toBe("Failure");
      expect(
        yield* sql`SELECT request_id FROM remote_secret_requests WHERE status='saving'`,
      ).toHaveLength(1);
      yield* sql`DROP TRIGGER fixture385_startup_transition`;
      yield* setup.b.rebuildSecretOwner;
      expect((yield* opened.answer(opened.input)).exit?._tag).toBe("Success");
      expect((yield* Fiber.join(opened.pending)).isError).not.toBe(true);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
