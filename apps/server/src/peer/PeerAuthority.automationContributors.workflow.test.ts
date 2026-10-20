import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcGroup, RpcServer, RpcSerialization } from "effect/rpc";
import { WsRpcGroup, WS_METHODS } from "@t3tools/contracts";
import * as Queue from "effect/Queue";
import { McpScheduledMutationReceipt, SecretRef } from "@t3tools/contracts";
import * as ScheduleMutation from "../scheduledTasks/mutationReceipts.ts";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Sink from "effect/Sink";
import * as ScheduledChecks from "../scheduledTaskChecks/ScheduledTaskChecks.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as CheckStore from "../scheduledTaskChecks/store.ts";
import { ThreadToolkit } from "../mcp/toolkits/thread/tools.ts";
import * as FileSystem from "effect/FileSystem";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpBody from "effect/http/HttpBody";
import * as Schema from "effect/Schema";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, OrchestratorMcpFailure, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as PeerForwarding from "./PeerForwarding.ts";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import {
  automationAdapter,
  automationReply,
  automationWorkspace,
  awaitAutomationRunning,
  caller,
  decodeAutomationAck,
  decodeAutomationCertificate,
  decodeAutomationScheduleReceipt,
  decodePendingWsReply,
  decodeScheduledTaskList,
  decodeWorktreeBinding,
  finishAutomationRun,
  instanceId,
  json,
  makeNode,
  nativeCall,
  seedParent,
  selection,
  traceAutomationValidation,
} from "./PeerAuthority.workflow.fixture.ts";

it.live.each([false, true])(
  "automation384-r2: authenticated local save preserves the captured schedule predecessor when concurrent acceptance is %s",
  (concurrent) =>
    Effect.scoped(
      Effect.gen(function* () {
        const armed = yield* Ref.make(false);
        const captured = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const a = yield* makeNode(
          descriptorOf("automation384-r2-origin", "Origin", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            captureReplyGate: Effect.gen(function* () {
              if (!(yield* Ref.getAndSet(armed, false))) return;
              yield* Deferred.succeed(captured, undefined);
              yield* Deferred.await(release);
            }),
          },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-r2-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const c = yield* makeNode(
          descriptorOf("automation384-r2-narrower", "Narrower", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        const parent = ThreadId.make("automation384-r2-parent");
        const other = ThreadId.make("automation384-r2-other");
        yield* seedParent(a, parent);
        yield* seedParent(c, other);
        for (const [source, thread] of [
          [a, parent],
          [c, other],
        ] as const) {
          const routed = yield* nativeCall(source, thread, "t3_project_list", {
            environmentId: b.environmentId,
          });
          expect(routed.isError, json(routed)).not.toBe(true);
        }
        for (const grant of yield* b.authority.grants)
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            preview: grant.environmentId === a.environmentId,
          });
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Immutable accepted contributors",
          title: "Original",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-r2-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === receipt.scheduledTaskId,
        )!;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const automation = yield* AutomationAuthority.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const current = Context.get(automation, AutomationAuthority.DestinationAutomationAuthority)
          .current("schedule", task.id)
          .pipe(Effect.map(Option.getOrThrow));
        const original = yield* current;
        expect(original.ceiling.capabilities).toContain("preview");

        const upsertRpc = WsRpcGroup.requests.get(WS_METHODS.scheduledTasksUpsert);
        if (upsertRpc === undefined || upsertRpc._tag !== WS_METHODS.scheduledTasksUpsert)
          return yield* Effect.die("Missing production scheduled-upsert RPC");
        const group = RpcGroup.make(upsertRpc);
        const server = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
        const route = HttpRouter.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const authenticated = yield* b.auth
              .authenticateWebSocketUpgrade(request)
              .pipe(Effect.result);
            if (authenticated._tag === "Failure") return HttpServerResponse.empty({ status: 403 });
            const session = authenticated.success;
            const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
            yield* RpcServer.make(group, { disableTracing: true, disableFatalDefects: true }).pipe(
              Effect.provideService(RpcServer.Protocol, protocol),
              Effect.provide(
                Layer.merge(
                  group.toLayer({
                    [WS_METHODS.scheduledTasksUpsert]: (input) =>
                      b.scheduled.upsert(input, {
                        bareReadGrant: session.scopes.includes("filesystem:read")
                          ? { type: "all" }
                          : { type: "none" },
                      }),
                  }),
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
        if (address._tag === "UnixPathAddress")
          return yield* Effect.die("Expected isolated test socket");
        const pairing = yield* b.auth.issuePairingCredential({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const token = yield* b.auth.exchangeBootstrapCredentialForAccessToken(
          pairing.credential,
          undefined,
          { deviceType: "desktop", os: "fixture", browser: "fixture", ipAddress: "127.0.0.1" },
        );
        const session = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${token.access_token}` } }),
          ),
        );
        const ticket = yield* b.auth.issueWebSocketTicket(session);
        const messages = yield* Queue.unbounded<string>();
        const socket = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () =>
              new Promise<WebSocket>((resolve, reject) => {
                const socket = new WebSocket(
                  `ws://127.0.0.1:${address.port}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`,
                );
                socket.addEventListener("message", (event) =>
                  Queue.offerUnsafe(messages, String(event.data)),
                );
                socket.addEventListener("open", () => resolve(socket), { once: true });
                socket.addEventListener(
                  "error",
                  () => reject(new Error("Local fixture WebSocket refused")),
                  { once: true },
                );
              }),
            catch: () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Local fixture WebSocket failed",
              }),
          }),
          (socket) => Effect.sync(() => socket.close()),
        );
        yield* Ref.set(armed, true);
        socket.send(
          json({
            _tag: "Request",
            id: "1",
            tag: WS_METHODS.scheduledTasksUpsert,
            payload: {
              ...task,
              title: "Delayed local save",
              purposeIntent: { version: 1, intent: { type: "unscoped" } },
            },
            headers: [],
          }),
        );
        yield* Deferred.await(captured);
        if (concurrent) {
          const updated = yield* nativeCall(c, other, "update_scheduled_task", {
            environmentId: b.environmentId,
            scheduledTaskId: task.id,
            title: "New narrower acceptance",
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            clientRequestId: "automation384-r2-narrow-update",
          });
          expect(updated.isError, json(updated)).not.toBe(true);
          const narrower = yield* current;
          expect(narrower.id).not.toBe(original.id);
          expect(narrower.ceiling.capabilities).not.toContain("preview");
          expect(new Set(narrower.graph.records.map((node) => node.environmentId))).toEqual(
            new Set([a.environmentId, b.environmentId, c.environmentId]),
          );
        }
        const before = yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${task.id}`;
        const predecessor = yield* current;
        const certificates = yield* sql`SELECT * FROM fork_automation_certificates ORDER BY id`;
        const events = yield* sql`SELECT * FROM orchestration_v2_events ORDER BY sequence`;
        const effects = yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        yield* Deferred.succeed(release, undefined);
        const reply = yield* decodePendingWsReply(yield* Queue.take(messages));
        expect(reply.requestId).toBe("1");
        if (concurrent) {
          expect(reply.exit?._tag, json(reply)).toBe("Failure");
          expect(yield* current).toEqual(predecessor);
          expect(yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${task.id}`).toEqual(
            before,
          );
          expect(yield* sql`SELECT * FROM fork_automation_certificates ORDER BY id`).toEqual(
            certificates,
          );
        } else {
          expect(reply.exit?._tag, json(reply)).toBe("Success");
          const accepted = yield* current;
          expect(accepted.id).not.toBe(original.id);
          expect(accepted.ceiling.capabilities).toEqual(original.ceiling.capabilities);
          expect(
            (yield* b.scheduled.list()).tasks.find((value) => value.id === task.id)?.title,
          ).toBe("Delayed local save");
        }
        expect(yield* sql`SELECT * FROM orchestration_v2_events ORDER BY sequence`).toEqual(events);
        expect(yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`).toEqual(
          effects,
        );
        expect(
          yield* sql`SELECT 1 FROM fork_scheduled_task_checks WHERE task_id=${task.id}`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  45_000,
);

it.live(
  "automation384: a destination grant narrowed during creation checking rejects the whole acceptance",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-fence-source", "Fence source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const root = yield* automationWorkspace;
        let narrow: (() => Effect.Effect<void>) | undefined;
        let checks = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            expect(yield* ScheduleMutation.InAcceptance).toBe(false);
            if (narrow === undefined) return yield* Effect.die("Missing fixture grant mutation");
            checks++;
            yield* narrow().pipe(Effect.orDie);
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(384),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
          }),
        );
        const b = yield* makeNode(
          descriptorOf("automation384-fence-destination", "Fence destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            commandSpawner: spawner,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-fence-parent");
        yield* seedParent(a, parent);
        yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId });
        const originalGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...originalGrant, preview: true });
        narrow = () =>
          b.authority
            .setGrant({ ...originalGrant, preview: false })
            .pipe(Effect.asVoid, Effect.orDie);
        const result = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated grant fence",
          title: "Grant fence",
          checkCommand: "isolated-fence-check",
          checkReason: "Fixture outcome",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-fence-create",
        });
        expect(checks).toBe(1);
        expect(result.isError, json(result)).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT 1 FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "automation384: a source disable received before native acceptance tombstones the captured definition",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-tombstone-source", "Tombstone source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const root = yield* automationWorkspace;
        let narrow: (() => Effect.Effect<void>) | undefined;
        let checks = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            expect(yield* ScheduleMutation.InAcceptance).toBe(false);
            if (narrow === undefined) return yield* Effect.die("Missing fixture grant mutation");
            checks++;
            yield* narrow().pipe(Effect.orDie);
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(384),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
          }),
        );
        const b = yield* makeNode(
          descriptorOf("automation384-tombstone-destination", "Tombstone destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            commandSpawner: spawner,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-tombstone-parent");
        yield* seedParent(a, parent);
        narrow = () =>
          Effect.gen(function* () {
            const sql = Context.get(a.database, SqlClient.SqlClient);
            const registered =
              yield* sql`SELECT 1 FROM fork_automation_obligations WHERE originating=1 AND ack_json IS NULL`;
            expect(registered).toHaveLength(1);
            yield* a.links.unlink(b.environmentId);
            const pending = (yield* a.authority.pendingAutomationDisables)[0];
            if (pending === undefined || a.links.resolveAutomationRecovery === undefined)
              return yield* Effect.die(
                "Capture did not retain its exact source disable dependency",
              );
            const original = yield* a.links.resolveAutomationRecovery(
              pending.environmentId,
              pending.generation,
            );
            const http = yield* HttpClient.HttpClient;
            const reply = yield* http.execute(
              HttpClientRequest.post(`${original.url}/mcp/peer-control/automation-disable`).pipe(
                HttpClientRequest.setHeader("authorization", `Bearer ${original.token}`),
                HttpClientRequest.setBody(
                  HttpBody.text(
                    json({ version: 1, dependency: pending.dependency }),
                    "application/json",
                  ),
                ),
              ),
            );
            expect(reply.status).toBe(200);
            expect((yield* decodeAutomationAck(yield* reply.text)).dependency).toEqual(
              pending.dependency,
            );
          }).pipe(Effect.provide(FetchHttpClient.layer), Effect.orDie);
        const result = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated grant fence",
          title: "Grant fence",
          checkCommand: "isolated-fence-check",
          checkReason: "Fixture outcome",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-tombstone-create",
        });
        expect(checks).toBe(1);
        expect(result.isError, json(result)).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        expect(yield* sql`SELECT 1 FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "automation384: accepted checked sends and outcome checks run offline and disable blocks the next check spawn",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-checked-source", "Checked source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "automation384-checked-" });
        const git = (args: ReadonlyArray<string>) =>
          ProcessRunner.ProcessRunner.pipe(
            Effect.flatMap((runner) => runner.run({ command: "git", args: ["-C", root, ...args] })),
            Effect.provide(ProcessRunner.layer),
          );
        expect((yield* git(["init", "--quiet", "--initial-branch=main"])).code).toBe(0);
        yield* fs.writeFileString(`${root}/README.md`, "Isolated checked fixture");
        expect((yield* git(["add", "README.md"])).code).toBe(0);
        expect(
          (yield* git([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "Fixture baseline",
          ])).code,
        ).toBe(0);
        let checkStarts = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            expect(yield* ScheduleMutation.InAcceptance).toBe(false);
            checkStarts++;
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(384),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(checkStarts === 1 ? 1 : 0)),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
          }),
        );
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-checked-destination", "Checked destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            commandSpawner: spawner,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
            providerAdapter: automationAdapter(started, opened, (turn, events) =>
              automationReply(turn, events, "Isolated checked answer"),
            ),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-checked-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated checked work",
          checkCommand: "isolated-outcome-check-384",
          checkReason: "An isolated external adapter reports the outcome",
          title: "Offline checked native work",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-checked-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        expect(
          checkStarts,
          json({ b: (yield* b.scheduled.list()).tasks, a: (yield* a.scheduled.list()).tasks }),
        ).toBe(1);
        yield* a.stopConnections;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const read = CheckStore.readCheckState(receipt.scheduledTaskId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.flatMap((state) =>
            state === null ? Effect.die("Missing native checked state") : Effect.succeed(state),
          ),
        );
        const checks = Option.getOrThrow(b.scheduledChecks);
        const execute = Effect.gen(function* () {
          yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
          const state = yield* read;
          const run = state.runs.at(-1);
          if (run?.threadId === null || run?.threadId === undefined)
            return yield* Effect.die("Missing pinned checked native thread");
          expect(run.automationCertificateId).toBe(state.automationCertificateId);
          expect(run.error, json(run)).toBeNull();
          expect(yield* b.threads.getThreadShell(run.threadId), json(run)).not.toBeNull();
          const prepared = yield* b.threads
            .streamStoredEventsFrom({
              threadId: run.threadId,
              afterSequence: 0,
              eventType: "run.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.status !== "preparing",
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
          expect(
            prepared.event.type === "run.updated" ? prepared.event.payload.status : "missing",
            json(prepared),
          ).not.toBe("failed");
          yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
          const complete = yield* finishAutomationRun(b, run.threadId);
          expect(
            complete.event.type === "run.updated" ? complete.event.payload.status : "missing",
            json(complete),
          ).toBe("completed");
        });
        yield* execute;
        yield* checks.reconcile;
        const passed = yield* b.scheduled.subscribeList().pipe(
          Stream.filter((list) =>
            list.tasks.some(
              (task) =>
                task.id === receipt.scheduledTaskId &&
                task.outcomeCheck?.lastVerdict?.passed === true,
            ),
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
        expect(
          passed.tasks.find((task) => task.id === receipt.scheduledTaskId)?.outcomeCheck
            ?.lastVerdict?.passed,
        ).toBe(true);
        expect(checkStarts).toBe(2);
        yield* execute;
        yield* b.scheduled.setEnabled({ id: receipt.scheduledTaskId, enabled: false });
        yield* checks.reconcile;
        expect(checkStarts).toBe(2);
        expect(started).toHaveLength(2);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation')`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "automation384: local definition edits retain original contributors and cannot replace a revoked owner with ambient authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-edit-source", "Edit source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-edit-destination", "Edit destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-edit-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Original bounded owner",
          prompt: "Isolated modified work",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-edit-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const task = (yield* b.scheduled.list()).tasks.find(
          (value) => value.id === receipt.scheduledTaskId,
        )!;
        const live = yield* b.scheduled.upsert({ ...task, title: "Live authorized local edit" });
        expect(live.task.title).toBe("Live authorized local edit");
        yield* b.auth.revokeSession((yield* b.linkedSessions)[0]!.sessionId);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const before = yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${task.id}`;
        const denied = yield* Effect.result(
          b.scheduled.upsert({ ...live.task, title: "Must not launder the revoked owner" }),
        );
        expect(denied._tag).toBe("Failure");
        expect(yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${task.id}`).toEqual(before);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "automation384: legacy accepted work keeps local autonomy but live onward calls still require its original source",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-legacy-onward-a", "Legacy source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-legacy-onward-b", "Legacy destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter: automationAdapter([], []),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const c = yield* makeNode(
          descriptorOf("automation384-legacy-onward-c", "Onward target", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("automation384-legacy-onward-parent");
        const local = ThreadId.make("automation384-legacy-onward-local");
        yield* seedParent(a, parent);
        yield* seedParent(b, local);
        yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const native = yield* b.registry.issue({ threadId: local, providerInstanceId: instanceId });
        const credential = {
          ...native,
          config: {
            ...native.config,
            endpoint: `${b.url}/mcp`,
            authorizationHeader: (yield* Ref.get(b.bearers)).at(-1)!,
          },
        };
        const created = yield* nativeCall(
          b,
          local,
          "schedule_task",
          {
            projectId: b.project.id,
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            prompt: "Isolated legacy onward",
            title: "Legacy onward",
            schedule: { type: "interval", everyMs: 60_000 },
            bindToCurrentThread: false,
            workspaceStrategy: { type: "root" },
          },
          undefined,
          credential,
        );
        expect(created.isError, json(created)).not.toBe(true);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.title === "Legacy onward",
        );
        if (task === undefined)
          return yield* Effect.die("The legacy HTTP call did not save its schedule");
        yield* b.scheduled.runNow({ id: task.id });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const shell = (yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id} AND thread_id<>${local}`)[0]!;
        const threadId = ThreadId.make(shell.thread_id);
        yield* b.threads
          .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
          .pipe(
            Stream.filter(
              (event) =>
                event.event.type === "run.updated" && event.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        yield* awaitAutomationRunning(b, threadId);
        const online = yield* nativeCall(b, threadId, "t3_project_list", {
          environmentId: c.environmentId,
        });
        expect(online.isError, json(online)).not.toBe(true);
        const certificate = (yield* sql<{
          certificate_json: string;
        }>`SELECT certificate_json FROM fork_automation_certificates WHERE id=(SELECT certificate_id FROM fork_automation_bindings WHERE resource_id=${task.id})`)[0]!;
        expect(
          (yield* decodeAutomationCertificate(certificate.certificate_json)).recordIds,
        ).toHaveLength(0);
        yield* a.stopConnections;
        expect((yield* Effect.result(b.authority.checkThread(threadId)))._tag).toBe("Success");
        const offline = yield* nativeCall(b, threadId, "t3_project_list", {
          environmentId: c.environmentId,
        });
        expect(offline.isError, json(offline)).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  40_000,
);

it.live(
  "automation384: an onward automation captures every legacy ancestor and still validates them for interactive onward work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-legacy-multihop-a", "Legacy source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const root = yield* automationWorkspace;
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-legacy-multihop-b", "Legacy destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(started, opened),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const c = yield* makeNode(
          descriptorOf("automation384-legacy-multihop-c", "Onward target", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter: automationAdapter([], []),
          },
        );
        const d = yield* makeNode(
          descriptorOf("automation384-legacy-multihop-d", "Final target", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(c.links, d, "full-access");
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(b.links, c, "full-access");
        const parent = ThreadId.make("automation384-legacy-multihop-parent");
        const local = ThreadId.make("automation384-legacy-multihop-local");
        yield* seedParent(a, parent);
        yield* seedParent(b, local);
        yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, onward: true });
        const native = yield* b.registry.issue({ threadId: local, providerInstanceId: instanceId });
        const credential = {
          ...native,
          config: {
            ...native.config,
            endpoint: `${b.url}/mcp`,
            authorizationHeader: (yield* Ref.get(b.bearers)).at(-1)!,
          },
        };
        const created = yield* nativeCall(
          b,
          local,
          "schedule_task",
          {
            projectId: b.project.id,
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            prompt: "Isolated legacy onward",
            title: "Legacy onward",
            schedule: { type: "interval", everyMs: 60_000 },
            bindToCurrentThread: false,
          },
          undefined,
          credential,
        );
        expect(created.isError, json(created)).not.toBe(true);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.title === "Legacy onward",
        );
        if (task === undefined)
          return yield* Effect.die("The legacy HTTP call did not save its schedule");
        yield* b.scheduled.runNow({ id: task.id });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const shell = (yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id} AND thread_id<>${local}`)[0]!;
        const threadId = ThreadId.make(shell.thread_id);
        yield* b.threads
          .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
          .pipe(
            Stream.filter(
              (event) =>
                event.event.type === "run.updated" && event.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const online = yield* nativeCall(b, threadId, "t3_project_list", {
          environmentId: c.environmentId,
        });
        expect(online.isError, json(online)).not.toBe(true);
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        yield* b.threads
          .streamStoredEventsFrom({
            threadId,
            afterSequence: 0,
            eventType: "provider-turn.updated",
          })
          .pipe(
            Stream.filter(
              (event) =>
                event.event.type === "provider-turn.updated" &&
                event.event.payload.status === "running",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        const onwardGrant = (yield* c.authority.grants)[0]!;
        yield* c.authority.setGrant({
          ...onwardGrant,
          onward: true,
          resourceScope: "environment-resources",
        });
        const next = yield* nativeCall(b, threadId, "schedule_task", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated multi-hop accepted work",
          title: "Accepted legacy ancestor",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-legacy-multihop-next",
          workspaceStrategy: { type: "root" },
        });
        expect(next.isError, json(next)).not.toBe(true);
        const nextReceipt = yield* decodeAutomationScheduleReceipt(next.structuredContent);
        const destinationSql = Context.get(c.database, SqlClient.SqlClient);
        const current = (yield* destinationSql<{
          certificate_json: string;
        }>`SELECT certificate_json FROM fork_automation_certificates WHERE id=(SELECT certificate_id FROM fork_automation_bindings WHERE resource_id=${nextReceipt.scheduledTaskId})`)[0]!;
        const inherited = yield* decodeAutomationCertificate(current.certificate_json);
        expect(
          inherited.graph.legacy.some(
            (owner) =>
              owner.environmentId === b.environmentId && owner.sessionId === grant.sessionId,
          ),
        ).toBe(true);
        yield* c.scheduled.runNow({ id: nextReceipt.scheduledTaskId });
        const nextThread = ThreadId.make(
          (yield* destinationSql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${c.project.id}`)[0]!
            .thread_id,
        );
        yield* Context.get(c.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        yield* awaitAutomationRunning(c, nextThread);
        const onlineNext = yield* nativeCall(c, nextThread, "t3_project_list", {
          environmentId: d.environmentId,
        });
        expect(onlineNext.isError, json(onlineNext)).not.toBe(true);
        yield* a.stopConnections;
        expect((yield* Effect.result(c.authority.checkThread(nextThread)))._tag).toBe("Success");
        const offline = yield* nativeCall(c, nextThread, "t3_project_list", {
          environmentId: d.environmentId,
        });
        expect(offline.isError, json(offline)).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  40_000,
);

it.live(
  "automation384: a bound legacy birth owner and modern run contributor both survive local offline execution",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-bound-modern", "Modern", { peerAuthority: 1 }),
          "disk",
        );
        const c = yield* makeNode(
          descriptorOf("automation384-bound-legacy", "Legacy", { peerAuthority: 1 }),
          "disk",
        );
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const replies: Effect.Effect<void>[] = [];
        let steers = 0;
        const root = yield* automationWorkspace;
        const b = yield* makeNode(
          descriptorOf("automation384-bound-destination", "Bound", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(
              started,
              opened,
              (turn, events) => {
                const reply = automationReply(turn, events, "Bound native answer");
                if (started.length <= 2) {
                  replies.push(reply);
                  return Effect.void;
                }
                return reply;
              },
              () => {
                steers++;
              },
            ),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const parent = ThreadId.make("automation384-bound-parent");
        const legacyParent = ThreadId.make("automation384-bound-legacy-parent");
        yield* seedParent(a, parent);
        yield* seedParent(c, legacyParent);
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(c.links, b, "full-access");
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        yield* c.forwarding.call(
          caller(c, legacyParent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const modern = (yield* b.authority.grants).find(
          (grant) => grant.environmentId === a.environmentId,
        )!;
        const legacy = (yield* b.authority.grants).find(
          (grant) => grant.environmentId === c.environmentId,
        )!;
        yield* b.authority.setGrant({ ...modern, resourceScope: "environment-resources" });
        const target = ThreadId.make("automation384-bound-target");
        yield* b.threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("automation384-bound-create"),
          threadId: target,
          projectId: b.project.id,
          title: "Imported bound work",
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "agent",
          creationSource: "mcp",
          linkOrigin: { sessionId: legacy.sessionId, label: "Legacy" },
        });
        const sent = yield* nativeCall(a, parent, "t3_thread_send", {
          environmentId: b.environmentId,
          threadId: target,
          message: "Modern contribution",
          clientRequestId: "automation384-bound-send",
        });
        expect(sent.isError, json(sent)).not.toBe(true);
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(started).toHaveLength(1);
        const accepted = yield* b.scheduled.upsert({
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Bound accepted work",
          prompt: "Bound offline continuation",
          enabled: true,
          projectId: b.project.id,
          threadId: target,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          schedule: { type: "interval", everyMs: 60_000 },
        });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const state = (yield* sql<{
          certificate_json: string;
        }>`SELECT certificate_json FROM fork_automation_certificates WHERE id=(SELECT certificate_id FROM fork_automation_bindings WHERE resource_id=${accepted.task.id})`)[0]!;
        const certificate = yield* decodeAutomationCertificate(state.certificate_json);
        expect(certificate.graph.legacy.some((owner) => owner.sessionId === legacy.sessionId)).toBe(
          true,
        );
        expect(
          certificate.graph.records.some(
            (owner) => "envelope" in owner.record && owner.record.sessionId === modern.sessionId,
          ),
        ).toBe(true);
        const initialReply = replies[0];
        if (initialReply === undefined)
          return yield* Effect.die("Missing isolated external provider response");
        yield* initialReply;
        yield* finishAutomationRun(b, target);
        const validationAttempts = yield* traceAutomationValidation(b);
        yield* a.stopConnections;
        yield* c.stopConnections;
        yield* b.scheduled.runNow({ id: accepted.task.id });
        const next = yield* b.threads.getThreadShell(target);
        if (next?.activeRunId === null || next?.activeRunId === undefined)
          return yield* Effect.die("Missing accepted bound run");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(started).toHaveLength(2);
        expect(yield* validationAttempts).toHaveLength(0);
        const liveLegacy = yield* c.reconnect();
        const incoming = yield* Context.get(
          liveLegacy.services,
          PeerForwarding.PeerForwarding,
        ).call(caller(c, legacyParent), OrchestratorToolkit.tools.t3_thread_send, b.environmentId, {
          threadId: target,
          mode: "steer",
          message: "A new unrelated live contributor",
          clientRequestId: "automation384-unrelated-input",
        });
        expect(incoming.threadId).toBe(target);
        yield* c.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("automation384-unrelated-narrow"),
          threadId: legacyParent,
          runtimeMode: "auto",
        });
        expect(
          (yield* Effect.result(
            b.authority.automationExecution(certificate.id, "original-record-still-valid"),
          ))._tag,
        ).toBe("Success");
        expect((yield* b.authority.checkThread(target))?.ceiling.runtimeMode).toBe("auto");
        const narrowedCredential = yield* b.registry.issue({
          threadId: target,
          providerInstanceId: instanceId,
        });
        const token = narrowedCredential.config.authorizationHeader.slice("Bearer ".length);
        expect((yield* b.registry.resolve(token))?.authority?.ceiling.runtimeMode).toBe("auto");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(started).toHaveLength(2);
        expect(steers).toBe(0);
        yield* liveLegacy.stop;
        yield* c.links.unlink(b.environmentId);
        expect((yield* c.authority.pendingAutomationDisables).length).toBe(1);
        const recoveredLegacy = yield* c.reconnect();
        if (recoveredLegacy.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing legacy disable recovery owner");
        yield* recoveredLegacy.peers.drainAutomationDisables;
        expect(yield* c.authority.pendingAutomationDisables).toEqual([]);
        expect(
          (yield* Effect.result(
            b.authority.automationExecution(certificate.id, "disabled-bound-run"),
          ))._tag,
        ).toBe("Failure");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(started).toHaveLength(2);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation','capture-legacy-automation')`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "automation384: importing an unbound legacy schedule requires its live creator on every accepted edit",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* makeNode(
          descriptorOf("automation384-legacy-source", "Legacy source", { peerAuthority: 1 }),
          "disk",
        );
        const b = yield* makeNode(
          descriptorOf("automation384-legacy-destination", "Legacy destination", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(source.links, b, "full-access");
        const parent = ThreadId.make("automation384-legacy-parent");
        yield* seedParent(source, parent);
        yield* source.forwarding.call(
          caller(source, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const grant = (yield* b.authority.grants)[0]!;
        const created = yield* b.scheduled.upsert({
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          projectId: b.project.id,
          title: "Historical imported schedule",
          enabled: true,
          prompt: "Isolated legacy work",
          schedule: { type: "interval", everyMs: 60_000 },
          threadId: null,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        // Reproduce the historical row format, which has no acceptance certificate or native thread.
        yield* sql`UPDATE scheduled_tasks SET schedule_json=${json(created.task.schedule)},link_origin_json=${json({ sessionId: grant.sessionId, label: "Legacy source" })} WHERE task_id=${created.task.id}`;
        yield* sql`DELETE FROM fork_automation_bindings WHERE kind='schedule' AND resource_id=${created.task.id}`;
        const imported = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === created.task.id,
        )!;
        const live = yield* b.scheduled.upsert({ ...imported, title: "Explicit live import" });
        const sourceSql = Context.get(source.database, SqlClient.SqlClient);
        expect(
          yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE json_extract(dependency_json,'$.resourceId')=${created.task.id}`,
        ).not.toHaveLength(0);
        yield* b.auth.revokeSession(grant.sessionId);
        const before = yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${created.task.id}`;
        expect(
          (yield* Effect.result(
            b.scheduled.upsert({ ...live.task, title: "Revoked import must fail" }),
          ))._tag,
        ).toBe("Failure");
        expect(yield* sql`SELECT * FROM scheduled_tasks WHERE task_id=${created.task.id}`).toEqual(
          before,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "scheduler-replay382: replayed creation preserves later independent schedule edits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("scheduler-replay382", "Scheduler", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("scheduler-replay382-parent");
        yield* seedParent(node, parent);
        const input = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          prompt: "Original isolated schedule intent",
          title: "Original title",
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "scheduler-replay382-create",
        };
        const first = yield* nativeCall(node, parent, "schedule_task", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ scheduledTaskId: Schema.String }),
        )(first.structuredContent);
        const later = yield* nativeCall(node, parent, "update_scheduled_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          scheduledTaskId: result.scheduledTaskId,
          title: "Later independent title",
          clientRequestId: "scheduler-replay382-later-edit",
        });
        expect(later.isError, json(later)).not.toBe(true);
        const replay = yield* nativeCall(node, parent, "schedule_task", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(
          (yield* node.scheduled.list()).tasks.find((task) => task.id === result.scheduledTaskId)
            ?.title,
        ).toBe("Later independent title");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "scheduler-replay382: native acceptance survives edits, reconstruction and schedule incarnation replacement",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("scheduler-history382", "Scheduler history", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("scheduler-history382-parent");
        yield* seedParent(node, parent);
        const firstPrivate = "ISOLATED_SCHEDULE_PROMPT_382_ALPHA_NEVER_IN_RECEIPTS";
        const changedPrivate = "ISOLATED_SCHEDULE_PROMPT_382_BETA_CHANGED_INPUT";
        const create = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          prompt: firstPrivate,
          title: "Original",
          schedule: { type: "interval", everyMs: 60_000 },
          checkCommand: "test -f isolated-original",
          checkReason: "Original isolated proof",
          bindToCurrentThread: false,
          clientRequestId: "history382-create",
        };
        const initialRivals = yield* Effect.all(
          [
            nativeCall(node, parent, "schedule_task", create),
            nativeCall(node, parent, "schedule_task", create),
          ],
          { concurrency: "unbounded" },
        );
        const first = initialRivals[0]!;
        expect(initialRivals[1]?.structuredContent).toEqual(first.structuredContent);
        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(first.structuredContent);
        expect(Object.keys(receipt).sort()).toEqual([
          "applied",
          "operationId",
          "requestId",
          "scheduledTaskId",
          "stage",
          "type",
        ]);
        const id = receipt.scheduledTaskId;
        const edit1 = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          scheduledTaskId: id,
          title: "First edit",
          prompt: "Independent prompt",
          checkCommand: "test -f isolated-first",
          checkReason: "First edit",
          clientRequestId: "history382-edit1",
        };
        const edit2 = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          scheduledTaskId: id,
          title: "Second edit",
          checkCommand: "test -f isolated-second",
          checkReason: "Second edit",
          clientRequestId: "history382-edit2",
        };
        const firstEdit = yield* nativeCall(node, parent, "update_scheduled_task", edit1);
        expect(firstEdit.isError, json(firstEdit)).not.toBe(true);
        expect((yield* nativeCall(node, parent, "update_scheduled_task", edit2)).isError).not.toBe(
          true,
        );
        expect(
          (yield* nativeCall(node, parent, "schedule_task", create)).structuredContent,
        ).toEqual(receipt);
        expect((yield* nativeCall(node, parent, "update_scheduled_task", edit1)).isError).not.toBe(
          true,
        );
        let current = (yield* node.scheduled.list()).tasks.find((task) => task.id === id)!;
        expect(current.title).toBe("Second edit");
        expect(current.prompt).toBe("Independent prompt");
        expect(current.outcomeCheck?.command).toBe("test -f isolated-second");
        const changed = yield* nativeCall(node, parent, "schedule_task", {
          ...create,
          prompt: changedPrivate,
        });
        expect(changed.isError).toBe(true);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const before = yield* sql`SELECT * FROM scheduled_mutation_receipts ORDER BY request_id`;
        expect(before).toHaveLength(3);
        for (const rows of [before, yield* sql`SELECT * FROM mcp_request_receipts`]) {
          expect(json(rows)).not.toContain(firstPrivate);
          expect(json(rows)).not.toContain(changedPrivate);
          expect(json(rows)).not.toContain("isolated-second");
        }
        const reopened = yield* node.reopenDatabase;
        const original = (yield* sql<{
          request_id: string;
          generation: string;
          input_digest: string;
        }>`SELECT * FROM scheduled_mutation_receipts WHERE request_id=${receipt.operationId}`)[0]!;
        const recovered = yield* ScheduleMutation.confirmed({
          id: original.request_id,
          generation: original.generation,
          inputDigest: original.input_digest,
          clientRequestId: create.clientRequestId,
        }).pipe(Effect.provide(Layer.succeedContext(reopened)));
        expect(Option.getOrThrow(recovered)).toEqual(receipt);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const transportRecovered = yield* nativeCall(active, parent, "schedule_task", create);
        expect(transportRecovered.isError, json(transportRecovered)).not.toBe(true);
        expect(transportRecovered.structuredContent).toEqual(receipt);
        expect((yield* active.scheduled.list()).tasks.find((task) => task.id === id)?.title).toBe(
          "Second edit",
        );
        const concurrent = yield* Effect.all(
          [
            nativeCall(active, parent, "schedule_task", create),
            nativeCall(active, parent, "schedule_task", create),
          ],
          { concurrency: "unbounded" },
        );
        expect(concurrent.map((result) => result.structuredContent)).toEqual([receipt, receipt]);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts ORDER BY request_id`).toEqual(
          before,
        );
        const rivalEdits = yield* Effect.all(
          [
            nativeCall(active, parent, "update_scheduled_task", {
              purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
              scheduledTaskId: id,
              title: "Rival A",
              clientRequestId: "history382-rival-a",
            }),
            nativeCall(active, parent, "update_scheduled_task", {
              purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
              scheduledTaskId: id,
              prompt: "ISOLATED_RIVAL_PRIVATE_PROMPT_382",
              clientRequestId: "history382-rival-b",
            }),
          ],
          { concurrency: "unbounded" },
        );
        expect(rivalEdits.every((result) => result.isError !== true)).toBe(true);
        const rivalTask = (yield* active.scheduled.list()).tasks.find((task) => task.id === id);
        expect(rivalTask?.title).toBe("Rival A");
        expect(rivalTask?.prompt).toBe("ISOLATED_RIVAL_PRIVATE_PROMPT_382");
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(5);
        const failure = yield* nativeCall(active, parent, "update_scheduled_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          scheduledTaskId: id,
          schedule: { type: "once", at: "2000-01-01T00:00:00.000Z" },
          clientRequestId: "history382-before-commit",
        });
        expect(failure.isError).toBe(true);
        expect(yield* sql`SELECT * FROM scheduled_mutation_receipts`).toHaveLength(5);
        const remove = { scheduledTaskId: id, clientRequestId: "history382-delete" };
        const deleted = yield* nativeCall(active, parent, "delete_scheduled_task", remove);
        expect(deleted.isError, json(deleted)).not.toBe(true);
        expect(
          (yield* nativeCall(active, parent, "schedule_task", create)).structuredContent,
        ).toEqual(receipt);
        expect(
          (yield* nativeCall(active, parent, "update_scheduled_task", edit1)).structuredContent,
        ).toEqual(firstEdit.structuredContent);
        expect(
          (yield* nativeCall(active, parent, "delete_scheduled_task", remove)).structuredContent,
        ).toEqual(deleted.structuredContent);
        // Deliberate unkeyed local replacement keeps the visible id but gets a new native incarnation.
        yield* active.scheduled.upsert({
          ...current,
          title: "Replacement incarnation",
          prompt: "Replacement private prompt",
          requireExisting: false,
        });
        const replacement = (yield* active.scheduled.list()).tasks.find((task) => task.id === id)!;
        for (const [tool, input] of [
          ["schedule_task", create],
          ["update_scheduled_task", edit1],
          ["delete_scheduled_task", remove],
        ] as const) {
          const historical = yield* nativeCall(active, parent, tool, input);
          expect(historical.isError, json(historical)).not.toBe(true);
        }
        current = (yield* active.scheduled.list()).tasks.find((task) => task.id === id)!;
        expect(current).toEqual(replacement);
        const nativeRows = yield* sql<{
          incarnation: string;
        }>`SELECT incarnation FROM scheduled_task_request_incarnations WHERE task_id=${id}`;
        const acceptedRows = yield* sql<{
          incarnation: string;
        }>`SELECT incarnation FROM scheduled_mutation_receipts WHERE request_id=${receipt.operationId}`;
        expect(nativeRows[0]?.incarnation).not.toBe(acceptedRows[0]?.incarnation);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "scheduler-secret382: committed native replay consumes once, while consume-to-commit uncertainty never consumes again",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("scheduler-secret-a382", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("scheduler-secret-b382", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("scheduler-secret382-parent");
        yield* seedParent(a, parent);
        const store = Context.get(b.services, Secrets.ServerSecretStore);
        const seed = (hex: string) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((savedAt) =>
              store.set(
                `secret-request-${hex}`,
                new TextEncoder().encode(
                  json({ projectId: b.project.id, value: `ISOLATED_SECRET_382_${hex}`, savedAt }),
                ),
              ),
            ),
          );
        const firstHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        yield* seed(firstHex);
        const input = {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Webhook",
          prompt: "ISOLATED_WEBHOOK_PROMPT_382",
          schedule: {
            type: "webhook",
            signature: {
              header: "x-fixture-signature",
              encoding: "hex",
              prefix: "sha256=",
              secretRef: SecretRef.make(`secret-ref:${firstHex}`),
            },
          },
          clientRequestId: "secret382-committed",
        };
        yield* Ref.set(a.dropCommittedToolReply, "schedule_task");
        const lost = yield* nativeCall(a, parent, "schedule_task", input);
        expect(lost.isError, json(lost)).toBe(true);
        expect(yield* Ref.get(b.secretConsumes)).toBe(1);
        expect(Option.isNone(yield* store.get(`secret-request-${firstHex}`))).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM scheduled_mutation_receipts WHERE stage='confirmed'`;
        expect(rows).toHaveLength(1);
        const receipt = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(McpScheduledMutationReceipt),
        )(rows[0]!.result_json);
        const beforeRoute = Option.getOrThrow(yield* a.links.get(b.environmentId));
        const resumed = { ...b, ...(yield* b.restartSchedulerTransport) };
        // Only isolated transport location changes. Existing session, grant and
        // generation stay untouched; the real resolver probes the new server.
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([resumed.url])} WHERE environment_id=${b.environmentId}`;
        expect(Option.getOrThrow(yield* a.links.get(b.environmentId)).launchIdentity).toBe(
          beforeRoute.launchIdentity,
        );
        const replay = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(receipt);
        expect(yield* Ref.get(b.secretConsumes)).toBe(1);
        const listed = yield* nativeCall(a, parent, "list_scheduled_tasks", {
          environmentId: b.environmentId,
          projectId: b.project.id,
        });
        const projection = yield* decodeScheduledTaskList(listed.structuredContent);
        expect(projection.tasks[0]?.schedule.type).toBe("webhook");
        expect(
          decodeURIComponent(new URL(projection.tasks[0]!.webhookUrl!).pathname.split("/").at(-2)!),
        ).toBe(receipt.scheduledTaskId);
        const secondHex = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        yield* seed(secondHex);
        yield* Ref.set(b.loseSecretConfirmation, true);
        const uncertain = {
          ...input,
          clientRequestId: "secret382-uncertain",
          schedule: {
            ...input.schedule,
            signature: {
              ...input.schedule.signature,
              secretRef: SecretRef.make(`secret-ref:${secondHex}`),
            },
          },
        };
        const failed = yield* nativeCall(a, parent, "schedule_task", uncertain);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
        expect(Option.isNone(yield* store.get(`secret-request-${secondHex}`))).toBe(true);
        const reconstructed = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([reconstructed.url])} WHERE environment_id=${b.environmentId}`;
        const native = reconstructed.scheduled;
        expect((yield* native.list()).tasks).toHaveLength(1);
        const again = yield* nativeCall(a, parent, "schedule_task", uncertain);
        expect(json(again)).toContain("outcome_unknown");
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
        for (const records of [
          yield* sql`SELECT * FROM scheduled_mutation_receipts`,
          yield* sql`SELECT * FROM mcp_request_receipts`,
        ]) {
          expect(json(records)).not.toContain("ISOLATED_SECRET_382_");
          expect(json(records)).not.toContain(input.prompt);
          expect(json(records)).not.toContain("https://hooks.");
        }
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replaced = yield* nativeCall(a, parent, "schedule_task", input);
        expect(replaced.isError).toBe(true);
        expect(yield* Ref.get(b.secretConsumes)).toBe(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "links382: registered SQL and native command link receipts cannot undo later independent links",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("links382", "Links", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, nativeOwners: true },
        );
        const parent = ThreadId.make("links382-parent");
        yield* seedParent(node, parent);
        const caps = new Set<
          McpThreadInvocationScope["capabilities"] extends ReadonlySet<infer A> ? A : never
        >(["orchestration", "pull-requests"]);
        const call = (tool: string, args: Record<string, unknown>) =>
          nativeCall(node, parent, tool, args, caps);
        for (const [link, unlink, list, url] of [
          [
            "link_issue",
            "unlink_issue",
            "list_thread_issues",
            "https://github.com/fixture/repo/issues/382",
          ],
          [
            "link_pull_request",
            "unlink_pull_request",
            "list_thread_pull_requests",
            "https://github.com/fixture/repo/pull/382",
          ],
        ]) {
          const initial = { url, clientRequestId: `${link}-original` };
          const first = yield* call(link!, initial);
          expect(first.isError, json(first)).not.toBe(true);
          const removed = yield* call(unlink!, { url, clientRequestId: `${unlink}-first` });
          expect(removed.isError, json(removed)).not.toBe(true);
          expect((yield* call(link!, initial)).structuredContent).toEqual(first.structuredContent);
          const absent = yield* call(list!, {});
          expect(json(absent.structuredContent)).not.toContain('"number":382');
          const later = yield* call(link!, { url, clientRequestId: `${link}-later` });
          expect(later.isError, json(later)).not.toBe(true);
          expect(
            (yield* call(unlink!, { url, clientRequestId: `${unlink}-first` })).structuredContent,
          ).toEqual(removed.structuredContent);
          const present = yield* call(list!, {});
          expect(json(present.structuredContent)).toContain('"number":382');
          const noOpLink = { url, clientRequestId: `${link}-original-no-op` };
          const noOp = yield* call(link!, noOpLink);
          expect(noOp.isError, json(noOp)).not.toBe(true);
          expect(
            (yield* call(unlink!, { url, clientRequestId: `${unlink}-independent` })).isError,
          ).not.toBe(true);
          expect((yield* call(link!, noOpLink)).structuredContent).toEqual(noOp.structuredContent);
          expect(json((yield* call(list!, {})).structuredContent)).not.toContain('"number":382');
          const noOpUnlink = { url, clientRequestId: `${unlink}-original-no-op` };
          const noOpRemoved = yield* call(unlink!, noOpUnlink);
          expect(noOpRemoved.isError, json(noOpRemoved)).not.toBe(true);
          expect(
            (yield* call(link!, { url, clientRequestId: `${link}-after-no-op` })).isError,
          ).not.toBe(true);
          expect((yield* call(unlink!, noOpUnlink)).structuredContent).toEqual(
            noOpRemoved.structuredContent,
          );
          expect(json((yield* call(list!, {})).structuredContent)).toContain('"number":382');
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "worktree382: native worktree acceptance preserves files and bounds unresolved follow-up effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "worktree382-native-" });
        const git = (args: ReadonlyArray<string>) =>
          ProcessRunner.ProcessRunner.pipe(
            Effect.flatMap((runner) => runner.run({ command: "git", args: ["-C", root, ...args] })),
            Effect.provide(ProcessRunner.layer),
          );
        expect((yield* git(["init", "--quiet", "--initial-branch=main"])).code).toBe(0);
        yield* fs.writeFileString(`${root}/README.md`, "Isolated repository");
        expect((yield* git(["add", "README.md"])).code).toBe(0);
        expect(
          (yield* git([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "Fixture baseline",
          ])).code,
        ).toBe(0);
        const node = yield* makeNode(
          descriptorOf("worktree382", "Worktree", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            nativeOwners: true,
            ownerWorkspace: root,
          },
        );
        const parent = ThreadId.make("worktree382-parent");
        yield* seedParent(node, parent);
        const capabilities = new Set(["orchestration", "worktree"] as const);
        const input = {
          branch: "worktree382-accepted",
          baseRef: "main",
          startFromOrigin: false,
          runSetupScript: false,
          clientRequestId: "worktree382-once",
        };
        const first = yield* nativeCall(node, parent, "t3_worktree_handoff", input, capabilities);
        expect(first.isError, json(first)).not.toBe(true);
        const accepted = yield* decodeWorktreeBinding(first.structuredContent);
        expect((yield* node.threads.getThreadShell(parent))?.worktreePath).toBe(
          accepted.worktreePath,
        );
        yield* fs.writeFileString(`${accepted.worktreePath}/README.md`, "Later independent edit");
        const replay = yield* nativeCall(node, parent, "t3_worktree_handoff", input, capabilities);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(yield* fs.readFileString(`${accepted.worktreePath}/README.md`)).toBe(
          "Later independent edit",
        );
        const next = ThreadId.make("worktree382-ambiguous-parent");
        yield* seedParent(node, next);
        yield* Ref.set(node.loseWorktreeReply, true);
        const ambiguousInput = {
          ...input,
          branch: "worktree382-ambiguous",
          clientRequestId: "worktree382-lost-native-reply",
        };
        const lost = yield* nativeCall(
          node,
          next,
          "t3_worktree_handoff",
          ambiguousInput,
          capabilities,
        );
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("outcome_unknown");
        const ambiguousPath = (yield* node.threads.getThreadShell(next))?.worktreePath;
        if (ambiguousPath == null)
          throw new Error("Expected the native worktree binding after reply loss");
        expect(ambiguousPath).not.toBe(accepted.worktreePath);
        expect(yield* fs.exists(`${ambiguousPath}/README.md`)).toBe(true);
        const again = yield* nativeCall(
          node,
          next,
          "t3_worktree_handoff",
          ambiguousInput,
          capabilities,
        );
        expect(again.isError).toBe(true);
        expect(json(again)).toContain("native_binding_accepted");
        expect((yield* node.threads.getThreadShell(next))?.worktreePath).toBe(ambiguousPath);
        expect(yield* fs.exists(`${ambiguousPath}/README.md`)).toBe(true);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type='thread.metadata.update' AND command_id LIKE 'worktree-handoff:%'`,
        ).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "manual382: a durable command admission survives reconstruction without a second external start",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        let starts = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            starts++;
            yield* Deferred.succeed(started, undefined);
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(382),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              kill: () => Effect.void,
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
              unref: Effect.succeed(Effect.void),
            });
          }),
        );
        const node = yield* makeNode(
          descriptorOf("manual382-command", "Command", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, commandSpawner: spawner },
        );
        const parent = ThreadId.make("manual382-command-parent");
        yield* seedParent(node, parent);
        const scheduled = yield* nativeCall(node, parent, "schedule_task", {
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          prompt: "Isolated external command",
          command: "isolated-external-boundary-382",
          title: "One command",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "manual382-command-schedule",
        });
        expect(scheduled.isError, json(scheduled)).not.toBe(true);
        const task = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ scheduledTaskId: Schema.String }),
        )(scheduled.structuredContent);
        const input = { taskId: task.scheduledTaskId, clientRequestId: "manual382-command-fire" };
        const first = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          ThreadToolkit.tools.run_scheduled_task_now.successSchema,
        )(first.structuredContent);
        expect(result.manualReceipt?.stage).toBe("admitted");
        yield* Deferred.await(started);
        expect(starts).toBe(1);
        expect(
          (yield* nativeCall(node, parent, "run_scheduled_task_now", input)).structuredContent,
        ).toEqual(result);
        expect(starts).toBe(1);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const row = (yield* sql<{
          request_id: string;
          generation: string;
          input_digest: string;
        }>`SELECT request_id,generation,input_digest FROM scheduled_manual_requests`)[0]!;
        const rebuilt = yield* node.rebuildScheduler;
        const owner = Context.get(rebuilt, ScheduledTasks.ScheduledTaskService);
        const identity = {
          id: row.request_id,
          generation: row.generation,
          inputDigest: row.input_digest,
          clientRequestId: input.clientRequestId,
        };
        const replay = yield* owner.runNow({ id: result.taskId }, { request: identity });
        expect(replay.manualReceipt).toEqual(result.manualReceipt);
        expect(starts).toBe(1);
        // Drive the existing command owner after a process restart. Its unresolved
        // external start remains needs-you, never silently executed again.
        yield* Option.getOrThrow(Context.getOption(rebuilt, ScheduledChecks.ScheduledTaskChecks))
          .reconcile;
        expect(starts).toBe(1);
        const current = (yield* owner.list()).tasks.find((t) => t.id === result.taskId)!;
        expect(current.command?.run).toMatchObject({ stage: "needs-you", imported: false });
        expect(current.command?.run?.error).toBeTruthy();
        expect(current.runCount).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);
