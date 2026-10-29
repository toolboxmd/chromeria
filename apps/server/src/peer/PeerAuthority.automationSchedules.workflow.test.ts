import * as ServerSettings from "../serverSettings.ts";
import * as DateTime from "effect/DateTime";
import * as Ref from "effect/Ref";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerLinkEvents from "./PeerLinkEvents.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as PubSub from "effect/PubSub";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { SecretRef } from "@t3tools/contracts";
import * as ScheduleMutation from "../scheduledTasks/mutationReceipts.ts";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Sink from "effect/Sink";
import * as ProcessRunner from "../processRunner.ts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as CheckStore from "../scheduledTaskChecks/store.ts";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";
import * as FileSystem from "effect/FileSystem";
import * as Clock from "effect/Clock";
import * as TestClock from "effect/testing/TestClock";
import * as Deferred from "effect/Deferred";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as ProviderRuntimeRecovery from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpBody from "effect/http/HttpBody";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, EnvironmentId, MessageId, RunId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import {
  activate,
  automationAdapter,
  automationReply,
  automationWorkspace,
  awaitAutomationRunning,
  decodeAutomationAck,
  decodeAutomationCertificate,
  decodeAutomationScheduleReceipt,
  decodeLaunchReceipt,
  decodeFixtureDependency,
  decodeFixtureDisable,
  delegate,
  drainAutomationRun,
  finishAutomationRun,
  instanceId,
  json,
  makeNode,
  nativeCall,
  nativeInvalidParams,
  seedParent,
  selection,
  traceAutomationValidation,
} from "./PeerAuthority.workflow.fixture.ts";

it.live.each(["client", "mcp", "unkeyed-mcp"] as const)(
  "paused certificate schedule refuses %s manual runs with resume guidance",
  (entry) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("paused586-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("paused586-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("paused586-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Must not run while paused",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "paused586-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const id = receipt.scheduledTaskId;
        yield* b.scheduled.setEnabled({ id, enabled: false });
        const paused = (yield* b.scheduled.list()).tasks.find((task) => task.id === id);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const localParent = ThreadId.make("paused586-local-parent");
        if (entry === "unkeyed-mcp") yield* seedParent(b, localParent);
        const effectsBefore = yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`;
        expect(
          yield* sql`SELECT 1 FROM fork_automation_certificates WHERE disabled=1`,
        ).toHaveLength(1);
        if (entry === "client") {
          const error = yield* b.scheduled.runNow({ id }).pipe(Effect.flip);
          expect(error.message).toMatch(/paused.*resume/i);
        } else {
          const refused = yield* nativeCall(
            entry === "mcp" ? a : b,
            entry === "mcp" ? parent : localParent,
            "run_scheduled_task_now",
            {
              ...(entry === "mcp" ? { environmentId: b.environmentId } : {}),
              taskId: id,
              ...(entry === "mcp" ? { clientRequestId: "paused586-run" } : {}),
            },
          );
          expect(refused.isError, json(refused)).toBe(true);
          expect(json(refused)).toMatch(/paused.*resume/i);
        }
        expect((yield* b.scheduled.list()).tasks.find((task) => task.id === id)).toEqual(paused);
        expect(yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`).toEqual(effectsBefore);
        expect(yield* sql`SELECT 1 FROM scheduled_manual_requests`).toHaveLength(0);
        expect(
          yield* sql`SELECT 1 FROM fork_automation_certificates WHERE disabled=1`,
        ).toHaveLength(1);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384: an old peer keeps ordinary reads but refuses unsupported automation before checking or native effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-old-source", "Old source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, legacyAutomationPeer: true },
        );
        let checks = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.sync(() => {
            checks++;
          }).pipe(
            Effect.andThen(Effect.die("An unsupported automation reached command execution")),
          ),
        );
        const b = yield* makeNode(
          descriptorOf("automation384-old-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, commandSpawner: spawner },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-old-parent");
        yield* seedParent(a, parent);
        const read = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(read.isError, json(read)).not.toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          (yield* sql<{
            automation_version: number;
          }>`SELECT automation_version FROM peer_authority_sessions`)[0]?.automation_version,
        ).toBe(0);
        const refused = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated unsupported automation",
          title: "Unsupported",
          bindToCurrentThread: false,
          checkCommand: "isolated-never-execute",
          checkReason: "Unsupported protocol must refuse first",
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-old-create",
        });
        expect(refused.isError, json(refused)).toBe(true);
        expect(checks).toBe(0);
        expect(yield* sql`SELECT 1 FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`).toHaveLength(0);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384: an accepted destination webhook retains native authority with its source disconnected",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-source", "Automation source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "automation384-native-" });
        const git = (args: ReadonlyArray<string>) =>
          ProcessRunner.ProcessRunner.pipe(
            Effect.flatMap((runner) => runner.run({ command: "git", args: ["-C", root, ...args] })),
            Effect.provide(ProcessRunner.layer),
          );
        expect((yield* git(["init", "--quiet", "--initial-branch=main"])).code).toBe(0);
        yield* fs.writeFileString(`${root}/README.md`, "Isolated automation fixture");
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
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-destination", "Automation destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            providerAdapter: automationAdapter(started, opened),
            ownerWorkspace: root,
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-parent");
        yield* seedParent(a, parent);
        const signingRef = "cccccccccccccccccccccccccccccccc";
        yield* Context.get(b.services, Secrets.ServerSecretStore).set(
          `secret-request-${signingRef}`,
          new TextEncoder().encode(
            json({
              projectId: b.project.id,
              value: "ISOLATED_SIGNING_KEY_384",
              savedAt: yield* Clock.currentTimeMillis,
            }),
          ),
        );
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Automation 384 native authority",
          prompt: "Isolated accepted destination work",
          bindToCurrentThread: false,
          schedule: {
            type: "webhook",
            signature: {
              header: "x-isolated-signature",
              encoding: "hex",
              prefix: "sha256=",
              secretRef: SecretRef.make(`secret-ref:${signingRef}`),
            },
          },
          clientRequestId: "automation384-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === receipt.scheduledTaskId,
        )!;
        expect(task.webhook?.url).toMatch(/^https:\/\/hooks\.example\.invalid\//);
        const unchangedGrant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant(unchangedGrant);
        const admissionSql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          (yield* admissionSql<{
            disabled: number;
          }>`SELECT disabled FROM fork_automation_certificates WHERE id=(SELECT certificate_id FROM fork_automation_bindings WHERE resource_id=${task.id})`)[0]
            ?.disabled,
        ).toBe(0);
        const validationAttempts = yield* traceAutomationValidation(b);
        yield* a.stopConnections;
        const request = {
          hookId: task.id,
          token: new URL(task.webhook!.url!).pathname.split("/").at(-1)!,
          method: "POST",
          path: "/isolated-384",
          query: "",
          headers: {
            "x-isolated-signature":
              "sha256=4ba717bd166307a688cf02dec392a90be3ce186be07234ff65b1004d64084a30",
          },
          bodyText: "{}",
          body: new TextEncoder().encode("{}"),
          relayDeliveryId: "automation384-delivery",
        };
        const delivered = yield* b.scheduled.triggerWebhook(request);
        const duplicate = yield* b.scheduled.triggerWebhook(request);
        expect(duplicate._tag).toBe("accepted");
        expect(duplicate._tag === "accepted" ? duplicate.outcome : "rejected").toBe("duplicate");
        expect(delivered._tag).toBe("accepted");
        const finished = yield* b.scheduled.subscribeList().pipe(
          Stream.filter((list) =>
            list.tasks.some((entry) => entry.id === task.id && entry.runCount === 1),
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
        expect(finished.tasks.find((entry) => entry.id === task.id)?.lastRunStatus).toBe(
          "succeeded",
        );
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const shells = yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`;
        expect(shells).toHaveLength(1);
        const owner = yield* b.authority.checkThread(ThreadId.make(shells[0]!.thread_id));
        expect(
          owner,
          "The actual native run must retain accepted authority rather than ambient destination access",
        ).toBeDefined();
        expect(owner?.ceiling.capabilities).not.toContain("preview");
        const credential = yield* b.registry.issue({
          threadId: ThreadId.make(shells[0]!.thread_id),
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview"]),
        });
        expect(credential.config.capabilities?.has("preview")).toBe(false);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE json_extract(frame_json,'$.body.type') IN ('validate','capture-automation') AND answer_json IS NULL`,
        ).toHaveLength(0);
        const prepared = yield* b.threads
          .streamStoredEventsFrom({
            threadId: ThreadId.make(shells[0]!.thread_id),
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          prepared.event.type === "run.updated" ? prepared.event.payload.status : "missing",
          json(prepared),
        ).not.toBe("failed");
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        yield* b.threads.dispatch({
          type: "queue.resume",
          threadId: ThreadId.make(shells[0]!.thread_id),
          commandId: CommandId.make("automation384-promote"),
        });
        yield* worker.drain();
        expect(
          started,
          json(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`),
        ).toHaveLength(1);
        expect(opened).toHaveLength(1);
        expect(yield* validationAttempts).toHaveLength(0);
        const second = yield* b.scheduled.triggerWebhook({
          hookId: task.id,
          token: new URL(task.webhook!.url!).pathname.split("/").at(-1)!,
          method: "POST",
          path: "/isolated-384",
          query: "",
          headers: {
            "x-isolated-signature":
              "sha256=4ba717bd166307a688cf02dec392a90be3ce186be07234ff65b1004d64084a30",
          },
          bodyText: "{}",
          body: new TextEncoder().encode("{}"),
          relayDeliveryId: "automation384-delivery-second",
        });
        expect(second._tag).toBe("accepted");
        yield* b.scheduled.subscribeList().pipe(
          Stream.filter((list) =>
            list.tasks.some((entry) => entry.id === task.id && entry.runCount === 2),
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
        const secondShell = (yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id} AND thread_id<>${shells[0]!.thread_id}`)[0]!;
        const nextPrepared = yield* b.threads
          .streamStoredEventsFrom({
            threadId: ThreadId.make(secondShell.thread_id),
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          nextPrepared.event.type === "run.updated" ? nextPrepared.event.payload.status : "missing",
        ).not.toBe("failed");
        expect(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id=${secondShell.thread_id} AND effect_type='provider-turn.start' AND status='pending'`,
        ).toHaveLength(1);
        const reusableCredential = yield* b.registry.issue({
          threadId: ThreadId.make(shells[0]!.thread_id),
          providerInstanceId: instanceId,
        });
        expect(
          yield* b.registry.resolve(
            reusableCredential.config.authorizationHeader.slice("Bearer ".length),
          ),
        ).toBeDefined();
        yield* b.scheduled.setEnabled({ id: task.id, enabled: false });
        const deniedCredential = yield* Effect.exit(
          b.registry.issue({
            threadId: ThreadId.make(secondShell.thread_id),
            providerInstanceId: instanceId,
          }),
        );
        expect(Exit.isFailure(deniedCredential)).toBe(true);
        yield* b.registry.touch(ThreadId.make(shells[0]!.thread_id));
        expect(
          Exit.isFailure(
            yield* Effect.exit(
              b.registry.resolve(
                reusableCredential.config.authorizationHeader.slice("Bearer ".length),
              ),
            ),
          ),
        ).toBe(true);
        expect(
          Exit.isFailure(
            yield* Effect.exit(b.registry.scopeForThread(ThreadId.make(shells[0]!.thread_id))),
          ),
        ).toBe(true);
        yield* worker.drain();
        expect(started).toHaveLength(1);
        expect(opened).toHaveLength(1);
        expect(
          (yield* Effect.result(b.authority.checkThread(ThreadId.make(secondShell.thread_id))))
            ._tag,
        ).toBe("Failure");
        const sourceSql = Context.get(a.database, SqlClient.SqlClient);
        expect(
          yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE originating=1 AND ack_json IS NULL`,
        ).toHaveLength(1);
        const originalSession = (yield* b.linkedSessions)[0]!;
        yield* a.links.unlink(b.environmentId);
        expect((yield* Effect.result(a.links.resolve(b.environmentId)))._tag).toBe("Failure");
        expect((yield* a.authority.pendingAutomationDisables).length).toBe(1);
        yield* b.auth.revokeSession(originalSession.sessionId);
        const pending = (yield* a.authority.pendingAutomationDisables)[0]!;
        if (a.links.resolveAutomationRecovery === undefined)
          return yield* Effect.die("Missing private recovery resolver");
        const original = yield* a.links.resolveAutomationRecovery(
          pending.environmentId,
          pending.generation,
        );
        const unavailableTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) =>
              request.url.endsWith("/mcp/peer-control/automation-disable")
                ? Effect.succeed(
                    HttpClientResponse.fromWeb(request, new Response(null, { status: 503 })),
                  )
                : effect,
            ),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        const unavailable = yield* a.reconstructAutomationWith(unavailableTransport);
        if (unavailable.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing reconstructed disable recovery owner");
        yield* unavailable.peers.drainAutomationDisables;
        expect((yield* unavailable.authority.pendingAutomationDisables).length).toBe(1);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(0);
        const http = yield* HttpClient.HttpClient;
        const recoveryPost = (dependency: AutomationAuthority.AutomationDependency) =>
          http.execute(
            HttpClientRequest.post(`${original.url}/mcp/peer-control/automation-disable`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${original.token}`),
              HttpClientRequest.setBody(
                HttpBody.text(json({ version: 1, dependency }), "application/json"),
              ),
            ),
          );
        const wrong = yield* recoveryPost({
          ...pending.dependency,
          definitionDigest: "wrong-definition",
        });
        expect(wrong.status).toBe(403);
        const ordinary = yield* http.execute(
          HttpClientRequest.post(`${original.url}/mcp`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${original.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({ jsonrpc: "2.0", id: "retired-384", method: "tools/list", params: {} }),
                "application/json",
              ),
            ),
          ),
        );
        expect(ordinary.status).toBe(401);
        const lostAcknowledgementTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) =>
              request.url.endsWith("/mcp/peer-control/automation-disable")
                ? effect.pipe(
                    Effect.map(() =>
                      HttpClientResponse.fromWeb(request, new Response(null, { status: 503 })),
                    ),
                  )
                : effect,
            ),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        const lostAcknowledgement = yield* a.reconstructAutomationWith(
          lostAcknowledgementTransport,
        );
        if (lostAcknowledgement.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing lost-acknowledgement recovery owner");
        yield* lostAcknowledgement.peers.drainAutomationDisables;
        expect((yield* lostAcknowledgement.authority.pendingAutomationDisables).length).toBe(1);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        const applied = yield* recoveryPost(pending.dependency);
        expect(applied.status).toBe(200);
        const ack = yield* decodeAutomationAck(yield* applied.text);
        expect(ack.dependency).toEqual(pending.dependency);
        const repeated = yield* recoveryPost(pending.dependency);
        expect(repeated.status).toBe(200);
        expect(yield* decodeAutomationAck(yield* repeated.text)).toEqual(ack);
        expect((yield* a.authority.pendingAutomationDisables).length).toBe(1);
        yield* linkTo(a.links, b, "full-access");
        const successor = yield* a.links.get(b.environmentId);
        expect(Option.isSome(successor) ? successor.value.launchIdentity : undefined).not.toBe(
          pending.generation,
        );
        expect(
          (yield* Effect.result(b.scheduled.setEnabled({ id: task.id, enabled: true })))._tag,
        ).toBe("Failure");
        yield* b.stopConnections;
        const recovered = yield* a.reconstructAutomation;
        if (recovered.links.resolveAutomationRecovery === undefined)
          return yield* Effect.die("Missing reconstructed recovery resolver");
        const originalAfterRestart = yield* recovered.links.resolveAutomationRecovery(
          pending.environmentId,
          pending.generation,
        );
        expect(originalAfterRestart.token).toBe(original.token);
        expect(originalAfterRestart.link.launchIdentity).toBe(pending.generation);
        if (recovered.peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing native disable recovery owner");
        yield* recovered.peers.drainAutomationDisables;
        expect(yield* a.authority.pendingAutomationDisables).toEqual([]);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        expect(
          (yield* Effect.result(b.authority.checkThread(ThreadId.make(shells[0]!.thread_id))))._tag,
        ).toBe("Failure");
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each(["disable", "edit", "delete"] as const)(
  "automation384-r1: an ordinary local schedule keeps user follow-ups after %s",
  (change) =>
    Effect.scoped(
      Effect.gen(function* () {
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-r1-local", "Local only", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter: automationAdapter(started, opened, (turn, events) =>
              automationReply(turn, events, "Local done"),
            ),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const definition = {
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Ordinary local",
          prompt: "Local scheduled work",
          enabled: true,
          projectId: b.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          schedule: { type: "interval", everyMs: 60_000 },
          createdBy: "user",
          creationSource: "web",
        } as const;
        const saved = yield* b.scheduled.upsert(definition);
        if (change === "disable")
          yield* b.scheduled.setEnabled({ id: saved.task.id, enabled: false });
        const manual = yield* b.scheduled.runNow({ id: saved.task.id });
        expect(manual.task.enabled).toBe(change !== "disable");
        expect(manual.task.runCount).toBe(1);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const threadId = ThreadId.make(
          (yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!
            .thread_id,
        );
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const run = (created: { readonly id: RunId; readonly status: string }) =>
          Effect.gen(function* () {
            if (created.status === "preparing")
              yield* b.threads
                .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
                .pipe(
                  Stream.filter(
                    (event) =>
                      event.event.type === "run.updated" &&
                      event.event.payload.id === created.id &&
                      event.event.payload.status !== "preparing",
                  ),
                  Stream.runHead,
                  Effect.flatMap(Effect.fromOption),
                );
            yield* worker.drain();
            yield* finishAutomationRun(b, threadId, created.id);
          });
        const first = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs[0]!;
        yield* run(first);
        const follow = (label: string) =>
          Effect.gen(function* () {
            const result = yield* b.threads.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`local-r1-${label}`),
              threadId,
              messageId: MessageId.make(`local-r1-message-${label}`),
              text: "Ordinary local user follow-up",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            const created = result.storedEvents.find((event) => event.event.type === "run.created");
            if (created?.event.type !== "run.created")
              return yield* Effect.die("Missing local follow-up run");
            yield* run(created.event.payload);
          });
        yield* follow("control");
        expect(started).toHaveLength(2);
        if (change === "disable")
          yield* b.scheduled.setEnabled({ id: saved.task.id, enabled: false });
        else if (change === "delete") yield* b.scheduled.delete({ id: saved.task.id });
        else yield* b.scheduled.upsert({ ...definition, id: saved.task.id, title: "Local edited" });
        yield* follow("after");
        expect(started).toHaveLength(3);
        expect(yield* b.authority.checkThread(threadId)).toBeUndefined();
        expect(
          yield* sql`SELECT 1 FROM peer_thread_authority WHERE thread_id=${threadId}`,
        ).toHaveLength(0);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each(["disable", "edit", "delete"] as const)(
  "automation384-r2-schedule: peer-born thread keeps original live ownership after %s",
  (change) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-r2-schedule-source", "Schedule source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-r1-local", "Local only", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter: automationAdapter(started, opened, (turn, events) =>
              automationReply(turn, events, "Local done"),
            ),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        const definition = {
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Ordinary local",
          prompt: "Local scheduled work",
          enabled: true,
          projectId: b.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          schedule: { type: "interval", everyMs: 60_000 },
          createdBy: "user",
          creationSource: "web",
        } as const;
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r2-schedule-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: definition.title,
          prompt: definition.prompt,
          schedule: definition.schedule,
          bindToCurrentThread: false,
          workspaceStrategy: { type: "root" },
          clientRequestId: "automation384-r2-schedule-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === receipt.scheduledTaskId,
        )!;
        const saved = { task };
        yield* b.scheduled.runNow({ id: saved.task.id });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const threadId = ThreadId.make(
          (yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!
            .thread_id,
        );
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const run = (created: { readonly id: RunId; readonly status: string }) =>
          Effect.gen(function* () {
            if (created.status === "preparing")
              yield* b.threads
                .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
                .pipe(
                  Stream.filter(
                    (event) =>
                      event.event.type === "run.updated" &&
                      event.event.payload.id === created.id &&
                      event.event.payload.status !== "preparing",
                  ),
                  Stream.runHead,
                  Effect.flatMap(Effect.fromOption),
                );
            yield* worker.drain();
            yield* finishAutomationRun(b, threadId, created.id);
          });
        const first = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs[0]!;
        yield* run(first);
        const follow = (label: string) =>
          Effect.gen(function* () {
            const result = yield* b.threads.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`local-r1-${label}`),
              threadId,
              messageId: MessageId.make(`local-r1-message-${label}`),
              text: "Ordinary local user follow-up",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            const created = result.storedEvents.find((event) => event.event.type === "run.created");
            if (created?.event.type !== "run.created")
              return yield* Effect.die("Missing local follow-up run");
            yield* run(created.event.payload);
          });
        const births = yield* sql`SELECT * FROM peer_thread_authority WHERE thread_id=${threadId}`;
        expect(births).toHaveLength(1);
        yield* follow("control");
        expect(started).toHaveLength(2);
        if (change === "disable")
          yield* b.scheduled.setEnabled({ id: saved.task.id, enabled: false });
        else if (change === "delete") yield* b.scheduled.delete({ id: saved.task.id });
        else yield* b.scheduled.upsert({ ...definition, id: saved.task.id, title: "Local edited" });
        yield* follow("after");
        expect(started).toHaveLength(3);
        expect(yield* b.authority.checkThread(threadId)).toBeDefined();
        expect(yield* sql`SELECT * FROM peer_thread_authority WHERE thread_id=${threadId}`).toEqual(
          births,
        );
        yield* b.auth.revokeSession((yield* b.linkedSessions)[0]!.sessionId);
        expect((yield* Effect.result(b.authority.checkThread(threadId)))._tag).toBe("Failure");
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each([
  "certificate",
  "audience",
  "resource",
  "generation",
  "digest",
  "local-record",
] as const)(
  "automation384-r1: authenticated upstream capture refuses forged %s before registering its disable right",
  (forgery) =>
    Effect.scoped(
      Effect.gen(function* () {
        const target = descriptorOf("automation384-r1-forged-destination", "Forged destination", {
          peerAuthority: 1,
        });
        let tampered = 0;
        const a = yield* makeNode(
          descriptorOf("automation384-r1-forged-source", "Forged source", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            captureReplyTransform: (graph) => {
              tampered++;
              if (forgery === "local-record")
                return {
                  ...graph,
                  records: graph.records.map((record) => ({
                    ...record,
                    environmentId: target.environmentId,
                  })),
                };
              return {
                ...graph,
                dependencies: graph.dependencies.map((dependency) => ({
                  ...dependency,
                  ...(forgery === "certificate" ? { certificateId: "forged-r1-certificate" } : {}),
                  ...(forgery === "audience"
                    ? { destinationEnvironmentId: EnvironmentId.make("forged-r1-audience") }
                    : {}),
                  ...(forgery === "resource" ? { resourceId: "forged-r1-resource" } : {}),
                  ...(forgery === "generation"
                    ? { definitionGeneration: "forged-r1-generation" }
                    : {}),
                  ...(forgery === "digest" ? { definitionDigest: "forged-r1-digest" } : {}),
                })),
              };
            },
          },
        );
        const b = yield* makeNode(target, "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r1-forged-parent");
        yield* seedParent(a, parent);
        const result = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Forged capture must not admit work",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-r1-forged-create",
        });
        expect(tampered).toBeGreaterThan(0);
        expect(result.isError, json(result)).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT 1 FROM fork_automation_recovery WHERE dependency_json LIKE '%forged-r1-%'`,
        ).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM scheduled_tasks`).toHaveLength(0);
        expect(yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox`).toHaveLength(0);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each(["schedule_task", "start_spectrum"] as const)(
  "automation384-r1: current negotiated %s reaches its native destination and refuses a narrower link",
  (tool) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-r1-routing-source", "Routing source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-r1-routing-destination", "Routing destination", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r1-routing-parent");
        yield* seedParent(a, parent);
        const { childId } = yield* delegate(a, b, parent, "automation384-r1-routing-child");
        yield* activate(b, childId);
        const args =
          tool === "schedule_task"
            ? {
                environmentId: b.environmentId,
                projectId: b.project.id,
                purposeIntent: { version: 1, intent: { type: "unscoped" } },
                prompt: "Native routed schedule",
                bindToCurrentThread: false,
                schedule: { type: "interval", everyMs: 60_000 },
              }
            : {
                environmentId: b.environmentId,
                callerThreadId: childId,
                question: "Native routed Spectrum",
                mode: "council",
                colors: [
                  { label: "Blue", selection },
                  { label: "Red", selection },
                ],
              };
        const accepted = yield* nativeCall(a, parent, tool, {
          ...args,
          clientRequestId: "automation384-r1-routing-accepted",
        });
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(1);
        expect(
          yield* Context.get(
            a.database,
            SqlClient.SqlClient,
          )`SELECT 1 FROM fork_automation_certificates`,
        ).toHaveLength(0);
        const effects = yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`;
        yield* linkTo(a.links, b, "auto");
        const refused = yield* nativeCall(a, parent, tool, {
          ...args,
          clientRequestId: "automation384-r1-routing-refused",
        });
        expect(refused.isError, json(refused)).toBe(true);
        expect(yield* sql`SELECT 1 FROM fork_automation_certificates`).toHaveLength(1);
        expect(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`).toEqual(effects);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384-r1: exact valid multi-hop dependencies remain bound to the actual destination definition",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-r1-hop-a", "Hop A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-r1-hop-b", "Hop B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const c = yield* makeNode(
          descriptorOf("automation384-r1-hop-c", "Hop C", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r1-hop-parent");
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        yield* b.authority.setGrant({
          ...(yield* b.authority.grants)[0]!,
          onward: true,
          resourceScope: "environment-resources",
        });
        const { childId } = yield* delegate(a, b, parent, "automation384-r1-hop-child");
        yield* activate(b, childId);
        yield* linkTo(b.links, c, "full-access");
        expect(
          (yield* nativeCall(b, childId, "t3_project_list", { environmentId: c.environmentId }))
            .isError,
        ).not.toBe(true);
        yield* c.authority.setGrant({
          ...(yield* c.authority.grants)[0]!,
          resourceScope: "environment-resources",
        });
        const created = yield* nativeCall(b, childId, "schedule_task", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Exact multi-hop admission",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-r1-hop-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const sql = Context.get(c.database, SqlClient.SqlClient);
        const certificate = yield* decodeAutomationCertificate(
          (yield* sql<{
            certificate_json: string;
          }>`SELECT certificate_json FROM fork_automation_certificates`)[0]!.certificate_json,
        );
        expect(certificate.graph.dependencies.length).toBeGreaterThanOrEqual(2);
        expect(
          new Set(
            certificate.graph.dependencies.map((dependency) => dependency.sourceEnvironmentId),
          ),
        ).toEqual(new Set([a.environmentId, b.environmentId]));
        for (const dependency of certificate.graph.dependencies) {
          expect(dependency).toMatchObject({
            certificateId: certificate.id,
            destinationEnvironmentId: c.environmentId,
            resourceId: certificate.binding.resourceId,
            definitionGeneration: certificate.binding.definitionGeneration,
            definitionDigest: certificate.binding.definitionDigest,
          });
          expect(
            yield* sql`SELECT 1 FROM fork_automation_recovery WHERE dependency_id=${RequestReceipts.requestDigest(dependency)}`,
          ).toHaveLength(1);
        }
        yield* a.stopConnections;
        const local = yield* c.authority.automationExecution(certificate.id, "offline-valid-hop");
        expect("certificateId" in local ? local.certificateId : undefined).toBe(certificate.id);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384-r1: the automatic driver reaches a healthy thirty-third disable while its first batch stays unavailable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-r1-fair-source", "Fair source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-r1-fair-destination", "Fair destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r1-fair-parent");
        yield* seedParent(a, parent);
        for (let index = 0; index < 33; index++) {
          const created = yield* nativeCall(a, parent, "schedule_task", {
            environmentId: b.environmentId,
            projectId: b.project.id,
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            prompt: "Live admitted fair recovery fixture",
            bindToCurrentThread: false,
            schedule: { type: "interval", everyMs: 60_000 },
            clientRequestId: `automation384-r1-fair-${index}`,
          });
          expect(created.isError, json(created)).not.toBe(true);
        }
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const dependencies = (yield* sql<{
          dependency_json: string;
        }>`SELECT dependency_json FROM fork_automation_obligations ORDER BY dependency_id`).map(
          (row) => decodeFixtureDependency(row.dependency_json),
        );
        expect(dependencies).toHaveLength(33);
        const healthy = dependencies.at(-1)!;
        yield* a.links.unlink(b.environmentId);
        const firstBatch = yield* Deferred.make<void>();
        const delivered = yield* Deferred.make<void>();
        let unavailable = 0;
        const transport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) => {
              if (
                !request.url.endsWith("/mcp/peer-control/automation-disable") ||
                request.body._tag !== "Uint8Array"
              )
                return effect;
              const disable = decodeFixtureDisable(new TextDecoder().decode(request.body.body));
              if (disable.dependency.certificateId === healthy.certificateId)
                return TestClock.withLive(effect).pipe(
                  Effect.tap(() => Deferred.succeed(delivered, undefined)),
                );
              unavailable++;
              return (
                unavailable >= 32 ? Deferred.succeed(firstBatch, undefined) : Effect.void
              ).pipe(
                Effect.as(HttpClientResponse.fromWeb(request, new Response(null, { status: 503 }))),
              );
            }),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        yield* Effect.gen(function* () {
          const recovered = yield* a.reconstructAutomationWith(transport);
          const changes = yield* recovered.linkEvents.subscribeAutomationChanges;
          yield* recovered.peers.start;
          yield* Deferred.await(firstBatch);
          expect(
            yield* sql`SELECT 1 FROM fork_automation_obligations WHERE ack_json IS NULL`,
          ).toHaveLength(33);
          yield* TestClock.adjust("2 seconds");
          yield* Deferred.await(delivered);
          while (
            (yield* sql`SELECT 1 FROM fork_automation_obligations WHERE dependency_id=${RequestReceipts.requestDigest(healthy)} AND ack_json IS NULL`)
              .length !== 0
          )
            yield* PubSub.take(changes);
          expect(
            yield* sql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`,
          ).toHaveLength(32);
          expect(unavailable).toBeGreaterThanOrEqual(32);
          expect(
            (yield* Effect.result(
              b.authority.automationExecution(healthy.certificateId, "disabled-healthy-r1"),
            ))._tag,
          ).toBe("Failure");
          const destination = Context.get(b.database, SqlClient.SqlClient);
          expect(
            yield* destination`SELECT 1 FROM fork_automation_disable_acks WHERE certificate_id=${healthy.certificateId}`,
          ).toHaveLength(1);
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384-r1: an ordinary local checked run keeps its outcome check after an edit and permits a later user follow-up",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let checkStarts = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.sync(() => {
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
          descriptorOf("automation384-r1-local-checked", "Local checked", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            commandSpawner: spawner,
            ownerWorkspace: yield* automationWorkspace,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
            providerAdapter: automationAdapter(started, opened, (turn, events) =>
              automationReply(turn, events, "Local checked result"),
            ),
          },
        );
        const parent = ThreadId.make("automation384-r1-local-checked-parent");
        yield* seedParent(b, parent);
        const created = yield* nativeCall(b, parent, "schedule_task", {
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Ordinary local checked work",
          checkCommand: "synthetic-local-r1-check",
          checkReason: "Synthetic external outcome adapter",
          title: "Local checked",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-r1-local-checked-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        expect(checkStarts).toBe(1);
        yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const read = CheckStore.readCheckState(receipt.scheduledTaskId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.map(Option.fromNullishOr),
          Effect.flatMap(Effect.fromOption),
        );
        const checked = (yield* read).runs.at(-1)!;
        if (checked.threadId === null) return yield* Effect.die("Missing local checked thread");
        const threadId = checked.threadId;
        const nativeRun = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs.at(-1)!;
        yield* drainAutomationRun(b, threadId, nativeRun);
        const task = (yield* b.scheduled.list()).tasks.find(
          (task) => task.id === receipt.scheduledTaskId,
        )!;
        yield* b.scheduled.upsert({
          ...task,
          title: "Edited while the outcome is pending",
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
        });
        yield* Option.getOrThrow(b.scheduledChecks).reconcile;
        expect(checkStarts).toBe(2);
        const finished = yield* read;
        expect(finished.runs.at(-1)?.stage).toBe("done");
        expect(finished.automationCertificateId).toBeUndefined();
        expect(checked.automationCertificateId).toBeUndefined();
        const follow = yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("local-checked-r1-follow"),
          threadId,
          messageId: MessageId.make("local-checked-r1-follow-message"),
          text: "Ordinary user follow-up",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const run = follow.storedEvents.find((event) => event.event.type === "run.created");
        if (run?.event.type !== "run.created")
          return yield* Effect.die("Missing checked-thread follow-up");
        yield* drainAutomationRun(b, threadId, run.event.payload);
        expect(yield* b.authority.checkThread(threadId)).toBeUndefined();
        expect(
          yield* sql`SELECT 1 FROM peer_thread_authority WHERE thread_id=${threadId}`,
        ).toHaveLength(0);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each([503, 202, "lost-ack"] as const)(
  "automation384-r1: automatic recovery after unlink retries %s until the exact durable acknowledgement",
  (failure) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-r1-source", "Retry source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-r1-destination", "Retry destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-r1-parent");
        yield* seedParent(a, parent);
        const result = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated automatic disable recovery",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-r1-create",
        });
        expect(result.isError, json(result)).not.toBe(true);
        yield* a.links.unlink(b.environmentId);
        const originalSession = (yield* b.linkedSessions)[0]!;
        yield* b.auth.revokeSession(originalSession.sessionId);
        const sourceSql = Context.get(a.database, SqlClient.SqlClient);
        const destinationSql = Context.get(b.database, SqlClient.SqlClient);
        const first = yield* Deferred.make<void>();
        const retried = yield* Deferred.make<void>();
        let attempts = 0;
        const recoveryTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) => {
              if (!request.url.endsWith("/mcp/peer-control/automation-disable")) return effect;
              attempts++;
              if (attempts !== 1)
                return Deferred.succeed(retried, undefined).pipe(
                  Effect.andThen(TestClock.withLive(effect)),
                );
              return (failure === "lost-ack" ? TestClock.withLive(effect) : Effect.void).pipe(
                Effect.andThen(Deferred.succeed(first, undefined)),
                Effect.as(
                  HttpClientResponse.fromWeb(
                    request,
                    new Response(null, { status: failure === 202 ? 202 : 503 }),
                  ),
                ),
              );
            }),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        yield* Effect.gen(function* () {
          const recovered = yield* a.reconstructAutomationWith(recoveryTransport);
          const changes = yield* recovered.linkEvents.subscribeAutomationChanges;
          yield* recovered.peers.start;
          yield* Deferred.await(first);
          expect(attempts).toBe(1);
          expect(
            yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`,
          ).toHaveLength(1);
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(
            failure === "lost-ack" ? 1 : 0,
          );
          yield* TestClock.adjust("2 seconds");
          yield* Deferred.await(retried);
          expect(attempts).toBeGreaterThanOrEqual(2);
          while (
            (yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`)
              .length !== 0
          )
            yield* PubSub.take(changes);
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
          expect((yield* Effect.result(recovered.links.resolve(b.environmentId)))._tag).toBe(
            "Failure",
          );
          yield* TestClock.adjust("4 seconds");
          const idleAttempts = attempts;
          yield* TestClock.adjust("1 hour");
          expect(attempts).toBe(idleAttempts);
          yield* TestClock.withLive(
            Effect.gen(function* () {
              yield* linkTo(recovered.links, b, "full-access");
              const next = yield* nativeCall(a, parent, "schedule_task", {
                environmentId: b.environmentId,
                projectId: b.project.id,
                purposeIntent: { version: 1, intent: { type: "unscoped" } },
                prompt: "Fresh generation after idle recovery",
                bindToCurrentThread: false,
                schedule: { type: "interval", everyMs: 60_000 },
                clientRequestId: "automation384-r1-after-idle",
              });
              expect(next.isError, json(next)).not.toBe(true);
              yield* recovered.links.unlink(b.environmentId);
            }),
          );
          while (
            (yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`)
              .length !== 0
          )
            yield* PubSub.take(changes);
          expect(attempts).toBeGreaterThan(idleAttempts);
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(2);
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384: native restart continuation retains accepted ownership offline and disable blocks its next recovery",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-restart-source", "Restart source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const root = yield* automationWorkspace;
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("automation384-restart-destination", "Restart destination", {
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
        yield* b.fixtureSettings.updateSettings({ continueThreadsAfterServerUpdate: true });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-restart-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated restart work",
          title: "Restart authority",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "automation384-restart-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const validationAttempts = yield* traceAutomationValidation(b);
        yield* a.stopConnections;
        yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const shell = (yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!;
        const threadId = ThreadId.make(shell.thread_id);
        const prepared = (afterSequence: number) =>
          b.threads
            .streamStoredEventsFrom({ threadId, afterSequence, eventType: "run.updated" })
            .pipe(
              Stream.filter(
                (event) =>
                  event.event.type === "run.updated" && event.event.payload.status !== "preparing",
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* prepared(0);
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        yield* worker.drain();
        expect(started).toHaveLength(1);
        const running = yield* b.threads
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
        const originalOwner = (yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_run_authority WHERE run_id=${started[0]!.runId}`)[0]!;
        const recoveryOwner = Context.getOption(
          b.runtime,
          ProviderRuntimeRecovery.ProviderRuntimeRecoveryService,
        );
        if (Option.isNone(recoveryOwner))
          return yield* Effect.die("The production kernel has no runtime recovery owner");
        const recovery = recoveryOwner.value;
        yield* recovery.reconcile("startup");
        yield* worker.drain();
        yield* prepared(running.sequence);
        yield* worker.drain();
        expect(started).toHaveLength(2);
        const next = started[1]!;
        expect(
          (yield* sql<{
            record_id: string;
          }>`SELECT record_id FROM peer_run_authority WHERE run_id=${next.runId}`)[0]?.record_id,
        ).toBe(originalOwner.record_id);
        expect(
          (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs.find(
            (run) => run.id === next.runId,
          )?.restartContinuationOfRunId,
        ).toBe(started[0]!.runId);
        expect(
          (yield* Effect.result(b.registry.issue({ threadId, providerInstanceId: instanceId })))
            ._tag,
        ).toBe("Success");
        expect(yield* validationAttempts).toHaveLength(0);
        yield* b.scheduled.setEnabled({ id: receipt.scheduledTaskId, enabled: false });
        const beforeRecovery =
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        expect((yield* Effect.result(recovery.reconcile("startup")))._tag).toBe("Failure");
        expect(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        ).toEqual(beforeRecovery);
        yield* worker.drain();
        expect(started).toHaveLength(2);
        expect((yield* Effect.result(b.authority.checkThread(threadId)))._tag).toBe("Failure");
        expect(yield* validationAttempts).toHaveLength(0);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.effect(
  "automation384: known captured expiry denies the queued native credential and provider effect",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { b, threadId, certificate, started, opened, validationAttempts } =
          yield* TestClock.withLive(
            Effect.gen(function* () {
              const a = yield* makeNode(
                descriptorOf("automation384-expiry-source", "Expiry source", { peerAuthority: 1 }),
                "disk",
                { productionKernel: true, attachmentRequests: true },
              );
              const root = yield* automationWorkspace;
              const started: Adapter.ProviderAdapterV2TurnInput[] = [];
              const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
              const b = yield* makeNode(
                descriptorOf("automation384-expiry-destination", "Expiry destination", {
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
              yield* linkTo(a.links, b, "full-access");
              const parent = ThreadId.make("automation384-expiry-parent");
              yield* seedParent(a, parent);
              const created = yield* nativeCall(a, parent, "schedule_task", {
                environmentId: b.environmentId,
                projectId: b.project.id,
                purposeIntent: { version: 1, intent: { type: "unscoped" } },
                prompt: "Isolated expiry work",
                title: "Known expiry",
                bindToCurrentThread: false,
                schedule: { type: "interval", everyMs: 60_000 },
                clientRequestId: "automation384-expiry-create",
              });
              expect(created.isError, json(created)).not.toBe(true);
              const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
              const sql = Context.get(b.database, SqlClient.SqlClient);
              const stored = (yield* sql<{
                certificate_json: string;
              }>`SELECT certificate_json FROM fork_automation_certificates WHERE id=(SELECT certificate_id FROM fork_automation_bindings WHERE resource_id=${receipt.scheduledTaskId})`)[0]!;
              const certificate = yield* decodeAutomationCertificate(stored.certificate_json);
              if (certificate.expiresAt === null)
                return yield* Effect.die("Capture omitted the known grant expiry");
              const validationAttempts = yield* traceAutomationValidation(b);
              yield* a.stopConnections;
              yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
              const shell = (yield* sql<{
                thread_id: string;
              }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!;
              const threadId = ThreadId.make(shell.thread_id);
              const prepared = yield* b.threads
                .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.updated" })
                .pipe(
                  Stream.filter(
                    (event) =>
                      event.event.type === "run.updated" &&
                      event.event.payload.status !== "preparing",
                  ),
                  Stream.runHead,
                  Effect.flatMap(Effect.fromOption),
                );
              expect(
                prepared.event.type === "run.updated" ? prepared.event.payload.status : "missing",
              ).not.toBe("failed");
              expect((yield* Effect.result(b.authority.checkThread(threadId)))._tag).toBe(
                "Success",
              );
              expect(
                yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE thread_id=${threadId} AND effect_type='provider-turn.start' AND status='pending'`,
              ).toHaveLength(1);
              return {
                b,
                threadId,
                certificate: { ...certificate, expiresAt: certificate.expiresAt },
                started,
                opened,
                validationAttempts,
              };
            }),
          );
        yield* TestClock.setTime(certificate.expiresAt);
        expect(
          Exit.isFailure(
            yield* Effect.exit(b.registry.issue({ threadId, providerInstanceId: instanceId })),
          ),
        ).toBe(true);
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(started).toHaveLength(0);
        expect(opened).toHaveLength(0);
        expect(yield* validationAttempts).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, ServerSettings.layerTest()))),
);

it.live(
  "automation384: the registered timer runs a pinned command offline and local disable prevents another spawn",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const work = new Map<string, Effect.Effect<void>>();
        const scheduler = Scheduler.Scheduler.of({
          register: <E, R>(name: string, run: Effect.Effect<void, E, R>) =>
            Effect.gen(function* () {
              const context = yield* Effect.context<R>();
              work.set(name, run.pipe(Effect.provideContext(context), Effect.orDie));
            }),
        });
        let starts = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            expect(yield* ScheduleMutation.InAcceptance).toBe(false);
            starts++;
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(384),
              exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
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
        const a = yield* makeNode(
          descriptorOf("automation384-command-source", "Command source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-command-destination", "Command destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            commandSpawner: spawner,
            manualScheduler: scheduler,
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-command-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated pinned command",
          command: "isolated-command-384",
          title: "Offline command timer",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          clientRequestId: "automation384-command-timer",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const taskId = receipt.scheduledTaskId;
        yield* a.stopConnections;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        // Set only the clock-owned due slot; the registered timer performs actual admission and spawn.
        yield* sql`UPDATE scheduled_tasks SET next_run_at='2000-01-01T00:00:00.000Z' WHERE task_id=${taskId}`;
        const tick = work.get("scheduled-tasks");
        if (tick === undefined)
          return yield* Effect.die("The actual scheduler did not register its due-work owner");
        yield* tick;
        const completed = yield* b.scheduled.subscribeList().pipe(
          Stream.filter((list) =>
            list.tasks.some(
              (task) =>
                task.id === taskId &&
                task.command?.lastSuccessfulRunId !== null &&
                task.command?.lastSuccessfulRunId !== undefined,
            ),
          ),
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
        );
        expect(starts).toBe(1);
        expect(completed.tasks.find((task) => task.id === taskId)?.command?.run?.stage).toBe(
          "done",
        );
        const state = yield* CheckStore.readCheckState(taskId).pipe(
          Effect.provide(Layer.succeedContext(b.database)),
        );
        expect(state?.automationCertificateId).toBeDefined();
        expect(state?.runs.at(-1)?.automationCertificateId).toBe(state?.automationCertificateId);
        yield* b.scheduled.setEnabled({ id: taskId, enabled: false });
        yield* sql`UPDATE scheduled_tasks SET next_run_at='2000-01-02T00:00:00.000Z' WHERE task_id=${taskId}`;
        yield* tick;
        expect(starts).toBe(1);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation')`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, ServerSettings.layerTest()))),
);

it.live.each(["non-turn", "non-turn-offline", "policy", "onward"] as const)(
  "automation384-r3: peer schedule birth retains %s behavior across lifecycle and policy changes",
  (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("r3-schedule-a", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const starts: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("r3-schedule-b", "Destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
            providerAdapter: automationAdapter(starts, opened, (turn, events) =>
              turn.message.text === "R3 active onward"
                ? Effect.void
                : automationReply(turn, events, "Done"),
            ),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r3-schedule-parent");
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const originalGrant = (yield* b.authority.grants)[0]!;
        const grant = {
          ...originalGrant,
          resourceScope:
            scenario === "onward"
              ? ("environment-resources" as const)
              : originalGrant.resourceScope,
          preview: true,
          onward: true,
        };
        yield* b.authority.setGrant(grant);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "R3 scheduled work",
          bindToCurrentThread: false,
          workspaceStrategy: { type: "root" },
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "r3-schedule-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const threadId = ThreadId.make(
          (yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!
            .thread_id,
        );
        const first = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs[0]!;
        expect((yield* drainAutomationRun(b, threadId, first)).event.payload).toMatchObject({
          status: "completed",
        });
        const birth = yield* sql`SELECT * FROM peer_thread_authority WHERE thread_id=${threadId}`;
        const follow = (id: string, active = false) =>
          Effect.gen(function* () {
            const result = yield* b.threads.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(id),
              threadId,
              messageId: MessageId.make(id),
              text: active ? "R3 active onward" : "R3 ordinary follow-up",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            const run = result.storedEvents.find((event) => event.event.type === "run.created");
            if (run?.event.type !== "run.created")
              return yield* Effect.die("Missing R3 follow-up run");
            if (active) yield* awaitAutomationRunning(b, threadId);
            else
              expect(
                (yield* drainAutomationRun(b, threadId, run.event.payload)).event.payload,
              ).toMatchObject({ status: "completed" });
          });
        if (scenario === "non-turn" || scenario === "non-turn-offline") {
          const control = yield* delegate(a, b, parent, "r3-interactive-control");
          const rename = (target: ThreadId, title: string) =>
            nativeCall(a, parent, "t3_thread_update", {
              environmentId: b.environmentId,
              threadId: target,
              action: "rename",
              title,
            });
          expect((yield* rename(control.childId, "Interactive control")).isError).not.toBe(true);
          const renamed = yield* rename(threadId, "Automation member renamed");
          expect(renamed.isError, json(renamed)).not.toBe(true);
          expect((yield* b.threads.getThreadShell(threadId))?.title).toBe(
            "Automation member renamed",
          );
          yield* follow("r3-interrupt-active", true);
          const stopped = yield* nativeCall(a, parent, "t3_thread_interrupt", {
            environmentId: b.environmentId,
            threadId,
            stop: true,
            clientRequestId: "r3-stop",
          });
          expect(stopped.isError, json(stopped)).not.toBe(true);
          yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
          expect(
            yield* sql`SELECT e.status FROM orchestration_v2_effect_outbox e JOIN peer_effect_authority p ON p.effect_id=e.effect_id WHERE e.thread_id=${threadId} AND e.effect_type='provider-turn.interrupt'`,
          ).toEqual([{ status: "succeeded" }]);
          if (scenario === "non-turn") yield* b.auth.revokeSession(grant.sessionId);
          else yield* a.stopConnections;
          const refused = yield* Effect.exit(
            rename(threadId, "Must refuse unavailable original source"),
          );
          expect(refused._tag === "Failure" || refused.value.isError === true, json(refused)).toBe(
            true,
          );
          expect((yield* b.threads.getThreadShell(threadId))?.title).toBe(
            "Automation member renamed",
          );
        } else if (scenario === "policy") {
          const broadened = {
            ...grant,
            resourceScope: "environment-resources" as const,
            device: true,
          };
          yield* b.authority.setGrant(broadened);
          const owner = yield* b.authority.checkThread(threadId);
          expect(owner?.ceiling.resourceScope).toBe("created-work");
          expect(owner?.ceiling.capabilities).not.toContain("device");
          yield* follow("r3-policy-broadened");
          expect(yield* sql`SELECT disabled,revoked FROM fork_automation_certificates`).toEqual([
            { disabled: 1, revoked: 0 },
          ]);
          const domain = Context.get(
            yield* AutomationAuthority.layer.pipe(
              Layer.provide(Layer.succeedContext(b.database)),
              Layer.build,
            ),
            AutomationAuthority.DestinationAutomationAuthority,
          );
          const accepted = (yield* sql<{
            certificate_id: string;
          }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind='schedule' AND resource_id=${receipt.scheduledTaskId}`)[0]!;
          expect((yield* Effect.result(domain.claim(accepted.certificate_id)))._tag).toBe(
            "Failure",
          );
          yield* b.authority.setGrant({ ...broadened, preview: false });
          const narrowed = yield* Effect.result(follow("r3-policy-narrowed"));
          expect(narrowed._tag).toBe("Failure");
          const liveNarrowed = yield* Effect.result(b.authority.checkThread(threadId));
          expect(liveNarrowed._tag).toBe("Failure");
          if (liveNarrowed._tag === "Failure")
            expect(liveNarrowed.failure.message).toContain("narrowed by current policy");
          expect(yield* sql`SELECT revoked FROM fork_automation_certificates`).toEqual([
            { revoked: 0 },
          ]);
          yield* b.authority.setGrant(broadened);
          yield* follow("r3-policy-restored");
          yield* b.auth.revokeSession(grant.sessionId);
          expect((yield* Effect.result(b.authority.setGrant(broadened)))._tag).toBe("Failure");
          expect((yield* Effect.result(follow("r3-revoked-cannot-restore")))._tag).toBe("Failure");
          expect(yield* sql`SELECT revoked FROM fork_automation_certificates`).toEqual([
            { revoked: 1 },
          ]);
        } else {
          yield* b.scheduled.setEnabled({ id: receipt.scheduledTaskId, enabled: false });
          yield* follow("r3-schedule-onward-active", true);
          const c = yield* makeNode(
            descriptorOf("r3-schedule-c", "Onward", { peerAuthority: 1 }),
            "disk",
            { productionKernel: true, attachmentRequests: true },
          );
          yield* linkTo(b.links, c, "full-access");
          expect(
            (yield* nativeCall(b, threadId, "t3_project_list", { environmentId: c.environmentId }))
              .isError,
          ).not.toBe(true);
          yield* c.authority.setGrant({
            ...(yield* c.authority.grants)[0]!,
            resourceScope: "environment-resources",
          });
          const args = {
            environmentId: c.environmentId,
            projectId: c.project.id,
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            prompt: "R3 onward from retired schedule",
            bindToCurrentThread: false,
            schedule: { type: "interval", everyMs: 60000 },
          };
          const onward = yield* nativeCall(b, threadId, "schedule_task", {
            ...args,
            clientRequestId: "r3-schedule-onward",
          });
          expect(onward.isError, json(onward)).not.toBe(true);
          const before = (yield* c.scheduled.list()).tasks;
          expect(before).toHaveLength(1);
          yield* a.stopConnections;
          const offline = yield* Effect.exit(
            nativeCall(b, threadId, "schedule_task", {
              ...args,
              clientRequestId: "r3-schedule-offline",
            }),
          );
          expect(offline._tag === "Failure" || offline.value.isError === true, json(offline)).toBe(
            true,
          );
          expect((yield* c.scheduled.list()).tasks).toEqual(before);
        }
        expect(yield* sql`SELECT * FROM peer_thread_authority WHERE thread_id=${threadId}`).toEqual(
          birth,
        );
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, ServerSettings.layerTest()))),
);

it.live(
  "automation384-r4: original recovery identity reaches a relocated, freshly paired destination",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("r4-moving-a", "R4 source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("r4-moving-b", "R4 destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r4-moving-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "R4 isolated relocation recovery",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "r4-moving-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        yield* a.stopConnections;
        yield* a.links.unlink(b.environmentId);
        const originalSession = (yield* b.linkedSessions)[0]!;
        yield* b.auth.revokeSession(originalSession.sessionId);
        const pending = (yield* a.authority.pendingAutomationDisables)[0]!;
        const original = yield* a.links.resolveAutomationRecovery!(
          pending.environmentId,
          pending.generation,
        );
        const restarted = yield* b.restartSchedulerTransport;
        expect(restarted.url).not.toBe(original.url);
        yield* linkTo(a.links, restarted, "full-access");
        const ordinary = yield* a.links.resolve(b.environmentId);
        expect(ordinary.url).toBe(restarted.url);
        expect(ordinary.link.launchIdentity).not.toBe(pending.generation);
        const requests: string[] = [];
        const movedTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) => {
              requests.push(request.url);
              return request.url.startsWith(original.url + "/")
                ? Effect.succeed(
                    HttpClientResponse.fromWeb(request, new Response(null, { status: 503 })),
                  )
                : effect;
            }),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        const recoveredAuthority = (yield* a.reconstructAutomation).authority;
        const relocatedLinks = Context.get(
          yield* PeerLinks.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeedContext(a.database),
                Layer.succeed(
                  Secrets.ServerSecretStore,
                  Context.get(a.services, Secrets.ServerSecretStore),
                ),
                Layer.succeed(Environment.ServerEnvironment, {
                  getEnvironmentId: Effect.succeed(a.environmentId),
                  getDescriptor: Effect.succeed(
                    descriptorOf("r4-moving-a", "R4 source", { peerAuthority: 1 }),
                  ),
                }),
                movedTransport,
              ),
            ),
            Layer.fresh,
            Layer.build,
          ),
          PeerLinks.PeerLinks,
        );
        const relocatedClient = Context.get(
          yield* PeerMcpClient.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(PeerLinks.PeerLinks, relocatedLinks),
                Layer.succeed(PeerAuthority.PeerAuthority, recoveredAuthority),
                movedTransport,
              ),
            ),
            Layer.fresh,
            Layer.build,
          ),
          PeerMcpClient.PeerMcpClient,
        );
        const recovered = {
          authority: recoveredAuthority,
          links: relocatedLinks,
          peers: relocatedClient,
        };
        const storedDescriptor = yield* Ref.get(restarted.descriptor);
        yield* Ref.set(restarted.descriptor, {
          ...storedDescriptor,
          environmentId: EnvironmentId.make("r4-unrelated-destination"),
        });
        expect(
          (yield* Effect.result(
            recovered.links.resolveAutomationRecovery!(pending.environmentId, pending.generation),
          ))._tag,
        ).toBe("Failure");
        yield* recovered.peers.drainAutomationDisables!;
        expect(yield* recovered.authority.pendingAutomationDisables).toHaveLength(1);
        expect(requests.some((url) => url.endsWith("/automation-disable"))).toBe(false);
        yield* Ref.set(restarted.descriptor, storedDescriptor);
        expect(
          (yield* Effect.result(
            recovered.links.resolveAutomationRecovery!(
              pending.environmentId,
              "r4-wrong-original-generation",
            ),
          ))._tag,
        ).toBe("Failure");
        const resolved = yield* Effect.result(
          recovered.links.resolveAutomationRecovery!(pending.environmentId, pending.generation),
        );
        expect(resolved._tag).toBe("Success");
        if (resolved._tag === "Failure") return yield* Effect.die(resolved.failure);
        expect(resolved.success.url).toBe(restarted.url);
        expect(resolved.success.token).toBe(original.token);
        expect(resolved.success.link.launchIdentity).toBe(pending.generation);
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(DateTime.toEpochMillis(ordinary.link.expiresAt) + 1);
          const expired = yield* Effect.result(recovered.links.resolve(b.environmentId));
          expect(expired._tag).toBe("Failure");
          if (expired._tag === "Success")
            return yield* Effect.die("An expired ordinary link was accepted");
          expect(expired.failure.reason).toBe("expired");
          const recovery = yield* recovered.links.resolveAutomationRecovery!(
            pending.environmentId,
            pending.generation,
          );
          expect(recovery.url).toBe(restarted.url);
          expect(recovery.token).toBe(original.token);
          yield* recovered.peers.drainAutomationDisables!;
        }).pipe(Effect.provide(TestClock.layer()));
        expect(yield* recovered.authority.pendingAutomationDisables).toEqual([]);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        expect(
          requests.some((url) => url === restarted.url + "/mcp/peer-control/automation-disable"),
        ).toBe(true);
        // The original protected token is valid for its exact registered disable at the new destination route.
        const http = yield* HttpClient.HttpClient;
        const ordinaryRetired = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({ jsonrpc: "2.0", id: "retired-r4", method: "tools/list", params: {} }),
                "application/json",
              ),
            ),
          ),
        );
        expect(ordinaryRetired.status).toBe(401);
        const wrong = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp/peer-control/automation-disable").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  version: 1,
                  dependency: { ...pending.dependency, definitionDigest: "r4-wrong-definition" },
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(wrong.status).toBe(403);
        const response = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp/peer-control/automation-disable").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({ version: 1, dependency: pending.dependency }),
                "application/json",
              ),
            ),
          ),
        );
        expect(response.status).toBe(200);
        const ack = yield* decodeAutomationAck(yield* response.text);
        expect(ack.dependency).toEqual(pending.dependency);
        yield* recovered.authority.acknowledgeAutomationDisable(
          ack,
          pending.environmentId,
          pending.generation,
        );
        expect(yield* recovered.authority.pendingAutomationDisables).toEqual([]);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each(["browser-off-after", "device-default", "browser-off-nested"] as const)(
  "automation384-r4: %s filters capabilities without denying unrelated native birth work",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`probe384r4-${variant}-source`, "Probe source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const started: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf(`probe384r4-${variant}-dest`, "Probe destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            providerAdapter: automationAdapter(started, opened, (turn, events) =>
              turn.message.text === "R4 nested capture"
                ? Effect.void
                : automationReply(turn, events, "Done"),
            ),
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`probe384r4-${variant}-parent`);
        yield* seedParent(a, parent);
        // Interactive-born control under the same link, project and grant.
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Interactive control",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: `probe384r4-${variant}-child`,
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        const originalGrant = (yield* b.authority.grants)[0]!;
        const grant = {
          ...originalGrant,
          resourceScope:
            variant === "browser-off-nested"
              ? ("environment-resources" as const)
              : originalGrant.resourceScope,
        };
        if (variant !== "device-default") {
          yield* b.authority.setGrant({ ...grant, preview: true });
        } else {
          // Product default for B: agent device access is off.
          yield* b.fixtureSettings.updateSettings({ enableAgentDeviceAccess: false });
          yield* b.authority.setGrant({ ...grant, device: true });
        }
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          title: "Probe schedule",
          prompt: "Scheduled work",
          schedule: { type: "interval", everyMs: 60_000 },
          bindToCurrentThread: false,
          workspaceStrategy: { type: "root" },
          clientRequestId: `probe384r4-${variant}-create`,
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const certificate = yield* decodeAutomationCertificate(
          (yield* sql<{
            certificate_json: string;
          }>`SELECT certificate_json FROM fork_automation_certificates`)[0]!.certificate_json,
        );
        expect(certificate.ceiling.capabilities).toContain(
          variant === "device-default" ? "device" : "preview",
        );
        yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
        let threadId = ThreadId.make(
          (yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id} AND thread_id<>${child}`)[0]!
            .thread_id,
        );
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const run = (target: ThreadId, created: { readonly id: RunId; readonly status: string }) =>
          Effect.gen(function* () {
            if (created.status === "preparing")
              yield* b.threads
                .streamStoredEventsFrom({
                  threadId: target,
                  afterSequence: 0,
                  eventType: "run.updated",
                })
                .pipe(
                  Stream.filter(
                    (event) =>
                      event.event.type === "run.updated" &&
                      event.event.payload.id === created.id &&
                      event.event.payload.status !== "preparing",
                  ),
                  Stream.runHead,
                  Effect.flatMap(Effect.fromOption),
                );
            yield* worker.drain();
            yield* finishAutomationRun(b, target, created.id);
          });
        const first = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs[0]!;
        yield* run(threadId, first);
        if (variant === "browser-off-nested") {
          yield* b.threads.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r4-nested-active"),
            threadId,
            messageId: MessageId.make("r4-nested-active"),
            text: "R4 nested capture",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* awaitAutomationRunning(b, threadId);
          const nested = yield* nativeCall(b, threadId, "schedule_task", {
            projectId: b.project.id,
            purposeIntent: { version: 1, intent: { type: "unscoped" } },
            prompt: "Nested local birth",
            bindToCurrentThread: false,
            workspaceStrategy: { type: "root" },
            schedule: { type: "interval", everyMs: 60_000 },
            clientRequestId: "r4-nested-create",
          });
          expect(nested.isError, json(nested)).not.toBe(true);
          const nestedReceipt = yield* decodeAutomationScheduleReceipt(nested.structuredContent);
          const nestedManual = yield* b.scheduled.runNow({ id: nestedReceipt.scheduledTaskId });
          expect(nestedManual.task.lastRunError, json(nestedManual)).toBeNull();
          yield* worker.drain();
          const nestedRows = yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id} AND thread_id<>${child} AND thread_id<>${threadId}`;
          expect(nestedRows).toHaveLength(1);
          threadId = ThreadId.make(nestedRows[0]!.thread_id);
          const created = yield* b.threads
            .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "run.created" })
            .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));
          if (created.event.type !== "run.created")
            return yield* Effect.die("Missing nested native run");
          yield* run(threadId, created.event.payload);
        }
        const follow = (target: ThreadId, label: string) =>
          Effect.gen(function* () {
            const result = yield* b.threads.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`probe384r4-${variant}-${label}`),
              threadId: target,
              messageId: MessageId.make(`probe384r4-${variant}-message-${label}`),
              text: "Ordinary local user follow-up",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            const createdRun = result.storedEvents.find(
              (event) => event.event.type === "run.created",
            );
            if (createdRun?.event.type !== "run.created")
              return yield* Effect.die("Missing follow-up run");
            yield* run(target, createdRun.event.payload);
          });
        if (variant !== "device-default") {
          const before = yield* Effect.result(follow(threadId, "before"));
          expect(before._tag).toBe("Success");
          yield* b.fixtureSettings.updateSettings({ enableAgentBrowserAccess: false });
        }
        const startsBefore = started.length;
        const after = yield* Effect.result(follow(threadId, "after"));
        const owner = yield* b.authority.checkThread(threadId);
        expect(owner?.ceiling.capabilities).not.toContain(
          variant === "device-default" ? "device" : "preview",
        );
        expect((yield* Effect.result(b.authority.checkThread(child)))._tag).toBe("Success");
        const renamed = yield* nativeCall(a, parent, "t3_thread_update", {
          environmentId: b.environmentId,
          threadId: child,
          action: "rename",
          title: "R4 interactive control",
        });
        expect(renamed.isError, json(renamed)).not.toBe(true);
        if (variant === "device-default") {
          const deniedTool = yield* nativeInvalidParams(b, threadId, "device_commands", {});
          expect(deniedTool.error.message).toMatch(/not found|not available|unknown tool/i);
        } else {
          const deniedTool = yield* nativeCall(b, threadId, "preview_status", {});
          expect(deniedTool.isError, json(deniedTool)).toBe(true);
          expect(json(deniedTool)).toMatch(/browser|preview|capability/i);
        }
        if (variant !== "device-default") {
          yield* b.fixtureSettings.updateSettings({ enableAgentBrowserAccess: true });
          expect((yield* Effect.result(follow(threadId, "restored")))._tag).toBe("Success");
        }
        expect(after._tag).toBe("Success");
        expect(started.length).toBeGreaterThan(startsBefore);
        yield* b.authority.setGrant({ ...grant, preview: false, device: false });
        expect((yield* Effect.result(b.authority.checkThread(threadId)))._tag).toBe("Failure");
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);
it.live(
  "automation384-r4: birth-only onward automation survives broadening and receives exact DISABLE on real revocation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("probe384r4-ob-a", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const starts: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const b = yield* makeNode(
          descriptorOf("probe384r4-ob-b", "Destination", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: yield* automationWorkspace,
            manualScheduler: Scheduler.Scheduler.of({ register: () => Effect.void }),
            providerAdapter: automationAdapter(starts, opened, (turn, events) =>
              turn.message.text === "R3 active onward"
                ? Effect.void
                : automationReply(turn, events, "Done"),
            ),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("probe384r4-ob-parent");
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const grant = {
          ...(yield* b.authority.grants)[0]!,
          resourceScope: "environment-resources" as const,
          onward: true,
        };
        yield* b.authority.setGrant(grant);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Scheduled work",
          bindToCurrentThread: false,
          workspaceStrategy: { type: "root" },
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "probe384r4-ob-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(created.structuredContent);
        yield* b.scheduled.runNow({ id: receipt.scheduledTaskId });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const threadId = ThreadId.make(
          (yield* sql<{
            thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`)[0]!
            .thread_id,
        );
        const first = (yield* b.threads.getThreadRecords(threadId, ["runs"])).runs[0]!;
        yield* drainAutomationRun(b, threadId, first);
        // A retired definition must not retire its thread's original live contributors.
        yield* b.scheduled.setEnabled({ id: receipt.scheduledTaskId, enabled: false });
        const result = yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("probe384r4-ob-active"),
          threadId,
          messageId: MessageId.make("probe384r4-ob-active"),
          text: "R3 active onward",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        expect(result.storedEvents.some((event) => event.event.type === "run.created")).toBe(true);
        yield* awaitAutomationRunning(b, threadId);
        const c = yield* makeNode(
          descriptorOf("probe384r4-ob-c", "Onward", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(b.links, c, "full-access");
        expect(
          (yield* nativeCall(b, threadId, "t3_project_list", { environmentId: c.environmentId }))
            .isError,
        ).not.toBe(true);
        yield* c.authority.setGrant({
          ...(yield* c.authority.grants)[0]!,
          resourceScope: "environment-resources",
        });
        const onward = yield* nativeCall(b, threadId, "schedule_task", {
          environmentId: c.environmentId,
          projectId: c.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Onward from completed birth",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "probe384r4-ob-onward",
        });
        expect(onward.isError, json(onward)).not.toBe(true);
        const csql = Context.get(c.database, SqlClient.SqlClient);
        // Pure broadening on B: add device to A's grant. No revocation.
        yield* b.authority.setGrant({ ...grant, device: true });
        const peers = Context.get(b.services, PeerMcpClient.PeerMcpClient);
        if (peers.drainAutomationDisables === undefined)
          return yield* Effect.die("Missing production disable driver");
        yield* peers.drainAutomationDisables;
        const cRows = yield* csql`SELECT disabled,revoked FROM fork_automation_certificates`;
        const cTask = (yield* c.scheduled.list()).tasks[0]!;
        expect(cRows).toEqual([{ disabled: 0, revoked: 0 }]);
        expect(yield* csql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(0);
        expect(yield* b.authority.pendingAutomationDisables).toEqual([]);
        expect(cTask.enabled).toBe(true);
        yield* b.auth.revokeSession(grant.sessionId);
        const pending = (yield* b.authority.pendingAutomationDisables).filter(
          (item) => item.environmentId === c.environmentId,
        );
        expect(pending.length).toBeGreaterThan(0);
        yield* peers.drainAutomationDisables;
        expect(yield* csql`SELECT disabled,revoked FROM fork_automation_certificates`).toEqual([
          { disabled: 1, revoked: 1 },
        ]);
        const acknowledgements = yield* csql<{
          ack_json: string;
        }>`SELECT ack_json FROM fork_automation_disable_acks`;
        expect(acknowledgements).toHaveLength(pending.length);
        for (const item of pending)
          expect(
            yield* Effect.forEach(acknowledgements, (row) => decodeAutomationAck(row.ack_json)),
          ).toContainEqual({ version: 1, dependency: item.dependency, disabled: true });
        expect(yield* b.authority.pendingAutomationDisables).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, ServerSettings.layerTest()))),
);

it.live(
  "automation384-r5: repeated unlink and reconstruction retain only the original recovery identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("r5-moving-a", "R5 source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("r5-moving-b", "R5 destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("r5-moving-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "R5 isolated relocation recovery",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60000 },
          clientRequestId: "r5-moving-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        yield* a.stopConnections;
        yield* a.links.unlink(b.environmentId);
        const originalSession = (yield* b.linkedSessions)[0]!;
        yield* b.auth.revokeSession(originalSession.sessionId);
        const pending = (yield* a.authority.pendingAutomationDisables)[0]!;
        const original = yield* a.links.resolveAutomationRecovery!(
          pending.environmentId,
          pending.generation,
        );
        const restarted = yield* b.restartSchedulerTransport;
        expect(restarted.url).not.toBe(original.url);
        yield* linkTo(a.links, restarted, "full-access");
        const ordinary = yield* a.links.resolve(b.environmentId);
        expect(ordinary.url).toBe(restarted.url);
        expect(ordinary.link.launchIdentity).not.toBe(pending.generation);
        const requests: string[] = [];
        const movedTransport = Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.transform(client, (effect, request) => {
              requests.push(request.url);
              return request.url.startsWith(original.url + "/")
                ? Effect.succeed(
                    HttpClientResponse.fromWeb(request, new Response(null, { status: 503 })),
                  )
                : effect;
            }),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer));
        // Reopen the pre-R5 private schema while the replacement ordinary link still exists.
        const sourceSql = Context.get(a.database, SqlClient.SqlClient);
        if (
          (yield* sourceSql<{
            name: string;
          }>`PRAGMA table_info(fork_automation_retired_links)`).some(
            (column) => column.name === "paired_primary_origin",
          )
        )
          yield* sourceSql`ALTER TABLE fork_automation_retired_links DROP COLUMN paired_primary_origin`;
        const recoveredAuthority = (yield* a.reconstructAutomation).authority;
        const rebuildLinks = Effect.gen(function* () {
          return Context.get(
            yield* PeerLinks.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeedContext(a.database),
                  Layer.succeed(
                    Secrets.ServerSecretStore,
                    Context.get(a.services, Secrets.ServerSecretStore),
                  ),
                  Layer.succeed(Environment.ServerEnvironment, {
                    getEnvironmentId: Effect.succeed(a.environmentId),
                    getDescriptor: Effect.succeed(
                      descriptorOf("r5-moving-a", "R5 source", { peerAuthority: 1 }),
                    ),
                  }),
                  movedTransport,
                ),
              ),
              Layer.fresh,
              Layer.build,
            ),
            PeerLinks.PeerLinks,
          );
        });
        const relocatedLinks = yield* rebuildLinks;
        const relocatedClient = Context.get(
          yield* PeerMcpClient.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(PeerLinks.PeerLinks, relocatedLinks),
                Layer.succeed(PeerAuthority.PeerAuthority, recoveredAuthority),
                movedTransport,
              ),
            ),
            Layer.fresh,
            Layer.build,
          ),
          PeerMcpClient.PeerMcpClient,
        );
        const recovered = {
          authority: recoveredAuthority,
          links: relocatedLinks,
          peers: relocatedClient,
        };
        const storedDescriptor = yield* Ref.get(restarted.descriptor);
        yield* Ref.set(restarted.descriptor, {
          ...storedDescriptor,
          environmentId: EnvironmentId.make("r5-unrelated-destination"),
        });
        expect(
          (yield* Effect.result(
            recovered.links.resolveAutomationRecovery!(pending.environmentId, pending.generation),
          ))._tag,
        ).toBe("Failure");
        yield* recovered.peers.drainAutomationDisables!;
        expect(yield* recovered.authority.pendingAutomationDisables).toHaveLength(1);
        expect(requests.some((url) => url.endsWith("/automation-disable"))).toBe(false);
        yield* Ref.set(restarted.descriptor, storedDescriptor);
        expect(
          (yield* Effect.result(
            recovered.links.resolveAutomationRecovery!(
              pending.environmentId,
              "r5-wrong-original-generation",
            ),
          ))._tag,
        ).toBe("Failure");
        const resolved = yield* Effect.result(
          recovered.links.resolveAutomationRecovery!(pending.environmentId, pending.generation),
        );
        expect(resolved._tag).toBe("Success");
        if (resolved._tag === "Failure") return yield* Effect.die(resolved.failure);
        expect(resolved.success.url).toBe(restarted.url);
        expect(resolved.success.token).toBe(original.token);
        expect(resolved.success.link.launchIdentity).toBe(pending.generation);
        const successor = (yield* sourceSql<{
          secret_name: string;
        }>`SELECT secret_name FROM peer_environment_links WHERE environment_id=${b.environmentId}`)[0]!;
        yield* recovered.links.unlink(b.environmentId);
        expect((yield* Effect.result(recovered.links.resolve(b.environmentId)))._tag).toBe(
          "Failure",
        );
        expect(
          yield* Context.get(a.services, Secrets.ServerSecretStore).get(successor.secret_name),
        ).toEqual(Option.none());
        const rebuiltLinks = yield* rebuildLinks;
        yield* Ref.set(restarted.descriptor, {
          ...storedDescriptor,
          environmentId: EnvironmentId.make("r5-unrelated-after-unlink"),
        });
        expect(
          (yield* Effect.result(
            rebuiltLinks.resolveAutomationRecovery!(pending.environmentId, pending.generation),
          ))._tag,
        ).toBe("Failure");
        yield* Ref.set(restarted.descriptor, storedDescriptor);
        const afterUnlink = yield* rebuiltLinks.resolveAutomationRecovery!(
          pending.environmentId,
          pending.generation,
        );
        expect(afterUnlink.url).toBe(restarted.url);
        expect(afterUnlink.token).toBe(original.token);
        expect(afterUnlink.link.launchIdentity).toBe(pending.generation);
        // A further paired generation must not be required to keep the earlier route alive.
        const pairing = yield* restarted.auth.issuePairingCredential();
        yield* rebuiltLinks.link({
          url: restarted.url,
          alternateUrls: ["https://unpaired.invalid"],
          pairingCode: pairing.credential,
          access: "full-access",
        });
        yield* rebuiltLinks.unlink(b.environmentId);
        const finalLinks = yield* rebuildLinks;
        const unreachableStart = requests.length;
        yield* Ref.set(restarted.descriptor, {
          ...storedDescriptor,
          environmentId: EnvironmentId.make("r5-unrelated-final"),
        });
        expect(
          (yield* Effect.result(
            finalLinks.resolveAutomationRecovery!(pending.environmentId, pending.generation),
          ))._tag,
        ).toBe("Failure");
        expect(
          requests
            .slice(unreachableStart)
            .some((url) => url.startsWith("https://unpaired.invalid")),
        ).toBe(false);
        yield* Ref.set(restarted.descriptor, storedDescriptor);
        const finalClient = Context.get(
          yield* PeerMcpClient.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(PeerLinks.PeerLinks, finalLinks),
                Layer.succeed(PeerAuthority.PeerAuthority, recoveredAuthority),
                movedTransport,
              ),
            ),
            Layer.fresh,
            Layer.build,
          ),
          PeerMcpClient.PeerMcpClient,
        );
        const requestCount = requests.length;
        yield* finalClient.drainAutomationDisables!;
        expect(yield* recovered.authority.pendingAutomationDisables).toEqual([]);
        expect(
          requests
            .slice(requestCount)
            .some(
              (url) => url.startsWith(restarted.url + "/") && url.endsWith("/automation-disable"),
            ),
        ).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        expect(
          (yield* Effect.result(
            finalLinks.resolveAutomationRecovery!(pending.environmentId, pending.generation),
          ))._tag,
        ).toBe("Failure");
        expect(
          (yield* sourceSql<{
            origin: string | null;
          }>`SELECT paired_primary_origin AS origin FROM fork_automation_retired_links WHERE generation=${pending.generation}`)[0]
            ?.origin,
        ).toBeNull();
        // The original protected token is valid for its exact registered disable at the new destination route.
        const http = yield* HttpClient.HttpClient;
        const ordinaryRetired = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({ jsonrpc: "2.0", id: "retired-r5", method: "tools/list", params: {} }),
                "application/json",
              ),
            ),
          ),
        );
        expect(ordinaryRetired.status).toBe(401);
        const wrong = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp/peer-control/automation-disable").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  version: 1,
                  dependency: { ...pending.dependency, definitionDigest: "r5-wrong-definition" },
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(wrong.status).toBe(403);
        const response = yield* http.execute(
          HttpClientRequest.post(restarted.url + "/mcp/peer-control/automation-disable").pipe(
            HttpClientRequest.setHeader("authorization", "Bearer " + original.token),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({ version: 1, dependency: pending.dependency }),
                "application/json",
              ),
            ),
          ),
        );
        expect(response.status).toBe(200);
        const ack = yield* decodeAutomationAck(yield* response.text);
        expect(ack.dependency).toEqual(pending.dependency);
        yield* recovered.authority.acknowledgeAutomationDisable(
          ack,
          pending.environmentId,
          pending.generation,
        );
        expect(yield* recovered.authority.pendingAutomationDisables).toEqual([]);
        expect(yield* sql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live(
  "automation384-r6: a captured known expiry wakes reconstructed idle recovery and durably acknowledges without another mutation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("r6-expiry-a", "Expiry source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("r6-expiry-b", "Expiry destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const sourceSql = Context.get(a.database, SqlClient.SqlClient);
        const destinationSql = Context.get(b.database, SqlClient.SqlClient);
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const expiry = now + 60_000;
        yield* sourceSql`UPDATE peer_environment_links SET expires_at=${DateTime.formatIso(DateTime.makeUnsafe(expiry))} WHERE environment_id=${b.environmentId}`;
        const parent = ThreadId.make("r6-expiry-parent");
        yield* seedParent(a, parent);
        const created = yield* nativeCall(a, parent, "schedule_task", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "Isolated captured expiry",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60_000 },
          clientRequestId: "r6-expiry-create",
        });
        expect(created.isError, json(created)).not.toBe(true);
        const before = yield* sourceSql<{
          expires_at: number;
          pending: number;
          ack_json: string | null;
        }>`SELECT expires_at,pending,ack_json FROM fork_automation_obligations`;
        expect(before).toHaveLength(1);
        expect(before[0]).toMatchObject({ expires_at: expiry, pending: 0, ack_json: null });
        yield* a.stopConnections;
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(now);
          const recovered = yield* a.reconstructAutomation;
          const armed = yield* Deferred.make<void>();
          const selected = yield* Deferred.make<void>();
          let deadlineReads = 0;
          const actualAuthority = PeerAuthority.PeerAuthority.of({
            ...recovered.authority,
            nextAutomationDisableDeadline: recovered.authority.nextAutomationDisableDeadline.pipe(
              Effect.tap((deadline) => {
                if (deadline === null) return Effect.void;
                expect(deadline).toBe(expiry);
                deadlineReads++;
                return deadlineReads >= 2 ? Deferred.succeed(armed, undefined) : Effect.void;
              }),
            ),
            pendingAutomationDisables: recovered.authority.pendingAutomationDisables.pipe(
              Effect.tap((batch) =>
                batch.length !== 0 ? Deferred.succeed(selected, undefined) : Effect.void,
              ),
            ),
          });
          const transport = Layer.effect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (client) =>
              HttpClient.transform(client, (effect) => TestClock.withLive(effect)),
            ),
          ).pipe(Layer.provide(FetchHttpClient.layer));
          const peers = Context.get(
            yield* PeerMcpClient.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(PeerLinks.PeerLinks, recovered.links),
                  Layer.succeed(PeerAuthority.PeerAuthority, actualAuthority),
                  Layer.succeed(PeerLinkEvents.PeerLinkEvents, recovered.linkEvents),
                  transport,
                ),
              ),
              Layer.fresh,
              Layer.build,
            ),
            PeerMcpClient.PeerMcpClient,
          );
          const changes = yield* recovered.linkEvents.subscribeAutomationChanges;
          yield* peers.start;
          yield* Deferred.await(armed);
          yield* TestClock.adjust("0 millis");
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(0);
          yield* TestClock.setTime(expiry + 1);
          yield* Deferred.await(selected);
          const due = (yield* sourceSql<{
            pending: number;
            ack_json: string | null;
          }>`SELECT pending,ack_json FROM fork_automation_obligations`)[0]!;
          expect(due.pending === 1 || due.ack_json !== null).toBe(true);
          while (
            (yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE ack_json IS NULL`)
              .length !== 0
          )
            yield* PubSub.take(changes);
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
          expect(yield* recovered.authority.pendingAutomationDisables).toEqual([]);
          const expired = yield* Effect.result(recovered.links.resolve(b.environmentId));
          expect(expired._tag).toBe("Failure");
          if (expired._tag === "Failure") expect(expired.failure.reason).toBe("expired");
          yield* TestClock.adjust("2 hours");
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);

it.live.each(["late", "earlier", "simultaneous"] as const)(
  "automation384-r6: parked recovery rearms from actual capture registration (%s)",
  (variant) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`r6-${variant}-a`, "Register source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf(`r6-${variant}-b`, "Register destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const sourceSql = Context.get(a.database, SqlClient.SqlClient);
        const destinationSql = Context.get(b.database, SqlClient.SqlClient);
        const parent = ThreadId.make(`r6-${variant}-parent`);
        yield* seedParent(a, parent);
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const early = now + 60_000;
        const late = now + 120_000;
        const events = Context.get(a.services, PeerLinkEvents.PeerLinkEvents);
        yield* Effect.gen(function* () {
          yield* TestClock.setTime(now);
          const parked = yield* Deferred.make<void>();
          const earlyArmed = yield* Deferred.make<void>();
          const lateArmed = yield* Deferred.make<void>();
          const lateRearmed = yield* Deferred.make<void>();
          const cancelled = yield* Deferred.make<void>();
          let lateReads = 0;
          let cancelling = false;
          const selected = yield* Deferred.make<void>();
          let emptyReads = 0;
          let attempts = 0;
          const observedAuthority = PeerAuthority.PeerAuthority.of({
            ...a.authority,
            nextAutomationDisableDeadline: a.authority.nextAutomationDisableDeadline.pipe(
              Effect.tap((deadline) => {
                if (deadline === early) return Deferred.succeed(earlyArmed, undefined);
                if (deadline === late) {
                  lateReads++;
                  return Deferred.succeed(lateReads >= 2 ? lateRearmed : lateArmed, undefined);
                }
                if (deadline === null && cancelling) return Deferred.succeed(cancelled, undefined);
                if (deadline === null && ++emptyReads >= 2)
                  return Deferred.succeed(parked, undefined);
                return Effect.void;
              }),
            ),
            pendingAutomationDisables: a.authority.pendingAutomationDisables.pipe(
              Effect.tap((batch) =>
                batch.length ? Deferred.succeed(selected, undefined) : Effect.void,
              ),
            ),
          });
          const transport = Layer.effect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (client) =>
              HttpClient.transform(client, (effect, request) => {
                if (request.url.endsWith("/automation-disable")) attempts++;
                return TestClock.withLive(effect);
              }),
            ),
          ).pipe(Layer.provide(FetchHttpClient.layer));
          const peers = Context.get(
            yield* PeerMcpClient.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(PeerLinks.PeerLinks, a.links),
                  Layer.succeed(PeerAuthority.PeerAuthority, observedAuthority),
                  Layer.succeed(PeerLinkEvents.PeerLinkEvents, events),
                  transport,
                ),
              ),
              Layer.fresh,
              Layer.build,
            ),
            PeerMcpClient.PeerMcpClient,
          );
          const changes = yield* events.subscribeAutomationChanges;
          yield* peers.start;
          yield* Deferred.await(parked);
          const create = (key: string, expiry: number) =>
            TestClock.withLive(
              Effect.gen(function* () {
                yield* sourceSql`UPDATE peer_environment_links SET expires_at=${DateTime.formatIso(DateTime.makeUnsafe(expiry))} WHERE environment_id=${b.environmentId}`;
                const result = yield* nativeCall(a, parent, "schedule_task", {
                  environmentId: b.environmentId,
                  projectId: b.project.id,
                  purposeIntent: { version: 1, intent: { type: "unscoped" } },
                  prompt: "Isolated later deadline registration",
                  bindToCurrentThread: false,
                  schedule: { type: "interval", everyMs: 60_000 },
                  clientRequestId: `r6-${variant}-${key}`,
                });
                expect(result.isError, json(result)).not.toBe(true);
              }),
            );
          if (variant !== "late") {
            yield* create("late", late);
            yield* Deferred.await(lateArmed);
          }
          yield* create("early", early);
          yield* Deferred.await(earlyArmed);
          expect(attempts).toBe(0);
          if (variant === "simultaneous")
            yield* Effect.all([TestClock.setTime(early + 1), events.automationChanged], {
              concurrency: 2,
            });
          else yield* TestClock.setTime(early + 1);
          yield* Deferred.await(selected);
          while (
            (yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE expires_at=${early} AND ack_json IS NULL`)
              .length !== 0
          )
            yield* PubSub.take(changes);
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(1);
          if (variant !== "late") {
            expect(
              yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE expires_at=${late} AND pending=0 AND ack_json IS NULL`,
            ).toHaveLength(1);
            if (variant === "earlier") {
              yield* TestClock.adjust("2 seconds");
              yield* Deferred.await(lateRearmed);
              const remaining = (yield* sourceSql<{
                dependency_json: string;
                next_generation: string;
              }>`SELECT dependency_json,next_generation FROM fork_automation_obligations WHERE ack_json IS NULL`)[0]!;
              const dependency = decodeFixtureDependency(remaining.dependency_json);
              const original = yield* a.links.resolveAutomationRecovery!(
                b.environmentId,
                remaining.next_generation,
              );
              const http = yield* HttpClient.HttpClient;
              const response = yield* TestClock.withLive(
                http.execute(
                  HttpClientRequest.post(
                    `${original.url}/mcp/peer-control/automation-disable`,
                  ).pipe(
                    HttpClientRequest.setHeader("authorization", `Bearer ${original.token}`),
                    HttpClientRequest.setBody(
                      HttpBody.text(json({ version: 1, dependency }), "application/json"),
                    ),
                  ),
                ),
              );
              expect(response.status).toBe(200);
              const ack = yield* decodeAutomationAck(yield* response.text);
              cancelling = true;
              yield* a.authority.acknowledgeAutomationDisable(
                ack,
                b.environmentId,
                remaining.next_generation,
              );
              yield* Deferred.await(cancelled);
            }
            yield* TestClock.setTime(late + 1);
            while (
              (yield* sourceSql`SELECT 1 FROM fork_automation_obligations WHERE ack_json IS NULL`)
                .length !== 0
            )
              yield* PubSub.take(changes);
          }
          expect(attempts).toBe(variant === "simultaneous" ? 2 : 1);
          yield* TestClock.adjust("2 seconds");
          const completeAttempts = attempts;
          yield* TestClock.adjust("2 hours");
          expect(attempts).toBe(completeAttempts);
          expect(yield* a.authority.nextAutomationDisableDeadline).toBeNull();
          expect(yield* destinationSql`SELECT 1 FROM fork_automation_disable_acks`).toHaveLength(
            variant === "late" ? 1 : 2,
          );
        }).pipe(Effect.provide(TestClock.layer()));
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, ServerSettings.layerTest()),
      ),
    ),
);
