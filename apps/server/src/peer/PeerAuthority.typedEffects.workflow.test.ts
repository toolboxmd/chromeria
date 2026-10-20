import { nativeRpcCall, sendResult, native383RpcGroups } from "./PeerAuthority.workflow.fixture.ts";
import {
  instanceId,
  selection,
  makeNode,
  decodeTyped383Open,
  typed383Digest,
  decodeTyped383Command,
  decodeTyped383Upload,
  decodeTyped383Artifact,
  typed383Pair,
  activate,
  seedParent,
  caller,
  json,
  decodeLaunchReceipt,
  nativeCall,
  decodePendingWsReply,
  decodeDeviceViewerAccess,
  decodeProducerCounter,
  decodeProducerClickCount,
  decodeProducerInput,
  createPreviewStreamClient,
  browserExternalContexts,
  makeNativePromiseBridge,
  idleResource383Pair,
  backgroundRoster383,
  localBackground383,
  stageInput383,
  browserExternalBoundary,
  assertTyped383SourceIdle,
  decodeAutomationSpectrumResult,
  decodeManualRun,
  decodeSavedPngReceipt,
  decodeSend,
  decodeTyped383Example,
  decodeTyped383Target,
  nativeInvalidParams,
} from "./PeerAuthority.workflow.fixture.ts";
import { WsRpcGroup } from "@t3tools/contracts";
import * as Tracer from "effect/Tracer";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as Path from "effect/Path";
import { agentDeviceConfigPath } from "../device/AgentDeviceTarget.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "../device/DeviceToolchain.ts";
import {
  LOCAL_DEVICE_HOST_ID,
  DeviceId,
  PeerAttachmentReference,
  CommandId,
  MessageId,
  ThreadId,
  DeviceViewerAccess,
  PreviewViewerResource,
  NativeBrowserDownloadAssetResource,
  PeerAttachmentAssetResource,
  PreviewViewerAccess,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2CommandOutputFrame,
  type OrchestrationV2TurnItem,
  PreviewStreamInput,
  type PreviewViewportSetting,
  DeviceViewerClientRpcGroup,
  WS_METHODS,
  OrchestratorMcpFailure,
  EventId,
  ProviderInstanceId,
  TurnItemId,
} from "@t3tools/contracts";
import * as AttachmentUpload from "../assets/AttachmentUpload.ts";
import { resolveAttachmentPathById, resolveAttachmentPath } from "../attachmentStore.ts";
import * as Fiber from "effect/Fiber";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";
import * as FileSystem from "effect/FileSystem";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpBody from "effect/http/HttpBody";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as Settings from "../serverSettings.ts";

import * as TestClock from "effect/testing/TestClock";
// @effect-diagnostics nodeBuiltinImport:off
// Retain the original native fixture directive while moving its socket controls.

import * as NodeModule from "node:module";
import * as NodeURL from "node:url";

import * as PubSub from "effect/PubSub";
import { compactDynamicToolOutput, resourcesFromToolItem } from "@t3tools/shared/toolOutput";
import { projectThreadProjectionForWire } from "../orchestration-v2/WireProjection.ts";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import * as DeviceHubProxy from "../device/DeviceHubProxy.ts";

import * as Auth from "../auth/EnvironmentAuth.ts";

import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { RpcServer, RpcSerialization } from "effect/rpc";

import * as AuthSessions from "../persistence/AuthSessions.ts";

import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Queue from "effect/Queue";

import * as NativeBrowser from "../preview/ServerBrowser.ts";

import {
  makeExternalBrowserContext,
  makeExternalBrowserDownload,
  makeExternalBrowserFileChooser,
} from "../preview/testing/BrowserExternal.fixture.ts";

import { vi } from "vite-plus/test";

import * as Clock from "effect/Clock";

import { NodeHttpServer } from "@effect/platform-node";

import * as DateTime from "effect/DateTime";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";

const { CoreServerWsRpcGroup, OrchestrationServerWsRpcGroup, ServerWsRpcGroup, ForkWsRpc } =
  native383RpcGroups;

it.live(
  "typed383-core: registered two-peer loop preserves private binding, PNG bytes and exact historical receipts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open, footprint } =
          yield* typed383Pair("typed383-core");
        const discovery = yield* nativeCall(a, parent, "device_commands", route);
        expect(discovery.isError, json(discovery)).not.toBe(true);
        expect(discovery.structuredContent).toMatchObject({
          protocol: 1,
          agentDeviceVersion: AGENT_DEVICE_VERSION,
          deviceHubVersion: DEVICE_HUB_VERSION,
          commands: expect.arrayContaining([
            "open",
            "snapshot",
            "click",
            "fill",
            "screenshot",
            "close",
          ]),
        });
        const opened = yield* open("typed383-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target, quickStart } = yield* decodeTyped383Open(opened.structuredContent);
        expect(target).toMatchObject({
          hostId: "local",
          deviceId: "fixture-device382",
          toolSessionId: expect.stringMatching(/^[0-9a-f-]{36}$/),
        });
        expect(quickStart).toContain("device_commands");
        expect(quickStart).toContain("device_execute");
        for (const privateValue of [
          "agentDevice",
          "--config",
          "ISOLATED_DEVICE_TOKEN_382",
          b.config.stateDir,
        ])
          expect(json(opened.structuredContent)).not.toContain(privateValue);
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        expect(
          (yield* drive("typed383-app", { type: "open", app: "com.example.app" }))
            .structuredContent,
        ).toEqual({ type: "action", target, status: "dispatched" });
        const tree = yield* drive("typed383-tree", {
          type: "snapshot",
          interactiveOnly: true,
          depth: 8,
        });
        expect(tree.structuredContent).toEqual({
          type: "snapshot",
          target,
          nodes: [{ index: 1, ref: "e1", role: "button", label: "Continue" }],
          truncated: false,
        });
        expect((yield* drive("typed383-click", { type: "click", ref: "@e1" })).isError).not.toBe(
          true,
        );
        expect(
          (yield* drive("typed383-fill", { type: "fill", ref: "@e1", text: "--config /evil\n" }))
            .isError,
        ).not.toBe(true);
        const fs = yield* FileSystem.FileSystem;
        const sessions = new Set<string>();
        for (const call of b.deviceProcessCalls) {
          expect(call.command).not.toBe("/remote-host/node-must-never-execute");
          expect(call.args[0]).toBe(
            `${b.config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}/node_modules/agent-device/bin/agent-device.mjs`,
          );
          const separator = call.args.indexOf("--");
          expect(separator).toBeGreaterThan(1);
          const flags = call.args.slice(0, separator);
          for (const flag of ["--json", "--platform", "--serial", "--config", "--session"])
            expect(flags).toContain(flag);
          expect(flags[flags.indexOf("--platform") + 1]).toBe("android");
          expect(flags[flags.indexOf("--serial") + 1]).toBe("fixture-device382");
          const config = flags[flags.indexOf("--config") + 1]!;
          expect(config).toBe(`${b.config.stateDir}/device/sessions/${target.toolSessionId}.json`);
          expect(yield* fs.readFileString(config)).toContain("ISOLATED_DEVICE_TOKEN_382");
          sessions.add(flags[flags.indexOf("--session") + 1]!);
          if (call.args[1] === "fill") {
            expect(call.stdin).toBe("--config /evil\n\r\n");
            expect(call.args.slice(separator + 1)).toEqual(["@e1"]);
          }
        }
        expect(sessions.size).toBe(1);
        const pngInput = {
          ...route,
          target,
          command: { type: "screenshot" },
          clientRequestId: "typed383-png",
        };
        yield* Ref.set(a.dropCommittedToolReply, "device_execute");
        const lost = yield* nativeCall(a, parent, "device_execute", pngInput);
        expect(lost.isError).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        const beforeReplay = yield* footprint();
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const image = yield* nativeCall(a, parent, "device_execute", pngInput);
        expect(image.isError, json(image)).not.toBe(true);
        const { artifact } = yield* decodeTyped383Artifact(image.structuredContent);
        const png = Buffer.from(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
          "base64",
        );
        const digest = yield* typed383Digest(png);
        expect(artifact).toMatchObject({
          environmentId: b.environmentId,
          mimeType: "image/png",
          sizeBytes: png.length,
          digest,
        });
        const file = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: artifact.attachmentId,
        });
        expect(file).not.toBeNull();
        expect(Array.from(yield* fs.readFile(file!))).toEqual(Array.from(png));
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          owner: string;
          digest: string;
          expected_digest: string;
          manifest: string;
        }>`SELECT owner,digest,expected_digest,manifest FROM attachment_staging WHERE id=${artifact.attachmentId}`;
        expect(rows).toMatchObject([{ owner: artifact.owner, digest, expected_digest: digest }]);
        expect(rows[0]!.manifest).toContain("image/png");
        expect(rows[0]!.manifest).toContain(String(png.length));
        expect(
          (yield* nativeCall(a, parent, "device_execute", pngInput)).structuredContent,
        ).toEqual(image.structuredContent);
        expect(yield* footprint()).toEqual(beforeReplay);
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(1);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "ISOLATED_DEVICE_TOKEN_382",
        );
        const close = {
          ...route,
          target,
          command: { type: "close" },
          clientRequestId: "typed383-close",
        };
        const closed = yield* nativeCall(a, parent, "device_execute", close);
        expect(closed.structuredContent).toEqual({ type: "closed", target });
        expect(yield* b.fixtureDevice!.sessionsForThread(child)).toHaveLength(0);
        const reopened = yield* decodeTyped383Open(
          (yield* open("typed383-reopen")).structuredContent,
        );
        expect(reopened.target.toolSessionId).not.toBe(target.toolSessionId);
        const stable = yield* footprint();
        expect((yield* nativeCall(a, parent, "device_execute", close)).structuredContent).toEqual(
          closed.structuredContent,
        );
        expect((yield* drive("typed383-stale", { type: "click", ref: "@e1" })).isError).toBe(true);
        expect(yield* footprint()).toEqual(stable);
        expect((yield* b.fixtureDevice!.sessionsForThread(child))[0]?.toolSessionId).toBe(
          reopened.target.toolSessionId,
        );
        const fresh = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target: reopened.target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-reopened-click",
        });
        expect(fresh.isError, json(fresh)).not.toBe(true);
        const last = b.deviceProcessCalls.at(-1)!;
        expect(last.args[last.args.indexOf("--session") + 1]).not.toBe([...sessions][0]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-guidance: an A-only caller executes the returned snapshot example unchanged",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, open } = yield* typed383Pair("typed383-guidance");
        const opened = yield* open("typed383-guidance-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target, quickStart } = yield* decodeTyped383Open(opened.structuredContent);
        const discoveryExample = quickStart
          .split("\n")
          .find((line) => line.startsWith("device_commands "));
        expect(discoveryExample).toBeDefined();
        const discoveryArgs = yield* decodeTyped383Example(
          discoveryExample!.slice("device_commands ".length),
        );
        const discovery = yield* nativeCall(a, parent, "device_commands", discoveryArgs);
        expect(discovery.isError, json(discovery)).not.toBe(true);
        expect(discovery.structuredContent).toMatchObject({ protocol: 1, hostId: "local" });
        const example = quickStart
          .split("\n")
          .find((line) => line.startsWith("device_execute ") && line.includes('"type":"snapshot"'));
        expect(example).toBeDefined();
        const args = yield* decodeTyped383Example(example!.slice("device_execute ".length));
        // No caller-supplied routing repair: follow the exact returned public example on A.
        const result = yield* nativeCall(a, parent, "device_execute", args);
        expect(result.isError, json(result)).not.toBe(true);
        expect(result.structuredContent).toEqual({
          type: "snapshot",
          target,
          nodes: [{ index: 1, ref: "e1", role: "button", label: "Continue" }],
          truncated: false,
        });
        expect(b.deviceProcessCalls.filter((call) => call.args.includes("snapshot"))).toHaveLength(
          1,
        );
        yield* assertTyped383SourceIdle(a);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["device_open", "device_execute"] as const)(
  "typed383-preflight: %s rechecks the original grant after host inspection before replay or fresh effects",
  (operation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair(
          `typed383-${operation}-preflight`,
        );
        const opened = yield* open("preflight-opening");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const invoke = (key: string) =>
          operation === "device_open"
            ? open(key)
            : nativeCall(a, parent, "device_execute", {
                ...route,
                target,
                command: { type: "snapshot" },
                clientRequestId: key,
              });
        const key = operation === "device_open" ? "preflight-opening" : "preflight-snapshot";
        const completed = yield* invoke(key);
        expect(completed.isError, json(completed)).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        const armed = yield* Ref.make(false);
        yield* Ref.set(
          b.deviceSummaryHook,
          Effect.gen(function* () {
            if ((yield* RequestReceipts.CurrentOperation)?.tool !== operation) return;
            if (!(yield* Ref.getAndSet(armed, false))) return;
            // External host inspection completes only after the real grant changes.
            yield* b.authority.setGrant({ ...grant, device: false }).pipe(Effect.orDie);
          }),
        );
        for (const nextKey of [key, `${key}-fresh`]) {
          const before = yield* footprint();
          yield* Ref.set(armed, true);
          const denied = yield* invoke(nextKey);
          expect(denied.isError, json(denied)).toBe(true);
          expect(yield* Ref.get(armed)).toBe(false);
          expect(yield* footprint()).toEqual(before);
          yield* b.authority.setGrant({ ...grant, device: true });
          expect((yield* invoke(key)).structuredContent).toEqual(completed.structuredContent);
          expect(yield* footprint()).toEqual(before);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: pure consent retries while actual config persistence remains unknown across transport reconstruction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-consent");
        yield* b.fixtureSettings.updateSettings({ enableAgentDeviceAccess: false });
        const untouched = yield* footprint();
        const denied = yield* open("typed383-consent-open");
        expect(denied.isError).toBe(true);
        expect(json(denied)).not.toContain("outcome_unknown");
        expect(yield* footprint()).toEqual(untouched);
        yield* b.fixtureSettings.updateSettings({ enableAgentDeviceAccess: true });
        expect((yield* open("typed383-consent-open")).isError).not.toBe(true);
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-current-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-config-unknown",
        };
        const before = yield* footprint();
        yield* Ref.set(b.loseDeviceConfiguration, true);
        const unknown = yield* nativeCall(a, parent, "device_execute", input);
        expect(unknown.isError).toBe(true);
        expect(json(unknown)).toContain("outcome_unknown");
        const accepted = yield* footprint();
        expect(accepted.config).toBeGreaterThan(before.config);
        expect(accepted.process).toBe(before.process);
        expect(accepted.configuration).toContain("ISOLATED_DEVICE_TOKEN_382");
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        expect(json(yield* nativeCall(a, parent, "device_execute", input))).toContain(
          "outcome_unknown",
        );
        expect(yield* footprint()).toEqual(accepted);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: current actor mode contributor host device and generation deny before leaves on fresh and replay keys",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-authority");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-authority-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-authority-click",
        };
        expect((yield* nativeCall(a, parent, "device_execute", input)).isError).not.toBe(true);
        const stable = yield* footprint();
        const rival = ThreadId.make("typed383-rival");
        yield* seedParent(a, rival);
        for (const key of [input.clientRequestId, "typed383-rival-fresh"])
          expect(
            (yield* nativeCall(a, rival, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        for (const changed of [
          { hostId: "foreign-host" },
          { deviceId: "foreign-device" },
          { toolSessionId: "00000000-0000-4000-8000-000000000000" },
        ])
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...input,
              target: { ...target, ...changed },
              clientRequestId: `typed383-target-${Object.keys(changed)[0]}`,
            })).isError,
          ).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("typed383-plan"),
          threadId: parent,
          runtimeMode: "auto",
        });
        for (const key of [input.clientRequestId, "typed383-plan-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("typed383-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.auth.revokeSession(grant.sessionId);
        for (const key of [input.clientRequestId, "typed383-revoked-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...input, clientRequestId: key }))
              .isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(stable);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-wait: original source mode changes while the native command lock is held deny queued effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const queued = yield* Deferred.make<void>();
        let invocations = 0;
        const tracer = Tracer.make({
          span: (options) => {
            if (options.name === "DeviceService.executeCommand" && ++invocations === 2)
              Deferred.doneUnsafe(queued, Effect.void);
            return Tracer.nativeTracer.span(options);
          },
        });
        yield* Effect.gen(function* () {
          const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-lock");
          const { target } = yield* decodeTyped383Open(
            (yield* open("lock-open")).structuredContent,
          );
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.set(b.deviceProcessHook, (call) =>
            call.args[1] === "click"
              ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
          );
          const first = yield* nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "lock-click",
          }).pipe(Effect.forkChild);
          yield* Deferred.await(held);
          const input = {
            ...route,
            target,
            command: { type: "fill", ref: "@e1", text: "queued" },
            clientRequestId: "lock-fill",
          };
          const second = yield* nativeCall(a, parent, "device_execute", input).pipe(
            Effect.forkChild,
          );
          // Observe the real service span BEFORE its actual semaphore acquisition.
          // No semaphore, service, authority or policy method is mocked or injected.
          yield* Deferred.await(queued);
          const before = yield* footprint();
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("lock-source-plan"),
            threadId: parent,
            runtimeMode: "auto",
          });
          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(first)).isError).not.toBe(true);
          const denied = yield* Fiber.join(second);
          expect(denied.isError, json(denied)).toBe(true);
          expect(yield* footprint()).toEqual(before);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
          yield* a.threads.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("lock-source-restore"),
            threadId: parent,
            runtimeMode: "full-access",
          });
          expect((yield* nativeCall(a, parent, "device_execute", input)).isError).not.toBe(true);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(1);
        }).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-media: native batch input media claims and reconstruction preserve exact bytes and caller ownership",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-media");
        const { target } = yield* decodeTyped383Open((yield* open("media-open")).structuredContent);
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        const batch = {
          type: "batch",
          steps: [
            { type: "snapshot", interactiveOnly: true },
            { type: "click", ref: "@e1" },
            { type: "fill", ref: "@e1", text: "--session rival\n" },
          ],
        };
        const first = yield* drive("media-loop", batch);
        expect(first.isError, json(first)).not.toBe(true);
        expect(first.structuredContent).toMatchObject({
          type: "batch",
          target,
          results: [
            { type: "snapshot", nodes: [{ ref: "e1" }] },
            { status: "dispatched" },
            { status: "dispatched" },
          ],
        });
        const calls = b.deviceProcessCalls.length;
        expect((yield* drive("media-loop", batch)).structuredContent).toEqual(
          first.structuredContent,
        );
        expect(b.deviceProcessCalls).toHaveLength(calls);
        const beforeInvalid = yield* footprint();
        for (const steps of [
          [
            { type: "click", ref: "@e1" },
            { type: "fill", ref: "@e1", text: "safe", daemonAuthToken: "borrowed" },
          ],
          [{ type: "batch", steps: [{ type: "click", ref: "@e1" }] }],
        ]) {
          const raw = yield* nativeInvalidParams(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "batch", steps },
            clientRequestId: "invalid-native-batch",
          });
          expect(raw).toMatchObject({ jsonrpc: "2.0", id: 1, error: { code: -32602 } });
        }
        expect(yield* footprint()).toEqual(beforeInvalid);
        const fs = yield* FileSystem.FileSystem;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        for (const type of ["record", "trace"] as const) {
          expect((yield* drive(`${type}-start`, { type, action: "start" })).isError).not.toBe(true);
          if (type === "record") {
            yield* Ref.set(a.dropCommittedToolReply, "device_execute");
            const lost = yield* drive(`${type}-stop`, { type, action: "stop" });
            expect(lost.isError).toBe(true);
            expect(json(lost)).toContain("Isolated committed tool reply loss");
            const count = b.deviceProcessCalls.length;
            const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
            yield* Context.get(
              a.database,
              SqlClient.SqlClient,
            )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
            const replay = yield* drive(`${type}-stop`, { type, action: "stop" });
            expect(replay.isError, json(replay)).not.toBe(true);
            expect(b.deviceProcessCalls).toHaveLength(count);
          }
          const stopped = yield* drive(`${type}-stop`, { type, action: "stop" });
          expect(stopped.isError, json(stopped)).not.toBe(true);
          const result = yield* decodeTyped383Command(stopped.structuredContent);
          if (result.type !== "artifacts") throw new Error("Expected native media references");
          expect(result.recording).toEqual({
            durationMs: 200,
            capturedDurationMs: 150,
            recorder: "unconfirmed",
            nativePathDisposition: "pending",
          });
          const reference = result.artifacts[0]!;
          const file = resolveAttachmentPathById({
            attachmentsDir: b.config.attachmentsDir,
            attachmentId: reference.attachmentId,
          })!;
          const bytes = yield* fs.readFile(file);
          const expected =
            type === "record"
              ? new Uint8Array([
                  0, 0, 0, 0, 0x66, 0x74, 0x79, 0x70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  0,
                ])
              : new TextEncoder().encode("trace [private runtime] [private runtime]");
          expect(Array.from(bytes)).toEqual(Array.from(expected));
          expect(reference).toMatchObject({
            environmentId: b.environmentId,
            digest: yield* typed383Digest(expected),
            sizeBytes: expected.length,
          });
          const rows = yield* sql<{
            digest: string;
            expected_digest: string;
            manifest: string;
            consumer: string | null;
          }>`SELECT digest,expected_digest,manifest,consumer FROM attachment_staging WHERE id=${reference.attachmentId}`;
          expect(rows[0]).toMatchObject({
            digest: reference.digest,
            expected_digest: reference.digest,
            consumer: null,
          });
          expect(JSON.parse(rows[0]!.manifest)).toMatchObject({
            sizeBytes: expected.length,
            name: reference.name,
            mimeType: reference.mimeType,
          });
          expect(json(stopped)).not.toMatch(
            /ISOLATED_DEVICE_TOKEN|device-agent382|outPath|localPath/,
          );
          const stable = yield* footprint();
          expect(
            (yield* drive(`${type}-stop`, { type, action: "stop" })).structuredContent,
          ).toEqual(stopped.structuredContent);
          expect((yield* drive(`${type}-fresh-stop`, { type, action: "stop" })).isError).toBe(true);
          expect(yield* footprint()).toEqual(stable);
        }
        // Public upload + real native staging. The qualified reference is read from
        // its existing owner using an observed, genuinely verified invocation.
        const metadata = {
          type: "file" as const,
          name: "app.apk",
          mimeType: "application/vnd.android.package-archive",
          sizeBytes: 4,
        };
        const packageBytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const digest = yield* typed383Digest(packageBytes);
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          ...route,
          upload: metadata,
          digest,
          clientRequestId: "media-package-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(packageBytes)),
            ),
          )).status,
        ).toBe(204);
        const observed = Option.getOrThrow(yield* Ref.get(b.deviceObservedScope));
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(b.requestContext)),
          ),
        );
        const nativeReference = yield* staging.verify(observed, {
          ...metadata,
          id: issued.attachmentId,
        });
        const reference = nativeReference.reference;
        const beforePackages = yield* footprint();
        for (const [index, artifact] of [
          { ...reference, environmentId: a.environmentId },
          { ...reference, owner: "0".repeat(64) },
          { ...reference, digest: "0".repeat(64) },
          { ...reference, name: "borrowed.apk" },
        ].entries())
          expect(
            (yield* drive(`package-denied-${index}`, {
              type: "install",
              artifact,
              app: "com.example.app",
            })).isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(beforePackages);
        const command = { type: "install", artifact: reference, app: "com.example.app" };
        const installed = yield* drive("package-install", command);
        expect(installed.isError, json(installed)).not.toBe(true);
        const leaf = b.deviceProcessCalls.at(-1)!;
        expect(leaf.args.slice(leaf.args.indexOf("--") + 1)).toEqual([
          "com.example.app",
          nativeReference.path,
        ]);
        expect(json(installed)).not.toContain(nativeReference.path);
        const afterInstall = yield* footprint();
        expect((yield* drive("package-install", command)).structuredContent).toEqual(
          installed.structuredContent,
        );
        yield* fs.writeFile(nativeReference.path, new Uint8Array([1, 2, 3, 4]));
        expect((yield* drive("package-mutated", command)).isError).toBe(true);
        expect(yield* footprint()).toEqual(afterInstall);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-media: producer escape and changed package bytes remain unknown without publication or repeated effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } =
          yield* typed383Pair("typed383-media-denial");
        const { target } = yield* decodeTyped383Open(
          (yield* open("escape-open")).structuredContent,
        );
        const drive = (key: string, command: unknown) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command,
            clientRequestId: key,
          });
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const beforeRows = yield* sql`SELECT * FROM attachment_staging`;
        expect((yield* drive("escape-start", { type: "trace", action: "start" })).isError).not.toBe(
          true,
        );
        yield* Ref.set(b.deviceMediaOutside, true);
        const escaped = yield* drive("escape-stop", { type: "trace", action: "stop" });
        expect(escaped.isError).toBe(true);
        expect(json(escaped)).toContain("outcome_unknown");
        expect(yield* sql`SELECT * FROM attachment_staging`).toEqual(beforeRows);
        const afterEscape = yield* footprint();
        yield* Ref.set(b.deviceMediaOutside, false);
        expect(json(yield* drive("escape-stop", { type: "trace", action: "stop" }))).toContain(
          "outcome_unknown",
        );
        expect(yield* footprint()).toEqual(afterEscape);
        const fs = yield* FileSystem.FileSystem;
        for (const file of b.deviceMediaPaths.values()) expect(yield* fs.exists(file)).toBe(true);
        const metadata = {
          type: "file" as const,
          name: "app.apk",
          mimeType: "application/vnd.android.package-archive",
          sizeBytes: 4,
        };
        const bytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const digest = yield* typed383Digest(bytes);
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          ...route,
          upload: metadata,
          digest,
          clientRequestId: "race-package-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const scope = Option.getOrThrow(yield* Ref.get(b.deviceObservedScope));
        const staging = Option.getOrThrow(
          yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging).pipe(
            Effect.provide(Layer.succeedContext(b.requestContext)),
          ),
        );
        const verified = yield* staging.verify(scope, { ...metadata, id: issued.attachmentId });
        const input = { type: "install", app: "com.example.app", artifact: verified.reference };
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "capabilities"
            ? fs.writeFile(verified.path, new Uint8Array([4, 3, 2, 1])).pipe(Effect.orDie)
            : Effect.void,
        );
        const changed = yield* drive("race-package-install", input);
        expect(changed.isError).toBe(true);
        expect(json(changed)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "install")).toHaveLength(0);
        const afterChange = yield* footprint();
        yield* fs.writeFile(verified.path, bytes);
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        expect(json(yield* drive("race-package-install", input))).toContain("outcome_unknown");
        expect(yield* footprint()).toEqual(afterChange);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-negotiation: requests-v1 with actual old device domain refuses before destination preparation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { open, footprint } = yield* typed383Pair("typed383-legacy-domain", true);
        const untouched = yield* footprint();
        const response = yield* open("legacy-domain-open");
        expect(response.isError, json(response)).toBe(true);
        expect(json(response)).not.toContain("--config");
        expect(json(response)).not.toContain("agent-device.mjs");
        expect(yield* footprint()).toEqual(untouched);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-effects: native grant retirement after click prevents fill and uncertain replay never repeats work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair("typed383-retire");
        const opened = yield* open("retire-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Target(opened.structuredContent);
        const grant = (yield* b.authority.grants)[0]!;
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? b.authority.setGrant({ ...grant, device: false }).pipe(Effect.asVoid, Effect.orDie)
            : Effect.void,
        );
        const input = {
          ...route,
          target,
          command: {
            type: "batch",
            steps: [
              { type: "click", ref: "@e1" },
              { type: "fill", ref: "@e1", text: "must never dispatch" },
            ],
          },
          clientRequestId: "retire-batch",
        };
        const failed = yield* nativeCall(a, parent, "device_execute", input);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(1);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
        yield* b.authority.setGrant({ ...grant, device: true });
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        const before = b.deviceProcessCalls.length;
        const replay = yield* nativeCall(a, parent, "device_execute", input);
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls).toHaveLength(before);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-source-policy: actual originating project policy retirement prevents the next batch effect",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open } = yield* typed383Pair(
          "typed383-source-policy",
          false,
          true,
        );
        const opened = yield* open("source-policy-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const original = yield* a.fixtureSettings.getSettings;
        yield* Ref.set(b.deviceProcessHook, (call) =>
          call.args[1] === "click"
            ? a.fixtureSettings
                .updateSettings({
                  projectSettingsOverrides: {
                    ...original.projectSettingsOverrides,
                    [a.project.id]: { enableAgentDeviceAccess: false },
                  },
                })
                .pipe(Effect.asVoid, Effect.orDie)
            : Effect.void,
        );
        const input = {
          ...route,
          target,
          clientRequestId: "source-policy-batch",
          command: {
            type: "batch",
            steps: [
              { type: "click", ref: "@e1" },
              { type: "fill", ref: "@e1", text: "must not dispatch" },
            ],
          },
        };
        const denied = yield* nativeCall(a, parent, "device_execute", input);
        expect(denied.isError, json(denied)).toBe(true);
        expect(json(denied)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(1);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "fill")).toHaveLength(0);
        expect(
          (yield* a.fixtureSettings.getSettings).projectSettingsOverrides[a.project.id],
        ).toMatchObject({ enableAgentDeviceAccess: false });
        yield* a.fixtureSettings.updateSettings({
          projectSettingsOverrides: original.projectSettingsOverrides,
        });
        yield* Ref.set(b.deviceProcessHook, () => Effect.void);
        const before = b.deviceProcessCalls.length;
        const replay = yield* nativeCall(a, parent, "device_execute", input);
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(b.deviceProcessCalls).toHaveLength(before);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: wrong installed runtime or missing pin refuses without preparation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-version");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-version-open")).structuredContent,
        )).target;
        const stable = yield* footprint();
        yield* Ref.set(b.deviceOldVersion, true);
        expect((yield* nativeCall(a, parent, "device_commands", route)).isError).toBe(true);
        expect(
          (yield* nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "typed383-old-version",
          })).isError,
        ).toBe(true);
        expect(yield* footprint()).toEqual(stable);
        yield* Ref.set(b.deviceOldVersion, false);
        const fs = yield* FileSystem.FileSystem;
        const sentinel = `${b.config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}/.install-complete`;
        yield* fs.remove(sentinel);
        expect((yield* nativeCall(a, parent, "device_commands", route)).isError).toBe(true);
        expect((yield* open("typed383-missing-pin")).isError).toBe(true);
        expect(yield* footprint()).toEqual(stable);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: closed inputs and actual capabilities cannot redirect or dispatch an unsupported action",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } = yield* typed383Pair("typed383-boundary");
        const target = (yield* decodeTyped383Open(
          (yield* open("typed383-boundary-open")).structuredContent,
        )).target;
        const input = {
          ...route,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-invalid",
        };
        const stable = yield* footprint();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const receipts = yield* sql`SELECT id,state FROM mcp_request_receipts ORDER BY id`;
        for (const command of [
          { type: "click", ref: "@e1", config: "/evil" },
          { type: "fill", ref: "@e1", text: "x", session: "evil" },
          { type: "snapshot", argv: ["--config", "/evil"] },
          { type: "click", x: NaN, y: Infinity },
          { type: "close", token: "evil" },
        ]) {
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", { ...input, command })).error
              .code,
          ).toBe(-32602);
        }
        for (const extra of [
          { session: "evil" },
          { config: "/evil" },
          { token: "evil" },
          { argv: [] },
          { daemon: true },
        ])
          expect(
            (yield* nativeInvalidParams(a, parent, "device_execute", { ...input, ...extra })).error
              .code,
          ).toBe(-32602);
        expect(yield* footprint()).toEqual(stable);
        expect(yield* sql`SELECT id,state FROM mcp_request_receipts ORDER BY id`).toEqual(receipts);
        const supported = yield* Ref.get(b.deviceCapabilities);
        for (const [index, device] of [
          { ...supported.device, id: "rival" },
          { ...supported.device, platform: "ios" },
          { ...supported.device, kind: "physical" },
        ].entries()) {
          yield* Ref.set(b.deviceCapabilities, { ...supported, device });
          const before = b.deviceProcessCalls.filter((call) => call.args[1] === "click").length;
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...input,
              clientRequestId: `typed383-capability-${index}`,
            })).isError,
          ).toBe(true);
          expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(
            before,
          );
        }
        yield* Ref.set(b.deviceCapabilities, { ...supported, availableCommands: ["snapshot"] });
        const before = b.deviceProcessCalls.filter((call) => call.args[1] === "click").length;
        expect(
          (yield* nativeCall(a, parent, "device_execute", {
            ...input,
            clientRequestId: "typed383-missing-command",
          })).isError,
        ).toBe(true);
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(
          before,
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: an old peer without durable requests cannot fall back to a destination launcher",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("typed383-old-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, deviceOwner: true },
        );
        yield* Effect.addFinalizer(() => assertTyped383SourceIdle(a));
        const b = yield* makeNode(
          descriptorOf("typed383-old-b", "B", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("typed383-old-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Old peer",
          message: "Wait",
          modelSelection: selection,
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, device: true });
        const sessions = yield* b.fixtureDevice!.sessionsForThread(child);
        // Launch and pair were accepted by the modern owner. Reconstruct the actual
        // same-database transport without request/staging owners before the device call.
        // No auth/session/grant/pair is fabricated or weakened.
        const old = { ...b, ...(yield* b.restartWithoutRequestsTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([old.url])} WHERE environment_id=${b.environmentId}`;
        const unsupported = yield* Context.get(a.services, PeerMcpClient.PeerMcpClient)
          .requireRequestProtocol!(b.environmentId).pipe(Effect.result);
        expect(unsupported._tag).toBe("Failure");
        const result = yield* nativeCall(a, parent, "device_open", {
          environmentId: b.environmentId,
          contextThreadId: child,
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "typed383-old-peer-open",
        });
        expect(result.isError, json(result)).toBe(true);
        expect(yield* Ref.get(b.deviceHostStarts)).toBe(0);
        expect(yield* Ref.get(b.deviceAgentStarts)).toBe(0);
        expect(yield* Ref.get(b.deviceConfigurations)).toBe(0);
        expect(b.deviceProcessCalls).toHaveLength(0);
        expect(yield* b.fixtureDevice!.sessionsForThread(child)).toEqual(sessions);
        expect(json(result)).not.toContain("agent-device.mjs");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "typed383-core: actual unlink and replacement grant fence old openings while a lawful new child can open",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open, footprint } =
          yield* typed383Pair("typed383-generation");
        const oldTarget = (yield* decodeTyped383Open(
          (yield* open("typed383-generation-open")).structuredContent,
        )).target;
        const original = {
          ...route,
          target: oldTarget,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-generation-click",
        };
        expect((yield* nativeCall(a, parent, "device_execute", original)).isError).not.toBe(true);
        const oldLink = yield* a.links.resolve(b.environmentId);
        const oldGrant = (yield* b.authority.grants)[0]!;
        const before = yield* footprint();
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const currentLink = yield* a.links.resolve(b.environmentId);
        expect(currentLink.link.launchIdentity).not.toBe(oldLink.link.launchIdentity);
        const enrolled = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(enrolled.isError, json(enrolled)).not.toBe(true);
        const replacementGrant = (yield* b.authority.grants).find(
          (grant) => grant.sessionId !== oldGrant.sessionId,
        );
        if (replacementGrant === undefined)
          return yield* Effect.die("Expected a real newly paired grant");
        yield* b.authority.setGrant({ ...replacementGrant, device: true });
        // The old exact context and old completed action must fail under current link authority.
        for (const key of [original.clientRequestId, "typed383-generation-old-fresh"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", { ...original, clientRequestId: key }))
              .isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(before);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Replacement generation",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "typed383-generation-child-new",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const newChild = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        expect(newChild).not.toBe(child);
        yield* activate(b, newChild);
        const replacementRoute = { environmentId: b.environmentId, contextThreadId: newChild };
        // A current lawful context cannot claim the old opening, even with the old key.
        for (const key of [original.clientRequestId, "typed383-generation-new-context-old-target"])
          expect(
            (yield* nativeCall(a, parent, "device_execute", {
              ...original,
              ...replacementRoute,
              clientRequestId: key,
            })).isError,
          ).toBe(true);
        expect(yield* footprint()).toEqual(before);
        expect(yield* b.fixtureDevice!.sessionsForThread(newChild)).toHaveLength(0);
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...replacementRoute,
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "typed383-generation-legitimate-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const target = (yield* decodeTyped383Open(opened.structuredContent)).target;
        expect(target.toolSessionId).not.toBe(oldTarget.toolSessionId);
        const action = yield* nativeCall(a, parent, "device_execute", {
          ...replacementRoute,
          target,
          command: { type: "click", ref: "@e1" },
          clientRequestId: "typed383-generation-legitimate-click",
        });
        expect(action.structuredContent).toEqual({ type: "action", target, status: "dispatched" });
        expect(b.deviceProcessCalls.filter((call) => call.args[1] === "click")).toHaveLength(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "device382: registered native device owners start after receipt and historical open/empty-close cannot replace later intent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("device382", "Isolated devices", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            preferencesOwner: true,
            deviceOwner: true,
          },
        );
        const parent = ThreadId.make("device382-parent");
        yield* seedParent(node, parent);
        const devices = node.fixtureDevice;
        if (devices === undefined) throw new Error("Expected the real isolated device owner");
        const emptyClose = { clientRequestId: "device382-empty-close" };
        expect((yield* nativeCall(node, parent, "device_close", emptyClose)).isError).not.toBe(
          true,
        );
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(0);
        yield* devices.open({
          threadId: parent,
          hostId: LOCAL_DEVICE_HOST_ID,
          deviceId: DeviceId.make("fixture-device382"),
          platform: "android",
        });
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(1);
        expect((yield* nativeCall(node, parent, "device_close", emptyClose)).isError).not.toBe(
          true,
        );
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(1);
        const input = {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "device382-open",
        };
        const opened = yield* nativeCall(node, parent, "device_open", input);
        expect(opened.isError, json(opened)).not.toBe(true);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(1);
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          opened.structuredContent,
        );
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(1);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(1);
        yield* devices.close({ threadId: parent });
        expect((yield* nativeCall(node, parent, "device_open", input)).structuredContent).toEqual(
          opened.structuredContent,
        );
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(0);
        const fs = yield* FileSystem.FileSystem;
        const file = yield* agentDeviceConfigPath(
          node.config.stateDir,
          LOCAL_DEVICE_HOST_ID,
          yield* Path.Path,
        );
        yield* fs.writeFileString(file, "Later native configuration before the new action");
        yield* Ref.set(node.loseDeviceConfiguration, true);
        const uncertain = { ...input, clientRequestId: "device382-uncertain" };
        const failed = yield* nativeCall(node, parent, "device_open", uncertain);
        expect(failed.isError, json(failed)).toBe(true);
        expect(json(failed)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
        expect(yield* fs.readFileString(file)).toContain("ISOLATED_DEVICE_TOKEN_382");
        const later = "Independent configuration after the uncertain original action";
        yield* fs.writeFileString(file, later);
        const active = { ...node, ...(yield* node.restartSchedulerTransport) };
        const again = yield* nativeCall(active, parent, "device_open", uncertain);
        expect(json(again)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
        expect(yield* fs.readFileString(file)).toBe(later);
        expect(yield* devices.sessionsForThread(parent)).toHaveLength(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "ISOLATED_DEVICE_TOKEN_382",
        );
        yield* node.fixtureSettings.updateSettings({ enableAgentDeviceAccess: false });
        const denied = yield* nativeCall(active, parent, "device_open", input);
        expect(denied.isError, json(denied)).toBe(true);
        expect(yield* Ref.get(node.deviceAgentStarts)).toBe(2);
        expect(yield* Ref.get(node.deviceConfigurations)).toBe(2);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "png382: registered peer PNG saves stay on the originating environment and retain exact artifact bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("png382-source", "PNG source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("png382-destination", "PNG destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("png382-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "PNG owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "png382-owner",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const connected = yield* Deferred.make<void>();
        const png = Buffer.from(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
          "base64",
        );
        let reads = 0;
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          save: true,
          includeImage: true,
          clientRequestId: "png382-saved",
        };
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Stream.runForEach(
              yield* b.browser.connect({
                clientId: "png382-renderer",
                environmentId: b.environmentId,
              }),
              (event) =>
                event.type === "connected"
                  ? Deferred.succeed(connected, undefined)
                  : Effect.gen(function* () {
                      reads++;
                      yield* b.browser.respond({
                        clientId: "png382-renderer",
                        connectionId: event.connectionId,
                        requestId: event.request.requestId,
                        ok: true,
                        result: {
                          url: "https://isolated-png.example.test/private?fixture=DO_NOT_CACHE_PAGE_URL_382",
                          title: "Isolated page",
                          loading: false,
                          visibleText: "DO_NOT_CACHE_PAGE_TEXT_382",
                          interactiveElements: [],
                          accessibilityTree: {},
                          consoleEntries: [],
                          networkEntries: [],
                          actionTimeline: [],
                          screenshot: {
                            mimeType: "image/png",
                            data: png.toString("base64"),
                            width: 1,
                            height: 1,
                          },
                        },
                      });
                    }),
            ).pipe(Effect.forkScoped);
            yield* Deferred.await(connected);
            return yield* nativeCall(a, parent, "preview_snapshot", input);
          }),
        );

        expect(first.isError, json(first)).not.toBe(true);
        const receipt = yield* decodeSavedPngReceipt(first.structuredContent);
        expect(receipt.artifact.environmentId).toBe(a.environmentId);
        expect(receipt.artifact.sizeBytes).toBe(png.byteLength);
        expect(receipt.artifact.digest).toBe(
          "2640059609118b695c139804676372eca75c30d30e5906775902d13e44f5356c",
        );
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual(
          Array.from(png),
        );
        expect(receipt.artifact.screenshotPath.startsWith(a.config.browserArtifactsDir)).toBe(true);
        expect(yield* fs.exists(b.config.browserArtifactsDir)).toBe(false);
        expect(first.content.some((item) => item.type === "image")).toBe(true);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        const sql = Context.get(a.database, SqlClient.SqlClient);
        expect(json(yield* sql`SELECT * FROM mcp_request_receipts`)).not.toContain(
          "DO_NOT_CACHE_PAGE_",
        );
        expect(reads).toBe(1);
        // Existing confirmed PNGs can reconcile their exact original forwarding ledger.
        yield* sql`DELETE FROM mcp_request_destinations WHERE request_id=${receipt.operationId} AND operation='saved-preview-authorization'`;
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        const revokedPreview = { ...grant, preview: false };
        yield* b.authority.setGrant(revokedPreview);
        const previewDenied = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(previewDenied.isError).toBe(true);
        expect(previewDenied.structuredContent).toMatchObject({
          code: "capability_denied",
          message:
            'MCP credential does not grant the preview capability: browser preview tools are off for this thread. Do not retry them. To check a page, use a headless browser from the shell, such as Playwright, or curl. The user can turn on "Agent browser access" in Settings; it applies when the agent session next starts.',
        });
        const newSaveDenied = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          clientRequestId: "png382-ungranted-new-save",
        });
        expect(newSaveDenied.isError).toBe(true);
        expect(newSaveDenied.structuredContent).toEqual(previewDenied.structuredContent);
        expect(reads).toBe(1);
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        expect(Array.from(yield* fs.readFile(receipt.artifact.screenshotPath))).toEqual(
          Array.from(png),
        );
        yield* b.authority.setGrant({ ...grant, preview: true });
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("png382-narrow-current"),
          threadId: parent,
          runtimeMode: "auto",
        });
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("png382-restore-current"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        const unrelated = ThreadId.make("png382-unrelated-context");
        yield* seedParent(b, unrelated);
        const outside = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          contextThreadId: unrelated,
          clientRequestId: "png382-outside-context",
        });
        expect(outside.isError).toBe(true);
        expect(json(outside)).toContain("resource scope");
        yield* b.setSavedPreviewTransport("unsupported");
        const older = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(older.isError).toBe(true);
        expect(json(older)).toContain("does not support saved-preview authorization");
        const unsupportedNewSave = yield* nativeCall(a, parent, "preview_snapshot", {
          ...input,
          clientRequestId: "png382-old-peer-new-save",
        });
        expect(unsupportedNewSave.isError).toBe(true);
        expect(json(unsupportedNewSave)).toContain("does not support saved-preview authorization");
        expect(yield* fs.readDirectory(a.config.browserArtifactsDir)).toHaveLength(1);
        yield* b.setSavedPreviewTransport("unavailable");
        const unavailableAuthority = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(unavailableAuthority.isError).toBe(true);
        expect(json(unavailableAuthority)).toContain("authority is unavailable");
        yield* b.setSavedPreviewTransport("supported");
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).structuredContent).toEqual(
          receipt,
        );
        expect(reads).toBe(1);
        yield* fs.writeFileString(
          receipt.artifact.screenshotPath,
          "Later independent PNG path edit",
        );
        const replay = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(replay.structuredContent).toEqual(receipt);
        expect(replay.content.some((item) => item.type === "image")).toBe(false);
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent PNG path edit",
        );
        expect(reads).toBeGreaterThan(0);
        // Ordinary inspection remains available independently of historical saves.
        yield* Stream.runForEach(
          yield* b.browser.connect({ clientId: "png382-renderer", environmentId: b.environmentId }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  reads++;
                  yield* b.browser.respond({
                    clientId: "png382-renderer",
                    connectionId: event.connectionId,
                    requestId: event.request.requestId,
                    ok: true,
                    result: {
                      url: "https://isolated-png.example.test/private?fixture=DO_NOT_CACHE_PAGE_URL_382",
                      title: "Isolated page",
                      loading: false,
                      visibleText: "DO_NOT_CACHE_PAGE_TEXT_382",
                      interactiveElements: [],
                      accessibilityTree: {},
                      consoleEntries: [],
                      networkEntries: [],
                      actionTimeline: [],
                      screenshot: {
                        mimeType: "image/png",
                        data: png.toString("base64"),
                        width: 1,
                        height: 1,
                      },
                    },
                  });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        yield* b.setSavedPreviewTransport("unsupported");
        const inspect = yield* nativeCall(a, parent, "preview_snapshot", {
          environmentId: b.environmentId,
          contextThreadId: child,
        });
        expect(inspect.isError, json(inspect)).not.toBe(true);
        expect(inspect.structuredContent).not.toHaveProperty("type");
        yield* b.setSavedPreviewTransport("supported");
        yield* b.auth.revokeSession(grant.sessionId);
        const revoked = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(revoked.isError).toBe(true);
        expect(revoked.content.some((item) => item.type === "image")).toBe(false);
        yield* a.links.unlink(b.environmentId);
        expect((yield* nativeCall(a, parent, "preview_snapshot", input)).isError).toBe(true);
        yield* linkTo(a.links, b, "full-access");
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const replacementGrant = (yield* b.authority.grants).find(
          (item) => item.sessionId !== grant.sessionId,
        )!;
        yield* b.authority.setGrant({
          ...replacementGrant,
          preview: true,
          resourceScope: "environment-resources",
        });
        const replaced = yield* nativeCall(a, parent, "preview_snapshot", input);
        expect(replaced.isError).toBe(true);
        expect(json(replaced)).toContain("original destination grant generation");
        expect(yield* fs.readFileString(receipt.artifact.screenshotPath)).toBe(
          "Later independent PNG path edit",
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "recording382: registered peer recording returns an owned bounded reference usable by attachment send",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("recording382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("recording382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("recording382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Recording owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "recording382-owner",
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const bytes = new Uint8Array([0x1a, 0x45, 0xdf, 0xa3, 0x81, 1, 0x42, 0x86, 0x81, 1]);
        const uploaded = yield* AttachmentUpload.issueAttachmentUploadUrl({
          type: "file",
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        }).pipe(
          Effect.provideService(Config.ServerConfig, b.config),
          Effect.provideService(
            Secrets.ServerSecretStore,
            Context.get(b.services, Secrets.ServerSecretStore),
          ),
        );
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${uploaded.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const connected = yield* Deferred.make<void>();
        let stops = 0;
        let rendererCalls = 0;
        yield* Stream.runForEach(
          yield* b.browser.connect({
            clientId: "recording382-renderer",
            environmentId: b.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  const request = event.request;
                  rendererCalls++;
                  if (request.operation === "recordingStop") {
                    stops++;
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        id: "renderer-recording",
                        tabId: "recording382-tab",
                        path: "/isolated-renderer/recording.webm",
                        uploadedAttachmentId: uploaded.attachmentId,
                        mimeType: "video/webm",
                        sizeBytes: bytes.byteLength,
                        createdAt: "2026-10-18T00:00:00Z",
                      },
                    });
                  } else
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        available: true,
                        visible: true,
                        tabId: null,
                        url: null,
                        title: null,
                        loading: false,
                      },
                    });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          clientRequestId: "recording382-stop",
        };
        const stopped = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(stopped.isError, json(stopped)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PeerAttachmentReference }),
        )(stopped.structuredContent);
        expect(resource).toMatchObject({
          environmentId: b.environmentId,
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        });
        expect(resource.owner).toMatch(/^[a-f0-9]{64}$/);
        expect(resource.digest).toMatch(/^[a-f0-9]{64}$/);
        expect(stopped.structuredContent).not.toHaveProperty("path");
        expect(json(stopped)).not.toContain("/isolated-renderer/");
        const sent = yield* nativeCall(a, parent, "t3_thread_send_attachments", {
          environmentId: b.environmentId,
          threadId: child,
          clientRequestId: "recording382-send",
          message: "Owned recording",
          attachments: [
            {
              type: "file",
              id: resource.attachmentId,
              environmentId: resource.environmentId,
              name: resource.name,
              mimeType: resource.mimeType,
              sizeBytes: resource.sizeBytes,
            },
          ],
        });
        expect(sent.isError, json(sent)).not.toBe(true);
        const delivered = (yield* b.threads.getThreadRecords(child, ["messages"])).messages.flatMap(
          (message) => message.attachments,
        );
        expect(delivered).toHaveLength(1);
        const path = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: delivered[0]!.id,
        });
        expect(path).not.toBeNull();
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(path!))).toEqual(Array.from(bytes));
        const nativeDb = Context.get(b.database, SqlClient.SqlClient);
        const nativeBefore =
          yield* nativeDb`SELECT id,generation,input_digest,state,result_json FROM mcp_request_receipts WHERE state='completed' ORDER BY id`;
        expect(nativeBefore.length).toBeGreaterThan(0);
        const originalPath = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: resource.attachmentId,
        })!;
        const laterBytes = new Uint8Array([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        yield* fs.writeFile(originalPath, laterBytes);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        // A delayed duplicate POST uses the same still-valid app-issued upload token.
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${uploaded.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const replay = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(stopped.structuredContent);
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        expect(
          yield* nativeDb`SELECT id,generation,input_digest,state,result_json FROM mcp_request_receipts WHERE state='completed' ORDER BY id`,
        ).toEqual(nativeBefore);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        const originalNative =
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`;
        const rebuilt = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([rebuilt.url])} WHERE environment_id=${b.environmentId}`;
        const renewed = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(renewed.isError, json(renewed)).not.toBe(true);
        expect(renewed.structuredContent).toEqual(stopped.structuredContent);
        expect(stops).toBe(1);
        expect(rendererCalls).toBe(1);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
        expect(
          (yield* nativeCall(a, parent, "preview_recording_stop", {
            ...input,
            tabId: "changed-intent",
          })).isError,
        ).toBe(true);
        const other = ThreadId.make("recording382-rival");
        yield* seedParent(a, other);
        expect((yield* nativeCall(a, other, "preview_recording_stop", input)).isError).toBe(true);
        // A separate authorized caller has an independent request namespace.
        // Its new stop may run, but cannot acquire the first caller's reference.
        expect(stops).toBe(2);
        expect(rendererCalls).toBe(2);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
        const unrelated = ThreadId.make("recording382-outside-grant");
        yield* seedParent(b, unrelated);
        expect(
          (yield* nativeCall(a, parent, "preview_recording_stop", {
            ...input,
            contextThreadId: unrelated,
            clientRequestId: "recording382-outside",
          })).isError,
        ).toBe(true);
        expect(stops).toBe(2);
        yield* b.authority.setGrant({ ...grant, preview: false });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        yield* b.authority.setGrant({ ...grant, preview: true });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).not.toBe(
          true,
        );
        expect(stops, "grant denial / restore must not stop renderer").toBe(2);
        yield* a.links.unlink(b.environmentId);
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        expect(stops, "unlink denial must not stop renderer").toBe(2);
        yield* linkTo(a.links, rebuilt, "full-access");
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const replacement = (yield* b.authority.grants).find(
          (g) => g.sessionId !== grant.sessionId,
        )!;
        yield* b.authority.setGrant({ ...replacement, preview: true });
        expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(true);
        expect(stops).toBe(2);
        expect(rendererCalls).toBe(2);
        expect(Array.from(yield* fs.readFile(originalPath))).toEqual(Array.from(laterBytes));
        expect(
          yield* nativeDb`SELECT id,owner,generation,manifest,digest,expected_digest FROM attachment_staging WHERE id=${resource.attachmentId}`,
        ).toEqual(originalNative);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "spectrum382: registered Spectrum and participant commands retain identity across credential renewal",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("spectrum382", "Spectrum", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("spectrum382-parent");
        yield* seedParent(node, parent);
        const input = {
          question: "An isolated identity check",
          mode: "council",
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: "spectrum382-once",
        };
        const first = yield* nativeCall(node, parent, "start_spectrum", input);
        expect(first.isError, json(first)).not.toBe(true);
        const spectrum = yield* decodeAutomationSpectrumResult(first.structuredContent);
        expect(
          (yield* nativeCall(node, parent, "start_spectrum", input)).structuredContent,
        ).toEqual(spectrum);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        expect(yield* sql`SELECT thread_id FROM fork_spectra`).toHaveLength(1);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_type='spectrum.register'`,
        ).toHaveLength(1);
        const member = spectrum.participants[0]!.threadId;
        yield* node.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("spectrum382-member-turn"),
          threadId: member,
          messageId: MessageId.make("spectrum382-member-input"),
          text: "Isolated work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(node, member);
        const send = {
          threadId: parent,
          message: "Isolated participant report",
          clientRequestId: "spectrum382-member-send",
        };
        const sent = yield* nativeCall(node, member, "t3_thread_send", send);
        expect(sent.isError, json(sent)).not.toBe(true);
        const decoded = yield* decodeSend(sent.structuredContent);
        const replay = yield* nativeCall(node, member, "t3_thread_send", send);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(decoded);
        const messages = (yield* node.threads.getThreadRecords(parent, [
          "messages",
        ])).messages.filter((message) => message.id === decoded.messageId);
        expect(messages).toHaveLength(1);
        const changed = yield* nativeCall(node, parent, "start_spectrum", {
          ...input,
          question: "Changed question",
        });
        expect(changed.isError).toBe(true);
        expect(yield* sql`SELECT thread_id FROM fork_spectra`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "html382: registered peer HTML publication keeps its accepted artifact after replay and later edits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("html382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("html382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("html382-parent");
        yield* seedParent(a, parent);
        const launch = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "HTML target",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "html382-target",
        });
        expect(launch.isError, json(launch)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launch.structuredContent)).threadId;
        yield* activate(b, child);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          html: "<!doctype html><html><body>Isolated HTML 382</body></html>",
          title: "Stable page",
          height: 420,
          clientRequestId: "html382-once",
        };
        const first = yield* nativeCall(a, parent, "html_render", input);
        expect(first.isError, json(first)).not.toBe(true);
        const result = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ htmlRender: Schema.Struct({ attachmentId: Schema.String }) }),
        )(first.structuredContent);
        const path = resolveAttachmentPathById({
          attachmentsDir: a.config.attachmentsDir,
          attachmentId: result.htmlRender.attachmentId,
        });
        expect(path).not.toBeNull();
        const fs = yield* FileSystem.FileSystem;
        expect(yield* fs.readFileString(path!)).toContain("Isolated HTML 382");
        yield* fs.writeFileString(path!, "Later independent page edit 382");
        const replay = yield* nativeCall(a, parent, "html_render", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(first.structuredContent);
        expect(yield* fs.readFileString(path!)).toBe("Later independent page edit 382");
        const changed = yield* nativeCall(a, parent, "html_render", {
          ...input,
          html: "<html>Changed semantic input</html>",
        });
        expect(changed.isError).toBe(true);
        expect(yield* fs.readFileString(path!)).toBe("Later independent page edit 382");
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const rows = yield* sql`SELECT * FROM mcp_request_receipts WHERE state='completed'`;
        expect(rows).toHaveLength(1);
        expect(json(rows)).not.toContain(input.html);
        expect(json(rows)).toContain(result.htmlRender.attachmentId);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "named382: registered title-only project creation replays its exact native owner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("named382-registered", "Named", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, nativeOwners: true },
        );
        const parent = ThreadId.make("named382-parent");
        yield* seedParent(node, parent);
        const input = { title: "Isolated named request", clientRequestId: "named382-original" };
        const first = yield* nativeCall(node, parent, "t3_project_create", input);
        expect(first.isError, json(first)).not.toBe(true);
        const created = yield* Schema.decodeUnknownEffect(
          ProjectToolkit.tools.t3_project_create.successSchema,
        )(first.structuredContent);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const rows = yield* sql<{
          project_id: string;
          workspace_root: string;
        }>`SELECT project_id,workspace_root FROM managed_project_requests`;
        expect(rows).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        const readme = `${rows[0]!.workspace_root}/README.md`;
        yield* fs.writeFileString(readme, "Later independent README edit 382");
        const replay = yield* nativeCall(node, parent, "t3_project_create", input);
        expect(replay.isError, json(replay)).not.toBe(true);
        expect(replay.structuredContent).toEqual(created);
        expect(yield* fs.readFileString(readme)).toBe("Later independent README edit 382");
        expect(yield* sql`SELECT project_id FROM managed_project_requests`).toHaveLength(1);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE aggregate_kind='project' AND command_type='project.create'`,
        ).toHaveLength(1);
        const changed = yield* nativeCall(node, parent, "t3_project_create", {
          ...input,
          title: "Different title",
        });
        expect(changed.isError).toBe(true);
        expect(yield* sql`SELECT project_id FROM managed_project_requests`).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "manual382: registered manual identity recovers native acceptance and preserves later scheduler work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("manual382", "Manual", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const parent = ThreadId.make("manual382-parent");
        yield* seedParent(node, parent);
        const { task } = yield* node.scheduled.upsert({
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          commandId: CommandId.make("manual382-schedule"),
          title: "One native action",
          prompt: "Isolated manual work",
          enabled: true,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: node.project.id,
          threadId: null,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const input = { taskId: task.id, clientRequestId: "manual382-request" };
        const call = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(call.isError, json(call)).not.toBe(true);
        const decoded = yield* decodeManualRun(call.structuredContent);
        expect(decoded.manualReceipt).toMatchObject({
          requestId: input.clientRequestId,
          stage: "native-accepted",
        });
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const messages =
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`;
        expect(messages).toHaveLength(2);
        const before = (yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)!;
        expect(before.runCount).toBe(1);
        yield* node.scheduled.setEnabled({ id: task.id, enabled: false });
        const later = (yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)!;
        const replay = yield* nativeCall(node, parent, "run_scheduled_task_now", input);
        expect(replay.structuredContent).toEqual(decoded);
        expect((yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)).toEqual(later);
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(2);
        const changed = yield* nativeCall(node, parent, "run_scheduled_task_now", {
          ...input,
          taskId: "different-schedule",
        });
        expect(changed.isError).toBe(true);

        const disabled = yield* nativeCall(node, parent, "run_scheduled_task_now", {
          ...input,
          clientRequestId: "manual382-disabled-new",
        });
        expect(disabled.isError, json(disabled)).not.toBe(true);
        const pausedManual = yield* decodeManualRun(disabled.structuredContent);
        expect(pausedManual.manualReceipt).toMatchObject({
          requestId: "manual382-disabled-new",
          stage: "native-accepted",
        });
        const paused = (yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)!;
        expect(paused.enabled).toBe(false);
        expect(paused.nextRunAt).toBeNull();
        expect(paused.runCount).toBe(2);
        expect(
          (yield* nativeCall(node, parent, "run_scheduled_task_now", {
            ...input,
            clientRequestId: "manual382-disabled-new",
          })).structuredContent,
        ).toEqual(pausedManual);
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(3);
        yield* node.scheduled.setEnabled({ id: task.id, enabled: true });

        // Stop the caller after actual native commit but before its reply/bookkeeping.
        yield* Ref.set(node.holdManualLaunchReply, true);
        const requestOwner = yield* RequestReceipts.layer.pipe(
          Layer.provide(Layer.succeedContext(node.database)),
          Layer.fresh,
          Layer.build,
        );
        const identity = yield* Context.get(
          requestOwner,
          RequestReceipts.McpRequestReceipts,
        ).identify(
          caller(node, parent),
          "run_scheduled_task_now",
          { taskId: task.id },
          "manual382-lost-native-reply",
        );
        const interrupted = yield* node.scheduled
          .runNow({ id: task.id }, { request: identity })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(node.manualLaunchReplyHeld);
        yield* Fiber.interrupt(interrupted);
        const nativeMessages =
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`;
        expect(nativeMessages).toHaveLength(4);
        const rebuilt = yield* node.rebuildScheduler;
        const recovered = yield* Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
          { id: task.id },
          { request: identity },
        );
        expect(recovered.manualReceipt?.stage).toBe("native-accepted");
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(4);
        expect((yield* node.scheduled.list()).tasks.find((t) => t.id === task.id)?.runCount).toBe(
          3,
        );
        const denied = yield* Effect.result(
          Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
            { id: task.id },
            { request: { ...identity, generation: "replacement" } },
          ),
        );
        expect(denied._tag).toBe("Failure");
        yield* node.scheduled.delete({ id: task.id });
        yield* node.scheduled.upsert({
          purposeIntent: { version: 1 as const, intent: { type: "unscoped" as const } },
          id: task.id,
          title: "Replacement",
          prompt: "Other action",
          enabled: false,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: node.project.id,
          threadId: parent,
          workspaceStrategy: { type: "root" },
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const replacement = yield* Effect.result(
          Context.get(rebuilt, ScheduledTasks.ScheduledTaskService).runNow(
            { id: task.id },
            { request: identity },
          ),
        );
        expect(replacement._tag).toBe("Failure");
        expect(
          yield* sql`SELECT message_id FROM orchestration_v2_projection_messages WHERE role='user'`,
        ).toHaveLength(4);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "preferences382: negotiated peer result retains its bounded variant and original grant checks",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("preferences382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        const b = yield* makeNode(
          descriptorOf("preferences382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("preferences382-peer-parent");
        yield* seedParent(a, parent);
        yield* a.forwarding.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const input = {
          environmentId: b.environmentId,
          clientRequestId: "peer-preferences-confirmed",
          sourceControlWritingStyle: { customInstructions: "ISOLATED_PEER_PRIVATE_TEXT_382" },
        };
        // The approved direct full-access peer exception remains supported.
        const accepted = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const decoded = yield* Schema.decodeUnknownEffect(
          EnvironmentToolkit.tools.t3_environment_preferences_update.successSchema,
        )(accepted.structuredContent);
        expect(decoded).toMatchObject({
          type: "confirmed-operation",
          applied: true,
          stage: "confirmed",
        });
        const writes = yield* Ref.get(b.settingsWrites);
        expect(writes).toBeGreaterThan(0);
        expect(
          (yield* nativeCall(a, parent, "t3_environment_preferences_update", input))
            .structuredContent,
        ).toEqual(decoded);
        const reordered = yield* nativeCall(
          a,
          parent,
          "t3_environment_preferences_update",
          input,
          new Set(["device", "preview", "orchestration"]),
        );
        expect(reordered.isError, json(reordered)).not.toBe(true);
        expect(reordered.structuredContent).toEqual(decoded);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        for (const rows of [
          yield* sql`SELECT * FROM mcp_request_receipts`,
          yield* sql`SELECT * FROM peer_authority_records`,
          yield* sql`SELECT * FROM peer_control_frames`,
        ])
          expect(json(rows)).not.toContain(input.sourceControlWritingStyle.customInstructions);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("preferences382-narrow"),
          threadId: parent,
          runtimeMode: "auto",
        });
        const narrowed = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(narrowed.isError).toBe(true);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
        yield* a.threads.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("preferences382-restore"),
          threadId: parent,
          runtimeMode: "full-access",
        });
        // A destination restored without its request ledger cannot turn source
        // historical intent into a fresh operation under a replacement grant.
        yield* sql`DELETE FROM mcp_request_receipts`;
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        const replaced = yield* nativeCall(a, parent, "t3_environment_preferences_update", input);
        expect(replaced.isError).toBe(true);
        expect(yield* Ref.get(b.settingsWrites)).toBe(writes);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "preferences382: registered keyed receipts exclude private text and never overwrite later settings",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const node = yield* makeNode(
          descriptorOf("preferences382", "Preferences", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true, preferencesOwner: true },
        );
        const parent = ThreadId.make("preferences382-parent");
        yield* seedParent(node, parent);
        const firstPrivate = "ISOLATED_PRIVATE_INSTRUCTIONS_382_ALPHA_DO_NOT_PERSIST_IN_RECEIPTS";
        const secondPrivate = "ISOLATED_PRIVATE_INSTRUCTIONS_382_BETA_CHANGED_INPUT";
        const input = {
          clientRequestId: "preferences-confirmed",
          sourceControlWritingStyle: { customInstructions: firstPrivate },
        };
        const response = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          input,
        );
        expect(response.isError, json(response)).not.toBe(true);
        const receipt = yield* Schema.decodeUnknownEffect(
          EnvironmentToolkit.tools.t3_environment_preferences_update.successSchema,
        )(response.structuredContent);
        expect(receipt).toMatchObject({
          type: "confirmed-operation",
          applied: true,
          requestId: "preferences-confirmed",
          stage: "confirmed",
        });
        expect(Object.keys(receipt).sort()).toEqual([
          "applied",
          "operationId",
          "requestId",
          "stage",
          "type",
        ]);
        const writes = yield* Ref.get(node.settingsWrites);
        expect(writes).toBeGreaterThan(0);
        const sql = Context.get(node.database, SqlClient.SqlClient);
        const persisted = yield* sql`SELECT * FROM mcp_request_receipts`;
        expect(json(persisted)).not.toContain(firstPrivate);
        expect(json(persisted)).toContain("confirmed-operation");
        // Lose the confirmed wire reply, renew the credential and reconstruct the SQL receipt owner.
        const reopened = yield* node.reopenDatabase;
        const rebuilt = yield* RequestReceipts.layer.pipe(
          Layer.provide(Layer.succeedContext(reopened)),
          Layer.fresh,
          Layer.build,
        );
        const rows = yield* sql<{
          id: string;
          generation: string;
          input_digest: string;
        }>`SELECT id,generation,input_digest FROM mcp_request_receipts WHERE state='completed'`;
        expect(rows).toHaveLength(1);
        const row = rows[0]!;
        const recovered = yield* Context.get(rebuilt, RequestReceipts.McpRequestReceipts).replay({
          id: row.id,
          generation: row.generation,
          inputDigest: row.input_digest,
          clientRequestId: input.clientRequestId,
        });
        expect(Option.getOrThrow(recovered)).toEqual(receipt);
        const replay = yield* nativeCall(node, parent, "t3_environment_preferences_update", input);
        expect(replay.structuredContent).toEqual(receipt);
        expect(yield* Ref.get(node.settingsWrites)).toBe(writes);
        const changed = yield* nativeCall(node, parent, "t3_environment_preferences_update", {
          ...input,
          sourceControlWritingStyle: { customInstructions: secondPrivate },
        });
        expect(changed.isError).toBe(true);
        expect(yield* Ref.get(node.settingsWrites)).toBe(writes);
        const later = yield* nativeCall(node, parent, "t3_environment_preferences_update", {
          sourceControlWritingStyle: { customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382" },
        });
        expect(later.isError, json(later)).not.toBe(true);
        expect(later.structuredContent).toMatchObject({
          sourceControlWritingStyle: { customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382" },
        });
        expect(later.structuredContent).not.toHaveProperty("type");
        const afterLater = yield* Ref.get(node.settingsWrites);
        expect(
          (yield* nativeCall(node, parent, "t3_environment_preferences_update", input))
            .structuredContent,
        ).toEqual(receipt);
        expect(yield* Ref.get(node.settingsWrites)).toBe(afterLater);
        const current = yield* nativeCall(node, parent, "t3_environment_read", {});
        expect(current.structuredContent).toMatchObject({
          preferences: {
            sourceControlWritingStyle: {
              customInstructions: "ISOLATED_LATER_INDEPENDENT_EDIT_382",
            },
          },
        });
        yield* Ref.set(node.loseSettingsConfirmation, true);
        const ambiguousInput = {
          clientRequestId: "preferences-write-confirmation-lost",
          sourceControlWritingStyle: { customInstructions: "ISOLATED_AMBIGUOUS_INSTRUCTIONS_382" },
        };
        const failed = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          ambiguousInput,
        );
        expect(failed.isError).toBe(true);
        const ambiguousWrites = yield* Ref.get(node.settingsWrites);
        // Reopen the actual settings owner and observe that the write happened despite failed acknowledgement.
        const freshSettings = yield* node.rebuildSettings;
        expect(
          (yield* Context.get(freshSettings, Settings.ServerSettingsService).getSettings)
            .sourceControlWritingStyle.customInstructions,
        ).toBe(ambiguousInput.sourceControlWritingStyle.customInstructions);
        const unknown = yield* nativeCall(
          node,
          parent,
          "t3_environment_preferences_update",
          ambiguousInput,
        );
        expect(unknown.isError).toBe(true);
        expect(json(unknown)).toContain("outcome_unknown");
        expect(yield* Ref.get(node.settingsWrites)).toBe(ambiguousWrites);
        const allRows = json(yield* sql`SELECT * FROM mcp_request_receipts`);
        for (const privateText of [
          firstPrivate,
          secondPrivate,
          ambiguousInput.sourceControlWritingStyle.customInstructions,
          "ISOLATED_LATER_INDEPENDENT_EDIT_382",
        ])
          expect(allRows).not.toContain(privateText);
        expect(allRows).not.toContain("customInstructions");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: destination upload rivals consume once and an old generation cannot publish bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("destination382-a", "A", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("destination382-b", "B", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("destination382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Destination",
          message: "Receive",
          modelSelection: selection,
          clientRequestId: "destination-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const upload = {
          type: "file",
          name: "once.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepare = (key: string) =>
          nativeCall(
            a,
            parent,
            "t3_attachment_prepare_upload",
            {
              environmentId: b.environmentId,
              upload,
              digest: "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
              clientRequestId: key,
            },
            capabilities,
            credential,
          );
        const prepared = yield* prepare("destination-upload");
        expect(prepared.isError, json(prepared.content)).not.toBe(true);
        const decodeUpload = Schema.decodeUnknownEffect(
          Schema.Struct({
            attachmentId: Schema.String,
            relativeUrl: Schema.String,
            uploadOrigin: Schema.String,
          }),
        );
        const issued = yield* decodeUpload(prepared.structuredContent);
        expect(issued.uploadOrigin).toBe(b.url);
        const http = yield* HttpClient.HttpClient;
        const store = (url: string) =>
          http.execute(
            HttpClientRequest.post(url).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
            ),
          );
        expect((yield* store(`${issued.uploadOrigin}${issued.relativeUrl}`)).status).toBe(204);
        const send = (key: string) =>
          nativeCall(
            a,
            parent,
            "t3_thread_send_attachments",
            {
              environmentId: b.environmentId,
              threadId: child,
              message: "One use",
              attachments: [{ ...upload, id: issued.attachmentId, environmentId: b.environmentId }],
              clientRequestId: key,
            },
            capabilities,
            credential,
          );
        const raced = yield* Effect.all(
          [send("destination-consumer-a"), send("destination-consumer-b")],
          { concurrency: "unbounded" },
        );
        expect(
          raced.filter((result) => result.isError !== true),
          json(raced),
        ).toHaveLength(1);
        expect(raced.filter((result) => result.isError === true)).toHaveLength(1);
        const records = yield* b.threads.getThreadRecords(child, ["messages"]);
        expect(records.messages.filter((message) => message.role === "user")).toHaveLength(2);
        expect(records.messages.flatMap((message) => message.attachments)).toHaveLength(1);
        const stale = yield* prepare("stale-upload");
        expect(stale.isError, json(stale.content)).not.toBe(true);
        const old = yield* decodeUpload(stale.structuredContent);
        yield* a.links.unlink(b.environmentId);
        expect((yield* store(`${old.uploadOrigin}${old.relativeUrl}`)).status).toBe(403);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          (yield* sql<{
            digest: string | null;
          }>`SELECT digest FROM attachment_staging WHERE id=${old.attachmentId}`)[0]?.digest,
        ).toBe(null);
        const fs = yield* FileSystem.FileSystem;
        expect(
          (yield* fs.readDirectory(b.config.attachmentsDir)).some((path) =>
            path.startsWith(old.attachmentId),
          ),
        ).toBe(false);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: registered media send preserves each MIME and digest and refuses rival source consumption",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("media382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("media382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("media382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Media",
          message: "Receive files",
          modelSelection: selection,
          clientRequestId: "media-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const fixtures = [
          {
            type: "image",
            name: "pixel.png",
            mimeType: "image/png",
            bytes: new Uint8Array(
              Buffer.from(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
                "base64",
              ),
            ),
          },
          {
            type: "file",
            name: "clip.mp4",
            mimeType: "video/mp4",
            bytes: new Uint8Array([
              0, 0, 0, 20, 102, 116, 121, 112, 105, 115, 111, 109, 0, 0, 0, 0, 105, 115, 111, 109,
            ]),
          },
          {
            type: "file",
            name: "sound.wav",
            mimeType: "audio/wav",
            bytes: new TextEncoder().encode("RIFF0000WAVEfmt "),
          },
          {
            type: "file",
            name: "notes.txt",
            mimeType: "text/plain",
            bytes: new TextEncoder().encode("destination bytes\n"),
          },
        ];
        const http = yield* HttpClient.HttpClient;
        const attachments = yield* Effect.forEach(fixtures, (fixture, index) =>
          Effect.gen(function* () {
            const upload = {
              type: fixture.type,
              name: fixture.name,
              mimeType: fixture.mimeType,
              sizeBytes: fixture.bytes.byteLength,
            };
            const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
              upload,
              clientRequestId: `media-upload-${index}`,
            });
            expect(prepared.isError, json(prepared.content)).not.toBe(true);
            const issued = yield* Schema.decodeUnknownEffect(
              Schema.Struct({ attachmentId: Schema.String, relativeUrl: Schema.String }),
            )(prepared.structuredContent);
            expect(
              (yield* http.execute(
                HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
                  HttpClientRequest.setBody(HttpBody.uint8Array(fixture.bytes)),
                ),
              )).status,
            ).toBe(204);
            return { ...upload, id: issued.attachmentId };
          }),
        );
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const send = (clientRequestId: string) =>
          nativeCall(
            a,
            parent,
            "t3_thread_send_attachments",
            {
              environmentId: b.environmentId,
              threadId: child,
              attachments,
              message: "Exact media",
              clientRequestId,
            },
            capabilities,
            credential,
          );
        const raced = yield* Effect.all([send("source-consumer-a"), send("source-consumer-b")], {
          concurrency: "unbounded",
        });
        expect(
          raced.filter((result) => result.isError !== true),
          json(raced),
        ).toHaveLength(1);
        expect(raced.filter((result) => result.isError === true)).toHaveLength(1);
        const winningKey = raced[0]!.isError === true ? "source-consumer-b" : "source-consumer-a";
        const replayed = yield* send(winningKey);
        expect(replayed.isError, json(replayed.content)).not.toBe(true);
        const records = yield* b.threads.getThreadRecords(child, ["messages", "runs"]);
        expect(records.messages.filter((message) => message.role === "user")).toHaveLength(2);
        const delivered = records.messages.flatMap((message) => message.attachments);
        expect(delivered).toHaveLength(fixtures.length);
        const fs = yield* FileSystem.FileSystem;
        for (const fixture of fixtures) {
          const attachment = delivered.find((item) => item.name === fixture.name)!;
          expect(attachment.mimeType).toBe(fixture.mimeType);
          expect(attachment.sizeBytes).toBe(fixture.bytes.byteLength);
          const path = resolveAttachmentPath({
            attachmentsDir: b.config.attachmentsDir,
            attachment,
          });
          expect(Array.from(yield* fs.readFile(path!))).toEqual(Array.from(fixture.bytes));
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "requests382: mixed versions negotiate before effects and imported unowned launches cannot duplicate",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("import382-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("import382-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const old = yield* makeNode(
          descriptorOf("import382-old", "Old", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true },
        );
        const parent = ThreadId.make("import382-parent");
        yield* seedParent(a, parent);
        yield* linkTo(a.links, b, "full-access");
        yield* linkTo(a.links, old, "full-access");
        const unavailable = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: old.environmentId,
          projectId: old.project.id,
          title: "Unsupported",
          message: "Do not launch",
          modelSelection: selection,
          clientRequestId: "needs-protocol",
        });
        expect(unavailable.isError).toBe(true);
        const oldSql = Context.get(old.database, SqlClient.SqlClient);
        expect(
          yield* oldSql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${old.project.id}`,
        ).toHaveLength(0);
        const http = yield* HttpClient.HttpClient;
        expect((yield* http.get(`${b.url}/mcp/peer-control/requests`)).status).toBe(401);
        // The old source path has no new receipt owner. Its original accepted
        // destination record remains verifiable as an effect, but has no input ledger.
        const legacy = yield* PeerForwarding.PeerForwarding.pipe(
          Effect.provide(
            PeerForwarding.layer.pipe(
              Layer.provide(
                Layer.succeedContext(Context.omit(RequestReceipts.McpRequestReceipts)(a.services)),
              ),
              Layer.provide(Layer.succeedContext(a.runtime)),
            ),
          ),
        );
        const input = {
          projectId: b.project.id,
          title: "Imported",
          message: "Exactly one",
          modelSelection: selection,
          clientRequestId: "legacy-launch",
        };
        const accepted = yield* legacy.call(
          caller(a, parent),
          ProjectToolkit.tools.t3_thread_launch,
          b.environmentId,
          input,
        );
        const receipt = yield* decodeLaunchReceipt(accepted);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* sql`DELETE FROM mcp_request_receipts`;
        const upgraded = yield* Effect.result(
          a.forwarding.call(
            caller(a, parent),
            ProjectToolkit.tools.t3_thread_launch,
            b.environmentId,
            input,
          ),
        );
        expect(upgraded._tag).toBe("Failure");
        if (upgraded._tag === "Failure")
          expect(upgraded.failure.message).toContain("legacy operation");
        const records = yield* b.threads.getThreadRecords(receipt.threadId, ["messages", "runs"]);
        expect(records.messages.filter((m) => m.role === "user")).toHaveLength(1);
        expect(
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "requests382: registered browser ambiguity persists before dispatch and cannot repeat an action",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("external-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("external-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("external-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Browser owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "browser-owner",
        });
        expect(launched.isError, json(launched.content)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        yield* activate(b, child);
        const connected = yield* Deferred.make<void>();
        let actions = 0;
        const sql = Context.get(b.database, SqlClient.SqlClient);
        yield* Stream.runForEach(
          yield* b.browser.connect({
            clientId: "deterministic-browser",
            environmentId: b.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  const request = event.request;
                  if (request.operation === "click") {
                    const started = yield* sql<{
                      state: string;
                    }>`SELECT state FROM mcp_request_receipts WHERE state='started'`;
                    expect(started).toHaveLength(1);
                    actions++;
                    yield* b.browser.respond({
                      clientId: "deterministic-browser",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: false,
                      error: {
                        _tag: "PreviewAutomationExecutionError",
                        message: "External action happened; its confirmation was lost.",
                      },
                    });
                  } else
                    yield* b.browser.respond({
                      clientId: "deterministic-browser",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        available: true,
                        visible: true,
                        tabId: null,
                        url: null,
                        title: null,
                        loading: false,
                      },
                    });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          locator: "role=button[name='Send']",
          clientRequestId: "deliberate-click",
        };
        for (let attempt = 0; attempt < 2; attempt++) {
          const response = yield* nativeCall(a, parent, "preview_click", input);
          expect(response.isError).toBe(true);
          const failure = response.content.map((item) => item.text ?? "").join("");
          expect(failure, json(response)).toContain('"code":"outcome_unknown"');
          expect(failure).toContain('"requestId":"deliberate-click"');
          expect(failure).toContain('"lastConfirmedStage":"dispatch_started"');
        }
        expect(actions).toBe(1);
        const inspected = yield* nativeCall(a, parent, "preview_status", {
          environmentId: b.environmentId,
          contextThreadId: child,
        });
        expect(inspected.isError, json(inspected.content)).not.toBe(true);
        expect(actions).toBe(1);
        const changed = yield* nativeCall(a, parent, "preview_click", {
          ...input,
          locator: "role=button[name='Different']",
        });
        expect(changed.isError).toBe(true);
        expect(actions).toBe(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "attachments382: registered native and peer launches preserve bytes and one accepted launch after reply loss",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(descriptorOf("attachment-a", "A", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        const b = yield* makeNode(descriptorOf("attachment-b", "B", { peerAuthority: 1 }), "disk", {
          productionKernel: true,
          attachmentRequests: true,
        });
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("attachment-parent");
        yield* seedParent(a, parent);
        const metadata = {
          type: "file",
          name: "sample.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          upload: metadata,
          clientRequestId: "source-upload",
        });
        expect(prepared.isError, json(prepared.content)).not.toBe(true);
        const upload = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ attachmentId: Schema.String, relativeUrl: Schema.String }),
        )(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        const stored = yield* http.execute(
          HttpClientRequest.post(`${a.url}${upload.relativeUrl}`).pipe(
            HttpClientRequest.setBody(HttpBody.uint8Array(new Uint8Array([1, 2, 3, 4]))),
          ),
        );
        expect(stored.status, yield* stored.text).toBe(204);
        const input = {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Attachment fixture",
          message: "Read this file",
          attachments: [{ ...metadata, id: upload.attachmentId }],
          modelSelection: selection,
          clientRequestId: "one-peer-launch",
        };
        yield* Ref.set(a.dropLaunchResponses, 1);
        const lost = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(lost.isError).toBe(true);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const before = yield* sql<{
          thread_id: string;
        }>`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`;
        expect(before, json(lost)).toHaveLength(1);
        const acceptedId = ThreadId.make(before[0]!.thread_id);
        const first = yield* b.threads.getThreadRecords(acceptedId, ["messages", "runs"]);
        expect(first.messages.filter((message) => message.role === "user")).toHaveLength(1);
        const delivered = first.messages.flatMap((message) => message.attachments);
        expect(delivered).toHaveLength(1);
        expect(delivered[0]).toMatchObject({
          mimeType: "application/pdf",
          sizeBytes: 4,
          name: "sample.pdf",
        });
        const path = resolveAttachmentPath({
          attachmentsDir: b.config.attachmentsDir,
          attachment: delivered[0]!,
        });
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(path!))).toEqual([1, 2, 3, 4]);
        // Simulate pending-upload expiry after the actual native commit and
        // lost reply. Native acceptance, not ephemeral pending bytes, owns replay.
        const sourcePending = resolveAttachmentPath({
          attachmentsDir: a.config.attachmentsDir,
          attachment: { ...metadata, type: "file", id: upload.attachmentId },
        });
        yield* fs.remove(sourcePending!);
        const pending = yield* sql<{ id: string }>`SELECT id FROM attachment_staging`;
        for (const row of pending) {
          const pendingPath = resolveAttachmentPathById({
            attachmentsDir: b.config.attachmentsDir,
            attachmentId: row.id,
          });
          if (pendingPath !== null) yield* fs.remove(pendingPath);
        }
        yield* a.stopConnections;
        const reconnected = yield* a.reconnect();
        yield* reconnected.peers.start;
        const retried = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(retried.isError, json(retried.content)).not.toBe(true);
        const receipt = yield* decodeLaunchReceipt(retried.structuredContent);
        expect(receipt.threadId).toBe(acceptedId);
        expect(
          yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id=${b.project.id}`,
        ).toHaveLength(1);
        const after = yield* b.threads.getThreadRecords(acceptedId, ["messages", "runs"]);
        expect(after.messages.map((m) => [m.id, m.attachments])).toEqual(
          first.messages.map((m) => [m.id, m.attachments]),
        );
        expect(after.runs.map((r) => r.id)).toEqual(first.runs.map((r) => r.id));
        // Later independent local edits belong to the accepted file. A replay
        // must consult the native receipt instead of recopying the upload.
        yield* fs.writeFile(path!, new Uint8Array([9, 8, 7, 6]));
        const again = yield* nativeCall(a, parent, "t3_thread_launch", input);
        expect(again.isError, json(again.content)).not.toBe(true);
        expect(Array.from(yield* fs.readFile(path!))).toEqual([9, 8, 7, 6]);
        const reopened = yield* a.reopenDatabase;
        const rebuilt = yield* RequestReceipts.McpRequestReceipts.pipe(
          Effect.provide(RequestReceipts.layer.pipe(Layer.provide(Layer.succeedContext(reopened)))),
        );
        yield* rebuilt.identify(
          caller(a, parent),
          "t3_thread_launch",
          input,
          input.clientRequestId,
        );
        const changed = yield* nativeCall(a, parent, "t3_thread_launch", {
          ...input,
          message: "Different intent",
        });
        expect(changed.isError).toBe(true);
        expect((yield* b.threads.getThreadRecords(acceptedId, ["messages"])).messages).toEqual(
          after.messages,
        );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-viewer: authenticated HTTP binds one source viewer to the exact native opening",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const n = yield* makeNode(
          descriptorOf("native383-viewer-local", "Source", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            deviceOwner: true,
            viewerOwner: true,
          },
        );
        const parent = ThreadId.make("native383-viewer-parent");
        yield* seedParent(n, parent);
        yield* Ref.set(n.deviceHubPlatform, "ios");
        const open = (key: string) =>
          nativeCall(n, parent, "device_open", {
            hostId: "local",
            deviceId: "fixture-device382",
            platform: "ios",
            clientRequestId: key,
          });
        const opened = yield* open("viewer-opening");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const resource = {
          type: "device",
          environmentId: n.environmentId,
          threadId: parent,
          platform: "ios",
          ...target,
        };
        const owner = yield* n.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const reader = yield* n.auth.issueSession({ scopes: ["orchestration:read"] });
        const rival = yield* n.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const post = (token: string, path: string, body: unknown) =>
          http.execute(
            HttpClientRequest.post(`${n.url}${path}`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${token}`),
              HttpClientRequest.setBody(HttpBody.text(json(body), "application/json")),
            ),
          );
        const makeViewer = (token: string, operation: "view" | "control") =>
          post(token, "/api/device-hub/viewer/open", { resource, operation });
        const lawful = yield* makeViewer(owner.token, "control");
        expect(lawful.status, yield* lawful.text).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* lawful.json);
        const mediaPath = `/api/device-hub/vendor/serve-sim/helper/${target.deviceId}/stream.avcc`;
        const media = (ticket: string, path = mediaPath) =>
          http.get(`${n.url}${path}?viewerTicket=${encodeURIComponent(ticket)}`);
        const video = yield* media(viewer.ticket);
        expect(video.status, yield* video.text).toBe(200);
        expect(new Uint8Array(yield* video.arrayBuffer)).toEqual(n.deviceViewerFrame);
        const before = n.deviceViewerRequests.length;
        expect(n.deviceViewerRequests).toEqual([
          "http://device382.invalid/vendor/serve-sim/helper/fixture-device382/stream.avcc",
        ]);
        expect(
          (yield* media(viewer.ticket, mediaPath.replace(target.deviceId, "rival-device"))).status,
        ).toBe(403);
        const refused = yield* makeViewer(reader.token, "control");
        expect(refused.status).toBe(403);
        const viewOnly = yield* makeViewer(reader.token, "view");
        expect(viewOnly.status).toBe(200);
        const readViewer = yield* decodeDeviceViewerAccess(yield* viewOnly.json);
        expect((yield* media(readViewer.ticket)).status).toBe(200);
        const afterRead = n.deviceViewerRequests.length;
        const malformed = yield* post(owner.token, "/api/device-hub/viewer/open", {
          resource: { ...resource, token: "must-not-be-a-public-option" },
          operation: "control",
        });
        expect(malformed.status).toBe(400);
        const agent = yield* n.auth.issueMcpClientSession({
          label: "not a native human",
          access: "full-access",
          peerLink: false,
          client: {
            deviceType: "desktop",
            os: "fixture",
            browser: "fixture",
            ipAddress: "127.0.0.1",
          },
        });
        expect((yield* makeViewer(agent.token, "view")).status).toBe(401);
        expect(n.deviceViewerRequests.length).toBe(afterRead);
        const close = (token: string, viewerId: string) =>
          post(token, "/api/device-hub/viewer/close", { viewerId });
        const rivalClose = yield* close(rival.token, viewer.viewerId);
        expect(yield* rivalClose.json).toBe(false);
        const ownerClose = yield* close(owner.token, viewer.viewerId);
        expect(yield* ownerClose.json).toBe(true);
        expect((yield* media(viewer.ticket)).status).toBe(401);
        expect((yield* media(readViewer.ticket)).status).toBe(200);
        expect((yield* n.fixtureDevice!.sessionsForThread(parent))[0]?.toolSessionId).toBe(
          target.toolSessionId,
        );
        const afterClose = n.deviceViewerRequests.length;
        yield* n.auth.revokeSession(reader.sessionId);
        expect((yield* media(readViewer.ticket)).status).toBe(401);
        expect(n.deviceViewerRequests.length).toBe(afterClose);
        expect(before).toBe(1);
        const currentResponse = yield* makeViewer(owner.token, "view");
        expect(currentResponse.status).toBe(200);
        const current = yield* decodeDeviceViewerAccess(yield* currentResponse.json);
        const closed = yield* nativeCall(n, parent, "device_close", {
          hostId: target.hostId,
          deviceId: target.deviceId,
          clientRequestId: "viewer-tool-close",
        });
        expect(closed.isError, json(closed)).not.toBe(true);
        const afterToolClose = n.deviceViewerRequests.length;
        expect((yield* media(current.ticket)).status).toBe(409);
        expect(n.deviceViewerRequests.length).toBe(afterToolClose);
        const reopened = yield* open("viewer-later-opening");
        expect(reopened.isError, json(reopened)).not.toBe(true);
        const later = yield* decodeTyped383Open(reopened.structuredContent);
        expect(later.target.toolSessionId).not.toBe(target.toolSessionId);
        expect((yield* media(current.ticket)).status).toBe(409);
        expect(n.deviceViewerRequests.length).toBe(afterToolClose);
        const lateClose = yield* close(owner.token, current.viewerId);
        expect(yield* lateClose.json).toBe(true);
        expect((yield* n.fixtureDevice!.sessionsForThread(parent))[0]?.toolSessionId).toBe(
          later.target.toolSessionId,
        );
        const freshResponse = yield* post(owner.token, "/api/device-hub/viewer/open", {
          resource: { ...resource, ...later.target },
          operation: "control",
        });
        expect(freshResponse.status).toBe(200);
        const fresh = yield* decodeDeviceViewerAccess(yield* freshResponse.json);
        const lawfulLater = yield* media(fresh.ticket);
        expect(lawfulLater.status).toBe(200);
        expect(new Uint8Array(yield* lawfulLater.arrayBuffer)).toEqual(n.deviceViewerFrame);
        const afterLater = n.deviceViewerRequests.length;
        const checks = yield* Ref.make(0);
        yield* Ref.set(
          n.deviceViewerCurrentHook,
          Ref.updateAndGet(checks, (value) => value + 1).pipe(
            Effect.flatMap((value) =>
              value === 2
                ? n.fixtureSettings
                    .updateSettings({ enableDeviceSupport: false })
                    .pipe(Effect.asVoid, Effect.orDie)
                : Effect.void,
            ),
          ),
        );
        yield* Effect.result(media(fresh.ticket));
        expect((yield* n.fixtureSettings.getSettings).enableDeviceSupport).toBe(false);
        expect(
          n.deviceViewerRequests.length,
          "no hub dispatch after readiness retired native consent",
        ).toBe(afterLater);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  ...(["subscribe", "extra-target", "consent", "session", "view-input"] as const).map((reason) => ({
    platform: "android" as const,
    reason,
  })),
  ...(["subscribe", "extra-target", "consent", "session"] as const).map((reason) => ({
    platform: "ios" as const,
    reason,
  })),
])(
  "native383-viewer: $platform socket refuses $reason before the external leaf",
  ({ platform, reason }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const n = yield* makeNode(
          descriptorOf("native383-viewer-socket", "Source", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            deviceOwner: true,
            viewerOwner: true,
            viewerSocketOwner: true,
          },
        );
        const parent = ThreadId.make("native383-viewer-socket-parent");
        yield* Ref.set(n.deviceHubPlatform, platform);
        yield* seedParent(n, parent);
        const opened = yield* nativeCall(n, parent, "device_open", {
          hostId: "local",
          deviceId: "fixture-device382",
          platform,
          clientRequestId: "socket-opening",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* n.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const response = yield* http.execute(
          HttpClientRequest.post(`${n.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: n.environmentId,
                    threadId: parent,
                    platform,
                    ...target,
                  },
                  operation: reason === "view-input" ? "view" : "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(response.status).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* response.json);
        const outcome = Promise.withResolvers<"closed" | "forwarded">();
        const first = Promise.withResolvers<void>();
        const lawful =
          platform === "ios"
            ? '\x03{"type":"begin","x":0.25,"y":0.5}'
            : reason === "view-input"
              ? '{"type":"reset-video","ack":true}'
              : '{"type":"touch","action":"down","x":0.25,"y":0.5}';
        const socket = yield* Effect.acquireRelease(
          Effect.sync(
            () =>
              new NodeWS.WebSocket(
                `${n.url.replace(/^http/, "ws")}/api/device-hub/vendor/${platform === "ios" ? "serve-sim/helper" : "serve-emu"}/ws?device=fixture-device382${platform === "android" ? "&frame-meta=1" : ""}&viewerTicket=${encodeURIComponent(viewer.ticket)}`,
              ),
          ),
          (socket) => Effect.sync(() => socket.close()),
        );
        socket.once("error", (error) => {
          first.reject(error);
          outcome.reject(error);
        });
        socket.once("close", () => outcome.resolve("closed"));
        socket.once("open", () =>
          socket.send(platform === "ios" ? new TextEncoder().encode(lawful) : lawful),
        );
        let acknowledged = false;
        socket.on("message", () => {
          if (acknowledged) {
            outcome.resolve("forwarded");
            return;
          }
          acknowledged = true;
          first.resolve();
        });
        yield* Effect.promise(() => first.promise);
        if (reason === "consent")
          yield* n.fixtureSettings.updateSettings({ enableDeviceSupport: false });
        if (reason === "session") yield* n.auth.revokeSession(owner.sessionId);
        const denied =
          platform === "ios"
            ? reason === "subscribe"
              ? '\x10{"requestId":1,"command":{"control":"subscribe","device":"rival-device"}}'
              : reason === "extra-target"
                ? '\x03{"type":"move","x":0.2,"y":0.5,"device":"rival-device"}'
                : '\x03{"type":"move","x":0.2,"y":0.5}'
            : reason === "subscribe"
              ? '{"type":"subscribe","device":"rival-device"}'
              : reason === "extra-target"
                ? '{"type":"touch","action":"down","x":0.2,"y":0.5,"device":"rival-device"}'
                : '{"type":"touch","action":"move","x":0.5,"y":0.5}';
        socket.send(platform === "ios" ? new TextEncoder().encode(denied) : denied);
        expect(yield* Effect.promise(() => outcome.promise)).toBe("closed");
        expect(acknowledged).toBe(true);
        expect(n.deviceViewerInputs).toEqual([lawful]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-viewer: A-only human receives only source-owned access to the permitted B opening",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-remote-viewer",
          false,
          false,
          true,
        );
        yield* Ref.set(b.deviceHubPlatform, "ios");
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...route,
          hostId: "local",
          deviceId: "fixture-device382",
          platform: "ios",
          clientRequestId: "remote-viewer-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const response = yield* http.execute(
          HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: b.environmentId,
                    threadId: child,
                    platform: "ios",
                    ...target,
                  },
                  operation: "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(response.status, yield* response.text).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* response.json);
        expect(viewer.hubBasePath).toBe("/api/device-hub");
        expect(json(viewer)).not.toContain(b.url);
        const media = yield* http.get(
          `${a.url}/api/device-hub/vendor/serve-sim/helper/fixture-device382/stream.avcc?viewerTicket=${encodeURIComponent(viewer.ticket)}`,
        );
        expect(media.status).toBe(200);
        expect(new Uint8Array(yield* media.arrayBuffer)).toEqual(b.deviceViewerFrame);
        expect(a.deviceViewerRequests).toEqual([]);
        expect(b.deviceViewerRequests).toEqual([
          "http://device382.invalid/vendor/serve-sim/helper/fixture-device382/stream.avcc",
        ]);
        const path = "/api/device-hub/vendor/serve-sim/helper/fixture-device382/stream.avcc";
        const mediaAt = (origin: string, ticket: string, requestedPath = path) =>
          http.get(`${origin}${requestedPath}?viewerTicket=${encodeURIComponent(ticket)}`);
        const before = b.deviceViewerRequests.length;
        expect(
          (yield* mediaAt(a.url, viewer.ticket, path.replace("fixture-device382", "rival-device")))
            .status,
        ).toBe(403);
        expect((yield* mediaAt(b.url, viewer.ticket)).status).toBe(401);
        expect(b.deviceViewerRequests.length).toBe(before);
        const grant = (yield* b.authority.grants)[0]!;
        const checks = yield* Ref.make(0);
        yield* Ref.set(
          b.deviceViewerCurrentHook,
          Ref.updateAndGet(checks, (n) => n + 1).pipe(
            Effect.flatMap((n) =>
              n === 2
                ? b.authority
                    .setGrant({ ...grant, device: false })
                    .pipe(Effect.asVoid, Effect.orDie)
                : Effect.void,
            ),
          ),
        );
        yield* Effect.result(mediaAt(a.url, viewer.ticket));
        expect((yield* b.authority.grants)[0]!.device).toBe(false);
        expect(
          b.deviceViewerRequests.length,
          "no hub dispatch after readiness retired the real grant",
        ).toBe(before);
        yield* Ref.set(b.deviceViewerCurrentHook, Effect.void);
        yield* b.authority.setGrant({ ...grant, device: true });
        yield* b.authority.setGrant({ ...grant, device: false });
        expect((yield* mediaAt(a.url, viewer.ticket)).status).toBe(403);
        expect(b.deviceViewerRequests.length).toBe(before);
        yield* b.authority.setGrant({ ...grant, device: true });
        const resumed = yield* mediaAt(a.url, viewer.ticket);
        expect(resumed.status).toBe(200);
        expect(new Uint8Array(yield* resumed.arrayBuffer)).toEqual(b.deviceViewerFrame);
        const afterResume = b.deviceViewerRequests.length;
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        expect((yield* mediaAt(a.url, viewer.ticket)).status).toBe(403);
        expect(b.deviceViewerRequests.length).toBe(afterResume);
        const enrolled = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(enrolled.isError, json(enrolled)).not.toBe(true);
        const replacement = (yield* b.authority.grants).find(
          (current) => current.sessionId !== grant.sessionId,
        )!;
        expect(replacement).toBeDefined();
        yield* b.authority.setGrant({ ...replacement, device: true });
        const reopenViewer = () =>
          http.execute(
            HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
              HttpClientRequest.setBody(
                HttpBody.text(
                  json({
                    resource: {
                      type: "device",
                      environmentId: b.environmentId,
                      threadId: child,
                      platform: "ios",
                      ...target,
                    },
                    operation: "control",
                  }),
                  "application/json",
                ),
              ),
            ),
          );
        // The replacement created-work grant does not own the old session's context.
        expect((yield* reopenViewer()).status).toBe(403);
        expect(b.deviceViewerRequests.length).toBe(afterResume);
        yield* b.authority.setGrant({
          ...replacement,
          device: true,
          resourceScope: "environment-resources",
        });
        const freshResponse = yield* reopenViewer();
        expect(freshResponse.status).toBe(200);
        const fresh = yield* decodeDeviceViewerAccess(yield* freshResponse.json);
        expect(fresh.viewerId).not.toBe(viewer.viewerId);
        const freshMedia = yield* mediaAt(a.url, fresh.ticket);
        expect(freshMedia.status).toBe(200);
        expect(new Uint8Array(yield* freshMedia.arrayBuffer)).toEqual(b.deviceViewerFrame);
        const original = yield* a.fixtureViewerSessions!.authenticateViewerToken(fresh.ticket);
        if (original.resource.type !== "device")
          return yield* Effect.die("Wrong fixture viewer resource");
        const privateDestination = yield* a.fixtureViewers!.resolve(
          fresh.viewerId,
          {
            resource: original.resource,
            operation: original.operation,
          },
          original.generation,
        );
        expect(privateDestination.ticket).toBeDefined();
        expect(json(fresh)).not.toContain(privateDestination.ticket!);
        yield* b.fixtureViewerSessions!.authenticateViewerToken(privateDestination.ticket!);
        const closed = yield* http.execute(
          HttpClientRequest.post(`${a.url}/api/device-hub/viewer/close`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(json({ viewerId: fresh.viewerId }), "application/json"),
            ),
          ),
        );
        expect(closed.status).toBe(200);
        expect(
          yield* b
            .fixtureViewerSessions!.authenticateViewerToken(privateDestination.ticket!)
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure" });
        const finalResponse = yield* reopenViewer();
        expect(finalResponse.status).toBe(200);
        const finalViewer = yield* decodeDeviceViewerAccess(yield* finalResponse.json);
        const finalCount = b.deviceViewerRequests.length;
        yield* a.auth.revokeSession(owner.sessionId);
        expect((yield* mediaAt(a.url, finalViewer.ticket)).status).toBe(401);
        expect(b.deviceViewerRequests.length).toBe(finalCount);
        expect(a.deviceViewerRequests).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-viewer: old device peer refuses authenticated viewer admission without new destination effects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, child, open, footprint } = yield* typed383Pair(
          "native383-old-viewer",
          false,
          false,
          { source: true, destination: false },
        );
        const opened = yield* open("old-viewer-opening");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const before = yield* footprint();
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const response = yield* http.execute(
          HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: b.environmentId,
                    threadId: child,
                    platform: "android",
                    ...target,
                  },
                  operation: "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(response.status).toBe(403);
        expect(yield* response.json).toMatchObject({ reason: "device_viewer_unsupported" });
        expect(yield* footprint()).toEqual(before);
        expect(a.deviceViewerRequests).toEqual([]);
        expect(b.deviceViewerRequests).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-viewer: source authenticated RPC admits qualified B view and keeps control guarded",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const {
          a,
          b,
          child,
          parent,
          route: destination,
        } = yield* typed383Pair("native383-viewer-rpc", false, false, true);
        yield* Ref.set(b.deviceHubPlatform, "ios");
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...destination,
          hostId: "local",
          deviceId: "fixture-device382",
          platform: "ios",
          clientRequestId: "rpc-viewer-device-opening",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const resource = {
          type: "device" as const,
          environmentId: b.environmentId,
          threadId: child,
          platform: "ios" as const,
          ...target,
        };
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
            yield* RpcServer.make(DeviceViewerClientRpcGroup, {
              disableTracing: true,
              disableFatalDefects: true,
            }).pipe(
              Effect.provideService(RpcServer.Protocol, protocol),
              Effect.provide(
                Layer.merge(
                  DeviceViewerClientRpcGroup.toLayer(
                    DeviceHubProxy.clientHandlers(session, a.fixtureViewers!),
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
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const reader = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const connect = (token: string) =>
          Effect.gen(function* () {
            const session = yield* a.auth.authenticateHttpRequest(
              HttpServerRequest.fromWeb(
                new Request(a.url, {
                  headers: { authorization: `Bearer ${token}` },
                }),
              ),
            );
            const ticket = yield* a.auth.issueWebSocketTicket(session);
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
                      () => reject(new Error("Native viewer socket refused")),
                      { once: true },
                    );
                  }),
                catch: () =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "Native viewer socket failed",
                  }),
              }),
              (socket) => Effect.sync(() => socket.close()),
            );
            let ordinal = 0;
            return (tag: string, payload: unknown) =>
              Effect.gen(function* () {
                const id = String(++ordinal);
                socket.send(json({ _tag: "Request", id, tag, payload, headers: [] }));
                const reply = yield* decodePendingWsReply(yield* Queue.take(messages));
                expect(reply.requestId).toBe(id);
                return reply;
              });
          });
        const rpc = yield* connect(owner.token);
        const readonlyRpc = yield* connect(reader.token);
        const viewed = yield* readonlyRpc(WS_METHODS.deviceViewerOpen, {
          resource,
          operation: "view",
        });
        expect(viewed.exit?._tag).toBe("Success");
        const readViewer = yield* decodeDeviceViewerAccess(viewed.exit?.value);
        const controlRefused = yield* readonlyRpc(WS_METHODS.deviceViewerOpen, {
          resource,
          operation: "control",
        });
        expect(controlRefused.exit?._tag).toBe("Failure");
        expect(json(controlRefused)).toContain("orchestration:operate");
        const controlled = yield* rpc(WS_METHODS.deviceViewerOpen, {
          resource,
          operation: "control",
        });
        expect(controlled.exit?._tag).toBe("Success");
        const viewer = yield* decodeDeviceViewerAccess(controlled.exit?.value);
        expect(json(viewer)).not.toContain(b.url);
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const media = yield* http.get(
          `${a.url}/api/device-hub/vendor/serve-sim/helper/fixture-device382/stream.avcc?viewerTicket=${encodeURIComponent(viewer.ticket)}`,
        );
        expect(media.status).toBe(200);
        expect(new Uint8Array(yield* media.arrayBuffer)).toEqual(b.deviceViewerFrame);
        const rivalClose = yield* readonlyRpc(WS_METHODS.deviceViewerClose, {
          viewerId: viewer.viewerId,
        });
        expect(rivalClose.exit?.value).toBe(false);
        const closed = yield* rpc(WS_METHODS.deviceViewerClose, { viewerId: viewer.viewerId });
        expect(closed.exit?.value).toBe(true);
        expect(
          (yield* http.get(
            `${a.url}/api/device-hub/vendor/serve-sim/helper/fixture-device382/stream.avcc?viewerTicket=${encodeURIComponent(viewer.ticket)}`,
          )).status,
        ).toBe(401);
        expect(
          (yield* readonlyRpc(WS_METHODS.deviceViewerClose, { viewerId: readViewer.viewerId })).exit
            ?.value,
        ).toBe(true);
        expect(a.deviceViewerRequests).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  { platform: "ios", retirement: "grant" },
  { platform: "android", retirement: "grant" },
  { platform: "ios", retirement: "session" },
  { platform: "android", retirement: "session" },
] as const)(
  "native383-viewer: $platform source socket keeps both peer hops bound after actual $retirement retirement",
  ({ platform, retirement }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, child, parent, route } = yield* typed383Pair(
          `native383-two-hop-${platform}`,
          false,
          false,
          { source: true, destination: true, destinationSocket: true },
        );
        yield* Ref.set(b.deviceHubPlatform, platform);
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...route,
          hostId: "local",
          deviceId: "fixture-device382",
          platform,
          clientRequestId: "two-hop-opening",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = HttpClient.withScope(yield* HttpClient.HttpClient);
        const admitted = yield* http.execute(
          HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: b.environmentId,
                    threadId: child,
                    platform,
                    ...target,
                  },
                  operation: "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(admitted.status).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* admitted.json);
        const media = yield* Queue.unbounded<Uint8Array>();
        if (platform === "ios") {
          yield* Ref.set(b.continuousDeviceMedia, true);
          yield* Queue.offer(b.deviceMediaChunks, b.deviceViewerFrame);
          const response = yield* http.get(
            `${a.url}/api/device-hub/vendor/serve-sim/helper/fixture-device382/stream.avcc?viewerTicket=${encodeURIComponent(viewer.ticket)}`,
          );
          expect(response.status).toBe(200);
          yield* response.stream.pipe(
            Stream.runForEach((chunk) => Queue.offer(media, chunk)),
            Effect.forkScoped,
          );
          expect(Array.from(yield* Queue.take(media))).toEqual(Array.from(b.deviceViewerFrame));
        }
        const first = Promise.withResolvers<void>();
        let outcome = Promise.withResolvers<"closed" | "forwarded">();
        const initial =
          platform === "ios"
            ? '\x03{"type":"begin","x":0.25,"y":0.5}'
            : '{"type":"touch","action":"down","x":0.25,"y":0.5}';
        const next =
          platform === "ios"
            ? '\x03{"type":"move","x":0.5,"y":0.5}'
            : '{"type":"touch","action":"move","x":0.5,"y":0.5}';
        const socket = yield* Effect.acquireRelease(
          Effect.sync(
            () =>
              new NodeWS.WebSocket(
                `${a.url.replace(/^http/, "ws")}/api/device-hub/vendor/${platform === "ios" ? "serve-sim/helper" : "serve-emu"}/ws?device=fixture-device382&viewerTicket=${encodeURIComponent(viewer.ticket)}`,
              ),
          ),
          (socket) => Effect.sync(() => socket.close()),
        );
        socket.once("error", (error) => {
          first.reject(error);
          outcome.reject(error);
        });
        socket.once("close", () => outcome.resolve("closed"));
        socket.once("open", () =>
          socket.send(platform === "ios" ? new TextEncoder().encode(initial) : initial),
        );
        let received = false;
        socket.on("message", () => {
          if (received) outcome.resolve("forwarded");
          else {
            received = true;
            first.resolve();
          }
        });
        yield* Effect.promise(() => first.promise);
        expect(b.deviceViewerInputs).toEqual([initial]);
        expect(a.deviceViewerInputs).toEqual([]);
        yield* TestClock.adjust("65 seconds");
        if (platform === "ios") {
          yield* Queue.offer(b.deviceMediaChunks, b.deviceViewerFrame);
          expect(
            Array.from(yield* Queue.take(media)),
            "same AVCC response carries frames after admission expiry",
          ).toEqual(Array.from(b.deviceViewerFrame));
        }
        socket.send(platform === "ios" ? new TextEncoder().encode(next) : next);
        expect(
          yield* Effect.promise(() => outcome.promise),
          "authorized control must survive admission expiry",
        ).toBe("forwarded");
        const screenshotUrl = (ticket: string) =>
          `${a.url}/api/device-hub/vendor/${platform === "ios" ? "serve-sim" : "serve-emu"}/api/screenshot?device=fixture-device382&viewerTicket=${encodeURIComponent(ticket)}`;
        const screenshotsBefore = yield* Ref.get(b.deviceScreenshotCalls);
        expect(
          (yield* http.execute(HttpClientRequest.post(screenshotUrl(viewer.ticket)))).status,
        ).toBe(401);
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(screenshotsBefore);
        const resource = {
          type: "device" as const,
          environmentId: b.environmentId,
          threadId: child,
          platform,
          ...target,
        };
        const renew = () =>
          http.execute(
            HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
              HttpClientRequest.setBody(
                HttpBody.text(
                  json({ resource, operation: "control", viewerId: viewer.viewerId }),
                  "application/json",
                ),
              ),
            ),
          );
        const renewedResponse = yield* renew();
        expect(renewedResponse.status, yield* renewedResponse.text).toBe(200);
        const renewed = yield* decodeDeviceViewerAccess(yield* renewedResponse.json);
        expect(renewed.viewerId).toBe(viewer.viewerId);
        const shot = yield* http.execute(HttpClientRequest.post(screenshotUrl(renewed.ticket)));
        expect(
          shot.status,
          json({
            body: shot.status === 200 ? "" : yield* shot.text,
            calls: yield* Ref.get(b.deviceScreenshotCalls),
          }),
        ).toBe(200);
        expect(Array.from(new Uint8Array(yield* shot.arrayBuffer))).toEqual(
          Array.from(
            Buffer.from(
              "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
              "base64",
            ),
          ),
        );
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(screenshotsBefore + 1);
        outcome = Promise.withResolvers<"closed" | "forwarded">();
        const grant = (yield* b.authority.grants)[0]!;
        if (retirement === "grant") yield* b.authority.setGrant({ ...grant, device: false });
        else yield* a.auth.revokeSession(owner.sessionId);
        expect((yield* renew()).status).toBe(retirement === "grant" ? 403 : 401);
        expect(
          (yield* http.execute(HttpClientRequest.post(screenshotUrl(renewed.ticket)))).status,
        ).toBe(retirement === "grant" ? 403 : 401);
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(screenshotsBefore + 1);
        socket.send(platform === "ios" ? new TextEncoder().encode(next) : next);
        expect(yield* Effect.promise(() => outcome.promise)).toBe("closed");
        expect(b.deviceViewerInputs).toEqual([initial, next]);
        expect(a.deviceViewerInputs).toEqual([]);
      }),
    ).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, TestClock.layer())),
    ),
  30_000,
);

it.live(
  "native383-browser: original grant retirement during element geometry prevents the later click",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-browser-page-wait",
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          {
            ...route,
            url: "https://example.test/",
            show: false,
            clientRequestId: "browser-page-wait-opening",
          },
          capabilities,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const status = yield* Schema.decodeUnknownEffect(Schema.Struct({ tabId: Schema.String }))(
          opened.structuredContent,
        );
        const external = browserExternalContexts.at(-1)!;
        const click = (key: string) =>
          nativeCall(
            a,
            parent,
            "preview_click",
            {
              ...route,
              tabId: status.tabId,
              selector: "#button",
              clientRequestId: key,
            },
            capabilities,
            credential,
          );
        const lawful = yield* click("browser-page-wait-lawful");
        expect(lawful.isError, json(lawful)).not.toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(1);
        external.controls.beforeBounds = async () => {
          await nativePromise(b.authority.setGrant({ ...grant, preview: false }));
        };
        const retired = yield* click("browser-page-wait-retired");
        expect(
          retired.isError,
          json({
            retired,
            effects: external.effects,
            preview: (yield* b.authority.grants)[0]?.preview,
          }),
        ).toBe(true);
        expect((yield* b.authority.grants)[0]?.preview).toBe(false);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(1);
        expect(json(retired)).toContain("outcome_unknown");
        external.controls.beforeBounds = async () => {};
        yield* b.authority.setGrant({ ...grant, preview: true });
        const replay = yield* click("browser-page-wait-retired");
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser: modern opener survives authenticated reads renewal and transport reconstruction without borrowing rival ownership",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-browser-identity",
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const capabilities = new Set(["orchestration", "preview", "device"] as const);
        let credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        const call = (tool: string, input: Record<string, unknown>) =>
          nativeCall(a, parent, tool, { ...route, ...input }, capabilities, credential);
        const opened = yield* call("preview_open", {
          url: "https://example.test/",
          show: false,
          clientRequestId: "browser-identity-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const status = yield* Schema.decodeUnknownEffect(Schema.Struct({ tabId: Schema.String }))(
          opened.structuredContent,
        );
        const external = browserExternalContexts.at(-1)!;
        const intent = {
          tabId: status.tabId,
          selector: "#button",
          clientRequestId: "browser-identity-click",
        };
        expect((yield* call("preview_click", intent)).isError).not.toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(1);
        const sourceDb = Context.get(a.database, SqlClient.SqlClient);
        const destinationDb = Context.get(b.database, SqlClient.SqlClient);
        const receiptsBeforeRead = [
          yield* sourceDb`SELECT id,state FROM mcp_request_receipts ORDER BY id`,
          yield* destinationDb`SELECT id,state FROM mcp_request_receipts ORDER BY id`,
        ];
        const read = yield* call("preview_status", { tabId: status.tabId });
        expect(read.isError, json(read)).not.toBe(true);
        expect(read.structuredContent).toMatchObject({
          tabId: status.tabId,
          control: { ownedByCaller: true },
        });
        expect([
          yield* sourceDb`SELECT id,state FROM mcp_request_receipts ORDER BY id`,
          yield* destinationDb`SELECT id,state FROM mcp_request_receipts ORDER BY id`,
        ]).toEqual(receiptsBeforeRead);
        credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities,
        });
        expect(
          (yield* call("preview_status", { tabId: status.tabId })).structuredContent,
        ).toMatchObject({ control: { ownedByCaller: true } });
        expect(
          (yield* call("preview_click", { ...intent, clientRequestId: "browser-identity-renewed" }))
            .isError,
        ).not.toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(2);
        const reconstructed = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* sourceDb`UPDATE peer_environment_links SET urls_json=${json([reconstructed.url])} WHERE environment_id=${b.environmentId}`;
        expect(
          (yield* call("preview_status", { tabId: status.tabId })).structuredContent,
        ).toMatchObject({ control: { ownedByCaller: true } });
        expect(
          (yield* call("preview_click", {
            ...intent,
            clientRequestId: "browser-identity-reconstructed",
          })).isError,
        ).not.toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(3);
        expect(
          (yield* call("preview_click", { ...intent, selector: "#changed-intent" })).isError,
        ).toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(3);
        const rival = ThreadId.make("native383-browser-identity-rival");
        yield* seedParent(a, rival);
        for (const key of [intent.clientRequestId, "browser-identity-rival-fresh"])
          expect(
            (yield* nativeCall(a, rival, "preview_click", {
              ...route,
              ...intent,
              clientRequestId: key,
            })).isError,
          ).toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(3);
        yield* b.authority.setGrant({ ...grant, preview: false });
        for (const key of [intent.clientRequestId, "browser-identity-revoked-fresh"])
          expect((yield* call("preview_click", { ...intent, clientRequestId: key })).isError).toBe(
            true,
          );
        expect((yield* call("preview_status", { tabId: status.tabId })).isError).toBe(true);
        expect(external.effects.filter((effect) => effect === "click")).toHaveLength(3);
        yield* b.authority.setGrant({ ...grant, preview: true });
        const old = { ...b, ...(yield* b.restartWithoutRequestsTransport) };
        yield* sourceDb`UPDATE peer_environment_links SET urls_json=${json([old.url])} WHERE environment_id=${b.environmentId}`;
        const beforeOld = [...external.effects];
        const beforeContexts = browserExternalContexts.length;
        for (const tool of ["preview_status", "preview_open"])
          expect(
            (yield* call(
              tool,
              tool === "preview_status"
                ? { tabId: status.tabId }
                : { show: false, clientRequestId: "browser-old-peer-open" },
            )).isError,
          ).toBe(true);
        expect(external.effects).toEqual(beforeOld);
        expect(browserExternalContexts).toHaveLength(beforeContexts);
        yield* sourceDb`UPDATE peer_environment_links SET urls_json=${json([reconstructed.url])} WHERE environment_id=${b.environmentId}`;
        expect(
          (yield* call("t3_preview_close", {
            tabId: status.tabId,
            clientRequestId: "browser-identity-close",
          })).isError,
        ).not.toBe(true);
        const next = yield* call("preview_open", {
          show: false,
          clientRequestId: "browser-identity-reopen",
        });
        expect(next.isError, json(next)).not.toBe(true);
        const nextStatus = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ tabId: Schema.String }),
        )(next.structuredContent);
        expect(nextStatus.tabId).not.toBe(status.tabId);
        expect(
          (yield* call("t3_preview_close", {
            tabId: status.tabId,
            clientRequestId: "browser-identity-close",
          })).isError,
        ).not.toBe(true);
        expect(
          (yield* b.nativePreviewManager!.list({ threadId: child })).sessions.map(
            (session) => session.tabId,
          ),
        ).toContain(nextStatus.tabId);
        expect(
          (yield* call("preview_click", {
            ...intent,
            clientRequestId: "browser-identity-stale-tab",
          })).isError,
        ).toBe(true);
        const oldLink = yield* a.links.resolve(b.environmentId);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        expect((yield* a.links.resolve(b.environmentId)).link.launchIdentity).not.toBe(
          oldLink.link.launchIdentity,
        );
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const replacement = (yield* b.authority.grants).find(
          (value) => value.sessionId !== grant.sessionId,
        );
        if (replacement === undefined)
          return yield* Effect.die("Expected actual replacement grant");
        yield* b.authority.setGrant({ ...replacement, preview: true });
        expect(
          (yield* call("preview_click", {
            ...intent,
            tabId: nextStatus.tabId,
            clientRequestId: "browser-identity-old-generation",
          })).isError,
        ).toBe(true);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "New browser generation",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "browser-identity-generation-child",
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const newChild = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, newChild);
        const replacementRoute = { environmentId: b.environmentId, contextThreadId: newChild };
        const legitimate = yield* nativeCall(a, parent, "preview_open", {
          ...replacementRoute,
          show: false,
          clientRequestId: "browser-generation-lawful-open",
        });
        expect(legitimate.isError, json(legitimate)).not.toBe(true);
        const finalStatus = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ tabId: Schema.String }),
        )(legitimate.structuredContent);
        expect(
          (yield* nativeCall(a, parent, "preview_click", {
            ...replacementRoute,
            tabId: finalStatus.tabId,
            selector: "#button",
            clientRequestId: "browser-generation-lawful-click",
          })).isError,
        ).not.toBe(true);
        expect(
          (yield* nativeCall(a, parent, "preview_click", {
            ...replacementRoute,
            tabId: nextStatus.tabId,
            selector: "#button",
            clientRequestId: "browser-generation-old-tab",
          })).isError,
        ).toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["layout", "clear"] as const)(
  "native383-browser: actual grant retirement during %s prevents later capture or input dispatch",
  (boundary) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          `native383-browser-${boundary}`,
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: `browser-${boundary}-open`,
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { tabId } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ tabId: Schema.String }),
        )(opened.structuredContent);
        const external = browserExternalContexts.at(-1)!;
        const tool = boundary === "layout" ? "preview_snapshot" : "preview_type";
        const input =
          boundary === "layout"
            ? { ...route, tabId, includeImage: false }
            : {
                ...route,
                tabId,
                text: "literal",
                clear: true,
                clientRequestId: "browser-clear-lawful",
              };
        const lawful = yield* nativeCall(a, parent, tool, input);
        expect(lawful.isError, json(lawful)).not.toBe(true);
        const before = [...external.effects];
        const retire = async () => {
          await nativePromise(b.authority.setGrant({ ...grant, preview: false }));
        };
        if (boundary === "layout") external.controls.beforeLayout = retire;
        else external.controls.afterKey = retire;
        const retiredInput =
          boundary === "layout" ? input : { ...input, clientRequestId: "browser-clear-retired" };
        const denied = yield* nativeCall(a, parent, tool, retiredInput);
        expect(denied.isError, json(denied)).toBe(true);
        expect((yield* b.authority.grants)[0]?.preview).toBe(false);
        expect(external.effects).toEqual(
          boundary === "layout" ? before : [...before, "key:ControlOrMeta+A"],
        );
        external.controls.beforeLayout = async () => {};
        external.controls.afterKey = async () => {};
        yield* b.authority.setGrant({ ...grant, preview: true });
        if (boundary === "clear") {
          const replay = yield* nativeCall(a, parent, tool, retiredInput);
          expect(replay.isError, json(replay)).toBe(true);
          expect(json(replay)).toContain("outcome_unknown");
          expect(external.effects).toEqual([...before, "key:ControlOrMeta+A"]);
        } else {
          expect((yield* nativeCall(a, parent, tool, input)).isError).not.toBe(true);
          expect(external.effects.filter((effect) => effect === "capture")).toHaveLength(2);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser: original grant retirement during context preparation prevents page creation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-browser-context-wait",
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            browserExternalBoundary.beforeContextReturn = async () => {};
          }),
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const open = (key: string) =>
          nativeCall(a, parent, "preview_open", {
            ...route,
            url: "https://example.test/",
            show: false,
            reuseExistingTab: false,
            clientRequestId: key,
          });
        const lawful = yield* open("browser-context-lawful");
        expect(lawful.isError, json(lawful)).not.toBe(true);
        expect(browserExternalContexts.at(-1)!.effects).toContain("navigate");
        browserExternalBoundary.beforeContextReturn = async () => {
          await nativePromise(b.authority.setGrant({ ...grant, preview: false }));
        };
        const denied = yield* open("browser-context-retired");
        expect((yield* b.authority.grants)[0]?.preview).toBe(false);
        expect(browserExternalContexts.at(-1)!.effects, json(denied)).not.toContain("new-page");
        expect(denied.isError, json(denied)).toBe(true);
        expect(json(denied)).toContain("outcome_unknown");
        browserExternalBoundary.beforeContextReturn = async () => {};
        yield* b.authority.setGrant({ ...grant, preview: true });
        const replay = yield* open("browser-context-retired");
        expect(replay.isError, json(replay)).toBe(true);
        expect(json(replay)).toContain("outcome_unknown");
        expect(browserExternalContexts.at(-1)!.effects).not.toContain("new-page");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser: native human session retirement during viewer attachment prevents private capture",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-browser-native-view",
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "browser-human-view-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { tabId } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ tabId: Schema.String }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const nativeSession = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const authRepository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const authorization = yield* Auth.nativeSessionAuthorization(
          nativeSession,
          "orchestration:read",
        ).pipe(Effect.provideContext(authRepository));
        const input = {
          threadId: child,
          tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: false,
          authorize: () => nativePromise(authorization),
        };
        yield* Effect.scoped(b.nativeBrowser!.attachViewer(input));
        const external = browserExternalContexts.at(-1)!;
        expect(external.effects.filter((effect) => effect === "capture")).toHaveLength(1);
        external.controls.beforeSession = async () => {
          await nativePromise(
            Effect.gen(function* () {
              yield* Context.get(authRepository, AuthSessions.AuthSessionRepository).revoke({
                sessionId: issued.sessionId,
                revokedAt: yield* DateTime.now,
              });
            }),
          );
        };
        const retired = yield* Effect.scoped(b.nativeBrowser!.attachViewer(input)).pipe(
          Effect.result,
        );
        expect((yield* authorization.pipe(Effect.result))._tag).toBe("Failure");
        expect(retired._tag, json({ result: retired._tag, effects: external.effects })).toBe(
          "Failure",
        );
        expect(external.effects.filter((effect) => effect === "capture")).toHaveLength(1);
        expect((yield* authorization.pipe(Effect.result))._tag).toBe("Failure");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser-viewer: A-only native session streams B JPEG controls exact viewer lifecycle and stops input after grant retirement",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        yield* TestClock.withLive(Clock.currentTimeMillis).pipe(Effect.flatMap(TestClock.setTime));
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-browser-viewer-loop",
          false,
          false,
          {
            source: false,
            destination: false,
            destinationBrowser: true,
          },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          url: "https://example.test/",
          show: false,
          clientRequestId: "browser-viewer-tool-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const connectRpc = (session: Auth.AuthenticatedSession) =>
          Effect.gen(function* () {
            const ticket = yield* a.auth.issueWebSocketTicket(session);
            const messages = yield* Queue.unbounded<string>();
            const socket = yield* Effect.acquireRelease(
              Effect.tryPromise({
                try: () =>
                  new Promise<WebSocket>((resolve, reject) => {
                    const ws = new WebSocket(
                      `${a.nativeViewerRpcUrl}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`,
                    );
                    ws.addEventListener("message", (event) =>
                      Queue.offerUnsafe(messages, String(event.data)),
                    );
                    ws.addEventListener("open", () => resolve(ws), { once: true });
                    ws.addEventListener(
                      "error",
                      () => reject(new Error("Native browser viewer RPC refused")),
                      { once: true },
                    );
                  }),
                catch: (cause) =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: String(cause),
                  }),
              }),
              (socket) => Effect.sync(() => socket.close()),
            );
            let ordinal = 0;
            return (tag: string, payload: unknown) =>
              Effect.gen(function* () {
                const id = String(++ordinal);
                socket.send(json({ _tag: "Request", id, tag, payload, headers: [] }));
                const reply = yield* decodePendingWsReply(yield* Queue.take(messages));
                expect(reply.requestId).toBe(id);
                return reply;
              });
          });
        const rpc = yield* connectRpc(native);
        const readOwner = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const readNative = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${readOwner.token}` } }),
          ),
        );
        const readRpc = yield* connectRpc(readNative);
        const sourceDb = Context.get(a.database, SqlClient.SqlClient);
        const beforeRefusal =
          yield* sourceDb`SELECT id,record_json FROM peer_authority_records ORDER BY id`;
        const forbiddenControl = yield* readRpc(WS_METHODS.previewViewerOpen, {
          resource,
          operation: "control",
        });
        expect(forbiddenControl.exit?._tag).toBe("Failure");
        expect(json(forbiddenControl)).toContain("preview:operate");
        expect(
          yield* sourceDb`SELECT id,record_json FROM peer_authority_records ORDER BY id`,
        ).toEqual(beforeRefusal);
        const permittedView = yield* readRpc(WS_METHODS.previewViewerOpen, {
          resource,
          operation: "view",
        });
        expect(permittedView.exit?._tag).toBe("Success");
        const readViewer = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(
          permittedView.exit?.value,
        );
        expect(
          (yield* readRpc(WS_METHODS.previewViewerClose, { viewerId: readViewer.viewerId })).exit
            ?.value,
        ).toBe(true);
        const admitted = yield* rpc(WS_METHODS.previewViewerOpen, {
          resource,
          operation: "control",
        });
        expect(admitted.exit?._tag, json(admitted)).toBe("Success");
        const viewer = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(admitted.exit?.value);
        expect(viewer.streamBasePath).toBe("/api/preview-stream");
        expect(json(viewer)).not.toContain(b.url);
        const signed = yield* a.fixtureBrowserViewerSessions!.authenticateViewerToken(
          viewer.ticket,
        );
        const resolved = yield* a.fixtureBrowserViewers!.resolve(
          viewer.viewerId,
          { resource, operation: "control" },
          signed.generation,
        );
        if (resolved.type !== "peer") return yield* Effect.die("Expected private peer viewer");
        expect(json(viewer)).not.toContain(resolved.ticket);
        yield* b.fixtureBrowserViewerSessions!.authenticateViewerToken(resolved.ticket);
        const frame = Promise.withResolvers<Uint8Array>();
        const acquired = Promise.withResolvers<void>();
        let pickerOffered = Promise.withResolvers<string>();
        const downloadOffered = Promise.withResolvers<string>();
        const pickerCanceled = Promise.withResolvers<void>();
        let cancelPickerId = "";
        const closed = Promise.withResolvers<void>();
        const decodeControl = Schema.decodeUnknownOption(
          Schema.fromJsonString(
            Schema.Struct({
              type: Schema.String,
              controller: Schema.optionalKey(Schema.String),
              id: Schema.optionalKey(Schema.String),
            }),
          ),
        );
        const params = new URLSearchParams({
          threadId: child,
          tabId: resource.tabId,
          viewerTicket: viewer.ticket,
        });
        const socket = yield* Effect.acquireRelease(
          Effect.tryPromise({
            try: () =>
              new Promise<WebSocket>((resolve, reject) => {
                const ws = new WebSocket(
                  `${a.url.replace(/^http/, "ws")}/api/preview-stream/ws?${params}`,
                );
                ws.binaryType = "arraybuffer";
                ws.addEventListener("message", (event) => {
                  if (typeof event.data !== "string")
                    frame.resolve(new Uint8Array(event.data as ArrayBuffer));
                  else {
                    const control = decodeControl(event.data);
                    if (
                      Option.isSome(control) &&
                      control.value.type === "fileChooserClosed" &&
                      control.value.id === cancelPickerId
                    )
                      pickerCanceled.resolve();
                    if (
                      Option.isSome(control) &&
                      control.value.type === "download" &&
                      control.value.id !== undefined
                    )
                      downloadOffered.resolve(control.value.id);
                    if (
                      Option.isSome(control) &&
                      control.value.type === "fileChooser" &&
                      control.value.id !== undefined
                    )
                      pickerOffered.resolve(control.value.id);
                    if (
                      Option.isSome(control) &&
                      control.value.type === "control" &&
                      control.value.controller === "you"
                    )
                      acquired.resolve();
                  }
                });
                ws.addEventListener("open", () => resolve(ws), { once: true });
                ws.addEventListener(
                  "error",
                  () => reject(new Error("Source browser viewer refused")),
                  { once: true },
                );
                ws.addEventListener("close", () => closed.resolve(), { once: true });
              }),
            catch: (cause) =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: String(cause),
              }),
          }),
          (ws) => Effect.sync(() => ws.close()),
        );
        const jpeg = yield* Effect.promise(() => frame.promise);
        expect(jpeg.byteLength).toBe(773);
        expect(yield* typed383Digest(jpeg)).toBe(
          "030b4f6d1c87d5d9ec0d9dd75721bf0137a6948d2719d6d3e7f5a6d273926acb",
        );
        expect(Array.from(jpeg.subarray(0, 2))).toEqual([255, 216]);
        expect(Array.from(jpeg.subarray(-2))).toEqual([255, 217]);
        socket.send(json({ type: "ack" }));
        socket.send(json({ type: "takeControl" }));
        yield* Effect.promise(() => acquired.promise);
        const external = browserExternalContexts.at(-1)!;
        const delivered = Promise.withResolvers<void>();
        external.controls.afterInput = async (method) => {
          if (method === "Input.insertText") delivered.resolve();
        };
        socket.send(json({ type: "text", text: "literal source A" }));
        yield* Effect.promise(() => delivered.promise);
        expect(external.inputs.filter((input) => input.method === "Input.insertText")).toEqual([
          { method: "Input.insertText", params: { text: "literal source A" } },
        ]);
        yield* TestClock.adjust("65 seconds");
        const filePayloads: Array<{ name: string; mimeType: string; bytes: number[] }> = [];
        const chooser = makeExternalBrowserFileChooser<
          Parameters<
            NativeBrowser.ServerBrowser["Service"]["answerFileChooser"]
          >[0]["files"][number]
        >(async (files) => {
          for (const file of files)
            filePayloads.push({
              name: file.name,
              mimeType: file.mimeType,
              bytes: Array.from(file.buffer),
            });
        });
        yield* Effect.promise(() => external.emitAsync("filechooser", chooser.chooser));
        const chooserId = yield* Effect.promise(() => pickerOffered.promise);
        const expiredPickerUrl = `${a.url}/api/preview-stream/upload?${params}&chooser=${encodeURIComponent(chooserId)}`;
        const renewViewer = () =>
          rpc(WS_METHODS.previewViewerOpen, {
            resource,
            operation: "control",
            viewerId: viewer.viewerId,
          });
        const renewedReply = yield* renewViewer();
        expect(renewedReply.exit?._tag, json(renewedReply)).toBe("Success");
        const renewed = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(
          renewedReply.exit?.value,
        );
        expect(renewed.viewerId).toBe(viewer.viewerId);
        expect(renewed.ticket).not.toBe(viewer.ticket);
        const freshParams = new URLSearchParams(params);
        freshParams.set("viewerTicket", renewed.ticket);
        const pickerUrl = `${a.url}/api/preview-stream/upload?${freshParams}&chooser=${encodeURIComponent(chooserId)}`;
        const form = () => {
          const data = new FormData();
          data.append(
            "file",
            new File([new Uint8Array([1, 3, 5, 7])], "phone.txt", { type: "text/plain" }),
          );
          return HttpBody.formData(data);
        };
        const pickerHttp = yield* HttpClient.HttpClient;
        expect(
          (yield* pickerHttp.execute(
            HttpClientRequest.post(expiredPickerUrl).pipe(HttpClientRequest.setBody(form())),
          )).status,
        ).toBe(403);
        expect(chooser.calls).toHaveLength(0);
        const uploaded = yield* pickerHttp.execute(
          HttpClientRequest.post(pickerUrl).pipe(HttpClientRequest.setBody(form())),
        );
        expect(
          uploaded.status,
          json({
            source: a.previewUploadContentTypes,
            destination: b.previewUploadContentTypes,
            body: yield* uploaded.text,
          }),
        ).toBe(204);
        expect(filePayloads).toEqual([
          { name: "phone.txt", mimeType: "text/plain", bytes: [1, 3, 5, 7] },
        ]);
        expect(chooser.calls).toHaveLength(1);
        const repeated = yield* pickerHttp.execute(
          HttpClientRequest.post(pickerUrl).pipe(HttpClientRequest.setBody(form())),
        );
        expect(repeated.status).toBe(409);
        expect(chooser.calls).toHaveLength(1);
        pickerOffered = Promise.withResolvers<string>();
        const canceled = makeExternalBrowserFileChooser(async () => {});
        yield* Effect.promise(() => external.emitAsync("filechooser", canceled.chooser));
        const cancelId = yield* Effect.promise(() => pickerOffered.promise);
        cancelPickerId = cancelId;
        yield* TestClock.adjust("65 seconds");
        const cancelReply = yield* renewViewer();
        expect(cancelReply.exit?._tag).toBe("Success");
        const cancelAccess = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(
          cancelReply.exit?.value,
        );
        expect(cancelAccess.viewerId).toBe(viewer.viewerId);
        const cancelParams = new URLSearchParams(freshParams);
        cancelParams.set("viewerTicket", cancelAccess.ticket);
        cancelParams.set("chooser", cancelId);
        const cancelUrl = `${a.url}/api/preview-stream/upload?${cancelParams}`;
        const cancelRequest = () =>
          pickerHttp.execute(
            HttpClientRequest.post(cancelUrl).pipe(
              HttpClientRequest.setBody(HttpBody.formData(new FormData())),
            ),
          );
        expect((yield* cancelRequest()).status).toBe(204);
        expect(canceled.calls).toHaveLength(0);
        yield* Effect.promise(() => pickerCanceled.promise);
        expect((yield* cancelRequest()).status).toBe(409);
        expect(canceled.calls).toHaveLength(0);
        const fs = yield* FileSystem.FileSystem;
        const downloadBytes = new Uint8Array([11, 22, 33, 44]);
        const download = makeExternalBrowserDownload((path) =>
          nativePromise(fs.writeFile(path, downloadBytes)),
        );
        yield* Effect.promise(() => external.emitAsync("download", download.download));
        const downloadId = yield* Effect.promise(() => downloadOffered.promise);
        yield* TestClock.adjust("65 seconds");
        const downloadUrl = (ticket: string) =>
          `${a.url}/api/preview-stream/download?${new URLSearchParams({ threadId: child, tabId: resource.tabId, id: downloadId, viewerTicket: ticket })}`;
        expect((yield* pickerHttp.get(downloadUrl(cancelAccess.ticket))).status).toBe(403);
        const downloadReply = yield* renewViewer();
        expect(downloadReply.exit?._tag).toBe("Success");
        const downloadAccess = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(
          downloadReply.exit?.value,
        );
        expect(downloadAccess.viewerId).toBe(viewer.viewerId);
        const fetchedDownload = yield* pickerHttp.get(downloadUrl(downloadAccess.ticket));
        expect(fetchedDownload.status).toBe(200);
        expect(Array.from(new Uint8Array(yield* fetchedDownload.arrayBuffer))).toEqual(
          Array.from(downloadBytes),
        );
        const rangedDownload = yield* pickerHttp.execute(
          HttpClientRequest.get(downloadUrl(downloadAccess.ticket)).pipe(
            HttpClientRequest.setHeader("range", "bytes=1-2"),
          ),
        );
        expect(rangedDownload.status).toBe(206);
        expect(Array.from(new Uint8Array(yield* rangedDownload.arrayBuffer))).toEqual([22, 33]);
        expect(download.saves).toHaveLength(1);
        yield* TestClock.adjust("65 seconds");
        const laterFrame = Promise.withResolvers<Uint8Array>();
        socket.addEventListener("message", (event) => {
          if (typeof event.data !== "string")
            laterFrame.resolve(new Uint8Array(event.data as ArrayBuffer));
        });
        external.emit("Page.screencastFrame", {
          data: Buffer.from(jpeg).toString("base64"),
          sessionId: 41,
        });
        expect(
          yield* Effect.promise(() =>
            Promise.race([
              laterFrame.promise.then((bytes) => Array.from(bytes)),
              closed.promise.then(() => "closed"),
            ]),
          ),
          "same browser socket carries JPEG after admission expiry",
        ).toEqual(Array.from(jpeg));
        socket.send(json({ type: "ack" }));
        const laterInput = Promise.withResolvers<void>();
        external.controls.afterInput = async (method) => {
          if (method === "Input.insertText") laterInput.resolve();
        };
        socket.send(json({ type: "text", text: "still controlled after admission expiry" }));
        expect(
          yield* Effect.promise(() =>
            Promise.race([
              laterInput.promise.then(() => "forwarded"),
              closed.promise.then(() => "closed"),
            ]),
          ),
          "same browser control must survive admission expiry",
        ).toBe("forwarded");
        const rival = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const rivalNative = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${rival.token}` } }),
          ),
        );
        const rivalRpc = yield* connectRpc(rivalNative);
        expect((yield* renewViewer()).exit?._tag).toBe("Success");
        expect(
          (yield* rivalRpc(WS_METHODS.previewViewerOpen, {
            resource,
            operation: "control",
            viewerId: viewer.viewerId,
          })).exit?._tag,
        ).toBe("Failure");
        expect(
          (yield* rpc(WS_METHODS.previewViewerOpen, {
            resource: { ...resource, tabId: "other" },
            operation: "control",
            viewerId: viewer.viewerId,
          })).exit?._tag,
        ).toBe("Failure");
        expect(
          (yield* rivalRpc(WS_METHODS.previewViewerClose, { viewerId: viewer.viewerId })).exit
            ?.value,
        ).toBe(false);
        expect((yield* renewViewer()).exit?._tag).toBe("Success");
        yield* b.authority.setGrant({ ...grant, preview: false });
        expect((yield* renewViewer()).exit?._tag).toBe("Failure");
        expect((yield* pickerHttp.get(downloadUrl(downloadAccess.ticket))).status).toBe(403);
        socket.send(json({ type: "text", text: "must not arrive" }));
        yield* Effect.promise(() => closed.promise);
        expect(external.inputs.filter((input) => input.method === "Input.insertText")).toHaveLength(
          2,
        );
        yield* b.authority.setGrant({ ...grant, preview: true });
        expect(
          (yield* rivalRpc(WS_METHODS.previewViewerOpen, {
            resource,
            operation: "control",
            viewerId: viewer.viewerId,
          })).exit?._tag,
        ).toBe("Failure");
        expect(
          (yield* rpc(WS_METHODS.previewViewerOpen, {
            resource: { ...resource, tabId: "other" },
            operation: "control",
            viewerId: viewer.viewerId,
          })).exit?._tag,
        ).toBe("Failure");
        expect(
          (yield* rivalRpc(WS_METHODS.previewViewerClose, { viewerId: viewer.viewerId })).exit
            ?.value,
        ).toBe(false);
        expect(
          (yield* rpc(WS_METHODS.previewViewerClose, { viewerId: viewer.viewerId })).exit?.value,
        ).toBe(true);
        expect(
          (yield* b
            .fixtureBrowserViewerSessions!.authenticateViewerToken(resolved.ticket)
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(
          (yield* b.nativePreviewManager!.list({ threadId: child })).sessions.map(
            (session) => session.tabId,
          ),
        ).toContain(resource.tabId);
        const reopenedReply = yield* rpc(WS_METHODS.previewViewerOpen, {
          resource,
          operation: "view",
        });
        expect(reopenedReply.exit?._tag).toBe("Success");
        const reopened = yield* Schema.decodeUnknownEffect(PreviewViewerAccess)(
          reopenedReply.exit?.value,
        );
        expect(reopened.viewerId).not.toBe(viewer.viewerId);
        const before = [...external.effects];
        for (const suffix of [`&tabId=other`, `&device=alternate`, `&viewerTicket=duplicate`]) {
          const response = yield* (yield* HttpClient.HttpClient).get(
            `${a.url}/api/preview-stream/download?${params}&id=missing${suffix}`,
          );
          expect(response.status).toBe(403);
        }
        expect(external.effects).toEqual(before);
        expect(
          (yield* rpc(WS_METHODS.previewViewerClose, { viewerId: reopened.viewerId })).exit?.value,
        ).toBe(true);
      }),
    ).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer, TestClock.layer())),
    ),
  30_000,
);

it.live.each(["messages", "bytes", "progress"] as const)(
  "backlog383: accepted linked viewer refuses bounded pending %s behind a stalled native action",
  (bound) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-input-backlog",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "backlog-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const access = yield* a.fixtureBrowserViewers!.open(native, {
          resource,
          operation: "control",
        });
        const started = Promise.withResolvers<void>();
        const finish = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => finish.resolve()));
        const external = browserExternalContexts.at(-1)!;
        external.controls.afterInput = async (method) => {
          if (method === "Input.insertText" && external.inputs.at(-1)?.params?.text === "hold") {
            started.resolve();
            await finish.promise;
          }
        };
        const controlled = Promise.withResolvers<void>();
        const observed = Promise.withResolvers<number | "accepted backlog">();
        let submitted = false;
        let sawDialog = false;
        let retaking = false;
        const dialogCleared = Promise.withResolvers<void>();
        const retaken = Promise.withResolvers<void>();
        const params = new URLSearchParams({
          threadId: child,
          tabId: resource.tabId,
          viewerTicket: access.ticket,
        });
        const socket = yield* Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<WebSocket>((resolve, reject) => {
                const ws = new WebSocket(
                  `${a.url.replace(/^http/, "ws")}/api/preview-stream/ws?${params}`,
                );
                ws.binaryType = "arraybuffer";
                ws.addEventListener("open", () => resolve(ws), { once: true });
                ws.addEventListener("error", () => reject(new Error("Backlog viewer refused")), {
                  once: true,
                });
                ws.addEventListener("close", (event) => observed.resolve(event.code), {
                  once: true,
                });
                ws.addEventListener("message", (event) => {
                  if (typeof event.data !== "string") {
                    ws.send(json({ type: "ack" }));
                    return;
                  }
                  const message = JSON.parse(event.data);
                  if (message.type === "control" && message.controller === "you") {
                    controlled.resolve();
                    if (retaking) retaken.resolve();
                    if (message.dialog) sawDialog = true;
                    else if (sawDialog) dialogCleared.resolve();
                  }
                  if (submitted && message.type === "control" && message.controller !== "you")
                    observed.resolve("accepted backlog");
                });
              }),
          ),
          (ws) => Effect.sync(() => ws.close()),
        );
        socket.send(json({ type: "takeControl" }));
        yield* Effect.promise(() => controlled.promise);
        socket.send(json({ type: "text", text: "hold" }));
        yield* Effect.promise(() => started.promise);
        if (bound === "progress") {
          for (let i = 0; i < 63; i++) socket.send(json({ type: "text", text: `queued-${i}` }));
          const dialogAnswered = Promise.withResolvers<void>();
          external.emit("dialog", {
            type: () => "alert",
            message: () => "unblock",
            defaultValue: () => "",
            accept: async () => dialogAnswered.resolve(),
            dismiss: async () => {},
          });
          const acked = Promise.withResolvers<void>();
          const originalSend = external.cdp.send;
          const spy = vi.spyOn(external.cdp, "send").mockImplementation(async (method, params) => {
            const result = await originalSend(method, params);
            if (method === "Page.screencastFrameAck") acked.resolve();
            return result;
          });
          yield* Effect.addFinalizer(() => Effect.sync(() => spy.mockRestore()));
          external.emit("Page.screencastFrame", {
            sessionId: 1,
            data: Buffer.from([255, 216, 255, 217]).toString("base64"),
          });
          socket.send(json({ type: "dialog", accept: true }));
          yield* Effect.promise(() =>
            Promise.all([dialogAnswered.promise, dialogCleared.promise, acked.promise]),
          );
          retaking = true;
          socket.send(json({ type: "takeControl" }));
          yield* Effect.promise(() => retaken.promise);
        }
        const count = bound === "messages" ? 1024 : bound === "bytes" ? 20 : 0;
        for (let i = 0; i < count; i++) {
          socket.send(
            json({ type: "text", text: bound === "messages" ? `queued-${i}` : "é".repeat(10_000) }),
          );
        }
        submitted = true;
        socket.send(json({ type: "releaseControl" }));
        const result = yield* Effect.promise(() => observed.promise);
        expect(
          result,
          "the accepted viewer must explicitly refuse overload before draining every message into native work",
        ).toBe(bound === "progress" ? "accepted backlog" : 1013);
        const connectReplacement = () =>
          Effect.gen(function* () {
            const replacement = yield* a.fixtureBrowserViewers!.open(native, {
              resource,
              operation: "control",
            });
            const taking = Promise.withResolvers<void>();
            const result = Promise.withResolvers<number | "released">();
            let releasing = false;
            const query = new URLSearchParams({
              threadId: child,
              tabId: resource.tabId,
              viewerTicket: replacement.ticket,
            });
            const ws = yield* Effect.acquireRelease(
              Effect.promise(
                () =>
                  new Promise<WebSocket>((resolve, reject) => {
                    const next = new WebSocket(
                      `${a.url.replace(/^http/, "ws")}/api/preview-stream/ws?${query}`,
                    );
                    next.addEventListener("open", () => resolve(next), { once: true });
                    next.addEventListener(
                      "error",
                      () => reject(new Error("Replacement viewer refused")),
                      { once: true },
                    );
                    next.addEventListener("close", (event) => result.resolve(event.code), {
                      once: true,
                    });
                    next.addEventListener("message", (event) => {
                      if (typeof event.data !== "string") {
                        next.send(json({ type: "ack" }));
                        return;
                      }
                      const message = JSON.parse(event.data);
                      if (message.type === "control" && message.controller === "you")
                        taking.resolve();
                      if (releasing && message.type === "control" && message.controller !== "you")
                        result.resolve("released");
                    });
                  }),
              ),
              (next) => Effect.sync(() => next.close()),
            );
            ws.send(json({ type: "takeControl" }));
            yield* Effect.promise(() => taking.promise);
            return {
              socket: ws,
              result: result.promise,
              release: () => {
                releasing = true;
                ws.send(json({ type: "releaseControl" }));
              },
            };
          });
        if (bound === "messages") {
          const replacement = yield* connectReplacement();
          replacement.socket.send(
            json({ type: "text", text: "replacement must not enter the old backlog" }),
          );
          replacement.release();
          expect(yield* Effect.promise(() => replacement.result)).toBe(1013);
        }
        finish.resolve();
        const resumed = yield* nativeCall(a, parent, "preview_navigate", {
          ...route,
          tabId: resource.tabId,
          url: "https://example.test/recovered",
        });
        expect(resumed.isError, json(resumed)).not.toBe(true);
        if (bound === "messages") {
          const delivered = Promise.withResolvers<void>();
          external.controls.afterInput = async (method) => {
            if (method === "Input.insertText") delivered.resolve();
          };
          const fresh = yield* connectReplacement();
          fresh.socket.send(json({ type: "text", text: "recovered" }));
          yield* Effect.promise(() => delivered.promise);
          fresh.release();
          expect(yield* Effect.promise(() => fresh.result)).toBe("released");
        }
        expect(external.inputs.filter((input) => input.method === "Input.insertText")).toEqual([
          { method: "Input.insertText", params: { text: "hold" } },
          ...(bound === "messages"
            ? [{ method: "Input.insertText", params: { text: "recovered" } }]
            : []),
        ]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([5, 20])(
  "resource-links383: %i small native downloads retain resource links",
  (count) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-download-fetch",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "download-fetch-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource: browser } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        expect(browser.threadId).toBe(child);
        const fs = yield* FileSystem.FileSystem;
        const bytes = Uint8Array.from([11, 22, 33, 44]);
        const download = makeExternalBrowserDownload((path) =>
          nativePromise(fs.writeFile(path, bytes)),
        );
        yield* Effect.promise(() =>
          browserExternalContexts.at(-1)!.emitAsync("download", download.download),
        );
        expect(download.saves).toHaveLength(1);
        const status = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: browser.tabId,
        });
        expect(status.isError, json(status)).not.toBe(true);
        const { downloads } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            downloads: Schema.Array(
              Schema.Struct({ resource: NativeBrowserDownloadAssetResource }),
            ),
          }),
        )(status.structuredContent);
        expect(resourcesFromToolItem({ status: "completed", output: status }).length).toBe(2);
        for (let i = 1; i < count; i++) {
          const extra = makeExternalBrowserDownload((path) =>
            nativePromise(fs.writeFile(path, bytes)),
          );
          yield* Effect.promise(() =>
            browserExternalContexts.at(-1)!.emitAsync("download", extra.download),
          );
          expect(extra.saves.length).toBe(1);
        }
        const finalStatus = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: browser.tabId,
        });
        expect(finalStatus.isError, json(finalStatus)).not.toBe(true);
        const actual = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            downloads: Schema.Array(
              Schema.Struct({ resource: NativeBrowserDownloadAssetResource }),
            ),
          }),
        )(finalStatus.structuredContent);
        expect(actual.downloads.length).toBe(count);
        const now = yield* DateTime.now;
        const itemId = TurnItemId.make("resource-links383-downloads-item");
        yield* a.sink.write({
          commandId: CommandId.make("resource-links383-downloads-output"),
          events: [
            {
              id: EventId.make("resource-links383-downloads-output"),
              type: "turn-item.updated",
              threadId: parent,
              occurredAt: now,
              payload: {
                id: itemId,
                type: "dynamic_tool",
                threadId: parent,
                runId: null,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 1,
                status: "completed",
                title: null,
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                toolName: "preview_status",
                input: { ...route, tabId: browser.tabId },
                output: finalStatus,
              },
            },
          ],
        });
        const wire = projectThreadProjectionForWire(
          yield* a.orchestrator.getThreadProjection(parent),
        );
        const projected = wire.turnItems.find((item) => item.id === itemId);
        if (projected?.type !== "dynamic_tool")
          return yield* Effect.die("Missing native tool projection");
        const resources = resourcesFromToolItem(projected);
        const expected = [browser, ...actual.downloads.map((entry) => entry.resource)];
        const canonicalBytes = Buffer.byteLength(json({ resources: expected }));
        const statusBytes = Buffer.byteLength(json(finalStatus));
        yield* Effect.logInfo("resource-links383 native producer to wire", {
          downloads: count,
          statusBytes,
          canonicalBytes,
          threadIdLength: browser.threadId.length,
          tabIdLength: browser.tabId.length,
          links: resources.length,
        });
        if (count === 20) {
          expect(statusBytes).toBeGreaterThan(16_384);
          expect(canonicalBytes).toBeGreaterThan(8_192);
        } else expect(statusBytes).toBeLessThanOrEqual(16_384);
        expect(canonicalBytes).toBeLessThanOrEqual(16_384);
        expect(
          resources.length,
          "ordinary four-byte downloads must retain browser and download navigation",
        ).toBe(count + 1);
        expect(downloads).toHaveLength(1);
        expect(resources).toEqual(expected);
        expect(projected.output).toEqual({ resources: expected });
        expect(compactDynamicToolOutput(json(projected.output))).toEqual(projected.output);
        expect(
          resourcesFromToolItem({
            status: "completed",
            output: compactDynamicToolOutput(projected.output),
          }),
        ).toEqual(expected);
        expect(json(projected.output)).not.toContain(b.config.stateDir);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-artifact: A-only native access verifies a registered B capture and current grant",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open } = yield* typed383Pair(
          "native383-artifact-fetch",
        );
        const opened = yield* open("artifact-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const screenshot = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "screenshot" },
          clientRequestId: "artifact-capture",
        });
        expect(screenshot.isError, json(screenshot)).not.toBe(true);
        const { artifact } = yield* decodeTyped383Artifact(screenshot.structuredContent);
        // Store the actual public result at the native event/projection owner,
        // then consume its ordinary compact wire form. No B connection is added.
        const now = yield* DateTime.now;
        const itemId = TurnItemId.make("native383-qualified-capture-item");
        yield* a.sink.write({
          commandId: CommandId.make("native383-qualified-capture-output"),
          events: [
            {
              id: EventId.make("native383-qualified-capture-output"),
              type: "turn-item.updated",
              threadId: parent,
              occurredAt: now,
              payload: {
                id: itemId,
                type: "dynamic_tool",
                threadId: parent,
                runId: null,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 1,
                status: "completed",
                title: null,
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                toolName: "device_execute",
                input: { ...route, target, command: { type: "screenshot" } },
                output: screenshot,
              },
            },
          ],
        });
        const wire = projectThreadProjectionForWire(
          yield* a.orchestrator.getThreadProjection(parent),
        );
        const projected = wire.turnItems.find((item) => item.id === itemId);
        if (projected?.type !== "dynamic_tool")
          return yield* Effect.die("Missing native tool projection");
        const resources = resourcesFromToolItem(projected);
        expect(resources).toEqual([
          {
            _tag: "peer-attachment",
            threadId: child,
            reference: artifact,
          },
        ]);
        expect(json(projected.output)).not.toContain("quickStart");
        expect(json(projected.output)).not.toContain(b.config.attachmentsDir);
        const qualified = resources[0];
        if (qualified === undefined || !("_tag" in qualified))
          return yield* Effect.die("Missing qualified projected artifact");
        const owner = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, {
              headers: { authorization: `Bearer ${owner.token}` },
            }),
          ),
        );
        const resource = { ...qualified, disposition: "inline" as const };
        const issued = yield* a.issueAssetUrl({ resource, session: native });
        expect(issued.relativeUrl).toMatch(/^\/api\/assets\//);
        expect(json(issued)).not.toContain(b.url);
        expect(json(issued)).not.toContain(b.config.attachmentsDir);
        const client = yield* HttpClient.HttpClient;
        const fetchAsset = (method: "GET" | "HEAD", headers: Record<string, string> = {}) =>
          Effect.gen(function* () {
            const ticket = yield* a.auth.issueWebSocketTicket(native);
            return yield* client.execute(
              HttpClientRequest.make(method)(
                `${a.url}${issued.relativeUrl}?wsTicket=${encodeURIComponent(ticket.ticket)}`,
              ).pipe(HttpClientRequest.setHeaders(headers)),
            );
          });
        const fetched = yield* fetchAsset("GET");
        expect(fetched.status).toBe(200);
        const body = new Uint8Array(yield* fetched.arrayBuffer);
        expect(body.byteLength).toBe(artifact.sizeBytes);
        expect(yield* typed383Digest(body)).toBe(artifact.digest);
        const head = yield* fetchAsset("HEAD");
        expect(head.status).toBe(200);
        expect(head.headers["content-length"]).toBe(String(body.byteLength));
        expect((yield* head.arrayBuffer).byteLength).toBe(0);
        const range = yield* fetchAsset("GET", { range: "bytes=1-3" });
        expect(range.status).toBe(206);
        expect(Array.from(new Uint8Array(yield* range.arrayBuffer))).toEqual(
          Array.from(body.subarray(1, 4)),
        );
        const rival = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const rivalNative = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, {
              headers: { authorization: `Bearer ${rival.token}` },
            }),
          ),
        );
        const rivalTicket = yield* a.auth.issueWebSocketTicket(rivalNative);
        const rivalFetch = yield* client.execute(
          HttpClientRequest.get(
            `${a.url}${issued.relativeUrl}?wsTicket=${encodeURIComponent(rivalTicket.ticket)}`,
          ),
        );
        expect(rivalFetch.status).toBe(403);
        const receiptsBefore = yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`SELECT * FROM mcp_request_receipts`;
        const capturesBefore = yield* Ref.get(b.deviceScreenshotCalls);
        for (const changed of [
          { ...resource, threadId: parent },
          { ...resource, reference: { ...artifact, digest: "0".repeat(64) } },
          { ...resource, reference: { ...artifact, owner: "0".repeat(64) } },
          { ...resource, reference: { ...artifact, mimeType: "text/html" } },
          { ...resource, reference: { ...artifact, sizeBytes: artifact.sizeBytes - 1 } },
        ]) {
          expect(
            (yield* a.issueAssetUrl({ resource: changed, session: native }).pipe(Effect.result))
              ._tag,
          ).toBe("Failure");
        }
        const fs = yield* FileSystem.FileSystem;
        const stored = resolveAttachmentPathById({
          attachmentsDir: b.config.attachmentsDir,
          attachmentId: artifact.attachmentId,
        });
        expect(stored).not.toBeNull();
        const altered = body.slice();
        altered[altered.length - 1] = altered.at(-1)! ^ 1;
        yield* fs.writeFile(stored!, altered);
        expect((yield* fetchAsset("GET")).status).toBe(403);
        yield* fs.writeFile(stored!, body);
        expect((yield* fetchAsset("GET")).status).toBe(200);
        const grant = (yield* b.authority.grants)[0]!;
        yield* Ref.set(
          b.artifactResponseHook,
          b.authority
            .setGrant({ ...grant, device: false })
            .pipe(Effect.andThen(Ref.set(b.artifactResponseHook, Effect.void))),
        );
        expect(
          (yield* fetchAsset("GET")).status,
          "retirement after B read and before A publication must prevent the public response",
        ).toBe(403);
        yield* b.authority.setGrant(grant);
        expect((yield* fetchAsset("GET")).status).toBe(200);
        yield* b.authority.setGrant({ ...grant, device: false });
        expect((yield* fetchAsset("GET")).status).toBe(403);
        yield* b.authority.setGrant(grant);
        expect((yield* fetchAsset("GET")).status).toBe(200);
        expect(yield* Ref.get(b.deviceScreenshotCalls)).toBe(capturesBefore);
        expect(
          yield* Context.get(b.database, SqlClient.SqlClient)`SELECT * FROM mcp_request_receipts`,
        ).toEqual(receiptsBefore);
        // The URL is source-session-bound and also pinned to the actual link generation.
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, b, "full-access");
        expect((yield* fetchAsset("GET")).status).toBe(403);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser: human popup uses its native current session after the agent grant retires",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-human-popup",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "popup-agent-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const opener = browserExternalContexts.at(-1)!;
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const repository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
          Effect.provideContext(repository),
        );
        const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
          Effect.provideContext(repository),
        );
        const viewer = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: () => nativePromise(read.pipe(Effect.andThen(operate))),
        });
        yield* viewer.input({ type: "takeControl" });
        yield* viewer.input({
          type: "mouse",
          action: "down",
          x: 10,
          y: 10,
          button: "left",
          buttons: 1,
        });
        expect(opener.inputs.some((input) => input.method === "Input.dispatchMouseEvent")).toBe(
          true,
        );
        const events = yield* b.nativePreviewManager!.subscribeEvents;
        const observeOpening = Effect.gen(function* () {
          while (true) {
            const event = yield* PubSub.take(events);
            if (event.type === "opened")
              return { state: "opened" as const, snapshot: event.snapshot };
          }
        });
        const popup = makeExternalBrowserContext();
        const closed = yield* Deferred.make<{ state: "closed" }>();
        popup.controls.afterClose = async () => {
          await nativePromise(Deferred.succeed(closed, { state: "closed" as const }));
        };
        yield* b.authority.setGrant({ ...grant, preview: false });
        opener.emit("popup", popup.page);
        const outcome = yield* observeOpening.pipe(Effect.raceFirst(Deferred.await(closed)));
        expect(outcome.state, json(outcome)).toBe("opened");
        if (outcome.state !== "opened") return;
        expect(outcome.snapshot.automationOwner).toBeUndefined();
        // The opened event precedes the manager transaction's publication. Await
        // the actual viewer notification, then attach through the native owner.
        const popupResource = yield* Effect.gen(function* () {
          while (true) {
            const output = yield* Queue.take(viewer.output);
            if (output._tag === "frame") yield* output.ack;
            if (output._tag === "popup") return output.resource;
          }
        });
        expect(popupResource?.tabId).toBe(outcome.snapshot.tabId);
        yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: outcome.snapshot.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: false,
          authorize: () => nativePromise(read),
        });
        // The native authorization remains live independently of the retired MCP actor.
        yield* read;
        yield* operate;
        const next = makeExternalBrowserContext();
        const nextClosed = yield* Deferred.make<void>();
        next.controls.afterClose = async () => {
          await nativePromise(Deferred.succeed(nextClosed, undefined));
        };
        yield* Context.get(repository, AuthSessions.AuthSessionRepository).revoke({
          sessionId: issued.sessionId,
          revokedAt: yield* DateTime.now,
        });
        const before = (yield* b.nativePreviewManager!.list({ threadId: child })).sessions.map(
          (session) => session.tabId,
        );
        opener.emit("popup", next.page);
        yield* Deferred.await(nextClosed);
        expect(
          (yield* b.nativePreviewManager!.list({ threadId: child })).sessions.map(
            (session) => session.tabId,
          ),
        ).toEqual(before);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-attachment: public inspection distinguishes incomplete uploads before typed source input transfer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, route, open, footprint } =
          yield* typed383Pair("native383-public-input");
        const opened = yield* open("public-input-open");
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const bytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const upload = {
          type: "file" as const,
          name: "app.apk",
          mimeType: "application/vnd.android.package-archive",
          sizeBytes: bytes.length,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          upload,
          digest: yield* typed383Digest(bytes),
          clientRequestId: "public-input-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const inspect = { source: { type: "upload", attachmentId: issued.attachmentId, upload } };
        const raw = yield* nativeRpcCall(a, parent, "t3_attachment_inspect", inspect);
        expect(raw, raw).not.toContain('"error"');
        const incomplete = (yield* sendResult(raw)).result;
        expect(incomplete.isError, json(incomplete)).not.toBe(true);
        expect(incomplete.structuredContent).toEqual({
          type: "upload",
          inspection: { stage: "awaiting-bytes" },
        });
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const staged = yield* nativeCall(a, parent, "t3_attachment_inspect", inspect);
        expect(staged.isError, json(staged)).not.toBe(true);
        const { inspection } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            inspection: Schema.Struct({
              stage: Schema.Literal("staged"),
              reference: PeerAttachmentReference,
            }),
          }),
        )(staged.structuredContent);
        expect(inspection.reference.environmentId).toBe(a.environmentId);
        expect(inspection.reference.digest).toBe(yield* typed383Digest(bytes));
        const before = yield* footprint();
        const drive = (key: string) =>
          nativeCall(a, parent, "device_execute", {
            ...route,
            target,
            command: { type: "install", app: "com.example.app", artifact: inspection.reference },
            clientRequestId: key,
          });
        yield* Ref.set(a.dropCommittedToolReply, "device_execute");
        const lost = yield* drive("public-input-install");
        expect(lost.isError, json(lost)).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        const after = yield* footprint();
        const installed = yield* drive("public-input-install");
        expect(installed.isError, json(installed)).not.toBe(true);
        expect(yield* footprint()).toEqual(after);
        expect(after.process).toBeGreaterThan(before.process);
        const leaf = b.deviceProcessCalls.at(-1)!;
        const inputPath = leaf.args.at(-1)!;
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(inputPath))).toEqual(Array.from(bytes));
        expect(inputPath).not.toContain(a.config.attachmentsDir);
        expect(json(installed)).not.toContain(inputPath);
        expect((yield* drive("public-input-install")).structuredContent).toEqual(
          installed.structuredContent,
        );
        expect(yield* footprint()).toEqual(after);
        const reused = yield* drive("public-input-different-consumer");
        expect(reused.isError, json(reused)).toBe(true);
        expect(yield* footprint()).toEqual(after);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-attachment: original MCP actor acquires a B capture into one native source publication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open } = yield* typed383Pair(
          "native383-public-acquire",
        );
        const opened = yield* open("public-acquire-open");
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const screenshot = yield* nativeCall(a, parent, "device_execute", {
          ...route,
          target,
          command: { type: "screenshot" },
          clientRequestId: "public-acquire-capture",
        });
        expect(screenshot.isError, json(screenshot)).not.toBe(true);
        const { artifact } = yield* decodeTyped383Artifact(screenshot.structuredContent);
        const resource = { _tag: "peer-attachment" as const, threadId: child, reference: artifact };
        const input = { resource, clientRequestId: "public-acquire-copy" };
        const raw = yield* nativeRpcCall(a, parent, "t3_attachment_acquire", input);
        expect(raw, raw).not.toContain('"error"');
        const acquired = (yield* sendResult(raw)).result;
        expect(acquired.isError, json(acquired)).not.toBe(true);
        const value = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            resource: Schema.Struct({ reference: PeerAttachmentReference }),
            path: Schema.String,
          }),
        )(acquired.structuredContent);
        expect(value.resource.reference.environmentId).toBe(a.environmentId);
        expect(value.resource.reference.digest).toBe(artifact.digest);
        expect(value.path).toBe(
          resolveAttachmentPathById({
            attachmentsDir: a.config.attachmentsDir,
            attachmentId: value.resource.reference.attachmentId,
          }),
        );
        expect(json(acquired)).not.toContain(b.config.attachmentsDir);
        expect(json(acquired)).not.toContain("ISOLATED_DEVICE_TOKEN");
        const fs = yield* FileSystem.FileSystem;
        const original = yield* fs.readFile(value.path);
        expect(yield* typed383Digest(original)).toBe(artifact.digest);
        const replay = yield* nativeCall(a, parent, "t3_attachment_acquire", input);
        expect(replay.structuredContent).toEqual(acquired.structuredContent);
        const nativeRows = yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`SELECT * FROM attachment_staging WHERE id=${value.resource.reference.attachmentId}`;
        expect(nativeRows).toHaveLength(1);
        expect(nativeRows[0]).toMatchObject({
          digest: artifact.digest,
          expected_digest: artifact.digest,
          publication_capability: "device",
        });
        // Every nativeCall issues a fresh lawful credential. Same-disk source and
        // destination transport reconstruction must still use the accepted copy.
        const activeA = { ...a, ...(yield* a.restartSchedulerTransport) };
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([activeA.url])} WHERE environment_id=${a.environmentId}`;
        const activeB = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([activeB.url])} WHERE environment_id=${b.environmentId}`;
        expect(
          (yield* nativeCall(activeA, parent, "t3_attachment_acquire", input)).structuredContent,
        ).toEqual(acquired.structuredContent);
        const edited = new Uint8Array([9, 8, 7]);
        yield* fs.writeFile(value.path, edited);
        const historical = yield* nativeCall(activeA, parent, "t3_attachment_acquire", input);
        expect(historical.isError, json(historical)).not.toBe(true);
        expect(historical.structuredContent).toEqual(acquired.structuredContent);
        expect(Array.from(yield* fs.readFile(value.path))).toEqual(Array.from(edited));
        expect(
          yield* Context.get(
            a.database,
            SqlClient.SqlClient,
          )`SELECT * FROM attachment_staging WHERE id=${value.resource.reference.attachmentId}`,
        ).toEqual(nativeRows);
        expect(
          (yield* nativeCall(activeA, parent, "t3_attachment_acquire", {
            ...input,
            resource: { ...resource, reference: { ...artifact, name: "changed.png" } },
          })).isError,
        ).toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, device: false });
        expect((yield* nativeCall(activeA, parent, "t3_attachment_acquire", input)).isError).toBe(
          true,
        );
        expect(Array.from(yield* fs.readFile(value.path))).toEqual(Array.from(edited));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each(["agent", "human"] as const)(
  "native383-browser: %s download rechecks its actual actor after readiness before saving bytes",
  (actor) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          `native383-download-${actor}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "download-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const external = browserExternalContexts.at(-1)!;
        const fs = yield* FileSystem.FileSystem;
        const bytes = new TextEncoder().encode("native download bytes");
        let retire: Effect.Effect<void>;
        if (actor === "human") {
          const issued = yield* b.auth.issueSession({
            scopes: ["orchestration:read", "preview:operate"],
          });
          const native = yield* b.auth.authenticateHttpRequest(
            HttpServerRequest.fromWeb(
              new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
            ),
          );
          const repository = yield* AuthSessions.layer.pipe(
            Layer.provide(Layer.succeedContext(b.database)),
            Layer.build,
          );
          const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
            Effect.provideContext(repository),
          );
          const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
            Effect.provideContext(repository),
          );
          const viewer = yield* b.nativeBrowser!.attachViewer({
            threadId: child,
            tabId: resource.tabId,
            maxWidth: 800,
            maxHeight: 600,
            quality: 70,
            canOperate: true,
            authorize: () => nativePromise(read.pipe(Effect.andThen(operate))),
          });
          yield* viewer.input({ type: "takeControl" });
          yield* viewer.input({
            type: "mouse",
            action: "down",
            x: 10,
            y: 10,
            button: "left",
            buttons: 1,
            clickCount: 1,
            modifiers: 0,
          });
          // Human ownership does not borrow the retired opening agent's grant.
          yield* b.authority.setGrant({ ...grant, preview: false });
          retire = Context.get(repository, AuthSessions.AuthSessionRepository)
            .revoke({ sessionId: issued.sessionId, revokedAt: yield* DateTime.now })
            .pipe(Effect.asVoid, Effect.orDie);
        } else
          retire = b.authority
            .setGrant({ ...grant, preview: false })
            .pipe(Effect.asVoid, Effect.orDie);
        const lawful = makeExternalBrowserDownload((path) =>
          nativePromise(fs.writeFile(path, bytes)),
        );
        yield* Effect.promise(() => external.emitAsync("download", lawful.download));
        expect(lawful.saves).toHaveLength(1);
        expect(Array.from(yield* fs.readFile(lawful.saves[0]!))).toEqual(Array.from(bytes));
        const refused = makeExternalBrowserDownload(
          (path) => nativePromise(fs.writeFile(path, bytes)),
          () => nativePromise(retire),
        );
        yield* Effect.promise(() => external.emitAsync("download", refused.download));
        expect(
          refused.saves,
          "readiness retirement must prevent the next native saveAs dispatch",
        ).toHaveLength(0);
        expect(Array.from(yield* fs.readFile(lawful.saves[0]!))).toEqual(Array.from(bytes));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-browser: registered download status does not instruct an A-only caller to use a B path",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-download-reference",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "download-reference-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        const bytes = new TextEncoder().encode("portable browser download");
        const download = makeExternalBrowserDownload((path) =>
          nativePromise(fs.writeFile(path, bytes)),
        );
        yield* Effect.promise(() =>
          browserExternalContexts.at(-1)!.emitAsync("download", download.download),
        );
        expect(download.saves).toHaveLength(1);
        const status = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: resource.tabId,
        });
        expect(status.isError, json(status)).not.toBe(true);
        expect(json(status)).toContain("report.txt");
        expect(
          json(status),
          "A-only registered output must not expose a B-local download path",
        ).not.toContain(b.config.stateDir);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-download: qualified source HTTP fetch and native acquisition retain exact bytes and current owners",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-download-fetch",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "download-fetch-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource: browser } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        const bytes = Uint8Array.from({ length: 2 * 1024 * 1024 + 7 }, (_, index) => index % 251);
        const download = makeExternalBrowserDownload((path) =>
          nativePromise(fs.writeFile(path, bytes)),
        );
        yield* Effect.promise(() =>
          browserExternalContexts.at(-1)!.emitAsync("download", download.download),
        );
        const status = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: browser.tabId,
        });
        expect(status.isError, json(status)).not.toBe(true);
        const { downloads } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            downloads: Schema.Array(
              Schema.Struct({ resource: NativeBrowserDownloadAssetResource }),
            ),
          }),
        )(status.structuredContent);
        const resource = downloads[0]!.resource;
        expect(resource.sizeBytes).toBe(bytes.length);
        expect(resource.digest).toBe(yield* typed383Digest(bytes));
        expect(resource.resource).toEqual(browser);
        const owner = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const issued = yield* a.issueAssetUrl({ resource, session: native });
        expect(json(issued)).not.toContain(b.url);
        expect(json(issued)).not.toContain(b.config.stateDir);
        const client = yield* HttpClient.HttpClient;
        const fetchAsset = (method: "GET" | "HEAD", headers: Record<string, string> = {}) =>
          Effect.gen(function* () {
            const ticket = yield* a.auth.issueWebSocketTicket(native);
            return yield* client.execute(
              HttpClientRequest.make(method)(
                `${a.url}${issued.relativeUrl}?wsTicket=${encodeURIComponent(ticket.ticket)}`,
              ).pipe(HttpClientRequest.setHeaders(headers)),
            );
          });
        const head = yield* fetchAsset("HEAD");
        expect(head.status).toBe(200);
        expect(head.headers["content-length"]).toBe(String(bytes.length));
        expect((yield* head.arrayBuffer).byteLength).toBe(0);
        const fetched = yield* fetchAsset("GET");
        expect(fetched.status).toBe(200);
        expect(yield* typed383Digest(new Uint8Array(yield* fetched.arrayBuffer))).toBe(
          resource.digest,
        );
        const range = yield* fetchAsset("GET", { range: "bytes=1048574-1048580" });
        expect(range.status).toBe(206);
        expect(Array.from(new Uint8Array(yield* range.arrayBuffer))).toEqual(
          Array.from(bytes.subarray(1048574, 1048581)),
        );
        expect((yield* fetchAsset("GET", { range: `bytes=${bytes.length}-` })).status).toBe(416);
        const beforeRetirement = yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`SELECT * FROM attachment_staging`;
        let retiredBlocks = 0;
        const retirementObserved = yield* Deferred.make<void>();
        let binaryStarted = false;
        yield* Ref.set(b.artifactRequestHook, (checkOnly) =>
          Effect.gen(function* () {
            if (checkOnly && binaryStarted) yield* Deferred.await(retirementObserved);
            if (!checkOnly) binaryStarted = true;
          }),
        );
        yield* Ref.set(
          b.artifactResponseHook,
          Effect.gen(function* () {
            retiredBlocks += 1;
            yield* b.authority.setGrant({ ...grant, preview: false });
            yield* Deferred.succeed(retirementObserved, undefined);
            yield* Ref.set(b.artifactResponseHook, Effect.void);
          }).pipe(Effect.orDie),
        );
        const interrupted = yield* fetchAsset("GET").pipe(
          Effect.flatMap((response) => response.arrayBuffer),
          Effect.result,
        );
        expect(
          interrupted._tag,
          "retirement after the native block read must prevent source publication",
        ).toBe("Failure");
        expect(retiredBlocks).toBe(1);
        expect(
          yield* Context.get(a.database, SqlClient.SqlClient)`SELECT * FROM attachment_staging`,
        ).toEqual(beforeRetirement);
        yield* Ref.set(b.artifactRequestHook, () => Effect.void);
        yield* b.authority.setGrant({ ...grant, preview: true });
        const rival = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        expect(
          (yield* client.execute(
            HttpClientRequest.get(`${a.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${rival.token}`),
            ),
          )).status,
        ).toBe(403);
        const acquired = yield* nativeCall(a, parent, "t3_attachment_acquire", {
          resource,
          clientRequestId: "download-acquire",
        });
        expect(acquired.isError, json(acquired)).not.toBe(true);
        const acquiredValue = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ path: Schema.String, resource: PeerAttachmentAssetResource }),
        )(acquired.structuredContent);
        expect(acquiredValue.resource.reference.environmentId).toBe(a.environmentId);
        expect(yield* typed383Digest(yield* fs.readFile(acquiredValue.path))).toBe(resource.digest);
        expect(
          (yield* nativeCall(a, parent, "t3_attachment_acquire", {
            resource,
            clientRequestId: "download-acquire",
          })).structuredContent,
        ).toEqual(acquired.structuredContent);
        yield* fs.writeFile(
          download.saves[0]!,
          bytes.map((byte) => byte ^ 1),
        );
        expect((yield* fetchAsset("HEAD")).status).toBe(403);
        expect(
          (yield* nativeCall(a, parent, "t3_attachment_acquire", {
            resource,
            clientRequestId: "download-corrupt",
          })).isError,
        ).toBe(true);
        yield* b.authority.setGrant({ ...grant, preview: false });
        expect((yield* fetchAsset("GET")).status).toBe(403);
        expect(yield* typed383Digest(yield* fs.readFile(acquiredValue.path))).toBe(resource.digest);
        expect(resource.resource.threadId).toBe(child);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-download: native 1 GiB sparse owner streams a bounded range and refuses larger publication before staging",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-download-gib",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "download-gib-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource: browser } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const fs = yield* FileSystem.FileSystem;
        const size = 1024 * 1024 * 1024;
        const external = browserExternalContexts.at(-1)!;
        const save = (length: number) =>
          makeExternalBrowserDownload((path) =>
            nativePromise(
              fs.writeFile(path, new Uint8Array(0)).pipe(Effect.andThen(fs.truncate(path, length))),
            ),
          );
        const download = save(size);
        yield* Effect.promise(() => external.emitAsync("download", download.download));
        expect(download.saves).toHaveLength(1);
        const status = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: browser.tabId,
        });
        expect(status.isError, json(status)).not.toBe(true);
        const { downloads } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            downloads: Schema.Array(
              Schema.Struct({ resource: NativeBrowserDownloadAssetResource }),
            ),
          }),
        )(status.structuredContent);
        expect(downloads).toHaveLength(1);
        const resource = downloads[0]!.resource;
        expect(resource.sizeBytes).toBe(size);
        // Independent Python hashlib oracle over1024 repeated1 MiB zero blocks.
        expect(resource.digest).toBe(
          "49bc20df15e412a64472421e13fe86ff1c5165e18b2afccf160d4dc19fe68a14",
        );
        const sql = Context.get(a.database, SqlClient.SqlClient);
        const before = yield* sql`SELECT * FROM attachment_staging`;
        const acquired = yield* nativeCall(a, parent, "t3_attachment_acquire", {
          resource,
          clientRequestId: "download-gib-acquire",
        });
        expect(acquired.isError, json(acquired)).toBe(true);
        expect(json(acquired)).toContain("50 MiB");
        expect(yield* sql`SELECT * FROM attachment_staging`).toEqual(before);
        const owner = yield* a.auth.issueSession({ scopes: ["orchestration:read"] });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const issued = yield* a.issueAssetUrl({ resource, session: native });
        const client = yield* HttpClient.HttpClient;
        const fetchAsset = (method: "GET" | "HEAD", headers: Record<string, string> = {}) =>
          Effect.gen(function* () {
            const ticket = yield* a.auth.issueWebSocketTicket(native);
            return yield* client.execute(
              HttpClientRequest.make(method)(
                `${a.url}${issued.relativeUrl}?wsTicket=${encodeURIComponent(ticket.ticket)}`,
              ).pipe(HttpClientRequest.setHeaders(headers)),
            );
          });
        const head = yield* fetchAsset("HEAD");
        expect(head.status).toBe(200);
        expect(head.headers["content-length"]).toBe(String(size));
        expect((yield* head.arrayBuffer).byteLength).toBe(0);
        const range = yield* fetchAsset("GET", { range: `bytes=${size - 7}-` });
        expect(range.status).toBe(206);
        expect(Array.from(new Uint8Array(yield* range.arrayBuffer))).toEqual([0, 0, 0, 0, 0, 0, 0]);
        const nativeViewer = yield* a.fixtureBrowserViewers!.open(native, {
          resource: browser,
          operation: "view",
        });
        const viewerUrl = (ticket: string) =>
          `${a.url}/api/preview-stream/download?${new URLSearchParams({ threadId: browser.threadId, tabId: browser.tabId, id: resource.downloadId, viewerTicket: ticket })}`;
        const viewerHead = yield* client.execute(
          HttpClientRequest.head(viewerUrl(nativeViewer.ticket)),
        );
        expect(viewerHead.status).toBe(200);
        expect(viewerHead.headers["content-length"]).toBe(String(size));
        expect(viewerHead.headers.etag).toBe(`"sha256-${resource.digest}"`);
        const viewerRange = yield* client.execute(
          HttpClientRequest.get(viewerUrl(nativeViewer.ticket)).pipe(
            HttpClientRequest.setHeader("range", `bytes=${size - 7}-`),
          ),
        );
        expect(viewerRange.status).toBe(206);
        expect(Array.from(new Uint8Array(yield* viewerRange.arrayBuffer))).toEqual([
          0, 0, 0, 0, 0, 0, 0,
        ]);
        expect(yield* a.fixtureBrowserViewers!.close(native.sessionId, nativeViewer.viewerId)).toBe(
          true,
        );
        expect(
          (yield* client.execute(HttpClientRequest.head(viewerUrl(nativeViewer.ticket)))).status,
        ).toBe(403);
        const reopenedViewer = yield* a.fixtureBrowserViewers!.open(native, {
          resource: browser,
          operation: "view",
        });
        expect(reopenedViewer.viewerId).not.toBe(nativeViewer.viewerId);
        expect(yield* a.fixtureBrowserViewers!.close(native.sessionId, nativeViewer.viewerId)).toBe(
          false,
        );
        expect(
          (yield* client.execute(HttpClientRequest.head(viewerUrl(reopenedViewer.ticket)))).status,
        ).toBe(200);
        expect(
          yield* a.fixtureBrowserViewers!.close(native.sessionId, reopenedViewer.viewerId),
        ).toBe(true);
        const secondEntered = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const hold = yield* Deferred.make<void>();
        const firstConsumed = yield* Deferred.make<void>();
        let blockReplies = 0;
        yield* Ref.set(b.artifactRequestHook, (checkOnly) =>
          Effect.gen(function* () {
            if (checkOnly) return;
            blockReplies += 1;
            if (blockReplies === 2) {
              yield* Deferred.succeed(secondEntered, undefined);
              yield* Deferred.await(hold).pipe(
                Effect.ensuring(Deferred.succeed(cancelled, undefined)),
              );
            }
          }),
        );
        const downstream = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(downstream, Exit.void));
        const cancellationTicket = yield* a.auth.issueWebSocketTicket(native);
        const streaming = yield* HttpClient.withScope(client)
          .execute(
            HttpClientRequest.get(
              `${a.url}${issued.relativeUrl}?wsTicket=${encodeURIComponent(cancellationTicket.ticket)}`,
            ),
          )
          .pipe(Scope.provide(downstream));
        expect(streaming.status).toBe(200);
        let consumed = 0;
        const consumer = yield* streaming.stream.pipe(
          Stream.runForEach((chunk) =>
            Effect.gen(function* () {
              consumed += chunk.byteLength;
              if (consumed >= 1024 * 1024) {
                yield* Deferred.succeed(firstConsumed, undefined);
                yield* Deferred.await(hold);
              }
            }),
          ),
          Effect.forkScoped,
        );
        yield* Deferred.await(firstConsumed);
        yield* Deferred.await(secondEntered);
        expect(blockReplies).toBe(2);
        yield* Scope.close(downstream, Exit.void);
        yield* Fiber.interrupt(consumer);
        yield* Deferred.await(cancelled);
        expect(blockReplies, "cancellation must stop subsequent native HTTP block reads").toBe(2);
        yield* Ref.set(b.artifactRequestHook, () => Effect.void);
        const excess = save(size + 1);
        yield* Effect.promise(() => external.emitAsync("download", excess.download));
        expect(excess.saves).toHaveLength(1);
        expect(yield* fs.exists(excess.saves[0]!)).toBe(false);
        const remaining = yield* nativeCall(a, parent, "preview_status", {
          ...route,
          tabId: browser.tabId,
        });
        expect(
          (yield* Schema.decodeUnknownEffect(
            Schema.Struct({ downloads: Schema.Array(Schema.Unknown) }),
          )(remaining.structuredContent)).downloads,
        ).toHaveLength(1);
        const closed = yield* nativeCall(a, parent, "t3_preview_close", {
          ...route,
          tabId: browser.tabId,
          clientRequestId: "download-gib-close",
        });
        expect(closed.isError, json(closed)).not.toBe(true);
        expect((yield* fetchAsset("HEAD")).status).toBe(403);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-input: public qualified browser upload reaches the exact native file input once",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-browser-input",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "browser-input-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const bytes = new Uint8Array([7, 8, 9, 10]);
        const upload = {
          type: "file" as const,
          name: "report.txt",
          mimeType: "text/plain",
          sizeBytes: bytes.length,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          upload,
          digest: yield* typed383Digest(bytes),
          clientRequestId: "browser-input-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const staged = yield* nativeCall(a, parent, "t3_attachment_inspect", {
          source: { type: "upload", attachmentId: issued.attachmentId, upload },
        });
        expect(staged.isError, json(staged)).not.toBe(true);
        const { inspection } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            inspection: Schema.Struct({
              stage: Schema.Literal("staged"),
              reference: PeerAttachmentReference,
            }),
          }),
        )(staged.structuredContent);
        const external = browserExternalContexts.at(-1)!;
        const fs = yield* FileSystem.FileSystem;
        const observed: Array<ReadonlyArray<number>> = [];
        external.controls.afterSetFiles = async (paths) => {
          for (const path of paths)
            observed.push(Array.from(await nativePromise(fs.readFile(path))));
        };
        const input = {
          ...route,
          tabId: resource.tabId,
          resource,
          artifacts: [inspection.reference],
          locator: "input[type=file]",
          clientRequestId: "browser-input-pick",
        };
        const raw = yield* nativeRpcCall(a, parent, "preview_upload", input);
        expect(
          raw,
          "qualified public upload must be registered, not interpreted as private paths",
        ).not.toContain('"error"');
        const result = (yield* sendResult(raw)).result;
        expect(result.isError, json(result)).not.toBe(true);
        expect(observed).toEqual([[7, 8, 9, 10]]);
        expect(json(result)).not.toContain(b.config.attachmentsDir);
        expect((yield* nativeCall(a, parent, "preview_upload", input)).structuredContent).toEqual(
          result.structuredContent,
        );
        expect(observed).toHaveLength(1);
        const stageAnother = (name: string) =>
          Effect.gen(function* () {
            const upload = {
              type: "file" as const,
              name,
              mimeType: "text/plain",
              sizeBytes: bytes.length,
            };
            const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
              upload,
              digest: yield* typed383Digest(bytes),
              clientRequestId: `prepare-${name}`,
            });
            expect(prepared.isError, json(prepared)).not.toBe(true);
            const issued = yield* decodeTyped383Upload(prepared.structuredContent);
            expect(
              (yield* http.execute(
                HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
                  HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
                ),
              )).status,
            ).toBe(204);
            const staged = yield* nativeCall(a, parent, "t3_attachment_inspect", {
              source: { type: "upload", attachmentId: issued.attachmentId, upload },
            });
            const { inspection } = yield* Schema.decodeUnknownEffect(
              Schema.Struct({
                inspection: Schema.Struct({
                  stage: Schema.Literal("staged"),
                  reference: PeerAttachmentReference,
                }),
              }),
            )(staged.structuredContent);
            return inspection.reference;
          });
        const second = yield* stageAnother("second.txt");
        const lostInput = { ...input, artifacts: [second], clientRequestId: "browser-input-lost" };
        yield* Ref.set(a.dropCommittedToolReply, "preview_upload");
        const lost = yield* nativeCall(a, parent, "preview_upload", lostInput);
        expect(lost.isError, json(lost)).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        expect(observed).toHaveLength(2);
        expect((yield* nativeCall(a, parent, "preview_upload", lostInput)).isError).not.toBe(true);
        expect(observed).toHaveLength(2);
        const activeA = { ...a, ...(yield* a.restartSchedulerTransport) };
        yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([activeA.url])} WHERE environment_id=${a.environmentId}`;
        const activeB = { ...b, ...(yield* b.restartSchedulerTransport) };
        yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`UPDATE peer_environment_links SET urls_json=${json([activeB.url])} WHERE environment_id=${b.environmentId}`;
        expect((yield* nativeCall(activeA, parent, "preview_upload", lostInput)).isError).not.toBe(
          true,
        );
        expect(observed).toHaveLength(2);
        const sourceRows = yield* Context.get(
          a.database,
          SqlClient.SqlClient,
        )`SELECT * FROM attachment_staging`;
        const destinationRows = yield* Context.get(
          b.database,
          SqlClient.SqlClient,
        )`SELECT * FROM attachment_staging`;
        for (const bad of [
          { ...lostInput, artifacts: [{ ...second, name: "changed.txt" }] },
          { ...lostInput, clientRequestId: "browser-input-consumed" },
          {
            ...input,
            paths: ["/private/source/report.txt"],
            artifacts: undefined,
            resource: undefined,
            clientRequestId: "browser-input-foreign-path",
          },
          {
            ...input,
            resource: { ...resource, serverEpoch: "replacement" },
            clientRequestId: "browser-input-epoch",
          },
        ])
          expect((yield* nativeCall(activeA, parent, "preview_upload", bad)).isError).toBe(true);
        expect(observed).toHaveLength(2);
        expect(
          yield* Context.get(a.database, SqlClient.SqlClient)`SELECT * FROM attachment_staging`,
        ).toEqual(sourceRows);
        expect(
          yield* Context.get(b.database, SqlClient.SqlClient)`SELECT * FROM attachment_staging`,
        ).toEqual(destinationRows);
        const rival = ThreadId.make("native383-browser-input-rival");
        yield* seedParent(a, rival);
        for (const key of [input.clientRequestId, "browser-input-rival-fresh"])
          expect(
            (yield* nativeCall(activeA, rival, "preview_upload", {
              ...input,
              clientRequestId: key,
            })).isError,
          ).toBe(true);
        expect(observed).toHaveLength(2);
        const clear = { ...input, artifacts: [], clientRequestId: "browser-input-clear" };
        const beforeClear = external.effects.filter((effect) => effect === "files").length;
        expect((yield* nativeCall(activeA, parent, "preview_upload", clear)).isError).not.toBe(
          true,
        );
        expect(external.effects.filter((effect) => effect === "files")).toHaveLength(
          beforeClear + 1,
        );
        expect((yield* nativeCall(activeA, parent, "preview_upload", clear)).isError).not.toBe(
          true,
        );
        expect(external.effects.filter((effect) => effect === "files")).toHaveLength(
          beforeClear + 1,
        );
        const chooser = makeExternalBrowserFileChooser(async (paths) => {
          for (const path of paths)
            observed.push(Array.from(await nativePromise(fs.readFile(path))));
        });
        yield* Effect.promise(() => external.emitAsync("filechooser", chooser.chooser));
        const beforeCancel = yield* nativeCall(activeA, parent, "preview_status", {
          ...route,
          tabId: resource.tabId,
        });
        expect(
          (yield* Schema.decodeUnknownEffect(Schema.Struct({ fileChooser: Schema.Unknown }))(
            beforeCancel.structuredContent,
          )).fileChooser,
        ).toEqual({ multiple: false, accept: "" });
        const { locator: _locator, ...cancel } = clear;
        expect(
          (yield* nativeCall(activeA, parent, "preview_upload", {
            ...cancel,
            clientRequestId: "browser-input-cancel",
          })).isError,
        ).not.toBe(true);
        expect(chooser.calls).toEqual([]);
        const afterCancel = yield* nativeCall(activeA, parent, "preview_status", {
          ...route,
          tabId: resource.tabId,
        });
        expect(
          (yield* Schema.decodeUnknownEffect(Schema.Struct({ fileChooser: Schema.Unknown }))(
            afterCancel.structuredContent,
          )).fileChooser,
        ).toBeNull();
        yield* b.authority.setGrant({ ...grant, preview: false });
        expect((yield* nativeCall(activeA, parent, "preview_upload", lostInput)).isError).toBe(
          true,
        );
        expect(observed).toHaveLength(2);
        yield* b.authority.setGrant({ ...grant, preview: true });
        const oldLink = yield* a.links.resolve(b.environmentId);
        yield* a.links.unlink(b.environmentId);
        yield* linkTo(a.links, activeB, "full-access");
        expect((yield* a.links.resolve(b.environmentId)).link.launchIdentity).not.toBe(
          oldLink.link.launchIdentity,
        );
        expect(
          (yield* nativeCall(activeA, parent, "t3_project_list", {
            environmentId: b.environmentId,
          })).isError,
        ).not.toBe(true);
        const replacement = (yield* b.authority.grants).find(
          (value) => value.sessionId !== grant.sessionId,
        );
        if (replacement === undefined)
          return yield* Effect.die("Expected actual replacement input grant");
        yield* b.authority.setGrant({ ...replacement, preview: true });
        for (const key of [lostInput.clientRequestId, "browser-input-new-generation"])
          expect(
            (yield* nativeCall(activeA, parent, "preview_upload", {
              ...lostInput,
              clientRequestId: key,
            })).isError,
          ).toBe(true);
        expect(observed).toHaveLength(2);
        const launched = yield* nativeCall(activeA, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "New input generation",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "browser-input-new-child",
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const newChild = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, newChild);
        const newRoute = { environmentId: b.environmentId, contextThreadId: newChild };
        const newOpening = yield* nativeCall(activeA, parent, "preview_open", {
          ...newRoute,
          show: false,
          clientRequestId: "browser-input-new-open",
        });
        expect(newOpening.isError, json(newOpening)).not.toBe(true);
        const { resource: newResource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(newOpening.structuredContent);
        expect(
          (yield* nativeCall(activeA, parent, "preview_upload", {
            ...newRoute,
            resource: newResource,
            artifacts: [],
            locator: "input[type=file]",
            clientRequestId: "browser-input-new-clear",
          })).isError,
        ).not.toBe(true);
        expect(resource.threadId).toBe(child);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-input: whole mixed browser batch is verified before new source transfer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-browser-batch",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "batch-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const http = yield* HttpClient.HttpClient;
        const stage = (remote: boolean, name: string, bytes: Uint8Array) =>
          Effect.gen(function* () {
            const target = remote ? route : {};
            const upload = {
              type: "file" as const,
              name,
              mimeType: "text/plain",
              sizeBytes: bytes.length,
            };
            const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
              ...target,
              upload,
              digest: yield* typed383Digest(bytes),
              clientRequestId: `batch-${name}`,
            });
            expect(prepared.isError, json(prepared)).not.toBe(true);
            const issued = yield* decodeTyped383Upload(prepared.structuredContent);
            expect(
              (yield* http.execute(
                HttpClientRequest.post(`${remote ? b.url : a.url}${issued.relativeUrl}`).pipe(
                  HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
                ),
              )).status,
            ).toBe(204);
            const inspected = yield* nativeCall(a, parent, "t3_attachment_inspect", {
              ...target,
              source: { type: "upload", attachmentId: issued.attachmentId, upload },
            });
            expect(inspected.isError, json(inspected)).not.toBe(true);
            return (yield* Schema.decodeUnknownEffect(
              Schema.Struct({
                inspection: Schema.Struct({
                  stage: Schema.Literal("staged"),
                  reference: PeerAttachmentReference,
                }),
              }),
            )(inspected.structuredContent)).inspection.reference;
          });
        const local = yield* stage(false, "source.txt", new Uint8Array([1, 2]));
        const destination = yield* stage(true, "destination.txt", new Uint8Array([3, 4]));
        const fs = yield* FileSystem.FileSystem;
        const external = browserExternalContexts.at(-1)!;
        const batches: number[][][] = [];
        external.controls.afterSetFiles = async (paths) => {
          const batch = [];
          for (const path of paths) batch.push(Array.from(await nativePromise(fs.readFile(path))));
          batches.push(batch);
        };
        const input = {
          ...route,
          resource,
          artifacts: [local, destination],
          locator: "input[type=file]",
          clientRequestId: "mixed-input",
        };
        const accepted = yield* nativeCall(a, parent, "preview_upload", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        expect(batches).toEqual([
          [
            [1, 2],
            [3, 4],
          ],
        ]);
        expect((yield* nativeCall(a, parent, "preview_upload", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(batches).toHaveLength(1);
        const fresh = yield* stage(false, "fresh.txt", new Uint8Array([5, 6]));
        const sourceDb = Context.get(a.database, SqlClient.SqlClient);
        const destinationDb = Context.get(b.database, SqlClient.SqlClient);
        const sourceRows = yield* sourceDb`SELECT * FROM attachment_staging`;
        const destinationRows = yield* destinationDb`SELECT * FROM attachment_staging`;
        for (const bad of [
          {
            ...input,
            artifacts: [fresh, { ...destination, digest: "0".repeat(64) }],
            clientRequestId: "mixed-forged",
          },
          { ...input, artifacts: [fresh, destination], clientRequestId: "mixed-consumed" },
        ]) {
          expect((yield* nativeCall(a, parent, "preview_upload", bad)).isError).toBe(true);
          expect(yield* sourceDb`SELECT * FROM attachment_staging`).toEqual(sourceRows);
          expect(yield* destinationDb`SELECT * FROM attachment_staging`).toEqual(destinationRows);
          expect(batches).toHaveLength(1);
        }
        for (const artifacts of [
          Array.from({ length: 21 }, () => fresh),
          [fresh, { ...destination, sizeBytes: 51 * 1024 * 1024 }],
          [
            fresh,
            { ...destination, sizeBytes: 50 * 1024 * 1024 },
            { ...fresh, attachmentId: destination.attachmentId, sizeBytes: 50 * 1024 * 1024 },
          ],
        ]) {
          const response = yield* nativeRpcCall(a, parent, "preview_upload", {
            ...input,
            artifacts,
            clientRequestId: "mixed-bounds",
          });
          expect(response).toContain('"code":-32602');
          expect(yield* sourceDb`SELECT * FROM attachment_staging`).toEqual(sourceRows);
          expect(yield* destinationDb`SELECT * FROM attachment_staging`).toEqual(destinationRows);
          expect(batches).toHaveLength(1);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "native383-input: lost destination prepare reply preserves partial browser transfer and consumes once on retry",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, route } = yield* typed383Pair(
          "native383-browser-partial",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        yield* b.authority.setGrant({ ...(yield* b.authority.grants)[0]!, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "partial-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const http = yield* HttpClient.HttpClient;
        const artifacts = [];
        for (const name of ["first.txt", "second.txt"]) {
          const bytes = new TextEncoder().encode(name);
          const upload = {
            type: "file" as const,
            name,
            mimeType: "text/plain",
            sizeBytes: bytes.length,
          };
          const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
            upload,
            digest: yield* typed383Digest(bytes),
            clientRequestId: `partial-${name}`,
          });
          expect(prepared.isError, json(prepared)).not.toBe(true);
          const issued = yield* decodeTyped383Upload(prepared.structuredContent);
          expect(
            (yield* http.execute(
              HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
                HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
              ),
            )).status,
          ).toBe(204);
          const inspected = yield* nativeCall(a, parent, "t3_attachment_inspect", {
            source: { type: "upload", attachmentId: issued.attachmentId, upload },
          });
          artifacts.push(
            (yield* Schema.decodeUnknownEffect(
              Schema.Struct({
                inspection: Schema.Struct({
                  stage: Schema.Literal("staged"),
                  reference: PeerAttachmentReference,
                }),
              }),
            )(inspected.structuredContent)).inspection.reference,
          );
        }
        const external = browserExternalContexts.at(-1)!;
        const fs = yield* FileSystem.FileSystem;
        const selected: string[][] = [];
        external.controls.afterSetFiles = async (paths) => {
          const bytes = [];
          for (const path of paths) bytes.push(await nativePromise(fs.readFileString(path)));
          selected.push(bytes);
        };
        const input = {
          ...route,
          resource,
          artifacts,
          locator: "input[type=file]",
          clientRequestId: "partial-pick",
        };
        yield* Ref.set(a.dropCommittedToolReply, "t3_attachment_prepare_upload");
        const lost = yield* nativeCall(a, parent, "preview_upload", input);
        expect(lost.isError, json(lost)).toBe(true);
        expect(json(lost)).toContain("Isolated committed tool reply loss");
        expect(selected).toEqual([]);
        const db = Context.get(b.database, SqlClient.SqlClient);
        expect(yield* db`SELECT * FROM attachment_staging`).toHaveLength(1);
        expect((yield* nativeCall(a, parent, "preview_upload", input)).isError).not.toBe(true);
        expect(selected).toEqual([["first.txt", "second.txt"]]);
        const accepted = yield* db`SELECT * FROM attachment_staging`;
        expect(accepted).toHaveLength(2);
        expect((yield* nativeCall(a, parent, "preview_upload", input)).isError).not.toBe(true);
        expect(selected).toHaveLength(1);
        expect(yield* db`SELECT * FROM attachment_staging`).toEqual(accepted);
        expect(
          (yield* nativeCall(a, parent, "preview_upload", {
            ...input,
            clientRequestId: "partial-new-consumer",
          })).isError,
        ).toBe(true);
        expect(selected).toHaveLength(1);
        expect(yield* db`SELECT * FROM attachment_staging`).toEqual(accepted);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each(["android", "ios"] as const)(
  "native383-parser: %s source and external hub reject oversize on distinct native hops",
  (platform) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          `native383-parser-${platform}`,
          false,
          false,
          { source: true, destination: true, destinationSocket: true },
        );
        yield* Ref.set(b.deviceHubPlatform, platform);
        const opened = yield* nativeCall(a, parent, "device_open", {
          ...route,
          hostId: "local",
          deviceId: "fixture-device382",
          platform,
          clientRequestId: "parser-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = yield* HttpClient.HttpClient;
        const admission = yield* http.execute(
          HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: b.environmentId,
                    threadId: child,
                    platform,
                    ...target,
                  },
                  operation: "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(admission.status).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* admission.json);
        const endpoint = `${a.url.replace(/^http/, "ws")}/api/device-hub/vendor/${platform === "ios" ? "serve-sim/helper" : "serve-emu"}/ws?device=fixture-device382&viewerTicket=${encodeURIComponent(viewer.ticket)}`;
        const connect = Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<NodeWS.WebSocket>((resolve, reject) => {
                const socket = new NodeWS.WebSocket(endpoint);
                socket.on("error", () => {});
                socket.once("open", () => resolve(socket));
                socket.once("error", reject);
              }),
          ),
          (socket) => Effect.sync(() => socket.terminate()),
        );
        const input =
          platform === "ios"
            ? new TextEncoder().encode('\x03{"type":"begin","x":0.25,"y":0.5}')
            : '{"type":"touch","action":"down","x":0.25,"y":0.5}';
        const messageOrClose = (socket: NodeWS.WebSocket) =>
          new Promise<{ kind: "message"; bytes: number } | { kind: "close"; code: number }>(
            (resolve) => {
              const message = (bytes: Buffer) => {
                socket.off("close", close);
                resolve({ kind: "message", bytes: bytes.byteLength });
              };
              const close = (code: number) => {
                socket.off("message", message);
                resolve({ kind: "close", code });
              };
              socket.once("message", message);
              socket.once("close", close);
            },
          );
        const source = yield* connect;
        const lawful = messageOrClose(source);
        source.send(input);
        expect(yield* Effect.promise(() => lawful)).toEqual({ kind: "message", bytes: 11 });
        expect(b.deviceViewerInputs).toHaveLength(1);
        const sourceRejected = messageOrClose(source);
        source.send(Buffer.alloc(9 * 1024 * 1024));
        expect(yield* Effect.promise(() => sourceRejected)).toEqual({ kind: "close", code: 1009 });
        expect(b.deviceViewerInputs).toHaveLength(1);
        const downstream = yield* connect;
        const lawfulAgain = messageOrClose(downstream);
        downstream.send(input);
        expect(yield* Effect.promise(() => lawfulAgain)).toEqual({ kind: "message", bytes: 11 });
        expect(b.deviceViewerInputs).toHaveLength(2);
        b.deviceViewerOutput.bytes = Buffer.alloc(9 * 1024 * 1024);
        const hubRejected = messageOrClose(downstream);
        downstream.send(input);
        expect((yield* Effect.promise(() => hubRejected)).kind).toBe("close");
        expect(b.deviceViewerInputs).toHaveLength(3);
        expect(a.deviceViewerInputs).toEqual([]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
);

it.live(
  "native383-http: current native session retired while control body arrives cannot dispatch another hub mutation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const n = yield* makeNode(
          descriptorOf("native383-http-body", "Local", { peerAuthority: 1 }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            deviceOwner: true,
            viewerOwner: true,
          },
        );
        const parent = ThreadId.make("native383-http-body-parent");
        yield* seedParent(n, parent);
        const opened = yield* nativeCall(n, parent, "device_open", {
          hostId: "local",
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "http-body-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* n.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const http = yield* HttpClient.HttpClient;
        const admitted = yield* http.execute(
          HttpClientRequest.post(`${n.url}/api/device-hub/viewer/open`).pipe(
            HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
            HttpClientRequest.setBody(
              HttpBody.text(
                json({
                  resource: {
                    type: "device",
                    environmentId: n.environmentId,
                    threadId: parent,
                    platform: "android",
                    ...target,
                  },
                  operation: "control",
                }),
                "application/json",
              ),
            ),
          ),
        );
        expect(admitted.status).toBe(200);
        const viewer = yield* decodeDeviceViewerAccess(yield* admitted.json);
        const endpoint = `${n.url}/api/device-hub/vendor/serve-emu/api/fold?device=fixture-device382&viewerTicket=${encodeURIComponent(viewer.ticket)}`;
        const lawful = yield* http.execute(
          HttpClientRequest.post(endpoint).pipe(
            HttpClientRequest.bodyText('{"posture":"closed"}', "application/json"),
          ),
        );
        expect(lawful.status).toBe(200);
        yield* lawful.text;
        expect(n.deviceViewerHttpInputs.map((value) => value.body)).toEqual([
          '{"posture":"closed"}',
        ]);
        const oversized = yield* http.execute(
          HttpClientRequest.post(endpoint).pipe(
            HttpClientRequest.bodyText('"' + "x".repeat(8 * 1024) + '"', "application/json"),
          ),
        );
        expect(oversized.status).toBe(413);
        yield* oversized.text;
        expect(n.deviceViewerHttpInputs).toHaveLength(1);
        const beforeBody = yield* Deferred.make<void>();
        let checks = 0;
        yield* Ref.set(
          n.deviceViewerCurrentHook,
          Effect.gen(function* () {
            checks++;
            if (checks === 2) yield* Deferred.succeed(beforeBody, undefined);
          }),
        );
        const nativeSession = yield* n.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(n.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const tail = Deferred.await(beforeBody).pipe(
          Effect.andThen(n.auth.revokeSession(nativeSession.sessionId)),
          Effect.as(new TextEncoder().encode('"opened"}')),
        );
        const bytes = Stream.concat(
          Stream.succeed(new TextEncoder().encode('{"posture":')),
          Stream.fromEffect(tail),
        );
        const retired = yield* http.execute(
          HttpClientRequest.post(endpoint).pipe(
            HttpClientRequest.bodyStream(bytes, { contentType: "application/json" }),
          ),
        );
        yield* retired.text.pipe(Effect.result);
        expect(checks).toBeGreaterThanOrEqual(2);
        expect(
          n.deviceViewerHttpInputs,
          "no new hub effect after real source session retirement at the body wait",
        ).toHaveLength(1);
        expect(retired.status).toBe(401);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
);

it.live(
  "native383-rpc-groups: native authenticated core and real command output stream enforce scopes and finalize on session retirement",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent } = yield* typed383Pair("native383-rpc-groups", false, false, {
          source: false,
          destination: false,
          destinationBrowser: true,
        });
        const now = yield* DateTime.now;
        const itemId = TurnItemId.make("native383-rpc-command");
        const item = {
          id: itemId,
          threadId: parent,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 100,
          title: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
          type: "command_execution",
          input: "deterministic external output",
          status: "running",
        } satisfies OrchestrationV2TurnItem;
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("native383-rpc-command-start"),
              type: "turn-item.updated",
              threadId: parent,
              occurredAt: now,
              payload: item,
            },
          ],
        });
        yield* a.commandOutput!.append({ threadId: parent, itemId, chunk: "first native chunk\n" });
        const decodeReply = Schema.decodeUnknownEffect(
          Schema.fromJsonString(
            Schema.Struct({
              _tag: Schema.String,
              requestId: Schema.optionalKey(Schema.String),
              values: Schema.optionalKey(Schema.Array(OrchestrationV2CommandOutputFrame)),
              exit: Schema.optionalKey(
                Schema.Struct({
                  _tag: Schema.String,
                  value: Schema.optionalKey(Schema.Unknown),
                  cause: Schema.optionalKey(Schema.Unknown),
                }),
              ),
            }),
          ),
        );
        const connect = (
          scopes: ReadonlyArray<import("@t3tools/contracts").AuthEnvironmentScope>,
        ) =>
          Effect.gen(function* () {
            const owner = yield* a.auth.issueSession({ scopes });
            const authenticated = yield* a.auth.authenticateHttpRequest(
              HttpServerRequest.fromWeb(
                new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
              ),
            );
            const ticket = yield* a.auth.issueWebSocketTicket(authenticated);
            const messages = yield* Queue.unbounded<string>();
            const closed = Promise.withResolvers<void>();
            const socket = yield* Effect.acquireRelease(
              Effect.tryPromise({
                try: () =>
                  new Promise<WebSocket>((resolve, reject) => {
                    const ws = new WebSocket(
                      `${a.nativeViewerRpcUrl}/ws?wsTicket=${encodeURIComponent(ticket.ticket)}`,
                    );
                    ws.addEventListener("message", (event) =>
                      Queue.offerUnsafe(messages, String(event.data)),
                    );
                    ws.addEventListener("open", () => resolve(ws), { once: true });
                    ws.addEventListener(
                      "error",
                      () => reject(new Error("Native owner RPC refused")),
                      { once: true },
                    );
                    ws.addEventListener("close", () => closed.resolve(), { once: true });
                  }),
                catch: (cause) =>
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: String(cause),
                  }),
              }),
              (ws) => Effect.sync(() => ws.close()),
            );
            return {
              owner,
              authenticated,
              closed,
              send: (id: string, tag: string, payload: unknown) =>
                Effect.sync(() => {
                  socket.send(json({ _tag: "Request", id, tag, payload, headers: [] }));
                }),
              next: Queue.take(messages).pipe(
                Effect.flatMap(decodeReply),
                Effect.tap((reply) =>
                  reply._tag === "Chunk"
                    ? Effect.sync(() =>
                        socket.send(json({ _tag: "Ack", requestId: reply.requestId })),
                      )
                    : Effect.void,
                ),
              ),
            };
          });
        const denied = yield* connect(["preview:operate"]);
        yield* denied.send("1", WS_METHODS.peerLinksList, {});
        const coreDenied = yield* denied.next;
        expect(coreDenied.exit?._tag).toBe("Failure");
        expect(json(coreDenied)).toContain("access:read");
        yield* denied.send("2", ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput, {
          threadId: parent,
          itemId,
        });
        const streamDenied = yield* denied.next;
        expect(streamDenied.exit?._tag).toBe("Failure");
        expect(json(streamDenied)).toContain("orchestration:read");
        const allowed = yield* connect(["access:read", "orchestration:read"]);
        yield* allowed.send("1", WS_METHODS.peerLinksList, {});
        const core = yield* allowed.next;
        expect(core.exit?._tag).toBe("Success");
        expect(core.exit?.value).toMatchObject({
          links: [{ environmentId: b.environmentId, label: "B", status: "reachable" }],
        });
        expect(json(core)).not.toContain("launchIdentity");
        yield* allowed.send("2", ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput, {
          threadId: parent,
          itemId,
        });
        const first = yield* allowed.next;
        expect(first._tag).toBe("Chunk");
        expect(first.requestId).toBe("2");
        expect(first.values).toEqual([
          { kind: "replace", text: "first native chunk\n", truncated: false, running: true },
        ]);
        yield* a.commandOutput!.append({
          threadId: parent,
          itemId,
          chunk: "second native chunk\n",
        });
        const append = yield* allowed.next;
        expect(append._tag).toBe("Chunk");
        expect(append.values).toEqual([
          { kind: "append", text: "second native chunk\n", truncated: false, running: true },
        ]);
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("native383-rpc-command-finish"),
              type: "turn-item.updated",
              threadId: parent,
              occurredAt: now,
              payload: {
                ...item,
                status: "completed",
                completedAt: now,
                output: "persisted final native output",
              },
            },
          ],
        });
        yield* a.commandOutput!.finish({ threadId: parent, itemId });
        const final = yield* allowed.next;
        expect(final._tag).toBe("Chunk");
        expect(final.values).toEqual([
          {
            kind: "replace",
            text: "persisted final native output",
            truncated: false,
            running: false,
          },
        ]);
        expect((yield* allowed.next).exit?._tag).toBe("Success");
        expect(yield* Queue.take(a.nativeCommandOutputFinalized)).toBe(parent);
        const liveItemId = TurnItemId.make("native383-rpc-command-retire");
        yield* a.sink.write({
          events: [
            {
              id: EventId.make("native383-rpc-command-retire-start"),
              type: "turn-item.updated",
              threadId: parent,
              occurredAt: now,
              payload: { ...item, id: liveItemId, ordinal: 101 },
            },
          ],
        });
        yield* allowed.send("3", ORCHESTRATION_V2_WS_METHODS.subscribeCommandOutput, {
          threadId: parent,
          itemId: liveItemId,
        });
        expect((yield* allowed.next).values?.[0]?.running).toBe(true);
        // Revoke through the same native session owner used by the production-style socket lifetime.
        expect(yield* a.fixtureBrowserViewerSessions!.revoke(allowed.owner.sessionId)).toBe(true);
        yield* Effect.promise(() => allowed.closed.promise);
        expect(yield* Queue.take(a.nativeCommandOutputFinalized)).toBe(parent);
        // Ticket minting is not admission. The real upgrade owner must reject the retired session.
        const retiredTicket = yield* a.auth.issueWebSocketTicket(allowed.authenticated);
        const retired = yield* a.auth
          .authenticateWebSocketUpgrade(
            HttpServerRequest.fromWeb(
              new Request(
                `${a.nativeViewerRpcUrl!.replace(/^ws/, "http")}/ws?wsTicket=${encodeURIComponent(retiredTicket.ticket)}`,
              ),
            ),
          )
          .pipe(Effect.result);
        expect(retired._tag).toBe("Failure");
        yield* a.commandOutput!.append({
          threadId: parent,
          itemId: liveItemId,
          chunk: "after retirement",
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each([false, true])(
  "recording534-preview: pending read retains current authority (retire: %s)",
  (retireOnPublication) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("recording382-source", "Source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const realFs = yield* FileSystem.FileSystem;
        let retireOnRead = false;
        let retire: Effect.Effect<unknown, OrchestratorMcpFailure> = Effect.void;
        let intercepted = 0;
        const recordingFs = FileSystem.FileSystem.of({
          ...realFs,
          readFile: (path) =>
            realFs.readFile(path).pipe(
              Effect.tap(() =>
                Effect.gen(function* () {
                  if (!retireOnRead || !path.includes("pending-")) return;
                  retireOnRead = false;
                  intercepted++;
                  yield* retire.pipe(Effect.orDie);
                }),
              ),
            ),
        });
        const b = yield* makeNode(
          descriptorOf("recording382-destination", "Destination", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        ).pipe(Effect.provideService(FileSystem.FileSystem, recordingFs));
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("recording382-parent");
        yield* seedParent(a, parent);
        const launched = yield* nativeCall(a, parent, "t3_thread_launch", {
          environmentId: b.environmentId,
          projectId: b.project.id,
          title: "Recording owner",
          message: "Wait",
          modelSelection: selection,
          clientRequestId: "recording382-owner",
        });
        expect(launched.isError, json(launched)).not.toBe(true);
        const child = (yield* decodeLaunchReceipt(launched.structuredContent)).threadId;
        yield* activate(b, child);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const bytes = new Uint8Array([0x1a, 0x45, 0xdf, 0xa3, 0x81, 1, 0x42, 0x86, 0x81, 1]);
        const uploaded = yield* AttachmentUpload.issueAttachmentUploadUrl({
          type: "file",
          name: "recording.webm",
          mimeType: "video/webm",
          sizeBytes: bytes.byteLength,
        }).pipe(
          Effect.provideService(Config.ServerConfig, b.config),
          Effect.provideService(
            Secrets.ServerSecretStore,
            Context.get(b.services, Secrets.ServerSecretStore),
          ),
        );
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${b.url}${uploaded.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const connected = yield* Deferred.make<void>();
        let stops = 0;
        yield* Stream.runForEach(
          yield* b.browser.connect({
            clientId: "recording382-renderer",
            environmentId: b.environmentId,
          }),
          (event) =>
            event.type === "connected"
              ? Deferred.succeed(connected, undefined)
              : Effect.gen(function* () {
                  const request = event.request;
                  if (request.operation === "recordingStop") {
                    stops++;
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        id: "renderer-recording",
                        tabId: "recording382-tab",
                        path: "/isolated-renderer/recording.webm",
                        uploadedAttachmentId: uploaded.attachmentId,
                        mimeType: "video/webm",
                        sizeBytes: bytes.byteLength,
                        createdAt: "2026-10-18T00:00:00Z",
                      },
                    });
                  } else
                    yield* b.browser.respond({
                      clientId: "recording382-renderer",
                      connectionId: event.connectionId,
                      requestId: request.requestId,
                      ok: true,
                      result: {
                        available: true,
                        visible: true,
                        tabId: null,
                        url: null,
                        title: null,
                        loading: false,
                      },
                    });
                }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        const input = {
          environmentId: b.environmentId,
          contextThreadId: child,
          clientRequestId: "recording382-stop",
        };
        retire = b.authority.setGrant({ ...grant, preview: false });
        retireOnRead = retireOnPublication;
        const stopped = yield* nativeCall(a, parent, "preview_recording_stop", input);
        expect(intercepted).toBe(retireOnPublication ? 1 : 0);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const published =
          yield* sql`SELECT id,digest,publication_capability FROM attachment_staging WHERE publication_capability='preview'`;
        if (retireOnPublication) {
          expect((yield* b.authority.grants)[0]?.preview).toBe(false);
          expect(stopped.isError, json(stopped)).toBe(true);
          expect(json(stopped)).toContain("capability_denied");
          expect(published).toHaveLength(0);
          expect(
            (yield* realFs.readDirectory(b.config.attachmentsDir)).filter(
              (name) => !name.startsWith("pending-"),
            ),
          ).toEqual([]);
          expect(stops).toBe(1);
          expect((yield* nativeCall(a, parent, "preview_recording_stop", input)).isError).toBe(
            true,
          );
          expect(stops, "revoked replay must not stop the renderer again").toBe(1);
        } else {
          expect(stopped.isError, json(stopped)).not.toBe(true);
          const { resource } = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ resource: PeerAttachmentReference }),
          )(stopped.structuredContent);
          expect(published).toEqual([
            {
              id: resource.attachmentId,
              digest: yield* typed383Digest(bytes),
              publication_capability: "preview",
            },
          ]);
          const file = resolveAttachmentPathById({
            attachmentsDir: b.config.attachmentsDir,
            attachmentId: resource.attachmentId,
          })!;
          expect(Array.from(yield* realFs.readFile(file))).toEqual(Array.from(bytes));
          expect(
            (yield* nativeCall(a, parent, "preview_recording_stop", input)).structuredContent,
          ).toEqual(stopped.structuredContent);
          expect(stops).toBe(1);
        }
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each([false, true])(
  "recording534-device: pending read retains current authority (retire: %s)",
  (retireOnPublication) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        let retire: Effect.Effect<unknown, OrchestratorMcpFailure> = Effect.void;
        let armed = false;
        let intercepted = 0;
        const recordingFs = FileSystem.FileSystem.of({
          ...fs,
          readFile: (file) =>
            fs.readFile(file).pipe(
              Effect.tap(() =>
                Effect.gen(function* () {
                  if (!armed || !file.includes("pending-")) return;
                  armed = false;
                  intercepted++;
                  yield* retire.pipe(Effect.orDie);
                }),
              ),
            ),
        });
        yield* Effect.gen(function* () {
          const { a, b, parent, route, open } = yield* typed383Pair("recording534-device");
          const opened = yield* open("recording-open");
          expect(opened.isError, json(opened)).not.toBe(true);
          const { target } = yield* decodeTyped383Open(opened.structuredContent);
          const drive = (action: "start" | "stop") =>
            nativeCall(a, parent, "device_execute", {
              ...route,
              target,
              command: { type: "record", action },
              clientRequestId: `recording-${action}`,
            });
          expect((yield* drive("start")).isError).not.toBe(true);
          const grant = (yield* b.authority.grants)[0]!;
          retire = b.authority.setGrant({ ...grant, device: false });
          armed = retireOnPublication;
          const stopped = yield* drive("stop");
          expect(intercepted).toBe(retireOnPublication ? 1 : 0);
          const sql = Context.get(b.database, SqlClient.SqlClient);
          const published =
            yield* sql`SELECT id,digest FROM attachment_staging WHERE publication_capability='device'`;
          const calls = b.deviceProcessCalls.length;
          if (retireOnPublication) {
            expect((yield* b.authority.grants)[0]?.device).toBe(false);
            expect(stopped.isError, json(stopped)).toBe(true);
            expect(json(stopped)).toContain("outcome_unknown");
            expect(published).toHaveLength(0);
            expect(
              (yield* fs.readDirectory(b.config.attachmentsDir)).filter(
                (name) => !name.startsWith("pending-"),
              ),
            ).toEqual([]);
            expect((yield* drive("stop")).isError).toBe(true);
            yield* b.authority.setGrant({ ...grant, device: true });
            expect(json(yield* drive("stop"))).toContain("outcome_unknown");
          } else {
            expect(stopped.isError, json(stopped)).not.toBe(true);
            const result = yield* decodeTyped383Command(stopped.structuredContent);
            if (result.type !== "artifacts")
              return yield* Effect.die("Expected recording reference");
            const reference = result.artifacts[0]!;
            const file = resolveAttachmentPathById({
              attachmentsDir: b.config.attachmentsDir,
              attachmentId: reference.attachmentId,
            })!;
            const bytes = yield* fs.readFile(file);
            expect(new DataView(bytes.buffer, bytes.byteOffset).getUint32(4)).toBe(0x66747970);
            expect(published).toEqual([
              { id: reference.attachmentId, digest: yield* typed383Digest(bytes) },
            ]);
            expect((yield* drive("stop")).structuredContent).toEqual(stopped.structuredContent);
          }
          expect(
            b.deviceProcessCalls,
            "publication replay must not repeat native recording stop",
          ).toHaveLength(calls);
        }).pipe(Effect.provideService(FileSystem.FileSystem, recordingFs));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview_upload", "preview_navigate"] as const)(
  "review534-r2: original preview grant survives %s receipt wait",
  (tool) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          "native383-browser-input",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "browser-input-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const bytes = new Uint8Array([7, 8, 9, 10]);
        const upload = {
          type: "file" as const,
          name: "report.txt",
          mimeType: "text/plain",
          sizeBytes: bytes.length,
        };
        const prepared = yield* nativeCall(a, parent, "t3_attachment_prepare_upload", {
          upload,
          digest: yield* typed383Digest(bytes),
          clientRequestId: "browser-input-upload",
        });
        expect(prepared.isError, json(prepared)).not.toBe(true);
        const issued = yield* decodeTyped383Upload(prepared.structuredContent);
        const http = yield* HttpClient.HttpClient;
        expect(
          (yield* http.execute(
            HttpClientRequest.post(`${a.url}${issued.relativeUrl}`).pipe(
              HttpClientRequest.setBody(HttpBody.uint8Array(bytes)),
            ),
          )).status,
        ).toBe(204);
        const staged = yield* nativeCall(a, parent, "t3_attachment_inspect", {
          source: { type: "upload", attachmentId: issued.attachmentId, upload },
        });
        expect(staged.isError, json(staged)).not.toBe(true);
        const { inspection } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({
            inspection: Schema.Struct({
              stage: Schema.Literal("staged"),
              reference: PeerAttachmentReference,
            }),
          }),
        )(staged.structuredContent);
        const external = browserExternalContexts.at(-1)!;
        const fs = yield* FileSystem.FileSystem;
        const observed: Array<ReadonlyArray<number>> = [];
        external.controls.afterSetFiles = async (paths) => {
          for (const path of paths)
            observed.push(Array.from(await nativePromise(fs.readFile(path))));
        };
        const input = {
          ...route,
          tabId: resource.tabId,
          resource,
          artifacts: [inspection.reference],
          locator: "input[type=file]",
          clientRequestId: "browser-input-pick",
        };
        const raw = yield* nativeRpcCall(a, parent, "preview_upload", input);
        expect(
          raw,
          "qualified public upload must be registered, not interpreted as private paths",
        ).not.toContain('"error"');
        const result = (yield* sendResult(raw)).result;
        expect(result.isError, json(result)).not.toBe(true);
        expect(observed).toEqual([[7, 8, 9, 10]]);
        expect(json(result)).not.toContain(b.config.attachmentsDir);
        expect((yield* nativeCall(a, parent, "preview_upload", input)).structuredContent).toEqual(
          result.structuredContent,
        );
        expect(observed).toHaveLength(1);
        const replayInput =
          tool === "preview_upload"
            ? input
            : {
                ...route,
                tabId: resource.tabId,
                url: "https://example.test/next",
                clientRequestId: "browser-navigation-replay",
              };
        const lawful = yield* nativeCall(a, parent, tool, replayInput);
        expect(lawful.isError, json(lawful)).not.toBe(true);
        const effects = [...external.effects];
        expect((yield* nativeCall(a, parent, tool, replayInput)).structuredContent).toEqual(
          lawful.structuredContent,
        );
        expect(external.effects).toEqual(effects);
        const receiptOwner = Context.getOption(
          b.requestContext,
          RequestReceipts.McpRequestReceipts,
        );
        if (Option.isNone(receiptOwner)) return yield* Effect.die("Missing real receipt owner");
        const receipts = receiptOwner.value;
        const actualReplay = receipts.replay;
        let retired = 0;
        const observation = vi.spyOn(receipts, "replay").mockImplementation((identity) =>
          actualReplay(identity).pipe(
            Effect.tap((value) =>
              Effect.gen(function* () {
                if (Option.isNone(value) || retired !== 0) return;
                retired++;
                yield* b.authority.setGrant({ ...grant, preview: false });
              }),
            ),
          ),
        );
        const replayed = yield* nativeCall(a, parent, tool, replayInput).pipe(
          Effect.ensuring(Effect.sync(() => observation.mockRestore())),
        );
        expect((yield* b.authority.grants)[0]?.preview).toBe(false);
        expect(external.effects).toEqual(effects);
        expect(retired).toBe(1);
        expect(observed).toHaveLength(1);
        expect(replayed.isError, "receipt wait retired the real preview grant").toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live(
  "review534-r3: failed source device admission closes the already opened B viewer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route, open } = yield* typed383Pair(
          "device-partial-open",
          false,
          false,
          true,
        );
        const opened = yield* open("device-partial-native-open");
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const peers = Context.get(a.services, PeerMcpClient.PeerMcpClient);
        const actual = peers.openDeviceViewer;
        if (actual === undefined) return yield* Effect.die("Missing real remote viewer owner");
        let accepted: DeviceViewerAccess | undefined;
        const observation = vi.spyOn(peers, "openDeviceViewer").mockImplementation((input) =>
          actual(input).pipe(
            Effect.tap((result) =>
              Effect.gen(function* () {
                accepted = result.access;
                yield* a.auth.revokeSession(owner.sessionId).pipe(Effect.orDie);
              }),
            ),
          ),
        );
        const response = yield* (yield* HttpClient.HttpClient)
          .execute(
            HttpClientRequest.post(`${a.url}/api/device-hub/viewer/open`).pipe(
              HttpClientRequest.setHeader("authorization", `Bearer ${owner.token}`),
              HttpClientRequest.setBody(
                HttpBody.text(
                  json({
                    resource: {
                      type: "device",
                      environmentId: b.environmentId,
                      threadId: child,
                      platform: "android",
                      ...target,
                    },
                    operation: "control",
                  }),
                  "application/json",
                ),
              ),
            ),
          )
          .pipe(Effect.ensuring(Effect.sync(() => observation.mockRestore())));
        expect(response.status).not.toBe(200);
        if (accepted === undefined) return yield* Effect.die("Remote admission was not exercised");
        expect(
          (yield* b
            .fixtureViewerSessions!.authenticateViewerToken(accepted.ticket)
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  { pattern: "toggle", count: 8 },
  { pattern: "take", count: 8 },
  { pattern: "toggle", count: 9 },
  { pattern: "take", count: 9 },
] as const)(
  "backlog383-r9: bounded $count $pattern controls preserve dialog progress behind a stalled action",
  ({ pattern, count }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r9-control-lane-${pattern}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          {
            ...route,
            show: false,
            clientRequestId: `r9-open-${pattern}`,
          },
          undefined,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const access = yield* a.fixtureBrowserViewers!.open(native, {
          resource,
          operation: "control",
        });
        const drained = Promise.withResolvers<void>();
        const released = Promise.withResolvers<void>();
        let releasing = false;
        const started = Promise.withResolvers<void>();
        const finish = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => finish.resolve()));
        const external = browserExternalContexts.at(-1)!;
        external.controls.afterInput = async (method) => {
          if (
            method === "Input.insertText" &&
            external.inputs.at(-1)?.params?.text === "after-dialog"
          )
            drained.resolve();
          if (method === "Input.insertText" && external.inputs.at(-1)?.params?.text === "hold") {
            started.resolve();
            await finish.promise;
          }
        };
        const controlled = Promise.withResolvers<void>();
        const closed = Promise.withResolvers<number>();
        const controlsReserved = Array.from({ length: 8 }, () => Promise.withResolvers<void>());
        const params = new URLSearchParams({
          threadId: child,
          tabId: resource.tabId,
          viewerTicket: access.ticket,
        });
        const socket = yield* Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<WebSocket>((resolve, reject) => {
                const ws = new WebSocket(
                  `${a.url.replace(/^http/, "ws")}/api/preview-stream/ws?${params}`,
                );
                ws.binaryType = "arraybuffer";
                ws.addEventListener("open", () => resolve(ws), { once: true });
                ws.addEventListener("error", () => reject(new Error("r9 viewer refused")), {
                  once: true,
                });
                ws.addEventListener("close", (event) => closed.resolve(event.code), {
                  once: true,
                });
                ws.addEventListener("message", (event) => {
                  if (typeof event.data !== "string") {
                    ws.send(json({ type: "ack" }));
                    return;
                  }
                  const message = JSON.parse(event.data);
                  if (
                    releasing &&
                    message.type === "control" &&
                    message.controller !== "you" &&
                    message.generation >= (pattern === "toggle" ? 10 : 2)
                  )
                    released.resolve();
                  if (message.type === "control" && message.controller === "you")
                    controlled.resolve();
                  if (message.type === "control")
                    controlsReserved[message.generation - 2]?.resolve();
                });
              }),
          ),
          (ws) => Effect.sync(() => ws.close()),
        );
        socket.send(json({ type: "takeControl" }));
        yield* Effect.promise(() => controlled.promise);
        socket.send(json({ type: "text", text: "hold" }));
        yield* Effect.promise(() => started.promise);
        const observedOwner = Promise.withResolvers<void>();
        const retiredOwner = Promise.withResolvers<void>();
        let sawOwner = false;
        const observerAccess = yield* a.fixtureBrowserViewers!.open(native, {
          resource,
          operation: "view",
        });
        const observer = createPreviewStreamClient(
          {
            threadId: child,
            tabId: resource.tabId,
            maxWidth: 800,
            maxHeight: 600,
            access: {
              httpBase: `${a.url}/api/preview-stream`,
              wsBase: `${a.url.replace(/^http/, "ws")}/api/preview-stream`,
              query: { viewerTicket: observerAccess.ticket },
              credentials: false,
              canOperate: false,
            },
          },
          {
            onFrame: () => {},
            onViewport: () => {},
            onConnectedChange: () => {},
            onUnauthorized: () => {},
            onControl: (control) => {
              if (control.controller === "another-viewer") {
                sawOwner = true;
                observedOwner.resolve();
              } else if (
                sawOwner &&
                control.controller === "agent" &&
                control.generation >= (pattern === "toggle" ? 10 : 2)
              )
                retiredOwner.resolve();
            },
          },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => observer.stop()));
        yield* Effect.promise(() => observedOwner.promise);
        const dialogAnswered = Promise.withResolvers<void>();
        const promptText = "界".repeat(16_384);
        let answers = 0;
        if (count === 8)
          external.emit("dialog", {
            type: () => "prompt",
            message: () => "unblock",
            defaultValue: () => "",
            accept: async (text: string) => {
              expect(text).toBe(promptText);
              answers++;
              dialogAnswered.resolve();
            },
            dismiss: async () => {},
          });
        for (let i = 0; i < count; i++) {
          socket.send(
            json({
              type: pattern === "take" || i % 2 === 1 ? "takeControl" : "releaseControl",
            }),
          );
          if (pattern === "toggle" && i < controlsReserved.length)
            yield* Effect.promise(() => controlsReserved[i]!.promise);
        }
        if (count === 8) {
          if (pattern === "toggle") yield* Effect.promise(() => controlsReserved[7]!.promise);
          socket.send(json({ type: "dialog", accept: true, promptText }));
        }
        const outcome = yield* Effect.promise(() =>
          Promise.race([
            dialogAnswered.promise.then(() => "dialog answered" as const),
            closed.promise.then((code) => `closed ${code}` as const),
          ]),
        );

        finish.resolve();
        if (outcome === "dialog answered") {
          socket.send(json({ type: "text", text: "after-dialog" }));
          yield* Effect.promise(() => drained.promise);
          releasing = true;
          socket.send(json({ type: "releaseControl" }));
          yield* Effect.promise(() => released.promise);
          const resumed = yield* nativeCall(
            a,
            parent,
            "preview_navigate",
            {
              ...route,
              tabId: resource.tabId,
              url: "https://example.test/after-dialog",
            },
            undefined,
            credential,
          );
          expect(resumed.isError, json(resumed)).not.toBe(true);
        }
        socket.close();
        yield* Effect.promise(() => closed.promise);
        expect(yield* a.fixtureBrowserViewers!.close(owner.sessionId, access.viewerId)).toBe(true);
        yield* Effect.promise(() => retiredOwner.promise);
        const viewerAnswers = answers;
        if (count === 8 && outcome !== "dialog answered") {
          // Dispose a pending fixture dialog when the regression refuses its reply.
          yield* b.nativePreviewManager!.close({ threadId: child, tabId: resource.tabId });
        } else {
          const afterClose = yield* nativeCall(
            a,
            parent,
            "preview_navigate",
            {
              ...route,
              tabId: resource.tabId,
              url: "https://example.test/drained-after-close",
            },
            undefined,
            credential,
          );
          expect(afterClose.isError, json(afterClose)).not.toBe(true);
        }
        expect(
          outcome,
          "a dialog reply must stay admitted while lawful control messages wait behind the stalled action",
        ).toBe(count === 8 ? "dialog answered" : "closed 1013");
        expect(viewerAnswers).toBe(count === 8 ? 1 : 0);
        expect(answers).toBe(count === 8 ? 1 : 0);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live(
  "backlog383-r9: local native viewer large paste keeps the original truncating behavior",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair("r9-local-paste", false, false, {
          source: false,
          destination: false,
          destinationBrowser: true,
        });
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          {
            ...route,
            show: false,
            clientRequestId: "r9-open-paste",
          },
          undefined,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const local = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const { ticket } = yield* b.auth.issueWebSocketTicket(local);
        const external = browserExternalContexts.at(-1)!;
        const inserted = Promise.withResolvers<string>();
        const afterPaste = Promise.withResolvers<void>();
        const paste = "p".repeat(9999) + "界" + "q".repeat(290_000);
        external.controls.afterInput = async (method) => {
          if (method === "Input.insertText") {
            const text = String(external.inputs.at(-1)?.params?.text ?? "");
            if (text === "after-paste") afterPaste.resolve();
            else inserted.resolve(text);
          }
        };
        const released = Promise.withResolvers<void>();
        let releasing = false;
        const controlled = Promise.withResolvers<void>();
        const closed = Promise.withResolvers<number>();
        const params = new URLSearchParams({
          threadId: child,
          tabId: resource.tabId,
          wsTicket: ticket,
        });
        const socket = yield* Effect.acquireRelease(
          Effect.promise(
            () =>
              new Promise<WebSocket>((resolve, reject) => {
                const ws = new WebSocket(
                  `${b.url.replace(/^http/, "ws")}/api/preview-stream/ws?${params}`,
                );
                ws.binaryType = "arraybuffer";
                ws.addEventListener("open", () => resolve(ws), { once: true });
                ws.addEventListener("error", () => reject(new Error("r9 local viewer refused")), {
                  once: true,
                });
                ws.addEventListener("close", (event) => closed.resolve(event.code), {
                  once: true,
                });
                ws.addEventListener("message", (event) => {
                  if (typeof event.data !== "string") {
                    ws.send(json({ type: "ack" }));
                    return;
                  }
                  const message = JSON.parse(event.data);
                  if (releasing && message.type === "control" && message.controller !== "you")
                    released.resolve();
                  if (message.type === "control" && message.controller === "you")
                    controlled.resolve();
                });
              }),
          ),
          (ws) => Effect.sync(() => ws.close()),
        );
        socket.send(json({ type: "takeControl" }));
        yield* Effect.promise(() => controlled.promise);
        // A large clipboard paste, as ServerBrowserSurface onPaste sends it.
        socket.send(json({ type: "text", text: paste }));
        const outcome = yield* Effect.promise(() =>
          Promise.race([
            inserted.promise.then((text) => text),
            closed.promise.then((code) => `closed ${code}` as const),
          ]),
        );

        expect(outcome, "the local paste must still insert its truncated text").toBe(
          "p".repeat(9999) + "界",
        );
        socket.send(json({ type: "text", text: "after-paste", ignored: "x".repeat(300_000) }));
        yield* Effect.promise(() => afterPaste.promise);
        expect(socket.readyState).toBe(WebSocket.OPEN);
        releasing = true;
        socket.send(json({ type: "releaseControl" }));
        yield* Effect.promise(() => released.promise);
        const resumed = yield* nativeCall(
          a,
          parent,
          "preview_navigate",
          {
            ...route,
            tabId: resource.tabId,
            url: "https://example.test/after-local-paste",
          },
          undefined,
          credential,
        );
        expect(resumed.isError, json(resumed)).not.toBe(true);
        expect(external.inputs.filter((entry) => entry.method === "Input.insertText")).toEqual([
          { method: "Input.insertText", params: { text: "p".repeat(9999) + "界" } },
          { method: "Input.insertText", params: { text: "after-paste" } },
        ]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([10_000, 10_001, 20_000])(
  "backlog383-r9: actual shared qualified client delivers native paste at %i characters without reconnecting",
  (length) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          "r9-qualified-paste",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          { ...route, show: false, clientRequestId: "r9-qualified-paste-open" },
          undefined,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const access = yield* a.fixtureBrowserViewers!.open(native, {
          resource,
          operation: "control",
        });
        const external = browserExternalContexts.at(-1)!;
        const inserted = Promise.withResolvers<void>();
        const continued = Promise.withResolvers<void>();
        external.controls.afterInput = async (method) => {
          if (method !== "Input.insertText") return;
          if (external.inputs.at(-1)?.params?.text === "after-paste") continued.resolve();
          else inserted.resolve();
        };
        const released = Promise.withResolvers<void>();
        let releasing = false;
        const ready = Promise.withResolvers<void>();
        const controlled = Promise.withResolvers<void>();
        const connections: boolean[] = [];
        const disconnected = Promise.withResolvers<void>();
        const client = createPreviewStreamClient(
          {
            threadId: child,
            tabId: resource.tabId,
            maxWidth: 800,
            maxHeight: 600,
            access: {
              httpBase: `${a.url}/api/preview-stream`,
              wsBase: `${a.url.replace(/^http/, "ws")}/api/preview-stream`,
              query: { viewerTicket: access.ticket },
              credentials: false,
            },
          },
          {
            onFrame: () => {},
            onViewport: () => {},
            onControl: (control) => {
              if (releasing && control.controller !== "you") released.resolve();
              if (control.controller === "you") controlled.resolve();
              else ready.resolve();
            },
            onConnectedChange: (connected) => {
              connections.push(connected);
              if (!connected) disconnected.resolve();
            },
            onUnauthorized: () => {},
          },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => client.stop()));
        yield* Effect.promise(() => ready.promise);
        expect(client.send({ type: "takeControl" })).toBe(true);
        yield* Effect.promise(() => controlled.promise);
        expect(client.send({ type: "text", text: "界".repeat(length) })).toBe(true);
        const outcome = yield* Effect.promise(() =>
          Promise.race([
            inserted.promise.then(() => "inserted"),
            disconnected.promise.then(() => "disconnected"),
          ]),
        );
        expect(outcome).toBe("inserted");
        expect(client.send({ type: "text", text: "after-paste" })).toBe(true);
        yield* Effect.promise(() => continued.promise);
        expect(connections).toEqual([true]);
        releasing = true;
        expect(client.send({ type: "releaseControl" })).toBe(true);
        yield* Effect.promise(() => released.promise);
        const resumed = yield* nativeCall(
          a,
          parent,
          "preview_navigate",
          {
            ...route,
            tabId: resource.tabId,
            url: "https://example.test/after-qualified-paste",
          },
          undefined,
          credential,
        );
        expect(resumed.isError, json(resumed)).not.toBe(true);
        expect(external.inputs.filter((entry) => entry.method === "Input.insertText")).toEqual([
          { method: "Input.insertText", params: { text: "界".repeat(10_000) } },
          { method: "Input.insertText", params: { text: "after-paste" } },
        ]);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  { surface: "web", path: "apps/web/src/browser/ServerBrowserSurface.tsx", length: 3 },
  {
    surface: "mobile",
    path: "apps/mobile/src/features/browser/preview-stream.browser.ts",
    length: 4,
  },
  { surface: "web", path: "apps/web/src/browser/ServerBrowserSurface.tsx", length: 6 },
  {
    surface: "mobile",
    path: "apps/mobile/src/features/browser/preview-stream.browser.ts",
    length: 6,
  },
])(
  "input383-r10: actual $surface producer preserves consecutive native clicks through $length",
  ({ surface, path, length }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const source = yield* fs.readFileString(
          NodeURL.fileURLToPath(new URL(`../../../../${path}`, import.meta.url)),
        );
        const readProducer = (name: string) => {
          const start = source.indexOf(`  const ${name} =`);
          const end = source.indexOf("\n  };", start) + 6;
          expect(start).toBeGreaterThan(0);
          return NodeModule.stripTypeScriptTypes(source.slice(start, end));
        };
        const constants = ["MULTI_CLICK_MS", "MULTI_CLICK_SLOP_PX"]
          .map((name) => {
            const value = source.match(new RegExp(`const ${name} = [^;]+;`));
            expect(value).not.toBeNull();
            return value![0];
          })
          .join("\n");
        const setup =
          surface === "web" ? "const lastClickRef = { current: null };" : "let lastTap = null;";
        const call =
          surface === "web"
            ? 'countClick("left", 10, 10, time)'
            : 'countClick({ clientX: 10, clientY: 10, timeStamp: time }, "left")';
        const counter = yield* decodeProducerCounter(
          new Function(
            `${setup}\n${constants}\n${readProducer("countClick")}\nreturn time => ${call};`,
          )(),
        );
        const { a, b, parent, child, route } = yield* typed383Pair(
          "r10-qualified-click",
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          { ...route, show: false, clientRequestId: "r10-qualified-click-open" },
          undefined,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const owner = yield* a.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* a.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(a.url, { headers: { authorization: `Bearer ${owner.token}` } }),
          ),
        );
        const access = yield* a.fixtureBrowserViewers!.open(native, {
          resource,
          operation: "control",
        });
        const external = browserExternalContexts.at(-1)!;
        const released = Promise.withResolvers<void>();
        let releasing = false;
        const ready = Promise.withResolvers<void>();
        const controlled = Promise.withResolvers<void>();
        const connections: boolean[] = [];
        const refusals: string[] = [];
        const disconnected = Promise.withResolvers<void>();
        const client = createPreviewStreamClient(
          {
            threadId: child,
            tabId: resource.tabId,
            maxWidth: 800,
            maxHeight: 600,
            access: {
              httpBase: `${a.url}/api/preview-stream`,
              wsBase: `${a.url.replace(/^http/, "ws")}/api/preview-stream`,
              query: { viewerTicket: access.ticket },
              credentials: false,
            },
          },
          {
            onFrame: () => {},
            onViewport: () => {},
            onControl: (control) => {
              if (releasing && control.controller !== "you") released.resolve();
              if (control.controller === "you") controlled.resolve();
              else ready.resolve();
            },
            onConnectedChange: (connected) => {
              connections.push(connected);
              if (!connected) disconnected.resolve();
            },
            onUnauthorized: () => {},
            onInputRejected: (message) => refusals.push(message),
          },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => client.stop()));
        yield* Effect.promise(() => ready.promise);
        expect(client.send({ type: "takeControl" })).toBe(true);
        yield* Effect.promise(() => controlled.promise);
        for (let count = 1; count <= length; count++) {
          const clickCount = yield* decodeProducerClickCount(counter(count * 100));
          expect(clickCount).toBe(count);
          const dispatched = Promise.withResolvers<void>();
          external.controls.afterInput = async (method) => {
            if (method === "Input.dispatchMouseEvent") dispatched.resolve();
          };
          expect(
            client.send({
              type: "mouse",
              action: "down",
              x: 10,
              y: 10,
              button: "left",
              buttons: 1,
              clickCount,
              modifiers: 0,
            }),
          ).toBe(true);
          const outcome = yield* Effect.promise(() =>
            Promise.race([
              dispatched.promise.then(() => "dispatched"),
              disconnected.promise.then(() => "disconnected"),
            ]),
          );
          expect(outcome, "ordinary consecutive clicks must preserve the controlling viewer").toBe(
            "dispatched",
          );
        }
        expect(counter(2000)).toBe(1);
        expect(client.send({ type: "dialog", accept: true, promptText: "界".repeat(16385) })).toBe(
          false,
        );
        expect(refusals).toEqual(["Browser input is invalid or too large. Edit it and try again."]);
        const continued = Promise.withResolvers<void>();
        external.controls.afterInput = async (method) => {
          if (method === "Input.insertText") continued.resolve();
        };
        expect(client.send({ type: "text", text: "after-click" })).toBe(true);
        yield* Effect.promise(() => continued.promise);
        expect(connections).toEqual([true]);
        expect(
          external.inputs.filter((event) => event.method === "Input.dispatchMouseEvent"),
        ).toEqual(
          Array.from({ length }, (_, index) => ({
            method: "Input.dispatchMouseEvent",
            params: {
              type: "mousePressed",
              x: 10,
              y: 10,
              button: "left",
              buttons: 1,
              clickCount: index + 1,
              modifiers: 0,
            },
          })),
        );
        if (length === 6) {
          const wheelSetup =
            surface === "web"
              ? "const pendingWheelRef = { current: null }; const scheduleFlush = () => {};"
              : "let pendingWheel = null; let wheelFrame = null; const requestAnimationFrame = () => 1; const flushWheel = () => {};";
          const wheelResult = surface === "web" ? "pendingWheelRef.current" : "pendingWheel";
          const wheel: unknown = new Function(
            `${wheelSetup}\n${readProducer("queueWheel")}\nqueueWheel({x:10,y:20},75000,-80000,0); queueWheel({x:10,y:20},75000,-80000,0); return ${wheelResult};`,
          )();
          const wheelInput = yield* decodeProducerInput(wheel);
          for (const input of [
            {
              type: "mouse" as const,
              action: "move" as const,
              x: 10,
              y: 20,
              button: "none" as const,
              buttons: 31,
              clickCount: 0,
              modifiers: 0,
            },
            wheelInput,
          ]) {
            const dispatched = Promise.withResolvers<void>();
            external.controls.afterInput = async (method) => {
              if (method === "Input.dispatchMouseEvent") dispatched.resolve();
            };
            expect(client.send(input)).toBe(true);
            yield* Effect.promise(() => dispatched.promise);
          }
          expect(
            external.inputs
              .filter((event) => event.method === "Input.dispatchMouseEvent")
              .slice(-2),
          ).toEqual([
            {
              method: "Input.dispatchMouseEvent",
              params: {
                type: "mouseMoved",
                x: 10,
                y: 20,
                button: "none",
                buttons: 31,
                clickCount: 0,
                modifiers: 0,
              },
            },
            {
              method: "Input.dispatchMouseEvent",
              params: {
                type: "mouseWheel",
                x: 10,
                y: 20,
                deltaX: 150000,
                deltaY: -160000,
                modifiers: 0,
              },
            },
          ]);
          expect(connections).toEqual([true]);
        }
        releasing = true;
        expect(client.send({ type: "releaseControl" })).toBe(true);
        yield* Effect.promise(() => released.promise);
        const resumed = yield* nativeCall(
          a,
          parent,
          "preview_navigate",
          {
            ...route,
            tabId: resource.tabId,
            url: "https://example.test/after-click",
            clientRequestId: "r10-after-click",
          },
          undefined,
          credential,
        );
        expect(resumed.isError, json(resumed)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  { kind: "empty", route: "qualified" },
  { kind: "blank", route: "qualified" },
  { kind: "long", route: "qualified" },
  { kind: "url-boundary", route: "qualified" },
  { kind: "invalid-url-control", route: "qualified" },
  { kind: "empty", route: "local" },
  { kind: "prompt-boundary", route: "qualified" },
  { kind: "prompt-overflow", route: "qualified" },
  { kind: "viewport", route: "qualified" },
  { kind: "viewport", route: "local" },
] as const)(
  "input383-r10-opus: $route $kind",
  (probe) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r10-${probe.route}-${probe.kind}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const credential = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration", "preview", "device"]),
        });
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(
          a,
          parent,
          "preview_open",
          { ...route, show: false, clientRequestId: "r10-opus-open" },
          undefined,
          credential,
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const external = browserExternalContexts.at(-1)!;
        const destination = probe.route === "qualified" ? a : b;
        const issued = yield* destination.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const principal = yield* destination.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(destination.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const query: Record<string, string> = {};
        if (probe.route === "qualified") {
          const access = yield* a.fixtureBrowserViewers!.open(principal, {
            resource,
            operation: "control",
          });
          query.viewerTicket = access.ticket;
        } else {
          const ticket = yield* b.auth.issueWebSocketTicket(principal);
          query.wsTicket = ticket.ticket;
        }
        const ready = Promise.withResolvers<void>();
        const controlled = Promise.withResolvers<void>();
        const released = Promise.withResolvers<void>();
        const disconnected = Promise.withResolvers<void>();
        const promptReady = Promise.withResolvers<void>();
        const connections: boolean[] = [];
        const refusals: string[] = [];
        let releasing = false;
        const client = createPreviewStreamClient(
          {
            threadId: child,
            tabId: resource.tabId,
            maxWidth: 800,
            maxHeight: 600,
            access: {
              httpBase: `${destination.url}/api/preview-stream`,
              wsBase: `${destination.url.replace(/^http/, "ws")}/api/preview-stream`,
              query,
              credentials: false,
            },
          },
          {
            onFrame: () => {},
            onViewport: () => {},
            onControl: (control) => {
              if (control.controller === "you") controlled.resolve();
              else ready.resolve();
              if (releasing && control.controller !== "you") released.resolve();
              if (control.dialog?.message === "r10-prompt") promptReady.resolve();
            },
            onConnectedChange: (connected) => {
              connections.push(connected);
              if (!connected) disconnected.resolve();
            },
            onUnauthorized: () => {},
            onInputRejected: (message) => refusals.push(message),
          },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => client.stop()));
        yield* Effect.promise(() => ready.promise);
        expect(client.send({ type: "takeControl" })).toBe(true);
        yield* Effect.promise(() => controlled.promise);
        const drain = (text: string) =>
          Effect.gen(function* () {
            const continued = Promise.withResolvers<void>();
            external.controls.afterInput = async (method) => {
              if (method === "Input.insertText" && external.inputs.at(-1)?.params?.text === text)
                continued.resolve();
            };
            expect(client.send({ type: "text", text })).toBe(true);
            const outcome = yield* Effect.promise(() =>
              Promise.race([
                continued.promise.then(() => "continued"),
                disconnected.promise.then(() => "disconnected"),
              ]),
            );
            expect(outcome, "producer input must preserve the same controlling viewer").toBe(
              "continued",
            );
          });
        if (probe.kind === "viewport") {
          const settings: PreviewViewportSetting[] = [
            { _tag: "freeform", width: 500, height: 700 },
            { _tag: "preset", width: 375, height: 667, presetId: "iphone-se" },
            { _tag: "fill" },
          ];
          for (const setting of settings) {
            const input = {
              type: "viewport" as const,
              setting:
                probe.route === "local" ? { ...setting, ignored: "x".repeat(300000) } : setting,
            };
            expect(client.send(input)).toBe(true);
            yield* drain(`after-${setting._tag}`);
            const expected =
              setting._tag === "fill"
                ? { width: 1280, height: 800 }
                : { width: setting.width, height: setting.height };
            expect(external.page.viewportSize(), `native ${setting._tag} viewport`).toEqual(
              expected,
            );
            const snapshot = (yield* b.nativePreviewManager!.list({
              threadId: child,
            })).sessions.find((session) => session.tabId === resource.tabId);
            expect(snapshot?.viewport).toEqual(setting);
          }
        } else if (probe.kind === "prompt-boundary" || probe.kind === "prompt-overflow") {
          const answered = Promise.withResolvers<void>();
          const answers: string[] = [];
          external.emit("dialog", {
            type: () => "prompt",
            message: () => "r10-prompt",
            defaultValue: () => "",
            accept: async (value: string) => {
              answers.push(value);
              answered.resolve();
            },
            dismiss: async () => {},
          });
          yield* Effect.promise(() => promptReady.promise);
          let overflowSent = false;
          if (probe.kind === "prompt-overflow")
            overflowSent = client.send({
              type: "dialog",
              accept: true,
              promptText: "界".repeat(16385),
            });
          const answer = "界".repeat(16384);
          expect(client.send({ type: "dialog", accept: true, promptText: answer })).toBe(true);
          const outcome = yield* Effect.promise(() =>
            Promise.race([
              answered.promise.then(() => "answered"),
              disconnected.promise.then(() => "disconnected"),
            ]),
          );
          expect(
            outcome,
            "a refused oversized draft must leave its exact pending dialog answerable",
          ).toBe("answered");
          expect(overflowSent).toBe(false);
          expect(answers).toEqual([answer]);
          expect(refusals).toHaveLength(probe.kind === "prompt-overflow" ? 1 : 0);
          yield* drain("after-prompt");
        } else {
          const prefix = "https://example.test/";
          const url =
            probe.kind === "empty"
              ? ""
              : probe.kind === "blank"
                ? "   "
                : probe.kind === "invalid-url-control"
                  ? "http://"
                  : prefix + "a".repeat((probe.kind === "long" ? 2049 : 2048) - prefix.length);
          const before = external.effects.filter((effect) => effect === "navigate").length;
          const sent = client.send({ type: "navigate", url });
          yield* drain("after-navigation-input");
          const refused =
            probe.route === "qualified" &&
            (probe.kind === "empty" || probe.kind === "blank" || probe.kind === "long");
          expect(sent).toBe(!refused);
          expect(refusals).toHaveLength(refused ? 1 : 0);
          expect(external.effects.filter((effect) => effect === "navigate")).toHaveLength(
            before + (probe.kind === "url-boundary" ? 1 : 0),
          );
          if (probe.kind === "url-boundary") expect(external.page.url()).toBe(url);
        }
        expect(connections).toEqual([true]);
        releasing = true;
        expect(client.send({ type: "releaseControl" })).toBe(true);
        yield* Effect.promise(() => released.promise);
        const resumed = yield* nativeCall(
          a,
          parent,
          "preview_navigate",
          {
            ...route,
            tabId: resource.tabId,
            url: "https://example.test/after-opus-input",
            clientRequestId: "r10-opus-resume",
          },
          undefined,
          credential,
        );
        expect(resumed.isError, json(resumed)).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live.each([
  ...[1, 2].flatMap((wait) =>
    [
      "unchanged",
      "handoff-original",
      "handoff-replacement",
      "same-controller-replacement",
      "same-controller-return",
    ].map((transition) => ({ wait, transition })),
  ),
])(
  "dialog383-r11: $transition during authorization $wait",
  ({ wait, transition }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r11-dialog-${transition}-${wait}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "r11-dialog-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const repository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
          Effect.provideContext(repository),
        );
        const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
          Effect.provideContext(repository),
        );
        const check = read.pipe(Effect.andThen(operate));
        let armed = false;
        let calls = 0;
        const reached = Promise.withResolvers<void>();
        const resume = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => resume.resolve()));
        const first = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: async () => {
            await nativePromise(check);
            if (armed && ++calls === wait) {
              reached.resolve();
              await resume.promise;
            }
          },
        });
        const second = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: () => nativePromise(check),
        });
        yield* first.input({ type: "takeControl" });
        const external = browserExternalContexts.at(-1)!;
        let originalAccepted = 0;
        let originalDismissed = 0;
        let replacementAccepted = 0;
        external.emit("dialog", {
          type: () => "prompt",
          message: () => "original",
          defaultValue: () => "",
          accept: async () => {
            originalAccepted++;
          },
          dismiss: async () => {
            originalDismissed++;
          },
        });
        armed = true;
        const stale = yield* first
          .input({ type: "dialog", accept: true, promptText: "old-viewer-reply" })
          .pipe(Effect.forkScoped);
        yield* Effect.promise(() => reached.promise);
        const handoff = transition.startsWith("handoff") || transition === "same-controller-return";
        const replacement = transition.endsWith("replacement");
        if (handoff) {
          yield* first.input({ type: "releaseControl" });
          yield* second.input({ type: "takeControl" });
        }
        const current = handoff ? second : first;
        if (replacement) {
          yield* current.input({ type: "dialog", accept: false });
          external.emit("dialog", {
            type: () => "prompt",
            message: () => "replacement",
            defaultValue: () => "",
            accept: async () => {
              replacementAccepted++;
            },
            dismiss: async () => {},
          });
        }
        if (transition === "same-controller-return") {
          yield* second.input({ type: "releaseControl" });
          yield* first.input({ type: "takeControl" });
        }
        resume.resolve();
        yield* Fiber.join(stale);
        yield* check;
        expect(replacementAccepted, "a stale reply must never answer a replacement dialog").toBe(0);
        expect(originalAccepted).toBe(transition === "unchanged" ? 1 : 0);
        expect(originalDismissed).toBe(replacement ? 1 : 0);
        const owner = transition === "same-controller-return" ? first : current;
        if (transition !== "unchanged")
          yield* owner.input({ type: "dialog", accept: true, promptText: "current-owner" });
        expect(replacementAccepted).toBe(replacement ? 1 : 0);
        expect(originalAccepted).toBe(replacement ? 0 : 1);
        yield* owner.input({ type: "dialog", accept: true, promptText: "duplicate" });
        expect(replacementAccepted).toBe(replacement ? 1 : 0);
        expect(originalAccepted).toBe(replacement ? 0 : 1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30000,
);

it.live.each([
  ...[
    {
      kind: "text",
      input: { type: "text", text: "before-authorization" },
      method: "Input.insertText",
      params: { text: "before-authorization" },
    },
    {
      kind: "key",
      input: { type: "key", action: "up", key: "a", code: "KeyA" },
      method: "Input.dispatchKeyEvent",
      params: { type: "keyUp", key: "a", code: "KeyA" },
    },
    {
      kind: "mouse",
      input: { type: "mouse", action: "move", button: "none", x: 17, y: 23 },
      method: "Input.dispatchMouseEvent",
      params: { type: "mouseMoved", x: 17, y: 23 },
    },
    {
      kind: "wheel",
      input: { type: "wheel", x: 17, y: 23, deltaX: 1, deltaY: 2 },
      method: "Input.dispatchMouseEvent",
      params: { type: "mouseWheel", deltaX: 1, deltaY: 2 },
    },
    {
      kind: "navigate",
      input: { type: "navigate", url: "https://example.test/admitted-before-wait" },
      method: null,
      params: null,
    },
  ].flatMap((entry) =>
    ["unchanged", "release-reacquire"].map((transition) => ({ ...entry, wait: 1, transition })),
  ),
  {
    kind: "queued-text",
    input: { type: "text", text: "before-authorization" },
    method: "Input.insertText",
    params: { text: "before-authorization" },
    wait: 1,
    transition: "release",
  },
  ...["unchanged", "release"].map((transition) => ({
    kind: "running-text",
    input: { type: "text", text: "before-authorization" },
    method: "Input.insertText",
    params: { text: "before-authorization" },
    wait: 3,
    transition,
  })),
])(
  "input383-r12: $kind $transition authorization $wait",
  ({ kind, input, method, params, wait, transition }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r12-input-${kind}-${transition}-${wait}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "r12-input-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const repository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
          Effect.provideContext(repository),
        );
        const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
          Effect.provideContext(repository),
        );
        const check = read.pipe(Effect.andThen(operate));
        let armed = false;
        let calls = 0;
        const reached = Promise.withResolvers<void>();
        const resume = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => resume.resolve()));
        const first = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: async () => {
            await nativePromise(check);
            if (armed && ++calls === wait) {
              reached.resolve();
              await resume.promise;
            }
          },
        });
        yield* first.input({ type: "takeControl" });
        const external = browserExternalContexts.at(-1)!;
        const controls: Array<{ controller: string; generation: number }> = [];
        const released = Promise.withResolvers<void>();
        const reacquired = Promise.withResolvers<void>();
        yield* Queue.takeAll(first.output);
        yield* Effect.gen(function* () {
          while (true) {
            const out = yield* Queue.take(first.output);
            if (out._tag !== "control") continue;
            controls.push({ controller: out.controller, generation: out.generation });
            if (out.controller === "agent" && out.generation === 2) released.resolve();
            if (out.controller === "you" && out.generation === 3) reacquired.resolve();
          }
        }).pipe(Effect.forkScoped);
        const inputOffset = external.inputs.length;
        const navigationOffset = external.effects.filter((effect) => effect === "navigate").length;
        armed = true;
        const stale = yield* first.input(input).pipe(Effect.forkScoped);
        yield* Effect.promise(() => reached.promise);
        expect(external.inputs.slice(inputOffset)).toEqual([]);
        expect(external.effects.filter((effect) => effect === "navigate")).toHaveLength(
          navigationOffset,
        );
        let releaseCompleted = false;
        const releasing =
          transition !== "unchanged"
            ? yield* first.input({ type: "releaseControl" }).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    releaseCompleted = true;
                  }),
                ),
                Effect.forkScoped,
              )
            : undefined;
        if (releasing) yield* Effect.promise(() => released.promise);
        const reacquiring =
          transition === "release-reacquire"
            ? yield* first.input({ type: "takeControl" }).pipe(Effect.forkScoped)
            : undefined;
        if (reacquiring) {
          yield* Effect.promise(() => reacquired.promise);
          expect(controls).toEqual(
            expect.arrayContaining([
              { controller: "agent", generation: 2 },
              { controller: "you", generation: 3 },
            ]),
          );
        } else if (transition === "release") {
          expect(releaseCompleted, "release must drain the action closure already running").toBe(
            false,
          );
        }
        resume.resolve();
        yield* Fiber.join(stale);
        if (releasing) {
          yield* Fiber.join(releasing);
          expect(releaseCompleted).toBe(true);
        }
        if (reacquiring) yield* Fiber.join(reacquiring);
        if (transition === "release") {
          yield* first.input({ type: "takeControl" });
        }
        yield* first.input({ type: "text", text: "current-owner-text" });
        expect(
          external.inputs.filter(
            (event) =>
              event.method === "Input.insertText" && event.params?.text === "current-owner-text",
          ),
        ).toHaveLength(1);
        const expected = transition === "unchanged" || wait === 3 ? 1 : 0;
        if (method === null) {
          expect(external.effects.filter((effect) => effect === "navigate")).toHaveLength(
            navigationOffset + expected,
          );
        } else {
          const observed = external.inputs
            .slice(inputOffset)
            .filter(
              (event) =>
                event.method === method &&
                (method !== "Input.insertText" || event.params?.text === "before-authorization"),
            );
          expect(
            observed,
            "ordinary input must retain its pre-queue generation rather than adopt a reacquired one",
          ).toHaveLength(expected);
          if (expected === 1) expect(observed[0]?.params).toMatchObject(params);
        }
        yield* check;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  ...[
    {
      kind: "text",
      one: { type: "text", text: "first" },
      two: { type: "text", text: "second" },
      expected: ["first", "second"],
    },
    {
      kind: "key",
      one: {
        type: "key",
        action: "down",
        key: "Backspace",
        code: "Backspace",
        keyCode: 8,
        modifiers: 0,
      },
      two: {
        type: "key",
        action: "up",
        key: "Backspace",
        code: "Backspace",
        keyCode: 8,
        modifiers: 0,
      },
      expected: ["rawKeyDown", "keyUp"],
    },
    {
      kind: "mouse",
      one: {
        type: "mouse",
        action: "down",
        button: "left",
        x: 17,
        y: 23,
        buttons: 1,
        clickCount: 1,
        modifiers: 0,
      },
      two: {
        type: "mouse",
        action: "up",
        button: "left",
        x: 17,
        y: 23,
        buttons: 0,
        clickCount: 1,
        modifiers: 0,
      },
      expected: ["mousePressed", "mouseReleased"],
    },
  ].flatMap((entry) => [false, true].map((held) => ({ ...entry, held }))),
])(
  "order383-r13: $kind held-first=$held",
  ({ kind, one, two, expected, held }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r13-order-${kind}-${held}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "r13-order-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const repository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
          Effect.provideContext(repository),
        );
        const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
          Effect.provideContext(repository),
        );
        const check = read.pipe(Effect.andThen(operate));
        let armed = false;
        const authorizations: Promise<void>[] = [];
        const reached = Promise.withResolvers<void>();
        const resume = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => resume.resolve()));
        const viewer = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: () => {
            const first = armed && authorizations.length === 0;
            const checking = (async () => {
              await nativePromise(check);
              if (first && held) {
                reached.resolve();
                await resume.promise;
              }
            })();
            if (armed) authorizations.push(checking);
            return checking;
          },
        });
        yield* viewer.input({ type: "takeControl" });
        const firstInput = yield* Schema.decodeUnknownEffect(PreviewStreamInput)(one);
        const secondInput = yield* Schema.decodeUnknownEffect(PreviewStreamInput)(two);
        const external = browserExternalContexts.at(-1)!;
        const start = external.inputs.length;
        armed = true;
        if (held) {
          const first = yield* viewer.input(firstInput).pipe(Effect.forkScoped);
          yield* Effect.promise(() => reached.promise);
          expect(external.inputs.slice(start)).toEqual([]);
          const beforeSuccessor = authorizations.length;
          yield* viewer.enqueueInput(secondInput);
          // Wait only for checks already started, never for queued successor work.
          const startedChecks = authorizations.slice(beforeSuccessor);
          yield* Effect.promise(() => Promise.all(startedChecks));
          resume.resolve();
          yield* Fiber.join(first);
        } else {
          yield* viewer.input(firstInput);
          yield* viewer.input(secondInput);
        }
        yield* viewer.input({ type: "text", text: "later-lawful" });
        expect(
          external.inputs.filter(
            (event) => event.method === "Input.insertText" && event.params?.text === "later-lawful",
          ),
        ).toHaveLength(1);
        const effects = external.inputs
          .slice(start)
          .filter((event) => event.params?.text !== "later-lawful");
        expect(
          effects.map((event) => (kind === "text" ? event.params?.text : event.params?.type)),
          "native effects must retain submitted order across authorization",
        ).toEqual(expected);
        yield* check;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each([
  ...[1, 2].flatMap((wait) =>
    ["unchanged", "cycle"].map((transition) => ({ kind: "take", wait, transition })),
  ),
  ...["unchanged", "cycle"].map((transition) => ({ kind: "release", wait: 1, transition })),
])(
  "control383-r14: $kind $transition authorization $wait",
  ({ kind, wait, transition }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, route } = yield* typed383Pair(
          `r14-control-${kind}-${transition}-${wait}`,
          false,
          false,
          { source: false, destination: false, destinationBrowser: true },
        );
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, preview: true });
        const opened = yield* nativeCall(a, parent, "preview_open", {
          ...route,
          show: false,
          clientRequestId: "r14-control-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const issued = yield* b.auth.issueSession({
          scopes: ["orchestration:read", "preview:operate"],
        });
        const native = yield* b.auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request(b.url, { headers: { authorization: `Bearer ${issued.token}` } }),
          ),
        );
        const repository = yield* AuthSessions.layer.pipe(
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const read = yield* Auth.nativeSessionAuthorization(native, "orchestration:read").pipe(
          Effect.provideContext(repository),
        );
        const operate = yield* Auth.nativeSessionAuthorization(native, "preview:operate").pipe(
          Effect.provideContext(repository),
        );
        const check = read.pipe(Effect.andThen(operate));
        let armed = false;
        let calls = 0;
        const reached = Promise.withResolvers<void>();
        const resume = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => resume.resolve()));
        const viewer = yield* b.nativeBrowser!.attachViewer({
          threadId: child,
          tabId: resource.tabId,
          maxWidth: 1280,
          maxHeight: 800,
          quality: 70,
          canOperate: true,
          authorize: async () => {
            await nativePromise(check);
            if (armed && ++calls === wait) {
              reached.resolve();
              await resume.promise;
            }
          },
        });
        const take = yield* Schema.decodeUnknownEffect(PreviewStreamInput)({ type: "takeControl" });
        const release = yield* Schema.decodeUnknownEffect(PreviewStreamInput)({
          type: "releaseControl",
        });
        if (kind === "release") yield* viewer.input(take);
        yield* Queue.clear(viewer.output);
        armed = true;
        const delayed = yield* viewer
          .input(kind === "take" ? take : release)
          .pipe(Effect.forkScoped);
        yield* Effect.promise(() => reached.promise);
        if (transition === "cycle") {
          yield* viewer.input(kind === "take" ? take : release);
          yield* viewer.input(kind === "take" ? release : take);
        }
        const before = (yield* Queue.clear(viewer.output)).filter((out) => out._tag === "control");
        if (transition === "cycle") {
          expect(
            before.map((out) => ({ controller: out.controller, generation: out.generation })),
          ).toEqual(
            expect.arrayContaining(
              kind === "take"
                ? [
                    { controller: "you", generation: 1 },
                    { controller: "agent", generation: 2 },
                  ]
                : [
                    { controller: "agent", generation: 2 },
                    { controller: "you", generation: 3 },
                  ],
            ),
          );
        }
        resume.resolve();
        yield* Fiber.join(delayed);
        const after = (yield* Queue.clear(viewer.output)).filter((out) => out._tag === "control");
        const observed = [...before, ...after].at(-1);
        const external = browserExternalContexts.at(-1)!;
        yield* viewer.input(take);
        yield* viewer.input({ type: "text", text: "current-owner-marker" });
        expect(
          external.inputs.filter(
            (event) =>
              event.method === "Input.insertText" && event.params?.text === "current-owner-marker",
          ),
        ).toHaveLength(1);
        yield* check;
        const expected =
          transition === "cycle"
            ? kind === "take"
              ? { controller: "agent", generation: 2 }
              : { controller: "you", generation: 3 }
            : kind === "take"
              ? { controller: "you", generation: 1 }
              : { controller: "agent", generation: 2 };
        expect(
          observed,
          "pending old control must not reverse a newer completed opposite intent",
        ).toMatchObject(expected);
        yield* viewer.input(release);
        expect(
          (yield* Queue.clear(viewer.output)).filter((out) => out._tag === "control").at(-1)
            ?.controller,
        ).toBe("agent");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  30_000,
);

it.live.each(["preview", "device"] as const)(
  "idle-resource383: active A operates and closes existing idle B %s without a B credential",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, call, assertIdle } = yield* idleResource383Pair(
          `idle-resource383-${capability}`,
        );
        const contextsBefore = browserExternalContexts.length;
        if (capability === "preview") {
          const opened = yield* call("preview_open", {
            url: "https://example.test/",
            show: false,
            clientRequestId: "idle-open",
          });
          expect(opened.isError, json(opened)).not.toBe(true);
          const { resource } = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ resource: PreviewViewerResource }),
          )(opened.structuredContent);
          expect(resource).toMatchObject({ environmentId: b.environmentId, threadId: child });
          expect(browserExternalContexts).toHaveLength(contextsBefore + 1);
          const action = yield* call("preview_type", {
            tabId: resource.tabId,
            text: "idle-destination",
            clientRequestId: "idle-type",
          });
          expect(action.isError, json(action)).not.toBe(true);
          expect(
            browserExternalContexts
              .at(-1)!
              .effects.filter((effect) => effect === "text:idle-destination"),
          ).toHaveLength(1);
          const read = yield* call("preview_status", { tabId: resource.tabId });
          expect(read.isError, json(read)).not.toBe(true);
          const close = yield* call("t3_preview_close", {
            tabId: resource.tabId,
            clientRequestId: "idle-close",
          });
          expect(close.isError, json(close)).not.toBe(true);
          expect((yield* b.nativePreviewManager!.list({ threadId: child })).sessions).toHaveLength(
            0,
          );
        } else {
          const opened = yield* call("device_open", {
            deviceId: "fixture-device382",
            platform: "android",
            clientRequestId: "idle-open",
          });
          expect(opened.isError, json(opened)).not.toBe(true);
          const { target } = yield* decodeTyped383Open(opened.structuredContent);
          const action = yield* call("device_execute", {
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "idle-click",
          });
          expect(action.isError, json(action)).not.toBe(true);
          const read = yield* call("device_execute", {
            target,
            command: { type: "snapshot", depth: 8 },
            clientRequestId: "idle-snapshot",
          });
          expect(read.structuredContent).toMatchObject({
            type: "snapshot",
            nodes: [{ ref: "e1" }],
          });
          expect(b.deviceProcessCalls.filter((x) => x.args.includes("click"))).toHaveLength(1);
          const close = yield* call("device_close", {
            deviceId: target.deviceId,
            clientRequestId: "idle-close",
          });
          expect(close.isError, json(close)).not.toBe(true);
          expect(yield* b.fixtureDevice!.sessionsForThread(child)).toHaveLength(0);
        }
        yield* assertIdle;
        expect((yield* a.threads.getThreadShell(parent))?.activeRunId).not.toBeNull();
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "idle-resource383: %s intake retains original source and current destination refusals",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, grant, caps, call, assertIdle } = yield* idleResource383Pair(
          `idle-resource383-policy-${capability}`,
        );
        let next = 0;
        const open = () =>
          call(capability === "preview" ? "preview_open" : "device_open", {
            ...(capability === "preview"
              ? { show: false, reuseExistingTab: false }
              : { deviceId: "fixture-device382", platform: "android" }),
            clientRequestId: `policy-${next++}`,
          });
        const read = yield* call(capability === "preview" ? "preview_status" : "device_list");
        expect(read.isError, json(read)).not.toBe(true);
        const lawful = yield* open();
        expect(lawful.isError, json(lawful)).not.toBe(true);
        const effects = () => ({
          contexts: browserExternalContexts.length,
          deviceCalls: b.deviceProcessCalls.length,
        });
        const before = effects();
        const wrong = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: ProviderInstanceId.make("idle-wrong-provider"),
          capabilities: caps,
        });
        const mismatch = yield* call(
          capability === "preview" ? "preview_open" : "device_open",
          {
            ...(capability === "preview"
              ? { show: false }
              : { deviceId: "fixture-device382", platform: "android" }),
            clientRequestId: `policy-wrong-provider`,
          },
          wrong,
        );
        expect(mismatch.isError).toBe(true);
        expect(json(mismatch)).toContain("parent_not_active");
        expect(effects()).toEqual(before);
        const missing = yield* call(capability === "preview" ? "preview_open" : "device_open", {
          contextThreadId: "idle-resource-missing-context",
          ...(capability === "preview"
            ? { show: false }
            : { deviceId: "fixture-device382", platform: "android" }),
          clientRequestId: "policy-missing-context",
        });
        expect(missing.isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "created-work",
          preview: true,
          device: true,
        });
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: capability !== "preview",
          device: capability !== "device",
        });
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        yield* b.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: false }
            : { enableAgentDeviceAccess: false },
        );
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* b.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: true }
            : { enableAgentDeviceAccess: true },
        );
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("idle-source-plan"),
          threadId: parent,
          interactionMode: "plan",
        });
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          commandId: CommandId.make("idle-source-default"),
          threadId: parent,
          interactionMode: "default",
        });
        expect((yield* open()).isError).not.toBe(true);
        yield* assertIdle;
        const resumed = effects();
        yield* a.threads.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("idle-source-ended"),
          threadId: parent,
        });
        const ended = yield* open();
        expect(ended.isError).toBe(true);
        expect(json(ended)).toContain("parent_not_active");
        expect(effects()).toEqual(resumed);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "idle-resource383: incoming %s authority is rechecked after queued native preparation",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, call, assertIdle } = yield* idleResource383Pair(
          `idle-resource383-wait-${capability}`,
        );
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        if (capability === "preview") {
          browserExternalBoundary.beforeContextReturn = () =>
            nativePromise(
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            );
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              browserExternalBoundary.beforeContextReturn = async () => {};
            }),
          );
        } else
          yield* Ref.set(
            b.deviceReadinessHook,
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          );
        const pending = yield* call(capability === "preview" ? "preview_open" : "device_open", {
          ...(capability === "preview"
            ? { show: false, reuseExistingTab: false }
            : { deviceId: "fixture-device382", platform: "android" }),
          clientRequestId: "idle-held-native",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* assertIdle;
        yield* a.threads.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("idle-retire-during-wait"),
          threadId: parent,
        });
        yield* Deferred.succeed(release, undefined);
        const denied = yield* Fiber.join(pending);
        expect(denied.isError, json(denied)).toBe(true);
        if (capability === "preview")
          expect(browserExternalContexts.at(-1)!.effects).not.toContain("new-page");
        else expect(b.deviceProcessCalls.filter((x) => x.args.includes("open"))).toHaveLength(0);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live(
  "idle-resource383: canonical eight-scenario source-credential discriminator requires the idle positive",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, child, grant, caps, call, assertIdle } = yield* idleResource383Pair(
          "idle-resource383-eight",
          instanceId,
        );
        const contextsBefore = browserExternalContexts.length;
        const open = (key: string) =>
          call("preview_open", {
            url: "https://example.test/",
            show: false,
            reuseExistingTab: false,
            clientRequestId: key,
          });
        const idle = yield* open("eight-idle");
        expect(idle.isError, json(idle)).not.toBe(true);
        expect(browserExternalContexts).toHaveLength(contextsBefore + 1);
        yield* assertIdle;
        expect((yield* call("preview_status")).isError).not.toBe(true);
        // A fixture-state discriminator after the independent idle positive, never a prerequisite.
        yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("eight-activate-target"),
          threadId: child,
          messageId: MessageId.make("eight-target-message"),
          text: "fixture state control",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* activate(b, child);
        expect((yield* open("eight-active-control")).isError).not.toBe(true);
        expect(browserExternalContexts).toHaveLength(contextsBefore + 2);
        expect(yield* b.registry.scopeForThread(child)).toBeUndefined();
        const wrong = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: ProviderInstanceId.make("eight-wrong-provider"),
          capabilities: caps,
        });
        const mismatch = yield* call(
          "preview_open",
          { show: false, clientRequestId: "eight-wrong" },
          wrong,
        );
        expect(json(mismatch)).toContain("parent_not_active");
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "created-work",
          preview: true,
          device: true,
        });
        expect((yield* open("eight-outside")).isError).toBe(true);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: false,
          device: true,
        });
        expect((yield* open("eight-revoked")).isError).toBe(true);
        expect(browserExternalContexts).toHaveLength(contextsBefore + 2);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        yield* b.threads.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("eight-stop-target"),
          threadId: child,
        });
        expect((yield* open("eight-idle-again")).isError).not.toBe(true);
        yield* assertIdle;
        expect(browserExternalContexts).toHaveLength(contextsBefore + 3);
        yield* a.threads.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("eight-stop-source"),
          threadId: parent,
        });
        expect(json(yield* open("eight-source-ended"))).toContain("parent_not_active");
        expect(browserExternalContexts).toHaveLength(contextsBefore + 3);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["unchanged", "source-ended", "grant-retired"] as const)(
  "idle-resource383: queued incoming device effect retains original authority (%s)",
  (transition) =>
    Effect.scoped(
      Effect.gen(function* () {
        const queued = yield* Deferred.make<void>();
        let commands = 0;
        const tracer = Tracer.make({
          span: (options) => {
            if (options.name === "DeviceService.executeCommand" && ++commands === 2)
              Deferred.doneUnsafe(queued, Effect.void);
            return Tracer.nativeTracer.span(options);
          },
        });
        yield* Effect.gen(function* () {
          const { a, b, parent, grant, call, assertIdle } = yield* idleResource383Pair(
            `idle-resource383-queued-${transition}`,
          );
          const opened = yield* call("device_open", {
            deviceId: "fixture-device382",
            platform: "android",
            clientRequestId: "queued-open",
          });
          expect(opened.isError, json(opened)).not.toBe(true);
          const { target } = yield* decodeTyped383Open(opened.structuredContent);
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.set(b.deviceProcessHook, (input) =>
            input.args[1] === "click"
              ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
          );
          const first = yield* call("device_execute", {
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "queued-click",
          }).pipe(Effect.forkChild);
          yield* Deferred.await(held);
          const input = {
            target,
            command: { type: "fill", ref: "@e1", text: "queued-idle-context" },
            clientRequestId: "queued-fill",
          };
          const second = yield* call("device_execute", input).pipe(Effect.forkChild);
          yield* Deferred.await(queued);
          yield* assertIdle;
          expect(b.deviceProcessCalls.filter((x) => x.args[1] === "fill")).toHaveLength(0);
          if (transition === "source-ended")
            yield* a.threads.dispatch({
              type: "thread.stop",
              commandId: CommandId.make("queued-source-ended"),
              threadId: parent,
            });
          if (transition === "grant-retired")
            yield* b.authority.setGrant({
              ...grant,
              resourceScope: "environment-resources",
              preview: true,
              device: false,
            });
          yield* Deferred.succeed(release, undefined);
          const firstResult = yield* Fiber.join(first);
          const secondResult = yield* Fiber.join(second);
          expect(b.deviceProcessCalls.filter((x) => x.args[1] === "click")).toHaveLength(1);
          expect(secondResult.isError, json(secondResult)).toBe(transition !== "unchanged");
          expect(b.deviceProcessCalls.filter((x) => x.args[1] === "fill")).toHaveLength(
            transition === "unchanged" ? 1 : 0,
          );
          if (transition === "unchanged")
            expect(firstResult.isError, json(firstResult)).not.toBe(true);
          if (transition === "grant-retired")
            yield* b.authority.setGrant({
              ...grant,
              resourceScope: "environment-resources",
              preview: true,
              device: true,
            });
          const replay = yield* call("device_execute", input);
          expect(replay.isError, json(replay)).toBe(transition === "source-ended");
          expect(b.deviceProcessCalls.filter((x) => x.args[1] === "fill")).toHaveLength(
            transition === "source-ended" ? 0 : 1,
          );
          yield* assertIdle;
        }).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "background383-r16: local %s lifecycle survives foreground completion with the same credential",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { node, threadId, caps, call } = yield* localBackground383(
          `background383-life-${capability}`,
        );
        const open = (key: string) =>
          call(capability === "preview" ? "preview_open" : "device_open", {
            ...(capability === "preview"
              ? { show: false, reuseExistingTab: false }
              : { deviceId: "fixture-device382", platform: "android" }),
            clientRequestId: key,
          });
        const active = yield* open("active-control");
        expect(active.isError).not.toBe(true);
        if (capability === "preview") {
          const { resource } = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ resource: PreviewViewerResource }),
          )(active.structuredContent);
          expect(
            (yield* call("t3_preview_close", {
              tabId: resource.tabId,
              clientRequestId: "active-close",
            })).isError,
          ).not.toBe(true);
        }
        yield* backgroundRoster383(node, threadId, true, true);
        const shell = yield* node.threads.getThreadShell(threadId);
        expect(shell?.activeRunId).toBeNull();
        expect(shell?.pendingBackgroundTasks).toContainEqual(
          expect.objectContaining({ kind: "subagent", providerInstanceId: instanceId }),
        );
        const opened = yield* open("background-positive");
        // Run strict admission negatives before the positive oracle, including on the red candidate.
        const wrong = yield* node.registry.issue({
          threadId,
          providerInstanceId: ProviderInstanceId.make("background-wrong"),
          capabilities: caps,
        });
        const refused = yield* call(
          "preview_open",
          { show: false, clientRequestId: "background-wrong" },
          wrong,
        );
        expect(refused.isError).toBe(true);
        expect(json(refused)).toContain("parent_not_active");
        expect(opened.isError, json(opened)).not.toBe(true);
        if (capability === "preview") {
          const { resource } = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ resource: PreviewViewerResource }),
          )(opened.structuredContent);
          expect(
            (yield* call("preview_type", {
              tabId: resource.tabId,
              text: "background-native",
              clientRequestId: "background-type",
            })).isError,
          ).not.toBe(true);
          expect(
            browserExternalContexts.at(-1)!.effects.filter((x) => x === "text:background-native"),
          ).toHaveLength(1);
          expect((yield* call("preview_status", { tabId: resource.tabId })).isError).not.toBe(true);
          expect(
            (yield* call("t3_preview_close", {
              tabId: resource.tabId,
              clientRequestId: "background-close",
            })).isError,
          ).not.toBe(true);
          expect((yield* node.nativePreviewManager!.list({ threadId })).sessions).toHaveLength(0);
        } else {
          const { target } = yield* decodeTyped383Open(opened.structuredContent);
          expect(
            (yield* call("device_execute", {
              target,
              command: { type: "click", ref: "@e1" },
              clientRequestId: "background-click",
            })).isError,
          ).not.toBe(true);
          expect(
            (yield* call("device_execute", {
              target,
              command: { type: "snapshot", depth: 8 },
              clientRequestId: "background-read",
            })).structuredContent,
          ).toMatchObject({ type: "snapshot" });
          expect(node.deviceProcessCalls.filter((x) => x.args.includes("click"))).toHaveLength(1);
          expect(
            (yield* call("device_close", {
              deviceId: target.deviceId,
              clientRequestId: "background-close",
            })).isError,
          ).not.toBe(true);
          expect(yield* node.fixtureDevice!.sessionsForThread(threadId)).toHaveLength(0);
        }
        yield* backgroundRoster383(node, threadId, false);
        const ended = yield* open("background-ended");
        expect(ended.isError).toBe(true);
        expect(json(ended)).toContain("parent_not_active");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["source", "destination", "mixed"] as const)(
  "idle-attachment383-r16: exact browser bytes from %s with idle B and historical replay",
  (where) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, call, caps, credential, assertIdle } = yield* idleResource383Pair(
          `idle-attachment383-browser-${where}`,
        );
        const local = (name: string, input: Record<string, unknown>) =>
          nativeCall(a, parent, name, input, caps, credential);
        const opened = yield* call("preview_open", { show: false, clientRequestId: "open" });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ resource: PreviewViewerResource }),
        )(opened.structuredContent);
        const artifacts = [];
        const expected = [];
        if (where !== "destination") {
          artifacts.push(
            yield* stageInput383(
              local,
              a.url,
              "source.txt",
              new Uint8Array([7, 8, 9, 10]),
              "stage-source",
            ),
          );
          expected.push([7, 8, 9, 10]);
        }
        if (where !== "source") {
          artifacts.push(
            yield* stageInput383(
              call,
              b.url,
              "destination.txt",
              new Uint8Array([11, 12]),
              "stage-destination",
            ),
          );
          expected.push([11, 12]);
        }
        const fs = yield* FileSystem.FileSystem;
        const batches: number[][][] = [];
        browserExternalContexts.at(-1)!.controls.afterSetFiles = async (paths) => {
          const batch = [];
          for (const path of paths) batch.push(Array.from(await nativePromise(fs.readFile(path))));
          batches.push(batch);
        };
        const input = {
          resource,
          artifacts,
          locator: "input[type=file]",
          clientRequestId: "idle-upload",
        };
        const accepted = yield* call("preview_upload", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        expect(batches).toEqual([expected]);
        expect((yield* call("preview_upload", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(batches).toEqual([expected]);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["install", "reinstall"] as const)(
  "idle-attachment383-r16: typed %s consumes exact source bytes once without a B provider",
  (type) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, call, caps, credential, assertIdle } = yield* idleResource383Pair(
          `idle-attachment383-${type}`,
        );
        const opened = yield* call("device_open", {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        yield* Ref.update(b.deviceCapabilities, (current) => ({
          ...current,
          availableCommands: [...current.availableCommands, "reinstall"],
        }));
        const bytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);
        const artifact = yield* stageInput383(
          (name, input) => nativeCall(a, parent, name, input, caps, credential),
          a.url,
          "app.apk",
          bytes,
          "stage-app",
        );
        const input = {
          target,
          command: { type, app: "com.example.app", artifact },
          clientRequestId: "idle-install",
        };
        const accepted = yield* call("device_execute", input);
        expect(accepted.isError, json(accepted)).not.toBe(true);
        const calls = b.deviceProcessCalls.filter((x) => x.args[1] === type);
        expect(calls).toHaveLength(1);
        const fs = yield* FileSystem.FileSystem;
        expect(Array.from(yield* fs.readFile(calls[0]!.args.at(-1)!))).toEqual(Array.from(bytes));
        expect((yield* call("device_execute", input)).structuredContent).toEqual(
          accepted.structuredContent,
        );
        expect(b.deviceProcessCalls.filter((x) => x.args[1] === type)).toHaveLength(1);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "background383-r16: local %s retains current mode/settings/capability/archive refusals",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { node, threadId, credential, call } = yield* localBackground383(
          `background383-policy-${capability}`,
        );
        yield* backgroundRoster383(node, threadId, true, true);
        let next = 0;
        const open = (token?: Parameters<typeof call>[2]) =>
          call(
            capability === "preview" ? "preview_open" : "device_open",
            {
              ...(capability === "preview"
                ? { show: false, reuseExistingTab: false }
                : { deviceId: "fixture-device382", platform: "android" }),
              clientRequestId: `policy-${next++}`,
            },
            token,
          );
        expect((yield* open()).isError).not.toBe(true);
        const effects = () => ({
          browser: browserExternalContexts.length,
          device: node.deviceProcessCalls.length,
        });
        const before = effects();
        const insufficient = yield* node.registry.issue({
          threadId,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration"] as const),
        });
        expect((yield* open(insufficient)).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* node.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: false }
            : { enableAgentDeviceAccess: false },
        );
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* node.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: true }
            : { enableAgentDeviceAccess: true },
        );
        const targetId = ThreadId.make(`background383-mode-target-${capability}`);
        yield* node.threads.dispatch({
          type: "thread.create",
          threadId: targetId,
          projectId: node.project.id,
          commandId: CommandId.make("mode-target"),
          title: "Mode target",
          modelSelection: selection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        const original = (yield* node.registry.resolve(
          credential.config.authorizationHeader.slice(7),
        ))!;
        yield* node.authority.recheckInvocation(original, capability, targetId);
        yield* node.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId,
          commandId: CommandId.make("background-plan"),
          interactionMode: "plan",
        });
        expect(
          Exit.isFailure(
            yield* node.authority
              .recheckInvocation(original, capability, targetId)
              .pipe(Effect.exit),
          ),
        ).toBe(true);
        expect(effects()).toEqual(before);
        yield* node.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId,
          commandId: CommandId.make("background-default"),
          interactionMode: "default",
        });
        yield* node.threads.dispatch({
          type: "thread.archive",
          threadId,
          commandId: CommandId.make("background-archive"),
        });
        expect((yield* open()).isError).toBe(true);
        expect(effects()).toEqual(before);
        yield* node.threads.dispatch({
          type: "thread.unarchive",
          threadId,
          commandId: CommandId.make("background-unarchive"),
        });
        expect((yield* open()).isError).not.toBe(true);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each([false, true])(
  "background383-r16: native browser preparation rechecks background retirement (%s)",
  (retire) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { node, threadId, call } = yield* localBackground383(`background383-wait-${retire}`);
        yield* backgroundRoster383(node, threadId, true, true);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        browserExternalBoundary.beforeContextReturn = () =>
          nativePromise(
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            browserExternalBoundary.beforeContextReturn = async () => {};
          }),
        );
        const pending = yield* call("preview_open", {
          show: false,
          reuseExistingTab: false,
          clientRequestId: "held-preparation",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        if (retire) yield* backgroundRoster383(node, threadId, false);
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending);
        expect(result.isError, json(result)).toBe(retire);
        expect(
          browserExternalContexts.at(-1)!.effects.filter((x) => x === "new-page"),
        ).toHaveLength(retire ? 0 : 1);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each([false, true])(
  "background383-r16: queued device work rechecks original background while invoked work drains (%s)",
  (retire) =>
    Effect.scoped(
      Effect.gen(function* () {
        const queued = yield* Deferred.make<void>();
        let commands = 0;
        const tracer = Tracer.make({
          span: (options) => {
            if (options.name === "DeviceService.executeCommand" && ++commands === 2)
              Deferred.doneUnsafe(queued, Effect.void);
            return Tracer.nativeTracer.span(options);
          },
        });
        yield* Effect.gen(function* () {
          const { node, threadId, call } = yield* localBackground383(
            `background383-queue-${retire}`,
          );
          yield* backgroundRoster383(node, threadId, true, true);
          const opened = yield* call("device_open", {
            deviceId: "fixture-device382",
            platform: "android",
            clientRequestId: "open",
          });
          expect(opened.isError, json(opened)).not.toBe(true);
          const { target } = yield* decodeTyped383Open(opened.structuredContent);
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.set(node.deviceProcessHook, (input) =>
            input.args[1] === "click"
              ? Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
          );
          const first = yield* call("device_execute", {
            target,
            command: { type: "click", ref: "@e1" },
            clientRequestId: "click",
          }).pipe(Effect.forkChild);
          yield* Deferred.await(held);
          const second = yield* call("device_execute", {
            target,
            command: { type: "fill", ref: "@e1", text: "queued-background" },
            clientRequestId: "fill",
          }).pipe(Effect.forkChild);
          yield* Deferred.await(queued);
          expect(node.deviceProcessCalls.filter((x) => x.args[1] === "fill")).toHaveLength(0);
          if (retire) yield* backgroundRoster383(node, threadId, false);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(first);
          const result = yield* Fiber.join(second);
          expect(result.isError, json(result)).toBe(retire);
          expect(node.deviceProcessCalls.filter((x) => x.args[1] === "click")).toHaveLength(1);
          expect(node.deviceProcessCalls.filter((x) => x.args[1] === "fill")).toHaveLength(
            retire ? 0 : 1,
          );
        }).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "background383-r16: incoming %s authority still expires with its original foreground run",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, call, assertIdle } = yield* idleResource383Pair(
          `background383-incoming-${capability}`,
        );
        let next = 0;
        const open = () =>
          call(capability === "preview" ? "preview_open" : "device_open", {
            ...(capability === "preview"
              ? { show: false, reuseExistingTab: false }
              : { deviceId: "fixture-device382", platform: "android" }),
            clientRequestId: `incoming-${next++}`,
          });
        expect((yield* open()).isError).not.toBe(true);
        const before = {
          browser: browserExternalContexts.length,
          device: b.deviceProcessCalls.length,
        };
        yield* backgroundRoster383(a, parent, true, true);
        expect(
          Invocation.isLiveCallerShell(yield* a.threads.getThreadShell(parent), instanceId),
        ).toBe(true);
        const result = yield* open();
        expect(result.isError, json(result)).toBe(true);
        expect({
          browser: browserExternalContexts.length,
          device: b.deviceProcessCalls.length,
        }).toEqual(before);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["preview", "device"] as const)(
  "idle-attachment383-r16: %s staging and native consumption retain current authority refusals",
  (capability) =>
    Effect.scoped(
      Effect.gen(function* () {
        const nativePromise = yield* makeNativePromiseBridge;
        const { a, b, parent, child, call, caps, credential, grant, assertIdle } =
          yield* idleResource383Pair(`idle-attachment383-policy-${capability}`);
        const local = (name: string, input: Record<string, unknown>) =>
          nativeCall(a, parent, name, input, caps, credential);
        const bytes = new Uint8Array([7, 8, 9, 10]);
        const artifact = yield* stageInput383(
          local,
          a.url,
          capability === "preview" ? "input.txt" : "app.apk",
          bytes,
          "stage-policy",
        );
        const opened = yield* call(
          capability === "preview" ? "preview_open" : "device_open",
          capability === "preview"
            ? { show: false, clientRequestId: "open" }
            : { deviceId: "fixture-device382", platform: "android", clientRequestId: "open" },
        );
        expect(opened.isError, json(opened)).not.toBe(true);
        const input =
          capability === "preview"
            ? {
                ...(yield* Schema.decodeUnknownEffect(
                  Schema.Struct({ resource: PreviewViewerResource }),
                )(opened.structuredContent)),
                artifacts: [artifact],
                locator: "input[type=file]",
              }
            : {
                target: (yield* decodeTyped383Open(opened.structuredContent)).target,
                command: { type: "install", app: "com.example.app", artifact },
              };
        const fs = yield* FileSystem.FileSystem;
        const observed: number[][] = [];
        if (capability === "preview")
          browserExternalContexts.at(-1)!.controls.afterSetFiles = async (paths) => {
            for (const path of paths)
              observed.push(Array.from(await nativePromise(fs.readFile(path))));
          };
        let next = 0;
        const drive = (token = credential) =>
          call(
            capability === "preview" ? "preview_upload" : "device_execute",
            { ...input, clientRequestId: `policy-action-${next++}` },
            token,
          );
        const effects = () => ({
          files: observed.length,
          installs: b.deviceProcessCalls.filter((x) => x.args[1] === "install").length,
        });
        const before = effects();
        const wrong = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: ProviderInstanceId.make("attachment-wrong"),
          capabilities: caps,
        });
        expect((yield* drive(wrong)).isError).toBe(true);
        const reduced = yield* a.registry.issue({
          threadId: parent,
          providerInstanceId: instanceId,
          capabilities: new Set(["orchestration"] as const),
        });
        expect((yield* drive(reduced)).isError).toBe(true);
        yield* b.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: false }
            : { enableAgentDeviceAccess: false },
        );
        expect((yield* drive()).isError).toBe(true);
        yield* b.fixtureSettings.updateSettings(
          capability === "preview"
            ? { enableAgentBrowserAccess: true }
            : { enableAgentDeviceAccess: true },
        );
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parent,
          commandId: CommandId.make("attachment-plan"),
          interactionMode: "plan",
        });
        expect((yield* drive()).isError).toBe(true);
        yield* a.threads.dispatch({
          type: "thread.interaction-mode.set",
          threadId: parent,
          commandId: CommandId.make("attachment-default"),
          interactionMode: "default",
        });
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: capability !== "preview",
          device: capability !== "device",
        });
        expect((yield* drive()).isError).toBe(true);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "created-work",
          preview: true,
          device: true,
        });
        expect(
          (yield* call("t3_attachment_inspect", { source: { type: "capabilities" } })).isError,
        ).toBe(true);
        expect(
          (yield* call("t3_attachment_prepare_upload", {
            upload: { type: "file", name: "blocked.txt", mimeType: "text/plain", sizeBytes: 4 },
            clientRequestId: "blocked-stage",
          })).isError,
        ).toBe(true);
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          device: true,
        });
        yield* b.threads.dispatch({
          type: "thread.archive",
          threadId: child,
          commandId: CommandId.make("attachment-archive"),
        });
        expect((yield* drive()).isError).toBe(true);
        expect(
          (yield* call("t3_attachment_inspect", { source: { type: "capabilities" } })).isError,
        ).toBe(true);
        expect(
          (yield* call("t3_attachment_prepare_upload", {
            upload: { type: "file", name: "blocked.txt", mimeType: "text/plain", sizeBytes: 4 },
            clientRequestId: "archived-stage",
          })).isError,
        ).toBe(true);
        expect(effects()).toEqual(before);
        yield* b.threads.dispatch({
          type: "thread.unarchive",
          threadId: child,
          commandId: CommandId.make("attachment-unarchive"),
        });
        const restored = { ...input, clientRequestId: "restored-action" };
        const accepted = yield* call(
          capability === "preview" ? "preview_upload" : "device_execute",
          restored,
        );
        expect(accepted.isError, json(accepted)).not.toBe(true);
        expect(effects()).toEqual({
          files: capability === "preview" ? 1 : 0,
          installs: capability === "device" ? 1 : 0,
        });
        if (capability === "preview") expect(observed).toEqual([Array.from(bytes)]);
        expect(
          (yield* call(capability === "preview" ? "preview_upload" : "device_execute", restored))
            .structuredContent,
        ).toEqual(accepted.structuredContent);
        const after = effects();
        yield* a.threads.dispatch({
          type: "thread.stop",
          threadId: parent,
          commandId: CommandId.make("attachment-stop"),
        });
        expect((yield* drive()).isError).toBe(true);
        expect(effects()).toEqual(after);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["unchanged", "source-ended", "scope-retired"] as const)(
  "idle-attachment383-r16: direct inspection rechecks actual pending-file read (%s)",
  (transition) =>
    Effect.scoped(
      Effect.gen(function* () {
        const realFs = yield* FileSystem.FileSystem;
        let armed = false;
        let destinationPath = "";
        let reads = 0;
        let retire: Effect.Effect<unknown> = Effect.void;
        const intercepted = FileSystem.FileSystem.of({
          ...realFs,
          readFile: (path) =>
            realFs.readFile(path).pipe(
              Effect.tap(() => {
                if (!armed || !path.startsWith(destinationPath)) return Effect.void;
                armed = false;
                reads++;
                return retire;
              }),
            ),
        });
        const { a, b, parent, grant, call, assertIdle } = yield* idleResource383Pair(
          `idle-attachment383-read-${transition}`,
        ).pipe(Effect.provideService(FileSystem.FileSystem, intercepted));
        const bytes = new Uint8Array([7, 8, 9, 10]);
        const upload = {
          type: "file" as const,
          name: "inspect.txt",
          mimeType: "text/plain",
          sizeBytes: bytes.length,
        };
        const artifact = yield* stageInput383(call, b.url, upload.name, bytes, "stage-inspect");
        destinationPath = b.config.attachmentsDir;
        retire =
          transition === "source-ended"
            ? a.threads
                .dispatch({
                  type: "thread.stop",
                  threadId: parent,
                  commandId: CommandId.make("retire-inspection"),
                })
                .pipe(Effect.orDie)
            : transition === "scope-retired"
              ? b.authority
                  .setGrant({
                    ...grant,
                    resourceScope: "created-work",
                    preview: true,
                    device: true,
                  })
                  .pipe(Effect.orDie)
              : Effect.void;
        armed = true;
        const result = yield* call("t3_attachment_inspect", {
          source: { type: "upload", attachmentId: artifact.attachmentId, upload },
        });
        expect(reads).toBe(1);
        expect(result.isError, json(result)).toBe(transition !== "unchanged");
        if (transition === "unchanged")
          expect(result.structuredContent).toMatchObject({
            inspection: { stage: "staged", reference: { digest: artifact.digest } },
          });
        expect(b.deviceProcessCalls).toHaveLength(0);
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it.live.each(["source-ended", "grant-retired"] as const)(
  "idle-attachment383-r16: typed install rechecks native readiness (%s)",
  (transition) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { a, b, parent, call, caps, credential, grant, assertIdle } =
          yield* idleResource383Pair(`idle-attachment383-ready-${transition}`);
        const opened = yield* call("device_open", {
          deviceId: "fixture-device382",
          platform: "android",
          clientRequestId: "open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { target } = yield* decodeTyped383Open(opened.structuredContent);
        const artifact = yield* stageInput383(
          (name, input) => nativeCall(a, parent, name, input, caps, credential),
          a.url,
          "app.apk",
          new Uint8Array([7, 8, 9, 10]),
          "stage-readiness",
        );
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Ref.set(
          b.deviceSummaryHook,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        );
        const input = {
          target,
          command: { type: "install", app: "com.example.app", artifact },
          clientRequestId: "held-install",
        };
        const pending = yield* call("device_execute", input).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        if (transition === "source-ended")
          yield* a.threads.dispatch({
            type: "thread.stop",
            threadId: parent,
            commandId: CommandId.make("readiness-stop"),
          });
        else
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            preview: true,
            device: false,
          });
        yield* Deferred.succeed(release, undefined);
        const denied = yield* Fiber.join(pending);
        expect(denied.isError, json(denied)).toBe(true);
        expect(b.deviceProcessCalls.filter((x) => x.args[1] === "install")).toHaveLength(0);
        yield* Ref.set(b.deviceSummaryHook, Effect.void);
        if (transition === "grant-retired") {
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            preview: true,
            device: true,
          });
          const retry = { ...input, clientRequestId: "restored-install" };
          const accepted = yield* call("device_execute", retry);
          expect(accepted.isError, json(accepted)).not.toBe(true);
          expect((yield* call("device_execute", retry)).structuredContent).toEqual(
            accepted.structuredContent,
          );
          expect(b.deviceProcessCalls.filter((x) => x.args[1] === "install")).toHaveLength(1);
        }
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);

it("native383-rpc-groups: production partitions preserve the complete public method schemas and middleware", () => {
  const groups = [CoreServerWsRpcGroup, OrchestrationServerWsRpcGroup, ForkWsRpc.group];
  const registered = [
    ...CoreServerWsRpcGroup.requests.values(),
    ...OrchestrationServerWsRpcGroup.requests.values(),
    ...ForkWsRpc.group.requests.values(),
  ];
  expect(new Set(registered.map((rpc) => rpc._tag)).size).toBe(registered.length);
  expect(registered.map((rpc) => rpc._tag).sort()).toEqual(
    Array.from(WsRpcGroup.requests.keys()).sort(),
  );
  for (const group of groups) {
    expect(group.annotations).toEqual(ServerWsRpcGroup.annotations);
    for (const rpc of group.requests.values()) {
      const original = ServerWsRpcGroup.requests.get(rpc._tag)!;
      expect(rpc.payloadSchema).toBe(original.payloadSchema);
      expect(rpc.successSchema).toBe(original.successSchema);
      expect(rpc.errorSchema).toBe(original.errorSchema);
      expect(rpc.defectSchema).toBe(original.defectSchema);
      expect(rpc.annotations).toEqual(original.annotations);
      expect(rpc.middlewares).toEqual(original.middlewares);
      if (group !== ForkWsRpc.group) expect(rpc).toBe(original);
    }
  }
});

const decodeVault379Open = Schema.decodeUnknownEffect(
  Schema.Struct({ resource: PreviewViewerResource }),
);

it.live(
  "vault379: preview-granted authority-v1 peers cannot start or end destination vault logins",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { b, child, call, assertIdle } = yield* idleResource383Pair("vault379-linked");
        const opened = yield* call("preview_open", {
          url: "https://example.test/login",
          profileId: "default",
          show: false,
          clientRequestId: "vault379-open",
        });
        expect(opened.isError, json(opened)).not.toBe(true);
        const { resource } = yield* decodeVault379Open(opened.structuredContent);
        const before = yield* b.nativePreviewManager!.list({ threadId: child });
        const ended = yield* call("preview_end_login", {
          tabId: resource.tabId,
          clientRequestId: "vault379-end",
        });
        const filled = yield* call("preview_fill_credential", {
          tabId: resource.tabId,
          item: "synthetic-item",
          field: "password",
          locator: "#password",
          clientRequestId: "vault379-fill",
        });
        expect(json(ended)).toContain("capability_denied");
        expect(json(filled)).toContain("capability_denied");
        expect(yield* b.nativePreviewManager!.list({ threadId: child })).toEqual(before);
        const status = yield* call("preview_status", { tabId: resource.tabId });
        expect(status.isError, json(status)).not.toBe(true);
        expect(json(status)).not.toContain('"loginInProgress":true');
        expect(json(status)).toContain("https://example.test/login");
        yield* assertIdle;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  60_000,
);
