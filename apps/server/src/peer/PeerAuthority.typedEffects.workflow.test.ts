import * as Tracer from "effect/Tracer";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as Path from "effect/Path";
import { agentDeviceConfigPath } from "../device/AgentDeviceTarget.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "../device/DeviceToolchain.ts";
import { LOCAL_DEVICE_HOST_ID, DeviceId } from "@t3tools/contracts";
import * as AttachmentUpload from "../assets/AttachmentUpload.ts";
import { PeerAttachmentReference } from "@t3tools/contracts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as Fiber from "effect/Fiber";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";
import { resolveAttachmentPath } from "../attachmentStore.ts";
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
import { CommandId, MessageId, ThreadId } from "@t3tools/contracts";
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
import {
  activate,
  assertTyped383SourceIdle,
  caller,
  decodeAutomationSpectrumResult,
  decodeLaunchReceipt,
  decodeManualRun,
  decodeSavedPngReceipt,
  decodeSend,
  decodeTyped383Artifact,
  decodeTyped383Command,
  decodeTyped383Example,
  decodeTyped383Open,
  decodeTyped383Target,
  decodeTyped383Upload,
  instanceId,
  json,
  makeNode,
  nativeCall,
  nativeInvalidParams,
  seedParent,
  selection,
  typed383Digest,
  typed383Pair,
} from "./PeerAuthority.workflow.fixture.ts";

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
