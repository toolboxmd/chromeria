import * as ServerSettings from "../serverSettings.ts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  DeviceCommandTarget,
  DeviceCommandResult,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer } from "effect/ai";
import { HttpClient, HttpClientResponse } from "effect/http";
import * as Config from "../config.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as DeviceHost from "./DeviceHost.ts";
import { makeDeviceHubOperations } from "./DeviceHubOperations.ts";
import * as DeviceService from "./DeviceService.ts";
import { AGENT_DEVICE_VERSION, DEVICE_HUB_VERSION } from "./DeviceToolchain.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import { createAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import * as RequestRuntime from "../mcp/McpRequestRuntime.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as AccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as Net from "@t3tools/shared/Net";

const decodeCommandResult = Schema.decodeUnknownEffect(DeviceCommandResult);
const decodeOpened = Schema.decodeUnknownEffect(
  Schema.Struct({ target: DeviceCommandTarget, quickStart: Schema.String }),
);

const threadId = ThreadId.make("device-command383");
const scope: Invocation.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("device383"),
  requestNamespace: "isolated-device383",
  issuedAt: 1,
  thread: {
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "isolated383",
  },
  client: undefined,
  capabilities: new Set(["device"]),
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "device383", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "device383", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const png = new Uint8Array(24);
new DataView(png.buffer).setUint32(0, 0x89504e47);
new DataView(png.buffer).setUint32(4, 0x0d0a1a0a);
new DataView(png.buffer).setUint32(12, 0x49484452);
new DataView(png.buffer).setUint32(16, 1206);
new DataView(png.buffer).setUint32(20, 2622);

/** Real device service, settings, disk receipts and artifact owner. External host/process leaves only.
 * The access-shell fixture is limited to these domain tests, not whole peer/kernel acceptance.
 */
const fixture = Effect.gen(function* () {
  const configContext = yield* Config.layerTest(process.cwd(), {
    prefix: "device383-domain-",
  }).pipe(Layer.fresh, Layer.build);
  const configLayer = Layer.succeedContext(configContext);
  const config = Context.get(configContext, Config.ServerConfig);
  const database = yield* Sqlite.layerConfig.pipe(
    Layer.provide(configLayer),
    Layer.fresh,
    Layer.build,
  );
  const db = Layer.succeedContext(database);
  const settingsContext = yield* Settings.layer.pipe(
    Layer.provide(Secrets.layer.pipe(Layer.provide(configLayer))),
    Layer.provide(configLayer),
    Layer.provide(db),
    Layer.build,
  );
  const settings = Context.get(settingsContext, Settings.ServerSettingsService);
  yield* settings.updateSettings({ enableDeviceSupport: true, enableAgentDeviceAccess: true });
  const fs = yield* FileSystem.FileSystem;
  const install = `${config.baseDir}/tools/agent-device/${AGENT_DEVICE_VERSION}`;
  const entry = `${install}/node_modules/agent-device/bin/agent-device.mjs`;
  yield* fs.makeDirectory(`${install}/node_modules/agent-device/bin`, { recursive: true });
  yield* fs.writeFileString(entry, "// Deterministic external process leaf. Never executed.\n");
  yield* fs.writeFileString(`${install}/.install-complete`, AGENT_DEVICE_VERSION);
  const calls: ProcessRunner.ProcessRunInput[] = [];
  let preparations = 0;
  let captures = 0;
  let failPreparation = false;
  let oldVersion = false;
  let badOutput = false;
  let wrongTarget = false;
  let largeSnapshot = false;
  let outsideMedia = false;
  let failCaptureStart = false;
  let revokeAfter: string | undefined;
  const mediaPaths = new Map<string, string>();
  const hub = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.url.includes("screenshot")) {
        captures++;
        return HttpClientResponse.fromWeb(request, new Response(png));
      }
      return HttpClientResponse.fromWeb(
        request,
        Response.json({
          simulators: [],
          emulators: [
            {
              id: "emulator-383",
              name: "Isolated",
              platform: "android",
              version: "fixture",
              booted: true,
              physical: false,
            },
          ],
        }),
      );
    }),
  );
  const run: DeviceHost.DeviceHostReady["run"] = (command, args) => {
    // Opening an Android device probes the host's ffmpeg encoders before streaming.
    if (args[0] === "-e" && args[1]?.includes('"-encoders"'))
      return Effect.succeed({ code: 0, stdout: " V....D libx264  H.264\n", stderr: "" });
    expect([command, ...args]).toEqual(["emulator", "-list-avds"]);
    return Effect.succeed({ code: 0, stdout: "Isolated\n", stderr: "" });
  };
  const ready: DeviceHost.DeviceHostReady = {
    nodePath: "/remote-host/node-must-never-execute",
    hub: { origin: "http://device383.invalid" },
    devices: makeDeviceHubOperations({
      hostId: "local",
      origin: "http://device383.invalid",
      http: hub,
      run,
      androidAvailable: Effect.succeed(true),
    }),
    helpers: { serveSimAxSettings: null, serveSimCli: null },
    run,
  };
  const host: DeviceHost.DeviceHost["Service"] = {
    id: "local",
    label: "Isolated leaf",
    summary: Effect.sync(() => ({
      id: "local",
      kind: "local" as const,
      label: "Isolated leaf",
      platforms: [{ platform: "android" as const, available: true }],
      hubInstalled: true,
      agentDeviceInstalled: true,
      tools: {
        agent: {
          requiredVersion: AGENT_DEVICE_VERSION,
          installedVersions: [oldVersion ? "0.21.12" : AGENT_DEVICE_VERSION],
          runningVersion: oldVersion ? "0.21.12" : AGENT_DEVICE_VERSION,
        },
        hub: {
          requiredVersion: DEVICE_HUB_VERSION,
          installedVersions: [DEVICE_HUB_VERSION],
          runningVersion: DEVICE_HUB_VERSION,
        },
      },
    })),
    platformAvailability: (platform) => Effect.succeed({ platform, available: true }),
    ensureReady: () => Effect.succeed(ready),
    ensureAgentReady: () =>
      Effect.gen(function* () {
        preparations++;
        yield* fs.writeFileString(`${config.stateDir}/prepared`, "prepared").pipe(Effect.orDie);
        if (failPreparation)
          return yield* new DeviceHost.DeviceHostError({
            hostId: "local",
            step: "fixture",
            cause: "lost after preparation",
          });
        return {
          ...ready,
          agentDevice: {
            baseUrl: "http://private383.invalid",
            token: "PRIVATE_TOKEN_383",
            entryPath: entry,
          },
        };
      }),
    current: Effect.succeed(ready),
    stopAgent: Effect.void,
    stop: Effect.void,
  };
  const runner = ProcessRunner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        calls.push(input);
        const family = input.args[1];
        const positionals = input.args.slice(input.args.indexOf("--") + 1);
        const session = input.args[input.args.indexOf("--session") + 1];
        const captureKey = `${session}:${family}`;
        let data: unknown = {};
        if (family === "capabilities")
          data = {
            device: {
              id: wrongTarget ? "rival383" : "emulator-383",
              platform: "android",
              target: "mobile",
              kind: "emulator",
            },
            availableCommands: [
              "open",
              "snapshot",
              "click",
              "fill",
              "screenshot",
              "close",
              "record",
              "trace",
              "get",
              "install",
              "reinstall",
            ],
          };
        if (family === "snapshot")
          data = {
            nodes: [
              {
                index: 1,
                ref: "e1",
                role: "button",
                label: "Continue",
                privatePath: "/host/private",
              },
            ],
            config: "/host/private",
            token: "PRIVATE_TOKEN_383",
          };
        if (family === "snapshot" && largeSnapshot)
          data = {
            nodes: Array.from({ length: 200 }, (_, index) => ({
              index,
              ref: `e${index}`,
              label: "x".repeat(400),
            })),
          };
        if (family === "get")
          data = { text: "Continue", privatePath: "/host/private", token: "PRIVATE_TOKEN_383" };
        if (family === "record" || family === "trace") {
          if (positionals[0] === "start") {
            if (failCaptureStart) {
              failCaptureStart = false;
              return {
                code: ChildProcessSpawner.ExitCode(1),
                stdout: "",
                stderr: "Capture start failed",
                timedOut: false,
                stdoutTruncated: false,
                stderrTruncated: false,
                stdoutInvalidUtf8: false,
                stderrInvalidUtf8: false,
              };
            }
            const path = positionals[1];
            if (path === undefined) throw new Error("Expected private capture output path");
            mediaPaths.set(captureKey, path);
          }
          const outPath = mediaPaths.get(captureKey)!;
          if (positionals[0] === "stop") {
            if (family === "record") {
              const bytes = new Uint8Array(24);
              new DataView(bytes.buffer).setUint32(4, 0x66747970);
              yield* fs.writeFile(outPath, bytes).pipe(Effect.orDie);
            } else
              yield* fs
                .writeFileString(outPath, "trace PRIVATE_TOKEN_383 http://private383.invalid")
                .pipe(Effect.orDie);
          }
          data = {
            [family === "record" ? "recording" : "trace"]:
              positionals[0] === "start" ? "started" : "stopped",
            outPath,
            artifacts: [{ field: "outPath", localPath: outsideMedia ? entry : outPath }],
            durationMs: 200,
            capturedDurationMs: 150,
            recorder: "unconfirmed",
            nativePathDisposition: "pending",
          };
        }
        if (family === revokeAfter)
          yield* settings.updateSettings({ enableAgentDeviceAccess: false }).pipe(Effect.orDie);
        return {
          code: ChildProcessSpawner.ExitCode(0),
          stdout: badOutput
            ? "private /host/path PRIVATE_TOKEN_383"
            : JSON.stringify({ success: true, data }),
          stderr: "",
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const requests = yield* RequestRuntime.layer.pipe(
    Layer.provide(db),
    Layer.provide(configLayer),
    Layer.build,
  );
  const service = yield* DeviceService.make.pipe(
    Effect.provideService(DeviceHost.DeviceHost, host),
    Effect.provideService(HttpClient.HttpClient, hub),
    Effect.provideService(ProcessRunner.ProcessRunner, runner),
    Effect.provide(Layer.mergeAll(configLayer, Layer.succeedContext(settingsContext))),
  );
  const serverContext = yield* McpHttpServer.layerDeviceToolkit.pipe(
    Layer.provide(ServerSettings.layerTest()),
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(AccessTestkit.liveThreadsLayer),
    Layer.provide(Layer.succeed(DeviceService.DeviceService, service)),
    Layer.provide(Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone })),
    Layer.provide(configLayer),
    Layer.build,
  );
  const server = Context.get(serverContext, McpServer.McpServer);
  const call = (name: string, args: Record<string, unknown>, invocation = scope) =>
    server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(Invocation.McpInvocationContext, invocation),
        Effect.provideService(McpSchema.McpServerClient, client),
        Effect.provide(Layer.succeedContext(requests)),
      );
  const open = (key: string) =>
    call("device_open", {
      deviceId: "emulator-383",
      platform: "android",
      clientRequestId: key,
    }).pipe(
      Effect.flatMap((result) => {
        expect(result.isError, JSON.stringify(result)).not.toBe(true);
        return decodeOpened(result.structuredContent);
      }),
    );
  const publishPackage = Effect.gen(function* () {
    const id = createAttachmentId(threadId, "apk");
    if (id === null) throw new Error("Expected qualified native attachment id");
    const attachment = {
      type: "file" as const,
      id,
      name: "app.apk",
      mimeType: "application/vnd.android.package-archive",
      sizeBytes: 4,
    };
    const path = resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment });
    if (path === null) throw new Error("Expected canonical native file path");
    yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
    yield* fs.writeFile(path, new Uint8Array([0x50, 0x4b, 0x03, 0x04]), { flag: "wx" });
    const reference = yield* Context.get(
      requests,
      AttachmentStaging.AttachmentStaging,
    ).publishReference(scope, attachment);
    return { reference, path };
  });
  return {
    publishPackage,
    call,
    open,
    calls,
    fs,
    config,
    settings,
    sql: Context.get(database, SqlClient.SqlClient),
    preparations: () => preparations,
    captures: () => captures,
    failCaptureStart: () => {
      failCaptureStart = true;
    },
    failPreparation: () => {
      failPreparation = true;
    },
    oldVersion: () => {
      oldVersion = true;
    },
    revokeAfter: (family: string) => {
      revokeAfter = family;
    },
    largeSnapshot: () => {
      largeSnapshot = true;
    },
    wrongTarget: () => {
      wrongTarget = true;
    },
    outsideMedia: () => {
      outsideMedia = true;
    },
    badOutput: () => {
      badOutput = true;
    },
  };
});

it.effect(
  "registered typed loop binds its session, redacts private output and publishes native PNG references",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const opened = yield* f.open("open-loop");
      expect(opened.quickStart).toContain("device_execute");
      expect(opened.quickStart).not.toContain("--config");
      const execute = (key: string, command: unknown) =>
        f.call("device_execute", { target: opened.target, command, clientRequestId: key });
      expect(
        (yield* execute("app-open", { type: "open", app: "com.example.app" })).isError,
      ).not.toBe(true);
      const tree = yield* execute("snapshot", { type: "snapshot", interactiveOnly: true });
      expect(tree.isError, JSON.stringify(tree)).not.toBe(true);
      expect(tree.structuredContent).toMatchObject({
        type: "snapshot",
        nodes: [{ ref: "e1", label: "Continue" }],
      });
      expect(JSON.stringify(tree)).not.toMatch(/PRIVATE_TOKEN_383|privatePath|\/host\/private/);
      for (const [key, command] of [
        ["click", { type: "click", ref: "@e1" }],
        ["fill", { type: "fill", ref: "@e1", text: "--config /evil\n" }],
      ] as const) {
        const result = yield* execute(key, command);
        expect(result.isError, JSON.stringify(result)).not.toBe(true);
      }
      expect(f.calls.find((call) => call.args[1] === "fill")?.stdin).toBe("--config /evil\n\r\n");
      expect(
        f.calls.every(
          (call) => call.args.includes("--serial") && call.args.includes("emulator-383"),
        ),
      ).toBe(true);
      expect(f.calls.every((call) => call.command !== "/remote-host/node-must-never-execute")).toBe(
        true,
      );
      expect(
        new Set(f.calls.map((call) => call.args[call.args.indexOf("--session") + 1])).size,
      ).toBe(1);
      const screenshot = yield* execute("png", { type: "screenshot" });
      expect(screenshot.isError, JSON.stringify(screenshot)).not.toBe(true);
      const artifact = yield* decodeCommandResult(screenshot.structuredContent);
      expect(artifact.type).toBe("artifact");
      if (artifact.type === "artifact") {
        expect(artifact.artifact).toMatchObject({
          mimeType: "image/png",
          sizeBytes: 24,
          environmentId: "device383",
        });
        expect(artifact.artifact.digest).toMatch(/^[0-9a-f]{64}$/);
      }
      expect((yield* f.sql`SELECT id FROM attachment_staging`).length).toBe(1);
      const replay = yield* execute("png", { type: "screenshot" });
      expect(replay.structuredContent).toEqual(screenshot.structuredContent);
      expect(f.captures()).toBe(1);
      const closed = yield* execute("close", { type: "close" });
      expect(closed.isError, JSON.stringify(closed)).not.toBe(true);
      const reopened = yield* f.open("reopen-loop");
      expect(reopened.target.toolSessionId).not.toBe(opened.target.toolSessionId);
      yield* execute("close", { type: "close" });
      const later = yield* f.call("device_execute", {
        target: reopened.target,
        command: { type: "click", ref: "@e1" },
        clientRequestId: "later-click",
      });
      expect(later.isError, JSON.stringify(later)).not.toBe(true);
      const stale = yield* execute("stale-click", { type: "click", ref: "@e1" });
      expect(stale.isError).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "pure version and consent refusals do nothing; lost preparation and malformed replies stay unknown without re-execution",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("failure-open");
      const preparedBefore = f.preparations();
      const execute = (key: string, command: unknown) =>
        f.call("device_execute", { target, command, clientRequestId: key });
      yield* f.settings.updateSettings({ enableAgentDeviceAccess: false });
      expect((yield* execute("consent", { type: "click", ref: "@e1" })).isError).toBe(true);
      expect(f.preparations()).toBe(preparedBefore);
      yield* f.settings.updateSettings({ enableAgentDeviceAccess: true });
      const invalid = yield* execute("invalid", {
        type: "click",
        ref: "@e1",
        session: "other",
        daemonAuthToken: "arbitrary",
      }).pipe(Effect.flip);
      expect(invalid._tag).toBe("InvalidParams");
      expect(f.preparations()).toBe(preparedBefore);
      f.failPreparation();
      const lost = yield* execute("lost", { type: "click", ref: "@e1" });
      expect(JSON.stringify(lost)).toContain("outcome_unknown");
      expect(f.preparations()).toBe(preparedBefore + 1);
      expect(f.calls).toHaveLength(0);
      const repeated = yield* execute("lost", { type: "click", ref: "@e1" });
      expect(JSON.stringify(repeated)).toContain("outcome_unknown");
      expect(f.preparations()).toBe(preparedBefore + 1);
      f.oldVersion();
      expect((yield* execute("old-version", { type: "snapshot" })).isError).toBe(true);
      expect(f.preparations()).toBe(preparedBefore + 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "wrong caller, exact target and known platform refusals leave the external leaf untouched",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("boundary-open");
      const preparedBefore = f.preparations();
      const otherScope: Invocation.McpThreadInvocationScope = {
        ...scope,
        thread: { ...scope.thread, threadId: ThreadId.make("rival383") },
      };
      const deniedActor = yield* f.call(
        "device_execute",
        { target, command: { type: "click", ref: "@e1" }, clientRequestId: "wrong-actor" },
        otherScope,
      );
      expect(deniedActor.isError).toBe(true);
      for (const [key, altered] of [
        ["generation", { ...target, toolSessionId: "00000000-0000-4000-8000-000000000000" }],
        ["device", { ...target, deviceId: "rival-device" }],
        ["host", { ...target, hostId: "rival-host" }],
      ] as const) {
        expect(
          (yield* f.call("device_execute", {
            target: altered,
            command: { type: "click", ref: "@e1" },
            clientRequestId: key,
          })).isError,
        ).toBe(true);
      }
      const wrongPlatform = yield* f.call("device_execute", {
        target,
        command: { type: "prepare", action: "ios-runner" },
        clientRequestId: "wrong-platform",
      });
      expect(wrongPlatform.isError).toBe(true);
      expect(f.preparations()).toBe(preparedBefore);
      expect(f.calls).toHaveLength(0);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "a malformed committed adapter reply remains unknown and does not expose private diagnostics or run twice",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("malformed-open");
      f.badOutput();
      const input = {
        target,
        command: { type: "click", ref: "@e1" },
        clientRequestId: "malformed383",
      };
      for (let attempt = 0; attempt < 2; attempt++) {
        const result = yield* f.call("device_execute", input);
        expect(JSON.stringify(result)).toContain("outcome_unknown");
        expect(JSON.stringify(result)).not.toMatch(/PRIVATE_TOKEN_383|\/host\/path/);
      }
      expect(f.calls).toHaveLength(1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "registered media uses native one-use claims and preserves recorder uncertainty and exact-session replay",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("media-open");
      const execute = (key: string, command: unknown) =>
        f.call("device_execute", { target, command, clientRequestId: key });
      for (const type of ["record", "trace"] as const) {
        expect((yield* execute(`${type}-start`, { type, action: "start" })).isError).not.toBe(true);
        const stopped = yield* execute(`${type}-stop`, { type, action: "stop" });
        expect(stopped.isError, JSON.stringify(stopped)).not.toBe(true);
        const result = yield* decodeCommandResult(stopped.structuredContent);
        expect(result.type).toBe("artifacts");
        if (result.type === "artifacts") {
          expect(result.artifacts).toHaveLength(1);
          expect(result.artifacts[0]).toMatchObject({
            environmentId: "device383",
            mimeType: type === "record" ? "video/mp4" : "text/plain",
          });
          const reference = result.artifacts[0];
          if (reference === undefined) throw new Error("Expected one qualified media artifact");
          const path = resolveAttachmentPath({
            attachmentsDir: f.config.attachmentsDir,
            attachment: {
              type: "file",
              id: reference.attachmentId,
              name: reference.name,
              mimeType: reference.mimeType,
              sizeBytes: reference.sizeBytes,
            },
          });
          if (path === null) throw new Error("Expected native claimed media path");
          const bytes = yield* f.fs.readFile(path);
          const expected =
            type === "record"
              ? new Uint8Array([
                  0, 0, 0, 0, 0x66, 0x74, 0x79, 0x70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                  0,
                ])
              : new TextEncoder().encode("trace [private runtime] [private runtime]");
          expect(Array.from(bytes)).toEqual(Array.from(expected));
          expect(reference.digest).toBe(Hex.encode(sha256(expected)));
          expect(reference.sizeBytes).toBe(expected.length);
          const staged =
            (yield* f.sql`SELECT digest,expected_digest,manifest,consumer FROM attachment_staging WHERE id=${reference.attachmentId}`)[0];
          expect(staged).toMatchObject({
            digest: reference.digest,
            expected_digest: reference.digest,
            consumer: null,
          });
          expect(JSON.parse(String(staged?.manifest))).toMatchObject({
            name: reference.name,
            mimeType: reference.mimeType,
            sizeBytes: expected.length,
          });
          expect(result.recording).toMatchObject({
            recorder: "unconfirmed",
            nativePathDisposition: "pending",
            durationMs: 200,
            capturedDurationMs: 150,
          });
        }
        expect(JSON.stringify(stopped)).not.toMatch(
          /PRIVATE_TOKEN_383|private383|outPath|localPath|device383-domain/,
        );
        const count = f.calls.length;
        expect(
          (yield* execute(`${type}-stop`, { type, action: "stop" })).structuredContent,
        ).toEqual(stopped.structuredContent);
        expect(f.calls).toHaveLength(count);
        const noCapture = yield* execute(`${type}-stop-new`, { type, action: "stop" });
        expect(noCapture.isError).toBe(true);
        expect(f.calls).toHaveLength(count);
      }
      expect((yield* f.sql`SELECT id FROM attachment_staging`).length).toBe(2);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect.each(["record", "trace"] as const)(
  "a failed %s start releases the capture for the same session",
  (type) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("failed-capture-open");
      const execute = (key: string, action: "start" | "stop") =>
        f.call("device_execute", { target, command: { type, action }, clientRequestId: key });
      f.failCaptureStart();
      const failed = yield* execute("failed-start", "start");
      expect(failed.isError).toBe(true);
      expect(JSON.stringify(failed)).toContain("outcome_unknown");
      expect(f.calls.filter((call) => call.args[1] === type)).toHaveLength(1);
      const restarted = yield* execute("fresh-start", "start");
      expect(restarted.isError, JSON.stringify(restarted)).not.toBe(true);
      const stopped = yield* execute("fresh-stop", "stop");
      expect(stopped.isError, JSON.stringify(stopped)).not.toBe(true);
      expect((yield* decodeCommandResult(stopped.structuredContent)).type).toBe("artifacts");
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "actual target mismatch and escaped media paths cannot dispatch an action or claim an artifact",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("escaped-media-open");
      const execute = (key: string, command: unknown) =>
        f.call("device_execute", { target, command, clientRequestId: key });
      expect(
        (yield* execute("record-start", { type: "record", action: "start" })).isError,
      ).not.toBe(true);
      f.outsideMedia();
      const input = { type: "record", action: "stop" };
      const escaped = yield* execute("escaped-stop", input);
      expect(JSON.stringify(escaped)).toContain("outcome_unknown");
      expect((yield* f.sql`SELECT id FROM attachment_staging`).length).toBe(0);
      const count = f.calls.length;
      yield* execute("escaped-stop", input);
      expect(f.calls).toHaveLength(count);
      f.wrongTarget();
      const mismatch = yield* execute("mismatched-device", { type: "click", ref: "@e1" });
      expect(mismatch.isError).toBe(true);
      expect(f.calls.at(-1)?.args[1]).toBe("capabilities");
      expect(f.calls.filter((call) => call.args[1] === "click")).toHaveLength(0);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "batch validates every nested intent before effects and replays one exact session sequence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("batch-open");
      const execute = (key: string, command: unknown) =>
        f.call("device_execute", { target, command, clientRequestId: key });
      const prepared = f.preparations();
      for (const steps of [
        [
          { type: "click", ref: "@e1" },
          { type: "fill", ref: "@e1", text: "safe", daemonAuthToken: "BORROWED" },
        ],
        [{ type: "batch", steps: [{ type: "close" }] }],
      ])
        expect(
          (yield* execute("invalid-batch", { type: "batch", steps }).pipe(Effect.flip))._tag,
        ).toBe("InvalidParams");
      expect(
        (yield* execute("early-close", {
          type: "batch",
          steps: [{ type: "close" }, { type: "click", ref: "@e1" }],
        })).isError,
      ).toBe(true);
      expect(f.preparations()).toBe(prepared);
      expect(f.calls).toHaveLength(0);
      const command = {
        type: "batch",
        steps: [
          { type: "snapshot" },
          { type: "click", ref: "@e1" },
          { type: "fill", ref: "@e1", text: "value" },
        ],
      };
      const first = yield* execute("batch-loop", command);
      expect(first.isError, JSON.stringify(first)).not.toBe(true);
      expect(first.structuredContent).toMatchObject({
        type: "batch",
        results: [
          { type: "snapshot", nodes: [{ ref: "e1" }] },
          { type: "action", status: "dispatched" },
          { type: "action", status: "dispatched" },
        ],
      });
      expect(f.calls.map((call) => call.args[1])).toEqual([
        "capabilities",
        "snapshot",
        "click",
        "fill",
      ]);
      expect(f.preparations()).toBe(prepared + 1);
      expect((yield* execute("batch-loop", command)).structuredContent).toEqual(
        first.structuredContent,
      );
      expect(f.calls).toHaveLength(4);
      expect(f.preparations()).toBe(prepared + 1);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "batch checks present consent at each effect and does not repeat partial work after refusal",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("batch-revoke-open");
      f.revokeAfter("click");
      const input = {
        target,
        command: {
          type: "batch",
          steps: [
            { type: "click", ref: "@e1" },
            { type: "fill", ref: "@e1", text: "must not run" },
          ],
        },
        clientRequestId: "batch-revoke",
      };
      const first = yield* f.call("device_execute", input);
      expect(JSON.stringify(first)).toContain("outcome_unknown");
      expect(f.calls.map((call) => call.args[1])).toEqual(["capabilities", "click"]);
      yield* f.settings.updateSettings({ enableAgentDeviceAccess: true });
      const repeated = yield* f.call("device_execute", input);
      expect(JSON.stringify(repeated)).toContain("outcome_unknown");
      expect(f.calls).toHaveLength(2);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "qualified app packages are checked by the native artifact owner before any preparation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("package-open");
      const { reference, path } = yield* f.publishPackage;
      const execute = (key: string, artifact: unknown) =>
        f.call("device_execute", {
          target,
          command: { type: "install", artifact, app: "com.example.app" },
          clientRequestId: key,
        });
      const prepared = f.preparations();
      for (const [index, artifact] of [
        { ...reference, environmentId: "other383" },
        { ...reference, owner: "0".repeat(64) },
        { ...reference, digest: "0".repeat(64) },
        { ...reference, name: "borrowed.apk" },
      ].entries())
        expect((yield* execute(`invalid-package-${index}`, artifact)).isError).toBe(true);
      expect(f.preparations()).toBe(prepared);
      expect(f.calls).toHaveLength(0);
      const installed = yield* execute("install-package", reference);
      expect(installed.isError, JSON.stringify(installed)).not.toBe(true);
      expect(f.calls.at(-1)?.args.slice(f.calls.at(-1)!.args.indexOf("--") + 1)).toEqual([
        "com.example.app",
        path,
      ]);
      expect(JSON.stringify(installed)).not.toContain(path);
      const count = f.calls.length;
      expect((yield* execute("install-package", reference)).structuredContent).toEqual(
        installed.structuredContent,
      );
      expect(f.calls).toHaveLength(count);
      yield* f.fs.writeFile(path, new Uint8Array([1, 2, 3, 4]));
      expect((yield* execute("changed-package", reference)).isError).toBe(true);
      expect(f.calls).toHaveLength(count);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "discovery distinguishes typed commands, native owners and actual unsupported prerequisites without starting helpers",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const discovered = yield* f.call("device_commands", {});
      expect(discovered.isError, JSON.stringify(discovered)).not.toBe(true);
      expect(discovered.structuredContent).toMatchObject({
        protocol: 1,
        agentDeviceVersion: AGENT_DEVICE_VERSION,
        deviceHubVersion: DEVICE_HUB_VERSION,
        families: expect.arrayContaining([
          {
            name: "hover",
            interface: "structured",
            status: "unsupported-target",
            reason: expect.stringContaining("web"),
          },
          {
            name: "boot",
            interface: "structured",
            status: "native-owner",
            reason: expect.stringContaining("device_open"),
          },
          {
            name: "replay",
            interface: "structured",
            status: "prerequisite",
            reason: expect.stringContaining("validated"),
          },
          {
            name: "auth",
            interface: "cli-only",
            status: "prerequisite",
            reason: expect.stringContaining("authority"),
          },
        ]),
      });
      expect(f.preparations()).toBe(0);
      expect(f.calls).toHaveLength(0);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);

it.effect(
  "large valid trees remain usable as native artifacts instead of overflowing the durable receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { target } = yield* f.open("large-tree-open");
      f.largeSnapshot();
      const input = { target, command: { type: "snapshot" }, clientRequestId: "large-tree" };
      const first = yield* f.call("device_execute", input);
      expect(first.isError, JSON.stringify(first)).not.toBe(true);
      const result = yield* decodeCommandResult(first.structuredContent);
      expect(result.type).toBe("artifact");
      if (result.type !== "artifact") throw new Error("Expected bounded artifact response");
      expect(result.artifact.mimeType).toBe("application/json");
      expect(result.artifact.sizeBytes).toBeGreaterThan(64_000);
      const path = resolveAttachmentPath({
        attachmentsDir: f.config.attachmentsDir,
        attachment: {
          type: "file",
          id: result.artifact.attachmentId,
          name: result.artifact.name,
          mimeType: result.artifact.mimeType,
          sizeBytes: result.artifact.sizeBytes,
        },
      });
      if (path === null) throw new Error("Expected qualified native JSON path");
      const document = JSON.parse(yield* f.fs.readFileString(path));
      expect(document).toMatchObject({ type: "snapshot", target, truncated: false });
      expect(document.nodes).toHaveLength(200);
      expect(document.nodes[199]).toEqual({ index: 199, ref: "e199", label: "x".repeat(400) });
      const count = f.calls.length;
      expect((yield* f.call("device_execute", input)).structuredContent).toEqual(
        first.structuredContent,
      );
      expect(f.calls).toHaveLength(count);
      const batch = yield* f.call("device_execute", {
        target,
        command: {
          type: "batch",
          steps: [{ type: "snapshot" }, { type: "snapshot" }, { type: "snapshot" }],
        },
        clientRequestId: "large-batch",
      });
      const batchResult = yield* decodeCommandResult(batch.structuredContent);
      expect(batchResult.type).toBe("artifact");
      if (batchResult.type !== "artifact")
        throw new Error("Expected bounded batch artifact response");
      const batchPath = resolveAttachmentPath({
        attachmentsDir: f.config.attachmentsDir,
        attachment: {
          type: "file",
          id: batchResult.artifact.attachmentId,
          name: batchResult.artifact.name,
          mimeType: batchResult.artifact.mimeType,
          sizeBytes: batchResult.artifact.sizeBytes,
        },
      });
      if (batchPath === null) throw new Error("Expected qualified native batch JSON path");
      const sequence = JSON.parse(yield* f.fs.readFileString(batchPath));
      expect(sequence.type).toBe("batch");
      expect(sequence.results).toHaveLength(3);
      expect(
        sequence.results.every((result: { nodes: unknown[] }) => result.nodes.length === 200),
      ).toBe(true);
      const rows =
        yield* f.sql`SELECT result_json FROM mcp_request_receipts WHERE state='completed'`;
      expect(rows.every((row) => String(row.result_json).length <= 64_000)).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(Net.layer, NodeServices.layer))),
);
