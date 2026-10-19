import * as DesktopBackendManager from "../backend/DesktopBackendManager.ts";
import * as DesktopObservability from "../app/DesktopObservability.ts";
import * as DesktopBrowserHost from "./DesktopBrowserHost.ts";
import * as Option from "effect/Option";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { HttpClientResponse } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import { vi } from "vite-plus/test";
import * as DesktopApp from "../app/DesktopApp.ts";
import * as DesktopEnvironment from "../app/DesktopEnvironment.ts";
import * as DesktopConfig from "../app/DesktopConfig.ts";
import * as DesktopAppIdentity from "../app/DesktopAppIdentity.ts";
import * as DesktopAppActivation from "../app/DesktopAppActivation.ts";
import * as DesktopLifecycle from "../app/DesktopLifecycle.ts";
import * as DesktopLinuxUrlHandler from "../app/DesktopLinuxUrlHandler.ts";
import * as DesktopClerk from "../app/DesktopClerk.ts";
import * as DesktopState from "../app/DesktopState.ts";
import * as DesktopShutdown from "../app/DesktopShutdown.ts";
import * as DesktopLegacyLocalStorage from "../app/DesktopLegacyLocalStorage.ts";
import * as DesktopPreReadyPlatform from "../app/DesktopPreReadyPlatform.ts";
import * as DesktopConnectionCatalogStore from "../app/DesktopConnectionCatalogStore.ts";
import * as DesktopApplicationMenu from "../window/DesktopApplicationMenu.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";
import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as DesktopServerExposure from "../backend/DesktopServerExposure.ts";
import * as DesktopShellEnvironment from "../shell/DesktopShellEnvironment.ts";
import * as DesktopSshEnvironment from "../ssh/DesktopSshEnvironment.ts";
import * as DesktopWslBackend from "../wsl/DesktopWslBackend.ts";
import * as DesktopUpdates from "../updates/DesktopUpdates.ts";
import * as DesktopSnapShot from "../snapShot/DesktopSnapShot.ts";
import * as DesktopRendererHistory from "../telemetry/DesktopRendererHistory.ts";
import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronProtocol from "../electron/ElectronProtocol.ts";
import * as ElectronSafeStorage from "../electron/ElectronSafeStorage.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as ElectronDialog from "../electron/ElectronDialog.ts";
import * as DesktopAppSettings from "../settings/DesktopAppSettings.ts";
import * as DesktopClientSettings from "../settings/DesktopClientSettings.ts";
import * as BrowserSession from "./BrowserSession.ts";
import * as PreviewPasskeys from "./Passkeys.ts";
import * as DesktopCookieMirror from "./DesktopCookieMirror.ts";
import * as BrowserImport from "./BrowserImport/BrowserImport.ts";
import * as DesktopBackendConfiguration from "../backend/DesktopBackendConfiguration.ts";
import * as DesktopCliCommand from "../app/DesktopCliCommand.ts";
import * as DesktopIpc from "../ipc/DesktopIpc.ts";
import * as DesktopLocalEnvironmentAuth from "../backend/DesktopLocalEnvironmentAuth.ts";
import * as DesktopRenewalStore from "../app/DesktopRenewalStore.ts";
import * as DesktopSshPasswordPrompts from "../ssh/DesktopSshPasswordPrompts.ts";
import * as DesktopTelemetryPublisher from "../telemetry/DesktopTelemetryPublisher.ts";
import * as DesktopWebLinks from "../app/DesktopWebLinks.ts";
import * as DesktopWslEnvironment from "../wsl/DesktopWslEnvironment.ts";
import * as ElectronMenu from "../electron/ElectronMenu.ts";
import * as ElectronShell from "../electron/ElectronShell.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as MacPermissions from "../permissions/MacPermissions.ts";
import * as NetService from "@t3tools/shared/Net";
import * as PreviewManager from "./Manager.ts";

const { readKey } = vi.hoisted(() => ({ readKey: vi.fn() }));
vi.mock("@clerk/electron", () => ({ createClerkBridge: vi.fn() }));
vi.mock("@clerk/electron/storage", () => ({ storage: vi.fn() }));
vi.mock("electron-updater", () => ({ autoUpdater: {} }));
vi.mock("electron", () => ({ app: { on: vi.fn(), off: vi.fn(), whenReady: async () => {} } }));
vi.mock("./ChromeCookieKey.ts", async (original) => {
  const module = await original<typeof import("./ChromeCookieKey.ts")>();
  return { ...module, makeChromeCookieKeyCache: () => module.makeChromeCookieKeyCache(readKey) };
});
vi.mock("../ipc/DesktopIpcHandlers.ts", () => ({ installDesktopIpcHandlers: () => Effect.void }));
vi.mock("../updates/DesktopRemoteUpdates.ts", () => ({ listen: Effect.void }));

it.effect.each([
  { command: undefined, name: "default settings never read a Chrome key" },
  { command: [], name: "an empty or invalid key command leaves the source off" },
  {
    command: ["synthetic-key-command"],
    name: "an enabled source with an unresolved key still lets the app and backend report ready",
  },
])("$name", ({ command }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const home = yield* fs.makeTempDirectoryScoped({ prefix: "chromeria-541-startup-" });
    const chrome = `${home}/Library/Application Support/Google/Chrome/Default`;
    yield* fs.makeDirectory(chrome, { recursive: true });
    yield* fs.writeFileString(`${chrome}/Cookies`, "synthetic database; key must not resolve");
    const keyStarted = yield* Deferred.make<void>();
    readKey
      .mockReset()
      .mockImplementation(() =>
        Deferred.succeed(keyStarted, undefined).pipe(
          Effect.andThen(
            command?.length ? Effect.never : Effect.succeed({ cbcV10: Buffer.alloc(16) }),
          ),
        ),
      );
    const healthy = yield* Deferred.make<void>();
    const windowCreated = vi.fn();
    const backendExited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
    const backend = yield* DesktopBackendManager.makeBackendInstance({
      id: DesktopBackendManager.PRIMARY_INSTANCE_ID,
      label: Effect.succeed("Synthetic bundled server"),
      configResolve: Effect.succeed({
        executablePath: "/synthetic/electron",
        args: ["/synthetic/server/bin.mjs"],
        entryPath: "/synthetic/server/bin.mjs",
        cwd: home,
        env: {},
        extendEnv: true,
        bootstrap: {
          mode: "desktop",
          noBrowser: true,
          port: 3773,
          host: "127.0.0.1",
          t3Home: home,
          desktopBootstrapToken: "synthetic-token",
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
        },
        bootstrapDelivery: "fd3",
        httpBaseUrl: new URL("http://127.0.0.1:3773"),
        captureOutput: false,
        preflightFailure: Option.none(),
      }),
      onReady: () =>
        Effect.sync(windowCreated).pipe(
          Effect.andThen(Deferred.succeed(healthy, undefined)),
          Effect.asVoid,
        ),
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          FileSystem.layerNoop({ exists: () => Effect.succeed(true) }),
          Layer.succeed(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() =>
              Effect.acquireRelease(
                Effect.succeed(
                  ChildProcessSpawner.makeHandle({
                    pid: ChildProcessSpawner.ProcessId(123),
                    exitCode: Deferred.await(backendExited),
                    isRunning: Effect.succeed(true),
                    kill: () =>
                      Deferred.succeed(backendExited, ChildProcessSpawner.ExitCode(0)).pipe(
                        Effect.asVoid,
                      ),
                    stdin: Sink.drain,
                    stdout: Stream.empty,
                    stderr: Stream.empty,
                    all: Stream.empty,
                    unref: Effect.succeed(Effect.void),
                    getInputFd: () => Sink.drain,
                    getOutputFd: () => Stream.empty,
                  }),
                ),
                () =>
                  Deferred.succeed(backendExited, ChildProcessSpawner.ExitCode(0)).pipe(
                    Effect.asVoid,
                  ),
              ),
            ),
          ),
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.succeed(
                HttpClientResponse.fromWeb(request, new Response(null, { status: 200 })),
              ),
            ),
          ),
          Layer.succeed(DesktopObservability.DesktopBackendOutputLogFactory, {
            forInstance: () =>
              Effect.succeed({
                beginSession: () => Effect.void,
                writeOutputChunk: () => Effect.void,
                persistFailureSnapshot: () => Effect.void,
                persistFailure: () => Effect.void,
                discardSession: Effect.void,
              }),
          }),
          Layer.mock(DesktopTelemetryPublisher.DesktopTelemetryPublisher, {
            encoded: Stream.empty,
            handleControlForSource: () => Effect.void,
            removeControlSource: () => Effect.void,
          }),
          DesktopBrowserHost.layer,
          DesktopWslEnvironment.layerTest({}),
        ),
      ),
    );

    const desktopSettings = {
      ...DesktopAppSettings.DEFAULT_DESKTOP_SETTINGS,
      ...(command ? { chromeCookieKeyCommand: command } : {}),
    };
    const environment = DesktopEnvironment.layer({
      dirname: `${home}/app/dist`,
      homeDirectory: home,
      platform: "darwin",
      processArch: "arm64",
      appVersion: "0.1.0",
      appPath: home,
      isPackaged: true,
      resourcesPath: home,
      runningUnderArm64Translation: false,
    }).pipe(Layer.provide(DesktopConfig.layerTest({ T3CODE_HOME: home, T3CODE_PORT: "3773" })));
    const dependencies = Layer.mergeAll(
      environment,
      Layer.mock(PreviewManager.PreviewManager, { isBrowserPartition: () => false }),
      Layer.mock(BrowserImport.BrowserImport, {}),
      Layer.mock(DesktopBackendConfiguration.DesktopBackendConfiguration, {}),
      Layer.mock(DesktopCliCommand.DesktopCliCommand, {}),
      Layer.mock(DesktopIpc.DesktopIpc, {}),
      Layer.mock(DesktopLocalEnvironmentAuth.DesktopLocalEnvironmentAuth, {}),
      Layer.mock(DesktopRenewalStore.DesktopRenewalStore, {}),
      Layer.mock(DesktopSshPasswordPrompts.DesktopSshPasswordPrompts, {}),
      Layer.mock(DesktopTelemetryPublisher.DesktopTelemetryPublisher, {}),
      Layer.mock(DesktopWebLinks.DesktopWebLinks, {}),
      Layer.mock(DesktopWslEnvironment.DesktopWslEnvironment, {}),
      Layer.mock(ElectronMenu.ElectronMenu, {}),
      Layer.mock(ElectronShell.ElectronShell, {}),
      Layer.mock(ElectronTheme.ElectronTheme, {}),
      Layer.mock(MacPermissions.MacPermissions, {}),
      Layer.mock(NetService.NetService, {}),
      Layer.succeed(HostProcess.Platform, "darwin"),
      Layer.succeed(HostProcess.Environment, { HOME: home }),
      DesktopState.layer,
      DesktopShutdown.layer,
      Layer.mock(DesktopAppSettings.DesktopAppSettings, {
        load: Effect.succeed(desktopSettings),
        get: Effect.succeed(desktopSettings),
      }),
      Layer.mock(DesktopClientSettings.DesktopClientSettings, { get: Effect.succeedNone }),
      Layer.mock(BrowserSession.BrowserSession, { isPartition: () => true }),
      Layer.mock(DesktopConnectionCatalogStore.DesktopConnectionCatalogStore, {
        get: Effect.succeedNone,
      }),
      Layer.mock(DesktopBackendPool.DesktopBackendPool, {
        list: Effect.succeed([backend]),
        primary: Effect.succeed(backend),
      }),
      Layer.mock(DesktopSshEnvironment.DesktopSshEnvironment, {}),
      Layer.mock(ElectronWindow.ElectronWindow, { main: Effect.succeedNone }),
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("Unexpected authentication")),
      ),
      Layer.mock(DesktopAppIdentity.DesktopAppIdentity, {
        resolveUserDataPath: Effect.succeed(home),
        configure: Effect.void,
      }),
      Layer.mock(DesktopAppActivation.DesktopAppActivation, { start: Effect.void }),
      Layer.mock(DesktopLifecycle.DesktopLifecycle, { register: Effect.void }),
      Layer.mock(DesktopLinuxUrlHandler.DesktopLinuxUrlHandler, { register: Effect.void }),
      Layer.mock(DesktopClerk.DesktopClerk, { configure: Effect.void }),
      Layer.mock(DesktopShellEnvironment.DesktopShellEnvironment, {
        installIntoProcess: Effect.succeed(true),
      }),
      Layer.mock(ElectronApp.ElectronApp, {
        setPath: () => Effect.void,
        whenReady: Effect.void,
      }),
      Layer.mock(ElectronSafeStorage.ElectronSafeStorage, {}),
      Layer.mock(ElectronDialog.ElectronDialog, {}),
      Layer.mock(DesktopUpdates.DesktopUpdates, { configure: Effect.void }),
      Layer.mock(PreviewPasskeys.PreviewPasskeys, {
        configure: Effect.void,
        bridgeEnabled: false,
        installSessionHandlers: () => {},
        attachGuest: () => () => {},
      }),
      Layer.mock(DesktopApplicationMenu.DesktopApplicationMenu, { configure: Effect.void }),
      Layer.mock(DesktopWindow.DesktopWindow, {}),
      Layer.mock(ElectronProtocol.ElectronProtocol, {
        registerDesktopProtocol: () => Effect.void,
      }),
      Layer.mock(DesktopSnapShot.DesktopSnapShot, { initialize: Effect.void }),
      Layer.mock(DesktopLegacyLocalStorage.DesktopLegacyLocalStorage, {
        load: () => Effect.void,
      }),
      Layer.mock(DesktopServerExposure.DesktopServerExposure, {
        configureFromSettings: () =>
          Effect.succeed({
            mode: "local-only",
            endpointUrl: null,
            advertisedHost: null,
            tailscaleServeEnabled: false,
            tailscaleServePort: 443,
          }),
        backendConfig: Effect.succeed({
          httpBaseUrl: new URL("http://127.0.0.1:3773"),
          port: 3773,
          bindHost: "127.0.0.1",
          tailscaleServeEnabled: false,
          tailscaleServePort: 443,
        }),
      }),
      Layer.mock(DesktopWslBackend.DesktopWslBackend, { reconcile: Effect.void }),
      Layer.mock(DesktopRendererHistory.DesktopRendererHistory, { shutdown: Effect.void }),
      Layer.succeed(DesktopPreReadyPlatform.DesktopPreReadyElectronOptions, {
        linux: null,
        linuxPasswordStoreCommandLine: null,
      }),
    );
    yield* Effect.gen(function* () {
      const mirror = yield* DesktopCookieMirror.DesktopCookieMirror;
      // The enabled scan reaches the unresolved key before app bootstrap starts.
      if (command?.length) yield* Deferred.await(keyStarted);
      const app = yield* DesktopApp.program.pipe(Effect.forkScoped);
      yield* Deferred.await(healthy);
      expect(windowCreated).toHaveBeenCalledOnce();
      expect((yield* backend.snapshot).ready).toBe(true);
      if (command?.length) expect(readKey).toHaveBeenCalledOnce();
      else {
        // Let the initial ready callback acquire the scan lock, then wait for
        // that scan to finish before asserting the absence of a key read.
        yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
        yield* mirror.connection({ action: "status" });
        expect(readKey).not.toHaveBeenCalled();
      }
      yield* Fiber.interrupt(app);
    }).pipe(Effect.provide(DesktopCookieMirror.layer.pipe(Layer.provideMerge(dependencies))));
  }).pipe(
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provide(NodeServices.layer),
    Effect.scoped,
  ),
);
