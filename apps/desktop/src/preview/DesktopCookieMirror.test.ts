import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import { vi } from "vite-plus/test";

vi.mock("electron", () => ({
  app: { on: vi.fn(), off: vi.fn(), whenReady: () => new Promise(() => {}) },
}));
vi.mock("./CookieMirrorGrant.ts", () => ({
  requestCookieMirrorGrant: async () => "wss://synthetic.test/mirror",
}));
vi.mock("./ElectronCookieMirror.ts", () => ({
  electronCookieMirrorStore: () => ({
    read: async () => [],
    apply: async () => {},
    close: () => {},
  }),
}));
vi.mock("./CookieMirrorSocket.ts", () => ({
  CookieMirrorSocket: class {
    connected = true;
    flush() {}
    close() {
      this.connected = false;
    }
  },
}));

import * as DesktopEnvironment from "../app/DesktopEnvironment.ts";
import * as DesktopConfig from "../app/DesktopConfig.ts";
import * as DesktopConnectionCatalogStore from "../app/DesktopConnectionCatalogStore.ts";
import * as DesktopBackendPool from "../backend/DesktopBackendPool.ts";
import * as DesktopSshEnvironment from "../ssh/DesktopSshEnvironment.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as DesktopAppSettings from "../settings/DesktopAppSettings.ts";
import * as DesktopClientSettings from "../settings/DesktopClientSettings.ts";
import * as BrowserSession from "./BrowserSession.ts";
import * as DesktopCookieMirror from "./DesktopCookieMirror.ts";

it.effect(
  "reports the new connection and keeps a replacement alive when an old generation disconnects",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "chromeria-mirror-main-test-" });
      const dependencies = Layer.mergeAll(
        DesktopEnvironment.layer({
          dirname: `${stateDir}/app/dist`,
          homeDirectory: stateDir,
          platform: "linux",
          processArch: "x64",
          appVersion: "0.1.0",
          appPath: stateDir,
          isPackaged: true,
          resourcesPath: stateDir,
          runningUnderArm64Translation: false,
        }).pipe(Layer.provide(DesktopConfig.layerTest({ T3CODE_HOME: stateDir }))),
        Layer.mock(DesktopAppSettings.DesktopAppSettings, {
          load: Effect.succeed(DesktopAppSettings.DEFAULT_DESKTOP_SETTINGS),
        }),
        Layer.mock(DesktopClientSettings.DesktopClientSettings, { get: Effect.succeedNone }),
        Layer.mock(BrowserSession.BrowserSession, {
          isPartition: () => true,
          getPartition: () => Effect.succeed("synthetic-partition"),
          getSession: () =>
            Effect.succeed({
              cookies: { on: vi.fn(), off: vi.fn() },
            } as unknown as Electron.Session),
        }),
        Layer.mock(DesktopConnectionCatalogStore.DesktopConnectionCatalogStore, {
          get: Effect.succeedNone,
        }),
        Layer.mock(DesktopBackendPool.DesktopBackendPool, { list: Effect.succeed([]) }),
        Layer.mock(DesktopSshEnvironment.DesktopSshEnvironment, {}),
        Layer.mock(ElectronWindow.ElectronWindow, { main: Effect.succeedNone }),
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make(() => Effect.die("Unexpected authentication")),
        ),
      );
      yield* Effect.gen(function* () {
        const mirror = yield* DesktopCookieMirror.DesktopCookieMirror;
        const environmentId = EnvironmentId.make("synthetic-environment");
        const first = yield* mirror.connection({ action: "connect", environmentId });
        expect(first.connected).toBe(true);
        const replacement = yield* mirror.connection({ action: "connect", environmentId });
        expect(replacement.connected).toBe(true);
        expect(replacement.generation).toBeGreaterThan(first.generation);
        expect(
          (yield* mirror.connection({
            action: "disconnect",
            environmentId,
            generation: first.generation,
          })).connected,
        ).toBe(true);
        expect((yield* mirror.connection({ action: "status", environmentId })).connected).toBe(
          true,
        );
        expect(
          (yield* mirror.connection({
            action: "disconnect",
            environmentId,
            generation: replacement.generation,
          })).connected,
        ).toBe(false);
      }).pipe(Effect.provide(DesktopCookieMirror.layer.pipe(Layer.provide(dependencies))));
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);
