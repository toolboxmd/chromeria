// @effect-diagnostics nodeBuiltinImport:off globalFetchInEffect:off - The handoff test uses a real localhost listener without an OpenAI account.
import * as NodeHttp from "node:http";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, ProviderInstanceId } from "@t3tools/contracts";
import { codexAuthHandoffUrl, readCodexAuthDelivery } from "@t3tools/shared/codexAuthHandoff";
import { HostProcessArguments } from "@t3tools/shared/hostProcess";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { beforeEach, vi } from "vite-plus/test";

// Every module under test reads the embedded identity; this file runs them as a V2 build.
vi.mock("../../../../scripts/lib/chromeria-desktop-identity.ts", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../../../scripts/lib/chromeria-desktop-identity.ts")>();
  return { ...actual, CHROMERIA_DESKTOP_IDENTITY: actual.CHROMERIA_DESKTOP_IDENTITIES.v2 };
});

const { createClerkBridgeMock, registerSchemesAsPrivilegedMock, requestSingleInstanceLockMock } =
  vi.hoisted(() => ({
    createClerkBridgeMock: vi.fn(),
    registerSchemesAsPrivilegedMock: vi.fn(),
    requestSingleInstanceLockMock: vi.fn(),
  }));
vi.mock("@clerk/electron", () => ({ createClerkBridge: createClerkBridgeMock }));
vi.mock("@clerk/electron/storage", () => ({ storage: () => ({}) }));
vi.mock("electron", () => ({
  app: { requestSingleInstanceLock: requestSingleInstanceLockMock },
  protocol: { registerSchemesAsPrivileged: registerSchemesAsPrivilegedMock },
}));

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronProtocol from "../electron/ElectronProtocol.ts";
import * as ElectronShell from "../electron/ElectronShell.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as DesktopClerk from "./DesktopClerk.ts";
import * as DesktopConfig from "./DesktopConfig.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopLegacyLocalStorage from "./DesktopLegacyLocalStorage.ts";
import * as DesktopUserData from "./DesktopUserData.ts";

const PROFILE = "/Users/alice/Library/Application Support/chromeria-v2";

const layerEnvironment = (env: Record<string, string | undefined> = {}, appVersion = "0.0.45") =>
  DesktopEnvironment.layer({
    dirname:
      "/Applications/Chromeria V2.app/Contents/Resources/app.asar/apps/desktop/dist-electron",
    homeDirectory: "/Users/alice",
    platform: "darwin",
    processArch: "arm64",
    appVersion,
    appPath: "/Applications/Chromeria V2.app/Contents/Resources/app.asar",
    isPackaged: true,
    resourcesPath: "/Applications/Chromeria V2.app/Contents/Resources",
    runningUnderArm64Translation: false,
  }).pipe(
    Layer.provide(
      Layer.mergeAll(NodeServices.layer, NodePath.layerPosix, DesktopConfig.layerTest(env)),
    ),
  );

/** A filesystem that records every call, so tests can prove nothing was read or written. */
const recordingFileSystem = (calls: string[]) =>
  FileSystem.layerNoop({
    exists: (path) => Effect.sync(() => (calls.push(`exists:${path}`), false)),
    readDirectory: (path) => Effect.sync(() => (calls.push(`readDirectory:${path}`), [])),
    readFileString: (path) => Effect.sync(() => (calls.push(`readFileString:${path}`), "")),
    makeDirectory: (path) => Effect.sync(() => void calls.push(`makeDirectory:${path}`)),
    writeFileString: (path) => Effect.sync(() => void calls.push(`writeFileString:${path}`)),
  });

const layerClerk = (
  electronApp: Partial<ElectronApp.ElectronApp["Service"]>,
  shell: Partial<ElectronShell.ElectronShell["Service"]> = {},
) =>
  DesktopClerk.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodePath.layerPosix,
        layerEnvironment(),
        Layer.succeed(ElectronApp.ElectronApp, electronApp as ElectronApp.ElectronApp["Service"]),
        Layer.succeed(ElectronShell.ElectronShell, shell as ElectronShell.ElectronShell["Service"]),
        recordingFileSystem([]),
      ),
    ),
  );

describe("Chromeria V2 desktop identity", () => {
  beforeEach(() => {
    createClerkBridgeMock.mockReset();
    registerSchemesAsPrivilegedMock.mockReset();
    requestSingleInstanceLockMock.mockReset().mockReturnValue(true);
  });

  it.effect("defaults to ~/.t3-v2 with a fixed name, and an explicit T3CODE_HOME wins", () =>
    Effect.gen(function* () {
      const defaults = yield* DesktopEnvironment.DesktopEnvironment.pipe(
        Effect.provide(layerEnvironment({}, "0.0.45-nightly.20261009.1")),
      );
      assert.equal(defaults.baseDir, "/Users/alice/.t3-v2");
      assert.equal(defaults.stateDir, "/Users/alice/.t3-v2/userdata");
      assert.equal(defaults.displayName, "Chromeria V2");

      const explicit = yield* DesktopEnvironment.DesktopEnvironment.pipe(
        Effect.provide(layerEnvironment({ T3CODE_HOME: "/Volumes/work/t3" })),
      );
      assert.equal(explicit.baseDir, "/Volumes/work/t3");
    }),
  );

  it.effect("registers only the chromeria-v2 scheme, in every mode", () =>
    Effect.gen(function* () {
      yield* Layer.build(ElectronProtocol.layerSchemePrivileges);

      const registered = registerSchemesAsPrivilegedMock.mock.calls.flatMap(([schemes]) =>
        (schemes as ReadonlyArray<{ readonly scheme: string }>).map((entry) => entry.scheme),
      );
      assert.deepStrictEqual(registered, ["chromeria-v2"]);
      assert.equal(ElectronProtocol.getDesktopUrl(false), "chromeria-v2://app/");
      assert.equal(ElectronProtocol.getDesktopUrl(true), "chromeria-v2://app/");
    }).pipe(Effect.scoped),
  );

  it.effect.each([
    { platform: "win32" as const, isDevelopment: false },
    { platform: "darwin" as const, isDevelopment: true },
  ])(
    "uses its own profile without migrating another ($platform, development: $isDevelopment)",
    ({ platform, isDevelopment }) =>
      Effect.gen(function* () {
        const calls: string[] = [];
        const userData = yield* DesktopUserData.resolveUserDataPath({
          appDataDirectory: "/app-data",
          isDevelopment,
          platform,
        }).pipe(Effect.provide(Layer.mergeAll(NodePath.layerPosix, recordingFileSystem(calls))));

        assert.equal(userData, "/app-data/chromeria-v2");
        assert.deepStrictEqual(calls, []);
      }),
  );

  it.effect("never reads a V1 profile's Local Storage", () =>
    Effect.gen(function* () {
      const calls: string[] = [];
      yield* Effect.gen(function* () {
        const legacy = yield* DesktopLegacyLocalStorage.DesktopLegacyLocalStorage;
        yield* legacy.load(PROFILE);
        assert.isTrue(Option.isNone(yield* legacy.take));
      }).pipe(
        Effect.provide(
          DesktopLegacyLocalStorage.layer.pipe(
            Layer.provide(
              Layer.mergeAll(layerEnvironment(), NodePath.layerPosix, recordingFileSystem(calls)),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(calls, []);
    }),
  );

  it.effect("names itself and locks its profile before the Clerk bridge exists", () =>
    Effect.gen(function* () {
      const events: string[] = [];
      createClerkBridgeMock.mockImplementation((options) => {
        events.push(`createClerkBridge:${options.renderer.scheme}://${options.renderer.host}`);
        return { cleanup: vi.fn(), isPrimaryInstance: true };
      });
      requestSingleInstanceLockMock.mockImplementation(() => (events.push("lock"), true));
      const electronApp = {
        setName: (name: string) => Effect.sync(() => void events.push(`setName:${name}`)),
        setPath: (name: string, value: string) =>
          Effect.sync(() => void events.push(`setPath:${name}:${value}`)),
      } as unknown as ElectronApp.ElectronApp["Service"];

      yield* Layer.build(layerClerk(electronApp));

      assert.deepStrictEqual(events, [
        "setName:Chromeria V2",
        `setPath:userData:${PROFILE}`,
        "lock",
        "createClerkBridge:chromeria-v2://app",
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("a second V2 instance quits before creating the bridge", () =>
    Effect.gen(function* () {
      const quit = vi.fn();
      requestSingleInstanceLockMock.mockReturnValue(false);
      const electronApp = {
        setName: () => Effect.void,
        setPath: () => Effect.void,
        quit: Effect.sync(quit),
      } as unknown as ElectronApp.ElectronApp["Service"];

      const exit = yield* Effect.exit(Layer.build(layerClerk(electronApp)));

      assert.isTrue(Exit.hasInterrupts(exit));
      assert.equal(quit.mock.calls.length, 1);
      assert.equal(createClerkBridgeMock.mock.calls.length, 0);
    }).pipe(Effect.scoped),
  );

  it.effect("receives ChatGPT sign-in only through chromeria-v2://auth/codex", () =>
    Effect.gen(function* () {
      createClerkBridgeMock.mockReturnValue({ cleanup: vi.fn(), isPrimaryInstance: true });
      const port = yield* Effect.promise(async () => {
        const server = NodeHttp.createServer();
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        if (!address || typeof address === "string") throw new Error("address");
        await new Promise<void>((resolve) => server.close(() => resolve()));
        return address.port;
      });
      const authorize = new URL("https://auth.openai.com/api/accounts/authorize");
      authorize.search = new URLSearchParams({
        client_id: "dynamic_agent_client",
        response_type: "code",
        redirect_uri: `http://127.0.0.1:${port}/auth/callback`,
        state: "a".repeat(43),
        code_challenge_method: "S256",
        code_challenge: "b".repeat(43),
      }).toString();
      const request = {
        authorizationUrl: authorize.toString(),
        returnUrl: "https://app.t3.codes/welcome#agents:remote-one",
        environmentId: EnvironmentId.make("remote-one"),
        instanceId: ProviderInstanceId.make("work"),
        flowId: "flow-one",
      };
      const link = codexAuthHandoffUrl(request, false, "chromeria-v2");
      assert.isTrue(link.startsWith("chromeria-v2://auth/codex?request="));

      const opened: string[] = [];
      const delivered = Promise.withResolvers<string>();
      const shell = {
        openExternal: (value: unknown) =>
          Effect.promise(async () => {
            opened.push(String(value));
            const url = new URL(String(value));
            const callback = new URL(url.searchParams.get("redirect_uri")!);
            callback.search = new URLSearchParams({
              state: url.searchParams.get("state")!,
              code: "test-code",
            }).toString();
            const response = await fetch(callback, { redirect: "manual" });
            delivered.resolve(response.headers.get("location")!);
            return true;
          }),
      };
      const listeners = new Map<string, (...args: unknown[]) => void>();
      const electronApp = {
        setName: () => Effect.void,
        setPath: () => Effect.void,
        whenReady: Effect.void,
        on: (name: string, listener: (...args: unknown[]) => void) =>
          Effect.sync(() => void listeners.set(name, listener)),
      } as unknown as ElectronApp.ElectronApp["Service"];

      yield* Effect.gen(function* () {
        const clerk = yield* DesktopClerk.DesktopClerk;
        yield* clerk.configure;
        const openUrl = (url: string) => {
          const event = { preventDefault: vi.fn() };
          listeners.get("open-url")!(event, url);
          return event.preventDefault.mock.calls.length === 1;
        };

        // Handoffs addressed to the default app's schemes are not V2's to receive.
        assert.isFalse(openUrl(codexAuthHandoffUrl(request)));
        assert.isFalse(openUrl(codexAuthHandoffUrl(request, true)));
        assert.deepStrictEqual(opened, []);

        assert.isTrue(openUrl(link));
        const delivery = readCodexAuthDelivery(yield* Effect.promise(() => delivered.promise));
        assert.equal(delivery?.flowId, request.flowId);
        assert.equal(delivery?.returnUrl, request.returnUrl);
        assert.deepStrictEqual(opened, [request.authorizationUrl]);
      }).pipe(
        Effect.provide(layerClerk(electronApp, shell)),
        Effect.provideService(HostProcessArguments, ["Chromeria V2"]),
        Effect.provideService(ElectronApp.ElectronApp, electronApp),
        Effect.provideService(
          ElectronWindow.ElectronWindow,
          {} as ElectronWindow.ElectronWindow["Service"],
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("returns to provider setup only through chromeria-v2://app", () =>
    Effect.gen(function* () {
      createClerkBridgeMock.mockReturnValue({ cleanup: vi.fn(), isPrimaryInstance: true });
      const listeners = new Map<string, (...args: unknown[]) => void>();
      const revealed = Promise.withResolvers<void>();
      const loadURL = vi.fn(async (_url: string) => undefined);
      const electronApp = {
        setName: () => Effect.void,
        setPath: () => Effect.void,
        on: (name: string, listener: (...args: unknown[]) => void) =>
          Effect.sync(() => void listeners.set(name, listener)),
      } as unknown as ElectronApp.ElectronApp["Service"];
      const electronWindow = {
        currentMainOrFirst: Effect.succeedSome({ loadURL }),
        reveal: () => Effect.sync(() => revealed.resolve()),
      } as unknown as ElectronWindow.ElectronWindow["Service"];

      yield* Effect.gen(function* () {
        const clerk = yield* DesktopClerk.DesktopClerk;
        yield* clerk.configure;
        const event = { preventDefault: vi.fn() };
        listeners.get("open-url")!(event, "t3code://app/settings/providers?instanceId=work");
        listeners.get("open-url")!(event, "chromeria-v2://attacker/settings/providers");
        assert.equal(event.preventDefault.mock.calls.length, 0);

        listeners.get("open-url")!(
          event,
          "chromeria-v2://app/settings/providers?instanceId=work&code=never-forward",
        );
        assert.equal(event.preventDefault.mock.calls.length, 1);
        yield* Effect.promise(() => revealed.promise);
        assert.deepStrictEqual(loadURL.mock.calls, [
          ["chromeria-v2://app/settings/providers?instanceId=work"],
        ]);
      }).pipe(
        Effect.provide(layerClerk(electronApp)),
        Effect.provideService(HostProcessArguments, ["Chromeria V2"]),
        Effect.provideService(ElectronApp.ElectronApp, electronApp),
        Effect.provideService(ElectronWindow.ElectronWindow, electronWindow),
      );
    }).pipe(Effect.scoped),
  );
});
