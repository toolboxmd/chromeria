import * as NodeServices from "@effect/platform-node/NodeServices";
import { type AuthRenewalLocalRecord } from "@t3tools/contracts";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as DesktopConfig from "./DesktopConfig.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as ElectronSafeStorage from "../electron/ElectronSafeStorage.ts";
import * as Store from "./DesktopRenewalStore.ts";

vi.mock("electron", () => ({}));
const secret = "a".repeat(43);

it.effect("serializes competing IPC writes across async encryption and exposes hashes only", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-renewal-storage-" });
    const environment = DesktopEnvironment.layer({
      dirname: "/repo/apps/desktop/src",
      homeDirectory: baseDir,
      platform: "darwin",
      processArch: "arm64",
      appVersion: "1.2.3",
      appPath: "/repo",
      isPackaged: true,
      resourcesPath: "/missing/resources",
      runningUnderArm64Translation: false,
    }).pipe(
      Layer.provide(
        Layer.mergeAll(NodeServices.layer, DesktopConfig.layerTest({ T3CODE_HOME: baseDir })),
      ),
    );
    const safe = Layer.succeed(ElectronSafeStorage.ElectronSafeStorage, {
      isEncryptionAvailable: Effect.succeed(true),
      selectedStorageBackend: Effect.succeedNone,
      encryptString: (value) => Effect.yieldNow.pipe(Effect.as(new TextEncoder().encode(value))),
      decryptString: (value) => Effect.yieldNow.pipe(Effect.as(new TextDecoder().decode(value))),
    });
    const dependencies = Layer.mergeAll(environment, safe, NodeServices.layer);
    yield* Effect.gen(function* () {
      const store = yield* Store.DesktopRenewalStore;
      const env = yield* DesktopEnvironment.DesktopEnvironment;
      const value: AuthRenewalLocalRecord = {
        revision: 1,
        active: null,
        provisional: [
          {
            epoch: "one",
            environmentId: "rocky",
            origin: "http://rocky:3773",
            kind: "bearer",
            current: secret,
            generation: 0,
            pending: null,
            accessToken: "access-secret",
            expiresAt: null,
            renewAfter: null,
          },
        ],
      };
      const results = yield* Effect.all(
        [
          store.compareAndSet({ key: "rocky", revision: null, value }),
          store.compareAndSet({ key: "rocky", revision: null, value: { ...value, revision: 99 } }),
        ],
        { concurrency: "unbounded" },
      );
      expect(results.filter(Boolean)).toHaveLength(1);
      const persisted = yield* store.read("rocky");
      expect(persisted?.revision).toBe(results[0] ? 1 : 99);
      const diagnostic = yield* fs.readFileString(
        env.path.join(env.stateDir, "renewal-commitments.json"),
      );
      expect(diagnostic).toContain(renewalCommitment(secret));
      expect(diagnostic).not.toContain(secret);
      expect(diagnostic).not.toContain("access-secret");
      expect(yield* store.compareAndSet({ key: "rocky", revision: null, value })).toBe(false);
    }).pipe(Effect.provide(Store.layer.pipe(Layer.provideMerge(dependencies))));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
