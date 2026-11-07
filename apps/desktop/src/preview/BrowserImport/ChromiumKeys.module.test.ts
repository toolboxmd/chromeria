// @effect-diagnostics nodeBuiltinImport:off - Models a native module-load failure on the worker boundary.
import * as NodeEvents from "node:events";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

vi.mock("node:worker_threads", () => ({
  Worker: class extends NodeEvents.EventEmitter {
    constructor() {
      super();
      queueMicrotask(() =>
        this.emit("error", new Error("Cannot find native binding: synthetic-sentinel")),
      );
    }
    unref() {}
    async terminate() {
      return 0;
    }
  },
}));

it.effect("reports an unavailable keychain when the macOS binding cannot load", () =>
  Effect.gen(function* () {
    const { ChromiumKeyError, resolveChromiumKeys } = yield* Effect.promise(
      () => import("./ChromiumKeys.ts"),
    );
    const error = yield* resolveChromiumKeys({
      platform: "darwin",
      keychainService: "Chrome Safe Storage",
      keychainAccount: "Chrome",
      linuxSecretApplication: undefined,
    }).pipe(Effect.flip);

    expect(error).toBeInstanceOf(ChromiumKeyError);
    expect(error.reason).toBe("keychainUnavailable");
    expect(error.cause).toBeUndefined();
    expect(JSON.stringify(error)).not.toContain("synthetic-sentinel");
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("must not spawn")),
    ),
  ),
);
