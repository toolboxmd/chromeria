// @effect-diagnostics nodeBuiltinImport:off - Models the worker boundary without reading any real Keychain.
import * as NodeEvents from "node:events";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { vi } from "vite-plus/test";

const { workers, started } = vi.hoisted(() => ({
  workers: [] as Array<
    NodeEvents.EventEmitter & {
      terminate: ReturnType<typeof vi.fn>;
      unref: ReturnType<typeof vi.fn>;
    }
  >,
  started: { current: () => {} },
}));
vi.mock("node:worker_threads", () => ({
  Worker: class extends NodeEvents.EventEmitter {
    terminate = vi.fn(async () => 0);
    unref = vi.fn();
    constructor(_path: string, _options: unknown) {
      super();
      workers.push(this);
      queueMicrotask(() => started.current());
    }
  },
}));
vi.mock("@napi-rs/keyring", () => ({
  Entry: vi.fn(() => {
    throw new Error("Keychain must never load on main");
  }),
}));

import { resolveChromiumKeys } from "./ChromiumKeys.ts";

const read = resolveChromiumKeys({
  platform: "darwin",
  keychainService: "Synthetic Safe Storage",
  keychainAccount: "Synthetic",
  linuxSecretApplication: undefined,
}).pipe(Effect.provide(NodeServices.layer));

it.effect(
  "a pending native prompt yields main and is cancelled without waiting for the worker",
  () =>
    Effect.gen(function* () {
      const began = yield* Deferred.make<void>();
      started.current = () => {
        Deferred.doneUnsafe(began, Effect.void);
      };
      const fiber = yield* read.pipe(Effect.forkScoped);
      yield* Deferred.await(began);
      const worker = workers.at(-1)!;
      expect(worker.unref).toHaveBeenCalledOnce();
      yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
      yield* Fiber.interrupt(fiber);
      expect(worker.terminate).toHaveBeenCalledOnce();
    }).pipe(Effect.scoped),
);

it.effect("one-off imports derive the same key from the worker reply", () =>
  Effect.gen(function* () {
    const began = yield* Deferred.make<void>();
    started.current = () => {
      Deferred.doneUnsafe(began, Effect.void);
    };
    const fiber = yield* read.pipe(Effect.forkScoped);
    yield* Deferred.await(began);
    workers.at(-1)!.emit("message", { secret: "synthetic-secret" });
    expect((yield* Fiber.join(fiber)).cbcV10?.toString("hex")).toBe(
      "783696d70baa14a426ca8e202f73152f",
    );
  }).pipe(Effect.scoped),
);

it.effect("worker failures remain typed and never leak native payloads", () =>
  Effect.gen(function* () {
    const began = yield* Deferred.make<void>();
    started.current = () => {
      Deferred.doneUnsafe(began, Effect.void);
    };
    const fiber = yield* read.pipe(Effect.result, Effect.forkScoped);
    yield* Deferred.await(began);
    workers.at(-1)!.emit("error", new Error("synthetic-secret-sentinel"));
    const result = yield* Fiber.join(fiber);
    expect(result._tag).toBe("Failure");
    expect(JSON.stringify(result)).not.toContain("synthetic-secret-sentinel");
  }).pipe(Effect.scoped),
);

it.effect("routes the import through a worker before waiting for a native result", () =>
  Effect.gen(function* () {
    const count = workers.length;
    started.current = () => {};
    yield* read.pipe(Effect.forkScoped);
    yield* Effect.yieldNow;
    expect(workers.length).toBe(count + 1);
  }).pipe(Effect.scoped),
);
