import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";

import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import * as OpenCodeServerOwner from "./OpenCodeServerOwner.ts";

const unusedRuntimeMethod = () =>
  Effect.fail(
    new OpenCodeRuntime.OpenCodeRuntimeError({
      operation: "unused",
      detail: "unused test method",
    }),
  );

const makeRuntime = Effect.gen(function* () {
  const starts = yield* Ref.make(0);
  const closes = yield* Ref.make(0);
  const failNextStart = yield* Ref.make(false);
  const started = yield* Deferred.make<void>();
  const closed = yield* Deferred.make<void>();
  const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
    startOpenCodeServerProcess: () =>
      Effect.gen(function* () {
        if (yield* Ref.getAndSet(failNextStart, false)) {
          return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "startOpenCodeServerProcess",
            detail: "start failed",
          });
        }
        const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
        yield* Deferred.succeed(started, undefined).pipe(Effect.ignore);
        yield* Effect.addFinalizer(() =>
          Ref.update(closes, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(closed, undefined)),
            Effect.ignore,
          ),
        );
        return {
          url: `http://127.0.0.1:${index}`,
          version: "1.14.19",
          isRunning: Effect.succeed(true),
          exitCode: Effect.never,
        };
      }),
    connectToOpenCodeServer: unusedRuntimeMethod,
    runOpenCodeCommand: unusedRuntimeMethod,
    createOpenCodeSdkClient: () => ({}) as never,
    loadOpenCodeInventory: unusedRuntimeMethod,
    loadOpenCodeSkills: unusedRuntimeMethod,
  };
  return { runtime, starts, closes, failNextStart, started, closed };
});

it.effect("shares concurrent borrowers and closes after the idle TTL", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    const release = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        const useServer = owner.withServer((server) =>
          Deferred.await(release).pipe(Effect.as(server.url)),
        );
        const fibers = yield* Effect.all([useServer, useServer], {
          concurrency: "unbounded",
        }).pipe(Effect.forkChild);
        yield* Deferred.await(testRuntime.started);
        expect(yield* Ref.get(testRuntime.starts)).toBe(1);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(fibers)).toEqual(["http://127.0.0.1:1", "http://127.0.0.1:1"]);
        yield* TestClock.adjust(Duration.seconds(31));
        yield* Deferred.await(testRuntime.closed);
        expect(yield* Ref.get(testRuntime.closes)).toBe(1);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, testRuntime.runtime));
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("retries a failed start and closes on owner scope shutdown", () =>
  Effect.gen(function* () {
    const testRuntime = yield* makeRuntime;
    yield* Ref.set(testRuntime.failNextStart, true);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        expect(
          (yield* Effect.exit(owner.withServer((server) => Effect.succeed(server.url))))._tag,
        ).toBe("Failure");
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:1",
        );
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, testRuntime.runtime));
    expect(yield* Ref.get(testRuntime.starts)).toBe(1);
    expect(yield* Ref.get(testRuntime.closes)).toBe(1);
  }),
);

it.effect("replaces a dead cached process before its exit watcher runs", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const closes = yield* Ref.make(0);
    const processRunning: Array<Ref.Ref<boolean>> = [];
    const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          const isRunning = yield* Ref.make(true);
          processRunning.push(isRunning);
          yield* Effect.addFinalizer(() => Ref.update(closes, (count) => count + 1));
          return {
            url: `http://127.0.0.1:${index}`,
            version: "1.14.19",
            isRunning: Ref.get(isRunning),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:1",
        );
        yield* Ref.set(processRunning[0]!, false);

        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:2",
        );
        expect(yield* Ref.get(starts)).toBe(2);
        expect(yield* Ref.get(closes)).toBe(1);
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }),
);

it.effect("cleans up an interrupted startup and allows a retry", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const firstStartEntered = yield* Deferred.make<void>();
    const firstStartClosed = yield* Deferred.make<void>();
    const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          yield* Effect.addFinalizer(() =>
            index === 1
              ? Deferred.succeed(firstStartClosed, undefined).pipe(Effect.ignore)
              : Effect.void,
          );
          if (index === 1) {
            yield* Deferred.succeed(firstStartEntered, undefined);
            return yield* Effect.never;
          }
          return {
            url: `http://127.0.0.1:${index}`,
            version: "1.14.19",
            isRunning: Effect.succeed(true),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
    };

    yield* Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* OpenCodeServerOwner.make({
          binaryPath: "opencode",
          directory: "/project",
        });
        const firstBorrower = yield* owner
          .withServer((server) => Effect.succeed(server.url))
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstStartEntered);
        yield* Fiber.interrupt(firstBorrower);
        yield* Deferred.await(firstStartClosed);
        expect(yield* owner.withServer((server) => Effect.succeed(server.url))).toBe(
          "http://127.0.0.1:2",
        );
      }),
    ).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
  }),
);

// Disabling or replacing an instance must not wait out a child that never becomes ready.
it.effect("stops a startup that never becomes ready when the owner closes", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const stopped = yield* Deferred.make<void>();
    const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(stopped, undefined).pipe(Effect.ignore),
          );
          yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
    };
    const ownerScope = yield* Scope.make();
    const owner = yield* OpenCodeServerOwner.make({
      binaryPath: "opencode",
      directory: "/project",
    }).pipe(
      Effect.provideService(Scope.Scope, ownerScope),
      Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime),
    );
    const borrower = yield* owner
      .withServer((server) => Effect.succeed(server.url))
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const closing = yield* Scope.close(ownerScope, Exit.void).pipe(Effect.forkChild);
    const closed = yield* Fiber.await(closing).pipe(
      Effect.timeoutOption("1 second"),
      Effect.forkChild,
    );
    yield* TestClock.adjust("1 second");
    expect(Option.isSome(yield* Fiber.join(closed))).toBe(true);
    expect(yield* Deferred.isDone(stopped)).toBe(true);
    expect((yield* Fiber.await(borrower))._tag).toBe("Failure");
  }).pipe(Effect.provide(TestClock.layer())),
);

// A reconnect that is still cleaning up a dead process when shutdown begins must not relaunch.
it.effect("starts no replacement once the owner begins closing", () =>
  Effect.gen(function* () {
    const starts = yield* Ref.make(0);
    const firstRunning = yield* Ref.make(true);
    const cleaning = yield* Deferred.make<void>();
    const finishCleanup = yield* Deferred.make<void>();
    const closingStarted = yield* Deferred.make<void>();
    const runtime: OpenCodeRuntime.OpenCodeRuntime["Service"] = {
      startOpenCodeServerProcess: () =>
        Effect.gen(function* () {
          const index = yield* Ref.updateAndGet(starts, (count) => count + 1);
          if (index === 1)
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(cleaning, undefined).pipe(
                Effect.andThen(Deferred.await(finishCleanup)),
              ),
            );
          return {
            url: `http://127.0.0.1:${index}`,
            version: "2.3.2",
            isRunning: index === 1 ? Ref.get(firstRunning) : Effect.succeed(true),
            exitCode: Effect.never,
          };
        }),
      connectToOpenCodeServer: unusedRuntimeMethod,
      runOpenCodeCommand: unusedRuntimeMethod,
      createOpenCodeSdkClient: () => ({}) as never,
      loadOpenCodeInventory: unusedRuntimeMethod,
      loadOpenCodeSkills: unusedRuntimeMethod,
    };
    const ownerScope = yield* Scope.make();
    const owner = yield* OpenCodeServerOwner.make({
      binaryPath: "opencode",
      directory: "/project",
    }).pipe(
      Effect.provideService(Scope.Scope, ownerScope),
      Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime),
    );
    // Registered after the owner, so it runs first and marks the start of shutdown.
    yield* Scope.addFinalizer(ownerScope, Deferred.succeed(closingStarted, undefined));
    yield* owner.withServer((server) => Effect.succeed(server.url));
    yield* Ref.set(firstRunning, false);
    const reconnect = yield* owner
      .withServer((server) => Effect.succeed(server.url))
      .pipe(Effect.exit, Effect.forkChild);
    yield* Deferred.await(cleaning);
    const closing = yield* Scope.close(ownerScope, Exit.void).pipe(Effect.forkChild);
    yield* Deferred.await(closingStarted);
    // Let shutdown run on to the lock the reconnect holds before the reconnect resumes.
    yield* Effect.yieldNow;
    yield* Deferred.succeed(finishCleanup, undefined);
    const reconnected = yield* Fiber.join(reconnect);
    yield* Fiber.join(closing);
    expect(yield* Ref.get(starts)).toBe(1);
    expect(reconnected._tag).toBe("Failure");
  }),
);
