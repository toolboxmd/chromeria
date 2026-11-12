import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import { nativeDownloadStream } from "./AssetAccess.ts";
import { OrchestratorMcpFailure } from "@t3tools/contracts";

it.effect(
  "native download streaming pulls one bounded block and stops at downstream cancellation",
  () =>
    Effect.gen(function* () {
      const pulled: Array<number> = [];
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const stream = nativeDownloadStream(
        {
          authorize: Effect.void,
          readBlock: (offset) =>
            Effect.sync(() => {
              pulled.push(offset);
              return new Uint8Array(1024 * 1024);
            }),
        },
        1024 * 1024 * 1024,
      );
      const consumer = yield* stream.pipe(
        Stream.runForEach(() =>
          Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(held);
      expect(pulled).toEqual([0]);
      yield* Fiber.interrupt(consumer);
      expect(pulled).toEqual([0]);
    }),
);

it.effect("native download ranges verify aligned blocks and clip only requested bytes", () =>
  Effect.gen(function* () {
    const pulled: Array<number> = [];
    const size = 1024 * 1024;
    const chunks = yield* nativeDownloadStream(
      {
        authorize: Effect.void,
        readBlock: (offset) =>
          Effect.sync(() => {
            pulled.push(offset);
            return new Uint8Array(size).fill(offset === 0 ? 7 : 8);
          }),
      },
      size * 1024,
      size - 2,
      5,
    ).pipe(Stream.runCollect);
    expect(pulled).toEqual([0, size]);
    expect(chunks.flatMap((chunk) => [...chunk])).toEqual([7, 7, 8, 8, 8]);
  }),
);

it.effect("retired original authority or wrong byte count cannot publish another block", () =>
  Effect.gen(function* () {
    let live = true;
    const authorize = Effect.suspend(() =>
      live
        ? Effect.void
        : Effect.fail(
            new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "Retired test reader",
            }),
          ),
    );
    const chunks: Array<Uint8Array> = [];
    const denied = yield* nativeDownloadStream(
      {
        authorize,
        readBlock: () =>
          Effect.sync(() => {
            live = false;
            return new Uint8Array(1024 * 1024);
          }),
      },
      1024 * 1024,
    ).pipe(
      Stream.runForEach((chunk) => Effect.sync(() => chunks.push(chunk))),
      Effect.result,
    );
    expect(denied._tag).toBe("Failure");
    expect(chunks).toEqual([]);
    const truncated = yield* nativeDownloadStream(
      { authorize: Effect.void, readBlock: () => Effect.succeed(new Uint8Array(1)) },
      1024 * 1024,
    ).pipe(Stream.runCollect, Effect.result);
    expect(truncated._tag).toBe("Failure");
  }),
);
