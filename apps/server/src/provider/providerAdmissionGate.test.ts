import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Layer from "effect/Layer";

import { make } from "./providerAdmissionGate.ts";
import { readBusyProviderDrivers } from "./providerAutoUpdateState.ts";

const installKey = "codex:npm-global:/shared-prefix";

it.effect("start admission publishes busy state before a waiting update can check it", () =>
  Effect.gen(function* () {
    const gate = yield* make;
    const startEntered = yield* Deferred.make<void>();
    const publish = yield* Deferred.make<void>();
    const updateRequested = yield* Deferred.make<void>();
    const installed = yield* Deferred.make<void>();
    let busy = false;
    const start = yield* gate
      .withStart(
        installKey,
        Deferred.succeed(startEntered, undefined).pipe(
          Effect.andThen(Deferred.await(publish)),
          Effect.andThen(
            Effect.sync(() => {
              busy = true;
            }),
          ),
        ),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(startEntered);
    const update = yield* Deferred.succeed(updateRequested, undefined).pipe(
      Effect.andThen(
        gate.withUpdate({
          installKey,
          isBusy: Effect.sync(() => busy),
          install: Deferred.succeed(installed, undefined),
        }),
      ),
      Effect.forkScoped,
    );
    yield* Deferred.await(updateRequested);
    assert.isFalse(yield* Deferred.isDone(installed));
    yield* Deferred.succeed(publish, undefined);
    yield* Fiber.join(start);
    assert.isTrue(Option.isNone(yield* Fiber.join(update)));
    assert.isFalse(yield* Deferred.isDone(installed));
  }).pipe(Effect.scoped),
);

it.effect("a start sharing the installation waits until the update finishes", () =>
  Effect.gen(function* () {
    const gate = yield* make;
    const installEntered = yield* Deferred.make<void>();
    const finishInstall = yield* Deferred.make<void>();
    const startRequested = yield* Deferred.make<void>();
    const started = yield* Deferred.make<void>();
    let installFinished = false;
    const update = yield* gate
      .withUpdate({
        installKey,
        isBusy: Effect.succeed(false),
        install: Deferred.succeed(installEntered, undefined).pipe(
          Effect.andThen(Deferred.await(finishInstall)),
          Effect.andThen(
            Effect.sync(() => {
              installFinished = true;
            }),
          ),
        ),
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(installEntered);
    const start = yield* Deferred.succeed(startRequested, undefined).pipe(
      Effect.andThen(
        gate.withStart(
          installKey,
          Effect.gen(function* () {
            assert.isTrue(installFinished);
            yield* Deferred.succeed(started, undefined);
          }),
        ),
      ),
      Effect.forkScoped,
    );
    yield* Deferred.await(startRequested);
    assert.isFalse(yield* Deferred.isDone(started));
    yield* Deferred.succeed(finishInstall, undefined);
    assert.isTrue(Option.isSome(yield* Fiber.join(update)));
    yield* Fiber.join(start);
    assert.isTrue(yield* Deferred.isDone(started));
  }).pipe(Effect.scoped),
);

it.effect("interrupting an update releases admission for the next start", () =>
  Effect.gen(function* () {
    const gate = yield* make;
    const entered = yield* Deferred.make<void>();
    const update = yield* gate
      .withUpdate({
        installKey,
        isBusy: Effect.succeed(false),
        install: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(update);
    assert.equal(yield* gate.withStart(installKey, Effect.succeed("started")), "started");
  }).pipe(Effect.scoped),
);

it.effect("busy admission reads only active V2 projections", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions
      (provider_session_id, provider, status, updated_at, payload_json)
      VALUES ('running', 'codex', 'running', '2026-10-01', '{}'),
             ('idle', 'idle-driver', 'ready', '2026-10-01', '{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs
      (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
      VALUES ('queued', 'thread', 1, 'queued-driver', 'queued', '2026-10-01', '{}'),
             ('done', 'thread', 2, 'done-driver', 'completed', '2026-10-01', '{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads
      (provider_thread_id, provider, status, updated_at, payload_json)
      VALUES ('active', 'child-driver', 'active', '2026-10-01', '{}')`;
    assert.deepEqual([...(yield* readBusyProviderDrivers)].sort(), [
      "child-driver",
      "codex",
      "queued-driver",
    ]);
  }).pipe(Effect.provide(Layer.orDie(SqlitePersistence.layerMemory))),
);

it.effect("independent starts sharing a CLI are admitted concurrently", () =>
  Effect.gen(function* () {
    const gate = yield* make;
    const firstEntered = yield* Deferred.make<void>();
    const secondEntered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<void>();
    const first = yield* gate
      .withStart(
        installKey,
        Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Deferred.await(finish))),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(firstEntered);
    const second = yield* gate
      .withStart(
        installKey,
        Deferred.succeed(secondEntered, undefined).pipe(Effect.andThen(Deferred.await(finish))),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(secondEntered);
    yield* Deferred.succeed(finish, undefined);
    yield* Fiber.join(first);
    yield* Fiber.join(second);
  }).pipe(Effect.scoped),
);
