import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  type OrchestrationV2ServerCommand,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "./SecretRequests.ts";

const threadId = ThreadId.make("thread-orchestrator");
const turnItemId = TurnItemId.make("turn-item:secret-request:1");
const projectId = ProjectId.make("project-1");
const twoFields = [
  { name: "client_id", label: "Client ID" },
  { name: "client_secret", label: "Client secret", placeholder: "Paste the secret" },
];

const withService = <A, E>(
  body: (input: {
    readonly service: SecretRequests.SecretRequests["Service"];
    readonly stored: Map<string, Uint8Array>;
    readonly dispatched: Array<OrchestrationV2ServerCommand>;
    /** Where request files live, under the environment's state directory. */
    readonly filesRoot: string;
  }) => Effect.Effect<A, E, FileSystem.FileSystem>,
  options: {
    readonly threadId?: ThreadId;
    readonly runStatus?: string;
    readonly removeFails?: boolean;
    readonly closedFirst?: boolean;
    readonly failedRecords?: number;
    /** The card's fields; absent for a single-field request. */
    readonly fields?: ReadonlyArray<{ name: string; label: string; placeholder?: string }>;
  } = {},
) =>
  Effect.gen(function* () {
    const stored = new Map<string, Uint8Array>();
    const dispatched: Array<OrchestrationV2ServerCommand> = [];
    let secretStatus = "pending";
    let failedRecords = options.failedRecords ?? 0;
    const requestThreadId = options.threadId ?? threadId;
    const layerDependencies = Layer.mergeAll(
      NodeCrypto.layer,
      NodeServices.layer,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-secret-request-files-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
      Layer.succeed(
        ServerSecretStore.ServerSecretStore,
        ServerSecretStore.ServerSecretStore.of({
          get: (name) => Effect.yieldNow.pipe(Effect.as(Option.fromNullishOr(stored.get(name)))),
          set: (name, value) => Effect.sync(() => void stored.set(name, value)),
          create: (name, value) =>
            stored.has(name)
              ? Effect.fail(
                  new ServerSecretStore.SecretStorePersistError({
                    resource: name,
                    cause: new PlatformError.PlatformError(
                      new PlatformError.SystemError({
                        _tag: "AlreadyExists",
                        module: "FileSystem",
                        method: "open",
                      }),
                    ),
                  } as never),
                )
              : Effect.sync(() => void stored.set(name, value)),
          getOrCreateRandom: (name, bytes) =>
            Effect.sync(() => {
              const existing = stored.get(name);
              if (existing) return existing;
              const value = new Uint8Array(bytes).fill(7);
              stored.set(name, value);
              return value;
            }),
          remove: (name) =>
            options.removeFails
              ? Effect.fail(
                  new ServerSecretStore.SecretStorePersistError({
                    resource: name,
                    cause: new Error("read-only"),
                  }),
                )
              : Effect.sync(() => void stored.delete(name)),
        }),
      ),
      Layer.mock(ThreadManagementService.ThreadManagementService)({
        getThreadRecords: () =>
          Effect.succeed({
            thread: { projectId },
            runs: [{ id: "run-1", status: options.runStatus ?? "running" }],
            turnItems: [
              {
                id: turnItemId,
                threadId: requestThreadId,
                runId: "run-1",
                nodeId: "node-root",
                type: "secret_request",
                label: "GitHub token",
                reason: "Used as GH_TOKEN.",
                ...(options.fields === undefined ? {} : { fields: options.fields }),
                secretStatus,
              },
            ],
          } as never),
        dispatch: (command) => {
          if (command.type === "secret_request.record" && failedRecords > 0) {
            failedRecords -= 1;
            return Effect.fail(new Error("orchestrator unavailable") as never);
          }
          return Effect.sync(() => {
            dispatched.push(command);
            if (options.closedFirst && command.type === "secret_request.record") {
              secretStatus = "cancelled";
            } else if (command.type === "secret_request.record" && secretStatus === "pending") {
              secretStatus = command.secretStatus;
            }
            return {} as never;
          });
        },
      }),
    );
    return yield* Effect.gen(function* () {
      const service = yield* SecretRequests.SecretRequests;
      const config = yield* ServerConfig.ServerConfig;
      return yield* body({
        service,
        stored,
        dispatched,
        filesRoot: `${config.stateDir}/secret-requests`,
      });
    }).pipe(
      Effect.provide(SecretRequests.layer.pipe(Layer.provideMerge(layerDependencies))),
      Effect.scoped,
    );
  });

const valuesOf = (stored: Map<string, Uint8Array>) =>
  Array.from(stored.entries())
    .filter(([name]) => name !== "secret-request-salt")
    .map(([, bytes]) => new TextDecoder().decode(bytes));

it.effect("a saved answer becomes a one-use ref, and the thread only learns it was saved", () =>
  withService(({ service, stored, dispatched }) =>
    Effect.gen(function* () {
      yield* service.answer({
        threadId,
        turnItemId,
        answer: { type: "save", secret: "ghp_secret" },
      });
      assert.equal(dispatched.length, 1);
      assert.include(dispatched[0] as object, {
        type: "secret_request.record",
        secretStatus: "saved",
      });
      assert.notInclude(Object.values(dispatched[0] as object).map(String), "ghp_secret");

      const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
      assert.equal(yield* service.consume({ ref, projectId }), "ghp_secret");
      assert.isFalse(valuesOf(stored).some((value) => value.includes("ghp_secret")));
      const again = yield* service.consume({ ref, projectId }).pipe(Effect.flip);
      assert.include(again.message, "already used");
    }),
  ),
);

it.effect("two concurrent uses of one ref hand the value out once", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      yield* service.answer({
        threadId,
        turnItemId,
        answer: { type: "save", secret: "ghp_secret" },
      });
      const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
      const results = yield* Effect.forEach(
        [service.consume({ ref, projectId }), service.consume({ ref, projectId })],
        Effect.result,
        { concurrency: "unbounded" },
      );
      assert.deepEqual(results.map((result) => result._tag).toSorted(), ["Failure", "Success"]);
    }),
  ),
);

it.effect("a save that loses to the card closing deletes the value and says so", () =>
  withService(
    ({ service, stored }) =>
      Effect.gen(function* () {
        const error = yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "ghp_secret" } })
          .pipe(Effect.flip);
        assert.equal(error.reason, "agent_stopped");
        assert.isFalse(valuesOf(stored).some((value) => value.includes("ghp_secret")));
      }),
    { closedFirst: true },
  ),
);

it.effect("a save whose record failed can be saved again", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const failed = yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "ghp_secret" } })
          .pipe(Effect.flip);
        assert.equal(failed.reason, "record_failed");
        yield* service.answer({
          threadId,
          turnItemId,
          answer: { type: "save", secret: "ghp_secret" },
        });
        const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
        assert.equal(yield* service.consume({ ref, projectId }), "ghp_secret");
      }),
    { failedRecords: 1 },
  ),
);

it.effect("a ref only works in the project it was entered for", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      yield* service.answer({
        threadId,
        turnItemId,
        answer: { type: "save", secret: "ghp_secret" },
      });
      const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
      const elsewhere = yield* service
        .consume({ ref, projectId: ProjectId.make("project-other") })
        .pipe(Effect.flip);
      assert.include(elsewhere.message, "does not exist");
      assert.equal(yield* service.consume({ ref, projectId }), "ghp_secret");
    }),
  ),
);

it.effect("declining stores nothing, and a request is answered once", () =>
  withService(({ service, stored, dispatched }) =>
    Effect.gen(function* () {
      yield* service.answer({ threadId, turnItemId, answer: { type: "decline" } });
      assert.deepEqual(valuesOf(stored), []);
      assert.isTrue(Option.isNone(yield* service.saved({ threadId, turnItemId })));
      const late = yield* service
        .answer({ threadId, turnItemId, answer: { type: "save", secret: "ghp_secret" } })
        .pipe(Effect.flip);
      assert.include(late.message, "already answered");
      assert.equal(dispatched.length, 1);
      assert.deepEqual(valuesOf(stored), []);
    }),
  ),
);

it.effect("traces and counts a saved answer without ever recording the value", () =>
  Effect.gen(function* () {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const counted = Metric.snapshot.pipe(
      Effect.map((snapshots) => {
        const found = snapshots.find(
          (snapshot) =>
            snapshot.id === "t3_secret_refs_consumed_total" &&
            snapshot.attributes?.result === "used",
        );
        return found?.type === "Counter" ? Number(found.state.count) : 0;
      }),
    );
    const before = yield* counted;
    yield* withService(({ service }) =>
      Effect.gen(function* () {
        yield* service.answer({
          threadId,
          turnItemId,
          answer: { type: "save", secret: "ghp_secret" },
        });
        const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
        yield* service.consume({ ref, projectId });
      }),
    ).pipe(Effect.withTracer(tracer));
    assert.equal((yield* counted) - before, 1);
    const recorded = spans.flatMap((span) => [
      span.name,
      ...Array.from(span.attributes.values(), String),
    ]);
    assert.include(recorded, "SecretRequests.answer");
    assert.include(recorded, "SecretRequests.consume");
    assert.isFalse(recorded.some((value) => value.includes("ghp_secret")));
  }),
);

it.effect("works in threads with long ids, such as a delegated subagent's", () => {
  const delegatedThreadId = ThreadId.make(
    `thread:delegated-task:command%3Amcp%3A${"a".repeat(36)}%3Adelegate-task%3Arelease-notes-v0.2.0-${"b".repeat(40)}`,
  );
  return withService(
    ({ service, stored }) =>
      Effect.gen(function* () {
        yield* service.answer({
          threadId: delegatedThreadId,
          turnItemId,
          answer: { type: "save", secret: "ghp_secret" },
        });
        assert.isTrue(Array.from(stored.keys()).every((name) => name.length < 100));
        const ref = Option.getOrThrow(
          yield* service.saved({ threadId: delegatedThreadId, turnItemId }),
        ).secretRef!;
        assert.equal(yield* service.consume({ ref, projectId }), "ghp_secret");
      }),
    { threadId: delegatedThreadId },
  );
});

it.effect("refuses an answer once the agent that asked has stopped", () =>
  withService(
    ({ service, stored, dispatched }) =>
      Effect.gen(function* () {
        const late = yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "ghp_secret" } })
          .pipe(Effect.flip);
        assert.include(late.message, "has stopped");
        assert.deepEqual(valuesOf(stored), []);
        assert.equal(dispatched.length, 0);
      }),
    { runStatus: "completed" },
  ),
);

it.effect("a used value that cannot be deleted is not handed out", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        yield* service.answer({
          threadId,
          turnItemId,
          answer: { type: "save", secret: "ghp_secret" },
        });
        const ref = Option.getOrThrow(yield* service.saved({ threadId, turnItemId })).secretRef!;
        const failed = yield* service.consume({ ref, projectId }).pipe(Effect.flip);
        assert.include(failed.message, "Could not use");
      }),
    { removeFails: true },
  ),
);

/** Every request directory under the files root, empty when nothing was written. */
const requestDirs = (filesRoot: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    if (!(yield* fileSystem.exists(filesRoot))) return [];
    return yield* fileSystem.readDirectory(filesRoot);
  });

const modeOf = (path: string) =>
  FileSystem.FileSystem.use((fileSystem) => fileSystem.stat(path)).pipe(
    Effect.map((info) => info.mode & 0o777),
  );

it.effect("a single-field save also leaves the value in a private file", () =>
  withService(({ service, filesRoot }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      yield* service.answer({
        threadId,
        turnItemId,
        answer: { type: "save", secret: "ghp_secret" },
      });
      const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
      const path = saved.files.secret!;
      assert.deepEqual(Object.keys(saved.files), ["secret"]);
      assert.isTrue(path.startsWith(`${filesRoot}/`));
      assert.equal(yield* fileSystem.readFileString(path), "ghp_secret");
      assert.equal(yield* modeOf(path), 0o600);
      assert.isDefined(saved.secretRef);
    }),
  ),
);

it.effect("a multi-field save writes each value to its own 0600 file in a 0700 directory", () =>
  withService(
    ({ service, stored, dispatched, filesRoot }) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* service.answer({
          threadId,
          turnItemId,
          answer: { type: "save", secrets: { client_id: "id-1", client_secret: "cs-2" } },
        });
        const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
        assert.deepEqual(Object.keys(saved.files), ["client_id", "client_secret"]);
        assert.equal(yield* fileSystem.readFileString(saved.files.client_id!), "id-1");
        assert.equal(yield* fileSystem.readFileString(saved.files.client_secret!), "cs-2");
        assert.equal(yield* modeOf(saved.files.client_id!), 0o600);
        assert.equal(yield* modeOf(saved.files.client_secret!), 0o600);
        const [dir] = yield* requestDirs(filesRoot);
        assert.equal(yield* modeOf(`${filesRoot}/${dir}`), 0o700);
        // A ref carries one value, so a multi-field request has none.
        assert.isUndefined(saved.secretRef);
        assert.deepEqual(valuesOf(stored), []);
        // The thread records the fields asked for, never what was typed.
        assert.include(dispatched[0] as object, { secretStatus: "saved" });
        assert.notMatch(JSON.stringify(dispatched), /id-1|cs-2/);
      }),
    { fields: twoFields },
  ),
);

it.effect("a save missing a field, or with one too many, writes nothing", () =>
  withService(
    ({ service, dispatched, filesRoot }) =>
      Effect.gen(function* () {
        for (const secrets of [
          { client_id: "id-1" },
          { client_id: "id-1", client_secret: "cs-2", extra: "x" },
        ]) {
          const error = yield* service
            .answer({ threadId, turnItemId, answer: { type: "save", secrets } })
            .pipe(Effect.flip);
          assert.equal(error.reason, "fields_mismatch");
        }
        const single = yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "x" } })
          .pipe(Effect.flip);
        assert.equal(single.reason, "fields_mismatch");
        assert.deepEqual(yield* requestDirs(filesRoot), []);
        assert.equal(dispatched.length, 0);
      }),
    { fields: twoFields },
  ),
);

it.effect("declining, or losing the save to the card closing, leaves no files", () =>
  Effect.gen(function* () {
    yield* withService(({ service, filesRoot }) =>
      Effect.gen(function* () {
        yield* service.answer({ threadId, turnItemId, answer: { type: "decline" } });
        assert.deepEqual(yield* requestDirs(filesRoot), []);
      }),
    );
    yield* withService(
      ({ service, filesRoot }) =>
        Effect.gen(function* () {
          yield* service
            .answer({
              threadId,
              turnItemId,
              answer: { type: "save", secrets: { client_id: "id-1", client_secret: "cs-2" } },
            })
            .pipe(Effect.flip);
          assert.deepEqual(yield* requestDirs(filesRoot), []);
        }),
      { closedFirst: true, fields: twoFields },
    );
  }),
);

it.effect("drops request files once they expire, and directories the agent emptied", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const config = yield* ServerConfig.ServerConfig;
    const root = `${config.stateDir}/secret-requests`;
    yield* TestClock.adjust("2 days");
    const now = yield* Clock.currentTimeMillis;
    const write = (dir: string, savedAt: number | null) =>
      Effect.gen(function* () {
        yield* fileSystem.makeDirectory(`${root}/${dir}`, { recursive: true });
        if (savedAt === null) return;
        yield* fileSystem.writeFileString(`${root}/${dir}/token`, "ghp_secret");
        yield* fileSystem.utimes(`${root}/${dir}/token`, savedAt / 1000, savedAt / 1000);
      });
    const stale = "a".repeat(32);
    const fresh = "b".repeat(32);
    const emptied = "c".repeat(32);
    const unrelated = "notes";
    yield* write(stale, now - 25 * 60 * 60 * 1000);
    yield* write(fresh, now);
    yield* write(emptied, null);
    yield* write(unrelated, now - 25 * 60 * 60 * 1000);

    // Building the service runs the first sweep.
    yield* SecretRequests.SecretRequests.pipe(
      Effect.asVoid,
      Effect.provide(
        SecretRequests.layer.pipe(
          Layer.provide(Layer.mock(ThreadManagementService.ThreadManagementService)({})),
        ),
      ),
      Effect.scoped,
    );

    assert.deepEqual((yield* fileSystem.readDirectory(root)).toSorted(), [fresh, unrelated]);
  }).pipe(
    Effect.provide(
      ServerSecretStore.layer.pipe(
        Layer.provideMerge(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-secret-request-sweep-" }),
        ),
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);

it.effect("a value is kept exactly as typed, spaces included", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      yield* service.answer({
        threadId,
        turnItemId,
        answer: { type: "save", secret: " pass word " },
      });
      const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
      assert.equal(yield* fileSystem.readFileString(saved.files.secret!), " pass word ");
      assert.equal(yield* service.consume({ ref: saved.secretRef!, projectId }), " pass word ");
    }),
  ),
);

it.effect("a retry after a failed record replaces a ref that could not be deleted", () =>
  withService(
    ({ service, stored }) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "first" } })
          .pipe(Effect.flip);
        yield* service.answer({ threadId, turnItemId, answer: { type: "save", secret: "second" } });
        const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
        assert.equal(yield* fileSystem.readFileString(saved.files.secret!), "second");
        assert.deepEqual(
          valuesOf(stored).map((value) => JSON.parse(value).value),
          ["second"],
        );
      }),
    { failedRecords: 1, removeFails: true },
  ),
);

it.effect("a retry after a failed record replaces files that could not be deleted", () =>
  withService(
    ({ service, stored, filesRoot }) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* service
          .answer({ threadId, turnItemId, answer: { type: "save", secret: "first" } })
          .pipe(Effect.flip);
        // The failed save's ref was deleted, but its directory was left behind.
        const name = Array.from(stored.keys()).find((key) =>
          /^secret-request-[0-9a-f]{32}$/.test(key),
        )!;
        stored.delete(name);
        const leftover = `${filesRoot}/${name.slice("secret-request-".length)}`;
        yield* fileSystem.makeDirectory(leftover, { recursive: true });
        yield* fileSystem.writeFileString(`${leftover}/secret`, "first");

        yield* service.answer({ threadId, turnItemId, answer: { type: "save", secret: "second" } });
        const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
        assert.equal(saved.files.secret, `${leftover}/secret`);
        assert.equal(yield* fileSystem.readFileString(saved.files.secret!), "second");
        assert.deepEqual(
          valuesOf(stored).map((value) => JSON.parse(value).value),
          ["second"],
        );
      }),
    // The store cannot delete, so the failed save's ref survives for the test to find.
    { failedRecords: 1, removeFails: true },
  ),
);

it.effect("an answer whose record failed never deletes what a racing answer saved", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const results = yield* Effect.forEach(
          ["value-a", "value-b"],
          (secret) =>
            Effect.result(
              service.answer({ threadId, turnItemId, answer: { type: "save", secret } }),
            ),
          { concurrency: "unbounded" },
        );
        assert.deepEqual(results.map((result) => result._tag).toSorted(), ["Failure", "Success"]);
        const saved = Option.getOrThrow(yield* service.saved({ threadId, turnItemId }));
        const fromFile = yield* fileSystem.readFileString(saved.files.secret!);
        assert.equal(yield* service.consume({ ref: saved.secretRef!, projectId }), fromFile);
      }),
    { failedRecords: 1 },
  ),
);

it.effect("a directory the sweep cannot read does not keep it from the rest", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const config = yield* ServerConfig.ServerConfig;
    const root = `${config.stateDir}/secret-requests`;
    yield* TestClock.adjust("2 days");
    const now = yield* Clock.currentTimeMillis;
    // Sorted first, and its dangling link fails every stat.
    const broken = "0".repeat(32);
    const stale = "a".repeat(32);
    yield* fileSystem.makeDirectory(`${root}/${broken}`, { recursive: true });
    yield* fileSystem.symlink(`${root}/missing`, `${root}/${broken}/token`);
    yield* fileSystem.makeDirectory(`${root}/${stale}`, { recursive: true });
    yield* fileSystem.writeFileString(`${root}/${stale}/token`, "ghp_secret");
    const old = (now - 25 * 60 * 60 * 1000) / 1000;
    yield* fileSystem.utimes(`${root}/${stale}/token`, old, old);

    yield* SecretRequests.SecretRequests.pipe(
      Effect.asVoid,
      Effect.provide(
        SecretRequests.layer.pipe(
          Layer.provide(Layer.mock(ThreadManagementService.ThreadManagementService)({})),
        ),
      ),
      Effect.scoped,
    );

    assert.deepEqual(yield* fileSystem.readDirectory(root), [broken]);
  }).pipe(
    Effect.provide(
      ServerSecretStore.layer.pipe(
        Layer.provideMerge(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-secret-request-sweep-" }),
        ),
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  ),
);
