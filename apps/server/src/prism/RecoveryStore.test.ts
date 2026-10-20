// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { MessageId, RunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Store from "./RecoveryStore.ts";
import { decideRecovery, retryCommand } from "./recoveryPolicy.ts";

import { recoveryRun as run } from "./recovery.testkit.ts";
const threadId = run.threadId;
const input = {
  previous: null,
  run,
  failure: { class: "unknown" as const, message: "Provider failed", code: null, retryable: true },
  stoppedOrRetired: false,
  autoResume: true,
};
const storeLayer = Store.layer.pipe(Layer.provide(Persistence.layerMemory));

describe("Prism persisted recovery", () => {
  it("mixed transport and provider failures keep distinct identities and the one immediate retry budget", () => {
    const immediate = decideRecovery(input);
    const transport = decideRecovery({
      ...input,
      previous: immediate,
      run: {
        ...run,
        id: RunId.make("after-immediate"),
        ordinal: 2,
        userMessageId: immediate.retryMessageId,
      },
      failure: { ...input.failure, class: "transport_error" },
    });
    assert.strictEqual(transport.state, "retry_pending");
    assert.notEqual(transport.retryMessageId, immediate.retryMessageId);
    const exhausted = decideRecovery({
      ...input,
      previous: transport,
      run: {
        ...run,
        id: RunId.make("after-transport"),
        ordinal: 3,
        userMessageId: transport.retryMessageId,
      },
    });
    assert.strictEqual(exhausted.state, "closed");
    const firstTransport = decideRecovery({
      ...input,
      failure: { ...input.failure, class: "transport_error" },
    });
    const firstProvider = decideRecovery({
      ...input,
      previous: firstTransport,
      run: {
        ...run,
        id: RunId.make("first-provider"),
        ordinal: 2,
        userMessageId: firstTransport.retryMessageId,
      },
    });
    assert.strictEqual(firstProvider.state, "retry_pending");
    assert.strictEqual(firstProvider.retryAt, undefined);
    assert.notEqual(firstProvider.retryMessageId, firstTransport.retryMessageId);
    const secondProvider = decideRecovery({
      ...input,
      previous: firstProvider,
      run: {
        ...run,
        id: RunId.make("second-provider"),
        ordinal: 3,
        userMessageId: firstProvider.retryMessageId,
      },
    });
    assert.strictEqual(secondProvider.state, "closed");
  });
  it.effect(
    "transport retry waits sixty seconds across reloads, independently of usage-limit opt-out",
    () =>
      Effect.gen(function* () {
        const store = yield* Store.RecoveryStore;
        const failedAt = Date.parse("2026-10-08T10:01:00Z");
        const transport = {
          ...input,
          autoResume: false,
          failure: { ...input.failure, class: "transport_error" as const },
        };
        const armed = yield* store.reconcile(transport);
        assert.strictEqual(retryCommand(armed, failedAt), null);
        assert.strictEqual(retryCommand(armed, failedAt + 59_999), null);
        const reloaded = yield* store.get(threadId);
        const repeated = yield* store.reconcile(transport);
        assert.deepStrictEqual(repeated, reloaded);
        const command = retryCommand(reloaded!, failedAt + 60_000);
        assert.strictEqual(command?.type, "message.dispatch");
        assert.strictEqual(command?.commandId, "prism-retry:thread:recovery:run:original");
        yield* store.started(reloaded!);
        assert.strictEqual(retryCommand((yield* store.get(threadId))!, failedAt + 120_000), null);
      }).pipe(Effect.provide(storeLayer)),
  );
  it.effect("concurrent duplicate failure observers produce one stable retry identity", () =>
    Effect.gen(function* () {
      const store = yield* Store.RecoveryStore;
      const records = yield* Effect.all(
        Array.from({ length: 8 }, () => store.reconcile(input)),
        { concurrency: "unbounded" },
      );
      const ids = records.map((record) =>
        String(retryCommand(record, Number.MAX_SAFE_INTEGER)?.commandId),
      );
      assert.deepStrictEqual(new Set(ids), new Set(["prism-retry:thread:recovery:run:original"]));
      assert.strictEqual((yield* store.pending).length, 1);
      assert.strictEqual(records[0]?.originalRunId, run.id);
    }).pipe(Effect.provide(storeLayer)),
  );
  it.effect(
    "record-before-dispatch recovery repeats the exact command and message without a new budget",
    () =>
      Effect.gen(function* () {
        const store = yield* Store.RecoveryStore;
        const armed = yield* store.reconcile(input);
        // The persisted row is all a fresh reactor needs after a lost dispatch response.
        const reloaded = yield* store.get(threadId);
        assert.deepStrictEqual(
          retryCommand(reloaded!, Number.MAX_SAFE_INTEGER),
          retryCommand(armed, Number.MAX_SAFE_INTEGER),
        );
        const repeated = yield* store.reconcile(input);
        assert.deepStrictEqual(
          retryCommand(repeated, Number.MAX_SAFE_INTEGER),
          retryCommand(armed, Number.MAX_SAFE_INTEGER),
        );
        const command = retryCommand(armed, Number.MAX_SAFE_INTEGER);
        assert.strictEqual(command?.type, "message.dispatch");
        if (command?.type === "message.dispatch") {
          assert.deepStrictEqual(command.modelSelection, run.modelSelection);
          assert.strictEqual(command.creationSource, "server");
          assert.strictEqual(command.createdBy, "system");
        }
      }).pipe(Effect.provide(storeLayer)),
  );
  it.effect(
    "a database reopen preserves both record-before-dispatch and committed-retry windows",
    () => {
      const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "prism-restart-"));
      const dbPath = NodePath.join(dir, "state.sqlite");
      // Separate scopes close each connection before the next simulated process starts.
      const inProcess = <A, E>(work: Effect.Effect<A, E, Store.RecoveryStore>) =>
        work.pipe(
          Effect.provide(
            Store.layer.pipe(
              Layer.provide(
                Persistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer)),
              ),
            ),
          ),
        );
      return Effect.gen(function* () {
        const armed = yield* inProcess(
          Effect.gen(function* () {
            return yield* (yield* Store.RecoveryStore).reconcile({
              ...input,
              failure: { ...input.failure, class: "transport_error" },
            });
          }),
        );
        const reloaded = yield* inProcess(
          Effect.gen(function* () {
            const store = yield* Store.RecoveryStore;
            const record = yield* store.get(threadId);
            assert.strictEqual(record?.transportAttempts, 1);
            const next = decideRecovery({
              ...input,
              previous: record,
              run: {
                ...run,
                id: RunId.make("next-after-reopen"),
                ordinal: 2,
                userMessageId: record!.retryMessageId,
              },
              failure: { ...input.failure, class: "transport_error" },
            });
            assert.strictEqual(next.retryAt, Date.parse("2026-10-08T10:03:00Z"));
            assert.strictEqual(next.transportAttempts, 2);
            assert.strictEqual(retryCommand(record!, Date.parse("2026-10-08T10:01:59.999Z")), null);
            assert.strictEqual(
              retryCommand(record!, Date.parse("2026-10-08T10:02:00Z"))?.type,
              "message.dispatch",
            );
            assert.deepStrictEqual(
              retryCommand(record!, Number.MAX_SAFE_INTEGER),
              retryCommand(armed, Number.MAX_SAFE_INTEGER),
            );
            yield* store.started(record!);
            return record;
          }),
        );
        yield* inProcess(
          Effect.gen(function* () {
            const store = yield* Store.RecoveryStore;
            const record = yield* store.reconcile(input);
            assert.strictEqual(record.state, "retry_started");
            assert.strictEqual(record.originalRunId, reloaded?.originalRunId);
            assert.strictEqual(retryCommand(record, Number.MAX_SAFE_INTEGER), null);
            const completed = yield* store.reconcile({
              ...input,
              run: {
                ...run,
                id: RunId.make("run:after-restart"),
                ordinal: 2,
                userMessageId: armed.retryMessageId,
                status: "completed",
              },
            });
            assert.strictEqual(completed.state, "closed");
            assert.strictEqual(completed.transportAttempts, 0);
          }),
        );
      }).pipe(
        Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))),
      );
    },
  );
  it("stale failure events cannot reopen an exhausted budget, and Stop closes a newer active continuation", () => {
    const armed = decideRecovery(input);
    const active = decideRecovery({
      ...input,
      previous: armed,
      run: {
        ...run,
        id: RunId.make("run:retry"),
        ordinal: 2,
        userMessageId: armed.retryMessageId,
        status: "running",
      },
    });
    assert.strictEqual(
      decideRecovery({ ...input, previous: active }).sourceRunId,
      active.sourceRunId,
    );
    assert.strictEqual(
      decideRecovery({ ...input, previous: active, stoppedOrRetired: true }).state,
      "cancelled",
    );
    const exhausted = decideRecovery({
      ...input,
      previous: active,
      run: {
        ...run,
        id: active.sourceRunId,
        ordinal: 2,
        userMessageId: armed.retryMessageId,
      },
    });
    assert.strictEqual(exhausted.state, "closed");
    assert.strictEqual(decideRecovery({ ...input, previous: exhausted }).state, "closed");
  });
  it.effect(
    "exhaustion closes the retry and stale reactor writes cannot re-block finalization",
    () =>
      Effect.gen(function* () {
        const store = yield* Store.RecoveryStore;
        const armed = yield* store.reconcile(input);
        const retryRun = {
          ...run,
          id: RunId.make("run:retry"),
          ordinal: 2,
          userMessageId: armed.retryMessageId,
        };
        const exhausted = yield* store.reconcile({ ...input, run: retryRun });
        assert.strictEqual(exhausted.originalRunId, run.id);
        assert.strictEqual(exhausted.state, "closed");
        assert.strictEqual(retryCommand(exhausted, Number.MAX_SAFE_INTEGER), null);
        yield* store.started(armed);
        assert.strictEqual((yield* store.get(threadId))?.state, "closed");
        assert.strictEqual((yield* store.pending).length, 0);
      }).pipe(Effect.provide(storeLayer)),
  );
  it("Stop and retirement close both retry and reset wait", () => {
    const armed = decideRecovery(input);
    assert.strictEqual(
      decideRecovery({ ...input, previous: armed, stoppedOrRetired: true }).state,
      "cancelled",
    );
    const waiting = decideRecovery({
      ...input,
      failure: { ...input.failure, class: "usage_limit", resetAt: "2026-10-08T11:00:00Z" },
    });
    assert.strictEqual(waiting.state, "reset_wait");
    assert.strictEqual(
      decideRecovery({ ...input, previous: waiting, stoppedOrRetired: true }).state,
      "cancelled",
    );
  });
  it("reset continuation preserves retry budget and closes on the eventual result", () => {
    const waiting = decideRecovery({
      ...input,
      failure: { ...input.failure, class: "usage_limit", resetAt: "2026-10-08T11:00:00Z" },
    });
    const resumed = {
      ...run,
      id: RunId.make("run:reset"),
      ordinal: 2,
      userMessageId: MessageId.make(`limit-resume:${threadId}:${run.id}:reset`),
    };
    const retry = decideRecovery({ ...input, previous: waiting, run: resumed });
    assert.strictEqual(retry.originalRunId, run.id);
    assert.strictEqual(retry.state, "retry_pending");
    const completed = decideRecovery({
      ...input,
      previous: retry,
      run: {
        ...resumed,
        id: RunId.make("run:retried"),
        ordinal: 3,
        userMessageId: retry.retryMessageId,
        status: "completed",
      },
    });
    assert.strictEqual(completed.state, "closed");
  });
  it("nonretryable failure, absent or expired reset, and model change cannot recover", () => {
    assert.strictEqual(
      decideRecovery({ ...input, failure: { ...input.failure, retryable: false } }).state,
      "closed",
    );
    assert.strictEqual(
      decideRecovery({ ...input, failure: { ...input.failure, class: "usage_limit" } }).state,
      "closed",
    );
    assert.strictEqual(
      decideRecovery({
        ...input,
        failure: { ...input.failure, class: "usage_limit", resetAt: "2026-10-08T09:00:00Z" },
      }).state,
      "closed",
    );
    const armed = decideRecovery(input);
    assert.strictEqual(
      decideRecovery({
        ...input,
        previous: armed,
        run: { ...run, modelSelection: { ...run.modelSelection, model: "different" } },
      }).state,
      "closed",
    );
  });
});
