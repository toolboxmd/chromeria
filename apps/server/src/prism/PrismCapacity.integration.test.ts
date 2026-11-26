import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2TurnItem,
  type ProviderConsumeResetCreditOutcome,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Prism from "./PrismService.ts";
import * as ResetRedemptions from "./ResetRedemptions.ts";
import { errorFor, recoveryEvents, recoveryRun, recoveryTestLayer } from "./recovery.testkit.ts";

const limited = recoveryRun.modelSelection;
const spare = { ...limited, instanceId: ProviderInstanceId.make("codex-spare") };
const resetAt = "2026-10-08T12:00:00Z";
const now = DateTime.makeUnsafe("2026-10-08T11:00:00Z");
const projectId = ProjectId.make("project:recovery");
const failure: OrchestrationV2ProviderFailure = {
  class: "usage_limit",
  code: "rate_limit",
  message: "Subscription exhausted",
  retryable: true,
  resetAt,
};

const providerThreadId = ProviderThreadId.make("provider-thread:recovery");

const seed = Effect.fn("capacityTest.seed")(function* (
  observed = failure,
  nodeId = recoveryRun.rootNodeId,
  retired = false,
  driver = "codex",
) {
  const error = errorFor(recoveryRun);
  const run = { ...recoveryRun, providerThreadId };
  const thread: OrchestrationV2DomainEvent = {
    id: EventId.make("capacity:provider-thread"),
    threadId: run.threadId,
    occurredAt: run.requestedAt,
    type: "provider-thread.updated",
    payload: {
      id: providerThreadId,
      driver: ProviderDriverKind.make(driver),
      providerInstanceId: run.providerInstanceId!,
      providerSessionId: null,
      appThreadId: run.threadId,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      goal: null,
      createdAt: run.requestedAt,
      updatedAt: run.requestedAt,
    },
  };
  yield* (yield* EventSink.EventSinkV2).commitCommand({
    commandId: CommandId.make("capacity:seed"),
    commandType: "fixture",
    threadId: recoveryRun.threadId,
    acceptedAt: recoveryRun.requestedAt,
    events: [...recoveryEvents(run), thread].map((event) =>
      event.type === "turn-item.updated"
        ? { ...event, payload: { ...error, nodeId, failure: observed } }
        : event.type === "thread.created" && retired
          ? {
              ...event,
              payload: {
                ...event.payload,
                settledOverride: "settled",
                modelSelection: spare,
                providerInstanceId: spare.instanceId,
              },
            }
          : event,
    ),
    effects: [],
  });
  yield* TestClock.setTime(DateTime.toEpochMillis(now));
});

const dependencies = (
  staleWindow = false,
  candidates = [limited, spare],
  window: { readonly id: string; readonly label: string; readonly usedPercent: number } = {
    id: "session",
    label: "Session",
    usedPercent: 10,
  },
  driverKind = "codex",
) =>
  Layer.mergeAll(
    recoveryTestLayer,
    Settings.layerTest({
      autoResumeLimitedThreads: false,
      prismRoles: {
        worker: { lanes: { hard: candidates } },
        "computer-use": {
          models: candidates.map((candidate) => ({ ...candidate, model: "small" })),
        },
      },
    }),
    Layer.mock(Providers.ProviderRegistry)({
      getProviders: Effect.map(
        buildUnavailableProviderSnapshot({
          driverKind: ProviderDriverKind.make(driverKind),
          instanceId: limited.instanceId,
          reason: "fixture",
        }),
        (p) =>
          [limited, spare].map((selection) => ({
            ...p,
            instanceId: selection.instanceId,
            enabled: true,
            availability: "available" as const,
            models: ["fixed", "small"].map((slug) => ({
              slug,
              name: slug,
              isCustom: false,
              capabilities: null,
            })),
            ...(staleWindow
              ? {
                  usageLimits: {
                    checkedAt: "2026-10-08T10:00:00Z",
                    windows: [{ ...window, kind: "session" as const, resetsAt: resetAt }],
                  },
                }
              : {}),
          })),
      ),
    }),
  );

const withRedemptions = (deps: ReturnType<typeof dependencies>) =>
  Layer.mergeAll(
    deps,
    Prism.layer.pipe(Layer.provide(deps)),
    ResetRedemptions.layer.pipe(Layer.provide(deps)),
  );

/** A reset redeemed on `instanceId` through `driver`, its clock at `at`, with the provider's `outcome`. */
const redeemAt = Effect.fn("capacityTest.redeemAt")(function* (
  instanceId: ProviderInstanceId,
  at: string,
  outcome: ProviderConsumeResetCreditOutcome = "reset",
  driver = "codex",
) {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(at)));
  yield* (yield* ResetRedemptions.ResetRedemptions).redeem(
    { instanceId, driver: ProviderDriverKind.make(driver) },
    Effect.succeed(outcome),
  );
  yield* TestClock.setTime(DateTime.toEpochMillis(now));
});

const voided = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) =>
    sql<{
      readonly turn_item_id: string;
    }>`SELECT turn_item_id FROM fork_prism_reset_voided_failures`,
);

const resolve = (role: "worker" | "computer-use" = "worker") =>
  Effect.flatMap(Prism.PrismService, (prism) => prism.resolve({ projectId, role, lane: "hard" }));

describe("Prism capacity from durable run failures", () => {
  it.effect.each([false, true])(
    "skips known limits across new role calls (stale windows: %s)",
    (stale) => {
      const deps = dependencies(stale);
      return Effect.gen(function* () {
        yield* seed();
        for (const role of ["worker", "computer-use", "worker"] as const) {
          assert.deepStrictEqual((yield* resolve(role)).modelSelection, {
            ...spare,
            model: role === "computer-use" ? "small" : "fixed",
          });
        }
      }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
    },
  );

  it.effect(
    "retains the executing instance's limit after retirement and a thread Color change",
    () => {
      const deps = dependencies();
      return Effect.gen(function* () {
        yield* seed(failure, recoveryRun.rootNodeId, true);
        assert.deepStrictEqual((yield* resolve()).modelSelection, spare);
      }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
    },
  );

  it.effect("does not escape configured candidates when all are exhausted", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed();
      const result = yield* Effect.flip(resolve());
      assert.strictEqual(result.code, "provider_unavailable");
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });

  it.effect("keeps explicit Colors and releases only the limited instance at expiry", () => {
    const deps = dependencies();
    return Effect.gen(function* () {
      yield* seed();
      const prism = yield* Prism.PrismService;
      const explicit = yield* prism.resolve({
        projectId,
        role: "worker",
        explicit: limited,
      });
      assert.deepStrictEqual(explicit.modelSelection, limited);
      assert.deepStrictEqual((yield* resolve()).modelSelection, spare);
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(resetAt)));
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });

  it.effect.each([
    { ...failure, resetAt: null },
    { ...failure, resetAt: "invalid" },
    { ...failure, resetAt: "2026-10-08T09:00:00Z" },
    { ...failure, class: "provider_error" as const },
  ])("does not invent an unexpired limit for %j", (observed) => {
    const deps = dependencies();
    return Effect.gen(function* () {
      yield* seed(observed);
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });

  it.effect("ignores nested errors that are not the executing run's root failure", () => {
    const deps = dependencies();
    return Effect.gen(function* () {
      yield* seed(failure, NodeId.make("nested"));
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });

  it.effect("uses the latest root failure rather than an earlier quota error", () => {
    const deps = dependencies();
    return Effect.gen(function* () {
      yield* seed();
      const later = { ...errorFor(recoveryRun), id: TurnItemId.make("later-error"), ordinal: 2 };
      yield* (yield* EventSink.EventSinkV2).commitCommand({
        commandId: CommandId.make("capacity:later-error"),
        commandType: "fixture",
        threadId: recoveryRun.threadId,
        acceptedAt: now,
        events: [
          {
            id: EventId.make("capacity:later-error"),
            threadId: recoveryRun.threadId,
            occurredAt: now,
            type: "turn-item.updated",
            payload: later,
          },
        ],
        effects: [],
      });
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });
  it.effect("a reset redeemed after a failure voids it on that instance only", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed();
      yield* redeemAt(spare.instanceId, "2026-10-08T10:30:00Z");
      assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
      yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z");
      assert.deepStrictEqual(yield* voided, [{ turn_item_id: errorFor(recoveryRun).id }]);
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  // A redemption voids only the failures that existed when it started, whatever the clocks say.
  it.effect.each([
    { case: "with the clock stepped back", at: "2026-10-08T10:30:00Z" },
    { case: "on an ordinary clock", at: "2026-10-08T10:00:30Z" },
  ])("keeps a failure recorded after a reset redeemed $case", ({ at }) => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* redeemAt(limited.instanceId, at);
      yield* seed();
      const result = yield* Effect.flip(resolve());
      assert.strictEqual(
        result.message,
        "No eligible model with usage headroom for this Prism role and lane. Skipped: codex/fixed: run run:original hit a usage limit at 2026-10-08T10:01:00.000Z, blocked until 2026-10-08T12:00:00Z.",
      );
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect("keeps a voided failure voided when Codex later rewrites it", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed({ ...failure, resetAt: null });
      // Without a reset time the failure blocks nothing yet; record one so it is in the set.
      const restamp = (at: string) =>
        Effect.gen(function* () {
          const occurredAt = DateTime.makeUnsafe(at);
          yield* (yield* EventSink.EventSinkV2).commitCommand({
            commandId: CommandId.make(`capacity:restamp:${at}`),
            commandType: "fixture",
            threadId: recoveryRun.threadId,
            acceptedAt: occurredAt,
            events: [
              {
                id: EventId.make(`capacity:restamp:${at}`),
                threadId: recoveryRun.threadId,
                occurredAt,
                type: "turn-item.updated",
                payload: {
                  ...(errorFor(recoveryRun) as Extract<OrchestrationV2TurnItem, { type: "error" }>),
                  updatedAt: occurredAt,
                  failure,
                },
              },
            ],
            effects: [],
          });
        });
      yield* restamp("2026-10-08T10:02:00Z");
      yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z");
      // A late rate-limit notification gives the same error a newer timestamp.
      yield* restamp("2026-10-08T10:45:00Z");
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect("does not void a failure made by another driver under the same instance id", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed(failure, recoveryRun.rootNodeId, false, "claudeAgent");
      yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z", "reset", "codex");
      assert.deepStrictEqual(yield* voided, []);
      assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
      yield* (yield* SqlClient.SqlClient)`INSERT INTO fork_prism_reset_voided_failures
        VALUES (${errorFor(recoveryRun).id}, ${limited.instanceId}, 'codex', '2026-10-08T10:30:00Z')`;
      assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect("keeps a failure recorded while the reset is being confirmed", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2026-10-08T10:00:30Z")));
      const redemptions = yield* ResetRedemptions.ResetRedemptions;
      yield* redemptions.redeem(
        { instanceId: limited.instanceId, driver: ProviderDriverKind.make("codex") },
        seed().pipe(Effect.as("reset" as const)),
      );
      assert.deepStrictEqual(yield* voided, []);
      assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect.each(["nothingToReset", "noCredit", "alreadyRedeemed"] as const)(
    "records nothing for a %s outcome",
    (outcome) => {
      const deps = dependencies(false, [limited]);
      return Effect.gen(function* () {
        yield* seed();
        yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z", outcome);
        assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
        assert.deepStrictEqual(yield* voided, []);
      }).pipe(Effect.provide(withRedemptions(deps)));
    },
  );

  it.effect("still skips a voided provider whose usage window is exhausted", () => {
    const deps = dependencies(true, [limited], {
      id: "session",
      label: "Session",
      usedPercent: 100,
    });
    return Effect.gen(function* () {
      yield* seed();
      yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z");
      const result = yield* Effect.flip(resolve());
      assert.strictEqual(
        result.message,
        "No eligible model with usage headroom for this Prism role and lane. Skipped: codex/fixed: Session usage window is exhausted until 2026-10-08T12:00:00Z.",
      );
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect("skips Cursor when only its Overall window is exhausted", () => {
    const deps = dependencies(
      true,
      [limited],
      { id: "totalPercentUsed", label: "Overall", usedPercent: 100 },
      "cursor",
    );
    return Effect.gen(function* () {
      yield* TestClock.setTime(DateTime.toEpochMillis(now));
      const result = yield* Effect.flip(resolve());
      assert.strictEqual(
        result.message,
        "No eligible model with usage headroom for this Prism role and lane. Skipped: codex/fixed: Overall usage window is exhausted until 2026-10-08T12:00:00Z.",
      );
    }).pipe(Effect.provide(withRedemptions(deps)));
  });

  it.effect("keeps a recorded reset across a fresh service on the same database", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed();
      // A restart builds the service again over the same database.
      const database = Layer.succeed(SqlClient.SqlClient, yield* SqlClient.SqlClient);
      const restarted = () => Layer.fresh(ResetRedemptions.layer.pipe(Layer.provide(database)));
      yield* redeemAt(limited.instanceId, "2026-10-08T10:30:00Z").pipe(Effect.provide(restarted()));
      yield* Effect.provide(Effect.void, restarted());
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Layer.mergeAll(deps, Prism.layer.pipe(Layer.provide(deps)))));
  });

  it.effect("keeps the block and the reset's success when the record cannot be written", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      // Readable, but without the column the insert writes.
      yield* sql`DROP TABLE fork_prism_reset_voided_failures`;
      yield* sql`CREATE TABLE fork_prism_reset_voided_failures
        (turn_item_id TEXT, provider_instance_id TEXT, driver TEXT)`;
      const outcome = yield* (yield* ResetRedemptions.ResetRedemptions).redeem(
        { instanceId: limited.instanceId, driver: ProviderDriverKind.make("codex") },
        Effect.succeed("reset" as const),
      );
      assert.strictEqual(outcome, "reset");
      assert.deepStrictEqual(yield* voided, []);
      assert.strictEqual((yield* Effect.flip(resolve())).code, "provider_unavailable");
    }).pipe(Effect.provide(withRedemptions(deps)));
  });
});
