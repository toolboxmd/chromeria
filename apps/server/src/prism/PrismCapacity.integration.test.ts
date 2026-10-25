import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  TurnItemId,
  type OrchestrationV2ProviderFailure,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import { buildUnavailableProviderSnapshot } from "../provider/unavailableProviderSnapshot.ts";
import * as Settings from "../serverSettings.ts";
import * as Prism from "./PrismService.ts";
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

const seed = Effect.fn("capacityTest.seed")(function* (
  observed = failure,
  nodeId = recoveryRun.rootNodeId,
  retired = false,
) {
  const error = errorFor(recoveryRun);
  yield* (yield* EventSink.EventSinkV2).commitCommand({
    commandId: CommandId.make("capacity:seed"),
    commandType: "fixture",
    threadId: recoveryRun.threadId,
    acceptedAt: recoveryRun.requestedAt,
    events: recoveryEvents(recoveryRun).map((event) =>
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

const dependencies = (staleWindow = false, candidates = [limited, spare]) =>
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
          driverKind: "codex",
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
                    windows: [
                      {
                        id: "session",
                        kind: "session" as const,
                        label: "Session",
                        usedPercent: 10,
                        resetsAt: resetAt,
                      },
                    ],
                  },
                }
              : {}),
          })),
      ),
    }),
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
      }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
    },
  );

  it.effect(
    "retains the executing instance's limit after retirement and a thread Color change",
    () => {
      const deps = dependencies();
      return Effect.gen(function* () {
        yield* seed(failure, recoveryRun.rootNodeId, true);
        assert.deepStrictEqual((yield* resolve()).modelSelection, spare);
      }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
    },
  );

  it.effect("does not escape configured candidates when all are exhausted", () => {
    const deps = dependencies(false, [limited]);
    return Effect.gen(function* () {
      yield* seed();
      const result = yield* Effect.flip(resolve());
      assert.strictEqual(result.code, "provider_unavailable");
    }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
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
    }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
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
    }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
  });

  it.effect("ignores nested errors that are not the executing run's root failure", () => {
    const deps = dependencies();
    return Effect.gen(function* () {
      yield* seed(failure, NodeId.make("nested"));
      assert.deepStrictEqual((yield* resolve()).modelSelection, limited);
    }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
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
    }).pipe(Effect.provide(Prism.layer.pipe(Layer.provide(deps))), Effect.provide(deps));
  });
});
