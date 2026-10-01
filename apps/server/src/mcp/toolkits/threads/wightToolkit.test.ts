// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  CommandId,
  MessageId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import * as TestClock from "effect/testing/TestClock";
import { RESUME_TEXT } from "./usageLimitResume.ts";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { ServerSettingsService } from "../../../serverSettings.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Option from "effect/Option";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import {
  createParent,
  dispatchUntil,
  temporaryDirectory,
  withServer,
  PARENT_ID,
  NOW,
  session,
} from "./handlers.testFixtures.ts";

/** Real toolkit subscriptions/commands/projections; only provider capacity and settings storage are fake. */
describe("Wight toolkit wiring", () => {
  it.effect(
    "a legacy usage resume loses admission when Wight takes quota ownership after its last check",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-wight-legacy-");
        let current = DEFAULT_SERVER_SETTINGS;
        let usage = 100;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const watcherRead = yield* Deferred.make<void>();
        const settings = Layer.mock(ServerSettingsService)({
          getSettings: Effect.sync(() => current),
          streamChanges: Stream.empty,
          subscribeChanges: Effect.succeed(Stream.empty),
        });
        const registry = Layer.mock(ProviderRegistry)({
          getProviders: Effect.sync(() => [
            {
              instanceId: ProviderInstanceId.make("codex"),
              enabled: true,
              usageLimits: {
                checkedAt: NOW,
                windows: [
                  {
                    id: "session",
                    kind: "session",
                    label: "Session",
                    usedPercent: usage,
                    resetsAt: "1970-01-01T01:00:00.000Z",
                  },
                ],
              },
            } as unknown as ServerProvider,
          ]),
          streamChanges: Stream.empty,
        });
        yield* withServer(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            const engine = yield* OrchestrationEngineService;
            yield* engine.dispatch({
              type: "thread.turn.start",
              commandId: CommandId.make("legacy-initial"),
              threadId: PARENT_ID,
              message: {
                messageId: MessageId.make("legacy-initial"),
                role: "user",
                text: "Work",
                attachments: [],
              },
              runtimeMode: "full-access",
              interactionMode: "default",
              createdAt: NOW,
            });
            yield* engine.dispatch(session(PARENT_ID, "running", "limited"));
            yield* engine.dispatch(
              session(PARENT_ID, "error", "limited", "Codex usage limit reached"),
            );
            yield* Deferred.await(watcherRead);
            yield* TestClock.adjust(6_000);
            usage = 20;
            const advancing = yield* Effect.forkChild(TestClock.adjust(3_700_000));
            // Toolkit has already checked Wight off and constructed its dispatch at this point.
            yield* Deferred.await(entered);
            current = {
              ...DEFAULT_SERVER_SETTINGS,
              wightModes: { [PARENT_ID]: { enabledAt: NOW, expiresAt: null } },
              providerInstances: {
                [ProviderInstanceId.make("codex")]: {
                  driver: ProviderDriverKind.make("codex"),
                  wightLimitPercent: 10,
                },
              },
            };
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(advancing);
            // Drain through a later real engine command, without a sleep or polling.
            yield* engine.dispatch(session(PARENT_ID, "ready", null));
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages.map(
                (message) => message.text,
              ),
            ).toEqual(["Work"]);
          }),
          () => Deferred.succeed(watcherRead, undefined).pipe(Effect.asVoid),
          { services: Layer.merge(settings, registry) },
          (command) =>
            command.type === "thread.turn.start" && command.message.text === RESUME_TEXT
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        );
      }).pipe(Effect.scoped),
  );

  it.effect(
    "a live settings toggle starts idle work; active toggles wait for the next idle receipt",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-wight-toolkit-");
        let current = DEFAULT_SERVER_SETTINGS;
        const changes = yield* PubSub.unbounded<typeof current>();
        const settings = ServerSettingsService.of({
          start: Effect.void,
          ready: Effect.void,
          getSettings: Effect.sync(() => current),
          updateSettings: (patch) =>
            Effect.gen(function* () {
              current = applyServerSettingsPatch(current, patch);
              yield* PubSub.publish(changes, current);
              return current;
            }),
          streamChanges: Stream.fromPubSub(changes),
          subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
        });
        let provider = {
          instanceId: ProviderInstanceId.make("codex"),
          enabled: true,
          usageLimits: {
            checkedAt: NOW,
            windows: [{ id: "session", kind: "session", label: "Session", usedPercent: 20 }],
          },
        } as unknown as ServerProvider;
        const providerChanges = yield* PubSub.unbounded<ReadonlyArray<ServerProvider>>();
        const setUsage = (usedPercent: number) =>
          Effect.gen(function* () {
            provider = {
              ...provider,
              usageLimits: {
                checkedAt: NOW,
                windows: [{ id: "session", kind: "session", label: "Session", usedPercent }],
              },
            };
            yield* PubSub.publish(providerChanges, [provider]);
          });
        const registry = Layer.mock(ProviderRegistry)({
          getProviders: Effect.sync(() => [provider]),
          streamChanges: Stream.fromPubSub(providerChanges),
        });
        yield* withServer(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            const first = yield* dispatchUntil(
              settings.updateSettings({
                wightModes: { [PARENT_ID]: { enabledAt: NOW, expiresAt: null } },
              }),
              (event) =>
                event.type === "thread.turn-start-requested" && event.aggregateId === PARENT_ID,
            );
            expect(first.event.type).toBe("thread.turn-start-requested");
            const engine = yield* OrchestrationEngineService;
            yield* engine.dispatch(session(PARENT_ID, "running", "first"));
            yield* settings.updateSettings({ wightModes: { [PARENT_ID]: null } });
            yield* settings.updateSettings({
              wightModes: {
                [PARENT_ID]: { enabledAt: "2026-01-01T00:01:00.000Z", expiresAt: null },
              },
            });
            const second = yield* dispatchUntil(
              engine.dispatch(session(PARENT_ID, "ready", null)),
              (event) =>
                event.type === "thread.turn-start-requested" && event.aggregateId === PARENT_ID,
            );
            expect(second.event.sequence).toBeGreaterThan(first.event.sequence);
            yield* engine.dispatch(session(PARENT_ID, "running", "second"));
            yield* setUsage(90);
            yield* engine.dispatch(session(PARENT_ID, "ready", null));
            const resumed = yield* dispatchUntil(
              setUsage(20),
              (event) =>
                event.type === "thread.turn-start-requested" && event.aggregateId === PARENT_ID,
            );
            expect(resumed.event.sequence).toBeGreaterThan(second.event.sequence);
            for (let index = 0; index < 3; index++) {
              yield* engine.dispatch(session(PARENT_ID, "running", `continued-${index}`));
              yield* dispatchUntil(
                engine.dispatch(session(PARENT_ID, "ready", null)),
                (event) =>
                  event.type === "thread.turn-start-requested" && event.aggregateId === PARENT_ID,
              );
            }
            const query = yield* ProjectionSnapshotQuery;
            expect(
              Option.getOrThrow(yield* query.getThreadDetailById(PARENT_ID)).messages,
            ).toHaveLength(6);
            yield* settings.updateSettings({ wightModes: { [PARENT_ID]: null } });
          }),
          undefined,
          { services: Layer.mergeAll(Layer.succeed(ServerSettingsService, settings), registry) },
        );
      }).pipe(Effect.scoped),
  );
});
