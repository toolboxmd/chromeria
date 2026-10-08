import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as ProviderRegistryModule from "./ProviderRegistry.ts";
import * as AdmissionGate from "./providerAdmissionGate.ts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { ProviderAdapterV2SessionRuntime } from "../orchestration-v2/ProviderAdapter.ts";
import type { ProviderAdapterRegistryV2Shape } from "../orchestration-v2/ProviderAdapterRegistry.ts";
import {
  ProviderSessionOpenError,
  type ProviderSessionManagerV2Shape,
} from "../orchestration-v2/ProviderSessionManager.ts";
import type { ProviderRegistry } from "./ProviderRegistry.ts";
import type { ProviderAdmissionGateShape } from "./providerAdmissionGate.ts";

/** One production seam covers process opening and starts on already-open sessions. */
function admitProviderStarts(
  sessions: ProviderSessionManagerV2Shape,
  registry: ProviderAdapterRegistryV2Shape,
  admission: ProviderAdmissionGateShape,
  providers: ProviderRegistry["Service"],
): ProviderSessionManagerV2Shape {
  const keyFor = (
    instanceId: Parameters<
      ProviderRegistry["Service"]["getProviderMaintenanceCapabilitiesForInstance"]
    >[0],
    driver: Parameters<
      ProviderRegistry["Service"]["getProviderMaintenanceCapabilitiesForInstance"]
    >[1],
  ) =>
    providers
      .getProviderMaintenanceCapabilitiesForInstance(instanceId, driver)
      .pipe(
        Effect.map(
          (capabilities) => `${driver}:${capabilities.update?.lockKey ?? "no-managed-install"}`,
        ),
      );
  const start = <A, E, R>(
    session: ProviderAdapterV2SessionRuntime,
    effect: Effect.Effect<A, E, R>,
  ) =>
    keyFor(session.instanceId, session.driver).pipe(
      Effect.flatMap((key) => admission.withStart(key, effect)),
    );
  const runtimes = new WeakMap<ProviderAdapterV2SessionRuntime, ProviderAdapterV2SessionRuntime>();
  const runtime = (session: ProviderAdapterV2SessionRuntime): ProviderAdapterV2SessionRuntime => {
    const existing = runtimes.get(session);
    if (existing !== undefined) return existing;
    const wrapped: ProviderAdapterV2SessionRuntime = {
      ...session,
      startTurn: (input) => start(session, session.startTurn(input)),
      ...(session.compactThread === undefined
        ? {}
        : {
            compactThread: (
              input: Parameters<NonNullable<ProviderAdapterV2SessionRuntime["compactThread"]>>[0],
            ) => start(session, session.compactThread!(input)),
          }),
    };
    runtimes.set(session, wrapped);
    return wrapped;
  };
  return {
    ...sessions,
    open: (input) =>
      registry.get(input.modelSelection.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderSessionOpenError({
              instanceId: input.modelSelection.instanceId,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
        Effect.flatMap((adapter) =>
          keyFor(input.modelSelection.instanceId, adapter.driver).pipe(
            Effect.flatMap((key) => admission.withStart(key, sessions.open(input))),
          ),
        ),
        Effect.map(runtime),
      ),
    get: (id) => sessions.get(id).pipe(Effect.map(Option.map(runtime))),
  };
}

export class ProviderStartAdmission extends Context.Reference<{
  readonly wrap: (
    sessions: ProviderSessionManagerV2Shape,
    registry: ProviderAdapterRegistryV2Shape,
  ) => ProviderSessionManagerV2Shape;
}>("t3/provider/ProviderStartAdmission", {
  defaultValue: () => ({ wrap: (sessions) => sessions }),
}) {}

/** The server explicitly installs this wrapper; standalone upstream compositions stay unchanged. */
export const layer = Layer.effect(
  ProviderStartAdmission,
  Effect.gen(function* () {
    const gate = yield* AdmissionGate.ProviderAdmissionGate;
    const providers = yield* ProviderRegistryModule.ProviderRegistry;
    return {
      wrap: (sessions: ProviderSessionManagerV2Shape, registry: ProviderAdapterRegistryV2Shape) =>
        admitProviderStarts(sessions, registry, gate, providers),
    };
  }),
).pipe(Layer.provide(AdmissionGate.layer));

export const applyProviderStartAdmission = (
  sessions: ProviderSessionManagerV2Shape,
  registry: ProviderAdapterRegistryV2Shape,
) => Effect.map(ProviderStartAdmission, (admission) => admission.wrap(sessions, registry));
