import { DEFAULT_WIGHT_LIMIT_PERCENT, type OrchestrationV2ServerCommand } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { keepWarmPaused } from "../keepWarm/keepWarm.ts";
import { wightPaused } from "./wightMode.ts";

type IdleCommand = Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>;

/** Settings and provider admission runs under dispatch serialization, outside SQL guards. */
export class AdmissionHooks extends Context.Reference<{
  readonly permits: (command: IdleCommand) => Effect.Effect<boolean>;
}>("t3/wight/AdmissionHooks", {
  defaultValue: () => ({
    permits: (command) => Effect.succeed(command.idleAdmission === undefined),
  }),
}) {}

export const layer = Layer.effect(
  AdmissionHooks,
  Effect.gen(function* () {
    const settings = yield* ServerSettings.ServerSettingsService;
    const registry = yield* ProviderRegistry.ProviderRegistry;
    return {
      permits: Effect.fn("IdleMode.permits")(
        function* (command: IdleCommand) {
          const admission = command.idleAdmission;
          if (admission === undefined) return true;
          const current = yield* settings.getSettings;
          const active = current.threadIdleModes[command.threadId];
          if (
            active === undefined ||
            active.type !== admission.mode ||
            active.enabledAt !== admission.enabledAt
          )
            return false;
          const provider = (yield* registry.getProviders).find(
            (entry) => entry.instanceId === admission.providerInstanceId,
          );
          if (active.type === "keepWarm")
            return active.stopped === null && !keepWarmPaused(provider);
          const now = yield* Clock.currentTimeMillis;
          if (active.expiresAt !== null && active.expiresAt <= now) return false;
          return !wightPaused(
            provider,
            current.providerInstances[admission.providerInstanceId]?.wightLimitPercent ??
              DEFAULT_WIGHT_LIMIT_PERCENT,
          );
        },
        Effect.catchCause(() => Effect.succeed(false)),
      ),
    };
  }),
);
