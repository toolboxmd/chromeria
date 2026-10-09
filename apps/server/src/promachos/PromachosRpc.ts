import { OrchestrationV2ThreadLaunchError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Adapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Prism from "../prism/PrismService.ts";
import * as PromachosHome from "./PromachosHome.ts";
import * as PromachosLaunch from "./PromachosLaunch.ts";

/** Fork RPC wiring and wire-error mapping stay outside the upstream WebSocket handlers. */
export const makeHandlers = Effect.gen(function* () {
  const launch = yield* PromachosLaunch.PromachosLaunch.pipe(
    Effect.provide(
      PromachosLaunch.layer.pipe(
        Layer.provide(Prism.layer),
        Layer.provide(Receipts.layer),
        Layer.provide(Adapters.layerFromProviderInstanceRegistry),
      ),
    ),
  );
  const home = yield* PromachosHome.PromachosHome.pipe(Effect.provide(PromachosHome.layer));
  return {
    createHome: home.create,
    prepareLaunch: (input: PromachosLaunch.PromachosLaunchInput) =>
      launch.prepare(input).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationV2ThreadLaunchError({
              commandId: input.commandId,
              projectId: input.projectId,
              message: cause.message,
              cause,
            }),
        ),
      ),
  };
});
