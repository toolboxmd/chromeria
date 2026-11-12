import type { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

/** Wake the link dialer after a durable link change. Startup also scans stored links. */
export class PeerLinkEvents extends Context.Service<
  PeerLinkEvents,
  {
    readonly changed: (environmentId: EnvironmentId) => Effect.Effect<void>;
    readonly changes: Stream.Stream<EnvironmentId>;
    readonly automationChanged: Effect.Effect<void>;
    readonly subscribeAutomationChanges: Effect.Effect<
      PubSub.Subscription<void>,
      never,
      Scope.Scope
    >;
  }
>()("t3/peer/PeerLinkEvents") {}
export const layer = Layer.effect(
  PeerLinkEvents,
  Effect.gen(function* () {
    const changes = yield* PubSub.sliding<EnvironmentId>(64);
    const automation = yield* PubSub.sliding<void>(1);
    return PeerLinkEvents.of({
      changed: Effect.fn("PeerLinkEvents.changed")((id) =>
        PubSub.publish(changes, id).pipe(Effect.asVoid),
      ),
      changes: Stream.fromPubSub(changes),
      automationChanged: PubSub.publish(automation, undefined).pipe(Effect.asVoid),
      subscribeAutomationChanges: PubSub.subscribe(automation),
    });
  }),
);
