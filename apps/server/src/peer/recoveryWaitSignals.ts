import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

/** Recovery decisions can change without a provider event, such as opting out of a reset. */
export class RecoveryWaitSignals extends Context.Reference<{
  readonly publish: (threadId: ThreadId) => Effect.Effect<void>;
  readonly subscribe: Effect.Effect<Stream.Stream<ThreadId>, never, Scope.Scope>;
}>("t3/peer/RecoveryWaitSignals", {
  defaultValue: () => ({ publish: () => Effect.void, subscribe: Effect.succeed(Stream.never) }),
}) {}

export const layerSignals = Layer.effect(
  RecoveryWaitSignals,
  Effect.gen(function* () {
    const changes = yield* PubSub.unbounded<ThreadId>();
    return {
      publish: (threadId: ThreadId) => PubSub.publish(changes, threadId).pipe(Effect.asVoid),
      subscribe: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
    };
  }),
);
