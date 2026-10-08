import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

export interface ProviderAdmissionGateShape {
  readonly withStart: <A, E, R>(
    installKey: string,
    start: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly withUpdate: <A, E, R>(input: {
    readonly installKey: string;
    readonly isBusy: Effect.Effect<boolean, E, R>;
    readonly install: Effect.Effect<A, E, R>;
  }) => Effect.Effect<Option.Option<A>, E, R>;
}

export class ProviderAdmissionGate extends Context.Service<
  ProviderAdmissionGate,
  ProviderAdmissionGateShape
>()("t3/provider/providerAdmissionGate") {}

// The turnstile prevents new starts overtaking a waiting update. Starts consume
// one shared permit; an update holds every permit through its busy check/install.
const sharedPermits = Number.MAX_SAFE_INTEGER;

interface AdmissionEntry {
  readonly turnstile: Semaphore.Semaphore;
  readonly shared: Semaphore.Semaphore;
  readonly users: number;
}

export const make = Effect.gen(function* () {
  const entries = yield* Ref.make<ReadonlyMap<string, AdmissionEntry>>(new Map());
  const acquire = (key: string) =>
    Effect.gen(function* () {
      const turnstile = yield* Semaphore.make(1);
      const shared = yield* Semaphore.make(sharedPermits);
      return yield* Ref.modify(entries, (current) => {
        const entry = current.get(key) ?? { turnstile, shared, users: 0 };
        const next = new Map(current);
        next.set(key, { ...entry, users: entry.users + 1 });
        return [entry, next] as const;
      });
    });
  const release = (key: string) =>
    Ref.update(entries, (current) => {
      const entry = current.get(key);
      if (entry === undefined) return current;
      const next = new Map(current);
      if (entry.users === 1) next.delete(key);
      else next.set(key, { ...entry, users: entry.users - 1 });
      return next;
    });
  const withEntry = <A, E, R>(
    key: string,
    use: (entry: AdmissionEntry) => Effect.Effect<A, E, R>,
  ) => Effect.acquireUseRelease(acquire(key), use, () => release(key));
  return ProviderAdmissionGate.of({
    withStart: (key, start) =>
      withEntry(key, (entry) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.acquireUseRelease(
            restore(entry.turnstile.withPermit(entry.shared.take(1))),
            () => restore(start),
            () => entry.shared.release(1),
          ),
        ),
      ),
    withUpdate: ({ installKey, isBusy, install }) =>
      withEntry(installKey, (entry) =>
        entry.turnstile.withPermit(
          entry.shared.withPermits(sharedPermits)(
            Effect.flatMap(isBusy, (busy) =>
              busy ? Effect.succeed(Option.none()) : Effect.map(install, Option.some),
            ),
          ),
        ),
      ),
  });
});

export const layer = Layer.effect(ProviderAdmissionGate, make);
