import { AuthRenewalLocalRecord } from "@t3tools/contracts";
import { RenewalRecordStore, ConnectionTransientError } from "@t3tools/client-runtime/connection";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SecureStore from "expo-secure-store";

const failure = () =>
  new ConnectionTransientError({
    reason: "transport",
    detail: "Device renewal storage is unavailable.",
  });
const make = Effect.gen(function* () {
  const lock = yield* Semaphore.make(1);
  const schema = Schema.fromJsonString(AuthRenewalLocalRecord);
  const keyFor = (key: string) => `t3code.renewal.${renewalCommitment(key)}`;
  const read = (key: string) =>
    Effect.tryPromise({ try: () => SecureStore.getItemAsync(keyFor(key)), catch: failure }).pipe(
      Effect.flatMap((raw) =>
        raw === null
          ? Effect.succeed(null)
          : Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.mapError(failure)),
      ),
    );
  return RenewalRecordStore.RenewalRecordStore.of({
    read,
    compareAndSet: (key, revision, value) =>
      lock.withPermit(
        Effect.gen(function* () {
          const old = yield* read(key);
          if ((old?.revision ?? null) !== revision) return false;
          const encoded = yield* Schema.encodeEffect(schema)(value).pipe(Effect.mapError(failure));
          yield* Effect.tryPromise({
            try: () => SecureStore.setItemAsync(keyFor(key), encoded),
            catch: failure,
          });
          return true;
        }),
      ),
  });
});
export const layer = Layer.effect(RenewalRecordStore.RenewalRecordStore, make);
