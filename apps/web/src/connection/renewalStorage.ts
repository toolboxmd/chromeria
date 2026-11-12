import { AuthRenewalLocalRecord } from "@t3tools/contracts";
import { RenewalRecordStore, ConnectionTransientError } from "@t3tools/client-runtime/connection";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

const decode = Schema.decodeUnknownSync(AuthRenewalLocalRecord);
const failure = () =>
  new ConnectionTransientError({
    reason: "transport",
    detail: "Device renewal storage is unavailable.",
  });

/** Separate database: catalog writes and startup order cannot roll renewal authority back. */
function open(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("t3code:session-renewal", 1);
    request.onupgradeneeded = () => request.result.createObjectStore("records");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
    request.onblocked = () => reject(new Error("Renewal storage upgrade is blocked."));
  });
}

export const layer = Layer.sync(RenewalRecordStore.RenewalRecordStore, () => {
  const bridge = typeof window === "undefined" ? undefined : window.desktopBridge;
  if (bridge?.getRenewalRecord && bridge.compareRenewalRecord) {
    return RenewalRecordStore.RenewalRecordStore.of({
      read: Effect.fn("RenewalRecordStore.read")((key) =>
        Effect.tryPromise({ try: () => bridge.getRenewalRecord!(key), catch: failure }),
      ),
      compareAndSet: Effect.fn("RenewalRecordStore.compareAndSet")((key, revision, value) =>
        Effect.tryPromise({
          try: () => bridge.compareRenewalRecord!({ key, revision, value }),
          catch: failure,
        }),
      ),
    });
  }
  return RenewalRecordStore.RenewalRecordStore.of({
    read: Effect.fn("RenewalRecordStore.read")((key) =>
      Effect.tryPromise({
        try: async () => {
          const database = await open();
          try {
            return await new Promise<AuthRenewalLocalRecord | null>((resolve, reject) => {
              const transaction = database.transaction("records", "readonly");
              const request = transaction.objectStore("records").get(key);
              request.onsuccess = () => {
                try {
                  resolve(request.result === undefined ? null : decode(request.result));
                } catch (error) {
                  reject(error);
                }
              };
              request.onerror = () => reject(request.error);
            });
          } finally {
            database.close();
          }
        },
        catch: failure,
      }),
    ),
    compareAndSet: Effect.fn("RenewalRecordStore.compareAndSet")((key, revision, value) =>
      Effect.tryPromise({
        try: async () => {
          const database = await open();
          try {
            return await new Promise<boolean>((resolve, reject) => {
              const transaction = database.transaction("records", "readwrite");
              const store = transaction.objectStore("records");
              const read = store.get(key);
              let changed = false;
              read.onsuccess = () => {
                try {
                  const old = read.result === undefined ? null : decode(read.result);
                  if ((old?.revision ?? null) === revision) {
                    store.put(decode(value), key);
                    changed = true;
                  }
                } catch (error) {
                  transaction.abort();
                  reject(error);
                }
              };
              transaction.oncomplete = () => resolve(changed);
              transaction.onerror = () => reject(transaction.error);
              transaction.onabort = () => reject(transaction.error);
            });
          } finally {
            database.close();
          }
        },
        catch: failure,
      }),
    ),
  });
});
