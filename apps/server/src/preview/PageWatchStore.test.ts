import { expect, it } from "vite-plus/test";
import {
  ThreadId,
  PREVIEW_WATCH_MAX_EVENTS,
  PREVIEW_WATCH_MAX_WATCHES,
  PREVIEW_WATCH_MAX_RECORDS,
} from "@t3tools/contracts";
import { PageWatchStore } from "./PageWatchStore.ts";

const threadId = ThreadId.make("synthetic-thread");
const controller = () => new AbortController();
const permit = async () => {};
const create = (store: PageWatchStore, watchId = "watch", options = {}) =>
  store.register(watchId, threadId, {
    authorize: permit,
    fence: () => undefined,
    cleanup: permit,
    ...options,
  });
const input = (after = 0, watchId = "watch") => ({
  threadId,
  watchId,
  epoch: "process-a",
  after,
  timeoutMs: 0,
});
const read = (store: PageWatchStore, after = 0, watchId = "watch") =>
  store.read(input(after, watchId), permit, controller().signal);

it("baseline starts at one, replay is non-consuming, and batches never skip unseen events", async () => {
  const store = new PageWatchStore("process-a");
  expect(create(store).cursor).toBe(0);
  await Promise.all(
    Array.from({ length: 100 }, (_, index) => store.emit("watch", JSON.stringify({ index }))),
  );
  const first = await read(store);
  expect(first.events.map((e) => e.sequence)).toEqual(Array.from({ length: 64 }, (_, i) => i + 1));
  expect(first.cursor).toBe(64);
  expect(await read(store)).toEqual(first);
  const second = await read(store, first.cursor);
  expect(second.events.map((e) => e.value)).toEqual(
    Array.from({ length: 36 }, (_, index) => ({ index: index + 64 })),
  );
  expect(second.cursor).toBe(100);
  expect(await read(store, 100)).toEqual({
    epoch: "process-a",
    state: "active",
    events: [],
    cursor: 100,
  });
  await expect(read(store, 101)).rejects.toMatchObject({ reason: "gap" });
});

it("a concurrent waiting read observes an emission and rechecks reader authority", async () => {
  const store = new PageWatchStore("process-a");
  create(store);
  const started = Promise.withResolvers<void>();
  let checks = 0;
  const waiting = store.read(
    { ...input(), timeoutMs: 30000 },
    async () => {
      checks++;
      started.resolve();
    },
    controller().signal,
  );
  await started.promise;
  await store.emit("watch", '{"change":1}');
  expect((await waiting).cursor).toBe(1);
  expect(checks).toBe(2);
});

it("revocation during a bounded wait cannot return queued data", async () => {
  const store = new PageWatchStore("process-a");
  create(store);
  const started = Promise.withResolvers<void>();
  let allowed = true;
  const waiting = store.read(
    { ...input(), timeoutMs: 30000 },
    async () => {
      if (!allowed) throw new Error("revoked reader");
      started.resolve();
    },
    controller().signal,
  );
  await started.promise;
  allowed = false;
  await store.emit("watch", "1");
  await expect(waiting).rejects.toThrow("revoked reader");
});

it("cancellation releases a bounded wait and leaves the stream available", async () => {
  const store = new PageWatchStore("process-a");
  create(store);
  const abort = controller();
  const admitted = Promise.withResolvers<void>();
  const waiting = store.read(
    { ...input(), timeoutMs: 30000 },
    async () => {
      admitted.resolve();
    },
    abort.signal,
  );
  await admitted.promise;
  abort.abort(new Error("cancelled"));
  await expect(waiting).rejects.toThrow("cancelled");
  await store.emit("watch", "1");
  expect((await read(store)).cursor).toBe(1);
});

it.each(["malformed_payload", "overflow"] as const)(
  "%s is terminal independently of payload retention",
  async (reason) => {
    const store = new PageWatchStore("process-a");
    create(store);
    await store.emit("watch", "1");
    await store.emit(
      "watch",
      reason === "overflow" ? JSON.stringify("x".repeat(1024 * 1024)) : "{broken",
    );
    await expect(read(store)).rejects.toMatchObject({ reason });
    await store.emit("watch", "2");
    await expect(read(store)).rejects.toMatchObject({ reason });
  },
);

it("caps count and in-flight bytes before authority I/O can accumulate", async () => {
  const store = new PageWatchStore("process-a");
  const gate = Promise.withResolvers<void>();
  create(store, "watch", { authorize: () => gate.promise });
  const emissions = Array.from({ length: PREVIEW_WATCH_MAX_EVENTS + 1 }, () =>
    store.emit("watch", "1"),
  );
  gate.resolve();
  await Promise.all(emissions);
  await expect(read(store)).rejects.toMatchObject({ reason: "overflow" });
});

it("retains 100 KiB snapshots unchanged and caps bytes per reply and across watches", async () => {
  const store = new PageWatchStore("process-a");
  create(store);
  const snapshot = { records: "s".repeat(100 * 1024) };
  await store.emit("watch", JSON.stringify(snapshot));
  expect((await read(store)).events[0]?.value).toEqual(snapshot);
  const json = JSON.stringify("x".repeat(1024 * 1024 - 10));
  for (let i = 0; i < 8; i++) {
    create(store, `bulk-${i}`);
    for (let j = 0; j < 8; j++) await store.emit(`bulk-${i}`, json);
  }
  expect((await read(store, 0, "bulk-0")).cursor).toBe(2);
  await expect(read(store, 0, "bulk-7")).rejects.toMatchObject({ reason: "overflow" });
});

it("bounds watch records, evicts only terminal tombstones, and explicitly rejects old epochs and wrong hosts", async () => {
  const store = new PageWatchStore("process-a");
  for (let i = 0; i < PREVIEW_WATCH_MAX_WATCHES; i++) create(store, `active-${i}`);
  expect(() => create(store)).toThrow();
  await store.stop({ ...input(), watchId: "active-0" });
  for (let i = 0; i < PREVIEW_WATCH_MAX_RECORDS; i++) {
    create(store, `retired-${i}`);
    await store.stop({ ...input(), watchId: `retired-${i}` });
  }
  await expect(read(store, 0, "active-0")).rejects.toMatchObject({ reason: "unknown" });
  await expect(
    store.read({ ...input(0, "active-1"), epoch: "old" }, permit, controller().signal),
  ).rejects.toMatchObject({ reason: "stale_epoch" });
  await expect(
    store.read(
      { ...input(0, "active-1"), threadId: ThreadId.make("other") },
      permit,
      controller().signal,
    ),
  ).rejects.toMatchObject({ reason: "unknown" });
});

it("fences late queued callbacks when ownership changes while authority is waiting", async () => {
  const store = new PageWatchStore("process-a");
  const gate = Promise.withResolvers<void>();
  let fenced = false;
  let cleaned = 0;
  create(store, "watch", {
    authorize: () => gate.promise,
    fence: () => (fenced ? "ownership" : undefined),
    cleanup: async () => {
      cleaned++;
    },
  });
  const event = store.emit("watch", "1");
  fenced = true;
  gate.resolve();
  await event;
  await expect(read(store)).rejects.toMatchObject({ reason: "ownership" });
  expect(cleaned).toBe(1);
});

it("bounds concurrent reads even while authorization is waiting and releases admission afterwards", async () => {
  const store = new PageWatchStore("process-a");
  create(store);
  const gate = Promise.withResolvers<void>();
  const reads = Array.from({ length: PREVIEW_WATCH_MAX_WATCHES }, () =>
    store.read(input(), () => gate.promise, controller().signal),
  );
  await expect(read(store)).rejects.toMatchObject({ reason: "capacity" });
  gate.resolve();
  await Promise.all(reads);
  expect((await read(store)).cursor).toBe(0);
});
