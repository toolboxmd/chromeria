// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Native callbacks and cancellable queue waits use the host event loop.
import {
  PREVIEW_WATCH_MAX_PAYLOAD_BYTES,
  PREVIEW_WATCH_MAX_EVENTS,
  PREVIEW_WATCH_MAX_BYTES,
  PREVIEW_WATCH_TOTAL_BYTES,
  PREVIEW_WATCH_MAX_WATCHES,
  PREVIEW_WATCH_MAX_RECORDS,
  PREVIEW_WATCH_MAX_READ_EVENTS,
  PREVIEW_WATCH_MAX_READ_BYTES,
  PreviewWatchError,
  type PreviewWatchReadInput,
  type PreviewWatchReadResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

type Reason = PreviewWatchError["reason"];
type Event = { sequence: number; json: string; bytes: number };
type Active = {
  state: "active";
  events: Event[];
  bytes: number;
  sequence: number;
  pendingCount: number;
  pendingBytes: number;
  tail: Promise<void>;
  authorize: () => Promise<void>;
  fence: () => Reason | undefined;
  cleanup: () => Promise<void>;
};
type Record = {
  threadId: string;
  agentSessionId: string | undefined;
  state: Active | { state: "terminal"; reason: Reason };
  listeners: Set<() => void>;
};
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json));

/** In-memory retention only. A process epoch and terminal records distinguish loss from silence. */
export class PageWatchStore {
  private readonly records = new Map<string, Record>();
  private bytes = 0;
  private pendingBytes = 0;
  private readers = 0;

  readonly epoch: string;
  constructor(epoch: string) {
    this.epoch = epoch;
  }

  register(
    watchId: string,
    threadId: string,
    options: Pick<Active, "authorize" | "fence" | "cleanup">,
    agentSessionId?: string,
  ) {
    if (
      [...this.records.values()].filter((record) => record.state.state === "active").length >=
      PREVIEW_WATCH_MAX_WATCHES
    )
      throw new PreviewWatchError({ reason: "capacity" });
    if (this.records.size >= PREVIEW_WATCH_MAX_RECORDS) {
      const expired = [...this.records].find(([, record]) => record.state.state === "terminal");
      if (expired) this.records.delete(expired[0]);
      else throw new PreviewWatchError({ reason: "capacity" });
    }
    this.records.set(watchId, {
      threadId,
      agentSessionId,
      state: {
        state: "active",
        events: [],
        bytes: 0,
        sequence: 0,
        pendingCount: 0,
        pendingBytes: 0,
        tail: Promise.resolve(),
        ...options,
      },
      listeners: new Set(),
    });
    return { watchId, epoch: this.epoch, cursor: 0 };
  }

  private notify(record: Record) {
    for (const listener of record.listeners) listener();
  }

  terminate(watchId: string, reason: Reason) {
    const record = this.records.get(watchId);
    if (!record || record.state.state === "terminal") return Promise.resolve();
    const active = record.state;
    this.bytes -= active.bytes;
    active.events = [];
    active.bytes = 0;
    record.state = { state: "terminal", reason };
    this.notify(record);
    // The fence is installed before any native cleanup can wait or fail.
    return active.cleanup();
  }

  terminateThread(threadId: string, reason: Reason) {
    for (const [id, record] of this.records)
      if (record.threadId === threadId) void this.terminate(id, reason).catch(() => {});
  }

  active(watchId: string) {
    return this.records.get(watchId)?.state.state === "active";
  }

  private exact(input: { threadId: string; watchId: string; epoch: string }) {
    if (input.epoch !== this.epoch) throw new PreviewWatchError({ reason: "stale_epoch" });
    const record = this.records.get(input.watchId);
    if (!record || record.threadId !== input.threadId)
      throw new PreviewWatchError({ reason: "unknown" });
    return record;
  }

  async stop(input: { threadId: string; watchId: string; epoch: string; agentSessionId?: string }) {
    const record = this.exact(input);
    if (record.agentSessionId !== input.agentSessionId)
      throw new PreviewWatchError({ reason: "ownership" });
    await this.terminate(input.watchId, "stopped");
    return { watchId: input.watchId, epoch: this.epoch, state: "stopped" as const };
  }

  private async validate(watchId: string, record: Record) {
    const active = record.state;
    if (active.state === "terminal") throw new PreviewWatchError({ reason: active.reason });
    const fenced = active.fence();
    if (fenced) {
      void this.terminate(watchId, fenced).catch(() => {});
      throw new PreviewWatchError({ reason: fenced });
    }
    try {
      await active.authorize();
    } catch {
      void this.terminate(watchId, "revoked").catch(() => {});
      throw new PreviewWatchError({ reason: "revoked" });
    }
    const after = active.fence();
    if (after) void this.terminate(watchId, after).catch(() => {});
    if (record.state !== active)
      throw new PreviewWatchError({
        reason: record.state.state === "terminal" ? record.state.reason : "unknown",
      });
    return active;
  }

  /** Admission is synchronous and bounded, including callbacks waiting on authority I/O. */
  emit(watchId: string, json: string): Promise<void> {
    const record = this.records.get(watchId);
    if (!record || record.state.state !== "active") return Promise.resolve();
    const active = record.state;
    const bytes = Buffer.byteLength(json, "utf8");
    const fail = (reason: Reason) => {
      void this.terminate(watchId, reason).catch(() => {});
      return Promise.resolve();
    };
    if (bytes > PREVIEW_WATCH_MAX_PAYLOAD_BYTES) return fail("overflow");
    if (
      active.pendingCount + active.events.length >= PREVIEW_WATCH_MAX_EVENTS ||
      active.bytes + active.pendingBytes + bytes > PREVIEW_WATCH_MAX_BYTES ||
      this.bytes + this.pendingBytes + bytes > PREVIEW_WATCH_TOTAL_BYTES
    )
      return fail("overflow");
    try {
      decodeJson(json);
    } catch {
      return fail("malformed_payload");
    }
    active.pendingCount++;
    active.pendingBytes += bytes;
    this.pendingBytes += bytes;
    const work = active.tail.then(async () => {
      try {
        if (record.state !== active) return;
        await this.validate(watchId, record);
        if (record.state !== active) return;
        active.events.push({ sequence: ++active.sequence, json, bytes });
        active.bytes += bytes;
        this.bytes += bytes;
        this.notify(record);
      } catch {
        // validate has already retained the specific terminal reason.
      } finally {
        active.pendingCount--;
        active.pendingBytes -= bytes;
        this.pendingBytes -= bytes;
      }
    });
    active.tail = work;
    return work;
  }

  async flush(watchId: string) {
    const record = this.records.get(watchId);
    if (!record) throw new PreviewWatchError({ reason: "unknown" });
    if (record.state.state === "active") await record.state.tail;
    await this.validate(watchId, record);
  }

  async read(
    input: PreviewWatchReadInput,
    authorizeReader: () => Promise<void>,
    signal: AbortSignal,
  ): Promise<PreviewWatchReadResult> {
    if (this.readers >= PREVIEW_WATCH_MAX_WATCHES)
      throw new PreviewWatchError({ reason: "capacity" });
    this.readers++;
    try {
      return await this.readRetained(input, authorizeReader, signal);
    } finally {
      this.readers--;
    }
  }

  private async readRetained(
    input: PreviewWatchReadInput,
    authorizeReader: () => Promise<void>,
    signal: AbortSignal,
  ): Promise<PreviewWatchReadResult> {
    await authorizeReader();
    const record = this.exact(input);
    let active = await this.validate(input.watchId, record);
    if (input.after > active.sequence) throw new PreviewWatchError({ reason: "gap" });
    if (input.after === active.sequence && (input.timeoutMs ?? 0) > 0) {
      await new Promise<void>((resolve, reject) => {
        const done = () => {
          clearTimeout(timer);
          record.listeners.delete(done);
          signal.removeEventListener("abort", abort);
          resolve();
        };
        const abort = () => {
          clearTimeout(timer);
          record.listeners.delete(done);
          signal.removeEventListener("abort", abort);
          reject(signal.reason);
        };
        const timer = setTimeout(done, input.timeoutMs);
        record.listeners.add(done);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
        else if (record.state !== active || active.sequence > input.after) done();
      });
    }
    // Revocation and deletion while waiting must fence both queued data and empty replies.
    await authorizeReader();
    active = await this.validate(input.watchId, record);
    const events: PreviewWatchReadResult["events"][number][] = [];
    let bytes = 0;
    for (const event of active.events) {
      if (event.sequence <= input.after) continue;
      if (
        events.length >= PREVIEW_WATCH_MAX_READ_EVENTS ||
        bytes + event.bytes > PREVIEW_WATCH_MAX_READ_BYTES
      )
        break;
      events.push({ sequence: event.sequence, value: decodeJson(event.json) });
      bytes += event.bytes;
    }
    return {
      epoch: this.epoch,
      state: "active",
      events,
      cursor: events.at(-1)?.sequence ?? input.after,
    };
  }
}
