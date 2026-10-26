import type { PreviewAutomationControlReason } from "@t3tools/contracts";

export class BrowserControlInterrupted extends Error {
  readonly reason: PreviewAutomationControlReason;
  constructor(
    message = "Browser control changed. Refresh the snapshot before trying again.",
    reason: PreviewAutomationControlReason = "interrupted",
  ) {
    super(message);
    this.reason = reason;
  }
}

const settle = (work: Promise<unknown>) =>
  work.then(
    () => undefined,
    () => undefined,
  );

/** Reserves control immediately, but drains running work before the new owner can act. */
export class SessionControl {
  readonly agentId: string | null;
  private readonly onGenerationChange: () => void;
  private tail: Promise<unknown> = Promise.resolve();
  private pending: Promise<void> = Promise.resolve();
  private readonly handoff = new Set<Promise<void>>();
  private owner: string | null = null;
  private epoch = 0;
  private closed = false;
  private readonly inputs = {
    action: { count: 0, bytes: 0, maxCount: 64, maxBytes: 256 * 1024 },
    control: { count: 0, bytes: 0, maxCount: 8, maxBytes: 64 * 1024 },
    dialog: { count: 0, bytes: 0, maxCount: 8, maxBytes: 64 * 1024 },
  };

  constructor(agentId: string | null, onGenerationChange: () => void = () => {}) {
    this.agentId = agentId;
    this.onGenerationChange = onGenerationChange;
  }

  get controller() {
    return this.owner;
  }

  get generation() {
    return this.epoch;
  }

  // Tab-wide, including old connections still draining native promises. Keep
  // separate room for dialog replies: ownership changes also wait on the tail.
  reserveInput(bytes: number, lane: "action" | "control" | "dialog"): (() => void) | null {
    const budget = this.inputs[lane];
    if (
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      budget.count >= budget.maxCount ||
      budget.bytes + bytes > budget.maxBytes
    )
      return null;
    budget.count++;
    budget.bytes += bytes;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      budget.count--;
      budget.bytes -= bytes;
    };
  }

  private enqueue<A>(run: () => Promise<A>): Promise<A> {
    const result = this.tail.then(run);
    this.tail = result.then(
      () => this.pending,
      () => this.pending,
    );
    return result;
  }

  /** An action can respond before its navigation commits, while later actions still wait. */
  track(work: Promise<unknown>) {
    this.pending = Promise.all([this.pending, settle(work)]).then(() => undefined);
  }

  /** Viewer input stays responsive, but control handoffs still drain its navigations. */
  trackUntilHandoff(work: Promise<unknown>) {
    const settled = settle(work).finally(() => {
      this.handoff.delete(settled);
    });
    this.handoff.add(settled);
  }

  private assertOpen() {
    if (this.closed) throw new BrowserControlInterrupted("This browser tab is closed.", "closed");
  }

  private changeOwner(owner: string | null) {
    this.owner = owner;
    this.epoch++;
    this.onGenerationChange();
  }

  private async action<A>(
    allowed: () => boolean,
    run: () => Promise<A>,
    deadlineMs?: number,
    authorize?: () => Promise<void>,
  ) {
    this.assertOpen();
    if (!allowed()) throw new BrowserControlInterrupted("You do not control this browser tab.");
    const epoch = this.epoch;
    return this.enqueue(async () => {
      if (authorize) await authorize();
      this.assertOpen();
      if (epoch !== this.epoch || !allowed()) throw new BrowserControlInterrupted();
      if (deadlineMs !== undefined && performance.now() >= deadlineMs) {
        throw new DOMException(
          "This browser request expired before it could start.",
          "TimeoutError",
        );
      }
      return run();
    });
  }

  agentMayAct(agentId: string) {
    return this.agentId === null || this.agentId === agentId;
  }

  /** May act on its own tab or one a human opened, never while a human controls it. */
  agent<A>(
    agentId: string,
    run: () => Promise<A>,
    deadlineMs?: number,
    authorize?: () => Promise<void>,
  ) {
    if (!this.agentMayAct(agentId))
      return Promise.reject(
        new BrowserControlInterrupted("This tab belongs to another agent.", "agentMismatch"),
      );
    if (this.owner !== null)
      return Promise.reject(
        new BrowserControlInterrupted("A human controls this tab.", "humanControl"),
      );
    return this.action(
      () => this.agentMayAct(agentId) && this.owner === null,
      run,
      deadlineMs,
      authorize,
    );
  }

  agentCanActNow(agentId: string) {
    return this.agentMayAct(agentId) && this.owner === null;
  }

  /** Read without control or queueing so agents can observe without delaying human input. */
  observe<A>(run: () => Promise<A>) {
    this.assertOpen();
    return run();
  }

  system<A>(run: () => Promise<A>) {
    this.assertOpen();
    return this.enqueue(async () => {
      this.assertOpen();
      return run();
    });
  }

  human<A>(viewerId: string, run: () => Promise<A>, authorize?: () => Promise<void>) {
    return this.action(() => this.owner === viewerId, run, undefined, authorize);
  }

  async take(viewerId: string, authorize?: () => Promise<void>, onReserved: () => void = () => {}) {
    const admittedGeneration = this.epoch;
    if (authorize) await authorize();
    this.assertOpen();
    if (this.epoch !== admittedGeneration) throw new BrowserControlInterrupted();
    if (this.owner !== null && this.owner !== viewerId) {
      throw new BrowserControlInterrupted("Another viewer controls this browser tab.");
    }
    const changed = this.owner !== viewerId;
    if (changed) this.changeOwner(viewerId);
    const generation = this.epoch;
    try {
      onReserved();
      await this.human(viewerId, async () => {}, authorize);
    } catch (cause) {
      // Retire only this failed reservation, never a later takeover or reopening.
      if (changed && this.owner === viewerId && this.epoch === generation) this.changeOwner(null);
      throw cause;
    }
  }

  async release(viewerId: string, afterDrain: () => Promise<void> = async () => {}) {
    this.assertOpen();
    if (this.owner !== viewerId) {
      throw new BrowserControlInterrupted(
        "Only the controlling viewer can release this browser tab.",
      );
    }
    this.changeOwner(null);
    await this.enqueue(async () => {
      await Promise.all(this.handoff);
      await afterDrain();
    });
  }

  async disconnect(viewerId: string, afterDrain?: () => Promise<void>) {
    if (!this.closed && this.owner === viewerId) await this.release(viewerId, afterDrain);
  }

  async close() {
    if (!this.closed) {
      this.closed = true;
      this.changeOwner(null);
    }
    await this.tail;
  }
}
