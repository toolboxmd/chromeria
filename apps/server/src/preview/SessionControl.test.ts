import { describe, expect, it } from "vite-plus/test";

import { BrowserControlInterrupted, SessionControl } from "./SessionControl.ts";

describe("SessionControl", () => {
  it("replaces waiting hover work without changing the running action or delaying the following click", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const events: Array<number | string> = [];
    const running = control.humanLatest("viewer", "hover:0", undefined, async () => {
      events.push("running");
      started.resolve();
      await finish.promise;
    });
    await started.promise;
    const moves = Array.from({ length: 120 }, (_, position) =>
      control.humanLatest("viewer", "hover:0", undefined, async () => {
        events.push(position);
      }),
    );
    const click = control.human("viewer", async () => {
      events.push("click");
    });
    finish.resolve();
    await Promise.all([running, ...moves, click]);
    expect(events).toEqual(["running", 119, "click"]);
  });

  it.each(["human", "system", "modifiers"] as const)(
    "does not merge waiting hover work across a %s action",
    async (barrier) => {
      const control = new SessionControl("agent");
      await control.take("viewer");
      const finish = Promise.withResolvers<void>();
      const events: string[] = [];
      const running = control.human("viewer", () => finish.promise);
      const before = control.humanLatest("viewer", "hover:0", undefined, async () => {
        events.push("before");
      });
      const recordBarrier = async () => {
        events.push("barrier");
      };
      const between =
        barrier === "modifiers"
          ? control.humanLatest("viewer", "hover:2", undefined, recordBarrier)
          : barrier === "system"
            ? control.system(recordBarrier)
            : control.human("viewer", recordBarrier);
      const after = control.humanLatest("viewer", "hover:0", undefined, async () => {
        events.push("after");
      });
      finish.resolve();
      await Promise.all([running, before, between, after]);
      expect(events).toEqual(["before", "barrier", "after"]);
    },
  );

  it("does not let another viewer replace or split the owner's waiting hover", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const finish = Promise.withResolvers<void>();
    const events: string[] = [];
    const running = control.human("viewer", () => finish.promise);
    const stale = control.humanLatest("viewer", "hover", undefined, async () => {
      events.push("stale");
    });
    await expect(
      control.humanLatest("other", "hover", undefined, async () => {
        events.push("other");
      }),
    ).rejects.toThrow(BrowserControlInterrupted);
    const latest = control.humanLatest("viewer", "hover", undefined, async () => {
      events.push("latest");
    });
    finish.resolve();
    await Promise.all([running, stale, latest]);
    expect(events).toEqual(["latest"]);
  });

  it.each(["release", "disconnect"] as const)(
    "does not revive waiting hover work after %s and retaking control",
    async (handoff) => {
      const control = new SessionControl("agent");
      await control.take("viewer");
      const started = Promise.withResolvers<void>();
      const finish = Promise.withResolvers<void>();
      const events: string[] = [];
      const running = control.human("viewer", () => {
        started.resolve();
        return finish.promise;
      });
      await started.promise;
      const stale = expect(
        control.humanLatest("viewer", "hover", undefined, async () => {
          events.push("stale");
        }),
      ).rejects.toThrow(BrowserControlInterrupted);
      const released = control[handoff]("viewer");
      const retaken = control.take("viewer");
      const fresh = control.humanLatest("viewer", "hover", undefined, async () => {
        events.push("fresh");
      });
      const latest = control.humanLatest("viewer", "hover", undefined, async () => {
        events.push("latest");
      });
      finish.resolve();
      await Promise.all([running, stale, released, retaken, fresh, latest]);
      expect(events).toEqual(["latest"]);
    },
  );

  it("rejects every caller of a merged hover when the tab closes", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const events: string[] = [];
    const running = control.human("viewer", () => {
      started.resolve();
      return finish.promise;
    });
    await started.promise;
    const waiting = [1, 2].map((position) =>
      expect(
        control.humanLatest("viewer", "hover", undefined, async () => {
          events.push(`hover ${position}`);
        }),
      ).rejects.toThrow("closed"),
    );
    const closed = control.close();
    await expect(
      control.humanLatest("viewer", "hover", undefined, async () => {
        events.push("after close");
      }),
    ).rejects.toThrow("closed");
    finish.resolve();
    await Promise.all([running, ...waiting, closed]);
    expect(events).toEqual([]);
  });

  it("shares a merged dispatch failure and accepts fresh hover work afterwards", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const finish = Promise.withResolvers<void>();
    const events: string[] = [];
    const running = control.human("viewer", () => finish.promise);
    const first = expect(
      control.humanLatest("viewer", "hover", undefined, async () => {
        events.push("superseded");
      }),
    ).rejects.toThrow("dispatch failed");
    const last = expect(
      control.humanLatest("viewer", "hover", undefined, async () => {
        throw new Error("dispatch failed");
      }),
    ).rejects.toThrow("dispatch failed");
    finish.resolve();
    await Promise.all([running, first, last]);
    await control.humanLatest("viewer", "hover", undefined, async () => {
      events.push("recovered");
    });
    expect(events).toEqual(["recovered"]);
  });

  it("waits for tracked navigation before dispatching the latest human hover", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const committed = Promise.withResolvers<void>();
    const events: string[] = [];
    const navigation = control.human("viewer", async () => {
      control.track(
        committed.promise.then(() => {
          events.push("navigation committed");
        }),
      );
      return "started";
    });
    const first = control.humanLatest("viewer", "hover", undefined, async () => {
      events.push("superseded");
    });
    const latest = control.humanLatest("viewer", "hover", undefined, async () => {
      events.push("latest");
    });
    await expect(navigation).resolves.toBe("started");
    expect(events).toEqual([]);
    committed.resolve();
    await Promise.all([first, latest]);
    expect(events).toEqual(["navigation committed", "latest"]);
  });

  it("bounds tab-wide pending input while reserving independent unblocking capacity", async () => {
    const control = new SessionControl("agent");
    await control.take("viewer");
    const actions = Array.from({ length: 64 }, () => control.reserveInput(1, "action"));
    expect(actions.every((release) => release !== null)).toBe(true);
    expect(control.reserveInput(1, "action")).toBeNull();
    const unblocking = Array.from({ length: 8 }, () => control.reserveInput(1, "control"));
    expect(unblocking.every((release) => release !== null)).toBe(true);
    expect(control.reserveInput(1, "control")).toBeNull();
    const dialog = control.reserveInput(64 * 1024, "dialog");
    expect(dialog).not.toBeNull();
    expect(control.reserveInput(1, "dialog")).toBeNull();
    dialog?.();
    const dialogs = Array.from({ length: 8 }, () => control.reserveInput(1, "dialog"));
    expect(dialogs.every((release) => release !== null)).toBe(true);
    expect(control.reserveInput(1, "dialog")).toBeNull();
    for (const release of dialogs) release?.();
    await control.disconnect("viewer");
    await control.take("replacement");
    expect(control.reserveInput(1, "action")).toBeNull();
    expect(control.reserveInput(1, "control")).toBeNull();
    for (const release of [...actions, ...unblocking]) {
      release?.();
      release?.();
    }
    const recovered = control.reserveInput(256 * 1024, "action");
    expect(recovered).not.toBeNull();
    expect(control.reserveInput(1, "action")).toBeNull();
    recovered?.();
    expect(control.reserveInput(1, "action")).not.toBeNull();
  });

  it("enforces exact input byte ceilings and does not expand them after repeated release", () => {
    const control = new SessionControl(null);
    for (const [lane, maximum] of [
      ["action", 256 * 1024],
      ["control", 64 * 1024],
      ["dialog", 64 * 1024],
    ] as const) {
      expect(control.reserveInput(maximum + 1, lane)).toBeNull();
      expect(control.reserveInput(-1, lane)).toBeNull();
      expect(control.reserveInput(Number.NaN, lane)).toBeNull();
      const release = control.reserveInput(maximum, lane);
      expect(release).not.toBeNull();
      expect(control.reserveInput(1, lane)).toBeNull();
      release?.();
      release?.();
      const one = control.reserveInput(1, lane);
      expect(one).not.toBeNull();
      expect(control.reserveInput(maximum, lane)).toBeNull();
      one?.();
    }
  });

  it("checks action authorization after draining earlier navigation", async () => {
    const control = new SessionControl("agent");
    const drained = Promise.withResolvers<void>();
    const effects: string[] = [];
    await control.agent("agent", async () => control.track(drained.promise));
    let current = true;
    const action = control.agent(
      "agent",
      async () => effects.push("click"),
      undefined,
      async () => {
        if (!current) throw new BrowserControlInterrupted("Invocation retired");
      },
    );
    const refused = expect(action).rejects.toThrow("Invocation retired");
    current = false;
    drained.resolve();
    await refused;
    expect(effects).toEqual([]);
    await expect(
      control.agent(
        "agent",
        async () => "lawful",
        undefined,
        async () => {},
      ),
    ).resolves.toBe("lawful");
  });

  it("retired takeover releases only its own reservation after drain", async () => {
    const control = new SessionControl("agent");
    const drained = Promise.withResolvers<void>();
    await control.agent("agent", async () => control.track(drained.promise));
    let checks = 0;
    const takeover = control.take("viewer", async () => {
      if (++checks === 1) return;
      throw new BrowserControlInterrupted("Invocation retired during drain");
    });
    const refused = expect(takeover).rejects.toThrow("Invocation retired during drain");
    drained.resolve();
    await refused;
    expect(control.controller).toBe(null);
    await expect(control.take("later", async () => {})).resolves.toBeUndefined();
    expect(control.controller).toBe("later");
  });

  it("refuses takeover before changing the existing control generation", async () => {
    const control = new SessionControl("agent");
    const generation = control.generation;
    await expect(
      control.take("viewer", async () => {
        throw new BrowserControlInterrupted("Invocation retired");
      }),
    ).rejects.toThrow("Invocation retired");
    expect(control.controller).toBe(null);
    expect(control.generation).toBe(generation);
    await control.take("lawful");
    expect(control.controller).toBe("lawful");
  });

  it("returns an action result before tracked navigation but drains it before already queued actions", async () => {
    const control = new SessionControl("agent");
    const committed = Promise.withResolvers<void>();
    const events: string[] = [];
    const navigation = control.agent("agent", async () => {
      control.track(committed.promise.then(() => events.push("navigation committed")));
      return "started";
    });
    const next = control.agent("agent", async () => events.push("next action"));
    await expect(navigation).resolves.toBe("started");
    expect(events).toEqual([]);
    committed.resolve();
    await next;
    expect(events).toEqual(["navigation committed", "next action"]);
  });

  it("handles tracked navigation failures without poisoning the next action", async () => {
    const control = new SessionControl("agent");
    const committed = Promise.withResolvers<void>();
    await control.agent("agent", async () => control.track(committed.promise));
    committed.reject(new Error("navigation failed"));
    await expect(control.agent("agent", async () => "resumed")).resolves.toBe("resumed");
  });

  it("only lets the assigned agent act and restores that agent after human control", async () => {
    const control = new SessionControl("agent-a");
    await expect(control.agent("agent-b", async () => "wrong agent")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    await expect(control.agent("agent-a", async () => "first")).resolves.toBe("first");
    await control.take("viewer-a");
    await expect(control.agent("agent-a", async () => "racing")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    await expect(control.human("viewer-a", async () => "typed")).resolves.toBe("typed");
    await control.release("viewer-a");
    await expect(control.agent("agent-a", async () => "resumed")).resolves.toBe("resumed");
    await expect(control.agent("agent-b", async () => "wrong agent")).rejects.toThrow(
      BrowserControlInterrupted,
    );
  });

  it("drains running work before takeover and cancels previously queued agent actions", async () => {
    const control = new SessionControl("agent");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const events: string[] = [];
    const running = control.agent("agent", async () => {
      events.push("agent started");
      started.resolve();
      await finish.promise;
      events.push("agent finished");
    });
    await started.promise;
    const queued = expect(
      control.agent("agent", async () => events.push("stale agent action")),
    ).rejects.toThrow(BrowserControlInterrupted);
    const takeover = control.take("viewer");
    const typing = control.human("viewer", async () => events.push("human typed"));
    await expect(control.agent("agent", async () => "late action")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    expect(events).toEqual(["agent started"]);
    finish.resolve();
    await Promise.all([running, queued, takeover, typing]);
    expect(events).toEqual(["agent started", "agent finished", "human typed"]);
  });

  it("does not let a second viewer steal, type, release, or disconnect the owner", async () => {
    const control = new SessionControl("agent");
    const takeover = control.take("viewer-a");
    await expect(control.take("viewer-b")).rejects.toThrow("Another viewer");
    await takeover;
    await expect(control.human("viewer-b", async () => "wrong viewer")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    await expect(control.release("viewer-b")).rejects.toThrow("Only the controlling viewer");
    await control.disconnect("viewer-b");
    expect(control.controller).toBe("viewer-a");
    await expect(control.human("viewer-a", async () => "still controlled")).resolves.toBe(
      "still controlled",
    );
  });

  it("invalidates queued human actions and refs when a viewer disconnects", async () => {
    let invalidations = 0;
    const control = new SessionControl("agent", () => invalidations++);
    await control.take("viewer");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const events: string[] = [];
    const running = control.human("viewer", async () => {
      started.resolve();
      await finish.promise;
      events.push("human finished");
    });
    await started.promise;
    const queued = expect(
      control.human("viewer", async () => events.push("stale human action")),
    ).rejects.toThrow(BrowserControlInterrupted);
    const disconnected = control.disconnect("viewer");
    const resumed = control.agent("agent", async () => events.push("agent resumed"));
    expect(control.generation).toBe(2);
    expect(invalidations).toBe(2);
    finish.resolve();
    await Promise.all([running, queued, disconnected, resumed]);
    expect(events).toEqual(["human finished", "agent resumed"]);
  });

  it("does not revive stale actions when takeover is released before running work finishes", async () => {
    const control = new SessionControl("agent");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const running = control.agent("agent", async () => {
      started.resolve();
      await finish.promise;
    });
    await started.promise;
    const staleAgent = expect(control.agent("agent", async () => "stale")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    const takeover = expect(control.take("viewer")).rejects.toThrow(BrowserControlInterrupted);
    const staleHuman = expect(control.human("viewer", async () => "stale")).rejects.toThrow(
      BrowserControlInterrupted,
    );
    const release = control.release("viewer");
    const resumed = control.agent("agent", async () => "fresh");
    finish.resolve();
    await Promise.all([running, staleAgent, takeover, staleHuman, release]);
    await expect(resumed).resolves.toBe("fresh");
  });

  it("closes after running work finishes and never runs queued or new actions", async () => {
    const control = new SessionControl("agent");
    const started = Promise.withResolvers<void>();
    const finish = Promise.withResolvers<void>();
    const running = control.agent("agent", async () => {
      started.resolve();
      await finish.promise;
      return "finished";
    });
    await started.promise;
    const queued = expect(control.agent("agent", async () => "stale")).rejects.toThrow("closed");
    const closed = control.close();
    await expect(control.take("viewer")).rejects.toThrow("closed");
    await expect(control.agent("agent", async () => "new")).rejects.toThrow("closed");
    finish.resolve();
    await expect(running).resolves.toBe("finished");
    await Promise.all([queued, closed]);
    await control.disconnect("viewer");
    await control.close();
    expect(control.generation).toBe(1);
  });

  it("lets any agent act on a tab no agent opened, but only while no human controls it", async () => {
    const control = new SessionControl(null);
    await expect(control.agent("agent-a", async () => "acted")).resolves.toBe("acted");
    await control.take("viewer-a");
    await expect(control.agent("agent-a", async () => "racing")).rejects.toMatchObject({
      reason: "humanControl",
    });
    await expect(control.observe(async () => "read")).resolves.toBe("read");
    await control.release("viewer-a");
    await expect(control.agent("agent-b", async () => "acted")).resolves.toBe("acted");
  });
});
