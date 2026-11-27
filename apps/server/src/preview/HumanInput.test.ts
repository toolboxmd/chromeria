import { describe, expect, it } from "vite-plus/test";

import { keyDelays, pageClass, pointerPath, SiteActionGate, wheelSteps } from "./HumanInput.ts";

const seeded = (seed: number) => () => ((seed = (seed * 16807) % 2147483647) - 1) / 2147483646;

describe("pageClass", () => {
  it.each([
    ["https://allegro.pl/listing", "public"],
    ["https://shop.example.co.uk/", "public"],
    ["http://localhost:3000/", "local"],
    ["http://app.localhost:5173/", "local"],
    ["http://127.0.0.1:8080/", "local"],
    ["http://[::1]:8080/", "local"],
    ["http://192.168.1.20/", "local"],
    ["http://devbox:3000/", "local"],
    ["https://bestia.tail1234.ts.net/", "local"],
    ["file:///home/user/index.html", "local"],
    ["about:blank", "local"],
    ["data:text/html,<p>x</p>", "local"],
    ["https://shop.example.test/", "local"],
    ["http://app.localhost.:5173/", "local"],
  ])("%s is %s", (url, expected) => {
    expect(pageClass(url)).toBe(expected);
  });
});

describe("human input plans", () => {
  it("varies key gaps around a person's pace and fits a tight budget", () => {
    const delays = keyDelays("karma dla kota 2 kg", 10_000, seeded(3));
    expect(delays).toHaveLength(19);
    const sorted = delays.toSorted((a, b) => a - b);
    expect(sorted[9]).toBeGreaterThan(60);
    expect(sorted[9]).toBeLessThan(220);
    expect(new Set(delays).size).toBeGreaterThan(10);
    const tight = keyDelays("x".repeat(100), 1_000, seeded(3));
    expect(tight.reduce((sum, delay) => sum + delay, 0)).toBeLessThanOrEqual(1_000);
  });

  it("moves the pointer in 12 to 30 steps that end on the target", () => {
    for (const seed of [1, 2, 3, 4, 5]) {
      const path = pointerPath({ x: 0, y: 0 }, { x: 400, y: 300 }, seeded(seed));
      expect(path.length).toBeGreaterThanOrEqual(12);
      expect(path.length).toBeLessThanOrEqual(30);
      expect(path.at(-1)!.x).toBeCloseTo(400);
      expect(path.at(-1)!.y).toBeCloseTo(300);
    }
  });

  it("splits a scroll into wheel notches that add up to it", () => {
    const steps = wheelSteps(0, 450, seeded(9));
    expect(steps).toHaveLength(5);
    expect(steps.reduce((sum, [, y]) => sum + y, 0)).toBe(450);
  });
});

describe("SiteActionGate", () => {
  it("runs one action per site at a time and waits the rest of a random gap", async () => {
    let now = 0;
    const waits: number[] = [];
    const gate = new SiteActionGate(
      {
        random: () => 0.5,
        sleep: async (ms) => {
          waits.push(ms);
          now += ms;
        },
      },
      () => now,
      () => new Promise<void>(() => {}),
    );
    const order: string[] = [];
    const action = (name: string, ms: number) => async () => {
      order.push(`${name}:start`);
      now += ms;
      await Promise.resolve();
      order.push(`${name}:end`);
    };
    await Promise.all([
      gate.run("profile|shop.test", 60_000, action("a", 100)),
      gate.run("profile|shop.test", 60_000, action("b", 100)),
      gate.run("profile|other.test", 60_000, action("c", 100)),
    ]);
    expect(order.indexOf("b:start")).toBeGreaterThan(order.indexOf("a:end"));
    // The gap is 800 + 0.5 * 1700 ms after a ended; the first action on each site waits for none.
    expect(waits).toEqual([1650]);
    // Right after an action, with 1 s left: the gap would be 1650 ms, half the budget is 500.
    const ended = now;
    await gate.run("profile|shop.test", ended + 1_000, action("d", 0));
    expect(waits.at(-1), "a gap never takes more than half of what the deadline leaves").toBe(500);
    await expect(
      gate.run("profile|shop.test", now - 1, action("e", 0)),
      "a request that waited past its deadline never starts",
    ).rejects.toThrow("expired");
    expect(order).not.toContain("e:start");
  });

  it("frees the site when an action waits on another action there", async () => {
    const gate = new SiteActionGate(
      { random: () => 0, sleep: async () => {} },
      () => 0,
      async () => {},
    );
    const clicked = Promise.withResolvers<void>();
    const fill = gate.run("profile|login.test", 60_000, () => clicked.promise);
    const click = gate.run("profile|login.test", 60_000, async () => clicked.resolve());
    // Without the hold limit the click would wait for the fill forever.
    await Promise.all([fill, click]);
  });
});
