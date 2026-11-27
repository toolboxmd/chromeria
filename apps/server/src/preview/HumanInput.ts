// @effect-diagnostics globalTimers:off - Promise-based page input owns its pacing waits.
import * as NodeNet from "node:net";

import { isLocalAddress } from "../htmlRender/publicProxy.ts";

// Private names: dev servers, LAN hosts and tailnet shares, never a public site.
// Reserved names (RFC 2606, RFC 6761) never belong to a public site either.
const LOCAL_SUFFIX =
  /(?:^|\.)(?:localhost|local|internal|lan|home\.arpa|ts\.net|test|example|invalid)$/;

/**
 * Public pages get input a person could produce; local and dev pages keep instant
 * input, because nothing there judges the agent and tests and builds wait on it.
 */
export type PageClass = "local" | "public";

export const pageClass = (url: string): PageClass => {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return "local";
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "local";
  const host = parsed.hostname
    .toLowerCase()
    .replace(/^\[|\]$/g, "")
    .replace(/\.$/, "");
  if (!host.includes(".") && NodeNet.isIP(host) === 0) return "local";
  if (LOCAL_SUFFIX.test(host)) return "local";
  const family = NodeNet.isIP(host);
  return family !== 0 && isLocalAddress(host, family) ? "local" : "public";
};

/** Randomness and waiting are injected so tests assert the plan, not elapsed time. */
export interface HumanPacing {
  readonly random: () => number;
  readonly sleep: (ms: number) => Promise<void>;
}

export const livePacing: HumanPacing = {
  random: Math.random,
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

/** A right-skewed sample around `median`: most values near it, a few much longer. */
const skewed = (random: () => number, median: number) =>
  median *
  Math.exp(
    0.45 * Math.sqrt(-2 * Math.log(1 - random() * 0.999)) * Math.cos(2 * Math.PI * random()),
  );

/**
 * Delay before each key, about 110 ms with longer pauses after spaces. The plan
 * shrinks to fit `budgetMs`, so pacing never turns into a timeout.
 */
export const keyDelays = (text: string, budgetMs: number, random: () => number): number[] => {
  const keys = [...text];
  const delays = keys.map((key, index) => {
    const base = skewed(random, 110);
    return Math.round(Math.min(600, keys[index - 1] === " " || key === " " ? base * 1.6 : base));
  });
  const total = delays.reduce((sum, delay) => sum + delay, 0);
  const allowed = Math.max(0, budgetMs);
  if (total <= allowed || total === 0) return delays;
  const scale = allowed / total;
  return delays.map((delay) => Math.floor(delay * scale));
};

export interface Point {
  readonly x: number;
  readonly y: number;
}

/** 12 to 30 moves on a slightly bent, uneven path from `from` to `to`. */
export const pointerPath = (from: Point, to: Point, random: () => number): Point[] => {
  const steps = 12 + Math.floor(random() * 19);
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const distance = Math.hypot(dx, dy);
  const bend = (random() - 0.5) * Math.min(80, distance * 0.3);
  const normal = distance === 0 ? { x: 0, y: 0 } : { x: -dy / distance, y: dx / distance };
  const path: Point[] = [];
  for (let step = 1; step <= steps; step++) {
    const linear = step / steps;
    // Ease in and out, with a little jitter that vanishes at the target.
    const t = linear * linear * (3 - 2 * linear);
    const arc = Math.sin(Math.PI * t) * bend;
    const jitter = step === steps ? 0 : (random() - 0.5) * 2;
    path.push({
      x: from.x + dx * t + normal.x * arc + jitter,
      y: from.y + dy * t + normal.y * arc + jitter,
    });
  }
  return path;
};

/** A point inside the middle of a box, not its exact center. */
export const pointInBox = (
  box: { readonly x: number; readonly y: number; readonly width: number; readonly height: number },
  random: () => number,
): Point => ({
  x: box.x + box.width * (0.3 + random() * 0.4),
  y: box.y + box.height * (0.3 + random() * 0.4),
});

export const pressMs = (random: () => number) => 40 + Math.round(random() * 80);

/** Wheel notches of about 100 px, at most 12, that add up to the requested scroll. */
export const wheelSteps = (
  deltaX: number,
  deltaY: number,
  random: () => number,
): Array<readonly [number, number]> => {
  // At most 12 notches, so a long scroll stays brief.
  const count = Math.min(
    12,
    Math.max(1, Math.ceil(Math.max(Math.abs(deltaX), Math.abs(deltaY)) / 100)),
  );
  const steps: Array<readonly [number, number]> = [];
  let sentX = 0;
  let sentY = 0;
  for (let step = 1; step <= count; step++) {
    const share = step === count ? 1 : (step + (random() - 0.5) * 0.4) / count;
    const x = Math.round(deltaX * share) - sentX;
    const y = Math.round(deltaY * share) - sentY;
    sentX += x;
    sentY += y;
    steps.push([x, y]);
  }
  return steps;
};

// An action waiting on another tab's action, such as a fill and its click, frees its site after this.
const MAX_HOLD_MS = 3_000;

/**
 * Serializes agent actions on one site in one cookie jar, with a random 0.8 to 2.5 s
 * gap since the previous one ended, taking at most half of what the deadline leaves.
 */

export class SiteActionGate {
  private readonly sites = new Map<string, { tail: Promise<void>; endedAt: number | undefined }>();
  private readonly pacing: HumanPacing;
  private readonly now: () => number;
  private readonly holdExpired: (ms: number) => Promise<void>;

  constructor(
    pacing: HumanPacing = livePacing,
    now: () => number = Date.now,
    holdExpired: (ms: number) => Promise<void> = livePacing.sleep,
  ) {
    this.pacing = pacing;
    this.now = now;
    this.holdExpired = holdExpired;
  }

  run<A>(key: string, deadline: number, action: () => Promise<A>): Promise<A> {
    const site = this.sites.get(key) ?? { tail: Promise.resolve(), endedAt: undefined };
    this.sites.set(key, site);
    let started: () => void = () => {};
    const start = new Promise<void>((resolve) => (started = resolve));
    const result = site.tail.then(async () => {
      const gap = 800 + this.pacing.random() * 1700;
      const wait =
        site.endedAt === undefined
          ? 0
          : Math.min(site.endedAt + gap - this.now(), (deadline - this.now()) / 2);
      if (wait > 0) await this.pacing.sleep(wait);
      started();
      // A request that waited past its deadline never starts.
      if (this.now() >= deadline)
        throw new DOMException("This browser request expired while it waited.", "TimeoutError");
      try {
        return await action();
      } finally {
        site.endedAt = this.now();
      }
    });
    const settled = result.then(
      () => undefined,
      () => undefined,
    );
    site.tail = Promise.race([settled, start.then(() => this.holdExpired(MAX_HOLD_MS))]);
    return result;
  }
}
