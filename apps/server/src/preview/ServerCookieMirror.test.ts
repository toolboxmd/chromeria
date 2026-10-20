import { EnvironmentId, type CookieMirrorBatch } from "@t3tools/contracts";
import type { BrowserContext, Page } from "playwright-core";
import { describe, expect, it, vi } from "vite-plus/test";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";

const batch: CookieMirrorBatch = {
  environmentId: EnvironmentId.make("synthetic-environment"),
  sequence: 0,
  profile: { id: "work", name: "Work", kind: "persistent" },
  mutations: [],
};

const harness = () => {
  const page = { close: vi.fn(async () => {}) } as unknown as Page;
  const sessions: Array<{ send: ReturnType<typeof vi.fn>; detach: ReturnType<typeof vi.fn> }> = [];
  const closeListeners: Array<() => void> = [];
  const context = {
    newCDPSession: vi.fn(async () => {
      const session = {
        send: vi.fn(async () => ({ cookies: [] })),
        detach: vi.fn(async () => {}),
        on: vi.fn((event: string, listener: () => void) => {
          if (event === "close") closeListeners.push(listener);
        }),
      };
      sessions.push(session);
      return session;
    }),
  } as unknown as BrowserContext & { newCDPSession: ReturnType<typeof vi.fn> };
  const contexts = {
    contextFor: vi.fn(async () => context),
    reservedPage: vi.fn(async () => page),
  } as unknown as ServerBrowserContexts;
  return { page, context, contexts, sessions, closeListeners };
};

describe("ServerCookieMirror", () => {
  it("leaves the reserved page open when attaching fails, then attaches on the next batch", async () => {
    const { page, context, contexts } = harness();
    context.newCDPSession.mockRejectedValueOnce(new Error("attach failed"));
    const mirror = new ServerCookieMirror(contexts);

    await expect(mirror.apply(batch)).rejects.toThrow("attach failed");

    expect(page.close).not.toHaveBeenCalled();
    await mirror.apply(batch);
    expect(context.newCDPSession).toHaveBeenCalledTimes(2);
    expect(context.newCDPSession).toHaveBeenLastCalledWith(page);
  });

  it("reuses one session per context and re-attaches after it closes", async () => {
    const { context, contexts, closeListeners } = harness();
    const mirror = new ServerCookieMirror(contexts);

    await mirror.apply(batch);
    await mirror.apply(batch);
    expect(context.newCDPSession).toHaveBeenCalledTimes(1);
    for (const listener of closeListeners) listener();
    await mirror.apply(batch);

    expect(context.newCDPSession).toHaveBeenCalledTimes(2);
  });
});
