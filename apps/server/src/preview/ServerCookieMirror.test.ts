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

  it("replays never overwrite a cookie the server profile changed, but fresh desktop values do", async () => {
    const { context, contexts } = harness();
    const jar = new Map<string, { name: string; value: string; domain: string; path: string }>();
    context.newCDPSession.mockImplementation(async () => ({
      on: vi.fn(),
      detach: vi.fn(async () => {}),
      send: vi.fn(
        async (
          method: string,
          params?: {
            cookies?: Array<{ name: string; value: string; domain?: string; path: string }>;
            name?: string;
          },
        ) => {
          if (method === "Storage.getCookies")
            return {
              cookies: [...jar.values()].map((cookie) => ({
                ...cookie,
                secure: true,
                httpOnly: true,
                session: true,
                expires: -1,
              })),
            };
          if (method === "Storage.setCookies")
            for (const cookie of params!.cookies!)
              jar.set(cookie.name, { ...cookie, domain: cookie.domain! });
          if (method === "Network.deleteCookies") jar.delete(params!.name!);
          return {};
        },
      ),
    }));
    const mirror = new ServerCookieMirror(contexts);
    const desktop = (name: string, value: string): CookieMirrorBatch["mutations"][number] => ({
      type: "set",
      cookie: {
        name,
        value,
        domain: ".dash.example.test",
        path: "/",
        secure: true,
        httpOnly: true,
        sameSite: "unspecified",
      },
    });
    const send = (mutations: CookieMirrorBatch["mutations"]) =>
      mirror.apply({ ...batch, mutations });
    const values = () => Object.fromEntries([...jar].map(([name, cookie]) => [name, cookie.value]));

    await send([desktop("session", "desktop-logged-out"), desktop("other", "desktop-other")]);
    jar.set("session", { ...jar.get("session")!, value: "agent-login" });
    jar.set("own", { name: "own", value: "agent-own", domain: ".dash.example.test", path: "/" });
    // A reconnect or resync replays every value the desktop already sent.
    await send([desktop("session", "desktop-logged-out"), desktop("other", "desktop-other")]);
    await send([
      { type: "remove", key: { name: "session", domain: ".dash.example.test", path: "/" } },
    ]);
    expect(values()).toEqual({ session: "agent-login", other: "desktop-other", own: "agent-own" });

    jar.delete("other");
    await send([desktop("session", "desktop-login"), desktop("other", "desktop-other")]);
    expect(values()).toEqual({
      session: "desktop-login",
      other: "desktop-other",
      own: "agent-own",
    });
  });
});
