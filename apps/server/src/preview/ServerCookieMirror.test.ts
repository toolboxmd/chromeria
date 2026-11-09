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
    type Stored = { name: string; value: string; domain: string; path: string; expires: number };
    const jar = new Map<string, Stored>();
    context.newCDPSession.mockImplementation(async () => ({
      on: vi.fn(),
      detach: vi.fn(async () => {}),
      send: vi.fn(
        async (
          method: string,
          params?: {
            cookies?: Array<Omit<Stored, "expires"> & { expires?: number }>;
            name?: string;
          },
        ) => {
          if (method === "Storage.getCookies")
            return {
              cookies: [...jar.values()].map((cookie) => ({
                ...cookie,
                secure: true,
                httpOnly: true,
                session: cookie.expires < 0,
              })),
            };
          if (method === "Storage.setCookies")
            for (const { name, value, domain, path, expires } of params!.cookies!)
              jar.set(name, { name, value, domain: domain!, path, expires: expires ?? -1 });
          if (method === "Network.deleteCookies") jar.delete(params!.name!);
          return {};
        },
      ),
    }));
    const mirror = new ServerCookieMirror(contexts);
    const desktop = (
      name: string,
      value: string,
      expires?: number,
    ): CookieMirrorBatch["mutations"][number] => ({
      type: "set",
      cookie: {
        name,
        value,
        domain: ".dash.example.test",
        path: "/",
        secure: true,
        httpOnly: true,
        sameSite: "unspecified",
        ...(expires === undefined ? {} : { expires }),
      },
    });
    const send = (mutations: CookieMirrorBatch["mutations"]) =>
      mirror.apply({ ...batch, mutations });
    const state = () =>
      Object.fromEntries([...jar].map(([name, { value, expires }]) => [name, [value, expires]]));
    const replay = [
      desktop("session", "desktop-logged-out"),
      desktop("other", "desktop-other"),
      desktop("renewed", "shared", 2_000_000_000),
    ];

    await send(replay);
    jar.set("session", { ...jar.get("session")!, value: "agent-login" });
    jar.set("renewed", { ...jar.get("renewed")!, expires: 2_100_000_000 });
    jar.set("own", { ...jar.get("other")!, name: "own", value: "agent-own" });
    // A reconnect or resync replays every cookie the desktop already sent.
    await send(replay);
    await send([
      { type: "remove", key: { name: "session", domain: ".dash.example.test", path: "/" } },
    ]);
    expect(state()).toEqual({
      session: ["agent-login", -1],
      other: ["desktop-other", -1],
      renewed: ["shared", 2_100_000_000],
      own: ["agent-own", -1],
    });

    jar.delete("other");
    await send([
      desktop("session", "desktop-login"),
      desktop("other", "desktop-other"),
      desktop("renewed", "shared", 2_200_000_000),
    ]);
    expect(state()).toEqual({
      session: ["desktop-login", -1],
      other: ["desktop-other", -1],
      renewed: ["shared", 2_200_000_000],
      own: ["agent-own", -1],
    });
  });
});
