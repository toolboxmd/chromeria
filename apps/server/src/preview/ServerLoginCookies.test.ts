import { EnvironmentId, type MirrorCookie } from "@t3tools/contracts";
import type { BrowserContext, Page } from "playwright-core";
import { describe, expect, it, vi } from "vite-plus/test";

import type { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";
import { ServerLoginCookies, type LoginProfiles } from "./ServerLoginCookies.ts";

interface Stored {
  name: string;
  value: string;
  domain: string;
  path: string;
}

/** A CDP cookie jar keyed as Chrome keys it: domain, path and name. */
const harness = (initial: ReadonlyArray<Stored> = []) => {
  const jar = new Map<string, Stored>();
  const key = (cookie: Pick<Stored, "domain" | "path" | "name">) =>
    `${cookie.domain}|${cookie.path}|${cookie.name}`;
  for (const cookie of initial) jar.set(key(cookie), cookie);
  const page = {} as Page;
  const context = {
    newCDPSession: vi.fn(async () => ({
      on: vi.fn(),
      detach: vi.fn(async () => {}),
      send: vi.fn(
        async (
          method: string,
          params?: {
            cookies?: Array<{
              name: string;
              value: string;
              url: string;
              domain?: string;
              path: string;
            }>;
            name?: string;
            domain?: string;
            path?: string;
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
            for (const cookie of params!.cookies!) {
              const stored = {
                name: cookie.name,
                value: cookie.value,
                domain: cookie.domain ?? new URL(cookie.url).hostname,
                path: cookie.path,
              };
              jar.set(key(stored), stored);
            }
          if (method === "Network.deleteCookies")
            jar.delete(key({ name: params!.name!, domain: params!.domain!, path: params!.path! }));
          return {};
        },
      ),
    })),
  } as unknown as BrowserContext;
  const contexts = {
    contextFor: vi.fn(async () => context),
    reservedPage: vi.fn(async () => page),
  } as unknown as ServerBrowserContexts & { contextFor: ReturnType<typeof vi.fn> };
  const profiles: LoginProfiles = {
    find: (id) =>
      id === "work"
        ? { id, name: "Work", kind: "persistent" }
        : id === "incognito"
          ? { id, name: "Incognito", kind: "incognito" }
          : undefined,
  };
  const mirror = new ServerCookieMirror(contexts);
  const values = () =>
    Object.fromEntries(
      [...jar.values()].map((cookie) => [
        `${cookie.domain}${cookie.path}${cookie.name}`,
        cookie.value,
      ]),
    );
  const login = new ServerLoginCookies(mirror, profiles);
  return { context, contexts, mirror, login, values };
};

const snapshotCookie = (domain: string, name: string, value: string): MirrorCookie => ({
  name,
  value,
  domain,
  path: "/",
  secure: true,
  httpOnly: true,
  sameSite: "unspecified",
});
const none = { deviceBound: 0, partitioned: 0 };

describe("ServerLoginCookies", () => {
  it("overwrites snapshot keys, deletes omitted host-only cookies and never deletes shared-domain cookies", async () => {
    const { login, values } = harness([
      { name: "session", value: "stale", domain: "admin.shop.test", path: "/" },
      { name: "signed-out", value: "old", domain: "admin.shop.test", path: "/" },
      { name: "other-account", value: "keep", domain: ".shop.test", path: "/" },
      { name: "unlisted", value: "keep", domain: "www.shop.test", path: "/" },
    ]);

    const result = await login.import({
      profileId: "work",
      hosts: ["admin.shop.test"],
      cookies: [
        snapshotCookie("admin.shop.test", "session", "fresh"),
        snapshotCookie(".shop.test", "parent", "fresh-parent"),
      ],
      skipped: none,
    });

    expect(result).toEqual({
      profileId: "work",
      imported: 2,
      skipped: { deviceBound: 0, partitioned: 0, sharedDomain: 1 },
    });
    expect(values()).toEqual({
      "admin.shop.test/session": "fresh",
      ".shop.test/other-account": "keep",
      "www.shop.test/unlisted": "keep",
      ".shop.test/parent": "fresh-parent",
    });
  });

  it("refuses unknown and incognito profiles before opening any browser context", async () => {
    const { login, contexts } = harness();
    const args = { hosts: ["shop.test"], cookies: [], skipped: none };
    await expect(login.import({ ...args, profileId: "typo" })).rejects.toMatchObject({
      message: expect.stringMatching(/^unknown_profile:/),
    });
    await expect(
      login.export({ hosts: ["shop.test"], profileId: "incognito" }),
    ).rejects.toMatchObject({ message: expect.stringMatching(/^incognito_profile:/) });
    expect(contexts.contextFor).not.toHaveBeenCalled();
  });

  it("exports only the listed hosts' cookies and refuses Google hosts", async () => {
    const { login } = harness([
      { name: "session", value: "a", domain: "shop.test", path: "/" },
      { name: "bank", value: "b", domain: "bank.test", path: "/" },
    ]);
    expect(await login.export({ profileId: "work", hosts: ["shop.test"] })).toEqual({
      cookies: [snapshotCookie("shop.test", "session", "a")],
      skipped: none,
    });
    await expect(
      login.export({ profileId: "work", hosts: ["mail.google.com"] }),
    ).rejects.toMatchObject({ message: expect.stringMatching(/^device_bound:/) });
  });

  it("keeps an import through a desktop replay, while a new desktop value still wins", async () => {
    const { mirror, login, values } = harness();
    const desktop = (value: string) => ({
      environmentId: EnvironmentId.make("synthetic-environment"),
      sequence: 0,
      profile: { id: "work", name: "Work", kind: "persistent" as const },
      mutations: [{ type: "set" as const, cookie: snapshotCookie(".shop.test", "sid", value) }],
    });
    await mirror.apply(desktop("desktop-old"));
    await login.import({
      profileId: "work",
      hosts: ["shop.test"],
      cookies: [snapshotCookie(".shop.test", "sid", "imported")],
      skipped: none,
    });
    await mirror.apply(desktop("desktop-old"));
    expect(values()).toEqual({ ".shop.test/sid": "imported" });
    await mirror.apply(desktop("desktop-new"));
    expect(values()).toEqual({ ".shop.test/sid": "desktop-new" });

    // A host-only cookie the import deleted, because the source had signed out, stays deleted
    // through a desktop replay, while a new desktop value still restores it.
    const hostOnly = (value: string) => ({
      ...desktop(value),
      mutations: [{ type: "set" as const, cookie: snapshotCookie("shop.test", "a", value) }],
    });
    await mirror.apply(hostOnly("desktop-a"));
    await login.import({
      profileId: "work",
      hosts: ["shop.test"],
      cookies: [snapshotCookie(".shop.test", "sid", "desktop-new")],
      skipped: none,
    });
    expect(values()).toEqual({ ".shop.test/sid": "desktop-new" });
    await mirror.apply(hostOnly("desktop-a"));
    expect(values()).toEqual({ ".shop.test/sid": "desktop-new" });
    await mirror.apply(hostOnly("desktop-a2"));
    expect(values()).toEqual({ ".shop.test/sid": "desktop-new", "shop.test/a": "desktop-a2" });
  });
});
