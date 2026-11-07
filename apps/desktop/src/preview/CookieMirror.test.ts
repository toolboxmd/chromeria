import {
  BROWSER_PROFILE_MAX_COUNT,
  cookieMirrorKey,
  type CookieMirrorMutation,
  type MirrorCookie,
} from "@t3tools/contracts";
import { applyMirrorMutations, type CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";
import { describe, expect, it, vi } from "vite-plus/test";

import { CookieMirror, type ChromeCookieSnapshot } from "./CookieMirror.ts";

const profile = { id: "default", name: "Default", kind: "persistent" } as const;
const cookie = (value = "synthetic-source", patch: Partial<MirrorCookie> = {}): MirrorCookie => ({
  name: "login",
  value,
  domain: "login.example.test",
  path: "/account",
  secure: true,
  httpOnly: true,
  sameSite: "unspecified",
  ...patch,
});
const snapshot = (cookies: ReadonlyArray<MirrorCookie>, complete = true): ChromeCookieSnapshot => ({
  cookies,
  complete,
  identities: cookies.map(({ name, domain, path, partitionKey, expires }) => ({
    key: { name, domain, path, ...(partitionKey ? { partitionKey } : {}) },
    persistent: expires !== undefined,
  })),
});
const store = (initial: ReadonlyArray<MirrorCookie> = []) => {
  const cookies = new Map(initial.map((cookie) => [cookieMirrorKey(cookie), cookie]));
  const boundary = {
    read: vi.fn<CookieMirrorStore["read"]>(async () => [...cookies.values()]),
    apply: vi.fn(async (mutations: ReadonlyArray<CookieMirrorMutation>) => {
      applyMirrorMutations(cookies, mutations);
    }),
  } satisfies CookieMirrorStore;
  return { cookies, ...boundary };
};
const source = (mirror: CookieMirror, cookies: ReadonlyArray<MirrorCookie>) =>
  mirror.observeChrome("chrome-default", profile, snapshot(cookies));
const prepare = async (mirror: CookieMirror, environmentId = "env") => {
  for (const batch of mirror.snapshot(environmentId))
    await mirror.prepareDelivery(environmentId, batch.profile.id, batch.mutations);
};

describe("one-way mirror service seam", () => {
  it("adds, changes and removes cookies in all mapped partitions and preserves downstream-only cookies", async () => {
    const local = cookie("local", { name: "local" });
    const persistent = cookie("initial", { expires: 2_000_000_000 });
    const first = store([local]);
    const second = store();
    const mirror = new CookieMirror();
    await mirror.addPartition("first", "env", profile, first);
    await mirror.addPartition("second", "other-env", profile, second);
    await source(mirror, [persistent]);
    await prepare(mirror);
    expect([...first.cookies.values()]).toEqual([local, persistent]);
    expect([...second.cookies.values()]).toEqual([persistent]);
    await source(mirror, [{ ...persistent, value: "changed" }]);
    expect(second.cookies.get(cookieMirrorKey(cookie()))?.value).toBe("changed");
    await source(mirror, []);
    expect([...first.cookies.values()]).toEqual([local]);
    expect(second.cookies.size).toBe(0);
    expect(mirror.snapshot("env")[0]?.mutations).toContainEqual({
      type: "remove",
      key: { name: "login", domain: "login.example.test", path: "/account" },
    });
  });

  it("feeds each environment only from its own desktop partition", async () => {
    const first = store([cookie("first-account")]);
    const second = store([cookie("second-account")]);
    const mirror = new CookieMirror();
    await mirror.addPartition("first", "first-env", profile, first);
    await mirror.addPartition("second", "second-env", profile, second);
    expect(mirror.snapshot("first-env")[0]?.mutations).toEqual([
      { type: "set", cookie: cookie("first-account") },
    ]);
    expect(mirror.snapshot("second-env")[0]?.mutations).toEqual([
      { type: "set", cookie: cookie("second-account") },
    ]);
    first.cookies.set(cookieMirrorKey(cookie()), cookie("first-changed"));
    await mirror.observeDesktop("first");
    expect(mirror.snapshot("second-env")[0]?.mutations).toEqual([
      { type: "set", cookie: cookie("second-account") },
    ]);
  });

  it("coalesces overwrite remove/insert pairs and forwards Chrome changes once without echoing Electron notifications", async () => {
    const changed = vi.fn();
    const destination = store();
    const mirror = new CookieMirror({ changed });
    await mirror.addPartition("partition", "env", profile, destination);
    changed.mockClear();
    await source(mirror, [cookie()]);
    expect(changed).toHaveBeenCalledExactlyOnceWith("env");
    changed.mockClear();
    await mirror.observeDesktop("partition");
    expect(changed).not.toHaveBeenCalled();
    destination.cookies.delete(cookieMirrorKey(cookie()));
    destination.cookies.set(cookieMirrorKey(cookie()), cookie("overwritten"));
    await mirror.observeDesktop("partition");
    expect(changed).toHaveBeenCalledExactlyOnceWith("env");
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([
      { type: "set", cookie: cookie("overwritten") },
    ]);
    destination.apply.mockClear();
    await source(mirror, [cookie()]);
    expect(destination.apply).not.toHaveBeenCalled();
  });

  it("protects unreadable identities on partial reads while copying readable additions", async () => {
    const destination = store();
    const mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    const persistent = cookie("persistent", { expires: 2_000_000_000 });
    await source(mirror, [persistent]);
    destination.read.mockRejectedValueOnce(new Error("synthetic read failure"));
    await expect(mirror.observeDesktop("partition")).rejects.toThrow("synthetic read failure");
    const added = cookie("added", { name: "new" });
    await mirror.observeChrome("chrome-default", profile, {
      ...snapshot([added], false),
      identities: [...snapshot([persistent]).identities, ...snapshot([added]).identities],
    });
    expect([...destination.cookies.values()]).toEqual([persistent, added]);
    expect(mirror.metadata().chrome[0]?.managed).toHaveLength(2);
    await source(mirror, [added]);
    expect([...destination.cookies.values()]).toEqual([added]);
  });

  it("removes a vanished Chrome session cookie in every environment while the same Chrome process stays open", async () => {
    const first = store();
    const second = store();
    const mirror = new CookieMirror();
    await mirror.addPartition("first", "bestia", profile, first);
    await mirror.addPartition("second", "rocky", profile, second);
    await mirror.observeChrome("chrome-default", profile, {
      ...snapshot([cookie()]),
      processIdentity: "mac:42:2026-10-19T08:00:00",
    });
    await prepare(mirror, "bestia");
    await prepare(mirror, "rocky");
    await mirror.observeChrome("chrome-default", profile, {
      ...snapshot([]),
      processIdentity: "mac:42:2026-10-19T08:00:00",
    });
    expect([...first.cookies.values()]).toEqual([]);
    expect([...second.cookies.values()]).toEqual([]);
    expect(mirror.snapshot("bestia")[0]?.mutations).toEqual([
      { type: "remove", key: { name: "login", domain: "login.example.test", path: "/account" } },
    ]);
  });

  it("preserves vanished Chrome session cookies after relaunch, an unknown process or a desktop restart", async () => {
    const destination = store();
    let mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    await mirror.observeChrome("chrome-default", profile, {
      ...snapshot([cookie()]),
      processIdentity: "mac:42:first-start",
    });
    await prepare(mirror);
    await mirror.observeChrome("chrome-default", profile, {
      ...snapshot([]),
      processIdentity: "mac:42:second-start",
    });
    expect([...destination.cookies.values()]).toEqual([cookie()]);
    await source(mirror, []);
    expect([...destination.cookies.values()]).toEqual([cookie()]);
    await source(mirror, [cookie("signed-in-again")]);
    await source(mirror, []);
    expect([...destination.cookies.values()]).toEqual([cookie("signed-in-again")]);
    expect(mirror.snapshot("env")[0]?.mutations.every((mutation) => mutation.type === "set")).toBe(
      true,
    );
    await source(mirror, [cookie()]);
    await prepare(mirror);
    const metadata = mirror.metadata();
    destination.cookies.clear();
    mirror = new CookieMirror({ metadata });
    await mirror.addPartition("partition", "env", profile, destination);
    await source(mirror, []);
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([]);
  });

  it("removes known absent identities while an unrelated row remains unreadable", async () => {
    const unreadable = cookie("last-readable", { name: "unreadable", expires: 2_000_000_000 });
    const removed = cookie("signed-out", { name: "removed", expires: 2_000_000_000 });
    const downstream = cookie("local", { name: "downstream-only" });
    const destination = store([downstream]);
    const mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    await source(mirror, [unreadable, removed]);
    await prepare(mirror);
    await mirror.observeChrome("chrome-default", profile, {
      cookies: [],
      identities: snapshot([unreadable]).identities,
      complete: false,
    });
    expect([...destination.cookies.values()]).toEqual([downstream, unreadable]);
    expect(mirror.snapshot("env")[0]?.mutations).toContainEqual({
      type: "remove",
      key: { name: "removed", domain: "login.example.test", path: "/account" },
    });
  });

  it("does not forward a Chrome write again when CDP rounds fractional microsecond expiry", async () => {
    const changed = vi.fn();
    const destination = store();
    const mirror = new CookieMirror({ changed });
    await mirror.addPartition("partition", "env", profile, destination);
    const chrome = cookie("fractional", { expires: 2_000_000_000.123456 });
    changed.mockClear();
    await source(mirror, [chrome]);
    expect(changed).toHaveBeenCalledExactlyOnceWith("env");
    destination.cookies.set(cookieMirrorKey(chrome), { ...chrome, expires: 2_000_000_000.123 });
    changed.mockClear();
    await mirror.observeDesktop("partition");
    expect(changed).not.toHaveBeenCalled();
  });

  it("protects both unknown ancestor siblings without hiding absence at a different top-level site", async () => {
    const first = cookie("first", {
      expires: 2_000_000_000,
      partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: false },
    });
    const sibling = {
      ...first,
      value: "sibling",
      partitionKey: { ...first.partitionKey!, hasCrossSiteAncestor: true },
    };
    const absent = {
      ...first,
      value: "absent",
      partitionKey: { ...first.partitionKey!, topLevelSite: "https://other.test" },
    };
    const destination = store();
    const mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    await source(mirror, [first, sibling, absent]);
    await mirror.observeChrome("chrome-default", profile, {
      cookies: [],
      identities: [],
      complete: false,
      unresolvedPartitions: [
        {
          domain: first.domain,
          name: first.name,
          path: first.path,
          topLevelSite: "https://top.test",
        },
      ],
    });
    expect([...destination.cookies.values()]).toEqual([first, sibling]);
  });

  it("preserves an offline logout through a desktop restart using identities without values", async () => {
    const persistent = cookie("synthetic-private-value", { expires: 2_000_000_000 });
    const onlyDownstream = cookie("local", { name: "downstream-only" });
    const desktop = store();
    const server = store([onlyDownstream]);
    let mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, desktop);
    await source(mirror, [persistent]);
    const initial = mirror.snapshot("env")[0]!;
    await mirror.prepareDelivery("env", profile.id, initial.mutations);
    await server.apply(initial.mutations);
    // The reply may be lost after the server has applied it.
    await source(mirror, []);
    const durable = JSON.parse(JSON.stringify(mirror.metadata()));
    expect(JSON.stringify(durable)).not.toContain("synthetic-private-value");
    expect(JSON.stringify(durable)).not.toContain('"value"');
    mirror = new CookieMirror({ metadata: durable });
    await mirror.addPartition("partition", "env", profile, desktop);
    await source(mirror, []);
    const caughtUp = mirror.snapshot("env")[0]!;
    await server.apply(caughtUp.mutations);
    await mirror.acknowledge("env", profile.id, caughtUp.mutations);
    expect([...server.cookies.values()]).toEqual([onlyDownstream]);
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([]);
  });

  it("discovers a persistent removal that happened while the desktop app was stopped", async () => {
    const desktop = store();
    let mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, desktop);
    await source(mirror, [cookie("old", { expires: 2_000_000_000 })]);
    await prepare(mirror);
    mirror = new CookieMirror({ metadata: mirror.metadata() });
    await mirror.addPartition("partition", "env", profile, desktop);
    await source(mirror, []);
    expect(desktop.cookies.size).toBe(0);
    expect(mirror.snapshot("env")[0]?.mutations[0]?.type).toBe("remove");
  });

  it("keeps profile accounts and both partition-key fields distinct on exact removal", async () => {
    const first = store();
    const second = store();
    const work = { ...profile, id: "work", name: "Work" };
    const mirror = new CookieMirror();
    await mirror.addPartition("personal", "env", profile, first);
    await mirror.addPartition("work", "env", work, second);
    const partitioned = cookie("partitioned", {
      domain: ".example.test",
      sameSite: "no_restriction",
      expires: 2_000_000_000,
      partitionKey: { topLevelSite: "https://top.example", hasCrossSiteAncestor: true },
    });
    const sibling = {
      ...partitioned,
      value: "sibling",
      partitionKey: { topLevelSite: "https://top.example", hasCrossSiteAncestor: false },
    };
    await source(mirror, [cookie("personal"), partitioned, sibling]);
    await mirror.observeChrome("chrome-work", work, snapshot([cookie("work")]));
    await source(mirror, [cookie("personal"), sibling]);
    expect([...first.cookies.values()]).toEqual([cookie("personal"), sibling]);
    expect([...second.cookies.values()]).toEqual([cookie("work")]);
  });

  it("never opens Incognito and initializes a later partition without a tab from the current Chrome baseline", async () => {
    const mirror = new CookieMirror();
    const privateStore = store();
    await mirror.addPartition(
      "private",
      "env",
      { ...profile, id: "incognito", name: "Incognito" },
      privateStore,
    );
    expect(privateStore.read).not.toHaveBeenCalled();
    await source(mirror, [cookie("latest")]);
    const late = store([cookie("local", { name: "local" })]);
    await mirror.addPartition("late", "late-env", profile, late);
    expect([...late.cookies.values()]).toEqual([
      cookie("local", { name: "local" }),
      cookie("latest"),
    ]);
  });

  it("retries failed browser writes and does not send an onward write before the desktop applied it", async () => {
    const destination = store();
    const mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    destination.apply.mockRejectedValueOnce(new Error("synthetic write failure"));
    expect(await source(mirror, [cookie()])).toEqual(["partition"]);
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([]);
    await mirror.observeDesktop("partition");
    expect([...destination.cookies.values()]).toEqual([cookie()]);
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([{ type: "set", cookie: cookie() }]);
  });

  it("serializes a pending observation before the next source observation", async () => {
    const destination = store([cookie()]);
    const mirror = new CookieMirror();
    await mirror.addPartition("partition", "env", profile, destination);
    const observed = Promise.withResolvers<ReadonlyArray<MirrorCookie>>();
    destination.read.mockReturnValueOnce(observed.promise);
    const earlier = mirror.observeDesktop("partition");
    const later = source(mirror, [cookie("latest")]);
    observed.resolve([cookie("earlier")]);
    await Promise.all([earlier, later]);
    expect(mirror.snapshot("env")[0]?.mutations).toEqual([
      { type: "set", cookie: cookie("latest") },
    ]);
  });
});

describe("persisted Chrome profile mapping", () => {
  it("uses root and directory as identity, preserves user labels and disambiguates new label collisions", async () => {
    let mirror = new CookieMirror();
    const defaultProfile = await mirror.mapChromeProfile({
      root: "/synthetic/chrome",
      directory: "Default",
      name: "Person",
      proposedId: "ignored",
      profiles: [],
    });
    expect(defaultProfile.profile.id).toBe("default");
    const first = await mirror.mapChromeProfile({
      root: "/synthetic/chrome",
      directory: "Profile 1",
      name: "Work",
      proposedId: "chrome-one",
      profiles: defaultProfile.profiles,
    });
    const second = await mirror.mapChromeProfile({
      root: "/synthetic/chrome",
      directory: "Profile 2",
      name: "Work",
      proposedId: "chrome-two",
      profiles: first.profiles,
    });
    expect(second.profile.id).toBe("chrome-two");
    expect(second.profile.name).toBe("Work (Profile 2)");
    mirror = new CookieMirror({ metadata: mirror.metadata() });
    const renamed = await mirror.mapChromeProfile({
      root: "/synthetic/chrome",
      directory: "Profile 1",
      name: "Renamed",
      proposedId: "different-proposal",
      profiles: second.profiles.map((profile) =>
        profile.id === "chrome-one" ? { ...profile, name: "My work account" } : profile,
      ),
    });
    expect(renamed.profile).toEqual({
      id: "chrome-one",
      name: "My work account",
      kind: "persistent",
    });
  });

  it("enforces label length and the existing profile cap without merging accounts", async () => {
    const mirror = new CookieMirror();
    const truncated = await mirror.mapChromeProfile({
      root: "/synthetic",
      directory: "Profile 1",
      name: "x".repeat(80),
      proposedId: "long",
      profiles: [],
    });
    expect(truncated.profile.name).toHaveLength(48);
    const full = Array.from({ length: BROWSER_PROFILE_MAX_COUNT - 2 }, (_, index) => ({
      id: `custom-${index}`,
      name: `Profile ${index}`,
      kind: "persistent" as const,
    }));
    await expect(
      mirror.mapChromeProfile({
        root: "/other",
        directory: "Profile 1",
        name: "Other",
        proposedId: "new",
        profiles: full,
      }),
    ).rejects.toThrow("profile limit reached");
  });
});
