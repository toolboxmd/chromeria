import { describe, expect, it } from "vite-plus/test";
import type { MirrorCookie } from "@t3tools/contracts";

import { planLoginImport, selectLoginCookies } from "./browserLoginImport.ts";

// A fixed suffix list keeps the oracle independent of the server's tldts lookup.
const SUFFIXES = new Set(["com", "test", "co.uk", "github.io"]);
const registrableDomain = (host: string) => {
  const labels = host.split(".");
  for (let index = 0; index < labels.length - 1; index++) {
    if (SUFFIXES.has(labels.slice(index + 1).join("."))) return labels.slice(index).join(".");
  }
  return undefined;
};

const cookie = (domain: string, name = "session", extra: Partial<MirrorCookie> = {}) =>
  ({
    name,
    value: `value-of-${name}`,
    domain,
    path: "/",
    secure: true,
    httpOnly: true,
    sameSite: "lax",
    ...extra,
  }) satisfies MirrorCookie;

const select = (cookies: ReadonlyArray<MirrorCookie>, hosts: ReadonlyArray<string>) =>
  selectLoginCookies({ cookies, hosts, registrableDomain });

describe("selectLoginCookies", () => {
  it("takes what the browser sends to the listed hosts and nothing for siblings", () => {
    const selected = select(
      [
        cookie("admin.shop.test", "host-only"),
        cookie(".shop.test", "parent"),
        cookie("www.shop.test", "sibling-host-only"),
        cookie(".www.shop.test", "sibling-domain"),
        cookie("shop.test", "apex-host-only"),
        cookie(".evilshop.test", "lookalike"),
        cookie("admin.shop.test", "deep-path", { path: "/settings" }),
      ],
      ["admin.shop.test"],
    );
    expect(selected._tag).toBe("selected");
    if (selected._tag !== "selected") return;
    expect(selected.cookies.map((c) => c.name)).toEqual(["host-only", "parent", "deep-path"]);
  });

  it("never treats a domain cookie on a public suffix as covering a host", () => {
    const selected = select(
      [cookie(".github.io", "suffix-wide"), cookie(".a.github.io", "own"), cookie(".co.uk", "uk")],
      ["a.github.io", "shop.co.uk"],
    );
    expect(selected._tag === "selected" && selected.cookies.map((c) => c.name)).toEqual(["own"]);
  });

  it("refuses Google hosts as device-bound and skips Google credentials elsewhere", () => {
    expect(select([cookie(".google.com")], ["mail.google.com"])).toEqual({
      _tag: "refused",
      reason: "device_bound",
    });
    expect(select([], ["shop.test", "accounts.google.co.uk"])).toMatchObject({
      reason: "device_bound",
    });
    const selected = select(
      [
        cookie(".youtube.com", "__Secure-3PSID"),
        cookie(".youtube.com", "PREF"),
        cookie("www.youtube.com", "SID"),
        // An ordinary site's SID is its own session cookie.
        cookie("shop.test", "SID"),
      ],
      ["www.youtube.com", "shop.test"],
    );
    expect(selected).toMatchObject({
      _tag: "selected",
      skipped: { deviceBound: 2, partitioned: 0 },
    });
    expect(selected._tag === "selected" && selected.cookies.map((c) => c.name)).toEqual([
      "PREF",
      "SID",
    ]);
  });

  it("skips partitioned cookies", () => {
    const selected = select(
      [
        cookie("shop.test", "chips", {
          partitionKey: { topLevelSite: "https://other.test", hasCrossSiteAncestor: false },
        }),
        cookie("shop.test"),
      ],
      ["shop.test"],
    );
    expect(selected).toMatchObject({ skipped: { partitioned: 1, deviceBound: 0 } });
    expect(selected._tag === "selected" && selected.cookies).toHaveLength(1);
  });

  it("refuses the whole batch above 256 cookies or 64 KiB of names and values", () => {
    const many = Array.from({ length: 257 }, (_, index) => cookie("shop.test", `c${index}`));
    expect(select(many, ["shop.test"])).toEqual({ _tag: "refused", reason: "too_many_cookies" });
    expect(select(many.slice(0, 256), ["shop.test"])._tag).toBe("selected");
    const big = (name: string) => cookie("shop.test", name, { value: "x".repeat(32 * 1024) });
    expect(select([big("a"), big("b")], ["shop.test"])).toMatchObject({
      reason: "too_many_cookies",
    });
    expect(select([big("a")], ["shop.test"])._tag).toBe("selected");
  });
});

describe("planLoginImport", () => {
  const plan = (
    current: ReadonlyArray<MirrorCookie>,
    snapshot: ReadonlyArray<MirrorCookie>,
    hosts: ReadonlyArray<string>,
  ) => planLoginImport({ current, snapshot, hosts, registrableDomain });

  it("overwrites snapshot keys, deletes omitted host-only cookies and keeps shared-domain ones", () => {
    const result = plan(
      [
        cookie("admin.shop.test", "stale-host-only"),
        cookie(".shop.test", "other-account"),
        cookie("www.shop.test", "unlisted-host"),
        cookie("admin.shop.test", "session", { value: "old" }),
        cookie("elsewhere.test", "unrelated"),
      ],
      [cookie("admin.shop.test", "session", { value: "new" }), cookie(".shop.test", "parent")],
      ["admin.shop.test"],
    );
    expect(result).toEqual({
      _tag: "planned",
      imported: 2,
      sharedDomain: 1,
      mutations: [
        { type: "set", cookie: cookie("admin.shop.test", "session", { value: "new" }) },
        { type: "set", cookie: cookie(".shop.test", "parent") },
        {
          type: "remove",
          key: { name: "stale-host-only", domain: "admin.shop.test", path: "/" },
        },
      ],
    });
  });

  it("drops snapshot cookies outside the listed hosts, so a source cannot widen the write", () => {
    const result = plan([], [cookie("bank.test", "planted"), cookie("shop.test")], ["shop.test"]);
    expect(result).toMatchObject({ imported: 1 });
    expect(result._tag === "planned" && result.mutations).toEqual([
      { type: "set", cookie: cookie("shop.test") },
    ]);
  });

  it("refuses an over-cap snapshot whole", () => {
    const many = Array.from({ length: 257 }, (_, index) => cookie("shop.test", `c${index}`));
    expect(plan([], many, ["shop.test"])).toEqual({ _tag: "refused", reason: "too_many_cookies" });
  });
});
