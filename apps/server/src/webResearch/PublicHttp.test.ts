import { afterEach, vi } from "vite-plus/test";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as PublicHttp from "./PublicHttp.ts";

const isNetworkBlocked = Schema.is(PublicHttp.PublicNetworkBlockedError);
const dns = vi.hoisted(() => ({ lookup: vi.fn() }));
vi.mock("node:dns/promises", () => dns);
afterEach(() => dns.lookup.mockReset());

it.effect.each([
  "http://127.0.0.1/",
  "http://0.0.0.0/",
  "http://10.0.0.1/",
  "http://172.16.0.1/",
  "http://192.168.1.1/",
  "http://100.100.100.100/",
  "http://169.254.169.254/",
  "http://[::1]/",
  "http://[fc00::1]/",
  "http://[fe80::1]/",
  "http://[::ffff:127.0.0.1]/",
  "http://[64:ff9b::7f00:1]/",
  "http://[2002:7f00:1::]/",
])("refuses local destination %s before an HTTP connection", (url) =>
  Effect.gen(function* () {
    const http = yield* PublicHttp.PublicHttp;
    const result = yield* Effect.result(http.get(new URL(url)));
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(isNetworkBlocked(result.failure)).toBe(true);
    expect(dns.lookup).not.toHaveBeenCalled();
  }).pipe(Effect.provide(PublicHttp.layer)),
);

it.effect("rejects a DNS answer containing any private address", () =>
  Effect.gen(function* () {
    dns.lookup.mockResolvedValue([
      { address: "93.184.216.34", family: 4 },
      { address: "127.0.0.1", family: 4 },
    ]);
    const http = yield* PublicHttp.PublicHttp;
    const result = yield* Effect.result(http.get(new URL("http://mixed.example/")));
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(isNetworkBlocked(result.failure)).toBe(true);
  }).pipe(Effect.provide(PublicHttp.layer)),
);

it.effect("checks DNS again at connection time and blocks a rebound private address", () =>
  Effect.gen(function* () {
    dns.lookup
      .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
      .mockResolvedValueOnce([{ address: "127.0.0.1", family: 4 }]);
    const http = yield* PublicHttp.PublicHttp;
    const result = yield* Effect.result(http.get(new URL("http://rebound.example/")));
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") expect(isNetworkBlocked(result.failure)).toBe(true);
    expect(dns.lookup).toHaveBeenCalledTimes(2);
  }).pipe(Effect.provide(PublicHttp.layer)),
);
