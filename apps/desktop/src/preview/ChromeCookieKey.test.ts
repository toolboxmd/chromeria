// @effect-diagnostics nodeBuiltinImport:off - Verifies synthetic key derivation with Node's reference primitive.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { CookieMirrorError } from "@t3tools/contracts";
import * as TestClock from "effect/testing/TestClock";
import * as NodeCrypto from "node:crypto";
import { vi } from "vite-plus/test";

import { makeChromeCookieKeyCache, readChromeCookieKey } from "./ChromeCookieKey.ts";
import { resolveChromiumKeys } from "./BrowserImport/ChromiumKeys.ts";

vi.mock("./BrowserImport/ChromiumKeys.ts", async (original) => ({
  ...(await original<typeof import("./BrowserImport/ChromiumKeys.ts")>()),
  resolveChromiumKeys: vi.fn(() => Effect.succeed({ cbcV10: Buffer.alloc(16, 7) })),
}));

describe("pluggable Chrome Safe Storage secret", () => {
  it.effect("retries failed reads with backoff and caches only the recovered key", () =>
    Effect.gen(function* () {
      const key = { cbcV10: Buffer.alloc(16, 9) };
      const read = vi
        .fn<typeof readChromeCookieKey>()
        .mockReturnValueOnce(Effect.fail(new CookieMirrorError({ reason: "unavailable" })))
        .mockReturnValue(Effect.succeed(key));
      const cached = makeChromeCookieKeyCache(read);
      expect((yield* cached(null).pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* cached(null).pipe(Effect.result))._tag).toBe("Failure");
      expect(read).toHaveBeenCalledTimes(1);
      yield* TestClock.adjust("1 second");
      expect(yield* cached(null)).toBe(key);
      expect(yield* cached(null)).toBe(key);
      expect(read).toHaveBeenCalledTimes(2);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("invalidates a successful key or failure backoff when the command changes", () =>
    Effect.gen(function* () {
      const first = { cbcV10: Buffer.alloc(16, 1) };
      const second = { cbcV10: Buffer.alloc(16, 2) };
      const read = vi
        .fn<typeof readChromeCookieKey>()
        .mockReturnValueOnce(Effect.succeed(first))
        .mockReturnValueOnce(Effect.fail(new CookieMirrorError({ reason: "unavailable" })))
        .mockReturnValue(Effect.succeed(second));
      const cached = makeChromeCookieKeyCache(read);
      expect(yield* cached(null)).toBe(first);
      expect(yield* cached(null)).toBe(first);
      expect((yield* cached(["synthetic-failure"]).pipe(Effect.result))._tag).toBe("Failure");
      expect(yield* cached(["synthetic-new-command", "argument"])).toBe(second);
      expect(yield* cached(["synthetic-new-command", "argument"])).toBe(second);
      expect(read.mock.calls).toEqual([
        [null],
        [["synthetic-failure"]],
        [["synthetic-new-command", "argument"]],
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("executes argv without a shell and derives from stdout while discarding stderr", () =>
    Effect.gen(function* () {
      vi.mocked(resolveChromiumKeys).mockClear();
      const literal = "synthetic; $(no-shell-expansion)";
      const material = yield* readChromeCookieKey([
        process.execPath,
        "-e",
        'process.stdout.write(process.argv[1]+"\\n"); process.stderr.write("synthetic-error-output")',
        literal,
      ]);
      expect(material.cbcV10).toEqual(
        NodeCrypto.pbkdf2Sync(literal, "saltysalt", 1003, 16, "sha1"),
      );
      expect(resolveChromiumKeys).not.toHaveBeenCalled();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("uses Keychain only when the command is unset", () =>
    Effect.gen(function* () {
      vi.mocked(resolveChromiumKeys).mockClear();
      const material = yield* readChromeCookieKey(null);
      expect(material.cbcV10).toEqual(Buffer.alloc(16, 7));
      expect(resolveChromiumKeys).toHaveBeenCalledOnce();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("redacts command failures and does not fall back to Keychain on command failure", () =>
    Effect.gen(function* () {
      vi.mocked(resolveChromiumKeys).mockClear();
      const result = yield* readChromeCookieKey([
        process.execPath,
        "-e",
        'process.stdout.write("synthetic-secret-sentinel"); process.stderr.write("synthetic-secret-sentinel"); process.exitCode=1',
      ]).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      expect(JSON.stringify(result)).not.toContain("synthetic-secret-sentinel");
      expect(resolveChromiumKeys).not.toHaveBeenCalled();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a configured empty command without falling back to Keychain", () =>
    Effect.gen(function* () {
      vi.mocked(resolveChromiumKeys).mockClear();
      expect((yield* readChromeCookieKey([]).pipe(Effect.result))._tag).toBe("Failure");
      expect(resolveChromiumKeys).not.toHaveBeenCalled();
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
