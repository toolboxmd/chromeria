import { CookieMirrorError, type ChromeCookieKeyCommand } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { macChromiumKeyMaterial, resolveChromiumKeys } from "./BrowserImport/ChromiumKeys.ts";
import type { ChromiumKeyMaterial } from "./BrowserImport/ChromiumKeys.ts";

export const chromeCookieKeyCommandsEqual = (
  previous: ChromeCookieKeyCommand | null | undefined,
  command: ChromeCookieKeyCommand | null,
) =>
  previous === command ||
  (previous != null &&
    command !== null &&
    previous.length === command.length &&
    previous.every((value, index) => value === command[index]));

export function makeChromeCookieKeyCache(read: typeof readChromeCookieKey = readChromeCookieKey) {
  let configured: ChromeCookieKeyCommand | null | undefined;
  let cached: ChromiumKeyMaterial | undefined;
  let retryAt = 0;
  let failures = 0;
  return (command: ChromeCookieKeyCommand | null) =>
    Effect.gen(function* () {
      if (!chromeCookieKeyCommandsEqual(configured, command)) {
        configured = command === null ? null : [...command];
        cached = undefined;
        retryAt = 0;
        failures = 0;
      }
      if (cached) return cached;
      const now = yield* Clock.currentTimeMillis;
      if (now < retryAt) return yield* new CookieMirrorError({ reason: "unavailable" });
      const result = yield* read(command).pipe(Effect.result);
      if (result._tag === "Failure") {
        retryAt = now + Math.min(30_000, 1000 * 2 ** Math.min(failures++, 5));
        return yield* result.failure;
      }
      cached = result.success;
      failures = 0;
      return cached;
    });
}

export const readChromeCookieKey = Effect.fnUntraced(function* (
  command: ChromeCookieKeyCommand | null,
) {
  if (command === null) {
    return yield* resolveChromiumKeys({
      platform: "darwin",
      keychainService: "Chrome Safe Storage",
      keychainAccount: "Chrome",
      linuxSecretApplication: undefined,
    }).pipe(Effect.mapError(() => new CookieMirrorError({ reason: "unavailable" })));
  }
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const environment = yield* HostProcess.Environment;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const [executable, ...args] = command;
      if (!executable) return yield* new CookieMirrorError({ reason: "unavailable" });
      // The spawner's default is exec, never a shell; stderr is deliberately discarded.
      const child = yield* spawner.spawn(
        ChildProcess.make(executable, args, { env: environment, stdin: "ignore" }),
      );
      const [stdout, , code] = yield* Effect.all(
        [
          child.stdout.pipe(Stream.decodeText(), Stream.mkString),
          child.stderr.pipe(Stream.runDrain),
          child.exitCode,
        ],
        { concurrency: "unbounded" },
      );
      const secret = stdout.replace(/\r?\n$/, "");
      if (Number(code) !== 0 || secret.length === 0 || secret.length > 4096)
        return yield* new CookieMirrorError({ reason: "unavailable" });
      return macChromiumKeyMaterial(secret);
    }),
  ).pipe(
    Effect.timeout("15 seconds"),
    // Spawn and command failures can contain stdout, stderr or argv.
    Effect.catchCause(() => new CookieMirrorError({ reason: "unavailable" })),
  );
});
