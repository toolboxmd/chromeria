import { CookieMirrorError, type ChromeCookieKeyCommand } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { macChromiumKeyMaterial, resolveChromiumKeys } from "./BrowserImport/ChromiumKeys.ts";

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
