import {
  CREDENTIAL_ITEM_PATTERN,
  PreviewCredentialError,
  type PreviewCredentialField,
  type PreviewCredentialReason,
} from "@t3tools/contracts";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ServerSettings from "../serverSettings.ts";

const CALL_TIMEOUT = "15 seconds";
const MAX_OUTPUT_BYTES = 64 * 1024;
// The rocky-tool wrapper exits 69 (EX_UNAVAILABLE) when the vault's machine is unreachable.
const WRAPPER_UNREACHABLE = 69;

/**
 * Reads vault values with rbw. The value goes only to the caller: stdout is never
 * logged, and failures carry a fixed reason instead of rbw's output.
 */
export class CredentialVault extends Context.Service<
  CredentialVault,
  {
    /** The item's site URIs; reading them needs no secret. */
    readonly uris: (item: string) => Effect.Effect<ReadonlyArray<string>, PreviewCredentialError>;
    readonly read: (
      item: string,
      field: PreviewCredentialField,
    ) => Effect.Effect<string, PreviewCredentialError>;
  }
>()("t3/preview/CredentialVault") {}

/** Whether `DEBUG`, read the way Playwright's `debug` loggers read it, enables `namespace`. */
export const debugEnables = (debug: string, namespace: string) => {
  const matches = (pattern: string) =>
    new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*?")}$`).test(
      namespace,
    );
  const patterns = debug.split(/[\s,]+/).filter(Boolean);
  return (
    patterns.some((pattern) => !pattern.startsWith("-") && matches(pattern)) &&
    !patterns.some((pattern) => pattern.startsWith("-") && matches(pattern.slice(1)))
  );
};

// Protocol logs carry the typed text; API logs carry call parameters.
const LEAKING_NAMESPACES = ["pw:protocol", "pw:api"];

const fail = (reason: PreviewCredentialReason) => new PreviewCredentialError({ reason });

/** rbw prints the value with one newline; anything else is part of the value. */
const stripNewline = (text: string) => text.replace(/\r?\n$/, "");

const collect = (stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
  stream.pipe(
    Stream.runFold(
      () => ({ chunks: [] as Array<Uint8Array>, bytes: 0 }),
      (state, chunk) => {
        state.bytes += chunk.byteLength;
        if (state.bytes <= MAX_OUTPUT_BYTES) state.chunks.push(chunk);
        return state;
      },
    ),
    Effect.flatMap((state) =>
      state.bytes > MAX_OUTPUT_BYTES
        ? Effect.fail(fail("vaultUnavailable"))
        : Effect.succeed(new TextDecoder().decode(Buffer.concat(state.chunks))),
    ),
  );

/** rbw's own error lines; its exit status is 1 for all of them. */
const reasonFromStderr = (stderr: string): PreviewCredentialReason =>
  /no entry found/.test(stderr)
    ? "itemNotFound"
    : /multiple entries found/.test(stderr)
      ? "itemAmbiguous"
      : /does not contain a totp secret|not a login entry|had no password/.test(stderr)
        ? "fieldMissing"
        : /locked|unlock|pinentry|agent/i.test(stderr)
          ? "vaultLocked"
          : "vaultUnavailable";

const make = Effect.gen(function* () {
  const settings = yield* ServerSettings.ServerSettingsService;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const debug = yield* Config.String("DEBUG").pipe(Config.option);

  const run = (rbwPath: string, args: ReadonlyArray<string>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const child = yield* spawner.spawn(
          ChildProcess.make(rbwPath, args, { shell: false, stdin: "ignore" }),
        );
        const [stdout, stderr, exitCode] = yield* Effect.all(
          [collect(child.stdout), collect(child.stderr), child.exitCode],
          { concurrency: "unbounded" },
        );
        return { stdout, stderr, exitCode: Number(exitCode) };
      }),
    ).pipe(
      Effect.timeoutOption(CALL_TIMEOUT),
      Effect.mapError(() => fail("vaultUnavailable")),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.fail(fail("vaultUnavailable")),
          onSome: Effect.succeed,
        }),
      ),
    );

  /** Checks the settings and the vault, then runs one rbw command for `item`. */
  const call = Effect.fn("CredentialVault.call")(function* (
    item: string,
    command: "get" | "code",
    field?: "uris" | "username" | "password",
  ) {
    const vault = (yield* settings.getSettings.pipe(
      Effect.mapError(() => fail("vaultUnavailable")),
    )).credentialVault;
    if (vault.kind === "none") return yield* fail("vaultNotConfigured");
    if (
      Option.isSome(debug) &&
      LEAKING_NAMESPACES.some((namespace) => debugEnables(debug.value, namespace))
    )
      return yield* fail("protocolLogging");
    // The contract checks it too; the argument array must never carry an option or a URI.
    if (!CREDENTIAL_ITEM_PATTERN.test(item)) return yield* fail("itemNotFound");
    // rbw unlocks on demand with pinentry, which an agent can never answer.
    const unlocked = yield* run(vault.rbwPath, ["unlocked"]);
    if (unlocked.exitCode === WRAPPER_UNREACHABLE) return yield* fail("vaultUnavailable");
    if (unlocked.exitCode !== 0) return yield* fail("vaultLocked");
    const args = command === "code" ? ["code", "--", item] : ["get", "--field", field!, "--", item];
    const result = yield* run(vault.rbwPath, args);
    if (result.exitCode === WRAPPER_UNREACHABLE) return yield* fail("vaultUnavailable");
    if (result.exitCode !== 0) {
      const reason = reasonFromStderr(result.stderr);
      yield* Effect.logWarning("credential vault refused a read", {
        command,
        reason,
        exitCode: result.exitCode,
      });
      return yield* fail(reason);
    }
    const value = stripNewline(result.stdout);
    if (value === "") return yield* fail("fieldMissing");
    return value;
  });

  return CredentialVault.of({
    uris: Effect.fn("CredentialVault.uris")((item) =>
      call(item, "get", "uris").pipe(
        Effect.map((text) =>
          text
            .split("\n")
            .map((uri) => uri.trim())
            .filter(Boolean),
        ),
      ),
    ),
    read: Effect.fn("CredentialVault.read")((item, field) =>
      field === "totp" ? call(item, "code") : call(item, "get", field),
    ),
  });
});

export const layer = Layer.effect(CredentialVault, make);
