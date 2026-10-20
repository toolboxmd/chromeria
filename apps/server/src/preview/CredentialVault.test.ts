import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { PreviewCredentialReason } from "@t3tools/contracts";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerSettings from "../serverSettings.ts";
import * as CredentialVault from "./CredentialVault.ts";

const SENTINEL = "sentinel-Zq8#pw";

interface Reply {
  readonly exitCode?: number;
  readonly stdout?: string;
  readonly stderr?: string;
  readonly spawnError?: boolean;
}

const text = (value: string | undefined) =>
  value === undefined ? Stream.empty : Stream.make(new TextEncoder().encode(value));

const harness = (input: {
  readonly reply: (args: ReadonlyArray<string>) => Reply;
  readonly vault?: { readonly kind: "none" } | { readonly kind: "local"; readonly rbwPath: string };
  readonly debug?: string;
}) => {
  const calls: Array<{ command: string; args: ReadonlyArray<string>; shell: unknown }> = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.suspend(() => {
      if (!ChildProcess.isStandardCommand(command)) return Effect.die("expected a command");
      calls.push({ command: command.command, args: command.args, shell: command.options.shell });
      const reply = input.reply(command.args);
      if (reply.spawnError)
        return Effect.fail(
          PlatformError.systemError({ _tag: "NotFound", module: "ChildProcess", method: "spawn" }),
        );
      return Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(reply.exitCode ?? 0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: text(reply.stdout),
          stderr: text(reply.stderr),
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      );
    }),
  );
  const layer = CredentialVault.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ServerSettings.layerTest({
          credentialVault: input.vault ?? { kind: "local", rbwPath: "/opt/fake/rbw" },
        }),
        ConfigProvider.layer(
          ConfigProvider.fromEnv({ env: input.debug === undefined ? {} : { DEBUG: input.debug } }),
        ),
      ),
    ),
    Layer.provide(NodeServices.layer),
  );
  return { calls, layer };
};

const unlockedThen =
  (reply: Reply) =>
  (args: ReadonlyArray<string>): Reply =>
    args[0] === "unlocked" ? {} : reply;

const reasonOf = <A>(effect: Effect.Effect<A, { readonly reason: PreviewCredentialReason }>) =>
  effect.pipe(
    Effect.flip,
    Effect.map((error) => error.reason),
  );

describe("CredentialVault", () => {
  it.effect("passes the item as one argument after --, without a shell", () => {
    const { calls, layer } = harness({ reply: unlockedThen({ stdout: `${SENTINEL}\n` }) });
    return Effect.gen(function* () {
      const vault = yield* CredentialVault.CredentialVault;
      assert.equal(yield* vault.read("O'Brien & Co (shop)", "password"), SENTINEL);
      assert.equal(yield* vault.read("Shopify", "username"), SENTINEL);
      assert.equal(yield* vault.read("Shopify", "totp"), SENTINEL);
      assert.deepEqual(
        calls.map(({ command, args, shell }) => ({ command, args, shell })),
        [
          { command: "/opt/fake/rbw", args: ["unlocked"], shell: false },
          {
            command: "/opt/fake/rbw",
            args: ["get", "--field", "password", "--", "O'Brien & Co (shop)"],
            shell: false,
          },
          { command: "/opt/fake/rbw", args: ["unlocked"], shell: false },
          {
            command: "/opt/fake/rbw",
            args: ["get", "--field", "username", "--", "Shopify"],
            shell: false,
          },
          { command: "/opt/fake/rbw", args: ["unlocked"], shell: false },
          { command: "/opt/fake/rbw", args: ["code", "--", "Shopify"], shell: false },
        ],
      );
    }).pipe(Effect.provide(layer));
  });

  it.effect("keeps a value's own whitespace and drops only rbw's newline", () => {
    const { layer } = harness({ reply: unlockedThen({ stdout: " pass word \n\n" }) });
    return Effect.gen(function* () {
      const vault = yield* CredentialVault.CredentialVault;
      assert.equal(yield* vault.read("Shopify", "password"), " pass word \n");
    }).pipe(Effect.provide(layer));
  });

  it.effect("splits URIs one per line", () => {
    const { layer } = harness({
      reply: unlockedThen({ stdout: "https://accounts.shopify.com/login\nshopify.com\n" }),
    });
    return Effect.gen(function* () {
      const vault = yield* CredentialVault.CredentialVault;
      assert.deepEqual(yield* vault.uris("Shopify"), [
        "https://accounts.shopify.com/login",
        "shopify.com",
      ]);
    }).pipe(Effect.provide(layer));
  });

  it.effect("never spawns for an item that could be an option, a URI, or shell syntax", () => {
    const { calls, layer } = harness({ reply: () => ({ stdout: SENTINEL }) });
    return Effect.gen(function* () {
      const vault = yield* CredentialVault.CredentialVault;
      for (const item of [
        "-rf",
        "--field=password",
        "https://evil.example",
        "a;b",
        "$(id)",
        "`id`",
        "a\nb",
        "a/b",
        "x".repeat(129),
        "",
      ]) {
        assert.equal(yield* reasonOf(vault.read(item, "password")), "itemNotFound", item);
      }
      assert.deepEqual(calls, []);
    }).pipe(Effect.provide(layer));
  });

  const reasons: ReadonlyArray<[string, (args: ReadonlyArray<string>) => Reply, string]> = [
    ["vaultLocked", (args) => (args[0] === "unlocked" ? { exitCode: 1 } : {}), "locked vault"],
    [
      "vaultUnavailable",
      (args) => (args[0] === "unlocked" ? { exitCode: 69 } : {}),
      "wrapper cannot reach the vault machine",
    ],
    ["vaultUnavailable", () => ({ spawnError: true }), "missing executable"],
    [
      "vaultUnavailable",
      unlockedThen({ exitCode: 69 }),
      "vault machine lost between unlock and read",
    ],
    [
      "itemNotFound",
      unlockedThen({ exitCode: 1, stderr: "couldn't find entry for 'Shopify': no entry found\n" }),
      "missing item",
    ],
    [
      "itemAmbiguous",
      unlockedThen({
        exitCode: 1,
        stderr: "couldn't find entry for 'Shop': multiple entries found: Shop A, Shop B\n",
      }),
      "ambiguous item",
    ],
    [
      "fieldMissing",
      unlockedThen({ exitCode: 1, stderr: "entry does not contain a totp secret\n" }),
      "no TOTP secret",
    ],
    ["fieldMissing", unlockedThen({ stdout: "" }), "empty field"],
    ["fieldMissing", unlockedThen({ stderr: "entry for 'Shopify' had no password\n" }), "no pw"],
    ["vaultUnavailable", unlockedThen({ exitCode: 2, stderr: "boom" }), "unknown failure"],
    ["vaultUnavailable", unlockedThen({ stdout: "x".repeat(70_000) }), "oversized output"],
  ];
  it.effect.each(reasons.map(([reason, reply, name]) => ({ reason, reply, name })))(
    "maps $name to $reason",
    ({ reason, reply }) => {
      const { layer } = harness({ reply });
      return Effect.gen(function* () {
        const vault = yield* CredentialVault.CredentialVault;
        assert.equal(yield* reasonOf(vault.read("Shopify", "password")), reason);
      }).pipe(Effect.provide(layer));
    },
  );

  it.effect("refuses without a configured vault, before spawning", () => {
    const { calls, layer } = harness({ reply: () => ({}), vault: { kind: "none" } });
    return Effect.gen(function* () {
      const vault = yield* CredentialVault.CredentialVault;
      assert.equal(yield* reasonOf(vault.uris("Shopify")), "vaultNotConfigured");
      assert.deepEqual(calls, []);
    }).pipe(Effect.provide(layer));
  });

  it.effect("refuses while Playwright protocol or API logging is on", () =>
    Effect.gen(function* () {
      for (const debug of ["pw:protocol", "pw:*", "*", "foo,pw:api"]) {
        const { calls, layer } = harness({ reply: () => ({}), debug });
        yield* Effect.gen(function* () {
          const vault = yield* CredentialVault.CredentialVault;
          assert.equal(yield* reasonOf(vault.read("Shopify", "password")), "protocolLogging");
        }).pipe(Effect.provide(layer));
        assert.deepEqual(calls, []);
      }
    }),
  );

  it("reads DEBUG the way the debug package does", () => {
    assert.equal(CredentialVault.debugEnables("pw:browser", "pw:protocol"), false);
    assert.equal(CredentialVault.debugEnables("pw:*,-pw:protocol", "pw:protocol"), false);
    assert.equal(CredentialVault.debugEnables("pw:* -pw:protocol", "pw:api"), true);
    assert.equal(CredentialVault.debugEnables("t3:*", "pw:protocol"), false);
  });
});
