import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
import * as HostProcess from "@t3tools/shared/HostProcess";

import {
  applyPreferredCodexDefaultModel,
  codexCustomEndpoint,
  codexRoutesSpendNativeQuota,
  makeCodexSkillsProbe,
  mapCodexModelCapabilities,
  readCodexAccountLimits,
} from "./CodexProvider.ts";
import { READ_BOUND, withinReadBound } from "../prism/CodexCredits.ts";

function makeFakeCodexAppServers(
  input: {
    readonly results?: Readonly<Record<string, unknown>>;
    readonly startupWorkRunning?: () => boolean;
    /** Completed when `initialize` arrives, which then gets no answer. */
    readonly stallHandshake?: Deferred.Deferred<void>;
  } = {},
) {
  const processGroupChecks: Array<string> = [];
  const helpers: Array<{
    readonly codexHome: unknown;
    readonly crash: Effect.Effect<void>;
    readonly requests: Array<unknown>;
    readonly methods: Array<string>;
    closed: boolean;
  }> = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (command._tag === "StandardCommand" && command.command === "pgrep") {
        processGroupChecks.push(String(command.args[1]));
        // Codex's own startup work runs in its process group beside the app-server.
        const group = input.startupWorkRunning?.() ? `${command.args[1]}\n4242\n` : "";
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(4243),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.make(new TextEncoder().encode(group)),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }
      const output = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const crash = Deferred.succeed(exited, ChildProcessSpawner.ExitCode(1)).pipe(
        Effect.andThen(Queue.end(output)),
        Effect.asVoid,
      );
      const helper = {
        codexHome: command._tag === "StandardCommand" ? command.options.env?.CODEX_HOME : undefined,
        crash,
        requests: [] as Array<unknown>,
        methods: [] as Array<string>,
        closed: false,
      };
      helpers.push(helper);
      yield* Effect.addFinalizer(() => Effect.sync(() => (helper.closed = true)));
      const respond = (id: unknown, result: unknown) =>
        Queue.offer(output, new TextEncoder().encode(`${JSON.stringify({ id, result })}\n`));
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(900_000_000 + helpers.length),
        exitCode: Deferred.await(exited),
        isRunning: Effect.map(Deferred.isDone(exited), (done) => !done),
        kill: () => crash,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: string | Uint8Array) =>
          Effect.forEach(
            (typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk))
              .split("\n")
              .filter(Boolean),
            (line) => {
              const frame = JSON.parse(line) as {
                readonly id?: number;
                readonly method: string;
                readonly params?: { readonly cwds: ReadonlyArray<string> };
              };
              if (frame.id === undefined) return Effect.void;
              if (frame.method === "initialize" && input.stallHandshake)
                return Deferred.succeed(input.stallHandshake, undefined);
              if (frame.method === "initialize") {
                return respond(frame.id, {
                  codexHome: "/codex-home",
                  platformFamily: "unix",
                  platformOs: "linux",
                  userAgent: "codex/0.0.0",
                });
              }
              helper.methods.push(frame.method);
              if (input.results && frame.method in input.results)
                return respond(frame.id, input.results[frame.method]);
              helper.requests.push(frame.params);
              return respond(frame.id, {
                data: (frame.params?.cwds ?? []).map((cwd) => ({
                  cwd,
                  errors: [],
                  skills: [
                    {
                      name: `skill-in-${cwd}`,
                      path: `${cwd}/SKILL.md`,
                      enabled: true,
                      description: "",
                      scope: "repo",
                    },
                  ],
                })),
              });
            },
            { discard: true },
          ),
        ),
        stdout: Stream.fromQueue(output),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  return { helpers, spawner, processGroupChecks };
}

it.effect("reuses one Codex skills helper per launch until it idles out", () =>
  Effect.gen(function* () {
    const { helpers, spawner } = makeFakeCodexAppServers();
    const providerScope = yield* Scope.make();
    const probe = yield* makeCodexSkillsProbe().pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Scope.provide(providerScope),
    );
    const read = (cwd: string, homePath = "/home-a") =>
      probe({ binaryPath: "codex", homePath, cwd, environment: {} }).pipe(
        Effect.map((skills) => skills.map((skill) => skill.name)),
      );

    assert.deepStrictEqual(yield* read("/alpha"), ["skill-in-/alpha"]);
    assert.deepStrictEqual(yield* read("/beta"), ["skill-in-/beta"]);
    assert.strictEqual(helpers.length, 1);
    assert.deepStrictEqual(helpers[0]!.requests, [
      { cwds: ["/alpha"], forceReload: true },
      { cwds: ["/beta"], forceReload: true },
    ]);

    // Another home is another launch identity; the previous helper retires at once.
    assert.deepStrictEqual(yield* read("/alpha", "/home-b"), ["skill-in-/alpha"]);
    assert.strictEqual(helpers.length, 2);
    assert.strictEqual(helpers[0]!.closed, true);
    assert.strictEqual(helpers[1]!.codexHome, "/home-b");

    // A helper that exits is replaced within the same read.
    yield* helpers[1]!.crash;
    assert.deepStrictEqual(yield* read("/beta", "/home-b"), ["skill-in-/beta"]);
    assert.strictEqual(helpers.length, 3);

    yield* TestClock.adjust("29 seconds");
    assert.strictEqual(helpers[2]!.closed, false);
    yield* TestClock.adjust("1 second");
    assert.strictEqual(helpers[2]!.closed, true);
    assert.deepStrictEqual(yield* read("/alpha", "/home-b"), ["skill-in-/alpha"]);
    assert.strictEqual(helpers.length, 4);

    yield* Scope.close(providerScope, Exit.void);
    assert.strictEqual(helpers[3]!.closed, true);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

it.effect("reads the account's own limits and leaves Codex's startup work to finish", () =>
  Effect.gen(function* () {
    let startupWorkRunning = true;
    const rateLimits = {
      limitId: "codex",
      secondary: { usedPercent: 100, resetsAt: 1_796_083_200 },
      credits: { hasCredits: true, unlimited: false, balance: "60011" },
    };
    const results = (config: unknown) => ({
      "account/read": {
        account: { type: "chatgpt", email: "a@example.com", planType: "pro" },
        requiresOpenaiAuth: true,
      },
      "config/read": { config, origins: {} },
      "account/rateLimits/read": {
        rateLimits: { limitId: "codex" },
        rateLimitsByLimitId: { codex: rateLimits },
      },
    });
    const native = makeFakeCodexAppServers({
      results: results({}),
      startupWorkRunning: () => startupWorkRunning,
    });
    const read = (spawner: ChildProcessSpawner.ChildProcessSpawner["Service"]) =>
      readCodexAccountLimits({ binaryPath: "codex", cwd: "/", environment: {} }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

    assert.deepStrictEqual(yield* read(native.spawner), { route: "native", rateLimits });
    assert.deepStrictEqual(native.helpers[0]!.methods, [
      "account/read",
      "config/read",
      "account/rateLimits/read",
    ]);
    // The answer returns at once, but stopping Codex mid-fetch would strand its ssh.
    yield* TestClock.adjust("30 seconds");
    assert.strictEqual(native.helpers[0]!.closed, false);
    startupWorkRunning = false;
    yield* TestClock.adjust("500 millis");
    assert.strictEqual(native.helpers[0]!.closed, true);

    const proxy = makeFakeCodexAppServers({
      results: results({ openai_base_url: "http://127.0.0.1:8317/v1" }),
    });
    assert.deepStrictEqual(yield* read(proxy.spawner), { route: "other" });
    assert.deepStrictEqual(proxy.helpers[0]!.methods, ["account/read", "config/read"]);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

it.effect("ends a read stalled before its handshake through the startup teardown", () =>
  Effect.gen(function* () {
    let startupWorkRunning = true;
    const stallHandshake = yield* Deferred.make<void>();
    const codex = makeFakeCodexAppServers({
      stallHandshake,
      startupWorkRunning: () => startupWorkRunning,
    });
    const decision = yield* Effect.forkChild(
      withinReadBound(
        readCodexAccountLimits({ binaryPath: "codex", cwd: "/", environment: {} }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, codex.spawner),
        ),
      ),
    );
    yield* Deferred.await(stallHandshake);
    // The decision returns at the bound while Codex's startup work still runs.
    yield* TestClock.adjust(READ_BOUND);
    assert.isTrue(Option.isNone(yield* Fiber.join(decision)));
    assert.deepStrictEqual(codex.processGroupChecks, [String(900_000_001)]);
    assert.strictEqual(codex.helpers[0]!.closed, false);
    startupWorkRunning = false;
    yield* TestClock.adjust("500 millis");
    assert.strictEqual(codex.helpers[0]!.closed, true);
  }).pipe(Effect.provideService(HostProcess.Platform, "linux")),
);

it("maps current Codex model capability fields", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "super-high",
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    availableAccessPrograms: { cyber: ["standard", "daybreakBlue"] },
    defaultServiceTier: "flex",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "Lower latency responses.",
      },
      {
        id: "flex",
        name: "Flex",
        description: "Lower-cost asynchronous routing.",
      },
    ],
    supportedReasoningEfforts: [
      {
        description: "Maximum reasoning",
        reasoningEffort: "super-high",
      },
    ],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [{ id: "super-high", label: "super-high", isDefault: true }],
      currentValue: "super-high",
    },
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard" },
        {
          id: "priority",
          label: "Fast",
          description: "Lower latency responses.",
        },
        {
          id: "flex",
          label: "Flex",
          description: "Lower-cost asynchronous routing.",
          isDefault: true,
        },
      ],
      currentValue: "flex",
    },
    {
      id: "cyberAccessProgram",
      label: "Cyber Access",
      type: "select",
      options: [
        { id: "standard", label: "Standard", isDefault: true },
        { id: "daybreakBlue", label: "Daybreak Blue" },
      ],
      currentValue: "standard",
    },
  ]);
});

it("uses standard routing when the catalog has no default service tier", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: ["fast"],
    defaultReasoningEffort: "medium",
    defaultServiceTier: null,
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "1.5x speed, increased usage",
      },
      {
        id: "ultrafast",
        name: "Ultrafast",
        description: "The fastest available responses for latency-sensitive work.",
      },
    ],
    supportedReasoningEfforts: [],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard", isDefault: true },
        {
          id: "priority",
          label: "Fast",
          description: "1.5x speed, increased usage",
        },
        {
          id: "ultrafast",
          label: "Ultrafast",
          description: "Even faster, more expensive",
        },
      ],
      currentValue: "default",
    },
  ]);
});

it("marks the most preferred available model as default", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(
    models.map((model) => ({ slug: model.slug, isDefault: model.isDefault })),
    [
      { slug: "gpt-5.6-terra", isDefault: true },
      { slug: "gpt-5.4", isDefault: undefined },
    ],
  );
});

it("ranks qualified Codex models while preserving their wire ids", () => {
  const models = applyPreferredCodexDefaultModel([
    {
      slug: "openai.gpt-5.6-luna",
      name: "Luna",
      isCustom: false,
      isDefault: true,
      capabilities: null,
    },
    { slug: "openai.gpt-5.6-sol", name: "Sol", isCustom: false, capabilities: null },
  ]);
  assert.deepStrictEqual(
    models.filter((model) => model.isDefault).map((model) => model.slug),
    ["openai.gpt-5.6-sol"],
  );
});

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it("reports native quota only for an OpenAI login on a native route", () => {
  const chatgpt = { type: "chatgpt", email: "a@example.com", planType: "pro" } as const;
  const hub = {
    model_provider: "hub",
    model_providers: { hub: { base_url: "https://hub.example.com/v1" } },
  };
  const cases = [
    ["default route", chatgpt, true, {}, true],
    ["openai base url", chatgpt, true, { openai_base_url: "https://chatgpt.com/x" }, true],
    ["openai proxy url", chatgpt, true, { openai_base_url: "https://proxy.example.com" }, false],
    ["custom provider", chatgpt, true, hub, false],
    ["unknown provider id", chatgpt, true, { model_provider: "missing" }, false],
    ["unreadable config", chatgpt, true, null, false],
    ["login not required", chatgpt, false, {}, false],
    ["api key", { type: "apiKey" }, true, {}, false],
  ] as const;
  for (const [name, account, requiresOpenaiAuth, config, expected] of cases) {
    assert.strictEqual(
      codexRoutesSpendNativeQuota({ account, requiresOpenaiAuth }, config),
      expected,
      name,
    );
  }
  // The custom endpoint is what a usage-limit hub is matched against.
  assert.strictEqual(codexCustomEndpoint(hub), "https://hub.example.com");
  assert.strictEqual(
    codexCustomEndpoint({ openai_base_url: "http://127.0.0.1:8317/v1" }),
    "http://127.0.0.1:8317",
  );
  assert.strictEqual(codexCustomEndpoint({ openai_base_url: "https://chatgpt.com/x" }), undefined);
  assert.strictEqual(codexCustomEndpoint({}), undefined);
});
