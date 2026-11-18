import * as Digest from "effect/crypto/Digest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CodexSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeClaudeQueryOptions } from "./ClaudeAdapterV2.ts";
import {
  codexThreadRuntimeParams,
  buildCodexTurnStartParams,
  makeCodexAdapterV2,
  type CodexAppServerClientFactoryShape,
} from "./CodexAdapterV2.ts";
import * as ServerConfig from "../../config.ts";
import { makeCursorAgentOptions } from "../../../../../packages/provider-cursor/src/server/adapter.ts";
import { openCodePermissionRules } from "../../../../../packages/provider-opencode/src/server/adapter.ts";
import {
  CODEX_BARE_CONFIG,
  OPEN_CODE_BARE_CONFIG,
  prepareCodexBareHome,
  prepareOpenCodeBareEnvironment,
} from "@t3tools/provider-core/server/BareProviderRuntime";
import {
  GROK_BARE_DISALLOWED,
  GROK_BARE_TOOLS,
  GROK_BARE_COMMANDS,
  grokBareSessionMeta,
  enabledGrokBareHook,
  grokInventoryStable,
} from "../../../../../packages/provider-grok/src/server/GrokBareRuntime.ts";
import {
  grokAcpSpawnArgs,
  buildGrokAcpSpawnInput,
} from "../../../../../packages/provider-grok/src/server/acpSupport.ts";

const threadId = ThreadId.make("thread:bare-options");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "test" };
const bare = { additions: [] };
const decodeCodexSettings = Schema.decodeEffect(CodexSettings);
const policy = (runtimeMode: RuntimeMode) => ({
  runtimeMode,
  interactionMode: "default" as const,
  cwd: "/workspace",
  bare,
});

it("Claude replaces discovery and system content while preserving a restrictive admitted mode", () => {
  const options = makeClaudeQueryOptions({
    bare,
    modelSelection: selection,
    nativeThreadId: "native",
    resume: true,
    cwd: "/workspace",
    permissionMode: "default",
    environment: { HOME: "/real" },
  });
  expect(options).toMatchObject({
    resume: "native",
    systemPrompt: ".",
    settingSources: [],
    strictMcpConfig: true,
    skills: [],
    mcpServers: {},
    permissionMode: "default",
    extraArgs: { "disable-slash-commands": null },
    env: {
      HOME: "/real",
      CLAUDE_CODE_DISABLE_CLAUDE_MDS: "1",
      CLAUDE_CODE_DISABLE_AUTO_MEMORY: "1",
    },
  });
  expect(options.tools).toEqual(["Bash", "Read", "Write", "Edit", "Glob", "Grep"]);
  const normal = makeClaudeQueryOptions({
    modelSelection: selection,
    nativeThreadId: "normal",
    resume: false,
    cwd: null,
  });
  expect(normal.systemPrompt).toMatchObject({ type: "preset", preset: "claude_code" });
  expect(normal).not.toHaveProperty("strictMcpConfig");
});

it("Claude attaches the scoped T3 endpoint alongside explicitly configured private servers", () => {
  const t3 = {
    type: "http" as const,
    url: "http://127.0.0.1/mcp",
    headers: { Authorization: "Bearer fake-bare-credential" },
  };
  const options = makeClaudeQueryOptions({
    bare: { additions: [{ type: "mcp", server: "t3-code", tool: "preview_open" }] },
    modelSelection: { instanceId: ProviderInstanceId.make("claudeAgent"), model: "claude-opus" },
    permissionMode: "bypassPermissions",
    nativeThreadId: "bare-tools",
    resume: false,
    cwd: "/workspace",
    mcpServers: { "t3-code": t3 },
    toolPolicy: {
      allow: ["mcp:t3-code:preview_open", "mcp:outreach:submit"],
      mcpConfigId: "private",
    },
    threadMcpServers: { outreach: { type: "http", url: "http://127.0.0.1/outreach" } },
  });
  expect(options.strictMcpConfig).toBe(true);
  expect(options.mcpServers?.["t3-code"]).toEqual(t3);
  expect(options.mcpServers?.outreach).toBeDefined();
});

it.effect(
  "Codex clears context at the thread and turn seams without raising approvals or sandbox",
  () =>
    Effect.gen(function* () {
      const params = codexThreadRuntimeParams({
        threadId: null,
        mcpSession: undefined,
        modelSelection: selection,
        runtimePolicy: policy("approval-required"),
      });
      expect(params).toMatchObject({
        baseInstructions: "",
        developerInstructions: null,
        config: { "tools.update_plan.enabled": false },
      });
      expect(params.config).not.toHaveProperty("mcp_servers");
      const turn = yield* buildCodexTurnStartParams({
        nativeThreadId: "native",
        codexInput: [{ type: "text", text: "Task", text_elements: [] }],
        modelSelection: selection,
        runtimePolicy: { ...policy("approval-required"), interactionMode: "plan" },
        hasT3Mcp: true,
        appContext: [{ key: "note", text: "Do not inject" }],
      });
      expect(turn).not.toHaveProperty("additionalContext");
      expect(turn).not.toHaveProperty("collaborationMode");
      expect(turn.approvalPolicy).not.toBe("never");
      expect(turn.sandboxPolicy?.type).not.toBe("dangerFullAccess");
      expect(
        codexThreadRuntimeParams({ threadId: null, mcpSession: undefined }).config[
          "tools.update_plan.enabled"
        ],
      ).toBe(true);
    }),
);

it("Cursor uses a positive tool list on the common create/resume options and leaves the prompt gate unknown", () => {
  const options = makeCursorAgentOptions({
    threadId,
    mcpSession: undefined,
    modelSelection: selection,
    platform: "linux",
    runtimePolicy: policy("approval-required"),
  });
  expect(options.tools).toEqual(["shell", "read", "edit", "grep", "glob", "ls", "delete"]);
  expect(options.local).not.toHaveProperty("settingSources");
  expect(options).not.toHaveProperty("systemPrompt");
  expect(options.local?.sandboxOptions?.enabled).toBe(true);
  const normal = makeCursorAgentOptions({
    threadId,
    mcpSession: undefined,
    modelSelection: selection,
    platform: "linux",
    runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: "/workspace" },
  });
  expect(normal.local?.settingSources).toContain("project");
  expect(normal).not.toHaveProperty("tools");
});

it.effect.each(["existing", "managed"] as const)(
  "Codex isolates concurrent bare thread homes and preserves authentication and normal launches: %s",
  (setupMode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const serverConfig = yield* ServerConfig.ServerConfig;
      const sourceHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-auth-owner-" });
      yield* fs.writeFileString(`${sourceHome}/auth.json`, "FAKE_TEST_CREDENTIAL");
      const settings = yield* decodeCodexSettings({
        setupMode,
        homePath: sourceHome,
      });
      const launchArgs = "-c 'model_provider=managed-test'";
      const environment = {
        ...(setupMode === "managed" ? { ACCESS_TOKEN: "FAKE_MANAGED_TOKEN" } : {}),
        T3CODE_CODEX_LAUNCH_ARGS: "unwanted override",
      };
      const config = { ...settings, launchArgs };
      const spawned = new Map<
        ProviderSessionId,
        Parameters<CodexAppServerClientFactoryShape["open"]>[0]
      >();
      const launchFor = (sessionId: string) => {
        const launch = spawned.get(ProviderSessionId.make(sessionId));
        if (launch === undefined)
          throw new Error(`Session ${sessionId} never reached the process boundary.`);
        return launch;
      };
      const adapter = yield* makeCodexAdapterV2({
        instanceId: selection.instanceId,
        settings,
        environment: {},
        resolveRuntime: Effect.succeed({
          revision: "test",
          config,
          environment,
        }),
        digest: yield* Digest.Digest,
        fileSystem: fs,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig,
        clientFactory: {
          open: (input) => {
            spawned.set(input.providerSessionId, input);
            return Effect.fail(
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: ProviderDriverKind.make("codex"),
                providerSessionId: input.providerSessionId,
                cause: "Stopped at the process boundary in this test",
              }),
            );
          },
        },
      });
      const open = (threadId: ThreadId, sessionId: string, isBare: boolean) =>
        adapter
          .openSession({
            threadId,
            providerSessionId: ProviderSessionId.make(sessionId),
            modelSelection: selection,
            runtimePolicy: {
              runtimeMode: "approval-required",
              interactionMode: "default",
              cwd: "/workspace",
              ...(isBare ? { bare: { ...bare, nativeHomeOwnerThreadId: threadId } } : {}),
            },
          })
          .pipe(Effect.flip);
      const otherThreadId = ThreadId.make(`thread:bare-other:${"nested-delegate:".repeat(25)}`);
      yield* Effect.all(
        [open(threadId, "bare-first", true), open(otherThreadId, "bare-second", true)],
        { concurrency: "unbounded" },
      );
      const first = launchFor("bare-first");
      const second = launchFor("bare-second");
      expect(first.settings.homePath).not.toBe(second.settings.homePath);
      for (const [launch, suffix] of [
        [first, "d7de77e952284741465194c614e01e70df21c9aa8097958cceb67c2629d8ddea"],
        [second, "d1347d04b2508200fbf31366dc505863ffb70acfa7969e982a5e9a808ee51dba"],
      ] as const) {
        const home = `${serverConfig.stateDir}/bare/codex/codex/thread-${suffix}`;
        expect(launch.settings.homePath).toBe(home);
        expect(launch.environment.CODEX_HOME).toBe(home);
        expect(launch.environment).not.toHaveProperty("T3CODE_CODEX_LAUNCH_ARGS");
        expect(launch.settings.launchArgs.trim()).toBe(
          setupMode === "managed" ? `${launchArgs} --strict-config` : "--strict-config",
        );
        expect((yield* fs.stat(home)).mode & 0o777).toBe(0o700);
        const strictConfig = yield* fs.readFileString(`${home}/config.toml`);
        expect(strictConfig).toContain("[skills]\ninclude_instructions = false");
        expect(strictConfig).toContain("skip_host_skill_discovery = true");
        if (setupMode === "managed") {
          expect(launch.environment.ACCESS_TOKEN).toBe("FAKE_MANAGED_TOKEN");
          expect(yield* fs.exists(`${home}/auth.json`)).toBe(false);
        } else {
          expect(strictConfig).toContain('cli_auth_credentials_store = "file"');
          expect(yield* fs.readLink(`${home}/auth.json`)).toBe(`${sourceHome}/auth.json`);
        }
      }
      yield* open(threadId, "bare-reopened", true);
      expect(launchFor("bare-reopened").settings.homePath).toBe(first.settings.homePath);
      yield* open(otherThreadId, "normal", false);
      const normal = launchFor("normal");
      expect(normal.settings).toEqual(config);
      expect(normal.environment).toEqual(environment);
      expect(yield* fs.readFileString(`${sourceHome}/auth.json`)).toBe("FAKE_TEST_CREDENTIAL");
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          Layer.merge(NodeServices.layer, NodeCrypto.layer),
          IdAllocator.layer,
          McpProviderSessions.layer,
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-bare-codex-threads-" }).pipe(
            Layer.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
    ),
);

it("OpenCode filters effective permissions instead of re-enabling every tool with a session wildcard", () => {
  expect(OPEN_CODE_BARE_CONFIG.agent.build.prompt).toBe(".");
  expect(OPEN_CODE_BARE_CONFIG.agent.plan.prompt).toBe(".");
  const rules = openCodePermissionRules(policy("full-access"));
  const action = (name: string) =>
    rules
      .toReversed()
      .find((rule) => rule.pattern === "*" && (rule.permission === name || rule.permission === "*"))
      ?.action;
  expect(action("bash")).toBe("allow");
  expect(action("read")).toBe("allow");
  expect(action("task")).toBe("deny");
  expect(action("webfetch")).toBe("deny");
  const plan = openCodePermissionRules(policy("full-access"), {
    agent: "plan",
    plansDirectory: "/data/plans",
    plansPattern: "../../data/plans/*.md",
  });
  expect(
    plan.toReversed().find((rule) => rule.permission === "edit" && rule.pattern === "*")?.action,
  ).toBe("deny");
  expect(plan).toContainEqual({
    permission: "edit",
    pattern: ".opencode/plans/*.md",
    action: "allow",
  });
  const restricted = openCodePermissionRules(policy("approval-required"));
  expect(restricted.toReversed().find((rule) => rule.permission === "bash")?.action).toBe("ask");
  expect(
    openCodePermissionRules({
      runtimeMode: "full-access",
      interactionMode: "default",
      cwd: "/workspace",
    }),
  ).toEqual([
    { permission: "*", pattern: "*", action: "allow" },
    { permission: "external_directory", pattern: "*", action: "allow" },
  ]);
});

it("Grok keeps mode-derived argv and gates both tool and command identities", () => {
  expect(buildGrokAcpSpawnInput(null, "/cwd", {}, "approval-required", true).args).toEqual([
    "--permission-mode",
    "default",
    "agent",
    "--no-leader",
    "stdio",
  ]);
  expect(grokAcpSpawnArgs("full-access")).toEqual(["agent", "--always-approve", "stdio"]);
  expect(grokBareSessionMeta(false)).not.toHaveProperty("yoloMode");
  expect(grokBareSessionMeta(true)).toHaveProperty("yoloMode", true);
  expect(GROK_BARE_DISALLOWED).toContain("web_fetch");
  expect(GROK_BARE_TOOLS).toContain("run_terminal_command");
  expect(GROK_BARE_COMMANDS).toContain("reload-plugins");
  const inventory = {
    skills: [{ name: "skill", disabled: true }],
    plugins: [{ name: "plugin", enabled: true }],
    hooks: [{ event: "(plugin)", source: { type: "plugin", plugin_name: "plugin" } }],
  };
  expect(grokInventoryStable(inventory, new Set(["skill"]), new Set(["plugin"]))).toBe(true);
  expect(enabledGrokBareHook(inventory, new Set(["plugin"]))).toBeUndefined();
  expect(
    enabledGrokBareHook(
      { ...inventory, hooks: [{ event: "pre_tool_use", source: { type: "user" } }] },
      new Set(["plugin"]),
    )?.event,
  ).toBe("pre_tool_use");
});

it.layer(Layer.merge(NodeServices.layer, NodeCrypto.layer))("bare homes", (it) => {
  it.effect(
    "links the native credential owner and leaves managed authentication without auth.json",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        yield* fs.makeDirectory(`${root}/native`);
        yield* fs.writeFileString(`${root}/native/auth.json`, "FAKE_TEST_CREDENTIAL");
        yield* Effect.forEach(
          [1, 2],
          () =>
            prepareCodexBareHome({
              home: `${root}/bare`,
              sourceHome: `${root}/native`,
              managed: false,
            }),
          { concurrency: "unbounded" },
        );
        expect(yield* fs.readLink(`${root}/bare/auth.json`)).toBe(`${root}/native/auth.json`);
        expect(yield* fs.readFileString(`${root}/bare/config.toml`)).toContain(
          "[skills]\ninclude_instructions = false",
        );
        expect(CODEX_BARE_CONFIG).toContain("skip_host_skill_discovery = true");
        yield* prepareCodexBareHome({
          home: `${root}/managed`,
          sourceHome: `${root}/missing`,
          managed: true,
        });
        expect(yield* fs.exists(`${root}/managed/auth.json`)).toBe(false);
        yield* prepareCodexBareHome({
          home: `${root}/bare`,
          sourceHome: `${root}/native`,
          managed: true,
        });
        expect(yield* fs.exists(`${root}/bare/auth.json`)).toBe(false);
        expect(yield* fs.exists(`${root}/native/auth.json`)).toBe(true);
        expect((yield* fs.stat(`${root}/bare`)).mode & 0o777).toBe(0o700);
      }),
  );
  it.effect("shadows only OpenCode config while preserving native shell app configuration", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped();
      const real = `${root}/real`;
      yield* fs.makeDirectory(`${real}/opencode`, { recursive: true });
      yield* fs.makeDirectory(`${real}/gh`);
      yield* fs.writeFileString(
        `${real}/opencode/config.json`,
        JSON.stringify({ provider: { legacy: { name: "Legacy" } }, theme: "dark" }),
      );
      yield* fs.writeFileString(
        `${real}/opencode/opencode.json`,
        JSON.stringify({
          providers: { omlx: { name: "old", settings: { baseURL: "http://old/v1" } } },
          mcp: { secret: { type: "local", command: ["serve"] } },
        }),
      );
      const provider = `{ "omlx": { "options": {
            "baseURL": "http://new/v1",
            "apiKey": "{env:OMLX_KEY}",
            "headers": { "a": "{file:~/key}", "b": "{file:/abs/key}", "c": "{file:{env:KEY_PATH}}" },
          } } }`;
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{
          // the user's local model server
          "provider": ${provider},
          "instructions": ["AGENTS.md"],
          "plugin": ["some-plugin"],
          "agent": { "build": { "prompt": "user prompt" } },
        }`,
      );
      const prepare = (environment: NodeJS.ProcessEnv = {}) =>
        prepareOpenCodeBareEnvironment({
          root,
          home: `${root}/bare`,
          environment: {
            HOME: root,
            XDG_CONFIG_HOME: real,
            OPENCODE_CONFIG: "/unwanted/config",
            OPENCODE_CONFIG_DIR: "/unwanted",
            KEY_PATH: "/secrets/key",
            // An ordinary value that looks like a file placeholder must not be refused.
            OMLX_KEY: "q0t3q1t3q2t3",
            ...environment,
          },
        });
      // Earlier builds wrote provider copies, possibly with keys, into T3 state; any launch,
      // even a refused one, removes them first.
      const stale = `${root}/bare/xdg-config/opencode`;
      yield* fs.makeDirectory(stale, { recursive: true });
      for (const name of ["config.json", "opencode.json", "opencode.jsonc"])
        yield* fs.writeFileString(
          `${stale}/${name}`,
          `{ "provider": { "x": { "options": { "apiKey": "sk-stale" } } } }`,
        );
      // An interrupted earlier write left its temporary copy in a directory of its own.
      yield* fs.makeDirectory(`${stale}/.bare-config-abc`);
      yield* fs.writeFileString(`${stale}/.bare-config-abc/123`, "sk-stale");
      yield* fs.writeFileString(`${stale}/.bare-config-def`, "sk-stale");
      // OpenCode deep-merges documents, which one config content cannot, so bare refuses
      // providers spread over several files.
      expect((yield* Effect.flip(prepare())).message).toBe(
        `OpenCode configs ${real}/opencode/config.json and ${real}/opencode/opencode.json and ${real}/opencode/opencode.jsonc both define providers, which bare cannot combine; keep them in one file.`,
      );
      expect(yield* fs.readDirectory(stale)).toEqual([]);
      // OpenCode 2 ignores the legacy `config.json`, so bare does too.
      const openCode2 = yield* Effect.flip(
        prepareOpenCodeBareEnvironment({
          root,
          home: `${root}/bare2`,
          environment: { HOME: root, XDG_CONFIG_HOME: real, OMLX_KEY: "k", KEY_PATH: "/k" },
          config: { mcp: {} },
        }),
      );
      expect(openCode2.message).toBe(
        `OpenCode configs ${real}/opencode/opencode.json and ${real}/opencode/opencode.jsonc both define providers, which bare cannot combine; keep them in one file.`,
      );
      yield* fs.writeFileString(`${real}/opencode/config.json`, JSON.stringify({ theme: "dark" }));
      yield* fs.writeFileString(
        `${real}/opencode/opencode.json`,
        JSON.stringify({ mcp: { secret: { type: "local", command: ["serve"] } } }),
      );
      const environment = yield* prepare();
      expect(yield* fs.readLink(`${environment.XDG_CONFIG_HOME}/gh`)).toBe(`${real}/gh`);
      expect(environment.HOME).toBe(root);
      expect(environment).not.toHaveProperty("OPENCODE_CONFIG");
      // The raw provider sections travel in memory beside the bare config, so OpenCode still
      // normalizes and substitutes them; absolute and home file paths read the same file.
      const strip = JSON.stringify(OPEN_CODE_BARE_CONFIG).slice(1);
      expect(environment.OPENCODE_CONFIG_CONTENT).toBe(`{\n"provider": ${provider},\n${strip}`);
      const content = (sections: string) => `{\n${sections},\n${strip}`;

      // A relative path would no longer resolve beside its config file, so bare refuses it,
      // even when only an env value makes it relative.
      const relative = `OpenCode config ${real}/opencode/opencode.jsonc has a provider {file:} path relative to its folder, which bare cannot read; use an absolute or ~/ path.`;
      expect((yield* Effect.flip(prepare({ KEY_PATH: "./key" }))).message).toBe(relative);
      const refused = (reason: string) =>
        `OpenCode config ${real}/opencode/opencode.jsonc ${reason}`;
      // OpenCode 2 re-expands the whole document escaped when a raw value breaks it, so
      // bare refuses a value that is not plain string content anywhere in the document.
      expect((yield* Effect.flip(prepare({ OMLX_KEY: 'say "hi"' }))).message).toBe(
        refused(
          "has an {env:} value that is not plain JSON string content, or a {file:} token outside a JSON string, which bare cannot copy.",
        ),
      );
      expect(
        (yield* Effect.flip(
          prepare({
            OMLX_KEY:
              'k"}}},"instructions":["/context.md"],"provider":{"x":{"options":{"apiKey":"z',
          }),
        )).message,
      ).toBe(refused("has an {env:} value that changes the provider structure."));
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "provider": { "omlx": { "options": { "apiKey": "{file:{env:XDG_CONFIG_HOME}/key}" } } } }`,
      );
      expect((yield* Effect.flip(prepare())).message).toBe(
        refused("has a provider using {env:XDG_CONFIG_HOME}, which bare changes."),
      );
      // File content replaces a token before OpenCode parses, so one outside a string, even in a
      // comment of a dropped field, could reshape the document or flip OpenCode 2's escaping.
      const outsideString = refused(
        "has an {env:} value that is not plain JSON string content, or a {file:} token outside a JSON string, which bare cannot copy.",
      );
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "providers": { /* {file:/break} */ "local": { "name": "Local" } } }`,
      );
      expect((yield* Effect.flip(prepare())).message).toBe(outsideString);
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "providers": { "local": { "settings": { "apiKey": "{env:OMLX_KEY}" } } }, "instructions": [] /* {file:/c} */ }`,
      );
      expect((yield* Effect.flip(prepare())).message).toBe(outsideString);
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "providers": { "local": { "name": "q0t3" /* {file:/break} */ } } }`,
      );
      expect((yield* Effect.flip(prepare())).message).toBe(outsideString);
      // Bare picks provider sections before substitution, so an env-built key must be refused,
      // also when it is the document's only provider.
      for (const document of [
        `{ "providers": {}, "{env:SECTION}": { "local": { "name": "Local" } } }`,
        `{ "{env:SECTION}": { "local": { "name": "Local" } } }`,
        `{ "{file:/section}": { "local": { "name": "Local" } } }`,
      ]) {
        yield* fs.writeFileString(`${real}/opencode/opencode.jsonc`, document);
        expect((yield* Effect.flip(prepare({ SECTION: "provider" }))).message).toBe(
          refused("has an {env:} value that changes the provider structure."),
        );
      }
      // A later `null` overrides an earlier provider object in OpenCode, so it is copied too.
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "provider": { "old": { "name": "Old" } }, "provider": null }`,
      );
      expect((yield* prepare()).OPENCODE_CONFIG_CONTENT).toBe(
        content(`"provider": { "old": { "name": "Old" } },\n"provider": null`),
      );
      // Nothing from the user's config is written to T3 state, so a literal key stays in memory,
      // and comments, which could hold an old key, are dropped.
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "provider": { "omlx": {\n// "apiKey": "sk-commented",\n"options": { "apiKey": "sk-literal" } /* old */ } } }`,
      );
      expect((yield* prepare()).OPENCODE_CONFIG_CONTENT).toBe(
        content(`"provider": { "omlx": {\n\n"options": { "apiKey": "sk-literal" }  } }`),
      );
      for (const entry of yield* fs.readDirectory(`${root}/bare`, { recursive: true })) {
        const file = `${root}/bare/${entry}`;
        if ((yield* fs.stat(file)).type === "File")
          expect(yield* fs.readFileString(file)).not.toContain("sk-literal");
      }
      // Placeholder numbers must not prefix each other: eleven file tokens in strings are
      // accepted, and the same with the second one in a comment is refused.
      const headers = (second: string) =>
        `{ "provider": { "omlx": { "options": { "headers": { "h0": "{file:/k0}", ${second}, ${Array.from(
          { length: 9 },
          (_, index) => `"h${index + 2}": "{file:/k${index + 2}}"`,
        ).join(", ")} } } } } }`;
      yield* fs.writeFileString(`${real}/opencode/opencode.jsonc`, headers(`"h1": "{file:/k1}"`));
      yield* prepare();
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        headers(`/* {file:/k1} */ "h1": "x"`),
      );
      expect((yield* Effect.flip(prepare())).message).toBe(outsideString);
      yield* fs.writeFileString(
        `${real}/opencode/opencode.jsonc`,
        `{ "provider": { "omlx": { "options": {} /* {file:./key} */ } } }`,
      );
      expect((yield* Effect.flip(prepare())).message).toBe(outsideString);

      // On case-insensitive filesystems a mirrored `OpenCode` would alias the shadow directory,
      // so bare must neither mirror it nor remove files through an existing link to it.
      const cased = `${root}/cased`;
      yield* fs.makeDirectory(`${cased}/OpenCode`, { recursive: true });
      yield* fs.makeDirectory(`${root}/bare-cased/xdg-config`, { recursive: true });
      yield* fs.symlink(`${cased}/OpenCode`, `${root}/bare-cased/xdg-config/OpenCode`);
      const user = JSON.stringify({ provider: { local: { name: "Local" } }, mcp: {} });
      yield* fs.writeFileString(`${cased}/OpenCode/opencode.json`, user);
      const casedEnvironment = yield* prepareOpenCodeBareEnvironment({
        root,
        home: `${root}/bare-cased`,
        environment: { HOME: root, XDG_CONFIG_HOME: cased },
      });
      expect(yield* fs.readFileString(`${cased}/OpenCode/opencode.json`)).toBe(user);
      expect(
        Option.isNone(
          yield* fs.readLink(`${casedEnvironment.XDG_CONFIG_HOME}/opencode`).pipe(Effect.option),
        ),
      ).toBe(true);

      // Cleanup deletes files, so a bare home reached through a link, at any level, refuses the
      // launch and leaves the linked files alone.
      const victim = `${root}/victim`;
      yield* fs.makeDirectory(`${victim}/xdg-config/opencode`, { recursive: true });
      yield* fs.writeFileString(`${victim}/xdg-config/opencode/opencode.json`, user);
      yield* fs.makeDirectory(`${root}/bare-linked`);
      yield* fs.symlink(`${victim}/xdg-config`, `${root}/bare-linked/xdg-config`);
      yield* fs.makeDirectory(`${root}/state/bare`, { recursive: true });
      yield* fs.symlink(victim, `${root}/state/bare/instance`);
      for (const [bareRoot, home] of [
        [root, `${root}/bare-linked`],
        [`${root}/state/bare`, `${root}/state/bare/instance`],
      ] as const)
        expect(
          (yield* Effect.flip(
            prepareOpenCodeBareEnvironment({
              root: bareRoot,
              home,
              environment: { HOME: root, XDG_CONFIG_HOME: `${root}/empty` },
            }),
          )).message,
        ).toBe(
          `Bare OpenCode home ${home}/xdg-config must be a real directory inside ${bareRoot}, not reached through a link.`,
        );
      expect(yield* fs.readFileString(`${victim}/xdg-config/opencode/opencode.json`)).toBe(user);
      // A linked config file inside the bare home is unlinked, so OpenCode never loads it, and
      // its target survives.
      yield* fs.writeFileString(`${real}/opencode/opencode.jsonc`, `{ "theme": "dark" }`);
      yield* fs.symlink(`${victim}/xdg-config/opencode/opencode.json`, `${stale}/opencode.json`);
      yield* prepare();
      expect(Option.isNone(yield* fs.lstat(`${stale}/opencode.json`).pipe(Effect.option))).toBe(
        true,
      );
      expect(yield* fs.readFileString(`${victim}/xdg-config/opencode/opencode.json`)).toBe(user);

      yield* fs.remove(`${real}/opencode/config.json`);
      yield* fs.writeFileString(`${real}/opencode/opencode.jsonc`, `{ "theme": "dark" }`);
      yield* fs.writeFileString(
        `${real}/opencode/opencode.json`,
        `{ "providers": { "omlx": { "name": "cut`,
      );
      const malformed = yield* Effect.flip(prepare());
      expect(malformed.message).toBe(
        `OpenCode config ${real}/opencode/opencode.json is not valid JSONC before {env:} substitution.`,
      );
    }),
  );
});
