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
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Crypto from "effect/Crypto";
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
        codexThreadRuntimeParams({ mcpSession: undefined }).config["tools.update_plan.enabled"],
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
        crypto: yield* Crypto.Crypto,
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
          NodeServices.layer,
          IdAllocator.layer,
          McpProviderSessions.layer,
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-bare-codex-threads-" }).pipe(
            Layer.provide(NodeServices.layer),
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
  ).toEqual([{ permission: "*", pattern: "*", action: "allow" }]);
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

it.layer(NodeServices.layer)("bare homes", (it) => {
  it.effect(
    "links the native credential owner and leaves managed authentication without auth.json",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        yield* fs.makeDirectory(`${root}/native`);
        yield* fs.writeFileString(`${root}/native/auth.json`, "FAKE_TEST_CREDENTIAL");
        yield* Effect.all(
          [1, 2].map(() =>
            prepareCodexBareHome({
              home: `${root}/bare`,
              sourceHome: `${root}/native`,
              managed: false,
            }),
          ),
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
      yield* fs.makeDirectory(`${root}/real/opencode`, { recursive: true });
      yield* fs.makeDirectory(`${root}/real/gh`);
      const environment = yield* prepareOpenCodeBareEnvironment({
        home: `${root}/bare`,
        environment: {
          HOME: root,
          XDG_CONFIG_HOME: `${root}/real`,
          OPENCODE_CONFIG: "/unwanted/config",
          OPENCODE_CONFIG_DIR: "/unwanted",
        },
      });
      expect(yield* fs.readLink(`${environment.XDG_CONFIG_HOME}/gh`)).toBe(`${root}/real/gh`);
      expect(yield* fs.exists(`${environment.XDG_CONFIG_HOME}/opencode`)).toBe(false);
      expect(environment.HOME).toBe(root);
      expect(environment).not.toHaveProperty("OPENCODE_CONFIG");
      const config = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(
          Schema.Struct({
            agent: Schema.Struct({ plan: Schema.Struct({ prompt: Schema.String }) }),
          }),
        ),
      )(environment.OPENCODE_CONFIG_CONTENT);
      expect(config.agent.plan.prompt).toBe(".");
    }),
  );
});
