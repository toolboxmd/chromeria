import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { type ProviderInstanceId } from "@t3tools/contracts";
import { BareLaunchError, writeBareConfig } from "./bareLaunch.ts";

const encodeConfig = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const bareHome = (stateDir: string, instanceId: ProviderInstanceId, harness: string) =>
  `${stateDir}/bare/${encodeURIComponent(instanceId)}/${harness}`;

export const CODEX_BARE_CONFIG = `project_doc_max_bytes = 0
include_permissions_instructions = false
include_apps_instructions = false
include_collaboration_mode_instructions = false
include_environment_context = false
web_search = "disabled"
[skills]
include_instructions = false
[features]
apps = false
plugins = false
remote_plugin = false
hooks = false
memories = false
multi_agent = false
goals = false
browser_use = false
browser_use_external = false
computer_use = false
in_app_browser = false
image_generation = false
tool_suggest = false
skill_search = false
skill_mcp_dependency_install = false
skip_host_skill_discovery = true
[features.multi_agent_v2]
root_agent_usage_hint_text = ""
multi_agent_mode_hint_text = ""
`;

/** Native stores stay their owner's stores; only a link is created here. */
export const linkBareAuth = Effect.fn("BareProviderRuntime.linkAuth")(function* (
  source: string,
  target: string,
  sourceMayBeMissing = false,
) {
  const fs = yield* FileSystem.FileSystem;
  if (!sourceMayBeMissing && !(yield* fs.exists(source)))
    return yield* new BareLaunchError({
      reason:
        "Native file authentication is required for this bare runtime; keyring-only authentication is unsupported.",
    });
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(target), { recursive: true, mode: 0o700 });
  const verify = fs.readLink(target).pipe(
    Effect.mapError(
      (cause) =>
        new BareLaunchError({
          reason: "Bare auth target must be a link to its existing credential owner.",
          cause,
        }),
    ),
    Effect.flatMap((link) =>
      link === source
        ? Effect.void
        : new BareLaunchError({
            reason: "Bare auth link points to a different credential owner.",
          }),
    ),
  );
  // Concurrent bare sessions can create the same link. A collision must verify
  // the existing target, never replace an account file or trust a stale link.
  yield* fs.symlink(source, target).pipe(
    Effect.catchIf(
      (error) => error.reason._tag === "AlreadyExists",
      () => verify,
    ),
  );
}, Effect.provide(Path.layer));
export const nativeHome = (environment: NodeJS.ProcessEnv, key: string, folder: string) =>
  environment[key] || `${environment.HOME || NodeOS.homedir()}/${folder}`;

export const prepareCodexBareHome = Effect.fn("BareProviderRuntime.codex")(function* (input: {
  home: string;
  sourceHome: string;
  managed: boolean;
}) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(input.home, { recursive: true, mode: 0o700 });
  yield* fs.chmod(input.home, 0o700);
  if (!input.managed)
    yield* linkBareAuth(`${input.sourceHome}/auth.json`, `${input.home}/auth.json`);
  else yield* fs.remove(`${input.home}/auth.json`, { force: true });
  yield* writeBareConfig(
    `${input.home}/config.toml`,
    `${input.managed ? "" : 'cli_auth_credentials_store = "file"\n'}${CODEX_BARE_CONFIG}`,
  );
});

const OPEN_CODE_BARE_TOOLS = {
  "*": false,
  bash: true,
  read: true,
  write: true,
  edit: true,
  glob: true,
  grep: true,
  list: true,
} as const;
export const OPEN_CODE_BARE_CONFIG = {
  instructions: [],
  plugin: [],
  mcp: {},
  agent: {
    build: { prompt: ".", tools: OPEN_CODE_BARE_TOOLS },
    plan: { prompt: ".", tools: OPEN_CODE_BARE_TOOLS },
    title: { disable: true },
  },
};
export const prepareOpenCodeBareEnvironment = Effect.fn("BareProviderRuntime.openCode")(
  function* (input: { home: string; environment: NodeJS.ProcessEnv; namedMcp?: boolean }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const source =
      input.environment.XDG_CONFIG_HOME ||
      path.join(input.environment.HOME || NodeOS.homedir(), ".config");
    const target = path.join(input.home, "xdg-config");
    yield* fs.makeDirectory(target, { recursive: true, mode: 0o700 });
    yield* fs.chmod(input.home, 0o700);
    yield* fs.chmod(target, 0o700);
    if (yield* fs.exists(source)) {
      for (const entry of yield* fs.readDirectory(source)) {
        if (entry === "opencode") continue;
        const destination = path.join(target, entry);
        if (!(yield* fs.exists(destination)))
          yield* fs.symlink(path.join(source, entry), destination);
      }
    }
    const tools = {
      ...OPEN_CODE_BARE_TOOLS,
      ...(input.namedMcp ? { "t3-code_*": true, "t3_code_*": true } : {}),
    };
    const config = {
      ...OPEN_CODE_BARE_CONFIG,
      agent: {
        ...OPEN_CODE_BARE_CONFIG.agent,
        build: { ...OPEN_CODE_BARE_CONFIG.agent.build, tools },
        plan: { ...OPEN_CODE_BARE_CONFIG.agent.plan, tools },
      },
    };
    const environment: NodeJS.ProcessEnv = {
      ...input.environment,
      XDG_CONFIG_HOME: target,
      OPENCODE_DISABLE_CLAUDE_CODE: "1",
      OPENCODE_DISABLE_EXTERNAL_SKILLS: "1",
      OPENCODE_DISABLE_PROJECT_CONFIG: "1",
      OPENCODE_DISABLE_DEFAULT_PLUGINS: "1",
      OPENCODE_CONFIG_CONTENT: yield* encodeConfig(config).pipe(Effect.orDie),
    };
    // These discovery selectors could otherwise escape the isolated config root.
    delete environment.OPENCODE_CONFIG;
    delete environment.OPENCODE_CONFIG_DIR;
    return environment;
  },
  Effect.provide(Path.layer),
);
