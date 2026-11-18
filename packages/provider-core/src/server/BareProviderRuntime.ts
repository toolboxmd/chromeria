import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { parse as parseJsonc, type ParseError } from "jsonc-parser";
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
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

// OpenCode 1.x reads all three in this order; OpenCode 2 reads the last two.
const OPEN_CODE_GLOBAL_CONFIG_FILES = ["config.json", "opencode.json", "opencode.jsonc"];
const OPEN_CODE_PROVIDER_KEYS = ["provider", "providers"];

/**
 * OpenCode resolves a relative `{file:...}` beside its config document, which bare moves.
 * A path starting with `{env:...}` is left alone: OpenCode expands it before deciding relativity.
 */
const anchorFileReferences = (value: unknown, directory: string, path: Path.Path): unknown =>
  typeof value === "string"
    ? value.replace(/\{file:([^}]+)\}/g, (reference, file: string) =>
        file.startsWith("~") || file.startsWith("{") || path.isAbsolute(file)
          ? reference
          : `{file:${path.resolve(directory, file)}}`,
      )
    : Array.isArray(value)
      ? value.map((entry) => anchorFileReferences(entry, directory, path))
      : isRecord(value)
        ? Object.fromEntries(
            Object.entries(value).map(([key, entry]) => [
              key,
              anchorFileReferences(entry, directory, path),
            ]),
          )
        : value;

/**
 * Provider definitions are model plumbing, not prompt context, so bare keeps them: each global
 * document keeps only its provider keys, so OpenCode still normalizes and orders the documents.
 * Values stay unresolved; `{env:...}` and `{file:...}` resolve in the bare server, never here.
 */
const shadowOpenCodeProviders = Effect.fn("BareProviderRuntime.openCodeProviders")(function* (
  source: string,
  target: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const name of OPEN_CODE_GLOBAL_CONFIG_FILES) {
    const file = path.join(source, name);
    const shadow = path.join(target, name);
    const errors: Array<ParseError> = [];
    const document: unknown = (yield* fs.exists(file))
      ? parseJsonc(yield* fs.readFileString(file), errors, { allowTrailingComma: true })
      : undefined;
    // OpenCode rejects a malformed document; a recovered partial one must not reach bare.
    if (errors.length > 0)
      return yield* new BareLaunchError({ reason: `OpenCode config ${file} is not valid JSON.` });
    const original = isRecord(document)
      ? Object.fromEntries(
          OPEN_CODE_PROVIDER_KEYS.flatMap((key) =>
            isRecord(document[key]) ? [[key, document[key]]] : [],
          ),
        )
      : {};
    const providers = anchorFileReferences(original, source, path);
    const [anchored, raw] = yield* Effect.all([
      encodeConfig(providers),
      encodeConfig(original),
    ]).pipe(Effect.orDie);
    // OpenCode substitutes `{file:...}` in raw text, so a JSON-escaped directory names another file.
    if (anchored !== raw && /["\\\u0000-\u001f]/.test(source))
      return yield* new BareLaunchError({
        reason: `OpenCode config ${file} uses a relative {file:} path that bare cannot move.`,
      });
    if (Object.keys(original).length > 0) yield* writeBareConfig(shadow, anchored);
    else if (yield* fs.exists(shadow)) yield* fs.remove(shadow);
  }
});

export const prepareOpenCodeBareEnvironment = Effect.fn("BareProviderRuntime.openCode")(
  function* (input: {
    home: string;
    environment: NodeJS.ProcessEnv;
    namedMcp?: boolean;
    /** OpenCode 2 reads its own config shape and restricts tools per session instead. */
    config?: unknown;
  }) {
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
    yield* shadowOpenCodeProviders(path.join(source, "opencode"), path.join(target, "opencode"));
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
      OPENCODE_CONFIG_CONTENT: yield* encodeConfig(input.config ?? config).pipe(Effect.orDie),
    };
    // These discovery selectors could otherwise escape the isolated config root.
    delete environment.OPENCODE_CONFIG;
    delete environment.OPENCODE_CONFIG_DIR;
    return environment;
  },
  Effect.provide(Path.layer),
);
