import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import { parseTree, type ParseError } from "jsonc-parser";
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
// OpenCode 1.x reads all three in this order; OpenCode 2 reads the last two.
const OPEN_CODE_GLOBAL_CONFIG_FILES = ["config.json", "opencode.json", "opencode.jsonc"];
const OPEN_CODE_PROVIDER_KEYS = new Set(["provider", "providers"]);

// Names OpenCode loads as context from its global config directory; a linked file there would leak.
const OPEN_CODE_DISCOVERED = new Set([
  ...OPEN_CODE_GLOBAL_CONFIG_FILES,
  ...["agent", "mode", "command", "skill", "plugin", "tool"].flatMap((name) => [name, `${name}s`]),
  "AGENTS.md",
  "package.json",
  "node_modules",
]);

const expandEnv = (text: string, environment: NodeJS.ProcessEnv, escape: boolean) =>
  text.replace(/\{env:([^}]+)\}/g, (_, variable: string) => {
    const value = environment[variable] || "";
    return escape ? JSON.stringify(value).slice(1, -1) : value;
  });

/** The `{file:...}` paths OpenCode reads from a document, in its substitution order. */
const fileReferences = (document: string, environment: NodeJS.ProcessEnv) => {
  const raw = expandEnv(document, environment, false);
  const errors: Array<ParseError> = [];
  // OpenCode 2 retries with JSON-escaped values only when raw ones break the document.
  if (document.includes("{env:"))
    parseTree(raw.replace(/\{file:[^}]+\}/g, ""), errors, { allowTrailingComma: true });
  const expanded = errors.length > 0 ? expandEnv(document, environment, true) : raw;
  return Array.from(expanded.matchAll(/\{file:([^}]+)\}/g)).flatMap((match) =>
    expanded
      .slice(expanded.lastIndexOf("\n", match.index - 1) + 1, match.index)
      .trimStart()
      .startsWith("//")
      ? []
      : [match[1]!],
  );
};

/**
 * Provider definitions are model plumbing, not prompt context, so bare keeps them. Each global
 * document keeps its provider sections as raw text, so OpenCode still orders, normalizes and
 * substitutes them itself; `{env:...}` and `{file:...}` never resolve in T3 Code. A relative
 * `{file:...}` resolves beside its document, so each one is linked at the same relative path.
 */
const shadowOpenCodeProviders = Effect.fn("BareProviderRuntime.openCodeProviders")(function* (
  source: string,
  home: string,
  environment: NodeJS.ProcessEnv,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.join(home, "xdg-config");
  const target = path.join(root, "opencode");
  const inside = (parent: string, child: string) => child.startsWith(`${parent}${path.sep}`);
  const links = new Map<string, string>();
  for (const name of OPEN_CODE_GLOBAL_CONFIG_FILES) {
    const file = path.join(source, name);
    const shadow = path.join(target, name);
    const errors: Array<ParseError> = [];
    const text = (yield* fs.exists(file)) ? yield* fs.readFileString(file) : undefined;
    const tree =
      text === undefined ? undefined : parseTree(text, errors, { allowTrailingComma: true });
    // Bare reads the document before OpenCode substitutes it, so it must already be valid JSONC.
    if (errors.length > 0)
      return yield* new BareLaunchError({ reason: `OpenCode config ${file} is not valid JSON.` });
    const sections = (tree?.type === "object" ? (tree.children ?? []) : []).flatMap((property) => {
      const [key, value] = property.children ?? [];
      return key !== undefined &&
        value?.type === "object" &&
        OPEN_CODE_PROVIDER_KEYS.has(String(key.value))
        ? [`"${key.value}": ${text!.slice(value.offset, value.offset + value.length)}`]
        : [];
    });
    if (sections.length === 0) {
      if (yield* fs.exists(shadow)) yield* fs.remove(shadow);
      continue;
    }
    const document = `{\n${sections.join(",\n")}\n}\n`;
    yield* writeBareConfig(shadow, document);
    for (const reference of fileReferences(document, environment)) {
      if (reference.startsWith("~/") || path.isAbsolute(reference)) continue;
      const link = path.resolve(target, reference);
      if (!inside(home, link))
        return yield* new BareLaunchError({
          reason: `OpenCode config ${file} reads a file too far outside its directory for bare.`,
        });
      // The rest of the XDG root already links each original entry.
      if (inside(root, link) && !inside(target, link)) continue;
      if (
        inside(target, link) &&
        OPEN_CODE_DISCOVERED.has(path.relative(target, link).split(path.sep)[0]!)
      )
        return yield* new BareLaunchError({
          reason: `OpenCode config ${file} reads a file where OpenCode loads context, which bare strips.`,
        });
      links.set(link, path.resolve(source, reference));
    }
  }
  for (const [link, original] of links) {
    const current = yield* fs.readLink(link).pipe(Effect.option);
    if (Option.isSome(current) && current.value === original) continue;
    if (Option.isSome(current)) yield* fs.remove(link);
    yield* fs.makeDirectory(path.dirname(link), { recursive: true, mode: 0o700 });
    yield* fs.symlink(original, link);
  }
  // Drop links a reference no longer needs; the mirrored XDG root is maintained elsewhere.
  const owned = [
    ...((yield* fs.exists(target)) ? yield* fs.readDirectory(target, { recursive: true }) : []).map(
      (entry) => path.join(target, entry),
    ),
    ...(yield* fs.readDirectory(home, { recursive: true }))
      .filter((entry) => entry !== "xdg-config" && !entry.startsWith(`xdg-config${path.sep}`))
      .map((entry) => path.join(home, entry)),
  ];
  for (const entry of owned)
    if (!links.has(entry) && Option.isSome(yield* fs.readLink(entry).pipe(Effect.option)))
      yield* fs.remove(entry);
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
    yield* shadowOpenCodeProviders(path.join(source, "opencode"), input.home, environment);
    return environment;
  },
  Effect.provide(Path.layer),
);
