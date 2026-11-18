import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import { parseTree, stripComments, type Node, type ParseError } from "jsonc-parser";
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

const ENV_TOKEN = /\{env:([^}]+)\}/g;
const FILE_TOKEN = /\{file:([^}]+)\}/g;

const parses = (text: string) => {
  const errors: Array<ParseError> = [];
  const tree = parseTree(text, errors, { allowTrailingComma: true });
  return errors.length > 0 ? undefined : tree;
};

/**
 * Parses OpenCode's raw `{env:...}` expansion with each `{file:...}` held by a unique placeholder,
 * only when each placeholder lands once inside a JSON string: file content then cannot change the
 * parse. A placeholder after a lone backslash is an invalid escape, so that case fails too.
 */
const parseExpanded = (text: string, environment: NodeJS.ProcessEnv, nonce: string) => {
  const markers: Array<string> = [];
  const tree = parses(
    text
      .replace(ENV_TOKEN, (_, variable: string) => environment[variable] || "")
      .replace(FILE_TOKEN, () => {
        markers.push(`q${nonce}x${markers.length}z`);
        return markers.at(-1)!;
      }),
  );
  const strings = (node: Node | undefined): Array<string> =>
    node === undefined
      ? []
      : node.type === "string"
        ? [String(node.value)]
        : (node.children ?? []).flatMap(strings);
  const values = strings(tree);
  return tree !== undefined &&
    markers.every(
      (marker) => values.reduce((count, value) => count + value.split(marker).length - 1, 0) === 1,
    )
    ? tree
    : undefined;
};

/** Every value at `path` over all duplicate keys; `*` matches any key. */
const valuesAt = (node: Node | undefined, [key, ...rest]: ReadonlyArray<string>): Array<Node> =>
  node === undefined
    ? []
    : key === undefined
      ? [node]
      : node.type === "object"
        ? (node.children ?? []).flatMap(({ children: [name, value] = [] }) =>
            key === "*" || name?.value === key ? valuesAt(value, rest) : [],
          )
        : [];
// OpenCode 1.x `provider` entries keep settings in `options`, OpenCode 2 `providers` in `settings`.
const CREDENTIAL_PATHS = ["options", "settings"].flatMap((group) => [
  ["*", group, "apiKey"],
  ["*", group, "headers", "*"],
]);

const topLevelKeys = (tree: Node | undefined) =>
  (tree?.type === "object" ? (tree.children ?? []) : []).map((property) =>
    String(property.children?.[0]?.value),
  );

/**
 * Bare keeps provider definitions as raw text per document so OpenCode substitutes them itself,
 * and refuses anything that would make the copy read differently from the original.
 */
const shadowOpenCodeProviders = Effect.fn("BareProviderRuntime.openCodeProviders")(function* (
  source: string,
  target: string,
  original: NodeJS.ProcessEnv,
  bare: NodeJS.ProcessEnv,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const name of OPEN_CODE_GLOBAL_CONFIG_FILES) {
    const file = path.join(source, name);
    const shadow = path.join(target, name);
    const refuse = (reason: string) =>
      new BareLaunchError({ reason: `OpenCode config ${file} ${reason}` });
    const text = (yield* fs.exists(file)) ? yield* fs.readFileString(file) : undefined;
    const tree = text === undefined ? undefined : parses(text);
    // Bare reads the document before OpenCode substitutes it, so it must already be valid JSONC.
    if (text !== undefined && tree === undefined)
      return yield* refuse("is not valid JSONC before {env:} substitution.");
    const sections = (tree?.type === "object" ? (tree.children ?? []) : []).flatMap((property) => {
      const [key, value] = property.children ?? [];
      // Every occurrence, whatever its value, so JSONC's last-key rule still picks the same one.
      // Comments are dropped so a commented-out key never lands in T3 state.
      return key !== undefined &&
        value !== undefined &&
        OPEN_CODE_PROVIDER_KEYS.has(String(key.value))
        ? [
            `"${key.value}": ${stripComments(text!.slice(value.offset, value.offset + value.length))}`,
          ]
        : [];
    });
    // Random, so no value in the user's config can pose as a placeholder.
    const nonce = (yield* Effect.all([Random.next, Random.next]))
      .map((value) => value.toString(36).slice(2))
      .join("");
    const whole =
      text !== undefined && /\{(env|file):/.test(text)
        ? parseExpanded(text, original, nonce)
        : tree;
    const unparsable = refuse(
      "has an {env:} value that is not plain JSON string content, or a {file:} token outside a JSON string, which bare cannot copy.",
    );
    const restructured = refuse("has an {env:} value that changes the provider structure.");
    if (whole === undefined && text !== undefined) return yield* unparsable;
    // Substitution must not add, remove or rename top-level keys, even in fields bare drops.
    if (topLevelKeys(whole).join("\n") !== topLevelKeys(tree).join("\n"))
      return yield* restructured;
    if (sections.length === 0) {
      if (yield* fs.exists(shadow)) yield* fs.remove(shadow);
      continue;
    }
    const document = `{\n${sections.join(",\n")}\n}\n`;
    const changed = Array.from(document.matchAll(ENV_TOKEN), ([, variable]) => variable!).find(
      (variable) => (original[variable] || "") !== (bare[variable] || ""),
    );
    if (changed !== undefined)
      return yield* refuse(`has a provider using {env:${changed}}, which bare changes.`);
    // A relative path would resolve beside the copy.
    const relative = Array.from(
      document
        .replace(ENV_TOKEN, (_, variable: string) => original[variable] || "")
        .matchAll(FILE_TOKEN),
    ).some(([, reference]) => !reference!.startsWith("~/") && !path.isAbsolute(reference!));
    if (relative)
      return yield* refuse(
        "has a provider {file:} path relative to its folder, which bare cannot read; use an absolute or ~/ path.",
      );
    const expanded = parseExpanded(document, original, nonce);
    if (expanded === undefined) return yield* unparsable;
    const keys = topLevelKeys(expanded);
    if (keys.length !== sections.length || keys.some((key) => !OPEN_CODE_PROVIDER_KEYS.has(key)))
      return yield* restructured;
    // API keys never land in T3 state, so a credential must stay a reference OpenCode resolves.
    const literal = CREDENTIAL_PATHS.flatMap((credential) =>
      (tree?.children ?? []).flatMap(({ children: [key, value] = [] }) =>
        OPEN_CODE_PROVIDER_KEYS.has(String(key?.value)) ? valuesAt(value, credential) : [],
      ),
    ).some((node) => node.type !== "null" && !/\{(env|file):[^}]+\}/.test(String(node.value)));
    if (literal) {
      if (yield* fs.exists(shadow)) yield* fs.remove(shadow);
      return yield* refuse(
        "has a provider with a literal apiKey or header value, which bare will not copy into T3 state; use {env:VAR}, {file:path} or `opencode auth login` instead.",
      );
    }
    yield* writeBareConfig(shadow, document);
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
        // Case-insensitive filesystems treat any case of this name as the shadow directory.
        if (entry.toLowerCase() === "opencode") continue;
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
    // An earlier mirror may hold a case variant that aliases the shadow directory; unlinking
    // removes only the link, never the user's directory.
    for (const entry of yield* fs.readDirectory(target))
      if (
        entry.toLowerCase() === "opencode" &&
        Option.isSome(yield* fs.readLink(path.join(target, entry)).pipe(Effect.option))
      )
        yield* fs.remove(path.join(target, entry));
    yield* shadowOpenCodeProviders(
      path.join(source, "opencode"),
      path.join(target, "opencode"),
      input.environment,
      environment,
    );
    return environment;
  },
  Effect.provide(Path.layer),
);
