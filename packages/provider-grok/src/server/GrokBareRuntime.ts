import * as Path from "effect/Path";
import { sha256 } from "@noble/hashes/sha2";
import { bytesToHex } from "@noble/hashes/utils";
import { type BareEvidence } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import { BareLaunchError, writeBareConfig } from "@t3tools/provider-core/server/bareLaunch";
import { linkBareAuth, nativeHome } from "@t3tools/provider-core/server/BareProviderRuntime";

export const GROK_BARE_TOOLS = [
  "run_terminal_command",
  "read_file",
  "search_replace",
  "list_dir",
  "grep",
  "write",
] as const;
export const GROK_BARE_COMMANDS = [
  "compact",
  "always-approve",
  "context",
  "plugins",
  "reload-plugins",
  "session-info",
  "feedback",
] as const;
export const GROK_BARE_DISALLOWED = [
  "search_tool",
  "use_tool",
  "todo_write",
  "monitor",
  "update_goal",
  "web_search",
  "web_fetch",
  "image_gen",
  "image_edit",
  "image_to_video",
  "reference_to_video",
  "enter_plan_mode",
  "exit_plan_mode",
  "ask_user_question",
] as const;
export const grokBareSessionMeta = (fullAccess: boolean) => ({
  systemPromptOverride: ".",
  ...(fullAccess ? { yoloMode: true } : {}),
  agentProfile: {
    name: "bare",
    description: "Bare launch",
    // The positive list suppresses background/scheduler defaults; the denylist removes auxiliary tools.
    tools: [...GROK_BARE_TOOLS],
    disallowedTools: [...GROK_BARE_DISALLOWED],
  },
});
const Inventory = Schema.Struct({
  skills: Schema.Array(
    Schema.Struct({ name: Schema.String, disabled: Schema.optional(Schema.Boolean) }),
  ),
  plugins: Schema.Array(Schema.Struct({ name: Schema.String })),
  hooks: Schema.Array(
    Schema.Struct({
      event: Schema.String,
      disabled: Schema.optional(Schema.Boolean),
      source: Schema.Struct({ type: Schema.String, plugin_name: Schema.optional(Schema.String) }),
    }),
  ),
});
const decodeInventory = Schema.decodeEffect(Schema.fromJsonString(Inventory));
const encodeNames = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const encodeValue = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
type GrokBareInventory = typeof Inventory.Type;
function grokBareConfig(
  home: string,
  realHome: string,
  skills: ReadonlyArray<string>,
  plugins: ReadonlyArray<string>,
): string {
  return `[workflows]\nenabled = false\n[skills]\nignore = ${encodeNames([`${home}/bundled`, `${realHome}/.agents/skills`])}\ndisabled = ${encodeNames([...skills].sort())}\n[plugins]\ndisabled = ${encodeNames([...plugins].sort())}\n[compat.claude]\nskills = false\nrules = false\nagents = false\nmcps = false\nhooks = false\n[compat.cursor]\nskills = false\nrules = false\nagents = false\nmcps = false\nhooks = false\n[compat.codex]\nskills = false\nhooks = false\n[memory]\nenabled = false\n[memory_v2]\nenabled = false\n[subagents]\nenabled = false\n[features]\nsubagent_bundle = false\ntitle_refresh = false\n`;
}
export function grokInventoryStable(
  inventory: GrokBareInventory,
  skills: ReadonlySet<string>,
  plugins: ReadonlySet<string>,
) {
  return (
    inventory.skills.every((skill) => skills.has(skill.name) && skill.disabled === true) &&
    inventory.plugins.every((plugin) => plugins.has(plugin.name))
  );
}
export function enabledGrokBareHook(inventory: GrokBareInventory, plugins: ReadonlySet<string>) {
  return inventory.hooks.find(
    (hook) =>
      hook.disabled !== true &&
      !(
        hook.source.type === "plugin" &&
        hook.source.plugin_name !== undefined &&
        plugins.has(hook.source.plugin_name)
      ),
  );
}
export const prepareGrokBare = Effect.fn("GrokBareRuntime.prepare")(function* (input: {
  home: string;
  cwd: string;
  environment: NodeJS.ProcessEnv;
  binary: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const path = yield* Path.Path;
  const home = input.home;
  const source = nativeHome(input.environment, "GROK_HOME", ".grok");
  yield* fs.makeDirectory(home, { recursive: true, mode: 0o700 });
  yield* fs.chmod(home, 0o700);
  yield* linkBareAuth(path.join(source, "auth.json"), path.join(home, "auth.json"));
  // Native refresh owns this lock; its path can be absent between refreshes.
  yield* linkBareAuth(path.join(source, "auth.json.lock"), path.join(home, "auth.json.lock"), true);
  const environment = {
    ...input.environment,
    GROK_HOME: home,
    GROK_MEMORY: "0",
    GROK_SUBAGENTS: "0",
    GROK_WORKFLOWS: "0",
  };
  const skills = new Set<string>();
  const plugins = new Set<string>();
  const inspect = Effect.scoped(
    Effect.gen(function* () {
      const process = yield* spawner.spawn(
        ChildProcess.make(input.binary || "grok", ["inspect", "--json"], {
          cwd: input.cwd,
          env: environment,
          extendEnv: false,
        }),
      );
      const [out, err, code] = yield* Effect.all(
        [
          collectUint8StreamText({ stream: process.stdout, maxBytes: 4 * 1024 * 1024 }),
          collectUint8StreamText({ stream: process.stderr, maxBytes: 1024 }),
          process.exitCode,
        ],
        { concurrency: "unbounded" },
      );
      if (Number(code) !== 0 || out.truncated || out.invalidUtf8)
        return yield* new BareLaunchError({
          reason: "Grok bare native inventory failed or exceeded its bound.",
        });
      void err;
      return yield* decodeInventory(out.text).pipe(
        Effect.mapError(
          (cause) =>
            new BareLaunchError({
              reason: "Grok bare native inventory is missing required identities.",
              cause,
            }),
        ),
      );
    }),
  );
  let inventory: GrokBareInventory | undefined;
  for (let pass = 0; pass < 3; pass++) {
    yield* writeBareConfig(
      path.join(home, "config.toml"),
      grokBareConfig(
        home,
        input.environment.HOME || nativeHome(input.environment, "HOME", ""),
        [...skills],
        [...plugins],
      ),
    );
    inventory = yield* inspect;
    if (grokInventoryStable(inventory, skills, plugins)) {
      const hook = enabledGrokBareHook(inventory, plugins);
      if (hook !== undefined)
        return yield* new BareLaunchError({
          reason: `Grok bare cannot disable an enabled ${hook.source.type} hook (${hook.event}) outside a disabled plugin.`,
        });
      return {
        environment,
        skills: [...skills].sort(),
        plugins: [...plugins].sort(),
        observedDisabledSkills: inventory.skills
          .filter((skill) => skill.disabled)
          .map((skill) => skill.name),
      };
    }
    inventory.skills.forEach((skill) => skills.add(skill.name));
    inventory.plugins.forEach((plugin) => plugins.add(plugin.name));
  }
  return yield* new BareLaunchError({
    reason: "Grok bare inventory did not stabilize within three passes.",
  });
}, Effect.provide(Path.layer));
const HistoryEntry = Schema.Struct({ type: Schema.String, content: Schema.Unknown });
const decodeHistoryEntry = Schema.decodeEffect(Schema.fromJsonString(HistoryEntry));
/** Read the native history, rather than mistaking prompt_context for the whole input. */
export const grokBareHistoryEvidence = Effect.fn("GrokBareRuntime.history")(function* (
  home: string,
  cwd: string,
  sessionId: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const record = `${home}/sessions/${encodeURIComponent(cwd)}/${sessionId}/chat_history.jsonl`;
  const info = yield* fs.stat(record);
  if (info.size > BigInt(4 * 1024 * 1024)) return [];
  const text = yield* fs.readFileString(record);
  const entries = yield* Effect.forEach(text.split("\n").filter(Boolean), (line) =>
    decodeHistoryEntry(line),
  );
  const hash = (value: string) => bytesToHex(sha256(new TextEncoder().encode(value)));
  const system = entries.filter((entry) => entry.type === "system");
  const reminders = entries.filter((entry) => entry.type === "system_reminder");
  const evidence: Array<BareEvidence> = [];
  if (system.length > 0)
    evidence.push({
      category: "system_text",
      status: system.every((entry) => entry.content === ".")
        ? "observed_retained"
        : "observed_unexpected",
      identities: system.map((entry) => hash(encodeValue(entry.content))),
      source: record,
    });
  if (reminders.length > 0)
    evidence.push({
      category: "instructions",
      status: "observed_unexpected",
      identities: reminders.map((entry) => hash(encodeValue(entry.content))),
      source: record,
    });
  return evidence;
});
