import {
  DeviceCommandSnapshot,
  DeviceOperationError,
  type DeviceCommand,
  type DeviceAtomicCommand,
  type DeviceCommandFamilySupport,
  type DevicePlatform,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as ProcessRunner from "../processRunner.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";

const failure = () =>
  new DeviceOperationError({
    operation: "typed command",
    reason: "command_failed",
    cause: "The pinned device runtime did not return a valid bounded result.",
  });
const envelope = Schema.fromJsonString(
  Schema.Struct({ success: Schema.Literal(true), data: Schema.Unknown }),
);
const snapshot = DeviceCommandSnapshot;
const capabilities = Schema.Struct({
  device: Schema.Struct({
    id: Schema.String.check(Schema.isMaxLength(256)),
    platform: Schema.Literals(["ios", "android"]),
    target: Schema.Literal("mobile"),
    kind: Schema.Literals(["simulator", "emulator"]),
  }),
  availableCommands: Schema.Array(Schema.String.check(Schema.isMaxLength(64))).check(
    Schema.isMaxLength(128),
  ),
});
const media = Schema.Struct({
  outPath: Schema.String.check(Schema.isMaxLength(8_192)),
  artifacts: Schema.optional(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        artifactType: Schema.optional(Schema.String),
        path: Schema.optional(Schema.String),
        localPath: Schema.optional(Schema.String),
      }),
    ).check(Schema.isMaxLength(32)),
  ),
  durationMs: Schema.optional(
    Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
  ),
  capturedDurationMs: Schema.optional(
    Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
  ),
  recorder: Schema.optional(Schema.Literals(["confirmed", "unconfirmed", "lost"])),
  nativePathDisposition: Schema.optional(Schema.Literals(["pending", "retirable", "retired"])),
});

export const supportedCommands = [
  "open",
  "snapshot",
  "click",
  "press",
  "fill",
  "longpress",
  "focus",
  "type",
  "swipe",
  "scroll",
  "screenshot",
  "back",
  "home",
  "orientation",
  "fold",
  "app-switcher",
  "action-button",
  "react-native",
  "clipboard",
  "keyboard",
  "alert",
  "wait",
  "prepare",
  "capabilities",
  "record",
  "trace",
  "get",
  "is",
  "find",
  "apps",
  "appstate",
  "events",
  "network",
  "logs",
  "diff",
  "doctor",
  "perf",
  "gesture",
  "install",
  "reinstall",
  "push",
  "trigger-app-event",
  "settings",
  "batch",
  "close",
] as const satisfies ReadonlyArray<DeviceCommand["type"]>;

const decodeEnvelope = Schema.decodeUnknownEffect(envelope);
const decodeSnapshot = Schema.decodeUnknownEffect(snapshot);
const decodeCapabilities = Schema.decodeUnknownEffect(capabilities);
const decodeMedia = Schema.decodeUnknownEffect(media);
const decodeRecordingState = Schema.decodeUnknownEffect(
  Schema.Struct({ recording: Schema.Literals(["started", "stopped"]) }),
);
const decodeTraceState = Schema.decodeUnknownEffect(
  Schema.Struct({ trace: Schema.Literals(["started", "stopped"]) }),
);

// Host-level discovery describes policy, not a claim that a particular target supports a command.
// Runtime capabilities are checked for the exact target before dispatch.
export const catalogFamilies: ReadonlyArray<DeviceCommandFamilySupport> = [
  ...supportedCommands.map((name) => ({
    name,
    interface: "structured" as const,
    status: "typed" as const,
    reason:
      name === "batch"
        ? "Up to 32 typed atomic steps, one exact opening and durable request; no nested batches, runtime overrides or steps after close."
        : name === "install" || name === "reinstall"
          ? "Destination-owned verified app-package artifact, current device consent and actual deployment capability; no URL/source/config input."
          : name === "perf"
            ? "Frames and memory sample produce bounded diagnostic artifacts. Native profiling bundles need a qualified directory-artifact owner."
            : name === "logs"
              ? "Start, stop, clear, doctor and mark produce bounded redacted diagnostics. Raw daemon path is not a portable log artifact."
              : name === "doctor"
                ? "Host/runtime diagnostics run in the private session directory; no caller project or workspace diagnosis is implied."
                : name === "settings"
                  ? "Mobile settings with actual target capability; no host TCC, keychain reset or credential administration."
                  : "Bound to the opened simulator/emulator and current device consent; only the declared typed options are accepted.",
  })),
  {
    name: "devices",
    interface: "structured",
    status: "native-owner",
    reason:
      "Use device_list for the app-owned permitted host inventory; daemon-wide device selection flags are private.",
  },
  {
    name: "boot",
    interface: "structured",
    status: "native-owner",
    reason:
      "Use device_open; existing DeviceService owns boot, exact target selection and app session publication.",
  },
  {
    name: "shutdown",
    interface: "structured",
    status: "native-owner",
    reason:
      "Use device_close with shutdown; the native device lifecycle owner closes its app-owned sessions.",
  },
  {
    name: "artifacts",
    interface: "structured",
    status: "native-owner",
    reason:
      "Command outputs return qualified native artifact references for authenticated fetch. Cloud-provider or daemon-wide artifact enumeration needs its own permitted context.",
  },
  {
    name: "session",
    interface: "structured",
    status: "native-owner",
    reason:
      "Use the exact target returned by device_open and device_execute close. Daemon-wide session listing/state directories and script paths are private.",
  },
  {
    name: "hover",
    interface: "structured",
    status: "unsupported-target",
    reason:
      "The pinned catalog supports hover on web, not this service's mobile simulator/emulator targets.",
  },
  {
    name: "viewport",
    interface: "structured",
    status: "unsupported-target",
    reason:
      "The pinned catalog resizes web viewports. Mobile device geometry is not a web viewport.",
  },
  {
    name: "tv-remote",
    interface: "structured",
    status: "unsupported-target",
    reason: "TV targets are not advertised by this iOS/Android mobile device service.",
  },
  {
    name: "install-from-source",
    interface: "structured",
    status: "prerequisite",
    reason:
      "URL/CI/provider source resolution requires a permitted source-resource owner. Use install/reinstall with a qualified destination artifact; no generic URL fetch is exposed.",
  },
  {
    name: "replay",
    interface: "structured",
    status: "prerequisite",
    reason:
      "Native .ad/Maestro scripts can open/retarget sessions and carry runtime overrides. A recursively validated typed plan is required; raw script execution cannot bypass command policy. Use typed batch.",
  },
  {
    name: "test",
    interface: "structured",
    status: "prerequisite",
    reason:
      "Script suites inherit replay's session/runtime effects and artifact directories. They need validated plans and owned suite resources; no raw executable script or directory input.",
  },
  {
    name: "audio",
    interface: "structured",
    status: "prerequisite",
    reason:
      "Pinned mobile audio probes capture host-rendered audio. Device consent does not provide host-audio capture consent.",
  },
  {
    name: "debug",
    interface: "structured",
    status: "prerequisite",
    reason:
      "Symbolication needs a verified crash artifact plus a matching dSYM directory bundle. The existing native file artifact owner does not admit arbitrary host search directories.",
  },
  {
    name: "metro",
    interface: "structured",
    status: "prerequisite",
    reason:
      "Preparing/reloading a development server needs an owned project/runtime binding. Device command input cannot supply project paths, URL/proxy endpoints, bearer credentials or install flags.",
  },
  ...[
    "plugins",
    "auth",
    "daemon",
    "device",
    "connect",
    "connection",
    "disconnect",
    "mcp",
    "proxy",
    "takeover",
  ].map((name) => ({
    name,
    interface: "cli-only" as const,
    status: "prerequisite" as const,
    reason:
      "CLI administration or connection ownership is outside a device-opening command: no account, daemon, plugin, proxy or session takeover authority is accepted.",
  })),
  {
    name: "cdp",
    interface: "cli-only",
    status: "unsupported-target",
    reason:
      "CDP is a browser protocol; this service advertises mobile simulator/emulator commands, not a browser transport.",
  },
  {
    name: "web",
    interface: "cli-only",
    status: "unsupported-target",
    reason:
      "Web browser runtime management belongs to the browser owner, not this mobile device service.",
  },
  {
    name: "react-devtools",
    interface: "cli-only",
    status: "prerequisite",
    reason:
      "React Native debugger/profiling connections need an owned development-runtime binding and qualified profile artifact resources.",
  },
];

/** The only translation to CLI syntax. No caller can supply flags or a runtime session. */
export const run = Effect.fn("AgentDeviceCommand.run")(function* (input: {
  readonly command: Exclude<DeviceAtomicCommand, { type: "screenshot" }>;
  readonly nodePath: string;
  readonly entryPath: string;
  readonly configPath: string;
  readonly cwd: string;
  readonly session: string;
  readonly platform: DevicePlatform;
  readonly deviceId: string;
  readonly runner: ProcessRunner.ProcessRunner["Service"];
  readonly outputPath?: string;
  readonly inputPath?: string;
}) {
  const command = input.command;
  const args = [input.entryPath, command.type];
  const positionals: string[] = [];
  let stdin: string | undefined;
  const element = (
    value: Extract<DeviceAtomicCommand, { type: "click" | "press" | "fill" | "longpress" }>,
  ) =>
    "ref" in value
      ? [value.ref]
      : value.element.kind === "selector"
        ? [value.element.selector]
        : [String(value.element.point.x), String(value.element.point.y)];
  switch (command.type) {
    case "open":
      positionals.push(command.app);
      if (command.url !== undefined) positionals.push(command.url);
      break;
    case "click":
    case "press":
      positionals.push(...element(command));
      break;
    case "fill":
      positionals.push(...element(command));
      if (command.text === "") positionals.push("");
      else {
        args.push("--text-stdin");
        // The pinned reader strips CRLF as a unit. Empty stdin is rejected,
        // so clearing uses its supported empty positional after `--` instead.
        stdin = command.text + "\r\n";
      }
      break;
    case "snapshot":
      if (command.interactiveOnly) args.push("-i");
      if (command.depth !== undefined) args.push("--depth", String(command.depth));
      if (command.customActions) args.push("--actions");
      if (command.observeOnly) args.push("--observe-only");
      if (command.forceFull) args.push("--force-full");
      break;
    case "capabilities":
    case "close":
      break;
    case "longpress":
      positionals.push(...element(command), String(command.durationMs));
      break;
    case "focus":
      positionals.push(String(command.x), String(command.y));
      break;
    case "type":
      positionals.push(command.text);
      if (command.delayMs !== undefined) args.push("--delay-ms", String(command.delayMs));
      break;
    case "swipe":
      positionals.push(
        String(command.from.x),
        String(command.from.y),
        String(command.to.x),
        String(command.to.y),
      );
      if (command.count !== undefined) args.push("--count", String(command.count));
      if (command.pauseMs !== undefined) args.push("--pause-ms", String(command.pauseMs));
      if (command.pattern !== undefined) args.push("--pattern", command.pattern);
      break;
    case "scroll":
      positionals.push(command.direction);
      if (command.amount !== undefined) positionals.push(String(command.amount));
      if (command.pixels !== undefined) args.push("--pixels", String(command.pixels));
      if (command.durationMs !== undefined) args.push("--duration-ms", String(command.durationMs));
      break;
    case "back":
      if (command.mode !== undefined)
        args.push(command.mode === "system" ? "--system" : "--in-app");
      break;
    case "home":
    case "app-switcher":
    case "action-button":
      break;
    case "orientation":
      positionals.push(command.orientation);
      break;
    case "fold":
      positionals.push(command.pose);
      break;
    case "react-native":
    case "keyboard":
    case "alert":
    case "prepare":
      positionals.push(command.action);
      break;
    case "record":
      positionals.push(command.action);
      if (command.action === "start") {
        if (input.outputPath === undefined) return yield* failure();
        positionals.push(input.outputPath);
        if (command.fps !== undefined) args.push("--fps", String(command.fps));
        if (command.quality !== undefined) args.push("--quality", command.quality);
        if (command.recordingScope !== undefined) args.push("--scope", command.recordingScope);
        if (command.hideTouches) args.push("--hide-touches");
      }
      break;
    case "trace":
      if (input.outputPath === undefined) return yield* failure();
      positionals.push(command.action, input.outputPath);
      break;
    case "clipboard":
      positionals.push(command.action);
      if (command.action === "write") positionals.push(command.text);
      break;
    case "wait":
      positionals.push(
        command.kind === "duration"
          ? String(command.durationMs)
          : command.kind === "ref"
            ? command.ref
            : command.selector,
      );
      if (command.kind !== "duration" && command.timeoutMs !== undefined)
        positionals.push(String(command.timeoutMs));
      break;
    case "get":
      positionals.push(command.format, command.ref);
      break;
    case "is":
      positionals.push(command.predicate, command.selector);
      if (command.predicate === "text") positionals.push(command.value);
      break;
    case "find":
      if (command.locator !== undefined && command.locator !== "any")
        positionals.push(command.locator);
      positionals.push(command.query);
      if (command.action === "getText" || command.action === "getAttrs")
        positionals.push("get", command.action === "getText" ? "text" : "attrs");
      else positionals.push(command.action);
      if ("value" in command) positionals.push(command.value);
      if (command.action === "wait" && command.timeoutMs !== undefined)
        positionals.push(String(command.timeoutMs));
      break;
    case "gesture":
      positionals.push(command.kind);
      switch (command.kind) {
        case "pan":
          positionals.push(
            String(command.origin.x),
            String(command.origin.y),
            String(command.delta.x),
            String(command.delta.y),
          );
          if (command.durationMs !== undefined) positionals.push(String(command.durationMs));
          if (command.pointerCount !== undefined)
            args.push("--pointer-count", String(command.pointerCount));
          break;
        case "fling":
          positionals.push(command.direction, String(command.origin.x), String(command.origin.y));
          if (command.distance !== undefined) positionals.push(String(command.distance));
          break;
        case "swipe":
          positionals.push(command.preset);
          break;
        case "pinch":
        case "rotate":
          positionals.push(String(command.kind === "pinch" ? command.scale : command.degrees));
          if (command.origin !== undefined)
            positionals.push(String(command.origin.x), String(command.origin.y));
          break;
        case "transform":
          positionals.push(
            String(command.origin.x),
            String(command.origin.y),
            String(command.delta.x),
            String(command.delta.y),
            String(command.scale),
            String(command.degrees),
          );
          if (command.durationMs !== undefined) positionals.push(String(command.durationMs));
          break;
        case "drag":
          positionals.push(command.source, command.destination);
          if (
            command.sourceHoldMs !== undefined ||
            command.moveMs !== undefined ||
            command.destinationHoldMs !== undefined
          )
            positionals.push(String(command.sourceHoldMs ?? 800));
          if (command.moveMs !== undefined || command.destinationHoldMs !== undefined)
            positionals.push(String(command.moveMs ?? 500));
          if (command.destinationHoldMs !== undefined)
            positionals.push(String(command.destinationHoldMs));
          break;
      }
      break;
    case "install":
    case "reinstall":
      if (input.inputPath === undefined) return yield* failure();
      if (command.app !== undefined) positionals.push(command.app);
      positionals.push(input.inputPath);
      break;
    case "push":
      positionals.push(command.app, command.payloadJson);
      break;
    case "trigger-app-event":
      positionals.push(command.event);
      if (command.payloadJson !== undefined) positionals.push(command.payloadJson);
      break;
    case "settings":
      positionals.push(command.setting);
      if ("state" in command && command.state !== undefined) positionals.push(command.state);
      if (command.setting === "location" && command.state === "set")
        positionals.push(String(command.latitude), String(command.longitude));
      if (command.setting === "permission") {
        positionals.push(command.permission);
        if (command.mode !== undefined) positionals.push(command.mode);
      }
      if ("app" in command && command.app !== undefined) args.push("--app", command.app);
      break;
    case "apps":
      if (command.appsFilter === "all") args.push("--all");
      break;
    case "appstate":
      break;
    case "events":
      if (command.limit !== undefined || command.cursor !== undefined)
        positionals.push(String(command.limit ?? 100));
      if (command.cursor !== undefined) positionals.push(command.cursor);
      break;
    case "network":
      positionals.push(command.action);
      if (command.limit !== undefined) positionals.push(String(command.limit));
      if (command.include !== undefined) args.push("--include", command.include);
      break;
    case "logs":
      positionals.push(command.action);
      if (command.action === "mark") positionals.push(command.message);
      break;
    case "diff":
      positionals.push(command.kind);
      if (command.interactiveOnly) args.push("-i");
      if (command.depth !== undefined) args.push("--depth", String(command.depth));
      break;
    case "doctor":
      if (command.targetApp !== undefined) args.push("--app", command.targetApp);
      break;
    case "perf":
      positionals.push(command.area);
      if (command.area === "memory") positionals.push(command.action);
      break;
    default: {
      const unhandled: never = command;
      return unhandled;
    }
  }
  if (
    command.type !== "wait" &&
    command.type !== "find" &&
    "timeoutMs" in command &&
    command.timeoutMs !== undefined
  )
    args.push("--timeout", String(command.timeoutMs));
  args.push(
    "--json",
    "--platform",
    input.platform,
    input.platform === "ios" ? "--udid" : "--serial",
    input.deviceId,
    "--config",
    input.configPath,
    "--session",
    input.session,
    "--",
    ...positionals,
  );
  const inherited = yield* HostProcess.Environment;
  const env: Record<string, string> = {};
  for (const key of [
    "PATH",
    "SystemRoot",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "TZ",
  ])
    if (inherited[key] !== undefined) env[key] = inherited[key];
  // The client has an explicit private daemon binding. It needs neither provider
  // credentials nor another agent-device account/configuration from the host home.
  env.HOME = input.cwd;
  env.USERPROFILE = input.cwd;
  env.ELECTRON_RUN_AS_NODE = "1";
  const result = yield* input.runner
    .run({
      command: input.nodePath,
      args,
      stdin,
      env,
      cwd: input.cwd,
      timeout: "90 seconds",
      maxOutputBytes: 512 * 1024,
      outputMode: "error",
    })
    .pipe(Effect.mapError(failure));
  if (result.code !== 0 || result.timedOut || result.stdoutTruncated || result.stdoutInvalidUtf8)
    return yield* failure();
  const value = yield* decodeEnvelope(result.stdout).pipe(Effect.mapError(failure));
  if (command.type === "snapshot") {
    // Decode only public node fields. Runtime paths, diagnostics, tokens and arbitrary data never escape.
    return yield* decodeSnapshot(value.data).pipe(Effect.mapError(failure));
  }
  if (command.type === "capabilities")
    return yield* decodeCapabilities(value.data).pipe(Effect.mapError(failure));
  if (command.type === "record" || command.type === "trace") {
    const marker =
      command.type === "record"
        ? (yield* decodeRecordingState(value.data).pipe(Effect.mapError(failure))).recording
        : (yield* decodeTraceState(value.data).pipe(Effect.mapError(failure))).trace;
    if (marker !== (command.action === "start" ? "started" : "stopped")) return yield* failure();
    if (command.action === "stop")
      return yield* decodeMedia(value.data).pipe(Effect.mapError(failure));
  }
  if (
    [
      "get",
      "is",
      "find",
      "apps",
      "appstate",
      "events",
      "network",
      "logs",
      "diff",
      "doctor",
      "perf",
      "settings",
      "push",
      "trigger-app-event",
    ].includes(command.type) ||
    (command.type === "keyboard" && command.action === "status") ||
    (command.type === "clipboard" && command.action === "read") ||
    (command.type === "alert" && (command.action === "get" || command.action === "wait"))
  )
    return { document: value.data };
  return undefined;
});

/** Keep private runtime material out of text artifacts, including diagnostics from external tools. */
export function redactText(value: string, privateValues: ReadonlyArray<string>) {
  let text = value;
  let redactedFields = 0;
  for (const secret of privateValues)
    if (secret.length > 0 && text.includes(secret)) {
      text = text.replaceAll(secret, "[private runtime]");
      redactedFields++;
    }
  text = text.replace(/(^|[\s"'(=:])\/(?!\/)[^\s"'<>)]*/g, (_path, prefix: string) => {
    redactedFields++;
    return `${prefix}[private path]`;
  });
  text = text.replace(/[A-Za-z]:\\[^\s"'<>)]*/g, () => {
    redactedFields++;
    return "[private path]";
  });
  return { text, redactedFields };
}

/** Diagnostic JSON is a bounded, explicitly redacted artifact, never a source of executable instructions. */
export function diagnosticDocument(
  command: DeviceCommand["type"],
  input: unknown,
  privateValues: ReadonlyArray<string>,
) {
  let visited = 0;
  let redacted = 0;
  let truncated = false;
  const scrub = (value: unknown, depth: number): unknown => {
    if (++visited > 10_000 || depth > 16) {
      truncated = true;
      return "[truncated]";
    }
    if (typeof value === "string") {
      const result = redactText(value, privateValues);
      let text = result.text;
      redacted += result.redactedFields;
      if (text.length > 16_384) {
        text = text.slice(0, 16_384);
        truncated = true;
      }
      return text;
    }
    if (typeof value === "number") return Number.isFinite(value) ? value : "[non-finite]";
    if (value === null || typeof value === "boolean") return value;
    if (Array.isArray(value)) {
      if (value.length > 1_000) truncated = true;
      return value.slice(0, 1_000).map((item) => scrub(item, depth + 1));
    }
    if (typeof value === "object") {
      const fields = Object.entries(value);
      if (fields.length > 128) truncated = true;
      const sensitiveHeader = fields.some(
        ([key, value]) =>
          key === "name" &&
          typeof value === "string" &&
          /authorization|cookie|token|api.?key/i.test(value),
      );
      return Object.fromEntries(
        fields.slice(0, 128).flatMap(([key, value]) => {
          if (
            /token|authorization|cookie|api.?key|credential|secret|password|config|state.?dir|path|argv|args|flags|positionals/i.test(
              key,
            ) ||
            (sensitiveHeader && key === "value") ||
            (key === "command" &&
              (typeof value !== "string" || !supportedCommands.some((name) => name === value)))
          ) {
            redacted++;
            return [];
          }
          return [[key.slice(0, 256), scrub(value, depth + 1)]];
        }),
      );
    }
    return "[omitted]";
  };
  const data = scrub(input, 0);
  return { command, data, redactedFields: redacted, truncated };
}
