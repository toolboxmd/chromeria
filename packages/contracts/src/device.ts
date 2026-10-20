import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { EnvironmentId, ThreadId, TrimmedNonEmptyString, TrimmedString } from "./baseSchemas.ts";
import { PeerAttachmentReference } from "./peerAttachments.ts";
import { PeerAttachmentAssetResource } from "./assets.ts";

export const DevicePlatform = Schema.Literals(["ios", "android"]);
export type DevicePlatform = typeof DevicePlatform.Type;

export const DeviceHostId = TrimmedNonEmptyString.check(Schema.isMaxLength(128));
export type DeviceHostId = typeof DeviceHostId.Type;

export const LOCAL_DEVICE_HOST_ID = "local" as DeviceHostId;
export const LOCAL_DEVICE_HOST_LABEL = "This machine";

const ConfiguredDeviceHostId = DeviceHostId.check(
  Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_-]*$/),
  Schema.makeFilter((id) => id !== "local" || "The local host id is reserved."),
);

export const SshDeviceHostConfig = Schema.Struct({
  // Hosts saved before EAS hosts existed carry no kind.
  kind: Schema.Literal("ssh").pipe(Schema.withDecodingDefaultKey(Effect.succeed("ssh" as const))),
  id: ConfiguredDeviceHostId,
  label: TrimmedNonEmptyString,
  target: TrimmedNonEmptyString.check(Schema.isPattern(/^[^\s-][^\s]*$/)),
  identityFile: Schema.optional(TrimmedNonEmptyString),
  port: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 }))),
});
export type SshDeviceHostConfig = typeof SshDeviceHostConfig.Type;

/**
 * Cloud simulators from EAS Simulator. Sessions bill the Expo account until
 * they stop, so every one is bounded. The Expo credential is not stored here.
 */
export const EasDeviceHostConfig = Schema.Struct({
  kind: Schema.Literal("eas"),
  id: ConfiguredDeviceHostId,
  label: TrimmedNonEmptyString,
  /** EAS project that sessions run under, when the project's app config does not name one. */
  appId: Schema.optional(TrimmedNonEmptyString),
  /**
   * Expo access token, kept in the secret store. When empty the server uses
   * EXPO_TOKEN, then the `eas login` session. This is `secretSetting` from
   * settings.ts, which imports this module.
   */
  accessToken: TrimmedString.pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("")),
    Schema.annotateKey({ settingsSecret: true }),
  ),
  /** EAS allows at most 40 minutes on free accounts and 115 on paid ones. */
  maxSessionMinutes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 115 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(30)),
  ),
  idleMinutes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(10)),
  ),
  maxConcurrentSessions: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(2)),
  ),
}).check(
  // EAS requires the idle limit to be smaller than the session limit.
  Schema.makeFilter(
    (host: { readonly idleMinutes: number; readonly maxSessionMinutes: number }) =>
      host.idleMinutes < host.maxSessionMinutes ||
      "Idle minutes must be smaller than session minutes.",
  ),
);
export type EasDeviceHostConfig = typeof EasDeviceHostConfig.Type;

/**
 * Alpha: devices leased from a Simlock gateway (https://incubator.callstack.com/simlock).
 * T3 never installs Simlock; the gateway and its workers are the user's own.
 */
export const SimlockDeviceHostConfig = Schema.Struct({
  kind: Schema.Literal("simlock"),
  id: ConfiguredDeviceHostId,
  label: TrimmedNonEmptyString,
  /** The gateway's origin, such as https://gateway.example.ts.net:4700. */
  gatewayUrl: TrimmedNonEmptyString.check(Schema.isPattern(/^https?:\/\/[^\s/]+\/?$/)),
  /** Leases, drives, watches, and takes input on its own leases. Kept in the secret store. */
  agentToken: TrimmedString.pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("")),
    Schema.annotateKey({ settingsSecret: true }),
  ),
  maxLeases: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(2)),
  ),
  /** A lease the Device panel keeps alive ends this long after it started. */
  maxLeaseMinutes: Schema.Int.check(Schema.isBetween({ minimum: 5, maximum: 240 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(60)),
  ),
});
export type SimlockDeviceHostConfig = typeof SimlockDeviceHostConfig.Type;

export type DeviceHostConfig = SshDeviceHostConfig | EasDeviceHostConfig | SimlockDeviceHostConfig;

const uniqueHostIds = Schema.makeFilter(
  (hosts: ReadonlyArray<{ readonly id: string }>) =>
    new Set(hosts.map((host) => host.id)).size === hosts.length ||
    "Device host ids must be unique.",
);

export const SshDeviceHostConfigs = Schema.Array(SshDeviceHostConfig).check(uniqueHostIds);

/**
 * Kept apart from the SSH hosts in settings: servers and clients from before
 * EAS hosts decode `deviceHosts` as SSH hosts and would reject the whole file
 * or snapshot over one EAS entry.
 */
export const EasDeviceHostConfigs = Schema.Array(EasDeviceHostConfig).check(uniqueHostIds);

/** Kept apart from the other host lists for the same reason as EAS hosts. */
export const SimlockDeviceHostConfigs = Schema.Array(SimlockDeviceHostConfig).check(uniqueHostIds);

export const DeviceEasAccountInput = Schema.Struct({
  /** A saved EAS host whose stored token is checked when no new token is given. */
  hostId: Schema.optional(DeviceHostId),
  accessToken: Schema.optional(TrimmedString),
});
export type DeviceEasAccountInput = typeof DeviceEasAccountInput.Type;

export const DeviceEasAccount = Schema.Struct({
  signedInAs: Schema.String,
  credentialSource: Schema.Literals(["settings", "env", "eas-login"]),
  apps: Schema.Array(Schema.Struct({ id: Schema.String, fullName: Schema.String })),
});
export type DeviceEasAccount = typeof DeviceEasAccount.Type;

export const DeviceSimlockIdentityInput = Schema.Struct({
  /** Without one, the gateway this machine's agent-device points at. */
  gatewayUrl: Schema.optional(TrimmedString),
  /** A saved Simlock host whose stored token is checked when no new token is given. */
  hostId: Schema.optional(DeviceHostId),
  agentToken: Schema.optional(TrimmedString),
});
export type DeviceSimlockIdentityInput = typeof DeviceSimlockIdentityInput.Type;

export const DeviceSimlockIdentity = Schema.Struct({
  gatewayUrl: Schema.String,
  /** The label the gateway gave the token. */
  signedInAs: Schema.String,
  /** `env`: this machine's AGENT_DEVICE_DAEMON_AUTH_TOKEN, used because it targets this gateway. */
  tokenSource: Schema.Literals(["entered", "settings", "env"]),
});
export type DeviceSimlockIdentity = typeof DeviceSimlockIdentity.Type;

export class DeviceSimlockIdentityError extends Schema.TaggedError<DeviceSimlockIdentityError>()(
  "DeviceSimlockIdentityError",
  { reason: Schema.String },
) {
  override get message(): string {
    return this.reason;
  }
}

/** Checking an Expo credential failed; no device host is involved yet. */
export class DeviceEasAccountError extends Schema.TaggedError<DeviceEasAccountError>()(
  "DeviceEasAccountError",
  // No cause: this error reaches the browser, and the reason already says what Expo answered.
  { reason: Schema.String },
) {
  override get message(): string {
    return this.reason;
  }
}

export const DeviceId = TrimmedNonEmptyString.check(Schema.isMaxLength(256));
export type DeviceId = typeof DeviceId.Type;

export const DeviceSummary = Schema.Struct({
  hostId: DeviceHostId,
  id: DeviceId,
  platform: DevicePlatform,
  name: TrimmedNonEmptyString,
  version: Schema.String,
  booted: Schema.Boolean,
  physical: Schema.Boolean,
});
export type DeviceSummary = typeof DeviceSummary.Type;

export const DevicePlatformAvailability = Schema.Struct({
  platform: DevicePlatform,
  available: Schema.Boolean,
  reason: Schema.optional(Schema.String),
});
export type DevicePlatformAvailability = typeof DevicePlatformAvailability.Type;

export const DeviceToolVersion = Schema.Struct({
  requiredVersion: Schema.String,
  installedVersions: Schema.Array(Schema.String),
  runningVersion: Schema.NullOr(Schema.String),
});
export type DeviceToolVersion = typeof DeviceToolVersion.Type;

export const DeviceToolVersions = Schema.Struct({
  hub: DeviceToolVersion,
  agent: DeviceToolVersion,
});
export type DeviceToolVersions = typeof DeviceToolVersions.Type;

const deviceHostSummaryFields = {
  id: DeviceHostId,
  label: TrimmedNonEmptyString,
  platforms: Schema.Array(DevicePlatformAvailability),
  tools: Schema.optional(DeviceToolVersions),
  toolInspectionError: Schema.optional(Schema.String),
  hubInstalled: Schema.Boolean,
  agentDeviceInstalled: Schema.Boolean,
};
const MachineDeviceHostSummary = Schema.Struct({
  ...deviceHostSummaryFields,
  kind: Schema.Literals(["local", "ssh"]),
});
const EasDeviceHostSummary = Schema.Struct({
  ...deviceHostSummaryFields,
  kind: Schema.Literal("eas"),
});
const SimlockDeviceHostSummary = Schema.Struct({
  ...deviceHostSummaryFields,
  kind: Schema.Literal("simlock"),
});
export const DeviceHostSummary = Schema.Union([
  MachineDeviceHostSummary,
  EasDeviceHostSummary,
  SimlockDeviceHostSummary,
]);
export type DeviceHostSummary = typeof DeviceHostSummary.Type;

export const DeviceHostStatus = Schema.Literals([
  "disabled",
  "idle",
  "installing",
  "starting",
  "ready",
  "failed",
]);
export type DeviceHostStatus = typeof DeviceHostStatus.Type;

export const DeviceSession = Schema.Struct({
  threadId: ThreadId,
  hostId: DeviceHostId,
  deviceId: DeviceId,
  platform: DevicePlatform,
  openedAt: Schema.String,
  /** Generation of this exact opening, independent of clock resolution. */
  toolSessionId: Schema.optional(Schema.String),
});
export type DeviceSession = typeof DeviceSession.Type;

export const DeviceHostStatuses = Schema.Record(
  DeviceHostId,
  Schema.Struct({
    status: DeviceHostStatus,
    detail: Schema.optional(Schema.String),
    androidDiscoveryIncomplete: Schema.optional(Schema.Boolean),
  }),
);

const deviceServiceStateFields = {
  supportsHostRetry: Schema.optional(Schema.Boolean),
  supportsToolUpdate: Schema.optional(Schema.Boolean),
  supportsToolInspection: Schema.optional(Schema.Boolean),
  /** Servers from before EAS hosts ignore `easDeviceHosts`, so clients offer them only when set. */
  supportsEasHosts: Schema.optional(Schema.Boolean),
  /** Servers from before Simlock hosts ignore `simlockDeviceHosts`. */
  supportsSimlockHosts: Schema.optional(Schema.Boolean),
  hostStatus: DeviceHostStatus,
  hostStatusDetail: Schema.optional(Schema.String),
  hostStatuses: DeviceHostStatuses,
  devices: Schema.Array(DeviceSummary),
  sessions: Schema.Array(DeviceSession),
  bootingDevices: Schema.optional(
    Schema.Array(Schema.Struct({ ...DeviceSummary.fields, threadId: ThreadId })),
  ),
  onboardingCompleted: Schema.Boolean,
  agentAccessEnabled: Schema.Boolean,
  hubBasePath: Schema.String,
  revision: Schema.Int,
};

/**
 * EAS host summaries travel as `easHosts`: clients from before EAS hosts
 * decode `hosts` with a `local | ssh` kind and would reject the whole state.
 * Decoded, every host is in `hosts`.
 */
const DeviceServiceStateWire = Schema.Struct({
  ...deviceServiceStateFields,
  hosts: Schema.Array(MachineDeviceHostSummary),
  easHosts: Schema.optional(Schema.Array(EasDeviceHostSummary)),
  simlockHosts: Schema.optional(Schema.Array(SimlockDeviceHostSummary)),
});
const DeviceServiceStateDecoded = Schema.Struct({
  ...deviceServiceStateFields,
  hosts: Schema.Array(DeviceHostSummary),
});

export const DeviceServiceState = DeviceServiceStateWire.pipe(
  Schema.decodeTo(
    Schema.toType(DeviceServiceStateDecoded),
    SchemaTransformation.transform<
      typeof DeviceServiceStateDecoded.Type,
      typeof DeviceServiceStateWire.Type
    >({
      decode: ({ easHosts, simlockHosts, ...state }) => ({
        ...state,
        hosts: [...state.hosts, ...(easHosts ?? []), ...(simlockHosts ?? [])],
      }),
      encode: (state) => ({
        ...state,
        hosts: state.hosts.filter(
          (host): host is typeof MachineDeviceHostSummary.Type =>
            host.kind === "local" || host.kind === "ssh",
        ),
        easHosts: state.hosts.filter(
          (host): host is typeof EasDeviceHostSummary.Type => host.kind === "eas",
        ),
        simlockHosts: state.hosts.filter(
          (host): host is typeof SimlockDeviceHostSummary.Type => host.kind === "simlock",
        ),
      }),
    }),
  ),
);
export type DeviceServiceState = typeof DeviceServiceState.Type;

export const DeviceListInput = Schema.Struct({
  updateTool: Schema.optional(Schema.Literals(["hub", "agent"])),
  inspectOnly: Schema.optional(Schema.Boolean),
  retryHostId: Schema.optional(DeviceHostId),
});
export type DeviceListInput = typeof DeviceListInput.Type;

export const DeviceConfigureInput = Schema.Struct({
  enabled: Schema.optional(Schema.Boolean),
  agentAccessEnabled: Schema.optional(Schema.Boolean),
  onboardingCompleted: Schema.optional(Schema.Boolean),
});
export type DeviceConfigureInput = typeof DeviceConfigureInput.Type;

export const DeviceOpenInput = Schema.Struct({
  threadId: ThreadId,
  hostId: Schema.optional(DeviceHostId),
  deviceId: DeviceId,
  platform: DevicePlatform,
  /** Boot the simulator or emulator when it is not running. Defaults to true. */
  boot: Schema.optional(Schema.Boolean),
});
export type DeviceOpenInput = typeof DeviceOpenInput.Type;

export const DeviceCloseInput = Schema.Struct({
  hostId: Schema.optional(DeviceHostId),
  threadId: ThreadId,
  /** Omit to close every device session for the thread. */
  deviceId: Schema.optional(DeviceId),
  /** Also shut the simulator or emulator down. Defaults to false. */
  shutdown: Schema.optional(Schema.Boolean),
});
export type DeviceCloseInput = typeof DeviceCloseInput.Type;

export const DeviceShutdownInput = Schema.Struct({
  hostId: Schema.optional(DeviceHostId),
  deviceId: DeviceId,
  platform: DevicePlatform,
});
export type DeviceShutdownInput = typeof DeviceShutdownInput.Type;

export const DeviceAppearance = Schema.Literals(["light", "dark"]);
export type DeviceAppearance = typeof DeviceAppearance.Type;

export const DeviceTextSize = Schema.Literals(["small", "default", "large", "extra-large"]);
export type DeviceTextSize = typeof DeviceTextSize.Type;

export const DeviceColorFilter = Schema.Literals([
  "none",
  "grayscale",
  "red-green",
  "green-red",
  "blue-yellow",
]);
export type DeviceColorFilter = typeof DeviceColorFilter.Type;

export const DeviceOrientation = Schema.Literals([
  "portrait",
  "landscape_left",
  "portrait_upside_down",
  "landscape_right",
]);
export type DeviceOrientation = typeof DeviceOrientation.Type;

export const DeviceSettings = Schema.Struct({
  appearance: Schema.optional(DeviceAppearance),
  textSize: Schema.optional(DeviceTextSize),
  reduceMotion: Schema.optional(Schema.Boolean),
  increaseContrast: Schema.optional(Schema.Boolean),
  reduceTransparency: Schema.optional(Schema.Boolean),
  showBorders: Schema.optional(Schema.Boolean),
  voiceOver: Schema.optional(Schema.Boolean),
  liquidGlass: Schema.optional(Schema.Literals(["clear", "tinted"])),
  colorFilter: Schema.optional(DeviceColorFilter),
  networkEnabled: Schema.optional(Schema.Boolean),
  location: Schema.optional(
    Schema.NullOr(Schema.Struct({ latitude: Schema.Number, longitude: Schema.Number })),
  ),
});
export type DeviceSettings = typeof DeviceSettings.Type;

export const DeviceForegroundApp = Schema.Struct({
  id: Schema.String,
  name: Schema.optional(Schema.String),
  version: Schema.optional(Schema.String),
});
export type DeviceForegroundApp = typeof DeviceForegroundApp.Type;

export const DeviceDetail = Schema.Struct({
  hostId: DeviceHostId,
  deviceId: DeviceId,
  settings: DeviceSettings,
  foregroundApp: Schema.NullOr(DeviceForegroundApp),
  readAt: Schema.String,
});
export type DeviceDetail = typeof DeviceDetail.Type;

export const DevicePermission = Schema.Literals([
  "camera",
  "microphone",
  "photos",
  "contacts",
  "calendar",
  "reminders",
  "location",
  "notifications",
  "motion",
  "media-library",
  "faceid",
]);
export type DevicePermission = typeof DevicePermission.Type;

const DeviceTarget = {
  hostId: Schema.optional(DeviceHostId),
  deviceId: DeviceId,
};

export const DeviceActionInput = Schema.Union([
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setAppearance"),
    value: DeviceAppearance,
  }),
  Schema.Struct({ ...DeviceTarget, type: Schema.Literal("setTextSize"), value: DeviceTextSize }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setToggle"),
    setting: Schema.Literals([
      "reduceMotion",
      "increaseContrast",
      "reduceTransparency",
      "showBorders",
      "voiceOver",
      "networkEnabled",
    ]),
    value: Schema.Boolean,
  }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setLiquidGlass"),
    value: Schema.Literals(["clear", "tinted"]),
  }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setColorFilter"),
    value: DeviceColorFilter,
  }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setOrientation"),
    value: DeviceOrientation,
  }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setLocation"),
    latitude: Schema.Number.check(Schema.isBetween({ minimum: -90, maximum: 90 })),
    longitude: Schema.Number.check(Schema.isBetween({ minimum: -180, maximum: 180 })),
  }),
  Schema.Struct({ ...DeviceTarget, type: Schema.Literal("clearLocation") }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("setPermission"),
    appId: TrimmedNonEmptyString,
    permission: DevicePermission,
    decision: Schema.Literals(["grant", "revoke", "reset"]),
  }),
  Schema.Struct({ ...DeviceTarget, type: Schema.Literal("openUrl"), url: TrimmedNonEmptyString }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("launchApp"),
    appId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("terminateApp"),
    appId: TrimmedNonEmptyString,
  }),
  Schema.Struct({ ...DeviceTarget, type: Schema.Literal("shake") }),
  Schema.Struct({
    ...DeviceTarget,
    type: Schema.Literal("sendPush"),
    appId: TrimmedNonEmptyString,
    payload: Schema.Union([Schema.String, Schema.Record(Schema.String, Schema.Unknown)]),
  }),
]);
export type DeviceActionInput = typeof DeviceActionInput.Type;
export type DeviceActionType = DeviceActionInput["type"];

export const DeviceDetailInput = Schema.Struct(DeviceTarget);
export type DeviceDetailInput = typeof DeviceDetailInput.Type;

export class DeviceHostUnavailableError extends Schema.TaggedError<DeviceHostUnavailableError>()(
  "DeviceHostUnavailableError",
  {
    hostId: DeviceHostId,
    /** The host's name in Settings, which the message shows instead of its id. */
    hostLabel: Schema.optional(Schema.String),
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `${this.hostLabel ?? `Device host ${this.hostId}`} is unavailable: ${this.reason}`;
  }
}

export class DevicePlatformUnavailableError extends Schema.TaggedError<DevicePlatformUnavailableError>()(
  "DevicePlatformUnavailableError",
  {
    hostId: DeviceHostId,
    platform: DevicePlatform,
    reason: Schema.String,
  },
) {
  override get message(): string {
    return `${this.platform} devices are unavailable on host ${this.hostId}: ${this.reason}`;
  }
}

export class DeviceNotFoundError extends Schema.TaggedError<DeviceNotFoundError>()(
  "DeviceNotFoundError",
  {
    hostId: DeviceHostId,
    deviceId: DeviceId,
  },
) {
  override get message(): string {
    return `Device ${this.deviceId} was not found on host ${this.hostId}.`;
  }
}

export class DeviceBootError extends Schema.TaggedError<DeviceBootError>()("DeviceBootError", {
  hostId: DeviceHostId,
  deviceId: DeviceId,
  reason: Schema.Literals(["disk_space", "timeout", "launch_failed"]),
  diagnostics: Schema.optional(Schema.String.check(Schema.isMaxLength(2_000))),
  cause: Schema.Defect(),
}) {
  override get message(): string {
    const explanation = {
      disk_space: "There is not enough free disk space on its device host.",
      timeout: "The device did not become ready in time.",
      launch_failed: "The simulator or emulator could not start.",
    }[this.reason];
    const summary = `Device ${this.deviceId} failed to boot: ${explanation}`;
    return this.diagnostics
      ? `${summary}\n\n${this.diagnostics}`
      : `${summary} The device host sent no details. Try again or open another device; if it keeps failing, check the device on that host.`;
  }
}

export class DeviceOperationError extends Schema.TaggedError<DeviceOperationError>()(
  "DeviceOperationError",
  {
    operation: Schema.String,
    reason: Schema.Literals([
      "command_failed",
      "request_failed",
      "invalid_payload",
      "settings_failed",
      "hub_rejected",
    ]),
    exitCode: Schema.optional(Schema.Number),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    const explanation = {
      command_failed: `The device command failed${this.exitCode === undefined ? "" : ` (exit code ${this.exitCode})`}.`,
      request_failed: "Could not communicate with device support. Try refreshing devices.",
      invalid_payload: "The device request could not be encoded.",
      settings_failed: "Could not read or save device settings.",
      hub_rejected: "The device hub could not complete the request.",
    }[this.reason];
    return `Device ${this.operation} failed: ${explanation}`;
  }
}

export class DeviceActionUnavailableError extends Schema.TaggedError<DeviceActionUnavailableError>()(
  "DeviceActionUnavailableError",
  {
    operation: Schema.String,
    platform: DevicePlatform,
    reason: Schema.Literals(["unsupported", "helper_missing"]),
  },
) {
  override get message(): string {
    return this.reason === "helper_missing"
      ? `Device ${this.operation} requires a helper missing from this install. Set up device support again.`
      : `Device ${this.operation} is not supported on ${this.platform}.`;
  }
}

export const DeviceError = Schema.Union([
  DeviceHostUnavailableError,
  DevicePlatformUnavailableError,
  DeviceNotFoundError,
  DeviceBootError,
  DeviceOperationError,
  DeviceActionUnavailableError,
]);
export type DeviceError = typeof DeviceError.Type;

export const DeviceToolListResult = Schema.Struct({
  hostStatuses: DeviceHostStatuses,
  hosts: Schema.Array(DeviceHostSummary),
  devices: Schema.Array(DeviceSummary),
  open: Schema.Array(Schema.Struct({ hostId: DeviceHostId, deviceId: DeviceId })),
});
export type DeviceToolListResult = typeof DeviceToolListResult.Type;

export const DeviceToolOpenInput = Schema.Struct({
  deviceId: Schema.optional(
    DeviceId.annotate({
      description: "Device id from `device_list`. Default: the booted or last used device.",
    }),
  ),
  platform: Schema.optional(
    DevicePlatform.annotate({
      description: "Needed with no `deviceId` when both platforms exist.",
    }),
  ),
  hostId: Schema.optional(
    DeviceHostId.annotate({ description: "Host from `device_list`. Default local." }),
  ),
});
export type DeviceToolOpenInput = typeof DeviceToolOpenInput.Type;

/** Keep unknown keys until the closed-command check can reject them. Normal struct decoding strips them. */
export const closedDeviceCommand = <F extends Schema.Struct.Fields>(fields: F) =>
  Schema.StructWithRest(Schema.Struct(fields), [Schema.Record(Schema.String, Schema.Unknown)])
    .check(
      Schema.makeFilter(
        (value) =>
          Object.keys(value).every((key) => Object.hasOwn(fields, key)) ||
          "Unknown device command option.",
      ),
    )
    .pipe(Schema.decodeTo(Schema.Struct(fields)));

export const DeviceCommandTarget = closedDeviceCommand({
  hostId: DeviceHostId,
  deviceId: DeviceId,
  toolSessionId: Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/)),
});
export type DeviceCommandTarget = typeof DeviceCommandTarget.Type;

/** One exact device opening, qualified independently of the client's source connection. */
export const DeviceViewerResource = closedDeviceCommand({
  type: Schema.Literal("device"),
  environmentId: EnvironmentId.check(Schema.isMaxLength(256)),
  threadId: ThreadId.check(Schema.isMaxLength(256)),
  hostId: DeviceHostId,
  deviceId: DeviceId,
  toolSessionId: Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/)),
  platform: DevicePlatform,
});
export type DeviceViewerResource = typeof DeviceViewerResource.Type;

export const DeviceViewerOpenInput = closedDeviceCommand({
  viewerId: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/))),
  resource: DeviceViewerResource,
  operation: Schema.Literals(["view", "control"]),
});
export type DeviceViewerOpenInput = typeof DeviceViewerOpenInput.Type;
export const DeviceViewerCloseInput = closedDeviceCommand({
  viewerId: Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/)),
});
export const DeviceViewerAccess = Schema.Struct({
  viewerId: Schema.String.check(Schema.isPattern(/^[0-9a-f-]{36}$/)),
  ticket: TrimmedNonEmptyString.check(Schema.isMaxLength(4096)),
  expiresAt: Schema.String.check(Schema.isMaxLength(64)),
  hubBasePath: Schema.Literal("/api/device-hub"),
});
export type DeviceViewerAccess = typeof DeviceViewerAccess.Type;
/** Authenticated peer discovery; absence refuses before a viewer is admitted. */
export const DeviceViewerProtocol = Schema.Struct({
  version: Schema.Literal(1),
  purpose: Schema.Literal("device-viewer"),
});

const DeviceElementRef = Schema.String.check(Schema.isPattern(/^@?e[0-9]{1,6}$/));
const DeviceInputArtifact = closedDeviceCommand(PeerAttachmentReference.fields);
const DeviceCommandText = Schema.String.check(Schema.isMaxLength(16_384));
const DeviceCommandTimeout = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60_000 }));
const DeviceCommandCoordinate = Schema.Number.check(
  Schema.isFinite(),
  Schema.isBetween({ minimum: 0, maximum: 100_000 }),
);
const DeviceAppId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/),
);
const DeviceJsonPayload = Schema.String.check(
  Schema.isMaxLength(16_384),
  Schema.makeFilter((text) => {
    try {
      const value: unknown = JSON.parse(text);
      return value !== null && typeof value === "object" && !Array.isArray(value);
    } catch {
      return false;
    }
  }),
);
const DeviceCommandDuration = Schema.Int.check(Schema.isBetween({ minimum: 16, maximum: 10_000 }));
const DeviceCommandVector = closedDeviceCommand({
  x: Schema.Number.check(
    Schema.isFinite(),
    Schema.isBetween({ minimum: -100_000, maximum: 100_000 }),
  ),
  y: Schema.Number.check(
    Schema.isFinite(),
    Schema.isBetween({ minimum: -100_000, maximum: 100_000 }),
  ),
});
const DeviceCommandPoint = closedDeviceCommand({
  x: DeviceCommandCoordinate,
  y: DeviceCommandCoordinate,
});

const DeviceCommandElement = Schema.Union([
  closedDeviceCommand({
    kind: Schema.Literal("selector"),
    selector: DeviceCommandText.check(Schema.isMinLength(1)),
  }),
  closedDeviceCommand({ kind: Schema.Literal("point"), point: DeviceCommandPoint }),
]);

/** Caller intent only. Launchers, daemon credentials and runtime session flags are private. */
export const DeviceAtomicCommand = Schema.Union([
  closedDeviceCommand({
    type: Schema.Literal("open"),
    app: TrimmedNonEmptyString.check(
      Schema.isMaxLength(256),
      Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/),
    ),
    url: Schema.optional(
      Schema.String.check(
        Schema.isMaxLength(8_192),
        Schema.isPattern(/^[a-zA-Z][a-zA-Z0-9+.-]*:[^\s\u0000-\u001f\u007f]+$/),
      ),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("snapshot"),
    interactiveOnly: Schema.optional(Schema.Boolean),
    depth: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 64 }))),
    customActions: Schema.optional(Schema.Boolean),
    observeOnly: Schema.optional(Schema.Boolean),
    forceFull: Schema.optional(Schema.Boolean),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("click"),
    ref: DeviceElementRef,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("fill"),
    ref: DeviceElementRef,
    text: DeviceCommandText,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literals(["click", "press"]),
    element: DeviceCommandElement,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("fill"),
    element: DeviceCommandElement,
    text: DeviceCommandText,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("longpress"),
    element: DeviceCommandElement,
    durationMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000 })),
  }),
  closedDeviceCommand({ type: Schema.Literal("screenshot") }),
  closedDeviceCommand({ type: Schema.Literal("close") }),
  closedDeviceCommand({ type: Schema.Literal("capabilities") }),
  closedDeviceCommand({
    type: Schema.Literal("press"),
    ref: DeviceElementRef,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("longpress"),
    ref: DeviceElementRef,
    durationMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000 })),
  }),
  closedDeviceCommand({
    type: Schema.Literal("focus"),
    x: DeviceCommandCoordinate,
    y: DeviceCommandCoordinate,
  }),
  closedDeviceCommand({
    type: Schema.Literal("type"),
    text: DeviceCommandText,
    delayMs: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000 }))),
  }),
  closedDeviceCommand({
    type: Schema.Literal("swipe"),
    from: DeviceCommandPoint,
    to: DeviceCommandPoint,
    count: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }))),
    pauseMs: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000 }))),
    pattern: Schema.optional(Schema.Literals(["one-way", "ping-pong"])),
  }),
  closedDeviceCommand({
    type: Schema.Literal("scroll"),
    direction: Schema.Literals(["up", "down", "left", "right", "top", "bottom"]),
    amount: Schema.optional(
      Schema.Number.check(Schema.isFinite(), Schema.isBetween({ minimum: 0.1, maximum: 20 })),
    ),
    pixels: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100_000 }))),
    durationMs: Schema.optional(
      Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10_000 })),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("back"),
    mode: Schema.optional(Schema.Literals(["in-app", "system"])),
  }),
  closedDeviceCommand({ type: Schema.Literal("home") }),
  closedDeviceCommand({
    type: Schema.Literal("orientation"),
    orientation: Schema.Literals([
      "portrait",
      "portrait-upside-down",
      "landscape-left",
      "landscape-right",
    ]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("fold"),
    pose: Schema.Literals(["closed", "half-open", "open"]),
  }),
  closedDeviceCommand({ type: Schema.Literal("app-switcher") }),
  closedDeviceCommand({ type: Schema.Literal("action-button") }),
  closedDeviceCommand({
    type: Schema.Literal("react-native"),
    action: Schema.Literal("dismiss-overlay"),
  }),
  closedDeviceCommand({
    type: Schema.Literal("clipboard"),
    action: Schema.Literal("write"),
    text: DeviceCommandText,
  }),
  closedDeviceCommand({
    type: Schema.Literal("keyboard"),
    action: Schema.Literals(["dismiss", "enter", "return"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("alert"),
    action: Schema.Literals(["accept", "dismiss"]),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("wait"),
    kind: Schema.Literal("duration"),
    durationMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30_000 })),
  }),
  closedDeviceCommand({
    type: Schema.Literal("wait"),
    kind: Schema.Literal("ref"),
    ref: DeviceElementRef,
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("prepare"),
    action: Schema.Literal("ios-runner"),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("wait"),
    kind: Schema.Literal("selector"),
    selector: DeviceCommandText.check(Schema.isMinLength(1)),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("record"),
    action: Schema.Literal("start"),
    recordingScope: Schema.optional(Schema.Literals(["app", "device", "system"])),
    fps: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 120 }))),
    quality: Schema.optional(Schema.Literals(["medium", "high"])),
    hideTouches: Schema.optional(Schema.Boolean),
  }),
  closedDeviceCommand({ type: Schema.Literal("record"), action: Schema.Literal("stop") }),
  closedDeviceCommand({
    type: Schema.Literal("trace"),
    action: Schema.Literals(["start", "stop"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("get"),
    format: Schema.Literals(["text", "attrs"]),
    ref: DeviceElementRef,
  }),
  closedDeviceCommand({
    type: Schema.Literal("is"),
    predicate: Schema.Literals([
      "visible",
      "hidden",
      "exists",
      "absent",
      "editable",
      "selected",
      "focused",
    ]),
    selector: DeviceCommandText.check(Schema.isMinLength(1)),
  }),
  closedDeviceCommand({
    type: Schema.Literal("is"),
    predicate: Schema.Literal("text"),
    selector: DeviceCommandText.check(Schema.isMinLength(1)),
    value: DeviceCommandText,
  }),
  closedDeviceCommand({
    type: Schema.Literal("find"),
    query: DeviceCommandText.check(Schema.isMinLength(1)),
    locator: Schema.optional(Schema.Literals(["any", "text", "label", "value", "role", "id"])),
    action: Schema.Literals(["click", "focus", "exists", "list", "getText", "getAttrs"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("find"),
    query: DeviceCommandText.check(Schema.isMinLength(1)),
    locator: Schema.optional(Schema.Literals(["any", "text", "label", "value", "role", "id"])),
    action: Schema.Literals(["fill", "type"]),
    value: DeviceCommandText,
  }),
  closedDeviceCommand({
    type: Schema.Literal("find"),
    query: DeviceCommandText.check(Schema.isMinLength(1)),
    locator: Schema.optional(Schema.Literals(["any", "text", "label", "value", "role", "id"])),
    action: Schema.Literal("wait"),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("pan"),
    origin: DeviceCommandPoint,
    delta: DeviceCommandVector,
    durationMs: Schema.optional(DeviceCommandDuration),
    pointerCount: Schema.optional(Schema.Literals([1, 2])),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("fling"),
    direction: Schema.Literals(["up", "down", "left", "right"]),
    origin: DeviceCommandPoint,
    distance: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100_000 }))),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("swipe"),
    preset: Schema.Literals(["left", "right", "left-edge", "right-edge"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("pinch"),
    scale: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: 0.01, maximum: 100 }),
    ),
    origin: Schema.optional(DeviceCommandPoint),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("rotate"),
    degrees: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: -3600, maximum: 3600 }),
    ),
    origin: Schema.optional(DeviceCommandPoint),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("transform"),
    origin: DeviceCommandPoint,
    delta: DeviceCommandVector,
    scale: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: 0.01, maximum: 100 }),
    ),
    degrees: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: -3600, maximum: 3600 }),
    ),
    durationMs: Schema.optional(DeviceCommandDuration),
  }),
  closedDeviceCommand({
    type: Schema.Literal("gesture"),
    kind: Schema.Literal("drag"),
    source: DeviceElementRef,
    destination: DeviceElementRef,
    sourceHoldMs: Schema.optional(
      Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10_000 })),
    ),
    moveMs: Schema.optional(DeviceCommandDuration),
    destinationHoldMs: Schema.optional(
      Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10_000 })),
    ),
  }).check(
    Schema.makeFilter(
      (command) =>
        (command.sourceHoldMs ?? 800) +
          (command.moveMs ?? 500) +
          (command.destinationHoldMs ?? 0) <=
        10_000,
    ),
  ),
  closedDeviceCommand({
    type: Schema.Literal("install"),
    artifact: DeviceInputArtifact,
    app: Schema.optional(
      TrimmedNonEmptyString.check(
        Schema.isMaxLength(256),
        Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/),
      ),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("reinstall"),
    artifact: DeviceInputArtifact,
    app: TrimmedNonEmptyString.check(
      Schema.isMaxLength(256),
      Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("push"),
    app: DeviceAppId,
    payloadJson: DeviceJsonPayload,
  }),
  closedDeviceCommand({
    type: Schema.Literal("trigger-app-event"),
    event: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
    payloadJson: Schema.optional(DeviceJsonPayload),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literals(["wifi", "airplane", "animations"]),
    state: Schema.Literals(["on", "off"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("appearance"),
    state: Schema.Literals(["light", "dark", "toggle"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("text-size"),
    state: Schema.optional(
      Schema.Literals([
        "extra-small",
        "small",
        "medium",
        "large",
        "extra-large",
        "extra-extra-large",
        "extra-extra-extra-large",
        "accessibility-medium",
        "accessibility-large",
        "accessibility-extra-large",
        "accessibility-extra-extra-large",
        "accessibility-extra-extra-extra-large",
      ]),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("location"),
    state: Schema.Literals(["on", "off"]),
    app: Schema.optional(DeviceAppId),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("location"),
    state: Schema.Literal("set"),
    latitude: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: -90, maximum: 90 }),
    ),
    longitude: Schema.Number.check(
      Schema.isFinite(),
      Schema.isBetween({ minimum: -180, maximum: 180 }),
    ),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("permission"),
    state: Schema.Literals(["grant", "deny", "reset"]),
    permission: Schema.Literals([
      "all",
      "camera",
      "microphone",
      "photos",
      "contacts",
      "contacts-limited",
      "notifications",
      "calendar",
      "location",
      "location-always",
      "media-library",
      "motion",
      "reminders",
      "siri",
    ]),
    app: Schema.optional(DeviceAppId),
    mode: Schema.optional(Schema.Literals(["full", "limited"])),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("clear-app-state"),
    app: DeviceAppId,
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literals(["faceid", "touchid"]),
    state: Schema.Literals(["match", "nonmatch", "enroll", "unenroll"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("settings"),
    setting: Schema.Literal("fingerprint"),
    state: Schema.Literals(["match", "nonmatch"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("apps"),
    appsFilter: Schema.optional(Schema.Literals(["user-installed", "all"])),
  }),
  closedDeviceCommand({ type: Schema.Literal("appstate") }),
  closedDeviceCommand({ type: Schema.Literal("keyboard"), action: Schema.Literal("status") }),
  closedDeviceCommand({ type: Schema.Literal("clipboard"), action: Schema.Literal("read") }),
  closedDeviceCommand({
    type: Schema.Literal("alert"),
    action: Schema.Literals(["get", "wait"]),
    timeoutMs: Schema.optional(DeviceCommandTimeout),
  }),
  closedDeviceCommand({
    type: Schema.Literal("events"),
    limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 }))),
    cursor: Schema.optional(Schema.String.check(Schema.isMaxLength(512))),
  }),
  closedDeviceCommand({
    type: Schema.Literal("network"),
    action: Schema.Literals(["dump", "log"]),
    limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 }))),
    include: Schema.optional(Schema.Literals(["summary", "headers", "body", "all"])),
  }),
  closedDeviceCommand({
    type: Schema.Literal("logs"),
    action: Schema.Literals(["start", "stop", "doctor", "clear"]),
  }),
  closedDeviceCommand({
    type: Schema.Literal("logs"),
    action: Schema.Literal("mark"),
    message: DeviceCommandText,
  }),
  closedDeviceCommand({
    type: Schema.Literal("diff"),
    kind: Schema.Literal("snapshot"),
    interactiveOnly: Schema.optional(Schema.Boolean),
    depth: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 64 }))),
  }),
  closedDeviceCommand({
    type: Schema.Literal("doctor"),
    targetApp: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  }),
  closedDeviceCommand({ type: Schema.Literal("perf"), area: Schema.Literal("frames") }),
  closedDeviceCommand({
    type: Schema.Literal("perf"),
    area: Schema.Literal("memory"),
    action: Schema.Literal("sample"),
  }),
]);
export type DeviceAtomicCommand = typeof DeviceAtomicCommand.Type;
export const DeviceCommand = Schema.Union([
  DeviceAtomicCommand,
  closedDeviceCommand({
    type: Schema.Literal("batch"),
    steps: Schema.Array(DeviceAtomicCommand).check(Schema.isMinLength(1), Schema.isMaxLength(32)),
  }),
]);
export type DeviceCommand = typeof DeviceCommand.Type;

export const DeviceCommandInput = Schema.Struct({
  target: DeviceCommandTarget,
  command: DeviceCommand,
});
export type DeviceCommandInput = typeof DeviceCommandInput.Type;

export const DeviceCommandNode = Schema.Struct({
  ref: DeviceElementRef,
  index: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100_000 })),
  role: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  label: Schema.optional(DeviceCommandText),
  value: Schema.optional(DeviceCommandText),
  enabled: Schema.optional(Schema.Boolean),
  selected: Schema.optional(Schema.Boolean),
  checked: Schema.optional(Schema.Boolean),
  focused: Schema.optional(Schema.Boolean),
  rect: Schema.optional(
    Schema.Struct({
      x: Schema.Number.check(
        Schema.isFinite(),
        Schema.isBetween({ minimum: -100_000, maximum: 100_000 }),
      ),
      y: Schema.Number.check(
        Schema.isFinite(),
        Schema.isBetween({ minimum: -100_000, maximum: 100_000 }),
      ),
      width: DeviceCommandCoordinate,
      height: DeviceCommandCoordinate,
    }),
  ),
  kind: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  type: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  identifier: Schema.optional(DeviceCommandText),
  contentDescription: Schema.optional(DeviceCommandText),
  placeholder: Schema.optional(DeviceCommandText),
  depth: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1_000 }))),
  editable: Schema.optional(Schema.Boolean),
  password: Schema.optional(Schema.Boolean),
  visibleToUser: Schema.optional(Schema.Boolean),
  hittable: Schema.optional(Schema.Boolean),
  inheritsLabel: Schema.optional(Schema.Literal(true)),
  inheritsIdentifier: Schema.optional(Schema.Literal(true)),
  actions: Schema.optional(Schema.Array(DeviceCommandText).check(Schema.isMaxLength(128))),
  parentIndex: Schema.optional(Schema.Int),
});
const DeviceSnapshotMeasure = Schema.Number.check(
  Schema.isFinite(),
  Schema.isBetween({ minimum: -100_000, maximum: 100_000 }),
);
const DeviceSnapshotRect = Schema.Struct({
  x: DeviceSnapshotMeasure,
  y: DeviceSnapshotMeasure,
  width: DeviceCommandCoordinate,
  height: DeviceCommandCoordinate,
});
const DeviceSnapshotCount = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100_000 }));
/** Bounded facts the pinned snapshot producer publishes, including incomplete or occluding captures. */
export const DeviceCommandSnapshot = Schema.Struct({
  nodes: Schema.Array(DeviceCommandNode).check(Schema.isMaxLength(2_000)),
  truncated: Schema.optional(Schema.Boolean),
  comparisonSafe: Schema.optional(Schema.Boolean),
  systemSurfaceOnly: Schema.optional(Schema.Boolean),
  iosSystemSurfaceBundleId: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  backend: Schema.optional(Schema.String.check(Schema.isMaxLength(64))),
  producer: Schema.optional(Schema.String.check(Schema.isMaxLength(64))),
  viewport: Schema.optional(
    Schema.Struct({ width: DeviceCommandCoordinate, height: DeviceCommandCoordinate }),
  ),
  keyboard: Schema.optional(
    Schema.Union([
      Schema.Struct({ kind: Schema.Literal("visible"), frame: DeviceSnapshotRect }),
      Schema.Struct({ kind: Schema.Literal("absent") }),
      Schema.Struct({ kind: Schema.Literal("unmeasurable"), reason: DeviceCommandText }),
    ]),
  ),
  targetActivation: Schema.optional(
    Schema.Struct({
      reason: Schema.Literals([
        "bundle_changed",
        "stale_target",
        "missing_after_wait",
        "interaction_foreground_guard",
      ]),
      priorState: Schema.Literals([
        "unknown",
        "notRunning",
        "runningBackgroundSuspended",
        "runningBackground",
      ]),
      otherActiveApplicationPid: Schema.optional(DeviceSnapshotCount),
    }),
  ),
  postGestureOutcome: Schema.optional(
    Schema.Struct({ kind: Schema.Literals(["unsettled", "no-effect"]) }),
  ),
  snapshotQuality: Schema.optional(
    Schema.Struct({
      state: Schema.Literals(["healthy", "recovered", "sparse"]),
      backend: Schema.Literals(["tree", "queries", "private-ax", "android-helper"]),
      reason: Schema.optional(DeviceCommandText),
      reasonCode: Schema.optional(
        Schema.Literals([
          "ax-rejected",
          "sparse-tree",
          "budget",
          "no-nodes",
          "capture-failed",
          "presentation-failed",
          "deferred",
          "requested-backend",
        ]),
      ),
      effectiveDepth: Schema.optional(DeviceSnapshotCount),
      collapsedLeafIndexes: Schema.optional(
        Schema.Array(DeviceSnapshotCount).check(Schema.isMaxLength(2_000)),
      ),
      customActions: Schema.optional(
        Schema.Struct({
          read: DeviceSnapshotCount,
          candidates: DeviceSnapshotCount,
          truncated: DeviceSnapshotCount,
          blocked: Schema.Boolean,
        }),
      ),
      timing: Schema.optional(
        Schema.Struct({
          acquisitionMs: DeviceCommandCoordinate,
          presentationMs: DeviceCommandCoordinate,
        }),
      ),
    }),
  ),
});

export const DeviceAtomicCommandResult = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("artifacts"),
    target: DeviceCommandTarget,
    resources: Schema.optional(
      Schema.Array(PeerAttachmentAssetResource).check(Schema.isMaxLength(32)),
    ),
    artifacts: Schema.Array(PeerAttachmentReference).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(32),
    ),
    recording: Schema.optional(
      Schema.Struct({
        durationMs: Schema.optional(
          Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
        ),
        capturedDurationMs: Schema.optional(
          Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
        ),
        recorder: Schema.optional(Schema.Literals(["confirmed", "unconfirmed", "lost"])),
        nativePathDisposition: Schema.optional(
          Schema.Literals(["pending", "retirable", "retired"]),
        ),
      }),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("capabilities"),
    target: DeviceCommandTarget,
    commands: Schema.Array(Schema.String.check(Schema.isMaxLength(64))).check(
      Schema.isMaxLength(128),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("snapshot"),
    target: DeviceCommandTarget,
    ...DeviceCommandSnapshot.fields,
    truncated: Schema.Boolean,
  }),
  Schema.Struct({
    type: Schema.Literal("action"),
    target: DeviceCommandTarget,
    status: Schema.Literal("dispatched"),
  }),
  Schema.Struct({ type: Schema.Literal("closed"), target: DeviceCommandTarget }),
  Schema.Struct({
    type: Schema.Literal("artifact"),
    target: DeviceCommandTarget,
    artifact: PeerAttachmentReference,
    resource: Schema.optional(PeerAttachmentAssetResource),
  }),
]);
export type DeviceAtomicCommandResult = typeof DeviceAtomicCommandResult.Type;
export const DeviceCommandResult = Schema.Union([
  DeviceAtomicCommandResult,
  Schema.Struct({
    type: Schema.Literal("batch"),
    target: DeviceCommandTarget,
    results: Schema.Array(DeviceAtomicCommandResult).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(32),
    ),
  }),
]);
export type DeviceCommandResult = typeof DeviceCommandResult.Type;

export const DeviceCommandFamilySupport = Schema.Struct({
  name: Schema.String.check(Schema.isMaxLength(64)),
  interface: Schema.Literals(["structured", "cli-only"]),
  status: Schema.Literals(["typed", "native-owner", "unsupported-target", "prerequisite"]),
  reason: Schema.String.check(Schema.isMaxLength(1_024)),
});
export type DeviceCommandFamilySupport = typeof DeviceCommandFamilySupport.Type;

export const DeviceCommandCatalog = Schema.Struct({
  protocol: Schema.Literal(1),
  inputArtifacts: Schema.optional(Schema.Literal(1)),
  targetPlatform: Schema.optional(DevicePlatform),
  hostId: DeviceHostId,
  agentDeviceVersion: Schema.String,
  deviceHubVersion: Schema.String,
  commands: Schema.Array(Schema.String.check(Schema.isMaxLength(64))),
  families: Schema.Array(DeviceCommandFamilySupport).check(Schema.isMaxLength(128)),
});
export type DeviceCommandCatalog = typeof DeviceCommandCatalog.Type;

export const DeviceToolOpenResult = Schema.Union([
  Schema.Struct({
    device: DeviceSummary,
    /** Ready-to-run agent-device invocation pinned to this device. */
    agentDevice: Schema.Struct({
      command: Schema.String,
      /** Flags that pin every command to this device, e.g. `--udid <id>`. */
      targetArgs: Schema.Array(Schema.String),
    }),
    quickStart: Schema.String,
  }),
  Schema.Struct({
    device: DeviceSummary,
    target: DeviceCommandTarget,
    quickStart: Schema.String,
    resource: Schema.optional(DeviceViewerResource),
  }),
]);
export type DeviceToolOpenResult = typeof DeviceToolOpenResult.Type;

export const DeviceToolTargetInput = Schema.Struct({
  deviceId: Schema.optional(
    DeviceId.annotate({
      description: "Device. Default: the last one opened here.",
    }),
  ),
  hostId: Schema.optional(DeviceHostId),
});
export type DeviceToolTargetInput = typeof DeviceToolTargetInput.Type;

export const DeviceToolScreenshotResult = Schema.Struct({
  device: DeviceSummary,
  screenshot: Schema.Struct({
    mimeType: Schema.Literal("image/png"),
    data: Schema.String,
    width: Schema.Int,
    height: Schema.Int,
  }),
});
export type DeviceToolScreenshotResult = typeof DeviceToolScreenshotResult.Type;

export const DeviceToolCloseInput = Schema.Struct({
  deviceId: Schema.optional(
    DeviceId.annotate({
      description: "Device to close. None: close all here.",
    }),
  ),
  hostId: Schema.optional(DeviceHostId),
  shutdown: Schema.optional(
    Schema.Boolean.annotate({
      description: "Also turn it off. Default false.",
    }),
  ),
});
export type DeviceToolCloseInput = typeof DeviceToolCloseInput.Type;

export class DeviceToolUnavailableError extends Schema.TaggedError<DeviceToolUnavailableError>()(
  "DeviceToolUnavailableError",
  {
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason;
  }
}

export const DeviceToolError = Schema.Union([
  DeviceToolUnavailableError,
  DeviceHostUnavailableError,
  DevicePlatformUnavailableError,
  DeviceNotFoundError,
  DeviceBootError,
  DeviceOperationError,
  DeviceActionUnavailableError,
]);
export type DeviceToolError = typeof DeviceToolError.Type;

export function deviceToolInstallMessage(name: string, tool: DeviceToolVersion | undefined) {
  if (!tool) return `Installing ${name}…`;
  const previous =
    tool.runningVersion ??
    [...tool.installedVersions].sort((a, b) => a.localeCompare(b, "en", { numeric: true })).at(-1);
  return previous && !tool.installedVersions.includes(tool.requiredVersion)
    ? `Updating ${name} from ${previous} to ${tool.requiredVersion}…`
    : `Installing ${name} ${tool.requiredVersion}…`;
}
