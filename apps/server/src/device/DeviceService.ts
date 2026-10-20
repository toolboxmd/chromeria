import {
  type DeviceActionInput,
  type DeviceViewerResource,
  type DeviceCloseInput,
  type DeviceConfigureInput,
  type DeviceDetail,
  type DeviceDetailInput,
  type DeviceError,
  type DeviceHostId,
  type DeviceId,
  DeviceBootError,
  DeviceHostUnavailableError,
  DeviceNotFoundError,
  DeviceOperationError,
  DeviceActionUnavailableError,
  type DeviceOpenInput,
  type DevicePlatform,
  DevicePlatformUnavailableError,
  type DeviceServiceState,
  type DeviceSession,
  type DeviceShutdownInput,
  type DeviceSummary,
  type SshDeviceHostConfig,
  type DeviceHostSummary,
  LOCAL_DEVICE_HOST_ID,
  type ThreadId,
  DeviceCommandInput,
  type DeviceCommandResult,
  type DeviceAtomicCommand,
  type DeviceAtomicCommandResult,
  type DeviceCommandCatalog,
  type PeerAttachmentReference,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import { resolveNodeExecutable, nodeRuntimeUnavailableMessage } from "@t3tools/shared/nodeRuntime";
import * as Path from "effect/Path";
import { ensureAgentDevice, ensureDeviceHub } from "./DeviceToolchain.ts";
import * as ServerConfig from "../config.ts";
import {
  agentDeviceConfigPath,
  agentDeviceSession,
  writeAgentDeviceConfig,
} from "./AgentDeviceTarget.ts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";

import * as ServerSettings from "../serverSettings.ts";
import { isLocalSshDeviceHost, remoteSshDeviceHosts } from "./localSshDeviceHost.ts";

import { readDeviceDetail, runDeviceAction } from "./DeviceActions.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as DeviceHost from "./DeviceHost.ts";
import * as SshDeviceHost from "./SshDeviceHost.ts";
import * as Exit from "effect/Exit";
import * as LocalDeviceHost from "./LocalDeviceHost.ts";
import * as AgentDeviceCommand from "./AgentDeviceCommand.ts";
import {
  AGENT_DEVICE_VERSION,
  DEVICE_HUB_VERSION,
  installedAgentDevice,
} from "./DeviceToolchain.ts";
import * as Option from "effect/Option";
import { requestDigest } from "../mcp/McpRequestReceipts.ts";
import { requestNamespace, requestGeneration } from "../mcp/requestIdentity.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { CurrentRequest, externalDispatch, externalReplay } from "../mcp/McpRequestReceipts.ts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import {
  createAttachmentId,
  createPendingAttachmentId,
  parseAttachmentUuid,
  parseAttachmentFileExtension,
  toSafeThreadAttachmentSegment,
  resolveAttachmentPath,
} from "../attachmentStore.ts";

export const DEVICE_HUB_ROUTE_PREFIX = "/api/device-hub";

const BOOT_TIMEOUT = Duration.minutes(3);
const SCREENSHOT_TIMEOUT = Duration.seconds(20);

const HubDevice = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  version: Schema.String,
  platform: Schema.Literals(["ios", "android"]),
  booted: Schema.Boolean,
  physical: Schema.Boolean,
});
const HubDeviceList = Schema.Struct({
  simulators: Schema.Array(HubDevice),
  emulators: Schema.Array(HubDevice),
  errors: Schema.optional(Schema.Array(Schema.Struct({ message: Schema.String }))),
});
const HubActionResult = Schema.Struct({
  ok: Schema.Boolean,
  id: Schema.optional(Schema.String),
  serial: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});

export interface DeviceScreenshot {
  readonly device: DeviceSummary;
  readonly png: Uint8Array;
}

export interface DeviceReadiness extends DeviceHost.DeviceHostReady {
  readonly hostId: DeviceHostId;
}

export interface DeviceAgentReadiness extends DeviceReadiness {
  readonly agentDevice: DeviceHost.AgentDeviceEndpoint;
}

const decodeCommandInput = Schema.decodeUnknownEffect(DeviceCommandInput, {
  onExcessProperty: "error",
});
const encodeCommandJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeCommandJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

export class DeviceService extends Context.Service<
  DeviceService,
  {
    readonly agentCli: Effect.Effect<string, DeviceError>;
    readonly checkCommandAccess: (hostId?: DeviceHostId) => Effect.Effect<void, DeviceError>;
    readonly commandCatalog: (
      hostId?: DeviceHostId,
      target?: DeviceCommandInput["target"],
    ) => Effect.Effect<DeviceCommandCatalog, DeviceError | OrchestratorMcpFailure>;
    readonly executeCommand: (
      input: DeviceCommandInput,
    ) => Effect.Effect<
      DeviceCommandResult,
      DeviceError | OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
    readonly testHost: (
      config: SshDeviceHostConfig,
    ) => Effect.Effect<DeviceHostSummary, DeviceError>;
    /** Fresh consent and host availability, without starting a host or writing config. */
    readonly checkAgentAccess?: (hostId?: DeviceHostId) => Effect.Effect<void, DeviceError>;
    readonly agentTarget: (input: {
      threadId: ThreadId;
      hostId: DeviceHostId;
      deviceId: DeviceId;
    }) => Effect.Effect<ReadonlyArray<string>, DeviceError>;
    readonly state: Effect.Effect<DeviceServiceState>;
    readonly subscribe: Effect.Effect<PubSub.Subscription<DeviceServiceState>, never, Scope.Scope>;
    readonly configure: (
      input: DeviceConfigureInput,
    ) => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly list: Effect.Effect<DeviceServiceState, DeviceError>;
    readonly updateTool: (tool: "hub" | "agent") => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly inspect: Effect.Effect<DeviceServiceState>;
    readonly retryHost: (hostId: DeviceHostId) => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly open: (input: DeviceOpenInput) => Effect.Effect<DeviceSession, DeviceError>;
    readonly close: (input: DeviceCloseInput) => Effect.Effect<void, DeviceError>;
    readonly shutdown: (input: DeviceShutdownInput) => Effect.Effect<void, DeviceError>;
    readonly detail: (input: DeviceDetailInput) => Effect.Effect<DeviceDetail, DeviceError>;
    readonly action: (input: DeviceActionInput) => Effect.Effect<DeviceDetail, DeviceError>;
    readonly screenshot: (input: {
      readonly hostId?: DeviceHostId | undefined;
      readonly deviceId: DeviceId;
    }) => Effect.Effect<DeviceScreenshot, DeviceError>;
    readonly readiness: (hostId?: DeviceHostId) => Effect.Effect<DeviceReadiness, DeviceError>;
    readonly readinessIfSupported: (
      hostId?: DeviceHostId,
    ) => Effect.Effect<DeviceReadiness | null, DeviceError>;
    readonly agentReadinessIfSupported: (
      hostId?: DeviceHostId,
    ) => Effect.Effect<DeviceAgentReadiness | null, DeviceError>;
    /** Checks the exact still-open resource and consent without preparing a host. */
    readonly authorizeViewer: (
      resource: DeviceViewerResource,
    ) => Effect.Effect<DeviceReadiness, DeviceHostUnavailableError | DeviceOperationError>;
    readonly currentReadiness: (hostId?: DeviceHostId) => Effect.Effect<DeviceReadiness | null>;
    readonly sessionsForThread: (threadId: ThreadId) => Effect.Effect<ReadonlyArray<DeviceSession>>;
  }
>()("t3/device/DeviceService") {}

interface ServiceState {
  readonly state: DeviceServiceState;
}

const vendorPrefix = (platform: DevicePlatform) =>
  platform === "ios" ? "/vendor/serve-sim" : "/vendor/serve-emu";

export const makeWithHosts = Effect.fn("DeviceService.makeWithHosts")(function* (
  hosts: ReadonlyMap<DeviceHostId, DeviceHost.DeviceHost["Service"]>,
  testHost: DeviceService["Service"]["testHost"] = (host) =>
    Effect.fail(
      new DeviceHostUnavailableError({
        hostId: host.id,
        reason: "SSH probing is unavailable in this device service.",
      }),
    ),
  configureAgent: (
    hostId: DeviceHostId,
    ready: DeviceHost.DeviceHostAgentReady,
  ) => Effect.Effect<string, DeviceError> = (hostId) =>
    Effect.fail(
      new DeviceHostUnavailableError({
        hostId,
        reason: "Agent configuration is unavailable in this device service.",
      }),
    ),
  installTool?: (tool: "hub" | "agent") => Effect.Effect<unknown, DeviceError>,
) {
  const settings = yield* ServerSettings.ServerSettingsService;
  const crypto = yield* Crypto.Crypto;
  const commandLock = yield* Semaphore.make(1);
  const recheckAction = Effect.gen(function* () {
    // Reads have verified invocation authority too, without a mutation receipt.
    // Direct native UI calls retain their existing owner when there is no MCP scope.
    const invocation = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isNone(invocation) || invocation.value.thread === undefined) return;
    const original = yield* Invocation.requireThreadScope(invocation.value, "Device action");
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isSome(authority)) {
      // The authority owner consumes the returned current ceiling before admitting this effect.
      yield* authority.value.recheckInvocation(original, "device", original.thread.threadId);
    } else if (original.authority !== undefined || original.client?.linked === true)
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "Current invocation authority is unavailable.",
      });
  }).pipe(
    Effect.mapError(
      (cause) =>
        new DeviceHostUnavailableError({
          hostId: LOCAL_DEVICE_HOST_ID,
          reason: "The current device invocation is no longer authorized.",
          cause,
        }),
    ),
  );

  const commandConfig = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const commandFs = yield* Effect.serviceOption(FileSystem.FileSystem);
  const commandPath = yield* Effect.serviceOption(Path.Path);
  const commandRunner = yield* Effect.serviceOption(ProcessRunner.ProcessRunner);
  // Resource bindings, not authority: every command still checks present consent and its opening.
  const commandBindings = new Map<
    DeviceSession,
    Omit<Parameters<typeof AgentDeviceCommand.run>[0], "command">
  >();
  // An opening belongs to the actor that opened it, even when multiple source
  // actors may independently access the same destination thread. This resource
  // identity never substitutes for the shared owner's current authority checks.
  const commandOwners = new Map<DeviceSession, { namespace: string; generation: string }>();
  const commandCaptures = new Map<string, { directory: string; outputPath: string }>();
  const lifecycleLock = yield* Semaphore.make(1);
  const readDeviceSettings = settings.getSettings.pipe(
    Effect.map((value) => ({
      enabled: value.enableDeviceSupport,
      agentAccessEnabled: value.enableAgentDeviceAccess,
      onboardingCompleted: value.deviceOnboardingCompleted,
    })),
    Effect.mapError(
      (cause) =>
        new DeviceOperationError({ operation: "settings", reason: "settings_failed", cause }),
    ),
  );
  const initialSettings = yield* readDeviceSettings;

  const httpClient = (yield* HttpClient.HttpClient).pipe(HttpClient.withScope);
  const statePubSub = yield* PubSub.unbounded<DeviceServiceState>();
  const initialHosts = yield* Effect.forEach(hosts.values(), (host) => host.summary);
  let publishedHosts = new Map(hosts);
  let nextRefreshSequence = 0;
  const publishedRefreshes = new WeakMap<DeviceHost.DeviceHost["Service"], number>();
  const stateRef = yield* SynchronizedRef.make<ServiceState>({
    state: {
      supportsHostRetry: true,
      supportsToolUpdate: installTool !== undefined,
      supportsToolInspection: true,
      hosts: initialHosts,
      hostStatus: initialSettings.enabled ? "idle" : "disabled",
      hostStatuses: {},
      devices: [],
      sessions: [],
      onboardingCompleted: initialSettings.onboardingCompleted,
      agentAccessEnabled: initialSettings.agentAccessEnabled,
      hubBasePath: DEVICE_HUB_ROUTE_PREFIX,
      revision: 0,
    },
  });

  const publish = (update: (state: DeviceServiceState) => DeviceServiceState) =>
    SynchronizedRef.updateAndGetEffect(stateRef, ({ state }) => {
      const next = { ...update(state), revision: state.revision + 1 };
      return PubSub.publish(statePubSub, next).pipe(Effect.as({ state: next }));
    }).pipe(Effect.map(({ state }) => state));

  const resolveHost = (hostId: DeviceHostId | undefined) =>
    Effect.gen(function* () {
      const id = hostId ?? LOCAL_DEVICE_HOST_ID;
      const host = hosts.get(id);
      if (!host) {
        return yield* new DeviceHostUnavailableError({ hostId: id, reason: "Unknown host." });
      }
      return host;
    });

  const setHostStatus = (
    hostId: DeviceHostId,
    status: DeviceServiceState["hostStatuses"][string],
  ) =>
    Effect.suspend(() =>
      !hosts.has(hostId)
        ? SynchronizedRef.get(stateRef).pipe(Effect.map(({ state }) => state))
        : publish((state) => ({
            ...state,
            ...(hostId === LOCAL_DEVICE_HOST_ID
              ? { hostStatus: status.status, hostStatusDetail: status.detail }
              : {}),
            hostStatuses: {
              ...state.hostStatuses,
              [hostId]: state.hostStatuses[hostId]?.androidDiscoveryIncomplete
                ? { ...status, androidDiscoveryIncomplete: true }
                : status,
            },
          })),
    );

  const beginRefresh = (hostId: DeviceHostId) => ({
    host: hosts.get(hostId),
    sequence: ++nextRefreshSequence,
  });

  type ReadinessWithRefreshAttempt<
    K extends "readiness" | "readinessIfSupported" | "agentReadinessIfSupported",
  > = (
    hostId?: DeviceHostId,
    attempt?: ReturnType<typeof beginRefresh>,
  ) => ReturnType<DeviceService["Service"][K]>;

  const canPublishRefresh = (attempt: ReturnType<typeof beginRefresh>) =>
    readDeviceSettings.pipe(
      Effect.map(
        ({ enabled }) =>
          enabled &&
          attempt.host !== undefined &&
          hosts.get(attempt.host.id) === attempt.host &&
          (publishedRefreshes.get(attempt.host) ?? 0) <= attempt.sequence,
      ),
      Effect.orElseSucceed(() => false),
    );

  const publishRefresh = (
    attempt: ReturnType<typeof beginRefresh>,
    result: Effect.Effect<DeviceServiceState>,
  ) =>
    lifecycleLock.withPermit(
      Effect.gen(function* () {
        if (!(yield* canPublishRefresh(attempt)) || !attempt.host)
          return (yield* SynchronizedRef.get(stateRef)).state;
        publishedRefreshes.set(attempt.host, attempt.sequence);
        return yield* result;
      }),
    );

  // Readiness already holds the lifecycle lock; its status writes use the same attempt guard.
  const setReadinessStatus = (
    hostId: DeviceHostId,
    status: DeviceServiceState["hostStatuses"][string],
    attempt?: ReturnType<typeof beginRefresh>,
  ) =>
    Effect.gen(function* () {
      if (attempt) {
        if (!(yield* canPublishRefresh(attempt)) || !attempt.host)
          return (yield* SynchronizedRef.get(stateRef)).state;
        if (status.status === "failed") publishedRefreshes.set(attempt.host, attempt.sequence);
      }
      return yield* setHostStatus(hostId, status);
    });

  const readiness: (
    hostId?: DeviceHostId,
    attempt?: ReturnType<typeof beginRefresh>,
  ) => ReturnType<DeviceService["Service"]["readiness"]> = Effect.fn("DeviceService.readiness")(
    function* (hostId?: DeviceHostId, attempt?: ReturnType<typeof beginRefresh>) {
      const host = yield* resolveHost(hostId);
      if (!(yield* readDeviceSettings).enabled) {
        return yield* new DeviceHostUnavailableError({
          hostId: host.id,
          reason:
            "Device support is off. Enable it in the Device panel before installing or starting device tools.",
        });
      }
      yield* recheckAction;
      const ready = yield* host
        .ensureReady((status, detail) =>
          setReadinessStatus(host.id, { status, detail }, attempt).pipe(Effect.asVoid),
        )
        .pipe(
          Effect.tapError((error) =>
            setReadinessStatus(host.id, { status: "failed", detail: error.message }, attempt),
          ),
          Effect.mapError(
            (error) =>
              new DeviceHostUnavailableError({
                hostId: host.id,
                reason:
                  error._tag === "NodeRuntimeUnavailableError"
                    ? nodeRuntimeUnavailableMessage("Local device support")
                    : error.step === "probe"
                      ? "Could not connect to this host over SSH."
                      : `Device support failed during ${error.step}.`,
                cause: error,
              }),
          ),
        );
      if (hosts.get(host.id) !== host)
        return yield* new DeviceHostUnavailableError({
          hostId: host.id,
          reason: "Host configuration changed. Retry the operation.",
        });
      const { state } = yield* SynchronizedRef.get(stateRef);
      if (state.hostStatuses[host.id]?.status !== "ready") {
        yield* setReadinessStatus(host.id, { status: "ready" }, attempt);
      }
      return { hostId: host.id, ...ready };
    },
    lifecycleLock.withPermit,
  );

  const readinessIfSupported = Effect.fn("DeviceService.readinessIfSupported")(function* (
    hostId?: DeviceHostId,
    attempt?: ReturnType<typeof beginRefresh>,
  ) {
    if (!(yield* readDeviceSettings).enabled) return null;
    const host = yield* resolveHost(hostId);
    const summary = yield* host.summary;
    if (summary.kind === "local" && !summary.platforms.some((platform) => platform.available))
      return null;
    return yield* readiness(host.id, attempt);
  });

  const supportedAgentHost = (hostId?: DeviceHostId) =>
    Effect.gen(function* () {
      const deviceSettings = yield* readDeviceSettings;
      if (!deviceSettings.enabled || !deviceSettings.agentAccessEnabled) return undefined;
      const host = yield* resolveHost(hostId);
      const summary = yield* host.summary;
      if (summary.kind === "local" && !summary.platforms.some((platform) => platform.available))
        return undefined;
      return host;
    });
  const checkAgentAccess = (hostId?: DeviceHostId) =>
    supportedAgentHost(hostId).pipe(
      Effect.flatMap((host) =>
        host === undefined
          ? Effect.fail(
              new DeviceHostUnavailableError({
                hostId: hostId ?? LOCAL_DEVICE_HOST_ID,
                reason:
                  "Agent device access requires enabled device support, agent access, and an available simulator platform on this host.",
              }),
            )
          : Effect.void,
      ),
    );

  const commandUnavailable = (hostId = LOCAL_DEVICE_HOST_ID) =>
    new DeviceHostUnavailableError({
      hostId,
      reason:
        "Typed device execution requires the native service and the pinned installed agent-device and device-hub runtimes.",
    });
  const commandPreflight = (hostId?: DeviceHostId) =>
    Effect.gen(function* () {
      yield* checkAgentAccess(hostId);
      if (
        Option.isNone(commandConfig) ||
        Option.isNone(commandFs) ||
        Option.isNone(commandPath) ||
        Option.isNone(commandRunner)
      )
        return yield* commandUnavailable(hostId);
      const host = yield* resolveHost(hostId);
      const summary = yield* host.summary;
      const tools = summary.tools;
      if (
        tools === undefined ||
        tools.agent.requiredVersion !== AGENT_DEVICE_VERSION ||
        tools.hub.requiredVersion !== DEVICE_HUB_VERSION ||
        !tools.agent.installedVersions.includes(AGENT_DEVICE_VERSION) ||
        !tools.hub.installedVersions.includes(DEVICE_HUB_VERSION) ||
        (tools.agent.runningVersion !== null &&
          tools.agent.runningVersion !== AGENT_DEVICE_VERSION) ||
        (tools.hub.runningVersion !== null && tools.hub.runningVersion !== DEVICE_HUB_VERSION)
      )
        return yield* commandUnavailable(host.id);
      const entry = yield* installedAgentDevice(commandConfig.value.baseDir).pipe(
        Effect.provideService(FileSystem.FileSystem, commandFs.value),
        Effect.provideService(Path.Path, commandPath.value),
      );
      if (entry === undefined) return yield* commandUnavailable(host.id);
      const nodePath = yield* resolveNodeExecutable("Device automation").pipe(
        Effect.provideService(FileSystem.FileSystem, commandFs.value),
        Effect.provideService(Path.Path, commandPath.value),
        Effect.mapError(() => commandUnavailable(host.id)),
      );
      return {
        host,
        entry,
        nodePath,
        fs: commandFs.value,
        path: commandPath.value,
        runner: commandRunner.value,
        config: commandConfig.value,
      };
    });
  const checkCommandAccess = (hostId?: DeviceHostId) =>
    commandPreflight(hostId).pipe(Effect.andThen(recheckAction));

  const forgetCommandSession = (session: DeviceSession) =>
    Effect.gen(function* () {
      const binding = commandBindings.get(session);
      if (binding !== undefined && Option.isSome(commandFs))
        yield* commandFs.value.remove(binding.configPath, { force: true }).pipe(Effect.orDie);
      commandBindings.delete(session);
      commandOwners.delete(session);
      commandCaptures.delete(`${session.toolSessionId}:record`);
      commandCaptures.delete(`${session.toolSessionId}:trace`);
    });
  const retireCommandSession = (session: DeviceSession) =>
    Effect.gen(function* () {
      const binding = commandBindings.get(session);
      if (binding === undefined) return yield* forgetCommandSession(session);
      yield* recheckAction;
      yield* AgentDeviceCommand.run({ ...binding, command: { type: "close" } });
      yield* forgetCommandSession(session);
    });

  const agentReadinessIfSupported: ReadinessWithRefreshAttempt<"agentReadinessIfSupported"> =
    Effect.fn("DeviceService.agentReadinessIfSupported")(function* (hostId, attempt) {
      const host = yield* supportedAgentHost(hostId);
      if (host === undefined) return null;
      yield* recheckAction;
      const ready = yield* host
        .ensureAgentReady((phase, detail) =>
          setReadinessStatus(host.id, { status: phase, detail }, attempt).pipe(Effect.asVoid),
        )
        .pipe(
          Effect.tapError((error) =>
            setReadinessStatus(host.id, { status: "failed", detail: error.message }, attempt),
          ),
          Effect.mapError(
            (error) =>
              new DeviceHostUnavailableError({
                hostId: host.id,
                reason:
                  error._tag === "NodeRuntimeUnavailableError"
                    ? nodeRuntimeUnavailableMessage("Local device support")
                    : error._tag === "DeviceHostTimeoutError"
                      ? `Agent tools did not start within ${error.timeoutMs} ms.`
                      : error._tag === "AgentDeviceStartError"
                        ? error.message
                        : error.step === "probe"
                          ? "Could not connect to this host over SSH."
                          : `Device support failed during ${error.step}.`,
                cause: error,
              }),
          ),
        );
      const hostSummaries = yield* Effect.forEach(hosts.values(), (candidate) => candidate.summary);
      if (!attempt || (yield* canPublishRefresh(attempt)))
        yield* publish((state) => ({ ...state, hosts: hostSummaries }));
      yield* setReadinessStatus(host.id, { status: "ready" }, attempt);
      return { hostId: host.id, ...ready };
    }, lifecycleLock.withPermit);

  const currentReadiness: DeviceService["Service"]["currentReadiness"] = (hostId) =>
    resolveHost(hostId).pipe(
      Effect.flatMap((host) =>
        host.current.pipe(Effect.map((ready) => (ready ? { hostId: host.id, ...ready } : null))),
      ),
      Effect.orElseSucceed(() => null),
    );

  const hubJson = <A, I>(
    request: HttpClientRequest.HttpClientRequest,
    schema: Schema.Codec<A, I>,
    operation: string,
    timeout: Duration.Input = Duration.seconds(15),
  ) =>
    httpClient.execute(request).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)),
      Effect.scoped,
      Effect.timeout(timeout),
      Effect.mapError(
        (cause) =>
          new DeviceOperationError({
            operation,
            reason: "request_failed",
            cause,
          }),
      ),
    );

  const fetchDevices = Effect.fn("DeviceService.fetchDevices")(function* (ready: DeviceReadiness) {
    const list = yield* hubJson(
      HttpClientRequest.get(`${ready.hub.origin}/api/devices`),
      HubDeviceList,
      "list",
    );
    const toSummary = (device: typeof HubDevice.Type): DeviceSummary => ({
      hostId: ready.hostId,
      id: device.id,
      platform: device.platform,
      name: device.name,
      version: device.version,
      booted: device.booted,
      physical: device.physical,
    });
    const devices = [...list.simulators, ...list.emulators].map(toSummary);
    const details = list.errors?.map((error) => error.message) ?? [];
    let androidDiscoveryIncomplete = false;
    const host = yield* resolveHost(ready.hostId);
    if ((yield* host.platformAvailability("android")).available) {
      const avds = yield* ready.run("emulator", ["-list-avds"]);
      if (avds.code !== 0) {
        androidDiscoveryIncomplete = true;
        details.push(
          `Could not list Android virtual devices (emulator -list-avds, exit code ${avds.code}). Discovered devices remain available.`,
        );
      }
      for (const name of (avds.code === 0 ? avds.stdout : "")
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)) {
        if (!devices.some((device) => device.platform === "android" && device.name === name)) {
          devices.push({
            hostId: ready.hostId,
            id: name,
            name,
            platform: "android",
            version: "Android",
            booted: false,
            physical: false,
          });
        }
      }
    }
    return { devices, detail: details.join("\n") || undefined, androidDiscoveryIncomplete };
  });

  const refresh = Effect.fn("DeviceService.refresh")(function* (
    ready: DeviceReadiness,
    attempt?: ReturnType<typeof beginRefresh>,
  ) {
    const currentAttempt = attempt ?? beginRefresh(ready.hostId);
    const { devices, detail, androidDiscoveryIncomplete } = yield* fetchDevices(ready);
    const hostSummaries = yield* Effect.forEach(hosts.values(), (host) => host.summary);
    return yield* publishRefresh(
      currentAttempt,
      publish((state) => ({
        ...state,
        hosts: hostSummaries,
        ...(ready.hostId === LOCAL_DEVICE_HOST_ID
          ? { hostStatus: "ready", hostStatusDetail: detail }
          : {}),
        devices: [...state.devices.filter((device) => device.hostId !== ready.hostId), ...devices],
        hostStatuses: {
          ...state.hostStatuses,
          [ready.hostId]: {
            status: "ready",
            ...(detail ? { detail } : {}),
            ...(androidDiscoveryIncomplete ? { androidDiscoveryIncomplete } : {}),
          },
        },
      })),
    );
  });

  const list: DeviceService["Service"]["list"] = Effect.gen(function* () {
    if (!(yield* readDeviceSettings).enabled) return (yield* SynchronizedRef.get(stateRef)).state;
    yield* Effect.forEach(
      hosts.values(),
      (host) =>
        Effect.gen(function* () {
          const attempt = beginRefresh(host.id);
          yield* Effect.gen(function* () {
            const ready = yield* readinessIfSupported(host.id, attempt);
            if (ready) yield* refresh(ready, attempt);
          }).pipe(
            Effect.catch((error) =>
              publishRefresh(
                attempt,
                setHostStatus(host.id, {
                  status: "failed",
                  detail:
                    error._tag === "DeviceHostUnavailableError" ? error.reason : error.message,
                }),
              ),
            ),
          );
        }),
      { concurrency: 4 },
    );
    return (yield* SynchronizedRef.get(stateRef)).state;
  }).pipe(Effect.withSpan("DeviceService.list"));

  const inspect = Effect.gen(function* () {
    yield* Effect.forEach(
      hosts.values(),
      (host) =>
        Effect.gen(function* () {
          const result = yield* (host.inspect ?? host.summary).pipe(Effect.result);
          if (hosts.get(host.id) !== host) return;
          if (result._tag === "Failure") {
            yield* publish((state) => ({
              ...state,
              hosts: state.hosts.map((value) =>
                value.id === host.id
                  ? {
                      ...value,
                      toolInspectionError:
                        "Cannot check versions. Reconnect the host and check again. Installed tools have not been changed.",
                    }
                  : value,
              ),
            }));
            return;
          }
          yield* publish((state) => ({
            ...state,
            hosts: state.hosts.map((value) => (value.id === host.id ? result.success : value)),
          }));
        }),
      { concurrency: 4 },
    );
    return (yield* SynchronizedRef.get(stateRef)).state;
  });

  const retryHost: DeviceService["Service"]["retryHost"] = Effect.fn("DeviceService.retryHost")(
    function* (hostId) {
      yield* resolveHost(hostId);
      if (!(yield* readDeviceSettings).enabled) return (yield* SynchronizedRef.get(stateRef)).state;
      const attempt = beginRefresh(hostId);
      yield* Effect.gen(function* () {
        const ready =
          (yield* agentReadinessIfSupported(hostId, attempt)) ??
          (yield* readinessIfSupported(hostId, attempt));
        if (ready) yield* refresh(ready, attempt);
      }).pipe(
        Effect.catch((error) =>
          publishRefresh(
            attempt,
            setHostStatus(hostId, {
              status: "failed",
              detail: error._tag === "DeviceHostUnavailableError" ? error.reason : error.message,
            }),
          ),
        ),
      );
      return (yield* SynchronizedRef.get(stateRef)).state;
    },
  );

  const configure: DeviceService["Service"]["configure"] = Effect.fn("DeviceService.configure")(
    function* (input) {
      const currentSettings = yield* readDeviceSettings;
      const nextEnabled = input.enabled ?? currentSettings.enabled;
      const nextAgentAccess = input.agentAccessEnabled ?? currentSettings.agentAccessEnabled;
      yield* lifecycleLock.withPermit(
        Effect.gen(function* () {
          yield* settings
            .updateSettings({
              ...(input.enabled === undefined ? {} : { enableDeviceSupport: input.enabled }),
              ...(input.agentAccessEnabled === undefined
                ? {}
                : { enableAgentDeviceAccess: input.agentAccessEnabled }),
              ...(input.onboardingCompleted === undefined
                ? {}
                : { deviceOnboardingCompleted: input.onboardingCompleted }),
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new DeviceOperationError({
                    operation: "configure",
                    reason: "settings_failed",
                    cause,
                  }),
              ),
            );
          if (!nextEnabled) {
            yield* Effect.forEach(hosts.values(), (host) => host.stop, { discard: true });
          } else if (input.agentAccessEnabled === false) {
            yield* Effect.forEach(hosts.values(), (host) => host.stopAgent, { discard: true });
          }
          if (!nextEnabled || input.agentAccessEnabled === false)
            yield* Effect.forEach(commandOwners.keys(), forgetCommandSession, { discard: true });
          yield* publish((state) => ({
            ...state,
            hostStatus: nextEnabled ? "idle" : "disabled",
            hostStatusDetail: undefined,
            hostStatuses: {},
            devices: nextEnabled ? state.devices : [],
            sessions: nextEnabled ? state.sessions : [],
            bootingDevices: nextEnabled ? state.bootingDevices : [],
            agentAccessEnabled: nextAgentAccess,
            onboardingCompleted: input.onboardingCompleted ?? state.onboardingCompleted,
          }));
        }),
      );
      if (nextEnabled && nextAgentAccess && input.agentAccessEnabled === true) {
        yield* agentReadinessIfSupported();
      }
      return yield* list;
    },
  );

  const findDevice = (
    state: DeviceServiceState,
    hostId: DeviceHostId,
    deviceId: DeviceId,
  ): DeviceSummary | undefined =>
    state.devices.find((device) => device.hostId === hostId && device.id === deviceId);

  const ensurePlatform = Effect.fn("DeviceService.ensurePlatform")(function* (
    host: DeviceHost.DeviceHost["Service"],
    platform: DevicePlatform,
  ) {
    const availability = yield* host.platformAvailability(platform);
    if (!availability.available) {
      return yield* new DevicePlatformUnavailableError({
        hostId: host.id,
        platform,
        reason: availability.reason ?? "Platform toolchain missing.",
      });
    }
  });

  /**
   * Android AVDs change id when they boot (AVD name to emulator serial), so the
   * returned id is authoritative.
   */
  const boot = Effect.fn("DeviceService.boot")(function* (
    ready: DeviceReadiness,
    device: DeviceSummary,
  ) {
    const result = yield* HttpClientRequest.post(`${ready.hub.origin}/api/devices/boot`).pipe(
      HttpClientRequest.bodyJson({ platform: device.platform, id: device.id, name: device.name }),
      Effect.mapError(
        (cause) =>
          new DeviceOperationError({ operation: "boot", reason: "invalid_payload", cause }),
      ),
      Effect.flatMap((request) => hubJson(request, HubActionResult, "boot", BOOT_TIMEOUT)),
    );
    if (!result.ok) {
      return yield* new DeviceBootError({
        hostId: ready.hostId,
        deviceId: device.id,
        reason: /insufficient.*(?:disk|space)|not enough.*(?:disk|space)|no space left/i.test(
          result.error ?? "",
        )
          ? "disk_space"
          : /timed? out|timeout/i.test(result.error ?? "")
            ? "timeout"
            : "launch_failed",
        cause: result,
      });
    }
    if (device.platform === "ios") {
      // Booting alone does not attach a serve-sim helper; the grid start
      // does both and is idempotent for a booted simulator.
      yield* HttpClientRequest.post(
        `${ready.hub.origin}${vendorPrefix("ios")}/grid/api/start`,
      ).pipe(
        HttpClientRequest.bodyJson({ udid: device.id }),
        Effect.mapError(
          (cause) =>
            new DeviceOperationError({ operation: "boot", reason: "invalid_payload", cause }),
        ),
        Effect.flatMap((request) =>
          hubJson(request, HubActionResult, "attach stream", BOOT_TIMEOUT),
        ),
      );
    }
    return result.serial ?? result.id ?? device.id;
  });

  const open: DeviceService["Service"]["open"] = Effect.fn("DeviceService.open")(function* (input) {
    yield* recheckAction;
    const host = yield* resolveHost(input.hostId);
    yield* ensurePlatform(host, input.platform);
    const ready = yield* readiness(host.id);
    let state = yield* refresh(ready);
    let device = findDevice(state, host.id, input.deviceId);
    if (!device) {
      return yield* new DeviceNotFoundError({ hostId: host.id, deviceId: input.deviceId });
    }
    if (!device.booted && input.boot !== false) {
      const booting = { ...device, threadId: input.threadId };
      yield* publish((current) => ({
        ...current,
        bootingDevices: [
          ...(current.bootingDevices ?? []).filter(
            (entry) => entry.hostId !== booting.hostId || entry.id !== booting.id,
          ),
          booting,
        ],
      }));
      yield* recheckAction;
      const bootedId = yield* boot(ready, device).pipe(
        Effect.ensuring(
          publish((current) => ({
            ...current,
            bootingDevices: (current.bootingDevices ?? []).filter(
              (entry) => entry.hostId !== booting.hostId || entry.id !== booting.id,
            ),
          })),
        ),
      );
      state = yield* refresh(ready);
      device = findDevice(state, host.id, bootedId) ?? findDevice(state, host.id, device.id);
      if (!device) {
        return yield* new DeviceNotFoundError({ hostId: host.id, deviceId: bootedId });
      }
    } else if (device.platform === "ios" && device.booted) {
      yield* recheckAction;
      // A simulator booted outside T3 has no helper attached yet.
      yield* HttpClientRequest.post(
        `${ready.hub.origin}${vendorPrefix("ios")}/grid/api/start`,
      ).pipe(
        HttpClientRequest.bodyJson({ udid: device.id }),
        Effect.mapError(
          (cause) =>
            new DeviceOperationError({ operation: "open", reason: "invalid_payload", cause }),
        ),
        Effect.flatMap((request) =>
          hubJson(request, HubActionResult, "attach stream", BOOT_TIMEOUT),
        ),
      );
    }
    if (hosts.get(host.id) !== host)
      return yield* new DeviceHostUnavailableError({
        hostId: host.id,
        reason: "Host configuration changed. Retry the operation.",
      });
    const openedAt = DateTime.formatIso(yield* DateTime.now);
    const session: DeviceSession = {
      threadId: input.threadId,
      hostId: host.id,
      deviceId: device.id,
      platform: device.platform,
      openedAt,
      toolSessionId: yield* crypto.randomUUIDv4().pipe(Effect.orDie),
    };
    yield* lifecycleLock.withPermit(
      Effect.gen(function* () {
        if (!(yield* readDeviceSettings).enabled)
          return yield* new DeviceHostUnavailableError({
            hostId: host.id,
            reason: "Device support was turned off while the device was opening.",
          });
        const previous = (yield* SynchronizedRef.get(stateRef)).state.sessions.filter(
          (existing) =>
            existing.threadId === session.threadId &&
            existing.hostId === session.hostId &&
            existing.deviceId === session.deviceId,
        );
        yield* recheckAction;
        yield* Effect.forEach(previous, retireCommandSession, { discard: true });
        const caller = yield* Effect.serviceOption(Invocation.McpInvocationContext);
        const request = yield* CurrentRequest;
        if (Option.isSome(caller) && request !== undefined)
          commandOwners.set(session, {
            namespace: requestNamespace(caller.value),
            generation: request.generation,
          });
        yield* publish((current) => ({
          ...current,
          sessions: [
            ...current.sessions.filter(
              (existing) =>
                !(
                  existing.threadId === session.threadId &&
                  existing.hostId === session.hostId &&
                  existing.deviceId === session.deviceId
                ),
            ),
            session,
          ],
        }));
      }),
    );
    return session;
  }, commandLock.withPermit);

  const shutdownDevice = Effect.fn("DeviceService.shutdownDevice")(function* (
    hostId: DeviceHostId,
    deviceId: DeviceId,
    platform: DevicePlatform,
  ) {
    const ready = yield* readiness(hostId);
    const closing = (yield* SynchronizedRef.get(stateRef)).state.sessions.filter(
      (session) => session.hostId === ready.hostId && session.deviceId === deviceId,
    );
    const postShutdown = (path: string, body: Record<string, string>) =>
      HttpClientRequest.post(`${ready.hub.origin}${path}`).pipe(
        HttpClientRequest.bodyJson(body),
        Effect.mapError(
          (cause) =>
            new DeviceOperationError({ operation: "shutdown", reason: "invalid_payload", cause }),
        ),
        Effect.flatMap((request) => hubJson(request, HubActionResult, "shutdown")),
        Effect.flatMap((result) =>
          result.ok
            ? Effect.void
            : Effect.fail(
                new DeviceOperationError({
                  operation: "shutdown",
                  reason: "hub_rejected",
                  cause: result,
                }),
              ),
        ),
      );
    // serve-sim's shutdown closes its in-process capture session before it runs
    // `simctl shutdown`; the hub's generic shutdown can leave that session cached
    // across a reboot. serve-sim runs simctl bare, though, so a simulator that is
    // already off fails there. Accept that failure only when the hub confirms
    // the simulator is off; a failure on a running one still surfaces.
    // Readiness can wait while authority changes. Keep this refusal outside
    // the iOS already-stopped recovery so it cannot be converted to success.
    yield* recheckAction;
    yield* platform === "ios"
      ? postShutdown(`${vendorPrefix("ios")}/grid/api/shutdown`, { udid: deviceId }).pipe(
          Effect.catch((cause) =>
            fetchDevices(ready).pipe(
              Effect.flatMap(({ devices }) =>
                devices.find((device) => device.id === deviceId)?.booted === false
                  ? Effect.logInfo("iOS simulator was already shut down", { deviceId })
                  : Effect.fail(cause),
              ),
            ),
          ),
        )
      : postShutdown("/api/devices/shutdown", { platform, id: deviceId });
    yield* Effect.forEach(closing, forgetCommandSession, { discard: true });
    yield* publish((state) => ({
      ...state,
      devices: state.devices.map((device) =>
        device.hostId === ready.hostId && device.id === deviceId
          ? { ...device, booted: false }
          : device,
      ),
      sessions: state.sessions.filter(
        (session) => !(session.hostId === ready.hostId && session.deviceId === deviceId),
      ),
    }));
    // Discovery can stall while an emulator saves its snapshot. A failed
    // refresh must not turn an accepted shutdown into an action failure.
    yield* refresh(ready).pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Device discovery unavailable after shutdown", { cause }),
      ),
    );
  });

  const close: DeviceService["Service"]["close"] = Effect.fn("DeviceService.close")(function* (
    input,
  ) {
    yield* recheckAction;
    const { state } = yield* SynchronizedRef.get(stateRef);
    const closing = state.sessions.filter(
      (session) =>
        session.threadId === input.threadId &&
        (input.hostId === undefined || session.hostId === input.hostId) &&
        (input.deviceId === undefined || session.deviceId === input.deviceId),
    );
    if (closing.length === 0) return;
    yield* Effect.forEach(closing, retireCommandSession, { discard: true });
    yield* publish((current) => ({
      ...current,
      sessions: current.sessions.filter((session) => !closing.includes(session)),
    }));
    if (input.shutdown) {
      yield* Effect.forEach(
        closing,
        (session) => shutdownDevice(session.hostId, session.deviceId, session.platform),
        { discard: true },
      );
    }
  }, commandLock.withPermit);

  const shutdown: DeviceService["Service"]["shutdown"] = Effect.fn("DeviceService.shutdown")(
    function* (input) {
      const host = yield* resolveHost(input.hostId);
      yield* shutdownDevice(host.id, input.deviceId, input.platform);
      yield* publish((current) => ({
        ...current,
        sessions: current.sessions.filter(
          (session) => !(session.hostId === host.id && session.deviceId === input.deviceId),
        ),
      }));
    },
    commandLock.withPermit,
  );

  const screenshot: DeviceService["Service"]["screenshot"] = Effect.fn("DeviceService.screenshot")(
    function* (input) {
      const { ready, device } = yield* resolveDevice(input.hostId, input.deviceId);
      const url = `${ready.hub.origin}${vendorPrefix(device.platform)}/api/screenshot?device=${encodeURIComponent(device.id)}`;
      yield* recheckAction;
      const png = yield* httpClient.execute(HttpClientRequest.post(url)).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap((response) => response.arrayBuffer),
        Effect.map((buffer) => new Uint8Array(buffer)),
        Effect.scoped,
        Effect.timeout(SCREENSHOT_TIMEOUT),
        Effect.mapError(
          (cause) =>
            new DeviceOperationError({
              operation: "screenshot",
              reason: "request_failed",
              cause,
            }),
        ),
      );
      return { device, png };
    },
  );

  const resolveDevice = Effect.fn("DeviceService.resolveDevice")(function* (
    hostId: DeviceHostId | undefined,
    deviceId: DeviceId,
  ) {
    const host = yield* resolveHost(hostId);
    const ready = yield* readiness(host.id);
    const { state } = yield* SynchronizedRef.get(stateRef);
    const device =
      findDevice(state, host.id, deviceId) ?? findDevice(yield* refresh(ready), host.id, deviceId);
    if (!device) return yield* new DeviceNotFoundError({ hostId: host.id, deviceId });
    return { ready, device };
  });

  const detail: DeviceService["Service"]["detail"] = Effect.fn("DeviceService.detail")(
    function* (input) {
      const { ready, device } = yield* resolveDevice(input.hostId, input.deviceId);
      const read = yield* readDeviceDetail(ready, device.platform, device.id);
      return {
        hostId: ready.hostId,
        deviceId: device.id,
        settings: read.settings,
        foregroundApp: read.foregroundApp,
        readAt: DateTime.formatIso(yield* DateTime.now),
      };
    },
  );

  const action: DeviceService["Service"]["action"] = Effect.fn("DeviceService.action")(
    function* (input) {
      const { ready, device } = yield* resolveDevice(input.hostId, input.deviceId);
      yield* runDeviceAction(ready, device.platform, input);
      return yield* detail({ hostId: ready.hostId, deviceId: device.id });
    },
  );

  const sessionsForThread: DeviceService["Service"]["sessionsForThread"] = (threadId) =>
    SynchronizedRef.get(stateRef).pipe(
      Effect.map(({ state }) => state.sessions.filter((session) => session.threadId === threadId)),
    );

  const authorizeViewer: DeviceService["Service"]["authorizeViewer"] = Effect.fn(
    "DeviceService.authorizeViewer",
  )(function* (resource) {
    yield* recheckAction;
    const { state } = yield* SynchronizedRef.get(stateRef);
    const session = state.sessions.find(
      (session) =>
        session.threadId === resource.threadId &&
        session.hostId === resource.hostId &&
        session.deviceId === resource.deviceId &&
        session.platform === resource.platform &&
        session.toolSessionId === resource.toolSessionId,
    );
    if (!session || !(yield* readDeviceSettings).enabled)
      return yield* new DeviceHostUnavailableError({
        hostId: resource.hostId,
        reason: "This exact device opening is no longer available.",
      });
    const ready = yield* currentReadiness(resource.hostId);
    if (!ready)
      return yield* new DeviceHostUnavailableError({
        hostId: resource.hostId,
        reason: "The device hub is not running.",
      });
    yield* recheckAction;
    const currentSettings = yield* readDeviceSettings;
    const current = yield* SynchronizedRef.get(stateRef);
    if (!currentSettings.enabled || !current.state.sessions.includes(session))
      return yield* new DeviceHostUnavailableError({
        hostId: resource.hostId,
        reason: "This device opening or its consent was retired during readiness.",
      });
    return ready;
  });

  const executeCommand: DeviceService["Service"]["executeCommand"] = Effect.fn(
    "DeviceService.executeCommand",
  )((untrusted) =>
    commandLock.withPermit(
      Effect.gen(function* () {
        const input = yield* decodeCommandInput(untrusted).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Invalid typed device command.",
              }),
          ),
        );
        const originalScope = yield* Invocation.requireThreadMcpCapability("device").pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "Agent device access is unavailable for this caller.",
              }),
          ),
        );
        let scope = originalScope;
        const recheck = Effect.gen(function* () {
          const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
          if (Option.isSome(authority))
            scope = yield* authority.value.recheckInvocation(
              originalScope,
              "device",
              originalScope.thread.threadId,
            );
          else if (originalScope.authority !== undefined || originalScope.client?.linked === true)
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "Current invocation authority is unavailable.",
            });
        });
        yield* recheck;
        const runtime = yield* commandPreflight(input.target.hostId);
        const request = yield* CurrentRequest;
        if (request === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Typed device commands require negotiated durable request identity.",
          });
        // Host/version inspection can wait after admission. Replay is still a current-authority action.
        yield* recheck;
        const previous = yield* externalReplay<DeviceCommandResult>();
        if (Option.isSome(previous)) {
          yield* recheck;
          return previous.value;
        }
        const session = (yield* sessionsForThread(scope.thread.threadId)).find(
          (session) =>
            session.hostId === input.target.hostId &&
            session.deviceId === input.target.deviceId &&
            session.toolSessionId === input.target.toolSessionId,
        );
        if (session === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "This exact device session is no longer open in this thread.",
          });
        const owner = commandOwners.get(session);
        if (
          owner === undefined ||
          owner.namespace !== requestNamespace(scope) ||
          owner.generation !== request.generation
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "This exact opening belongs to another caller or connection generation.",
          });
        const commands = input.command.type === "batch" ? input.command.steps : [input.command];
        // Validate every step before accepting any effect. Nested batches and runtime overrides
        // are absent from the atomic schema, and close can only terminate the sequence.
        const captures = new Set(commandCaptures.keys());
        for (const [index, command] of commands.entries()) {
          if (command.type === "close" && index !== commands.length - 1)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Close must be the final batch step.",
            });
          if (
            (command.type === "prepare" || command.type === "action-button") &&
            session.platform !== "ios"
          )
            return yield* new DeviceActionUnavailableError({
              operation: command.type,
              platform: session.platform,
              reason: "unsupported",
            });
          if (
            command.type === "settings" &&
            ((["faceid", "touchid"].includes(command.setting) && session.platform !== "ios") ||
              (command.setting === "fingerprint" && session.platform !== "android"))
          )
            return yield* new DeviceActionUnavailableError({
              operation: `settings ${command.setting}`,
              platform: session.platform,
              reason: "unsupported",
            });
          if (command.type === "record" || command.type === "trace") {
            const key = `${session.toolSessionId}:${command.type}`;
            if ((command.action === "start") === captures.has(key))
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Capture sequence does not belong to this exact opening.",
              });
            if (command.action === "start") captures.add(key);
            else captures.delete(key);
          }
        }
        const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
        if (Option.isNone(staging))
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Native artifact ownership is unavailable; no capture was accepted.",
          });
        const inputArtifact = (command: DeviceAtomicCommand) =>
          Effect.gen(function* () {
            yield* recheck;
            if (command.type !== "install" && command.type !== "reinstall") return undefined;
            const reference = command.artifact;
            const extension = runtime.path.extname(reference.name).toLowerCase();
            const allowed = session.platform === "android" ? [".apk", ".aab"] : [".zip"];
            if (reference.environmentId !== scope.environmentId || !allowed.includes(extension))
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message:
                  "Stage a supported app package in the destination context before installation.",
              });
            const verified = yield* staging.value.verify(scope, {
              type: "file",
              id: reference.attachmentId,
              name: reference.name,
              mimeType: reference.mimeType,
              sizeBytes: reference.sizeBytes,
            });
            if (
              verified.reference.owner !== reference.owner ||
              verified.digest !== reference.digest ||
              verified.reference.environmentId !== reference.environmentId
            )
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "The qualified app artifact no longer matches its owner and bytes.",
              });
            return verified.path;
          });
        const publishJson = (document: unknown, name: string, maximumBytes: number) =>
          Effect.gen(function* () {
            yield* recheck;
            const text = yield* encodeCommandJson(document).pipe(Effect.orDie);
            const bytes = new TextEncoder().encode(text);
            const id = createAttachmentId(scope.thread.threadId, "json");
            if (id === null || bytes.length > maximumBytes)
              return yield* commandUnavailable(session.hostId);
            const attachment = {
              type: "file" as const,
              id,
              name,
              mimeType: "application/json",
              sizeBytes: bytes.length,
            };
            const file = resolveAttachmentPath({
              attachmentsDir: runtime.config.attachmentsDir,
              attachment,
            });
            if (file === null) return yield* commandUnavailable(session.hostId);
            yield* runtime.fs
              .makeDirectory(runtime.path.dirname(file), { recursive: true })
              .pipe(Effect.orDie);
            yield* recheck;
            yield* runtime.fs.writeFile(file, bytes, { flag: "wx" }).pipe(Effect.orDie);
            return {
              type: "artifact" as const,
              target: input.target,
              artifact: yield* recheck.pipe(
                Effect.andThen(staging.value.publishReference(scope, attachment, "device")),
              ),
            };
          });
        const qualifyArtifact = (reference: PeerAttachmentReference) => ({
          _tag: "peer-attachment" as const,
          threadId: scope.thread.threadId,
          reference,
        });
        const qualifyResult = (result: DeviceAtomicCommandResult): DeviceAtomicCommandResult =>
          result.type === "artifact"
            ? { ...result, resource: qualifyArtifact(result.artifact) }
            : result.type === "artifacts"
              ? { ...result, resources: result.artifacts.map(qualifyArtifact) }
              : result;
        const boundedResult = (raw: DeviceCommandResult) =>
          Effect.gen(function* () {
            const result =
              raw.type === "batch"
                ? { ...raw, results: raw.results.map(qualifyResult) }
                : qualifyResult(raw);
            const text = yield* encodeCommandJson(result).pipe(Effect.orDie);
            // Keep the complete validated tree/sequence at its native artifact owner,
            // instead of turning a successful capture into an oversized unknown receipt.
            return new TextEncoder().encode(text).length <= 48_000
              ? result
              : qualifyResult(
                  yield* publishJson(result, `device-${input.command.type}.json`, 20 * 1024 * 1024),
                );
          });
        // Ownership and platform shape can be refused without starting any external helper.
        yield* Effect.forEach(commands, inputArtifact, { discard: true, concurrency: 1 });
        const inputReferences = commands.flatMap((command) =>
          command.type === "install" || command.type === "reinstall" ? [command.artifact] : [],
        );
        if (inputReferences.length > 0) {
          yield* recheck;
          yield* staging.value.reserveTransfer(
            scope,
            `device-input:${request.id}`,
            `device:${requestDigest(input.target)}`,
            inputReferences.map((reference) => ({
              type: "file" as const,
              id: reference.attachmentId,
              name: reference.name,
              mimeType: reference.mimeType,
              sizeBytes: reference.sizeBytes,
            })),
            inputReferences,
          );
          yield* recheck;
        }
        return yield* externalDispatch(
          Effect.gen(function* () {
            // Preparation is effectful and belongs to the same unknown-outcome boundary as dispatch.
            yield* recheck;
            yield* checkAgentAccess(session.hostId);
            const ready = yield* agentReadinessIfSupported(session.hostId);
            if (ready === null) return yield* commandUnavailable(session.hostId);
            const running = (yield* runtime.host.summary).tools;
            if (
              running?.agent.runningVersion !== AGENT_DEVICE_VERSION ||
              running.hub.runningVersion !== DEVICE_HUB_VERSION
            )
              return yield* commandUnavailable(session.hostId);
            yield* recheck;
            yield* configureAgent(session.hostId, ready);
            const current = (yield* sessionsForThread(scope.thread.threadId)).includes(session);
            if (!current || hosts.get(session.hostId) !== runtime.host)
              return yield* commandUnavailable(session.hostId);
            const privateSession = `${yield* agentDeviceSession(scope.thread.threadId, session.hostId, session.deviceId).pipe(Effect.provideService(Crypto.Crypto, crypto))}-${session.toolSessionId}`;
            const configured = runtime.path.join(
              runtime.config.stateDir,
              "device",
              "sessions",
              `${session.toolSessionId}.json`,
            );
            yield* recheck;
            yield* writeAgentDeviceConfig(configured, ready.agentDevice).pipe(
              Effect.provideService(FileSystem.FileSystem, runtime.fs),
              Effect.provideService(Path.Path, runtime.path),
              Effect.mapError(() => commandUnavailable(session.hostId)),
            );
            const binding = {
              nodePath: runtime.nodePath,
              entryPath: runtime.entry.entryPath,
              configPath: configured,
              cwd: runtime.path.dirname(configured),
              session: privateSession,
              platform: session.platform,
              deviceId: session.deviceId,
              runner: runtime.runner,
            };
            commandBindings.set(session, binding);
            let availableCommands: ReadonlyArray<string> = [];
            if (commands.some((command) => command.type !== "close")) {
              yield* recheck;
              const available = yield* AgentDeviceCommand.run({
                ...binding,
                command: { type: "capabilities" },
              });
              if (
                available === undefined ||
                !("availableCommands" in available) ||
                available.device.id !== session.deviceId ||
                available.device.platform !== session.platform
              )
                return yield* commandUnavailable(session.hostId);
              availableCommands = available.availableCommands;
              for (const command of commands) {
                if (
                  command.type !== "close" &&
                  command.type !== "capabilities" &&
                  !availableCommands.includes(command.type)
                )
                  return yield* new DeviceActionUnavailableError({
                    operation: command.type,
                    platform: session.platform,
                    reason: "unsupported",
                  });
              }
            }
            const executeBound = (
              command: DeviceAtomicCommand,
            ): Effect.Effect<DeviceAtomicCommandResult, DeviceError | OrchestratorMcpFailure> =>
              Effect.gen(function* () {
                yield* recheck;
                if (command.type === "capabilities")
                  return {
                    type: "capabilities" as const,
                    target: input.target,
                    commands: [
                      ...new Set([
                        ...availableCommands.filter((name) =>
                          AgentDeviceCommand.supportedCommands.some(
                            (supported) => supported === name,
                          ),
                        ),
                        "batch",
                      ]),
                    ],
                  };
                const captureKey =
                  command.type === "record" || command.type === "trace"
                    ? `${session.toolSessionId}:${command.type}`
                    : undefined;
                yield* checkAgentAccess(session.hostId);
                if (
                  !(yield* sessionsForThread(scope.thread.threadId)).includes(session) ||
                  hosts.get(session.hostId) !== runtime.host
                )
                  return yield* commandUnavailable(session.hostId);
                let capture =
                  captureKey === undefined ? undefined : commandCaptures.get(captureKey);
                if (captureKey !== undefined && "action" in command && command.action === "start") {
                  const id = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
                  const directory = runtime.path.join(
                    runtime.config.stateDir,
                    "device",
                    "media",
                    input.target.toolSessionId,
                    id,
                  );
                  yield* runtime.fs
                    .makeDirectory(directory, { recursive: true })
                    .pipe(Effect.orDie);
                  capture = {
                    directory,
                    outputPath: runtime.path.join(
                      directory,
                      command.type === "record" ? "recording.mp4" : "trace.log",
                    ),
                  };
                  commandCaptures.set(captureKey, capture);
                }
                if (command.type === "screenshot") {
                  yield* recheck;
                  const capture = yield* screenshot(session);
                  const bytes = capture.png;
                  if (
                    bytes.length < 24 ||
                    bytes.length > 10 * 1024 * 1024 ||
                    new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0) !==
                      0x89504e47
                  )
                    return yield* new DeviceOperationError({
                      operation: "capture",
                      reason: "invalid_payload",
                      cause: "Invalid PNG capture.",
                    });
                  const id = createAttachmentId(scope.thread.threadId, "png");
                  if (id === null || Option.isNone(staging))
                    return yield* commandUnavailable(session.hostId);
                  const attachment = {
                    type: "image" as const,
                    id,
                    name: "device.png",
                    mimeType: "image/png",
                    sizeBytes: bytes.length,
                  };
                  const file = resolveAttachmentPath({
                    attachmentsDir: runtime.config.attachmentsDir,
                    attachment,
                  });
                  if (file === null) return yield* commandUnavailable(session.hostId);
                  yield* runtime.fs
                    .makeDirectory(runtime.path.dirname(file), { recursive: true })
                    .pipe(Effect.orDie);
                  yield* recheck;
                  yield* runtime.fs.writeFile(file, bytes, { flag: "wx" }).pipe(Effect.orDie);
                  return {
                    type: "artifact" as const,
                    target: input.target,
                    artifact: yield* recheck.pipe(
                      Effect.andThen(staging.value.publishReference(scope, attachment, "device")),
                    ),
                  };
                }
                const inputPath = yield* inputArtifact(command);
                yield* recheck;
                const result = yield* AgentDeviceCommand.run({
                  ...binding,
                  command: command,
                  ...(inputPath === undefined ? {} : { inputPath }),
                  ...(capture === undefined ? {} : { outputPath: capture.outputPath }),
                });
                if (result !== undefined && "document" in result) {
                  if (Option.isNone(staging)) return yield* commandUnavailable(session.hostId);
                  const document = AgentDeviceCommand.diagnosticDocument(
                    command.type,
                    result.document,
                    [
                      ready.agentDevice.token,
                      ready.agentDevice.baseUrl,
                      runtime.config.baseDir,
                      runtime.entry.entryPath,
                      configured,
                    ],
                  );
                  return yield* publishJson(document, `device-${command.type}.json`, 512 * 1024);
                }
                if (captureKey !== undefined && "action" in command && command.action === "stop") {
                  if (
                    capture === undefined ||
                    result === undefined ||
                    !("outPath" in result) ||
                    Option.isNone(staging)
                  )
                    return yield* commandUnavailable(session.hostId);
                  const artifacts: PeerAttachmentReference[] = [];
                  const paths = result.artifacts?.length
                    ? result.artifacts.map((artifact) => artifact.localPath ?? artifact.path)
                    : [capture.outputPath];
                  yield* recheck;
                  const directory = yield* runtime.fs
                    .realPath(capture.directory)
                    .pipe(Effect.orDie);
                  for (const [index, candidate] of paths.entries()) {
                    if (candidate === undefined || !runtime.path.isAbsolute(candidate))
                      return yield* commandUnavailable(session.hostId);
                    const file = yield* runtime.fs.realPath(candidate).pipe(Effect.orDie);
                    const relative = runtime.path.relative(directory, file);
                    if (
                      relative === "" ||
                      relative.startsWith("..") ||
                      runtime.path.isAbsolute(relative)
                    )
                      return yield* commandUnavailable(session.hostId);
                    const info = yield* runtime.fs.stat(file).pipe(Effect.orDie);
                    if (info.type !== "File" || info.size < 1n || info.size > 50n * 1024n * 1024n)
                      return yield* commandUnavailable(session.hostId);
                    const extension = runtime.path.extname(file).toLowerCase().slice(1);
                    if (!["mp4", "png", "json", "log"].includes(extension))
                      return yield* commandUnavailable(session.hostId);
                    yield* recheck;
                    let bytes = yield* runtime.fs.readFile(file).pipe(Effect.orDie);
                    if (
                      extension === "mp4" &&
                      (bytes.length < 12 ||
                        new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
                          4,
                        ) !== 0x66747970)
                    )
                      return yield* commandUnavailable(session.hostId);
                    if (
                      extension === "png" &&
                      (bytes.length < 24 ||
                        new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
                          0,
                        ) !== 0x89504e47)
                    )
                      return yield* commandUnavailable(session.hostId);
                    if (extension === "json" || extension === "log") {
                      let text = yield* Effect.try({
                        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
                        catch: () => commandUnavailable(session.hostId),
                      });
                      const privateValues = [
                        ready.agentDevice.token,
                        ready.agentDevice.baseUrl,
                        runtime.config.baseDir,
                        runtime.entry.entryPath,
                        configured,
                      ];
                      if (extension === "json") {
                        const value = yield* decodeCommandJson(text).pipe(
                          Effect.mapError(() => commandUnavailable(session.hostId)),
                        );
                        text = yield* encodeCommandJson(
                          AgentDeviceCommand.diagnosticDocument(command.type, value, privateValues),
                        ).pipe(Effect.orDie);
                      } else text = AgentDeviceCommand.redactText(text, privateValues).text;
                      bytes = new TextEncoder().encode(text);
                    }
                    const pendingId = createPendingAttachmentId(extension);
                    const uuid = parseAttachmentUuid(pendingId);
                    const segment = toSafeThreadAttachmentSegment(scope.thread.threadId);
                    const suffix = parseAttachmentFileExtension(pendingId);
                    if (!uuid || !segment || !suffix)
                      return yield* commandUnavailable(session.hostId);
                    const attachment = {
                      type: "file" as const,
                      id: `${segment}-${uuid}-${suffix}`,
                      name: `device-${command.type}-${index + 1}.${extension}`,
                      mimeType:
                        extension === "mp4"
                          ? "video/mp4"
                          : extension === "png"
                            ? "image/png"
                            : extension === "json"
                              ? "application/json"
                              : "text/plain",
                      sizeBytes: bytes.length,
                    };
                    const pending = resolveAttachmentPath({
                      attachmentsDir: runtime.config.attachmentsDir,
                      attachment: { ...attachment, id: pendingId },
                    });
                    if (pending === null) return yield* commandUnavailable(session.hostId);
                    yield* runtime.fs
                      .makeDirectory(runtime.path.dirname(pending), { recursive: true })
                      .pipe(Effect.orDie);
                    yield* recheck;
                    yield* runtime.fs.writeFile(pending, bytes, { flag: "wx" }).pipe(Effect.orDie);
                    yield* recheck;
                    artifacts.push(
                      yield* staging.value.claimRecording(
                        scope,
                        pendingId,
                        attachment,
                        "device",
                        recheck,
                      ),
                    );
                  }
                  commandCaptures.delete(captureKey);
                  return {
                    type: "artifacts" as const,
                    target: input.target,
                    artifacts,
                    recording: {
                      ...(result.durationMs === undefined ? {} : { durationMs: result.durationMs }),
                      ...(result.capturedDurationMs === undefined
                        ? {}
                        : { capturedDurationMs: result.capturedDurationMs }),
                      ...(result.recorder === undefined ? {} : { recorder: result.recorder }),
                      ...(result.nativePathDisposition === undefined
                        ? {}
                        : { nativePathDisposition: result.nativePathDisposition }),
                    },
                  };
                }
                if (command.type === "close") {
                  yield* forgetCommandSession(session);
                  yield* publish((current) => ({
                    ...current,
                    sessions: current.sessions.filter((candidate) => candidate !== session),
                  }));
                  return { type: "closed" as const, target: input.target };
                }
                if (command.type === "snapshot" && result !== undefined && "nodes" in result) {
                  const serialized = yield* encodeCommandJson(result).pipe(Effect.orDie);
                  if (
                    [
                      ready.agentDevice.token,
                      ready.agentDevice.baseUrl,
                      configured,
                      runtime.entry.entryPath,
                    ].some(
                      (privateValue) =>
                        privateValue.length > 0 && serialized.includes(privateValue),
                    )
                  )
                    return yield* commandUnavailable(session.hostId);
                  return {
                    type: "snapshot" as const,
                    target: input.target,
                    ...result,
                    truncated: result.truncated ?? false,
                  };
                }
                return {
                  type: "action" as const,
                  target: input.target,
                  status: "dispatched" as const,
                };
              }).pipe(
                Effect.onError(() =>
                  Effect.sync(() => {
                    if (
                      (command.type === "record" || command.type === "trace") &&
                      command.action === "start"
                    )
                      commandCaptures.delete(`${session.toolSessionId}:${command.type}`);
                  }),
                ),
              );
            if (input.command.type !== "batch")
              return yield* boundedResult(yield* executeBound(input.command));
            const results = yield* Effect.forEach(input.command.steps, executeBound, {
              concurrency: 1,
            });
            return yield* boundedResult({ type: "batch" as const, target: input.target, results });
          }),
        );
      }),
    ),
  );

  return {
    ...DeviceService.of({
      testHost,
      updateTool: (tool) =>
        lifecycleLock.withPermit(
          Effect.gen(function* () {
            if (!installTool)
              return yield* new DeviceOperationError({
                operation: "update device tool",
                reason: "request_failed",
                cause: new Error("Tool installation is unavailable in this device service."),
              });
            yield* installTool(tool);
            return yield* inspect;
          }),
        ),
      retryHost,
      inspect,
      agentCli: Effect.fail(
        new DeviceHostUnavailableError({
          hostId: LOCAL_DEVICE_HOST_ID,
          reason: "Agent CLI installation is unavailable in this device service.",
        }),
      ),
      checkAgentAccess,
      checkCommandAccess,
      commandCatalog: (hostId, target) =>
        Effect.gen(function* () {
          const { host } = yield* commandPreflight(hostId);
          let targetPlatform: DevicePlatform | undefined;
          if (target !== undefined) {
            const current = yield* Effect.serviceOption(Invocation.McpInvocationContext);
            const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
            if (Option.isNone(current) || Option.isNone(authority) || target.hostId !== host.id)
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "Exact device input preflight is unavailable.",
              });
            const original = yield* Invocation.requireThreadScope(
              current.value,
              "Device input preflight",
            );
            const scope = yield* authority.value.recheckInvocation(
              original,
              "device",
              original.thread.threadId,
            );
            const session = (yield* sessionsForThread(scope.thread.threadId)).find(
              (session) =>
                session.hostId === target.hostId &&
                session.deviceId === target.deviceId &&
                session.toolSessionId === target.toolSessionId,
            );
            targetPlatform = session?.platform;
            const owner = session === undefined ? undefined : commandOwners.get(session);
            if (
              owner === undefined ||
              owner.namespace !== requestNamespace(scope) ||
              owner.generation !== requestGeneration(scope)
            )
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "This exact device opening is unavailable to the input caller.",
              });
            yield* authority.value.recheckInvocation(original, "device", original.thread.threadId);
          }
          return {
            protocol: 1 as const,
            inputArtifacts: 1 as const,
            ...(targetPlatform === undefined ? {} : { targetPlatform }),
            hostId: host.id,
            agentDeviceVersion: AGENT_DEVICE_VERSION,
            deviceHubVersion: DEVICE_HUB_VERSION,
            commands: AgentDeviceCommand.supportedCommands,
            families: AgentDeviceCommand.catalogFamilies,
          };
        }),
      executeCommand,
      agentTarget: (input) =>
        Effect.gen(function* () {
          const host = yield* resolveHost(input.hostId);
          const ready = yield* agentReadinessIfSupported(input.hostId);
          if (!ready)
            return yield* new DeviceHostUnavailableError({
              hostId: input.hostId,
              reason:
                "Agent device access requires enabled device support, agent access, and an available simulator platform on this host.",
            });
          const configPath = yield* lifecycleLock.withPermit(
            Effect.gen(function* () {
              if (hosts.get(host.id) !== host)
                return yield* new DeviceHostUnavailableError({
                  hostId: host.id,
                  reason: "Host configuration changed. Retry the operation.",
                });
              yield* recheckAction;
              return yield* configureAgent(input.hostId, ready);
            }),
          );
          const session = yield* agentDeviceSession(
            input.threadId,
            input.hostId,
            input.deviceId,
          ).pipe(Effect.provideService(Crypto.Crypto, crypto));
          return ["--config", configPath, "--session", session];
        }),
      state: SynchronizedRef.get(stateRef).pipe(Effect.map(({ state }) => state)),
      subscribe: PubSub.subscribe(statePubSub),
      configure,
      list,
      open,
      close,
      shutdown,
      detail,
      action,
      screenshot,
      readiness,
      readinessIfSupported,
      agentReadinessIfSupported,
      authorizeViewer,
      currentReadiness,
      sessionsForThread,
    }),
    setHostStatus,
    withLifecycleLock: lifecycleLock.withPermit,
    refreshHosts: Effect.gen(function* () {
      const summaries = yield* Effect.forEach(hosts.values(), (host) => host.summary);
      const unchanged = (id: DeviceHostId) =>
        hosts.has(id) && hosts.get(id) === publishedHosts.get(id);
      yield* Effect.forEach(
        [...commandOwners.keys()].filter((session) => !unchanged(session.hostId)),
        forgetCommandSession,
        { discard: true },
      );
      yield* publish((state) => ({
        ...state,
        hosts: summaries,
        hostStatuses: Object.fromEntries(
          Object.entries(state.hostStatuses).filter(([id]) => unchanged(id)),
        ),
        devices: state.devices.filter((device) => unchanged(device.hostId)),
        sessions: state.sessions.filter((session) => unchanged(session.hostId)),
      }));
      publishedHosts = new Map(hosts);
    }),
  };
});

/** @public */
/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const localHost = yield* DeviceHost.DeviceHost;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const runner = yield* ProcessRunner.ProcessRunner;
  const settings = yield* ServerSettings.ServerSettingsService;
  const scope = yield* Scope.Scope;
  const hosts = new Map<DeviceHostId, DeviceHost.DeviceHost["Service"]>([
    [localHost.id, localHost],
  ]);
  const crypto = yield* Crypto.Crypto;
  const configPath = (hostId: DeviceHostId) =>
    agentDeviceConfigPath(config.stateDir, hostId, path).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
    );
  const configureAgent = (hostId: DeviceHostId, ready: DeviceHost.DeviceHostAgentReady) =>
    Effect.gen(function* () {
      const file = yield* configPath(hostId);
      yield* writeAgentDeviceConfig(file, ready.agentDevice);
      return file;
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.mapError(
        (cause) =>
          new DeviceOperationError({
            operation: "configure agent",
            reason: "settings_failed",
            cause,
          }),
      ),
    );
  const probeContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof SshDeviceHost.probe>>>();
  const localTargetContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof isLocalSshDeviceHost>>>();
  const service = yield* makeWithHosts(
    hosts,
    (host) =>
      Effect.gen(function* () {
        if (yield* isLocalSshDeviceHost(host).pipe(Effect.provide(localTargetContext))) {
          return yield* localHost.summary;
        }
        return yield* SshDeviceHost.probe(host).pipe(Effect.provide(probeContext));
      }).pipe(
        Effect.mapError(
          (error) =>
            new DeviceOperationError({
              operation: "probe host",
              reason: "request_failed",
              cause: error,
            }),
        ),
      ),
    configureAgent,
    (tool) =>
      (tool === "hub" ? ensureDeviceHub(config.baseDir) : ensureAgentDevice(config.baseDir)).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.provideService(ProcessRunner.ProcessRunner, runner),
        Effect.mapError(
          (cause) =>
            new DeviceOperationError({
              operation: "update device tool",
              reason: "command_failed",
              cause,
            }),
        ),
      ),
  );
  const hostContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof SshDeviceHost.make>>>();
  const configured = new Map<string, { config: SshDeviceHostConfig; scope: Scope.Closeable }>();
  const reconcile = (configuredHosts: ReadonlyArray<SshDeviceHostConfig>) =>
    Effect.gen(function* () {
      const next = yield* remoteSshDeviceHosts(configuredHosts).pipe(
        Effect.provide(localTargetContext),
      );
      const removed = yield* service.withLifecycleLock(
        Effect.gen(function* () {
          const removed: Array<{ id: string; scope: Scope.Closeable }> = [];
          for (const [id, previous] of configured) {
            if (
              next.some(
                (host) =>
                  host.id === id &&
                  host.label === previous.config.label &&
                  host.target === previous.config.target &&
                  host.port === previous.config.port &&
                  host.identityFile === previous.config.identityFile,
              )
            )
              continue;
            hosts.delete(id);
            configured.delete(id);
            removed.push({ id, scope: previous.scope });
          }
          if (removed.length > 0) yield* service.refreshHosts;
          return removed;
        }),
      );
      // Stop old writers before deleting config files or publishing replacements, without blocking healthy hosts.
      yield* Effect.forEach(
        removed,
        ({ id, scope }) =>
          Effect.gen(function* () {
            yield* Scope.close(scope, Exit.void);
            yield* fs.remove(yield* configPath(id), { force: true }).pipe(Effect.ignore);
          }),
        { concurrency: 4, discard: true },
      );
      yield* service.withLifecycleLock(
        Effect.gen(function* () {
          let added = false;
          for (const host of next) {
            if (configured.has(host.id)) continue;
            added = true;
            const hostScope = yield* Scope.fork(scope);
            const instance = yield* SshDeviceHost.make(
              host,
              (ready) =>
                configureAgent(host.id, ready).pipe(
                  Effect.asVoid,
                  Effect.mapError(
                    (error) =>
                      new DeviceHost.DeviceHostError({
                        hostId: host.id,
                        step: "configuring agent access",
                        cause: error,
                      }),
                  ),
                ),
              (status, detail) =>
                service
                  .setHostStatus(host.id, { status, ...(detail ? { detail } : {}) })
                  .pipe(Effect.asVoid),
            ).pipe(Effect.provideService(Scope.Scope, hostScope), Effect.provide(hostContext));
            hosts.set(host.id, instance);
            configured.set(host.id, { config: host, scope: hostScope });
          }
          // Refresh again after removed scopes closed so a late status write from an old host cannot linger.
          if (added || removed.length > 0) yield* service.refreshHosts;
        }),
      );
    });
  const changes = yield* settings.subscribeChanges;
  yield* reconcile((yield* settings.getSettings).deviceHosts);
  yield* changes.pipe(
    Stream.runForEach((value) => reconcile(value.deviceHosts)),
    Effect.forkIn(scope),
  );
  yield* Effect.addFinalizer(() =>
    Effect.forEach(configured.values(), (value) => Scope.close(value.scope, Exit.void), {
      discard: true,
      concurrency: 4,
    }),
  );
  return {
    ...service,
    agentCli: resolveNodeExecutable("Device automation").pipe(
      Effect.andThen(ensureAgentDevice(config.baseDir)),
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.map((tool) => tool.entryPath),
      Effect.mapError((error) =>
        error._tag === "NodeRuntimeUnavailableError"
          ? new DeviceHostUnavailableError({
              hostId: LOCAL_DEVICE_HOST_ID,
              reason: nodeRuntimeUnavailableMessage("Device automation"),
              cause: error,
            })
          : new DeviceOperationError({
              operation: "install agent CLI",
              reason: "command_failed",
              cause: error,
            }),
      ),
    ),
  };
});

export const layer = Layer.effect(DeviceService, make).pipe(Layer.provide(LocalDeviceHost.layer));

export const stateStream = (service: DeviceService["Service"]): Stream.Stream<DeviceServiceState> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Subscribe before reading the snapshot so no change between the two
      // is lost; the scope lives as long as the stream does.
      const subscription = yield* service.subscribe;
      const initial = yield* service.state;
      return Stream.concat(Stream.make(initial), Stream.fromSubscription(subscription));
    }),
  ).pipe(Stream.scoped);
