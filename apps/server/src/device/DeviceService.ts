import {
  type DeviceActionInput,
  type DeviceViewerResource,
  type DeviceCloseInput,
  type DeviceConfigureInput,
  type DeviceDetail,
  type DeviceDetailInput,
  type DeviceError,
  DeviceHostId,
  DeviceId,
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
  type DeviceEasAccount,
  DeviceEasAccountError,
  type DeviceEasAccountInput,
  type DeviceSimlockIdentity,
  DeviceSimlockIdentityError,
  type DeviceSimlockIdentityInput,
  type DeviceHostConfig,
  type EasDeviceHostConfig,
  type SimlockDeviceHostConfig,
  type SshDeviceHostConfig,
  type DeviceHostSummary,
  LOCAL_DEVICE_HOST_ID,
  LOCAL_DEVICE_HOST_LABEL,
  type ThreadId,
  DeviceCommandInput,
  type DeviceCommandResult,
  type DeviceAtomicCommand,
  type DeviceAtomicCommandResult,
  type DeviceCommandCatalog,
  type PeerAttachmentReference,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { SshCommandError } from "@t3tools/ssh/errors";
import * as FileSystem from "effect/FileSystem";
import { resolveNodeExecutable, nodeRuntimeUnavailableMessage } from "@t3tools/shared/nodeRuntime";
import * as Path from "effect/Path";
import { ensureAgentDevice, ensureDeviceHub } from "./DeviceToolchain.ts";
import {
  agentDeviceCloseOutcome,
  bindAgentDeviceLauncher,
  ensureAgentDeviceShim,
  pruneAgentDeviceLaunchers,
} from "./AgentDeviceShim.ts";
import * as ServerConfig from "../config.ts";
import {
  agentDeviceConfigPath,
  agentDeviceSession,
  writeAgentDeviceConfig,
} from "./AgentDeviceTarget.ts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as HttpClient from "effect/http/HttpClient";

import * as ServerSettings from "../serverSettings.ts";
import { isLocalSshDeviceHost, remoteSshDeviceHosts } from "./localSshDeviceHost.ts";

import { readDeviceDetail, runDeviceAction } from "./DeviceActions.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as DeviceHost from "./DeviceHost.ts";
import * as SshDeviceHost from "./SshDeviceHost.ts";
import * as EasDeviceHost from "./EasDeviceHost.ts";
import * as SimlockDeviceHost from "./SimlockDeviceHost.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { makeExpoApi, resolveExpoCredential } from "./ExpoApi.ts";
import * as Exit from "effect/Exit";
import * as LocalDeviceHost from "./LocalDeviceHost.ts";
import * as AgentDeviceCommand from "./AgentDeviceCommand.ts";
import {
  AGENT_DEVICE_VERSION,
  DEVICE_HUB_VERSION,
  installedAgentDevice,
} from "./DeviceToolchain.ts";
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

const sshProbeFailureMessage = (error: DeviceHost.DeviceHostError): string => {
  const cause = error.cause;
  if (
    !(cause instanceof SshCommandError) ||
    cause.exitCode === null ||
    cause.exitCode < 1 ||
    cause.exitCode > 254
  )
    return "Could not connect to this host over SSH.";

  // Only the known bootstrap diagnostic is safe to expose as Node guidance.
  if (
    cause.exitCode === 1 &&
    cause.stderr
      .split(/\r?\n/u)
      .some((line) => line.trim() === "Node is missing from the non-interactive SSH PATH")
  )
    return "Connected over SSH, but Node is not on this host's PATH for non-interactive SSH commands.";

  return `Connected over SSH, but the device host check failed (exit code ${cause.exitCode}).`;
};

export interface DeviceScreenshot {
  readonly device: DeviceSummary;
  readonly png: Uint8Array;
}

export interface DeviceReadiness extends DeviceHost.DeviceHostReady {
  readonly hostId: DeviceHostId;
}

export interface DeviceAgentReadiness extends DeviceReadiness {
  /** Null when each device has its own daemon; see `DeviceOperations.agentEndpoint`. */
  readonly agentDevice: DeviceHost.AgentDeviceEndpoint | null;
}

const DeviceHostName = Schema.Struct({ id: DeviceHostId, label: Schema.optional(Schema.String) });
const hostName = (host: typeof DeviceHostName.Type) => host.label ?? `host ${host.id}`;

export class DeviceHostAmbiguousError extends Schema.TaggedError<DeviceHostAmbiguousError>()(
  "DeviceHostAmbiguousError",
  { deviceId: DeviceId, hosts: Schema.Array(DeviceHostName) },
) {
  override get message(): string {
    return `Device ${this.deviceId} is on more than one host: ${this.hosts.map(hostName).join(", ")}.`;
  }
}

export class DeviceOnOtherHostError extends Schema.TaggedError<DeviceOnOtherHostError>()(
  "DeviceOnOtherHostError",
  { deviceId: DeviceId, host: DeviceHostName, hosts: Schema.Array(DeviceHostName) },
) {
  override get message(): string {
    return `Device ${this.deviceId} is not on ${hostName(this.host)}. It is on ${this.hosts.map(hostName).join(", ")}.`;
  }
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
    /** Who an Expo credential signs in as and which EAS projects it can run sessions under. */
    readonly easAccount: (
      input: DeviceEasAccountInput,
    ) => Effect.Effect<DeviceEasAccount, DeviceEasAccountError>;
    /** Who a Simlock agent token is; without a gateway or token, this machine's agent-device's. */
    readonly simlockIdentity: (
      input: DeviceSimlockIdentityInput,
    ) => Effect.Effect<DeviceSimlockIdentity, DeviceSimlockIdentityError>;
    /**
     * The agent-device flags that reach the device: its config and session,
     * and the id agent-device selects it by, which can differ from its id here.
     */
    /** Fresh consent and host availability, without starting a host or writing config. */
    readonly checkAgentAccess?: (hostId?: DeviceHostId) => Effect.Effect<void, DeviceError>;
    readonly agentTarget: (input: {
      threadId: ThreadId;
      hostId: DeviceHostId;
      deviceId: DeviceId;
    }) => Effect.Effect<
      {
        readonly agentDeviceId: string | null;
        readonly session: string;
        readonly args: ReadonlyArray<string>;
        readonly installDirectory?: string | undefined;
      },
      DeviceError
    >;
    /**
     * The command agents run for one agent-device session; it is removed when the session ends.
     * None when the session closed before it could be bound.
     */
    readonly agentLauncher: (input: {
      readonly threadId: ThreadId;
      readonly hostId: DeviceHostId;
      readonly deviceId: DeviceId;
      readonly targetArgs: ReadonlyArray<string>;
    }) => Effect.Effect<Option.Option<string>, DeviceError>;
    readonly state: Effect.Effect<DeviceServiceState>;
    readonly subscribe: Effect.Effect<PubSub.Subscription<DeviceServiceState>, never, Scope.Scope>;
    readonly configure: (
      input: DeviceConfigureInput,
    ) => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly list: Effect.Effect<DeviceServiceState, DeviceError>;
    readonly updateTool: (tool: "hub" | "agent") => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly inspect: Effect.Effect<DeviceServiceState>;
    readonly retryHost: (hostId: DeviceHostId) => Effect.Effect<DeviceServiceState, DeviceError>;
    readonly open: (
      input: DeviceOpenInput,
      context?: DeviceHost.DeviceOpenContext,
    ) => Effect.Effect<DeviceSession, DeviceError>;
    readonly close: (input: DeviceCloseInput) => Effect.Effect<void, DeviceError>;
    readonly shutdown: (input: DeviceShutdownInput) => Effect.Effect<void, DeviceError>;
    readonly detail: (input: DeviceDetailInput) => Effect.Effect<DeviceDetail, DeviceError>;
    readonly action: (input: DeviceActionInput) => Effect.Effect<DeviceDetail, DeviceError>;
    readonly screenshot: (input: {
      readonly hostId?: DeviceHostId | undefined;
      readonly deviceId: DeviceId;
    }) => Effect.Effect<DeviceScreenshot, DeviceError>;
    /**
     * The device an id names in the last listing. Ids are unique per host, not across hosts (an
     * Android AVD name can exist on two), so a hostId decides, and without one the id has to be
     * on a single host.
     */
    readonly locateDevice: (input: {
      readonly hostId?: DeviceHostId | undefined;
      readonly deviceId: DeviceId;
    }) => Effect.Effect<
      Option.Option<DeviceSummary>,
      DeviceHostAmbiguousError | DeviceOnOtherHostError
    >;
    /** As `locateDevice`, among the devices a thread has open. */
    readonly locateSession: (input: {
      readonly threadId: ThreadId;
      readonly hostId?: DeviceHostId | undefined;
      readonly deviceId: DeviceId;
    }) => Effect.Effect<
      Option.Option<DeviceSession>,
      DeviceHostAmbiguousError | DeviceOnOtherHostError
    >;
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

export interface AgentDeviceLaunchers {
  readonly bind: (input: {
    readonly session: string;
    readonly targetArgs: ReadonlyArray<string>;
  }) => Effect.Effect<string, DeviceError>;
  /** Removes every launcher whose session is not in `live`. */
  readonly prune: (live: ReadonlySet<string>) => Effect.Effect<void>;
  /** Ends the agent-device session behind a launcher, which releases its claim on the device. */
  readonly release?: (launcher: string) => Effect.Effect<void>;
}

export const makeWithHosts = Effect.fn("DeviceService.makeWithHosts")(function* (
  hosts: ReadonlyMap<DeviceHostId, DeviceHost.DeviceHost["Service"]>,
  testHost: DeviceService["Service"]["testHost"] = (host) =>
    Effect.fail(
      new DeviceHostUnavailableError({
        hostId: host.id,
        hostLabel: host.label,
        reason: "SSH probing is unavailable in this device service.",
      }),
    ),
  configureAgent: (
    hostId: DeviceHostId,
    endpoint: DeviceHost.AgentDaemonEndpoint & { readonly stateDir?: string | undefined },
    deviceId?: DeviceId,
  ) => Effect.Effect<string, DeviceError> = (hostId) =>
    Effect.fail(
      new DeviceHostUnavailableError({
        hostId,
        reason: "Agent configuration is unavailable in this device service.",
      }),
    ),
  installTool?: (tool: "hub" | "agent") => Effect.Effect<unknown, DeviceError>,
  launchers?: AgentDeviceLaunchers,
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
      supportsEasHosts: true,
      supportsSimlockHosts: true,
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

  const launcherLock = yield* Semaphore.make(1);
  /** Launchers bound since startup, by agent-device session. */
  const boundLaunchers = new Map<string, string>();
  /** Launchers on a session the host leases to every thread; the host ends it, not a retiring thread. */
  const hostSessionLaunchers = new Set<string>();
  // Binding and pruning read sessions under the lock, so neither acts on a session list the other has outdated.
  // A retired session's agent-device session ends before its launcher goes, and a reopen of the same
  // device waits for the lock, so the close cannot end the reopened session.
  const pruneLaunchers = launcherLock.withPermit(
    Effect.gen(function* () {
      if (!launchers) return;
      const { state } = yield* SynchronizedRef.get(stateRef);
      const live = new Set(
        yield* Effect.forEach(state.sessions, (session) =>
          agentDeviceSession(session.threadId, session.hostId, session.deviceId),
        ).pipe(Effect.provideService(Crypto.Crypto, crypto)),
      );
      const retired = [...boundLaunchers].filter(([session]) => !live.has(session));
      for (const [session] of retired) boundLaunchers.delete(session);
      const ending = retired.filter(([session]) => !hostSessionLaunchers.delete(session));
      const release = launchers.release;
      if (release)
        yield* Effect.forEach(ending, ([, launcher]) => release(launcher), {
          concurrency: "unbounded",
          discard: true,
        });
      yield* launchers.prune(live);
    }),
  );

  // Uninterruptible so a committed retirement always prunes, even if its caller is cancelled.
  const publish = (update: (state: DeviceServiceState) => DeviceServiceState) =>
    SynchronizedRef.modifyEffect(stateRef, ({ state }) => {
      const next = { ...update(state), revision: state.revision + 1 };
      const retired = state.sessions.some((session) => !next.sessions.includes(session));
      return PubSub.publish(statePubSub, next).pipe(
        Effect.as([{ next, retired }, { state: next }] as const),
      );
    }).pipe(
      Effect.tap(({ retired }) => (retired ? pruneLaunchers : Effect.void)),
      Effect.map(({ next }) => next),
      Effect.uninterruptible,
    );

  const resolveHost = Effect.fn("DeviceService.resolveHost")(function* (
    hostId: DeviceHostId | undefined,
  ) {
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
              [hostId]: {
                ...(state.hostStatuses[hostId]?.androidDiscoveryIncomplete
                  ? { androidDiscoveryIncomplete: true }
                  : {}),
                ...status,
              },
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
  const setReadinessStatus = Effect.fn("DeviceService.setReadinessStatus")(function* (
    hostId: DeviceHostId,
    status: DeviceServiceState["hostStatuses"][string],
    attempt?: ReturnType<typeof beginRefresh>,
  ) {
    if (attempt) {
      if (!(yield* canPublishRefresh(attempt)) || !attempt.host)
        return (yield* SynchronizedRef.get(stateRef)).state;
      if (status.status === "failed") publishedRefreshes.set(attempt.host, attempt.sequence);
    }
    return yield* setHostStatus(hostId, status);
  });

  const readiness: ReadinessWithRefreshAttempt<"readiness"> = Effect.fn("DeviceService.readiness")(
    function* (hostId, attempt) {
      const host = yield* resolveHost(hostId);
      if (!(yield* readDeviceSettings).enabled) {
        return yield* new DeviceHostUnavailableError({
          hostId: host.id,
          hostLabel: host.label,
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
            setReadinessStatus(
              host.id,
              {
                status: "failed",
                detail:
                  error._tag === "DeviceHostError" && error.step === "probe"
                    ? sshProbeFailureMessage(error)
                    : error.message,
              },
              attempt,
            ),
          ),
          Effect.mapError(
            (error) =>
              new DeviceHostUnavailableError({
                hostId: host.id,
                hostLabel: host.label,
                reason:
                  error._tag === "NodeRuntimeUnavailableError"
                    ? nodeRuntimeUnavailableMessage("Local device support")
                    : error.step === "probe"
                      ? sshProbeFailureMessage(error)
                      : `Device support failed during ${error.step}.`,
                cause: error,
              }),
          ),
        );
      if (hosts.get(host.id) !== host)
        return yield* new DeviceHostUnavailableError({
          hostId: host.id,
          hostLabel: host.label,
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

  const readinessIfSupported: ReadinessWithRefreshAttempt<"readinessIfSupported"> = Effect.fn(
    "DeviceService.readinessIfSupported",
  )(function* (hostId, attempt) {
    if (!(yield* readDeviceSettings).enabled) return null;
    const host = yield* resolveHost(hostId);
    const summary = yield* host.summary;
    if (summary.kind === "local" && !summary.platforms.some((platform) => platform.available))
      return null;
    return yield* readiness(host.id, attempt);
  });

  const supportedAgentHost = Effect.fn("DeviceService.supportedAgentHost")(function* (
    hostId?: DeviceHostId,
  ) {
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
  const commandPreflight = Effect.fn("DeviceService.commandPreflight")(function* (
    hostId?: DeviceHostId,
  ) {
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

  const forgetCommandSession = Effect.fn("DeviceService.forgetCommandSession")(function* (
    session: DeviceSession,
  ) {
    const binding = commandBindings.get(session);
    if (binding !== undefined && Option.isSome(commandFs))
      yield* commandFs.value.remove(binding.configPath, { force: true }).pipe(Effect.orDie);
    commandBindings.delete(session);
    commandOwners.delete(session);
    commandCaptures.delete(`${session.toolSessionId}:record`);
    commandCaptures.delete(`${session.toolSessionId}:trace`);
  });
  const retireCommandSession = Effect.fn("DeviceService.retireCommandSession")(function* (
    session: DeviceSession,
  ) {
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
            setReadinessStatus(
              host.id,
              {
                status: "failed",
                detail:
                  error._tag === "DeviceHostError" && error.step === "probe"
                    ? sshProbeFailureMessage(error)
                    : error.message,
              },
              attempt,
            ),
          ),
          Effect.mapError(
            (error) =>
              new DeviceHostUnavailableError({
                hostId: host.id,
                hostLabel: host.label,
                reason:
                  error._tag === "NodeRuntimeUnavailableError"
                    ? nodeRuntimeUnavailableMessage("Local device support")
                    : error._tag === "DeviceHostTimeoutError"
                      ? `Agent tools did not start within ${error.timeoutMs} ms.`
                      : error._tag === "AgentDeviceStartError"
                        ? error.message
                        : error.step === "probe"
                          ? sshProbeFailureMessage(error)
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

  const fetchDevices = Effect.fn("DeviceService.fetchDevices")(function* (ready: DeviceReadiness) {
    const { devices, detail, androidDiscoveryIncomplete } = yield* ready.devices.list;
    return {
      devices: devices.map((device): DeviceSummary => ({ ...device, hostId: ready.hostId })),
      detail,
      androidDiscoveryIncomplete,
    };
  });

  const refresh = Effect.fn("DeviceService.refresh")(function* (
    ready: DeviceReadiness,
    attempt = beginRefresh(ready.hostId),
  ) {
    // A session opened while the listing ran is on a device the listing could
    // not have seen yet; only sessions from before it can be pruned by it.
    const sessionKey = (session: DeviceSession) =>
      `${session.hostId}\u0000${session.deviceId}\u0000${session.threadId}\u0000${session.openedAt}`;
    const before = new Set((yield* SynchronizedRef.get(stateRef)).state.sessions.map(sessionKey));
    const { devices, detail, androidDiscoveryIncomplete } = yield* fetchDevices(ready);
    const hostSummaries = yield* Effect.forEach(hosts.values(), (host) => host.summary);
    return yield* publishRefresh(
      attempt,
      publish((state) => ({
        ...state,
        hosts: hostSummaries,
        ...(ready.hostId === LOCAL_DEVICE_HOST_ID
          ? { hostStatus: "ready", hostStatusDetail: detail }
          : {}),
        devices: [...state.devices.filter((device) => device.hostId !== ready.hostId), ...devices],
        // A host that ends devices itself (EAS stops sessions at their limits)
        // leaves sessions on devices it no longer lists with nothing to stream.
        // Other hosts can miss a device for one listing, so theirs stay.
        sessions: ready.devices.endsDevices
          ? state.sessions.filter(
              (session) =>
                session.hostId !== ready.hostId ||
                !before.has(sessionKey(session)) ||
                devices.some((device) => device.id === session.deviceId),
            )
          : state.sessions,
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

  const open: DeviceService["Service"]["open"] = Effect.fn("DeviceService.open")(function* (
    input,
    context,
  ) {
    yield* recheckAction;
    const host = yield* resolveHost(input.hostId);
    yield* ensurePlatform(host, input.platform);
    const ready = yield* readiness(host.id);
    let state = yield* refresh(ready);
    let device = findDevice(state, host.id, input.deviceId);
    if (!device) {
      return yield* new DeviceNotFoundError({ hostId: host.id, deviceId: input.deviceId });
    }
    // Hosts that route their own streams never encode emulator video with serve-emu's ffmpeg.
    if (device.platform === "android" && !device.physical && !ready.upstream) {
      // Resolve the override on the device host the way its hub does.
      const encoders = yield* ready.run(ready.nodePath, [
        "-e",
        `const { spawnSync } = require("node:child_process");
  const result = spawnSync(process.env.SERVE_EMU_FFMPEG?.trim() || "ffmpeg", ["-hide_banner", "-encoders"], {
    stdio: "inherit", timeout: 10000, killSignal: "SIGKILL",
  });
  process.exitCode = result.error ? 1 : (result.status ?? 1);`,
      ]);
      if (encoders.code !== 0) {
        return yield* new DevicePlatformUnavailableError({
          hostId: host.id,
          platform: "android",
          reason:
            "Android emulator video needs working ffmpeg with the libx264 encoder. Install it on the device host's PATH or set SERVE_EMU_FFMPEG there, then retry opening the emulator. After changing PATH or SERVE_EMU_FFMPEG, turn device support off and on for SSH hosts, or restart the local environment server.",
        });
      }
      if (!/^\s*V\S*[ \t]+libx264(?:[ \t]|$)/m.test(`${encoders.stdout}\n${encoders.stderr}`)) {
        return yield* new DevicePlatformUnavailableError({
          hostId: host.id,
          platform: "android",
          reason:
            "Android emulator video needs the libx264 encoder. Install an ffmpeg build that includes libx264 on the device host, then retry opening the emulator. After changing PATH or SERVE_EMU_FFMPEG, turn device support off and on for SSH hosts, or restart the local environment server.",
        });
      }
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
      const bootedId = yield* ready.devices.boot(device, context).pipe(
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
    } else if (device.booted) {
      yield* recheckAction;
      yield* ready.devices.attach(device);
    }
    if (hosts.get(host.id) !== host)
      return yield* new DeviceHostUnavailableError({
        hostId: host.id,
        hostLabel: host.label,
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
            hostLabel: host.label,
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
    // Readiness can wait while authority changes; recheck before the device action.
    yield* recheckAction;
    yield* ready.devices.shutdown({ id: deviceId, platform });
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
    // Snapshot saves can stall discovery; refresh failure must not fail an accepted shutdown.
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
    const ending = (session: DeviceSession) =>
      closing.includes(session) ||
      (input.shutdown === true &&
        closing.some(
          (closed) => closed.hostId === session.hostId && closed.deviceId === session.deviceId,
        ));
    yield* Effect.forEach(state.sessions.filter(ending), retireCommandSession, { discard: true });
    yield* publish((current) => ({
      ...current,
      sessions: current.sessions.filter((session) => !ending(session)),
    }));
    if (input.shutdown) {
      yield* Effect.forEach(
        closing,
        (session) => shutdownDevice(session.hostId, session.deviceId, session.platform),
        { discard: true },
      );
    }
  }, commandLock.withPermit);

  // Sessions retire before the power-off, while their agent-device sessions can still close the app;
  // agent-device keeps a claim whose app close fails.
  const shutdown: DeviceService["Service"]["shutdown"] = Effect.fn("DeviceService.shutdown")(
    function* (input) {
      const host = yield* resolveHost(input.hostId);
      yield* publish((current) => ({
        ...current,
        sessions: current.sessions.filter(
          (session) => !(session.hostId === host.id && session.deviceId === input.deviceId),
        ),
      }));
      yield* shutdownDevice(host.id, input.deviceId, input.platform);
    },
    commandLock.withPermit,
  );

  const screenshot: DeviceService["Service"]["screenshot"] = Effect.fn("DeviceService.screenshot")(
    function* (input) {
      const { ready, device } = yield* resolveDevice(input.hostId, input.deviceId);
      yield* recheckAction;
      return { device, png: yield* ready.devices.screenshot(device) };
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

  const locate = <T extends { readonly hostId: DeviceHostId }>(
    state: DeviceServiceState,
    input: { readonly hostId?: DeviceHostId | undefined; readonly deviceId: DeviceId },
    matches: ReadonlyArray<T>,
  ): Effect.Effect<Option.Option<T>, DeviceHostAmbiguousError | DeviceOnOtherHostError> => {
    const named = (id: DeviceHostId) => ({
      id,
      label: state.hosts.find((host) => host.id === id)?.label,
    });
    const onHost = matches.filter(
      (match) => input.hostId === undefined || match.hostId === input.hostId,
    );
    const hostIds = [...new Set(onHost.map((match) => match.hostId))];
    if (hostIds.length === 1) return Effect.succeedSome(onHost.at(-1)!);
    if (hostIds.length > 1) {
      return Effect.fail(
        new DeviceHostAmbiguousError({ deviceId: input.deviceId, hosts: hostIds.map(named) }),
      );
    }
    const elsewhere = [...new Set(matches.map((match) => match.hostId))];
    return input.hostId !== undefined && elsewhere.length > 0
      ? Effect.fail(
          new DeviceOnOtherHostError({
            deviceId: input.deviceId,
            host: named(input.hostId),
            hosts: elsewhere.map(named),
          }),
        )
      : Effect.succeedNone;
  };

  const locateDevice: DeviceService["Service"]["locateDevice"] = Effect.fn(
    "DeviceService.locateDevice",
  )(function* (input) {
    const { state } = yield* SynchronizedRef.get(stateRef);
    return yield* locate(
      state,
      input,
      state.devices.filter((device) => device.id === input.deviceId),
    );
  });

  const locateSession: DeviceService["Service"]["locateSession"] = Effect.fn(
    "DeviceService.locateSession",
  )(function* (input) {
    const { state } = yield* SynchronizedRef.get(stateRef);
    return yield* locate(
      state,
      input,
      state.sessions.filter(
        (session) => session.threadId === input.threadId && session.deviceId === input.deviceId,
      ),
    );
  });

  // Sessions live in memory, so this removes every launcher a previous run left behind.
  yield* pruneLaunchers;
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
        const inputArtifact = Effect.fn("DeviceService.inputArtifact")(function* (
          command: DeviceAtomicCommand,
        ) {
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
        const publishJson = Effect.fn("DeviceService.publishJson")(function* (
          document: unknown,
          name: string,
          maximumBytes: number,
        ) {
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
        const boundedResult = Effect.fn("DeviceService.boundedResult")(function* (
          raw: DeviceCommandResult,
        ) {
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
            // Registered commands drive one host-wide daemon; per-device daemon hosts are not supported.
            const agentDevice = ready?.agentDevice ?? null;
            if (agentDevice === null) return yield* commandUnavailable(session.hostId);
            const running = (yield* runtime.host.summary).tools;
            if (
              running?.agent.runningVersion !== AGENT_DEVICE_VERSION ||
              running.hub.runningVersion !== DEVICE_HUB_VERSION
            )
              return yield* commandUnavailable(session.hostId);
            yield* recheck;
            yield* configureAgent(session.hostId, agentDevice);
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
            yield* writeAgentDeviceConfig(configured, agentDevice).pipe(
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
            const executeBound: (
              command: DeviceAtomicCommand,
            ) => Effect.Effect<DeviceAtomicCommandResult, DeviceError | OrchestratorMcpFailure> =
              Effect.fn("DeviceService.executeBound")(
                function* (command: DeviceAtomicCommand) {
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
                  if (
                    captureKey !== undefined &&
                    "action" in command &&
                    command.action === "start"
                  ) {
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
                      new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
                        0,
                      ) !== 0x89504e47
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
                        agentDevice.token,
                        agentDevice.baseUrl,
                        runtime.config.baseDir,
                        runtime.entry.entryPath,
                        configured,
                      ],
                    );
                    return yield* publishJson(document, `device-${command.type}.json`, 512 * 1024);
                  }
                  if (
                    captureKey !== undefined &&
                    "action" in command &&
                    command.action === "stop"
                  ) {
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
                          agentDevice.token,
                          agentDevice.baseUrl,
                          runtime.config.baseDir,
                          runtime.entry.entryPath,
                          configured,
                        ];
                        if (extension === "json") {
                          const value = yield* decodeCommandJson(text).pipe(
                            Effect.mapError(() => commandUnavailable(session.hostId)),
                          );
                          text = yield* encodeCommandJson(
                            AgentDeviceCommand.diagnosticDocument(
                              command.type,
                              value,
                              privateValues,
                            ),
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
                      yield* runtime.fs
                        .writeFile(pending, bytes, { flag: "wx" })
                        .pipe(Effect.orDie);
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
                        ...(result.durationMs === undefined
                          ? {}
                          : { durationMs: result.durationMs }),
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
                        agentDevice.token,
                        agentDevice.baseUrl,
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
                },
                (effect, command) =>
                  effect.pipe(
                    Effect.onError(() =>
                      Effect.sync(() => {
                        if (
                          (command.type === "record" || command.type === "trace") &&
                          command.action === "start"
                        )
                          commandCaptures.delete(`${session.toolSessionId}:${command.type}`);
                      }),
                    ),
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
      updateTool: Effect.fn("DeviceService.updateTool")(function (tool) {
        return lifecycleLock.withPermit(
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
        );
      }),
      retryHost,
      inspect,
      easAccount: Effect.fn("DeviceService.easAccount")(function () {
        return Effect.fail(
          new DeviceEasAccountError({
            reason: "EAS accounts are unavailable in this device service.",
          }),
        );
      }),
      simlockIdentity: Effect.fn("DeviceService.simlockIdentity")(function () {
        return Effect.fail(
          new DeviceSimlockIdentityError({
            reason: "Simlock hosts are unavailable in this device service.",
          }),
        );
      }),

      agentCli: Effect.fail(
        new DeviceHostUnavailableError({
          hostId: LOCAL_DEVICE_HOST_ID,
          reason: "Agent CLI installation is unavailable in this device service.",
        }),
      ),
      checkAgentAccess,
      checkCommandAccess,
      commandCatalog: Effect.fn("DeviceService.commandCatalog")(function* (hostId, target) {
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
      agentTarget: Effect.fn("DeviceService.agentTarget")(function* (input) {
        const host = yield* resolveHost(input.hostId);
        const ready = yield* agentReadinessIfSupported(input.hostId);
        if (!ready)
          return yield* new DeviceHostUnavailableError({
            hostId: host.id,
            hostLabel: host.label,
            reason:
              "Agent device access requires enabled device support, agent access, and an available simulator platform on this host.",
          });
        const { configPath, agentDeviceId, boundSession, installDirectory } =
          yield* lifecycleLock.withPermit(
            Effect.gen(function* () {
              if (hosts.get(host.id) !== host)
                return yield* new DeviceHostUnavailableError({
                  hostId: host.id,
                  hostLabel: host.label,
                  reason: "Host configuration changed. Retry the operation.",
                });
              yield* recheckAction;
              if (ready.devices.agentEndpoint) {
                const endpoint = yield* ready.devices.agentEndpoint(input.deviceId);
                return {
                  configPath: yield* configureAgent(input.hostId, endpoint, input.deviceId),
                  agentDeviceId: endpoint.agentDeviceId,
                  boundSession: endpoint.session,
                  installDirectory: endpoint.installDirectory,
                };
              }
              if (!ready.agentDevice)
                return yield* new DeviceHostUnavailableError({
                  hostId: host.id,
                  hostLabel: host.label,
                  reason: "This host has no agent-device daemon.",
                });
              return {
                configPath: yield* configureAgent(input.hostId, ready.agentDevice),
                agentDeviceId: input.deviceId,
                boundSession: undefined,
                installDirectory: undefined,
              };
            }),
          );
        const ownSession = yield* agentDeviceSession(
          input.threadId,
          input.hostId,
          input.deviceId,
        ).pipe(Effect.provideService(Crypto.Crypto, crypto));
        if (boundSession) hostSessionLaunchers.add(ownSession);
        const session = boundSession ?? ownSession;
        return {
          agentDeviceId,
          session,
          args: ["--config", configPath, "--session", session],
          installDirectory,
        };
      }),
      agentLauncher: Effect.fn("DeviceService.agentLauncher")(function* ({
        targetArgs,
        ...target
      }) {
        if (!launchers)
          return yield* new DeviceHostUnavailableError({
            hostId: target.hostId,
            reason: "Agent launchers are unavailable in this device service.",
          });
        const session = yield* agentDeviceSession(
          target.threadId,
          target.hostId,
          target.deviceId,
        ).pipe(Effect.provideService(Crypto.Crypto, crypto));
        return yield* launcherLock.withPermit(
          Effect.gen(function* () {
            const { state } = yield* SynchronizedRef.get(stateRef);
            if (
              !state.sessions.some(
                (live) =>
                  live.threadId === target.threadId &&
                  live.hostId === target.hostId &&
                  live.deviceId === target.deviceId,
              )
            )
              return Option.none();
            const launcher = yield* launchers.bind({ session, targetArgs });
            boundLaunchers.set(session, launcher);
            return Option.some(launcher);
          }),
        );
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
      locateDevice,
      locateSession,
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
/** @public */
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
  const configPath = (hostId: DeviceHostId, deviceId?: DeviceId) =>
    agentDeviceConfigPath(config.stateDir, hostId, path, deviceId).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
    );
  const configureAgent = Effect.fn("DeviceService.configureAgent")(
    function* (
      hostId: DeviceHostId,
      endpoint: DeviceHost.AgentDaemonEndpoint & { readonly stateDir?: string | undefined },
      deviceId?: DeviceId,
    ) {
      const file = yield* configPath(hostId, deviceId);
      yield* writeAgentDeviceConfig(file, endpoint);
      return file;
    },
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
  const agentCli = resolveNodeExecutable("Device automation").pipe(
    Effect.andThen(ensureAgentDevice(config.baseDir)),
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
    Effect.provideService(ProcessRunner.ProcessRunner, runner),
    Effect.map((tool) => tool.entryPath),
    Effect.mapError((error) =>
      error._tag === "NodeRuntimeUnavailableError"
        ? new DeviceHostUnavailableError({
            hostId: LOCAL_DEVICE_HOST_ID,
            hostLabel: LOCAL_DEVICE_HOST_LABEL,
            reason: nodeRuntimeUnavailableMessage("Device automation"),
            cause: error,
          })
        : new DeviceOperationError({
            operation: "install agent CLI",
            reason: "command_failed",
            cause: error,
          }),
    ),
  );
  const launchers: AgentDeviceLaunchers = {
    bind: Effect.fn("DeviceService.bindLauncher")(
      function* (input) {
        const entryPath = yield* agentCli;
        // The unbound launcher stays current for agents that still call it with flags.
        yield* ensureAgentDeviceShim({ entryPath, stateDir: config.stateDir });
        return yield* bindAgentDeviceLauncher({ entryPath, stateDir: config.stateDir, ...input });
      },
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.catchTags({
        NodeRuntimeUnavailableError: (cause) =>
          Effect.fail(
            new DeviceHostUnavailableError({
              hostId: LOCAL_DEVICE_HOST_ID,
              hostLabel: LOCAL_DEVICE_HOST_LABEL,
              reason: nodeRuntimeUnavailableMessage("Device automation"),
              cause,
            }),
          ),
        PlatformError: (cause) =>
          Effect.fail(
            new DeviceHostUnavailableError({
              hostId: LOCAL_DEVICE_HOST_ID,
              hostLabel: LOCAL_DEVICE_HOST_LABEL,
              reason: "Could not prepare the agent-device launcher.",
              cause,
            }),
          ),
      }),
    ),
    prune: (live) =>
      pruneAgentDeviceLaunchers({ stateDir: config.stateDir, live }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
      ),
    // Best effort: an unreachable host fails fast in the launcher's connection check, and the
    // launcher goes either way. Only a failed or timed-out close can leave the device claimed.
    release: (launcher) =>
      resolveNodeExecutable("Device automation").pipe(
        Effect.flatMap((node) =>
          runner.run({
            command: node,
            args: [path.join(path.dirname(launcher), "agent-device-launcher.mjs"), "close"],
            env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
            timeout: "15 seconds",
            timeoutBehavior: "timedOutResult",
          }),
        ),
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.flatMap((result) => {
          const outcome = agentDeviceCloseOutcome(result);
          return outcome === "failed" || outcome === "timed-out"
            ? Effect.logWarning("Could not end the agent-device session", {
                launcher,
                outcome,
                stderr: result.stderr.slice(-500),
              })
            : Effect.void;
        }),
        Effect.tapError((cause) =>
          Effect.logWarning("Could not end the agent-device session", { launcher, cause }),
        ),
        Effect.ignore,
      ),
  };
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
              // Only the exit code goes into the shown message; npm's stderr can carry registry credentials.
              ...(cause.exitCode !== undefined ? { exitCode: cause.exitCode } : {}),
              cause,
            }),
        ),
      ),
    launchers,
  );
  const hostContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof SshDeviceHost.make>>>();
  const easContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof EasDeviceHost.make>>>();
  const simlockContext =
    yield* Effect.context<Effect.Services<ReturnType<typeof SimlockDeviceHost.make>>>();
  const simlockAgentCli = Effect.fn("DeviceService.simlockAgentCli")(
    function* (hostId: string) {
      const nodePath = yield* resolveNodeExecutable("Device automation");
      const tool = yield* ensureAgentDevice(config.baseDir).pipe(
        Effect.mapError(
          (cause) =>
            new DeviceHost.DeviceHostError({ hostId, step: "installing agent tools", cause }),
        ),
      );
      return { nodePath, entryPath: tool.entryPath };
    },
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
    Effect.provideService(ProcessRunner.ProcessRunner, runner),
  );
  const configured = new Map<string, { config: DeviceHostConfig; scope: Scope.Closeable }>();
  const unchanged = (current: DeviceHostConfig, previous: DeviceHostConfig) =>
    current.kind === "ssh" && previous.kind === "ssh"
      ? current.label === previous.label &&
        current.target === previous.target &&
        current.port === previous.port &&
        current.identityFile === previous.identityFile
      : JSON.stringify(current) === JSON.stringify(previous);
  const reconcile = Effect.fn("DeviceService.reconcile")(function* (
    configuredHosts: ReadonlyArray<DeviceHostConfig>,
  ) {
    const next = yield* remoteSshDeviceHosts(configuredHosts).pipe(
      Effect.provide(localTargetContext),
    );
    const removed = yield* service.withLifecycleLock(
      Effect.gen(function* () {
        const removed: Array<{ id: string; scope: Scope.Closeable }> = [];
        for (const [id, previous] of configured) {
          if (next.some((host) => host.id === id && unchanged(host, previous.config))) continue;
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
          if (host.kind === "simlock") {
            // Closing this scope releases the host's leases back to the pool.
            const instance = yield* SimlockDeviceHost.make(host, simlockAgentCli(host.id)).pipe(
              Effect.provideService(Scope.Scope, hostScope),
              Effect.provide(simlockContext),
            );
            hosts.set(host.id, instance);
            configured.set(host.id, { config: host, scope: hostScope });
            // Ask the gateway now, so a refused token shows in Settings before anyone opens a device.
            if ((yield* settings.getSettings).enableDeviceSupport)
              yield* instance
                .platformAvailability("ios")
                .pipe(
                  Effect.andThen(service.withLifecycleLock(service.refreshHosts)),
                  Effect.forkIn(hostScope),
                );
            continue;
          }
          if (host.kind === "eas") {
            // Closing this scope stops the host's running sessions, so a changed
            // limit or token never leaves a billed session behind.
            const instance = yield* EasDeviceHost.make(host).pipe(
              Effect.provideService(Scope.Scope, hostScope),
              Effect.provide(easContext),
            );
            hosts.set(host.id, instance);
            configured.set(host.id, { config: host, scope: hostScope });
            continue;
          }
          const instance = yield* SshDeviceHost.make(
            host,
            (ready) =>
              (ready.agentDevice
                ? configureAgent(host.id, ready.agentDevice).pipe(Effect.asVoid)
                : Effect.void
              ).pipe(
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
  // Each kind has its own settings list; an id two lists share stays the first one's.
  const configuredHosts = (value: {
    readonly deviceHosts: ReadonlyArray<SshDeviceHostConfig>;
    readonly easDeviceHosts: ReadonlyArray<EasDeviceHostConfig>;
    readonly simlockDeviceHosts: ReadonlyArray<SimlockDeviceHostConfig>;
    readonly simlockDeviceHostsEnabled: boolean;
  }): ReadonlyArray<DeviceHostConfig> => {
    const hosts: Array<DeviceHostConfig> = [...value.deviceHosts];
    for (const host of [
      ...value.easDeviceHosts,
      ...(value.simlockDeviceHostsEnabled ? value.simlockDeviceHosts : []),
    ]) {
      if (!hosts.some((existing) => existing.id === host.id)) hosts.push(host);
    }
    return hosts;
  };
  yield* reconcile(configuredHosts(yield* settings.getSettings));
  yield* changes.pipe(
    Stream.runForEach((value) => reconcile(configuredHosts(value))),
    Effect.forkIn(scope),
  );
  yield* Effect.addFinalizer(() =>
    Effect.forEach(configured.values(), (value) => Scope.close(value.scope, Exit.void), {
      discard: true,
      concurrency: 4,
    }),
  );
  const easAccount: DeviceService["Service"]["easAccount"] = Effect.fn("DeviceService.easAccount")(
    function* (input) {
      const saved =
        input.hostId === undefined
          ? ""
          : ((yield* settings.getSettings).easDeviceHosts.find((host) => host.id === input.hostId)
              ?.accessToken ?? "");
      const credential = yield* resolveExpoCredential(input.accessToken ?? saved);
      const viewer = yield* makeExpoApi(yield* HttpClient.HttpClient, credential).viewer;
      return { signedInAs: viewer.name, credentialSource: credential.source, apps: viewer.apps };
    },
    Effect.provide(easContext),
    // Only bounded text reaches the browser: the credential errors' own
    // guidance, or a category for what Expo answered, never Expo's text.
    Effect.mapError(
      (cause) =>
        new DeviceEasAccountError({
          reason:
            cause._tag === "ExpoApiError"
              ? cause.status === 401 || cause.status === 403
                ? `Expo rejected the credential (HTTP ${cause.status}).`
                : cause.rejected
                  ? "Expo refused the account check."
                  : "Could not complete the Expo account check."
              : cause.message,
        }),
    ),
  );

  const simlockIdentity: DeviceService["Service"]["simlockIdentity"] = Effect.fn(
    "DeviceService.simlockIdentity",
  )(
    function* (input) {
      const environment = yield* HostProcess.Environment;
      const gatewayUrl = (
        input.gatewayUrl || SimlockDeviceHost.machineSimlockToken(environment)?.gatewayUrl
      )?.replace(/\/$/, "");
      if (!gatewayUrl)
        return yield* new DeviceSimlockIdentityError({
          reason: "This machine's agent-device does not point at a Simlock gateway.",
        });
      const savedHost =
        input.hostId === undefined
          ? undefined
          : (yield* settings.getSettings).simlockDeviceHosts.find(
              (host) => host.id === input.hostId,
            );
      const token = input.agentToken
        ? { agentToken: input.agentToken, source: "entered" as const }
        : SimlockDeviceHost.resolveSimlockToken(gatewayUrl, savedHost, environment);
      if (!token)
        return yield* new DeviceSimlockIdentityError({
          reason: savedHost?.agentToken
            ? "The saved token is for another gateway. Enter one for this gateway."
            : "No agent token for this gateway on this machine.",
        });
      const identity = yield* SimlockDeviceHost.simlockIdentity(gatewayUrl, token.agentToken).pipe(
        Effect.mapError(
          (cause) =>
            new DeviceSimlockIdentityError({
              reason: SimlockDeviceHost.describeGatewayFailure(
                cause,
                gatewayUrl,
                token.source === "env" ? "this machine" : "this environment",
              ),
            }),
        ),
      );
      if (identity.role !== "agent")
        return yield* new DeviceSimlockIdentityError({
          reason: `That is a ${identity.role} token. Create one with --role agent.`,
        });
      return { gatewayUrl, signedInAs: identity.name, tokenSource: token.source };
    },
    Effect.catchTags({
      ServerSettingsError: () =>
        Effect.fail(new DeviceSimlockIdentityError({ reason: "Could not read the saved token." })),
    }),
    Effect.provide(simlockContext),
  );

  return {
    ...service,
    easAccount,
    simlockIdentity,
  };
});

export const layer = Layer.effect(DeviceService, make).pipe(Layer.provide(LocalDeviceHost.layer));

export const stateStream = (service: DeviceService["Service"]): Stream.Stream<DeviceServiceState> =>
  Stream.unwrap(
    Effect.gen(function* () {
      // Subscribe before reading so no change is lost.
      const subscription = yield* service.subscribe;
      const initial = yield* service.state;
      return Stream.concat(Stream.make(initial), Stream.fromSubscription(subscription));
    }),
  ).pipe(Stream.scoped);
