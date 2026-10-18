import type * as Tool from "effect/ai/Tool";
import {
  OrchestratorMcpFailure,
  type DeviceError,
  type DeviceHostId,
  type DeviceId,
  type DevicePlatform,
  type DeviceSummary,
  DeviceToolUnavailableError,
  LOCAL_DEVICE_HOST_ID,
  type DeviceCommandTarget,
  type DeviceAtomicCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ServerConfig from "../../../config.ts";
import { ensureAgentDeviceShim } from "../../../device/AgentDeviceShim.ts";
import { nodeRuntimeUnavailableMessage } from "@t3tools/shared/nodeRuntime";

import * as DeviceService from "../../../device/DeviceService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { DeviceScreenshotToolkit, DeviceStandardToolkit, DeviceToolkit } from "./tools.ts";

export function agentDeviceTargetArgs(device: DeviceSummary): ReadonlyArray<string> {
  return device.platform === "ios"
    ? ["--platform", "ios", "--udid", device.id]
    : ["--platform", "android", "--serial", device.id];
}

export function agentDeviceQuickStart(
  device: DeviceSummary,
  targetArgs = agentDeviceTargetArgs(device),
  command = "agent-device",
): string {
  const executable = /^[a-zA-Z0-9_./:-]+$/.test(command)
    ? command
    : "'" + command.replaceAll("'", "'\"'\"'") + "'";
  const target = targetArgs
    .map((arg) =>
      /^[a-zA-Z0-9_./:-]+$/.test(arg) ? arg : "'" + arg.replaceAll("'", "'\"'\"'") + "'",
    )
    .join(" ");
  const platformNotes =
    device.platform === "ios"
      ? "First use builds an XCTest runner and can take a couple of minutes; later commands are fast."
      : "The Android snapshot helper installs itself on first use.";
  return [
    `The user is watching ${device.name} (${device.version}) in the Device panel.`,
    `Drive it with ${executable}. Use this exact executable path; login shells may reset PATH. Always pass ${target}.`,
    "Typical loop:",
    `  ${executable} open <bundle-or-package-id> ${target}     # or: open <app> <deep-link-url>`,
    `  ${executable} snapshot -i ${target}                     # accessibility tree with @eN refs`,
    `  ${executable} click @e3 ${target}`,
    `  ${executable} fill @e5 "text" ${target}`,
    `  ${executable} screenshot /tmp/shot.png ${target}        # or call device_screenshot`,
    `  ${executable} install <app> <path-to-.app-or-.apk> ${target}`,
    `Prefer snapshot refs over coordinates. Run ${executable} help for workflow guides and ${executable} <command> --help for flags.`,
    "Prefer agent-device for driving this device. simctl, adb, and xcrun remain available for anything it does not cover.",
    "For remote hosts, arrange builds, app installation, and any Metro reverse forwarding yourself. T3 provides discovery, streaming, and control only.",
    "Keep the returned --config and --session flags on every command. Other hosts can be used concurrently; opening one does not switch these commands.",
    platformNotes,
  ].join("\n");
}

/** Remote guidance contains public typed intent and this exact opening only. */
function deviceCommandQuickStart(
  target: DeviceCommandTarget,
  scope: Pick<McpInvocationContext.McpThreadInvocationScope, "environmentId" | "thread">,
): string {
  const route = { environmentId: scope.environmentId, contextThreadId: scope.thread.threadId };
  const call = (command: DeviceAtomicCommand) =>
    JSON.stringify({
      ...route,
      target,
      command,
      clientRequestId: `${target.toolSessionId}:${command.type}:1`,
    });
  return [
    "Call device_commands to inspect destination command support. Each example includes a session-specific clientRequestId. Increment its suffix for each new intent; retry the same intent with the exact same key and input.",
    `device_commands ${JSON.stringify({ ...route, hostId: target.hostId })}`,
    `device_execute ${call({ type: "open", app: "com.example.app" })}`,
    `device_execute ${call({ type: "snapshot", interactiveOnly: true })}`,
    `device_execute ${call({ type: "click", ref: "@e3" })}`,
    `device_execute ${call({ type: "fill", ref: "@e5", text: "text" })}`,
    `device_execute ${call({ type: "screenshot" })}`,
    `device_execute ${call({ type: "close" })}`,
    "Use refs from this session's snapshot. Dispatched does not mean verified: inspect the resulting screen. Fetch returned artifact references through the authenticated attachment owner. Reopening returns a new session target.",
  ].join("\n");
}

const requireDeviceAccess = McpInvocationContext.requireThreadMcpCapability("device").pipe(
  Effect.mapError(
    () =>
      new DeviceToolUnavailableError({
        reason: "Agent device access is turned off for this environment.",
      }),
  ),
);

const pickDevice = (
  devices: ReadonlyArray<DeviceSummary>,
  input: {
    readonly deviceId?: DeviceId | undefined;
    readonly platform?: DevicePlatform | undefined;
    readonly hostId?: DeviceHostId | undefined;
  },
): Effect.Effect<DeviceSummary, DeviceToolUnavailableError> =>
  Effect.gen(function* () {
    const hostId = input.hostId ?? LOCAL_DEVICE_HOST_ID;
    if (input.deviceId !== undefined) {
      const match = devices.find(
        (device) => device.hostId === hostId && device.id === input.deviceId,
      );
      if (match && (input.platform === undefined || input.platform === match.platform))
        return match;
      return yield* new DeviceToolUnavailableError({
        reason: `No device ${input.deviceId} on host ${hostId}. Call device_list for current ids.`,
      });
    }
    const candidates = devices.filter(
      (device) =>
        device.hostId === hostId &&
        (input.platform === undefined || device.platform === input.platform),
    );
    if (candidates.length === 0) {
      return yield* new DeviceToolUnavailableError({
        reason:
          input.platform === undefined
            ? "No simulators or emulators were found. Call device_list to see why."
            : `No ${input.platform} devices were found on host ${hostId}. Call device_list to see why.`,
      });
    }
    const platforms = new Set(candidates.map((device) => device.platform));
    if (input.platform === undefined && platforms.size > 1) {
      return yield* new DeviceToolUnavailableError({
        reason: "Both iOS and Android devices are available; pass platform or deviceId.",
      });
    }
    return candidates.find((device) => device.booted) ?? candidates[0]!;
  });

const toolError = (
  error:
    | DeviceError
    | DeviceToolUnavailableError
    | import("@t3tools/contracts").OrchestratorMcpFailure,
) => error;

const handlers = {
  device_commands: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      yield* requireDeviceAccess;
      const devices = yield* DeviceService.DeviceService;
      return yield* devices.commandCatalog(input.hostId);
    }),
  ),
  device_execute: McpToolAccess.actsAsCaller(
    (input) =>
      Effect.gen(function* () {
        yield* requireDeviceAccess;
        const devices = yield* DeviceService.DeviceService;
        return yield* devices.executeCommand({ target: input.target, command: input.command });
      }),
    "external-unknown",
  ),
  device_list: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* requireDeviceAccess;
      const devices = yield* DeviceService.DeviceService;
      const state = yield* devices.list;
      if (state.hostStatus === "disabled") {
        return yield* new DeviceToolUnavailableError({
          reason:
            "Device support is off. Ask the user to enable it in the Device panel before installing or starting device tools.",
        });
      }
      const hostId = input?.hostId;
      const open = state.sessions
        .filter((session) => session.threadId === scope.thread.threadId)
        .map((session) => ({ hostId: session.hostId, deviceId: session.deviceId }));
      return {
        hostStatuses: Object.fromEntries(
          Object.entries(state.hostStatuses).filter(([id]) => !hostId || id === hostId),
        ),
        hosts: hostId ? state.hosts.filter((host) => host.id === hostId) : state.hosts,
        devices: hostId
          ? state.devices.filter((device) => device.hostId === hostId)
          : state.devices,
        open,
      };
    }).pipe(Effect.mapError(toolError)),
  ),
  device_open: McpToolAccess.actsAsCaller(
    (input) =>
      Effect.gen(function* () {
        const scope = yield* requireDeviceAccess;
        const devices = yield* DeviceService.DeviceService;
        const request = yield* CurrentRequest;
        if (request === undefined && scope.client?.linked === true)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message:
              "This peer has not negotiated durable typed device commands; no action was accepted.",
          });
        if (request !== undefined) {
          if (devices.checkAgentAccess === undefined)
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message:
                "The native device consent preflight is unavailable; no device action was accepted.",
            });
          yield* devices.checkAgentAccess(input.hostId);
          yield* devices.checkCommandAccess(input.hostId);
          const previous =
            yield* externalReplay<Tool.Success<typeof DeviceToolkit.tools.device_open>>();
          if (Option.isSome(previous)) return previous.value;
        }
        return yield* externalDispatch(
          Effect.gen(function* () {
            const state = yield* devices.list;
            if (state.hostStatus === "disabled") {
              return yield* new DeviceToolUnavailableError({
                reason:
                  "Device support is off. Ask the user to enable it in the Device panel before installing or starting device tools.",
              });
            }
            const target = yield* pickDevice(state.devices, input);
            if (request !== undefined && target.physical)
              return yield* new DeviceToolUnavailableError({
                reason:
                  "Typed execution currently uses the existing simulator/emulator capability. Physical-device consent and support must be established separately.",
              });
            // Resolve consent and agent connectivity before booting or registering a session.
            const agentArgs = yield* devices.agentTarget({
              threadId: scope.thread.threadId,
              hostId: target.hostId,
              deviceId: target.id,
            });
            const session = yield* devices.open({
              threadId: scope.thread.threadId,
              hostId: target.hostId,
              deviceId: target.id,
              platform: target.platform,
            });
            const after = yield* devices.state;
            const device =
              after.devices.find(
                (candidate) =>
                  candidate.hostId === session.hostId && candidate.id === session.deviceId,
              ) ?? target;
            if (request !== undefined) {
              if (session.toolSessionId === undefined)
                return yield* new DeviceToolUnavailableError({
                  reason: "The native exact-session owner is unavailable.",
                });
              const commandTarget = {
                hostId: session.hostId,
                deviceId: session.deviceId,
                toolSessionId: session.toolSessionId,
              };
              return {
                device,
                target: commandTarget,
                quickStart: deviceCommandQuickStart(commandTarget, scope),
              };
            }
            const targetArgs = [...agentDeviceTargetArgs(device), ...agentArgs];
            const config = yield* ServerConfig.ServerConfig;
            const path = yield* Path.Path;
            const platform = yield* HostProcess.Platform;
            const shimDir = yield* ensureAgentDeviceShim({
              entryPath: yield* devices.agentCli,
              stateDir: config.stateDir,
            }).pipe(
              Effect.mapError(
                (error) =>
                  new DeviceToolUnavailableError({
                    reason:
                      error._tag === "NodeRuntimeUnavailableError"
                        ? nodeRuntimeUnavailableMessage("Device automation")
                        : "Could not prepare the agent-device launcher.",
                    cause: error,
                  }),
              ),
            );
            const command = path.join(
              shimDir,
              platform === "win32" ? "agent-device.cmd" : "agent-device",
            );
            return {
              device,
              agentDevice: { command, targetArgs },
              quickStart: agentDeviceQuickStart(device, targetArgs, command),
            };
          }),
        );
      }).pipe(Effect.mapError(toolError)),
    "external-unknown",
  ),
  device_screenshot: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* requireDeviceAccess;
      const devices = yield* DeviceService.DeviceService;
      const sessions = yield* devices.sessionsForThread(scope.thread.threadId);
      const hostId =
        input.deviceId === undefined ? input.hostId : (input.hostId ?? LOCAL_DEVICE_HOST_ID);
      const target = sessions.findLast(
        (session) =>
          (hostId === undefined || session.hostId === hostId) &&
          (input.deviceId === undefined || session.deviceId === input.deviceId),
      );
      if (!target) {
        return yield* new DeviceToolUnavailableError({
          reason:
            input.deviceId === undefined
              ? "No device is open in this thread. Call device_open first."
              : `Device ${input.deviceId} on host ${hostId} is not open in this thread. Call device_open first.`,
        });
      }
      const shot = yield* devices.screenshot(target);
      return {
        device: shot.device,
        screenshot: {
          mimeType: "image/png" as const,
          data: Buffer.from(shot.png).toString("base64"),
          ...pngDimensions(shot.png),
        },
      };
    }).pipe(Effect.mapError(toolError)),
  ),
  device_close: McpToolAccess.actsAsCaller(
    (input) =>
      Effect.gen(function* () {
        const scope = yield* requireDeviceAccess;
        const devices = yield* DeviceService.DeviceService;
        if (Option.isSome(yield* externalReplay())) return {};
        const sessions = yield* devices.sessionsForThread(scope.thread.threadId);
        const closing = sessions.filter(
          (session) =>
            session.threadId === scope.thread.threadId &&
            (input.hostId === undefined || session.hostId === input.hostId) &&
            (input.deviceId === undefined || session.deviceId === input.deviceId),
        );
        yield* externalDispatch(
          closing.length === 0
            ? Effect.void
            : devices.close({
                threadId: scope.thread.threadId,
                ...(input.hostId === undefined ? {} : { hostId: input.hostId }),
                ...(input.deviceId === undefined ? {} : { deviceId: input.deviceId }),
                ...(input.shutdown === undefined ? {} : { shutdown: input.shutdown }),
              }),
        );
        return {};
      }).pipe(Effect.mapError(toolError)),
    "external-unknown",
  ),
} satisfies McpToolAccess.Handlers<typeof DeviceToolkit.tools>;

export function pngDimensions(png: Uint8Array): { width: number; height: number } {
  if (png.length < 24) return { width: 0, height: 0 };
  const view = new DataView(png.buffer, png.byteOffset, png.byteLength);
  const isPng =
    view.getUint32(0) === 0x89504e47 &&
    view.getUint32(4) === 0x0d0a1a0a &&
    view.getUint32(12) === 0x49484452;
  return isPng
    ? { width: view.getUint32(16), height: view.getUint32(20) }
    : { width: 0, height: 0 };
}

const { device_screenshot, ...standardHandlers } = handlers;

export const layerStandard = McpToolAccess.toLayer(DeviceStandardToolkit, standardHandlers);

export const layerScreenshot = McpToolAccess.toLayer(DeviceScreenshotToolkit, {
  device_screenshot,
});
import * as Option from "effect/Option";
import { CurrentRequest, externalDispatch, externalReplay } from "../../McpRequestReceipts.ts";
