import * as McpRouting from "../../McpRouting.ts";
import {
  OrchestratorMcpFailure,
  DeviceToolCloseInput,
  DeviceToolError,
  DeviceToolListResult,
  DeviceToolOpenInput,
  DeviceToolOpenResult,
  DeviceToolScreenshotResult,
  DeviceToolTargetInput,
  DeviceCommandInput,
  DeviceCommandResult,
  DeviceCommandCatalog,
  DeviceCommandTarget,
  DeviceHostId,
  closedDeviceCommand,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ServerConfig from "../../../config.ts";
import { Tool, Toolkit } from "effect/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as DeviceService from "../../../device/DeviceService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  DeviceService.DeviceService,
];

const DeviceToolFailure = Schema.Union([DeviceToolError, OrchestratorMcpFailure]);

const DeviceListTool = Tool.make("device_list", {
  description:
    "List iOS Simulators and Android Emulators on this environment's device hosts, which platforms each host can run, and which devices are already open in this thread's Device panel. Call this before device_open when you do not know a device id.",
  // An empty struct serializes as `anyOf [object, array]`, which some
  // providers reject and then drop every tool on the server with it.
  parameters: McpRouting.parameters(
    Schema.Struct({
      hostId: Schema.optional(
        Schema.String.annotate({ description: "Limit to one device host. Defaults to all hosts." }),
      ),
    }),
  ),
  success: DeviceToolListResult,
  failure: DeviceToolFailure,
  dependencies,
})
  .annotate(Tool.Title, "List devices")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const DeviceOpenTool = Tool.make("device_open", {
  description:
    "Open a simulator or emulator for this thread and show its Device panel. With durable requests, returns an exact session target for device_execute. Call device_commands first to check the destination's typed protocol and pinned runtime prerequisites.",
  parameters: McpRouting.parameters(DeviceToolOpenInput),
  success: DeviceToolOpenResult,
  failure: DeviceToolFailure,
  dependencies: [...dependencies, FileSystem.FileSystem, Path.Path, ServerConfig.ServerConfig],
})
  .annotate(Tool.Title, "Open device")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

export const DeviceScreenshotTool = Tool.make("device_screenshot", {
  description:
    "Capture the current screen of an open device as a PNG image. Use device_execute for typed taps/text and for a portable screenshot artifact reference.",
  parameters: McpRouting.parameters(DeviceToolTargetInput),
  success: DeviceToolScreenshotResult,
  failure: DeviceToolFailure,
  dependencies,
})
  .annotate(Tool.Title, "Screenshot device")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const DeviceCloseTool = Tool.make("device_close", {
  description:
    "Remove a device from this thread's Device panel. Pass shutdown=true to also power the simulator or emulator off.",
  parameters: McpRouting.parameters(DeviceToolCloseInput),
  success: Schema.Record(Schema.String, Schema.Never).annotate({
    description: "The device was closed.",
  }),
  failure: DeviceToolFailure,
  dependencies,
})
  .annotate(Tool.Title, "Close device")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

const DeviceCommandsTool = Tool.make("device_commands", {
  description:
    "Discover app-owned typed command support and check the installed destination runtime without starting helpers or touching a device. An older peer without this tool cannot execute typed commands.",
  parameters: McpRouting.parameters(
    Schema.Struct({
      hostId: Schema.optional(DeviceHostId),
      target: Schema.optional(DeviceCommandTarget),
    }),
  ),
  success: DeviceCommandCatalog,
  failure: DeviceToolFailure,
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.OpenWorld, false);

const DeviceExecuteTool = Tool.make("device_execute", {
  description:
    "Execute typed intent on the exact host/device/session returned by device_open. Snapshot refs belong to that session. Results contain bounded nodes, dispatch status or owned artifact references. Use the same clientRequestId for a retry; outcome_unknown never authorizes re-execution.",
  parameters: closedDeviceCommand(McpRouting.parameters(DeviceCommandInput).fields),
  success: DeviceCommandResult,
  failure: DeviceToolFailure,
  dependencies,
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.OpenWorld, true);

export const DeviceStandardToolkit = Toolkit.make(
  DeviceListTool,
  DeviceOpenTool,
  DeviceCloseTool,
  DeviceCommandsTool,
  DeviceExecuteTool,
);

export const DeviceScreenshotToolkit = Toolkit.make(DeviceScreenshotTool);

export const DeviceToolkit = Toolkit.make(
  DeviceListTool,
  DeviceOpenTool,
  DeviceScreenshotTool,
  DeviceCloseTool,
  DeviceCommandsTool,
  DeviceExecuteTool,
);
