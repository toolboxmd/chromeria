import {
  type ClientOrchestrationCommand,
  type ModelSelection,
  type ServerProvider,
  type ServerSettings,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";

import { effortOptionId, pickRoleModel } from "../mcp/toolkits/threads/roles.ts";
import { isSubagentThreadId } from "../mcp/toolkits/threads/subagentThreadId.ts";

/** Resolve the first turn only; an established conversation cannot reroute through Prism. */
export function resolvePromachosStart(
  command: Extract<ClientOrchestrationCommand, { type: "thread.turn.start" }>,
  threadExists: boolean,
  settings: ServerSettings,
  providers: ReadonlyArray<ServerProvider>,
  nowMs: number,
): { command: ClientOrchestrationCommand } | { refusal: string } {
  const createThread = command.bootstrap?.createThread;
  if (!createThread || threadExists || isSubagentThreadId(command.threadId)) {
    return {
      refusal:
        "Prism can select the Promachos model only when creating a new top-level conversation.",
    };
  }
  const preferences = resolveProjectSettings(settings, createThread.projectId).settings.prismRoles
    .promachos.models;
  const result = pickRoleModel(preferences, providers, nowMs);
  if ("refusal" in result) return result;
  const provider = providers.find((entry) => entry.instanceId === result.pick.instanceId)!;
  const modelSelection: ModelSelection = {
    instanceId: result.pick.instanceId,
    model: result.pick.model,
    ...(result.pick.effort
      ? { options: [{ id: effortOptionId(provider.driver), value: result.pick.effort }] }
      : {}),
  };
  const { prismRole: _prismRole, ...turn } = command;
  return {
    command: {
      ...turn,
      modelSelection,
      bootstrap: { ...command.bootstrap, createThread: { ...createThread, modelSelection } },
    },
  };
}
