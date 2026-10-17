import { type OrchestrationV2Subagent, type ThreadId, WS_METHODS } from "@t3tools/contracts";
import type * as Atom from "effect/reactivity/Atom";
import type * as EnvironmentRegistry from "../connection/registry.ts";
import { createEnvironmentRpcCommand, createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";

/** Queries remain on the parent's authenticated environment; no destination profile is needed. */
export function createRemoteTaskState<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, E>,
) {
  const view = createEnvironmentRpcQueryAtomFamily(runtime, {
    label: "remote-tasks:view",
    tag: WS_METHODS.remoteTaskView,
  });
  const stop = createEnvironmentRpcCommand(runtime, {
    label: "remote-tasks:stop",
    tag: WS_METHODS.remoteTaskStop,
  });
  return { view, stop };
}

/** Imported jobs keep direct navigation; only acknowledged source-owned support selects this viewer. */
export function hasSourceRemoteTaskView(
  task: Pick<OrchestrationV2Subagent, "remoteChild"> | undefined,
): task is {
  remoteChild: NonNullable<OrchestrationV2Subagent["remoteChild"]> & { threadId: ThreadId };
} {
  return (
    task?.remoteChild?.threadId !== undefined && task.remoteChild.launch?.lifecycleVersion === 1
  );
}
