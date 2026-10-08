import { requestGuarded } from "@t3tools/client-runtime/rpc";
import { createEnvironmentRpcCommand } from "@t3tools/client-runtime/state/runtime";
import { ORCHESTRATION_V2_WS_METHODS, type CommandId, type ThreadId } from "@t3tools/contracts";

import { connectionAtomRuntime } from "../../connection/runtime";
import type { ThreadSharingAction } from "./personView";

export type ThreadSharingInput = ThreadSharingAction & {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  /** The acting person. The server replaces it with the session's own label. */
  readonly actor: string;
};

/** Dispatches `thread.share`, `thread.unshare` or `thread.leave` on one environment. */
export const threadSharingCommand = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:commands:thread:sharing",
  tag: ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
  execute: (input: ThreadSharingInput) =>
    requestGuarded(ORCHESTRATION_V2_WS_METHODS.dispatchCommand, input),
});
