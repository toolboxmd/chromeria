import { type CommandId, type RunId, SPECTRUM_WS_METHODS, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { Atom, AtomRegistry } from "effect/reactivity";

import type { EnvironmentRegistry } from "./connection/registry.ts";
import type { InterruptThreadTurnInput } from "./operations/commands.ts";
import { requestGuarded, RpcPermissionGuard } from "./rpc/client.ts";
import { createCommandPermissions } from "./state/commandPermissions.ts";
import type { EnvironmentThreadShell } from "./state/models.ts";

/**
 * Fork (toolboxmd/chromeria#176): Stop on a thread that runs a Spectrum stops
 * the whole thread, Spectrum included, even while no run is active. The server
 * reports a running Spectrum on the thread shell; clients never infer it.
 */

/**
 * Asks the server to stop the thread with its Spectrum. The server checks the
 * Spectrum itself and replays the same `commandId` as the same stop.
 */
export const stopForkSpectrumThread = (threadId: ThreadId, commandId: CommandId) =>
  requestGuarded(SPECTRUM_WS_METHODS.stop, { threadId, commandId });

/**
 * Lets the plain Stop command send the guarded bridge: each guarded request is
 * checked against the target session's grants, as `createEnvironmentRpcCommand`
 * does for its own tag. Without it the default guard refuses every session.
 */
export const withGuardedRequests = <R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  registry: AtomRegistry.AtomRegistry,
) =>
  Effect.provideService(RpcPermissionGuard, {
    authorize: (environmentId, method, payload) =>
      createCommandPermissions(runtime, method).authorize(registry, environmentId, payload),
  });

/** Whether the server reports a Spectrum running on this thread. */
export const isForkSpectrumRunning = (
  shell: Pick<EnvironmentThreadShell, "source"> | null | undefined,
): boolean => shell?.source.forkSpectrumRunning === true;

/**
 * What Stop sends for a thread, or null when there is nothing to stop: the
 * whole thread while a Spectrum runs, otherwise its interruptible run.
 */
export function threadStopInput(
  shell: Pick<EnvironmentThreadShell, "id" | "source">,
  interruptibleRunId: RunId | null,
): InterruptThreadTurnInput | null {
  if (isForkSpectrumRunning(shell)) return { threadId: shell.id, forkSpectrumRunning: true };
  return interruptibleRunId === null ? null : { threadId: shell.id, runId: interruptibleRunId };
}
