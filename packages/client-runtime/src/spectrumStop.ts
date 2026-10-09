import { type CommandId, type RunId, SPECTRUM_WS_METHODS, type ThreadId } from "@t3tools/contracts";

import type { InterruptThreadTurnInput } from "./operations/commands.ts";
import { request } from "./rpc/client.ts";
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
  request(SPECTRUM_WS_METHODS.stop, { threadId, commandId });

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
