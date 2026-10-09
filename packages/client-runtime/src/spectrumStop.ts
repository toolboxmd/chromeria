import type { RunId } from "@t3tools/contracts";

import type { InterruptThreadTurnInput } from "./operations/commands.ts";
import type { EnvironmentThreadShell } from "./state/models.ts";

/**
 * Fork (toolboxmd/chromeria#176): Stop on a thread that runs a Spectrum stops
 * the whole thread, Spectrum included, even while no run is active. The server
 * reports a running Spectrum on the thread shell; clients never infer it.
 */

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
