import type { ThreadRuntimeSummary } from "@t3tools/client-runtime/state/shell";

import { deriveCanInterruptRunningThread } from "./session-logic";

export { isForkSpectrumRunning } from "@t3tools/client-runtime/spectrum-stop";

/**
 * Fork (toolboxmd/chromeria#176): whether web and desktop offer Stop, by button
 * or keybinding. Upstream offers it for an interruptible run; a thread running
 * a Spectrum offers it too, run or not, and Stop then stops the whole thread.
 */
export function deriveCanStopThread(
  hasActiveThread: boolean,
  runtime: ThreadRuntimeSummary | null,
  forkSpectrumRunning: boolean,
): boolean {
  return (
    deriveCanInterruptRunningThread(hasActiveThread, runtime) ||
    (hasActiveThread && forkSpectrumRunning)
  );
}
