import {
  purposeSelectionOf,
  savedPurposeIntentForScope,
  type PurposeIntent,
} from "@t3tools/contracts";

import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useStableCallback } from "../../hooks/useStableCallback";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentServerConfigsAtom } from "../../state/server";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";

/** Starts a turn, recording the thread's stored purpose when the caller did not choose one. */
export function useStartTurnWithPurpose(
  activeThread: EnvironmentThreadShell | undefined,
  isServerThread: boolean,
) {
  const dispatchThreadTurn = useOrchestrationCommand(threadEnvironment.startTurn, {
    reportFailure: false,
  });
  const selection = purposeSelectionOf(activeThread ?? {});
  return useStableCallback((request: Parameters<typeof dispatchThreadTurn>[0]) => {
    const supportsPurpose =
      appAtomRegistry
        .get(environmentServerConfigsAtom)
        .get(request.environmentId)
        ?.environment.capabilities.executionPurposeVersions?.includes(1) === true;
    if (!supportsPurpose || request.input.purposeIntent !== undefined)
      return dispatchThreadTurn(request);
    const purposeIntent: PurposeIntent =
      request.input.manualContinuationOfRunId !== undefined
        ? { version: 1, intent: { type: "inherit" } }
        : request.input.dispatchMode === "steer" || request.input.dispatchMode === "restart"
          ? { version: 1, intent: { type: "target" } }
          : isServerThread &&
              request.input.threadId === activeThread?.id &&
              request.input.bootstrap?.createThread === undefined
            ? { version: 1, intent: { type: "selection", expectedRevision: selection.revision } }
            : savedPurposeIntentForScope(selection.scope);
    return dispatchThreadTurn({ ...request, input: { ...request.input, purposeIntent } });
  });
}

export type StartTurnWithPurpose = ReturnType<typeof useStartTurnWithPurpose>;
