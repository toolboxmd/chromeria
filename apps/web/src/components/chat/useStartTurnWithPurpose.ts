import { purposeSelectionOf, type PurposeIntent } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";

import { appAtomRegistry } from "../../rpc/atomRegistry";
import { readThreadShell } from "../../state/entities";
import { environmentServerConfigsAtom } from "../../state/server";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";

interface TurnPurposeInput {
  readonly dispatchMode?: string | undefined;
  readonly manualContinuationOfRunId?: unknown;
  readonly bootstrap?: { readonly createThread?: unknown } | undefined;
}

/**
 * Purpose for a turn the user did not scope: continuations inherit, Steer/Restart keep the target
 * run, new threads start unscoped, and existing threads use their own stored selection. A thread
 * the client has not loaded stays legacy so its stored selection is never overwritten.
 */
export function purposeIntentForTurn(
  input: TurnPurposeInput,
  destination: Pick<EnvironmentThreadShell, "purposeSelection"> | null,
): PurposeIntent | undefined {
  if (input.manualContinuationOfRunId !== undefined)
    return { version: 1, intent: { type: "inherit" } };
  if (input.dispatchMode === "steer" || input.dispatchMode === "restart")
    return { version: 1, intent: { type: "target" } };
  if (input.bootstrap?.createThread !== undefined)
    return { version: 1, intent: { type: "unscoped" } };
  if (destination === null) return undefined;
  return {
    version: 1,
    intent: { type: "selection", expectedRevision: purposeSelectionOf(destination).revision },
  };
}

/** Starts a turn, recording the destination thread's purpose when the caller did not choose one. */
export function useStartTurnWithPurpose() {
  const dispatchThreadTurn = useOrchestrationCommand(threadEnvironment.startTurn, {
    reportFailure: false,
  });
  return (request: Parameters<typeof dispatchThreadTurn>[0]) => {
    const supportsPurpose =
      appAtomRegistry
        .get(environmentServerConfigsAtom)
        .get(request.environmentId)
        ?.environment.capabilities.executionPurposeVersions?.includes(1) === true;
    if (!supportsPurpose || request.input.purposeIntent !== undefined)
      return dispatchThreadTurn(request);
    const purposeIntent = purposeIntentForTurn(
      request.input,
      readThreadShell(scopeThreadRef(request.environmentId, request.input.threadId)),
    );
    return dispatchThreadTurn(
      purposeIntent === undefined
        ? request
        : { ...request, input: { ...request.input, purposeIntent } },
    );
  };
}
