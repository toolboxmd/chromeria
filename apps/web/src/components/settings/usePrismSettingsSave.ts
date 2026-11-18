import { useEffect } from "react";
import { useStore } from "zustand";
import { useEnvironments } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { prismSaveStore } from "./PrismSettings.state";
import {
  planPrismRolePatch,
  type PrismRoleUpdate,
  type PrismWriteExpectation,
} from "./PrismSettings.logic";
import { persistScopedSettingsPatch } from "./scopedSettings";
import { useSettingsScope } from "./SettingsScopeContext";

/** Both pages share one write barrier so a scope change cannot overwrite an in-flight kit edit. */
export function usePrismSettingsSave(): {
  pendingWrite: ReturnType<typeof prismSaveStore.getState>["pendingWrite"];
  saveError: string | null;
  setSaveError: (message: string | null) => void;
  savePlan: (
    plan: ReturnType<typeof planPrismRolePatch>,
    expectation: PrismWriteExpectation,
  ) => Promise<void>;
  saveRole: (update: PrismRoleUpdate) => Promise<void>;
} {
  const { scope, environments } = useSettingsScope();
  const { pendingWrite, saveError, setSaveError } = useStore(prismSaveStore);
  const { environments: allEnvironments } = useEnvironments();
  const persist = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Settle the pending settings write when server environment observations acknowledge it.
  useEffect(() => {
    if (pendingWrite?.acknowledged) prismSaveStore.getState().observe(allEnvironments);
  }, [allEnvironments, pendingWrite]);
  async function savePlan(
    plan: ReturnType<typeof planPrismRolePatch>,
    expectation: PrismWriteExpectation,
  ) {
    if (plan.unavailableReason) throw new Error(plan.unavailableReason);
    if (!prismSaveStore.getState().begin(plan, expectation)) {
      throw new Error("Wait for the current settings update to finish.");
    }
    let result;
    try {
      result = await persistScopedSettingsPatch(plan, persist, () => {});
    } catch (cause) {
      prismSaveStore
        .getState()
        .fail(cause instanceof Error ? cause.message : "Could not save preferences.");
      throw cause;
    }
    const failed = new Set(result.failedEnvironments.map((env) => env.environmentId));
    prismSaveStore.getState().acknowledge(failed);
    if (failed.size) {
      const message = `Could not save preferences on ${result.failedEnvironments.map((env) => env.label).join(", ")}.${result.savedEnvironmentCount ? " Other selected environments saved the change." : ""}`;
      // The list editor restores its order silently, so the page shows why.
      setSaveError(message);
      throw new Error(message);
    }
  }
  const saveRole = (update: PrismRoleUpdate) =>
    savePlan(planPrismRolePatch(scope, environments, update), update).catch((cause: unknown) => {
      setSaveError(cause instanceof Error ? cause.message : "Could not save preferences.");
      throw cause;
    });
  return { pendingWrite, saveError, setSaveError, savePlan, saveRole };
}
