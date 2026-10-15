import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useCallback, useRef } from "react";

import { mobilePreferencesAtom, updateMobilePreferencesAtom } from "../../state/preferences";

/**
 * Shared persisted shelf state for the compact Home list and iPad sidebar.
 * Refs advance before persistence starts so consecutive presses always toggle
 * the latest value, even if React has not rendered the optimistic patch yet.
 */
export function useThreadListV2ShelfPreferences() {
  const preferencesResult = useAtomValue(mobilePreferencesAtom);
  const savePreferences = useAtomSet(updateMobilePreferencesAtom);
  const loaded = AsyncResult.isSuccess(preferencesResult);
  const noProjectShelfExpanded =
    loaded && preferencesResult.value.threadListNoProjectShelfExpanded === true;
  const snoozedShelfExpanded =
    loaded && preferencesResult.value.threadListSnoozedShelfExpanded === true;
  const settledShelfExpanded =
    loaded && preferencesResult.value.threadListSettledShelfExpanded === true;
  // Working section beta: off until the preference loads and is enabled.
  const workingShelfEnabled = loaded && preferencesResult.value.workingShelfEnabled === true;
  const workingShelfExpanded =
    loaded && preferencesResult.value.threadListWorkingShelfExpanded === true;
  const noProjectShelfExpandedRef = useRef(noProjectShelfExpanded);
  noProjectShelfExpandedRef.current = noProjectShelfExpanded;
  const snoozedShelfExpandedRef = useRef(snoozedShelfExpanded);
  const settledShelfExpandedRef = useRef(settledShelfExpanded);
  const workingShelfExpandedRef = useRef(workingShelfExpanded);
  snoozedShelfExpandedRef.current = snoozedShelfExpanded;
  settledShelfExpandedRef.current = settledShelfExpanded;
  workingShelfExpandedRef.current = workingShelfExpanded;

  const toggleNoProjectShelf = useCallback(() => {
    if (!loaded) return;
    const expanded = !noProjectShelfExpandedRef.current;
    noProjectShelfExpandedRef.current = expanded;
    savePreferences({ threadListNoProjectShelfExpanded: expanded });
  }, [loaded, savePreferences]);
  const toggleSnoozedShelf = useCallback(() => {
    if (!loaded) return;
    const expanded = !snoozedShelfExpandedRef.current;
    snoozedShelfExpandedRef.current = expanded;
    savePreferences({ threadListSnoozedShelfExpanded: expanded });
  }, [loaded, savePreferences]);
  const toggleSettledShelf = useCallback(() => {
    if (!loaded) return;
    const expanded = !settledShelfExpandedRef.current;
    settledShelfExpandedRef.current = expanded;
    savePreferences({ threadListSettledShelfExpanded: expanded });
  }, [loaded, savePreferences]);
  const toggleWorkingShelf = useCallback(() => {
    if (!loaded) return;
    const expanded = !workingShelfExpandedRef.current;
    workingShelfExpandedRef.current = expanded;
    savePreferences({ threadListWorkingShelfExpanded: expanded });
  }, [loaded, savePreferences]);

  return {
    loaded,
    noProjectShelfExpanded,
    toggleNoProjectShelf,
    settledShelfExpanded,
    snoozedShelfExpanded,
    workingShelfEnabled,
    workingShelfExpanded,
    toggleSettledShelf,
    toggleSnoozedShelf,
    toggleWorkingShelf,
  } as const;
}
