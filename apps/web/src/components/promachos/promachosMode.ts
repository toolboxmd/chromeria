/**
 * Promachos mode (toolboxmd/chromeria#116): a chat-first view of the
 * conversations in the Promachos home, switched per client.
 *
 * Both values live in this client's local storage: the mode is a view
 * preference, and the home names one project on one environment, so the same
 * project id on two environments never collides.
 */
import { ScopedProjectRef } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { promachosMultipleModelSelections } from "./promachosStart";

import { useLocalStorage } from "../../hooks/useLocalStorage";
import { useNewThreadHandler } from "../../hooks/useHandleNewThread";

const PROMACHOS_MODE_STORAGE_KEY = "chromeria:promachos-mode";
const PROMACHOS_HOME_STORAGE_KEY = "chromeria:promachos-home";
const PromachosHome = Schema.NullOr(ScopedProjectRef);

export function usePromachosMode(): [boolean, (enabled: boolean) => void] {
  const [enabled, setEnabled] = useLocalStorage(PROMACHOS_MODE_STORAGE_KEY, false, Schema.Boolean);
  return [enabled, setEnabled];
}

export function usePromachosHome(): [
  ScopedProjectRef | null,
  (home: ScopedProjectRef | null) => void,
] {
  const [home, setHome] = useLocalStorage(PROMACHOS_HOME_STORAGE_KEY, null, PromachosHome);
  return [home, setHome];
}

function isPromachosHome(
  projectRef: ScopedProjectRef | null,
  home: ScopedProjectRef | null,
): boolean {
  return (
    projectRef !== null &&
    home !== null &&
    projectRef.environmentId === home.environmentId &&
    projectRef.projectId === home.projectId
  );
}

/** Whether a thread in `projectRef` renders as a Promachos chat right now. */
function usePromachosChat(projectRef: ScopedProjectRef | null): boolean {
  const [enabled] = usePromachosMode();
  const [home] = usePromachosHome();
  return enabled && isPromachosHome(projectRef, home);
}

/**
 * Opens a new Promachos conversation in the home. Its first send starts on
 * the Promachos's Prism role when that role has a model list
 * (`withPromachosStart`, toolboxmd/chromeria#115).
 */
export function useStartPromachosConversation() {
  const startNewThread = useNewThreadHandler();
  return (home: ScopedProjectRef) => startNewThread(home);
}

/** Keep home/model and saved fanout presentation policy behind one chat-view hook. */
export function usePromachosPresentation<T>(
  projectRef: ScopedProjectRef | null,
  configuredModels: ReadonlyArray<unknown>,
  savedMultipleModels: T | null,
) {
  const enabled = usePromachosChat(projectRef);
  const models = enabled ? configuredModels : null;
  return {
    enabled,
    models,
    multipleModels: promachosMultipleModelSelections(savedMultipleModels, models),
  };
}
