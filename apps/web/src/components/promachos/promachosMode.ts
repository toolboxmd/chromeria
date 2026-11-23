/**
 * Promachos mode: a chat-first view of the Promachos home's conversations, switched per client. Each
 * environment names its own home in its server settings (Settings → Promachos); the home spans every
 * project the sidebar merges with one, so the Promachos runs on any machine.
 */
import type { ScopedProjectRef } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { promachosMultipleModelSelections } from "@t3tools/client-runtime/promachos/start";

import { useLocalStorage } from "../../hooks/useLocalStorage";
import { useNewThreadHandler } from "../../hooks/useHandleNewThread";
import { useClientSettings } from "../../hooks/useSettings";
import { selectProjectGroupingSettings } from "@t3tools/client-runtime/state/project-grouping";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { useProjects, useServerConfigs } from "../../state/entities";
import {
  isInPromachosHome,
  promachosHomeRefs,
  promachosHomes,
} from "@t3tools/client-runtime/promachos/conversations";

const PROMACHOS_MODE_STORAGE_KEY = "chromeria:promachos-mode";

export function usePromachosMode(): [boolean, (enabled: boolean) => void] {
  const [enabled, setEnabled] = useLocalStorage(PROMACHOS_MODE_STORAGE_KEY, false, Schema.Boolean);
  return [enabled, setEnabled];
}

/**
 * The Promachos home as the connected environments name it: `start` is where a new conversation
 * opens, null when no environment names a home this client knows, and `refs` every project of it.
 * `active: false` skips the work for callers outside Promachos mode.
 */
export function usePromachosHome(active = true): {
  readonly start: ScopedProjectRef | null;
  readonly refs: ReadonlyArray<ScopedProjectRef>;
} {
  const projects = useProjects();
  const configs = useServerConfigs();
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const groupingSettings = useClientSettings(selectProjectGroupingSettings);
  const homes = active ? promachosHomes(configs, projects, primaryEnvironmentId) : [];
  return {
    start: homes[0] ?? null,
    refs: promachosHomeRefs(projects, homes, groupingSettings),
  };
}

/** Whether a thread in `projectRef` renders as a Promachos chat right now. */
function usePromachosChat(projectRef: ScopedProjectRef | null): boolean {
  const [enabled] = usePromachosMode();
  const { refs } = usePromachosHome(enabled);
  return enabled && projectRef !== null && isInPromachosHome(projectRef, refs);
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
