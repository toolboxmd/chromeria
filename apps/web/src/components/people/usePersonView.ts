import { useAtomValue } from "@effect/atom-react";
import { DEFAULT_PERSON, type EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Atom } from "effect/reactivity";
import { useCallback, useEffect, useMemo } from "react";

import { useLocalStorage } from "../../hooks/useLocalStorage";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentPresentations } from "../../state/presentation";
import { environmentSession } from "../../state/session";
import {
  draftInPersonView,
  PERSON_VIEWS,
  personViewFor,
  resolveDevicePerson,
  threadInPersonView,
  type PersonView,
  type ThreadOwnership,
} from "./personView";

// A view preference of this client, like Promachos mode. Null follows the
// device's person, so relabelling the device moves the default with it.
const PERSON_VIEW_STORAGE_KEY = "chromeria:person-view";
const StoredPersonView = Schema.NullOr(Schema.Literals(PERSON_VIEWS));

interface DevicePeople {
  readonly device: string;
  readonly byEnvironment: ReadonlyMap<EnvironmentId, string>;
}

function devicePeopleEqual(left: DevicePeople, right: DevicePeople): boolean {
  if (left.device !== right.device || left.byEnvironment.size !== right.byEnvironment.size) {
    return false;
  }
  for (const [environmentId, person] of left.byEnvironment) {
    if (right.byEnvironment.get(environmentId) !== person) return false;
  }
  return true;
}

// The person each environment's session labels this device with. Equality
// keeps one value across unrelated connection and session refreshes, so
// filtered lists only recompute when a label changes.
const devicePeopleAtom = Atom.make((get): DevicePeople => {
  const sessions: Array<{ primary: boolean; person: string | undefined }> = [];
  const byEnvironment = new Map<EnvironmentId, string>();
  for (const [environmentId, environment] of get(environmentPresentations.presentationsAtom)) {
    const person = get(environmentSession.sessionStateValueAtom(environmentId))?.person;
    if (person !== undefined) byEnvironment.set(environmentId, person);
    sessions.push({
      primary: environment.entry.target._tag === "PrimaryConnectionTarget",
      person,
    });
  }
  return { device: resolveDevicePerson(sessions), byEnvironment };
}).pipe(Atom.withEquality(devicePeopleEqual), Atom.withLabel("chromeria:device-people"));

// Focus and visibility both fire when the app comes back; one refetch covers both.
const FOCUS_REFRESH_INTERVAL_MS = 5_000;
let lastFocusRefreshAt = 0;

function refreshDevicePeople() {
  if (document.visibilityState !== "visible") return;
  const now = Date.now();
  if (now - lastFocusRefreshAt < FOCUS_REFRESH_INTERVAL_MS) return;
  lastFocusRefreshAt = now;
  for (const environmentId of appAtomRegistry
    .get(environmentPresentations.presentationsAtom)
    .keys()) {
    appAtomRegistry.refresh(environmentSession.sessionStateAtom(environmentId));
  }
}

/**
 * Refetches this device's session labels when the app regains focus, so a
 * relabel made from another device's Connections shows without a reload.
 * Session state has no live stream; it otherwise refetches only on reconnect.
 */
export function useRefreshDevicePeopleOnFocus() {
  useEffect(() => {
    window.addEventListener("focus", refreshDevicePeople);
    document.addEventListener("visibilitychange", refreshDevicePeople);
    return () => {
      window.removeEventListener("focus", refreshDevicePeople);
      document.removeEventListener("visibilitychange", refreshDevicePeople);
    };
  }, []);
}

/** The person this device acts as on one environment. */
export function useEnvironmentPerson(environmentId: EnvironmentId): string {
  return useAtomValue(devicePeopleAtom).byEnvironment.get(environmentId) ?? DEFAULT_PERSON;
}

export function usePersonView(): [PersonView, (view: PersonView) => void] {
  const [stored, setStored] = useLocalStorage(PERSON_VIEW_STORAGE_KEY, null, StoredPersonView);
  const { device } = useAtomValue(devicePeopleAtom);
  return [stored ?? personViewFor(device), setStored];
}

/** The threads in the current person view, by their own owner and co-owners. */
export function usePersonViewThreads<T extends { readonly source: ThreadOwnership }>(
  threads: ReadonlyArray<T>,
): ReadonlyArray<T> {
  const [view] = usePersonView();
  return useMemo(
    () =>
      view === "All"
        ? threads
        : threads.filter((thread) => threadInPersonView(thread.source, view)),
    [threads, view],
  );
}

/** Whether an unsent draft on an environment belongs in the current person view. */
export function useDraftInPersonView(): (environmentId: EnvironmentId) => boolean {
  const [view] = usePersonView();
  const { byEnvironment } = useAtomValue(devicePeopleAtom);
  return useCallback(
    (environmentId) => draftInPersonView(byEnvironment.get(environmentId) ?? DEFAULT_PERSON, view),
    [byEnvironment, view],
  );
}
