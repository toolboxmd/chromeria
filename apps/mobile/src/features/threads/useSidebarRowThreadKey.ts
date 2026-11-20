import { useAtomValue } from "@effect/atom-react";
import { parseScopedThreadKey } from "@t3tools/client-runtime/environment";
import { resolveListedAncestorThreadRef } from "@t3tools/client-runtime/state/threads";
import { Atom } from "effect/reactivity";

import { scopedThreadKey } from "../../lib/scopedEntities";
import { environmentThreadShells } from "../../state/threads";

// Derived per open thread so a shell update re-renders the caller only when its row changes.
const sidebarRowThreadKeyAtom = Atom.family((openThreadKey: string | null) =>
  Atom.make((get) => {
    const openThreadRef = openThreadKey === null ? null : parseScopedThreadKey(openThreadKey);
    if (openThreadRef === null) return openThreadKey;
    const rowRef = resolveListedAncestorThreadRef(
      openThreadRef,
      get(environmentThreadShells.quietThreadShellsAtom),
    );
    return scopedThreadKey(rowRef.environmentId, rowRef.threadId);
  }),
);

/** An open subagent, side chat or delegated task has no row; its listed ancestor's row is selected. */
export function useSidebarRowThreadKey(openThreadKey: string | null): string | null {
  return useAtomValue(sidebarRowThreadKeyAtom(openThreadKey));
}
