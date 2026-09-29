import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { EnvironmentId, ScopedThreadRef } from "@t3tools/contracts";
import { Atom } from "effect/unstable/reactivity";

import { descendantThreads } from "../components/threadDescendants.logic";
import { environmentThreadShells } from "./threads";

const EMPTY: ReadonlyArray<EnvironmentThreadShell> = Object.freeze([]);
const EMPTY_ATOM = Atom.make(EMPTY).pipe(Atom.withLabel("web-thread-descendants:empty"));

/**
 * A thread's descendant shells. Shells change on every thread event; this value changes only when
 * a descendant appears or goes, or its title, branch or pull requests change.
 */
const descendantShellsAtom = Atom.family((key: string) => {
  const { environmentId, threadId } = JSON.parse(key) as {
    readonly environmentId: EnvironmentId;
    readonly threadId: string;
  };
  let previous = EMPTY;
  let previousKey = "[]";
  return Atom.make((get) => {
    const next = descendantThreads(
      get(environmentThreadShells.threadShellsAtom).filter(
        (shell) => shell.environmentId === environmentId,
      ),
      threadId,
    );
    const nextKey = JSON.stringify(
      next.map((shell) => [shell.id, shell.title, shell.branch, shell.pullRequests]),
    );
    if (nextKey === previousKey) return previous;
    previous = next;
    previousKey = nextKey;
    return next;
  }).pipe(Atom.withLabel(`web-thread-descendants:${key}`));
});

export function useDescendantThreadShells(
  ref: ScopedThreadRef | null,
): ReadonlyArray<EnvironmentThreadShell> {
  return useAtomValue(
    ref === null
      ? EMPTY_ATOM
      : descendantShellsAtom(
          JSON.stringify({ environmentId: ref.environmentId, threadId: ref.threadId }),
        ),
  );
}
