import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  EnvironmentId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { useThreadRelationshipShells } from "./entities";

vi.mock("@effect/atom-react", () => ({
  useAtomValue: (atom: Atom.Atom<unknown>) => registry.get(atom),
}));
vi.mock("./threads", () => ({
  environmentThreadDetails: { threadAtom: () => projectionAtom },
  environmentThreadShells: { environmentThreadsAtom: (id: EnvironmentId) => shellsAtom(id) },
}));

const registry = AtomRegistry.make();
const projectionAtom = Atom.make<{ readonly projection: OrchestrationV2ThreadProjection } | null>(
  null,
);
const shellsAtom = Atom.family((_id: EnvironmentId) =>
  Atom.make<ReadonlyArray<OrchestrationV2ThreadShell>>([]),
);
const local = EnvironmentId.make("local");
const remote = EnvironmentId.make("remote");
const ref = scopeThreadRef(local, ThreadId.make("parent"));
const shell = (id: string, title = id) =>
  ({
    id: ThreadId.make(id),
    title,
    forkedFrom: null,
    lineage: { parentThreadId: null, relationshipToParent: null },
  }) as OrchestrationV2ThreadShell;

afterEach(() => registry.reset());

describe("remote relationship shell subscription", () => {
  it("uses the named destination's shell even when the thread ID also exists locally", () => {
    const parent = shell("parent");
    registry.set(shellsAtom(local), [parent, shell("child", "Local collision")]);
    registry.set(shellsAtom(remote), [shell("child", "Remote child"), shell("unrelated")]);
    registry.set(projectionAtom, {
      projection: {
        thread: parent,
        subagents: [
          {
            childThreadId: null,
            remoteChild: { environmentId: remote, threadId: ThreadId.make("child") },
          },
        ],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection,
    });

    const first = useThreadRelationshipShells(ref);
    expect(first).toEqual([
      parent,
      { environmentId: remote, shell: expect.objectContaining({ title: "Remote child" }) },
    ]);

    registry.set(shellsAtom(remote), [shell("child", "Renamed remote child")]);
    expect(useThreadRelationshipShells(ref)).toEqual([
      parent,
      { environmentId: remote, shell: expect.objectContaining({ title: "Renamed remote child" }) },
    ]);
    registry.set(shellsAtom(remote), []);
    expect(useThreadRelationshipShells(ref)).toEqual([parent]);
  });

  it("keeps identity across unrelated streaming updates and removes retired remote children", () => {
    const parent = shell("parent");
    const child = shell("child");
    registry.set(shellsAtom(local), [parent, shell("local-unrelated")]);
    registry.set(shellsAtom(remote), [child, shell("remote-unrelated")]);
    const projection = {
      thread: parent,
      subagents: [
        {
          childThreadId: null,
          remoteChild: { environmentId: remote, threadId: ThreadId.make("child") },
        },
      ],
      contextTransfers: [],
    } as unknown as OrchestrationV2ThreadProjection;
    registry.set(projectionAtom, { projection });
    const first = useThreadRelationshipShells(ref);

    registry.set(shellsAtom(local), [parent, shell("local-unrelated", "Streaming locally")]);
    registry.set(shellsAtom(remote), [child, shell("remote-unrelated", "Streaming remotely")]);
    expect(useThreadRelationshipShells(ref)).toBe(first);

    registry.set(projectionAtom, { projection: { ...projection, subagents: [] } });
    expect(useThreadRelationshipShells(ref)).toEqual([parent]);
  });
});
