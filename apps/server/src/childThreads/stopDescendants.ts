import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { subagentDescendants, type RetirementThread } from "./retirement.ts";

type LineageThread = Pick<RetirementThread, "id" | "lineage">;

/** Deleted parents are absent from UI shells but still connect living descendants to Stop. */
export const readStopDescendants = Effect.fn("childThreads.readStopDescendants")(function* <E, R>(
  threadId: ThreadId,
  shells: ReadonlyArray<LineageThread>,
  loadAncestor: (id: ThreadId) => Effect.Effect<Option.Option<LineageThread>, E, R>,
) {
  const graph = new Map(shells.map((thread) => [thread.id, thread]));
  const pending = [...shells];
  const attempted = new Set<ThreadId>();
  for (let index = 0; index < pending.length; index++) {
    const thread = pending[index]!;
    const parentId = thread.lineage.parentThreadId;
    if (
      thread.lineage.relationshipToParent !== "subagent" ||
      parentId === null ||
      parentId === threadId ||
      graph.has(parentId) ||
      attempted.has(parentId)
    )
      continue;
    attempted.add(parentId);
    const parent = yield* loadAncestor(parentId);
    if (Option.isSome(parent)) {
      graph.set(parentId, parent.value);
      pending.push(parent.value);
    }
  }
  const visible = new Set(shells.map((thread) => thread.id));
  return subagentDescendants(threadId, [...graph.values()]).filter((id) => visible.has(id));
});
