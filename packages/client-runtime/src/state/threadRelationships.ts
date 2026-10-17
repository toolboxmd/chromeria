import type {
  EnvironmentId,
  OrchestrationV2Subagent,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadShell,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export type ThreadRelationshipKind = "parent" | "fork" | "subagent" | "transfer";

export interface ThreadRelationshipNode {
  readonly threadId: ThreadId;
  readonly thread: OrchestrationV2ThreadShell | null;
  readonly missing: boolean;
  /** The linked environment this thread runs in, when it is not the local one. */
  readonly environmentId: EnvironmentId | null;
}

export interface ThreadRelationshipEdge {
  readonly sourceThreadId: ThreadId;
  /** Set when the source runs in a linked environment instead of locally. */
  readonly sourceEnvironmentId: EnvironmentId | null;
  readonly targetThreadId: ThreadId;
  readonly kind: ThreadRelationshipKind;
  readonly status: string | null;
  /** Set when the target runs in a linked environment instead of locally. */
  readonly targetEnvironmentId: EnvironmentId | null;
}

/**
 * Identity of one graph node. Thread ids are only unique inside one
 * environment (a copied database replays the same ids on another machine),
 * so a remote thread is keyed by its environment as well. A local thread
 * keys by its bare thread id.
 */
export function threadRelationshipNodeKey(
  threadId: ThreadId,
  environmentId: EnvironmentId | null,
): string {
  return environmentId === null ? `${threadId}` : `${environmentId}\u001f${threadId}`;
}

export function getThreadRelationshipNode(
  graph: ThreadRelationshipGraph,
  threadId: ThreadId,
  environmentId: EnvironmentId | null = null,
): ThreadRelationshipNode | undefined {
  return graph.nodes.get(threadRelationshipNodeKey(threadId, environmentId));
}

export interface SubagentChildTarget {
  readonly threadId: ThreadId;
  /** The linked environment the child runs in; null for a local child thread. */
  readonly environmentId: EnvironmentId | null;
}

/**
 * Where a subagent's work can be opened. A task delegated to a linked
 * environment has no local `childThreadId`; its thread lives in
 * `remoteChild` once the launch there is attached. Before that there is
 * nothing to open yet.
 */
export function subagentChildTarget(
  subagent: Pick<OrchestrationV2Subagent, "childThreadId" | "remoteChild">,
): SubagentChildTarget | null {
  if (subagent.childThreadId !== null) {
    return { threadId: subagent.childThreadId, environmentId: null };
  }
  const remoteChild = subagent.remoteChild;
  if (remoteChild?.threadId === undefined) return null;
  return { threadId: remoteChild.threadId, environmentId: remoteChild.environmentId };
}

export interface ThreadRelationshipGraph {
  /** Keyed by `threadRelationshipNodeKey`, not by bare thread id. */
  readonly nodes: ReadonlyMap<string, ThreadRelationshipNode>;
  readonly edges: ReadonlyArray<ThreadRelationshipEdge>;
}

export interface ThreadRelationshipWalkRow {
  readonly threadId: ThreadId;
  /** The linked environment this related thread runs in; null when local. */
  readonly environmentId: EnvironmentId | null;
  readonly fromThreadId: ThreadId;
  readonly depth: number;
  readonly edge: ThreadRelationshipEdge;
}

export function resolveMergeBackTargetThreadId(
  projection: Pick<OrchestrationV2ThreadProjection, "thread"> | null,
): ThreadId | null {
  if (projection?.thread.lineage.relationshipToParent !== "fork") return null;
  return projection.thread.forkedFrom?.type === "run"
    ? projection.thread.forkedFrom.threadId
    : projection.thread.lineage.parentThreadId;
}

function edgeKey(edge: ThreadRelationshipEdge): string {
  return `${threadRelationshipNodeKey(edge.sourceThreadId, edge.sourceEnvironmentId)}\u001f${threadRelationshipNodeKey(edge.targetThreadId, edge.targetEnvironmentId)}\u001f${edge.kind}`;
}

/** A shell, or a shell tagged with the linked environment it came from. */
export type ThreadRelationshipShellInput =
  | OrchestrationV2ThreadShell
  | {
      readonly shell: OrchestrationV2ThreadShell;
      readonly environmentId: EnvironmentId;
    };

function scopedShell(entry: ThreadRelationshipShellInput): {
  readonly shell: OrchestrationV2ThreadShell;
  readonly environmentId: EnvironmentId | null;
} {
  return "shell" in entry ? entry : { shell: entry, environmentId: null };
}

export function deriveThreadRelationshipGraph(input: {
  readonly threads: ReadonlyArray<ThreadRelationshipShellInput>;
  readonly projection: OrchestrationV2ThreadProjection | null;
}): ThreadRelationshipGraph {
  const threadsByKey = new Map<
    string,
    { readonly shell: OrchestrationV2ThreadShell; readonly environmentId: EnvironmentId | null }
  >();
  for (const entry of input.threads) {
    const scoped = scopedShell(entry);
    const key = threadRelationshipNodeKey(scoped.shell.id, scoped.environmentId);
    // Callers order shells from most to least authoritative. In particular,
    // live shells precede archived snapshots, which may still contain a stale
    // copy during archive refresh.
    if (!threadsByKey.has(key)) {
      threadsByKey.set(key, scoped);
    }
  }
  const threads = [...threadsByKey.values()];
  const nodes = new Map<string, ThreadRelationshipNode>(
    threads.map(({ shell, environmentId }) => [
      threadRelationshipNodeKey(shell.id, environmentId),
      { threadId: shell.id, thread: shell, missing: false, environmentId },
    ]),
  );
  const edgesByKey = new Map<string, ThreadRelationshipEdge>();
  const ensureNode = (threadId: ThreadId, environmentId: EnvironmentId | null = null) => {
    const key = threadRelationshipNodeKey(threadId, environmentId);
    if (!nodes.has(key)) {
      nodes.set(key, { threadId, thread: null, missing: true, environmentId });
    }
  };
  const addEdge = (edge: ThreadRelationshipEdge) => {
    ensureNode(edge.sourceThreadId, edge.sourceEnvironmentId);
    ensureNode(edge.targetThreadId, edge.targetEnvironmentId);
    edgesByKey.set(
      edgeKey(edge),
      edge.kind === "subagent" && edge.status === "rolled_back"
        ? { ...edge, status: "interrupted" }
        : edge,
    );
  };

  for (const { shell: thread, environmentId } of threads) {
    const parentThreadId =
      thread.forkedFrom?.type === "run"
        ? thread.forkedFrom.threadId
        : thread.lineage.parentThreadId;
    if (parentThreadId === null) continue;
    addEdge({
      sourceThreadId: parentThreadId,
      // Lineage is recorded inside one environment, so the parent of a
      // scoped shell lives in that same environment.
      sourceEnvironmentId: environmentId,
      targetThreadId: thread.id,
      kind: thread.lineage.relationshipToParent === "subagent" ? "subagent" : "fork",
      status: thread.activityRunStatus ?? thread.status,
      targetEnvironmentId: environmentId,
    });
  }

  if (input.projection !== null) {
    const ownerThreadId = input.projection.thread.id;
    for (const subagent of input.projection.subagents) {
      const target = subagentChildTarget(subagent);
      if (target === null) continue;
      const child = threadsByKey.get(
        threadRelationshipNodeKey(target.threadId, target.environmentId),
      )?.shell;
      // The row describes the child conversation, including queued follow-ups
      // and their outcomes. A child without an app run still uses its task.
      addEdge({
        sourceThreadId: ownerThreadId,
        sourceEnvironmentId: null,
        targetThreadId: target.threadId,
        kind: "subagent",
        status:
          child?.activityRunStatus ??
          (child?.latestRunId != null
            ? child.status
            : subagent.remoteChild?.snapshot !== undefined &&
                subagent.remoteChild.snapshot.workState !== "result_available"
              ? "running"
              : (subagent.remoteChild?.latestResult?.status ?? subagent.status)),
        targetEnvironmentId: target.environmentId,
      });
    }
    for (const transfer of input.projection.contextTransfers) {
      if (transfer.sourceThreadId === transfer.targetThreadId) continue;
      addEdge({
        sourceThreadId: transfer.sourceThreadId,
        sourceEnvironmentId: null,
        targetThreadId: transfer.targetThreadId,
        kind: "transfer",
        status: transfer.status,
        targetEnvironmentId: null,
      });
    }
  }

  return { nodes, edges: [...edgesByKey.values()] };
}

export function relatedThreadIds(
  graph: ThreadRelationshipGraph,
  threadId: ThreadId,
): ReadonlyArray<ThreadId> {
  const ids = new Set<ThreadId>();
  for (const edge of graph.edges) {
    if (edge.sourceThreadId === threadId) ids.add(edge.targetThreadId);
    if (edge.targetThreadId === threadId) ids.add(edge.sourceThreadId);
  }
  return [...ids];
}

function relatedNodeRef(
  edge: ThreadRelationshipEdge,
  currentThreadId: ThreadId,
  currentEnvironmentId: EnvironmentId | null,
): { readonly threadId: ThreadId; readonly environmentId: EnvironmentId | null } | null {
  if (
    edge.sourceThreadId === currentThreadId &&
    edge.sourceEnvironmentId === currentEnvironmentId
  ) {
    return { threadId: edge.targetThreadId, environmentId: edge.targetEnvironmentId };
  }
  if (
    edge.targetThreadId === currentThreadId &&
    edge.targetEnvironmentId === currentEnvironmentId
  ) {
    return { threadId: edge.sourceThreadId, environmentId: edge.sourceEnvironmentId };
  }
  return null;
}

export function walkThreadRelationships(
  graph: ThreadRelationshipGraph,
  threadId: ThreadId,
): ReadonlyArray<ThreadRelationshipWalkRow> {
  const visited = new Set<string>([threadRelationshipNodeKey(threadId, null)]);
  const pending: Array<{
    readonly threadId: ThreadId;
    readonly environmentId: EnvironmentId | null;
    readonly depth: number;
  }> = [{ threadId, environmentId: null, depth: 0 }];
  const rows: ThreadRelationshipWalkRow[] = [];

  for (let index = 0; index < pending.length; index += 1) {
    const current = pending[index];
    if (current === undefined) continue;
    for (const edge of graph.edges) {
      const related = relatedNodeRef(edge, current.threadId, current.environmentId);
      if (related === null) continue;
      const relatedKey = threadRelationshipNodeKey(related.threadId, related.environmentId);
      if (visited.has(relatedKey)) continue;
      visited.add(relatedKey);
      const depth = current.depth + 1;
      rows.push({
        threadId: related.threadId,
        environmentId: related.environmentId,
        fromThreadId: current.threadId,
        depth,
        edge,
      });
      pending.push({ threadId: related.threadId, environmentId: related.environmentId, depth });
    }
  }

  return rows;
}

export function immediateThreadRelationships(
  graph: ThreadRelationshipGraph,
  threadId: ThreadId,
): ReadonlyArray<ThreadRelationshipWalkRow> {
  const visited = new Set<string>();
  const rows: ThreadRelationshipWalkRow[] = [];

  for (const edge of graph.edges) {
    const related = relatedNodeRef(edge, threadId, null);
    if (related === null) continue;
    const relatedKey = threadRelationshipNodeKey(related.threadId, related.environmentId);
    if (visited.has(relatedKey)) continue;
    visited.add(relatedKey);
    rows.push({
      threadId: related.threadId,
      environmentId: related.environmentId,
      fromThreadId: threadId,
      depth: 1,
      edge,
    });
  }

  return rows;
}

export function isParentThreadRelationship(
  edge: ThreadRelationshipEdge,
  currentThreadId: ThreadId,
): boolean {
  // The current thread is the graph's local root, so a same-id child in a
  // linked environment (target environment set) is not its parent edge:
  // bare id equality would misclassify that child as the parent.
  return (
    edge.kind !== "transfer" &&
    edge.targetThreadId === currentThreadId &&
    edge.targetEnvironmentId === null
  );
}

export function threadRelationshipRowStatus(
  graph: ThreadRelationshipGraph,
  row: Pick<ThreadRelationshipWalkRow, "threadId" | "environmentId" | "edge">,
): string | null {
  if (
    row.edge.kind === "transfer" ||
    (row.threadId === row.edge.targetThreadId && row.environmentId === row.edge.targetEnvironmentId)
  ) {
    return row.edge.status;
  }
  const thread = getThreadRelationshipNode(graph, row.threadId, row.environmentId)?.thread;
  return thread?.activityRunStatus ?? thread?.status ?? null;
}

function threadCreatedAtMillis(node: ThreadRelationshipNode | undefined): number | null {
  const createdAt: unknown = node?.thread?.createdAt;
  if (!DateTime.isDateTime(createdAt)) return null;
  const millis = DateTime.toEpochMillis(createdAt);
  return Number.isFinite(millis) ? millis : null;
}

/**
 * Web-specific by design: the panel pins the parent row first and a distinct
 * merge-back target second so their actions stay where the user expects, and
 * only then falls back to newest-created-first. Mobile does not share that
 * exception, so this is not the canonical relationship order and should not be
 * reused as one.
 */
export function orderWebThreadLineageRows(input: {
  readonly graph: ThreadRelationshipGraph;
  readonly rows: ReadonlyArray<ThreadRelationshipWalkRow>;
  readonly currentThreadId: ThreadId;
  readonly mergeTargetThreadId: ThreadId | null;
}): ReadonlyArray<ThreadRelationshipWalkRow> {
  const pinRank = (row: ThreadRelationshipWalkRow): number => {
    if (isParentThreadRelationship(row.edge, input.currentThreadId)) return 0;
    // The merge target is a local thread; a same-id child in a linked
    // environment must not inherit its pin.
    if (row.threadId === input.mergeTargetThreadId && row.environmentId === null) return 1;
    return 2;
  };

  return [...input.rows].sort((left, right) => {
    const rankDelta = pinRank(left) - pinRank(right);
    if (rankDelta !== 0) return rankDelta;
    const leftCreatedAt = threadCreatedAtMillis(
      getThreadRelationshipNode(input.graph, left.threadId, left.environmentId),
    );
    const rightCreatedAt = threadCreatedAtMillis(
      getThreadRelationshipNode(input.graph, right.threadId, right.environmentId),
    );
    if (leftCreatedAt !== rightCreatedAt) {
      if (leftCreatedAt === null) return 1;
      if (rightCreatedAt === null) return -1;
      return rightCreatedAt - leftCreatedAt;
    }
    if (left.threadId !== right.threadId) return left.threadId < right.threadId ? -1 : 1;
    const leftEnv = left.environmentId ?? "";
    const rightEnv = right.environmentId ?? "";
    return leftEnv < rightEnv ? -1 : leftEnv > rightEnv ? 1 : 0;
  });
}
