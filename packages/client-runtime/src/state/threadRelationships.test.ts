import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import {
  deriveThreadRelationshipGraph,
  getThreadRelationshipNode,
  immediateThreadRelationships,
  isParentThreadRelationship,
  orderWebThreadLineageRows,
  resolveMergeBackTargetThreadId,
  threadRelationshipRowStatus,
} from "./threadRelationships.ts";

describe("thread relationships", () => {
  it.each([
    ["running", "completed"],
    ["completed", "running"],
  ])("keeps parent %s independent of child %s", (parentStatus, childStatus) => {
    const parent = ThreadId.make("parent");
    const child = ThreadId.make("child");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        {
          id: parent,
          activityRunStatus: parentStatus,
          status: "cancelled",
          lineage: { parentThreadId: null },
        },
        {
          id: child,
          activityRunStatus: childStatus,
          lineage: { parentThreadId: parent, relationshipToParent: "fork" },
        },
      ] as never,
      projection: null,
    });
    expect(threadRelationshipRowStatus(graph, immediateThreadRelationships(graph, child)[0]!)).toBe(
      parentStatus,
    );
    expect(
      threadRelationshipRowStatus(graph, immediateThreadRelationships(graph, parent)[0]!),
    ).toBe(childStatus);
  });

  it("does not label a missing parent with its child's running status", () => {
    const parent = ThreadId.make("missing-parent");
    const child = ThreadId.make("child");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        {
          id: child,
          status: "running",
          lineage: { parentThreadId: parent, relationshipToParent: "subagent" },
        },
      ] as never,
      projection: null,
    });
    expect(
      threadRelationshipRowStatus(graph, immediateThreadRelationships(graph, child)[0]!),
    ).toBeNull();
  });

  it("keeps an older activity run visible over a newer cancelled run", () => {
    const parent = ThreadId.make("thread-parent");
    const child = ThreadId.make("thread-child");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        {
          id: child,
          title: "Active child",
          activityRunStatus: "running",
          status: "cancelled",
          forkedFrom: { type: "run", threadId: parent, runId: "run-parent" },
          lineage: {
            rootThreadId: parent,
            parentThreadId: parent,
            relationshipToParent: "subagent",
          },
        },
      ] as never,
      projection: null,
    });

    expect(graph.edges).toEqual([
      expect.objectContaining({ targetThreadId: child, status: "running" }),
    ]);
  });

  it("keeps missing parents and cycles navigable without recursive traversal", () => {
    const root = ThreadId.make("thread-root");
    const child = ThreadId.make("thread-child");
    const missing = ThreadId.make("thread-missing");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        {
          id: root,
          title: "Root",
          status: "completed",
          forkedFrom: { type: "run", threadId: child, runId: "run-cycle" },
          lineage: { rootThreadId: root, parentThreadId: child, relationshipToParent: "fork" },
        },
        {
          id: child,
          title: "Child",
          status: "completed",
          forkedFrom: { type: "run", threadId: missing, runId: "run-missing" },
          lineage: { rootThreadId: root, parentThreadId: missing, relationshipToParent: "fork" },
        },
      ] as never,
      projection: null,
    });

    expect(graph.nodes.get(missing)?.missing).toBe(true);
    expect(immediateThreadRelationships(graph, root).map(({ threadId }) => threadId)).toEqual([
      child,
    ]);
  });

  it("combines subagent and transfer edges with archived shell state", () => {
    const parent = ThreadId.make("thread-parent");
    const child = ThreadId.make("thread-child");
    const transferTarget = ThreadId.make("thread-transfer");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        {
          id: parent,
          title: "Parent",
          status: "completed",
          archivedAt: null,
          forkedFrom: null,
          lineage: { rootThreadId: parent, parentThreadId: null, relationshipToParent: null },
        },
        {
          id: child,
          title: "Subagent",
          status: "completed",
          archivedAt: "2026-06-24T00:00:00.000Z",
          forkedFrom: null,
          lineage: {
            rootThreadId: parent,
            parentThreadId: parent,
            relationshipToParent: "subagent",
          },
        },
      ] as never,
      projection: {
        thread: { id: parent },
        subagents: [{ childThreadId: child, status: "completed" }],
        contextTransfers: [
          {
            sourceThreadId: child,
            targetThreadId: transferTarget,
            status: "completed",
          },
        ],
      } as never,
    });

    expect(graph.nodes.get(child)?.thread?.archivedAt).not.toBeNull();
    expect(graph.nodes.get(transferTarget)?.missing).toBe(true);
    expect(graph.edges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceThreadId: parent,
          targetThreadId: child,
          kind: "subagent",
        }),
        expect.objectContaining({
          sourceThreadId: child,
          targetThreadId: transferTarget,
          kind: "transfer",
        }),
      ]),
    );
  });

  it("links a remote delegated child through remoteChild, not childThreadId", () => {
    const parent = ThreadId.make("thread-parent");
    const child = ThreadId.make("thread-remote-child");
    const remoteEnvironment = EnvironmentId.make("environment-cavallo");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        { id: parent, status: "running", forkedFrom: null, lineage: { parentThreadId: null } },
      ] as never,
      projection: {
        thread: { id: parent },
        subagents: [
          {
            childThreadId: null,
            remoteChild: {
              environmentId: remoteEnvironment,
              threadId: child,
              label: "cavallo",
            },
            status: "running",
          },
        ],
        contextTransfers: [],
      } as never,
    });

    expect(graph.edges).toEqual([
      expect.objectContaining({
        sourceThreadId: parent,
        targetThreadId: child,
        kind: "subagent",
        status: "running",
        targetEnvironmentId: remoteEnvironment,
      }),
    ]);
    expect(getThreadRelationshipNode(graph, child, remoteEnvironment)?.environmentId).toBe(
      remoteEnvironment,
    );
    expect(getThreadRelationshipNode(graph, child)).toBeUndefined();
    const row = immediateThreadRelationships(graph, parent)[0]!;
    expect(row.environmentId).toBe(remoteEnvironment);
    expect(threadRelationshipRowStatus(graph, row)).toBe("running");
  });

  it.each([
    ["running", "running"],
    ["rolled_back", "interrupted"],
  ])(
    "shows a remote child shell activity %s as %s after its delegated task settled",
    (childActivity, expected) => {
      const parent = ThreadId.make("thread-parent");
      const child = ThreadId.make("thread-remote-child");
      const remoteEnvironment = EnvironmentId.make("environment-cavallo");
      const graph = deriveThreadRelationshipGraph({
        threads: [
          { id: parent, status: "running", forkedFrom: null, lineage: { parentThreadId: null } },
          {
            shell: {
              id: child,
              title: "Remote child",
              status: "completed",
              activityRunStatus: childActivity,
              forkedFrom: null,
              lineage: { parentThreadId: null, relationshipToParent: null },
            },
            environmentId: remoteEnvironment,
          },
        ] as never,
        projection: {
          thread: { id: parent },
          subagents: [
            {
              childThreadId: null,
              remoteChild: {
                environmentId: remoteEnvironment,
                threadId: child,
                label: "cavallo",
              },
              status: "completed",
            },
          ],
          contextTransfers: [],
        } as never,
      });

      const row = immediateThreadRelationships(graph, parent)[0]!;
      expect(threadRelationshipRowStatus(graph, row)).toBe(expected);
      expect(getThreadRelationshipNode(graph, child, remoteEnvironment)?.thread?.title).toBe(
        "Remote child",
      );
    },
  );

  it("keeps same-id threads in different environments as separate rows", () => {
    const parent = ThreadId.make("thread-parent");
    const shared = ThreadId.make("thread-shared");
    const cavallo = EnvironmentId.make("environment-cavallo");
    const bestia = EnvironmentId.make("environment-bestia");
    const remoteShell = (environmentId: EnvironmentId, title: string, status: string) => ({
      shell: {
        id: shared,
        title,
        status,
        activityRunStatus: status,
        forkedFrom: null,
        lineage: { parentThreadId: null, relationshipToParent: null },
      },
      environmentId,
    });
    const graph = deriveThreadRelationshipGraph({
      threads: [
        { id: parent, status: "running", forkedFrom: null, lineage: { parentThreadId: null } },
        remoteShell(cavallo, "Cavallo child", "running"),
        remoteShell(bestia, "Bestia child", "completed"),
      ] as never,
      projection: {
        thread: { id: parent },
        subagents: [
          {
            childThreadId: null,
            remoteChild: { environmentId: cavallo, threadId: shared, label: "cavallo" },
            status: "running",
          },
          {
            childThreadId: null,
            remoteChild: { environmentId: bestia, threadId: shared, label: "bestia" },
            status: "completed",
          },
        ],
        contextTransfers: [],
      } as never,
    });

    const rows = immediateThreadRelationships(graph, parent);
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.environmentId)).toEqual([cavallo, bestia]);
    expect(getThreadRelationshipNode(graph, shared, cavallo)?.thread?.title).toBe("Cavallo child");
    expect(getThreadRelationshipNode(graph, shared, bestia)?.thread?.title).toBe("Bestia child");
    expect(rows.map((row) => threadRelationshipRowStatus(graph, row))).toEqual([
      "running",
      "completed",
    ]);
  });

  it("does not classify a same-id remote child as the current thread's parent", () => {
    const shared = ThreadId.make("thread-shared");
    const remoteEnvironment = EnvironmentId.make("environment-cavallo");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        { id: shared, status: "running", forkedFrom: null, lineage: { parentThreadId: null } },
      ] as never,
      projection: {
        thread: { id: shared },
        subagents: [
          {
            childThreadId: null,
            remoteChild: {
              environmentId: remoteEnvironment,
              threadId: shared,
              label: "cavallo",
            },
            status: "running",
          },
        ],
        contextTransfers: [],
      } as never,
    });

    const row = immediateThreadRelationships(graph, shared)[0]!;
    expect(row.environmentId).toBe(remoteEnvironment);
    expect(isParentThreadRelationship(row.edge, shared)).toBe(false);
    expect(threadRelationshipRowStatus(graph, row)).toBe("running");
  });

  it("keeps an unattached remote launch rowless until its thread is known", () => {
    const parent = ThreadId.make("thread-parent");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        { id: parent, status: "running", forkedFrom: null, lineage: { parentThreadId: null } },
      ] as never,
      projection: {
        thread: { id: parent },
        subagents: [
          {
            childThreadId: null,
            remoteChild: {
              environmentId: EnvironmentId.make("environment-cavallo"),
              label: "cavallo",
            },
            status: "running",
          },
        ],
        contextTransfers: [],
      } as never,
    });

    expect(graph.edges).toEqual([]);
  });

  it("keeps the live shell when an archived snapshot contains the same thread id", () => {
    const parent = ThreadId.make("thread-parent");
    const staleParent = ThreadId.make("thread-stale-parent");
    const child = ThreadId.make("thread-child");
    const liveChild = {
      id: child,
      title: "Live child",
      status: "running",
      archivedAt: null,
      forkedFrom: { type: "run", threadId: parent, runId: "run-live" },
      lineage: {
        rootThreadId: parent,
        parentThreadId: parent,
        relationshipToParent: "fork",
      },
    } as const;
    const staleArchivedChild = {
      ...liveChild,
      title: "Stale archived child",
      status: "completed",
      archivedAt: "2026-06-24T00:00:00.000Z",
      forkedFrom: { type: "run", threadId: staleParent, runId: "run-stale" },
      lineage: {
        rootThreadId: staleParent,
        parentThreadId: staleParent,
        relationshipToParent: "fork",
      },
    } as const;

    const graph = deriveThreadRelationshipGraph({
      threads: [liveChild, staleArchivedChild] as never,
      projection: null,
    });

    expect(graph.nodes.get(child)?.thread).toMatchObject({
      title: "Live child",
      status: "running",
      archivedAt: null,
    });
    expect(graph.edges).toEqual([
      expect.objectContaining({
        sourceThreadId: parent,
        targetThreadId: child,
      }),
    ]);
    expect(graph.nodes.has(staleParent)).toBe(false);
  });

  it("resolves merge-back only for forks and prefers the recorded fork source", () => {
    const source = ThreadId.make("thread-source");
    const fallbackParent = ThreadId.make("thread-parent");
    const fork = ThreadId.make("thread-fork");

    expect(
      resolveMergeBackTargetThreadId({
        thread: {
          id: fork,
          forkedFrom: { type: "run", threadId: source, runId: "run-source" },
          lineage: {
            rootThreadId: source,
            parentThreadId: fallbackParent,
            relationshipToParent: "fork",
          },
        },
      } as never),
    ).toBe(source);
    expect(
      resolveMergeBackTargetThreadId({
        thread: {
          id: fork,
          forkedFrom: null,
          lineage: {
            rootThreadId: source,
            parentThreadId: fallbackParent,
            relationshipToParent: "fork",
          },
        },
      } as never),
    ).toBe(fallbackParent);
    expect(
      resolveMergeBackTargetThreadId({
        thread: {
          id: fork,
          forkedFrom: null,
          lineage: {
            rootThreadId: source,
            parentThreadId: fallbackParent,
            relationshipToParent: "subagent",
          },
        },
      } as never),
    ).toBeNull();
  });
});

const current = ThreadId.make("thread-current");

function forkShell(input: {
  readonly id: ThreadId;
  readonly parentThreadId: ThreadId | null;
  readonly createdAt?: string;
  readonly updatedAt?: string;
  readonly status?: string;
}) {
  return {
    id: input.id,
    title: input.id,
    status: input.status ?? "completed",
    archivedAt: null,
    forkedFrom:
      input.parentThreadId === null
        ? null
        : { type: "run", threadId: input.parentThreadId, runId: `run-${input.id}` },
    lineage: {
      rootThreadId: input.parentThreadId ?? input.id,
      parentThreadId: input.parentThreadId,
      relationshipToParent: input.parentThreadId === null ? null : "fork",
    },
    createdAt: input.createdAt === undefined ? undefined : DateTime.makeUnsafe(input.createdAt),
    updatedAt: DateTime.makeUnsafe(input.updatedAt ?? "2026-06-20T00:00:00.000Z"),
  };
}

function orderedLineageIds(input: {
  readonly threads: ReadonlyArray<unknown>;
  readonly mergeTargetThreadId?: ThreadId | null;
  readonly projection?: unknown;
}): ReadonlyArray<ThreadId> {
  const graph = deriveThreadRelationshipGraph({
    threads: input.threads as never,
    projection: (input.projection ?? null) as never,
  });
  return orderWebThreadLineageRows({
    graph,
    rows: immediateThreadRelationships(graph, current),
    currentThreadId: current,
    mergeTargetThreadId: input.mergeTargetThreadId ?? null,
  }).map(({ threadId }) => threadId);
}

describe("web thread lineage ordering", () => {
  it("does not reorder when related-thread activity arrives", () => {
    const first = ThreadId.make("thread-fork-a");
    const second = ThreadId.make("thread-fork-b");
    const third = ThreadId.make("thread-fork-c");
    const before = orderedLineageIds({
      threads: [
        forkShell({ id: current, parentThreadId: null, createdAt: "2026-06-01T00:00:00.000Z" }),
        forkShell({ id: first, parentThreadId: current, createdAt: "2026-06-02T00:00:00.000Z" }),
        forkShell({ id: second, parentThreadId: current, createdAt: "2026-06-03T00:00:00.000Z" }),
        forkShell({ id: third, parentThreadId: current, createdAt: "2026-06-04T00:00:00.000Z" }),
      ],
    });

    const after = orderedLineageIds({
      threads: [
        forkShell({
          id: second,
          parentThreadId: current,
          createdAt: "2026-06-03T00:00:00.000Z",
          updatedAt: "2026-07-30T00:00:00.000Z",
          status: "running",
        }),
        forkShell({ id: third, parentThreadId: current, createdAt: "2026-06-04T00:00:00.000Z" }),
        forkShell({ id: current, parentThreadId: null, createdAt: "2026-06-01T00:00:00.000Z" }),
        forkShell({
          id: first,
          parentThreadId: current,
          createdAt: "2026-06-02T00:00:00.000Z",
          updatedAt: "2026-07-31T00:00:00.000Z",
          status: "failed",
        }),
      ],
    });

    expect(before).toEqual([third, second, first]);
    expect(after).toEqual(before);
  });

  it("pins the parent first and a distinct merge-back target second", () => {
    const parent = ThreadId.make("thread-parent");
    const mergeTarget = ThreadId.make("thread-merge-target");
    const newestFork = ThreadId.make("thread-newest-fork");

    expect(
      orderedLineageIds({
        threads: [
          forkShell({ id: parent, parentThreadId: null, createdAt: "2026-06-01T00:00:00.000Z" }),
          forkShell({
            id: current,
            parentThreadId: parent,
            createdAt: "2026-06-02T00:00:00.000Z",
          }),
          forkShell({
            id: mergeTarget,
            parentThreadId: current,
            createdAt: "2026-06-03T00:00:00.000Z",
          }),
          forkShell({
            id: newestFork,
            parentThreadId: current,
            createdAt: "2026-06-09T00:00:00.000Z",
          }),
        ],
        mergeTargetThreadId: mergeTarget,
      }),
    ).toEqual([parent, mergeTarget, newestFork]);
  });

  it("does not pin a same-id remote child as the merge-back target", () => {
    const parent = ThreadId.make("thread-parent");
    const newestFork = ThreadId.make("thread-newest-fork");
    const cavallo = EnvironmentId.make("environment-cavallo");
    const graph = deriveThreadRelationshipGraph({
      threads: [
        forkShell({ id: parent, parentThreadId: null, createdAt: "2026-06-01T00:00:00.000Z" }),
        forkShell({ id: current, parentThreadId: parent, createdAt: "2026-06-02T00:00:00.000Z" }),
        forkShell({
          id: newestFork,
          parentThreadId: current,
          createdAt: "2026-06-09T00:00:00.000Z",
        }),
        {
          shell: forkShell({
            id: parent,
            parentThreadId: null,
            createdAt: "2026-06-03T00:00:00.000Z",
          }),
          environmentId: cavallo,
        },
      ] as never,
      projection: {
        thread: { id: current },
        subagents: [
          {
            childThreadId: null,
            remoteChild: { environmentId: cavallo, threadId: parent, label: "cavallo" },
            status: "running",
          },
        ],
        contextTransfers: [],
      } as never,
    });

    const ordered = orderWebThreadLineageRows({
      graph,
      rows: immediateThreadRelationships(graph, current),
      currentThreadId: current,
      mergeTargetThreadId: parent,
    }).map((row) => `${row.environmentId ?? "local"}:${row.threadId}`);

    expect(ordered).toEqual([`local:${parent}`, `local:${newestFork}`, `${cavallo}:${parent}`]);
  });

  it("sinks missing nodes and shells without a decoded createdAt", () => {
    const missingTransfer = ThreadId.make("thread-missing-transfer");
    const undated = ThreadId.make("thread-undated-fork");
    const dated = ThreadId.make("thread-dated-fork");

    expect(
      orderedLineageIds({
        threads: [
          forkShell({ id: current, parentThreadId: null, createdAt: "2026-06-01T00:00:00.000Z" }),
          forkShell({ id: undated, parentThreadId: current }),
          forkShell({ id: dated, parentThreadId: current, createdAt: "2026-06-02T00:00:00.000Z" }),
        ],
        projection: {
          thread: { id: current },
          subagents: [],
          contextTransfers: [
            { sourceThreadId: current, targetThreadId: missingTransfer, status: "completed" },
          ],
        },
      }),
    ).toEqual([dated, missingTransfer, undated]);
  });
});
