import {
  EventId,
  type OrchestrationProjectShell,
  type OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  type RepositoryIdentity,
  ThreadId,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectService from "../project/ProjectService.ts";

const CREATED_AT = "2026-09-01T00:00:00.000Z";
const providerInstanceId = ProviderInstanceId.make("codex");

/** A GitHub identity the way the resolver reports one for a checkout of `owner/name`. */
export function gitHubIdentity(repository: string, origin?: string): RepositoryIdentity {
  const [owner, name] = repository.split("/");
  return {
    canonicalKey: `github.com/${repository.toLowerCase()}`,
    locator: {
      source: "git-remote",
      remoteName: "origin",
      remoteUrl: `git@github.com:${repository}.git`,
    },
    provider: "github",
    displayName: repository,
    ...(owner ? { owner } : {}),
    ...(name ? { name } : {}),
    ...(origin === undefined ? {} : { origin: { canonicalKey: `github.com/${origin}` } }),
  };
}

/** The real V2 event and projection stores over the database in context. */
export const v2Stores = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(EventStore.layer, ProjectionStore.layer, ProjectStore.layer)),
    Layer.provideMerge(database),
  );

/**
 * The reads `IssueLinks` makes of the orchestrator and the project service, served by the real
 * V2 projection and project rows. A checkout resolves to the identity mapped for its workspace
 * root instead of asking git.
 */
export const readLayer = (identities: Readonly<Record<string, RepositoryIdentity>>) => {
  const enrich = (project: OrchestrationProjectShell): OrchestrationProjectShell => ({
    ...project,
    repositoryIdentity: identities[project.workspaceRoot] ?? null,
  });
  return Layer.mergeAll(
    Layer.unwrap(
      Effect.map(ProjectionStore.ProjectionStoreV2, (store) =>
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadShell: (threadId) => store.getThreadShell(threadId).pipe(Effect.orDie),
        }),
      ),
    ),
    Layer.unwrap(
      Effect.map(ProjectStore.ProjectStoreV2, (store) =>
        Layer.mock(ProjectService.ProjectService)({
          getShell: (projectId) =>
            store.getShell(projectId).pipe(Effect.map(Option.map(enrich)), Effect.orDie),
          listShells: (options) =>
            store.listShells(options).pipe(
              Effect.map((shells) => shells.map(enrich)),
              Effect.orDie,
            ),
        }),
      ),
    ),
  );
};

export const insertProject = (id: string, workspaceRoot: string) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql`
      INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES (${id}, ${id}, ${workspaceRoot}, '[]', ${CREATED_AT}, ${CREATED_AT})
    `,
  );

/** A pull request link as the thread's own `pullRequests` hold it. */
export const pullRequestLink = (
  repository: string,
  number: number,
  source: ThreadPullRequestLink["source"] = "manual",
): ThreadPullRequestLink => ({
  host: "github.com",
  repository,
  number,
  url: `https://github.com/${repository}/pull/${number}`,
  source,
  linkedAt: CREATED_AT,
  snapshot: null,
  stack: null,
});

let eventCount = 0;

const writeEvent = (
  type: "thread.created" | "thread.metadata-updated" | "thread.deleted",
  thread: OrchestrationV2AppThread,
) =>
  Effect.flatMap(EventSink.EventSinkV2, (sink) =>
    sink.write({
      events: [
        {
          id: EventId.make(`event:issue-links:${(eventCount += 1)}`),
          type,
          threadId: thread.id,
          providerInstanceId,
          occurredAt: thread.updatedAt,
          payload: thread,
        },
      ],
    }),
  ).pipe(Effect.asVoid);

/** A V2 thread, created through the real event sink and projection. */
export const writeThread = (input: {
  readonly id: string;
  readonly projectId: string;
  readonly branch?: string | null;
  readonly updatedAt?: string;
  readonly createdAt?: string;
  readonly pullRequests?: ReadonlyArray<ThreadPullRequestLink>;
  readonly parent?: { readonly id: string; readonly root?: string };
  readonly relationshipToParent?: "subagent" | "fork";
}) => {
  const id = ThreadId.make(input.id);
  const createdAt = DateTime.makeUnsafe(input.createdAt ?? CREATED_AT);
  return writeEvent("thread.created", {
    createdBy: "user",
    creationSource: "web",
    id,
    projectId: ProjectId.make(input.projectId),
    title: input.id,
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: input.branch ?? null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage:
      input.parent === undefined
        ? { parentThreadId: null, relationshipToParent: null, rootThreadId: id }
        : {
            parentThreadId: ThreadId.make(input.parent.id),
            relationshipToParent: input.relationshipToParent ?? "subagent",
            rootThreadId: ThreadId.make(input.parent.root ?? input.parent.id),
          },
    forkedFrom: null,
    ...(input.pullRequests === undefined ? {} : { pullRequests: input.pullRequests }),
    createdAt,
    updatedAt: DateTime.makeUnsafe(input.updatedAt ?? input.createdAt ?? CREATED_AT),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  });
};

/** Changes a V2 thread the way its commands do: a new event carrying the whole thread. */
export const updateThread = (
  id: string,
  change: (thread: OrchestrationV2AppThread) => OrchestrationV2AppThread,
) =>
  Effect.flatMap(ProjectionStore.ProjectionStoreV2, (store) =>
    store.getThread(ThreadId.make(id)),
  ).pipe(
    Effect.orDie,
    Effect.flatMap((thread) => {
      const next = change(thread);
      return writeEvent(
        next.deletedAt === null ? "thread.metadata-updated" : "thread.deleted",
        next,
      );
    }),
  );

export const deleteThread = (id: string, at: string) =>
  updateThread(id, (thread) => ({
    ...thread,
    deletedAt: DateTime.makeUnsafe(at),
    updatedAt: DateTime.makeUnsafe(at),
  }));

export const restoreThread = (id: string, at: string) =>
  updateThread(id, (thread) => ({
    ...thread,
    deletedAt: null,
    updatedAt: DateTime.makeUnsafe(at),
  }));
