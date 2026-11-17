import {
  type EnvironmentId,
  type HostResourcesSnapshot,
  type NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type ProjectId,
} from "@t3tools/contracts";
import { chooseLoadBalancedEnvironment } from "@t3tools/shared/loadBalancing";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Tool from "effect/ai/Tool";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as HostProcess from "@t3tools/shared/HostProcess";

import { runtimeModeRank } from "../mcp/delegatedTaskModes.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { EnvironmentToolkit } from "../mcp/toolkits/environment/tools.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as HostResources from "../resourceTelemetry/HostResources.ts";
import * as Settings from "../serverSettings.ts";
import { sourceControlDefinitions } from "../sourceControl/sourceControlDefinitions.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as DelegatedWorktreeBase from "./DelegatedWorktreeBase.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";

/** A linked machine that does not answer within this is left out, not waited for. */
const PEER_READ_TIMEOUT = Duration.seconds(3);

interface Candidate {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly resources: HostResourcesSnapshot | null;
  readonly receivedAt: number;
  readonly weight: number;
  readonly projectId: ProjectId | undefined;
}

/**
 * Places an unnamed worktree `delegate_task` on the least loaded eligible machine when this
 * machine's Auto balance is on. Returns the remote task, or undefined to run it here.
 */
export class DelegationPlacement extends Context.Service<
  DelegationPlacement,
  {
    readonly delegate: (
      scope: McpInvocationContext.McpThreadInvocationScope,
      input: OrchestratorMcpDelegateTaskInput,
      acceptedHere: boolean,
    ) => Effect.Effect<NodeId | undefined, OrchestratorMcpFailure>;
  }
>()("t3/peer/DelegationPlacement") {}

const make = Effect.gen(function* () {
  const settings = yield* Settings.ServerSettingsService;
  const links = yield* PeerLinks.PeerLinks;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const remote = yield* RemoteDelegation.RemoteDelegation;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const identities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const hostResources = yield* HostResources.HostResources;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const random = yield* SecureRandom.SecureRandom;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const clones = yield* Scope.Scope;
  const cloning = new Set<string>();
  // A clone that succeeded there; a later child only registers it if registration failed.
  const clonedAt = new Map<string, string>();

  const gitOut = (cwd: string, args: ReadonlyArray<string>) =>
    git.execute({ operation: "DelegationPlacement.git", cwd, args, allowNonZeroExit: true }).pipe(
      Effect.map((result) => (result.exitCode === 0 ? result.stdout.trim() : null)),
      Effect.orElseSucceed(() => null),
    );

  const remoteTaskFor = (
    scope: McpInvocationContext.McpThreadInvocationScope,
    clientRequestId: string,
  ) =>
    threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
      Effect.map(
        ({ subagents }) =>
          subagents.find(
            (task) =>
              task.remoteChild?.launch?.clientRequestId ===
              `delegate:${scope.thread.threadId}:${clientRequestId}`,
          )?.remoteChild,
      ),
      // An unreadable record is not proof that nothing was sent there.
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Unable to read prior delegated launches.",
          }),
      ),
    );

  /** Reads one linked machine's load and matching project; undefined when it does not answer in time. */
  const readPeer = Effect.fn("DelegationPlacement.readPeer")(
    function* (
      scope: McpInvocationContext.McpThreadInvocationScope,
      environmentId: EnvironmentId,
      label: string,
      weight: number,
      repositoryKeys: ReadonlySet<string>,
    ) {
      const read = yield* forwarding.call(
        scope,
        EnvironmentToolkit.tools.t3_environment_read,
        environmentId,
        {},
      );
      const receivedAt = yield* Clock.currentTimeMillis;
      let projectId: ProjectId | undefined;
      let cursor: number | null = 0;
      while (cursor !== null && projectId === undefined) {
        const listed: Tool.Success<typeof ProjectToolkit.tools.t3_project_list> =
          yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_list, environmentId, {
            cursor,
            limit: 100,
          });
        projectId = listed.projects.find((project) =>
          repositoryKeys.has(project.repositoryIdentity?.canonicalKey ?? ""),
        )?.id;
        cursor = listed.nextCursor;
      }
      return {
        environmentId,
        label,
        resources: read.hostResources ?? null,
        receivedAt,
        weight,
        projectId,
      } satisfies Candidate;
    },
    Effect.timeoutOption(PEER_READ_TIMEOUT),
    Effect.map(Option.getOrUndefined),
    Effect.orElseSucceed(() => undefined),
  );

  /** Clones the repository there in the background so later children can land; never awaited. */
  const startClone = (
    scope: McpInvocationContext.McpThreadInvocationScope,
    environmentId: EnvironmentId,
    remoteUrl: string,
    workspaceRoot: string,
  ) => {
    const relative = path.relative(home, workspaceRoot);
    const key = `${environmentId}\n${sourceControlDefinitions.repositoryKeyFromRemote(remoteUrl)}`;
    // The same place under that machine's home, which it resolves itself.
    if (relative.startsWith("..") || path.isAbsolute(relative) || cloning.has(key))
      return Effect.void;
    cloning.add(key);
    return Effect.gen(function* () {
      const cwd =
        clonedAt.get(key) ??
        (yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_clone, environmentId, {
          remoteUrl: withoutCredentials(remoteUrl),
          destinationPath: `~/${relative}`,
        })).cwd;
      clonedAt.set(key, cwd);
      yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_create, environmentId, {
        title: path.basename(workspaceRoot),
        workspaceRoot: cwd,
      });
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Background clone for delegation placement failed", cause).pipe(
          // A later child may try again once that machine recovers.
          Effect.andThen(Effect.sync(() => cloning.delete(key))),
        ),
      ),
      Effect.forkIn(clones),
      Effect.asVoid,
    );
  };

  const delegate: DelegationPlacement["Service"]["delegate"] = Effect.fn(
    "DelegationPlacement.delegate",
  )(function* (scope, input, acceptedHere) {
    if (
      input.isolation !== "worktree" ||
      input.target?.environmentId !== undefined ||
      input.projectId !== undefined ||
      input.workspaceStrategy !== undefined
    )
      return undefined;
    // A retry lands where the first request was recorded, whatever the settings say now.
    const recorded =
      input.clientRequestId === undefined
        ? undefined
        : yield* remoteTaskFor(scope, input.clientRequestId);
    if (recorded?.launch !== undefined) {
      const { taskId } = yield* remote.delegate(scope, {
        ...input,
        target: {
          ...input.target,
          environmentId: recorded.environmentId,
          projectId: recorded.launch.projectId,
        },
      });
      return taskId;
    }
    if (acceptedHere) return undefined;
    const current = yield* settings.getSettings.pipe(Effect.orElseSucceed(() => undefined));
    if (current?.autoBalance?.enabled !== true || !current.linkedEnvironmentsEnabled)
      return undefined;
    const clientRequestId = input.clientRequestId ?? (yield* random.uuidV4());

    const parent = yield* threads
      .getThreadRecords(scope.thread.threadId, ["runs"])
      .pipe(Effect.orElseSucceed(() => undefined));
    if (parent === undefined) return undefined;
    const project = yield* projects.getById(parent.thread.projectId).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.orElseSucceed(() => undefined),
    );
    if (project === undefined) return undefined;
    const checkout = parent.thread.worktreePath ?? project.workspaceRoot;
    // Another machine can start only from a pushed commit.
    const pushed = yield* DelegatedWorktreeBase.resolveBase(checkout).pipe(
      Effect.provideService(GitVcsDriver.GitVcsDriver, git),
      Effect.option,
    );
    if (Option.isNone(pushed)) return undefined;
    const upstreamRemote = (yield* gitOut(checkout, [
      "rev-parse",
      "--abbrev-ref",
      "@{upstream}",
    ]))?.split("/")[0];
    const remoteUrl =
      upstreamRemote === undefined
        ? null
        : yield* gitOut(checkout, ["remote", "get-url", "--", upstreamRemote]);
    const identity = yield* identities.resolve(project.workspaceRoot);
    const repositoryKeys = new Set(
      [
        identity?.canonicalKey,
        remoteUrl === null
          ? undefined
          : sourceControlDefinitions.repositoryKeyFromRemote(remoteUrl),
      ].filter((key): key is string => key !== undefined),
    );

    const parentMode = McpInvocationContext.intersectModes(
      parent.thread,
      scope.authority?.ceiling,
    ).runtimeMode;
    // The same per-machine weights the composer's Auto balance uses; 0 is Manual only.
    const { weights } = current.autoBalance;
    const weightOf = (environmentId: string) => weights[environmentId] ?? 50;
    const eligible = (yield* links.list.pipe(Effect.orElseSucceed(() => []))).filter(
      (link) =>
        link.status === "reachable" &&
        weightOf(link.environmentId) > 0 &&
        link.access !== "read-only" &&
        runtimeModeRank(link.access) >= runtimeModeRank(parentMode),
    );
    if (eligible.length === 0) return undefined;
    const peers = (yield* Effect.forEach(
      eligible,
      (link) =>
        readPeer(
          scope,
          link.environmentId,
          link.label,
          weightOf(link.environmentId),
          repositoryKeys,
        ),
      { concurrency: "unbounded" },
    )).filter((candidate) => candidate !== undefined);
    const now = yield* Clock.currentTimeMillis;
    const here: Candidate = {
      environmentId: scope.environmentId,
      label: "",
      resources: yield* hostResources.read,
      receivedAt: now,
      weight: weightOf(scope.environmentId),
      projectId: undefined,
    };
    const chosenId = chooseLoadBalancedEnvironment([here, ...peers], now);
    const chosen = peers.find((peer) => peer.environmentId === chosenId);
    if (chosen === undefined) return undefined;
    const { environmentId } = chosen;
    if (chosen.projectId === undefined) {
      if (remoteUrl !== null)
        yield* startClone(scope, environmentId, remoteUrl, project.workspaceRoot);
      return undefined;
    }
    return yield* remote
      .delegate(scope, {
        ...input,
        clientRequestId,
        task: `${input.task}\n\nYou run on ${chosen.label}, a linked machine chosen for its free capacity.`,
        target: { ...input.target, environmentId, projectId: chosen.projectId },
      })
      .pipe(
        Effect.map(({ taskId }) => taskId),
        // Before the request is recorded nothing ran there, so the task runs here instead.
        Effect.catch((error) =>
          remoteTaskFor(scope, clientRequestId).pipe(
            Effect.flatMap((task) => (task === undefined ? Effect.undefined : Effect.fail(error))),
            Effect.mapError(() => error),
          ),
        ),
      );
  });

  return DelegationPlacement.of({ delegate });
});

const withoutCredentials = (remoteUrl: string) => {
  try {
    const url = new URL(remoteUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") return remoteUrl;
    url.username = "";
    url.password = "";
    return url.toString();
  } catch {
    return remoteUrl;
  }
};

export const layer = Layer.effect(DelegationPlacement, make);
