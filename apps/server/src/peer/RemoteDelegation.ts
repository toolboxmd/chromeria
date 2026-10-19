import {
  toolAllowlistsEqual,
  toolPolicyModeRejection,
} from "@t3tools/provider-core/server/toolPolicy";
import * as ThreadMcpConfig from "../orchestration-v2/ThreadMcpConfig.ts";
import * as Schema from "effect/Schema";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Semaphore from "effect/Semaphore";
import { reconcileRemoteDelegatedTaskQuestions } from "../orchestration-v2/DelegatedTaskQuestions.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import {
  type PeerReturnPair,
  type PeerTaskObservation,
  type PeerTaskStop,
  type PeerTaskView,
  type PeerTaskResource,
  type PeerTaskLifecycleResult,
  PeerTaskSnapshot,
} from "@t3tools/contracts";
import * as Prism from "../prism/PrismService.ts";
import { executionPurposeOf } from "@t3tools/contracts";
import * as PurposeAdmission from "../orchestration-v2/ExecutionPurpose.ts";
import {
  CommandId,
  type EnvironmentId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestratorMcpThreadTimelineItem,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2Subagent,
  type ProjectId,
  type ProviderInstanceId,
  ProviderDriverKind,
  type RunId,
  type ThreadId,
  admitDelegateRole,
  PRISM_DELEGATE_ROLE_ERROR,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Tool from "effect/ai/Tool";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import {
  resolveInteractionMode,
  resolveRuntimeMode,
  runtimeModeRank,
} from "../mcp/delegatedTaskModes.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { forkParked } from "../serverActivation.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";

/** How long one follow of a remote task waits on its thread there before checking again. */
const FOLLOW_WAIT_MS = 10 * 60 * 1_000;
/** A linked environment that stops answering is retried, backing off to this. */
const FOLLOW_MAX_BACKOFF = Duration.minutes(5);
const backoff = Schedule.exponential(Duration.seconds(2)).pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, FOLLOW_MAX_BACKOFF)),
  ),
);

type ThreadScope = McpInvocationContext.McpThreadInvocationScope;
const encodeSnapshot = Schema.encodeSync(Schema.fromJsonString(PeerTaskSnapshot));
const snapshotWithoutTime = (snapshot: PeerTaskSnapshot) =>
  encodeSnapshot({ ...snapshot, observedAt: "1970-01-01T00:00:00.000Z" });

/**
 * `delegate_task` to a linked environment. The task's child is an ordinary
 * thread there, launched through the link; the parent here records the task
 * like a local one, without a child thread. Durable observation preserves the
 * first result and delivers later real results through the existing parent wake owner.
 */
export class RemoteDelegation extends Context.Service<
  RemoteDelegation,
  {
    readonly requestStop: (
      parentThreadId: ThreadId,
      taskId: NodeId,
      requestId: string,
      reason?: string,
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    readonly view: (
      parentThreadId: ThreadId,
      taskId: NodeId,
      after: number,
      textOffset?: number,
      resource?: PeerTaskResource,
    ) => Effect.Effect<PeerTaskView, OrchestratorMcpFailure>;
    readonly delegate: (
      scope: ThreadScope,
      input: OrchestratorMcpDelegateTaskInput & {
        readonly target: NonNullable<OrchestratorMcpDelegateTaskInput["target"]> & {
          readonly environmentId: EnvironmentId;
        };
      },
    ) => Effect.Effect<{ readonly taskId: NodeId }, OrchestratorMcpFailure>;
    /**
     * Records a durable Stop obligation. Receipts distinguish a pending request,
     * destination acknowledgement and settled execution; the first result stays intact.
     */
    readonly cancel: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      input: {
        readonly reason?: string | undefined;
        readonly clientRequestId?: string | undefined;
      },
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    /** Restores lifecycle observation and outstanding Stop obligations after a restart. */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/peer/RemoteDelegation") {}

/**
 * Refusals that following again cannot change: the link was revoked, expired
 * or forgotten, or the thread or run there is gone. Anything else (the other
 * machine asleep, a dropped route) is worth retrying.
 */
/** The other side refused because the thread there runs above this caller's modes. */
const isModeRefusal = (error: OrchestratorMcpFailure) =>
  error.code === "runtime_mode_escalation_denied" ||
  error.code === "interaction_mode_escalation_denied";

const isFinal = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied" ||
  error.code === "thread_not_found" ||
  error.code === "run_not_found";

const make = Effect.gen(function* () {
  const configs = yield* Effect.serviceOption(ThreadMcpConfig.ThreadMcpConfig);
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const requireLifecycle = (environmentId: EnvironmentId, generation?: string) =>
    peers.requireTaskLifecycle === undefined
      ? Effect.fail(
          new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Delegated lifecycle v1 is unsupported by this transport.",
          }),
        )
      : peers.requireTaskLifecycle(environmentId, generation);

  const links = yield* PeerLinks.PeerLinks;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const commandReceipts = yield* CommandReceipts.CommandReceiptStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const repositoryIdentities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const crypto = yield* Crypto.Crypto;
  const hereId = yield* ServerEnvironment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  // Followers live as long as this service, so shutting down stops them.
  const followers = yield* Scope.Scope;

  /** The peer's project with this thread's repository, or the one the caller named. */
  const resolveRemoteProject = (
    scope: ThreadScope,
    environmentId: EnvironmentId,
    parentProjectId: ProjectId,
    requested: ProjectId | undefined,
  ) =>
    Effect.gen(function* () {
      if (requested !== undefined) return requested;
      const here = yield* projects.getById(parentProjectId).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.orElseSucceed(() => undefined),
      );
      // The project's own identity is a one-minute cache that reads blank when
      // cold, so resolve it here, as the other side's t3_project_list does.
      const key =
        here === undefined
          ? undefined
          : (yield* repositoryIdentities.resolve(here.workspaceRoot))?.canonicalKey;
      if (key === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "target_required",
          message: "Pass target.projectId: this thread's project has no repository to match there.",
        });
      }
      const matches: Array<{ readonly id: ProjectId; readonly title: string }> = [];
      let cursor: number | null = 0;
      while (cursor !== null) {
        const listed: Tool.Success<typeof ProjectToolkit.tools.t3_project_list> =
          yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_list, environmentId, {
            cursor,
            limit: 100,
          });
        matches.push(
          ...listed.projects.filter((project) => project.repositoryIdentity?.canonicalKey === key),
        );
        cursor = listed.nextCursor;
      }
      if (matches.length === 1) return matches[0]!.id;
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message:
          matches.length === 0
            ? `No project there has this repository (${key}). Pass target.projectId; t3_project_list with this environmentId lists them.`
            : `Several projects there have this repository: ${matches.map((project) => `${project.title} (${project.id})`).join(", ")}. Pass target.projectId.`,
      });
    });

  const delegate: RemoteDelegation["Service"]["delegate"] = (scope, input) =>
    Effect.gen(function* () {
      const parent = yield* threads.getThreadRecords(scope.thread.threadId, ["runs"]).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "thread_not_found",
              message: "The calling thread was not found.",
            }),
        ),
      );
      const parentRun = parent.runs
        .filter(ThreadManagement.isActiveRun)
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      if (
        parentRun === undefined ||
        parentRun.rootNodeId === null ||
        parentRun.providerInstanceId !== scope.thread.providerInstanceId
      ) {
        return yield* new OrchestratorMcpFailure({
          code: "parent_not_active",
          message: "Delegated tasks require an active run owned by this MCP provider session.",
        });
      }
      const parentNodeId = parentRun.rootNodeId;
      const parentModes = McpInvocationContext.intersectModes(
        parent.thread,
        scope.authority?.ceiling,
      );
      const { target } = input;
      const modelSelection =
        target.model === undefined
          ? undefined
          : {
              instanceId: target.providerInstanceId ?? parent.thread.modelSelection.instanceId,
              model: target.model,
              ...(target.options === undefined ? {} : { options: target.options }),
            };
      // Explicit modes are checked against this thread. Inherited modes are
      // resolved below against the link before storing the launch intent.
      const runtimeMode =
        input.runtimeMode === undefined || input.runtimeMode === "inherit"
          ? undefined
          : yield* resolveRuntimeMode(parentModes.runtimeMode, input.runtimeMode);
      const interactionMode =
        input.interactionMode === undefined || input.interactionMode === "inherit"
          ? undefined
          : yield* resolveInteractionMode(parentModes.interactionMode, input.interactionMode);
      const projectId = yield* resolveRemoteProject(
        scope,
        target.environmentId,
        parent.thread.projectId,
        target.projectId,
      );
      // One key for the launch there and the record here, so a retry finds both.
      // The parent and key survive provider credential rotation. The launch
      // keeps its original link generation, checked on every recovery.
      const key = input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie));
      const task = input.task;
      const link = yield* links.get(target.environmentId).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
        ),
      );
      if (link === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The destination is no longer linked here.",
        });
      }
      const existing = (yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to read the originating task identity.",
            }),
        ),
      )).subagents.find(
        (task) =>
          task.remoteChild?.launch?.clientRequestId === `delegate:${scope.thread.threadId}:${key}`,
      );
      if (
        existing?.remoteChild?.launch !== undefined &&
        (existing.remoteChild.environmentId !== target.environmentId ||
          existing.remoteChild.launch.linkIdentity !== link.launchIdentity)
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "This task key belongs to its original destination and link generation; it cannot be replayed through a replacement grant.",
        });
      if (existing === undefined || existing.remoteChild?.launch?.lifecycleVersion === 1)
        yield* requireLifecycle(target.environmentId, link.launchIdentity);
      // Keep the original inherited modes across recovery, even if the parent
      // is later raised. Forwarding still checks the current parent's limits.
      const linkRuntimeMode =
        link?.access === "read-only"
          ? "approval-required"
          : (link?.access ?? parentModes.runtimeMode);
      const inheritedRuntimeMode =
        runtimeModeRank(linkRuntimeMode) < runtimeModeRank(parentModes.runtimeMode)
          ? linkRuntimeMode
          : parentModes.runtimeMode;
      const commandId = CommandId.make(
        `command:mcp:${encodeURIComponent(`thread:${hereId}:${scope.thread.threadId}`)}:remote-delegate:${encodeURIComponent(key)}`,
      );
      const prismRole =
        input.prismRole === undefined
          ? Prism.delegatedPrismRole(input.role)
          : admitDelegateRole(input.prismRole, yield* threads.hasReceipt(commandId));
      if (prismRole === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: PRISM_DELEGATE_ROLE_ERROR,
        });
      const parentPurpose = executionPurposeOf(parentRun);
      const resolvedPurpose = yield* PurposeAdmission.resolvePurposeIntent({
        intent: input.purposeIntent,
        selection: parent.thread.purposeSelection,
        inherited: {
          version: 1,
          scope: parentPurpose.scope,
          provenance: {
            type: "parent_run",
            threadId: parent.thread.id,
            runId: parentRun.id,
          },
        },
      }).pipe(
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
        ),
      );
      const earlier = (yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to read prior delegated launches.",
            }),
        ),
      )).subagents.find(
        (task) =>
          task.remoteChild?.launch?.requestNamespace === scope.requestNamespace &&
          task.remoteChild.launch.clientRequestId === `delegate:${scope.thread.threadId}:${key}`,
      )?.remoteChild?.launch;
      if (earlier !== undefined && !toolAllowlistsEqual(input.tools, earlier.tools))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Launch tool allowlist changed.",
        });
      if (input.tools !== undefined) {
        const modeError = toolPolicyModeRejection({
          runtimeMode: runtimeMode ?? inheritedRuntimeMode,
          interactionMode: interactionMode ?? parent.thread.interactionMode,
        });
        if (modeError !== undefined)
          return yield* new OrchestratorMcpFailure({ code: "invalid_request", message: modeError });
      }
      let safeTools = earlier?.tools;
      if (input.tools !== undefined && earlier === undefined) {
        if (Option.isNone(configs))
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Thread MCP configuration service is unavailable.",
          });
        safeTools = yield* configs.value.prepare(input.tools).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Thread MCP configuration could not be saved.",
              }),
          ),
        );
      }
      const launch = {
        lifecycleVersion: 1 as const,
        ...(input.bare === undefined ? {} : { bare: input.bare }),
        ...(safeTools === undefined ? {} : { tools: safeTools }),
        requestNamespace: `thread:${hereId}:${scope.thread.threadId}`,
        clientRequestId: `delegate:${scope.thread.threadId}:${key}`,
        linkIdentity: link.launchIdentity,
        capabilities: [...scope.capabilities],
        projectId,
        title: input.title ?? parent.thread.title,
        ...(modelSelection === undefined ? {} : { modelSelection }),
        prismRole,
        ...(input.lane === undefined ? {} : { lane: input.lane }),
        ...(input.workspaceStrategy === undefined
          ? {}
          : { workspaceStrategy: input.workspaceStrategy }),
        runtimeMode: runtimeMode ?? inheritedRuntimeMode,
        interactionMode: interactionMode ?? parentModes.interactionMode,
        delegatedFrom: {
          environmentId: hereId,
          threadId: parent.thread.id,
          title: parent.thread.title,
          ...(input.purposeIntent === undefined
            ? {}
            : {
                purposeLaunch: {
                  environmentId: hereId,
                  threadId: parent.thread.id,
                  runId: parentRun.id,
                  launchId: commandId,
                  purpose: resolvedPurpose.purpose,
                },
              }),
        },
      };
      const remoteChild: OrchestrationV2RemoteTaskChild = {
        environmentId: target.environmentId,
        label: link?.label ?? target.environmentId,
        launch,
      };
      const recorded = yield* threads
        .dispatch({
          type: "delegated_task.remote.request",
          ...(input.purposeIntent === undefined ? {} : { purposeIntent: input.purposeIntent }),
          commandId,
          parentThreadId: scope.thread.threadId,
          parentRunId: parentRun.id,
          parentNodeId,
          task,
          ...(input.title === undefined ? {} : { title: input.title }),
          // The driver runs there; this side only labels the task with it.
          driver: ProviderDriverKind.make("remote"),
          ...(launch.modelSelection === undefined ? {} : { modelSelection: launch.modelSelection }),
          remoteChild,
          completionWake: input.mode === "wait" ? "settled_only" : "always",
        })
        .pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: `Unable to record the delegated task: ${error.message}`,
              }),
          ),
        );
      const taskEvent = recorded.storedEvents.find(
        (stored) => stored.event.type === "subagent.updated",
      );
      const taskId =
        taskEvent?.event.type === "subagent.updated"
          ? taskEvent.event.payload.id
          : // A replayed request recorded the task the first time.
            yield* threads.getThreadRecords(scope.thread.threadId, ["subagents"]).pipe(
              Effect.map(
                ({ subagents }) =>
                  subagents.find(
                    (task) =>
                      task.remoteChild?.launch?.requestNamespace === launch.requestNamespace &&
                      task.remoteChild.launch.clientRequestId === launch.clientRequestId,
                  )?.id,
              ),
              Effect.orElseSucceed(() => undefined),
            );
      if (taskId === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The delegated task was not recorded.",
        });
      }
      yield* launchPending(scope.thread.threadId, taskId).pipe(
        Effect.ensuring(follow(scope.thread.threadId, taskId).pipe(Effect.forkIn(followers))),
      );
      return { taskId };
    });

  /**
   * Completes `task` here with its thread there's final state. Each attempt
   * is its own command, so one that failed never blocks the next. A report
   * refused because the task already has its result (from a cancel, a Stop
   * or an earlier report) is done. `stoppedThere` records that nothing is
   * left to stop there, also on a task that already has its result.
   */
  const complete = (
    parentThreadId: ThreadId,
    taskId: NodeId,
    status: "completed" | "failed" | "cancelled" | "interrupted",
    result: string,
    stoppedThere: "ran-out" | "stopped" | undefined,
    launchError?: string,
  ) =>
    Effect.gen(function* () {
      const attempt = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      yield* threads
        .dispatch({
          type: "delegated_task.remote.complete",
          commandId: CommandId.make(`command:remote-task-complete:${taskId}:${attempt}`),
          parentThreadId,
          taskId,
          status,
          result,
          ...(stoppedThere === undefined ? {} : { stoppedThere }),
          ...(launchError === undefined ? {} : { launchError }),
        })
        .pipe(
          Effect.catch((error) =>
            threads
              .getThreadRecords(parentThreadId, ["subagents"])
              .pipe(
                Effect.flatMap(({ subagents }) =>
                  subagents.find((task) => task.id === taskId)?.result != null
                    ? Effect.void
                    : Effect.fail(error),
                ),
              ),
          ),
        );
    });

  /** Replays the stored launch identity; a different follower namespace would launch another child. */
  const launchPending = (parentThreadId: ThreadId, taskId: NodeId) =>
    Effect.gen(function* () {
      let records = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      let task = records.subagents.find((candidate) => candidate.id === taskId);
      if (task?.remoteChild === undefined || task.remoteChild.threadId !== undefined) return;
      if (task.result !== null && task.remoteChild.stoppedThere === true) return;
      const launch = task.remoteChild.launch;
      if (launch === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The pending remote task has no launch inputs.",
        });
      }
      if (task.remoteChild.launchStarted !== true) {
        // The parent's dispatch lock orders this against Stop/cancel. Once committed,
        // acceptance is uncertain until the same key returns a destination receipt.
        if (task.result !== null) {
          yield* observe(parentThreadId, taskId, {
            stop: {
              requestId: task.remoteChild.stop?.requestId ?? `not-sent:${taskId}`,
              state: "not_started",
            },
          });
          yield* complete(
            parentThreadId,
            taskId,
            terminalStatus(task.status),
            task.result,
            "stopped",
          );
          return;
        }
        yield* threads
          .dispatch({
            type: "delegated_task.remote.launch",
            commandId: CommandId.make(`command:remote-task-launch:${taskId}`),
            parentThreadId,
            taskId,
          })
          .pipe(
            Effect.catch((error) =>
              threads
                .getThreadRecords(parentThreadId, ["subagents"])
                .pipe(
                  Effect.flatMap(({ subagents }) =>
                    subagents.find((candidate) => candidate.id === taskId)?.result != null
                      ? Effect.void
                      : Effect.fail(error),
                  ),
                ),
            ),
          );
        records = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
        task = records.subagents.find((candidate) => candidate.id === taskId);
        if (task?.remoteChild?.launchStarted !== true) {
          if (task?.result != null) {
            yield* observe(parentThreadId, taskId, {
              stop: {
                requestId: task.remoteChild?.stop?.requestId ?? `not-sent:${taskId}`,
                state: "not_started",
              },
            });
            yield* complete(
              parentThreadId,
              taskId,
              terminalStatus(task.status),
              task.result,
              "stopped",
            );
          }
          return;
        }
      }
      const remote = task.remoteChild;
      if (remote === undefined) return;
      const scope: ThreadScope = {
        ...followScope(
          { threadId: parentThreadId, providerInstanceId: records.thread.providerInstanceId },
          remote,
        ),
        requestNamespace: launch.requestNamespace,
        capabilities: new Set(launch.capabilities ?? ["orchestration"]),
      };
      let tools: import("@t3tools/contracts").ThreadToolsRequest | undefined;
      if (launch.tools !== undefined) {
        let mcpServers: import("@t3tools/contracts").ThreadMcpServers | undefined;
        if (launch.tools.mcpConfigId !== undefined) {
          if (Option.isNone(configs))
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Thread MCP configuration service is unavailable.",
            });
          mcpServers = yield* configs.value.read(launch.tools.mcpConfigId).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "Thread MCP configuration is unavailable.",
                }),
            ),
          );
        }
        tools = { allow: launch.tools.allow, ...(mcpServers === undefined ? {} : { mcpServers }) };
      }
      const launched = yield* (
        launch.lifecycleVersion === 1
          ? requireLifecycle(remote.environmentId, launch.linkIdentity)
          : Effect.void
      )
        .pipe(
          Effect.andThen(
            forwarding.call(
              scope,
              ProjectToolkit.tools.t3_thread_launch,
              remote.environmentId,
              {
                projectId: launch.projectId,
                ...(launch.bare === undefined ? {} : { bare: launch.bare }),
                ...(tools === undefined ? {} : { tools }),
                title: launch.title,
                ...(launch.modelSelection === undefined
                  ? {}
                  : { modelSelection: launch.modelSelection }),
                ...(launch.prismRole === undefined ? {} : { prismRole: launch.prismRole }),
                ...(launch.lane === undefined ? {} : { lane: launch.lane }),
                ...(launch.workspaceStrategy === undefined
                  ? {}
                  : { workspaceStrategy: launch.workspaceStrategy }),
                ...(launch.runtimeMode === undefined ? {} : { runtimeMode: launch.runtimeMode }),
                ...(launch.interactionMode === undefined
                  ? {}
                  : { interactionMode: launch.interactionMode }),
                delegatedFrom: launch.delegatedFrom,
                ...(launch.delegatedFrom.purposeLaunch === undefined
                  ? {}
                  : {
                      purposeIntent: { version: 1, intent: { type: "inherit" } },
                    }),
                message: task.prompt,
                clientRequestId: launch.clientRequestId,
              },
              { expectedLinkIdentity: launch.linkIdentity, pair: { parentThreadId, taskId } },
            ),
          ),
        )
        .pipe(
          Effect.catchTags({
            OrchestratorMcpFailure: (error) => {
              if (isFinal(error) || isModeRefusal(error) || error.code === "invalid_request") {
                const message = `${error.message} The remote launch could not be confirmed. If it was already accepted, work may still be running on ${remote.label}.`;
                return complete(parentThreadId, taskId, "failed", message, undefined, message).pipe(
                  Effect.andThen(Effect.fail(error)),
                );
              }
              return Effect.fail(error);
            },
          }),
        );
      yield* threads.dispatch({
        type: "delegated_task.remote.attach",
        commandId: CommandId.make(`command:remote-task-attach:${taskId}:${launched.threadId}`),
        parentThreadId,
        taskId,
        environmentId: remote.environmentId,
        threadId: launched.threadId,
        runId: launched.runId,
        modelSelection: launched.modelSelection,
      });
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to reconcile the remote task's launch.",
            }),
      ),
    );

  /**
   * How this environment reaches a task's thread there: as the parent
   * thread, under one namespace per parent, so a stop sent again after a
   * restart or a dropped route is recorded there once.
   */
  const followScope = (
    parent: { readonly threadId: ThreadId; readonly providerInstanceId: ProviderInstanceId },
    remote: OrchestrationV2RemoteTaskChild,
  ): ThreadScope => ({
    environmentId: hereId,
    requestNamespace: `remote-task:${parent.threadId}`,
    thread: {
      threadId: parent.threadId,
      providerSessionId: `remote-task:${remote.threadId}`,
      providerInstanceId: parent.providerInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 0,
  });

  /**
   * The last reply `runId` gave on the task's thread there, which is its
   * result, read whole however long it is. A read only pages forward, so
   * it pages through the thread.
   */
  const lastReply = (
    scope: ThreadScope,
    remote: OrchestrationV2RemoteTaskChild & { readonly threadId: ThreadId },
    runId: RunId | null,
  ) =>
    Effect.gen(function* () {
      let reply: OrchestratorMcpThreadTimelineItem | undefined;
      let afterPosition: number | undefined;
      while (true) {
        const read = yield* forwarding.readThread(scope, remote.environmentId, {
          threadId: remote.threadId,
          view: "messages",
          limit: 100,
          ...(afterPosition === undefined ? {} : { afterPosition }),
        });
        reply =
          read.items.findLast(
            (item) => item.type === "assistant_message" && (runId === null || item.runId === runId),
          ) ?? reply;
        if (!read.hasMore || read.nextPosition === null) break;
        afterPosition = read.nextPosition;
      }
      if (reply === undefined) return undefined;
      let text = reply.text ?? "";
      let textOffset = reply.textTruncated ? (reply.nextTextOffset ?? null) : null;
      while (textOffset !== null) {
        const rest = yield* forwarding.readThread(scope, remote.environmentId, {
          threadId: remote.threadId,
          itemId: reply.itemId,
          textOffset,
          maxCharsPerItem: 50_000,
        });
        const part = rest.items[0];
        if (part === undefined) break;
        text += part.text ?? "";
        textOffset = part.textTruncated ? (part.nextTextOffset ?? null) : null;
      }
      return text;
    });

  /**
   * Why following a task can never succeed, if that is what `error` means.
   * The far side's message already says what was refused and what to do; a
   * link forgotten here only reads as a bad request, so it is named.
   */
  const finalReason = (parentThreadId: ThreadId, taskId: NodeId, error: OrchestratorMcpFailure) =>
    Effect.gen(function* () {
      if (isFinal(error)) return error.message;
      if (error.code !== "invalid_request") return undefined;
      const { subagents } = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      const environmentId = subagents.find((task) => task.id === taskId)?.remoteChild
        ?.environmentId;
      if (environmentId === undefined) return error.message;
      const link = yield* links.get(environmentId);
      return Option.isSome(link) ? undefined : error.message;
    }).pipe(Effect.orElseSucceed(() => undefined));

  /** A task's thread there that ended without a reply. */
  const noReply = "(The thread in the linked environment ended without a reply.)";

  /** One observer per job, with bounded peer requests and durable replay on either restart. */
  const networkSlots = yield* Semaphore.make(8);
  const activeFollowers = new Set<NodeId>();
  const acknowledgedCursors = new Map<NodeId, number>();
  const follow = (parentThreadId: ThreadId, taskId: NodeId): Effect.Effect<void> =>
    Effect.suspend(() => {
      if (activeFollowers.has(taskId)) return Effect.void;
      activeFollowers.add(taskId);
      return followOnce(parentThreadId, taskId).pipe(
        // `false` means it is still running there: follow again straight away.
        Effect.repeat({ while: (done) => !done }),
        Effect.catchTags({
          OrchestratorMcpFailure: (error) =>
            Effect.gen(function* () {
              const current = (yield* threads.getThreadRecords(parentThreadId, [
                "subagents",
              ])).subagents.find((task) => task.id === taskId);
              if (current?.remoteChild?.launch !== undefined) {
                yield* observe(parentThreadId, taskId, {
                  availability: error.code === "invalid_request" ? "unsupported" : "stale",
                });
                return yield* error;
              }
              const reason = yield* finalReason(parentThreadId, taskId, error);
              if (reason === undefined) return yield* error;
              // Gone there, or no link to reach it by: nothing left to stop there.
              yield* complete(parentThreadId, taskId, "failed", reason, "stopped");
            }),
        }),
        Effect.retry(backoff),
        Effect.ignoreCause({ log: true }),
        Effect.ensuring(
          Effect.sync(() => {
            activeFollowers.delete(taskId);
            acknowledgedCursors.delete(taskId);
          }),
        ),
      );
    });

  const lifecycle: NonNullable<PeerMcpClient.PeerMcpClient["Service"]["taskLifecycle"]> = (
    environmentId,
    generation,
    input,
  ) =>
    peers.taskLifecycle === undefined
      ? Effect.fail(
          new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Delegated lifecycle v1 is unsupported by this transport.",
          }),
        )
      : peers.taskLifecycle(environmentId, generation, input).pipe(networkSlots.withPermits(1));
  const pairFor = (parentThreadId: ThreadId, task: OrchestrationV2Subagent): PeerReturnPair => ({
    parentEnvironmentId: hereId,
    parentThreadId,
    taskId: task.id,
    childEnvironmentId: task.remoteChild!.environmentId,
    childThreadId: task.remoteChild!.threadId!,
    childRunId: null,
  });
  const observe = (
    parentThreadId: ThreadId,
    taskId: NodeId,
    update: {
      observation?: PeerTaskObservation;
      availability?: "current" | "stale" | "unsupported";
      stop?: typeof PeerTaskStop.Type;
      stopResult?: { readonly status: "cancelled" | "interrupted"; readonly result: string };
    },
  ) =>
    Effect.gen(function* () {
      const attempt = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      yield* threads
        .dispatch({
          type: "delegated_task.remote.observe",
          commandId: CommandId.make(`remote-observation:${taskId}:${attempt}`),
          parentThreadId,
          taskId,
          ...update,
        })
        .pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Unable to persist the delegated lifecycle observation.",
              }),
          ),
        );
    });
  const modernFollow = (
    parentThreadId: ThreadId,
    task: OrchestrationV2Subagent,
    scope: ThreadScope,
    afterSequence: number,
  ) =>
    Effect.gen(function* () {
      const remote = task.remoteChild!;
      const launch = remote.launch!;
      if (remote.stop?.state === "not_started") return true;
      if ((remote.cursor ?? 0) > (acknowledgedCursors.get(task.id) ?? 0)) {
        const ack = yield* lifecycle(remote.environmentId, launch.linkIdentity, {
          pair: pairFor(parentThreadId, task),
          operation: { type: "acknowledge", through: remote.cursor! },
        });
        if (ack.type !== "acknowledged" || ack.through !== remote.cursor)
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The destination did not acknowledge the accepted cursor.",
          });
        acknowledgedCursors.set(task.id, remote.cursor!);
      }
      yield* reconcileRemoteDelegatedTaskQuestions(task, threads.dispatch).pipe(
        Effect.provideService(ProjectionStore.ProjectionStoreV2, projections),
        Effect.provideService(CommandReceipts.CommandReceiptStoreV2, commandReceipts),
      );
      const failedStop =
        remote.stop?.destinationCommandId !== undefined &&
        remote.snapshot?.stopExecution?.commandId === remote.stop.destinationCommandId &&
        remote.snapshot.stopExecution.state === "failed";
      if (
        remote.stop?.state === "pending" ||
        (remote.stop?.state === "unconfirmed" && !failedStop)
      ) {
        const negotiation = yield* requireLifecycle(remote.environmentId, launch.linkIdentity).pipe(
          Effect.result,
        );
        if (negotiation._tag === "Failure") {
          yield* observe(parentThreadId, task.id, {
            stop: { ...remote.stop, state: "unconfirmed" },
            availability: negotiation.failure.code === "invalid_request" ? "unsupported" : "stale",
          });
          return yield* negotiation.failure;
        }
        const stopped = yield* lifecycle(remote.environmentId, launch.linkIdentity, {
          pair: pairFor(parentThreadId, task),
          operation: {
            type: "stop",
            requestId: remote.stop.requestId,
            ...(remote.stop.reason === undefined ? {} : { reason: remote.stop.reason }),
          },
        }).pipe(
          Effect.flatMap((result) =>
            result.type === "stop"
              ? Effect.succeed({ stopCommandId: result.commandId })
              : Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "The destination did not return its Stop receipt.",
                  }),
                ),
          ),
          Effect.result,
        );
        yield* observe(parentThreadId, task.id, {
          stop: {
            ...remote.stop,
            state: stopped._tag === "Success" ? "acknowledged" : "unconfirmed",
            ...(stopped._tag === "Success" && stopped.success.stopCommandId !== undefined
              ? { destinationCommandId: stopped.success.stopCommandId }
              : {}),
          },
        });
        if (stopped._tag === "Failure") return yield* stopped.failure;
      }
      const backpressured =
        task.result !== null &&
        (task.completionDelivery?.state === "pending" ||
          task.completionDelivery?.state === "claimed");
      const stopObservation =
        remote.stop?.destinationCommandId === undefined
          ? {}
          : {
              stopCommandId: remote.stop.destinationCommandId,
              ...(remote.snapshot?.stopExecution?.commandId === remote.stop.destinationCommandId
                ? { afterStopExecution: remote.snapshot.stopExecution.state }
                : {}),
            };
      const sourceChanged = threads
        .streamStoredEventsFrom({ threadId: parentThreadId, afterSequence })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.id === task.id &&
              (stored.event.payload.remoteChild?.stop?.state === "pending" ||
                (backpressured &&
                  stored.event.payload.completionDelivery?.state !==
                    task.completionDelivery?.state)),
          ),
          Stream.runHead,
          Effect.as(undefined),
        );
      const response = yield* lifecycle(remote.environmentId, launch.linkIdentity, {
        pair: pairFor(parentThreadId, task),
        operation: backpressured
          ? {
              type: "snapshot",
              after: remote.snapshot?.sequence ?? 0,
              wait: true,
              ...stopObservation,
            }
          : {
              type: "observe",
              ...stopObservation,
              after: remote.cursor ?? 0,
              afterSnapshot: remote.snapshot?.sequence ?? 0,
              wait: true,
            },
      }).pipe(Effect.raceFirst(sourceChanged));
      if (response === undefined) return false;
      if (response.type !== "observation" && response.type !== "snapshot")
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The destination returned an incompatible lifecycle receipt.",
        });
      let observation: PeerTaskObservation =
        response.type === "observation"
          ? response.observation
          : { cursor: remote.cursor ?? 0, snapshot: response.snapshot };
      if (observation.result?.truncated === true) {
        const result = observation.result;
        let text = result.text;
        let textOffset: number | null = text.length;
        while (textOffset !== null) {
          const page: PeerTaskLifecycleResult | undefined = yield* lifecycle(
            remote.environmentId,
            launch.linkIdentity,
            {
              pair: pairFor(parentThreadId, task),
              operation: {
                type: "resultText",
                eventId: result.eventId,
                runId: result.runId,
                sequence: result.sequence,
                textOffset,
              },
            },
          ).pipe(Effect.raceFirst(sourceChanged));
          if (page === undefined) return false;
          if (
            page.type !== "resultText" ||
            (page.nextTextOffset !== null && page.nextTextOffset <= textOffset)
          )
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The destination returned an invalid result continuation.",
            });
          text += page.text;
          textOffset = page.nextTextOffset;
        }
        observation = { ...observation, result: { ...result, text, truncated: false } };
      }
      if (observation.result !== undefined) {
        yield* threads
          .dispatch({
            type: "delegated_task.remote.complete",
            commandId: CommandId.make(
              `remote-result:${task.id}:${observation.result.eventId}:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
            ),
            parentThreadId,
            taskId: task.id,
            status: observation.result.status,
            result: observation.result.text,
            observation,
          })
          .pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Unable to commit the remote result and its delivery.",
                }),
            ),
          );
      } else if (
        observation.cursor !== remote.cursor ||
        remote.snapshot === undefined ||
        snapshotWithoutTime(observation.snapshot) !== snapshotWithoutTime(remote.snapshot) ||
        remote.availability !== "current"
      ) {
        yield* observe(parentThreadId, task.id, { observation, availability: "current" });
      }
      const accepted = (yield* threads.getThreadRecords(parentThreadId, [
        "subagents",
      ])).subagents.find((candidate) => candidate.id === task.id);
      const stop = accepted?.remoteChild?.stop;
      if (
        stop?.state === "acknowledged" &&
        stop.destinationCommandId !== undefined &&
        accepted?.remoteChild?.snapshot?.stopExecution?.commandId === stop.destinationCommandId &&
        accepted.remoteChild.snapshot.stopExecution.state === "failed"
      )
        yield* observe(parentThreadId, task.id, { stop: { ...stop, state: "unconfirmed" } });
      if (
        stop?.state === "acknowledged" &&
        accepted?.remoteChild?.snapshot?.executionSettled &&
        stop.destinationCommandId !== undefined &&
        accepted.remoteChild.snapshot.stopExecution?.commandId === stop.destinationCommandId &&
        accepted.remoteChild.snapshot.stopExecution.state === "settled"
      ) {
        yield* observe(parentThreadId, task.id, { stop: { ...stop, state: "confirmed" } });
      }
      return false;
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to follow the delegated job.",
            }),
      ),
    );
  const requestStop: RemoteDelegation["Service"]["requestStop"] = (
    parentThreadId,
    taskId,
    requestId,
    reason,
  ) =>
    Effect.gen(function* () {
      const parent = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      if (parent.thread.deletedAt !== null || parent.thread.archivedAt !== null)
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: "The delegated relationship was retired.",
        });
      const task = parent.subagents.find(
        (task) => task.id === taskId && task.origin === "app_owned",
      );
      if (task?.remoteChild?.launch === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: "This delegated job has no verified remote stop target.",
        });
      yield* observe(parentThreadId, taskId, {
        stop: { requestId, state: "pending", ...(reason === undefined ? {} : { reason }) },
        stopResult: {
          status: "cancelled",
          result: reason === undefined ? "Cancelled." : `Cancelled: ${reason}`,
        },
      });
      yield* follow(parentThreadId, taskId).pipe(Effect.forkIn(followers));
      const current = (yield* threads.getThreadRecords(parentThreadId, [
        "subagents",
      ])).subagents.find((candidate) => candidate.id === taskId);
      return {
        taskId,
        status: "cancel_requested",
        stopState: current?.remoteChild?.stop?.state ?? "pending",
      } as const;
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to record the remote stop request.",
            }),
      ),
    );
  const view: RemoteDelegation["Service"]["view"] = (
    parentThreadId,
    taskId,
    after,
    textOffset,
    resource,
  ) =>
    Effect.gen(function* () {
      const parent = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      if (parent.thread.deletedAt !== null || parent.thread.archivedAt !== null)
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: "The delegated relationship was retired.",
        });
      const task = parent.subagents.find(
        (task) => task.id === taskId && task.origin === "app_owned",
      );
      if (task?.remoteChild?.threadId === undefined || task.remoteChild.launch === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: "This delegated job has no verified remote address.",
        });
      const result = yield* lifecycle(
        task.remoteChild.environmentId,
        task.remoteChild.launch.linkIdentity,
        {
          pair: pairFor(parentThreadId, task),
          operation: {
            type: "view",
            after,
            ...(textOffset === undefined ? {} : { textOffset }),
            ...(resource === undefined ? {} : { resource }),
          },
        },
      );
      if (result.type !== "view")
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The remote lifecycle view is unavailable.",
        });
      return result.view;
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to read the remote lifecycle view.",
            }),
      ),
    );

  /**
   * One wait on the task's thread there; true once the task here is done.
   * A task completed here first (cancelled, or its parent stopped) is
   * stopped there, and done once that stop is recorded here.
   */
  const followOnce = (parentThreadId: ThreadId, taskId: NodeId) =>
    Effect.gen(function* () {
      yield* launchPending(parentThreadId, taskId);
      const sequence = yield* threads.getThreadEventSequence(parentThreadId);
      const records = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      const task = records.subagents.find((candidate) => candidate.id === taskId);
      if (
        task?.remoteChild === undefined ||
        records.thread.archivedAt != null ||
        records.thread.deletedAt != null
      )
        return true;
      if (task.remoteChild.threadId === undefined) return task.result !== null;
      const remote = { ...task.remoteChild, threadId: task.remoteChild.threadId };
      const scope = followScope(
        { threadId: parentThreadId, providerInstanceId: records.thread.providerInstanceId },
        remote,
      );
      if (remote.launch?.lifecycleVersion === 1)
        return yield* modernFollow(parentThreadId, task, scope, sequence);
      // Imported #380 launches retain their original read/wait behavior. A new
      // explicit Stop may use pair control only after the actual peer negotiates it.
      if (remote.launch !== undefined && remote.stop !== undefined) {
        const support = yield* requireLifecycle(
          remote.environmentId,
          remote.launch.linkIdentity,
        ).pipe(Effect.result);
        if (support._tag === "Success")
          return yield* modernFollow(parentThreadId, task, scope, sequence);
        yield* observe(parentThreadId, task.id, {
          stop: { ...remote.stop, state: "unconfirmed" },
          availability: support.failure.code === "invalid_request" ? "unsupported" : "stale",
        });
        if (support.failure.code !== "invalid_request") return yield* support.failure;
        return true;
      }

      if (task.result !== null) {
        if (
          (task.status === "cancelled" || task.status === "interrupted") &&
          remote.stoppedThere !== true
        ) {
          const requestId = remote.stop?.requestId ?? `legacy-stop:${task.id}`;
          if (remote.stop === undefined)
            yield* observe(parentThreadId, task.id, { stop: { requestId, state: "pending" } });
          yield* observe(parentThreadId, task.id, {
            stop: { requestId, state: "unconfirmed" },
            availability: "unsupported",
          });
        }
        return true;
      }
      const endedHere = threads
        .streamStoredEventsFrom({
          threadId: parentThreadId,
          afterSequence: sequence,
          eventType: "subagent.updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "subagent.updated" &&
              stored.event.payload.id === taskId &&
              stored.event.payload.result !== null,
          ),
          Stream.runHead,
          Effect.flatMap((ended) =>
            Option.isSome(ended) ? Effect.succeed({ endedHere: true } as const) : Effect.never,
          ),
        );
      const waited = yield* forwarding
        .waitForThread(scope, remote.environmentId, {
          threadId: remote.threadId,
          ...(remote.runId === undefined ? {} : { runId: remote.runId }),
          timeoutMs: FOLLOW_WAIT_MS,
        })
        .pipe(Effect.raceFirst(endedHere));
      // The next look reads the result and stops it there.
      if ("endedHere" in waited) return false;
      if (waited.timedOut) return false;
      // A task recorded without its run waits on the thread's latest; a
      // thread there with none has nothing left to wait for.
      if (waited.runId === null) {
        const reply = yield* lastReply(scope, remote, null);
        yield* complete(
          parentThreadId,
          taskId,
          reply === undefined ? "failed" : "completed",
          reply ?? "The thread in the linked environment has no run to follow.",
          "ran-out",
        );
        return yield* settled(parentThreadId, taskId);
      }
      if (!isTerminal(waited.status)) return false;
      const result = yield* lastReply(scope, remote, waited.runId);
      yield* complete(
        parentThreadId,
        taskId,
        terminalStatus(waited.status),
        result ?? noReply,
        "ran-out",
      );
      return yield* settled(parentThreadId, taskId);
    });

  /**
   * Whether the task is done here after the follower reported its run ran
   * out there. A cancel or Stop that landed while the reply was read keeps
   * its result, and still owes a stop there for what may follow the run;
   * the next look sends it.
   */
  const settled = (parentThreadId: ThreadId, taskId: NodeId) =>
    threads
      .getThreadRecords(parentThreadId, ["subagents"])
      .pipe(
        Effect.map(
          ({ subagents }) =>
            subagents.find((task) => task.id === taskId)?.remoteChild?.stoppedThere === true,
        ),
      );

  const cancel: RemoteDelegation["Service"]["cancel"] = (scope, task, input) =>
    Effect.gen(function* () {
      const remote = task.remoteChild;
      if (remote === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "task_not_found",
          message: `Delegated task ${task.id} is not remote.`,
        });
      }
      const { reason } = input;
      if (remote.launch !== undefined) {
        const key = `cancel:${encodeURIComponent(scope.requestNamespace)}:${input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie))}`;
        return yield* requestStop(task.threadId, task.id, key, reason);
      }
      // Imported records cannot identify a replacement grant. Retain their
      // first result, dispose wakes and expose the unresolved Stop without a remote effect.
      const requestId = `legacy-cancel:${input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie))}`;
      const result = `${reason === undefined ? "Cancelled." : `Cancelled: ${reason}`} The legacy task has no recorded link generation; its destination Stop is unconfirmed.`;
      yield* observe(task.threadId, task.id, {
        stop: { requestId, state: "pending", ...(reason === undefined ? {} : { reason }) },
        stopResult: { status: "cancelled", result },
      });
      yield* observe(task.threadId, task.id, {
        stop: { requestId, state: "unconfirmed", ...(reason === undefined ? {} : { reason }) },
        availability: "unsupported",
      });
      return {
        taskId: task.id,
        status: task.result === null ? "cancel_requested" : terminalStatus(task.status),
        stopState: "unconfirmed",
      } as const;
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorMcpFailure"
          ? error
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to record the delegated stop obligation.",
            }),
      ),
    );

  const start: RemoteDelegation["Service"]["start"] = () =>
    forkParked(
      Effect.gen(function* () {
        const open = yield* projections.getOpenRemoteDelegatedTasks;
        yield* Effect.forEach(
          open,
          ({ parentThreadId, taskId }) =>
            follow(parentThreadId, taskId).pipe(Effect.forkIn(followers)),
          {
            concurrency: "unbounded",
            discard: true,
          },
        );
      }).pipe(Effect.ignoreCause({ log: true })),
    );

  return RemoteDelegation.of({ delegate, cancel, start, view, requestStop });
});

const isTerminal = (status: string) =>
  status === "completed" ||
  status === "failed" ||
  status === "cancelled" ||
  status === "interrupted" ||
  status === "rolled_back";

const terminalStatus = (status: string): "completed" | "failed" | "cancelled" | "interrupted" =>
  status === "completed" || status === "failed" || status === "interrupted" ? status : "cancelled";

export const layer = Layer.effect(RemoteDelegation, make).pipe(
  Layer.provide(CommandReceipts.layer),
);
