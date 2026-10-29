import { CurrentRequest, externalDispatch, externalReplay } from "./McpRequestReceipts.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import {
  CommandId,
  OrchestratorMcpFailure,
  MessageId,
  type ProjectId,
  WorktreeMcpFailure,
  type WorktreeMcpContinuationStatus,
  type WorktreeMcpHandoffInput,
  type WorktreeMcpHandoffResult,
  type WorktreeMcpSetupScriptStatus,
  type WorktreeMcpStatusResult,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "./McpInvocationContext.ts";

export class WorktreeMcpService extends Context.Service<
  WorktreeMcpService,
  {
    readonly handoff: (
      scope: McpInvocationScope,
      input: WorktreeMcpHandoffInput,
    ) => Effect.Effect<WorktreeMcpHandoffResult, WorktreeMcpFailure | OrchestratorMcpFailure>;
    readonly status: (
      scope: McpInvocationScope,
    ) => Effect.Effect<WorktreeMcpStatusResult, WorktreeMcpFailure>;
  }
>()("t3/mcp/WorktreeMcpService") {}

function failure(code: WorktreeMcpFailure["code"], message: string): WorktreeMcpFailure {
  return new WorktreeMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
}

const asOperationFailed = (prefix: string) =>
  Effect.mapError((error: unknown) =>
    failure("operation_failed", `${prefix}: ${errorMessage(error)}`),
  );

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const nativeReceipts = yield* Effect.serviceOption(CommandReceipts.CommandReceiptStoreV2);
  const _path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;

  // Serialize handoffs per thread to avoid creating two worktrees and orphaning one.
  const handoffThreadsInFlight = new Set<string>();

  const requireThreadScope = (scope: McpInvocationScope) =>
    scope.thread === undefined
      ? Effect.fail(
          failure(
            "thread_credential_required",
            "Worktree handoff and status act as the calling T3 thread, so they need an agent running inside T3 Code.",
          ),
        )
      : Effect.succeed(scope as McpThreadInvocationScope);

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("worktree")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP credential does not grant worktree capabilities."),
        );

  const loadThread = (scope: McpThreadInvocationScope) =>
    threadManagement.getThreadRecords(scope.thread.threadId, []).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorProjectionError"
          ? failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`)
          : failure(
              "operation_failed",
              `Unable to read thread ${scope.thread.threadId}: ${errorMessage(error)}`,
            ),
      ),
      Effect.filterOrFail(
        (projection) => projection.thread.deletedAt === null,
        () => failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`),
      ),
    );

  const loadProject = (scope: McpThreadInvocationScope, projectId: ProjectId) =>
    projects.getById(projectId).pipe(
      asOperationFailed(`Unable to read project ${projectId}`),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              failure(
                "project_not_found",
                `Project '${projectId}' was not found for thread '${scope.thread.threadId}'.`,
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const readDefaultStartFromOrigin = serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.newWorktreesStartFromOrigin),
    asOperationFailed("Unable to read server settings"),
  );

  const handoffIds = Effect.fn("WorktreeMcpService.handoffIds")(function* (
    scope: McpThreadInvocationScope,
  ) {
    const request = yield* CurrentRequest;
    if (request !== undefined)
      return {
        commandId: CommandId.make(`worktree-handoff:${request.id}`),
        continuationCommandId: CommandId.make(`worktree-continuation:${request.id}`),
        continuationMessageId: MessageId.make(`worktree-continuation:${request.id}`),
      };
    return yield* crypto.randomUUIDv4().pipe(
      Effect.map((uuid) => {
        const part = (kind: string, operation: string) =>
          [kind, "mcp", encodeURIComponent(scope.thread.providerSessionId), operation, uuid].join(
            ":",
          );
        return {
          commandId: CommandId.make(part("command", "worktree-handoff")),
          continuationCommandId: CommandId.make(part("command", "worktree-continuation")),
          continuationMessageId: MessageId.make(part("message", "worktree-continuation")),
        };
      }),
      Effect.orDie,
    );
  });

  const performHandoff = Effect.fn("WorktreeMcpService.performHandoff")(function* (
    scope: McpThreadInvocationScope,
    input: WorktreeMcpHandoffInput,
  ) {
    const request = yield* CurrentRequest;
    const alreadyInWorktree = (worktreePath: string) =>
      failure(
        "already_in_worktree",
        `Thread '${scope.thread.threadId}' is already attached to worktree '${worktreePath}'.`,
      );

    const projection = yield* loadThread(scope);
    if (projection.thread.worktreePath !== null) {
      return yield* alreadyInWorktree(projection.thread.worktreePath);
    }
    if (projection.thread.archivedAt !== null) {
      return yield* failure(
        "invalid_request",
        `Thread '${scope.thread.threadId}' is archived and cannot be handed off to a worktree.`,
      );
    }

    const project = yield* loadProject(scope, projection.thread.projectId);
    const projectCwd = project.workspaceRoot;

    const localStatus = yield* gitWorkflow
      .localStatus({ cwd: projectCwd })
      .pipe(asOperationFailed("Unable to read git status"));
    if (!localStatus.isRepo) {
      return yield* failure(
        "invalid_request",
        `Project workspace '${projectCwd}' is not a git repository.`,
      );
    }

    const localBranchNames = yield* gitWorkflow
      .listLocalBranchNames(projectCwd)
      .pipe(asOperationFailed("Unable to list branches"));
    if (localBranchNames.includes(input.branch)) {
      const existingRef = yield* gitWorkflow
        .listRefs({ cwd: projectCwd, query: input.branch, refKind: "local" })
        .pipe(
          Effect.map((result) =>
            result.refs.find((ref) => ref.name === input.branch && ref.isRemote !== true),
          ),
          Effect.orElseSucceed(() => undefined),
        );
      const checkoutPath = existingRef?.worktreePath ?? null;
      return yield* failure(
        "invalid_request",
        `Branch '${input.branch}' already exists${
          checkoutPath === null ? "" : ` and is checked out at '${checkoutPath}'`
        }. Choose a different branch name, or delete the existing branch${
          checkoutPath === null ? "" : " and its worktree"
        } first.`,
      );
    }

    let baseRef = input.baseRef;
    if (baseRef === undefined) {
      if (localStatus.refName === null) {
        return yield* failure(
          "invalid_request",
          "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
        );
      }
      baseRef = localStatus.refName;
    }

    const startFromOrigin = input.startFromOrigin ?? (yield* readDefaultStartFromOrigin);

    const dispatch = Effect.gen(function* () {
      let worktreeBaseRef = baseRef;
      let baseRefName = baseRef;
      let startedFromOrigin = false;
      if (startFromOrigin) {
        const resolvedRemoteBase = yield* gitWorkflow
          .fetchWorktreeBaseCommit({ cwd: projectCwd, refName: baseRef })
          .pipe(asOperationFailed(`Unable to fetch the remote base of '${baseRef}'`));
        if (resolvedRemoteBase !== null) {
          worktreeBaseRef = resolvedRemoteBase.commitSha;
          baseRefName = `refs/remotes/${resolvedRemoteBase.remoteRefName}`;
          startedFromOrigin = true;
        }
      }

      const ids = yield* handoffIds(scope);

      // Mask interruption after worktree creation through setup; binding can detach and interrupt the caller.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const worktree = yield* restore(
            gitWorkflow
              .createWorktree({
                cwd: projectCwd,
                refName: worktreeBaseRef,
                newRefName: input.branch,
                baseRefName,
                path: null,
              })
              .pipe(asOperationFailed("Unable to create the worktree")),
          );
          const worktreePath = worktree.worktree.path;

          // Shared shape for "the handoff already succeeded, so report the failure
          // in the result instead of failing the call" (continuation, setup script).
          const reportFailed = (logMessage: string) =>
            Effect.catchCause((cause: Cause.Cause<unknown>) => {
              const detail = errorMessage(Cause.squash(cause));
              return Effect.logWarning(logMessage, {
                threadId: scope.thread.threadId,
                worktreePath,
                detail,
              }).pipe(Effect.as({ status: "failed", detail } as const));
            });

          // suspend: build the rollback only if cleanup actually runs. Removing
          // the worktree must succeed before deleting its freshly created branch;
          // otherwise the branch may still be checked out there.
          const removeCreatedWorktree = Effect.suspend(() =>
            gitWorkflow.removeWorktree({ cwd: projectCwd, path: worktreePath, force: true }).pipe(
              Effect.andThen(
                Effect.suspend(() =>
                  gitWorkflow.deleteLocalBranch({
                    cwd: projectCwd,
                    refName: worktree.worktree.refName,
                    force: true,
                  }),
                ),
              ),
            ),
          ).pipe(Effect.ignoreCause({ log: true }));

          let bindingDispatched = false;
          const recheckAndBind = Effect.gen(function* () {
            // The projection was read before the potentially slow git work
            // above; a concurrent binding (for example from the UI) could have
            // attached the thread in the meantime. Re-check before committing so
            // the race cannot leave a second, untracked worktree.
            const recheck = yield* loadThread(scope);
            if (recheck.thread.worktreePath !== null) {
              return yield* alreadyInWorktree(recheck.thread.worktreePath);
            }
            // Mirror the up-front archived check: the thread may have been
            // archived during the slow git work, and an archived thread must
            // not be bound to a fresh worktree it can never use.
            if (recheck.thread.archivedAt !== null) {
              return yield* failure(
                "invalid_request",
                `Thread '${scope.thread.threadId}' was archived while the worktree was being created; the handoff was rolled back.`,
              );
            }
            bindingDispatched = true;
            yield* threadManagement
              .dispatch({
                type: "thread.metadata.update",
                commandId: ids.commandId,
                threadId: scope.thread.threadId,
                branch: worktree.worktree.refName,
                worktreePath,
                expectedWorktreePath: null,
              })
              .pipe(
                Effect.catchCause((cause) =>
                  // Interrupt-only causes propagate unchanged: whether the
                  // dispatch committed is unknown, so neither a typed failure
                  // nor a rollback would be correct. Failures and defects
                  // (including mixed causes) map to a typed operation_failed.
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.failCause(cause as Cause.Cause<never>)
                    : Effect.fail(
                        failure(
                          "operation_failed",
                          `Unable to re-point the thread at the worktree: ${errorMessage(Cause.squash(cause))}`,
                        ),
                      ),
                ),
              );
          }).pipe(
            // onError: the worktree exists, so any failure before the binding commits removes it again.
            // Interrupt-only causes skip removal: the binding may have committed, and deleting a worktree the
            // thread now uses is worse than leaking one.
            Effect.onError((cause) =>
              Cause.hasInterruptsOnly(cause)
                ? Effect.void
                : request === undefined || !bindingDispatched
                  ? removeCreatedWorktree
                  : Option.isNone(nativeReceipts)
                    ? Effect.void
                    : nativeReceipts.value.getByCommandId(ids.commandId).pipe(
                        Effect.flatMap((receipt) =>
                          Option.isSome(receipt) && receipt.value.status === "rejected"
                            ? removeCreatedWorktree
                            : Effect.void,
                        ),
                        Effect.ignore,
                      ),
            ),
          );

          // Queue the continuation right after the binding commits: the scheduled detach kills this session,
          // and the durably queued message relaunches the thread in the worktree once the run ends. Suspend
          // builds the send only after the binding succeeded.
          const queueContinuation: Effect.Effect<
            WorktreeMcpContinuationStatus,
            WorktreeMcpFailure
          > = Effect.suspend(() =>
            input.continuationPrompt === undefined
              ? Effect.succeed<WorktreeMcpContinuationStatus>({ status: "skipped" })
              : threadManagement
                  .sendToThread({
                    projectId: projection.thread.projectId,
                    commandId: ids.continuationCommandId,
                    threadId: scope.thread.threadId,
                    messageId: ids.continuationMessageId,
                    text: input.continuationPrompt,
                    attachments: [],
                    mode: "queue",
                    createdBy: "agent",
                    creationSource: "mcp",
                  })
                  .pipe(
                    Effect.map((sendResult): WorktreeMcpContinuationStatus => ({
                      status: "scheduled",
                      delivery: sendResult.delivery,
                    })),
                    reportFailed("worktree handoff continuation failed to queue"),
                  ),
          );

          const continuation = yield* recheckAndBind.pipe(Effect.andThen(queueContinuation));

          yield* vcsStatusBroadcaster
            .refreshStatus(worktreePath)
            .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach);

          let setupScript: WorktreeMcpSetupScriptStatus = { status: "skipped" };
          if (input.runSetupScript ?? true) {
            setupScript = yield* setupScriptRunner
              .runForThread({
                threadId: scope.thread.threadId,
                projectId: projection.thread.projectId,
                projectCwd,
                worktreePath,
                project: {
                  id: project.id,
                  workspaceRoot: project.workspaceRoot,
                  scripts: project.scripts,
                },
              })
              .pipe(
                Effect.map((result): WorktreeMcpSetupScriptStatus =>
                  result.status === "started"
                    ? {
                        status: "started",
                        scriptName: result.scriptName,
                        terminalId: result.terminalId,
                      }
                    : result.status === "skipped-for-link"
                      ? { status: "skipped" }
                      : { status: "no-script" },
                ),
                // catchCause via reportFailed: the thread is already re-pointed at the
                // worktree, so even a defect in the setup runner must not fail the handoff.
                reportFailed("worktree handoff setup script failed"),
              );
          }

          const result: WorktreeMcpHandoffResult = {
            worktreePath,
            branch: worktree.worktree.refName,
            baseRef,
            startedFromOrigin,
            setupScript,
            continuation,
            note:
              continuation.status === "scheduled"
                ? "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the queued continuation prompt then starts the next turn inside the worktree with the conversation preserved. The worktree is not removed automatically when the thread is deleted."
                : "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the conversation continues inside the worktree when the thread receives its next message. Pass continuationPrompt to resume automatically. The worktree is not removed automatically when the thread is deleted.",
          };
          return result;
        }),
      );
    });
    return yield* externalDispatch(dispatch);
  });

  const handoff: WorktreeMcpService["Service"]["handoff"] = Effect.fn("WorktreeMcpService.handoff")(
    function* (callerScope, input) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      // Re-authorize the actual resource before returning historical acceptance.
      if ((yield* CurrentRequest) !== undefined) {
        if (Option.isNone(nativeReceipts))
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "Native worktree receipts are unavailable; no new handoff was authorized.",
          });
        yield* loadThread(scope);
      }
      const previous = yield* externalReplay<WorktreeMcpHandoffResult>().pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            if (error.code !== "outcome_unknown" || Option.isNone(nativeReceipts))
              return yield* error;
            const request = yield* CurrentRequest;
            if (request === undefined) return yield* error;
            const receipt = yield* nativeReceipts.value
              .getByCommandId(CommandId.make(`worktree-handoff:${request.id}`))
              .pipe(Effect.catch(() => Effect.succeedNone));
            if (
              Option.isSome(receipt) &&
              receipt.value.status === "accepted" &&
              receipt.value.threadId === scope.thread.threadId &&
              receipt.value.commandType === "thread.metadata.update"
            )
              return yield* new OrchestratorMcpFailure({
                code: "outcome_unknown",
                requestId: request.clientRequestId,
                lastConfirmedStage: "native_binding_accepted",
                message:
                  "The native worktree binding was accepted; continuation or setup completion is unresolved. This request will not create another worktree or repeat setup.",
              });
            return yield* error;
          }),
        ),
      );
      if (Option.isSome(previous)) return previous.value;
      // uninterruptibleMask: acquire the guard and register its releasing finalizer with no gap, or an
      // interrupt between them leaks the entry and blocks every handoff for this thread until restart.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          if (handoffThreadsInFlight.has(scope.thread.threadId)) {
            return Effect.fail(
              failure(
                "handoff_in_progress",
                `A worktree handoff is already in progress for thread '${scope.thread.threadId}'.`,
              ),
            );
          }
          handoffThreadsInFlight.add(scope.thread.threadId);
          return restore(performHandoff(scope, input)).pipe(
            Effect.ensuring(
              Effect.sync(() => handoffThreadsInFlight.delete(scope.thread.threadId)),
            ),
          );
        }),
      );
    },
  );

  const status: WorktreeMcpService["Service"]["status"] = Effect.fn("WorktreeMcpService.status")(
    function* (callerScope) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      const projection = yield* loadThread(scope);
      const project = yield* loadProject(scope, projection.thread.projectId);

      const defaultStartFromOrigin = yield* readDefaultStartFromOrigin;

      const result: WorktreeMcpStatusResult = {
        attached: projection.thread.worktreePath !== null,
        worktreePath: projection.thread.worktreePath,
        branch: projection.thread.branch,
        projectWorkspaceRoot: project.workspaceRoot,
        defaultStartFromOrigin,
      };
      return result;
    },
  );

  return WorktreeMcpService.of({ handoff, status });
});

export const layer: Layer.Layer<
  WorktreeMcpService,
  never,
  | Crypto.Crypto
  | Path.Path
  | ThreadManagementService.ThreadManagementService
  | ProjectService.ProjectService
  | ServerSettings.ServerSettingsService
  | GitWorkflowService.GitWorkflowService
  | ProjectSetupScriptRunner.ProjectSetupScriptRunner
  | VcsStatusBroadcaster.VcsStatusBroadcaster
> = Layer.effect(WorktreeMcpService, make);
