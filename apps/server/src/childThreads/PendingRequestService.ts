import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import {
  OrchestratorMcpFailure,
  ThreadId,
  PendingRequestInput,
  PendingRequestContent,
  type ProviderUserInputAnswers,
  type ProviderApprovalDecision,
  type RemotePendingRequestInput,
  type EnvironmentId,
} from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import { PeerAuthority } from "../peer/PeerAuthority.ts";
import * as Crypto from "effect/Crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import {
  readCaller,
  newCommandId,
  dispatchFailure,
  assertTargetWithinLimits,
  unavailable,
} from "../mcp/threadAccess.ts";
import { assertSameLink } from "../mcp/linkOrigin.ts";
import { isCallerAncestor, readPendingRequest } from "./pendingRequests.ts";
import { PeerForwarding } from "../peer/PeerForwarding.ts";
import * as Tools from "../mcp/toolkits/thread/tools.ts";
import { CurrentRequest, CommandIndex, McpRequestReceipts } from "../mcp/McpRequestReceipts.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { PendingClientSession, PendingRequestRelay } from "../peer/PeerAuthority.ts";
import { PeerDispatchAuthority } from "../peer/peerEffectAuthority.ts";
import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";

type Input = typeof PendingRequestInput.Type;
type Action =
  | { readonly type: "read" }
  | {
      readonly type: "respond";
      readonly answers?: ProviderUserInputAnswers | undefined;
      readonly decision?: ProviderApprovalDecision | undefined;
    }
  | { readonly type: "dismiss" };
const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message:
      "Only a verified ancestor or authenticated source client may answer this descendant request.",
  });

export class PendingRequests extends Context.Service<
  PendingRequests,
  {
    readonly read: (
      input: Input,
    ) => Effect.Effect<
      PendingRequestContent,
      OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
    readonly respond: (
      input: Input,
      action: Exclude<Action, { type: "read" }>,
    ) => Effect.Effect<
      { sequence: number },
      OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
    readonly fromTask: (
      sessionId: string,
      input: RemotePendingRequestInput,
    ) => Effect.Effect<PendingRequestContent | { sequence: number }, OrchestratorMcpFailure>;
  }
>()("t3/childThreads/PendingRequestService/PendingRequests") {}

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagementService;
  const crypto = yield* Crypto.Crypto;
  const forwarding = yield* Effect.serviceOption(PeerForwarding);
  const environment = yield* Effect.serviceOption(ServerEnvironment);
  const receipts = yield* Effect.serviceOption(McpRequestReceipts);
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const projections = yield* Effect.serviceOption(ProjectionStoreV2);
  const authority = yield* Effect.serviceOption(PeerAuthority);
  let dependencies = Context.make(ThreadManagementService, threads).pipe(
    Context.add(Crypto.Crypto, crypto),
  );
  if (Option.isSome(sql)) dependencies = Context.add(dependencies, SqlClient.SqlClient, sql.value);
  if (Option.isSome(projections))
    dependencies = Context.add(dependencies, ProjectionStoreV2, projections.value);
  if (Option.isSome(authority))
    dependencies = Context.add(dependencies, PeerAuthority, authority.value);
  // Resolve only real native subagent edges below this root. No foreign local shell.
  const subtree = Effect.fn("PendingRequestService.subtree")(function* (root: ThreadId) {
    const queue = [root];
    const seen = new Set<ThreadId>();
    const tasks: Array<import("@t3tools/contracts").OrchestrationV2Subagent> = [];
    for (const id of queue) {
      if (seen.has(id)) continue;
      if (seen.size >= 256) return yield* denied();
      seen.add(id);
      const records = yield* threads
        .getThreadRecords(id, ["subagents"])
        .pipe(Effect.mapError(unavailable));
      if (records.thread.deletedAt !== null || records.thread.archivedAt !== null) continue;
      for (const task of records.subagents) {
        if (
          task.origin !== "app_owned" ||
          task.completionDelivery?.state === "disposed" ||
          ["cancelled", "interrupted"].includes(task.status)
        )
          continue;
        if (task.childThreadId !== null) {
          const child = yield* threads
            .getThreadShell(task.childThreadId)
            .pipe(Effect.mapError(unavailable));
          if (
            child?.lineage.relationshipToParent === "subagent" &&
            child.lineage.parentThreadId === id
          )
            queue.push(task.childThreadId);
        } else if (task.remoteChild?.threadId !== undefined) tasks.push(task);
      }
    }
    return { seen, tasks };
  });
  const forward = Effect.fn("PendingRequestService.forward")(
    function* (
      scope: Invocation.McpInvocationScope,
      destination: EnvironmentId,
      input: Input,
      action: Action,
      generation: string,
    ) {
      if (Option.isNone(forwarding)) return yield* denied();
      const common = {
        ...input,
        environmentId: destination,
        ...(action.type === "respond"
          ? { answers: action.answers, decision: action.decision }
          : {}),
      };
      const identity = yield* CurrentRequest;
      const parameters = {
        ...common,
        ...(identity === undefined ? {} : { clientRequestId: identity.clientRequestId }),
      };
      if (action.type === "read")
        return yield* forwarding.value.call(
          scope,
          Tools.PendingRequestReadTool,
          destination,
          parameters,
          { expectedLinkIdentity: generation },
        );
      if (action.type === "dismiss")
        return yield* forwarding.value.call(
          scope,
          Tools.PendingRequestDismissTool,
          destination,
          parameters,
          { expectedLinkIdentity: generation },
        );
      return yield* forwarding.value.call(
        scope,
        Tools.PendingRequestRespondTool,
        destination,
        parameters,
        { expectedLinkIdentity: generation },
      );
    },
    Effect.mapError((error) => (error._tag === "OrchestratorMcpFailure" ? error : unavailable())),
  );
  const execute = Effect.fn("PendingRequestService.execute")(function* (
    input: Input,
    action: Action,
  ) {
    const caller = yield* readCaller();
    const root = input.threadId ?? caller.caller?.id;
    if (root === undefined) return yield* denied();
    if (input.requestTarget !== undefined) {
      if (
        (input.requestTarget.environmentId !== caller.scope.environmentId ||
          input.requestTarget.threadId !== root) &&
        caller.caller?.id !== root &&
        !(yield* isCallerAncestor(caller, root))
      )
        return yield* denied();
      const { seen, tasks } = yield* subtree(root);
      const here = caller.scope.environmentId;
      if (input.requestTarget.environmentId !== here) {
        const hop = input.requestRoute?.[0] ?? input.requestTarget;
        const task = tasks.find(
          (task) =>
            task.remoteChild?.environmentId === hop.environmentId &&
            task.remoteChild.threadId === hop.threadId,
        );
        if (task?.remoteChild?.launch === undefined) return yield* denied();
        return yield* forward(
          caller.scope,
          hop.environmentId,
          { ...input, threadId: hop.threadId, requestRoute: input.requestRoute?.slice(1) },
          action,
          task.remoteChild.launch.linkIdentity,
        ).pipe(Effect.provideService(PendingRequestRelay, true));
      }
      if (
        !seen.has(input.requestTarget.threadId) &&
        (Option.isNone(authority) ||
          !(yield* authority.value.pendingWithinTask(
            { environmentId: here, threadId: root },
            input.requestTarget.threadId,
          )))
      )
        return yield* denied();
    }
    const target = input.requestTarget?.threadId ?? root;
    const context = yield* readPendingRequest(
      { ...input, threadId: target },
      action.type !== "read",
    );
    if (context.projection.thread.archivedAt !== null) return yield* denied();
    if (Option.isSome(authority)) {
      // Request contents and responses use the actual destination, including
      // every current contributor, rather than only the selected route root.
      yield* authority.value.checkThread(target);
      yield* authority.value.checkTarget(context.scope, { type: "thread", threadId: target });
    } else if (context.answerer?.kind !== "promachos") {
      yield* assertSameLink(context, context.projection.thread);
    }
    yield* assertTargetWithinLimits(context.limits, context.projection.thread);
    // A thread's own question, or an ordinary local client, keeps local read and
    // answer (readPendingRequest refuses own approvals). A linked client or a child
    // delegated from another environment answers only through qualified ancestry.
    if (
      context.answerer === undefined ||
      (context.answerer.kind === "own" &&
        (action.type === "dismiss" ||
          context.scope.client?.linked === true ||
          context.projection.thread.delegatedFrom !== undefined))
    )
      return yield* denied();
    if (action.type === "read") {
      const { item, revision } = context;
      const common = {
        requestId: input.requestId,
        environmentId: context.scope.environmentId,
        threadId: target,
        status: "pending" as const,
        responseType: context.request.responseCapability.type,
        ...(context.request.responseCapability.type === "not_resumable"
          ? { unavailableReason: context.request.responseCapability.reason }
          : {}),
        ...(revision === undefined ? {} : { revision }),
      };
      return item.type === "user_input_request"
        ? { ...common, kind: "user-input" as const, questions: item.questions }
        : {
            ...common,
            kind: "approval" as const,
            requestKind: item.requestKind,
            ...(item.prompt === undefined ? {} : { prompt: item.prompt }),
            ...(item.appName === undefined ? {} : { appName: item.appName }),
            ...(item.options === undefined ? {} : { options: item.options }),
          };
    }
    if (context.scope.client?.linked === true && input.expectedRevision === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Read the qualified request and pass expectedRevision before answering.",
      });
    if (
      action.type === "respond" &&
      ((context.item.type === "approval_request" &&
        (action.decision === undefined || action.answers !== undefined)) ||
        (context.item.type === "user_input_request" &&
          (action.answers === undefined || action.decision !== undefined)))
    )
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Pass only answers for a question or decision for an approval.",
      });
    const result = yield* threads
      .dispatch({
        type: action.type === "dismiss" ? "thread.user-input.dismiss" : "runtime-request.respond",
        threadId: target,
        commandId: yield* newCommandId(),
        requestId: input.requestId,
        ...(action.type === "respond" && context.answerer.kind === "promachos"
          ? { answeredBy: context.answerer.answeredBy }
          : {}),
        ...(input.expectedRevision === undefined
          ? {}
          : { expectedRevision: input.expectedRevision }),
        ...(action.type === "respond"
          ? { answers: action.answers, decision: action.decision }
          : {}),
      })
      .pipe(Effect.mapError(dispatchFailure));
    return { sequence: result.sequence };
  }, Effect.provide(dependencies));
  const fromTask: PendingRequests["Service"]["fromTask"] = Effect.fn(
    "PendingRequestService.fromTask",
  )(
    function* (sessionId, input) {
      const parent = yield* threads
        .getThreadRecords(input.parentThreadId, ["subagents"])
        .pipe(Effect.mapError(unavailable));
      const task = parent.subagents.find(
        (task) => task.id === input.taskId && task.origin === "app_owned",
      );
      if (
        parent.thread.deletedAt !== null ||
        parent.thread.archivedAt !== null ||
        task?.remoteChild?.threadId === undefined ||
        task.remoteChild.launch?.lifecycleVersion !== 1 ||
        task.completionDelivery?.state === "disposed" ||
        task.remoteChild.stop !== undefined ||
        ["cancelled", "interrupted"].includes(task.status)
      )
        return yield* denied();
      if (Option.isNone(environment)) return yield* denied();
      const here = yield* environment.value.getEnvironmentId;
      const scope: Invocation.McpInvocationScope = {
        environmentId: here,
        requestNamespace: `client:${sessionId}`,
        issuedAt: 0,
        thread: undefined,
        client: { sessionId, label: "Authenticated source client", access: "full-access" },
        capabilities: new Set(["orchestration"]),
      };
      const operation = input.operation;
      const action: Action =
        operation.type === "respond"
          ? { type: "respond", answers: operation.answers, decision: operation.decision }
          : { type: operation.type };
      let perform = forward(
        scope,
        task.remoteChild.environmentId,
        {
          threadId: task.remoteChild.threadId,
          requestId: input.requestId,
          requestTarget: input.requestTarget,
          requestRoute: input.requestRoute,
          ...(operation.type === "read" ? {} : { expectedRevision: operation.expectedRevision }),
        },
        action,
        task.remoteChild.launch.linkIdentity,
      );
      // A later descendant can return to this environment. Resolve it locally
      // after proving the selected task's actual pair ancestry, rather than
      // routing A -> B -> A through a generic transport loop.
      if (input.requestTarget.environmentId === here) {
        if (
          Option.isNone(authority) ||
          !(yield* authority.value.pendingWithinTask(
            { environmentId: task.remoteChild.environmentId, threadId: task.remoteChild.threadId },
            input.requestTarget.threadId,
          ))
        )
          return yield* denied();
        const owner = yield* authority.value.checkThread(input.requestTarget.threadId);
        if (owner === undefined) return yield* denied();
        const target = yield* threads
          .getThreadShell(input.requestTarget.threadId)
          .pipe(Effect.mapError(unavailable));
        if (target === null) return yield* denied();
        yield* assertTargetWithinLimits(owner.ceiling, target);
        perform = execute(
          {
            threadId: target.id,
            requestId: input.requestId,
            ...(operation.type === "read" ? {} : { expectedRevision: operation.expectedRevision }),
          },
          action,
        ).pipe(
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provideService(PeerDispatchAuthority, owner.id),
          Effect.provideService(DispatchModeLimit, owner.ceiling),
        );
      }
      const client = { sessionId, readOnly: operation.type === "read" };
      if (operation.type === "read")
        return yield* perform.pipe(Effect.provideService(PendingClientSession, client));
      if (Option.isNone(receipts)) return yield* denied();
      const identity = yield* receipts.value.identify(
        scope,
        "remoteTasks.request",
        input,
        operation.clientRequestId,
      );
      return yield* perform.pipe(
        Effect.provideService(PendingClientSession, client),
        Effect.provideService(CurrentRequest, identity),
        Effect.provideService(CommandIndex, yield* Ref.make(0)),
      );
    },
    Effect.mapError((error) => (error._tag === "OrchestratorMcpFailure" ? error : unavailable())),
  );
  const read: PendingRequests["Service"]["read"] = (input) =>
    execute(input, { type: "read" }).pipe(
      Effect.flatMap((result) =>
        "kind" in result ? Effect.succeed(result) : Effect.fail(unavailable()),
      ),
    );
  const respond: PendingRequests["Service"]["respond"] = (input, action) =>
    execute(input, action).pipe(
      Effect.flatMap((result) =>
        "sequence" in result ? Effect.succeed(result) : Effect.fail(unavailable()),
      ),
    );
  return PendingRequests.of({ read, respond, fromTask });
});
export const layer = Layer.effect(PendingRequests, make);
