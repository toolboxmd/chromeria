import {
  CommandId,
  EventId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationSession,
  DEFAULT_PRISM_LANE,
  DEFAULT_RUNTIME_MODE,
  type OrchestrationThreadShell,
  PRISM_ROLE_LABELS,
  prismRoleModels,
  type ProviderOptionSelection,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "../../../provider/Services/ProviderRegistry.ts";
import * as ProviderService from "../../../provider/Services/ProviderService.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { pickRoleModel, prismRoleSuffix, roleTaskMessage } from "./roles.ts";
import {
  type ChildReportState,
  childReportStatesFrom,
  unrecordedChildReportState,
} from "./childReportState.ts";
import { isSubagentThreadId, makeSubagentThreadId, parentThreadIdOf } from "./subagentThreadId.ts";
import {
  isRouterJobMessage,
  RESUME_TEXT,
  resumeAfterUsageLimitReset,
  resumeNotice,
} from "./usageLimitResume.ts";
import {
  type SubagentStatus,
  type ThreadScope,
  ThreadsToolError,
  ThreadsToolkit,
} from "./tools.ts";

const REPORT_TEXT_LIMIT = 4_000;
/** How long interrupt_thread waits for the turn to settle before returning interrupt_requested. */
export const INTERRUPT_SETTLE_TIMEOUT = Duration.seconds(30);
/** How long interrupt_thread waits for a starting thread to report its turn id. */
export const INTERRUPT_TURN_START_TIMEOUT = Duration.seconds(30);
type ThreadScopeIdentity = { readonly id: string; readonly projectId: string };
type ThreadLifecycle = ThreadScopeIdentity & {
  readonly archivedAt: string | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
};

/** The toolkit's coarse status vocabulary for a thread's provider session. */
export function subagentStatusOf(session: OrchestrationSession | null): SubagentStatus {
  switch (session?.status) {
    case undefined:
    case "starting":
      return "starting";
    case "running":
      return "running";
    case "idle":
    case "ready":
      return "idle";
    case "error":
      return "failed";
    case "interrupted":
    case "stopped":
      return "stopped";
  }
}

/**
 * Provider option id that carries reasoning effort, per driver. Codex and
 * Grok advertise `reasoningEffort`, OpenCode advertises `variant`, and
 * Claude, Cursor and Antigravity read `effort`. ModelSelection options are
 * free-form id/value pairs that adapters ignore when unknown, so sending
 * `effort` to Antigravity (which has no effort control) is a harmless no-op.
 */
export function effortOptionId(driverKind: string): string {
  switch (driverKind) {
    case "codex":
    case "grok":
      return "reasoningEffort";
    case "opencode":
      return "variant";
    default:
      return "effort";
  }
}

const fail = (reason: string) => Effect.fail(new ThreadsToolError({ reason }));

/**
 * How a message to a child is delivered. A message sent while the child
 * works steers its running turn (a new turn supersedes the running one at
 * the orchestration layer, uniformly for every provider); an idle, failed
 * or stopped child starts a fresh turn. Callers must refuse `starting`
 * before reaching here: a message sent before the first turn starts left a
 * turn open forever in live runs.
 */
export function deliveryOf(statusBefore: SubagentStatus): "new-turn" | "steer" {
  return statusBefore === "running" ? "steer" : "new-turn";
}

/** Whether a thread has a turn interrupt_thread can stop: one running or still starting. */
function hasActiveTurn(session: OrchestrationSession | null): boolean {
  return session?.status === "running" || session?.status === "starting";
}

export function isSettled(thread: Pick<ThreadLifecycle, "settledOverride" | "settledAt">) {
  return thread.settledOverride === "settled" || thread.settledAt !== null;
}

export function threadIsInScope(
  thread: ThreadScopeIdentity,
  caller: ThreadScopeIdentity,
  scope: ThreadScope,
) {
  return scope === "project"
    ? thread.projectId === caller.projectId
    : parentThreadIdOf(thread.id) === caller.id;
}

export function threadShouldBeListed(
  thread: ThreadLifecycle,
  caller: ThreadScopeIdentity,
  scope: ThreadScope,
  includeSettled: boolean,
) {
  return (
    thread.archivedAt === null &&
    (includeSettled || !isSettled(thread)) &&
    threadIsInScope(thread, caller, scope)
  );
}

export function scopeRefusal(threadId: string, scope: ThreadScope) {
  return scope === "children"
    ? `Thread ${threadId} is not in scope: children. Use scope: "project" to access threads in this project.`
    : `Thread ${threadId} is outside scope: project.`;
}

export function attributedMessage(
  text: string,
  caller: { readonly id: string; readonly title: string },
  target: Pick<ThreadScopeIdentity, "id">,
) {
  return parentThreadIdOf(target.id) === caller.id
    ? text
    : `[Message from ${caller.title} (thread ${caller.id})]\n\n${text}`;
}

const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providers = yield* ProviderService.ProviderService;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettingsService;
  const crypto = yield* Crypto.Crypto;

  /** Child thread id -> whether its turn results go back to the parent.
   * This and `lastReported` are also written to the parent's task.* rows and
   * restored from all of them after a restart (see childReportState.ts). */
  const reportBack = new Map<string, boolean>();
  /** Child thread id -> last status the parent's Agents panel was told. */
  const lastStatus = new Map<string, SubagentStatus>();
  /** Child thread id -> assistant message id last reported to the parent. */
  const lastReported = new Map<string, string>();
  /** Child thread id -> whether its newest parent row before this process recorded it idle. */
  const recordedIdle = new Map<string, boolean>();

  const uuid = crypto.randomUUIDv4.pipe(Effect.orDie);
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const commandId = (tag: string) =>
    Effect.map(uuid, (id) => CommandId.make(`server:mcp-threads-${tag}:${id}`));

  const dispatch = (command: Parameters<typeof engine.dispatch>[0]) =>
    engine
      .dispatch(command)
      .pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause as Cause.Cause<never>)
            : fail(`Command ${command.type} failed: ${Cause.pretty(cause).slice(0, 500)}`),
        ),
      );

  const threadShell = (threadId: string) =>
    snapshots.getThreadShellById(ThreadId.make(threadId)).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.catchCause(() => Effect.succeed(undefined)),
    );

  const lastAssistantMessage = (threadId: string) =>
    snapshots.getThreadDetailById(ThreadId.make(threadId)).pipe(
      Effect.map((thread) => {
        if (Option.isNone(thread)) return null;
        const messages = thread.value.messages;
        const userMessageCount = messages.filter((message) => message.role === "user").length;
        for (let index = messages.length - 1; index >= 0; index -= 1) {
          const message = messages[index]!;
          if (message.role === "assistant" && message.text.trim().length > 0) {
            return { id: message.id, text: message.text, userMessageCount };
          }
        }
        return { id: null, text: null, userMessageCount };
      }),
      Effect.catchCause(() => Effect.succeed(null)),
    );

  const remember = (childId: string, state: ChildReportState) => {
    reportBack.set(childId, state.reportBack);
    if (state.lastReported !== null && !lastReported.has(childId)) {
      lastReported.set(childId, state.lastReported);
    }
  };

  /**
   * Every child's report state from every parent's task rows, read once per
   * process. Reading by kind skips the thread detail's activity window, so
   * a child keeps its setting however many parent activities followed.
   */
  const restoreAllReportStates = yield* Effect.cached(
    Effect.gen(function* () {
      const rows = yield* Effect.forEach(
        ["task.started", "task.progress", "task.updated"],
        (kind) =>
          snapshots.listActivitiesByKind(kind).pipe(Effect.catchCause(() => Effect.succeed([]))),
      );
      const chronological = rows
        .flat()
        .toSorted((left, right) => left.createdAt.localeCompare(right.createdAt));
      for (const [id, state] of childReportStatesFrom(chronological)) {
        if (!reportBack.has(id)) remember(id, state);
      }
      for (const row of chronological) {
        const payload = row.payload as { taskId?: unknown; status?: unknown } | null;
        if (typeof payload?.taskId !== "string" || !isSubagentThreadId(payload.taskId)) continue;
        recordedIdle.set(payload.taskId, row.kind === "task.progress" && payload.status === "idle");
      }
    }),
  );

  /** Restores report state after a restart; a child with no row keeps its current reply as reported. */
  const restoreReportState = (childId: string) =>
    Effect.gen(function* () {
      if (reportBack.has(childId)) return;
      yield* restoreAllReportStates;
      if (reportBack.has(childId)) return;
      const last = yield* lastAssistantMessage(childId);
      remember(childId, unrecordedChildReportState(last?.id ?? null));
    });

  const summarize = (thread: OrchestrationThreadShell) =>
    Effect.gen(function* () {
      const last = yield* lastAssistantMessage(thread.id);
      return {
        threadId: thread.id,
        id: thread.id,
        title: thread.title,
        status: subagentStatusOf(thread.session),
        instanceId: thread.session?.providerInstanceId ?? thread.modelSelection.instanceId,
        provider: thread.session?.providerInstanceId ?? thread.modelSelection.instanceId,
        model: thread.modelSelection.model,
        parentId: parentThreadIdOf(thread.id),
        lastError: thread.session?.lastError ?? null,
        lastAssistantMessage: last?.text ?? null,
        userMessageCount: last?.userMessageCount ?? 0,
      };
    });

  /** Prism role kits as resolved for a project. */
  const roleKits = (projectId: OrchestrationThreadShell["projectId"]) =>
    serverSettings.getSettings.pipe(
      Effect.map((settings) => resolveProjectSettings(settings, projectId).settings.prismRoles),
      Effect.catchCause(() => fail("Could not read Prism role settings.")),
    );

  /** The calling thread. Every role may use every thread tool. */
  const callingThread = Effect.gen(function* () {
    const invocation = yield* McpInvocationContext.McpInvocationContext;
    const caller = yield* threadShell(invocation.threadId);
    if (!caller) return yield* fail(`Thread ${invocation.threadId} was not found.`);
    return caller;
  });

  /** The calling thread, plus a guard that the target is in the requested scope. */
  const callerScopedThread = (threadId: string, scope: ThreadScope) =>
    Effect.gen(function* () {
      const caller = yield* callingThread;
      const target = yield* threadShell(threadId);
      if (!target) return yield* fail(`Thread ${threadId} was not found.`);
      if (!threadIsInScope(target, caller, scope))
        return yield* fail(scopeRefusal(threadId, scope));
      return { caller, target };
    });

  const listThreads = (scope: ThreadScope, includeSettled: boolean) =>
    Effect.gen(function* () {
      const caller = yield* callingThread;
      const shells = yield* snapshots.getShellSnapshot().pipe(
        Effect.map((snapshot) => snapshot.threads),
        Effect.catchCause(() => fail("Could not read threads.")),
      );
      const threads = shells.filter((thread) =>
        threadShouldBeListed(thread, caller, scope, includeSettled),
      );
      return { threads: yield* Effect.forEach(threads, summarize) };
    });

  const listChildThreads = () =>
    Effect.gen(function* () {
      const caller = yield* callingThread;
      const shells = yield* snapshots.getShellSnapshot().pipe(
        Effect.map((snapshot) => snapshot.threads),
        Effect.catchCause(() => fail("Could not read threads.")),
      );
      const children = shells.filter((thread) => threadIsInScope(thread, caller, "children"));
      return { threads: yield* Effect.forEach(children, summarize) };
    });

  const appendParentActivity = (
    parentThreadId: string,
    kind: "task.started" | "task.progress" | "task.updated" | "task.completed",
    summary: string,
    payload: Record<string, unknown>,
  ) =>
    Effect.gen(function* () {
      const createdAt = yield* nowIso;
      yield* dispatch({
        type: "thread.activity.append",
        commandId: yield* commandId("activity"),
        threadId: ThreadId.make(parentThreadId),
        activity: {
          id: EventId.make(yield* uuid),
          tone: "info",
          kind,
          summary,
          // agentKind "agent" is what admits a task row to the Agents panel.
          payload: { agentKind: "agent", taskType: "t3_thread", ...payload },
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    });

  const startTurn = (thread: OrchestrationThreadShell, text: string, turnCommandId?: CommandId) =>
    Effect.gen(function* () {
      const createdAt = yield* nowIso;
      yield* dispatch({
        type: "thread.turn.start",
        commandId: turnCommandId ?? (yield* commandId("turn")),
        threadId: thread.id,
        message: {
          messageId: MessageId.make(yield* uuid),
          role: "user",
          text,
          attachments: [],
        },
        runtimeMode: thread.runtimeMode,
        interactionMode: thread.interactionMode,
        createdAt,
      });
    });

  /**
   * Mirrors a child thread's lifecycle into its parent's activities as the
   * task.* rows the Agents panel already folds, and optionally sends each
   * finished turn's reply back to the parent as a message.
   */
  const bridge = Effect.fn("ThreadsToolkit.bridge")(function* (event: OrchestrationEvent) {
    if (event.aggregateKind !== "thread" || !isSubagentThreadId(event.aggregateId)) return;
    const childId = event.aggregateId;
    const parentId = parentThreadIdOf(childId)!;
    if (event.type === "thread.created") {
      const selection = event.payload.modelSelection;
      const effort = selection.options?.find((option) =>
        ["effort", "reasoningEffort", "variant"].includes(option.id),
      )?.value;
      lastStatus.set(childId, "starting");
      yield* appendParentActivity(parentId, "task.started", `Started ${event.payload.title}`, {
        taskId: childId,
        title: event.payload.title,
        reportBack: reportBack.get(childId) !== false,
        role: selection.instanceId,
        model: selection.model,
        ...(typeof effort === "string" ? { effort } : {}),
        detail: event.payload.title,
      });
      return;
    }
    if (event.type !== "thread.session-set") return;
    yield* restoreReportState(childId);
    const status = subagentStatusOf(event.payload.session);
    const previous = lastStatus.get(childId);
    if (status === previous) return;
    lastStatus.set(childId, status);
    if (status === "running") {
      yield* appendParentActivity(parentId, "task.updated", "Subagent working", {
        taskId: childId,
        status: "running",
      });
      return;
    }
    if (status === "failed") {
      yield* appendParentActivity(parentId, "task.updated", "Subagent failed", {
        taskId: childId,
        status: "failed",
        error: event.payload.session.lastError ?? "Provider session error",
      });
      return;
    }
    if (status === "stopped") {
      yield* appendParentActivity(parentId, "task.updated", "Subagent stopped", {
        taskId: childId,
        status: "interrupted",
      });
      return;
    }
    if (status !== "idle") return;
    yield* reportFinishedTurn(parentId, childId, { recordIdle: true });
  });

  /**
   * Sends a child's newest reply to its parent once, then records the idle
   * row carrying the report state. The report's command id is derived from
   * the reply, so the engine's command receipts drop a resend after a
   * restart even if the process stopped before the row was written.
   */
  const reportFinishedTurn = (
    parentId: string,
    childId: string,
    options: { readonly recordIdle: boolean },
  ) =>
    Effect.gen(function* () {
      const last = yield* lastAssistantMessage(childId);
      const report =
        reportBack.get(childId) === true &&
        last?.id &&
        last.text &&
        lastReported.get(childId) !== last.id
          ? { id: last.id, text: last.text }
          : null;
      if (!report && !options.recordIdle) return;
      if (report) {
        const parent = yield* threadShell(parentId);
        const child = yield* threadShell(childId);
        if (parent) {
          const text =
            report.text.length > REPORT_TEXT_LIMIT
              ? `${report.text.slice(0, REPORT_TEXT_LIMIT)}…`
              : report.text;
          yield* startTurn(
            parent,
            `[Subagent ${child?.title ?? childId} (thread ${childId}) finished a turn]\n\n${text}`,
            CommandId.make(`server:mcp-threads-report:${childId}:${report.id}`),
          );
        }
        lastReported.set(childId, report.id);
      }
      const reportedMessageId = lastReported.get(childId);
      // Every idle row carries the report state, so a restart restores it from
      // the newest row for the child.
      yield* appendParentActivity(parentId, "task.progress", "Subagent idle", {
        taskId: childId,
        status: "idle",
        ...(last?.text ? { summary: last.text } : {}),
        reportBack: reportBack.get(childId) === true,
        ...(reportedMessageId ? { reportedMessageId } : {}),
      });
    });

  /**
   * Catches up children whose last transition no bridge saw, such as a turn
   * that finished as the previous process stopped: every child whose newest
   * parent row does not record it idle but whose session is idle now gets
   * its idle row, and its reply is reported once if it reports back.
   */
  const catchUpUnrecordedIdle = Effect.gen(function* () {
    yield* restoreAllReportStates;
    for (const [childId, idle] of recordedIdle) {
      if (idle || lastStatus.has(childId)) continue;
      const child = yield* threadShell(childId);
      if (!child || child.archivedAt !== null || subagentStatusOf(child.session) !== "idle") {
        continue;
      }
      lastStatus.set(childId, "idle");
      yield* reportFinishedTurn(parentThreadIdOf(childId)!, childId, { recordIdle: true });
    }
  });

  const scope = yield* Scope.Scope;
  /** Thread id -> the failed turn a usage-limit resume is pending for. */
  const pendingResumes = new Map<string, string>();
  /** Provider instance id -> resumes waiting for any thread's next reply from it. */
  const replyWaiters = new Map<string, Deferred.Deferred<void>>();
  /** Threads resumed early that have not had a reply since. */
  const earlyResumeSpent = new Set<string>();

  const nextReplyOn = (instanceId: string) =>
    Effect.suspend(() => {
      let waiter = replyWaiters.get(instanceId);
      if (!waiter) {
        waiter = Deferred.makeUnsafe<void>();
        replyWaiters.set(instanceId, waiter);
      }
      return Deferred.await(waiter);
    });

  /** A finished assistant message proves its provider instance serves requests again. */
  const releaseReplyWaiters = Effect.fn("ThreadsToolkit.releaseReplyWaiters")(function* (
    event: OrchestrationEvent,
  ) {
    if (event.type !== "thread.message-sent") return;
    if (event.payload.role !== "assistant" || event.payload.streaming) return;
    earlyResumeSpent.delete(event.payload.threadId);
    if (replyWaiters.size === 0) return;
    const thread = yield* threadShell(event.payload.threadId);
    if (!thread) return;
    const instanceId = thread.session?.providerInstanceId ?? thread.modelSelection.instanceId;
    const waiter = replyWaiters.get(instanceId);
    if (!waiter) return;
    replyWaiters.delete(instanceId);
    yield* Deferred.succeed(waiter, undefined);
  });

  /** Prism job threads open with Model Router's job tag (see usageLimitResume.ts). */
  const isRouterJobThread = (threadId: string) =>
    snapshots.getThreadDetailById(ThreadId.make(threadId)).pipe(
      Effect.map((thread) => {
        if (Option.isNone(thread)) return false;
        const first = thread.value.messages.find((message) => message.role === "user");
        return first !== undefined && isRouterJobMessage(first.text);
      }),
      Effect.catchCause(() => Effect.succeed(false)),
    );

  /**
   * Schedules one "continue" after the usage limit that failed a thread's
   * turn lifts (see usageLimitResume.ts), and tells a child's parent when it
   * is sent. Starting another turn first, or archiving the thread, cancels
   * it. Prism job threads are skipped.
   */
  const resumeAfterUsageLimit = Effect.fn("ThreadsToolkit.resumeAfterUsageLimit")(function* (
    event: OrchestrationEvent,
  ) {
    if (event.type !== "thread.session-set" || event.payload.session.status !== "error") return;
    const threadId = event.payload.threadId;
    const failed = yield* threadShell(threadId);
    const turnId = failed?.latestTurn?.turnId;
    if (!failed || !turnId || pendingResumes.get(threadId) === turnId) return;
    if (isSubagentThreadId(threadId) && (yield* isRouterJobThread(threadId))) return;
    const instanceId = event.payload.session.providerInstanceId ?? failed.modelSelection.instanceId;
    pendingResumes.set(threadId, turnId);
    yield* resumeAfterUsageLimitReset(
      {
        thread: threadShell,
        providers: registry.getProviders,
        nextReplyOn,
        resume: (thread, trigger) =>
          Effect.gen(function* () {
            if (trigger === "lifted") earlyResumeSpent.add(thread.id);
            yield* startTurn(thread, RESUME_TEXT);
            const parentId = parentThreadIdOf(thread.id);
            const parent = parentId === null ? undefined : yield* threadShell(parentId);
            if (parent) yield* startTurn(parent, resumeNotice(thread, trigger));
          }),
      },
      { threadId, turnId, instanceId, resumeEarly: !earlyResumeSpent.has(threadId) },
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("usage-limit resume failed", { threadId, cause: Cause.pretty(cause) }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          if (pendingResumes.get(threadId) === turnId) pendingResumes.delete(threadId);
        }),
      ),
      Effect.forkIn(scope),
    );
  });

  const skipOnFailure = (name: string, event: OrchestrationEvent) =>
    Effect.catchCause((cause: Cause.Cause<unknown>) =>
      Effect.logWarning(`threads toolkit ${name} skipped an event`, {
        eventType: event.type,
        cause: Cause.pretty(cause),
      }),
    );

  // Consume the hot stream like the upstream reactors do. This layer builds
  // with the HTTP routes, so the subscription is taken in the forked fiber,
  // not at build time. Subscribing before the catch-up pass buffers every
  // event that lands during it; the bridge then handles them, and an idle
  // transition both saw is reported once (lastReported, then the report's
  // command receipt).
  yield* Effect.forkScoped(
    Effect.gen(function* () {
      const events = yield* engine.subscribeDomainEvents;
      yield* catchUpUnrecordedIdle.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("threads toolkit restart catch-up failed", {
            cause: Cause.pretty(cause),
          }),
        ),
      );
      yield* Stream.runForEach(events, (event) =>
        bridge(event).pipe(
          skipOnFailure("bridge", event),
          Effect.andThen(
            resumeAfterUsageLimit(event).pipe(skipOnFailure("usage-limit resume", event)),
          ),
          Effect.andThen(releaseReplyWaiters(event).pipe(skipOnFailure("reply waiters", event))),
        ),
      );
    }).pipe(Effect.scoped),
  );

  /**
   * The turn id a starting thread reports once its turn is active, or null
   * when it stops being active or reports none within the wait, with the
   * session last seen.
   */
  const awaitTurnId = (threadId: ThreadId) =>
    Effect.scoped(
      Effect.gen(function* () {
        const decided = (session: OrchestrationSession | null) =>
          !hasActiveTurn(session) || (session?.activeTurnId ?? null) !== null;
        const events = yield* engine.subscribeDomainEvents;
        const now = (yield* threadShell(threadId))?.session ?? null;
        const session = decided(now)
          ? now
          : yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "thread.session-set" &&
                  event.aggregateId === threadId &&
                  decided(event.payload.session),
              ),
              Stream.runHead,
              Effect.map(
                Option.map((event) =>
                  event.type === "thread.session-set" ? event.payload.session : null,
                ),
              ),
              Effect.timeoutOption(INTERRUPT_TURN_START_TIMEOUT),
              Effect.map(Option.flatten),
              Effect.map(Option.getOrElse(() => now)),
            );
        const turnId = hasActiveTurn(session) ? (session?.activeTurnId ?? null) : null;
        return { turnId, session };
      }),
    );

  return ThreadsToolkit.of({
    spawn_thread: (input) =>
      Effect.gen(function* () {
        const parent = yield* callingThread;
        const role = input.role;
        const kits = yield* roleKits(parent.projectId);
        const kit = role ? kits[role] : undefined;
        const lane = input.lane ?? DEFAULT_PRISM_LANE;
        const laneModels = role ? prismRoleModels(kits, role, lane) : [];
        // A role's model list applies only when the caller names no model.
        let preferred: { instanceId: string; model: string; effort?: string } | undefined;
        if (kit && !input.model && !input.instanceId && laneModels.length > 0) {
          const nowMs = yield* Clock.currentTimeMillis;
          const picked = pickRoleModel(laneModels, yield* registry.getProviders, nowMs);
          if ("refusal" in picked) return yield* fail(picked.refusal);
          preferred = picked.pick;
        }
        const instanceId = ProviderInstanceId.make(
          input.instanceId ?? preferred?.instanceId ?? parent.modelSelection.instanceId,
        );
        const sameInstance = instanceId === parent.modelSelection.instanceId;
        const model =
          input.model ??
          preferred?.model ??
          (sameInstance ? parent.modelSelection.model : undefined);
        if (!model) return yield* fail("Pass model when instanceId differs from this thread's.");
        const effort = input.effort ?? preferred?.effort;
        const info = yield* providers
          .getInstanceInfo(instanceId)
          .pipe(Effect.catchCause(() => fail(`Unknown provider instance ${instanceId}.`)));
        if (!info.enabled) {
          return yield* fail(`Provider instance ${instanceId} is disabled in T3 Code settings.`);
        }
        const options: ProviderOptionSelection[] = effort
          ? [{ id: effortOptionId(info.driverKind), value: effort }]
          : [];
        const modelSelection = {
          instanceId,
          model,
          ...(options.length > 0 ? { options } : {}),
        };
        const random = (yield* uuid).replaceAll("-", "").slice(0, 12);
        const childId = ThreadId.make(
          makeSubagentThreadId(parent.id, role ? prismRoleSuffix(role, random) : random),
        );
        reportBack.set(childId, input.reportBack !== false);
        const createdAt = yield* nowIso;
        const titlePrefix = role ? PRISM_ROLE_LABELS[role] : "Subagent";
        yield* dispatch({
          type: "thread.create",
          commandId: yield* commandId("create"),
          threadId: childId,
          projectId: parent.projectId,
          title: input.title ?? `${titlePrefix}: ${input.task.slice(0, 60)}`,
          modelSelection,
          // Children never inherit a restricted mode: their approvals would go to the
          // user, not the planner, and stall the child unseen.
          runtimeMode: DEFAULT_RUNTIME_MODE,
          interactionMode: "default",
          branch: parent.branch,
          worktreePath: parent.worktreePath,
          createdAt,
        });
        const child = yield* threadShell(childId);
        if (!child) return yield* fail(`Child thread ${childId} was not created.`);
        yield* startTurn(child, kit ? roleTaskMessage(kit, input.task) : input.task);
        return {
          threadId: childId,
          ...(role ? { role, lane } : {}),
          parentThreadId: parent.id,
          instanceId,
          model,
        };
      }),
    message_thread: ({ threadId, text, scope }) =>
      Effect.gen(function* () {
        const { caller, target } = yield* callerScopedThread(threadId, scope);
        const statusBefore = subagentStatusOf(target.session);
        if (statusBefore === "starting") {
          return yield* fail(`Thread ${threadId} is still starting. Retry in a few seconds.`);
        }
        yield* startTurn(target, attributedMessage(text, caller, target));
        return { threadId, statusBefore, delivery: deliveryOf(statusBefore) };
      }),
    interrupt_thread: ({ threadId, scope }) =>
      Effect.gen(function* () {
        const { target } = yield* callerScopedThread(threadId, scope);
        if (!hasActiveTurn(target.session)) {
          return {
            threadId,
            turnId: null,
            status: "no_active_run" as const,
            statusAfter: subagentStatusOf(target.session),
          };
        }
        // Every interrupt names its turn, so the reactor drops it if a newer
        // turn is active by then. A starting thread has no turn id yet: wait
        // for it rather than send an interrupt that could stop a later turn.
        const sampled = target.session?.activeTurnId ?? null;
        const started = sampled !== null ? null : yield* awaitTurnId(target.id);
        const turnId = sampled ?? started?.turnId ?? null;
        if (turnId === null) {
          return {
            threadId,
            turnId: null,
            status: "no_active_run" as const,
            statusAfter: subagentStatusOf(started?.session ?? target.session),
          };
        }
        // Settled once that turn stopped running, whether or not a newer one started.
        const turnSettled = (session: OrchestrationSession | null) =>
          !hasActiveTurn(session) || session?.activeTurnId !== turnId;
        const settled = yield* Effect.scoped(
          Effect.gen(function* () {
            // Subscribe first so a settle that lands right after the command is not missed.
            const events = yield* engine.subscribeDomainEvents;
            yield* dispatch({
              type: "thread.turn.interrupt",
              commandId: yield* commandId("interrupt"),
              threadId: target.id,
              turnId,
              createdAt: yield* nowIso,
            });
            const now = yield* threadShell(threadId);
            if (now && turnSettled(now.session)) return Option.some(now.session);
            return yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "thread.session-set" &&
                  event.aggregateId === target.id &&
                  turnSettled(event.payload.session),
              ),
              Stream.runHead,
              Effect.map(
                Option.map((event) =>
                  event.type === "thread.session-set" ? event.payload.session : null,
                ),
              ),
              Effect.timeoutOption(INTERRUPT_SETTLE_TIMEOUT),
              Effect.map(Option.flatten),
            );
          }),
        );
        if (Option.isSome(settled)) {
          return {
            threadId,
            turnId,
            status: "interrupted" as const,
            statusAfter: subagentStatusOf(settled.value),
          };
        }
        const latest = yield* threadShell(threadId);
        return {
          threadId,
          turnId,
          status: "interrupt_requested" as const,
          statusAfter: subagentStatusOf(latest?.session ?? target.session),
        };
      }),
    read_thread: ({ threadId, scope }) =>
      callerScopedThread(threadId, scope).pipe(Effect.flatMap(({ target }) => summarize(target))),
    list_child_threads: () => listChildThreads(),
    list_threads: ({ scope, includeSettled }) => listThreads(scope, includeSettled),
  });
});

export const ThreadsToolkitHandlersLive = ThreadsToolkit.toLayer(make);
