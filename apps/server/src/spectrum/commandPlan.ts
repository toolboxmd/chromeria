import {
  CommandId,
  EventId,
  OrchestrationV2RunJson,
  type ThreadId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  ForkCommandInterceptor,
  ForkCommandPlanError,
  type ForkCommandPlan,
} from "../fork/ForkCommandInterceptor.ts";
import { explicitMessage, humanMessage, readRetirementState } from "../childThreads/retirement.ts";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import {
  ForkProviderlessMessages,
  ForkProviderlessMessageReadError,
} from "../fork/ForkProviderlessMessages.ts";
import { ownedActiveRuns, ownedThreads, ownedRunsDrainedPlan } from "./cancellationPlan.ts";
import { stoppedText } from "./freeMode.ts";
import { lifecycleEvents } from "./lifecycle.ts";
import { spectrumTaskUpdate } from "./delegatedTask.ts";
import { membersOf } from "./state.ts";
import { readSpectrum } from "./store.ts";
import {
  makeInitialReport,
  reportDrainProof,
  reportDrainedPlan,
  attemptOutcome,
  reportChainsUnchanged,
} from "./reportPolicy.ts";
import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import { planTranscriptAppend } from "./transcript.ts";
import * as Automation from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { CurrentRequest, requestDigest } from "../mcp/McpRequestReceipts.ts";
import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";

const plan = Effect.fn("Spectrum.commandPlan")(function* (
  command: OrchestrationV2ServerCommand,
): Effect.fn.Return<
  ForkCommandPlan | null,
  ForkCommandPlanError,
  SqlClient.SqlClient | Projection.ProjectionStoreV2
> {
  if (
    command.type !== "message.dispatch" &&
    command.type !== "thread.stop" &&
    command.type !== "thread.unsettle"
  )
    return null;
  return yield* Effect.gen(function* () {
    const found = yield* readSpectrum(command.threadId);
    if (Option.isNone(found)) return null; // Identity prefixes never confer Spectrum behavior.
    const state = found.value;
    const projections = yield* Projection.ProjectionStoreV2;
    const thread = yield* projections.getThread(state.threadId);
    const now = yield* DateTime.now;
    let next = state;
    const events: OrchestrationV2DomainEvent[] = [];
    const guards: ForkCommitPlan[] = [];
    let acceptance: Automation.ValidatedAcceptance | undefined;
    let acceptanceCommit: Effect.Effect<void, ForkCommitGuardRejected> | undefined;
    if (command.type === "thread.stop") {
      // Common Stop still records retirement, cancels/drains core effects and propagates native lineage.
      // These exact-run commands explicitly retain Spectrum's participant ownership after restart.
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{ payload_json: string }>`WITH RECURSIVE owned(thread_id) AS (
        SELECT thread_id FROM orchestration_v2_projection_threads WHERE json_extract(payload_json,'$.lineage.parentThreadId')=${state.threadId}
        AND json_extract(payload_json,'$.lineage.relationshipToParent')='subagent'
        UNION SELECT t.thread_id FROM orchestration_v2_projection_threads t JOIN owned p ON json_extract(t.payload_json,'$.lineage.parentThreadId')=p.thread_id
        WHERE json_extract(t.payload_json,'$.lineage.relationshipToParent')='subagent'
      ) SELECT r.payload_json FROM orchestration_v2_projection_runs r JOIN owned o ON r.thread_id=o.thread_id
        WHERE r.status NOT IN('completed','failed','interrupted','cancelled','rolled_back')`;
      const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));
      const cancellations = yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const run = yield* decode(row.payload_json);
          const identity = {
            commandId: CommandId.make(`${command.commandId}:owned:${run.id}`),
            threadId: run.threadId,
            runId: run.id,
          };
          return run.status === "queued"
            ? { ...identity, type: "queued-run.cancel" as const }
            : {
                ...identity,
                type: "run.interrupt" as const,
                holdQueue: false,
                reason: "Spectrum stopped",
              };
        }),
      );
      // A settled report still owes the caller its only wake, so Stop keeps its pending send.
      next = {
        ...state,
        status: "retired",
        generation: state.generation + 1,
        revision: state.revision + 1,
        round: null,
        outbox: [
          ...cancellations,
          ...state.outbox.filter((pending) => pending.commandId === state.report?.commandId),
        ],
      };
      if (state.report === null) {
        next = makeInitialReport(next, stoppedText(state), true);
        events.push(
          ...(yield* spectrumTaskUpdate(
            next,
            { status: "cancelled", result: stoppedText(state) },
            command.commandId,
            now,
          )),
        );
      }
    } else {
      const resume = command.type === "thread.unsettle" || explicitMessage(command);
      if (state.status !== "active") {
        if (!resume)
          return yield* new ForkCommandPlanError({
            threadId: state.threadId,
            cause: "A retired Spectrum requires explicit input.",
          });
        if (
          state.report !== null &&
          state.reportAbandonment?.commandId !== state.report.commandId
        ) {
          const outcome = yield* attemptOutcome(state.report.threadId, state.report);
          if (outcome.kind !== "delivered")
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause: "Finish or explicitly abandon the current report before reopening.",
            });
          guards.push(reportChainsUnchanged(state.threadId, [outcome.chain]));
        }
        if (
          state.report !== null &&
          state.reportAbandonment?.commandId === state.report.commandId
        ) {
          const proof = yield* reportDrainProof(state);
          if (proof === null)
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause:
                "The abandoned report is still draining. Reopen once its exact continuation chain has stopped.",
            });
          guards.push(reportDrainedPlan(proof));
        }
        if (state.status === "retired") {
          if ((yield* ownedActiveRuns(state.threadId)).length !== 0)
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause: "Owned Drafters are still draining. Reopen after they stop.",
            });
          const descendants = yield* ownedThreads(state.threadId);
          const sql = yield* SqlClient.SqlClient;
          const pending =
            descendants.length === 0
              ? []
              : yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox
            WHERE thread_id IN ${sql.in(descendants)} AND status IN('pending','running')
              AND json_extract(payload_json,'$.type') IN('provider-turn.start','provider-turn.restart','provider-turn.interrupt','provider-runtime.continue') LIMIT 1`;
          if (pending.length !== 0)
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause: "Owned Drafter effects are still draining. Reopen after they stop.",
            });
          guards.push(ownedRunsDrainedPlan(state.threadId, descendants));
        }
        next = {
          ...state,
          status: "active",
          generation: state.generation + 1,
          cycle: state.cycle + 1,
          round: null,
          outbox: [],
          report: null,
          reportAbandonment: null,
        };
        const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
        if (state.automationCertificateId !== undefined && Option.isNone(authority))
          return yield* new ForkCommandPlanError({
            threadId: state.threadId,
            cause: "Native automation authority is unavailable.",
          });
        if (Option.isSome(authority)) {
          const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
          const action = yield* CurrentRequest;
          const members = membersOf(state);
          const first = members[0];
          if (first === undefined)
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause: "The Spectrum has no accepted members.",
            });
          const binding: Automation.AutomationBinding = {
            kind: "spectrum",
            resourceId: state.threadId,
            threadId: state.threadId,
            callerThreadId: state.callerThreadId,
            memberThreadIds: [first.threadId, ...members.slice(1).map((member) => member.threadId)],
            projectId: thread.projectId,
            incarnation: null,
            definitionGeneration: command.commandId,
            workflowGeneration: next.generation,
            definitionDigest: requestDigest({
              command,
              state: next,
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
            }),
          };
          acceptance = yield* authority.value.captureAutomation(
            Option.getOrUndefined(scope),
            binding,
            action ?? { nativeCommandId: command.commandId },
            {
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
              resourceScope: "environment-resources",
              onward: true,
              capabilities: Option.isSome(scope)
                ? [...scope.value.capabilities]
                : ["orchestration", "worktree", "pull-requests", "preview", "device"],
            },
          );
          if (acceptance !== undefined) {
            const admitted = yield* authority.value.automationAdmission(
              acceptance,
              binding,
              command.commandId,
            );
            acceptanceCommit = admitted.commit.pipe(
              Effect.mapError(
                () =>
                  new ForkCommitGuardRejected({
                    threadId: state.threadId,
                    kind: "invalid_transition",
                  }),
              ),
            );
            next = { ...next, automationCertificateId: acceptance.certificate.id };
          }
        }
        // The reopened Spectrum is working again; its next report settles the row anew.
        events.push(
          ...(yield* spectrumTaskUpdate(next, { status: "running" }, command.commandId, now)),
        );
        for (const color of membersOf(state)) {
          const child = yield* projections.getThread(color.threadId);
          const retirement = yield* readRetirementState(child.id);
          events.push({
            id: EventId.make(`${command.commandId}:resume:${child.id}`),
            type: "thread.metadata-updated",
            threadId: child.id,
            occurredAt: now,
            payload: {
              ...child,
              forkResumedRetirements: retirement.tokens,
              settledOverride: "active",
              settledAt: null,
              updatedAt: now,
            },
          });
        }
      }
      if (command.type === "message.dispatch") {
        const append = planTranscriptAppend(
          next,
          thread,
          {
            type: "spectrum.transcript.append",
            commandId: command.commandId,
            threadId: command.threadId,
            generation: next.generation,
            revision: next.revision,
            messageId: command.messageId,
            role: "user",
            text: command.text,
          },
          now,
        );
        next = append.mutation.state;
        const provenance = {
          createdBy: command.createdBy,
          creationSource: command.creationSource,
          attachments: command.attachments,
          ...(command.senderThreadId === undefined
            ? {}
            : { senderThreadId: command.senderThreadId }),
          ...(command.scheduledTaskId === undefined
            ? {}
            : { scheduledTaskId: command.scheduledTaskId }),
          ...(command.context === undefined ? {} : { context: command.context }),
        };
        for (const event of append.events) {
          if (event.type === "message.updated")
            events.push({ ...event, payload: { ...event.payload, ...provenance } });
          else if (event.type === "turn-item.updated" && event.payload.type === "user_message")
            events.push({ ...event, payload: { ...event.payload, ...provenance } });
          else events.push(event);
        }
      } else next = { ...next, revision: state.revision + 1 };
      const retirement = yield* readRetirementState(thread.id);
      events.push({
        id: EventId.make(`${command.commandId}:spectrum:resume`),
        type: "thread.metadata-updated",
        threadId: thread.id,
        occurredAt: now,
        payload: {
          ...thread,
          forkSpectrumRunning: next.status === "active",
          forkResumedRetirements: retirement.tokens,
          settledOverride: "active",
          settledAt: null,
          updatedAt: now,
        },
      });
    }
    if (command.type === "thread.stop")
      events.push(...lifecycleEvents(thread, next, command.commandId, now));
    const nativePlan = spectrumPlan(
      { expectedRevision: state.revision, expectedGeneration: state.generation, state: next },
      command.type === "thread.stop"
        ? undefined
        : {
            resume: command.type === "thread.unsettle" || explicitMessage(command),
            humanOverride: command.type === "thread.unsettle" || humanMessage(command),
          },
    );
    const pendingAcceptance = acceptance;
    const reopenPlan: ForkCommitPlan =
      pendingAcceptance === undefined
        ? nativePlan
        : {
            guards: nativePlan.guards.map((guard) =>
              guard.pipe(
                Effect.provideService(Automation.CurrentAutomationAcceptance, pendingAcceptance),
              ),
            ),
            mutations: [
              ...(acceptanceCommit === undefined ? [] : [acceptanceCommit]),
              ...nativePlan.mutations.map((mutation) =>
                mutation.pipe(
                  Effect.provideService(Automation.CurrentAutomationAcceptance, pendingAcceptance),
                ),
              ),
            ],
          };
    return {
      events,
      effects: [],
      forkPlans: [...guards, reopenPlan],
    };
  }).pipe(
    Effect.mapError((cause) =>
      cause._tag === "ForkCommandPlanError"
        ? cause
        : new ForkCommandPlanError({ threadId: command.threadId, cause }),
    ),
  );
});
const providerlessMessages = Layer.effect(
  ForkProviderlessMessages,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      contains: (threadId: ThreadId) =>
        readSpectrum(threadId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.map(Option.isSome),
          Effect.mapError((cause) => new ForkProviderlessMessageReadError({ threadId, cause })),
        ),
    };
  }),
);
export const layer = Layer.merge(
  Layer.effect(
    ForkCommandInterceptor,
    Effect.gen(function* () {
      const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
      return {
        plan: (command: OrchestrationV2ServerCommand) =>
          Option.isNone(authority)
            ? plan(command)
            : plan(command).pipe(
                Effect.provideService(PeerAuthority.PeerAuthority, authority.value),
              ),
      };
    }),
  ),
  providerlessMessages,
);
