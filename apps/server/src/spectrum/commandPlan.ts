import {
  CommandId,
  EventId,
  OrchestrationV2RunJson,
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
import { followSend } from "../scheduledTaskChecks/handoff.ts";
import { lifecycleEvents } from "./lifecycle.ts";
import { readSpectrum } from "./store.ts";
import {
  makeInitialReport,
  dropUnsentReport,
  reportDrainProof,
  reportDrainedPlan,
} from "./reportPolicy.ts";
import type { ForkCommitPlan } from "../childThreads/ForkCommitPlan.ts";
import { spectrumPlan } from "./spectrumPlan.ts";
import { planTranscriptAppend } from "./transcript.ts";

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
      next = dropUnsentReport({
        ...state,
        status: "retired",
        generation: state.generation + 1,
        revision: state.revision + 1,
        round: null,
        outbox: cancellations,
      });
      if (state.report === null) next = makeInitialReport(next, "Spectrum stopped.", true);
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
          const end = yield* followSend(state.report.threadId, state.report.messageId);
          if (end.kind !== "completed")
            return yield* new ForkCommandPlanError({
              threadId: state.threadId,
              cause: "Finish or explicitly abandon the current report before reopening.",
            });
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
        for (const color of state.participants) {
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
    return {
      events,
      effects: [],
      forkPlans: [
        ...guards,
        spectrumPlan(
          { expectedRevision: state.revision, expectedGeneration: state.generation, state: next },
          command.type === "thread.stop"
            ? undefined
            : {
                resume: command.type === "thread.unsettle" || explicitMessage(command),
                humanOverride: command.type === "thread.unsettle" || humanMessage(command),
              },
        ),
      ],
    };
  }).pipe(
    Effect.mapError((cause) =>
      cause._tag === "ForkCommandPlanError"
        ? cause
        : new ForkCommandPlanError({ threadId: command.threadId, cause }),
    ),
  );
});
export const layer = Layer.succeed(ForkCommandInterceptor, { plan });
