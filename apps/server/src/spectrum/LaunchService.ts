import {
  CommandId,
  EventId,
  ProviderInstanceId,
  ThreadId,
  threadOwner,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type PrismLane,
  type PrismRole,
  type RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Prism from "../prism/PrismService.ts";
import { resolveSchedulerRun, schedulerRunOpenGuard } from "../scheduledTaskChecks/handoff.ts";
import { spectrumRegistrationPlan } from "./registrationPlan.ts";
import { SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

export interface SpectrumStart {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly callerThreadId: ThreadId;
  readonly callerRunId: RunId | null;
  readonly question: string;
  readonly title?: string;
  readonly mode: "council" | "free";
  readonly limit: number;
  readonly moderator: number;
  readonly colors: ReadonlyArray<{
    readonly label: string;
    readonly role?: PrismRole | undefined;
    readonly lane?: PrismLane | undefined;
    readonly selection?: ModelSelection | undefined;
    readonly instructions?: string | undefined;
    readonly instanceId?: string | undefined;
    readonly model?: string | undefined;
    readonly effort?: string | undefined;
  }>;
}

export class SpectrumLaunchError extends Schema.TaggedError<SpectrumLaunchError>()(
  "SpectrumLaunchError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}

export class SpectrumLaunchService extends Context.Service<
  SpectrumLaunchService,
  { readonly register: (input: SpectrumStart) => Effect.Effect<SpectrumState, SpectrumLaunchError> }
>()("t3/spectrum/LaunchService/SpectrumLaunchService") {}

const validateState = Schema.decodeEffect(SpectrumState);

/** No provider is attached while shells are registered; durable round dispatch launches Drafters. */
function shell(
  caller: OrchestrationV2AppThread,
  input: {
    readonly id: ThreadId;
    readonly parentId: ThreadId;
    readonly title: string;
    readonly selection: ModelSelection;
    readonly now: DateTime.Utc;
  },
): OrchestrationV2AppThread {
  return {
    id: input.id,
    projectId: caller.projectId,
    title: input.title,
    providerInstanceId: input.selection.instanceId,
    modelSelection: input.selection,
    owner: threadOwner(caller),
    coOwners: [],
    createdBy: "agent",
    creationSource: "server",
    runtimeMode: caller.runtimeMode,
    interactionMode: caller.interactionMode,
    branch: caller.branch,
    worktreePath: caller.worktreePath,
    activeProviderThreadId: null,
    lineage: {
      parentThreadId: input.parentId,
      relationshipToParent: "subagent",
      rootThreadId: caller.lineage.rootThreadId,
    },
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const make = Effect.gen(function* () {
  const prism = yield* Prism.PrismService;
  const adapters = yield* ProviderAdapters.ProviderAdapterRegistryV2;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const register = Effect.fn("SpectrumLaunch.register")(function* (input: SpectrumStart) {
    const existing = yield* receipts.getByCommandId(input.commandId);
    if (Option.isSome(existing)) {
      if (
        existing.value.threadId !== input.threadId ||
        existing.value.commandType !== "spectrum.register" ||
        existing.value.status !== "accepted"
      )
        return yield* new SpectrumLaunchError({
          threadId: input.threadId,
          cause: "Receipt mismatch",
        });
      return Option.getOrThrow(
        yield* readSpectrum(input.threadId).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      );
    }
    if (
      input.colors.length < 2 ||
      input.colors.length > 8 ||
      new Set(input.colors.map((color) => color.label)).size !== input.colors.length ||
      input.colors.some(
        (color) => color.label.trim().length === 0 || color.label.toLowerCase() === "user",
      ) ||
      (input.mode === "council" && input.limit < 2) ||
      input.moderator < 0 ||
      input.moderator >= input.colors.length
    )
      return yield* new SpectrumLaunchError({
        threadId: input.threadId,
        cause: "Invalid Color configuration",
      });
    const caller = yield* projections.getThread(input.callerThreadId);
    const resolved = yield* Effect.forEach(input.colors, (color, index) =>
      Effect.gen(function* () {
        let explicit =
          color.selection ?? (color.role === undefined ? caller.modelSelection : undefined);
        const inheritedSelection = explicit ?? caller.modelSelection;
        if (
          color.instanceId !== undefined ||
          color.model !== undefined ||
          color.effort !== undefined
        ) {
          const instanceId =
            color.instanceId === undefined
              ? inheritedSelection.instanceId
              : ProviderInstanceId.make(color.instanceId);
          const provider = (yield* providers.getProviders).find(
            (provider) => provider.instanceId === instanceId,
          );
          const effortOption =
            provider?.driver === "codex" || provider?.driver === "grok"
              ? "reasoningEffort"
              : provider?.driver === "opencode"
                ? "variant"
                : "effort";
          explicit = {
            ...inheritedSelection,
            instanceId,
            model: color.model ?? inheritedSelection.model,
            ...(color.effort === undefined
              ? {}
              : { options: [{ id: effortOption, value: color.effort }] }),
          };
        }
        const target = yield* prism.resolve({
          projectId: caller.projectId,
          role: color.role ?? "worker",
          lane: color.lane,
          explicit,
          inherited: caller.modelSelection,
          validate: (selection) =>
            Prism.validateLaunchSelection(selection).pipe(
              Effect.provideService(ProviderAdapters.ProviderAdapterRegistryV2, adapters),
              Effect.provideService(ProviderRegistry.ProviderRegistry, providers),
            ),
        });
        const selection = target.modelSelection;
        return {
          threadId: ThreadId.make(`sub.${input.threadId}.color.${index}`),
          label: color.label,
          selection,
          instructions: [color.role === undefined ? "" : target.kitText, color.instructions ?? ""]
            .filter(Boolean)
            .join("\n\n"),
        };
      }),
    );
    const binding =
      input.callerRunId === null
        ? null
        : yield* resolveSchedulerRun({
            callerThreadId: caller.id,
            callerRunId: input.callerRunId,
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    let state = yield* validateState({
      version: 1,
      threadId: input.threadId,
      callerThreadId: caller.id,
      callerRunId: input.callerRunId,
      scheduledTaskId: binding?.scheduledTaskId ?? null,
      schedulerRunId: binding?.schedulerRunId ?? null,
      question: input.question,
      mode: input.mode,
      limit: input.limit,
      moderator: input.moderator,
      participants: resolved,
      generation: 0,
      revision: 0,
      cursor: 0,
      status: "active",
      cycle: 0,
      round: null,
      transcript: [],
      inbox: [],
      outbox: [],
      report: null,
      reportAbandonment: null,
    });
    const now = yield* DateTime.now;
    const threads = [
      shell(caller, {
        id: state.threadId,
        parentId: caller.id,
        title: input.title ?? `Spectrum: ${input.question.slice(0, 60)}`,
        selection: caller.modelSelection,
        now,
      }),
      ...resolved.map((color) =>
        shell(caller, {
          id: color.threadId,
          parentId: state.threadId,
          title: color.label,
          selection: color.selection,
          now,
        }),
      ),
    ];
    const commit = (boundState: SpectrumState) =>
      sink.commitCommand({
        commandId: input.commandId,
        threadId: state.threadId,
        commandType: "spectrum.register",
        acceptedAt: now,
        events: threads.map((thread) => ({
          id: EventId.make(`${input.commandId}:create:${thread.id}`),
          type: "thread.created" as const,
          threadId: thread.id,
          occurredAt: now,
          payload:
            thread.id === boundState.threadId
              ? { ...thread, forkSpectrumRunning: boundState.status === "active" }
              : thread,
        })),
        effects: [],
        forkPlans: [
          {
            ...spectrumRegistrationPlan(boundState, caller),
            guards: [
              ...spectrumRegistrationPlan(boundState, caller).guards,
              ...(boundState.schedulerRunId === null || boundState.scheduledTaskId === null
                ? []
                : [
                    schedulerRunOpenGuard({
                      scheduledTaskId: boundState.scheduledTaskId,
                      schedulerRunId: boundState.schedulerRunId,
                      callerThreadId: caller.id,
                    }),
                  ]),
            ],
          },
        ],
      });
    yield* commit(state).pipe(
      Effect.catchTags({
        ForkCommitGuardRejected: (error) => {
          if (binding === null || error.kind !== "state_conflict") return Effect.fail(error);
          state = { ...state, scheduledTaskId: null, schedulerRunId: null };
          return commit(state);
        },
      }),
    );
    return state;
  });
  return SpectrumLaunchService.of({
    register: (input) =>
      register(input).pipe(
        Effect.mapError((cause) => new SpectrumLaunchError({ threadId: input.threadId, cause })),
      ),
  });
});

export const layer = Layer.effect(SpectrumLaunchService, make);
