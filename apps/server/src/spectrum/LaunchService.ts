import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import {
  CommandId,
  EventId,
  ProviderInstanceId,
  ThreadId,
  threadOwner,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestratorMcpFailure,
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
import { spectrumTaskEvent, spectrumTaskId, spectrumTaskRow } from "./delegatedTask.ts";
import { spectrumRegistrationPlan } from "./registrationPlan.ts";
import { SpectrumState } from "./state.ts";
import { readSpectrum } from "./store.ts";

/** A Color candidate: a full selection, or overrides of the inherited one. */
interface SpectrumCandidate {
  readonly selection?: ModelSelection | undefined;
  readonly instanceId?: string | undefined;
  readonly model?: string | undefined;
  readonly effort?: string | undefined;
}

export interface SpectrumStart {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly callerThreadId: ThreadId;
  readonly callerRunId: RunId | null;
  /** An agent in the caller thread started it, so the Spectrum is that thread's delegated task. */
  readonly delegatedTask?: boolean | undefined;
  readonly question: string;
  readonly title?: string;
  readonly mode: "council" | "free";
  readonly limit: number;
  readonly moderator: number;
  readonly colors: ReadonlyArray<
    SpectrumCandidate & {
      readonly bare?: import("@t3tools/contracts").BareLaunchRequest | undefined;
      readonly label: string;
      readonly role?: PrismRole | undefined;
      readonly lane?: PrismLane | undefined;
      readonly instructions?: string | undefined;
      readonly fallbacks?: ReadonlyArray<SpectrumCandidate> | undefined;
    }
  >;
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
    ...(caller.linkOrigin === undefined ? {} : { linkOrigin: caller.linkOrigin }),
    // Registration admits only a live caller, so its acknowledged lineage tokens are the shells' too.
    ...(caller.forkResumedRetirements === undefined
      ? {}
      : { forkResumedRetirements: caller.forkResumedRetirements }),
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
  const bareLaunch = yield* BareLaunch.BareLaunch;
  const projects = yield* ProjectStore.ProjectStoreV2;
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
      (input.mode === "free" && input.limit < input.colors.length) ||
      input.moderator < 0 ||
      input.moderator >= input.colors.length
    )
      return yield* new SpectrumLaunchError({
        threadId: input.threadId,
        cause: "Invalid Color configuration",
      });
    if (
      input.colors.some((color) => color.bare !== undefined && (color.fallbacks?.length ?? 0) > 0)
    )
      return yield* new SpectrumLaunchError({
        threadId: input.threadId,
        cause: "Bare Colors have no fallbacks.",
      });
    const caller = yield* projections.getThread(input.callerThreadId);
    const withOverrides = Effect.fnUntraced(function* (
      base: ModelSelection,
      spec: SpectrumCandidate,
    ) {
      if (spec.instanceId === undefined && spec.model === undefined && spec.effort === undefined)
        return base;
      const instanceId =
        spec.instanceId === undefined ? base.instanceId : ProviderInstanceId.make(spec.instanceId);
      const provider = (yield* providers.getProviders).find(
        (provider) => provider.instanceId === instanceId,
      );
      const effortOption =
        provider?.driver === "codex" || provider?.driver === "grok"
          ? "reasoningEffort"
          : provider?.driver === "opencode"
            ? "variant"
            : "effort";
      return {
        ...base,
        instanceId,
        model: spec.model ?? base.model,
        ...(spec.effort === undefined
          ? {}
          : { options: [{ id: effortOption, value: spec.effort }] }),
      } satisfies ModelSelection;
    });
    const resolved = yield* Effect.forEach(input.colors, (color, index) =>
      Effect.gen(function* () {
        const primary =
          color.selection ?? (color.role === undefined ? caller.modelSelection : undefined);
        const explicit =
          primary === undefined &&
          color.instanceId === undefined &&
          color.model === undefined &&
          color.effort === undefined
            ? undefined
            : yield* withOverrides(primary ?? caller.modelSelection, color);
        const resolveCandidate = (candidate: ModelSelection | undefined) =>
          prism.resolve({
            bare: color.bare !== undefined,
            projectId: caller.projectId,
            role: color.role ?? "worker",
            lane: color.lane,
            explicit: candidate,
            inherited: caller.modelSelection,
            validate: (selection) =>
              Prism.validateLaunchSelection(selection, color.bare !== undefined).pipe(
                Effect.provideService(ProviderAdapters.ProviderAdapterRegistryV2, adapters),
                Effect.provideService(ProviderRegistry.ProviderRegistry, providers),
              ),
          });
        const attempted: ModelSelection[] = [];
        // A candidate that cannot start now is recorded and skipped while a later one remains.
        const resolveFrom = (
          candidate: ModelSelection | undefined,
          later: ReadonlyArray<ModelSelection>,
        ): Effect.Effect<
          {
            readonly target: { readonly modelSelection: ModelSelection; readonly kitText: string };
            readonly fallbacks: ReadonlyArray<ModelSelection>;
          },
          OrchestratorMcpFailure
        > =>
          resolveCandidate(candidate).pipe(
            Effect.map((target) => ({ target, fallbacks: later })),
            Effect.catch((error) => {
              const [next, ...rest] = later;
              if (
                next === undefined ||
                (error.code !== "provider_unavailable" && error.code !== "model_unavailable")
              )
                return Effect.fail(error);
              if (candidate !== undefined) attempted.push(candidate);
              return resolveFrom(next, rest);
            }),
          );
        const { target, fallbacks } = yield* resolveFrom(
          explicit,
          yield* Effect.forEach(color.fallbacks ?? [], (fallback) =>
            withOverrides(fallback.selection ?? caller.modelSelection, fallback),
          ),
        );
        const selection = target.modelSelection;
        const project =
          color.bare === undefined || caller.worktreePath !== null
            ? Option.none()
            : yield* projects.get(caller.projectId);
        const cwd =
          caller.worktreePath ?? (Option.isSome(project) ? project.value.workspaceRoot : undefined);
        if (color.bare !== undefined && cwd === undefined)
          return yield* new SpectrumLaunchError({
            threadId: input.threadId,
            cause: "Bare workspace is unavailable.",
          });
        const bare =
          color.bare === undefined
            ? undefined
            : yield* bareLaunch.resolve({
                request: color.bare,
                instanceId: selection.instanceId,
                cwd: cwd!,
                readGrant:
                  caller.runtimeMode === "full-access"
                    ? { type: "all" }
                    : { type: "roots", roots: [cwd!] },
              });
        return {
          ...(bare === undefined ? {} : { bare }),
          threadId: ThreadId.make(`sub.${input.threadId}.color.${index}`),
          label: color.label,
          selection,
          fallbacks,
          attempted,
          instructions: [
            color.bare !== undefined || color.role === undefined ? "" : target.kitText,
            color.instructions ?? "",
          ]
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
      free: null,
      report: null,
      delegatedTaskId:
        input.delegatedTask === true ? spectrumTaskId({ threadId: input.threadId }) : null,
      reportAbandonment: null,
    });
    const now = yield* DateTime.now;
    const title = input.title ?? `Spectrum: ${input.question.slice(0, 60)}`;
    const callerRun =
      state.delegatedTaskId === null || input.callerRunId === null
        ? undefined
        : (yield* projections.getThreadRecords(caller.id, ["runs"])).runs.find(
            (run) => run.id === input.callerRunId,
          );
    const task =
      state.delegatedTaskId === null
        ? null
        : spectrumTaskRow({
            state,
            caller,
            parentNodeId: callerRun?.rootNodeId ?? null,
            driver: (yield* adapters.get(caller.modelSelection.instanceId)).driver,
            selection: caller.modelSelection,
            title,
            now,
          });
    const threads = [
      shell(caller, {
        id: state.threadId,
        parentId: caller.id,
        title,
        selection: caller.modelSelection,
        now,
      }),
      ...resolved.map((color) => ({
        ...shell(caller, {
          id: color.threadId,
          parentId: state.threadId,
          title: color.label,
          selection: color.selection,
          now,
        }),
        ...(color.bare === undefined
          ? {}
          : { bare: { ...color.bare, nativeHomeOwnerThreadId: color.threadId } }),
      })),
    ];
    const commit = (boundState: SpectrumState) =>
      sink.commitCommand({
        commandId: input.commandId,
        threadId: state.threadId,
        commandType: "spectrum.register",
        acceptedAt: now,
        events: [
          ...threads.map((thread) => ({
            id: EventId.make(`${input.commandId}:create:${thread.id}`),
            type: "thread.created" as const,
            threadId: thread.id,
            occurredAt: now,
            payload:
              thread.id === boundState.threadId
                ? { ...thread, forkSpectrumRunning: boundState.status === "active" }
                : thread,
          })),
          ...(task === null ? [] : [spectrumTaskEvent(task, input.commandId, now)]),
        ],
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
