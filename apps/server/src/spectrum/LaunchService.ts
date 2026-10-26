import { ForkCommitGuardRejected } from "../childThreads/ForkCommitPlan.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { PeerDispatchAuthority } from "../peer/peerEffectAuthority.ts";
import { CurrentRequest, requestDigest } from "../mcp/McpRequestReceipts.ts";
import * as BareLaunch from "../orchestration-v2/BareLaunch.ts";
import * as Settings from "../serverSettings.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  resolveSpectrumStart,
  SpectrumStartValidationError,
  isStartValidationError,
} from "./settings.ts";
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

export type SpectrumColorSpec = SpectrumCandidate & {
  readonly bare?: import("@t3tools/contracts").BareLaunchRequest | undefined;
  readonly label: string;
  readonly role?: PrismRole | undefined;
  readonly lane?: PrismLane | undefined;
  readonly instructions?: string | undefined;
  readonly fallbacks?: ReadonlyArray<SpectrumCandidate> | undefined;
};
/** White is configured like a Color but is not one, so it has no label. */
export type SpectrumWhiteSpec = Omit<SpectrumColorSpec, "label" | "instructions">;

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
  readonly white: SpectrumWhiteSpec;
  readonly relayRounds?: number | undefined;
  readonly turnsPerColor?: number | undefined;
  readonly limit: number;
  readonly colors: ReadonlyArray<SpectrumColorSpec>;
}

export type SpectrumStartRequest = Omit<SpectrumStart, "mode" | "colors" | "limit" | "white"> & {
  readonly mode?: SpectrumStart["mode"] | undefined;
  readonly colors?: SpectrumStart["colors"] | undefined;
  readonly limit?: number | undefined;
  /** White's own Color, or (legacy) the label of a passed Color whose model White copies. */
  readonly white?: string | SpectrumWhiteSpec | undefined;
  /** Legacy zero-based index of the passed Color whose model White copies. */
  readonly moderator?: number | undefined;
};

export class SpectrumLaunchError extends Schema.TaggedError<SpectrumLaunchError>()(
  "SpectrumLaunchError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {}

export class SpectrumLaunchService extends Context.Service<
  SpectrumLaunchService,
  {
    readonly register: (
      input: SpectrumStartRequest,
    ) => Effect.Effect<SpectrumState, SpectrumLaunchError | SpectrumStartValidationError>;
  }
>()("t3/spectrum/LaunchService/SpectrumLaunchService") {}

const validateState = Schema.decodeEffect(SpectrumState);

/** A Color may be called White, so the separate member takes the first name no Color uses. */
export function whiteMemberLabel(colorLabels: ReadonlyArray<string>): string {
  const taken = new Set(colorLabels.map((label) => label.trim().toLowerCase()));
  for (let n = 1; ; n++) {
    const label = n === 1 ? "White" : n === 2 ? "White (moderator)" : `White (moderator ${n - 1})`;
    if (!taken.has(label.toLowerCase())) return label;
  }
}

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
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const settings = yield* Settings.ServerSettingsService;
  const bareLaunch = yield* BareLaunch.BareLaunch;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const prism = yield* Prism.PrismService;
  const adapters = yield* ProviderAdapters.ProviderAdapterRegistryV2;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const register = Effect.fn("SpectrumLaunch.register")(function* (request: SpectrumStartRequest) {
    const existing = yield* receipts.getByCommandId(request.commandId);
    if (Option.isSome(existing)) {
      if (
        existing.value.threadId !== request.threadId ||
        existing.value.commandType !== "spectrum.register" ||
        existing.value.status !== "accepted"
      )
        return yield* new SpectrumLaunchError({
          threadId: request.threadId,
          cause: "Receipt mismatch",
        });
      return Option.getOrThrow(
        yield* readSpectrum(request.threadId).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      );
    }
    const caller = yield* projections.getThread(request.callerThreadId);
    const defaults = resolveProjectSettings(yield* settings.getSettings, caller.projectId).settings
      .spectrum;
    const input = yield* Effect.try({
      try: () => resolveSpectrumStart(request, defaults),
      catch: (cause) =>
        isStartValidationError(cause)
          ? cause
          : Schema.isSchemaError(cause)
            ? new SpectrumStartValidationError({ message: cause.message })
            : new SpectrumLaunchError({ threadId: request.threadId, cause }),
    });
    if (
      [...input.colors, input.white].some(
        (color) => color.bare !== undefined && (color.fallbacks?.length ?? 0) > 0,
      )
    )
      return yield* new SpectrumLaunchError({
        threadId: input.threadId,
        cause: "Bare Colors have no fallbacks.",
      });
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
    const resolveMember = Effect.fn("LaunchService.resolveMember")(function* (
      color: SpectrumColorSpec,
      threadId: ThreadId,
    ) {
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
        threadId,
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
    });
    const resolved = yield* Effect.forEach(input.colors, (color, index) =>
      resolveMember(color, ThreadId.make(`sub.${input.threadId}.color.${index}`)),
    );
    const whiteLabel = whiteMemberLabel(input.colors.map((color) => color.label));
    const white = yield* resolveMember(
      { ...input.white, label: whiteLabel },
      ThreadId.make(`sub.${input.threadId}.white`),
    );
    const binding =
      input.callerRunId === null
        ? null
        : yield* resolveSchedulerRun({
            callerThreadId: caller.id,
            callerRunId: input.callerRunId,
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
    let state = yield* validateState({
      version: 2,
      threadId: input.threadId,
      callerThreadId: caller.id,
      callerRunId: input.callerRunId,
      scheduledTaskId: binding?.scheduledTaskId ?? null,
      schedulerRunId: binding?.schedulerRunId ?? null,
      question: input.question,
      mode: input.mode,
      turnsPerColor: input.turnsPerColor,
      limit: input.limit,
      white,
      participants: resolved,
      generation: 0,
      revision: 0,
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
      ...[...resolved, white].map((color) => ({
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
    const scope = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isSome(scope) && scope.value.authority !== undefined && Option.isNone(authority))
      return yield* new SpectrumLaunchError({
        threadId: state.threadId,
        cause: "Native automation authority is unavailable",
      });
    const automationBinding: AutomationAuthority.AutomationBinding = {
      kind: "spectrum",
      resourceId: state.threadId,
      threadId: state.threadId,
      callerThreadId: caller.id,
      memberThreadIds: [
        resolved[0]!.threadId,
        ...resolved.slice(1).map((member) => member.threadId),
        white.threadId,
      ],
      projectId: caller.projectId,
      incarnation: null,
      definitionGeneration: input.commandId,
      workflowGeneration: state.generation,
      definitionDigest: requestDigest({
        input,
        resolved,
        white,
        runtimeMode: caller.runtimeMode,
        interactionMode: caller.interactionMode,
      }),
    };
    const action = yield* CurrentRequest;
    const accepted = Option.isNone(authority)
      ? undefined
      : yield* authority.value.captureAutomation(
          Option.getOrUndefined(scope),
          automationBinding,
          action ?? { nativeCommandId: input.commandId },
          {
            runtimeMode: caller.runtimeMode,
            interactionMode: caller.interactionMode,
            resourceScope: "environment-resources",
            onward: true,
            capabilities: Option.isSome(scope)
              ? [...scope.value.capabilities]
              : ["orchestration", "worktree", "pull-requests", "preview", "device"],
          },
        );
    const admission =
      accepted === undefined || Option.isNone(authority)
        ? undefined
        : yield* authority.value.automationAdmission(accepted, automationBinding, input.commandId);
    if (accepted !== undefined)
      state = { ...state, automationCertificateId: accepted.certificate.id };
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
            mutations: [
              ...spectrumRegistrationPlan(boundState, caller).mutations,
              ...(admission === undefined
                ? []
                : [
                    admission.commit.pipe(
                      Effect.mapError(
                        () =>
                          new ForkCommitGuardRejected({
                            threadId: state.threadId,
                            kind: "invalid_transition",
                          }),
                      ),
                    ),
                  ]),
            ],
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
      Effect.provideService(PeerDispatchAuthority, admission?.record.id),
      Effect.catchTags({
        ForkCommitGuardRejected: (error) => {
          if (binding === null || error.kind !== "state_conflict") return Effect.fail(error);
          state = { ...state, scheduledTaskId: null, schedulerRunId: null };
          return commit(state).pipe(
            Effect.provideService(PeerDispatchAuthority, admission?.record.id),
          );
        },
      }),
    );
    return state;
  });
  return SpectrumLaunchService.of({
    register: (input) =>
      register(input).pipe(
        Effect.mapError((cause) =>
          isStartValidationError(cause)
            ? cause
            : new SpectrumLaunchError({ threadId: input.threadId, cause }),
        ),
      ),
  });
});

export const layer = Layer.effect(SpectrumLaunchService, make);
