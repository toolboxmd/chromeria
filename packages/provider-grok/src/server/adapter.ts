import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { bareHome } from "@t3tools/provider-core/server/BareProviderRuntime";
import {
  prepareGrokBare,
  GROK_BARE_TOOLS,
  GROK_BARE_COMMANDS,
  grokBareSessionMeta,
  grokBareHistoryEvidence,
} from "./GrokBareRuntime.ts";
import {
  bareReceipt,
  bareInventoryEvidence,
  namesT3Mcp,
} from "@t3tools/provider-core/server/bareLaunch";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import {
  XAiPromptFailureText,
  isXAiTaskCompletedWakeNotification,
  xAiRateLimitedErrorCode,
} from "./xaiAcpExtension.ts";
import { discoverGrokSkills, leadingGrokSkillMention } from "./skills.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { resolveSelfInvocation, type SelfInvocation } from "@t3tools/shared/nodeRuntime";
import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  type OrchestrationV2ProviderCapabilities,
  type RuntimeMode,
} from "@t3tools/contracts";
import { GrokSettings } from "../settings.ts";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";

import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { makeAcpNativeLoggerFactory } from "@t3tools/provider-acp/server/nativeLogging";
import {
  applyGrokAcpModelSelection,
  currentGrokModelIdFromSessionSetup,
  currentGrokReasoningEffortFromSessionSetup,
  grokApprovalOptions,
  makeGrokAcpRuntime,
  resolveGrokAcpBaseModelId,
} from "./acpSupport.ts";
import {
  extractGrokPlanMarkdownFromToolCallData,
  extractXAiAcpBackgroundToolMutation,
  extractXAiAcpSubagentEndNotice,
  extractXAiAcpSubagentUpdate,
  extractXAiAskUserQuestionIdentity,
  extractXAiAskUserQuestions,
  extractXAiBackgroundTaskCompletion,
  extractXAiKilledBackgroundTasks,
  extractXAiMonitorTaskId,
  xAiBackgroundShellTaskId,
  isXAiNonBlockingBackgroundTool,
  isXAiMonitorTool,
  extractXAiExitPlanMarkdown,
  registerXAiSubagentFinished,
  makeXAiAskUserQuestionCancelledResponse,
  makeXAiAskUserQuestionResponse,
  makeXAiExitPlanModeCapturedResponse,
  normalizeXAiAcpToolCallState,
  registerXAiBackgroundTaskTracking,
  XAiAskUserQuestionRequest,
  XAiExitPlanModeRequest,
} from "./xaiAcpExtension.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { acpPermissionDisposition } from "@t3tools/provider-acp/server/clientPolicy";
import {
  findModelConfigOption,
  sessionModelStateFromInitialize,
} from "@t3tools/provider-acp/server/runtimeModel";
import * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2ExtensionContext,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "@t3tools/provider-acp/server/adapter";

export const GROK_PROVIDER = ProviderDriverKind.make("grok");
const GROK_DRIVER_KIND = GROK_PROVIDER;
export const GROK_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(GROK_DRIVER_KIND);
const DEFAULT_GROK_SETTINGS = Schema.decodeSync(GrokSettings)({});
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);

export const GrokProviderCapabilitiesV2 = {
  ...AcpProviderCapabilitiesV2,
  sessions: {
    ...AcpProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: true,
    supportsRuntimeModeSwitchInSession: false,
  },
  threads: {
    ...AcpProviderCapabilitiesV2.threads,
    canReadThreadSnapshot: true,
    canForkThread: false,
    canForkFromTurn: false,
  },
  subagents: {
    ...AcpProviderCapabilitiesV2.subagents,
    supportsSubagents: true,
    exposesSubagentThreadIds: true,
    emitsSubagentLifecycle: true,
  },
  tools: {
    ...AcpProviderCapabilitiesV2.tools,
    supportsMcpTools: true,
  },
  checkpointing: {
    ...AcpProviderCapabilitiesV2.checkpointing,
    providerCanReadConversationSnapshot: true,
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface GrokAdapterV2Options {
  readonly instanceId: Parameters<typeof makeAcpAdapterV2>[0]["instanceId"];
  readonly settings: GrokSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly hostPlatform: NodeJS.Platform;
  readonly selfInvocation: SelfInvocation;
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  readonly continuationRequests?: Parameters<typeof makeAcpAdapterV2>[0]["continuationRequests"];
  readonly testHooks?: Parameters<typeof makeAcpAdapterV2>[0]["testHooks"];
  readonly makeRuntime?: (
    input: AcpAdapterV2RuntimeInput,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError,
    ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Scope.Scope
  >;
  readonly assertComplete?: Effect.Effect<void, EffectAcpErrors.AcpError>;
}

const registerGrokAcpExtensions: NonNullable<AcpAdapterV2Flavor["registerExtensions"]> = ({
  runtime,
  requestUserInput,
  applyBackgroundTaskMutation,
  finishSubagent,
  captureProposedPlan,
  lastProposedPlanMarkdown,
}) =>
  registerXAiBackgroundTaskTracking(runtime, applyBackgroundTaskMutation).pipe(
    Effect.andThen(registerXAiSubagentFinished(runtime, finishSubagent)),
    Effect.andThen(registerGrokAskUserQuestionExtensions({ runtime, requestUserInput })),
    Effect.andThen(
      registerGrokExitPlanModeExtensions({
        runtime,
        captureProposedPlan,
        lastProposedPlanMarkdown,
      }),
    ),
  );

// Capture the reverse-requested plan, then abandon the native gate to finish the turn.
const registerGrokExitPlanModeExtensions = ({
  runtime,
  captureProposedPlan,
  lastProposedPlanMarkdown,
}: Pick<
  AcpAdapterV2ExtensionContext,
  "runtime" | "captureProposedPlan" | "lastProposedPlanMarkdown"
>) =>
  Effect.forEach(
    ["x.ai/exit_plan_mode", "_x.ai/exit_plan_mode"] as const,
    (method) =>
      runtime.handleExtRequest(method, XAiExitPlanModeRequest, (params) =>
        Effect.gen(function* () {
          const fallback = yield* lastProposedPlanMarkdown;
          yield* captureProposedPlan({
            planMarkdown: extractXAiExitPlanMarkdown(params, fallback),
          });
          return makeXAiExitPlanModeCapturedResponse();
        }),
      ),
    { discard: true },
  );

const registerGrokAskUserQuestionExtensions = ({
  runtime,
  requestUserInput,
}: Pick<AcpAdapterV2ExtensionContext, "runtime" | "requestUserInput">) =>
  Effect.forEach(
    ["x.ai/ask_user_question", "_x.ai/ask_user_question"] as const,
    (method) =>
      runtime.handleExtRequest(method, XAiAskUserQuestionRequest, (params, requestContext) => {
        const identity = extractXAiAskUserQuestionIdentity(params);
        const questions = extractXAiAskUserQuestions(params).map((question) => ({
          ...question,
          options: [...question.options],
        }));
        return requestUserInput(
          {
            nativeItemId: `${identity.sessionId}:xai-question:${identity.toolCallId}`,
            nativeRequestId: identity.toolCallId,
            questions,
          },
          requestContext,
        ).pipe(
          Effect.flatMap(({ acknowledgeNativeResponse, answers }) =>
            Effect.succeed(
              answers === null
                ? makeXAiAskUserQuestionCancelledResponse()
                : makeXAiAskUserQuestionResponse(params, answers),
            ).pipe(Effect.tap(() => acknowledgeNativeResponse)),
          ),
        );
      }),
    { discard: true },
  );

/** Explicit policy overrides launch in ask mode so Grok cannot bypass T3 approval checks. */
export function grokLaunchRuntimeMode(
  runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
): RuntimeMode {
  return runtimePolicy.approvalPolicy === undefined && runtimePolicy.sandboxPolicy === undefined
    ? runtimePolicy.runtimeMode
    : "approval-required";
}

export function makeGrokAcpAdapterFlavor(
  options: GrokAdapterV2Options,
  homeDirectory: string,
): AcpAdapterV2Flavor {
  return {
    driver: GROK_PROVIDER,
    runtimeHarness: "Grok",
    capabilities: GrokProviderCapabilitiesV2,
    interruptPromptOnCancel: false,
    // Stop and explicit cut-ins (requestRuntimeRestart) restart the process group so background
    // tasks end too; routine steering cancels softly to preserve background work.
    restartRuntimeAfterInterrupt: true,
    terminateRuntimeProcessGroupOnInterrupt: true,
    // Settled Grok sessions accept concurrent prompts; skip cancel to preserve background subagents.
    preserveRuntimeOnSettledInterrupt: true,
    // Grok accepts image blocks despite advertising promptCapabilities.image:false.
    supportsImagePrompts: true,
    supportsCompaction: true,
    // Grok expands only a leading `/name`, while the `$` picker inserts `$name` from the
    // `grok inspect` catalog. Ask that catalog again so other `$` text is left alone.
    rewriteUserText: (text, turnInput) => {
      const mention = leadingGrokSkillMention(text);
      if (mention === undefined) return Effect.succeed(text);
      return discoverGrokSkills(
        options.settings,
        options.environment,
        turnInput.runtimePolicy.cwd ?? undefined,
      ).pipe(
        Effect.map((skills) =>
          skills.some((skill) => skill.enabled && skill.name === mention.name)
            ? `/${mention.name}${mention.rest}`
            : text,
        ),
        Effect.orElseSucceed(() => text),
      );
    },
    resolveModelId: (selection) => resolveGrokAcpBaseModelId(selection.model),
    getModelContextWindow: (selection, session) => {
      if (selection.instanceId !== options.instanceId) return undefined;
      const modelId = resolveGrokAcpBaseModelId(selection.model);
      const models =
        session.sessionSetupResult.models ??
        sessionModelStateFromInitialize(session.initializeResult);
      const window = models?.availableModels.find((model) => model.modelId === modelId)?._meta
        ?.totalContextTokens;
      // contextWindows includes selectable larger windows; totalContextTokens is the default.
      return typeof window === "number" && Number.isSafeInteger(window) && window > 0
        ? window
        : undefined;
    },
    applyModelSelection: Effect.fn("GrokAdapter.applyModelSelection")(function* ({
      runtime,
      startResult,
      modelSelection,
      previousSelection,
    }) {
      const legacy = startResult.initializeResult.protocolVersion === 1;
      const options = legacy ? [] : yield* runtime.getConfigOptions;
      const configuredModel = findModelConfigOption(options)?.currentValue;
      // The composer stores effort as `reasoningEffort`, but Grok exposes it as the `thought_level`
      // option, so the id-matched loop never applies it. Protocol 1 takes it as `session/set_model`
      // metadata; later protocols write the session's thought-level option.
      const effortOption = options.find(
        (option) => option.category === "thought_level" && option.type === "select",
      );
      const configuredEffort = effortOption?.currentValue;
      const currentModelId = legacy
        ? currentGrokModelIdFromSessionSetup(startResult.sessionSetupResult)
        : typeof configuredModel === "string"
          ? configuredModel
          : undefined;
      return yield* applyGrokAcpModelSelection({
        runtime: legacy
          ? runtime
          : {
              setSessionModel: Effect.fn("GrokAdapter.setSessionModel")(function* (model, meta) {
                if (model !== currentModelId) {
                  yield* runtime.setModel(model);
                }
                const effort = meta?.reasoningEffort;
                if (effortOption === undefined || typeof effort !== "string") {
                  return {};
                }
                // A model switch can change which efforts the session offers, so
                // check the live option and, like the generic option loop, skip a
                // value it no longer advertises or rejects instead of failing the
                // session open; the agent's default applies.
                const liveOption = (yield* runtime.getConfigOptions).find(
                  (option) => option.id === effortOption.id,
                );
                if (
                  liveOption === undefined ||
                  (liveOption.type === "select" &&
                    !liveOption.options
                      .flatMap((entry) =>
                        "value" in entry
                          ? [entry.value]
                          : entry.options.map((choice) => choice.value),
                      )
                      .includes(effort))
                ) {
                  yield* Effect.logWarning(
                    "Grok session does not offer the requested reasoning effort",
                    { optionId: effortOption.id, value: effort },
                  );
                  return {};
                }
                yield* runtime.setConfigOption(effortOption.id, effort).pipe(
                  Effect.catchTags({
                    AcpRequestError: (error) =>
                      Effect.logWarning("Grok session rejected the requested reasoning effort", {
                        optionId: effortOption.id,
                        value: effort,
                        detail: error.message,
                      }),
                  }),
                );
                return {};
              }),
            },
        currentModelId,
        // Protocol 1 has no live effort state. After a selection ran, baseline on its effort; an
        // effort-free selection leaves the live value unknown, so a requested effort is sent rather than
        // compared with stale setup metadata.
        currentReasoningEffort: legacy
          ? previousSelection === null
            ? currentGrokReasoningEffortFromSessionSetup(startResult.sessionSetupResult)
            : getModelSelectionStringOptionValue(previousSelection, "reasoningEffort")
          : typeof configuredEffort === "string"
            ? configuredEffort
            : undefined,
        requestedModelId: resolveGrokAcpBaseModelId(modelSelection.model),
        requestedReasoningEffort: getModelSelectionStringOptionValue(
          modelSelection,
          "reasoningEffort",
        ),
        mapError: (cause) => cause,
      });
    }),
    makeRuntime:
      options.makeRuntime ??
      (({ runtimePolicy, ...input }) =>
        makeGrokAcpRuntime({
          ...input,
          interruptPromptOnCancel: input.interruptPromptOnCancel ?? false,
          grokSettings: options.settings,
          environment: options.environment,
          runtimeMode: grokLaunchRuntimeMode(runtimePolicy),
        })),
    // Auto prompts represent classifier refusals and must reach the user.
    permissionDisposition: (policy, request) =>
      grokLaunchRuntimeMode(policy) === "auto" ? "ask" : acpPermissionDisposition(policy, request),
    approvalOptions: grokApprovalOptions,
    promptFailure: (cause) =>
      makeProviderFailure({
        cause,
        ...(isAcpRequestError(cause)
          ? {
              message:
                cause.cause instanceof XAiPromptFailureText
                  ? cause.cause.message
                  : cause.errorMessage,
              code: String(cause.code),
              class: cause.code === xAiRateLimitedErrorCode ? "usage_limit" : "provider_error",
            }
          : { class: "provider_error" }),
      }),
    registerExtensions: registerGrokAcpExtensions,
    extractSubagentUpdate: extractXAiAcpSubagentUpdate,
    extractSubagentEndNotice: extractXAiAcpSubagentEndNotice,
    normalizeToolCall: normalizeXAiAcpToolCallState,
    extractProposedPlanMarkdown: (toolCall) =>
      extractGrokPlanMarkdownFromToolCallData(toolCall.data, {
        platform: options.hostPlatform,
        environment: options.environment,
        homeDirectory,
      }),
    extractBackgroundTaskId: extractXAiMonitorTaskId,
    isBackgroundCommandTool: (toolCall) => xAiBackgroundShellTaskId(toolCall) !== undefined,
    extractBackgroundToolMutation: extractXAiAcpBackgroundToolMutation,
    extractBackgroundTaskCompletion: (toolCall) => [
      ...extractXAiBackgroundTaskCompletion(toolCall),
      ...extractXAiKilledBackgroundTasks(toolCall),
    ],
    isPersistentBackgroundTool: isXAiNonBlockingBackgroundTool,
    backgroundToolWakePolicy: (toolCall) =>
      isXAiMonitorTool(toolCall) ? "progress" : "completion",
    extractNativePromptId: (notification) => {
      const update = notification.update;
      if (update.sessionUpdate !== "user_message_chunk") return undefined;
      const meta = update._meta;
      const index = meta?.promptIndex;
      if (
        meta?.syntheticPrompt === true ||
        meta?.hideFromScrollback === true ||
        typeof index !== "number" ||
        !Number.isSafeInteger(index) ||
        index < 0
      )
        return undefined;
      return `prompt:${index}`;
    },
    isProviderWakeNotification: isXAiTaskCompletedWakeNotification,
    deferFinalizeForBackgroundWork: true,
    enablePostSettleContinuation: true,
    ...(options.assertComplete === undefined ? {} : { assertComplete: options.assertComplete }),
  };
}

const decodeBareTools = Schema.decodeUnknownOption(
  Schema.Struct({ _meta: Schema.Struct({ tools: Schema.Array(Schema.String) }) }),
);

export const makeGrokAdapterV2 = Effect.fn("makeGrokAdapterV2")(function* (
  options: GrokAdapterV2Options,
) {
  const context = yield* Effect.context<Effect.Services<ReturnType<typeof makeAcpAdapterV2>>>();
  const host = yield* ProviderHost.ProviderHost;
  const fileSystem = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const flavor = makeGrokAcpAdapterFlavor(options, yield* HostProcess.HomeDirectory);
  const normal = yield* makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor,
    selfInvocation: options.selfInvocation,
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
    ...(options.continuationRequests === undefined
      ? {}
      : { continuationRequests: options.continuationRequests }),
    ...(options.testHooks === undefined ? {} : { testHooks: options.testHooks }),
  });
  return ProviderAdapter.ProviderAdapterV2.of({
    ...normal,
    bareSupport: Effect.succeed({ supported: true }),
    openSession: (input) =>
      input.runtimePolicy.bare === undefined
        ? normal.openSession(input)
        : Effect.gen(function* () {
            const bare = input.runtimePolicy.bare!;
            if (namesT3Mcp(bare))
              return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: GROK_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause:
                  "Grok bare named MCP needs search_tool/use_tool; the exact attach path is unsupported.",
              });
            const threadHash = Hex.encode(
              yield* crypto.digest("SHA-256", new TextEncoder().encode(input.threadId)).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterOpenSessionError({
                      driver: GROK_PROVIDER,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              ),
            );
            const home = `${bareHome(host.paths.stateDir, options.instanceId, "grok")}/thread-${threadHash}`;
            const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
            let gate = yield* Deferred.make<void, EffectAcpErrors.AcpError>();
            let metadataError: string | undefined;
            let evidence: ReadonlyArray<import("@t3tools/contracts").BareEvidence> = [];
            let plannedSkills: ReadonlyArray<string> = [];
            let plannedPlugins: ReadonlyArray<string> = [];
            const fail = (reason: string) =>
              new EffectAcpErrors.AcpRequestError({ code: -32602, errorMessage: reason });
            const bareFlavor: AcpAdapterV2Flavor = {
              ...flavor,
              beforePrompt: Effect.suspend(() =>
                Deferred.await(gate).pipe(
                  Effect.timeoutOption("30 seconds"),
                  Effect.flatMap((ready) =>
                    Option.isNone(ready)
                      ? Effect.fail(
                          fail("Grok bare is missing before-prompt tool and command metadata."),
                        )
                      : metadataError === undefined
                        ? Effect.void
                        : Effect.fail(fail(metadataError)),
                  ),
                ),
              ),
              onAvailableCommandsUpdate: Effect.fn("GrokAdapter.onAvailableCommandsUpdate")(
                function* (commands, notification) {
                  const decoded = decodeBareTools(notification?.update);
                  const tools = Option.isSome(decoded) ? decoded.value._meta.tools : [];
                  const names = commands.map((command) => command.name);
                  const source = `Grok available_commands_update ${notification?.sessionId ?? "unknown"}; before session/prompt`;
                  evidence = [
                    bareInventoryEvidence("tools", tools, GROK_BARE_TOOLS, source),
                    bareInventoryEvidence(
                      "slash_commands",
                      names,
                      GROK_BARE_COMMANDS,
                      source,
                      true,
                    ),
                    ...evidence.filter(
                      (entry) => entry.category !== "tools" && entry.category !== "slash_commands",
                    ),
                  ];
                  const extras = [
                    ...tools.filter(
                      (name) => !GROK_BARE_TOOLS.includes(name as (typeof GROK_BARE_TOOLS)[number]),
                    ),
                    ...names.filter(
                      (name) =>
                        !GROK_BARE_COMMANDS.includes(name as (typeof GROK_BARE_COMMANDS)[number]),
                    ),
                  ];
                  const missing = [
                    ...GROK_BARE_TOOLS.filter((name) => !tools.includes(name)),
                    ...GROK_BARE_COMMANDS.filter((name) => !names.includes(name)),
                  ];
                  metadataError =
                    Option.isNone(decoded) || extras.length > 0 || missing.length > 0
                      ? `Grok bare inventory mismatch: unexpected ${extras.join(", ") || "none"}; missing ${missing.join(", ") || "none"}.`
                      : undefined;
                  yield* Deferred.succeed(gate, undefined);
                },
              ),
              makeRuntime: Effect.fn("GrokAdapter.makeRuntime")(function* (runtimeInput) {
                gate = yield* Deferred.make<void, EffectAcpErrors.AcpError>();
                metadataError = undefined;
                const prepared = yield* prepareGrokBare({
                  home,
                  cwd,
                  environment: options.environment,
                  binary: options.settings.binaryPath || "grok",
                }).pipe(
                  Effect.provideService(FileSystem.FileSystem, fileSystem),
                  Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                  Effect.mapError((cause) =>
                    fail(cause instanceof Error ? cause.message : "Grok bare discovery failed."),
                  ),
                );
                plannedSkills = prepared.skills;
                plannedPlugins = prepared.plugins;
                evidence = [
                  {
                    category: "skills",
                    status: "observed_clean",
                    identities: prepared.observedDisabledSkills,
                    source:
                      "native grok inspect; observed disabled identities, not hook execution proof",
                  },
                ];
                if (options.makeRuntime !== undefined)
                  return yield* options.makeRuntime(runtimeInput);
                const { runtimePolicy, ...nativeInput } = runtimeInput;
                return yield* makeGrokAcpRuntime({
                  ...nativeInput,
                  grokSettings: options.settings,
                  environment: prepared.environment,
                  runtimeMode: grokLaunchRuntimeMode(runtimePolicy),
                  bare: true,
                  sessionMeta: grokBareSessionMeta(
                    grokLaunchRuntimeMode(runtimePolicy) === "full-access",
                  ),
                });
              }),
            };
            const adapter = yield* makeAcpAdapterV2({
              instanceId: options.instanceId,
              flavor: bareFlavor,
              selfInvocation: options.selfInvocation,
              ...(options.nativeLogging === undefined
                ? {}
                : { nativeLogging: options.nativeLogging }),
              ...(options.testHooks === undefined ? {} : { testHooks: options.testHooks }),
            }).pipe(Effect.provide(context));
            const runtime = yield* adapter.openSession(input);
            return {
              ...runtime,
              getBareReceipt: Effect.fn("GrokAdapter.getBareReceipt")(function* (
                thread: import("@t3tools/contracts").OrchestrationV2ProviderThread,
              ) {
                const sessionId = thread.nativeThreadRef?.nativeId ?? null;
                const history =
                  sessionId === null
                    ? []
                    : yield* grokBareHistoryEvidence(home, cwd, sessionId).pipe(
                        Effect.provideService(FileSystem.FileSystem, fileSystem),
                        Effect.catch(() => Effect.succeed([])),
                      );
                return bareReceipt({
                  driver: GROK_PROVIDER,
                  bare,
                  mode: input.runtimePolicy.runtimeMode,
                  effective: `Grok ${grokLaunchRuntimeMode(input.runtimePolicy)}`,
                  controls: [
                    "own GROK_HOME and --no-leader",
                    "systemPromptOverride='.'",
                    "agentProfile.disallowedTools (14)",
                    "compat/memory/workflows/subagents off",
                    ...plannedSkills.map((name) => `skills.disabled:${name}`),
                    ...plannedPlugins.map((name) => `plugins.disabled:${name}`),
                  ],
                  retained: [
                    "system text '.'",
                    "native user-info/environment/rules block",
                    "native task wrapper",
                    "native title/helper requests",
                    `native commands: ${GROK_BARE_COMMANDS.join(", ")}`,
                    "six native tool schemas",
                  ],
                  root: `${home}/sessions`,
                  sessionId,
                  evidence: [...evidence, ...history],
                });
              }),
            };
          }),
  });
});

export type GrokAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | Path.Path
  | McpProviderSessions.McpProviderSessions
  | ProviderEventLoggers.ProviderEventLoggers
  | ProviderHost.ProviderHost;

export const GrokAdapterV2Driver: ProviderAdapterDriver<GrokSettings, GrokAdapterV2DriverEnv> = {
  driverKind: GROK_DRIVER_KIND,
  configSchema: GrokSettings,
  defaultConfig: (): GrokSettings => DEFAULT_GROK_SETTINGS,
  create: Effect.fn("GrokAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<GrokSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const hostPlatform = yield* HostProcess.Platform;
      const selfInvocation = yield* resolveSelfInvocation();
      const providerEventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      const makeNativeLogger = yield* makeAcpNativeLoggerFactory();
      return yield* makeGrokAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        hostPlatform,
        selfInvocation,
        continuationRequests,
        nativeLogging: (threadId) =>
          makeNativeLogger({
            nativeEventLogger: providerEventLoggers.native,
            provider: GROK_PROVIDER,
            threadId,
          }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: GROK_DRIVER_KIND,
              instanceId: input.instanceId,
              detail: "Failed to create Grok ACP adapter.",
              cause,
            }),
        ),
      ),
  ),
};
