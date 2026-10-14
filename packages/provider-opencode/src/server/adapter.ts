import * as Path from "effect/Path";
import * as FileSystem from "effect/FileSystem";
import {
  bareHome,
  prepareOpenCodeBareEnvironment,
} from "@t3tools/provider-core/server/BareProviderRuntime";
import {
  BareLaunchError,
  bareReceipt,
  bareInventoryEvidence,
  namesT3Mcp,
} from "@t3tools/provider-core/server/bareLaunch";
import type {
  Event as OpenCodeEvent,
  Message as OpenCodeMessage,
  Part as OpenCodePart,
  PermissionRequest,
  PermissionRuleset,
  QuestionRequest,
  Session as OpenCodeSession,
  Todo as OpenCodeTodo,
  ToolPart,
} from "@opencode-ai/sdk/v2";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";
import {
  defaultInstanceIdForDriver,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanStep,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type PlanId,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRequestKind,
  type ProviderSessionId,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import { OpenCodeSettings } from "../settings.ts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import {
  structuralProtocolMethod,
  summarizeNativeProtocolPayload,
} from "@t3tools/provider-core/server/nativeProtocolLogging";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { t3OrchestrationSystemPrompt } from "@t3tools/provider-core/server/orchestrationInstructions";
import { buildRuntimeInstructions } from "@t3tools/provider-core/server/runtimeInstructions";
import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import { readableProviderErrorMessage } from "./providerErrorMessage.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  makeSubagentChildThread,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
import { openCodeCommandFailed, openCodeToolOutput, openCodeToolTurnItem } from "./toolItems.ts";

export const OPENCODE_PROVIDER = ProviderDriverKind.make("opencode");
export const OPENCODE_DEFAULT_INSTANCE_ID = defaultInstanceIdForDriver(OPENCODE_PROVIDER);
export const OPENCODE_SDK_PROTOCOL = "opencode-sdk.sse" as const;
const DEFAULT_OPENCODE_SETTINGS = Schema.decodeSync(OpenCodeSettings)({});

const makeOpenCodeMessageId = Effect.fnUntraced(function* (
  clock: Ref.Ref<{ readonly epochMillis: number; readonly counter: number }>,
) {
  const epochMillis = DateTime.toEpochMillis(yield* DateTime.now);
  const counter = yield* Ref.modify(clock, (previous) => {
    const next = previous.epochMillis === epochMillis ? previous.counter + 1 : 1;
    return [next, { epochMillis, counter: next }] as const;
  });
  const encodedTime = BigInt.asUintN(48, BigInt(epochMillis) * 0x1000n + BigInt(counter))
    .toString(16)
    .padStart(12, "0");
  const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const random = (yield* Effect.forEach(Array.from({ length: 28 }), () =>
    Random.nextIntBetween(0, alphabet.length - 1),
  ))
    .map((index) => alphabet[index])
    .join("");
  return `msg_${encodedTime}${random}`;
});

// OpenCode has no turn object; correlate by initiating user message and settle on session idle.
const OpenCodeProviderCapabilitiesV2 = {
  sessions: {
    // The directory-bound client/server is not safe for cross-thread pooling.
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: true,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: true,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: true,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: true,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: true,
    canCloseSubagents: false,
    canForkSubagentThread: true,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: true,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: true,
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
  runtimePolicy: {
    enforcement: "native",
  },
} satisfies OrchestrationV2ProviderCapabilities;

type TerminalTurnStatus = Extract<
  OrchestrationV2ProviderTurn["status"],
  "completed" | "interrupted" | "failed" | "cancelled"
>;

type OpenCodeStepUsage = Pick<
  Extract<OpenCodePart, { readonly type: "step-finish" }>,
  "id" | "tokens"
>;

interface OpenCodeTurnTokenUsageAccumulator {
  readonly partIds: Set<string>;
  readonly promptMessageIds: Set<string>;
  readonly assistantOwnershipByMessageId: Map<string, "owned" | "other" | "unknown">;
  readonly unresolvedStepsByMessageId: Map<string, Map<string, OpenCodeStepUsage>>;
  inputTokens: number;
  cachedInputTokens: number;
  cacheCreationTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  hasSubagents: boolean;
  complete: boolean;
}

function makeOpenCodeTurnTokenUsageAccumulator(): OpenCodeTurnTokenUsageAccumulator {
  return {
    partIds: new Set(),
    promptMessageIds: new Set(),
    assistantOwnershipByMessageId: new Map(),
    unresolvedStepsByMessageId: new Map(),
    inputTokens: 0,
    cachedInputTokens: 0,
    cacheCreationTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    hasSubagents: false,
    complete: true,
  };
}

function accumulateOpenCodeStepUsage(
  accumulator: OpenCodeTurnTokenUsageAccumulator,
  part: OpenCodeStepUsage,
): void {
  if (accumulator.partIds.has(part.id)) return;
  accumulator.partIds.add(part.id);
  accumulator.inputTokens += part.tokens.input + part.tokens.cache.read + part.tokens.cache.write;
  accumulator.cachedInputTokens += part.tokens.cache.read;
  accumulator.cacheCreationTokens += part.tokens.cache.write;
  accumulator.outputTokens += part.tokens.output + part.tokens.reasoning;
  accumulator.reasoningTokens += part.tokens.reasoning;
}

interface ActiveOpenCodeTurn {
  readonly usage: OpenCodeTurnTokenUsageAccumulator;
  readonly isRoot: boolean;
  readonly threadId: ThreadId;
  readonly runId: ProviderAdapter.ProviderAdapterV2TurnInput["runId"] | null;
  readonly rootNodeId: ProviderAdapter.ProviderAdapterV2TurnInput["rootNodeId"];
  readonly appThread: OrchestrationV2AppThread;
  readonly modelSelection: ModelSelection;
  readonly runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
  readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
  readonly providerThreadId: OrchestrationV2ProviderTurn["providerThreadId"];
  readonly runOrdinal: number;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  readonly parts: Map<string, Exclude<OpenCodePart, ToolPart>>;
  readonly partIdsByMessage: Map<string, Set<string>>;
  readonly toolNamesByCallId: Map<string, string>;
  mcpServerNames?: ReadonlyArray<string>;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  nextItemOrdinal: number;
  nativeUserMessageId: string | null;
  admissionMessageId: string | null;
  interrupted: boolean;
  finalized: boolean;
  planId: PlanId | null;
  admissionGeneration: number;
  admissionReconciliationGeneration: number | null;
  admissionPending: boolean;
  admissionAccepted: boolean;
  admissionMessageObserved: boolean;
  idleDuringAdmission: boolean;
  admissionSettled: Deferred.Deferred<void>;
  admissionAbortController: AbortController | null;
}

type OpenCodeAdmissionSignal =
  | "accepted"
  | "assistant-completed"
  | "busy"
  | "idle"
  | "user-message";
type OpenCodeAdmissionAction = "hold" | "reconcile-idle" | "release";

export function advanceOpenCodePromptAdmission(
  admission: Pick<
    ActiveOpenCodeTurn,
    "admissionAccepted" | "admissionMessageObserved" | "admissionPending" | "idleDuringAdmission"
  >,
  signal: OpenCodeAdmissionSignal,
): OpenCodeAdmissionAction {
  if (!admission.admissionPending) return "release";
  if (signal === "assistant-completed") {
    admission.admissionAccepted = true;
    admission.admissionMessageObserved = true;
    admission.admissionPending = false;
    return "release";
  }
  if (signal === "idle") {
    admission.idleDuringAdmission = true;
    return "hold";
  }
  if (signal === "accepted") admission.admissionAccepted = true;
  if (signal === "busy" || signal === "user-message") admission.admissionMessageObserved = true;
  if (!admission.admissionAccepted || !admission.admissionMessageObserved) return "hold";
  if (admission.idleDuringAdmission) return "reconcile-idle";
  admission.admissionPending = false;
  return "release";
}

export function cancelOpenCodePromptAdmission(
  admission: Pick<ActiveOpenCodeTurn, "admissionGeneration" | "admissionPending">,
  nextGeneration: number,
): void {
  admission.admissionGeneration = nextGeneration;
  admission.admissionPending = false;
}

export const reconcileOpenCodePromptAdmissionStatus = Effect.fn(
  "reconcileOpenCodePromptAdmissionStatus",
)(function* (
  admission: Pick<ActiveOpenCodeTurn, "admissionGeneration" | "admissionPending">,
  generation: number,
  readStatus: Effect.Effect<"busy" | "idle" | "unknown">,
) {
  if (admission.admissionGeneration !== generation || !admission.admissionPending) {
    return "stale" as const;
  }
  const status = yield* readStatus;
  if (admission.admissionGeneration !== generation || !admission.admissionPending) {
    return "stale" as const;
  }
  if (status !== "unknown") admission.admissionPending = false;
  return status;
});

interface OpenCodeSubagentContext {
  readonly nativeItemId: string;
  readonly nodeId: OrchestrationV2Subagent["id"];
  readonly parentState: OpenCodeThreadState;
  readonly parentTurn: ActiveOpenCodeTurn;
  readonly prompt: string;
  readonly title: string | null;
  readonly startedAt: DateTime.Utc;
  childSessionId: string | null;
  childThreadId: ThreadId | null;
  childProviderThreadId: OrchestrationV2ProviderThread["id"] | null;
  model: string | null;
  result: string | null;
}

interface OpenCodeThreadState {
  readonly nativeSessionId: string;
  providerThread: OrchestrationV2ProviderThread;
  appThread: OrchestrationV2AppThread | null;
  activeTurn: ActiveOpenCodeTurn | null;
  readonly providerTurns: Map<string, OrchestrationV2ProviderTurn>;
  readonly runtimeRequests: Map<string, OrchestrationV2RuntimeRequest>;
  readonly messageRoles: Map<string, "user" | "assistant">;
  readonly userMessageIds: Array<string>;
  parentSubagent: OpenCodeSubagentContext | null;
  nextChildTurnOrdinal: number;
  nextAdmissionGeneration: number;
  permission: PermissionRuleset;
  /** Parent rules last written into this child's `permission`; empty for root sessions. */
  inheritedPermission: PermissionRuleset;
}

interface OpenCodeRequestOwner {
  readonly state: OpenCodeThreadState;
  readonly turn: ActiveOpenCodeTurn;
  readonly subagent: OpenCodeSubagentContext | null;
}

function topLevelRequestOwner(state: OpenCodeThreadState): OpenCodeRequestOwner | undefined {
  let owner = state;
  let subagent: OpenCodeSubagentContext | null = null;
  while (owner.parentSubagent !== null) {
    subagent = owner.parentSubagent;
    owner = subagent.parentState;
  }
  return owner.activeTurn === null ? undefined : { state: owner, turn: owner.activeTurn, subagent };
}

interface PendingOpenCodeRequest {
  readonly requestId: RuntimeRequestId;
  readonly nativeRequestId: string;
  readonly nativeSessionId: string;
  readonly turn: ActiveOpenCodeTurn;
  readonly state: OpenCodeThreadState;
  readonly nodeId: OrchestrationV2ExecutionNode["id"];
  readonly parentNodeId: OrchestrationV2ExecutionNode["id"];
  readonly turnItemId: OrchestrationV2TurnItem["id"];
  readonly requestKind: OpenCodePermissionRequestKind | "user_input";
  readonly createdAt: DateTime.Utc;
  readonly permission?: PermissionRequest;
  readonly question?: QuestionRequest;
}

export interface OpenCodeAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: OpenCodeSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: ProviderEventLoggers.EventNdjsonLogger;
}

export interface OpenCodeProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly messageKind: "request" | "response" | "notification" | "error";
  readonly method: string;
  readonly payload: unknown;
}

function formatOpenCodeProtocolLogPayload(event: OpenCodeProtocolLogEvent) {
  return {
    direction: event.direction,
    messageKind: event.messageKind,
    method: structuralProtocolMethod(event.method),
    payload: summarizeNativeProtocolPayload(event.payload),
  };
}

export function makeOpenCodeProtocolLogger(input: {
  readonly nativeEventLogger: ProviderEventLoggers.EventNdjsonLogger | undefined;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly threadId: ThreadId;
}): (event: OpenCodeProtocolLogEvent) => Effect.Effect<void, never> {
  return (event) =>
    Effect.gen(function* () {
      // Per-token deltas; the part's final message.part.updated is still logged.
      if (!input.nativeEventLogger || event.method === "message.part.delta") return;
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      const method = structuralProtocolMethod(event.method);
      yield* input.nativeEventLogger.write(
        {
          observedAt,
          event: {
            id: yield* input.idAllocator.allocate.rawEvent({
              providerSessionId: input.providerSessionId,
              method,
            }),
            kind: "protocol",
            protocol: OPENCODE_SDK_PROTOCOL,
            provider: OPENCODE_PROVIDER,
            providerInstanceId: input.providerInstanceId,
            providerSessionId: input.providerSessionId,
            createdAt: observedAt,
            threadId: input.threadId,
            payload: formatOpenCodeProtocolLogPayload(event),
          },
        },
        input.threadId,
      );
    }).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logWarning("Failed to write native OpenCode event log.", {
              errorTag: causeErrorTag(cause),
              reasonCount: cause.reasons.length,
              provider: OPENCODE_PROVIDER,
              threadId: input.threadId,
            }),
      ),
    );
}

function protocolError(
  detail: string,
  payload?: unknown,
): ProviderAdapter.ProviderAdapterProtocolError {
  return new ProviderAdapter.ProviderAdapterProtocolError({
    driver: OPENCODE_PROVIDER,
    detail,
    ...(payload === undefined ? {} : { payload }),
  });
}

function nativeThreadId(providerThread: OrchestrationV2ProviderThread): string {
  const nativeId = providerThread.nativeThreadRef?.nativeId;
  if (nativeId === null || nativeId === undefined) {
    throw protocolError(`Provider thread ${providerThread.id} has no OpenCode session id`);
  }
  return nativeId;
}

function dateTimeFromEpoch(value: number | undefined, fallback: DateTime.Utc): DateTime.Utc {
  if (value === undefined) return fallback;
  return Option.getOrElse(DateTime.make(value), () => fallback);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function recordValue(input: unknown, key: string): unknown {
  return typeof input === "object" && input !== null && key in input
    ? (input as Record<string, unknown>)[key]
    : undefined;
}

function recordString(input: unknown, ...keys: ReadonlyArray<string>): string | undefined {
  for (const key of keys) {
    const value = nonEmptyString(recordValue(input, key));
    if (value !== undefined) return value;
  }
  return undefined;
}

function sdkResponseForRawLog(value: unknown): unknown {
  if (typeof value !== "object" || value === null) return value;
  if ("data" in value) return { data: (value as { readonly data?: unknown }).data ?? null };
  if ("stream" in value) return { subscribed: true };
  return value;
}

type OpenCodePermissionRequestKind = Extract<
  ProviderRequestKind,
  "command" | "file-read" | "file-change"
>;

export function openCodePermissionRequestKind(
  permission: string,
  toolName?: string,
): OpenCodePermissionRequestKind {
  const normalized = permission.toLowerCase();
  const normalizedTool = toolName?.toLowerCase() ?? "";
  if (
    normalized === "edit" ||
    normalized === "write" ||
    normalized === "patch" ||
    normalized === "apply_patch" ||
    normalizedTool === "edit" ||
    normalizedTool === "write" ||
    normalizedTool === "patch" ||
    normalizedTool === "apply_patch"
  ) {
    return "file-change";
  }
  if (
    normalized === "read" ||
    normalized === "glob" ||
    normalized === "grep" ||
    normalized === "lsp" ||
    normalized === "external_directory" ||
    normalizedTool === "read" ||
    normalizedTool.includes("glob") ||
    normalizedTool.includes("grep") ||
    normalizedTool.includes("search")
  ) {
    return "file-read";
  }
  return "command";
}

const OPENCODE_ALWAYS_ALLOWED_PERMISSIONS = [
  "question",
  "read",
  "glob",
  "grep",
  "lsp",
  "todowrite",
  "task",
  "skill",
] as const;

const OPENCODE_RESTRICTED_PERMISSIONS = [
  "bash",
  "edit",
  "webfetch",
  "websearch",
  "codesearch",
  "external_directory",
  "doom_loop",
] as const;

/** OpenCode has no OS sandbox; permission rules must enforce non-interactive policies too. */
function normalOpenCodePermissionRules(
  runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
): PermissionRuleset {
  const sandboxPolicy = recordValue(runtimePolicy, "sandboxPolicy");
  const sandboxType = recordString(sandboxPolicy, "type");
  const rawApprovalPolicy = runtimePolicy.approvalPolicy;
  const approvalPolicy = nonEmptyString(rawApprovalPolicy);
  const requiresApproval =
    approvalPolicy === undefined
      ? (typeof rawApprovalPolicy === "object" && rawApprovalPolicy !== null) ||
        runtimePolicy.runtimeMode !== "full-access"
      : approvalPolicy !== "never";
  const externallySandboxed = sandboxType === "externalSandbox";
  const dangerFullAccess = sandboxType === "dangerFullAccess";
  const implicitFullAccess =
    sandboxType === undefined && runtimePolicy.runtimeMode === "full-access";

  if (!requiresApproval && (externallySandboxed || dangerFullAccess || implicitFullAccess)) {
    return [{ permission: "*", pattern: "*", action: "allow" }];
  }

  // Children initially inherit only denies; seed them before overrides to protect the policy-install gap.
  const rules: PermissionRuleset = [
    { permission: "*", pattern: "*", action: "deny" },
    ...OPENCODE_RESTRICTED_PERMISSIONS.map((permission) => ({
      permission,
      pattern: "*",
      action: "deny" as const,
    })),
  ];

  if (requiresApproval) {
    rules.push({ permission: "*", pattern: "*", action: "ask" });
    for (const permission of OPENCODE_RESTRICTED_PERMISSIONS) {
      rules.push({ permission, pattern: "*", action: "ask" });
    }
  }

  rules.push(
    ...OPENCODE_ALWAYS_ALLOWED_PERMISSIONS.map((permission) => ({
      permission,
      pattern: "*",
      action: "allow" as const,
    })),
  );

  rules.push(
    { permission: "read", pattern: "*.env", action: requiresApproval ? "ask" : "deny" },
    { permission: "read", pattern: "*.env.*", action: requiresApproval ? "ask" : "deny" },
    { permission: "read", pattern: "*.env.example", action: "allow" },
  );

  if (
    runtimePolicy.runtimeMode === "auto-accept-edits" ||
    (!requiresApproval && sandboxType === "workspaceWrite")
  ) {
    rules.push({ permission: "edit", pattern: "*", action: "allow" });
  }

  if (!requiresApproval && recordValue(sandboxPolicy, "networkAccess") === true) {
    for (const permission of ["webfetch", "websearch", "codesearch"] as const) {
      rules.push({ permission, pattern: "*", action: "allow" });
    }
  }

  if (!requiresApproval && sandboxType === "readOnly") {
    const access = recordValue(sandboxPolicy, "access");
    if (recordString(access, "type") === "fullAccess") {
      rules.push({ permission: "external_directory", pattern: "*", action: "allow" });
    }
  }

  if (!requiresApproval && sandboxType === "workspaceWrite") {
    const writableRoots = recordValue(sandboxPolicy, "writableRoots");
    if (Array.isArray(writableRoots)) {
      for (const root of writableRoots) {
        if (typeof root === "string" && root.trim().length > 0) {
          rules.push({
            permission: "external_directory",
            pattern: `${root.replace(/\/$/, "")}/*`,
            action: "allow",
          });
        }
      }
    }
  }

  return rules;
}

/** Agent tool configuration is permission sugar in v1. Session rules must retain
 * both the admitted ceiling and that positive filter when they override it. */
export function openCodePermissionRules(
  policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  plan?: {
    readonly agent: string;
    readonly plansDirectory: string;
    readonly plansPattern?: string;
  },
): PermissionRuleset {
  const rules = normalOpenCodePermissionRules(policy);
  if (policy.bare === undefined) return rules;
  const native = [
    "bash",
    "read",
    "edit",
    "glob",
    "grep",
    "list",
    "external_directory",
    ...(namesT3Mcp(policy.bare) ? ["t3-code_*", "t3_code_*"] : []),
  ];
  const filtered: PermissionRuleset = [
    { permission: "*", pattern: "*", action: "deny" },
    ...rules.flatMap((rule) =>
      rule.permission === "*"
        ? native.map((permission) => ({ ...rule, permission }))
        : native.includes(rule.permission)
          ? [rule]
          : [],
    ),
  ];
  if ((plan?.agent ?? (policy.interactionMode === "plan" ? "plan" : "build")) === "plan") {
    const action =
      filtered.toReversed().find((rule) => rule.permission === "edit" && rule.pattern === "*")
        ?.action ?? "ask";
    filtered.push(
      { permission: "edit", pattern: "*", action: "deny" },
      { permission: "edit", pattern: ".opencode/plans/*.md", action },
    );
    if (plan !== undefined)
      filtered.push({
        permission: "edit",
        pattern: plan.plansPattern ?? `${plan.plansDirectory}/*.md`,
        action,
      });
  }
  return filtered;
}

function permissionRuleEquals(
  left: PermissionRuleset[number],
  right: PermissionRuleset[number],
): boolean {
  return (
    left.permission === right.permission &&
    left.pattern === right.pattern &&
    left.action === right.action
  );
}

/**
 * Children inherit only deny/external-directory rules; restore parent policy while retaining agent restrictions.
 * `previousParentRules` are the parent rules already written into `childRules`.
 */
export function openCodeChildPermissionRules(
  runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  childRules: PermissionRuleset,
  previousParentRules: PermissionRuleset = [],
): PermissionRuleset {
  const parentRules = openCodePermissionRules(runtimePolicy);
  const inheritedRules = [
    ...parentRules.filter(
      (rule) => rule.permission === "external_directory" || rule.action === "deny",
    ),
    ...previousParentRules,
  ];
  const childSpecificRules = childRules.filter(
    (childRule) =>
      !inheritedRules.some((inheritedRule) => permissionRuleEquals(childRule, inheritedRule)),
  );
  return [...parentRules, ...childSpecificRules];
}

/** Boundaries are exclusive: use the next user message, or omit at head to retain the selected turn. */
export function openCodeBoundaryAfterProviderTurn(
  providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
  selectedProviderTurnId: OrchestrationV2ProviderTurn["id"],
): string | undefined {
  const selected = providerTurns.find((turn) => turn.id === selectedProviderTurnId);
  if (selected === undefined) return undefined;
  return providerTurns
    .filter((turn) => turn.ordinal > selected.ordinal)
    .toSorted((left, right) => left.ordinal - right.ordinal)
    .map((turn) => turn.nativeTurnRef?.nativeId)
    .find((nativeId): nativeId is string => nativeId !== null && nativeId !== undefined);
}

function toolStatus(part: ToolPart): {
  readonly node: OrchestrationV2ExecutionNode["status"];
  readonly item: OrchestrationV2TurnItem["status"];
} {
  switch (part.state.status) {
    case "pending":
      return { node: "pending", item: "pending" };
    case "running":
      return { node: "running", item: "running" };
    case "completed":
      return openCodeCommandFailed(part.tool, part.state.metadata)
        ? { node: "failed", item: "failed" }
        : { node: "completed", item: "completed" };
    case "error":
      return { node: "failed", item: "failed" };
  }
}

function toolInput(part: ToolPart): Record<string, unknown> {
  return part.state.input;
}

function toolOutput(part: ToolPart): string | undefined {
  if (part.state.status === "completed") return part.state.output;
  if (part.state.status === "error") return part.state.error;
  return undefined;
}

function toolStartedAt(part: ToolPart, now: DateTime.Utc): DateTime.Utc {
  return dateTimeFromEpoch(
    part.state.status === "pending" ? undefined : part.state.time.start,
    now,
  );
}

function toolCompletedAt(part: ToolPart, now: DateTime.Utc): DateTime.Utc | null {
  return part.state.status === "completed" || part.state.status === "error"
    ? dateTimeFromEpoch(part.state.time.end, now)
    : null;
}

function toolTitle(part: ToolPart): string | null {
  return part.state.status === "running" || part.state.status === "completed"
    ? (part.state.title ?? null)
    : null;
}

function toolModel(part: ToolPart): string | null {
  const metadata =
    part.state.status === "running" ||
    part.state.status === "completed" ||
    part.state.status === "error"
      ? part.state.metadata
      : undefined;
  const model = recordValue(metadata, "model");
  const providerId = recordString(model, "providerID", "providerId");
  const modelId = recordString(model, "modelID", "modelId", "id");
  return providerId !== undefined && modelId !== undefined ? `${providerId}/${modelId}` : null;
}

function taskSessionId(part: ToolPart): string | null {
  const metadata =
    part.state.status === "running" ||
    part.state.status === "completed" ||
    part.state.status === "error"
      ? part.state.metadata
      : undefined;
  return recordString(metadata, "sessionId", "sessionID") ?? null;
}

function makeProviderThread(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: OrchestrationV2ProviderThread["providerSessionId"];
  readonly appThreadId: OrchestrationV2ProviderThread["appThreadId"];
  readonly ownerNodeId?: OrchestrationV2ProviderThread["ownerNodeId"];
  readonly nativeSession: OpenCodeSession;
  readonly forkedFrom?: OrchestrationV2ProviderThread["forkedFrom"];
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderThread {
  const createdAt = dateTimeFromEpoch(input.nativeSession.time.created, input.now);
  return {
    id: input.idAllocator.derive.providerThread({
      driver: OPENCODE_PROVIDER,
      nativeThreadId: input.nativeSession.id,
    }),
    driver: OPENCODE_PROVIDER,
    providerInstanceId: input.providerInstanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.appThreadId,
    ownerNodeId: input.ownerNodeId ?? null,
    nativeThreadRef: {
      driver: OPENCODE_PROVIDER,
      nativeId: input.nativeSession.id,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: input.forkedFrom ?? null,
    createdAt,
    updatedAt: dateTimeFromEpoch(input.nativeSession.time.updated, input.now),
  };
}

function providerRef(nativeId: string, strength: "strong" | "weak" = "strong") {
  return {
    driver: OPENCODE_PROVIDER,
    nativeId,
    strength,
  } satisfies OrchestrationV2ProviderRef;
}

function openCodeErrorMessage(event: Extract<OpenCodeEvent, { type: "session.error" }>): string {
  const error = event.properties.error;
  if (error === undefined) return "OpenCode session failed without an error payload.";
  const statusCode = recordValue(error.data, "statusCode");
  return (
    readableProviderErrorMessage(
      recordString(error.data, "message"),
      typeof statusCode === "number" ? statusCode : undefined,
      recordString(error.data, "responseBody"),
    ) ?? error.name
  );
}

function terminalStatusForError(
  event: Extract<OpenCodeEvent, { type: "session.error" }>,
  turn: ActiveOpenCodeTurn,
): TerminalTurnStatus {
  return turn.interrupted || isMessageAbortedError(event) ? "interrupted" : "failed";
}

function isMessageAbortedError(event: Extract<OpenCodeEvent, { type: "session.error" }>): boolean {
  return event.properties.error?.name === "MessageAbortedError";
}

function isOpenCodeNotFound(cause: unknown): boolean {
  const seen = new Set<unknown>();
  const queue: Array<unknown> = [cause];
  for (let steps = 0; queue.length > 0 && steps < 32; steps += 1) {
    const node = queue.shift();
    if (node === null || typeof node !== "object" || seen.has(node)) {
      continue;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;

    const response = record.response;
    const statuses = [
      record.status,
      record.statusCode,
      response !== null && typeof response === "object"
        ? (response as { readonly status?: unknown }).status
        : undefined,
    ].filter((status): status is number => typeof status === "number");
    if (statuses.includes(404)) {
      return true;
    }
    if (statuses.length > 0) {
      continue;
    }

    const name = record.name;
    if (typeof name === "string" && name.toLowerCase() === "notfounderror") {
      return true;
    }

    for (const key of ["cause", "body", "error", "data"] as const) {
      if (record[key] !== undefined) {
        queue.push(record[key]);
      }
    }
  }
  return false;
}

function unwrapData<A>(operation: string, result: { readonly data?: A }): NonNullable<A> {
  if (result.data === undefined) {
    throw new OpenCodeRuntime.OpenCodeRuntimeError({
      operation,
      detail: `OpenCode ${operation} returned no response payload.`,
    });
  }
  return result.data as NonNullable<A>;
}

export const makeOpenCodeAdapterV2 = Effect.fn("makeOpenCodeAdapterV2")(function* (
  options: OpenCodeAdapterV2Options,
) {
  const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const fileSystem = yield* FileSystem.FileSystem;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const messageIdClock = yield* Ref.make({ epochMillis: -1, counter: 0 });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: OPENCODE_PROVIDER,
        });
        const plansDirectory = `${options.environment.XDG_DATA_HOME ?? `${options.environment.HOME ?? "~"}/.local/share`}/opencode/plans`;
        const plansPattern = yield* Path.Path.pipe(
          Effect.map((path) => path.relative(cwd, `${plansDirectory}/*.md`)),
          Effect.provide(Path.layer),
        );
        const permissions = (policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy) =>
          permissionsFor(policy, input.modelSelection);
        const permissionsFor = (
          policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
          selection: ModelSelection,
        ) =>
          openCodePermissionRules(policy, {
            agent:
              getModelSelectionStringOptionValue(selection, "agent") ??
              (policy.interactionMode === "plan" ? "plan" : "build"),
            plansDirectory,
            plansPattern,
          });
        const client = runtime.createOpenCodeSdkClient({
          baseUrl: connection.url,
          directory: cwd,
          ...(connection.serverPassword !== undefined
            ? { serverPassword: connection.serverPassword }
            : {}),
        });

        const mcpSession = yield* mcpSessions.read(input.threadId);
              yield* submitPrompt(
                state,
                turn,
                {
                  sessionID: sessionId,
                  messageID: turn.admissionMessageId!,
                  model: parsedModel,
                  ...turnPromptOptions(turn),
                  parts,
                },
                admissionAbortController!,
              ).pipe(
                Effect.tapError((cause) =>
                  admissionAbortController!.signal.aborted
                    ? Effect.void
                    : finalizeTurn(state, turn, "failed", {
                        failure: makeProviderFailure({ cause, class: "provider_error" }),
                      }),
                ),
                Effect.catch((cause) =>
                  admissionAbortController!.signal.aborted ? Effect.void : Effect.fail(cause),
                ),
                Effect.ensuring(
                  Effect.all([
                    Deferred.succeed(admissionSettled, undefined).pipe(Effect.ignore),
                    Effect.sync(() => {
                      if (turn.admissionAbortController === admissionAbortController) {
                        turn.admissionAbortController = null;
                      }
                    }),
                  ]).pipe(Effect.asVoid),
                ),
              );
              if (state.activeTurn === turn && !turn.finalized && !turn.interrupted) {
                const admissionAction = advanceOpenCodePromptAdmission(turn, "accepted");
                if (admissionAction === "reconcile-idle") {
                  yield* reconcilePromptAdmission(state, turn);
                }
              }
            },
            (effect, turnInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver: OPENCODE_PROVIDER,
                      threadId: turnInput.threadId,
                      providerThreadId: turnInput.providerThread.id,
                      runId: turnInput.runId,
                      cause,
                    }),
                ),
              ),
          ),
          steerTurn: Effect.fn("OpencodeAdapter.steerTurn")(
            function* (steerInput) {
              const sessionId = nativeThreadId(steerInput.providerThread);
              const state = threads.get(sessionId);
              const turn = state?.activeTurn;
              if (
                state === undefined ||
                turn === undefined ||
                turn === null ||
                turn.providerTurnId !== steerInput.providerTurnId ||
                turn.interrupted
              ) {
                return yield* protocolError(
                  `OpenCode turn ${steerInput.providerTurnId} is not active`,
                );
              }
              const parsedModel = OpenCodeRuntime.parseOpenCodeModelSlug(turn.modelSelection.model);
              if (parsedModel === null) {
                return yield* protocolError(
                  `OpenCode model '${turn.modelSelection.model}' must use provider/model format`,
                );
              }
              const text = (yield* providerMessageTextWithAttachmentPaths({
                text: steerInput.message.text,
                attachments: steerInput.message.attachments,
                resolveAttachmentPath: host.resolveAttachmentPath,
                resolveExtractedTextPath: host.resolveExtractedTextPath,
              })).trim();
              if (state.activeTurn !== turn || turn.interrupted || turn.finalized) {
                return yield* protocolError(
                  `OpenCode turn ${steerInput.providerTurnId} is not active`,
                );
              }
              const files = OpenCodeRuntime.toOpenCodeFileParts({
                attachments: steerInput.message.attachments,
                resolveAttachmentPath: host.resolveAttachmentPath,
              });
              if (text.length === 0 && files.length === 0) {
                return yield* protocolError("OpenCode steering requires text or an attachment");
              }
              const parts = [
                ...(text.length === 0 ? [] : [{ type: "text" as const, text }]),
                ...files,
              ];
              turn.admissionGeneration = state.nextAdmissionGeneration++;
              turn.admissionMessageId = yield* makeOpenCodeMessageId(messageIdClock);
              turn.admissionPending = true;
              turn.admissionAccepted = false;
              turn.admissionMessageObserved = false;
              turn.idleDuringAdmission = false;
              turn.admissionSettled = Deferred.makeUnsafe<void>();
              // @effect-diagnostics-next-line abortControllerInEffect:off - Steering replaces the admission request independently of this command fiber.
              turn.admissionAbortController = new AbortController();
              if (turn.admissionMessageId !== null)
                turn.usage.promptMessageIds.add(turn.admissionMessageId);
              const admissionSettled = turn.admissionSettled;
              const admissionAbortController = turn.admissionAbortController;
              yield* submitPrompt(
                state,
                turn,
                {
                  sessionID: sessionId,
                  messageID: turn.admissionMessageId,
                  model: parsedModel,
                  ...turnPromptOptions(turn),
                  parts,
                },
                admissionAbortController!,
              ).pipe(
                Effect.ensuring(
                  Effect.all([
                    Deferred.succeed(admissionSettled, undefined).pipe(Effect.ignore),
                    Effect.sync(() => {
                      if (turn.admissionAbortController === admissionAbortController) {
                        turn.admissionAbortController = null;
                      }
                    }),
                  ]).pipe(Effect.asVoid),
                ),
              );
              if (state.activeTurn === turn && !turn.finalized) {
                const admissionAction = advanceOpenCodePromptAdmission(turn, "accepted");
                if (admissionAction === "reconcile-idle") {
                  yield* reconcilePromptAdmission(state, turn);
                }
              }
            },
            (effect, steerInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterSteerRunError({
                      driver: OPENCODE_PROVIDER,
                      providerThreadId: steerInput.providerThread.id,
                      providerTurnId: steerInput.providerTurnId,
                      cause,
                    }),
                ),
              ),
          ),
          interruptTurn: Effect.fn("OpencodeAdapter.interruptTurn")(
            function* (interruptInput) {
              const sessionId = nativeThreadId(interruptInput.providerThread);
              const state = threads.get(sessionId);
              const turn = state?.activeTurn;
              if (
                (turn === undefined || turn === null) &&
                interruptInput.requestRuntimeRestart === true
              ) {
                for (const controller of commandControllers.get(sessionId) ?? [])
                  controller.abort();
                yield* abortDescendants(sessionId);
                return;
              }
              if (
                turn === undefined ||
                turn === null ||
                turn.providerTurnId !== interruptInput.providerTurnId
              ) {
                return yield* protocolError(
                  `OpenCode turn ${interruptInput.providerTurnId} is not active`,
                );
              }
              turn.interrupted = true;
              for (const controller of commandControllers.get(sessionId) ?? []) controller.abort();
              const admissionWasPending = turn.admissionPending;
              cancelOpenCodePromptAdmission(turn, state!.nextAdmissionGeneration++);
              turn.admissionAbortController?.abort();
              if (admissionWasPending) {
                // Local admission timeout must not block the server-side abort.
                yield* Deferred.await(turn.admissionSettled).pipe(
                  Effect.timeout("10 seconds"),
                  Effect.ignore,
                );
              }
              yield* sdkCall("session.abort", { sessionID: sessionId }, (signal) =>
                client.session.abort({ sessionID: sessionId }, { signal }),
              ).pipe(
                Effect.timeout("10 seconds"),
                Effect.catch((cause) =>
                  turn.finalized
                    ? Effect.void
                    : Effect.suspend(() => {
                        turn.interrupted = false;
                        return Effect.fail(cause);
                      }),
                ),
              );
              // Root abort leaves children running; report incomplete cleanup even after root termination.
              yield* abortDescendants(sessionId);
            },
            (effect, interruptInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterInterruptError({
                      driver: OPENCODE_PROVIDER,
                      providerThreadId: interruptInput.providerThread.id,
                      providerTurnId: interruptInput.providerTurnId,
                      cause,
                    }),
                ),
              ),
          ),
          respondToRuntimeRequest: Effect.fn("OpencodeAdapter.respondToRuntimeRequest")(
            function* (requestInput) {
              const pending = pendingRequests.get(String(requestInput.requestId));
              if (pending === undefined) {
                return yield* protocolError(
                  `No pending OpenCode request ${requestInput.requestId}`,
                );
              }
              if (pending.question !== undefined) {
                if (requestInput.answers === undefined) {
                  return yield* protocolError(
                    `OpenCode question request ${requestInput.requestId} requires answers`,
                  );
                }
                const answers = OpenCodeRuntime.toOpenCodeQuestionAnswers(
                  pending.question,
                  requestInput.answers,
                );
                yield* sdkCall(
                  "question.reply",
                  { requestID: pending.nativeRequestId, answers },
                  (signal) =>
                    client.question.reply(
                      {
                        requestID: pending.nativeRequestId,
                        answers,
                      },
                      { signal },
                    ),
                ).pipe(Effect.timeout("10 seconds"));
                return;
              }
              if (requestInput.decision === undefined) {
                return yield* protocolError(
                  `OpenCode approval request ${requestInput.requestId} requires a decision`,
                );
              }
              const reply = OpenCodeRuntime.toOpenCodePermissionReply(requestInput.decision);
              yield* sdkCall(
                "permission.reply",
                { requestID: pending.nativeRequestId, reply },
                (signal) =>
                  client.permission.reply(
                    {
                      requestID: pending.nativeRequestId,
                      reply,
                    },
                    { signal },
                  ),
              ).pipe(Effect.timeout("10 seconds"));
            },
            (effect, requestInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                      driver: OPENCODE_PROVIDER,
                      requestId: requestInput.requestId,
                      cause,
                    }),
                ),
              ),
          ),
          readThreadSnapshot: (snapshotInput) =>
            readSnapshot(snapshotInput.providerThread).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                    driver: OPENCODE_PROVIDER,
                    providerThreadId: snapshotInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
          rollbackThread: Effect.fn("OpencodeAdapter.rollbackThread")(
            function* (rollbackInput) {
              const sessionId = nativeThreadId(rollbackInput.providerThread);
              const state = threads.get(sessionId);
              if (state?.activeTurn !== null && state?.activeTurn !== undefined) {
                return yield* protocolError(
                  `Cannot roll back OpenCode thread ${rollbackInput.providerThread.id} while a turn is active`,
                );
              }
              const response = yield* sdkCall("session.messages", { sessionID: sessionId }, () =>
                client.session.messages({ sessionID: sessionId }),
              );
              const messages = unwrapData("session.messages", response);
              let boundaryMessageId: string | undefined;
              if (rollbackInput.target.type === "thread_start") {
                boundaryMessageId = messages.find(({ info }) => info.role === "user")?.info.id;
              } else {
                boundaryMessageId = openCodeBoundaryAfterProviderTurn(
                  rollbackInput.providerThreadTurns,
                  rollbackInput.target.providerTurn.id,
                );
              }
              let retainedThread = rollbackInput.providerThread;
              if (boundaryMessageId !== undefined) {
                const boundaryIndex = messages.findIndex(
                  ({ info }) => info.id === boundaryMessageId,
                );
                if (boundaryIndex < 0)
                  return yield* protocolError(
                    "The OpenCode rewind boundary is no longer available.",
                  );
                const fork = unwrapData(
                  "session.fork",
                  yield* sdkCall(
                    "session.fork",
                    { sessionID: sessionId, messageID: boundaryMessageId },
                    () =>
                      client.session.fork({ sessionID: sessionId, messageID: boundaryMessageId }),
                  ),
                );
                const retained = unwrapData(
                  "session.messages",
                  yield* sdkCall("session.messages", { sessionID: fork.id }, () =>
                    client.session.messages({ sessionID: fork.id }),
                  ),
                );
                if (retained.length !== boundaryIndex)
                  return yield* protocolError(
                    "OpenCode did not preserve the requested rewind boundary.",
                  );
                yield* sdkCall("session.update", { sessionID: fork.id }, () =>
                  client.session.update({
                    sessionID: fork.id,
                    permission: permissions(input.runtimePolicy),
                  }),
                );
                retainedThread = {
                  ...rollbackInput.providerThread,
                  nativeThreadRef: providerRef(fork.id),
                };
                registerThread(fork, retainedThread, state?.appThread ?? null);
              }
              const snapshot = yield* readSnapshot(retainedThread);
              return {
                ...snapshot,
                providerThread: {
                  ...snapshot.providerThread,
                  nativeConversationHeadRef:
                    rollbackInput.target.type === "provider_turn"
                      ? rollbackInput.target.providerTurn.nativeTurnRef
                      : null,
                },
              };
            },
            (effect, rollbackInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterRollbackThreadError({
                      driver: OPENCODE_PROVIDER,
                      providerThreadId: rollbackInput.providerThread.id,
                      checkpointId: rollbackInput.target.checkpointId,
                      cause,
                    }),
                ),
              ),
          ),
          forkThread: Effect.fn("OpencodeAdapter.forkThread")(
            function* (forkInput) {
              const sourceSessionId = nativeThreadId(forkInput.sourceProviderThread);
              const sourceState = threads.get(sourceSessionId);
              if (sourceState?.activeTurn !== null && sourceState?.activeTurn !== undefined) {
                return yield* protocolError(
                  `Cannot fork OpenCode thread ${forkInput.sourceProviderThread.id} while a turn is active`,
                );
              }
              let boundaryMessageId: string | undefined;
              if (forkInput.providerTurnId !== undefined) {
                const sourceTurns = forkInput.sourceProviderTurns ?? [];
                const selected = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
                if (selected === undefined) {
                  return yield* protocolError(
                    `OpenCode fork boundary turn ${forkInput.providerTurnId} was not found`,
                  );
                }
                boundaryMessageId = openCodeBoundaryAfterProviderTurn(sourceTurns, selected.id);
              }
              const response = yield* sdkCall(
                "session.fork",
                {
                  sessionID: sourceSessionId,
                  ...(boundaryMessageId === undefined ? {} : { messageID: boundaryMessageId }),
                },
                () =>
                  client.session.fork({
                    sessionID: sourceSessionId,
                    ...(boundaryMessageId === undefined ? {} : { messageID: boundaryMessageId }),
                  }),
              );
              const nativeSession = unwrapData("session.fork", response);
              const forkedAt = yield* DateTime.now;
              const providerThread = makeProviderThread({
                idAllocator,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: forkInput.targetThreadId,
                ...(forkInput.ownerNodeId === undefined
                  ? {}
                  : { ownerNodeId: forkInput.ownerNodeId }),
                nativeSession,
                forkedFrom: {
                  providerThreadId: forkInput.sourceProviderThread.id,
                  ...(forkInput.providerTurnId === undefined
                    ? {}
                    : { providerTurnId: forkInput.providerTurnId }),
                },
                now: forkedAt,
              });
              registerThread(nativeSession, providerThread, null);
              return providerThread;
            },
            (effect, forkInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterForkThreadError({
                      driver: OPENCODE_PROVIDER,
                      providerThreadId: forkInput.sourceProviderThread.id,
                      cause,
                    }),
                ),
              ),
          ),
        };

        return runtimeSession;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: OPENCODE_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
});

export type OpenCodeAdapterV2DriverEnv =
  | FileSystem.FileSystem
  | OpenCodeRuntime.OpenCodeRuntime
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderEventLoggers.ProviderEventLoggers
  | ProviderHost.ProviderHost;

export const OpenCodeAdapterV2Driver: ProviderAdapterDriver<
  OpenCodeSettings,
  OpenCodeAdapterV2DriverEnv
> = {
  driverKind: OPENCODE_PROVIDER,
  configSchema: OpenCodeSettings,
  defaultConfig: (): OpenCodeSettings => DEFAULT_OPENCODE_SETTINGS,
  create: Effect.fn("OpenCodeAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<OpenCodeSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const providerEventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
      return yield* makeOpenCodeAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        ...(providerEventLoggers.native === undefined
          ? {}
          : { nativeEventLogger: providerEventLoggers.native }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: OPENCODE_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create OpenCode v2 adapter.",
              cause,
            }),
        ),
      ),
  ),
};
