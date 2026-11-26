import type { SDKResultMessage } from "@anthropic-ai/claude-agent-sdk";
import type {
  RuntimeTurnState,
  TurnTokenUsage,
  UsageClosure,
  UsageClosureOpenReason,
} from "@t3tools/contracts";

function finiteNonNegativeInteger(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value)
    : undefined;
}

const BILLED_USAGE_FIELDS = [
  "input_tokens",
  "cache_read_input_tokens",
  "cache_creation_input_tokens",
  "output_tokens",
] as const;

function addKnownCount(finalCount: unknown, earlierCounts: ReadonlyArray<unknown>): unknown {
  const counts = [finalCount, ...earlierCounts]
    .map(finiteNonNegativeInteger)
    .filter((count) => count !== undefined);
  return counts.length === 0 ? finalCount : counts.reduce((sum, count) => sum + count, 0);
}

// Steering aborts the current segment; each SDK result reports only that segment's usage.
function withEarlierSegmentUsage(
  usage: Record<string, unknown>,
  earlierSegmentUsage: ReadonlyArray<unknown>,
): Record<string, unknown> {
  if (earlierSegmentUsage.length === 0) return usage;
  const segments = earlierSegmentUsage.map((segment) => (segment ?? {}) as Record<string, unknown>);
  const total = { ...usage };
  for (const field of BILLED_USAGE_FIELDS) {
    total[field] = addKnownCount(
      usage[field],
      segments.map((segment) => segment[field]),
    );
  }
  const outputDetails = usage.output_tokens_details as Record<string, unknown> | undefined;
  total.output_tokens_details = {
    ...outputDetails,
    thinking_tokens: addKnownCount(
      outputDetails?.thinking_tokens,
      segments.map(
        (segment) =>
          (segment.output_tokens_details as Record<string, unknown> | undefined)?.thinking_tokens,
      ),
    ),
  };
  return total;
}

export interface ClaudeUsageCounters {
  // Uncached input, as the API's input_tokens.
  readonly input: number;
  readonly cacheRead: number;
  readonly cacheCreation: number;
  readonly output: number;
}

const ZERO_COUNTERS: ClaudeUsageCounters = { input: 0, cacheRead: 0, cacheCreation: 0, output: 0 };
const COUNTER_FIELDS = ["input", "cacheRead", "cacheCreation", "output"] as const;

function addCounters(left: ClaudeUsageCounters, right: ClaudeUsageCounters): ClaudeUsageCounters {
  return {
    input: left.input + right.input,
    cacheRead: left.cacheRead + right.cacheRead,
    cacheCreation: left.cacheCreation + right.cacheCreation,
    output: left.output + right.output,
  };
}

/** Per-model totals from the SDK's `modelUsage`, which grows for the life of one CLI process. */
export type ClaudeModelLedger = ReadonlyMap<string, ClaudeUsageCounters>;

/** Ledger growth across one or more results, by model; models that did not grow are absent. */
export interface ClaudeLedgerUsage {
  readonly byModel: ReadonlyMap<string, ClaudeUsageCounters>;
  // Measured from a baseline restored at process start, which only an exact match with the
  // main agent's usage proves.
  readonly restoredBaseline: boolean;
}

/** Unknown when absent or empty: a process that did model work never reports an empty ledger. */
export function claudeModelLedger(modelUsage: unknown): ClaudeModelLedger | undefined {
  if (typeof modelUsage !== "object" || modelUsage === null) return undefined;
  if (Object.keys(modelUsage).length === 0) return undefined;
  const ledger = new Map<string, ClaudeUsageCounters>();
  for (const [model, entry] of Object.entries(modelUsage as Record<string, unknown>)) {
    const fields = (entry ?? {}) as Record<string, unknown>;
    const input = finiteNonNegativeInteger(fields.inputTokens);
    const output = finiteNonNegativeInteger(fields.outputTokens);
    if (input === undefined || output === undefined) return undefined;
    ledger.set(model, {
      input,
      cacheRead: finiteNonNegativeInteger(fields.cacheReadInputTokens) ?? 0,
      cacheCreation: finiteNonNegativeInteger(fields.cacheCreationInputTokens) ?? 0,
      output,
    });
  }
  return ledger;
}

/** The ledger in a transcript's last cost-state record, which Claude restores on resume. */
export function claudeTranscriptRestoredLedger(
  lines: ReadonlyArray<string>,
  sessionId: string,
): ClaudeModelLedger | undefined {
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (line === undefined || !line.includes('"cost-state"')) continue;
    let record: unknown;
    try {
      record = JSON.parse(line);
    } catch {
      continue;
    }
    if (typeof record !== "object" || record === null) continue;
    const fields = record as Record<string, unknown>;
    if (fields.type !== "cost-state" || fields.sessionId !== sessionId) continue;
    const modelUsage = fields.modelUsage;
    // A session that never called a model records an empty ledger or none at all.
    if (modelUsage === undefined) return new Map();
    return typeof modelUsage === "object" &&
      modelUsage !== null &&
      Object.keys(modelUsage).length === 0
      ? new Map()
      : claudeModelLedger(modelUsage);
  }
  return undefined;
}

/** The window each model in the SDK's `modelUsage` is measured against, as Claude Code resolved it. */
export function claudeReportedContextWindows(modelUsage: unknown): ReadonlyMap<string, number> {
  const windows = new Map<string, number>();
  if (typeof modelUsage !== "object" || modelUsage === null) return windows;
  for (const [model, entry] of Object.entries(modelUsage as Record<string, unknown>)) {
    const window = finiteNonNegativeInteger(
      (entry as Record<string, unknown> | null)?.contextWindow,
    );
    if (window !== undefined && window > 0) windows.set(model, window);
  }
  return windows;
}

/** Ledger growth since `previous`. Unknown without a baseline or when a counter goes backwards. */
export function claudeLedgerDelta(
  previous: ClaudeModelLedger | undefined,
  current: ClaudeModelLedger | undefined,
  restoredBaseline = false,
): ClaudeLedgerUsage | undefined {
  if (previous === undefined || current === undefined) return undefined;
  for (const [model, before] of previous) {
    const after = current.get(model);
    if (after === undefined || COUNTER_FIELDS.some((field) => after[field] < before[field])) {
      return undefined;
    }
  }
  const byModel = new Map<string, ClaudeUsageCounters>();
  for (const [model, after] of current) {
    const before = previous.get(model) ?? ZERO_COUNTERS;
    const growth = {
      input: after.input - before.input,
      cacheRead: after.cacheRead - before.cacheRead,
      cacheCreation: after.cacheCreation - before.cacheCreation,
      output: after.output - before.output,
    };
    if (COUNTER_FIELDS.some((field) => growth[field] > 0)) byModel.set(model, growth);
  }
  return { byModel, restoredBaseline };
}

export function addClaudeLedgerUsage(
  left: ClaudeLedgerUsage | undefined,
  right: ClaudeLedgerUsage | undefined,
): ClaudeLedgerUsage | undefined {
  if (left === undefined || right === undefined) return undefined;
  const byModel = new Map(left.byModel);
  for (const [model, growth] of right.byModel) {
    byModel.set(model, addCounters(byModel.get(model) ?? ZERO_COUNTERS, growth));
  }
  return { byModel, restoredBaseline: left.restoredBaseline || right.restoredBaseline };
}

function claudeLedgerTotal(ledger: ClaudeLedgerUsage): ClaudeUsageCounters {
  let total = ZERO_COUNTERS;
  for (const growth of ledger.byModel.values()) total = addCounters(total, growth);
  return total;
}

function sameCounters(left: ClaudeUsageCounters, right: ClaudeUsageCounters): boolean {
  return COUNTER_FIELDS.every((field) => left[field] === right[field]);
}

export interface ClaudeTurnObservations {
  // Models on the turn's root assistant messages.
  readonly rootModels?: ReadonlySet<string>;
  // Ledger growth across every result this turn consumed, including steered segments.
  readonly ledger?: ClaudeLedgerUsage | undefined;
}

export function normalizeClaudeTurnTokenUsage(
  result:
    | {
        readonly subtype: SDKResultMessage["subtype"];
        readonly usage?: unknown;
        readonly local_command?: unknown;
      }
    | undefined,
  hasSubagents: boolean,
  terminalStatus: RuntimeTurnState,
  earlierSegmentUsage: ReadonlyArray<unknown> = [],
  observed: ClaudeTurnObservations = {},
): TurnTokenUsage {
  const observedFields = {
    ...(observed.rootModels !== undefined && observed.rootModels.size > 0
      ? { models: [...observed.rootModels] }
      : {}),
    ...(typeof result?.local_command === "string" && result.local_command.length > 0
      ? { localCommand: true }
      : {}),
  };
  const finalUsage = result?.usage as Record<string, unknown> | undefined;
  const usage = withEarlierSegmentUsage(finalUsage ?? {}, earlierSegmentUsage);
  const uncachedInputTokens = finiteNonNegativeInteger(usage.input_tokens);
  const cachedInputTokens = finiteNonNegativeInteger(usage.cache_read_input_tokens);
  const cacheCreationTokens = finiteNonNegativeInteger(usage.cache_creation_input_tokens);
  const rawOutputTokens = finiteNonNegativeInteger(usage.output_tokens);
  const outputDetails = usage.output_tokens_details as Record<string, unknown> | undefined;
  const thinkingTokens = finiteNonNegativeInteger(outputDetails?.thinking_tokens);
  const cachedInputContribution = usage.cache_read_input_tokens == null ? 0 : cachedInputTokens;
  const cacheCreationContribution =
    usage.cache_creation_input_tokens == null ? 0 : cacheCreationTokens;
  const inputTokens =
    uncachedInputTokens !== undefined &&
    cachedInputContribution !== undefined &&
    cacheCreationContribution !== undefined
      ? uncachedInputTokens + cachedInputContribution + cacheCreationContribution
      : undefined;
  const hasKnownUsage =
    uncachedInputTokens !== undefined ||
    cachedInputTokens !== undefined ||
    cacheCreationTokens !== undefined ||
    rawOutputTokens !== undefined;
  const hasPositiveUsage =
    (uncachedInputTokens ?? 0) +
      (cachedInputTokens ?? 0) +
      (cacheCreationTokens ?? 0) +
      (rawOutputTokens ?? 0) >
    0;

  if (!hasKnownUsage || (result?.subtype !== "success" && !hasPositiveUsage)) {
    return {
      usageStatus: "unavailable",
      usageScope: "main_agent",
      hasSubagents,
      ...observedFields,
    };
  }

  // The result omits subagent and compaction work that the process-wide ledger records.
  // Growth from a restored baseline may hold an earlier session's usage, so it is
  // never reported as other usage unless it equals the main agent's exactly.
  const ledgerTotal =
    observed.ledger === undefined ? undefined : claudeLedgerTotal(observed.ledger);
  const mainCounters =
    uncachedInputTokens !== undefined &&
    cachedInputContribution !== undefined &&
    cacheCreationContribution !== undefined &&
    rawOutputTokens !== undefined
      ? {
          input: uncachedInputTokens,
          cacheRead: cachedInputContribution,
          cacheCreation: cacheCreationContribution,
          output: rawOutputTokens,
        }
      : undefined;
  const otherUsage =
    ledgerTotal !== undefined &&
    mainCounters !== undefined &&
    inputTokens !== undefined &&
    rawOutputTokens !== undefined &&
    (observed.ledger?.restoredBaseline !== true || sameCounters(ledgerTotal, mainCounters)) &&
    ledgerTotal.input + ledgerTotal.cacheRead + ledgerTotal.cacheCreation >= inputTokens &&
    ledgerTotal.output >= rawOutputTokens
      ? {
          inputTokens:
            ledgerTotal.input + ledgerTotal.cacheRead + ledgerTotal.cacheCreation - inputTokens,
          outputTokens: ledgerTotal.output - rawOutputTokens,
        }
      : undefined;
  const commonUsage = {
    ...observedFields,
    ...(otherUsage === undefined ? {} : { otherUsage }),
    usageScope: "main_agent",
    ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
    ...(cacheCreationTokens !== undefined ? { cacheCreationTokens } : {}),
    ...(thinkingTokens !== undefined && rawOutputTokens !== undefined
      ? { reasoningTokens: Math.min(rawOutputTokens, thinkingTokens) }
      : {}),
    hasSubagents,
  } as const;
  if (
    terminalStatus === "completed" &&
    result?.subtype === "success" &&
    finiteNonNegativeInteger(finalUsage?.input_tokens) !== undefined &&
    finiteNonNegativeInteger(finalUsage?.output_tokens) !== undefined &&
    inputTokens !== undefined &&
    rawOutputTokens !== undefined &&
    BILLED_USAGE_FIELDS.every(
      (field) =>
        finalUsage?.[field] == null || finiteNonNegativeInteger(finalUsage[field]) !== undefined,
    )
  ) {
    return {
      ...commonUsage,
      usageStatus: "complete",
      inputTokens,
      outputTokens: rawOutputTokens,
    };
  }
  return {
    ...commonUsage,
    usageStatus: "partial",
    ...(inputTokens !== undefined ? { inputTokens } : {}),
    ...(rawOutputTokens !== undefined ? { outputTokens: rawOutputTokens } : {}),
  };
}

export interface ClaudeUsageClosureInput {
  readonly terminalStatus: RuntimeTurnState;
  readonly usage: TurnTokenUsage;
  // Ledger growth across every result the turn consumed; undefined when any is unknown.
  readonly ledger: ClaudeLedgerUsage | undefined;
  readonly rootModels: ReadonlySet<string>;
  // A subagent, workflow member or other model-capable native task ran in the turn.
  readonly nativeChildObserved: boolean;
  // One is still on the session roster at the terminal, or of a type not known to be a command.
  readonly nativeChildRunning: boolean;
}

/** Closed only when the CLI's all-participant ledger grew by exactly the main agent's usage. */
export function claudeUsageClosure(input: ClaudeUsageClosureInput): UsageClosure {
  const open = (reason: UsageClosureOpenReason): UsageClosure => ({
    version: 1,
    state: "open",
    reason,
  });
  if (input.terminalStatus !== "completed") return open("turn_not_completed");
  const usage = input.usage;
  if (
    usage.usageStatus !== "complete" ||
    usage.inputTokens === undefined ||
    usage.outputTokens === undefined
  ) {
    return open("usage_incomplete");
  }
  if (input.nativeChildObserved) return open("native_child_observed");
  if (input.nativeChildRunning) return open("native_child_running");
  if (input.ledger === undefined) return open("ledger_baseline_unproven");
  const cacheRead = usage.cachedInputTokens ?? 0;
  const cacheCreation = usage.cacheCreationTokens ?? 0;
  const matches = sameCounters(claudeLedgerTotal(input.ledger), {
    input: usage.inputTokens - cacheRead - cacheCreation,
    cacheRead,
    cacheCreation,
    output: usage.outputTokens,
  });
  if (input.ledger.restoredBaseline && !matches) return open("ledger_baseline_unproven");
  if (
    input.rootModels.size > 1 ||
    [...input.ledger.byModel.keys()].some((model) => !input.rootModels.has(model))
  ) {
    return open("ledger_growth_outside_root_model");
  }
  if (!matches) return open("ledger_mismatch");
  return {
    version: 1,
    state: "main_agent_only",
    witness: "process_ledger",
    uncountedCompactions: 0,
  };
}
