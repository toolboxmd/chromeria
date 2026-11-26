import type { TurnTokenUsage, UsageClosure, UsageClosureOpenReason } from "@t3tools/contracts";
import type * as EffectCodexSchema from "effect-codex-app-server/schema";

type CodexCumulativeTokenUsage = {
  readonly inputTokens: number;
  readonly cachedInputTokens: number;
  readonly cacheCreationTokens?: number;
  readonly outputTokens: number;
  readonly reasoningTokens: number;
};

interface CodexTurnTokenUsageAccumulator {
  inputTokens: number;
  cachedInputTokens: number;
  cacheCreationTokens: number | undefined;
  outputTokens: number;
  reasoningTokens: number;
  observed: boolean;
  hasSubagents: boolean;
  // A thread-wide total went backwards mid-turn, so its counter fell back to single responses.
  streamBroken: boolean;
  // The thread's total grew under another turn id while this turn ran.
  unattributed: boolean;
}

export interface CodexTurnTokenUsageState {
  baseline: CodexCumulativeTokenUsage | undefined;
  activeTurnId: string | undefined;
  readonly byTurnId: Map<string, CodexTurnTokenUsageAccumulator>;
}

function codexTokenUsageBreakdown(
  usage: EffectCodexSchema.V2ThreadTokenUsageUpdatedNotification__TokenUsageBreakdown,
): CodexCumulativeTokenUsage {
  return {
    inputTokens: usage.inputTokens,
    cachedInputTokens: usage.cachedInputTokens,
    ...(usage.cacheWriteInputTokens !== undefined
      ? { cacheCreationTokens: usage.cacheWriteInputTokens }
      : {}),
    outputTokens: usage.outputTokens,
    reasoningTokens: usage.reasoningOutputTokens,
  };
}

export function makeCodexTurnTokenUsageState(): CodexTurnTokenUsageState {
  return {
    baseline: undefined,
    activeTurnId: undefined,
    byTurnId: new Map(),
  };
}

export function getCodexTurnAccumulator(
  state: CodexTurnTokenUsageState,
  turnId: string,
): CodexTurnTokenUsageAccumulator {
  const existing = state.byTurnId.get(turnId);
  if (existing) return existing;
  const created: CodexTurnTokenUsageAccumulator = {
    inputTokens: 0,
    cachedInputTokens: 0,
    cacheCreationTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    observed: false,
    hasSubagents: false,
    streamBroken: false,
    unattributed: false,
  };
  state.byTurnId.set(turnId, created);
  return created;
}

function codexTotalRegressed(
  previous: CodexCumulativeTokenUsage,
  current: CodexCumulativeTokenUsage,
): boolean {
  return (
    current.inputTokens < previous.inputTokens ||
    current.cachedInputTokens < previous.cachedInputTokens ||
    current.outputTokens < previous.outputTokens ||
    current.reasoningTokens < previous.reasoningTokens
  );
}

/** total is thread-wide; last covers the newest model response. */
function codexTurnTokenUsageDelta(
  previous: CodexCumulativeTokenUsage | undefined,
  current: CodexCumulativeTokenUsage,
  last: CodexCumulativeTokenUsage,
): CodexCumulativeTokenUsage {
  if (previous === undefined || codexTotalRegressed(previous, current)) {
    return last;
  }
  return {
    inputTokens: current.inputTokens - previous.inputTokens,
    cachedInputTokens: current.cachedInputTokens - previous.cachedInputTokens,
    ...(current.cacheCreationTokens !== undefined &&
    previous.cacheCreationTokens !== undefined &&
    current.cacheCreationTokens >= previous.cacheCreationTokens
      ? { cacheCreationTokens: current.cacheCreationTokens - previous.cacheCreationTokens }
      : {}),
    outputTokens: current.outputTokens - previous.outputTokens,
    reasoningTokens: current.reasoningTokens - previous.reasoningTokens,
  };
}

export function accumulateCodexTurnTokenUsage(
  state: CodexTurnTokenUsageState,
  turnId: string,
  usage: EffectCodexSchema.V2ThreadTokenUsageUpdatedNotification["tokenUsage"],
): void {
  const current = codexTokenUsageBreakdown(usage.total);
  if (state.activeTurnId !== turnId) {
    // Every thread-wide total updates the baseline; late finished-turn updates do not count toward the live turn.
    if (
      state.activeTurnId !== undefined &&
      state.baseline !== undefined &&
      !codexTotalRegressed(state.baseline, current) &&
      (current.inputTokens > state.baseline.inputTokens ||
        current.outputTokens > state.baseline.outputTokens)
    ) {
      getCodexTurnAccumulator(state, state.activeTurnId).unattributed = true;
    }
    state.baseline = current;
    return;
  }

  const accumulator = getCodexTurnAccumulator(state, turnId);
  if (state.baseline !== undefined && codexTotalRegressed(state.baseline, current)) {
    accumulator.streamBroken = true;
  }
  const delta = codexTurnTokenUsageDelta(
    state.baseline,
    current,
    codexTokenUsageBreakdown(usage.last),
  );
  state.baseline = current;

  if (
    delta.inputTokens > 0 ||
    delta.cachedInputTokens > 0 ||
    delta.outputTokens > 0 ||
    delta.reasoningTokens > 0
  ) {
    accumulator.observed = true;
  }
  accumulator.inputTokens += delta.inputTokens;
  accumulator.cachedInputTokens += delta.cachedInputTokens;
  accumulator.outputTokens += delta.outputTokens;
  accumulator.reasoningTokens += delta.reasoningTokens;
  if (delta.cacheCreationTokens === undefined) {
    accumulator.cacheCreationTokens = undefined;
  } else if (accumulator.cacheCreationTokens !== undefined) {
    accumulator.cacheCreationTokens += delta.cacheCreationTokens;
  }
}

export interface CodexTurnClosureObservations {
  // A child thread from an earlier turn still ran, or a child turn had no known parent.
  readonly childThreadActive: boolean;
  // A prompt suggestion samples the thread after this turn's usage is final.
  readonly promptSampling: boolean;
  readonly compactions: number;
}

/** `model` is the thread model Codex last reported, which a reroute replaces. */
export function completeCodexTurnTokenUsage(
  state: CodexTurnTokenUsageState,
  turnId: string,
  completed: boolean,
  model?: string,
  observations: CodexTurnClosureObservations = {
    childThreadActive: false,
    promptSampling: false,
    compactions: 0,
  },
): { readonly turnTokenUsage: TurnTokenUsage; readonly usageClosure: UsageClosure } {
  const usage = state.byTurnId.get(turnId);
  state.byTurnId.delete(turnId);
  if (state.activeTurnId === turnId) state.activeTurnId = undefined;
  const models = model === undefined ? {} : { models: [model] };
  const open = (reason: UsageClosureOpenReason): UsageClosure => ({
    version: 1,
    state: "open",
    reason,
  });
  if (!usage?.observed) {
    return {
      turnTokenUsage: {
        usageStatus: "unavailable",
        usageScope: "main_agent",
        hasSubagents: usage?.hasSubagents ?? false,
        ...models,
      },
      usageClosure: open(completed ? "usage_incomplete" : "turn_not_completed"),
    };
  }

  // Codex counts cache reads and writes inside inputTokens.
  const turnTokenUsage: TurnTokenUsage = {
    usageStatus: completed ? "complete" : "partial",
    usageScope: "main_agent",
    inputTokens: usage.inputTokens,
    cachedInputTokens: Math.min(usage.inputTokens, usage.cachedInputTokens),
    ...(usage.cacheCreationTokens !== undefined
      ? { cacheCreationTokens: Math.min(usage.inputTokens, usage.cacheCreationTokens) }
      : {}),
    outputTokens: usage.outputTokens,
    reasoningTokens: Math.min(usage.outputTokens, usage.reasoningTokens),
    hasSubagents: usage.hasSubagents,
    ...models,
  };
  // A compaction's own usage never reaches thread/tokenUsage/updated, so it is counted, not closed over.
  const usageClosure: UsageClosure = !completed
    ? open("turn_not_completed")
    : usage.hasSubagents
      ? open("native_child_observed")
      : observations.childThreadActive
        ? open("native_child_running")
        : usage.streamBroken
          ? open("usage_stream_broken")
          : usage.unattributed
            ? open("usage_unattributed")
            : observations.promptSampling
              ? open("prompt_suggestion_sampled")
              : {
                  version: 1,
                  state: "main_agent_only",
                  witness: "event_stream",
                  uncountedCompactions: observations.compactions,
                };
  return { turnTokenUsage, usageClosure };
}
