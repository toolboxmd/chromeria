import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import { CLAUDE_IDLE_RESUME_PROMPT_1, CLAUDE_IDLE_RESUME_PROMPT_2 } from "./input.ts";

export function assertClaudeIdleResumeOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [CLAUDE_IDLE_RESUME_PROMPT_1, CLAUDE_IDLE_RESUME_PROMPT_2]);

  const assistantTexts = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.deepEqual(assistantTexts, [
    "idle resume first turn complete",
    "idle resume second turn complete",
  ]);
  // The resumed process has no transcript cost-state to prove its baseline, so the
  // first turn is never counted again and the resumed turn makes no other-usage claim.
  assert.deepEqual(
    projection.providerTurns.map((turn) => [
      turn.turnTokenUsage?.outputTokens,
      turn.turnTokenUsage?.otherUsage,
      turn.usageClosure?.state === "open" ? turn.usageClosure.reason : turn.usageClosure?.state,
    ]),
    [
      [7, { inputTokens: 0, outputTokens: 0 }, "main_agent_only"],
      [7, undefined, "ledger_baseline_unproven"],
    ],
  );
}
