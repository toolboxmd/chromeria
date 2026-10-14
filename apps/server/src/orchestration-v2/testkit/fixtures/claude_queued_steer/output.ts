import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessageInputIntents,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  MESSAGE_STEERING_INITIAL_PROMPT,
  MESSAGE_STEERING_STEER_PROMPT,
  projectionFor,
} from "../shared.ts";

function framePriority(frame: unknown): unknown {
  if (typeof frame !== "object" || frame === null || !("message" in frame)) return undefined;
  const message = frame.message;
  if (typeof message !== "object" || message === null || !("priority" in message)) return undefined;
  return message.priority;
}

export function assertClaudeQueuedSteerOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assert.equal(transcript.provider, "claudeAgent");
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertRunOrdinals(projection, [1, 2]);
  assertTurnItemTypes(projection, ["user_message", "assistant_message"]);
  assertUserMessagesInclude(projection, [
    MESSAGE_STEERING_INITIAL_PROMPT,
    MESSAGE_STEERING_STEER_PROMPT,
  ]);
  assertUserMessageInputIntents(projection, ["turn_start", "queued_turn"]);
  assertAssistantTextIncludes(projection, "steering fixture initial response");
  assertAssistantTextIncludes(projection, "steering fixture observed");
  assert.equal(projection.providerTurns.length, 2);
  assert.isTrue(
    transcript.entries
      .filter((entry) => entry.type === "expect_outbound")
      .every((entry) => framePriority(entry.frame) === undefined),
    "a Claude steer must not be offered with priority now",
  );
}
