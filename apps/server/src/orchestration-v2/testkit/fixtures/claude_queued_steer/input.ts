import {
  MESSAGE_STEERING_INITIAL_PROMPT,
  MESSAGE_STEERING_STEER_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

const CLAUDE_STEER_HELD_RESULT = "claude-steer-held-result";

/**
 * The steer is dispatched while Claude's first turn is still running. The
 * held result is released only after that steer has been queued, so the
 * follow-up cannot be offered with priority `now`.
 */
export function claudeQueuedSteerInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: MESSAGE_STEERING_INITIAL_PROMPT },
      {
        type: "steer",
        text: MESSAGE_STEERING_STEER_PROMPT,
        targetRunIndex: 1,
      },
      { type: "release_replay_gate", label: CLAUDE_STEER_HELD_RESULT },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
    ],
  };
}
