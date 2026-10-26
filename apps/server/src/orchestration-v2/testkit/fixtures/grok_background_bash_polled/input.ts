import type { OrchestratorFixtureInput } from "../shared.ts";

const GROK_BACKGROUND_BASH_POLLED_PROMPT =
  "Run the build, lint and test commands, wait for all three, then reply exactly POLLED_DONE.";

// Grok 1.0.46 backgrounds each command as `call-<uuid>-<n>`, ends it with `task_completed` before
// the poll result (one without output), then reports one via TaskOutput `Result` and two via one
// `MultiResult`. Every command ends inside the turn, so nothing holds run 1.
export function grokBackgroundBashPolledInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: GROK_BACKGROUND_BASH_POLLED_PROMPT }],
  };
}
