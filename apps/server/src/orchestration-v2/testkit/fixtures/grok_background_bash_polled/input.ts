import type { OrchestratorFixtureInput } from "../shared.ts";

export const GROK_BACKGROUND_BASH_POLLED_PROMPT =
  "Run the build, lint and test commands, wait for all three, then reply exactly POLLED_DONE.";

export const GROK_BACKGROUND_BASH_POLLED_COMMANDS = [
  { description: "Build the app", output: "build ok\n" },
  { description: "Lint the app", output: "lint ok\n" },
  { description: "Test the app", output: "test ok\n" },
] as const;

// Grok 1.0.46 moves each command to the background with a `call-<uuid>-<n>`
// task id, ends it with `task_completed` before the poll result, and reports
// the first through a TaskOutput `Result` and the other two through one
// `MultiResult`. Every command ends inside the turn, so nothing holds run 1.
export function grokBackgroundBashPolledInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: GROK_BACKGROUND_BASH_POLLED_PROMPT }],
  };
}
