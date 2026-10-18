import type { OrchestratorFixtureInput } from "../shared.ts";
import { GROK_BACKGROUND_BASH_PROMPT } from "../grok_background_bash/input.ts";

// Same prompt as grok_background_bash, recorded from Grok 1.0.44. Older Grok
// (1.0.41) waited about a minute after `x.ai/task_completed` before starting
// its `task-completed-*` reply; 1.0.44 starts it within ~2s. Frames replay
// ungated: the root and the later background reply must remain separate runs.
export function grokBackgroundBashFastWakeInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: GROK_BACKGROUND_BASH_PROMPT },
      { type: "await_run_status", targetRunIndex: 1, status: "completed" },
      { type: "finish_held_run", targetRunIndex: 2, status: "completed" },
    ],
  };
}
