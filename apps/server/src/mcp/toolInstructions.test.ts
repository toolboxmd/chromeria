import { describe, expect, it } from "vite-plus/test";
import { buildRuntimeInstructions } from "../provider/RuntimeInstructions.ts";

describe("t3-code tool instructions", () => {
  it.each(["Claude Code", "Codex", "Cursor", "Grok", "OpenCode", "Antigravity"])(
    "tell %s agents how to route delegated work",
    (harness) => {
      const instructions = buildRuntimeInstructions({ harness });
      expect(instructions).toContain("<t3_code_tool_use>");
      const routing = [
        "Delegate with spawn_thread and a role (worker with a lane by difficulty, reviewer); Prism picks the model.",
        "For a job that ends in one PR, start spawn_thread(role: dispatcher) with the brief, unless the user or a comparison job says to coordinate it yourself.",
        "When a child fails, hits a limit or goes stale, interrupt_thread it and, once it reports settled, spawn retry, then escalation, then ask whoever started you.",
        "Only if no dispatcher can start, fall back to prism_submit",
      ];
      const routingPositions = routing.map((rule) => instructions.indexOf(rule));
      expect(routingPositions.every((position) => position >= 0)).toBe(true);
      expect(routingPositions).toEqual([...routingPositions].sort((a, b) => a - b));
      expect(instructions).toContain(
        "or when a specific model and effort is wanted, call spawn_thread",
      );
      expect(instructions).toContain(
        "never by starting codex exec, claude -p, opencode run or grok in the shell",
      );
      const defectSteps = [
        "have a spawn_thread child with role reviewer confirm the defect from prism_status and ~/.local/share/durable-runner/outputs/<requestId>/",
        "then open or update the toolboxmd/model-router Issue with that confirmation",
        "then start a spawn_thread child with role worker on the fix in its own worktree, with a normal PR and review",
        "Meanwhile keep the job going or recover it.",
      ];
      const positions = defectSteps.map((step) => instructions.indexOf(step));
      expect(positions.every((position) => position >= 0)).toBe(true);
      expect(positions).toEqual([...positions].sort((a, b) => a - b));
      expect(instructions).toContain(
        "Comment an unconfirmed suspicion on an existing Issue or drop it; never file it as a new Issue.",
      );
    },
  );

  it("keep the Codex runtime entry inside its 1,000-token additional-context cap", () => {
    // Codex cuts the middle of any additionalContext value above 1,000 tokens.
    // Three characters per token is a conservative bound for this English text.
    const instructions = buildRuntimeInstructions({
      harness: "Codex",
      model: "gpt-5.5",
      modelName: "GPT-5.5",
      reasoningEffort: "xhigh",
    });
    expect(instructions.length).toBeLessThan(3_000);
  });
});
