import { PRISM_DELEGATE_ROLES } from "@t3tools/contracts";
import { T3_CODE_TOOL_USE_INSTRUCTIONS } from "@t3tools/shared/forkToolInstructions";
import { describe, expect, it } from "vite-plus/test";

import {
  T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  T3_CODE_ORCHESTRATION_SECTIONS,
} from "./orchestrationInstructions.ts";

// Fork (toolboxmd/chromeria#571, #565): Prism delegation guidance must survive upstream copy cuts.
describe("fork orchestration guidance", () => {
  it("names every Prism delegate role where agents read their guidance", () => {
    for (const role of PRISM_DELEGATE_ROLES) {
      expect(T3_CODE_ORCHESTRATION_INSTRUCTIONS).toContain(role);
      expect(T3_CODE_TOOL_USE_INSTRUCTIONS).toContain(role);
    }
    expect(T3_CODE_ORCHESTRATION_INSTRUCTIONS).toContain(
      "When the user assigns research, design, review, judging, roasting or browser/desktop driving as a task",
    );
    expect(T3_CODE_ORCHESTRATION_INSTRUCTIONS).toContain(
      'For browser or desktop driving beyond a quick check, delegate with delegate_task using prismRole: "computer-use".',
    );
  });

  it("keeps each orchestration section under Codex's entry limit", () => {
    for (const section of T3_CODE_ORCHESTRATION_SECTIONS) {
      expect(new TextEncoder().encode(section).length).toBeLessThan(4_000);
    }
  });
});
