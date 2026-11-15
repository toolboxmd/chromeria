import { PRISM_DELEGATE_ROLES } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { taskPrompt } from "../../delegatedTaskModes.ts";
import { OrchestratorToolkit } from "./tools.ts";

// Fork: tool copy upstream trims must keep these rules.
describe("fork orchestrator tool guidance", () => {
  it("delegate_task names every Prism role and the computer-use hand-off", () => {
    const description = OrchestratorToolkit.tools.delegate_task.description ?? "";
    for (const role of PRISM_DELEGATE_ROLES) expect(description).toContain(role);
    expect(description).toContain('prismRole: "computer-use"');
    expect(description).toContain(
      "opening a page and taking one snapshot, read or screenshot of it stays inline",
    );
  });

  it("tells a computer-use child it is the driver, so it does not delegate again", () => {
    expect(taskPrompt({ task: "Open /pricing.", prismRole: "computer-use", role: "test" })).toBe(
      "Act as the computer-use sub-agent for this task.\n\nOpen /pricing.",
    );
    expect(taskPrompt({ task: "Open /pricing.", prismRole: "reviewer" })).toBe("Open /pricing.");
  });

  it("schedule_task explains the 55-minute thread binding", () => {
    const description = OrchestratorToolkit.tools.schedule_task.description ?? "";
    expect(description).toContain("55 minutes");
    expect(description).toContain("bindToCurrentThread=true");
    expect(description).toContain("bindToCurrentThread=false");
  });
});
