import { PRISM_DELEGATE_ROLES } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { OrchestratorToolkit } from "./tools.ts";

// Fork (toolboxmd/chromeria#523, #565, #571): tool copy upstream trims must keep these rules.
describe("fork orchestrator tool guidance", () => {
  it("delegate_task names every Prism role and the computer-use hand-off", () => {
    const description = OrchestratorToolkit.tools.delegate_task.description ?? "";
    for (const role of PRISM_DELEGATE_ROLES) expect(description).toContain(role);
    expect(description).toContain('prismRole: "computer-use"');
    expect(description).toContain("beyond a quick check");
  });

  it("schedule_task explains the 55-minute thread binding", () => {
    const description = OrchestratorToolkit.tools.schedule_task.description ?? "";
    expect(description).toContain("55 minutes");
    expect(description).toContain("bindToCurrentThread=true");
    expect(description).toContain("bindToCurrentThread=false");
  });
});
