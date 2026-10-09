// Fork: agents continue a stopped delegated job in its own thread (toolboxmd/chromeria#203).
import { assert, describe, it } from "@effect/vitest";

import { T3_CODE_ORCHESTRATION_INSTRUCTIONS } from "../provider/T3OrchestrationInstructions.ts";
import { T3_CODE_TOOL_USE_INSTRUCTIONS } from "./toolInstructions.ts";
import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";

describe("delegated task continuation guidance", () => {
  it("tells agents to continue the same job in its child thread", () => {
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "To continue the same delegated job");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "does not wake you");
    assert.include(T3_CODE_TOOL_USE_INSTRUCTIONS, "To continue the same job after a stop");
    assert.include(
      OrchestratorToolkit.tools.t3_thread_send.description ?? "",
      "To continue the same delegated job after it stopped",
    );
  });
});
