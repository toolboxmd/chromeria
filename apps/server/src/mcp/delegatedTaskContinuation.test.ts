// Fork: agents continue a stopped delegated job in its own thread (toolboxmd/chromeria#203).
import { assert, describe, it } from "@effect/vitest";

import { T3_CODE_ORCHESTRATION_INSTRUCTIONS } from "@t3tools/provider-core/server/orchestrationInstructions";
import { T3_CODE_TOOL_USE_INSTRUCTIONS } from "@t3tools/shared/forkToolInstructions";
import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";

describe("delegated task continuation guidance", () => {
  it("tells agents to continue the same job in its child thread", () => {
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "To continue the same delegated job");
    assert.include(T3_CODE_ORCHESTRATION_INSTRUCTIONS, "the finished follow-up wakes you");
    assert.include(T3_CODE_TOOL_USE_INSTRUCTIONS, "To continue the same job after a stop");
    assert.include(
      OrchestratorToolkit.tools.t3_thread_send.description ?? "",
      "To continue the same delegated job after it stopped",
    );
  });
});
