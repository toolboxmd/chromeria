import { NodeId, type OrchestrationV2Subagent } from "@t3tools/contracts";
import { expect, it } from "vite-plus/test";

import { runGroupWakeDetail } from "./RunGroups.ts";

it("renders a member's follow-up result instead of its first one", () => {
  const groupId = NodeId.make("node:group");
  const member = {
    id: NodeId.make("node:member"),
    origin: "app_owned",
    runGroupId: groupId,
    model: "model",
    status: "completed",
    result: "First answer.",
    followUpResult: "Follow-up answer.",
  } as OrchestrationV2Subagent;
  const detail = runGroupWakeDetail({ subagents: [member] }, groupId);
  expect(detail).toContain("Follow-up answer.");
  expect(detail).not.toContain("First answer.");
});
