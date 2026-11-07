import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { BareLaunchRequest, BareReceipt, isBareReceiptVerified } from "./bareLaunch.ts";
import {
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpDelegateTaskInput,
} from "./orchestratorMcp.ts";
import { OrchestrationV2Command } from "./orchestrationV2.ts";

const decode = Schema.decodeUnknownSync(BareLaunchRequest);
const decodeDelegateTask = Schema.decodeSync(OrchestratorMcpDelegateTaskInput);
const decodeCapabilities = Schema.decodeSync(OrchestratorMcpCapabilitiesResult);
const decodeCommand = Schema.decodeSync(OrchestrationV2Command);
const decodeReceipt = Schema.decodeSync(BareReceipt);
describe("bare launch contracts", () => {
  it("keeps omitted intent ordinary and accepts only bounded, named additions", () => {
    expect(decode({ additions: [], nativeHomeOwnerThreadId: "caller-chosen-owner" })).toEqual({
      additions: [],
    });
    expect(decodeDelegateTask({ task: "test" })).not.toHaveProperty("bare");
    expect(
      decode({
        additions: [
          { type: "skillFile", path: "/work/SKILL.md" },
          { type: "mcp", server: "test", tool: "read" },
          { type: "plugin", name: "test" },
          { type: "app", name: "test" },
        ],
      }).additions,
    ).toHaveLength(4);
    expect(() => decode({ additions: [{ type: "file", path: "relative.txt" }] })).toThrow();
    expect(() =>
      decode({ additions: Array.from({ length: 17 }, () => ({ type: "skill", name: "test" })) }),
    ).toThrow();
    expect(() => decode({ additions: [{ type: "unknown" }] })).toThrow();
  });
  it("decodes an old peer without making it bare capable", () => {
    const old = decodeCapabilities({
      parentThreadId: null,
      inheritedProviderInstanceId: null,
      inheritedModel: null,
      runtimeMode: "full-access",
      interactionMode: "default",
      providers: [],
      features: {
        appOwnedSubagents: true,
        asyncPolling: true,
        cancellation: true,
        batchThreadCreation: true,
        threadManagement: true,
        incrementalThreadRead: true,
        scheduledTasks: true,
        maxBatchThreads: 20,
      },
    });
    expect(old.bareLaunch).toBeUndefined();
  });
  it("retains server resolved state in commands and never certifies a partial receipt", () => {
    const command = decodeCommand({
      type: "thread.create",
      commandId: "bare-create",
      threadId: "thread-bare",
      projectId: "project-bare",
      title: "Bare",
      modelSelection: { instanceId: "codex", model: "test" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "agent",
      creationSource: "mcp",
      bare: { additions: [], nativeHomeOwnerThreadId: "thread-bare" },
    });
    expect(command).toHaveProperty("bare", {
      additions: [],
      nativeHomeOwnerThreadId: "thread-bare",
    });
    const receipt = decodeReceipt({
      driver: "codex",
      permissions: {
        requested: null,
        admitted: "full-access",
        effective: "never / dangerFullAccess",
      },
      additions: [],
      planned: { controls: ["strict-config"], removes: ["skills"] },
      knownRetained: ["tool schemas"],
      nativeRecord: { harness: "codex", root: "/records", sessionId: "native" },
      evidence: [
        {
          category: "tools",
          status: "observed_clean",
          identities: ["exec_command"],
          source: "native",
        },
      ],
    });
    expect(isBareReceiptVerified(receipt)).toBe(false);
  });
});
