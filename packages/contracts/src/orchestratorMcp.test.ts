import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  OrchestratorMcpDelegateTaskInput,
  OrchestratorMcpDelegateTaskResult,
  OrchestratorMcpRequestSecretInput,
  OrchestratorMcpThreadRun,
} from "./orchestratorMcp.ts";

const decodeDelegateTaskInput = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskInput);
const decodeDelegateTaskResult = Schema.decodeUnknownSync(OrchestratorMcpDelegateTaskResult);
const decodeThreadRun = Schema.decodeUnknownSync(OrchestratorMcpThreadRun);

describe("orchestrator MCP contracts", () => {
  it("reads runs from older linked environments without inventing recovery facts", () => {
    const olderRun = {
      runId: "run:older-peer",
      ordinal: 1,
      status: "failed",
      providerInstanceId: "codex",
      model: "fixed",
      requestedAt: "2026-10-08T10:00:00Z",
      startedAt: null,
      completedAt: "2026-10-08T10:01:00Z",
    };
    expect(decodeThreadRun(olderRun)).toEqual(olderRun);
    expect(
      decodeThreadRun({ ...olderRun, recovery: { state: "given_up", reason: "exhausted" } })
        .recovery,
    ).toEqual({ state: "given_up", reason: "exhausted" });
  });

  it("accepts 1-5 uniquely named secret fields that can each be a file name", () => {
    const decode = Schema.decodeUnknownOption(OrchestratorMcpRequestSecretInput);
    const ask = (fields: ReadonlyArray<{ name: string; label: string }>) =>
      decode({ label: "Credentials", reason: "For the deploy job.", fields });
    const field = (name: string) => ({ name, label: name });
    expect(ask([field("client_id"), field("client-secret")])._tag).toBe("Some");
    expect(ask([field("constructor")])._tag).toBe("Some");
    for (const fields of [
      [],
      [field("a"), field("a")],
      [1, 2, 3, 4, 5, 6].map((index) => field(`f${index}`)),
      [field("../x")],
      [field("Token")],
      [field("__proto__")],
    ]) {
      expect(ask(fields)._tag).toBe("None");
    }
  });

  it("decodes cross-provider delegated task requests and durable results", () => {
    const request = decodeDelegateTaskInput({
      task: "Inspect the workspace and report the result.",
      target: {
        providerInstanceId: "claudeAgent",
        model: "claude-sonnet-4-6",
      },
      mode: "wait",
      timeoutMs: 5_000,
      clientRequestId: "delegate-1",
      runtimeMode: "inherit",
      interactionMode: "inherit",
    });
    const result = decodeDelegateTaskResult({
      taskId: "node-task-1",
      childThreadId: "thread-child-1",
      childRunId: "run-child-1",
      childNodeId: "node-task-1",
      status: "completed",
      workState: "result_available",
      hasPendingChildRuns: false,
      providerInstanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
      summary: "Workspace inspected.",
      resultContextTransferId: "context-transfer-result-1",
      latestTerminalRunId: "run-child-1",
      latestTerminalStatus: "completed",
      latestTerminalSummary: "Workspace inspected.",
      latestTerminalResultContextTransferId: "context-transfer-result-1",
      hasPendingQuestion: false,
      pendingApproval: null,
      waitTimedOut: false,
    });

    expect(request.target?.providerInstanceId).toBe("claudeAgent");
    expect(result.status).toBe("completed");
    expect(result.summary).toBe("Workspace inspected.");
    expect(result.pendingApproval).toBeNull();
    const pendingApproval = {
      requestId: "request-approval-1",
      kind: "command",
      runId: "run-child-1",
    };
    expect(
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, input: { command: "private command" } },
      }).pendingApproval,
    ).toEqual(pendingApproval);
    expect(() =>
      decodeDelegateTaskResult({
        ...result,
        pendingApproval: { ...pendingApproval, kind: "user_input" },
      }),
    ).toThrow();
  });
});
