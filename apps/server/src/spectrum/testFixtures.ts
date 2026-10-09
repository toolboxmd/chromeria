import {
  CommandId,
  MessageId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { SpectrumSlot, SpectrumState } from "./state.ts";

export const NOW = DateTime.makeUnsafe("2026-10-08T12:00:00Z");
const selection = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
export function makeState(overrides: Partial<SpectrumState> = {}): SpectrumState {
  return {
    version: 1,
    threadId: ThreadId.make("spectrum.test"),
    callerThreadId: ThreadId.make("caller"),
    callerRunId: RunId.make("caller-run"),
    scheduledTaskId: null,
    schedulerRunId: null,
    question: "What should we build?",
    mode: "council",
    limit: 2,
    moderator: 1,
    participants: [
      { threadId: ThreadId.make("blue"), label: "Blue", selection },
      { threadId: ThreadId.make("red"), label: "Red", selection },
    ],
    generation: 0,
    revision: 0,
    cursor: 0,
    status: "active",
    cycle: 0,
    round: null,
    transcript: [],
    inbox: [],
    outbox: [],
    report: null,
    reportAbandonment: null,
    ...overrides,
  };
}

export function makeRun(
  slot: SpectrumSlot,
  overrides: Partial<OrchestrationV2Run> = {},
): OrchestrationV2Run {
  return {
    id: RunId.make(`run:${slot.messageId}`),
    threadId: slot.threadId,
    ordinal: 1,
    providerInstanceId: selection.instanceId,
    modelSelection: selection,
    providerThreadId: null,
    userMessageId: slot.messageId,
    rootNodeId: null,
    activeAttemptId: null,
    status: "completed",
    requestedAt: NOW,
    startedAt: NOW,
    completedAt: NOW,
    checkpointId: null,
    contextHandoffId: null,
    ...overrides,
  };
}

export function makeMessage(
  run: OrchestrationV2Run,
  text: string,
  overrides: Partial<OrchestrationV2ConversationMessage> = {},
): OrchestrationV2ConversationMessage {
  return {
    createdBy: "agent",
    creationSource: "provider",
    id: MessageId.make(`reply:${run.id}`),
    threadId: run.threadId,
    runId: run.id,
    nodeId: null,
    role: "assistant",
    text,
    attachments: [],
    streaming: false,
    createdAt: NOW,
    updatedAt: NOW,
    ...overrides,
  };
}

export function makeThread(): OrchestrationV2AppThread {
  const id = ThreadId.make("spectrum.test");
  return {
    createdBy: "system",
    creationSource: "server",
    id,
    projectId: ProjectId.make("project"),
    title: "Spectrum",
    providerInstanceId: selection.instanceId,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
    forkedFrom: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

export const APPEND = {
  type: "spectrum.transcript.append" as const,
  threadId: ThreadId.make("spectrum.test"),
  commandId: CommandId.make("append:test"),
  messageId: MessageId.make("transcript:test"),
  generation: 0,
  revision: 0,
  role: "assistant" as const,
  text: "Exact reply",
};
