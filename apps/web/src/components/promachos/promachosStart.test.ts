import {
  ClientOrchestrationCommand,
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { promachosMultipleModelSelections, withPromachosStart } from "./promachosStart";

const decodeCommand = Schema.decodeUnknownSync(ClientOrchestrationCommand);
const createdAt = "2026-10-01T20:00:00.000Z";
// The composer's own selection: a valid placeholder Prism replaces.
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.5" };
const promachosModels = [{ instanceId: "claudeAgent", model: "claude-opus-5-5" }];

function firstSend(threadId: string) {
  return {
    environmentId: EnvironmentId.make("env"),
    input: {
      threadId: ThreadId.make(threadId),
      message: {
        messageId: MessageId.make("m1"),
        role: "user" as const,
        text: "Hi",
        attachments: [],
      },
      modelSelection,
      titleSeed: "Hi",
      runtimeMode: "full-access" as const,
      interactionMode: "default" as const,
      bootstrap: {
        createThread: {
          projectId: ProjectId.make("home"),
          title: "Hi",
          modelSelection,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          branch: null,
          worktreePath: null,
          createdAt,
        },
      },
      createdAt,
    },
  };
}

function followUp(threadId: string) {
  const { bootstrap: _bootstrap, ...input } = firstSend(threadId).input;
  return { environmentId: EnvironmentId.make("env"), input };
}

/** Records what reaches the dispatch, as the wire command the server decodes. */
function recordingStart<R>(result: R) {
  const sent: ClientOrchestrationCommand[] = [];
  const start = (request: { input: object }) => {
    sent.push(
      decodeCommand({
        ...request.input,
        type: "thread.turn.start",
        commandId: CommandId.make(`c${sent.length}`),
      }),
    );
    return result;
  };
  return { sent, start };
}

describe("withPromachosStart", () => {
  it("asks Prism for a new top-level Promachos conversation with a configured list", () => {
    const { sent, start } = recordingStart("ok");
    withPromachosStart(start, promachosModels)(firstSend("new-thread"));
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      type: "thread.turn.start",
      threadId: "new-thread",
      prismRole: "promachos",
      modelSelection,
      bootstrap: { createThread: { projectId: "home", modelSelection } },
    });
  });

  it("starts normally when the Promachos list is empty or the thread is not his chat", () => {
    const { sent, start } = recordingStart("ok");
    withPromachosStart(start, [])(firstSend("empty-list"));
    withPromachosStart(start, null)(firstSend("standard-view"));
    expect(sent).toHaveLength(2);
    for (const command of sent) expect(command).not.toHaveProperty("prismRole");
  });

  it("never asks Prism for an existing conversation or a child thread", () => {
    const { sent, start } = recordingStart("ok");
    withPromachosStart(start, promachosModels)(followUp("existing"));
    withPromachosStart(start, promachosModels)(firstSend("sub.parent.child"));
    for (const command of sent) expect(command).not.toHaveProperty("prismRole");
  });

  it("returns a Prism refusal as the send's failure without a normal retry", () => {
    const refusal = { _tag: "Failure", message: "No Promachos model has capacity." };
    const { sent, start } = recordingStart(refusal);
    expect(withPromachosStart(start, promachosModels)(firstSend("refused"))).toBe(refusal);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ prismRole: "promachos" });
  });
});

describe("promachosMultipleModelSelections", () => {
  const saved = [modelSelection, { ...modelSelection, model: "gpt-5.5-mini" }];

  it("acts on no saved multiple-model selection where Prism picks the model", () => {
    expect(promachosMultipleModelSelections(saved, promachosModels)).toBeNull();
  });

  it("keeps the saved selection for an empty list and the standard view", () => {
    expect(promachosMultipleModelSelections(saved, [])).toBe(saved);
    expect(promachosMultipleModelSelections(saved, null)).toBe(saved);
  });
});
