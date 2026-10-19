import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";

import { withoutPromachosClaims } from "./PromachosClientCommands.ts";

it("drops the Promachos role and answerer a client sends", () => {
  const threadId = ThreadId.make("thread");
  const create = withoutPromachosClaims({
    type: "thread.create",
    commandId: CommandId.make("create"),
    threadId,
    projectId: ProjectId.make("project"),
    title: "Thread",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
    prismRole: "promachos",
  });
  expect(create.type === "thread.create" && "prismRole" in create).toBe(false);
  const respond = withoutPromachosClaims({
    type: "runtime-request.respond",
    commandId: CommandId.make("respond"),
    threadId,
    requestId: RuntimeRequestId.make("request"),
    answers: { choice: "yes" },
    answeredBy: { kind: "promachos", threadId: ThreadId.make("promachos") },
  });
  expect(respond).toEqual({
    type: "runtime-request.respond",
    commandId: "respond",
    threadId,
    requestId: "request",
    answers: { choice: "yes" },
  });
});
