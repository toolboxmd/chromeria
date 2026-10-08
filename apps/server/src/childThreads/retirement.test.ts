import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import {
  explicitMessage,
  humanMessage,
  retirementState,
  subagentDescendants,
  type RetirementThread,
} from "./retirement.ts";

const rootId = ThreadId.make("root");
const childId = ThreadId.make("child");
const root: RetirementThread = {
  id: rootId,
  lineage: { rootThreadId: rootId, parentThreadId: null, relationshipToParent: null },
  forkRetirement: { token: CommandId.make("stop:root") },
};
const child: RetirementThread = {
  id: childId,
  lineage: { rootThreadId: rootId, parentThreadId: rootId, relationshipToParent: "subagent" },
};

describe("retirement ancestry", () => {
  it("resumes only the named thread and recognizes later ancestor Stops", () => {
    const resumed = { ...child, forkResumedRetirements: [CommandId.make("stop:root")] };
    assert.isFalse(retirementState(resumed, new Map([[rootId, root]])).retired);
    const grandchild: RetirementThread = {
      id: ThreadId.make("grandchild"),
      lineage: { rootThreadId: rootId, parentThreadId: childId, relationshipToParent: "subagent" },
    };
    assert.isTrue(
      retirementState(
        grandchild,
        new Map([
          [rootId, root],
          [childId, resumed],
        ]),
      ).retired,
    );
    assert.isTrue(
      retirementState(
        resumed,
        new Map([
          [rootId, { ...root, forkRetirement: { token: CommandId.make("stop:root:again") } }],
        ]),
      ).retired,
    );
  });
  it("separates missing/cyclic ancestry from retirement and ignores fork edges", () => {
    assert.deepEqual(retirementState(child, new Map()), {
      tokens: [],
      complete: false,
      retired: false,
    });
    const cycle = {
      ...root,
      lineage: {
        ...root.lineage,
        parentThreadId: childId,
        relationshipToParent: "subagent" as const,
      },
    };
    assert.isFalse(
      retirementState(
        child,
        new Map([
          [rootId, cycle],
          [childId, child],
        ]),
      ).complete,
    );
    const fork = { ...child, lineage: { ...child.lineage, relationshipToParent: "fork" as const } };
    assert.deepEqual(retirementState(fork, new Map([[rootId, root]])), {
      tokens: [],
      complete: true,
      retired: false,
    });
    assert.deepEqual(
      subagentDescendants(rootId, [root, child, { ...fork, id: ThreadId.make("fork") }]),
      [childId],
    );
  });
});

const message: Extract<OrchestrationV2Command, { type: "message.dispatch" }> = {
  type: "message.dispatch",
  commandId: CommandId.make("message"),
  messageId: MessageId.make("message"),
  threadId: childId,
  text: "Resume",
  attachments: [],
  dispatchMode: { type: "start_immediately" },
  createdBy: "user",
  creationSource: "web",
};
it.each(["web", "mobile", "mcp", "server"] as const)(
  "classifies trusted %s messages without createdBy-only revival",
  (creationSource) => {
    const command = { ...message, creationSource };
    assert.equal(explicitMessage(command), creationSource !== "server");
    assert.equal(humanMessage(command), creationSource === "web" || creationSource === "mobile");
    assert.isFalse(
      explicitMessage({ ...command, usageLimitContinuationOfRunId: RunId.make("original") }),
    );
  },
);
