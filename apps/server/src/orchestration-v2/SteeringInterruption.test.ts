import { assert, it } from "@effect/vitest";
import { ProviderDriverKind, ThreadId } from "@t3tools/contracts";
import { planClaudeSkillDispatch } from "../provider/Drivers/ClaudeSkillDispatch.ts";
import {
  isPersonSteering,
  steeringRestartNote,
  type SteeringRequester,
} from "./SteeringInterruption.ts";

const person = { createdBy: "user", creationSource: "web" } as const;
const agent = {
  createdBy: "agent",
  creationSource: "mcp",
  senderThreadId: ThreadId.make("sender-thread"),
} as const;

it.each([
  [person, true],
  [{ createdBy: "user", creationSource: "mobile" }, true],
  [{ createdBy: "user", creationSource: "server" }, false],
  [{ createdBy: "system", creationSource: "server" }, false],
  [agent, false],
] satisfies ReadonlyArray<readonly [SteeringRequester, boolean]>)(
  "classifies requester %j",
  (requester, expected) => {
    assert.equal(isPersonSteering(requester), expected);
  },
);

it("explains an agent interruption without attributing it to the user", () => {
  const note = steeringRestartNote({
    requester: agent,
    author: agent,
    driver: ProviderDriverKind.make("codex"),
  });
  assert.include(note, "agent in T3 thread sender-thread interrupted");
  assert.include(note, "interrupted, not refused");
  assert.include(note, "actions that finished keep their effects");
  assert.notInclude(note, "the user interrupted");
});

it("distinguishes a promotion requester from the queued message's author", () => {
  assert.include(
    steeringRestartNote({
      requester: person,
      author: agent,
      driver: ProviderDriverKind.make("claudeAgent"),
    }),
    "the user interrupted",
  );
  assert.include(
    steeringRestartNote({
      requester: person,
      author: agent,
      driver: ProviderDriverKind.make("claudeAgent"),
    }),
    "which the agent in T3 thread sender-thread wrote",
  );
  assert.include(
    steeringRestartNote({
      requester: agent,
      author: person,
      driver: ProviderDriverKind.make("grok"),
    }),
    "which the user wrote",
  );
  assert.include(
    steeringRestartNote({
      requester: { createdBy: "agent", creationSource: "mcp" },
      author: person,
      driver: ProviderDriverKind.make("grok"),
    }),
    "another agent interrupted",
  );
});

it("corrects Claude's refusal wording for a human cut-in too", () => {
  assert.include(
    steeringRestartNote({
      requester: person,
      author: person,
      driver: ProviderDriverKind.make("claudeAgent"),
    }),
    "interrupted, not refused",
  );
  assert.equal(
    steeringRestartNote({
      requester: person,
      author: person,
      driver: ProviderDriverKind.make("codex"),
    }),
    "",
  );
});

it("preserves the interruption explanation before a native Claude skill block", () => {
  const note = steeringRestartNote({
    requester: agent,
    author: agent,
    driver: ProviderDriverKind.make("claudeAgent"),
  });
  const dispatch = planClaudeSkillDispatch(
    `${note}\n\n$review changed recipients`,
    new Map([["review", { path: "/home/me/.claude/skills/review/SKILL.md" }]]),
  );
  assert.equal(dispatch?.leadingText, note);
  assert.equal(dispatch?.commandText, "/review changed recipients");
});
