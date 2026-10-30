import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2ProviderTurnTokenUsage,
  OrchestrationV2SubagentWorkflow,
  OrchestrationV2UserInputQuestion,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SchemaAST from "effect/SchemaAST";
import { retainNewerFields } from "./rollbackCompatibility.ts";

const at = "2026-10-30T00:00:00Z";
const watch = {
  startedAt: at,
  headSha: "head:one",
  failedChecks: [],
  passed: false,
  passedChecks: [],
  remarksThrough: at,
  remarkIds: [],
  conflicting: false,
  wakes: 0,
};
const link = {
  host: "github.com",
  repository: "fixture/project",
  number: 1,
  url: "https://fixture.invalid/pull/1",
  source: "manual",
  linkedAt: at,
  snapshot: null,
  stack: null,
};
const threadAst = SchemaAST.toEncoded(OrchestrationV2AppThreadJson.ast);
const retain = (schema: Schema.Top, stored: unknown, next: unknown) =>
  retainNewerFields(SchemaAST.toEncoded(schema.ast), stored, next);

it("rollback PR retention follows link/watch identity, head reset, end and removal", () => {
  const original = {
    id: "thread:one",
    updatedAt: at,
    pullRequests: [
      {
        ...link,
        repositoryWebUrl: "https://fixture.invalid",
        watch: { ...watch, owedChecks: ["advisory"], checksAwaitedSince: at },
      },
    ],
  };
  const retained = retainNewerFields(
    threadAst,
    original,
    { ...original, pullRequests: [{ ...link, watch }] },
    [],
    at,
  );
  assert.deepEqual(retained, original);
  const ended = retainNewerFields(
    threadAst,
    original,
    { id: "thread:one", updatedAt: at, pullRequests: [link] },
    [],
    at,
  );
  assert.deepEqual(ended, {
    ...original,
    pullRequests: [
      {
        ...link,
        repositoryWebUrl: "https://fixture.invalid",
        endedWatch: original.pullRequests[0]!.watch,
      },
    ],
  });
  const restartedAt = "2026-10-30T00:01:00Z";
  const freshWatch = {
    ...watch,
    startedAt: restartedAt,
    remarksThrough: restartedAt,
    headSha: null,
  };
  const resumedWatch = {
    ...original.pullRequests[0]!.watch,
    startedAt: restartedAt,
    remarksThrough: restartedAt,
  };
  const resumed = retainNewerFields(
    threadAst,
    ended,
    { id: "thread:one", updatedAt: restartedAt, pullRequests: [{ ...link, watch: freshWatch }] },
    [],
    restartedAt,
  );
  assert.deepEqual(resumed, {
    id: "thread:one",
    updatedAt: restartedAt,
    pullRequests: [{ ...link, repositoryWebUrl: "https://fixture.invalid", watch: resumedWatch }],
  });
  assert.deepEqual(
    retainNewerFields(
      threadAst,
      resumed,
      { id: "thread:one", updatedAt: restartedAt, pullRequests: [link] },
      [],
      restartedAt,
    ),
    {
      id: "thread:one",
      updatedAt: restartedAt,
      pullRequests: [
        { ...link, repositoryWebUrl: "https://fixture.invalid", endedWatch: resumedWatch },
      ],
    },
  );
  const moved = retainNewerFields(
    threadAst,
    original,
    { ...original, pullRequests: [{ ...link, watch: { ...watch, headSha: "head:two" } }] },
    [],
    "2026-10-30T00:02:00Z",
  );
  assert.deepEqual(moved, {
    ...original,
    pullRequests: [
      {
        ...link,
        repositoryWebUrl: "https://fixture.invalid",
        watch: {
          ...watch,
          headSha: "head:two",
          owedChecks: [],
          checksAwaitedSince: "2026-10-30T00:02:00Z",
        },
      },
    ],
  });
  const changed = { ...link, number: 2, watch };
  assert.deepEqual(retainNewerFields(threadAst, original, { pullRequests: [changed] }), {
    pullRequests: [changed],
  });
  assert.deepEqual(retainNewerFields(threadAst, original, { pullRequests: [] }), {
    pullRequests: [],
  });
});

it("rollback nested retention matches array identities and respects known field clears", () => {
  const schema = OrchestrationV2UserInputQuestion;
  const question = {
    id: "question:1",
    header: "Choice",
    question: "Pick",
    options: [
      { label: "One", description: "First", preview: "Preserve one", value: "old" },
      { label: "Two", description: "Second", preview: "Preserve two" },
    ],
  };
  assert.deepEqual(
    retain(schema, question, {
      ...question,
      options: [
        { label: "Two", description: "Updated second" },
        { label: "New", description: "New option" },
      ],
    }),
    {
      ...question,
      options: [
        { label: "Two", description: "Updated second", preview: "Preserve two" },
        { label: "New", description: "New option" },
      ],
    },
  );
  const workflow = {
    phases: [],
    runHandles: { runId: "workflow:one" },
    agents: [
      {
        index: 0,
        label: "A",
        attempt: 1,
        state: "running",
        effort: "high",
        speed: "fast",
        completedAt: 123,
      },
    ],
  };
  assert.deepEqual(
    retain(OrchestrationV2SubagentWorkflow, workflow, {
      ...workflow,
      agents: [{ index: 0, label: "Renamed", attempt: 1, state: "completed" }],
    }),
    { ...workflow, agents: [{ ...workflow.agents[0], label: "Renamed", state: "completed" }] },
  );
  const retry = { ...workflow, agents: [{ index: 0, label: "A", attempt: 2, state: "running" }] };
  assert.deepEqual(retain(OrchestrationV2SubagentWorkflow, workflow, retry), retry);
  const nextWorkflow = { ...retry, runHandles: { runId: "workflow:two" } };
  assert.deepEqual(retain(OrchestrationV2SubagentWorkflow, workflow, nextWorkflow), nextWorkflow);
});

it("rollback retains provider metadata, task awaited flags and usage facts without positional matching", () => {
  const before = {
    id: "provider:one",
    nativeMetadata: {
      title: "Old",
      forkHeadMessageId: "message:head",
      claudeAttachmentBudget: { bytesUsed: 20, pagesUsed: 2 },
    },
    pendingBackgroundTasks: [
      { kind: "command", taskId: "task:one", awaited: true },
      { kind: "monitor", taskId: "task:two", awaited: true },
    ],
  };
  const next = {
    id: "provider:one",
    nativeMetadata: {},
    pendingBackgroundTasks: [
      { kind: "monitor", taskId: "task:two" },
      { kind: "command", taskId: "task:new" },
    ],
  };
  assert.deepEqual(retain(OrchestrationV2ProviderThreadJson, before, next), {
    ...next,
    nativeMetadata: {
      forkHeadMessageId: "message:head",
      claudeAttachmentBudget: { bytesUsed: 20, pagesUsed: 2 },
    },
    pendingBackgroundTasks: [
      { kind: "monitor", taskId: "task:two", awaited: true },
      { kind: "command", taskId: "task:new" },
    ],
  });
  const usage = {
    usedTokens: 1,
    updatedAt: at,
    autoCompactThreshold: 180000,
    promptCacheExpiresAt: "2026-10-30T00:05:00Z",
  };
  assert.deepEqual(
    retain(OrchestrationV2ProviderTurnTokenUsage, usage, { usedTokens: 2, updatedAt: at }),
    { ...usage, usedTokens: 2 },
  );
});

it("rollback settings retention uses record and device identities", () => {
  const stored = {
    projectSettingsOverrides: { first: { defaultAutoPull: true, threadTitleInstructions: "Keep" } },
    deviceHosts: [
      { id: "one", label: "Old", target: "fixture.invalid", kind: "ssh" },
      { id: "two", label: "Removed", target: "fixture.invalid", kind: "ssh" },
    ],
    guestSharingEnabled: false,
  };
  const next = {
    projectSettingsOverrides: { first: { defaultAutoPull: false } },
    deviceHosts: [{ id: "one", label: "Edited", target: "fixture.invalid" }],
  };
  assert.deepEqual(retain(ServerSettings, stored, next), {
    projectSettingsOverrides: {
      first: { defaultAutoPull: false, threadTitleInstructions: "Keep" },
    },
    deviceHosts: [{ id: "one", label: "Edited", target: "fixture.invalid", kind: "ssh" }],
    guestSharingEnabled: false,
  });
});

it("rollback refuses ambiguous nested identities instead of stripping newer fields", () => {
  const question = {
    id: "question:ambiguous",
    header: "Choice",
    question: "Pick",
    options: [
      { label: "Same", description: "One", preview: "First" },
      { label: "Same", description: "Two", preview: "Second" },
    ],
  };
  assert.throws(
    () =>
      retain(OrchestrationV2UserInputQuestion, question, {
        ...question,
        options: [{ label: "Same", description: "Changed" }],
      }),
    /Ambiguous persisted array identity/,
  );
});
