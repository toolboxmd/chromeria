import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { CursorProviderCapabilitiesV2 } from "@t3tools/provider-cursor/testing";
import { GrokProviderCapabilitiesV2 } from "@t3tools/provider-grok/testing";
import * as CommandPolicy from "./CommandPolicy.ts";

const commandId = CommandId.make("command-policy-test");
const threadId = ThreadId.make("command-policy-thread");
const activeRunId = RunId.make("command-policy-active-run");
const activeAttemptId = RunAttemptId.make("command-policy-active-attempt");
const userMessageId = MessageId.make("command-policy-user-message");

const baseCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;

function capabilities(
  override: (current: OrchestrationV2ProviderCapabilities) => OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities {
  return override(baseCapabilities);
}

function dispatchProjection(
  sessionCapabilities?: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ThreadProjection {
  const providerThreadId = ProviderThreadId.make("command-policy-provider-thread");
  const providerSessionId = ProviderSessionId.make("command-policy-provider-session");
  return {
    runs:
      sessionCapabilities === undefined
        ? []
        : [
            {
              id: activeRunId,
              status: "running",
              providerThreadId,
              activeAttemptId,
              userMessageId,
            },
          ],
    messages: [{ id: userMessageId, text: "first", attachments: [] }],
    providerThreads:
      sessionCapabilities === undefined ? [] : [{ id: providerThreadId, providerSessionId }],
    providerSessions:
      sessionCapabilities === undefined
        ? []
        : [{ id: providerSessionId, capabilities: sessionCapabilities }],
    providerTurns:
      sessionCapabilities === undefined
        ? []
        : [{ runAttemptId: activeAttemptId, status: "running" }],
  } as unknown as OrchestrationV2ThreadProjection;
}

it.each([
  ["Claude", ClaudeProviderCapabilitiesV2],
  ["Codex", CodexProviderCapabilitiesV2],
  ["Grok", GrokProviderCapabilitiesV2],
  ["Cursor", CursorProviderCapabilitiesV2],
] as const)(
  "human Steer interrupts a running %s turn, including existing mobile clients",
  (_provider, caps) => {
    for (const [source, intent] of [
      ["web", "steer"],
      ["mobile", "steer"],
      ["mobile", "auto"],
    ] as const) {
      assert.deepEqual(
        CommandPolicy.resolveMessageDispatchIntent(
          dispatchProjection(caps),
          { type: "steer_active", targetRunId: activeRunId },
          intent,
          { createdBy: "user", creationSource: source },
        ),
        { type: "restart_active", targetRunId: activeRunId },
      );
    }
  },
);

it.each(["preparing", "starting", "waiting"] as const)(
  "retains human Claude Steer while the run is %s",
  (status) => {
    const projection = dispatchProjection(ClaudeProviderCapabilitiesV2);
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        { ...projection, runs: projection.runs.map((run) => ({ ...run, status })) },
        { type: "start_immediately" },
        "steer",
        { createdBy: "user", creationSource: "web" },
      ),
      { type: "queue_after_active" },
    );
  },
);

it("keeps routine agent and server updates queued on Claude", () => {
  for (const requester of [
    { createdBy: "agent", creationSource: "mcp" },
    { createdBy: "user", creationSource: "server" },
    { createdBy: "system", creationSource: "server" },
  ] as const) {
    for (const intent of ["auto", "steer"] as const) {
      assert.deepEqual(
        CommandPolicy.resolveMessageDispatchIntent(
          dispatchProjection(ClaudeProviderCapabilitiesV2),
          { type: "start_immediately" },
          intent,
          requester,
        ),
        { type: "queue_after_active" },
      );
    }
  }
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(ClaudeProviderCapabilitiesV2),
      { type: "start_immediately" },
      "auto",
      { createdBy: "user", creationSource: "web" },
    ),
    { type: "queue_after_active" },
  );
});

it.effect("rejects unsafe native Claude steering even when a caller bypasses queue admission", () =>
  Effect.gen(function* () {
    const policy = yield* CommandPolicy.CommandPolicyV2;
    const error = yield* policy
      .decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        capabilities: ClaudeProviderCapabilitiesV2,
      })
      .pipe(Effect.flip);
    assert.equal(error._tag, "CommandPolicyCapabilityUnsupportedError");
  }).pipe(Effect.provide(CommandPolicy.layer)),
);

it("resolves automatic message delivery from authoritative provider capabilities", () => {
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "start_immediately" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(baseCapabilities),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: false,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
      { createdBy: "system", creationSource: "server" },
    ),
    // A routine wake may use a soft restart, but cannot become an explicit Stop.
    { type: "steer_active", targetRunId: activeRunId },
  );
});

it.each(["preparing", "starting"] as const)(
  "queues an automatic message while the handoff run is %s",
  (status) => {
    const projection = dispatchProjection(baseCapabilities);
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        { ...projection, runs: projection.runs.map((run) => ({ ...run, status })) },
        { type: "start_immediately" },
        "auto",
      ),
      { type: "queue_after_active" },
    );
  },
);

it("queues auto and steer when native steering cancels running tools", () => {
  const claude = dispatchProjection(ClaudeProviderCapabilitiesV2);
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(claude, { type: "start_immediately" }, "auto"),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(claude, { type: "start_immediately" }, "steer"),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(claude, {
      type: "steer_active",
      targetRunId: activeRunId,
    }),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(dispatchProjection(baseCapabilities), {
      type: "steer_active",
      targetRunId: activeRunId,
    }),
    { type: "steer_active", targetRunId: activeRunId },
  );
});

it.each([
  ["has not started", []],
  ["is pending", [{ runAttemptId: activeAttemptId, status: "pending" }]],
  ["has completed", [{ runAttemptId: activeAttemptId, status: "completed" }]],
  [
    "belongs to a previous attempt",
    [{ runAttemptId: RunAttemptId.make("previous-attempt"), status: "running" }],
  ],
])("queues only automatic delivery when the active run's provider turn %s", (_, providerTurns) => {
  const projection = {
    ...dispatchProjection(baseCapabilities),
    providerTurns,
  } as unknown as OrchestrationV2ThreadProjection;
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "auto"),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "steer"),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it.each(["/compact", " /LOGOUT "])("queues a steer sent while %s runs", (text) => {
  const projection = dispatchProjection(baseCapabilities);
  const maintenance = {
    ...projection,
    messages: [{ id: userMessageId, text, attachments: [] }],
  } as unknown as OrchestrationV2ThreadProjection;
  for (const deliveryIntent of ["auto", "steer"] as const) {
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        maintenance,
        { type: "start_immediately" },
        deliveryIntent,
      ),
      { type: "queue_after_active" },
    );
  }
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(maintenance, {
      type: "steer_active",
      targetRunId: activeRunId,
    }),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      maintenance,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

const layer = it.layer(CommandPolicy.layer);

layer("CommandPolicyV2", (it) => {
  it.effect.each([
    { provider: "claudeAgent", capabilities: ClaudeProviderCapabilitiesV2 },
    { provider: "codex", capabilities: CodexProviderCapabilitiesV2 },
    { provider: "grok", capabilities: GrokProviderCapabilitiesV2 },
  ])("allows an explicit urgent restart for $provider", ({ provider, capabilities }) =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;
      assert.equal(
        yield* policy.decideSteeringExecution({
          commandId,
          threadId,
          providerInstanceId: ProviderInstanceId.make(provider),
          capabilities,
          forceRestart: true,
        }),
        "interrupt_restart",
      );
    }),
  );

  it.effect("falls back to portable context when Cursor cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
      });

      assert.equal(result, "portable_context");
    }),
  );
});
