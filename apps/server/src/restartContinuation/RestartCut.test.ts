import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2RunJson,
  OrchestratorMcpThreadRun,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import { recoveryRun } from "../prism/recovery.testkit.ts";
import { mergeRestartCut, RESTART_CUT_REASONS, restartCutView } from "@t3tools/shared/restartCut";

const at = DateTime.makeUnsafe("2026-10-14T10:00:00Z");
const later = DateTime.makeUnsafe("2026-10-14T10:01:00Z");
const source: OrchestrationV2Run = {
  ...recoveryRun,
  status: "cancelled",
  completedAt: at,
  restartCut: { continuation: "scheduled", cutAt: at },
};
const replacement: OrchestrationV2Run = {
  ...source,
  id: RunId.make("run:replacement"),
  ordinal: 2,
  restartContinuationOfRunId: source.id,
  restartCut: undefined,
  status: "starting",
  startedAt: null,
  completedAt: null,
};

it("round trips both nested cut dates and preserves absent metadata", () => {
  const encode = Schema.encodeSync(OrchestrationV2RunJson);
  const decode = Schema.decodeUnknownSync(OrchestrationV2RunJson);
  for (const restartCut of [
    undefined,
    { continuation: "scheduled" as const, cutAt: at },
    { continuation: "declined" as const, cutAt: at, reason: RESTART_CUT_REASONS.CONTINUATION_OFF },
  ]) {
    const run = { ...source, restartCut };
    const encoded = encode(run);
    assert.strictEqual(
      encoded.restartCut?.cutAt,
      restartCut === undefined ? undefined : DateTime.formatIso(at),
    );
    assert.deepEqual(decode(encoded), run);
  }
  const encoded = encode(source);
  assert.throws(() =>
    decode({ ...encoded, restartCut: { continuation: "scheduled", cutAt: "bad date" } }),
  );
  for (const reason of ["", " ", "x".repeat(513)]) {
    assert.throws(() =>
      decode({
        ...encoded,
        restartCut: { continuation: "declined", cutAt: DateTime.formatIso(at), reason },
      }),
    );
  }
});

it("keeps the first cut time and first final decline", () => {
  const first = { continuation: "scheduled" as const, cutAt: at };
  const declined = {
    continuation: "declined" as const,
    cutAt: later,
    reason: RESTART_CUT_REASONS.THREAD_INACTIVE,
  };
  assert.strictEqual(mergeRestartCut(undefined, first), first);
  assert.strictEqual(mergeRestartCut(first, undefined), first);
  assert.strictEqual(mergeRestartCut(first, { ...first, cutAt: later }), first);
  const terminal = mergeRestartCut(first, declined);
  assert.deepEqual(terminal, { ...declined, cutAt: at });
  assert.strictEqual(mergeRestartCut(terminal, first), terminal);
  assert.strictEqual(
    mergeRestartCut(terminal, { ...declined, reason: RESTART_CUT_REASONS.SUPERSEDED }),
    terminal,
  );
});

it("distinguishes unread, active, terminal, and missing intent", () => {
  assert.deepEqual(restartCutView(source, [source]), { state: "continuation_pending" });
  assert.deepEqual(restartCutView(source, [source], new Map()), { state: "continuation_pending" });
  for (const status of ["pending", "running", "succeeded", "failed", "cancelled", null] as const) {
    assert.deepEqual(
      restartCutView(source, [source], new Map([[source.id, status]])),
      status === "pending" || status === "running"
        ? { state: "continuation_pending" }
        : {
            state: "declined",
            reason:
              status === "failed"
                ? RESTART_CUT_REASONS.EFFECT_FAILED
                : RESTART_CUT_REASONS.NO_CONTINUATION,
          },
    );
  }
  const legacy = { ...source, restartCut: undefined };
  assert.isUndefined(restartCutView(legacy, [legacy]));
  assert.isUndefined(restartCutView(legacy, [legacy], new Map([[legacy.id, null]])));
  assert.deepEqual(restartCutView(legacy, [legacy], new Map([[legacy.id, "pending"]])), {
    state: "continuation_pending",
  });
  assert.deepEqual(restartCutView(legacy, [legacy], new Map([[legacy.id, "succeeded"]])), {
    state: "declined",
    reason: RESTART_CUT_REASONS.NO_CONTINUATION,
  });
});

it("never presents a natural or rolled-back source as a decline", () => {
  for (const status of [
    "completed",
    "failed",
    "interrupted",
    "rolled_back",
    "running",
    "waiting",
  ] as const) {
    assert.isUndefined(
      restartCutView({ ...source, status }, [replacement], new Map([[source.id, "failed"]])),
    );
  }
});

it("requires a start milestone and keeps that historical success after later terminal states", () => {
  for (const status of ["preparing", "queued", "starting", "running", "waiting"] as const) {
    assert.deepEqual(restartCutView(source, [{ ...replacement, status }]), {
      state: "continuation_pending",
    });
  }
  for (const status of [
    "completed",
    "failed",
    "interrupted",
    "cancelled",
    "rolled_back",
  ] as const) {
    const unstarted = { ...replacement, status, completedAt: later };
    assert.deepEqual(restartCutView(source, [unstarted]), {
      state: "declined",
      reason:
        status === "failed"
          ? RESTART_CUT_REASONS.EFFECT_FAILED
          : RESTART_CUT_REASONS.NO_CONTINUATION,
      continuationRunId: replacement.id,
      continuationStatus: status,
    });
    assert.deepEqual(restartCutView(source, [{ ...unstarted, startedAt: later }]), {
      state: "continued",
      continuationRunId: replacement.id,
      continuationStatus: status,
    });
  }
  assert.deepEqual(
    restartCutView(
      {
        ...source,
        restartCut: {
          continuation: "declined",
          cutAt: at,
          reason: RESTART_CUT_REASONS.EFFECT_FAILED,
        },
      },
      [{ ...replacement, startedAt: later }],
    ),
    {
      state: "continued",
      continuationRunId: replacement.id,
      continuationStatus: "starting",
    },
  );
});

it("follows A to an unstarted cut B to C without false failure or success", () => {
  const cutB: OrchestrationV2Run = {
    ...replacement,
    status: "cancelled",
    completedAt: later,
    restartCut: { continuation: "scheduled", cutAt: later },
  };
  const c: OrchestrationV2Run = {
    ...replacement,
    id: RunId.make("run:third"),
    ordinal: 3,
    restartContinuationOfRunId: cutB.id,
  };
  const statuses = new Map([
    [source.id, "succeeded"],
    [cutB.id, "succeeded"],
  ] as const);
  assert.deepEqual(restartCutView(source, [cutB, c], statuses), { state: "continuation_pending" });
  assert.deepEqual(restartCutView(source, [{ ...cutB, restartCut: undefined }, c]), {
    state: "continuation_pending",
  });
  assert.deepEqual(restartCutView(source, [cutB], new Map([[cutB.id, null]])), {
    state: "declined",
    reason: RESTART_CUT_REASONS.NO_CONTINUATION,
    continuationRunId: cutB.id,
    continuationStatus: "cancelled",
  });
  assert.deepEqual(
    restartCutView(source, [cutB, { ...c, startedAt: later, status: "cancelled" }], statuses),
    {
      state: "continued",
      continuationRunId: c.id,
      continuationStatus: "cancelled",
    },
  );
  const declinedC = {
    ...c,
    status: "cancelled" as const,
    completedAt: later,
    restartCut: {
      continuation: "declined" as const,
      cutAt: later,
      reason: RESTART_CUT_REASONS.STOP_REQUESTED,
    },
  };
  assert.deepEqual(restartCutView(source, [cutB, declinedC], statuses), {
    state: "declined",
    reason: RESTART_CUT_REASONS.STOP_REQUESTED,
    continuationRunId: c.id,
    continuationStatus: "cancelled",
  });
});

it("protects against cycles, ambiguous links, and foreign replacements", () => {
  const unstartedA = { ...source, startedAt: null, restartContinuationOfRunId: replacement.id };
  const cutB = {
    ...replacement,
    status: "cancelled" as const,
    completedAt: later,
    restartCut: source.restartCut,
  };
  assert.strictEqual(restartCutView(unstartedA, [unstartedA, cutB])?.state, "declined");
  assert.deepEqual(restartCutView(unstartedA, [unstartedA, cutB]), {
    state: "declined",
    reason: RESTART_CUT_REASONS.INVALID_CHAIN,
    continuationRunId: cutB.id,
    continuationStatus: "cancelled",
  });
  assert.deepEqual(
    restartCutView(source, [replacement, { ...replacement, id: RunId.make("run:duplicate") }]),
    { state: "declined", reason: RESTART_CUT_REASONS.INVALID_CHAIN },
  );
  assert.deepEqual(
    restartCutView(source, [
      { ...replacement, threadId: ThreadId.make("thread:foreign"), startedAt: later },
    ]),
    { state: "continuation_pending" },
  );
});

it("exports only bounded fixed reasons and does not echo unrecognized persisted detail", () => {
  assert.strictEqual(
    RESTART_CUT_REASONS.BACKGROUND_ONLY,
    "The run had no unfinished root turn or awaited work to continue.",
  );
  for (const reason of Object.values(RESTART_CUT_REASONS))
    assert.isTrue(reason.length > 0 && reason.length <= 512);
  const unsafe = {
    ...source,
    restartCut: {
      continuation: "declined" as const,
      cutAt: at,
      reason: "arbitrary provider defect text",
    },
  };
  assert.deepEqual(restartCutView(unsafe, []), {
    state: "declined",
    reason: RESTART_CUT_REASONS.NO_CONTINUATION,
  });
});

it("encodes every derived outcome on the additive MCP run schema", () => {
  const decode = Schema.decodeUnknownSync(OrchestratorMcpThreadRun);
  const base = {
    runId: source.id,
    recovery: { state: "none" as const },
    ordinal: 1,
    status: source.status,
    providerInstanceId: source.providerInstanceId,
    model: "fixed",
    requestedAt: DateTime.formatIso(at),
    startedAt: null,
    completedAt: DateTime.formatIso(at),
  };
  for (const restartCut of [
    undefined,
    restartCutView(source, []),
    restartCutView(source, [{ ...replacement, startedAt: later }]),
    restartCutView(source, [{ ...replacement, status: "failed" }]),
  ])
    assert.deepEqual(decode({ ...base, restartCut }), { ...base, restartCut });
});
