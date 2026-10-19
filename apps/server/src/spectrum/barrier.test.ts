import {
  MessageId,
  type OrchestrationV2Command,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import * as Schema from "effect/Schema";
import {
  acceptReply,
  barrierComplete,
  bindRun,
  continueRun,
  fallBack,
  nextRound,
} from "./barrier.ts";
import { makeMessage, makeRun, makeState } from "./testFixtures.ts";
import { APPEND } from "./testFixtures.ts";
import { SpectrumState, type SpectrumRound } from "./state.ts";

const spare = { instanceId: ProviderInstanceId.make("codex-spare"), model: "test-model" };
/** A Spectrum started after #525: White is its own member, not one of the Colors. */
function withWhite(overrides: Partial<SpectrumState> = {}): SpectrumState {
  const { moderator: _moderator, ...state } = makeState(overrides);
  return {
    ...state,
    white: {
      threadId: ThreadId.make("white"),
      label: "White",
      selection: { ...spare, model: "judge" },
      fallbacks: [spare],
      attempted: [],
    },
  };
}
const encodeState = Schema.encodeSync(SpectrumState);
const decodeState = Schema.decodeUnknownSync(SpectrumState);

function complete(round: SpectrumRound): SpectrumRound {
  for (const slot of round.slots) {
    const run = makeRun(slot);
    round = bindRun(round, { generation: round.generation, commandId: slot.commandId, run });
    const result = acceptReply(round, {
      generation: round.generation,
      run,
      messages: [makeMessage(run, `Answer from ${slot.threadId}`)],
      recoveryPending: false,
    });
    expect(result.type).toBe("accepted");
    if (result.type === "accepted") round = result.round;
  }
  return round;
}

describe("Spectrum barriers", () => {
  it("holds council until every Color replies, runs two relays, then only the moderator", () => {
    let state = makeState();
    let round = nextRound(state)!;
    expect(round.phase).toBe("independent");
    expect(round.slots.map((slot) => slot.threadId)).toEqual(["blue", "red"]);
    expect(nextRound({ ...state, round })).toBeNull();
    const blue = makeRun(round.slots[0]!);
    round = bindRun(round, { generation: 0, commandId: round.slots[0]!.commandId, run: blue });
    const result = acceptReply(round, {
      generation: 0,
      run: blue,
      messages: [makeMessage(blue, "A")],
      recoveryPending: false,
    });
    expect(result.type).toBe("accepted");
    if (result.type === "accepted") round = result.round;
    expect(barrierComplete(round)).toBe(false);
    expect(nextRound({ ...state, round })).toBeNull();
    // Restart from the partial barrier: the already accepted slot is not required again.
    const redSlot = round.slots[1]!;
    const red = makeRun(redSlot);
    round = bindRun(round, { generation: 0, commandId: redSlot.commandId, run: red });
    const redResult = acceptReply(round, {
      generation: 0,
      run: red,
      messages: [makeMessage(red, "B")],
      recoveryPending: false,
    });
    if (redResult.type === "accepted") round = redResult.round;
    expect(barrierComplete(round)).toBe(true);
    state = { ...state, round };
    for (const step of [1, 2]) {
      round = nextRound(state)!;
      expect(round.phase).toBe("relay");
      expect(round.step).toBe(step);
      expect(round.slots).toHaveLength(2);
      state = Object.assign({}, state, { round: complete(round) });
    }
    round = nextRound(state)!;
    expect(round.phase).toBe("synthesis");
    expect(round.slots.map((slot) => slot.threadId)).toEqual(["red"]);
    expect(nextRound({ ...state, round: complete(round) })).toBeNull();
  });

  it("opens free mode in Color order only after each reply, then White moderates", () => {
    let state = makeState({ mode: "free", limit: 3 });
    for (const color of ["blue", "red"]) {
      const round = nextRound(state)!;
      expect(round.phase).toBe("free");
      expect(round.slots.map((slot) => slot.threadId)).toEqual([color]);
      expect(nextRound({ ...state, round })).toBeNull();
      state = Object.assign({}, state, { round: complete(round) });
    }
    const moderation = nextRound(state)!;
    expect(moderation.phase).toBe("moderation");
    expect(moderation.slots.map((slot) => slot.threadId)).toEqual(["red"]);
  });

  it("does not cross a barrier until its transcript append outbox is consumed", () => {
    const state = makeState();
    const round = complete(nextRound(state)!);
    expect(nextRound({ ...state, round, outbox: [APPEND] })).toBeNull();
    expect(nextRound({ ...state, round, outbox: [] })!.phase).toBe("relay");
    expect(nextRound({ ...state, generation: 1, round })).toBeNull();
  });

  it("refuses a short council, and a stored White that is neither a member nor a Color", () => {
    expect(() => nextRound(makeState({ limit: 1 }))).toThrow("at least two");
    expect(nextRound(makeState({ status: "retired" }))).toBeNull();
    expect(decodeState(encodeState(makeState())).moderator).toBe(1);
    expect(decodeState(encodeState(withWhite())).white?.threadId).toBe("white");
    const { moderator: _moderator, ...neither } = encodeState(makeState());
    expect(() => decodeState(neither)).toThrow("White");
    expect(() => decodeState({ ...encodeState(makeState()), moderator: 2 })).toThrow("White");
    expect(() => decodeState({ ...encodeState(withWhite()), moderator: 1 })).toThrow("White");
  });

  it("council with a separate White: every Color answers and relays, then only White synthesizes", () => {
    let state = withWhite();
    for (const phase of ["independent", "relay", "relay"]) {
      const round = nextRound(state)!;
      expect(round.phase).toBe(phase);
      expect(round.slots.map((slot) => slot.threadId)).toEqual(["blue", "red"]);
      state = Object.assign({}, state, { round: complete(round) });
    }
    const synthesis = nextRound(state)!;
    expect(synthesis.phase).toBe("synthesis");
    expect(synthesis.slots.map((slot) => slot.threadId)).toEqual(["white"]);
    expect(nextRound({ ...state, round: complete(synthesis) })).toBeNull();
  });

  it("falls back White's model only on a refused start, like a slot's", () => {
    let state = withWhite();
    for (let step = 0; step < 3; step++)
      state = Object.assign({}, state, { round: complete(nextRound(state)!) });
    const round = nextRound(state)!;
    const slot = round.slots[0]!;
    const start: OrchestrationV2Command = {
      type: "message.dispatch",
      commandId: slot.commandId,
      messageId: slot.messageId,
      threadId: slot.threadId,
      senderThreadId: state.threadId,
      createdBy: "system",
      creationSource: "server",
      attachments: [],
      modelSelection: state.white!.selection,
      dispatchMode: { type: "start_immediately" },
      text: "Synthesize.",
    };
    const next = fallBack({ ...state, round, outbox: [start] }, slot.commandId)!;
    expect(next.white?.selection).toEqual(spare);
    expect(next.white?.fallbacks).toEqual([]);
    expect(next.white?.attempted).toEqual([{ ...spare, model: "judge" }]);
    expect(next.participants).toEqual(state.participants);
    expect(next.outbox[0]).toMatchObject({ threadId: "white", modelSelection: spare });
  });

  it("ignores a successor run, wrong request, and an old generation", () => {
    const round = nextRound(makeState())!;
    const slot = round.slots[0]!;
    const wrong = makeRun(slot, { userMessageId: MessageId.make("other-request") });
    expect(bindRun(round, { generation: 0, commandId: slot.commandId, run: wrong })).toBe(round);
    const run = makeRun(slot);
    expect(bindRun(round, { generation: 1, commandId: slot.commandId, run })).toBe(round);
    const bound = bindRun(round, { generation: 0, commandId: slot.commandId, run });
    expect(
      acceptReply(bound, {
        generation: 0,
        run: { ...run, id: RunId.make("newer-run") },
        messages: [makeMessage(run, "late")],
        recoveryPending: false,
      }),
    ).toEqual({ type: "ignored" });
    expect(
      acceptReply(bound, {
        generation: 1,
        run,
        messages: [makeMessage(run, "late")],
        recoveryPending: false,
      }),
    ).toEqual({ type: "ignored" });
  });

  it("keeps all reply bytes and source order while excluding other runs", () => {
    let round = nextRound(makeState())!;
    const slot = round.slots[0]!;
    const run = makeRun(slot);
    round = bindRun(round, { generation: 0, commandId: slot.commandId, run });
    const messages = [
      makeMessage(run, " \n🟦e\u0301\r\n".repeat(5000), { id: MessageId.make("first") }),
      makeMessage(run, "tail\n", { id: MessageId.make("second") }),
    ];
    const result = acceptReply(round, {
      generation: 0,
      run,
      messages: [...messages, makeMessage(run, "exclude", { runId: RunId.make("other") })],
      recoveryPending: false,
    });
    expect(result.type).toBe("accepted");
    if (result.type === "accepted") {
      expect(result.messages).toEqual(messages);
      expect(result.round.slots[0]!.replyIds).toEqual(["first", "second"]);
      expect(
        acceptReply(result.round, { generation: 0, run, messages, recoveryPending: false }),
      ).toEqual({ type: "ignored" });
    }
  });

  it("waits on partial replies and pending recovery, then follows only the named retry", () => {
    let round = nextRound(makeState())!;
    const slot = round.slots[0]!;
    const run = makeRun(slot);
    round = bindRun(round, { generation: 0, commandId: slot.commandId, run });
    expect(
      acceptReply(round, {
        generation: 0,
        run,
        messages: [makeMessage(run, "partial", { streaming: true })],
        recoveryPending: false,
      }),
    ).toEqual({ type: "waiting" });
    expect(
      acceptReply(round, {
        generation: 0,
        run: { ...run, status: "failed" },
        messages: [],
        recoveryPending: true,
      }),
    ).toEqual({ type: "waiting" });
    const retry = makeRun(slot, {
      id: RunId.make("retry"),
      userMessageId: MessageId.make("retry-input"),
    });
    expect(
      continueRun(round, { generation: 0, sourceRunId: RunId.make("unrelated"), run: retry }),
    ).toBe(round);
    round = continueRun(round, { generation: 0, sourceRunId: run.id, run: retry });
    expect(
      acceptReply(round, {
        generation: 0,
        run,
        messages: [makeMessage(run, "stale")],
        recoveryPending: false,
      }),
    ).toEqual({ type: "ignored" });
    expect(
      acceptReply(round, {
        generation: 0,
        run: retry,
        messages: [makeMessage(retry, "eventual")],
        recoveryPending: false,
      }).type,
    ).toBe("accepted");
  });

  it("fails completed empty replies and unrecoverable interrupted work", () => {
    let round = nextRound(makeState())!;
    const slot = round.slots[0]!;
    const run = makeRun(slot);
    round = bindRun(round, { generation: 0, commandId: slot.commandId, run });
    expect(
      acceptReply(round, { generation: 0, run, messages: [], recoveryPending: false }),
    ).toEqual({ type: "failed", reason: "missing-reply" });
    expect(
      acceptReply(round, {
        generation: 0,
        run: { ...run, status: "interrupted" },
        messages: [makeMessage(run, "unfinished")],
        recoveryPending: false,
      }),
    ).toEqual({ type: "failed", reason: "run-ended" });
  });
});
