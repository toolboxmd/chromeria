import { MessageId, RunId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { acceptReply, barrierComplete, bindRun, continueRun, nextRound } from "./barrier.ts";
import { makeMessage, makeRun, makeState } from "./testFixtures.ts";
import { APPEND } from "./testFixtures.ts";
import type { SpectrumRound } from "./state.ts";

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

  it("starts free turns in Color order only after each reply", () => {
    let state = makeState({ mode: "free", limit: 3 });
    for (const color of ["blue", "red", "blue"]) {
      const round = nextRound(state)!;
      expect(round.phase).toBe("free");
      expect(round.slots.map((slot) => slot.threadId)).toEqual([color]);
      expect(nextRound({ ...state, round })).toBeNull();
      state = Object.assign({}, state, { round: complete(round) });
    }
    expect(nextRound(state)).toBeNull();
  });

  it("does not cross a barrier until its transcript append outbox is consumed", () => {
    const state = makeState();
    const round = complete(nextRound(state)!);
    expect(nextRound({ ...state, round, outbox: [APPEND] })).toBeNull();
    expect(nextRound({ ...state, round, outbox: [] })!.phase).toBe("relay");
    expect(nextRound({ ...state, generation: 1, round })).toBeNull();
  });

  it("refuses a short council and an unknown moderator", () => {
    expect(() => nextRound(makeState({ limit: 1 }))).toThrow("at least two");
    expect(() => nextRound(makeState({ moderator: 2 }))).toThrow("participating Color");
    expect(nextRound(makeState({ status: "retired" }))).toBeNull();
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
