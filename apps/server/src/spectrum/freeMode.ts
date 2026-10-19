import type { FreeProgress, SpectrumRound, SpectrumState } from "./state.ts";

/**
 * White-moderated free mode (#398). `state.limit` is the cap on Color turns,
 * the opening round included; moderation, re-asks and the summary are White's
 * calls, not Color turns.
 */
type Next = FreeProgress["next"];

const moderation = { type: "moderation", reask: false, retried: false } as const satisfies Next;
const summary: Next = { type: "summary", retried: false };

function afterColorTurn(state: SpectrumState, turns: number): Next {
  if (turns < state.participants.length) return { type: "color", participant: turns };
  // White always moderates once after the opening round, even when that round used the cap.
  if (turns === state.participants.length || turns < state.limit) return moderation;
  return summary;
}

/** The persisted progress of the current cycle; a reopened cycle starts with the opening round. */
function freeProgress(state: SpectrumState): FreeProgress {
  if (state.free !== null && state.free.cycle === state.cycle) return state.free;
  // A Spectrum persisted before moderation existed only ever ran Color turns.
  const previous = state.cycle === 0 ? state.round : null;
  const turns =
    previous?.phase === "free"
      ? previous.step + (previous.slots.every((slot) => slot.replyIds !== null) ? 1 : 0)
      : 0;
  return { cycle: state.cycle, turns, next: afterColorTurn(state, turns) };
}

/** Who speaks next and in which phase, or null once the summary is in. */
export function freeSpeaker(
  state: SpectrumState,
): { readonly participant: number; readonly phase: SpectrumRound["phase"] } | null {
  // A failed call closes its barrier with no replies; only a written summary ends the cycle.
  if (state.round?.phase === "summary" && state.round.slots.every((slot) => slot.replyIds?.length))
    return null;
  const { next } = freeProgress(state);
  return next.type === "color"
    ? { participant: next.participant, phase: "free" }
    : { participant: state.moderator, phase: next.type };
}

type Moderation =
  | { readonly type: "next"; readonly participant: number; readonly message: string }
  | { readonly type: "done"; readonly message: string }
  | { readonly type: "malformed" };

/** The completed output's last non-empty line is the only control line. */
export function parseModeration(state: SpectrumState, text: string): Moderation {
  const lines = text.split(/\r?\n/);
  let last = lines.length - 1;
  while (last >= 0 && lines[last]!.trim().length === 0) last--;
  if (last < 0) return { type: "malformed" };
  const control = lines[last]!.trim();
  const message = lines.slice(0, last).join("\n").trim();
  if (control === "DONE") return { type: "done", message };
  const named = /^NEXT:\s*(.+)$/.exec(control)?.[1]?.trim();
  const participant = state.participants.findIndex((color) => color.label === named);
  return participant === -1 ? { type: "malformed" } : { type: "next", participant, message };
}

/**
 * Advances progress for an accepted reply. `transcript` is the text that joins
 * the discussion: control lines, malformed moderation and re-asks never do.
 */
export function acceptFreeReply(
  state: SpectrumState,
  phase: SpectrumRound["phase"],
  text: string,
): { readonly free: FreeProgress; readonly transcript: string | null } {
  const progress = freeProgress(state);
  if (phase === "free") {
    const turns = progress.turns + 1;
    return { free: { ...progress, turns, next: afterColorTurn(state, turns) }, transcript: text };
  }
  if (phase === "summary") return { free: progress, transcript: text };
  const parsed = parseModeration(state, text);
  if (parsed.type === "malformed") {
    const reasked = progress.next.type === "moderation" && progress.next.reask;
    return {
      free: { ...progress, next: reasked ? summary : { ...moderation, reask: true } },
      transcript: null,
    };
  }
  const next: Next =
    parsed.type === "next" && progress.turns < state.limit
      ? { type: "color", participant: parsed.participant }
      : summary;
  return {
    free: { ...progress, next },
    transcript: parsed.message.length === 0 ? null : parsed.message,
  };
}

/** A failed White call gets one retry; null means it already had it. */
export function retryWhiteCall(state: SpectrumState): FreeProgress | null {
  const progress = freeProgress(state);
  const { next } = progress;
  if (next.type === "color" || next.retried) return null;
  return { ...progress, next: { ...next, retried: true } };
}

export const isWhiteCall = (round: SpectrumRound) =>
  round.phase === "moderation" || round.phase === "summary";

export function failedSummaryText(reason: string, discussion: string) {
  return `Spectrum ended without White's summary: ${reason}.\n\n${discussion}`;
}

export const stoppedText = (state: SpectrumState) =>
  state.mode === "free" ? "Spectrum stopped before White's summary." : "Spectrum stopped.";

/** The White-only prompt for a moderation, re-ask or summary call. */
export function whiteInstruction(state: SpectrumState, phase: SpectrumRound["phase"]): string {
  const progress = freeProgress(state);
  const budget = `This discussion is at Color turn ${progress.turns} of ${state.limit}.`;
  if (phase === "summary")
    return `${budget} The discussion has ended. Write White's closing summary of it.`;
  const labels = state.participants.map((color) => color.label).join(", ");
  const reask =
    progress.next.type === "moderation" && progress.next.reask
      ? "Your previous moderation reply did not end with a valid control line. "
      : "";
  return [
    `${reask}You are White, moderating this free discussion. ${budget}`,
    "You may write a short message for the discussion. Then end with one final line:",
    `NEXT: <label> to choose who speaks next (one of: ${labels}), or DONE to close the discussion.`,
  ].join("\n");
}
