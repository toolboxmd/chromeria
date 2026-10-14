/**
 * One selected moment: what the raw harness event is, which stored entries match
 * it, and the harness profile the serving state needs to answer it.
 */

export type MomentKind =
  | "session_start"
  | "user_message"
  | "pre_tool"
  | "tool_result"
  | "tool_failure"
  | "before_done"
  | "compaction";

export interface Read {
  readonly path: string;
  readonly kind: "read_tool" | "read_partial" | "shell_full" | "shell_partial";
}

export interface Candidate {
  readonly matcher: string;
  readonly entry: string;
  readonly version: string;
  readonly key: string | null;
  readonly text: string;
  readonly must_know: boolean;
  readonly rule: "pointer" | "inline" | "entry" | null;
  readonly target: string | null;
  /** The complete read paths and instruction that a deny must preserve. */
  readonly opening?: string | null;
  readonly deny_text?: string | null;
  /** Cue's score (logit yes minus no) for an `intent` candidate. */
  readonly score?: number | null;
}

/** A finding that is not a candidate: an unreadable key, a missing file, failure evidence. */
export interface Note {
  readonly matcher: string;
  readonly outcome: string;
  readonly entry?: string;
  readonly reason?: string;
}

export interface Profile {
  readonly harness: string;
  readonly delivering: ReadonlyArray<MomentKind>;
  readonly carry_to: MomentKind | null;
  readonly limit_bytes: number;
  /** How long a reply may wait for the hook's settle before it is released as unverified. */
  readonly settle_within_ms: number;
  readonly session_start_limit_bytes: number;
  readonly new_epoch_sources: ReadonlyArray<string>;
  readonly templates: {
    readonly context: unknown;
    readonly deny: unknown;
    readonly pointer?: unknown;
  };
  readonly heads: { readonly deny: string; readonly context: string };
  readonly deny_evidence?: "projection" | "none";
  /** A deny reason the harness shows only up to this many bytes; unset means `limit_bytes`. */
  readonly deny_limit_bytes?: number;
}

export interface Selection {
  /** Verified only while owed; its complete line is never truncated or run-deduped. */
  readonly greatLibrary?: Candidate | null;
  /** Configured whole pointer bytes, kept back from ordinary serving even while not owed. */
  readonly greatLibraryBytes?: number;
  readonly moment: {
    readonly kind: MomentKind;
    /** The event name the reply answers, in the hook-output vocabulary. */
    readonly event: string | null;
    readonly session: string;
    readonly tool_use_id: string | null;
    readonly tool: string | null;
    readonly source: string | null;
    readonly transcript_path?: string | null;
    readonly native_turn?: string | null;
    /** True on failure evidence, false on proven success, null when the harness cannot tell. */
    readonly failed: boolean | null;
    readonly reads: ReadonlyArray<Read>;
    /** The tool call, `<tool>: <input>`, for Cue's window at tool moments; never logged. */
    readonly call?: string | null;
    /** The end of a tool's output, for Cue's window after a failure; never logged. */
    readonly output?: string | null;
  };
  readonly candidates: ReadonlyArray<Candidate>;
  readonly notes: ReadonlyArray<Note>;
  readonly owners: Readonly<Record<string, string | null>>;
  readonly profile: Profile;
}
