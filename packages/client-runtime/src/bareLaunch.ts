import type { BareLaunchRequest, ServerProvider } from "@t3tools/contracts";

/** What the composer's Bare toggle shows (toolboxmd/chromeria#319). */
export type BareToggleState =
  | { readonly kind: "hidden" }
  /** `reason` is null when the server predates bare support reporting. */
  | { readonly kind: "unsupported"; readonly reason: string | null }
  | { readonly kind: "available"; readonly on: boolean };

/** Bare is chosen at launch, so only a thread that has not started offers it. */
export function resolveBareToggle(input: {
  readonly isNewThread: boolean;
  readonly provider: Pick<ServerProvider, "bare"> | null | undefined;
  readonly requested: boolean;
}): BareToggleState {
  if (!input.isNewThread) return { kind: "hidden" };
  const support = input.provider?.bare;
  if (support === undefined) return { kind: "unsupported", reason: null };
  if (!support.supported) return { kind: "unsupported", reason: support.reason };
  return { kind: "available", on: input.requested };
}

/** The launch request for a toggle state; an unsupported provider launches normally. */
export function bareLaunchRequest(state: BareToggleState): BareLaunchRequest | undefined {
  return state.kind === "available" && state.on ? {} : undefined;
}

const requestedDrafts = new Set<string>();
const listeners = new Set<() => void>();

/** Per-draft Bare intent. In memory only, so every new thread starts with Bare off. */
export const bareDraftIntent = {
  get: (draftKey: string): boolean => requestedDrafts.has(draftKey),
  set: (draftKey: string, on: boolean): void => {
    if (requestedDrafts.has(draftKey) === on) return;
    if (on) requestedDrafts.add(draftKey);
    else requestedDrafts.delete(draftKey);
    for (const listener of listeners) listener();
  },
  subscribe: (listener: () => void): (() => void) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};

/** The launch request a new thread's draft carries, resolved against its provider at send. */
export function draftBareLaunchRequest(input: {
  readonly draftKey: string;
  readonly provider: Pick<ServerProvider, "bare"> | null | undefined;
}): BareLaunchRequest | undefined {
  return bareLaunchRequest(
    resolveBareToggle({
      isNewThread: true,
      provider: input.provider,
      requested: bareDraftIntent.get(input.draftKey),
    }),
  );
}
