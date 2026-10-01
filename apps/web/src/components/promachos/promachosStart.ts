import { isSubagentThreadId } from "../subagentThreads";

interface TurnStartInput {
  readonly threadId: string;
  readonly bootstrap?: { readonly createThread?: unknown } | undefined;
}

/**
 * Asks Prism for the Promachos's model when a send creates a new top-level
 * Promachos conversation and his role has a model list (toolboxmd/chromeria#115).
 * The server then replaces the composer's model, which stays as a valid
 * placeholder. With an empty list the conversation starts normally.
 *
 * `promachosModels` is the home's Promachos model list, or null when the
 * thread is not a Promachos chat.
 */
function promachosTurnStartInput<T extends TurnStartInput>(
  input: T,
  promachosModels: ReadonlyArray<unknown> | null,
): T {
  const createsTopLevelThread =
    input.bootstrap?.createThread !== undefined && !isSubagentThreadId(input.threadId);
  return createsTopLevelThread && promachosModels !== null && promachosModels.length > 0
    ? { ...input, prismRole: "promachos" }
    : input;
}

/**
 * Wraps the turn-start command so a qualifying send starts as a Promachos
 * conversation. A Prism refusal comes back as that send's own failure: it is
 * never retried as a normal start, so the user sees why it did not start.
 */
export function withPromachosStart<E, I extends TurnStartInput, R>(
  start: (request: { environmentId: E; input: I }) => R,
  promachosModels: ReadonlyArray<unknown> | null,
) {
  return (request: { environmentId: E; input: I }): R =>
    start({ ...request, input: promachosTurnStartInput(request.input, promachosModels) });
}
