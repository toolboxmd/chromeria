interface TurnStartInput {
  readonly threadId: string;
  readonly bootstrap?: { readonly createThread?: unknown } | undefined;
}

/**
 * When a send starts a new top-level Promachos conversation and his role has a model list, asks Prism
 * for the model; the server replaces the composer's placeholder model. An empty list starts normally.
 * `promachosModels` is null when the thread is not a Promachos chat.
 */
function promachosTurnStartInput<T extends TurnStartInput>(
  input: T,
  promachosModels: ReadonlyArray<unknown> | null,
): T {
  const createsTopLevelThread = input.bootstrap?.createThread !== undefined;
  return createsTopLevelThread && promachosPicksModel(promachosModels)
    ? { ...input, prismRole: "promachos" }
    : input;
}

/** Whether Prism picks the model for a new conversation. */
export function promachosPicksModel(promachosModels: ReadonlyArray<unknown> | null): boolean {
  return promachosModels !== null && promachosModels.length > 0;
}

/**
 * The multiple-model selection the view acts on: none where Prism picks the model, since each
 * started thread would be a new Promachos conversation. The saved selection is kept and returns in
 * the standard view.
 */
export function promachosMultipleModelSelections<T>(
  saved: T | null,
  promachosModels: ReadonlyArray<unknown> | null,
): T | null {
  return promachosPicksModel(promachosModels) ? null : saved;
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
