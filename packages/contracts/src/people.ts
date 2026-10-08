export const PEOPLE = ["Luke", "Pauli"] as const;
export const DEFAULT_PERSON = "Luke";

export const threadOwner = (thread: { readonly owner?: string | null | undefined }) =>
  thread.owner ?? DEFAULT_PERSON;
