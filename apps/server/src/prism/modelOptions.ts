import type {
  Color,
  ModelSelection,
  ProviderOptionDescriptor,
  ProviderOptionSelection,
} from "@t3tools/contracts";

/** Legacy effort overrides its option while retaining the selection's other model options. */
export function colorModelSelection(color: Color, driver: string | undefined): ModelSelection {
  const { effort, ...selection } = color;
  if (effort === undefined) return selection;
  const id =
    driver === "codex" || driver === "grok" || driver === "muse"
      ? "reasoningEffort"
      : driver === "opencode"
        ? "variant"
        : "effort";
  return {
    ...selection,
    options: [
      ...(selection.options ?? []).filter((option) => option.id !== id),
      { id, value: effort },
    ],
  };
}

/**
 * Models without descriptors skip the descriptor checks (mirroring how model slugs are only
 * validated when the provider advertises models), but duplicate ids always fail: downstream
 * consumers disagree on whether the first or last value of a duplicated id wins.
 */
export function invalidOptionSelections(
  selections: ReadonlyArray<ProviderOptionSelection>,
  descriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined,
): ReadonlyArray<string> {
  const problems: Array<string> = [];
  const seen = new Set<string>();
  for (const selection of selections) {
    if (seen.has(selection.id)) {
      problems.push(`Option ${selection.id} was specified more than once.`);
      continue;
    }
    seen.add(selection.id);
    if (descriptors === undefined) continue;
    const descriptor = descriptors.find((candidate) => candidate.id === selection.id);
    if (descriptor === undefined) {
      const known = descriptors.map((candidate) => candidate.id).join(", ");
      problems.push(`Unknown option ${selection.id}; supported options: ${known || "none"}.`);
      continue;
    }
    if (descriptor.type === "boolean" && typeof selection.value !== "boolean") {
      problems.push(`Option ${selection.id} expects a boolean value.`);
      continue;
    }
    if (
      descriptor.type === "select" &&
      !descriptor.options.some((choice) => choice.id === selection.value)
    ) {
      const choices = descriptor.options.map((choice) => choice.id).join(", ");
      problems.push(`Option ${selection.id} must be one of: ${choices}.`);
    }
  }
  return problems;
}
