import { T, useGT } from "gt-react";

import { Badge } from "../ui/badge";

/** The "Shared" label on a thread row, shown while the thread has co-owners. */
export function SharedThreadLabel({
  coOwners,
}: {
  readonly coOwners?: ReadonlyArray<string> | undefined;
}) {
  const gt = useGT();
  if (coOwners === undefined || coOwners.length === 0) return null;
  return (
    <Badge
      className="shrink-0"
      size="sm"
      variant="secondary"
      title={gt("Shared with {names}", { names: coOwners.join(", ") })}
    >
      <T context="thread sharing state">Shared</T>
    </Badge>
  );
}
