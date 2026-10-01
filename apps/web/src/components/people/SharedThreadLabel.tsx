import { Badge } from "../ui/badge";

/** The "Shared" label on a thread row, shown while the thread has co-owners. */
export function SharedThreadLabel({ coOwners }: { coOwners: ReadonlyArray<string> }) {
  if (coOwners.length === 0) return null;
  return (
    <Badge
      className="shrink-0"
      size="sm"
      variant="secondary"
      title={`Shared with ${coOwners.join(", ")}`}
    >
      Shared
    </Badge>
  );
}
