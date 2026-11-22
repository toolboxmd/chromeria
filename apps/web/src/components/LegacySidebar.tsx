import { isThreadVisibleInNavigation } from "@t3tools/client-runtime/state/models";
import { sortThreads } from "@t3tools/client-runtime/state/thread-sort";
import type { SidebarThreadSortOrder } from "@t3tools/contracts/settings";

import type { SidebarThreadSummary } from "../types";

export function legacySidebarVisibleThreads(
  threads: readonly SidebarThreadSummary[],
  sortOrder: SidebarThreadSortOrder,
) {
  return sortThreads(
    threads.filter((thread) => isThreadVisibleInNavigation(thread)),
    sortOrder,
  );
}
