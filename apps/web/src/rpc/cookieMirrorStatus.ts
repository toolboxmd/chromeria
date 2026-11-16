import type { CookieMirrorConnectionStatus, CookieMirrorSiteLimitation } from "@t3tools/contracts";
import { useSyncExternalStore } from "react";

const listeners = new Set<() => void>();
let limitations: ReadonlyArray<CookieMirrorSiteLimitation> = [];
export function recordCookieMirrorStatus(status: CookieMirrorConnectionStatus) {
  if (
    limitations.length === status.limitations.length &&
    limitations.every(
      (item, index) =>
        item.host === status.limitations[index]?.host &&
        item.reason === status.limitations[index]?.reason,
    )
  )
    return;
  limitations = status.limitations;
  for (const listener of listeners) listener();
}
export const useCookieMirrorLimitations = () =>
  useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    () => limitations,
    () => limitations,
  );
