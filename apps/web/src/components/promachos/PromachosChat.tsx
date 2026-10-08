import { useCallback, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

import { ComposerBanner } from "../chat/ComposerBanner";

/**
 * Re-points the message tokens so the user's bubble reads as the outbound
 * side of a chat: the theme's foreground as surface, its background as ink.
 */
export const PROMACHOS_OUTBOUND_BUBBLE_CLASS_NAME =
  "rounded-2xl px-4 py-2.5 [--contrast-message-foreground:var(--background)] [--message-foreground:var(--background)] [--message-surface:var(--foreground)]";

const INBOUND_BUBBLE_CLASS_NAME =
  "min-w-0 max-w-[85%] rounded-2xl bg-message px-4 py-2.5 text-message-foreground";

/** Static, so a long turn costs no repaints. */
export function PromachosWorkingBubble() {
  return (
    <div className={INBOUND_BUBBLE_CLASS_NAME} role="status" aria-label="Working">
      <span aria-hidden className="flex h-5 items-center gap-1 text-muted-foreground">
        <span className="size-1.5 rounded-full bg-current opacity-40" />
        <span className="size-1.5 rounded-full bg-current opacity-70" />
        <span className="size-1.5 rounded-full bg-current" />
      </span>
    </div>
  );
}

/** Where the composer's question or approval card appears inside the chat. */
export function PromachosInlineCardHost({
  hostRef,
}: {
  hostRef: (element: HTMLDivElement | null) => void;
}) {
  return (
    <div
      ref={hostRef}
      className="mx-auto flex w-full max-w-(--chat-max-width) flex-col items-start empty:hidden"
      data-promachos-inline-card-host=""
    />
  );
}

/**
 * Renders a composer card inline in the chat when the chat has a host for it,
 * and attached to the composer otherwise. The card keeps its owner, so its
 * handlers and state are the composer's own.
 */
export function PromachosInlineCard({
  host,
  children,
}: {
  host: HTMLElement | null;
  children: ReactNode;
}) {
  if (host === null) return <ComposerBanner.Attachment>{children}</ComposerBanner.Attachment>;
  return createPortal(<div className="w-full max-w-lg pb-4">{children}</div>, host);
}

/**
 * The inline card host, owned by the thread whose timeline renders it. While
 * a thread switch still paints the previous thread's timeline, the active
 * thread's composer gets no host and keeps its cards attached to itself.
 */
export function usePromachosInlineCardHost(input: {
  enabled: boolean;
  activeThreadKey: string | null;
  displayedThreadKey: string | null;
}) {
  const { enabled, activeThreadKey, displayedThreadKey } = input;
  const [host, setHost] = useState<{ threadKey: string | null; element: HTMLDivElement } | null>(
    null,
  );
  const timelineHostRef = useCallback(
    (element: HTMLDivElement | null) =>
      setHost(element === null ? null : { threadKey: displayedThreadKey, element }),
    [displayedThreadKey],
  );
  const composerHost =
    enabled && host !== null && host.threadKey !== null && host.threadKey === activeThreadKey
      ? host.element
      : null;
  return { timelineHostRef, composerHost };
}
