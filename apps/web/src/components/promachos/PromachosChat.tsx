import type { MaintainScrollAtEndOptions } from "@legendapp/list/react";
import { useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

import { ComposerBanner } from "../chat/ComposerBanner";

/**
 * Re-points the message tokens so the user's bubble is the theme's ink, as the send button is.
 * The message body paints itself `text-foreground`, so it is re-inked too.
 */
export const PROMACHOS_OUTBOUND_BUBBLE_CLASS_NAME =
  "rounded-[18px] px-3.5 py-2.5 [--contrast-message-foreground:var(--background)] [--message-foreground:var(--background)] [--message-surface:var(--foreground)] [&_.text-foreground]:text-message-foreground [&_.text-muted-foreground]:text-message-foreground/75";

/**
 * One pill row for the composer: attach on the left, the prompt in the middle, dictation and send
 * on the right. Rows the composer adds (validation, live transcription) wrap onto their own line.
 * The send button takes the theme's ink instead of the accent.
 */
export const PROMACHOS_COMPOSER_SURFACE_CLASS_NAME =
  "flex flex-wrap items-end rounded-4xl [--message-action-foreground:var(--background)] [--message-action-hover:color-mix(in_srgb,var(--foreground)_85%,var(--background))] [--message-action:var(--foreground)] [&>*]:basis-full [&>[data-chat-composer-body]]:min-w-0 [&>[data-chat-composer-body]]:flex-1 [&>[data-chat-composer-body]]:basis-0 [&>[data-chat-composer-body]]:px-2 [&>[data-chat-composer-body]]:py-2 [&_.composer-tiptap]:min-h-[calc(1lh+--spacing(2))] [&>[data-chat-composer-footer]:not(.hidden)]:contents [&_[data-chat-composer-controls=footer]]:hidden [&_[data-chat-composer-attach]]:order-first [&_[data-chat-composer-attach]]:m-0 [&_[data-chat-composer-attach]]:basis-auto [&_[data-chat-composer-attach]]:py-1.5 [&_[data-chat-composer-attach]]:ps-1.5 [&_[data-chat-composer-actions=right]]:flex-none [&_[data-chat-composer-actions=right]]:basis-auto [&_[data-chat-composer-actions=right]]:py-1.5 [&_[data-chat-composer-actions=right]]:pe-1.5";

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
  const timelineHostRef = (element: HTMLDivElement | null) =>
    setHost(element === null ? null : { threadKey: displayedThreadKey, element });
  const composerHost =
    enabled && host !== null && host.threadKey !== null && host.threadKey === activeThreadKey
      ? host.element
      : null;
  return { timelineHostRef, composerHost };
}

/** Keep inline question/approval card growth visible while following the chat end. */
export function promachosScrollFollow(
  enabled: boolean,
  normal: MaintainScrollAtEndOptions,
  smooth: MaintainScrollAtEndOptions,
) {
  return enabled
    ? {
        normal: { ...normal, on: { ...normal.on, footerLayout: true } },
        smooth: { ...smooth, on: { ...smooth.on, footerLayout: true } },
      }
    : { normal, smooth };
}
