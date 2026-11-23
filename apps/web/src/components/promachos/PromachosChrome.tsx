import { useGT } from "gt-react";
import { SmileIcon } from "lucide-react";
import type { MouseEvent, ReactNode } from "react";

import { cn } from "~/lib/utils";

import { observeVisibleAnimation } from "../../lib/visibleAnimation";
import { ActivityShimmerOverlay } from "../chat/TimelineRowParts";

/** His avatar: Chromeria's own mark on the accent, the same in the list and the chat. */
export function PromachosAvatar({ size }: { size: "sm" | "md" | "lg" }) {
  return (
    <span
      aria-hidden
      className={cn(
        "inline-flex shrink-0 items-center justify-center rounded-full bg-primary text-primary-foreground",
        size === "sm" ? "size-7" : size === "md" ? "size-8" : "size-12",
      )}
    >
      <SmileIcon className={size === "lg" ? "size-6" : "size-4"} />
    </span>
  );
}

/** The chat's centered name pill: his avatar and the conversation's title. */
export function PromachosTitlePill({
  title,
  label,
  onClick,
}: {
  title: string;
  label: string;
  onClick?: ((event: MouseEvent<HTMLButtonElement>) => void) | undefined;
}) {
  return (
    <button
      type="button"
      aria-label={label}
      aria-haspopup={onClick ? "menu" : undefined}
      disabled={onClick === undefined}
      onClick={onClick}
      className="inline-flex h-10 min-w-0 max-w-80 items-center gap-2 rounded-full border bg-card pr-4 pl-1.5 shadow-xs/5 enabled:cursor-pointer enabled:hover:bg-accent focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
      <PromachosAvatar size="sm" />
      <span className="min-w-0 truncate font-medium text-sm">{title}</span>
    </button>
  );
}

/** A round floating control, as the list's and the chat's header buttons. */
export function PromachosRoundButton({
  label,
  onClick,
  children,
}: {
  label: string;
  onClick: (event: MouseEvent<HTMLButtonElement>) => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      aria-label={label}
      onClick={onClick}
      className="inline-flex size-10 shrink-0 cursor-pointer items-center justify-center rounded-full border bg-card shadow-xs/5 hover:bg-accent focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring [&_svg]:size-4"
    >
      {children}
    </button>
  );
}

/** One line under the user's message while he works, naming his current step. */
export function PromachosStatusLine({
  label,
  typing,
}: {
  label?: string | undefined;
  typing: boolean;
}) {
  const gt = useGT();
  const text = typing ? gt("Typing…") : (label ?? gt("Working"));
  return (
    <div className="flex min-h-9 items-center gap-2.5 py-1" role="status" aria-label={text}>
      {typing ? (
        <span aria-hidden className="flex items-center gap-1 px-1 text-primary">
          <span className="size-1.5 rounded-full bg-current opacity-40" />
          <span className="size-1.5 rounded-full bg-current opacity-70" />
          <span className="size-1.5 rounded-full bg-current" />
        </span>
      ) : (
        <PromachosAvatar size="md" />
      )}
      <span
        ref={observeVisibleAnimation}
        className="relative min-w-0 overflow-hidden whitespace-nowrap text-muted-foreground text-sm"
      >
        {text}
        <ActivityShimmerOverlay>{text}</ActivityShimmerOverlay>
      </span>
    </div>
  );
}
