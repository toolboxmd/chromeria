import { promachosStatusText } from "@t3tools/client-runtime/promachos/status";
import { useGT, useLocale } from "gt-react";
import { ClockIcon } from "lucide-react";
import type { MouseEvent, ReactNode } from "react";

import { observeVisibleAnimation } from "../../lib/visibleAnimation";
import { ActivityShimmerOverlay } from "../chat/TimelineRowParts";

/** The chat's centered name pill: the conversation's title. */
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
      className="inline-flex h-10 min-w-0 max-w-80 items-center rounded-full border bg-card px-4 shadow-xs/5 enabled:cursor-pointer enabled:hover:bg-accent focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
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
  const text = typing ? gt("Typing…") : (promachosStatusText(label) ?? gt("Working"));
  return (
    <div className="flex min-h-9 items-center gap-2.5 py-1" role="status" aria-label={text}>
      {typing ? (
        <span aria-hidden className="flex items-center gap-1 px-1 text-muted-foreground">
          <span className="size-1.5 rounded-full bg-current opacity-40" />
          <span className="size-1.5 rounded-full bg-current opacity-70" />
          <span className="size-1.5 rounded-full bg-current" />
        </span>
      ) : null}
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

function lineTime(input: string, locale: string, today: string, yesterday: string) {
  const at = new Date(input);
  if (Number.isNaN(at.getTime())) return "";
  const time = at.toLocaleTimeString(locale, { hour: "numeric", minute: "2-digit" });
  const now = new Date();
  const dayBefore = new Date(now);
  dayBefore.setDate(now.getDate() - 1);
  if (at.toDateString() === now.toDateString()) return `${today} ${time}`;
  if (at.toDateString() === dayBefore.toDateString()) return `${yesterday} ${time}`;
  return `${at.toLocaleDateString(locale, { month: "short", day: "numeric" })} ${time}`;
}

/** A centered gray line between groups: the time they start, or an event with its icon. */
export function PromachosLine(
  props: { kind: "time"; at: string } | { kind: "notice"; label: string },
) {
  const gt = useGT();
  const locale = useLocale();
  return (
    <div className="flex items-center justify-center gap-1.5 py-2 text-muted-foreground/80 text-xs">
      {props.kind === "notice" ? <ClockIcon aria-hidden className="size-3.5 shrink-0" /> : null}
      <span className="min-w-0 truncate">
        {props.kind === "time"
          ? lineTime(
              props.at,
              locale,
              gt("Today", { $context: "time line in a chat, before a time of day" }),
              gt("Yesterday", { $context: "time line in a chat, before a time of day" }),
            )
          : props.label}
      </span>
    </div>
  );
}
