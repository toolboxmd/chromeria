// Fork: marks settings that Chromeria adds on top of upstream T3 (toolboxmd/chromeria#199).
import { type ReactNode, useId } from "react";

import { Tooltip, TooltipPopup, TooltipTrigger } from "./ui/tooltip";

/** Small spectrum crystal with a "Chromeria feature" tooltip. */
export function ChromeriaFeatureMark() {
  // useId contains characters that break url(#...) references.
  const gradientId = `chromeria-mark-${useId().replace(/[^a-zA-Z0-9]/g, "")}`;
  return (
    <Tooltip>
      <TooltipTrigger
        delay={200}
        render={
          <span
            role="img"
            aria-label="Chromeria feature"
            tabIndex={0}
            className="inline-flex size-3.5 shrink-0 items-center justify-center rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
          />
        }
      >
        <svg viewBox="0 0 16 16" className="size-3.5" aria-hidden="true">
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" style={{ stopColor: "var(--color-red-500)" }} />
              <stop offset="0.35" style={{ stopColor: "var(--color-amber-500)" }} />
              <stop offset="0.6" style={{ stopColor: "var(--color-green-500)" }} />
              <stop offset="0.8" style={{ stopColor: "var(--color-blue-500)" }} />
              <stop offset="1" style={{ stopColor: "var(--color-purple-500)" }} />
            </linearGradient>
          </defs>
          <path
            d="M8 1.2 11.2 4.6V13.4L8 14.8 4.8 13.4V4.6Z"
            fill={`url(#${gradientId})`}
            stroke="currentColor"
            strokeWidth="1"
            strokeLinejoin="round"
            className="text-muted-foreground"
          />
        </svg>
      </TooltipTrigger>
      <TooltipPopup side="top">Chromeria feature</TooltipPopup>
    </Tooltip>
  );
}

/** A settings title followed by the Chromeria mark. */
export function ChromeriaTitle({ children }: { readonly children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {children}
      <ChromeriaFeatureMark />
    </span>
  );
}
