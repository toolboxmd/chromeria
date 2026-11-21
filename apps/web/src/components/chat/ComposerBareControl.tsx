import {
  bareDraftIntent,
  resolveBareToggle,
  type BareToggleState,
} from "@t3tools/client-runtime/bare-launch";
import type { ServerProvider } from "@t3tools/contracts";
import { useGT } from "gt-react";
import { FeatherIcon } from "lucide-react";
import { useSyncExternalStore } from "react";

import { cn } from "~/lib/utils";
import type { DraftId } from "../../composerDraftStore";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { COMPOSER_FOOTER_SEGMENT } from "./ComposerModelEffort";

/** Fork (toolboxmd/chromeria#319): the Bare toggle's state for the composer's draft. */
export function useComposerBareToggle(input: {
  readonly draftId: DraftId | null;
  readonly provider: Pick<ServerProvider, "bare"> | null | undefined;
}) {
  const { draftId } = input;
  const requested = useSyncExternalStore(
    bareDraftIntent.subscribe,
    () => draftId !== null && bareDraftIntent.get(draftId),
  );
  const state = resolveBareToggle({
    isNewThread: draftId !== null,
    provider: input.provider,
    requested,
  });
  const toggle = (on: boolean) => {
    if (draftId !== null) bareDraftIntent.set(draftId, on);
  };
  return { state, toggle };
}

export function ComposerBareControl(props: {
  readonly state: BareToggleState;
  readonly onToggle: (on: boolean) => void;
}) {
  const gt = useGT();
  const { state } = props;
  if (state.kind === "hidden") return null;
  const available = state.kind === "available";
  const on = available && state.on;
  const description = available
    ? gt("Start this thread without instructions, plugins, MCP servers or skills.")
    : state.reason === null
      ? gt("This server cannot start bare threads.")
      : gt("Bare is not available for this provider: {reason}", { reason: state.reason });
  return (
    <div className="flex rounded-lg bg-input/40 p-0.5">
      <Tooltip>
        <TooltipTrigger
          render={
            <button
              type="button"
              aria-pressed={on}
              aria-disabled={!available || undefined}
              data-checked={on || undefined}
              className={cn(
                COMPOSER_FOOTER_SEGMENT,
                "aria-disabled:cursor-not-allowed aria-disabled:opacity-50",
              )}
              onClick={() => {
                if (available) props.onToggle(!on);
              }}
            />
          }
        >
          <FeatherIcon aria-hidden="true" className="size-3" />
          {gt("Bare")}
        </TooltipTrigger>
        <TooltipPopup side="top" className="max-w-64">
          {description}
        </TooltipPopup>
      </Tooltip>
    </div>
  );
}
