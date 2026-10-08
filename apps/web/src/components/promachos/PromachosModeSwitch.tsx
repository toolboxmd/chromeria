import type { ComponentProps } from "react";
import { useLocation } from "@tanstack/react-router";

import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { usePromachosMode } from "./promachosMode";

/**
 * Switches the sidebar and home threads between the standard view and
 * Promachos mode. Its own full-width row, so it fits at the sidebar's minimum
 * width beside any titlebar inset.
 */
function PromachosModeSwitch() {
  const onSettings = useLocation({
    select: (location) =>
      location.pathname === "/settings" || location.pathname.startsWith("/settings/"),
  });
  const [enabled, setEnabled] = usePromachosMode();
  if (onSettings) return null;

  return (
    <div className="shrink-0 px-2 pb-1">
      <ToggleGroup
        aria-label="View"
        className="w-full"
        value={[enabled ? "promachos" : "standard"]}
        onValueChange={(next) => {
          if (next[0] !== undefined) setEnabled(next[0] === "promachos");
        }}
      >
        <Toggle className="flex-1" value="standard" title="Standard view">
          Code
        </Toggle>
        <Toggle className="flex-1" value="promachos" title="Promachos mode">
          Promachos
        </Toggle>
      </ToggleGroup>
    </div>
  );
}

/** Preserve the upstream titlebar DOM and attach the fork mode switch below it. */
export function PromachosSidebarHeader(props: ComponentProps<"div">) {
  return (
    <>
      <div {...props} />
      <PromachosModeSwitch />
    </>
  );
}
