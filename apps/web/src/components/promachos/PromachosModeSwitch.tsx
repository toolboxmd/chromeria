import { useLocation } from "@tanstack/react-router";
import { CodeIcon, MessagesSquareIcon } from "lucide-react";

import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { usePromachosMode } from "./promachosMode";

/** Switches the sidebar and home threads between the standard view and Promachos mode. */
export function PromachosModeSwitch() {
  const onSettings = useLocation({
    select: (location) =>
      location.pathname === "/settings" || location.pathname.startsWith("/settings/"),
  });
  const [enabled, setEnabled] = usePromachosMode();
  if (onSettings) return null;

  return (
    <ToggleGroup
      aria-label="View"
      className="relative z-10 me-2 ms-auto"
      value={[enabled ? "promachos" : "standard"]}
      onValueChange={(next) => {
        if (next[0] !== undefined) setEnabled(next[0] === "promachos");
      }}
    >
      <Toggle value="standard" aria-label="Standard view" title="Standard view">
        <CodeIcon />
        <span className="hidden @[15rem]/sidebar-header:inline">Code</span>
      </Toggle>
      <Toggle value="promachos" aria-label="Promachos mode" title="Promachos mode">
        <MessagesSquareIcon />
        <span className="hidden @[15rem]/sidebar-header:inline">Promachos</span>
      </Toggle>
    </ToggleGroup>
  );
}
