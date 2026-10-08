import { useLocation } from "@tanstack/react-router";

import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { PERSON_VIEWS } from "./personView";
import { usePersonView, useRefreshDevicePeopleOnFocus } from "./usePersonView";

/**
 * Picks whose threads the sidebar lists. Its own full-width row, under the
 * sidebar header.
 */
export function PersonPicker() {
  const onSettings = useLocation({
    select: (location) =>
      location.pathname === "/settings" || location.pathname.startsWith("/settings/"),
  });
  const [view, setView] = usePersonView();
  useRefreshDevicePeopleOnFocus();
  if (onSettings) return null;

  return (
    <div className="shrink-0 px-2 pb-1">
      <ToggleGroup
        aria-label="Person"
        className="w-full"
        value={[view]}
        onValueChange={(next) => {
          const picked = PERSON_VIEWS.find((candidate) => candidate === next[0]);
          if (picked !== undefined) setView(picked);
        }}
      >
        {PERSON_VIEWS.map((option) => (
          <Toggle key={option} className="flex-1" value={option}>
            {option}
          </Toggle>
        ))}
      </ToggleGroup>
    </div>
  );
}
