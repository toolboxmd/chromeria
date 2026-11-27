import { redirect } from "@tanstack/react-router";

import { validateScheduledTasksSearch } from "./scheduledTasksSettings.logic";
import { validateSettingsRouteSearch } from "./settingsScopeNavigation";

/** Fork (toolboxmd/chromeria#920): scheduled tasks open from the sidebar, outside Settings. */
export function validateScheduledTasksPageSearch(raw: Record<string, unknown>) {
  return { ...validateSettingsRouteSearch(raw), ...validateScheduledTasksSearch(raw) };
}

/**
 * Old `/settings/scheduled-tasks` links keep their task, environment and scope. Their anchor only
 * named the page, and clearing it there would also drop the task before its editor opens.
 */
export function redirectToScheduledTasksPage(): never {
  throw redirect({ to: "/scheduled-tasks", search: true, replace: true });
}
