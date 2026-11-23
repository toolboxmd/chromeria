import { redirect } from "@tanstack/react-router";

import { validateScheduledTasksSearch } from "./scheduledTasksSettings.logic";
import { validateSettingsRouteSearch } from "./settingsScopeNavigation";

/** Fork (toolboxmd/chromeria#920): scheduled tasks open from the sidebar, outside Settings. */
export function validateScheduledTasksPageSearch(raw: Record<string, unknown>) {
  return { ...validateSettingsRouteSearch(raw), ...validateScheduledTasksSearch(raw) };
}

/** Old `/settings/scheduled-tasks` links keep their task, environment, scope and anchor. */
export function redirectToScheduledTasksPage(): never {
  throw redirect({ to: "/scheduled-tasks", search: true, hash: true, replace: true });
}
