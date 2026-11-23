import { createFileRoute } from "@tanstack/react-router";
import { T, useGT } from "gt-react";

import { AppRouteModal, AppRouteModalClose } from "../components/AppRouteModal";
import { ScheduledTasksSettings } from "../components/settings/ScheduledTasksSettings";
import { SettingsScopeProvider } from "../components/settings/SettingsScopeContext";
import { validateScheduledTasksPageSearch } from "../components/settings/scheduledTasksPage";
import { retainSettingsScope } from "../components/settings/settingsScopeNavigation";
import { WorkspaceBreadcrumb, WorkspaceBreadcrumbItem } from "../components/WorkspaceBreadcrumb";
import { workspaceAreaProps } from "../lib/workspaceFocus";

function ScheduledTasksPage() {
  const gt = useGT();
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  const { project, machine, checkout, ...target } = search;
  return (
    <SettingsScopeProvider
      search={search}
      onChange={(next) => {
        // Every axis is sent so retain middleware recognizes "all" as an explicit target.
        void navigate({
          search: () => ({ project: next.project, machine: next.machine, checkout: next.checkout }),
          hash: "",
          resetScroll: false,
        });
      }}
    >
      <AppRouteModal label={gt("Scheduled tasks")}>
        <div
          {...workspaceAreaProps("scheduled-tasks", 1, gt("Scheduled tasks"))}
          className="flex min-h-0 min-w-0 flex-1 flex-col"
        >
          <header className="flex h-12 shrink-0 items-center gap-3 border-b ps-5 pe-2">
            <WorkspaceBreadcrumb ariaLabel={gt("Scheduled tasks breadcrumb")}>
              <WorkspaceBreadcrumbItem current className="truncate">
                <h1>
                  <T>Scheduled tasks</T>
                </h1>
              </WorkspaceBreadcrumbItem>
            </WorkspaceBreadcrumb>
            <div className="ms-auto flex shrink-0 items-center gap-1">
              <AppRouteModalClose />
            </div>
          </header>
          <div
            key={JSON.stringify({ project, machine, checkout })}
            className="flex min-h-0 flex-1 flex-col"
          >
            <ScheduledTasksSettings {...target} />
          </div>
        </div>
      </AppRouteModal>
    </SettingsScopeProvider>
  );
}

export const Route = createFileRoute("/_chat/scheduled-tasks")({
  validateSearch: validateScheduledTasksPageSearch,
  search: { middlewares: [retainSettingsScope] },
  component: ScheduledTasksPage,
});
