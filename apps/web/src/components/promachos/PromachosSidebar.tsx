import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { Link, useParams } from "@tanstack/react-router";
import { HouseIcon, SquarePenIcon } from "lucide-react";
import { useMemo } from "react";

import { isElectron } from "../../env";
import { useNowMinute } from "../../hooks/useNowMinute";
import { useEnvironments } from "../../state/environments";
import { useProjects, useThreadShells } from "../../state/entities";
import { buildThreadRouteParams, resolveThreadRouteRef } from "../../threadRoutes";
import { formatRelativeTimeLabel } from "../../timestampFormat";
import { resolveSidebarThreadStatus, type SidebarThreadStatus } from "../Sidebar.logic";
import {
  childThreadActivityByThreadKey,
  withChildThreadActivity,
} from "../SidebarChildActivity.logic";
import { SidebarChromeFooter, SidebarChromeHeader } from "../sidebar/SidebarChrome";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import {
  SidebarContent,
  SidebarGroup,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "../ui/sidebar";
import { SharedThreadLabel } from "../people/SharedThreadLabel";
import { usePersonViewThreads } from "../people/usePersonView";
import { promachosConversations } from "./promachosConversations";
import { usePromachosHome, useStartPromachosConversation } from "./promachosMode";

const STATUS_BADGE: Partial<
  Record<SidebarThreadStatus, { label: string; variant: "warning" | "info" | "error" }>
> = {
  approval: { label: "Needs you", variant: "warning" },
  input: { label: "Needs you", variant: "warning" },
  working: { label: "Working", variant: "info" },
  failed: { label: "Failed", variant: "error" },
};

function projectLabel(
  project: EnvironmentProject,
  environmentLabelById: ReadonlyMap<string, string>,
  multipleEnvironments: boolean,
): string {
  const environmentLabel = environmentLabelById.get(project.environmentId);
  return multipleEnvironments && environmentLabel
    ? `${project.title} · ${environmentLabel}`
    : project.title;
}

/** Promachos mode's sidebar: the home's conversations in Chromeria's own chrome. */
export function PromachosSidebar() {
  const [home, setHome] = usePromachosHome();
  const projects = useProjects();
  const { environments } = useEnvironments();
  const environmentLabelById = useMemo(
    () =>
      new Map(environments.map((environment) => [environment.environmentId, environment.label])),
    [environments],
  );
  const multipleEnvironments = new Set(projects.map((project) => project.environmentId)).size > 1;
  const homeProject =
    home === null
      ? null
      : (projects.find(
          (project) =>
            project.environmentId === home.environmentId && project.id === home.projectId,
        ) ?? null);

  return (
    <>
      <SidebarChromeHeader isElectron={isElectron} />
      <SidebarContent>
        {homeProject !== null && home !== null ? (
          <PromachosConversationList
            home={home}
            homeLabel={projectLabel(homeProject, environmentLabelById, multipleEnvironments)}
            onChangeHome={() => setHome(null)}
          />
        ) : (
          <SidebarGroup>
            <p className="px-2 pt-1 pb-2 text-muted-foreground text-xs">
              {home === null
                ? "Choose the project that is the Promachos home. His conversations live there."
                : "The Promachos home is not available here. Choose it again."}
            </p>
            <SidebarMenu>
              {projects.map((project) => (
                <SidebarMenuItem key={`${project.environmentId}:${project.id}`}>
                  <SidebarMenuButton
                    onClick={() =>
                      setHome({ environmentId: project.environmentId, projectId: project.id })
                    }
                  >
                    <HouseIcon />
                    <span>{projectLabel(project, environmentLabelById, multipleEnvironments)}</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroup>
        )}
      </SidebarContent>
      <SidebarChromeFooter />
    </>
  );
}

function PromachosConversationList({
  home,
  homeLabel,
  onChangeHome,
}: {
  home: ScopedProjectRef;
  homeLabel: string;
  onChangeHome: () => void;
}) {
  const threads = useThreadShells();
  const personThreads = usePersonViewThreads(threads);
  const conversations = useMemo(
    () => promachosConversations(personThreads, home),
    [personThreads, home],
  );
  const childActivityByThreadKey = useMemo(
    () => childThreadActivityByThreadKey(threads),
    [threads],
  );
  const startConversation = useStartPromachosConversation();
  const { isMobile, setOpenMobile } = useSidebar();
  const routeThreadRef = useParams({
    strict: false,
    select: (params) => resolveThreadRouteRef(params),
  });
  const routeThreadKey = routeThreadRef ? scopedThreadKey(routeThreadRef) : null;
  // Re-render each minute so the relative times stay true.
  useNowMinute();

  return (
    <SidebarGroup>
      <div className="flex items-center gap-1 pb-1">
        <Menu>
          <MenuTrigger
            render={
              <Button
                className="min-w-0 flex-1 justify-start"
                size="sm"
                variant="ghost"
                aria-label="Promachos home"
              />
            }
          >
            <HouseIcon />
            <span className="truncate">{homeLabel}</span>
          </MenuTrigger>
          <MenuPopup align="start">
            <MenuItem onClick={onChangeHome}>Choose another home</MenuItem>
          </MenuPopup>
        </Menu>
        <Button
          size="icon-sm"
          variant="ghost"
          aria-label="New conversation"
          title="New conversation"
          onClick={() => {
            if (isMobile) setOpenMobile(false);
            void startConversation(home);
          }}
        >
          <SquarePenIcon />
        </Button>
      </div>
      <SidebarMenu>
        {conversations.length === 0 ? (
          <p className="px-2 py-1 text-muted-foreground text-xs">No conversations yet.</p>
        ) : null}
        {conversations.map((thread) => {
          const threadRef = scopeThreadRef(thread.environmentId, thread.id);
          const threadKey = scopedThreadKey(threadRef);
          const status = resolveSidebarThreadStatus(
            withChildThreadActivity(thread, childActivityByThreadKey.get(threadKey) ?? null),
          );
          const badge = STATUS_BADGE[status];
          return (
            <SidebarMenuItem key={threadKey}>
              <SidebarMenuButton
                isActive={threadKey === routeThreadKey}
                render={
                  <Link
                    to="/$environmentId/$threadId"
                    params={buildThreadRouteParams(threadRef)}
                    onClick={() => {
                      if (isMobile) setOpenMobile(false);
                    }}
                  />
                }
              >
                <span className="min-w-0 flex-1 truncate">{thread.title}</span>
                <SharedThreadLabel coOwners={thread.coOwners} />
                {badge ? (
                  <Badge className="shrink-0" size="sm" variant={badge.variant}>
                    {badge.label}
                  </Badge>
                ) : (
                  <span className="shrink-0 text-muted-foreground text-xs tabular-nums">
                    {formatRelativeTimeLabel(thread.latestUserMessageAt ?? thread.updatedAt)}
                  </span>
                )}
              </SidebarMenuButton>
            </SidebarMenuItem>
          );
        })}
      </SidebarMenu>
    </SidebarGroup>
  );
}
