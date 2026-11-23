import { PersonPicker } from "../people/PersonPicker";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { Link, useParams } from "@tanstack/react-router";
import { msg, T, useGT, useLocale, useMessages } from "gt-react";
import { HouseIcon, PlusIcon } from "lucide-react";

import { isElectron } from "../../env";
import { useNowMinute } from "../../hooks/useNowMinute";
import { useEnvironments } from "../../state/environments";
import { useProjects, useVisibleThreadShells } from "../../state/entities";
import { buildThreadRouteParams, resolveThreadRouteRef } from "../../threadRoutes";
import {
  hasUnseenCompletion,
  resolveSidebarRowStatus,
  type SidebarThreadStatus,
} from "../Sidebar.logic";
import { SidebarChromeFooter, SidebarChromeHeader } from "../sidebar/SidebarChrome";
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
import { promachosConversations } from "@t3tools/client-runtime/promachos/conversations";
import { PromachosRoundButton } from "./PromachosChrome";
import { PromachosNewHome } from "./PromachosNewHome";
import {
  usePromachosHome,
  usePromachosHomeRefs,
  useStartPromachosConversation,
} from "./promachosMode";

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
  const gt = useGT();
  const [home, setHome] = usePromachosHome();
  const homeRefs = usePromachosHomeRefs(home);
  const projects = useProjects();
  const { environments } = useEnvironments();
  const environmentLabelById = new Map(
    environments.map((environment) => [environment.environmentId, environment.label]),
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
      <PersonPicker />
      <SidebarContent>
        {homeProject !== null && home !== null ? (
          <PromachosConversationList
            home={home}
            homeLabel={
              homeRefs.length > 1
                ? homeProject.title
                : projectLabel(homeProject, environmentLabelById, multipleEnvironments)
            }
            onChangeHome={() => setHome(null)}
          />
        ) : (
          <SidebarGroup>
            <p className="px-2 pt-1 pb-2 text-muted-foreground text-xs">
              {home === null
                ? gt(
                    "Choose the project that is the Promachos home, or create a new one. His conversations live there.",
                  )
                : gt("The Promachos home is not available here. Choose it again.")}
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
              {environments.map((environment) => (
                <PromachosNewHome
                  key={environment.environmentId}
                  environmentId={environment.environmentId}
                  label={
                    environments.length > 1
                      ? gt("New home · {environment}", { environment: environment.label })
                      : gt("New home")
                  }
                  onCreated={setHome}
                />
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
  const gt = useGT();
  const resolve = useMessages();
  const locale = useLocale();
  const threads = useVisibleThreadShells();
  const personThreads = usePersonViewThreads(threads);
  const homeRefs = usePromachosHomeRefs(home);
  const conversations = promachosConversations(personThreads, homeRefs);
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
      <div className="flex items-center justify-between px-3 pb-2">
        <Menu>
          <MenuTrigger
            render={
              <button
                type="button"
                aria-label={gt("Promachos home: {home}", { home: homeLabel })}
                className="inline-flex size-10 cursor-pointer items-center justify-center rounded-full border bg-card shadow-xs/5 hover:bg-accent focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring [&_svg]:size-4"
              />
            }
          >
            <HouseIcon />
          </MenuTrigger>
          <MenuPopup align="start">
            <MenuItem disabled>{homeLabel}</MenuItem>
            <MenuItem onClick={onChangeHome}>
              <T>Choose another home</T>
            </MenuItem>
          </MenuPopup>
        </Menu>
        <PromachosRoundButton
          label={gt("New conversation")}
          onClick={() => {
            if (isMobile) setOpenMobile(false);
            void startConversation(home);
          }}
        >
          <PlusIcon />
        </PromachosRoundButton>
      </div>
      {conversations.length === 0 ? (
        <p className="px-3 py-1 text-muted-foreground text-sm">
          <T>No conversations yet.</T>
        </p>
      ) : null}
      <ul className="flex flex-col">
        {conversations.map((thread) => {
          const threadRef = scopeThreadRef(thread.environmentId, thread.id);
          const threadKey = scopedThreadKey(threadRef);
          const status = resolveSidebarRowStatus(thread);
          const unread =
            status === "ready"
              ? hasUnseenCompletion(thread)
              : status !== "working" && status !== "preparing";
          return (
            <li key={threadKey}>
              <Link
                to="/$environmentId/$threadId"
                params={buildThreadRouteParams(threadRef)}
                aria-current={threadKey === routeThreadKey ? "page" : undefined}
                onClick={() => {
                  if (isMobile) setOpenMobile(false);
                }}
                className="flex h-18 items-center px-3 hover:bg-accent/60 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring aria-[current=page]:bg-accent"
              >
                <span className="flex min-w-0 flex-1 flex-col gap-0.5">
                  <span className="flex items-baseline gap-2">
                    <span className="min-w-0 flex-1 truncate font-medium text-base text-foreground">
                      {thread.title}
                    </span>
                    <SharedThreadLabel coOwners={thread.source.coOwners} />
                    <span className="shrink-0 text-muted-foreground/70 text-xs tabular-nums">
                      {rowTime(thread.latestUserMessageAt ?? thread.updatedAt, locale)}
                    </span>
                  </span>
                  <span className="flex items-center gap-2">
                    <span className="min-w-0 flex-1 truncate text-muted-foreground text-sm">
                      {resolve(PREVIEW[status])}
                    </span>
                    {unread ? (
                      <span
                        aria-label={gt("Unread")}
                        className="size-2.5 shrink-0 rounded-full bg-primary"
                      />
                    ) : null}
                  </span>
                </span>
              </Link>
            </li>
          );
        })}
      </ul>
    </SidebarGroup>
  );
}

// Thread shells carry no message text, so the preview line says where the conversation stands.
const PREVIEW: Record<SidebarThreadStatus, string> = {
  approval: msg("Needs your approval", { $context: "Promachos conversation preview" }),
  input: msg("Waiting for your answer", { $context: "Promachos conversation preview" }),
  request: msg("A request is waiting", { $context: "Promachos conversation preview" }),
  preparing: msg("Working…", { $context: "Promachos conversation preview" }),
  working: msg("Working…", { $context: "Promachos conversation preview" }),
  waiting: msg("Waiting", { $context: "Promachos conversation preview" }),
  failed: msg("Something went wrong", { $context: "Promachos conversation preview" }),
  limited: msg("Reached a usage limit", { $context: "Promachos conversation preview" }),
  ready: msg("Replied", { $context: "Promachos conversation preview" }),
};

/** Today's time of day, or the date for an older conversation. */
function rowTime(input: string, locale: string) {
  const at = new Date(input);
  if (Number.isNaN(at.getTime())) return "";
  return at.toDateString() === new Date().toDateString()
    ? at.toLocaleTimeString(locale, { hour: "numeric", minute: "2-digit" })
    : at.toLocaleDateString(locale, { month: "short", day: "numeric" });
}
