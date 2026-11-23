import { PersonPicker } from "../people/PersonPicker";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { Link, useParams } from "@tanstack/react-router";
import { msg, T, useGT, useLocale, useMessages } from "gt-react";
import { PlusIcon } from "lucide-react";

import { isElectron } from "../../env";
import { useNowMinute } from "../../hooks/useNowMinute";
import { useVisibleThreadShells } from "../../state/entities";
import { buildThreadRouteParams, resolveThreadRouteRef } from "../../threadRoutes";
import {
  hasUnseenCompletion,
  resolveSidebarRowStatus,
  type SidebarThreadStatus,
} from "../Sidebar.logic";
import { SidebarChromeFooter, SidebarChromeHeader } from "../sidebar/SidebarChrome";
import { Button } from "../ui/button";
import { SidebarContent, SidebarGroup, useSidebar } from "../ui/sidebar";
import { SharedThreadLabel } from "../people/SharedThreadLabel";
import { usePersonViewThreads } from "../people/usePersonView";
import { promachosConversations } from "@t3tools/client-runtime/promachos/conversations";
import { PromachosRoundButton } from "./PromachosChrome";
import { usePromachosHome, useStartPromachosConversation } from "./promachosMode";

/** Promachos mode's sidebar: the home's conversations in Chromeria's own chrome. */
export function PromachosSidebar() {
  const { start, refs } = usePromachosHome();
  return (
    <>
      <SidebarChromeHeader isElectron={isElectron} />
      <PersonPicker />
      <SidebarContent>
        {start !== null ? (
          <PromachosConversationList start={start} homeRefs={refs} />
        ) : (
          <SidebarGroup>
            <p className="px-2 pt-1 pb-2 text-muted-foreground text-xs">
              <T>Choose the project that is the Promachos home to see his conversations.</T>
            </p>
            <div className="px-2">
              <Button size="sm" variant="outline" render={<Link to="/settings/promachos" />}>
                <T>Choose home</T>
              </Button>
            </div>
          </SidebarGroup>
        )}
      </SidebarContent>
      <SidebarChromeFooter />
    </>
  );
}

function PromachosConversationList({
  start,
  homeRefs,
}: {
  start: ScopedProjectRef;
  homeRefs: ReadonlyArray<ScopedProjectRef>;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const locale = useLocale();
  const threads = useVisibleThreadShells();
  const personThreads = usePersonViewThreads(threads);
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
      <div className="flex items-center justify-end px-3 pb-2">
        <PromachosRoundButton
          label={gt("New conversation")}
          onClick={() => {
            if (isMobile) setOpenMobile(false);
            void startConversation(start);
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
