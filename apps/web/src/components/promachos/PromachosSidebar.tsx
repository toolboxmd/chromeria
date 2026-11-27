import { PersonPicker } from "../people/PersonPicker";
import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { ScopedProjectRef } from "@t3tools/contracts";
import { Link, useParams } from "@tanstack/react-router";
import { msg, T, useGT, useLocale, useMessages } from "gt-react";
import { ChevronDownIcon, PlusIcon } from "lucide-react";
import { useState } from "react";

import { contextMenuSourceOf } from "../../contextMenuSource";
import { isElectron } from "../../env";
import { useNowMinute } from "../../hooks/useNowMinute";
import { useThreadActionMenu } from "../../hooks/useThreadActionMenu";
import { useServerConfigs, useThreadShells, useVisibleThreadShells } from "../../state/entities";
import { useOlderSettledShelf, useOlderSettledShelfGrowth } from "../../state/olderSettledShelf";
import { buildThreadRouteParams, resolveThreadRouteRef } from "../../threadRoutes";
import {
  hasUnseenCompletion,
  resolveSidebarRowStatus,
  useSnoozeWakeClock,
  visibleSidebarShelfThreads,
  type SidebarThreadStatus,
} from "../Sidebar.logic";
import { SidebarChromeFooter, SidebarChromeHeader } from "../sidebar/SidebarChrome";
import { Button } from "../ui/button";
import { CollapsibleSectionHeader } from "../ui/collapsible-section-header";
import { SidebarContent, SidebarGroup, useSidebar } from "../ui/sidebar";
import { SharedThreadLabel } from "../people/SharedThreadLabel";
import { usePersonViewThreads } from "../people/usePersonView";
import {
  promachosConversationGroups,
  promachosConversations,
  promachosOlderSettledScopes,
} from "@t3tools/client-runtime/promachos/conversations";
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
  const allThreads = useThreadShells();
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
  // The clock passes each wake time as it comes, so a woken conversation returns on time.
  const snoozeClockMs = useSnoozeWakeClock(conversations);
  const serverConfigs = useServerConfigs();
  const groups = promachosConversationGroups(conversations, {
    now: new Date(snoozeClockMs).toISOString(),
    supports: (environmentId) => serverConfigs.has(environmentId),
  });
  const [expanded, setExpanded] = useState({ snoozed: false, settled: false });
  // Shells leave older settled threads out; the Settled group pages them in, as the sidebar does.
  const [settledVisibleCount, setSettledVisibleCount] = useState(SETTLED_PAGE_COUNT);
  const olderSettled = useOlderSettledShelf({
    scopes: promachosOlderSettledScopes(homeRefs),
    expanded: expanded.settled,
    visibleCount: settledVisibleCount,
  });
  const listedSettled = groups.settled.filter(olderSettled.isListed);
  useOlderSettledShelfGrowth(olderSettled, listedSettled);
  // Counted against every loaded shell: a row the person or tag filter hides is still loaded.
  const settledCount = groups.settled.length + olderSettled.unloadedCount(allThreads);
  const pagedSettled = listedSettled.slice(0, settledVisibleCount);
  // The open conversation stays reachable when it sorts below the page.
  const openSettled = groups.settled.find(
    (thread) => scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)) === routeThreadKey,
  );
  const shownSettled =
    openSettled === undefined || pagedSettled.includes(openSettled)
      ? pagedSettled
      : [...pagedSettled, openSettled];
  // The open conversation's header owns renaming and guest sharing.
  const { openMenu } = useThreadActionMenu({
    threadRef: null,
    projectCwd: null,
    canShareWithGuest: false,
  });
  const renderConversation = (thread: (typeof conversations)[number]) => {
    const threadRef = scopeThreadRef(thread.environmentId, thread.id);
    const threadKey = scopedThreadKey(threadRef);
    const status = resolveSidebarRowStatus(thread);
    const unread =
      status === "ready"
        ? hasUnseenCompletion(thread)
        : status !== "working" && status !== "preparing";
    // While he works or something needs the user, the row says so instead of quoting.
    const latest = status === "ready" ? thread.source.latestVisibleMessage : null;
    return (
      <li key={threadKey}>
        <Link
          to="/$environmentId/$threadId"
          params={buildThreadRouteParams(threadRef)}
          aria-current={threadKey === routeThreadKey ? "page" : undefined}
          onClick={() => {
            if (isMobile) setOpenMobile(false);
          }}
          onContextMenu={(event) => {
            event.preventDefault();
            openMenu({ x: event.clientX, y: event.clientY }, contextMenuSourceOf(event), threadRef);
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
                {latest === null
                  ? resolve(PREVIEW[status])
                  : latest.role === "user"
                    ? gt("You: {text}", {
                        text: latest.text,
                        $context: "Promachos conversation preview of the user's own message",
                      })
                    : latest.text}
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
  };

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
      {conversations.length === 0 && settledCount === 0 ? (
        <p className="px-3 py-1 text-muted-foreground text-sm">
          <T>No conversations yet.</T>
        </p>
      ) : null}
      <ul className="flex flex-col">{groups.active.map(renderConversation)}</ul>
      {(
        [
          [
            "snoozed",
            gt("Snoozed", { $context: "sidebar section" }),
            groups.snoozed,
            groups.snoozed.length,
          ],
          ["settled", gt("Settled", { $context: "sidebar section" }), shownSettled, settledCount],
        ] as const
      ).map(([group, label, parked, count]) =>
        count === 0 ? null : (
          <div key={group} className="px-1 pt-1">
            <CollapsibleSectionHeader
              expanded={expanded[group]}
              onClick={() => setExpanded({ ...expanded, [group]: !expanded[group] })}
              accessory={<span className="font-normal tabular-nums">{count}</span>}
            >
              {label}
            </CollapsibleSectionHeader>
            <ul className="-mx-1 flex flex-col">
              {visibleSidebarShelfThreads(parked, expanded[group], routeThreadKey).map(
                renderConversation,
              )}
            </ul>
            {group === "settled" && expanded.settled && settledCount > shownSettled.length ? (
              <button
                type="button"
                aria-disabled={olderSettled.loading}
                onClick={() => {
                  if (olderSettled.loading) return;
                  olderSettled.retry();
                  setSettledVisibleCount(settledVisibleCount + SETTLED_PAGE_COUNT);
                }}
                className="flex h-9 w-full cursor-pointer items-center gap-2.5 rounded-md px-2 text-left text-sm text-muted-foreground hover:bg-accent/60 hover:text-foreground aria-disabled:cursor-default"
              >
                <ChevronDownIcon aria-hidden className="size-4 shrink-0" />
                {olderSettled.loading
                  ? gt("Loading")
                  : olderSettled.failed
                    ? gt("Could not load older threads. Retry")
                    : gt("{count, plural, one {Show {count} more} other {Show {count} more}}", {
                        count: Math.min(settledCount - shownSettled.length, SETTLED_PAGE_COUNT),
                      })}
              </button>
            ) : null}
          </div>
        ),
      )}
    </SidebarGroup>
  );
}

const SETTLED_PAGE_COUNT = 25;

// Where the conversation stands, for rows that quote no message.
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
