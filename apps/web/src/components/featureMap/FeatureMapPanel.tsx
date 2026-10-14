import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type {
  EnvironmentId,
  FeatureMapRef,
  FeatureMapSnapshot,
  ProjectId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { BotIcon, RefreshCwIcon } from "lucide-react";
import { Suspense, useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from "react";

import { useOpenLink } from "~/browser/useOpenLink";
import { observeResize } from "~/lib/observeResize";
import { useOpenChangeRequestLink } from "~/lib/openPullRequestLink";
import { useEnvironments } from "~/state/environments";
import { useFeatureMap } from "~/state/featureMap";
import { buildThreadRouteParams } from "~/threadRoutes";
import { MermaidDiagram } from "../chat/MermaidDiagram";
import { environmentIdsWithCapability } from "../issues/issueList.logic";
import type { IssueRowThread } from "../issues/issueStatus.logic";
import { useIssueRowThreads } from "../issues/useIssueRowThreads";
import { useOpenIssueOrPullRequestLink } from "../issues/useOpenIssueOrPullRequestLink";
import { Button } from "../ui/button";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { ScrollArea } from "../ui/scroll-area";
import { Spinner } from "../ui/spinner";
import {
  featureMapClassOf,
  featureMapDiagramSource,
  featureMapGaps,
  featureMapPartIdOf,
  type FeatureMapPartView,
  featureMapShown,
  foldFeatureMap,
} from "./featureMap.logic";

/** How often a visible Map rereads GitHub. */
const FEATURE_MAP_REFRESH_MS = 60_000;

type MapSnapshot = Extract<FeatureMapSnapshot, { _tag: "Map" }>;

const STATE_LABELS = {
  planned: "Planned",
  working: "Working",
  broken: "Broken",
  done: "Done",
  cancelled: "Cancelled",
  unavailable: "Unavailable",
  unknown: "Unknown",
  incomplete: "Incomplete",
  untracked: "Untracked",
  context: "Context",
} as const;

const labelOf = (view: Pick<FeatureMapPartView, "state" | "note">) =>
  STATE_LABELS[view.state ?? view.note ?? "unknown"];

const refLabel = (ref: FeatureMapRef, repository: string) =>
  `${ref.repository.toLowerCase() === repository.toLowerCase() ? "" : ref.repository}${
    ref.kind === "pull" ? " PR " : ""
  }#${ref.number}`.trim();

const timeOf = (iso: string) =>
  new Date(iso).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });

/**
 * Fork: the thread's project as its feature map (toolboxmd/chromeria#235): `docs/map.md` drawn
 * with each part coloured by what GitHub proves, a keyboard-reachable part list, and what the map
 * does not track. Reread every minute while shown and whenever the window regains focus.
 */
export function FeatureMapPanel({
  environmentId,
  projectId,
  threadRef,
  supported,
  theme,
}: {
  environmentId: EnvironmentId;
  projectId: ProjectId | null;
  threadRef: ScopedThreadRef;
  /** The server answers `featureMap.read`; an older one leaves the map unknown. */
  supported: boolean;
  theme: "light" | "dark";
}) {
  const query = useFeatureMap(environmentId, supported ? projectId : null);
  const { refresh } = query;
  // The interval lives with the mounted tab, so it stops when the tab closes.
  useEffect(() => {
    if (!supported || projectId === null) return;
    const interval = window.setInterval(refresh, FEATURE_MAP_REFRESH_MS);
    window.addEventListener("focus", refresh);
    return () => {
      window.clearInterval(interval);
      window.removeEventListener("focus", refresh);
    };
  }, [projectId, refresh, supported]);

  const shown = featureMapShown({
    supported,
    latest:
      query.error !== null
        ? { _tag: "failure", message: query.error }
        : query.isSuccess && query.data !== null
          ? { _tag: "success", snapshot: query.data }
          : null,
    lastGood: query.data,
  });

  const header = (
    <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border px-3 text-xs text-muted-foreground">
      <span className="min-w-0 flex-1 truncate">
        {shown.kind === "snapshot"
          ? `Map from ${shown.snapshot.branch} at ${shown.snapshot.commit.slice(0, 7)}`
          : "Map"}
        {shown.kind === "snapshot" && shown.stale !== null
          ? ` · as of ${timeOf(shown.stale.since)}, GitHub unreachable`
          : null}
      </span>
      <Button
        size="icon-xs"
        variant="ghost"
        aria-label="Refresh map"
        disabled={!supported || projectId === null || query.isPending}
        onClick={refresh}
      >
        <RefreshCwIcon />
      </Button>
    </div>
  );

  let body;
  if (projectId === null) {
    body = <Message>This thread has no project.</Message>;
  } else if (shown.kind === "unsupported") {
    body = <Message>Unknown: this server cannot read a feature map.</Message>;
  } else if (shown.kind === "loading") {
    body = (
      <Message>
        <Spinner aria-label="Loading map" />
      </Message>
    );
  } else if (shown.kind === "error") {
    body = <Message>{shown.message}</Message>;
  } else if (shown.snapshot._tag === "NoMap") {
    body = (
      <Message>
        {shown.snapshot.reason === "missing"
          ? `No docs/map.md on ${shown.snapshot.branch}.`
          : `docs/map.md on ${shown.snapshot.branch} has no mermaid block.`}
      </Message>
    );
  } else {
    body = <FeatureMapBody map={shown.snapshot} threadRef={threadRef} theme={theme} />;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {header}
      {body}
    </div>
  );
}

function Message({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex flex-1 items-center justify-center p-6 text-center text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function FeatureMapBody({
  map,
  threadRef,
  theme,
}: {
  map: MapSnapshot;
  threadRef: ScopedThreadRef;
  theme: "light" | "dark";
}) {
  const navigate = useNavigate();
  const { environments } = useEnvironments();
  const linkEnvironments = useMemo(
    () => new Set(environmentIdsWithCapability(environments, "issueLinks")),
    [environments],
  );
  // Linked threads are read for the map's Issue refs, from every server that keeps links.
  const issueEntries = useMemo(
    () =>
      map.facts.flatMap((entry) =>
        entry.fact?.kind === "issue"
          ? [
              {
                host: map.host,
                repository: entry.repository,
                number: entry.number,
                closingPullRequests: entry.fact.openClosingPullRequests,
              },
            ]
          : [],
      ),
    [map],
  );
  const {
    threadsByIssue,
    working,
    error: threadsError,
  } = useIssueRowThreads(issueEntries, linkEnvironments);
  const views = useMemo(
    () => foldFeatureMap(map, { threadsByIssue, working }),
    [map, threadsByIssue, working],
  );
  const gaps = useMemo(() => featureMapGaps(map), [map]);
  const source = useMemo(() => featureMapDiagramSource(map.source, views), [map.source, views]);

  const openLink = useOpenLink(threadRef);
  const openChangeRequestLink = useOpenChangeRequestLink(threadRef);
  const openIssueOrPullRequest = useOpenIssueOrPullRequestLink(
    openChangeRequestLink,
    openLink,
    threadRef,
  );
  const openUrl = useCallback(
    (url: string) => {
      if (openIssueOrPullRequest(url, threadRef.environmentId) === null) void openLink(url);
    },
    [openIssueOrPullRequest, openLink, threadRef.environmentId],
  );
  const openThread = useCallback(
    (thread: IssueRowThread) =>
      void navigate({
        to: "/$environmentId/$threadId",
        params: buildThreadRouteParams(scopeThreadRef(thread.environmentId, thread.id)),
      }),
    [navigate],
  );

  return (
    <ScrollArea className="min-h-0 flex-1">
      <div className="flex flex-col gap-4 p-3">
        {threadsError !== null ? (
          <p className="m-0 text-xs text-muted-foreground">
            Some servers could not say which threads are working: {threadsError}
          </p>
        ) : null}
        <Suspense
          fallback={
            <Message>
              <Spinner aria-label="Drawing map" />
            </Message>
          }
        >
          <FeatureMapDiagram
            source={source}
            theme={theme}
            views={views}
            onOpen={openUrl}
            onOpenThread={openThread}
          />
        </Suspense>
        <PartList
          views={views}
          repository={map.repository}
          onOpen={openUrl}
          onOpenThread={openThread}
        />
        <GapList gaps={gaps} repository={map.repository} onOpen={openUrl} />
      </div>
    </ScrollArea>
  );
}

interface IconPlacement {
  readonly id: string;
  readonly left: number;
  readonly top: number;
}

/**
 * The drawing itself. Parts open their primary ref on click; a part some thread is working on
 * carries an app-drawn icon at its top right, placed from the rendered node's box.
 */
function FeatureMapDiagram({
  source,
  theme,
  views,
  onOpen,
  onOpenThread,
}: {
  source: string;
  theme: "light" | "dark";
  views: ReadonlyArray<FeatureMapPartView>;
  onOpen: (url: string) => void;
  onOpenThread: (thread: IssueRowThread) => void;
}) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState<SVGSVGElement | null>(null);
  const [icons, setIcons] = useState<ReadonlyArray<IconPlacement>>([]);
  const byId = useMemo(() => new Map(views.map((view) => [view.part.id, view])), [views]);
  // Node clicks read the newest views, so a refresh never needs the SVG rebound.
  const openNode = useEffectEvent((elementId: string) => {
    const id = featureMapPartIdOf(elementId, new Set(byId.keys()));
    const url = id === null ? null : (byId.get(id)?.url ?? null);
    if (url !== null) onOpen(url);
  });
  useEffect(() => {
    if (svg === null) return;
    const nodes = [...svg.querySelectorAll<SVGGElement>("g.node")];
    const onClick = (event: MouseEvent) => openNode((event.currentTarget as SVGGElement).id);
    for (const node of nodes) {
      node.style.cursor = "pointer";
      node.addEventListener("click", onClick);
    }
    return () => {
      for (const node of nodes) node.removeEventListener("click", onClick);
    };
  }, [svg]);

  const workingIds = useMemo(
    () => views.filter((view) => view.threads.length > 0).map((view) => view.part.id),
    [views],
  );
  useEffect(() => {
    const wrapper = wrapperRef.current;
    if (svg === null || wrapper === null) return;
    const place = () => {
      const origin = wrapper.getBoundingClientRect();
      const wanted = new Set(workingIds);
      const placements: Array<IconPlacement> = [];
      for (const node of svg.querySelectorAll<SVGGElement>("g.node")) {
        const id = featureMapPartIdOf(node.id, wanted);
        if (id === null) continue;
        const box = node.getBoundingClientRect();
        placements.push({ id, left: box.right - origin.left, top: box.top - origin.top });
      }
      setIcons(placements);
    };
    place();
    return observeResize(wrapper, () => {
      place();
    });
  }, [svg, workingIds]);

  return (
    <div ref={wrapperRef} className="relative">
      <MermaidDiagram source={source} onRendered={setSvg} />
      {icons.map((icon) => {
        const view = byId.get(icon.id);
        if (view === undefined) return null;
        return (
          <div
            key={icon.id}
            className="absolute -translate-x-1/2 -translate-y-1/2"
            style={{ left: icon.left, top: icon.top }}
          >
            <ThreadIcon threads={view.threads} partName={view.part.name} onOpen={onOpenThread} />
          </div>
        );
      })}
    </div>
  );
}

/** One working thread opens directly; several open a menu of their titles. */
function ThreadIcon({
  threads,
  partName,
  onOpen,
}: {
  threads: ReadonlyArray<IssueRowThread>;
  partName: string;
  onOpen: (thread: IssueRowThread) => void;
}) {
  const only = threads.length === 1 ? threads[0]! : null;
  const label =
    only === null
      ? `${threads.length} threads working on ${partName}`
      : `Open thread working on ${partName}: ${only.title}`;
  if (only !== null) {
    return (
      <Button size="icon-micro" variant="outline" aria-label={label} onClick={() => onOpen(only)}>
        <BotIcon />
      </Button>
    );
  }
  return (
    <Menu>
      <MenuTrigger
        render={
          <Button size="micro" variant="outline" aria-label={label}>
            <BotIcon />
            {threads.length}
          </Button>
        }
      />
      <MenuPopup align="end" side="bottom" sideOffset={4} className="max-w-72">
        {threads.map((thread) => (
          <MenuItem key={`${thread.environmentId}:${thread.id}`} onClick={() => onOpen(thread)}>
            <span className="truncate">{thread.title}</span>
          </MenuItem>
        ))}
      </MenuPopup>
    </Menu>
  );
}

/** Every part in words, with the same links as the drawing, reachable by keyboard. */
function PartList({
  views,
  repository,
  onOpen,
  onOpenThread,
}: {
  views: ReadonlyArray<FeatureMapPartView>;
  repository: string;
  onOpen: (url: string) => void;
  onOpenThread: (thread: IssueRowThread) => void;
}) {
  return (
    <section aria-label="Parts" className="flex flex-col gap-1">
      <h3 className="m-0 text-xs font-medium text-muted-foreground">Parts</h3>
      <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
        {views.map((view) => {
          const [primary, ...secondary] = view.part.refs;
          return (
            <li key={view.part.id} className="flex items-center gap-2 text-sm">
              <StateSwatch view={view} />
              {view.url === null || primary === undefined ? (
                <span className="min-w-0 flex-1 truncate">{view.part.name}</span>
              ) : (
                <button
                  type="button"
                  className="min-w-0 flex-1 truncate rounded-sm text-left hover:underline focus-visible:outline-2 focus-visible:outline-ring"
                  onClick={() => onOpen(view.url!)}
                >
                  {view.part.name}{" "}
                  <span className="text-muted-foreground">{refLabel(primary, repository)}</span>
                </button>
              )}
              {secondary.map((ref) => (
                <button
                  key={`${ref.repository}#${ref.number}`}
                  type="button"
                  className="shrink-0 rounded-sm text-xs text-muted-foreground hover:underline focus-visible:outline-2 focus-visible:outline-ring"
                  onClick={() => onOpen(ref.url)}
                >
                  {refLabel(ref, repository)}
                </button>
              ))}
              {view.threads.length > 0 ? (
                <ThreadIcon
                  threads={view.threads}
                  partName={view.part.name}
                  onOpen={onOpenThread}
                />
              ) : null}
            </li>
          );
        })}
      </ul>
    </section>
  );
}

function StateSwatch({ view }: { view: FeatureMapPartView }) {
  const label = labelOf(view);
  // The swatch reuses the diagram's own fill and stroke, so list and drawing agree.
  const style = Object.fromEntries(
    featureMapClassOf(view)
      .style.split(",")
      .map((declaration) => declaration.split(":") as [string, string]),
  );
  return (
    <span className="flex w-24 shrink-0 items-center gap-1.5 text-xs text-muted-foreground">
      <span
        aria-hidden
        className="size-2.5 shrink-0 rounded-sm border"
        style={{ backgroundColor: style.fill, borderColor: style.stroke }}
      />
      {label}
    </span>
  );
}

function GapList({
  gaps,
  repository,
  onOpen,
}: {
  gaps: ReturnType<typeof featureMapGaps>;
  repository: string;
  onOpen: (url: string) => void;
}) {
  const rows: Array<{
    title: string;
    items: ReadonlyArray<{ readonly key: string; readonly node: React.ReactNode }>;
  }> = [
    {
      title: "Untracked parts",
      items: gaps.untrackedParts.map((part) => ({ key: part.id, node: part.name })),
    },
    {
      title: "Shared Issues",
      items: gaps.sharedIssues.map(({ ref, partIds }) => ({
        key: `${ref.repository}#${ref.number}`,
        node: (
          <span>
            <LinkButton onClick={() => onOpen(ref.url)}>{refLabel(ref, repository)}</LinkButton> on{" "}
            {partIds.join(", ")}
          </span>
        ),
      })),
    },
    {
      title: gaps.openIssuesPartial ? "Open Issues on no part (partial)" : "Open Issues on no part",
      items: gaps.openIssuesOnNoPart.map((issue) => ({
        key: String(issue.number),
        node: (
          <LinkButton onClick={() => onOpen(issue.url)}>
            #{issue.number} {issue.title}
          </LinkButton>
        ),
      })),
    },
    {
      title: "Cancelled, unavailable and incomplete refs",
      items: gaps.problemRefs.map(({ partId, ref, note }) => ({
        key: `${partId} ${ref.repository}#${ref.number}`,
        node: (
          <span>
            {partId}:{" "}
            <LinkButton onClick={() => onOpen(ref.url)}>{refLabel(ref, repository)}</LinkButton>{" "}
            {note}
          </span>
        ),
      })),
    },
    {
      title: "Ref lines for undeclared IDs",
      items: gaps.undeclaredRefLines.map((line, index) => ({
        key: String(index),
        node: <code>{`%% ${line.id}: ${line.text}`}</code>,
      })),
    },
    {
      title: "Unreadable refs",
      items: gaps.unreadableRefs.map((problem, index) => ({
        key: String(index),
        node: (
          <span>
            {problem.id}: <code>{problem.text}</code>
          </span>
        ),
      })),
    },
  ];
  return (
    <section aria-label="Gaps" className="flex flex-col gap-2">
      <h3 className="m-0 text-xs font-medium text-muted-foreground">Gaps</h3>
      {rows.map((row) => (
        <div key={row.title} className="text-sm">
          <div className="text-xs text-muted-foreground">
            {row.title} ({row.items.length})
          </div>
          {row.items.length === 0 ? null : (
            <ul className="m-0 flex list-none flex-col gap-0.5 p-0 pl-2">
              {row.items.map((item) => (
                <li key={item.key}>{item.node}</li>
              ))}
            </ul>
          )}
        </div>
      ))}
    </section>
  );
}

function LinkButton({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      className="rounded-sm text-left hover:underline focus-visible:outline-2 focus-visible:outline-ring"
      onClick={onClick}
    >
      {children}
    </button>
  );
}
