import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type {
  EnvironmentId,
  FeatureMapRef,
  FeatureMapSnapshot,
  ProjectId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import {
  BotIcon,
  Maximize2Icon,
  RefreshCwIcon,
  RotateCcwIcon,
  XIcon,
  ZoomInIcon,
  ZoomOutIcon,
} from "lucide-react";
import { Suspense, useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { useOpenLink } from "~/browser/useOpenLink";
import { observeResize } from "~/lib/observeResize";
import { useOpenChangeRequestLink } from "~/lib/openPullRequestLink";
import { useEnvironments } from "~/state/environments";
import { useFeatureMap } from "~/state/featureMap";
import { buildThreadRouteParams } from "~/threadRoutes";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
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
  FEATURE_MAP_NODE_SELECTOR,
  featureMapPartIdOf,
  type FeatureMapPartView,
  featureMapShown,
  fitMapViewport,
  foldFeatureMap,
  iconAnchorInLayer,
  isChromeriaFeatureMapPart,
  type MapViewport,
  panMapViewport,
  pointerMovedPastDragThreshold,
  scaleAfterWheel,
  stepMapScale,
  zoomMapViewport,
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
  live: "Live",
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
  const expandRef = useRef<HTMLButtonElement>(null);
  const [expanded, setExpanded] = useState(false);
  const closeExpanded = () => {
    setExpanded(false);
    expandRef.current?.focus();
  };
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

  const title =
    shown.kind === "snapshot"
      ? `Map from ${shown.snapshot.branch} at ${shown.snapshot.commit.slice(0, 7)}`
      : "Map";
  const titleSuffix =
    shown.kind === "snapshot" && shown.stale !== null
      ? ` · as of ${timeOf(shown.stale.since)}, GitHub unreachable`
      : null;
  const map = shown.kind === "snapshot" && shown.snapshot._tag === "Map" ? shown.snapshot : null;

  const header = (
    <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border px-3 text-xs text-muted-foreground">
      <span className="min-w-0 flex-1 truncate">
        {title}
        {titleSuffix}
      </span>
      <Button
        ref={expandRef}
        size="icon-xs"
        variant="ghost"
        aria-label="Expand map"
        disabled={map === null}
        onClick={() => setExpanded(true)}
      >
        <Maximize2Icon />
      </Button>
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
    body = (
      <FeatureMapBody
        map={shown.snapshot}
        threadRef={threadRef}
        theme={theme}
        title={`${title}${titleSuffix ?? ""}`}
        expanded={expanded && map !== null}
        onClose={closeExpanded}
      />
    );
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
  title,
  expanded,
  onClose,
}: {
  map: MapSnapshot;
  threadRef: ScopedThreadRef;
  theme: "light" | "dark";
  title: string;
  expanded: boolean;
  onClose: () => void;
}) {
  const navigate = useNavigate();
  const { environments } = useEnvironments();
  const linkEnvironments = new Set(environmentIdsWithCapability(environments, "issueLinks"));
  // Linked threads are read for the map's Issue refs, from every server that keeps links.
  const issueEntries = map.facts.flatMap((entry) =>
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
  );
  const {
    threadsByIssue,
    working,
    error: threadsError,
  } = useIssueRowThreads(issueEntries, linkEnvironments);
  const views = foldFeatureMap(map, { threadsByIssue, working });
  const gaps = featureMapGaps(map);
  const source = featureMapDiagramSource(map.source, views);

  const openLink = useOpenLink(threadRef);
  const openChangeRequestLink = useOpenChangeRequestLink(threadRef);
  const openIssueOrPullRequest = useOpenIssueOrPullRequestLink(
    openChangeRequestLink,
    openLink,
    threadRef,
  );
  const openUrl = (url: string) => {
    if (openIssueOrPullRequest(url, threadRef.environmentId) === null) void openLink(url);
  };
  const openThread = (thread: IssueRowThread) =>
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(scopeThreadRef(thread.environmentId, thread.id)),
    });

  return (
    <ScrollArea className="min-h-0 flex-1">
      {expanded
        ? createPortal(
            <ExpandedMap
              title={title}
              onClose={onClose}
              source={source}
              theme={theme}
              views={views}
              onOpen={openUrl}
              onOpenThread={openThread}
            />,
            document.body,
          )
        : null}
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
          host={map.host}
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

function samePlacements(current: ReadonlyArray<IconPlacement>, next: ReadonlyArray<IconPlacement>) {
  return (
    current.length === next.length &&
    current.every(
      (icon, index) =>
        icon.id === next[index]?.id &&
        icon.left === next[index]?.left &&
        icon.top === next[index]?.top,
    )
  );
}

interface MapPointerDrag {
  readonly id: number;
  readonly startX: number;
  readonly startY: number;
  lastX: number;
  lastY: number;
  moved: boolean;
}

/**
 * Full-window map. The diagram is the only content: the part list stays in the panel.
 * Pan and zoom live here; closing does not reread the map.
 */
function ExpandedMap({
  title,
  onClose,
  source,
  theme,
  views,
  onOpen,
  onOpenThread,
}: {
  title: string;
  onClose: () => void;
  source: string;
  theme: "light" | "dark";
  views: ReadonlyArray<FeatureMapPartView>;
  onOpen: (url: string) => void;
  onOpenThread: (thread: IssueRowThread) => void;
}) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const fittedRef = useRef(false);
  const pointerRef = useRef<MapPointerDrag | null>(null);
  const suppressClickRef = useRef(false);
  const [viewport, setViewport] = useState<MapViewport>({ scale: 1, x: 0, y: 0 });
  const [panning, setPanning] = useState(false);

  const fit = () => {
    const view = viewportRef.current;
    const content = contentRef.current;
    if (view === null || content === null) return false;
    const next = fitMapViewport({
      contentWidth: content.offsetWidth,
      contentHeight: content.offsetHeight,
      viewWidth: view.clientWidth,
      viewHeight: view.clientHeight,
    });
    if (next === null) return false;
    setViewport(next);
    return true;
  };

  const onContentReady = () => {
    if (fittedRef.current) return;
    if (fit()) fittedRef.current = true;
  };

  useEffect(() => {
    closeRef.current?.focus();
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || event.defaultPrevented) return;
      event.preventDefault();
      onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  useEffect(() => {
    const view = viewportRef.current;
    if (view === null) return;
    if (!fittedRef.current && fit()) fittedRef.current = true;
    return observeResize(view, () => {
      if (fittedRef.current) return;
      if (fit()) fittedRef.current = true;
    });
  }, [fit]);

  useEffect(() => {
    const view = viewportRef.current;
    if (view === null) return;
    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      const rect = view.getBoundingClientRect();
      const deltaMode = event.deltaMode === 1 || event.deltaMode === 2 ? event.deltaMode : 0;
      const anchor = { x: event.clientX - rect.left, y: event.clientY - rect.top };
      setViewport((current) =>
        zoomMapViewport(current, anchor, scaleAfterWheel(current.scale, event.deltaY, deltaMode)),
      );
    };
    const onClick = (event: MouseEvent) => {
      if (!suppressClickRef.current) return;
      suppressClickRef.current = false;
      event.preventDefault();
      event.stopPropagation();
    };
    view.addEventListener("wheel", onWheel, { passive: false });
    view.addEventListener("click", onClick, true);
    return () => {
      view.removeEventListener("wheel", onWheel);
      view.removeEventListener("click", onClick, true);
    };
  }, []);

  const zoomAtCenter = (direction: "in" | "out") => {
    const rect = viewportRef.current?.getBoundingClientRect();
    const anchor = { x: (rect?.width ?? 0) / 2, y: (rect?.height ?? 0) / 2 };
    setViewport((current) =>
      zoomMapViewport(current, anchor, stepMapScale(current.scale, direction)),
    );
  };

  const onPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    const target = event.target;
    if (target instanceof Element && target.closest("button")) return;
    pointerRef.current = {
      id: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      lastX: event.clientX,
      lastY: event.clientY,
      moved: false,
    };
  };

  const onPointerMove = (event: React.PointerEvent<HTMLDivElement>) => {
    const drag = pointerRef.current;
    if (drag === null || drag.id !== event.pointerId) return;
    if (!drag.moved) {
      if (
        !pointerMovedPastDragThreshold(
          { x: drag.startX, y: drag.startY },
          { x: event.clientX, y: event.clientY },
        )
      ) {
        return;
      }
      drag.moved = true;
      suppressClickRef.current = true;
      setPanning(true);
      event.currentTarget.setPointerCapture(event.pointerId);
    }
    const dx = event.clientX - drag.lastX;
    const dy = event.clientY - drag.lastY;
    drag.lastX = event.clientX;
    drag.lastY = event.clientY;
    setViewport((current) => panMapViewport(current, { x: dx, y: dy }));
  };

  const endDrag = (event: React.PointerEvent<HTMLDivElement>) => {
    const drag = pointerRef.current;
    if (drag === null || drag.id !== event.pointerId) return;
    pointerRef.current = null;
    setPanning(false);
    if (!drag.moved) return;
    suppressClickRef.current = true;
    window.setTimeout(() => {
      suppressClickRef.current = false;
    }, 0);
  };

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={title}
      className="fixed inset-0 z-50 flex flex-col bg-background [-webkit-app-region:no-drag]"
    >
      <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border px-3 text-xs text-muted-foreground">
        <span className="min-w-0 flex-1 truncate">{title}</span>
        <div className="flex shrink-0 items-center gap-1">
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Zoom out"
            disabled={stepMapScale(viewport.scale, "out") === viewport.scale}
            onClick={() => zoomAtCenter("out")}
          >
            <ZoomOutIcon />
          </Button>
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Zoom in"
            disabled={stepMapScale(viewport.scale, "in") === viewport.scale}
            onClick={() => zoomAtCenter("in")}
          >
            <ZoomInIcon />
          </Button>
          <Button
            size="icon-xs"
            variant="ghost"
            aria-label="Fit map"
            onClick={() => {
              if (fit()) fittedRef.current = true;
            }}
          >
            <RotateCcwIcon />
          </Button>
          <Button
            ref={closeRef}
            size="icon-xs"
            variant="ghost"
            aria-label="Close expanded map"
            onClick={onClose}
          >
            <XIcon />
          </Button>
        </div>
      </div>
      <div
        ref={viewportRef}
        className={`relative min-h-0 flex-1 overflow-hidden touch-none ${panning ? "cursor-grabbing" : "cursor-grab"}`}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
      >
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
            onOpen={onOpen}
            onOpenThread={onOpenThread}
            viewport={viewport}
            contentRef={contentRef}
            onContentReady={onContentReady}
          />
        </Suspense>
      </div>
    </div>
  );
}

/**
 * The drawing itself. Parts open their primary ref on click; a part some thread is working on
 * carries an app-drawn icon at its top right, placed from the rendered node's box.
 * In the expanded view the wrapper is translated and scaled, and icon positions are converted
 * from screen space into that layer.
 */
function FeatureMapDiagram({
  source,
  theme,
  views,
  onOpen,
  onOpenThread,
  viewport = null,
  contentRef,
  onContentReady,
}: {
  source: string;
  theme: "light" | "dark";
  views: ReadonlyArray<FeatureMapPartView>;
  onOpen: (url: string) => void;
  onOpenThread: (thread: IssueRowThread) => void;
  viewport?: MapViewport | null;
  contentRef?: React.RefObject<HTMLDivElement | null>;
  onContentReady?: () => void;
}) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState<SVGSVGElement | null>(null);
  const [icons, setIcons] = useState<ReadonlyArray<IconPlacement>>([]);
  const byId = new Map(views.map((view) => [view.part.id, view]));
  const assignWrapper = (node: HTMLDivElement | null) => {
    wrapperRef.current = node;
    if (contentRef) contentRef.current = node;
  };
  // Node clicks read the newest views, so a refresh never needs the SVG rebound.
  const openNode = useEffectEvent((nodeId: string | null) => {
    const id = featureMapPartIdOf(nodeId, new Set(byId.keys()));
    const url = id === null ? null : (byId.get(id)?.url ?? null);
    if (url !== null) onOpen(url);
  });
  useEffect(() => {
    if (svg === null) return;
    const nodes = [...svg.querySelectorAll<SVGGElement>(FEATURE_MAP_NODE_SELECTOR)];
    const onClick = (event: MouseEvent) =>
      openNode((event.currentTarget as SVGGElement).getAttribute("data-id"));
    for (const node of nodes) {
      node.style.cursor = "pointer";
      node.addEventListener("click", onClick);
    }
    return () => {
      for (const node of nodes) node.removeEventListener("click", onClick);
    };
  }, [svg]);

  const workingIds = views.filter((view) => view.threads.length > 0).map((view) => view.part.id);
  const scale = viewport?.scale ?? 1;
  const expandedLayer = viewport !== null;
  // The expanded drawing uses its viewBox size, so fit and zoom are not capped by the window.
  const viewBox = expandedLayer && svg !== null ? svg.viewBox.baseVal : null;
  const naturalSize =
    viewBox !== null && viewBox.width > 0 && viewBox.height > 0
      ? { width: viewBox.width, height: viewBox.height }
      : null;
  useLayoutEffect(() => {
    if (!expandedLayer || svg === null) return;
    onContentReady?.();
  }, [expandedLayer, onContentReady, svg]);
  useEffect(() => {
    const wrapper = wrapperRef.current;
    if (svg === null || wrapper === null) return;
    const place = () => {
      const origin = wrapper.getBoundingClientRect();
      const wanted = new Set(workingIds);
      const placements: Array<IconPlacement> = [];
      for (const node of svg.querySelectorAll<SVGGElement>(FEATURE_MAP_NODE_SELECTOR)) {
        const id = featureMapPartIdOf(node.getAttribute("data-id"), wanted);
        if (id === null) continue;
        const box = node.getBoundingClientRect();
        placements.push({
          id,
          ...iconAnchorInLayer({
            node: { right: box.right, top: box.top },
            layer: { left: origin.left, top: origin.top },
            scale,
          }),
        });
      }
      setIcons((current) => (samePlacements(current, placements) ? current : placements));
    };
    place();
    return observeResize(wrapper, () => {
      place();
    });
  }, [scale, svg, workingIds]);

  return (
    <div
      ref={assignWrapper}
      className={
        viewport === null
          ? "relative"
          : "relative [&_svg]:h-full [&_svg]:w-full [&_svg]:max-w-none!"
      }
      style={
        viewport === null
          ? undefined
          : {
              transform: `translate(${viewport.x}px, ${viewport.y}px) scale(${viewport.scale})`,
              transformOrigin: "0 0",
              width: naturalSize?.width,
              height: naturalSize?.height,
            }
      }
    >
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
  host,
  views,
  repository,
  onOpen,
  onOpenThread,
}: {
  host: string;
  views: ReadonlyArray<FeatureMapPartView>;
  repository: string;
  onOpen: (url: string) => void;
  onOpenThread: (thread: IssueRowThread) => void;
}) {
  return (
    <section aria-label="Parts" className="flex flex-col gap-1">
      <h3 className="m-0 text-xs font-medium text-muted-foreground">Parts</h3>
      <p className="m-0 text-xs text-muted-foreground">
        Live: an existing capability asserted by the map author. Done: completed tracked work.
      </p>
      <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
        {views.map((view) => {
          const [primary, ...secondary] = view.part.refs;
          return (
            <li key={view.part.id} className="flex items-center gap-2 text-sm">
              <StateSwatch view={view} />
              {isChromeriaFeatureMapPart({ host, repository }, view.part) ? (
                <ChromeriaFeatureMark />
              ) : null}
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
