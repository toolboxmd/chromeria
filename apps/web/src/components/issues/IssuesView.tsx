import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  type EnvironmentId,
  type GitHubIssueListInput,
  type GitHubIssueListSort,
  type GitHubIssueListState,
  type IssuePullRequest,
  type GitHubIssueRef,
  type IssueGroup,
  issueGroupOf,
  parseIssueUrl,
} from "@t3tools/contracts";
import { Link, useNavigate, useSearch } from "@tanstack/react-router";
import {
  CalendarArrowDownIcon,
  ChevronRightIcon,
  ClockIcon,
  CornerDownRightIcon,
  ExternalLinkIcon,
  HashIcon,
  LayersIcon,
  ListTreeIcon,
  MessageSquareIcon,
  MessageSquarePlusIcon,
  SearchIcon,
} from "lucide-react";
import { msg, T, useGT, useMessages } from "gt-react";
import { memo, useEffect, useRef, useState, type MouseEvent, type ReactNode } from "react";

import { RelativeTime } from "~/components/RelativeTime";
import { isElectron } from "~/env";
import { useOpenChangeRequestLink } from "~/lib/openPullRequestLink";
import { cn } from "~/lib/utils";
import { PULL_REQUESTS_PANEL_REF } from "~/rightPanelStore";
import { useAllEnvironmentShellsBootstrapped, useProjects } from "~/state/entities";
import { useEnvironments } from "~/state/environments";
import { issueDetailRead, useIssueList } from "~/state/githubIssues";
import { useDebouncedValue } from "~/state/queries";
import { useAtomCommand } from "~/state/use-atom-command";
import { buildThreadRouteParams } from "~/threadRoutes";

import {
  PULL_REQUEST_ROW_CLASS,
  PULL_REQUEST_ROW_NUMBER_CLASS,
  PullRequestRowLines,
} from "../pullRequest/PullRequestListRow";
import type { PullRequestFilterOption } from "../pullRequest/PullRequestListFilters";
import { resolveFilterOptions } from "../pullRequest/pullRequestListOptions";
import { PullRequestGlyph } from "../pullRequest/pullRequestIcons";
import { PullRequestLabelChip } from "../pullRequest/pullRequestPresentation";
import { Button } from "../ui/button";
import { InputGroup, InputGroupAddon, InputGroupInput } from "../ui/input-group";
import { RefreshIcon } from "../ui/refresh-icon";
import { SidebarInset } from "../ui/sidebar";
import { Spinner } from "../ui/spinner";
import { Toggle } from "../ui/toggle";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { WorkspaceBreadcrumb, WorkspaceBreadcrumbItem } from "../WorkspaceBreadcrumb";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { IssueDetailPanel } from "./IssueDetailPanel";
import {
  buildIssueTree,
  collectIssueFacets,
  environmentIdsWithCapability,
  issueCountCertainty,
  issueKey,
  issueNumbersScope,
  matchesIssueFilters,
  repositoryKey,
  sortIssues,
  staleIssueCount,
  type EnvironmentIssueEntry,
  type IssueTreeNode,
} from "./issueList.logic";
import { publishIssuePaletteSource } from "./issuePaletteStore";
import { issueMarkdownCwd, resolveIssuePanelEnvironment } from "./issueLinks.logic";
import {
  ISSUE_GROUP_PRESENTATION,
  IssueGroupGlyph,
  issueReasonLabel,
  IssueStateGlyph,
  IssueStatusGlyph,
} from "./issuePresentation";
import {
  COLLAPSED_ISSUE_GROUPS,
  groupIssues,
  issueGroupInputOf,
  matchesIssueGroupFilters,
  type IssueRowGrouping,
  type IssueRowThread,
  type IssueThreadRef,
} from "./issueStatus.logic";
import {
  ISSUE_STATE_OPTIONS,
  IssueFiltersMenu,
  IssueSortMenu,
  type IssueMenuFilters,
} from "./IssueFiltersMenu";
import { ListModeToggle } from "./ListModeToggle";
import { useIssueRowThreads } from "./useIssueRowThreads";
import { useStartThreadFromIssue } from "./useStartThreadFromIssue";

// GitHub's ceiling for a search page and one `threadsForIssues` batch, so a later page leaves the
// thread reads of the rows already shown alone.
const PAGE_SIZE = 100;
const SEARCH_DEBOUNCE_MS = 250;
const PREFERENCES_KEY = "t3code:issue-list-preferences";

const SORT_OPTIONS = [
  { value: "updated", label: msg("Recently updated"), Icon: ClockIcon },
  {
    value: "created",
    label: msg("Newest", { $context: "issue sort order" }),
    Icon: CalendarArrowDownIcon,
  },
  {
    value: "number",
    label: msg("Number", { $context: "issue sort order, by Issue number" }),
    Icon: HashIcon,
  },
] as const satisfies ReadonlyArray<PullRequestFilterOption<GitHubIssueListSort>>;

interface IssueListPreferences {
  readonly state: GitHubIssueListState;
  readonly sort: GitHubIssueListSort;
  readonly groupByParent: boolean;
}

const DEFAULT_PREFERENCES: IssueListPreferences = {
  state: "open",
  sort: "updated",
  groupByParent: false,
};

function readPreferences(): IssueListPreferences {
  try {
    const raw = JSON.parse(localStorage.getItem(PREFERENCES_KEY) ?? "{}") as Record<
      string,
      unknown
    >;
    return {
      state: ISSUE_STATE_OPTIONS.find((option) => option.value === raw.state)?.value ?? "open",
      sort: SORT_OPTIONS.find((option) => option.value === raw.sort)?.value ?? "updated",
      groupByParent: raw.groupByParent === true,
    };
  } catch {
    return DEFAULT_PREFERENCES;
  }
}

interface SelectedIssue {
  readonly environmentId: EnvironmentId;
  readonly reference: GitHubIssueRef;
}

const NO_THREADS: ReadonlyArray<IssueRowThread> = [];
const NO_LOGINS: ReadonlySet<string> = new Set();
const NO_LINKED_PULL_REQUESTS: ReadonlyMap<string, IssuePullRequest> = new Map();

interface IssueRowFacts {
  readonly grouping: IssueRowGrouping;
  readonly threads: ReadonlyArray<IssueRowThread>;
}

const NOBODY_ON_IT: IssueRowFacts = {
  grouping: { group: "nobody-on-it", status: "to-do", reason: null, target: { kind: "issue" } },
  threads: NO_THREADS,
};

/** A count with the mark of how far it can be trusted: `12`, `12+` while rows arrive, `~12`. */
const marked = (count: number, certainty: ReturnType<typeof issueCountCertainty>) =>
  certainty === "floor" ? `${count}+` : certainty === "unsettled" ? `~${count}` : String(count);

/** The Issues half of the Pull Requests page (`/pull-requests?view=issues`). GitHub only. */
export function IssuesView() {
  const gt = useGT();
  const messages = useMessages();
  const { environments } = useEnvironments();
  const environmentIds = environmentIdsWithCapability(environments, "issues");
  const capabilityKnown = environments.some((environment) => environment.serverConfig !== null);
  const projects = useProjects();
  const projectsKnown = useAllEnvironmentShellsBootstrapped();

  const [preferences, setPreferences] = useState(readPreferences);
  const updatePreferences = (patch: Partial<IssueListPreferences>) => {
    setPreferences((previous) => {
      const next = { ...previous, ...patch };
      try {
        localStorage.setItem(PREFERENCES_KEY, JSON.stringify(next));
      } catch {
        // Storage is optional; the choice still holds for this visit.
      }
      return next;
    });
  };
  const [searchValue, setSearchValue] = useState("");
  const query = useDebouncedValue(searchValue.trim(), SEARCH_DEBOUNCE_MS);
  const [filters, setFilters] = useState<IssueMenuFilters>({});
  const [collapsed, setCollapsed] = useState<ReadonlySet<IssueGroup>>(COLLAPSED_ISSUE_GROUPS);
  // The instant "not updated for 30 days" is measured from; Refresh takes a new one.
  const [now, setNow] = useState(() => Date.now());
  const navigate = useNavigate();
  const search = useSearch({ from: "/_chat/pull-requests" });

  // The open side panel lives in the URL, so a thread's linked Issue can open it from elsewhere.
  const selected = ((): SelectedIssue | null => {
    const reference = search.issue === undefined ? null : parseIssueUrl(search.issue);
    if (reference === null) return null;
    const environmentId = resolveIssuePanelEnvironment(
      reference,
      search.selectedEnvironmentId,
      projects,
      {
        issues: environmentIdsWithCapability(environments, "issues"),
        issueLinks: environmentIdsWithCapability(environments, "issueLinks"),
      },
    );
    return environmentId === null ? null : { environmentId, reference };
  })();
  const selectedKey = selected === null ? null : issueKey(selected.reference);
  const select = (next: { readonly environmentId: EnvironmentId; readonly url: string } | null) =>
    void navigate({
      to: "/pull-requests",
      search: (() => {
        const { issue: _issue, selectedEnvironmentId: _environmentId, ...rest } = search;
        return next === null
          ? rest
          : { ...rest, issue: next.url, selectedEnvironmentId: next.environmentId };
      })(),
      replace: true,
    });

  // Repository, labels and milestone narrow the search on GitHub too, so the rows are complete
  // rather than a filter over one page. Parent narrows only the loaded rows.
  const baseInput = ((): GitHubIssueListInput => {
    const repository = filters.repository?.split(" ");
    return {
      state: preferences.state,
      sort: preferences.sort,
      limit: PAGE_SIZE,
      ...(query.length > 0 ? { query: query.slice(0, 200) } : {}),
      ...(filters.labels && filters.labels.length > 0 ? { labels: filters.labels } : {}),
      ...(filters.milestone ? { milestone: filters.milestone } : {}),
      ...(repository?.length === 2 ? { repositories: [filters.repository!] } : {}),
    };
  })();
  const list = useIssueList(
    environmentIds.map((environmentId) => ({ environmentId, input: baseInput })),
  );
  const data = list.data;

  const refresh = () => {
    setNow(Date.now());
    list.refresh();
  };

  const linkEnvironments = new Set(environmentIdsWithCapability(environments, "issueLinks"));
  const rowThreads = useIssueRowThreads(data?.entries ?? [], linkEnvironments);
  const { threadsByIssue, working, waiting } = rowThreads;
  // A row's group is computed from every row, the pull requests every page carries and every
  // row's threads, so a group count can move either way until all of them are read. The total
  // only grows while rows are missing, unless a group or linked filter ties it to those facts too.
  const rowsComplete =
    data !== null && list.rows === "complete" && list.error === null && data.errors.length === 0;
  const threadsRead = !rowThreads.loading && rowThreads.error === null;
  const groupCertainty = issueCountCertainty({ rowsComplete, threadsRead, byGroup: true });
  const rowCertainty = issueCountCertainty({ rowsComplete, threadsRead, byGroup: false });
  const shownCertainty =
    (filters.groups?.length ?? 0) > 0 || filters.linked !== undefined
      ? groupCertainty
      : rowCertainty;
  const facts = (() => {
    const byKey = new Map<string, IssueRowFacts>();
    for (const entry of data?.entries ?? []) {
      const threads = threadsByIssue.get(issueKey(entry)) ?? NO_THREADS;
      byKey.set(issueKey(entry), {
        threads,
        grouping: issueGroupOf(
          issueGroupInputOf(entry, threads, {
            working,
            waiting,
            linkedPullRequests: data!.linkedPullRequests,
            trustedLogins: data!.trustedLogins.get(issueKey(entry)) ?? NO_LOGINS,
          }),
        ),
      });
    }
    return byKey;
  })();
  const factsOf = (entry: EnvironmentIssueEntry): IssueRowFacts =>
    facts.get(issueKey(entry)) ?? NOBODY_ON_IT;

  const visible =
    data === null
      ? []
      : sortIssues(
          data.entries.filter((entry) => {
            if (!matchesIssueFilters(entry, filters)) return false;
            const { grouping, threads } = factsOf(entry);
            return matchesIssueGroupFilters(
              { group: grouping.group, linkedThreadCount: threads.length },
              filters,
            );
          }),
          preferences.sort,
        );
  const groups = preferences.groupByParent
    ? null
    : groupIssues(visible, (entry) => factsOf(entry).grouping.group);
  // The line of numbers counts every loaded row and each repository once, whatever the filters
  // that only narrow the rows shown.
  const openCount = (data?.entries ?? []).filter((entry) => entry.state === "open").length;
  const staleCount = staleIssueCount(data?.entries ?? [], now);
  const facets = collectIssueFacets(data?.entries ?? []);
  const projectRepositories = new Set(
    (data?.repositories ?? []).map((repository) =>
      repositoryKey(repository.host, repository.repository),
    ),
  );
  const tree = preferences.groupByParent ? buildIssueTree(visible, projectRepositories) : null;

  const open = (entry: EnvironmentIssueEntry) =>
    select({ environmentId: entry.environmentId, url: entry.url });
  const openThread = (thread: IssueThreadRef) =>
    void navigate({
      to: "/$environmentId/$threadId",
      params: buildThreadRouteParams(scopeThreadRef(thread.environmentId, thread.id)),
    });
  // The side panel's opener: the in-app panel where a server reads the pull request, else GitHub.
  const openPullRequest = useOpenChangeRequestLink(undefined, PULL_REQUESTS_PANEL_REF);
  const { resolve: resolveStart, start: startFromIssue } = useStartThreadFromIssue();
  const readDetail = useAtomCommand(issueDetailRead, { reportFailure: false });
  // The row whose thread is being started; its button waits and repeat clicks do nothing.
  const [startingKey, setStartingKey] = useState<string | null>(null);
  const starting = useRef(false);
  // The list carries no bodies; read the one Issue, and start with title and URL if that fails.
  const startFromEntry = async (entry: EnvironmentIssueEntry) => {
    if (starting.current) return;
    starting.current = true;
    setStartingKey(issueKey(entry));
    const reference = { host: entry.host, repository: entry.repository, number: entry.number };
    const detail = await readDetail({ environmentId: entry.environmentId, input: reference });
    await startFromIssue({
      ...reference,
      url: entry.url,
      title: entry.title,
      body: detail._tag === "Success" ? detail.value.body : null,
    }).finally(() => {
      starting.current = false;
      setStartingKey(null);
    });
  };
  const startDisabledReason = (entry: EnvironmentIssueEntry) => {
    const target = resolveStart(entry);
    return "reason" in target ? target.reason : null;
  };
  const renderRow = (entry: EnvironmentIssueEntry, showGroupLabel: boolean) => {
    const { grouping, threads } = factsOf(entry);
    return (
      <IssueRow
        entry={entry}
        grouping={grouping}
        showGroupLabel={showGroupLabel}
        threads={threads}
        showProject={projectRepositories.size > 1}
        selected={selectedKey === issueKey(entry)}
        startDisabledReason={startDisabledReason(entry)}
        starting={startingKey === issueKey(entry)}
        startBusy={startingKey !== null}
        onOpen={open}
        onOpenThread={openThread}
        onOpenPullRequest={openPullRequest}
        onStart={startFromEntry}
      />
    );
  };
  // The command palette searches what the page shows while it is open.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish visible entries to the external command-palette registry.
  useEffect(() => {
    publishIssuePaletteSource({ entries: visible, open });
  }, [open, visible]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Remove this page from the command-palette registry when it unmounts.
  useEffect(() => () => publishIssuePaletteSource(null), []);

  const selectedCwd =
    selected === null
      ? null
      : issueMarkdownCwd(projects, selected.environmentId, selected.reference);

  const shown = marked(visible.length, shownCertainty);
  const numbersScope = issueNumbersScope(baseInput);
  const numbers = (
    <p className="flex flex-wrap items-center gap-x-3 gap-y-0.5 px-2 text-xs text-muted-foreground">
      <span>
        {numbersScope === "matching"
          ? // GitHub narrowed the list, so these are the matches, not the open total.
            gt("Matching Issues: {count}", { count: shown })
          : numbersScope === "listed"
            ? shownCertainty === "final"
              ? gt("{count, plural, one {{count} Issue} other {{count} Issues}}", {
                  count: visible.length,
                })
              : shownCertainty === "floor"
                ? gt("{count}+ Issues", { count: visible.length })
                : gt("~{count} Issues", { count: visible.length })
            : visible.length !== openCount
              ? // Filters that only narrow the rows shown leave the total of open Issues in sight.
                gt("{shown} of {total} open Issues", {
                  shown,
                  total: marked(openCount, rowCertainty),
                })
              : shownCertainty === "final"
                ? gt("{count, plural, one {{count} open Issue} other {{count} open Issues}}", {
                    count: openCount,
                  })
                : gt("{count} open Issues", { count: shown })}
      </span>
      {numbersScope === "open" ? (
        <span>
          {gt("Not updated for 30 days: {count}", { count: marked(staleCount, rowCertainty) })}
        </span>
      ) : null}
      {(list.isPending && !rowsComplete) || rowThreads.loading ? (
        <Spinner size="xs" aria-label={gt("Loading more Issues")} />
      ) : null}
    </p>
  );
  let body: ReactNode;
  if (environmentIds.length === 0) {
    body = (
      <EmptyState>
        {capabilityKnown ? gt("No connected server can read Issues.") : <Spinner aria-hidden />}
      </EmptyState>
    );
  } else if (data === null) {
    body = list.error ? (
      <EmptyState>{list.error}</EmptyState>
    ) : (
      <EmptyState>
        <Spinner aria-label={gt("Loading Issues")} />
      </EmptyState>
    );
  } else if (data.repositories.length === 0 && projectsKnown) {
    body = (
      <EmptyState>
        {data.unsupported.length > 0
          ? gt("None of your projects are on GitHub. Issues are read from GitHub only.")
          : gt("Add a project with a GitHub repository to see its Issues.")}
      </EmptyState>
    );
  } else if (visible.length === 0) {
    // A list read to its end with nothing in it still states its total.
    body = list.isPending ? (
      <EmptyState>
        <Spinner aria-hidden />
      </EmptyState>
    ) : (
      <>
        {numbers}
        <EmptyState>{gt("No Issues match.")}</EmptyState>
      </>
    );
  } else {
    body = (
      <>
        {numbers}
        <ul className="flex flex-col">
          {tree === null
            ? (groups ?? []).map(({ group, entries }) => (
                <IssueGroupSection
                  key={group}
                  group={group}
                  count={marked(entries.length, groupCertainty)}
                  collapsed={collapsed.has(group)}
                  onToggle={() =>
                    setCollapsed((previous) => {
                      const next = new Set(previous);
                      if (!next.delete(group)) next.add(group);
                      return next;
                    })
                  }
                >
                  {entries.map((entry) => (
                    <li key={issueKey(entry)}>{renderRow(entry, false)}</li>
                  ))}
                </IssueGroupSection>
              ))
            : tree.map((node) => (
                <IssueTreeItem key={node.key} node={node} depth={0} renderRow={renderRow} />
              ))}
        </ul>
      </>
    );
  }

  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none">
      <div className="flex min-h-0 flex-1">
        <div className="flex min-h-0 min-w-0 flex-1 flex-col bg-background">
          <WorkspacePageHeader electron={isElectron} className="bg-background">
            <WorkspaceBreadcrumb ariaLabel={gt("Issues breadcrumb")}>
              <WorkspaceBreadcrumbItem current>
                <h1 className="truncate">
                  <T>Issues</T>
                </h1>
              </WorkspaceBreadcrumbItem>
            </WorkspaceBreadcrumb>
            <div className="[-webkit-app-region:no-drag]">
              <ListModeToggle mode="issues" />
            </div>
            <div className="min-w-0 flex-1" />
          </WorkspacePageHeader>
          <div className="min-h-0 flex-1 overflow-y-auto">
            <WorkspacePageContainer width="expanded" className="min-h-full gap-4">
              <div className="flex flex-wrap items-center gap-2">
                <InputGroup className="min-w-0 flex-1 basis-60 **:[input]:h-9 sm:**:[input]:h-8">
                  <InputGroupAddon>
                    {list.isPending && query.length > 0 ? (
                      <Spinner aria-hidden />
                    ) : (
                      <SearchIcon aria-hidden />
                    )}
                  </InputGroupAddon>
                  <InputGroupInput
                    type="search"
                    value={searchValue}
                    onChange={(event) => setSearchValue(event.currentTarget.value)}
                    placeholder={gt("Search Issues")}
                    aria-label={gt("Search Issues")}
                  />
                </InputGroup>
                <IssueSortMenu
                  value={preferences.sort}
                  options={resolveFilterOptions(SORT_OPTIONS, messages)}
                  onChange={(sort) => updatePreferences({ sort })}
                />
                <IssueFiltersMenu
                  state={preferences.state}
                  onState={(state) => updatePreferences({ state })}
                  filters={filters}
                  onFilters={setFilters}
                  facets={facets}
                  repositories={data?.repositories ?? []}
                />
                <Toggle
                  variant="outline"
                  aria-label={gt("Group by parent")}
                  title={gt("Group by parent instead of status")}
                  pressed={preferences.groupByParent}
                  onPressedChange={(pressed) => updatePreferences({ groupByParent: pressed })}
                >
                  <ListTreeIcon className="size-4" />
                </Toggle>
                <Button
                  size="icon"
                  variant="outline"
                  aria-label={gt("Refresh Issues")}
                  onClick={refresh}
                  disabled={list.isPending}
                >
                  <RefreshIcon size="md" refreshing={list.isPending} />
                </Button>
              </div>

              {data !== null && data.unsupported.length > 0 ? (
                <p className="text-xs text-muted-foreground">
                  {gt("Not supported: {hosts}. Issues are read from GitHub only.", {
                    hosts: data.unsupported
                      .map(({ host, repositoryCount }) =>
                        gt(
                          "{count, plural, one {{count} repository on {host}} other {{count} repositories on {host}}}",
                          { count: repositoryCount, host },
                        ),
                      )
                      .join(", "),
                  })}
                </p>
              ) : null}
              {/* A server or a further page that failed leaves the rows already loaded in
                  place, and says so rather than going quiet. */}
              {list.error !== null && data !== null ? (
                <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning-surface px-3 py-2 text-xs">
                  <span>
                    {gt("{error} Showing the last Issues loaded.", { error: list.error })}
                  </span>
                  <Button size="xs" variant="outline" onClick={refresh}>
                    <T>Retry</T>
                  </Button>
                </div>
              ) : null}
              {/* A search GitHub refused inside an answered page ends that search's walk; reading
                  again is the way to carry on. */}
              {data !== null && data.errors.length > 0 ? (
                <div className="flex items-center justify-between gap-3 text-xs text-destructive-foreground">
                  <span>
                    {data.errors.map((error) => `${error.host}: ${error.message}`).join(" · ")}
                  </span>
                  <Button size="xs" variant="outline" onClick={refresh}>
                    <T>Retry</T>
                  </Button>
                </div>
              ) : null}
              {rowThreads.error !== null ? (
                <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning-surface px-3 py-2 text-xs">
                  <span>
                    {gt(
                      "Linked threads on {server} could not be read, so statuses and counts may be wrong. {error}",
                      {
                        server:
                          environments.find(
                            (environment) =>
                              environment.environmentId === rowThreads.failedEnvironmentId,
                          )?.label ?? String(rowThreads.failedEnvironmentId),
                        error: rowThreads.error,
                      },
                    )}
                  </span>
                  <Button size="xs" variant="outline" onClick={rowThreads.retry}>
                    <T>Retry</T>
                  </Button>
                </div>
              ) : null}
              {data !== null && data.errors.length === 0 && list.rows === "truncated" ? (
                <p className="text-xs text-muted-foreground">
                  {gt("The Issues list may be incomplete, so the counts are provisional.")}
                </p>
              ) : null}
              {data !== null && list.rows === "unknown" ? (
                <p className="text-xs text-muted-foreground">
                  {gt(
                    "A connected server is too old to say whether this list is complete, so the total is not final.",
                  )}
                </p>
              ) : null}

              {body}
            </WorkspacePageContainer>
          </div>
        </div>
        {selected !== null && selectedCwd !== null ? (
          <IssueDetailPanel
            key={`${selected.environmentId} ${selectedKey}`}
            environmentId={selected.environmentId}
            reference={selected.reference}
            cwd={selectedCwd}
            onClose={() => select(null)}
            onChanged={refresh}
            startDisabledReason={(() => {
              const target = resolveStart(selected.reference);
              return "reason" in target ? target.reason : null;
            })()}
            linkEnvironments={linkEnvironments}
            linkedPullRequests={data?.linkedPullRequests ?? NO_LINKED_PULL_REQUESTS}
            onOpenThread={openThread}
            onStart={(detail) =>
              void startFromIssue({
                ...selected.reference,
                url: detail.url,
                title: detail.title,
                body: detail.body,
              })
            }
          />
        ) : null}
      </div>
    </SidebarInset>
  );
}

function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-40 items-center justify-center text-sm text-muted-foreground">
      {children}
    </div>
  );
}

function IssueGroupSection({
  group,
  count,
  collapsed,
  onToggle,
  children,
}: {
  group: IssueGroup;
  /** Marked `~` while a fact that can move a row between groups is unread. */
  count: string;
  collapsed: boolean;
  onToggle: () => void;
  children: ReactNode;
}) {
  const messages = useMessages();
  return (
    <li>
      <button
        type="button"
        aria-expanded={!collapsed}
        className="flex w-full items-center gap-1.5 px-2 pt-3 pb-1 text-xs font-medium text-muted-foreground hover:text-foreground"
        onClick={onToggle}
      >
        <ChevronRightIcon
          aria-hidden
          className={cn("size-3.5 transition-transform", !collapsed && "rotate-90")}
        />
        <IssueGroupGlyph group={group} className="size-3.5" />
        <span>{messages(ISSUE_GROUP_PRESENTATION[group].label)}</span>
        <span className="tabular-nums">{count}</span>
      </button>
      {collapsed ? null : <ul className="flex flex-col">{children}</ul>}
    </li>
  );
}

interface IssueRowProps {
  entry: EnvironmentIssueEntry;
  grouping: IssueRowGrouping;
  /** Group headers already say it; the parent tree does not. */
  showGroupLabel: boolean;
  threads: ReadonlyArray<IssueRowThread>;
  showProject: boolean;
  selected: boolean;
  startDisabledReason: string | null;
  /** Its thread is being started. */
  starting: boolean;
  /** Some row's thread is being started; one start at a time. */
  startBusy: boolean;
  onOpen: (entry: EnvironmentIssueEntry) => void;
  onOpenThread: (thread: IssueThreadRef) => void;
  onOpenPullRequest: (event: MouseEvent<HTMLElement>, url: string) => void;
  onStart: (entry: EnvironmentIssueEntry) => Promise<void>;
}

/** A link in the row's second line, above the row's own hit area. */
const ISSUE_ROW_ACTION_CLASS =
  "relative inline-flex shrink-0 items-center gap-1 rounded-sm font-medium text-foreground underline decoration-muted-foreground/60 underline-offset-2 outline-none hover:decoration-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset";
/** Its words show only on a row wide enough for the longest translation beside the reason. */
const ISSUE_ROW_ACTION_TEXT_CLASS = "hidden @3xl/issue-row:inline";

/** What the row's own action opens: the thread or pull request its grouping names, by a key. */
const targetKey = ({ target }: IssueRowGrouping) =>
  target.kind === "thread"
    ? `thread ${target.thread.environmentId} ${target.thread.id}`
    : target.kind === "pull-request"
      ? `pull-request ${target.pullRequest.url}`
      : "issue";

/**
 * A grouping is computed anew whenever any thread starts or stops, so rows compare what it says,
 * not which object says it: a row whose own facts did not change does not render again.
 */
function sameIssueRow(previous: IssueRowProps, next: IssueRowProps) {
  for (const key of Object.keys(next) as Array<keyof IssueRowProps>) {
    if (key !== "grouping" && !Object.is(previous[key], next[key])) return false;
  }
  const [a, b] = [previous.grouping, next.grouping];
  return (
    a.group === b.group &&
    a.status === b.status &&
    targetKey(a) === targetKey(b) &&
    JSON.stringify(a.reason) === JSON.stringify(b.reason)
  );
}

const IssueRow = memo(function IssueRow({
  entry,
  grouping,
  showGroupLabel,
  threads,
  showProject,
  selected,
  startDisabledReason,
  starting,
  startBusy,
  onOpen,
  onOpenThread,
  onOpenPullRequest,
  onStart,
}: IssueRowProps) {
  const gt = useGT();
  const messages = useMessages();
  const { group, status, reason, target } = grouping;
  const openThread = gt("Open thread", { $context: "link on an Issue row" });
  const openPullRequest =
    target.kind === "pull-request"
      ? gt("Open pull request #{number}", { number: String(target.pullRequest.number) })
      : "";
  return (
    <div
      className={cn(
        PULL_REQUEST_ROW_CLASS,
        "group/issue-row @container/issue-row relative px-2 hover:bg-accent/50",
        selected && "bg-accent text-accent-foreground",
      )}
    >
      {/* The glyph of the status the row names, where it names one. */}
      <IssueStatusGlyph status={reason?.kind === "status" ? reason.status : status} />
      <PullRequestRowLines
        number={<span className={PULL_REQUEST_ROW_NUMBER_CLASS}>#{entry.number}</span>}
        title={
          <button
            type="button"
            aria-current={selected ? "true" : undefined}
            // Its hit area covers the row, so the second line and the update time open the Issue
            // too. Only the title is inside it: the second line holds a link of its own.
            className="block max-w-full truncate text-left after:absolute after:inset-0"
            onClick={() => onOpen(entry)}
          >
            {entry.title}
          </button>
        }
        status={
          <>
            {showGroupLabel ? (
              <span className="text-muted-foreground">
                {messages(ISSUE_GROUP_PRESENTATION[group].label)}
              </span>
            ) : null}
            {entry.subIssueCount > 0 ? (
              <span className="inline-flex items-center gap-0.5 text-muted-foreground">
                <LayersIcon aria-hidden className="size-3" />
                {entry.subIssueCount}
              </span>
            ) : null}
          </>
        }
        meta={
          <>
            {reason !== null ? (
              <span className="min-w-0 truncate font-medium text-foreground/80">
                {issueReasonLabel(reason, messages)}
              </span>
            ) : null}
            {/* The one thing the reason points at, beside it so the title keeps its width. The
                row itself opens the Issue. A narrow row shows the link as its icon alone, which
                the line cannot clip. */}
            {target.kind === "thread" ? (
              <Link
                className={ISSUE_ROW_ACTION_CLASS}
                aria-label={openThread}
                to="/$environmentId/$threadId"
                params={buildThreadRouteParams(
                  scopeThreadRef(target.thread.environmentId, target.thread.id),
                )}
              >
                <MessageSquareIcon aria-hidden className="size-3.5" />
                <span className={ISSUE_ROW_ACTION_TEXT_CLASS}>{openThread}</span>
              </Link>
            ) : target.kind === "pull-request" ? (
              <a
                className={ISSUE_ROW_ACTION_CLASS}
                aria-label={openPullRequest}
                href={target.pullRequest.url}
                target="_blank"
                rel="noreferrer"
                onClick={(event) => onOpenPullRequest(event, target.pullRequest.url)}
              >
                <PullRequestGlyph.pullRequest aria-hidden className="size-3.5" />
                <span className={ISSUE_ROW_ACTION_TEXT_CLASS}>{openPullRequest}</span>
              </a>
            ) : null}
            {/* Shrinks away before the reason does, so a narrow row cuts these first. */}
            <span className="flex min-w-0 shrink-[9999] items-center gap-1.5 overflow-hidden">
              {entry.author ? <span className="shrink-0">{entry.author}</span> : null}
              {showProject ? <span className="shrink-0 truncate">{entry.repository}</span> : null}
              {entry.milestone ? (
                <span className="shrink-0 truncate">{entry.milestone}</span>
              ) : null}
              {entry.labels.slice(0, 3).map((label) => (
                <PullRequestLabelChip key={label.name} label={label} />
              ))}
            </span>
          </>
        }
      />
      {threads.length > 0 ? (
        // The title keeps its room: a narrow row shows the threads as icons, a narrower one only
        // the first, with the rest behind the count.
        <span className="flex max-w-[40%] shrink-0 items-center gap-1">
          {threads.slice(0, 2).map((thread, index) => (
            // The layout sits on this one-track grid, which the button fills and shrinks with.
            <span
              key={`${thread.environmentId} ${thread.id}`}
              className={cn(
                "max-w-48 min-w-0 grid-cols-1",
                index === 0 ? "grid" : "hidden @sm/issue-row:grid",
              )}
            >
              <Button
                size="xs"
                variant="ghost"
                aria-label={thread.title}
                title={thread.title}
                onClick={() => onOpenThread(thread)}
              >
                <MessageSquareIcon />
                <span className="hidden truncate @lg/issue-row:block">{thread.title}</span>
              </Button>
            </span>
          ))}
          {threads.length > 1 ? (
            <Tooltip>
              <TooltipTrigger
                render={
                  <span
                    tabIndex={0}
                    className={cn(
                      "relative shrink-0 rounded-sm text-2xs text-muted-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
                      threads.length === 2 && "@sm/issue-row:hidden",
                    )}
                  />
                }
              >
                <span className="@sm/issue-row:hidden">+{threads.length - 1}</span>
                <span className="hidden @sm/issue-row:inline">+{threads.length - 2}</span>
              </TooltipTrigger>
              <TooltipPopup>{threads.map((thread) => thread.title).join(", ")}</TooltipPopup>
            </Tooltip>
          ) : null}
        </span>
      ) : null}
      {/* Outside the title block, so it holds one column whether or not threads sit before it. */}
      <span className="min-w-16 shrink-0 text-right text-2xs whitespace-nowrap text-muted-foreground tabular-nums">
        <RelativeTime timestamp={entry.updatedAt} />
      </span>
      <span
        className={cn(
          "relative flex opacity-0 group-hover/issue-row:opacity-100 has-[:focus-visible]:opacity-100",
          starting && "opacity-100",
        )}
      >
        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label={gt("Start a thread from #{number}", { number: String(entry.number) })}
                aria-busy={starting || undefined}
                disabled={startDisabledReason !== null || startBusy}
                onClick={() => void onStart(entry)}
              />
            }
          >
            {starting ? <Spinner aria-hidden /> : <MessageSquarePlusIcon />}
          </TooltipTrigger>
          <TooltipPopup>
            {startDisabledReason ??
              (starting
                ? gt("Starting a thread...")
                : startBusy
                  ? gt("Another thread is starting")
                  : gt("Start a thread from #{number}", { number: String(entry.number) }))}
          </TooltipPopup>
        </Tooltip>
      </span>
    </div>
  );
}, sameIssueRow);

function IssueTreeItem({
  node,
  depth,
  renderRow,
}: {
  node: IssueTreeNode;
  depth: number;
  renderRow: (entry: EnvironmentIssueEntry, showGroupLabel: boolean) => ReactNode;
}) {
  const gt = useGT();
  return (
    <li style={{ paddingLeft: depth === 0 ? undefined : `${Math.min(depth, 6) * 1.25}rem` }}>
      {node.entry !== null ? (
        renderRow(node.entry, true)
      ) : (
        // A sub-Issue the page does not hold: filtered out, not loaded, or in another repository.
        <a
          href={node.link.url}
          target="_blank"
          rel="noreferrer"
          className={cn(PULL_REQUEST_ROW_CLASS, "px-2 text-muted-foreground hover:bg-accent/50")}
        >
          <IssueStateGlyph state={node.link.state} className="opacity-70" />
          <span className="min-w-0 flex-1 truncate text-sm">
            <span className={PULL_REQUEST_ROW_NUMBER_CLASS}>
              {node.link.repository}#{node.link.number}
            </span>{" "}
            {node.link.title}
          </span>
          {node.outsideProjects ? (
            <span className="shrink-0 rounded-sm border border-border px-1 text-2xs">
              <T>Outside your projects</T>
            </span>
          ) : null}
          <ExternalLinkIcon aria-hidden className="size-3 shrink-0" />
        </a>
      )}
      {node.children.length > 0 || node.unlistedChildCount > 0 ? (
        <ul className="flex flex-col">
          {node.children.map((child) => (
            <IssueTreeItem key={child.key} node={child} depth={depth + 1} renderRow={renderRow} />
          ))}
          {node.unlistedChildCount > 0 ? (
            <li
              className="flex items-center gap-1 px-2 py-1 text-2xs text-muted-foreground"
              style={{ paddingLeft: `${Math.min(depth + 1, 6) * 1.25 + 0.5}rem` }}
            >
              <CornerDownRightIcon aria-hidden className="size-3" />
              {gt(
                "{count, plural, one {{count} more sub-Issues on GitHub} other {{count} more sub-Issues on GitHub}}",
                { count: node.unlistedChildCount },
              )}
            </li>
          ) : null}
        </ul>
      ) : null}
    </li>
  );
}
