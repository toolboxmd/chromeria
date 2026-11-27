import type {
  EnvironmentId,
  IssueLink,
  GitHubIssueCountsInput,
  GitHubIssueCountsResult,
  GitHubIssueListCursors,
  GitHubIssueListEntry,
  GitHubIssueListInput,
  IssueListRepository,
  GitHubIssueListResult,
  GitHubIssueListSort,
  GitHubIssueListState,
  IssuePullRequest,
} from "@t3tools/contracts";

export interface EnvironmentIssueEntry extends GitHubIssueListEntry {
  /** The server that read the row, which is also the one its actions go to. */
  readonly environmentId: EnvironmentId;
}

export interface EnvironmentIssueRepository extends IssueListRepository {
  readonly environmentId: EnvironmentId;
}

export interface MergedIssueList {
  readonly entries: ReadonlyArray<EnvironmentIssueEntry>;
  readonly repositories: ReadonlyArray<EnvironmentIssueRepository>;
  /** Repositories on other forges, once each however many servers or worktrees hold them. */
  readonly unsupported: ReadonlyArray<{ readonly host: string; readonly repositoryCount: number }>;
  readonly errors: ReadonlyArray<{
    readonly environmentId: EnvironmentId;
    readonly host: string;
    readonly message: string;
  }>;
  /** The newest continuation per server; absent once a server has nothing more. */
  readonly nextCursors: ReadonlyMap<EnvironmentId, GitHubIssueListCursors>;
  /** Thread-linked pull requests any server read, by `issueKey`. */
  readonly linkedPullRequests: ReadonlyMap<string, IssuePullRequest>;
  /**
   * Per `issueKey`, the lowercase logins of every server that listed the Issue: a review mark
   * any of them posted is trusted.
   */
  readonly trustedLogins: ReadonlyMap<string, ReadonlySet<string>>;
}

export interface IssueListFilters {
  /** A `repositoryKey`. */
  readonly repository?: string | undefined;
  /** Every one must be on the Issue. */
  readonly labels?: ReadonlyArray<string> | undefined;
  readonly milestone?: string | undefined;
  /** `"none"` for top-level Issues, or the `issueKey` of a parent for its children. */
  readonly parent?: string | undefined;
}

/**
 * Servers that advertise a fork capability, sorted. Upstream and older servers answer `issues.*`
 * with an unknown-tag error, so the Issues list and its entry points ask only these.
 */
export function environmentIdsWithCapability(
  environments: ReadonlyArray<{
    readonly environmentId: EnvironmentId;
    readonly serverConfig: {
      readonly environment: {
        readonly capabilities: { readonly issues?: boolean; readonly issueLinks?: boolean };
      };
    } | null;
  }>,
  capability: "issues" | "issueLinks",
): ReadonlyArray<EnvironmentId> {
  return environments
    .filter(
      (environment) => environment.serverConfig?.environment.capabilities[capability] === true,
    )
    .map((environment) => environment.environmentId)
    .toSorted((left, right) => left.localeCompare(right));
}

export function repositoryKey(host: string, repository: string): string {
  return `${host.toLowerCase()} ${repository.toLowerCase()}`;
}

export function issueKey(issue: Pick<IssueLink, "host" | "repository" | "number">): string {
  return `${repositoryKey(issue.host, issue.repository)}#${issue.number}`;
}

export interface IssueListTarget {
  readonly environmentId: EnvironmentId;
  readonly input: GitHubIssueListInput;
}

/**
 * Every page to read: each server's listing, then each answered page's continuation. Group counts
 * are totals only when `rows` is `complete`: `pending` while a page has no answer, `truncated`
 * when a server's pages hold fewer rows than GitHub matched, `unknown` when a server gives no match counts.
 */
export function issueListPages(
  listings: ReadonlyArray<IssueListTarget>,
  answerOf: (
    target: IssueListTarget,
  ) => Pick<GitHubIssueListResult, "entries" | "nextCursors" | "matchCounts"> | null,
): {
  readonly targets: ReadonlyArray<IssueListTarget>;
  readonly rows: "complete" | "pending" | "truncated" | "unknown";
} {
  const targets: Array<IssueListTarget> = [];
  /** Per server and search: the cursors already read from. */
  const read = new Map<string, Set<string>>();
  /** Per server: the raw rows its pages returned, and GitHub's latest match count per search. */
  const returned = new Map<EnvironmentId, { rows: number; matches: Map<string, number> }>();
  let pending = false;
  let truncated = false;
  let unknown = false;
  // Page by page across servers, so a page that arrives later adds its rows after the ones shown.
  let round = listings;
  while (round.length > 0) {
    const next: Array<IssueListTarget> = [];
    for (const target of round) {
      targets.push(target);
      const answer = answerOf(target);
      if (answer === null) {
        pending = true;
        continue;
      }
      const server = returned.get(target.environmentId) ?? { rows: 0, matches: new Map() };
      returned.set(target.environmentId, server);
      server.rows += answer.entries.length;
      if (answer.matchCounts === undefined) unknown = true;
      for (const [search, count] of Object.entries(answer.matchCounts ?? {})) {
        server.matches.set(search, count);
      }
      const cursors: Record<string, string> = {};
      for (const [search, cursor] of Object.entries(answer.nextCursors)) {
        const key = `${target.environmentId} ${search}`;
        const seen = read.get(key) ?? new Set<string>();
        read.set(key, seen);
        // A search answering with a cursor it was read from would be read forever.
        if (seen.has(cursor)) {
          truncated = true;
          continue;
        }
        seen.add(cursor);
        cursors[search] = cursor;
      }
      if (Object.keys(cursors).length > 0) {
        next.push({ environmentId: target.environmentId, input: { ...target.input, cursors } });
      }
    }
    round = next;
  }
  // A search never returns more rows than it matches, so a server whose rows fall short of its
  // searches' matches is missing rows of one of them, however many others it read in full.
  for (const { rows, matches } of returned.values()) {
    if (rows < [...matches.values()].reduce((sum, count) => sum + count, 0)) truncated = true;
  }
  return {
    targets,
    rows: pending ? "pending" : truncated ? "truncated" : unknown ? "unknown" : "complete",
  };
}

/**
 * The answers of every server and every page already loaded, in the order they were asked. Two
 * servers with one repository return the same Issue; the first answer keeps it. A later page of a
 * server replaces its continuation, which is how the page walk moves forward.
 */
export function mergeIssueLists(
  values: ReadonlyArray<readonly [EnvironmentId, GitHubIssueListResult]>,
): MergedIssueList | null {
  if (values.length === 0) return null;
  const entries = new Map<string, EnvironmentIssueEntry>();
  const repositories = new Map<string, EnvironmentIssueRepository>();
  const unsupported = new Map<string, number>();
  const unsupportedSeen = new Set<string>();
  const errors: Array<MergedIssueList["errors"][number]> = [];
  const nextCursors = new Map<EnvironmentId, GitHubIssueListCursors>();
  const linkedPullRequests = new Map<string, IssuePullRequest>();
  const trustedLogins = new Map<string, Set<string>>();
  for (const [environmentId, result] of values) {
    const viewers = new Map(
      result.viewers.map(({ host, login }) => [host.toLowerCase(), login.toLowerCase()]),
    );
    for (const entry of result.entries) {
      const key = issueKey(entry);
      if (!entries.has(key)) entries.set(key, { ...entry, environmentId });
      const login = viewers.get(entry.host.toLowerCase());
      if (login === undefined || login.length === 0) continue;
      const logins = trustedLogins.get(key) ?? new Set<string>();
      logins.add(login);
      trustedLogins.set(key, logins);
    }
    for (const pullRequest of result.linkedPullRequests) {
      const key = issueKey(pullRequest);
      if (!linkedPullRequests.has(key)) linkedPullRequests.set(key, pullRequest);
    }
    for (const repository of result.repositories) {
      const key = repositoryKey(repository.host, repository.repository);
      if (!repositories.has(key)) repositories.set(key, { ...repository, environmentId });
    }
    // Keyed like the GitHub repositories: continuation pages and other servers repeat them.
    for (const { host, repository } of result.unsupported) {
      const key = repositoryKey(host, repository);
      if (unsupportedSeen.has(key)) continue;
      unsupportedSeen.add(key);
      unsupported.set(host.toLowerCase(), (unsupported.get(host.toLowerCase()) ?? 0) + 1);
    }
    for (const error of result.errors) errors.push({ environmentId, ...error });
    if (Object.keys(result.nextCursors).length > 0) {
      nextCursors.set(environmentId, result.nextCursors);
    } else {
      nextCursors.delete(environmentId);
    }
  }
  return {
    entries: [...entries.values()],
    repositories: [...repositories.values()],
    unsupported: [...unsupported].map(([host, repositoryCount]) => ({ host, repositoryCount })),
    errors,
    nextCursors,
    linkedPullRequests,
    trustedLogins,
  };
}

export interface IssueCountsTarget {
  readonly environmentId: EnvironmentId;
  readonly input: GitHubIssueCountsInput;
}

/** What a server's listing says now of the repositories it holds. */
export interface IssueListing {
  readonly environmentId: EnvironmentId;
  /** `failed`: connected, and its latest read failed or GitHub refused a search in it. */
  readonly state: "answered" | "pending" | "failed" | "disconnected";
  /** Repository keys of its latest answer, sorted, kept through a failed reread; null for none. */
  readonly repositories: ReadonlyArray<string> | null;
}

export function issueListingOf(input: {
  readonly environmentId: EnvironmentId;
  readonly connected: boolean;
  /** The listing's latest answer, also the one a failed reread left behind; null for none. */
  readonly answer: Pick<GitHubIssueListResult, "repositories" | "errors"> | null;
  readonly failed: boolean;
}): IssueListing {
  return {
    environmentId: input.environmentId,
    // A server that dropped its connection keeps its reads waiting with their last answer, so
    // the read alone would still look answered.
    state: !input.connected
      ? "disconnected"
      : // An answer carrying GitHub's refusal still says what the server lists, but a server
        // that cannot search cannot count.
        input.failed || (input.answer?.errors.length ?? 0) > 0
        ? "failed"
        : input.answer === null
          ? "pending"
          : "answered",
    repositories:
      input.answer?.repositories
        .map((repository) => repositoryKey(repository.host, repository.repository))
        .toSorted() ?? null,
  };
}

/** The repositories a listing server holds, null while unknown, and whether it can be asked. */
export interface IssueListingScope {
  readonly environmentId: EnvironmentId;
  readonly repositories: ReadonlyArray<string> | null;
  readonly answering: boolean;
}

/**
 * The scope of each server the counts must cover: what its listing says now, and `held`, the
 * scopes as last worked out, where it says nothing. A listing read again keeps its server's scope
 * until it answers, and a server that stopped answering keeps the repositories it last listed.
 * A server not connected that has listed nothing adds no rows to the view, and has no scope here.
 */
export function issueListingScopes(
  listings: ReadonlyArray<IssueListing>,
  held: ReadonlyArray<IssueListingScope>,
): ReadonlyArray<IssueListingScope> {
  return listings.flatMap(({ environmentId, state, repositories }) => {
    const last = held.find((scope) => scope.environmentId === environmentId);
    if (state === "answered") return [{ environmentId, repositories, answering: true }];
    if (state === "pending") {
      return [last ?? { environmentId, repositories: null, answering: false }];
    }
    const listed = repositories ?? last?.repositories ?? null;
    return listed === null && state === "disconnected"
      ? []
      : [{ environmentId, repositories: listed, answering: false }];
  });
}

/**
 * The counts read each server is asked for: every repository goes to the first answering server
 * that lists it, so none is counted twice and no answer has to be split. Null when the counts
 * cannot cover the view: a server's scope is unknown, or no answering server lists a repository.
 */
export function issueCountTargets(
  scopes: ReadonlyArray<IssueListingScope>,
): ReadonlyArray<IssueCountsTarget> | null {
  // Sorted, so the same scopes always make the same reads however the listings arrived.
  const servers = scopes.toSorted((left, right) =>
    left.environmentId.localeCompare(right.environmentId),
  );
  const byServer = new Map<EnvironmentId, Array<string>>();
  const owned = new Set<string>();
  for (const { environmentId, repositories, answering } of servers) {
    if (repositories === null) return null;
    if (!answering) continue;
    for (const repository of repositories) {
      if (owned.has(repository)) continue;
      owned.add(repository);
      byServer.set(environmentId, [...(byServer.get(environmentId) ?? []), repository]);
    }
  }
  if (servers.some(({ repositories }) => repositories!.some((key) => !owned.has(key)))) {
    return null;
  }
  return [...byServer].map(([environmentId, repositories]) => ({
    environmentId,
    input: { repositories: repositories.toSorted() },
  }));
}

/** The totals of the listed repositories; not `exact` while a read of them is in flight. */
export interface IssueCountsTotal extends GitHubIssueCountsResult {
  readonly exact: boolean;
}

/**
 * The totals over every server's counts read, or null for unknown: the reads cannot cover the
 * view (`targets` is null), one failed, or one has no answer yet. Never a sum over some servers.
 * With every answer in hand and a read in flight, they are the answers being replaced.
 */
export function issueCountsTotal(input: {
  readonly targets: ReadonlyArray<IssueCountsTarget> | null;
  readonly answers: ReadonlyArray<readonly [EnvironmentId, GitHubIssueCountsResult]>;
  readonly failed: boolean;
  readonly reading: boolean;
}): IssueCountsTotal | null {
  if (input.targets === null || input.failed) return null;
  const byServer = new Map(input.answers);
  const totals = { open: 0, filed: 0, closed: 0 };
  for (const { environmentId } of input.targets) {
    const answer = byServer.get(environmentId);
    if (answer === undefined) return null;
    totals.open += answer.open;
    totals.filed += answer.filed;
    totals.closed += answer.closed;
  }
  return { ...totals, exact: !input.reading };
}

/** A count with how far it can be trusted; see `issueCountCertainty`. */
export interface MarkedIssueCount {
  readonly count: number;
  readonly certainty: "final" | "floor" | "unsettled";
}

/**
 * How the line above the list states the rows shown against the open total. Rows that are every
 * open Issue (State Open, nothing searched on GitHub, every page read) count themselves; else the
 * counts read does. `listed` rows claim no total and carry it apart in `open`, null for unknown.
 */
export function issueNumbersLine(input: {
  readonly state: GitHubIssueListState;
  /** Text, labels, a milestone or a repository narrowed the search on GitHub. */
  readonly searched: boolean;
  readonly shown: MarkedIssueCount;
  /** The open Issues among the loaded rows. */
  readonly loadedOpen: number;
  /** `refreshing`: every page is read, and some are waiting on a read of them again. */
  readonly rows: "complete" | "refreshing" | "incomplete";
  /** The open total of the counts read, unsettled while it is read again; null for unknown. */
  readonly counted: MarkedIssueCount | null;
}):
  | { readonly kind: "open"; readonly count: MarkedIssueCount }
  | { readonly kind: "subset"; readonly shown: MarkedIssueCount; readonly total: MarkedIssueCount }
  | {
      readonly kind: "listed";
      readonly count: MarkedIssueCount;
      readonly open: MarkedIssueCount | null;
    } {
  const everyOpenIssue = input.state === "open" && !input.searched;
  // The loaded open rows are a floor of the open total: a counted total below them was read
  // before Issues were opened, and is unknown until it is read again.
  const counted =
    input.counted !== null && input.counted.count >= input.loadedOpen ? input.counted : null;
  // Rows read to their end are the total. Rows still arriving are its floor when nothing was
  // counted. Rows being read again say nothing of it alone: with the counts read again too, as
  // on Refresh, they carry its mark, and a counts read that left no answer leaves it unknown.
  const fromRows =
    everyOpenIssue &&
    (input.rows === "complete" ||
      (input.rows === "incomplete" && counted === null) ||
      (input.rows === "refreshing" && counted?.certainty === "unsettled"));
  const total: MarkedIssueCount | null = fromRows
    ? {
        count: input.loadedOpen,
        certainty:
          input.rows === "complete" ? "final" : input.rows === "refreshing" ? "unsettled" : "floor",
      }
    : counted;
  if (input.state !== "open" || total === null) {
    return { kind: "listed", count: input.shown, open: total };
  }
  // No filter narrows the rows, and they are as many as the total: the rows are the total.
  if (
    everyOpenIssue &&
    input.shown.count === input.loadedOpen &&
    total.count === input.loadedOpen
  ) {
    return {
      kind: "open",
      count: fromRows && input.shown.certainty !== "final" ? input.shown : total,
    };
  }
  return { kind: "subset", shown: input.shown, total };
}

const STALE_AFTER_MS = 30 * 24 * 60 * 60 * 1000;

/** Open Issues among the rows that nobody updated for 30 days. */
export function staleIssueCount(
  entries: ReadonlyArray<Pick<GitHubIssueListEntry, "state" | "updatedAt">>,
  now: number,
): number {
  return entries.filter(
    (entry) => entry.state === "open" && now - Date.parse(entry.updatedAt) >= STALE_AFTER_MS,
  ).length;
}

/**
 * How far a count over the loaded rows can be trusted. Rows only arrive, so a count of rows is a
 * `floor` until every page is read. A count that depends on which group a row is in is
 * `unsettled` until every page and every linked-thread read has answered: a later page can carry
 * the pull request, and a thread read the thread, that moves a row already shown.
 */
export function issueCountCertainty(input: {
  /** Every page of every search is read, with no error. */
  readonly rowsComplete: boolean;
  /** Every linked-thread read answered. */
  readonly threadsRead: boolean;
  /** The count depends on rows' groups or links: a group's count, or a total under such a filter. */
  readonly byGroup: boolean;
}): "final" | "floor" | "unsettled" {
  if (input.byGroup) return input.rowsComplete && input.threadsRead ? "final" : "unsettled";
  return input.rowsComplete ? "final" : "floor";
}

export function matchesIssueFilters(
  entry: GitHubIssueListEntry,
  filters: IssueListFilters,
): boolean {
  if (
    filters.repository !== undefined &&
    repositoryKey(entry.host, entry.repository) !== filters.repository
  ) {
    return false;
  }
  if (filters.labels !== undefined && filters.labels.length > 0) {
    const names = new Set(entry.labels.map((label) => label.name.toLowerCase()));
    if (!filters.labels.every((label) => names.has(label.toLowerCase()))) return false;
  }
  if (
    filters.milestone !== undefined &&
    entry.milestone?.toLowerCase() !== filters.milestone.toLowerCase()
  ) {
    return false;
  }
  if (filters.parent === "none") return entry.parent === null;
  if (filters.parent !== undefined) {
    return entry.parent !== null && issueKey(entry.parent) === filters.parent;
  }
  return true;
}

/** Newest first for every key; ties fall back to repository and number so the order is stable. */
export function sortIssues<Entry extends GitHubIssueListEntry>(
  entries: ReadonlyArray<Entry>,
  sort: GitHubIssueListSort,
): ReadonlyArray<Entry> {
  const time = (entry: Entry) => Date.parse(sort === "updated" ? entry.updatedAt : entry.createdAt);
  return entries.toSorted((left, right) => {
    const primary =
      sort === "number" ? right.number - left.number : (time(right) || 0) - (time(left) || 0);
    if (primary !== 0) return primary;
    const repository = left.repository.localeCompare(right.repository);
    return repository !== 0 ? repository : right.number - left.number;
  });
}

export interface IssueFacets {
  /** Each with how many loaded Issues carry it. */
  readonly labels: ReadonlyArray<{
    readonly name: string;
    readonly color: string;
    readonly count: number;
  }>;
  readonly milestones: ReadonlyArray<string>;
  /** Issues that are the parent of at least one loaded Issue. */
  readonly parents: ReadonlyArray<IssueLink>;
}

/** What the filter menus offer: only values the loaded rows actually carry, most used first. */
export function collectIssueFacets(entries: ReadonlyArray<GitHubIssueListEntry>): IssueFacets {
  const labels = new Map<string, { name: string; color: string; count: number }>();
  const milestones = new Map<string, number>();
  const parents = new Map<string, IssueLink>();
  for (const entry of entries) {
    for (const label of entry.labels) {
      const key = label.name.toLowerCase();
      const held = labels.get(key);
      if (held === undefined) labels.set(key, { ...label, count: 1 });
      else held.count += 1;
    }
    if (entry.milestone !== null) {
      milestones.set(entry.milestone, (milestones.get(entry.milestone) ?? 0) + 1);
    }
    if (entry.parent !== null) parents.set(issueKey(entry.parent), entry.parent);
  }
  return {
    labels: [...labels.values()].toSorted(
      (left, right) => right.count - left.count || left.name.localeCompare(right.name),
    ),
    milestones: [...milestones]
      .toSorted((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
      .map(([name]) => name),
    parents: [...parents.values()].toSorted(
      (left, right) =>
        left.repository.localeCompare(right.repository) || right.number - left.number,
    ),
  };
}

export interface IssueTreeNode {
  readonly key: string;
  /** The loaded row, or null for a sub-Issue the list does not hold. */
  readonly entry: EnvironmentIssueEntry | null;
  readonly link: IssueLink;
  /** Its repository belongs to none of the user's projects. */
  readonly outsideProjects: boolean;
  readonly children: ReadonlyArray<IssueTreeNode>;
  /** Sub-Issues GitHub has beyond the ones it sent with the row. */
  readonly unlistedChildCount: number;
}

/**
 * The parent tree of the rows on the page. An Issue whose parent is on the page sits under it;
 * every other Issue is a root, in the order given. Each parent also shows the sub-Issues the page
 * does not hold, including ones in other repositories, as link-only nodes marked when their
 * repository is outside the user's projects.
 */
export function buildIssueTree(
  entries: ReadonlyArray<EnvironmentIssueEntry>,
  projectRepositories: ReadonlySet<string>,
): ReadonlyArray<IssueTreeNode> {
  const byKey = new Map(entries.map((entry) => [issueKey(entry), entry] as const));
  const childrenOf = new Map<string, Array<string>>();
  for (const entry of entries) {
    if (entry.parent === null) continue;
    const parentKey = issueKey(entry.parent);
    if (!byKey.has(parentKey)) continue;
    const children = childrenOf.get(parentKey) ?? [];
    children.push(issueKey(entry));
    childrenOf.set(parentKey, children);
  }
  const outside = (link: IssueLink) =>
    !projectRepositories.has(repositoryKey(link.host, link.repository));

  const build = (link: IssueLink, visiting: ReadonlySet<string>): IssueTreeNode => {
    const key = issueKey(link);
    const entry = byKey.get(key) ?? null;
    const path = new Set(visiting).add(key);
    const childKeys: Array<string> = [];
    const childLinks = new Map<string, IssueLink>();
    for (const child of entry?.subIssues ?? []) {
      const childKey = issueKey(child);
      if (childLinks.has(childKey)) continue;
      childKeys.push(childKey);
      childLinks.set(childKey, child);
    }
    // A loaded child GitHub left off the parent's first page still belongs under it.
    for (const childKey of childrenOf.get(key) ?? []) {
      if (childLinks.has(childKey)) continue;
      childKeys.push(childKey);
      childLinks.set(childKey, byKey.get(childKey)!);
    }
    const children = childKeys
      .filter((childKey) => !path.has(childKey))
      .map((childKey) => build(byKey.get(childKey) ?? childLinks.get(childKey)!, path));
    return {
      key,
      entry,
      link: entry ?? link,
      outsideProjects: outside(link),
      children,
      unlistedChildCount: Math.max(0, (entry?.subIssueCount ?? 0) - (entry?.subIssues.length ?? 0)),
    };
  };

  return entries
    .filter((entry) => entry.parent === null || !byKey.has(issueKey(entry.parent)))
    .map((entry) => build(entry, new Set()));
}
