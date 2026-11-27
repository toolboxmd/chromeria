import { useAtomValue } from "@effect/atom-react";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import {
  type EnvironmentId,
  type IssueKey,
  type GitHubIssueRef,
  type IssueStatesInput,
  ISSUE_WS_METHODS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { AsyncResult, Atom } from "effect/reactivity";
import { useEffect, useRef, useState } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import {
  issueCountsTotal,
  issueCountTargets,
  issueListingOf,
  issueListingScopes,
  issueListPages,
  mergeIssueLists,
  type IssueCountsTarget,
  type IssueListing,
  type IssueListingScope,
  type IssueListTarget,
} from "../components/issues/issueList.logic";
import {
  issueStateChunks,
  linkedIssueEntries,
  linkedIssueStates,
  linkedIssueStatesReadOf,
} from "../components/issues/issueLinks.logic";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { environmentPresentations, environmentSummaries } from "./presentation";
import { createMergedEnvironmentQuery, type EnvironmentQueryTarget } from "./pullRequests";
import type { GitHubIssueListInput } from "@t3tools/contracts";

/** Whether any connected server lists Issues; changes only when that answer does. */
const issuesSupportedAtom = Atom.make((get) =>
  get(environmentSummaries.environmentIdsAtom).some(
    (environmentId) =>
      get(environmentPresentations.presentationAtom(environmentId))?.serverConfig?.environment
        .capabilities.githubIssues === true,
  ),
).pipe(Atom.withLabel("web-issues:supported"));

export function useIssuesSupported() {
  return useAtomValue(issuesSupportedAtom);
}

const issueList = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:issues:list",
  tag: ISSUE_WS_METHODS.issuesList,
  staleTimeMs: 30_000,
});

export const issueDetail = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:issues:detail",
  tag: ISSUE_WS_METHODS.issuesDetail,
  staleTimeMs: 15_000,
});

/** Reads the side panel again after the Issue changed. */
const refreshDetail = (
  target: { readonly environmentId: EnvironmentId; readonly input: GitHubIssueRef },
  registry: { refresh: (atom: ReturnType<typeof issueDetail>) => void },
) =>
  Effect.sync(() =>
    registry.refresh(
      issueDetail({
        environmentId: target.environmentId,
        input: {
          host: target.input.host,
          repository: target.input.repository,
          number: target.input.number,
        },
      }),
    ),
  );

export const issueComment = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:issues:comment",
  tag: ISSUE_WS_METHODS.issuesComment,
  onSuccess: refreshDetail,
});

export const issueSetState = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:issues:set-state",
  tag: ISSUE_WS_METHODS.issuesSetState,
  onSuccess: refreshDetail,
});

/** One Issue read on demand: a row's "Start thread" needs the body the list does not carry. */
export const issueDetailRead = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:issues:detail-read",
  tag: ISSUE_WS_METHODS.issuesDetail,
});

const useIssueListsQuery = createMergedEnvironmentQuery("web-issues:list", issueList);

/** Each listing with the continuation pages it leads to, read as soon as a page names one. */
const issueListPagesAtom = Atom.family((key: string) =>
  Atom.make((get) =>
    issueListPages(JSON.parse(key) as ReadonlyArray<IssueListTarget>, (target) =>
      AsyncResult.getOrElse(get(issueList(target)), () => null),
    ),
  ).pipe(Atom.withLabel(`web-issues:list-pages:${key}`)),
);

/** What each listing says of its server's repositories, and whether that server answers. */
const issueListingsAtom = Atom.family((key: string) =>
  Atom.make((get) =>
    (JSON.parse(key) as ReadonlyArray<IssueListTarget>).map((target) => {
      const result = get(issueList(target));
      return issueListingOf({
        environmentId: target.environmentId,
        connected:
          get(environmentPresentations.presentationAtom(target.environmentId))?.connection.phase ===
          "connected",
        answer: AsyncResult.getOrElse(result, () => null),
        failed: result._tag === "Failure",
      });
    }),
  ).pipe(Atom.withLabel(`web-issues:listings:${key}`)),
);

/**
 * One listing per environment with every further page, merged into the list the page renders.
 * `rows` is the page walk's verdict; a page whose reread failed still counts as answered there,
 * so a reader checks `error` too.
 */
export function useIssueList(
  listings: ReadonlyArray<EnvironmentQueryTarget<GitHubIssueListInput>>,
) {
  const key = JSON.stringify(listings);
  const pages = useAtomValue(issueListPagesAtom(key));
  const servers = useAtomValue(issueListingsAtom(key));
  const query = useIssueListsQuery(pages.targets);
  const data = mergeIssueLists(query.values);
  return {
    data,
    rows: pages.rows,
    listings: servers,
    error: query.error?.message ?? null,
    isPending: query.isPending,
    refresh: query.refresh,
  };
}

const issueCounts = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:issues:counts",
  tag: ISSUE_WS_METHODS.issuesCounts,
  staleTimeMs: 30_000,
});

const useIssueCountsQuery = createMergedEnvironmentQuery("web-issues:counts", issueCounts);

const NO_COUNTS_TARGETS: ReadonlyArray<IssueCountsTarget> = [];
const NO_SCOPES: ReadonlyArray<IssueListingScope> = [];

/**
 * The open total and the week's filed and closed of the repositories the listings hold, read
 * apart from the list: when the page opens and on `refresh`. `totals` is null while unknown.
 */
export function useIssueCounts(listings: ReadonlyArray<IssueListing>) {
  // A listing read again for a new sort, search, filter or State says nothing until it answers.
  // Each server's scope is held meanwhile, so none of those changes starts a counts read or
  // blanks numbers already known; a scope that does change replaces the one held.
  const [held, setHeld] = useState(NO_SCOPES);
  const scopes = issueListingScopes(listings, held);
  if (JSON.stringify(scopes) !== JSON.stringify(held)) setHeld(scopes);
  const targets = issueCountTargets(scopes);
  const query = useIssueCountsQuery(targets ?? NO_COUNTS_TARGETS);
  return {
    totals: issueCountsTotal({
      targets,
      answers: query.values,
      failed: query.error !== null,
      reading: query.isPending,
    }),
    isPending: query.isPending,
    refresh: query.refresh,
  };
}

export function useIssueDetail(environmentId: EnvironmentId, ref: GitHubIssueRef) {
  return useAtomValue(issueDetail({ environmentId, input: ref }));
}

/** How often an open links panel rereads its Issues' states when no pull request sync prompts it. */
const LINKED_ISSUE_STATES_REFRESH_MS = 60_000;

const issueStates = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "environment-data:issues:states",
  tag: ISSUE_WS_METHODS.issuesStates,
  staleTimeMs: 15_000,
  refreshIntervalMs: LINKED_ISSUE_STATES_REFRESH_MS,
});

/** Every chunk's states query for one panel, read together. */
const linkedIssueStatesReads = Atom.family((key: string) => {
  const targets = JSON.parse(key) as ReadonlyArray<EnvironmentQueryTarget<IssueStatesInput>>;
  return Atom.make((get) => targets.map((target) => get(issueStates(target)))).pipe(
    Atom.withLabel(`environment-data:issues:states-chunks:${key}`),
  );
});

/**
 * The current GitHub state of a thread's linked Issues, and each returned Issue's title, author
 * and update time, read through the thread's server: on
 * mount, every `LINKED_ISSUE_STATES_REFRESH_MS` while shown, and whenever `syncKey` changes,
 * e.g. on each sync of the thread's pull requests.
 */
export function useLinkedIssueStates(
  environmentId: EnvironmentId | null,
  links: ReadonlyArray<IssueKey>,
  syncKey: string | null,
) {
  const key = JSON.stringify(
    environmentId === null
      ? []
      : issueStateChunks(links).map((chunk) => ({
          environmentId,
          input: {
            issues: chunk.map(({ host, repository, number }) => ({ host, repository, number })),
          },
        })),
  );
  const results = useAtomValue(linkedIssueStatesReads(key));
  const lastSyncKey = useRef(syncKey);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Invalidate registered issue-state RPC reads when persisted link state changes.
  useEffect(() => {
    if (lastSyncKey.current === syncKey) return;
    lastSyncKey.current = syncKey;
    const targets = JSON.parse(key) as ReadonlyArray<EnvironmentQueryTarget<IssueStatesInput>>;
    for (const target of targets) appAtomRegistry.refresh(issueStates(target));
  }, [syncKey, key]);
  return (() => {
    const reads = results.map(linkedIssueStatesReadOf);
    return { states: linkedIssueStates(links, reads), entries: linkedIssueEntries(reads) };
  })();
}
