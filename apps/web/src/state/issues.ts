import { useAtomValue } from "@effect/atom-react";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import {
  type EnvironmentId,
  type IssueKey,
  type IssueRef,
  type IssueStatesInput,
  ISSUE_WS_METHODS,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Atom } from "effect/unstable/reactivity";
import { useEffect, useMemo, useRef } from "react";

import { connectionAtomRuntime } from "../connection/runtime";
import { mergeIssueLists } from "../components/issues/issueList.logic";
import {
  issueStateChunks,
  linkedIssueEntries,
  linkedIssueStates,
  linkedIssueStatesReadOf,
} from "../components/issues/issueLinks.logic";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { createMergedEnvironmentQuery, type EnvironmentQueryTarget } from "./pullRequests";
import type { IssueListInput } from "@t3tools/contracts";

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
  target: { readonly environmentId: EnvironmentId; readonly input: IssueRef },
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

/** One listing per environment, merged into the list the page renders. */
export function useIssueList(targets: ReadonlyArray<EnvironmentQueryTarget<IssueListInput>>) {
  const query = useIssueListsQuery(targets);
  const data = useMemo(() => mergeIssueLists(query.values), [query.values]);
  return { data, error: query.error, isPending: query.isPending, refresh: query.refresh };
}

export function useIssueDetail(environmentId: EnvironmentId, ref: IssueRef) {
  return useAtomValue(issueDetail({ environmentId, input: ref }));
}

/** How often an open links panel rereads its Issues' states when no pull request sync prompts it. */
export const LINKED_ISSUE_STATES_REFRESH_MS = 60_000;

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
  useEffect(() => {
    if (lastSyncKey.current === syncKey) return;
    lastSyncKey.current = syncKey;
    const targets = JSON.parse(key) as ReadonlyArray<EnvironmentQueryTarget<IssueStatesInput>>;
    for (const target of targets) appAtomRegistry.refresh(issueStates(target));
  }, [syncKey, key]);
  return useMemo(() => {
    const reads = results.map(linkedIssueStatesReadOf);
    return { states: linkedIssueStates(links, reads), entries: linkedIssueEntries(reads) };
  }, [links, results]);
}
