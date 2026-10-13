import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import type {
  FeatureMapPart,
  FeatureMapRef,
  FeatureMapRefFact,
  FeatureMapRefLineProblem,
  FeatureMapSnapshot,
} from "@t3tools/contracts";

import { issueKey } from "../issues/issueList.logic";
import type { IssueRowThread } from "../issues/issueStatus.logic";

type MapSnapshot = Extract<FeatureMapSnapshot, { _tag: "Map" }>;
type Fact = NonNullable<FeatureMapRefFact["fact"]>;

/** The four states GitHub can prove. */
export type FeatureMapState = "planned" | "working" | "broken" | "done";
/** Shown in place of a state when the primary ref cannot decide one. */
export type FeatureMapNote =
  | "cancelled"
  | "unavailable"
  | "unknown"
  | "incomplete"
  | "untracked"
  | "context";

export interface FeatureMapPartView {
  readonly part: FeatureMapPart;
  readonly state: FeatureMapState | null;
  readonly note: FeatureMapNote | null;
  /** Where clicking the part goes: the primary ref, as GitHub named it when it answered. */
  readonly url: string | null;
  /** Linked threads working on the part now, for its icon. */
  readonly threads: ReadonlyArray<IssueRowThread>;
}

export interface FeatureMapThreads {
  /** Linked threads by `issueKey`. */
  readonly threadsByIssue: ReadonlyMap<string, ReadonlyArray<IssueRowThread>>;
  /** Scoped keys of threads working now. */
  readonly working: ReadonlySet<string>;
}

const refKey = (ref: { readonly repository: string; readonly number: number }) =>
  `${ref.repository.toLowerCase()}#${ref.number}`;

/** Each ref's fact; absent means not read yet, null that GitHub answered nothing. */
function factsByRef(map: MapSnapshot): ReadonlyMap<string, Fact | null> {
  return new Map(map.facts.map((entry) => [refKey(entry), entry.fact]));
}

/** An Issue written as a ref, or a ref GitHub answered as an Issue. */
const isIssueRef = (ref: FeatureMapRef, fact: Fact | null | undefined) =>
  fact ? fact.kind === "issue" : ref.kind === "issue";

/**
 * Whether the part is broken: proven by the evidence read, or possible because an open Issue's
 * labels or closing pull requests ran past the page GitHub returned.
 */
function brokenBy(
  part: FeatureMapPart,
  facts: ReadonlyMap<string, Fact | null>,
): "broken" | "possibly" | "no" {
  let possibly = false;
  const proven = part.refs.some((ref, index) => {
    const fact = facts.get(refKey(ref));
    if (!fact) return false;
    if (fact.kind === "pull") return fact.state === "open" && fact.checks === "failing";
    if (fact.state !== "open") return false;
    if (!fact.complete) possibly = true;
    if (fact.labels.some((label) => label.toLowerCase() === "bug")) return true;
    // Only the primary Issue's closing pull requests are the part's.
    return (
      index === 0 &&
      fact.openClosingPullRequests.some((pullRequest) => pullRequest.checks === "failing")
    );
  });
  return proven ? "broken" : possibly ? "possibly" : "no";
}

function workingThreadsOf(
  part: FeatureMapPart,
  facts: ReadonlyMap<string, Fact | null>,
  host: string,
  threads: FeatureMapThreads,
  /** By Issue `refKey`: the primary pull requests of the parts that name that Issue. */
  siblingPrimaryPullRequests: ReadonlyMap<string, ReadonlySet<string>>,
): ReadonlyArray<IssueRowThread> {
  const primary = part.refs[0];
  if (primary === undefined) return [];
  const isWorking = (thread: IssueRowThread) =>
    threads.working.has(scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)));
  const linkedTo = (ref: FeatureMapRef) =>
    threads.threadsByIssue.get(
      issueKey({ host, repository: ref.repository, number: ref.number }),
    ) ?? [];
  if (isIssueRef(primary, facts.get(refKey(primary)))) {
    // A thread on this Issue that carries the primary pull request of another part naming the same
    // Issue works on that part; any other pull request it carries says nothing about this one.
    const siblings = siblingPrimaryPullRequests.get(refKey(primary));
    return linkedTo(primary).filter(
      (thread) =>
        isWorking(thread) &&
        !thread.pullRequests.some((pullRequest) => siblings?.has(refKey(pullRequest)) === true),
    );
  }
  // A pull request primary: threads on the part's Issues that carry that pull request, so a
  // thread on a sibling part's pull request does not light this one.
  const seen = new Set<string>();
  return part.refs
    .filter((ref) => isIssueRef(ref, facts.get(refKey(ref))))
    .flatMap(linkedTo)
    .filter((thread) => {
      const key = scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id));
      if (seen.has(key) || !isWorking(thread)) return false;
      seen.add(key);
      return thread.pullRequests.some((pullRequest) => refKey(pullRequest) === refKey(primary));
    });
}

/**
 * Each part's state. The primary ref decides planned, working or done; broken overrides it when
 * an open Issue among the refs is labelled `bug` or an open pull request of the part fails its
 * checks. Missing evidence is shown as such, never as a known state.
 */
export function foldFeatureMap(
  map: MapSnapshot,
  threads: FeatureMapThreads,
): ReadonlyArray<FeatureMapPartView> {
  const facts = factsByRef(map);
  const siblingPrimaryPullRequests = new Map<string, Set<string>>();
  for (const part of map.parts) {
    const primary = part.refs[0];
    if (primary === undefined || isIssueRef(primary, facts.get(refKey(primary)))) continue;
    for (const ref of part.refs.slice(1)) {
      if (!isIssueRef(ref, facts.get(refKey(ref)))) continue;
      const pullRequests = siblingPrimaryPullRequests.get(refKey(ref)) ?? new Set();
      pullRequests.add(refKey(primary));
      siblingPrimaryPullRequests.set(refKey(ref), pullRequests);
    }
  }
  return map.parts.map((part): FeatureMapPartView => {
    const primary = part.refs[0];
    const working = workingThreadsOf(part, facts, map.host, threads, siblingPrimaryPullRequests);
    const fact = primary === undefined ? undefined : facts.get(refKey(primary));
    // An unreadable first token leaves no primary: the next ref must not decide or open it.
    const url = part.primaryUnreadable ? null : (fact?.url ?? primary?.url ?? null);
    if (part.context) return { part, state: null, note: "context", url, threads: working };
    if (primary === undefined)
      return { part, state: null, note: "untracked", url, threads: working };
    const base = ((): { state: FeatureMapState | null; note: FeatureMapNote | null } => {
      if (part.primaryUnreadable || fact === undefined) return { state: null, note: "unknown" };
      if (fact === null) return { state: null, note: "unavailable" };
      if (fact.kind === "pull") {
        if (fact.state === "closed") return { state: null, note: "cancelled" };
        return { state: fact.mergedIntoDefault ? "done" : "working", note: null };
      }
      if (fact.state === "not-planned") return { state: null, note: "cancelled" };
      if (fact.state === "done") return { state: "done", note: null };
      return {
        state:
          fact.openClosingPullRequests.length > 0 || working.length > 0 ? "working" : "planned",
        note: null,
      };
    })();
    const broken = brokenBy(part, facts);
    if (broken === "broken") return { part, state: "broken", note: null, url, threads: working };
    // The unread evidence could hold a defect, so no clean state is claimed.
    if (broken === "possibly") {
      return { part, state: null, note: "incomplete", url, threads: working };
    }
    return { part, ...base, url, threads: working };
  });
}

export interface FeatureMapGaps {
  readonly untrackedParts: ReadonlyArray<FeatureMapPart>;
  readonly sharedIssues: ReadonlyArray<{
    readonly ref: FeatureMapRef;
    readonly partIds: ReadonlyArray<string>;
  }>;
  readonly openIssuesOnNoPart: MapSnapshot["openIssues"];
  readonly openIssuesPartial: boolean;
  readonly problemRefs: ReadonlyArray<{
    readonly partId: string;
    readonly ref: FeatureMapRef;
    readonly note: "cancelled" | "unavailable" | "incomplete";
  }>;
  readonly undeclaredRefLines: ReadonlyArray<FeatureMapRefLineProblem>;
  readonly unreadableRefs: ReadonlyArray<FeatureMapRefLineProblem>;
}

/** What the map does not track, each row named for what it proves. */
export function featureMapGaps(map: MapSnapshot): FeatureMapGaps {
  const facts = factsByRef(map);
  const issueParts = new Map<string, { ref: FeatureMapRef; partIds: Array<string> }>();
  const named = new Set<string>();
  const problemRefs: Array<FeatureMapGaps["problemRefs"][number]> = [];
  for (const part of map.parts) {
    for (const ref of part.refs) {
      const key = refKey(ref);
      named.add(key);
      const fact = facts.get(key);
      if (fact === null) problemRefs.push({ partId: part.id, ref, note: "unavailable" });
      else if (
        (fact?.kind === "issue" && fact.state === "not-planned") ||
        (fact?.kind === "pull" && fact.state === "closed")
      ) {
        problemRefs.push({ partId: part.id, ref, note: "cancelled" });
      } else if (fact?.kind === "issue" && fact.state === "open" && !fact.complete) {
        problemRefs.push({ partId: part.id, ref, note: "incomplete" });
      }
      if (!isIssueRef(ref, fact)) continue;
      const entry = issueParts.get(key) ?? { ref, partIds: [] };
      if (!entry.partIds.includes(part.id)) entry.partIds.push(part.id);
      issueParts.set(key, entry);
    }
  }
  return {
    untrackedParts: map.parts.filter((part) => !part.context && part.refs.length === 0),
    sharedIssues: [...issueParts.values()].filter((entry) => entry.partIds.length > 1),
    openIssuesOnNoPart: map.openIssues.filter(
      (openIssue) => !named.has(refKey({ repository: map.repository, number: openIssue.number })),
    ),
    openIssuesPartial: map.openIssuesPartial,
    problemRefs,
    undeclaredRefLines: map.undeclaredRefLines,
    unreadableRefs: map.unreadableRefs,
  };
}

/** Mermaid class names; explicit text colours keep both themes readable. */
const FEATURE_MAP_CLASSES = {
  planned: { name: "fmPlanned", style: "fill:#e0e7ff,stroke:#4f46e5,color:#111827" },
  working: { name: "fmWorking", style: "fill:#fde68a,stroke:#b45309,color:#111827" },
  broken: { name: "fmBroken", style: "fill:#fecaca,stroke:#b91c1c,color:#111827" },
  done: { name: "fmDone", style: "fill:#bbf7d0,stroke:#15803d,color:#111827" },
  muted: {
    name: "fmMuted",
    style: "fill:#f3f4f6,stroke:#9ca3af,color:#4b5563,stroke-dasharray:4 3",
  },
  context: { name: "fmContext", style: "fill:#ffffff,stroke:#9ca3af,color:#4b5563" },
} as const;

export function featureMapClassOf(view: Pick<FeatureMapPartView, "state" | "note">) {
  if (view.state !== null) return FEATURE_MAP_CLASSES[view.state];
  return view.note === "context" ? FEATURE_MAP_CLASSES.context : FEATURE_MAP_CLASSES.muted;
}

/**
 * The map's own drawing with `classDef` and `class` lines appended for each part's state. Labels
 * stay as the file wrote them, so no GitHub text enters the diagram.
 */
export function featureMapDiagramSource(
  source: string,
  views: ReadonlyArray<FeatureMapPartView>,
): string {
  const byClass = new Map<string, Array<string>>();
  for (const view of views) {
    const { name } = featureMapClassOf(view);
    byClass.set(name, [...(byClass.get(name) ?? []), view.part.id]);
  }
  const lines = Object.values(FEATURE_MAP_CLASSES).flatMap(({ name, style }) => {
    const ids = byClass.get(name);
    return ids === undefined
      ? []
      : [`    classDef ${name} ${style}`, `    class ${ids.join(",")} ${name}`];
  });
  return `${source.trimEnd()}\n${lines.join("\n")}\n`;
}

// Mermaid 11 names a flowchart node `<diagram id>-flowchart-<node id>-<counter>`.
const RENDERED_NODE_ID = /^mermaid-diagram-\d+-flowchart-(.+)-\d+$/;

/** The part a rendered node draws, or null for anything else in the SVG. */
export function featureMapPartIdOf(elementId: string, partIds: ReadonlySet<string>) {
  const id = RENDERED_NODE_ID.exec(elementId)?.[1];
  return id !== undefined && partIds.has(id) ? id : null;
}

export type FeatureMapShown =
  | { readonly kind: "unsupported" }
  | { readonly kind: "loading" }
  | { readonly kind: "error"; readonly message: string }
  | {
      readonly kind: "snapshot";
      readonly snapshot: FeatureMapSnapshot;
      /** The latest read failed; this is the last good one. */
      readonly stale: { readonly since: string; readonly message: string } | null;
    };

/**
 * What the tab shows: a server without the read is unknown, a failed read keeps the last good
 * snapshot with its time, and only with none at all is the error shown.
 */
export function featureMapShown(input: {
  readonly supported: boolean;
  readonly latest:
    | { readonly _tag: "success"; readonly snapshot: FeatureMapSnapshot }
    | { readonly _tag: "failure"; readonly message: string }
    | null;
  readonly lastGood: FeatureMapSnapshot | null;
}): FeatureMapShown {
  if (!input.supported) return { kind: "unsupported" };
  const { latest, lastGood } = input;
  if (latest?._tag === "success")
    return { kind: "snapshot", snapshot: latest.snapshot, stale: null };
  if (latest === null) {
    return lastGood === null
      ? { kind: "loading" }
      : { kind: "snapshot", snapshot: lastGood, stale: null };
  }
  return lastGood === null
    ? { kind: "error", message: latest.message }
    : {
        kind: "snapshot",
        snapshot: lastGood,
        stale: { since: lastGood.fetchedAt, message: latest.message },
      };
}
