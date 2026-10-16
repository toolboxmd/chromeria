import {
  EnvironmentId,
  type FeatureMapIssueFact,
  type FeatureMapPart,
  type FeatureMapPullRequestFact,
  type FeatureMapRef,
  type FeatureMapRefFact,
  type FeatureMapSnapshot,
  ThreadId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { IssueRowThread } from "../issues/issueStatus.logic";
import {
  clampMapScale,
  featureMapClassOf,
  featureMapDiagramSource,
  featureMapGaps,
  featureMapPartIdOf,
  featureMapShown,
  fitMapViewport,
  foldFeatureMap,
  iconAnchorInLayer,
  isChromeriaFeatureMapPart,
  MAP_DRAG_THRESHOLD_PX,
  MAP_FIT_PADDING_PX,
  MAP_SCALE_MAX,
  MAP_SCALE_MIN,
  MAP_WHEEL_SENSITIVITY,
  panMapViewport,
  pointerMovedPastDragThreshold,
  scaleAfterWheel,
  stepMapScale,
  zoomMapViewport,
} from "./featureMap.logic";

type MapSnapshot = Extract<FeatureMapSnapshot, { _tag: "Map" }>;

const REPO = "toolboxmd/contextmd";
const issue = (number: number, repository = REPO): FeatureMapRef => ({
  kind: "issue",
  repository,
  number,
  url: `https://github.com/${repository}/issues/${number}`,
});
const pull = (number: number, repository = REPO): FeatureMapRef => ({
  kind: "pull",
  repository,
  number,
  url: `https://github.com/${repository}/pull/${number}`,
});
const part = (id: string, refs: ReadonlyArray<FeatureMapRef>, context = false): FeatureMapPart => ({
  id,
  name: `Part ${id}`,
  context,
  live: false,
  primaryUnreadable: false,
  refs,
});
const issueFact = (
  number: number,
  overrides: Partial<FeatureMapIssueFact> = {},
  repository = REPO,
): FeatureMapRefFact => ({
  repository,
  number,
  fact: {
    kind: "issue",
    url: `https://github.com/${repository}/issues/${number}`,
    state: "open",
    labels: [],
    openClosingPullRequests: [],
    complete: true,
    ...overrides,
  },
});
const pullFact = (
  number: number,
  overrides: Partial<FeatureMapPullRequestFact> = {},
  repository = REPO,
): FeatureMapRefFact => ({
  repository,
  number,
  fact: {
    kind: "pull",
    url: `https://github.com/${repository}/pull/${number}`,
    state: "open",
    isDraft: false,
    mergedIntoDefault: false,
    checks: null,
    ...overrides,
  },
});

const snapshot = (
  parts: ReadonlyArray<FeatureMapPart>,
  facts: ReadonlyArray<FeatureMapRefFact>,
  extra: Partial<MapSnapshot> = {},
): MapSnapshot => ({
  _tag: "Map",
  host: "github.com",
  repository: REPO,
  branch: "main",
  commit: "3f2a1c0",
  fetchedAt: "2026-10-13T15:42:00.000Z",
  source: "flowchart TD",
  parts,
  undeclaredRefLines: [],
  unreadableRefs: [],
  facts,
  openIssues: [],
  openIssuesPartial: false,
  ...extra,
});

const environmentId = EnvironmentId.make("rocky");
const thread = (id: string, pullRequests: ReadonlyArray<number> = []): IssueRowThread => ({
  environmentId,
  id: ThreadId.make(id),
  title: `Thread ${id}`,
  archivedAt: null,
  sources: ["manual"],
  pullRequests: pullRequests.map((number) => ({ host: "github.com", repository: REPO, number })),
});
const threadKey = (id: string) => `${environmentId}:${id}`;
const noThreads = { threadsByIssue: new Map(), working: new Set<string>() };

const stateOf = (map: MapSnapshot, id: string, threads = noThreads) => {
  const view = foldFeatureMap(map, threads).find((entry) => entry.part.id === id)!;
  return { state: view.state, note: view.note };
};

describe("foldFeatureMap: an Issue primary", () => {
  const map = (fact: FeatureMapRefFact) => snapshot([part("C", [issue(7)])], [fact]);

  it("is planned while open with no closing pull request and no working thread", () => {
    expect(stateOf(map(issueFact(7)), "C")).toEqual({ state: "planned", note: null });
  });

  it("is working while an open pull request would close it", () => {
    const fact = issueFact(7, {
      openClosingPullRequests: [
        { repository: REPO, number: 22, url: "u22", isDraft: true, checks: null },
      ],
    });
    expect(stateOf(map(fact), "C")).toEqual({ state: "working", note: null });
  });

  it("is working while a linked thread works on it, and planned once it stops", () => {
    const threads = new Map([["github.com toolboxmd/contextmd#7", [thread("t1")]]]);
    expect(
      stateOf(map(issueFact(7)), "C", {
        threadsByIssue: threads,
        working: new Set([threadKey("t1")]),
      }),
    ).toEqual({ state: "working", note: null });
    expect(
      stateOf(map(issueFact(7)), "C", { threadsByIssue: threads, working: new Set() }),
    ).toEqual({ state: "planned", note: null });
  });

  it("is done once closed as completed, and planned again when reopened", () => {
    expect(stateOf(map(issueFact(7, { state: "done" })), "C")).toEqual({
      state: "done",
      note: null,
    });
    expect(stateOf(map(issueFact(7, { state: "open" })), "C")).toEqual({
      state: "planned",
      note: null,
    });
  });

  it("shows cancelled, not done, when closed as not planned", () => {
    expect(stateOf(map(issueFact(7, { state: "not-planned" })), "C")).toEqual({
      state: null,
      note: "cancelled",
    });
  });

  it("shows unavailable, never a known state, when GitHub returned nothing", () => {
    expect(stateOf(map({ repository: REPO, number: 7, fact: null }), "C")).toEqual({
      state: null,
      note: "unavailable",
    });
  });

  it("shows unknown when the snapshot has no fact for the ref yet", () => {
    expect(stateOf(snapshot([part("C", [issue(7)])], []), "C")).toEqual({
      state: null,
      note: "unknown",
    });
  });
});

describe("foldFeatureMap: a pull request primary", () => {
  it("is done when merged into the default branch, even while a later Issue ref stays open", () => {
    const map = snapshot(
      [part("SR", [pull(38), issue(15)])],
      [pullFact(38, { state: "merged", mergedIntoDefault: true }), issueFact(15)],
    );
    expect(stateOf(map, "SR")).toEqual({ state: "done", note: null });
  });

  it("is working when merged into a branch other than the default", () => {
    const map = snapshot(
      [part("W", [pull(44)])],
      [pullFact(44, { state: "merged", mergedIntoDefault: false })],
    );
    expect(stateOf(map, "W")).toEqual({ state: "working", note: null });
  });

  it("is working while open, and cancelled when closed unmerged", () => {
    expect(stateOf(snapshot([part("W", [pull(44)])], [pullFact(44)]), "W").state).toBe("working");
    expect(
      stateOf(snapshot([part("W", [pull(44)])], [pullFact(44, { state: "closed" })]), "W"),
    ).toEqual({ state: null, note: "cancelled" });
  });
});

describe("foldFeatureMap: broken", () => {
  it("wins when any open Issue among the refs carries the bug label", () => {
    const map = snapshot(
      [part("SR", [pull(38), issue(50)])],
      [
        pullFact(38, { state: "merged", mergedIntoDefault: true }),
        issueFact(50, { labels: ["Bug"] }),
      ],
    );
    expect(stateOf(map, "SR").state).toBe("broken");
  });

  it("ignores the bug label on a closed Issue", () => {
    const map = snapshot(
      [part("SR", [pull(38), issue(50)])],
      [
        pullFact(38, { state: "merged", mergedIntoDefault: true }),
        issueFact(50, { state: "done", labels: ["bug"] }),
      ],
    );
    expect(stateOf(map, "SR").state).toBe("done");
  });

  it("wins when an open pull request of the part fails its checks, but not a merged one", () => {
    const failingClosing = issueFact(7, {
      openClosingPullRequests: [
        { repository: REPO, number: 22, url: "u22", isDraft: false, checks: "failing" },
      ],
    });
    expect(stateOf(snapshot([part("C", [issue(7)])], [failingClosing]), "C").state).toBe("broken");
    expect(
      stateOf(snapshot([part("C", [pull(9)])], [pullFact(9, { checks: "failing" })]), "C").state,
    ).toBe("broken");
    expect(
      stateOf(
        snapshot(
          [part("C", [pull(9)])],
          [pullFact(9, { state: "merged", mergedIntoDefault: true, checks: "failing" })],
        ),
        "C",
      ).state,
    ).toBe("done");
  });

  it("stays broken while a thread works on the part, and keeps the thread for the icon", () => {
    const map = snapshot([part("C", [issue(7)])], [issueFact(7, { labels: ["bug"] })]);
    const threads = new Map([["github.com toolboxmd/contextmd#7", [thread("fixer")]]]);
    const [view] = foldFeatureMap(map, {
      threadsByIssue: threads,
      working: new Set([threadKey("fixer")]),
    });
    expect(view?.state).toBe("broken");
    expect(view?.threads.map((entry) => entry.id)).toEqual(["fixer"]);
  });
});

describe("foldFeatureMap: incomplete or unreadable evidence", () => {
  it("does not claim done when an open Issue ref has more labels than GitHub returned", () => {
    // The `bug` label could be the 21st label: the part may be broken.
    const map = snapshot(
      [part("SR", [pull(38), issue(50)])],
      [
        pullFact(38, { state: "merged", mergedIntoDefault: true }),
        issueFact(50, { complete: false }),
      ],
    );
    expect(stateOf(map, "SR")).toEqual({ state: null, note: "incomplete" });
  });

  it("does not claim working when the primary Issue has more closing PRs than GitHub returned", () => {
    const map = snapshot(
      [part("C", [issue(7)])],
      [
        issueFact(7, {
          complete: false,
          openClosingPullRequests: [
            { repository: REPO, number: 22, url: "u22", isDraft: false, checks: "passing" },
          ],
        }),
      ],
    );
    expect(stateOf(map, "C")).toEqual({ state: null, note: "incomplete" });
  });

  it("still shows broken when the evidence that was read proves it", () => {
    const map = snapshot(
      [part("C", [issue(7)])],
      [issueFact(7, { complete: false, labels: ["bug"] })],
    );
    expect(stateOf(map, "C").state).toBe("broken");
  });

  it("ignores incomplete evidence on a closed Issue", () => {
    const map = snapshot(
      [part("C", [issue(7)])],
      [issueFact(7, { state: "done", complete: false })],
    );
    expect(stateOf(map, "C").state).toBe("done");
  });

  it("shows unknown, and opens nothing, when the primary ref was unreadable", () => {
    const map = snapshot(
      [{ ...part("A", [pull(38)]), primaryUnreadable: true }],
      [pullFact(38, { state: "merged", mergedIntoDefault: true })],
    );
    const [view] = foldFeatureMap(map, noThreads);
    expect({ state: view?.state, note: view?.note, url: view?.url }).toEqual({
      state: null,
      note: "unknown",
      url: null,
    });
  });
});

describe("foldFeatureMap: the thread icon", () => {
  it("lights only the part whose pull request the working thread carries", () => {
    // SR, SC and LJ all name #15; only the thread on SC's PR #45 works.
    const map = snapshot(
      [
        part("SR", [pull(38), issue(15)]),
        part("SC", [pull(45), issue(15)]),
        part("LJ", [issue(15)]),
      ],
      [pullFact(38), pullFact(45), issueFact(15)],
    );
    const threads = new Map([["github.com toolboxmd/contextmd#15", [thread("card", [45])]]]);
    const views = foldFeatureMap(map, {
      threadsByIssue: threads,
      working: new Set([threadKey("card")]),
    });
    expect(
      Object.fromEntries(views.map((view) => [view.part.id, view.threads.map((t) => t.id)])),
    ).toEqual({ SR: [], SC: ["card"], LJ: [] });
    expect(views.find((view) => view.part.id === "LJ")?.state).toBe("planned");
  });

  it("keeps a working thread on an Issue primary that also carries an unrelated part's PR", () => {
    // B does not name #7, so the thread's old link to B's PR #38 says nothing about A.
    const map = snapshot(
      [part("A", [issue(7)]), part("B", [pull(38)])],
      [issueFact(7), pullFact(38, { state: "merged", mergedIntoDefault: true })],
    );
    const threads = new Map([["github.com toolboxmd/contextmd#7", [thread("t7", [38])]]]);
    const [a] = foldFeatureMap(map, {
      threadsByIssue: threads,
      working: new Set([threadKey("t7")]),
    });
    expect(a?.state).toBe("working");
    expect(a?.threads.map((entry) => entry.id)).toEqual(["t7"]);
  });

  it("shows no icon for a linked thread that is not working", () => {
    const map = snapshot([part("C", [issue(7)])], [issueFact(7)]);
    const threads = new Map([["github.com toolboxmd/contextmd#7", [thread("idle")]]]);
    expect(
      foldFeatureMap(map, { threadsByIssue: threads, working: new Set() })[0]?.threads,
    ).toEqual([]);
  });
});

describe("featureMapGaps", () => {
  it("lists untracked parts, shared Issues, open Issues on no part, problem refs and lines", () => {
    const map = snapshot(
      [
        part("S", [], true),
        part("LJ", []),
        part("SR", [pull(38), issue(15)]),
        part("SC", [pull(45), issue(15)]),
        part("X", [issue(60)]),
        part("Y", [pull(70)]),
        part("Z", [issue(80)]),
      ],
      [
        pullFact(38, { state: "merged", mergedIntoDefault: true }),
        issueFact(15),
        pullFact(45),
        issueFact(60, { state: "not-planned" }),
        { repository: REPO, number: 70, fact: null },
        issueFact(80, { complete: false }),
      ],
      {
        openIssues: [
          { number: 15, title: "Serving labels", url: "u15" },
          { number: 49, title: "Recurrence lookups", url: "u49" },
        ],
        openIssuesPartial: true,
        undeclaredRefLines: [{ id: "GHOST", text: "#5" }],
        unreadableRefs: [{ id: "X", text: "issue-9" }],
      },
    );
    const gaps = featureMapGaps(map);
    expect(gaps.untrackedParts.map((entry) => entry.id)).toEqual(["LJ"]);
    expect(gaps.sharedIssues).toEqual([{ ref: issue(15), partIds: ["SR", "SC"] }]);
    expect(gaps.openIssuesOnNoPart.map((entry) => entry.number)).toEqual([49]);
    expect(gaps.openIssuesPartial).toBe(true);
    expect(gaps.problemRefs.map((entry) => [entry.partId, entry.ref.number, entry.note])).toEqual([
      ["X", 60, "cancelled"],
      ["Y", 70, "unavailable"],
      ["Z", 80, "incomplete"],
    ]);
    expect(gaps.undeclaredRefLines).toEqual([{ id: "GHOST", text: "#5" }]);
    expect(gaps.unreadableRefs).toEqual([{ id: "X", text: "issue-9" }]);
  });
});

describe("featureMapDiagramSource", () => {
  it("appends one class line per shown state and leaves the drawing untouched", () => {
    const map = snapshot(
      [part("S", [], true), part("C", [issue(7)]), part("D", [issue(8)]), part("E", [issue(9)])],
      [issueFact(7), issueFact(8), issueFact(9, { state: "done" })],
      { source: 'flowchart TD\n    C["Capture"] --> D' },
    );
    const source = featureMapDiagramSource(map.source, foldFeatureMap(map, noThreads));
    expect(source.startsWith('flowchart TD\n    C["Capture"] --> D\n')).toBe(true);
    expect(source).toContain("\n    class C,D fmPlanned");
    expect(source).toContain("\n    class E fmDone");
    expect(source).toContain("\n    class S fmContext");
    expect(source).toMatch(/\n {4}classDef fmPlanned /);
  });
});

describe("featureMapPartIdOf", () => {
  const ids = new Set(["C", "SR", "flowchart"]);
  it("reads the part from a rendered node id", () => {
    expect(featureMapPartIdOf("mermaid-diagram-12-flowchart-SR-3", ids)).toBe("SR");
    expect(featureMapPartIdOf("mermaid-diagram-0-flowchart-flowchart-0", ids)).toBe("flowchart");
  });
  it("ignores ids that are not parts", () => {
    expect(featureMapPartIdOf("mermaid-diagram-12-flowchart-GHOST-3", ids)).toBeNull();
    expect(featureMapPartIdOf("L_C_SR_0", ids)).toBeNull();
  });
});

describe("expanded map viewport", () => {
  const viewport = { scale: 2, x: 10, y: -20 };

  it("keeps the content point under the cursor when zooming", () => {
    // Content point at the anchor before the zoom, checked against the result independently.
    const anchor = { x: 30, y: 40 };
    const contentX = (anchor.x - viewport.x) / viewport.scale;
    const contentY = (anchor.y - viewport.y) / viewport.scale;
    const zoomed = zoomMapViewport(viewport, anchor, 4);
    expect(zoomed).toEqual({ scale: 4, x: -10, y: -80 });
    expect(zoomed.x + contentX * zoomed.scale).toBeCloseTo(anchor.x);
    expect(zoomed.y + contentY * zoomed.scale).toBeCloseTo(anchor.y);
  });

  it("does not drift when the next scale is already at the limit", () => {
    const limited = { scale: MAP_SCALE_MAX, x: 5, y: 6 };
    expect(zoomMapViewport(limited, { x: 12, y: 12 }, 100)).toEqual(limited);
    expect(zoomMapViewport({ scale: MAP_SCALE_MIN, x: 5, y: 6 }, { x: 1, y: 1 }, 0)).toEqual({
      scale: MAP_SCALE_MIN,
      x: 5,
      y: 6,
    });
  });

  it("clamps scale and ignores a scale that is not a number", () => {
    expect(clampMapScale(0.02)).toBe(MAP_SCALE_MIN);
    expect(clampMapScale(9)).toBe(MAP_SCALE_MAX);
    expect(clampMapScale(Number.NaN)).toBe(1);
    expect(clampMapScale(1.5)).toBe(1.5);
  });

  it("pans in viewport pixels without changing scale", () => {
    expect(panMapViewport(viewport, { x: 4, y: -3 })).toEqual({ scale: 2, x: 14, y: -23 });
    expect(panMapViewport(viewport, { x: Number.NaN, y: 1 })).toBe(viewport);
  });

  it("steps toward the cursor's side of the range and stops at the ends", () => {
    expect(stepMapScale(1, "in")).toBe(1.25);
    expect(stepMapScale(1, "out")).toBe(0.8);
    expect(stepMapScale(MAP_SCALE_MAX, "in")).toBe(MAP_SCALE_MAX);
    expect(stepMapScale(MAP_SCALE_MIN, "out")).toBe(MAP_SCALE_MIN);
  });

  it("zooms out on a positive wheel delta and in on a negative one, in pixels or lines", () => {
    const notch = scaleAfterWheel(1, 100, 0);
    expect(notch).toBeCloseTo(Math.exp(-100 * MAP_WHEEL_SENSITIVITY));
    expect(notch).toBeLessThan(1);
    expect(scaleAfterWheel(1, -100, 0)).toBeGreaterThan(1);
    expect(scaleAfterWheel(1, 1, 1)).toBeCloseTo(Math.exp(-16 * MAP_WHEEL_SENSITIVITY));
    expect(scaleAfterWheel(MAP_SCALE_MIN, 500, 0)).toBe(MAP_SCALE_MIN);
    expect(scaleAfterWheel(1, 0, 0)).toBe(1);
  });

  it("fits the diagram inside the viewport with padding, and centers it when scale hits a limit", () => {
    const padding = MAP_FIT_PADDING_PX;
    const fitted = fitMapViewport({
      contentWidth: 200,
      contentHeight: 100,
      viewWidth: 400,
      viewHeight: 400,
    });
    const scale = Math.min((400 - padding * 2) / 200, (400 - padding * 2) / 100);
    expect(fitted).toEqual({
      scale,
      x: (400 - 200 * scale) / 2,
      y: (400 - 100 * scale) / 2,
    });
    // The whole drawing is inside the padded viewport.
    expect(fitted!.x).toBeGreaterThanOrEqual(padding - 0.001);
    expect(fitted!.y).toBeGreaterThanOrEqual(padding - 0.001);
    expect(fitted!.x + 200 * scale).toBeLessThanOrEqual(400 - padding + 0.001);

    const shrunk = fitMapViewport({
      contentWidth: 10_000,
      contentHeight: 10_000,
      viewWidth: 400,
      viewHeight: 400,
    });
    expect(shrunk?.scale).toBe(MAP_SCALE_MIN);
    expect(shrunk).toEqual({
      scale: MAP_SCALE_MIN,
      x: (400 - 10_000 * MAP_SCALE_MIN) / 2,
      y: (400 - 10_000 * MAP_SCALE_MIN) / 2,
    });
    expect(
      fitMapViewport({ contentWidth: 0, contentHeight: 10, viewWidth: 10, viewHeight: 10 }),
    ).toBeNull();
  });

  it("treats a movement past 4px as a drag and a shorter one as a click", () => {
    const start = { x: 0, y: 0 };
    expect(pointerMovedPastDragThreshold(start, { x: MAP_DRAG_THRESHOLD_PX, y: 0 })).toBe(false);
    expect(pointerMovedPastDragThreshold(start, { x: 3, y: 3 })).toBe(true);
    expect(pointerMovedPastDragThreshold(start, { x: 2, y: 2 })).toBe(false);
  });

  it("converts a node's screen top-right into the scaled layer", () => {
    expect(
      iconAnchorInLayer({
        node: { right: 110, top: 40 },
        layer: { left: 10, top: 20 },
        scale: 2,
      }),
    ).toEqual({ left: 50, top: 10 });
    // The same layer-local corner, after the layer is translated and scaled, converts back.
    const local = { left: 80, top: 16 };
    const scale = 2.5;
    const layer = { left: 12, top: -4 };
    expect(
      iconAnchorInLayer({
        node: { right: layer.left + local.left * scale, top: layer.top + local.top * scale },
        layer,
        scale,
      }),
    ).toEqual(local);
  });
});

describe("featureMapShown", () => {
  const good = snapshot([part("C", [issue(7)])], [issueFact(7)]);

  it("shows unknown, not an empty map, on a server without the map read", () => {
    expect(featureMapShown({ supported: false, latest: null, lastGood: null })).toEqual({
      kind: "unsupported",
    });
  });

  it("keeps the last good snapshot with its time while GitHub is unreachable", () => {
    expect(
      featureMapShown({
        supported: true,
        latest: { _tag: "failure", message: "Could not reach GitHub at github.com." },
        lastGood: good,
      }),
    ).toEqual({
      kind: "snapshot",
      snapshot: good,
      stale: { since: good.fetchedAt, message: "Could not reach GitHub at github.com." },
    });
  });

  it("shows the read error when there is no snapshot yet", () => {
    expect(
      featureMapShown({
        supported: true,
        latest: { _tag: "failure", message: "No GitHub repository." },
        lastGood: null,
      }),
    ).toEqual({ kind: "error", message: "No GitHub repository." });
  });
});

describe("live parts and Chromeria marks", () => {
  it("keeps Live distinct from Done and Context, with no untracked gap or ref link", () => {
    const map = snapshot(
      [{ ...part("L", []), live: true }, part("C", [], true), part("D", [issue(1)]), part("U", [])],
      [issueFact(1, { state: "done" })],
    );
    const views = foldFeatureMap(map, noThreads);
    expect(views.map(({ state, note }) => [state, note])).toEqual([
      [null, "live"],
      [null, "context"],
      ["done", null],
      [null, "untracked"],
    ]);
    expect(views[0]).toMatchObject({ url: null, threads: [] });
    expect(featureMapClassOf(views[0]!).name).toBe("fmLive");
    expect(featureMapDiagramSource('flowchart TD\nL["Live"]', views)).toContain("class L fmLive");
    expect(featureMapGaps(map).untrackedParts.map((p) => p.id)).toEqual(["U"]);
  });

  it("scopes marks to the snapshot repository and excludes live, context and untracked parts", () => {
    const tracked = part("A", [issue(1)]);
    const chromeria = { host: "GitHub.COM", repository: "ToolboxMD/Chromeria" };
    expect(isChromeriaFeatureMapPart(chromeria, tracked)).toBe(true);
    expect(isChromeriaFeatureMapPart({ ...chromeria, repository: REPO }, tracked)).toBe(false);
    expect(isChromeriaFeatureMapPart({ ...chromeria, host: "github.example.com" }, tracked)).toBe(
      false,
    );
    expect(isChromeriaFeatureMapPart(chromeria, { ...tracked, live: true })).toBe(false);
    expect(isChromeriaFeatureMapPart(chromeria, { ...tracked, context: true })).toBe(false);
    expect(isChromeriaFeatureMapPart(chromeria, part("U", []))).toBe(false);
  });

  it.each([
    ["planned", issueFact(1)],
    [
      "working",
      issueFact(1, {
        openClosingPullRequests: [
          { repository: REPO, number: 2, url: "u2", isDraft: false, checks: null },
        ],
      }),
    ],
    ["broken", issueFact(1, { labels: ["bug"] })],
    ["done", issueFact(1, { state: "done" })],
    [null, issueFact(1, { state: "not-planned" })],
    [null, issueFact(1, { complete: false })],
    [null, { repository: REPO, number: 1, fact: null }],
  ] as const)("marks tracked work with state %s", (state, fact) => {
    const map = snapshot([part("A", [issue(1)])], [fact], { repository: "toolboxmd/chromeria" });
    const view = foldFeatureMap(map, noThreads)[0]!;
    expect(view.state).toBe(state);
    expect(isChromeriaFeatureMapPart(map, view.part)).toBe(true);
  });
});

it("keeps an invalid live/context binding unknown rather than Live or Context", () => {
  const invalid = { ...part("A", []), primaryUnreadable: true };
  expect(foldFeatureMap(snapshot([invalid], []), noThreads)[0]).toMatchObject({
    state: null,
    note: "unknown",
  });
});

it("marks tracked work whose GitHub facts have not arrived", () => {
  const map = snapshot([part("A", [issue(1)])], [], { repository: "toolboxmd/chromeria" });
  const view = foldFeatureMap(map, noThreads)[0]!;
  expect(view.note).toBe("unknown");
  expect(isChromeriaFeatureMapPart(map, view.part)).toBe(true);
});
