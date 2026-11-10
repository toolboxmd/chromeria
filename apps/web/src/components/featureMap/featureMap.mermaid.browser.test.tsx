import markdown from "../../../../../docs/map.md?raw";
import { render } from "../../../test/render";
import { Suspense } from "react";
import { describe, expect, it } from "vite-plus/test";

import { MermaidDiagram } from "../chat/MermaidDiagram";
import {
  FEATURE_MAP_NODE_SELECTOR,
  featureMapPartIdOf,
  featureMapDiagramSource,
} from "./featureMap.logic";

async function renderedSvg(source: string): Promise<{ svg: SVGSVGElement } & AsyncDisposable> {
  await using stack = new AsyncDisposableStack();
  let onRendered!: (element: SVGSVGElement) => void;
  const rendered = new Promise<SVGSVGElement>((resolve) => {
    onRendered = resolve;
  });
  stack.use(
    await render(
      <Suspense fallback={null}>
        <MermaidDiagram source={source} onRendered={onRendered} />
      </Suspense>,
    ),
  );
  const svg = await rendered;
  return Object.assign(stack.move(), { svg });
}

describe("the feature map on the pinned Mermaid", () => {
  it("finds every part's node by its rendered id, with its state class applied", async () => {
    await using result = await renderedSvg(`flowchart TD
    C["Capture"]
    %% C: #7
    SR["Serve report"]
    %% SR: PR #38 #15
    flowchart_2["Node named like the prefix"]
    C --> SR --> flowchart_2
    classDef fmDone fill:#bbf7d0
    class SR fmDone`);
    const svg = result.svg;
    const parts = new Set(["C", "SR", "flowchart_2"]);
    const nodes = [...svg.querySelectorAll<SVGGElement>(FEATURE_MAP_NODE_SELECTOR)].map((node) => ({
      part: featureMapPartIdOf(node.getAttribute("data-id"), parts),
      done: node.classList.contains("fmDone"),
    }));
    expect(nodes).toEqual([
      { part: "C", done: false },
      { part: "SR", done: true },
      { part: "flowchart_2", done: false },
    ]);
    // Edges are never read as parts.
    const others = [...svg.querySelectorAll("[data-id]")].filter(
      (element) => !element.matches(FEATURE_MAP_NODE_SELECTOR),
    );
    expect(others.length).toBeGreaterThan(0);
    expect(
      others.map((element) => featureMapPartIdOf(element.getAttribute("data-id"), parts)),
    ).toEqual(others.map(() => null));
  });
});

it("renders subgraphs without turning clusters into clickable parts", async () => {
  await using result = await renderedSvg(`flowchart TD
    subgraph BASE["Existing capabilities"]
      L["Live capability"]
      C["Clients"]
      L --> C
    end
    F["Fork work"]
    C --> F
    classDef fmLive fill:#cffafe
    class L,C fmLive
    classDef fmPlanned fill:#e0e7ff
    class F fmPlanned`);
  const svg = result.svg;
  const parts = new Set(["L", "C", "F"]);
  const nodes = [...svg.querySelectorAll<SVGGElement>(FEATURE_MAP_NODE_SELECTOR)];
  expect(
    nodes.map((node) => featureMapPartIdOf(node.getAttribute("data-id"), parts)).sort(),
  ).toEqual(["C", "F", "L"]);
  expect(nodes.filter((node) => node.classList.contains("fmLive"))).toHaveLength(2);
  expect(nodes.filter((node) => node.classList.contains("fmPlanned"))).toHaveLength(1);
  const clusters = [...svg.querySelectorAll("g.pele-cluster")];
  expect(clusters).toHaveLength(1);
  expect(
    clusters.map((cluster) => featureMapPartIdOf(cluster.getAttribute("data-id"), parts)),
  ).toEqual([null]);
});

it("renders the real holistic map, preserving every part and live class", async () => {
  const source = /```mermaid\n([\s\S]*?)\n```/.exec(markdown)![1]!;
  const parts = new Set(
    [...source.matchAll(/^\s*(\w+)\["[^"\n]*"\]\s*$/gm)].map((match) => match[1]!),
  );
  const liveIds = [...source.matchAll(/^\s*%% (\w+): live\s*$/gm)].map((match) => match[1]!);
  const views = [...parts].map((id) => ({
    part: {
      id,
      name: id,
      live: liveIds.includes(id),
      context: false,
      primaryUnreadable: false,
      refs: [],
    },
    state: null,
    note: liveIds.includes(id) ? ("live" as const) : ("unknown" as const),
    url: null,
    threads: [],
  }));
  await using result = await renderedSvg(featureMapDiagramSource(source, views));
  const svg = result.svg;
  const nodes = [...svg.querySelectorAll<SVGGElement>(FEATURE_MAP_NODE_SELECTOR)];
  expect(
    nodes.map((node) => featureMapPartIdOf(node.getAttribute("data-id"), parts)).sort(),
  ).toEqual([...parts].sort());
  expect(
    nodes
      .filter((node) => node.classList.contains("fmLive"))
      .map((node) => featureMapPartIdOf(node.getAttribute("data-id"), parts))
      .sort(),
  ).toEqual([...liveIds].sort());
  expect(
    [...svg.querySelectorAll("g.pele-cluster")].map((cluster) =>
      featureMapPartIdOf(cluster.getAttribute("data-id"), parts),
    ),
  ).toEqual([null]);
});
