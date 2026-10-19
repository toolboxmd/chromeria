// @vitest-environment jsdom
// @effect-diagnostics nodeBuiltinImport:off - Reads the real map as the renderer fixture.

import * as NodeFS from "node:fs";
import { act, Suspense } from "react";
import { createRoot } from "react-dom/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";

import { MermaidDiagram } from "../chat/MermaidDiagram";
import { featureMapPartIdOf, featureMapDiagramSource } from "./featureMap.logic";

// The installed Mermaid, not a mock, renders here; jsdom lacks SVG layout, so sizes are stubbed.
const prototype = window.SVGElement.prototype as SVGElement & {
  getBBox?: () => DOMRect;
  getComputedTextLength?: () => number;
};
beforeAll(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  prototype.getBBox = () => ({ x: 0, y: 0, width: 80, height: 24 }) as DOMRect;
  prototype.getComputedTextLength = () => 80;
});
afterAll(() => {
  delete prototype.getBBox;
  delete prototype.getComputedTextLength;
  vi.unstubAllGlobals();
});

async function renderedSvg(source: string): Promise<SVGSVGElement> {
  const host = document.createElement("div");
  document.body.append(host);
  let onRendered!: (element: SVGSVGElement) => void;
  const rendered = new Promise<SVGSVGElement>((resolve) => {
    onRendered = resolve;
  });
  await act(async () => {
    createRoot(host).render(
      <Suspense fallback={null}>
        <MermaidDiagram source={source} onRendered={onRendered} />
      </Suspense>,
    );
  });
  return rendered;
}

describe("the feature map on the pinned Mermaid", () => {
  it("finds every part's node by its rendered id, with its state class applied", async () => {
    const svg = await renderedSvg(`flowchart TD
    C["Capture"]
    %% C: #7
    SR["Serve report"]
    %% SR: PR #38 #15
    flowchart_2["Node named like the prefix"]
    C --> SR --> flowchart_2
    classDef fmDone fill:#bbf7d0
    class SR fmDone`);
    const parts = new Set(["C", "SR", "flowchart_2"]);
    const nodes = [...svg.querySelectorAll<SVGGElement>("g.node")].map((node) => ({
      part: featureMapPartIdOf(node.id, parts),
      done: node.classList.contains("fmDone"),
    }));
    expect(nodes).toEqual([
      { part: "C", done: false },
      { part: "SR", done: true },
      { part: "flowchart_2", done: false },
    ]);
    // Edges and markers are never read as parts.
    const others = [...svg.querySelectorAll("[id]")].filter(
      (element) => !element.matches("g.node"),
    );
    expect(others.length).toBeGreaterThan(0);
    expect(others.map((element) => featureMapPartIdOf(element.id, parts))).toEqual(
      others.map(() => null),
    );
  });
});

it("renders subgraphs without turning clusters into clickable parts", async () => {
  const svg = await renderedSvg(`flowchart TD
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
  const parts = new Set(["L", "C", "F"]);
  const nodes = [...svg.querySelectorAll<SVGGElement>("g.node")];
  expect(nodes.map((node) => featureMapPartIdOf(node.id, parts)).sort()).toEqual(["C", "F", "L"]);
  expect(nodes.filter((node) => node.classList.contains("fmLive"))).toHaveLength(2);
  expect(nodes.filter((node) => node.classList.contains("fmPlanned"))).toHaveLength(1);
  const clusters = [...svg.querySelectorAll("g.cluster")];
  expect(clusters).toHaveLength(1);
  expect(clusters.map((cluster) => featureMapPartIdOf(cluster.id, parts))).toEqual([null]);
});

it("renders the real holistic map, preserving every part and live class", async () => {
  const markdown = NodeFS.readFileSync(`${import.meta.dirname}/../../../../../docs/map.md`, "utf8");
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
  const svg = await renderedSvg(featureMapDiagramSource(source, views));
  const nodes = [...svg.querySelectorAll<SVGGElement>("g.node")];
  expect(nodes.map((node) => featureMapPartIdOf(node.id, parts)).sort()).toEqual([...parts].sort());
  expect(
    nodes
      .filter((node) => node.classList.contains("fmLive"))
      .map((node) => featureMapPartIdOf(node.id, parts))
      .sort(),
  ).toEqual([...liveIds].sort());
  expect(
    [...svg.querySelectorAll("g.cluster")].map((cluster) => featureMapPartIdOf(cluster.id, parts)),
  ).toEqual([null]);
});
