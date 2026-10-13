// @vitest-environment jsdom

import { act, Suspense } from "react";
import { createRoot } from "react-dom/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vite-plus/test";

import { MermaidDiagram } from "../chat/MermaidDiagram";
import { featureMapPartIdOf } from "./featureMap.logic";

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
  let svg: SVGSVGElement | null = null;
  await act(async () => {
    createRoot(host).render(
      <Suspense fallback={null}>
        <MermaidDiagram
          source={source}
          onRendered={(element) => {
            svg = element;
          }}
        />
      </Suspense>,
    );
  });
  await vi.waitFor(() => expect(svg).not.toBeNull());
  return svg!;
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
