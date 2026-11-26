import * as Schema from "effect/Schema";
import { describe, expect, it } from "@effect/vitest";
import { FeatureMapSnapshot } from "./featureMap.ts";

const decode = Schema.decodeSync(FeatureMapSnapshot);
const encode = Schema.encodeSync(FeatureMapSnapshot);

const oldSnapshot = {
  _tag: "Map" as const,
  host: "github.com",
  repository: "toolboxmd/chromeria",
  branch: "main",
  commit: "abc",
  fetchedAt: "2026-10-16T10:00:00.000Z",
  source: "flowchart TD",
  facts: [],
  openIssues: [],
  openIssuesPartial: false,
  undeclaredRefLines: [],
  unreadableRefs: [],
  parts: [{ id: "A", name: "Alpha", context: false, primaryUnreadable: false, refs: [] }],
};

describe("FeatureMapSnapshot live compatibility", () => {
  it("decodes older snapshots with absent live as false", () => {
    const decoded = decode(oldSnapshot);
    if (decoded._tag !== "Map") throw new Error("expected map");
    expect(decoded.parts[0]?.live).toBe(false);
  });
  it("preserves explicit live through a snapshot encode/decode round trip", () => {
    const decoded = decode({
      ...oldSnapshot,
      parts: [{ ...oldSnapshot.parts[0]!, live: true }],
    });
    const encoded = encode(decoded);
    expect(decode(encoded)).toEqual(decoded);
    if (decoded._tag !== "Map") throw new Error("expected map");
    expect(decoded.parts[0]?.live).toBe(true);
  });
});
