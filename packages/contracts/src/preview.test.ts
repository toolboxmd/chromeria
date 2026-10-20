import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { PreviewStreamInput } from "./preview.ts";
import {
  PreviewAutomationHost,
  PreviewAutomationStatus,
  PreviewAutomationUploadInput,
} from "./previewAutomation.ts";

const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);
const decodeAutomationStatus = Schema.decodeUnknownSync(PreviewAutomationStatus);

describe("PreviewAutomationHost", () => {
  it("accepts legacy hosts and current operation advertisements", () => {
    expect(decodeAutomationHost({ clientId: "legacy", environmentId: "environment-1" })).toEqual({
      clientId: "legacy",
      environmentId: "environment-1",
    });
    expect(
      decodeAutomationHost({
        clientId: "current",
        environmentId: "environment-1",
        supportedOperations: ["status", "resize"],
      }).supportedOperations,
    ).toEqual(["status", "resize"]);
  });
});

describe("PreviewAutomationStatus", () => {
  it("accepts old hosts without viewport data and exposes it from current hosts", () => {
    const base = {
      available: true,
      visible: false,
      tabId: "preview-t",
      url: "https://example.com",
      title: "Example",
      loading: false,
    };
    expect(decodeAutomationStatus(base)).toEqual(base);
    expect(
      decodeAutomationStatus({
        ...base,
        viewportSetting: { _tag: "preset", presetId: "pixel-8", width: 412, height: 915 },
        viewport: { width: 412, height: 915 },
      }).viewport,
    ).toEqual({ width: 412, height: 915 });
  });
});

describe("qualified preview upload", () => {
  const resource = {
    type: "preview",
    environmentId: "b",
    threadId: "thread-b",
    tabId: "tab-b",
    serverEpoch: "epoch-b",
  };
  const artifact = {
    environmentId: "a",
    attachmentId: "thread-a-550e8400-e29b-41d4-a716-446655440000",
    owner: "a".repeat(64),
    digest: "b".repeat(64),
    name: "report.txt",
    mimeType: "text/plain",
    sizeBytes: 50 * 1024 * 1024,
  };
  const decode = Schema.decodeUnknownSync(PreviewAutomationUploadInput);
  it("preserves local paths and deliberate empty clear/cancel while binding artifacts to their opening", () => {
    expect(decode({ paths: [] })).toEqual({ paths: [] });
    expect(decode({ paths: ["/local/report.txt"], locator: "input[type=file]" }).paths).toEqual([
      "/local/report.txt",
    ]);
    expect(decode({ resource, artifacts: [], locator: "input[type=file]" }).artifacts).toEqual([]);
    expect(
      decode({ resource, artifacts: [artifact, { ...artifact, environmentId: "b" }] }).artifacts,
    ).toHaveLength(2);
  });
  it("rejects conflict, missing/contradictory target, duplicate files and aggregate limits", () => {
    for (const input of [
      {},
      { paths: [], artifacts: [], resource },
      { artifacts: [] },
      { resource, artifacts: [], tabId: "other" },
      { resource, artifacts: [artifact, artifact] },
      {
        resource,
        artifacts: Array.from({ length: 21 }, (_, index) => ({
          ...artifact,
          attachmentId: `thread-${index}-550e8400-e29b-41d4-a716-446655440000`,
          sizeBytes: 1,
        })),
      },
      {
        resource,
        artifacts: Array.from({ length: 3 }, (_, index) => ({
          ...artifact,
          attachmentId: `thread-${index}-550e8400-e29b-41d4-a716-446655440000`,
        })),
      },
      { resource, artifacts: [{ ...artifact, sizeBytes: artifact.sizeBytes + 1 }] },
    ])
      expect(() => decode(input)).toThrow();
  });
});

describe("native viewer scalar domains", () => {
  const decode = Schema.decodeUnknownSync(PreviewStreamInput);
  const mouse = {
    type: "mouse",
    action: "down",
    x: 0,
    y: 0,
    button: "left",
    buttons: 1,
    clickCount: 1,
    modifiers: 0,
  };
  it("retains native fourth/later click counts, five button bits and finite wheel displacement", () => {
    for (const clickCount of [0, 1, 3, 4, 6, 2147483647]) {
      expect(decode({ ...mouse, clickCount, buttons: 31 })).toEqual({
        ...mouse,
        clickCount,
        buttons: 31,
      });
    }
    const wheel = {
      type: "wheel",
      x: 100000,
      y: -100000,
      deltaX: 150000,
      deltaY: -250000,
      modifiers: 15,
    };
    expect(decode(wheel)).toEqual(wheel);
  });
  it("rejects nonfinite/fractional/out-of-domain scalars and private fields", () => {
    for (const fields of [
      { clickCount: -1 },
      { clickCount: 0.5 },
      { clickCount: 2147483648 },
      { clickCount: Infinity },
      { buttons: -1 },
      { buttons: 32 },
      { buttons: 1.5 },
      { x: 100001 },
      { y: NaN },
      { modifiers: 16 },
      { private: true },
    ])
      expect(() => decode({ ...mouse, ...fields })).toThrow();
    for (const deltaX of [NaN, Infinity, -Infinity])
      expect(() =>
        decode({ type: "wheel", x: 0, y: 0, deltaX, deltaY: 0, modifiers: 0 }),
      ).toThrow();
  });
});
