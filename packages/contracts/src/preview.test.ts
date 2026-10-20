import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  ConfiguredLocalServerUrls,
  CONFIGURED_LOCAL_SERVER_URLS_MAX_ITEMS,
  DiscoveredLocalServer,
  PREVIEW_URL_MAX_LENGTH,
  PreviewEvent,
  PreviewStreamInput,
  PreviewNavStatus,
  PreviewSessionSnapshot,
  PreviewViewportSetting,
} from "./preview.ts";
import {
  PreviewAutomationHost,
  PreviewAutomationError,
  PreviewAutomationOpenInput,
  PreviewAutomationResizeInput,
  PreviewAutomationResizeResult,
  PreviewAutomationStatus,
  PreviewAutomationUploadInput,
} from "./previewAutomation.ts";

const decodePreviewEvent = Schema.decodeUnknownSync(PreviewEvent);
const decodeSnapshot = Schema.decodeUnknownSync(PreviewSessionSnapshot);
const decodeNavStatus = Schema.decodeUnknownSync(PreviewNavStatus);
const decodeServer = Schema.decodeUnknownSync(DiscoveredLocalServer);
const decodeConfiguredLocalServerUrls = Schema.decodeUnknownSync(ConfiguredLocalServerUrls);
const decodeViewport = Schema.decodeUnknownSync(PreviewViewportSetting);
const decodeResizeInput = Schema.decodeUnknownSync(PreviewAutomationResizeInput);
const decodeOpenInput = Schema.decodeUnknownSync(PreviewAutomationOpenInput);
const decodeResizeResult = Schema.decodeUnknownSync(PreviewAutomationResizeResult);
const decodeAutomationHost = Schema.decodeUnknownSync(PreviewAutomationHost);
const decodeAutomationError = Schema.decodeUnknownSync(PreviewAutomationError);
const decodeAutomationStatus = Schema.decodeUnknownSync(PreviewAutomationStatus);

describe("PreviewAutomationOpenInput", () => {
  it("accepts the inline preview visibility flag", () => {
    expect(decodeOpenInput({ open: false })).toEqual({ open: false });
  });

  it("retains the legacy show visibility alias", () => {
    expect(decodeOpenInput({ show: false })).toEqual({ show: false });
  });
});

describe("PreviewNavStatus", () => {
  it("decodes Idle", () => {
    expect(decodeNavStatus({ _tag: "Idle" })).toEqual({ _tag: "Idle" });
  });

  it("decodes Loading with title", () => {
    expect(decodeNavStatus({ _tag: "Loading", url: "http://localhost:5173/", title: "" })).toEqual({
      _tag: "Loading",
      url: "http://localhost:5173/",
      title: "",
    });
  });

  it("decodes LoadFailed with code/description", () => {
    expect(
      decodeNavStatus({
        _tag: "LoadFailed",
        url: "https://example.com/",
        title: "Example",
        code: -105,
        description: "ERR_NAME_NOT_RESOLVED",
      }),
    ).toEqual({
      _tag: "LoadFailed",
      url: "https://example.com/",
      title: "Example",
      code: -105,
      description: "ERR_NAME_NOT_RESOLVED",
    });
  });

  it("rejects empty url", () => {
    expect(() => decodeNavStatus({ _tag: "Loading", url: "", title: "" })).toThrow();
  });
});

describe("PreviewSessionSnapshot", () => {
  it("round-trips a Success snapshot", () => {
    const snapshot = decodeSnapshot({
      threadId: "thread-1",
      tabId: "preview-thread-1",
      navStatus: {
        _tag: "Success",
        url: "http://localhost:5173/",
        title: "Vite App",
      },
      canGoBack: false,
      canGoForward: false,
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
    expect(snapshot.tabId).toBe("preview-thread-1");
    expect(snapshot.navStatus._tag).toBe("Success");
  });
});

describe("PreviewViewportSetting", () => {
  it("decodes fill, freeform, and preset modes", () => {
    expect(decodeViewport({ _tag: "fill" })).toEqual({ _tag: "fill" });
    expect(decodeViewport({ _tag: "freeform", width: 1024, height: 768 })).toEqual({
      _tag: "freeform",
      width: 1024,
      height: 768,
    });
    expect(
      decodeViewport({
        _tag: "preset",
        presetId: "iphone-15-pro",
        width: 393,
        height: 852,
      }),
    ).toMatchObject({ _tag: "preset", presetId: "iphone-15-pro" });
  });

  it("rejects unsafe dimensions and oversized render areas", () => {
    expect(() => decodeViewport({ _tag: "freeform", width: 100, height: 800 })).toThrow();
    expect(() => decodeViewport({ _tag: "freeform", width: 3840, height: 3840 })).toThrow();
  });
});

describe("PreviewAutomationResizeInput", () => {
  it("requires fields that match the selected mode", () => {
    expect(decodeResizeInput({ mode: "fill" })).toEqual({ mode: "fill" });
    expect(
      decodeResizeInput({ mode: "preset", preset: "pixel-7", orientation: "landscape" }),
    ).toMatchObject({ mode: "preset", preset: "pixel-7" });
    expect(() => decodeResizeInput({ mode: "preset", preset: "pixel-8" })).toThrow();
    expect(() => decodeResizeInput({ mode: "freeform", width: 1024 })).toThrow();
    expect(() => decodeResizeInput({ mode: "fill", width: 1024, height: 768 })).toThrow();
  });

  it("allows fill-mode measurements below the minimum selectable fixed size", () => {
    expect(
      decodeResizeResult({
        tabId: "preview-t",
        setting: { _tag: "fill" },
        viewport: { width: 180, height: 120 },
      }).viewport,
    ).toEqual({ width: 180, height: 120 });
  });
});

describe("preview automation tab targeting", () => {
  it("accepts an explicit tab and rejects contradictory open behavior", () => {
    expect(decodeResizeInput({ tabId: "tab-app", mode: "fill" })).toMatchObject({
      tabId: "tab-app",
      mode: "fill",
    });
    expect(decodeOpenInput({ tabId: "tab-app", reuseExistingTab: true })).toMatchObject({
      tabId: "tab-app",
      reuseExistingTab: true,
    });
    expect(() => decodeOpenInput({ tabId: "tab-app", reuseExistingTab: false })).toThrow();
  });
});

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

describe("PreviewAutomationError", () => {
  it("preserves a typed non-editable target failure", () => {
    const error = decodeAutomationError({
      _tag: "PreviewAutomationTargetNotEditableError",
      operation: "type",
      environmentId: "environment-1",
      threadId: "thread-1",
      providerSessionId: "provider-session-1",
      providerInstanceId: "codex",
      clientId: "client-1",
      connectionId: "connection-1",
      requestId: "request-1",
      tabId: "tab-1",
      timeoutMs: 1_000,
      remoteTag: "PreviewAutomationTargetNotEditableError",
      remoteMessageLength: 12,
      cause: {},
      selectorKind: "focused-element",
    });

    expect(error._tag).toBe("PreviewAutomationTargetNotEditableError");
    if (error._tag === "PreviewAutomationTargetNotEditableError") {
      expect(error.selectorKind).toBe("focused-element");
      expect(error.message).toBe("Preview automation type requires an editable focused element.");
    }
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

describe("PreviewEvent", () => {
  it("decodes opened", () => {
    const event = decodePreviewEvent({
      type: "opened",
      threadId: "t",
      tabId: "preview-t",
      createdAt: "2026-01-01T00:00:00.000Z",
      serverEpoch: "server-a",
      revision: 1,
      snapshot: {
        threadId: "t",
        tabId: "preview-t",
        navStatus: { _tag: "Idle" },
        canGoBack: false,
        canGoForward: false,
        updatedAt: "2026-01-01T00:00:00.000Z",
      },
    });
    expect(event.type).toBe("opened");
  });

  it("decodes failed with code/description", () => {
    const event = decodePreviewEvent({
      type: "failed",
      threadId: "t",
      tabId: "preview-t",
      createdAt: "2026-01-01T00:00:00.000Z",
      serverEpoch: "server-a",
      revision: 1,
      url: "https://example.com/",
      title: "",
      code: -105,
      description: "ERR_NAME_NOT_RESOLVED",
    });
    expect(event.type).toBe("failed");
    if (event.type === "failed") {
      expect(event.code).toBe(-105);
    }
  });

  it("decodes resized with tab viewport state", () => {
    const event = decodePreviewEvent({
      type: "resized",
      threadId: "t",
      tabId: "preview-t",
      createdAt: "2026-01-01T00:00:00.000Z",
      serverEpoch: "server-a",
      revision: 1,
      snapshot: {
        threadId: "t",
        tabId: "preview-t",
        navStatus: { _tag: "Idle" },
        canGoBack: false,
        canGoForward: false,
        viewport: { _tag: "freeform", width: 1024, height: 768 },
        updatedAt: "2026-01-01T00:00:00.000Z",
      },
    });
    expect(event.type).toBe("resized");
  });

  it("decodes closed without snapshot", () => {
    const event = decodePreviewEvent({
      type: "closed",
      threadId: "t",
      tabId: "preview-t",
      createdAt: "2026-01-01T00:00:00.000Z",
      serverEpoch: "server-a",
      revision: 1,
    });
    expect(event.type).toBe("closed");
  });
});

describe("DiscoveredLocalServer", () => {
  it("decodes a server with process metadata", () => {
    const server = decodeServer({
      host: "localhost",
      port: 5173,
      url: "http://localhost:5173",
      processName: "node",
      pid: 12345,
      terminal: null,
    });
    expect(server.port).toBe(5173);
    expect(server.processName).toBe("node");
  });

  it("decodes a server without process metadata", () => {
    const server = decodeServer({
      host: "localhost",
      port: 3000,
      url: "http://localhost:3000",
      processName: null,
      pid: null,
      terminal: null,
    });
    expect(server.processName).toBeNull();
  });

  it("rejects invalid ports", () => {
    expect(() =>
      decodeServer({
        host: "localhost",
        port: 0,
        url: "http://localhost:0",
        processName: null,
        pid: null,
        terminal: null,
      }),
    ).toThrow();
    expect(() =>
      decodeServer({
        host: "localhost",
        port: 70000,
        url: "http://localhost:70000",
        processName: null,
        pid: null,
        terminal: null,
      }),
    ).toThrow();
  });
});

describe("ConfiguredLocalServerUrls", () => {
  it("bounds the number and length of probe candidates", () => {
    expect(() =>
      decodeConfiguredLocalServerUrls(
        Array.from(
          { length: CONFIGURED_LOCAL_SERVER_URLS_MAX_ITEMS + 1 },
          (_, index) => `http://localhost:${3_000 + index}`,
        ),
      ),
    ).toThrow();
    expect(() =>
      decodeConfiguredLocalServerUrls([`http://localhost/${"a".repeat(PREVIEW_URL_MAX_LENGTH)}`]),
    ).toThrow();
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
