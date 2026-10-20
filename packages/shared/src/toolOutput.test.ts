import { describe, expect, it } from "vite-plus/test";

import {
  compactDynamicToolOutput,
  resourcesFromToolItem,
  MAX_TOOL_OUTPUT_IMAGES,
  toolOutputImages,
  toolOutputIndicatesFailure,
} from "./toolOutput.ts";

describe("compactDynamicToolOutput", () => {
  const previewResource = {
    type: "preview",
    environmentId: "environment-b",
    threadId: "thread-b",
    tabId: "tab-1",
    serverEpoch: "epoch-1",
  };
  const deviceResource = {
    type: "device",
    environmentId: "environment-b",
    threadId: "thread-b",
    hostId: "local",
    deviceId: "device-1",
    platform: "ios",
    toolSessionId: "12345678-1234-4234-8234-123456789abc",
  };
  const assetResource = {
    _tag: "peer-attachment",
    threadId: "thread-b",
    reference: {
      environmentId: "environment-b",
      attachmentId: "attachment-1",
      owner: "a".repeat(64),
      digest: "b".repeat(64),
      name: "capture.png",
      mimeType: "image/png",
      sizeBytes: 68,
    },
  };

  const downloadResource = {
    _tag: "native-browser-download",
    resource: previewResource,
    downloadId: "550e8400-e29b-41d4-a716-446655440000",
    name: "report.txt",
    mimeType: "application/octet-stream",
    sizeBytes: 1024 * 1024 * 1024,
    digest: "c".repeat(64),
  };
  it("keeps only canonical browser download metadata within the existing compaction budget", () => {
    const output = {
      downloads: [{ resource: downloadResource, path: "/private/b", url: "http://private/token" }],
    };
    expect(compactDynamicToolOutput(output)).toEqual({ resources: [downloadResource] });
    expect(compactDynamicToolOutput(compactDynamicToolOutput(output))).toEqual({
      resources: [downloadResource],
    });
    expect(resourcesFromToolItem({ status: "failed", output })).toEqual([]);
    expect(
      compactDynamicToolOutput({
        downloads: [{ resource: { ...downloadResource, path: "/private/b" } }],
      }),
    ).toBeUndefined();
  });

  it("retains up to twenty bounded downloads, deduplicating the current tab across envelopes", () => {
    const epoch = "550e8400-e29b-41d4-a716-446655440000";
    const browser = {
      ...previewResource,
      environmentId: "b33f8400-e29b-41d4-a716-446655440000",
      threadId:
        "thread:delegated-task:command%3Amcp%3A85cbf26d-4b23-45fc-92db-01903bb425f1%3Adelegate-task%3A379-383-publication-authority-fix-sol-medium-v1",
      tabId: `tab-1_${epoch}`,
      serverEpoch: epoch,
    };
    for (let count = 1; count <= 20; count++) {
      const downloads = Array.from({ length: count }, (_, index) => ({
        ...downloadResource,
        resource: browser,
        downloadId: `550e8400-e29b-41d4-a716-${String(index).padStart(12, "0")}`,
      }));
      const expected = { resources: [browser, ...downloads] };
      expect(new TextEncoder().encode(JSON.stringify(expected)).byteLength).toBeLessThanOrEqual(
        16_384,
      );
      const status = {
        resource: browser,
        tabs: [{ resource: browser, path: "/private/tab" }],
        downloads: downloads.map((resource) => ({ resource, path: "/private/download" })),
      };
      for (const output of [status, { structuredContent: status }, JSON.stringify(status)]) {
        const compact = compactDynamicToolOutput(output);
        expect(compact).toEqual(expected);
        expect(compactDynamicToolOutput(compact)).toEqual(expected);
        expect(resourcesFromToolItem({ status: "completed", output: compact })).toEqual(
          expected.resources,
        );
      }
    }
  });

  it("rejects malformed or private resources after a valid multi-download prefix", () => {
    const valid = Array.from({ length: 20 }, () => downloadResource);
    for (const invalid of [
      { ...downloadResource, digest: "invalid" },
      { ...downloadResource, ticket: "private" },
      { ...downloadResource, resource: { ...previewResource, path: "/private" } },
      { ...downloadResource, extra: { nested: { deeper: { value: "private" } } } },
      { ...downloadResource, name: "x".repeat(16_385) },
    ]) {
      expect(compactDynamicToolOutput({ resources: [...valid, invalid] })).toBeUndefined();
    }
    expect(compactDynamicToolOutput({ resources: valid })).toEqual({
      resources: [downloadResource],
    });
    expect(
      compactDynamicToolOutput({ resources: Array.from({ length: 33 }, () => downloadResource) }),
    ).toBeUndefined();
    // Deduplication must not let repeated candidate scans escape the byte cap.
    expect(
      compactDynamicToolOutput({
        resources: Array.from({ length: 32 }, () => ({
          ...previewResource,
          environmentId: "e".repeat(512),
        })),
      }),
    ).toBeUndefined();
  });

  it("keeps canonical qualified resources through envelopes and repeated compaction", () => {
    for (const resource of [previewResource, deviceResource, assetResource]) {
      const metadata = { resources: [resource] };
      for (const value of [
        { resource, ticket: "private", response: "private" },
        { structuredContent: { resource }, content: "private" },
        { content: [{ text: JSON.stringify({ resource }) }] },
        metadata,
      ]) {
        const compact = compactDynamicToolOutput(value);
        expect(compact).toEqual(metadata);
        expect(compactDynamicToolOutput(compact)).toEqual(metadata);
      }
    }
    expect(compactDynamicToolOutput({ tabs: [{ resource: previewResource }] })).toEqual({
      resources: [previewResource],
    });
    expect(
      compactDynamicToolOutput({
        type: "batch",
        results: [
          { type: "artifact", resource: assetResource },
          { type: "artifacts", resources: [assetResource] },
        ],
      }),
    ).toEqual({ resources: [assetResource] });
    expect(
      resourcesFromToolItem({ status: "completed", output: { resource: previewResource } }),
    ).toEqual([previewResource]);
    expect(
      resourcesFromToolItem({ status: "failed", output: { resource: previewResource } }),
    ).toEqual([]);
    expect(
      resourcesFromToolItem({ status: "cancelled", output: { resource: previewResource } }),
    ).toEqual([]);
  });

  it("does not turn failed, malformed or unbounded resource output into navigation", () => {
    for (const resource of [
      { ...previewResource, tabId: "" },
      { ...previewResource, ticket: "private" },
      { ...deviceResource, toolSessionId: "guessed" },
      { ...assetResource, reference: { ...assetResource.reference, digest: "invalid" } },
      { ...previewResource, serverEpoch: "x".repeat(1_000_000) },
    ])
      expect(compactDynamicToolOutput({ resource })).toBeUndefined();
    expect(compactDynamicToolOutput({ isError: true, resource: previewResource })).toEqual({
      isError: true,
    });
    expect(
      compactDynamicToolOutput({
        content: [
          { text: JSON.stringify({ resource: previewResource }) },
          { text: JSON.stringify({ isError: true }) },
        ],
      }),
    ).toEqual({ isError: true });
    const cyclic = { ...assetResource, reference: { ...assetResource.reference, extra: {} } };
    cyclic.reference.extra = cyclic;
    expect(compactDynamicToolOutput({ resource: cyclic })).toBeUndefined();
  });

  it("bounds final metadata after resource extraction from structured objects", () => {
    const resource = {
      ...previewResource,
      environmentId: "e".repeat(4_100),
      threadId: "t".repeat(4_100),
    };
    expect(compactDynamicToolOutput({ taskId: "task-1", resource })).toEqual({
      taskId: "task-1",
      resources: [resource],
    });
    const resources = Array.from({ length: 32 }, (_, index) => ({
      ...previewResource,
      tabId: `tab-${index}`,
      threadId: "t".repeat(256),
      environmentId: "e".repeat(256),
    }));
    const output = compactDynamicToolOutput({ taskId: "task-1", resources });
    expect(new TextEncoder().encode(JSON.stringify(output)).byteLength).toBeLessThanOrEqual(8_192);
    // Dropping the whole list avoids suggesting that an incomplete list is complete.
    expect(output).toEqual({ taskId: "task-1" });
    expect(
      compactDynamicToolOutput({ resources: Array.from({ length: 33 }, () => previewResource) }),
    ).toBeUndefined();
  });

  it("enforces the exact resource metadata ceiling in serialized UTF-8 bytes", () => {
    const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
    const empty = { resources: [{ ...previewResource, threadId: "" }] };
    const remaining = 16_384 - bytes(empty);
    const resource = {
      ...previewResource,
      threadId: "é".repeat(Math.floor(remaining / 2)) + "x".repeat(remaining % 2),
    };
    const metadata = { resources: [resource] };
    expect(bytes(metadata)).toBe(16_384);
    expect(compactDynamicToolOutput(metadata)).toEqual(metadata);
    expect(compactDynamicToolOutput(JSON.stringify(metadata))).toEqual(metadata);
    expect(compactDynamicToolOutput(compactDynamicToolOutput(metadata))).toEqual(metadata);
    const beyond = { resources: [{ ...resource, threadId: resource.threadId + "x" }] };
    expect(bytes(beyond)).toBe(16_385);
    expect(compactDynamicToolOutput(beyond)).toBeUndefined();
    expect(compactDynamicToolOutput(JSON.stringify(beyond))).toBeUndefined();
    expect(compactDynamicToolOutput({ ...beyond, taskId: "retained" })).toEqual({
      taskId: "retained",
    });
  });

  it("keeps the exact non-resource metadata ceiling at 8192 UTF-8 bytes", () => {
    const batch = (target: number) => {
      const metadata = { threads: Array.from({ length: 32 }, () => ({ threadId: "x" })) };
      let remaining = target - new TextEncoder().encode(JSON.stringify(metadata)).byteLength;
      for (const entry of metadata.threads) {
        const count = Math.min(remaining, 255);
        entry.threadId += "x".repeat(count);
        remaining -= count;
      }
      expect(remaining).toBe(0);
      return metadata;
    };
    const atLimit = batch(8_192);
    expect(new TextEncoder().encode(JSON.stringify(atLimit)).byteLength).toBe(8_192);
    expect(compactDynamicToolOutput(atLimit)).toEqual(atLimit);
    const beyond = batch(8_193);
    expect(new TextEncoder().encode(JSON.stringify(beyond)).byteLength).toBe(8_193);
    expect(compactDynamicToolOutput(beyond)).toBeUndefined();
  });

  it("extracts IDs through the MCP result envelopes used by T3 summaries", () => {
    const metadata = { threadId: "thread-1", messageId: "message-1" };
    const json = JSON.stringify({ ...metadata, response: "Private response body" });
    for (const value of [
      { ...metadata, response: "Private response body" },
      { structuredContent: JSON.parse(json), content: "Ignored alternative body" },
      [{ type: "text", text: json }],
      { content: [{ text: { text: json } }], isError: false },
      json,
    ]) {
      expect(compactDynamicToolOutput(value)).toEqual(metadata);
    }
  });

  it("keeps nested thread identity and scalar task IDs without result bodies", () => {
    const output = {
      thread: { threadId: "thread-1", messages: ["Private message"] },
      taskId: "task-1",
      scheduledTaskId: "scheduled-1",
      status: "failed",
      summary: "A child failed; the tool itself succeeded.",
    };
    expect(compactDynamicToolOutput(output)).toEqual({
      thread: { threadId: "thread-1" },
      taskId: "task-1",
      scheduledTaskId: "scheduled-1",
    });
    expect(output.thread.messages).toEqual(["Private message"]);
  });

  it("keeps explicit envelope failure while dropping error text and arbitrary output", () => {
    for (const failure of [
      { isError: true },
      { is_error: true },
      { _tag: "OrchestratorMcpFailure" },
      { error: { message: "Private error" } },
    ]) {
      expect(
        compactDynamicToolOutput({
          ...failure,
          structuredContent: { threadId: "thread-1", error: "Private details" },
        }),
      ).toEqual({ isError: true, threadId: "thread-1" });
    }
    expect(compactDynamicToolOutput("Private plain text")).toBeUndefined();
    expect(compactDynamicToolOutput({ result: { body: "Private result" } })).toBeUndefined();
  });

  it("uses the first result data but combines explicit failure flags across content blocks", () => {
    expect(
      compactDynamicToolOutput([
        { text: JSON.stringify({ threadId: "first" }) },
        { text: JSON.stringify({ threadId: "second", isError: true, error: "Private error" }) },
      ]),
    ).toEqual({ threadId: "first", isError: true });
    expect(
      compactDynamicToolOutput([{ text: "{}" }, { text: JSON.stringify({ threadId: "second" }) }]),
    ).toBeUndefined();
  });

  it("keeps complete thread creation batches and rollback markers without row bodies", () => {
    expect(
      compactDynamicToolOutput({
        threads: [
          { threadId: "created", status: "running", messages: ["Private message"] },
          { threadId: "reverted", status: "rolled_back", error: "Private error" },
          { status: "rolled_back" },
        ],
      }),
    ).toEqual({
      threads: [
        { threadId: "created" },
        { threadId: "reverted", status: "rolled_back" },
        { status: "rolled_back" },
      ],
    });
    expect(compactDynamicToolOutput({ threadId: "reverted", status: "rolled_back" })).toEqual({
      threadId: "reverted",
      status: "rolled_back",
    });
    expect(compactDynamicToolOutput({ threads: [] })).toEqual({ threads: [] });
  });

  it("omits incomplete or oversized creation evidence instead of returning a partial count", () => {
    for (const threads of [
      [{ threadId: "known" }, { title: "No confirmed ID" }],
      Array.from({ length: 101 }, (_, index) => ({ threadId: `thread-${index}` })),
      Array.from({ length: 100 }, (_, index) => ({ threadId: `${index}`.padEnd(256, "x") })),
      [{ threadId: "x".repeat(257) }],
    ]) {
      expect(
        compactDynamicToolOutput({
          threadId: "must-not-be-counted-as-one",
          status: "rolled_back",
          taskId: "task-1",
          threads,
        }),
      ).toEqual({ taskId: "task-1" });
    }
    const normalBatch = {
      threads: Array.from({ length: 100 }, (_, index) => ({
        threadId: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      })),
    };
    expect(compactDynamicToolOutput(normalBatch)).toEqual(normalBatch);
    expect(
      new TextEncoder().encode(JSON.stringify(compactDynamicToolOutput(normalBatch))).byteLength,
    ).toBeLessThanOrEqual(8_192);
  });

  it("bounds parsing and envelope scanning while preserving an outer failure marker", () => {
    const oversizedJson = JSON.stringify({ threadId: "hidden", body: "😄".repeat(5_000) });
    for (const content of [
      "x".repeat(1_000_000),
      oversizedJson,
      Array.from({ length: 33 }, () => ({ text: JSON.stringify({ threadId: "hidden" }) })),
      { content: { content: { content: { content: { threadId: "too-deep" } } } } },
    ]) {
      expect(compactDynamicToolOutput({ isError: true, content })).toEqual({ isError: true });
    }
    expect(compactDynamicToolOutput({ threadId: "known", body: "x".repeat(1_000_000) })).toEqual({
      threadId: "known",
    });
  });
});

describe("toolOutputIndicatesFailure", () => {
  it("preserves the command failure phrases across shells without copying the output", () => {
    for (const text of [
      "FILE NOT FOUND",
      "No files found",
      "ENOENT",
      "No such file or directory",
      "CommandNotFoundException",
      "command not found",
      "Cannot find path 'a' because it does not exist",
      "The term 'example' is not recognized",
      "is not recognized as the name of a cmdlet",
      "A parameter cannot be found that matches parameter name",
      "<exited with exit code 2>",
      "Exited with exit code 1",
      "exit code: 127",
    ])
      expect(toolOutputIndicatesFailure(text)).toBe(true);
  });

  it("does not mark successful exit codes or incomplete failure phrases", () => {
    for (const text of [
      "Done",
      "exit code: 0",
      "Exited with exit code 0",
      "cannot find path",
      "is not recognized",
    ]) {
      expect(toolOutputIndicatesFailure(text)).toBe(false);
    }
  });
});

describe("toolOutputImages", () => {
  const mcpImage = { type: "image", data: "AAAA", mimeType: "image/png" };
  const claudeImage = {
    type: "image",
    source: { type: "base64", media_type: "image/JPEG", data: "BBBB" },
  };
  const text = { type: "text", text: "{}" };

  it("finds images in each place providers store them, in order", () => {
    expect(toolOutputImages({ content: [text, claudeImage, mcpImage] })).toEqual([
      { mimeType: "image/jpeg", data: "BBBB" },
      { mimeType: "image/png", data: "AAAA" },
    ]);
    expect(toolOutputImages([mcpImage])).toEqual([{ mimeType: "image/png", data: "AAAA" }]);
    expect(toolOutputImages(mcpImage)).toEqual([{ mimeType: "image/png", data: "AAAA" }]);
  });

  it("skips types the server must not serve inline", () => {
    expect(
      toolOutputImages([
        { ...mcpImage, mimeType: "image/svg+xml" },
        { ...mcpImage, mimeType: "text/html" },
        { type: "image", source: { type: "url", url: "https://example.com/a.png" } },
      ]),
    ).toEqual([]);
  });

  it("stops after the first few images", () => {
    const images = toolOutputImages(Array.from({ length: 10_000 }, () => mcpImage));
    expect(images).toHaveLength(MAX_TOOL_OUTPUT_IMAGES);
  });
});
