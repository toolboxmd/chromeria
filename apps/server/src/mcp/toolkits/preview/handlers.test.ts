import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import {
  createPendingAttachmentId,
  parseThreadSegmentFromAttachmentId,
} from "../../../attachmentStore.ts";
import * as ServerConfig from "../../../config.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import {
  claimPreviewRecording,
  normalizePreviewOpenInput,
  PreviewStandardToolkitHandlersLive,
} from "./handlers.ts";
import { PreviewStandardToolkit } from "./tools.ts";

const makePreviewHarness = Effect.fnUntraced(function* () {
  const requests: PreviewAutomationBroker.PreviewAutomationInvokeInput[] = [];
  const broker = Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({
    invoke: <A>(request: PreviewAutomationBroker.PreviewAutomationInvokeInput) => {
      requests.push(request);
      return Effect.succeed({
        available: true,
        visible: false,
        tabId: null,
        url: null,
        title: null,
        loading: false,
      } as A);
    },
  });
  const toolkit = yield* PreviewStandardToolkit.pipe(
    Effect.provide(PreviewStandardToolkitHandlersLive.pipe(Layer.provide(broker))),
  );
  const call = <Name extends "preview_open" | "preview_navigate">(
    name: Name,
    input: Parameters<typeof toolkit.handle<Name>>[1],
  ) =>
    toolkit.handle(name, input).pipe(
      Stream.unwrap,
      Stream.runCollect,
      Effect.provide(broker),
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-1"),
        threadId: ThreadId.make("thread-1"),
        providerSessionId: "session-1",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["preview"] as const),
        issuedAt: 1,
      }),
    );
  return { call, requests };
});

describe("preview URL validation", () => {
  it.effect.each(["preview_open", "preview_navigate"] as const)(
    "%s rejects about:blank before dispatch with actionable, URL-free diagnostics",
    (name) =>
      Effect.gen(function* () {
        const { call, requests } = yield* makePreviewHarness();
        const result = yield* call(name, { url: "about:blank" }).pipe(Effect.result);
        expect(requests).toEqual([]);
        expect(result._tag).toBe("Failure");
        if (result._tag !== "Failure") return;
        expect(result.failure._tag).toBe("PreviewAutomationInvalidUrlError");
        expect(result.failure.message).toContain(name);
        expect(result.failure.message).toContain("absolute http(s) URL");
        expect(result.failure.message).toContain("example.com");
        expect(result.failure.message).toContain("localhost:5173");
        expect(result.failure.message).not.toContain("about:blank");
        expect(result.failure.message).toContain(
          name === "preview_open" ? "Omit url to open a blank tab." : "environment-port",
        );
      }),
  );

  it.effect("dispatches valid URLs unchanged, blank opens, and environment targets", () =>
    Effect.gen(function* () {
      const { call, requests } = yield* makePreviewHarness();
      yield* call("preview_open", { url: "example.com" });
      yield* call("preview_open", {});
      yield* call("preview_navigate", { url: "localhost:5173" });
      yield* call("preview_navigate", { target: { kind: "environment-port", port: 5173 } });
      expect(requests.map(({ operation, input }) => ({ operation, input }))).toEqual([
        { operation: "open", input: { url: "example.com", reuseExistingTab: true } },
        { operation: "open", input: { reuseExistingTab: true } },
        { operation: "navigate", input: { url: "localhost:5173" } },
        { operation: "navigate", input: { target: { kind: "environment-port", port: 5173 } } },
      ]);
    }),
  );
});

describe("normalizePreviewOpenInput", () => {
  it("leaves an unstated visibility for the client preference to decide", () => {
    // Filling `open` in here would outrank `browserAutoShowFloatingPreview`,
    // which is desktop-local and cannot be read from the server.
    expect(normalizePreviewOpenInput({})).toEqual({ reuseExistingTab: true });
  });

  it("preserves an explicit background-only opt-out", () => {
    expect(normalizePreviewOpenInput({ open: false })).toEqual({
      open: false,
      reuseExistingTab: true,
      show: false,
    });
  });

  it("supports show as a legacy alias while preferring open", () => {
    expect(normalizePreviewOpenInput({ show: false })).toEqual({
      open: false,
      reuseExistingTab: true,
      show: false,
    });
    expect(normalizePreviewOpenInput({ open: true, show: false })).toEqual({
      open: true,
      reuseExistingTab: true,
      show: true,
    });
  });
});

describe("claimPreviewRecording", () => {
  it.effect("overlapping and repeated claims return the same retained recording", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const uploadedAttachmentId = createPendingAttachmentId(".webm");
      const pendingPath = path.join(config.attachmentsDir, `${uploadedAttachmentId}.webm`);
      yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
      yield* fileSystem.writeFileString(pendingPath, "video!");
      const response = {
        id: "desktop-recording",
        tabId: "tab-1",
        path: "/desktop/recording.webm",
        mimeType: "video/webm",
        sizeBytes: 6,
        createdAt: "2026-09-07T00:00:00.000Z",
        uploadedAttachmentId,
      };
      const claim = claimPreviewRecording(ThreadId.make("thread-1"), response);
      const [first, second] = yield* Effect.all([claim, claim], { concurrency: "unbounded" });
      expect(first).toEqual(second);
      expect(yield* claim).toEqual(first);
      expect(yield* fileSystem.readFileString(first.path)).toBe("video!");
      expect(yield* fileSystem.exists(pendingPath)).toBe(false);
      const wrongThread = yield* claimPreviewRecording(ThreadId.make("thread-2"), response).pipe(
        Effect.result,
      );
      expect(wrongThread._tag).toBe("Failure");
      const wrongPath = yield* claimPreviewRecording(ThreadId.make("thread-1"), {
        ...response,
        uploadedAttachmentId: `../${uploadedAttachmentId}`,
      }).pipe(Effect.result);
      expect(wrongPath._tag).toBe("Failure");
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-preview-recording-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );

  it.effect.each([6, 5])(
    "claims only a complete uploaded recording (reported bytes: %s)",
    (sizeBytes) =>
      Effect.gen(function* () {
        const config = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const uploadedAttachmentId = createPendingAttachmentId(".webm");
        const pendingPath = path.join(config.attachmentsDir, `${uploadedAttachmentId}.webm`);
        yield* fileSystem.makeDirectory(config.attachmentsDir, { recursive: true });
        yield* fileSystem.writeFileString(pendingPath, "video!");
        const response = {
          id: "desktop-recording",
          tabId: "tab-1",
          path: "/desktop/recording.webm",
          mimeType: "video/webm",
          sizeBytes,
          createdAt: "2026-09-07T00:00:00.000Z",
          uploadedAttachmentId,
        };
        const result = yield* claimPreviewRecording(ThreadId.make("thread-1"), response).pipe(
          Effect.result,
        );
        if (sizeBytes === 6) {
          expect(result._tag).toBe("Success");
          if (result._tag !== "Success") return;
          expect(result.success.path).not.toBe(response.path);
          expect(parseThreadSegmentFromAttachmentId(result.success.id)).toBe("thread-1");
          expect(yield* fileSystem.readFileString(result.success.path)).toBe("video!");
          expect(yield* fileSystem.exists(pendingPath)).toBe(false);
        } else {
          expect(result._tag).toBe("Failure");
          if (result._tag !== "Failure") return;
          expect(result.failure._tag).toBe("PreviewAutomationRecordingTransferError");
          expect(yield* fileSystem.exists(pendingPath)).toBe(true);
        }
      }).pipe(
        Effect.provide(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-preview-recording-" }).pipe(
            Layer.provideMerge(NodeServices.layer),
          ),
        ),
      ),
  );

  it.effect("reports an older desktop without returning its inaccessible path", () =>
    Effect.gen(function* () {
      const result = yield* claimPreviewRecording(ThreadId.make("thread-1"), {
        id: "desktop-recording",
        tabId: "tab-1",
        path: "/desktop/recording.webm",
        mimeType: "video/webm",
        sizeBytes: 6,
        createdAt: "2026-09-07T00:00:00.000Z",
      }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag !== "Failure") return;
      expect(result.failure._tag).toBe("PreviewAutomationRecordingDesktopUpdateRequiredError");
      expect(result.failure.message).toContain("Update the desktop app");
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-preview-recording-" }).pipe(
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
});
