import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as HtmlRender from "../../../htmlRender/HtmlRender.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { loadCaller } from "../../threadAccess.ts";
import { HtmlPreviewToolkit, HtmlRenderToolkit, type HtmlToolkit } from "./tools.ts";

const INVALID_PAGE_ERRORS = new Set([
  "HtmlRenderFileError",
  "HtmlRenderFileTooLargeError",
  "HtmlRenderImagesNotFoundError",
  "HtmlRenderImageTooLargeError",
  "HtmlRenderPageTooLargeError",
  "HtmlRenderViewportHeightError",
]);

// Every HTML render error message is built on the server and tells the agent what to do next.
const isMcpFailure = Schema.is(OrchestratorMcpFailure);
const toFailure = (error: { readonly _tag: string; readonly message: string }) =>
  isMcpFailure(error)
    ? error
    : new OrchestratorMcpFailure({
        code: INVALID_PAGE_ERRORS.has(error._tag) ? "invalid_request" : "orchestration_error",
        message: error.message,
      });

// The server reads the file for the caller, so only a thread that reads files without asking may
// pass a path; a supervised thread passes the HTML its provider already let it read.
const pageSource = Effect.fn("HtmlToolkit.pageSource")(function* (input: {
  readonly html?: string | undefined;
  readonly path?: string | undefined;
}) {
  if (input.html !== undefined && input.path === undefined) {
    return { html: input.html } satisfies HtmlRender.HtmlRenderSource;
  }
  if (input.path === undefined || input.html !== undefined) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "Pass exactly one of html or path.",
    });
  }
  const scope = yield* McpInvocationContext.McpInvocationContext;
  if (scope.client?.linked === true) {
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "Pass html: a path is read only on the caller's own environment.",
    });
  }
  const { limits } = yield* loadCaller();
  if (limits.runtimeMode !== "full-access") {
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Pass html instead of path: only full-access threads may pass a path.",
    });
  }
  return { path: input.path } satisfies HtmlRender.HtmlRenderSource;
});

const handlers = {
  // Local-file access limits this host browser to T3-launched agents.
  html_preview: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const htmlRender = yield* HtmlRender.HtmlRender;
      const source = yield* pageSource(input);
      const { png, ...preview } = yield* htmlRender
        .preview({ ...source, width: input.width, appearance: input.appearance })
        .pipe(Effect.mapError(toFailure));
      return {
        ...preview,
        screenshot: {
          mimeType: "image/png" as const,
          data: png,
          width: preview.width,
          height: preview.capturedHeight,
        },
      };
    }),
  ),
  // Linked HTML prepares destination resources; the originating live thread owns publication.
  html_render: McpToolAccess.actsOnResource(
    "thread-resource",
    (input) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const { thread } = yield* McpInvocationContext.requireThreadScope(scope, "html_render");
        const htmlRender = yield* HtmlRender.HtmlRender;
        if (scope.client?.linked === true) {
          if (input.html === undefined || input.path !== undefined)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Pass html: a path is read only on the caller's own environment.",
            });
          const preparedHtml = yield* htmlRender
            .prepare(input.html)
            .pipe(Effect.mapError(toFailure));
          yield* McpToolAccess.recheckResource("thread-resource");
          return {
            preparedHtml,
            title: input.title,
            height: input.height,
          };
        }
        const source = yield* pageSource(input);
        const reference = yield* htmlRender
          .publish({
            ...source,
            threadId: thread.threadId,
            title: input.title,
            height: input.height,
          })
          .pipe(Effect.mapError(toFailure));
        return {
          htmlRender: reference,
          message:
            "Shown to the reader above your reply. Don't mention or describe the page; reply with only what it doesn't already say.",
        };
      }),
    "external-unknown",
  ),
} satisfies McpToolAccess.Handlers<typeof HtmlToolkit.tools>;

export const layerPreview = McpToolAccess.toLayer(HtmlPreviewToolkit, {
  html_preview: handlers.html_preview,
});

export const layerRender = McpToolAccess.toLayer(HtmlRenderToolkit, {
  html_render: handlers.html_render,
});
