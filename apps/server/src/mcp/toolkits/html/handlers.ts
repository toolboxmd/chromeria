import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as HtmlRender from "../../../htmlRender/HtmlRender.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { HtmlPreviewToolkit, HtmlRenderToolkit, type HtmlToolkit } from "./tools.ts";

const INVALID_PAGE_ERRORS = new Set([
  "HtmlRenderImagesNotFoundError",
  "HtmlRenderImageTooLargeError",
  "HtmlRenderPageTooLargeError",
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

const handlers = {
  // The headless browser runs on the host and can open local files, so only
  // agents T3 launched, which already work on this machine, get it.
  html_preview: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const htmlRender = yield* HtmlRender.HtmlRender;
      const { png, ...preview } = yield* htmlRender.preview(input).pipe(Effect.mapError(toFailure));
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
        const reference = yield* htmlRender
          .publish({ threadId: thread.threadId, ...input })
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
