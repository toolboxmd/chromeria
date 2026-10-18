import { CurrentRequest } from "../../McpRequestReceipts.ts";
import { loadCaller } from "../../threadAccess.ts";
import { assertSameLink } from "../../linkOrigin.ts";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import {
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  PREVIEW_RECORDING_STOP_TIMEOUT_MS,
  PreviewAutomationRecordingTransferError,
  PreviewAutomationRecordingDesktopUpdateRequiredError,
  PreviewAutomationRecordingArtifact,
  OrchestratorMcpFailure,
  type ToolActivityIcon,
  type ThreadId,
  type PreviewAutomationOperation,
  type PreviewAutomationEndLoginResult,
  type PreviewAutomationFillCredentialResult,
  type PreviewAutomationOpenInput,
  type PreviewAutomationRecordingStatus,
  type PreviewAutomationResizeResult,
  type PreviewAutomationSelectResult,
  type PreviewAutomationSetColorSchemeResult,
  type PreviewAutomationSnapshot,
  type PreviewAutomationStatus,
  type PreviewTabId,
} from "@t3tools/contracts";

import {
  parseAttachmentUuid,
  parseAttachmentFileExtension,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  toSafeThreadAttachmentSegment,
} from "../../../attachmentStore.ts";
import { resolveAttachmentRelativePath } from "../../../attachmentPaths.ts";
import * as ServerConfig from "../../../config.ts";
import { recordCredentialFill } from "../../../preview/CredentialAudit.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as AttachmentStaging from "../../../assets/AttachmentStaging.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import { PreviewSnapshotToolkit, PreviewStandardToolkit, PreviewToolkit } from "./tools.ts";

/** Whether a preview the agent
 * said nothing about surfaces is the user's `browserAutoShowFloatingPreview`
 * preference, which is desktop-local and unreadable from here — filling in
 * `true` would silently override it for every `preview_open`.
 */
export function normalizePreviewOpenInput(
  input: PreviewAutomationOpenInput,
): PreviewAutomationOpenInput {
  const open = input.open ?? input.show;
  return {
    ...input,
    ...(open === undefined ? {} : { open, show: open }),
    reuseExistingTab: input.reuseExistingTab ?? true,
  };
}

const invoke = Effect.fn("PreviewToolkit.invoke")(function* <A>(
  operation: PreviewAutomationOperation,
  input: unknown,
  timeoutMs?: number,
  tabId?: PreviewTabId,
): Effect.fn.Return<
  { result: A; toolIcon?: ToolActivityIcon },
  | import("@t3tools/contracts").PreviewAutomationError
  | import("@t3tools/contracts").OrchestratorMcpFailure,
  McpInvocationContext.McpInvocationContext | PreviewAutomationBroker.PreviewAutomationBroker
> {
  const scope = yield* McpInvocationContext.requireThreadMcpCapability("preview");
  const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
  let targetTabId = tabId;
  const result = yield* broker.invoke<A>({
    onTargetTab: (resolvedTabId) => {
      targetTabId = resolvedTabId;
    },
    scope,
    operation,
    input,
    ...(timeoutMs === undefined ? {} : { timeoutMs }),
    ...(tabId === undefined ? {} : { tabId }),
  });
  if (["status", "open", "navigate", "snapshot", "recordingStop"].includes(operation))
    return { result };
  const statusTabId =
    (operation !== "evaluate" && typeof result === "object" && result !== null
      ? (result as { tabId?: PreviewTabId }).tabId
      : undefined) ?? targetTabId;
  const page = yield* broker
    .invoke<PreviewAutomationStatus>({
      scope,
      operation: "status",
      input: {},
      timeoutMs: 500,
      updateCurrentTab: false,
      ...(statusTabId === undefined ? {} : { tabId: statusTabId }),
    })
    .pipe(Effect.orElseSucceed(() => null));
  return {
    result,
    ...(page?.url && /^https?:\/\//i.test(page.url) && page.url.length <= 4096
      ? { toolIcon: { _tag: "website" as const, pageUrl: page.url } }
      : {}),
  };
});

const invokeTargeted = <A extends object>(
  operation: PreviewAutomationOperation,
  input: {
    readonly tabId?: PreviewTabId | undefined;
    readonly [key: string]: unknown;
  },
  timeoutMs?: number,
) => {
  const { tabId, ...operationInput } = input;
  return invoke<A>(operation, operationInput, timeoutMs, tabId).pipe(
    Effect.map(({ result, toolIcon }) => ({
      ...result,
      ...(toolIcon ? { toolIcon } : {}),
    })),
  );
};

const UploadedRecordingArtifact = Schema.Struct({
  ...PreviewAutomationRecordingArtifact.fields,
  uploadedAttachmentId: Schema.optional(Schema.String),
});
const decodeUploadedRecordingArtifact = Schema.decodeUnknownEffect(UploadedRecordingArtifact);

export const claimPreviewRecording = Effect.fn("PreviewToolkit.claimRecording")(function* (
  threadId: ThreadId,
  response: unknown,
) {
  const artifact = yield* decodeUploadedRecordingArtifact(response).pipe(
    Effect.mapError(
      (cause) =>
        new PreviewAutomationRecordingTransferError({
          threadId,
          cause,
        }),
    ),
  );
  if (!artifact.uploadedAttachmentId) {
    return yield* new PreviewAutomationRecordingDesktopUpdateRequiredError({ threadId });
  }
  const config = yield* ServerConfig.ServerConfig;
  const uuid = parseAttachmentUuid(artifact.uploadedAttachmentId);
  const extension = parseAttachmentFileExtension(artifact.uploadedAttachmentId);
  const threadSegment = toSafeThreadAttachmentSegment(threadId);
  const pendingId = `${PENDING_ATTACHMENT_THREAD_SEGMENT}-${uuid}-${extension}`;
  if (!uuid || !extension || !threadSegment || artifact.uploadedAttachmentId !== pendingId) {
    return yield* new PreviewAutomationRecordingTransferError({
      threadId,
    });
  }
  const finalId = `${threadSegment}-${uuid}-${extension}`;
  const currentPath = resolveAttachmentRelativePath({
    attachmentsDir: config.attachmentsDir,
    relativePath: `${pendingId}.${extension}`,
  });
  const finalPath = resolveAttachmentRelativePath({
    attachmentsDir: config.attachmentsDir,
    relativePath: `${finalId}.${extension}`,
  });
  if (!currentPath || !finalPath) {
    return yield* new PreviewAutomationRecordingTransferError({ threadId });
  }
  const { uploadedAttachmentId: _uploadedAttachmentId, ...recording } = artifact;
  const attachment = {
    type: "file" as const,
    id: finalId,
    name: `recording.${extension}`,
    mimeType: artifact.mimeType,
    sizeBytes: artifact.sizeBytes,
  };
  const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
  const scope = yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext);
  if (Option.isSome(staging) && Option.isSome(scope)) {
    if (scope.value.thread?.threadId !== threadId)
      return yield* new PreviewAutomationRecordingTransferError({ threadId });
    const resource = yield* staging.value.claimRecording(scope.value, pendingId, attachment);
    return { ...recording, id: finalId, path: finalPath, resource };
  }
  // Minimal legacy compositions have no durable acceptance evidence. First
  // publication is exclusive; an existing path is never guessed to be ours.
  const fileSystem = yield* FileSystem.FileSystem;
  yield* Effect.gen(function* () {
    const bytes = yield* fileSystem.readFile(currentPath);
    if (
      bytes.byteLength !== artifact.sizeBytes ||
      artifact.sizeBytes < 1 ||
      artifact.sizeBytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES
    )
      return yield* new PreviewAutomationRecordingTransferError({ threadId });
    yield* fileSystem.writeFile(finalPath, bytes, { flag: "wx" });
  }).pipe(
    Effect.mapError((cause) => new PreviewAutomationRecordingTransferError({ threadId, cause })),
  );
  return { ...recording, id: finalId, path: finalPath, resource: undefined };
});

/** Snapshot access is independent of any renderer, page or tab state. */
const authorizePreviewTarget = McpInvocationContext.requireThreadMcpCapability("preview").pipe(
  Effect.andThen(loadCaller()),
  Effect.flatMap((caller) =>
    caller.caller === undefined
      ? Effect.fail(
          new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "Snapshot access needs its authorized calling thread.",
          }),
        )
      : assertSameLink(caller, caller.caller),
  ),
);

export const authorizeSnapshot = authorizePreviewTarget;

const handlers = {
  preview_dialog: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationStatus>("dialog", input),
    "external-unknown",
  ),
  preview_status: McpToolAccess.readsAsCaller((input) =>
    invokeTargeted<PreviewAutomationStatus>("status", input ?? {}),
  ),
  preview_open: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationStatus>("open", normalizePreviewOpenInput(input)),
    "external-unknown",
  ),
  preview_navigate: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationStatus>("navigate", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_resize: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationResizeResult>("resize", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_set_appearance: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationSetColorSchemeResult>("setColorScheme", input),
    "external-unknown",
  ),
  preview_snapshot: McpToolAccess.readsAsCaller(
    (input) => {
      // Output selection and saving are MCP-only; the browser still produces a complete snapshot.
      const { includeImage: _includeImage, save: _save, ...operationInput } = input ?? {};
      return authorizeSnapshot.pipe(
        Effect.andThen(invokeTargeted<PreviewAutomationSnapshot>("snapshot", operationInput)),
      );
    },
    {
      effect: (input) =>
        typeof input === "object" && input !== null && "save" in input && input.save === true
          ? "external-unknown"
          : "read",
    },
  ),
  preview_click: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("click", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_type: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("type", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_hover: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("hover", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_select: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationSelectResult>("select", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_drag: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("drag", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_fill_credential: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.requireThreadMcpCapability("preview");
      // The audit line takes nothing from the page: only the matched vault host.
      const audit = (site: string | null, result: string) =>
        recordCredentialFill({
          environmentId: scope.environmentId,
          threadId: scope.thread.threadId,
          item: input.item,
          field: input.field,
          site,
          result,
        });
      return yield* invokeTargeted<PreviewAutomationFillCredentialResult>(
        "fillCredential",
        input,
        input.timeoutMs,
      ).pipe(
        Effect.tap((result) => audit(result.site, "filled")),
        Effect.tapError((error) =>
          audit(null, error._tag === "PreviewCredentialError" ? error.reason : error._tag),
        ),
      );
    }),
  ),
  preview_end_login: McpToolAccess.actsAsCaller((input) =>
    invokeTargeted<PreviewAutomationEndLoginResult>("endLogin", input ?? {}),
  ),
  preview_upload: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("upload", input, input.timeoutMs),
    "external-unknown",
  ),
  preview_press: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("press", input),
    "external-unknown",
  ),
  preview_scroll: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<object>("scroll", input),
    "external-unknown",
  ),
  preview_evaluate: McpToolAccess.actsAsCaller(
    ({ tabId, ...input }) =>
      invoke<unknown>("evaluate", input, undefined, tabId).pipe(
        Effect.map(({ result, toolIcon }) => ({
          value: result ?? null,
          ...(toolIcon ? { toolIcon } : {}),
        })),
      ),
    "external-unknown",
  ),
  preview_wait_for: McpToolAccess.readsAsCaller((input) =>
    invokeTargeted<object>("waitFor", input, input.timeoutMs),
  ),
  preview_recording_start: McpToolAccess.actsAsCaller(
    (input) => invokeTargeted<PreviewAutomationRecordingStatus>("recordingStart", input ?? {}),
    "external-unknown",
  ),
  preview_recording_stop: McpToolAccess.actsAsCaller(
    (input) =>
      Effect.gen(function* () {
        yield* authorizePreviewTarget;
        const scope = yield* McpInvocationContext.requireThreadMcpCapability("preview");
        const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
        const modern =
          scope.client?.linked === true && scope.authority?.envelope.requestOwner !== undefined;
        const request = yield* CurrentRequest;
        if ((modern || request !== undefined) && Option.isNone(staging))
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message:
              "The durable media reference owner is unavailable; recording stop was not dispatched.",
          });
        const { tabId, ...operationInput } = input;
        const response = yield* invoke<unknown>(
          "recordingStop",
          { ...operationInput, transferToEnvironment: true },
          PREVIEW_RECORDING_STOP_TIMEOUT_MS,
          tabId,
        );
        const { resource, ...artifact } = yield* claimPreviewRecording(
          scope.thread.threadId,
          response.result,
        );
        if (modern && resource !== undefined) {
          return {
            resource,
            tabId: artifact.tabId,
            mimeType: artifact.mimeType,
            sizeBytes: artifact.sizeBytes,
            createdAt: artifact.createdAt,
            ...(response.toolIcon ? { toolIcon: response.toolIcon } : {}),
          };
        }
        return { ...artifact, ...(response.toolIcon ? { toolIcon: response.toolIcon } : {}) };
      }),
    "external-unknown",
  ),
} satisfies McpToolAccess.Handlers<typeof PreviewToolkit.tools>;

const { preview_snapshot, ...standardHandlers } = handlers;

export const layerStandard = McpToolAccess.toLayer(PreviewStandardToolkit, standardHandlers);

export const layerSnapshot = McpToolAccess.toLayer(PreviewSnapshotToolkit, {
  preview_snapshot,
});
