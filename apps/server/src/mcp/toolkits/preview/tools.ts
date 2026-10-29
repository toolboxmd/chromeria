import { PreviewAutomationPressInput, PreviewAutomationScrollInput } from "@t3tools/contracts";
import {
  PreviewAutomationClickInput,
  PreviewAutomationTypeInput,
  PreviewAutomationHoverInput,
  PreviewAutomationSelectInput,
  PreviewAutomationDragInput,
  PreviewAutomationUploadInput,
} from "@t3tools/contracts";
import { McpSavedSnapshotReceipt } from "@t3tools/contracts";
import * as McpRouting from "../../McpRouting.ts";
import {
  OrchestratorMcpFailure,
  PreviewWatchStartInput,
  PreviewWatchStartResult,
  PreviewWatchStopInput,
  PreviewWatchStopResult,
  PreviewWatchReadInput,
  PreviewWatchReadResult,
  ToolActivityIcon,
  PreviewAutomationActInput,
  PreviewAutomationDialogInput,
  PreviewAutomationError,
  PreviewAutomationEvaluateInput,
  PreviewAutomationFillCredentialInput,
  PreviewAutomationFillCredentialResult,
  PreviewAutomationEndLoginResult,
  PreviewAutomationNavigateInput,
  PreviewAutomationOpenInput,
  PreviewAutomationRecordingArtifact,
  PeerAttachmentReference,
  PreviewAutomationRecordingStatus,
  PreviewAutomationResizeInput,
  PreviewAutomationResizeResult,
  PreviewAutomationSelectResult,
  PreviewAutomationSetColorSchemeInput,
  PreviewAutomationSetColorSchemeResult,
  PreviewAutomationSnapshot,
  PreviewAutomationStatus,
  PreviewAutomationTabTargetInput,
  PreviewAutomationWaitForInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { Tool, Toolkit } from "effect/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import * as ServerConfig from "../../../config.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";

const COMPUTER_USE_HINT =
  ' For browser or desktop driving beyond a quick check, call delegate_task with prismRole: "computer-use".';

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  PreviewAutomationBroker.PreviewAutomationBroker,
];

const PreviewToolFailure = Schema.Union([PreviewAutomationError, OrchestratorMcpFailure]);

const presentationFields = { toolIcon: Schema.optional(ToolActivityIcon) };

const PreviewActionResult = Schema.Struct(presentationFields);

const browserTool = <T extends Tool.Any>(tool: T): T =>
  tool.annotate(Tool.OpenWorld, true).annotate(Tool.Destructive, true) as T;

const safeBrowserTool = <T extends Tool.Any>(tool: T): T =>
  tool.annotate(Tool.OpenWorld, true).annotate(Tool.Destructive, false) as T;

const readonlyBrowserTool = <T extends Tool.Any>(tool: T): T =>
  safeBrowserTool(tool).annotate(Tool.Readonly, true).annotate(Tool.Idempotent, true) as T;

const PreviewStatusTool = Tool.make("preview_status", {
  description:
    "Report preview tab state: owner, pending dialog, URL, title, loading, viewport, and size. `tabs` lists this thread's tabs. `profiles` lists logins for `preview_open`. Read any listed tab. Act only on your tab, or an unclaimed tab with no human in control." +
    COMPUTER_USE_HINT,
  parameters: McpRouting.parameters(PreviewAutomationTabTargetInput),
  success: PreviewAutomationStatus,
  failure: PreviewToolFailure,
  dependencies,
})
  .annotate(Tool.Title, "Get preview status")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);

const PreviewOpenTool = browserTool(
  Tool.make("preview_open", {
    description:
      "Open a preview tab in this thread and show it. `open=false` keeps it in the background. Reuses your current tab unless `tabId` or `reuseExistingTab=false` says otherwise. Parallel subagents in one session must each use `reuseExistingTab=false`, then their own returned `tabId`." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationOpenInput),
    success: PreviewAutomationStatus,
    failure: PreviewToolFailure,
    dependencies,
  })
    .annotate(Tool.Title, "Open browser preview")
    .annotate(Tool.Destructive, false),
);

const PreviewDialogTool = browserTool(
  Tool.make("preview_dialog", {
    description:
      "Accept or dismiss the dialog `preview_status` reports. Pass `promptText` to accept a prompt. Needs your own tab, or an unclaimed one. Desktop hosts may not support this." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationDialogInput),
    success: PreviewAutomationStatus,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Resolve browser dialog"),
);

const PreviewNavigateTool = safeBrowserTool(
  Tool.make("preview_navigate", {
    description:
      "Navigate the chosen tab. Pass `url` for a website, or `target:{kind:'environment-port',port}` for a dev server. Exactly one of `url` and `target`." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationNavigateInput),
    success: PreviewAutomationStatus,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Navigate browser preview"),
);

const PreviewResizeTool = safeBrowserTool(
  Tool.make("preview_resize", {
    description:
      "Resize the chosen tab: {mode:'fill'}, {mode:'freeform',width,height}, or {mode:'preset',preset,orientation}. Changes CSS size only, not the user agent." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationResizeInput),
    success: Schema.Struct({ ...PreviewAutomationResizeResult.fields, ...presentationFields }),
    failure: PreviewToolFailure,
    dependencies,
  })
    .annotate(Tool.Title, "Resize browser viewport")
    .annotate(Tool.Idempotent, true),
);

const PreviewSetAppearanceTool = safeBrowserTool(
  Tool.make("preview_set_appearance", {
    description:
      "Set `prefers-color-scheme` for the chosen tab: 'dark', 'light', or 'system' to clear the override." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationSetColorSchemeInput),
    success: Schema.Struct({
      ...PreviewAutomationSetColorSchemeResult.fields,
      ...presentationFields,
    }),
    failure: PreviewToolFailure,
    dependencies,
  })
    .annotate(Tool.Title, "Set preview appearance")
    .annotate(Tool.Idempotent, true),
);

// Not read-only: save=true writes the screenshot to disk.
export const PreviewSnapshotTool = safeBrowserTool(
  Tool.make("preview_snapshot", {
    description:
      "Use it only when the current request asks for browser work, including checking an edit in the browser. Never use it unasked to verify your own edit, or to repeat a snapshot from an earlier turn. Works on a tab the user is driving. Returns page state, elements, diagnostics, and screenshot size. Target one element with `locator='aria-ref=<ref>'` from `accessibilityTree`, even in frames. Refs die on navigation, a new snapshot, or handoff. Text caps near 20 KB; on your own or an unclaimed tab, `preview_evaluate` reads more. To show a screenshot: `save=true`, then embed ![alt](screenshotPath)." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(
      Schema.Struct({
        ...PreviewAutomationTabTargetInput.fields,
        includeImage: Schema.optional(
          Schema.Boolean.annotate({
            description: "Default false. True only to see the page.",
          }),
        ),
        save: Schema.optional(
          Schema.Boolean.annotate({
            description: "Write PNG, return `screenshotPath`.",
          }),
        ),
      }),
    ),
    success: Schema.Union([PreviewAutomationSnapshot, McpSavedSnapshotReceipt]),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Inspect browser page"),
);

const PreviewPressTool = browserTool(
  Tool.make("preview_press", {
    description:
      "Press a key, such as {key:'Enter'} or {key:'a',modifiers:['Meta']}." + COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationPressInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Press key in preview page"),
);

const PreviewScrollTool = safeBrowserTool(
  Tool.make("preview_scroll", {
    description:
      "Scroll the chosen tab, or one container with a locator or selector. Positive `deltaY` scrolls down. Positive `deltaX` scrolls right." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationScrollInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Scroll preview page"),
);

const PreviewClickTool = browserTool(
  Tool.make("preview_click", {
    description:
      "Click exactly one target. Prefer a Playwright locator. `x` and `y` go together." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationClickInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Click preview page"),
);

const PreviewTypeTool = browserTool(
  Tool.make("preview_type", {
    description:
      "Type literal text into one input. `clear=true` replaces the current text." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationTypeInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Type into preview page"),
);

const PreviewHoverTool = safeBrowserTool(
  Tool.make("preview_hover", {
    description: "Hover one target to show menus or tooltips." + COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationHoverInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Hover preview page"),
);

const PreviewSelectTool = browserTool(
  Tool.make("preview_select", {
    description:
      "Pick options in one native <select>, by value or label. For a custom dropdown, click it open, then click the option." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationSelectInput),
    success: Schema.Struct({
      ...PreviewAutomationSelectResult.fields,
      ...presentationFields,
    }),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Select preview option"),
);

const PreviewDragTool = browserTool(
  Tool.make("preview_drag", {
    description: "Drag one element onto another." + COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationDragInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Drag in preview page"),
);

const PreviewUploadTool = browserTool(
  Tool.make("preview_upload", {
    description:
      "Give files to the page. After a click opens a file picker, `preview_status` reports it. Local callers pass absolute `paths`; a linked browser takes `artifacts` from `t3_attachment_inspect` with the exact `resource` from `preview_open` or `preview_status`. One of the two, at most 20 files/100 MiB. An empty list cancels the picker or clears a named input. Or pass a locator for an <input type=file>." +
      COMPUTER_USE_HINT,
    // Routing rebuilds the struct, so the upload input's cross-field filter is checked again here.
    parameters: McpRouting.parameters(PreviewAutomationUploadInput).check(
      Schema.makeFilter(
        (value) =>
          isPreviewAutomationUpload(value) ||
          "Provide either local paths or qualified artifacts for their exact browser resource, within 20 files/100 MiB.",
      ),
    ),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Upload files to preview page"),
);

const PreviewFillCredentialTool = browserTool(
  Tool.make("preview_fill_credential", {
    description:
      "Fill one login input in the tab selected by tabId, or this agent session's current tab when omitted, with a value from this environment's credential vault: the username, the password, or the current one-time code (totp) of a vault item. You never see the value; the result says only whether it was filled. The input's page must be https and one of the item's sites. The fill starts a login for the tab's whole browser profile, even when it fails: no tab in that profile, other agents' included, can be read (no snapshot, evaluate, recording, URL or title) until it ends, so keep logins short. Finish signing in with more fills, preview_click or preview_type with a locator, preview_press and preview_wait_for, then call preview_end_login, which closes the profile's browser with every tab in it; open a new tab to continue, signed in through the profile's saved cookies. After a failed fill, end the login and retry in a new tab. Server browser tabs only, not desktop-hosted ones." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationFillCredentialInput),
    success: Schema.Struct({
      ...PreviewAutomationFillCredentialResult.fields,
      ...presentationFields,
    }),
    failure: PreviewToolFailure,
    dependencies: [...dependencies, FileSystem.FileSystem, Path.Path, ServerConfig.ServerConfig],
  }).annotate(Tool.Title, "Fill credential in preview page"),
);

const PreviewEndLoginTool = browserTool(
  Tool.make("preview_end_login", {
    description:
      "End the login started by preview_fill_credential in the tab selected by tabId, or this agent session's current tab when omitted: closes that browser profile's browser, with every tab in it, including tabs still opening, and returns once it has closed. Then call preview_open for a new tab; the profile's saved cookies keep the session, except cookies the site set without an expiry. Returns the closed tab ids, or none when the tab was not in a login." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationTabTargetInput),
    success: Schema.Struct({
      ...PreviewAutomationEndLoginResult.fields,
      ...presentationFields,
    }),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "End browser login"),
);

const PreviewActTool = browserTool(
  Tool.make("preview_act", {
    description:
      "Act on the chosen tab. Target with `locator='aria-ref=<ref>'` from `preview_snapshot`; `x` and `y` go together. `timeoutMs` defaults to 15000. click: `button`, `clickCount` 1 to 3. type: literal `text`, `clear=true` replaces it. hover: shows menus or tooltips. select: `values` in one native <select>, by value or label; for a custom dropdown, click it open, then click the option. drag: `source` onto `target`. upload: absolute `paths` answer the file picker `preview_status` reports, [] cancels it, or a `locator` sets an <input type=file>. press: one `key`, such as 'Enter', with `modifiers`, to the focused element; click a field first to focus it. scroll: the tab, or one container with a locator; positive `deltaY` scrolls down, positive `deltaX` right." +
      COMPUTER_USE_HINT,
    parameters: PreviewAutomationActInput,
    success: Schema.Union([
      Schema.Struct({ ...PreviewAutomationSelectResult.fields, ...presentationFields }),
      PreviewActionResult,
    ]),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Act in preview page"),
);

/** Wrap non-objects: MCP requires object structuredContent, and Claude Code rejects invalid results entirely. */
export const PreviewEvaluateResult = Schema.Struct({
  ...presentationFields,
  value: Schema.Unknown.annotate({
    description: "The JSON-serializable value the expression produced, or null.",
  }),
});

const PreviewEvaluateTool = browserTool(
  Tool.make("preview_evaluate", {
    description:
      "Run JavaScript in the page. It can change page state. Returns `{value}`, up to 64 KB. Needs your tab, or an unclaimed one. To read a tab the user drives, use `preview_snapshot`. Page tools reached here: read data only, never create, save, or display. Show results with `html_render`." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationEvaluateInput),
    success: PreviewEvaluateResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Evaluate JavaScript in preview"),
);

const PreviewWaitForTool = readonlyBrowserTool(
  Tool.make("preview_wait_for", {
    description:
      "Wait until all given locator, selector, text, and URL conditions match in the chosen tab." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationWaitForInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Wait for preview page condition"),
);

const PreviewRecordingStartTool = safeBrowserTool(
  Tool.make("preview_recording_start", {
    description: "Start recording the chosen tab." + COMPUTER_USE_HINT,
    parameters: PreviewAutomationTabTargetInput,
    success: Schema.Struct({ ...PreviewAutomationRecordingStatus.fields, ...presentationFields }),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Start browser recording"),
);

const PreviewRecordingStopTool = safeBrowserTool(
  Tool.make("preview_recording_stop", {
    description:
      "Stop recording the chosen tab. Returns a local path to the recording file (max 50 MiB). Negotiated peer calls return an environment-qualified resource instead; send it by its `attachmentId`, never as a source path. An unconfirmed durable publication reports `outcome_unknown` and is not repeated." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationTabTargetInput),
    success: Schema.Union([
      Schema.Struct({ ...PreviewAutomationRecordingArtifact.fields, ...presentationFields }),
      Schema.Struct({
        resource: PeerAttachmentReference,
        tabId: PreviewAutomationRecordingArtifact.fields.tabId,
        mimeType: Schema.String,
        sizeBytes: Schema.Int,
        createdAt: Schema.String,
        ...presentationFields,
      }),
    ]),
    failure: PreviewToolFailure,
    dependencies: [...dependencies, FileSystem.FileSystem, ServerConfig.ServerConfig],
  }).annotate(Tool.Title, "Stop browser recording"),
);

export const PreviewToolkit = Toolkit.make(
  PreviewDialogTool,
  PreviewStatusTool,
  PreviewOpenTool,
  PreviewNavigateTool,
  PreviewResizeTool,
  PreviewSetAppearanceTool,
  PreviewSnapshotTool,
  PreviewActTool,
  PreviewPressTool,
  PreviewScrollTool,
  PreviewClickTool,
  PreviewTypeTool,
  PreviewHoverTool,
  PreviewSelectTool,
  PreviewDragTool,
  PreviewUploadTool,
  PreviewFillCredentialTool,
  PreviewEndLoginTool,
  PreviewEvaluateTool,
  PreviewWaitForTool,
  PreviewRecordingStartTool,
  PreviewRecordingStopTool,
);

export const PreviewStandardToolkit = Toolkit.make(
  PreviewDialogTool,
  PreviewStatusTool,
  PreviewOpenTool,
  PreviewNavigateTool,
  PreviewResizeTool,
  PreviewSetAppearanceTool,
  PreviewActTool,
  PreviewPressTool,
  PreviewScrollTool,
  PreviewClickTool,
  PreviewTypeTool,
  PreviewHoverTool,
  PreviewSelectTool,
  PreviewDragTool,
  PreviewUploadTool,
  PreviewFillCredentialTool,
  PreviewEndLoginTool,
  PreviewEvaluateTool,
  PreviewWaitForTool,
  PreviewRecordingStartTool,
  PreviewRecordingStopTool,
);

export const PreviewSnapshotToolkit = Toolkit.make(PreviewSnapshotTool);

export const PreviewWatchToolkit = Toolkit.make(
  browserTool(
    Tool.make("preview_watch_start", {
      description:
        "Subscribe once in an existing owned server tab with a caller-authored (emit) => cleanup function. Emit JSON changes or a baseline using DOM events or an application subscription. No page polling or model calls. Survives turn completion while current authority and the exact opening remain valid. Start cursor is zero, before synchronous baseline events." +
        COMPUTER_USE_HINT,
      parameters: McpRouting.parameters(PreviewWatchStartInput),
      success: PreviewWatchStartResult,
      failure: PreviewToolFailure,
      dependencies,
    }).annotate(Tool.Title, "Subscribe to page changes"),
  ),
  safeBrowserTool(
    Tool.make("preview_watch_stop", {
      description:
        "Stop this exact browser watch and run its cleanup. Requires normal preview action authority in its host thread. Reads afterwards report terminal stopped state; events are never acknowledged by stopping.",
      parameters: McpRouting.parameters(PreviewWatchStopInput),
      success: PreviewWatchStopResult,
      failure: PreviewToolFailure,
      dependencies,
    }).annotate(Tool.Title, "Stop page subscription"),
  ),
  Tool.make("preview_watch_read", {
    description:
      "Read only an exact watch's retained ordered JSON queue, without browser or status requests. Pass the host threadId, watchId, epoch and last processed sequence as after. Reads replay without consuming. Commit the returned cursor atomically with your own work. Empty active reads preserve after; terminal, stale, gap and overflow states error explicitly. Limited read clients need an approved project and an undeleted host thread. Optional bounded wait never polls the page.",
    parameters: McpRouting.parameters(PreviewWatchReadInput),
    success: PreviewWatchReadResult,
    failure: PreviewToolFailure,
    dependencies: [
      McpInvocationContext.McpInvocationContext,
      ThreadManagementService.ThreadManagementService,
    ],
  })
    .annotate(Tool.Title, "Read retained page changes")
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true),
);

const isPreviewAutomationUpload = Schema.is(PreviewAutomationUploadInput);
