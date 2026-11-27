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
  BrowserNavigationTarget,
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
  PreviewEvaluateWorld,
  PreviewAutomationFillCredentialInput,
  PreviewAutomationFillCredentialResult,
  PreviewAutomationSaveSecretInput,
  PreviewAutomationSaveSecretResult,
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
  PreviewAutomationSnapshotMetadata,
  PreviewAutomationStatus,
  PreviewAutomationTabTargetInput,
  PreviewAutomationWaitForInput,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { McpSchema, Tool, Toolkit } from "effect/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import * as ServerConfig from "../../../config.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";

const COMPUTER_USE_HINT =
  ' Browser or desktop driving goes to delegate_task with prismRole: "computer-use"; that role lists the exceptions.';

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  PreviewAutomationBroker.PreviewAutomationBroker,
];

const PreviewToolFailure = Schema.Union([PreviewAutomationError, OrchestratorMcpFailure]);

const presentationFields = { toolIcon: Schema.optional(ToolActivityIcon) };

const PreviewActionResult = Schema.Struct(presentationFields);

const browserTool = <T extends Tool.Any>(tool: T): T =>
  tool
    .annotate(Tool.OpenWorld, true)
    .annotate(Tool.Destructive, true)
    .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients) as T;

const safeBrowserTool = <T extends Tool.Any>(tool: T): T =>
  tool
    .annotate(Tool.OpenWorld, true)
    .annotate(Tool.Destructive, false)
    .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients) as T;

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
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

// `url` already covers websites, so open only adds the dev-server target.
const EnvironmentPortTarget = BrowserNavigationTarget.members[1];
const isPreviewAutomationOpen = Schema.is(PreviewAutomationOpenInput);

const PreviewOpenTool = browserTool(
  Tool.make("preview_open", {
    description:
      "Open a preview tab in this thread and show it. For a dev server, pass `target` instead of `url`. `open=false` keeps it in the background. Reuses your current tab unless `tabId` or `reuseExistingTab=false` says otherwise. Parallel subagents in one session must each use `reuseExistingTab=false`, then their own returned `tabId`." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(
      PreviewAutomationOpenInput.mapFields(
        (fields) => ({ ...fields, target: Schema.optional(EnvironmentPortTarget) }),
        { unsafePreserveChecks: true },
      ),
    ).check(
      Schema.makeFilter(
        (input) =>
          isPreviewAutomationOpen(input) || "tabId cannot be combined with reuseExistingTab=false.",
      ),
    ),
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
      "Navigate the chosen tab. Pass `url` for a website, or `target:{kind:'environment-port',port}` for a dev server. Exactly one of `url` and `target`. On a public website, reach search results and listings through the page itself, with its search box and links, instead of typing their URLs; keep one tab per site." +
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
      "Use it only when the current request asks for browser work, including checking an edit in the browser. Never use it unasked to verify your own edit, or to repeat a snapshot from an earlier turn. Works on a tab the user is driving. Returns page state, elements, diagnostics, and screenshot size. Target one element with `locator='aria-ref=<ref>'` from `accessibilityTree`, even in frames. Refs die on navigation, a new snapshot, or handoff. Text caps near 20 KB; on your own or an unclaimed tab, `preview_evaluate` reads more. To show a screenshot: `save=true`, then embed ![alt](screenshotPath). Save a secret the page shows, such as an API key, with `preview_save_secret` instead of reading it." +
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
    success: Schema.Union([
      PreviewAutomationSnapshot,
      PreviewAutomationSnapshotMetadata,
      McpSavedSnapshotReceipt,
    ]),
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
      "Fill one login input in the tab selected by tabId, or this agent session's current tab when omitted, with a value from this environment's credential vault: the username, the password, or the current one-time code (totp) of a vault item. You never see the value; the result says only whether it was filled. The input's page must be https and one of the item's sites. The page stays readable while you sign in: after each submit, take a snapshot to see what the site asks next, such as an error, \"Try another way\" or a one-time code step, and continue in the same tab. Password fields stay masked in snapshots. Server browser tabs only, not desktop-hosted ones." +
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

const PreviewSaveSecretTool = browserTool(
  Tool.make("preview_save_secret", {
    description:
      "Save the value one element shows, such as an API key on a dashboard, to a private file without it entering your context. Reads the tab selected by tabId, or this agent session's current tab when omitted: an input's or textarea's value, otherwise the element's visible text, one line up to 4 KB. Locate the element with a ref from a snapshot taken before the value was revealed, or a unique selector that does not contain the value (text=/sk_test_/ is fine). The element's page and frame must be https. Returns only the file's path and the value's length. The file is owner-only on this environment's machine: read it inside commands (KEY=\"$(cat <path>)\"), never print it, delete it when done; it expires after 24 hours. This tool keeps the value out of its own result only: after revealing a value, do not snapshot, evaluate or record that page. Server browser tabs only, and not for calls from linked environments." +
      COMPUTER_USE_HINT,
    parameters: McpRouting.parameters(PreviewAutomationSaveSecretInput),
    success: PreviewAutomationSaveSecretResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Save value from preview page"),
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
  world: Schema.optional(PreviewEvaluateWorld),
});

const PreviewEvaluateTool = browserTool(
  Tool.make("preview_evaluate", {
    description:
      "Run JavaScript in the page. It can change page state. Returns `{value, world}`, up to 64 KB. Pass `world:'isolated'` to run where the page cannot see your globals; it shares the DOM but not page variables or bindings such as `document.modelContext`. Needs your tab, or an unclaimed one. To read a tab the user drives, use `preview_snapshot`. Page tools write only on user request. Show results with `html_render`, never page tools. Save a secret the page shows, such as an API key, with `preview_save_secret` instead of reading it." +
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
  PreviewSaveSecretTool,
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
  PreviewSaveSecretTool,
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
