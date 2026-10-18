import { McpSavedSnapshotReceipt } from "@t3tools/contracts";
import * as McpRouting from "../../McpRouting.ts";
import {
  OrchestratorMcpFailure,
  ToolActivityIcon,
  PreviewAutomationClickInput,
  PreviewAutomationDialogInput,
  PreviewAutomationDragInput,
  PreviewAutomationError,
  PreviewAutomationEvaluateInput,
  PreviewAutomationFillCredentialInput,
  PreviewAutomationFillCredentialResult,
  PreviewAutomationEndLoginResult,
  PreviewAutomationHoverInput,
  PreviewAutomationNavigateInput,
  PreviewAutomationOpenInput,
  PreviewAutomationPressInput,
  PreviewAutomationRecordingArtifact,
  PeerAttachmentReference,
  PreviewAutomationRecordingStatus,
  PreviewAutomationResizeInput,
  PreviewAutomationResizeResult,
  PreviewAutomationScrollInput,
  PreviewAutomationSelectInput,
  PreviewAutomationSelectResult,
  PreviewAutomationSetColorSchemeInput,
  PreviewAutomationSetColorSchemeResult,
  PreviewAutomationSnapshot,
  PreviewAutomationStatus,
  PreviewAutomationTabTargetInput,
  PreviewAutomationTypeInput,
  PreviewAutomationUploadInput,
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

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  PreviewAutomationBroker.PreviewAutomationBroker,
];

const PreviewToolFailure = Schema.Union([PreviewAutomationError, OrchestratorMcpFailure]);

const presentationFields = { toolIcon: Schema.optional(ToolActivityIcon) };

const PreviewActionResult = Schema.Struct(presentationFields).annotate({
  description: "The preview action completed successfully.",
});

const browserTool = <T extends Tool.Any>(tool: T): T =>
  tool.annotate(Tool.OpenWorld, true).annotate(Tool.Destructive, true) as T;

const safeBrowserTool = <T extends Tool.Any>(tool: T): T =>
  tool.annotate(Tool.OpenWorld, true).annotate(Tool.Destructive, false) as T;

const readonlyBrowserTool = <T extends Tool.Any>(tool: T): T =>
  safeBrowserTool(tool).annotate(Tool.Readonly, true).annotate(Tool.Idempotent, true) as T;

const PreviewStatusTool = Tool.make("preview_status", {
  description:
    "Report whether a collaborative browser tab is automation-capable, including its control owner, pending dialog, URL, title, visibility, loading state, viewport mode, and measured CSS-pixel size. Pass tabId to inspect a specific tab; omit it to use this agent session's current tab, or the tab the user is viewing when you have none. Server hosts also list every tab in the thread (tabs) with its owner, including tabs the user opened, and the browser profiles preview_open accepts. You can read any listed tab; act only on your own tabs, or on an unclaimed tab while no human controls it.",
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
      "Initialize a collaborative browser tab and open its thread-bound inline preview by default. Set open=false for background-only automation. Pass tabId to reuse a specific existing tab, set reuseExistingTab=false to create another tab, or omit both to use this agent session's current tab. Parallel subagents sharing a provider session must each open with reuseExistingTab=false and pass their returned tabId on every call. Pass profileId (an id or name from preview_status profiles) to open under a browser profile and its saved logins; omit it for the user's default profile. Another agent session's tabs can be read but not operated.",
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
      "Accept or dismiss the server browser dialog reported by preview_status. For a prompt, supply promptText when accepting. Requires this agent to own the tab, or the tab to be unclaimed with no human in control. Desktop hosts may not support this operation.",
    parameters: McpRouting.parameters(PreviewAutomationDialogInput),
    success: PreviewAutomationStatus,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Resolve browser dialog"),
);

const PreviewNavigateTool = safeBrowserTool(
  Tool.make("preview_navigate", {
    description:
      "Navigate a collaborative browser tab. Pass tabId to target a specific tab, plus {url:'https://t3.chat'} for a website or {target:{kind:'environment-port',port:5173}} for a dev server. Exactly one of url or target is required.",
    parameters: McpRouting.parameters(PreviewAutomationNavigateInput),
    success: PreviewAutomationStatus,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Navigate browser preview"),
);

const PreviewResizeTool = safeBrowserTool(
  Tool.make("preview_resize", {
    description:
      "Resize a collaborative browser tab, optionally selected by tabId. Use {mode:'fill'}, {mode:'freeform',width:1024,height:768}, or {mode:'preset',preset:'iphone-12-pro',orientation:'portrait'}. This changes CSS layout breakpoints without changing the desktop browser user agent.",
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
      "Emulate prefers-color-scheme in a collaborative browser tab, optionally selected by tabId. Use {colorScheme:'dark'} or {colorScheme:'light'} to preview the page in that appearance, and {colorScheme:'system'} to clear the override and follow the OS appearance.",
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
      "Inspect a page before interacting. Pass tabId to inspect a specific tab, including one the user opened (see preview_status tabs), even while they control it; omit it to use this agent session's current tab, or the tab the user is viewing when you have none. Returns page state, semantic elements, diagnostics, action history, and screenshot dimensions. Server snapshots include an accessibilityTree with refs; pass locator=aria-ref=<ref> to target one exact element, including inside frames. Refresh refs after navigation, another snapshot, or human takeover. The text is capped near 20 KB and lists what it omitted; use preview_evaluate to read more. Set includeImage=true only when you need to see the page; the image stays in tool history. Negotiated keyed save calls return a bounded historical confirmed-operation receipt with artifact.screenshotPath, digest, MIME, size and environment. The receipt never stores page text or URL. Reusing that key returns the original saved artifact; current inspection uses save=false. Set save=true to write the PNG to disk and get back only the url and screenshotPath. Embed that path in your reply as ![alt](screenshotPath) so the user sees it. This is the only way to show the user a screenshot; the image in the tool result is not saved anywhere.",
    parameters: McpRouting.parameters(
      Schema.Struct({
        ...PreviewAutomationTabTargetInput.fields,
        includeImage: Schema.optional(
          Schema.Boolean.annotate({
            description:
              "Include the PNG image in the tool response. Defaults to false. Set true only when you need to see the page.",
          }),
        ),
        save: Schema.optional(
          Schema.Boolean.annotate({
            description:
              "Write the screenshot PNG to disk and return its absolute path as screenshotPath. Unless includeImage=true, return only the url and screenshotPath. Defaults to false.",
          }),
        ),
      }),
    ),
    success: Schema.Union([PreviewAutomationSnapshot, McpSavedSnapshotReceipt]),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Inspect browser page"),
);

const PreviewClickTool = browserTool(
  Tool.make("preview_click", {
    description:
      "Click exactly one target in the tab selected by tabId, or this agent session's current tab when omitted. Prefer a Playwright locator; selector accepts legacy CSS; x and y must be supplied together. Set button=right for a context menu or clickCount=2 for a double-click; server browser tabs only.",
    parameters: McpRouting.parameters(PreviewAutomationClickInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Click preview page"),
);

const PreviewTypeTool = browserTool(
  Tool.make("preview_type", {
    description:
      "Insert literal text into one input in the tab selected by tabId, or this agent session's current tab when omitted. Prefer a Playwright locator; set clear=true to replace existing text.",
    parameters: McpRouting.parameters(PreviewAutomationTypeInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Type into preview page"),
);

const PreviewHoverTool = safeBrowserTool(
  Tool.make("preview_hover", {
    description:
      "Move the mouse over exactly one target in the tab selected by tabId, or this agent session's current tab when omitted, to reveal hover menus and tooltips. Server browser tabs only.",
    parameters: McpRouting.parameters(PreviewAutomationHoverInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Hover preview page"),
);

const PreviewSelectTool = browserTool(
  Tool.make("preview_select", {
    description:
      "Choose options in one native <select> in the tab selected by tabId, or this agent session's current tab when omitted, by option value or visible label. Custom dropdowns are not <select>; click them open and click the option instead. Server browser tabs only.",
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
    description:
      "Drag one element onto another in the tab selected by tabId, or this agent session's current tab when omitted. Server browser tabs only.",
    parameters: McpRouting.parameters(PreviewAutomationDragInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Drag in preview page"),
);

const PreviewUploadTool = browserTool(
  Tool.make("preview_upload", {
    description:
      "Give files on the environment to the page in the tab selected by tabId, or this agent session's current tab when omitted. After clicking an upload control, preview_status reports the open fileChooser; call this with absolute paths to answer it, or with an empty list to cancel. Pass a locator for an <input type=file> to set its files without a picker. Server browser tabs only.",
    parameters: McpRouting.parameters(PreviewAutomationUploadInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Upload files to preview page"),
);

const PreviewFillCredentialTool = browserTool(
  Tool.make("preview_fill_credential", {
    description:
      "Fill one login input in the tab selected by tabId, or this agent session's current tab when omitted, with a value from this environment's credential vault: the username, the password, or the current one-time code (totp) of a vault item. You never see the value; the result says only whether it was filled. The input's page must be https and one of the item's sites. The fill starts a login for the tab's whole browser profile, even when it fails: no tab in that profile, other agents' included, can be read (no snapshot, evaluate, recording, URL or title) until it ends, so keep logins short. Finish signing in with more fills, preview_click or preview_type with a locator, preview_press and preview_wait_for, then call preview_end_login, which closes the profile's browser with every tab in it; open a new tab to continue, signed in through the profile's saved cookies. After a failed fill, end the login and retry in a new tab. Server browser tabs only, not desktop-hosted ones.",
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
      "End the login started by preview_fill_credential in the tab selected by tabId, or this agent session's current tab when omitted: closes that browser profile's browser, with every tab in it, including tabs still opening, and returns once it has closed. Then call preview_open for a new tab; the profile's saved cookies keep the session, except cookies the site set without an expiry. Returns the closed tab ids, or none when the tab was not in a login.",
    parameters: McpRouting.parameters(PreviewAutomationTabTargetInput),
    success: Schema.Struct({
      ...PreviewAutomationEndLoginResult.fields,
      ...presentationFields,
    }),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "End browser login"),
);

const PreviewPressTool = browserTool(
  Tool.make("preview_press", {
    description:
      "Press one keyboard key in the tab selected by tabId, or this agent session's current tab when omitted. Examples: {key:'Enter'}, {key:'Escape'}, or {key:'a',modifiers:['Meta']}.",
    parameters: McpRouting.parameters(PreviewAutomationPressInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Press key in preview page"),
);

const PreviewScrollTool = safeBrowserTool(
  Tool.make("preview_scroll", {
    description:
      "Scroll the tab selected by tabId, or this agent session's current tab when omitted. Positive deltaY scrolls down and positive deltaX scrolls right; a locator/selector targets a container.",
    parameters: McpRouting.parameters(PreviewAutomationScrollInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Scroll preview page"),
);

/**
 * MCP `structuredContent` must be a JSON object, and Claude Code rejects the
 * whole result when it is not. Wrapping keeps arrays, strings, numbers, and
 * null valid instead of failing only for non-object expressions.
 */
export const PreviewEvaluateResult = Schema.Struct({
  ...presentationFields,
  value: Schema.Unknown.annotate({
    description: "The JSON-serializable value the expression produced, or null.",
  }),
}).annotate({ description: "The evaluated expression result." });

const PreviewEvaluateTool = browserTool(
  Tool.make("preview_evaluate", {
    description:
      "Evaluate JavaScript in the tab selected by tabId, or this agent session's current tab when omitted. Needs the same control as clicking: it works on your own tabs and on the user's tab only while nobody controls it. To read a tab the user is driving, use preview_snapshot. Returns {value} with a serializable result up to 64 KB; the expression may mutate page state.",
    parameters: McpRouting.parameters(PreviewAutomationEvaluateInput),
    success: PreviewEvaluateResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Evaluate JavaScript in preview"),
);

const PreviewWaitForTool = readonlyBrowserTool(
  Tool.make("preview_wait_for", {
    description:
      "Wait in the tab selected by tabId, or this agent session's current tab when omitted, until all supplied locator, selector, text, and URL conditions match.",
    parameters: McpRouting.parameters(PreviewAutomationWaitForInput),
    success: PreviewActionResult,
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Wait for preview page condition"),
);

const PreviewRecordingStartTool = safeBrowserTool(
  Tool.make("preview_recording_start", {
    description:
      "Start recording the collaborative browser tab selected by tabId, or this agent session's current tab when omitted.",
    parameters: McpRouting.parameters(PreviewAutomationTabTargetInput),
    success: Schema.Struct({ ...PreviewAutomationRecordingStatus.fields, ...presentationFields }),
    failure: PreviewToolFailure,
    dependencies,
  }).annotate(Tool.Title, "Start browser recording"),
);

const PreviewRecordingStopTool = safeBrowserTool(
  Tool.make("preview_recording_stop", {
    description:
      "Stop recording the collaborative browser tab selected by tabId, or this agent session's current tab when omitted, and transfer the compressed recording once (up to 50 MiB) to an evidence file readable in this agent's environment. Native/legacy calls return the local evidence path. Negotiated peer calls return a bounded environment-qualified resource with its name, owner, byte digest, MIME and size; use its attachmentId with the supported attachment send/launch inputs instead of interpreting it as a source path. Accepted native claims replay the original evidence without replacing later file edits; an unconfirmed durable publication reports outcome_unknown and is not repeated.",
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
  PreviewClickTool,
  PreviewTypeTool,
  PreviewHoverTool,
  PreviewSelectTool,
  PreviewDragTool,
  PreviewUploadTool,
  PreviewFillCredentialTool,
  PreviewEndLoginTool,
  PreviewPressTool,
  PreviewScrollTool,
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
  PreviewClickTool,
  PreviewTypeTool,
  PreviewHoverTool,
  PreviewSelectTool,
  PreviewDragTool,
  PreviewUploadTool,
  PreviewFillCredentialTool,
  PreviewEndLoginTool,
  PreviewPressTool,
  PreviewScrollTool,
  PreviewEvaluateTool,
  PreviewWaitForTool,
  PreviewRecordingStartTool,
  PreviewRecordingStopTool,
);

export const PreviewSnapshotToolkit = Toolkit.make(PreviewSnapshotTool);
