// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off - Playwright callbacks run outside the Effect runtime.
// Screencasts ignore emulated device scale; real 2x keeps captures sharp.
// --disable-gpu uses cheaper software compositing while preserving SwiftShader WebGL.
import {
  BrowserProfile,
  BrowserProfileId,
  BUILT_IN_BROWSER_PROFILES,
  PreviewWatchError,
  PreviewWatchStartInput,
  PreviewWatchStopInput,
  type PreviewWatchReadInput,
  type PreviewWatchReadResult,
  CookieMirrorError,
  type CookieMirrorBatch,
  type MirrorCookie,
  type PreviewLoginExportResult,
  type PreviewLoginImportResult,
  OrchestratorMcpFailure,
  type NativeBrowserDownloadAssetResource,
  DEFAULT_BROWSER_PROFILE_ID,
  DEFAULT_BROWSER_RECORDING_FRAME_RATE,
  FILL_PREVIEW_VIEWPORT,
  findBrowserProfile,
  INCOGNITO_BROWSER_PROFILE_ID,
  isBuiltInBrowserProfileId,
  resolveBrowserProfiles,
  type PreviewReportProfilesInput,
  type PreviewReportStatusInput,
  PREVIEW_AUTOMATION_SERVER_OPERATIONS,
  PreviewViewportSetting as PreviewViewportSettingSchema,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  type PreviewAutomationActionEvent,
  type PreviewAutomationClickInput,
  type PreviewAutomationDialogInput,
  type PreviewAutomationConsoleEntry,
  type PreviewAutomationDragInput,
  type PreviewAutomationEvaluateInput,
  type PreviewAutomationFillCredentialInput,
  type PreviewAutomationSaveSecretInput,
  PreviewTabId,
  type PreviewAutomationHoverInput,
  type PreviewAutomationLoadError,
  type PreviewAutomationNavigateInput,
  type PreviewAutomationNetworkEntry,
  type PreviewAutomationOpenInput,
  PreviewAutomationReadPageInput,
  type PreviewAutomationPressInput,
  type PreviewAutomationRequest,
  type PreviewAutomationResizeInput,
  type PreviewAutomationScrollInput,
  type PreviewAutomationSelectInput,
  type PreviewAutomationSetColorSchemeInput,
  type PreviewAutomationSnapshotInput,
  type PreviewAutomationStatus,
  type PreviewAutomationTypeInput,
  type PreviewAutomationUploadInput,
  type PreviewAutomationWaitForInput,
  PreviewClearProfileError,
  type PreviewNavStatus,
  type PreviewSessionSnapshot,
  type PreviewViewportSetting,
  ThreadId,
  SERVER_BROWSER_AUTOMATION_CLIENT_ID,
  type PreviewAppearancePreference,
  type PreviewViewerResource,
} from "@t3tools/contracts";
import { AgentScope, agentScopeLauncherScript } from "@t3tools/shared/AgentScope";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { appLinkUrl } from "@t3tools/shared/appLinks";
import { normalizePreviewUrl } from "@t3tools/shared/preview";
import { resolvePreviewViewport } from "@t3tools/shared/previewViewport";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { constVoid } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import type {
  BrowserContext,
  CDPSession,
  Dialog,
  Download,
  FileChooser,
  Frame,
  Page,
} from "playwright-core";

import { PENDING_ATTACHMENT_THREAD_SEGMENT } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { resolveRootCliCommand } from "../cli/invocation.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { fillCredential, serverBrowserOnly } from "./CredentialFill.ts";
import { CredentialVault, protocolLoggingEnabled } from "./CredentialVault.ts";
import { saveSecret, saveSecretServerBrowserOnly } from "./SecretCapture.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as DesktopBrowserChannel from "./DesktopBrowserChannel.ts";
import * as PreviewAgentActivity from "./PreviewAgentActivity.ts";
import * as PreviewManager from "./Manager.ts";
import * as ServerBrowserPage from "./ServerBrowserPage.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as PreviewBrowserHost from "./PreviewBrowserHost.ts";
import { ServerCookieMirrorProfiles } from "./ServerCookieMirrorProfiles.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";
import { ServerLoginCookies } from "./ServerLoginCookies.ts";
import {
  agentColorScheme,
  presentAsChrome,
  previewViewportSize,
  previewViewportZoom,
  RENDER_SCALE,
  resizeAgentViewport,
  ServerBrowserContexts,
  viewportAtZoom,
} from "./ServerBrowserContexts.ts";
import { PageWatchStore } from "./PageWatchStore.ts";
import { installPageWatch } from "./ServerBrowserWatch.ts";
import { BrowserControlInterrupted, SessionControl } from "./SessionControl.ts";
import { classifyAntiBotPage, siteOf, SiteHolds, type HeldSite } from "./SiteHold.ts";
import { installPdfDocumentNavigation, isPdfAssetUrl } from "./PdfDocumentNavigation.ts";

const SERVER_HOST_CLIENT_ID = SERVER_BROWSER_AUTOMATION_CLIENT_ID;
// Each client's report is capped, but the saved catalog is their union, so it has no cap.
const SavedProfilesJson = Schema.fromJsonString(
  Schema.Struct({ profiles: Schema.Array(BrowserProfile), defaultProfileId: BrowserProfileId }),
);
const decodeSavedProfiles = Schema.decodeUnknownEffect(SavedProfilesJson);
const encodeSavedProfiles = Schema.encodeSync(SavedProfilesJson);
// Chromium allows three unacked frames, so 100 ms pacing caps viewers near 30 fps.
const SCREENCAST_ACK_PACE_MS = 100;
const SCREENCAST_SETTLE_MS = 200;
const SCREENCAST_MOTION_FRAMES = 4;
const SCREENCAST_MOTION_WINDOW_MS = 300;
const SCREENCAST_MOTION_QUALITY = 50;
const HOST_RECONNECT_DELAY = "1 second";
const DESKTOP_ATTACH_TIMEOUT = "10 seconds";
// A viewer still is best-effort; a capture Chromium never answers must not hold the capture lock.
const VIEWER_STILL_TIMEOUT_MS = 2_000;
const VIEWER_OUTPUT_LIMIT = 64;
const RECORDING_SCREENCAST = { format: "jpeg", quality: 90, everyNthFrame: 1 } as const;
const RECORDING_BINARY_EXPORT_MIN_BYTES = 8 * 1024 * 1024;
const RECORDING_DOWNLOAD_TIMEOUT_MS = 5_000;
const decodeViewportSetting = Schema.decodeUnknownSync(PreviewViewportSettingSchema);
const decodeReadPage = Schema.decodeUnknownSync(PreviewAutomationReadPageInput);
const decodeWatchStart = Schema.decodeUnknownSync(PreviewWatchStartInput);
const decodeWatchStop = Schema.decodeUnknownSync(PreviewWatchStopInput);
const isWatchReadFailure = Schema.is(Schema.Union([PreviewWatchError, OrchestratorMcpFailure]));
const encodeInputJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const AGENT_CURSOR_MOVE_MS = 160;
const AGENT_CURSOR_CLICK_LEAD_MS = 40;
/** A dialog blocks page script, so a desktop size read gives up after this. */
const DESKTOP_VIEWPORT_READ_TIMEOUT = "1 second";
/** A desktop page that keeps its size draws nothing on a resize, so the wait gives up. */
const DESKTOP_RESIZE_WAIT_MS = 1000;
/** Answered only once the guest paints a frame; one pixel keeps it cheap. */
const DESKTOP_PAINT_CAPTURE = {
  format: "jpeg",
  quality: 1,
  clip: { x: 0, y: 0, width: 1, height: 1, scale: 1 },
} as const;
// A revealed tab's client opens the panel and attaches its stream a few round trips later, remote ones included.
const REVEAL_VIEWER_WAIT_MS = 1000;
// A desktop panel's shown report waits on the renderer's layout, which a busy machine can push past a second.
const REVEAL_DESKTOP_SHOWN_WAIT_MS = 3000;

const sleepUntil = (deadline: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, deadline - Date.now())));

const awaitViewer = async (tab: ServerTab, timeoutMs: number) => {
  const attached = (tab.viewerAttached ??= Promise.withResolvers());
  let timer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    attached.promise,
    new Promise<void>((resolve) => {
      timer = setTimeout(resolve, Math.max(0, timeoutMs));
    }),
  ]);
  clearTimeout(timer);
};

const EDITABLE_AT_POINT_SCRIPT = `(x, y) => {
  let element = document.elementFromPoint(x, y);
  while (element && element.shadowRoot) {
    const inner = element.shadowRoot.elementFromPoint(x, y);
    if (!inner || inner === element) break;
    element = inner;
  }
  if (element && element.tagName === "LABEL" && element.control) element = element.control;
  if (!element) return false;
  if (element.tagName === "IFRAME" || element.tagName === "FRAME") return true;
  if (element.isContentEditable) return true;
  if (element.tagName === "TEXTAREA") return !element.disabled && !element.readOnly;
  if (element.tagName !== "INPUT") return false;
  const nonText = ["button", "checkbox", "color", "file", "hidden", "image", "radio", "range", "reset", "submit"];
  return !nonText.includes(element.type) && !element.disabled && !element.readOnly;
}`;
// Larger selects keep the real press: their options would not fit the 8 MB viewer socket bound.
const SELECT_MENU_OPTION_LIMIT = 10_000;
const SELECT_MENU_LABEL_LIMIT = 150;
/**
 * Guards the dropdown select at a viewport point so the trusted press that follows runs page
 * handlers, then is cancelled after them; `pressed` records whether the page cancelled it first.
 * Returns null where the native popup should open, including selects in frames.
 */
const GUARD_SELECT_AT_POINT_SCRIPT = `(x, y, optionLimit) => {
  let element = document.elementFromPoint(x, y);
  while (element && element.shadowRoot) {
    const inner = element.shadowRoot.elementFromPoint(x, y);
    if (!inner || inner === element) break;
    element = inner;
  }
  if (element && element.tagName === "OPTION") element = element.closest("select");
  if (!element || element.tagName !== "SELECT") return null;
  if (element.multiple || element.size > 1 || element.matches(":disabled")) return null;
  // A customizable select draws its picker in the page, where the stream already shows it.
  if (
    getComputedStyle(element).appearance === "base-select" &&
    getComputedStyle(element, "::picker(select)").appearance === "base-select"
  )
    return null;
  if (element.options.length > optionLimit) return null;
  const state = { select: element, pressed: false, cancelled: false };
  let settled = false;
  const settle = (event) => {
    if (settled || !event.composedPath().includes(element)) return;
    settled = true;
    state.pressed = !event.defaultPrevented;
    Event.prototype.preventDefault.call(event);
    if (state.pressed) element.focus({ preventScroll: true });
  };
  // A page that stops the press keeps it from the bubbling guard, so stopping it settles it;
  // the page may still cancel it afterwards, which cancelled records.
  const watch = (event) => {
    event.preventDefault = function () {
      if (this.composedPath().includes(element)) state.cancelled = true;
      return Event.prototype.preventDefault.call(this);
    };
    for (const name of ["stopPropagation", "stopImmediatePropagation"]) {
      const stop = event[name];
      event[name] = function () {
        settle(this);
        return stop.call(this);
      };
    }
  };
  addEventListener("mousedown", watch, true);
  addEventListener("mousedown", settle);
  state.stop = () => {
    removeEventListener("mousedown", watch, true);
    removeEventListener("mousedown", settle);
  };
  return state;
}`;
/** Describes a pressed select in viewport CSS px and keeps the offered options. */
const OFFER_SELECT_MENU_SCRIPT = `function (labelLimit, optionLimit) {
  const select = this.select;
  // Page handlers ran in between: they may have added options or removed the select.
  if (!this.pressed || this.cancelled || !select.isConnected || !select.ownerDocument.defaultView) return null;
  if (select.options.length > optionLimit) return null;
  const { left, top, width, height } = select.getBoundingClientRect();
  const view = select.ownerDocument.defaultView;
  const shown = (element) => !element.hidden && view.getComputedStyle(element).display !== "none";
  this.offered = Array.from(select.options).filter(
    (option) => shown(option) && (option.parentElement.tagName !== "OPTGROUP" || shown(option.parentElement)),
  );
  return {
    options: this.offered.map((option) => ({
      label: option.label.slice(0, labelLimit),
      disabled: option.matches(":disabled"),
    })),
    selectedIndex: this.offered.indexOf(select.options[select.selectedIndex]),
    x: left,
    y: top,
    width,
    height,
  };
}`;
const CHOOSE_SELECT_OPTION_SCRIPT = `function (index) {
  const { select, offered } = this;
  const option = offered && offered[index];
  if (!option || !option.isConnected || !select.ownerDocument.defaultView || option.closest("select") !== select) return;
  if (option.matches(":disabled") || select.matches(":disabled") || option.selected) return;
  option.selected = true;
  select.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
  select.dispatchEvent(new Event("change", { bubbles: true }));
}`;
const UNATTACHED_FILL_VIEWPORT = { width: 1280, height: 800 } as const;
const NAVIGATION_TIMEOUT_MS = 15_000;
// Status reads run in the renderer; a busy page must not hold a status past the broker's grace.
const STATUS_READ_MS = 250;
const VIEWER_NAVIGATION_OPTIONS = { waitUntil: "commit", timeout: NAVIGATION_TIMEOUT_MS } as const;
const ACTION_TIMELINE_LIMIT = 50;

export class ServerBrowserTabNotFoundError extends Schema.TaggedError<ServerBrowserTabNotFoundError>()(
  "ServerBrowserTabNotFoundError",
  { threadId: Schema.String, tabId: Schema.String },
) {
  override get message(): string {
    return "The server preview tab does not exist.";
  }
}

const isTabNotFound = Schema.is(ServerBrowserTabNotFoundError);
const isHostSetupError = Schema.is(
  Schema.Union([
    PreviewBrowserHost.PreviewBrowserSandboxError,
    PreviewBrowserHost.PreviewBrowserLibrariesError,
    PreviewBrowserHost.PreviewBrowserRootError,
  ]),
);

class ServerBrowserDownloadsCleanupError extends Schema.TaggedError<ServerBrowserDownloadsCleanupError>()(
  "ServerBrowserDownloadsCleanupError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "The server preview could not clear earlier downloads.";
  }
}

export class ServerBrowserLaunchError extends Schema.TaggedError<ServerBrowserLaunchError>()(
  "ServerBrowserLaunchError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "The server preview browser could not start.";
  }
}

export type ServerBrowserViewerOutput =
  | {
      readonly _tag: "frame";
      readonly data: Uint8Array;
      readonly ack: Effect.Effect<void>;
    }
  | { readonly _tag: "viewport"; readonly width: number; readonly height: number }
  | {
      readonly _tag: "control";
      readonly canOperate: boolean;
      readonly controller: "agent" | "you" | "another-viewer" | "unclaimed";
      readonly generation: number;
      readonly dialog: {
        readonly type: string;
        readonly message: string;
        readonly defaultValue: string;
      } | null;
    }
  | {
      readonly _tag: "probe";
      readonly x: number;
      readonly y: number;
      readonly editable: boolean;
    }
  | { readonly _tag: "clipboard"; readonly text: string }
  | {
      readonly _tag: "pointer";
      readonly phase: "move" | "click";
      readonly x: number;
      readonly y: number;
      readonly sequence: number;
    }
  | {
      readonly _tag: "fileChooser";
      readonly id: string;
      readonly multiple: boolean;
      readonly accept: string;
    }
  | { readonly _tag: "fileChooserClosed"; readonly id: string }
  | {
      readonly _tag: "selectMenu";
      readonly id: string;
      readonly options: ReadonlyArray<{ readonly label: string; readonly disabled: boolean }>;
      readonly selectedIndex: number;
      readonly x: number;
      readonly y: number;
      readonly width: number;
      readonly height: number;
    }
  | { readonly _tag: "selectMenuClosed"; readonly id: string }
  | { readonly _tag: "controlFailed"; readonly action: "take" | "release" }
  /** The page this viewer controls opened a new tab; the viewer shows it, as a browser would. */
  | { readonly _tag: "popup"; readonly tabId: string; readonly resource?: PreviewViewerResource }
  | {
      readonly _tag: "download";
      readonly id: string;
      readonly fileName: string;
      readonly sizeBytes: number;
    }
  | { readonly _tag: "gone" }
  | { readonly _tag: "reconnect" };

class BrowserInputOverloaded extends Schema.TaggedError<BrowserInputOverloaded>()(
  "BrowserInputOverloaded",
  {},
) {
  override get message() {
    return "Browser input backlog is full. Reconnect after pending input drains.";
  }
}

export interface ServerBrowserViewer {
  readonly output: Queue.Dequeue<ServerBrowserViewerOutput>;
  readonly input: (message: unknown) => Effect.Effect<void, BrowserInputOverloaded>;
  readonly enqueueInput: (
    message: unknown,
  ) => Effect.Effect<void, BrowserInputOverloaded, Scope.Scope>;
}

export type ProfileReporter = (input: PreviewReportProfilesInput) => Effect.Effect<void>;

export class ServerBrowser extends Context.Service<
  ServerBrowser,
  {
    /** Retained queue only, including while the host thread is settled. */
    readonly readWatch?: <R>(
      input: PreviewWatchReadInput,
      authorizeReader: Effect.Effect<void, OrchestratorMcpFailure, R>,
    ) => Effect.Effect<PreviewWatchReadResult, PreviewWatchError | OrchestratorMcpFailure, R>;
    readonly attachViewer: (input: {
      readonly threadId: string;
      readonly tabId: string;
      readonly maxWidth: number;
      readonly maxHeight: number;
      readonly quality: number;
      readonly canOperate: boolean;
      readonly disconnectReason?: () => "transport" | "inputOverload";
      readonly authorize?: () => Promise<void>;
      /** The authenticated admission, distinct from this connection's control id. */
      readonly resourceViewerId?: string;
    }) => Effect.Effect<
      ServerBrowserViewer,
      ServerBrowserTabNotFoundError | ServerBrowserLaunchError,
      Scope.Scope
    >;

    readonly answerFileChooser: (input: {
      readonly threadId: string;
      readonly tabId: string;
      readonly chooserId: string;
      readonly authorize?: () => Promise<void>;
      readonly resourceViewerId?: string;
      readonly files: ReadonlyArray<{
        readonly name: string;
        readonly mimeType: string;
        readonly buffer: Buffer;
      }>;
    }) => Effect.Effect<boolean>;
    readonly openDownload: (input: {
      readonly threadId: string;
      readonly tabId: string;
      readonly downloadId: string;
    }) => Effect.Effect<
      Option.Option<{
        readonly path: string;
        readonly fileName: string;
        readonly resource?: NativeBrowserDownloadAssetResource;
      }>
    >;
    /** Exact native file only; each read is bounded and independently reauthorized. */
    readonly inspectDownload?: (
      resource: NativeBrowserDownloadAssetResource,
      authorize: Effect.Effect<void, OrchestratorMcpFailure>,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    readonly readDownloadBlock?: (
      resource: NativeBrowserDownloadAssetResource,
      offset: number,
      authorize: Effect.Effect<void, OrchestratorMcpFailure>,
    ) => Effect.Effect<Uint8Array, OrchestratorMcpFailure>;
    /** Deletes a human profile's server-side storage, closing its open tabs first. */
    readonly clearProfile: (profileId: string) => Effect.Effect<void, PreviewClearProfileError>;

    readonly profileReporter: Effect.Effect<ProfileReporter, never, Scope.Scope>;
    readonly mirrorResets: Stream.Stream<string>;
    readonly mirrorCookies: (input: CookieMirrorBatch) => Effect.Effect<void, CookieMirrorError>;
    /** Fork: one-snapshot login copies between linked environments' server browsers. */
    readonly loginCookies?: {
      readonly check: (profileId: string) => Effect.Effect<void, OrchestratorMcpFailure>;
      readonly export: (input: {
        readonly profileId: string;
        readonly hosts: ReadonlyArray<string>;
      }) => Effect.Effect<PreviewLoginExportResult, OrchestratorMcpFailure>;
      readonly import: (input: {
        readonly profileId: string;
        readonly hosts: ReadonlyArray<string>;
        readonly cookies: ReadonlyArray<MirrorCookie>;
        readonly skipped: PreviewLoginExportResult["skipped"];
      }) => Effect.Effect<PreviewLoginImportResult, OrchestratorMcpFailure>;
    };

    readonly authorizeViewer?: (
      resource: PreviewViewerResource,
    ) => Effect.Effect<void, ServerBrowserTabNotFoundError>;
  }
>()("t3/preview/ServerBrowser") {}

interface ViewerState {
  readonly authorize: () => Promise<void>;
  readonly id: string;
  readonly resourceViewerId: string | undefined;
  readonly canOperate: boolean;
  readonly pressedKeys: Map<string, { key: string; code: string; keyCode: number }>;
  readonly pressedButtons: Map<"left" | "middle" | "right", { x: number; y: number }>;
  /** Handle for the guarded select under a held left press; its release offers the menu. */
  selectPress: string | null;
  /** Handle for the select and options this viewer was last offered; only that offer answers. */
  selectMenu: {
    readonly id: string;
    readonly objectId: string;
    readonly session: CDPSession;
  } | null;
  readonly push: (output: ServerBrowserViewerOutput) => void;
  readonly pause: () => Promise<void>;
  readonly resume: () => Promise<void>;
  scrolledAt: number;
  /** Set once the viewer starts detaching; it can no longer receive a human claim. */
  leaving: boolean;

  requestedSize: { width: number; height: number; order: number } | null;
}

interface EncoderWindow {
  __t3Recorder: {
    cursor(x: number, y: number, click: boolean): void;
    frame(data: string, width: number): Promise<boolean>;
    stop(): Promise<{ mimeType: string | null; count: number; bytes: number }>;
    chunk(index: number): Promise<string>;
    download(): void;
  };
}

interface Recording {
  readonly encoder: Page;
  readonly session: CDPSession;
  readonly startedAt: string;
  readonly framesInFlight: Set<Promise<void>>;
  readonly authorize: () => Promise<void>;
}

interface ServerTab {
  readonly authorize: () => Promise<void>;
  readonly threadId: ThreadId;
  readonly tabId: string;
  readonly page: Page;
  readonly cdp: CDPSession;
  readonly frameSessions: Map<Frame, CDPSession>;
  /** Copy reads run in key order, outside the input queue. */
  copyReads: Promise<void>;
  /** When the last copy or cut key was acknowledged; the next one starts after it. */
  copyKeyedUntil: number;
  readonly createdAt: number;
  usedAt: number;
  readonly viewers: Set<ViewerState>;
  viewerAttached: PromiseWithResolvers<void> | null;
  readonly consoleEntries: Array<PreviewAutomationConsoleEntry>;
  readonly networkEntries: Array<PreviewAutomationNetworkEntry>;
  readonly actionTimeline: Array<PreviewAutomationActionEvent>;
  readonly control: SessionControl;
  agentOperating: boolean;
  /** The latest main-frame document response; clearing a hold classifies the page with it. */
  mainDocument: {
    readonly status: number;
    readonly headers: Record<string, string>;
    readonly url: string;
    readonly at: number;
  } | null;
  /** Classification of the latest error document, so an action can report the hold it caused. */
  siteHoldCheck: Promise<void>;
  fileChooserIntercepted: boolean;
  readonly onFileChooser: (chooser: FileChooser) => void;
  readonly watches: Set<string>;

  readonly isolatedContext: boolean;

  readonly desktop: { readonly close: () => Promise<void> } | null;
  readonly profileId: string | undefined;
  readonly openerTabId: string | undefined;

  readonly downloads: Array<ServerDownload>;
  fileChooser: {
    readonly id: string;
    readonly chooser: FileChooser;
    readonly accept: string;
    readonly offeredTo: string | null;
  } | null;
  dialog: Dialog | null;
  setting: PreviewViewportSetting;
  colorScheme: PreviewAppearancePreference;
  appliedColorScheme: "light" | "dark" | null | undefined;
  /** Last usable CSS size measured on the desktop. */
  desktopViewport: { readonly width: number; readonly height: number } | null;
  desktopViewportRead: Promise<ServerTab["desktopViewport"]>;
  /** A read queued behind `desktopViewportRead`, shared by every caller that waits for it. */
  desktopViewportNext: Promise<ServerTab["desktopViewport"]> | null;
  /** A page resize or load since the last read started; status measures again. */
  desktopViewportStale: boolean;
  /** Counts page resizes, loads and dialog closes, so a caller can tell one landed during a read. */
  desktopViewportChanges: number;
  /** Settles at the desktop page's next main-frame resize. */
  desktopResize: PromiseWithResolvers<void>;
  /** The last resize failed undrawn, so the tab's setting is a size the page never took. */
  desktopResizeUndrawn: boolean;
  loading: boolean;
  loadError: PreviewAutomationLoadError | undefined;
  /** The renderer crash this page died of, or the one the page it replaced died of. */
  crash: PreviewAutomationLoadError | undefined;
  /** What Reload loads after a crash; the reported URL is shortened and drops data payloads. */
  crashedUrl: string | undefined;
  closing: boolean;
  recording: Recording | null;
  initialNavigation: Promise<void> | null;
  pendingInitialRequestUrl: string | null;
  navigationGeneration: number;
  stoppedGeneration: number | null;
  /** The reported title a watch in the main document's isolated world waits to change from. */
  titleWatch: { readonly contextId: number; readonly title: string } | null;
  lastReport: PreviewReportStatusInput | null;

  abortedNavigation: {
    readonly url: string;
    readonly generation: number;
    readonly timer: ReturnType<typeof setTimeout>;
  } | null;

  recordingStart: Promise<Recording> | null;
  captureLock: Promise<void>;
  capturing: number;
}

interface ServerDownload {
  readonly digest: string;
  readonly blockDigests: ReadonlyArray<string>;
  readonly fingerprint: string;
  readonly id: string;
  readonly fileName: string;
  readonly path: string;
  readonly sizeBytes: number;
  readonly url: string;
  readonly completedAt: string;
}

/**
 * Operations an agent may run on any tab in its thread, even while a human
 * drives it. Evaluate is not one: page script can change anything, so it needs
 * the same control as clicking.
 */
const READ_OPERATIONS: ReadonlySet<PreviewAutomationRequest["operation"]> = new Set([
  "status",
  "snapshot",
  "waitFor",
]);

/** Operations that can send a request to the tab's site, so a Site hold refuses them. */
const SITE_HOLD_OPERATIONS: ReadonlySet<PreviewAutomationRequest["operation"]> = new Set([
  "navigate",
  "readPage",
  "click",
  "type",
  "press",
  "scroll",
  "hover",
  "select",
  "drag",
  "upload",
  "fillCredential",
  "evaluate",
  "watchStart",
]);

const SITE_HOLD_VENDOR_NAMES = {
  datadome: "DataDome",
  cloudflare: "Cloudflare",
  akamai: "Akamai",
  human: "HUMAN",
  imperva: "Imperva",
} as const;

const siteHoldError = (hold: HeldSite) =>
  new BrowserControlInterrupted({
    reason: hold.kind === "challenge" ? "antiBotChallenge" : "antiBotBlocked",
    message:
      hold.kind === "challenge"
        ? `${hold.site} shows a ${SITE_HOLD_VENDOR_NAMES[hold.vendor]} anti-bot check. Agent actions on ${hold.site} are stopped in this browser profile, and the user was asked to pass it in this tab. Do not retry, reload, or open the site in another tab or profile. Tell the user and end your turn; the user tells you when the check is done.`
        : `${hold.site} blocked this browser (${SITE_HOLD_VENDOR_NAMES[hold.vendor]}). Agent actions on ${hold.site} are stopped in this browser profile, and the user was told. Do not retry, reload, or open the site in another tab or profile. Tell the user and end your turn.`,
  });

const AGENT_TAB_LIMIT = 8;
const SERVER_TAB_LIMIT = 32;
const AGENT_TAB_IDLE_MS = 30 * 60 * 1000;
const IDLE_SWEEP_INTERVAL = "1 minute";

const DOWNLOAD_LIMIT = 20;
const ABORTED_NAVIGATION_DOWNLOAD_WAIT_MS = 5_000;
const PAGE_CLOSE_GRACE_MS = 5_000;
const DOWNLOAD_MAX_BYTES = 1024 * 1024 * 1024;
export const DOWNLOAD_BLOCK_BYTES = 1024 * 1024;
const fileFingerprint = (stat: Awaited<ReturnType<NodeFSP.FileHandle["stat"]>>) =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
const readDownloadBlockBytes = async (file: NodeFSP.FileHandle, offset: number, size: number) => {
  const bytes = new Uint8Array(Math.min(DOWNLOAD_BLOCK_BYTES, size - offset));
  let read = 0;
  while (read < bytes.byteLength) {
    const result = await file.read(bytes, read, bytes.byteLength - read, offset + read);
    if (result.bytesRead === 0) throw new Error("Native download was truncated.");
    read += result.bytesRead;
  }
  return bytes;
};

const tabKey = (threadId: string, tabId: string) => `${threadId}\u0000${tabId}`;

const pushBounded = <A>(
  buffer: Array<A>,
  entry: A,
  limit = ServerBrowserPage.DIAGNOSTIC_BUFFER_LIMIT,
) => {
  buffer.push(entry);
  if (buffer.length > limit) buffer.splice(0, buffer.length - limit);
};

const viewportSettingsEqual = (left: PreviewViewportSetting, right: PreviewViewportSetting) =>
  left._tag === right._tag &&
  (left._tag === "fill" ||
    (right._tag !== "fill" && left.width === right.width && left.height === right.height));

const fixedViewportSize = (setting: PreviewViewportSetting) =>
  setting._tag === "fill" ? null : { width: setting.width, height: setting.height };

const asRecord = (value: unknown): Record<string, unknown> | null =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>) : null;

// Retain only native-consumed fields, including the existing paste clamp,
// before charging queued work. Legacy local viewers send whole clipboards.
const viewerInputFields = new Map<string, readonly string[]>([
  ["takeControl", []],
  ["releaseControl", []],
  ["dialog", ["accept", "promptText"]],
  ["mouse", ["action", "button", "x", "y", "buttons", "clickCount", "modifiers"]],
  ["wheel", ["x", "y", "deltaX", "deltaY", "modifiers"]],
  ["key", ["action", "key", "code", "text", "keyCode", "modifiers"]],
  ["text", ["text"]],
  ["resize", ["width", "height"]],
  ["viewport", []],
  ["navigate", ["url"]],
  ["history", ["delta"]],
  ["reload", ["ignoreCache"]],
  ["stopLoading", []],
  ["probe", ["x", "y"]],
  ["selectMenu", ["id", "index"]],
]);
const normalizeViewerInput = (raw: unknown): Record<string, unknown> | null => {
  const source = asRecord(raw);
  if (!source || typeof source.type !== "string") return null;
  const fields = viewerInputFields.get(source.type);
  if (!fields) return null;
  const message: Record<string, unknown> = { type: source.type };
  for (const field of fields) {
    const value = source[field];
    if (["accept", "ignoreCache"].includes(field)) {
      if (typeof value === "boolean") message[field] = value;
    } else if (
      ["action", "button", "key", "code", "text", "url", "promptText", "id"].includes(field)
    ) {
      if (typeof value === "string") message[field] = value;
    } else if (typeof value === "number" && Number.isFinite(value)) message[field] = value;
  }
  if (source.type === "mouse") {
    message.action = source.action === "down" || source.action === "up" ? source.action : "move";
    try {
      const button = String(source.button);
      if (["none", "left", "middle", "right"].includes(button)) message.button = button;
      else delete message.button;
    } catch {
      return null;
    }
  }
  if (source.type === "key") {
    message.action = source.action === "up" ? "up" : "down";
    if (message.action === "up") {
      delete message.text;
      delete message.keyCode;
    }
  }
  if (source.type === "dialog" && message.accept !== true) delete message.promptText;
  if (source.type === "text" && typeof message.text === "string")
    message.text = message.text.slice(0, 10_000);
  if (source.type === "viewport") {
    try {
      message.setting = decodeViewportSetting(source.setting);
    } catch {
      return null;
    }
  }
  return message;
};

const num = (value: unknown, fallback = 0) =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;

const modifiersOf = (message: Record<string, unknown>) => {
  const value = num(message.modifiers);
  return Number.isInteger(value) && value >= 0 && value < 16 ? value : 0;
};

// Cmd shortcuts from Apple viewers are no editing shortcut for Linux or headless
// Chromium, so they carry the command. Ctrl already works natively on Linux,
// except copy and cut, which headless Chromium only runs as commands.
const editingCommand = (key: string, modifiers: number) => {
  const modifier = modifiers & 0b0111;
  if (modifier !== 2 && modifier !== 4) return null;
  const lower = key.toLowerCase();
  const command =
    lower === "c"
      ? "copy"
      : lower === "x"
        ? "cut"
        : modifier !== 4
          ? null
          : lower === "a"
            ? "selectAll"
            : lower === "z"
              ? modifiers & 8
                ? "redo"
                : "undo"
              : null;
  return command ? { commands: [command] } : null;
};

const TITLE_LIMIT = 512;
const TITLE_WORLD = "t3-preview-title";
// Observe root replacement without subscribing to body subtree mutations.
const titleChange = (reported: string) => `new Promise((resolve) => {
  const changed = () => document.title.slice(0, ${TITLE_LIMIT}) !== ${JSON.stringify(reported)};
  if (changed()) return resolve();
  let head;
  let root;
  let title;
  const observer = new MutationObserver(() => {
    if (changed()) {
      observer.disconnect();
      resolve();
    } else if (head !== document.head || root !== document.documentElement || title !== document.querySelector('title')) {
      observe();
    }
  });
  const observe = () => {
    observer.disconnect();
    observer.observe(document, { childList: true });
    root = document.documentElement;
    head = document.head;
    title = document.querySelector('title');
    if (root) observer.observe(root, { childList: true });
    if (head) observer.observe(head, { subtree: true, childList: true, characterData: true });
    if (title && !head?.contains(title)) {
      observer.observe(title, { subtree: true, childList: true, characterData: true });
      for (let ancestor = title.parentElement; ancestor; ancestor = ancestor.parentElement) {
        observer.observe(ancestor, { childList: true });
      }
    }
  };
  observe();
})`;

const CLIPBOARD_TEXT_LIMIT = 1024 * 1024;
const CLIPBOARD_WORLD = "t3-preview-clipboard";
// Headless Chromium shares one clipboard between every context, so the copy
// a viewer's copy or cut key makes is read from this world instead of the
// clipboard. Page scripts cannot see the world, and only a copy made while a
// real key is handled counts.
const CLIPBOARD_SCRIPT = `(() => {
  let keying = false;
  // One entry per copy or cut key, oldest first, so a later key cannot overwrite an unread copy.
  const copies = [];
  // The same Ctrl or Cmd + C or X that editingCommand sends as copy or cut.
  addEventListener("keydown", (event) => {
    if (!event.isTrusted || event.altKey || event.ctrlKey === event.metaKey) return;
    const key = event.key.toLowerCase();
    if (key !== "c" && key !== "x") return;
    keying = true;
    copies.push({ at: Date.now(), text: "" });
    setTimeout(() => { keying = false; });
  }, true);
  const selection = () => {
    let field = document.activeElement;
    while (field?.shadowRoot?.activeElement) field = field.shadowRoot.activeElement;
    if ((field instanceof HTMLInputElement || field instanceof HTMLTextAreaElement) && field.selectionStart !== null)
      return field.value.slice(field.selectionStart, field.selectionEnd ?? field.selectionStart);
    return String(document.getSelection() ?? "");
  };
  const record = (text) => {
    const copy = copies.at(-1);
    if (copy) copy.text = String(text ?? "").slice(0, ${CLIPBOARD_TEXT_LIMIT});
  };
  // Capturing at window runs before page handlers can stop the event, and
  // before cut removes the selection.
  const snapshot = (event) => {
    if (keying && event.isTrusted) record(selection());
  };
  // Bubbling back to window picks up text a page handler set instead.
  const replace = (event) => {
    if (keying && event.isTrusted && event.defaultPrevented) record(event.clipboardData?.getData("text/plain"));
  };
  for (const type of ["copy", "cut"]) {
    addEventListener(type, snapshot, true);
    addEventListener(type, replace);
  }
  // A key's copy was keyed between its dispatch and acknowledgement; older copies are stale.
  globalThis.takeCopy = (since, until) => {
    while (copies.length > 0 && copies[0].at < since) copies.shift();
    return copies[0] && copies[0].at <= until ? copies.shift().text : "";
  };
})();`;

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const host = { platform: yield* HostProcess.Platform, arch: yield* HostProcess.Architecture };
  const manager = yield* PreviewManager.PreviewManager;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const previewBrowser = yield* PreviewBrowser.PreviewBrowser;
  const desktopChannel = yield* DesktopBrowserChannel.DesktopBrowserChannel;
  const agentTurns = yield* (yield* PreviewAgentActivity.PreviewAgentActivity).changes;
  const vault = yield* CredentialVault;
  const secrets = yield* SecretRequests.SecretRequests;
  const protocolLogging = yield* protocolLoggingEnabled;
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  const runSyncExit = Effect.runSyncExitWith(yield* Effect.context<never>());
  const launchServices = yield* Effect.context<ChildProcessSpawner.ChildProcessSpawner>();
  // sudo resets the environment, so the command has to name this server's home.
  const setupCommand = yield* resolveRootCliCommand(PreviewBrowserHost.SETUP_SUBCOMMAND, [
    "--base-dir",
    config.baseDir,
  ]);
  const agentScope = yield* AgentScope;
  const launchersDir = NodePath.join(config.stateDir, "server-browser", "launchers");

  const tabs = new Map<string, ServerTab>();
  const watches = new PageWatchStore(NodeCrypto.randomUUID());
  const stopTabWatches = (tab: ServerTab, reason: PreviewWatchError["reason"]) => {
    for (const id of tab.watches) void watches.terminate(id, reason).catch(constVoid);
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const tab of tabs.values()) stopTabWatches(tab, "closed");
    }),
  );
  const pendingTabs = new Map<
    string,
    {
      readonly agentId: string | null;
      readonly threadId?: string;
      promise?: Promise<ServerTab>;
      settled?: boolean;
    }
  >();
  // Retain the original invocation across a failed launch or a desktop reconnect.
  // These are current checks, never cached grant results.
  const tabAuthorizations = new Map<string, () => Promise<void>>();
  /** Sessions closed while their tab was still opening; the open discards its page. */
  const closedPendingTabs = new Set<string>();
  const adoptedPages = new Map<string, { readonly page: Page; readonly openerTabId: string }>();
  const siteHolds = new SiteHolds();
  // A tab holding its site waits for the person, so no automatic cleanup closes it.
  const mayCloseSettledTab = (tab: ServerTab) =>
    tab.control.agentId !== null &&
    tab.control.controller === null &&
    tab.viewers.size === 0 &&
    tab.watches.size === 0 &&
    siteHolds.heldBy(tab.tabId).length === 0;
  const closedSessions = new Set<string>();
  let hostConnectionId: string | null = null;
  let viewerResizeOrder = 0;

  const mirrorResets = yield* PubSub.unbounded<string>();
  const mirroredContexts = new WeakMap<BrowserContext, string>();
  const catalog = new ServerCookieMirrorProfiles(
    NodePath.join(config.stateDir, "server-browser", "mirrored-profiles.json"),
  );
  const catalogRead = yield* Effect.tryPromise({
    try: () => catalog.read(),
    catch: () => new CookieMirrorError({ reason: "unavailable" }),
  }).pipe(Effect.result);
  if (catalogRead._tag === "Failure")
    yield* Effect.logWarning(
      "Mirrored browser profile catalog unavailable; cookie mirroring stopped.",
    );
  const contexts = new ServerBrowserContexts({
    profilesDir: NodePath.join(config.stateDir, "server-browser", "profiles"),
    idleBrowserMs: 5 * 60_000,
    executable: () => runPromise(previewBrowser.executable),
    // An agent scope keeps Chrome out of the server's memory protection and OOM score.
    launcher: async (executable) => {
      const launch = await runPromise(
        agentScope.wrap({ command: executable, args: [], name: "preview-browser" }),
      );
      if (launch.command === executable) return undefined;
      await NodeFSP.mkdir(launchersDir, { recursive: true });
      const launcher = NodePath.join(launchersDir, NodeCrypto.randomUUID());
      await NodeFSP.writeFile(launcher, agentScopeLauncherScript(launch), { mode: 0o700 });
      return { path: launcher, release: () => NodeFSP.rm(launcher, { force: true }) };
    },
    // An unsupported host never launches: its executable fails first.
    version: PreviewBrowser.previewBrowserRelease(host.platform, host.arch)?.version ?? "",
    host,
    // Playwright's launch error drops Chrome's own output; its sandbox note survives.
    diagnose: (executable, cause) =>
      Effect.runPromiseWith(launchServices)(
        PreviewBrowserHost.diagnoseLaunchFailure({
          executable,
          setupCommand,
          output: /sandboxing failed/i.test(String(cause))
            ? PreviewBrowserHost.NO_SANDBOX_SIGNATURE
            : "",
        }),
      ),
    onContextClose: (context) => {
      const profileId = mirroredContexts.get(context);
      if (profileId !== undefined) runFork(PubSub.publish(mirrorResets, profileId));
      for (const tab of tabs.values()) {
        if (tab.page.context() === context) dropTab(tab, true);
      }
    },
  });
  const cookieMirror = new ServerCookieMirror(contexts);

  // Playwright cannot reload a crashed page, so a fresh page takes over its tab.
  const reopenCrashedTab = async (tab: ServerTab) => {
    if (tab.desktop) return dropTab(tab, true);
    const key = tabKey(tab.threadId, tab.tabId);
    if (tabs.get(key) !== tab) return;
    const crash = {
      url: ServerBrowserPage.withoutDataPayload(tab.page.url()).slice(0, 2048),
      code: -2,
      description: "Page crashed",
    };
    tab.crash = crash;
    const reopening = (async () => {
      // The session must name the crash before a new page reads it, or it would load the URL again.
      await runPromise(
        manager
          .reportStatus({
            threadId: tab.threadId,
            tabId: tab.tabId,
            serverControlled: true,
            navStatus: { _tag: "LoadFailed", title: "", ...crash },
            canGoBack: false,
            canGoForward: false,
          })
          .pipe(Effect.ignore),
      );
      dropTab(tab, false, true);
      // A context that is still closing would take the new page down with it.
      if (tab.isolatedContext) await tab.page.context().close().catch(constVoid);
      const { sessions } = await runPromise(manager.list({ threadId: tab.threadId }));
      const snapshot = sessions.find(
        (session) => session.tabId === tab.tabId && session.runtime === "server",
      );
      if (!snapshot) {
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      return createTab(snapshot, tabAuthorizations.get(key) ?? (async () => {}), tab.page.url());
    })().finally(() => {
      pendingTabs.delete(key);
      closedPendingTabs.delete(key);
    });
    // Reserved before the dead page leaves, so a lookup in between waits for its replacement.
    pendingTabs.set(key, { agentId: tab.control.agentId, promise: reopening });
    await reopening.catch(constVoid);
  };

  const dialogStatus = (tab: ServerTab) =>
    tab.dialog
      ? {
          type: tab.dialog.type(),
          message: tab.dialog.message().slice(0, 4000),
          defaultValue: tab.dialog.defaultValue().slice(0, 4000),
        }
      : null;

  const updateFileChooserInterception = (tab: ServerTab) => {
    // Only agents and streamed controllers can answer intercepted uploads; native desktop views use the OS picker.
    const intercept = !tab.desktop || tab.agentOperating || tab.control.controller !== null;
    if (intercept === tab.fileChooserIntercepted) return;
    tab.fileChooserIntercepted = intercept;
    if (intercept) tab.page.on("filechooser", tab.onFileChooser);
    else tab.page.off("filechooser", tab.onFileChooser);
  };

  const broadcastControl = (tab: ServerTab) => {
    updateFileChooserInterception(tab);
    for (const viewer of tab.viewers)
      viewer.push({
        _tag: "control",
        canOperate: viewer.canOperate,
        controller:
          tab.control.controller === viewer.id
            ? "you"
            : tab.control.humanDisconnected
              ? "unclaimed"
              : tab.control.controller !== null
                ? "another-viewer"
                : tab.control.agentId !== null
                  ? "agent"
                  : "unclaimed",
        generation: tab.control.generation,
        dialog: dialogStatus(tab),
      });
  };

  /** A claim parked by a lost viewer goes to an operating viewer, which then offers Release control. */
  const resumeHumanControl = (tab: ServerTab) => {
    const viewer = [...tab.viewers].findLast(
      (candidate) => candidate.canOperate && !candidate.leaving,
    );
    if (viewer && tab.control.resume(viewer.id)) pushFileChooser(tab);
  };

  const resolveDialog = async (tab: ServerTab, input: PreviewAutomationDialogInput) => {
    const dialog = tab.dialog;
    if (!dialog) throw new Error("No dialog is pending.");
    if (input.accept) await dialog.accept(input.promptText);
    else await dialog.dismiss();
    if (tab.dialog === dialog) tab.dialog = null;
    ServerBrowserPage.invalidateRefs(tab.page);
    broadcastControl(tab);
  };

  const report = (tab: ServerTab, navStatus: PreviewNavStatus) => {
    const generation = tab.navigationGeneration;
    const loadError = tab.loadError;
    void tab.cdp
      .send("Page.getNavigationHistory")
      .catch(() => null)
      .then((history) => {
        // A status the dead page still had in flight must not replace the crash.
        if (tab.crash) return;
        if (
          navStatus._tag === "Success" &&
          (tab.navigationGeneration !== generation || tab.loadError !== loadError)
        )
          return;
        const index = history?.currentIndex ?? 0;
        const count = history?.entries.length ?? 0;
        const input = {
          threadId: tab.threadId,
          tabId: tab.tabId,
          serverControlled: true,
          navStatus,
          canGoBack: index > 0,
          canGoForward: index < count - 1,
        };
        if (NodeUtil.isDeepStrictEqual(tab.lastReport, input)) return;
        tab.lastReport = input;
        runFork(manager.reportStatus(input).pipe(Effect.ignore));
      });
  };

  const reportLoaded = async (tab: ServerTab) => {
    const url = tab.page.url();
    // Chromium's error page loads after `requestfailed` and must not clear LoadFailed.
    if (url === "about:blank" || url.startsWith("chrome-error://")) return;
    // A navigation that starts and fails during the title read leaves the URL unchanged.
    const generation = tab.navigationGeneration;
    const loadError = tab.loadError;
    const title = (await tab.page.title().catch(() => "")).slice(0, TITLE_LIMIT);
    if (
      tab.navigationGeneration !== generation ||
      tab.loadError !== loadError ||
      tab.loading ||
      tab.page.url() !== url
    )
      return;
    report(tab, {
      _tag: "Success",
      url: ServerBrowserPage.withoutDataPayload(url).slice(0, 2048),
      title,
    });
    void watchTitle(tab, title);
  };

  // A title change fires no page event, so one watch per document reports the next one.
  const watchTitle = async (tab: ServerTab, title: string) => {
    // Chromium keeps one world per name and document, so a new document gets a new context.
    const world = await tab.cdp
      .send("Page.getFrameTree")
      .then(({ frameTree }) =>
        tab.cdp.send("Page.createIsolatedWorld", {
          frameId: frameTree.frame.id,
          worldName: TITLE_WORLD,
        }),
      )
      .catch(() => null);
    if (!world) return;
    const watch = { contextId: world.executionContextId, title };
    if (tab.titleWatch?.contextId === watch.contextId && tab.titleWatch.title === title) return;
    tab.titleWatch = watch;
    const changed = await tab.cdp
      .send("Runtime.evaluate", {
        contextId: watch.contextId,
        expression: titleChange(title),
        awaitPromise: true,
      })
      .then(
        ({ exceptionDetails }) => exceptionDetails === undefined,
        () => false,
      );
    if (tab.titleWatch !== watch) return;
    tab.titleWatch = null;
    // A dropped tab's page can still change its title while it unloads.
    if (changed && !tab.closing && !tab.loadError) await reportLoaded(tab);
  };

  const reportLiveTabs = () => {
    const connectionId = hostConnectionId;
    if (connectionId === null) return;
    runFork(
      environment.getEnvironmentId.pipe(
        Effect.flatMap((environmentId) =>
          broker.focusHost({
            clientId: SERVER_HOST_CLIENT_ID,
            environmentId,
            connectionId,
            focused: true,
            liveTabs: [...tabs.values()].map((tab) => ({
              threadId: tab.threadId,
              tabId: tab.tabId,
              visible: tab.viewers.size > 0,
            })),
          }),
        ),
      ),
    );
  };

  // Desktop pages have no Playwright viewport; an isolated world keeps page script
  // from overriding the CSS size. A failed read answers null and keeps the last usable size.
  const readDesktopViewport = async (tab: ServerTab) => {
    const readSize = async () => {
      const { frameTree } = await tab.cdp.send("Page.getFrameTree");
      const { executionContextId } = await tab.cdp.send("Page.createIsolatedWorld", {
        frameId: frameTree.frame.id,
        worldName: "t3-preview-viewport",
      });
      const { result } = await tab.cdp.send("Runtime.evaluate", {
        expression: "({ width: innerWidth, height: innerHeight })",
        contextId: executionContextId,
        returnByValue: true,
      });
      return asRecord(result.value);
    };
    const size = await runPromise(
      // A navigation can destroy the isolated world mid-read.
      Effect.promise(() => readSize().catch(() => null)).pipe(
        Effect.timeoutOption(DESKTOP_VIEWPORT_READ_TIMEOUT),
        Effect.map(Option.getOrNull),
      ),
    );
    const width = size?.width;
    const height = size?.height;
    if (typeof width === "number" && width > 0 && typeof height === "number" && height > 0)
      return (tab.desktopViewport = { width, height });
    tab.desktopViewportStale = true;
    return null;
  };

  // Reads run one at a time, and callers that arrive during a read share the next one.
  const measureDesktopViewport = (tab: ServerTab) =>
    (tab.desktopViewportNext ??= tab.desktopViewportRead.then(() => {
      tab.desktopViewportNext = null;
      tab.desktopViewportStale = false;
      return (tab.desktopViewportRead = readDesktopViewport(tab));
    }));

  const layoutSize = async (tab: ServerTab) => {
    // A dialog blocks page script; its close measures again.
    if (tab.desktop) {
      return tab.dialog
        ? tab.desktopViewport
        : ((await measureDesktopViewport(tab)) ?? tab.desktopViewport);
    }
    const size = previewViewportSize(tab.page);
    return size && viewportAtZoom(size, previewViewportZoom(tab.page));
  };

  // Page resizes and loads mark the size stale; only then does status read the page. One
  // during the read it waits for can leave that answer old, so it reads once more.
  const desktopStatusViewport = async (tab: ServerTab) => {
    const changes = tab.desktopViewportChanges;
    await (tab.desktopViewportStale || tab.dialog ? layoutSize(tab) : tab.desktopViewportRead);
    if (tab.desktopViewportChanges !== changes) await layoutSize(tab);
    return tab.desktopViewport;
  };

  const broadcastViewport = async (tab: ServerTab) => {
    const size = await layoutSize(tab);
    if (!size || tab.closing) return;
    for (const viewer of tab.viewers) viewer.push({ _tag: "viewport", ...size });
  };

  const applySetting = async (
    tab: ServerTab,
    setting: PreviewViewportSetting,
    authorize = async () => {},
  ) => {
    await authorize();
    tab.setting = setting;

    if (tab.desktop) return;
    const size =
      fixedViewportSize(setting) ??
      [...tab.viewers]
        .map((viewer) => viewer.requestedSize)
        .filter((requested) => requested !== null)
        .sort((left, right) => right.order - left.order)[0] ??
      UNATTACHED_FILL_VIEWPORT;
    await resizeAgentViewport(
      tab.page,
      tab.cdp,
      { width: size.width, height: size.height },
      previewViewportZoom(tab.page),
    );
    await broadcastViewport(tab);
  };

  /** Applies a tab's published appearance and, for headless tabs, zoom. */
  const applyRendering = async (
    tab: ServerTab,
    snapshot: PreviewSessionSnapshot,
    authorize = async () => {},
  ) => {
    await authorize();
    const colorScheme = snapshot.colorScheme ?? "system";
    // Playwright starts in light mode. Native tabs on System follow the desktop's scheme.
    const appliedColorScheme =
      colorScheme === "system"
        ? tab.desktop
          ? null
          : agentColorScheme(new Date().getHours())
        : colorScheme;
    if (appliedColorScheme !== tab.appliedColorScheme) {
      await tab.page.emulateMedia({ colorScheme: appliedColorScheme });
      tab.appliedColorScheme = appliedColorScheme;
    }
    tab.colorScheme = colorScheme;
    const zoomFactor = snapshot.zoomFactor ?? 1;
    const previousZoom = previewViewportZoom(tab.page);
    if (zoomFactor !== previousZoom && !tab.desktop) {
      await authorize();
      await resizeAgentViewport(
        tab.page,
        tab.cdp,
        previewViewportSize(tab.page) ?? UNATTACHED_FILL_VIEWPORT,
        zoomFactor,
      );
      await broadcastViewport(tab);
    }
  };

  const sessionOpen = (tab: ServerTab) => !closedSessions.has(tabKey(tab.threadId, tab.tabId));

  // Closing the session makes the desktop destroy its webview, which drops an unload still in
  // progress, and `runBeforeUnload` resolves before it runs. So wait for the page's `pagehide`.
  const unloadDesktopPage = async (page: Page) => {
    const unloaded = Promise.withResolvers<void>();
    const timeout = setTimeout(unloaded.resolve, PAGE_CLOSE_GRACE_MS);
    const binding = `__t3PreviewUnloaded${NodeCrypto.randomUUID().replaceAll("-", "")}`;
    void (async () => {
      await page.exposeFunction(binding, unloaded.resolve);
      await page.evaluate((name) => {
        const target = globalThis as unknown as {
          addEventListener: (type: string, listener: () => void) => void;
        };
        target.addEventListener("pagehide", () =>
          Reflect.apply(Reflect.get(target, name), target, []),
        );
      }, binding);
      await page.close({ runBeforeUnload: true });
    })().catch(unloaded.resolve);
    await unloaded.promise;
    clearTimeout(timeout);
  };

  const unloadingTabs = new WeakMap<ServerTab, Promise<void>>();
  yield* manager.registerSiteHoldClear(({ threadId, tabId, authorize }) =>
    Effect.promise(async () => {
      const tab = tabs.get(tabKey(threadId, tabId));
      if (tab) await settleSiteHold(tab, "clear", authorize);
      else {
        // A detached tab can still be cleared by the person.
        await authorize?.();
        for (const [scope, hold] of siteHolds.heldBy(tabId))
          if (hold.threadId === threadId) siteHolds.clear(scope, hold);
        await runPromise(
          manager
            .setSiteHold({ threadId: ThreadId.make(threadId), tabId, siteHold: undefined })
            .pipe(Effect.ignore),
        );
      }
    }),
  );
  yield* manager.registerBeforeClose((snapshots) =>
    Effect.promise(async () => {
      await Promise.all(
        snapshots.map((snapshot) => {
          const key = tabKey(snapshot.threadId, snapshot.tabId);
          const tab = tabs.get(key);
          if (!tab) {
            const opening = pendingTabs.get(key);
            if (!opening) return;
            closedPendingTabs.add(key);
            if (!opening.promise) return;
            const settled = Promise.withResolvers<void>();
            const timeout = setTimeout(settled.resolve, PAGE_CLOSE_GRACE_MS);
            void opening.promise.finally(settled.resolve).catch(constVoid);
            return settled.promise.finally(() => clearTimeout(timeout));
          }
          void tab.control.close().catch(constVoid);
          if (!tab.desktop || tab.dialog !== null) return;
          const pending = unloadingTabs.get(tab);
          if (pending) return pending;
          tab.closing = true;
          const unloading = unloadDesktopPage(tab.page);
          unloadingTabs.set(tab, unloading);
          return unloading;
        }),
      );
    }),
  );

  const dropTab = (tab: ServerTab, closeSession: boolean, reopening = false) => {
    stopTabWatches(tab, "closed");
    const key = tabKey(tab.threadId, tab.tabId);
    if (tabs.get(key) !== tab) return;
    tabs.delete(key);
    tab.closing = true;
    tab.viewerAttached?.resolve();
    clearAbortedNavigation(tab);
    // A desktop page outlives the connection unless its session closed with it.
    const end =
      (tab.desktop || reopening) && !closeSession && sessionOpen(tab) ? "reconnect" : "gone";
    // Nobody can pass a check in a closed tab; the next challenge page holds the site again.
    if (end === "gone")
      for (const [scope, hold] of siteHolds.heldBy(tab.tabId)) siteHolds.clear(scope, hold);
    for (const viewer of tab.viewers) viewer.push({ _tag: end });
    void tab.control.close().catch(constVoid);
    if (tab.desktop) void tab.desktop.close().catch(constVoid);
    else {
      const page = tab.page;
      // `runBeforeUnload` returns before the page closes, so closing an isolated context
      // must wait for it. A pending dialog would block `beforeunload` from running at all.
      const closed = page.isClosed()
        ? Promise.resolve()
        : new Promise<void>((resolve) => page.once("close", () => resolve()));
      // A renderer that never finishes unloading would outlive its tab, so force it closed.
      const forceClose = setTimeout(() => void page.close().catch(constVoid), PAGE_CLOSE_GRACE_MS);
      void closed.then(() => clearTimeout(forceClose));
      void page
        // A crashed page never answers `beforeunload`, so that close would not finish.
        .close({ runBeforeUnload: tab.dialog === null && tab.crash === undefined })
        .catch(() => page.close())
        .catch(constVoid);
      if (tab.isolatedContext) void closed.then(() => page.context().close()).catch(constVoid);
    }
    void tab.recording?.encoder.close().catch(constVoid);
    void NodeFSP.rm(downloadDir(tab), { recursive: true, force: true }).catch(constVoid);
    reportLiveTabs();
    if (closeSession) {
      runFork(manager.close({ threadId: tab.threadId, tabId: tab.tabId }).pipe(Effect.ignore));
    }
  };

  const desktopRenders = (snapshot: PreviewSessionSnapshot) =>
    desktopChannel.available && !snapshot.headless && snapshot.serverPage !== true
      ? runPromise(
          Effect.gen(function* () {
            const key = { threadId: snapshot.threadId, tabId: snapshot.tabId };
            // A reloaded desktop mounts a thread's tabs again only once an event for it arrives,
            // and a renderer still reloading misses events, so repeat it until the wait ends.
            if (!(yield* desktopChannel.isAttached(key))) {
              yield* manager
                .announce({ threadId: ThreadId.make(key.threadId), tabId: key.tabId })
                .pipe(Effect.ignore, Effect.repeat(Schedule.spaced("1 second")), Effect.forkScoped);
            }
            return yield* desktopChannel.awaitAttached(key, DESKTOP_ATTACH_TIMEOUT);
          }).pipe(Effect.scoped),
        )
      : Promise.resolve(false);

  const connectDesktop = async (snapshot: PreviewSessionSnapshot) => {
    const scope = await runPromise(Scope.make());
    try {
      const endpoint = await runPromise(
        desktopChannel
          .endpoint({ threadId: snapshot.threadId, tabId: snapshot.tabId })
          .pipe(Scope.provide(scope)),
      );
      const connected = await contexts.connectDesktopPage(endpoint);
      return {
        page: connected.page,
        close: async () => {
          await connected.browser.close().catch(constVoid);
          await runPromise(Scope.close(scope, Exit.void));
        },
      };
    } catch (cause) {
      await runPromise(Scope.close(scope, Exit.void));
      throw cause;
    }
  };

  const createTab = async (
    snapshot: PreviewSessionSnapshot,
    authorize: () => Promise<void>,
    crashedUrl?: string,
  ): Promise<ServerTab> => {
    await authorize();
    const adopted = adoptedPages.get(tabKey(snapshot.threadId, snapshot.tabId));
    adoptedPages.delete(tabKey(snapshot.threadId, snapshot.tabId));
    let desktop: Awaited<ReturnType<typeof connectDesktop>> | null = null;
    let context: BrowserContext | undefined;
    let isolatedContext = false;
    let pendingPage: Page | undefined = adopted?.page;
    let pendingCdp: CDPSession | undefined;
    let createdTab: ServerTab | undefined;
    try {
      desktop =
        adopted === undefined && (await desktopRenders(snapshot))
          ? await connectDesktop(snapshot)
          : null;
      // Otherwise a desktop that attaches late, or has no channel to draw it, shows its own page.
      if (!desktop) {
        await runPromise(
          manager
            .markHeadless({ threadId: ThreadId.make(snapshot.threadId), tabId: snapshot.tabId })
            .pipe(Effect.ignore),
        );
      }
      isolatedContext =
        adopted === undefined &&
        desktop === null &&
        ((snapshot.automationOwner !== undefined && snapshot.profileId === undefined) ||
          snapshot.profileId === INCOGNITO_BROWSER_PROFILE_ID);
      await authorize();
      context =
        adopted?.page.context() ??
        desktop?.page.context() ??
        (await contexts.contextFor(
          snapshot.profileId ?? "default",
          isolatedContext ? tabKey(snapshot.threadId, snapshot.tabId) : undefined,
        ));
      await authorize();
      if (adopted?.page.isClosed()) throw new Error("The popup closed before it opened.");
      await authorize();
      const page = (pendingPage = adopted?.page ?? desktop?.page ?? (await context.newPage()));
      await authorize();
      const cdp = (pendingCdp = await context.newCDPSession(page));
      // The desktop page already has its own clipboard.
      if (!desktop) await grantClipboardWrite(context, cdp);
      await installPdfDocumentNavigation(cdp);
      await authorize();
      // The desktop's page is already a normal browser. A popup's first request went out
      // before it became a tab, under the launch user agent; everything after presents as Chrome.
      if (!desktop) await presentAsChrome(cdp, contexts.identity);
      await authorize();
      if (!desktop) await recordCopies(cdp, authorize);
      page.setDefaultTimeout(NAVIGATION_TIMEOUT_MS);
      page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
      const control = new SessionControl(snapshot.automationOwner ?? null, () => {
        ServerBrowserPage.invalidateRefs(page);
        if (createdTab) stopTabWatches(createdTab, "ownership");
      });
      let pendingFileChooserOffer = Promise.resolve();
      const tab: ServerTab = (createdTab = {
        authorize,
        threadId: ThreadId.make(snapshot.threadId),
        tabId: snapshot.tabId,
        page,
        cdp,
        frameSessions: new Map(),
        copyReads: Promise.resolve(),
        copyKeyedUntil: 0,
        createdAt: Date.now(),
        usedAt: Date.now(),
        viewers: new Set(),
        viewerAttached: null,
        consoleEntries: [],
        networkEntries: [],
        actionTimeline: [],
        control,
        agentOperating: false,
        mainDocument: null,
        siteHoldCheck: Promise.resolve(),
        fileChooserIntercepted: false,
        onFileChooser: (chooser) => {
          pendingFileChooserOffer = pendingFileChooserOffer
            .then(() => offerFileChooser(tab, chooser))
            .catch(constVoid);
        },
        watches: new Set(),
        isolatedContext,
        desktop: desktop === null ? null : { close: desktop.close },
        profileId: snapshot.profileId,
        openerTabId: adopted?.openerTabId,
        downloads: [],
        fileChooser: null,
        dialog: null,
        setting: snapshot.viewport ?? FILL_PREVIEW_VIEWPORT,
        colorScheme: "system",
        appliedColorScheme: undefined,
        desktopViewport: null,
        desktopViewportRead: Promise.resolve(null),
        desktopViewportNext: null,
        desktopViewportStale: true,
        desktopViewportChanges: 0,
        desktopResize: Promise.withResolvers<void>(),
        desktopResizeUndrawn: false,
        loading: false,
        loadError: undefined,
        // The page a crash left behind is blank; the session still names what crashed.
        crash:
          !desktop && !adopted && snapshot.navStatus._tag === "LoadFailed"
            ? {
                url: snapshot.navStatus.url,
                code: snapshot.navStatus.code,
                description: snapshot.navStatus.description,
              }
            : undefined,
        crashedUrl,
        closing: false,
        recording: null,
        recordingStart: null,
        initialNavigation: null,
        pendingInitialRequestUrl: null,
        navigationGeneration: 0,
        stoppedGeneration: null,
        titleWatch: null,
        lastReport: null,
        abortedNavigation: null,
        captureLock: Promise.resolve(),
        capturing: 0,
      });
      if (!desktop) {
        await authorize();
        await resizeAgentViewport(
          page,
          cdp,
          fixedViewportSize(tab.setting) ?? UNATTACHED_FILL_VIEWPORT,
          1,
        );
      }
      await applyRendering(tab, snapshot, authorize);
      if (desktop) {
        // The desktop resizes and zooms its webview itself; viewers map input with the new size.
        const remeasure = () => {
          tab.desktopViewportStale = true;
          tab.desktopViewportChanges += 1;
          if (tab.viewers.size > 0) return broadcastViewport(tab).catch(constVoid);
        };
        cdp.on("Page.frameResized", () => {
          const drawn = tab.desktopResize;
          tab.desktopResize = Promise.withResolvers();
          drawn.resolve();
          return remeasure();
        });
        // A read a navigation broke has a fresh page to measure.
        cdp.on("Page.loadEventFired", remeasure);
        // Closing a dialog resizes nothing, and the host can close it without us.
        cdp.on("Page.javascriptDialogClosed", () => {
          tab.dialog = null;
          return remeasure();
        });
        await cdp.send("Page.enable");
      }
      // Playwright's `request.frame()` throws for a request issued before its frame exists, and a
      // throw from a page listener is uncaught, so such a request is not a main-frame navigation.
      const isMainFrameNavigation = (request: {
        isNavigationRequest(): boolean;
        frame(): unknown;
      }) => {
        try {
          return request.isNavigationRequest() && request.frame() === page.mainFrame();
        } catch {
          return false;
        }
      };
      // Chromium aborts an app link's request and keeps the page, so it is no navigation.
      const isMainNavigation = (request: {
        isNavigationRequest(): boolean;
        url(): string;
        frame(): unknown;
      }) => appLinkUrl(request.url()) === null && isMainFrameNavigation(request);
      const navigationGenerations = new WeakMap<object, number>();
      page.on("request", (request) => {
        if (appLinkUrl(request.url()) !== null && isMainFrameNavigation(request)) {
          // A redirect into an app link ends the navigation that led there.
          const redirectedFrom = request.redirectedFrom();
          const generation = redirectedFrom && navigationGenerations.get(redirectedFrom);
          if (generation) settleAbortedNavigation(tab, generation, request.url(), undefined);
          return;
        }
        if (!isMainNavigation(request)) return;
        stopTabWatches(tab, "navigation");
        // Playwright can deliver the first request after Stop has already settled its navigation.
        const stoppedInitialRequest =
          request.url() === tab.pendingInitialRequestUrl &&
          tab.stoppedGeneration === tab.navigationGeneration;
        tab.pendingInitialRequestUrl = null;
        navigationGenerations.set(request, ++tab.navigationGeneration);
        clearAbortedNavigation(tab);
        tab.loading = true;
        tab.loadError = undefined;
        tab.crash = undefined;
        tab.crashedUrl = undefined;
        if (stoppedInitialRequest) {
          tab.stoppedGeneration = tab.navigationGeneration;
          settleStoppedNavigation(tab, tab.navigationGeneration);
          return;
        }
        report(tab, {
          _tag: "Loading",
          url: ServerBrowserPage.withoutDataPayload(request.url()).slice(0, 2048),
          title: "",
        });
      });
      page.on("load", () => {
        tab.pendingInitialRequestUrl = null;
        tab.loading = false;
        void reportLoaded(tab);
      });
      // frameNavigated is cross-document only; a select's handle dies with its document.
      cdp.on("Page.frameNavigated", ({ frame }) => {
        if (!frame.parentId) for (const viewer of tab.viewers) withdrawSelectMenu(viewer);
      });
      page.on("framenavigated", (frame) => {
        const open = tab.fileChooser;
        if (open)
          void isFileChooserCurrent(open.chooser).then((current) => {
            if (!current && tab.fileChooser?.chooser === open.chooser) closeFileChooser(tab);
          });
        if (frame === page.mainFrame()) stopTabWatches(tab, "navigation");
        // Same-document navigations (SPA routes) fire no load event.
        if (frame === page.mainFrame() && !tab.loading) void reportLoaded(tab);
        if (!desktop) void recordFrameCopies(tab, frame);
      });
      // An adopted popup's frames may have loaded before the tab listened.
      if (adopted) for (const frame of page.frames()) void recordFrameCopies(tab, frame);
      page.on("requestfailed", (request) => {
        const errorText = request.failure()?.errorText ?? "";
        pushBounded(tab.networkEntries, {
          url: ServerBrowserPage.withoutDataPayload(request.url()),
          method: request.method(),
          status: null,
          failed: true,
          errorText,
          timestamp: new Date().toISOString(),
        });
        if (!isMainNavigation(request)) return;
        // A navigation a newer one replaced leaves the status to the newer one. A
        // popup's first request can predate the listener, so it is untracked and
        // superseded by any tracked one.
        const generation = navigationGenerations.get(request) ?? 0;
        if (generation !== tab.navigationGeneration) return;
        // An aborted navigation is usually a download; its `download` event settles the status.
        if (errorText.includes("ERR_ABORTED")) {
          if (tab.stoppedGeneration === generation) {
            settleStoppedNavigation(tab, generation);
            return;
          }
          const url = request.url();
          const navigationGeneration = tab.navigationGeneration;
          clearAbortedNavigation(tab);
          tab.abortedNavigation = {
            url,
            generation: navigationGeneration,
            timer: setTimeout(() => {
              if (tab.abortedNavigation?.generation !== navigationGeneration) return;
              tab.abortedNavigation = null;
              if (!tab.closing) settleAbortedNavigation(tab, navigationGeneration, url, undefined);
            }, ABORTED_NAVIGATION_DOWNLOAD_WAIT_MS),
          };
          return;
        }
        tab.loading = false;
        const { code, description } = ServerBrowserPage.parseNetError(errorText);
        const url = ServerBrowserPage.withoutDataPayload(request.url()).slice(0, 2048);
        tab.loadError = { url, code, description };
        report(tab, { _tag: "LoadFailed", url, title: "", code, description });
      });
      page.on("response", (response) => {
        pushBounded(tab.networkEntries, {
          url: ServerBrowserPage.withoutDataPayload(response.url()),
          method: response.request().method(),
          status: response.status(),
          failed: false,
          timestamp: new Date().toISOString(),
        });
        const isDocument = isMainNavigation(response.request());
        // A same-site API answer can itself be the check a page then shows without navigating.
        if (
          !isDocument &&
          response.status() >= 400 &&
          (response.headers()["cf-mitigated"] !== undefined ||
            response.headers()["x-dd-b"] !== undefined ||
            response.headers()["x-datadome"] !== undefined) &&
          siteOf(response.url()) === siteOf(page.url())
        ) {
          const answer = { status: response.status(), headers: response.headers() };
          tab.siteHoldCheck = Promise.all([
            tab.siteHoldCheck,
            response
              .text()
              .catch(() => "")
              .then((body) => {
                const verdict = classifyAntiBotPage({ ...answer, html: body.slice(0, 65_536) });
                if (verdict) return raiseSiteHold(tab, verdict, response.url());
              })
              .catch(constVoid),
          ]).then(constVoid);
        }
        if (isDocument) {
          const document = {
            status: response.status(),
            headers: response.headers(),
            url: response.url(),
            at: Date.now(),
          };
          tab.mainDocument = document;
          if (document.status >= 400 || document.headers["cf-mitigated"] !== undefined)
            tab.siteHoldCheck = Promise.all([
              tab.siteHoldCheck,
              response
                .text()
                .catch(() => "")
                .then((html) => {
                  const verdict = classifyAntiBotPage({ ...document, html });
                  if (verdict) return raiseSiteHold(tab, verdict, response.url());
                })
                .catch(constVoid),
            ]).then(constVoid);
        }
      });
      page.on("console", (message) => {
        pushBounded(tab.consoleEntries, {
          level: message.type(),
          text: ServerBrowserPage.consoleText(message).slice(0, 2_000),
          timestamp: new Date().toISOString(),
        });
      });
      page.on("dialog", (dialog) => {
        // Closing is already decided, so an unsaved-changes guard must not keep the page open.
        if (tab.closing && dialog.type() === "beforeunload") {
          void dialog.accept().catch(constVoid);
          return;
        }
        tab.dialog = dialog;
        broadcastControl(tab);
      });
      page.on("download", (download) => saveDownload(tab, download));
      updateFileChooserInterception(tab);
      // Popups become tabs and keep `window.opener`, so sign-in popups can report back.
      page.on("popup", (popup) => void adoptPopup(tab, popup));
      const key = tabKey(tab.threadId, tab.tabId);
      if (closedPendingTabs.delete(key)) {
        if (desktop) {
          tab.closing = true;
          await unloadDesktopPage(page).catch(constVoid);
        }
        await control.close().catch(constVoid);
        if (desktop) await desktop.close().catch(constVoid);
        else await page.close().catch(constVoid);
        if (isolatedContext) await context.close().catch(constVoid);
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      page.on("close", () => dropTab(tab, true));
      page.on("crash", () => void reopenCrashedTab(tab));
      if (page.isClosed()) {
        await control.close().catch(constVoid);
        await page.close().catch(constVoid);
        if (isolatedContext) await context.close().catch(constVoid);
        await runPromise(
          manager.close({ threadId: tab.threadId, tabId: tab.tabId }).pipe(Effect.ignore),
        );
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      tabs.set(key, tab);
      if (pendingTabs.get(key)?.settled && mayCloseSettledTab(tab)) {
        dropTab(tab, true);
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      reportLiveTabs();
      if (desktop) void page.waitForLoadState("load").then(() => reportLoaded(tab), constVoid);
      // Existing desktop and popup PDFs can start loading before interception attaches.
      if (
        snapshot.navStatus._tag === "Loading" &&
        ((!adopted && !desktop) || isPdfAssetUrl(snapshot.navStatus.url))
      ) {
        await authorize();
        tab.loading = true;
        const initialUrl = URL.parse(snapshot.navStatus.url);
        if (initialUrl) initialUrl.hash = "";
        tab.pendingInitialRequestUrl = initialUrl?.href ?? null;
        const options = { waitUntil: "commit", timeout: NAVIGATION_TIMEOUT_MS } as const;
        tab.initialNavigation = (
          (desktop || adopted) && page.url() === snapshot.navStatus.url
            ? page.reload(options)
            : page.goto(snapshot.navStatus.url, options)
        ).then(constVoid, (cause: unknown) => {
          throw ServerBrowserPage.toOperationError(cause, NAVIGATION_TIMEOUT_MS);
        });
        // Background creation keeps the failed tab; automation awaits the original error.
        if (tab.control.agentId !== null) tab.control.track(tab.initialNavigation);
        else void tab.initialNavigation.catch(constVoid);
      }
      return tab;
    } catch (cause) {
      if (
        createdTab !== undefined &&
        tabs.get(tabKey(createdTab.threadId, createdTab.tabId)) === createdTab
      ) {
        dropTab(createdTab, true);
      } else {
        await pendingCdp?.detach().catch(constVoid);
        if (desktop) await desktop.close().catch(constVoid);
        else await pendingPage?.close().catch(constVoid);
        if (isolatedContext) await context?.close().catch(constVoid);
      }
      throw cause;
    }
  };

  const downloadsRoot = NodePath.join(config.stateDir, "server-browser", "downloads");
  // Saved downloads are indexed in memory, so an earlier run's files can never be opened again.
  yield* Effect.tryPromise({
    try: () => NodeFSP.rm(downloadsRoot, { recursive: true, force: true }),
    catch: (cause) => new ServerBrowserDownloadsCleanupError({ cause }),
  }).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("server preview could not clear earlier downloads", { cause }),
    ),
  );
  // Tab ids are opaque server strings; hashing keeps them out of the path.
  const downloadDir = (tab: ServerTab) =>
    NodePath.join(
      downloadsRoot,
      NodeCrypto.createHash("sha256").update(tabKey(tab.threadId, tab.tabId)).digest("hex"),
    );

  const clearAbortedNavigation = (tab: ServerTab) => {
    if (tab.abortedNavigation !== null) clearTimeout(tab.abortedNavigation.timer);
    tab.abortedNavigation = null;
  };

  const settleStoppedNavigation = (tab: ServerTab, generation: number) => {
    if (tab.navigationGeneration !== generation || !tab.loading) return;
    clearAbortedNavigation(tab);
    tab.loading = false;
    if (tab.page.url() === "about:blank") report(tab, { _tag: "Idle" });
    else void reportLoaded(tab);
  };

  const settleAbortedNavigation = (
    tab: ServerTab,
    generation: number,
    url: string,
    offered: { readonly id: string; readonly fileName: string } | undefined,
  ) => {
    if (tab.navigationGeneration !== generation) return;
    tab.loading = false;
    if (tab.page.url() !== "about:blank") {
      void reportLoaded(tab);
      return;
    }
    report(tab, {
      _tag: "LoadFailed",
      url: ServerBrowserPage.withoutDataPayload(url).slice(0, 2048),
      title: "",
      code: -3,
      description: "ERR_ABORTED",
      ...(offered === undefined ? {} : { download: offered }),
    });
  };

  const saveDownload = async (tab: ServerTab, download: Download) => {
    const controller = [...tab.viewers].find((viewer) => viewer.id === tab.control.controller);
    const generation = tab.control.generation;
    const agentId = tab.control.agentId;
    const authorize = async () => {
      if (controller === undefined) await tab.authorize();
      else await controller.authorize();
      if (
        tab.closing ||
        tabs.get(tabKey(tab.threadId, tab.tabId)) !== tab ||
        tab.control.generation !== generation ||
        tab.control.agentId !== agentId ||
        tab.control.controller !== (controller?.id ?? null) ||
        (controller !== undefined && !controller.canOperate)
      )
        throw new BrowserControlInterrupted();
    };
    const id = NodeCrypto.randomUUID();
    const path = NodePath.join(downloadDir(tab), id);
    const navigation =
      download.url() === tab.abortedNavigation?.url ? tab.abortedNavigation.generation : null;
    if (navigation !== null) clearAbortedNavigation(tab);
    let offered: { readonly id: string; readonly fileName: string } | undefined;
    try {
      await authorize();
      if (await download.failure()) return;
      await authorize();
      await NodeFSP.mkdir(downloadDir(tab), { recursive: true, mode: 0o700 });
      // mkdir leaves the mode of a folder that already exists, such as one an older build made.
      await NodeFSP.chmod(downloadDir(tab), 0o700);
      await authorize();
      await download.saveAs(path);
      // saveAs copies with the process umask; a download should stay readable by its owner only.
      await NodeFSP.chmod(path, 0o600);
      await authorize();
      const { size } = await NodeFSP.stat(path);
      await authorize();
      if (tab.closing || size > DOWNLOAD_MAX_BYTES) {
        await NodeFSP.rm(path, { force: true });
        return;
      }
      const file = await NodeFSP.open(
        path,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      let digest: string;
      let fingerprint: string;
      const blockDigests: Array<string> = [];
      try {
        const initial = await file.stat();
        if (!initial.isFile() || initial.size !== size)
          throw new Error("Native download identity changed.");
        fingerprint = fileFingerprint(initial);
        const hash = NodeCrypto.createHash("sha256");
        for (let offset = 0; offset < size; offset += DOWNLOAD_BLOCK_BYTES) {
          await authorize();
          const block = await readDownloadBlockBytes(file, offset, size);
          await authorize();
          hash.update(block);
          blockDigests.push(NodeCrypto.createHash("sha256").update(block).digest("hex"));
        }
        if (fileFingerprint(await file.stat()) !== fingerprint)
          throw new Error("Native download changed during verification.");
        digest = hash.digest("hex");
        await authorize();
      } finally {
        await file.close();
      }
      const saved: ServerDownload = {
        digest,
        fingerprint,
        blockDigests,
        id,
        fileName:
          NodePath.basename(download.suggestedFilename())
            .replace(/[\x00-\x1f\x7f/\\]/g, "_")
            .slice(0, 255)
            .trim() || "download",
        path,
        sizeBytes: size,
        url: ServerBrowserPage.withoutDataPayload(download.url()).slice(0, 2048),
        completedAt: new Date().toISOString(),
      };
      tab.downloads.push(saved);
      for (const evicted of tab.downloads.splice(0, tab.downloads.length - DOWNLOAD_LIMIT)) {
        await NodeFSP.rm(evicted.path, { force: true }).catch(constVoid);
      }
      await authorize();
      offered = { id, fileName: saved.fileName };
      const shownInTab =
        navigation === tab.navigationGeneration && tab.page.url() === "about:blank";
      if (shownInTab) return;
      // Only the person driving the page gets the file offered; agents read it from status.
      await authorize();
      controller?.push({
        _tag: "download",
        id,
        fileName: saved.fileName,
        sizeBytes: saved.sizeBytes,
      });
    } catch (cause) {
      const savedIndex = tab.downloads.findIndex((saved) => saved.id === id);
      if (savedIndex !== -1) tab.downloads.splice(savedIndex, 1);
      await NodeFSP.rm(path, { force: true }).catch(constVoid);
      runFork(Effect.logWarning("server preview download failed", { cause }));
    } finally {
      if (navigation !== null && !tab.closing) {
        settleAbortedNavigation(tab, navigation, download.url(), offered);
      }
      // saveAs copies the artifact; cached profile contexts otherwise retain the original.
      await download.delete().catch(constVoid);
    }
  };

  const fileChooserMessage = (tab: ServerTab): ServerBrowserViewerOutput | null =>
    tab.fileChooser
      ? {
          _tag: "fileChooser",
          id: tab.fileChooser.id,
          multiple: tab.fileChooser.chooser.isMultiple(),
          accept: tab.fileChooser.accept,
        }
      : null;

  // Not isConnected: upload helpers often click an input never added to the page. The handle's
  // context is destroyed with its document, so a replaced document rejects.
  const isFileChooserCurrent = (chooser: FileChooser) =>
    chooser
      .element()
      .evaluate(
        (element) =>
          element.ownerDocument === (globalThis as unknown as { document: unknown }).document,
      )
      .catch(() => false);

  const offerFileChooser = async (tab: ServerTab, chooser: FileChooser) => {
    const previous = tab.fileChooser;
    const accept =
      (await chooser
        .element()
        .getAttribute("accept")
        .catch(() => null)) ?? "";
    if (!(await isFileChooserCurrent(chooser))) return;
    if (previous) closeFileChooser(tab);
    tab.fileChooser = {
      id: NodeCrypto.randomUUID(),
      chooser,
      accept: accept.slice(0, 1024),
      offeredTo: null,
    };
    pushFileChooser(tab);
  };

  const pushFileChooser = (tab: ServerTab) => {
    const open = tab.fileChooser;
    const controller = [...tab.viewers].find((viewer) => viewer.id === tab.control.controller);
    if (!open || !controller) return;
    if (open.offeredTo !== controller.id) {
      // A new controller gets a fresh id; the previous one can no longer answer.
      tab.fileChooser = { ...open, id: NodeCrypto.randomUUID(), offeredTo: controller.id };
      if (open.offeredTo !== null)
        for (const viewer of tab.viewers) viewer.push({ _tag: "fileChooserClosed", id: open.id });
    }
    const message = fileChooserMessage(tab);
    if (message) controller.push(message);
  };

  const closeFileChooser = (tab: ServerTab) => {
    const open = tab.fileChooser;
    if (!open) return;
    tab.fileChooser = null;
    for (const viewer of tab.viewers) viewer.push({ _tag: "fileChooserClosed", id: open.id });
  };

  const setChooserFiles = async (
    input: Parameters<ServerBrowser["Service"]["answerFileChooser"]>[0],
  ) => {
    const authorize = input.authorize ?? (async () => {});
    await authorize();
    const tab = tabs.get(tabKey(input.threadId, input.tabId));
    const open = tab?.fileChooser;
    if (
      !tab ||
      !open ||
      open.id !== input.chooserId ||
      open.offeredTo === null ||
      open.offeredTo !== tab.control.controller
    )
      return false;
    const controller = [...tab.viewers].find((viewer) => viewer.id === open.offeredTo);
    if (controller?.resourceViewerId !== input.resourceViewerId) return false;
    return tab.control.human(
      open.offeredTo,
      async () => {
        if (tab.fileChooser !== open) return false;
        if (input.files.length > 0) {
          await open.chooser.setFiles(
            open.chooser.isMultiple() ? [...input.files] : input.files.slice(0, 1),
          );
        }
        if (tab.fileChooser === open) closeFileChooser(tab);
        return true;
      },
      authorize,
    );
  };
  /** The agent's files go to a named file input, or else to the page's open picker. */
  const uploadFiles = async (
    tab: ServerTab,
    input: PreviewAutomationUploadInput,
    authorize: () => Promise<void>,
  ) => {
    if (input.paths === undefined || input.artifacts !== undefined || input.resource !== undefined)
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationExecutionError",
        message: "Browser files must be resolved by their native staging owner before dispatch.",
      });
    const resolved = { ...input, paths: input.paths };
    const relative = input.paths.find((path) => !NodePath.isAbsolute(path));
    if (relative !== undefined)
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationExecutionError",
        message: `Upload paths must be absolute: ${relative}`,
      });
    if (await ServerBrowserPage.setInputFiles(tab.page, resolved)) return undefined;
    const open = tab.fileChooser;
    if (!open)
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationExecutionError",
        message:
          "No file picker is open. Click the page's upload control first, or pass the file input's locator.",
      });
    if (input.paths.length > 1 && !open.chooser.isMultiple())
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationExecutionError",
        message: "This file picker accepts one file.",
      });
    if (input.paths.length > 0) {
      await authorize();
      await open.chooser.setFiles([...input.paths], {
        timeout: input.timeoutMs ?? NAVIGATION_TIMEOUT_MS,
      });
    }
    if (tab.fileChooser === open) closeFileChooser(tab);
    return undefined;
  };

  const answerFileChooser: ServerBrowser["Service"]["answerFileChooser"] = (input) =>
    Effect.promise(() => setChooserFiles(input).catch(() => false));

  const openDownload: ServerBrowser["Service"]["openDownload"] = Effect.fn(
    "ServerBrowser.openDownload",
  )(function* (input) {
    const tab = tabs.get(tabKey(input.threadId, input.tabId));
    const download = tab?.downloads.find((candidate) => candidate.id === input.downloadId);
    if (tab === undefined || tab.closing || download === undefined) return Option.none();
    const { serverEpoch } = yield* manager.list({ threadId: tab.threadId });
    return Option.some({
      path: download.path,
      fileName: download.fileName,
      resource: {
        _tag: "native-browser-download" as const,
        resource: {
          type: "preview" as const,
          environmentId,
          threadId: tab.threadId,
          tabId: tab.tabId,
          serverEpoch,
        },
        downloadId: download.id,
        name: download.fileName,
        mimeType: "application/octet-stream" as const,
        sizeBytes: download.sizeBytes,
        digest: download.digest,
      },
    });
  });

  const nativeDownload = Effect.fn("ServerBrowser.nativeDownload")(function* (
    resource: NativeBrowserDownloadAssetResource,
    authorize: Effect.Effect<void, OrchestratorMcpFailure>,
    offset?: number,
  ) {
    const refused = () =>
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The native browser download is no longer available to its current reader.",
      });
    const current = Effect.gen(function* () {
      yield* authorize;
      const { serverEpoch } = yield* manager.list({ threadId: resource.resource.threadId });
      const tab = tabs.get(tabKey(resource.resource.threadId, resource.resource.tabId));
      const download = tab?.downloads.find((candidate) => candidate.id === resource.downloadId);
      if (
        resource.resource.environmentId !== environmentId ||
        resource.resource.serverEpoch !== serverEpoch ||
        tab === undefined ||
        tab.closing ||
        download === undefined ||
        download.digest !== resource.digest ||
        download.sizeBytes !== resource.sizeBytes ||
        download.fileName !== resource.name ||
        resource.mimeType !== "application/octet-stream"
      )
        return yield* refused();
      return download;
    });
    const download = yield* current;
    return yield* Effect.acquireUseRelease(
      Effect.tryPromise({
        try: () =>
          NodeFSP.open(download.path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW),
        catch: refused,
      }),
      (file) =>
        Effect.gen(function* () {
          const check = Effect.gen(function* () {
            if ((yield* current) !== download) return yield* refused();
            const info = yield* Effect.tryPromise({ try: () => file.stat(), catch: refused });
            if (!info.isFile() || fileFingerprint(info) !== download.fingerprint)
              return yield* refused();
          });
          yield* check;
          if (offset === undefined) return new Uint8Array(0);
          if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            offset >= download.sizeBytes ||
            offset % DOWNLOAD_BLOCK_BYTES !== 0
          )
            return yield* refused();
          const block = yield* Effect.tryPromise({
            try: () => readDownloadBlockBytes(file, offset, download.sizeBytes),
            catch: refused,
          });
          if (
            NodeCrypto.createHash("sha256").update(block).digest("hex") !==
            download.blockDigests[offset / DOWNLOAD_BLOCK_BYTES]
          )
            return yield* refused();
          yield* check;
          return block;
        }),
      (file) => Effect.promise(() => file.close()).pipe(Effect.orDie),
    );
  });

  // Only clipboard-write is granted: grantPermissions would show every other one as "denied".
  // Chrome drops an override when its session detaches, so it is set on a session as long-lived
  // as the context. Tabs opening together share one grant; a failed grant is retried.
  const contextGrants = new WeakMap<BrowserContext, Promise<void>>();
  const browserSessions = new WeakMap<object, Promise<CDPSession>>();
  const grantClipboardWrite = (context: BrowserContext, cdp: CDPSession): Promise<void> => {
    let grant = contextGrants.get(context);
    if (!grant) {
      grant = setClipboardWrite(context, cdp);
      contextGrants.set(context, grant);
      grant.catch(() => {
        if (contextGrants.get(context) === grant) contextGrants.delete(context);
      });
    }
    return grant;
  };
  const setClipboardWrite = async (context: BrowserContext, cdp: CDPSession) => {
    const browser = context.browser();
    const permission = { permission: { name: "clipboard-write" }, setting: "granted" } as const;
    if (browser) {
      const { targetInfo } = await cdp.send("Target.getTargetInfo");
      let session = browserSessions.get(browser);
      if (!session) {
        session = browser.newBrowserCDPSession();
        browserSessions.set(browser, session);
        const created = session;
        created.catch(() => {
          if (browserSessions.get(browser) === created) browserSessions.delete(browser);
        });
      }
      await (
        await session
      ).send("Browser.setPermission", {
        ...permission,
        ...(targetInfo.browserContextId ? { browserContextId: targetInfo.browserContextId } : {}),
      });
      return;
    }
    const reserved = await contexts.reservedPage(context);
    const session = await context.newCDPSession(reserved);
    await session.send("Browser.setPermission", permission);
  };

  /** Records the copies made in each frame a session reaches. */
  const recordCopies = async (cdp: CDPSession, authorize = async () => {}) => {
    // Scripts for new documents run only while this session's Page domain is on.
    await authorize();
    await cdp.send("Page.enable");
    await authorize();
    await cdp.send("Page.addScriptToEvaluateOnNewDocument", {
      source: CLIPBOARD_SCRIPT,
      worldName: CLIPBOARD_WORLD,
      runImmediately: true,
    });
  };

  /**
   * A frame in its own process, such as one with an `Origin-Agent-Cluster`
   * header, is out of the page session's reach, so it records copies through
   * its own session until that target closes.
   */
  const recordFrameCopies = async (tab: ServerTab, frame: Frame) => {
    if (frame === tab.page.mainFrame() || tab.frameSessions.has(frame)) return;

    const cdp = await tab.page
      .context()
      .newCDPSession(frame)
      .catch(() => null);
    if (!cdp) return;
    if (tab.frameSessions.has(frame) || frame.isDetached()) {
      await cdp.detach().catch(constVoid);
      return;
    }
    tab.frameSessions.set(frame, cdp);
    cdp.on("close", () => {
      if (tab.frameSessions.get(frame) !== cdp) return;
      tab.frameSessions.delete(frame);

      if (!frame.isDetached()) void recordFrameCopies(tab, frame);
    });
    await recordCopies(cdp, tab.authorize).catch(constVoid);
  };

  const takeSessionCopies = async (cdp: CDPSession, since: number, until: number) => {
    const tree = await cdp.send("Page.getFrameTree").catch(() => null);
    if (!tree) return [];
    const frameIds = (node: typeof tree.frameTree): Array<string> => [
      node.frame.id,
      ...(node.childFrames ?? []).flatMap(frameIds),
    ];
    return Promise.all(
      frameIds(tree.frameTree).map((frameId) =>
        cdp
          .send("Page.createIsolatedWorld", { frameId, worldName: CLIPBOARD_WORLD })
          .then(({ executionContextId }) =>
            cdp.send("Runtime.evaluate", {
              contextId: executionContextId,
              expression: `globalThis.takeCopy?.(${since}, ${until})`,
              returnByValue: true,
            }),
          )
          .then(
            ({ result }) => (typeof result.value === "string" ? result.value : ""),
            () => "",
          ),
      ),
    );
  };

  const takeCopiedText = async (tab: ServerTab, since: number, until: number) => {
    const copies = await Promise.all(
      [tab.cdp, ...tab.frameSessions.values()].map((cdp) => takeSessionCopies(cdp, since, until)),
    );
    return copies.flat().find((text) => text.length > 0);
  };

  const adoptPopup = async (opener: ServerTab, popup: Page) => {
    // The current controller owns a human popup, even when an agent opened its parent.
    const controller = [...opener.viewers].find(
      (viewer) => viewer.id === opener.control.controller,
    );
    const generation = opener.control.generation;
    const agentId = controller === undefined ? opener.control.agentId : null;
    const authorize = async () => {
      if (controller === undefined) return opener.authorize();
      await controller.authorize();
      if (
        opener.closing ||
        opener.control.controller !== controller.id ||
        opener.control.generation !== generation ||
        !controller.canOperate
      )
        throw new BrowserControlInterrupted();
    };
    // A popup past an agent's limit closes; its page sees window.open return a closed window.
    if (opener.closing || (agentId !== null && atTabLimit(agentId))) {
      await popup.close().catch(constVoid);
      return;
    }
    try {
      await authorize();
    } catch {
      await popup.close().catch(constVoid);
      return;
    }
    const url = popup.url();

    await runPromise(
      openTab({
        threadId: opener.threadId,
        ...(/^https?:/i.test(url) ? { url } : {}),
        runtime: "server",
        authorize,
        ...(opener.profileId === undefined ? {} : { profileId: opener.profileId }),
        colorScheme: opener.colorScheme,
        // Agent popups stay with the agent and only float when it asks, like its own opens.
        ...(agentId === null ? {} : { automationOwner: agentId, reveal: false }),
        // A person's popup from a server page is still that page's popup, not a desktop one.
        ...(opener.desktop === null ? { serverPage: true } : {}),
        beforePublish: (snapshot) =>
          adoptedPages.set(tabKey(snapshot.threadId, snapshot.tabId), {
            page: popup,
            openerTabId: opener.tabId,
          }),
      }),
    ).then(
      async (snapshot) => {
        // The opener stays open behind it, so `window.opener` can still report back.
        if (controller !== undefined) {
          try {
            await authorize();
            const { serverEpoch } = await runPromise(
              manager.list({ threadId: ThreadId.make(snapshot.threadId) }),
            );
            await authorize();
            controller.push({
              _tag: "popup",
              tabId: snapshot.tabId,
              resource: {
                type: "preview",
                environmentId,
                threadId: ThreadId.make(snapshot.threadId),
                tabId: snapshot.tabId,
                serverEpoch,
              },
            });
          } catch {
            await popup.close().catch(constVoid);
          }
        }
      },
      async () => {
        await popup.close().catch(constVoid);
      },
    );
  };

  const ensureTab = (
    snapshot: PreviewSessionSnapshot,
    authorize?: () => Promise<void>,
  ): Promise<ServerTab> => {
    const key = tabKey(snapshot.threadId, snapshot.tabId);
    if (authorize !== undefined && !tabAuthorizations.has(key))
      tabAuthorizations.set(key, authorize);
    const pending = pendingTabs.get(key);
    if (pending?.promise) return pending.promise;
    const existing = tabs.get(key);
    if (existing) return Promise.resolve(existing);
    if (snapshot.automationOwner !== undefined && pending === undefined)
      assertTabCapacity(snapshot.automationOwner);
    const opening = createTab(snapshot, tabAuthorizations.get(key) ?? (async () => {}))
      .catch((cause: unknown) => {
        runFork(
          Effect.logWarning(
            isHostSetupError(cause) ? cause.message : "server preview tab failed to start",
            { cause },
          ),
        );
        throw cause;
      })
      .finally(() => {
        pendingTabs.delete(key);
        closedPendingTabs.delete(key);
      });
    pendingTabs.set(key, {
      agentId: snapshot.automationOwner ?? null,
      threadId: snapshot.threadId,
      promise: opening,
      settled: false,
    });
    return opening;
  };

  const findTab = Effect.fn("ServerBrowser.findTab")(function* (threadId: string, tabId: string) {
    const existing = tabs.get(tabKey(threadId, tabId));
    if (existing) return existing;
    const { sessions } = yield* manager.list({ threadId: ThreadId.make(threadId) });
    const snapshot = sessions.find(
      (session) => session.tabId === tabId && session.runtime === "server",
    );
    if (!snapshot) return yield* new ServerBrowserTabNotFoundError({ threadId, tabId });
    return yield* Effect.tryPromise({
      try: () => ensureTab(snapshot),
      catch: (cause) => (isTabNotFound(cause) ? cause : new ServerBrowserLaunchError({ cause })),
    });
  });

  const openingReservations = () =>
    [...pendingTabs.entries()].filter(([key]) => !tabs.has(key)).map(([, pending]) => pending);
  const serverAtTabLimit = () => tabs.size + openingReservations().length >= SERVER_TAB_LIMIT;
  const agentAtTabLimit = (agentSessionId: string) =>
    [...tabs.values()].filter((tab) => tab.control.agentId === agentSessionId).length +
      openingReservations().filter((pending) => pending.agentId === agentSessionId).length >=
    AGENT_TAB_LIMIT;
  const atTabLimit = (agentSessionId: string) =>
    serverAtTabLimit() || agentAtTabLimit(agentSessionId);

  const assertTabCapacity = (agentSessionId: string) => {
    if (atTabLimit(agentSessionId))
      throw new BrowserControlInterrupted({
        message: `Too many server browser tabs are open (${AGENT_TAB_LIMIT} per agent session, ${SERVER_TAB_LIMIT} per server). Close one with t3_preview_close or reuse one from preview_status tabs.`,
        reason: "tabLimit",
      });
  };

  const isVisible = (tab: ServerTab) => tab.viewers.size > 0 || desktopChannel.isShown(tab);

  const reclaimingTabs = new Set<ServerTab>();
  const idleAgentTabs = () => {
    const cutoff = Date.now() - AGENT_TAB_IDLE_MS;
    return [...tabs.values()].filter(
      (tab) =>
        tab.control.agentId !== null &&
        !reclaimingTabs.has(tab) &&
        !isVisible(tab) &&
        tab.watches.size === 0 &&
        siteHolds.heldBy(tab.tabId).length === 0 &&
        tab.usedAt < cutoff,
    );
  };

  // The desktop app renders every server tab, so idle desktop agent tabs would otherwise
  // hold the server's budget forever. Hidden ones give up their slots, longest idle first,
  // only when an agent needs one and has room for it.
  const freeIdleAgentSlot = async (agentSessionId: string) => {
    if (!serverAtTabLimit() || agentAtTabLimit(agentSessionId)) return;
    const longestIdle = idleAgentTabs().sort((left, right) => left.usedAt - right.usedAt)[0];
    if (longestIdle) {
      reclaimingTabs.add(longestIdle);
      try {
        await runPromise(
          manager.close({ threadId: longestIdle.threadId, tabId: longestIdle.tabId }),
        );
        dropTab(longestIdle, false);
      } finally {
        reclaimingTabs.delete(longestIdle);
      }
    }
  };

  // A Playwright launch must settle before its capacity can be released.
  const openTab = Effect.fn("ServerBrowser.openTab")(function* (
    input: Parameters<typeof manager.open>[0],
    options: { readonly reclaimIdleSlot?: boolean } = {},
  ) {
    let key: string = NodeCrypto.randomUUID();
    const reservation = { agentId: input.automationOwner ?? null };
    return yield* Effect.acquireUseRelease(
      Effect.tryPromise({
        try: async () => {
          if (input.automationOwner !== undefined) {
            while (
              options.reclaimIdleSlot &&
              serverAtTabLimit() &&
              !agentAtTabLimit(input.automationOwner) &&
              idleAgentTabs().length > 0
            ) {
              await freeIdleAgentSlot(input.automationOwner);
            }
            assertTabCapacity(input.automationOwner);
          }
          pendingTabs.set(key, reservation);
        },
        catch: ServerBrowserPage.toOperationError,
      }),
      () =>
        manager
          .open({
            ...input,
            beforePublish: (snapshot) => {
              pendingTabs.delete(key);
              key = tabKey(snapshot.threadId, snapshot.tabId);
              pendingTabs.set(key, reservation);
              input.beforePublish?.(snapshot);
            },
          })
          .pipe(
            Effect.flatMap((snapshot) =>
              Effect.tryPromise({
                try: () => ensureTab(snapshot, input.authorize),
                catch: ServerBrowserPage.toOperationError,
              }).pipe(
                Effect.onError(() =>
                  manager
                    .close({ threadId: ThreadId.make(snapshot.threadId), tabId: snapshot.tabId })
                    .pipe(Effect.ignore),
                ),
              ),
            ),
          ),
      () =>
        Effect.sync(() => {
          pendingTabs.delete(key);
        }),
    );
  }, Effect.uninterruptible);

  // Desktop browsing bypasses streamed viewers and server input tracking,
  // so these idle signals only apply to headless tabs.
  const closeIdleAgentTabs = () => {
    for (const tab of idleAgentTabs()) if (tab.desktop === null) dropTab(tab, true);
  };

  const latestThreadTab = (threadId: string, agentSessionId?: string) => {
    const threadTabs = [...tabs.values()].filter((tab) => tab.threadId === threadId);
    return (
      threadTabs
        .filter((tab) => tab.control.agentId === agentSessionId)
        .sort((left, right) => right.createdAt - left.createdAt)[0] ??
      threadTabs.sort(
        (left, right) =>
          Number(isVisible(right)) - Number(isVisible(left)) || right.usedAt - left.usedAt,
      )[0]
    );
  };

  const tabOwner = (tab: ServerTab) =>
    tab.control.controller !== null
      ? ("human" as const)
      : tab.control.agentId !== null
        ? ("agent" as const)
        : ("unclaimed" as const);

  const resolveReportedProfiles = (input: PreviewReportProfilesInput) => {
    const profiles = resolveBrowserProfiles(input.profiles);
    return {
      profiles,
      defaultProfileId:
        findBrowserProfile(profiles, input.defaultProfileId)?.id ?? DEFAULT_BROWSER_PROFILE_ID,
    };
  };

  // Clients report profiles only when they connect, so the catalog at the last report is
  // kept on disk for agents and scheduled tasks that open a profile after a restart.
  const savedProfilesPath = NodePath.join(
    config.stateDir,
    "server-browser",
    "reported-profiles.json",
  );
  const fileServices = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
  const saveProfilesLock = yield* Semaphore.make(1);
  let savedProfiles = yield* (yield* FileSystem.FileSystem).readFileString(savedProfilesPath).pipe(
    Effect.tapError((cause) =>
      cause.reason._tag === "NotFound"
        ? Effect.void
        : Effect.logWarning("failed to read saved browser profiles", {
            path: savedProfilesPath,
            cause,
          }),
    ),
    Effect.orElseSucceed(() => ""),
  );
  const reportsByClient = new Map<number, PreviewReportProfilesInput>();
  let nextClient = 0;
  const mirroredProfiles = new Map<string, BrowserProfile>(
    catalogRead._tag === "Success"
      ? catalogRead.success.map((profile) => [profile.id, profile])
      : [],
  );
  const availableProfiles = () =>
    resolveBrowserProfiles([
      ...mirroredProfiles.values(),
      ...(reportedProfiles?.profiles ?? BUILT_IN_BROWSER_PROFILES),
    ]);
  let reportedProfiles: {
    readonly profiles: ReadonlyArray<BrowserProfile>;
    readonly defaultProfileId: string;
  } | null = yield* decodeSavedProfiles(savedProfiles).pipe(
    Effect.map(resolveReportedProfiles),
    Effect.tapError((cause) =>
      savedProfiles === ""
        ? Effect.void
        : Effect.logWarning("ignoring invalid saved browser profiles", {
            path: savedProfilesPath,
            cause,
          }),
    ),
    Effect.orElseSucceed(() => null),
  );
  // Only reports change what is saved, so the order clients disconnect in at shutdown,
  // even while a save waits for the lock, cannot shrink the saved catalog.
  let catalogToSave = reportedProfiles;

  // Agent recordings encode here, so they need the client's Browser recording frame rate. The
  // last report outlives the client, which lets agents record while no client is connected.
  let recordingFrameRate = DEFAULT_BROWSER_RECORDING_FRAME_RATE;

  // Each client reports only the profiles in its own settings, so the catalog is
  // their union while they stay connected. The last catalog stays usable after
  // every client disconnects, as a single report did before.
  const rebuildReportedProfiles = () => {
    const byId = new Map<string, BrowserProfile>();
    let latest: PreviewReportProfilesInput | undefined;
    for (const report of reportsByClient.values()) {
      for (const profile of report.profiles) byId.set(profile.id, profile);
      latest = report;
    }
    if (latest === undefined) return;
    reportedProfiles = resolveReportedProfiles({
      profiles: [...byId.values()],
      defaultProfileId: latest.defaultProfileId,
    });
  };

  const saveReportedProfiles = Effect.fn("ServerBrowser.saveReportedProfiles")(function* () {
    if (catalogToSave === null) return;
    const contents = encodeSavedProfiles({
      profiles: catalogToSave.profiles.filter((profile) => !isBuiltInBrowserProfileId(profile.id)),
      defaultProfileId: catalogToSave.defaultProfileId,
    });
    if (contents === savedProfiles) return;
    yield* writeFileStringAtomically({ filePath: savedProfilesPath, contents }).pipe(
      Effect.provideContext(fileServices),
      Effect.tap(() =>
        Effect.sync(() => {
          savedProfiles = contents;
        }),
      ),
      Effect.catch((cause) =>
        Effect.logWarning("failed to save reported browser profiles", { cause }),
      ),
    );
  }, saveProfilesLock.withPermits(1));

  const profileReporter: ServerBrowser["Service"]["profileReporter"] = Effect.acquireRelease(
    Effect.sync(() => nextClient++),
    (client) =>
      Effect.sync(() => {
        reportsByClient.delete(client);
        rebuildReportedProfiles();
      }),
  ).pipe(
    Effect.map(
      (client): ProfileReporter =>
        (input) =>
          Effect.sync(() => {
            if (input.recordingFrameRate !== undefined)
              recordingFrameRate = input.recordingFrameRate;
            reportsByClient.delete(client);
            reportsByClient.set(client, input);
            rebuildReportedProfiles();
            catalogToSave = reportedProfiles;
          }).pipe(
            Effect.andThen(saveReportedProfiles),
            // A disconnect, even while an earlier save holds the lock, must not drop this
            // report's save or leave the saved-content cache stale.
            Effect.uninterruptible,
          ),
    ),
  );

  const profileStatus = () => {
    const profiles = availableProfiles();
    return {
      profiles: profiles.map((profile) => ({
        id: profile.id,
        name: profile.name,
        ...(profile.kind === "incognito" ? { incognito: true } : {}),
      })),
      ...(reportedProfiles
        ? { defaultProfileId: reportedProfiles.defaultProfileId }
        : mirroredProfiles.has(DEFAULT_BROWSER_PROFILE_ID)
          ? { defaultProfileId: DEFAULT_BROWSER_PROFILE_ID }
          : {}),
    };
  };

  const resolveOpenProfile = (requested: string | undefined): string | undefined => {
    if (requested === undefined)
      return (
        reportedProfiles?.defaultProfileId ??
        (mirroredProfiles.has(DEFAULT_BROWSER_PROFILE_ID) ? DEFAULT_BROWSER_PROFILE_ID : undefined)
      );
    const profiles = availableProfiles();
    const wanted = requested.toLowerCase();
    const match =
      findBrowserProfile(profiles, requested) ??
      profiles.find((profile) => profile.name.toLowerCase() === wanted);
    if (match) return match.id;
    throw new ServerBrowserPage.ServerBrowserOperationError({
      tag: "PreviewAutomationUnknownProfileError",
      message: `No browser profile is named "${requested}". Use one of: ${profiles
        .map((profile) => `${profile.name} (id ${profile.id})`)
        .join(", ")}.`,
    });
  };

  const loadErrorOf = (tab: ServerTab): PreviewAutomationLoadError | undefined => {
    if (tab.crash) return tab.crash;
    if (!tab.page.url().startsWith("chrome-error://")) return undefined;
    // A desktop failure from before CDP attached is superseded by any navigation we observe.
    if (tab.loadError || !tab.desktop || tab.navigationGeneration > 0) return tab.loadError;
    const reported = desktopChannel.loadFailure({ threadId: tab.threadId, tabId: tab.tabId });
    return reported && { ...reported, url: ServerBrowserPage.withoutDataPayload(reported.url) };
  };

  const assertNavigationSucceeded = (tab: ServerTab) => {
    const failure = loadErrorOf(tab);
    if (failure) {
      const diagnosticUrl = URL.parse(failure.url);
      if (diagnosticUrl) {
        diagnosticUrl.username = "";
        diagnosticUrl.password = "";
        diagnosticUrl.search = "";
        diagnosticUrl.hash = "";
      }
      const url = diagnosticUrl
        ? ServerBrowserPage.withoutDataPayload(diagnosticUrl.href)
        : "the requested page";
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationExecutionError",
        message: `Navigation to ${url} failed: ${failure.description}`,
        detail: { ...failure, url },
      });
    }
  };

  const withinStatusDeadline = <A>(read: Promise<A>, late: () => A): Promise<A> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<A>((resolve) => {
      timer = setTimeout(() => resolve(late()), STATUS_READ_MS);
    });
    return Promise.race([read, deadline]).finally(() => clearTimeout(timer));
  };

  const statusWithTitle = async (
    tab: ServerTab | undefined,
    agentSessionId?: string,
  ): Promise<PreviewAutomationStatus> => {
    if (!tab) {
      return {
        available: false,
        visible: false,
        tabId: null,
        url: null,
        title: null,
        loading: false,
        ...profileStatus(),
      };
    }
    const url = ServerBrowserPage.withoutDataPayload(tab.page.url());
    const { serverEpoch } = await runPromise(manager.list({ threadId: tab.threadId }));
    const resource = (candidate: ServerTab): PreviewViewerResource => ({
      type: "preview",
      environmentId,
      threadId: candidate.threadId,
      tabId: candidate.tabId,
      serverEpoch,
    });
    const loadError = loadErrorOf(tab);
    const statusUrl = loadError?.url ?? (url === "about:blank" ? null : url);
    const [viewport, title] = await Promise.all([
      // A late measurement still lands in desktopViewport for the next status.
      tab.desktop
        ? withinStatusDeadline(desktopStatusViewport(tab), () => tab.desktopViewport)
        : previewViewportSize(tab.page),
      statusUrl === null || tab.dialog
        ? null
        : withinStatusDeadline(
            tab.page.title().catch(() => ""),
            () => "",
          ).then((title) => title || null),
    ]);
    const status = {
      resource: resource(tab),
      available: true,
      visible: isVisible(tab),
      tabId: tab.tabId,
      url: statusUrl,
      title,
      loading: tab.loading,
      ...(loadError ? { loadError } : {}),
      control: {
        owner: tabOwner(tab),
        ownedByCaller: tab.control.agentId === agentSessionId,
        generation: tab.control.generation,
      },
      dialog: dialogStatus(tab),
      fileChooser: tab.fileChooser
        ? { multiple: tab.fileChooser.chooser.isMultiple(), accept: tab.fileChooser.accept }
        : null,
      viewportSetting: tab.setting,
      ...(viewport ? { viewport } : {}),
      tabs: [...tabs.values()]
        .filter((candidate) => candidate.threadId === tab.threadId)
        .map((candidate) => ({
          tabId: candidate.tabId,
          resource: resource(candidate),
          url:
            candidate.page.url() === "about:blank"
              ? null
              : (loadErrorOf(candidate)?.url ??
                ServerBrowserPage.withoutDataPayload(candidate.page.url())),
          ...(candidate.openerTabId === undefined ? {} : { openerTabId: candidate.openerTabId }),
          owner: tabOwner(candidate),
          ownedByCaller: candidate.control.agentId === agentSessionId,
          visible: isVisible(candidate),
          ...(candidate.profileId === undefined ? {} : { profileId: candidate.profileId }),
        })),
      ...profileStatus(),
      ...(() => {
        const hold = siteHoldOf(tab);
        return hold ? { siteHold: publicSiteHold(hold) } : {};
      })(),
      downloads: tab.downloads.map(
        ({ id, fileName, path, sizeBytes, url, completedAt, digest }) => ({
          resource: {
            _tag: "native-browser-download" as const,
            resource: resource(tab),
            downloadId: id,
            name: fileName,
            mimeType: "application/octet-stream" as const,
            sizeBytes,
            digest,
          },
          fileName,
          path,
          sizeBytes,
          url,
          completedAt,
        }),
      ),
    };
    return status;
  };

  const resolveNavigationUrl = (input: PreviewAutomationNavigateInput) => {
    if (input.url !== undefined) return normalizePreviewUrl(input.url);
    const target = input.target!;
    if (target.kind === "url") return normalizePreviewUrl(target.url);

    const path = target.path ?? "";
    return `${target.protocol ?? "http"}://localhost:${target.port}${path.startsWith("/") || path === "" ? path : `/${path}`}`;
  };

  /** A pending dialog also leaves the probe unanswered, but its script is waiting, not stuck. */
  const leaveBusyPage = (tab: ServerTab, cdp: CDPSession, budgetMs?: number) =>
    tab.dialog ? Promise.resolve() : ServerBrowserPage.stopUnresponsiveScript(cdp, budgetMs);

  const navigate = async (
    tab: ServerTab,
    url: string,
    readiness: "load" | "domContentLoaded" | "none",
    timeout: number,
  ) => {
    tab.pendingInitialRequestUrl = null;
    const startedAt = performance.now();
    const remaining = () => Math.max(1, timeout - (performance.now() - startedAt));
    await leaveBusyPage(tab, tab.cdp, timeout / 2);
    const navigation = tab.page.goto(url, { timeout: remaining(), waitUntil: "commit" });
    if (readiness === "none") {
      tab.control.track(navigation);
      return;
    }
    // Once the target commits, a stalled subresource only delays readiness; report the loading tab.
    const ready = () =>
      tab.page
        .waitForLoadState(readiness === "domContentLoaded" ? "domcontentloaded" : "load", {
          timeout: remaining(),
        })
        .catch((cause: unknown) => {
          if (!(cause instanceof Error && cause.name === "TimeoutError")) throw cause;
        });
    await navigation.then(ready).catch((cause: unknown) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      if (message.includes("Download is starting")) {
        throw new ServerBrowserPage.ServerBrowserOperationError({
          tag: "PreviewAutomationExecutionError",
          message: `${url} is a file this browser cannot show, so it was downloaded instead. preview_status lists it under downloads.`,
        });
      }
      if (/ERR_[A-Z_]+/.test(message)) {
        throw new ServerBrowserPage.ServerBrowserOperationError({
          tag: "PreviewAutomationExecutionError",
          message: `Navigation to ${url} failed: ${ServerBrowserPage.parseNetError(message).description}`,
        });
      }
      throw ServerBrowserPage.toOperationError(cause, timeout);
    });
    assertNavigationSucceeded(tab);
  };

  const withCaptureLock = <A>(tab: ServerTab, operation: () => Promise<A>): Promise<A> => {
    const run = tab.captureLock.then(() => {
      if (tab.closing) throw new Error("The preview tab closed.");
      return operation();
    });
    tab.captureLock = run.then(constVoid, constVoid);
    return run;
  };

  const startRecording = (
    tab: ServerTab,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
  ): Promise<Recording> => {
    const started = withCaptureLock(tab, async () => {
      await authorize();
      if (tab.recording) return tab.recording;
      const encoder = await contexts.scratchPage();
      let session: CDPSession | null = null;
      try {
        await authorize();
        await encoder.evaluate(ServerBrowserPage.recordingEncoderScript(recordingFrameRate));
        // Seed idle pages before returning so an immediate stop has a frame to encode.
        await authorize();
        const firstFrame = await ServerBrowserPage.captureViewport(tab.cdp, {
          format: "jpeg",
          quality: RECORDING_SCREENCAST.quality,
        });
        await encoder.evaluate(
          ([data, width]) =>
            (globalThis as unknown as EncoderWindow).__t3Recorder.frame(data, width),
          [
            firstFrame,
            previewViewportSize(tab.page)?.width ?? UNATTACHED_FILL_VIEWPORT.width,
          ] as const,
        );
        await authorize();
        const opened = await tab.page.context().newCDPSession(tab.page);
        session = opened;
        const framesInFlight = new Set<Promise<void>>();
        // Chrome withholds frames while acks are outstanding, so ack on arrival and let a
        // slow encoder skip to the newest frame instead of throttling the capture.
        let pendingFrame: readonly [string, number] | null = null;
        const deliverPending = async () => {
          while (pendingFrame) {
            const next = pendingFrame;
            pendingFrame = null;
            const accepted = await resourceAuthorization()
              .then(() =>
                encoder.evaluate(
                  ([data, width]) =>
                    (globalThis as unknown as EncoderWindow).__t3Recorder.frame(data, width),
                  next,
                ),
              )
              // Stop a retired producer; cleanup never consumes more private media.
              .catch(() => false);
            if (!accepted) {
              pendingFrame = null;
              await opened.send("Page.stopScreencast").catch(constVoid);
            }
          }
        };
        opened.on("Page.screencastFrame", (frame) => {
          void opened
            .send("Page.screencastFrameAck", { sessionId: frame.sessionId })
            .catch(constVoid);
          const cssWidth = previewViewportSize(tab.page)?.width ?? frame.metadata.deviceWidth;
          const delivering = framesInFlight.size > 0;
          pendingFrame = [frame.data, cssWidth];
          if (delivering) return;
          const delivered: Promise<void> = deliverPending().finally(() =>
            framesInFlight.delete(delivered),
          );
          framesInFlight.add(delivered);
        });
        await authorize();
        await opened.send("Page.startScreencast", RECORDING_SCREENCAST);
        if (tab.closing) throw new Error("The tab closed while the recording started.");
        const recording: Recording = {
          encoder,
          session: opened,
          startedAt: new Date().toISOString(),
          framesInFlight,
          authorize: resourceAuthorization,
        };
        tab.recording = recording;
        return recording;
      } catch (cause) {
        await encoder.close().catch(constVoid);
        await session?.detach().catch(constVoid);
        throw cause;
      }
    }).finally(() => {
      if (tab.recordingStart === started) tab.recordingStart = null;
    });
    tab.recordingStart = started;
    return started;
  };

  // Scaled captures repaint every screencast; pause them to avoid leaking that frame.
  const withScreencastsPaused = <A>(
    tab: ServerTab,
    capture: () => Promise<A>,
    authorize: () => Promise<void>,
  ): Promise<A> =>
    withCaptureLock(tab, async () => {
      await authorize();
      tab.capturing += 1;
      const recording = tab.recording;
      try {
        await Promise.all([
          ...[...tab.viewers].map((viewer) => viewer.pause()),
          recording?.session.send("Page.stopScreencast").catch(constVoid),
        ]);
        return await capture();
      } finally {
        tab.capturing -= 1;

        await Promise.all([
          ...[...tab.viewers].map((viewer) => viewer.resume()),
          recording && tab.recording === recording
            ? recording.session.send("Page.startScreencast", RECORDING_SCREENCAST).catch(constVoid)
            : undefined,
        ]);
      }
    });

  const stopRecording = (tab: ServerTab, authorize: () => Promise<void>) =>
    withCaptureLock(tab, async () => {
      await authorize();
      const recording = tab.recording;
      if (!recording) {
        throw new ServerBrowserPage.ServerBrowserOperationError({
          tag: "PreviewAutomationRecordingNotActiveError",
          message: "No recording is active for this tab.",
        });
      }
      try {
        await recording.authorize();
        await recording.session.send("Page.stopScreencast").catch(constVoid);
        await Promise.all(recording.framesInFlight);
        await recording.session.detach().catch(constVoid);
        // A screencast can miss the last paint when the page goes idle.
        if (!tab.closing && !tab.dialog) {
          const finalFrame = await ServerBrowserPage.captureFinalRecordingFrame(
            tab.cdp,
            RECORDING_SCREENCAST.quality,
          );
          if (finalFrame !== undefined) {
            await recording.encoder
              .evaluate(
                ([data, width]) =>
                  (globalThis as unknown as EncoderWindow).__t3Recorder.frame(data, width),
                [
                  finalFrame,
                  previewViewportSize(tab.page)?.width ?? UNATTACHED_FILL_VIEWPORT.width,
                ] as const,
              )
              .catch(constVoid);
          }
        }
        await authorize();
        await recording.authorize();
        const { mimeType, count, bytes } = await recording.encoder.evaluate(() =>
          (globalThis as unknown as EncoderWindow).__t3Recorder.stop(),
        );
        // Checked before the transfer so an oversized video never lands in this process.
        if (bytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES) {
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationRecordingTooLargeError",
            message: "The recording is larger than the attachment limit.",
          });
        }
        if (!mimeType || bytes === 0) {
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationExecutionError",
            message: "The recording captured no frames.",
          });
        }
        const extension = mimeType.startsWith("video/mp4") ? "mp4" : "webm";
        const pendingId = `${PENDING_ATTACHMENT_THREAD_SEGMENT}-${NodeCrypto.randomUUID()}-${extension}`;
        const path = NodePath.join(config.attachmentsDir, `${pendingId}.${extension}`);
        await authorize();
        await recording.authorize();
        await NodeFSP.mkdir(config.attachmentsDir, { recursive: true });
        await authorize();
        await recording.authorize();
        if (bytes >= RECORDING_BINARY_EXPORT_MIN_BYTES) {
          const stagedPath = `${path}.partial`;
          try {
            const [download] = await Promise.all([
              recording.encoder.waitForEvent("download", {
                timeout: RECORDING_DOWNLOAD_TIMEOUT_MS,
              }),
              recording.encoder.evaluate(() =>
                (globalThis as unknown as EncoderWindow).__t3Recorder.download(),
              ),
            ]);
            await download.saveAs(stagedPath);
            if ((await NodeFSP.stat(stagedPath)).size !== bytes) {
              throw new Error("The recording download is incomplete.");
            }
            await NodeFSP.rename(stagedPath, path);
          } finally {
            await NodeFSP.rm(stagedPath, { force: true }).catch(constVoid);
          }
        } else {
          const chunks: Array<Buffer> = [];
          for (let index = 0; index < count; index += 1) {
            await authorize();
            await recording.authorize();
            const chunk = await recording.encoder.evaluate(
              (chunkIndex) =>
                (globalThis as unknown as EncoderWindow).__t3Recorder.chunk(chunkIndex),
              index,
            );
            chunks.push(Buffer.from(chunk, "base64"));
          }
          const data = Buffer.concat(chunks);
          if (data.byteLength !== bytes) throw new Error("The recording transfer is incomplete.");
          await NodeFSP.writeFile(path, data);
        }
        return {
          id: pendingId,
          tabId: tab.tabId,
          path,
          mimeType: mimeType.split(";")[0]!,
          sizeBytes: bytes,
          createdAt: new Date().toISOString(),
          uploadedAttachmentId: pendingId,
        };
      } finally {
        await recording.encoder.close().catch(constVoid);
        tab.recording = null;
      }
    });

  let pointerSequence = 0;
  const pointerFor =
    (
      tab: ServerTab,
      authorize: () => Promise<void> = async () => {},
    ): ServerBrowserPage.PointerReporter =>
    async ({ x, y }, phase) => {
      const encoder = tab.recording?.encoder;

      if (tab.viewers.size === 0 && !encoder && !tab.desktop) return;
      const show = (next: "move" | "click") => {
        const sequence = ++pointerSequence;
        for (const viewer of tab.viewers)
          viewer.push({ _tag: "pointer", phase: next, x, y, sequence });
        if (tab.desktop)
          runFork(
            desktopChannel.pointer(
              { threadId: tab.threadId, tabId: tab.tabId },
              { phase: next, x, y },
            ),
          );
        void encoder
          ?.evaluate(
            ([px, py, click]) =>
              (globalThis as unknown as EncoderWindow).__t3Recorder?.cursor(px, py, click),
            [x, y, next === "click"] as const,
          )
          .catch(constVoid);
      };
      await authorize();
      show("move");
      await sleepUntil(Date.now() + AGENT_CURSOR_MOVE_MS);
      if (phase !== "click") return;
      await authorize();
      show("click");
      await sleepUntil(Date.now() + AGENT_CURSOR_CLICK_LEAD_MS);
    };

  const recordAction = <A>(tab: ServerTab, action: string, run: () => Promise<A>): Promise<A> => {
    const event: {
      -readonly [K in keyof PreviewAutomationActionEvent]: PreviewAutomationActionEvent[K];
    } = {
      id: NodeCrypto.randomUUID(),
      action,
      status: "running",
      startedAt: new Date().toISOString(),
    };
    pushBounded(tab.actionTimeline, event, ACTION_TIMELINE_LIMIT);
    return run().then(
      (result) => {
        event.status = "succeeded";
        event.completedAt = new Date().toISOString();
        return result;
      },
      (cause: unknown) => {
        event.status = "failed";
        event.completedAt = new Date().toISOString();
        event.error = cause instanceof Error ? cause.message.split("\n")[0] : String(cause);
        throw cause;
      },
    );
  };

  const requireTab = async (request: PreviewAutomationRequest) => {
    if (!request.agentSessionId)
      throw new BrowserControlInterrupted({
        message: "The agent session is missing. Reconnect the provider.",
      });
    const owned = [...tabs.values()].filter(
      (tab) => tab.threadId === request.threadId && tab.control.agentId === request.agentSessionId,
    );
    if (!request.tabIdExplicit && owned.length > 1)
      throw new BrowserControlInterrupted({
        message:
          "Multiple tabs belong to this agent session. Pass a tabId from preview_open or preview_status tabs.",
        reason: "tabRequired",
      });
    const tab =
      request.tabId === undefined
        ? latestThreadTab(request.threadId, request.agentSessionId)
        : await runPromise(
            findTab(request.threadId, request.tabId).pipe(Effect.orElseSucceed(constVoid)),
          );
    if (!tab) {
      throw new ServerBrowserPage.ServerBrowserOperationError({
        tag: "PreviewAutomationTabNotFoundError",
        message: "No server preview tab is open for this thread. Call preview_open first.",
      });
    }
    if (!READ_OPERATIONS.has(request.operation) && !tab.control.agentMayAct(request.agentSessionId))
      throw new BrowserControlInterrupted({
        message:
          "This tab belongs to another agent session. You can read it, but open your own tab to act.",
        reason: "agentMismatch",
      });
    return tab;
  };

  const markUsed = (request: PreviewAutomationRequest) => {
    const now = Date.now();
    for (const tab of tabs.values()) {
      if (
        tab.threadId === request.threadId &&
        (tab.control.agentId === request.agentSessionId || tab.tabId === request.tabId)
      )
        tab.usedAt = now;
    }
  };

  const isolatedScopes = new WeakMap<object, string>();
  /** One scope per cookie jar: a persistent profile per runtime, or one isolated context. */
  const siteHoldScope = (tab: ServerTab) => {
    if (!tab.isolatedContext) return `profile:${tab.profileId ?? DEFAULT_BROWSER_PROFILE_ID}`;
    const context = tab.page.context();
    let scope = isolatedScopes.get(context);
    if (scope === undefined) {
      scope = `isolated:${tab.threadId}:${tab.tabId}`;
      isolatedScopes.set(context, scope);
    }
    return scope;
  };
  // A profile's hold covers its desktop and server tabs; a thread's hold also covers its
  // other tabs, so a fresh incognito tab or a popup is no way around it.
  const siteHoldOf = (tab: ServerTab, url = tab.page.url()) =>
    siteHolds.get(siteHoldScope(tab), siteOf(url)) ??
    siteHolds.forThread(tab.threadId, siteOf(url));
  const publicSiteHold = ({ site, vendor, kind, url, since }: HeldSite) => ({
    site,
    vendor,
    kind,
    url,
    since,
  });

  const raiseSiteHold = async (
    tab: ServerTab,
    verdict: NonNullable<ReturnType<typeof classifyAntiBotPage>>,
    url: string,
  ) => {
    const site = siteOf(url);
    if (site === undefined || tab.closing) return;
    const hold: HeldSite = {
      site,
      ...verdict,
      url: ServerBrowserPage.withoutDataPayload(url).slice(0, 2048),
      since: new Date().toISOString(),
      threadId: tab.threadId,
      tabId: tab.tabId,
    };
    if (!siteHolds.create(siteHoldScope(tab), hold)) return;
    runFork(Effect.logInfo("Site hold raised", { site, vendor: hold.vendor, kind: hold.kind }));
    // Page watches run caller code on the page, so they stop with the agent.
    for (const covered of tabs.values())
      if (covered === tab || siteHoldOf(covered) === hold) stopTabWatches(covered, "revoked");
    const target = { threadId: tab.threadId, tabId: tab.tabId };
    await runPromise(
      manager.setSiteHold({ ...target, siteHold: publicSiteHold(hold) }).pipe(Effect.ignore),
    );
    await runPromise(manager.requestReveal({ ...target, force: true }).pipe(Effect.ignore));
  };

  /** Refuses before anything reaches a held site; after the action, reports the hold it met. */
  const guardSiteHold = async <A>(
    tab: ServerTab,
    request: PreviewAutomationRequest,
    run: () => Promise<A>,
  ): Promise<A> => {
    if (!SITE_HOLD_OPERATIONS.has(request.operation)) return run();
    let target = tab.page.url();
    try {
      if (request.operation === "navigate")
        target = resolveNavigationUrl(request.input as PreviewAutomationNavigateInput);
      else if (request.operation === "readPage")
        target = normalizePreviewUrl(decodeReadPage(request.input).url);
    } catch {
      // The operation reports its own input error.
    }
    // A check still being classified must not let the next action through.
    await tab.siteHoldCheck;
    const before = siteHoldOf(tab, target);
    if (before) throw siteHoldError(before);
    const met = async () => {
      await tab.siteHoldCheck;
      const hold = siteHoldOf(tab);
      if (hold) throw siteHoldError(hold);
    };
    let result: A;
    try {
      result = await run();
    } catch (cause) {
      await met();
      throw cause;
    }
    await met();
    return result;
  };

  /**
   * A human release clears the tab's hold only on a verified clean page; Clear hold
   * clears it either way. The hold stays set until classification ends.
   */
  const settleSiteHold = async (
    tab: ServerTab,
    how: "release" | "clear",
    authorize: () => Promise<void> = async () => {},
  ) => {
    for (const [scope, hold] of siteHolds.heldBy(tab.tabId)) {
      await tab.siteHoldCheck;
      // Only an observed main document on the held site, with no check on it, is verified
      // clean; a navigation or control change during the read makes the result stale.
      const generation = [tab.navigationGeneration, tab.control.generation] as const;
      // The document must be the one on screen and must have loaded after the check appeared.
      const candidate = tab.mainDocument;
      const document =
        candidate !== null &&
        siteOf(tab.page.url()) === hold.site &&
        candidate.url === tab.page.url() &&
        candidate.at >= Date.parse(hold.since)
          ? candidate
          : null;
      // A page that cannot be read is not proof of a clean one.
      const html = document === null ? null : await tab.page.content().catch(() => null);
      const verified =
        document !== null &&
        html !== null &&
        classifyAntiBotPage({ ...document, html }) === undefined &&
        generation[0] === tab.navigationGeneration &&
        generation[1] === tab.control.generation;
      if (!verified && how === "release") continue;
      // The person's access is checked again right before the hold goes.
      await authorize();
      if (!siteHolds.clear(scope, hold)) continue;
      runFork(Effect.logInfo("Site hold cleared", { site: hold.site, verified }));
      // Another site this tab still holds keeps its banner.
      const [remaining] = siteHolds.heldBy(tab.tabId);
      await runPromise(
        manager
          .setSiteHold({
            threadId: ThreadId.make(hold.threadId),
            tabId: hold.tabId,
            siteHold: remaining ? publicSiteHold(remaining[1]) : undefined,
          })
          .pipe(Effect.ignore),
      );
    }
  };

  const runOperation = async (
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
    deadlineMs: number,
  ): Promise<unknown> => {
    await authorize();
    const input = request.input;
    switch (request.operation) {
      case "watchStop": {
        const input = decodeWatchStop(request.input);
        return watches.stop({
          ...input,
          threadId: request.threadId,
          ...(request.agentSessionId === undefined
            ? {}
            : { agentSessionId: request.agentSessionId }),
        });
      }
      case "status":
        return statusWithTitle(
          request.tabId === undefined
            ? latestThreadTab(request.threadId, request.agentSessionId)
            : tabs.get(tabKey(request.threadId, request.tabId)),
          request.agentSessionId,
        );
      case "open": {
        if (!request.agentSessionId)
          throw new BrowserControlInterrupted({
            message: "The agent session is missing. Reconnect the provider.",
          });
        const open = input as PreviewAutomationOpenInput;
        const url = open.url === undefined ? undefined : normalizePreviewUrl(open.url);
        const reuse = open.reuseExistingTab ?? true;
        if (
          reuse &&
          !request.tabIdExplicit &&
          [...tabs.values()].filter(
            (tab) =>
              tab.threadId === request.threadId && tab.control.agentId === request.agentSessionId,
          ).length > 1
        )
          throw new BrowserControlInterrupted({
            message: "Multiple tabs are open. Pass tabId or reuseExistingTab=false.",
            reason: "tabRequired",
          });

        const found =
          reuse && request.tabId !== undefined
            ? await runPromise(
                findTab(request.threadId, request.tabId).pipe(
                  Effect.catchTags({
                    ServerBrowserTabNotFoundError: () => Effect.void,
                  }),
                ),
              )
            : undefined;
        if (reuse && request.tabIdExplicit && !found) {
          const { sessions } = await runPromise(
            manager.list({ threadId: ThreadId.make(request.threadId) }),
          );
          const desktop = sessions.some(
            (session) => session.tabId === request.tabId && session.runtime !== "server",
          );
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: desktop ? "PreviewAutomationExecutionError" : "PreviewAutomationTabNotFoundError",
            message: desktop
              ? "This tab belongs to the desktop runtime. This environment's server browser cannot control desktop tabs. Omit tabId to open a server tab instead."
              : "The requested server preview tab does not exist.",
          });
        }
        // Only an explicit tabId reuses a tab this session did not open; a tab
        // it last read (such as the user's) is not taken over implicitly.
        const existing =
          found && (request.tabIdExplicit || found.control.agentId === request.agentSessionId)
            ? found
            : undefined;
        const navigationTimeout = Math.min(request.timeoutMs, NAVIGATION_TIMEOUT_MS);
        const profileId = existing ? undefined : resolveOpenProfile(open.profileId);
        if (url) {
          const held = existing
            ? siteHoldOf(existing, url)
            : profileId === undefined || profileId === INCOGNITO_BROWSER_PROFILE_ID
              ? siteHolds.forThread(request.threadId, siteOf(url))
              : (siteHolds.get(`profile:${profileId}`, siteOf(url)) ??
                siteHolds.forThread(request.threadId, siteOf(url)));
          if (held) throw siteHoldError(held);
        }
        if (!existing) await authorize();
        const tab =
          existing ??
          (await runPromise(
            openTab(
              {
                threadId: request.threadId,
                ...(url ? { url } : {}),
                ...(profileId === undefined ? {} : { profileId }),
                runtime: "server",
                reveal: false,
                automationOwner: request.agentSessionId,
                authorize: resourceAuthorization,
              },
              { reclaimIdleSlot: true },
            ),
          ));
        if (existing?.dialog)
          throw new BrowserControlInterrupted({
            message:
              "A browser dialog is pending. Read preview_status and use preview_dialog first.",
            reason: "dialogPending",
          });
        return tab.control.agent(
          request.agentSessionId,
          async () => {
            if (tab.dialog)
              throw new BrowserControlInterrupted({
                message:
                  "A browser dialog is pending. Read preview_status and use preview_dialog first.",
                reason: "dialogPending",
              });
            if (existing) {
              if (url) {
                // A hold may have been raised while this open waited for the tab.
                const held = siteHoldOf(tab, url);
                if (held) throw siteHoldError(held);
                await navigate(tab, url, "load", navigationTimeout);
              }
            } else {
              // A failed open returns no tabId; a leftover hidden tab would block later reuse.
              await Promise.resolve(tab.initialNavigation)
                .then(async () => {
                  if (url) {
                    await tab.page
                      .waitForLoadState("load", { timeout: navigationTimeout })
                      .catch(constVoid);
                  }
                  assertNavigationSucceeded(tab);
                })
                .catch(async (cause: unknown) => {
                  // Playwright rejects downloads too; their tab must survive to save the file.
                  if (cause instanceof Error && cause.message.includes("Download is starting")) {
                    throw cause;
                  }
                  await runPromise(manager.close({ threadId: tab.threadId, tabId: tab.tabId }));
                  dropTab(tab, false);
                  throw cause;
                });
            }
            const reveal = open.open ?? open.show;
            let shown: Promise<void> | undefined;
            if (reveal !== false) {
              await authorize();
              await runPromise(
                manager.requestReveal({
                  threadId: tab.threadId,
                  tabId: tab.tabId,
                  force: reveal === true,
                }),
              );
              // Only an explicit show is sure to reach a client; desktop tabs are visible without a viewer.
              const remainingMs = deadlineMs - performance.now();
              if (reveal === true && tab.viewers.size === 0 && tab.desktop === null)
                shown = awaitViewer(tab, Math.min(REVEAL_VIEWER_WAIT_MS, remainingMs));
              // A desktop panel reports the revealed tab shown a few hops after the reveal.
              else if (reveal === true && !isVisible(tab))
                shown = runPromise(
                  desktopChannel.awaitShown(
                    tab,
                    Math.max(0, Math.min(REVEAL_DESKTOP_SHOWN_WAIT_MS, remainingMs)),
                  ),
                ).then(constVoid);
            }
            await shown;
            await tab.siteHoldCheck;
            const held = siteHoldOf(tab);
            if (held) throw siteHoldError(held);
            return statusWithTitle(tab, request.agentSessionId);
          },
          deadlineMs,
          authorize,
        );
      }
      case "recordingStop": {
        if (
          !request.tabIdExplicit &&
          [...tabs.values()].filter(
            (candidate) =>
              candidate.threadId === request.threadId &&
              candidate.control.agentId === request.agentSessionId,
          ).length > 1
        )
          throw new BrowserControlInterrupted({
            message:
              "Multiple tabs belong to this agent session. Pass a tabId from preview_open or preview_status tabs.",
            reason: "tabRequired",
          });
        const recordings = [...tabs.values()].filter(
          (candidate) =>
            candidate.threadId === request.threadId &&
            candidate.control.agentMayAct(request.agentSessionId ?? "") &&
            (candidate.recording || candidate.recordingStart),
        );
        const targetTabId =
          request.tabId ?? latestThreadTab(request.threadId, request.agentSessionId)?.tabId;
        const tab =
          recordings.find((candidate) => candidate.tabId === targetTabId) ??
          (!request.tabIdExplicit && recordings.length === 1 ? recordings[0] : undefined);
        if (!tab) {
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationRecordingNotActiveError",
            message: "No recording is active for this thread.",
          });
        }
        return tab.control.agent(
          request.agentSessionId ?? "",
          // A stop cannot act twice (a retry finds no recording); dropping a late one would
          // leave capture running, so it takes no deadline.
          () => stopRecording(tab, authorize),
          undefined,
          authorize,
        );
      }
    }
    if (
      request.operation === "watchStart" &&
      (!request.tabIdExplicit || !tabs.has(tabKey(request.threadId, request.tabId ?? "")))
    )
      throw new PreviewWatchError({ reason: "unknown" });
    const tab = await requireTab(request);
    // Closing must unblock an action waiting on a dialog, without queueing behind it.
    if (request.operation === "close") {
      await authorize();
      if (tab.control.agentId === null)
        throw new BrowserControlInterrupted({
          message: "This tab has no agent owner. Ask the user to close it in the browser panel.",
          reason: "unclaimedTab",
        });
      if (tab.control.controller !== null)
        throw new BrowserControlInterrupted({
          message: "A human controls this tab.",
          reason: "humanControl",
        });
      // Closing ends the tab's hold, so only the user may close a tab that holds its site.
      const [held] = siteHolds.heldBy(tab.tabId);
      if (held) throw siteHoldError(held[1]);
      stopTabWatches(tab, "closed");
      await runPromise(manager.close({ threadId: tab.threadId, tabId: tab.tabId }));
      dropTab(tab, false);
      return {};
    }
    // A click can be waiting for its dialog. Resolve it outside the serial queue,
    // with the same owner check, so the operation can finish and control can drain.
    if (request.operation === "dialog") {
      await authorize();
      if (tab.control.controller !== null)
        throw new BrowserControlInterrupted({
          message: "A human controls this tab.",
          reason: "humanControl",
        });
      await resolveDialog(tab, input as PreviewAutomationDialogInput);
      return statusWithTitle(tab, request.agentSessionId);
    }
    const agentSessionId = request.agentSessionId!;

    if (READ_OPERATIONS.has(request.operation) && !tab.control.agentCanActNow(agentSessionId)) {
      return tab.control.observe(async () => {
        await authorize();
        if (tab.dialog)
          throw new BrowserControlInterrupted({
            message: "A browser dialog is pending. Read preview_status.",
            reason: "dialogPending",
          });
        return runTabOperation(tab, request, authorize, resourceAuthorization, deadlineMs);
      });
    }
    return tab.control.agent(
      agentSessionId,
      async () => {
        if (tab.dialog)
          throw new BrowserControlInterrupted({
            message:
              "A browser dialog is pending. Read preview_status and use preview_dialog first.",
            reason: "dialogPending",
          });
        const generation = tab.control.generation;
        tab.agentOperating = !READ_OPERATIONS.has(request.operation);
        updateFileChooserInterception(tab);
        try {
          return await guardSiteHold(tab, request, () =>
            runTabOperation(tab, request, authorize, resourceAuthorization, deadlineMs),
          );
        } finally {
          tab.agentOperating = false;
          updateFileChooserInterception(tab);
          if (generation !== tab.control.generation) ServerBrowserPage.invalidateRefs(tab.page);
        }
      },
      deadlineMs,
      authorize,
    );
  };

  const fillInTab = async (
    tab: ServerTab,
    input: PreviewAutomationFillCredentialInput,
    authorize: () => Promise<void>,
  ) => {
    if (tab.desktop) throw serverBrowserOnly();
    return recordAction(tab, "fillCredential", () =>
      fillCredential({
        tabId: tab.tabId,
        vault,
        request: input,
        locate: (locator) => ServerBrowserPage.targetLocator(tab.page, { locator })!,
        beforeTyping: authorize,
      }),
    );
  };

  const runTabOperation = async (
    tab: ServerTab,
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
    deadlineMs: number,
  ) => {
    await authorize();
    const input = request.input;
    switch (request.operation) {
      case "readPage": {
        const read = decodeReadPage(input);
        const generation = tab.control.generation;
        if (
          !request.tabIdExplicit ||
          tab.isolatedContext ||
          (tab.profileId ?? DEFAULT_BROWSER_PROFILE_ID) !== read.profileId ||
          read.profileId === INCOGNITO_BROWSER_PROFILE_ID
        )
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationExecutionError",
            message: "Select an existing tab in the requested persistent browser profile.",
          });
        const response = await tab.page.goto(normalizePreviewUrl(read.url), {
          waitUntil: "domcontentloaded",
          timeout: request.timeoutMs,
        });
        // A human may reserve control while navigation is in flight.
        if (!tab.control.agentCanActNow(request.agentSessionId!))
          throw new BrowserControlInterrupted({
            message: "Browser control changed during the read.",
          });
        const html = await tab.page.content();
        if (
          generation !== tab.control.generation ||
          !tab.control.agentCanActNow(request.agentSessionId!)
        )
          throw new BrowserControlInterrupted({
            message: "Browser control changed during the read.",
          });
        if (Buffer.byteLength(html, "utf8") > 4 * 1024 * 1024)
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationResultTooLargeError",
            message: "The session page exceeds the research extraction limit.",
          });
        return {
          tabId: tab.tabId,
          profileId: read.profileId,
          generation: tab.control.generation,
          finalUrl: tab.page.url(),
          httpStatus: response?.status() ?? null,
          html,
        };
      }
      case "navigate": {
        const navigateInput = input as PreviewAutomationNavigateInput;
        await recordAction(tab, "navigate", () =>
          navigate(
            tab,
            resolveNavigationUrl(navigateInput),
            navigateInput.readiness ?? "load",
            navigateInput.timeoutMs ?? request.timeoutMs,
          ),
        );
        return statusWithTitle(tab, request.agentSessionId);
      }
      case "resize": {
        const setting = resolvePreviewViewport(input as PreviewAutomationResizeInput);
        const from = fixedViewportSize(tab.setting);
        const to = fixedViewportSize(setting);
        const drawn = tab.desktopResize.promise;
        await runPromise(
          manager.resize(
            { threadId: tab.threadId, tabId: tab.tabId, viewport: setting },
            resourceAuthorization,
          ),
        );
        await authorize();
        await applySetting(tab, setting, authorize);
        if (!tab.desktop) {
          return {
            tabId: tab.tabId,
            setting,
            viewport: previewViewportSize(tab.page) ?? UNATTACHED_FILL_VIEWPORT,
          };
        }
        const changed =
          tab.desktopResizeUndrawn || from?.width !== to?.width || from?.height !== to?.height;
        const remainingMs = () => Math.max(0, deadlineMs - performance.now());
        const within = <A>(promise: Promise<A>, ms: number) =>
          runPromise(
            Effect.promise(() => promise).pipe(
              Effect.timeoutOption(ms),
              Effect.map(Option.getOrNull),
            ),
          );
        // The desktop draws the new size after the session update; its page resize says it has.
        // Half the time left keeps room for the read, which runs even if nothing was drawn,
        // because a page can keep another size than the one asked for.
        let undrawn = false;
        if (changed) {
          // Electron resizes no hidden guest it does not paint; a pending capture makes the desktop paint it.
          let waiting = true;
          void (async () => {
            while (waiting && !tab.closing) {
              await tab.cdp.send("Page.captureScreenshot", DESKTOP_PAINT_CAPTURE);
            }
          })().catch(constVoid);
          try {
            undrawn =
              (await within(
                drawn.then(() => true),
                Math.min(DESKTOP_RESIZE_WAIT_MS, remainingMs() / 2),
              )) === null;
          } finally {
            waiting = false;
          }
        }
        const changes = tab.desktopViewportChanges;
        const measured = await within(measureDesktopViewport(tab), remainingMs());
        // A window that does not paint, such as a minimized one, never draws the new size.
        // Fill asks for no size, so only a fixed one can be missed.
        tab.desktopResizeUndrawn =
          undrawn &&
          to !== null &&
          (measured?.width !== to.width || measured?.height !== to.height);
        if (tab.desktopResizeUndrawn) {
          throw new ServerBrowserPage.ServerBrowserOperationError({
            tag: "PreviewAutomationExecutionError",
            message:
              "The desktop did not draw the page at the new size. Show or restore the T3 Code window and try again.",
          });
        }
        // Only a read after the draw, with no page resize or load during it, measures this resize.
        return {
          tabId: tab.tabId,
          setting,
          ...(measured && tab.desktopViewportChanges === changes ? { viewport: measured } : {}),
        };
      }
      case "setColorScheme": {
        const { colorScheme } = input as PreviewAutomationSetColorSchemeInput;
        const snapshot = await runPromise(
          manager.adjust(
            { threadId: tab.threadId, tabId: tab.tabId, colorScheme },
            resourceAuthorization,
          ),
        );
        await authorize();
        await applyRendering(tab, snapshot, authorize);
        return { tabId: tab.tabId, colorScheme };
      }
      case "snapshot": {
        const snapshotInput = input as PreviewAutomationSnapshotInput | undefined;
        const rendering = {
          ...tab,
          renderScale: tab.desktop ? "display" : RENDER_SCALE,
          fullScreenshot: snapshotInput?.fullScreenshot,
        } as const;
        const snapshot = await withScreencastsPaused(
          tab,
          () =>
            snapshotInput?.pixelsRequired === false
              ? ServerBrowserPage.snapshotMetadata(rendering, authorize)
              : ServerBrowserPage.snapshot(rendering, authorize),
          authorize,
        );
        const loadError = loadErrorOf(tab);
        return loadError ? { ...snapshot, url: loadError.url, loadError } : snapshot;
      }
      case "click": {
        // Time spent queued behind other work on this tab counts against the broker's deadline.
        const clickInput = {
          ...(input as PreviewAutomationClickInput),
          timeoutMs: Math.max(1, Math.floor(deadlineMs - performance.now())),
        };
        await recordAction(tab, "click", () =>
          ServerBrowserPage.click(tab.page, clickInput, pointerFor(tab, authorize), authorize),
        );
        return undefined;
      }
      case "hover":
        return recordAction(tab, "hover", () =>
          ServerBrowserPage.hover(
            tab.page,
            input as PreviewAutomationHoverInput,
            pointerFor(tab, authorize),
            authorize,
          ),
        );
      case "select":
        return recordAction(tab, "select", () =>
          ServerBrowserPage.select(tab.page, input as PreviewAutomationSelectInput, authorize),
        );
      case "drag":
        return recordAction(tab, "drag", () =>
          ServerBrowserPage.drag(
            tab.page,
            input as PreviewAutomationDragInput,
            pointerFor(tab, authorize),
            authorize,
          ),
        );
      case "upload":
        return recordAction(tab, "upload", () =>
          uploadFiles(tab, input as PreviewAutomationUploadInput, authorize),
        );
      case "type": {
        // As for click: typing stops at the broker's deadline instead of running on after the reply.
        const typeInput = {
          ...(input as PreviewAutomationTypeInput),
          timeoutMs: Math.max(1, Math.floor(deadlineMs - performance.now())),
        };
        return recordAction(tab, "type", () =>
          ServerBrowserPage.type(tab.page, typeInput, authorize),
        );
      }
      case "press":
        return recordAction(tab, "press", () =>
          ServerBrowserPage.press(tab.page, input as PreviewAutomationPressInput, authorize),
        );
      case "scroll":
        return recordAction(tab, "scroll", () =>
          ServerBrowserPage.scroll(tab.page, input as PreviewAutomationScrollInput, authorize),
        );
      case "fillCredential":
        return fillInTab(tab, input as PreviewAutomationFillCredentialInput, authorize);
      case "saveSecret": {
        if (tab.desktop) throw saveSecretServerBrowserOnly();
        return recordAction(tab, "saveSecret", () =>
          saveSecret({
            tabId: PreviewTabId.make(tab.tabId),
            request: input as PreviewAutomationSaveSecretInput,
            protocolLogging,
            locate: (locator) => ServerBrowserPage.targetLocator(tab.page, { locator })!,
            beforeSave: authorize,
            save: (value) => runPromise(secrets.saveCaptured({ threadId: tab.threadId, value })),
          }),
        );
      }
      case "watchStart": {
        const input = decodeWatchStart({
          ...(request.input as object),
          tabId: tab.tabId,
        });
        if (tab.control.agentId !== request.agentSessionId)
          throw new PreviewWatchError({ reason: "ownership" });
        const generation = tab.control.generation;
        const navigation = tab.navigationGeneration;
        const fence = (): PreviewWatchError["reason"] | undefined => {
          if (tabs.get(tabKey(tab.threadId, tab.tabId)) !== tab || tab.closing) return "closed";
          if (tab.navigationGeneration !== navigation) return "navigation";
          if (
            tab.control.generation !== generation ||
            !tab.control.agentCanActNow(request.agentSessionId ?? "")
          )
            return "ownership";
          return undefined;
        };
        await resourceAuthorization();
        const cdp = await tab.page.context().newCDPSession(tab.page);
        const watchId = NodeCrypto.randomUUID();
        let cleanup: (() => Promise<void>) | undefined;
        const installed = Promise.withResolvers<void>();
        let cleanupTask: Promise<void> | undefined;
        const dispose = () =>
          (cleanupTask ??= (async () => {
            // A synchronous baseline can terminate before registration returns its disposer.
            await installed.promise;
            try {
              await cleanup?.();
            } finally {
              tab.watches.delete(watchId);
              await cdp.detach().catch(constVoid);
            }
          })());
        let started: ReturnType<PageWatchStore["register"]>;
        try {
          started = watches.register(
            watchId,
            tab.threadId,
            {
              authorize: resourceAuthorization,
              fence,
              cleanup: dispose,
            },
            request.agentSessionId,
          );
        } catch (cause) {
          await cdp.detach().catch(constVoid);
          throw cause;
        }
        tab.watches.add(watchId);
        try {
          try {
            cleanup = await installPageWatch({
              cdp,
              watchId,
              subscription: input.subscription,
              store: watches,
              authorize: resourceAuthorization,
            });
          } finally {
            installed.resolve();
          }
          await authorize();
          await watches.flush(watchId);
          return started;
        } catch (cause) {
          await watches.terminate(watchId, "subscription_failed").catch(constVoid);
          await dispose().catch(constVoid);
          throw cause;
        }
      }
      case "evaluate":
        return ServerBrowserPage.evaluate(
          tab.cdp,
          input as PreviewAutomationEvaluateInput,
          request.timeoutMs,
          authorize,
        );
      case "waitFor":
        return ServerBrowserPage.waitFor(
          tab.page,
          input as PreviewAutomationWaitForInput,
          authorize,
        );
      case "recordingStart": {
        const recording = await startRecording(tab, authorize, resourceAuthorization);
        return { tabId: tab.tabId, recording: true, startedAt: recording.startedAt };
      }
    }
  };

  // Grants are checked again as the result leaves.
  const authorizedOperation = async (
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
    deadlineMs: number,
  ) => {
    const result = await runOperation(request, authorize, resourceAuthorization, deadlineMs);
    await authorize();
    return result;
  };

  const handleRequest = Effect.fn("ServerBrowser.handleRequest")(
    function* (connectionId: string, request: PreviewAutomationRequest) {
      if (broker.requestAuthorization === undefined)
        return yield* new ServerBrowserPage.ServerBrowserOperationError({
          tag: "PreviewAutomationUnsupportedClientError",
          message: "Current preview invocation checks are unsupported.",
        });
      const authorization = yield* broker
        .requestAuthorization({
          clientId: SERVER_HOST_CLIENT_ID,
          connectionId,
          requestId: request.requestId,
        })
        .pipe(Effect.mapError(ServerBrowserPage.toOperationError));
      return yield* Effect.tryPromise({
        try: () =>
          authorizedOperation(
            request,
            () => runPromise(authorization.dispatch),
            () =>
              runPromise(
                request.operation === "watchStart"
                  ? (authorization.watch ??
                      Effect.fail(
                        new OrchestratorMcpFailure({
                          code: "capability_denied",
                          message: "Watch continuation authority is unavailable.",
                        }),
                      ))
                  : authorization.resource,
              ),
            performance.now() + request.timeoutMs,
          ).finally(() => markUsed(request)),
        catch: ServerBrowserPage.toOperationError,
      });
    },
    (effect, connectionId, request) =>
      effect.pipe(
        Effect.match({
          onSuccess: (result) => ({ ok: true as const, result }),
          onFailure: (error) => ({
            ok: false as const,
            error: {
              _tag: error.tag,
              message: error.message,
              ...(error.detail === undefined ? {} : { detail: error.detail }),
            },
          }),
        }),
        Effect.flatMap((outcome) =>
          broker.respond({
            clientId: SERVER_HOST_CLIENT_ID,
            connectionId,
            requestId: request.requestId,
            ...outcome,
          }),
        ),
        Effect.ignore,
      ),
  );

  const mirrorManagerEvent = ({ event, authorize = async () => {} }: PreviewManager.ServerEvent) =>
    Effect.promise(async () => {
      if (event.type === "opened" && event.snapshot.runtime === "server") {
        try {
          await ensureTab(event.snapshot, authorize);
        } catch {
          if (event.snapshot.automationOwner !== undefined)
            await runPromise(
              manager
                .close({ threadId: ThreadId.make(event.threadId), tabId: event.tabId })
                .pipe(Effect.ignore),
            );
        }
        return;
      }
      const key = tabKey(event.threadId, event.tabId);
      const tab = tabs.get(key);
      // A closed session is a closed page; a detached desktop page is not.
      if (event.type === "closed" && !tab && pendingTabs.has(key)) closedPendingTabs.add(key);
      if (event.type === "closed") tabAuthorizations.delete(key);
      // A detached tab's session closing ends its holds too; nothing else could clear them.
      if (event.type === "closed" && !tab)
        for (const [scope, hold] of siteHolds.heldBy(event.tabId))
          if (hold.threadId === event.threadId) siteHolds.clear(scope, hold);
      if (!tab) return;
      if (event.type === "closed") {
        closedSessions.add(key);
        dropTab(tab, false);
        closedSessions.delete(key);
        return;
      }
      if (event.type === "resized") {
        // Desktop tabs only record the setting, so status need not wait behind an agent action.
        if (tab.desktop) tab.setting = event.snapshot.viewport ?? FILL_PREVIEW_VIEWPORT;
        await tab.control
          .system(async () => {
            await authorize();
            if (
              !viewportSettingsEqual(event.snapshot.viewport ?? FILL_PREVIEW_VIEWPORT, tab.setting)
            ) {
              await applySetting(tab, event.snapshot.viewport ?? FILL_PREVIEW_VIEWPORT, authorize);
            }
            await applyRendering(tab, event.snapshot, authorize);
            const request = event.request;
            await authorize();
            if (request?.clear === "cookies") await tab.cdp.send("Network.clearBrowserCookies");
            await authorize();
            if (request?.clear === "cache") await tab.cdp.send("Network.clearBrowserCache");
            await authorize();
            if (request?.hardReload) {
              tab.pendingInitialRequestUrl = null;
              await leaveBusyPage(tab, tab.cdp);
              await tab.cdp.send("Page.reload", { ignoreCache: true });
            }
          })
          .catch((cause: unknown) =>
            runFork(Effect.logWarning("server preview could not apply a tab setting", { cause })),
          );
      }
    });

  const releaseViewerInput = async (viewer: ViewerState, session: CDPSession) => {
    for (const { key, code, keyCode } of viewer.pressedKeys.values()) {
      await session
        .send("Input.dispatchKeyEvent", {
          type: "keyUp",
          key,
          code,
          windowsVirtualKeyCode: keyCode,
        })
        .catch(constVoid);
    }
    viewer.pressedKeys.clear();
    for (const [button, point] of viewer.pressedButtons) {
      await session
        .send("Input.dispatchMouseEvent", {
          type: "mouseReleased",
          button,
          ...point,
          buttons: 0,
          clickCount: 1,
        })
        .catch(constVoid);
    }
    viewer.pressedButtons.clear();
    const press = viewer.selectPress;
    viewer.selectPress = null;
    if (press) releaseSelectHandle(viewer, session, press);
    withdrawSelectMenu(viewer);
    viewer.requestedSize = null;
  };

  const releaseSelectHandle = (viewer: ViewerState, session: CDPSession, objectId: string) => {
    if (objectId === viewer.selectPress || objectId === viewer.selectMenu?.objectId) return;
    void session.send("Runtime.releaseObject", { objectId }).catch(constVoid);
  };

  const withdrawSelectMenu = (viewer: ViewerState) => {
    const offer = viewer.selectMenu;
    if (!offer) return;
    viewer.selectMenu = null;
    viewer.push({ _tag: "selectMenuClosed", id: offer.id });
    releaseSelectHandle(viewer, offer.session, offer.objectId);
  };

  // Chromium draws a select's popup outside the page, where the screencast never sees it, and on
  // a desktop host it is a native menu CDP cannot dismiss, so the press must not open it.
  const guardSelectPress = async (session: CDPSession, x: number, y: number) => {
    const { result } = await session.send("Runtime.evaluate", {
      expression: `(${GUARD_SELECT_AT_POINT_SCRIPT})(${x}, ${y}, ${SELECT_MENU_OPTION_LIMIT})`,
    });
    return result.objectId ?? null;
  };

  // Offered after the release, so options a page adds or replaces on mousedown or click count.
  const offerSelectMenu = async (session: CDPSession, viewer: ViewerState, objectId: string) => {
    const described = await session
      .send("Runtime.callFunctionOn", {
        objectId,
        functionDeclaration: OFFER_SELECT_MENU_SCRIPT,
        arguments: [{ value: SELECT_MENU_LABEL_LIMIT }, { value: SELECT_MENU_OPTION_LIMIT }],
        returnByValue: true,
      })
      .catch(() => null);
    const menu = described?.result.value as Omit<
      Extract<ServerBrowserViewerOutput, { _tag: "selectMenu" }>,
      "_tag" | "id"
    > | null;
    if (!menu) return;
    const previous = viewer.selectMenu;
    viewer.selectMenu = { id: NodeCrypto.randomUUID(), objectId, session };
    if (previous) releaseSelectHandle(viewer, previous.session, previous.objectId);
    viewer.push({ _tag: "selectMenu", id: viewer.selectMenu.id, ...menu });
  };

  const dispatchViewerInput = async (
    tab: ServerTab,
    session: CDPSession,
    viewer: ViewerState,
    raw: unknown,
    authorize: () => Promise<void>,
  ) => {
    await authorize();
    const message = asRecord(raw);
    if (!message) return;
    const modifiers = modifiersOf(message);
    switch (message.type) {
      case "mouse": {
        const action = message.action;
        const left = message.button === "left";
        if (left && action === "down") {
          const stale = viewer.selectPress;
          viewer.selectPress = null;
          if (stale) releaseSelectHandle(viewer, session, stale);
          if (!tab.dialog) {
            viewer.selectPress = await guardSelectPress(
              session,
              num(message.x),
              num(message.y),
            ).catch(() => null);
          }
        }
        const guarded = left && action === "down" ? viewer.selectPress : null;
        const type =
          action === "down" ? "mousePressed" : action === "up" ? "mouseReleased" : "mouseMoved";
        const button = ["none", "left", "middle", "right"].includes(String(message.button))
          ? (message.button as "none" | "left" | "middle" | "right")
          : "none";
        try {
          await session.send("Input.dispatchMouseEvent", {
            type,
            x: num(message.x),
            y: num(message.y),
            button,
            buttons: num(message.buttons),
            clickCount: num(message.clickCount, type === "mouseMoved" ? 0 : 1),
            modifiers,
            // A pressed button has the W3C default pressure, as a mouse in Chrome reports.
            ...(num(message.buttons) !== 0 || type === "mousePressed" ? { force: 0.5 } : {}),
          });
        } finally {
          if (guarded) {
            await session
              .send("Runtime.callFunctionOn", {
                objectId: guarded,
                functionDeclaration: "function () { this.stop(); }",
              })
              .catch(constVoid);
          }
        }
        if (button !== "none") {
          if (action === "down")
            viewer.pressedButtons.set(button, { x: num(message.x), y: num(message.y) });
          else if (action === "up") viewer.pressedButtons.delete(button);
        }
        const press = left && action === "up" ? viewer.selectPress : null;
        if (press) {
          viewer.selectPress = null;
          await offerSelectMenu(session, viewer, press);
          releaseSelectHandle(viewer, session, press);
        }
        return;
      }
      case "wheel":
        viewer.scrolledAt = Date.now();
        await session.send("Input.dispatchMouseEvent", {
          type: "mouseWheel",
          x: num(message.x),
          y: num(message.y),
          deltaX: num(message.deltaX),
          deltaY: num(message.deltaY),
          modifiers,
        });
        return;
      case "key": {
        const key = typeof message.key === "string" ? message.key : "";
        const code = typeof message.code === "string" ? message.code : "";
        const text = typeof message.text === "string" ? message.text : undefined;
        if (message.action === "up") {
          await session.send("Input.dispatchKeyEvent", {
            type: "keyUp",
            key,
            code,
            modifiers,
            // Pages read keyup.keyCode too; Chrome sends the same code as on keydown.
            windowsVirtualKeyCode: num(
              message.keyCode,
              key.length === 1 ? key.toUpperCase().charCodeAt(0) : 0,
            ),
          });
          viewer.pressedKeys.delete(code || key);
          return;
        }
        const command = editingCommand(key, modifiers);
        const copying =
          !tab.desktop && (command?.commands[0] === "copy" || command?.commands[0] === "cut");

        // Copy keys get disjoint millisecond windows so each frame's copy belongs to one key.
        if (copying)
          while (Date.now() <= tab.copyKeyedUntil) await sleepUntil(tab.copyKeyedUntil + 1);
        const since = Date.now();
        const keyCode = num(
          message.keyCode,
          key.length === 1 ? key.toUpperCase().charCodeAt(0) : 0,
        );
        await authorize();
        await session.send("Input.dispatchKeyEvent", {
          type: text ? "keyDown" : "rawKeyDown",
          key,
          code,
          modifiers,
          ...(text ? { text, unmodifiedText: text } : {}),
          ...command,
          windowsVirtualKeyCode: keyCode,
        });
        viewer.pressedKeys.set(code || key, { key, code, keyCode });
        if (!copying) return;
        const until = Date.now();
        tab.copyKeyedUntil = until;
        // A busy frame answers late; its read must not hold later input.
        const read = tab.copyReads.then(async () => {
          await authorize();
          const copied = await takeCopiedText(tab, since, until);
          await authorize();
          if (copied) viewer.push({ _tag: "clipboard", text: copied });
        });
        tab.copyReads = read.catch(constVoid);
        return () => read;
      }
      case "text":
        if (typeof message.text === "string" && message.text.length > 0) {
          await session.send("Input.insertText", { text: message.text.slice(0, 10_000) });
        }
        return;
      case "resize": {
        const width = Math.min(Math.round(num(message.width)), 3840);
        const height = Math.min(Math.round(num(message.height)), 2160);
        if (width < 100 || height < 100) return;
        viewer.requestedSize = { width, height, order: ++viewerResizeOrder };
        if (tab.setting._tag !== "fill" || tab.desktop) return;
        const current = previewViewportSize(tab.page);
        if (current?.width === width && current.height === height) return;
        await resizeAgentViewport(
          tab.page,
          tab.cdp,
          { width, height },
          previewViewportZoom(tab.page),
        );
        await broadcastViewport(tab);
        return;
      }
      case "viewport": {
        const setting = decodeViewportSetting(message.setting);
        await runPromise(
          manager.resize(
            { threadId: tab.threadId, tabId: tab.tabId, viewport: setting },
            authorize,
          ),
        );
        await applySetting(tab, setting, authorize);
        return;
      }
      // A stalled navigation must not block a corrected URL or further viewer input; only the
      // bounded wait for a page script that never yields comes first.
      case "navigate":
        if (typeof message.url === "string") {
          const url = normalizePreviewUrl(message.url);
          tab.pendingInitialRequestUrl = null;
          await leaveBusyPage(tab, session);
          tab.control.trackUntilHandoff(tab.page.goto(url, VIEWER_NAVIGATION_OPTIONS));
        }
        return;
      case "history":
        tab.pendingInitialRequestUrl = null;
        await leaveBusyPage(tab, session);
        tab.control.trackUntilHandoff(
          num(message.delta) < 0
            ? tab.page.goBack(VIEWER_NAVIGATION_OPTIONS)
            : tab.page.goForward(VIEWER_NAVIGATION_OPTIONS),
        );
        return;
      case "stopLoading": {
        const generation = tab.navigationGeneration;
        tab.stoppedGeneration = generation;
        await session.send("Page.stopLoading");
        // Stopping a committed document need not emit Playwright's load or requestfailed events.
        settleStoppedNavigation(tab, generation);
        return;
      }
      case "reload":
        tab.pendingInitialRequestUrl = null;
        if (tab.crash) {
          tab.control.trackUntilHandoff(
            tab.page.goto(tab.crashedUrl ?? tab.crash.url, VIEWER_NAVIGATION_OPTIONS),
          );
          return;
        }
        await leaveBusyPage(tab, session);
        if (message.ignoreCache === true) {
          await session.send("Page.reload", { ignoreCache: true });
          return;
        }
        tab.control.trackUntilHandoff(tab.page.reload(VIEWER_NAVIGATION_OPTIONS));
        return;
      case "selectMenu": {
        const offer = viewer.selectMenu;
        if (!offer || message.id !== offer.id || typeof message.index !== "number") return;
        viewer.selectMenu = null;
        await session
          .send("Runtime.callFunctionOn", {
            objectId: offer.objectId,
            functionDeclaration: CHOOSE_SELECT_OPTION_SCRIPT,
            arguments: [{ value: message.index }],
            userGesture: true,
          })
          .catch(constVoid);
        releaseSelectHandle(viewer, session, offer.objectId);
        return;
      }
      case "probe": {
        const x = num(message.x);
        const y = num(message.y);
        const result = await session.send("Runtime.evaluate", {
          expression: `(${EDITABLE_AT_POINT_SCRIPT})(${x}, ${y})`,
          returnByValue: true,
        });
        await authorize();
        viewer.push({ _tag: "probe", x, y, editable: result.result.value === true });
        return;
      }
    }
  };

  const attachViewer: ServerBrowser["Service"]["attachViewer"] = Effect.fn(
    "ServerBrowser.attachViewer",
  )(function* (input) {
    const authorize = input.authorize ?? (async () => {});
    const recheck = Effect.tryPromise({
      try: authorize,
      catch: (cause) => new ServerBrowserLaunchError({ cause }),
    });
    yield* recheck;
    const tab = yield* findTab(input.threadId, input.tabId);
    yield* recheck;
    const output = yield* Queue.make<ServerBrowserViewerOutput>({
      capacity: VIEWER_OUTPUT_LIMIT,
      strategy: "dropping",
    });
    const session = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () => tab.page.context().newCDPSession(tab.page),
        catch: (cause) => new ServerBrowserLaunchError({ cause }),
      }),
      (session) =>
        Effect.promise(() =>
          session
            .send("Page.stopScreencast")
            .catch(constVoid)
            .then(() => session.detach())
            .catch(constVoid),
        ),
    );
    yield* recheck;
    const quality = Math.min(100, Math.max(1, Math.round(input.quality)));
    // Chromium may drop the final frame during a burst; send a still when it settles.
    let framesInFlight = 0;
    let mayHaveDropped = false;
    let motion = false;
    const recentFrames: Array<number> = [];
    let screencastParams = Promise.resolve();
    let screencastScale = 1;
    const startScreencast = (scale: number) => {
      screencastScale = scale;
      screencastParams = screencastParams.then(async () => {
        if (tab.capturing > 0) return;
        await authorize();
        await session.send("Page.stopScreencast").catch(constVoid);
        await authorize();
        await session
          .send("Page.startScreencast", {
            format: "jpeg",
            quality: screencastScale < 1 ? Math.min(quality, SCREENCAST_MOTION_QUALITY) : quality,
            maxWidth: Math.max(1, Math.round(input.maxWidth * screencastScale)),
            maxHeight: Math.max(1, Math.round(input.maxHeight * screencastScale)),
          })
          .catch(constVoid);
      });
      return screencastParams;
    };
    const viewer: ViewerState = {
      authorize,
      id: NodeCrypto.randomUUID(),
      resourceViewerId: input.resourceViewerId,
      canOperate: input.canOperate,
      pressedKeys: new Map(),
      pressedButtons: new Map(),
      selectPress: null,
      selectMenu: null,
      push: (next) => {
        // Dropped frames must still release Chromium.
        if (Queue.offerUnsafe(output, next)) return;
        if (next._tag === "frame") runFork(next.ack);
        // State a stalled viewer cannot miss compacts its backlog. It runs
        // synchronously so an older compaction can never land after a newer one.
        else if (
          [
            "gone",
            "reconnect",
            "control",
            "controlFailed",
            "viewport",
            "fileChooser",
            "selectMenu",
            "selectMenuClosed",
          ].includes(next._tag)
        ) {
          const dropped = runSyncExit(Queue.clear(output));
          if (dropped._tag === "Failure") return;
          const items = [...dropped.value, next];
          // Without its latest size and control the viewer cannot map input at all.
          const state = (["control", "viewport"] as const).flatMap(
            (tag) => items.findLast((item) => item._tag === tag) ?? [],
          );
          // Everything else is one-time; if it no longer fits, the oldest goes first.
          const rest = items.filter(
            (item) => item._tag !== "frame" && item._tag !== "control" && item._tag !== "viewport",
          );
          for (const item of [...rest.slice(state.length - VIEWER_OUTPUT_LIMIT), ...state])
            Queue.offerUnsafe(output, item);
          for (const item of dropped.value) if (item._tag === "frame") runFork(item.ack);
        }
      },
      pause: () => {
        screencastParams = screencastParams.then(() =>
          session.send("Page.stopScreencast").then(constVoid, constVoid),
        );
        return screencastParams;
      },
      resume: () => startScreencast(screencastScale),
      scrolledAt: 0,
      leaving: false,
      requestedSize: null,
    };
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        tab.viewers.add(viewer);
        tab.viewerAttached?.resolve();
        tab.viewerAttached = null;
        reportLiveTabs();
      }),
      () =>
        Effect.promise(async () => {
          viewer.leaving = true;
          // Terminal admission refusal retires control; transport loss retains it for reconnection.
          if (
            input.disconnectReason?.() === "inputOverload" &&
            tab.control.controller === viewer.id
          )
            await tab.control.release(viewer.id, () => releaseViewerInput(viewer, session));
          else await tab.control.disconnect(viewer.id, () => releaseViewerInput(viewer, session));
          tab.viewers.delete(viewer);
          resumeHumanControl(tab);
          broadcastControl(tab);
          reportLiveTabs();
        }),
    );
    if (input.canOperate && tab.control.agentId === null && tab.control.controller === null) {
      yield* Effect.tryPromise({
        try: () => tab.control.take(viewer.id, authorize),
        catch: (cause) => new ServerBrowserLaunchError({ cause }),
      });
    } else resumeHumanControl(tab);
    broadcastControl(tab);
    pushFileChooser(tab);
    // Full scale: a scaled capture would flash in every other viewer.
    const pushStill = async () => {
      const data = await withCaptureLock(tab, async () => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          return await Promise.race([
            ServerBrowserPage.captureViewport(session, { format: "jpeg", quality }, authorize),
            new Promise<null>((resolve) => {
              timer = setTimeout(() => resolve(null), VIEWER_STILL_TIMEOUT_MS);
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      }).catch(() => null);
      if (data) viewer.push({ _tag: "frame", data: Buffer.from(data, "base64"), ack: Effect.void });
    };
    let settleTimer: ReturnType<typeof setTimeout> | null = null;
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (settleTimer !== null) clearTimeout(settleTimer);
      }),
    );
    let screencastStarted = false;
    session.on("Page.screencastFrame", (frame) => {
      screencastStarted = true;
      if (framesInFlight > 0) mayHaveDropped = true;
      framesInFlight += 1;
      const arrivedAt = Date.now();
      recentFrames.push(arrivedAt);
      while (recentFrames[0]! < arrivedAt - SCREENCAST_MOTION_WINDOW_MS) recentFrames.shift();
      if (
        !motion &&
        recentFrames.length >= SCREENCAST_MOTION_FRAMES &&
        arrivedAt - viewer.scrolledAt < SCREENCAST_MOTION_WINDOW_MS
      ) {
        motion = true;
        void startScreencast(0.5).catch(() => viewer.push({ _tag: "reconnect" }));
      } else if (motion && arrivedAt - viewer.scrolledAt > SCREENCAST_SETTLE_MS) {
        motion = false;
        recentFrames.length = 0;
        void startScreencast(1).catch(() => viewer.push({ _tag: "reconnect" }));
      }
      if (settleTimer !== null) clearTimeout(settleTimer);
      settleTimer = setTimeout(() => {
        settleTimer = null;
        if (!motion && !mayHaveDropped) return;
        mayHaveDropped = false;
        if (motion) {
          motion = false;
          recentFrames.length = 0;
          void startScreencast(1).catch(() => viewer.push({ _tag: "reconnect" }));
        }
        void pushStill();
      }, SCREENCAST_SETTLE_MS);
      viewer.push({
        _tag: "frame",
        data: Buffer.from(frame.data, "base64"),
        ack: Effect.promise(() =>
          sleepUntil(arrivedAt + SCREENCAST_ACK_PACE_MS).then(() => {
            framesInFlight -= 1;
            return session
              .send("Page.screencastFrameAck", { sessionId: frame.sessionId })
              .catch(constVoid);
          }),
        ),
      });
    });
    broadcastViewport(tab);
    yield* Effect.tryPromise({
      try: () => startScreencast(1),
      catch: (cause) => new ServerBrowserLaunchError({ cause }),
    });
    // An idle page does not repaint for a new screencast, so the viewer
    // starts from a still unless a live frame beat it.
    if (!screencastStarted) yield* Effect.forkScoped(Effect.promise(pushStill));
    const prepareInput = Effect.fnUntraced(function* (raw: unknown) {
      const message = normalizeViewerInput(raw);
      if (!viewer.canOperate || !message) return Effect.void;
      const lane =
        message.type === "dialog"
          ? "dialog"
          : message.type === "takeControl" || message.type === "releaseControl"
            ? "control"
            : "action";
      const release = tab.control.reserveInput(Buffer.byteLength(encodeInputJson(message)), lane);
      if (release === null) return yield* new BrowserInputOverloaded({});
      const controlGeneration = lane === "control" ? tab.control.generation : null;
      const inputAdmission =
        lane === "control"
          ? null
          : {
              dialog: message.type === "dialog" ? tab.dialog : null,
              controller: tab.control.controller,
              generation: tab.control.generation,
            };
      const assertCurrentInput = () => {
        if (
          !inputAdmission ||
          inputAdmission.controller !== viewer.id ||
          tab.control.controller !== viewer.id ||
          tab.control.generation !== inputAdmission.generation ||
          (message.type === "dialog" &&
            (!inputAdmission.dialog || tab.dialog !== inputAdmission.dialog))
        ) {
          throw new BrowserControlInterrupted();
        }
      };
      // The promise owns the reservation even if its observing Effect is
      // interrupted: native work and queued closures may still be draining.
      const work = (async () => {
        try {
          if (lane === "action") {
            const authorizeInput = async () => {
              await authorize();
              assertCurrentInput();
            };
            const dispatch = async (input: Record<string, unknown>) => {
              await dispatchViewerInput(tab, session, viewer, input, authorizeInput);
            };
            if (
              message.type === "mouse" &&
              message.action === "move" &&
              message.button === "none" &&
              message.buttons === 0
            ) {
              await tab.control.humanLatest(
                viewer.id,
                `hover:${modifiersOf(message)}`,
                message,
                dispatch,
              );
            } else if (message.type === "wheel") {
              // Keep reversals separate so scrolling at an edge behaves as if unmerged.
              await tab.control.humanLatest(
                viewer.id,
                `wheel:${num(message.x)}:${num(message.y)}:${modifiersOf(message)}:${Math.sign(num(message.deltaX))}:${Math.sign(num(message.deltaY))}`,
                message,
                dispatch,
                (waiting, next) => ({
                  ...next,
                  deltaX: num(waiting.deltaX) + num(next.deltaX),
                  deltaY: num(waiting.deltaY) + num(next.deltaY),
                }),
              );
            } else {
              const copyRead = await tab.control.human(
                viewer.id,
                () => dispatchViewerInput(tab, session, viewer, message, authorize),
                async () => {
                  await authorize();
                  assertCurrentInput();
                  await authorize();
                },
              );
              await copyRead?.();
            }
            return;
          }
          await authorize();
          if (controlGeneration !== null && tab.control.generation !== controlGeneration)
            throw new BrowserControlInterrupted();
          if (inputAdmission) assertCurrentInput();
          if (message.type === "takeControl") {
            const taking = tab.control.take(viewer.id, authorize, () => broadcastControl(tab));
            await taking;
            pushFileChooser(tab);
          } else if (message.type === "releaseControl") {
            const releasing = tab.control.release(viewer.id, () =>
              releaseViewerInput(viewer, session),
            );
            broadcastControl(tab);
            await releasing;
            void settleSiteHold(tab, "release");
          } else if (
            message.type === "dialog" &&
            tab.control.controller === viewer.id &&
            typeof message.accept === "boolean"
          ) {
            await authorize();
            assertCurrentInput();
            await resolveDialog(tab, {
              accept: message.accept,
              ...(typeof message.promptText === "string" ? { promptText: message.promptText } : {}),
            });
          } else {
            const copyRead = await tab.control.human(
              viewer.id,
              () => dispatchViewerInput(tab, session, viewer, message, authorize),
              authorize,
            );
            await copyRead?.();
          }
        } catch {
          if (message.type === "takeControl" || message.type === "releaseControl")
            viewer.push({
              _tag: "controlFailed",
              action: message.type === "takeControl" ? "take" : "release",
            });
          broadcastControl(tab);
        } finally {
          release();
        }
      })();
      return Effect.promise(() => work);
    });
    return {
      output,
      input: (raw: unknown) => prepareInput(raw).pipe(Effect.flatten),
      enqueueInput: (raw: unknown) =>
        prepareInput(raw).pipe(
          Effect.flatMap((work) => work.pipe(Effect.forkScoped, Effect.asVoid)),
        ),
    } satisfies ServerBrowserViewer;
  });

  // Subscribe before forking: a forked stream subscribes only once it runs, so a tab opened
  // right after startup would publish its `opened` event to no one and never load.
  const managerEvents = yield* manager.subscribeServerEvents;
  yield* Stream.fromSubscription(managerEvents).pipe(
    Stream.runForEach(mirrorManagerEvent),
    Effect.forkScoped,
  );
  yield* orchestrator.streamDomainEvents.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => {
        if (event.type === "thread.deleted" || event.type === "thread.archived") {
          watches.terminateThread(event.threadId, "thread_deleted");
          return;
        }
        if (event.type !== "thread.settled") return;
        for (const pending of pendingTabs.values()) {
          if (pending.threadId === event.threadId) pending.settled = true;
        }
        for (const tab of tabs.values()) {
          if (tab.threadId === event.threadId && mayCloseSettledTab(tab)) dropTab(tab, true);
        }
      }),
    ),
    Effect.forkScoped,
  );
  // Whoever runs the server learns the fix before anyone opens a tab.
  if (
    !PreviewBrowserHost.sandboxDisabled(yield* HostProcess.Environment) &&
    (yield* PreviewBrowserHost.sandboxBlocked)
  ) {
    yield* Effect.logWarning(
      `This host blocks the sandbox T3's browser runs in, so browser tabs and HTML previews will not start. Run \`${setupCommand}\` once to allow it.`,
    );
  }

  yield* desktopChannel.detached.pipe(
    Stream.runForEach((key) =>
      Effect.sync(() => {
        const tab = tabs.get(tabKey(key.threadId, key.tabId));
        if (tab?.desktop) dropTab(tab, false);
      }),
    ),
    Effect.forkScoped,
  );
  // The desktop keeps the tabs an agent uses focused until its turn ends, not just per command.
  yield* Stream.fromSubscription(agentTurns).pipe(
    Stream.runForEach((change) => desktopChannel.turn(change.threadId, change.active)),
    Effect.forkScoped,
  );
  // Reconnect returned pages immediately so all clients keep receiving URL and title changes.
  yield* desktopChannel.attached.pipe(
    Stream.runForEach((key) =>
      Effect.gen(function* () {
        if (tabs.has(tabKey(key.threadId, key.tabId))) return;
        const { sessions } = yield* manager.list({ threadId: ThreadId.make(key.threadId) });
        const snapshot = sessions.find(
          (session) => session.tabId === key.tabId && session.runtime === "server",
        );
        if (snapshot) yield* Effect.promise(() => ensureTab(snapshot).catch(constVoid));
      }).pipe(Effect.ignore),
    ),
    Effect.forkScoped,
  );
  yield* Effect.sync(closeIdleAgentTabs).pipe(
    Effect.repeat(Schedule.spaced(IDLE_SWEEP_INTERVAL)),
    Effect.forkScoped,
  );
  const hostSession = broker
    .connect(
      {
        clientId: SERVER_HOST_CLIENT_ID,
        environmentId,
        supportedOperations: [...PREVIEW_AUTOMATION_SERVER_OPERATIONS],
      },
      { preferred: true },
    )
    .pipe(
      Effect.flatMap((events) =>
        events.pipe(
          Stream.runForEach((event) => {
            if (event.type === "connected") {
              hostConnectionId = event.connectionId;
              return Effect.sync(reportLiveTabs);
            }
            return handleRequest(event.connectionId, event.request).pipe(
              Effect.forkScoped,
              Effect.asVoid,
            );
          }),
        ),
      ),
    );
  // The broker disconnects timed-out hosts, including slow first installs. Reconnect.
  yield* hostSession.pipe(
    Effect.exit,
    Effect.andThen(Effect.sleep(HOST_RECONNECT_DELAY)),
    Effect.forever,
    Effect.forkScoped,
  );
  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      await contexts.close();
    }),
  );

  const clearProfile = (profileId: string) =>
    Effect.tryPromise({
      try: () => contexts.clearProfile(profileId),
      catch: (cause) => new PreviewClearProfileError({ profileId, cause }),
    });

  const loginCookies = new ServerLoginCookies(cookieMirror, {
    find: (profileId) => availableProfiles().find((profile) => profile.id === profileId),
  });
  const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);
  // Causes may quote cookie values, so only refusals keep their own message.
  const loginCookieCall = <A>(run: () => Promise<A>) =>
    Effect.tryPromise({
      try: run,
      catch: (cause) =>
        isOrchestratorMcpFailure(cause)
          ? cause
          : new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The browser could not read or write this profile's cookies.",
            }),
    });

  return ServerBrowser.of({
    readWatch: Effect.fn("ServerBrowser.readWatch")(function* (input, authorizeReader) {
      const context = yield* Effect.context<Effect.Services<typeof authorizeReader>>();
      return yield* Effect.tryPromise({
        try: (signal) =>
          watches.read(
            input,
            async () => {
              const result = await Effect.runPromiseWith(context)(Effect.result(authorizeReader));
              if (result._tag === "Failure") throw result.failure;
            },
            signal,
          ),
        catch: (cause) =>
          isWatchReadFailure(cause)
            ? cause
            : new PreviewWatchError({ reason: "subscription_failed" }),
      });
    }),
    mirrorResets: Stream.fromPubSub(mirrorResets),
    mirrorCookies: Effect.fn("ServerBrowser.mirrorCookies")((batch) =>
      Effect.tryPromise({
        try: async () => {
          if (catalogRead._tag === "Failure")
            throw new Error("Mirrored profile catalog unavailable.");
          if (batch.mutations.length > 0) {
            const context = await cookieMirror.apply(batch);
            mirroredContexts.set(context, batch.profile.id);
          }
          await catalog.put(batch.profile);
          mirroredProfiles.set(batch.profile.id, batch.profile);
        },
        catch: () => new CookieMirrorError({ reason: "unavailable" }),
      }),
    ),
    authorizeViewer: Effect.fn("ServerBrowser.authorizeViewer")(function* (resource) {
      const current = yield* manager.list({ threadId: resource.threadId });
      if (
        resource.environmentId !== environmentId ||
        resource.serverEpoch !== current.serverEpoch ||
        !current.sessions.some(
          (session) => session.tabId === resource.tabId && session.runtime === "server",
        )
      )
        return yield* new ServerBrowserTabNotFoundError({
          threadId: resource.threadId,
          tabId: resource.tabId,
        });
    }),
    attachViewer,
    clearProfile,
    loginCookies: {
      check: (profileId) => loginCookieCall(async () => loginCookies.check(profileId)),
      export: (input) => loginCookieCall(() => loginCookies.export(input)),
      import: (input) => loginCookieCall(() => loginCookies.import(input)),
    },
    profileReporter,
    openDownload,
    inspectDownload: Effect.fn("ServerBrowser.inspectDownload")((resource, authorize) =>
      nativeDownload(resource, authorize).pipe(Effect.asVoid),
    ),
    readDownloadBlock: Effect.fn("ServerBrowser.readDownloadBlock")((resource, offset, authorize) =>
      nativeDownload(resource, authorize, offset),
    ),
    answerFileChooser,
  });
});

export const layer = Layer.effect(ServerBrowser, make);
