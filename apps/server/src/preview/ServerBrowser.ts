// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off - Playwright callbacks run outside the Effect runtime.
// Screencasts ignore emulated device scale; real 2x keeps captures sharp.
// --disable-gpu uses cheaper software compositing while preserving SwiftShader WebGL.
import {
  BUILT_IN_BROWSER_PROFILES,
  CookieMirrorError,
  type CookieMirrorBatch,
  OrchestratorMcpFailure,
  type NativeBrowserDownloadAssetResource,
  DEFAULT_BROWSER_PROFILE_ID,
  FILL_PREVIEW_VIEWPORT,
  findBrowserProfile,
  INCOGNITO_BROWSER_PROFILE_ID,
  resolveBrowserProfiles,
  type BrowserProfile,
  type PreviewReportProfilesInput,
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
  type PreviewAutomationHoverInput,
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
import * as HostProcess from "@t3tools/shared/HostProcess";
import { normalizePreviewUrl } from "@t3tools/shared/preview";
import { resolvePreviewViewport } from "@t3tools/shared/previewViewport";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { constVoid } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
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
import {
  fillCredential,
  loginInProgress,
  LOGIN_OPERATIONS,
  serverBrowserOnly,
} from "./CredentialFill.ts";
import { CredentialVault } from "./CredentialVault.ts";
import * as DesktopBrowserChannel from "./DesktopBrowserChannel.ts";
import * as PreviewManager from "./Manager.ts";
import * as ServerBrowserPage from "./ServerBrowserPage.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as PreviewBrowserHost from "./PreviewBrowserHost.ts";
import { ServerCookieMirrorProfiles } from "./ServerCookieMirrorProfiles.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";
import { presentAsChrome, ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import { BrowserControlInterrupted, SessionControl } from "./SessionControl.ts";

const SERVER_HOST_CLIENT_ID = SERVER_BROWSER_AUTOMATION_CLIENT_ID;
const RENDER_SCALE = 2;
// Chromium allows three unacked frames, so 100 ms pacing caps viewers near 30 fps.
const SCREENCAST_ACK_PACE_MS = 100;
const SCREENCAST_SETTLE_MS = 200;
const SCREENCAST_MOTION_FRAMES = 4;
const SCREENCAST_MOTION_WINDOW_MS = 300;
const SCREENCAST_MOTION_QUALITY = 50;
const HOST_RECONNECT_DELAY = "1 second";
const DESKTOP_ATTACH_TIMEOUT = "10 seconds";
const VIEWER_OUTPUT_LIMIT = 64;
const RECORDING_SCREENCAST = { format: "jpeg", quality: 90, everyNthFrame: 1 } as const;
const decodeViewportSetting = Schema.decodeUnknownSync(PreviewViewportSettingSchema);
const decodeReadPage = Schema.decodeUnknownSync(PreviewAutomationReadPageInput);
const encodeInputJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const AGENT_CURSOR_MOVE_MS = 160;
const AGENT_CURSOR_CLICK_LEAD_MS = 40;

const sleepUntil = (deadline: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, deadline - Date.now())));

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
const UNATTACHED_FILL_VIEWPORT = { width: 1280, height: 800 } as const;
const NAVIGATION_TIMEOUT_MS = 15_000;
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
  ]),
);

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

export class ServerBrowser extends Context.Service<
  ServerBrowser,
  {
    readonly attachViewer: (input: {
      readonly threadId: string;
      readonly tabId: string;
      readonly maxWidth: number;
      readonly maxHeight: number;
      readonly quality: number;
      readonly canOperate: boolean;
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

    readonly mirrorResets: Stream.Stream<string>;
    readonly mirrorCookies: (input: CookieMirrorBatch) => Effect.Effect<void, CookieMirrorError>;

    readonly reportProfiles: (input: PreviewReportProfilesInput) => Effect.Effect<void>;
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
  readonly pressedKeys: Map<string, { key: string; code: string }>;
  readonly pressedButtons: Map<"left" | "middle" | "right", { x: number; y: number }>;
  readonly push: (output: ServerBrowserViewerOutput) => void;
  readonly pause: () => Promise<void>;
  readonly resume: () => Promise<void>;
  scrolledAt: number;

  requestedSize: { width: number; height: number; order: number } | null;
}

interface EncoderWindow {
  __t3Recorder: {
    cursor(x: number, y: number, click: boolean): void;
    frame(data: string, width: number): Promise<boolean>;
    stop(): Promise<{ mimeType: string | null; count: number; bytes: number }>;
    chunk(index: number): Promise<string>;
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
  readonly createdAt: number;
  usedAt: number;
  readonly viewers: Set<ViewerState>;
  readonly consoleEntries: Array<PreviewAutomationConsoleEntry>;
  readonly networkEntries: Array<PreviewAutomationNetworkEntry>;
  readonly actionTimeline: Array<PreviewAutomationActionEvent>;
  readonly control: SessionControl;
  /**
   * The browser profile whose storage the tab shares: a server profile id, its own
   * context for an isolated tab and its popups, or a desktop tab of its own. A login
   * covers a whole profile.
   */
  readonly profileKey: string;

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
  zoomFactor: number;
  loading: boolean;
  closing: boolean;
  recording: Recording | null;
  initialNavigation: Promise<void> | null;
  navigationGeneration: number;

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

// Results that are a tab's status, taken again when a login began meanwhile.
const STATUS_RESULTS: ReadonlySet<PreviewAutomationRequest["operation"]> = new Set([
  "status",
  "open",
  "navigate",
  "dialog",
]);
// Results that carry no page content whenever they finish.
const CONTENT_FREE_RESULTS: ReadonlySet<PreviewAutomationRequest["operation"]> = new Set([
  "click",
  "press",
  "type",
  "waitFor",
  "fillCredential",
  "endLogin",
  "close",
]);

const AGENT_TAB_LIMIT = 8;
const SERVER_TAB_LIMIT = 32;
const AGENT_TAB_IDLE_MS = 30 * 60 * 1000;
const IDLE_SWEEP_INTERVAL = "1 minute";

const DOWNLOAD_LIMIT = 20;
const ABORTED_NAVIGATION_DOWNLOAD_WAIT_MS = 5_000;
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
  ["probe", ["x", "y"]],
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
    } else if (["action", "button", "key", "code", "text", "url", "promptText"].includes(field)) {
      if (typeof value === "string") message[field] = value;
    } else if (typeof value === "number" && Number.isFinite(value)) message[field] = value;
  }
  if (source.type === "mouse") {
    message.action = source.action === "down" || source.action === "up" ? source.action : "move";
    try {
      const button = String(source.button);
      message.button = ["none", "left", "middle", "right"].includes(button) ? button : "none";
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

const CLIPBOARD_TEXT_LIMIT = 1024 * 1024;
const CLIPBOARD_WORLD = "t3-preview-clipboard";
// Headless Chromium shares one clipboard between every context, so the copy
// a viewer's copy or cut key makes is read from this world instead of the
// clipboard. Page scripts cannot see the world, and only a copy made while a
// real key is handled counts.
const CLIPBOARD_SCRIPT = `(() => {
  let keying = false;
  let copied = "";
  // The same Ctrl or Cmd + C or X that editingCommand sends as copy or cut.
  addEventListener("keydown", (event) => {
    if (!event.isTrusted || event.altKey || event.ctrlKey === event.metaKey) return;
    const key = event.key.toLowerCase();
    if (key !== "c" && key !== "x") return;
    keying = true;
    copied = "";
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
    copied = String(text ?? "").slice(0, ${CLIPBOARD_TEXT_LIMIT});
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
  globalThis.takeCopy = () => {
    const text = copied;
    copied = "";
    return text;
  };
})();`;

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const host = { platform: yield* HostProcess.Platform, arch: yield* HostProcess.Architecture };
  const manager = yield* PreviewManager.PreviewManager;
  const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const previewBrowser = yield* PreviewBrowser.PreviewBrowser;
  const desktopChannel = yield* DesktopBrowserChannel.DesktopBrowserChannel;
  const vault = yield* CredentialVault;
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const launchServices = yield* Effect.context<ChildProcessSpawner.ChildProcessSpawner>();
  const setupCommand = yield* resolveRootCliCommand(PreviewBrowserHost.SETUP_SUBCOMMAND);

  const tabs = new Map<string, ServerTab>();
  const pendingTabs = new Map<string, Promise<ServerTab>>();
  // Retain the original invocation across a failed launch or a desktop reconnect.
  // These are current checks, never cached grant results.
  const tabAuthorizations = new Map<string, () => Promise<void>>();
  /** Sessions closed while their tab was still opening; the open discards its page. */
  const closedPendingTabs = new Set<string>();
  const adoptedPages = new Map<
    string,
    { readonly page: Page; readonly openerTabId: string; readonly profileKey: string }
  >();

  /**
   * Logins, one per server browser profile. A fill starts one, and while it lasts
   * nothing in any tab of the profile can be read and every tab created there starts
   * in it. It ends only when the profile's browser context has closed: through
   * preview_end_login, which closes it and waits, or any other close. Pages and
   * attachments are never counted; the next tab opens a fresh context from the same
   * profile directory. `loginEpoch` counts each change so results can be checked
   * again as they leave.
   */
  const profileLogins = new Map<string, { ending: boolean; context: BrowserContext }>();
  let loginEpoch = 0;
  const inLogin = (tab: ServerTab) => profileLogins.has(tab.profileKey);
  const profileTabs = (profileKey: string) =>
    [...tabs.values()].filter((tab) => tab.profileKey === profileKey);
  const endLoginsOf = (context: BrowserContext) => {
    for (const [profileKey, login] of profileLogins)
      if (login.context === context) {
        profileLogins.delete(profileKey);
        loginEpoch++;
      }
  };
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
    executable: () => Effect.runPromise(previewBrowser.executable),
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
      endLoginsOf(context);
      const profileId = mirroredContexts.get(context);
      if (profileId !== undefined) runFork(PubSub.publish(mirrorResets, profileId));
      for (const tab of tabs.values()) {
        if (tab.page.context() === context) dropTab(tab, true);
      }
    },
  });
  const cookieMirror = new ServerCookieMirror(contexts);

  const dialogStatus = (tab: ServerTab) =>
    tab.dialog
      ? {
          type: tab.dialog.type(),
          message: tab.dialog.message().slice(0, 4000),
          defaultValue: tab.dialog.defaultValue().slice(0, 4000),
        }
      : null;

  const broadcastControl = (tab: ServerTab) => {
    for (const viewer of tab.viewers)
      viewer.push({
        _tag: "control",
        canOperate: viewer.canOperate,
        controller:
          tab.control.controller === viewer.id
            ? "you"
            : tab.control.controller !== null
              ? "another-viewer"
              : tab.control.agentId !== null
                ? "agent"
                : "unclaimed",
        generation: tab.control.generation,
        dialog: dialogStatus(tab),
      });
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

  const report = (tab: ServerTab, status: PreviewNavStatus) => {
    // Session state reaches every client; a login page's URL or title can hold a value.
    const navStatus: PreviewNavStatus = inLogin(tab)
      ? { _tag: "Success", url: "about:blank", title: "Login in progress" }
      : status;
    void tab.cdp
      .send("Page.getNavigationHistory")
      .catch(() => null)
      .then((history) => {
        const index = history?.currentIndex ?? 0;
        const count = history?.entries.length ?? 0;
        runFork(
          manager
            .reportStatus({
              threadId: tab.threadId,
              tabId: tab.tabId,
              serverControlled: true,
              navStatus,
              canGoBack: index > 0,
              canGoForward: index < count - 1,
            })
            .pipe(Effect.ignore),
        );
      });
  };

  const reportLoaded = async (tab: ServerTab) => {
    const url = tab.page.url();
    // Chromium's error page loads after `requestfailed` and must not clear LoadFailed.
    if (url === "about:blank" || url.startsWith("chrome-error://")) return;
    const title = (await tab.page.title().catch(() => "")).slice(0, 512);
    if (tab.loading || tab.page.url() !== url) return;
    report(tab, { _tag: "Success", url: url.slice(0, 2048), title });
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

  const layoutSize = (tab: ServerTab) => {
    const size = tab.page.viewportSize();
    if (!size || tab.desktop) return size;
    return {
      width: Math.max(1, Math.round(size.width / tab.zoomFactor)),
      height: Math.max(1, Math.round(size.height / tab.zoomFactor)),
    };
  };

  const applyZoom = async (tab: ServerTab) => {
    if (tab.desktop) return;
    const size = tab.page.viewportSize();
    if (!size) return;
    if (tab.zoomFactor === 1) {
      await tab.page.setViewportSize(size);
      return;
    }
    const layout = layoutSize(tab)!;
    await tab.cdp.send("Emulation.setDeviceMetricsOverride", {
      width: layout.width,
      height: layout.height,
      deviceScaleFactor: RENDER_SCALE * tab.zoomFactor,
      mobile: false,
    });
  };

  const broadcastViewport = (tab: ServerTab) => {
    const size = layoutSize(tab);
    if (!size) return;
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
    await tab.page.setViewportSize({ width: size.width, height: size.height });
    await authorize();
    await applyZoom(tab);
    broadcastViewport(tab);
  };

  /** Applies a tab's published appearance and, for headless tabs, zoom. */
  const applyRendering = async (
    tab: ServerTab,
    snapshot: PreviewSessionSnapshot,
    authorize = async () => {},
  ) => {
    await authorize();
    const colorScheme = snapshot.colorScheme ?? "system";
    if (colorScheme !== tab.colorScheme) {
      tab.colorScheme = colorScheme;
      await tab.page.emulateMedia({ colorScheme: colorScheme === "system" ? null : colorScheme });
    }
    const zoomFactor = snapshot.zoomFactor ?? 1;
    if (zoomFactor !== tab.zoomFactor && !tab.desktop) {
      await authorize();
      tab.zoomFactor = zoomFactor;
      await applyZoom(tab);
      broadcastViewport(tab);
    }
  };

  const sessionOpen = (tab: ServerTab) => !closedSessions.has(tabKey(tab.threadId, tab.tabId));

  const dropTab = (tab: ServerTab, closeSession: boolean) => {
    const key = tabKey(tab.threadId, tab.tabId);
    if (tabs.get(key) !== tab) return;
    tabs.delete(key);
    tab.closing = true;
    clearAbortedNavigation(tab);
    // A desktop page outlives the connection unless its session closed with it.
    const end = tab.desktop && !closeSession && sessionOpen(tab) ? "reconnect" : "gone";
    for (const viewer of tab.viewers) viewer.push({ _tag: end });
    void tab.control.close().catch(constVoid);
    if (tab.desktop) void tab.desktop.close().catch(constVoid);
    else void tab.page.close().catch(constVoid);
    if (tab.isolatedContext) void tab.page.context().close().catch(constVoid);
    void tab.recording?.encoder.close().catch(constVoid);
    void NodeFSP.rm(downloadDir(tab), { recursive: true, force: true }).catch(constVoid);
    reportLiveTabs();
    if (closeSession) {
      runFork(manager.close({ threadId: tab.threadId, tabId: tab.tabId }).pipe(Effect.ignore));
    }
  };

  const desktopRenders = (snapshot: PreviewSessionSnapshot) =>
    desktopChannel.available
      ? Effect.runPromise(
          desktopChannel.awaitAttached(
            { threadId: snapshot.threadId, tabId: snapshot.tabId },
            DESKTOP_ATTACH_TIMEOUT,
          ),
        )
      : Promise.resolve(false);

  const connectDesktop = async (snapshot: PreviewSessionSnapshot) => {
    const scope = await Effect.runPromise(Scope.make());
    try {
      const endpoint = await Effect.runPromise(
        desktopChannel
          .endpoint({ threadId: snapshot.threadId, tabId: snapshot.tabId })
          .pipe(Scope.provide(scope)),
      );
      const connected = await contexts.connectDesktopPage(endpoint);
      return {
        page: connected.page,
        close: async () => {
          await connected.browser.close().catch(constVoid);
          await Effect.runPromise(Scope.close(scope, Exit.void));
        },
      };
    } catch (cause) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
      throw cause;
    }
  };

  const createTab = async (
    snapshot: PreviewSessionSnapshot,
    authorize: () => Promise<void>,
  ): Promise<ServerTab> => {
    await authorize();
    const adopted = adoptedPages.get(tabKey(snapshot.threadId, snapshot.tabId));
    adoptedPages.delete(tabKey(snapshot.threadId, snapshot.tabId));
    const desktop =
      adopted === undefined && (await desktopRenders(snapshot))
        ? await connectDesktop(snapshot)
        : null;
    const isolatedContext =
      adopted === undefined &&
      desktop === null &&
      ((snapshot.automationOwner !== undefined && snapshot.profileId === undefined) ||
        snapshot.profileId === INCOGNITO_BROWSER_PROFILE_ID);
    await authorize();
    const context =
      adopted?.page.context() ??
      desktop?.page.context() ??
      (await contexts.contextFor(
        snapshot.profileId ?? "default",
        isolatedContext ? tabKey(snapshot.threadId, snapshot.tabId) : undefined,
      ));
    let pendingPage: Page | undefined;
    let pendingCdp: CDPSession | undefined;
    let createdTab: ServerTab | undefined;
    try {
      await authorize();
      if (adopted?.page.isClosed()) throw new Error("The popup closed before it opened.");
      // The desktop page already has its own clipboard.
      if (!desktop) await prepareContext(context);
      await authorize();
      const page = (pendingPage = adopted?.page ?? desktop?.page ?? (await context.newPage()));
      await authorize();
      const cdp = (pendingCdp = await context.newCDPSession(page));
      await authorize();
      // The desktop's page is already a normal browser. A popup's first request
      // went out before it became a tab; everything after presents as Chrome.
      if (!desktop) await presentAsChrome(cdp, host);
      await authorize();
      if (!desktop) await recordCopies(cdp, authorize);
      page.setDefaultTimeout(NAVIGATION_TIMEOUT_MS);
      page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
      const control = new SessionControl(snapshot.automationOwner ?? null, () =>
        ServerBrowserPage.invalidateRefs(page),
      );
      const tab: ServerTab = (createdTab = {
        authorize,
        threadId: ThreadId.make(snapshot.threadId),
        tabId: snapshot.tabId,
        page,
        cdp,
        frameSessions: new Map(),
        createdAt: Date.now(),
        usedAt: Date.now(),
        viewers: new Set(),
        consoleEntries: [],
        networkEntries: [],
        actionTimeline: [],
        control,
        profileKey:
          adopted?.profileKey ??
          (desktop
            ? `desktop:${tabKey(snapshot.threadId, snapshot.tabId)}`
            : isolatedContext
              ? `isolated:${tabKey(snapshot.threadId, snapshot.tabId)}`
              : `profile:${snapshot.profileId ?? DEFAULT_BROWSER_PROFILE_ID}`),
        isolatedContext,
        desktop: desktop === null ? null : { close: desktop.close },
        profileId: snapshot.profileId,
        openerTabId: adopted?.openerTabId,
        downloads: [],
        fileChooser: null,
        dialog: null,
        setting: snapshot.viewport ?? FILL_PREVIEW_VIEWPORT,
        colorScheme: "system",
        zoomFactor: 1,
        loading: false,
        closing: false,
        recording: null,
        recordingStart: null,
        initialNavigation: null,
        navigationGeneration: 0,
        abortedNavigation: null,
        captureLock: Promise.resolve(),
        capturing: 0,
      });
      if (!desktop) {
        await authorize();
        await page.setViewportSize(fixedViewportSize(tab.setting) ?? UNATTACHED_FILL_VIEWPORT);
      }
      await applyRendering(tab, snapshot, authorize);
      const isMainNavigation = (request: { isNavigationRequest(): boolean; frame(): unknown }) =>
        request.isNavigationRequest() && request.frame() === page.mainFrame();
      const navigationGenerations = new WeakMap<object, number>();
      page.on("request", (request) => {
        if (!isMainNavigation(request)) return;
        navigationGenerations.set(request, ++tab.navigationGeneration);
        clearAbortedNavigation(tab);
        tab.loading = true;
        report(tab, { _tag: "Loading", url: request.url().slice(0, 2048), title: "" });
      });
      page.on("load", () => {
        tab.loading = false;
        void reportLoaded(tab);
      });
      page.on("framenavigated", (frame) => {
        // Same-document navigations (SPA routes) fire no load event.
        if (frame === page.mainFrame() && !tab.loading) void reportLoaded(tab);
        if (!desktop) void recordFrameCopies(tab, frame);
      });
      // An adopted popup's frames may have loaded before the tab listened.
      if (adopted) for (const frame of page.frames()) void recordFrameCopies(tab, frame);
      page.on("requestfailed", (request) => {
        const errorText = request.failure()?.errorText ?? "";
        pushBounded(tab.networkEntries, {
          url: request.url(),
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
        report(tab, {
          _tag: "LoadFailed",
          url: request.url().slice(0, 2048),
          title: "",
          code,
          description,
        });
      });
      page.on("response", (response) => {
        pushBounded(tab.networkEntries, {
          url: response.url(),
          method: response.request().method(),
          status: response.status(),
          failed: false,
          timestamp: new Date().toISOString(),
        });
      });
      page.on("console", (message) => {
        pushBounded(tab.consoleEntries, {
          level: message.type(),
          text: message.text().slice(0, 2_000),
          timestamp: new Date().toISOString(),
        });
      });
      page.on("dialog", (dialog) => {
        tab.dialog = dialog;
        broadcastControl(tab);
      });
      page.on("download", (download) => saveDownload(tab, download));
      page.on("filechooser", (chooser) => offerFileChooser(tab, chooser));
      // Popups become tabs and keep `window.opener`, so sign-in popups can report back.
      page.on("popup", (popup) => void adoptPopup(tab, popup));
      // Playwright cannot reload a crashed page, so it must leave the tab list.
      const key = tabKey(tab.threadId, tab.tabId);
      if (closedPendingTabs.delete(key)) {
        await control.close().catch(constVoid);
        if (desktop) await desktop.close().catch(constVoid);
        else await page.close().catch(constVoid);
        if (isolatedContext) await context.close().catch(constVoid);
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      page.on("close", () => dropTab(tab, true));
      page.on("crash", () => dropTab(tab, true));
      if (page.isClosed() || profileLogins.get(tab.profileKey)?.ending) {
        await control.close().catch(constVoid);
        await page.close().catch(constVoid);
        if (isolatedContext) await context.close().catch(constVoid);
        await Effect.runPromise(
          manager.close({ threadId: tab.threadId, tabId: tab.tabId }).pipe(Effect.ignore),
        );
        throw new ServerBrowserTabNotFoundError({ threadId: tab.threadId, tabId: tab.tabId });
      }
      tabs.set(key, tab);
      if (inLogin(tab)) report(tab, { _tag: "Idle" });
      reportLiveTabs();
      // A popup is already loading its own URL, and the desktop loads its tab's.
      if (!adopted && !desktop && snapshot.navStatus._tag === "Loading") {
        await authorize();
        tab.initialNavigation = page
          .goto(snapshot.navStatus.url, { waitUntil: "commit", timeout: NAVIGATION_TIMEOUT_MS })
          .then(constVoid);
        // Background creation keeps the failed tab; automation awaits the original error.
        void tab.initialNavigation.catch(constVoid);
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
        if (isolatedContext) await context.close().catch(constVoid);
      }
      throw cause;
    }
  };

  const downloadsRoot = NodePath.join(config.stateDir, "server-browser", "downloads");
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
      url: url.slice(0, 2048),
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
      await NodeFSP.mkdir(downloadDir(tab), { recursive: true });
      await authorize();
      await download.saveAs(path);
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
        url: download.url().slice(0, 2048),
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

  const offerFileChooser = async (tab: ServerTab, chooser: FileChooser) => {
    const previous = tab.fileChooser;
    const accept =
      (await chooser
        .element()
        .getAttribute("accept")
        .catch(() => null)) ?? "";
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
      throw new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationExecutionError",
        "Browser files must be resolved by their native staging owner before dispatch.",
      );
    const resolved = { ...input, paths: input.paths };
    const relative = input.paths.find((path) => !NodePath.isAbsolute(path));
    if (relative !== undefined)
      throw new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationExecutionError",
        `Upload paths must be absolute: ${relative}`,
      );
    if (await ServerBrowserPage.setInputFiles(tab.page, resolved, authorize)) return undefined;
    const open = tab.fileChooser;
    if (!open)
      throw new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationExecutionError",
        "No file picker is open. Click the page's upload control first, or pass the file input's locator.",
      );
    if (input.paths.length > 1 && !open.chooser.isMultiple())
      throw new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationExecutionError",
        "This file picker accepts one file.",
      );
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

  const openDownload: ServerBrowser["Service"]["openDownload"] = (input) =>
    Effect.gen(function* () {
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

  const nativeDownload = (
    resource: NativeBrowserDownloadAssetResource,
    authorize: Effect.Effect<void, OrchestratorMcpFailure>,
    offset?: number,
  ) =>
    Effect.gen(function* () {
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

  const preparedContexts = new WeakSet<BrowserContext>();
  const prepareContext = async (context: BrowserContext) => {
    if (preparedContexts.has(context)) return;
    preparedContexts.add(context);
    await context.grantPermissions(["clipboard-write"]);
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

  const takeSessionCopies = async (cdp: CDPSession) => {
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
              expression: "globalThis.takeCopy?.()",
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

  const takeCopiedText = async (tab: ServerTab) => {
    const copies = await Promise.all(
      [tab.cdp, ...tab.frameSessions.values()].map(takeSessionCopies),
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
    // A popup in a profile with a login publishes no URL.
    const url = profileLogins.has(opener.profileKey) ? "" : popup.url();

    const controller = [...opener.viewers].find(
      (viewer) => viewer.id === opener.control.controller,
    );
    await Effect.runPromise(
      manager.open({
        threadId: opener.threadId,
        ...(/^https?:/i.test(url) ? { url } : {}),
        runtime: "server",
        authorize,
        ...(opener.profileId === undefined ? {} : { profileId: opener.profileId }),
        // Agent popups stay with the agent and only float when it asks, like its own opens.
        ...(agentId === null ? {} : { automationOwner: agentId, reveal: false }),
        beforePublish: (snapshot) =>
          adoptedPages.set(tabKey(snapshot.threadId, snapshot.tabId), {
            page: popup,
            openerTabId: opener.tabId,
            profileKey: opener.profileKey,
          }),
      }),
    ).then(
      async (snapshot) => {
        // The opener stays open behind it, so `window.opener` can still report back.
        if (controller !== undefined) {
          try {
            await authorize();
            const { serverEpoch } = await Effect.runPromise(
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
    if (pending) return pending;
    const existing = tabs.get(key);
    if (existing) return Promise.resolve(existing);
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
    pendingTabs.set(key, opening);
    return opening;
  };

  const findTab = (threadId: string, tabId: string) =>
    Effect.gen(function* () {
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

  const atTabLimit = (agentSessionId: string) =>
    tabs.size + pendingTabs.size >= SERVER_TAB_LIMIT ||
    [...tabs.values()].filter((tab) => tab.control.agentId === agentSessionId).length >=
      AGENT_TAB_LIMIT;

  const assertTabCapacity = (agentSessionId: string) => {
    if (atTabLimit(agentSessionId))
      throw new BrowserControlInterrupted(
        `Too many server browser tabs are open (${AGENT_TAB_LIMIT} per agent session, ${SERVER_TAB_LIMIT} per server). Close one with t3_preview_close or reuse one from preview_status tabs.`,
        "tabLimit",
      );
  };

  const closeIdleAgentTabs = () => {
    const cutoff = Date.now() - AGENT_TAB_IDLE_MS;
    for (const tab of tabs.values()) {
      if (tab.control.agentId !== null && tab.viewers.size === 0 && tab.usedAt < cutoff)
        dropTab(tab, true);
    }
  };

  const latestThreadTab = (threadId: string, agentSessionId?: string) => {
    const threadTabs = [...tabs.values()].filter((tab) => tab.threadId === threadId);
    return (
      threadTabs
        .filter((tab) => tab.control.agentId === agentSessionId)
        .sort((left, right) => right.createdAt - left.createdAt)[0] ??
      threadTabs.sort(
        (left, right) =>
          Number(right.viewers.size > 0) - Number(left.viewers.size > 0) ||
          right.usedAt - left.usedAt,
      )[0]
    );
  };

  const tabOwner = (tab: ServerTab) =>
    tab.control.controller !== null
      ? ("human" as const)
      : tab.control.agentId !== null
        ? ("agent" as const)
        : ("unclaimed" as const);

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
  } | null = null;

  const reportProfiles: ServerBrowser["Service"]["reportProfiles"] = (input) =>
    Effect.sync(() => {
      const profiles = resolveBrowserProfiles(input.profiles);
      reportedProfiles = {
        profiles,
        defaultProfileId:
          findBrowserProfile(profiles, input.defaultProfileId)?.id ?? DEFAULT_BROWSER_PROFILE_ID,
      };
    });

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
    throw new ServerBrowserPage.ServerBrowserOperationError(
      "PreviewAutomationUnknownProfileError",
      `No browser profile is named "${requested}". Use one of: ${profiles
        .map((profile) => `${profile.name} (id ${profile.id})`)
        .join(", ")}.`,
    );
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
    const url = tab.page.url();
    const viewport = tab.page.viewportSize();
    const { serverEpoch } = await Effect.runPromise(manager.list({ threadId: tab.threadId }));
    const resource = (candidate: ServerTab): PreviewViewerResource => ({
      type: "preview",
      environmentId,
      threadId: candidate.threadId,
      tabId: candidate.tabId,
      serverEpoch,
    });
    const status = {
      resource: resource(tab),
      available: true,
      visible: tab.viewers.size > 0,
      tabId: tab.tabId,
      url: url === "about:blank" ? null : url,
      title: null,
      loading: tab.loading,
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
          url: candidate.page.url() === "about:blank" ? null : candidate.page.url(),
          ...(candidate.openerTabId === undefined ? {} : { openerTabId: candidate.openerTabId }),
          owner: tabOwner(candidate),
          ownedByCaller: candidate.control.agentId === agentSessionId,
          visible: candidate.viewers.size > 0,
          ...(candidate.profileId === undefined ? {} : { profileId: candidate.profileId }),
          ...(inLogin(candidate) ? { url: null, loginInProgress: true } : {}),
        })),
      ...profileStatus(),
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
    if (inLogin(tab))
      return {
        ...status,
        url: null,
        dialog: status.dialog === null ? null : { type: "", message: "", defaultValue: "" },
        fileChooser: null,
        downloads: [],
        loginInProgress: true,
      };
    if (status.url === null || tab.dialog) return status;
    return { ...status, title: (await tab.page.title().catch(() => "")) || null };
  };

  const resolveNavigationUrl = (input: PreviewAutomationNavigateInput) => {
    if (input.url !== undefined) return normalizePreviewUrl(input.url);
    const target = input.target!;
    if (target.kind === "url") return normalizePreviewUrl(target.url);

    const path = target.path ?? "";
    return `${target.protocol ?? "http"}://localhost:${target.port}${path.startsWith("/") || path === "" ? path : `/${path}`}`;
  };

  const navigate = async (
    tab: ServerTab,
    url: string,
    readiness: "load" | "domContentLoaded" | "none",
    timeout: number,
  ) => {
    const navigation = tab.page.goto(url, {
      timeout,
      waitUntil:
        readiness === "domContentLoaded"
          ? "domcontentloaded"
          : readiness === "none"
            ? "commit"
            : "load",
    });
    if (readiness === "none") {
      tab.control.track(navigation);
      return;
    }
    await navigation.catch((cause: unknown) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      if (message.includes("Download is starting")) {
        throw new ServerBrowserPage.ServerBrowserOperationError(
          "PreviewAutomationExecutionError",
          `${url} is a file this browser cannot show, so it was downloaded instead. preview_status lists it under downloads.`,
        );
      }
      if (/ERR_[A-Z_]+/.test(message)) {
        throw new ServerBrowserPage.ServerBrowserOperationError(
          "PreviewAutomationExecutionError",
          `Navigation to ${url} failed: ${ServerBrowserPage.parseNetError(message).description}`,
        );
      }
      throw cause;
    });
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
        await encoder.evaluate(ServerBrowserPage.RECORDING_ENCODER_SCRIPT);
        // Seed idle pages before returning so an immediate stop has a frame to encode.
        const firstFrame = await ServerBrowserPage.captureViewport(
          tab.page,
          tab.cdp,
          {
            format: "jpeg",
            quality: RECORDING_SCREENCAST.quality,
            scale: 1,
          },
          authorize,
        );
        await authorize();
        await encoder.evaluate(
          ([data, width]) =>
            (globalThis as unknown as EncoderWindow).__t3Recorder.frame(data, width),
          [firstFrame, tab.page.viewportSize()?.width ?? UNATTACHED_FILL_VIEWPORT.width] as const,
        );
        await authorize();
        const opened = await tab.page.context().newCDPSession(tab.page);
        session = opened;
        const framesInFlight = new Set<Promise<void>>();
        opened.on("Page.screencastFrame", (frame) => {
          const cssWidth = tab.page.viewportSize()?.width ?? frame.metadata.deviceWidth;
          const delivered: Promise<void> = resourceAuthorization()
            .then(() =>
              encoder.evaluate(
                ([data, width]) =>
                  (globalThis as unknown as EncoderWindow).__t3Recorder.frame(data, width),
                [frame.data, cssWidth] as const,
              ),
            )
            .then(async (accepted) => {
              if (!accepted) await opened.send("Page.stopScreencast");
            })
            .catch(async () => {
              // Stop a retired producer; cleanup never consumes more private media.
              await opened.send("Page.stopScreencast").catch(constVoid);
            })
            .finally(() => {
              framesInFlight.delete(delivered);
              void opened
                .send("Page.screencastFrameAck", { sessionId: frame.sessionId })
                .catch(constVoid);
            });
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
        throw new ServerBrowserPage.ServerBrowserOperationError(
          "PreviewAutomationRecordingNotActiveError",
          "No recording is active for this tab.",
        );
      }
      let mimeType: string | null;
      const chunks: Array<Buffer> = [];
      try {
        await recording.authorize();
        await recording.session.send("Page.stopScreencast").catch(constVoid);
        await Promise.all(recording.framesInFlight);
        await recording.session.detach().catch(constVoid);
        await authorize();
        await recording.authorize();
        const stopped = await recording.encoder.evaluate(() =>
          (globalThis as unknown as EncoderWindow).__t3Recorder.stop(),
        );
        mimeType = stopped.mimeType;
        // Checked before the transfer so an oversized video never lands in this process.
        if (stopped.bytes > PROVIDER_SEND_TURN_MAX_FILE_BYTES) {
          throw new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationRecordingTooLargeError",
            "The recording is larger than the attachment limit.",
          );
        }
        for (let index = 0; index < stopped.count; index += 1) {
          await authorize();
          await recording.authorize();
          const chunk = await recording.encoder.evaluate(
            (chunkIndex) => (globalThis as unknown as EncoderWindow).__t3Recorder.chunk(chunkIndex),
            index,
          );
          chunks.push(Buffer.from(chunk, "base64"));
        }
      } finally {
        await recording.encoder.close().catch(constVoid);
        tab.recording = null;
      }
      const data = Buffer.concat(chunks);
      if (!mimeType || data.byteLength === 0) {
        throw new ServerBrowserPage.ServerBrowserOperationError(
          "PreviewAutomationExecutionError",
          "The recording captured no frames.",
        );
      }

      const extension = mimeType.startsWith("video/mp4") ? "mp4" : "webm";
      const pendingId = `${PENDING_ATTACHMENT_THREAD_SEGMENT}-${NodeCrypto.randomUUID()}-${extension}`;
      const path = NodePath.join(config.attachmentsDir, `${pendingId}.${extension}`);
      await authorize();
      await recording.authorize();
      await NodeFSP.mkdir(config.attachmentsDir, { recursive: true });
      await authorize();
      await recording.authorize();
      await NodeFSP.writeFile(path, data);
      return {
        id: pendingId,
        tabId: tab.tabId,
        path,
        mimeType: mimeType.split(";")[0]!,
        sizeBytes: data.byteLength,
        createdAt: new Date().toISOString(),
        uploadedAttachmentId: pendingId,
      };
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
      throw new BrowserControlInterrupted("The agent session is missing. Reconnect the provider.");
    const owned = [...tabs.values()].filter(
      (tab) => tab.threadId === request.threadId && tab.control.agentId === request.agentSessionId,
    );
    if (!request.tabIdExplicit && owned.length > 1)
      throw new BrowserControlInterrupted(
        "Multiple tabs belong to this agent session. Pass a tabId from preview_open or preview_status tabs.",
        "tabRequired",
      );
    const tab =
      request.tabId === undefined
        ? latestThreadTab(request.threadId, request.agentSessionId)
        : await Effect.runPromise(
            findTab(request.threadId, request.tabId).pipe(Effect.orElseSucceed(constVoid)),
          );
    if (!tab) {
      throw new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationTabNotFoundError",
        "No server preview tab is open for this thread. Call preview_open first.",
      );
    }
    if (!READ_OPERATIONS.has(request.operation) && !tab.control.agentMayAct(request.agentSessionId))
      throw new BrowserControlInterrupted(
        "This tab belongs to another agent session. You can read it, but open your own tab to act.",
        "agentMismatch",
      );
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

  const runOperation = async (
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
  ): Promise<unknown> => {
    await authorize();
    const input = request.input;
    switch (request.operation) {
      case "status":
        return statusWithTitle(
          request.tabId === undefined
            ? latestThreadTab(request.threadId, request.agentSessionId)
            : tabs.get(tabKey(request.threadId, request.tabId)),
          request.agentSessionId,
        );
      case "open": {
        if (!request.agentSessionId)
          throw new BrowserControlInterrupted(
            "The agent session is missing. Reconnect the provider.",
          );
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
          throw new BrowserControlInterrupted(
            "Multiple tabs are open. Pass tabId or reuseExistingTab=false.",
            "tabRequired",
          );

        const found =
          reuse && request.tabId !== undefined
            ? await Effect.runPromise(
                findTab(request.threadId, request.tabId).pipe(
                  Effect.catchTags({
                    ServerBrowserTabNotFoundError: () => Effect.succeed(undefined),
                  }),
                ),
              )
            : undefined;
        // Only an explicit tabId reuses a tab this session did not open; a tab
        // it last read (such as the user's) is not taken over implicitly.
        const existing =
          found && (request.tabIdExplicit || found.control.agentId === request.agentSessionId)
            ? found
            : undefined;
        const navigationTimeout = Math.min(request.timeoutMs, NAVIGATION_TIMEOUT_MS);
        const profileId = existing ? undefined : resolveOpenProfile(open.profileId);
        if (!existing) {
          await authorize();
          closeIdleAgentTabs();
          assertTabCapacity(request.agentSessionId);
        }
        const tab =
          existing ??
          (await ensureTab(
            await Effect.runPromise(
              manager.open({
                threadId: request.threadId,
                ...(url ? { url } : {}),
                ...(profileId === undefined ? {} : { profileId }),
                runtime: "server",
                reveal: false,
                automationOwner: request.agentSessionId,
                authorize: resourceAuthorization,
              }),
            ),
            resourceAuthorization,
          ));
        if (existing && inLogin(existing)) throw loginInProgress();
        if (existing?.dialog)
          throw new BrowserControlInterrupted(
            "A browser dialog is pending. Read preview_status and use preview_dialog first.",
            "dialogPending",
          );
        return tab.control.agent(
          request.agentSessionId,
          async () => {
            if (tab.dialog)
              throw new BrowserControlInterrupted(
                "A browser dialog is pending. Read preview_status and use preview_dialog first.",
                "dialogPending",
              );
            if (existing) {
              if (url) await navigate(tab, url, "load", navigationTimeout);
            } else {
              // Await the original navigation failure even though background creation keeps the tab.
              await tab.initialNavigation;
            }
            const reveal = open.open ?? open.show;
            if (reveal !== false) {
              await authorize();
              await Effect.runPromise(
                manager.requestReveal({
                  threadId: tab.threadId,
                  tabId: tab.tabId,
                  force: reveal === true,
                }),
              );
            }
            if (!existing && url) {
              await tab.page
                .waitForLoadState("load", { timeout: navigationTimeout })
                .catch(constVoid);
            }
            return statusWithTitle(tab, request.agentSessionId);
          },
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
          throw new BrowserControlInterrupted(
            "Multiple tabs belong to this agent session. Pass a tabId from preview_open or preview_status tabs.",
            "tabRequired",
          );
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
          throw new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationRecordingNotActiveError",
            "No recording is active for this thread.",
          );
        }
        return tab.control.agent(
          request.agentSessionId ?? "",
          () => stopRecording(tab, authorize),
          authorize,
        );
      }
    }
    const tab = await requireTab(request);
    // Closing must unblock an action waiting on a dialog, without queueing behind it.
    if (request.operation === "close") {
      await authorize();
      if (tab.control.agentId !== request.agentSessionId)
        throw new BrowserControlInterrupted(
          "Only the agent session that opened this tab can close it.",
          "agentMismatch",
        );
      if (tab.control.controller !== null)
        throw new BrowserControlInterrupted("A human controls this tab.", "humanControl");
      void tab.control.close().catch(constVoid);
      await Effect.runPromise(manager.close({ threadId: tab.threadId, tabId: tab.tabId }));
      dropTab(tab, false);
      return {};
    }
    if (request.operation === "endLogin") {
      const login = profileLogins.get(tab.profileKey);
      if (login === undefined) return { closed: [] };
      const members = profileTabs(tab.profileKey);
      if (members.some((member) => member.control.controller !== null))
        throw new BrowserControlInterrupted(
          "A human controls a tab in this login.",
          "humanControl",
        );
      // The whole context closes, and the login lifts only once that close has
      // finished: every page and every tab still attaching goes with it. Cookies
      // with an expiry stay in the profile directory for the next context.
      login.ending = true;
      for (const member of members) {
        void member.control.close().catch(constVoid);
        await Effect.runPromise(
          manager.close({ threadId: member.threadId, tabId: member.tabId }).pipe(Effect.ignore),
        );
      }
      await contexts.closeContext(login.context);
      endLoginsOf(login.context);
      return { closed: members.map((member) => member.tabId) };
    }
    // A click can be waiting for its dialog. Resolve it outside the serial queue,
    // with the same owner check, so the operation can finish and control can drain.
    if (request.operation === "dialog") {
      await authorize();
      if (tab.control.controller !== null)
        throw new BrowserControlInterrupted("A human controls this tab.", "humanControl");
      await resolveDialog(tab, input as PreviewAutomationDialogInput);
      return statusWithTitle(tab, request.agentSessionId);
    }
    const agentSessionId = request.agentSessionId!;

    if (
      READ_OPERATIONS.has(request.operation) &&
      tab.control.agentId !== agentSessionId &&
      !tab.control.agentCanActNow(agentSessionId)
    ) {
      return tab.control.observe(async () => {
        await authorize();
        if (tab.dialog)
          throw new BrowserControlInterrupted(
            "A browser dialog is pending. Read preview_status.",
            "dialogPending",
          );
        return executeTabOperation(tab, request, authorize, resourceAuthorization);
      });
    }
    return tab.control.agent(
      agentSessionId,
      async () => {
        if (tab.dialog)
          throw new BrowserControlInterrupted(
            "A browser dialog is pending. Read preview_status and use preview_dialog first.",
            "dialogPending",
          );
        const generation = tab.control.generation;
        try {
          return await executeTabOperation(tab, request, authorize, resourceAuthorization);
        } finally {
          if (generation !== tab.control.generation) ServerBrowserPage.invalidateRefs(tab.page);
        }
      },
      authorize,
    );
  };

  // Operations running inside a tab's queue. A fill waits for its profile's before it
  // types, so nothing that began before the login can see the value.
  const running = new Map<ServerTab, Set<Promise<unknown>>>();

  const executeTabOperation = async (
    tab: ServerTab,
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
  ) => {
    await authorize();
    // Checked inside the tab's queue, so it holds for what this operation does.
    if (inLogin(tab) && !LOGIN_OPERATIONS.has(request.operation)) throw loginInProgress();
    if (
      inLogin(tab) &&
      request.operation === "click" &&
      (request.input as PreviewAutomationClickInput).locator === undefined &&
      (request.input as PreviewAutomationClickInput).selector === undefined
    )
      throw loginInProgress();
    const operation = runTabOperation(tab, request, authorize, resourceAuthorization).catch((cause: unknown) => {
      // Playwright's errors can quote the page; a login tab reports only what failed.
      if (!inLogin(tab) || cause instanceof BrowserControlInterrupted) throw cause;
      const error = ServerBrowserPage.toOperationError(cause);
      if (error.tag === "PreviewCredentialError") throw error;
      throw new ServerBrowserPage.ServerBrowserOperationError(
        error.tag,
        `Preview automation ${request.operation} failed in a login tab.`,
      );
    });
    if (request.operation !== "fillCredential") {
      const set = running.get(tab) ?? new Set();
      running.set(tab, set);
      set.add(operation);
      void operation.finally(() => set.delete(operation)).catch(constVoid);
    }
    return operation;
  };

  /**
   * Starts a login on the tab's whole server browser profile as the fill starts,
   * before the vault is read, or joins the one it has. Nothing lifts it but
   * preview_end_login or the profile's tabs closing, not even a failed fill.
   */
  const fillInLogin = async (tab: ServerTab, input: PreviewAutomationFillCredentialInput) => {
    if (tab.desktop) throw serverBrowserOnly();
    if (profileLogins.get(tab.profileKey)?.ending) throw loginInProgress();
    const recordingIn = (members: ReadonlyArray<ServerTab>) =>
      members.some((member) => member.recording || member.recordingStart);
    const recordingRunning = () =>
      new ServerBrowserPage.ServerBrowserOperationError(
        "PreviewAutomationExecutionError",
        "A recording is running in this browser profile. Stop it before filling a credential.",
      );
    if (recordingIn(profileTabs(tab.profileKey))) throw recordingRunning();
    if (!profileLogins.has(tab.profileKey)) {
      profileLogins.set(tab.profileKey, { ending: false, context: tab.page.context() });
      loginEpoch++;
      // What every tab of the profile already showed is replaced at once.
      for (const member of profileTabs(tab.profileKey)) report(member, { _tag: "Idle" });
    }
    return recordAction(tab, "fillCredential", () =>
      fillCredential({
        tabId: tab.tabId,
        vault,
        request: input,
        locate: (locator) => ServerBrowserPage.targetLocator(tab.page, { locator })!,
        beforeTyping: async () => {
          const members = profileTabs(tab.profileKey);
          await Promise.allSettled(members.flatMap((member) => [...(running.get(member) ?? [])]));
          if (recordingIn(members)) throw recordingRunning();
        },
      }),
    );
  };

  const runTabOperation = async (
    tab: ServerTab,
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
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
          throw new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationExecutionError",
            "Select an existing tab in the requested persistent browser profile.",
          );
        const response = await tab.page.goto(normalizePreviewUrl(read.url), {
          waitUntil: "domcontentloaded",
          timeout: request.timeoutMs,
        });
        // A human may reserve control while navigation is in flight.
        if (!tab.control.agentCanActNow(request.agentSessionId!))
          throw new BrowserControlInterrupted("Browser control changed during the read.");
        const html = await tab.page.content();
        if (
          generation !== tab.control.generation ||
          !tab.control.agentCanActNow(request.agentSessionId!)
        )
          throw new BrowserControlInterrupted("Browser control changed during the read.");
        if (Buffer.byteLength(html, "utf8") > 4 * 1024 * 1024)
          throw new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationResultTooLargeError",
            "The session page exceeds the research extraction limit.",
          );
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
        await Effect.runPromise(
          manager.resize(
            { threadId: tab.threadId, tabId: tab.tabId, viewport: setting },
            resourceAuthorization,
          ),
        );
        await authorize();
        await applySetting(tab, setting, authorize);
        return {
          tabId: tab.tabId,
          setting,
          viewport: tab.page.viewportSize() ?? UNATTACHED_FILL_VIEWPORT,
        };
      }
      case "setColorScheme": {
        const { colorScheme } = input as PreviewAutomationSetColorSchemeInput;
        const snapshot = await Effect.runPromise(
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
        return withScreencastsPaused(
          tab,
          () => ServerBrowserPage.snapshot({ ...tab, renderScale: RENDER_SCALE }, authorize),
          authorize,
        );
      }
      case "click": {
        const clickInput = input as PreviewAutomationClickInput;
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
      case "type":
        return recordAction(tab, "type", () =>
          ServerBrowserPage.type(tab.page, input as PreviewAutomationTypeInput, authorize),
        );
      case "press":
        return recordAction(tab, "press", () =>
          ServerBrowserPage.press(tab.page, input as PreviewAutomationPressInput, authorize),
        );
      case "scroll":
        return recordAction(tab, "scroll", () =>
          ServerBrowserPage.scroll(tab.page, input as PreviewAutomationScrollInput, authorize),
        );
      case "fillCredential":
        return fillInLogin(tab, input as PreviewAutomationFillCredentialInput);
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

  /**
   * Checks a result once more as it leaves: a read that began before a fill, such
   * as status, which runs outside the tab's queue, cannot return what the fill put
   * in the page. Status-shaped results are taken again; other reads of a tab now in
   * a login are refused.
   */
  const checkedOperation = async (
    request: PreviewAutomationRequest,
    authorize: () => Promise<void>,
    resourceAuthorization: () => Promise<void>,
  ) => {
    let epoch = loginEpoch;
    let result = await runOperation(request, authorize, resourceAuthorization);
    while (epoch !== loginEpoch) {
      epoch = loginEpoch;
      if (STATUS_RESULTS.has(request.operation)) {
        const tabId =
          typeof result === "object" && result !== null && "tabId" in result
            ? (result.tabId as string | null)
            : null;
        result = await statusWithTitle(
          tabId === null ? undefined : tabs.get(tabKey(request.threadId, tabId)),
          request.agentSessionId,
        );
        continue;
      }
      if (CONTENT_FREE_RESULTS.has(request.operation)) break;
      const target =
        request.tabId === undefined
          ? latestThreadTab(request.threadId, request.agentSessionId)
          : tabs.get(tabKey(request.threadId, request.tabId));
      if (target && inLogin(target)) throw loginInProgress();
    }
    return result;
  };

  const handleRequest = (connectionId: string, request: PreviewAutomationRequest) =>
    Effect.gen(function* () {
      if (broker.requestAuthorization === undefined)
        return yield* Effect.fail(
          new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationUnsupportedClientError",
            "Current preview invocation checks are unsupported.",
          ),
        );
      const authorization = yield* broker
        .requestAuthorization({
          clientId: SERVER_HOST_CLIENT_ID,
          connectionId,
          requestId: request.requestId,
        })
        .pipe(Effect.mapError(ServerBrowserPage.toOperationError));
      return yield* Effect.tryPromise({
        try: () =>
          checkedOperation(
            request,
            () => Effect.runPromise(authorization.dispatch),
            () => Effect.runPromise(authorization.resource),
          ).finally(() => markUsed(request)),
        catch: ServerBrowserPage.toOperationError,
      });
    }).pipe(
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
    );

  const mirrorManagerEvent = ({ event, authorize = async () => {} }: PreviewManager.ServerEvent) =>
    Effect.promise(async () => {
      if (event.type === "opened" && event.snapshot.runtime === "server") {
        await ensureTab(event.snapshot, authorize).catch(constVoid);
        return;
      }
      const key = tabKey(event.threadId, event.tabId);
      const tab = tabs.get(key);
      // A closed session is a closed page; a detached desktop page is not.
      if (event.type === "closed" && !tab && pendingTabs.has(key)) closedPendingTabs.add(key);
      if (event.type === "closed") tabAuthorizations.delete(key);
      if (!tab) return;
      if (event.type === "closed") {
        closedSessions.add(key);
        dropTab(tab, false);
        closedSessions.delete(key);
        return;
      }
      if (event.type === "resized") {
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
            if (request?.hardReload) await tab.cdp.send("Page.reload", { ignoreCache: true });
          })
          .catch((cause: unknown) =>
            runFork(Effect.logWarning("server preview could not apply a tab setting", { cause })),
          );
      }
    });

  const releaseViewerInput = async (viewer: ViewerState, session: CDPSession) => {
    for (const { key, code } of viewer.pressedKeys.values()) {
      await session.send("Input.dispatchKeyEvent", { type: "keyUp", key, code }).catch(constVoid);
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
    viewer.requestedSize = null;
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
        const type =
          action === "down" ? "mousePressed" : action === "up" ? "mouseReleased" : "mouseMoved";
        const button = ["none", "left", "middle", "right"].includes(String(message.button))
          ? (message.button as "none" | "left" | "middle" | "right")
          : "none";
        await session.send("Input.dispatchMouseEvent", {
          type,
          x: num(message.x),
          y: num(message.y),
          button,
          buttons: num(message.buttons),
          clickCount: num(message.clickCount, type === "mouseMoved" ? 0 : 1),
          modifiers,
        });
        if (button !== "none") {
          if (action === "down")
            viewer.pressedButtons.set(button, { x: num(message.x), y: num(message.y) });
          else if (action === "up") viewer.pressedButtons.delete(button);
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
          await session.send("Input.dispatchKeyEvent", { type: "keyUp", key, code, modifiers });
          viewer.pressedKeys.delete(code || key);
          return;
        }
        const command = editingCommand(key, modifiers);
        const copying =
          !tab.desktop && (command?.commands[0] === "copy" || command?.commands[0] === "cut");

        if (copying) await takeCopiedText(tab);
        await authorize();
        await session.send("Input.dispatchKeyEvent", {
          type: text ? "keyDown" : "rawKeyDown",
          key,
          code,
          modifiers,
          ...(text ? { text, unmodifiedText: text } : {}),
          ...command,
          windowsVirtualKeyCode: num(
            message.keyCode,
            key.length === 1 ? key.toUpperCase().charCodeAt(0) : 0,
          ),
        });
        viewer.pressedKeys.set(code || key, { key, code });
        if (copying) {
          await authorize();
          const copied = await takeCopiedText(tab);
          await authorize();
          if (copied) viewer.push({ _tag: "clipboard", text: copied });
        }
        return;
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
        const current = tab.page.viewportSize();
        if (current?.width === width && current.height === height) return;
        await tab.page.setViewportSize({ width, height });
        broadcastViewport(tab);
        return;
      }
      case "viewport": {
        const setting = decodeViewportSetting(message.setting);
        await Effect.runPromise(
          manager.resize(
            { threadId: tab.threadId, tabId: tab.tabId, viewport: setting },
            authorize,
          ),
        );
        await applySetting(tab, setting, authorize);
        return;
      }
      case "navigate":
        if (typeof message.url === "string") {
          const url = normalizePreviewUrl(message.url);
          await tab.page.goto(url, VIEWER_NAVIGATION_OPTIONS);
        }
        return;
      case "history":
        await (num(message.delta) < 0
          ? tab.page.goBack(VIEWER_NAVIGATION_OPTIONS)
          : tab.page.goForward(VIEWER_NAVIGATION_OPTIONS));
        return;
      case "reload":
        if (message.ignoreCache === true) {
          await session.send("Page.reload", { ignoreCache: true });
          return;
        }
        await tab.page.reload(VIEWER_NAVIGATION_OPTIONS);
        return;
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

  const attachViewer: ServerBrowser["Service"]["attachViewer"] = (input) =>
    Effect.gen(function* () {
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
        push: (next) => {
          // Dropped frames must still release Chromium.
          if (Queue.offerUnsafe(output, next)) return;
          if (next._tag === "frame") runFork(next.ack);
          // State a stalled viewer cannot miss replaces its backlog. It runs
          // synchronously so an older replacement can never land after a newer one.
          else if (next._tag === "gone" || next._tag === "control" || next._tag === "fileChooser") {
            const dropped = Effect.runSyncExit(Queue.clear(output));
            if (dropped._tag === "Failure") return;
            Queue.offerUnsafe(output, next);

            const chooser = next._tag === "control" ? fileChooserMessage(tab) : null;
            if (chooser && tab.control.controller === viewer.id) Queue.offerUnsafe(output, chooser);
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
        requestedSize: null,
      };
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          tab.viewers.add(viewer);
          reportLiveTabs();
        }),
        () =>
          Effect.promise(async () => {
            await tab.control.disconnect(viewer.id, () => releaseViewerInput(viewer, session));
            tab.viewers.delete(viewer);
            broadcastControl(tab);
            reportLiveTabs();
          }),
      );
      if (input.canOperate && tab.control.agentId === null && tab.control.controller === null) {
        yield* Effect.tryPromise({
          try: () => tab.control.take(viewer.id, authorize),
          catch: (cause) => new ServerBrowserLaunchError({ cause }),
        });
      }
      broadcastControl(tab);
      pushFileChooser(tab);
      // Full scale: a scaled capture would flash in every other viewer.
      const pushStill = async () => {
        const data = await withCaptureLock(tab, () =>
          ServerBrowserPage.captureViewport(
            tab.page,
            session,
            {
              format: "jpeg",
              quality,
              scale: 1,
            },
            authorize,
          ),
        ).catch(() => null);
        if (data)
          viewer.push({ _tag: "frame", data: Buffer.from(data, "base64"), ack: Effect.void });
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
      if (!screencastStarted) yield* Effect.promise(pushStill);
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
        if (release === null) return yield* Effect.fail(new BrowserInputOverloaded({}));
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
              await tab.control.human(
                viewer.id,
                () => dispatchViewerInput(tab, session, viewer, message, authorize),
                async () => {
                  await authorize();
                  assertCurrentInput();
                  await authorize();
                },
              );
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
            } else if (
              message.type === "dialog" &&
              tab.control.controller === viewer.id &&
              typeof message.accept === "boolean"
            ) {
              await authorize();
              assertCurrentInput();
              await resolveDialog(tab, {
                accept: message.accept,
                ...(typeof message.promptText === "string"
                  ? { promptText: message.promptText }
                  : {}),
              });
            } else {
              await tab.control.human(
                viewer.id,
                () => dispatchViewerInput(tab, session, viewer, message, authorize),
                authorize,
              );
            }
          } catch {
            broadcastControl(tab);
          } finally {
            release();
          }
        })();
        return Effect.promise(() => work);
      });
      return {
        output,
        input: (raw: unknown) => prepareInput(raw).pipe(Effect.flatMap((work) => work)),
        enqueueInput: (raw: unknown) =>
          prepareInput(raw).pipe(
            Effect.flatMap((work) => work.pipe(Effect.forkScoped, Effect.asVoid)),
          ),
      } satisfies ServerBrowserViewer;
    });

  yield* manager.serverEvents.pipe(Stream.runForEach(mirrorManagerEvent), Effect.forkScoped);
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
  // The page came back, for example after its DevTools closed. Reconnect now
  // rather than on the next viewer or agent, or the tab's URL and title stop
  // reaching every client in the meantime.
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

  return ServerBrowser.of({
    mirrorResets: Stream.fromPubSub(mirrorResets),
    mirrorCookies: (batch) =>
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
    authorizeViewer: (resource) =>
      Effect.gen(function* () {
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
    reportProfiles,
    openDownload,
    inspectDownload: (resource, authorize) =>
      nativeDownload(resource, authorize).pipe(Effect.asVoid),
    readDownloadBlock: (resource, offset, authorize) => nativeDownload(resource, authorize, offset),
    answerFileChooser,
  });
});

export const layer = Layer.effect(ServerBrowser, make);
