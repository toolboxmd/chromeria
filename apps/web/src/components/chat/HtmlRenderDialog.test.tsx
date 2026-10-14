// @vitest-environment jsdom

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { htmlRenderTheme, HTML_RENDER_DEFAULT_FONTS } from "@t3tools/shared/htmlRender";
import { T3_CODE_DARK_THEME_COLORS } from "@t3tools/shared/themePalettes";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";

import { readLocalApi } from "~/localApi";
import { selectActiveRightPanelSurface, useRightPanelStore } from "~/rightPanelStore";

import { HtmlRenderDialog } from "./HtmlRenderDialog";
import { HtmlRenderFrame } from "./HtmlRenderFrame";

const {
  cachedUrl,
  refresh,
  showError,
  openLink,
  openChangeRequestLink,
  sourceControlRead,
  linkEnvironment,
  navigate,
} = vi.hoisted(() => ({
  cachedUrl: { lifeMs: 60 * 60_000 },
  refresh: vi.fn(),
  showError: vi.fn(),
  openLink: vi.fn(),
  openChangeRequestLink: vi.fn(),
  // The forge read behind an Issue link. A result here is not a live GitHub response.
  sourceControlRead: vi.fn(async () => ({ _tag: "Failure" as const })),
  linkEnvironment: {
    configs: new Map<
      string,
      {
        environment: {
          capabilities: { issues: boolean; pullRequests: boolean; threadPullRequests: boolean };
        };
      }
    >(),
    projects: [] as ReadonlyArray<unknown>,
  },
  navigate: vi.fn(),
}));
vi.mock("~/browser/useOpenLink", () => ({ useOpenLink: () => openLink }));
vi.mock("~/lib/openPullRequestLink", async (importOriginal) => {
  const actual = await importOriginal<typeof import("~/lib/openPullRequestLink")>();
  return { ...actual, useOpenChangeRequestLink: () => openChangeRequestLink };
});
vi.mock("~/state/entities", async (importOriginal) => {
  const actual = await importOriginal<typeof import("~/state/entities")>();
  return {
    ...actual,
    useProjects: () => linkEnvironment.projects,
    useServerConfigs: () => linkEnvironment.configs,
  };
});
vi.mock("~/state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => sourceControlRead,
}));
vi.mock("@tanstack/react-router", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@tanstack/react-router")>();
  return { ...actual, useNavigate: () => navigate };
});
vi.mock("~/assets/assetUrls", () => ({
  useAssetUrlState: () => ({
    _tag: "Success",
    url: "https://environment.test/original.html",
    expiresAt: Date.now() + cachedUrl.lifeMs,
  }),
  useAssetUrlRefresh: () => refresh,
}));
vi.mock("../ui/toast", () => ({
  toastManager: { add: showError },
  stackedThreadToast: (toast: unknown) => toast,
}));

vi.mock("~/hooks/useHtmlRenderTheme", () => ({
  useHtmlRenderTheme: () =>
    htmlRenderTheme(T3_CODE_DARK_THEME_COLORS, "dark", HTML_RENDER_DEFAULT_FONTS),
}));

let root: Root;
let container: HTMLDivElement;
let shellOpen: ReturnType<typeof vi.spyOn>;
const onClose = vi.fn();
const capableEnvironmentId = EnvironmentId.make("environment");
const otherEnvironmentId = EnvironmentId.make("other-environment");
const threadId = ThreadId.make("thread");
const issueUrl = "https://github.com/toolboxmd/t3code/issues/40";

function issuesConfig(issues: boolean) {
  return {
    environment: {
      capabilities: { issues, pullRequests: false, threadPullRequests: false },
    },
  };
}

function threadOn(environmentId: typeof capableEnvironmentId) {
  return { environmentId, threadId };
}

function activeIssue(environmentId: typeof capableEnvironmentId) {
  return selectActiveRightPanelSurface(
    useRightPanelStore.getState().byThreadKey,
    threadOn(environmentId),
  );
}

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  Object.defineProperty(navigator, "userActivation", {
    configurable: true,
    value: { isActive: true },
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
      unobserve() {}
    },
  );
  refresh.mockReset();
  cachedUrl.lifeMs = 60 * 60_000;
  openLink.mockReset().mockResolvedValue(undefined);
  openChangeRequestLink.mockReset().mockReturnValue(false);
  sourceControlRead.mockReset().mockResolvedValue({ _tag: "Failure" });
  navigate.mockClear();
  showError.mockClear();
  onClose.mockClear();
  linkEnvironment.configs = new Map();
  linkEnvironment.projects = [];
  useRightPanelStore.setState({ byThreadKey: {} });
  shellOpen = vi.spyOn(readLocalApi()!.shell, "openExternal").mockResolvedValue(undefined);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(() =>
    root.render(
      <HtmlRenderDialog
        src="https://environment.test/page.html"
        title="Diagram"
        onClose={onClose}
      />,
    ),
  );
});

afterEach(async () => {
  shellOpen.mockRestore();
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function click(label: string) {
  const button = document.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
  expect(button).not.toBeNull();
  await act(() => button!.click());
}

it("zooms the loaded page within bounds and resets without reloading it", async () => {
  const frame = document.querySelector("iframe")!;
  const src = frame.src;
  for (let step = 0; step < 6; step += 1) await click("Zoom in");
  expect(document.querySelector<HTMLButtonElement>('[aria-label="Zoom in"]')!.disabled).toBe(true);
  expect(frame.parentElement!.style.getPropertyValue("zoom")).toBe("2");
  for (let step = 0; step < 8; step += 1) await click("Zoom out");
  expect(document.querySelector<HTMLButtonElement>('[aria-label="Zoom out"]')!.disabled).toBe(true);
  expect(frame.parentElement!.style.getPropertyValue("zoom")).toBe("0.5");
  await click("Reset zoom");
  expect(frame.parentElement!.style.getPropertyValue("zoom")).toBe("1");
  expect(document.querySelector("iframe")).toBe(frame);
  expect(frame.src).toBe(src);
  expect(frame.getAttribute("sandbox")).toBe("allow-scripts allow-forms");
});

it("closes with the close button or Escape", async () => {
  await click("Close");
  expect(onClose).toHaveBeenCalledOnce();
  onClose.mockClear();
  await act(() => {
    document.activeElement?.dispatchEvent(
      new KeyboardEvent("keydown", { key: "Escape", bubbles: true }),
    );
  });
  expect(onClose).toHaveBeenCalledOnce();
});

async function renderInline(environmentId = capableEnvironmentId) {
  // Async so a URL minted at mount lands inside act.
  await act(async () =>
    root.render(
      <HtmlRenderFrame
        environmentId={environmentId}
        htmlRender={{ attachmentId: "html", title: "Diagram", height: 400 }}
        threadRef={threadOn(environmentId)}
      />,
    ),
  );
}

function linkMessage(url: string, modifiers?: { metaKey?: boolean; ctrlKey?: boolean }) {
  return {
    jsonrpc: "2.0",
    id: "link",
    method: "ui/open-link",
    params: {
      url,
      metaKey: modifiers?.metaKey === true,
      ctrlKey: modifiers?.ctrlKey === true,
    },
  };
}

async function postLink(
  frame: HTMLIFrameElement,
  url: string,
  options?: { metaKey?: boolean; ctrlKey?: boolean; source?: MessageEventSource | null },
) {
  const readsBefore = sourceControlRead.mock.calls.length;
  await act(async () => {
    window.dispatchEvent(
      new MessageEvent("message", {
        source: options && "source" in options ? options.source : frame.contentWindow,
        data: linkMessage(url, options),
      }),
    );
    await Promise.all(
      sourceControlRead.mock.results.slice(readsBefore).map((result) => result.value),
    );
  });
}

async function expandedFrame() {
  refresh.mockResolvedValue("https://environment.test/renewed.html");
  await click("Open full screen");
  const frame = document.querySelector<HTMLIFrameElement>('[role="dialog"] iframe')!;
  frame.focus();
  return frame;
}

it("offers only full screen and opens with the inline URL while keeping the inline page loaded", async () => {
  await renderInline();
  expect(document.querySelector('[aria-label="Open in panel"]')).toBeNull();
  const inlineFrame = document.querySelector("iframe")!;
  const originalSrc = inlineFrame.src;
  const expandButton = document.querySelector<HTMLButtonElement>(
    '[aria-label="Open full screen"]',
  )!;
  expandButton.focus();
  await click("Open full screen");
  expect(refresh).not.toHaveBeenCalled();
  expect(document.querySelector('[role="dialog"] iframe')?.getAttribute("src")).toContain(
    "https://environment.test/original.html#",
  );
  expect(inlineFrame.src).toBe(originalSrc);
  await click("Close");
  expect(document.querySelector('[role="dialog"]')).toBeNull();
  expect(document.querySelector("iframe")).toBe(inlineFrame);
  expect(document.activeElement).toBe(expandButton);
});

// An inline URL near expiry is minted at mount, and full screen mints another.
async function renderInlineNearExpiry() {
  cachedUrl.lifeMs = 60_000;
  refresh.mockResolvedValueOnce("https://environment.test/inline.html");
  await renderInline();
  expect(refresh).toHaveBeenCalledOnce();
}

it("opens full screen with a fresh URL when the inline one is near expiry", async () => {
  await renderInlineNearExpiry();
  refresh.mockResolvedValue("https://environment.test/renewed.html");
  await click("Open full screen");
  expect(refresh).toHaveBeenCalledTimes(2);
  expect(document.querySelector('[role="dialog"] iframe')?.getAttribute("src")).toContain(
    "https://environment.test/renewed.html#",
  );
});

it("routes links from the expanded page through its thread", async () => {
  await renderInline();
  openChangeRequestLink.mockReturnValue(true);
  await click("Open full screen");
  const frame = document.querySelector<HTMLIFrameElement>('[role="dialog"] iframe')!;
  frame.focus();
  const url = "https://github.com/pingdotgg/t3code/pull/17561";
  await act(() => {
    window.dispatchEvent(
      new MessageEvent("message", {
        source: frame.contentWindow,
        data: { jsonrpc: "2.0", id: "link", method: "ui/open-link", params: { url } },
      }),
    );
  });
  expect(openChangeRequestLink).toHaveBeenCalledWith(
    expect.objectContaining({ metaKey: false, ctrlKey: false }),
    url,
    undefined,
    EnvironmentId.make("environment"),
  );
  expect(openLink).not.toHaveBeenCalled();
});

function allowIssues() {
  linkEnvironment.configs = new Map([
    [capableEnvironmentId, issuesConfig(true)],
    [otherEnvironmentId, issuesConfig(false)],
  ]);
}

it("opens an Issue once from the inline page and from the expanded page", async () => {
  allowIssues();
  sourceControlRead.mockResolvedValue({ _tag: "Success", value: {} });
  await renderInline();
  const inlineFrame = document.querySelector("iframe")!;
  inlineFrame.focus();
  await act(async () => {
    for (let time = 0; time < 2; time += 1) {
      window.dispatchEvent(
        new MessageEvent("message", {
          source: inlineFrame.contentWindow,
          data: linkMessage(issueUrl),
        }),
      );
    }
    await sourceControlRead.mock.results[0]?.value;
  });
  expect(sourceControlRead).toHaveBeenCalledTimes(1);
  expect(activeIssue(capableEnvironmentId)).toEqual({
    id: "issue",
    kind: "issue",
    environmentId: capableEnvironmentId,
    url: issueUrl,
  });
  expect(openLink).not.toHaveBeenCalled();
  expect(openChangeRequestLink).not.toHaveBeenCalled();
  expect(shellOpen).not.toHaveBeenCalled();
  expect(navigate).not.toHaveBeenCalled();

  const expandedIssue = "https://github.com/toolboxmd/t3code/issues/41";
  const dialogFrame = await expandedFrame();
  await postLink(dialogFrame, expandedIssue);
  expect(sourceControlRead).toHaveBeenCalledTimes(2);
  expect(activeIssue(capableEnvironmentId)).toEqual({
    id: "issue",
    kind: "issue",
    environmentId: capableEnvironmentId,
    url: expandedIssue,
  });
  expect(openLink).not.toHaveBeenCalled();
  expect(shellOpen).not.toHaveBeenCalled();
});

it("leaves an ordinary link to the general opener", async () => {
  allowIssues();
  await renderInline();
  const frame = document.querySelector("iframe")!;
  frame.focus();
  const url = "https://example.com/release-notes";
  await postLink(frame, url);
  expect(sourceControlRead).not.toHaveBeenCalled();
  expect(openChangeRequestLink).toHaveBeenCalledTimes(1);
  expect(openLink).toHaveBeenCalledTimes(1);
  expect(openLink).toHaveBeenCalledWith(url, {
    event: { id: "link", url, metaKey: false, ctrlKey: false },
  });
  expect(activeIssue(capableEnvironmentId)).toBeNull();
  expect(shellOpen).not.toHaveBeenCalled();
});

it("uses one external fallback when an Issue link cannot be opened in the app", async () => {
  allowIssues();
  await renderInline();
  const frame = document.querySelector("iframe")!;
  frame.focus();
  await postLink(frame, issueUrl);
  expect(sourceControlRead).toHaveBeenCalledTimes(1);
  expect(openLink).toHaveBeenCalledTimes(1);
  expect(openLink).toHaveBeenCalledWith(issueUrl);
  expect(openChangeRequestLink).not.toHaveBeenCalled();
  expect(shellOpen).not.toHaveBeenCalled();
  expect(activeIssue(capableEnvironmentId)).toBeNull();
});

it.each([
  ["metaKey", { metaKey: true }],
  ["ctrlKey", { ctrlKey: true }],
] as const)("sends a %s Issue click to the system browser once", async (_name, modifiers) => {
  allowIssues();
  sourceControlRead.mockResolvedValue({ _tag: "Success", value: {} });
  await renderInline();
  const frame = document.querySelector("iframe")!;
  frame.focus();
  await postLink(frame, issueUrl, modifiers);
  expect(shellOpen).toHaveBeenCalledTimes(1);
  expect(shellOpen).toHaveBeenCalledWith(issueUrl);
  expect(sourceControlRead).not.toHaveBeenCalled();
  expect(openLink).not.toHaveBeenCalled();
  expect(openChangeRequestLink).not.toHaveBeenCalled();
  expect(activeIssue(capableEnvironmentId)).toBeNull();
});

it("does not open an Issue for an environment that does not list Issues", async () => {
  allowIssues();
  sourceControlRead.mockResolvedValue({ _tag: "Success", value: {} });
  await renderInline(otherEnvironmentId);
  const frame = document.querySelector("iframe")!;
  frame.focus();
  await postLink(frame, issueUrl);
  expect(sourceControlRead).not.toHaveBeenCalled();
  expect(openLink).toHaveBeenCalledTimes(1);
  expect(openLink).toHaveBeenCalledWith(issueUrl, {
    event: { id: "link", url: issueUrl, metaKey: false, ctrlKey: false },
  });
  expect(activeIssue(otherEnvironmentId)).toBeNull();
  expect(activeIssue(capableEnvironmentId)).toBeNull();
  expect(shellOpen).not.toHaveBeenCalled();
});

it("ignores a link from another window, an unfocused frame, or an inactive activation", async () => {
  allowIssues();
  sourceControlRead.mockResolvedValue({ _tag: "Success", value: {} });
  await renderInline();
  const frame = document.querySelector("iframe")!;
  const stranger = document.createElement("iframe");
  document.body.append(stranger);
  expect(stranger.contentWindow).not.toBe(frame.contentWindow);
  frame.focus();
  await postLink(frame, issueUrl, { source: stranger.contentWindow });
  document.querySelector<HTMLButtonElement>('[aria-label="Open full screen"]')!.focus();
  await postLink(frame, issueUrl);
  Object.defineProperty(navigator, "userActivation", {
    configurable: true,
    value: { isActive: false },
  });
  frame.focus();
  await postLink(frame, issueUrl);
  stranger.remove();
  expect(sourceControlRead).not.toHaveBeenCalled();
  expect(openLink).not.toHaveBeenCalled();
  expect(openChangeRequestLink).not.toHaveBeenCalled();
  expect(shellOpen).not.toHaveBeenCalled();
  expect(activeIssue(capableEnvironmentId)).toBeNull();
});

it.each([null, new Error("Disconnected")])(
  "reports an unavailable URL instead of opening a blank page: %s",
  async (failure) => {
    await renderInlineNearExpiry();
    if (failure === null) refresh.mockResolvedValue(null);
    else refresh.mockRejectedValue(failure);
    await click("Open full screen");
    expect(document.querySelector('[role="dialog"]')).toBeNull();
    expect(showError).toHaveBeenCalledWith(
      expect.objectContaining({ type: "error", title: "Page unavailable" }),
    );
  },
);
