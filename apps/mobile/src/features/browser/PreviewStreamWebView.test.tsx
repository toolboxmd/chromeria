// @vitest-environment jsdom
import { act, useEffect, useImperativeHandle, type ReactNode, type Ref } from "react";
import { createRoot } from "react-dom/client";
import type { WebViewMessageEvent, WebViewProps } from "react-native-webview";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { DeviceHubAccess } from "@t3tools/client-runtime/device/hub-access";
import { expect, it, vi } from "vite-plus/test";
const observed = vi.hoisted(() => ({
  message: undefined as WebViewProps["onMessage"],
  mounts: 0,
  unmounts: 0,
  appState: "active",
  focused: true,
  listeners: new Set<(state: string) => void>(),
  renew: vi.fn(),
  refresh: vi.fn(),
  picker: vi.fn(),
  share: vi.fn(),
  alert: vi.fn(),
  changePrompt: undefined as ((value: string) => void) | undefined,
  inject: vi.fn(),
  refreshedAccess: null as DeviceHubAccess | null,
  source: undefined as WebViewProps["source"],
}));
vi.mock("react-native", () => ({
  Platform: { OS: "android" },
  AppState: {
    get currentState() {
      return observed.appState;
    },
    addEventListener: (_: string, listener: (state: string) => void) => {
      observed.listeners.add(listener);
      return { remove: () => observed.listeners.delete(listener) };
    },
  },
  View: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  Pressable: ({ children, onPress }: { children?: ReactNode; onPress?: () => void }) => (
    <button onClick={onPress}>{children}</button>
  ),
  TextInput: ({
    value,
    onChangeText,
  }: {
    value: string;
    onChangeText: (value: string) => void;
  }) => {
    observed.changePrompt = onChangeText;
    return <input aria-label="Dialog response" value={value} readOnly />;
  },
}));
vi.mock("react-native-webview", () => ({
  WebView: (
    props: WebViewProps & { ref?: Ref<{ injectJavaScript: (script: string) => void }> },
  ) => {
    observed.message = props.onMessage;
    observed.source = props.source;
    useImperativeHandle(props.ref, () => ({ injectJavaScript: observed.inject }));
    useEffect(() => {
      observed.mounts++;
      return () => {
        observed.unmounts++;
      };
    }, []);
    return <div />;
  },
}));
vi.mock("@t3tools/mobile-preview-stream", () => ({ default: "" }));
vi.mock("expo-clipboard", () => ({ setStringAsync: async () => {} }));
vi.mock("expo-document-picker", () => ({ getDocumentAsync: observed.picker }));
vi.mock("../../components/ui/Dialog", () => ({ alert: observed.alert }));
vi.mock("../../components/ui/AppActivityIndicator", () => ({ AppActivityIndicator: () => null }));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../lib/attachmentDownload", () => ({ downloadAndShareAttachment: observed.share }));
vi.mock("../../state/preview", () => {
  const access = {
    httpBase: "https://a.test/api/preview-stream",
    wsBase: "wss://a.test/api/preview-stream",
    query: { viewerTicket: "expired" },
    credentials: false,
    canOperate: true,
    requestCurrentAccess: observed.renew,
  };
  return {
    usePreviewStreamAccess: () => ({
      access: observed.refreshedAccess ?? access,
      error: null,
      refresh: observed.refresh,
    }),
    useThreadServerBrowserTabs: () => null,
  };
});
vi.mock("@react-navigation/native", () => ({
  useIsFocused: () => observed.focused,
  useNavigation: () => ({ goBack() {} }),
}));
vi.mock("@react-navigation/native-stack", () => ({
  createNativeStackNavigator: () => ({
    Navigator: ({ children }: { children: ReactNode }) => children,
    Screen: ({ children }: { children: () => ReactNode }) => children(),
  }),
}));
vi.mock("react-native-safe-area-context", () => ({ useSafeAreaInsets: () => ({ bottom: 0 }) }));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeVariables: {} }),
}));
vi.mock("../../components/ui/Toolbar", () => ({ Toolbar: () => null }));
vi.mock("../../components/ui/ControlPill", () => ({ ControlPill: () => null }));
vi.mock("../../components/ui/AppBar", () => ({ AppBar: () => null }));
vi.mock("./BrowserTabMenu", () => ({ BrowserTabMenu: () => null }));
import { BrowserPreviewRouteScreen } from "./BrowserPreviewRouteScreen";
import { isForegroundHandoffActive } from "../../lib/foreground-handoff";
import { PreviewStreamWebView } from "./PreviewStreamWebView";

it("renews expired access without replacing a background picture-in-picture document", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  observed.mounts = observed.unmounts = 0;
  observed.appState = "active";
  observed.focused = true;
  observed.refresh.mockReset();
  observed.inject.mockClear();
  observed.renew.mockReset();
  observed.renew.mockRejectedValue(new Error("The exact active viewer is unavailable."));
  const current = {
    httpBase: "https://a.test/api/preview-stream",
    wsBase: "wss://a.test/api/preview-stream",
    credentials: false,
    canOperate: true,
    query: { viewerTicket: "fresh-opening" },
    viewer: { id: "fresh-viewer", expiresAt: "2027-01-15T08:01:30.000Z" },
    expiresAt: 1_800_000_090_000,
  };
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  const message = (value: unknown) =>
    observed.message!({ nativeEvent: { data: JSON.stringify(value) } } as WebViewMessageEvent);
  const render = () =>
    root.render(
      <BrowserPreviewRouteScreen
        route={{
          params: {
            environmentId: "A",
            threadId: "b",
            resource: {
              type: "preview",
              environmentId: EnvironmentId.make("B"),
              threadId: ThreadId.make("b"),
              tabId: "tab",
              serverEpoch: "epoch",
            },
          },
        }}
      />,
    );
  try {
    await act(async () => render());
    const source = observed.source;
    await act(async () => {
      message({ type: "status", status: "streaming" });
      message({ type: "pictureInPicture", supported: true, active: true });
    });
    await act(async () => {
      observed.appState = "background";
      for (const listener of observed.listeners) listener("background");
    });
    await act(async () => {
      message({ type: "unauthorized" });
      vi.advanceTimersByTime(0);
    });
    expect(observed.renew).not.toHaveBeenCalled();
    expect(observed.refresh).toHaveBeenCalledOnce();
    expect(observed.unmounts).toBe(0);
    await act(async () => {
      observed.refreshedAccess = current;
      render();
    });
    expect(observed.inject).toHaveBeenCalledWith(
      `window.T3PreviewStream?.updateAccess(${JSON.stringify(current)}); true;`,
    );
    expect(observed.mounts).toBe(1);
    expect(observed.unmounts).toBe(0);
    expect(observed.source).toBe(source);
    await act(async () => message({ type: "status", status: "error", detail: "Socket failed" }));
    await act(async () => {
      [...host.querySelectorAll("button")]
        .find((button) => button.textContent === "Reconnect")!
        .click();
    });
    expect(observed.refresh).toHaveBeenCalledTimes(2);
    await act(async () => {
      observed.refreshedAccess = { ...current, query: { viewerTicket: "manual-retry" } };
      render();
    });
    expect(observed.mounts).toBe(2);
    expect(observed.unmounts).toBe(1);
    expect(observed.source).not.toBe(source);
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.useRealTimers();
    vi.unstubAllGlobals();
    observed.appState = "active";
    observed.mounts = observed.unmounts = 0;
    observed.refreshedAccess = null;
    observed.refresh.mockReset();
  }
});

it("admits native files after picker completion and downloads after save confirmation without reopening control", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  let ticket = "not-yet-current";
  let retired = false;
  observed.renew.mockImplementation(async () => {
    if (retired) throw new Error("Original authority retired");
    return { query: { viewerTicket: ticket } };
  });
  const chosen = Promise.withResolvers<{
    canceled: false;
    assets: Array<{ uri: string; name: string; mimeType: string }>;
  }>();
  observed.picker.mockReturnValue(chosen.promise);
  const uploaded = Promise.withResolvers<void>();
  const posted: Array<{ url: string; body: unknown }> = [];
  vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
    posted.push({ url, body: init.body });
    uploaded.resolve();
    return new Response(null, { status: 204 });
  });
  class NativeFormData {
    readonly parts: Array<{ name: string; value: unknown }> = [];
    append(name: string, value: unknown) {
      this.parts.push({ name, value });
    }
  }
  vi.stubGlobal("FormData", NativeFormData);
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  const message = (value: unknown) =>
    observed.message!({ nativeEvent: { data: JSON.stringify(value) } } as WebViewMessageEvent);
  const picker = {
    multiple: false,
    accept: "",
    uploadUrl:
      "https://a.test/api/preview-stream/upload?threadId=b&tabId=tab&chooser=exact&viewerTicket=expired",
    credentials: false,
  };
  const click = (text: string) => {
    const button = [...host.querySelectorAll("button")].find((value) => value.textContent === text);
    expect(button).toBeDefined();
    button!.click();
  };
  try {
    await act(async () =>
      root.render(
        <PreviewStreamWebView
          environmentId={EnvironmentId.make("A")}
          threadId="b"
          tabId="tab"
          interactive
          background="#000"
        />,
      ),
    );
    await act(async () =>
      message({
        type: "control",
        controller: "you",
        canOperate: true,
        generation: 3,
        dialog: { type: "prompt", message: "Enter a response", defaultValue: "original" },
      }),
    );
    const draft = "界".repeat(16385);
    await act(async () => observed.changePrompt!(draft));
    expect(host.querySelector("input")?.value).toBe(draft);
    const mountsBeforeRefusal = observed.mounts;
    await act(async () =>
      message({
        type: "inputRejected",
        message: "Browser input is invalid or too large. Edit it and try again.",
      }),
    );
    expect(observed.alert).toHaveBeenCalledWith(
      "Browser input refused",
      "Browser input is invalid or too large. Edit it and try again.",
    );
    expect(observed.mounts).toBe(mountsBeforeRefusal);
    expect(host.querySelector("input")?.value).toBe(draft);
    expect(host.textContent).not.toContain("Reconnect");
    await act(async () => {
      message({
        type: "control",
        controller: "you",
        canOperate: true,
        generation: 3,
        dialog: null,
      });
      message({ type: "fileChooser", chooser: picker });
    });
    await act(async () => click("Choose file"));
    expect(observed.renew).not.toHaveBeenCalled();
    ticket = "fresh-after-native-picker";
    await act(async () => {
      chosen.resolve({
        canceled: false,
        assets: [{ uri: "file:///fixture/chosen.txt", name: "chosen.txt", mimeType: "text/plain" }],
      });
      await uploaded.promise;
    });
    expect(new URL(posted[0]!.url).searchParams.get("viewerTicket")).toBe(ticket);
    expect((posted[0]!.body as NativeFormData).parts).toEqual([
      {
        name: "file",
        value: { uri: "file:///fixture/chosen.txt", name: "chosen.txt", type: "text/plain" },
      },
    ]);
    ticket = "fresh-cancel";
    await act(async () => {
      message({ type: "fileChooser", chooser: picker });
    });
    await act(async () => click("Cancel"));
    expect(new URL(posted[1]!.url).searchParams.get("viewerTicket")).toBe(ticket);
    expect((posted[1]!.body as NativeFormData).parts).toEqual([]);
    await act(async () =>
      message({
        type: "download",
        fileName: "report.txt",
        sizeBytes: 4,
        url: "https://a.test/api/preview-stream/download?threadId=b&tabId=tab&id=exact-download&viewerTicket=expired",
      }),
    );
    const buttons = observed.alert.mock.calls.at(-1)![2] as Array<{
      text: string;
      onPress?: () => void;
    }>;
    ticket = "fresh-save";
    const shared = Promise.withResolvers<string>();
    observed.share.mockImplementation(async ({ url }: { url: string }) => {
      shared.resolve(url);
    });
    await act(async () => {
      buttons.find((button) => button.text === "Save or share")!.onPress!();
      expect(new URL(await shared.promise).searchParams.get("viewerTicket")).toBe(ticket);
    });
    retired = true;
    await act(async () => message({ type: "fileChooser", chooser: picker }));
    await act(async () => click("Cancel"));
    expect(observed.alert).toHaveBeenLastCalledWith(
      "Could not send the files to the page",
      "Original authority retired",
    );
    expect(posted).toHaveLength(2);
    expect(observed.share).toHaveBeenCalledTimes(1);
    expect(observed.mounts).toBe(1);
    expect(observed.refresh).not.toHaveBeenCalled();
  } finally {
    await act(async () => root.unmount());
    host.remove();
    vi.unstubAllGlobals();
  }
});

it.each(["upload", "cancel", "failure", "retired", "blur", "unmount", "refocus"] as const)(
  "qualified Android picker preserves the mounted viewer during background handoff (%s)",
  async (outcome) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    observed.mounts = 0;
    observed.unmounts = 0;
    observed.appState = "active";
    observed.focused = true;
    observed.alert.mockClear();
    observed.renew.mockReset();
    observed.renew.mockResolvedValue({ query: { viewerTicket: "current-same-viewer" } });
    const chosen = Promise.withResolvers<{
      canceled: boolean;
      assets: Array<{ uri: string; name: string; mimeType: string }>;
    }>();
    const pickerOpened = Promise.withResolvers<void>();
    observed.picker.mockImplementation(() => {
      pickerOpened.resolve();
      return chosen.promise;
    });
    const finished = Promise.withResolvers<void>();
    const oldPost = Promise.withResolvers<void>();
    const replacementPost = Promise.withResolvers<void>();
    const replacementPosted = Promise.withResolvers<void>();
    observed.alert.mockImplementation(() => finished.resolve());
    const posts: string[] = [];
    vi.stubGlobal("fetch", async (url: string) => {
      posts.push(url);
      finished.resolve();
      if (outcome === "refocus") {
        if (posts.length === 1) await oldPost.promise;
        else {
          replacementPosted.resolve();
          await replacementPost.promise;
        }
      }
      return new Response(null, { status: 204 });
    });
    vi.stubGlobal(
      "FormData",
      class {
        append() {}
      },
    );
    const host = document.createElement("div");
    document.body.append(host);
    const root = createRoot(host);
    const route = {
      params: {
        environmentId: "A",
        threadId: "b",
        resource: {
          type: "preview" as const,
          environmentId: EnvironmentId.make("B"),
          threadId: ThreadId.make("b"),
          tabId: "tab",
          serverEpoch: "epoch",
        },
      },
    };
    const state = (value: string) => {
      observed.appState = value;
      for (const listener of observed.listeners) listener(value);
    };
    try {
      await act(async () => root.render(<BrowserPreviewRouteScreen route={route} />));
      expect(observed.mounts).toBe(1);
      await act(async () => state("background"));
      expect(observed.unmounts).toBe(1);
      await act(async () => state("active"));
      expect(observed.mounts).toBe(2);
      await act(async () =>
        observed.message!({
          nativeEvent: {
            data: JSON.stringify({
              type: "control",
              controller: "you",
              canOperate: true,
              generation: 3,
              dialog: null,
            }),
          },
        } as WebViewMessageEvent),
      );
      await act(async () =>
        observed.message!({
          nativeEvent: {
            data: JSON.stringify({
              type: "fileChooser",
              chooser: {
                multiple: false,
                accept: "",
                credentials: false,
                uploadUrl:
                  "https://a.test/api/preview-stream/upload?threadId=b&tabId=tab&chooser=original&viewerTicket=expired",
              },
            }),
          },
        } as WebViewMessageEvent),
      );
      const button = [...host.querySelectorAll("button")].find(
        (value) => value.textContent === "Choose file",
      )!;
      await act(async () => {
        button.click();
        await pickerOpened.promise;
      });
      expect(isForegroundHandoffActive()).toBe(true);
      await act(async () => state("background"));
      expect(
        observed.unmounts,
        "Android picker background must not tear down its exact active viewer",
      ).toBe(1);
      expect(observed.renew).not.toHaveBeenCalled();
      if (outcome === "blur") {
        await act(async () => {
          observed.focused = false;
          root.render(<BrowserPreviewRouteScreen route={route} />);
        });
        expect(observed.unmounts).toBe(2);
      } else if (outcome === "unmount") {
        await act(async () => root.unmount());
        expect(observed.unmounts).toBe(2);
      }
      if (outcome === "refocus") {
        await act(async () => {
          state("active");
          chosen.resolve({
            canceled: false,
            assets: [{ uri: "file:///chosen.txt", name: "chosen.txt", mimeType: "text/plain" }],
          });
          await finished.promise;
        });
        expect(isForegroundHandoffActive()).toBe(false);
        await act(async () => state("background"));
        expect(observed.unmounts, "the original viewer retains its own pending POST").toBe(1);
        await act(async () => {
          state("active");
          observed.focused = false;
          root.render(<BrowserPreviewRouteScreen route={route} />);
        });
        expect(observed.unmounts).toBe(2);
        await act(async () => {
          observed.focused = true;
          root.render(<BrowserPreviewRouteScreen route={route} />);
        });
        expect(observed.mounts).toBe(3);
        await act(async () => state("background"));
        expect(
          observed.unmounts,
          "a replacement must not inherit the retired viewer's pending POST exemption",
        ).toBe(3);
        await act(async () => state("active"));
        expect(observed.mounts).toBe(4);
        await act(async () => {
          observed.message!({
            nativeEvent: {
              data: JSON.stringify({
                type: "control",
                controller: "you",
                canOperate: true,
                generation: 4,
                dialog: null,
              }),
            },
          } as WebViewMessageEvent);
          observed.message!({
            nativeEvent: {
              data: JSON.stringify({
                type: "fileChooser",
                chooser: {
                  multiple: false,
                  accept: "",
                  credentials: false,
                  uploadUrl:
                    "https://a.test/api/preview-stream/upload?threadId=b&tabId=tab&chooser=replacement&viewerTicket=expired",
                },
              }),
            },
          } as WebViewMessageEvent);
        });
        await act(async () => {
          [...host.querySelectorAll("button")]
            .find((value) => value.textContent === "Choose file")!
            .click();
          await replacementPosted.promise;
        });
        expect(isForegroundHandoffActive()).toBe(false);
        await act(async () => state("background"));
        expect(observed.unmounts).toBe(3);
        await act(async () => oldPost.resolve());
        expect(
          observed.unmounts,
          "retired promise settlement cannot end a replacement's own pending POST exemption",
        ).toBe(3);
        await act(async () => replacementPost.resolve());
        expect(observed.unmounts).toBe(4);
        expect(posts.map((url) => new URL(url).searchParams.get("chooser"))).toEqual([
          "original",
          "replacement",
        ]);
        expect(observed.renew).toHaveBeenCalledTimes(2);
        return;
      }
      const refused = outcome === "retired" || outcome === "blur" || outcome === "unmount";
      if (refused)
        observed.renew.mockRejectedValue(new Error("Original viewer no longer authorized"));
      await act(async () => {
        if (outcome === "failure") chosen.reject(new Error("Picker failed"));
        else
          chosen.resolve({
            canceled: outcome === "cancel",
            assets:
              outcome === "cancel"
                ? []
                : [
                    {
                      uri: "file:///chosen.txt",
                      name: "chosen.txt",
                      mimeType: "text/plain",
                    },
                  ],
          });
        await finished.promise;
      });
      expect(isForegroundHandoffActive()).toBe(false);
      expect(observed.unmounts, "true background must pause again after the handoff settles").toBe(
        2,
      );
      expect(observed.mounts).toBe(2);
      if (refused) {
        expect(posts).toEqual([]);
        expect(observed.renew).toHaveBeenCalledTimes(1);
        expect(observed.alert).toHaveBeenLastCalledWith(
          "Could not send the files to the page",
          "Original viewer no longer authorized",
        );
      } else if (outcome === "failure") {
        expect(posts).toEqual([]);
        expect(observed.renew).not.toHaveBeenCalled();
        expect(observed.alert).toHaveBeenLastCalledWith(
          "Could not send the files to the page",
          "Picker failed",
        );
      } else {
        expect(posts).toHaveLength(1);
        expect(observed.renew).toHaveBeenCalledTimes(1);
        const url = new URL(posts[0]!);
        expect(url.searchParams.get("chooser")).toBe("original");
        expect(url.searchParams.get("viewerTicket")).toBe("current-same-viewer");
      }
    } finally {
      await act(async () => {
        chosen.resolve({ canceled: true, assets: [] });
        oldPost.resolve();
        replacementPost.resolve();
      });
      await act(async () => root.unmount());
      host.remove();
      observed.alert.mockReset();
      vi.unstubAllGlobals();
    }
  },
);
