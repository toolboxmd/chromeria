// @vitest-environment jsdom
import { act, useEffect, useImperativeHandle, type ComponentProps, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { EnvironmentId, ThreadId, PreviewTabId } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
const observed = vi.hoisted(() => ({ command: vi.fn(), alert: vi.fn() }));
vi.mock("react-native", () => ({
  Platform: { OS: "android" },
  AppState: { currentState: "active", addEventListener: () => ({ remove() {} }) },
  Alert: { alert: observed.alert },
  View: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
  Pressable: ({
    children,
    onPress,
    disabled,
  }: {
    children?: ReactNode;
    onPress?: () => void;
    disabled?: boolean;
  }) => (
    <button disabled={disabled} onClick={onPress}>
      {children}
    </button>
  ),
  TextInput: ({
    value,
    onChangeText,
    onSubmitEditing,
    accessibilityLabel,
    editable,
  }: {
    value: string;
    onChangeText: (value: string) => void;
    onSubmitEditing: () => void;
    accessibilityLabel: string;
    editable: boolean;
  }) => (
    <input
      aria-label={accessibilityLabel}
      value={value}
      disabled={!editable}
      onInput={(event) => onChangeText(event.currentTarget.value)}
      onKeyDown={(event) => {
        if (event.key === "Enter") onSubmitEditing();
      }}
    />
  ),
}));
vi.mock("@react-navigation/native", () => ({
  useIsFocused: () => true,
  useNavigation: () => ({ goBack() {} }),
}));
vi.mock("@react-navigation/native-stack", () => ({
  createNativeStackNavigator: () => ({
    Navigator: ({ children }: { children: ReactNode }) => children,
    Screen: ({ children }: { children: () => ReactNode }) => children(),
  }),
}));
vi.mock("react-native-safe-area-context", () => ({ useSafeAreaInsets: () => ({ bottom: 0 }) }));
vi.mock("../../state/preview", () => ({ useThreadServerBrowserTabs: () => null }));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeVariables: {} }),
}));
vi.mock("../../components/ui/Toolbar", () => ({ Toolbar: () => null }));
vi.mock("../../components/ui/AppActivityIndicator", () => ({ AppActivityIndicator: () => null }));
vi.mock("../../components/ui/ControlPill", () => ({ ControlPill: () => null }));
vi.mock("../../components/ui/AppBar", () => ({ AppBar: () => null }));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
}));
vi.mock("./BrowserTabMenu", () => ({ BrowserTabMenu: () => null }));
vi.mock("./PreviewStreamWebView", () => ({
  PreviewStreamWebView: (
    props: ComponentProps<typeof import("./PreviewStreamWebView").PreviewStreamWebView>,
  ) => {
    useImperativeHandle(
      props.ref,
      () => ({ command: observed.command, togglePictureInPicture() {}, paste() {} }),
      [],
    );
    useEffect(() => {
      props.onStreamingChange?.(true);
      props.onControl?.({ canOperate: true, controller: "you", generation: 1, dialog: null });
    }, []);
    return null;
  },
}));
import { BrowserPreviewRouteScreen } from "./BrowserPreviewRouteScreen";
it("qualified navigation reports invalid addresses and sends only normalized valid input", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  observed.command.mockClear();
  observed.alert.mockClear();
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const errors: unknown[] = [];
  const error = (event: ErrorEvent) => {
    errors.push(event.error);
    event.preventDefault();
  };
  window.addEventListener("error", error);
  try {
    await act(async () =>
      root.render(
        <BrowserPreviewRouteScreen
          route={{
            params: {
              environmentId: "source-A",
              threadId: "destination-thread",
              resource: {
                type: "preview",
                environmentId: EnvironmentId.make("destination-B"),
                threadId: ThreadId.make("destination-thread"),
                tabId: PreviewTabId.make("tab"),
                serverEpoch: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
              },
            },
          }}
        />,
      ),
    );
    const input = container.querySelector<HTMLInputElement>("input")!;
    const go = [...container.querySelectorAll("button")].find(
      (button) => button.textContent === "Go",
    )!;
    expect(input.disabled).toBe(false);
    const enter = async (value: string) =>
      act(async () => {
        input.value = value;
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
    for (const [index, value] of ["http://", "file:///private/test"].entries()) {
      await enter(value);
      await act(async () => {
        if (index === 0) go.click();
        else input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
      });
      expect(observed.command).not.toHaveBeenCalled();
      expect(observed.alert).toHaveBeenLastCalledWith(
        "Could not open this address",
        "Enter an http or https URL.",
      );
    }
    expect(errors).toEqual([]);
    await enter("localhost:3000");
    await act(async () => go.click());
    expect(observed.command.mock.calls).toEqual([
      [{ type: "navigate", url: "http://localhost:3000/" }],
    ]);
  } finally {
    await act(async () => root.unmount());
    window.removeEventListener("error", error);
    container.remove();
    vi.unstubAllGlobals();
  }
});
