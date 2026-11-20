import { SheetHeader } from "../../components/ui/SheetHeader";
import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import * as Dialog from "../../components/ui/Dialog";
import { useIsFocused, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { EnvironmentId, PreviewViewerResource, ThreadId } from "@t3tools/contracts";
import { normalizePreviewUrl, previewPdfAssetUrl } from "@t3tools/shared/preview";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { msg, useGT, useMessages } from "gt-react-native";
import { useCallback, useEffect, useRef, useState } from "react";
import { AppState, Platform, TextInput, View, Pressable } from "react-native";
import { SafeAreaView, useSafeAreaInsets } from "react-native-safe-area-context";
import { AppText } from "../../components/ui/AppText";
import { ControlPill } from "../../components/ui/ControlPill";
import { Menu } from "../../components/ui/Menu";
import { useContentTopInset } from "../../components/ui/useBarInset";
import { m } from "../../i18n/messages";
import { useAppForeground } from "../../lib/appState";
import { useThreadServerBrowserTabs } from "../../state/preview";
import { usePreparedConnection } from "../../state/session";
import { useAppearancePreferences } from "../settings/appearance/AppearancePreferencesProvider";
import { BrowserChrome } from "./BrowserBar";
import { BrowserTabMenu } from "./BrowserTabMenu";
import { browserTabTitle, browserTabUrl, latestBrowserTab } from "./browserTabs";
import {
  PreviewStreamWebView,
  type PreviewPictureInPictureState,
  type PreviewStreamRef,
} from "./PreviewStreamWebView";

const decodePreviewViewerResource = Schema.decodeUnknownOption(PreviewViewerResource);

const NO_PICTURE_IN_PICTURE: PreviewPictureInPictureState = { supported: false, active: false };

type BrowserPreviewRouteScreenProps = StaticScreenProps<{
  readonly environmentId: string;
  readonly threadId: string;
  readonly tabId?: string;
  readonly resource?: PreviewViewerResource;
}>;

export function BrowserPreviewRouteScreen({ route }: BrowserPreviewRouteScreenProps) {
  const navigation = useNavigation();
  const onClose = useCallback(() => navigation.goBack(), [navigation]);
  const resource =
    route.params.resource === undefined
      ? undefined
      : Option.getOrNull(decodePreviewViewerResource(route.params.resource));
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Close the navigation route when its external resource identity is invalid.
  useEffect(() => {
    if (resource === null) onClose();
  }, [resource, onClose]);
  if (resource === null) return null;
  const { environmentId, threadId } = route.params;
  if (!environmentId?.trim() || (resource === undefined && !threadId?.trim())) {
    return <BrowserUnavailableScreen onClose={onClose} />;
  }
  return resource === undefined ? (
    <BrowserPreviewScreen
      environmentId={EnvironmentId.make(environmentId)}
      threadId={ThreadId.make(threadId)}
      initialTabId={route.params.tabId ?? null}
      onClose={onClose}
    />
  ) : (
    <QualifiedBrowserPreviewScreen
      key={JSON.stringify(resource)}
      environmentId={EnvironmentId.make(route.params.environmentId)}
      resource={resource}
      onClose={onClose}
    />
  );
}

function QualifiedBrowserPreviewScreen({
  environmentId,
  resource,
  onClose,
}: {
  readonly environmentId: EnvironmentId;
  readonly resource: PreviewViewerResource;
  readonly onClose: () => void;
}) {
  const gt = useGT();
  const focused = useIsFocused();
  const { themeVariables } = useAppearancePreferences();
  const [foreground, setForeground] = useState(AppState.currentState !== "background");
  const [selected, setSelected] = useState(resource);
  const [tabs, setTabs] = useState([resource]);
  const [address, setAddress] = useState("");
  const [canControl, setCanControl] = useState(false);
  const [streaming, setStreaming] = useState(false);
  const [pictureInPicture, setPictureInPicture] = useState(NO_PICTURE_IN_PICTURE);
  const streamRef = useRef<PreviewStreamRef>(null);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Subscribe to native foreground changes while the preview is mounted.
  useEffect(() => {
    const subscription = AppState.addEventListener("change", (state) =>
      setForeground(state !== "background"),
    );
    return () => subscription.remove();
  }, []);
  const navigate = () => {
    if (!canControl || !streaming || !address.trim()) return;
    const stream = streamRef.current;
    if (stream === null) return;
    try {
      stream.command({ type: "navigate", url: normalizePreviewUrl(address) });
    } catch {
      Dialog.alert(gt("Could not open this address"), gt("Enter an http or https URL."));
    }
  };
  return (
    <View className="flex-1 bg-sheet">
      <SheetHeader
        title={gt("Browser", { $context: "screen title" })}
        onClose={onClose}
        accessibilityLabel={gt("Close browser")}
        actions={
          Platform.OS === "ios" && pictureInPicture.supported
            ? [
                {
                  accessibilityLabel: pictureInPicture.active
                    ? gt("Exit picture in picture")
                    : gt("Picture in picture"),
                  icon: {
                    ios: pictureInPicture.active ? "pip.exit" : "pip.enter",
                    android: "visibility",
                  },
                  onPress: () => streamRef.current?.togglePictureInPicture(),
                },
              ]
            : []
        }
        menus={
          tabs.length > 1
            ? [
                {
                  title: gt("Tabs", { $context: "browser tabs menu" }),
                  icon: "square.on.square",
                  items: tabs.map((tab, index) => ({
                    id: `tab-${index}`,
                    title: gt("Browser {number}", { number: String(index + 1) }),
                    selected: tab.tabId === selected.tabId,
                    onPress: () => {
                      setSelected(tab);
                      setPictureInPicture(NO_PICTURE_IN_PICTURE);
                    },
                  })),
                },
              ]
            : []
        }
      />
      <View className="flex-row items-center gap-3 px-4 py-2">
        <Pressable accessibilityRole="button" onPress={onClose}>
          <AppText>{gt("Close view")}</AppText>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          disabled={!streaming || !canControl}
          onPress={() => streamRef.current?.command({ type: "history", delta: -1 })}
        >
          <AppText>{gt("Back", { $context: "browser history" })}</AppText>
        </Pressable>
        <TextInput
          className="flex-1 text-foreground"
          accessibilityLabel={gt("Browser address")}
          placeholder={gt("Address", { $context: "browser address bar" })}
          value={address}
          onChangeText={setAddress}
          onSubmitEditing={navigate}
          autoCapitalize="none"
          autoCorrect={false}
          editable={streaming && canControl}
        />
        <Pressable
          accessibilityRole="button"
          disabled={!streaming || !canControl}
          onPress={navigate}
        >
          <AppText>{gt("Go", { $context: "navigate to the address" })}</AppText>
        </Pressable>
      </View>
      <PreviewStreamWebView
        key={`${selected.environmentId}:${selected.threadId}:${selected.tabId}:${selected.serverEpoch}`}
        environmentId={environmentId}
        resource={selected}
        threadId={selected.threadId}
        tabId={selected.tabId}
        paused={!(focused || pictureInPicture.active)}
        backgrounded={!(foreground || pictureInPicture.active)}
        ref={streamRef}
        interactive
        background={themeVariables["--color-sheet-solid"]}
        onPictureInPicture={(state, detail) => {
          setPictureInPicture(state);
          if (detail) Dialog.alert(gt("Picture in picture is unavailable"), detail);
        }}
        onStreamingChange={setStreaming}
        onControl={(control) => setCanControl(control?.controller === "you")}
        onPopup={(popup) => {
          setTabs((current) =>
            current.some((tab) => tab.tabId === popup.tabId) ? current : [...current, popup],
          );
          setSelected(popup);
          setPictureInPicture(NO_PICTURE_IN_PICTURE);
        }}
      />
    </View>
  );
}

function BrowserUnavailableScreen({ onClose }: { readonly onClose: () => void }) {
  const gt = useGT();
  const insets = useSafeAreaInsets();
  const contentTopInset = useContentTopInset();
  return (
    <View
      className="flex-1 bg-sheet"
      style={{ paddingTop: contentTopInset, paddingBottom: insets.bottom }}
    >
      <SheetHeader
        title={gt("Browser", { $context: "screen title" })}
        onClose={onClose}
        accessibilityLabel={gt("Close browser")}
      />
    </View>
  );
}

function BrowserPreviewScreen({
  environmentId,
  threadId,
  initialTabId,
  onClose,
}: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly initialTabId: string | null;
  readonly onClose: () => void;
}) {
  const gt = useGT();
  const resolveMessage = useMessages();
  const { themeVariables } = useAppearancePreferences();
  const prepared = usePreparedConnection(environmentId);
  const httpBaseUrl = Option.isSome(prepared) ? prepared.value.httpBaseUrl : null;
  const focused = useIsFocused();
  const foreground = useAppForeground();
  const [selectedTabId, setSelectedTabId] = useState(initialTabId);
  const [pictureInPicture, setPictureInPicture] = useState(NO_PICTURE_IN_PICTURE);
  const [streaming, setStreaming] = useState(false);
  const [canControl, setCanControl] = useState(false);
  const streamRef = useRef<PreviewStreamRef>(null);
  const { tabs, loaded } = useThreadServerBrowserTabs({ environmentId, threadId, enabled: true });
  const tab = tabs.find((entry) => entry.tabId === selectedTabId) ?? latestBrowserTab(tabs);
  const pdfUri =
    tab?.navStatus._tag === "Success" ? previewPdfAssetUrl(tab.navStatus.url, httpBaseUrl) : null;
  const tabId = tab?.tabId ?? null;
  if (tabId !== null && tabId !== selectedTabId) {
    setPictureInPicture(NO_PICTURE_IN_PICTURE);
    setSelectedTabId(tabId);
  }
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- closes the focused route through the navigator once the thread has no browser tabs left
  useEffect(() => {
    if (focused && loaded && tabs.length === 0) onClose();
  }, [focused, loaded, tabs.length, onClose]);
  const onPictureInPicture = (state: PreviewPictureInPictureState, detail?: string) => {
    setPictureInPicture({ supported: state.supported, active: state.active });
    if (detail) Dialog.alert(m(msg("Picture in picture is unavailable")), detail);
  };
  const selectTab = (next: string) => {
    setPictureInPicture(NO_PICTURE_IN_PICTURE);
    setSelectedTabId(next);
  };
  const live = (focused && foreground) || pictureInPicture.active;

  const ready = streaming && canControl;
  const command: PreviewStreamRef["command"] = (input) => streamRef.current?.command(input);
  const showPictureInPicture =
    // Android WebView cannot play video in picture in picture.
    Platform.OS === "ios" &&
    pictureInPicture.supported &&
    (pictureInPicture.active || pdfUri === null);

  return (
    // The route spans the Duo's rail, so the screen insets its own sides.
    <SafeAreaView
      edges={["left", "right"]}
      style={{ flex: 1, backgroundColor: themeVariables["--color-sheet-solid"] }}
    >
      <BrowserChrome
        key={tab ? `address:${tab.tabId}` : "address"}
        scrollsUnderBar={false}
        bar={{
          url: tab ? browserTabUrl(tab) : "",
          editable: ready,
          canGoBack: ready && (tab?.canGoBack ?? false),
          onClose,
          onBack: () => command({ type: "history", delta: -1 }),
          onReload: () => command({ type: "reload" }),
          onSubmit: (value) => {
            const url = normalizedOrNull(value);
            if (url === null)
              Dialog.alert(
                m(msg("Could not open this address")),
                m(msg("Enter an http or https URL.")),
              );
            else command({ type: "navigate", url });
          },
          trailing: (
            <>
              {showPictureInPicture ? (
                <ControlPill
                  icon={{
                    ios: pictureInPicture.active ? "pip.exit" : "pip.enter",
                    android: "visibility",
                  }}
                  accessibilityLabel={
                    pictureInPicture.active
                      ? gt("Exit picture in picture")
                      : gt("Picture in picture")
                  }
                  onPress={() => streamRef.current?.togglePictureInPicture()}
                />
              ) : null}
              {tabs.length > 1 ? (
                <Menu
                  accessible
                  accessibilityLabel={gt("Tabs", { $context: "browser tabs menu" })}
                  accessibilityRole="button"
                  actions={tabs.map((entry) => ({
                    id: entry.tabId,
                    title: browserTabTitle(entry, resolveMessage),
                    subtitle: browserTabUrl(entry),
                    state: entry.tabId === tabId ? "on" : "off",
                  }))}
                  onPressAction={({ nativeEvent }) => selectTab(nativeEvent.event)}
                >
                  <ControlPill
                    icon="square.on.square"
                    accessibilityLabel={gt("Tabs", { $context: "browser tabs menu" })}
                  />
                </Menu>
              ) : null}
              {tab ? (
                <BrowserTabMenu
                  environmentId={environmentId}
                  tab={tab}
                  streaming={streaming}
                  onPaste={ready ? () => streamRef.current?.paste() : undefined}
                  onReleaseControl={ready ? () => command({ type: "releaseControl" }) : undefined}
                />
              ) : null}
            </>
          ),
        }}
      >
        {({ onScrollDirection }) =>
          tab ? (
            live ? (
              <PreviewStreamWebView
                key={`stream:${tab.tabId}`}
                environmentId={environmentId}
                threadId={threadId}
                tabId={tab.tabId}
                pdfUri={pdfUri}
                ref={streamRef}
                interactive
                background={themeVariables["--color-sheet-solid"]}
                onPictureInPicture={onPictureInPicture}
                onStreamingChange={setStreaming}
                onScrollDirection={onScrollDirection}
                onControl={(control) => setCanControl(control?.controller === "you")}
              />
            ) : (
              <View className="flex-1" />
            )
          ) : (
            <View className="flex-1 items-center justify-center">
              {focused && foreground ? (
                <AppActivityIndicator color={themeVariables["--color-icon"]} />
              ) : null}
            </View>
          )
        }
      </BrowserChrome>
    </SafeAreaView>
  );
}

function normalizedOrNull(value: string): string | null {
  try {
    return normalizePreviewUrl(value);
  } catch {
    return null;
  }
}
