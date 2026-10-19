import { useIsFocused, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import {
  PreviewViewerResource,
  EnvironmentId,
  ThreadId,
  type PreviewSessionSnapshot,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { normalizePreviewUrl } from "@t3tools/shared/preview";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  AppState,
  Platform,
  TextInput,
  View,
  Pressable,
} from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppText } from "../../components/AppText";
import { ControlPill } from "../../components/ControlPill";
import { ScreenHeader } from "../../components/ScreenHeader";
import { NativeHeaderToolbar } from "../../native/StackHeader";
import { useThreadServerBrowserTabs } from "../../state/preview";
import { useAppearancePreferences } from "../settings/appearance/AppearancePreferencesProvider";
import { BrowserTabMenu } from "./BrowserTabMenu";
import { browserTabTitle, browserTabUrl, latestBrowserTab } from "./browserTabs";
import {
  PreviewStreamWebView,
  type PreviewPictureInPictureState,
  type PreviewStreamRef,
} from "./PreviewStreamWebView";

const BrowserPreviewStack = createNativeStackNavigator<{ BrowserPreview: undefined }>();

const NO_PICTURE_IN_PICTURE: PreviewPictureInPictureState = { supported: false, active: false };

type BrowserPreviewRouteScreenProps = StaticScreenProps<{
  readonly environmentId: string;
  readonly threadId: string;
  readonly tabId?: string;
  readonly resource?: PreviewViewerResource;
}>;

/** The nested native stack supplies the navigation bar inside the modal. */
export function BrowserPreviewRouteScreen({ route }: BrowserPreviewRouteScreenProps) {
  const navigation = useNavigation();
  const onClose = useCallback(() => navigation.goBack(), [navigation]);
  const resource =
    route.params.resource === undefined
      ? undefined
      : Option.getOrNull(Schema.decodeUnknownOption(PreviewViewerResource)(route.params.resource));
  useEffect(() => {
    if (resource === null) onClose();
  }, [resource, onClose]);
  if (resource === null) return null;
  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      <BrowserPreviewStack.Navigator
        screenOptions={{
          headerShown: Platform.OS === "ios",
          headerBackVisible: false,
          headerShadowVisible: false,
          headerTransparent: false,
          headerTitleStyle: { fontSize: 17, fontWeight: "600" },
        }}
      >
        <BrowserPreviewStack.Screen name="BrowserPreview">
          {() =>
            resource === undefined ? (
              <BrowserPreviewScreen
                environmentId={EnvironmentId.make(route.params.environmentId)}
                threadId={ThreadId.make(route.params.threadId)}
                initialTabId={route.params.tabId ?? null}
                onClose={onClose}
              />
            ) : (
              <QualifiedBrowserPreviewScreen
                environmentId={EnvironmentId.make(route.params.environmentId)}
                resource={resource}
                onClose={onClose}
              />
            )
          }
        </BrowserPreviewStack.Screen>
      </BrowserPreviewStack.Navigator>
    </View>
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
  useEffect(() => {
    const subscription = AppState.addEventListener("change", (state) =>
      setForeground(state !== "background"),
    );
    return () => subscription.remove();
  }, []);
  useEffect(() => {
    setSelected(resource);
    setTabs([resource]);
  }, [resource]);
  const navigate = () => {
    if (canControl && streaming && address.trim())
      streamRef.current?.command({ type: "navigate", url: normalizePreviewUrl(address) });
  };
  return (
    <View className="flex-1 bg-sheet">
      <ScreenHeader
        title="Browser"
        sidebar={false}
        onBack={onClose}
        options={{ headerBackVisible: false }}
        actions={
          Platform.OS === "ios" && pictureInPicture.supported
            ? [
                {
                  accessibilityLabel: pictureInPicture.active
                    ? "Exit picture in picture"
                    : "Picture in picture",
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
                  title: "Tabs",
                  icon: "square.on.square",
                  items: tabs.map((tab, index) => ({
                    id: `tab-${index}`,
                    title: `Browser ${index + 1}`,
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
          <AppText>Close view</AppText>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          disabled={!streaming || !canControl}
          onPress={() => streamRef.current?.command({ type: "history", delta: -1 })}
        >
          <AppText>Back</AppText>
        </Pressable>
        <TextInput
          className="flex-1 text-foreground"
          accessibilityLabel="Browser address"
          placeholder="Address"
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
          <AppText>Go</AppText>
        </Pressable>
      </View>
      <PreviewStreamWebView
        key={`${selected.environmentId}:${selected.threadId}:${selected.tabId}:${selected.serverEpoch}`}
        environmentId={environmentId}
        resource={selected}
        threadId={selected.threadId}
        tabId={selected.tabId}
        paused={!((focused && foreground) || pictureInPicture.active)}
        ref={streamRef}
        interactive
        background={themeVariables["--color-sheet-solid"]}
        onPictureInPicture={(state, detail) => {
          setPictureInPicture(state);
          if (detail) Alert.alert("Picture in picture is unavailable", detail);
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
  const insets = useSafeAreaInsets();
  const { themeVariables } = useAppearancePreferences();
  const focused = useIsFocused();
  const [foreground, setForeground] = useState(AppState.currentState !== "background");
  const [selectedTabId, setSelectedTabId] = useState(initialTabId);
  const [pictureInPicture, setPictureInPicture] = useState(NO_PICTURE_IN_PICTURE);
  // Address bar commands only reach a page that is streaming.
  const [streaming, setStreaming] = useState(false);
  const [canControl, setCanControl] = useState(false);
  const streamRef = useRef<PreviewStreamRef>(null);
  const { tabs, loaded } = useThreadServerBrowserTabs({ environmentId, threadId, enabled: true });
  const tab = tabs.find((entry) => entry.tabId === selectedTabId) ?? latestBrowserTab(tabs);
  const tabId = tab?.tabId ?? null;
  // Pin the fallback so another tab's activity does not switch the view.
  if (tabId !== null && tabId !== selectedTabId) {
    setPictureInPicture(NO_PICTURE_IN_PICTURE);
    setSelectedTabId(tabId);
  }
  useEffect(() => {
    const subscription = AppState.addEventListener("change", (state) =>
      setForeground(state !== "background"),
    );
    return () => subscription.remove();
  }, []);
  useEffect(() => {
    if (focused && loaded && tabs.length === 0) onClose();
  }, [focused, loaded, tabs.length, onClose]);
  const onPictureInPicture = useCallback((state: PreviewPictureInPictureState, detail?: string) => {
    setPictureInPicture({ supported: state.supported, active: state.active });
    if (detail) Alert.alert("Picture in picture is unavailable", detail);
  }, []);
  const selectTab = (next: string) => {
    setPictureInPicture(NO_PICTURE_IN_PICTURE);
    setSelectedTabId(next);
  };
  // System picture in picture keeps showing this stream after the app leaves the foreground.
  const live = (focused && foreground) || pictureInPicture.active;

  return (
    <View className="flex-1 bg-sheet" style={{ paddingBottom: insets.bottom }}>
      <ScreenHeader
        title={tab ? browserTabTitle(tab) : "Browser"}
        sidebar={false}
        onBack={onClose}
        options={{ headerBackVisible: false }}
        actions={
          // Android WebView cannot play video in picture in picture.
          Platform.OS === "ios" && pictureInPicture.supported
            ? [
                {
                  accessibilityLabel: pictureInPicture.active
                    ? "Exit picture in picture"
                    : "Picture in picture",
                  icon: {
                    ios: pictureInPicture.active ? "pip.exit" : "pip.enter",
                    android: "visibility",
                  },
                  selected: pictureInPicture.active,
                  onPress: () => streamRef.current?.togglePictureInPicture(),
                },
              ]
            : []
        }
        menus={
          tabs.length > 1
            ? [
                {
                  title: "Tabs",
                  icon: "square.on.square",
                  items: tabs.map((entry) => ({
                    id: entry.tabId,
                    title: browserTabTitle(entry),
                    subtitle: browserTabUrl(entry),
                    selected: entry.tabId === tabId,
                    onPress: () => selectTab(entry.tabId),
                  })),
                },
              ]
            : []
        }
      />
      {Platform.OS === "ios" ? (
        <NativeHeaderToolbar placement="left">
          <NativeHeaderToolbar.Button
            icon="xmark"
            accessibilityLabel="Close browser"
            onPress={onClose}
            separateBackground
          />
        </NativeHeaderToolbar>
      ) : null}
      {tab ? (
        <>
          <BrowserAddressBar
            key={`address:${tab.tabId}`}
            environmentId={environmentId}
            tab={tab}
            streaming={streaming}
            ready={streaming && canControl}
            onCommand={(input) => streamRef.current?.command(input)}
          />
          {live ? (
            <PreviewStreamWebView
              key={`stream:${tab.tabId}`}
              environmentId={environmentId}
              threadId={threadId}
              tabId={tab.tabId}
              ref={streamRef}
              interactive
              background={themeVariables["--color-sheet-solid"]}
              onPictureInPicture={onPictureInPicture}
              onStreamingChange={setStreaming}
              onControl={(control) => setCanControl(control?.controller === "you")}
            />
          ) : (
            <View className="flex-1" />
          )}
        </>
      ) : (
        <View className="flex-1 items-center justify-center">
          {focused && foreground ? (
            <ActivityIndicator color={themeVariables["--color-icon"]} />
          ) : null}
        </View>
      )}
    </View>
  );
}

function BrowserAddressBar({
  environmentId,
  tab,
  ready,
  streaming,
  onCommand,
}: {
  readonly environmentId: EnvironmentId;
  readonly tab: PreviewSessionSnapshot;
  /** The viewer controls the page, so navigation and reload apply. */
  readonly ready: boolean;
  /** The menu's changes need no control, only a live tab. */
  readonly streaming: boolean;
  readonly onCommand: PreviewStreamRef["command"];
}) {
  const url = browserTabUrl(tab);
  // Null while not editing, so agent navigation keeps the field current.
  const [draft, setDraft] = useState<string | null>(null);
  const submit = () => {
    const value = draft ?? url;
    setDraft(null);
    try {
      onCommand({ type: "navigate", url: normalizePreviewUrl(value) });
    } catch {
      Alert.alert("Could not open this address", "Enter an http or https URL.");
    }
  };
  return (
    <View className="flex-row items-center gap-1 px-2 pb-2">
      <ControlPill
        icon="chevron.left"
        accessibilityLabel="Back"
        disabled={!ready || !tab.canGoBack}
        onPress={() => onCommand({ type: "history", delta: -1 })}
      />
      <ControlPill
        icon="chevron.right"
        accessibilityLabel="Forward"
        disabled={!ready || !tab.canGoForward}
        onPress={() => onCommand({ type: "history", delta: 1 })}
      />
      <TextInput
        accessibilityLabel="Address"
        editable={ready}
        value={draft ?? url}
        onChangeText={setDraft}
        onBlur={() => setDraft(null)}
        onSubmitEditing={submit}
        placeholder="Enter a URL"
        placeholderTextColorClassName="accent-placeholder"
        autoCapitalize="none"
        autoCorrect={false}
        keyboardType="url"
        returnKeyType="go"
        selectTextOnFocus
        className="h-10 min-w-0 flex-1 rounded-full border border-input-border bg-input px-4 font-sans text-sm text-foreground"
      />
      <ControlPill
        icon="arrow.clockwise"
        accessibilityLabel="Reload page"
        disabled={!ready}
        onPress={() => onCommand({ type: "reload" })}
      />
      <BrowserTabMenu environmentId={environmentId} tab={tab} disabled={!streaming} />
    </View>
  );
}
