import type { ThreadListV2ListItem } from "../threads/threadListV2";
import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  ProjectId,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
} from "@t3tools/contracts";
import type { EnvironmentThreadSearchMatch } from "@t3tools/client-runtime/state/thread-search";
import { Atom, AtomRegistry } from "effect/reactivity";
import { act, type ComponentProps, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import {
  v2ShellSnapshot,
  v2ThreadShell,
} from "../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures";

const state = vi.hoisted(() => ({
  container: ({ children }: { children?: ReactNode }) => children ?? null,
  snapshots: undefined as
    | ((id: EnvironmentId) => Atom.Writable<OrchestrationV2ShellSnapshot | null>)
    | undefined,
  listeners: new Map<string, (event: { data: { closing: boolean } }) => void>(),
  matches: [] as EnvironmentThreadSearchMatch[],
  listRenders: 0,
  selected: [] as string[],
}));

vi.mock("expo-crypto", () => ({ randomUUID: () => globalThis.crypto.randomUUID() }));
vi.mock("expo", () => ({ requireOptionalNativeModule: () => null }));
vi.mock("expo-blur", () => ({ BlurView: () => null }));
vi.mock("expo-constants", () => ({ default: { expoConfig: { extra: {} } } }));
vi.mock("expo-glass-effect", () => ({
  GlassView: () => null,
  isGlassEffectAPIAvailable: () => false,
}));
vi.mock("expo-secure-store", () => ({
  getItemAsync: async () => null,
  setItemAsync: async () => {},
  deleteItemAsync: async () => {},
}));
vi.mock("react-native-keyboard-controller", () => ({ useKeyboardState: () => false }));
vi.mock("uniwind", () => ({ withUniwind: (component: unknown) => component }));
vi.mock("../../components/ui/Toast", () => ({ toast: () => {}, ToastViewport: () => null }));
vi.mock("../threads/threadListAnimations", () => ({ THREAD_SIDEBAR_ROW_LAYOUT: undefined }));
vi.mock("./ThreadUndoNotice", () => ({ ThreadUndoNotice: () => null }));
// No older settled shelf here: every thread under test is loaded.
vi.mock("../../state/olderSettledShelf", () => ({
  useOlderSettledShelf: () => ({
    environments: new Map(),
    visibleCount: 0,
    pages: new Map(),
    grow: () => {},
    isListed: () => true,
    unloadedCount: () => 0,
    loading: false,
    failed: false,
    retry: () => {},
  }),
  useOlderSettledShelfGrowth: () => {},
}));
vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
  View: state.container,
  ScrollView: state.container,
  useWindowDimensions: () => ({ width: 390, height: 844 }),
}));
vi.mock("@react-navigation/native", () => ({
  useFocusEffect: () => undefined,
  useNavigation: () => ({
    isFocused: () => true,
    navigate: () => undefined,
    addListener: (type: string, listener: (event: { data: { closing: boolean } }) => void) => {
      state.listeners.set(type, listener);
      return () => state.listeners.delete(type);
    },
  }),
}));
vi.mock("react-native-screens", () => ({ ScrollViewMarker: state.container }));
vi.mock("react-native-gesture-handler", () => ({
  GestureDetector: state.container,
  useNativeGesture: () => null,
}));
vi.mock("react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ top: 0, bottom: 0, left: 0, right: 0 }),
}));
const legendList = vi.hoisted(
  () =>
    (props: {
      data: readonly ThreadListV2ListItem[];
      renderItem: (input: { item: ThreadListV2ListItem }) => ReactNode;
    }) => {
      state.listRenders++;
      return props.data.map((item) => <div key={item.key}>{props.renderItem({ item })}</div>);
    },
);
vi.mock("@legendapp/list/react-native", () => ({ LegendList: legendList }));
vi.mock("@legendapp/list/reanimated", () => ({ AnimatedLegendList: legendList }));
vi.mock("../../native/native-layout-metrics", () => ({
  useDeviceType: () => "phone",
  useNativeColumnLayoutMetrics: () => null,
  useNativeLayoutMetrics: () => null,
}));
vi.mock("../../native/native-glass", () => ({ NATIVE_LIQUID_GLASS_SUPPORTED: false }));
vi.mock("../../native/NativeWorkspaceColumns", () => ({
  useNativeWorkspaceColumnsSupported: () => false,
}));
vi.mock("../../native/StackHeader", () => ({ NativeStackScreenOptions: () => null }));
vi.mock("../../components/ui/Toolbar", () => ({
  Toolbar: Object.assign(state.container, { Button: () => null }),
}));
vi.mock("../../components/ui/AndroidScreenHeader", () => ({ AndroidScreenHeader: () => null }));
vi.mock("../../components/ui/useAndroidControlSizing", () => ({
  useAndroidControlSizing: () => ({ fabClearance: 0 }),
}));
vi.mock("../../components/ui/AppActivityIndicator", () => ({ AppActivityIndicator: () => null }));
vi.mock("../../components/ui/FloatingActionButton", () => ({ FloatingActionButton: () => null }));
vi.mock("../../components/ui/EmptyState", () => ({ EmptyState: () => <div>No matches</div> }));
vi.mock("../layout/WorkspaceEmptyDetail", () => ({ WorkspaceEmptyDetail: () => null }));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  useAdaptiveWorkspaceLayout: () => ({
    layout: { usesSplitView: false },
    panes: {},
    primarySidebarSearchQuery: "",
  }),
}));
vi.mock("../workspace/WorkspaceTabsContext", () => ({ useWorkspaceTabs: () => null }));
vi.mock("../updates/app-updates", () => ({
  checkForAppUpdateOnLaunch: () => undefined,
  startAppUpdateForegroundRecheck: () => undefined,
}));
vi.mock("./AndroidHomeFab", () => ({ AndroidHomeFabLayout: state.container }));
vi.mock("./WorkspaceConnectionTitle", () => ({ getConnectionAwareBrandHeaderOptions: () => ({}) }));
vi.mock("./FabScrollContext", () => ({ useFabScroll: () => undefined }));
vi.mock("./ThreadListFilters", () => ({ ThreadListFilters: () => null }));
vi.mock("./thread-swipe-actions", () => ({
  SwipeableScrollGateProvider: state.container,
  useSwipeableScrollGate: () => ({ swipeEnabled: false, scrollGateHandlers: {} }),
}));
vi.mock("./useThreadListActions", () => ({ useThreadListActions: () => ({}) }));
vi.mock("./usePendingTaskListActions", () => ({ usePendingTaskListActions: () => ({}) }));
vi.mock("../workspace/useSelectWorkspaceDetail", () => ({
  useSelectWorkspaceDetail:
    () => (detail: { readonly params: { environmentId: string; threadId: string } }) => {
      state.selected = [detail.params.environmentId, detail.params.threadId];
    },
}));
vi.mock("./home-list-options", () => ({
  useHomeListOptions: () => ({
    options: {
      selectedEnvironmentId: null,
      selectedProjectKey: null,
      projectSortOrder: "updated_at",
      projectGroupingMode: "separate",
    },
  }),
}));
vi.mock("./HomeHeader", () => ({
  HomeHeader: (props: { onSearchQueryChange: (query: string) => void }) => (
    <>
      <button onClick={() => props.onSearchQueryChange("Remote audit")}>Title search</button>
      <button onClick={() => props.onSearchQueryChange("contents")}>Content search</button>
      <button onClick={() => props.onSearchQueryChange("")}>Clear search</button>
    </>
  ),
}));
vi.mock("../../state/threads", async () => {
  const { Atom } = await import("effect/reactivity");
  const Option = await import("effect/Option");
  const { PrimaryConnectionTarget } =
    await import("../../../../../packages/client-runtime/src/connection/model");
  const { createEnvironmentThreadShellAtoms } =
    await import("../../../../../packages/client-runtime/src/state/threadShell");
  const { v2ShellSnapshot } =
    await import("../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures");
  const { EnvironmentId } = await import("@t3tools/contracts");
  const snapshots = Atom.family((_id: EnvironmentId) =>
    Atom.make<OrchestrationV2ShellSnapshot | null>(v2ShellSnapshot),
  );
  state.snapshots = snapshots;
  return {
    startOpeningEnvironmentThread: () => {},
    environmentThreadShells: createEnvironmentThreadShellAtoms({
      snapshotAtom: snapshots,
      catalogValueAtom: Atom.make({
        isReady: true,
        entries: new Map(
          ["destination", "other"].map((value) => {
            const environmentId = EnvironmentId.make(value);
            return [
              environmentId,
              {
                enabled: true,
                profile: Option.none(),
                target: new PrimaryConnectionTarget({
                  environmentId,
                  label: value,
                  httpBaseUrl: "https://example.test",
                  wsBaseUrl: "wss://example.test",
                }),
              },
            ];
          }),
        ),
      }),
    }),
  };
});
vi.mock("../../state/entities", () => ({ useProjects: () => [] }));
vi.mock("../../state/workspace", () => ({
  useWorkspaceState: () => ({ environments: [], state: { hasConnections: true } }),
}));
vi.mock("../../state/use-remote-environment-registry", () => ({
  useSavedRemoteConnections: () => ({ savedConnectionsById: {} }),
}));
vi.mock("../../state/use-pending-new-tasks", () => ({ usePendingNewTasks: () => [] }));
vi.mock("../../state/use-thread-outbox", () => ({ useQueuedThreadKeys: () => new Set() }));
vi.mock("../../state/queries", () => ({
  useThreadSearch: (_environmentIds: unknown, query: string) => ({
    matches: state.matches,
    query: query.trim(),
    isPending: false,
  }),
}));
vi.mock("../../state/thread-order", () => ({ usePendingThreadOrder: () => null }));
vi.mock("../../state/server", async () => {
  const { Atom } = await import("effect/reactivity");
  const ids = new Set(["destination", "other"]);
  return {
    threadListEnvironmentsAtom: Atom.make({
      providersByEnvironmentId: new Map(),
      machineByEnvironmentId: new Map(),
      settlementEnvironmentIds: ids,
      snoozeEnvironmentIds: ids,
      pinningEnvironmentIds: ids,
      autoSettleOptOutEnvironmentIds: ids,
      pinReorderEnvironmentIds: ids,
      activeReorderEnvironmentIds: ids,
      titleRegenerationEnvironmentIds: ids,
    }),
  };
});
vi.mock("../threads/thread-provider-instance", () => ({
  useThreadRowProviderInstanceResolver: () => () => null,
}));
vi.mock("../threads/use-open-thread-pull-requests", () => ({
  useOpenThreadPullRequests: () => undefined,
}));
vi.mock("../threads/use-thread-list-v2-shelf-preferences", () => ({
  useThreadListV2ShelfPreferences: () => ({
    loaded: true,
    workingShelfEnabled: false,
    settledShelfExpanded: true,
    snoozedShelfExpanded: true,
    noProjectShelfExpanded: true,
  }),
}));
vi.mock("../keyboard/threadKeyboardShortcuts", () => ({ useThreadJumpShortcuts: () => undefined }));
vi.mock("../threads/thread-list-v2-items", () => ({
  ThreadListV2Row: (props: {
    thread: { environmentId: string; id: string; title: string };
    canMoveUp: boolean;
    canMoveDown: boolean;
    onSelectThread: (thread: unknown) => void;
  }) => (
    <button
      data-thread={`${props.thread.environmentId}:${props.thread.id}`}
      data-moves={`${props.canMoveUp}:${props.canMoveDown}`}
      onClick={() => props.onSelectThread(props.thread)}
    >
      {props.thread.title}
    </button>
  ),
  ThreadListV2PendingRow: () => null,
  ThreadListV2SettledShelfHeader: () => null,
  ThreadListV2ShowMoreRow: () => null,
  ThreadListV2SnoozedShelfHeader: () => null,
  ThreadListV2NoProjectShelfHeader: () => null,
  ThreadListV2WorkingShelfHeader: () => null,
}));

import { HomeRouteScreen } from "./HomeRouteScreen";
import { HomeScreen } from "./HomeScreen";
import { presentThreadShell } from "@t3tools/client-runtime/state/models";

const destination = EnvironmentId.make("destination");
const other = EnvironmentId.make("other");
const rawChild = {
  ...v2ThreadShell,
  id: ThreadId.make("same-id"),
  title: "Remote audit",
  projectId: ProjectId.make("project"),
  delegatedFrom: {
    environmentId: EnvironmentId.make("offline-parent"),
    threadId: ThreadId.make("parent"),
    title: "Parent",
  },
};
const rawFork = {
  ...rawChild,
  id: ThreadId.make("fork"),
  title: "User fork",
  lineage: {
    ...rawChild.lineage,
    parentThreadId: rawChild.id,
    relationshipToParent: "fork" as const,
  },
};
let registry: AtomRegistry.AtomRegistry;
let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  registry = AtomRegistry.make();
  state.matches = [];
  state.listeners.clear();
  state.listRenders = 0;
  state.selected = [];
  registry.set(state.snapshots!(destination), {
    ...v2ShellSnapshot,
    threads: [
      rawChild,
      rawFork,
      { ...rawChild, id: ThreadId.make("archived"), archivedAt: rawChild.createdAt },
      {
        ...rawChild,
        id: ThreadId.make("local-child"),
        lineage: { ...rawChild.lineage, relationshipToParent: "subagent" },
      },
    ],
  });
  registry.set(state.snapshots!(other), {
    ...v2ShellSnapshot,
    threads: [{ ...rawChild, title: "Other destination" }],
  });
  container = document.createElement("div");
  root = createRoot(container);
});
afterEach(() => {
  act(() => root.unmount());
  registry.dispose();
});
function mount() {
  act(() =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <HomeRouteScreen />
      </RegistryContext.Provider>,
    ),
  );
}
function click(label: string) {
  const button = [...container.querySelectorAll("button")].find(
    (element) => element.textContent === label,
  );
  expect(button).toBeDefined();
  act(() => button!.click());
}
function rows() {
  return [...container.querySelectorAll("[data-thread]")].map((row) =>
    row.getAttribute("data-thread"),
  );
}

it("phone title/content search finds qualified offline children, then hides them on clear", () => {
  mount();
  expect(rows()).toEqual(["destination:fork"]);
  click("Title search");
  expect(rows()).toEqual(["destination:same-id"]);
  click("Remote audit");
  expect(state.selected).toEqual(["destination", "same-id"]);
  state.matches = [
    {
      environmentId: other,
      threadId: rawChild.id,
      source: "assistant",
      snippet: "contents",
    } as EnvironmentThreadSearchMatch,
  ];
  click("Content search");
  expect(rows()).toEqual(["other:same-id"]);
  click("Clear search");
  expect(rows()).toEqual(["destination:fork"]);
});

it("phone keeps the visibility subscription guard while searching", () => {
  mount();
  click("Title search");
  act(() => state.listeners.get("transitionEnd")!({ data: { closing: true } }));
  const before = state.listRenders;
  act(() =>
    registry.set(state.snapshots!(destination), {
      ...v2ShellSnapshot,
      threads: [{ ...rawChild, title: "Remote audit refreshed" }, rawFork],
    }),
  );
  expect(state.listRenders).toBe(before);
  act(() => state.listeners.get("focus")!({ data: { closing: false } }));
  expect(rows()).toEqual(["destination:same-id"]);
  expect(container.textContent).toContain("Remote audit refreshed");
});

it("HomeScreen excludes local children from search even when given full shells", () => {
  const local = {
    ...rawChild,
    lineage: { ...rawChild.lineage, relationshipToParent: "subagent" as const },
  };
  const shells = [rawChild, local].map((raw) => presentThreadShell(destination, raw));
  const props = {
    projects: [],
    threads: shells,
    navigationThreads: [],
    pendingTasks: [],
    catalogState: { hasConnections: true },
    savedConnectionsById: {},
    environments: [],
    searchQuery: "Remote audit",
    selectedEnvironmentId: null,
    selectedProjectKey: null,
    projectGroupingMode: "separate",
  } as unknown as ComponentProps<typeof HomeScreen>;
  act(() =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <HomeScreen {...props} />
      </RegistryContext.Provider>,
    ),
  );
  expect(rows()).toEqual(["destination:same-id"]);
});

it("HomeScreen move availability stays based on default navigation during search", () => {
  const child = presentThreadShell(destination, rawChild);
  const fork = presentThreadShell(destination, rawFork);
  const props = {
    projects: [],
    threads: [child, fork],
    navigationThreads: [fork],
    pendingTasks: [],
    catalogState: { hasConnections: true },
    savedConnectionsById: {},
    environments: [],
    searchQuery: "User fork",
    selectedEnvironmentId: null,
    selectedProjectKey: null,
    projectGroupingMode: "separate",
  } as unknown as ComponentProps<typeof HomeScreen>;
  act(() =>
    root.render(
      <RegistryContext.Provider value={registry}>
        <HomeScreen {...props} />
      </RegistryContext.Provider>,
    ),
  );
  expect(rows()).toEqual(["destination:fork"]);
  expect(container.querySelector("[data-thread]")?.getAttribute("data-moves")).toBe("false:false");
});

it("phone default navigation does not rebuild for hidden-child shell updates", () => {
  mount();
  const before = state.listRenders;
  act(() =>
    registry.set(state.snapshots!(destination), {
      ...v2ShellSnapshot,
      threads: [{ ...rawChild, title: "Remote audit refreshed" }, rawFork],
    }),
  );
  expect(rows()).toEqual(["destination:fork"]);
  expect(state.listRenders).toBe(before);
});
