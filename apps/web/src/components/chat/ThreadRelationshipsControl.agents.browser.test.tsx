import { act, cloneElement, type ReactElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
  EnvironmentId,
  ThreadId,
  type ModelSelection,
  type ProviderOptionDescriptor,
  type OrchestrationV2ContextTransfer,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import "~/index.css";

const state = vi.hoisted(() => ({
  stopWorkflowLabel: "environment-data:orchestration:stop-workflow",
  projection: null as unknown,
  navigate: vi.fn(),
  shells: [] as unknown[],
  projects: [] as unknown[],
  configs: new Map<string, unknown>(),
  showTooltips: false,
  command: vi.fn().mockResolvedValue({ _tag: "Success" }),
  workflowCommand: vi.fn().mockResolvedValue({ _tag: "Success" }),
  toast: vi.fn(),
}));

vi.mock(import("@tanstack/react-router"), async (original) => ({
  ...(await original()),
  useNavigate: (() => state.navigate) as never,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadProjection: (() => ({ projection: state.projection })) as never,
  useThreadRelationshipShells: ((ref: { readonly environmentId: string }) =>
    (state.shells as ReadonlyArray<{ readonly environmentId: string; readonly source: unknown }>)
      .filter((shell) => shell.environmentId === ref.environmentId)
      .map((shell) => shell.source)) as never,
  useProjects: (() => state.projects) as never,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironmentProviders: (() =>
    new Map(
      [...state.configs].map(([id, config]) => [id, (config as { providers: unknown }).providers]),
    )) as never,
}));
vi.mock(import("../../lib/archivedThreadsState"), async (original) => ({
  ...(await original()),
  useArchivedThreadSnapshots: (() => ({ snapshots: [] })) as never,
}));
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: { label?: string }) =>
    command.label === state.stopWorkflowLabel ? state.workflowCommand : state.command) as never,
}));
vi.mock(import("../ui/toast"), async (original) => {
  const actual = await original();
  return { ...actual, toastManager: { ...actual.toastManager, add: state.toast } as never };
});
vi.mock(import("../../state/orchestration"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const granted = Atom.make(true);
  const stopWorkflow = actual.orchestrationEnvironment.stopWorkflow;
  return {
    ...actual,
    orchestrationEnvironment: Object.assign(Object.create(actual.orchestrationEnvironment), {
      stopWorkflow: Object.assign(Object.create(stopWorkflow), {
        label: state.stopWorkflowLabel,
        permissionAtom: () => granted,
      }),
    }),
  };
});
// Tooltip and hover card content renders inline so the suite can read every row's agent
// details across many states without hovering each row.
vi.mock(import("../ui/tooltip"), async () => {
  const { cloneElement } = await import("react");
  return {
    Tooltip: (({ children }: { children: ReactNode }) => children) as never,
    TooltipTrigger: (({ render, children }: { render: ReactElement; children: ReactNode }) =>
      cloneElement(render, {}, children)) as never,
    TooltipPopup: (({ children }: { children: ReactNode }) =>
      state.showTooltips ? children : null) as never,
  };
});

import { ThreadRelationshipsPanel } from "./ThreadRelationshipsControl";

let root: Root;
let container: HTMLDivElement;
let current: ReactElement;

async function render(element: ReactElement) {
  current = element;
  await act(async () => root.render(element));
}
// The mocked hooks read `state` while rendering, so a new element picks up changes.
const update = () => act(async () => root.render(cloneElement(current)));
const click = (element: HTMLElement) => act(async () => element.click());
const buttons = () => [...container.querySelectorAll("button")];
const allByLabel = (label: string) =>
  buttons().filter((button) => button.getAttribute("aria-label") === label);
function byLabel(label: string) {
  const matches = allByLabel(label);
  if (matches.length !== 1)
    throw new Error(`Expected one "${label}" button, got ${matches.length}`);
  return matches[0]!;
}
const sectionToggles = (expanded: boolean) =>
  buttons().filter(
    (button) =>
      button.getAttribute("aria-expanded") === String(expanded) &&
      !button.hasAttribute("aria-label"),
  );
function sectionToggle(expanded: boolean) {
  const [toggle] = sectionToggles(expanded);
  if (!toggle) throw new Error(`No ${expanded ? "expanded" : "collapsed"} section toggle`);
  return toggle;
}
const heading = () => container.querySelector("h3")?.textContent;
const buttonsWithLabel = (matches: (label: string) => boolean) =>
  buttons().filter((button) => matches(button.getAttribute("aria-label") ?? ""));
// Joins rendered text nodes; `visibleOnly` drops screen-reader-only text.
function textNodes(separator: string, visibleOnly = false) {
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
  const parts: string[] = [];
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    if (visibleOnly && node.parentElement?.closest(".sr-only")) continue;
    parts.push(node.textContent ?? "");
  }
  return parts.join(separator);
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
  state.shells = [];
  state.projects = [];
  state.configs.clear();
  state.showTooltips = false;
  state.navigate.mockClear();
  state.command.mockClear();
  state.workflowCommand.mockReset().mockResolvedValue({ _tag: "Success" });
  state.toast.mockClear();
  state.projection = null;
});

it("shows a child delegated to a linked environment and opens it there", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.navigate.mockClear();
  const agent = {
    id: "remote-agent",
    childThreadId: null,
    remoteChild: {
      environmentId: EnvironmentId.make("cavallo"),
      threadId: ThreadId.make("remote-child"),
      label: "cavallo",
    },
    origin: "app_owned",
    driver: "cursor",
    providerInstanceId: "cursor",
    title: "Remote roast task",
    prompt: "Roast the draft",
    model: "grok-4.7",
    // The delegated task settled; the child is on a live follow-up there.
    status: "completed",
    progress: null,
    result: null,
    startedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z"),
    completedAt: DateTime.makeUnsafe("2026-09-16T12:01:00Z"),
    updatedAt: DateTime.makeUnsafe("2026-09-16T12:01:00Z"),
  };
  state.shells = [
    {
      environmentId: "cavallo",
      source: {
        id: "remote-child",
        title: "Remote roast thread",
        status: "running",
        activityRunStatus: "running",
        lineage: { parentThreadId: null, relationshipToParent: null },
      },
    },
  ];
  state.configs.set("test", { providers: [] });
  state.configs.set("cavallo", { providers: [] });
  state.projection = {
    thread: {
      id: "parent",
      lineage: { relationshipToParent: null },
      activeProviderThreadId: null,
    },
    runs: [],
    providerThreads: [],
    providerSessions: [],
    contextTransfers: [],
    subagents: [agent],
  };
  await act(async () => {
    renderer = create(
      <ThreadRelationshipsPanel
        environmentId={EnvironmentId.make("test")}
        threadId={ThreadId.make("parent")}
      />,
    );
  });
  const text = renderer.root
    .findAll((node) => typeof node.type === "string")
    .flatMap((node) => node.children.filter((child) => typeof child === "string"))
    .join(" ")
    .replace(/\s+/g, " ");
  // The row takes its title and live status from the remote shell, not the
  // settled task record, and is counted once: not once as a childless runner
  // and again as an active row.
  expect(text).toContain("Remote roast thread");
  expect(text).not.toContain("Remote roast task");
  expect(text).toContain("Lineage · 1 running");
  expect(text).not.toContain("2 running");
  // Stopping it belongs to the owning task, not a local interrupt.
  expect(
    renderer.root.findAllByProps({ "aria-label": "Stop subagent Remote roast thread" }),
  ).toHaveLength(0);
  const rowButton = renderer.root
    .findAllByType("button")
    .find((button) =>
      button
        .findAll((node) => typeof node.type === "string")
        .some((node) => node.children.includes("Remote roast thread")),
    );
  expect(rowButton).toBeDefined();
  expect(rowButton!.props.disabled).not.toBe(true);
  await act(async () => rowButton!.props.onClick());
  expect(state.navigate).toHaveBeenCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "cavallo", threadId: "remote-child" },
  });

  // Without a client connection to cavallo the same row renders disabled.
  state.navigate.mockClear();
  state.configs.delete("cavallo");
  await act(async () => {
    renderer.update(
      <ThreadRelationshipsPanel
        environmentId={EnvironmentId.make("test")}
        threadId={ThreadId.make("parent")}
      />,
    );
  });
  const disabledRowButton = renderer.root
    .findAllByType("button")
    .find((button) =>
      button
        .findAll((node) => typeof node.type === "string")
        .some((node) => node.children.includes("Remote roast thread")),
    );
  expect(disabledRowButton).toBeDefined();
  expect(disabledRowButton!.props.disabled).toBe(true);
  expect(state.navigate).not.toHaveBeenCalled();
});
