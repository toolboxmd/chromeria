import { act, render } from "../../../test/render";
import { cloneElement, type ReactElement, type ReactNode } from "react";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { expect, it, vi } from "vite-plus/test";

import "~/index.css";

const state = vi.hoisted(() => ({
  stopWorkflowLabel: "environment-data:orchestration:stop-workflow",
  projection: null as unknown,
  navigate: vi.fn(),
  shells: [] as unknown[],
  projects: [] as unknown[],
  configs: new Map<string, unknown>(),
  disabledEnvironments: new Set<string>(),
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
    (
      state.shells as ReadonlyArray<{ readonly environmentId: string; readonly source: unknown }>
    ).map(({ environmentId, source }) =>
      environmentId === ref.environmentId ? source : { environmentId, shell: source },
    )) as never,
  useProjects: (() => state.projects) as never,
  useServerConfigs: (() =>
    new Map([...state.configs].filter(([id]) => !state.disabledEnvironments.has(id)))) as never,
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

it("shows a child delegated to a linked environment and opens it there", async () => {
  using _cleanup = {
    [Symbol.dispose]: () => {
      vi.restoreAllMocks();
      vi.unstubAllGlobals();
    },
  };

  state.shells = [];
  state.projects = [];
  state.configs.clear();
  state.disabledEnvironments.clear();
  state.showTooltips = false;
  state.navigate.mockClear();
  state.command.mockClear();
  state.workflowCommand.mockReset().mockResolvedValue({ _tag: "Success" });
  state.toast.mockClear();
  state.projection = null;

  await using screen = await render(null);
  const container = screen.container;
  let current: ReactElement;

  async function rerender(element: ReactElement) {
    current = element;
    await act(async () => screen.rerender(element));
  }
  // The mocked hooks read `state` while rendering, so a new element picks up changes.
  const update = () => act(async () => screen.rerender(cloneElement(current)));
  const click = (element: HTMLElement) => act(async () => element.click());
  const buttons = () => [...container.querySelectorAll("button")];
  const allByLabel = (label: string) =>
    buttons().filter((button) => button.getAttribute("aria-label") === label);

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
  await rerender(
    <ThreadRelationshipsPanel
      environmentId={EnvironmentId.make("test")}
      threadId={ThreadId.make("parent")}
      showLineage={true}
      showSideChats={true}
    />,
  );
  const text = container.textContent!.replace(/\s+/g, " ");
  // The row takes its title and live status from the remote shell, not the
  // settled task record, and is counted once: not once as a childless runner
  // and again as an active row.
  expect(text).toContain("Remote roast thread");
  expect(text).not.toContain("Remote roast task");
  expect(text).toContain("Lineage · 1 running");
  expect(text).not.toContain("2 running");
  // Stopping it belongs to the owning task, not a local interrupt.
  expect(allByLabel("Stop subagent Remote roast thread")).toHaveLength(0);
  const rowButton = buttons().find((button) => button.textContent?.includes("Remote roast thread"));
  expect(rowButton).toBeDefined();
  expect(rowButton!.disabled).toBe(false);
  expect(rowButton!.getAttribute("aria-disabled")).toBeNull();
  await click(rowButton!);
  expect(state.navigate).toHaveBeenCalledWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "cavallo", threadId: "remote-child" },
  });

  // Disabling a saved connection retains its cached providers.
  state.navigate.mockClear();
  state.disabledEnvironments.add("cavallo");
  await update();
  const disabledRowButton = buttons().find((button) =>
    button.textContent?.includes("Remote roast thread"),
  );
  expect(disabledRowButton).toBeDefined();
  expect(disabledRowButton!.disabled).toBe(false);
  expect(disabledRowButton!.getAttribute("aria-disabled")).toBe("true");
  await click(disabledRowButton!);
  expect(state.navigate).not.toHaveBeenCalled();

  state.disabledEnvironments.delete("cavallo");
  await update();
  const enabledRowButton = buttons().find((button) =>
    button.textContent?.includes("Remote roast thread"),
  )!;
  expect(enabledRowButton.disabled).toBe(false);
  expect(enabledRowButton.getAttribute("aria-disabled")).toBeNull();
  await click(enabledRowButton);
  expect(state.navigate).toHaveBeenCalledExactlyOnceWith({
    to: "/$environmentId/$threadId",
    params: { environmentId: "cavallo", threadId: "remote-child" },
  });

  state.navigate.mockClear();
  state.configs.delete("cavallo");
  await update();
  const disconnectedRowButton = buttons().find((button) =>
    button.textContent?.includes("Remote roast thread"),
  )!;
  expect(disconnectedRowButton.disabled).toBe(false);
  expect(disconnectedRowButton.getAttribute("aria-disabled")).toBe("true");
  await click(disconnectedRowButton);
  expect(state.navigate).not.toHaveBeenCalled();
});

it.each([false, true])("shows live workflow member state with phased=%s", async (phased) => {
  const { ThreadLineageWorkflowRow } = await import("./ThreadLineageWorkflowRow");
  const member = {
    id: "member",
    childThreadId: "member-child",
    title: "Worker",
    model: "test-model",
    status: "completed",
    startedAt: "2026-09-16T12:00:00Z",
    completedAt: "2026-09-16T12:01:00Z",
    progress: null,
    result: null,
    error: null,
  };
  const group = {
    workflow: { id: "workflow", title: "Review", workflowName: "Review", status: "running" },
    phases: phased
      ? [{ index: 0, title: "Work", state: "running", settledCount: 0, members: [member] }]
      : [],
    unphasedMembers: phased ? [] : [member],
  };
  const view = (shell: object) => (
    <ThreadLineageWorkflowRow
      group={group as never}
      header={<span>Review</span>}
      providerInstanceId={"test" as never}
      provider={undefined}
      providers={[]}
      providerEntries={[]}
      driver={undefined}
      onOpenThread={() => {}}
      isThreadAvailable={() => true}
      getThreadShell={() => shell as never}
      stopping={false}
      stopDisabled={false}
    />
  );
  await using screen = await render(view({}));
  const expand = screen.container.querySelector<HTMLButtonElement>('[aria-label="Expand Review"]');
  expect(expand).not.toBeNull();
  await act(async () => expand!.click());
  for (const [shell, label] of [
    [{ status: "running", pendingRuntimeRequest: { kind: "command" } }, "Awaiting approval"],
    [{ status: "running", pendingRuntimeRequest: { kind: "user_input" } }, "Awaiting input"],
    [
      {
        status: "completed",
        activityRunStatus: null,
        pendingBackgroundTasks: [{ taskId: "bg", kind: "command" }],
      },
      "Background active",
    ],
  ] as const) {
    await act(async () => screen.rerender(view(shell)));
    const row = screen.container.querySelector('[aria-label^="Open Worker chat"]');
    expect(row?.textContent).toContain(label);
    const labelElement = [...(row?.querySelectorAll("span") ?? [])].find(
      (element) => element.textContent === label,
    );
    expect(labelElement?.className).not.toContain("sr-only");
  }
  await act(async () =>
    screen.rerender(
      view({ status: "completed", pendingRuntimeRequest: null, pendingBackgroundTasks: [] }),
    ),
  );
  expect(screen.container.textContent).not.toContain("Background active");
});
