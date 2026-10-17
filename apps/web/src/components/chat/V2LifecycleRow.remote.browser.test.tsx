import { act, cloneElement, type ReactElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { EnvironmentId, NodeId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import * as DateTime from "effect/DateTime";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  subagents: [] as unknown[],
  configs: new Map<string, unknown>(),
  queries: [] as unknown[],
}));

vi.mock("../../state/threads", async (importOriginal) => {
  const original = await importOriginal<typeof import("../../state/threads")>();
  const { Atom } = await import("effect/reactivity");
  return {
    ...original,
    environmentThreadDetails: {
      ...original.environmentThreadDetails,
      threadAtom: () => Atom.make({ projection: { subagents: state.subagents } }),
    },
  };
});

vi.mock("../../state/entities", async (importOriginal) => {
  const original = await importOriginal<typeof import("../../state/entities")>();
  return {
    ...original,
    useServerConfigs: () => state.configs,
    useThreadShell: () => null,
    useProject: () => null,
  };
});

vi.mock("../ui/tooltip", () => ({
  Tooltip: ({ children }: { children: ReactNode }) => children,
  TooltipTrigger: ({ render, children }: { render: ReactElement; children: ReactNode }) =>
    cloneElement(render, {}, children),
  TooltipPopup: () => null,
}));

vi.mock("../ThreadHoverCard", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ThreadHoverCard")>()),
  ThreadHoverCardPopup: () => null,
}));

vi.mock("../../state/remoteTasks", async () => {
  const { Atom, AsyncResult } = await import("effect/reactivity");
  return {
    remoteTasks: {
      view: (input: unknown) => {
        state.queries.push(input);
        return Atom.make(
          AsyncResult.success({
            taskId: "remote-agent",
            environmentId: "cavallo",
            threadId: "remote-child",
            snapshot: {
              title: "Source inspected work",
              observedAt: "2026-10-17T00:00:00Z",
              workState: "working",
              pendingChildren: 1,
              queuedRuns: 2,
              requestCount: 0,
              recoveryWaiting: false,
              incomplete: false,
            },
            childrenTruncated: false,
            children: [],
            messages: [{ id: "source-message", role: "assistant", text: "Read through A" }],
            nextCursor: null,
            nextTextOffset: null,
          }),
        );
      },
      stop: { permissionAtom: () => Atom.make(false) },
    },
  };
});
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("../ui/dialog", () => ({
  Dialog: ({ children }: { children: ReactNode }) => children,
  DialogPopup: ({ children }: { children: ReactNode }) => children,
  DialogTitle: ({ children }: { children: ReactNode }) => children,
}));
vi.mock("../ui/button", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ui/button")>()),
  Button: (props: Record<string, unknown>) => <button {...props} />,
}));
import { BackgroundSubagentOpenButton } from "./BackgroundSubagentOpenButton";
import { NotificationSubagentOpenButton, SubagentNotificationLink } from "./V2LifecycleRow";

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  state.subagents = [];
  state.configs = new Map();
  state.queries = [];
});

const remoteAgent = {
  threadId: ThreadId.make("parent"),
  id: NodeId.make("remote-agent"),
  childThreadId: null,
  remoteChild: {
    environmentId: EnvironmentId.make("cavallo"),
    threadId: ThreadId.make("remote-child"),
    label: "cavallo",
  },
  origin: "app_owned",
  driver: "cursor",
  providerInstanceId: "cursor",
  title: "Remote roast",
  prompt: "Roast the draft",
  model: "grok-4.7",
  modelSelection: undefined,
  status: "completed",
  progress: null,
  result: "Done",
  startedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z"),
  completedAt: DateTime.makeUnsafe("2026-09-16T12:01:00Z"),
  updatedAt: DateTime.makeUnsafe("2026-09-16T12:01:00Z"),
};

const parentRef = scopeThreadRef(EnvironmentId.make("test"), ThreadId.make("parent"));

function buttonsByLabel(label: string) {
  return [...container.querySelectorAll("button")].filter(
    (button) => button.getAttribute("aria-label") === label,
  );
}

it("opens a remote delegated task's notification card in its own environment", async () => {
  state.subagents = [remoteAgent];
  state.configs = new Map([["cavallo", {}]]);
  const onOpenThread = vi.fn();
  await act(async () => {
    root.render(
      <SubagentNotificationLink
        parentRef={parentRef}
        subagentId={NodeId.make("remote-agent")}
        outcome="completed"
        createdAt="2026-09-16T12:02:00Z"
        timestampFormat="locale"
        onOpenThread={onOpenThread}
        fallback={<span>fallback</span>}
      />,
    );
  });
  const buttons = buttonsByLabel("Open Remote roast");
  expect(buttons).toHaveLength(1);
  await act(async () => buttons[0]!.click());
  expect(onOpenThread).toHaveBeenCalledWith(
    ThreadId.make("remote-child"),
    EnvironmentId.make("cavallo"),
  );
});

it("renders no link for a remote notification when the client is not connected there", async () => {
  state.subagents = [remoteAgent];
  const onOpenThread = vi.fn();
  const render = () =>
    act(async () => {
      root.render(
        <NotificationSubagentOpenButton
          parentRef={parentRef}
          subagentId={NodeId.make("remote-agent")}
          onOpenThread={onOpenThread}
        />,
      );
    });
  await render();
  expect(buttonsByLabel("Open subagent thread")).toHaveLength(0);

  // The native config atom publishes a new snapshot when membership changes.
  // In-place edits hide those changes from the compiler's cached consumer.
  state.configs = new Map([["cavallo", {}]]);
  await render();
  const buttons = buttonsByLabel("Open subagent thread");
  expect(buttons).toHaveLength(1);
  await act(async () => buttons[0]!.click());
  expect(onOpenThread).toHaveBeenCalledWith(
    ThreadId.make("remote-child"),
    EnvironmentId.make("cavallo"),
  );

  onOpenThread.mockClear();
  state.configs = new Map();
  await render();
  expect(buttonsByLabel("Open subagent thread")).toHaveLength(0);
  expect(onOpenThread).not.toHaveBeenCalled();
});

const entries = ["transcript", "notification", "background"] as const;
it.each([false, true].flatMap((connected) => entries.map((entry) => ({ connected, entry }))))(
  "opens $entry through A for a negotiated job when B connected=$connected",
  async ({ connected, entry }) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    state.subagents = [
      {
        ...remoteAgent,
        remoteChild: { ...remoteAgent.remoteChild, launch: { lifecycleVersion: 1 } },
      },
    ];
    if (connected) state.configs.set("cavallo", {});
    const onOpenThread = vi.fn();
    const common = { parentRef, subagentId: NodeId.make("remote-agent"), onOpenThread };
    await act(async () => {
      renderer = create(
        entry === "transcript" ? (
          <SubagentNotificationLink
            {...common}
            outcome="completed"
            createdAt="2026-09-16T12:02:00Z"
            timestampFormat="locale"
            providerStatuses={[]}
            fallback={<span>fallback</span>}
          />
        ) : entry === "notification" ? (
          <NotificationSubagentOpenButton {...common} />
        ) : (
          <BackgroundSubagentOpenButton
            parentRef={parentRef}
            item={{
              taskId: NodeId.make("remote-agent"),
              label: "Remote work",
              childThreadId: ThreadId.make("remote-child"),
              childEnvironmentId: EnvironmentId.make("cavallo"),
            }}
            onOpenThread={onOpenThread}
          />
        ),
      );
    });
    const button = buttonsByLabel(
      entry === "transcript" ? "Open Remote roast" : "Open subagent thread",
    )[0];
    expect(button).toBeDefined();
    await act(async () => button!.props.onClick({ stopPropagation: vi.fn() }));
    expect(onOpenThread).not.toHaveBeenCalled();
    expect(state.queries).toContainEqual({
      environmentId: EnvironmentId.make("test"),
      input: {
        parentThreadId: ThreadId.make("parent"),
        taskId: NodeId.make("remote-agent"),
        after: 0,
        textOffset: 0,
      },
    });
    expect(JSON.stringify(renderer!.toJSON())).toContain("Read through A");
  },
);

it.each(
  [false, true].flatMap((connected) =>
    [false, true].flatMap((local) => entries.map((entry) => ({ connected, local, entry }))),
  ),
)(
  "retains local=$local $entry navigation with B connected=$connected",
  async ({ connected, local, entry }) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    state.subagents = [
      local
        ? {
            ...remoteAgent,
            childThreadId: ThreadId.make("local-child"),
            remoteChild: undefined,
          }
        : remoteAgent,
    ];
    if (connected) state.configs.set("cavallo", {});
    const onOpenThread = vi.fn();
    const common = { parentRef, subagentId: NodeId.make("remote-agent"), onOpenThread };
    await act(async () => {
      renderer = create(
        entry === "transcript" ? (
          <SubagentNotificationLink
            {...common}
            outcome="completed"
            createdAt="2026-09-16T12:02:00Z"
            timestampFormat="locale"
            providerStatuses={[]}
            fallback={<span>fallback</span>}
          />
        ) : entry === "notification" ? (
          <NotificationSubagentOpenButton {...common} />
        ) : (
          <BackgroundSubagentOpenButton
            parentRef={parentRef}
            item={{
              taskId: NodeId.make("remote-agent"),
              label: "Remote work",
              childThreadId: ThreadId.make("remote-child"),
              childEnvironmentId: EnvironmentId.make("cavallo"),
            }}
            onOpenThread={onOpenThread}
          />
        ),
      );
    });
    const buttons = buttonsByLabel(
      entry === "transcript" ? "Open Remote roast" : "Open subagent thread",
    );
    expect(buttons.length).toBe(local || connected ? 1 : 0);
    if (buttons[0]) {
      await act(async () => buttons[0]!.props.onClick({ stopPropagation: vi.fn() }));
      if (local) expect(onOpenThread).toHaveBeenCalledWith(ThreadId.make("local-child"));
      else
        expect(onOpenThread).toHaveBeenCalledWith(
          ThreadId.make("remote-child"),
          EnvironmentId.make("cavallo"),
        );
    }
    expect(state.queries).toEqual([]);
  },
);
