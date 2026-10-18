import { act, cloneElement, type ReactElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { EnvironmentId, NodeId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import * as DateTime from "effect/DateTime";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  subagents: [] as unknown[],
  configs: new Map<string, unknown>(),
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
});

const remoteAgent = {
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
