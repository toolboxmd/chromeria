import { act, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { EnvironmentId, NodeId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import * as DateTime from "effect/DateTime";
import { afterEach, expect, it, vi } from "vite-plus/test";

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
  TooltipTrigger: ({ render, children }: { render: ReactNode; children: ReactNode }) => (
    <>
      {render}
      {children}
    </>
  ),
  TooltipPopup: () => null,
}));

vi.mock("../ThreadHoverCard", () => ({
  ThreadHoverCardPopup: () => null,
}));

import { NotificationSubagentOpenButton, SubagentNotificationLink } from "./V2LifecycleRow";

let renderer: ReactTestRenderer | undefined;

afterEach(async () => {
  await act(async () => renderer?.unmount());
  renderer = undefined;
  state.subagents = [];
  state.configs.clear();
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
  return renderer!.root
    .findAllByType("button")
    .filter((button) => button.props["aria-label"] === label);
}

it("opens a remote delegated task's notification card in its own environment", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.subagents = [remoteAgent];
  state.configs.set("cavallo", {});
  const onOpenThread = vi.fn();
  await act(async () => {
    renderer = create(
      <SubagentNotificationLink
        parentRef={parentRef}
        subagentId={NodeId.make("remote-agent")}
        outcome="completed"
        createdAt="2026-09-16T12:02:00Z"
        timestampFormat="locale"
        providerStatuses={[]}
        onOpenThread={onOpenThread}
        fallback={<span>fallback</span>}
      />,
    );
  });
  const buttons = buttonsByLabel("Open Remote roast");
  expect(buttons).toHaveLength(1);
  await act(async () => buttons[0]!.props.onClick());
  expect(onOpenThread).toHaveBeenCalledWith(
    ThreadId.make("remote-child"),
    EnvironmentId.make("cavallo"),
  );
});

it("renders no link for a remote notification when the client is not connected there", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  state.subagents = [remoteAgent];
  const onOpenThread = vi.fn();
  await act(async () => {
    renderer = create(
      <NotificationSubagentOpenButton
        parentRef={parentRef}
        subagentId={NodeId.make("remote-agent")}
        onOpenThread={onOpenThread}
      />,
    );
  });
  expect(buttonsByLabel("Open subagent thread")).toHaveLength(0);

  state.configs.set("cavallo", {});
  await act(async () => {
    renderer!.update(
      <NotificationSubagentOpenButton
        parentRef={parentRef}
        subagentId={NodeId.make("remote-agent")}
        onOpenThread={onOpenThread}
      />,
    );
  });
  const buttons = buttonsByLabel("Open subagent thread");
  expect(buttons).toHaveLength(1);
  await act(async () => buttons[0]!.props.onClick({ stopPropagation: vi.fn() }));
  expect(onOpenThread).toHaveBeenCalledWith(
    ThreadId.make("remote-child"),
    EnvironmentId.make("cavallo"),
  );
});
