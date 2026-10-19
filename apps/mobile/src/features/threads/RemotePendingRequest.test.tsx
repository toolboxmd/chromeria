// @vitest-environment jsdom
import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  ThreadId,
  RuntimeRequestId,
  RunId,
  EventId,
  NodeId,
  type RemotePendingRequestInput,
} from "@t3tools/contracts";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";
import { act, type ComponentProps } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
const state = vi.hoisted(() => ({
  nextKey: 0,
  getRemote: (): unknown => {
    throw new Error("Uninitialized");
  },
}));
vi.mock("../../state/remote-tasks", () => ({
  get remoteTasks() {
    return state.getRemote();
  },
}));
vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
  View: ({ children }: { children?: import("react").ReactNode }) => <div>{children}</div>,
  Text: ({ children }: { children?: import("react").ReactNode }) => <span>{children}</span>,
  Pressable: ({
    children,
    onPress,
    disabled,
    accessibilityState,
    ...props
  }: {
    children?: import("react").ReactNode;
    onPress?: () => void;
    disabled?: boolean;
    accessibilityState?: { selected?: boolean; disabled?: boolean; expanded?: boolean };
  }) => (
    <button
      {...props}
      disabled={disabled}
      aria-pressed={accessibilityState?.selected}
      onClick={onPress}
    >
      {children}
    </button>
  ),
  TextInput: () => <input />,
  ScrollView: ({ children }: { children?: import("react").ReactNode }) => <div>{children}</div>,
}));
vi.mock("expo-crypto", () => ({ randomUUID: () => `action-${++state.nextKey}` }));
import { RemotePendingRequest } from "./RemotePendingRequest";

it.each([false, true])(
  "answers offered empty=%s from the source card, preserves retry identity, and refreshes before a new action",
  async (empty) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const registry = AtomRegistry.make();
    const calls: Array<{ environmentId: EnvironmentId; input: RemotePendingRequestInput }> = [];
    let fail = true;
    const content = {
      requestId: RuntimeRequestId.make("C-request"),
      kind: "user-input" as const,
      revision: 17,
      questions: [
        {
          id: "color",
          header: "Color",
          question: "Which colors?",
          multiSelect: !empty,
          allowCustomAnswer: false,
          options: empty
            ? [
                {
                  label: "Submit empty value",
                  description: "Send an empty string to the extension.",
                  value: "",
                },
              ]
            : [
                { label: "Blue", description: "First" },
                { label: "Green", description: "Second" },
              ],
        },
      ],
    };
    const query = Atom.make(AsyncResult.success(content));
    const mayRespond = Atom.make(true);
    const command = Object.assign(
      createRuntimeCommand(Atom.runtime(Layer.empty), {
        label: "isolated remote client RPC",
        execute: (input: { environmentId: EnvironmentId; input: RemotePendingRequestInput }) =>
          Effect.gen(function* () {
            calls.push(input);
            if (fail) return yield* Effect.fail("Lost acknowledgement after native commit");
            return { sequence: 18 };
          }),
      }),
      { permissionAtom: (_id: EnvironmentId) => mayRespond },
    );
    const remote = { readRequest: () => query, request: command };
    state.getRemote = () => remote;
    let resolved = 0;
    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    const props: ComponentProps<typeof RemotePendingRequest> = {
      environmentId: EnvironmentId.make("A-only-profile"),
      task: { id: NodeId.make("A-task"), threadId: ThreadId.make("A-parent") },
      request: {
        environmentId: EnvironmentId.make("C-no-profile"),
        threadId: ThreadId.make("C-child"),
        requestId: content.requestId,
        revision: 17,
        kind: "user_input",
        responseType: "live",
        route: [
          { environmentId: EnvironmentId.make("C-no-profile"), threadId: ThreadId.make("C-child") },
        ],
      },
      onResolved: () => {
        resolved++;
      },
    };
    const button = (label: string) =>
      [...container.querySelectorAll("button")].find((node) => node.textContent?.includes(label));
    const click = async (label: string) => {
      const target = button(label);
      expect(target, label).toBeDefined();
      await act(async () => target!.click());
    };
    try {
      await act(async () => {
        root.render(
          <RegistryContext.Provider value={registry}>
            <RemotePendingRequest {...props} />
          </RegistryContext.Provider>,
        );
      });
      expect(container.textContent).not.toContain("C-no-profile");
      const sendButton = () => button("Send answer");
      expect(sendButton()?.disabled).toBe(true);
      await click("Request details");
      expect(container.textContent).toContain("C-no-profile");
      await click("Request details");
      expect(container.textContent).not.toContain("C-no-profile");
      if (empty) await click("Submit empty value");
      else {
        await click("Blue");
        await click("Green");
      }
      expect(sendButton()?.disabled).toBe(false);
      const chosen = button(empty ? "Submit empty value" : "Blue");
      expect(chosen?.getAttribute("aria-pressed")).toBe("true");
      await act(async () => registry.set(mayRespond, false));
      expect(sendButton()?.disabled).toBe(true);
      expect(calls).toEqual([]);
      await act(async () => registry.set(mayRespond, true));
      expect(sendButton()?.disabled).toBe(false);
      await click("Send answer");
      expect(resolved).toBe(0);
      expect(calls[0]).toMatchObject({
        environmentId: "A-only-profile",
        input: {
          parentThreadId: "A-parent",
          taskId: "A-task",
          requestTarget: { environmentId: "C-no-profile", threadId: "C-child" },
          operation: {
            type: "respond",
            expectedRevision: 17,
            answers: { color: empty ? "" : ["Blue", "Green"] },
          },
        },
      });
      fail = false;
      await click("Send answer");
      expect(calls[1]).toEqual(calls[0]);
      expect(resolved).toBe(1);
      await click("Refresh request");
      await click("Send answer");
      expect(calls[2]?.input.operation).not.toEqual(calls[1]?.input.operation);
      expect(calls.every((call) => call.environmentId === props.environmentId)).toBe(true);
    } finally {
      await act(async () => root.unmount());
      container.remove();
      registry.dispose();
      vi.unstubAllGlobals();
    }
  },
);

vi.mock("../../components/ui/Sheet", () => ({
  Sheet: ({
    title,
    children,
    onClose,
  }: {
    title: string;
    children: import("react").ReactNode;
    onClose: () => void;
  }) => (
    <section>
      <h1>{title}</h1>
      <button onClick={onClose}>Done</button>
      {children}
    </section>
  ),
}));

import { RemoteTaskViewer } from "./RemoteTaskViewer";

it("keeps source-only navigation and pagination qualified while revealing task diagnostics on demand", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const registry = AtomRegistry.make();
  const queries: unknown[] = [];
  const view = Atom.make(
    AsyncResult.success({
      taskId: NodeId.make("A-task"),
      environmentId: EnvironmentId.make("C-private-id"),
      threadId: ThreadId.make("C-private-thread"),
      snapshot: {
        sequence: 1,
        executionSettled: false,
        pendingQuestions: 0,
        requests: [],
        title: "Meaningful work",
        observedAt: "2026-10-18T12:00:00Z",
        workState: "working",
        pendingChildren: 1,
        queuedRuns: 0,
        requestCount: 0,
        recoveryWaiting: false,
        incomplete: false,
      },
      childrenTruncated: false,
      children: [
        {
          taskId: NodeId.make("nested-id"),
          environmentId: EnvironmentId.make("C-private-id"),
          threadId: ThreadId.make("nested-private-thread"),
          title: "Nested work",
          status: "running",
        },
      ],
      messages: [{ id: "message", role: "assistant", text: "Useful answer" }],
      nextCursor: 12,
      nextTextOffset: 5,
    }),
  );
  const stopCalls: unknown[] = [];
  const stop = Object.assign(
    createRuntimeCommand(Atom.runtime(Layer.empty), {
      label: "source stop RPC",
      execute: (input: unknown) =>
        Effect.sync(() => {
          stopCalls.push(input);
        }),
    }),
    { permissionAtom: () => Atom.make(false) },
  );
  const remote = {
    view: (input: unknown) => {
      queries.push(input);
      return view;
    },
    stop,
  };
  state.getRemote = () => remote;
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const task = {
    id: NodeId.make("A-task"),
    threadId: ThreadId.make("A-parent"),
    title: "Original work",
    result: "Original result text",
    remoteChild: {
      environmentId: EnvironmentId.make("C-private-id"),
      threadId: ThreadId.make("C-private-thread"),
      label: "Build machine",
      availability: "current" as const,
      latestResult: {
        environmentId: EnvironmentId.make("C-private-id"),
        threadId: ThreadId.make("C-private-thread"),
        runId: RunId.make("private-latest-run"),
        eventId: EventId.make("private-result-event"),
        sequence: 20,
        status: "completed" as const,
        text: "Latest follow-up result",
        truncated: false,
      },
    },
  };
  const button = (label: string) =>
    [...container.querySelectorAll("button")].find((node) => node.textContent?.includes(label));
  const click = async (label: string) => {
    const target = button(label);
    expect(target, label).toBeDefined();
    await act(async () => target!.click());
  };
  try {
    await act(async () => {
      root.render(
        <RegistryContext.Provider value={registry}>
          <RemoteTaskViewer
            environmentId={EnvironmentId.make("A-only-profile")}
            task={task}
            onClose={() => {}}
          />
        </RegistryContext.Provider>,
      );
    });
    expect(container.textContent).toContain("Useful answer");
    expect(container.textContent).toContain("Original result text");
    expect(container.textContent).toContain("Latest follow-up result");
    expect(container.textContent).toContain("Build machine");
    expect(container.textContent).not.toContain("private-latest-run");
    expect(container.textContent).not.toContain("C-private-id");
    expect(button("Stop remote work")?.disabled).toBe(true);
    expect(stopCalls).toEqual([]);
    await click("Task details");
    expect(container.textContent).toContain("C-private-id");
    expect(container.textContent).toContain("private-latest-run");
    await click("Hide task details");
    expect(container.textContent).not.toContain("C-private-id");
    await click("Next messages");
    expect(queries.at(-1)).toMatchObject({
      environmentId: "A-only-profile",
      input: { parentThreadId: "A-parent", taskId: "A-task", after: 12, textOffset: 5 },
    });
    await click("Open nested work");
    expect(queries.at(-1)).toMatchObject({
      environmentId: "A-only-profile",
      input: {
        after: 0,
        textOffset: 0,
        resource: { environmentId: "C-private-id", threadId: "nested-private-thread" },
      },
    });
    await click("Back to delegated thread");
    expect(queries.at(-1)).toEqual({
      environmentId: "A-only-profile",
      input: { parentThreadId: "A-parent", taskId: "A-task", after: 0, textOffset: 0 },
    });
  } finally {
    await act(async () => root.unmount());
    container.remove();
    registry.dispose();
    vi.unstubAllGlobals();
  }
});
