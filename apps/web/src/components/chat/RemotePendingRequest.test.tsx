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
import { create, type ReactTestRenderer } from "react-test-renderer";
import { expect, it, vi } from "vite-plus/test";
const state = vi.hoisted(() => ({
  nextKey: 0,
  getRemote: (): unknown => {
    throw new Error("Uninitialized");
  },
}));
vi.mock("../../state/remoteTasks", () => ({
  get remoteTasks() {
    return state.getRemote();
  },
}));
vi.mock("../ui/button", () => ({
  Button: (props: ComponentProps<"button">) => <button {...props} />,
}));
vi.mock("../ui/input", () => ({ Input: (props: ComponentProps<"input">) => <input {...props} /> }));
vi.mock("../../lib/utils", () => ({ randomUUID: () => `action-${++state.nextKey}` }));
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
    let renderer: ReactTestRenderer | undefined;
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
    const click = async (label: string) => {
      const button = renderer!.root
        .findAllByType("button")
        .find((node) => node.findAll((child) => child.children.includes(label)).length > 0);
      expect(button, label).toBeDefined();
      await act(async () => {
        await button!.props.onClick();
      });
    };
    try {
      await act(async () => {
        renderer = create(
          <RegistryContext.Provider value={registry}>
            <RemotePendingRequest {...props} />
          </RegistryContext.Provider>,
        );
      });
      const text = (value: unknown): string => {
        if (typeof value === "string") return value;
        if (Array.isArray(value)) return value.map(text).join(" ");
        return value && typeof value === "object" && "children" in value
          ? text(value.children)
          : "";
      };
      expect(text(renderer!.toJSON())).not.toContain("C-no-profile");
      const sendButton = () =>
        renderer!.root
          .findAllByType("button")
          .find((node) => node.children.includes("Send answer"));
      expect(sendButton()?.props.disabled).toBe(true);
      await click("Request details");
      expect(text(renderer!.toJSON())).toContain("C-no-profile");
      await click("Request details");
      expect(text(renderer!.toJSON())).not.toContain("C-no-profile");
      if (empty) await click("Submit empty value");
      else {
        await click("Blue");
        await click("Green");
      }
      expect(sendButton()?.props.disabled).toBe(false);
      const chosen = renderer!.root
        .findAllByType("button")
        .find(
          (node) =>
            node.findAll((child) => child.children.includes(empty ? "Submit empty value" : "Blue"))
              .length > 0,
        );
      expect(chosen?.props["aria-pressed"]).toBe(true);
      await act(async () => registry.set(mayRespond, false));
      expect(sendButton()?.props.disabled).toBe(true);
      expect(calls).toEqual([]);
      await act(async () => registry.set(mayRespond, true));
      expect(sendButton()?.props.disabled).toBe(false);
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
      await act(async () => renderer?.unmount());
      registry.dispose();
      vi.unstubAllGlobals();
    }
  },
);

vi.mock("../ui/dialog", () => ({
  Dialog: ({ children }: { children: import("react").ReactNode }) => children,
  DialogPopup: ({ children }: { children: import("react").ReactNode }) => (
    <section>{children}</section>
  ),
  DialogTitle: ({ children }: { children: import("react").ReactNode }) => <h1>{children}</h1>,
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
  let renderer: ReactTestRenderer | undefined;
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
  const click = async (label: string) => {
    const button = renderer!.root
      .findAllByType("button")
      .find((node) => node.findAll((child) => child.children.includes(label)).length > 0);
    expect(button, label).toBeDefined();
    await act(async () => button!.props.onClick());
  };
  const visible = (value: unknown): string =>
    typeof value === "string"
      ? value
      : Array.isArray(value)
        ? value.map(visible).join(" ")
        : value && typeof value === "object" && "children" in value
          ? visible(value.children)
          : "";
  try {
    await act(async () => {
      renderer = create(
        <RegistryContext.Provider value={registry}>
          <RemoteTaskViewer
            environmentId={EnvironmentId.make("A-only-profile")}
            task={task}
            onClose={() => {}}
          />
        </RegistryContext.Provider>,
      );
    });
    expect(visible(renderer!.toJSON())).toContain("Useful answer");
    expect(visible(renderer!.toJSON())).toContain("Original result text");
    expect(visible(renderer!.toJSON())).toContain("Latest follow-up result");
    expect(visible(renderer!.toJSON())).toContain("Build machine");
    expect(visible(renderer!.toJSON())).not.toContain("private-latest-run");
    expect(visible(renderer!.toJSON())).not.toContain("C-private-id");
    expect(
      renderer!.root
        .findAllByType("button")
        .find(
          (node) => node.findAll((child) => child.children.includes("Stop remote work")).length > 0,
        )?.props.disabled,
    ).toBe(true);
    expect(stopCalls).toEqual([]);
    await click("Task details");
    expect(visible(renderer!.toJSON())).toContain("C-private-id");
    expect(visible(renderer!.toJSON())).toContain("private-latest-run");
    await click("Hide task details");
    expect(visible(renderer!.toJSON())).not.toContain("C-private-id");
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
    await act(async () => renderer?.unmount());
    registry.dispose();
    vi.unstubAllGlobals();
  }
});
