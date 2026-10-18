import { RegistryContext } from "@effect/atom-react";
import {
  EnvironmentId,
  ThreadId,
  RuntimeRequestId,
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
      { permissionAtom: (_id: EnvironmentId) => Atom.make(true) },
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
      const sendButton = () =>
        renderer!.root
          .findAllByType("button")
          .find((node) => node.children.includes("Send answer"));
      expect(sendButton()?.props.disabled).toBe(true);
      if (empty) await click("Submit empty value");
      else {
        await click("Blue");
        await click("Green");
      }
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
