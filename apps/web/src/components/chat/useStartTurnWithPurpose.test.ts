import {
  EnvironmentId,
  MessageId,
  ProviderInstanceId,
  ThreadId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import { useSideChat } from "./useSideChat";
import { purposeIntentForTurn } from "./useStartTurnWithPurpose";

const state = vi.hoisted(() => ({
  fork: vi.fn(),
  delete: vi.fn(),
  setRuntimeMode: vi.fn(),
  startTurn: vi.fn(),
  readShell: vi.fn(),
  waitForShell: vi.fn(),
  configs: vi.fn(),
  openPanel: vi.fn(),
}));

vi.mock("react", () => ({ useRef: (current: unknown) => ({ current }) }));
vi.mock("../../hooks/useStableCallback", () => ({
  useStableCallback: (callback: unknown) => callback,
}));
vi.mock("~/lib/utils", () => ({
  newThreadId: () => ThreadId.make("side-child"),
  newMessageId: () => MessageId.make("side-message"),
}));
vi.mock("../../composerDraftStore", () => ({ useComposerDraftStore: { getState: vi.fn() } }));
vi.mock("../../state/threads", () => ({
  threadEnvironment: {
    forkSideChat: state.fork,
    delete: state.delete,
    setRuntimeMode: state.setRuntimeMode,
    startTurn: state.startTurn,
  },
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => command,
}));
vi.mock("../../state/session", () => ({ readEnvironmentScope: () => true }));
vi.mock("../../state/entities", () => ({
  readThreadShell: state.readShell,
  waitForThreadShell: state.waitForShell,
}));
vi.mock("../../state/server", () => ({ environmentServerConfigsAtom: "configs" }));
vi.mock("../../rpc/atomRegistry", () => ({ appAtomRegistry: { get: state.configs } }));
vi.mock("../../rightPanelStore", () => ({
  useRightPanelStore: { getState: () => ({ openThread: state.openPanel }) },
}));

const scoped = {
  purposeSelection: {
    revision: 3,
    scope: {
      type: "single" as const,
      reference: {
        host: "github.com",
        repository: "toolboxmd/chromeria",
        kind: "issue" as const,
        number: 377,
      },
    },
  },
};

function setupSideChat(purposeVersions: ReadonlyArray<number> | undefined) {
  const parent = {
    environmentId: EnvironmentId.make("linked-environment"),
    threadId: ThreadId.make("parent"),
  };
  const child = { ...parent, threadId: ThreadId.make("side-child") };
  state.fork.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.delete.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.setRuntimeMode.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.startTurn.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.waitForShell.mockReset().mockResolvedValue(true);
  state.readShell
    .mockReset()
    .mockImplementation((ref: ScopedThreadRef) =>
      ref.environmentId === child.environmentId && ref.threadId === child.threadId
        ? { ...scoped, runtimeMode: "approval-required" }
        : { purposeSelection: { revision: 99, scope: { type: "unscoped", reason: "deliberate" } } },
    );
  state.configs.mockReset().mockReturnValue(
    new Map([
      [
        child.environmentId,
        { environment: { capabilities: { executionPurposeVersions: purposeVersions } } },
      ],
      [
        EnvironmentId.make("primary"),
        { environment: { capabilities: { executionPurposeVersions: [1] } } },
      ],
    ]),
  );
  state.openPanel.mockReset();
  const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
  const onError = vi.fn();
  const hook = useSideChat({
    threadRef: parent,
    readSettings: () => ({ runtimeMode: "full-access", modelSelection }),
    onError,
  });
  return { parent, child, modelSelection, onError, hook };
}

describe("purposeIntentForTurn", () => {
  it("uses the destination thread's own stored selection", () => {
    expect(purposeIntentForTurn({}, scoped)).toEqual({
      version: 1,
      intent: { type: "selection", expectedRevision: 3 },
    });
    expect(purposeIntentForTurn({}, {})).toEqual({
      version: 1,
      intent: { type: "selection", expectedRevision: 0 },
    });
  });

  it("never selects a purpose for a thread the client has not loaded", () => {
    expect(purposeIntentForTurn({}, null)).toBeUndefined();
  });

  it("starts new threads unscoped and keeps continuation and Steer purposes", () => {
    expect(purposeIntentForTurn({ bootstrap: { createThread: {} } }, scoped)).toEqual({
      version: 1,
      intent: { type: "unscoped" },
    });
    expect(purposeIntentForTurn({ manualContinuationOfRunId: "run" }, scoped)).toEqual({
      version: 1,
      intent: { type: "inherit" },
    });
    expect(purposeIntentForTurn({ dispatchMode: "steer" }, scoped)).toEqual({
      version: 1,
      intent: { type: "target" },
    });
  });
});

describe("side-chat first prompt purpose", () => {
  it.each([
    {
      versions: [1],
      expectedPurpose: { version: 1, intent: { type: "selection", expectedRevision: 3 } },
    },
    { versions: [], expectedPurpose: undefined },
    { versions: undefined, expectedPurpose: undefined },
  ])(
    "uses the destination's purpose capabilities ($versions)",
    async ({ versions, expectedPurpose }) => {
      const { parent, child, modelSelection, onError, hook } = setupSideChat(versions);

      expect(
        await hook.open({ text: "Discuss this", titleSeed: "Discussion", attachments: [] }),
      ).toBe(true);

      expect(state.waitForShell).toHaveBeenCalledWith(child);
      expect(state.setRuntimeMode).toHaveBeenCalledExactlyOnceWith({
        environmentId: child.environmentId,
        input: {
          threadId: child.threadId,
          runtimeMode: "full-access",
          createdAt: expect.any(String),
        },
      });
      expect(state.startTurn).toHaveBeenCalledExactlyOnceWith({
        environmentId: child.environmentId,
        input: {
          threadId: child.threadId,
          message: {
            messageId: "side-message",
            role: "user",
            text: "Discuss this",
            attachments: [],
          },
          titleSeed: "Discussion",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: expect.any(String),
          ...(expectedPurpose === undefined ? {} : { purposeIntent: expectedPurpose }),
        },
      });
      if (expectedPurpose === undefined) {
        expect(state.startTurn.mock.calls[0]?.[0].input).not.toHaveProperty("purposeIntent");
      }
      expect(state.openPanel).toHaveBeenCalledExactlyOnceWith(parent, child.threadId);
      expect(onError).not.toHaveBeenCalled();
    },
  );
});
