import { describe, expect, it } from "vite-plus/test";
import {
  MessageId,
  RunId,
  TurnItemId,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { turnItemHasDetail } from "../work-log/itemDetail.ts";
import { RESTART_CUT_REASONS } from "@t3tools/shared/restartCut";
import { v2Now, v2Projection, v2ThreadId } from "./orchestrationV2TestFixtures.ts";
import * as Option from "effect/Option";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { EnvironmentId } from "@t3tools/contracts";
import { EMPTY_THREAD_HISTORY_META } from "./threadHistoryMerge.ts";
import { createEnvironmentThreadDetailAtoms } from "./threadDetail.ts";
import { createRestartCutProjector } from "./restartCutPresentation.ts";

const source: OrchestrationV2Run = {
  id: RunId.make("cut"),
  threadId: v2ThreadId,
  ordinal: 1,
  providerInstanceId: v2Projection.thread.providerInstanceId,
  modelSelection: v2Projection.thread.modelSelection,
  providerThreadId: null,
  userMessageId: MessageId.make("user"),
  rootNodeId: null,
  activeAttemptId: null,
  status: "cancelled",
  requestedAt: v2Now,
  startedAt: v2Now,
  completedAt: v2Now,
  checkpointId: null,
  contextHandoffId: null,
  restartCut: { continuation: "scheduled", cutAt: v2Now },
};
const row: OrchestrationV2ThreadProjection["visibleTurnItems"][number] = {
  position: 0,
  visibility: "local",
  sourceThreadId: v2ThreadId,
  sourceItemId: TurnItemId.make("answer"),
  item: {
    id: TurnItemId.make("answer"),
    threadId: v2ThreadId,
    runId: source.id,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: null,
    startedAt: v2Now,
    completedAt: v2Now,
    updatedAt: v2Now,
    type: "assistant_message",
    messageId: MessageId.make("answer"),
    text: "Working",
    streaming: false,
  },
};
const rows = [row];
const project = (runs: ReadonlyArray<OrchestrationV2Run>) =>
  createRestartCutProjector()({ runs, visibleTurnItems: rows });
const explanation = (runs: ReadonlyArray<OrchestrationV2Run>) => {
  const item = project(runs).at(-1)!.item;
  return item.type === "notification" ? item.detail : null;
};

describe("restart-cut client presentation", () => {
  it.each([
    { name: "pending", runs: [source], expected: "Continuation pending." },
    {
      name: "continued",
      runs: [
        source,
        {
          ...source,
          id: RunId.make("resumed"),
          ordinal: 2,
          status: "running" as const,
          restartCut: undefined,
          restartContinuationOfRunId: source.id,
        },
      ],
      expected: "Continuation started (run 2, running).",
    },
    {
      name: "declined",
      runs: [
        {
          ...source,
          restartCut: {
            continuation: "declined" as const,
            cutAt: v2Now,
            reason: RESTART_CUT_REASONS.CONTINUATION_OFF,
          },
        },
      ],
      expected: "Continuation declined. Continuing threads after restarts is turned off.",
    },
  ])(
    "makes the full $name explanation expandable when the label is truncated",
    ({ runs, expected }) => {
      const item = project(runs).at(-1)!.item;
      expect(turnItemHasDetail(item)).toBe(true);
      expect(item.type).toBe("notification");
      if (item.type !== "notification") throw new Error("Missing restart notice");
      expect(item.summary).toBe("Stopped by restart");
      expect(item.detail).toContain("Stopped because Chromeria restarted.");
      expect(item.detail).toContain(expected);
    },
  );

  it("exposes the explanation through the shared client atom without altering durable history", () => {
    const projection = { ...v2Projection, runs: [source], visibleTurnItems: rows };
    const state = Atom.make(
      AsyncResult.success({
        data: Option.some(projection),
        status: "live" as const,
        error: Option.none<string>(),
        history: EMPTY_THREAD_HISTORY_META,
      }),
    );
    const details = createEnvironmentThreadDetailAtoms(() => state);
    const registry = AtomRegistry.make();
    const ref = { environmentId: EnvironmentId.make("local"), threadId: v2ThreadId };
    const visible = registry.get(details.visibleTurnItemsAtom(ref));
    expect(visible).toHaveLength(2);
    expect(visible[1]?.item.type === "notification" && visible[1].item.detail).toContain(
      "Stopped because Chromeria restarted",
    );
    expect(registry.get(details.threadAtom(ref))?.projection).toBe(projection);
    expect(projection.visibleTurnItems).toBe(rows);
    registry.dispose();
  });
  it("places the pending explanation after the cut run without changing its content", () => {
    const result = project([source]);
    expect(result[0]).toBe(row);
    expect(result).toHaveLength(2);
    expect(result[1]?.item.runId).toBe(source.id);
    expect(explanation([source])).toContain(
      "Stopped because Chromeria restarted. Continuation pending.",
    );
  });

  it("distinguishes an admitted continuation from one that actually started", () => {
    const continuation = {
      ...source,
      id: RunId.make("next"),
      ordinal: 2,
      restartContinuationOfRunId: source.id,
      restartCut: undefined,
      status: "starting" as const,
      startedAt: null,
      completedAt: null,
    };
    expect(explanation([source, continuation])).toContain("Continuation pending");
    expect(
      explanation([source, { ...continuation, startedAt: v2Now, status: "running" }]),
    ).toContain("Continuation started (run 2, running)");
  });

  it("shows the authoritative decline reason", () => {
    expect(
      explanation([
        {
          ...source,
          restartCut: {
            continuation: "declined",
            cutAt: v2Now,
            reason: RESTART_CUT_REASONS.CONTINUATION_OFF,
          },
        },
      ]),
    ).toContain("Continuation declined. Continuing threads after restarts is turned off.");
  });

  it("leaves ordinary cancellation and older rows without evidence alone", () => {
    expect(project([{ ...source, restartCut: undefined }])).toBe(rows);
    expect(project([{ ...source, status: "interrupted" }])).toBe(rows);
  });

  it("does not add unloaded or inherited runs to the visible window", () => {
    const render = createRestartCutProjector();
    expect(render({ runs: [source], visibleTurnItems: [] })).toEqual([]);
    const inherited = [{ ...row, visibility: "inherited" as const }];
    expect(render({ runs: [source], visibleTurnItems: inherited })).toBe(inherited);
  });

  it("updates after continuation starts and preserves row identity on unrelated updates", () => {
    const render = createRestartCutProjector();
    const runs = [source];
    const first = render({ runs, visibleTurnItems: rows });
    expect(render({ runs, visibleTurnItems: rows })).toBe(first);
    expect(render({ runs: [...runs], visibleTurnItems: rows })[1]).toBe(first[1]);
    const next = {
      ...source,
      id: RunId.make("next"),
      ordinal: 2,
      restartContinuationOfRunId: source.id,
      restartCut: undefined,
      status: "completed" as const,
    };
    const updated = render({ runs: [source, next], visibleTurnItems: rows });
    expect(updated[1]).not.toBe(first[1]);
    expect(updated[1]?.item.type === "notification" && updated[1].item.detail).toContain(
      "Continuation started (run 2, completed)",
    );
    expect(rows).toEqual([row]);
  });
});
