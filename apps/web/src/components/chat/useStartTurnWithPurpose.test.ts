import { describe, expect, it } from "vite-plus/test";

import { purposeIntentForTurn } from "./useStartTurnWithPurpose";

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
