import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { AuthOrchestrationOperateScope } from "./auth.ts";
import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import { WsRpcGroup } from "./rpc.ts";
import { SPECTRUM_WS_METHODS } from "./spectrumRpc.ts";

describe("spectrum.abandonReport", () => {
  it("needs the operate grant, so read-only sessions cannot abandon a report", () => {
    expect(clientRpcRequiredScopes(SPECTRUM_WS_METHODS.abandonReport, undefined)).toEqual([
      AuthOrchestrationOperateScope,
    ]);
  });

  it("is served over the WebSocket and names one exact scheduled run", () => {
    const rpc = WsRpcGroup.requests.get(SPECTRUM_WS_METHODS.abandonReport);
    if (rpc === undefined) throw new Error("spectrum.abandonReport is not registered");
    const decode = Schema.decodeUnknownExit(rpc.payloadSchema);

    expect(Exit.isSuccess(decode({ scheduledTaskId: "task-1", schedulerRunId: "run-7" }))).toBe(
      true,
    );
    for (const payload of [
      { scheduledTaskId: "task-1" },
      { scheduledTaskId: "task-1", schedulerRunId: "  " },
      { schedulerRunId: "run-7" },
    ]) {
      expect(Exit.isFailure(decode(payload))).toBe(true);
    }
  });
});
