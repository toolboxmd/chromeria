import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { ORCHESTRATION_V2_WS_METHODS } from "./orchestrationV2.ts";
import { WS_METHODS, WsRpcGroup } from "./rpc.ts";

const getSettings = WsRpcGroup.requests.get(WS_METHODS.serverGetSettings);
if (getSettings === undefined) throw new Error("server.getSettings is not registered");
const decodeSettings = Schema.decodeSync(getSettings.successSchema);
const decodeSettingsError = Schema.decodeExit(getSettings.errorSchema);

describe("WebSocket RPC contracts", () => {
  it("lists scheduled tasks through a non-stream RPC alongside subscriptions", () => {
    expect(WS_METHODS).toHaveProperty("scheduledTasksList", "scheduledTasks.list");
    const list = WsRpcGroup.requests.get("scheduledTasks.list");
    if (list === undefined) throw new Error("scheduledTasks.list is not registered");
    const decodeInput = Schema.decodeUnknownSync(list.payloadSchema);
    const decodeResult = Schema.decodeUnknownSync(list.successSchema);
    expect(decodeInput({})).toEqual({});
    expect(decodeResult({ tasks: [] })).toEqual({ tasks: [] });
    expect(WsRpcGroup.requests.has(WS_METHODS.scheduledTasksSubscribe)).toBe(true);
  });

  it("reads settings through the settings RPC and preserves settings errors", () => {
    const settings = decodeSettings({});
    expect(settings).toHaveProperty("providerInstances");
    expect(
      Exit.isSuccess(
        decodeSettingsError({
          _tag: "ServerSettingsError",
          settingsPath: "/settings.json",
          operation: "read-file",
        }),
      ),
    ).toBe(true);
  });

  it("rejects server-internal commands sent to dispatchCommand", () => {
    const dispatchCommand = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (dispatchCommand === undefined) throw new Error("dispatchCommand is not registered");
    const decode = Schema.decodeUnknownExit(dispatchCommand.payloadSchema);

    expect(
      Exit.isFailure(
        decode({
          type: "checkpoint.rollback.fail",
          commandId: "forged-rollback-failure",
          threadId: "thread-1",
          requestId: "rollback-1",
          message: "Forged failure.",
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decode({
          type: "checkpoint.rollback",
          commandId: "rollback-1",
          threadId: "thread-1",
          scopeId: "scope-1",
          checkpointId: "checkpoint-1",
        }),
      ),
    ).toBe(true);
  });

  it("lets the server pick a new worktree's folder", () => {
    const createWorktree = WsRpcGroup.requests.get(WS_METHODS.vcsCreateWorktree);
    if (createWorktree === undefined) throw new Error("vcs.createWorktree is not registered");
    const decode = Schema.decodeUnknownExit(createWorktree.payloadSchema);
    const request = { cwd: "/repo", refName: "main", newRefName: "feature/x" };

    expect(Exit.isSuccess(decode({ ...request, path: null }))).toBe(true);
    expect(Exit.isFailure(decode({ ...request, path: "/repo/.git/hooks" }))).toBe(true);
  });
});
