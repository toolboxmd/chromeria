import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  ISSUE_WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RpcTest from "effect/rpc/RpcTest";

import * as RpcAuthorization from "../auth/RpcAuthorization.ts";

const tested = [ISSUE_WS_METHODS.issuesComment, ISSUE_WS_METHODS.issuesSetState] as const;
const group = WsRpcGroup.omit(
  ...[...WsRpcGroup.requests.keys()].filter(
    (
      tag,
    ): tag is Exclude<keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES, (typeof tested)[number]> =>
      !(tested as ReadonlyArray<string>).includes(tag),
  ),
);
const issue = { host: "github.com", repository: "acme/web", number: 7 };

/** A connection with `scopes`, and how many Issue writes reached their handlers. */
const connect = (scopes: Parameters<typeof RpcAuthorization.layer>[0]) =>
  Effect.gen(function* () {
    let handled = 0;
    const client = yield* RpcTest.makeClient(group).pipe(
      Effect.provide(
        Layer.mergeAll(
          group.toLayerHandler(ISSUE_WS_METHODS.issuesComment, () =>
            Effect.sync(() => void handled++),
          ),
          group.toLayerHandler(ISSUE_WS_METHODS.issuesSetState, () =>
            Effect.sync(() => void handled++),
          ),
          RpcAuthorization.layer(scopes),
        ),
      ),
    );
    return { client, handled: () => handled };
  });

describe("Issue write authorization", () => {
  it.effect("refuses comment and close to an operate-only grant before the handler runs", () =>
    Effect.gen(function* () {
      const { client, handled } = yield* connect([
        AuthOrchestrationReadScope,
        AuthOrchestrationOperateScope,
      ]);
      for (const write of [
        client[ISSUE_WS_METHODS.issuesComment]({ ...issue, body: "Done" }),
        client[ISSUE_WS_METHODS.issuesSetState]({ ...issue, action: "close-completed" }),
      ]) {
        expect(yield* write.pipe(Effect.flip)).toMatchObject({
          _tag: "EnvironmentAuthorizationError",
          requiredPermission: AuthSourceControlWriteScope,
        });
      }
      expect(handled()).toBe(0);
    }).pipe(Effect.scoped),
  );

  it.effect("lets a source-control write grant comment and close without operate", () =>
    Effect.gen(function* () {
      const { client, handled } = yield* connect([
        AuthOrchestrationReadScope,
        AuthSourceControlWriteScope,
      ]);
      yield* client[ISSUE_WS_METHODS.issuesComment]({ ...issue, body: "Done" });
      yield* client[ISSUE_WS_METHODS.issuesSetState]({ ...issue, action: "reopen" });
      expect(handled()).toBe(2);
    }).pipe(Effect.scoped),
  );
});
