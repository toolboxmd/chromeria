import * as Effect from "effect/Effect";
import * as Preview from "../../../preview/Manager.ts";
import { requireThreadMcpCapability } from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { unavailable } from "../../threadAccess.ts";
import { PreviewControlsToolkit } from "./tools.ts";
import * as PreviewAutomationBroker from "../../PreviewAutomationBroker.ts";
import * as PeerAuthority from "../../../peer/PeerAuthority.ts";
import { OrchestratorMcpFailure } from "@t3tools/contracts";

const recheck = Effect.fn("PreviewControlToolHandlers.recheck")(function* (
  scope: import("../../McpInvocationContext.ts").McpThreadInvocationScope,
) {
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  if (Option.isSome(authority)) {
    yield* authority.value.recheckInvocation(scope, "preview", scope.thread.threadId);
  } else if (scope.authority !== undefined || scope.client?.linked === true)
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Current original preview authority is unavailable.",
    });
});

const access = Effect.gen(function* () {
  const scope = yield* requireThreadMcpCapability("preview");
  yield* recheck(scope);
  return { scope, manager: yield* Preview.PreviewManager };
});
export const layer = McpToolAccess.toLayer(PreviewControlsToolkit, {
  t3_preview_list: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const { scope, manager } = yield* access;
      const result = yield* manager.list({ threadId: scope.thread.threadId });
      yield* recheck(scope);
      const start = input.cursor ?? 0;
      const end = start + (input.limit ?? 20);
      return {
        ...result,
        sessions: result.sessions.slice(start, end),
        nextCursor: end < result.sessions.length ? end : null,
      };
    }),
  ),
  t3_preview_close: McpToolAccess.actsOnResource(
    "preview",
    (input) =>
      Effect.gen(function* () {
        const { scope, manager } = yield* access;
        const previous = yield* externalReplay();
        yield* recheck(scope);
        if (Option.isSome(previous)) return {};
        const { sessions } = yield* manager.list({ threadId: scope.thread.threadId });
        yield* recheck(scope);
        if (
          sessions.some((session) => session.tabId === input.tabId && session.runtime === "server")
        ) {
          const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
          yield* broker
            .invoke({ scope, operation: "close", input: {}, tabId: input.tabId })
            .pipe(
              Effect.mapError((error) =>
                error._tag === "OrchestratorMcpFailure" ? error : unavailable(),
              ),
            );
          return {};
        }
        yield* externalDispatch(
          recheck(scope).pipe(
            Effect.andThen(
              manager
                .close({ threadId: scope.thread.threadId, tabId: input.tabId })
                .pipe(Effect.mapError(unavailable)),
            ),
          ),
        );
        return {};
      }),
    "external-unknown",
  ),
});
import * as Option from "effect/Option";
import { externalDispatch, externalReplay } from "../../McpRequestReceipts.ts";
