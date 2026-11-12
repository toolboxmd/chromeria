import { OrchestratorMcpFailure, type PreviewWatchStartResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as ServerBrowser from "../../../preview/ServerBrowser.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import * as Broker from "../../PreviewAutomationBroker.ts";
import * as Access from "../../McpToolAccess.ts";
import { authorizeWatchRead } from "../../PreviewWatchAccess.ts";
import { PreviewWatchToolkit } from "./tools.ts";

export const layer = Access.toLayer(PreviewWatchToolkit, {
  preview_watch_start: Access.actsOnResource(
    "preview",
    (input) =>
      Effect.gen(function* () {
        const scope = yield* Invocation.requireThreadMcpCapability("preview");
        const broker = yield* Broker.PreviewAutomationBroker;
        return yield* broker.invoke<PreviewWatchStartResult>({
          scope,
          operation: "watchStart",
          input,
          tabId: input.tabId,
        });
      }),
    "external-unknown",
  ),
  preview_watch_stop: Access.actsOnResource(
    "preview",
    (input) =>
      Effect.gen(function* () {
        const scope = yield* Invocation.requireThreadMcpCapability("preview");
        const broker = yield* Broker.PreviewAutomationBroker;
        return yield* broker.invoke<{ watchId: string; epoch: string; state: "stopped" }>({
          scope,
          operation: "watchStop",
          input,
        });
      }),
    "external-unknown",
  ),
  preview_watch_read: Access.reads((input) =>
    Effect.gen(function* () {
      const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
      if (Option.isNone(browser) || browser.value.readWatch === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Retained browser watches are unavailable on this host.",
        });
      return yield* browser.value.readWatch(input, authorizeWatchRead(input));
    }),
  ),
});
