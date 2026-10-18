import { OrchestratorMcpFailure, type ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { CurrentRequest, externalDispatch, externalReplay } from "../../McpRequestReceipts.ts";
import * as Option from "effect/Option";
import * as Environment from "../../../environment/ServerEnvironment.ts";
import * as ThreadCommandExecutor from "../../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Settings from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { readCaller, unavailable } from "../../threadAccess.ts";
import { EnvironmentToolkit } from "./tools.ts";

export function preferences(settings: ServerSettings) {
  const {
    defaultThreadEnvMode,
    newWorktreesStartFromOrigin,
    enableProviderUpdateChecks,
    backgroundActivity,
    sourceControlWritingStyle,
  } = settings;
  const characters = Array.from(sourceControlWritingStyle.customInstructions);
  return {
    defaultThreadEnvMode,
    newWorktreesStartFromOrigin,
    enableProviderUpdateChecks,
    backgroundActivity: { profile: backgroundActivity.profile },
    sourceControlWritingStyle: {
      ...sourceControlWritingStyle,
      customInstructions: characters.slice(0, 4000).join(""),
      truncated: characters.length > 4000,
    },
  };
}
const access = Effect.gen(function* () {
  const context = yield* readCaller();
  const environment = yield* Environment.ServerEnvironment;
  const descriptor = yield* environment.getDescriptor;
  if (descriptor.environmentId !== context.scope.environmentId)
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "This credential belongs to another environment.",
    });
  return { ...context, descriptor, settings: yield* Settings.ServerSettingsService };
});
export const layer = McpToolAccess.toLayer(EnvironmentToolkit, {
  t3_environment_read: McpToolAccess.reads(() =>
    Effect.gen(function* () {
      const { descriptor, settings } = yield* access;
      const current = yield* settings.getSettings.pipe(Effect.mapError(unavailable));
      return {
        environmentId: descriptor.environmentId,
        label: descriptor.label,
        serverVersion: descriptor.serverVersion,
        platform: descriptor.platform,
        preferences: preferences(current),
      };
    }),
  ),
  t3_environment_preferences_update: McpToolAccess.writesEnvironment(
    ({ clientRequestId: _key, ...patch }, check) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const update = Effect.gen(function* () {
          // The turn may have ended, or the thread's modes changed, while this waited for the lock.
          yield* check;
          const { settings } = yield* access;
          const identity = yield* CurrentRequest;
          const negotiated =
            identity !== undefined &&
            (scope.client?.linked !== true || scope.authority?.envelope.requestOwner !== undefined);
          if (!negotiated)
            return preferences(
              yield* settings.updateSettings(patch).pipe(Effect.mapError(unavailable)),
            );
          const previous =
            yield* externalReplay<import("@t3tools/contracts").McpConfirmedOperation>();
          if (Option.isSome(previous)) return previous.value;
          return yield* externalDispatch(
            Effect.gen(function* () {
              yield* settings.updateSettings(patch).pipe(Effect.mapError(unavailable));
              return {
                type: "confirmed-operation" as const,
                applied: true as const,
                requestId: identity.clientRequestId,
                operationId: identity.id,
                stage: "confirmed" as const,
              };
            }),
          );
        });
        // A thread caller serializes with its own turn; a client has no thread to lock.
        return yield* scope.thread === undefined
          ? update
          : executor.withLock(scope.thread.threadId, update);
      }),
    "external-unknown",
  ),
});
