import { PeerDispatchAuthority } from "../peer/peerEffectAuthority.ts";
import * as ServerSettings from "../serverSettings.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpDelegateTaskInput,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Tool from "effect/ai/Tool";

import * as HtmlRender from "../htmlRender/HtmlRender.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Invocation from "./McpInvocationContext.ts";
import * as Registry from "./McpSessionRegistry.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import { assertSameLink } from "./linkOrigin.ts";
import { assertTargetWithinLimits, unavailable } from "./threadAccess.ts";

/** Destination-owned resources are addressed in that environment, never copied implicitly. */
export const fields = {
  environmentId: Schema.optional(EnvironmentId).annotate({
    description:
      "Run this tool on a linked environment. IDs, paths and pending uploads name destination resources. delegate_task keeps its source parent and launches its child there. Omit for this environment.",
  }),
  contextThreadId: Schema.optional(ThreadId).annotate({
    description:
      "For a tool acting as its caller, select a destination thread within this link’s resource scope and permissions. Panels belong to that destination thread; rendered HTML is displayed in this thread.",
  }),
  contextCapabilities: Schema.optional(
    Schema.Array(
      Schema.Literals(["preview", "orchestration", "worktree", "device", "pull-requests"]),
    ).annotate({
      description: "Peer transport metadata, set automatically from the calling credential.",
    }),
  ),
};

export const parameters = <F extends Schema.Struct.Fields>(schema: Schema.Struct<F>) =>
  Schema.Struct({ ...schema.fields, ...fields });

export class CurrentTool extends Context.Reference<Tool.Any | undefined>("t3/mcp/CurrentTool", {
  defaultValue: () => undefined,
}) {}

const decodePreparedDocument = Schema.decodeUnknownEffect(HtmlRender.PreparedDocument);
const decodeDelegate = Schema.decodeUnknownEffect(OrchestratorMcpDelegateTaskInput);
const decodeWait = Schema.decodeUnknownEffect(OrchestratorMcpThreadWaitInput);
const target = Schema.decodeUnknownOption(Schema.Struct(fields));
export const isRemote = (params: unknown, scope: Invocation.McpInvocationScope) => {
  const decoded = target(params);
  return (
    Option.isSome(decoded) &&
    PeerForwarding.remoteTarget(scope, decoded.value.environmentId) !== undefined
  );
};
const denied = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });

/**
 * A linked session may act in its own live destination thread, using an existing
 * credential's capability grants. The link ceiling is checked before borrowing
 * that scope; its modes must be at least the thread's, so no scope gains access.
 */
export const withContext = <A, E, R>(
  params: unknown,
  here: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | OrchestratorMcpFailure, R> =>
  Effect.gen(function* () {
    let invocation = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    const currentTool = yield* CurrentTool;
    if (
      Option.isSome(invocation) &&
      invocation.value.thread !== undefined &&
      invocation.value.client === undefined &&
      currentTool !== undefined
    ) {
      const owner = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
      if (Option.isSome(owner)) {
        const scope = invocation.value;
        const threadId = invocation.value.thread.threadId;
        const current =
          currentTool.name === "t3_thread_send" && isRemote(params, scope)
            ? yield* owner.value.forThread(threadId)
            : yield* owner.value.checkThread(threadId);
        const legacy =
          current === undefined ? yield* owner.value.legacyCeiling(threadId) : undefined;
        const { authority: _oldAuthority, legacyPeerCeiling: _oldLegacy, ...credential } = scope;
        const ceiling = current?.ceiling ?? legacy;
        invocation = Option.some({
          ...credential,
          ...(current === undefined ? {} : { authority: current }),
          ...(legacy === undefined ? {} : { legacyPeerCeiling: legacy }),
          capabilities:
            ceiling === undefined
              ? scope.capabilities
              : new Set(
                  [...scope.capabilities].filter((capability) =>
                    ceiling.capabilities.includes(capability),
                  ),
                ),
        });
      } else if (
        invocation.value.authority !== undefined ||
        invocation.value.legacyPeerCeiling !== undefined
      )
        return yield* unavailable();
    }
    const execution = Option.isSome(invocation)
      ? here.pipe(
          Effect.provideService(PeerDispatchAuthority, invocation.value.authority?.id),
          Effect.provideService(Invocation.McpInvocationContext, invocation.value),
        )
      : here;
    if (
      Option.isSome(invocation) &&
      invocation.value.client?.linked === true &&
      invocation.value.authority !== undefined &&
      currentTool !== undefined
    ) {
      const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
      if (Option.isNone(authority)) return yield* unavailable();
      yield* authority.value.checkPayload(invocation.value.authority, currentTool.name, params);
    }
    const decoded = target(params);
    if (Option.isNone(decoded) || decoded.value.contextThreadId === undefined)
      return yield* execution;
    const scopeOption = invocation;
    if (Option.isNone(scopeOption)) return yield* execution;
    const scope = scopeOption.value;
    if (isRemote(params, scope)) return yield* execution;
    if (scope.thread !== undefined) {
      if (scope.thread.threadId !== decoded.value.contextThreadId)
        return yield* denied("A local thread credential cannot select another calling thread.");
      return yield* execution;
    }
    if (scope.client?.linked !== true)
      return yield* denied("A destination thread context requires an authenticated peer link.");
    const registry = yield* Effect.serviceOption(Registry.McpSessionRegistry);
    const threads = yield* Effect.serviceOption(ThreadManagement.ThreadManagementService);
    if (Option.isNone(threads)) return yield* unavailable();
    const shell = yield* threads.value
      .getThreadShell(decoded.value.contextThreadId)
      .pipe(Effect.mapError(unavailable));
    if (shell === null || shell.deletedAt !== null)
      return yield* denied("The destination thread was not found.");
    yield* assertSameLink({ scope, caller: undefined }, shell);
    yield* assertTargetWithinLimits(
      Invocation.intersectModes(
        Invocation.clientModeCeiling(scope.client),
        scope.authority?.ceiling,
      ),
      shell,
    );
    if (scope.authority !== undefined) {
      // Negotiated peers use durable authority, never another run's token.
      const capabilities = new Set(scope.authority.ceiling.capabilities);
      const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      if (Option.isSome(settings)) {
        const current = yield* settings.value.getSettings.pipe(
          Effect.mapError(() => denied("Destination agent access settings are unavailable.")),
        );
        const effective = resolveProjectSettings(current, shell.projectId).settings;
        if (!effective.enableAgentBrowserAccess) capabilities.delete("preview");
        if (!effective.enableAgentDeviceAccess) capabilities.delete("device");
      }
      const bound: Invocation.McpInvocationScope = {
        ...scope,
        thread: {
          threadId: shell.id,
          providerSessionId: `peer:${scope.authority.id}`,
          providerInstanceId: shell.providerInstanceId,
        },
        capabilities,
      };
      return yield* here.pipe(
        Effect.provideService(PeerDispatchAuthority, scope.authority.id),
        Effect.provideService(Invocation.McpInvocationContext, bound),
      );
    }
    const modern = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isSome(modern))
      return yield* denied(
        "The peer must negotiate authority v1 and explicit capability consent before selecting a destination context.",
      );
    if (Option.isNone(registry)) return yield* unavailable();
    const credential = yield* registry.value.scopeForThread(shell.id);
    if (credential === undefined)
      return yield* denied("The destination thread has no live MCP credential.");
    const allowed = new Set(decoded.value.contextCapabilities ?? []);
    const bound: Invocation.McpInvocationScope = {
      ...credential,
      requestNamespace: scope.requestNamespace,
      client: scope.client,
      capabilities: new Set(
        [...credential.capabilities].filter((capability) => allowed.has(capability)),
      ),
    };
    return yield* here.pipe(Effect.provideService(Invocation.McpInvocationContext, bound));
  });

const capabilityFor = (name: string): Invocation.McpCapability | undefined =>
  name.startsWith("preview_") || name.startsWith("t3_preview_")
    ? "preview"
    : name.startsWith("device_")
      ? "device"
      : name.startsWith("t3_worktree_")
        ? "worktree"
        : name.includes("pull_request") ||
            name === "link_issue" ||
            name === "unlink_issue" ||
            name === "list_thread_issues"
          ? "pull-requests"
          : name.startsWith("html_")
            ? undefined
            : "orchestration";

/** Called only after the declaration's access check and before its local handler. */
export const invoke = <P, A, E, R>(
  params: P,
  handle: (params: P) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | OrchestratorMcpFailure, R> =>
  Effect.gen(function* () {
    const here = Effect.suspend(() => {
      if (typeof params !== "object" || params === null) return handle(params);
      const {
        environmentId: _environment,
        contextThreadId: _thread,
        contextCapabilities: _capabilities,
        ...local
      } = params as Record<string, unknown>;
      // Only routing metadata is removed from the paired input schema.
      return handle(local as P);
    });
    const tool = yield* CurrentTool;
    const decoded = target(params);
    if (tool === undefined || Option.isNone(decoded)) return yield* here;
    const scopeOption = yield* Effect.serviceOption(Invocation.McpInvocationContext);
    if (Option.isNone(scopeOption)) return yield* here;
    const scope = scopeOption.value;
    const environmentId = PeerForwarding.remoteTarget(scope, decoded.value.environmentId);
    if ((tool.name === "web_read" || tool.name === "web_search") && environmentId !== undefined)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Select the research host with webResearch.host instead of environmentId.",
      });
    if (tool.name === "delegate_task" && decoded.value.environmentId !== undefined) {
      const input = yield* decodeDelegate(params).pipe(Effect.mapError(unavailable));
      if (
        input.target?.environmentId !== undefined &&
        input.target.environmentId !== decoded.value.environmentId
      )
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "environmentId and target.environmentId must name the same destination.",
        });
      if (environmentId === undefined) return yield* here;
      if (!scope.capabilities.has("orchestration"))
        return yield* denied("This credential cannot delegate tasks.");
      // Delegation records ownership and report-back here; only its child launches there.
      // Both destination spellings use the existing RemoteDelegation service path.
      return yield* handle({ ...input, target: { ...input.target, environmentId } } as P);
    }
    if (environmentId === undefined) return yield* here;
    const capability = capabilityFor(tool.name);
    if (capability !== undefined && !scope.capabilities.has(capability))
      return yield* denied("This credential cannot use this tool's capability.");
    const forwarding = yield* Effect.serviceOption(PeerForwarding.PeerForwarding);
    if (Option.isNone(forwarding)) return yield* unavailable();
    if (tool.name === "t3_thread_wait") {
      const input = yield* decodeWait(params).pipe(Effect.mapError(unavailable));
      return (yield* forwarding.value.waitForThread(scope, environmentId, input)) as A;
    }
    const result = yield* forwarding.value.call(scope, tool, environmentId, {
      ...(params as object),
      contextCapabilities: [...scope.capabilities],
    });
    if (tool.name === "html_render") {
      const document = yield* decodePreparedDocument(result).pipe(Effect.mapError(unavailable));
      const { thread } = yield* Invocation.requireThreadScope(scope, "html_render");
      const render = yield* Effect.serviceOption(HtmlRender.HtmlRender);
      if (Option.isNone(render)) return yield* unavailable();
      const reference = yield* render.value
        .publishPrepared({
          threadId: thread.threadId,
          html: document.preparedHtml,
          title: document.title,
          height: document.height,
        })
        .pipe(Effect.mapError(unavailable));
      return {
        htmlRender: reference,
        message:
          "Shown to the reader above your reply. Don't mention or describe the page; reply with only what it doesn't already say.",
      } as A;
    }
    // PeerForwarding decodes the paired toolkit's success schema before returning.
    return result as A;
  });
