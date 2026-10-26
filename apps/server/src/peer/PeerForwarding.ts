import { OrchestratorMcpScheduleTaskInput, RemoteSecretReceiptQuery } from "@t3tools/contracts";
import * as RemoteSecretPersistence from "../secrets/remoteSecretPersistence.ts";
import * as SqlClient from "effect/sql/SqlClient";
import {
  isPromachosThread,
  OrchestratorMcpCapabilitiesResult,
  PurposeIntent,
} from "@t3tools/contracts";
import type { PreviewSnapshotTool } from "../mcp/toolkits/preview/tools.ts";
import { toolEffect } from "../mcp/McpToolEffects.ts";
import { McpAttachmentInput } from "../mcp/toolkits/attachment/input.ts";
import { requestNamespace } from "../mcp/requestIdentity.ts";
import * as Predicate from "effect/Predicate";
import {
  type EnvironmentId,
  DeviceCommandCatalog,
  PreviewAutomationUploadInput,
  PreviewAutomationStatus,
  PreviewTabId,
  DeviceCommandInput,
  DeviceAtomicCommand,
  PeerAttachmentInspectSource,
  PeerAttachmentInspectResult,
  PeerAttachmentReference,
  ThreadId,
  DeviceHostId,
  DeviceCommandTarget,
  OrchestratorMcpFailure,
  type OrchestratorMcpEnvironmentLinksResult,
  OrchestratorMcpThreadInterruptInput,
  OrchestratorMcpThreadInterruptResult,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadWaitInput,
  OrchestratorMcpThreadWaitResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tool from "effect/ai/Tool";

import { assertNotLinked, callerLinkOrigin } from "../mcp/linkOrigin.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { loadCaller, unavailable } from "../mcp/threadAccess.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as Option from "effect/Option";
import { PeerReturnPair } from "@t3tools/contracts";
import {
  ChatAttachment,
  AttachmentCreateUploadUrlInput,
  PeerAttachmentUploadResult,
  type PeerRequestProtocol,
} from "@t3tools/contracts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";
import * as FileSystem from "effect/FileSystem";
import * as Stream from "effect/Stream";
import * as RequestReceipts from "../mcp/McpRequestReceipts.ts";

/**
 * A peer's `/mcp` holds a wait open for its whole timeout without sending
 * anything, and T3 Connect's edge drops a request idle for about 100 s, so a
 * forwarded wait goes out in pieces no longer than this.
 */
export const FORWARDED_WAIT_CHUNK_MS = 50_000;
const decodeAttachments = Schema.decodeUnknownEffect(Schema.Array(McpAttachmentInput));

const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_WAIT_TIMEOUT_MS = 60 * 60 * 1_000;
const isPurposeIntent = Schema.is(PurposeIntent);

type Scope = McpInvocationContext.McpInvocationScope;

/**
 * Runs T3 MCP tools in a linked environment for an agent here. The tool's own
 * declaration checks the agent here first; the linked environment then holds
 * the call to the link's access, narrowed to the agent's modes, which every
 * forwarded call carries.
 */
export class PeerForwarding extends Context.Service<
  PeerForwarding,
  {
    readonly authorizeSavedPreview: (
      scope: Scope,
      environmentId: EnvironmentId,
      parameters: Tool.Parameters<typeof PreviewSnapshotTool>,
      request: RequestReceipts.RequestIdentity,
    ) => Effect.Effect<void, OrchestratorMcpFailure>;
    /** Runs `tool` in the linked environment `environmentId` with `params`. */
    readonly call: <T extends Tool.Any>(
      scope: Scope,
      tool: T,
      environmentId: EnvironmentId,
      params: Tool.Parameters<T>,
      options?: {
        readonly expectedLinkIdentity: string;
        readonly pair?: PeerAuthority.Authority["pair"];
      },
    ) => Effect.Effect<Tool.Success<T>, OrchestratorMcpFailure>;
    /**
     * Runs `here` unless `params.environmentId` names a linked environment, in
     * which case `tool` runs there with the same parameters.
     */
    readonly route: <T extends Tool.Any, A, E, R>(
      scope: Scope,
      tool: T,
      params: Tool.Parameters<T> & { readonly environmentId?: EnvironmentId | undefined },
      here: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A | Tool.Success<T>, E | OrchestratorMcpFailure, R>;
    /** `t3_thread_read` there. */
    readonly readThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadReadInput,
    ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>;
    /** `t3_thread_interrupt` there. */
    readonly interruptThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadInterruptInput,
      options?: { readonly expectedLinkIdentity: string },
    ) => Effect.Effect<OrchestratorMcpThreadInterruptResult, OrchestratorMcpFailure>;
    /** `t3_thread_wait` there, in pieces short enough to survive any route. */
    readonly waitForThread: (
      scope: Scope,
      environmentId: EnvironmentId,
      input: OrchestratorMcpThreadWaitInput,
    ) => Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>;
    /** The environments this one links to, as an agent sees them. */
    readonly links: (
      scope: Scope,
    ) => Effect.Effect<OrchestratorMcpEnvironmentLinksResult, OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerForwarding") {}

/** The linked environment a call names, or nothing when it names this one. */
export const remoteTarget = (scope: Scope, environmentId: EnvironmentId | undefined) =>
  environmentId === scope.environmentId ? undefined : environmentId;

const decodeLaunchedChild = Schema.decodeUnknownEffect(
  Schema.Struct({
    threadId: PeerReturnPair.fields.childThreadId,
    runId: PeerReturnPair.fields.childRunId,
  }),
);
const decodeAttachment = Schema.decodeUnknownEffect(ChatAttachment);
const uploadParameters = Schema.Struct({
  upload: AttachmentCreateUploadUrlInput,
  digest: Schema.optional(Schema.String),
  clientRequestId: Schema.optional(Schema.String),
  contextThreadId: Schema.optional(ThreadId),
});
const decodeSecretReceiptQuery = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RemoteSecretReceiptQuery),
);
const decodeSecretSchedule = Schema.decodeUnknownEffect(OrchestratorMcpScheduleTaskInput);
const make = Effect.gen(function* () {
  const peers = yield* PeerMcpClient.PeerMcpClient;
  const secretSql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const links = yield* PeerLinks.PeerLinks;
  const crypto = yield* Crypto.Crypto;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const requestOwner = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);

  /**
   * The caller's own limits here, which travel with every call it forwards.
   * Linked descendants need explicit durable onward consent. The destination
   * validates the originating grant through the concurrent control channel.
   */
  const forwardingCaller = Effect.fn("PeerForwarding.forwardingCaller")(function* (scope: Scope) {
    const pendingRelay = yield* PeerAuthority.PendingRequestRelay;
    return yield* loadCaller().pipe(
      Effect.tap((caller) =>
        pendingRelay && scope.client?.linked === true && scope.authority !== undefined
          ? Effect.void
          : callerLinkOrigin(caller) === undefined
            ? assertNotLinked(caller, "use this environment's links")
            : Option.isSome(authority) && scope.thread !== undefined
              ? authority.value.forThread(scope.thread.threadId).pipe(
                  Effect.flatMap((record) =>
                    record?.ceiling.onward === true
                      ? Effect.void
                      : Effect.fail(
                          new OrchestratorMcpFailure({
                            code: "capability_denied",
                            message:
                              "This child's durable grant does not permit onward delegation.",
                          }),
                        ),
                  ),
                )
              : Effect.fail(
                  new OrchestratorMcpFailure({
                    code: "capability_denied",
                    message: "A peer link cannot forward through another environment's links.",
                  }),
                ),
      ),
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
    );
  });

  /**
   * Every caller here reaches the peer as the one link session, so a retry
   * key is scoped to its caller before it leaves: two agents here reusing a
   * key never collide there, and one agent retrying gets its first result.
   */
  const scopedRequestId = (namespace: string, clientRequestId: string) =>
    crypto.digest("SHA-256", new TextEncoder().encode(`${namespace}\n${clientRequestId}`)).pipe(
      Effect.map((digest) => `fwd:${Hex.encode(digest)}`),
      Effect.orDie,
    );

  /** Tool and contract schemas are plain data: they encode and decode without services. */
  // Effect.fn erases type parameters; `forward` narrows the decoded result for each caller.
  const forwardDecoded: (
    scope: Scope,
    toolName: string,
    parameters: Schema.Top,
    success: Schema.Top,
    environmentId: EnvironmentId,
    params: unknown,
    options?: {
      readonly expectedLinkIdentity: string;
      readonly pair?: PeerAuthority.Authority["pair"];
    },
    readOnly?: boolean,
  ) => Effect.Effect<unknown, OrchestratorMcpFailure> = Effect.fn("PeerForwarding.forward")(
    function* (
      scope: Scope,
      toolName: string,
      parameters: Schema.Top,
      success: Schema.Top,
      environmentId: EnvironmentId,
      params: unknown,
      options?: {
        readonly expectedLinkIdentity: string;
        readonly pair?: PeerAuthority.Authority["pair"];
      },
      readOnly: boolean = false,
    ) {
      if (toolName === "request_secret")
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "Remote secrets require the dedicated negotiated owner; generic forwarding is unsupported.",
        });
      if (toolName === "t3_pending_request_respond" || toolName === "t3_pending_request_dismiss") {
        if (peers.requirePendingRequests === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The peer does not support verified pending request actions.",
          });
        yield* peers.requirePendingRequests(environmentId, options?.expectedLinkIdentity);
      }
      const encoded = (yield* Schema.encodeUnknownEffect(parameters as Schema.Encoder<unknown>)(
        params,
      ).pipe(Effect.mapError(unavailable))) as Record<string, unknown>;
      const { environmentId: _target, ...args } = encoded;
      if (toolName === "t3_thread_send") {
        if (args.delegatedContinuation !== undefined) {
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "Only the origin's recorded delegated launch can supply continuation evidence.",
          });
        }
        if (
          scope.thread !== undefined &&
          (args.purposeIntent === undefined ||
            (isPurposeIntent(args.purposeIntent) &&
              args.purposeIntent.intent.type === "inherit")) &&
          (args.mode === undefined || args.mode === "auto" || args.mode === "queue")
        ) {
          const parent = yield* threads
            .getThreadRecords(scope.thread.threadId, ["subagents"])
            .pipe(Effect.mapError(unavailable));
          const matches = parent.subagents.filter(
            (task) =>
              task.origin === "app_owned" &&
              task.remoteChild?.environmentId === environmentId &&
              task.remoteChild.threadId === args.threadId,
          );
          if (matches.length > 1) {
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The original remote delegated launch is ambiguous.",
            });
          }
          const task = matches[0];
          if (task !== undefined) {
            const evidence = task.remoteChild?.launch?.delegatedFrom.purposeLaunch;
            // A launch that omitted intent recorded no evidence. Its omitted
            // follow-up stays warned legacy unscoped there, as a local one does.
            if (evidence !== undefined || args.purposeIntent !== undefined) {
              if (evidence === undefined || evidence.threadId !== scope.thread.threadId) {
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message:
                    "The original remote delegated launch has no validated purpose evidence.",
                });
              }
              args.delegatedContinuation = evidence;
            }
          }
        }
      }
      if (
        toolName === "t3_thread_send" &&
        Option.isSome(authority) &&
        typeof args.threadId === "string" &&
        typeof args.message === "string"
      ) {
        const returned = yield* authority.value.returnNotice(scope, environmentId, {
          threadId: args.threadId as PeerReturnPair["parentThreadId"],
          text: args.message,
          ...(typeof args.mode === "string" ? { mode: args.mode } : {}),
          ...(typeof args.clientRequestId === "string"
            ? { clientRequestId: args.clientRequestId }
            : {}),
        });
        if (returned !== undefined)
          return yield* Schema.decodeUnknownEffect(success as Schema.Decoder<unknown>)(
            returned,
          ).pipe(Effect.mapError(unavailable));
      }
      const caller = yield* forwardingCaller(scope);
      const limits = caller.limits;
      if (!readOnly && Option.isSome(requestOwner) && typeof args.clientRequestId !== "string")
        args.clientRequestId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const callerRequestId =
        typeof args.clientRequestId === "string" ? args.clientRequestId : undefined;
      let requestProtocol: PeerRequestProtocol | undefined;
      // Browser reads and actions name the same opening owner. Its verified
      // identity exists independently of a mutation receipt or action id.
      const stableReadIdentity =
        (toolName.startsWith("preview_") ||
          toolName.startsWith("t3_preview_") ||
          toolName === "t3_attachment_inspect" ||
          toolName === "device_commands" ||
          (toolName === "web_read" && args.useSession === true)) &&
        Option.isSome(authority);
      if ((!readOnly && Option.isSome(requestOwner)) || stableReadIdentity) {
        if (peers.requireRequestProtocol === undefined) return yield* unavailable();
        requestProtocol = yield* peers.requireRequestProtocol(
          environmentId,
          options?.expectedLinkIdentity,
        );
      }
      const carriesBare = (value: unknown): boolean =>
        Predicate.isObject(value) &&
        (("bare" in value && value.bare !== undefined) ||
          ("threads" in value && Array.isArray(value.threads) && value.threads.some(carriesBare)) ||
          ("colors" in value && Array.isArray(value.colors) && value.colors.some(carriesBare)));
      const carriesTools = (value: unknown): boolean =>
        Predicate.isObject(value) &&
        (("tools" in value && value.tools !== undefined) ||
          ("threads" in value &&
            Array.isArray(value.threads) &&
            value.threads.some(carriesTools)) ||
          ("colors" in value && Array.isArray(value.colors) && value.colors.some(carriesTools)));
      if (carriesTools(args) || toolName === "t3_thread_mcp_configure") {
        const capabilities = yield* peers.call({
          environmentId,
          tool: "orchestrator_capabilities",
          arguments: {},
          limits,
          success: OrchestratorMcpCapabilitiesResult,
          ...(options === undefined ? {} : options),
        });
        if (capabilities.toolAllowlist !== true)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The destination does not advertise tool allowlists; nothing was forwarded.",
          });
      }
      if (carriesBare(args)) {
        const capabilities = yield* peers.call({
          environmentId,
          tool: "orchestrator_capabilities",
          arguments: {},
          limits,
          success: OrchestratorMcpCapabilitiesResult,
          ...(options === undefined ? {} : options),
        });
        if (capabilities.bareLaunch !== true)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The destination does not advertise bare launch; nothing was forwarded.",
          });
      }
      const carriesPurpose =
        args.purposeIntent !== undefined ||
        args.delegatedContinuation !== undefined ||
        (Predicate.isObject(args.delegatedFrom) &&
          "purposeLaunch" in args.delegatedFrom &&
          args.delegatedFrom.purposeLaunch !== undefined);
      if (carriesPurpose) {
        const capabilities = yield* peers.call({
          environmentId,
          tool: "orchestrator_capabilities",
          arguments: {},
          limits,
          success: OrchestratorMcpCapabilitiesResult,
          ...(options === undefined ? {} : options),
        });
        if (capabilities.executionPurposeVersions?.includes(1) !== true) {
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "The destination does not advertise execution purpose version 1; nothing was forwarded.",
          });
        }
      }
      // A pending upload lives here, so it cannot go with the call.
      const receipts = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);
      let expectedLinkIdentity = options?.expectedLinkIdentity;
      if (Option.isSome(receipts) && typeof args.clientRequestId === "string" && !readOnly) {
        const identity =
          (yield* RequestReceipts.CurrentRequest) ??
          (yield* receipts.value.identify(
            scope,
            toolName,
            { environmentId, ...args },
            args.clientRequestId,
          ));
        const destination = yield* links.get(environmentId).pipe(Effect.mapError(unavailable));
        if (Option.isNone(destination)) return yield* unavailable();
        if (
          expectedLinkIdentity !== undefined &&
          destination.value.launchIdentity !== expectedLinkIdentity
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The original destination grant was replaced.",
          });
        // Keep this obligation on the source as well: a replacement peer or an
        // imported destination ledger cannot reinterpret historical source intent.
        // A batch has one distinct caller key per actual downstream operation.
        yield* receipts.value.bindDestination(
          identity,
          RequestReceipts.requestDigest([toolName, args.clientRequestId]),
          environmentId,
          destination.value.launchIdentity,
        );
        expectedLinkIdentity = destination.value.launchIdentity;
      }
      if (toolName === "device_open") {
        if (Option.isNone(authority) || expectedLinkIdentity === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The destination cannot negotiate typed device commands before opening.",
          });
        // A distinct authenticated READ action, not the open action's payload or
        // cached support. Both calls remain bound to the original grant generation.
        const discoveryArgs = {
          ...(args.hostId === undefined ? {} : { hostId: args.hostId }),
          ...(args.contextThreadId === undefined ? {} : { contextThreadId: args.contextThreadId }),
        };
        const discoveryAuthority = yield* authority.value.prepare(
          scope,
          environmentId,
          expectedLinkIdentity,
          "device_commands",
          discoveryArgs,
          limits,
          undefined,
          true,
        );
        yield* peers.call({
          environmentId,
          tool: "device_commands",
          arguments: discoveryArgs,
          limits,
          success: DeviceCommandCatalog,
          authority: discoveryAuthority,
          expectedLinkIdentity,
        });
      }
      if (toolName === "device_execute" || toolName === "preview_upload") {
        const intent =
          toolName === "device_execute"
            ? yield* Schema.decodeUnknownEffect(DeviceCommandInput)(args).pipe(
                Effect.mapError(unavailable),
              )
            : undefined;
        const preview =
          toolName === "preview_upload"
            ? yield* Schema.decodeUnknownEffect(PreviewAutomationUploadInput)(args).pipe(
                Effect.mapError(unavailable),
              )
            : undefined;
        if (preview?.paths !== undefined && preview.paths.length > 0)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "A linked browser requires qualified artifacts; source paths are never destination paths.",
          });
        if (
          preview?.artifacts !== undefined &&
          (preview.resource === undefined ||
            preview.resource.environmentId !== environmentId ||
            preview.resource.threadId !== args.contextThreadId ||
            (preview.tabId !== undefined && preview.tabId !== preview.resource.tabId))
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Qualified browser input requires its exact opened destination context.",
          });
        const steps =
          intent === undefined
            ? []
            : intent.command.type === "batch"
              ? intent.command.steps
              : [intent.command];
        const inputs =
          preview?.artifacts ??
          steps.flatMap((command) =>
            command.type === "install" || command.type === "reinstall" ? [command.artifact] : [],
          );
        let previewCurrent: Effect.Effect<void, OrchestratorMcpFailure> | undefined;
        if (preview?.artifacts !== undefined) {
          if (
            Option.isNone(authority) ||
            expectedLinkIdentity === undefined ||
            requestProtocol?.attachments !== true ||
            preview.resource === undefined
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "This peer cannot negotiate qualified browser input before dispatch.",
            });
          const original = yield* McpInvocationContext.requireThreadScope(
            scope,
            "Qualified browser input",
          );
          const actualAuthority = authority.value;
          const resource = preview.resource;
          const generation = expectedLinkIdentity;
          const context =
            args.contextThreadId === undefined ? {} : { contextThreadId: args.contextThreadId };
          previewCurrent = Effect.gen(function* () {
            yield* actualAuthority.recheckInvocation(original, "preview", original.thread.threadId);
            const support = yield* forward<PeerAttachmentInspectResult>(
              scope,
              "t3_attachment_inspect",
              Schema.Struct({
                source: PeerAttachmentInspectSource,
                contextThreadId: Schema.optional(ThreadId),
              }),
              PeerAttachmentInspectResult,
              environmentId,
              { ...context, source: { type: "capabilities" } },
              { expectedLinkIdentity: generation },
              true,
            );
            if (support.type !== "capabilities" || support.previewInputArtifacts !== 1)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Qualified native browser input is unsupported by this peer.",
              });
            const status = yield* forward<PreviewAutomationStatus>(
              scope,
              "preview_status",
              Schema.Struct({ tabId: PreviewTabId, contextThreadId: Schema.optional(ThreadId) }),
              PreviewAutomationStatus,
              environmentId,
              { ...context, tabId: resource.tabId },
              { expectedLinkIdentity: generation },
              true,
            );
            if (
              status.resource === undefined ||
              RequestReceipts.requestDigest(status.resource) !==
                RequestReceipts.requestDigest(resource) ||
              status.control?.owner === "human" ||
              (status.control?.owner === "agent" && !status.control.ownedByCaller)
            )
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "The exact browser input opening or its control is no longer available.",
              });
          });
          yield* previewCurrent;
        }
        if (inputs.some((reference) => reference.environmentId !== environmentId)) {
          const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
          const fs = yield* Effect.serviceOption(FileSystem.FileSystem);
          if (
            Option.isNone(staging) ||
            Option.isNone(fs) ||
            Option.isNone(authority) ||
            scope.thread === undefined ||
            expectedLinkIdentity === undefined ||
            requestProtocol?.attachments !== true ||
            typeof args.clientRequestId !== "string"
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Qualified input transfer is unsupported; no upload was accepted.",
            });
          const original = yield* McpInvocationContext.requireThreadScope(
            scope,
            "Qualified input transfer",
          );
          const generation = expectedLinkIdentity;
          const destination = `peer:${environmentId}:${generation}`;
          const consumer = `peer-request:${RequestReceipts.requestDigest([requestNamespace(scope), toolName, args.clientRequestId])}`;
          const source = inputs.filter((reference) => reference.environmentId !== environmentId);
          if (source.some((reference) => reference.environmentId !== scope.environmentId))
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Stage an app artifact in its actual source environment before forwarding.",
            });
          const inspectParameters = Schema.Struct({
            source: PeerAttachmentInspectSource,
            contextThreadId: Schema.optional(ThreadId),
          });
          const catalogParameters = Schema.Struct({
            hostId: DeviceHostId,
            target: DeviceCommandTarget,
            contextThreadId: Schema.optional(ThreadId),
          });
          const context =
            args.contextThreadId === undefined ? {} : { contextThreadId: args.contextThreadId };
          const current = Effect.gen(function* () {
            yield* authority.value.recheckInvocation(
              original,
              preview === undefined ? "device" : "preview",
              original.thread.threadId,
            );
            if (previewCurrent !== undefined) return yield* previewCurrent;
            if (intent === undefined) return yield* unavailable();
            const catalog = yield* forward<DeviceCommandCatalog>(
              scope,
              "device_commands",
              catalogParameters,
              DeviceCommandCatalog,
              environmentId,
              { ...context, hostId: intent.target.hostId, target: intent.target },
              { expectedLinkIdentity: generation },
              true,
            );
            if (
              catalog.inputArtifacts !== 1 ||
              catalog.targetPlatform === undefined ||
              steps.some((command) => !catalog.commands.includes(command.type))
            )
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "This peer cannot preflight typed app input on the exact opening.",
              });
            for (const reference of source) {
              const extension = reference.name.slice(reference.name.lastIndexOf(".")).toLowerCase();
              if (
                !(catalog.targetPlatform === "android" ? [".apk", ".aab"] : [".zip"]).includes(
                  extension,
                )
              )
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "The app package is unsupported by this exact device platform.",
                });
            }
          });
          yield* authority.value.recheckInvocation(
            original,
            preview === undefined ? "device" : "preview",
            original.thread.threadId,
          );
          const support = yield* forward<PeerAttachmentInspectResult>(
            scope,
            "t3_attachment_inspect",
            inspectParameters,
            PeerAttachmentInspectResult,
            environmentId,
            { ...context, source: { type: "capabilities" } },
            { expectedLinkIdentity: generation },
            true,
          );
          if (
            support.type !== "capabilities" ||
            support.version !== 1 ||
            (preview !== undefined && support.previewInputArtifacts !== 1)
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "The peer cannot verify typed app input before transfer.",
            });
          const manifest = (reference: PeerAttachmentReference) => ({
            type: "file" as const,
            id: reference.attachmentId,
            name: reference.name,
            mimeType: reference.mimeType,
            sizeBytes: reference.sizeBytes,
          });
          if (preview !== undefined) {
            // Existing retained transfers need no new copy. A fresh mixed batch
            // must prove every destination-owned input before reserving any source input.
            const retained = yield* Effect.forEach(source, (reference) =>
              staging.value.transferReference(scope, consumer, destination, manifest(reference)),
            );
            if (retained.some(Option.isNone))
              for (const reference of inputs.filter(
                (value) => value.environmentId === environmentId,
              )) {
                yield* current;
                const inspected = yield* forward<PeerAttachmentInspectResult>(
                  scope,
                  "t3_attachment_inspect",
                  inspectParameters,
                  PeerAttachmentInspectResult,
                  environmentId,
                  {
                    ...context,
                    source: {
                      type: "upload",
                      attachmentId: reference.attachmentId,
                      upload: {
                        type: "file",
                        name: reference.name,
                        mimeType: reference.mimeType,
                        sizeBytes: reference.sizeBytes,
                      },
                    },
                  },
                  { expectedLinkIdentity: generation },
                  true,
                );
                if (
                  inspected.type !== "upload" ||
                  inspected.inspection.stage !== "staged" ||
                  RequestReceipts.requestDigest(inspected.inspection.reference) !==
                    RequestReceipts.requestDigest(reference)
                )
                  return yield* new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message:
                      "Every destination input must be verified before a new source transfer.",
                  });
              }
          }
          // The existing reservation persists exact original bytes. A confirmed
          // transfer may later be consumed, so retry does not reclassify it as staged.
          yield* current;
          yield* staging.value.reserveTransfer(
            scope,
            consumer,
            destination,
            source.map(manifest),
            source,
          );
          const transferred = new Map<string, PeerAttachmentReference>();
          for (const reference of source) {
            yield* current;
            const attachment = manifest(reference);
            const retained = yield* staging.value.transferReference(
              scope,
              consumer,
              destination,
              attachment,
            );
            if (Option.isSome(retained)) {
              if (
                retained.value.environmentId !== environmentId ||
                retained.value.digest !== reference.digest
              )
                return yield* unavailable();
              transferred.set(reference.attachmentId, retained.value);
              continue;
            }
            const upload = {
              type: "file" as const,
              name: reference.name,
              mimeType: reference.mimeType,
              sizeBytes: reference.sizeBytes,
            };
            yield* current;
            const prepared = yield* forward<PeerAttachmentUploadResult>(
              scope,
              "t3_attachment_prepare_upload",
              uploadParameters,
              PeerAttachmentUploadResult,
              environmentId,
              {
                ...context,
                upload,
                digest: reference.digest,
                clientRequestId: `${preview === undefined ? "device-input" : "preview-input"}:${RequestReceipts.requestDigest([consumer, reference.attachmentId])}`,
              },
              { expectedLinkIdentity: generation },
            );
            if (prepared.transferState?.stage !== "staged") {
              if (prepared.transferState?.stage === "native-accepted")
                return yield* new OrchestratorMcpFailure({
                  code: "outcome_unknown",
                  message:
                    "The original destination input was consumed before its transfer reference was retained.",
                  lastConfirmedStage: "native_binding_accepted",
                });
              const verified = yield* staging.value.verify(scope, attachment);
              if (verified.digest !== reference.digest) return yield* unavailable();
              yield* current;
              yield* peers.uploadAttachment({
                environmentId,
                generation,
                relativeUrl: prepared.relativeUrl,
                bytes: fs.value
                  .stream(verified.path, { chunkSize: 1024 * 1024 })
                  .pipe(Stream.tap(() => current)),
                sizeBytes: reference.sizeBytes,
              });
            }
            yield* current;
            const inspected = yield* forward<PeerAttachmentInspectResult>(
              scope,
              "t3_attachment_inspect",
              inspectParameters,
              PeerAttachmentInspectResult,
              environmentId,
              {
                ...context,
                source: { type: "upload", attachmentId: prepared.attachmentId, upload },
              },
              { expectedLinkIdentity: generation },
              true,
            );
            if (inspected.type !== "upload" || inspected.inspection.stage !== "staged")
              return yield* new OrchestratorMcpFailure({
                code: "outcome_unknown",
                message:
                  "The original destination input is not verified unconsumed staging; no installation was retried.",
                lastConfirmedStage: "native_binding_accepted",
              });
            const actual = inspected.inspection.reference;
            if (actual.environmentId !== environmentId || actual.digest !== reference.digest)
              return yield* unavailable();
            yield* current;
            yield* staging.value.transferReference(
              scope,
              consumer,
              destination,
              attachment,
              actual,
            );
            transferred.set(reference.attachmentId, actual);
          }
          yield* current;
          if (preview !== undefined) {
            args.artifacts = inputs.map((reference) =>
              reference.environmentId === scope.environmentId
                ? (transferred.get(reference.attachmentId) ?? reference)
                : reference,
            );
          } else if (intent !== undefined) {
            const replace = (command: DeviceAtomicCommand): DeviceAtomicCommand =>
              command.type === "install" || command.type === "reinstall"
                ? {
                    ...command,
                    artifact: transferred.get(command.artifact.attachmentId) ?? command.artifact,
                  }
                : command;
            args.command =
              intent.command.type === "batch"
                ? { ...intent.command, steps: intent.command.steps.map(replace) }
                : replace(intent.command);
          }
        }
      }
      // Transfer explicitly source-owned bytes, never a source path or an unqualified foreign id.
      if (
        (toolName === "t3_thread_launch" || toolName === "t3_thread_send_attachments") &&
        Array.isArray(args.attachments) &&
        args.attachments.length > 0
      ) {
        const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
        const fs = yield* Effect.serviceOption(FileSystem.FileSystem);
        const destination = yield* links.get(environmentId).pipe(Effect.mapError(unavailable));
        if (
          Option.isNone(staging) ||
          Option.isNone(fs) ||
          Option.isNone(destination) ||
          Option.isNone(authority)
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "Durable peer attachment transfer is unavailable.",
          });
        if (
          expectedLinkIdentity !== undefined &&
          expectedLinkIdentity !== destination.value.launchIdentity
        )
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "The original attachment destination grant was replaced.",
          });
        if (requestProtocol?.attachments !== true)
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "The destination does not support peer attachments v1; no transfer or launch was accepted.",
          });
        const key =
          typeof args.clientRequestId === "string"
            ? args.clientRequestId
            : yield* crypto.randomUUIDv4().pipe(Effect.orDie);
        args.clientRequestId = key;
        const sourceAttachments = yield* decodeAttachments(args.attachments).pipe(
          Effect.mapError(unavailable),
        );
        const sourceOwned = sourceAttachments.filter(
          (attachment) => attachment.environmentId !== environmentId,
        );
        if (
          sourceOwned.some(
            (attachment) =>
              attachment.environmentId !== undefined &&
              attachment.environmentId !== scope.environmentId,
          )
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "An attachment belongs to another environment.",
          });
        const consumer = `peer-request:${RequestReceipts.requestDigest([requestNamespace(scope), toolName, key])}`;
        yield* staging.value.reserveTransfer(scope, consumer, `peer:${environmentId}`, sourceOwned);
        args.attachments = yield* Effect.forEach(args.attachments, (raw, index) =>
          Effect.gen(function* () {
            const attachment = yield* decodeAttachment(raw).pipe(Effect.mapError(unavailable));
            const ownerEnvironment =
              Predicate.isObject(raw) && "environmentId" in raw
                ? raw.environmentId
                : scope.environmentId;
            if (ownerEnvironment === environmentId) return attachment;
            if (ownerEnvironment !== scope.environmentId)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "Stage an attachment from its owning environment before forwarding it.",
              });
            const digest = yield* staging.value.transferDigest(
              scope,
              consumer,
              `peer:${environmentId}`,
              attachment,
            );
            const request = {
              upload: {
                type: attachment.type,
                name: attachment.name,
                mimeType: attachment.mimeType,
                sizeBytes: attachment.sizeBytes,
              },
              digest,
              clientRequestId: `transfer:${RequestReceipts.requestDigest([toolName, key, index, attachment.id])}`,
            };
            const uploaded = yield* forward<PeerAttachmentUploadResult>(
              scope,
              "t3_attachment_prepare_upload",
              uploadParameters,
              PeerAttachmentUploadResult,
              environmentId,
              request,
              { expectedLinkIdentity: destination.value.launchIdentity },
            );
            if (
              uploaded.transferState?.stage === "staged" ||
              uploaded.transferState?.stage === "native-accepted"
            ) {
              if (uploaded.transferState.digest !== digest)
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message:
                    "The destination's original attachment manifest does not match this verified source.",
                });
            } else {
              const source = yield* staging.value.verify(scope, attachment);
              if (source.digest !== digest)
                return yield* new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "The prepared source bytes changed before transfer.",
                });
              yield* peers.uploadAttachment({
                environmentId,
                generation: destination.value.launchIdentity,
                relativeUrl: uploaded.relativeUrl,
                bytes: fs.value.stream(source.path),
                sizeBytes: attachment.sizeBytes,
              });
            }
            return { ...attachment, id: uploaded.attachmentId };
          }),
        );
      }
      if (typeof args.clientRequestId === "string") {
        args.clientRequestId = yield* scopedRequestId(
          requestNamespace(scope),
          args.clientRequestId,
        );
      }
      const link = Option.isSome(authority)
        ? yield* links.get(environmentId).pipe(Effect.mapError(unavailable))
        : Option.none();
      if (
        expectedLinkIdentity !== undefined &&
        Option.isSome(link) &&
        link.value.launchIdentity !== expectedLinkIdentity
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "The destination grant changed before dispatch.",
        });
      let envelope =
        Option.isSome(authority) && Option.isSome(link)
          ? yield* authority.value.prepare(
              scope,
              environmentId,
              link.value.launchIdentity,
              toolName,
              args,
              limits,
              options?.pair,
              readOnly,
              (!readOnly && Option.isSome(requestOwner)) || stableReadIdentity,
            )
          : undefined;
      if (toolName === "schedule_task") {
        const parsed = yield* decodeSecretSchedule(args).pipe(Effect.mapError(unavailable));
        const handle =
          parsed.schedule?.type === "webhook"
            ? parsed.schedule.signature?.secretRequest
            : undefined;
        if (handle !== undefined) {
          if (
            handle.environmentId !== environmentId ||
            parsed.projectId === undefined ||
            typeof args.clientRequestId !== "string" ||
            envelope?.requestOwner === undefined ||
            Option.isNone(link) ||
            callerRequestId === undefined ||
            peers.remoteSecret === undefined
          )
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Remote secret consumption needs its exact keyed destination and project.",
            });
          yield* peers.remoteSecret(environmentId, link.value.launchIdentity);
          const operation = {
            id: RequestReceipts.requestDigest([
              envelope.requestOwner.namespace,
              toolName,
              args.clientRequestId,
            ]),
            clientRequestId: args.clientRequestId,
            generation: envelope.requestOwner.generation,
            inputDigest: RequestReceipts.requestDigest(
              Object.fromEntries(
                Object.entries(args).filter(([key]) => key !== "contextCapabilities"),
              ),
            ),
          };
          const query = {
            handle,
            projectId: parsed.projectId,
            envelope,
            request: operation,
          } satisfies RemoteSecretReceiptQuery;
          if (Option.isNone(secretSql)) return yield* unavailable();
          const sql = secretSql.value;
          yield* RemoteSecretPersistence.schema.pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.mapError(unavailable),
          );
          yield* sql`INSERT INTO remote_secret_consumers(operation_id,namespace,client_key,input_digest,generation,environment_id,query_json) VALUES (${operation.id},${requestNamespace(scope)},${callerRequestId},${RequestReceipts.requestInputDigest(encoded)},${link.value.launchIdentity},${environmentId},${RequestReceipts.canonicalRequest(query)}) ON CONFLICT DO NOTHING`.pipe(
            Effect.mapError(unavailable),
          );
          const [retained] = yield* sql<{
            input_digest: string;
            generation: string;
            query_json: string;
          }>`SELECT input_digest,generation,query_json FROM remote_secret_consumers WHERE namespace=${requestNamespace(scope)} AND client_key=${callerRequestId}`.pipe(
            Effect.mapError(unavailable),
          );
          if (
            retained === undefined ||
            retained.input_digest !== RequestReceipts.requestInputDigest(encoded) ||
            retained.generation !== link.value.launchIdentity ||
            Option.isNone(authority)
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message: "The consumer is bound to its original intent and grant generation.",
            });
          const original = yield* decodeSecretReceiptQuery(retained.query_json).pipe(
            Effect.mapError(unavailable),
          );
          if (
            RequestReceipts.canonicalRequest(original.request) !==
            RequestReceipts.canonicalRequest(operation)
          )
            return yield* unavailable();
          yield* authority.value.checkInteractiveRecord(original.envelope.recordId);
          envelope = original.envelope;
        }
      }
      const result = yield* peers
        .call({
          environmentId,
          tool: toolName,
          arguments: args,
          limits,
          success: success as Schema.Decoder<unknown>,
          ...(toolName.startsWith("t3_pending_request_") &&
          caller.caller !== undefined &&
          isPromachosThread(caller.caller)
            ? { promachosThreadId: caller.caller.id }
            : {}),
          ...(envelope === undefined ? {} : { authority: envelope }),
          ...(options === undefined ? {} : options),
          ...(expectedLinkIdentity === undefined ? {} : { expectedLinkIdentity }),
        })
        .pipe(
          Effect.catchTags({
            OrchestratorMcpFailure: (error) =>
              Effect.fail(
                error.code === "outcome_unknown" && callerRequestId !== undefined
                  ? new OrchestratorMcpFailure({
                      code: error.code,
                      message: error.message,
                      requestId: callerRequestId,
                      ...(error.lastConfirmedStage === undefined
                        ? {}
                        : { lastConfirmedStage: error.lastConfirmedStage }),
                    })
                  : error,
              ),
          }),
        );
      if (envelope !== undefined && options?.pair !== undefined && Option.isSome(authority)) {
        const child = yield* decodeLaunchedChild(result).pipe(Effect.mapError(unavailable));
        yield* authority.value.registerPair(envelope, {
          parentEnvironmentId: scope.environmentId,
          parentThreadId: options.pair.parentThreadId,
          taskId: options.pair.taskId,
          childEnvironmentId: environmentId,
          childThreadId: child.threadId,
          childRunId: child.runId,
        });
      }
      return result;
    },
  );
  const forward = <A>(...args: Parameters<typeof forwardDecoded>) =>
    forwardDecoded(...args) as Effect.Effect<A, OrchestratorMcpFailure>;

  const call: PeerForwarding["Service"]["call"] = (scope, tool, environmentId, params, options) =>
    forward(
      scope,
      tool.name,
      tool.parametersSchema,
      tool.successSchema,
      environmentId,
      params,
      options,
      (toolEffect(tool, params) ??
        (Context.get(tool.annotations, Tool.Readonly) ? "read" : "owned")) === "read",
    );

  const route: PeerForwarding["Service"]["route"] = (scope, tool, params, here) => {
    const target = remoteTarget(scope, params.environmentId);
    return target === undefined ? here : call(scope, tool, target, params);
  };

  const readThread: PeerForwarding["Service"]["readThread"] = (scope, environmentId, input) =>
    forward<OrchestratorMcpThreadReadResult>(
      scope,
      "t3_thread_read",
      OrchestratorMcpThreadReadInput,
      OrchestratorMcpThreadReadResult,
      environmentId,
      input,
      undefined,
      true,
    );

  const interruptThread: PeerForwarding["Service"]["interruptThread"] = (
    scope,
    environmentId,
    input,
    options,
  ) =>
    forward<OrchestratorMcpThreadInterruptResult>(
      scope,
      "t3_thread_interrupt",
      OrchestratorMcpThreadInterruptInput,
      OrchestratorMcpThreadInterruptResult,
      environmentId,
      input,
      options,
    );

  const waitForThread: PeerForwarding["Service"]["waitForThread"] = Effect.fn(
    "PeerForwarding.waitForThread",
  )(function* (scope, environmentId, input) {
    let remaining = Math.min(
      MAX_WAIT_TIMEOUT_MS,
      Math.max(1, input.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS),
    );
    let runId = input.runId;
    while (true) {
      const chunk = Math.min(remaining, FORWARDED_WAIT_CHUNK_MS);
      const result = yield* forward<OrchestratorMcpThreadWaitResult>(
        scope,
        "t3_thread_wait",
        OrchestratorMcpThreadWaitInput,
        OrchestratorMcpThreadWaitResult,
        environmentId,
        { threadId: input.threadId, ...(runId === undefined ? {} : { runId }), timeoutMs: chunk },
        undefined,
        true,
      );
      remaining -= chunk;
      if (!result.timedOut || remaining <= 0) return result;
      // Later pieces wait for the run the first one picked, not a newer one.
      runId = result.runId ?? undefined;
    }
  });

  const listLinks: PeerForwarding["Service"]["links"] = (scope) =>
    links.list.pipe(
      Effect.mapError(
        (error) =>
          new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
      ),
      Effect.map((listed) => ({
        environmentId: scope.environmentId,
        links: listed.map((link) => ({
          environmentId: link.environmentId,
          label: link.label,
          status: link.status,
          access: link.access,
          expiresAt: DateTime.formatIso(link.expiresAt),
          lastError: link.lastError,
        })),
      })),
    );

  const authorizeSavedPreview: PeerForwarding["Service"]["authorizeSavedPreview"] = Effect.fn(
    "PeerForwarding.authorizeSavedPreview",
  )(function* (scope, environmentId, parameters, request) {
    if (
      Option.isNone(authority) ||
      Option.isNone(requestOwner) ||
      peers.authorizeSavedPreview === undefined ||
      parameters.contextThreadId === undefined ||
      !scope.capabilities.has("preview")
    )
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "Saved-preview authorization needs its verified source, preview capability and destination context.",
      });
    const { limits } = yield* forwardingCaller(scope);
    const link = yield* links.get(environmentId).pipe(Effect.mapError(unavailable));
    if (Option.isNone(link))
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The original saved-preview link is no longer available.",
      });
    const receipts = requestOwner.value;
    const operation = "saved-preview-authorization";
    const recorded = yield* receipts.destination(request, operation);
    if (Option.isNone(recorded) && Option.isSome(yield* receipts.replay(request))) {
      // A pre-path accepted save can use only its exact existing forwarding proof.
      // Missing proof is unsupported, never bound to today's replacement grant.
      const root = yield* receipts.identify(
        scope,
        "preview_snapshot",
        { ...parameters, environmentId, contextCapabilities: [...scope.capabilities] },
        request.clientRequestId,
      );
      const original = yield* receipts.destination(
        root,
        RequestReceipts.requestDigest(["preview_snapshot", request.clientRequestId]),
      );
      if (Option.isNone(original))
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message:
            "The saved preview has no verified original destination binding; automatic replay is unsupported.",
        });
      yield* receipts.bindDestination(
        request,
        operation,
        original.value.environmentId,
        original.value.generation,
      );
    }
    yield* receipts.bindDestination(request, operation, environmentId, link.value.launchIdentity);
    const input = {
      version: 1 as const,
      purpose: "saved-preview-authorization" as const,
      environmentId,
      generation: link.value.launchIdentity,
      contextThreadId: parameters.contextThreadId,
      operationId: request.id,
    };
    const envelope = yield* authority.value.prepare(
      scope,
      environmentId,
      link.value.launchIdentity,
      "preview_snapshot",
      input,
      limits,
      undefined,
      true,
      true,
    );
    yield* peers.authorizeSavedPreview({ request: input, authority: envelope, limits });
  });
  return PeerForwarding.of({
    authorizeSavedPreview,
    call,
    route,
    readThread,
    interruptThread,
    waitForThread,
    links: listLinks,
  });
});

export const layer = Layer.effect(PeerForwarding, make);
