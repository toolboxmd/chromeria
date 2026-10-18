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
});
const make = Effect.gen(function* () {
  const peers = yield* PeerMcpClient.PeerMcpClient;
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
  const forwardingCaller = (scope: Scope) =>
    loadCaller().pipe(
      Effect.tap((caller) =>
        callerLinkOrigin(caller) === undefined
          ? assertNotLinked(caller, "use this environment's links")
          : Option.isSome(authority) && scope.thread !== undefined
            ? authority.value.forThread(scope.thread.threadId).pipe(
                Effect.flatMap((record) =>
                  record?.ceiling.onward === true
                    ? Effect.void
                    : Effect.fail(
                        new OrchestratorMcpFailure({
                          code: "capability_denied",
                          message: "This child's durable grant does not permit onward delegation.",
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
  const forward = <A>(
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
    readOnly = false,
  ): Effect.Effect<A, OrchestratorMcpFailure> =>
    Effect.gen(function* () {
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
          return yield* Schema.decodeUnknownEffect(success as Schema.Decoder<A>)(returned).pipe(
            Effect.mapError(unavailable),
          );
      }
      const caller = yield* forwardingCaller(scope);
      const limits = caller.limits;
      if (!readOnly && Option.isSome(requestOwner) && typeof args.clientRequestId !== "string")
        args.clientRequestId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const callerRequestId =
        typeof args.clientRequestId === "string" ? args.clientRequestId : undefined;
      let requestProtocol: PeerRequestProtocol | undefined;
      if (!readOnly && Option.isSome(requestOwner)) {
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
      const envelope =
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
              !readOnly && Option.isSome(requestOwner),
            )
          : undefined;
      const result = yield* peers
        .call({
          environmentId,
          tool: toolName,
          arguments: args,
          limits,
          success: success as Schema.Decoder<A>,
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
    });

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

  const waitForThread: PeerForwarding["Service"]["waitForThread"] = (scope, environmentId, input) =>
    Effect.gen(function* () {
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

  const authorizeSavedPreview: PeerForwarding["Service"]["authorizeSavedPreview"] = (
    scope,
    environmentId,
    parameters,
    request,
  ) =>
    Effect.gen(function* () {
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
      const limits = yield* callerLimits(scope);
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
