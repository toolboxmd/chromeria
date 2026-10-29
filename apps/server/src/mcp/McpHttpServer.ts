import { RemoteSecretReceiptQuery } from "@t3tools/contracts";
import {
  RemoteSecretControl,
  RemoteSecretRetirement,
  RemoteSecretSupport,
} from "@t3tools/contracts";
import * as RemoteSecretRequests from "../secrets/RemoteSecretRequests.ts";
import { ThreadMcpToolkit } from "./toolkits/threadMcp/tools.ts";
import * as ThreadMcpHandlers from "./toolkits/threadMcp/handlers.ts";

import { PreviewViewerOpenInput, PreviewViewerCloseInput } from "@t3tools/contracts";
import * as ServerBrowser from "../preview/ServerBrowser.ts";
import * as ServerBrowserStream from "../preview/ServerBrowserStream.ts";
import { AuthSessionId } from "@t3tools/contracts";
import { DeviceViewerCloseInput } from "@t3tools/contracts";
import * as DeviceHubProxy from "../device/DeviceHubProxy.ts";
import { DeviceViewerOpenInput } from "@t3tools/contracts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import { PendingRequests } from "../childThreads/PendingRequestService.ts";
import { QualifiedAssetResource, PeerSavedPreviewAuthorizationRequest } from "@t3tools/contracts";
import * as McpRouting from "./McpRouting.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as ServerSettings from "../serverSettings.ts";
import { requestNamespace, requestGeneration } from "./requestIdentity.ts";
import { McpSavedSnapshotReceipt } from "@t3tools/contracts";
import { sha256 } from "@noble/hashes/sha2";
import * as Hex from "effect/encoding/Hex";
import * as PeerTaskLifecycle from "../peer/PeerTaskLifecycle.ts";
import { PeerTaskLifecycleRequest } from "@t3tools/contracts";
import * as SpectrumToolkit from "../spectrum/mcpToolkit.ts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import type * as Types from "effect/Types";
import { AiError, McpProtocol, McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import {
  DeviceToolError,
  OrchestratorMcpFailure,
  PreviewAutomationError,
  ProviderInteractionMode,
  type RunId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";

import packageJson from "../../package.json" with { type: "json" };
import * as ServerConfig from "../config.ts";
import * as DeviceService from "../device/DeviceService.ts";
import * as HtmlRender from "../htmlRender/HtmlRender.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import {
  type AgentThread,
  agentToolProperties,
  handoffSettings,
  MAX_REPORTED_DEPTH,
  toolOutcome,
} from "../telemetry/ProviderDimensions.ts";
import { advertisedJsonSchema } from "./advertisedJsonSchema.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import { PeerAuthorityEnvelope } from "@t3tools/contracts";
import * as McpToolAccess from "./McpToolAccess.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import { PreviewControlsToolkit } from "./toolkits/previewControls/tools.ts";
import * as PreviewControlsHandlers from "./toolkits/previewControls/handlers.ts";
import { EnvironmentToolkit } from "./toolkits/environment/tools.ts";
import * as EnvironmentHandlers from "./toolkits/environment/handlers.ts";
import { ProjectToolkit } from "./toolkits/project/tools.ts";
import * as ProjectHandlers from "./toolkits/project/handlers.ts";
import { AttachmentToolkit } from "./toolkits/attachment/tools.ts";
import * as AttachmentHandlers from "./toolkits/attachment/handlers.ts";
import { ThreadToolkit } from "./toolkits/thread/tools.ts";
import * as ThreadHandlers from "./toolkits/thread/handlers.ts";
import * as ThreadMetadataMcpService from "./ThreadMetadataMcpService.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as PreviewAutomationBroker from "./PreviewAutomationBroker.ts";
import * as OrchestratorHandlers from "./toolkits/orchestrator/handlers.ts";
import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";
import * as PreviewWatchHandlers from "./toolkits/preview/watchHandlers.ts";
import { PreviewWatchToolkit } from "./toolkits/preview/tools.ts";
import * as PreviewHandlers from "./toolkits/preview/handlers.ts";
import {
  PreviewSnapshotTool,
  PreviewSnapshotToolkit,
  PreviewStandardToolkit,
} from "./toolkits/preview/tools.ts";
import * as WorktreeHandlers from "./toolkits/worktree/handlers.ts";
import { WorktreeToolkit } from "./toolkits/worktree/tools.ts";
import * as WorktreeMcpService from "./WorktreeMcpService.ts";
import * as PullRequestsHandlers from "./toolkits/pullRequests/handlers.ts";
import { PullRequestsToolkit } from "./toolkits/pullRequests/tools.ts";
import * as IssuesHandlers from "./toolkits/issues/handlers.ts";
import { IssuesToolkit } from "./toolkits/issues/tools.ts";
import * as DeviceHandlers from "./toolkits/device/handlers.ts";
import {
  DeviceScreenshotTool,
  DeviceScreenshotToolkit,
  DeviceStandardToolkit,
} from "./toolkits/device/tools.ts";
import * as HtmlHandlers from "./toolkits/html/handlers.ts";
import { HtmlPreviewTool, HtmlPreviewToolkit, HtmlRenderToolkit } from "./toolkits/html/tools.ts";
import * as WebResearchHandlers from "./toolkits/webResearch/handlers.ts";
import { WebResearchToolkit } from "./toolkits/webResearch/tools.ts";
import * as WebResearch from "../webResearch/WebResearch.ts";
import * as ResearchHost from "../webResearch/ResearchHost.ts";
import * as SessionRead from "../webResearch/SessionRead.ts";
import * as PageCache from "../webResearch/PageCache.ts";
import * as PublicHttp from "../webResearch/PublicHttp.ts";
import * as PublicRender from "../webResearch/PublicRender.ts";
import * as Search from "../webResearch/Search.ts";
import * as TinyFish from "../webResearch/TinyFish.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as RequestReceipts from "./McpRequestReceipts.ts";
import * as AttachmentStaging from "../assets/AttachmentStaging.ts";

// EnabledWhen is synchronous, so read the request fiber rather than capturing
// a credential when the shared tool catalog is registered.
const bareToolAnnotations = (tool: {
  readonly name: string;
  readonly annotations: Context.Context<never>;
}) => {
  const enabled = Context.getOrUndefined(tool.annotations, McpSchema.EnabledWhen);
  return Context.add(tool.annotations, McpSchema.EnabledWhen, (client) => {
    const fiber = Fiber.getCurrent();
    const scope =
      fiber === undefined
        ? undefined
        : Context.getOrUndefined(fiber.context, McpInvocationContext.McpInvocationContext);
    return (
      (scope?.bareToolNames === undefined || scope.bareToolNames.includes(tool.name)) &&
      (enabled?.(client) ?? true)
    );
  });
};

const mcpResourceMetadataUrl = (request: HttpServerRequest.HttpServerRequest) =>
  HttpServerRequest.toURL(request).pipe(
    Option.map((url) => `${url.origin}/.well-known/oauth-protected-resource/mcp`),
  );

/** Launched agents cannot open OAuth browsers; OAuth would mint credentials outliving their session. */
const unauthorized = (input: {
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly presentedToken: boolean;
  readonly offerOAuth: boolean;
}) => {
  const metadataUrl = input.offerOAuth
    ? Option.getOrUndefined(mcpResourceMetadataUrl(input.request))
    : undefined;
  const challenge = [
    "Bearer",
    [
      ...(metadataUrl === undefined ? [] : [`resource_metadata="${metadataUrl}"`]),
      ...(input.presentedToken ? ['error="invalid_token"'] : []),
    ].join(", "),
  ]
    .filter((part) => part.length > 0)
    .join(" ");
  return HttpServerResponse.jsonUnsafe(
    {
      error: "invalid_mcp_credential",
      message: "A valid T3 Code MCP credential is required.",
    },
    {
      status: 401,
      headers: {
        "cache-control": "no-store",
        "www-authenticate": challenge,
      },
    },
  );
};

export class McpClientAuthenticator extends Context.Service<
  McpClientAuthenticator,
  {
    readonly authenticate: (
      request: HttpServerRequest.HttpServerRequest,
    ) => Effect.Effect<McpInvocationContext.McpInvocationScope | undefined>;
  }
>()("t3/mcp/McpHttpServer/McpClientAuthenticator") {}

type AuthenticatedHttpEffect = Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  McpInvocationContext.McpInvocationContext
>;

type McpAuthMiddleware = (
  httpEffect: AuthenticatedHttpEffect,
) => Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  Types.unhandled,
  HttpServerRequest.HttpServerRequest
>;

const normalizeMcpHttpResponse = (
  response: HttpServerResponse.HttpServerResponse,
): HttpServerResponse.HttpServerResponse => {
  const bodyIsEmpty =
    response.body._tag === "Empty" ||
    (response.body._tag === "Uint8Array" && response.body.contentLength === 0) ||
    (response.body._tag === "Raw" && response.body.contentLength === 0);
  return response.status === 200 && bodyIsEmpty
    ? HttpServerResponse.setStatus(response, 202)
    : response;
};

// Session tokens are `<payload>.<signature>`; registry tokens are a bare base64url secret.
const looksLikeProviderToken = (token: string) => token.length > 0 && !token.includes(".");

const decodeModeLimit = Schema.decodeUnknownOption(
  Schema.Struct({ runtimeMode: RuntimeMode, interactionMode: ProviderInteractionMode }),
);
const parseModeLimit = (value: string) => {
  const [runtimeMode, interactionMode, ...rest] = value.trim().split("/");
  return rest.length === 0 ? decodeModeLimit({ runtimeMode, interactionMode }) : Option.none();
};

const invalidModeLimit = HttpServerResponse.jsonUnsafe(
  {
    error: "invalid_mode_limit",
    message: "T3-Mode-Limit must be <runtimeMode>/<interactionMode>, e.g. auto/default.",
  },
  { status: 400, headers: { "cache-control": "no-store" } },
);

/** Applies a client's requested mode limit; undefined when the header is malformed. */
const withModeLimit = (
  invocation: McpInvocationContext.McpInvocationScope,
  request: HttpServerRequest.HttpServerRequest,
): McpInvocationContext.McpInvocationScope | undefined => {
  const header = request.headers[McpInvocationContext.MODE_LIMIT_HEADER];
  if (header === undefined || invocation.client === undefined) return invocation;
  const limit = parseModeLimit(header);
  return Option.isNone(limit)
    ? undefined
    : { ...invocation, client: { ...invocation.client, narrowedTo: limit.value } };
};

// The Linked environments beta is off here: links wait until it is on again.
const linkedEnvironmentsOff = HttpServerResponse.jsonUnsafe(
  {
    error: "linked_environments_off",
    message: "Linked environments is off on this machine. Turn it on in its Settings → Labs.",
  },
  { status: 403, headers: { "cache-control": "no-store" } },
);
/**
 * A linked environment passes on that its caller is the Promachos there
 * (toolboxmd/chromeria#532). Only a peer link may say so; other clients' headers are ignored.
 */
export const withPromachosThread = (
  invocation: McpInvocationContext.McpInvocationScope | undefined,
  headers: Readonly<Record<string, string | undefined>>,
): McpInvocationContext.McpInvocationScope | undefined => {
  const header = headers[McpInvocationContext.PROMACHOS_THREAD_HEADER]?.trim();
  if (invocation?.client?.linked !== true || header === undefined || header.length === 0)
    return invocation;
  return {
    ...invocation,
    client: { ...invocation.client, promachosThreadId: ThreadId.make(header) },
  };
};
const decodePeerSession = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PeerAuthority.PeerSession),
);
const decodePeerEnvelope = Schema.decodeUnknownEffect(Schema.fromJsonString(PeerAuthorityEnvelope));
const decodeTaskLifecycle = Schema.decodeUnknownEffect(PeerTaskLifecycleRequest);
const decodePeerExchange = Schema.decodeUnknownEffect(PeerAuthority.Exchange);
const isPeerFailure = Schema.is(OrchestratorMcpFailure);
const peerAuthorityFailure = (error: unknown) =>
  HttpServerResponse.jsonUnsafe(
    {
      code: isPeerFailure(error) ? error.code : "invalid_request",
      message: isPeerFailure(error) ? error.message : "Invalid peer authority envelope.",
    },
    { status: isPeerFailure(error) && error.code === "orchestration_error" ? 503 : 403 },
  );

const decodeSecretRetirement = Schema.decodeUnknownEffect(RemoteSecretRetirement);
const decodeSecretControl = Schema.decodeUnknownEffect(RemoteSecretControl);
const decodeSecretReceiptQuery = Schema.decodeUnknownEffect(RemoteSecretReceiptQuery);
const decodeAutomationDisable = Schema.decodeUnknownEffect(AutomationAuthority.AutomationDisable);

const makeMcpAuthMiddleware = Effect.gen(function* () {
  const registry = yield* McpSessionRegistry.McpSessionRegistry;
  const clients = yield* Effect.serviceOption(McpClientAuthenticator);
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const linksEnabled = serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.linkedEnvironmentsEnabled),
    Effect.orElseSucceed(() => false),
  );
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const remoteSecrets = yield* Effect.serviceOption(RemoteSecretRequests.RemoteSecretRequests);
  return Effect.fn("McpHttpServer.authenticateRequest")(function* (httpEffect) {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const authorization = request.headers.authorization;
    const token =
      authorization?.startsWith("Bearer ") === true
        ? authorization.slice("Bearer ".length).trim()
        : "";
    if (request.url.split("?")[0] === "/mcp/peer-control/secrets/retire") {
      if (request.method !== "POST" || Option.isNone(remoteSecrets) || token.length === 0)
        return HttpServerResponse.empty({ status: 403 });
      const retired = yield* Effect.gen(function* () {
        const input = yield* decodeSecretRetirement(yield* request.json);
        return yield* remoteSecrets.value.retire(
          input,
          Hex.encode(sha256(new TextEncoder().encode(token))),
        );
      }).pipe(Effect.result);
      return retired._tag === "Success"
        ? HttpServerResponse.jsonUnsafe(retired.success)
        : HttpServerResponse.empty({ status: 403 });
    }
    if (request.url.split("?")[0] === "/mcp/peer-control/automation-disable") {
      if (request.method !== "POST" || Option.isNone(authority) || token.length === 0)
        return HttpServerResponse.empty({ status: 403 });
      const result = yield* Effect.gen(function* () {
        const input = yield* decodeAutomationDisable(yield* request.json);
        return yield* authority.value.receiveAutomationDisable(
          input,
          Hex.encode(sha256(new TextEncoder().encode(token))),
        );
      }).pipe(Effect.result);
      return result._tag === "Failure"
        ? HttpServerResponse.empty({ status: 403 })
        : result.success === undefined
          ? HttpServerResponse.empty({ status: 202 })
          : HttpServerResponse.jsonUnsafe(result.success);
    }
    const invocation =
      (yield* registry.resolve(token)) ??
      (Option.isSome(clients) && token.length > 0
        ? yield* clients.value.authenticate(request)
        : undefined);
    if (!invocation) {
      yield* Effect.logWarning("rejected MCP request with an unusable credential", {
        reason: token.length === 0 ? "missing_bearer_token" : "unknown_or_expired_token",
      });
      return unauthorized({
        request,
        presentedToken: token.length > 0,
        offerOAuth: Option.isSome(clients) && !looksLikeProviderToken(token),
      });
    }
    const limited = withPromachosThread(withModeLimit(invocation, request), request.headers);
    if (limited === undefined) return invalidModeLimit;
    const linkedEnvironments = yield* linksEnabled;
    if (limited.client?.linked === true && !linkedEnvironments) return linkedEnvironmentsOff;
    const peerHeader = request.headers[PeerAuthority.LINK_HEADER];
    const envelopeHeader = request.headers[PeerAuthority.AUTHORITY_HEADER];
    let bound = limited;
    if (limited.thread !== undefined && Option.isSome(authority)) {
      const checked = yield* authority.value.forThread(limited.thread.threadId).pipe(Effect.result);
      if (checked._tag === "Failure") return peerAuthorityFailure(checked.failure);
      const frozen = checked.success;
      if (frozen !== undefined)
        bound = {
          ...limited,
          authority: frozen,
          capabilities: new Set(
            [...limited.capabilities].filter((capability) =>
              frozen.ceiling.capabilities.includes(capability),
            ),
          ),
        };
    }
    if (peerHeader !== undefined || envelopeHeader !== undefined) {
      if (Option.isNone(authority))
        return HttpServerResponse.jsonUnsafe(
          { error: "unsupported_peer_authority" },
          { status: 409 },
        );
      const verified = yield* Effect.gen(function* () {
        if (peerHeader !== undefined) {
          const peer = yield* decodePeerSession(peerHeader);
          yield* authority.value.enroll(
            limited,
            peer,
            Hex.encode(sha256(new TextEncoder().encode(token))),
          );
        }
        if (envelopeHeader === undefined) return limited;
        const envelope = yield* decodePeerEnvelope(envelopeHeader);
        const record = yield* authority.value.accept(limited, envelope);
        return {
          ...limited,
          authority: record,
          requestNamespace: record.envelope.requestOwner?.namespace ?? limited.requestNamespace,
          capabilities: new Set(record.ceiling.capabilities),
        };
      }).pipe(Effect.result);
      if (verified._tag === "Failure") return peerAuthorityFailure(verified.failure);
      bound = verified.success;
    }
    return yield* httpEffect.pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        ...bound,
        linkedEnvironments,
      }),
      Effect.map(normalizeMcpHttpResponse),
      Effect.map((response) =>
        Option.isSome(authority)
          ? HttpServerResponse.setHeader(response, PeerAuthority.AUTHORITY_HEADER, "1")
          : response,
      ),
    );
  }) satisfies McpAuthMiddleware;
}).pipe(Effect.withSpan("McpHttpServer.makeAuthMiddleware"));

const layerMcpAuthMiddleware = HttpRouter.middleware<{
  provides: McpInvocationContext.McpInvocationContext;
}>()(makeMcpAuthMiddleware).layer;

/** Claude Code spills oversized results to files, losing locators; bound both text and its preferred structuredContent. */
export const MAX_SNAPSHOT_TEXT_BYTES = 20_000;
const MAX_SNAPSHOT_VISIBLE_TEXT_CHARS = 8_000;
const MAX_SNAPSHOT_ELEMENT_NAME_CHARS = 200;
const MAX_SNAPSHOT_LOG_ENTRIES = 40;
const MAX_SNAPSHOT_LOG_TEXT_CHARS = 500;
const MAX_SNAPSHOT_IDENTIFIER_CHARS = 2_048;

const encodeJsonText = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const utf8Length = (text: string) => Buffer.byteLength(text, "utf8");
const cutText = (text: string, max: number) =>
  text.length > max ? `${text.slice(0, max)}…` : text;

const cutEntryStrings = <A>(entry: A): A =>
  typeof entry === "object" && entry !== null
    ? (Object.fromEntries(
        Object.entries(entry).map(([key, value]) => [
          key,
          typeof value === "string" ? cutText(value, MAX_SNAPSHOT_LOG_TEXT_CHARS) : value,
        ]),
      ) as A)
    : entry;

const hasLongString = (entry: unknown, max: number) =>
  typeof entry === "object" &&
  entry !== null &&
  Object.values(entry).some((value) => typeof value === "string" && value.length > max);

type SnapshotMetadata = {
  readonly url: string;
  readonly title: string;
  readonly visibleText: string;
  readonly interactiveElements: ReadonlyArray<{
    readonly name: string;
    readonly [key: string]: unknown;
  }>;
  readonly consoleEntries: ReadonlyArray<unknown>;
  readonly networkEntries: ReadonlyArray<unknown>;
  readonly actionTimeline: ReadonlyArray<unknown>;
  readonly [key: string]: unknown;
};

const boundSnapshotMetadata = (metadata: SnapshotMetadata) => {
  const omitted: Array<string> = [];
  const { accessibilityTree, ...withoutTree } = metadata;
  const ariaTree = typeof accessibilityTree === "string" ? accessibilityTree : undefined;
  if (accessibilityTree !== undefined && ariaTree === undefined) {
    omitted.push("accessibilityTree (use interactiveElements locators or preview_evaluate)");
  }
  const tail = <A>(entries: ReadonlyArray<A>, label: string) => {
    if (entries.length > MAX_SNAPSHOT_LOG_ENTRIES) {
      omitted.push(`${entries.length - MAX_SNAPSHOT_LOG_ENTRIES} older ${label}`);
    }
    const kept = entries.slice(-MAX_SNAPSHOT_LOG_ENTRIES);
    if (kept.some((entry) => hasLongString(entry, MAX_SNAPSHOT_LOG_TEXT_CHARS))) {
      omitted.push(`${label} text after ${MAX_SNAPSHOT_LOG_TEXT_CHARS} characters`);
    }
    return kept.map(cutEntryStrings);
  };
  if (
    metadata.url.length > MAX_SNAPSHOT_IDENTIFIER_CHARS ||
    metadata.title.length > MAX_SNAPSHOT_IDENTIFIER_CHARS
  ) {
    omitted.push(`url or title after ${MAX_SNAPSHOT_IDENTIFIER_CHARS} characters`);
  }
  if (
    metadata.interactiveElements.some(
      (element) => element.name.length > MAX_SNAPSHOT_ELEMENT_NAME_CHARS,
    )
  ) {
    omitted.push(`element names longer than ${MAX_SNAPSHOT_ELEMENT_NAME_CHARS} characters`);
  }
  const bounded = {
    ...withoutTree,
    url: cutText(metadata.url, MAX_SNAPSHOT_IDENTIFIER_CHARS),
    title: cutText(metadata.title, MAX_SNAPSHOT_IDENTIFIER_CHARS),
    interactiveElements: metadata.interactiveElements.map((element) => ({
      ...element,
      name: cutText(element.name, MAX_SNAPSHOT_ELEMENT_NAME_CHARS),
    })),
    consoleEntries: tail(metadata.consoleEntries, "console entries"),
    networkEntries: tail(metadata.networkEntries, "network entries"),
    actionTimeline: tail(metadata.actionTimeline, "action timeline entries"),
  };

  // Per-field character caps can still exceed the total byte ceiling.
  const shedOrder = [
    "actionTimeline",
    "networkEntries",
    "consoleEntries",
    "interactiveElements",
  ] as const;
  const lists: Record<(typeof shedOrder)[number], ReadonlyArray<unknown>> = {
    interactiveElements: bounded.interactiveElements,
    consoleEntries: bounded.consoleEntries,
    networkEntries: bounded.networkEntries,
    actionTimeline: bounded.actionTimeline,
  };
  const dropped: Record<(typeof shedOrder)[number], number> = {
    interactiveElements: 0,
    consoleEntries: 0,
    networkEntries: 0,
    actionTimeline: 0,
  };
  let visibleTextChars = Math.min(metadata.visibleText.length, MAX_SNAPSHOT_VISIBLE_TEXT_CHARS);
  let ariaTreeChars = Math.min(ariaTree?.length ?? 0, 20_000);
  const value = () => ({
    ...bounded,
    ...(ariaTree === undefined ? {} : { accessibilityTree: cutText(ariaTree, ariaTreeChars) }),
    visibleText: cutText(metadata.visibleText, visibleTextChars),
    ...lists,
  });
  let text = encodeJsonText(value());
  while (utf8Length(text) > MAX_SNAPSHOT_TEXT_BYTES) {
    // Elements carry the locators, so they go last; logs shed newest-last.
    const key =
      shedOrder.find(
        (candidate) => candidate !== "interactiveElements" && lists[candidate].length > 0,
      ) ??
      (visibleTextChars > 0
        ? "visibleText"
        : lists.interactiveElements.length > 0
          ? "interactiveElements"
          : ariaTreeChars > 0
            ? "accessibilityTree"
            : undefined);
    if (key === undefined) break;
    if (key === "visibleText") {
      visibleTextChars = Math.floor(visibleTextChars / 2);
    } else if (key === "accessibilityTree") {
      ariaTreeChars = Math.floor(ariaTreeChars / 2);
    } else {
      const keep = Math.floor(lists[key].length / 2);
      dropped[key] += lists[key].length - keep;
      lists[key] =
        keep === 0
          ? []
          : key === "interactiveElements"
            ? lists[key].slice(0, keep)
            : lists[key].slice(-keep);
    }
    text = encodeJsonText(value());
  }
  if (visibleTextChars < metadata.visibleText.length) {
    omitted.push(
      `visibleText after ${visibleTextChars} characters (use preview_evaluate for more)`,
    );
  }
  if (ariaTree !== undefined && ariaTreeChars < ariaTree.length) {
    omitted.push(`accessibilityTree after ${ariaTreeChars} characters`);
  }
  for (const key of shedOrder) {
    if (dropped[key] > 0) {
      omitted.push(`${dropped[key]} of ${bounded[key].length} ${key}`);
    }
  }
  return { value: value(), text, omitted };
};

export class PreviewScreenshotSaveError extends Schema.TaggedError<PreviewScreenshotSaveError>()(
  "PreviewScreenshotSaveError",
  { screenshotPath: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Could not save preview screenshot to ${this.screenshotPath}.`;
  }
}

const MAX_SCREENSHOT_SITE_SLUG_LENGTH = 40;

const screenshotSiteSlug = (rawUrl: string): string => {
  try {
    const slug = new URL(rawUrl).hostname
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, MAX_SCREENSHOT_SITE_SLUG_LENGTH)
      .replace(/-+$/g, "");
    return slug || "site";
  } catch {
    return "site";
  }
};

const saveScreenshot = Effect.fn("McpHttpServer.saveScreenshot")(function* (
  pageUrl: string,
  data: Uint8Array,
  operationId?: string,
) {
  const config = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const millis = yield* Clock.currentTimeMillis;
  const crypto = yield* Crypto.Crypto;
  // Two saves in the same millisecond must not overwrite each other.
  const unique = operationId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie)).slice(0, 8);
  const fileName = `browser-screenshot-${screenshotSiteSlug(pageUrl)}-${millis.toString(36)}-${unique}.png`;
  const screenshotPath = path.join(config.browserArtifactsDir, fileName);
  yield* fileSystem.makeDirectory(config.browserArtifactsDir, { recursive: true }).pipe(
    Effect.andThen(
      fileSystem.writeFile(
        screenshotPath,
        data,
        operationId === undefined ? undefined : { flag: "wx" },
      ),
    ),
    Effect.mapError((cause) => new PreviewScreenshotSaveError({ screenshotPath, cause })),
  );
  return screenshotPath;
});

/** Decode with the advertised JSON codec, which permits null for optional fields that the raw schema rejects. */
const decodeAsAdvertised = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
): Toolkit.Toolkit<Tools> =>
  Toolkit.make(
    ...Object.values(toolkit.tools).map((tool) =>
      tool.setParameters(Schema.toCodecJson(tool.parametersSchema)),
    ),
  ) as unknown as Toolkit.Toolkit<Tools>;
const decodeSavedSnapshotParameters = Schema.decodeUnknownEffect(
  PreviewSnapshotTool.parametersSchema,
);
const decodeSavedSnapshotReceipt = Schema.decodeUnknownEffect(McpSavedSnapshotReceipt);

const isPreviewAutomationError = Schema.is(PreviewAutomationError);

const previewSnapshotFailure = Effect.fn("previewSnapshotFailure")(function <E>(
  cause: Cause.Cause<E>,
) {
  if (Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason)) {
    return Effect.failCause(cause).pipe(Effect.orDie);
  }
  const failures = cause.reasons.filter(Cause.isFailReason);
  const firstFailure = failures[0]?.error;
  if (isOrchestratorMcpFailure(firstFailure))
    return Effect.succeed(
      new McpSchema.CallToolResult({
        isError: true,
        structuredContent: encodeOrchestratorMcpFailure(firstFailure),
        content: [
          { type: "text", text: encodeJsonText(encodeOrchestratorMcpFailure(firstFailure)) },
        ],
      }),
    );
  const errorTag =
    typeof firstFailure === "object" &&
    firstFailure !== null &&
    "_tag" in firstFailure &&
    typeof firstFailure._tag === "string"
      ? firstFailure._tag
      : "PreviewSnapshotError";
  // Preview errors contain server-authored recovery guidance, never page output.
  const message = isPreviewAutomationError(firstFailure) ? firstFailure.message : undefined;
  const result = new McpSchema.CallToolResult({
    isError: true,
    structuredContent: {
      error: {
        _tag: errorTag,
        operation: "snapshot",
        failureCount: failures.length,
        ...(message === undefined ? {} : { message }),
      },
    },
    // Some clients show only the text content and others only structuredContent, so both carry it.
    content: [{ type: "text", text: `Preview snapshot failed: ${message ?? `${errorTag}.`}` }],
  });
  return Effect.logWarning("preview snapshot failed", {
    operation: "snapshot",
    errorTag,
    failureCount: failures.length,
  }).pipe(Effect.as(result));
});

const registerPreviewSnapshot = Effect.fn("McpHttpServer.registerPreviewSnapshot")(function* () {
  const server = yield* McpServer.McpServer;
  const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const saveServices = yield* Effect.context<
    ServerConfig.ServerConfig | FileSystem.FileSystem | Path.Path | Crypto.Crypto
  >();
  const requestOwner = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);
  const authorityOwner = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const built = yield* decodeAsAdvertised(PreviewSnapshotToolkit);
  const tool = PreviewSnapshotTool;
  const forwarding = yield* Effect.serviceOption(PeerForwarding.PeerForwarding);
  const checkSavedSnapshot = (invocation: McpInvocationContext.McpInvocationScope) => {
    const checked = PreviewHandlers.authorizeSnapshot.pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
    );
    return Option.isSome(authorityOwner)
      ? checked.pipe(Effect.provideService(PeerAuthority.PeerAuthority, authorityOwner.value))
      : checked;
  };
  const formatSavedSnapshot = Effect.fn("McpHttpServer.formatSavedSnapshot")(function* (
    receipt: unknown,
    identity: RequestReceipts.RequestIdentity,
    invocation: McpInvocationContext.McpInvocationScope,
    includeImage: boolean,
  ) {
    const saved = yield* decodeSavedSnapshotReceipt(receipt).pipe(
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The saved snapshot receipt is invalid.",
          }),
      ),
    );
    const config = yield* ServerConfig.ServerConfig;
    const path = yield* Path.Path;
    if (
      saved.operationId !== identity.id ||
      saved.requestId !== identity.clientRequestId ||
      saved.artifact.environmentId !== invocation.environmentId ||
      saved.artifact.owner !==
        RequestReceipts.requestDigest([
          requestNamespace(invocation),
          requestGeneration(invocation),
        ]) ||
      path.resolve(path.dirname(saved.artifact.screenshotPath)) !==
        path.resolve(config.browserArtifactsDir) ||
      !path.basename(saved.artifact.screenshotPath).endsWith(`-${identity.id}.png`)
    )
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The saved snapshot does not belong to this verified operation.",
      });
    // Replays describe the original artifact. A later independent file
    // edit must neither be overwritten nor shown as that original PNG.
    const original = yield* FileSystem.FileSystem.pipe(
      Effect.flatMap((fs) => fs.readFile(saved.artifact.screenshotPath)),
      Effect.orElseSucceed(() => undefined),
    );
    const unchanged =
      original !== undefined &&
      original.byteLength === saved.artifact.sizeBytes &&
      Hex.encode(sha256(original)) === saved.artifact.digest;
    return new McpSchema.CallToolResult({
      isError: false,
      structuredContent: saved,
      content: [
        { type: "text", text: encodeJsonText(saved) },
        ...(includeImage && unchanged
          ? [{ type: "image" as const, data: original, mimeType: "image/png" }]
          : []),
      ],
    });
  });
  yield* server.addTool({
    tool: new McpSchema.Tool({
      name: tool.name,
      description: Tool.getDescription(tool),
      inputSchema: advertisedJsonSchema(tool),
      annotations: {
        ...Context.getOption(tool.annotations, Tool.Title).pipe(
          Option.map((title) => ({ title })),
          Option.getOrUndefined,
        ),
        readOnlyHint: Context.get(tool.annotations, Tool.Readonly),
        destructiveHint: Context.get(tool.annotations, Tool.Destructive),
        idempotentHint: Context.get(tool.annotations, Tool.Idempotent),
        openWorldHint: Context.get(tool.annotations, Tool.OpenWorld),
      },
    }),
    annotations: bareToolAnnotations(tool),
    handle: (payload) =>
      Effect.withFiber((fiber) => {
        const invocation = Context.getUnsafe(
          fiber.context,
          McpInvocationContext.McpInvocationContext,
        );
        return Effect.gen(function* () {
          const savedRequest =
            payload?.save === true &&
            typeof payload.clientRequestId === "string" &&
            invocation.client?.linked !== true &&
            Option.isSome(requestOwner)
              ? yield* Effect.gen(function* () {
                  const parameters = yield* decodeSavedSnapshotParameters(payload).pipe(
                    Effect.mapError(
                      () =>
                        new OrchestratorMcpFailure({
                          code: "invalid_request",
                          message: "Invalid saved snapshot request.",
                        }),
                    ),
                  );
                  const check = checkSavedSnapshot(invocation);
                  yield* check;
                  const identity = yield* requestOwner.value.identify(
                    invocation,
                    "preview_snapshot:save",
                    parameters,
                    parameters.clientRequestId,
                  );
                  const checkRemote =
                    parameters.environmentId === undefined ||
                    parameters.environmentId === invocation.environmentId
                      ? Effect.void
                      : Option.isSome(forwarding)
                        ? forwarding.value.authorizeSavedPreview(
                            invocation,
                            parameters.environmentId,
                            parameters,
                            identity,
                          )
                        : Effect.fail(
                            new OrchestratorMcpFailure({
                              code: "capability_denied",
                              message: "The saved-preview forwarding authority is unavailable.",
                            }),
                          );
                  return { identity, check, checkRemote, owner: requestOwner.value };
                })
              : undefined;
          if (savedRequest !== undefined) {
            const prior = yield* savedRequest.owner.replay(savedRequest.identity);
            yield* savedRequest.checkRemote;
            if (Option.isSome(prior))
              return yield* formatSavedSnapshot(
                prior.value,
                savedRequest.identity,
                invocation,
                payload.includeImage === true,
              );
          }
          const { encodedResult } = yield* built
            .handle("preview_snapshot", payload)
            .pipe(
              Stream.unwrap,
              Stream.run(Sink.last()),
              Effect.flatMap(Effect.fromOption),
              Effect.provideService(PreviewAutomationBroker.PreviewAutomationBroker, broker),
              Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
              Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
            );
          const snapshot = encodedResult as SnapshotMetadata & {
            readonly url: string;
            readonly screenshot: {
              readonly mimeType: "image/png";
              readonly data: string;
              readonly width: number;
              readonly height: number;
            };
          };
          // A peer needs the schema-complete result, not the client presentation's
          // bounded text or optional image. Its own registration formats it once.
          if (invocation.client?.linked === true)
            return new McpSchema.CallToolResult({
              isError: false,
              structuredContent: encodedResult,
              content: [{ type: "text", text: "Peer snapshot." }],
            });
          const { screenshot, ...page } = snapshot;
          const png = new Uint8Array(Buffer.from(screenshot.data, "base64"));
          if (savedRequest !== undefined) {
            yield* savedRequest.check;
            yield* savedRequest.checkRemote;
            const receipt = yield* savedRequest.owner.external(
              savedRequest.identity,
              Effect.gen(function* () {
                const screenshotPath = yield* saveScreenshot(
                  snapshot.url,
                  png,
                  savedRequest.identity.id,
                );
                return {
                  type: "confirmed-operation" as const,
                  applied: true as const,
                  requestId: savedRequest.identity.clientRequestId,
                  operationId: savedRequest.identity.id,
                  stage: "confirmed" as const,
                  artifact: {
                    environmentId: invocation.environmentId,
                    owner: RequestReceipts.requestDigest([
                      requestNamespace(invocation),
                      requestGeneration(invocation),
                    ]),
                    screenshotPath,
                    mimeType: "image/png" as const,
                    sizeBytes: png.byteLength,
                    digest: Hex.encode(sha256(png)),
                  },
                };
              }),
            );
            return yield* formatSavedSnapshot(
              receipt,
              savedRequest.identity,
              invocation,
              payload.includeImage === true,
            );
          }
          const screenshotPath =
            payload?.save === true ? yield* saveScreenshot(snapshot.url, png) : undefined;
          // Images stay out of tool history unless asked for: providers replay them on every
          // later request, and some reject inline images outright.
          const includeImage = payload?.includeImage === true;
          if (screenshotPath !== undefined && !includeImage) {
            // The agent only wants a file to show the user. The url keeps the site icon on the tool row.
            const saved = {
              url: cutText(snapshot.url, MAX_SNAPSHOT_IDENTIFIER_CHARS),
              screenshotPath,
            };
            return new McpSchema.CallToolResult({
              isError: false,
              structuredContent: saved,
              content: [{ type: "text", text: encodeJsonText(saved) }],
            });
          }
          const metadata = {
            ...page,
            screenshot: {
              mimeType: screenshot.mimeType,
              width: screenshot.width,
              height: screenshot.height,
            },
            ...(screenshotPath === undefined ? {} : { screenshotPath }),
          };
          const bounded = boundSnapshotMetadata(metadata);
          return new McpSchema.CallToolResult({
            isError: false,
            structuredContent:
              bounded.omitted.length === 0
                ? bounded.value
                : { ...bounded.value, omitted: bounded.omitted },
            content: [
              // Keep the page identity readable even if a provider truncates the snapshot.
              {
                type: "text",
                text: encodeJsonText({
                  url: cutText(snapshot.url, MAX_SNAPSHOT_IDENTIFIER_CHARS),
                }),
              },
              { type: "text", text: bounded.text },
              ...(bounded.omitted.length === 0
                ? []
                : [
                    {
                      type: "text" as const,
                      text: `Snapshot text was bounded. Omitted: ${bounded.omitted.join("; ")}.`,
                    },
                  ]),
              ...(includeImage
                ? [{ type: "image" as const, data: png, mimeType: screenshot.mimeType }]
                : []),
            ],
          });
        }).pipe(
          Effect.provide(saveServices),
          Effect.matchCauseEffect({
            onFailure: previewSnapshotFailure,
            onSuccess: Effect.succeed,
          }),
        );
      }),
  });
});

interface ImageToolResult {
  readonly screenshot: {
    readonly mimeType: "image/png";
    readonly data: string;
    readonly width: number;
    readonly height: number;
  };
  readonly [key: string]: unknown;
}

/** Expose only undescribed failures' tags; remote messages may contain renderer or device output. */
const imageToolFailure =
  <E>(toolName: string, operation: string, failureText: string | ((error: E) => string)) =>
  (cause: Cause.Cause<E>) => {
    if (Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason)) {
      return Effect.failCause(cause).pipe(Effect.orDie);
    }
    const failures = cause.reasons.filter(Cause.isFailReason);
    const firstFailure = failures[0]?.error;
    const errorTag =
      typeof firstFailure === "object" &&
      firstFailure !== null &&
      "_tag" in firstFailure &&
      typeof firstFailure._tag === "string"
        ? firstFailure._tag
        : `${toolName}Error`;
    const message =
      typeof failureText === "string" || failures[0] === undefined
        ? undefined
        : failureText(failures[0].error);
    const result = new McpSchema.CallToolResult({
      isError: true,
      structuredContent: {
        error: {
          _tag: errorTag,
          operation,
          failureCount: failures.length,
          ...(message === undefined ? {} : { message }),
        },
      },
      // Some clients show only the text content and others only structuredContent, so both carry it.
      content: [
        {
          type: "text",
          text: typeof failureText === "string" ? failureText : (message ?? `${toolName} failed.`),
        },
      ],
    });
    return Effect.logWarning(`${toolName} failed`, {
      operation,
      errorTag,
      failureCount: failures.length,
    }).pipe(Effect.as(result));
  };

/**
 * `McpServer.toolkit` serializes every result as JSON text, which is the wrong
 * shape for a screenshot: the model needs image content.
 */
const registerImageTool = Effect.fn("McpHttpServer.registerImageTool")(function* <
  T extends Tool.Any,
  E,
  R,
>(
  tool: T,
  handle: (payload: Tool.Parameters<T>) => Effect.Effect<{ readonly encodedResult: unknown }, E, R>,
  provide: (
    effect: Effect.Effect<{ readonly encodedResult: unknown }, E, R>,
  ) => Effect.Effect<
    { readonly encodedResult: unknown },
    E,
    McpInvocationContext.McpInvocationContext
  >,
  operation: string,
  failureText: string | ((error: E) => string),
) {
  const server = yield* McpServer.McpServer;
  yield* server.addTool({
    tool: new McpSchema.Tool({
      name: tool.name,
      description: Tool.getDescription(tool),
      inputSchema: advertisedJsonSchema(tool),
      annotations: {
        ...Context.getOption(tool.annotations, Tool.Title).pipe(
          Option.map((title) => ({ title })),
          Option.getOrUndefined,
        ),
        readOnlyHint: Context.get(tool.annotations, Tool.Readonly),
        destructiveHint: Context.get(tool.annotations, Tool.Destructive),
        idempotentHint: Context.get(tool.annotations, Tool.Idempotent),
        openWorldHint: Context.get(tool.annotations, Tool.OpenWorld),
      },
    }),
    annotations: bareToolAnnotations(tool),
    handle: (payload) =>
      Effect.withFiber((fiber) => {
        const invocation = Context.getUnsafe(
          fiber.context,
          McpInvocationContext.McpInvocationContext,
        );
        return provide(handle(payload as Tool.Parameters<T>)).pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
          Effect.matchCauseEffect({
            onFailure: imageToolFailure(tool.name, operation, failureText),
            onSuccess: ({ encodedResult }) => {
              if (invocation.client?.linked === true)
                return Effect.succeed(
                  new McpSchema.CallToolResult({
                    isError: false,
                    structuredContent: encodedResult,
                    content: [{ type: "text", text: "Peer image result." }],
                  }),
                );
              const { screenshot, ...rest } = encodedResult as ImageToolResult;
              const includeImage =
                (payload as { readonly includeImage?: boolean } | undefined)?.includeImage !==
                false;
              const metadata = {
                ...rest,
                screenshot: {
                  mimeType: screenshot.mimeType,
                  width: screenshot.width,
                  height: screenshot.height,
                },
              };
              return Effect.succeed(
                new McpSchema.CallToolResult({
                  isError: false,
                  structuredContent: metadata,
                  content: [
                    { type: "text", text: JSON.stringify(metadata) },
                    ...(includeImage
                      ? [
                          {
                            type: "image" as const,
                            data: new Uint8Array(Buffer.from(screenshot.data, "base64")),
                            mimeType: screenshot.mimeType,
                          },
                        ]
                      : []),
                  ],
                }),
              );
            },
          }),
        );
      }),
  });
});

const registerDeviceScreenshot = Effect.fn("McpHttpServer.registerDeviceScreenshot")(function* () {
  const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const devices = yield* DeviceService.DeviceService;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const built = yield* decodeAsAdvertised(DeviceScreenshotToolkit);
  yield* registerImageTool(
    DeviceScreenshotTool,
    (payload) =>
      built
        .handle("device_screenshot", payload)
        .pipe(Stream.unwrap, Stream.run(Sink.last()), Effect.flatMap(Effect.fromOption)),
    (effect) =>
      effect.pipe(
        (effect) =>
          Option.isSome(settings)
            ? Effect.provideService(effect, ServerSettings.ServerSettingsService, settings.value)
            : effect,
        Effect.provideService(DeviceService.DeviceService, devices),
        Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
      ),
    "screenshot",
    (error) =>
      isDeviceToolError(error) || isOrchestratorMcpFailure(error)
        ? error.message
        : "Device screenshot failed.",
  );
});

const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);
const isDeviceToolError = Schema.is(DeviceToolError);
const encodeOrchestratorMcpFailure = Schema.encodeSync(OrchestratorMcpFailure);

const registerHtmlPreview = Effect.fn("McpHttpServer.registerHtmlPreview")(function* () {
  const htmlRender = yield* HtmlRender.HtmlRender;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const built = yield* decodeAsAdvertised(HtmlPreviewToolkit);
  yield* registerImageTool(
    HtmlPreviewTool,
    (payload) =>
      built
        .handle("html_preview", payload)
        .pipe(Stream.unwrap, Stream.run(Sink.last()), Effect.flatMap(Effect.fromOption)),
    (effect) =>
      effect.pipe(
        Effect.provideService(HtmlRender.HtmlRender, htmlRender),
        Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
      ),
    "preview",

    (error) =>
      isOrchestratorMcpFailure(error) || AiError.isAiError(error)
        ? error.message
        : "HTML preview failed.",
  );
});

type WireTool<T> =
  T extends Tool.Tool<infer Name, infer Config, infer Requirements>
    ? Tool.Tool<
        Name,
        {
          readonly parameters: Config["parameters"];
          readonly success: Config["success"];
          readonly failure: Schema.Codec<Config["failure"]["Type"], unknown>;
          readonly failureMode: "return";
        },
        Requirements
      >
    : never;

const wireToolkit = <Tools extends Record<string, Tool.Any>>(toolkit: Toolkit.Toolkit<Tools>) =>
  // Object.values erases key/schema/dependency correlations; the wire schema preserves declared failure types and scrubs internal fields.
  Toolkit.make(
    ...Object.values(toolkit.tools).map((tool) =>
      Tool.make(tool.name, {
        description: Tool.getDescription(tool),
        parameters: tool.parametersSchema,
        success: tool.successSchema,
        failure: Schema.toType(tool.failureSchema).pipe(
          Schema.encodeTo(Schema.Unknown, {
            decode: SchemaGetter.forbidden(() => "Tool failures are output-only."),
            encode: SchemaGetter.transform((error) =>
              isOrchestratorMcpFailure(error)
                ? encodeOrchestratorMcpFailure(error)
                : { message: error instanceof Error ? error.message : "Tool execution failed." },
            ),
          }),
        ),
        failureMode: "return",
        needsApproval: tool.needsApproval,
      }).annotateMerge(bareToolAnnotations(tool)),
    ),
  ) as unknown as Toolkit.Toolkit<{ readonly [K in keyof Tools]: WireTool<Tools[K]> }>;

/**
 * Tools that hand work to another thread's agent: they create it, message it,
 * steer it, or move context into it. Their events name the receiving agent too.
 */
const HANDOFF_TOOLS: ReadonlySet<string> = new Set([
  "delegate_task",
  "create_threads",
  "t3_thread_launch",
  "t3_thread_send",
  "t3_thread_send_attachments",
  "schedule_task",
  "run_scheduled_task_now",
]);

/** Tool actions that hand work over with only a `{ sequence }` result; the input names the thread. */
const INPUT_TARGET_ACTIONS: Readonly<Record<string, ReadonlySet<string>>> = {
  t3_queue: new Set(["edit", "promote_to_steer"]),
  t3_pending_request: new Set(["respond"]),
};

/** Tool actions that hand work over and name the receiving thread in their result. */
const RESULT_TARGET_ACTIONS: Readonly<Record<string, ReadonlySet<string>>> = {
  t3_thread_transfer: new Set(["fork", "merge_back"]),
};

const hasAction = (
  actions: Readonly<Record<string, ReadonlySet<string>>>,
  tool: string,
  args: unknown,
) =>
  actions[tool]?.has(String((args as { readonly action?: unknown } | null | undefined)?.action)) ===
  true;

const isInputTargetHandoff = (tool: string, args: unknown) =>
  hasAction(INPUT_TARGET_ACTIONS, tool, args);

const resultThreadKeys = ["childThreadId", "targetThreadId", "boundThreadId", "threadId"] as const;

/** Thread ids a handoff tool result names: the thread that received the work. */
const resultThreadIds = (content: unknown): ReadonlyArray<string> => {
  if (typeof content !== "object" || content === null) return [];
  const record = content as Readonly<Record<string, unknown>>;
  if (Array.isArray(record.threads)) return record.threads.flatMap(resultThreadIds);
  const key = resultThreadKeys.find((candidate) => typeof record[candidate] === "string");
  return key === undefined ? [] : [record[key] as string];
};

/**
 * Thread ids that received a handoff. The input's `threadId` is used only after
 * the call succeeded, so it is one the handler already resolved and authorized.
 */
const handoffTargetIds = (
  tool: string,
  args: unknown,
  result: McpSchema.CallToolResult | undefined,
): ReadonlyArray<string> => {
  if (!isInputTargetHandoff(tool, args)) return resultThreadIds(result?.structuredContent);
  if (result === undefined || toolOutcome(result).outcome !== "ok") return [];
  const threadId = (args as { readonly threadId?: unknown } | null | undefined)?.threadId;
  return typeof threadId === "string" ? [threadId] : [];
};

/** Tool analytics must never include IDs, prompts or free text. */
const withToolAnalytics =
  /* Request instrumentation already owns the span; keep this helper in that context. */
  Effect.fnUntraced(function* <A, E, R>(registration: Effect.Effect<A, E, R>) {
    const server = yield* McpServer.McpServer;
    const analytics = yield* Effect.serviceOption(AnalyticsService.AnalyticsService);
    if (Option.isNone(analytics)) return yield* registration;
    const registry = yield* Effect.serviceOption(ProviderRegistry.ProviderRegistry);
    const threads = yield* Effect.serviceOption(ThreadManagementService.ThreadManagementService);
    const shellOf = (threadId: string) =>
      Option.isNone(threads)
        ? Effect.succeed(null)
        : threads.value
            .getThreadShell(ThreadId.make(threadId))
            .pipe(Effect.orElseSucceed(() => null));
    // Delegation depth: parents walked from the caller, capped.
    const depthOf =
      /* Request instrumentation already owns the span; keep this helper in that context. */
      Effect.fnUntraced(function* (thread: AgentThread) {
        let depth = 0;
        let parentId =
          thread.lineage.relationshipToParent === "subagent" ? thread.lineage.parentThreadId : null;
        while (parentId !== null && depth < MAX_REPORTED_DEPTH) {
          depth += 1;
          const parent = yield* shellOf(parentId);
          parentId =
            parent?.lineage.relationshipToParent === "subagent"
              ? parent.lineage.parentThreadId
              : null;
        }
        return depth;
      });
    // Whether the caller's active run was started by a scheduled task.
    const scheduledRunOf = (threadId: string, runId: RunId | null) =>
      Option.isNone(threads) || runId === null
        ? Effect.succeed(false)
        : threads.value
            .getThreadRecords(ThreadId.make(threadId), ["runs", "messages"], {
              runIds: [runId],
              messageRoles: ["user"],
            })
            .pipe(
              Effect.map((records) => {
                const run = records.runs.find((candidate) => candidate.id === runId);
                return records.messages.some(
                  (message) =>
                    message.id === run?.userMessageId && message.scheduledTaskId !== undefined,
                );
              }),
              Effect.orElseSucceed(() => false),
            );
    const record =
      /* Request instrumentation already owns the span; keep this helper in that context. */
      Effect.fnUntraced(
        function* (
          tool: string,
          args: unknown,
          result: McpSchema.CallToolResult | undefined,
          durationMs: number,
        ) {
          const handoff =
            HANDOFF_TOOLS.has(tool) ||
            isInputTargetHandoff(tool, args) ||
            hasAction(RESULT_TARGET_ACTIONS, tool, args);
          const outcome = toolOutcome(result);
          // Agents call status, read, and preview tools in tight loops, so an
          // event per successful call floods analytics. Only failures count there.
          if (!handoff && outcome.outcome === "ok") return;
          // Client credentials (no calling thread) report an unknown caller.
          const invocation = Option.getOrUndefined(
            yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext),
          )?.thread;
          const providers = Option.isSome(registry) ? yield* registry.value.getProviders : [];
          // Only handoff tools pay for thread lookups; the rest report the
          // caller's provider from the credential.
          const caller =
            handoff && invocation !== undefined
              ? ((yield* shellOf(invocation.threadId)) ?? undefined)
              : undefined;
          const targetIds = handoff
            ? [...new Set(handoffTargetIds(tool, args, result))].filter(
                (threadId) => threadId !== invocation?.threadId,
              )
            : [];
          const targets = yield* Effect.forEach(targetIds, shellOf);
          for (const properties of agentToolProperties({
            tool,
            providers,
            caller,
            ...(invocation === undefined
              ? {}
              : { callerProviderInstanceId: invocation.providerInstanceId }),
            ...(caller === undefined || invocation === undefined
              ? {}
              : {
                  callerDepth: yield* depthOf(caller),
                  callerScheduledRun: yield* scheduledRunOf(
                    invocation.threadId,
                    caller.activeRunId,
                  ),
                }),
            targets: targets.filter((thread) => thread !== null),
            outcome,
            ...(handoff
              ? { settings: handoffSettings(tool, args, result?.structuredContent) }
              : {}),
            durationMs,
          })) {
            yield* analytics.value.record("mcp.tool.invoked", properties);
          }
        },
        (effect, _tool, _args, _result, _durationMs) => effect.pipe(Effect.ignoreCause),
      );
    const recordingServer = McpServer.McpServer.of({
      ...server,
      addTool: Effect.fn("McpServer.addTool")(function (options) {
        return server.addTool({
          ...options,
          handle:
            /* Request instrumentation already owns the span; keep this helper in that context. */
            Effect.fnUntraced(function* (payload) {
              const startedAt = yield* Clock.currentTimeMillis;
              return yield* options.handle(payload).pipe(
                Effect.onExit((exit) => {
                  const result = exit._tag === "Success" ? exit.value : undefined;
                  // A call that asks for input is retried with it, and that call is the one counted.
                  if (result instanceof McpSchema.InputRequired) return Effect.void;
                  // Handoff tools look up threads to record; the response must not wait on them.
                  return Clock.currentTimeMillis.pipe(
                    Effect.flatMap((endedAt) =>
                      record(options.tool.name, payload, result, endedAt - startedAt),
                    ),
                    Effect.forkDetach,
                    Effect.asVoid,
                  );
                }),
              );
            }),
        });
      }),
    });
    return yield* registration.pipe(Effect.provideService(McpServer.McpServer, recordingServer));
  });

/** `McpServer.registerToolkit` advertises the full codec schema; agents get the compact one. */
const withAdvertisedSchemas = Effect.fn("McpHttpServer.withAdvertisedSchemas")(function* <
  Tools extends Record<string, Tool.Any>,
  A,
  E,
  R,
>(toolkit: Toolkit.Toolkit<Tools>, registration: Effect.Effect<A, E, R>) {
  const server = yield* McpServer.McpServer;
  const tools: Record<string, Tool.Any> = toolkit.tools;
  const advertisingServer = McpServer.McpServer.of({
    ...server,
    addTool: Effect.fn("McpServer.addTool")(function (options) {
      const tool = tools[options.tool.name];
      return server.addTool(
        tool === undefined
          ? options
          : {
              ...options,
              tool: new McpSchema.Tool({
                ...options.tool,
                inputSchema: advertisedJsonSchema(tool),
              }),
            },
      );
    }),
  });
  return yield* registration.pipe(Effect.provideService(McpServer.McpServer, advertisingServer));
});

/**
 * `McpServer.toolkit` asks for every service its tools declare when it
 * registers them, but the auth middleware provides `McpInvocationContext` to
 * each request instead. Registration must not get one: the services it
 * captures would replace the request's.
 */
export const toolkitRegistration = <Tools extends Record<string, Tool.Any>, EX, RX>(
  toolkit: Toolkit.Toolkit<Tools>,
  handlers: McpToolAccess.HandlersLayer<Tools, EX, RX>,
) => {
  // Return-mode registration keeps declared error fields (durable outcome receipts) on the
  // wire for MCP peers; error mode would emit only Error.message.
  const registration = Layer.effectDiscard(
    withToolAnalytics(
      withAdvertisedSchemas(
        wireToolkit(toolkit),
        McpServer.registerToolkit(decodeAsAdvertised(wireToolkit(toolkit))),
      ),
    ),
  ).pipe(Layer.provide(McpServer.McpServer.layer));
  // @effect-diagnostics-next-line unsafeEffectTypeAssertion:off - the auth middleware provides it per request.
  const registered = registration as Layer.Layer<
    never,
    never,
    Exclude<Layer.Services<typeof registration>, McpInvocationContext.McpInvocationContext>
  >;
  return registered.pipe(Layer.provide(McpToolAccess.HandlersLayer.layer(handlers)));
};

const imageToolRegistration = <Tools extends Record<string, Tool.Any>, A, E, R, EX, RX>(
  register: Effect.Effect<A, E, R>,
  handlers: McpToolAccess.HandlersLayer<Tools, EX, RX>,
) =>
  Layer.effectDiscard(withToolAnalytics(register)).pipe(
    Layer.provide(McpToolAccess.HandlersLayer.layer(handlers)),
  );

export const layerHtmlToolkit = Layer.mergeAll(
  toolkitRegistration(HtmlRenderToolkit, HtmlHandlers.layerRender),
  imageToolRegistration(registerHtmlPreview(), HtmlHandlers.layerPreview),
).pipe(Layer.provide(HtmlRender.layer));

// Effect's generic registration displays Error.message instead of encoding the
// declared error schema. Research peers need the code as well as the message.
const registerWebResearch = Effect.fn("McpHttpServer.registerWebResearch")(function* () {
  const server = yield* McpServer.McpServer;
  const research = yield* ResearchHost.ResearchHost;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const built = yield* WebResearchToolkit;
  for (const name of ["web_read", "web_search"] as const) {
    const tool = WebResearchToolkit.tools[name];
    yield* server.addTool({
      tool: new McpSchema.Tool({
        name: tool.name,
        description: Tool.getDescription(tool),
        inputSchema: Tool.getJsonSchema(tool),
        outputSchema: Tool.getJsonSchemaFromSchema(tool.successSchema),
        annotations: {
          ...Context.getOption(tool.annotations, Tool.Title).pipe(
            Option.map((title) => ({ title })),
            Option.getOrUndefined,
          ),
          readOnlyHint: Context.get(tool.annotations, Tool.Readonly),
          destructiveHint: Context.get(tool.annotations, Tool.Destructive),
          idempotentHint: Context.get(tool.annotations, Tool.Idempotent),
          openWorldHint: Context.get(tool.annotations, Tool.OpenWorld),
        },
      }),
      annotations: bareToolAnnotations(tool),
      handle: (payload) =>
        Effect.withFiber((fiber) =>
          built.handle(name, payload).pipe(
            Stream.unwrap,
            Stream.run(Sink.last()),
            Effect.flatMap(Effect.fromOption),
            Effect.provideService(ResearchHost.ResearchHost, research),
            Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              Context.getUnsafe(fiber.context, McpInvocationContext.McpInvocationContext),
            ),
            Effect.map(
              ({ encodedResult }) =>
                new McpSchema.CallToolResult({
                  structuredContent: encodedResult,
                  content: [{ type: "text", text: encodeJsonText(encodedResult) }],
                }),
            ),
            Effect.catchTags({
              AiError: (error) =>
                Effect.fail(
                  error.reason._tag === "ToolParameterValidationError"
                    ? new McpSchema.InvalidParams({ message: error.reason.message })
                    : new McpSchema.InternalError({ message: "The research request failed." }),
                ),
              NoSuchElementError: () =>
                Effect.fail(
                  new McpSchema.InternalError({
                    message: "The research request returned no result.",
                  }),
                ),
              OrchestratorMcpFailure: (error) => {
                const failure = { code: error.code, message: error.message };
                // outputSchema describes success. SDK clients also validate error structuredContent.
                return Effect.succeed(
                  new McpSchema.CallToolResult({
                    isError: true,
                    content: [{ type: "text", text: encodeJsonText(failure) }],
                  }),
                );
              },
            }),
          ),
        ),
    });
  }
});

// One environment-owned reader/cache/limiter is shared by all MCP sessions.
export const layerWebResearchToolkit = Layer.effectDiscard(registerWebResearch()).pipe(
  Layer.provide(McpToolAccess.HandlersLayer.layer(WebResearchHandlers.layer)),
  Layer.provide(ResearchHost.layer),
);

const layerWebResearch = layerWebResearchToolkit.pipe(
  Layer.provide(SessionRead.layer),
  Layer.provide(
    WebResearch.layer.pipe(
      Layer.provide(
        Search.layer.pipe(
          Layer.provide(NodeHttpClient.layerNodeHttp),
          Layer.provide(ServerSecretStore.layer),
        ),
      ),
      Layer.provide(PageCache.layer),
      Layer.provide(PublicHttp.layer),
      Layer.provide(PublicRender.layer),
      Layer.provide(
        TinyFish.layer.pipe(
          Layer.provide(NodeHttpClient.layerNodeHttp),
          Layer.provide(ServerSecretStore.layer),
        ),
      ),
    ),
  ),
);

const layerPreviewStandardToolkitRegistration = toolkitRegistration(
  PreviewStandardToolkit,
  PreviewHandlers.layerStandard,
);

const layerPreviewSnapshotRegistration = imageToolRegistration(
  registerPreviewSnapshot(),
  PreviewHandlers.layerSnapshot,
);

const decodeSavedPreviewAuthorization = Schema.decodeUnknownEffect(
  PeerSavedPreviewAuthorizationRequest,
);
const layerSavedPreviewAuthorization = Layer.unwrap(
  Effect.gen(function* () {
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    const threads = yield* Effect.serviceOption(ThreadManagementService.ThreadManagementService);
    const settings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
    const requests = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);
    const registry = yield* Effect.serviceOption(McpSessionRegistry.McpSessionRegistry);
    const router = yield* Effect.serviceOption(HttpRouter.HttpRouter);
    if (
      Option.isNone(authority) ||
      Option.isNone(threads) ||
      Option.isNone(settings) ||
      Option.isNone(requests) ||
      Option.isNone(registry) ||
      Option.isNone(router)
    )
      return Layer.empty;
    const support = HttpRouter.add(
      "GET",
      "/mcp/peer-control/saved-preview",
      Effect.succeed(
        HttpServerResponse.jsonUnsafe({ version: 1, purpose: "saved-preview-authorization" }),
      ),
    );
    const authorize = HttpRouter.add(
      "POST",
      "/mcp/peer-control/saved-preview",
      Effect.gen(function* () {
        if (
          Option.isNone(authority) ||
          Option.isNone(threads) ||
          Option.isNone(settings) ||
          Option.isNone(requests)
        )
          return HttpServerResponse.empty({ status: 409 });
        const http = yield* HttpServerRequest.HttpServerRequest;
        const scope = yield* McpInvocationContext.McpInvocationContext;
        return yield* Effect.gen(function* () {
          const input = yield* decodeSavedPreviewAuthorization(yield* http.json).pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "invalid_request",
                  message: "Invalid saved-preview authorization context.",
                }),
            ),
          );
          if (
            scope.client?.linked !== true ||
            scope.authority === undefined ||
            !("envelope" in scope.authority) ||
            input.environmentId !== scope.environmentId ||
            input.generation !== scope.authority.peer.generation
          )
            return yield* new OrchestratorMcpFailure({
              code: "capability_denied",
              message:
                "Saved-preview authorization needs its authenticated original destination generation.",
            });
          yield* McpRouting.withContext(input, PreviewHandlers.authorizeSnapshot).pipe(
            Effect.catchTags({
              PreviewAutomationUnavailableError: (error) =>
                Effect.fail(
                  new OrchestratorMcpFailure({ code: "capability_denied", message: error.message }),
                ),
            }),
            Effect.provideService(McpRouting.CurrentTool, PreviewSnapshotTool),
            Effect.provideService(ThreadManagementService.ThreadManagementService, threads.value),
            Effect.provideService(PeerAuthority.PeerAuthority, authority.value),
            Effect.provideService(ServerSettings.ServerSettingsService, settings.value),
          );
          return HttpServerResponse.jsonUnsafe({ ...input, authorized: true });
        }).pipe(Effect.orElseSucceed((error) => peerAuthorityFailure(error)));
      }),
    );
    return Layer.merge(support, authorize).pipe(
      Layer.provide(
        layerMcpAuthMiddleware.pipe(
          Layer.provide(Layer.succeed(McpSessionRegistry.McpSessionRegistry, registry.value)),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, authority.value)),
        ),
      ),
      Layer.provide(Layer.succeed(HttpRouter.HttpRouter, router.value)),
    );
  }),
);

const decodePreviewViewerInput = Schema.decodeUnknownEffect(PreviewViewerOpenInput);
const decodePreviewViewerClose = Schema.decodeUnknownEffect(PreviewViewerCloseInput);
const layerPreviewViewerAuthorization = Layer.unwrap(
  Effect.gen(function* () {
    const viewers = yield* Effect.serviceOption(ServerBrowserStream.PreviewViewers);
    const registry = yield* Effect.serviceOption(McpSessionRegistry.McpSessionRegistry);
    const router = yield* Effect.serviceOption(HttpRouter.HttpRouter);
    if (Option.isNone(viewers) || Option.isNone(registry) || Option.isNone(router))
      return Layer.empty;
    return Layer.mergeAll(
      HttpRouter.add(
        "GET",
        "/mcp/peer-control/preview-viewer",
        Effect.gen(function* () {
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return scope.client?.linked === true
            ? HttpServerResponse.jsonUnsafe({ version: 1, purpose: "preview-viewer" })
            : HttpServerResponse.empty({ status: 403 });
        }),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/preview-viewer",
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return yield* Effect.gen(function* () {
            const input = yield* decodePreviewViewerInput(yield* request.json).pipe(
              Effect.mapError(
                () =>
                  new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: "Invalid browser viewer resource.",
                  }),
              ),
            );
            return HttpServerResponse.jsonUnsafe(yield* viewers.value.accept(scope, input));
          }).pipe(Effect.orElseSucceed((error) => peerAuthorityFailure(error)));
        }),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/preview-viewer/close",
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          if (scope.client?.linked !== true) return HttpServerResponse.empty({ status: 403 });
          const input = yield* decodePreviewViewerClose(yield* request.json);
          return HttpServerResponse.jsonUnsafe(
            yield* viewers.value.close(AuthSessionId.make(scope.client.sessionId), input.viewerId),
          );
        }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 400 }))),
      ),
    ).pipe(
      Layer.provide(layerMcpAuthMiddleware),
      Layer.provide(Layer.succeed(McpSessionRegistry.McpSessionRegistry, registry.value)),
      Layer.provide(Layer.succeed(HttpRouter.HttpRouter, router.value)),
    );
  }),
);

export const layerPreviewToolkit = Layer.mergeAll(
  layerPreviewViewerAuthorization,
  layerSavedPreviewAuthorization,
  layerPreviewStandardToolkitRegistration,
  toolkitRegistration(PreviewWatchToolkit, PreviewWatchHandlers.layer),
  layerPreviewSnapshotRegistration,
);

export const layerOrchestratorToolkit = toolkitRegistration(
  OrchestratorToolkit,
  OrchestratorHandlers.layer,
).pipe(Layer.provide(OrchestratorMcpService.layer), Layer.provide(ThreadMetadataMcpService.layer));

export const layerThreadToolkit = toolkitRegistration(ThreadToolkit, ThreadHandlers.layer);

const layerWorktreeToolkitRegistration = toolkitRegistration(
  WorktreeToolkit,
  WorktreeHandlers.layer,
).pipe(Layer.provide(WorktreeMcpService.layer));

const layerPreviewControlsRegistration = toolkitRegistration(
  PreviewControlsToolkit,
  PreviewControlsHandlers.layer,
);

export const layerEnvironmentToolkit = toolkitRegistration(
  EnvironmentToolkit,
  EnvironmentHandlers.layer,
).pipe(Layer.provide(ThreadCommandExecutor.layer));

export const layerProjectRegistration = toolkitRegistration(ProjectToolkit, ProjectHandlers.layer);

export const layerAttachmentToolkit = toolkitRegistration(
  AttachmentToolkit,
  AttachmentHandlers.layer,
);

export const layerPullRequestsToolkit = toolkitRegistration(
  PullRequestsToolkit,
  PullRequestsHandlers.layer,
);

// Fork: Issue links (toolboxmd/t3code#28).
const layerIssuesToolkitRegistration = toolkitRegistration(IssuesToolkit, IssuesHandlers.layer);

const layerDeviceStandardToolkitRegistration = toolkitRegistration(
  DeviceStandardToolkit,
  DeviceHandlers.layerStandard,
);

const layerDeviceScreenshotRegistration = imageToolRegistration(
  registerDeviceScreenshot(),
  DeviceHandlers.layerScreenshot,
);

const decodeQualifiedAssetResource = Schema.decodeUnknownEffect(QualifiedAssetResource);
const decodeDeviceViewerInput = Schema.decodeUnknownEffect(DeviceViewerOpenInput);
const layerDeviceViewerAuthorization = Layer.unwrap(
  Effect.gen(function* () {
    const viewers = yield* Effect.serviceOption(DeviceHubProxy.DeviceViewers);
    const registry = yield* Effect.serviceOption(McpSessionRegistry.McpSessionRegistry);
    const router = yield* Effect.serviceOption(HttpRouter.HttpRouter);
    if (Option.isNone(viewers) || Option.isNone(registry) || Option.isNone(router))
      return Layer.empty;
    return Layer.mergeAll(
      HttpRouter.add(
        "GET",
        "/mcp/peer-control/device-viewer",
        Effect.gen(function* () {
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return scope.client?.linked === true
            ? HttpServerResponse.jsonUnsafe({ version: 1, purpose: "device-viewer" })
            : HttpServerResponse.empty({ status: 403 });
        }),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/device-viewer",
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return yield* Effect.gen(function* () {
            const input = yield* decodeDeviceViewerInput(yield* request.json).pipe(
              Effect.mapError(
                () =>
                  new OrchestratorMcpFailure({
                    code: "invalid_request",
                    message: "Invalid device viewer resource.",
                  }),
              ),
            );
            return HttpServerResponse.jsonUnsafe(yield* viewers.value.accept(scope, input));
          }).pipe(Effect.orElseSucceed((error) => peerAuthorityFailure(error)));
        }),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/device-viewer/close",
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          if (scope.client?.linked !== true) return HttpServerResponse.empty({ status: 403 });
          const input = yield* decodeDeviceViewerCloseInput(yield* request.json);
          return HttpServerResponse.jsonUnsafe(
            yield* viewers.value.close(AuthSessionId.make(scope.client.sessionId), input.viewerId),
          );
        }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 400 }))),
      ),
    ).pipe(
      Layer.provide(layerMcpAuthMiddleware),
      Layer.provide(Layer.succeed(McpSessionRegistry.McpSessionRegistry, registry.value)),
      Layer.provide(Layer.succeed(HttpRouter.HttpRouter, router.value)),
    );
  }),
);

export const layerDeviceToolkit = Layer.mergeAll(
  layerDeviceViewerAuthorization,
  layerDeviceStandardToolkitRegistration,
  layerDeviceScreenshotRegistration,
);

const layerPeerControl = Layer.unwrap(
  Effect.gen(function* () {
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    const secrets = yield* Effect.serviceOption(RemoteSecretRequests.RemoteSecretRequests);
    const secretSupport = HttpRouter.add(
      "GET",
      "/mcp/peer-control/secrets",
      Effect.succeed(
        Option.isSome(secrets)
          ? HttpServerResponse.jsonUnsafe({
              version: 1,
              families: ["schedule.webhook-signature"],
              consumers: ["schedule_task"],
            } satisfies typeof RemoteSecretSupport.Type)
          : HttpServerResponse.empty({ status: 409 }),
      ),
    );
    const secretControl = HttpRouter.add(
      "POST",
      "/mcp/peer-control/secrets",
      Effect.gen(function* () {
        if (Option.isNone(secrets)) return HttpServerResponse.empty({ status: 409 });
        const request = yield* HttpServerRequest.HttpServerRequest;
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const result = yield* secrets.value.control(
          scope,
          yield* decodeSecretControl(yield* request.json),
        );
        return HttpServerResponse.jsonUnsafe(result);
      }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 }))),
    );
    const secretReceipt = HttpRouter.add(
      "POST",
      "/mcp/peer-control/secrets/receipt",
      Effect.gen(function* () {
        if (Option.isNone(secrets)) return HttpServerResponse.empty({ status: 409 });
        const request = yield* HttpServerRequest.HttpServerRequest;
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const receipt = yield* secrets.value.receipt(
          scope,
          yield* decodeSecretReceiptQuery(yield* request.json),
        );
        return Option.isSome(receipt)
          ? HttpServerResponse.jsonUnsafe(receipt.value)
          : HttpServerResponse.empty({ status: 204 });
      }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 }))),
    );
    const secretRetirement = HttpRouter.add(
      "POST",
      "/mcp/peer-control/secrets/retire",
      Effect.succeed(HttpServerResponse.empty({ status: 403 })),
    );
    const automationSupport = HttpRouter.add(
      "GET",
      "/mcp/peer-control/automation",
      Effect.succeed(
        Option.isSome(authority)
          ? HttpServerResponse.jsonUnsafe({ version: 1 })
          : HttpServerResponse.empty({ status: 409 }),
      ),
    );
    const automationDisable = HttpRouter.add(
      "POST",
      "/mcp/peer-control/automation-disable",
      Effect.succeed(HttpServerResponse.empty({ status: 403 })),
    );
    const post = HttpRouter.add(
      "POST",
      "/mcp/peer-control",
      Effect.gen(function* () {
        if (Option.isNone(authority)) return HttpServerResponse.empty({ status: 409 });
        const request = yield* HttpServerRequest.HttpServerRequest;
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const result = yield* Effect.gen(function* () {
          const input = yield* decodePeerExchange(yield* request.json);
          return yield* authority.value.exchange(scope, input);
        }).pipe(Effect.result);
        return result._tag === "Success"
          ? HttpServerResponse.jsonUnsafe(result.success)
          : HttpServerResponse.empty({ status: 403 });
      }),
    );
    const streaming = HttpRouter.add(
      "GET",
      "/mcp/peer-control/stream",
      Effect.gen(function* () {
        if (Option.isNone(authority)) return HttpServerResponse.empty({ status: 409 });
        const request = yield* HttpServerRequest.HttpServerRequest;
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const peer = yield* decodePeerSession(request.headers[PeerAuthority.LINK_HEADER]);
        yield* authority.value.enroll(scope, peer);
        const encoder = new TextEncoder();
        const updates = Stream.unfold(0, (cursor) =>
          authority.value
            .exchange(scope, { ...peer, answers: [], after: cursor })
            .pipe(
              Effect.map(
                (batch) =>
                  [encoder.encode(`data: ${encodeJsonText(batch)}\n\n`), batch.cursor] as const,
              ),
            ),
        );
        return HttpServerResponse.stream(
          Stream.concat(Stream.make(encoder.encode(": peer-control\n\n")), updates),
          {
            headers: {
              "content-type": "text/event-stream",
              "cache-control": "no-store, no-transform",
              "x-accel-buffering": "no",
            },
          },
        );
      }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 }))),
    );
    const lifecycle = yield* Effect.serviceOption(PeerTaskLifecycle.PeerTaskLifecycle);
    const pendingRequests = yield* Effect.serviceOption(PendingRequests);
    const requests = yield* Effect.serviceOption(RequestReceipts.McpRequestReceipts);
    const attachments = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
    const downloadBrowser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
    if (Option.isNone(authority)) return Layer.merge(post, streaming);
    const pendingRequestProtocol = HttpRouter.add(
      "GET",
      "/mcp/peer-control/pending-requests",
      Effect.succeed(
        Option.isSome(pendingRequests)
          ? HttpServerResponse.jsonUnsafe({ version: 1 })
          : HttpServerResponse.empty({ status: 409 }),
      ),
    );
    const tasks = Layer.merge(
      HttpRouter.add(
        "GET",
        "/mcp/peer-control/tasks",
        Effect.succeed(
          Option.isSome(lifecycle)
            ? HttpServerResponse.jsonUnsafe({ version: 1 })
            : HttpServerResponse.empty({ status: 409 }),
        ),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/tasks",
        Effect.gen(function* () {
          if (Option.isNone(lifecycle)) return HttpServerResponse.empty({ status: 409 });
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return yield* Effect.gen(function* () {
            const input = yield* decodeTaskLifecycle(yield* request.json);
            return HttpServerResponse.jsonUnsafe(yield* lifecycle.value.execute(scope, input));
          }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 })));
        }),
      ),
    );
    const requestProtocol = HttpRouter.add(
      "GET",
      "/mcp/peer-control/requests",
      Effect.succeed(
        Option.isSome(requests)
          ? HttpServerResponse.jsonUnsafe({ version: 1, attachments: Option.isSome(attachments) })
          : HttpServerResponse.empty({ status: 409 }),
      ),
    );
    const attachmentAccess = Layer.merge(
      HttpRouter.add(
        "GET",
        "/mcp/peer-control/attachment",
        Effect.gen(function* () {
          const scope = yield* McpInvocationContext.McpInvocationContext;
          return Option.isSome(attachments) && scope.client?.linked === true
            ? HttpServerResponse.jsonUnsafe({
                version: 1,
                purpose: "attachment-read",
                nativeBrowserDownloads:
                  Option.isSome(downloadBrowser) &&
                  downloadBrowser.value.inspectDownload !== undefined &&
                  downloadBrowser.value.readDownloadBlock !== undefined
                    ? 1
                    : undefined,
              })
            : HttpServerResponse.empty({ status: 409 });
        }),
      ),
      HttpRouter.add(
        "POST",
        "/mcp/peer-control/attachment",
        Effect.gen(function* () {
          if (Option.isNone(attachments)) return HttpServerResponse.empty({ status: 409 });
          const request = yield* HttpServerRequest.HttpServerRequest;
          const scope = yield* McpInvocationContext.McpInvocationContext;
          if (
            request.url !== "/mcp/peer-control/attachment" ||
            (request.headers["t3-attachment-check-only"] !== undefined &&
              request.headers["t3-attachment-check-only"] !== "1")
          )
            return HttpServerResponse.empty({ status: 400 });
          const resource = yield* decodeQualifiedAssetResource(yield* request.json);
          if (resource._tag === "native-browser-download") {
            if (
              Option.isNone(downloadBrowser) ||
              downloadBrowser.value.inspectDownload === undefined ||
              downloadBrowser.value.readDownloadBlock === undefined
            )
              return HttpServerResponse.empty({ status: 409 });
            const authorize = authority.value.recheckAttachmentRead(scope, resource, "preview");
            yield* downloadBrowser.value.inspectDownload(resource, authorize);
            const headers = {
              "cache-control": "no-store",
              "t3-attachment-producer": "preview",
              "t3-download-digest": resource.digest,
              "t3-download-size": String(resource.sizeBytes),
            };
            if (request.headers["t3-attachment-check-only"] === "1")
              return HttpServerResponse.empty({ status: 204, headers });
            const offsetText = request.headers["t3-download-offset"];
            if (offsetText === undefined || !/^(0|[1-9][0-9]{0,9})$/.test(offsetText))
              return HttpServerResponse.empty({ status: 400 });
            const bytes = yield* downloadBrowser.value.readDownloadBlock(
              resource,
              Number(offsetText),
              authorize,
            );
            yield* authorize;
            return HttpServerResponse.uint8Array(bytes, {
              headers: {
                ...headers,
                "content-type": "application/octet-stream",
                "x-content-type-options": "nosniff",
                "t3-download-block-digest": Hex.encode(sha256(bytes)),
              },
            });
          }
          const authorize = (capability: "device" | "preview") =>
            authority.value.recheckAttachmentRead(scope, resource, capability);
          if (request.headers["t3-attachment-check-only"] === "1") {
            const producer = yield* attachments.value.authorizeReference(resource, authorize);
            return HttpServerResponse.empty({
              status: 204,
              headers: { "cache-control": "no-store", "t3-attachment-producer": producer },
            });
          }
          const bytes = yield* attachments.value.readReference(resource, authorize);
          const producer = yield* attachments.value.authorizeReference(resource, authorize);
          return HttpServerResponse.uint8Array(bytes, {
            headers: {
              "t3-attachment-producer": producer,
              "cache-control": "no-store",
              "content-type": "application/octet-stream",
              "x-content-type-options": "nosniff",
            },
          });
        }).pipe(Effect.orElseSucceed(() => HttpServerResponse.empty({ status: 403 }))),
      ),
    );
    return Layer.mergeAll(
      secretSupport,
      secretControl,
      secretRetirement,
      secretReceipt,
      automationSupport,
      automationDisable,
      post,
      streaming,
      tasks,
      requestProtocol,
      pendingRequestProtocol,
      attachmentAccess,
    );
  }),
);

export const layerMcpTransport = Layer.merge(
  McpServer.layerHttp({
    name: "T3 Code",
    version: packageJson.version,
    path: "/mcp",
    protocols: [McpProtocol.v2025_06_18],
    allowSessionTermination: true,
  }),
  layerPeerControl,
).pipe(Layer.provide(layerMcpAuthMiddleware));

/** Every tool the T3 MCP server advertises. AgentContextBudget.test.ts measures this set. */
export const layerTools = Layer.mergeAll(
  layerPreviewToolkit,
  layerOrchestratorToolkit,
  layerThreadToolkit,
  toolkitRegistration(ThreadMcpToolkit, ThreadMcpHandlers.layer),
  layerAttachmentToolkit,
  layerProjectRegistration,
  layerEnvironmentToolkit,
  layerPreviewControlsRegistration,
  layerWorktreeToolkitRegistration,
  layerPullRequestsToolkit,
  layerIssuesToolkitRegistration,
  layerDeviceToolkit,
  toolkitRegistration(SpectrumToolkit.SpectrumToolkit, SpectrumToolkit.layer),
  layerHtmlToolkit,
  layerWebResearch,
);

export const layer = layerTools.pipe(Layer.provideMerge(layerMcpTransport));

const decodeDeviceViewerCloseInput = Schema.decodeUnknownEffect(DeviceViewerCloseInput);
