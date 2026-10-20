import {
  EnvironmentId,
  OrchestratorMcpFailure,
  WebSearchInput,
  WebSearchResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";

import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as McpRouting from "../mcp/McpRouting.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as Settings from "../serverSettings.ts";
import * as PageCache from "./PageCache.ts";
import * as Search from "./Search.ts";
import * as SessionRead from "./SessionRead.ts";
import * as TinyFish from "./TinyFish.ts";
import * as WebResearch from "./WebResearch.ts";
import { SessionSelection, WebReadInput, WebReadPage, WebReadResult } from "./schemas.ts";

export const HostedWebReadInput = Schema.Struct({
  ...WebReadInput.fields,
  useSession: Schema.optional(Schema.Boolean).annotate({
    description:
      "Explicit opt-in, default false. Read only the selected existing session tab on its owner environment, outside the public page cache.",
  }),
  session: Schema.optional(SessionSelection),
  cacheEnvironmentId: Schema.optional(EnvironmentId).annotate({
    description: "Required with cacheId. Copy cacheEnvironmentId from the page's provenance.",
  }),
});

const HostedWebReadPage = Schema.Struct({
  ...WebReadPage.fields,
  cacheEnvironmentId: EnvironmentId,
});

export const HostedWebReadResult = Schema.Struct({
  ...WebReadResult.fields,
  ...HostedWebReadPage.fields,
  subpages: Schema.Array(HostedWebReadPage),
});

export const HostedWebSearchResult = Schema.Struct({
  ...WebSearchResult.fields,
  researchEnvironmentId: EnvironmentId,
});

export class ResearchHostUnavailable extends Schema.TaggedError<ResearchHostUnavailable>()(
  "ResearchHostUnavailable",
  {
    environmentId: EnvironmentId,
    reason: Schema.Literals(["link", "transport", "forwarding_loop", "invalid_response"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return "The configured research host is unavailable. Check its peer link and ensure it reads locally. No local fetch was attempted.";
  }
}

export class ResearchCacheOwnerError extends Schema.TaggedError<ResearchCacheOwnerError>()(
  "ResearchCacheOwnerError",
  {},
) {
  override get message() {
    return "Cache continuation requires the original cacheEnvironmentId and the same research host. Read the URL again after changing hosts.";
  }
}

/** @effect-expect-leaking McpInvocationContext -- forwarding preserves the current request caller, never the registering caller. */
export class ResearchHost extends Context.Service<
  ResearchHost,
  {
    readonly search: (
      input: typeof WebSearchInput.Type,
    ) => Effect.Effect<
      typeof HostedWebSearchResult.Type,
      Search.WebSearchInputError | ResearchHostUnavailable | OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
    readonly read: (
      input: typeof HostedWebReadInput.Type,
    ) => Effect.Effect<
      typeof HostedWebReadResult.Type,
      | WebResearch.WebResearchInputError
      | WebResearch.WebResearchBudgetError
      | TinyFish.TinyFishBudgetError
      | TinyFish.TinyFishProviderError
      | PageCache.PageCacheError
      | PageCache.PageContinuationExpired
      | ResearchHostUnavailable
      | ResearchCacheOwnerError
      | SessionRead.SessionReadError
      | OrchestratorMcpFailure,
      Invocation.McpInvocationContext
    >;
  }
>()("t3/webResearch/ResearchHost") {}

// The peer call uses the same wire schemas as the registered tool, without
// importing its registration and creating a service/transport dependency cycle.
const peerTool = Tool.make("web_read", {
  parameters: HostedWebReadInput,
  success: HostedWebReadResult,
  failure: OrchestratorMcpFailure,
}).annotate(Tool.Readonly, true);
const sessionPeerTool = Tool.make("web_read", {
  parameters: McpRouting.parameters(HostedWebReadInput),
  success: HostedWebReadResult,
  failure: OrchestratorMcpFailure,
}).annotate(Tool.Readonly, true);
const decodeInput = Schema.decodeUnknownEffect(HostedWebReadInput);

const make = Effect.gen(function* () {
  const research = yield* WebResearch.WebResearch;
  const settings = yield* Settings.ServerSettingsService;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const links = yield* PeerLinks.PeerLinks;
  const sessionReader = yield* Effect.serviceOption(SessionRead.SessionRead);

  const read = Effect.fn("ResearchHost.read")(function* (raw: typeof HostedWebReadInput.Type) {
    const input = yield* decodeInput(raw).pipe(
      Effect.mapError(() => new WebResearch.WebResearchInputError({ reason: "invalid_input" })),
    );
    const scope = yield* Invocation.McpInvocationContext;
    if (input.useSession === true) {
      const selection = input.session;
      if (selection === undefined)
        return yield* new SessionRead.SessionReadError({ reason: "selection" });
      if (
        (input.cacheId !== undefined &&
          (!input.cacheId.startsWith("session:") ||
            input.cacheEnvironmentId !== selection.environmentId)) ||
        (input.cacheId === undefined && input.cacheEnvironmentId !== undefined)
      )
        return yield* new SessionRead.SessionReadError({ reason: "continuation" });
      if (selection.environmentId === scope.environmentId) {
        if (Option.isNone(sessionReader))
          return yield* new SessionRead.SessionReadError({ reason: "owner_unavailable" });
        const page = yield* sessionReader.value.read({ ...input, session: selection });
        return { ...page, cacheEnvironmentId: scope.environmentId, subpages: [] };
      }
      if (scope.client?.linked === true || selection.contextThreadId === undefined)
        return yield* new SessionRead.SessionReadError({ reason: "forwarding" });
      if (!scope.capabilities.has("preview"))
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Session reads require the calling thread's preview capability.",
        });
      const resolved = yield* links
        .resolve(selection.environmentId)
        .pipe(
          Effect.mapError((cause) =>
            cause.reason === "expired"
              ? new OrchestratorMcpFailure({ code: "capability_denied", message: cause.message })
              : new SessionRead.SessionReadError({ reason: "owner_unavailable", cause }),
          ),
        );
      if (resolved.link.access === "read-only")
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Session reads require an agent-access peer link.",
        });
      const page = yield* forwarding
        .call(
          scope,
          sessionPeerTool,
          selection.environmentId,
          {
            ...input,
            contextThreadId: selection.contextThreadId,
            contextCapabilities: [...scope.capabilities],
          },
          { expectedLinkIdentity: resolved.link.launchIdentity },
        )
        .pipe(
          Effect.mapError((cause) =>
            cause.code === "orchestration_error"
              ? new SessionRead.SessionReadError({ reason: "owner_unavailable", cause })
              : cause,
          ),
        );
      if (
        page.cacheEnvironmentId !== selection.environmentId ||
        page.session?.environmentId !== selection.environmentId ||
        page.session.profileId !== selection.profileId ||
        page.session.tabId !== selection.tabId ||
        page.sourceRung !== "session" ||
        !page.cacheId.startsWith("session:") ||
        page.subpages.length !== 0
      )
        return yield* new SessionRead.SessionReadError({ reason: "response" });
      return page;
    }
    if (input.cacheId?.startsWith("session:"))
      return yield* new SessionRead.SessionReadError({ reason: "selection" });
    const host =
      (yield* settings.getSettings.pipe(
        Effect.mapError(
          (cause) =>
            new ResearchHostUnavailable({
              environmentId: scope.environmentId,
              reason: "link",
              cause,
            }),
        ),
      )).webResearch.host ?? scope.environmentId;
    if (
      (input.cacheId !== undefined && input.cacheEnvironmentId !== host) ||
      (input.cacheId === undefined && input.cacheEnvironmentId !== undefined)
    )
      return yield* new ResearchCacheOwnerError();

    if (host === scope.environmentId) {
      const page = yield* research.read(input);
      return {
        ...page,
        cacheEnvironmentId: host,
        subpages: page.subpages.map((child) => ({ ...child, cacheEnvironmentId: host })),
      };
    }
    // PeerForwarding already fences linked callers. Reject here as well so a
    // misconfigured host is a typed host failure and can never form a chain.
    if (scope.client?.linked === true)
      return yield* new ResearchHostUnavailable({ environmentId: host, reason: "forwarding_loop" });
    const resolved = yield* links
      .resolve(host)
      .pipe(
        Effect.mapError((cause) =>
          cause.reason === "expired"
            ? new OrchestratorMcpFailure({ code: "capability_denied", message: cause.message })
            : new ResearchHostUnavailable({ environmentId: host, reason: "link", cause }),
        ),
      );
    if (resolved.link.access === "read-only")
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "Research reads require a peer link with agent access.",
      });
    // Queueing and crawl delays can exceed a fetch budget; the host times active HTTP work.
    const page = yield* forwarding
      .call(scope, peerTool, host, input, { expectedLinkIdentity: resolved.link.launchIdentity })
      .pipe(
        Effect.mapError((cause) =>
          cause._tag === "OrchestratorMcpFailure" && cause.code === "orchestration_error"
            ? new ResearchHostUnavailable({ environmentId: host, reason: "transport", cause })
            : cause,
        ),
      );
    if (
      page.cacheEnvironmentId !== host ||
      page.subpages.some((child) => child.cacheEnvironmentId !== host)
    )
      return yield* new ResearchHostUnavailable({
        environmentId: host,
        reason: "invalid_response",
      });
    return page;
  });
  const searchTool = Tool.make("web_search", {
    parameters: WebSearchInput,
    success: HostedWebSearchResult,
    failure: OrchestratorMcpFailure,
  }).annotate(Tool.Readonly, true);
  const search = Effect.fn("ResearchHost.search")(function* (input: typeof WebSearchInput.Type) {
    const scope = yield* Invocation.McpInvocationContext;
    const host =
      (yield* settings.getSettings.pipe(
        Effect.mapError(
          (cause) =>
            new ResearchHostUnavailable({
              environmentId: scope.environmentId,
              reason: "link",
              cause,
            }),
        ),
      )).webResearch.host ?? scope.environmentId;
    if (host === scope.environmentId)
      return { ...(yield* research.search(input)), researchEnvironmentId: host };
    if (scope.client?.linked === true)
      return yield* new ResearchHostUnavailable({ environmentId: host, reason: "forwarding_loop" });
    const resolved = yield* links
      .resolve(host)
      .pipe(
        Effect.mapError((cause) =>
          cause.reason === "expired"
            ? new OrchestratorMcpFailure({ code: "capability_denied", message: cause.message })
            : new ResearchHostUnavailable({ environmentId: host, reason: "link", cause }),
        ),
      );
    if (resolved.link.access === "read-only")
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "Research searches require a peer link with agent access.",
      });
    const result = yield* forwarding
      .call(scope, searchTool, host, input, { expectedLinkIdentity: resolved.link.launchIdentity })
      .pipe(
        Effect.mapError((cause) =>
          cause._tag === "OrchestratorMcpFailure" && cause.code === "orchestration_error"
            ? new ResearchHostUnavailable({ environmentId: host, reason: "transport", cause })
            : cause,
        ),
      );
    if (result.researchEnvironmentId !== host)
      return yield* new ResearchHostUnavailable({
        environmentId: host,
        reason: "invalid_response",
      });
    return result;
  });
  return ResearchHost.of({ read, search });
});

export const layer = Layer.effect(ResearchHost, make);
