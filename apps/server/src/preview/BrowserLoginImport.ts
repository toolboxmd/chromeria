import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type PreviewAutomationUnavailableError,
  type PreviewLoginExportInput,
  type PreviewLoginExportResult,
  type PreviewLoginImportInput,
  type PreviewLoginImportResult,
} from "@t3tools/contracts";
import { isGoogleHost } from "@t3tools/shared/browserLoginImport";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import { callerLinkOrigin } from "../mcp/linkOrigin.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { loadCaller } from "../mcp/threadAccess.ts";
import { BrowserLoginExportTool } from "../mcp/toolkits/loginImport/tools.ts";
import { OrchestratorToolkit } from "../mcp/toolkits/orchestrator/tools.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import { interactiveAuthority } from "../peer/PeerAuthorityRecord.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import { registrableDomain } from "./CredentialFill.ts";
import { auditReason, recordLoginImport } from "./LoginImportAudit.ts";
import { loginRefusal } from "./ServerLoginCookies.ts";
import * as ServerBrowser from "./ServerBrowser.ts";

/**
 * Copies one site's session between linked environments' server browsers on demand:
 * the destination pulls one bounded snapshot from the peer that shares its logins.
 */
export class BrowserLoginImport extends Context.Service<
  BrowserLoginImport,
  {
    readonly importLogin: (
      scope: McpInvocationContext.McpInvocationScope,
      input: PreviewLoginImportInput,
    ) => Effect.Effect<
      PreviewLoginImportResult,
      OrchestratorMcpFailure | PreviewAutomationUnavailableError
    >;
    readonly exportLogin: (
      scope: McpInvocationContext.McpInvocationScope,
      input: PreviewLoginExportInput,
    ) => Effect.Effect<PreviewLoginExportResult, OrchestratorMcpFailure>;
  }
>()("t3/preview/BrowserLoginImport") {}

const denied = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Browser login import is unavailable in this environment.",
  });

/**
 * The thread that asked, as the calling link reports it, kept only when it names the
 * environment of the verified peer authority. It is the peer's own claim, not part of the digest.
 */
const initiatingThread = (scope: McpInvocationContext.McpInvocationScope) => {
  const sender = scope.client?.senderThread;
  const peer = interactiveAuthority(scope.authority)?.peer.environmentId;
  return sender !== undefined && peer !== undefined && sender.environmentId === peer
    ? sender.threadId
    : null;
};

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const forwarding = yield* Effect.serviceOption(PeerForwarding.PeerForwarding);
  const auditServices = yield* Effect.context<
    ServerConfig.ServerConfig | FileSystem.FileSystem | Path.Path
  >();
  const audit = (entry: Parameters<typeof recordLoginImport>[0]) =>
    recordLoginImport(entry).pipe(Effect.provideContext(auditServices));
  const loginCookies = () =>
    Option.isSome(browser) && browser.value.loginCookies !== undefined
      ? Effect.succeed(browser.value.loginCookies)
      : Effect.fail(unavailable());

  const sourceIsHere = (profileId: string) =>
    new OrchestratorMcpFailure({
      code: "invalid_request",
      message: `source_is_here: This environment holds the shared logins. Open profile "${profileId}" with preview_open instead.`,
    });

  /** The one linked environment whose grant shares logins; its export checks the grant again. */
  const resolveSource = Effect.fn("BrowserLoginImport.resolveSource")(function* (
    scope: McpInvocationContext.McpInvocationScope,
    input: PreviewLoginImportInput,
    sourceProfileId: string,
  ) {
    if (input.sourceEnvironmentId === scope.environmentId)
      return yield* sourceIsHere(sourceProfileId);
    if (Option.isNone(forwarding)) return yield* unavailable();
    if (input.sourceEnvironmentId !== undefined) return input.sourceEnvironmentId;
    if (Option.isSome(authority) && (yield* authority.value.grants).some((g) => g.browserExport))
      return yield* sourceIsHere(sourceProfileId);
    const listed = yield* forwarding.value.links(scope);
    const sharing = yield* Effect.filter(
      listed.links.filter((link) => link.status !== "expired"),
      (link) =>
        forwarding.value
          .call(scope, OrchestratorToolkit.tools.orchestrator_capabilities, link.environmentId, {})
          .pipe(
            Effect.map((capabilities) => capabilities.browserExport === true),
            Effect.orElseSucceed(() => false),
          ),
      { concurrency: "unbounded" },
    );
    const [only, ...rest] = sharing;
    if (only === undefined)
      return yield* denied(
        "grant_off: No linked environment shares its browser logins with this one. Sign in here with preview_fill_credential, or ask the user to turn on Share browser logins for this link on the machine that holds them.",
      );
    if (rest.length > 0)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: `source_ambiguous: Several linked environments share browser logins. Pass sourceEnvironmentId, one of: ${sharing
          .map((link) => `${link.label} (${link.environmentId})`)
          .join(", ")}.`,
      });
    return only.environmentId;
  });

  const importLogin: BrowserLoginImport["Service"]["importLogin"] = Effect.fn(
    "BrowserLoginImport.importLogin",
  )(function* (invocation, input) {
    const local = Effect.provideService(McpInvocationContext.McpInvocationContext, invocation);
    const scope = yield* McpInvocationContext.requireThreadMcpCapability("preview").pipe(local);
    const caller = yield* loadCaller().pipe(
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
      local,
    );
    // As for the credential vault: work a link started carries no consent to copy sessions.
    if (callerLinkOrigin(caller) !== undefined)
      return yield* denied(
        "linked_caller: Work a linked environment started cannot copy browser logins. Report to your parent, which can run its own computer-use child where the logins are.",
      );
    const login = yield* loginCookies();
    const hosts = [...new Set(input.hosts)];
    const sourceProfileId = input.sourceProfileId ?? input.profileId;
    let source: EnvironmentId | null = null;
    return yield* Effect.gen(function* () {
      if (hosts.some((host) => isGoogleHost(host, registrableDomain)))
        return yield* loginRefusal("device_bound");
      yield* login.check(input.profileId);
      source = yield* resolveSource(scope, input, sourceProfileId);
      if (Option.isNone(forwarding)) return yield* unavailable();
      const exported = yield* forwarding.value.call(scope, BrowserLoginExportTool, source, {
        hosts,
        profileId: sourceProfileId,
      });
      return yield* login.import({
        profileId: input.profileId,
        hosts,
        cookies: exported.cookies,
        skipped: exported.skipped,
      });
    }).pipe(
      Effect.tap((result) =>
        audit({
          side: "import",
          environmentId: scope.environmentId,
          peerEnvironmentId: source,
          threadId: scope.thread.threadId,
          profileId: input.profileId,
          sourceProfileId,
          hosts,
          cookies: result.imported,
          skipped: result.skipped,
          result: "imported",
        }),
      ),
      Effect.tapError((error) =>
        audit({
          side: "import",
          environmentId: scope.environmentId,
          peerEnvironmentId: source,
          threadId: scope.thread.threadId,
          profileId: input.profileId,
          sourceProfileId,
          hosts,
          cookies: null,
          skipped: null,
          result: auditReason(error),
        }),
      ),
    );
  });

  const exportLogin: BrowserLoginImport["Service"]["exportLogin"] = Effect.fn(
    "BrowserLoginImport.exportLogin",
  )(function* (scope, input) {
    if (Option.isNone(authority)) return yield* unavailable();
    const entry = {
      side: "export" as const,
      environmentId: scope.environmentId,
      peerEnvironmentId: interactiveAuthority(scope.authority)?.peer.environmentId ?? null,
      threadId: initiatingThread(scope),
      profileId: input.profileId,
      sourceProfileId: input.profileId,
      hosts: input.hosts,
    };
    return yield* Effect.gen(function* () {
      // Before any cookie read: listing who shares logins grants nothing.
      yield* authority.value.authorizeBrowserExport(scope);
      const login = yield* loginCookies();
      return yield* login.export(input);
    }).pipe(
      Effect.tap((result) =>
        audit({
          ...entry,
          cookies: result.cookies.length,
          skipped: result.skipped,
          result: "exported",
        }),
      ),
      Effect.tapError((error) =>
        audit({ ...entry, cookies: null, skipped: null, result: auditReason(error) }),
      ),
    );
  });

  return BrowserLoginImport.of({ importLogin, exportLogin });
});

export const layer = Layer.effect(BrowserLoginImport, make);
