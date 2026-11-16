import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ThreadId,
  type MirrorCookie,
  type PreviewLoginImportInput,
} from "@t3tools/contracts";
import { selectLoginCookies } from "@t3tools/shared/browserLoginImport";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as LoginImportHandlers from "../mcp/toolkits/loginImport/handlers.ts";
import { LoginExportToolkit } from "../mcp/toolkits/loginImport/tools.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import { descriptorOf, linkTo, servePeer } from "../peer/PeerLinks.testkit.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as BrowserLoginImport from "./BrowserLoginImport.ts";
import { BrowserLoginImport as BrowserLoginImportService } from "./BrowserLoginImportService.ts";
import { registrableDomain } from "./CredentialFill.ts";
import { LOGIN_IMPORT_AUDIT_FILE } from "./LoginImportAudit.ts";
import * as ServerBrowser from "./ServerBrowser.ts";

// bestia imports from rocky, which holds the user's logins.
const bestia = descriptorOf("login-import-bestia", "bestia", { peerAuthority: 1 });
const rocky = descriptorOf("login-import-rocky", "rocky", { peerAuthority: 1 });
const threadId = ThreadId.make("bestia-agent");
const shell = liveThreadShell(threadId, { runtimeMode: "full-access" });
const linkBornShell = {
  ...shell,
  linkOrigin: { sessionId: "rocky-session", label: "rocky", access: "full-access" as const },
};
const SECRET = "synthetic-session-value";
const rockyJar: ReadonlyArray<MirrorCookie> = [
  {
    name: "session",
    value: SECRET,
    domain: "admin.shop.test",
    path: "/",
    secure: true,
    httpOnly: true,
    sameSite: "lax",
  },
  {
    name: "bank",
    value: "unrelated-site",
    domain: "bank.test",
    path: "/",
    secure: true,
    httpOnly: true,
    sameSite: "lax",
  },
];
const scope: McpInvocationScope = {
  environmentId: bestia.environmentId,
  requestNamespace: "bestia-credential",
  issuedAt: 0,
  thread: {
    threadId,
    providerSessionId: "bestia-credential",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["preview", "orchestration"]),
};
const request: PreviewLoginImportInput = {
  hosts: ["admin.shop.test"],
  profileId: "work",
  sourceEnvironmentId: rocky.environmentId,
};

const auditLines = (stateDir: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(stateDir, LOGIN_IMPORT_AUDIT_FILE);
    if (!(yield* fs.exists(file))) return [];
    return (yield* fs.readFileString(file)).trim().split("\n");
  });

const setup = (callerShell = shell) =>
  Effect.gen(function* () {
    // rocky: serves browser_login_export over its real MCP transport and peer authority.
    const exportReads = yield* Ref.make(0);
    const beforeExport = yield* Ref.make<(scope: McpInvocationScope) => Effect.Effect<void>>(
      () => Effect.void,
    );
    const rockyDatabase = yield* Sqlite.layerMemory.pipe(Layer.fresh, Layer.build);
    const rockyConfig = yield* ServerConfig.layerTest(process.cwd(), {
      prefix: "login-import-rocky-",
    }).pipe(Layer.build);
    const rockyBrowser = Layer.mock(ServerBrowser.ServerBrowser)({
      loginCookies: {
        check: () => Effect.void,
        import: () => Effect.die("rocky only exports"),
        export: ({ hosts }) =>
          Effect.gen(function* () {
            yield* Ref.update(exportReads, (n) => n + 1);
            const selected = selectLoginCookies({ cookies: rockyJar, hosts, registrableDomain });
            if (selected._tag === "refused") return yield* Effect.die(selected.reason);
            return { cookies: selected.cookies, skipped: selected.skipped };
          }),
      },
    });
    const hooked = Layer.effect(
      BrowserLoginImportService,
      Effect.gen(function* () {
        const real = yield* BrowserLoginImportService;
        return BrowserLoginImportService.of({
          importLogin: real.importLogin,
          exportLogin: (accepted, input) =>
            Ref.get(beforeExport).pipe(
              Effect.flatMap((hook) => hook(accepted)),
              Effect.andThen(real.exportLogin(accepted, input)),
            ),
        });
      }),
    ).pipe(Layer.provide(BrowserLoginImport.layer));
    const projections = Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThreadShell: () => Effect.succeed(callerShell),
    });
    const threads = Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: () => Effect.succeed(callerShell),
    });
    const tools = McpHttpServer.toolkitRegistration(
      LoginExportToolkit,
      LoginImportHandlers.layerExport,
    ).pipe(Layer.provide(hooked), Layer.provide(rockyBrowser), Layer.provide(threads));
    const projectionContext = yield* projections.pipe(Layer.build);
    const serverContext = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
    const target = yield* servePeer(rocky, tools, {
      database: Layer.succeedContext(rockyDatabase),
      serverConfig: Context.get(rockyConfig, ServerConfig.ServerConfig),
      authority: { projections: Context.get(projectionContext, ProjectionStore.ProjectionStoreV2) },
    }).pipe(Effect.provide(Layer.succeedContext(serverContext)));

    // bestia: the importing agent's environment, with its real forwarding stack.
    const imports = yield* Ref.make<ReadonlyArray<ReadonlyArray<MirrorCookie>>>([]);
    const bestiaBrowser = Layer.mock(ServerBrowser.ServerBrowser)({
      loginCookies: {
        check: () => Effect.void,
        export: () => Effect.die("bestia only imports"),
        import: ({ profileId, cookies, skipped }) =>
          Ref.update(imports, (seen) => [...seen, cookies]).pipe(
            Effect.as({
              profileId,
              imported: cookies.length,
              skipped: { ...skipped, sharedDomain: 0 },
            }),
          ),
      },
    });
    const bestiaConfig = yield* ServerConfig.layerTest(process.cwd(), {
      prefix: "login-import-bestia-",
    }).pipe(Layer.build);
    const services = yield* BrowserLoginImport.layer.pipe(
      Layer.provide(bestiaBrowser),
      Layer.provideMerge(PeerForwarding.layer),
      Layer.provideMerge(PeerMcpClient.layer),
      Layer.provideMerge(PeerAuthority.layer),
      Layer.provideMerge(PeerLinks.layer),
      Layer.provide(Settings.layerTest({ linkedEnvironmentsEnabled: true })),
      Layer.provideMerge(Sqlite.layerMemory),
      Layer.provide(projections),
      Layer.provide(threads),
      Layer.provide(ServerSecretStore.layer),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(
        Layer.succeed(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(bestia.environmentId),
          getDescriptor: Effect.succeed(bestia),
        }),
      ),
      Layer.provide(Layer.succeedContext(bestiaConfig)),
      Layer.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
      Layer.fresh,
      Layer.build,
    );
    const links = Context.get(services, PeerLinks.PeerLinks);
    yield* linkTo(links, target, "full-access");
    const rockyAuthority = target.peerAuthority;
    if (rockyAuthority === undefined) return yield* Effect.die("rocky needs peer authority");
    const setGrant = (change: { readonly preview?: boolean; readonly browserExport?: boolean }) =>
      Effect.gen(function* () {
        const [summary] = yield* rockyAuthority.grants;
        if (summary === undefined) return yield* Effect.die("bestia has not enrolled yet");
        const { environmentId: _environmentId, label: _label, ...grant } = summary;
        yield* rockyAuthority.setGrant({ ...grant, ...change });
      });
    return {
      service: Context.get(services, BrowserLoginImportService),
      rockyAuthority,
      rockySql: Context.get(rockyDatabase, SqlClient.SqlClient),
      setGrant,
      exportReads,
      beforeExport,
      imports,
      rockyStateDir: Context.get(rockyConfig, ServerConfig.ServerConfig).stateDir,
      bestiaStateDir: Context.get(bestiaConfig, ServerConfig.ServerConfig).stateDir,
    };
  });

const provided = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)));

it.live("imports rocky's session only under its separate export grant, and logs no values", () =>
  provided(
    Effect.gen(function* () {
      const env = yield* setup();
      // The first call enrolls bestia's link on rocky with a default grant: export off.
      const off = yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      expect(off.message).toMatch(/^grant_off:/);
      yield* env.setGrant({ preview: true });
      const driveOnly = yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      expect(driveOnly.message).toMatch(/^grant_off:/);
      expect(yield* Ref.get(env.exportReads)).toBe(0);
      expect(yield* env.rockyAuthority.browserExportGranted(scope)).toBe(false);

      yield* env.setGrant({ browserExport: true });
      const [grant] = yield* env.rockyAuthority.grants;
      const linkScope = (sessionId: string): McpInvocationScope => ({
        ...scope,
        environmentId: rocky.environmentId,
        thread: undefined,
        client: { sessionId, label: "bestia", access: "full-access", linked: true },
      });
      // The capability hint that lets bestia find rocky without sourceEnvironmentId.
      expect(yield* env.rockyAuthority.browserExportGranted(linkScope(grant!.sessionId))).toBe(
        true,
      );
      expect(yield* env.rockyAuthority.browserExportGranted(linkScope("other-session"))).toBe(
        false,
      );
      const result = yield* env.service.importLogin(scope, request);
      expect(result).toEqual({
        profileId: "work",
        imported: 1,
        skipped: { deviceBound: 0, partitioned: 0, sharedDomain: 0 },
      });
      expect((yield* Ref.get(env.imports)).map((batch) => batch.map((c) => c.name))).toEqual([
        ["session"],
      ]);
      expect(JSON.stringify(result)).not.toContain(SECRET);

      const rockyLines = yield* auditLines(env.rockyStateDir);
      const bestiaLines = yield* auditLines(env.bestiaStateDir);
      // The source records the thread that asked, as its link reported it.
      expect(rockyLines.map((line) => JSON.parse(line).threadId)).toEqual([
        "bestia-agent",
        "bestia-agent",
        "bestia-agent",
      ]);
      expect(rockyLines.map((line) => JSON.parse(line).result)).toEqual([
        "grant_off",
        "grant_off",
        "exported",
      ]);
      expect(bestiaLines.map((line) => JSON.parse(line).result)).toEqual([
        "grant_off",
        "grant_off",
        "imported",
      ]);
      for (const line of [...rockyLines, ...bestiaLines]) expect(line).not.toContain(SECRET);
    }),
  ),
);

it.live("refuses at execution when the grant is turned off or the link revoked after accept", () =>
  provided(
    Effect.gen(function* () {
      const env = yield* setup();
      yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      yield* env.setGrant({ browserExport: true });

      yield* Ref.set(env.beforeExport, () =>
        env.setGrant({ browserExport: false }).pipe(Effect.orDie),
      );
      const turnedOff = yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      expect(turnedOff.message).toMatch(/^grant_off:/);

      yield* env.setGrant({ browserExport: true });
      yield* Ref.set(env.beforeExport, () =>
        env.rockySql`UPDATE auth_sessions SET revoked_at = '2020-01-01T00:00:00.000Z'`.pipe(
          Effect.asVoid,
          Effect.orDie,
        ),
      );
      const revoked = yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      expect(revoked.message).toContain("revoked");
      expect(yield* Ref.get(env.exportReads)).toBe(0);
    }),
  ),
);

it.live("refuses an accepted action whose stored authority is a descendant", () =>
  provided(
    Effect.gen(function* () {
      const env = yield* setup();
      yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      yield* env.setGrant({ browserExport: true });
      yield* Ref.set(env.beforeExport, (accepted) =>
        env.rockySql`UPDATE peer_authority_records SET record_json = json_set(record_json, '$.envelope.descendant', json('true')) WHERE id = ${accepted.authority?.id ?? ""}`.pipe(
          Effect.asVoid,
          Effect.orDie,
        ),
      );
      const failure = yield* env.service.importLogin(scope, request).pipe(Effect.flip);
      expect(failure.message).toBe("Linked work cannot export this environment's browser logins.");
      expect(yield* Ref.get(env.exportReads)).toBe(0);
    }),
  ),
);

it.live(
  "refuses link-born callers, Google hosts and direct export calls before reading cookies",
  () =>
    provided(
      Effect.gen(function* () {
        const linkBorn = yield* setup(linkBornShell);
        const refused = yield* linkBorn.service.importLogin(scope, request).pipe(Effect.flip);
        expect(refused.message).toMatch(/^linked_caller:/);
        expect(yield* linkBorn.rockyAuthority.grants).toEqual([]);

        const env = yield* setup();
        const google = yield* env.service
          .importLogin(scope, { ...request, hosts: ["mail.google.com"] })
          .pipe(Effect.flip);
        expect(google.message).toMatch(/^device_bound:/);
        const noPreview = yield* env.service
          .importLogin({ ...scope, capabilities: new Set(["orchestration"]) }, request)
          .pipe(Effect.flip);
        expect(noPreview._tag).toBe("PreviewAutomationUnavailableError");
        expect(yield* env.rockyAuthority.grants).toEqual([]);
        // A local agent calling the peer-only export directly has no accepted peer action.
        const direct = yield* env.service
          .exportLogin(scope, { hosts: ["admin.shop.test"], profileId: "work" })
          .pipe(Effect.flip);
        expect(direct.message).toBe("Exporting browser logins needs an accepted peer action.");
      }),
    ),
);
