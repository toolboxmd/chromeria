import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Settings from "../serverSettings.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import * as PeerLinks from "./PeerLinks.ts";
import { descriptorOf, linkTo, servePeer } from "./PeerLinks.testkit.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../project/ProjectService.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import type { McpInvocationScope } from "../mcp/McpInvocationContext.ts";

const a = descriptorOf("authority-a", "A", { peerAuthority: 1 });
const b = descriptorOf("authority-b", "B", { peerAuthority: 1 });
const sourceThread = ThreadId.make("source-thread");
const shell = liveThreadShell(sourceThread, { runtimeMode: "full-access" });
const scopeThread = {
  threadId: sourceThread,
  providerSessionId: "credential-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
};
const scope: McpInvocationScope = {
  environmentId: a.environmentId,
  requestNamespace: "credential-1",
  issuedAt: 0,
  thread: scopeThread,
  client: undefined,
  capabilities: new Set(["orchestration"]),
};
const projections = Layer.mock(ProjectionStore.ProjectionStoreV2)({
  getThreadShell: () => Effect.succeed(shell),
});
const threadService = Layer.mock(Threads.ThreadManagementService)({
  getThreadShell: () => Effect.succeed(shell),
});

const makeSource = (environment = a) =>
  Effect.gen(function* () {
    const services = yield* PeerForwarding.layer.pipe(
      Layer.provideMerge(PeerMcpClient.layer),
      Layer.provideMerge(PeerAuthority.layer),
      Layer.provideMerge(PeerLinks.layer),
      Layer.provideMerge(Sqlite.layerMemory),
      Layer.provide(projections),
      Layer.provide(threadService),
      Layer.provide(ServerSecretStore.layer),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(
        Layer.succeed(ServerEnvironment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(environment.environmentId),
          getDescriptor: Effect.succeed(environment),
        }),
      ),
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "authority-source-" })),
      Layer.provide(NodeServices.layer),
      Layer.fresh,
      Layer.build,
    );
    return {
      forwarding: Context.get(services, PeerForwarding.PeerForwarding),
      peers: Context.get(services, PeerMcpClient.PeerMcpClient),
      links: Context.get(services, PeerLinks.PeerLinks),
      authority: Context.get(services, PeerAuthority.PeerAuthority),
    };
  });

const makeDestination = (modern = true, environment = b, controlStreamUnavailable = false) =>
  Effect.gen(function* () {
    const called = yield* Ref.make(0);
    const captured = yield* Ref.make<PeerAuthority.Authority | undefined>(undefined);
    const callingThread = yield* Ref.make<ThreadId | undefined>(undefined);
    const tools = McpHttpServer.layerProjectRegistration.pipe(
      Layer.provide(threadService),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/test" }),
      ),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
      Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
      Layer.provide(Settings.layerTest()),
      Layer.provide(Layer.mock(Repositories.SourceControlRepositoryService)({})),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(
        Layer.mock(Projects.ProjectService)({
          listPage: () =>
            Effect.gen(function* () {
              yield* Ref.update(called, (n) => n + 1);
              const invocation = yield* Effect.serviceOption(Invocation.McpInvocationContext);
              yield* Ref.set(
                callingThread,
                invocation._tag === "Some" ? invocation.value.thread?.threadId : undefined,
              );
              yield* Ref.set(
                captured,
                invocation._tag === "Some" ? invocation.value.authority : undefined,
              );
              return { projects: [], nextCursor: null };
            }),
        }),
      ),
    );
    const projectionContext = yield* projections.pipe(Layer.build);
    const serverContext = yield* NodeHttpServer.layerTest.pipe(Layer.fresh, Layer.build);
    const peer = yield* servePeer(
      environment,
      tools,
      modern
        ? {
            controlStreamUnavailable,
            authority: {
              projections: Context.get(projectionContext, ProjectionStore.ProjectionStoreV2),
            },
          }
        : undefined,
    ).pipe(Effect.provide(Layer.succeedContext(serverContext)));
    return { ...peer, called, captured, callingThread };
  });

it.live(
  "validates a direct call over A's outbound channel without any delegation or B-to-A link",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const target = yield* makeDestination();
        const source = yield* makeSource();
        yield* linkTo(source.links, target, "full-access");
        const result = yield* source.forwarding.call(
          scope,
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        expect(result).toEqual({ projects: [], nextCursor: null });
        expect(yield* Ref.get(target.called)).toBe(1);
        if (target.peerLinks === undefined) return yield* Effect.die("Missing modern peer links");
        expect(yield* target.peerLinks.list).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects an altered action and an old destination before its handler runs", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const target = yield* makeDestination();
      const old = yield* makeDestination(false);
      const source = yield* makeSource();
      yield* linkTo(source.links, target, "full-access");
      const link = yield* source.links.resolve(b.environmentId);
      const envelope = yield* source.authority.prepare(
        scope,
        b.environmentId,
        link.link.launchIdentity,
        "t3_project_list",
        {},
        { runtimeMode: "full-access", interactionMode: "default" },
      );
      const failure = yield* source.peers
        .call({
          environmentId: b.environmentId,
          tool: "t3_project_list",
          arguments: { limit: 99 },
          limits: { runtimeMode: "full-access", interactionMode: "default" },
          authority: envelope,
          success: ProjectToolkit.tools.t3_project_list.successSchema,
        })
        .pipe(Effect.flip);
      expect(failure.code).toBe("capability_denied");
      expect(yield* Ref.get(target.called)).toBe(0);
      yield* linkTo(source.links, old, "full-access");
      const unsupported = yield* source.forwarding
        .call(scope, ProjectToolkit.tools.t3_project_list, b.environmentId, {})
        .pipe(Effect.flip);
      expect(unsupported.message).toContain("does not support peer authority");
      expect(yield* Ref.get(old.called)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("supports the opposite dialing direction without granting ordinary reverse access", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const target = yield* makeDestination(true, a);
      const source = yield* makeSource(b);
      yield* linkTo(source.links, target, "full-access");
      yield* source.forwarding.call(
        { ...scope, environmentId: b.environmentId },
        ProjectToolkit.tools.t3_project_list,
        a.environmentId,
        {},
      );
      expect(yield* Ref.get(target.called)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "freezes onward consent, validates A through B when C calls back, and denies after revocation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const c = descriptorOf("authority-c", "C", { peerAuthority: 1 });
        const third = yield* makeDestination(true, c);
        const target = yield* makeDestination();
        const source = yield* makeSource();
        yield* linkTo(source.links, target, "full-access");
        yield* source.forwarding.call(
          scope,
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          {},
        );
        const frozen = yield* Ref.get(target.captured);
        if (
          frozen === undefined ||
          target.peerAuthority === undefined ||
          target.peerLinks === undefined
        )
          return yield* Effect.die("Missing destination authority");
        expect(frozen.ceiling.onward).toBe(false);
        expect(frozen.ceiling.resourceScope).toBe("created-work");
        const grant = (yield* target.peerAuthority.grants)[0];
        if (grant === undefined) return yield* Effect.die("Missing peer grant");
        yield* target.peerAuthority.setGrant({
          ...grant,
          onward: true,
          resourceScope: "environment-resources",
        });
        const narrowChild = ThreadId.make("child-before-consent");
        yield* target.peerAuthority.bind(frozen, narrowChild);
        yield* source.forwarding.call(
          scope,
          ProjectToolkit.tools.t3_project_list,
          b.environmentId,
          { cursor: 1 },
        );
        const authorized = yield* Ref.get(target.captured);
        if (authorized === undefined)
          return yield* Effect.die("Missing freshly consented authority");
        expect(authorized.ceiling.onward).toBe(true);
        const child = ThreadId.make("child-after-consent");
        yield* target.peerAuthority.bind(authorized, child);
        yield* linkTo(target.peerLinks, third, "full-access");
        const bThreads = Layer.mock(Threads.ThreadManagementService)({
          getThreadShell: (id) =>
            Effect.succeed({
              ...shell,
              id,
              linkOrigin: { sessionId: grant.sessionId, label: "A" },
            }),
        });
        const middle = yield* PeerForwarding.layer.pipe(
          Layer.provideMerge(PeerMcpClient.layer),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, target.peerAuthority)),
          Layer.provide(Layer.succeed(PeerLinks.PeerLinks, target.peerLinks)),
          Layer.provide(bThreads),
          Layer.provide(FetchHttpClient.layer),
          Layer.fresh,
          Layer.build,
        );
        const onward = Context.get(middle, PeerForwarding.PeerForwarding);
        const childScope: McpInvocationScope = {
          ...scope,
          environmentId: b.environmentId,
          thread: { ...scopeThread, threadId: child },
        };
        const narrow = yield* onward
          .call(
            { ...childScope, thread: { ...scopeThread, threadId: narrowChild } },
            ProjectToolkit.tools.t3_project_list,
            c.environmentId,
            {},
          )
          .pipe(Effect.flip);
        expect(narrow.code).toBe("capability_denied");
        expect(yield* Ref.get(third.called)).toBe(0);
        yield* onward.call(childScope, ProjectToolkit.tools.t3_project_list, c.environmentId, {});
        expect(yield* Ref.get(third.called)).toBe(1);
        yield* target.peerAuthority.setGrant({ ...grant, onward: false });
        const revoked = yield* onward
          .call(childScope, ProjectToolkit.tools.t3_project_list, c.environmentId, { cursor: 2 })
          .pipe(Effect.flip);
        expect(revoked.code).toBe("capability_denied");
        expect(yield* Ref.get(third.called)).toBe(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("uses bounded long polling when the authenticated stream is unavailable", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const target = yield* makeDestination(true, b, true);
      const source = yield* makeSource();
      yield* linkTo(source.links, target, "full-access");
      yield* source.forwarding.call(
        scope,
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      expect(yield* Ref.get(target.called)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "rejects forged route metadata and a valid envelope from another link before invocation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const target = yield* makeDestination();
        const source = yield* makeSource();
        const other = yield* makeSource(
          descriptorOf("authority-other", "Other", { peerAuthority: 1 }),
        );
        yield* linkTo(source.links, target, "full-access");
        yield* linkTo(other.links, target, "full-access");
        const resolved = yield* source.links.resolve(b.environmentId);
        const limits = { runtimeMode: "full-access" as const, interactionMode: "default" as const };
        const authority = yield* source.authority.prepare(
          scope,
          b.environmentId,
          resolved.link.launchIdentity,
          "t3_project_list",
          {},
          limits,
          undefined,
          true,
        );
        for (const envelope of [
          { ...authority, route: [a.environmentId, a.environmentId] },
          { ...authority, route: [b.environmentId, a.environmentId] },
          { ...authority, descendant: true },
        ]) {
          const refusal = yield* source.peers
            .call({
              environmentId: b.environmentId,
              tool: "t3_project_list",
              arguments: {},
              limits,
              authority: envelope,
              success: ProjectToolkit.tools.t3_project_list.successSchema,
            })
            .pipe(Effect.flip);
          expect(refusal.code).toBe("capability_denied");
        }
        const mismatch = yield* other.peers
          .call({
            environmentId: b.environmentId,
            tool: "t3_project_list",
            arguments: {},
            limits,
            authority,
            success: ProjectToolkit.tools.t3_project_list.successSchema,
          })
          .pipe(Effect.flip);
        expect(mismatch.code).toBe("capability_denied");
        expect(yield* Ref.get(target.called)).toBe(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("selects durable destination context without borrowing a live provider credential", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const target = yield* makeDestination();
      const source = yield* makeSource();
      yield* linkTo(source.links, target, "full-access");
      yield* source.forwarding.call(
        scope,
        ProjectToolkit.tools.t3_project_list,
        b.environmentId,
        {},
      );
      if (target.peerAuthority === undefined) return yield* Effect.die("Missing authority service");
      const grant = (yield* target.peerAuthority.grants)[0];
      if (!grant) return yield* Effect.die("Missing consent");
      yield* target.peerAuthority.setGrant({ ...grant, resourceScope: "environment-resources" });
      yield* source.forwarding.call(scope, ProjectToolkit.tools.t3_project_list, b.environmentId, {
        contextThreadId: sourceThread,
      });
      expect(yield* Ref.get(target.callingThread)).toBe(sourceThread);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
