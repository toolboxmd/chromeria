// @effect-diagnostics nodeBuiltinImport:off - Synthetic Chromium integration owns its private browser resources.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  EnvironmentId,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  PreviewWatchStartResult,
  PreviewWatchReadResult,
  type PreviewAutomationStatus,
  type AuthMcpOperation,
  ProjectId,
  AuthSessionId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Context from "effect/Context";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import type { Page } from "playwright-core";
import * as Broker from "../mcp/PreviewAutomationBroker.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as ToolAccess from "../mcp/McpToolAccess.ts";
import { liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as WatchHandlers from "../mcp/toolkits/preview/watchHandlers.ts";
import { PreviewWatchToolkit } from "../mcp/toolkits/preview/tools.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as ServerSecrets from "../auth/ServerSecretStore.ts";
import * as Database from "../persistence/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Settings from "../serverSettings.ts";
import * as Manager from "./Manager.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as Desktop from "./DesktopBrowserChannel.ts";
import * as Vault from "./CredentialVault.ts";
import { settlementLayer, settleThread } from "./BrowserSettlement.testkit.ts";

const testPages = vi.hoisted(() => [] as Page[]);

// Exercise the native service using an existing development Chromium, with private test profiles.
// Production still enforces its own pinned browser version.
vi.mock("./ServerBrowserContexts.ts", async (importOriginal) => {
  const original = await importOriginal<typeof import("./ServerBrowserContexts.ts")>();
  return {
    ...original,
    ServerBrowserContexts: class extends original.ServerBrowserContexts {
      constructor(options: ConstructorParameters<typeof original.ServerBrowserContexts>[0]) {
        super({
          ...options,
          version: process.env.CHROMERIA_WATCH_TEST_CHROME_VERSION ?? options.version,
          env: { ...process.env, T3CODE_SERVER_BROWSER_SANDBOX: "0" },
        });
      }
      override async contextFor(profileId: string, isolationKey?: string) {
        const context = await super.contextFor(profileId, isolationKey);
        context.on("page", (page) => {
          if (!testPages.includes(page)) testPages.push(page);
        });
        return context;
      }
    },
  };
});

const decodeStart = Schema.decodeUnknownEffect(PreviewWatchStartResult);
const decodeRead = Schema.decodeUnknownEffect(PreviewWatchReadResult);
const threadId = ThreadId.make("synthetic-watch-thread");
const environmentId = EnvironmentId.make("synthetic-watch-environment");
const scope: Invocation.McpThreadInvocationScope = {
  environmentId,
  thread: {
    threadId,
    providerSessionId: "synthetic-agent",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["preview", "orchestration"]),
  issuedAt: 1,
  requestNamespace: "synthetic-agent",
};
const fixture = () =>
  Effect.gen(function* () {
    let shell = liveThreadShell(threadId);
    const shells = { getThreadShell: () => Effect.sync(() => shell) };
    const settings = Settings.layerTest({ enableAgentBrowserAccess: true });
    const environment = Layer.succeed(Environment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(environmentId),
      getDescriptor: Effect.die("unused descriptor"),
    });
    const deps = Layer.mergeAll(
      Layer.mock(Threads.ThreadManagementService)(shells),
      Layer.mock(Projections.ProjectionStoreV2)(shells),
      settings,
      environment,
      settlementLayer,
      Manager.layer,
      AuthSessions.layer,
      Layer.succeed(PreviewBrowser.PreviewBrowser, {
        executable: Effect.succeed(process.env.CHROMERIA_WATCH_TEST_CHROME!),
        installed: Effect.succeed(Option.some(process.env.CHROMERIA_WATCH_TEST_CHROME!)),
      }),
      Layer.mock(Desktop.DesktopBrowserChannel)({
        available: false,
        detached: Stream.never,
        attached: Stream.never,
      }),
      Layer.mock(Vault.CredentialVault)({ uris: () => Effect.succeed([]) }),
    ).pipe(Layer.provideMerge(Database.layerMemory));
    const authority = PeerAuthority.layer.pipe(
      Layer.provideMerge(PeerLinks.layer),
      Layer.provideMerge(ServerSecrets.layer),
      Layer.provideMerge(deps),
    );
    const native = ServerBrowser.layer.pipe(
      Layer.provideMerge(Broker.layer),
      Layer.provideMerge(authority),
      Layer.provideMerge(
        ServerConfig.layerTest(process.cwd(), { prefix: "page-watch-synthetic-" }),
      ),
      Layer.provideMerge(FetchHttpClient.layer),
      Layer.provideMerge(NodeServices.layer),
    );
    const context = yield* Layer.build(native);
    yield* Effect.yieldNow;
    const broker = Context.get(context, Broker.PreviewAutomationBroker);
    const browser = Context.get(context, ServerBrowser.ServerBrowser);
    const handlers = yield* Layer.build(
      ToolAccess.HandlersLayer.layer(WatchHandlers.layer).pipe(
        Layer.provide(Layer.succeedContext(context)),
      ),
    );
    const toolkit = yield* PreviewWatchToolkit.pipe(Effect.provide(handlers));
    const invoke = <A>(
      operation: Parameters<typeof broker.invoke>[0]["operation"],
      input: unknown = {},
      tabId?: PreviewTabId,
    ) => broker.invoke<A>({ scope, operation, input, ...(tabId ? { tabId } : {}) });
    const open = () =>
      invoke<PreviewAutomationStatus>("open", {
        reuseExistingTab: false,
        open: false,
        profileId: "incognito",
      }).pipe(Effect.map((s) => PreviewTabId.make(s.tabId!)));
    const start = (tabId: PreviewTabId, subscription: string) =>
      toolkit.handle("preview_watch_start", { tabId, subscription }).pipe(
        Effect.flatMap(Stream.runCollect),
        Effect.flatMap((r) => decodeStart(r[0]!.result)),
        Effect.provideService(Invocation.McpInvocationContext, scope),
        Effect.provide(context),
      );
    const read = (watch: PreviewWatchStartResult, after = 0, timeoutMs = 0) =>
      toolkit
        .handle("preview_watch_read", {
          threadId,
          watchId: watch.watchId,
          epoch: watch.epoch,
          after,
          timeoutMs,
        })
        .pipe(
          Effect.flatMap(Stream.runCollect),
          Effect.flatMap((r) => decodeRead(r[0]!.result)),
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provide(context),
        );
    const stop = (watch: PreviewWatchStartResult) =>
      toolkit
        .handle("preview_watch_stop", { watchId: watch.watchId, epoch: watch.epoch })
        .pipe(
          Effect.flatMap(Stream.runDrain),
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.provide(context),
        );
    const evaluate = (tabId: PreviewTabId, expression: string) =>
      invoke("evaluate", { expression }, tabId);
    return {
      context,
      broker,
      browser,
      toolkit,
      open,
      start,
      read,
      stop,
      evaluate,
      invoke,
      setShell: (next: typeof shell) => {
        shell = next;
      },
      shell: () => shell,
    };
  });
const integration = it.live.skipIf(!process.env.CHROMERIA_WATCH_TEST_CHROME);

integration(
  "two unrelated pages emit through subscription, native host and MCP retained reads after the creating turn",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const first = yield* f.open();
        const second = yield* f.open();
        const dom = yield* f.start(
          first,
          `(emit) => {
    document.body.innerHTML = '<output>0</output>';
    const listener = event => emit({ kind: 'counter', count: event.detail });
    window.addEventListener('counter', listener); emit({ kind: 'counter', count: 0 });
    return () => { window.removeEventListener('counter', listener); window.cleaned = true; };
  }`,
        );
        const store = yield* f.start(
          second,
          `(emit) => {
    window.store = { selected: [], update(value) { this.selected = value; this.notify(); }, notify() {} };
    window.store.notify = () => emit({ selected: window.store.selected });
    window.store.notify();
    return () => { window.store.notify = () => {}; window.cleaned = true; };
  }`,
        );
        expect(dom.cursor).toBe(0);
        expect(store.cursor).toBe(0);
        expect((yield* f.read(dom)).events).toEqual([
          { sequence: 1, value: { kind: "counter", count: 0 } },
        ]);
        expect((yield* f.read(store)).events).toEqual([{ sequence: 1, value: { selected: [] } }]);
        // These listeners fire after the native request/pending entry is gone and with no live caller run.
        f.setShell({ ...f.shell(), activeRunId: null, status: "idle" });
        yield* settleThread(threadId);
        const browser = f.browser;
        const readMethod = browser.readWatch!;
        const authority = Context.get(f.context, PeerAuthority.PeerAuthority);
        expect(
          (yield* Effect.result(authority.recheckInvocation(scope, "preview", threadId)))._tag,
        ).toBe("Failure");
        const pages = testPages.filter((p) => !p.isClosed());
        const counterPage = yield* Effect.promise(async () => {
          for (const page of pages)
            if (await page.evaluate("document.querySelector('output') !== null")) return page;
          throw new Error("Synthetic counter page missing");
        });
        const storePage = yield* Effect.promise(async () => {
          for (const page of pages)
            if (await page.evaluate("typeof window.store === 'object'")) return page;
          throw new Error("Synthetic store page missing");
        });
        const beforeDom = yield* Effect.promise(() => storePage.content());
        yield* Effect.promise(() =>
          counterPage.evaluate(
            `window.dispatchEvent(new CustomEvent('counter', { detail: 1 })); window.dispatchEvent(new CustomEvent('counter', { detail: 2 }));`,
          ),
        );
        yield* Effect.promise(() => storePage.evaluate(`window.store.update(['record-17']);`));
        expect(yield* Effect.promise(() => storePage.content())).toBe(beforeDom);
        const result = yield* f.read(dom, 1, 30000);
        expect(result.events.map((e) => e.value)).toEqual([
          { kind: "counter", count: 1 },
          { kind: "counter", count: 2 },
        ]);
        const browserCalls = vi.spyOn(f.broker, "invoke");
        expect(yield* f.read(dom, 1)).toEqual(result);
        expect(browserCalls).not.toHaveBeenCalled();
        browserCalls.mockRestore();
        expect((yield* f.read(store, 1, 30000)).events).toEqual([
          { sequence: 2, value: { selected: ["record-17"] } },
        ]);
        expect(
          (yield* readMethod({ threadId, ...store, after: 2, timeoutMs: 0 }, Effect.void)).cursor,
        ).toBe(2);
        f.setShell(liveThreadShell(threadId));
        yield* f.stop(dom);
        expect(
          yield* f.invoke<boolean>("evaluate", { expression: "window.cleaned === true" }, first),
        ).toBe(true);
        expect((yield* Effect.result(f.read(dom)))._tag).toBe("Failure");
      }),
    ),
);

integration(
  "native watches terminate on malformed JSON, overflow, navigation, close, human takeover, login and policy revocation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const assertTerminal = (watch: PreviewWatchStartResult, reason: string) =>
          f.read(watch).pipe(
            Effect.flip,
            Effect.tap((e) => Effect.sync(() => expect(e).toMatchObject({ reason }))),
          );
        const malformed = yield* f.open();
        const bad = yield* f.start(
          malformed,
          `(emit) => { window.bad = () => emit({ value: undefined }); return () => {}; }`,
        );
        yield* f.evaluate(malformed, "window.bad()");
        yield* assertTerminal(bad, "malformed_payload");
        const full = yield* f.open();
        const overflow = yield* f.start(
          full,
          `(emit) => { window.flood = () => { for(let i=0;i<300;i++) emit(i); }; return () => {}; }`,
        );
        yield* f.evaluate(full, "window.flood()");
        yield* assertTerminal(overflow, "overflow");
        yield* f.invoke("close", {}, malformed);
        yield* f.invoke("close", {}, full);
        const navigated = yield* f.open();
        const nav = yield* f.start(navigated, "(emit) => { emit(1); return () => {}; }");
        yield* f.evaluate(navigated, `location.hash = 'new-route';`);
        yield* assertTerminal(nav, "navigation");
        const closed = yield* f.open();
        const close = yield* f.start(closed, "(emit) => () => {}");
        yield* f.invoke("close", {}, closed);
        yield* assertTerminal(close, "closed");
        const human = yield* f.open();
        const takeover = yield* f.start(human, "(emit) => () => {}");
        const viewer = yield* f.browser.attachViewer({
          threadId,
          tabId: human,
          maxWidth: 100,
          maxHeight: 100,
          quality: 50,
          canOperate: true,
        });
        yield* viewer.input({ type: "takeControl" });
        yield* assertTerminal(takeover, "ownership");
        const login = yield* f.open();
        const quarantined = yield* f.start(login, "(emit) => () => {}");
        yield* f
          .invoke(
            "fillCredential",
            { item: "synthetic-missing-item", field: "username", selector: "input" },
            login,
          )
          .pipe(Effect.result);
        yield* assertTerminal(quarantined, "login");
        const revokedTab = yield* f.open();
        const revoked = yield* f.start(
          revokedTab,
          "(emit) => { window.ping = () => emit(1); return () => {}; }",
        );
        const settings = Context.get(f.context, Settings.ServerSettingsService);
        yield* settings.updateSettings({ enableAgentBrowserAccess: false });
        yield* assertTerminal(revoked, "revoked");
      }),
    ),
);

integration(
  "outside retained reads fail closed by exact watch/project/thread and session revocation without browser calls",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture();
        const tabId = yield* f.open();
        const watch = yield* f.start(
          tabId,
          "(emit) => { emit({ baseline: true }); return () => {}; }",
        );
        const sessions = Context.get(f.context, AuthSessions.AuthSessionRepository);
        const sessionId = AuthSessionId.make("synthetic-outside-reader");
        const now = yield* DateTime.now;
        yield* sessions.create({
          sessionId,
          subject: "synthetic-reader",
          scopes: ["orchestration:read"],
          method: "bearer-access-token",
          client: {
            label: null,
            ipAddress: null,
            userAgent: null,
            deviceType: "bot",
            os: null,
            browser: null,
          },
          issuedAt: now,
          expiresAt: DateTime.add(now, { hours: 1 }),
        });
        const clientScope = (
          projectId: ProjectId | undefined,
          operations: readonly AuthMcpOperation[] = ["read"],
        ): Invocation.McpInvocationScope => ({
          ...scope,
          thread: undefined,
          capabilities: new Set(["orchestration"]),
          client: {
            sessionId,
            label: "synthetic-reader",
            access: "read-only",
            limits: { ...(projectId ? { projectId } : {}), operations },
          },
        });
        const outsideRead = (
          client: Invocation.McpInvocationScope,
          host = threadId,
          watchId = watch.watchId,
          epoch = watch.epoch,
          after = 0,
          timeoutMs = 0,
        ) =>
          f.toolkit
            .handle("preview_watch_read", { threadId: host, watchId, epoch, after, timeoutMs })
            .pipe(
              Effect.flatMap(Stream.runCollect),
              Effect.provideService(Invocation.McpInvocationContext, client),
              Effect.provide(f.context),
            );
        const approved = clientScope(f.shell().projectId);
        const browserCalls = vi.spyOn(f.broker, "invoke");
        expect((yield* outsideRead(approved))[0]?.result).toMatchObject({
          cursor: 1,
          events: [{ sequence: 1, value: { baseline: true } }],
        });
        expect((yield* Effect.result(outsideRead(clientScope(undefined))))._tag).toBe("Failure");
        expect(
          (yield* Effect.result(outsideRead(clientScope(ProjectId.make("other-project")))))._tag,
        ).toBe("Failure");
        expect((yield* Effect.result(outsideRead(approved, threadId, "wrong-watch")))._tag).toBe(
          "Failure",
        );
        expect(
          (yield* Effect.result(outsideRead(approved, threadId, watch.watchId, "old-process")))
            ._tag,
        ).toBe("Failure");
        for (const name of ["preview_watch_start", "preview_watch_stop"] as const) {
          const args =
            name === "preview_watch_start"
              ? { tabId, subscription: "(emit) => () => {}" }
              : { watchId: watch.watchId, epoch: watch.epoch };
          const denied = yield* f.toolkit
            .handle(name, args)
            .pipe(
              Effect.flatMap(Stream.runCollect),
              Effect.provideService(Invocation.McpInvocationContext, approved),
              Effect.provide(f.context),
              Effect.result,
            );
          expect(denied._tag).toBe("Failure");
        }
        expect(browserCalls).not.toHaveBeenCalled();
        browserCalls.mockRestore();
        yield* sessions.revoke({ sessionId, revokedAt: now });
        expect((yield* Effect.result(outsideRead(approved)))._tag).toBe("Failure");
        f.setShell({ ...f.shell(), deletedAt: now });
        expect((yield* Effect.result(f.read(watch)))._tag).toBe("Failure");
      }),
    ),
);

integration("watch continuation retains its original mode ceiling after turn completion", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = yield* fixture();
      f.setShell(liveThreadShell(threadId, { runtimeMode: "auto-accept-edits" }));
      const tabId = yield* f.open();
      const watch = yield* f.start(tabId, "(emit) => { emit(1); return () => {}; }");
      f.setShell(liveThreadShell(threadId, { runtimeMode: "full-access", activeRunId: null }));
      const result = yield* Effect.result(f.read(watch));
      expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "revoked" } });
    }),
  ),
);
