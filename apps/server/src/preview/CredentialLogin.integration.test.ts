import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as PreviewAgentActivity from "./PreviewAgentActivity.ts";
import * as ServerSettings from "../serverSettings.ts";
// @effect-diagnostics nodeBuiltinImport:off - Finds Playwright's installed headless shell for a real Chromium run.
import * as NodeEvents from "node:events";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  PreviewCredentialError,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationSnapshot,
  type PreviewAutomationStatus,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";
import type { BrowserContext, Page } from "playwright-core";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as Broker from "../mcp/PreviewAutomationBroker.ts";
import { CREDENTIAL_AUDIT_FILE } from "./CredentialAudit.ts";
import { testChromium } from "./ChromeForTesting.testkit.ts";
import * as CredentialVault from "./CredentialVault.ts";
import * as DesktopChannel from "./DesktopBrowserChannel.ts";
import * as Manager from "./Manager.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import { settlementLayer } from "./BrowserSettlement.testkit.ts";
import { ServerBrowserContexts } from "./ServerBrowserContexts.ts";

const executable = testChromium;
// Like the repository's other real-browser tests, which launch Playwright without
// Chrome's sandbox: CI's Ubuntu runner blocks the unprivileged user namespaces it
// needs (AppArmor). This is the server's own operator opt-out.
vi.stubEnv("T3CODE_SERVER_BROWSER_SANDBOX", "0");

// Fake vault values that URL, form and HTML encodings all change.
const PASSWORD = `LOGIN-Password+&=#?% 42<"'>`;
const USERNAME = "LOGIN-user@example.test";
const CODE = "826493";
const SECRETS = [
  PASSWORD,
  USERNAME,
  CODE,
  encodeURIComponent(PASSWORD),
  // As JSON.stringify writes it, since the password holds a quote.
  JSON.stringify(PASSWORD).slice(1, -1),
];
/** Also decoded: Chromium escapes a URL its own way, such as ' as %27. */
const expectNoSecret = (value: unknown) => {
  const text = JSON.stringify(value);
  const decoded = text.replace(/(?:%[0-9a-f]{2})+/gi, (run) => {
    try {
      return decodeURIComponent(run);
    } catch {
      return run;
    }
  });
  for (const secret of SECRETS) {
    expect(text).not.toContain(secret);
    expect(decoded).not.toContain(secret);
  }
};

const LOGIN_PAGE = `<title>Login</title>
<form method="get" action="/done"><label>Password<input name="pw" id="pw" type="password"></label><button id="submit">Submit</button></form>
<label>User<input id="user"></label><label>Code<input id="otp"></label>
<button id="popup" onclick="window.open('/child?pw='+encodeURIComponent(pw.value))">Open popup</button>
<button id="plain-popup" onclick="window.open('/child')">Open blank popup</button>
<script>for (const id of ['pw','user','otp']) document.getElementById(id).addEventListener('input', e => { document.title = e.target.value; });</script>`;

const vault = {
  password: PASSWORD,
  uris: ["example.test"] as ReadonlyArray<string>,
  gate: undefined as Promise<void> | undefined,
  entered: undefined as (() => void) | undefined,
  fail: false,
};
const desktop = {
  enabled: false,
  page: undefined as Page | undefined,
  disconnected: undefined as (() => void) | undefined,
  bus: new NodeEvents.EventEmitter(),
};
const contexts: Array<BrowserContext> = [];
const posts: Array<{ readonly path: string; readonly body: string }> = [];

const routedContextFor = ServerBrowserContexts.prototype.contextFor;
vi.spyOn(ServerBrowserContexts.prototype, "contextFor").mockImplementation(async function (
  this: ServerBrowserContexts,
  ...args
) {
  const context = await routedContextFor.apply(this, args);
  if (!contexts.includes(context)) {
    contexts.push(context);
    await context.route("https://**/*", (route) => {
      const url = new URL(route.request().url());
      const page = (body: string) => route.fulfill({ contentType: "text/html", body });
      // A site whose passkey step fails after the password, then offers a one-time code.
      if (url.pathname === "/passkey")
        return page(
          "<title>Sign in</title><form method=post action=/passkey-error><label>Password<input type=password name=password></label><button>Continue</button></form>",
        );
      if (route.request().method() === "POST")
        posts.push({ path: url.pathname, body: route.request().postData() ?? "" });
      if (url.pathname === "/passkey-error") {
        return page(
          "<title>Passkey</title><p role=alert>This passkey can't be used on this device.</p><a href=/otp>Try another way</a>",
        );
      }
      if (url.pathname === "/otp")
        return page(
          "<title>Authenticator</title><form method=post action=/signed><label>Code<input name=code autocomplete=one-time-code></label><button>Verify</button></form>",
        );
      if (url.pathname === "/step1")
        return page(
          "<form action=/step2><input type=email name=email><button>Continue</button></form>",
        );
      if (url.pathname === "/step2")
        return page(
          "<form action=/step3><input type=password name=password><button>Continue</button></form>",
        );
      if (url.pathname === "/step3")
        return page(
          "<form action=/signed><input name=code autocomplete=one-time-code><button>Sign in</button></form>",
        );
      if (url.pathname === "/signed")
        return route.fulfill({
          contentType: "text/html",
          // A remembered sign-in: a cookie with an expiry outlives the context.
          headers: { "set-cookie": "signed=yes; Secure; Path=/; SameSite=Lax; Max-Age=3600" },
          body: "<title>Account</title><h1>Signed in</h1>",
        });
      if (url.pathname === "/account")
        return page(
          route.request().headers().cookie?.includes("signed=yes")
            ? "<h1>Account signed in</h1>"
            : "<h1>Not signed in</h1>",
        );
      if (url.pathname === "/child" || url.pathname === "/done")
        return page("<title>Child</title><h1>Child</h1>");
      return page(LOGIN_PAGE);
    });
  }
  return context;
});
// A desktop tab whose debugger connection can drop while the page lives on.
vi.spyOn(ServerBrowserContexts.prototype, "connectDesktopPage").mockImplementation(async function (
  this: ServerBrowserContexts,
) {
  if (!desktop.page) {
    const context = await this.contextFor("default");
    desktop.page = await context.newPage();
    await desktop.page.goto("https://login.example.test/");
  }
  return {
    page: desktop.page,
    browser: { close: async () => desktop.disconnected?.() },
  } as unknown as Awaited<ReturnType<ServerBrowserContexts["connectDesktopPage"]>>;
});

const desktopEvents = (name: string) =>
  Stream.callback<{ threadId: string; tabId: string }>((queue) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        const listener = (event: { threadId: string; tabId: string }) =>
          Queue.offerUnsafe(queue, event);
        desktop.bus.on(name, listener);
        return listener;
      }),
      (listener) => Effect.sync(() => desktop.bus.off(name, listener)),
    ),
  );

const scope = {
  environmentId: EnvironmentId.make("login-environment"),
  thread: {
    threadId: ThreadId.make("login-thread"),
    providerSessionId: "login-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  requestNamespace: "login",
  capabilities: new Set(["preview"] as const),
  issuedAt: 1,
};

const otherAgent = {
  ...scope,
  thread: {
    threadId: ThreadId.make("other-login-thread"),
    providerSessionId: "other-login-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  requestNamespace: "other-login",
};

const dependencies = Layer.mergeAll(
  settlementLayer,
  Broker.layer,
  Manager.layer,
  Layer.succeed(CredentialVault.CredentialVault, {
    uris: () => Effect.sync(() => vault.uris),
    read: (_item, field) =>
      Effect.tryPromise({
        try: async () => {
          vault.entered?.();
          await vault.gate;
          if (vault.fail) throw new Error("fake vault unavailable");
          return field === "password" ? vault.password : field === "totp" ? CODE : USERNAME;
        },
        catch: () => new PreviewCredentialError({ reason: "vaultUnavailable" }),
      }),
  }),
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: Effect.succeed(scope.environmentId),
    getDescriptor: Effect.die("unused descriptor"),
  }),
  Layer.succeed(PreviewBrowser.PreviewBrowser, {
    executable: Effect.sync(() => executable!),
    installed: Effect.sync(() => Option.fromNullishOr(executable)),
  }),
  Layer.succeed(DesktopChannel.DesktopBrowserChannel, {
    available: true,
    isShown: () => true,
    awaitShown: () => Effect.succeed(true),
    awaitAttached: () => Effect.sync(() => desktop.enabled),
    loadFailure: () => undefined,
    detached: desktopEvents("detach"),
    attached: desktopEvents("attach"),
    isAttached: () => Effect.sync(() => desktop.enabled),
    endpoint: () => Effect.succeed("ws://desktop-boundary"),
    pointer: () => Effect.void,
    turn: () => Effect.void,
  }),
).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-credential-login-" })),
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);
const layer = ServerBrowser.layer.pipe(
  Layer.provide(PreviewAgentActivity.layer),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(dependencies),
);

type BrokerService = Broker.PreviewAutomationBroker["Service"];
const invoke =
  (broker: BrokerService) =>
  <A = unknown>(tabId: PreviewTabId, operation: string, input: object = {}) =>
    broker.invoke<A>({
      scope,
      tabId,
      operation: operation as Parameters<BrokerService["invoke"]>[0]["operation"],
      input,
    });
const openTab = (broker: BrokerService, url = "https://login.example.test/") =>
  broker
    .invoke<PreviewAutomationStatus>({
      scope,
      operation: "open",
      input: { url, profileId: "default", show: false, reuseExistingTab: false },
    })
    .pipe(Effect.map((status) => PreviewTabId.make(status.tabId!)));
const fillPassword = (broker: BrokerService, tabId: PreviewTabId, locator = "#pw") =>
  invoke(broker)<{ site: string }>(tabId, "fillCredential", {
    item: "Example",
    field: "password",
    locator,
  });
/** The next tab the Manager opens, other than `besides`. */
const nextOpened = (besides: string) =>
  Effect.gen(function* () {
    const manager = yield* Manager.PreviewManager;
    const opened = Promise.withResolvers<string>();
    yield* manager.events.pipe(
      Stream.filter((event) => event.type === "opened" && event.tabId !== besides),
      Stream.take(1),
      Stream.runForEach((event) => Effect.sync(() => opened.resolve(event.tabId))),
      Effect.forkScoped,
    );
    return Effect.promise(() => opened.promise).pipe(Effect.map(PreviewTabId.make));
  });
const pageAt = (path: string) =>
  contexts
    .at(-1)!
    .pages()
    .find((page) => new URL(page.url()).pathname === path)!;
const withBrowser = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | Broker.PreviewAutomationBroker
    | Manager.PreviewManager
    | ServerBrowser.ServerBrowser
    | Scope.Scope
  >,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* ServerBrowser.ServerBrowser;
      yield* Effect.yieldNow;
      return yield* effect;
    }),
  ).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        vault.gate = undefined;
        vault.entered = undefined;
        vault.fail = false;
        vault.password = PASSWORD;
        vault.uris = ["example.test"];
        posts.length = 0;
        desktop.enabled = false;
        desktop.page = undefined;
        desktop.disconnected = undefined;
      }),
    ),
    Effect.provide(layer),
  );

describe.skipIf(!executable)("credential login in a real browser", () => {
  it.live("a sign-in stays readable from the password through a one-time code step", () =>
    withBrowser(
      Effect.gen(function* () {
        const broker = yield* Broker.PreviewAutomationBroker;
        const act = invoke(broker);
        const tabId = yield* openTab(broker, "https://login.example.test/passkey");
        // Another agent's tab in the same browser profile.
        const other = PreviewTabId.make(
          (yield* broker.invoke<PreviewAutomationStatus>({
            scope: otherAgent,
            operation: "open",
            input: {
              url: "https://login.example.test/child",
              profileId: "default",
              show: false,
              reuseExistingTab: false,
            },
          })).tabId!,
        );
        const filled = yield* fillPassword(broker, tabId, "input[type=password]");
        expect(filled).toEqual({
          filled: true,
          item: "Example",
          field: "password",
          site: "example.test",
          tabId,
        });
        // Readable at once, with the filled password masked.
        const before = yield* act<PreviewAutomationSnapshot>(tabId, "snapshot");
        expect(before.visibleText).toContain("Password");
        expectNoSecret({ ...before, screenshot: undefined });
        const otherRead = yield* broker.invoke<PreviewAutomationSnapshot>({
          scope: otherAgent,
          tabId: other,
          operation: "snapshot",
          input: {},
        });
        expect(otherRead.visibleText).toContain("Child");

        yield* act(tabId, "click", { locator: "button" });
        yield* act(tabId, "waitFor", { text: "Try another way" });
        const prompt = yield* act<PreviewAutomationSnapshot>(tabId, "snapshot");
        expect(prompt.visibleText).toContain("This passkey can't be used on this device.");
        // One password submission, carrying the vault's password.
        expect(posts).toEqual([
          { path: "/passkey-error", body: new URLSearchParams({ password: PASSWORD }).toString() },
        ]);
        const status = yield* act<PreviewAutomationStatus>(tabId, "status");
        expect(status).toMatchObject({
          url: "https://login.example.test/passkey-error",
          title: "Passkey",
        });

        yield* act(tabId, "click", { locator: "text=Try another way" });
        yield* act(tabId, "waitFor", { locator: "input[autocomplete=one-time-code]" });
        const code = yield* act(tabId, "fillCredential", {
          item: "Example",
          field: "totp",
          locator: "input[autocomplete=one-time-code]",
        });
        expectNoSecret(code);
        yield* act(tabId, "click", { locator: "button" });
        yield* act(tabId, "waitFor", { text: "Signed in", urlIncludes: "/signed" });
        expect(yield* act<PreviewAutomationStatus>(tabId, "status")).toMatchObject({
          tabId,
          title: "Account",
        });
        expect(posts.map((post) => post.path)).toEqual(["/passkey-error", "/signed"]);
        expect(posts[1]!.body).toBe(`code=${CODE}`);
        const manager = yield* Manager.PreviewManager;
        expect(
          (yield* manager.list({ threadId: scope.thread.threadId })).sessions.map(
            (session) => session.tabId,
          ),
        ).toEqual([tabId]);
      }),
    ),
  );

  it.live("a desktop-hosted tab refuses a fill and stays readable", () => {
    desktop.enabled = true;
    return withBrowser(
      Effect.gen(function* () {
        const broker = yield* Broker.PreviewAutomationBroker;
        // Only a person's tab is the desktop's own; an agent's would run in the server browser.
        const opened = yield* (yield* Manager.PreviewManager).open({
          threadId: scope.thread.threadId,
          url: "https://login.example.test/",
          runtime: "server",
        });
        const tabId = PreviewTabId.make(opened.tabId);
        expect(yield* fillPassword(broker, tabId).pipe(Effect.flip)).toMatchObject({
          _tag: "PreviewCredentialError",
          reason: "serverBrowserOnly",
        });
        expect(yield* Effect.promise(() => desktop.page!.inputValue("#pw"))).toBe("");
        expect(yield* invoke(broker)(tabId, "evaluate", { expression: "1" })).toBe(1);
      }),
    );
  });

  it.live("a later fill names the vault host, never a live origin that carries a value", () =>
    withBrowser(
      Effect.gen(function* () {
        const broker = yield* Broker.PreviewAutomationBroker;
        vault.password = "login-secret-42";
        const tabId = yield* openTab(broker);
        const page = pageAt("/");
        // The page sends itself to a host named after the typed password.
        yield* invoke(broker)(tabId, "evaluate", {
          expression:
            "document.getElementById('pw').addEventListener('input', e => { location.href = 'https://' + e.target.value + '.example.test/' })",
        });
        yield* fillPassword(broker, tabId).pipe(Effect.ignore);
        yield* Effect.promise(() => page.waitForURL("https://login-secret-42.example.test/"));
        const result = yield* invoke(broker)<{ site: string }>(tabId, "fillCredential", {
          item: "Example",
          field: "username",
          locator: "#user",
        });
        expect(result).toMatchObject({ filled: true, site: "example.test" });
        expect(JSON.stringify(result)).not.toContain("login-secret-42");
        // A refusal after the value went in names no origin either.
        vault.uris = ["other.test"];
        const refused = yield* invoke(broker)<void>(tabId, "fillCredential", {
          item: "Example",
          field: "username",
          locator: "#user",
        }).pipe(Effect.flip);
        expect(refused).toMatchObject({ reason: "originMismatch" });
        expect(JSON.stringify(refused)).not.toContain("login-secret-42");
      }),
    ),
  );

  it.live("the MCP result and audit line name the vault host, never the live origin", () => {
    const mcpLayer = McpHttpServer.layerPreviewToolkit.pipe(
      Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
      Layer.provideMerge(McpToolAccessTestkit.liveThreadsLayer),
      Layer.provideMerge(layer),
    );
    const client = McpSchema.McpServerClient.of({
      clientId: 1,
      clientCapabilities: {},
      clientInfo: { name: "login-test", version: "1" },
      protocolVersion: "2025-06-18",
      initializePayload: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "login-test", version: "1" },
      },
      getClient: Effect.die("unused"),
    });
    vault.password = "login-secret-43";
    return Effect.scoped(
      Effect.gen(function* () {
        yield* ServerBrowser.ServerBrowser;
        yield* Effect.yieldNow;
        const broker = yield* Broker.PreviewAutomationBroker;
        const server = yield* McpServer.McpServer;
        const tabId = yield* openTab(broker);
        const page = pageAt("/");
        yield* invoke(broker)(tabId, "evaluate", {
          expression:
            "document.getElementById('pw').addEventListener('input', e => { location.href = 'https://' + e.target.value + '.example.test/' })",
        });
        yield* fillPassword(broker, tabId).pipe(Effect.ignore);
        yield* Effect.promise(() => page.waitForURL("https://login-secret-43.example.test/"));
        const result = yield* server
          .callTool({
            name: "preview_fill_credential",
            arguments: { tabId, item: "Example", field: "username", locator: "#user" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, client),
          );
        expect(result.isError).toBe(false);
        expect(JSON.stringify(result)).not.toContain("login-secret-43");
        const config = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const audit = yield* fileSystem.readFileString(
          NodePath.join(config.stateDir, CREDENTIAL_AUDIT_FILE),
        );
        expect(audit).toContain('"site":"example.test"');
        expect(audit).not.toContain("login-secret-43");
      }),
    ).pipe(
      Effect.ensuring(Effect.sync(() => (vault.password = PASSWORD))),
      Effect.provide(mcpLayer),
    );
  });

  it.live("an overlapping fill's refusal writes no page origin to the audit", () => {
    const mcpLayer = McpHttpServer.layerPreviewToolkit.pipe(
      Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
      Layer.provideMerge(McpToolAccessTestkit.liveThreadsLayer),
      Layer.provideMerge(layer),
    );
    const client = McpSchema.McpServerClient.of({
      clientId: 1,
      clientCapabilities: {},
      clientInfo: { name: "login-test", version: "1" },
      protocolVersion: "2025-06-18",
      initializePayload: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "login-test", version: "1" },
      },
      getClient: Effect.die("unused"),
    });
    vault.password = "login-secret-44";
    return Effect.scoped(
      Effect.gen(function* () {
        yield* ServerBrowser.ServerBrowser;
        yield* Effect.yieldNow;
        const broker = yield* Broker.PreviewAutomationBroker;
        const server = yield* McpServer.McpServer;
        const tabId = yield* openTab(broker);
        const opener = pageAt("/");
        const popupOpened = yield* nextOpened(tabId);
        yield* invoke(broker)(tabId, "click", { locator: "#plain-popup" });
        const popup = yield* popupOpened;
        // The popup's input sends its opener to a host named after the typed value.
        yield* invoke(broker)(popup, "evaluate", {
          expression:
            "document.body.insertAdjacentHTML('beforeend', '<input id=pw>'); document.getElementById('pw').addEventListener('input', e => { opener.location.href = 'https://' + e.target.value + '.example.test/' })",
        });
        const gate = Promise.withResolvers<void>();
        const entered = Promise.withResolvers<void>();
        vault.gate = gate.promise;
        vault.entered = entered.resolve;
        const first = yield* server
          .callTool({
            name: "preview_fill_credential",
            arguments: { tabId, item: "Example", field: "username", locator: "#user" },
          })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, client),
            Effect.forkScoped,
          );
        yield* Effect.promise(() => entered.promise);
        vault.gate = undefined;
        vault.entered = undefined;
        yield* fillPassword(broker, popup);
        yield* Effect.promise(() => opener.waitForURL("https://login-secret-44.example.test/"));
        gate.resolve();
        const result = yield* Fiber.join(first);
        expect(result.isError).toBe(true);
        expect(JSON.stringify(result)).not.toContain("login-secret-44");
        const config = yield* ServerConfig.ServerConfig;
        const fileSystem = yield* FileSystem.FileSystem;
        const audit = yield* fileSystem.readFileString(
          NodePath.join(config.stateDir, CREDENTIAL_AUDIT_FILE),
        );
        expect(audit).toContain('"result":"originMismatch"');
        expect(audit).not.toContain("login-secret-44");
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          vault.password = PASSWORD;
          vault.gate = undefined;
          vault.entered = undefined;
        }),
      ),
      Effect.provide(mcpLayer),
    );
  });
});
