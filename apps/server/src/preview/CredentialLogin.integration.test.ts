// @effect-diagnostics nodeBuiltinImport:off - Finds Playwright's installed headless shell for a real Chromium run.
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  PreviewCredentialError,
  PreviewTabId,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationError,
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
import { chromium, type BrowserContext, type Page } from "playwright-core";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as Broker from "../mcp/PreviewAutomationBroker.ts";
import { CREDENTIAL_AUDIT_FILE } from "./CredentialAudit.ts";
import * as CredentialVault from "./CredentialVault.ts";
import * as DesktopChannel from "./DesktopBrowserChannel.ts";
import * as Manager from "./Manager.ts";
import * as PreviewBrowser from "./PreviewBrowser.ts";
import * as ServerBrowser from "./ServerBrowser.ts";
import { ServerBrowserContexts } from "./ServerBrowserContexts.ts";

/** Playwright's headless shell, which CI installs, beside the Chromium it would launch. */
const headlessShell = () => {
  if (process.env.CHROMERIA_TEST_CHROMIUM) return process.env.CHROMERIA_TEST_CHROMIUM;
  let root = chromium.executablePath();
  while (root !== NodePath.dirname(root) && !/^chromium-\d+$/.test(NodePath.basename(root)))
    root = NodePath.dirname(root);
  const revision = NodePath.basename(root).slice("chromium-".length);
  const shellRoot = NodePath.join(NodePath.dirname(root), `chromium_headless_shell-${revision}`);
  if (!NodeFS.existsSync(shellRoot)) return undefined;
  for (const platform of NodeFS.readdirSync(shellRoot)) {
    for (const name of ["chrome-headless-shell", "chrome-headless-shell.exe"]) {
      const candidate = NodePath.join(shellRoot, platform, name);
      if (NodeFS.existsSync(candidate)) return candidate;
    }
  }
  return undefined;
};
const executable = headlessShell();
// Like the repository's other real-browser tests, which launch Playwright without
// Chrome's sandbox: CI's Ubuntu runner blocks the unprivileged user namespaces it
// needs (AppArmor). This is the server's own operator opt-out.
vi.stubEnv("T3CODE_SERVER_BROWSER_SANDBOX", "0");

// Fake vault values that URL, form and HTML encodings all change.
const PASSWORD = `LOGIN-Password+&=#?% 42<"'>`;
const USERNAME = "LOGIN-user@example.test";
const CODE = "826493";
const SECRETS = [PASSWORD, USERNAME, CODE, encodeURIComponent(PASSWORD)];
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

const dependencies = Layer.mergeAll(
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
    awaitAttached: () => Effect.sync(() => desktop.enabled),
    detached: desktopEvents("detach"),
    attached: desktopEvents("attach"),
    isAttached: () => Effect.sync(() => desktop.enabled),
    endpoint: () => Effect.succeed("ws://desktop-boundary"),
    pointer: () => Effect.void,
  }),
).pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-credential-login-" })),
  Layer.provideMerge(NodeServices.layer),
);
const layer = ServerBrowser.layer.pipe(Layer.provideMerge(dependencies));

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
const expectLogin = (error: PreviewAutomationError) =>
  expect(error).toMatchObject({ _tag: "PreviewCredentialError", reason: "loginInProgress" });
const refusedRead = (broker: BrokerService, tabId: PreviewTabId) =>
  Effect.gen(function* () {
    for (const [operation, input] of [
      ["snapshot", {}],
      ["evaluate", { expression: "document.title" }],
    ] as const)
      expectLogin(yield* invoke(broker)<void>(tabId, operation, input).pipe(Effect.flip));
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
    Broker.PreviewAutomationBroker | Manager.PreviewManager | Scope.Scope
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
        desktop.enabled = false;
        desktop.page = undefined;
        desktop.disconnected = undefined;
      }),
    ),
    Effect.provide(layer),
  );

describe.skipIf(!executable)("credential login in a real browser", () => {
  it.live(
    "a status read that began before a fill returns without the filled title",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const page = pageAt("/");
          const entered = Promise.withResolvers<void>();
          const gate = Promise.withResolvers<void>();
          const title = page.title.bind(page);
          let held = false;
          vi.spyOn(page, "title").mockImplementation(async () => {
            if (!held) {
              held = true;
              entered.resolve();
              await gate.promise;
            }
            return title();
          });
          const status = yield* invoke(broker)<PreviewAutomationStatus>(tabId, "status").pipe(
            Effect.forkScoped,
          );
          yield* Effect.promise(() => entered.promise);
          yield* fillPassword(broker, tabId);
          // The page copied the password into its title.
          expect(yield* Effect.promise(() => title())).toBe(PASSWORD);
          gate.resolve();
          const result = yield* Fiber.join(status);
          expect(result).toMatchObject({ title: null, url: null, loginInProgress: true });
          expectNoSecret(result);
        }),
      ),
    60_000,
  );

  it.live(
    "a failed follow-up fill keeps a popup opened during it in the login",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const page = pageAt("/");
          yield* fillPassword(broker, tabId);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          vault.gate = gate.promise;
          vault.entered = entered.resolve;
          vault.fail = true;
          const retry = yield* fillPassword(broker, tabId).pipe(Effect.flip, Effect.forkScoped);
          yield* Effect.promise(() => entered.promise);
          const popupOpened = yield* nextOpened(tabId);
          yield* Effect.promise(() => page.evaluate(`window.open("/child")`));
          const popup = yield* popupOpened;
          yield* refusedRead(broker, popup);
          gate.resolve();
          expect(yield* Fiber.join(retry)).toMatchObject({ reason: "vaultUnavailable" });
          yield* refusedRead(broker, tabId);
          yield* refusedRead(broker, popup);
        }),
      ),
    60_000,
  );

  it.live(
    "a failed first fill keeps its group in the login after a related fill succeeds",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const popupOpened = yield* nextOpened(tabId);
          yield* invoke(broker)(tabId, "click", { locator: "#plain-popup" });
          const popup = yield* popupOpened;
          yield* invoke(broker)(popup, "evaluate", {
            expression:
              "document.body.insertAdjacentHTML('beforeend', '<input id=pw type=password>')",
          });
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          vault.gate = gate.promise;
          vault.entered = entered.resolve;
          const first = yield* fillPassword(broker, tabId).pipe(Effect.flip, Effect.forkScoped);
          yield* Effect.promise(() => entered.promise);
          // The popup's own fill reads the vault without waiting, and succeeds.
          vault.gate = undefined;
          vault.entered = undefined;
          expect((yield* fillPassword(broker, popup)).site).toBe("example.test");
          vault.fail = true;
          gate.resolve();
          expect(yield* Fiber.join(first)).toMatchObject({ reason: "vaultUnavailable" });
          yield* refusedRead(broker, tabId);
          yield* refusedRead(broker, popup);
          const status = yield* invoke(broker)<PreviewAutomationStatus>(popup, "status");
          expect(status).toMatchObject({ title: null, loginInProgress: true });
          expectNoSecret(status);
        }),
      ),
    60_000,
  );

  it.live(
    "a popup still being set up when a fill begins joins the login",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const opener = pageAt("/");
          const context = opener.context();
          const attach = context.newCDPSession.bind(context);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          let held = false;
          // Hold the popup's setup after its session exists, before it is published.
          vi.spyOn(context, "newCDPSession").mockImplementation(async (target) => {
            const session = await attach(target);
            if (target !== opener && "mainFrame" in target && !held) {
              held = true;
              const resize = target.setViewportSize.bind(target);
              vi.spyOn(target, "setViewportSize").mockImplementation(async (size) => {
                entered.resolve();
                await gate.promise;
                return resize(size);
              });
            }
            return session;
          });
          const popupOpened = yield* nextOpened(tabId);
          yield* invoke(broker)(tabId, "click", { locator: "#plain-popup" });
          const popup = yield* popupOpened;
          yield* Effect.promise(() => entered.promise);
          yield* fillPassword(broker, tabId);
          gate.resolve();
          yield* refusedRead(broker, popup);
        }),
      ),
    60_000,
  );

  it.live(
    "a tab opened in the same profile during a login starts in it",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          yield* fillPassword(broker, tabId);
          const other = yield* openTab(broker, "https://login.example.test/account");
          yield* refusedRead(broker, other);
          const ended = yield* invoke(broker)<{ closed: Array<string> }>(tabId, "endLogin");
          expect([...ended.closed].toSorted()).toEqual([tabId, other].toSorted());
          const fresh = yield* openTab(broker, "https://login.example.test/account");
          expect(yield* invoke(broker)(fresh, "evaluate", { expression: "1" })).toBe(1);
        }),
      ),
    60_000,
  );

  it.live(
    "a popup still attaching when its filled opener closes stays in the login",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const opener = pageAt("/");
          yield* fillPassword(broker, tabId);
          const context = opener.context();
          const attach = context.newCDPSession.bind(context);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          let held = false;
          vi.spyOn(context, "newCDPSession").mockImplementation(async (target) => {
            // The popup's own page session, not a frame session of the opener.
            if (target !== opener && "mainFrame" in target && !held) {
              held = true;
              entered.resolve();
              await gate.promise;
            }
            return attach(target);
          });
          const popupOpened = yield* nextOpened(tabId);
          yield* invoke(broker)(tabId, "click", { locator: "#popup" });
          const popup = yield* popupOpened;
          yield* Effect.promise(() => entered.promise);
          // The opener's session closes before the popup finishes attaching.
          const manager = yield* Manager.PreviewManager;
          const closed = Promise.withResolvers<void>();
          yield* manager.events.pipe(
            Stream.filter((event) => event.type === "closed" && event.tabId === tabId),
            Stream.take(1),
            Stream.runForEach(() => Effect.sync(() => closed.resolve())),
            Effect.forkScoped,
          );
          yield* invoke(broker)(tabId, "close");
          yield* Effect.promise(() => closed.promise);
          gate.resolve();
          yield* refusedRead(broker, popup);
          expectNoSecret(yield* invoke(broker)(popup, "status"));
        }),
      ),
    60_000,
  );

  it.live(
    "a desktop-hosted tab refuses a fill and stays readable",
    () => {
      desktop.enabled = true;
      return withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const opened = yield* broker.invoke<PreviewAutomationStatus>({
            scope,
            operation: "open",
            input: { url: "https://login.example.test/", show: false, reuseExistingTab: false },
          });
          const tabId = PreviewTabId.make(opened.tabId!);
          expect(yield* fillPassword(broker, tabId).pipe(Effect.flip)).toMatchObject({
            _tag: "PreviewCredentialError",
            reason: "serverBrowserOnly",
          });
          expect(yield* Effect.promise(() => desktop.page!.inputValue("#pw"))).toBe("");
          expect(yield* invoke(broker)(tabId, "evaluate", { expression: "1" })).toBe(1);
        }),
      );
    },
    60_000,
  );

  it.live(
    "ending a login closes a popup that was still attaching",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const opener = pageAt("/");
          yield* fillPassword(broker, tabId);
          const context = opener.context();
          const attach = context.newCDPSession.bind(context);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          let held = false;
          vi.spyOn(context, "newCDPSession").mockImplementation(async (target) => {
            if (target !== opener && "mainFrame" in target && !held) {
              held = true;
              entered.resolve();
              await gate.promise;
            }
            return attach(target);
          });
          const popupOpened = yield* nextOpened(tabId);
          // The popup's URL carries the filled password.
          yield* invoke(broker)(tabId, "click", { locator: "#popup" });
          const popup = yield* popupOpened;
          yield* Effect.promise(() => entered.promise);
          const popupPage = context.pages().find((page) => page !== opener)!;
          // End the login while the popup is still attaching, then let it finish.
          const ending = yield* invoke(broker)<{ closed: Array<string> }>(tabId, "endLogin").pipe(
            Effect.forkScoped,
          );
          gate.resolve();
          expect(yield* Fiber.join(ending)).toEqual({ closed: [tabId] });
          expect(popupPage.isClosed()).toBe(true);
          // The popup's page closed with its context. Its session, if read again, opens a
          // blank tab in the profile's fresh context.
          const read = yield* invoke(broker)<string>(popup, "evaluate", {
            expression: "location.href",
          }).pipe(Effect.result);
          expectNoSecret(read);
          // The profile is free again once nothing of the login is left.
          const fresh = yield* openTab(broker);
          expect(yield* invoke(broker)(fresh, "evaluate", { expression: "1" })).toBe(1);
        }),
      ),
    60_000,
  );

  it.live(
    "the login lasts until the profile's context has fully closed",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const page = pageAt("/");
          // The page answers any same-origin tab with its password input.
          yield* invoke(broker)(tabId, "evaluate", {
            expression:
              "globalThis.channel = new BroadcastChannel('login'); channel.onmessage = () => channel.postMessage(document.getElementById('pw').value)",
          });
          yield* fillPassword(broker, tabId);
          const context = page.context();
          const close = context.close.bind(context);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          vi.spyOn(context, "close").mockImplementation(async (options) => {
            entered.resolve();
            await gate.promise;
            return close(options);
          });
          const ending = yield* invoke(broker)<{ closed: Array<string> }>(tabId, "endLogin").pipe(
            Effect.forkScoped,
          );
          yield* Effect.promise(() => entered.promise);
          // While the context closes, reading the tab is refused, as it is in a login or
          // already gone, and a new tab of the profile waits for the close.
          expect(page.isClosed()).toBe(false);
          const during = yield* invoke(broker)<string>(tabId, "evaluate", {
            expression: "document.getElementById('pw').value",
          }).pipe(Effect.flip);
          expect(["PreviewCredentialError", "PreviewAutomationTabNotFoundError"]).toContain(
            during._tag,
          );
          expectNoSecret(during);
          const opening = yield* openTab(broker).pipe(Effect.forkScoped);
          gate.resolve();
          expect(yield* Fiber.join(ending)).toEqual({ closed: [tabId] });
          expect(page.isClosed()).toBe(true);
          const fresh = yield* Fiber.join(opening);
          const answer = yield* invoke(broker)<string>(fresh, "evaluate", {
            expression:
              "new Promise(resolve => { const channel = new BroadcastChannel('login'); channel.onmessage = e => resolve(e.data); channel.postMessage('ask'); setTimeout(() => resolve('no answer'), 500) })",
          });
          expect(answer).toBe("no answer");
        }),
      ),
    60_000,
  );

  it.live(
    "a tab still attaching when its profile's login ends does not open",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker);
          const opener = pageAt("/");
          yield* fillPassword(broker, tabId);
          const context = opener.context();
          const attach = context.newCDPSession.bind(context);
          const gate = Promise.withResolvers<void>();
          const entered = Promise.withResolvers<void>();
          let held = false;
          vi.spyOn(context, "newCDPSession").mockImplementation(async (target) => {
            if (target !== opener && "mainFrame" in target && !held) {
              held = true;
              entered.resolve();
              await gate.promise;
            }
            return attach(target);
          });
          // An ordinary new tab in the same profile, not a popup.
          const opening = yield* openTab(broker).pipe(Effect.result, Effect.forkScoped);
          yield* Effect.promise(() => entered.promise);
          const pending = context.pages().find((page) => page !== opener)!;
          expect(yield* invoke(broker)<{ closed: Array<string> }>(tabId, "endLogin")).toEqual({
            closed: [tabId],
          });
          gate.resolve();
          expect((yield* Fiber.join(opening))._tag).toBe("Failure");
          expect(pending.isClosed()).toBe(true);
        }),
      ),
    60_000,
  );

  it.live(
    "a popup from another tab of the profile publishes no URL during a login",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const manager = yield* Manager.PreviewManager;
          const first = yield* openTab(broker);
          const second = yield* openTab(broker);
          // The first page keeps what is typed; the second puts it in a popup's URL.
          yield* invoke(broker)(first, "evaluate", {
            expression:
              "document.getElementById('pw').addEventListener('input', e => { localStorage.echo = e.target.value })",
          });
          yield* invoke(broker)(second, "evaluate", {
            expression:
              "document.getElementById('popup').onclick = () => window.open('/child?pw=' + encodeURIComponent(localStorage.echo))",
          });
          yield* fillPassword(broker, first);
          // The whole profile is in the login, the unrelated second tab included.
          yield* refusedRead(broker, second);
          const events: Array<unknown> = [];
          yield* manager.events.pipe(
            Stream.runForEach((event) => Effect.sync(() => events.push(event))),
            Effect.forkScoped,
          );
          const popupOpened = yield* nextOpened(first);
          yield* invoke(broker)(second, "click", { locator: "#popup" });
          const popup = yield* popupOpened;
          yield* refusedRead(broker, popup);
          expectNoSecret(events);
          expectNoSecret((yield* manager.list({ threadId: scope.thread.threadId })).sessions);
          expectNoSecret(yield* invoke(broker)(popup, "status"));
        }),
      ),
    60_000,
  );

  it.live(
    "a later fill names the vault host, never a live origin that carries a value",
    () =>
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
    60_000,
  );

  it.live(
    "the MCP result and audit line name the vault host, never the live origin",
    () => {
      const mcpLayer = McpHttpServer.layerPreviewToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
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
    },
    60_000,
  );

  it.live(
    "an overlapping fill's refusal writes no page origin to the audit",
    () => {
      const mcpLayer = McpHttpServer.layerPreviewToolkit.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
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
    },
    60_000,
  );

  it.live(
    "a three-step sign-in finishes inside the login and a fresh tab keeps its cookie",
    () =>
      withBrowser(
        Effect.gen(function* () {
          const broker = yield* Broker.PreviewAutomationBroker;
          const tabId = yield* openTab(broker, "https://login.example.test/step1");
          const act = invoke(broker);
          const results = [
            yield* act(tabId, "fillCredential", {
              item: "Example",
              field: "username",
              locator: "input[type=email]",
            }),
            yield* act(tabId, "click", { locator: "button" }),
            yield* act(tabId, "waitFor", { locator: "input[type=password]" }),
            yield* fillPassword(broker, tabId, "input[type=password]"),
            yield* act(tabId, "click", { locator: "button" }),
            yield* act(tabId, "waitFor", { locator: "input[autocomplete=one-time-code]" }),
            yield* act(tabId, "fillCredential", {
              item: "Example",
              field: "totp",
              locator: "input[autocomplete=one-time-code]",
            }),
            yield* act(tabId, "click", { locator: "button" }),
            yield* act(tabId, "waitFor", { text: "Signed in", urlIncludes: "/signed" }),
          ];
          expectNoSecret(results);
          yield* refusedRead(broker, tabId);
          expect(yield* act<{ closed: Array<string> }>(tabId, "endLogin")).toEqual({
            closed: [tabId],
          });
          const fresh = yield* openTab(broker, "https://login.example.test/account");
          expect(
            yield* act(fresh, "evaluate", { expression: "document.body.innerText" }),
          ).toContain("Account signed in");
        }),
      ),
    60_000,
  );
});
