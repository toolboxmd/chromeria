import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  PREVIEW_AUTOMATION_SERVER_OPERATIONS,
  PreviewCredentialError,
  ProviderInstanceId,
  ThreadId,
  type PreviewAutomationFillCredentialInput,
  type PreviewAutomationConsoleEntry,
  type PreviewAutomationNetworkEntry,
  type PreviewCredentialField,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { McpSchema, McpServer } from "effect/ai";
import {
  chromium,
  type Browser,
  type BrowserContext,
  type CDPSession,
  type Page,
} from "playwright-core";

import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccessTestkit from "../mcp/McpToolAccess.testkit.ts";
import * as PreviewAutomationBroker from "../mcp/PreviewAutomationBroker.ts";
import { CREDENTIAL_AUDIT_FILE } from "./CredentialAudit.ts";
import { fillCredential, matchingItemHost } from "./CredentialFill.ts";
import type { CredentialVault } from "./CredentialVault.ts";
import * as ServerBrowserPage from "./ServerBrowserPage.ts";

// Characters that URL, form, JSON and HTML encoding all change.
const SENTINELS = {
  username: "sentinel-user-K7q@example.test",
  password: `sentinel-Pass+&=#?% 42<"'>`,
  totp: "804127",
} as const;
const ALL_SENTINELS = Object.values(SENTINELS);
const expectNoSentinel = (value: unknown) => {
  const text = JSON.stringify(value);
  for (const sentinel of ALL_SENTINELS) {
    expect(text).not.toContain(sentinel);
    expect(text).not.toContain(JSON.stringify(sentinel).slice(1, -1));
    expect(text).not.toContain(encodeURIComponent(sentinel));
  }
};

describe("matchingItemHost", () => {
  const table: ReadonlyArray<[string, ReadonlyArray<string>, boolean]> = [
    ["https://shopify.com/login", ["shopify.com"], true],
    ["https://accounts.shopify.com/login", ["shopify.com"], true],
    ["https://admin.shopify.com/store/x", ["https://shopify.com/"], true],
    ["https://admin.shopify.com", ["https://accounts.shopify.com/lookup"], false],
    ["https://evilshopify.com", ["shopify.com"], false],
    ["https://shopify.com.evil.com", ["shopify.com"], false],
    ["https://shopify.com:8443/", ["shopify.com"], true],
    ["https://SHOPIFY.com./", ["Shopify.COM"], true],
    ["https://example.com", ["androidapp://com.example"], false],
    ["https://com", ["com"], true],
    ["https://shopify.com", ["com"], false],
    ["https://10.0.0.5", ["0.5"], false],
    ["https://10.0.0.5", ["10.0.0.5"], true],
    ["https://shopify.com", [], false],
    ["https://shopify.com", ["not a uri::"], false],
  ];
  it.each(table)("%s against %j is %s", (page, uris, expected) => {
    expect(matchingItemHost(page, uris) !== undefined).toBe(expected);
  });
});

const LOGIN_PAGE = `<!doctype html><title>Sign in</title>
<form method="post" action="/session">
  <label>Email <input id="user" name="email" autocomplete="username"></label>
  <label>Password <input id="pw" name="password" type="password"></label>
  <label>Code <input id="otp" name="code" autocomplete="one-time-code"></label>
  <label>Short <input id="short" maxlength="3"></label>
  <button>Sign in</button>
</form>
<iframe title="identity" src="https://idp.other.test/frame"></iframe>
<script>
  for (const id of ["user", "pw", "otp"])
    document.getElementById(id).addEventListener("input", (event) => console.log(id + " " + event.target.value));
</script>`;
const GET_PAGE = `<!doctype html><title>Sign in</title>
<form method="get" action="/session">
  <label>Password <input id="pw" name="password" type="password"></label>
  <button>Sign in</button>
</form>`;
const FRAME_PAGE = `<!doctype html><label>Frame password <input id="frame-pw" type="password"></label>`;

describe("fillCredential in a real browser", () => {
  let browser: Browser;
  let context: BrowserContext;
  let page: Page;
  let cdp: CDPSession;
  let consoleEntries: Array<PreviewAutomationConsoleEntry>;
  let networkEntries: Array<PreviewAutomationNetworkEntry>;
  let vaultCalls: Array<string>;
  let itemUris: ReadonlyArray<string>;

  const vault: CredentialVault["Service"] = {
    uris: (item) =>
      Effect.sync(() => {
        vaultCalls.push(`uris:${item}`);
        return itemUris;
      }),
    read: (item, field) =>
      Effect.sync(() => {
        vaultCalls.push(`${field}:${item}`);
        return SENTINELS[field];
      }),
  };

  beforeAll(async () => {
    browser = await chromium.launch({ headless: true });
  });
  afterAll(async () => {
    await browser?.close();
  });
  beforeEach(async () => {
    context = await browser.newContext();
    await context.route("https://login.example.test/**", (route) =>
      route.request().method() === "POST" || route.request().url().includes("/session")
        ? route.fulfill({
            contentType: "text/html",
            // A destination that shows the submitted value, as the reviewer's probe did.
            body: `<title>Welcome ${new URL(route.request().url()).searchParams.get("password") ?? ""}</title><h1>Signed in</h1>`,
          })
        : route.fulfill({
            contentType: "text/html",
            body: route.request().url().endsWith("/get") ? GET_PAGE : LOGIN_PAGE,
          }),
    );
    await context.route("https://idp.other.test/**", (route) =>
      route.fulfill({ contentType: "text/html", body: FRAME_PAGE }),
    );
    await context.route("http://plain.example.test/**", (route) =>
      route.fulfill({ contentType: "text/html", body: LOGIN_PAGE }),
    );
    page = await context.newPage();
    cdp = await context.newCDPSession(page);
    typing = [];
    consoleEntries = [];
    networkEntries = [];
    vaultCalls = [];
    itemUris = ["https://example.test/"];
    page.on("console", (message) =>
      consoleEntries.push({ level: message.type(), text: message.text(), timestamp: "t" }),
    );
    page.on("response", (response) =>
      networkEntries.push({
        url: response.url(),
        method: response.request().method(),
        status: response.status(),
        failed: false,
        timestamp: "t",
      }),
    );
    await page.goto("https://login.example.test/login");
  });
  afterEach(async () => {
    await context.close();
  });

  let typing: Array<string>;
  const fill = (field: PreviewCredentialField, locator: string, item = "Example") =>
    fillCredential({
      tabId: "tab-1",
      vault,
      request: { item, field, locator, timeoutMs: 5_000 },
      locate: (selector) => ServerBrowserPage.targetLocator(page, { locator: selector })!,
      beforeTyping: async () => {
        typing.push(field);
      },
    });
  const refused = (promise: Promise<unknown>) =>
    promise.then(
      () => expect.fail("expected a refusal"),
      (error: ServerBrowserPage.ServerBrowserOperationError) => {
        expect(error).toBeInstanceOf(ServerBrowserPage.ServerBrowserOperationError);
        return error;
      },
    );
  // The test reads its own page to find refs; ServerBrowser never returns it in a login.
  const snapshot = () =>
    ServerBrowserPage.snapshot({
      page,
      cdp,
      renderScale: 1,
      consoleEntries,
      networkEntries,
      actionTimeline: [],
    });

  it("fills username, password and code without returning or showing them", async () => {
    const results = [
      await fill("username", "#user"),
      await fill("password", "#pw"),
      await fill("totp", "#otp"),
    ];
    expect(results).toEqual([
      {
        filled: true,
        item: "Example",
        field: "username",
        site: "example.test",
        tabId: "tab-1",
      },
      {
        filled: true,
        item: "Example",
        field: "password",
        site: "example.test",
        tabId: "tab-1",
      },
      {
        filled: true,
        item: "Example",
        field: "totp",
        site: "example.test",
        tabId: "tab-1",
      },
    ]);
    expectNoSentinel(results);
    expect(await page.inputValue("#user")).toBe(SENTINELS.username);
    expect(await page.inputValue("#pw")).toBe(SENTINELS.password);
    expect(await page.inputValue("#otp")).toBe(SENTINELS.totp);
    expect(vaultCalls).toEqual([
      "uris:Example",
      "username:Example",
      "uris:Example",
      "password:Example",
      "uris:Example",
      "totp:Example",
    ]);

    expect(typing).toEqual(["username", "password", "totp"]);
  });

  it("fills an input inside a cross-origin iframe against the frame's own origin", async () => {
    itemUris = ["other.test"];
    const frameRef = /textbox "Frame password" \[ref=([^\]]+)\]/.exec(
      String((await snapshot()).accessibilityTree),
    )![1];
    const result = await fill("password", `aria-ref=${frameRef}`);
    expect(result.site).toBe("other.test");
    const frame = page.frames().find((candidate) => candidate.url().startsWith("https://idp"))!;
    expect(await frame.inputValue("#frame-pw")).toBe(SENTINELS.password);
  });

  it("refuses a frame whose origin is not the item's, after reading only its URIs", async () => {
    const frameRef = /textbox "Frame password" \[ref=([^\]]+)\]/.exec(
      String((await snapshot()).accessibilityTree),
    )![1];
    const error = await refused(fill("password", `aria-ref=${frameRef}`));
    expect(error.tag).toBe("PreviewCredentialError");
    expect(error.detail).toEqual({
      reason: "originMismatch",
      tabId: "tab-1",
    });
    expect(vaultCalls).toEqual(["uris:Example"]);
    expect(typing).toEqual([]);
  });

  it("refuses a page outside the item's sites after reading only its URIs", async () => {
    itemUris = ["shopify.com"];
    const error = await refused(fill("password", "#pw"));
    expect(error.detail).toMatchObject({ reason: "originMismatch" });
    expect(vaultCalls).toEqual(["uris:Example"]);
    expect(await page.inputValue("#pw")).toBe("");
  });

  it("refuses http pages before touching the vault", async () => {
    await page.goto("http://plain.example.test/login");
    const error = await refused(fill("password", "#pw"));
    expect(error.detail).toEqual({ reason: "insecureOrigin", tabId: "tab-1" });
    expect(vaultCalls).toEqual([]);
  });

  it("refuses a non-text target before touching the vault", async () => {
    const error = await refused(fill("password", "role=button[name='Sign in']"));
    expect(error.tag).toBe("PreviewAutomationTargetNotEditableError");
    expect(vaultCalls).toEqual([]);
  });

  it("reports a value the input did not take without quoting it", async () => {
    const error = await refused(fill("password", "#short"));
    expect(error.detail).toMatchObject({ reason: "fillFailed" });
    expectNoSentinel([error.message, error.detail]);
  });

  it("passes vault refusals through with their reason", async () => {
    const error = await refused(
      fillCredential({
        tabId: "tab-1",
        vault: {
          ...vault,
          read: () => Effect.fail(new PreviewCredentialError({ reason: "vaultLocked" })),
        },
        request: { item: "Example", field: "password", locator: "#pw" },
        locate: (selector) => ServerBrowserPage.targetLocator(page, { locator: selector })!,
      }),
    );
    expect(error.detail).toMatchObject({ reason: "vaultLocked" });
  });

  it("types nothing when the caller refuses just before typing", async () => {
    const error = await refused(
      fillCredential({
        tabId: "tab-1",
        vault,
        request: { item: "Example", field: "password", locator: "#pw" },
        locate: (selector) => ServerBrowserPage.targetLocator(page, { locator: selector })!,
        beforeTyping: async () => {
          throw new ServerBrowserPage.ServerBrowserOperationError(
            "PreviewAutomationExecutionError",
            "A recording is running.",
          );
        },
      }),
    );
    expect(error.message).toBe("A recording is running.");
    expect(await page.inputValue("#pw")).toBe("");
  });

  describe("through the MCP tool", () => {
    const environmentId = EnvironmentId.make("environment-credential-test");
    const invocation = {
      environmentId,
      requestNamespace: "provider-session-credential-test",
      thread: {
        threadId: ThreadId.make("thread-credential-test"),
        providerSessionId: "provider-session-credential-test",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
      client: undefined,
      capabilities: new Set(["preview"] as const),
      issuedAt: 1,
    };
    const client = McpSchema.McpServerClient.of({
      clientId: 1,
      clientCapabilities: {},
      clientInfo: { name: "credential-test", version: "1.0.0" },
      protocolVersion: "2025-06-18",
      initializePayload: {
        protocolVersion: "2025-06-18",
        capabilities: {},
        clientInfo: { name: "credential-test", version: "1.0.0" },
      },
      getClient: Effect.die("unused"),
    });
    const layer = McpHttpServer.layerPreviewToolkit.pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provideMerge(McpToolAccessTestkit.liveThreadsLayer),
      Layer.provideMerge(PreviewAutomationBroker.layer),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-credential-fill-" })),
      Layer.provideMerge(NodeServices.layer),
    );

    it.effect("returns only the outcome and writes a value-free audit line per attempt", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const broker = yield* PreviewAutomationBroker.PreviewAutomationBroker;
          const connected = yield* Deferred.make<void>();
          const events = yield* broker.connect({
            clientId: "server-browser",
            environmentId,
            supportedOperations: [...PREVIEW_AUTOMATION_SERVER_OPERATIONS],
          });
          // Stands in for ServerBrowser: the real fill against the real page.
          yield* Stream.runForEach(events, (event) => {
            if (event.type === "connected") return Deferred.succeed(connected, undefined);
            const request = event.request;
            const respond = (
              outcome:
                | { ok: true; result: unknown }
                | { ok: false; error: { _tag: string; message: string; detail?: unknown } },
            ) =>
              broker.respond({
                clientId: "server-browser",
                connectionId: event.connectionId,
                requestId: request.requestId,
                ...outcome,
              });
            if (request.operation !== "fillCredential")
              return respond({
                ok: true,
                result: { available: true, tabId: "tab-1", url: page.url() },
              });
            return Effect.promise(() =>
              fillCredential({
                tabId: "tab-1",
                vault,
                request: request.input as PreviewAutomationFillCredentialInput,
                locate: (selector) => ServerBrowserPage.targetLocator(page, { locator: selector })!,
              }).then(
                (result) => ({ ok: true as const, result }),
                (error: ServerBrowserPage.ServerBrowserOperationError) => ({
                  ok: false as const,
                  error: { _tag: error.tag, message: error.message, detail: error.detail },
                }),
              ),
            ).pipe(Effect.flatMap(respond));
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(connected);

          const server = yield* McpServer.McpServer;
          const call = (args: Record<string, unknown>) =>
            server
              .callTool({ name: "preview_fill_credential", arguments: args })
              .pipe(
                Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
                Effect.provideService(McpSchema.McpServerClient, client),
              );
          for (const item of ["--field=password", "https://evil.test", "a;b", " Example"]) {
            const invalid = yield* call({ item, field: "password", locator: "#pw" }).pipe(
              Effect.flip,
            );
            expect(invalid._tag).toBe("InvalidParams");
          }
          expect(vaultCalls).toEqual([]);
          const filled = yield* call({ item: "Example", field: "password", locator: "#pw" });
          expect(filled.isError).toBe(false);
          expect(filled.structuredContent).toMatchObject({
            filled: true,
            item: "Example",
            field: "password",
            site: "example.test",
          });
          itemUris = ["shopify.com"];
          const mismatch = yield* call({ item: "Example", field: "username", locator: "#user" });
          expect(mismatch.isError).toBe(true);
          expect(mismatch.content).toEqual([
            {
              type: "text",
              text: new PreviewCredentialError({ reason: "originMismatch" }).message,
            },
          ]);
          expectNoSentinel([filled, mismatch]);

          const config = yield* ServerConfig.ServerConfig;
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const auditPath = path.join(config.stateDir, CREDENTIAL_AUDIT_FILE);
          const audit = yield* fileSystem.readFileString(auditPath);
          expectNoSentinel(audit);
          const lines = audit
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line));
          expect(lines).toMatchObject([
            {
              environmentId,
              threadId: invocation.thread.threadId,
              item: "Example",
              field: "password",
              site: "example.test",
              result: "filled",
            },
            {
              item: "Example",
              field: "username",
              site: null,
              result: "originMismatch",
            },
          ]);
          // Only these fields: nothing from the page or the agent's session.
          for (const line of lines)
            expect(Object.keys(line).toSorted()).toEqual([
              "environmentId",
              "field",
              "item",
              "result",
              "site",
              "threadId",
              "time",
            ]);
          expect(((yield* fileSystem.stat(auditPath)).mode & 0o777).toString(8)).toBe("600");
        }),
      ).pipe(Effect.provide(layer)),
    );
  });
});
