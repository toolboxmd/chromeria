import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthSessionId,
  LOCAL_DEVICE_HOST_ID,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpClient, HttpClientResponse, HttpRouter } from "effect/http";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as DeviceService from "./DeviceService.ts";
import * as DeviceHubProxy from "./DeviceHubProxy.ts";

const disposers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const dispose of disposers.splice(0)) await dispose();
});

const fixture = (
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  fail = false,
  authError?: EnvironmentAuth.ServerAuthCredentialError | EnvironmentAuth.ServerAuthInternalError,
  upstreamResponse?: Response,
) => {
  let finalized = 0;
  const requests: string[] = [];
  const upstreamHeaders: Array<Readonly<Record<string, string>>> = [];
  const client = HttpClient.make((request, _url, signal) =>
    Effect.gen(function* () {
      requests.push(request.url);
      upstreamHeaders.push(request.headers);
      signal.addEventListener("abort", () => {
        finalized++;
      });
      if (fail) return yield* Effect.die(new Error("upstream failed"));
      return HttpClientResponse.fromWeb(request, upstreamResponse ?? new Response("frame"));
    }),
  );
  const { handler, dispose } = HttpRouter.toWebHandler(
    DeviceHubProxy.layer.pipe(
      Layer.provideMerge(
        Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
          authenticateWebSocketUpgrade: () =>
            authError
              ? Effect.fail(authError)
              : Effect.succeed({
                  sessionId: AuthSessionId.make("test"),
                  subject: "test",
                  method: "bearer-access-token",
                  scopes,
                }),
        } as unknown as EnvironmentAuth.EnvironmentAuth["Service"]),
      ),
      Layer.provideMerge(
        Layer.succeed(DeviceService.DeviceService, {
          currentReadiness: () =>
            Effect.succeed({ hostId: LOCAL_DEVICE_HOST_ID, hub: { origin: "http://hub.test" } }),
        } as DeviceService.DeviceService["Service"]),
      ),
      Layer.provideMerge(Layer.succeed(HttpClient.HttpClient, client)),
    ),
    { disableLogger: true },
  );
  disposers.push(dispose);
  return { handler, requests, upstreamHeaders, finalized: () => finalized };
};

describe("device hub proxy", () => {
  it("releases the upstream response after forwarding its body and strips tickets", async () => {
    const { handler, requests, finalized } = fixture([AuthOrchestrationReadScope]);
    const response = await handler(
      new Request("http://t3.test/api/device-hub/api/devices?wsTicket=secret"),
    );
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("frame");
    expect(requests).toEqual(["http://hub.test/api/devices"]);
    expect(finalized()).toBe(1);
  });

  it("releases resources when upstream acquisition fails", async () => {
    const { handler, finalized } = fixture([AuthOrchestrationReadScope], true);
    const response = await handler(new Request("http://t3.test/api/device-hub/api/devices"));
    expect(response.status).toBe(500);
    expect(finalized()).toBe(1);
  });

  it.each(["/vendor/serve-sim/helper/ws", "/vendor/serve-emu/ws"])(
    "rejects input socket %s for a read-only session",
    async (path) => {
      const { handler, requests } = fixture([AuthOrchestrationReadScope]);
      const response = await handler(
        new Request(`http://t3.test/api/device-hub${path}`, { headers: { upgrade: "websocket" } }),
      );
      expect(response.status).toBe(403);
      expect(requests).toEqual([]);
    },
  );

  it("requires operate scope for stream tuning", async () => {
    const readOnly = fixture([AuthOrchestrationReadScope]);
    const path = "http://t3.test/api/device-hub/vendor/serve-emu/api/stream-settings";
    expect((await readOnly.handler(new Request(path, { method: "PATCH" }))).status).toBe(403);
    const operator = fixture([AuthOrchestrationOperateScope]);
    const response = await operator.handler(new Request(path, { method: "PATCH" }));
    expect(response.status).toBe(200);
    await response.text();
  });

  it("reads Android fold state but requires operate scope to change it", async () => {
    const path = "http://t3.test/api/device-hub/vendor/serve-emu/api/fold?device=emulator-5554";
    const reader = fixture([AuthOrchestrationReadScope]);
    const read = await reader.handler(new Request(path));
    expect(read.status).toBe(200);
    await read.text();
    expect(reader.requests).toEqual([
      "http://hub.test/vendor/serve-emu/api/fold?device=emulator-5554",
    ]);
    const denied = await reader.handler(
      new Request(path, { method: "POST", body: '{"posture":"closed"}' }),
    );
    expect(denied.status).toBe(403);
    expect(reader.requests).toHaveLength(1);

    const operator = fixture([AuthOrchestrationOperateScope]);
    const changed = await operator.handler(
      new Request(path, { method: "POST", body: '{"posture":"closed"}' }),
    );
    expect(changed.status).toBe(200);
    await changed.text();
    expect(operator.requests).toEqual([
      "http://hub.test/vendor/serve-emu/api/fold?device=emulator-5554",
    ]);
  });

  it("never forwards the vendor shell endpoint", async () => {
    const { handler, requests } = fixture([AuthOrchestrationOperateScope]);
    expect(
      (
        await handler(
          new Request("http://t3.test/api/device-hub/vendor/serve-sim/exec", { method: "POST" }),
        )
      ).status,
    ).toBe(404);
    expect(requests).toEqual([]);
  });
});

it.each([
  [new EnvironmentAuth.ServerAuthMissingCredentialError({}), 401],
  [
    new EnvironmentAuth.ServerAuthSessionCredentialValidationError({
      cause: new Error("private credential diagnostic"),
    }),
    500,
  ],
] as const)("translates authentication failure to HTTP %s", async (error, status) => {
  const { handler, requests } = fixture([], false, error);
  const response = await handler(new Request("http://t3.test/api/device-hub/api/devices"));
  expect(response.status).toBe(status);
  expect(await response.text()).not.toContain("private credential diagnostic");
  expect(requests).toEqual([]);
});

it.each([1, 3])(
  "forwards fixed Duo display %s through the authenticated read proxy",
  async (panel) => {
    const { handler, requests } = fixture([AuthOrchestrationReadScope]);
    const route = `/vendor/serve-sim/helper/duo/panel/${panel}/stream.avcc`;
    const response = await handler(
      new Request(`http://t3.test/api/device-hub${route}?wsTicket=secret`),
    );
    expect(response.status).toBe(200);
    await response.text();
    expect(requests).toEqual([`http://hub.test${route}`]);
  },
);

it.each(["/panel/2/stream.avcc", "/panel/1/webrtc/offer", "/panel/3/exec"])(
  "rejects unsupported Duo route %s",
  async (route) => {
    const { handler, requests } = fixture([AuthOrchestrationReadScope]);
    const response = await handler(
      new Request(`http://t3.test/api/device-hub/vendor/serve-sim/helper/duo${route}`),
    );
    expect(response.status).toBe(404);
    expect(requests).toEqual([]);
  },
);

// URL parsing must have one unambiguous target before any hub request starts.
it.each([
  "/vendor/serve-emu/api/screenshot?device=one&device=two",
  "/vendor/serve-emu/api/fold?device=one&serial=two",
  "/vendor/serve-sim/helper/one/stream.avcc?device=two",
  "/vendor/serve-sim/helper/one/stream.avcc?url=http://elsewhere.test",
  "/vendor/serve-emu/api/devices?token=private-daemon-token",
  "/api/devices?hostId=one&hostId=two",
  "/api/devices?wsTicket=one&wsTicket=two",
])("refuses ambiguous or undeclared hub query before upstream acquisition: %s", async (path) => {
  const { handler, requests } = fixture([AuthOrchestrationReadScope]);
  const response = await handler(new Request(`http://t3.test/api/device-hub${path}`));
  expect(response.status).toBe(400);
  expect(requests).toEqual([]);
});

it.each(["DELETE", "PUT", "PATCH"])(
  "refuses unsupported hub mutation method %s",
  async (method) => {
    const { handler, requests } = fixture([AuthOrchestrationOperateScope]);
    const response = await handler(
      new Request("http://t3.test/api/device-hub/vendor/serve-emu/api/fold?device=one", { method }),
    );
    expect(response.status).toBe(405);
    expect(requests).toEqual([]);
  },
);

it("does not disclose or follow a hub redirect", async () => {
  const { handler, requests } = fixture(
    [AuthOrchestrationReadScope],
    false,
    undefined,
    new Response(null, {
      status: 302,
      headers: { location: "http://private-daemon.test/token", "set-cookie": "daemon=private" },
    }),
  );
  const response = await handler(new Request("http://t3.test/api/device-hub/api/devices"));
  expect(response.status).toBe(502);
  expect(response.headers.get("location")).toBeNull();
  expect(response.headers.get("set-cookie")).toBeNull();
  expect(await response.text()).not.toContain("private");
  expect(requests).toEqual(["http://hub.test/api/devices"]);
});

it("keeps an HTTPS relay's forwarded protocol out of a loopback hub mutation", async () => {
  const { handler, upstreamHeaders } = fixture([AuthOrchestrationOperateScope]);
  const response = await handler(
    new Request(
      "https://relay.test/api/device-hub/vendor/serve-emu/api/fold?device=emulator-5554",
      {
        method: "POST",
        headers: { origin: "https://relay.test", "x-forwarded-proto": "https" },
        body: '{"posture":"opened"}',
      },
    ),
  );
  expect(response.status).toBe(200);
  await response.text();
  expect(upstreamHeaders).toHaveLength(1);
  expect(upstreamHeaders[0]?.origin).toBe("http://hub.test");
  expect(upstreamHeaders[0]).not.toHaveProperty("x-forwarded-proto");
});

it.each([
  ["PUT", "stream-mode", '{"mode":"scrcpy"}'],
  ["PATCH", "stream-settings", '{"h264Fps":30}'],
])("preserves pinned hub %s %s JSON requests", async (method, operation, body) => {
  const operator = fixture([AuthOrchestrationOperateScope]);
  const response = await operator.handler(
    new Request(
      `http://t3.test/api/device-hub/vendor/serve-emu/api/${operation}?device=emulator-5554`,
      { method, headers: { "content-type": "application/json" }, body },
    ),
  );
  expect(response.status).toBe(200);
  expect(operator.upstreamHeaders[0]?.["content-type"]).toBe("application/json");
  await response.text();
  const reader = fixture([AuthOrchestrationReadScope]);
  const denied = await reader.handler(
    new Request(
      `http://t3.test/api/device-hub/vendor/serve-emu/api/${operation}?device=emulator-5554`,
      { method, headers: { "content-type": "application/json" }, body },
    ),
  );
  expect(denied.status).toBe(403);
  expect(reader.requests).toEqual([]);
});
