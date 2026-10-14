import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Tracer from "effect/Tracer";
import { vi } from "vite-plus/test";
import { SessionRenewal } from "@t3tools/client-runtime/authorization";

import * as ClientTracer from "../observability/clientTracer";
import { runtime } from "./runtime";

describe("web runtime", () => {
  it("constructs renewal without a primary when the desktop local environment is disabled", async () => {
    vi.stubGlobal("window", {
      location: new URL("t3code://app/"),
      desktopBridge: { getLocalEnvironmentEnabled: () => false },
    });
    try {
      await expect(
        runtime.runPromise(SessionRenewal.SessionRenewal.pipe(Effect.asVoid)),
      ).resolves.toBeUndefined();
    } finally {
      vi.unstubAllGlobals();
    }
  });
  it("routes client spans to the exporter client tracing configured", async () => {
    const exported: Array<string> = [];
    ClientTracer.setDelegate(
      Tracer.make({
        span(options) {
          exported.push(options.name);
          return new Tracer.NativeSpan(options);
        },
      }),
    );

    try {
      await runtime.runPromise(Effect.void.pipe(Effect.withSpan("client.work")));
    } finally {
      ClientTracer.setDelegate(null);
    }

    expect(exported).toEqual(["client.work"]);
  });
});
