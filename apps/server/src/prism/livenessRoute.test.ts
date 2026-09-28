import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  PrismLiveness,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { HttpRouter } from "effect/unstable/http";
import { expect, it } from "vite-plus/test";

import { EnvironmentAuth } from "../auth/EnvironmentAuth.ts";
import { prismLivenessRouteLayer } from "./livenessRoute.ts";
import { StreamClock } from "./streamClock.ts";
import { makeStaleTurnDetectorState, StaleTurnDetector } from "./staleTurnDetector.ts";

const decodeLiveness = Schema.decodeUnknownSync(PrismLiveness);

it("serves the detector decision through the authenticated liveness route", async () => {
  const threadId = ThreadId.make("sub.parent.child");
  const live = {
    lastStreamAt: 0,
    openTool: null,
    turn: {
      turnId: "turn-1",
      provider: "opencode",
      model: "muse",
      startedAt: 0,
      eventCount: 1,
      firstTokenAt: 0,
    },
  };
  const state = makeStaleTurnDetectorState(() => {});
  state.observe(threadId, live, 0, "content.delta");
  state.tick(1_000, 1_000, []);
  const { handler, dispose } = HttpRouter.toWebHandler(
    prismLivenessRouteLayer.pipe(
      Layer.provideMerge(
        Layer.mock(EnvironmentAuth)({
          authenticateHttpRequest: () =>
            Effect.succeed({
              sessionId: AuthSessionId.make("test"),
              subject: "test",
              method: "bearer-access-token",
              scopes: [AuthOrchestrationReadScope],
            }),
        }),
      ),
      Layer.provideMerge(Layer.mock(StreamClock)({ liveness: () => Effect.succeed(live) })),
      Layer.provideMerge(Layer.succeed(StaleTurnDetector, { state, takeStale: Effect.never })),
    ),
    { disableLogger: true },
  );
  try {
    const response = await handler(
      new Request(`http://test/api/prism/liveness?threadId=${threadId}`),
    );
    expect(response.status).toBe(200);
    const body = decodeLiveness(await response.json());
    expect(body).toMatchObject({
      threadId,
      lastStreamAt: "1970-01-01T00:00:00.000Z",
      stale: true,
      staleSince: "1970-01-01T00:00:01.000Z",
      thresholdMs: 90_000,
      thresholdSource: "default",
      reason: "provider-dead",
    });
    expect(body.turn).not.toHaveProperty("firstTokenAt");
  } finally {
    await dispose();
  }
});
