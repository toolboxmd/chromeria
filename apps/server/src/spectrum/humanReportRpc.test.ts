import { assert, it } from "@effect/vitest";
import { AuthOrchestrationOperateScope, AuthSessionId, ScheduledTaskId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { base } from "./controllerTestkit.ts";
import { SessionStore } from "../auth/SessionStore.ts";
import { SpectrumReportService } from "./ReportService.ts";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import { requireHumanReportSession, abandonScheduledReport } from "./humanReportRpc.ts";

const human: AuthenticatedSession = {
  sessionId: AuthSessionId.make("human-session"),
  subject: "paired-device",
  method: "browser-session-cookie",
  scopes: [AuthOrchestrationOperateScope],
};
it.effect("only authenticated human operate sessions may abandon reports", () =>
  Effect.gen(function* () {
    for (const session of [
      undefined,
      { ...human, subject: "mcp-client" },
      { ...human, scopes: [] },
    ]) {
      const error = yield* requireHumanReportSession(session).pipe(Effect.flip);
      assert.strictEqual(error._tag, "EnvironmentAuthorizationError");
    }
    yield* requireHumanReportSession(human);
    yield* requireHumanReportSession({ ...human, method: "bearer-access-token" }); // authenticated mobile devices
  }),
);

const forbiddenServices = Layer.mergeAll(
  base,
  Layer.mock(SessionStore)({ cookieName: "test", legacyCookieName: undefined }),
  Layer.mock(SpectrumReportService)({}),
);
it.effect(
  "the abandonment RPC rejects unauthenticated, MCP and read-only callers before touching storage or reports",
  () =>
    Effect.gen(function* () {
      for (const session of [
        undefined,
        { ...human, subject: "mcp-client" },
        { ...human, scopes: [] },
      ]) {
        const error = yield* abandonScheduledReport(session, {
          scheduledTaskId: ScheduledTaskId.make("task"),
          schedulerRunId: "run",
        }).pipe(Effect.flip);
        assert.strictEqual(error._tag, "EnvironmentAuthorizationError");
      }
    }).pipe(Effect.provide(forbiddenServices)),
);
