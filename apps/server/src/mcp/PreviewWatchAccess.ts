import {
  AuthSessionId,
  OrchestratorMcpFailure,
  type PreviewWatchReadInput,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Invocation from "./McpInvocationContext.ts";
import { checkOutsideGrant } from "./outsideGrant.ts";

const denied = () =>
  new OrchestratorMcpFailure({
    code: "capability_denied",
    message: "Retained watch read is outside current thread or project authority.",
  });

/** This permission check uses durable metadata only, including after a retained-queue wait. */
export const authorizeWatchRead = (input: PreviewWatchReadInput) =>
  Effect.gen(function* () {
    const scope = yield* Invocation.McpInvocationContext;
    yield* checkOutsideGrant("preview_watch_read", input);
    const threads = yield* Threads.ThreadManagementService;
    const thread = yield* threads.getThreadShell(input.threadId).pipe(Effect.mapError(denied));
    if (thread === null || thread.deletedAt !== null || thread.archivedAt !== null)
      return yield* denied();
    if (scope.client === undefined) {
      if (scope.thread?.threadId !== input.threadId || !scope.capabilities.has("preview"))
        return yield* denied();
    } else {
      if (!scope.capabilities.has("orchestration")) return yield* denied();
      const sessions = yield* Effect.serviceOption(AuthSessions.AuthSessionRepository);
      if (Option.isNone(sessions)) return yield* denied();
      const session = yield* sessions.value
        .getById({ sessionId: AuthSessionId.make(scope.client.sessionId) })
        .pipe(Effect.mapError(denied));
      const now = yield* DateTime.now;
      if (
        Option.isNone(session) ||
        session.value.revokedAt !== null ||
        DateTime.isLessThanOrEqualTo(session.value.expiresAt, now)
      )
        return yield* denied();
    }
    const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
    if (Option.isSome(authority))
      yield* authority.value.checkTarget(scope, { type: "thread", threadId: input.threadId });
    else if (scope.authority !== undefined || scope.client?.linked === true) return yield* denied();
  });
