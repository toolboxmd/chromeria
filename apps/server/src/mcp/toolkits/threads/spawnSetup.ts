import {
  CommandId,
  EventId,
  type OrchestrationProjectShell,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";

import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectSetupScriptRunner } from "../../../project/ProjectSetupScriptRunner.ts";

/** Reuse bootstrap setup semantics without going through the WebSocket transport. */
export const makeSpawnSetup = Effect.gen(function* () {
  const runner = yield* ProjectSetupScriptRunner;
  const engine = yield* OrchestrationEngineService;
  const crypto = yield* Crypto.Crypto;
  const scope = yield* Scope.Scope;
  return Effect.fn("ThreadsToolkit.runSpawnSetup")(function* (
    threadId: ThreadId,
    project: OrchestrationProjectShell,
    worktreePath: string,
  ) {
    const record = (
      kind: string,
      summary: string,
      tone: "info" | "error",
      payload: Record<string, unknown>,
    ) =>
      Effect.gen(function* () {
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        yield* engine.dispatch({
          type: "thread.activity.append",
          commandId: CommandId.make(
            `server:spawn-setup:${yield* crypto.randomUUIDv4.pipe(Effect.orDie)}`,
          ),
          threadId,
          activity: {
            id: EventId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie)),
            kind,
            summary,
            tone,
            turnId: null,
            createdAt,
            payload: { worktreePath, ...payload },
          },
          createdAt,
        });
      }).pipe(Effect.ignoreCause({ log: true }));
    const setup = yield* runner
      .runForThread({
        threadId,
        projectId: project.id,
        projectCwd: project.workspaceRoot,
        worktreePath,
        observeCompletion: {},
      })
      .pipe(
        Effect.catch((error) =>
          record("setup-script.failed", "Setup script failed to start", "error", {
            detail: error.message,
          }).pipe(Effect.andThen(Effect.logWarning(error.message)), Effect.as(null)),
        ),
      );
    if (setup?.status !== "started") return;
    const payload = {
      scriptId: setup.scriptId,
      scriptName: setup.scriptName,
      terminalId: setup.terminalId,
    };
    yield* record("setup-script.requested", "Starting setup script", "info", payload);
    yield* record("setup-script.started", "Setup script started", "info", payload);
    if (!setup.completion) return;
    // Always consume completion, even if the first turn fails to dispatch.
    // The server owns the listener's lifetime, as it owns the spawned thread.
    const completion = yield* setup.completion.pipe(
      Effect.flatMap(({ exitCode, durationMs }) =>
        record(
          exitCode === 0 ? "setup-script.completed" : "setup-script.failed",
          exitCode === 0 ? "Setup script finished" : "Setup script failed",
          exitCode === 0 ? "info" : "error",
          { ...payload, exitCode, durationMs },
        ),
      ),
      Effect.forkIn(scope),
    );
    // A failed script remains visible but does not discard the new worktree.
    if (!setup.async) yield* Fiber.join(completion);
  });
});
