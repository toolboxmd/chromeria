import { assert, it } from "@effect/vitest";
import { AuthOrchestrationOperateScope, AuthSessionId, CommandId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Projection from "../orchestration-v2/ProjectionStore.ts";
import { layer as serialization } from "../orchestration-v2/ThreadCommandExecutor.ts";
import { initializeRecoveryHistory } from "../prism/RecoveryHistory.ts";
import * as Runtime from "./runtimeLayer.ts";
import * as Launch from "./LaunchService.ts";
import * as Controller from "./Controller.ts";
import * as Reports from "./ReportService.ts";
import * as Round from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import { base, input, setup } from "./controllerTestkit.ts";
import { stopSpectrum } from "./humanStopRpc.ts";
import { readSpectrum } from "./store.ts";

const dependencies = Layer.mergeAll(base, serialization);
const runtime = Layer.mergeAll(
  Runtime.layerAdmission.pipe(Layer.provide(dependencies)),
  Controller.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(Launch.layer, Reports.layer, Round.layer, Transcript.layer).pipe(
        Layer.provideMerge(dependencies),
      ),
    ),
  ),
);
const human = {
  sessionId: AuthSessionId.make("human"),
  subject: "paired-device",
  method: "browser-session-cookie" as const,
  scopes: [AuthOrchestrationOperateScope],
};

it.effect(
  "the GUI Stop RPC requires an authenticated human operate session before storage access",
  () =>
    Effect.gen(function* () {
      for (const session of [
        undefined,
        { ...human, subject: "mcp-client" },
        { ...human, scopes: [] },
      ]) {
        assert.strictEqual(
          (yield* stopSpectrum(session, {
            threadId: input.threadId,
            commandId: CommandId.make("unauthorized:stop"),
          }).pipe(Effect.flip))._tag,
          "EnvironmentAuthorizationError",
        );
      }
    }).pipe(Effect.provide(base)),
);
it.effect(
  "the GUI bridge stops a registered Spectrum, preserves replay, and never stops a normal thread",
  () =>
    Effect.gen(function* () {
      yield* setup;
      yield* initializeRecoveryHistory;
      const state = yield* (yield* Launch.SpectrumLaunchService).register(input);
      const controller = yield* Controller.SpectrumController;
      yield* controller.resume(state.threadId);
      const request = { threadId: state.threadId, commandId: CommandId.make("human:rpc-stop") };
      const first = yield* stopSpectrum({ ...human, method: "bearer-access-token" }, request);
      const retired = yield* controller.resume(state.threadId);
      assert.strictEqual(retired.status, "retired");
      assert.deepStrictEqual(yield* stopSpectrum(human, request), first);
      assert.deepStrictEqual(Option.getOrThrow(yield* readSpectrum(state.threadId)), retired);
      const projections = yield* Projection.ProjectionStoreV2;
      assert.strictEqual(
        (yield* projections.getThreadShell(state.threadId))!.forkSpectrumRunning,
        false,
      );
      for (const color of retired.participants)
        assert.isTrue(
          (yield* projections.getThreadRecords(color.threadId, ["runs"])).runs.every((run) =>
            ["cancelled", "interrupted", "completed", "failed", "rolled_back"].includes(run.status),
          ),
        );
      assert.strictEqual(
        (yield* stopSpectrum(human, {
          ...request,
          threadId: input.callerThreadId,
          commandId: CommandId.make("normal:refuse-stop"),
        }).pipe(Effect.flip))._tag,
        "SpectrumStopError",
      );
      assert.isUndefined((yield* projections.getThread(input.callerThreadId)).forkRetirement);
    }).pipe(Effect.provide(runtime)),
);
