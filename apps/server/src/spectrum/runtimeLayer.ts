import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import * as Commands from "./commandPlan.ts";
import * as McpSend from "./mcpSend.ts";
import * as Launch from "./LaunchService.ts";
import * as Rounds from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Reports from "./ReportService.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Controller from "./Controller.ts";

const services = Layer.mergeAll(Launch.layer, Rounds.layer, Transcript.layer, Reports.layer);
const controller = Controller.layer.pipe(Layer.provideMerge(services));
const worker = Layer.effectDiscard(
  Effect.gen(function* () {
    const controller = yield* Controller.SpectrumController;
    const sink = yield* Sink.EventSinkV2;
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("spectrum", controller.sweep);
    // Subscribe before replay. Wakeups are raw persisted event identities, never a wire projection.
    const sequence = yield* sink.latestSequence().pipe(Effect.orDie);
    yield* forkParked(
      sink.stream({ afterSequence: sequence }).pipe(
        Stream.runForEach(() => controller.sweep),
        Effect.catchCause((cause) => Effect.logWarning("Spectrum observer failed", { cause })),
      ),
    );
  }),
);
export const layer = Layer.mergeAll(controller, worker.pipe(Layer.provide(controller)));
export const layerAdmission = Layer.mergeAll(Commands.layer, McpSend.layer);
export const layerSchedulerAdapter = Adapter.layer;
