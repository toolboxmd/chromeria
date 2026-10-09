import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Commands from "./commandPlan.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
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
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("spectrum", controller.sweep);
    // The shared scheduler coalesces missed ticks and never queues global event bursts.
    // Each resume recovers raw stored events from its durable cursor.
  }),
);
export const layer = Layer.mergeAll(controller, worker.pipe(Layer.provide(controller)));
export const layerAdmission = Layer.mergeAll(Commands.layer, McpSend.layer, McpInterrupt.layer);
export const layerSchedulerAdapter = Adapter.layer;
