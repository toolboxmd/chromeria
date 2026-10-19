import * as Layer from "effect/Layer";
import * as Commands from "./commandPlan.ts";
import * as McpInterrupt from "./mcpInterrupt.ts";
import * as McpSend from "./mcpSend.ts";
import * as Launch from "./LaunchService.ts";
import * as Rounds from "./RoundService.ts";
import * as Transcript from "./TranscriptService.ts";
import * as Reports from "./ReportService.ts";
import * as Adapter from "./SchedulerAdapter.ts";
import * as Controller from "./Controller.ts";
import * as Wakes from "./wakes.ts";

const services = Layer.mergeAll(Launch.layer, Rounds.layer, Transcript.layer, Reports.layer);
const controller = Controller.layer.pipe(Layer.provideMerge(services));
export const layer = Layer.mergeAll(controller, Wakes.layer.pipe(Layer.provide(controller)));
export const layerAdmission = Layer.mergeAll(Commands.layer, McpSend.layer, McpInterrupt.layer);
export const layerSchedulerAdapter = Adapter.layer;
