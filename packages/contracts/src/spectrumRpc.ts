import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";

import { EnvironmentAuthorizationError } from "./auth.ts";
import { CommandId, ScheduledTaskId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * Fork (toolboxmd/chromeria#176): a person gives up on a Spectrum report that
 * holds a scheduled run at needs-you, so the run goes on to its check. Only
 * authenticated human sessions call it; agents and MCP clients never can.
 */
export const SPECTRUM_WS_METHODS = {
  abandonReport: "spectrum.abandonReport",
  stop: "spectrum.stop",
} as const;

/** How a scheduled run's error starts while a bound Spectrum report holds it at needs-you. */
export const SPECTRUM_REPORT_NEEDS_YOU_PREFIX = "Spectrum report:";

export class SpectrumReportAbandonError extends Schema.TaggedError<SpectrumReportAbandonError>()(
  "SpectrumReportAbandonError",
  { message: Schema.String },
) {}

export const SpectrumReportAbandonInput = Schema.Struct({
  scheduledTaskId: ScheduledTaskId,
  /** The run's id from the task's `outcomeCheck.run`. */
  schedulerRunId: TrimmedNonEmptyString,
});
export type SpectrumReportAbandonInput = typeof SpectrumReportAbandonInput.Type;

export const SpectrumReportAbandonResult = Schema.Struct({
  /** The report commands abandoned; empty when none was waiting. */
  abandonedCommandIds: Schema.Array(CommandId),
});
export type SpectrumReportAbandonResult = typeof SpectrumReportAbandonResult.Type;

/** Human GUI bridge to the server-only thread.stop command; never an MCP tool. */
export class SpectrumStopError extends Schema.TaggedError<SpectrumStopError>()(
  "SpectrumStopError",
  { message: Schema.String },
) {}
export const SpectrumStopInput = Schema.Struct({ threadId: ThreadId, commandId: CommandId });
export type SpectrumStopInput = typeof SpectrumStopInput.Type;
export const SpectrumStopResult = Schema.Struct({ sequence: Schema.Number });
export type SpectrumStopResult = typeof SpectrumStopResult.Type;

export const SpectrumRpcGroup = RpcGroup.make(
  Rpc.make(SPECTRUM_WS_METHODS.stop, {
    payload: SpectrumStopInput,
    success: SpectrumStopResult,
    error: Schema.Union([SpectrumStopError, EnvironmentAuthorizationError]),
  }),
  Rpc.make(SPECTRUM_WS_METHODS.abandonReport, {
    payload: SpectrumReportAbandonInput,
    success: SpectrumReportAbandonResult,
    error: Schema.Union([SpectrumReportAbandonError, EnvironmentAuthorizationError]),
  }),
);
