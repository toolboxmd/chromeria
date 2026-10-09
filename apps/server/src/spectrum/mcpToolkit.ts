import {
  CommandId,
  ThreadId,
  PrismRole,
  PrismLane,
  ModelSelection,
  TrimmedNonEmptyString,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import { readThread, newCommandId, unavailable } from "../mcp/threadAccess.ts";
import * as Launch from "./LaunchService.ts";

const Color = Schema.Struct({
  label: TrimmedNonEmptyString,
  role: Schema.optional(PrismRole),
  lane: Schema.optional(PrismLane),
  selection: Schema.optional(ModelSelection),
  instructions: Schema.optional(Schema.String),
  instanceId: Schema.optional(TrimmedNonEmptyString),
  model: Schema.optional(TrimmedNonEmptyString),
  effort: Schema.optional(TrimmedNonEmptyString),
});
export const StartSpectrumInput = Schema.Struct({
  callerThreadId: Schema.optional(ThreadId),
  clientRequestId: Schema.optional(TrimmedNonEmptyString),
  question: TrimmedNonEmptyString,
  title: Schema.optional(TrimmedNonEmptyString),
  colors: Schema.Array(Color).check(Schema.isMinLength(2), Schema.isMaxLength(8)),
  mode: Schema.Literals(["council", "free"]),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
  moderator: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
});
export const StartSpectrumResult = Schema.Struct({
  threadId: ThreadId,
  callerThreadId: ThreadId,
  mode: Schema.Literals(["council", "free"]),
  participants: Schema.Array(
    Schema.Struct({
      threadId: ThreadId,
      label: Schema.String,
      instanceId: Schema.String,
      model: Schema.String,
      selection: ModelSelection,
    }),
  ),
});
export const SpectrumToolkit = Toolkit.make(
  Tool.make("start_spectrum", {
    description:
      "Open a visible provider-less Spectrum transcript with Drafter Colors chosen through Prism. Council runs independent answers, at least two relay barriers and moderator synthesis; free mode takes ordered turns. All replies are relayed verbatim. Send normal messages to join at the next barrier, reopen to continue after the report finishes or is explicitly abandoned, and Stop to cancel owned Drafter work. limit is council relay rounds (default2) or free turns (default3 per Color).",
    parameters: StartSpectrumInput,
    success: StartSpectrumResult,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [
      Crypto.Crypto,
      Invocation.McpInvocationContext,
      Threads.ThreadManagementService,
      Launch.SpectrumLaunchService,
    ],
  }).annotate(Tool.Title, "Start Spectrum"),
);
export const layer = McpToolAccess.toLayer(SpectrumToolkit, {
  start_spectrum: McpToolAccess.startsThreads(
    () => ({}),
    (input) =>
      Effect.gen(function* () {
        const { scope, projection } = yield* readThread(input.callerThreadId);
        const launch = yield* Launch.SpectrumLaunchService;
        const caller = yield* (yield* Threads.ThreadManagementService)
          .getThreadShell(projection.thread.id)
          .pipe(Effect.mapError(unavailable));
        if (caller === null)
          return yield* new OrchestratorMcpFailure({
            code: "thread_not_found",
            message: "The Spectrum caller was not found.",
          });
        const request = input.clientRequestId ?? String(yield* newCommandId());
        const key = `spectrum:${encodeURIComponent(scope.requestNamespace)}:${encodeURIComponent(request)}`;
        const state = yield* launch
          .register({
            commandId: CommandId.make(`${key}:register`),
            threadId: ThreadId.make(key),
            callerThreadId: projection.thread.id,
            callerRunId: caller.activeRunId,
            question: input.question,
            ...(input.title === undefined ? {} : { title: input.title }),
            colors: input.colors,
            mode: input.mode,
            limit: input.limit ?? (input.mode === "council" ? 2 : input.colors.length * 3),
            moderator: input.moderator ?? 0,
          })
          .pipe(Effect.mapError(unavailable));
        return {
          threadId: state.threadId,
          callerThreadId: state.callerThreadId,
          mode: state.mode,
          participants: state.participants.map((color) => ({
            threadId: color.threadId,
            label: color.label,
            instanceId: color.selection.instanceId,
            model: color.selection.model,
            selection: color.selection,
          })),
        };
      }),
  ),
});
