import { BareLaunchRequest } from "@t3tools/contracts";
import * as McpRouting from "../mcp/McpRouting.ts";
import {
  SpectrumRelayRounds,
  SpectrumTurnsPerColor,
  CommandId,
  NodeId,
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
import {
  readThread,
  newCommandId,
  unavailable,
  assertTargetWithinLimits,
} from "../mcp/threadAccess.ts";
import * as Launch from "./LaunchService.ts";
import { assertSameLink } from "../mcp/linkOrigin.ts";

const Candidate = Schema.Struct({
  selection: Schema.optional(ModelSelection),
  instanceId: Schema.optional(TrimmedNonEmptyString),
  model: Schema.optional(TrimmedNonEmptyString),
  effort: Schema.optional(TrimmedNonEmptyString),
});
const Color = Schema.Struct({
  bare: Schema.optional(BareLaunchRequest),
  label: TrimmedNonEmptyString,
  role: Schema.optional(PrismRole),
  lane: Schema.optional(PrismLane),
  selection: Schema.optional(ModelSelection),
  instructions: Schema.optional(Schema.String),
  instanceId: Schema.optional(TrimmedNonEmptyString),
  model: Schema.optional(TrimmedNonEmptyString),
  effort: Schema.optional(TrimmedNonEmptyString),
  fallbacks: Schema.optional(Schema.Array(Candidate).check(Schema.isMaxLength(4))),
});
export const StartSpectrumInput = Schema.Struct({
  callerThreadId: Schema.optional(ThreadId),
  clientRequestId: Schema.optional(TrimmedNonEmptyString),
  question: TrimmedNonEmptyString,
  title: Schema.optional(TrimmedNonEmptyString),
  colors: Schema.optional(Schema.Array(Color).check(Schema.isMinLength(2), Schema.isMaxLength(8))),
  mode: Schema.optional(Schema.Literals(["council", "free"])),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
  white: Schema.optional(TrimmedNonEmptyString),
  relayRounds: Schema.optional(SpectrumRelayRounds),
  turnsPerColor: Schema.optional(SpectrumTurnsPerColor),
  moderator: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
});
export const StartSpectrumResult = Schema.Struct({
  threadId: ThreadId,
  taskId: Schema.NullOr(NodeId),
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
      "Open a visible provider-less Spectrum transcript with Drafter Colors chosen through Prism. Council runs independent answers, at least two relay barriers and White's synthesis. Free mode opens in Color order, then White picks each next speaker with NEXT: <label> or DONE and writes the closing summary. All replies are relayed verbatim. Send normal messages to join at the next barrier, reopen to continue after the report finishes or is explicitly abandoned, and Stop to cancel owned Drafter work. Omitted colors, White, mode and limits use Spectrum settings for the caller’s project/environment. Configure default slots in Settings > Spectrum, then pass only question. Explicit colors replace all slots and inherit no fallbacks. Explicit colors ignore saved White: if white and moderator are omitted, the first explicit Color is White. white names a slot label; moderator is its legacy zero-based index. relayRounds is council relay rounds (minimum 2); turnsPerColor (1-12) times the number of Colors sets the free-mode cap on Color turns. Legacy limit overrides the current mode’s total limit. A Color's fallbacks are tried in order only when its turn cannot start. Started by an agent from its own thread, the Spectrum is that thread's delegated task: taskId works with task_status (the report is its result) and task_cancel (Stop); the report message is the only wake.",
    parameters: McpRouting.parameters(StartSpectrumInput),
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
        const context = yield* readThread(input.callerThreadId);
        const { scope, projection } = context;
        yield* assertSameLink(context, projection.thread);
        yield* assertTargetWithinLimits(context.limits, projection.thread);
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
            delegatedTask: scope.thread?.threadId === projection.thread.id,
            question: input.question,
            ...(input.title === undefined ? {} : { title: input.title }),
            colors: input.colors,
            mode: input.mode,
            limit: input.limit,
            moderator: input.moderator,
            white: input.white,
            relayRounds: input.relayRounds,
            turnsPerColor: input.turnsPerColor,
          })
          .pipe(
            Effect.mapError((error) =>
              error._tag === "SpectrumStartValidationError"
                ? new OrchestratorMcpFailure({ code: "invalid_request", message: error.message })
                : unavailable(),
            ),
          );
        return {
          threadId: state.threadId,
          taskId: state.delegatedTaskId,
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
    { refused: "Spectrum cannot be started through a peer link." },
  ),
});
